# ADR-050: Admin views use invalidation and authoritative refetch

## Status

Proposed. The SSE transport experiment is opt-in and disabled by default.
[Draft PR #576](https://github.com/hardbyte/awa/pull/576) contains this decision
and its supporting design contract; it implements no subscription feature.

## Problem and constraints

The admin UI needs current views of jobs, queues, workers, schedules, the DLQ
and operator progress. Those views answer operational questions, not just whether
a job changed status. Today ordinary HTTP reads and TTL caches serve them, with
polling spread across pages. Adding push without a common refresh policy risks
multiplying reads while leaving missed notifications and stale cache fills
unresolved.

The priority order is job correctness, reliability and recovery first,
performance second, UX third. Observation must not become a dependency of
enqueue, claim, finalization or rescue. The initial consumer is the trusted admin
UI; a future application API must not acquire stronger delivery or authorization
promises by accident. The 0.6-to-0.7 transition permits interface simplification,
while the rolling-upgrade and database safety contracts still apply.

## Decision and scope

**Admin views use disposable invalidation hints, authoritative API reads and
independent bounded reconciliation.** This observation model is the enduring
choice. Fetch-based SSE is the proposed experimental transport for v1, not the
source of correctness and not a commitment to permanent transport infrastructure.

A hint means that a resource may have changed. The owning read API decides its
current contents, eligibility and accuracy. Filtered job views reuse
[ADR-049's JobFilter](049-job-filter-and-indexed-tags.md), including its bounds,
strict parsing and separation of population from paging. Unsupported filters
fail closed, including when successive requests reach servers of different
versions. Routing hints does not create a second filter evaluator.

The model has these invariants:

- **Recovery does not depend on hints.** Establish observation before acknowledging
  readiness, then refetch. Reconnect and scope changes resynchronize. Independent
  reconciliation repairs a missed final hint even while the transport looks
  healthy. A hint received during a read remains pending rather than being
  cleared by that response. Both entering and leaving a filtered population must
  invalidate the view.
- **Demand is bounded and shared.** Hints, polling, reconciliation, resume and
  retries feed one paced refresh policy. Repeated demand coalesces without one
  resource overwriting another. Read work, waiting work, fanout and slow clients
  have finite budgets. Overload reduces freshness rather than creating more
  polling or delaying job execution through an unbounded read backlog.
- **Invalidation cannot be undone by a late cache fill.** Once local state is
  invalidated, an older in-flight fill cannot reappear as current. Observation
  timestamps retain the source sample's age. Transport activity is distinct from
  source freshness; it cannot prove a worker alive or a replica caught up.
- **Observation stays off the job write path.** V1 adds no worker hooks, database
  triggers, mandatory listener, event log or subscription writes to job
  transitions. Each admin process can refresh independently; peer-process and
  worker changes converge through reads without a shared broker or leader.
- **Domain guards and access boundaries remain authoritative.** Hints and
  optimistic UI cannot enforce ownership, exclusivity or idempotency. Mutations
  retain atomic server guards, and refresh preserves selections and operator
  drafts. Read-only access remains sensitive admin access. Scopes cannot silently
  widen, and a connection does not extend authorization indefinitely.

Authoritative reads retain their existing limitations. Composite loading can
miss an in-flight claim or deferred-to-ready handoff; neither a stream nor
Repeatable Read makes ring pruning an atomic snapshot. Convergence means retained
state returned by ordinary APIs after changes settle and reads succeed, not
observation of every job or transition. A completion pruned between reads may
never be observed. Durable business reactions remain queue jobs through
[ADR-029](029-transactional-followup-jobs.md) and
[ADR-046](046-handler-staged-follow-ups.md) where applicable; existing best-effort
rescue follow-ups are not strengthened.

V1 remains within the existing trusted-admin boundary and existing read models.
It adds no tenant authorization framework, presence ownership, new queue metrics
or retained notification history. Protocol shapes, finite experimental limits,
cache mechanisms, lifecycle handling and acceptance tests live in
[the supporting v1 contract](../design/realtime-subscriptions-v1.md).

### Why experiment with SSE

A one-way HTTP stream fits invalidation from an admin process to its connected
views. Its immediate incremental opportunity is fanout after a committed local
admin mutation, so other views connected to the same process can refetch sooner
than their next poll. This does not accelerate worker-originated or peer-process
changes beyond the sampling cadence, and timer-driven SSE does not inherently
reduce database reads.

Streaming therefore stays disabled by default. The experiment must compare it
with improved polling, measuring local-mutation freshness alongside total read,
connection and operational costs. PostgreSQL load tests must show job execution
and recovery remain protected; actual ingress/pooler tests must establish that
authentication, buffering and connection lifecycle behave as intended. A useful
observation model does not by itself justify enabling a transport.

## Alternatives considered

### Improved polling with the same observation model

This is the strongest baseline and the default path while streaming is disabled.
One scheduler,
coalesced reads, bounded caches, clear stale state and reconciliation solve much
of the problem without long-lived HTTP connections. It handles worker and
peer-process changes at the same sampling cadence as timer-driven SSE.

Polling lacks immediate server fanout of a local mutation to other connected
views. SSE is worth retaining only if that incremental freshness, or another
measured benefit, justifies its costs. If it does not, keep improved polling;
none of the enduring invariants requires streaming.

### Fetch-based SSE

Selected for the opt-in v1 experiment because one-way hints fit the existing
HTTP admin surface and ordinary requests still perform reads and mutations.
It adds reconnect, parsing, proxy, authorization-lifetime and browser lifecycle
work; using fetch does not provide EventSource's automatic recovery behavior.
The supporting contract bounds that work and defines safe fallback, but those
requirements are costs to validate, not evidence of an advantage already earned.

### WebSocket

WebSocket can change subscriptions bidirectionally without replacing a stream
and would suit an actual interactive command protocol. V1 needs one-way hints,
so that bidirectionality is unused and SSE fits the current HTTP request model.
Both transports still need bounded state, parsing, reconnect, authorization and
lifecycle handling. Choose between them on demonstrated requirements and deployed
behavior, not on a claim that fetch-SSE avoids that complexity.

### Pushed snapshots or row deltas

Shared aggregate snapshots might save refetch round trips. Filtered snapshots
and row deltas require server-side query or membership state, including page
refill, population entry/exit and deletion semantics. Refetch keeps the existing
read API authoritative and avoids maintaining that second view model. Consider
pushed aggregates if measurements identify a material benefit without weakening
recovery.

### Durable event log or transactional outbox

A log can retain transitions and support replay; a transactional outbox can couple
publication to a committed state change. Those are appropriate capabilities when
every transition is required. They also need complete writer coverage, commit-safe
ordering, snapshot/replay boundaries, idempotence, retention and gap recovery. A
sequence allocated before commit is not itself a safe replay watermark.

Current admin views need retained state, while durable reactions already use
jobs. Paying WAL, publication and retention costs on job writes to obtain UI
freshness would introduce an unnecessary dependency. No log, outbox or
transactional directory is selected here.

### PostgreSQL LISTEN / NOTIFY

Existing wakeups can be useful disposable hints but do not cover all admin
resources or provide replay. New transactional emissions would add job-path cost;
a full notification queue can make notifying transactions fail at commit.
A mandatory listener would add session and deployment requirements without
removing the need for reconciliation. A later optional hint optimization must
preserve the observation model and earn its write/connection cost separately.

### Logical decoding / CDC

CDC can provide a change stream with a snapshot boundary, but requires slot,
privilege, failover and retention operations. Mapping Awa's leases, cursors,
compact completions and partition pruning into public changes would also require
a semantic adapter. It remains useful for external history and analytics, not a
required dependency of current admin views.

## Accepted consequences

Consumers can converge after lost hints, reconnects and process changes
without introducing a second event-processing engine. Job execution and durable
reactions remain independent of browser availability.

The tradeoff is sampled freshness, potentially redundant reads and no guarantee
of observing transient or already-pruned state. Coarse invalidation deliberately
trades routing precision for simpler entry/exit correctness. Multiple admin
processes have independent caches and read costs; their combined load still
needs a budget. A successful stream does not imply a healthy queue or current
source data.

The transport adds operational and client lifecycle costs while it remains an
experiment. Production SLOs and default enablement need evidence; documentation
checks establish neither implementation correctness nor capacity.

## Revisit conditions

Revisit the rationale when:

- A consumer requires replay, audit history or guaranteed completion observation;
  current-state refetch cannot provide those guarantees.
- Required worker or peer-process update latency cannot be met by sampled reads;
  investigate a safe source of hints rather than treating SSE alone as a fix.
- Measured read/connection cost or ingress behavior makes streaming less useful
  than improved polling, or makes shared pushed aggregates worthwhile.
- Application consumers require scoped authorization beyond trusted admin access,
  or real bidirectional interaction changes the transport requirements.

Revising a transport or sampling policy must preserve the enduring invariants
unless a later decision explicitly replaces them.

## Links and evidence

- [V1 implementation contract](../design/realtime-subscriptions-v1.md): protocol,
  budgets, cache fencing, browser lifecycle, security policy and validation gates.
- [ADR-049](049-job-filter-and-indexed-tags.md) and
  [implementation PR #572](https://github.com/hardbyte/awa/pull/572): shared job
  selection and advisory read behavior. The proposed adapter reuses that compiler.
- [Existing dashboard cache](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/awa-ui/src/cache.rs),
  [admin state](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/awa-ui/src/state.rs)
  and [frontend polling](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/awa-ui/frontend/src/hooks/use-poll-interval.ts):
  the current read/cache baseline, not a measured comparison with SSE.
- [Existing notification hub](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/awa-worker/src/notify_hub.rs):
  hints with polling recovery, not complete admin change coverage.
- [PostgreSQL NOTIFY](https://www.postgresql.org/docs/17/sql-notify.html),
  [LISTEN startup](https://www.postgresql.org/docs/17/sql-listen.html),
  [logical decoding](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html)
  and [SSE framing](https://html.spec.whatwg.org/multipage/server-sent-events.html):
  substrate behavior, not proof of end-to-end freshness or delivery.
- [ADR-041](041-rolling-upgrade-policy.md) and
  [ADR-045](045-hot-path-trigger-lock-contract.md): existing rolling-upgrade and
  hot-path constraints remain in force.
