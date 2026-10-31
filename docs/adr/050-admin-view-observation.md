# ADR-050: Admin views use invalidation and authoritative refetch

## Status

Proposed. V1 uses improved polling. SSE, WebSocket and inter-process notification
listeners are deferred to a separate decision, not included as opt-in features.
[Draft PR #576](https://github.com/hardbyte/awa/pull/576) is documentation only.

## Problem and constraints

Operators need current views of jobs, queues, workers, schedules, the DLQ and
administrative progress. The UI already reads those resources through HTTP and
TTL caches, but independent page timers, ambiguous filter compatibility and
in-flight cache fills make freshness and load difficult to reason about.

The priority order is correctness, reliability and recovery first, performance
second, UX third. A fresh view must not become a dependency of enqueue, claim,
finalization or rescue. The initial consumer is the trusted admin UI. Durable
business triggers and an application-facing notification API are different
requirements.

A common refresh scheduler, whole-read deadlines, fenced cache fills, truthful
source timestamps and strict read contracts improve the current UI without a
stream. There is no measured operator need or transport comparison that justifies
adopting SSE alongside those changes.

## Decision and scope

**Use disposable invalidation hints, authoritative owning-API reads and independent
bounded reconciliation, implemented with polling in v1.** A local mutation can
mark the initiating tab's views stale and invalidate that server's caches. Other
tabs and admin processes discover changes through their own paced reads.

The five invariants are:

1. **Read authority.** A hint only requests a read. The owning API and shared
   `JobFilter` define contents; unsupported semantics fail closed. An older cache
   fill cannot undo invalidation or be relabelled as newly observed state.
2. **Independent recovery.** Register active read demand and arm reconciliation
   before the first read. Lost hints, errors and resumes are repaired by scheduled
   reads; a dirty mark received during a read remains pending afterwards.
3. **Bounded shared demand.** Polling, reconciliation, focus, retries and mutations
   use one scheduler and finite client/server budgets. Coalescing retains demand
   for every resource; overload reduces freshness rather than multiplying reads.
4. **Job-path independence.** Observation adds no job-transition writes, worker
   hooks, mandatory listener or event-processing engine. Reliable business work
   remains durable queue jobs, not a reaction to a UI observation.
5. **Trusted administration.** Read-only still exposes sensitive admin data;
   scopes cannot silently widen. UI refresh preserves selections/drafts and does
   not replace atomic mutation guards or confer ownership through presence.

V1 reuses existing read models and ADR-049's filter/compiler. It adds no new
metrics, retained notice history, tenant authorization framework or application
completion guarantee. The [polling-first v1 contract](../design/admin-observation-v1.md)
specifies startup, read protocols, deadlines, cache fencing and validation.

Convergence means retained state returned by ordinary APIs after changes settle
and reads succeed. It is not observation of every transition or a stronger
cross-table snapshot. Composite reads can miss claims or deferred-to-ready
handoffs; Repeatable Read does not make ring truncation MVCC-safe. A completed
job pruned before a read may never be observed. Follow-up jobs in
[ADR-029](029-transactional-followup-jobs.md) and
[ADR-046](046-handler-staged-follow-ups.md) remain the durable mechanism where
applicable; best-effort rescue follow-ups are not strengthened.

## Alternatives considered

### Improved polling — chosen

One scheduler, strict read boundaries, bounded work and truthful stale state
address current correctness and operational problems without persistent browser
connections. Each increment is useful without a transport project. Reads remain
sampled and worker/peer changes wait for the configured cadence. We accept that
tradeoff rather than fund unproven low-latency delivery.

### SSE or WebSocket — deferred

A stream could invalidate other views immediately after a committed local admin
mutation. A process-local hub reaches only viewers connected to that process.
With independent uniform routing over N processes, a particular viewer shares
the mutation's process with probability 1/N; other routing/topologies differ.
Worker changes gain no speed from a timer-driven stream, and adding such a stream
does not inherently reduce database reads.

SSE fits one-way HTTP hints; WebSocket supplies bidirectionality and subscription
changes on an existing connection. Both require bounded parsing/state, reconnect,
expiry, ingress/authentication handling and browser lifecycle work. Those costs
are not justified by the observation invariants, which polling already satisfies.
There is no v1 stream endpoint, descriptor protocol, parser or stream budget.
A future hint descriptor should carry only an opaque key, type and necessary job
identity; full filters belong in authoritative reads unless measured filtered
routing earns the extra complexity and privacy exposure.

### Admin-to-admin PostgreSQL hints — optional future optimization

A dedicated listener per admin process could invalidate peer caches after admin
mutations, without touching worker/job-execution writes. It would not push new
data to browsers: they still wait for polling. One replica already has local
invalidation; two replicas need evidence that stale-cache windows and mutation
frequency justify dedicated sessions and reconnect handling. A listener needs a
direct/session-pooled connection; transaction pooling cannot preserve its session
registration. We do not select this optimization for v1.

`NOTIFY` is delivered after its transaction commits, but queue exhaustion can
fail that commit. To isolate hint failure from the authoritative mutation, a
future design would use a separate bounded best-effort post-commit attempt.
Crash/loss is acceptable because reconciliation remains independent. Commit
`LISTEN` before the initial cache resync and resync on reconnect. Payloads are
visible to database users, so use coarse nonsecret categories, never filters,
args or outputs. No listener is required for correctness.

### Pushed snapshots or row deltas

Shared aggregate snapshots might save refetches. Filtered snapshots/deltas need
membership, population entry/exit, deletion and page-refill semantics in a second
view model. Current-state refetch keeps those responsibilities in existing APIs.
Consider pushed aggregates only if measured read cost makes that complexity useful.

### Durable event log/outbox or logical decoding

A log/outbox can support retained events and replay, but needs complete writer
coverage, commit-safe ordering, idempotence, retention and gap recovery. A sequence
allocated before commit is not a replay watermark. CDC additionally requires
slot/privilege/failover operations and a semantic adapter over leases, cursors,
compact completions and pruning. Those can serve historical/event consumers;
they impose unnecessary dependencies for sampled admin views.

## Consequences

Operators get bounded, recoverable views with explicit stale state. Missed hints
cannot leave a retained job's detail stale indefinitely after successful polling
resumes. Late fills and overlapping refreshes do not silently reverse invalidation.

Freshness remains sampled. The initiating tab can refresh after its own mutation;
other clients are not promised immediate invalidation. Separate processes have
independent caches and aggregate read costs. Current-state reads cannot guarantee
an audit trail, every completion, or an exact census during transitions.

Shared read hardening and strict adapters are independently useful follow-ups to
#572. They should not wait for a streaming decision. Existing GET parsing and
per-statement timeouts must not be represented as strict mixed-version reads or
an end-to-end deadline; the supporting contract records the verified gaps.
Its interactive-read budgets do not replace batch-scan or DLQ bulk-mutation
policies. When strict adapters ship, upgrade notes must explain that a new UI
routed to an old replica loses filtered-view availability rather than widening
the query through a permissive GET fallback.

## Evidence required for a later transport decision

Reopen transport selection only with an unmet operator freshness requirement or
a concrete push consumer, a measured improved-polling baseline and the intended
deployment topology. Compare coverage of local mutations, worker changes and
peer changes; database/read amplification, connections and browser freshness;
and supported ingress, authentication, expiry and reconnect costs. Name an owner
and explicit, measurable retain/reject criteria for benefit, coverage and cost
before starting any experiment.
The transport proposer supplies the numeric targets and evaluation timeframe
from that baseline.

A durable application event/completion requirement needs a different contract,
not a faster version of these hints. Scoped application authorization or actual
bidirectional interaction also changes the decision boundary.

## Links and evidence

- [Polling-first v1 contract](../design/admin-observation-v1.md): normative read,
  scheduler, cache and access requirements with incremental delivery gates.
- [ADR-049](049-job-filter-and-indexed-tags.md) and
  [PR #572](https://github.com/hardbyte/awa/pull/572): shared population semantics;
  [current source evidence](../design/admin-observation-v1.md#dependencies-and-source-evidence)
  distinguishes remaining GET/deadline gaps from resolved DLQ/UI issues.
- [Existing cache](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/awa-ui/src/cache.rs),
  [admin state](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/awa-ui/src/state.rs)
  and [polling policy](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/awa-ui/frontend/src/hooks/use-poll-interval.ts):
  the baseline, not an SSE performance comparison.
- [PostgreSQL NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html)
  and [LISTEN](https://www.postgresql.org/docs/current/sql-listen.html):
  transaction failure, payload visibility and registration behavior for the
  deferred admin-cache optimization.
- [ADR-041](041-rolling-upgrade-policy.md) and
  [ADR-045](045-hot-path-trigger-lock-contract.md): rolling-upgrade and hot-path
  constraints remain in force.
