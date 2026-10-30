# ADR-050: Real-time resource subscriptions

## Status

Proposed. Documentation only; no subscription API is implemented by this record.
Number reserved in [#576](https://github.com/hardbyte/awa/pull/576).

## Context

The first consumer is Awa's admin UI. Its question is broader than whether a job
changed state: is a queue falling behind, is a capable worker present, did rescue
recover progress, has a schedule reconciled, or has an operator's batch finished?
Applications may later want to refresh a view when their jobs complete.

The priority order is strict: job correctness, reliability and recovery first;
performance second; UX third. A lost observation must never lose a job or become
the only trigger for business work. Few production users are moving from 0.6 to
0.7, so a simpler public interface is preferable to preserving an accidental one.
That freedom does not remove the database rolling-upgrade contract.

Today the UI polls ordinary HTTP reads. Aggregate endpoints have TTL caches with
request coalescing; the server advertises a polling interval at least as large as
the cache TTL. Job detail and batch progress have their own intervals. This
proposal builds on those reads rather than creating another model of job state.

[ADR-049](049-job-filter-and-indexed-tags.md) is accepted, although its index row
still says Proposed. Its implementation is separately under review in
[#572](https://github.com/hardbyte/awa/pull/572). The initial recommendation was
formed independently of the separate filtering review. Subsequent document-specific review amendments are recorded below.

## Decision

Use **resource invalidation followed by authoritative refetch** over one
multiplexed **fetch-based SSE connection per visible admin tab**. An invalidation
means only "this resource may have changed". Reconnect resynchronizes current
state; there is no durable cursor, transition replay or acknowledgement.

The v1 implementation contract is
[Real-time subscriptions v1](../design/realtime-subscriptions-v1.md). It fixes
`POST /api/subscriptions/v1`, strict versioned JSON reads, finite experimental
limits, one refresh scheduler, cache-generation fences and the trusted-admin
origin policy. These are requirements of this proposal, not optional follow-up
choices. **Streaming is disabled by default** and requires explicit server
opt-in until load and deployment validation supports default enablement.

Start with a process-local hub, periodic invalidation and hints after committed
local admin mutations. Opening a stream performs zero database connections and
zero state queries. Add no subscription writes, triggers, outbox, transactional
directory, mandatory listener or worker changes on enqueue, claim, heartbeat,
finalization or rescue. Neither delivery nor reconciliation depends on a worker
or maintenance leader producing notifications.

Worker-originated updates remain cadence-limited in v1. SSE alone makes them
neither instantaneous nor cheaper than polling. Durable business reactions remain
ordinary Awa jobs, using [ADR-029](029-transactional-followup-jobs.md) and
[ADR-046](046-handler-staged-follow-ups.md) where applicable. Existing rescue
follow-ups remain best effort after rescue; this proposal does not strengthen
them.

### Resources and shared filters

V1 admits `jobs`, `job`, `dlq`, `queues`, `workers`, `schedules`, `operations`,
`maintenance` and `stats`, with exact descriptors and existing read mappings in
[the resource contract](../design/realtime-subscriptions-v1.md#initial-resources).
These cover current admin observations, not new metrics. Queue backlog, oldest
waiting age, throughput and concurrency pressure may only use existing fields
with documented accuracy. Presence is heartbeat evidence, and age can change
without a write; stream traffic proves neither liveness nor source freshness.

Standalone recovery resources, retained notices/history, named queue/worker/
schedule selectors and application consumers are excluded from v1. A DLQ view
can observe current retained state, but an ephemeral notice is not an audit
trail. `/storage?history=true` does not provide server-retained history. Catalog
and kinds views keep their existing read behavior.

`jobs` and `dlq` share ADR-049's `JobFilter` deserialization **and validation**.
Fields intersect, states are any-of, tags are all-of, and creation bounds refer
to original insertion. Paging stays separate. The hub routes coarse categories
and retains keys/category mappings rather than evaluating a second filter.
Thus a queue/state move invalidates both old and new populations; a replacement
page removes departed jobs and refills vacancies. Routing solely on a new row
would miss the leave. Filter changes abort/fence old streams and reads.

Capabilities are advisory across mixed-server routing. New filtered reads use
strict v1 JSON POST adapters; every response must agree on MIME, envelope and
version. An old server may return SPA HTML with HTTP 200, which is an error.
Legacy fallback permits unfiltered lists/details only; a nonempty filter remains
unsupported, never rewritten by dropping predicates. Counts retain ADR-049's
cap and are excluded from automatic refresh; invalidation only marks them stale.

### Recovery uses one bounded scheduler

Register and validate the complete resource set before `ready`, then refetch.
Client dirty generations preserve a hint received during a read as one pending
refresh, and superseded responses cannot overwrite the new view. Render a
successful response even if newer demand remains; do not wait for quiet traffic.

One tab-level scheduler handles hints, a jittered 30-second reconciliation
watchdog, fallback polling, focus/resume, reconnect and mutations. Managed queries
have no independent automatic intervals or focus/reconnect fetches. Hints and
keepalives do not postpone the watchdog. Every demand, including resync, obeys
the same concurrency, cooldown, admission and retry budgets. A transport failure
must not add a second polling loop. Hidden tabs pause reads and streams; resume
requests one paced resync. Selected jobs remain paused and stale until resumed,
so refreshing cannot replace a selection underneath an operator.

Transport deadlines run independently of the read iterator so a silent hung
connection is detectable. Empty scope, cap rejection, cancellation and BFCache
restore have explicit behavior in the contract. Preserve operator drafts too;
optimistic UI and notifications never replace atomic server-side mutation guards
or confer ownership through viewing/presence.

The guarantee is convergence to retained state returned by ordinary APIs after
changes settle and reads succeed. It is not observation of every job or
transition, nor stronger cross-table consistency. Claims can be absent or stale
while the lane cursor is outside MVCC; ordinary deferred-to-ready handoffs can
also be missed by a composite read. Repeatable Read alone does not make ring
`TRUNCATE` MVCC-safe. A local epoch is not a database position. A completion
pruned before a read may never be observed; a reliable business reaction needs
a durable job or application record at the source.

The experimental defaults bound a stream to 16 descriptors and 64 KiB, a process
to 64 streams and four expensive reads, and a tab to two concurrent reads. A
whole read has a 15-second deadline covering admission, pool acquisition, every
query/pass and serialization, with SQL and lock-wait limits inside it. Releasing
a Rust future is not proof that PostgreSQL work stopped: cancellation and pool
reuse must be tested. Full limits, overflow/slow-consumer behavior and the
60-second maximum retry backoff are in the companion contract.

### Server caches, observations and multiple instances

A post-commit helper advances a bounded cache generation, retires its old cache
entries, then signals the hub. A fill may publish/return as current only if its
captured generation still matches. New requests cannot join an obsolete fill;
retries stay within the remaining read budget. Moka `invalidate_all()` alone is
not an established proof. Client dirty generations and server cache generations
solve different races and both are required.

Read responses preserve source observation time on cache hits and distinguish
`observed_at`, `served_at` and `cache_age_ms`. Report replica freshness as unknown
unless measured. A primary hint does not prove a replica has replayed the change.
Cache hits may bypass expensive-read admission, but each fill acquires it once.
Slow clients cannot hold a database connection or block shared fanout; dirty
state collapses to resync, then close if it cannot be delivered within budget.

Every admin process owns its hub, caches and timer. Peer mutations and worker
activity converge through periodic reads; no broker, leader or sticky routing is
needed. Reconnection to a different instance always resynchronizes. Combined
read cost still grows with active views and process count and must be measured.
Links to UIs for different databases remain navigation only.

### Trusted-admin scope and transport

V1 is the existing trusted admin surface, protected by ingress/private networking.
It adds no tenant authorization framework. `--read-only` disables mutations,
not access to sensitive job data. New stream/read adapters use same-origin JSON
POSTs, explicit configured-origin checks and credential policy; an outer
permissive CORS layer must not undo that boundary. An origin check neither
authorizes a filter nor makes a public unauthenticated admin safe.

Never place credentials, args, outputs or secrets in any URL. Do not place raw
filter descriptors in subscription URLs or automatic subscription logs. Existing
shareable search URLs may contain documented nonsecret opaque filter values.
Descriptors travel in JSON request bodies and are not echoed in events/logs.

Choose SSE because the contract is one-way hints with ordinary HTTP mutations
and reads. V1 uses `fetch` plus AbortController; it has no native EventSource GET
variant, registration service or server-side subscription session. Resource-set
changes abort the old stream before opening its replacement, retaining displayed
data as stale. The client implements bounded parsing and reconnect/backoff;
the [SSE standard](https://html.spec.whatwg.org/multipage/server-sent-events.html)
defines framing; EventSource's automatic reconnect does not apply to fetch.
Epochs are connection identity, SSE `id` is absent, and `Last-Event-ID` never
bypasses resync.

Five-minute stream lifetimes permit ingress reauthentication but do not promise
immediate per-user revocation in a server without identity. Deployment checks
must cover the actual authentication proxy, buffering, compression and timeouts.
Protocol/admission errors follow the contract's status/MIME and retry rules,
including respecting `Retry-After` for both streams and refetches.

## Alternatives considered

### Durable event log or transactional outbox

A semantic append-only log could support replay after downtime and retained
notices. An outbox written in the job transaction could couple a fact atomically
to a transition, unlike a post-commit in-process hook. This is the right family
of design if observing every retained event becomes an actual requirement.

It also requires complete coverage of Rust/Python/raw/bridge inserts, callbacks,
rescue, compact completion batches, bulk actions, schedule reconciliation and
pruning. It adds WAL, retention, schema/version contracts, publication lag and
write-path failure modes for a UI that needs current state. A sequence allocated
before commit is not a safe replay watermark: a later id may commit first and
cause a consumer to skip the earlier transaction. Commit-safe ordering, an
initial snapshot boundary, idempotence, gap detection, retention floors and
expired-cursor resync all need a proven protocol.

Not selected. It would duplicate durable reaction machinery already expressed by
queue jobs. The proposed stream has no event table, consumer offset or replay
retention to prune. Ordinary job/output/operation retention still applies, and
pruning can make resources disappear; reads must not present absence as proof
that a job never existed.

### PostgreSQL LISTEN / NOTIFY as delivery or hints

[PostgreSQL NOTIFY](https://www.postgresql.org/docs/17/sql-notify.html) delivers
after commit to connected listeners, can fold duplicates, has a small payload
limit, and exposes notifications to database users. A full notification queue
can make notifying transactions fail at commit. It is not replayable history.
Adding it to every transition would couple optional UI delivery to job commits.

Not selected as delivery or as a new mandatory write-path mechanism. Existing
queue wakeups are incomplete hints for this vocabulary: they do not mean that
completion, worker expiry, schedules or prune changes were all observed. A later
optimization may use one dedicated session listener per admin process, retaining
timers and broad resync on listener reconnect. It must not create one PostgreSQL
connection per browser, rely on transaction-mode poolers for session state, or
send untrusted payloads straight to clients.

The [LISTEN startup rule](https://www.postgresql.org/docs/17/sql-listen.html) is
to commit registration before inspecting state. Even when followed, it does not
repair an incomplete signal source or a missed notification. Any new emission
path needs measured commit/WAL cost and review against
[ADR-045](045-hot-path-trigger-lock-contract.md); no trigger change is implied.

### Logical decoding / CDC

[Logical decoding](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html)
can provide a database change stream with an exported snapshot boundary. It has
replication-slot, privilege and failover requirements; consumers must tolerate
duplicates after crashes. A stalled slot can retain WAL and catalog resources.

Awa's physical representation includes ready rows, cursors outside MVCC, leases,
receipts, compact completions and partition truncation. Decoding those into
public resource changes would expose storage evolution or require a semantic
adapter with its own recovery proof. CDC remains useful for external history
and analytics as documented in
[managed Postgres](../deploying-on-managed-postgres.md#logical-replication-and-cdc),
but is not a required dependency for the admin UI.

### Polling only, pushed snapshots, and WebSocket

**Keep polling only.** This has the fewest moving parts and remains a valid
deployment/fallback. The initial timer-driven SSE design does not claim fewer
database reads. SSE buys a common resource vocabulary and immediate local
invalidation, with room for safe hints later. If that benefit does not justify
connection/proxy complexity in an implementation benchmark, retain polling
behind the same vocabulary instead of shipping an idle streaming layer.

**Push snapshots or row deltas.** Shared sampled aggregate snapshots can save
refetch round trips, but filtered job snapshots add per-subscriber query/state
work. Row deltas need correct enter/leave, paging and deletion semantics plus
the same recovery machinery. Start with refetch; consider pushed aggregate
snapshots only when measurements show the read cost matters.

**WebSocket.** It supports bidirectional subscription changes without reopening
and would suit a later interactive command protocol. This design needs neither;
it would add framing, reconnect and connection-state machinery without stronger
delivery guarantees. Revisit if resource-set churn or actual bidirectional
requirements justify it. The envelope and recovery semantics should survive a
transport change.

## Consequences

- Job execution and durable reactions remain independent of browser health.
  There is no new schema migration, per-job write tax or notification retention
  job. Admin read load still competes for database resources and must be bounded.
- Consumers get convergent views, not a replayable event API. Transient states,
  completions before pruning and historical recovery notices may be missed.
- Coarse invalidation simplifies correctness at the cost of extra reads and
  cadence-limited remote updates. Finer routing must not weaken leave/resync
  behavior. Low-latency coverage is explicitly incomplete in the first stage.
- The initial public surface can change for 0.7, but must negotiate capabilities
  and reject unsupported subscription shapes. An old server gets ordinary reads
  and polling only for unfiltered legacy lists/details; nonempty filters are
  unsupported. Never silently drop predicates to make fallback work.

## Delivery and validation gates

Implementation belongs in a separate draft PR, stacked on #572 or after its
merge, reusing its actual filter/compiler rather than copying one into the UI
crate. The [acceptance sequence](../design/realtime-subscriptions-v1.md#implementation-and-acceptance)
requires strict protocol adapters first, bounded hub/read work, deterministic
cache/cancellation tests, one frontend scheduler and complete local-mutation
coverage. It includes empty-install and read-only behavior without a worker,
mixed-version routing, slow transports, peer-process convergence and paused
selection/count exceptions.

Opt-in implementation may begin once these amendments are adopted in the draft;
this documentation PR implements none of it. Keep the implementation PR draft
until deterministic correctness and read-budget/cancellation tests pass. Default
enablement additionally needs PostgreSQL 17/18 load evidence and actual
ingress/pooler validation. Timer-driven SSE is not inherently cheaper than
polling; measure local-mutation freshness and total read/connection costs.

The earlier uniqueness-prune and DLQ downgrade findings remain separate release
gates. Subscriptions neither cause nor repair them. This record does not assert
those issues have been resolved.

The contract now fixes endpoint/encoding, initial resource scope, experimental
limits, scheduler, cache fencing, origin and fallback semantics. Production SLOs,
measured default enablement, application authorization and historical notice
retention remain unproven or outside v1. A requirement for replay or guaranteed
completion observation changes the contract and needs a further decision.

## Evidence and relationship to existing decisions

Inspected at main `f644f321ad110c6436382b64bdc0d935dcdeba5a` and #572 head
`512f477785d327ac092b94c6a399db4a8b5c4555` on 2026-10-30:

- [Dashboard cache](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/awa-ui/src/cache.rs)
  and [AppState](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/awa-ui/src/state.rs):
  typed TTL caches, request coalescing, poll guidance and local invalidation.
- [Frontend polling](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/awa-ui/frontend/src/hooks/use-poll-interval.ts)
  and [router](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/awa-ui/src/lib.rs):
  ordinary reads, existing resource families and current permissive CORS.
- [Existing worker notification hub](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/awa-worker/src/notify_hub.rs):
  a shared listener, poll fallback and broad wakeup after reconnect; precedent
  for hints with recovery, not evidence of full resource change coverage.
- [#572 JobFilter](https://github.com/hardbyte/awa/blob/512f477785d327ac092b94c6a399db4a8b5c4555/awa-model/src/filter.rs)
  and [job reads](https://github.com/hardbyte/awa/blob/512f477785d327ac092b94c6a399db4a8b5c4555/awa-ui/src/handlers/jobs.rs):
  strict population semantics and shared bounded reads under implementation.
- [ADR-049](049-job-filter-and-indexed-tags.md): shared filter, advisory
  in-flight reads, capped counts, tag retention and fleet compatibility.
  [ADR-030](030-batch-operations.md) owns durable operator actions;
  subscriptions only observe their progress.
- [ADR-029](029-transactional-followup-jobs.md) and
  [ADR-046](046-handler-staged-follow-ups.md) own durable follow-up work.
  This proposal neither replaces nor strengthens the guarantees of those paths.
- [ADR-041](041-rolling-upgrade-policy.md) and
  [ADR-045](045-hot-path-trigger-lock-contract.md) remain binding for any future
  schema or hot-path hint changes. The
  [operations skill](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/skills/awa-operations/SKILL.md)
  documents the trusted admin boundary and read-only operation.

The actual ADR draft at `3dd7ee3696e706d80c4a2777d7d0642f1e3480cc`
(blob `154f328a7249229ad3e0a25db83827595ea95f21`) subsequently received a
document-specific adversarial review. This amendment adopts its concrete v1
contract in the companion design document. That review supports the direction
subject to implementation tests; neither agreement nor docs CI establishes
architectural correctness, load safety or a tested implementation. No executable
review-model artifacts were supplied to this documentation session, so none are
claimed as verified evidence here.
