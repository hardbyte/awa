> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

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

[ADR-049](../049-job-filter-and-indexed-tags/index.md) is accepted, although its index row
still says Proposed. Its implementation is separately under review in
[#572](https://github.com/hardbyte/awa/pull/572). Neither this proposal nor its
recommendation depends on that PR's independent adversarial review.

## Decision

Use **resource invalidation followed by authoritative refetch**, transported over
**one multiplexed SSE connection per admin tab**. An invalidation means only
"this resource may have changed". Reconnect always resynchronizes current state;
there is no durable event cursor, transition replay or delivery acknowledgement.

Start with a process-local hub, periodic resource invalidation, and immediate
hints after successful local admin mutations. Add no subscription writes,
triggers, outbox or mandatory listener to enqueue, claim, heartbeat, finalization
or rescue. Periodic client reconciliation remains mandatory even while SSE is
healthy. A failed or overloaded subscription system falls back to bounded reads.

This deliberately leaves worker-originated updates at a sampling cadence in the
first delivery. SSE alone does not make them instantaneous or cheaper than
polling. Faster hints are a later, measured optimization; they cannot replace
reconciliation. Durable business reactions remain ordinary Awa jobs, using the
transactional mechanisms in [ADR-029](../029-transactional-followup-jobs/index.md) and
[ADR-046](../046-handler-staged-follow-ups/index.md) where applicable.

### Resources, not storage events

The vocabulary describes observable resources, not tables or event names such as
`job.completed`. These are candidate use cases and an extensible vocabulary,
not a commitment to build every resource in the first release:

| Resource | What a consumer would refetch |
| --- | --- |
| `jobs(filter)` / `job(id)` | A filtered population or one job, including retained output and progress. |
| `queues(names?)` | Health, backlog, oldest waiting age, throughput and concurrency pressure. |
| `workers(ids?)` | Presence, liveness evidence, capabilities and queue coverage. |
| `recovery(queues?)` | Rescue and DLQ summaries or retained notices. |
| `schedules(names?)` | Definitions, pause/trigger lifecycle and reconciliation status. |
| `operations(ids?)` | Bulk operator action progress, cancellation and results. |
| `maintenance(scope?)` | Storage maintenance, pruning and transition progress. |

"Authoritative refetch" means the normal read API, with its documented accuracy
and retention limits. It does not turn sampled queue health, worker heartbeats
or a capped count into an exact instantaneous census. Oldest waiting age changes
with time without a write; worker liveness expires without a disconnect event;
throughput needs a measured time window. Their reads must define those meanings
and expose observation time before subscriptions can improve their presentation.
An invalidation cannot invent a metric the read API does not yet supply.

Application completion notifications use `job(id)` or `jobs(filter)` to refresh
current application-visible state. A job that completes and is pruned between
reads may never be observed as completed. A consumer needing that fact for a
business action must arrange a durable job or application record at the source.
Similarly, an ephemeral rescue/DLQ toast is not an audit trail; historical notices
need a separately specified retained read model.

### Share JobFilter without a second evaluator

`jobs(filter)` and DLQ populations use ADR-049's `JobFilter`, validation and wire
semantics: fields intersect, states are any-of, tags are all-of, and creation
bounds refer to original insertion. Reuse its strict deserializer and bounds;
reject unknown resource kinds, fields and unsupported protocol versions rather
than silently widening a subscription. Authorization is an additional scope,
never something inferred from a tag.

Paging stays outside the population. A subscription invalidates the population;
the client refetches its bounded page using the normal page parameters. Filter
changes create a new client subscription identity and discard old responses.
There is no promise of a stable multi-page snapshot or an endless live list.

Do not evaluate every transition against every subscriber's filter. Initially,
coarse resource-category invalidation is sufficient, even if it over-invalidates.
If later hints carry queue or job identity, narrow routing only when it is
provably safe; unknown routing information invalidates the broader category.
The read API, not hub routing, decides membership.

This handles **leaving** a population as well as entering it. For example, a job
moving from queue A to B or from `running` to `completed` must invalidate its old
view too. Routing solely on the new row misses the leave. No before-images or
per-client membership sets are needed for coarse invalidation. The replacement
page removes departed jobs and fills vacancies from current matches. A short
transition entirely between reads can be invisible, by design.

Counts retain ADR-049's cap, timeout and default policy of fetching on filter
change rather than polling. A population invalidation may mark the count stale;
it does not automatically run a count per notification. Explicit refresh or a
debounced refresh after an operator action can renew it. Live subscribed counts
would need their own measured read budget and an explicit change to that policy.

### Establishing a view and recovering it

The protocol should have a small versioned envelope: `ready` (connection epoch,
accepted resource identifiers and refresh guidance), `invalidate` (a bounded set
of resource identifiers), and `resync` (discard freshness assumptions for all
accepted resources). These names describe the proposal, not shipped endpoints.
An epoch is local connection identity, never a database commit position.

1. Validate and authorize the complete requested resource set, register it with
   the hub, then send `ready`. The client can display a preliminary read while
   connecting, but must fetch again after `ready`.
2. Refetch accepted resources. Track a client-local dirty generation per resource
   when invalidations arrive. A response does not clear invalidations received
   since that fetch started: keep one pending refetch, paced by the read budget.
   Render successful snapshots even under continuous traffic; never wait for
   the stream to become quiet.
3. Ignore responses from superseded filters/connections and serialize or fence
   requests per query key so an older response cannot overwrite a newer one.
4. Reconcile visible active resources periodically regardless of hints. Reconnect,
   tab resume, server restart, overflow or a subscription change triggers a full
   resync. Refetch failures retain stale status and retry with bounded backoff;
   they do not count as successful reconciliation.

This closes the ordinary fetch-before-subscribe gap without claiming an atomic
snapshot plus database stream. A missing hint or a job invisible during a claim
is repaired by a later read if that state is still retained and readable. The
guarantee is eventual convergence of observable retained state after changes
settle, given successful reads and fair scheduling. It is **not** observation of
every job or every transition. ADR-049 explicitly permits jobs mid-claim to be
absent or shown in their previous state because the lane cursor is outside MVCC.
Neither a repeatable-read transaction nor a local epoch removes that limitation.

The first implementation should advertise configurable refresh and reconciliation
intervals. A reasonable starting experiment is the existing server poll interval
for periodic invalidations, a 30-second client reconciliation watchdog, and
immediate refetch on reconnect/resume. These are proposed defaults, not measured
SLOs. Effective freshness also includes scheduler delays, upstream telemetry
refresh, query/cache time and replica lag; transport heartbeats prove none of
those are current. Display the last successful observation time and stale/error
state. Keep existing polling when the stream is unsupported or unhealthy.

### Producing invalidations safely

Each admin process maintains a bounded hub for its active resource categories.
Its timer emits coarse invalidations at the advertised cadence without scanning
jobs or comparing every filter. Local mutation handlers invalidate affected
server caches and signal the hub only after their database operation commits.
Other processes' mutations and worker activity are covered by periodic reads;
there is no dependency on a worker or maintenance leader emitting UI hints.

Refetch must not repeatedly return a server cache entry from before an immediate
hint. Existing dashboard cache invalidation is a starting point, but races with
an already in-flight cache fill need generation fencing: a fill started before
invalidation must not republish itself as fresh afterwards. Timer-driven refetch
must respect cache expiry. Retaining a bounded TTL and later reconciliation is
the recovery backstop; bypassing caches on every event would defeat coalescing.

No transaction or database connection is held for the life of an SSE stream.
No new writer permission is needed, so read-only admin instances work too.
Against a replica, refresh reads that replica's state and reports its lag/unknown
freshness; a primary hint would not prove replay has caught up. The initial
timer design works through transaction-mode poolers and on replicas without
requiring `LISTEN` support.

### Bounded work and multiple admin instances

Merge repeated invalidations into a dirty set, with at most one pending marker
per admitted resource. Use a short debounce with a maximum delay, one in-flight
read per query key, bounded read concurrency, statement timeouts and jitter.
Resource counts, descriptor bytes, connections per principal/instance, event
size, pending bytes and send time must all have hard limits before exposure.
Exact limits and admission behavior require load evidence; they are not supplied
by this ADR's design analysis.

Overflow collapses to one all-resources `resync`, rather than a queue of changes.
If even that cannot be delivered promptly, close the connection; reconnect
resynchronizes. Slow clients never block the shared hub, a database reader,
workers or producers. Cap fanout and connection admission; answer excess
admission with a retryable failure and continue paced polling. Under sustained
database overload, serve stale/error state and reduce refresh frequency rather
than building an unbounded retry backlog. A subscription is optional visibility,
not an obligation to keep every browser fresh at the expense of jobs.

Share aggregate reads through the existing typed caches. If filtered-page reads
are later coalesced, key them by backend, authorization scope, full normalized
filter and page; bound cache cardinality. Do not introduce an unbounded cache
entry or database task per distinct filter. One hub fanout is still proportional
to admitted connections, and uncached reads grow with active client views.

Several admin processes may serve the same database. Each owns its own caches,
timer and clients; no leader, broker, sticky routing or shared browser session
is required. A load balancer can reconnect a client to any instance, which sends
`ready` and requires refetch again. An immediate local hint need not reach peer
instances; their cadence determines freshness. Independent read cost grows with
instance count, so measure the combined budget. Links to UIs serving different
databases remain navigation only, as they are today.

### Read scope and transport

Subscriptions are read-only and inherit the read API's trust boundary. Awa's
admin currently has no built-in authentication; ingress/private-network controls
must protect streams as well as reads. `--read-only` disables mutations, not
access to sensitive job data. Start with this trusted admin boundary; do not
expose it as an application tenant API merely because it accepts filters.

When scoped consumers are introduced, authorize both subscription admission and
every refetch, key shared state by authorization scope, and close/resync streams
when access changes. Reject unauthorized requested resources before `ready`.
Events must not reveal unauthorized job ids, queue names or activity timing.
The stream must have an explicit same-origin/CORS and credential policy rather
than inherit the current router's permissive CORS accidentally. Never put tokens,
args, output, raw filters or secrets into URLs, logs or notification payloads.

Choose SSE because the contract is one-way hints with ordinary HTTP reads and
mutations. Resource-set changes replace the connection, with overlap bounded and
old responses fenced; no bidirectional command protocol is needed. Use an HTTP
streaming client where authenticated headers or a structured request body are
needed; native `EventSource` is optional, not the API boundary. Reuse the exact
JobFilter wire shape inside a versioned bounded request. Keep native URL-based
subscription descriptors to non-sensitive resource identifiers only.

[SSE](https://html.spec.whatwg.org/multipage/server-sent-events.html) supplies
event framing and browser reconnect behavior, not replay storage. Do not emit an
SSE `id` implying resumable history; `Last-Event-ID` cannot suppress the required
resync. Proxy buffering, idle timeouts, authentication expiry, tab suspension and
HTTP connection limits need deployment tests. Send small keepalives, disable
proxy response buffering where required, and fall back to polling if streaming
is unavailable. Non-browser clients can consume the same envelope later.

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
[ADR-045](../045-hot-path-trigger-lock-contract/index.md); no trigger change is implied.

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
[managed Postgres](../../deploying-on-managed-postgres/index.md#logical-replication-and-cdc),
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
  and polling only for filter semantics it supports; otherwise show an unsupported
  filter error. Never silently drop predicates to make fallback work.

## Validation required before implementation ships

This is a design recommendation grounded in the sources below, not a subscription
prototype or benchmark. Implementation must demonstrate:

- A write before registration, during the first read, during refetch and while
  disconnected; dropped/duplicate/coalesced hints; server and listener restart;
  a filter change with a late response; and convergence after an in-flight claim.
- Enter and leave for state/queue changes, retries, DLQ move/retry/purge,
  progress/output, pruning, schedule changes and bulk-operation progress.
  An event wholly between samples is allowed to be invisible; tests must not
  accidentally assert transition delivery.
- Cache-fill/invalidation races, read errors/timeouts, an empty install,
  old-server fallback, a read-only replica with lag, and two admin processes
  handling mutations and reconnects independently.
- Hard admission and memory bounds under slow/disconnected clients, maximum
  distinct filters and event storms, with prompt resync/close and fair refresh
  under continuous traffic. Verify database pool/connection counts and idle
  cleanup, not just heap bounds.
- Unauthorized admission/refetch/revocation, origin/credential handling, and a
  streaming deployment through the supported ingress/pooler topology.
- Job throughput and p95/p99 enqueue/claim/finalization latency, WAL per job,
  admin query concurrency/QPS and browser refresh latency with zero clients,
  polling, SSE, many clients and multiple admin instances. Set numeric budgets
  and overload defaults from those measurements before enabling by default.

Unresolved implementation choices are exact endpoint/request encoding, numeric
admission limits, cadence and stale-state SLOs, proxy/auth integration, and which
candidate resource reads already have adequate semantics. Application-scoped
authorization and historical notice retention require further design before
those consumers are exposed. None changes the invalidation/reconciliation
contract; a requirement for replay or guaranteed completion observation would.

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
- [ADR-049](../049-job-filter-and-indexed-tags/index.md): shared filter, advisory
  in-flight reads, capped counts, tag retention and fleet compatibility.
  [ADR-030](../030-batch-operations/index.md) owns durable operator actions;
  subscriptions only observe their progress.
- [ADR-029](../029-transactional-followup-jobs/index.md) and
  [ADR-046](../046-handler-staged-follow-ups/index.md) own durable follow-up work.
  This proposal neither replaces nor strengthens the guarantees of those paths.
- [ADR-041](../041-rolling-upgrade-policy/index.md) and
  [ADR-045](../045-hot-path-trigger-lock-contract/index.md) remain binding for any future
  schema or hot-path hint changes. The
  [operations skill](https://github.com/hardbyte/awa/blob/f644f321ad110c6436382b64bdc0d935dcdeba5a/skills/awa-operations/SKILL.md)
  documents the trusted admin boundary and read-only operation.
