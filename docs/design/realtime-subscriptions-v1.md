# Real-time subscriptions v1: implementation contract

Status: proposed companion contract to
[ADR-050](../adr/050-realtime-subscriptions.md). Streaming is **disabled by
default**, enabled only by explicit server opt-in. The settings below are finite
experimental engineering limits, not measured production SLOs. This document
specifies a future implementation; it does not describe a shipped API.

V1 serves the existing trusted admin UI. It uses process-local coarse
invalidation, ordinary read semantics and mandatory reconciliation. No
transactional directory, outbox, CDC integration, tenant authorization framework
or worker change is required. Durable business triggers remain queue jobs.

## Streaming request and validation

Use **`POST /api/subscriptions/v1`**, accepting strict JSON and returning
`text/event-stream`. The tab uses `fetch` and an AbortController. This is one
streaming request/response, not a registration/session service. There is no native
EventSource GET variant in v1. Resource changes abort the old stream before
opening its replacement. Retain displayed data, marked stale, until successful
reconciliation.

```json
{
  "version": 1,
  "resources": [
    {
      "key": "r1",
      "type": "jobs",
      "filter": {
        "queue": "emails",
        "tag": ["tenant:opaque", "order:opaque"]
      }
    },
    {"key": "r2", "type": "job", "job_id": "123"},
    {"key": "r3", "type": "workers"}
  ]
}
```

Validate the complete request before registration or HTTP 200:

- Exact protocol version; unknown fields rejected at envelope, resource and
  filter levels; unique opaque keys and type-specific fields.
- Both `JobFilter` deserialization **and `validate()`**, using the existing
  implementation and bounds. Strict parsing alone does not enforce validation.
- Valid IDs/UUIDs and every byte/count bound. Subscription job IDs are decimal
  strings to avoid another JavaScript integer-precision dependency.

Body/descriptor limits apply while receiving and parsing, before allocation can
grow without bound. Do not echo raw descriptors or filters in events or logs.
After validation the hub may retain only keys and category mappings; it never
evaluates filters. Opening a stream performs **zero database connections and zero
state queries**.

## Initial resources

These types invalidate existing read results; they promise no new metrics or
retained history. The UI owns explicit active-query mappings.

| Type | Additional fields | Existing reads invalidated |
| --- | --- | --- |
| `jobs` | Required `filter: JobFilter` | Jobs pages and dashboard job sample; count becomes stale only. |
| `job` | Required decimal-string `job_id` | One job's current detail, output, progress and source. |
| `dlq` | Required `filter: JobFilter` | DLQ page/detail and depth. |
| `queues` | None | Queue list/detail and `/queues/runtime`. |
| `workers` | None | `/runtime`, derived runtime details, capabilities and coverage. |
| `schedules` | None | `/cron` and `/cron/reconciliation`. |
| `operations` | Optional `operation_id: UUID` | Batch-operation list or one operation. |
| `maintenance` | None | `/storage` and existing sampling behavior. |
| `stats` | None | `/stats` and current timeseries views. |

Exclude standalone `recovery`, retained notices/history, application consumers
and named queue/worker/schedule selectors from v1. They need further read
contracts or add little with coarse routing. `/storage?history=true` supplies no
server-retained history; subscriptions must not imply otherwise. Catalog/kinds
views keep existing read behavior. Queue health, throughput, age and recovery
displays use only existing fields with documented accuracy.

`JobFilter` describes a population; pages remain separate. Invalidation refetches
the active bounded page, removing jobs that left and filling vacancies from
current matches. It does not promise stable multi-page snapshots, an infinite
list or visibility of every transition. Unknown routing information invalidates
the broader category rather than risking a missed leave.

## Events and stream parsing

The only data event types are `ready`, `invalidate` and `resync`:

```text
event: ready
data: {"version":1,"epoch":"opaque-connection-id","keys":["r1","r2","r3"],"refresh_ms":5000,"reconcile_ms":30000,"keepalive_ms":15000}

event: invalidate
data: {"version":1,"epoch":"opaque-connection-id","keys":["r1","r2"]}

event: resync
data: {"version":1,"epoch":"opaque-connection-id","reason":"overflow"}

```

`ready.keys` equals the requested set: never partially accept. Registration
precedes `ready`; a client displaying a preliminary read must refetch after
`ready`. Epoch is connection identity, not a database position. No SSE `id`,
offset, acknowledgement or transition payload is emitted. Keepalives are comments
and do not increment dirty generations.

Check version, envelope, accepted keys and epoch. Unknown versions or malformed
events close the stream and enter safe fallback with a visible protocol error;
invalid protocol does not cause an automatic stream retry loop. Enforce frame
limits even when a peer never finishes a frame. Parse chunked SSE, comments,
split UTF-8 and split line boundaries with finite buffers. A fetch client must
implement reconnect itself; native EventSource reconnect behavior is not automatic.

## Capabilities and strict versioned reads

Add optional structured `subscriptions` and separate `job_filter_protocols`
fields to `/api/capabilities`. Advertise enabled protocols, accepted types,
limits, refresh policy and disabled status. Absence means unsupported; do not
infer support from semver. Capabilities are advisory: the next request can reach
an older server.

Add thin strict JSON adapters:

- `POST /api/v1/jobs/query`
- `POST /api/v1/dlq/query`
- `POST /api/v1/jobs/count`

Query bodies use the existing filter and page semantics:

```json
{"version":1,"filter":{},"page":{}}
```

Count bodies omit `page`. Reject unknown fields, including page fields through
strict wrappers where existing page deserializers are permissive. Reuse the
model/compiler and existing response conversion, not a second evaluator or
storage model. Responses wrap the existing payload without changing its meaning:

```json
{
  "version": 1,
  "data": "<existing response payload>",
  "observation": {
    "observed_at": "<source observation time>",
    "served_at": "<response time>",
    "cache_age_ms": 0
  }
}
```

The strings above mark payload/timestamp placeholders, not literal wire values.
The new UI's filtered subscription/refetch path uses these adapters. Existing
permissive GET endpoints may remain for compatibility. Unknown `/api/*` routes
in the new router return API errors, not SPA HTML.

Unsupported routes, wrong MIME, malformed envelopes and mismatched versions are
errors. Check JSON/version agreement for every read and `text/event-stream` plus
valid `ready` for streams. HTTP 200 alone proves nothing: an older server's SPA
fallback may return login/HTML content. Never retry by dropping predicates.

Without v1 reads, permit legacy polling of **unfiltered** lists/details only.
Nonempty filters remain unsupported rather than rewritten; only a separately
tested legacy subset may later relax this. Do not display an unsupported job-match count
as zero. New-server read budgets apply to SSE-triggered reads and their legacy
fallback paths alike; fallback does not establish new server-side guarantees on
an old binary.

Never place credentials, args, outputs or secrets in any URL. Do not place raw
filter descriptors in subscription URLs or automatic subscription logs. Existing
shareable search URLs may contain documented nonsecret opaque filter values.
JSON adapters avoid filters in new API URLs without removing intentional search
and bookmark URLs.

### Errors and retry policy

Before streaming headers, return structured JSON errors:

| Condition | Status |
| --- | --- |
| Invalid/unsupported version, type or fields | 400 |
| Size limit | 413 |
| Authorization/origin failure | 401/403 |
| Admission limit | 429 with `Retry-After` |
| Temporary overload or disabled transport | 503 |

404/405 or wrong MIME means unsupported transport. After streaming headers, use
resync/close; do not pretend to change HTTP status.

- Network/transport failures retain stale data, use the **same paced read
  scheduler**, and reconnect with exponential backoff/jitter capped at 60 seconds.
- 429/503 honor `Retry-After` across stream attempts **and refetches**. Overload
  must not create immediate additional polling.
- 401/403 stop retries until credentials or user action change.
- Invalid protocol/request stops stream retries and shows the error. Safe read
  fallback still requires supported filter semantics and the same scheduler.
- Reprobe absent transport capability no more than every 60 seconds, unless the
  user explicitly requests it.

## Finite experimental budgets

All limits are configurable with sanity checks. Zero/unlimited must not defeat
the contract. These values are the initial opt-in implementation contract, not
production capacity or freshness promises.

| Bound | Initial setting |
| --- | --- |
| Feature default | Disabled; explicit server opt-in. |
| Resources | 16 descriptors per stream. |
| Resource keys | Unique; 1–64 ASCII alphanumeric, `_` or `-` characters. |
| Request size | 64 KiB total; 32 KiB per descriptor. |
| Streams | 64 per process. |
| Principal quotas | No invented identities in no-auth mode; ingress may enforce quotas. |
| Pending stream state | One dirty bit per admitted key plus one resync flag. |
| Event/frame budget | 4 KiB application pending/frame budget. |
| Debounce | 100 ms target; 500 ms maximum from first dirty event. |
| Keepalive | Comment every 15 seconds. |
| Client liveness | Reconnect after 45 seconds without bytes. |
| Connect/ready deadline | 10 seconds. |
| Slow transport | Abort after a frame is pending for 5 seconds without body-consumer progress. |
| Stream lifetime | 5 minutes, then reconnect/resync. |
| Periodic hints | Existing `poll_interval_ms`, currently at least 5 seconds and cache TTL. |
| Reconciliation | Jittered 30-second watchdog per active resource. |
| Client read concurrency | 2 per tab; one in flight and one pending marker per exact query key. |
| Refresh floor | Cached reads respect advertised TTL/poll interval; uncached detail/pages at least 2 seconds apart. |
| Server expensive reads | 4 simultaneous operations per process. |
| Read admission | At most 16 waiters; 250 ms admission wait; excess gets 429. |
| Whole-read deadline | 15 seconds, including admission, pool acquisition, all queries/passes and serialization. |
| SQL statement limit | At most 5 seconds, also bounded by remaining whole-read deadline. |
| SQL lock-wait limit | At most 1 second, also bounded by remaining deadline. |
| Repeated failure | Exponential backoff to 60 seconds; retain one pending demand. |

Bound request/descriptor parsing before large allocation. Coalesce repeated
invalidation into dirty bits. Overflow collapses to one all-resources resync;
close if that cannot make transport progress within budget. Slow consumers never
block the shared hub, readers, workers or producers. Release stream permits on
body drop, shutdown and failure. Idle time without a pending frame is not
backpressure. Enqueueing into an application channel is not evidence of transport
delivery; local body-consumer progress is not a remote receipt acknowledgement.

The process-wide expensive-read budget covers SSE-triggered and fallback reads.
Cache hits may bypass admission; each cache fill acquires it once. Do not create
a database task or pool connection per subscription. Bound obsolete fills,
filtered cache cardinality and total fanout as well as per-stream state. Share
aggregate reads; any filtered coalescing key includes backend, authorization
scope, full normalized filter and page.

A Rust future timeout alone does not prove SQL cancellation. Apply server-side
deadlines to **all** constituent queries through appropriately configured admin
read connections or deadline-aware transaction helpers. Do not alter mutation
session timeouts accidentally. Verify cancellation rolls back or closes
outstanding work before a connection becomes reusable.

Audit candidate queries, separate loader transactions, all listing passes,
aggregates and DLQ reads. The document-specific review identified a gap in
#572's candidate-query-only timing: its loader opens a separate transaction with
planning settings, and listing may make 32 candidate passes. This contract
requires the entire operation, not each pass independently, to fit the deadline.
The implementation must verify coverage against its actual base revision.

## One frontend refresh scheduler

Use one tab-level scheduler for SSE hints, watchdogs, legacy polling,
focus/resume, reconnect and mutation refreshes. Managed queries disable
independent automatic intervals, focus fetching and reconnect fetching. Migrate
the existing two-second detail/operation polling explicitly. Every cause marks
the same bounded dirty state; no component opens its own stream.

With healthy SSE, periodic hints and reconciliation drive reads. With unhealthy
SSE, polling takes over **through the same scheduler**. Do not run both plus
independent React Query timers. Hints and keepalives alone never postpone the
reconciliation watchdog. Resync obeys admission, cooldown and backoff. A failed
read does not count as successful reconciliation or clear pending demand.

A request captures its query's dirty generation at start. Its response clears
only that captured demand. A hint during the request leaves one pending refresh;
the client may render the successful response while still showing stale/dirty
state. Fence superseded filter/stream epochs and late responses. Continuous hints
must neither starve visible results nor build an unbounded queue.

Respect pauses and explicit actions:

- Jobs currently pause automatic refresh while IDs are selected. Hints mark the
  view stale without replacing the selection. Resume/manual refresh re-enters
  the scheduler; mutations still use explicit selected IDs and server guards.
- Filtered job-match counts are excluded from automatic SSE/watchdog/focus refresh. Mark them stale;
  refresh only on filter change, explicit refresh or the specified debounced
  operator action. Prefix invalidation must not accidentally poll counts.
- Hidden tabs pause streams and reads. Visible resume performs one paced resync.

Convergence means retained state returned by ordinary APIs after changes settle
and reads succeed. It does not strengthen cross-table consistency or guarantee
observation of every transient transition. Composite loading may miss ordinary
deferred-to-ready handoffs as well as claims; Repeatable Read alone does not make
ring `TRUNCATE` MVCC-safe. Preserve reconciliation rather than asserting a perfect
snapshot/stream boundary. Absence after retention is not proof of never existing.

## Cache generations and truthful observations

Use a bounded monotonic generation per aggregate cache group. After a successful
local mutation commits, one post-commit helper:

1. Advances the cache generation.
2. Invalidates or retires the old generation.
3. Signals the hub.

A fill captures its generation before starting. It may publish/return as current
only if that generation matches at completion. Requests after invalidation must
not join an old-generation fill. Use generation-keyed caches with bounded obsolete
work or an explicit synchronized check; never hold a cache lock over a database
query. Discard/retry obsolete completions only within the remaining read budget.
Do not relabel them fresh or loop indefinitely under continuous invalidations.
Existing Moka `invalidate_all()` alone is not an established proof. Require a
deterministic old-fill-after-invalidation test.

Client and server generation fences are both required. The client cannot detect
an obsolete cache fill served as fresh. Read metadata distinguishes:

- `observed_at`: source-fetch start, or a conservative start/end observation
  window for a composite read.
- `served_at`: response time.
- `cache_age_ms`: age of the cached observation.

Preserve original observation time on cache hits. A fresh HTTP response does not
make an old sample newly observed. Use a common wrapper or headers for existing
payloads; the new v1 adapters use the envelope above. Worker liveness remains
heartbeat evidence under its documented window. Replica freshness/lag is
**unknown** unless actually measured; primary hints do not prove replica replay.

V1 may conservatively invalidate all admitted categories and aggregate caches
after any successful local mutation. Include every family, notably DLQ
retry/purge/move and batch cancellation, whose current handlers do not uniformly
invalidate caches. Rolled-back mutations emit no hints. Worker and peer-process
activity is timer-covered, with no added hooks or triggers. Each admin process
owns its hub/cache/timer and reads its backend; no broker or sticky routing is
needed. Measure aggregate read costs across processes.

## Trusted-admin origin and credentials

V1 is the existing trusted admin surface, not an application tenant API.
`--read-only` disables mutations, not access to sensitive data. Ingress/private
network controls protect reads and streams; an origin check does not authorize a
JobFilter or make a public unauthenticated admin safe.

Default new adapters to same-origin browser access with existing ingress auth:

- Require JSON POST bodies. Reject a present Origin unless it matches the
  configured public admin origin. Never derive an allowlist from arbitrary
  forwarded-host headers.
- No wildcard credentialed CORS. Isolate new routes or deliberately replace
  existing permissive CORS so an outer layer cannot undo this policy.
- Browser fetch uses `credentials: "same-origin"` and `cache: "no-store"`.
  Reject cross-origin redirects/login HTML as protocol data.
- Non-browser requests without Origin retain the existing ingress/private-network
  trust requirement.

SSE response headers:

```text
Content-Type: text/event-stream
Cache-Control: private, no-store, no-transform
X-Accel-Buffering: no
```

Use the actual ingress's buffering controls, testing authentication proxy,
compression, idle timeouts and streaming behavior. The five-minute stream
lifetime allows ingress reauthentication; a server without identity cannot
promise immediate per-user revocation. Application-scoped consumers remain
outside v1. Do not put secrets, args or output in notifications or transport logs.

## Implementation and acceptance

Use a separate draft implementation PR, stacked on #572 or after its merge,
reusing its actual `JobFilter` compiler. Do not copy a second compiler into the
UI crate. Adopting this contract permits opt-in implementation, not default
enablement, release or merge.

1. Adopt ADR-050 and this protocol/resource/fallback/privacy/limit/scheduler/cache/
   origin contract. Keep experimental defaults and disabled-by-default status.
2. Add protocol types/capabilities, strict read adapters and status/MIME checks.
   Verify read-only and empty-install behavior without starting a worker.
3. Add the bounded process-local hub/timer and post-commit helper. Prove stream
   opening does zero state queries/connections. Test descriptor/frame bounds,
   permits, disconnect cleanup, shutdown and blocked response bodies.
4. Add cache generations, observations and whole-read budgets. Use deterministic
   barriers for stale-fill races. Prove timed-out SQL stops/rolls back and pool
   connections return to baseline before reuse.
5. Add one tab-level fetch-SSE manager/scheduler with explicit active-query
   mappings, dirty generations, abort/fencing, count/selection exceptions and
   fallback. Test finite parsing across chunks, comments, UTF-8 and line splits.
6. Cover every local mutation family. Prove rollback emits no hint and peer
   changes converge through periodic reads. Test filter entry/exit, page refill,
   retained-not-found presentation, errors, resume and heartbeat expiry without
   writes. Preserve "where applicable" for follow-ups: rescue follow-ups remain
   best effort after rescue.
7. Test old-server 200-HTML/404, mixed-version routing of capabilities/stream/read,
   401/403, 429 `Retry-After`, 503, healthy SSE with failed database reads,
   restarts, continuous hints, overflow/resync, slow consumers and fallback
   without duplicate loops. Counts/paused selections remain unchanged until
   explicitly resumed/refreshed under their rules.
8. Benchmark PostgreSQL 17 and 18 with zero clients, current polling, opted-in SSE,
   maximum admitted views and multiple admin processes. Measure p95/p99 enqueue,
   claim and finalization latency, rescue progress, WAL, query QPS, permits,
   database connections, cache fills and browser freshness. Validate through
   supported ingress/pooler topology.

Keep the implementation PR draft until deterministic correctness and
read-budget/cancellation tests pass. Default enablement requires load/deployment
evidence; missing evidence does not prevent writing an opt-in implementation.
Timer-driven SSE is not inherently cheaper than polling: measure local-mutation
freshness and total read/connection costs before default enablement.

The earlier uniqueness-prune and DLQ downgrade findings remain separate release
gates. This stream neither causes nor repairs them. No transactional-directory
proposal is included. New metrics, retained notices/history and application
authorization need separate work.

## Evidence boundary

This contract adopts the actual-document review of ADR-050 at
`3dd7ee3696e706d80c4a2777d7d0642f1e3480cc`, verified blob
`154f328a7249229ad3e0a25db83827595ea95f21`. The original repository and
PostgreSQL/SSE source evidence remains linked in
[ADR-050](../adr/050-realtime-subscriptions.md#evidence-and-relationship-to-existing-decisions).
Review agreement and documentation CI do not establish a tested subscription
implementation, SQL cancellation, load safety or deployment correctness. The
transferred review evidence paragraph was incomplete and executable model
artifacts were not supplied here; this document claims no verified model result.
