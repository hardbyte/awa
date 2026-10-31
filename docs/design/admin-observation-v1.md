# Admin observation v1: polling, strict reads and bounded reconciliation

Status: proposed supporting contract for
[ADR-050](../adr/050-admin-view-observation.md). V1 uses polling. It includes no
SSE/WebSocket endpoint, browser subscription service, worker hooks, database
notification listener, outbox or CDC integration. Future transport alternatives
and their evidence gate belong in the ADR, not this delivery sequence.

## Owning reads and startup

The owning APIs remain authoritative for jobs, queues, workers, DLQ, schedules,
operator progress, storage and existing statistics. Reuse their current meanings,
accuracy and retention. Do not invent metrics or historical notices. Catalog/kinds
keep their existing read behavior; `/storage?history=true` does not supply
server-retained history. Application consumers and tenant authorization are
outside v1.

For each visible tab, the automatic-read startup sequence below excludes
paused views and manually refreshed counts. Their explicit reads still share
the budgets but do not acquire an automatic watchdog.

1. Establish the backend identity and supported read protocols. Capabilities are
   advisory; verify the actual response to every request.
2. Register active exact query keys with the one tab-level scheduler, mark each
   initially dirty and arm its independent reconciliation timer **before**
   dispatching the first bounded read. A key includes backend, read scope, full
   normalized filter and page. There is no stream registration, `ready` event or
   snapshot cursor to wait for.
3. Read through the shared client concurrency/cooldown policy. Capture the key's
   dirty generation at request start. A response clears only that captured
   demand; a mutation/focus mark arriving during the read leaves one pending
   refresh. Successful data may render while still marked dirty/stale.
4. Fence old filter/backend identities and late responses. A filter or active-view
   change cancels/retires its old key before registering the replacement. Retain
   already displayed data as stale where appropriate rather than claiming a new
   successful observation.

No active keys means no resource reads or resource timers. Enforce the active-key
cap before registration; reject excess demand visibly rather than silently
truncating or creating another scheduler. One pending marker per key prevents
interleaved demand for different resources from overwriting each other.

Polling and reconciliation use one request-dispatch path. The periodic refresh
cadence may renew normal views sooner than the independent watchdog; neither a
local dirty hint nor a failed request postpones the watchdog. Successful reads
renew the observation. The watchdog marks demand through the same scheduler,
never launches a second request loop. Overload/cooldown/backoff still apply, so
these are scheduling bounds rather than a promised maximum data age.

## Filter and protocol boundary

Use the actual ADR-049 `JobFilter` compiler, deserializer **and `validate()`**.
Fields intersect, states are any-of, tags are all-of, and creation bounds refer
to original insertion. Page/window semantics remain separate from the population.
Refetching a page removes jobs that left and fills vacancies from current matches;
there is no stable multi-page snapshot or guarantee of every transition.

Add thin strict JSON read adapters:

- `POST /api/v1/jobs/query`
- `POST /api/v1/dlq/query`
- `POST /api/v1/jobs/count`

These paths are an additive, explicitly versioned **read contract**, alongside
Awa's existing unversioned `/api/*` endpoints. They do not rename or version all
existing APIs. In particular, retain `/api/dlq/retry-matching`,
`/api/dlq/purge-matching` and legacy mutation routes; cosmetic consistency does
not justify another mutation protocol change.

A version in the URI or JSON is not proof of strict behavior. Enforce exact
version agreement and reject unknown envelope/filter/page fields, using strict
page wrappers where needed. Reuse existing response conversion and model logic;
do not create a UI-side compiler. Query request:

```json
{"version":1,"filter":{},"page":{}}
```

Count requests omit `page`. Apply shared filter bounds and a 64 KiB request-body
limit while receiving/parsing, before allocation can grow without bound. Existing
`JobFilter` and payload ID representations remain unchanged; the current browser
rejects IDs outside JavaScript's safe-integer range rather than rounding them.
Do not claim a new arbitrary-precision or decimal-string ID protocol here.

Response envelope:

```json
{
  "version": 1,
  "data": "<existing response payload>",
  "observation": {
    "observed_at": "<source-fetch start time>",
    "served_at": "<response time>",
    "cache_age_ms": 0
  }
}
```

The example strings stand for real payloads/timestamps. Add optional structured
`job_filter_protocols` capability metadata; absent support is unsupported, never
inferred from semver. Do not advertise a `subscriptions` transport capability.
Capabilities can come from a different server version than the next read, so
check status, JSON MIME, complete envelope and exact response version every time.
Unknown `/api/*` paths on the new router return API errors, not SPA HTML.

An older server may return 200 HTML or 404/405 for the route. That is unsupported,
not an empty result. Permit unfiltered legacy lists/details through the same
scheduler; nonempty filters remain unsupported. Never retry by dropping
predicates, switching a filtered request to a permissive GET, or displaying an
unsupported count as zero. New-server budgets cover permitted legacy fallbacks
too; they do not establish new server-side guarantees on old binaries.

When the strict adapters ship, their implementation must include upgrade notes
and release notes for this availability tradeoff: a new UI routed to an older
replica shows "filter unsupported" instead of filtered results. During a rolling
deploy, filtered views can therefore alternate between success and unsupported
as routing changes. Document that behavior and the deployment/routing mitigation;
never hide it by falling back to a wider filtered GET. These adapters are proposed,
not claimed to have shipped here.

Fixing unknown/empty GET parsing on a current server is a separate improvement
from this strict mixed-version read boundary. Neither strict JSON parsing in one
path nor a `/v1` name makes an older GET implementation strict.

## Errors, deadlines and shared read budgets

The admission, client/whole-read, statement and lock-wait budgets below apply to
**interactive admin reads**: lists, details, previews, counts and status/aggregate
reads. Batch-operation execution scans and DLQ bulk mutations keep their own
policy and safeguards; this contract neither subjects them to the interactive
budgets nor exempts them from all limits. Do not apply them globally to compiler
paths or pooled sessions also used for execution scans or mutations. This preserves
ADR-049's scan policy rather than making an interactive timeout fail a whole
operation.

Use structured JSON API errors: invalid versions/fields/values return 400,
body-size excess 413, authorization/origin failure 401/403, admission excess
429 with `Retry-After`, and temporary overload 503. Unsupported routes, wrong
MIME and malformed envelopes are protocol errors, even with HTTP 200.

Network/read failures retain stale data and one pending demand, with exponential
backoff/jitter capped at 60 seconds. Honor 429/503 `Retry-After` across reads to
the affected backend; do not turn overload into an immediate extra poll. Stop
401/403 retries until credentials/user action change. Invalid filters/protocols
show an error instead of an automatic retry loop. Reprobe absent read capability
no more often than every 60 seconds unless explicitly requested.

The following are finite experimental read/scheduler defaults, not production
capacity commitments or freshness SLOs. Configure them with sanity checks; zero
or unlimited settings must not defeat the bounds.

| Interactive-read bound | Initial setting |
| --- | --- |
| Active managed query keys | 16 per tab; no silent eviction. |
| Client read concurrency | 2 per tab; one in flight and one pending marker per exact key. |
| Client request deadline | 20 seconds from dispatch through response-body decoding and protocol validation; finite and configured greater than the server whole-read deadline. |
| Debounced dirty demand | 100 ms target, 500 ms maximum before becoming eligible; cooldown/backoff still applies. |
| Cached refresh floor | At least advertised cache TTL/poll interval, currently at least 5 seconds. |
| Uncached detail/page floor | At least 2 seconds between reads. |
| Independent reconciliation | Nominal 30-second watchdog per active key, finite bounded jitter; expose the configured maximum. |
| Server expensive interactive reads | 4 simultaneous operations per process. |
| Interactive-read admission | At most 16 waiters; at most 250 ms waiting; excess receives 429. |
| Interactive whole-read deadline | 15 seconds including admission, pool acquisition, all queries/passes and serialization. |
| Interactive SQL statement limit | At most 5 seconds, also bounded by remaining whole-read deadline. |
| Interactive SQL lock wait | At most 1 second, also bounded by remaining deadline. |
| Repeated failure | Exponential backoff to 60 seconds; one pending demand retained. |

During incidents, uncached filtered job pages are expected to degrade first as
operators multiply demand: reads may receive 429 and the UI retains stale data.
Cached aggregate views bypass expensive-read admission only on a cache hit.
Their misses/refills compete for the same permits and waiters and can also be
rejected; aggregate views have no blanket immunity from overload.

One frontend scheduler handles periodic reads, reconciliation, focus/resume,
mutation refreshes, retry and manual refresh. Managed React Query queries disable
independent automatic intervals, focus fetches and reconnect fetches. Migrate
existing two-second job-detail/operation intervals into this scheduler explicitly.
Repeated demand coalesces; continuous changes must neither starve results nor
create an unbounded queue. The client deadline runs independently of pending
network/body reads so a silent hung request is still aborted.

The client deadline is one elapsed-time budget, not reset by headers, body bytes
or retries within that attempt. On expiry, abort the request and fence its attempt
identity before accepting any late completion. It cannot publish into the client
cache/UI, clear dirty demand or renew successful-observation time after expiry;
retain one pending demand under the normal backoff. Recheck expiry before
publishing decoded data even if cancellation was ignored. The 20-second client
default leaves transport margin over the 15-second server budget, not a network
latency guarantee. Configuration must preserve a finite client budget greater
than the server budget.

The server independently enforces its 15-second interactive whole-read deadline
from entry to read admission (including waiting) through serialization. Client
waiting/cancellation cannot extend it; candidate passes, loader queries and
retries cannot reset it. Server cache-fill
generation fences still apply: neither timeout nor late completion permits an
obsolete fill to become current.

A Rust future timeout does not prove SQL cancellation. For interactive reads,
propagate server-side limits through all candidate queries, loader transactions,
repeated passes, aggregates and DLQ inspection reads, bounded by the remaining
interactive whole-read budget. Cancellation must roll back or close outstanding
work before pool reuse. Do not leak read timeout settings into mutation sessions. Cache hits may bypass expensive-read admission;
each fill acquires it once. Bound obsolete fills, filter cache cardinality and
pool/connection use. Do not create a database task per registered tab query.

## Cache fencing and source observations

Use one local post-commit invalidation helper across admin mutation families,
including DLQ retry/purge/move and batch cancellation. After the authoritative
mutation commits, advance the cache generation, retire its old cache entries,
and mark affected server read state stale. Rolled-back mutations invalidate
nothing. The initiating browser marks its own affected keys dirty after a
successful action; other browsers/processes receive no v1 push and refresh on
schedule. Broader local invalidation is acceptable until measured routing is
worthwhile.

A fill captures a bounded monotonic generation before starting. It may
publish/return as current only if the generation still matches at completion.
Requests after invalidation cannot join an obsolete fill. Use generation-keyed
caches with bounded obsolete work or an explicit synchronized check, without
holding a cache lock across a query. Discard/retry obsolete completions only
within remaining read budget; never loop indefinitely or relabel old data fresh.
Moka `invalidate_all()` alone is not proof of this invariant.

Client dirty generations and server cache generations solve different races;
both are required. `observed_at` is source-fetch start (a conservative start for
composite reads), `served_at` is response time and `cache_age_ms` is cached
observation age. Preserve source time on cache hits. Existing reads can carry
common metadata/headers without changing payload meaning; strict adapters use
the envelope above. A response served now is not necessarily a new observation.

Each admin process owns its caches. Worker and peer mutations are covered by TTL
and periodic authoritative reads; a local generation does not create a global
snapshot. Heartbeat evidence determines worker liveness, never browser activity.
Replica freshness/lag is unknown unless actually measured.

## UI lifecycle, domain guards and access

Hidden tabs pause managed reads and timers. `pagehide`, including BFCache entry,
cancels them; persisted `pageshow` retires old request identities and starts
exactly one scheduler with one paced refresh if visible and nonempty. Cancelled
reads do not clear pending demand. Filter/view churn releases registrations and
obsolete work without losing unrelated keys' demand.

Preserve selected IDs and unsaved operator form/dialog values during refresh.
Jobs with an active selection remain paused and stale until explicitly resumed;
manual refresh re-enters the same scheduler. Filtered job-match counts are
excluded from periodic/watchdog/focus refresh and prefix invalidation: mark them
stale and fetch only on filter change, explicit refresh or the specified debounced
operator action. Counts remain capped under ADR-049. Selection/draft state never
replaces server-side eligibility, version/lease/fencing and idempotency guards.
Viewing/presence does not confer ownership; no viewer membership protocol is added.

This is the existing trusted-admin surface, protected by ingress/private network
controls. `--read-only` disables mutations, not sensitive reads. Authenticate and
authorize each read under that boundary; do not create longer-lived access via
client cache state. There is no new tenant authorization model.

The inspected router currently applies permissive CORS and a SPA fallback. Those
are current observations, not verification of configured origins in a deployment.
New strict JSON POST adapters must explicitly reject a present Origin unless it
matches a configured public admin origin; do not derive trust from arbitrary
forwarded-host headers. Isolate their route policy so outer permissive CORS
cannot undo it. No wildcard credentialed CORS. Browser fetch uses
`credentials: "same-origin"` and `cache: "no-store"`; reject cross-origin redirects
and login HTML as protocol data. Origin-less non-browser requests still require
ingress/private-network trust. Any later credentialed streaming proposal must
resolve/tighten this boundary before exposure, not inherit permissive CORS.

Never put credentials, args, outputs or secrets in URLs or observation logs.
Strict reads carry filters in bodies. Existing shareable search URLs may contain
documented nonsecret opaque values; no raw filter descriptors belong in automatic
logs. An origin check does not authorize a filter or make public unauthenticated
admin access safe.

## Incremental delivery and acceptance

Each increment is independently useful with polling:

1. Harden interactive owning reads: strict adapters/capability and response checks,
   API error fallback, bounded admission, whole-read deadlines and cancellation
   coverage. Verify that batch execution scans and DLQ bulk mutations retain
   their own policies. Ship the adapters' rolling-upgrade availability notes.
   Reuse #572's compiler; GET parsing/deadline repairs are shared follow-up work,
   not a transport dependency. Test empty install/read-only operation without a
   worker. Do not rename mutation endpoints for URI consistency.
2. Consolidate the tab scheduler and lifecycle. Register dirty demand before
   initial reads, disable competing automatic fetch loops, enforce per-key
   generations/concurrency/cooldowns/backoff and preserve paused selections/counts.
3. Add generation-fenced caches, conservative source metadata and complete local
   post-commit invalidation. Deterministic barriers must reproduce an old fill
   completing after invalidation and prove it cannot become current.
4. Complete independent reconciliation and failure recovery through that same
   scheduler. Prove convergence after lost local marks and peer/worker changes,
   request failure, scope changes, hidden/resume and BFCache restoration. No
   listener or transport handshake is a prerequisite for any read.
5. Measure the improved-polling baseline on PostgreSQL 17 and 18, with zero,
   ordinary and maximum admitted views and multiple admin processes. Check job
   enqueue/claim/finalization p95/p99, rescue progress, WAL, query QPS, pool counts,
   cache fills and browser read/render freshness under supported ingress/poolers.

Deterministic acceptance covers: initial/empty/over-cap registration; a dirty
mark during the first or later read; interleaved resource demand; old responses
following filter/backend changes; cache races; SQL timeout rollback and pool
recovery; slow/hung/aborted reads; continuous demand; errors/backoff/Retry-After;
client expiry with an ignored abort/late response and an invalidated late cache
fill, proving neither is published as current and the server budget is not reset;
old-server HTML/404/405 and capability/read requests routed to different versions;
401/403; counts, paused selections and drafts; local rollback; peer mutations;
heartbeat expiry without writes; page refill and retained-not-found presentation.
Tests must not demand every transient transition or a stronger snapshot than
the owning API provides.

Acceptance must distinguish interactive reads from execution/mutation paths:
assert the interactive admission/whole-read/statement limits on the former
without imposing them on batch scans or DLQ bulk mutations. Test uncached
filtered-page overload, aggregate cache hits versus fills, and fail-closed filtered
views when a new UI reaches an older replica; include the required upgrade notes
with the adapters' release.

Keep correctness/read-budget work separate from claims of production freshness.
Distinguish source/pipeline lag where measured, transport/network activity, API
latency and browser render age. Missing lag remains unknown, consumer-stage
metrics cannot establish universal subsecond freshness, and the last histogram
bucket does not identify an exact delay. Read-budget defaults are not freshness
SLOs. Any later transport proposer supplies its numeric criteria and evaluation
timeframe through the ADR's separate evidence/owner decision.

## Dependencies and source evidence

Source inspection on 2026-10-31 used #572 head
`c09b23ebed0d65614025271611c720ea584a55c9`, which includes change
`ac1e39440d89e6c0c5a5bb86a7d3af5d8db47d86`. These observations
are evidence of current gaps/fixes, not a claim that this proposed contract ships.

- [Candidate listing](https://github.com/hardbyte/awa/blob/c09b23ebed0d65614025271611c720ea584a55c9/awa-model/src/job_query.rs)
  loops up to 32 passes with a 30-second statement timeout per candidate query.
  [The loader](https://github.com/hardbyte/awa/blob/c09b23ebed0d65614025271611c720ea584a55c9/awa-model/src/queue_storage.rs)
  starts a separate transaction using planning settings without installing that
  timeout. This does not establish a whole-read deadline. Fixes belong in the
  shared read layer/#572 follow-ups; verify the actual implementation base.
  The batch-scan helpers deliberately pass no interactive timeout because a
  timed-out scan fails the operation; those paths retain their separate policy.
- [GET parsing](https://github.com/hardbyte/awa/blob/c09b23ebed0d65614025271611c720ea584a55c9/awa-ui/src/handlers/filter_params.rs)
  deliberately ignores unknown and empty values, including a test of that
  behavior. [Finding-jobs documentation](https://github.com/hardbyte/awa/blob/c09b23ebed0d65614025271611c720ea584a55c9/docs/finding-jobs.md)
  gives a general unknown-field rejection promise; it must not be interpreted as
  current GET strictness. Parser/documentation repair and strict mixed-version
  adapters are distinct work, both useful without this ADR or any streaming.
- [DLQ handlers](https://github.com/hardbyte/awa/blob/c09b23ebed0d65614025271611c720ea584a55c9/awa-ui/src/handlers/dlq.rs),
  [router](https://github.com/hardbyte/awa/blob/c09b23ebed0d65614025271611c720ea584a55c9/awa-ui/src/lib.rs)
  and [API test](https://github.com/hardbyte/awa/blob/c09b23ebed0d65614025271611c720ea584a55c9/awa-ui/tests/admin_api_test.rs)
  use separate retry/purge-matching paths for the full filter, legacy bodies on
  legacy paths and refusal of `all` with a nonempty filter. The obsolete richer-
  body-on-legacy-route downgrade concern is not retained as an unresolved v1 gate.
  The PR reports a real 0.6.9 rejection check; source inspection here is not a
  fresh execution of that rehearsal.
- [Search validation](https://github.com/hardbyte/awa/blob/c09b23ebed0d65614025271611c720ea584a55c9/awa-ui/frontend/src/components/SearchBar.tsx)
  accepts positive whole IDs only when `Number.isSafeInteger` succeeds;
  [the batch form](https://github.com/hardbyte/awa/blob/c09b23ebed0d65614025271611c720ea584a55c9/awa-ui/frontend/src/routes/batch-ops.tsx)
  uses that validator. This rejects unsupported IDs; it is not full-precision
  browser ID support.
- [Router](https://github.com/hardbyte/awa/blob/c09b23ebed0d65614025271611c720ea584a55c9/awa-ui/src/lib.rs)
  currently applies `CorsLayer::permissive()` and the SPA fallback. Access-policy
  claims above are implementation requirements, not assertions about a deployed
  origin configuration.

Other storage/uniqueness release concerns were not revalidated here; this
contract neither clears nor replaces their existing release gates.
