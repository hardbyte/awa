# Admin dashboard observation

The dashboard observes ordinary owning APIs through polling. One scheduler per
visible tab combines initial reads, periodic polling, independent reconciliation,
mutation invalidations, focus/resume and explicit refresh. There is no streaming
endpoint, database notification listener or worker execution-path change.

Managed views cover jobs and detail, filtered counts, DLQ and depth, queues and
queue runtime, workers/runtime, cron/reconciliation, batch operations, storage and
statistics. Catalog/kinds and search suggestions retain their existing policies.
Backend origin, trusted-admin scope, full query/filter and page identify a key.
Duplicate observers share admission; excess keys fail visibly without eviction.

The experimental client policy permits 16 active exact keys and two concurrent
managed reads. Cached reads respect the advertised interval (at least five
seconds); uncached reads have a two-second floor. Local dirty demand becomes
eligible after 100 ms from the first mark, subject to cooldown and backoff.
Reconciliation is independently armed before the initial automatic read, with
bounded jitter from 27 to 30 seconds; only a successful read renews the successful
observation. These are scheduling bounds, not a promise of maximum data age.
Repeated failures back off to 60 seconds. HTTP 429/503 Retry-After applies across
managed reads, and 401/403 stops retries until explicit user action. HTML responses,
redirects and malformed JSON fail instead of appearing as empty results.

The 35-second compatibility client deadline includes response-body decoding. It runs separately
from the read, aborts cancellation-aware work and fences late results before
React Query can publish them. Expiry is checked again even if the event loop
delayed the timer. Dirty demand remains pending. Client configuration must stay
finite and above the dependency's current 30-second listing/count budget. The
ADR's 20-second client target requires the separate 15-second server layer;
this increment does not advertise that target as implemented.
These interactive observation deadlines/admission limits do not apply to ADR-049
batch-operation execution scans or DLQ bulk retry/purge/move
mutations. Those retain their own operation policies. Polling their list/status
reads remains managed; cancelling a status read does not cancel the operation.
The explicit `apiRead` helper inherits observation cancellation regardless of
HTTP method, including future POST query adapters. Mutation helpers use the
separate transport without that ambient signal. Batch preview is an interactive
read, not an execution scan; its operator-triggered form still needs migration
into shared admission/deadline handling, so this increment makes no complete
preview-budget claim.

Hidden tabs and pagehide cancel reads and timers. BFCache restoration performs
one paced resynchronization. An empty managed scope has no resource timer or
resource reads. A mark during an in-flight read survives its response. Filter/view
changes retire old requests; jobs/DLQ lists may retain visibly marked previous
results while the replacement read completes. Job selection and operator form
state are retained. Selected job lists pause automatic replacement; explicit
refresh uses the same scheduler. Counts become stale on automatic marks, and
fetch only on filter change or explicit refresh.

## Aggregate cache behavior

The stats, queue list, runtime, queue-runtime and storage caches use monotonic
local generations. After a successful local admin mutation, the shared helper
advances generations and invalidates old entries. DLQ retry/purge/move and batch
cancellation use the same helper. Errors/rolled-back operations do not invalidate.
A request after invalidation cannot join an older fill; an obsolete result cannot
be returned as current. At most two generation attempts are made. Four expensive
aggregate fills share a process budget with at most 16 waiters and a single
250 ms admission window, including generation retries. Cache hits bypass fill
admission. Admission excess returns 429 and generation churn returns 503, both
with Retry-After. This aggregate budget is narrower than a whole-server read
budget.

Aggregate responses preserve existing JSON payloads and add:

| Header | Meaning |
| --- | --- |
| `X-Awa-Observed-At` | UTC source-fetch start, conservatively preceding composite queries. |
| `X-Awa-Served-At` | Response creation time. |
| `X-Awa-Cache-Age-Ms` | Monotonic elapsed age since that source-fetch start. |

Cache hits preserve the source time. A response served now is not a new source
sample. Each process owns its caches; worker/peer changes converge through TTL
and scheduled reads. Worker liveness remains heartbeat evidence. Replica lag is
unknown unless separately measured. Storage history remains client accumulation,
not server-retained history. Read-only mode still exposes sensitive admin data;
existing ingress/private-network controls remain necessary.

## Increment boundary and validation

This increment implements scheduler/lifecycle and aggregate-cache portions of the
polling-first ADR-050 contract reviewed at `4649c56932955416d2fee55accec277b32be5c78`.
It is based on filtering PR #572 at
`15b8e3ff66282cac00b744b6d0483ce2eb4f8cea`. It retains that compiler, current
safe-integer ID rejection, GET payloads and `/api/dlq/retry-matching` and
`/api/dlq/purge-matching` mutation protocols.

Strict versioned JSON read adapters/capabilities, origin-policy isolation and API
fallback hardening are separate work. This slice still uses the existing GET
reads and does not establish a strict mixed-server filtered-read guarantee.
The dependency now rejects unknown/empty GET parameters and supplies one
30-second listing/count budget across candidate passes and row loads, with
server statement timeouts and a pool-wait backstop. Batch execution scans retain
their own policy. This is narrower than the ADR's proposed process-wide read
admission, 15-second whole-read deadline including serialization, five-second SQL
and one-second lock limits. The aggregate admission window and client abort do
not prove SQL rollback/cancellation or safe pool reuse; those need independently
tested shared read-layer coverage. Operator-triggered preview admission and
consistent observation metadata beyond the five aggregate endpoints are also
not completed here.
These limitations must be resolved before claiming complete observation-v1
acceptance; client timeouts must never reset a server deadline.

Run `npm run test:observation` in `awa-ui/frontend` for dependency-free scheduler,
HTTP, cancellation and lifecycle tests. Rust cache tests use deterministic barriers
for an old fill finishing after invalidation, bounded churn, waiter cancellation
and monotonic cache age. The full React/build and PostgreSQL 17/18 suites remain
necessary, including real cancellation/pool recovery, rollback/peer convergence,
selection/draft behavior and page refill. Load/ingress/pooler measurements remain
outstanding; no production freshness SLO follows from these defaults.
