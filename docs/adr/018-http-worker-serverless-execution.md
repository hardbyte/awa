# ADR-018: HTTP Worker for Serverless Job Execution

## Status

Proposed

## Context

Awa is architected around long-running worker processes that poll Postgres, maintain heartbeats, and execute jobs locally. This works well for traditional deployments (containers, Kubernetes), but excludes teams that want to execute job logic in serverless functions (AWS Lambda, Google Cloud Run, Azure Functions, Cloudflare Workers, etc.).

Common motivations for serverless execution:

- **Scale-to-zero:** No idle worker costs when the queue is empty.
- **Per-job isolation:** Each invocation runs in a fresh environment — no shared memory, no resource contention between job kinds.
- **Language freedom:** Job logic can be written in any language the serverless platform supports, not just Rust or Python.
- **Operational simplicity:** Teams already running serverless infrastructure can reuse their deployment pipelines, monitoring, and IAM policies.

Awa already has a `waiting_external` state and callback infrastructure (ADR not numbered; implemented in `awa-model::admin` and `awa-worker::context`). This machinery — callback registration, timeout rescue, CEL-based resolution — provides most of what a serverless execution model needs. The missing piece is a built-in worker that bridges the gap: claim a job, POST it to an HTTP endpoint, and park until the function calls back. However, several correctness gaps in the existing callback path must be addressed first (see **Prerequisite: callback hardening** below).

## Decision

### Introduce `HttpWorker`, a new `Worker` implementation

`HttpWorker` implements the existing `Worker` trait in `awa-worker`. It requires no schema changes, no new job states, and no modifications to the dispatcher or executor. It is registered like any other worker, keyed by job `kind`.

#### Async (fire-and-forget) mode

This is the primary mode. The worker:

1. Claims the job (normal dispatcher path).
2. Registers a callback via `ctx.register_callback(timeout)`.
3. POSTs the job payload to a configured HTTP endpoint, including the `callback_id`.
4. Returns `JobResult::WaitForCallback(guard)`, parking the job in `waiting_external`.
5. The serverless function does its work and POSTs back to Awa's callback endpoint.

**Dispatch failure handling:** If step 3 fails, the behavior depends on whether the failure is *definitive* or *ambiguous*:

- **Definitive failure** (connection refused, DNS resolution error, non-2xx status code): The function platform rejected or never received the request. HttpWorker calls `cancel_callback(pool, job_id, run_lease)` to NULL out callback fields, does not return `WaitForCallback`, and returns `JobError::Retryable` for immediate retry.
- **Ambiguous failure** (HTTP timeout, connection reset mid-response): The function may have received the request and started executing. HttpWorker **must not** call `cancel_callback` — doing so would orphan an in-flight function, causing its eventual callback to be rejected and the job to be duplicated on retry. Instead, HttpWorker returns `WaitForCallback(guard)` normally and lets the callback timeout handle recovery. If the function is running, its callback will arrive and complete the job. If it is not, the callback timeout will rescue the job to `retryable`.

This distinction is critical for at-most-once processing. Aggressively cancelling on ambiguous timeouts trades faster retry recovery for duplicate execution — the wrong default for side-effecting jobs.

The `cancel_callback(pool, job_id, run_lease)` function NULLs out `callback_id`, `callback_timeout_at`, and all CEL fields — but only if `callback_id IS NOT NULL AND state = 'running' AND run_lease = $X` (preventing races with concurrent callback resolution or rescue). `cancel_callback` failure is **best-effort**: if the Postgres call fails, HttpWorker logs a warning and proceeds to return `Retryable`. This is safe because the `run_lease` guard (safeguard 2 below) is the true correctness backstop — a stale callback from this dispatch will be rejected on the next attempt regardless of whether cancel_callback succeeded.

**Stale callback prevention.** Even with correct dispatch-failure handling, two complementary safeguards are required:

1. **`cancel_callback` on definitive failure** (described above).
2. **Callback resolution binds to `run_lease`.** The `complete_external`, `fail_external`, `retry_external`, and `resolve_callback` functions currently match by `callback_id` alone (`WHERE callback_id = $1`). This is insufficient when callbacks can become orphaned. These queries must additionally verify the `run_lease` that was active when the callback was registered. This can be done by: (a) adding `run_lease` to the `register_callback` write and including it in the resolution WHERE clause, or (b) generating a new `callback_id` that embeds the lease (e.g., as an HMAC component) so stale IDs are cryptographically invalidated. Option (a) is simpler and recommended.

Note: `callback_id` uniqueness (fresh UUID per `register_callback`) is the primary defense against stale callbacks — a callback from attempt N carries a UUID that will not match after attempt N+1 registers a new UUID. The `run_lease` guard is defense-in-depth for the edge case where the worker process crashes between a failed POST and `cancel_callback`, leaving the old `callback_id` in the database.

For option (a), the `run_lease` must flow end-to-end through the HTTP contract. The request payload sent to the serverless function includes `run_lease`, and the function must echo it back in the callback request body (or it is embedded in a signed callback token — see **Callback contract** below). Without this, the `run_lease` guard cannot be enforced at the API boundary.

Safeguard 2 is a correctness hardening that benefits all callback users, not just `HttpWorker`. It should be implemented as a prerequisite change before `HttpWorker` ships.

#### Prerequisite: callback hardening

The existing callback path has two correctness gaps that must be fixed before `HttpWorker` ships:

**Race condition in `resolve_callback`.** The CEL-based `resolve_callback` function (`admin.rs:1293`) only matches jobs in `waiting_external` state (`WHERE callback_id = $1 AND state = 'waiting_external'`). In contrast, the direct `complete_external` and `fail_external` APIs accept `state IN ('waiting_external', 'running')` to handle the race where a fast callback arrives before the executor transitions `running` → `waiting_external` (at `executor.rs:536`). Because the executor only flips to `waiting_external` after the handler returns `WaitForCallback`, a fast serverless function can legitimately POST its callback while the job is still in `running`. If `callback_config` is set (routing through `resolve_callback`), this callback will be silently rejected with `CallbackNotFound`.

**Fix:** `resolve_callback` must accept `state IN ('waiting_external', 'running')`, matching the behavior of `complete_external` and `fail_external`. Without this, `callback_config` (CEL expressions) is not safe for the primary async flow and cannot be advertised as a working option for HttpWorker. The TLA+ spec (`correctness/AwaCbk.tla`) must also be updated: line 156 (`jobState = "waiting_external"`) should become `jobState \in {"waiting_external", "running"}`, and `LockHolderConsistent` (line 402) must be updated accordingly. TLC must re-verify `AtMostOnceResolution` holds with this change.

**`retry_external` state handling.** Unlike `complete_external`/`fail_external`, `retry_external` deliberately restricts to `waiting_external` only (`admin.rs:1110`). This is intentionally more conservative: retrying from `running` would risk concurrent dispatch if the original handler hasn't finished yet. This asymmetry is correct and should be preserved — but the implication is that a serverless function calling `/retry` during the racing window will get an error. The callback contract documentation should note this.

```
                  Awa dispatcher                         Serverless function
                  ─────────────                         ───────────────────
   available ──► claim job
                  │
                  ├─ register_callback(timeout)
                  │
                  ├─ POST /invoke ────────────────────► function starts
                  │   { job_id, kind, args,                │
                  │     callback_id, callback_url }        │
                  │                                        │  (does work)
                  ├─ return WaitForCallback                │
                  │                                        │
   waiting_external ◄──────────────────────────────────────┤
                  │                                        │
                  │   POST /callbacks/{id}/complete ◄──────┘
                  │   { "result": ... }
                  │
   completed ◄────┘

   --- Definitive failure (connection refused, DNS error, non-2xx) ---

   available ──► claim job
                  │
                  ├─ register_callback(timeout)
                  │
                  ├─ POST /invoke ─── ✗ connection refused / 5xx
                  │
                  ├─ cancel_callback(job_id, run_lease)  [best-effort]
                  │
                  ├─ return Retryable error
                  │
   retryable ◄──-─┘  (normal backoff, re-dispatched later)

   --- Ambiguous failure (timeout, connection reset) ---

   available ──► claim job
                  │
                  ├─ register_callback(timeout)
                  │
                  ├─ POST /invoke ─── ? timeout / reset
                  │                    (function may be running)
                  ├─ return WaitForCallback
                  │
   waiting_external ◄─── callback_timeout_at governs rescue
                  │
                  ├── function calls back → completed (happy path)
                  └── timeout expires → retryable (rescue path)
```

After the prerequisite hardening above, this reuses the existing callback infrastructure:

- **Timeout rescue:** Maintenance service rescues jobs where `callback_timeout_at < now()`, transitioning to `retryable` (with attempts remaining) or `failed`.
- **CEL expressions:** `register_callback_with_config` supports `filter`, `on_complete`, `on_fail`, and `transform` expressions evaluated against the function's callback payload. Requires `resolve_callback` race-condition fix (see above).
- **Retry semantics:** If the function calls `fail_external` or the callback times out, normal retry logic applies (exponential backoff, `max_attempts`).
- **Heartbeat/deadline:** Cleared when entering `waiting_external` — no false rescues while the function runs.
- **Idempotency and execution semantics:** HttpWorker provides **at-least-once** execution semantics, not exactly-once. Duplicate execution can occur when: (a) Lambda retries an invocation (same `job_id` + `run_lease` — deduplicable), or (b) Awa re-dispatches after an ambiguous timeout where the function was actually running (different `run_lease` — NOT deduplicable by `run_lease` alone). For Lambda retries, `job_id` + `run_lease` is a correct idempotency key. For side-effecting jobs (sending emails, charging payments), functions **must** implement application-level idempotency keyed on business data in `args` (e.g., an order ID), not `run_lease`, because Awa re-dispatch assigns a new `run_lease`.

#### Sync (request-response) mode

An optional secondary mode for short-lived functions (<30s). The worker:

1. Claims the job.
2. POSTs the job payload and **waits for the HTTP response**.
3. Maps the response to a `JobResult`:
   - `2xx` → `Completed`
   - `422` → `Cancel(body)`
   - `429` → `RetryAfter(Retry-After header or default)`
   - `5xx` → `Retryable` error
   - Timeout → `Retryable` error

No callback registration, no `waiting_external` state. The job completes in a single dispatcher cycle. Heartbeats remain active during the HTTP call since the job stays in `running` state. **Important:** `request_timeout` must be shorter than the heartbeat rescue threshold — otherwise a slow HTTP response triggers a spurious rescue while the sync call is still in flight, leading to duplicate execution on re-dispatch.

#### Dispatch backpressure

If many jobs become available simultaneously, the dispatcher will claim them and HttpWorker will fire concurrent HTTP POSTs. Without a limit, this can overwhelm the target function platform (hitting Lambda reserved concurrency, Cloud Run max instances, or Cloudflare CPU limits) and trigger cascading 429s or timeouts.

`HttpWorkerConfig` includes an optional `max_concurrent_dispatches` setting. When set, a per-kind semaphore limits how many in-flight HTTP POSTs the worker will maintain. Jobs claimed beyond this limit wait for a permit before dispatching. This is the correct place to apply backpressure — at the dispatch boundary — rather than adding an external message broker (SQS, Cloud Tasks, NATS), which would break Awa's transactional-enqueue-from-Postgres guarantee and duplicate job state across two systems.

**Dispatcher interaction:** The semaphore blocks inside `perform()`, which means a dispatcher slot is held while waiting for a permit. If `max_concurrent_dispatches` < the queue's `max_workers`, some dispatcher slots will be idle-blocked. For queues dedicated to a single HTTP worker kind, set `max_workers` equal to `max_concurrent_dispatches` to avoid waste. For mixed queues (local + HTTP workers for different kinds), consider placing HTTP worker kinds in dedicated queues.

### Configuration

HTTP workers are configured on `ClientBuilder`, parallel to local workers. The builder requires a `callback_base_url` — the publicly-reachable URL where Awa's callback endpoints are exposed. HttpWorker constructs `callback_url` in the dispatch payload by combining this base with `/api/callbacks/{callback_id}/complete`. This is a deployment-time configuration that must match the Awa process's actual ingress (e.g., a Kubernetes Ingress, Fly.io hostname, or VPS public IP).

```rust
let client = Client::builder()
    .queue("email", QueueConfig::default())
    // Required for HTTP workers — the public URL where callbacks are received
    .callback_base_url("https://awa.example.com".parse()?)
    // Local worker — unchanged
    .register::<ResizeImage, _, _>(resize_handler)
    // HTTP worker — new
    .http_worker(HttpWorkerConfig {
        kind: "send_email",
        endpoint: "https://abc123.lambda-url.us-east-1.on.aws/".parse()?,
        mode: HttpExecutionMode::Async {
            callback_timeout: Duration::from_secs(300),
            callback_config: None, // Optional CEL expressions
        },
        // Optional
        headers: vec![("Authorization", "Bearer ${AWA_LAMBDA_TOKEN}")],
        request_timeout: Duration::from_secs(10), // POST timeout, not job timeout
        max_concurrent_dispatches: Some(50), // Optional backpressure limit
    })
    .build(&pool)
    .await?;
```

Python equivalent:

```python
client = awa.AsyncClient(pool, callback_base_url="https://awa.example.com")

# Async mode — fire-and-forget with callback
client.http_worker_async(
    kind="send_email",
    queue="email",
    endpoint="https://abc123.lambda-url.us-east-1.on.aws/",
    callback_timeout=300,  # seconds; also accepts timedelta
    headers={"Authorization": f"Bearer {os.environ['AWA_LAMBDA_TOKEN']}"},
    max_concurrent_dispatches=50,  # Optional backpressure limit
)

# Sync mode — request-response
client.http_worker_sync(
    kind="resize_image",
    queue="media",
    endpoint="https://job-runner.myaccount.workers.dev/execute",
    timeout=25,
    headers={"X-Awa-Secret": os.environ["AWA_WORKER_SECRET"]},
)
```

#### Request payload

The POST body sent to the serverless function:

```json
{
  "job_id": 42,
  "kind": "send_email",
  "queue": "email",
  "args": { "to": "user@example.com", "template": "welcome" },
  "attempt": 1,
  "max_attempts": 25,
  "run_lease": 1742947200,
  "callback_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "callback_url": "https://awa.example.com/api/callbacks/f47ac10b-58cc-4372-a567-0e02b2c3d479/complete"
}
```

`callback_id`, `callback_url`, and `run_lease` are omitted in sync mode. `run_lease` is included so the function can echo it back in callbacks, enabling the stale-callback guard (see **Dispatch failure handling**).

#### Callback contract

The serverless function completes the job by POSTing to one of:

```
POST {callback_url}                          → complete (shorthand)
POST /api/callbacks/{callback_id}/complete   → complete with optional payload
POST /api/callbacks/{callback_id}/fail       → fail with error message
POST /api/callbacks/{callback_id}/retry      → retry (back to available)
```

Each callback request body must include `run_lease` (echoed from the dispatch payload) so that Awa can reject stale callbacks from previous attempts:

```json
{
  "run_lease": 1742947200,
  "result": { "email_id": "msg_abc123" }
}
```

The underlying model functions (`complete_external`, `fail_external`, `retry_external`, `resolve_callback`) already exist in `awa-model::admin` but need the `run_lease` guard added (see **Prerequisite: callback hardening**). They must be exposed as HTTP routes — either by extending `awa-ui`'s API router or as a standalone lightweight callback receiver (see **Callback endpoint exposure** below).

**Note on `/retry` during the racing window:** Because `retry_external` deliberately restricts to `waiting_external` state only (retrying from `running` would risk concurrent dispatch), a callback to `/retry` that arrives before the `running` → `waiting_external` transition will return `409 Conflict`. The function should treat this as transient and retry the callback after a short delay, or use `/fail` instead and let Awa's built-in retry logic handle re-dispatch.

**Warning: `/retry` resets attempts to 0.** A function calling `/retry` gives the job a full fresh set of attempts. This can lead to infinite retry loops if a function always calls `/retry` instead of `/fail`. For most use cases, `/fail` is the safer choice — Awa's built-in retry logic (exponential backoff, `max_attempts`) handles re-dispatch correctly.

### Implementation scope

The implementation should be split into three PRs to isolate correctness-critical changes and reduce blast radius:

**PR 1: Callback hardening (prerequisite, independently valuable)**

| Component | Change | Size |
|---|---|---|
| `awa-model/src/admin.rs` | Harden `resolve_callback` to accept `state IN ('waiting_external', 'running')`; add `run_lease` guard to `complete_external`, `fail_external`, `retry_external`, and `resolve_callback` WHERE clauses; store `run_lease` in `register_callback` / `register_callback_with_config`; add `cancel_callback` function | ~70 lines |
| `correctness/AwaCbk.tla` | Update `resolve_callback` precondition to accept `running` state; update `LockHolderConsistent`; re-verify `AtMostOnceResolution` with TLC | ~10 lines |
| Tests | Regression tests for `resolve_callback` race condition, stale callback resolution, and `cancel_callback` | ~100 lines |

**PR 2: Callback HTTP routes**

| Component | Change | Size |
|---|---|---|
| `awa-ui/src/lib.rs` | New callback routes: `/api/callbacks/{id}/complete`, `/api/callbacks/{id}/fail`, `/api/callbacks/{id}/retry`, `/api/callbacks/{id}/resolve`. Auth via HMAC-signed callback IDs or shared secret header. Return `409 Conflict` for callbacks rejected due to state transition race (e.g., `/retry` during `running`), `404` for unknown callback IDs | ~120 lines |
| Tests | HTTP-level integration tests for callback endpoints | ~80 lines |

**PR 3: HttpWorker**

| Component | Change | Size |
|---|---|---|
| `awa-worker/src/http_worker.rs` | New file: `HttpWorker` struct, `Worker` impl, config types, dispatch failure classification (definitive vs ambiguous), `max_concurrent_dispatches` semaphore | ~300 lines |
| `awa-worker/src/lib.rs` | Re-export `HttpWorkerConfig`, `HttpExecutionMode` | trivial |
| `awa-worker/src/runtime.rs` | Accept HTTP workers in builder (`callback_base_url`, worker map), register in worker map | ~40 lines |
| `awa-worker/Cargo.toml` | Add `reqwest` dependency (feature-gated behind `http-worker`, features: `json`, `rustls-tls`) | trivial |
| `awa-python/src/client.rs` | Expose `http_worker_async()` / `http_worker_sync()` methods; update `start()` to pass HTTP workers to builder; update empty-workers validation | ~60 lines |
| Tests | Integration tests with mock HTTP server for both sync and async modes; dispatch failure classification tests | ~200 lines |
| Docs | Configuration reference, deployment examples | ~100 lines |

**No schema changes.** No new job states. No migration. The `run_lease` column already exists on the jobs table — the change is purely in query logic. No new indexes are needed — the existing unique partial index on `callback_id` remains the access path for all callback resolution queries; `run_lease` is a cheap heap-tuple recheck filter on the single matched row.

#### Callback endpoint exposure

Today, `awa-ui` has no callback routes — its router (`awa-ui/src/lib.rs`) serves only the dashboard API (jobs, queues, cron, stats). The callback model functions exist in `awa-model::admin` but have no HTTP surface.

Two options:

1. **Extend `awa-ui` router** (recommended for Topology B). Add `/api/callbacks/*` routes to the existing `awa-ui` API. This means any process running `awa serve` (or the combined worker+UI binary) also accepts callbacks. Simple, but couples the callback receiver lifecycle to the UI.
2. **Standalone callback receiver.** A minimal axum server that only mounts `/api/callbacks/*` — no dashboard, no static files. This is lighter for Topology A where no UI is needed, but requires a separate binary or feature-gated entry point.

The recommended approach is option 1 as the initial implementation, with the standalone receiver as a follow-up if needed. The `awa worker --http-only` CLI mode (currently listed under Future Work) would need to embed this receiver, making it a prerequisite for Topology A.

### Security considerations

- **Callback authentication:** Callback endpoints must validate that the caller is authorized. Options: HMAC-signed callback IDs (recommended), shared secret in header, or network-level controls (VPC, IAM).
- **Request signing:** Outbound POSTs to serverless functions should support configurable auth headers (Bearer tokens, AWS SigV4 via SDK).
- **Payload sensitivity:** Job args may contain PII. TLS is required. Operators should consider whether args need encryption at rest in the function's logging/observability pipeline.

### Example application architecture

The key question is where the always-on Awa dispatcher runs relative to the serverless functions it invokes. Here are three concrete deployment topologies.

#### Topology A: Supabase-native (smallest footprint)

A SaaS application using Supabase for everything — auth, database, storage, and edge functions. Awa runs as a single always-on process on a small compute instance (Fly.io, Railway, or a $5 VPS), connected to the same Supabase Postgres.

**Note:** This topology requires callback routes to be exposed (see **Callback endpoint exposure** above) and the `awa worker --http-only` CLI mode (currently future work). Until both land, this topology requires either running the combined `awa serve` + worker binary, or a custom binary that embeds the callback receiver.

```
┌─────────────────────────────────────────────────────────────────┐
│  Supabase Project                                               │
│                                                                 │
│  ┌──────────────┐    ┌──────────────────────────────────────┐   │
│  │  Supabase     │    │  Supabase Postgres                   │   │
│  │  Auth / API   │    │                                      │   │
│  │  (PostgREST)  │    │  public.*    ── app tables           │   │
│  └──────┬───────┘    │  awa.*       ── job queue tables     │   │
│         │             │                                      │   │
│         │ INSERT INTO │  ◄── transactional enqueue ──────┐   │   │
│         │ awa.jobs    │                                  │   │   │
│         ▼             └────────┬─────────────────────────┼───┘   │
│  ┌──────────────┐              │ LISTEN/NOTIFY           │       │
│  │  Edge Funcs   │              │ + polling               │       │
│  │              │              │                         │       │
│  │  send-email  │◄─── POST ───┼─────────────┐           │       │
│  │  resize-img  │              │             │           │       │
│  │  gen-pdf     │              │             │           │       │
│  └──────┬───────┘              │             │           │       │
│         │                      │             │           │       │
│         │ POST callback        │             │           │       │
│         ▼                      │             │           │       │
└─────────────────────────────────┼─────────────┼───────────┼───────┘
                                 │             │           │
                          ┌──────┴─────────────┴───────────┴──────┐
                          │  Awa Dispatcher                        │
                          │  (Fly.io / Railway / VPS)              │
                          │                                        │
                          │  • 1 process, ~50MB RAM                │
                          │  • Dispatcher + Maintenance + Leader   │
                          │  • HttpWorker (no local job handlers)  │
                          │  • Exposes /api/callbacks/* endpoint   │
                          │                                        │
                          │  awa worker --http-only                │
                          └────────────────────────────────────────┘
```

**Data flow:**
1. User action → Supabase PostgREST or Edge Function → `INSERT INTO awa.jobs` (same Postgres transaction as business data)
2. Awa dispatcher (on Fly.io) picks up job via LISTEN/NOTIFY
3. HttpWorker POSTs to `https://project.supabase.co/functions/v1/send-email`
4. Edge Function does work, POSTs back to `https://awa.fly.dev/api/callbacks/{id}/complete`
5. Awa marks job completed

**Cost profile:** ~$3-7/month for the always-on dispatcher. Edge Functions scale to zero. No idle worker costs.

#### Topology B: Kubernetes + Lambda (enterprise mixed workload)

A larger team running Kubernetes for their core services. CPU-heavy jobs (video encoding, ML inference) run on local Awa workers with GPU access. Lightweight jobs (notifications, webhooks) fan out to Lambda for scale-to-zero cost savings.

```
┌─────────────────────────────────────────────────────┐
│  Kubernetes Cluster                                  │
│                                                      │
│  ┌──────────────────┐    ┌───────────────────────┐   │
│  │  App Pods         │    │  Awa Workers (2 pods) │   │
│  │                   │    │                       │   │
│  │  order-service    │    │  Local handlers:      │   │
│  │  user-service     │    │    • encode_video     │   │
│  │  payment-service  │    │    • run_ml_model     │   │
│  │                   │    │                       │   │
│  │  (enqueue jobs    │    │  HttpWorkers:         │   │
│  │   via awa::insert │    │    • send_email → λ   │   │
│  │   in transaction) │    │    • send_sms → λ     │   │
│  │                   │    │    • gen_pdf → λ      │   │
│  └──────────────────┘    │    • sync_crm → λ     │   │
│                           │                       │   │
│                           │  Callback endpoint:   │   │
│                           │    :8080/api/callbacks │   │
│                           └───────┬───────────────┘   │
│                                   │                   │
└───────────────────────────────────┼───────────────────┘
           │                        │
           │ sqlx                   │ POST to Lambda URLs
           ▼                        ▼
  ┌─────────────────┐    ┌─────────────────────────────┐
  │  Amazon RDS      │    │  AWS Lambda                  │
  │  (Postgres)      │    │                              │
  │                  │    │  send-email (Node.js)        │
  │  public.*        │    │  send-sms (Python)           │
  │  awa.*           │    │  gen-pdf (Go)                │
  └─────────────────┘    │  sync-crm (TypeScript)       │
                          │                              │
                          │  → POST callback to          │
                          │    k8s ingress / ALB          │
                          └─────────────────────────────┘
```

**Key insight:** Local and HTTP workers coexist. GPU jobs run on Kubernetes pods with access to local resources. Stateless I/O jobs fan out to Lambda in any language. One Awa deployment orchestrates both.

#### Topology C: Cloudflare-first (edge-native)

An application built on Cloudflare's stack — Workers for API, D1 or external Postgres for data, and Awa for durable job orchestration. Short I/O-bound jobs use sync mode (Workers), longer jobs use async mode via Workflows or Containers.

```
┌───────────────────────────────────────────────────────────────┐
│  Cloudflare Edge                                               │
│                                                                │
│  ┌───────────────────┐  ┌──────────────────┐  ┌────────────┐  │
│  │  API Worker         │  │  Job Workers      │  │  Workflows │  │
│  │                     │  │  (sync)           │  │  (async)   │  │
│  │  Handles requests,  │  │                   │  │            │  │
│  │  enqueues jobs via  │  │  resize-image     │  │  gen-pdf   │  │
│  │  Hyperdrive SQL     │  │  send-webhook     │  │  run-etl   │  │
│  │                     │  │  gen-thumbnail    │  │            │  │
│  └──────────┬──────────┘  └───────▲──────────┘  └──▲───┬─────┘  │
│             │                     │                │   │         │
│             │ INSERT INTO         │ POST (sync)    │   │ POST    │
│             │ awa.jobs            │ returns result  │   │callback │
│             ▼                     │                │   ▼         │
└─────────────┼─────────────────────┼────────────────┼────┼────────┘
              │                     │                │    │
     ┌────────┴─────────────────────┴────────────────┴────┴─────┐
     │  Awa Dispatcher (Fly.io)                                  │
     │                                                           │
     │  HttpWorkers (sync mode):                                 │
     │    • resize_image → worker.account.workers.dev            │
     │    • send_webhook → worker.account.workers.dev            │
     │  HttpWorkers (async mode):                                │
     │    • gen_pdf → workflow-trigger.account.workers.dev        │
     │    • run_etl → workflow-trigger.account.workers.dev        │
     │                                                           │
     │  Callback endpoint: /api/callbacks/*                      │
     │  Connected to Postgres via direct connection              │
     └────────────────────┬──────────────────────────────────────┘
                          │
                 ┌────────┴────────┐
                 │  Postgres        │
                 │  (Neon / Supabase│
                 │   / RDS)         │
                 └─────────────────┘
```

**Mixed sync + async.** Short I/O-bound jobs use sync mode with standard Workers (sub-ms cold starts, unlimited wall-clock, up to 5 min CPU). Longer or multi-step jobs use async mode, triggering a Cloudflare Workflow that executes durably and calls back on completion. The Awa dispatcher is the only always-on component.

### Platform-specific notes

#### AWS Lambda

Lambda is the strongest target for async mode. Function URLs provide direct HTTPS invocation without API Gateway, and Lambda's 15-minute timeout gives ample room for long-running jobs.

**Invocation:** Lambda function URLs (`https://<url-id>.lambda-url.<region>.on.aws/`) or API Gateway endpoints. HttpWorker POSTs to the function URL directly.

**Auth:** Function URLs support IAM auth (recommended) or `NONE` auth type with HMAC verification in the function. For IAM auth, HttpWorker needs AWS SigV4 request signing (listed under Future Work; use `NONE` + HMAC for initial implementation).

**Execution limits:** 15 min max timeout, 6 MB payload (request and response), 10 GB memory. Async mode is the natural fit for anything beyond ~30s.

**Idempotency:** Lambda may retry async invocations on transient errors. Functions should use `job_id` + `run_lease` as an idempotency key to prevent duplicate work. This is especially important for side-effecting jobs (sending emails, charging payments).

**Example config:**
```rust
.http_worker(HttpWorkerConfig {
    kind: "send_email",
    endpoint: "https://abc123.lambda-url.us-east-1.on.aws/".parse()?,
    mode: HttpExecutionMode::Async {
        callback_timeout: Duration::from_secs(300),
    },
    headers: vec![("Authorization", "Bearer ${AWA_LAMBDA_TOKEN}")],
    request_timeout: Duration::from_secs(10),
    callback_config: None,
})
```

#### Supabase Edge Functions

Supabase is a compelling target because Awa's Postgres tables can live in the **same Supabase Postgres instance** that powers the rest of the application. This eliminates the operational overhead of a separate database and means transactional enqueue (ADR-001) works natively with the application's Supabase client.

**Invocation:** Edge Functions are deployed at `https://<project-id>.supabase.co/functions/v1/<function-name>`. HttpWorker POSTs to this URL.

**Auth:** Supabase Edge Functions expect a valid JWT in the `Authorization` header by default (verified against the project's JWT signing keys). For HttpWorker, the simplest approach is to use the project's `service_role` key as a Bearer token in the `headers` config. Functions can also disable JWT verification in `config.toml` and rely on HMAC-signed callback IDs instead.

**Execution limits:** 150s response timeout (free and pro), 400s wall-clock (pro, for background work after initial response). CPU time is limited to 2s. This means:
- **Sync mode** works well for jobs completing within 150s.
- **Async mode** is preferred for longer jobs. The function **must** use `EdgeRuntime.waitUntil()` to continue work after returning the initial response — without it, the runtime terminates when the response is sent and the callback never fires. The 400s wall-clock limit applies to the total function lifetime including `waitUntil` work. For jobs exceeding 400s, break work into stages or self-host the edge runtime.

**Callback path:** The function calls back to Awa's callback endpoint. If the Awa dispatcher runs outside Supabase (e.g., on a VPS or Kubernetes), the callback URL must be publicly reachable. If the dispatcher runs on the same Supabase project (e.g., as a long-running compute instance), callbacks can stay internal.

**Direct Postgres access:** Edge Functions can query Supabase Postgres via the `supabase-js` client or direct connection string. A function could theoretically call `complete_external` by writing SQL directly — bypassing the HTTP callback entirely. This is a power-user shortcut but couples the function to Awa's schema.

**Example config:**
```rust
.http_worker(HttpWorkerConfig {
    kind: "send_welcome_email",
    endpoint: "https://myproject.supabase.co/functions/v1/send-email".parse()?,
    mode: HttpExecutionMode::Async {
        callback_timeout: Duration::from_secs(300),
    },
    headers: vec![("Authorization", "Bearer ${SUPABASE_SERVICE_ROLE_KEY}")],
    request_timeout: Duration::from_secs(10),
    callback_config: None,
})
```

**Example Supabase Edge Function (Deno/TypeScript):**

For async mode, the function must return a response immediately and continue work in the background using `EdgeRuntime.waitUntil()`. Without this, "return 200 immediately" does not actually keep the function alive to do work and call back.

```typescript
Deno.serve(async (req) => {
  const { job_id, kind, args, callback_id, callback_url, run_lease } = await req.json();

  // Return 200 immediately — work continues in background
  EdgeRuntime.waitUntil((async () => {
    try {
      const result = await sendEmail(args.to, args.template);
      await fetch(callback_url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ run_lease, result }),
      });
    } catch (err) {
      await fetch(callback_url.replace("/complete", "/fail"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ run_lease, error: err.message }),
      });
    }
  })());

  return new Response("accepted", { status: 202 });
});
```

For sync mode (short jobs), the simpler inline pattern works:

```typescript
Deno.serve(async (req) => {
  const { job_id, kind, args } = await req.json();
  const result = await sendEmail(args.to, args.template);
  return Response.json({ result });
});
```

#### Google Cloud Run

Cloud Run is a strong fit for the **always-on Awa control plane** itself: set `min-instances: 1`, configure a request timeout up to 3600s, and you get a managed container with autoscaling, health checks, and IAM-gated ingress out of the box.

**As control plane host:** If Awa already runs on Cloud Run with `min-instances: 1`, a plain local Awa worker may be simpler than adding HttpWorker — unless you specifically want polyglot handlers or stronger per-job isolation. HttpWorker is optional in this topology, not obviously the default.

**As execution plane:** Cloud Run services can also be HttpWorker targets. They support up to 3600s request timeout (configurable), making both sync and async modes viable. Cloud Run's cold-start latency (typically 200ms-2s) is acceptable for async jobs.

**Example config (as execution target):**
```rust
.http_worker(HttpWorkerConfig {
    kind: "generate_report",
    endpoint: "https://report-generator-abc123-uc.a.run.app/execute".parse()?,
    mode: HttpExecutionMode::Async {
        callback_timeout: Duration::from_secs(600),
    },
    headers: vec![("Authorization", "Bearer ${GCP_ID_TOKEN}")],
    request_timeout: Duration::from_secs(10),
    callback_config: None,
})
```

#### Cloudflare Workers, Workflows, and Containers

Cloudflare's platform has three execution models relevant to HttpWorker, each suited to different job profiles.

**Invocation:** Workers are deployed at `https://<worker-name>.<account>.workers.dev` or custom domain routes. HttpWorker POSTs to this URL.

**Auth:** Worker URLs are public by default. Auth must be implemented in the Worker itself. Recommended: pass a shared secret or HMAC-signed payload in the `Authorization` header, verified in the Worker's `fetch` handler. Cloudflare Access (Zero Trust) can also gate the endpoint with mTLS or JWT.

**Execution limits:**

| Mode | CPU time | Wall-clock | Fit |
|---|---|---|---|
| Standard Worker (paid) | 5 min (configurable) | **Unlimited** (while caller connected) | **Sync mode** — ideal for I/O-heavy jobs |
| Workflows (GA) | 5 min per step | Unlimited (25K steps) | **Async mode** — best Cloudflare fit for callbacks |
| Containers (beta) | Unlimited | Unlimited | Heavy compute, any language/runtime |
| Queue Consumers | 15 min | 15 min | Alternative dispatch path (future work) |

**Important:** The often-cited "30 second" Workers limit is *CPU time*, not wall-clock. Wall-clock time for HTTP-invoked Workers is unlimited as long as the caller (Awa's HttpWorker) holds the connection open. An I/O-heavy job that mostly awaits `fetch()` calls can run for minutes of wall time while consuming only milliseconds of CPU. This means sync mode is viable for a much wider range of jobs on Cloudflare than previously stated.

##### Standard Workers (sync mode)

Best for short, I/O-heavy jobs: webhook dispatch, image transforms, cache invalidation, API orchestration. The 5-minute CPU limit (configurable via `limits.cpu_ms` in `wrangler.toml`) is generous for most I/O-bound work. Sub-millisecond cold starts make the round-trip penalty negligible.

**Example config (sync mode):**
```rust
.http_worker(HttpWorkerConfig {
    kind: "resize_image",
    endpoint: "https://job-runner.myaccount.workers.dev/execute".parse()?,
    mode: HttpExecutionMode::Sync {
        timeout: Duration::from_secs(25),
    },
    headers: vec![("X-Awa-Secret", "${AWA_WORKER_SECRET}")],
    request_timeout: Duration::from_secs(30),
    max_concurrent_dispatches: Some(100),
    callback_config: None,
})
```

**Example Worker (sync mode):**
```javascript
export default {
  async fetch(request, env) {
    if (request.headers.get("X-Awa-Secret") !== env.AWA_WORKER_SECRET) {
      return new Response("Unauthorized", { status: 401 });
    }
    const { job_id, kind, args } = await request.json();
    const result = await resizeImage(args.url, args.width, args.height);
    return Response.json({ result });
  }
};
```

##### Cloudflare Workflows (async mode)

Workflows (GA April 2025) is the best fit for async callback-based jobs on Cloudflare. A Workflow is a durable, multi-step execution engine: each `step.do()` is persisted and automatically retried on failure. Crucially, `step.waitForEvent()` lets a Workflow pause for an external event (with up to 1 year timeout) — this maps directly to Awa's `waiting_external` + callback pattern.

**How it works with HttpWorker:**
1. HttpWorker POSTs to a Worker that creates a Workflow instance.
2. The Worker returns 202 immediately (HttpWorker is in async mode, job parks in `waiting_external`).
3. The Workflow executes the job logic across durable steps.
4. On completion, a final step POSTs to Awa's callback URL.

This gives you durable execution with automatic retries and no wall-clock limit, without needing `waitUntil()` hacks. Limits: 25,000 steps per instance, 10,000 concurrent instances, 100 instance creations/second, 5 min CPU per step.

**Example Workflow:**
```javascript
export class JobWorkflow extends WorkflowEntrypoint {
  async run(instance, env) {
    const { job_id, args, callback_url, run_lease } = instance.payload;

    const result = await instance.do("process", async () => {
      return await heavyProcessing(args);
    });

    await instance.do("callback", async () => {
      await fetch(callback_url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ run_lease, result }),
      });
    });
  }
}
```

##### Cloudflare Containers (heavy compute)

Containers (beta) run Docker images on Cloudflare with no execution timeout, any language, and up to 4 vCPUs / 12 GB RAM. Each container instance is backed by a Durable Object. Pricing is similar to Cloud Run (~$0.000020/vCPU-second, $0.0000025/GiB-second). Containers auto-sleep after a configurable idle timeout.

Best for: jobs that need native binaries, Python/Rust runtimes, heavy CPU, or execution times exceeding 5 minutes. A Worker front-door routes HTTP requests to container instances. For Awa, the Worker receives the HttpWorker POST and delegates to a container, which calls back on completion.

**Postgres via Hyperdrive:** Cloudflare Hyperdrive provides connection pooling to external Postgres databases. Workers can query Awa's Postgres directly using `pg` (node-postgres) through Hyperdrive — enabling a direct-SQL callback shortcut. However, the HttpWorker HTTP callback is preferred for decoupling.

### Alternatives considered

**Message broker dispatch (SQS, Cloud Tasks, NATS, Redis Streams).** Instead of HTTP POST, Awa could publish to a message queue and let the function platform consume natively. This provides built-in backpressure and retry. Rejected because: (a) it breaks Awa's core value — transactional enqueue from Postgres. Publishing to SQS cannot be made atomic with an `INSERT INTO awa.jobs` in the same Postgres transaction without outbox-pattern machinery. (b) It creates two sources of truth for job state (Postgres + broker). (c) It adds infrastructure, violating Awa's Postgres-only dependency model. The backpressure benefit is better addressed by `max_concurrent_dispatches` on HttpWorker.

**Pull-based dispatch (Temporal-style).** The function long-polls Awa for work instead of being POSTed to. This eliminates the callback-endpoint-exposure problem and is how Temporal's task queues work. Rejected for HttpWorker because it is incompatible with scale-to-zero — something must invoke the function to start polling, which brings you back to push-based dispatch. Pull-based dispatch also requires embedding Awa client logic (claim, heartbeat, complete) in the function. A future `PollWorker` mode could serve always-on services that cannot expose inbound endpoints.

### What this is NOT

- **Not a serverless runtime.** Awa still requires at least one always-on process for dispatching, maintenance, and leader election. The serverless function replaces only the job execution step.
- **Not a webhook ingestion system.** The callback endpoint is purpose-built for Awa job completion, not a general-purpose webhook receiver.
- **Not auto-scaling.** Awa does not provision or manage serverless functions. It simply calls a URL.

## Consequences

### Positive

- **Serverless teams can adopt Awa** for job orchestration while keeping execution in their existing function infrastructure.
- **Zero schema changes.** The `waiting_external` state and callback machinery already exist and are well-tested (13 Rust integration tests, 16 Python tests, TLA+ spec in `correctness/AwaCbk.tla`). The prerequisite callback hardening (race-condition fix, `run_lease` guard) is a correctness improvement that benefits all callback users, not just HttpWorker.
- **Incremental adoption.** Teams can mix local and HTTP workers on the same Awa instance — some job kinds run locally, others fan out to Lambda.
- **Language-agnostic execution.** Any language that can receive an HTTP POST and make an HTTP callback works. No Rust or Python SDK needed on the function side.
- **Feature-gated.** The `reqwest` dependency and HTTP worker code live behind a Cargo feature flag (`http-worker`), adding zero cost for users who don't need it.

### Negative

- **At-least-once execution semantics.** HttpWorker cannot guarantee exactly-once execution. Duplicate execution can occur when an HTTP timeout is ambiguous (the function may or may not be running) or when Lambda retries an invocation. The conservative dispatch-failure handling (prefer `WaitForCallback` over `cancel_callback` on ambiguous failures) minimizes duplicates, but side-effecting functions must implement application-level idempotency.
- **Always-on dispatcher required.** Awa cannot scale to zero entirely — the dispatcher, maintenance, and leader services must keep running. This limits the "pure serverless" appeal.
- **Added latency.** HTTP round-trips (especially with cold starts) add latency compared to local execution. Sync mode is particularly sensitive to cold starts.
- **Callback endpoint exposure.** The callback endpoint must be reachable by the serverless function, which may require public exposure or VPC peering. This increases the attack surface. The `callback_base_url` must be correctly configured to match the Awa process's actual ingress.
- **No progress reporting in async mode.** Serverless functions cannot call `ctx.set_progress()`. A future HTTP progress endpoint could address this, but is out of scope for the initial implementation.
- **Payload size limits.** AWS Lambda has a 6MB payload limit. Jobs with very large `args` may need to store data externally (S3, etc.) and pass a reference.

### Future work (out of scope)

- **CLI command (`awa worker --http-only`)** for running a dispatcher + callback receiver without any local workers. Essential for Topology A — until this ships, Topology A requires the combined `awa serve` binary or a custom entry point.
- **WebSocket connect-back mode.** Instead of Awa POSTing to the function and the function calling back, the function opens an outbound WebSocket to Awa and receives work over the connection. This sidesteps the callback-endpoint-must-be-public problem for environments where the Awa instance cannot expose inbound endpoints. Inspired by Inngest's similar pattern.
- HTTP progress reporting endpoint for long-running async functions.
- Outbound request signing (AWS SigV4, GCP identity tokens) as built-in auth strategies.
- Response body mapping to job metadata/output storage.
- Auto-scaling integration (scaling serverless concurrency based on queue depth).
- WASM producer SDK (`awa-wasm` crate) exposing `prepare_row()`, blake3 unique key computation, and CEL validation for use in Deno/Cloudflare Workers. Blocked today by sqlx/tokio WASM incompatibility for anything beyond the data preparation layer; revisit when WASI-P3 matures.
