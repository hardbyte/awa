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

Awa already has a `waiting_external` state and callback infrastructure (ADR not numbered; implemented in `awa-model::admin` and `awa-worker::context`). This machinery — callback registration, timeout rescue, CEL-based resolution — is exactly what a serverless execution model needs. The missing piece is a built-in worker that bridges the gap: claim a job, POST it to an HTTP endpoint, and park until the function calls back.

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

**Dispatch failure handling:** If step 3 fails (network error, non-2xx from the function platform), the worker must **not** return `WaitForCallback`. Instead, it drops the `CallbackGuard` and returns `JobError::Retryable`. However, the callback_id has already been written to the database in step 2. To prevent a stale callback from a failed dispatch from incorrectly resolving a later retry attempt, two complementary safeguards are required:

1. **HttpWorker clears the callback on dispatch failure.** Before returning the retryable error, the worker calls a new `cancel_callback(pool, job_id, run_lease)` function that NULLs out `callback_id`, `callback_timeout_at`, and all CEL fields — but only if the `run_lease` still matches (preventing races with rescue).
2. **Callback resolution binds to `run_lease`.** The `complete_external`, `fail_external`, and `resolve_callback` functions currently match by `callback_id` alone (`WHERE callback_id = $1`). This is insufficient when callbacks can become orphaned. These queries must additionally store and verify the `run_lease` that was active when the callback was registered. This can be done by: (a) adding `run_lease` to the `register_callback` write and including it in the resolution WHERE clause, or (b) generating a new `callback_id` that embeds the lease (e.g., as an HMAC component) so stale IDs are cryptographically invalidated. Option (a) is simpler and recommended.

Safeguard 2 is a correctness hardening that benefits all callback users, not just `HttpWorker`. It should be implemented as a prerequisite change before `HttpWorker` ships.

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

   --- Dispatch failure path ---

   available ──► claim job
                  │
                  ├─ register_callback(timeout)
                  │
                  ├─ POST /invoke ─── ✗ network error / 5xx
                  │
                  ├─ cancel_callback(job_id, run_lease)
                  │   (NULLs callback fields if lease matches)
                  │
                  ├─ return Retryable error
                  │
   retryable ◄──-─┘  (normal backoff, re-dispatched later)
```

This reuses 100% of the existing callback infrastructure:

- **Timeout rescue:** Maintenance service rescues jobs where `callback_timeout_at < now()`, transitioning to `retryable` (with attempts remaining) or `failed`.
- **CEL expressions:** `register_callback_with_config` supports `filter`, `on_complete`, `on_fail`, and `transform` expressions evaluated against the function's callback payload.
- **Retry semantics:** If the function calls `fail_external` or the callback times out, normal retry logic applies (exponential backoff, `max_attempts`).
- **Heartbeat/deadline:** Cleared when entering `waiting_external` — no false rescues while the function runs.

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

No callback registration, no `waiting_external` state. The job completes in a single dispatcher cycle. Heartbeats remain active during the HTTP call since the job stays in `running` state.

### Configuration

HTTP workers are configured on `ClientBuilder`, parallel to local workers:

```rust
let client = Client::builder()
    .queue("email", QueueConfig::default())
    // Local worker — unchanged
    .register::<ResizeImage, _, _>(resize_handler)
    // HTTP worker — new
    .http_worker(HttpWorkerConfig {
        kind: "send_email",
        endpoint: "https://abc123.lambda-url.us-east-1.on.aws/".parse()?,
        mode: HttpExecutionMode::Async {
            callback_timeout: Duration::from_secs(300),
        },
        // Optional
        headers: vec![("Authorization", "Bearer ${AWA_LAMBDA_TOKEN}")],
        request_timeout: Duration::from_secs(10), // POST timeout, not job timeout
        callback_config: None, // Optional CEL expressions
    })
    .build(&pool)
    .await?;
```

Python equivalent:

```python
client = awa.AsyncClient(pool)

client.http_worker(
    kind="send_email",
    endpoint="https://abc123.lambda-url.us-east-1.on.aws/",
    mode="async",
    callback_timeout=300,
    headers={"Authorization": f"Bearer {os.environ['AWA_LAMBDA_TOKEN']}"},
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
  "callback_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "callback_url": "https://awa.example.com/api/callbacks/f47ac10b-58cc-4372-a567-0e02b2c3d479/complete"
}

```

`callback_id` and `callback_url` are omitted in sync mode.

#### Callback contract

The serverless function completes the job by POSTing to one of:

```
POST {callback_url}                          → complete (shorthand)
POST /api/callbacks/{callback_id}/complete   → complete with optional payload
POST /api/callbacks/{callback_id}/fail       → fail with error message
POST /api/callbacks/{callback_id}/retry      → retry (back to available)
```

These endpoints already exist in `awa-model::admin` (`complete_external`, `fail_external`, `retry_external`, `resolve_callback`). They need to be exposed as HTTP routes — either in `awa-ui` (extending the existing API) or as a standalone lightweight callback receiver.

### Implementation scope

| Component | Change | Size |
|---|---|---|
| `awa-model/src/admin.rs` | **Prerequisite:** Add `run_lease` guard to `complete_external`, `fail_external`, `resolve_callback` WHERE clauses; add `cancel_callback` function | ~40 lines |
| `awa-model/src/admin.rs` | Store `run_lease` in `register_callback` / `register_callback_with_config` | ~10 lines |
| `awa-worker/src/http_worker.rs` | New file: `HttpWorker` struct, `Worker` impl, config types, dispatch failure handling | ~250 lines |
| `awa-worker/src/lib.rs` | Re-export `HttpWorkerConfig`, `HttpExecutionMode` | trivial |
| `awa-worker/src/runtime.rs` | Accept HTTP workers in builder, register in worker map | ~30 lines |
| `awa-worker/Cargo.toml` | Add `reqwest` dependency (feature-gated behind `http-worker`) | trivial |
| `awa-ui/src/api.rs` (or new) | Expose callback endpoints as HTTP routes | ~80 lines |
| `awa-python/src/client.rs` | Expose `http_worker()` config method | ~50 lines |
| Tests | Integration tests with mock HTTP server; regression tests for stale callback resolution | ~200 lines |
| Docs | Configuration reference, deployment examples | ~100 lines |

**No schema changes.** No new job states. No migration. The `run_lease` column already exists on the jobs table — the change is purely in query logic.

### Security considerations

- **Callback authentication:** Callback endpoints must validate that the caller is authorized. Options: HMAC-signed callback IDs (recommended), shared secret in header, or network-level controls (VPC, IAM).
- **Request signing:** Outbound POSTs to serverless functions should support configurable auth headers (Bearer tokens, AWS SigV4 via SDK).
- **Payload sensitivity:** Job args may contain PII. TLS is required. Operators should consider whether args need encryption at rest in the function's logging/observability pipeline.

### Platform-specific notes

#### Supabase Edge Functions

Supabase is a compelling target because Awa's Postgres tables can live in the **same Supabase Postgres instance** that powers the rest of the application. This eliminates the operational overhead of a separate database and means transactional enqueue (ADR-001) works natively with the application's Supabase client.

**Invocation:** Edge Functions are deployed at `https://<project-id>.supabase.co/functions/v1/<function-name>`. HttpWorker POSTs to this URL.

**Auth:** Supabase Edge Functions expect a valid JWT in the `Authorization` header by default (verified against the project's JWT signing keys). For HttpWorker, the simplest approach is to use the project's `service_role` key as a Bearer token in the `headers` config. Functions can also disable JWT verification in `config.toml` and rely on HMAC-signed callback IDs instead.

**Execution limits:** 150s response timeout (free and pro), 400s wall-clock (pro, for background work after initial response). CPU time is limited to 2s. This means:
- **Sync mode** works well for jobs completing within 150s.
- **Async mode** is preferred for longer jobs — the function returns a 200 immediately after receiving the job, then does work and calls back within the 400s wall-clock limit. For jobs exceeding 400s, break work into stages or self-host the edge runtime.

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
```typescript
Deno.serve(async (req) => {
  const { job_id, kind, args, callback_id, callback_url } = await req.json();

  // Do the work
  const result = await sendEmail(args.to, args.template);

  // Call back to Awa
  await fetch(callback_url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ result }),
  });

  return new Response("ok");
});
```

#### Cloudflare Workers

Cloudflare Workers run at the edge with sub-millisecond cold starts, making them well-suited for high-volume, latency-sensitive job execution.

**Invocation:** Workers are deployed at `https://<worker-name>.<account>.workers.dev` or custom domain routes. HttpWorker POSTs to this URL.

**Auth:** Worker URLs are public by default. Auth must be implemented in the Worker itself. Recommended: pass a shared secret or HMAC-signed payload in the `Authorization` header, verified in the Worker's `fetch` handler. Cloudflare Access (Zero Trust) can also gate the endpoint with mTLS or JWT.

**Execution limits:**

| Mode | CPU time | Wall-clock | Fit |
|---|---|---|---|
| Standard (paid) | 30s CPU | ~30s | Sync mode for fast jobs |
| Cron Triggers | 15 min | 15 min | Not applicable (push-based) |
| Queue Consumers | 15 min | 15 min | Alternative dispatch path (see below) |

- **Sync mode** works for jobs completing within ~30s wall-clock.
- **Async mode** is tricky — a standard Worker has only ~30s to receive the POST, do work, and call back. For longer jobs, use the `ctx.waitUntil()` pattern: return a 200 immediately, then continue processing in the background (limited to the Worker's wall-clock budget).
- For jobs exceeding 30s, **Durable Objects** (with alarms) provide extended execution with no hard wall-clock limit, billed per duration.

**Postgres via Hyperdrive:** Cloudflare Hyperdrive provides connection pooling to external Postgres databases. Workers can query Awa's Postgres directly using `pg` (node-postgres) through Hyperdrive — enabling the same direct-SQL callback shortcut as Supabase. However, the HttpWorker HTTP callback is preferred for decoupling.

**Cloudflare Queues alternative:** For teams already using Cloudflare Queues, an alternative to HttpWorker's HTTP dispatch is a future `CloudflareQueueWorker` that publishes jobs to a Queue instead of POSTing to a URL. The Queue consumer Worker then gets 15 minutes of execution time. This is out of scope for this ADR but worth noting as a natural extension.

**Example config:**
```rust
.http_worker(HttpWorkerConfig {
    kind: "resize_image",
    endpoint: "https://job-runner.myaccount.workers.dev/execute".parse()?,
    mode: HttpExecutionMode::Sync {
        timeout: Duration::from_secs(25),
    },
    headers: vec![("X-Awa-Secret", "${AWA_WORKER_SECRET}")],
    request_timeout: Duration::from_secs(30),
    callback_config: None,
})
```

**Example Cloudflare Worker:**
```javascript
export default {
  async fetch(request, env) {
    // Verify auth
    if (request.headers.get("X-Awa-Secret") !== env.AWA_WORKER_SECRET) {
      return new Response("Unauthorized", { status: 401 });
    }

    const { job_id, kind, args, callback_id, callback_url } = await request.json();

    // For sync mode: do work and return result directly
    const result = await resizeImage(args.url, args.width, args.height);
    return Response.json({ result });

    // For async mode: acknowledge and process in background
    // ctx.waitUntil(processAndCallback(callback_url, args));
    // return new Response("accepted", { status: 202 });
  }
};
```

### What this is NOT

- **Not a serverless runtime.** Awa still requires at least one always-on process for dispatching, maintenance, and leader election. The serverless function replaces only the job execution step.
- **Not a webhook ingestion system.** The callback endpoint is purpose-built for Awa job completion, not a general-purpose webhook receiver.
- **Not auto-scaling.** Awa does not provision or manage serverless functions. It simply calls a URL.

## Consequences

### Positive

- **Serverless teams can adopt Awa** for job orchestration while keeping execution in their existing function infrastructure.
- **Zero schema changes.** The `waiting_external` state and callback machinery already exist and are well-tested (13 Rust integration tests, 16 Python tests, TLA+ spec in `correctness/AwaCbk.tla`).
- **Incremental adoption.** Teams can mix local and HTTP workers on the same Awa instance — some job kinds run locally, others fan out to Lambda.
- **Language-agnostic execution.** Any language that can receive an HTTP POST and make an HTTP callback works. No Rust or Python SDK needed on the function side.
- **Feature-gated.** The `reqwest` dependency and HTTP worker code live behind a Cargo feature flag (`http-worker`), adding zero cost for users who don't need it.

### Negative

- **Always-on dispatcher required.** Awa cannot scale to zero entirely — the dispatcher, maintenance, and leader services must keep running. This limits the "pure serverless" appeal.
- **Added latency.** HTTP round-trips (especially with cold starts) add latency compared to local execution. Sync mode is particularly sensitive to cold starts.
- **Callback endpoint exposure.** The callback endpoint must be reachable by the serverless function, which may require public exposure or VPC peering. This increases the attack surface.
- **No progress reporting in async mode.** Serverless functions cannot call `ctx.set_progress()`. A future HTTP progress endpoint could address this, but is out of scope for the initial implementation.
- **Payload size limits.** AWS Lambda has a 6MB payload limit. Jobs with very large `args` may need to store data externally (S3, etc.) and pass a reference.

### Future work (out of scope)

- HTTP progress reporting endpoint for long-running async functions.
- Outbound request signing (AWS SigV4, GCP identity tokens) as built-in auth strategies.
- Response body mapping to job metadata/output storage.
- CLI command (`awa worker --http-only`) for running a dispatcher without any local workers.
- Auto-scaling integration (scaling serverless concurrency based on queue depth).
