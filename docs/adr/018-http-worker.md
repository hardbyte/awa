# ADR-018: HTTP Worker for Serverless Job Execution

## Status

Accepted

## Context

Awa workers run handler code in-process: the dispatcher claims a job, invokes `Worker::perform()`, and the handler executes within the same Rust or Python process. This is the simplest and fastest model, but it requires always-on worker processes for every job kind.

Production teams increasingly deploy short-lived compute via serverless platforms (AWS Lambda, Google Cloud Run, Azure Functions, Cloudflare Workers). These environments can't run a persistent Awa worker, but the work they perform fits naturally into a job queue: process a payment, generate a PDF, run an ML inference pipeline.

Today, integrating serverless functions with Awa requires custom glue: the application must enqueue a job, have a worker that makes an HTTP call and returns `WaitForCallback`, then separately expose an endpoint that calls `complete_external`. This is tedious and error-prone, especially around timeout handling and auth.

The callback infrastructure already supports everything needed:

- `register_callback` + `waiting_external` parks the job
- `complete_external` / `fail_external` resolves it
- `heartbeat_callback` keeps long-running functions alive
- `resume_external` enables multi-step flows (ADR-021)
- Callback timeout rescue handles crashed/abandoned functions

What's missing is a `Worker` implementation that automates the HTTP dispatch and callback plumbing.

## Decision

Add `HttpWorker`, a `Worker` trait implementation that dispatches jobs to HTTP endpoints. It operates in two modes:

### Async mode (primary)

The worker registers a callback, POSTs the job to the function URL with the callback information, and returns `WaitForCallback`. The serverless function calls back when done.

```
  Awa dispatcher                Function endpoint
       |                              |
       |--- claim job --------------->|
       |--- register_callback ------->| (DB)
       |--- POST /function ---------->|
       |    { job_id, args,           |
       |      callback_id,            |
       |      callback_url }          |
       |<-- 202 Accepted -------------|
       |--- WaitForCallback --------->| (DB: waiting_external)
       |                              |
       |    ... function executes ... |
       |                              |
       |<-- POST /callback/:id -------|
       |    { payload }               |
       |--- complete_external ------->| (DB: completed)
```

This is the right fit for functions that take seconds to hours: ML training, document generation, payment processing, human-in-the-loop approval.

### Sync mode

The worker POSTs the job and awaits the HTTP response directly, mapping the status code to a `JobResult`. No callback registration, no `waiting_external` state.

```
  Awa dispatcher                Function endpoint
       |                              |
       |--- claim job --------------->|
       |--- POST /function ---------->|
       |    { job_id, args }          |
       |                              |
       |    ... function executes ... |
       |                              |
       |<-- 200 OK -------------------|
       |    { result }                |
       |--- Completed --------------->| (DB)
```

This suits short-lived functions (< 30s) where the HTTP round-trip is acceptable and callback infrastructure is unnecessary.

### Callback authentication

The callback endpoint must verify that incoming requests are legitimate. The worker signs the callback ID using blake3 keyed hashing (already a workspace dependency via ADR-002) and includes the signature in the POST to the function. The function echoes the signature when calling back; the callback endpoint verifies it.

This is simpler and more performant than JWT or OAuth for this use case: the signature is tied to a specific callback ID, is cheap to compute, and doesn't require key rotation infrastructure.

### Feature gating

`HttpWorker` is gated behind the `http-worker` Cargo feature, which brings in `reqwest` as a dependency. Teams that don't need HTTP dispatch don't pay the compile-time cost.

### Registration

HTTP workers are registered on the `ClientBuilder` like any other worker, keyed by job kind:

```rust
Client::builder(pool)
    .http_worker("generate_pdf", HttpWorkerConfig {
        url: "https://pdf-service.run.app/generate".into(),
        mode: HttpWorkerMode::Async,
        callback_timeout: Duration::from_secs(300),
        ..Default::default()
    })
    .http_worker("validate_address", HttpWorkerConfig {
        url: "https://addr-fn.lambda-url.us-east-1.on.aws/".into(),
        mode: HttpWorkerMode::Sync {
            response_timeout: Duration::from_secs(10),
        },
        ..Default::default()
    })
    .queue("default", 4)
    .build()?;
```

HTTP workers and local `Worker` implementations coexist on the same client. The dispatcher doesn't know or care which kind of worker handles a given job kind.

### Callback receiver

The callback receiver is a set of HTTP endpoints added to `awa-ui`:

- `POST /api/callbacks/:callback_id/complete` -- calls `admin::complete_external`
- `POST /api/callbacks/:callback_id/fail` -- calls `admin::fail_external`
- `POST /api/callbacks/:callback_id/heartbeat` -- calls `admin::heartbeat_callback`

These are thin wrappers around the existing admin API. Users who don't run `awa-ui` can mount equivalent handlers in their own HTTP server using the admin functions directly.

## Consequences

### Positive

- **Zero new primitives.** `HttpWorker` is just a `Worker` implementation. No new job states, no schema changes, no migration.
- **Serverless integration without custom glue.** Teams can dispatch to Lambda/Cloud Run/Azure Functions by declaring an `HttpWorkerConfig` rather than writing bespoke callback plumbing.
- **Reuses battle-tested infrastructure.** Timeout rescue, heartbeat extension, HMAC auth, and at-most-once resolution (TLA+ verified) all apply to HTTP workers automatically.
- **Mixed deployment.** Some job kinds run in-process, others dispatch to HTTP endpoints. One Awa client handles both.

### Negative

- **Always-on dispatcher required.** The Awa worker process must be running to claim and dispatch jobs. This is not scale-to-zero; the dispatcher is the orchestrator. For true scale-to-zero, teams need an external trigger (e.g., a cron job or LISTEN/NOTIFY consumer) -- out of scope.
- **`reqwest` dependency.** Large transitive dependency tree (~40 crates). Mitigated by feature gating.
- **Two-leg latency for async mode.** The function must make a callback HTTP request to complete the job, adding network round-trip compared to in-process completion. This is inherent to the serverless model and not an Awa limitation.

## What we explicitly don't build

- **Function deployment or management.** Awa dispatches to URLs, not to cloud-provider APIs. Deploying and scaling the function is the user's responsibility.
- **Scale-to-zero.** The dispatcher must be running. If no jobs are queued, the function is never called, but the Awa process is still alive.
- **Response body parsing for async mode.** The 202 response from the function is acknowledged but not interpreted. All result data flows through the callback payload.
- **Automatic retry at the HTTP level.** If the POST to the function fails (connection error, 5xx), the worker returns `JobError::Retryable` and Awa's standard retry/backoff handles it. No HTTP-level retry loop.

## Prior Art

| System | Pattern | How Awa compares |
|--------|---------|-----------------|
| Step Functions + Lambda | State machine invokes Lambda via task token | `HttpWorker` async mode + callback |
| Temporal activity workers | Activity task dispatched to remote worker | Similar, but Awa is simpler (no workflow engine) |
| Celery + HTTP backend | Custom HTTP transport | Awa's callback auth and timeout rescue are built-in |
| Inngest | Event-driven function execution | Similar model; Awa adds transactional enqueue |
| River (Go) | In-process only | No HTTP dispatch equivalent |
