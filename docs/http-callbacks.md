# HTTP Workers and Callback Signatures

Awa has two callback-related surfaces:

- **In-process sequential callbacks**: a Rust or Python handler calls `register_callback()` / `wait_for_callback()` and an external system later resolves that callback through the SDK/admin API.
- **HTTP worker callbacks**: the Rust `http-worker` feature dispatches a job to an HTTP endpoint, parks the job in `waiting_external`, and expects the remote function to call Awa's callback receiver when it is done.

This page documents the second path: `HttpWorker` async mode and the `awa serve` callback receiver.

## End-to-end flow

```text
Awa runtime                  Function endpoint                  awa serve
    |                               |                               |
    | claim job                     |                               |
    | register callback in DB       |                               |
    | POST function URL             |                               |
    |   X-Awa-Signature: <sig>      |                               |
    |   { job_id, kind, args,       |                               |
    |     callback_id,              |                               |
    |     callback_url }            |                               |
    |------------------------------>|                               |
    |                               | work happens                  |
    |                               | POST callback_url             |
    |                               |   X-Awa-Signature: <same sig> |
    |                               |   { payload }                 |
    |                               |------------------------------>|
    |                               |                               | complete_external()
    | job becomes completed         |                               |
```

The worker signs the callback ID before it calls the function. The function does not need the shared secret if it simply forwards the `X-Awa-Signature` header it received from the worker when it calls Awa back.

## Configure the receiver

`awa serve` hosts the callback receiver:

```bash
export AWA_CALLBACK_HMAC_SECRET=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
awa --database-url "$DATABASE_URL" serve --host 0.0.0.0 --port 3000
```

The secret must be 32 bytes encoded as 64 hex characters. When it is set, `awa serve` rejects callback requests that do not include a valid `X-Awa-Signature` header. When it is not set, signature verification is disabled, so the callback receiver must be protected by private networking or an authenticating proxy.

`--read-only` disables callback resolution because callback endpoints mutate job state.

## Configure the worker

The worker and receiver must use the same 32-byte key:

```rust
use awa::{Client, HttpWorkerConfig, HttpWorkerMode, QueueConfig};
use std::time::Duration;

let callback_secret = hex::decode(std::env::var("AWA_CALLBACK_HMAC_SECRET")?)?
    .try_into()
    .map_err(|_| "callback secret must be 32 bytes")?;

let client = Client::builder(pool)
    .http_worker("generate_pdf", HttpWorkerConfig {
        url: "https://pdf-service.example.com/generate".into(),
        mode: HttpWorkerMode::Async,
        callback_base_url: Some("https://awa.example.com".into()),
        callback_timeout: Duration::from_secs(3600),
        hmac_secret: Some(callback_secret),
        ..Default::default()
    })
    .queue("default", QueueConfig {
        max_workers: 4,
        ..Default::default()
    })
    .build()?;
```

`callback_base_url` is the externally reachable base URL for `awa serve`. The runtime builds callback URLs as:

```text
{callback_base_url}{callback_path_prefix}/{callback_id}/complete
```

`callback_path_prefix` defaults to `/api/callbacks`, which matches the built-in `awa serve` route layout. Override it when the callback receiver is mounted somewhere else — for example, a callback-only deployment behind a reverse proxy, or a user-owned API layer that hosts the routes inside a FastAPI / axum application (see [ADR-027](./adr/027-callback-ingress-surface.md)):

```rust
let client = Client::builder(pool)
    .http_worker("generate_pdf", HttpWorkerConfig {
        url: "https://pdf-service.example.com/generate".into(),
        mode: HttpWorkerMode::Async,
        callback_base_url: Some("https://api.example.com".into()),
        callback_path_prefix: Some("/awa-cb".into()),
        ..Default::default()
    })
    /* ... */
    .build()?;
```

That config produces URLs like `https://api.example.com/awa-cb/{callback_id}/complete`. Empty / missing leading slashes are normalized; trailing slashes are stripped.

User-owned receivers should reuse [`awa::callback_contract::callback_url`] to build URLs and [`awa::callback_contract::verify`] to authenticate inbound requests so the wire contract cannot drift.

## Function request contract

In async mode, Awa sends a JSON request like:

```json
{
  "job_id": 123,
  "kind": "generate_pdf",
  "args": { "document_id": "doc_123" },
  "attempt": 1,
  "max_attempts": 25,
  "callback_id": "018f0f69-63c9-7c86-bf2f-9b62d2cda6f4",
  "callback_url": "https://awa.example.com/api/callbacks/018f0f69-63c9-7c86-bf2f-9b62d2cda6f4/complete"
}
```

and, when `hmac_secret` is configured, this header:

```text
X-Awa-Signature: <64-character hex signature>
```

A `2xx` response means the function accepted the job and Awa parks the attempt in `waiting_external`. A `5xx` response is treated as retryable. A `4xx` response is treated as terminal.

## Callback receiver contract

The receiver endpoints are:

```text
POST /api/callbacks/{callback_id}/complete
POST /api/callbacks/{callback_id}/fail
POST /api/callbacks/{callback_id}/heartbeat
```

All three verify `X-Awa-Signature` when `AWA_CALLBACK_HMAC_SECRET` is set. The signature is computed over the callback ID string, not over the request body:

```text
hex(blake3_keyed_hash(secret_32_bytes, callback_id_utf8))
```

Despite the `hmac_secret` option name, the algorithm is BLAKE3 keyed hashing, not RFC HMAC. The option name is retained because the secret is used for the same operational purpose: authenticating callback requests.

Complete a callback:

```bash
curl -X POST "$CALLBACK_URL" \
  -H "Content-Type: application/json" \
  -H "X-Awa-Signature: $AWA_SIGNATURE" \
  -d '{"payload":{"status":"ok","result_url":"s3://bucket/result.pdf"}}'
```

Fail a callback:

```bash
curl -X POST "https://awa.example.com/api/callbacks/$CALLBACK_ID/fail" \
  -H "Content-Type: application/json" \
  -H "X-Awa-Signature: $AWA_SIGNATURE" \
  -d '{"error":"renderer returned invalid PDF"}'
```

Heartbeat a long-running callback:

```bash
curl -X POST "https://awa.example.com/api/callbacks/$CALLBACK_ID/heartbeat" \
  -H "Content-Type: application/json" \
  -H "X-Awa-Signature: $AWA_SIGNATURE" \
  -d '{"timeout_seconds":3600}'
```

The `complete` endpoint completes the job. It does not resume an in-process handler for sequential callback workflows; use the Rust/Python admin API `resume_external` path for that pattern.

## Deploying callback ingress separately

`awa serve` bundles the admin UI, admin REST API, static fallback, and the callback receiver behind a single router with permissive CORS. That is convenient for development but undesirable when callbacks must be externally reachable while the admin surface must stay private.

For that case Awa ships a callback-only receiver as a deployable role (see [ADR-027](./adr/027-callback-ingress-surface.md)):

```text
awa callbacks serve \
  --host 0.0.0.0 --port 4000 \
  --callback-hmac-secret "$AWA_CALLBACK_HMAC_SECRET" \
  --path-prefix /api/callbacks
```

The router:

- Mounts only `POST {prefix}/{callback_id}/{complete,fail,heartbeat}`.
- Does not serve static UI assets or admin REST routes.
- Does not apply permissive CORS.
- Refuses to build against a read-only database (all three routes mutate job state).
- Requires a callback signing secret by default. Pass `--allow-unsigned` only when the receiver lives on a trusted network (mTLS at the load balancer, IP allow-list, private VPC, etc.).

The same router is available as a Rust library for embedding:

```rust
use awa_ui::{callback_router, CallbackAuth, CallbackReceiverConfig};

let router = callback_router(
    pool,
    CallbackReceiverConfig::new(CallbackAuth::Signed(secret)),
)
.await?;
```

Use [`HttpWorkerConfig::callback_path_prefix`](#configuration) on the worker side to match a non-default `--path-prefix` so the URLs the worker hands to your function point at the receiver.

If you want callbacks to land inside your own application (FastAPI, axum, etc.) rather than running Awa's receiver at all, see [`docs/callback-receivers.md`](./callback-receivers.md) for the user-owned API integration pattern.

## Function-side verification

The signature primarily protects the Awa callback receiver from unauthorized completion requests. If the function endpoint is public, you may also verify the worker-to-function request before starting work. In that case the function must know the same 32-byte secret and recompute the BLAKE3 keyed hash over the received `callback_id`.

Python example:

```python
# pip install blake3
import blake3
import hmac


def valid_signature(secret: bytes, callback_id: str, provided: str) -> bool:
    expected = blake3.blake3(callback_id.encode(), key=secret).hexdigest()
    return hmac.compare_digest(expected, provided)
```

Rust example:

```rust
fn valid_signature(secret: &[u8; 32], callback_id: &str, provided: &str) -> bool {
    let expected = blake3::keyed_hash(secret, callback_id.as_bytes());
    blake3::Hash::from_hex(provided).is_ok_and(|hash| expected == hash)
}
```

If the function does not verify the inbound request, keep it behind your normal cloud auth layer and still forward `X-Awa-Signature` to the callback receiver.
