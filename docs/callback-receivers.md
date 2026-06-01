# User-owned callback receivers

Awa supports three ways to host the HTTP callback ingress surface:

1. **Bundled with the admin UI** — the default. `awa serve` exposes the
   admin UI plus the three callback routes from the same router.
2. **Standalone receiver** — `awa callbacks serve` runs a router that
   mounts only the callback ingress endpoints. Use this when callbacks
   must be reachable from outside the operator network but the admin
   surface must remain private. See [`docs/http-callbacks.md`](./http-callbacks.md).
3. **User-owned API layer** — mount the callback ingress routes inside
   your own application (FastAPI / Starlette / Flask / Django / axum /
   actix). This page covers option 3.

The on-wire contract is identical across all three options:

- Routes: `POST {prefix}/{callback_id}/{complete,fail,heartbeat}`.
- Signature: BLAKE3 keyed-hash of the callback id, lowercase hex, in the
  `X-Awa-Signature` header.
- Payloads: see [`docs/http-callbacks.md`](./http-callbacks.md#callback-receiver-contract).

When you host the routes yourself, **reuse the shared signing and URL
helpers** from `awa::callback_contract` (Rust) or `awa.callback_contract`
(Python) so your implementation cannot drift from the worker's. The
helpers are exported specifically so this is a one-line dependency, not
a copy-paste of the algorithm.

## Security

The callback receiver routes mutate job state. Specifically, `complete`
and `fail` are terminal — once they succeed, the job leaves
`waiting_external`. A receiver that does not verify `X-Awa-Signature`
must be protected another way (mTLS at the load balancer, IP allow-list,
private VPC). See [ADR-027](./adr/027-callback-ingress.md) for the
deployable-role model.

## Custom axum receiver (Rust)

```rust
use awa::callback_contract;
use awa_model::admin;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Clone)]
struct CallbackState {
    pool: PgPool,
    secret: [u8; 32],
}

#[derive(Deserialize)]
struct CompletePayload {
    #[serde(default)]
    payload: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct CallbackResponse {
    id: i64,
    state: String,
}

async fn complete(
    State(state): State<Arc<CallbackState>>,
    headers: HeaderMap,
    Path(callback_id): Path<String>,
    Json(body): Json<CompletePayload>,
) -> Result<Json<CallbackResponse>, (StatusCode, String)> {
    let signature = headers
        .get(callback_contract::SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or((StatusCode::UNAUTHORIZED, "missing signature".into()))?;
    if !callback_contract::verify(&state.secret, &callback_id, signature) {
        return Err((StatusCode::UNAUTHORIZED, "invalid signature".into()));
    }

    let uuid = uuid::Uuid::parse_str(&callback_id)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let job = admin::complete_external(&state.pool, uuid, body.payload, None)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(CallbackResponse {
        id: job.id,
        state: format!("{:?}", job.state),
    }))
}

pub fn callback_routes(state: Arc<CallbackState>) -> Router {
    Router::new()
        .route(
            "/api/callbacks/{callback_id}/complete",
            post(complete),
        )
        // fail/heartbeat follow the same pattern — see
        // awa-ui/src/handlers/callbacks.rs for the canonical implementation.
        .with_state(state)
}
```

For most users the built-in receiver in `awa::callback_router` (PR #279)
covers this case. Mount the routes yourself only when you need them
*inside* an existing axum application.

## Custom FastAPI receiver (Python)

```python
import os
from typing import Any

import uuid
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from awa import AsyncClient, callback_contract

CALLBACK_SECRET = bytes.fromhex(os.environ["AWA_CALLBACK_HMAC_SECRET"])

router = APIRouter(prefix=callback_contract.DEFAULT_PATH_PREFIX)


def verify_signature(
    callback_id: str,
    x_awa_signature: str | None = Header(default=None),
) -> None:
    if x_awa_signature is None:
        raise HTTPException(
            status_code=401,
            detail=f"missing {callback_contract.SIGNATURE_HEADER} header",
        )
    if not callback_contract.verify(CALLBACK_SECRET, callback_id, x_awa_signature):
        raise HTTPException(status_code=401, detail="invalid callback signature")


class CompletePayload(BaseModel):
    payload: dict[str, Any] | None = None


class FailPayload(BaseModel):
    error: str


class HeartbeatPayload(BaseModel):
    timeout_seconds: float = callback_contract.DEFAULT_HEARTBEAT_TIMEOUT_SECS


def get_client(request: Request) -> AsyncClient:
    # AsyncClient is constructed in your application's lifespan and stashed on
    # app.state — see awa-python's getting-started guide.
    return request.app.state.awa


@router.post("/{callback_id}/complete")
async def complete(
    callback_id: str,
    body: CompletePayload,
    client: AsyncClient = Depends(get_client),
    _: None = Depends(verify_signature),
) -> dict[str, Any]:
    job = await client.complete_external(callback_id, body.payload)
    return {"id": job.id, "state": str(job.state)}


@router.post("/{callback_id}/fail")
async def fail(
    callback_id: str,
    body: FailPayload,
    client: AsyncClient = Depends(get_client),
    _: None = Depends(verify_signature),
) -> dict[str, Any]:
    job = await client.fail_external(callback_id, body.error)
    return {"id": job.id, "state": str(job.state)}


@router.post("/{callback_id}/heartbeat")
async def heartbeat(
    callback_id: str,
    body: HeartbeatPayload,
    client: AsyncClient = Depends(get_client),
    _: None = Depends(verify_signature),
) -> dict[str, Any]:
    if body.timeout_seconds < 0 or body.timeout_seconds != body.timeout_seconds:
        raise HTTPException(
            status_code=400,
            detail="timeout_seconds must be a finite, non-negative number",
        )
    job = await client.heartbeat_callback(callback_id, body.timeout_seconds)
    return {"id": job.id, "state": str(job.state)}


app = FastAPI()
app.include_router(router)
```

The `awa.callback_contract` module wraps the same Rust signing
implementation the worker uses, so a signature this verifier accepts is
bit-for-bit identical to a signature `awa serve` would accept. There is
a parity test (`awa-python/tests/test_callback_contract.py`) that pins
a known BLAKE3 vector across both languages.

## Configuring the worker to point at your receiver

Whichever option you pick, the worker side needs to know the full
callback URL. Set [`HttpWorkerConfig::callback_base_url`](./http-callbacks.md)
to your receiver's externally-reachable base URL, and override
`callback_path_prefix` if your routes are mounted somewhere other than
`/api/callbacks`:

```rust
HttpWorkerConfig {
    callback_base_url: Some("https://api.example.com".into()),
    callback_path_prefix: Some("/api/v1/awa-cb".into()),
    hmac_secret: Some(callback_secret),
    ..Default::default()
}
```

The worker uses `awa::callback_contract::callback_url` internally to
build the URL — the same helper your receiver can use to compute
URLs for outbound retries or diagnostics.
