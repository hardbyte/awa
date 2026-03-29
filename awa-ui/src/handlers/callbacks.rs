//! Callback receiver endpoints for HTTP workers and external systems.
//!
//! These endpoints are thin wrappers around the admin callback functions,
//! enabling serverless functions to resolve callbacks via HTTP.

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;

use crate::error::ApiError;
use crate::state::AppState;

#[derive(Deserialize, Default)]
pub struct CompletePayload {
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub struct FailPayload {
    pub error: String,
}

#[derive(Deserialize, Default)]
pub struct HeartbeatPayload {
    /// Timeout in seconds. Defaults to 3600 (1 hour).
    #[serde(default = "default_heartbeat_timeout")]
    pub timeout_seconds: f64,
}

fn default_heartbeat_timeout() -> f64 {
    3600.0
}

fn verify_signature(
    headers: &HeaderMap,
    callback_id: &str,
    state: &AppState,
) -> Result<(), ApiError> {
    let Some(secret) = state.callback_hmac_secret else {
        return Ok(());
    };

    let provided = headers
        .get("X-Awa-Signature")
        .ok_or_else(|| ApiError::unauthorized("missing X-Awa-Signature header"))?
        .to_str()
        .map_err(|_| ApiError::unauthorized("invalid X-Awa-Signature header"))?;

    let expected = blake3::keyed_hash(&secret, callback_id.as_bytes());
    if expected.to_hex().as_str() == provided {
        Ok(())
    } else {
        Err(ApiError::unauthorized("invalid callback signature"))
    }
}

fn parse_timeout(timeout_seconds: f64) -> Result<std::time::Duration, ApiError> {
    if !timeout_seconds.is_finite() || timeout_seconds.is_sign_negative() {
        return Err(ApiError::Awa(awa_model::AwaError::Validation(
            "timeout_seconds must be a finite, non-negative number".into(),
        )));
    }

    Ok(std::time::Duration::from_secs_f64(timeout_seconds))
}

/// POST /api/callbacks/:callback_id/complete
///
/// Completes a waiting job with an optional payload.
pub async fn complete_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(callback_id): Path<String>,
    Json(body): Json<CompletePayload>,
) -> Result<impl IntoResponse, ApiError> {
    state.require_writable()?;
    verify_signature(&headers, &callback_id, &state)?;
    let uuid = uuid::Uuid::parse_str(&callback_id)
        .map_err(|e| ApiError::Awa(awa_model::AwaError::Validation(e.to_string())))?;

    let job = awa_model::admin::complete_external(&state.pool, uuid, body.payload, None).await?;
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "id": job.id,
            "state": format!("{:?}", job.state),
        })),
    ))
}

/// POST /api/callbacks/:callback_id/fail
///
/// Fails a waiting job with an error message.
pub async fn fail_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(callback_id): Path<String>,
    Json(body): Json<FailPayload>,
) -> Result<impl IntoResponse, ApiError> {
    state.require_writable()?;
    verify_signature(&headers, &callback_id, &state)?;
    let uuid = uuid::Uuid::parse_str(&callback_id)
        .map_err(|e| ApiError::Awa(awa_model::AwaError::Validation(e.to_string())))?;

    let job = awa_model::admin::fail_external(&state.pool, uuid, &body.error, None).await?;
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "id": job.id,
            "state": format!("{:?}", job.state),
        })),
    ))
}

/// POST /api/callbacks/:callback_id/heartbeat
///
/// Extends the callback timeout for a long-running operation.
pub async fn heartbeat_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(callback_id): Path<String>,
    Json(body): Json<HeartbeatPayload>,
) -> Result<impl IntoResponse, ApiError> {
    state.require_writable()?;
    verify_signature(&headers, &callback_id, &state)?;
    let uuid = uuid::Uuid::parse_str(&callback_id)
        .map_err(|e| ApiError::Awa(awa_model::AwaError::Validation(e.to_string())))?;

    let timeout = parse_timeout(body.timeout_seconds)?;
    let job = awa_model::admin::heartbeat_callback(&state.pool, uuid, timeout).await?;
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "id": job.id,
            "state": format!("{:?}", job.state),
        })),
    ))
}
