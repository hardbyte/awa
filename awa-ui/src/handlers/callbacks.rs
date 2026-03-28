//! Callback receiver endpoints for HTTP workers and external systems.
//!
//! These endpoints are thin wrappers around the admin callback functions,
//! enabling serverless functions to resolve callbacks via HTTP.

use axum::extract::{Path, State};
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

/// POST /api/callbacks/:callback_id/complete
///
/// Completes a waiting job with an optional payload.
pub async fn complete_callback(
    State(state): State<AppState>,
    Path(callback_id): Path<String>,
    Json(body): Json<CompletePayload>,
) -> Result<impl IntoResponse, ApiError> {
    state.require_writable()?;
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
    Path(callback_id): Path<String>,
    Json(body): Json<FailPayload>,
) -> Result<impl IntoResponse, ApiError> {
    state.require_writable()?;
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
    Path(callback_id): Path<String>,
    Json(body): Json<HeartbeatPayload>,
) -> Result<impl IntoResponse, ApiError> {
    state.require_writable()?;
    let uuid = uuid::Uuid::parse_str(&callback_id)
        .map_err(|e| ApiError::Awa(awa_model::AwaError::Validation(e.to_string())))?;

    let timeout = std::time::Duration::from_secs_f64(body.timeout_seconds);
    let job = awa_model::admin::heartbeat_callback(&state.pool, uuid, timeout).await?;
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "id": job.id,
            "state": format!("{:?}", job.state),
        })),
    ))
}
