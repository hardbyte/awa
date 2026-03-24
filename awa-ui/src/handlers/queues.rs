use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;

use awa_model::admin;

use crate::error::ApiError;
use crate::state::AppState;

pub async fn list_queues(
    State(state): State<AppState>,
) -> Result<Json<Vec<admin::QueueStats>>, ApiError> {
    let stats = admin::queue_stats(&state.pool).await?;
    Ok(Json(stats))
}

#[derive(Debug, Deserialize)]
pub struct PausePayload {
    pub paused_by: Option<String>,
}

pub async fn pause_queue(
    State(state): State<AppState>,
    Path(queue): Path<String>,
    Json(payload): Json<PausePayload>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.require_writable().await?;
    admin::pause_queue(&state.pool, &queue, payload.paused_by.as_deref()).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn resume_queue(
    State(state): State<AppState>,
    Path(queue): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.require_writable().await?;
    admin::resume_queue(&state.pool, &queue).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn drain_queue(
    State(state): State<AppState>,
    Path(queue): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.require_writable().await?;
    let count = admin::drain_queue(&state.pool, &queue).await?;
    Ok(Json(serde_json::json!({ "drained": count })))
}
