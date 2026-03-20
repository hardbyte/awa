use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;
use sqlx::PgPool;

use awa_model::admin;

use crate::error::ApiError;

pub async fn list_queues(
    State(pool): State<PgPool>,
) -> Result<Json<Vec<admin::QueueStats>>, ApiError> {
    let stats = admin::queue_stats(&pool).await?;
    Ok(Json(stats))
}

#[derive(Debug, Deserialize)]
pub struct PausePayload {
    pub paused_by: Option<String>,
}

pub async fn pause_queue(
    State(pool): State<PgPool>,
    Path(queue): Path<String>,
    Json(payload): Json<PausePayload>,
) -> Result<Json<serde_json::Value>, ApiError> {
    admin::pause_queue(&pool, &queue, payload.paused_by.as_deref()).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn resume_queue(
    State(pool): State<PgPool>,
    Path(queue): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    admin::resume_queue(&pool, &queue).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn drain_queue(
    State(pool): State<PgPool>,
    Path(queue): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let count = admin::drain_queue(&pool, &queue).await?;
    Ok(Json(serde_json::json!({ "drained": count })))
}
