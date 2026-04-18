use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;

use awa_model::admin;

use crate::cache::CacheError;
use crate::error::ApiError;
use crate::state::AppState;

pub async fn list_queues(
    State(state): State<AppState>,
) -> Result<Json<Vec<admin::QueueOverview>>, ApiError> {
    let pool = state.pool.clone();
    let stats = state
        .cache
        .queues
        .try_get_with((), async {
            admin::queue_overviews(&pool)
                .await
                .map_err(CacheError::from)
        })
        .await?;
    Ok(Json(stats))
}

pub async fn get_queue(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
) -> Result<Json<admin::QueueOverview>, ApiError> {
    let queue = admin::queue_overview(&state.pool, &queue_name).await?;
    match queue {
        Some(queue) => Ok(Json(queue)),
        None => Err(ApiError::not_found(format!(
            "queue '{queue_name}' not found"
        ))),
    }
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
    state.require_writable()?;
    admin::pause_queue(&state.pool, &queue, payload.paused_by.as_deref()).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn resume_queue(
    State(state): State<AppState>,
    Path(queue): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.require_writable()?;
    admin::resume_queue(&state.pool, &queue).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn drain_queue(
    State(state): State<AppState>,
    Path(queue): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.require_writable()?;
    let count = admin::drain_queue(&state.pool, &queue).await?;
    Ok(Json(serde_json::json!({ "drained": count })))
}
