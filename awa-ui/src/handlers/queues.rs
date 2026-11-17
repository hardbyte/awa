use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

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
    state.invalidate_dashboard_caches();
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn resume_queue(
    State(state): State<AppState>,
    Path(queue): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.require_writable()?;
    admin::resume_queue(&state.pool, &queue).await?;
    state.invalidate_dashboard_caches();
    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Debug, Deserialize)]
pub struct DrainParams {
    /// Run one batch of up to this many jobs, resuming at `stage` and
    /// `after_job_id`. Without it, the server drains in batches until no
    /// matching job remains.
    pub batch_size: Option<i64>,
    #[serde(default)]
    pub stage: u8,
    #[serde(default)]
    pub after_job_id: i64,
}

#[derive(Debug, Serialize)]
pub struct DrainResponse {
    pub drained: u64,
    #[serde(flatten)]
    pub bulk: crate::handlers::dlq::BulkResponse,
}

pub async fn drain_queue(
    State(state): State<AppState>,
    Path(queue): Path<String>,
    Query(params): Query<DrainParams>,
) -> Result<Json<DrainResponse>, ApiError> {
    state.require_writable()?;
    let cursor = awa_model::BulkCursor {
        stage: params.stage,
        after_job_id: params.after_job_id,
    };
    let bulk =
        crate::handlers::dlq::run_bulk_route(params.batch_size, Some(cursor), |size, cursor| {
            admin::drain_queue_batch(&state.pool, &queue, size, cursor)
        })
        .await?;
    if bulk.count > 0 {
        state.invalidate_dashboard_caches();
    }
    Ok(Json(DrainResponse {
        drained: bulk.count,
        bulk,
    }))
}
