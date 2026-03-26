use axum::extract::State;
use axum::Json;

use awa_model::admin;

use crate::error::ApiError;
use crate::state::AppState;

pub async fn get_runtime(
    State(state): State<AppState>,
) -> Result<Json<admin::RuntimeOverview>, ApiError> {
    let pool = state.pool.clone();
    let overview = state
        .cache
        .get_or_fetch("runtime", || admin::runtime_overview(&pool))
        .await?;
    Ok(Json(overview))
}

pub async fn list_queue_runtime(
    State(state): State<AppState>,
) -> Result<Json<Vec<admin::QueueRuntimeSummary>>, ApiError> {
    let pool = state.pool.clone();
    let summary = state
        .cache
        .get_or_fetch("queue-runtime", || admin::queue_runtime_summary(&pool))
        .await?;
    Ok(Json(summary))
}
