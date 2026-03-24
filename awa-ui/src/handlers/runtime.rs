use axum::extract::State;
use axum::Json;

use awa_model::admin;

use crate::error::ApiError;
use crate::state::AppState;

pub async fn get_runtime(
    State(state): State<AppState>,
) -> Result<Json<admin::RuntimeOverview>, ApiError> {
    Ok(Json(admin::runtime_overview(&state.pool).await?))
}

pub async fn list_queue_runtime(
    State(state): State<AppState>,
) -> Result<Json<Vec<admin::QueueRuntimeSummary>>, ApiError> {
    Ok(Json(admin::queue_runtime_summary(&state.pool).await?))
}
