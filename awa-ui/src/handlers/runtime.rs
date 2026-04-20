use axum::extract::State;
use axum::Json;

use awa_model::admin;
use awa_model::storage;

use crate::cache::CacheError;
use crate::error::ApiError;
use crate::state::AppState;

pub async fn get_runtime(
    State(state): State<AppState>,
) -> Result<Json<admin::RuntimeOverview>, ApiError> {
    let pool = state.pool.clone();
    let overview = state
        .cache
        .runtime
        .try_get_with((), async {
            admin::runtime_overview(&pool)
                .await
                .map_err(CacheError::from)
        })
        .await?;
    Ok(Json(overview))
}

pub async fn list_queue_runtime(
    State(state): State<AppState>,
) -> Result<Json<Vec<admin::QueueRuntimeSummary>>, ApiError> {
    let pool = state.pool.clone();
    let summary = state
        .cache
        .queue_runtime
        .try_get_with((), async {
            admin::queue_runtime_summary(&pool)
                .await
                .map_err(CacheError::from)
        })
        .await?;
    Ok(Json(summary))
}

pub async fn get_storage(
    State(state): State<AppState>,
) -> Result<Json<storage::StorageStatusReport>, ApiError> {
    let pool = state.pool.clone();
    let report = state
        .cache
        .storage
        .try_get_with((), async {
            storage::status_report(&pool)
                .await
                .map_err(CacheError::from)
        })
        .await?;
    Ok(Json(report))
}
