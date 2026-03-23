use axum::extract::State;
use axum::Json;
use sqlx::PgPool;

use awa_model::admin;

use crate::error::ApiError;

pub async fn get_runtime(
    State(pool): State<PgPool>,
) -> Result<Json<admin::RuntimeOverview>, ApiError> {
    Ok(Json(admin::runtime_overview(&pool).await?))
}

pub async fn list_queue_runtime(
    State(pool): State<PgPool>,
) -> Result<Json<Vec<admin::QueueRuntimeSummary>>, ApiError> {
    Ok(Json(admin::queue_runtime_summary(&pool).await?))
}
