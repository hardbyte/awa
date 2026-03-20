use axum::extract::{Path, State};
use axum::Json;
use sqlx::PgPool;

use awa_model::cron;
use awa_model::job::JobRow;
use awa_model::CronJobRow;

use crate::error::ApiError;

pub async fn list_cron_jobs(State(pool): State<PgPool>) -> Result<Json<Vec<CronJobRow>>, ApiError> {
    let jobs = cron::list_cron_jobs(&pool).await?;
    Ok(Json(jobs))
}

pub async fn trigger_cron_job(
    State(pool): State<PgPool>,
    Path(name): Path<String>,
) -> Result<Json<JobRow>, ApiError> {
    let job = cron::trigger_cron_job(&pool, &name).await?;
    Ok(Json(job))
}
