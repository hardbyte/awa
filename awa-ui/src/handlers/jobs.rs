use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use sqlx::PgPool;

use awa_model::admin;
use awa_model::job::{JobRow, JobState};

use crate::error::ApiError;

#[derive(Debug, Deserialize)]
pub struct ListJobsParams {
    pub state: Option<JobState>,
    pub kind: Option<String>,
    pub queue: Option<String>,
    pub tag: Option<String>,
    pub before_id: Option<i64>,
    pub limit: Option<i64>,
}

pub async fn list_jobs(
    State(pool): State<PgPool>,
    Query(params): Query<ListJobsParams>,
) -> Result<Json<Vec<JobRow>>, ApiError> {
    let filter = admin::ListJobsFilter {
        state: params.state,
        kind: params.kind,
        queue: params.queue,
        tag: params.tag,
        before_id: params.before_id,
        limit: params.limit,
    };
    let jobs = admin::list_jobs(&pool, &filter).await?;
    Ok(Json(jobs))
}

pub async fn get_job(
    State(pool): State<PgPool>,
    Path(job_id): Path<i64>,
) -> Result<Json<JobRow>, ApiError> {
    let job = admin::get_job(&pool, job_id).await?;
    Ok(Json(job))
}

pub async fn retry_job(
    State(pool): State<PgPool>,
    Path(job_id): Path<i64>,
) -> Result<Json<Option<JobRow>>, ApiError> {
    let job = admin::retry(&pool, job_id).await?;
    Ok(Json(job))
}

pub async fn cancel_job(
    State(pool): State<PgPool>,
    Path(job_id): Path<i64>,
) -> Result<Json<Option<JobRow>>, ApiError> {
    let job = admin::cancel(&pool, job_id).await?;
    Ok(Json(job))
}

#[derive(Debug, Deserialize)]
pub struct BulkIdsPayload {
    pub ids: Vec<i64>,
}

pub async fn bulk_retry(
    State(pool): State<PgPool>,
    Json(payload): Json<BulkIdsPayload>,
) -> Result<Json<Vec<JobRow>>, ApiError> {
    let jobs = admin::bulk_retry(&pool, &payload.ids).await?;
    Ok(Json(jobs))
}

pub async fn bulk_cancel(
    State(pool): State<PgPool>,
    Json(payload): Json<BulkIdsPayload>,
) -> Result<Json<Vec<JobRow>>, ApiError> {
    let jobs = admin::bulk_cancel(&pool, &payload.ids).await?;
    Ok(Json(jobs))
}
