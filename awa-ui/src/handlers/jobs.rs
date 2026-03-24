use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;

use awa_model::admin;
use awa_model::job::{JobRow, JobState};

use crate::error::ApiError;
use crate::state::AppState;

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
    State(state): State<AppState>,
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
    let jobs = admin::list_jobs(&state.pool, &filter).await?;
    Ok(Json(jobs))
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<JobRow>, ApiError> {
    let job = admin::get_job(&state.pool, job_id).await?;
    Ok(Json(job))
}

pub async fn retry_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<Option<JobRow>>, ApiError> {
    state.require_writable()?;
    let job = admin::retry(&state.pool, job_id).await?;
    Ok(Json(job))
}

pub async fn cancel_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<Option<JobRow>>, ApiError> {
    state.require_writable()?;
    let job = admin::cancel(&state.pool, job_id).await?;
    Ok(Json(job))
}

#[derive(Debug, Deserialize)]
pub struct BulkIdsPayload {
    pub ids: Vec<i64>,
}

pub async fn bulk_retry(
    State(state): State<AppState>,
    Json(payload): Json<BulkIdsPayload>,
) -> Result<Json<Vec<JobRow>>, ApiError> {
    state.require_writable()?;
    let jobs = admin::bulk_retry(&state.pool, &payload.ids).await?;
    Ok(Json(jobs))
}

pub async fn bulk_cancel(
    State(state): State<AppState>,
    Json(payload): Json<BulkIdsPayload>,
) -> Result<Json<Vec<JobRow>>, ApiError> {
    state.require_writable()?;
    let jobs = admin::bulk_cancel(&state.pool, &payload.ids).await?;
    Ok(Json(jobs))
}
