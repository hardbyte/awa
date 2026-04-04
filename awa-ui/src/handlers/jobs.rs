use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use awa_model::admin;
use awa_model::job::{JobRow, JobState};

use crate::error::ApiError;
use crate::state::AppState;

/// API response for a job. Separates the API contract from the database row.
/// Includes computed fields that are not stored in the database.
#[derive(Debug, Serialize)]
pub struct JobResponse {
    #[serde(flatten)]
    pub row: JobRow,
    /// The priority assigned at enqueue time, if the job has been aged by the
    /// maintenance leader. Read from `metadata._awa_original_priority`.
    /// Equals `priority` when the job has not been aged.
    pub original_priority: i16,
}

impl JobResponse {
    fn from_row(row: JobRow) -> Self {
        let original_priority = row
            .metadata
            .get("_awa_original_priority")
            .and_then(|v| v.as_i64())
            .map(|v| v as i16)
            .unwrap_or(row.priority);
        Self {
            row,
            original_priority,
        }
    }

    fn from_rows(rows: Vec<JobRow>) -> Vec<Self> {
        rows.into_iter().map(Self::from_row).collect()
    }
}

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
) -> Result<Json<Vec<JobResponse>>, ApiError> {
    let filter = admin::ListJobsFilter {
        state: params.state,
        kind: params.kind,
        queue: params.queue,
        tag: params.tag,
        before_id: params.before_id,
        limit: params.limit,
    };
    let jobs = admin::list_jobs(&state.pool, &filter).await?;
    Ok(Json(JobResponse::from_rows(jobs)))
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<JobResponse>, ApiError> {
    let job = admin::get_job(&state.pool, job_id).await?;
    Ok(Json(JobResponse::from_row(job)))
}

pub async fn retry_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<Option<JobResponse>>, ApiError> {
    state.require_writable()?;
    let job = admin::retry(&state.pool, job_id).await?;
    Ok(Json(job.map(JobResponse::from_row)))
}

pub async fn cancel_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<Option<JobResponse>>, ApiError> {
    state.require_writable()?;
    let job = admin::cancel(&state.pool, job_id).await?;
    Ok(Json(job.map(JobResponse::from_row)))
}

#[derive(Debug, Deserialize)]
pub struct BulkIdsPayload {
    pub ids: Vec<i64>,
}

pub async fn bulk_retry(
    State(state): State<AppState>,
    Json(payload): Json<BulkIdsPayload>,
) -> Result<Json<Vec<JobResponse>>, ApiError> {
    state.require_writable()?;
    let jobs = admin::bulk_retry(&state.pool, &payload.ids).await?;
    Ok(Json(JobResponse::from_rows(jobs)))
}

pub async fn bulk_cancel(
    State(state): State<AppState>,
    Json(payload): Json<BulkIdsPayload>,
) -> Result<Json<Vec<JobResponse>>, ApiError> {
    state.require_writable()?;
    let jobs = admin::bulk_cancel(&state.pool, &payload.ids).await?;
    Ok(Json(JobResponse::from_rows(jobs)))
}
