use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use awa_model::admin;
use awa_model::job::{JobRow, JobState};

use crate::error::ApiError;
use crate::state::AppState;

/// Default priority aging interval in seconds, matching the maintenance task default.
const DEFAULT_AGING_INTERVAL_SECS: f64 = 60.0;

/// API response for a job. Separates the API contract from the database row.
/// Includes computed fields that are not stored in the database.
#[derive(Debug, Serialize)]
pub struct JobResponse {
    #[serde(flatten)]
    pub row: JobRow,
    /// The priority assigned at enqueue time, reconstructed from the current
    /// (possibly aged) priority and the time the job has been waiting.
    /// Matches `priority` for jobs that haven't been aged.
    pub original_priority: i16,
}

impl JobResponse {
    fn from_row(row: JobRow, aging_interval_secs: f64) -> Self {
        let original_priority = if row.state == JobState::Available && aging_interval_secs > 0.0 {
            let wait_secs = (Utc::now() - row.run_at).num_seconds().max(0) as f64;
            let levels_aged = (wait_secs / aging_interval_secs).floor() as i16;
            (row.priority + levels_aged).min(4)
        } else {
            row.priority
        };
        Self {
            row,
            original_priority,
        }
    }

    fn from_rows(rows: Vec<JobRow>, aging_interval_secs: f64) -> Vec<Self> {
        rows.into_iter()
            .map(|r| Self::from_row(r, aging_interval_secs))
            .collect()
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
    Ok(Json(JobResponse::from_rows(
        jobs,
        DEFAULT_AGING_INTERVAL_SECS,
    )))
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<JobResponse>, ApiError> {
    let job = admin::get_job(&state.pool, job_id).await?;
    Ok(Json(JobResponse::from_row(
        job,
        DEFAULT_AGING_INTERVAL_SECS,
    )))
}

pub async fn retry_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<Option<JobResponse>>, ApiError> {
    state.require_writable()?;
    let job = admin::retry(&state.pool, job_id).await?;
    Ok(Json(job.map(|j| {
        JobResponse::from_row(j, DEFAULT_AGING_INTERVAL_SECS)
    })))
}

pub async fn cancel_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<Option<JobResponse>>, ApiError> {
    state.require_writable()?;
    let job = admin::cancel(&state.pool, job_id).await?;
    Ok(Json(job.map(|j| {
        JobResponse::from_row(j, DEFAULT_AGING_INTERVAL_SECS)
    })))
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
    Ok(Json(JobResponse::from_rows(
        jobs,
        DEFAULT_AGING_INTERVAL_SECS,
    )))
}

pub async fn bulk_cancel(
    State(state): State<AppState>,
    Json(payload): Json<BulkIdsPayload>,
) -> Result<Json<Vec<JobResponse>>, ApiError> {
    state.require_writable()?;
    let jobs = admin::bulk_cancel(&state.pool, &payload.ids).await?;
    Ok(Json(JobResponse::from_rows(
        jobs,
        DEFAULT_AGING_INTERVAL_SECS,
    )))
}
