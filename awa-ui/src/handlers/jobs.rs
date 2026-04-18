use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

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
    pub queue_descriptor: Option<admin::QueueDescriptor>,
    pub kind_descriptor: Option<admin::JobKindDescriptor>,
}

impl JobResponse {
    fn from_row(
        row: JobRow,
        queue_descriptors: &HashMap<String, admin::QueueDescriptor>,
        kind_descriptors: &HashMap<String, admin::JobKindDescriptor>,
    ) -> Self {
        let original_priority = row
            .metadata
            .get("_awa_original_priority")
            .and_then(|v| v.as_i64())
            .map(|v| v as i16)
            .unwrap_or(row.priority);
        Self {
            queue_descriptor: queue_descriptors.get(&row.queue).cloned(),
            kind_descriptor: kind_descriptors.get(&row.kind).cloned(),
            row,
            original_priority,
        }
    }

    async fn from_rows(
        pool: &sqlx::PgPool,
        rows: Vec<JobRow>,
    ) -> Result<Vec<Self>, awa_model::AwaError> {
        let queue_names: Vec<String> = rows
            .iter()
            .map(|row| row.queue.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let kind_names: Vec<String> = rows
            .iter()
            .map(|row| row.kind.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        let queue_descriptors = admin::queue_descriptors_for_names(pool, &queue_names).await?;
        let kind_descriptors = admin::job_kind_descriptors_for_names(pool, &kind_names).await?;

        Ok(rows
            .into_iter()
            .map(|row| Self::from_row(row, &queue_descriptors, &kind_descriptors))
            .collect())
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
    Ok(Json(JobResponse::from_rows(&state.pool, jobs).await?))
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<JobResponse>, ApiError> {
    let job = admin::get_job(&state.pool, job_id).await?;
    let mut rows = JobResponse::from_rows(&state.pool, vec![job]).await?;
    Ok(Json(rows.remove(0)))
}

pub async fn retry_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<Option<JobResponse>>, ApiError> {
    state.require_writable()?;
    let job = admin::retry(&state.pool, job_id).await?;
    match job {
        Some(job) => {
            let mut rows = JobResponse::from_rows(&state.pool, vec![job]).await?;
            Ok(Json(Some(rows.remove(0))))
        }
        None => Ok(Json(None)),
    }
}

pub async fn cancel_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<Option<JobResponse>>, ApiError> {
    state.require_writable()?;
    let job = admin::cancel(&state.pool, job_id).await?;
    match job {
        Some(job) => {
            let mut rows = JobResponse::from_rows(&state.pool, vec![job]).await?;
            Ok(Json(Some(rows.remove(0))))
        }
        None => Ok(Json(None)),
    }
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
    Ok(Json(JobResponse::from_rows(&state.pool, jobs).await?))
}

pub async fn bulk_cancel(
    State(state): State<AppState>,
    Json(payload): Json<BulkIdsPayload>,
) -> Result<Json<Vec<JobResponse>>, ApiError> {
    state.require_writable()?;
    let jobs = admin::bulk_cancel(&state.pool, &payload.ids).await?;
    Ok(Json(JobResponse::from_rows(&state.pool, jobs).await?))
}
