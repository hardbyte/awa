use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

use awa_model::admin;
use awa_model::dlq::DlqMetadata;
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
    /// Populated when the job has been moved to the Dead Letter Queue. The
    /// frontend should send retry/cancel actions through `/api/dlq/*` rather
    /// than `/api/jobs/*` when this is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dlq: Option<DlqMetadata>,
}

impl JobResponse {
    fn from_row(
        row: JobRow,
        queue_descriptors: &HashMap<String, admin::QueueDescriptor>,
        kind_descriptors: &HashMap<String, admin::JobKindDescriptor>,
    ) -> Self {
        Self::from_row_with_dlq(row, queue_descriptors, kind_descriptors, None)
    }

    fn from_row_with_dlq(
        row: JobRow,
        queue_descriptors: &HashMap<String, admin::QueueDescriptor>,
        kind_descriptors: &HashMap<String, admin::JobKindDescriptor>,
        dlq: Option<DlqMetadata>,
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
            dlq,
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
    // Falls back to jobs_dlq so operators inspecting a DLQ'd job via the
    // standard job-detail URL see it instead of a 404. The response surfaces
    // the DLQ metadata under `.dlq` so the frontend can route retry/cancel
    // through the DLQ endpoints.
    let (job, dlq) = admin::get_job_with_source(&state.pool, job_id).await?;
    let queue_descriptors =
        admin::queue_descriptors_for_names(&state.pool, std::slice::from_ref(&job.queue)).await?;
    let kind_descriptors =
        admin::job_kind_descriptors_for_names(&state.pool, std::slice::from_ref(&job.kind)).await?;
    Ok(Json(JobResponse::from_row_with_dlq(
        job,
        &queue_descriptors,
        &kind_descriptors,
        dlq,
    )))
}

pub async fn retry_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<Option<JobResponse>>, ApiError> {
    state.require_writable()?;
    let job = admin::retry(&state.pool, job_id).await?;
    if job.is_some() {
        state.invalidate_dashboard_caches();
    }
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
    if job.is_some() {
        state.invalidate_dashboard_caches();
    }
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
    if !jobs.is_empty() {
        state.invalidate_dashboard_caches();
    }
    Ok(Json(JobResponse::from_rows(&state.pool, jobs).await?))
}

pub async fn bulk_cancel(
    State(state): State<AppState>,
    Json(payload): Json<BulkIdsPayload>,
) -> Result<Json<Vec<JobResponse>>, ApiError> {
    state.require_writable()?;
    let jobs = admin::bulk_cancel(&state.pool, &payload.ids).await?;
    if !jobs.is_empty() {
        state.invalidate_dashboard_caches();
    }
    Ok(Json(JobResponse::from_rows(&state.pool, jobs).await?))
}
