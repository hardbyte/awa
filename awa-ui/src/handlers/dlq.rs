use axum::extract::{Path, RawQuery, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use awa_model::dlq::{self, DlqRow, RetryFromDlqOpts};
use awa_model::{DlqPage, JobFilter};

use crate::error::ApiError;
use crate::state::AppState;

/// `GET /api/dlq`: the jobs endpoint's filter parameters, plus a
/// `(before_dlq_at, before_id)` cursor matching the newest-first order.
pub async fn list_dlq(
    State(state): State<AppState>,
    RawQuery(raw): RawQuery,
) -> Result<Json<Vec<DlqRow>>, ApiError> {
    let query = super::filter_params::parse(raw.as_deref())?;
    let page = DlqPage {
        before_dlq_at: query.before_dlq_at,
        before_id: query.before_id,
        limit: query.limit.unwrap_or(awa_model::filter::DEFAULT_PAGE_LIMIT),
    };
    let rows = dlq::find_dlq(&state.pool, &query.filter, page).await?;
    Ok(Json(rows))
}

pub async fn get_dlq_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<Option<DlqRow>>, ApiError> {
    let row = dlq::get_dlq_job(&state.pool, job_id).await?;
    Ok(Json(row))
}

#[derive(Debug, Serialize)]
pub struct DlqDepthResponse {
    pub total: i64,
    pub by_queue: Vec<DlqDepthByQueue>,
}

#[derive(Debug, Serialize)]
pub struct DlqDepthByQueue {
    pub queue: String,
    pub count: i64,
}

pub async fn dlq_depth(State(state): State<AppState>) -> Result<Json<DlqDepthResponse>, ApiError> {
    let total = dlq::dlq_depth(&state.pool, None).await?;
    let by_queue_raw = dlq::dlq_depth_by_queue(&state.pool).await?;
    let by_queue = by_queue_raw
        .into_iter()
        .map(|(queue, count)| DlqDepthByQueue { queue, count })
        .collect();
    Ok(Json(DlqDepthResponse { total, by_queue }))
}

#[derive(Debug, Deserialize)]
pub struct RetryPayload {
    #[serde(default)]
    pub run_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub priority: Option<i16>,
    #[serde(default)]
    pub queue: Option<String>,
}

pub async fn retry_dlq_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
    payload: Option<Json<RetryPayload>>,
) -> Result<Json<Option<awa_model::JobRow>>, ApiError> {
    state.require_writable()?;
    let opts = match payload {
        Some(Json(p)) => RetryFromDlqOpts {
            run_at: p.run_at,
            priority: p.priority,
            queue: p.queue,
        },
        None => RetryFromDlqOpts::default(),
    };
    // Capture the source queue before retry runs — `opts.queue` may
    // re-route the job to a different destination, in which case the
    // returned JobRow's `queue` is the destination, not the queue the
    // DLQ row came from. The metric tracks DLQ outflow per source
    // queue, matching `record_dlq_purged`'s pattern below.
    let source_queue = dlq::get_dlq_job(&state.pool, job_id)
        .await?
        .map(|row| row.job.queue);
    let job = dlq::retry_from_dlq(&state.pool, job_id, &opts).await?;
    if job.is_some() {
        state.metrics.record_dlq_retried(source_queue.as_deref(), 1);
    }
    Ok(Json(job))
}

#[derive(Debug, Deserialize)]
pub struct BulkFilterPayload {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub queue: Option<String>,
    #[serde(default)]
    pub tag: Option<String>,
    /// All of these tags, alongside `tag`.
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub ids: Option<Vec<i64>>,
    #[serde(default)]
    pub created_at_gte: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub created_at_lt: Option<chrono::DateTime<chrono::Utc>>,
    /// Explicit opt-in to act on every row in the DLQ when no filter is
    /// provided. The model layer rejects empty filters otherwise.
    #[serde(default)]
    pub all: bool,
}

impl BulkFilterPayload {
    fn filter(&self) -> JobFilter {
        JobFilter {
            kind: self.kind.clone(),
            queue: self.queue.clone(),
            ids: self.ids.clone(),
            created_at_gte: self.created_at_gte,
            created_at_lt: self.created_at_lt,
            ..Default::default()
        }
        .tags(self.tag.clone())
        .tags(self.tags.clone())
    }
}

#[derive(Debug, Serialize)]
pub struct CountResponse {
    pub count: u64,
}

pub async fn bulk_retry_dlq(
    State(state): State<AppState>,
    Json(payload): Json<BulkFilterPayload>,
) -> Result<Json<CountResponse>, ApiError> {
    state.require_writable()?;
    let queue_attr = payload.queue.clone();
    let count =
        dlq::bulk_retry_from_dlq_matching(&state.pool, &payload.filter(), payload.all).await?;
    if count > 0 {
        state
            .metrics
            .record_dlq_retried(queue_attr.as_deref(), count);
    }
    Ok(Json(CountResponse { count }))
}

pub async fn purge_dlq_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<CountResponse>, ApiError> {
    state.require_writable()?;
    // Look up the queue before deleting so the purge counter carries the same
    // `awa.job.queue` attribute the retention-sweep counter uses.
    let queue = dlq::get_dlq_job(&state.pool, job_id)
        .await?
        .map(|row| row.job.queue);
    let deleted = dlq::purge_dlq_job(&state.pool, job_id).await?;
    let count = if deleted { 1 } else { 0 };
    if count > 0 {
        state.metrics.record_dlq_purged(queue.as_deref(), count);
    }
    Ok(Json(CountResponse { count }))
}

pub async fn bulk_purge_dlq(
    State(state): State<AppState>,
    Json(payload): Json<BulkFilterPayload>,
) -> Result<Json<CountResponse>, ApiError> {
    state.require_writable()?;
    let queue_attr = payload.queue.clone();
    let count = dlq::purge_dlq_matching(&state.pool, &payload.filter(), payload.all).await?;
    if count > 0 {
        state
            .metrics
            .record_dlq_purged(queue_attr.as_deref(), count);
    }
    Ok(Json(CountResponse { count }))
}

#[derive(Debug, Deserialize)]
pub struct BulkMovePayload {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub queue: Option<String>,
    #[serde(default = "default_move_reason")]
    pub reason: String,
    #[serde(default)]
    pub all: bool,
}

fn default_move_reason() -> String {
    "ui_bulk_move".to_string()
}

/// Bulk-move failed jobs from terminal storage into the DLQ.
///
/// Requires at least one of `kind` or `queue` unless `all=true` is provided
/// explicitly.
pub async fn bulk_move_failed(
    State(state): State<AppState>,
    Json(payload): Json<BulkMovePayload>,
) -> Result<Json<CountResponse>, ApiError> {
    state.require_writable()?;
    let count = dlq::bulk_move_failed_to_dlq(
        &state.pool,
        payload.kind.as_deref(),
        payload.queue.as_deref(),
        &payload.reason,
        payload.all,
    )
    .await?;
    if count > 0 {
        state.metrics.record_dlq_moved_bulk(
            payload.kind.as_deref(),
            payload.queue.as_deref(),
            &payload.reason,
            count,
        );
    }
    Ok(Json(CountResponse { count }))
}
