use axum::extract::{Path, RawQuery, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use awa_model::dlq::{self, DlqRow, RetryFromDlqOpts};
use awa_model::{AwaError, DlqPage, JobFilter};

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
        state.invalidate_dashboard_caches();
        state.metrics.record_dlq_retried(source_queue.as_deref(), 1);
    }
    Ok(Json(job))
}

/// The 0.6 body of `POST /api/dlq/bulk-retry` and `/bulk-purge`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BulkFilterPayload {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub queue: Option<String>,
    #[serde(default)]
    pub tag: Option<String>,
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
            ..Default::default()
        }
        .tags(self.tag.clone())
    }
}

/// The body of `POST /api/dlq/retry-matching` and `/purge-matching`: the full
/// job filter (ADR-049). These are new paths rather than a new body on the
/// 0.6 ones: a 0.6 server ignores fields it doesn't know, so any body it can
/// reach is read as its flat fields, and `all: true` beside a filter it
/// ignored would act on the whole DLQ. A 0.6 server answers these paths with
/// 404 instead.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchingPayload {
    pub filter: JobFilter,
    /// Act on every row; only with an empty filter, where it is the explicit
    /// opt-in the model layer requires.
    #[serde(default)]
    pub all: bool,
}

impl MatchingPayload {
    fn filter(&self) -> Result<&JobFilter, ApiError> {
        if self.all && !self.filter.is_empty() {
            return Err(AwaError::Validation(
                "`all` acts on the whole DLQ, so it can't be combined with a filter".to_string(),
            )
            .into());
        }
        Ok(&self.filter)
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
    retry_matching_filter(&state, &payload.filter(), payload.all).await
}

pub async fn retry_dlq_matching(
    State(state): State<AppState>,
    Json(payload): Json<MatchingPayload>,
) -> Result<Json<CountResponse>, ApiError> {
    retry_matching_filter(&state, payload.filter()?, payload.all).await
}

async fn retry_matching_filter(
    state: &AppState,
    filter: &JobFilter,
    all: bool,
) -> Result<Json<CountResponse>, ApiError> {
    state.require_writable()?;
    let count = dlq::bulk_retry_from_dlq_matching(&state.pool, filter, all).await?;
    if count > 0 {
        state.invalidate_dashboard_caches();
        state
            .metrics
            .record_dlq_retried(filter.queue.as_deref(), count);
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
        state.invalidate_dashboard_caches();
        state.metrics.record_dlq_purged(queue.as_deref(), count);
    }
    Ok(Json(CountResponse { count }))
}

pub async fn bulk_purge_dlq(
    State(state): State<AppState>,
    Json(payload): Json<BulkFilterPayload>,
) -> Result<Json<CountResponse>, ApiError> {
    purge_matching_filter(&state, &payload.filter(), payload.all).await
}

pub async fn purge_dlq_matching(
    State(state): State<AppState>,
    Json(payload): Json<MatchingPayload>,
) -> Result<Json<CountResponse>, ApiError> {
    purge_matching_filter(&state, payload.filter()?, payload.all).await
}

async fn purge_matching_filter(
    state: &AppState,
    filter: &JobFilter,
    all: bool,
) -> Result<Json<CountResponse>, ApiError> {
    state.require_writable()?;
    let count = dlq::purge_dlq_matching(&state.pool, filter, all).await?;
    if count > 0 {
        state.invalidate_dashboard_caches();
        state
            .metrics
            .record_dlq_purged(filter.queue.as_deref(), count);
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
        state.invalidate_dashboard_caches();
        state.metrics.record_dlq_moved_bulk(
            payload.kind.as_deref(),
            payload.queue.as_deref(),
            &payload.reason,
            count,
        );
    }
    Ok(Json(CountResponse { count }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flat(json: serde_json::Value) -> Result<BulkFilterPayload, serde_json::Error> {
        serde_json::from_value(json)
    }

    fn matching(json: serde_json::Value) -> Result<MatchingPayload, serde_json::Error> {
        serde_json::from_value(json)
    }

    #[test]
    fn the_06_paths_take_only_the_flat_fields() {
        let body = flat(serde_json::json!({ "queue": "q", "tag": "a" })).unwrap();
        assert_eq!(body.filter(), JobFilter::new().queue("q").tag("a"));
        // A nested filter or a field 0.6 lacks belongs on the matching paths.
        assert!(flat(serde_json::json!({ "filter": { "tag": "a" } })).is_err());
        assert!(flat(serde_json::json!({ "queue": "q", "ids": [1] })).is_err());
        assert!(flat(serde_json::json!({ "tag": ["a", "b"] })).is_err());
    }

    #[test]
    fn the_matching_paths_take_the_whole_filter() {
        let body = matching(serde_json::json!({
            "filter": { "queue": "q", "tag": ["a", "b"], "ids": [1, 2] }
        }))
        .unwrap();
        assert_eq!(
            body.filter().unwrap(),
            &JobFilter::new().queue("q").tags(["a", "b"]).ids([1, 2])
        );
        assert!(matching(serde_json::json!({ "queue": "q" })).is_err());
        assert!(matching(serde_json::json!({ "filter": { "queue": "q" }, "kind": "k" })).is_err());
    }

    #[test]
    fn all_is_only_for_an_empty_filter() {
        let everything = matching(serde_json::json!({ "filter": {}, "all": true })).unwrap();
        assert!(everything.filter().unwrap().is_empty());
        let narrowed =
            matching(serde_json::json!({ "filter": { "tag": ["a", "b"] }, "all": true })).unwrap();
        assert!(narrowed.filter().is_err());
    }
}
