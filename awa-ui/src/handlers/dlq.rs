use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use awa_model::bulk::{self, BulkBatch, BulkCursor, BulkRun, BulkRunOptions};
use awa_model::dlq::{self, DlqRow, ListDlqFilter, RetryFromDlqOpts};

use crate::error::ApiError;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct ListDlqParams {
    pub kind: Option<String>,
    pub queue: Option<String>,
    pub tag: Option<String>,
    pub before_id: Option<i64>,
    /// Pair with `before_id` for a race-free `(dlq_at, id)` cursor that
    /// matches the response sort order. Legacy callers omitting this get
    /// the old id-only cursor.
    pub before_dlq_at: Option<chrono::DateTime<chrono::Utc>>,
    pub limit: Option<i64>,
}

pub async fn list_dlq(
    State(state): State<AppState>,
    Query(params): Query<ListDlqParams>,
) -> Result<Json<Vec<DlqRow>>, ApiError> {
    let filter = ListDlqFilter {
        kind: params.kind,
        queue: params.queue,
        tag: params.tag,
        before_id: params.before_id,
        before_dlq_at: params.before_dlq_at,
        limit: params.limit,
    };
    let rows = dlq::list_dlq(&state.pool, &filter).await?;
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
        state.invalidate_dashboard_caches();
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
    /// Explicit opt-in to act on every row in the DLQ when no filter is
    /// provided. The model layer rejects empty filters otherwise.
    #[serde(default)]
    pub all: bool,
    /// Run one batch of up to this many rows, resuming at `cursor`. Without
    /// it, the server runs batches until no matching row remains.
    #[serde(default)]
    pub batch_size: Option<i64>,
    #[serde(default)]
    pub cursor: Option<BulkCursor>,
}

#[derive(Debug, Deserialize)]
pub struct BulkRetryPayload {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub queue: Option<String>,
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default)]
    pub all: bool,
    /// Run one batch of up to this many rows, resuming at `cursor`, and
    /// return its cursor. Without it, the server runs batches of
    /// `bulk::DEFAULT_BATCH_SIZE` until no matching row remains.
    #[serde(default)]
    pub batch_size: Option<i64>,
    #[serde(default)]
    pub cursor: Option<BulkCursor>,
}

#[derive(Debug, Serialize)]
pub struct CountResponse {
    pub count: u64,
}

/// The result of a bulk route: one batch when the request set `batch_size`,
/// otherwise the whole run.
#[derive(Debug, Serialize)]
pub struct BulkResponse {
    pub count: u64,
    /// Rows skipped because the change would violate a unique claim.
    pub skipped: u64,
    /// IDs of skipped jobs, at most `bulk::MAX_REPORTED_CONFLICTS`.
    pub conflicts: Vec<i64>,
    /// Matching rows with an explicit job ID above the job ID sequence that
    /// the run leaves alone, because the database role may not advance the
    /// sequence past them.
    pub left_above_sequence: u64,
    /// One batch: every remaining matching row is held by another
    /// transaction; nothing changed and the caller should try again.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub busy: bool,
    /// No matching row remains.
    pub done: bool,
    /// One batch: pass this back to resume.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<BulkCursor>,
}

impl BulkResponse {
    pub fn from_batch(batch: BulkBatch) -> Self {
        Self {
            count: batch.count,
            skipped: batch.skipped,
            conflicts: batch.conflicts,
            left_above_sequence: batch.left_above_sequence,
            busy: batch.busy,
            done: batch.done,
            cursor: Some(batch.cursor),
        }
    }

    pub fn from_run(run: BulkRun) -> Result<Self, ApiError> {
        if run.busy_timed_out {
            let mut message = format!(
                "every remaining matching row has been held by another transaction for {}s; changed {} row(s) before stopping",
                bulk::DEFAULT_BUSY_TIMEOUT.as_secs(),
                run.count
            );
            if run.skipped > 0 {
                message.push_str(&format!(
                    "; skipped {} job(s) whose unique key a live job holds",
                    run.skipped
                ));
            }
            let mut detail = serde_json::Map::new();
            detail.insert("count".into(), run.count.into());
            detail.insert("skipped".into(), run.skipped.into());
            detail.insert("conflicts".into(), run.conflicts.into());
            detail.insert("left_above_sequence".into(), run.left_above_sequence.into());
            detail.insert("done".into(), false.into());
            return Err(ApiError::ConflictDetail { message, detail });
        }
        Ok(Self {
            count: run.count,
            skipped: run.skipped,
            conflicts: run.conflicts,
            left_above_sequence: run.left_above_sequence,
            busy: false,
            done: run.complete,
            cursor: None,
        })
    }
}

/// Run one batch when `batch_size` is set, resuming at `cursor`; otherwise
/// run batches of `bulk::DEFAULT_BATCH_SIZE` until no matching row remains.
///
/// `on_changed` sees the row count of each batch that changed rows, as the
/// batch commits, so a run that stops on a later batch's error or busy
/// timeout still accounts for the rows it changed. Routes record metrics and
/// invalidate caches there rather than from the response.
pub async fn run_bulk_route<F, Fut>(
    batch_size: Option<i64>,
    cursor: Option<BulkCursor>,
    mut batch: F,
    mut on_changed: impl FnMut(u64),
) -> Result<BulkResponse, ApiError>
where
    F: FnMut(i64, BulkCursor) -> Fut,
    Fut: std::future::Future<Output = Result<BulkBatch, awa_model::AwaError>>,
{
    match batch_size {
        Some(batch_size) => {
            let batch = batch(batch_size, cursor.unwrap_or_default()).await?;
            if batch.count > 0 {
                on_changed(batch.count);
            }
            Ok(BulkResponse::from_batch(batch))
        }
        None => BulkResponse::from_run(
            bulk::run_batches(BulkRunOptions::default(), batch, |batch, _| {
                if batch.count > 0 {
                    on_changed(batch.count);
                }
            })
            .await
            .map_err(|err| {
                ApiError::after_partial(
                    err.error,
                    crate::error::bulk::Committed {
                        changed: err.partial.count,
                        skipped: err.partial.skipped,
                        conflicts: err.partial.conflicts,
                    },
                )
            })?,
        ),
    }
}

pub async fn bulk_retry_dlq(
    State(state): State<AppState>,
    Json(payload): Json<BulkRetryPayload>,
) -> Result<Json<BulkResponse>, ApiError> {
    state.require_writable()?;
    let queue_attr = payload.queue.clone();
    let filter = ListDlqFilter {
        kind: payload.kind,
        queue: payload.queue,
        tag: payload.tag,
        ..Default::default()
    };
    let all = payload.all;
    let response = run_bulk_route(
        payload.batch_size,
        payload.cursor,
        |size, cursor| dlq::bulk_retry_from_dlq_batch(&state.pool, &filter, all, size, cursor),
        |count| {
            state
                .metrics
                .record_dlq_retried(queue_attr.as_deref(), count);
            state.invalidate_dashboard_caches();
        },
    )
    .await?;
    Ok(Json(response))
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
        state.invalidate_dashboard_caches();
    }
    Ok(Json(CountResponse { count }))
}

pub async fn bulk_purge_dlq(
    State(state): State<AppState>,
    Json(payload): Json<BulkFilterPayload>,
) -> Result<Json<BulkResponse>, ApiError> {
    state.require_writable()?;
    let queue_attr = payload.queue.clone();
    let filter = ListDlqFilter {
        kind: payload.kind,
        queue: payload.queue,
        tag: payload.tag,
        ..Default::default()
    };
    let all = payload.all;
    let response = run_bulk_route(
        payload.batch_size,
        payload.cursor,
        |size, cursor| dlq::purge_dlq_batch(&state.pool, &filter, all, size, cursor),
        |count| {
            state
                .metrics
                .record_dlq_purged(queue_attr.as_deref(), count);
            state.invalidate_dashboard_caches();
        },
    )
    .await?;
    Ok(Json(response))
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
    /// Run one batch of up to this many rows, resuming at `cursor`. Without
    /// it, the server runs batches until no matching row remains.
    #[serde(default)]
    pub batch_size: Option<i64>,
    #[serde(default)]
    pub cursor: Option<BulkCursor>,
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
) -> Result<Json<BulkResponse>, ApiError> {
    state.require_writable()?;
    let response = run_bulk_route(
        payload.batch_size,
        payload.cursor,
        |size, cursor| {
            dlq::bulk_move_failed_to_dlq_batch(
                &state.pool,
                payload.kind.as_deref(),
                payload.queue.as_deref(),
                &payload.reason,
                payload.all,
                size,
                cursor,
            )
        },
        |count| {
            state.metrics.record_dlq_moved_bulk(
                payload.kind.as_deref(),
                payload.queue.as_deref(),
                &payload.reason,
                count,
            );
            state.invalidate_dashboard_caches();
        },
    )
    .await?;
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
pub struct DlqCountParams {
    pub kind: Option<String>,
    pub queue: Option<String>,
    pub tag: Option<String>,
}

/// Matching DLQ rows are counted up to this many.
pub const DLQ_COUNT_CAP: i64 = 10_000;

pub async fn dlq_count(
    State(state): State<AppState>,
    Query(params): Query<DlqCountParams>,
) -> Result<Json<dlq::DlqCount>, ApiError> {
    let filter = ListDlqFilter {
        kind: params.kind,
        queue: params.queue,
        tag: params.tag,
        ..Default::default()
    };
    Ok(Json(
        dlq::count_dlq(&state.pool, &filter, DLQ_COUNT_CAP).await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn changed(count: u64) -> BulkBatch {
        let mut batch = BulkBatch::default();
        batch.count = count;
        batch
    }

    fn busy() -> BulkBatch {
        let mut batch = BulkBatch::default();
        batch.busy = true;
        batch
    }

    #[tokio::test(start_paused = true)]
    async fn a_whole_run_records_committed_batches_before_a_busy_timeout() {
        let mut calls = 0;
        let mut recorded = Vec::new();
        let result = run_bulk_route(
            None,
            None,
            |_, _| {
                calls += 1;
                let batch = if calls == 1 { changed(2) } else { busy() };
                async move { Ok(batch) }
            },
            |count| recorded.push(count),
        )
        .await;
        assert!(
            matches!(result, Err(ApiError::ConflictDetail { .. })),
            "{result:?}"
        );
        assert_eq!(recorded, vec![2]);
    }

    #[tokio::test]
    async fn a_whole_run_records_committed_batches_before_an_error() {
        let mut calls = 0;
        let mut recorded = Vec::new();
        let result = run_bulk_route(
            None,
            None,
            |_, _| {
                calls += 1;
                let batch = if calls == 1 {
                    Ok(changed(3))
                } else {
                    Err(awa_model::AwaError::Validation("later batch failed".into()))
                };
                async move { batch }
            },
            |count| recorded.push(count),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(recorded, vec![3]);
    }

    #[tokio::test]
    async fn one_batch_records_its_count() {
        let mut recorded = Vec::new();
        let response = run_bulk_route(
            Some(5),
            None,
            |_, _| async { Ok(changed(4)) },
            |count| recorded.push(count),
        )
        .await
        .expect("batch");
        assert_eq!((response.count, recorded), (4, vec![4]));
    }
}

#[cfg(test)]
mod from_run_tests {
    use super::*;
    use axum::response::IntoResponse;

    #[tokio::test]
    async fn a_busy_timeout_reports_skipped_conflicts() {
        let mut run = BulkRun::default();
        run.count = 3;
        run.skipped = 1;
        run.conflicts = vec![42];
        run.busy_timed_out = true;
        let response = BulkResponse::from_run(run)
            .expect_err("busy timeout")
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(
            (
                &body["count"],
                &body["skipped"],
                &body["conflicts"],
                &body["done"]
            ),
            (
                &serde_json::json!(3),
                &serde_json::json!(1),
                &serde_json::json!([42]),
                &serde_json::json!(false)
            )
        );
        assert!(
            body["error"]
                .as_str()
                .expect("error")
                .contains("skipped 1 job(s)"),
            "{body}"
        );
    }
}
