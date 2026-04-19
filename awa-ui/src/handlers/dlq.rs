use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use awa_model::dlq::{self, DlqRow, ListDlqFilter, RetryFromDlqOpts};

use crate::error::ApiError;
use crate::state::AppState;

/// Emit an OTel counter against the global meter.
///
/// The UI crate intentionally avoids depending on `awa-worker` (which would
/// pull in the dispatcher/runtime crate graph) for the sake of a few counter
/// increments. The OTel SDK caches instruments by name so repeated builds are
/// a hashmap lookup; attribute sets here match `AwaMetrics` so dashboards
/// don't drift between UI and worker sources.
fn emit_counter(name: &'static str, description: &'static str, count: u64, queue: Option<&str>) {
    if count == 0 {
        return;
    }
    let meter = opentelemetry::global::meter("awa");
    let counter = meter
        .u64_counter(name)
        .with_description(description)
        .with_unit("{job}")
        .build();
    let attrs: Vec<opentelemetry::KeyValue> = queue
        .map(|q| vec![opentelemetry::KeyValue::new("awa.job.queue", q.to_string())])
        .unwrap_or_default();
    counter.add(count, &attrs);
}

#[derive(Debug, Deserialize)]
pub struct ListDlqParams {
    pub kind: Option<String>,
    pub queue: Option<String>,
    pub tag: Option<String>,
    pub before_id: Option<i64>,
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
    let job = dlq::retry_from_dlq(&state.pool, job_id, &opts).await?;
    if let Some(job) = job.as_ref() {
        emit_counter(
            "awa.job.dlq_retried",
            "Number of jobs retried out of the Dead Letter Queue",
            1,
            Some(&job.queue),
        );
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
    let filter = ListDlqFilter {
        kind: payload.kind,
        queue: payload.queue,
        tag: payload.tag,
        ..Default::default()
    };
    let count = dlq::bulk_retry_from_dlq(&state.pool, &filter, payload.all).await?;
    emit_counter(
        "awa.job.dlq_retried",
        "Number of jobs retried out of the Dead Letter Queue",
        count,
        queue_attr.as_deref(),
    );
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
        .map(|row| row.queue);
    let deleted = dlq::purge_dlq_job(&state.pool, job_id).await?;
    let count = if deleted { 1 } else { 0 };
    emit_counter(
        "awa.job.dlq_purged",
        "Number of DLQ rows purged",
        count,
        queue.as_deref(),
    );
    Ok(Json(CountResponse { count }))
}

pub async fn bulk_purge_dlq(
    State(state): State<AppState>,
    Json(payload): Json<BulkFilterPayload>,
) -> Result<Json<CountResponse>, ApiError> {
    state.require_writable()?;
    let queue_attr = payload.queue.clone();
    let filter = ListDlqFilter {
        kind: payload.kind,
        queue: payload.queue,
        tag: payload.tag,
        ..Default::default()
    };
    let count = dlq::purge_dlq(&state.pool, &filter, payload.all).await?;
    emit_counter(
        "awa.job.dlq_purged",
        "Number of DLQ rows purged",
        count,
        queue_attr.as_deref(),
    );
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
}

fn default_move_reason() -> String {
    "ui_bulk_move".to_string()
}

/// Bulk-move failed jobs from `jobs_hot` into the DLQ. Requires at least one
/// of `kind` or `queue` to avoid accidentally archiving every failed row in
/// the system.
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
    )
    .await?;
    // Mirror the worker: emit the `awa.job.dlq_moved` counter so operator
    // dashboards reflect admin bulk moves alongside automatic routing.
    // Done inline against the global OTel meter rather than through an
    // awa-worker dependency, which would pull in the dispatcher/runtime
    // crate graph for a single counter increment. Attribute set matches
    // `AwaMetrics::record_dlq_moved_bulk` so dashboards don't drift.
    if count > 0 {
        let meter = opentelemetry::global::meter("awa");
        let counter = meter
            .u64_counter("awa.job.dlq_moved")
            .with_description("Number of jobs moved into the Dead Letter Queue")
            .with_unit("{job}")
            .build();
        let mut attrs = vec![opentelemetry::KeyValue::new(
            "awa.dlq.reason",
            payload.reason.clone(),
        )];
        if let Some(kind) = payload.kind.as_deref() {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.job.kind",
                kind.to_string(),
            ));
        }
        if let Some(queue) = payload.queue.as_deref() {
            attrs.push(opentelemetry::KeyValue::new(
                "awa.job.queue",
                queue.to_string(),
            ));
        }
        counter.add(count, &attrs);
    }
    Ok(Json(CountResponse { count }))
}
