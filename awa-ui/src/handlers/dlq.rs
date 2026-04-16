use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use awa_model::dlq::{self, DlqRow, ListDlqFilter, RetryFromDlqOpts};

use crate::error::ApiError;
use crate::state::AppState;

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
    let filter = ListDlqFilter {
        kind: payload.kind,
        queue: payload.queue,
        tag: payload.tag,
        ..Default::default()
    };
    let count = dlq::bulk_retry_from_dlq(&state.pool, &filter).await?;
    Ok(Json(CountResponse { count }))
}

pub async fn purge_dlq_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<CountResponse>, ApiError> {
    state.require_writable()?;
    let deleted = dlq::purge_dlq_job(&state.pool, job_id).await?;
    Ok(Json(CountResponse {
        count: if deleted { 1 } else { 0 },
    }))
}

pub async fn bulk_purge_dlq(
    State(state): State<AppState>,
    Json(payload): Json<BulkFilterPayload>,
) -> Result<Json<CountResponse>, ApiError> {
    state.require_writable()?;
    let filter = ListDlqFilter {
        kind: payload.kind,
        queue: payload.queue,
        tag: payload.tag,
        ..Default::default()
    };
    let count = dlq::purge_dlq(&state.pool, &filter).await?;
    Ok(Json(CountResponse { count }))
}
