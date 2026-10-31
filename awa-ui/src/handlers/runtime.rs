use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use awa_model::admin;
use awa_model::storage;

use crate::error::ApiError;
use crate::state::AppState;

/// Query parameters accepted by `GET /api/storage`.
///
/// `history=true` opts in to the epoch-anchored backlog history field on
/// the response. The 0.6 server has no persistent store for these samples
/// (see #180), so the field is currently an empty slice — clients
/// accumulate samples themselves, keyed by `transition_epoch`. The query
/// param exists so the contract is stable for future versions that may
/// add a server-side ring buffer without changing the API shape.
#[derive(Debug, Default, Deserialize)]
pub struct StorageQuery {
    #[serde(default)]
    pub history: bool,
}

pub async fn get_runtime(State(state): State<AppState>) -> Result<Response, ApiError> {
    let pool = state.pool.clone();
    let overview = state
        .cache
        .runtime
        .get(|| admin::runtime_overview(&pool))
        .await?;
    Ok(overview.into_response())
}

pub async fn list_queue_runtime(State(state): State<AppState>) -> Result<Response, ApiError> {
    let pool = state.pool.clone();
    let summary = state
        .cache
        .queue_runtime
        .get(|| admin::queue_runtime_summary(&pool))
        .await?;
    Ok(summary.into_response())
}

pub async fn get_storage(
    State(state): State<AppState>,
    Query(query): Query<StorageQuery>,
) -> Result<Response, ApiError> {
    let pool = state.pool.clone();
    let mut report = state
        .cache
        .storage
        .get(|| storage::status_report(&pool))
        .await?;
    if query.history {
        // Per #180 (rejected new audit table) the 0.6 server doesn't
        // accumulate samples. We still return the field — as an empty
        // vector — so clients can rely on its presence to detect the
        // contract and start their own client-side accumulation.
        report.data.history = Some(Vec::new());
    }
    Ok(report.into_response())
}
