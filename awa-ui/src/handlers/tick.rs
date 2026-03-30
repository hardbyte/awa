//! Tick endpoint for serverless/low-traffic deployments.
//!
//! Runs a single bounded maintenance pass: promote, rescue, cleanup.
//! Designed to be called from Cloud Scheduler, pg_cron, or a framework
//! background task.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;

use crate::error::ApiError;
use crate::state::AppState;

/// POST /api/tick
///
/// Runs a single bounded maintenance pass and returns a summary of work done.
pub async fn tick(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    state.require_writable()?;
    let result = awa_model::admin::tick(&state.pool).await?;
    Ok((StatusCode::OK, Json(result)))
}
