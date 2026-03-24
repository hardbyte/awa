use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use std::collections::HashMap;

use awa_model::admin;
use awa_model::job::JobState;

use crate::error::ApiError;
use crate::state::{AppState, Capabilities};

pub async fn get_stats(
    State(state): State<AppState>,
) -> Result<Json<HashMap<JobState, i64>>, ApiError> {
    let counts = admin::state_counts(&state.pool).await?;
    Ok(Json(counts))
}

#[derive(Debug, Deserialize)]
pub struct TimeseriesParams {
    pub minutes: Option<i32>,
}

pub async fn get_timeseries(
    State(state): State<AppState>,
    Query(params): Query<TimeseriesParams>,
) -> Result<Json<Vec<admin::StateTimeseriesBucket>>, ApiError> {
    let minutes = params.minutes.unwrap_or(60);
    let buckets = admin::state_timeseries(&state.pool, minutes).await?;
    Ok(Json(buckets))
}

pub async fn get_distinct_kinds(
    State(state): State<AppState>,
) -> Result<Json<Vec<String>>, ApiError> {
    let kinds = admin::distinct_kinds(&state.pool).await?;
    Ok(Json(kinds))
}

pub async fn get_distinct_queues(
    State(state): State<AppState>,
) -> Result<Json<Vec<String>>, ApiError> {
    let queues = admin::distinct_queues(&state.pool).await?;
    Ok(Json(queues))
}

pub async fn get_capabilities(
    State(state): State<AppState>,
) -> Result<Json<Capabilities>, ApiError> {
    Ok(Json(Capabilities {
        read_only: state.read_only,
    }))
}
