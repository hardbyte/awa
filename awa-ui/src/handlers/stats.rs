use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use sqlx::PgPool;
use std::collections::HashMap;

use awa_model::admin;
use awa_model::job::JobState;

use crate::error::ApiError;

pub async fn get_stats(
    State(pool): State<PgPool>,
) -> Result<Json<HashMap<JobState, i64>>, ApiError> {
    let counts = admin::state_counts(&pool).await?;
    Ok(Json(counts))
}

#[derive(Debug, Deserialize)]
pub struct TimeseriesParams {
    pub minutes: Option<i32>,
}

pub async fn get_timeseries(
    State(pool): State<PgPool>,
    Query(params): Query<TimeseriesParams>,
) -> Result<Json<Vec<admin::StateTimeseriesBucket>>, ApiError> {
    let minutes = params.minutes.unwrap_or(60);
    let buckets = admin::state_timeseries(&pool, minutes).await?;
    Ok(Json(buckets))
}

pub async fn get_distinct_kinds(State(pool): State<PgPool>) -> Result<Json<Vec<String>>, ApiError> {
    let kinds = admin::distinct_kinds(&pool).await?;
    Ok(Json(kinds))
}

pub async fn get_distinct_queues(
    State(pool): State<PgPool>,
) -> Result<Json<Vec<String>>, ApiError> {
    let queues = admin::distinct_queues(&pool).await?;
    Ok(Json(queues))
}
