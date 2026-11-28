use axum::extract::{Query, RawQuery, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

use awa_model::admin;

use crate::error::ApiError;
use crate::state::{AppState, Capabilities};

pub async fn get_stats(State(state): State<AppState>) -> Result<Response, ApiError> {
    let pool = state.pool.clone();
    let counts = state.cache.stats.get(|| admin::state_counts(&pool)).await?;
    Ok(counts.into_response())
}

#[derive(Debug, Deserialize)]
pub struct TimeseriesParams {
    pub minutes: Option<i32>,
}

/// `GET /api/stats/timeseries`: deprecated. Jobs by creation minute and
/// current state; the admin UI no longer reads it. Use
/// `GET /api/stats/outcomes` for outcomes over time.
pub async fn get_timeseries(
    State(state): State<AppState>,
    Query(params): Query<TimeseriesParams>,
) -> Result<Json<Vec<admin::StateTimeseriesBucket>>, ApiError> {
    let minutes = params.minutes.unwrap_or(60);
    let buckets = admin::state_timeseries(&state.pool, minutes).await?;
    Ok(Json(buckets))
}

/// `GET /api/stats/outcomes`: terminal outcomes per bucket; see
/// [`admin::job_outcomes`].
pub async fn get_outcomes(
    State(state): State<AppState>,
    RawQuery(raw): RawQuery,
) -> Result<Response, ApiError> {
    let query = super::filter_params::parse_outcomes(raw.as_deref())?;
    let pool = state.pool.clone();
    let outcomes = state
        .cache
        .outcomes
        .get_keyed(query.clone(), || admin::job_outcomes(&pool, &query))
        .await?;
    Ok(outcomes.into_response())
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
        poll_interval_ms: state.poll_interval_ms,
        instance_name: state.instance.name.clone(),
        instance_color: state.instance.color.clone(),
        peers: state.instance.peers.clone(),
    }))
}
