use axum::extract::{Path, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Serialize;

use awa_model::cron;
use awa_model::job::JobRow;
use awa_model::CronJobRow;

use crate::error::ApiError;
use crate::state::AppState;

/// Enriched cron job response with computed next fire time.
#[derive(Serialize)]
pub struct CronJobResponse {
    #[serde(flatten)]
    #[allow(unused)]
    row: CronJobRow,
    /// Next scheduled fire time (computed from cron_expr + timezone).
    next_fire_at: Option<DateTime<Utc>>,
}

pub async fn list_cron_jobs(
    State(state): State<AppState>,
) -> Result<Json<Vec<CronJobResponse>>, ApiError> {
    let jobs = cron::list_cron_jobs(&state.pool).await?;
    let response: Vec<CronJobResponse> = jobs
        .into_iter()
        .map(|row| {
            let next_fire_at = cron::next_fire_time(&row.cron_expr, &row.timezone);
            CronJobResponse { row, next_fire_at }
        })
        .collect();
    Ok(Json(response))
}

pub async fn trigger_cron_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<JobRow>, ApiError> {
    state.require_writable()?;
    let job = cron::trigger_cron_job(&state.pool, &name).await?;
    Ok(Json(job))
}
