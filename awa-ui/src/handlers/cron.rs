use axum::extract::{Path, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use awa_model::cron;
use awa_model::job::JobRow;
use awa_model::CronJobRow;

use crate::error::ApiError;
use crate::state::AppState;

/// Enriched cron job response with computed next fire time.
///
/// `next_fire_at` is the cron-expression's next tick. It is computed
/// even for paused schedules so the UI can show "would fire at X if
/// resumed" — callers should branch on `paused_at` to decide whether
/// that fire will actually happen.
#[derive(Serialize)]
pub struct CronJobResponse {
    #[serde(flatten)]
    #[allow(unused)]
    row: CronJobRow,
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
    state.invalidate_dashboard_caches();
    Ok(Json(job))
}

#[derive(Debug, Deserialize, Default)]
pub struct PauseCronPayload {
    pub paused_by: Option<String>,
}

pub async fn pause_cron_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
    payload: Option<Json<PauseCronPayload>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.require_writable()?;
    let paused_by = payload.as_ref().and_then(|p| p.paused_by.as_deref());
    let updated = cron::pause_cron_job(&state.pool, &name, paused_by).await?;
    if !updated {
        return Err(ApiError::not_found(format!(
            "cron schedule '{name}' not found"
        )));
    }
    state.invalidate_dashboard_caches();
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn resume_cron_job(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.require_writable()?;
    let updated = cron::resume_cron_job(&state.pool, &name).await?;
    if !updated {
        return Err(ApiError::not_found(format!(
            "cron schedule '{name}' not found"
        )));
    }
    state.invalidate_dashboard_caches();
    Ok(Json(serde_json::json!({ "ok": true })))
}
