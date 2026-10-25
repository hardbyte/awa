//! Queue health and history (#543).

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use awa_model::admin;
use awa_model::queue_history::{self, FleetHistoryRow, QueueHistoryRow};

use crate::assessment::{self, HealthReport, OverallStatus};
use crate::cache::CacheError;
use crate::error::ApiError;
use crate::state::AppState;

/// History read for the assessment: the trend window plus one minute so the
/// first in-window row has a predecessor.
const ASSESSMENT_WINDOW_MINUTES: i64 = assessment::TREND_MINUTES + 1;
const MAX_HISTORY_MINUTES: u32 = 24 * 60;

#[derive(Debug, Default, Deserialize)]
pub struct HealthQuery {
    /// `critical` or `warning`: answer 503 at or above that status so a
    /// plain HTTP check can alert on it. Otherwise always 200.
    pub fail_on: Option<String>,
}

async fn queue_overviews(state: &AppState) -> Result<Vec<admin::QueueOverview>, ApiError> {
    let pool = state.pool.clone();
    Ok(state
        .cache
        .queues
        .try_get_with((), async {
            admin::queue_overviews(&pool)
                .await
                .map_err(CacheError::from)
        })
        .await?)
}

async fn queue_runtime(state: &AppState) -> Result<Vec<admin::QueueRuntimeSummary>, ApiError> {
    let pool = state.pool.clone();
    Ok(state
        .cache
        .queue_runtime
        .try_get_with((), async {
            admin::queue_runtime_summary(&pool)
                .await
                .map_err(CacheError::from)
        })
        .await?)
}

pub async fn health_report(state: &AppState) -> Result<Arc<HealthReport>, ApiError> {
    let queues = queue_overviews(state).await?;
    let summaries = queue_runtime(state).await?;
    let pool = state.pool.clone();
    Ok(state
        .cache
        .queue_health
        .try_get_with((), async move {
            let installed = queue_history::history_installed(&pool)
                .await
                .map_err(CacheError::from)?;
            let now = Utc::now();
            let history = if installed {
                queue_history::queue_history_since(
                    &pool,
                    now - TimeDelta::minutes(ASSESSMENT_WINDOW_MINUTES),
                )
                .await
                .map_err(CacheError::from)?
            } else {
                BTreeMap::new()
            };
            Ok::<_, CacheError>(Arc::new(assessment::build_report(
                now, &queues, &history, &summaries, installed,
            )))
        })
        .await?)
}

/// `GET /api/health/queues`: whether every queue is keeping up, worst first.
pub async fn get_queue_health(
    State(state): State<AppState>,
    Query(query): Query<HealthQuery>,
) -> Result<Response, ApiError> {
    let report = health_report(&state).await?;
    let threshold = match query.fail_on.as_deref() {
        Some("critical") => Some(OverallStatus::Critical),
        Some("warning") => Some(OverallStatus::Warning),
        Some(other) => {
            return Err(ApiError::Awa(awa_model::AwaError::Validation(format!(
                "fail_on must be 'critical' or 'warning', got '{other}'"
            ))))
        }
        None => None,
    };
    let status = match threshold {
        Some(threshold) if report.status >= threshold => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::OK,
    };
    Ok((status, Json(report)).into_response())
}

#[derive(Debug, Default, Deserialize)]
pub struct HistoryQuery {
    pub minutes: Option<u32>,
}

/// Per-minute values aligned to `QueueHistoryResponse::buckets`; `null`
/// where the leader wrote nothing (no leader, a failed sample, or before
/// the queue existed) or a counter was unknown.
#[derive(Debug, Clone, Default, Serialize)]
pub struct QueueSeries {
    pub available: Vec<Option<i64>>,
    pub running: Vec<Option<i64>>,
    pub lag_seconds: Vec<Option<f64>>,
    pub claimed: Vec<Option<i64>>,
    pub completed: Vec<Option<i64>>,
    pub failed_attempts: Vec<Option<i64>>,
    pub rate_limited: Vec<Option<i64>>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct FleetSeries {
    pub live_instances: Vec<Option<i32>>,
    pub healthy_instances: Vec<Option<i32>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetEventKind {
    VersionChange,
    LeaderChange,
    NoLeader,
    InstancesLost,
}

/// A change in the fleet worth marking on a chart.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FleetEvent {
    pub at: DateTime<Utc>,
    pub kind: FleetEventKind,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueHistoryResponse {
    /// False on a schema before v053.
    pub installed: bool,
    pub bucket_secs: u32,
    pub minutes: u32,
    /// Earliest bucket with any row in the window.
    pub observed_since: Option<DateTime<Utc>>,
    /// Start of each one-minute bucket, oldest first.
    pub buckets: Vec<DateTime<Utc>>,
    pub queues: BTreeMap<String, QueueSeries>,
    pub fleet: FleetSeries,
    pub events: Vec<FleetEvent>,
}

fn short_id(id: Option<Uuid>) -> String {
    id.map_or_else(|| "none".to_string(), |id| id.to_string()[..8].to_string())
}

/// Changes between consecutive fleet samples. Gaps are not events: a
/// missing minute means nobody sampled, not that the fleet changed.
pub fn fleet_events(rows: &[FleetHistoryRow]) -> Vec<FleetEvent> {
    let mut events = Vec::new();
    for pair in rows.windows(2) {
        let (before, after) = (&pair[0], &pair[1]);
        if before.versions != after.versions {
            events.push(FleetEvent {
                at: after.bucket,
                kind: FleetEventKind::VersionChange,
                detail: format!(
                    "Versions {} → {}",
                    before.versions.join(", "),
                    after.versions.join(", ")
                ),
            });
        }
        match (before.leader_instance_id, after.leader_instance_id) {
            (Some(_), None) => events.push(FleetEvent {
                at: after.bucket,
                kind: FleetEventKind::NoLeader,
                detail: "No live maintenance leader".to_string(),
            }),
            (previous, Some(next)) if previous != Some(next) => events.push(FleetEvent {
                at: after.bucket,
                kind: FleetEventKind::LeaderChange,
                detail: format!("Leader {} → {}", short_id(previous), short_id(Some(next))),
            }),
            _ => {}
        }
        if after.live_instances < before.live_instances {
            let lost = before.live_instances - after.live_instances;
            events.push(FleetEvent {
                at: after.bucket,
                kind: FleetEventKind::InstancesLost,
                detail: format!(
                    "{lost} instance{} stopped reporting ({} → {} live)",
                    if lost == 1 { "" } else { "s" },
                    before.live_instances,
                    after.live_instances
                ),
            });
        }
    }
    events
}

pub fn build_history_response(
    now: DateTime<Utc>,
    minutes: u32,
    installed: bool,
    queues: BTreeMap<String, Vec<QueueHistoryRow>>,
    fleet: Vec<FleetHistoryRow>,
) -> QueueHistoryResponse {
    let last = now.duration_trunc(TimeDelta::minutes(1)).unwrap_or(now);
    let count = minutes.max(1) as usize;
    let first = last - TimeDelta::minutes(count as i64 - 1);
    let buckets: Vec<DateTime<Utc>> = (0..count)
        .map(|i| first + TimeDelta::minutes(i as i64))
        .collect();
    let index = |bucket: DateTime<Utc>| -> Option<usize> {
        let offset = (bucket - first).num_minutes();
        (0..count as i64)
            .contains(&offset)
            .then_some(offset as usize)
    };

    let observed_since = queues
        .values()
        .filter_map(|rows| rows.first().map(|row| row.bucket))
        .chain(fleet.first().map(|row| row.bucket))
        .min();

    let series = queues
        .into_iter()
        .map(|(name, rows)| {
            let mut s = QueueSeries {
                available: vec![None; count],
                running: vec![None; count],
                lag_seconds: vec![None; count],
                claimed: vec![None; count],
                completed: vec![None; count],
                failed_attempts: vec![None; count],
                rate_limited: vec![None; count],
            };
            for row in rows {
                let Some(i) = index(row.bucket) else { continue };
                s.available[i] = Some(row.available);
                s.running[i] = Some(row.running);
                s.lag_seconds[i] = row.lag_seconds;
                s.claimed[i] = row.claimed;
                s.completed[i] = row.completed;
                s.failed_attempts[i] = row.failed_attempts;
                s.rate_limited[i] = row.rate_limited;
            }
            (name, s)
        })
        .collect();

    let mut fleet_series = FleetSeries {
        live_instances: vec![None; count],
        healthy_instances: vec![None; count],
    };
    for row in &fleet {
        if let Some(i) = index(row.bucket) {
            fleet_series.live_instances[i] = Some(row.live_instances);
            fleet_series.healthy_instances[i] = Some(row.healthy_instances);
        }
    }

    QueueHistoryResponse {
        installed,
        bucket_secs: 60,
        minutes,
        observed_since,
        buckets,
        queues: series,
        fleet: fleet_series,
        events: fleet_events(&fleet),
    }
}

/// `GET /api/stats/queue-history?minutes=60`: per-minute series for charts.
pub async fn get_queue_history(
    State(state): State<AppState>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<Arc<QueueHistoryResponse>>, ApiError> {
    let minutes = query.minutes.unwrap_or(60).clamp(1, MAX_HISTORY_MINUTES);
    let pool = state.pool.clone();
    let response = state
        .cache
        .queue_history
        .try_get_with(minutes, async move {
            let now = Utc::now();
            let installed = queue_history::history_installed(&pool)
                .await
                .map_err(CacheError::from)?;
            let (queues, fleet) = if installed {
                let since = now.duration_trunc(TimeDelta::minutes(1)).unwrap_or(now)
                    - TimeDelta::minutes(i64::from(minutes) - 1);
                (
                    queue_history::queue_history_since(&pool, since)
                        .await
                        .map_err(CacheError::from)?,
                    queue_history::fleet_history_since(&pool, since)
                        .await
                        .map_err(CacheError::from)?,
                )
            } else {
                (BTreeMap::new(), Vec::new())
            };
            Ok::<_, CacheError>(Arc::new(build_history_response(
                now, minutes, installed, queues, fleet,
            )))
        })
        .await?;
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(minute: i64) -> DateTime<Utc> {
        // 1_800_000_000 is a whole minute, so buckets land on minute starts.
        DateTime::from_timestamp(1_800_000_000 + minute * 60, 0).unwrap()
    }

    fn fleet(minute: i64, live: i32, leader: Option<u128>, versions: &[&str]) -> FleetHistoryRow {
        FleetHistoryRow {
            bucket: at(minute),
            live_instances: live,
            healthy_instances: live,
            stale_instances: 0,
            leader_instance_id: leader.map(Uuid::from_u128),
            versions: versions.iter().map(|v| v.to_string()).collect(),
        }
    }

    #[test]
    fn fleet_events_mark_rollouts_leader_changes_and_losses() {
        let rows = vec![
            fleet(0, 4, Some(1), &["0.7.0"]),
            fleet(1, 4, Some(1), &["0.7.0"]),
            fleet(2, 3, Some(1), &["0.7.0", "0.7.1"]),
            fleet(3, 4, Some(2), &["0.7.1"]),
            fleet(4, 4, None, &["0.7.1"]),
        ];
        let kinds: Vec<(i64, FleetEventKind)> = fleet_events(&rows)
            .iter()
            .map(|e| ((e.at - at(0)).num_minutes(), e.kind))
            .collect();
        assert_eq!(
            kinds,
            [
                (2, FleetEventKind::VersionChange),
                (2, FleetEventKind::InstancesLost),
                (3, FleetEventKind::VersionChange),
                (3, FleetEventKind::LeaderChange),
                (4, FleetEventKind::NoLeader),
            ]
        );
    }

    #[test]
    fn history_response_aligns_rows_to_minute_buckets() {
        let now = at(10) + TimeDelta::seconds(30);
        let row = |minute: i64, available: i64| QueueHistoryRow {
            queue: "email".into(),
            bucket: at(minute),
            available,
            running: 1,
            scheduled: 0,
            retryable: 0,
            failed: 0,
            waiting_external: 0,
            lag_seconds: None,
            paused: false,
            claimed: None,
            completed: Some(3),
            failed_attempts: None,
            retried: None,
            rate_limited: None,
            claim_wait_ms: None,
            live_workers: 1,
            capacity: None,
        };
        let mut queues = BTreeMap::new();
        queues.insert(
            "email".to_string(),
            vec![row(5, 50), row(7, 70), row(10, 100)],
        );
        let response =
            build_history_response(now, 6, true, queues, vec![fleet(9, 2, Some(1), &["0.7.0"])]);
        assert_eq!(response.buckets.first(), Some(&at(5)));
        assert_eq!(response.buckets.last(), Some(&at(10)));
        assert_eq!(
            response.queues["email"].available,
            vec![Some(50), None, Some(70), None, None, Some(100)]
        );
        assert_eq!(response.queues["email"].claimed, vec![None; 6]);
        assert_eq!(response.fleet.live_instances[4], Some(2));
        assert_eq!(response.observed_since, Some(at(5)));
    }
}
