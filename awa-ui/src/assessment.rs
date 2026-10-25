//! Whether each queue is keeping up (#543).
//!
//! One source of truth for the overview, the Queues page and
//! `/api/health/queues`, so an alert and the screen an operator opens after
//! it never disagree. Inputs are the current queue snapshot, the leader's
//! per-minute history (`awa.queue_history`, v051) and live runtime summaries.

use std::collections::BTreeMap;

use awa_model::admin::{QueueOverview, QueueRuntimeMode, QueueRuntimeSummary};
use awa_model::queue_history::QueueHistoryRow;
use chrono::{DateTime, Utc};
use serde::Serialize;

/// Trailing window for trends and rates.
pub const TREND_MINUTES: i64 = 5;
/// Claimable work waiting this long with nothing claiming it is stalled.
pub const STALL_AFTER_SECONDS: f64 = 60.0;
/// An oldest claimable job older than this is a long wait even if depth holds.
pub const SLOW_WAIT_SECONDS: f64 = 300.0;
/// Depth only counts as growing or draining when the fitted change over the
/// trend window exceeds this many jobs or this share of the backlog,
/// whichever is larger; below that it is batching noise.
const MIN_TREND_JOBS: f64 = 20.0;
const MIN_TREND_SHARE: f64 = 0.05;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueHealth {
    Stalled,
    Behind,
    Slow,
    Paused,
    Draining,
    Unknown,
    Steady,
    Idle,
}

impl QueueHealth {
    fn label(self) -> &'static str {
        match self {
            Self::Stalled => "Not being worked",
            Self::Behind => "Falling behind",
            Self::Slow => "Long wait",
            Self::Paused => "Paused",
            Self::Draining => "Draining",
            Self::Unknown => "No trend yet",
            Self::Steady => "Steady",
            Self::Idle => "Idle",
        }
    }
}

/// Why waiting work is not moving faster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitedBy {
    Paused,
    NoWorkers,
    RateLimit,
    WorkerCapacity,
    /// Workers had free permits but did not claim faster: look at claim
    /// latency, the database, or a deliberately low `max_workers`.
    Nothing,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Trend {
    /// Least-squares slope of waiting jobs, per minute.
    pub per_minute: f64,
    /// Fitted change across the span of points used.
    pub change: f64,
    pub minutes: f64,
}

/// Rates over the trend window, from runtime counters. `None` where any
/// minute in the window had unknown counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct Flow {
    pub claimed_per_minute: Option<f64>,
    pub completed_per_minute: Option<f64>,
    pub failed_per_minute: Option<f64>,
    pub retried_per_minute: Option<f64>,
    /// Inferred: change in waiting jobs plus claims. Includes jobs that
    /// became due (scheduled, retries) as well as new enqueues.
    pub arrivals_per_minute: Option<f64>,
    pub mean_claim_wait_seconds: Option<f64>,
    pub rate_limited: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueAssessment {
    pub queue: String,
    pub health: QueueHealth,
    pub label: &'static str,
    /// One sentence on what was observed.
    pub reason: String,
    pub needs_attention: bool,
    pub rank: u8,
    pub limited_by: Option<LimitedBy>,
    pub trend: Option<Trend>,
    /// Seconds to clear the backlog at the trend's pace, when draining.
    pub clear_seconds: Option<f64>,
    pub flow: Flow,
    pub live_workers: u64,
    pub in_flight: u64,
    pub capacity: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OverallStatus {
    Ok,
    Unknown,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthReport {
    pub status: OverallStatus,
    pub title: String,
    pub detail: String,
    pub generated_at: DateTime<Utc>,
    /// False when the schema predates v051 or the leader has not sampled
    /// yet: only stalls and long waits can be detected.
    pub history_available: bool,
    pub history_since: Option<DateTime<Utc>>,
    pub queues: Vec<QueueAssessment>,
}

fn fit(points: &[(f64, f64)]) -> Option<Trend> {
    if points.len() < 3 {
        return None;
    }
    let span = points.last()?.0 - points.first()?.0;
    if span < 2.0 {
        return None;
    }
    let n = points.len() as f64;
    let mean_x = points.iter().map(|(x, _)| x).sum::<f64>() / n;
    let mean_y = points.iter().map(|(_, y)| y).sum::<f64>() / n;
    let (mut num, mut den) = (0.0, 0.0);
    for (x, y) in points {
        num += (x - mean_x) * (y - mean_y);
        den += (x - mean_x).powi(2);
    }
    let per_minute = if den == 0.0 { 0.0 } else { num / den };
    Some(Trend {
        per_minute,
        change: per_minute * span,
        minutes: span,
    })
}

fn minutes_since(now: DateTime<Utc>, at: DateTime<Utc>) -> f64 {
    (at - now).num_milliseconds() as f64 / 60_000.0
}

/// Fit the trailing window of waiting jobs, ending with the live snapshot.
fn depth_trend(now: DateTime<Utc>, rows: &[QueueHistoryRow], current: i64) -> Option<Trend> {
    let window_start = now - chrono::TimeDelta::minutes(TREND_MINUTES);
    let mut points: Vec<(f64, f64)> = rows
        .iter()
        .filter(|row| row.bucket >= window_start)
        .map(|row| (minutes_since(now, row.bucket), row.available as f64))
        .collect();
    points.push((0.0, current as f64));
    fit(&points)
}

fn flow(now: DateTime<Utc>, rows: &[QueueHistoryRow], trend: Option<Trend>) -> Flow {
    let window_start = now - chrono::TimeDelta::minutes(TREND_MINUTES);
    let recent: Vec<&QueueHistoryRow> = rows
        .iter()
        .filter(|row| row.bucket >= window_start)
        .collect();
    // Each row's deltas cover the minute before it, so a known row is a
    // genuine per-minute count even when a neighbour is unknown (a leader
    // change). Two known minutes are required so one odd minute is not
    // reported as a rate.
    let rate = |pick: fn(&QueueHistoryRow) -> Option<i64>| -> Option<f64> {
        let values: Vec<i64> = recent.iter().filter_map(|row| pick(row)).collect();
        if values.len() < 2 {
            return None;
        }
        Some(values.iter().sum::<i64>() as f64 / values.len() as f64)
    };
    let claimed = rate(|r| r.claimed);
    let wait_ms = rate(|r| r.claim_wait_ms);
    Flow {
        claimed_per_minute: claimed,
        completed_per_minute: rate(|r| r.completed),
        failed_per_minute: rate(|r| r.failed_attempts),
        retried_per_minute: rate(|r| r.retried),
        arrivals_per_minute: match (trend, claimed) {
            (Some(trend), Some(claimed)) => Some((trend.per_minute + claimed).max(0.0)),
            _ => None,
        },
        mean_claim_wait_seconds: match (wait_ms, claimed) {
            (Some(wait), Some(claimed)) if claimed > 0.0 => Some(wait / claimed / 1000.0),
            _ => None,
        },
        rate_limited: recent
            .iter()
            .any(|row| row.rate_limited.is_some_and(|n| n > 0)),
    }
}

fn capacity(summary: Option<&QueueRuntimeSummary>) -> Option<u64> {
    let summary = summary?;
    let config = summary.config.as_ref()?;
    if summary.config_mismatch || config.mode != QueueRuntimeMode::HardReserved {
        return None;
    }
    Some(u64::from(config.max_workers?) * summary.live_instances as u64)
}

pub fn format_duration(seconds: f64) -> String {
    let value = seconds.max(0.0).round() as u64;
    match value {
        0..=59 => format!("{value}s"),
        60..=3599 => format!("{}m {}s", value / 60, value % 60),
        3600..=86399 => format!("{}h {}m", value / 3600, (value % 3600) / 60),
        _ => format!("{}d {}h", value / 86400, (value % 86400) / 3600),
    }
}

fn group_thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn format_rate(per_minute: f64) -> String {
    let sign = if per_minute > 0.0 { "+" } else { "−" };
    let magnitude = per_minute.abs();
    if magnitude >= 10.0 {
        format!("{sign}{}/min", group_thousands(magnitude.round() as u64))
    } else {
        format!("{sign}{magnitude:.1}/min")
    }
}

pub fn assess_queue(
    now: DateTime<Utc>,
    queue: &QueueOverview,
    rows: &[QueueHistoryRow],
    summary: Option<&QueueRuntimeSummary>,
    history_available: bool,
) -> QueueAssessment {
    let backlog = queue.available;
    let wait = queue.lag_seconds.unwrap_or(0.0);
    let trend = history_available
        .then(|| depth_trend(now, rows, backlog))
        .flatten();
    let flow = flow(now, rows, trend);
    let live_workers = summary.map_or(0, |s| s.live_instances as u64);
    let in_flight = summary.map_or(0, |s| s.total_in_flight);
    let capacity = capacity(summary);
    let threshold = MIN_TREND_JOBS.max(backlog as f64 * MIN_TREND_SHARE);

    let limiting = (backlog > 0).then(|| {
        if queue.paused {
            LimitedBy::Paused
        } else if live_workers == 0 {
            LimitedBy::NoWorkers
        } else if flow.rate_limited {
            LimitedBy::RateLimit
        } else if capacity.is_some_and(|cap| in_flight >= cap) {
            LimitedBy::WorkerCapacity
        } else {
            LimitedBy::Nothing
        }
    });

    let (health, reason, clear_seconds) = if queue.paused {
        (
            QueueHealth::Paused,
            if backlog > 0 {
                format!("{backlog} waiting while paused")
            } else {
                "Nothing waiting".to_string()
            },
            None,
        )
    } else if backlog > 0 && wait >= STALL_AFTER_SECONDS && live_workers == 0 {
        (
            QueueHealth::Stalled,
            format!(
                "No live worker serves this queue; oldest has waited {}",
                format_duration(wait)
            ),
            None,
        )
    } else if backlog > 0
        && wait >= STALL_AFTER_SECONDS
        && queue.running == 0
        && flow.claimed_per_minute.is_none_or(|rate| rate == 0.0)
    {
        (
            QueueHealth::Stalled,
            format!(
                "Nothing running or claimed; oldest has waited {}",
                format_duration(wait)
            ),
            None,
        )
    } else if trend.is_some_and(|t| backlog > 0 && t.change > threshold) {
        let trend = trend.expect("checked above");
        (
            QueueHealth::Behind,
            format!(
                "Backlog {} over {}m",
                format_rate(trend.per_minute),
                trend.minutes.round()
            ),
            None,
        )
    } else if wait >= SLOW_WAIT_SECONDS {
        (
            QueueHealth::Slow,
            format!("Oldest has waited {}", format_duration(wait)),
            None,
        )
    } else if trend.is_some_and(|t| backlog > 0 && t.change < -threshold) {
        let clear = backlog as f64 / -trend.expect("checked above").per_minute * 60.0;
        (
            QueueHealth::Draining,
            format!("Clears in about {} at this pace", format_duration(clear)),
            Some(clear),
        )
    } else if backlog == 0 {
        (QueueHealth::Idle, "Nothing waiting".to_string(), None)
    } else if trend.is_none() {
        (
            QueueHealth::Unknown,
            if history_available {
                "Needs a few minutes of history".to_string()
            } else {
                "No queue history yet; run v051 and a current maintenance leader".to_string()
            },
            None,
        )
    } else {
        (
            QueueHealth::Steady,
            "Backlog holding level".to_string(),
            None,
        )
    };

    let needs_attention = matches!(
        health,
        QueueHealth::Stalled | QueueHealth::Behind | QueueHealth::Slow
    ) || (health == QueueHealth::Paused && backlog > 0);
    // Only explain what holds work back when work is being held back; on a
    // steady or draining queue "spare capacity" is noise.
    let limited_by = limiting.filter(|_| needs_attention);
    let rank = match health {
        QueueHealth::Paused if backlog == 0 => QueueHealth::Idle as u8,
        other => other as u8,
    };

    QueueAssessment {
        queue: queue.queue.clone(),
        health,
        label: health.label(),
        reason,
        needs_attention,
        rank,
        limited_by,
        trend,
        clear_seconds,
        flow,
        live_workers,
        in_flight,
        capacity,
    }
}

fn plural(count: usize, noun: &str) -> String {
    format!("{count} {noun}{}", if count == 1 { "" } else { "s" })
}

pub fn build_report(
    now: DateTime<Utc>,
    queues: &[QueueOverview],
    history: &BTreeMap<String, Vec<QueueHistoryRow>>,
    summaries: &[QueueRuntimeSummary],
    history_installed: bool,
) -> HealthReport {
    let history_since = history
        .values()
        .filter_map(|rows| rows.first().map(|row| row.bucket))
        .min();
    let history_available = history_installed && history_since.is_some();
    let mut assessed: Vec<QueueAssessment> = queues
        .iter()
        .map(|queue| {
            assess_queue(
                now,
                queue,
                history.get(&queue.queue).map_or(&[][..], Vec::as_slice),
                summaries.iter().find(|s| s.queue == queue.queue),
                history_available,
            )
        })
        .collect();
    let available = |name: &str| {
        queues
            .iter()
            .find(|q| q.queue == name)
            .map_or(0, |q| q.available)
    };
    assessed.sort_by(|a, b| {
        a.rank
            .cmp(&b.rank)
            .then(available(&b.queue).cmp(&available(&a.queue)))
            .then(a.queue.cmp(&b.queue))
    });

    let count = |health: QueueHealth| assessed.iter().filter(|a| a.health == health).count();
    let names = |health: QueueHealth| {
        assessed
            .iter()
            .filter(|a| a.health == health)
            .take(3)
            .map(|a| a.queue.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let paused_with_work = assessed
        .iter()
        .filter(|a| a.health == QueueHealth::Paused && a.needs_attention)
        .count();
    let mut others: Vec<String> = Vec::new();
    if count(QueueHealth::Behind) > 0 {
        others.push(format!(
            "{} falling behind",
            plural(count(QueueHealth::Behind), "queue")
        ));
    }
    if count(QueueHealth::Slow) > 0 {
        others.push(format!(
            "{} with long waits",
            plural(count(QueueHealth::Slow), "queue")
        ));
    }
    if paused_with_work > 0 {
        others.push(format!(
            "{} holding work",
            plural(paused_with_work, "paused queue")
        ));
    }

    let (status, title, detail) = if count(QueueHealth::Stalled) > 0 {
        let mut parts = vec![format!(
            "Waiting work nobody is claiming: {}.",
            names(QueueHealth::Stalled)
        )];
        parts.extend(others);
        (
            OverallStatus::Critical,
            format!(
                "{} not being worked",
                plural(count(QueueHealth::Stalled), "queue")
            ),
            parts.join(" · "),
        )
    } else if count(QueueHealth::Behind) > 0 {
        let mut parts = vec![format!(
            "Backlog growing in {}.",
            names(QueueHealth::Behind)
        )];
        parts.extend(others.into_iter().skip(1));
        (
            OverallStatus::Warning,
            format!(
                "{} falling behind",
                plural(count(QueueHealth::Behind), "queue")
            ),
            parts.join(" · "),
        )
    } else if count(QueueHealth::Slow) > 0 {
        let mut parts = vec![format!(
            "Oldest jobs waiting over {} in {}.",
            format_duration(SLOW_WAIT_SECONDS),
            names(QueueHealth::Slow)
        )];
        parts.extend(others.into_iter().skip(1));
        (
            OverallStatus::Warning,
            format!(
                "{} with long waits",
                plural(count(QueueHealth::Slow), "queue")
            ),
            parts.join(" · "),
        )
    } else if paused_with_work > 0 {
        (
            OverallStatus::Ok,
            format!("{} holding work", plural(paused_with_work, "paused queue")),
            "Every running queue is keeping up. Resume paused queues when ready.".to_string(),
        )
    } else if count(QueueHealth::Unknown) > 0
        && count(QueueHealth::Unknown)
            == queues
                .iter()
                .filter(|q| q.available > 0 && !q.paused)
                .count()
    {
        (
            OverallStatus::Unknown,
            "Not enough history yet".to_string(),
            if history_available {
                "Trends appear after a few minutes of samples. No queue is stalled or waiting long."
                    .to_string()
            } else {
                "No queue history yet, so only stalls and long waits are detected.".to_string()
            },
        )
    } else {
        (
            OverallStatus::Ok,
            "Keeping up".to_string(),
            format!(
                "{} steady, draining, or idle.",
                plural(assessed.len(), "queue")
            ),
        )
    };

    HealthReport {
        status,
        title,
        detail,
        generated_at: now,
        history_available,
        history_since,
        queues: assessed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use awa_model::admin::QueueRuntimeConfigSnapshot;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000, 0).unwrap()
    }

    fn queue(name: &str, available: i64) -> QueueOverview {
        QueueOverview {
            queue: name.into(),
            display_name: None,
            description: None,
            owner: None,
            docs_url: None,
            tags: vec![],
            extra: serde_json::Value::Null,
            descriptor_last_seen_at: None,
            descriptor_stale: false,
            descriptor_mismatch: false,
            total_queued: available,
            scheduled: 0,
            available,
            retryable: 0,
            running: 10,
            failed: 0,
            waiting_external: 0,
            completed_last_hour: 600,
            lag_seconds: Some(5.0),
            paused: false,
        }
    }

    /// One row per minute, oldest first, ending one minute before `now`.
    fn rows(name: &str, depths: &[i64], claimed: Option<i64>) -> Vec<QueueHistoryRow> {
        let n = depths.len() as i64;
        depths
            .iter()
            .enumerate()
            .map(|(i, &available)| QueueHistoryRow {
                queue: name.into(),
                bucket: now() - chrono::TimeDelta::minutes(n - i as i64),
                available,
                running: 10,
                scheduled: 0,
                retryable: 0,
                failed: 0,
                waiting_external: 0,
                lag_seconds: Some(5.0),
                paused: false,
                claimed,
                completed: claimed,
                failed_attempts: claimed.map(|_| 0),
                retried: claimed.map(|_| 0),
                rate_limited: claimed.map(|_| 0),
                claim_wait_ms: claimed.map(|c| c * 200),
                live_workers: 2,
                capacity: Some(16),
            })
            .collect()
    }

    fn summary(name: &str, live: usize, in_flight: u64) -> QueueRuntimeSummary {
        QueueRuntimeSummary {
            queue: name.into(),
            instance_count: live,
            live_instances: live,
            stale_instances: 0,
            healthy_instances: live,
            total_in_flight: in_flight,
            overflow_held_total: None,
            config_mismatch: false,
            config: Some(QueueRuntimeConfigSnapshot {
                mode: QueueRuntimeMode::HardReserved,
                max_workers: Some(8),
                min_workers: None,
                weight: None,
                global_max_workers: None,
                poll_interval_ms: 200,
                deadline_duration_secs: 300,
                priority_aging_interval_secs: 60,
                claimers: None,
                claim_batch_size: None,
                dlq_enabled: Some(true),
                rate_limit: None,
            }),
        }
    }

    #[test]
    fn waiting_work_without_live_workers_is_stalled() {
        let mut q = queue("email", 50);
        q.lag_seconds = Some(90.0);
        let a = assess_queue(now(), &q, &[], None, true);
        assert_eq!(a.health, QueueHealth::Stalled);
        assert_eq!(a.limited_by, Some(LimitedBy::NoWorkers));
        assert!(a.reason.contains("No live worker"));
    }

    #[test]
    fn live_workers_claiming_nothing_is_stalled() {
        let mut q = queue("email", 50);
        q.lag_seconds = Some(90.0);
        q.running = 0;
        let a = assess_queue(
            now(),
            &q,
            &rows("email", &[50, 50, 50, 50], Some(0)),
            Some(&summary("email", 2, 0)),
            true,
        );
        assert_eq!(a.health, QueueHealth::Stalled);
        assert_eq!(a.limited_by, Some(LimitedBy::Nothing));
    }

    #[test]
    fn a_fresh_queue_is_not_stalled_before_the_wait_threshold() {
        let mut q = queue("email", 50);
        q.running = 0;
        q.lag_seconds = Some(10.0);
        assert_eq!(
            assess_queue(now(), &q, &[], Some(&summary("email", 1, 0)), false).health,
            QueueHealth::Unknown
        );
    }

    #[test]
    fn growing_backlog_falls_behind_and_infers_arrivals() {
        let a = assess_queue(
            now(),
            &queue("email", 1500),
            &rows("email", &[1000, 1100, 1200, 1300, 1400], Some(300)),
            Some(&summary("email", 2, 16)),
            true,
        );
        assert_eq!(a.health, QueueHealth::Behind);
        let trend = a.trend.unwrap();
        assert!((trend.per_minute - 100.0).abs() < 1e-6);
        assert_eq!(a.flow.claimed_per_minute, Some(300.0));
        assert!((a.flow.arrivals_per_minute.unwrap() - 400.0).abs() < 1e-6);
        assert_eq!(a.flow.mean_claim_wait_seconds, Some(0.2));
        assert_eq!(a.limited_by, Some(LimitedBy::WorkerCapacity));
        assert!(a.needs_attention);
    }

    #[test]
    fn small_wobble_on_a_large_backlog_is_steady() {
        let a = assess_queue(
            now(),
            &queue("email", 184_000),
            &rows(
                "email",
                &[184_000, 184_400, 183_900, 184_300, 184_100],
                Some(2000),
            ),
            Some(&summary("email", 2, 3)),
            true,
        );
        assert_eq!(a.health, QueueHealth::Steady);
        assert!(!a.needs_attention);
        assert_eq!(a.limited_by, None);
    }

    #[test]
    fn draining_estimates_time_to_clear() {
        let a = assess_queue(
            now(),
            &queue("email", 600),
            &rows("email", &[1600, 1400, 1200, 1000, 800], Some(400)),
            Some(&summary("email", 2, 4)),
            true,
        );
        assert_eq!(a.health, QueueHealth::Draining);
        assert!((a.clear_seconds.unwrap() - 180.0).abs() < 1e-6);
        assert_eq!(a.reason, "Clears in about 3m 0s at this pace");
    }

    #[test]
    fn long_wait_with_level_depth_is_slow() {
        let mut q = queue("email", 1000);
        q.lag_seconds = Some(900.0);
        let a = assess_queue(
            now(),
            &q,
            &rows("email", &[1000, 1000, 1000, 1000], Some(10)),
            Some(&summary("email", 1, 1)),
            true,
        );
        assert_eq!(a.health, QueueHealth::Slow);
    }

    #[test]
    fn rate_limit_hits_explain_a_backlog() {
        let mut history = rows("email", &[1000, 1000, 1000, 1000], Some(10));
        history[2].rate_limited = Some(40);
        let mut q = queue("email", 1000);
        q.lag_seconds = Some(600.0);
        let a = assess_queue(now(), &q, &history, Some(&summary("email", 1, 1)), true);
        assert_eq!(a.limited_by, Some(LimitedBy::RateLimit));
        assert!(a.flow.rate_limited);
    }

    #[test]
    fn unknown_counters_leave_rates_unknown() {
        let mut history = rows("email", &[1000, 1000, 1000, 1000], Some(10));
        for row in &mut history[..3] {
            row.claimed = None;
        }
        let a = assess_queue(now(), &queue("email", 1000), &history, None, true);
        assert_eq!(a.flow.claimed_per_minute, None);
        assert_eq!(a.flow.arrivals_per_minute, None);
    }

    #[test]
    fn paused_queues_need_attention_only_while_holding_work() {
        let mut q = queue("email", 10);
        q.paused = true;
        let held = assess_queue(now(), &q, &[], None, true);
        assert!(held.needs_attention);
        assert_eq!(held.limited_by, Some(LimitedBy::Paused));
        q.available = 0;
        let empty = assess_queue(now(), &q, &[], None, true);
        assert!(!empty.needs_attention);
        assert_eq!(empty.rank, QueueHealth::Idle as u8);
    }

    #[test]
    fn report_orders_by_risk_and_picks_the_worst_status() {
        let mut stalled = queue("stalled", 100);
        stalled.lag_seconds = Some(120.0);
        let queues = vec![
            queue("steady", 9000),
            stalled,
            queue("idle", 0),
            queue("behind", 500),
        ];
        let mut history = BTreeMap::new();
        history.insert(
            "steady".to_string(),
            rows("steady", &[9000, 9000, 9000, 9000], Some(5)),
        );
        history.insert("idle".to_string(), rows("idle", &[0, 0, 0], Some(0)));
        history.insert(
            "behind".to_string(),
            rows("behind", &[100, 200, 300, 400], Some(5)),
        );
        let summaries = vec![
            summary("steady", 1, 1),
            summary("idle", 1, 0),
            summary("behind", 1, 1),
        ];
        let report = build_report(now(), &queues, &history, &summaries, true);
        let order: Vec<&str> = report.queues.iter().map(|q| q.queue.as_str()).collect();
        assert_eq!(order, ["stalled", "behind", "steady", "idle"]);
        assert_eq!(report.status, OverallStatus::Critical);
        assert_eq!(report.title, "1 queue not being worked");
        assert!(report.detail.contains("1 queue falling behind"));
    }

    #[test]
    fn report_without_history_is_unknown_not_ok() {
        let report = build_report(
            now(),
            &[queue("email", 10)],
            &BTreeMap::new(),
            &[summary("email", 1, 1)],
            false,
        );
        assert_eq!(report.status, OverallStatus::Unknown);
        assert!(!report.history_available);
    }

    #[test]
    fn idle_system_is_keeping_up() {
        let report = build_report(
            now(),
            &[queue("email", 0)],
            &BTreeMap::new(),
            &[summary("email", 1, 0)],
            true,
        );
        assert_eq!(report.status, OverallStatus::Ok);
        assert_eq!(report.title, "Keeping up");
    }

    #[test]
    fn durations_format_consistently() {
        assert_eq!(format_duration(2.4), "2s");
        assert_eq!(format_duration(458.0), "7m 38s");
        assert_eq!(format_duration(3720.0), "1h 2m");
        assert_eq!(format_rate(2500.0), "+2,500/min");
        assert_eq!(format_rate(-1_234_567.0), "−1,234,567/min");
        assert_eq!(format_rate(0.25), "+0.2/min");
    }
}
