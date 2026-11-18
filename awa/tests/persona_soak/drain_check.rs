//! Drain semantics, as `admin::drain_queue` documents them and as #801 settled them for
//! bulk operations: a drain covers the jobs that existed when it started. After it
//! returns,
//!
//! - every job committed before it started that had not begun to run is cancelled,
//!   including a scheduled job promoted while the drain ran (`drain_left_job`);
//! - no job whose insert began after it started has been cancelled by it
//!   (`drain_cancelled_new_job`).
//!
//! Producers of a drained queue record their inserts here with the time each insert call
//! began and returned. A job counts as enqueued after the drain only when its insert began
//! more than `AWA_SOAK_DRAIN_MARGIN_MS` after the drain call, and its id is not below the
//! job id sequence as read just before the call, since a drain reads its job id bound from
//! that sequence once its first statement runs.

use super::ledger::flag;
use super::shared::Shared;
use serde::Serialize;
use sqlx::PgPool;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::Mutex;

const WATCHED_JOBS: usize = 100_000;

#[derive(Debug, Clone, Serialize)]
pub struct WatchedJob {
    pub job_id: i64,
    pub queue: String,
    pub insert_started_ms: i64,
    pub committed_ms: i64,
    /// Run time the job was scheduled for, when it was not immediately available.
    pub scheduled_for_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct DrainResult {
    pub queue: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub drained: Option<u64>,
    pub before_checked: usize,
    pub after_checked: usize,
    pub left: Vec<String>,
    pub cancelled_new: Vec<String>,
    pub skipped: Option<String>,
}

#[derive(Default)]
pub struct DrainLog {
    watched: Mutex<VecDeque<WatchedJob>>,
    results: Mutex<Vec<DrainResult>>,
}

impl DrainLog {
    pub fn note_enqueued(&self, jobs: impl IntoIterator<Item = WatchedJob>) {
        let mut watched = self.watched.lock().expect("drain watch");
        for job in jobs {
            if watched.len() >= WATCHED_JOBS {
                watched.pop_front();
            }
            watched.push_back(job);
        }
    }

    fn watched(&self, queue: &str) -> Vec<WatchedJob> {
        self.watched
            .lock()
            .expect("drain watch")
            .iter()
            .filter(|job| super::intent::logical(&job.queue) == queue)
            .cloned()
            .collect()
    }

    pub fn results(&self) -> Vec<DrainResult> {
        self.results.lock().expect("drain results").clone()
    }
}

/// Splits the watched jobs of a queue into those a drain over `[start_ms, ...)` must
/// cancel (committed before it started and not yet started by a handler) and those it
/// must leave alone (insert begun after `start_ms + margin_ms`, id at or above `id_floor`).
pub fn classify(
    watched: &[WatchedJob],
    start_ms: i64,
    margin_ms: i64,
    id_floor: i64,
    started: impl Fn(i64) -> bool,
) -> (Vec<i64>, Vec<i64>) {
    let before = watched
        .iter()
        .filter(|job| job.committed_ms < start_ms && !started(job.job_id))
        .map(|job| job.job_id)
        .collect();
    let after = watched
        .iter()
        .filter(|job| job.insert_started_ms > start_ms + margin_ms && job.job_id >= id_floor)
        .map(|job| job.job_id)
        .collect();
    (before, after)
}

/// Reads the job id sequence's next value, the floor for jobs enqueued after a drain.
pub async fn id_floor(shared: &Shared, pool: &PgPool) -> Option<i64> {
    let schema = shared.info.queue_storage_schema.clone()?;
    sqlx::query_scalar(awa::audited_sql(format!(
        "SELECT {schema}.sequence_next_value('job_id_seq')"
    )))
    .fetch_one(pool)
    .await
    .ok()
}

/// Checks a finished drain of `queue` (logical name). `pool` should be a sampler pool.
pub async fn verify(
    shared: &Shared,
    pool: &PgPool,
    queue: &str,
    start_ms: i64,
    end_ms: i64,
    floor: Option<i64>,
    drained: Option<u64>,
) {
    let mut result = DrainResult {
        queue: queue.to_string(),
        start_ms,
        end_ms,
        drained,
        ..Default::default()
    };
    let watched = shared.drains.watched(queue);
    let Some(schema) = shared.info.queue_storage_schema.clone() else {
        return;
    };
    let (Some(floor), Some(_)) = (floor, drained) else {
        result.skipped = Some("the drain or the id floor read failed".into());
        shared
            .drains
            .results
            .lock()
            .expect("drain results")
            .push(result);
        return;
    };
    if watched.is_empty() {
        return;
    }
    let started = |job_id: i64| {
        shared.ledger.get(job_id).is_some_and(|record| {
            record.executions > 0
                || record.has(flag::OPERATOR_REMOVED)
                || record.has_durable_terminal()
        })
    };
    let (before, after) = classify(
        &watched,
        start_ms,
        shared.config.checks.drain_margin.as_millis() as i64,
        floor,
        started,
    );
    result.before_checked = before.len();
    result.after_checked = after.len();
    let left: Result<Vec<(i64, String, Option<f64>)>, _> = sqlx::query_as(awa::audited_sql(format!(
        "WITH {available}
         SELECT job_id, 'deferred:' || state::text, extract(epoch FROM run_at - now())::float8
           FROM {schema}.deferred_jobs WHERE job_id = ANY($1) AND state IN ('scheduled', 'retryable')
         UNION ALL
         SELECT job_id, 'available', NULL::float8 FROM available WHERE job_id = ANY($1)",
        available = super::oracle::available_cte(&schema)
    )))
    .bind(&before)
    .fetch_all(pool)
    .await;
    let cancelled: Result<Vec<i64>, _> = sqlx::query_scalar(awa::audited_sql(format!(
        "SELECT job_id FROM {schema}.done_entries WHERE state = 'cancelled' AND job_id = ANY($1)"
    )))
    .bind(&after)
    .fetch_all(pool)
    .await;
    let (left, cancelled) = match (left, cancelled) {
        (Ok(left), Ok(cancelled)) => (left, cancelled),
        (Err(error), _) | (_, Err(error)) => {
            shared.sample_skipped("drain.verify", &error);
            result.skipped = Some(error.to_string());
            shared
                .drains
                .results
                .lock()
                .expect("drain results")
                .push(result);
            return;
        }
    };
    shared.samplers.ok("drain.verify");
    let describe = |job_id: i64, found: &str| {
        let job = watched.iter().find(|job| job.job_id == job_id);
        serde_json::json!({
            "job_id": job_id,
            "found": found,
            "insert_started_s": job.map(|job| job.insert_started_ms as f64 / 1000.0),
            "committed_s": job.map(|job| job.committed_ms as f64 / 1000.0),
            "scheduled_for_s": job.and_then(|job| job.scheduled_for_ms).map(|at| at as f64 / 1000.0),
            "promoted_during_drain": job.and_then(|job| job.scheduled_for_ms).is_some_and(|at| at >= start_ms && at <= end_ms),
            "drain_s": [start_ms as f64 / 1000.0, end_ms as f64 / 1000.0],
            "id_floor": floor,
        })
        .to_string()
    };
    result.left = left
        .iter()
        // A job claimed by a worker between the drain and this read is no longer here.
        .filter(|(job_id, _, _)| !started(*job_id))
        .map(|(job_id, found, _)| describe(*job_id, found))
        .collect();
    result.cancelled_new = cancelled
        .iter()
        .filter(|job_id| {
            !shared
                .ledger
                .get(**job_id)
                .is_some_and(|record| record.has(flag::OPERATOR_REMOVED))
        })
        .map(|job_id| describe(*job_id, "done:cancelled"))
        .collect();
    if !result.left.is_empty() {
        shared.violation_at(
            "drain_left_job",
            end_ms,
            format!(
                "drain of {queue} ({:.1}s to {:.1}s) left {} job(s) that were enqueued before it started and had not run",
                start_ms as f64 / 1000.0,
                end_ms as f64 / 1000.0,
                result.left.len()
            ),
            result.left.iter().take(25).cloned().collect(),
        );
    }
    if !result.cancelled_new.is_empty() {
        shared.violation_at(
            "drain_cancelled_new_job",
            end_ms,
            format!(
                "drain of {queue} ({:.1}s to {:.1}s) cancelled {} job(s) whose insert began after it started",
                start_ms as f64 / 1000.0,
                end_ms as f64 / 1000.0,
                result.cancelled_new.len()
            ),
            result.cancelled_new.iter().take(25).cloned().collect(),
        );
    }
    shared
        .drains
        .results
        .lock()
        .expect("drain results")
        .push(result);
}

pub fn render(out: &mut String, results: &[DrainResult], shared: &Shared) {
    let _ = writeln!(out, "## Drain semantics");
    if results.is_empty() {
        let _ = writeln!(out, "- no drain of a watched queue ran\n");
        return;
    }
    let checked: Vec<&DrainResult> = results
        .iter()
        .filter(|result| result.skipped.is_none())
        .collect();
    let _ = writeln!(
        out,
        "- {} drain(s) of watched queues checked, {} skipped; {} job(s) enqueued before a drain checked to be cancelled or running, {} enqueued after it (insert begun more than {} ms after the call) checked to be left alone",
        checked.len(),
        results.len() - checked.len(),
        checked.iter().map(|result| result.before_checked).sum::<usize>(),
        checked.iter().map(|result| result.after_checked).sum::<usize>(),
        shared.config.checks.drain_margin.as_millis()
    );
    let _ = writeln!(
        out,
        "- left behind: {}; cancelled although enqueued after: {}",
        checked
            .iter()
            .map(|result| result.left.len())
            .sum::<usize>(),
        checked
            .iter()
            .map(|result| result.cancelled_new.len())
            .sum::<usize>()
    );
    let _ = writeln!(out);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(job_id: i64, insert_started_ms: i64, committed_ms: i64) -> WatchedJob {
        WatchedJob {
            job_id,
            queue: "drain".into(),
            insert_started_ms,
            committed_ms,
            scheduled_for_ms: None,
        }
    }

    #[test]
    fn jobs_are_split_by_commit_before_and_insert_after_the_drain() {
        let watched = [
            job(1, 900, 950),     // before, never ran: must be cancelled
            job(2, 900, 950),     // before, already running: left to finish
            job(3, 990, 1_020),   // straddles the start: neither
            job(4, 1_500, 1_600), // inside the margin: neither
            job(5, 2_500, 2_600), // after by time, but its id is below the floor: neither
            job(6, 2_500, 2_600), // after: must be left alone
        ];
        let (before, after) = classify(&watched, 1_000, 1_000, 6, |job_id| job_id == 2);
        assert_eq!(before, vec![1]);
        assert_eq!(after, vec![6]);
    }
}
