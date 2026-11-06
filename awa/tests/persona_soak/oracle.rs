//! The invariant oracle: periodic checks while the load runs and a final
//! reconciliation of the client ledger against the database.

use super::ledger::{expect, flag, samples_terminal_proof};
use super::pgserver::PgWatch;
use super::shared::Shared;
use super::stop::Stop;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const MAX_ROWS: usize = 25;

/// count, min and max job id, min and max lane sequence.
type ForensicRow = (i64, Option<i64>, Option<i64>, Option<i64>, Option<i64>);

fn schema(shared: &Shared) -> String {
    shared
        .info
        .queue_storage_schema
        .clone()
        .unwrap_or_else(|| "awa".to_string())
}

/// Every terminal outcome the store records, one row per `(job, attempt)`.
fn terminal_cte(schema: &str) -> String {
    format!(
        "terminal AS (
            SELECT job_id, run_lease, 'done:' || state::text AS src FROM {schema}.done_entries
            UNION ALL SELECT job_id, run_lease, 'dlq' FROM {schema}.dlq_entries
            UNION ALL SELECT b.job_id, b.run_lease, 'receipt_completed'
              FROM {schema}.receipt_completion_batches c,
                   LATERAL unnest(c.job_ids, c.run_leases) AS b(job_id, run_lease)
        )"
    )
}

/// Jobs the lane cursors still consider claimable: at or after the claim cursor and
/// not tombstoned.
fn available_cte(schema: &str) -> String {
    format!(
        "heads AS MATERIALIZED (
            SELECT queue, priority, enqueue_shard, {schema}.sequence_next_value(seq_name) AS claim_seq
            FROM {schema}.queue_claim_heads
        ),
        available AS MATERIALIZED (
            SELECT r.job_id, r.run_lease, r.queue
            FROM {schema}.ready_entries r
            JOIN heads h ON h.queue = r.queue AND h.priority = r.priority AND h.enqueue_shard = r.enqueue_shard
            WHERE r.lane_seq >= h.claim_seq
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.ready_tombstones t
                WHERE t.ready_slot = r.ready_slot AND t.ready_generation = r.ready_generation
                  AND t.queue = r.queue AND t.priority = r.priority
                  AND t.enqueue_shard = r.enqueue_shard AND t.lane_seq = r.lane_seq
              )
        )"
    )
}

#[derive(Default)]
struct StallTracker {
    /// queue -> (claim cursor, millis when the cursor last moved, already reported)
    cursors: HashMap<String, (i64, i64, bool)>,
}

pub struct Oracle {
    shared: Arc<Shared>,
    pool: PgPool,
    watch: Arc<PgWatch>,
    stall: StallTracker,
    last_cancel_scan_ms: i64,
    reported_duplicates: std::collections::HashSet<(i64, i64)>,
    reported_cancel: std::collections::HashSet<i64>,
    initial_stats_reset: std::sync::Mutex<Option<Option<String>>>,
}

impl Oracle {
    pub fn new(shared: Arc<Shared>, pool: PgPool, watch: Arc<PgWatch>) -> Self {
        Self {
            shared,
            pool,
            watch,
            stall: StallTracker::default(),
            last_cancel_scan_ms: 0,
            reported_duplicates: Default::default(),
            reported_cancel: Default::default(),
            initial_stats_reset: Default::default(),
        }
    }

    /// Runs the periodic checks until `stop`.
    pub async fn run(mut self, stop: Stop) -> Self {
        let mut tick = 0u64;
        loop {
            if stop.sleep(Duration::from_secs(5)).await {
                return self;
            }
            tick += 1;
            self.probe_server().await;
            self.check_stall().await;
            self.check_promotion().await;
            if tick.is_multiple_of(2) {
                self.check_terminal_uniqueness().await;
                self.check_unexplained_cancels().await;
            }
            if tick.is_multiple_of(3) {
                self.sample_resources().await;
            }
        }
    }

    /// Crash recovery discards the cumulative statistics, so a changed reset time means the
    /// server restarted its processes even though the postmaster, and its start time, survived.
    async fn probe_stats_reset(&self) {
        let reset: Result<Option<String>, _> =
            sqlx::query_scalar("SELECT stats_reset::text FROM pg_stat_bgwriter")
                .fetch_one(&self.pool)
                .await;
        let reset = match reset {
            Ok(reset) => reset,
            Err(error) => {
                self.shared
                    .incomplete(format!("crash-recovery probe failed: {error}"), vec![]);
                return;
            }
        };
        let mut initial = self.initial_stats_reset.lock().expect("stats reset");
        match initial.as_ref() {
            None => *initial = Some(reset),
            Some(first) if *first != reset => {
                let message = format!("pg_stat_bgwriter.stats_reset changed from {first:?} to {reset:?}: the server went through crash recovery");
                *initial = Some(reset);
                drop(initial);
                self.shared
                    .violation("server_crash_recovery", message, vec![]);
            }
            Some(_) => {}
        }
    }

    async fn probe_server(&self) {
        self.probe_stats_reset().await;
        let mut last_error = String::new();
        for attempt in 0..4 {
            match PgWatch::postmaster_start(&self.pool).await {
                Ok(start) if start == self.watch.started_at => return,
                Ok(start) => {
                    self.shared.violation(
                        "server_restart",
                        format!(
                            "pg_postmaster_start_time changed from {} to {start}",
                            self.watch.started_at
                        ),
                        vec![],
                    );
                    return;
                }
                Err(error) => {
                    last_error = error.to_string();
                    tokio::time::sleep(Duration::from_secs(1 + attempt)).await;
                }
            }
        }
        self.shared.run.add("server_probe_failures", 1);
        self.shared.incomplete(
            format!("server probe failed repeatedly: {last_error}"),
            vec![],
        );
    }

    async fn check_terminal_uniqueness(&mut self) {
        let schema = schema(&self.shared);
        let terminal = terminal_cte(&schema);
        let started = std::time::Instant::now();

        let duplicates: Result<Vec<(i64, i64, Vec<String>)>, _> = sqlx::query_as(&format!(
            "WITH {terminal}
             SELECT job_id, run_lease, array_agg(src ORDER BY src)
             FROM terminal GROUP BY job_id, run_lease HAVING count(*) > 1
             ORDER BY job_id LIMIT {MAX_ROWS}"
        ))
        .fetch_all(&self.pool)
        .await;
        match duplicates {
            Ok(rows) => {
                let fresh: Vec<_> = rows
                    .iter()
                    .filter(|(job_id, run_lease, _)| {
                        self.reported_duplicates.insert((*job_id, *run_lease))
                    })
                    .collect();
                if !fresh.is_empty() {
                    self.shared.violation(
                        "duplicate_terminal_rows",
                        format!("{} job attempt(s) have more than one terminal row", fresh.len()),
                        fresh
                            .iter()
                            .map(|(job_id, run_lease, sources)| {
                                format!("{{\"job_id\":{job_id},\"run_lease\":{run_lease},\"terminal_rows\":{sources:?}}}")
                            })
                            .collect(),
                    );
                }
            }
            Err(error) => self
                .shared
                .incomplete(format!("terminal uniqueness query failed: {error}"), vec![]),
        }

        let available = available_cte(&schema);
        let overlap: Result<Vec<(i64, String, i64, i64)>, _> = sqlx::query_as(&format!(
            "WITH {terminal}, {available}
             SELECT t.job_id, 'available'::text, t.run_lease, a.run_lease
               FROM terminal t JOIN available a ON a.job_id = t.job_id AND a.run_lease <= t.run_lease
             UNION ALL
             SELECT t.job_id, 'deferred:' || d.state::text, t.run_lease, d.run_lease
               FROM terminal t JOIN {schema}.deferred_jobs d ON d.job_id = t.job_id AND d.run_lease <= t.run_lease
             UNION ALL
             SELECT t.job_id, 'leased:' || l.state::text, t.run_lease, l.run_lease
               FROM terminal t JOIN {schema}.leases l ON l.job_id = t.job_id AND l.run_lease <= t.run_lease
             LIMIT {MAX_ROWS}"
        ))
        .fetch_all(&self.pool)
        .await;
        match overlap {
            Ok(rows) if !rows.is_empty() => {
                // Confirm on a second read: a single statement is one snapshot, so a
                // repeat that still shows the overlap is not a race with this probe.
                tokio::time::sleep(Duration::from_millis(500)).await;
                let ids: Vec<i64> = rows.iter().map(|row| row.0).collect();
                let still: Result<Vec<(i64, String)>, _> = sqlx::query_as(&format!(
                    "WITH {terminal}, {available}
                     SELECT t.job_id, 'available'::text FROM terminal t JOIN available a ON a.job_id = t.job_id AND a.run_lease <= t.run_lease WHERE t.job_id = ANY($1)
                     UNION ALL SELECT t.job_id, 'deferred' FROM terminal t JOIN {schema}.deferred_jobs d ON d.job_id = t.job_id AND d.run_lease <= t.run_lease WHERE t.job_id = ANY($1)
                     UNION ALL SELECT t.job_id, 'leased' FROM terminal t JOIN {schema}.leases l ON l.job_id = t.job_id AND l.run_lease <= t.run_lease WHERE t.job_id = ANY($1)"
                ))
                .bind(&ids)
                .fetch_all(&self.pool)
                .await;
                let confirmed: std::collections::HashSet<i64> = match still {
                    Ok(rows) => rows.into_iter().map(|row| row.0).collect(),
                    Err(error) => {
                        self.shared.incomplete(
                            format!("terminal/live confirmation failed: {error}"),
                            vec![],
                        );
                        Default::default()
                    }
                };
                let confirmed_rows: Vec<String> = rows
                    .iter()
                    .filter(|row| confirmed.contains(&row.0))
                    .filter(|row| self.reported_duplicates.insert((row.0, -row.2 - 1)))
                    .map(|(job_id, place, terminal_lease, live_lease)| {
                        format!("{{\"job_id\":{job_id},\"also_in\":\"{place}\",\"terminal_run_lease\":{terminal_lease},\"live_run_lease\":{live_lease}}}")
                    })
                    .collect();
                if !confirmed_rows.is_empty() {
                    self.shared.violation(
                        "terminal_and_live",
                        format!(
                            "{} job(s) are terminal and still available, deferred or leased",
                            confirmed_rows.len()
                        ),
                        confirmed_rows,
                    );
                }
            }
            Ok(_) => {}
            Err(error) => self
                .shared
                .incomplete(format!("terminal/live query failed: {error}"), vec![]),
        }
        self.shared.run.observe(
            "oracle_terminal_check_ms",
            started.elapsed().as_millis() as i64,
        );
    }

    /// A `cancelled` terminal row nobody asked for: no operator cancel, drain or purge
    /// touched the job and its handler did not cancel it.
    async fn check_unexplained_cancels(&mut self) {
        let schema = schema(&self.shared);
        let since_ms = self.last_cancel_scan_ms;
        let scan_started_ms = self.shared.now_ms();
        let rows: Result<Vec<(i64, String, i64)>, _> = sqlx::query_as(&format!(
            "SELECT job_id, queue, run_lease FROM {schema}.done_entries
             WHERE state = 'cancelled' AND finalized_at > now() - make_interval(secs => $1)
             LIMIT 2000"
        ))
        .bind(((scan_started_ms - since_ms) as f64 / 1000.0 + 15.0).max(20.0))
        .fetch_all(&self.pool)
        .await;
        self.last_cancel_scan_ms = scan_started_ms;
        let rows = match rows {
            Ok(rows) => rows,
            Err(error) => {
                self.shared
                    .incomplete(format!("invariant query failed: {error}"), vec![]);
                return;
            }
        };
        let mut unexplained = Vec::new();
        for (job_id, queue_name, run_lease) in rows {
            let Some(record) = self.shared.ledger.get(job_id) else {
                continue;
            };
            let queue = self.shared.queues.get(&queue_name);
            let drained = queue.as_ref().is_some_and(|queue| {
                let drained_at = queue
                    .drained_at_ms
                    .load(std::sync::atomic::Ordering::Relaxed);
                drained_at > 0 && i64::from(record.enqueued_ms) <= drained_at + 1000
            });
            let explained = record.has(flag::OPERATOR_REMOVED)
                || record.has(flag::HANDLER_CANCELLED)
                || record.has(flag::OPERATOR_MOVED)
                || drained;
            if !explained {
                // An operator flag is set just after its cancel commits; look again before accusing.
                let lag = if self
                    .shared
                    .ledger
                    .get(job_id)
                    .is_some_and(|record| record.persona == super::config::Persona::Python)
                {
                    4500
                } else {
                    1500
                };
                tokio::time::sleep(Duration::from_millis(lag)).await;
            }
            let explained = explained
                || self.shared.ledger.get(job_id).is_some_and(|record| {
                    record.has(flag::OPERATOR_REMOVED) || record.has(flag::HANDLER_CANCELLED)
                });
            if !explained && self.reported_cancel.insert(job_id) {
                unexplained.push(format!(
                    "{{\"job_id\":{job_id},\"queue\":\"{queue_name}\",\"run_lease\":{run_lease},\"executions\":{},\"running_now\":{},\"completed_events\":{}}}",
                    record.executions, record.in_flight, record.completed_events
                ));
            }
        }
        if !unexplained.is_empty() {
            self.shared.violation(
                "unexplained_cancel",
                format!(
                    "{} job(s) recorded as cancelled with no operator or handler cancel",
                    unexplained.len()
                ),
                unexplained.into_iter().take(MAX_ROWS).collect(),
            );
        }
    }

    async fn check_promotion(&self) {
        let schema = schema(&self.shared);
        let mut threshold = self.shared.config.promotion_overdue;
        if self.shared.chaos_active() {
            threshold += self.shared.config.chaos_slack;
        }
        let rows: Result<Vec<(i64, String, f64)>, _> = sqlx::query_as(&format!(
            "SELECT job_id, queue, extract(epoch FROM now() - run_at)::float8
             FROM {schema}.deferred_jobs
             WHERE state IN ('scheduled', 'retryable') AND run_at < now() - make_interval(secs => $1)
             ORDER BY run_at LIMIT {MAX_ROWS}"
        ))
        .bind(threshold.as_secs_f64())
        .fetch_all(&self.pool)
        .await;
        let rows = match rows {
            Ok(rows) => rows,
            Err(error) => {
                self.shared
                    .incomplete(format!("invariant query failed: {error}"), vec![]);
                return;
            }
        };
        if rows.is_empty() {
            return;
        }
        // Re-read so that a job promoted a moment ago is not reported.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let ids: Vec<i64> = rows.iter().map(|row| row.0).collect();
        let still: Result<Vec<i64>, _> = sqlx::query_scalar(&format!(
            "SELECT job_id FROM {schema}.deferred_jobs WHERE job_id = ANY($1) AND state IN ('scheduled','retryable')"
        ))
        .bind(&ids)
        .fetch_all(&self.pool)
        .await;
        let still = match still {
            Ok(still) => still,
            Err(error) => {
                self.shared
                    .incomplete(format!("promotion confirmation failed: {error}"), vec![]);
                return;
            }
        };
        let stuck: Vec<_> = rows.iter().filter(|row| still.contains(&row.0)).collect();
        if stuck.is_empty() {
            return;
        }
        let worst = stuck.iter().map(|row| row.2).fold(0.0, f64::max);
        self.shared
            .run
            .set_max("promotion_overdue_max_s", worst as i64);
        self.shared.violation(
            "promotion_stall",
            format!(
                "{} scheduled job(s) overdue beyond {}s, worst {worst:.1}s",
                stuck.len(),
                threshold.as_secs()
            ),
            stuck
                .iter()
                .map(|(job_id, queue, overdue)| {
                    format!(
                        "{{\"job_id\":{job_id},\"queue\":\"{queue}\",\"overdue_s\":{overdue:.1}}}"
                    )
                })
                .collect(),
        );
    }

    async fn check_stall(&mut self) {
        let schema = schema(&self.shared);
        let rows: Result<Vec<(String, i64, i64)>, _> = sqlx::query_as(&format!(
            "SELECT c.queue,
                    COALESCE(sum({schema}.sequence_next_value(e.seq_name) - {schema}.sequence_next_value(c.seq_name)), 0)::bigint,
                    COALESCE(sum({schema}.sequence_next_value(c.seq_name)), 0)::bigint
             FROM {schema}.queue_claim_heads c
             JOIN {schema}.queue_enqueue_heads e
               ON e.queue = c.queue AND e.priority = c.priority AND e.enqueue_shard = c.enqueue_shard
             GROUP BY c.queue"
        ))
        .fetch_all(&self.pool)
        .await;
        let rows = match rows {
            Ok(rows) => rows,
            Err(error) => {
                self.shared
                    .incomplete(format!("invariant query failed: {error}"), vec![]);
                return;
            }
        };
        let now = self.shared.now_ms();
        let mut window = self.shared.config.stall_window;
        if self.shared.chaos_active() {
            window += self.shared.config.chaos_slack;
        }
        let mut candidates = Vec::new();
        for (queue_name, backlog, cursor) in rows {
            let entry = self
                .stall
                .cursors
                .entry(queue_name.clone())
                .or_insert((cursor, now, false));
            if entry.0 != cursor {
                *entry = (cursor, now, false);
                continue;
            }
            let Some(queue) = self.shared.queues.get(&queue_name) else {
                continue;
            };
            let capacity = queue.capacity.load(std::sync::atomic::Ordering::Relaxed);
            let in_flight = queue.in_flight.load(std::sync::atomic::Ordering::Relaxed);
            let held = queue.is_held() || queue.rate_limited;
            let idle_workers = capacity > 0 && in_flight < capacity;
            if !entry.2
                && backlog > 0
                && idle_workers
                && !held
                && (now - entry.1) as u64 >= window.as_millis() as u64
            {
                candidates.push((queue_name, backlog, cursor, entry.1, capacity, in_flight));
            } else if !idle_workers || held || backlog <= 0 {
                // Busy or deliberately held: the stall clock restarts.
                entry.1 = now;
            }
        }
        for (queue_name, backlog, cursor, since, capacity, in_flight) in candidates {
            let exact: Result<i64, _> = sqlx::query_scalar(&format!(
                "WITH {} SELECT count(*)::bigint FROM available WHERE queue = $1",
                available_cte(&schema)
            ))
            .bind(&queue_name)
            .fetch_one(&self.pool)
            .await;
            let available = match exact {
                Ok(available) => available,
                Err(error) => {
                    self.shared
                        .incomplete(format!("exact claimable-row check failed: {error}"), vec![]);
                    continue;
                }
            };
            if available > 0 {
                if let Some(entry) = self.stall.cursors.get_mut(&queue_name) {
                    entry.2 = true;
                }
                self.shared.run.add("stalls", 1);
                self.shared.violation(
                    "stall",
                    format!(
                        "queue {queue_name}: {available} available job(s), claim cursor frozen at {cursor} for {}s with {}/{} workers busy",
                        (now - since) / 1000,
                        in_flight,
                        capacity
                    ),
                    vec![format!(
                        "{{\"queue\":\"{queue_name}\",\"available\":{available},\"lane_backlog\":{backlog},\"claim_cursor\":{cursor},\"workers_busy\":{in_flight},\"worker_capacity\":{capacity}}}"
                    )],
                );
            } else if let Some(entry) = self.stall.cursors.get_mut(&queue_name) {
                entry.1 = now;
            }
        }
    }

    /// Stops the run before it can fill the disk it shares with other work.
    fn check_disk(&self) {
        let output = std::process::Command::new("df")
            .args(["--output=avail", "-BM"])
            .arg(&self.shared.config.report_dir)
            .output();
        let Ok(output) = output else { return };
        let text = String::from_utf8_lossy(&output.stdout);
        let Some(free_mb) = text
            .lines()
            .nth(1)
            .and_then(|line| line.trim().trim_end_matches('M').parse::<i64>().ok())
        else {
            return;
        };
        self.shared.run.observe("free_disk_gb", free_mb / 1024);
        if free_mb < self.shared.config.min_free_disk_gb * 1024
            && !self.shared.load_stop.is_stopped()
        {
            self.shared.violation(
                "disk_low",
                format!(
                    "only {} GB free on the report directory's filesystem; stopping the load phase",
                    free_mb / 1024
                ),
                vec![],
            );
            self.shared.load_stop.trigger();
        }
    }

    async fn sample_resources(&self) {
        self.check_disk();
        let connections: Result<i64, _> = sqlx::query_scalar(
            "SELECT count(*)::bigint FROM pg_stat_activity WHERE backend_type = 'client backend'",
        )
        .fetch_one(&self.pool)
        .await;
        if let Ok(connections) = connections {
            self.shared.run.set_max("peak_connections", connections);
            self.shared.run.observe("connections", connections);
        }
        let size: Result<i64, _> =
            sqlx::query_scalar("SELECT pg_database_size(current_database())")
                .fetch_one(&self.pool)
                .await;
        if let Ok(size) = size {
            self.shared.run.set_max("peak_db_mb", size / (1024 * 1024));
            self.shared.run.observe("db_mb", size / (1024 * 1024));
        }
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            if let Some(kb) = status
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .and_then(|rest| {
                    rest.trim()
                        .trim_end_matches("kB")
                        .trim()
                        .parse::<i64>()
                        .ok()
                })
            {
                self.shared.run.set_max("peak_harness_rss_mb", kb / 1024);
            }
        }
        if let Ok(load) = std::fs::read_to_string("/proc/loadavg") {
            if let Some(one_minute) = load
                .split_whitespace()
                .next()
                .and_then(|v| v.parse::<f64>().ok())
            {
                self.shared
                    .run
                    .observe("loadavg_x100", (one_minute * 100.0) as i64);
            }
        }
    }

    /// Waits for the system to quiesce after load stops. Returns true when no live
    /// work remains.
    pub async fn wait_quiescent(&mut self, timeout: Duration) -> bool {
        let schema = schema(&self.shared);
        let deadline = std::time::Instant::now() + timeout;
        let mut quiet_polls = 0;
        loop {
            let live: Result<i64, _> = sqlx::query_scalar(&format!(
                "WITH {} SELECT (SELECT count(*) FROM available) + (SELECT count(*) FROM {schema}.leases)
                       + (SELECT count(*) FROM {schema}.deferred_jobs WHERE run_at < now() + interval '1 second')",
                available_cte(&schema)
            ))
            .fetch_one(&self.pool)
            .await;
            if let Err(error) = &live {
                self.shared
                    .incomplete(format!("quiescence query failed: {error}"), vec![]);
            }
            let mut pending_terminal_proof = 0usize;
            self.shared.ledger.for_each(|id, record| {
                if record.has(flag::RETURNED_COMPLETED)
                    && !record.has_durable_terminal()
                    && !record.has(flag::OPERATOR_REMOVED)
                    && !record.has(flag::DLQ_PURGED)
                    && (!record.has(flag::NO_EVENTS) || samples_terminal_proof(id))
                {
                    pending_terminal_proof += 1;
                }
            });
            let in_flight: i64 = self
                .shared
                .queues
                .all()
                .iter()
                .map(|queue| queue.in_flight.load(std::sync::atomic::Ordering::Relaxed))
                .sum();
            let python_active = self
                .shared
                .python_active
                .load(std::sync::atomic::Ordering::Relaxed);
            // Claims abandoned by a killed process sit in no table the live count reads.
            let abandoned_open = self
                .shared
                .abandon
                .open_claims
                .load(std::sync::atomic::Ordering::Relaxed);
            match live {
                Ok(count)
                    if in_flight == 0
                        && python_active == 0
                        && abandoned_open == 0
                        && pending_terminal_proof == 0
                        && count_is_idle(count) =>
                {
                    quiet_polls += 1
                }
                _ => quiet_polls = 0,
            }
            if quiet_polls >= 3 {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                self.shared.note(
                    "oracle",
                    format!(
                        "not quiescent after {}s: handlers running={in_flight}, live rows={live:?}",
                        timeout.as_secs()
                    ),
                );
                return false;
            }
            self.probe_server().await;
            self.check_stall().await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    /// Final reconciliation of the ledger against the database.
    pub async fn final_check(&mut self, quiesced: bool) {
        let schema = schema(&self.shared);
        self.check_terminal_uniqueness().await;
        self.check_unexplained_cancels().await;
        self.check_promotion().await;
        self.probe_server().await;

        self.shared.run.add(
            "cron_fires_executed",
            self.shared.ledger.cron_fire_count() as i64,
        );
        for ((name, fire), ids) in self.shared.ledger.cron_duplicates() {
            self.shared.violation(
                "cron_double_fire",
                format!("cron {name} fire {fire} produced jobs {ids:?}"),
                vec![],
            );
        }

        // Jobs with no terminal evidence on the client side have to be found in the database.
        let mut unresolved: Vec<i64> = Vec::new();
        let mut unexpected: Vec<String> = Vec::new();
        let mut sampled = 0i64;
        let mut confirmed = 0i64;
        let mut excluded = 0i64;
        self.shared.ledger.for_each(|id, record| {
            if record.has(flag::OPERATOR_REMOVED) || record.has(flag::DLQ_PURGED) {
                return;
            }
            let sampled_no_events = record.has(flag::NO_EVENTS) && samples_terminal_proof(id);
            sampled += i64::from(sampled_no_events);
            if record.has_durable_terminal() {
                confirmed += i64::from(sampled_no_events);
                let completed = record.completed_events > 0
                    || record.has(flag::RETURNED_COMPLETED)
                    || record.has(flag::DB_SEEN_COMPLETED);
                if record.has(flag::DB_SEEN_CANCELLED)
                    && record.expect & expect::CANCELLED == 0
                    && !record.has(flag::HANDLER_CANCELLED)
                    && unexpected.len() < MAX_ROWS
                {
                    unexpected.push(format!(
                        "{{\"job_id\":{id},\"outcome\":\"cancelled\",\"allowed\":{}}}",
                        record.expect
                    ));
                }
                if completed
                    && record.expect & expect::COMPLETED == 0
                    && unexpected.len() < MAX_ROWS
                {
                    unexpected.push(format!(
                        "{{\"job_id\":{id},\"outcome\":\"completed\",\"allowed\":{}}}",
                        record.expect
                    ));
                }
                if (record.exhausted_events > 0 || record.has(flag::DB_SEEN_FAILED))
                    && record.expect & expect::FAILED == 0
                    && unexpected.len() < MAX_ROWS
                {
                    unexpected.push(format!(
                        "{{\"job_id\":{id},\"outcome\":\"failed\",\"allowed\":{}}}",
                        record.expect
                    ));
                }
            } else if record.has(flag::NO_EVENTS)
                && record.has(flag::RETURNED_COMPLETED)
                && !sampled_no_events
            {
                // This optimized workload is sampled, not proven per job.
                // A return is execution evidence only, never commit evidence.
                excluded += 1;
            } else {
                unresolved.push(id);
            }
        });
        if !unexpected.is_empty() {
            self.shared.violation(
                "unexpected_outcome",
                format!(
                    "{} job(s) ended in an outcome their persona did not allow",
                    unexpected.len()
                ),
                unexpected,
            );
        }

        let mut live_stuck = Vec::new();
        let mut forensic_ids: Vec<i64> = Vec::new();
        let mut lost = Vec::new();
        let mut lost_after_run = Vec::new();
        let mut terminal_in_db = 0usize;
        for chunk in unresolved.chunks(5000) {
            let found: Result<Vec<(i64, String, Option<i64>)>, _> = sqlx::query_as(&format!(
                "SELECT job_id, string_agg(place, ','), max(terminal_run_lease) FROM (
                    SELECT job_id, 'done:' || state::text AS place, run_lease AS terminal_run_lease FROM {schema}.done_entries WHERE job_id = ANY($1)
                    UNION ALL SELECT job_id, 'dlq', run_lease FROM {schema}.dlq_entries WHERE job_id = ANY($1)
                    UNION ALL SELECT b.job_id, 'receipt_completed', b.run_lease
                      FROM {schema}.receipt_completion_batches c, LATERAL unnest(c.job_ids, c.run_leases) AS b(job_id, run_lease)
                      WHERE c.job_ids && $1 AND b.job_id = ANY($1)
                    UNION ALL SELECT job_id, 'deferred:' || state::text, NULL::bigint FROM {schema}.deferred_jobs WHERE job_id = ANY($1)
                    UNION ALL SELECT job_id, 'ready', NULL::bigint FROM {schema}.ready_entries WHERE job_id = ANY($1)
                    UNION ALL SELECT job_id, 'leased:' || state::text, NULL::bigint FROM {schema}.leases WHERE job_id = ANY($1)
                 ) places GROUP BY job_id"
            ))
            .bind(chunk)
            .fetch_all(&self.pool)
            .await;
            let found: HashMap<i64, (String, Option<i64>)> = match found {
                Ok(rows) => rows
                    .into_iter()
                    .map(|(id, place, lease)| (id, (place, lease)))
                    .collect(),
                Err(error) => {
                    self.shared.run.add("final_lookup_errors", 1);
                    self.shared.incomplete(
                        format!(
                            "final lookup failed for {} unresolved jobs: {error}",
                            chunk.len()
                        ),
                        vec![],
                    );
                    continue;
                }
            };
            for id in chunk {
                let record = self.shared.ledger.get(*id);
                let describe = |place: &str| {
                    let record = record.clone();
                    format!(
                        "{{\"job_id\":{id},\"found\":\"{place}\",\"executions\":{},\"persona\":\"{}\",\"flags\":{},\"enqueued_at_s\":{:.1},\"queue\":\"{}\"}}",
                        record.as_ref().map_or(0, |r| r.executions),
                        record.as_ref().map_or("?", |r| r.persona.name()),
                        record.as_ref().map_or(0, |r| r.flags),
                        record.as_ref().map_or(0.0, |r| f64::from(r.enqueued_ms) / 1000.0),
                        record
                            .as_ref()
                            .and_then(|r| self.shared.queues.by_index(r.queue))
                            .map_or("?".to_string(), |queue| queue.name.clone())
                    )
                };
                if record
                    .as_ref()
                    .is_some_and(|record| record.has_durable_terminal())
                {
                    confirmed += i64::from(
                        record
                            .as_ref()
                            .is_some_and(|record| record.has(flag::NO_EVENTS))
                            && samples_terminal_proof(*id),
                    );
                    continue;
                }
                match found.get(id) {
                    Some((place, terminal_lease))
                        if terminal_lease.is_some_and(|lease| {
                            lease >= record.as_ref().map_or(0, |record| record.latest_run_lease)
                        }) =>
                    {
                        terminal_in_db += 1;
                        confirmed += i64::from(
                            record
                                .as_ref()
                                .is_some_and(|record| record.has(flag::NO_EVENTS))
                                && samples_terminal_proof(*id),
                        );
                        if place.contains("done:cancelled") {
                            if let Some(record) = &record {
                                let queue = self.shared.queues.by_index(record.queue);
                                let drained = queue.is_some_and(|queue| {
                                    let at = queue
                                        .drained_at_ms
                                        .load(std::sync::atomic::Ordering::Relaxed);
                                    at > 0 && i64::from(record.enqueued_ms) <= at + 1000
                                });
                                if !(record.has(flag::OPERATOR_REMOVED) || drained)
                                    && record.expect & expect::CANCELLED == 0
                                {
                                    lost_after_run.push(describe(place));
                                    forensic_ids.push(*id);
                                }
                            }
                        }
                    }
                    Some((place, _)) => live_stuck.push(describe(place)),
                    None => {
                        let executed = record.as_ref().is_some_and(|r| r.executions > 0);
                        let returned = record
                            .as_ref()
                            .is_some_and(|r| r.has(flag::RETURNED_COMPLETED));
                        let drained = record.as_ref().is_some_and(|r| {
                            self.shared.queues.by_index(r.queue).is_some_and(|queue| {
                                let at = queue
                                    .drained_at_ms
                                    .load(std::sync::atomic::Ordering::Relaxed);
                                at > 0 && i64::from(r.enqueued_ms) <= at + 1000
                            })
                        });
                        if drained {
                            continue;
                        }
                        if !executed {
                            lost.push(describe("absent"));
                            forensic_ids.push(*id);
                        } else if !returned {
                            lost_after_run.push(describe("absent"));
                            forensic_ids.push(*id);
                        } else {
                            self.shared.incomplete(
                                "a returned handler has no durable terminal proof; a missed pruned outcome cannot be distinguished from a lost finalize",
                                vec![describe("absent without durable terminal evidence")],
                            );
                        }
                    }
                }
            }
        }
        if !forensic_ids.is_empty() {
            self.record_forensics(&schema, &forensic_ids).await;
        }
        self.shared
            .run
            .add("terminal_proof_no_events_sampled", sampled);
        self.shared
            .run
            .add("terminal_proof_no_events_confirmed", confirmed);
        self.shared
            .run
            .add("terminal_proof_no_events_excluded", excluded);
        self.shared
            .run
            .add("final_unresolved_ids", unresolved.len() as i64);
        self.shared
            .run
            .add("final_terminal_in_db", terminal_in_db as i64);
        let report = |kind: &str, what: &str, rows: Vec<String>| {
            if !rows.is_empty() {
                let count = rows.len();
                self.shared.violation(
                    kind,
                    format!("{count} job(s) {what}"),
                    rows.into_iter().take(MAX_ROWS).collect(),
                );
            }
        };
        if quiesced {
            report(
                "stuck_job",
                "never reached a terminal state although the system went quiet (available/deferred/leased)",
                live_stuck,
            );
        } else if !live_stuck.is_empty() {
            // Work still queued when the grace ended is backlog, not loss: the run was
            // sized beyond what the server could clear.
            self.shared.finding(
                "backlog_at_end",
                format!(
                    "{} job(s) were still queued when the drain grace ended",
                    live_stuck.len()
                ),
                live_stuck.into_iter().take(MAX_ROWS).collect(),
            );
        }
        report(
            "lost_job",
            "were enqueued, never ran and are in no table",
            lost,
        );
        report("lost_job_after_run", "ran without a recorded terminal outcome and are in no table, or ended cancelled without cause", lost_after_run);
    }

    /// For jobs that look lost: everywhere the store could still mention them, written to
    /// the timeline so the reproducer carries it.
    async fn record_forensics(&self, schema: &str, ids: &[i64]) {
        let sample: Vec<i64> = ids.iter().copied().take(200).collect();
        let tables: [(&str, String); 8] = [
            ("ready_entries", format!("SELECT count(*), min(job_id), max(job_id), min(lane_seq), max(lane_seq) FROM {schema}.ready_entries WHERE job_id = ANY($1)")),
            ("ready_tombstones", format!("SELECT count(*), min(job_id), max(job_id), min(lane_seq), max(lane_seq) FROM {schema}.ready_tombstones WHERE job_id = ANY($1)")),
            ("lease_claims", format!("SELECT count(*), min(job_id), max(job_id), min(lane_seq), max(lane_seq) FROM {schema}.lease_claims WHERE job_id = ANY($1)")),
            ("lease_claim_batches", format!("SELECT count(*), min(j), max(j), 0::bigint, 0::bigint FROM {schema}.lease_claim_batches b, LATERAL unnest(b.job_ids) AS j WHERE b.job_ids && $1 AND j = ANY($1)")),
            ("lease_claim_closures", format!("SELECT count(*), min(job_id), max(job_id), 0::bigint, 0::bigint FROM {schema}.lease_claim_closures WHERE job_id = ANY($1)")),
            ("attempt_state", format!("SELECT count(*), min(job_id), max(job_id), 0::bigint, 0::bigint FROM {schema}.attempt_state WHERE job_id = ANY($1)")),
            ("receipt_completion_tombstones", format!("SELECT count(*), 0::bigint, 0::bigint, 0::bigint, 0::bigint FROM {schema}.receipt_completion_tombstones")),
            ("job_unique_claims", "SELECT count(*), min(job_id), max(job_id), 0::bigint, 0::bigint FROM awa.job_unique_claims WHERE job_id = ANY($1)".to_string()),
        ];
        let mut lines = Vec::new();
        for (name, sql) in tables {
            let row: Result<ForensicRow, _> = sqlx::query_as(&sql)
                .bind(&sample)
                .fetch_one(&self.pool)
                .await;
            match row {
                Ok((count, min_id, max_id, min_seq, max_seq)) => lines.push(format!("{name}: {count} row(s) for the sampled ids (job ids {min_id:?}..{max_id:?}, lane_seq {min_seq:?}..{max_seq:?})")),
                Err(error) => lines.push(format!("{name}: query failed: {error}")),
            }
        }
        self.shared.note(
            "oracle",
            format!(
                "forensics for {} lost-looking job(s), first {:?}: {}",
                ids.len(),
                &ids[..ids.len().min(5)],
                lines.join("; ")
            ),
        );
    }

    pub fn crash_scan(&self) -> Vec<String> {
        match self.watch.log_since_start(&self.shared.config) {
            Some(log) => super::pgserver::crash_excerpts(&log),
            None => Vec::new(),
        }
    }
}

fn count_is_idle(count: i64) -> bool {
    count <= 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persona_soak::config::{replace_database_name, Persona, SoakConfig};
    use crate::persona_soak::db::{self, Budget};
    use crate::persona_soak::ledger::JobRecord;
    use awa::model::{ClaimedRuntimeJob, QueueStorage, QueueStorageConfig};
    use std::sync::atomic::Ordering;

    struct TestRun {
        shared: Arc<Shared>,
        pool: PgPool,
        store: QueueStorage,
        claim: ClaimedRuntimeJob,
        oracle: Oracle,
    }

    impl TestRun {
        async fn create() -> Self {
            Self::create_with(true, true).await
        }

        async fn create_with(no_events: bool, sampled: bool) -> Self {
            let base = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgres://postgres:test@localhost:15432/awa_test".to_string()
            });
            let name = format!("awa_soak_oracle_{}", uuid::Uuid::new_v4().simple());
            let config = SoakConfig::for_test(replace_database_name(&base, &name));
            db::recreate_database(&config)
                .await
                .expect("create isolated oracle database");
            let pool = db::connect_pool(&config.database_url, "soak_oracle_regression", 4)
                .await
                .expect("connect oracle");
            let info = db::server_info(&pool).await.expect("server info");
            let watch = Arc::new(PgWatch::begin(&config, &pool).await.expect("watch server"));
            let budget = Budget::new(&info, 0);
            let shared = Shared::new(config, info, budget);
            let queue = shared
                .queues
                .register("oracle_proof", Persona::Priya, false);
            let store = QueueStorage::new(QueueStorageConfig::default()).expect("store");
            store
                .enqueue_batch(&pool, "oracle_proof", 2, 64)
                .await
                .expect("enqueue sample candidates");
            let claimed = store
                .claim_runtime_batch(&pool, "oracle_proof", 64, Duration::ZERO)
                .await
                .expect("claim sample");
            let claim = claimed
                .into_iter()
                .find(|claim| samples_terminal_proof(claim.job.id) == sampled)
                .expect("immutable job ID selects the requested sample status");
            shared.ledger.register(
                claim.job.id,
                JobRecord::new(
                    Persona::Priya,
                    queue.index,
                    0,
                    expect::COMPLETED,
                    if no_events { flag::NO_EVENTS } else { 0 },
                ),
            );
            // The handler returned, but its caller has not finalized the claim.
            let execution = shared.begin_execution(Persona::Priya, &claim.job, no_events);
            shared.ledger.mark(claim.job.id, flag::RETURNED_COMPLETED);
            drop(execution);
            let oracle = Oracle::new(shared.clone(), pool.clone(), watch);
            Self {
                shared,
                pool,
                store,
                claim,
                oracle,
            }
        }

        async fn drop(self) {
            self.pool.close().await;
            let admin = db::connect_pool(&self.shared.config.admin_url(), "oracle_cleanup", 1)
                .await
                .expect("cleanup connection");
            sqlx::query(&format!(
                "DROP DATABASE {} WITH (FORCE)",
                self.shared.config.database_name()
            ))
            .execute(&admin)
            .await
            .expect("drop oracle database");
            admin.close().await;
        }
    }

    #[tokio::test]
    async fn returned_handler_without_finalization_cannot_pass_the_oracle() {
        let mut run = TestRun::create().await;
        run.oracle.final_check(true).await;
        assert!(
            run.shared
                .violations
                .lock()
                .expect("violations")
                .iter()
                .any(|violation| violation.kind == "stuck_job"),
            "a handler return must not stand in for a terminal commit"
        );
        run.drop().await;
    }

    #[tokio::test]
    async fn sampled_terminal_pruned_before_observation_is_inconclusive() {
        let mut run = TestRun::create().await;
        run.store
            .complete_runtime_batch(&run.pool, &[run.claim.clone()])
            .await
            .expect("commit completion");
        run.store
            .reset(&run.pool)
            .await
            .expect("remove retained evidence");
        run.oracle.final_check(true).await;
        assert!(
            run.shared.oracle_incomplete.load(Ordering::Relaxed),
            "an unobserved pruned result cannot establish a pass or a lost-job claim"
        );
        run.drop().await;
    }

    #[tokio::test]
    async fn observed_terminal_survives_pruning_in_the_client_ledger() {
        let mut run = TestRun::create().await;
        run.store
            .complete_runtime_batch(&run.pool, &[run.claim.clone()])
            .await
            .expect("commit completion");
        super::super::terminal_proof::observe(&run.shared, &run.pool).await;
        assert!(run
            .shared
            .ledger
            .get(run.claim.job.id)
            .expect("sample record")
            .has_durable_terminal());
        run.store
            .reset(&run.pool)
            .await
            .expect("prune retained evidence");
        run.oracle.final_check(true).await;
        assert!(
            run.shared.violations.lock().expect("violations").is_empty(),
            "captured durable proof must survive pruning"
        );
        run.drop().await;
    }

    #[tokio::test]
    async fn eventful_observed_terminal_without_callback_survives_pruning() {
        let mut run = TestRun::create_with(false, false).await;
        assert!(!samples_terminal_proof(run.claim.job.id));
        let completed = run
            .store
            .complete_runtime_batch(&run.pool, &[run.claim.clone()])
            .await
            .expect("commit completion without a lifecycle callback");
        assert_eq!(completed, vec![(run.claim.job.id, run.claim.job.run_lease)]);
        let durable: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM awa.receipt_completion_batches AS batch,
                 LATERAL unnest(batch.job_ids, batch.run_leases) AS item(job_id, run_lease)
                 WHERE item.job_id = $1 AND item.run_lease = $2
             )",
        )
        .bind(run.claim.job.id)
        .bind(run.claim.job.run_lease)
        .fetch_one(&run.pool)
        .await
        .expect("read committed completion independently of the observer");
        assert!(durable);
        super::super::terminal_proof::observe(&run.shared, &run.pool).await;
        let record = run
            .shared
            .ledger
            .get(run.claim.job.id)
            .expect("eventful record");
        assert!(record.has_durable_terminal());
        assert!(record.has(flag::DB_SEEN_COMPLETED));
        assert_eq!(record.completed_events, 0);
        run.store
            .reset(&run.pool)
            .await
            .expect("prune retained evidence");
        run.oracle.final_check(true).await;
        assert!(run.shared.violations.lock().expect("violations").is_empty());
        run.drop().await;
    }

    #[tokio::test]
    async fn eventful_return_without_finalization_cannot_gain_terminal_proof() {
        let mut run = TestRun::create_with(false, false).await;
        super::super::terminal_proof::observe(&run.shared, &run.pool).await;
        assert!(!run
            .shared
            .ledger
            .get(run.claim.job.id)
            .expect("record")
            .has_durable_terminal());
        run.oracle.final_check(true).await;
        assert!(run
            .shared
            .violations
            .lock()
            .expect("violations")
            .iter()
            .any(|violation| violation.kind == "stuck_job"));
        run.drop().await;
    }

    #[tokio::test]
    async fn eventful_terminal_pruned_before_observation_stays_inconclusive() {
        let mut run = TestRun::create_with(false, false).await;
        run.store
            .complete_runtime_batch(&run.pool, &[run.claim.clone()])
            .await
            .expect("commit completion");
        run.store
            .reset(&run.pool)
            .await
            .expect("remove evidence before observation");
        super::super::terminal_proof::observe(&run.shared, &run.pool).await;
        run.oracle.final_check(true).await;
        assert!(run.shared.oracle_incomplete.load(Ordering::Relaxed));
        run.drop().await;
    }

    #[tokio::test]
    async fn eventful_terminal_observer_query_failure_stays_inconclusive() {
        let run = TestRun::create_with(false, false).await;
        run.store
            .complete_runtime_batch(&run.pool, std::slice::from_ref(&run.claim))
            .await
            .expect("commit completion");
        sqlx::query("ALTER TABLE awa.receipt_completion_batches RENAME TO receipt_completion_batches_unavailable")
            .execute(&run.pool).await.expect("inject observer query failure");
        super::super::terminal_proof::observe(&run.shared, &run.pool).await;
        assert!(run.shared.oracle_incomplete.load(Ordering::Relaxed));
        assert!(!run
            .shared
            .ledger
            .get(run.claim.job.id)
            .expect("record")
            .has_durable_terminal());
        run.drop().await;
    }

    #[tokio::test]
    async fn eventful_terminal_observer_requires_the_exact_attempt() {
        let run = TestRun::create_with(false, false).await;
        run.store
            .complete_runtime_batch(&run.pool, std::slice::from_ref(&run.claim))
            .await
            .expect("complete old attempt");
        run.shared.ledger.with(
            run.claim.job.id,
            || unreachable!("registered"),
            |record| {
                record.latest_run_lease += 1;
            },
        );
        super::super::terminal_proof::observe(&run.shared, &run.pool).await;
        let record = run.shared.ledger.get(run.claim.job.id).expect("record");
        assert!(!record.has_durable_terminal());
        assert!(!record.has(flag::DB_SEEN_COMPLETED));
        run.drop().await;
    }

    #[tokio::test]
    async fn unsampled_no_events_job_does_not_gain_periodic_terminal_proof() {
        let run = TestRun::create_with(true, false).await;
        run.store
            .complete_runtime_batch(&run.pool, std::slice::from_ref(&run.claim))
            .await
            .expect("commit completion");
        super::super::terminal_proof::observe(&run.shared, &run.pool).await;
        assert!(!run
            .shared
            .ledger
            .get(run.claim.job.id)
            .expect("record")
            .has_durable_terminal());
        run.drop().await;
    }

    #[tokio::test]
    async fn failed_final_lookup_cannot_omit_unchecked_jobs_and_pass() {
        let mut run = TestRun::create().await;
        sqlx::query("ALTER TABLE awa.ready_entries RENAME TO ready_entries_unavailable")
            .execute(&run.pool)
            .await
            .expect("inject a lookup failure");
        run.oracle.final_check(true).await;
        assert!(run.shared.oracle_incomplete.load(Ordering::Relaxed));
        assert_eq!(
            run.shared.run.get("final_lookup_errors"),
            1,
            "the failed lookup chunk must be accounted for"
        );
        run.drop().await;
    }
}
