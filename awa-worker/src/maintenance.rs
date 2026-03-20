use crate::runtime::InFlightMap;
use awa_model::cron::{atomic_enqueue, list_cron_jobs, upsert_cron_job, CronJobRow};
use awa_model::{JobRow, PeriodicJob};
use chrono::Utc;
use croner::Cron;
use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Per-queue or global retention policy for completed and failed/cancelled jobs.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// How long to keep completed jobs before cleanup.
    pub completed: Duration,
    /// How long to keep failed/cancelled jobs before cleanup.
    pub failed: Duration,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            completed: Duration::from_secs(86400), // 24h
            failed: Duration::from_secs(259200),   // 72h
        }
    }
}

/// Maintenance service: runs leader-elected background tasks.
///
/// Tasks: heartbeat rescue, deadline rescue, scheduled promotion, cleanup,
/// periodic job sync and evaluation.
pub struct MaintenanceService {
    pool: PgPool,
    metrics: crate::metrics::AwaMetrics,
    cancel: CancellationToken,
    leader: Arc<AtomicBool>,
    periodic_jobs: Arc<Vec<PeriodicJob>>,
    /// In-flight job cancellation flags — used to signal deadline/heartbeat rescue
    /// to running handlers on this worker instance.
    in_flight: InFlightMap,
    heartbeat_rescue_interval: Duration,
    deadline_rescue_interval: Duration,
    callback_rescue_interval: Duration,
    promote_interval: Duration,
    cleanup_interval: Duration,
    cron_sync_interval: Duration,
    cron_eval_interval: Duration,
    leader_check_interval: Duration,
    leader_election_interval: Duration,
    heartbeat_staleness: Duration,
    completed_retention: Duration,
    failed_retention: Duration,
    cleanup_batch_size: i64,
    queue_retention_overrides: HashMap<String, RetentionPolicy>,
}

const PROMOTE_BATCH_SIZE: i64 = 4_096;
const PROMOTE_MAX_BATCHES_PER_TICK: usize = 32;

impl MaintenanceService {
    pub(crate) fn new(
        pool: PgPool,
        metrics: crate::metrics::AwaMetrics,
        leader: Arc<AtomicBool>,
        cancel: CancellationToken,
        periodic_jobs: Arc<Vec<PeriodicJob>>,
        in_flight: InFlightMap,
    ) -> Self {
        Self {
            pool,
            metrics,
            cancel,
            leader,
            periodic_jobs,
            in_flight,
            heartbeat_rescue_interval: Duration::from_secs(30),
            deadline_rescue_interval: Duration::from_secs(30),
            callback_rescue_interval: Duration::from_secs(30),
            promote_interval: Duration::from_millis(250),
            cleanup_interval: Duration::from_secs(60),
            cron_sync_interval: Duration::from_secs(60),
            cron_eval_interval: Duration::from_secs(1),
            leader_check_interval: Duration::from_secs(30),
            leader_election_interval: Duration::from_secs(10),
            heartbeat_staleness: Duration::from_secs(90),
            completed_retention: Duration::from_secs(86400), // 24h
            failed_retention: Duration::from_secs(259200),   // 72h
            cleanup_batch_size: 1000,
            queue_retention_overrides: HashMap::new(),
        }
    }

    /// Set the leader election retry interval (default: 10s).
    ///
    /// Controls how often a non-leader instance retries acquiring the
    /// advisory lock. Lower values speed up leader election in tests.
    pub fn leader_election_interval(mut self, interval: Duration) -> Self {
        self.leader_election_interval = interval;
        self
    }

    /// Set the promotion interval for scheduled/retryable jobs.
    pub fn promote_interval(mut self, interval: Duration) -> Self {
        self.promote_interval = interval;
        self
    }

    /// Set the cleanup interval (default: 60s).
    pub fn cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    /// Set retention for completed jobs (default: 24h).
    pub fn completed_retention(mut self, retention: Duration) -> Self {
        self.completed_retention = retention;
        self
    }

    /// Set retention for failed/cancelled jobs (default: 72h).
    pub fn failed_retention(mut self, retention: Duration) -> Self {
        self.failed_retention = retention;
        self
    }

    /// Set the maximum number of jobs to delete per cleanup pass (default: 1000).
    pub fn cleanup_batch_size(mut self, batch_size: i64) -> Self {
        self.cleanup_batch_size = batch_size;
        self
    }

    /// Set per-queue retention overrides.
    pub fn queue_retention_overrides(
        mut self,
        overrides: HashMap<String, RetentionPolicy>,
    ) -> Self {
        self.queue_retention_overrides = overrides;
        self
    }

    /// Run the maintenance loop. Attempts leader election first.
    pub async fn run(&self) {
        info!("Maintenance service starting");
        self.leader.store(false, Ordering::SeqCst);

        loop {
            // Try to acquire advisory lock for leader election.
            // We get back a dedicated connection that holds the lock.
            let mut leader_conn = match self.try_become_leader().await {
                Ok(Some(conn)) => conn,
                Ok(None) => {
                    // Not leader — back off and try again
                    tokio::select! {
                        _ = self.cancel.cancelled() => {
                            debug!("Maintenance service shutting down (not leader)");
                            self.leader.store(false, Ordering::SeqCst);
                            return;
                        }
                        _ = tokio::time::sleep(self.leader_election_interval) => continue,
                    }
                }
                Err(err) => {
                    warn!(error = %err, "Failed to check leader status");
                    tokio::select! {
                        _ = self.cancel.cancelled() => {
                            debug!("Maintenance service shutting down (leader check failed)");
                            self.leader.store(false, Ordering::SeqCst);
                            return;
                        }
                        _ = tokio::time::sleep(self.leader_election_interval) => continue,
                    }
                }
            };

            debug!("Elected as maintenance leader");
            self.leader.store(true, Ordering::SeqCst);

            // Run maintenance tasks as leader
            let mut heartbeat_rescue_timer = tokio::time::interval(self.heartbeat_rescue_interval);
            let mut deadline_rescue_timer = tokio::time::interval(self.deadline_rescue_interval);
            let mut callback_rescue_timer = tokio::time::interval(self.callback_rescue_interval);
            let mut promote_timer = tokio::time::interval(self.promote_interval);
            let mut cleanup_timer = tokio::time::interval(self.cleanup_interval);
            let mut cron_sync_timer = tokio::time::interval(self.cron_sync_interval);
            let mut cron_eval_timer = tokio::time::interval(self.cron_eval_interval);
            let mut leader_check_timer = tokio::time::interval(self.leader_check_interval);

            // Skip the first immediate tick
            heartbeat_rescue_timer.tick().await;
            deadline_rescue_timer.tick().await;
            callback_rescue_timer.tick().await;
            promote_timer.tick().await;
            cleanup_timer.tick().await;
            cron_sync_timer.tick().await;
            cron_eval_timer.tick().await;
            leader_check_timer.tick().await;

            // Do an initial sync immediately on becoming leader
            self.sync_periodic_jobs_to_db().await;

            loop {
                tokio::select! {
                    _ = self.cancel.cancelled() => {
                        debug!("Maintenance service shutting down");
                        self.leader.store(false, Ordering::SeqCst);
                        // Release leader lock on the same connection that acquired it.
                        // If this fails, dropping the connection will release the lock anyway.
                        let _ = Self::release_leader(&mut leader_conn).await;
                        return;
                    }
                    _ = heartbeat_rescue_timer.tick() => {
                        self.rescue_stale_heartbeats().await;
                    }
                    _ = deadline_rescue_timer.tick() => {
                        self.rescue_expired_deadlines().await;
                    }
                    _ = callback_rescue_timer.tick() => {
                        self.rescue_expired_callbacks().await;
                    }
                    _ = promote_timer.tick() => {
                        self.promote_scheduled().await;
                    }
                    _ = cleanup_timer.tick() => {
                        self.cleanup_completed().await;
                    }
                    _ = cron_sync_timer.tick() => {
                        self.sync_periodic_jobs_to_db().await;
                    }
                    _ = cron_eval_timer.tick() => {
                        self.evaluate_cron_schedules().await;
                    }
                    _ = leader_check_timer.tick() => {
                        // Verify leader connection is still alive.
                        // The advisory lock is session-scoped: if the connection is alive,
                        // the lock is held. If the query fails, the connection (and lock) are gone.
                        if sqlx::query("SELECT 1").execute(&mut *leader_conn).await.is_err() {
                            warn!("Leader connection lost, re-entering election loop");
                            self.leader.store(false, Ordering::SeqCst);
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Advisory lock key for Awa maintenance leader election.
    const LOCK_KEY: i64 = 0x_4157_415f_4d41_494e; // "AWA_MAIN" in hex-ish

    /// Try to acquire the advisory lock for leader election.
    ///
    /// Returns a dedicated connection holding the lock on success, or `None` if
    /// another instance already holds the lock. The lock is session-scoped in
    /// PostgreSQL, so it stays held as long as this connection is alive.
    async fn try_become_leader(&self) -> Result<Option<PoolConnection<Postgres>>, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        let result: (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
            .bind(Self::LOCK_KEY)
            .fetch_one(&mut *conn)
            .await?;
        if result.0 {
            Ok(Some(conn))
        } else {
            Ok(None)
        }
    }

    /// Release the advisory lock on the same connection that acquired it.
    ///
    /// Dropping the connection also releases the lock (PG session-scoped behavior),
    /// so this is a best-effort explicit release.
    async fn release_leader(conn: &mut PoolConnection<Postgres>) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(Self::LOCK_KEY)
            .execute(&mut **conn)
            .await?;
        Ok(())
    }

    /// Sync all registered periodic job schedules to `awa.cron_jobs` via UPSERT.
    ///
    /// Additive only — does NOT delete schedules not in the local set (multi-deployment safe).
    #[tracing::instrument(skip(self), name = "maintenance.cron_sync")]
    async fn sync_periodic_jobs_to_db(&self) {
        if self.periodic_jobs.is_empty() {
            return;
        }

        for job in self.periodic_jobs.iter() {
            if let Err(err) = upsert_cron_job(&self.pool, job).await {
                error!(name = %job.name, error = %err, "Failed to sync periodic job");
            }
        }

        debug!(
            count = self.periodic_jobs.len(),
            "Synced periodic jobs to database"
        );
    }

    /// Evaluate all cron schedules and enqueue any that are due.
    ///
    /// For each schedule, computes the latest fire time ≤ now that is after
    /// `last_enqueued_at`. If a fire is due, executes the atomic CTE to
    /// mark + insert in one statement.
    #[tracing::instrument(skip(self), name = "maintenance.cron_eval")]
    async fn evaluate_cron_schedules(&self) {
        let cron_rows = match list_cron_jobs(&self.pool).await {
            Ok(rows) => rows,
            Err(err) => {
                error!(error = %err, "Failed to load cron jobs for evaluation");
                return;
            }
        };

        if cron_rows.is_empty() {
            return;
        }

        let now = Utc::now();

        for row in &cron_rows {
            let fire_time = match compute_fire_time(row, now) {
                Some(time) => time,
                None => continue,
            };

            match atomic_enqueue(&self.pool, &row.name, fire_time, row.last_enqueued_at).await {
                Ok(Some(job)) => {
                    info!(
                        cron_name = %row.name,
                        job_id = job.id,
                        fire_time = %fire_time,
                        "Enqueued periodic job"
                    );
                }
                Ok(None) => {
                    // Another leader already claimed this fire — not an error
                    debug!(cron_name = %row.name, "Cron fire already claimed");
                }
                Err(err) => {
                    error!(
                        cron_name = %row.name,
                        error = %err,
                        "Failed to enqueue periodic job"
                    );
                }
            }
        }
    }

    /// Rescue jobs with stale heartbeats (crash detection).
    #[tracing::instrument(skip(self), name = "maintenance.rescue_stale")]
    async fn rescue_stale_heartbeats(&self) {
        let staleness_str = format!("{} seconds", self.heartbeat_staleness.as_secs());
        match sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE awa.jobs
            SET state = 'retryable',
                finalized_at = now(),
                heartbeat_at = NULL,
                deadline_at = NULL,
                callback_id = NULL,
                callback_timeout_at = NULL,
                callback_filter = NULL,
                callback_on_complete = NULL,
                callback_on_fail = NULL,
                callback_transform = NULL,
                errors = errors || jsonb_build_object(
                    'error', 'heartbeat stale: worker presumed dead',
                    'attempt', attempt,
                    'at', now()
                )::jsonb
            WHERE id IN (
                SELECT id FROM awa.jobs
                WHERE state = 'running'
                  AND heartbeat_at < now() - $1::interval
                LIMIT 500
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            "#,
        )
        .bind(&staleness_str)
        .fetch_all(&self.pool)
        .await
        {
            Ok(rescued) if !rescued.is_empty() => {
                warn!(count = rescued.len(), "Rescued stale heartbeat jobs");
                // Signal cancellation to any rescued jobs still running on this instance
                self.signal_cancellation(&rescued).await;
            }
            Err(err) => {
                error!(error = %err, "Failed to rescue stale heartbeat jobs");
            }
            _ => {}
        }
    }

    /// Rescue jobs that exceeded their hard deadline.
    #[tracing::instrument(skip(self), name = "maintenance.rescue_deadline")]
    async fn rescue_expired_deadlines(&self) {
        match sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE awa.jobs
            SET state = 'retryable',
                finalized_at = now(),
                heartbeat_at = NULL,
                deadline_at = NULL,
                callback_id = NULL,
                callback_timeout_at = NULL,
                callback_filter = NULL,
                callback_on_complete = NULL,
                callback_on_fail = NULL,
                callback_transform = NULL,
                errors = errors || jsonb_build_object(
                    'error', 'hard deadline exceeded',
                    'attempt', attempt,
                    'at', now()
                )::jsonb
            WHERE id IN (
                SELECT id FROM awa.jobs
                WHERE state = 'running'
                  AND deadline_at IS NOT NULL
                  AND deadline_at < now()
                LIMIT 500
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            "#,
        )
        .fetch_all(&self.pool)
        .await
        {
            Ok(rescued) if !rescued.is_empty() => {
                warn!(count = rescued.len(), "Rescued deadline-expired jobs");
                // Signal cancellation so handlers see ctx.is_cancelled() == true
                self.signal_cancellation(&rescued).await;
            }
            Err(err) => {
                error!(error = %err, "Failed to rescue deadline-expired jobs");
            }
            _ => {}
        }
    }

    /// Rescue jobs whose callback timeout has expired.
    #[tracing::instrument(skip(self), name = "maintenance.rescue_callback_timeout")]
    async fn rescue_expired_callbacks(&self) {
        match sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE awa.jobs
            SET state = CASE WHEN attempt >= max_attempts THEN 'failed'::awa.job_state ELSE 'retryable'::awa.job_state END,
                finalized_at = now(),
                callback_id = NULL,
                callback_timeout_at = NULL,
                callback_filter = NULL,
                callback_on_complete = NULL,
                callback_on_fail = NULL,
                callback_transform = NULL,
                run_at = CASE WHEN attempt >= max_attempts THEN run_at
                         ELSE now() + awa.backoff_duration(attempt, max_attempts) END,
                errors = errors || jsonb_build_object(
                    'error', 'callback timed out',
                    'attempt', attempt,
                    'at', now()
                )::jsonb
            WHERE id IN (
                SELECT id FROM awa.jobs
                WHERE state = 'waiting_external'
                  AND callback_timeout_at IS NOT NULL
                  AND callback_timeout_at < now()
                LIMIT 500
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            "#,
        )
        .fetch_all(&self.pool)
        .await
        {
            Ok(rescued) if !rescued.is_empty() => {
                warn!(count = rescued.len(), "Rescued callback-timed-out jobs");
            }
            Err(err) => {
                error!(error = %err, "Failed to rescue callback-timed-out jobs");
            }
            _ => {}
        }
    }

    /// Signal cancellation to any rescued jobs that are still running on this instance.
    async fn signal_cancellation(&self, rescued_jobs: &[JobRow]) {
        for job in rescued_jobs {
            if let Some(flag) = self.in_flight.get_cancel((job.id, job.run_lease)) {
                flag.store(true, Ordering::SeqCst);
                debug!(job_id = job.id, "Signalled cancellation for rescued job");
            }
        }
    }

    /// Promote scheduled jobs that are now due.
    #[tracing::instrument(skip(self), name = "maintenance.promote")]
    async fn promote_scheduled(&self) {
        if let Err(err) = self.promote_due_state("scheduled", "scheduled jobs").await {
            error!(error = %err, "Failed to promote scheduled jobs");
        }
        if let Err(err) = self
            .promote_due_state("retryable", "retryable jobs (backoff elapsed)")
            .await
        {
            error!(error = %err, "Failed to promote retryable jobs");
        }
    }

    async fn promote_due_state(
        &self,
        state: &'static str,
        label: &'static str,
    ) -> Result<(), sqlx::Error> {
        let mut promoted_total = 0usize;
        let mut notified_queues = HashSet::new();

        for _ in 0..PROMOTE_MAX_BATCHES_PER_TICK {
            if self.cancel.is_cancelled() {
                break;
            }

            let (promoted, queues) = self.promote_due_batch(state).await?;
            if promoted == 0 {
                break;
            }

            promoted_total += promoted;
            notified_queues.extend(queues);

            if promoted < PROMOTE_BATCH_SIZE as usize {
                break;
            }
        }

        if promoted_total > 0 {
            debug!(
                count = promoted_total,
                queues = notified_queues.len(),
                state,
                "Promoted {label}"
            );
        }

        Ok(())
    }

    async fn promote_due_batch(
        &self,
        state: &'static str,
    ) -> Result<(usize, HashSet<String>), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let promote_start = std::time::Instant::now();
        let promoted_rows: Vec<(String,)> = sqlx::query_as(
            r#"
            WITH due AS (
                DELETE FROM awa.scheduled_jobs
                WHERE id IN (
                    SELECT id
                    FROM awa.scheduled_jobs
                    WHERE state = $1::awa.job_state
                      AND run_at <= now()
                    ORDER BY run_at ASC, id ASC
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING *
            ),
            promoted AS (
                INSERT INTO awa.jobs_hot (
                    id, kind, queue, args, state, priority, attempt, max_attempts,
                    run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
                    created_at, errors, metadata, tags, unique_key, unique_states,
                    callback_id, callback_timeout_at, callback_filter, callback_on_complete,
                    callback_on_fail, callback_transform, run_lease, progress
                )
                SELECT
                    id,
                    kind,
                    queue,
                    args,
                    'available'::awa.job_state,
                    priority,
                    attempt,
                    max_attempts,
                    now(),
                    NULL,
                    NULL,
                    attempted_at,
                    finalized_at,
                    created_at,
                    errors,
                    metadata,
                    tags,
                    unique_key,
                    unique_states,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    run_lease,
                    progress
                FROM due
                RETURNING queue
            )
            SELECT queue FROM promoted
            "#,
        )
        .bind(state)
        .bind(PROMOTE_BATCH_SIZE)
        .fetch_all(&mut *tx)
        .await?;

        let promoted = promoted_rows.len();
        self.metrics
            .record_promotion_batch(state, promoted as u64, promote_start.elapsed());
        if promoted == 0 {
            tx.commit().await?;
            return Ok((0, HashSet::new()));
        }

        let queues: HashSet<String> = promoted_rows.into_iter().map(|(queue,)| queue).collect();

        tx.commit().await?;
        Ok((promoted, queues))
    }

    /// Clean up completed/failed/cancelled jobs past retention.
    ///
    /// Targets `jobs_hot` directly (bypassing the `awa.jobs` INSTEAD OF trigger)
    /// since terminal-state jobs always reside in `jobs_hot`.
    /// Runs a global pass for queues without overrides, then per-queue passes
    /// for queues with custom retention.
    #[tracing::instrument(skip(self), name = "maintenance.cleanup")]
    async fn cleanup_completed(&self) {
        let mut total_deleted: u64 = 0;

        // Collect override queue names for the exclusion clause
        let override_queues: Vec<String> = self.queue_retention_overrides.keys().cloned().collect();

        // Global pass: delete jobs in queues that do NOT have overrides
        let completed_retention = format!("{} seconds", self.completed_retention.as_secs());
        let failed_retention = format!("{} seconds", self.failed_retention.as_secs());

        let global_result = if override_queues.is_empty() {
            sqlx::query(
                r#"
                DELETE FROM awa.jobs_hot
                WHERE id IN (
                    SELECT id FROM awa.jobs_hot
                    WHERE (state = 'completed' AND finalized_at < now() - $1::interval)
                       OR (state IN ('failed', 'cancelled') AND finalized_at < now() - $2::interval)
                    LIMIT $3
                )
                "#,
            )
            .bind(&completed_retention)
            .bind(&failed_retention)
            .bind(self.cleanup_batch_size)
            .execute(&self.pool)
            .await
        } else {
            sqlx::query(
                r#"
                DELETE FROM awa.jobs_hot
                WHERE id IN (
                    SELECT id FROM awa.jobs_hot
                    WHERE ((state = 'completed' AND finalized_at < now() - $1::interval)
                       OR (state IN ('failed', 'cancelled') AND finalized_at < now() - $2::interval))
                      AND queue != ALL($4::text[])
                    LIMIT $3
                )
                "#,
            )
            .bind(&completed_retention)
            .bind(&failed_retention)
            .bind(self.cleanup_batch_size)
            .bind(&override_queues)
            .execute(&self.pool)
            .await
        };

        match global_result {
            Ok(result) if result.rows_affected() > 0 => {
                total_deleted += result.rows_affected();
            }
            Err(err) => {
                error!(error = %err, "Failed to clean up old jobs (global pass)");
            }
            _ => {}
        }

        // Per-queue override passes
        for (queue_name, policy) in &self.queue_retention_overrides {
            let queue_completed = format!("{} seconds", policy.completed.as_secs());
            let queue_failed = format!("{} seconds", policy.failed.as_secs());

            match sqlx::query(
                r#"
                DELETE FROM awa.jobs_hot
                WHERE id IN (
                    SELECT id FROM awa.jobs_hot
                    WHERE queue = $4
                      AND ((state = 'completed' AND finalized_at < now() - $1::interval)
                        OR (state IN ('failed', 'cancelled') AND finalized_at < now() - $2::interval))
                    LIMIT $3
                )
                "#,
            )
            .bind(&queue_completed)
            .bind(&queue_failed)
            .bind(self.cleanup_batch_size)
            .bind(queue_name)
            .execute(&self.pool)
            .await
            {
                Ok(result) if result.rows_affected() > 0 => {
                    total_deleted += result.rows_affected();
                    debug!(
                        queue = %queue_name,
                        count = result.rows_affected(),
                        "Cleaned up old jobs (queue override)"
                    );
                }
                Err(err) => {
                    error!(
                        queue = %queue_name,
                        error = %err,
                        "Failed to clean up old jobs (queue override)"
                    );
                }
                _ => {}
            }
        }

        if total_deleted > 0 {
            info!(count = total_deleted, "Cleaned up old jobs");
        }
    }
}

/// Compute the latest fire time for a cron job row, using its expression and timezone.
///
/// Returns `None` if no fire is due (next occurrence is in the future).
fn compute_fire_time(
    row: &CronJobRow,
    now: chrono::DateTime<Utc>,
) -> Option<chrono::DateTime<Utc>> {
    let cron = match Cron::new(&row.cron_expr).parse() {
        Ok(c) => c,
        Err(err) => {
            error!(cron_name = %row.name, error = %err, "Invalid cron expression in database");
            return None;
        }
    };

    let tz: chrono_tz::Tz = match row.timezone.parse() {
        Ok(tz) => tz,
        Err(err) => {
            error!(cron_name = %row.name, error = %err, "Invalid timezone in database");
            return None;
        }
    };

    let search_start = match row.last_enqueued_at {
        Some(last) => last.with_timezone(&tz),
        // First registration: search from cron_jobs.created_at so that
        // low-frequency schedules (weekly, monthly) still find their most
        // recent past fire. Previously capped at 24h which missed them.
        None => row.created_at.with_timezone(&tz),
    };

    let mut latest_fire: Option<chrono::DateTime<Utc>> = None;

    for fire_time in cron.iter_from(search_start) {
        let fire_utc = fire_time.with_timezone(&Utc);

        if fire_utc > now {
            break;
        }

        if let Some(last) = row.last_enqueued_at {
            if fire_utc <= last {
                continue;
            }
        }

        latest_fire = Some(fire_utc);
    }

    latest_fire
}
