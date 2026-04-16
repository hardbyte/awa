use crate::executor::DlqPolicy;
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
    alive: Arc<AtomicBool>,
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
    queue_stats_interval: Duration,
    dirty_key_recompute_interval: Duration,
    metadata_reconciliation_interval: Duration,
    /// Interval for priority aging — jobs waiting longer than this have their
    /// priority improved by one level per interval elapsed (default: 60s).
    priority_aging_interval: Duration,
    /// How long a descriptor catalog row can sit without being refreshed
    /// before the maintenance leader deletes it. Zero disables cleanup.
    /// Default: 30 days.
    descriptor_retention: Duration,
    dlq_retention: Duration,
    dlq_cleanup_batch_size: i64,
    dlq_policy: DlqPolicy,
}

const PROMOTE_BATCH_SIZE: i64 = 4_096;
const PROMOTE_MAX_BATCHES_PER_TICK: usize = 32;

impl MaintenanceService {
    pub(crate) fn new(
        pool: PgPool,
        metrics: crate::metrics::AwaMetrics,
        leader: Arc<AtomicBool>,
        alive: Arc<AtomicBool>,
        cancel: CancellationToken,
        periodic_jobs: Arc<Vec<PeriodicJob>>,
        in_flight: InFlightMap,
    ) -> Self {
        Self {
            pool,
            metrics,
            cancel,
            leader,
            alive,
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
            queue_stats_interval: Duration::from_secs(30),
            dirty_key_recompute_interval: Duration::from_secs(2),
            metadata_reconciliation_interval: Duration::from_secs(60),
            priority_aging_interval: Duration::from_secs(60),
            descriptor_retention: Duration::from_secs(30 * 86400), // 30d
            dlq_retention: Duration::from_secs(60 * 60 * 24 * 30), // 30 days
            dlq_cleanup_batch_size: 1000,
            dlq_policy: DlqPolicy::default(),
        }
    }

    /// Per-queue DLQ policy used to decide whether callback-timeout rescues on
    /// exhausted attempts should be routed into `jobs_dlq` instead of leaving
    /// the row in `jobs_hot` as `failed`.
    pub(crate) fn dlq_policy(mut self, policy: DlqPolicy) -> Self {
        self.dlq_policy = policy;
        self
    }

    /// Set retention for DLQ rows (default: 30 days).
    pub fn dlq_retention(mut self, retention: Duration) -> Self {
        self.dlq_retention = retention;
        self
    }

    /// Set the maximum number of DLQ rows deleted per cleanup pass (default: 1000).
    pub fn dlq_cleanup_batch_size(mut self, batch_size: i64) -> Self {
        self.dlq_cleanup_batch_size = batch_size;
        self
    }

    /// Set the priority aging interval (default: 60s).
    ///
    /// Jobs waiting longer than this per priority level are promoted:
    /// a priority-4 job waiting 180s is treated as priority-1.
    pub fn priority_aging_interval(mut self, interval: Duration) -> Self {
        self.priority_aging_interval = interval;
        self
    }

    /// How long a descriptor catalog row can go without being re-synced
    /// before the maintenance leader deletes it (default: 30 days). Set
    /// to `Duration::ZERO` to disable — useful if you maintain the catalog
    /// externally or want to keep historical descriptors forever.
    ///
    /// Descriptors carry no FK from jobs, so deletion is safe: a later
    /// worker restart that re-declares the same queue or kind will
    /// recreate the row from its declaration on the next snapshot tick.
    pub fn descriptor_retention(mut self, retention: Duration) -> Self {
        self.descriptor_retention = retention;
        self
    }

    /// Set the leader election retry interval (default: 10s).
    ///
    /// Controls how often a non-leader instance retries acquiring the
    /// advisory lock. Lower values speed up leader election in tests.
    pub fn leader_election_interval(mut self, interval: Duration) -> Self {
        self.leader_election_interval = interval;
        self
    }

    /// Set the leader connection health-check interval (default: 30s).
    pub fn leader_check_interval(mut self, interval: Duration) -> Self {
        self.leader_check_interval = interval;
        self
    }

    /// Set the promotion interval for scheduled/retryable jobs.
    pub fn promote_interval(mut self, interval: Duration) -> Self {
        self.promote_interval = interval;
        self
    }

    /// Set the stale-heartbeat rescue interval (default: 30s).
    pub fn heartbeat_rescue_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_rescue_interval = interval;
        self
    }

    /// Set the deadline rescue interval (default: 30s).
    pub fn deadline_rescue_interval(mut self, interval: Duration) -> Self {
        self.deadline_rescue_interval = interval;
        self
    }

    /// Set the callback-timeout rescue interval (default: 30s).
    pub fn callback_rescue_interval(mut self, interval: Duration) -> Self {
        self.callback_rescue_interval = interval;
        self
    }

    /// Set how long a heartbeat must be stale before the job is rescued (default: 90s).
    ///
    /// Should be at least 3× the heartbeat interval to avoid false rescues
    /// from transient delays. The run-lease guard prevents duplicate completions
    /// even if a false rescue occurs, but wasted work is still undesirable.
    pub fn heartbeat_staleness(mut self, staleness: Duration) -> Self {
        self.heartbeat_staleness = staleness;
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

    /// Set the interval for publishing queue depth/lag metrics (default: 30s).
    pub fn queue_stats_interval(mut self, interval: Duration) -> Self {
        self.queue_stats_interval = interval;
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
        self.alive.store(true, Ordering::SeqCst);
        let _alive_guard = MaintenanceAliveGuard(self.alive.clone());
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
            let mut queue_stats_timer = tokio::time::interval(self.queue_stats_interval);
            let mut dirty_key_timer = tokio::time::interval(self.dirty_key_recompute_interval);
            let mut metadata_reconciliation_timer =
                tokio::time::interval(self.metadata_reconciliation_interval);
            let mut priority_aging_timer = tokio::time::interval(self.priority_aging_interval);

            // Skip the first immediate tick
            heartbeat_rescue_timer.tick().await;
            deadline_rescue_timer.tick().await;
            callback_rescue_timer.tick().await;
            promote_timer.tick().await;
            cleanup_timer.tick().await;
            cron_sync_timer.tick().await;
            cron_eval_timer.tick().await;
            leader_check_timer.tick().await;
            queue_stats_timer.tick().await;
            dirty_key_timer.tick().await;
            metadata_reconciliation_timer.tick().await;
            priority_aging_timer.tick().await;

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
                        self.cleanup_stale_runtime_snapshots().await;
                        self.cleanup_stale_descriptors().await;
                        self.cleanup_dlq_rows().await;
                    }
                    _ = cron_sync_timer.tick() => {
                        self.sync_periodic_jobs_to_db().await;
                    }
                    _ = cron_eval_timer.tick() => {
                        self.evaluate_cron_schedules().await;
                    }
                    _ = queue_stats_timer.tick() => {
                        self.publish_queue_health_metrics().await;
                    }
                    _ = dirty_key_timer.tick() => {
                        self.recompute_dirty_admin_metadata().await;
                    }
                    _ = metadata_reconciliation_timer.tick() => {
                        self.refresh_admin_metadata().await;
                    }
                    _ = priority_aging_timer.tick() => {
                        self.age_waiting_priorities().await;
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
        let staleness_ms = self.heartbeat_staleness.as_millis() as i64;
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
                SELECT id FROM awa.jobs_hot
                WHERE state = 'running'
                  AND heartbeat_at < now() - ($1 * interval '1 millisecond')
                LIMIT 500
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            "#,
        )
        .bind(staleness_ms)
        .fetch_all(&self.pool)
        .await
        {
            Ok(rescued) if !rescued.is_empty() => {
                self.metrics.maintenance_rescues.add(
                    rescued.len() as u64,
                    &[opentelemetry::KeyValue::new("awa.rescue.kind", "heartbeat")],
                );
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
                SELECT id FROM awa.jobs_hot
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
                self.metrics.maintenance_rescues.add(
                    rescued.len() as u64,
                    &[opentelemetry::KeyValue::new("awa.rescue.kind", "deadline")],
                );
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
                SELECT id FROM awa.jobs_hot
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
                self.metrics.maintenance_rescues.add(
                    rescued.len() as u64,
                    &[opentelemetry::KeyValue::new(
                        "awa.rescue.kind",
                        "callback_timeout",
                    )],
                );
                warn!(count = rescued.len(), "Rescued callback-timed-out jobs");
            }
            Err(err) => {
                error!(error = %err, "Failed to rescue callback-timed-out jobs");
            }
            _ => {}
        }
    }

    /// Age priorities for jobs that have been waiting longer than `priority_aging_interval`.
    ///
    /// Decrements `priority` by 1 per pass for available jobs waiting longer than
    /// the aging interval (minimum priority 1). On the first age, stores the
    /// original priority in `metadata._awa_original_priority` so the API can
    /// report it accurately.
    #[tracing::instrument(skip(self), name = "maintenance.priority_aging")]
    async fn age_waiting_priorities(&self) {
        let aging_secs = self.priority_aging_interval.as_secs() as f64;
        if aging_secs <= 0.0 {
            return;
        }
        match sqlx::query_scalar::<_, i64>(
            r#"
            WITH eligible AS (
                SELECT id FROM awa.jobs_hot
                WHERE state = 'available'
                  AND priority > 1
                  AND run_at <= now() - make_interval(secs => $1)
                LIMIT 1000
                FOR UPDATE SKIP LOCKED
            )
            UPDATE awa.jobs_hot
            SET priority = priority - 1,
                metadata = CASE
                    WHEN NOT (metadata ? '_awa_original_priority')
                    THEN metadata || jsonb_build_object('_awa_original_priority', priority)
                    ELSE metadata
                END
            FROM eligible
            WHERE awa.jobs_hot.id = eligible.id
            RETURNING awa.jobs_hot.id
            "#,
        )
        .bind(aging_secs)
        .fetch_all(&self.pool)
        .await
        {
            Ok(ids) if !ids.is_empty() => {
                debug!(count = ids.len(), "Aged job priorities");
            }
            Err(err) => {
                error!(error = %err, "Failed to age job priorities");
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

    /// SQL template for promotion. The state literal is injected directly
    /// (not as a parameter) so the planner can match the partial index on
    /// `(run_at, id) WHERE state = '<state>'`. With a parameter, the planner
    /// cannot prove the partial index applies and falls back to a full
    /// bitmap scan on multi-million-row tables.
    fn promote_sql(state: &'static str) -> String {
        format!(
            r#"
            WITH due AS (
                DELETE FROM awa.scheduled_jobs
                WHERE id IN (
                    SELECT id
                    FROM awa.scheduled_jobs
                    WHERE state = '{state}'::awa.job_state
                      AND run_at <= now()
                    ORDER BY run_at ASC, id ASC
                    LIMIT $1
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
            "#
        )
    }

    async fn promote_due_batch(
        &self,
        state: &'static str,
    ) -> Result<(usize, HashSet<String>), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let promote_start = std::time::Instant::now();
        let sql = Self::promote_sql(state);
        let promoted_rows: Vec<(String,)> = sqlx::query_as(&sql)
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

struct MaintenanceAliveGuard(Arc<AtomicBool>);

impl Drop for MaintenanceAliveGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Compute the latest fire time for a cron job row, using its expression and timezone.
///
/// Returns `None` if no fire is due (next occurrence is in the future).
fn compute_fire_time(
    row: &CronJobRow,
    now: chrono::DateTime<Utc>,
) -> Option<chrono::DateTime<Utc>> {
    let cron = match Cron::new(&row.cron_expr).with_seconds_optional().parse() {
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
        // First registration: search from one interval before created_at
        // so that the current minute's fire is found. Without this,
        // a schedule created at HH:MM:30 won't find the HH:MM:00 fire
        // because created_at > fire_time, causing up to 60s delay.
        None => (row.created_at - chrono::Duration::minutes(1)).with_timezone(&tz),
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

impl MaintenanceService {
    /// Clean up runtime snapshots older than 24 hours.
    /// Runs as part of the leader's cleanup cycle (not on every snapshot publish).
    #[tracing::instrument(skip(self), name = "maintenance.cleanup_runtime_snapshots")]
    async fn cleanup_stale_runtime_snapshots(&self) {
        if let Err(err) = awa_model::admin::cleanup_runtime_snapshots(
            &self.pool,
            chrono::TimeDelta::try_hours(24).unwrap(),
        )
        .await
        {
            tracing::warn!(error = %err, "Failed to clean up stale runtime snapshots");
        }
    }

    /// Delete catalog rows whose last_seen_at is older than
    /// `descriptor_retention`. Runs alongside the existing cleanup cycle.
    /// When retention is zero this is a no-op, so this stays cheap for
    /// operators who don't want descriptor GC.
    #[tracing::instrument(skip(self), name = "maintenance.cleanup_stale_descriptors")]
    async fn cleanup_stale_descriptors(&self) {
        if self.descriptor_retention.is_zero() {
            return;
        }
        let max_age = chrono::TimeDelta::from_std(self.descriptor_retention)
            .unwrap_or_else(|_| chrono::TimeDelta::try_days(30).unwrap());
        for table in ["awa.queue_descriptors", "awa.job_kind_descriptors"] {
            match awa_model::admin::cleanup_stale_descriptors(&self.pool, table, max_age).await {
                Ok(deleted) if deleted > 0 => {
                    tracing::info!(table, deleted, "Cleaned up stale descriptor rows");
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(table, error = %err, "Failed to clean up stale descriptors");
                }
            }
        }
    }

    /// Drain dirty keys and recompute exact cached rows for recently-touched
    /// queues and kinds. This is the primary cache update mechanism — called
    /// every ~2s to keep dashboard counters fresh.
    #[tracing::instrument(skip(self), name = "maintenance.recompute_dirty_metadata")]
    async fn recompute_dirty_admin_metadata(&self) {
        match awa_model::admin::recompute_dirty_admin_metadata(&self.pool).await {
            Ok(count) if count > 0 => {
                tracing::debug!(count, "Recomputed dirty admin metadata keys");
            }
            Err(err) => {
                tracing::warn!(error = %err, "Failed to recompute dirty admin metadata");
            }
            _ => {}
        }
    }

    /// Full reconciliation of admin metadata from base tables.
    /// Safety net for any drift — runs infrequently (~60s).
    #[tracing::instrument(skip(self), name = "maintenance.refresh_admin_metadata")]
    async fn refresh_admin_metadata(&self) {
        if let Err(err) = awa_model::admin::refresh_admin_metadata(&self.pool).await {
            tracing::warn!(error = %err, "Failed to refresh admin metadata");
        }
    }

    /// Publish queue depth and lag as OTel gauge metrics.
    #[tracing::instrument(skip(self), name = "maintenance.queue_stats")]
    async fn publish_queue_health_metrics(&self) {
        let stats = match awa_model::admin::queue_overviews(&self.pool).await {
            Ok(stats) => stats,
            Err(err) => {
                tracing::warn!(error = %err, "Failed to query queue stats for metrics");
                return;
            }
        };

        for queue_stat in &stats {
            let queue = &queue_stat.queue;

            // Depth per state
            self.metrics
                .record_queue_depth(queue, "available", queue_stat.available);
            self.metrics
                .record_queue_depth(queue, "running", queue_stat.running);
            self.metrics
                .record_queue_depth(queue, "failed", queue_stat.failed);
            self.metrics
                .record_queue_depth(queue, "scheduled", queue_stat.scheduled);
            self.metrics
                .record_queue_depth(queue, "retryable", queue_stat.retryable);
            self.metrics
                .record_queue_depth(queue, "waiting_external", queue_stat.waiting_external);

            // Lag
            if let Some(lag_seconds) = queue_stat.lag_seconds {
                self.metrics.record_queue_lag(queue, lag_seconds);
            }
        }

        // DLQ depth — separate query because DLQ lives outside queue_stats.
        match awa_model::dlq::dlq_depth_by_queue(&self.pool).await {
            Ok(depths) => {
                for (queue, count) in depths {
                    self.metrics.record_dlq_depth(&queue, count);
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "Failed to query DLQ depth for metrics");
            }
        }
    }

    /// Delete DLQ rows older than `dlq_retention`.
    ///
    /// DLQ rows live in a separate table (`awa.jobs_dlq`) that the hot-path
    /// claim loop never touches, so DLQ retention is independent of the
    /// `failed_retention` policy and typically runs much longer (default 30d).
    #[tracing::instrument(skip(self), name = "maintenance.cleanup_dlq")]
    async fn cleanup_dlq_rows(&self) {
        match awa_model::dlq::cleanup_dlq(
            &self.pool,
            self.dlq_retention,
            self.dlq_cleanup_batch_size,
        )
        .await
        {
            Ok(count) if count > 0 => {
                self.metrics.record_dlq_purged(None, count);
                tracing::debug!(count, "Cleaned up expired DLQ rows");
            }
            Err(err) => {
                tracing::warn!(error = %err, "Failed to clean up DLQ rows");
            }
            _ => {}
        }
    }
}
