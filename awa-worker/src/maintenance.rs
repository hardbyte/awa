use awa_model::JobRow;
use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Maintenance service: runs leader-elected background tasks.
///
/// Tasks: heartbeat rescue, deadline rescue, scheduled promotion, cleanup.
pub struct MaintenanceService {
    pool: PgPool,
    cancel: CancellationToken,
    heartbeat_rescue_interval: Duration,
    deadline_rescue_interval: Duration,
    promote_interval: Duration,
    cleanup_interval: Duration,
    heartbeat_staleness: Duration,
    completed_retention: Duration,
    failed_retention: Duration,
}

impl MaintenanceService {
    pub fn new(pool: PgPool, cancel: CancellationToken) -> Self {
        Self {
            pool,
            cancel,
            heartbeat_rescue_interval: Duration::from_secs(30),
            deadline_rescue_interval: Duration::from_secs(30),
            promote_interval: Duration::from_secs(5),
            cleanup_interval: Duration::from_secs(60),
            heartbeat_staleness: Duration::from_secs(90),
            completed_retention: Duration::from_secs(86400), // 24h
            failed_retention: Duration::from_secs(259200),   // 72h
        }
    }

    /// Run the maintenance loop. Attempts leader election first.
    pub async fn run(&self) {
        info!("Maintenance service starting");

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
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_secs(10)) => continue,
                    }
                }
                Err(err) => {
                    warn!(error = %err, "Failed to check leader status");
                    tokio::select! {
                        _ = self.cancel.cancelled() => {
                            debug!("Maintenance service shutting down (leader check failed)");
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_secs(10)) => continue,
                    }
                }
            };

            debug!("Elected as maintenance leader");

            // Run maintenance tasks as leader
            let mut heartbeat_rescue_timer = tokio::time::interval(self.heartbeat_rescue_interval);
            let mut deadline_rescue_timer = tokio::time::interval(self.deadline_rescue_interval);
            let mut promote_timer = tokio::time::interval(self.promote_interval);
            let mut cleanup_timer = tokio::time::interval(self.cleanup_interval);

            // Skip the first immediate tick
            heartbeat_rescue_timer.tick().await;
            deadline_rescue_timer.tick().await;
            promote_timer.tick().await;
            cleanup_timer.tick().await;

            loop {
                tokio::select! {
                    _ = self.cancel.cancelled() => {
                        debug!("Maintenance service shutting down");
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
                    _ = promote_timer.tick() => {
                        self.promote_scheduled().await;
                    }
                    _ = cleanup_timer.tick() => {
                        self.cleanup_completed().await;
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

    /// Rescue jobs with stale heartbeats (crash detection).
    async fn rescue_stale_heartbeats(&self) {
        let staleness_str = format!("{} seconds", self.heartbeat_staleness.as_secs());
        match sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE awa.jobs
            SET state = 'retryable',
                finalized_at = now(),
                heartbeat_at = NULL,
                deadline_at = NULL,
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
            }
            Err(err) => {
                error!(error = %err, "Failed to rescue stale heartbeat jobs");
            }
            _ => {}
        }
    }

    /// Rescue jobs that exceeded their hard deadline.
    async fn rescue_expired_deadlines(&self) {
        match sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE awa.jobs
            SET state = 'retryable',
                finalized_at = now(),
                heartbeat_at = NULL,
                deadline_at = NULL,
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
            }
            Err(err) => {
                error!(error = %err, "Failed to rescue deadline-expired jobs");
            }
            _ => {}
        }
    }

    /// Promote scheduled jobs that are now due.
    async fn promote_scheduled(&self) {
        match sqlx::query(
            "UPDATE awa.jobs SET state = 'available' WHERE state = 'scheduled' AND run_at <= now()",
        )
        .execute(&self.pool)
        .await
        {
            Ok(result) if result.rows_affected() > 0 => {
                debug!(count = result.rows_affected(), "Promoted scheduled jobs");
            }
            Err(err) => {
                error!(error = %err, "Failed to promote scheduled jobs");
            }
            _ => {}
        }

        // Also promote retryable jobs whose backoff has elapsed
        match sqlx::query(
            "UPDATE awa.jobs SET state = 'available' WHERE state = 'retryable' AND run_at <= now()",
        )
        .execute(&self.pool)
        .await
        {
            Ok(result) if result.rows_affected() > 0 => {
                debug!(
                    count = result.rows_affected(),
                    "Promoted retryable jobs (backoff elapsed)"
                );
            }
            Err(err) => {
                error!(error = %err, "Failed to promote retryable jobs");
            }
            _ => {}
        }
    }

    /// Clean up completed/failed/cancelled jobs past retention.
    async fn cleanup_completed(&self) {
        let completed_retention = format!("{} seconds", self.completed_retention.as_secs());
        let failed_retention = format!("{} seconds", self.failed_retention.as_secs());

        match sqlx::query(
            r#"
            DELETE FROM awa.jobs
            WHERE id IN (
                SELECT id FROM awa.jobs
                WHERE (state = 'completed' AND finalized_at < now() - $1::interval)
                   OR (state IN ('failed', 'cancelled') AND finalized_at < now() - $2::interval)
                LIMIT 1000
            )
            "#,
        )
        .bind(&completed_retention)
        .bind(&failed_retention)
        .execute(&self.pool)
        .await
        {
            Ok(result) if result.rows_affected() > 0 => {
                info!(count = result.rows_affected(), "Cleaned up old jobs");
            }
            Err(err) => {
                error!(error = %err, "Failed to clean up old jobs");
            }
            _ => {}
        }
    }
}
