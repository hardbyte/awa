use crate::runtime::{InFlightMap, RunLease};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Background heartbeat service that periodically updates heartbeat_at
/// for all in-flight jobs, and flushes pending progress updates.
pub struct HeartbeatService {
    pool: PgPool,
    in_flight: InFlightMap,
    interval: std::time::Duration,
    batch_size: usize,
    alive: Arc<AtomicBool>,
    cancel: CancellationToken,
    metrics: crate::metrics::AwaMetrics,
}

impl HeartbeatService {
    pub(crate) fn new(
        pool: PgPool,
        in_flight: InFlightMap,
        interval: std::time::Duration,
        alive: Arc<AtomicBool>,
        cancel: CancellationToken,
        metrics: crate::metrics::AwaMetrics,
    ) -> Self {
        Self {
            pool,
            in_flight,
            interval,
            batch_size: 500,
            alive,
            cancel,
            metrics,
        }
    }

    /// Run the heartbeat loop. Returns when cancelled.
    #[tracing::instrument(skip(self), fields(interval_ms = self.interval.as_millis() as u64))]
    pub async fn run(&self) {
        self.alive.store(true, Ordering::SeqCst);
        debug!(
            interval_ms = self.interval.as_millis(),
            "Heartbeat service started"
        );

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    debug!("Heartbeat service shutting down");
                    // Final heartbeat before exit
                    self.heartbeat_once().await;
                    break;
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.heartbeat_once().await;
                }
            }
        }

        self.alive.store(false, Ordering::SeqCst);
    }

    #[tracing::instrument(skip(self))]
    async fn heartbeat_once(&self) {
        let mut all_keys: Vec<(i64, RunLease)> = self.in_flight.keys();
        all_keys.sort_unstable();

        if all_keys.is_empty() {
            return;
        }

        // Snapshot pending progress updates from the in-flight registry.
        // This locks each ProgressState briefly and sets in_flight snapshots.
        let mut pending_progress = self.in_flight.snapshot_pending_progress();
        pending_progress.sort_unstable_by_key(|(job_id, run_lease, _, _)| (*job_id, *run_lease));
        let progress_keys: std::collections::HashSet<(i64, i64)> = pending_progress
            .iter()
            .map(|(id, lease, _, _)| (*id, *lease))
            .collect();

        // Partition jobs: those with progress updates vs. heartbeat-only
        let heartbeat_only: Vec<(i64, RunLease)> = all_keys
            .iter()
            .filter(|key| !progress_keys.contains(key))
            .copied()
            .collect();

        // Tier 1: heartbeat-only jobs (unchanged query)
        for chunk in heartbeat_only.chunks(self.batch_size) {
            let job_ids: Vec<i64> = chunk.iter().map(|(job_id, _)| *job_id).collect();
            let run_leases: Vec<i64> = chunk.iter().map(|(_, run_lease)| *run_lease).collect();
            match sqlx::query(
                r#"
                WITH inflight AS (
                    SELECT * FROM unnest($1::bigint[], $2::bigint[]) AS v(id, run_lease)
                ),
                locked AS (
                    SELECT jobs.ctid
                    FROM awa.jobs_hot AS jobs
                    JOIN inflight
                      ON jobs.id = inflight.id
                     AND jobs.run_lease = inflight.run_lease
                    WHERE jobs.state = 'running'
                    ORDER BY jobs.id
                    FOR UPDATE OF jobs
                )
                UPDATE awa.jobs_hot AS jobs
                SET heartbeat_at = now()
                FROM locked
                WHERE jobs.ctid = locked.ctid
                "#,
            )
            .bind(&job_ids)
            .bind(&run_leases)
            .execute(&self.pool)
            .await
            {
                Ok(result) => {
                    self.metrics.heartbeat_batches.add(1, &[]);
                    debug!(
                        count = job_ids.len(),
                        updated = result.rows_affected(),
                        "Heartbeat batch sent"
                    );
                }
                Err(err) => {
                    warn!(error = %err, "Failed to send heartbeat batch");
                }
            }
        }

        // Tier 2: jobs with pending progress (heartbeat + progress flush)
        if !pending_progress.is_empty() {
            for chunk in pending_progress.chunks(self.batch_size) {
                let job_ids: Vec<i64> = chunk.iter().map(|(id, _, _, _)| *id).collect();
                let run_leases: Vec<i64> = chunk.iter().map(|(_, lease, _, _)| *lease).collect();
                let progress_values: Vec<serde_json::Value> =
                    chunk.iter().map(|(_, _, _, p)| p.clone()).collect();

                match sqlx::query(
                    r#"
                    WITH inflight AS (
                        SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::jsonb[]) AS v(id, run_lease, progress)
                    ),
                    locked AS (
                        SELECT jobs.ctid, inflight.progress
                        FROM awa.jobs_hot AS jobs
                        JOIN inflight
                          ON jobs.id = inflight.id
                         AND jobs.run_lease = inflight.run_lease
                        WHERE jobs.state = 'running'
                        ORDER BY jobs.id
                        FOR UPDATE OF jobs
                    )
                    UPDATE awa.jobs_hot AS jobs
                    SET heartbeat_at = now(),
                        progress = locked.progress
                    FROM locked
                    WHERE jobs.ctid = locked.ctid
                    "#,
                )
                .bind(&job_ids)
                .bind(&run_leases)
                .bind(&progress_values)
                .execute(&self.pool)
                .await
                {
                    Ok(result) => {
                        self.metrics.heartbeat_batches.add(1, &[]);
                        debug!(
                            count = job_ids.len(),
                            updated = result.rows_affected(),
                            "Heartbeat+progress batch sent"
                        );
                        // Acknowledge successful flush
                        let acked: Vec<(i64, i64, u64)> = chunk
                            .iter()
                            .map(|(id, lease, gen, _)| (*id, *lease, *gen))
                            .collect();
                        self.in_flight.ack_progress(&acked);
                    }
                    Err(err) => {
                        warn!(error = %err, "Failed to send heartbeat+progress batch");
                        // Clear in-flight snapshots so they can be retried next cycle
                        let failed: Vec<(i64, i64, u64)> = chunk
                            .iter()
                            .map(|(id, lease, gen, _)| (*id, *lease, *gen))
                            .collect();
                        self.in_flight.clear_in_flight_progress(&failed);
                    }
                }
            }
        }
    }
}
