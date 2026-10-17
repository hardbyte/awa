use crate::runtime::{InFlightMap, RunLease};
use crate::storage::{QueueStorageRuntime, RuntimeStorage};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Heartbeat timing: the tick interval and how old an attempt must be before
/// its first heartbeat is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeartbeatCadence {
    pub interval: Duration,
    pub first_heartbeat_after: Duration,
}

impl HeartbeatCadence {
    /// A claim stamps the attempt's start time, and the stale-heartbeat rescue
    /// treats that stamp as the heartbeat until one is written, so an attempt
    /// is covered for a full staleness window from its claim. Ticks run at an
    /// arbitrary phase relative to registration, so an attempt first becomes
    /// due on the first tick at least `first_heartbeat_after` after it was
    /// registered: with half an interval, that is at most 1.5 intervals after
    /// registration. When `staleness` honours the documented floor of three
    /// intervals this leaves 1.5 intervals before the cutoff, so one failed or
    /// stalled tick is still followed by another before the attempt goes
    /// stale. Attempts that finish within half an interval write no heartbeat.
    /// Below the floor every tick heartbeats every attempt.
    ///
    /// `staleness` is this runtime's setting; the rescue sweep applies the
    /// elected leader's. Fleets should use the same heartbeat settings on
    /// every runtime.
    pub fn new(interval: Duration, staleness: Duration) -> Self {
        let first_heartbeat_after = if staleness >= interval * 3 {
            interval / 2
        } else {
            Duration::ZERO
        };
        Self {
            interval,
            first_heartbeat_after,
        }
    }
}

/// Background heartbeat service that periodically updates heartbeat_at
/// for all in-flight jobs, and flushes pending progress updates.
pub struct HeartbeatService {
    pool: PgPool,
    storage: RuntimeStorage,
    in_flight: InFlightMap,
    cadence: HeartbeatCadence,
    batch_size: usize,
    alive: Arc<AtomicBool>,
    cancel: CancellationToken,
    metrics: crate::metrics::AwaMetrics,
}

impl HeartbeatService {
    pub(crate) fn new(
        pool: PgPool,
        storage: RuntimeStorage,
        in_flight: InFlightMap,
        cadence: HeartbeatCadence,
        alive: Arc<AtomicBool>,
        cancel: CancellationToken,
        metrics: crate::metrics::AwaMetrics,
    ) -> Self {
        Self {
            pool,
            storage,
            in_flight,
            cadence,
            batch_size: 500,
            alive,
            cancel,
            metrics,
        }
    }

    /// Run the heartbeat loop. Returns when cancelled.
    pub async fn run(&self) {
        self.alive.store(true, Ordering::SeqCst);
        debug!(
            interval_ms = self.cadence.interval.as_millis(),
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
                _ = tokio::time::sleep(self.cadence.interval) => {
                    self.heartbeat_once().await;
                }
            }
        }

        self.alive.store(false, Ordering::SeqCst);
    }

    /// One heartbeat pass: refresh leases for in-flight jobs that are due a
    /// heartbeat and flush any pending progress. No-op when neither applies.
    // Debug-level root span, as for the dispatcher's poll (#449, #455).
    #[tracing::instrument(
        level = "debug",
        parent = None,
        name = "heartbeat.tick",
        skip(self),
        fields(interval_ms = self.cadence.interval.as_millis() as u64)
    )]
    async fn heartbeat_once(&self) {
        let mut all_keys: Vec<(i64, RunLease)> = self
            .in_flight
            .keys_older_than(self.cadence.first_heartbeat_after);
        all_keys.sort_unstable();

        // Snapshot pending progress updates from the in-flight registry.
        // This locks each ProgressState briefly and sets in_flight snapshots.
        let mut pending_progress = self.in_flight.snapshot_pending_progress();
        if all_keys.is_empty() && pending_progress.is_empty() {
            return;
        }
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
        match &self.storage {
            RuntimeStorage::Canonical => {
                self.heartbeat_once_canonical(&heartbeat_only, &pending_progress)
                    .await;
            }
            RuntimeStorage::QueueStorage(runtime) => {
                self.heartbeat_once_queue_storage(runtime, &heartbeat_only, &pending_progress)
                    .await;
            }
        }
    }

    async fn heartbeat_once_canonical(
        &self,
        heartbeat_only: &[(i64, RunLease)],
        pending_progress: &[(i64, i64, u64, serde_json::Value)],
    ) {
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
                        let acked: Vec<(i64, i64, u64)> = chunk
                            .iter()
                            .map(|(id, lease, gen, _)| (*id, *lease, *gen))
                            .collect();
                        self.in_flight.ack_progress(&acked);
                    }
                    Err(err) => {
                        warn!(error = %err, "Failed to send heartbeat+progress batch");
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

    async fn heartbeat_once_queue_storage(
        &self,
        runtime: &QueueStorageRuntime,
        heartbeat_only: &[(i64, RunLease)],
        pending_progress: &[(i64, i64, u64, serde_json::Value)],
    ) {
        for chunk in heartbeat_only.chunks(self.batch_size) {
            match runtime.store.heartbeat_batch(&self.pool, chunk).await {
                Ok(updated) => {
                    self.metrics.heartbeat_batches.add(1, &[]);
                    debug!(count = chunk.len(), updated, "Heartbeat batch sent");
                }
                Err(err) => {
                    warn!(error = %err, "Failed to send heartbeat batch");
                }
            }
        }

        if !pending_progress.is_empty() {
            for chunk in pending_progress.chunks(self.batch_size) {
                let heartbeat_batch: Vec<(i64, i64, serde_json::Value)> = chunk
                    .iter()
                    .map(|(id, lease, _, progress)| (*id, *lease, progress.clone()))
                    .collect();
                match runtime
                    .store
                    .heartbeat_progress_batch(&self.pool, &heartbeat_batch)
                    .await
                {
                    Ok(updated) => {
                        self.metrics.heartbeat_batches.add(1, &[]);
                        debug!(
                            count = chunk.len(),
                            updated, "Heartbeat+progress batch sent"
                        );
                        let acked: Vec<(i64, i64, u64)> = chunk
                            .iter()
                            .map(|(id, lease, gen, _)| (*id, *lease, *gen))
                            .collect();
                        self.in_flight.ack_progress(&acked);
                    }
                    Err(err) => {
                        warn!(error = %err, "Failed to send heartbeat+progress batch");
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

#[cfg(test)]
mod tests {
    use super::HeartbeatCadence;
    use std::time::Duration;

    #[test]
    fn first_heartbeat_waits_half_an_interval_when_staleness_meets_the_floor() {
        let interval = Duration::from_secs(30);
        for staleness in [Duration::from_secs(90), Duration::from_secs(600)] {
            assert_eq!(
                HeartbeatCadence::new(interval, staleness).first_heartbeat_after,
                Duration::from_secs(15)
            );
        }
    }

    #[test]
    fn first_heartbeat_is_immediate_below_the_floor() {
        let interval = Duration::from_secs(30);
        for staleness in [Duration::from_secs(89), Duration::ZERO] {
            assert_eq!(
                HeartbeatCadence::new(interval, staleness).first_heartbeat_after,
                Duration::ZERO
            );
        }
    }
}
