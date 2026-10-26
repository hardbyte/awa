use crate::runtime::{InFlightMap, RunLease};
use crate::storage::{QueueStorageRuntime, RuntimeStorage};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Run `pass` every `interval` on a fixed grid until `cancel` fires. A pass
/// that outlasts the interval is followed by one full interval of rest, as
/// a sleep after each pass would give, rather than by an immediate pass.
pub(crate) async fn run_on_grid<F, Fut>(interval: Duration, cancel: &CancellationToken, mut pass: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = ticks.tick() => {
                let started = tokio::time::Instant::now();
                pass().await;
                if started.elapsed() >= interval {
                    ticks.reset();
                }
            }
        }
    }
}

/// Background heartbeat service that periodically updates heartbeat_at
/// for all in-flight jobs, and flushes pending progress updates.
pub struct HeartbeatService {
    pool: PgPool,
    storage: RuntimeStorage,
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
        storage: RuntimeStorage,
        in_flight: InFlightMap,
        interval: std::time::Duration,
        alive: Arc<AtomicBool>,
        cancel: CancellationToken,
        metrics: crate::metrics::AwaMetrics,
    ) -> Self {
        Self {
            pool,
            storage,
            in_flight,
            interval,
            batch_size: 500,
            alive,
            cancel,
            metrics,
        }
    }

    async fn timed_pass(&self) {
        let started = std::time::Instant::now();
        self.heartbeat_once().await;
        let elapsed = started.elapsed();
        if elapsed > self.interval {
            warn!(
                pass_ms = elapsed.as_millis() as u64,
                interval_ms = self.interval.as_millis() as u64,
                "Heartbeat pass took longer than the heartbeat interval; attempts it covered \
                 may be rescued as stale if this continues"
            );
        }
    }

    /// Run the heartbeat loop. Returns when cancelled.
    pub async fn run(&self) {
        self.alive.store(true, Ordering::SeqCst);
        debug!(
            interval_ms = self.interval.as_millis(),
            "Heartbeat service started"
        );

        run_on_grid(self.interval, &self.cancel, || self.timed_pass()).await;
        debug!("Heartbeat service shutting down");
        // Final heartbeat before exit
        self.heartbeat_once().await;

        self.alive.store(false, Ordering::SeqCst);
    }

    /// One heartbeat pass: refresh leases for in-flight jobs and flush any
    /// pending progress. No-op when nothing is in flight.
    // Debug-level root span, as for the dispatcher's poll (#449, #455).
    #[tracing::instrument(
        level = "debug",
        parent = None,
        name = "heartbeat.tick",
        skip(self),
        fields(interval_ms = self.interval.as_millis() as u64)
    )]
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
    use std::time::Duration;

    const SECOND: Duration = Duration::from_secs(1);

    async fn pass_starts(pass_duration: Duration, passes: usize) -> Vec<u64> {
        use std::sync::{Arc, Mutex};
        let cancel = tokio_util::sync::CancellationToken::new();
        let origin = tokio::time::Instant::now();
        let starts = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&starts);
        let stopper = cancel.clone();
        super::run_on_grid(30 * SECOND, &cancel, move || {
            let recorder = Arc::clone(&recorder);
            let stopper = stopper.clone();
            async move {
                let count = {
                    let mut starts = recorder.lock().unwrap();
                    starts.push(origin.elapsed().as_secs());
                    starts.len()
                };
                tokio::time::sleep(pass_duration).await;
                if count == passes {
                    stopper.cancel();
                }
            }
        })
        .await;
        let starts = starts.lock().unwrap().clone();
        starts
    }

    #[tokio::test(start_paused = true)]
    async fn passes_shorter_than_the_interval_keep_the_grid() {
        assert_eq!(pass_starts(5 * SECOND, 4).await, vec![30, 60, 90, 120]);
    }

    /// A 45 s pass every 30 s would otherwise run back to back (30, 75, 120);
    /// each overrun is followed by a full interval of rest, as the previous
    /// sleep-after-pass loop gave.
    #[tokio::test(start_paused = true)]
    async fn an_overrunning_pass_is_followed_by_a_full_interval() {
        assert_eq!(pass_starts(45 * SECOND, 4).await, vec![30, 105, 180, 255]);
    }
}
