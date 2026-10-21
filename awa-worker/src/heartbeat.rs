use crate::runtime::{InFlightMap, RunLease};
use crate::storage::{QueueStorageRuntime, RuntimeStorage};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Heartbeat timing: the tick interval and the staleness the deferral of a
/// fresh attempt's first heartbeat is budgeted against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeartbeatCadence {
    pub interval: Duration,
    pub staleness: Duration,
    pub defer_first: bool,
}

impl HeartbeatCadence {
    pub fn new(interval: Duration, staleness: Duration) -> Self {
        Self {
            interval,
            staleness,
            defer_first: false,
        }
    }

    pub fn with_first_heartbeat_deferral(mut self, enabled: bool) -> Self {
        self.defer_first = enabled;
        self
    }

    /// How long after its claim started an attempt may go without a
    /// heartbeat before a tick includes it.
    ///
    /// A claim stamps the attempt's start, and the stale-heartbeat rescue
    /// treats that stamp as the heartbeat until one is written. Ticks fire at
    /// a fixed rate, so the first tick that includes an attempt comes between
    /// `deferral` and `deferral + interval` after its claim, instead of within
    /// one interval. The deferral is capped at half an interval and is at
    /// most `staleness - 2 * interval - slowest recent pass`, so a database
    /// that has been slow to answer heartbeats shortens it; at the 3x floor a
    /// pass of one interval disables it. This is feedback from completed
    /// passes, not a prediction: a later stall still eats into the budget.
    /// Below the documented floor of three intervals, or when deferral is not
    /// enabled, every tick includes every attempt.
    ///
    /// The cost is stall tolerance for a fresh attempt. If its first included
    /// pass stalls, for example waiting for a pool connection, the attempt
    /// survives a stall shorter than `staleness - interval - deferral` rather
    /// than `staleness - interval`. Recent passes cannot predict a later
    /// stall, so a stall that starts after a healthy period can rescue a live
    /// attempt that an immediate first heartbeat would have protected.
    ///
    /// `staleness` is this runtime's setting; the rescue sweep applies the
    /// elected leader's. Fleets should use the same heartbeat settings on
    /// every runtime.
    pub fn first_heartbeat_after(&self, slowest_recent_pass: Duration) -> Duration {
        if !self.defer_first || self.staleness < self.interval * 3 {
            return Duration::ZERO;
        }
        (self.interval / 2).min(
            self.staleness
                .saturating_sub(self.interval * 2)
                .saturating_sub(slowest_recent_pass),
        )
    }
}

const RECENT_PASSES: usize = 8;

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
    cadence: HeartbeatCadence,
    batch_size: usize,
    alive: Arc<AtomicBool>,
    cancel: CancellationToken,
    metrics: crate::metrics::AwaMetrics,
    recent_passes: std::sync::Mutex<std::collections::VecDeque<Duration>>,
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
            recent_passes: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(
                RECENT_PASSES,
            )),
        }
    }

    fn slowest_recent_pass(&self) -> Duration {
        self.recent_passes
            .lock()
            .expect("recent heartbeat passes poisoned")
            .iter()
            .copied()
            .max()
            .unwrap_or(Duration::ZERO)
    }

    async fn timed_pass(&self) {
        let started = std::time::Instant::now();
        let deferral = self
            .cadence
            .first_heartbeat_after(self.slowest_recent_pass());
        self.heartbeat_once().await;
        let elapsed = started.elapsed();
        if elapsed > self.cadence.interval {
            warn!(
                pass_ms = elapsed.as_millis() as u64,
                interval_ms = self.cadence.interval.as_millis() as u64,
                first_heartbeat_deferral_ms = deferral.as_millis() as u64,
                staleness_ms = self.cadence.staleness.as_millis() as u64,
                "Heartbeat pass took longer than the heartbeat interval; attempts it covered \
                 may be rescued as stale if this continues"
            );
        }
        let mut passes = self
            .recent_passes
            .lock()
            .expect("recent heartbeat passes poisoned");
        if passes.len() == RECENT_PASSES {
            passes.pop_front();
        }
        passes.push_back(elapsed);
    }

    /// Run the heartbeat loop. Returns when cancelled.
    pub async fn run(&self) {
        self.alive.store(true, Ordering::SeqCst);
        debug!(
            interval_ms = self.cadence.interval.as_millis(),
            "Heartbeat service started"
        );

        run_on_grid(self.cadence.interval, &self.cancel, || self.timed_pass()).await;
        debug!("Heartbeat service shutting down");
        // Final heartbeat before exit
        self.heartbeat_once().await;

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
        let mut all_keys: Vec<(i64, RunLease)> = self.in_flight.keys_older_than(
            self.cadence
                .first_heartbeat_after(self.slowest_recent_pass()),
        );
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

    const SECOND: Duration = Duration::from_secs(1);

    #[test]
    fn first_heartbeat_waits_half_an_interval_when_staleness_meets_the_floor() {
        for staleness in [90 * SECOND, 600 * SECOND] {
            assert_eq!(
                HeartbeatCadence::new(30 * SECOND, staleness)
                    .with_first_heartbeat_deferral(true)
                    .first_heartbeat_after(Duration::ZERO),
                15 * SECOND
            );
        }
    }

    #[test]
    fn first_heartbeat_is_immediate_below_the_floor() {
        for staleness in [89 * SECOND, Duration::ZERO] {
            assert_eq!(
                HeartbeatCadence::new(30 * SECOND, staleness)
                    .with_first_heartbeat_deferral(true)
                    .first_heartbeat_after(Duration::ZERO),
                Duration::ZERO
            );
        }
    }

    #[test]
    fn slow_passes_shorten_and_then_disable_the_deferral() {
        let cadence =
            HeartbeatCadence::new(30 * SECOND, 90 * SECOND).with_first_heartbeat_deferral(true);
        assert_eq!(cadence.first_heartbeat_after(10 * SECOND), 15 * SECOND);
        assert_eq!(cadence.first_heartbeat_after(20 * SECOND), 10 * SECOND);
        assert_eq!(cadence.first_heartbeat_after(30 * SECOND), Duration::ZERO);
        assert_eq!(cadence.first_heartbeat_after(45 * SECOND), Duration::ZERO);
    }

    /// I = 30 s, S = 90 s, and the first pass that includes the attempt
    /// fails after 20 s while the next one is fast. With fixed-rate ticks the
    /// retry finishes inside the staleness window whatever the attempt's
    /// phase. A pass that stalls instead of failing is bounded separately;
    /// see the stall-tolerance test.
    #[test]
    fn a_failed_slow_pass_is_followed_by_a_retry_inside_the_window() {
        let interval = 30.0_f64;
        let staleness = 90.0_f64;
        let failed_pass = 20.0_f64;
        let retry_pass = 2.0_f64;
        let cadence = HeartbeatCadence::new(
            Duration::from_secs_f64(interval),
            Duration::from_secs_f64(staleness),
        )
        .with_first_heartbeat_deferral(true);
        let deferral = cadence.first_heartbeat_after(Duration::ZERO).as_secs_f64();
        for tenth in 0..300 {
            let claim = f64::from(tenth) / 10.0;
            let first_due = ((claim + deferral) / interval).ceil() * interval;
            let first_due = if first_due - claim < deferral {
                first_due + interval
            } else {
                first_due
            };
            let retry_start = (first_due + interval).max(first_due + failed_pass);
            assert!(
                retry_start + retry_pass < claim + staleness,
                "claim at {claim}: retry finishes at {} after cutoff {}",
                retry_start + retry_pass,
                claim + staleness
            );
        }
    }

    /// The first pass that includes a fresh attempt stalls before it reaches
    /// the database, for example waiting for a pool connection, and its
    /// heartbeat lands only when the stall ends. Over every claim phase the
    /// attempt survives exactly the stalls shorter than
    /// `staleness - interval - deferral`: 45 s at the 30 s / 90 s defaults,
    /// against 60 s without the deferral.
    #[test]
    fn deferral_shortens_first_heartbeat_stall_tolerance() {
        let interval = 30.0_f64;
        let staleness = 90.0_f64;
        let deferral = HeartbeatCadence::new(
            Duration::from_secs_f64(interval),
            Duration::from_secs_f64(staleness),
        )
        .with_first_heartbeat_deferral(true)
        .first_heartbeat_after(Duration::ZERO)
        .as_secs_f64();
        assert_eq!(deferral, 15.0);
        let survives_every_phase = |deferral: f64, stall: f64| {
            (0..300).all(|tenth| {
                let claim = f64::from(tenth) / 10.0 + 0.05;
                let first_included = ((claim + deferral) / interval).ceil() * interval;
                first_included + stall < claim + staleness
            })
        };
        let tolerance = staleness - interval - deferral;
        assert!(survives_every_phase(deferral, tolerance - 0.1));
        assert!(!survives_every_phase(deferral, tolerance + 0.1));
        assert!(survives_every_phase(0.0, staleness - interval - 0.1));

        // Claim at 15.1 s: the healthy 30 s tick skips the attempt, the 60 s
        // pass waits for a connection until 109 s, and rescue runs at 105.1 s.
        // An immediate first heartbeat at 30 s would have covered it to 120 s.
        let claim = 15.1;
        let first_included = ((claim + deferral) / interval).ceil() * interval;
        assert_eq!(first_included, 60.0);
        assert!(109.0 > claim + staleness);
    }

    #[test]
    fn deferral_is_off_unless_enabled() {
        assert_eq!(
            HeartbeatCadence::new(30 * SECOND, 90 * SECOND).first_heartbeat_after(Duration::ZERO),
            Duration::ZERO
        );
    }

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
    /// each overrun is followed by a full interval of rest, as main's
    /// sleep-after-pass loop gave.
    #[tokio::test(start_paused = true)]
    async fn an_overrunning_pass_is_followed_by_a_full_interval() {
        assert_eq!(pass_starts(45 * SECOND, 4).await, vec![30, 105, 180, 255]);
    }
}
