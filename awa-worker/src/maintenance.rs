use crate::executor::DlqPolicy;
use crate::runtime::InFlightMap;
use crate::storage::RuntimeStorage;
use awa_model::cron::{
    atomic_enqueue, list_cron_jobs, upsert_cron_job, CronJobRow, CronMissedFirePolicy,
};
#[cfg(test)]
use awa_model::SkipReason;
use awa_model::{
    JobRow, JobState, PeriodicJob, PruneOutcome, RotateOutcome, TerminalDeltaRollupOutcome,
};
use chrono::Utc;
use croner::Cron;
use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Per-queue or global retention policy for completed and failed/cancelled jobs.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// How long to keep completed jobs before cleanup.
    pub completed: Duration,
    /// How long to keep failed/cancelled jobs before cleanup.
    pub failed: Duration,
    /// How long to keep DLQ rows before cleanup.
    pub dlq: Option<Duration>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            completed: Duration::from_secs(86400), // 24h
            failed: Duration::from_secs(259200),   // 72h
            dlq: None,
        }
    }
}

/// Per-branch observability state for the maintenance leader's main
/// `tokio::select!` loop. Tracks the most recent body duration so the
/// next fire of the same branch can decide whether it was already
/// overdue, and an `is_delayed` flag so we only log/count one event per
/// "overrun episode" rather than once per tick.
///
/// Wired through `AwaMetrics` — see [`crate::metrics::AwaMetrics`] for the
/// instrument definitions. Issue #242.
#[derive(Debug, Default)]
struct MaintenanceBranchState {
    /// Body duration of the previous run of this branch. `None` until the
    /// branch has run at least once.
    last_duration: Option<Duration>,
    /// True when we've emitted the on-time -> delayed warning/counter
    /// for the current overrun episode. Cleared on the recovery
    /// transition.
    is_delayed: bool,
    /// Number of consecutive overrun observations (last_duration above
    /// the upper threshold). Samples inside the deadband neither
    /// advance this counter nor reset it.
    consecutive_overrun: u32,
    /// Mirror of `consecutive_overrun` for clearly-on-time samples
    /// (last_duration below the lower threshold). Used to gate
    /// delayed -> on-time recovery with the same K-consecutive
    /// requirement.
    consecutive_ontime: u32,
    /// Tick counter that suppresses the branch body while the branch
    /// is unhealthy. Decremented every tick; while non-zero the
    /// caller's body is skipped. Re-armed on every flip-to-delayed
    /// AND every overrun observation while already delayed, so a
    /// branch that keeps failing keeps quietly waiting instead of
    /// hammering the database.
    cooldown_ticks_remaining: u32,
}

/// How many consecutive observations the branch tracker requires before
/// flipping `is_delayed` either direction. Combined with the
/// duration-margin thresholds below: a sample inside the deadband
/// (between `LOWER_FACTOR_*` and `UPPER_FACTOR_*` of `tick_interval`)
/// doesn't advance either counter, so the boundary flap from #316's
/// post-mortem stops at the source. A sample outside the deadband
/// still needs `OVERRUN_HYSTERESIS_K` peers on the same side before
/// the state flips.
const OVERRUN_HYSTERESIS_K: u32 = 3;

/// Duration-margin thresholds. Crossing
/// `last_duration > tick_interval * UPPER` counts as an overrun
/// sample; `last_duration < tick_interval * LOWER` counts as an
/// on-time sample. Everything in between is a deadband that leaves
/// both counters untouched.
///
/// `UPPER = 3/2` and `LOWER = 7/10` give a generous deadband — the
/// branch has to be at 70% of its tick or below to count as recovered,
/// and 50% above its tick to count as overrunning. This prevents
/// 51ms-vs-49ms jitter at the 50ms boundary from advancing either
/// counter, which is what bench evidence on #316 showed was still
/// happening with K-only hysteresis. Integer ratios avoid f64 in the
/// hot path.
const OVERRUN_UPPER_NUM: u32 = 3;
const OVERRUN_UPPER_DEN: u32 = 2;
const OVERRUN_LOWER_NUM: u32 = 7;
const OVERRUN_LOWER_DEN: u32 = 10;

/// Number of ticks to suppress a delayed branch's body. At the default
/// `lease_rotate_interval = 250ms`, 120 ticks = 30s wall time of quiet
/// before the branch tries again. Re-armed on every overrun
/// observation while still delayed.
const BRANCH_COOLDOWN_TICKS: u32 = 120;

/// Records the start of one branch body and emits observability when the
/// body returns. Constructed by [`MaintenanceBranchTracker::try_begin`], which
/// also applies the previous-run overrun check before the body starts so
/// the warning/counter line up with the fire moment described in #242.
struct BranchTimer<'a> {
    tracker: &'a MaintenanceBranchTracker,
    branch: &'static str,
    metrics: &'a crate::metrics::AwaMetrics,
    started_at: Instant,
}

impl<'a> BranchTimer<'a> {
    /// Stop the timer, record the duration into both the per-branch
    /// histogram and the tracker's state. Must be called once after the
    /// body returns — there is no `Drop` impl so a panic-mid-body would
    /// leave the histogram un-recorded, which is fine: a panicking
    /// maintenance branch is a bigger story than the missing sample.
    fn finish(self) {
        let duration = self.started_at.elapsed();
        self.metrics
            .record_maintenance_branch_duration(self.branch, duration);
        self.tracker.record_finish(self.branch, duration);
    }
}

/// Owns the per-branch overrun state for the maintenance leader loop.
/// All access is logically single-threaded — the maintenance loop is the
/// only caller — but the loop runs inside a `tokio::spawn`-ed task that
/// requires `Send`, so we use `std::sync::Mutex` rather than `RefCell`.
/// The mutex is uncontended in practice (one acquirer); cost is one
/// uncontended lock per branch tick.
#[derive(Default)]
struct MaintenanceBranchTracker {
    branches: std::sync::Mutex<HashMap<&'static str, MaintenanceBranchState>>,
}

impl MaintenanceBranchTracker {
    fn new() -> Self {
        Self {
            branches: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Call at the moment a branch arm fires, before running its body.
    /// Applies cooldown + duration-margin hysteresis, then either:
    ///   * returns `Some(BranchTimer)` so the caller runs the body
    ///     and calls `.finish()`, or
    ///   * returns `None` to signal "skip this tick" — the body
    ///     must not run, and no duration sample is recorded for this
    ///     tick (so the K-on-time counter doesn't advance for skipped
    ///     work).
    ///
    /// Two gating phases:
    ///
    /// 1. **Cooldown.** If `cooldown_ticks_remaining > 0`, decrement
    ///    and return `None`. While cooldown is non-zero the branch is
    ///    quiet; this is set on flip-to-delayed and re-armed on
    ///    every subsequent overrun observation, so an unhealthy
    ///    branch backs off instead of beating on the database every
    ///    tick.
    /// 2. **Duration-margin hysteresis.** Compare `last_duration` to
    ///    `tick_interval * UPPER/LOWER`. Outside the deadband, advance
    ///    the matching K-counter; inside, leave both alone. Crossing
    ///    K-consecutive on either side flips `is_delayed` and may
    ///    arm cooldown.
    fn try_begin<'a>(
        &'a self,
        branch: &'static str,
        tick_interval: Duration,
        metrics: &'a crate::metrics::AwaMetrics,
    ) -> Option<BranchTimer<'a>> {
        self.try_begin_with_cooldown(branch, tick_interval, metrics, BRANCH_COOLDOWN_TICKS)
    }

    fn try_begin_without_cooldown<'a>(
        &'a self,
        branch: &'static str,
        tick_interval: Duration,
        metrics: &'a crate::metrics::AwaMetrics,
    ) -> Option<BranchTimer<'a>> {
        self.try_begin_with_cooldown(branch, tick_interval, metrics, 0)
    }

    fn try_begin_with_cooldown<'a>(
        &'a self,
        branch: &'static str,
        tick_interval: Duration,
        metrics: &'a crate::metrics::AwaMetrics,
        cooldown_ticks: u32,
    ) -> Option<BranchTimer<'a>> {
        let mut branches = self
            .branches
            .lock()
            .expect("maintenance branch tracker mutex");
        let state = branches.entry(branch).or_default();

        // Phase 1: cooldown gate. Counts down regardless of any
        // other signal; the branch stays quiet while non-zero.
        if state.cooldown_ticks_remaining > 0 {
            state.cooldown_ticks_remaining -= 1;
            return None;
        }

        // Phase 2: duration-margin + K-consecutive hysteresis.
        // `take()` consumes the sample so we evaluate each body's
        // duration exactly once. Without this, when cooldown returns
        // None (no body runs → no new sample), the next post-cooldown
        // `try_begin` would re-read the same pre-flip overrun sample,
        // see `consecutive_overrun >= K` already, and re-arm cooldown
        // forever. The branch would never run its body again until
        // the worker restarts. Codex + CodeRabbit both flagged this
        // on PR #318.
        if let Some(last_duration) = state.last_duration.take() {
            // Integer-ratio thresholds — avoid f64 in the hot path.
            let upper_threshold = tick_interval * OVERRUN_UPPER_NUM / OVERRUN_UPPER_DEN;
            let lower_threshold = tick_interval * OVERRUN_LOWER_NUM / OVERRUN_LOWER_DEN;

            if last_duration > upper_threshold {
                state.consecutive_overrun = state.consecutive_overrun.saturating_add(1);
                state.consecutive_ontime = 0;
                let cross_threshold = state.consecutive_overrun >= OVERRUN_HYSTERESIS_K;
                if cross_threshold && !state.is_delayed {
                    state.is_delayed = true;
                    state.cooldown_ticks_remaining = cooldown_ticks;
                    warn!(
                        branch,
                        last_duration_ms = last_duration.as_millis() as u64,
                        tick_interval_ms = tick_interval.as_millis() as u64,
                        upper_threshold_ms = upper_threshold.as_millis() as u64,
                        consecutive_overrun = state.consecutive_overrun,
                        cooldown_ticks,
                        "maintenance branch overran tick interval",
                    );
                    metrics.record_maintenance_branch_overrun(branch);
                    if cooldown_ticks > 0 {
                        return None;
                    }
                } else if cross_threshold && state.is_delayed && cooldown_ticks > 0 {
                    // Already delayed and another overrun arrived —
                    // re-arm cooldown but stay quiet about it.
                    state.cooldown_ticks_remaining = cooldown_ticks;
                    return None;
                }
            } else if last_duration < lower_threshold {
                state.consecutive_ontime = state.consecutive_ontime.saturating_add(1);
                state.consecutive_overrun = 0;
                if state.consecutive_ontime >= OVERRUN_HYSTERESIS_K && state.is_delayed {
                    state.is_delayed = false;
                    warn!(
                        branch,
                        last_duration_ms = last_duration.as_millis() as u64,
                        tick_interval_ms = tick_interval.as_millis() as u64,
                        lower_threshold_ms = lower_threshold.as_millis() as u64,
                        consecutive_ontime = state.consecutive_ontime,
                        "maintenance branch recovered to on-time",
                    );
                }
            } else {
                // Deadband — neither counter advances. This is the
                // explicit no-op that kills the 49ms-vs-51ms flap.
            }
        }
        drop(branches);
        Some(BranchTimer {
            tracker: self,
            branch,
            metrics,
            started_at: Instant::now(),
        })
    }

    /// Internal — called by [`BranchTimer::finish`] to stash the body's
    /// wall-clock duration so the next fire of the same branch can apply
    /// the overrun check.
    fn record_finish(&self, branch: &'static str, duration: Duration) {
        let mut branches = self
            .branches
            .lock()
            .expect("maintenance branch tracker mutex");
        let state = branches.entry(branch).or_default();
        state.last_duration = Some(duration);
    }

    /// Test helper — read the current state for one branch. Not used in
    /// production code paths.
    #[cfg(test)]
    fn snapshot(&self, branch: &'static str) -> Option<(Option<Duration>, bool)> {
        let branches = self
            .branches
            .lock()
            .expect("maintenance branch tracker mutex");
        branches
            .get(branch)
            .map(|state| (state.last_duration, state.is_delayed))
    }

    /// Test-only — read the per-branch cooldown counter and consecutive
    /// counters so tests can assert on the full state machine.
    #[cfg(test)]
    fn cooldown_snapshot(&self, branch: &'static str) -> Option<(u32, u32, u32)> {
        let branches = self
            .branches
            .lock()
            .expect("maintenance branch tracker mutex");
        branches.get(branch).map(|state| {
            (
                state.cooldown_ticks_remaining,
                state.consecutive_overrun,
                state.consecutive_ontime,
            )
        })
    }
}

/// Per-segment exponential backoff for the prune step of the queue /
/// lease / claim ring-rotation branches. The rotate step is always
/// cheap (cursor advance under an advisory lock); the prune step is
/// what hurts under pinned MVCC: every tick attempts `LOCK TABLE
/// <child> IN ACCESS EXCLUSIVE MODE` (with a 50ms timeout) followed by
/// `SELECT count(*) FROM <child>`. Under a pinned snapshot the child
/// can't be reclaimed, so the count walks dead tuples and the prune
/// returns `SkippedActive` — every tick. This tracker skips
/// the next 2^level ticks (capped at 32) after a `SkippedActive` or
/// `Blocked` outcome, doubling on each repeat. A successful `Pruned`
/// resets to no backoff. `Noop` (ring empty / nothing to consider)
/// leaves state unchanged — backoff is for "I tried and couldn't",
/// not "there's nothing to do."
#[derive(Default)]
struct PruneBackoffTracker {
    branches: std::sync::Mutex<HashMap<&'static str, PruneBackoffState>>,
}

#[derive(Debug, Default)]
struct PruneBackoffState {
    /// Number of upcoming ticks to skip the prune call entirely.
    /// Decremented on every `should_skip` poll; the tick where it
    /// reaches 0 actually runs prune again.
    skip_remaining: u32,
    /// Last backoff exponent applied. Used to set `skip_remaining` to
    /// `1 << level` on the next failure. Reset to 0 on `Pruned`.
    backoff_level: u8,
}

/// Cap on the backoff exponent. `2^5 = 32` ticks; at the 250ms default
/// `lease_rotate_interval` that is ~8s between prune attempts under
/// sustained pin pressure. Long enough to cut the per-tick scan cost
/// dramatically; short enough that prune resumes promptly once the
/// snapshot is released.
const MAX_PRUNE_BACKOFF_LEVEL: u8 = 5;

impl PruneBackoffTracker {
    fn new() -> Self {
        Self::default()
    }

    /// Returns true when the caller should skip this tick's prune.
    /// Side effect: decrements `skip_remaining` when non-zero.
    fn should_skip(&self, branch: &'static str) -> bool {
        let mut branches = self.branches.lock().expect("prune backoff tracker mutex");
        let state = branches.entry(branch).or_default();
        if state.skip_remaining > 0 {
            state.skip_remaining -= 1;
            true
        } else {
            false
        }
    }

    /// Update backoff state for a completed prune call. `Pruned` resets;
    /// `SkippedActive` / `Blocked` doubles; `Noop` is neutral.
    fn record_outcome(&self, branch: &'static str, outcome: &PruneOutcome) {
        let mut branches = self.branches.lock().expect("prune backoff tracker mutex");
        let state = branches.entry(branch).or_default();
        match outcome {
            PruneOutcome::Pruned { .. } => {
                state.skip_remaining = 0;
                state.backoff_level = 0;
            }
            PruneOutcome::SkippedActive { .. } | PruneOutcome::Blocked { .. } => {
                state.backoff_level = state
                    .backoff_level
                    .saturating_add(1)
                    .min(MAX_PRUNE_BACKOFF_LEVEL);
                state.skip_remaining = 1u32 << state.backoff_level;
            }
            PruneOutcome::Noop => {}
        }
    }

    #[cfg(test)]
    fn snapshot(&self, branch: &'static str) -> Option<(u32, u8)> {
        let branches = self.branches.lock().expect("prune backoff tracker mutex");
        branches
            .get(branch)
            .map(|state| (state.skip_remaining, state.backoff_level))
    }
}

/// Branch keys used by the prune backoff tracker. Kept as `&'static
/// str` so the tracker's HashMap doesn't have to allocate per call.
const PRUNE_BRANCH_LEASE: &str = "lease";
const PRUNE_BRANCH_CLAIM: &str = "claim";
const PRUNE_BRANCH_QUEUE: &str = "queue";

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
    /// ADR-029 follow-up specs registry — used to dispatch `Rescued`
    /// follow-ups (best-effort, separate tx) when this service rescues
    /// jobs from stale heartbeat / expired callback / exceeded deadline.
    enqueue_specs: Arc<
        HashMap<
            crate::enqueue_specs::Outcome,
            HashMap<String, Vec<crate::enqueue_specs::BoxedEnqueueSpec>>,
        >,
    >,
    /// In-process `Rescued` lifecycle hooks (ADR-015), shared with the
    /// executor. Best-effort fire-and-forget per rescued job.
    lifecycle_handlers: Arc<HashMap<String, Vec<crate::events::BoxedUntypedEventHandler>>>,
    /// In-flight job cancellation flags — used to signal deadline/heartbeat rescue
    /// to running handlers on this worker instance.
    in_flight: InFlightMap,
    storage: RuntimeStorage,
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
    dlq_retention: Duration,
    dlq_cleanup_batch_size: i64,
    dlq_policy: DlqPolicy,
    dirty_key_recompute_interval: Duration,
    metadata_reconciliation_interval: Duration,
    /// Interval for priority aging — jobs waiting longer than this have their
    /// priority improved by one level per interval elapsed (default: 60s).
    priority_aging_interval: Duration,
    batch_operations_interval: Duration,
    terminal_count_rollup_interval: Duration,
    /// How long a descriptor catalog row can sit without being refreshed
    /// before the maintenance leader deletes it. Zero disables cleanup.
    /// Default: 30 days.
    descriptor_retention: Duration,
}

const PROMOTE_BATCH_SIZE: i64 = 4_096;
const PROMOTE_MAX_BATCHES_PER_TICK: usize = 32;
const CRON_CATCH_UP_LIMIT: usize = 1_000;
const TERMINAL_COUNT_ROLLUP_MAX_SLOTS_PER_TICK: usize = 4;
type QueueStorageMetricRow = (String, i64, i64, i64, i64, i64, i64, i64, Option<f64>);

impl MaintenanceService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        pool: PgPool,
        metrics: crate::metrics::AwaMetrics,
        leader: Arc<AtomicBool>,
        alive: Arc<AtomicBool>,
        cancel: CancellationToken,
        periodic_jobs: Arc<Vec<PeriodicJob>>,
        in_flight: InFlightMap,
        storage: RuntimeStorage,
        enqueue_specs: Arc<
            HashMap<
                crate::enqueue_specs::Outcome,
                HashMap<String, Vec<crate::enqueue_specs::BoxedEnqueueSpec>>,
            >,
        >,
        lifecycle_handlers: Arc<HashMap<String, Vec<crate::events::BoxedUntypedEventHandler>>>,
    ) -> Self {
        Self {
            pool,
            metrics,
            cancel,
            leader,
            alive,
            periodic_jobs,
            in_flight,
            storage,
            enqueue_specs,
            lifecycle_handlers,
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
            dlq_retention: Duration::from_secs(60 * 60 * 24 * 30),
            dlq_cleanup_batch_size: 1000,
            dlq_policy: DlqPolicy::default(),
            dirty_key_recompute_interval: Duration::from_secs(2),
            metadata_reconciliation_interval: Duration::from_secs(60),
            priority_aging_interval: Duration::from_secs(60),
            batch_operations_interval: Duration::from_secs(1),
            terminal_count_rollup_interval: Duration::from_secs(30),
            descriptor_retention: Duration::from_secs(30 * 86400), // 30d
        }
    }

    /// Set the priority aging interval (default: 60s).
    ///
    /// Jobs waiting longer than this per priority level are promoted:
    /// a priority-4 job waiting 180s is treated as priority-1.
    pub fn priority_aging_interval(mut self, interval: Duration) -> Self {
        self.priority_aging_interval = interval;
        self
    }

    /// Set how often the maintenance leader processes batch-operation chunks.
    pub fn batch_operations_interval(mut self, interval: Duration) -> Self {
        self.batch_operations_interval = interval;
        self
    }

    /// Set how often terminal-count delta rows are folded into the live
    /// counter table for sealed queue segments (default: 30s).
    pub fn terminal_count_rollup_interval(mut self, interval: Duration) -> Self {
        self.terminal_count_rollup_interval = interval;
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

    /// Set the per-queue DLQ policy.
    pub(crate) fn dlq_policy(mut self, policy: DlqPolicy) -> Self {
        self.dlq_policy = policy;
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
            let mut leader_check_timer = tokio::time::interval(self.leader_check_interval);
            let mut queue_stats_timer = tokio::time::interval(self.queue_stats_interval);
            let mut dirty_key_timer = tokio::time::interval(self.dirty_key_recompute_interval);
            let mut metadata_reconciliation_timer =
                tokio::time::interval(self.metadata_reconciliation_interval);
            let mut priority_aging_timer = tokio::time::interval(self.priority_aging_interval);
            let mut batch_operations_timer = tokio::time::interval(self.batch_operations_interval);
            let mut terminal_count_rollup_timer =
                tokio::time::interval(self.terminal_count_rollup_interval);
            let mut vacuum_queue_timer = self
                .storage
                .queue_storage()
                .map(|runtime| tokio::time::interval(runtime.queue_rotate_interval));
            let mut vacuum_lease_timer = self
                .storage
                .queue_storage()
                .map(|runtime| tokio::time::interval(runtime.lease_rotate_interval));
            let mut vacuum_claim_timer = self
                .storage
                .queue_storage()
                .map(|runtime| tokio::time::interval(runtime.claim_rotate_interval));
            // Cache rotate intervals alongside the timers so the per-branch
            // observability calls can recover the configured period. Both
            // derive from the same `queue_storage()` source so they are
            // Some/None in lockstep with their corresponding timers.
            let vacuum_queue_interval = self
                .storage
                .queue_storage()
                .map(|runtime| runtime.queue_rotate_interval);
            let vacuum_lease_interval = self
                .storage
                .queue_storage()
                .map(|runtime| runtime.lease_rotate_interval);
            let vacuum_claim_interval = self
                .storage
                .queue_storage()
                .map(|runtime| runtime.claim_rotate_interval);
            // Per-branch overrun tracker. Issue #242 — observability only;
            // the architectural split of this `tokio::select!` is deferred
            // to v0.7 conditional on the overrun counter showing fleet hits.
            let branch_tracker = MaintenanceBranchTracker::new();
            // Per-segment prune backoff (#169). Gates the prune step of
            // each rotate branch so a pinned MVCC snapshot doesn't make
            // us repeat the ACCESS-EXCLUSIVE + count(*) attempt every
            // tick. Local to the leader loop so a re-election resets
            // backoff to zero.
            let prune_tracker = PruneBackoffTracker::new();

            // Skip the first immediate tick
            heartbeat_rescue_timer.tick().await;
            deadline_rescue_timer.tick().await;
            callback_rescue_timer.tick().await;
            promote_timer.tick().await;
            cleanup_timer.tick().await;
            cron_sync_timer.tick().await;
            leader_check_timer.tick().await;
            queue_stats_timer.tick().await;
            dirty_key_timer.tick().await;
            metadata_reconciliation_timer.tick().await;
            priority_aging_timer.tick().await;
            batch_operations_timer.tick().await;
            terminal_count_rollup_timer.tick().await;
            if let Some(timer) = &mut vacuum_queue_timer {
                timer.tick().await;
            }
            if let Some(timer) = &mut vacuum_lease_timer {
                timer.tick().await;
            }
            if let Some(timer) = &mut vacuum_claim_timer {
                timer.tick().await;
            }

            // Do an initial sync immediately on becoming leader
            self.sync_periodic_jobs_to_db().await;
            let cron_eval_cancel = self.cancel.child_token();
            let cron_eval_task = tokio::spawn(Self::run_cron_evaluator(
                self.pool.clone(),
                cron_eval_cancel.clone(),
                self.cron_eval_interval,
            ));

            loop {
                tokio::select! {
                    _ = self.cancel.cancelled() => {
                        debug!("Maintenance service shutting down");
                        self.leader.store(false, Ordering::SeqCst);
                        Self::stop_cron_evaluator(&cron_eval_cancel, &cron_eval_task);
                        // Release leader lock on the same connection that acquired it.
                        // If this fails, dropping the connection will release the lock anyway.
                        let _ = Self::release_leader(&mut leader_conn).await;
                        return;
                    }
                    _ = heartbeat_rescue_timer.tick() => {
                        if let Some(timer) = branch_tracker.try_begin("rescue_stale_heartbeats", self.heartbeat_rescue_interval, &self.metrics) {
                            self.rescue_stale_heartbeats().await;
                            timer.finish();
                        }
                    }
                    _ = deadline_rescue_timer.tick() => {
                        if let Some(timer) = branch_tracker.try_begin("rescue_expired_deadlines", self.deadline_rescue_interval, &self.metrics) {
                            self.rescue_expired_deadlines().await;
                            timer.finish();
                        }
                    }
                    _ = callback_rescue_timer.tick() => {
                        if let Some(timer) = branch_tracker.try_begin("rescue_expired_callbacks", self.callback_rescue_interval, &self.metrics) {
                            self.rescue_expired_callbacks().await;
                            timer.finish();
                        }
                    }
                    _ = promote_timer.tick() => {
                        if let Some(timer) = branch_tracker.try_begin("promote_scheduled", self.promote_interval, &self.metrics) {
                            self.promote_scheduled().await;
                            timer.finish();
                        }
                    }
                    _ = cleanup_timer.tick() => {
                        if let Some(timer) = branch_tracker.try_begin("cleanup", self.cleanup_interval, &self.metrics) {
                            self.cleanup_completed().await;
                            self.cleanup_dlq_rows().await;
                            self.cleanup_batch_operations().await;
                            self.cleanup_stale_runtime_snapshots().await;
                            self.cleanup_stale_descriptors().await;
                            timer.finish();
                        }
                    }
                    _ = cron_sync_timer.tick() => {
                        if let Some(timer) = branch_tracker.try_begin("cron_sync", self.cron_sync_interval, &self.metrics) {
                            self.sync_periodic_jobs_to_db().await;
                            timer.finish();
                        }
                    }
                    _ = queue_stats_timer.tick() => {
                        if let Some(timer) = branch_tracker.try_begin("queue_stats", self.queue_stats_interval, &self.metrics) {
                            self.publish_queue_health_metrics().await;
                            timer.finish();
                        }
                    }
                    _ = dirty_key_timer.tick() => {
                        if let Some(timer) = branch_tracker.try_begin("recompute_dirty_admin_metadata", self.dirty_key_recompute_interval, &self.metrics) {
                            self.recompute_dirty_admin_metadata().await;
                            timer.finish();
                        }
                    }
                    _ = metadata_reconciliation_timer.tick() => {
                        if let Some(timer) = branch_tracker.try_begin("refresh_admin_metadata", self.metadata_reconciliation_interval, &self.metrics) {
                            self.refresh_admin_metadata().await;
                            timer.finish();
                        }
                    }
                    _ = priority_aging_timer.tick() => {
                        if let Some(timer) = branch_tracker.try_begin("priority_aging", self.priority_aging_interval, &self.metrics) {
                            self.age_waiting_priorities().await;
                            timer.finish();
                        }
                    }
                    _ = batch_operations_timer.tick() => {
                        if let Some(timer) = branch_tracker.try_begin("batch_operations", self.batch_operations_interval, &self.metrics) {
                            self.process_batch_operation().await;
                            timer.finish();
                        }
                    }
                    _ = terminal_count_rollup_timer.tick() => {
                        if let Some(timer) = branch_tracker.try_begin_without_cooldown("terminal_count_rollup", self.terminal_count_rollup_interval, &self.metrics) {
                            self.rollup_terminal_count_deltas().await;
                            timer.finish();
                        }
                    }
                    _ = async {
                        if let Some(timer) = &mut vacuum_queue_timer {
                            timer.tick().await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    }, if vacuum_queue_timer.is_some() => {
                        let interval = vacuum_queue_interval
                            .expect("vacuum_queue_interval Some iff vacuum_queue_timer Some");
                        if let Some(timer) = branch_tracker.try_begin("rotate_queue", interval, &self.metrics) {
                            self.rotate_queue_storage_queue(&prune_tracker).await;
                            timer.finish();
                        }
                    }
                    _ = async {
                        if let Some(timer) = &mut vacuum_lease_timer {
                            timer.tick().await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    }, if vacuum_lease_timer.is_some() => {
                        let interval = vacuum_lease_interval
                            .expect("vacuum_lease_interval Some iff vacuum_lease_timer Some");
                        if let Some(timer) = branch_tracker.try_begin("rotate_lease", interval, &self.metrics) {
                            self.rotate_queue_storage_leases(&prune_tracker).await;
                            timer.finish();
                        }
                    }
                    _ = async {
                        if let Some(timer) = &mut vacuum_claim_timer {
                            timer.tick().await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    }, if vacuum_claim_timer.is_some() => {
                        let interval = vacuum_claim_interval
                            .expect("vacuum_claim_interval Some iff vacuum_claim_timer Some");
                        if let Some(timer) = branch_tracker.try_begin("rotate_claim", interval, &self.metrics) {
                            self.rotate_queue_storage_claims(&prune_tracker).await;
                            timer.finish();
                        }
                    }
                    _ = leader_check_timer.tick() => {
                        // Verify leader connection is still alive.
                        // The advisory lock is session-scoped: if the connection is alive,
                        // the lock is held. If the query fails, the connection (and lock) are gone.
                        if sqlx::query("SELECT 1").execute(&mut *leader_conn).await.is_err() {
                            warn!("Leader connection lost, re-entering election loop");
                            self.leader.store(false, Ordering::SeqCst);
                            Self::stop_cron_evaluator(&cron_eval_cancel, &cron_eval_task);
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

    async fn run_cron_evaluator(pool: PgPool, cancel: CancellationToken, interval: Duration) {
        let mut timer = tokio::time::interval(interval);
        timer.tick().await;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = timer.tick() => {
                    Self::evaluate_cron_schedules(&pool).await;
                }
            }
        }
    }

    fn stop_cron_evaluator(cancel: &CancellationToken, task: &JoinHandle<()>) {
        cancel.cancel();
        task.abort();
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

    async fn process_batch_operation(&self) {
        let runner_instance = Uuid::new_v4();
        match awa_model::batch_operations::run_one_default_chunk(&self.pool, runner_instance).await
        {
            Ok(outcome) if outcome.claimed => {
                debug!(
                    processed = outcome.processed,
                    skipped = outcome.skipped,
                    errored = outcome.errored,
                    finalized = outcome.finalized,
                    "processed batch operation chunk"
                );
            }
            Ok(_) => {}
            Err(err) => warn!(error = %err, "failed to process batch operation chunk"),
        }
    }

    async fn cleanup_batch_operations(&self) {
        match awa_model::batch_operations::cleanup_expired_batch_operations(&self.pool, 1000).await
        {
            Ok(deleted) if deleted > 0 => {
                debug!(deleted, "cleaned up expired batch operations");
            }
            Ok(_) => {}
            Err(err) => warn!(error = %err, "failed to clean up expired batch operations"),
        }
    }

    async fn rollup_terminal_count_deltas(&self) {
        let Some(runtime) = self.storage.queue_storage() else {
            return;
        };

        match runtime
            .store
            .rollup_terminal_count_deltas(&self.pool, TERMINAL_COUNT_ROLLUP_MAX_SLOTS_PER_TICK)
            .await
        {
            Ok(TerminalDeltaRollupOutcome {
                rolled_slots: 0,
                delta_rows: 0,
                grouped_keys: 0,
                skipped_active_slots: 0,
                blocked_slots: 0,
            }) => {}
            Ok(outcome) => {
                debug!(
                    rolled_slots = outcome.rolled_slots,
                    delta_rows = outcome.delta_rows,
                    grouped_keys = outcome.grouped_keys,
                    skipped_active_slots = outcome.skipped_active_slots,
                    blocked_slots = outcome.blocked_slots,
                    "rolled up queue-storage terminal count deltas"
                );
            }
            Err(err) => warn!(error = %err, "failed to roll up terminal count deltas"),
        }
    }

    /// Evaluate all cron schedules and enqueue any that are due.
    ///
    /// For each schedule, computes due fire times ≤ now that are after
    /// `last_enqueued_at`. If fires are due, executes the atomic CTE for each
    /// fire in order so delayed evaluation catches up instead of collapsing
    /// intermediate fires.
    #[tracing::instrument(skip(pool), name = "maintenance.cron_eval")]
    async fn evaluate_cron_schedules(pool: &PgPool) {
        let cron_rows = match list_cron_jobs(pool).await {
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
            if row.is_paused() {
                debug!(cron_name = %row.name, "Skipping paused cron schedule");
                continue;
            }
            let fire_times = compute_fire_times(row, now, CRON_CATCH_UP_LIMIT);
            if fire_times.is_empty() {
                continue;
            }
            if fire_times.len() == CRON_CATCH_UP_LIMIT {
                warn!(
                    cron_name = %row.name,
                    catch_up_limit = CRON_CATCH_UP_LIMIT,
                    "Cron catch-up limit reached; remaining due fires will be retried on the next evaluation"
                );
            }

            let mut previous_enqueued_at = row.last_enqueued_at;
            for fire_time in fire_times {
                match atomic_enqueue(pool, &row.name, fire_time, previous_enqueued_at).await {
                    Ok(Some(job)) => {
                        previous_enqueued_at = Some(fire_time);
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
                        break;
                    }
                    Err(err) => {
                        error!(
                            cron_name = %row.name,
                            error = %err,
                            "Failed to enqueue periodic job"
                        );
                        break;
                    }
                }
            }
        }
    }

    /// Rescue jobs with stale heartbeats (crash detection).
    #[tracing::instrument(skip(self), name = "maintenance.rescue_stale")]
    async fn rescue_stale_heartbeats(&self) {
        let outcome = match &self.storage {
            RuntimeStorage::Canonical => {
                let staleness_ms = self.heartbeat_staleness.as_millis() as i64;
                sqlx::query_as::<_, JobRow>(
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
                .map_err(awa_model::AwaError::Database)
            }
            RuntimeStorage::QueueStorage(runtime) => {
                runtime
                    .store
                    .rescue_stale_heartbeats(&self.pool, self.heartbeat_staleness)
                    .await
            }
        };
        match outcome {
            Ok(rescued) if !rescued.is_empty() => {
                self.metrics.maintenance_rescues.add(
                    rescued.len() as u64,
                    &[opentelemetry::KeyValue::new("awa.rescue.kind", "heartbeat")],
                );
                warn!(count = rescued.len(), "Rescued stale heartbeat jobs");
                // Signal cancellation to any rescued jobs still running on this instance
                self.signal_cancellation(&rescued).await;
                for job in &rescued {
                    self.emit_rescued(job, crate::events::RescueReason::StaleHeartbeat)
                        .await;
                }
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
        let outcome = match &self.storage {
            RuntimeStorage::Canonical => sqlx::query_as::<_, JobRow>(
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
            .map_err(awa_model::AwaError::Database),
            RuntimeStorage::QueueStorage(runtime) => {
                runtime.store.rescue_expired_deadlines(&self.pool).await
            }
        };
        match outcome {
            Ok(rescued) if !rescued.is_empty() => {
                self.metrics.maintenance_rescues.add(
                    rescued.len() as u64,
                    &[opentelemetry::KeyValue::new("awa.rescue.kind", "deadline")],
                );
                warn!(count = rescued.len(), "Rescued deadline-expired jobs");
                // Signal cancellation so handlers see ctx.is_cancelled() == true
                self.signal_cancellation(&rescued).await;
                for job in &rescued {
                    self.emit_rescued(job, crate::events::RescueReason::DeadlineExceeded)
                        .await;
                }
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
        let outcome = match &self.storage {
            RuntimeStorage::Canonical => sqlx::query_as::<_, JobRow>(
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
            .map_err(awa_model::AwaError::Database),
            RuntimeStorage::QueueStorage(runtime) => {
                runtime.store.rescue_expired_callbacks(&self.pool).await
            }
        };
        match outcome {
            Ok(rescued) if !rescued.is_empty() => {
                self.metrics.maintenance_rescues.add(
                    rescued.len() as u64,
                    &[opentelemetry::KeyValue::new(
                        "awa.rescue.kind",
                        "callback_timeout",
                    )],
                );
                warn!(count = rescued.len(), "Rescued callback-timed-out jobs");
                for job in &rescued {
                    self.emit_rescued(job, crate::events::RescueReason::ExpiredCallback)
                        .await;
                }
                if let RuntimeStorage::QueueStorage(runtime) = &self.storage {
                    for job in &rescued {
                        if job.state != JobState::Failed || !self.dlq_policy.enabled_for(&job.queue)
                        {
                            continue;
                        }
                        match runtime
                            .store
                            .move_failed_to_dlq(&self.pool, job.id, "callback_timeout")
                            .await
                        {
                            Ok(Some(_)) => {
                                self.metrics.record_dlq_moved(
                                    &job.kind,
                                    &job.queue,
                                    "callback_timeout",
                                );
                            }
                            Ok(None) => {}
                            Err(err) => {
                                error!(
                                    job_id = job.id,
                                    error = %err,
                                    "Failed to move rescued callback timeout into DLQ"
                                );
                            }
                        }
                    }
                }
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
        let aging_secs = self.priority_aging_interval.as_secs_f64();
        if aging_secs <= 0.0 {
            return;
        }
        if let Some(runtime) = self.storage.queue_storage() {
            debug!(
                schema = %runtime.store.schema(),
                "Queue storage uses claim-time priority aging; skipping physical reprioritization pass"
            );
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

    /// Emit a Rescued notification: dispatch the durable follow-up specs
    /// (best-effort, separate tx — see [`Self::dispatch_rescued_followups`])
    /// then fire the in-process hook detached so a slow observer does not
    /// gate the side-effect path or any work the caller does after this
    /// (e.g. the callback-rescue DLQ move in
    /// [`Self::rescue_expired_callbacks`]).
    async fn emit_rescued(&self, job: &JobRow, reason: crate::events::RescueReason) {
        self.dispatch_rescued_followups(job, reason).await;
        let handlers = self.lifecycle_handlers.clone();
        let kind = job.kind.clone();
        let event = crate::events::UntypedJobEvent::Rescued {
            job: job.clone(),
            reason,
        };
        tokio::spawn(async move {
            crate::executor::dispatch_lifecycle_event(&handlers, &kind, event).await;
        });
    }

    /// ADR-029 best-effort dispatch of `Rescued` follow-up specs.
    ///
    /// Like callback-resolution follow-ups, this runs in a *separate*
    /// transaction from the rescue UPDATE — the rescue UPDATE has already
    /// committed by the time we're here, so we can't be atomic without
    /// teaching every rescue path to take a `&mut tx`. If a spec INSERT
    /// fails it's logged; the rescue itself remains valid.
    async fn dispatch_rescued_followups(&self, job: &JobRow, reason: crate::events::RescueReason) {
        let Some(specs) = self
            .enqueue_specs
            .get(&crate::enqueue_specs::Outcome::Rescued)
            .and_then(|by_kind| by_kind.get(&job.kind))
            .cloned()
        else {
            return;
        };
        if specs.is_empty() {
            return;
        }
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(err) => {
                error!(
                    job_id = job.id,
                    kind = %job.kind,
                    rescue_reason = reason.as_str(),
                    error = %err,
                    "Rescued follow-up dispatch: failed to begin transaction"
                );
                return;
            }
        };
        let outcome_ctx = crate::enqueue_specs::OutcomeContext::Rescued { reason };
        let result =
            crate::enqueue_specs::dispatch_specs_in_tx(&mut tx, job, &specs, Some(&outcome_ctx))
                .await;
        match result {
            Ok(()) => {
                if let Err(err) = tx.commit().await {
                    error!(
                        job_id = job.id,
                        kind = %job.kind,
                        rescue_reason = reason.as_str(),
                        error = %err,
                        "Rescued follow-up dispatch: commit failed"
                    );
                }
            }
            Err(err) => {
                error!(
                    job_id = job.id,
                    kind = %job.kind,
                    rescue_reason = reason.as_str(),
                    error = %err,
                    "Rescued follow-up dispatch: spec INSERT failed; rolling back"
                );
                let _ = tx.rollback().await;
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
    ) -> Result<(), awa_model::AwaError> {
        let mut promoted_total = 0usize;
        let mut notified_queues = HashSet::new();

        for _ in 0..PROMOTE_MAX_BATCHES_PER_TICK {
            if self.cancel.is_cancelled() {
                break;
            }

            match &self.storage {
                RuntimeStorage::Canonical => {
                    let (promoted, queues) = self
                        .promote_due_batch(state)
                        .await
                        .map_err(awa_model::AwaError::Database)?;
                    if promoted == 0 {
                        break;
                    }

                    promoted_total += promoted;
                    notified_queues.extend(queues);

                    if promoted < PROMOTE_BATCH_SIZE as usize {
                        break;
                    }
                }
                RuntimeStorage::QueueStorage(runtime) => {
                    let job_state = match state {
                        "scheduled" => awa_model::JobState::Scheduled,
                        "retryable" => awa_model::JobState::Retryable,
                        other => {
                            return Err(awa_model::AwaError::Validation(format!(
                                "unsupported queue storage promote state: {other}"
                            )));
                        }
                    };
                    let promote_start = std::time::Instant::now();
                    let promoted = runtime
                        .store
                        .promote_due(&self.pool, job_state, PROMOTE_BATCH_SIZE)
                        .await?;
                    self.metrics.record_promotion_batch(
                        state,
                        promoted as u64,
                        promote_start.elapsed(),
                    );
                    if promoted == 0 {
                        break;
                    }

                    promoted_total += promoted;

                    if promoted < PROMOTE_BATCH_SIZE as usize {
                        break;
                    }
                }
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

    async fn rotate_queue_storage_queue(&self, prune_tracker: &PruneBackoffTracker) {
        let Some(runtime) = self.storage.queue_storage() else {
            return;
        };

        match runtime.store.rotate(&self.pool).await {
            Ok(outcome) => {
                self.metrics.record_rotate_outcome("queue", &outcome);
                match outcome {
                    RotateOutcome::Rotated { slot, generation } => {
                        debug!(slot, generation, "Rotated queue storage queue segment");
                    }
                    RotateOutcome::SkippedBusy { slot, busy } => {
                        debug!(
                            slot,
                            ready_rows = busy.queue_ready,
                            done_rows = busy.queue_done,
                            "Skipped busy queue storage queue segment",
                        );
                    }
                }
            }
            Err(err) => {
                error!(error = %err, "Failed to rotate queue storage queue segments");
                return;
            }
        }

        if prune_tracker.should_skip(PRUNE_BRANCH_QUEUE) {
            debug!(branch = PRUNE_BRANCH_QUEUE, "Prune backed off this tick");
            return;
        }

        match runtime.store.prune_oldest(&self.pool).await {
            Ok(outcome) => {
                self.metrics.record_prune_outcome("queue", &outcome);
                prune_tracker.record_outcome(PRUNE_BRANCH_QUEUE, &outcome);
                match outcome {
                    PruneOutcome::Noop => {}
                    PruneOutcome::Pruned { slot } => {
                        debug!(slot, "Pruned queue storage queue segment");
                    }
                    PruneOutcome::Blocked { slot } => {
                        debug!(slot, "Queue storage queue segment prune blocked");
                    }
                    PruneOutcome::SkippedActive {
                        slot,
                        reason,
                        count,
                    } => {
                        debug!(
                            slot,
                            reason = reason.as_str(),
                            count,
                            "Queue storage queue segment still active",
                        );
                    }
                }
            }
            Err(err) => {
                error!(error = %err, "Failed to prune queue storage queue segments");
            }
        }
    }

    async fn rotate_queue_storage_leases(&self, prune_tracker: &PruneBackoffTracker) {
        let Some(runtime) = self.storage.queue_storage() else {
            return;
        };

        match runtime.store.rotate_leases(&self.pool).await {
            Ok(outcome) => {
                self.metrics.record_rotate_outcome("lease", &outcome);
                match outcome {
                    RotateOutcome::Rotated { slot, generation } => {
                        debug!(slot, generation, "Rotated queue storage lease segment");
                    }
                    RotateOutcome::SkippedBusy { slot, busy } => {
                        debug!(
                            slot,
                            lease_rows = busy.leases,
                            "Skipped busy queue storage lease segment",
                        );
                    }
                }
            }
            Err(err) => {
                error!(error = %err, "Failed to rotate queue storage lease segments");
                return;
            }
        }

        if prune_tracker.should_skip(PRUNE_BRANCH_LEASE) {
            debug!(branch = PRUNE_BRANCH_LEASE, "Prune backed off this tick");
            return;
        }

        match runtime.store.prune_oldest_leases(&self.pool).await {
            Ok(outcome) => {
                self.metrics.record_prune_outcome("lease", &outcome);
                prune_tracker.record_outcome(PRUNE_BRANCH_LEASE, &outcome);
                match outcome {
                    PruneOutcome::Noop => {}
                    PruneOutcome::Pruned { slot } => {
                        debug!(slot, "Pruned queue storage lease segment");
                    }
                    PruneOutcome::Blocked { slot } => {
                        debug!(slot, "Queue storage lease segment prune blocked");
                    }
                    PruneOutcome::SkippedActive {
                        slot,
                        reason,
                        count,
                    } => {
                        debug!(
                            slot,
                            reason = reason.as_str(),
                            count,
                            "Queue storage lease segment still active",
                        );
                    }
                }
            }
            Err(err) => {
                error!(error = %err, "Failed to prune queue storage lease segments");
            }
        }
    }

    /// Claim-ring maintenance tick (see ADR-023). Rotates the claim-ring
    /// cursor and prunes the oldest fully-closed partition, mirroring the
    /// lease-ring rotate/prune pair above.
    async fn rotate_queue_storage_claims(&self, prune_tracker: &PruneBackoffTracker) {
        let Some(runtime) = self.storage.queue_storage() else {
            return;
        };

        match runtime.store.rotate_claims(&self.pool).await {
            Ok(outcome) => {
                self.metrics.record_rotate_outcome("claim", &outcome);
                match outcome {
                    RotateOutcome::Rotated { slot, generation } => {
                        debug!(slot, generation, "Rotated queue storage claim segment");
                    }
                    RotateOutcome::SkippedBusy { slot, busy } => {
                        debug!(
                            slot,
                            claim_rows = busy.claims,
                            closure_rows = busy.closures,
                            "Skipped busy queue storage claim segment",
                        );
                    }
                }
            }
            Err(err) => {
                error!(error = %err, "Failed to rotate queue storage claim segments");
                return;
            }
        }

        if prune_tracker.should_skip(PRUNE_BRANCH_CLAIM) {
            debug!(branch = PRUNE_BRANCH_CLAIM, "Prune backed off this tick");
            return;
        }

        match runtime.store.prune_oldest_claims(&self.pool).await {
            Ok(outcome) => {
                self.metrics.record_prune_outcome("claim", &outcome);
                prune_tracker.record_outcome(PRUNE_BRANCH_CLAIM, &outcome);
                match outcome {
                    PruneOutcome::Noop => {}
                    PruneOutcome::Pruned { slot } => {
                        debug!(slot, "Pruned queue storage claim segment");
                    }
                    PruneOutcome::Blocked { slot } => {
                        debug!(slot, "Queue storage claim segment prune blocked");
                    }
                    PruneOutcome::SkippedActive {
                        slot,
                        reason,
                        count,
                    } => {
                        debug!(
                            slot,
                            reason = reason.as_str(),
                            count,
                            "Queue storage claim segment still active",
                        );
                    }
                }
            }
            Err(err) => {
                error!(error = %err, "Failed to prune queue storage claim segments");
            }
        }
    }

    /// Clean up completed/failed/cancelled jobs past retention.
    ///
    /// Targets `jobs_hot` directly (bypassing the `awa.jobs` INSTEAD OF trigger)
    /// since terminal-state jobs always reside in `jobs_hot`.
    /// Runs a global pass for queues without overrides, then per-queue passes
    /// for queues with custom retention.
    #[tracing::instrument(skip(self), name = "maintenance.cleanup")]
    async fn cleanup_completed(&self) {
        if matches!(self.storage, RuntimeStorage::QueueStorage(_)) {
            // Queue storage uses rotation/prune rather than row-by-row cleanup.
            return;
        }

        let mut total_deleted: u64 = 0;

        // Collect override queue names for the exclusion clause
        let override_queues: Vec<String> = self.queue_retention_overrides.keys().cloned().collect();

        // Global pass: delete jobs in queues that do NOT have overrides
        let completed_retention_secs =
            i64::try_from(self.completed_retention.as_secs()).unwrap_or(i64::MAX);
        let failed_retention_secs =
            i64::try_from(self.failed_retention.as_secs()).unwrap_or(i64::MAX);

        let global_result = if override_queues.is_empty() {
            sqlx::query(
                r#"
                DELETE FROM awa.jobs_hot
                WHERE id IN (
                    SELECT id FROM awa.jobs_hot
                    WHERE (state = 'completed' AND finalized_at < now() - make_interval(secs => $1::bigint))
                       OR (state IN ('failed', 'cancelled') AND finalized_at < now() - make_interval(secs => $2::bigint))
                    LIMIT $3
                )
                "#,
            )
            .bind(completed_retention_secs)
            .bind(failed_retention_secs)
            .bind(self.cleanup_batch_size)
            .execute(&self.pool)
            .await
        } else {
            sqlx::query(
                r#"
                DELETE FROM awa.jobs_hot
                WHERE id IN (
                    SELECT id FROM awa.jobs_hot
                    WHERE ((state = 'completed' AND finalized_at < now() - make_interval(secs => $1::bigint))
                       OR (state IN ('failed', 'cancelled') AND finalized_at < now() - make_interval(secs => $2::bigint)))
                      AND queue != ALL($4::text[])
                    LIMIT $3
                )
                "#,
            )
            .bind(completed_retention_secs)
            .bind(failed_retention_secs)
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
            let queue_completed_secs =
                i64::try_from(policy.completed.as_secs()).unwrap_or(i64::MAX);
            let queue_failed_secs = i64::try_from(policy.failed.as_secs()).unwrap_or(i64::MAX);

            match sqlx::query(
                r#"
                DELETE FROM awa.jobs_hot
                WHERE id IN (
                    SELECT id FROM awa.jobs_hot
                    WHERE queue = $4
                      AND ((state = 'completed' AND finalized_at < now() - make_interval(secs => $1::bigint))
                        OR (state IN ('failed', 'cancelled') AND finalized_at < now() - make_interval(secs => $2::bigint)))
                    LIMIT $3
                )
                "#,
            )
            .bind(queue_completed_secs)
            .bind(queue_failed_secs)
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

    #[tracing::instrument(skip(self), name = "maintenance.cleanup_dlq")]
    async fn cleanup_dlq_rows(&self) {
        let RuntimeStorage::QueueStorage(runtime) = &self.storage else {
            return;
        };

        let schema = runtime.store.schema();
        let override_queues: Vec<&str> = self
            .queue_retention_overrides
            .iter()
            .filter(|(_, policy)| policy.dlq.is_some())
            .map(|(queue, _)| queue.as_str())
            .collect();
        let retention_secs = i64::try_from(self.dlq_retention.as_secs()).unwrap_or(i64::MAX);

        let global_result = if override_queues.is_empty() {
            sqlx::query(&format!(
                r#"
                DELETE FROM {schema}.dlq_entries
                WHERE job_id IN (
                    SELECT job_id FROM {schema}.dlq_entries
                    WHERE dlq_at < now() - make_interval(secs => $1::bigint)
                    LIMIT $2
                )
                "#
            ))
            .bind(retention_secs)
            .bind(self.dlq_cleanup_batch_size)
            .execute(&self.pool)
            .await
        } else {
            sqlx::query(&format!(
                r#"
                DELETE FROM {schema}.dlq_entries
                WHERE job_id IN (
                    SELECT job_id FROM {schema}.dlq_entries
                    WHERE dlq_at < now() - make_interval(secs => $1::bigint)
                      AND queue != ALL($3::text[])
                    LIMIT $2
                )
                "#
            ))
            .bind(retention_secs)
            .bind(self.dlq_cleanup_batch_size)
            .bind(&override_queues)
            .execute(&self.pool)
            .await
        };

        match global_result {
            Ok(result) if result.rows_affected() > 0 => {
                self.metrics.record_dlq_purged(None, result.rows_affected());
            }
            Err(err) => {
                error!(error = %err, "Failed to clean up DLQ rows (global pass)");
            }
            _ => {}
        }

        for (queue, policy) in &self.queue_retention_overrides {
            let Some(retention) = policy.dlq else {
                continue;
            };
            let retention_secs = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
            match sqlx::query(&format!(
                r#"
                DELETE FROM {schema}.dlq_entries
                WHERE job_id IN (
                    SELECT job_id FROM {schema}.dlq_entries
                    WHERE queue = $3
                      AND dlq_at < now() - make_interval(secs => $1::bigint)
                    LIMIT $2
                )
                "#
            ))
            .bind(retention_secs)
            .bind(self.dlq_cleanup_batch_size)
            .bind(queue)
            .execute(&self.pool)
            .await
            {
                Ok(result) if result.rows_affected() > 0 => {
                    self.metrics
                        .record_dlq_purged(Some(queue), result.rows_affected());
                }
                Err(err) => {
                    error!(queue, error = %err, "Failed to clean up DLQ rows");
                }
                _ => {}
            }
        }
    }
}

struct MaintenanceAliveGuard(Arc<AtomicBool>);

impl Drop for MaintenanceAliveGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Compute due fire times for a cron job row, using its expression and timezone.
///
/// Existing schedules can catch up missed fires up to `limit` when configured.
/// First registration always enqueues only the latest due fire to avoid
/// backfilling before the schedule was known to AWA.
fn compute_fire_times(
    row: &CronJobRow,
    now: chrono::DateTime<Utc>,
    limit: usize,
) -> Vec<chrono::DateTime<Utc>> {
    let cron = match Cron::new(&row.cron_expr).with_seconds_optional().parse() {
        Ok(c) => c,
        Err(err) => {
            error!(cron_name = %row.name, error = %err, "Invalid cron expression in database");
            return Vec::new();
        }
    };

    let tz: chrono_tz::Tz = match row.timezone.parse() {
        Ok(tz) => tz,
        Err(err) => {
            error!(cron_name = %row.name, error = %err, "Invalid timezone in database");
            return Vec::new();
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

    let missed_fire_policy = match CronMissedFirePolicy::parse(&row.missed_fire_policy) {
        Ok(policy) => policy,
        Err(err) => {
            error!(cron_name = %row.name, error = %err, "Invalid cron missed-fire policy in database");
            return Vec::new();
        }
    };
    let should_catch_up =
        row.last_enqueued_at.is_some() && missed_fire_policy == CronMissedFirePolicy::CatchUp;

    if !should_catch_up {
        return latest_due_fire(&cron, tz, search_start, row.last_enqueued_at, now)
            .into_iter()
            .collect();
    }

    let mut fire_times = Vec::new();
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

        fire_times.push(fire_utc);
        if fire_times.len() >= limit {
            break;
        }
    }

    fire_times
}

fn latest_due_fire(
    cron: &Cron,
    tz: chrono_tz::Tz,
    search_start: chrono::DateTime<chrono_tz::Tz>,
    last_enqueued_at: Option<chrono::DateTime<Utc>>,
    now: chrono::DateTime<Utc>,
) -> Option<chrono::DateTime<Utc>> {
    let first_due = first_due_fire(cron, search_start, last_enqueued_at, now)?;
    let total_span_seconds = now.signed_duration_since(first_due).num_seconds().max(1);
    let mut lookback_seconds = 1_i64;

    loop {
        // Croner has no previous-occurrence iterator. Search backward from
        // `now` with an exponentially growing window, then scan only that
        // small window to preserve coalesced/latest-only semantics.
        let window_start_utc = (now - chrono::Duration::seconds(lookback_seconds)).max(first_due);
        let window_start = window_start_utc.with_timezone(&tz);
        let next_in_window = cron
            .iter_from(window_start)
            .next()
            .map(|fire_time| fire_time.with_timezone(&Utc));

        if next_in_window.is_some_and(|fire_utc| {
            fire_utc <= now && last_enqueued_at.is_none_or(|last| fire_utc > last)
        }) {
            return latest_due_fire_in_window(cron, window_start, last_enqueued_at, now)
                .or(Some(first_due));
        }

        if lookback_seconds >= total_span_seconds {
            return Some(first_due);
        }

        lookback_seconds = lookback_seconds.saturating_mul(2).min(total_span_seconds);
    }
}

fn first_due_fire(
    cron: &Cron,
    search_start: chrono::DateTime<chrono_tz::Tz>,
    last_enqueued_at: Option<chrono::DateTime<Utc>>,
    now: chrono::DateTime<Utc>,
) -> Option<chrono::DateTime<Utc>> {
    for fire_time in cron.iter_from(search_start) {
        let fire_utc = fire_time.with_timezone(&Utc);
        if fire_utc > now {
            return None;
        }
        if last_enqueued_at.is_none_or(|last| fire_utc > last) {
            return Some(fire_utc);
        }
    }

    None
}

fn latest_due_fire_in_window(
    cron: &Cron,
    window_start: chrono::DateTime<chrono_tz::Tz>,
    last_enqueued_at: Option<chrono::DateTime<Utc>>,
    now: chrono::DateTime<Utc>,
) -> Option<chrono::DateTime<Utc>> {
    let mut latest_fire = None;

    for fire_time in cron.iter_from(window_start) {
        let fire_utc = fire_time.with_timezone(&Utc);
        if fire_utc > now {
            break;
        }
        if last_enqueued_at.is_none_or(|last| fire_utc > last) {
            latest_fire = Some(fire_utc);
        }
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
        if self.storage.queue_storage().is_some() {
            return;
        }
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
        if self.storage.queue_storage().is_some() {
            return;
        }
        if let Err(err) = awa_model::admin::refresh_admin_metadata(&self.pool).await {
            tracing::warn!(error = %err, "Failed to refresh admin metadata");
        }
    }

    /// Publish queue depth and lag as OTel gauge metrics.
    #[tracing::instrument(skip(self), name = "maintenance.queue_stats")]
    async fn publish_queue_health_metrics(&self) {
        if let RuntimeStorage::QueueStorage(runtime) = &self.storage {
            self.publish_queue_storage_health_metrics(runtime).await;
            return;
        }

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
    }

    async fn publish_queue_storage_health_metrics(
        &self,
        runtime: &crate::storage::QueueStorageRuntime,
    ) {
        let schema = runtime.store.schema();
        // This is a high-cadence metrics path, not an admin exact-count
        // endpoint. Keep it on queue-storage control tables and bounded
        // lane-head probes so long retained ready/done segments do not
        // compete with worker traffic. Admin surfaces that need exact
        // availability still go through QueueStorage::queue_counts().
        let rows: Vec<QueueStorageMetricRow> = match sqlx::query_as(&format!(
            r#"
            WITH head_signal AS (
                SELECT
                    enqueues.queue,
                    enqueues.priority,
                    enqueues.enqueue_shard,
                    {schema}.sequence_next_value(enqueues.seq_name) AS next_seq,
                    {schema}.sequence_next_value(claims.seq_name) AS claim_seq
                FROM {schema}.queue_enqueue_heads AS enqueues
                JOIN {schema}.queue_claim_heads AS claims
                  ON claims.queue = enqueues.queue
                 AND claims.priority = enqueues.priority
                 AND claims.enqueue_shard = enqueues.enqueue_shard
            ),
            queues AS (
                SELECT DISTINCT queue
                FROM (
                    SELECT queue FROM awa.queue_meta
                    UNION ALL
                    SELECT queue FROM head_signal
                    UNION ALL
                    SELECT queue FROM {schema}.leases
                    UNION ALL
                    SELECT queue FROM {schema}.lease_claims
                    UNION ALL
                    SELECT queue FROM {schema}.deferred_jobs
                    UNION ALL
                    SELECT queue FROM {schema}.queue_terminal_live_counts
                    UNION ALL
                    SELECT queue FROM {schema}.queue_terminal_rollups
                    UNION ALL
                    SELECT queue FROM {schema}.dlq_entries
                ) queues
            ),
            ready AS (
                SELECT
                    head_signal.queue,
                    COALESCE(
                        sum(GREATEST(head_signal.next_seq - head_signal.claim_seq, 0)),
                        0
                    )::bigint AS available
                FROM head_signal
                GROUP BY head_signal.queue
            ),
            lag AS (
                SELECT
                    head_signal.queue,
                    EXTRACT(EPOCH FROM clock_timestamp() - min(next_ready.run_at))::double precision
                        AS lag_seconds
                FROM head_signal
                JOIN LATERAL (
                    SELECT ready.run_at
                    FROM {schema}.ready_entries AS ready
                    WHERE ready.queue = head_signal.queue
                      AND ready.priority = head_signal.priority
                      AND ready.enqueue_shard = head_signal.enqueue_shard
                      AND ready.lane_seq >= head_signal.claim_seq
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {schema}.ready_tombstones AS tomb
                          WHERE tomb.ready_slot = ready.ready_slot
                            AND tomb.ready_generation = ready.ready_generation
                            AND tomb.queue = ready.queue
                            AND tomb.priority = ready.priority
                            AND tomb.enqueue_shard = ready.enqueue_shard
                            AND tomb.lane_seq = ready.lane_seq
                      )
                    ORDER BY ready.lane_seq
                    LIMIT 1
                ) AS next_ready ON TRUE
                GROUP BY head_signal.queue
            ),
            leases AS (
                SELECT
                    queue,
                    count(*) FILTER (WHERE state = 'running')::bigint AS running,
                    count(*) FILTER (WHERE state = 'waiting_external')::bigint
                        AS waiting_external
                FROM {schema}.leases
                GROUP BY queue
            ),
            receipt_claims AS (
                SELECT
                    claims.queue,
                    count(*)::bigint AS running
                FROM {schema}.lease_claims AS claims
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {schema}.lease_claim_closures AS closures
                    WHERE closures.claim_slot = claims.claim_slot
                      AND closures.job_id = claims.job_id
                      AND closures.run_lease = claims.run_lease
                )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.leases AS lease
                      WHERE lease.job_id = claims.job_id
                        AND lease.run_lease = claims.run_lease
                  )
                GROUP BY claims.queue
            ),
            deferred AS (
                SELECT
                    queue,
                    count(*) FILTER (WHERE state = 'scheduled')::bigint AS scheduled,
                    count(*) FILTER (WHERE state = 'retryable')::bigint AS retryable
                FROM {schema}.deferred_jobs
                GROUP BY queue
            ),
            terminal AS (
                SELECT
                    queue,
                    count(*)::bigint AS failed_done
                FROM {schema}.done_entries
                WHERE state = 'failed'
                GROUP BY queue
            ),
            dlq AS (
                SELECT
                    queue,
                    count(*)::bigint AS failed_dlq
                FROM {schema}.dlq_entries
                GROUP BY queue
            )
            SELECT
                queues.queue,
                COALESCE(ready.available, 0)::bigint AS available,
                (
                    COALESCE(leases.running, 0)
                    + COALESCE(receipt_claims.running, 0)
                )::bigint AS running,
                COALESCE(leases.waiting_external, 0)::bigint AS waiting_external,
                COALESCE(deferred.scheduled, 0)::bigint AS scheduled,
                COALESCE(deferred.retryable, 0)::bigint AS retryable,
                COALESCE(terminal.failed_done, 0)::bigint AS failed_done,
                COALESCE(dlq.failed_dlq, 0)::bigint AS failed_dlq,
                lag.lag_seconds
            FROM queues
            LEFT JOIN ready
              ON ready.queue = queues.queue
            LEFT JOIN lag
              ON lag.queue = queues.queue
            LEFT JOIN leases
              ON leases.queue = queues.queue
            LEFT JOIN receipt_claims
              ON receipt_claims.queue = queues.queue
            LEFT JOIN deferred
              ON deferred.queue = queues.queue
            LEFT JOIN terminal
              ON terminal.queue = queues.queue
            LEFT JOIN dlq
              ON dlq.queue = queues.queue
            ORDER BY queues.queue
            "#
        ))
        .fetch_all(&self.pool)
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(error = %err, "Failed to query queue storage stats for metrics");
                return;
            }
        };

        for (
            queue,
            available,
            running,
            waiting_external,
            scheduled,
            retryable,
            failed_done,
            failed_dlq,
            lag_seconds,
        ) in rows
        {
            self.metrics
                .record_queue_depth(&queue, "available", available);
            self.metrics.record_queue_depth(&queue, "running", running);
            self.metrics
                .record_queue_depth(&queue, "failed", failed_done + failed_dlq);
            self.metrics
                .record_queue_depth(&queue, "scheduled", scheduled);
            self.metrics
                .record_queue_depth(&queue, "retryable", retryable);
            self.metrics
                .record_queue_depth(&queue, "waiting_external", waiting_external);
            self.metrics.record_dlq_depth(&queue, failed_dlq);

            if let Some(lag_seconds) = lag_seconds {
                self.metrics.record_queue_lag(&queue, lag_seconds);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use awa_model::{migrations, QueueStorage, QueueStorageConfig};
    use chrono::TimeZone;
    use sqlx::postgres::PgPoolOptions;
    use std::sync::OnceLock;

    fn cron_row(
        cron_expr: &str,
        created_at: chrono::DateTime<Utc>,
        last_enqueued_at: Option<chrono::DateTime<Utc>>,
        missed_fire_policy: CronMissedFirePolicy,
    ) -> CronJobRow {
        CronJobRow {
            name: "test_cron".to_string(),
            cron_expr: cron_expr.to_string(),
            timezone: "UTC".to_string(),
            kind: "test_job".to_string(),
            queue: "default".to_string(),
            args: serde_json::json!({}),
            priority: 2,
            max_attempts: 25,
            tags: Vec::new(),
            metadata: serde_json::json!({}),
            missed_fire_policy: missed_fire_policy.as_str().to_string(),
            last_enqueued_at,
            created_at,
            updated_at: created_at,
            paused_at: None,
            paused_by: None,
        }
    }

    #[test]
    fn compute_fire_times_coalesces_missed_existing_fires_by_default() {
        let last = Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 20).unwrap();
        let row = cron_row(
            "*/5 * * * * *",
            last,
            Some(last),
            CronMissedFirePolicy::Coalesce,
        );

        let fires = compute_fire_times(&row, now, CRON_CATCH_UP_LIMIT);

        assert_eq!(
            fires,
            vec![Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 20).unwrap()]
        );
    }

    #[test]
    fn compute_fire_times_coalesces_to_latest_fire_after_long_outage() {
        let last = Utc.with_ymd_and_hms(2026, 5, 6, 12, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 20).unwrap();
        let row = cron_row(
            "*/1 * * * * *",
            last,
            Some(last),
            CronMissedFirePolicy::Coalesce,
        );

        let fires = compute_fire_times(&row, now, 2);

        assert_eq!(
            fires,
            vec![Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 20).unwrap()]
        );
    }

    #[test]
    fn compute_fire_times_catches_up_when_policy_requests_it() {
        let last = Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 20).unwrap();
        let row = cron_row(
            "*/5 * * * * *",
            last,
            Some(last),
            CronMissedFirePolicy::CatchUp,
        );

        let fires = compute_fire_times(&row, now, CRON_CATCH_UP_LIMIT);

        assert_eq!(
            fires,
            vec![
                Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 5).unwrap(),
                Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 10).unwrap(),
                Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 15).unwrap(),
                Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 20).unwrap(),
            ]
        );
    }

    #[test]
    fn compute_fire_times_limits_catch_up_work() {
        let last = Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 30).unwrap();
        let row = cron_row(
            "*/5 * * * * *",
            last,
            Some(last),
            CronMissedFirePolicy::CatchUp,
        );

        let fires = compute_fire_times(&row, now, 2);

        assert_eq!(
            fires,
            vec![
                Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 5).unwrap(),
                Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 10).unwrap(),
            ]
        );
    }

    // ── MaintenanceBranchTracker (#242) ──────────────────────────────

    fn metrics_for_test() -> crate::metrics::AwaMetrics {
        crate::metrics::AwaMetrics::from_global()
    }

    fn database_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
    }

    fn db_test_mutex() -> &'static tokio::sync::Mutex<()> {
        static MUTEX: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        MUTEX.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    async fn ensure_database_exists(url: &str) {
        let parts = url
            .rsplit_once('/')
            .expect("DATABASE_URL must include a database name");
        let database_name = parts.1.to_string();
        let admin_url = format!("{}/postgres", parts.0);
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .expect("Failed to connect to admin database for maintenance tests");
        let create_sql = format!("CREATE DATABASE {database_name}");
        match sqlx::query(&create_sql).execute(&admin_pool).await {
            Ok(_) => {}
            Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P04") => {}
            Err(err) => panic!("Failed to create maintenance test database {database_name}: {err}"),
        }
    }

    async fn setup_pool(max_connections: u32) -> PgPool {
        let url = database_url();
        ensure_database_exists(&url).await;
        PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&url)
            .await
            .expect("Failed to connect to maintenance test database")
    }

    async fn reset_schema(pool: &PgPool) {
        sqlx::raw_sql("DROP SCHEMA IF EXISTS awa CASCADE")
            .execute(pool)
            .await
            .expect("Failed to drop awa schema");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn queue_storage_metrics_query_uses_bounded_observability_path() {
        let _guard = db_test_mutex().lock().await;
        let pool = setup_pool(4).await;
        reset_schema(&pool).await;
        migrations::run(&pool)
            .await
            .expect("migrations should succeed");

        let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
        store.install(&pool).await.expect("queue storage install");

        let runtime = crate::storage::QueueStorageRuntime::new(
            QueueStorageConfig::default(),
            Duration::from_millis(1000),
            Duration::from_millis(250),
        )
        .expect("queue storage runtime");
        let service = MaintenanceService::new(
            pool,
            metrics_for_test(),
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            CancellationToken::new(),
            Arc::new(Vec::new()),
            InFlightMap::default(),
            RuntimeStorage::QueueStorage(runtime.clone()),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
        );

        service.publish_queue_storage_health_metrics(&runtime).await;
    }

    #[test]
    fn branch_tracker_initial_state_has_no_history() {
        let tracker = MaintenanceBranchTracker::new();
        assert_eq!(tracker.snapshot("promote_scheduled"), None);
    }

    #[test]
    fn branch_tracker_finish_records_last_duration() {
        let tracker = MaintenanceBranchTracker::new();
        let metrics = metrics_for_test();
        let timer = tracker
            .try_begin("promote_scheduled", Duration::from_secs(1), &metrics)
            .expect("first tick should not be skipped");
        // Body would run here in production; for the test we just finish.
        timer.finish();
        let (last_duration, is_delayed) = tracker
            .snapshot("promote_scheduled")
            .expect("snapshot should exist after one finish");
        assert!(last_duration.is_some());
        assert!(
            !is_delayed,
            "first tick has no prior duration → not delayed"
        );
    }

    /// Drive `n` consecutive `try_begin` ticks. When the body would
    /// have run (try_begin returns Some), call `record_finish` with
    /// `body_duration` to simulate the body completing in that time.
    /// When the body is skipped (cooldown returns None), `last_duration`
    /// is intentionally NOT updated — matching production where a
    /// skipped tick doesn't produce a new sample. Returns one bool per
    /// tick: true if the body ran, false if it was skipped.
    ///
    /// This helper does NOT seed `last_duration` itself. Tests that need
    /// an initial sample (e.g., to drive the very first overrun
    /// observation) must call `seed_last_duration` first.
    fn replay_ticks(
        tracker: &MaintenanceBranchTracker,
        branch: &'static str,
        body_duration: Duration,
        tick_interval: Duration,
        n: u32,
    ) -> Vec<bool> {
        let metrics = metrics_for_test();
        let mut ran = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let timer_opt = tracker.try_begin(branch, tick_interval, &metrics);
            let did_run = timer_opt.is_some();
            ran.push(did_run);
            if did_run {
                tracker.record_finish(branch, body_duration);
            }
        }
        ran
    }

    /// Explicitly seed `last_duration` as if a prior body completed in
    /// `dur`. Use to set up the initial state for tests that need
    /// the first `try_begin` to see a sample.
    fn seed_last_duration(tracker: &MaintenanceBranchTracker, branch: &'static str, dur: Duration) {
        tracker
            .branches
            .lock()
            .unwrap()
            .entry(branch)
            .or_default()
            .last_duration = Some(dur);
    }

    #[test]
    fn branch_tracker_single_overrun_does_not_flip() {
        // A single overrun sample (clearly above the upper threshold)
        // shouldn't flip is_delayed — K-consecutive is required.
        let tracker = MaintenanceBranchTracker::new();
        seed_last_duration(&tracker, "cleanup", Duration::from_millis(200));
        replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(200), // upper threshold for 100ms tick is 150
            Duration::from_millis(100),
            1,
        );
        assert!(
            !tracker.snapshot("cleanup").unwrap().1,
            "single overrun must not flip is_delayed (K=3 required)"
        );
    }

    #[test]
    fn branch_tracker_deadband_sample_does_not_advance_counters() {
        // Samples between LOWER (70ms) and UPPER (150ms) of a 100ms
        // tick — e.g., 101ms — must NOT advance either counter. This
        // is the fix the bench post-mortem prescribed: 49ms-vs-51ms
        // flap at the boundary no longer accumulates toward a flip.
        let tracker = MaintenanceBranchTracker::new();
        seed_last_duration(&tracker, "cleanup", Duration::from_millis(101));
        replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(101),
            Duration::from_millis(100),
            5,
        );
        let (cooldown, overrun, ontime) = tracker.cooldown_snapshot("cleanup").expect("snapshot");
        assert_eq!(cooldown, 0, "deadband samples should not arm cooldown");
        assert_eq!(overrun, 0, "deadband sample 101ms must not advance overrun");
        assert_eq!(ontime, 0, "deadband sample 101ms must not advance ontime");
    }

    #[test]
    fn branch_tracker_k_consecutive_overruns_flips_and_arms_cooldown() {
        // After K=3 consecutive samples clearly above UPPER, flip to
        // delayed AND arm cooldown. The third try_begin consumes the
        // K-th sample, crosses the threshold, and returns None (the
        // flip-tick skips the body so we don't immediately do more
        // expensive work).
        let tracker = MaintenanceBranchTracker::new();
        seed_last_duration(&tracker, "cleanup", Duration::from_millis(250));
        let ran = replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(250),
            Duration::from_millis(100),
            OVERRUN_HYSTERESIS_K,
        );
        assert_eq!(ran, vec![true, true, false], "flip-tick skips body");
        let (cooldown, overrun, _) = tracker.cooldown_snapshot("cleanup").expect("snapshot");
        assert!(tracker.snapshot("cleanup").unwrap().1, "flipped to delayed");
        assert_eq!(overrun, OVERRUN_HYSTERESIS_K);
        assert_eq!(
            cooldown, BRANCH_COOLDOWN_TICKS,
            "cooldown armed to BRANCH_COOLDOWN_TICKS at flip"
        );
    }

    #[test]
    fn branch_tracker_without_cooldown_tracks_overrun_but_keeps_running() {
        let tracker = MaintenanceBranchTracker::new();
        let metrics = metrics_for_test();
        seed_last_duration(
            &tracker,
            "terminal_count_rollup",
            Duration::from_millis(250),
        );

        let mut ran = Vec::new();
        for _ in 0..OVERRUN_HYSTERESIS_K {
            let timer = tracker.try_begin_without_cooldown(
                "terminal_count_rollup",
                Duration::from_millis(100),
                &metrics,
            );
            ran.push(timer.is_some());
            if timer.is_some() {
                tracker.record_finish("terminal_count_rollup", Duration::from_millis(250));
            }
        }

        assert_eq!(
            ran,
            vec![true, true, true],
            "no-cooldown branches should keep running on overrun"
        );
        let (cooldown, overrun, _) = tracker
            .cooldown_snapshot("terminal_count_rollup")
            .expect("snapshot");
        assert_eq!(cooldown, 0, "no-cooldown branch must not arm cooldown");
        assert_eq!(overrun, OVERRUN_HYSTERESIS_K);
        assert!(
            tracker.snapshot("terminal_count_rollup").unwrap().1,
            "overrun state should still be observable"
        );
    }

    #[test]
    fn branch_tracker_cooldown_skips_body() {
        // Drive past the flip, then assert subsequent ticks return
        // None until cooldown decrements to zero. After the flip,
        // `last_duration` was consumed by `take()`, so the cooldown
        // gate is the only thing returning None here — exactly the
        // production shape we want.
        let tracker = MaintenanceBranchTracker::new();
        seed_last_duration(&tracker, "cleanup", Duration::from_millis(250));
        replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(250),
            Duration::from_millis(100),
            OVERRUN_HYSTERESIS_K,
        );
        // Subsequent ticks return None until cooldown drains. We pass
        // 50ms as the body_duration but it's unused — `record_finish`
        // is only called when a body actually runs.
        let ran = replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(50),
            Duration::from_millis(100),
            BRANCH_COOLDOWN_TICKS,
        );
        assert!(
            ran.iter().all(|&r| !r),
            "every tick during cooldown must skip body"
        );
        let (cooldown, _, _) = tracker.cooldown_snapshot("cleanup").expect("snapshot");
        assert_eq!(cooldown, 0, "cooldown decrements to zero");
    }

    #[test]
    fn branch_tracker_cooldown_expires_then_body_runs() {
        // After cooldown drains, the first post-cooldown try_begin
        // sees `last_duration = None` (consumed at the flip, no body
        // ran during cooldown), so no hysteresis check fires; the
        // body simply runs. The counters do NOT advance on this
        // first post-cooldown tick — the sample this body produces
        // is evaluated on the *next* try_begin.
        let tracker = MaintenanceBranchTracker::new();
        seed_last_duration(&tracker, "cleanup", Duration::from_millis(250));
        replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(250),
            Duration::from_millis(100),
            OVERRUN_HYSTERESIS_K,
        );
        replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(50),
            Duration::from_millis(100),
            BRANCH_COOLDOWN_TICKS,
        );
        let ran = replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(50),
            Duration::from_millis(100),
            1,
        );
        assert_eq!(ran, vec![true], "post-cooldown body runs");
        let (cooldown, overrun, ontime) = tracker.cooldown_snapshot("cleanup").expect("snapshot");
        assert_eq!(
            cooldown, 0,
            "cooldown stays at zero after a single fast body"
        );
        assert_eq!(
            overrun, OVERRUN_HYSTERESIS_K,
            "consecutive_overrun preserved across cooldown (no eval on this tick)"
        );
        assert_eq!(
            ontime, 0,
            "ontime advances on the next tick — this one had no sample to evaluate"
        );
    }

    #[test]
    fn branch_tracker_cooldown_rearms_on_continued_overrun() {
        // After cooldown drains, the first post-cooldown body runs
        // and is slow. That slow body's sample is evaluated on the
        // *second* post-cooldown try_begin, where it re-arms cooldown
        // because consecutive_overrun is already at K (preserved
        // across the cooldown).
        let tracker = MaintenanceBranchTracker::new();
        seed_last_duration(&tracker, "cleanup", Duration::from_millis(250));
        replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(250),
            Duration::from_millis(100),
            OVERRUN_HYSTERESIS_K,
        );
        replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(50),
            Duration::from_millis(100),
            BRANCH_COOLDOWN_TICKS,
        );

        // Tick 1 post-cooldown: take None, no eval, body runs slow.
        // Tick 2: take Some(250), consecutive_overrun saturates past
        // K, is_delayed && cross → re-arm cooldown.
        let ran = replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(250),
            Duration::from_millis(100),
            2,
        );
        assert_eq!(
            ran,
            vec![true, false],
            "first tick runs body; second tick re-arms cooldown"
        );
        let (cooldown, _, _) = tracker.cooldown_snapshot("cleanup").expect("snapshot");
        assert_eq!(cooldown, BRANCH_COOLDOWN_TICKS, "cooldown re-armed");
        assert!(
            tracker.snapshot("cleanup").unwrap().1,
            "still delayed across re-arm"
        );
    }

    #[test]
    fn branch_tracker_intermittent_overrun_does_not_flip() {
        // Pattern: 3 clearly-overrun samples interleaved with
        // clearly-on-time samples. Non-consecutive overruns never
        // accumulate K=3 because each on-time tick resets the counter.
        let tracker = MaintenanceBranchTracker::new();
        seed_last_duration(&tracker, "cleanup", Duration::from_millis(200));
        for over in [true, false, true, false, true] {
            let dur = if over {
                Duration::from_millis(200)
            } else {
                Duration::from_millis(50)
            };
            replay_ticks(&tracker, "cleanup", dur, Duration::from_millis(100), 1);
        }
        assert!(
            !tracker.snapshot("cleanup").unwrap().1,
            "intermittent overruns must not flip"
        );
    }

    #[test]
    fn branch_tracker_recovers_only_after_k_ontime_ticks_post_cooldown() {
        // After cooldown drains, K consecutive on-time samples must be
        // *evaluated* before is_delayed clears. The very first
        // post-cooldown body has no sample to evaluate (last_duration
        // was consumed at the flip), so K evaluable samples require
        // K+1 post-cooldown ticks: one to seed, K to advance ontime.
        let tracker = MaintenanceBranchTracker::new();
        seed_last_duration(&tracker, "cleanup", Duration::from_millis(250));
        replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(250),
            Duration::from_millis(100),
            OVERRUN_HYSTERESIS_K,
        );
        replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(50),
            Duration::from_millis(100),
            BRANCH_COOLDOWN_TICKS,
        );

        // K post-cooldown ticks: tick 1 seeds, ticks 2..K evaluate K-1
        // on-time samples. Still delayed.
        replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(50),
            Duration::from_millis(100),
            OVERRUN_HYSTERESIS_K,
        );
        assert!(
            tracker.snapshot("cleanup").unwrap().1,
            "still delayed after only K-1 evaluations"
        );

        // (K+1)-th tick evaluates the K-th on-time sample → recovery.
        replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(50),
            Duration::from_millis(100),
            1,
        );
        assert!(
            !tracker.snapshot("cleanup").unwrap().1,
            "recovered after K evaluable on-time samples"
        );
    }

    #[test]
    fn branch_tracker_per_branch_state_is_independent() {
        // Drive "cleanup" past the K=3 overrun threshold while
        // "promote_scheduled" stays on-time the whole time. Branches
        // must not share state.
        let tracker = MaintenanceBranchTracker::new();
        seed_last_duration(&tracker, "cleanup", Duration::from_millis(500));
        replay_ticks(
            &tracker,
            "cleanup",
            Duration::from_millis(500),
            Duration::from_millis(100),
            OVERRUN_HYSTERESIS_K,
        );
        seed_last_duration(&tracker, "promote_scheduled", Duration::from_millis(10));
        replay_ticks(
            &tracker,
            "promote_scheduled",
            Duration::from_millis(10),
            Duration::from_millis(250),
            OVERRUN_HYSTERESIS_K,
        );
        assert!(tracker.snapshot("cleanup").unwrap().1);
        assert!(!tracker.snapshot("promote_scheduled").unwrap().1);
    }

    // ── PruneBackoffTracker (#169) ────────────────────────────────────

    fn skip_active(slot: i32) -> PruneOutcome {
        PruneOutcome::SkippedActive {
            slot,
            reason: SkipReason::LeaseActive,
            count: 1,
        }
    }

    #[test]
    fn prune_backoff_initial_state_does_not_skip() {
        let tracker = PruneBackoffTracker::new();
        assert!(!tracker.should_skip(PRUNE_BRANCH_LEASE));
        assert_eq!(
            tracker.snapshot(PRUNE_BRANCH_LEASE),
            Some((0, 0)),
            "polling once must not introduce backoff"
        );
    }

    #[test]
    fn prune_backoff_skipped_active_doubles_then_resets_on_pruned() {
        let tracker = PruneBackoffTracker::new();

        // First failure: backoff_level=1, skip the next 2 ticks.
        tracker.record_outcome(PRUNE_BRANCH_LEASE, &skip_active(0));
        assert_eq!(tracker.snapshot(PRUNE_BRANCH_LEASE), Some((2, 1)));
        assert!(tracker.should_skip(PRUNE_BRANCH_LEASE));
        assert!(tracker.should_skip(PRUNE_BRANCH_LEASE));
        assert!(!tracker.should_skip(PRUNE_BRANCH_LEASE));

        // Second failure: backoff_level=2, skip the next 4.
        tracker.record_outcome(PRUNE_BRANCH_LEASE, &skip_active(0));
        assert_eq!(tracker.snapshot(PRUNE_BRANCH_LEASE), Some((4, 2)));

        // Recovery: Pruned clears everything.
        tracker.record_outcome(PRUNE_BRANCH_LEASE, &PruneOutcome::Pruned { slot: 0 });
        assert_eq!(tracker.snapshot(PRUNE_BRANCH_LEASE), Some((0, 0)));
        assert!(!tracker.should_skip(PRUNE_BRANCH_LEASE));
    }

    #[test]
    fn prune_backoff_blocked_increases_level_same_as_skipped_active() {
        let tracker = PruneBackoffTracker::new();
        tracker.record_outcome(PRUNE_BRANCH_LEASE, &PruneOutcome::Blocked { slot: 0 });
        assert_eq!(tracker.snapshot(PRUNE_BRANCH_LEASE), Some((2, 1)));
        tracker.record_outcome(PRUNE_BRANCH_LEASE, &PruneOutcome::Blocked { slot: 0 });
        assert_eq!(tracker.snapshot(PRUNE_BRANCH_LEASE), Some((4, 2)));
    }

    #[test]
    fn prune_backoff_noop_is_neutral() {
        let tracker = PruneBackoffTracker::new();
        // Build up some backoff first so we have observable state.
        tracker.record_outcome(PRUNE_BRANCH_LEASE, &skip_active(0));
        let before = tracker.snapshot(PRUNE_BRANCH_LEASE);
        tracker.record_outcome(PRUNE_BRANCH_LEASE, &PruneOutcome::Noop);
        let after = tracker.snapshot(PRUNE_BRANCH_LEASE);
        assert_eq!(
            before, after,
            "Noop must not change backoff state — there was nothing to do, not a failure"
        );
    }

    #[test]
    fn prune_backoff_caps_at_max_level() {
        let tracker = PruneBackoffTracker::new();
        // Drive past the cap to make sure backoff_level saturates.
        for _ in 0..(MAX_PRUNE_BACKOFF_LEVEL as u32 + 5) {
            tracker.record_outcome(PRUNE_BRANCH_LEASE, &skip_active(0));
        }
        let (skip_remaining, backoff_level) =
            tracker.snapshot(PRUNE_BRANCH_LEASE).expect("snapshot");
        assert_eq!(backoff_level, MAX_PRUNE_BACKOFF_LEVEL);
        assert_eq!(skip_remaining, 1u32 << MAX_PRUNE_BACKOFF_LEVEL);
    }

    #[test]
    fn prune_backoff_per_branch_state_is_independent() {
        let tracker = PruneBackoffTracker::new();
        tracker.record_outcome(PRUNE_BRANCH_LEASE, &skip_active(0));
        tracker.record_outcome(PRUNE_BRANCH_LEASE, &skip_active(0));
        // Claim branch is untouched.
        assert_eq!(tracker.snapshot(PRUNE_BRANCH_LEASE), Some((4, 2)));
        assert_eq!(tracker.snapshot(PRUNE_BRANCH_CLAIM), None);
        assert!(!tracker.should_skip(PRUNE_BRANCH_CLAIM));
    }

    #[test]
    fn compute_fire_times_keeps_first_registration_latest_only() {
        let created_at = Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 30).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 55).unwrap();
        let row = cron_row(
            "*/5 * * * * *",
            created_at,
            None,
            CronMissedFirePolicy::CatchUp,
        );

        let fires = compute_fire_times(&row, now, CRON_CATCH_UP_LIMIT);

        assert_eq!(
            fires,
            vec![Utc.with_ymd_and_hms(2026, 5, 7, 12, 0, 55).unwrap()]
        );
    }
}
