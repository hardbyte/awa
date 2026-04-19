use crate::executor::JobExecutor;
use crate::runtime::InFlightMap;
use crate::storage::RuntimeStorage;
use awa_model::JobRow;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const CLAIM_BATCH_LIMIT: usize = 128;

/// Rate limit configuration for a queue.
#[derive(Debug, Clone)]
pub struct RateLimit {
    /// Maximum sustained dispatch rate (jobs per second).
    pub max_rate: f64,
    /// Maximum burst size. Defaults to ceil(max_rate) if 0.
    pub burst: u32,
}

/// Internal token bucket state for rate limiting.
struct TokenBucket {
    tokens: f64,
    max_tokens: f64,
    refill_rate: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(rate_limit: &RateLimit) -> Self {
        let burst = if rate_limit.burst == 0 {
            (rate_limit.max_rate.ceil() as u32).max(1)
        } else {
            rate_limit.burst
        };
        Self {
            tokens: burst as f64,
            max_tokens: burst as f64,
            refill_rate: rate_limit.max_rate,
            last_refill: Instant::now(),
        }
    }

    /// Return how many whole tokens are available after refilling.
    fn available(&mut self) -> u32 {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.max_tokens);
        self.last_refill = now;
        self.tokens.floor() as u32
    }

    /// Consume `n` tokens (caller must ensure n <= available()).
    fn consume(&mut self, n: u32) {
        self.tokens -= n as f64;
    }
}

/// Configuration for a single queue.
#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub max_workers: u32,
    pub poll_interval: Duration,
    pub deadline_duration: Duration,
    pub priority_aging_interval: Duration,
    /// Optional rate limit for this queue. None means unlimited.
    pub rate_limit: Option<RateLimit>,
    /// Minimum guaranteed workers in weighted mode (default: 0).
    pub min_workers: u32,
    /// Weight for overflow allocation in weighted mode (default: 1).
    pub weight: u32,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_workers: 50,
            poll_interval: Duration::from_millis(200),
            deadline_duration: Duration::from_secs(300), // 5 minutes
            priority_aging_interval: Duration::from_secs(60),
            rate_limit: None,
            min_workers: 0,
            weight: 1,
        }
    }
}

/// Wraps permits so the correct resource is released on drop.
/// The OwnedSemaphorePermit fields are held purely for their Drop behavior.
#[allow(dead_code)]
pub(crate) enum DispatchPermit {
    /// Hard-reserved semaphore permit (current default behavior).
    Hard(tokio::sync::OwnedSemaphorePermit),
    /// Local (guaranteed minimum) semaphore permit in weighted mode.
    Local(tokio::sync::OwnedSemaphorePermit),
    /// Overflow permit from the shared OverflowPool.
    Overflow {
        pool: Arc<OverflowPool>,
        queue: String,
    },
}

impl Drop for DispatchPermit {
    fn drop(&mut self) {
        if let DispatchPermit::Overflow { pool, queue } = self {
            pool.release(queue, 1);
        }
        // OwnedSemaphorePermit auto-releases on drop for Hard/Local
    }
}

/// Concurrency mode for a dispatcher.
pub(crate) enum ConcurrencyMode {
    /// Each queue has its own semaphore. No sharing. Default behavior.
    HardReserved { semaphore: Arc<Semaphore> },
    /// Queues share a global overflow pool with per-queue minimum guarantees.
    Weighted {
        local_semaphore: Arc<Semaphore>,
        overflow_pool: Arc<OverflowPool>,
        queue_name: String,
    },
}

/// Centralized overflow capacity allocator for weighted mode.
/// Thread-safe: called from multiple dispatcher poll loops via Mutex.
pub(crate) struct OverflowPool {
    total: u32,
    state: std::sync::Mutex<OverflowState>,
}

struct OverflowState {
    /// Per-queue: currently held overflow permits (decremented on release).
    held: HashMap<String, u32>,
    /// Per-queue: last-declared demand (updated every try_acquire call).
    demand: HashMap<String, u32>,
    /// Per-queue: configured weight (immutable after construction).
    weights: HashMap<String, u32>,
}

impl OverflowPool {
    pub fn new(total: u32, weights: HashMap<String, u32>) -> Self {
        Self {
            total,
            state: std::sync::Mutex::new(OverflowState {
                held: HashMap::new(),
                demand: HashMap::new(),
                weights,
            }),
        }
    }

    /// Try to acquire up to `wanted` overflow permits for `queue`.
    /// Returns the number actually granted (0..=wanted).
    ///
    /// Calling with wanted=0 is valid — it clears this queue's demand signal.
    pub fn try_acquire(&self, queue: &str, wanted: u32) -> u32 {
        let mut state = self.state.lock().unwrap();

        // Always update demand — this is the key signal for fairness
        state.demand.insert(queue.to_string(), wanted);

        if wanted == 0 {
            return 0;
        }

        let currently_used: u32 = state.held.values().sum();
        let available = self.total.saturating_sub(currently_used);
        if available == 0 {
            return 0;
        }

        let my_weight = state.weights.get(queue).copied().unwrap_or(1);

        // Contending = queues with demand > 0 OR held > 0
        let contending_weight: u32 = state
            .weights
            .iter()
            .filter(|(q, _)| {
                state.demand.get(q.as_str()).copied().unwrap_or(0) > 0
                    || state.held.get(q.as_str()).copied().unwrap_or(0) > 0
            })
            .map(|(_, w)| *w)
            .sum();

        if contending_weight == 0 {
            return 0;
        }

        // My fair share of the TOTAL pool (not just available)
        let my_fair_share =
            ((self.total as f64) * (my_weight as f64 / contending_weight as f64)).ceil() as u32;
        let my_held = state.held.get(queue).copied().unwrap_or(0);
        let room = my_fair_share.saturating_sub(my_held);

        let granted = wanted.min(available).min(room);
        if granted > 0 {
            *state.held.entry(queue.to_string()).or_insert(0) += granted;
        }
        granted
    }

    /// Release `n` overflow permits back to the pool.
    pub fn release(&self, queue: &str, n: u32) {
        let mut state = self.state.lock().unwrap();
        if let Some(held) = state.held.get_mut(queue) {
            *held = held.saturating_sub(n);
        }
    }

    /// Get the number of overflow permits currently held by a queue.
    pub fn held(&self, queue: &str) -> u32 {
        let state = self.state.lock().unwrap();
        state.held.get(queue).copied().unwrap_or(0)
    }
}

/// Dispatcher polls a single queue for available jobs and dispatches them.
pub struct Dispatcher {
    queue: String,
    config: QueueConfig,
    pool: PgPool,
    executor: Arc<JobExecutor>,
    metrics: crate::metrics::AwaMetrics,
    _in_flight: InFlightMap,
    concurrency: ConcurrencyMode,
    alive: Arc<AtomicBool>,
    cancel: CancellationToken,
    job_set: Arc<Mutex<JoinSet<()>>>,
    rate_limiter: Option<TokenBucket>,
    storage: RuntimeStorage,
    capacity_wake: Arc<Notify>,
}

impl Dispatcher {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        queue: String,
        config: QueueConfig,
        pool: PgPool,
        executor: Arc<JobExecutor>,
        metrics: crate::metrics::AwaMetrics,
        in_flight: InFlightMap,
        alive: Arc<AtomicBool>,
        cancel: CancellationToken,
        job_set: Arc<Mutex<JoinSet<()>>>,
        storage: RuntimeStorage,
    ) -> Self {
        let concurrency = ConcurrencyMode::HardReserved {
            semaphore: Arc::new(Semaphore::new(config.max_workers as usize)),
        };
        let rate_limiter = config.rate_limit.as_ref().map(TokenBucket::new);
        Self {
            queue,
            config,
            pool,
            executor,
            metrics,
            _in_flight: in_flight,
            concurrency,
            alive,
            cancel,
            job_set,
            rate_limiter,
            storage,
            capacity_wake: Arc::new(Notify::new()),
        }
    }

    /// Create a dispatcher with a specific concurrency mode (used for weighted mode).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_concurrency(
        queue: String,
        config: QueueConfig,
        pool: PgPool,
        executor: Arc<JobExecutor>,
        metrics: crate::metrics::AwaMetrics,
        in_flight: InFlightMap,
        alive: Arc<AtomicBool>,
        cancel: CancellationToken,
        job_set: Arc<Mutex<JoinSet<()>>>,
        concurrency: ConcurrencyMode,
        storage: RuntimeStorage,
    ) -> Self {
        let rate_limiter = config.rate_limit.as_ref().map(TokenBucket::new);
        Self {
            queue,
            config,
            pool,
            executor,
            metrics,
            _in_flight: in_flight,
            concurrency,
            alive,
            cancel,
            job_set,
            rate_limiter,
            storage,
            capacity_wake: Arc::new(Notify::new()),
        }
    }

    /// Run the poll loop. Returns when cancelled.
    #[tracing::instrument(skip(self), fields(queue = %self.queue))]
    pub async fn run(mut self) {
        self.alive.store(true, Ordering::SeqCst);
        info!(
            queue = %self.queue,
            poll_interval_ms = self.config.poll_interval.as_millis(),
            "Dispatcher started"
        );

        // Set up LISTEN/NOTIFY for this queue
        let notify_channel = format!("awa:{}", self.queue);
        let mut listener = match sqlx::postgres::PgListener::connect_with(&self.pool).await {
            Ok(listener) => listener,
            Err(err) => {
                error!(error = %err, "Failed to create PG listener, falling back to polling only");
                // Fall back to poll-only mode
                self.poll_loop_only().await;
                self.alive.store(false, Ordering::SeqCst);
                return;
            }
        };

        if let Err(err) = listener.listen(&notify_channel).await {
            warn!(error = %err, channel = %notify_channel, "Failed to LISTEN, falling back to polling");
            self.poll_loop_only().await;
            self.alive.store(false, Ordering::SeqCst);
            return;
        }

        debug!(channel = %notify_channel, "Listening for job notifications");

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    debug!(queue = %self.queue, "Dispatcher shutting down");
                    break;
                }
                // Wait for either a notification or the poll interval
                notification = listener.recv() => {
                    match notification {
                        Ok(_) => {
                            debug!(queue = %self.queue, "Woken by NOTIFY");
                            self.drain_ready().await;
                        }
                        Err(err) => {
                            warn!(error = %err, "PG listener error, will retry");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                _ = self.capacity_wake.notified() => {
                    self.drain_ready().await;
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    self.drain_ready().await;
                }
            }
        }

        self.alive.store(false, Ordering::SeqCst);
    }

    /// Poll-only fallback (no LISTEN/NOTIFY).
    async fn poll_loop_only(&mut self) {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    debug!(queue = %self.queue, "Dispatcher (poll-only) shutting down");
                    break;
                }
                _ = self.capacity_wake.notified() => {
                    self.drain_ready().await;
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    self.drain_ready().await;
                }
            }
        }
    }

    /// Pre-acquire permits (non-blocking). Returns a vec of permits.
    fn acquire_permits(&mut self) -> Vec<DispatchPermit> {
        let mut permits = Vec::new();
        match &self.concurrency {
            ConcurrencyMode::HardReserved { semaphore } => {
                for _ in 0..CLAIM_BATCH_LIMIT {
                    match semaphore.clone().try_acquire_owned() {
                        Ok(p) => permits.push(DispatchPermit::Hard(p)),
                        Err(_) => break,
                    }
                }
            }
            ConcurrencyMode::Weighted {
                local_semaphore,
                overflow_pool,
                queue_name,
            } => {
                // First: local (guaranteed) permits
                for _ in 0..CLAIM_BATCH_LIMIT {
                    match local_semaphore.clone().try_acquire_owned() {
                        Ok(p) => permits.push(DispatchPermit::Local(p)),
                        Err(_) => break,
                    }
                }
                // Then: overflow permits up to the claim batch limit.
                let overflow_wanted = (CLAIM_BATCH_LIMIT.saturating_sub(permits.len())) as u32;
                let granted = overflow_pool.try_acquire(queue_name, overflow_wanted);
                for _ in 0..granted {
                    permits.push(DispatchPermit::Overflow {
                        pool: overflow_pool.clone(),
                        queue: queue_name.clone(),
                    });
                }
            }
        }
        permits
    }

    /// Drain immediately available work after a wake-up until the queue is empty,
    /// capacity is exhausted, rate limiting stops us, or shutdown is requested.
    async fn drain_ready(&mut self) {
        while !self.cancel.is_cancelled() {
            if !self.poll_once().await {
                break;
            }
        }
    }

    /// Single poll iteration: pre-acquire permits, claim jobs, dispatch.
    #[tracing::instrument(skip(self), fields(queue = %self.queue))]
    async fn poll_once(&mut self) -> bool {
        // Phase 1: Pre-acquire permits (non-blocking)
        let mut permits = self.acquire_permits();
        if permits.is_empty() {
            return false;
        }

        // Phase 2: Apply rate limit
        let rate_available = self
            .rate_limiter
            .as_mut()
            .map(|rl| rl.available() as usize)
            .unwrap_or(usize::MAX);
        let batch_size = permits.len().min(rate_available).min(CLAIM_BATCH_LIMIT);
        if batch_size == 0 {
            // Drop all permits — rate limited
            return false;
        }
        // Release excess permits beyond what rate limit allows
        while permits.len() > batch_size {
            permits.pop(); // Drop releases the permit
        }

        // Phase 3: Claim jobs from DB.
        //
        // Uses a CTE (not a FROM-subquery) so the LIMIT is enforced as a
        // materialization barrier. PostgreSQL's planner can merge a
        // FROM-subquery with the UPDATE target when both reference the same
        // table, which under concurrent load causes the LIMIT to be ignored.
        //
        // Single index scan on idx_awa_jobs_hot_dequeue with FOR UPDATE SKIP LOCKED
        // acquires row locks during the scan. Priority ordering is strict
        // (priority ASC, run_at ASC, id ASC); cross-priority fairness is handled
        // by the maintenance leader's age_waiting_priorities task (ADR-005).
        let deadline_secs = self.config.deadline_duration.as_secs_f64();
        let claim_start = Instant::now();

        let jobs: Vec<JobRow> = match &self.storage {
            RuntimeStorage::Canonical => match sqlx::query_as::<_, JobRow>(
                r#"
                WITH claimed AS (
                    SELECT id
                    FROM awa.jobs_hot
                    WHERE state = 'available'
                      AND queue = $1
                      AND run_at <= now()
                      AND NOT EXISTS (
                          SELECT 1 FROM awa.queue_meta
                          WHERE queue = $1 AND paused = TRUE
                      )
                    ORDER BY priority ASC, run_at ASC, id ASC
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE awa.jobs_hot
                SET state = 'running',
                    attempt = attempt + 1,
                    run_lease = run_lease + 1,
                    attempted_at = now(),
                    heartbeat_at = now(),
                    deadline_at = now() + make_interval(secs => $3)
                FROM claimed
                WHERE awa.jobs_hot.id = claimed.id
                  AND awa.jobs_hot.state = 'available'
                RETURNING awa.jobs_hot.*
                "#,
            )
            .bind(&self.queue)
            .bind(batch_size as i32)
            .bind(deadline_secs)
            .fetch_all(&self.pool)
            .await
            {
                Ok(jobs) => jobs,
                Err(err) => {
                    warn!(queue = %self.queue, error = %err, "Failed to claim jobs");
                    return false;
                }
            },
            RuntimeStorage::QueueStorage(runtime) => match runtime
                .store
                .claim_job_batch(
                    &self.pool,
                    &self.queue,
                    batch_size as i64,
                    self.config.deadline_duration,
                )
                .await
            {
                Ok(jobs) => jobs,
                Err(err) => {
                    warn!(
                        queue = %self.queue,
                        error = %err,
                        "Failed to claim queue storage jobs"
                    );
                    return false;
                }
            },
        };
        self.metrics
            .record_claim_batch(&self.queue, jobs.len() as u64, claim_start.elapsed());
        if !jobs.is_empty() {
            self.metrics
                .record_job_claimed(&self.queue, jobs.len() as u64);
            for job in &jobs {
                if let Some(attempted_at) = job.attempted_at {
                    let wait_secs =
                        (attempted_at - job.created_at).num_milliseconds() as f64 / 1000.0;
                    if wait_secs >= 0.0 {
                        self.metrics.record_wait_duration(&self.queue, wait_secs);
                    }
                }
            }
        }

        // Phase 4: Release excess permits if DB had fewer jobs
        while permits.len() > jobs.len() {
            permits.pop();
        }

        // Phase 5: Clear overflow demand if no jobs found
        if jobs.is_empty() {
            if let ConcurrencyMode::Weighted {
                overflow_pool,
                queue_name,
                ..
            } = &self.concurrency
            {
                overflow_pool.try_acquire(queue_name, 0);
            }
            return false;
        }

        debug!(queue = %self.queue, count = jobs.len(), "Claimed jobs");

        // Phase 6: Consume rate limit tokens
        if let Some(rl) = &mut self.rate_limiter {
            rl.consume(jobs.len() as u32);
        }

        // Phase 7: Dispatch (each job takes one pre-acquired permit)
        let mut set = self.job_set.lock().await;
        for (job, permit) in jobs.into_iter().zip(permits) {
            let cancel_flag = Arc::new(AtomicBool::new(false));
            let task = self.executor.execute_task(job, cancel_flag);
            let capacity_wake = self.capacity_wake.clone();
            set.spawn(async move {
                task.await;
                drop(permit);
                capacity_wake.notify_one();
            });
        }

        true
    }
}
