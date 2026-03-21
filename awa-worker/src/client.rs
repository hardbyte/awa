use crate::completion::CompletionBatcher;
use crate::dispatcher::{ConcurrencyMode, Dispatcher, OverflowPool, QueueConfig};
use crate::executor::{BoxedWorker, JobError, JobExecutor, JobResult, Worker};
use crate::heartbeat::HeartbeatService;
use crate::maintenance::{MaintenanceService, RetentionPolicy};
use crate::runtime::{InFlightMap, InFlightRegistry};
use awa_model::{JobArgs, JobRow, PeriodicJob};
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Errors returned when building a worker client.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    #[error("at least one queue must be configured")]
    NoQueuesConfigured,
    #[error("sum of min_workers ({total_min}) exceeds global_max_workers ({global_max})")]
    MinWorkersExceedGlobal { total_min: u32, global_max: u32 },
    #[error("rate_limit max_rate must be > 0.0")]
    InvalidRateLimit,
    #[error("queue weight must be > 0")]
    InvalidWeight,
    #[error("cleanup_batch_size must be > 0")]
    InvalidBatchSize,
}

/// Health check result.
#[derive(Debug, Clone)]
pub struct HealthCheck {
    pub healthy: bool,
    pub postgres_connected: bool,
    pub poll_loop_alive: bool,
    pub heartbeat_alive: bool,
    pub shutting_down: bool,
    pub leader: bool,
    pub queues: HashMap<String, QueueHealth>,
}

/// Per-queue health.
#[derive(Debug, Clone)]
pub struct QueueHealth {
    pub in_flight: u32,
    pub available: u64,
    /// Capacity interpretation depends on mode.
    pub capacity: QueueCapacity,
}

/// Capacity information for a queue, mode-dependent.
#[derive(Debug, Clone)]
pub enum QueueCapacity {
    /// Hard-reserved: fixed max.
    HardReserved { max_workers: u32 },
    /// Weighted: min guaranteed + current overflow.
    Weighted {
        min_workers: u32,
        weight: u32,
        overflow_held: u32,
    },
}

/// Builder for the Awa worker client.
pub struct ClientBuilder {
    pool: PgPool,
    queues: Vec<(String, QueueConfig)>,
    workers: HashMap<String, BoxedWorker>,
    state: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    heartbeat_interval: Duration,
    promote_interval: Duration,
    heartbeat_rescue_interval: Option<Duration>,
    deadline_rescue_interval: Option<Duration>,
    callback_rescue_interval: Option<Duration>,
    periodic_jobs: Vec<PeriodicJob>,
    global_max_workers: Option<u32>,
    leader_election_interval: Option<Duration>,
    completed_retention: Option<Duration>,
    failed_retention: Option<Duration>,
    cleanup_batch_size: Option<i64>,
    cleanup_interval: Option<Duration>,
    queue_retention_overrides: HashMap<String, RetentionPolicy>,
}

impl ClientBuilder {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            queues: Vec::new(),
            workers: HashMap::new(),
            state: HashMap::new(),
            heartbeat_interval: Duration::from_secs(30),
            promote_interval: Duration::from_millis(250),
            heartbeat_rescue_interval: None,
            deadline_rescue_interval: None,
            callback_rescue_interval: None,
            periodic_jobs: Vec::new(),
            global_max_workers: None,
            leader_election_interval: None,
            completed_retention: None,
            failed_retention: None,
            cleanup_batch_size: None,
            cleanup_interval: None,
            queue_retention_overrides: HashMap::new(),
        }
    }

    /// Add a queue with its configuration.
    pub fn queue(mut self, name: impl Into<String>, config: QueueConfig) -> Self {
        self.queues.push((name.into(), config));
        self
    }

    /// Register a typed worker.
    ///
    /// The worker handles jobs of type `T` where `T: JobArgs + DeserializeOwned`.
    /// The handler function receives the deserialized args and job context.
    pub fn register<T, F, Fut>(mut self, handler: F) -> Self
    where
        T: JobArgs + DeserializeOwned + Send + Sync + 'static,
        F: Fn(T, &crate::context::JobContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<JobResult, JobError>> + Send + Sync + 'static,
    {
        let kind = T::kind().to_string();
        let worker = TypedWorker {
            kind: T::kind(),
            handler: Arc::new(handler),
            _phantom: std::marker::PhantomData,
        };
        self.workers.insert(kind, Box::new(worker));
        self
    }

    /// Register a raw worker implementation.
    pub fn register_worker(mut self, worker: impl Worker + 'static) -> Self {
        let kind = worker.kind().to_string();
        self.workers.insert(kind, Box::new(worker));
        self
    }

    /// Add shared state accessible via `ctx.extract::<T>()`.
    pub fn state<T: Any + Send + Sync + Clone>(mut self, value: T) -> Self {
        self.state.insert(TypeId::of::<T>(), Box::new(value));
        self
    }

    /// Set the heartbeat interval (default: 30s).
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Set the scheduled/retryable promotion interval (default: 250ms).
    pub fn promote_interval(mut self, interval: Duration) -> Self {
        self.promote_interval = interval;
        self
    }

    /// Set the stale-heartbeat rescue interval (default: 30s).
    pub fn heartbeat_rescue_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_rescue_interval = Some(interval);
        self
    }

    /// Set the deadline rescue interval (default: 30s).
    pub fn deadline_rescue_interval(mut self, interval: Duration) -> Self {
        self.deadline_rescue_interval = Some(interval);
        self
    }

    /// Set the callback-timeout rescue interval (default: 30s).
    pub fn callback_rescue_interval(mut self, interval: Duration) -> Self {
        self.callback_rescue_interval = Some(interval);
        self
    }

    /// Set the leader election retry interval (default: 10s).
    ///
    /// Controls how often a non-leader instance retries acquiring the maintenance
    /// advisory lock. Lower values are useful in tests.
    pub fn leader_election_interval(mut self, interval: Duration) -> Self {
        self.leader_election_interval = Some(interval);
        self
    }

    /// Set a global maximum worker count across all queues (enables weighted mode).
    ///
    /// When set, each queue gets `min_workers` guaranteed permits plus a share
    /// of the remaining overflow capacity based on `weight`.
    pub fn global_max_workers(mut self, max: u32) -> Self {
        self.global_max_workers = Some(max);
        self
    }

    /// Set retention for completed jobs (default: 24h).
    pub fn completed_retention(mut self, retention: Duration) -> Self {
        self.completed_retention = Some(retention);
        self
    }

    /// Set retention for failed/cancelled jobs (default: 72h).
    pub fn failed_retention(mut self, retention: Duration) -> Self {
        self.failed_retention = Some(retention);
        self
    }

    /// Set the maximum number of jobs to delete per cleanup pass (default: 1000).
    pub fn cleanup_batch_size(mut self, batch_size: i64) -> Self {
        self.cleanup_batch_size = Some(batch_size);
        self
    }

    /// Set the cleanup interval (default: 60s).
    pub fn cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = Some(interval);
        self
    }

    /// Set a per-queue retention override.
    pub fn queue_retention(mut self, queue: impl Into<String>, policy: RetentionPolicy) -> Self {
        self.queue_retention_overrides.insert(queue.into(), policy);
        self
    }

    /// Register a periodic (cron) job schedule.
    ///
    /// The schedule is synced to the database by the leader and evaluated
    /// every second. When a fire is due, a job is atomically enqueued.
    pub fn periodic(mut self, job: PeriodicJob) -> Self {
        self.periodic_jobs.push(job);
        self
    }

    /// Build the client.
    pub fn build(self) -> Result<Client, BuildError> {
        if self.queues.is_empty() {
            return Err(BuildError::NoQueuesConfigured);
        }

        // Validate rate limits and weights
        for (_, config) in &self.queues {
            if let Some(rl) = &config.rate_limit {
                if rl.max_rate <= 0.0 {
                    return Err(BuildError::InvalidRateLimit);
                }
            }
            if config.weight == 0 {
                return Err(BuildError::InvalidWeight);
            }
        }

        // Validate batch size
        if let Some(bs) = self.cleanup_batch_size {
            if bs <= 0 {
                return Err(BuildError::InvalidBatchSize);
            }
        }

        // Validate weighted mode constraints
        let overflow_pool = if let Some(global_max) = self.global_max_workers {
            let total_min: u32 = self.queues.iter().map(|(_, c)| c.min_workers).sum();
            if total_min > global_max {
                return Err(BuildError::MinWorkersExceedGlobal {
                    total_min,
                    global_max,
                });
            }
            let overflow_capacity = global_max - total_min;
            let weights: HashMap<String, u32> = self
                .queues
                .iter()
                .map(|(name, c)| (name.clone(), c.weight.max(1)))
                .collect();
            Some(Arc::new(OverflowPool::new(overflow_capacity, weights)))
        } else {
            None
        };

        let metrics = crate::metrics::AwaMetrics::from_global();
        let queue_in_flight = Arc::new(
            self.queues
                .iter()
                .map(|(name, _)| (name.clone(), Arc::new(AtomicU32::new(0))))
                .collect(),
        );
        let dispatcher_alive = Arc::new(
            self.queues
                .iter()
                .map(|(name, _)| (name.clone(), Arc::new(AtomicBool::new(false))))
                .collect(),
        );

        Ok(Client {
            pool: self.pool,
            queues: self.queues,
            workers: Arc::new(self.workers),
            state: Arc::new(self.state),
            heartbeat_interval: self.heartbeat_interval,
            promote_interval: self.promote_interval,
            heartbeat_rescue_interval: self.heartbeat_rescue_interval,
            deadline_rescue_interval: self.deadline_rescue_interval,
            callback_rescue_interval: self.callback_rescue_interval,
            periodic_jobs: Arc::new(self.periodic_jobs),
            dispatch_cancel: CancellationToken::new(),
            service_cancel: CancellationToken::new(),
            dispatcher_handles: RwLock::new(Vec::new()),
            service_handles: RwLock::new(Vec::new()),
            job_set: Arc::new(Mutex::new(JoinSet::new())),
            in_flight: Arc::new(InFlightRegistry::default()),
            queue_in_flight,
            dispatcher_alive,
            heartbeat_alive: Arc::new(AtomicBool::new(false)),
            leader: Arc::new(AtomicBool::new(false)),
            overflow_pool,
            metrics,
            leader_election_interval: self.leader_election_interval,
            completed_retention: self.completed_retention,
            failed_retention: self.failed_retention,
            cleanup_batch_size: self.cleanup_batch_size,
            cleanup_interval: self.cleanup_interval,
            queue_retention_overrides: self.queue_retention_overrides,
        })
    }
}

/// A typed worker that deserializes args and calls a handler function.
struct TypedWorker<T, F, Fut>
where
    T: JobArgs + DeserializeOwned + Send + Sync + 'static,
    F: Fn(T, &crate::context::JobContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<JobResult, JobError>> + Send + Sync + 'static,
{
    kind: &'static str,
    handler: Arc<F>,
    _phantom: std::marker::PhantomData<fn() -> (T, Fut)>,
}

#[async_trait::async_trait]
impl<T, F, Fut> Worker for TypedWorker<T, F, Fut>
where
    T: JobArgs + DeserializeOwned + Send + Sync + 'static,
    F: Fn(T, &crate::context::JobContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<JobResult, JobError>> + Send + Sync + 'static,
{
    fn kind(&self) -> &'static str {
        self.kind
    }

    async fn perform(
        &self,
        job_row: &JobRow,
        ctx: &crate::context::JobContext,
    ) -> Result<JobResult, JobError> {
        // Deserialize args
        let args: T = serde_json::from_value(job_row.args.clone())
            .map_err(|err| JobError::Terminal(format!("failed to deserialize args: {}", err)))?;

        (self.handler)(args, ctx).await
    }
}

/// The Awa worker client — manages dispatchers, heartbeat, and maintenance.
pub struct Client {
    pool: PgPool,
    queues: Vec<(String, QueueConfig)>,
    workers: Arc<HashMap<String, BoxedWorker>>,
    state: Arc<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    heartbeat_interval: Duration,
    promote_interval: Duration,
    heartbeat_rescue_interval: Option<Duration>,
    deadline_rescue_interval: Option<Duration>,
    callback_rescue_interval: Option<Duration>,
    periodic_jobs: Arc<Vec<PeriodicJob>>,
    /// Cancellation token for dispatchers only — stops claiming new jobs.
    dispatch_cancel: CancellationToken,
    /// Cancellation token for heartbeat + maintenance — kept alive during drain.
    service_cancel: CancellationToken,
    /// Handles for dispatcher tasks.
    dispatcher_handles: RwLock<Vec<tokio::task::JoinHandle<()>>>,
    /// Handles for service tasks (heartbeat + maintenance).
    service_handles: RwLock<Vec<tokio::task::JoinHandle<()>>>,
    /// JoinSet tracking in-flight job tasks for graceful drain.
    job_set: Arc<Mutex<JoinSet<()>>>,
    in_flight: InFlightMap,
    queue_in_flight: Arc<HashMap<String, Arc<AtomicU32>>>,
    dispatcher_alive: Arc<HashMap<String, Arc<AtomicBool>>>,
    heartbeat_alive: Arc<AtomicBool>,
    leader: Arc<AtomicBool>,
    /// Shared overflow pool for weighted mode (None in hard-reserved mode).
    overflow_pool: Option<Arc<OverflowPool>>,
    metrics: crate::metrics::AwaMetrics,
    leader_election_interval: Option<Duration>,
    completed_retention: Option<Duration>,
    failed_retention: Option<Duration>,
    cleanup_batch_size: Option<i64>,
    cleanup_interval: Option<Duration>,
    queue_retention_overrides: HashMap<String, RetentionPolicy>,
}

impl Client {
    /// Create a new builder.
    pub fn builder(pool: PgPool) -> ClientBuilder {
        ClientBuilder::new(pool)
    }

    /// Start the worker runtime. Spawns dispatchers, heartbeat, and maintenance.
    pub async fn start(&self) -> Result<(), awa_model::AwaError> {
        info!(
            queues = self.queues.len(),
            workers = self.workers.len(),
            "Starting Awa worker runtime"
        );

        // Completion batcher stays alive during drain so tasks can release
        // only after their completion has been acknowledged.
        let (completion_batcher, completion_handle) = CompletionBatcher::new(
            self.pool.clone(),
            self.service_cancel.clone(),
            self.metrics.clone(),
        );

        // Create executor with metrics
        let executor = Arc::new(JobExecutor::new(
            self.pool.clone(),
            self.workers.clone(),
            self.in_flight.clone(),
            self.queue_in_flight.clone(),
            self.state.clone(),
            self.metrics.clone(),
            completion_handle,
        ));

        let mut service_handles = self.service_handles.write().await;

        service_handles.extend(completion_batcher.spawn());

        // Start heartbeat service (uses service_cancel — stays alive during drain)
        let heartbeat = HeartbeatService::new(
            self.pool.clone(),
            self.in_flight.clone(),
            self.heartbeat_interval,
            self.heartbeat_alive.clone(),
            self.service_cancel.clone(),
        );
        service_handles.push(tokio::spawn(async move {
            heartbeat.run().await;
        }));

        // Start maintenance service (uses service_cancel — stays alive during drain)
        let mut maintenance = MaintenanceService::new(
            self.pool.clone(),
            self.metrics.clone(),
            self.leader.clone(),
            self.service_cancel.clone(),
            self.periodic_jobs.clone(),
            self.in_flight.clone(),
        )
        .promote_interval(self.promote_interval);
        if let Some(interval) = self.heartbeat_rescue_interval {
            maintenance = maintenance.heartbeat_rescue_interval(interval);
        }
        if let Some(interval) = self.deadline_rescue_interval {
            maintenance = maintenance.deadline_rescue_interval(interval);
        }
        if let Some(interval) = self.callback_rescue_interval {
            maintenance = maintenance.callback_rescue_interval(interval);
        }
        if let Some(interval) = self.leader_election_interval {
            maintenance = maintenance.leader_election_interval(interval);
        }
        if let Some(retention) = self.completed_retention {
            maintenance = maintenance.completed_retention(retention);
        }
        if let Some(retention) = self.failed_retention {
            maintenance = maintenance.failed_retention(retention);
        }
        if let Some(batch_size) = self.cleanup_batch_size {
            maintenance = maintenance.cleanup_batch_size(batch_size);
        }
        if let Some(interval) = self.cleanup_interval {
            maintenance = maintenance.cleanup_interval(interval);
        }
        if !self.queue_retention_overrides.is_empty() {
            maintenance =
                maintenance.queue_retention_overrides(self.queue_retention_overrides.clone());
        }
        service_handles.push(tokio::spawn(async move {
            maintenance.run().await;
        }));

        // Start a dispatcher per queue (uses dispatch_cancel — stops claiming first)
        let mut dispatcher_handles = self.dispatcher_handles.write().await;
        for (queue_name, config) in &self.queues {
            let alive = self
                .dispatcher_alive
                .get(queue_name)
                .cloned()
                .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

            let dispatcher = if let Some(overflow_pool) = &self.overflow_pool {
                // Weighted mode
                let concurrency = ConcurrencyMode::Weighted {
                    local_semaphore: Arc::new(tokio::sync::Semaphore::new(
                        config.min_workers as usize,
                    )),
                    overflow_pool: overflow_pool.clone(),
                    queue_name: queue_name.clone(),
                };
                Dispatcher::with_concurrency(
                    queue_name.clone(),
                    config.clone(),
                    self.pool.clone(),
                    executor.clone(),
                    self.metrics.clone(),
                    self.in_flight.clone(),
                    alive,
                    self.dispatch_cancel.clone(),
                    self.job_set.clone(),
                    concurrency,
                )
            } else {
                // Hard-reserved mode (default)
                Dispatcher::new(
                    queue_name.clone(),
                    config.clone(),
                    self.pool.clone(),
                    executor.clone(),
                    self.metrics.clone(),
                    self.in_flight.clone(),
                    alive,
                    self.dispatch_cancel.clone(),
                    self.job_set.clone(),
                )
            };
            dispatcher_handles.push(tokio::spawn(async move {
                dispatcher.run().await;
            }));
        }

        info!("Awa worker runtime started");
        Ok(())
    }

    /// Graceful shutdown with drain timeout.
    ///
    /// Phased lifecycle:
    /// 1. Stop dispatchers (no new jobs claimed)
    /// 2. Signal in-flight jobs to cancel
    /// 3. Wait for dispatchers to exit
    /// 4. Drain in-flight jobs (heartbeat + maintenance still alive!)
    /// 5. Stop heartbeat + maintenance
    pub async fn shutdown(&self, timeout: Duration) {
        info!("Initiating graceful shutdown");

        // Phase 1: Stop claiming new jobs
        self.dispatch_cancel.cancel();

        // Phase 2: Signal in-flight cancellation flags
        for flag in self.in_flight.flags() {
            flag.store(true, Ordering::SeqCst);
        }

        // Phase 3: Wait for dispatchers to exit their poll loops
        let dispatcher_handles: Vec<_> = {
            let mut guard = self.dispatcher_handles.write().await;
            std::mem::take(&mut *guard)
        };
        for handle in dispatcher_handles {
            let _ = handle.await;
        }

        // Phase 4: Drain in-flight jobs (heartbeat + maintenance still alive)
        let drain = async {
            let mut set = self.job_set.lock().await;
            while set.join_next().await.is_some() {}
        };
        if tokio::time::timeout(timeout, drain).await.is_err() {
            warn!(
                timeout_secs = timeout.as_secs(),
                "Shutdown drain timeout exceeded, some jobs may not have completed"
            );
        }

        // Phase 5: Stop background services (heartbeat + maintenance)
        self.service_cancel.cancel();
        let service_handles: Vec<_> = {
            let mut guard = self.service_handles.write().await;
            std::mem::take(&mut *guard)
        };
        for handle in service_handles {
            let _ = handle.await;
        }

        info!("Awa worker runtime stopped");
    }

    /// Get the pool reference.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Health check.
    pub async fn health_check(&self) -> HealthCheck {
        let postgres_connected = sqlx::query("SELECT 1").execute(&self.pool).await.is_ok();
        let poll_loop_alive = self
            .dispatcher_alive
            .values()
            .all(|alive| alive.load(Ordering::SeqCst));
        let heartbeat_alive = self.heartbeat_alive.load(Ordering::SeqCst);
        let shutting_down = self.dispatch_cancel.is_cancelled();
        let leader = self.leader.load(Ordering::SeqCst);
        let available_rows = sqlx::query_as::<_, (String, i64)>(
            r#"
            SELECT queue, count(*)::bigint AS available
            FROM awa.jobs_hot
            WHERE state = 'available'
            GROUP BY queue
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();
        let available_by_queue: HashMap<_, _> = available_rows.into_iter().collect();
        let queues = self
            .queues
            .iter()
            .map(|(queue, config)| {
                let in_flight = self
                    .queue_in_flight
                    .get(queue)
                    .map(|counter| counter.load(Ordering::SeqCst))
                    .unwrap_or(0);
                let available = available_by_queue.get(queue).copied().unwrap_or(0).max(0) as u64;
                let capacity = if let Some(overflow_pool) = &self.overflow_pool {
                    QueueCapacity::Weighted {
                        min_workers: config.min_workers,
                        weight: config.weight,
                        overflow_held: overflow_pool.held(queue),
                    }
                } else {
                    QueueCapacity::HardReserved {
                        max_workers: config.max_workers,
                    }
                };
                (
                    queue.clone(),
                    QueueHealth {
                        in_flight,
                        available,
                        capacity,
                    },
                )
            })
            .collect();

        HealthCheck {
            healthy: postgres_connected && poll_loop_alive && heartbeat_alive && !shutting_down,
            postgres_connected,
            poll_loop_alive,
            heartbeat_alive,
            shutting_down,
            leader,
            queues,
        }
    }
}
