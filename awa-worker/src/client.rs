use crate::completion::CompletionBatcher;
use crate::dispatcher::{ConcurrencyMode, Dispatcher, OverflowPool, QueueConfig};
use crate::events::{BoxedUntypedEventHandler, JobEvent, UntypedJobEvent};
use crate::executor::{BoxedWorker, DlqPolicy, JobError, JobExecutor, JobResult, Worker};
use crate::heartbeat::HeartbeatService;
use crate::maintenance::{MaintenanceService, RetentionPolicy};
use crate::runtime::{InFlightMap, InFlightRegistry};
use crate::storage::{QueueStorageRuntime, RuntimeStorage};
use awa_model::admin::{
    self, JobKindDescriptor, NamedJobKindDescriptor, NamedQueueDescriptor, QueueDescriptor,
    QueueRuntimeConfigSnapshot, QueueRuntimeMode, QueueRuntimeSnapshot, RateLimitSnapshot,
    RuntimeSnapshotInput, StorageCapability,
};
use awa_model::{storage as transition, JobArgs, PeriodicJob, QueueStorageConfig};
use chrono::{DateTime, Utc};
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
use uuid::Uuid;

/// Errors returned when building a worker client.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    #[error("at least one queue must be configured")]
    NoQueuesConfigured,
    #[error("queue descriptor declared for unknown queue '{queue}'")]
    QueueDescriptorWithoutQueue { queue: String },
    #[error("sum of min_workers ({total_min}) exceeds global_max_workers ({global_max})")]
    MinWorkersExceedGlobal { total_min: u32, global_max: u32 },
    #[error("rate_limit max_rate must be > 0.0")]
    InvalidRateLimit,
    #[error("queue weight must be > 0")]
    InvalidWeight,
    #[error("cleanup_batch_size must be > 0")]
    InvalidBatchSize,
    #[error("dlq_cleanup_batch_size must be > 0")]
    InvalidDlqBatchSize,
    #[error("invalid queue storage config: {0}")]
    InvalidQueueStorage(String),
}

/// Health check result.
#[derive(Debug, Clone)]
pub struct HealthCheck {
    pub healthy: bool,
    pub postgres_connected: bool,
    pub poll_loop_alive: bool,
    pub heartbeat_alive: bool,
    pub maintenance_alive: bool,
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

/// Temporary execution role used during a storage transition.
///
/// This is not intended as a long-term “run either backend forever” feature.
/// It exists so a `0.6` rollout can keep some runtimes draining canonical
/// backlog while other runtimes are already prepared to execute queue-storage
/// work as soon as routing flips.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransitionWorkerRole {
    /// Follow `awa.storage_status()`:
    /// canonical in `canonical` / `prepared`,
    /// queue storage in `mixed_transition` / `active`.
    #[default]
    Auto,
    /// Stay on canonical execution even after routing flips.
    CanonicalDrain,
    /// Run queue storage immediately, even before routing flips.
    QueueStorageTarget,
}

/// Builder for the Awa worker client.
pub struct ClientBuilder {
    pool: PgPool,
    queues: Vec<(String, QueueConfig)>,
    queue_descriptors: HashMap<String, QueueDescriptor>,
    job_kind_descriptors: HashMap<String, JobKindDescriptor>,
    workers: HashMap<String, BoxedWorker>,
    lifecycle_handlers: HashMap<String, Vec<BoxedUntypedEventHandler>>,
    state: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    heartbeat_interval: Duration,
    promote_interval: Duration,
    heartbeat_rescue_interval: Option<Duration>,
    heartbeat_staleness: Option<Duration>,
    deadline_rescue_interval: Option<Duration>,
    callback_rescue_interval: Option<Duration>,
    periodic_jobs: Vec<PeriodicJob>,
    global_max_workers: Option<u32>,
    leader_election_interval: Option<Duration>,
    leader_check_interval: Option<Duration>,
    completed_retention: Option<Duration>,
    failed_retention: Option<Duration>,
    descriptor_retention: Option<Duration>,
    cleanup_batch_size: Option<i64>,
    cleanup_interval: Option<Duration>,
    queue_retention_overrides: HashMap<String, RetentionPolicy>,
    runtime_snapshot_interval: Duration,
    queue_stats_interval: Option<Duration>,
    dlq_enabled_by_default: bool,
    dlq_retention: Option<Duration>,
    dlq_cleanup_batch_size: Option<i64>,
    dlq_overrides: HashMap<String, bool>,
    storage: RuntimeStorage,
    transition_role: TransitionWorkerRole,
    storage_error: Option<BuildError>,
}

impl ClientBuilder {
    pub fn new(pool: PgPool) -> Self {
        let (storage, storage_error) = match QueueStorageRuntime::new(
            QueueStorageConfig::default(),
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        ) {
            Ok(runtime) => (RuntimeStorage::QueueStorage(runtime), None),
            Err(err) => (
                RuntimeStorage::Canonical,
                Some(BuildError::InvalidQueueStorage(err.to_string())),
            ),
        };

        Self {
            pool,
            queues: Vec::new(),
            queue_descriptors: HashMap::new(),
            job_kind_descriptors: HashMap::new(),
            workers: HashMap::new(),
            lifecycle_handlers: HashMap::new(),
            state: HashMap::new(),
            heartbeat_interval: Duration::from_secs(30),
            promote_interval: Duration::from_millis(250),
            heartbeat_rescue_interval: None,
            heartbeat_staleness: None,
            deadline_rescue_interval: None,
            callback_rescue_interval: None,
            periodic_jobs: Vec::new(),
            global_max_workers: None,
            leader_election_interval: None,
            leader_check_interval: None,
            completed_retention: None,
            failed_retention: None,
            descriptor_retention: None,
            cleanup_batch_size: None,
            cleanup_interval: None,
            queue_retention_overrides: HashMap::new(),
            runtime_snapshot_interval: Duration::from_secs(10),
            queue_stats_interval: None,
            dlq_enabled_by_default: false,
            dlq_retention: None,
            dlq_cleanup_batch_size: None,
            dlq_overrides: HashMap::new(),
            storage,
            transition_role: TransitionWorkerRole::Auto,
            storage_error,
        }
    }

    /// Add a queue with its configuration.
    pub fn queue(mut self, name: impl Into<String>, config: QueueConfig) -> Self {
        self.queues.push((name.into(), config));
        self
    }

    /// Attach descriptive metadata (display name, description, owner,
    /// docs URL, tags, extra JSON) to a queue so it appears labelled in
    /// the admin API and UI. The queue must also be declared via
    /// [`queue`]; otherwise [`build`] fails with
    /// [`BuildError::QueueDescriptorWithoutQueue`].
    ///
    /// [`queue`]: ClientBuilder::queue
    /// [`build`]: ClientBuilder::build
    pub fn queue_descriptor(
        mut self,
        name: impl Into<String>,
        descriptor: QueueDescriptor,
    ) -> Self {
        self.queue_descriptors.insert(name.into(), descriptor);
        self
    }

    /// Attach descriptive metadata to a typed job kind. The kind string is
    /// taken from [`JobArgs::kind`] on `T`.
    pub fn job_kind_descriptor<T: JobArgs>(mut self, descriptor: JobKindDescriptor) -> Self {
        self.job_kind_descriptors
            .insert(T::kind().to_string(), descriptor);
        self
    }

    /// Attach descriptive metadata to a job kind by string name. Useful
    /// when the kind is known dynamically (e.g. from language bridges).
    pub fn job_kind_descriptor_kind(
        mut self,
        kind: impl Into<String>,
        descriptor: JobKindDescriptor,
    ) -> Self {
        self.job_kind_descriptors.insert(kind.into(), descriptor);
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

    /// Register a typed lifecycle event handler for a job kind.
    ///
    /// Handlers run only after the corresponding DB state transition commits.
    /// They are best-effort in-process hooks, not a durable workflow mechanism.
    /// Capture any shared dependencies you need in the closure environment.
    pub fn on_event<T, F, Fut>(mut self, handler: F) -> Self
    where
        T: JobArgs + DeserializeOwned + Send + Sync + 'static,
        F: Fn(JobEvent<T>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let kind = T::kind().to_string();
        let handler = Arc::new(handler);
        let erased: BoxedUntypedEventHandler = Arc::new(move |event: UntypedJobEvent| {
            let handler = handler.clone();
            Box::pin(async move {
                let args: T = match serde_json::from_value(event.job().args.clone()) {
                    Ok(args) => args,
                    Err(err) => {
                        warn!(
                            job_id = event.job().id,
                            kind = %event.job().kind,
                            error = %err,
                            "Failed to deserialize args for lifecycle event handler"
                        );
                        return;
                    }
                };

                (handler)(event.into_typed(args)).await;
            })
        });
        self.lifecycle_handlers
            .entry(kind)
            .or_default()
            .push(erased);
        self
    }

    /// Register an untyped lifecycle event handler for a specific job kind.
    ///
    /// Use this with `register_worker(...)` or for cross-cutting logic that
    /// doesn't need typed args.
    pub fn on_event_kind<F, Fut>(mut self, kind: impl Into<String>, handler: F) -> Self
    where
        F: Fn(UntypedJobEvent) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let kind = kind.into();
        let handler = Arc::new(handler);
        let erased: BoxedUntypedEventHandler = Arc::new(move |event: UntypedJobEvent| {
            let handler = handler.clone();
            Box::pin(async move {
                (handler)(event).await;
            })
        });
        self.lifecycle_handlers
            .entry(kind)
            .or_default()
            .push(erased);
        self
    }

    /// Register a raw worker implementation.
    pub fn register_worker(mut self, worker: impl Worker + 'static) -> Self {
        let kind = worker.kind().to_string();
        self.workers.insert(kind, Box::new(worker));
        self
    }

    /// Register an HTTP worker that dispatches jobs to a remote endpoint.
    ///
    /// In async mode the worker POSTs the job and parks in `waiting_external`.
    /// In sync mode the worker awaits the HTTP response directly.
    ///
    /// Requires the `http-worker` feature.
    #[cfg(feature = "http-worker")]
    pub fn http_worker(
        self,
        kind: impl Into<String>,
        config: crate::http_worker::HttpWorkerConfig,
    ) -> Self {
        let worker = crate::http_worker::HttpWorker::new(kind.into(), config);
        self.register_worker(worker)
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

    /// Set how long a heartbeat must be stale before the job is rescued (default: 90s).
    ///
    /// Should be at least 3× the heartbeat interval to avoid false rescues.
    pub fn heartbeat_staleness(mut self, staleness: Duration) -> Self {
        self.heartbeat_staleness = Some(staleness);
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

    /// Set the leader connection health-check interval (default: 30s).
    pub fn leader_check_interval(mut self, interval: Duration) -> Self {
        self.leader_check_interval = Some(interval);
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

    /// How long a descriptor catalog row can go un-refreshed before the
    /// maintenance leader deletes it (default: 30 days). Pass
    /// `Duration::ZERO` to disable — the catalog will then accumulate
    /// rows indefinitely. See [`MaintenanceService::descriptor_retention`].
    pub fn descriptor_retention(mut self, retention: Duration) -> Self {
        self.descriptor_retention = Some(retention);
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

    /// Set how often runtime observability snapshots are published (default: 10s).
    pub fn runtime_snapshot_interval(mut self, interval: Duration) -> Self {
        self.runtime_snapshot_interval = interval;
        self
    }

    /// Set how often queue depth/lag metrics are published (default: 30s).
    pub fn queue_stats_interval(mut self, interval: Duration) -> Self {
        self.queue_stats_interval = Some(interval);
        self
    }

    /// Enable or disable DLQ routing by default.
    pub fn dlq_enabled_by_default(mut self, enabled: bool) -> Self {
        self.dlq_enabled_by_default = enabled;
        self
    }

    /// Override DLQ routing for a single queue.
    pub fn queue_dlq_enabled(mut self, queue: impl Into<String>, enabled: bool) -> Self {
        self.dlq_overrides.insert(queue.into(), enabled);
        self
    }

    /// Set retention for DLQ rows.
    pub fn dlq_retention(mut self, retention: Duration) -> Self {
        self.dlq_retention = Some(retention);
        self
    }

    /// Set the maximum number of DLQ rows deleted per cleanup pass.
    pub fn dlq_cleanup_batch_size(mut self, batch_size: i64) -> Self {
        self.dlq_cleanup_batch_size = Some(batch_size);
        self
    }

    /// Override the segmented queue storage configuration for this runtime.
    ///
    /// Queue storage is the default worker engine. Canonical tables remain
    /// migration compatibility, not a second supported worker runtime.
    /// Use this to change the schema name or rotation sizing/timing.
    pub fn queue_storage(
        mut self,
        config: QueueStorageConfig,
        queue_rotate_interval: Duration,
        lease_rotate_interval: Duration,
    ) -> Self {
        match QueueStorageRuntime::new(config, queue_rotate_interval, lease_rotate_interval) {
            Ok(runtime) => {
                self.storage = RuntimeStorage::QueueStorage(runtime);
                self.storage_error = None;
            }
            Err(err) => {
                self.storage = RuntimeStorage::Canonical;
                self.storage_error = Some(BuildError::InvalidQueueStorage(err.to_string()));
            }
        }
        self
    }

    /// Choose how this runtime participates in a storage transition.
    pub fn transition_role(mut self, role: TransitionWorkerRole) -> Self {
        self.transition_role = role;
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

        if let Some(err) = self.storage_error.clone() {
            return Err(err);
        }

        for queue in self.queue_descriptors.keys() {
            if !self.queues.iter().any(|(name, _)| name == queue) {
                return Err(BuildError::QueueDescriptorWithoutQueue {
                    queue: queue.clone(),
                });
            }
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
        if let Some(bs) = self.dlq_cleanup_batch_size {
            if bs <= 0 {
                return Err(BuildError::InvalidDlqBatchSize);
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

        // Warn if heartbeat_staleness is less than 3× heartbeat_interval
        if let Some(staleness) = self.heartbeat_staleness {
            let min_safe = self.heartbeat_interval * 3;
            if staleness < min_safe {
                tracing::warn!(
                    heartbeat_staleness_ms = staleness.as_millis() as u64,
                    heartbeat_interval_ms = self.heartbeat_interval.as_millis() as u64,
                    recommended_min_ms = min_safe.as_millis() as u64,
                    "heartbeat_staleness ({:?}) is less than 3× heartbeat_interval ({:?}); \
                     this may cause false rescues of jobs that are still running",
                    staleness,
                    self.heartbeat_interval,
                );
            }
        }

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
        let dlq_policy = DlqPolicy::new(self.dlq_enabled_by_default, self.dlq_overrides);

        Ok(Client {
            pool: self.pool,
            queues: self.queues,
            queue_descriptors: self.queue_descriptors,
            job_kind_descriptors: self.job_kind_descriptors,
            workers: Arc::new(self.workers),
            lifecycle_handlers: Arc::new(self.lifecycle_handlers),
            state: Arc::new(self.state),
            heartbeat_interval: self.heartbeat_interval,
            promote_interval: self.promote_interval,
            heartbeat_rescue_interval: self.heartbeat_rescue_interval,
            heartbeat_staleness: self.heartbeat_staleness,
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
            maintenance_alive: Arc::new(AtomicBool::new(false)),
            leader: Arc::new(AtomicBool::new(false)),
            overflow_pool,
            metrics,
            leader_election_interval: self.leader_election_interval,
            leader_check_interval: self.leader_check_interval,
            completed_retention: self.completed_retention,
            failed_retention: self.failed_retention,
            descriptor_retention: self.descriptor_retention,
            cleanup_batch_size: self.cleanup_batch_size,
            cleanup_interval: self.cleanup_interval,
            queue_retention_overrides: self.queue_retention_overrides,
            queue_stats_interval: self.queue_stats_interval,
            dlq_policy,
            dlq_retention: self.dlq_retention,
            dlq_cleanup_batch_size: self.dlq_cleanup_batch_size,
            effective_storage: Arc::new(RwLock::new(self.storage.clone())),
            storage: self.storage,
            transition_role: self.transition_role,
            global_max_workers: self.global_max_workers,
            runtime_snapshot_interval: self.runtime_snapshot_interval,
            runtime_instance_id: Uuid::new_v4(),
            runtime_started_at: Utc::now(),
            runtime_hostname: std::env::var("HOSTNAME").ok(),
            runtime_pid: std::process::id() as i32,
            runtime_version: env!("CARGO_PKG_VERSION"),
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

    async fn perform(&self, ctx: &crate::context::JobContext) -> Result<JobResult, JobError> {
        let args: T = serde_json::from_value(ctx.job.args.clone())
            .map_err(|err| JobError::Terminal(format!("failed to deserialize args: {}", err)))?;

        (self.handler)(args, ctx).await
    }
}

/// The Awa worker client — manages dispatchers, heartbeat, and maintenance.
pub struct Client {
    pool: PgPool,
    queues: Vec<(String, QueueConfig)>,
    queue_descriptors: HashMap<String, QueueDescriptor>,
    job_kind_descriptors: HashMap<String, JobKindDescriptor>,
    workers: Arc<HashMap<String, BoxedWorker>>,
    lifecycle_handlers: Arc<HashMap<String, Vec<BoxedUntypedEventHandler>>>,
    state: Arc<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    heartbeat_interval: Duration,
    promote_interval: Duration,
    heartbeat_rescue_interval: Option<Duration>,
    heartbeat_staleness: Option<Duration>,
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
    maintenance_alive: Arc<AtomicBool>,
    leader: Arc<AtomicBool>,
    /// Shared overflow pool for weighted mode (None in hard-reserved mode).
    overflow_pool: Option<Arc<OverflowPool>>,
    metrics: crate::metrics::AwaMetrics,
    leader_election_interval: Option<Duration>,
    leader_check_interval: Option<Duration>,
    completed_retention: Option<Duration>,
    failed_retention: Option<Duration>,
    descriptor_retention: Option<Duration>,
    cleanup_batch_size: Option<i64>,
    cleanup_interval: Option<Duration>,
    queue_retention_overrides: HashMap<String, RetentionPolicy>,
    queue_stats_interval: Option<Duration>,
    dlq_policy: DlqPolicy,
    dlq_retention: Option<Duration>,
    dlq_cleanup_batch_size: Option<i64>,
    storage: RuntimeStorage,
    transition_role: TransitionWorkerRole,
    effective_storage: Arc<RwLock<RuntimeStorage>>,
    global_max_workers: Option<u32>,
    runtime_snapshot_interval: Duration,
    runtime_instance_id: Uuid,
    runtime_started_at: DateTime<Utc>,
    runtime_hostname: Option<String>,
    runtime_pid: i32,
    runtime_version: &'static str,
}

#[derive(Clone)]
struct RuntimeReporterState {
    pool: PgPool,
    queues: Vec<(String, QueueConfig)>,
    queue_descriptors: HashMap<String, QueueDescriptor>,
    job_kind_descriptors: HashMap<String, JobKindDescriptor>,
    worker_kinds: Vec<String>,
    queue_in_flight: Arc<HashMap<String, Arc<AtomicU32>>>,
    dispatcher_alive: Arc<HashMap<String, Arc<AtomicBool>>>,
    heartbeat_alive: Arc<AtomicBool>,
    maintenance_alive: Arc<AtomicBool>,
    leader: Arc<AtomicBool>,
    dispatch_cancel: CancellationToken,
    overflow_pool: Option<Arc<OverflowPool>>,
    global_max_workers: Option<u32>,
    instance_id: Uuid,
    started_at: DateTime<Utc>,
    hostname: Option<String>,
    pid: i32,
    version: &'static str,
    snapshot_interval: Duration,
    effective_storage: Arc<RwLock<RuntimeStorage>>,
    queue_storage_capable: bool,
    metrics: crate::metrics::AwaMetrics,
}

impl Client {
    /// Create a new builder.
    pub fn builder(pool: PgPool) -> ClientBuilder {
        ClientBuilder::new(pool)
    }

    fn expected_queue_storage_schema(
        status: &transition::StorageStatus,
    ) -> Result<Option<String>, awa_model::AwaError> {
        let prepared_schema = || {
            status
                .details
                .get("schema")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("awa_exp")
                .to_string()
        };

        match status.state.as_str() {
            "prepared" if status.prepared_engine.as_deref() == Some("queue_storage") => {
                Ok(Some(prepared_schema()))
            }
            "mixed_transition" | "active" if status.active_engine == "queue_storage" => {
                Ok(Some(prepared_schema()))
            }
            "canonical" if status.prepared_engine.as_deref() == Some("queue_storage") => {
                Ok(Some(prepared_schema()))
            }
            "mixed_transition" | "active" => Err(awa_model::AwaError::Validation(format!(
                "unsupported active storage engine '{}'",
                status.active_engine
            ))),
            _ => Ok(None),
        }
    }

    async fn resolve_effective_storage(&self) -> Result<RuntimeStorage, awa_model::AwaError> {
        let Some(runtime) = self.storage.queue_storage() else {
            return Ok(RuntimeStorage::Canonical);
        };

        let status = transition::status(&self.pool).await?;
        let expected_schema = Self::expected_queue_storage_schema(&status)?;
        let prepared_schema_ready = if let Some(schema) = expected_schema.as_deref() {
            if runtime.store.schema() != schema {
                return Err(awa_model::AwaError::Validation(format!(
                    "queue storage runtime configured for schema '{}' but transition state requires '{}'",
                    runtime.store.schema(),
                    schema
                )));
            }
            transition::queue_storage_schema_ready(&self.pool, schema).await?
        } else {
            false
        };

        match self.transition_role {
            TransitionWorkerRole::CanonicalDrain => Ok(RuntimeStorage::Canonical),
            TransitionWorkerRole::QueueStorageTarget => {
                let schema = expected_schema.ok_or_else(|| {
                    awa_model::AwaError::Validation(
                        "queue_storage_target requires a prepared queue-storage schema".into(),
                    )
                })?;
                if !prepared_schema_ready {
                    return Err(awa_model::AwaError::Validation(format!(
                        "queue storage schema '{schema}' is not prepared; run schema preparation before starting queue-storage-target runtimes"
                    )));
                }
                Ok(RuntimeStorage::QueueStorage(runtime.clone()))
            }
            TransitionWorkerRole::Auto => {
                if let Some(schema) = expected_schema.as_deref() {
                    if !prepared_schema_ready {
                        return Err(awa_model::AwaError::Validation(format!(
                            "queue storage schema '{schema}' is not prepared; run schema preparation before starting 0.6 runtimes"
                        )));
                    }
                }

                if matches!(status.state.as_str(), "mixed_transition" | "active")
                    && status.active_engine == "queue_storage"
                {
                    Ok(RuntimeStorage::QueueStorage(runtime.clone()))
                } else {
                    Ok(RuntimeStorage::Canonical)
                }
            }
        }
    }

    fn declared_queue_descriptors(&self) -> Vec<NamedQueueDescriptor> {
        self.queues
            .iter()
            .map(|(queue, _)| NamedQueueDescriptor {
                queue: queue.clone(),
                descriptor: self
                    .queue_descriptors
                    .get(queue)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect()
    }

    fn declared_job_kind_descriptors(&self) -> Vec<NamedJobKindDescriptor> {
        let mut kinds: Vec<String> = self.workers.keys().cloned().collect();
        for kind in self.job_kind_descriptors.keys() {
            if !kinds.iter().any(|existing| existing == kind) {
                kinds.push(kind.clone());
            }
        }
        kinds.sort();

        kinds
            .into_iter()
            .map(|kind| NamedJobKindDescriptor {
                descriptor: self
                    .job_kind_descriptors
                    .get(&kind)
                    .cloned()
                    .unwrap_or_default(),
                kind,
            })
            .collect()
    }

    fn runtime_reporter_state(&self) -> RuntimeReporterState {
        RuntimeReporterState {
            pool: self.pool.clone(),
            queues: self.queues.clone(),
            queue_descriptors: self.queue_descriptors.clone(),
            job_kind_descriptors: self.job_kind_descriptors.clone(),
            worker_kinds: self.workers.keys().cloned().collect(),
            queue_in_flight: self.queue_in_flight.clone(),
            dispatcher_alive: self.dispatcher_alive.clone(),
            heartbeat_alive: self.heartbeat_alive.clone(),
            maintenance_alive: self.maintenance_alive.clone(),
            leader: self.leader.clone(),
            dispatch_cancel: self.dispatch_cancel.clone(),
            overflow_pool: self.overflow_pool.clone(),
            global_max_workers: self.global_max_workers,
            instance_id: self.runtime_instance_id,
            started_at: self.runtime_started_at,
            hostname: self.runtime_hostname.clone(),
            pid: self.runtime_pid,
            version: self.runtime_version,
            snapshot_interval: self.runtime_snapshot_interval,
            effective_storage: self.effective_storage.clone(),
            queue_storage_capable: self.storage.queue_storage().is_some(),
            metrics: self.metrics.clone(),
        }
    }

    async fn publish_runtime_snapshot(&self) {
        let reporter = self.runtime_reporter_state();
        reporter.publish_snapshot().await;
    }

    async fn log_transition_startup_status(
        &self,
        effective_storage: &RuntimeStorage,
    ) -> Result<(), awa_model::AwaError> {
        if self.storage.queue_storage().is_none() {
            return Ok(());
        }

        let report = transition::status_report(&self.pool).await?;
        let effective_engine = match effective_storage {
            RuntimeStorage::Canonical => "canonical",
            RuntimeStorage::QueueStorage(_) => "queue_storage",
        };

        info!(
            transition_role = ?self.transition_role,
            state = %report.status.state,
            current_engine = %report.status.current_engine,
            active_engine = %report.status.active_engine,
            prepared_engine = ?report.status.prepared_engine,
            effective_engine,
            canonical_live_backlog = report.canonical_live_backlog,
            "Resolved storage transition state for worker startup"
        );

        if report.status.state == "prepared" && !report.can_enter_mixed_transition {
            warn!(
                blockers = %report.enter_mixed_transition_blockers.join("; "),
                "Storage transition is prepared but cannot yet enter mixed transition"
            );
        }

        if report.status.state == "mixed_transition" && !report.can_finalize {
            warn!(
                blockers = %report.finalize_blockers.join("; "),
                "Storage transition is in mixed_transition but cannot yet finalize"
            );
        }

        Ok(())
    }

    /// Start the worker runtime. Spawns dispatchers, heartbeat, and maintenance.
    pub async fn start(&self) -> Result<(), awa_model::AwaError> {
        info!(
            queues = self.queues.len(),
            workers = self.workers.len(),
            "Starting Awa worker runtime"
        );

        let effective_storage = self.resolve_effective_storage().await?;
        {
            let mut guard = self.effective_storage.write().await;
            *guard = effective_storage.clone();
        }

        self.log_transition_startup_status(&effective_storage)
            .await?;

        admin::sync_queue_descriptors(
            &self.pool,
            &self.declared_queue_descriptors(),
            self.runtime_snapshot_interval,
        )
        .await?;
        admin::sync_job_kind_descriptors(
            &self.pool,
            &self.declared_job_kind_descriptors(),
            self.runtime_snapshot_interval,
        )
        .await?;

        // Completion batcher stays alive during drain so tasks can release
        // only after their completion has been acknowledged.
        let (completion_batcher, completion_handle) = CompletionBatcher::new(
            self.pool.clone(),
            self.service_cancel.clone(),
            self.metrics.clone(),
            effective_storage.clone(),
        );

        // Create executor with metrics
        let executor = Arc::new(JobExecutor::new(
            self.pool.clone(),
            self.workers.clone(),
            self.lifecycle_handlers.clone(),
            self.in_flight.clone(),
            self.queue_in_flight.clone(),
            self.state.clone(),
            self.metrics.clone(),
            completion_handle,
            effective_storage.clone(),
            self.dlq_policy.clone(),
        ));

        let mut service_handles = self.service_handles.write().await;

        service_handles.extend(completion_batcher.spawn());

        // Start heartbeat service (uses service_cancel — stays alive during drain)
        let heartbeat = HeartbeatService::new(
            self.pool.clone(),
            self.storage.clone(),
            self.in_flight.clone(),
            self.heartbeat_interval,
            self.heartbeat_alive.clone(),
            self.service_cancel.clone(),
            self.metrics.clone(),
        );
        service_handles.push(tokio::spawn(async move {
            heartbeat.run().await;
        }));

        // Start maintenance service (uses service_cancel — stays alive during drain)
        let mut maintenance = MaintenanceService::new(
            self.pool.clone(),
            self.metrics.clone(),
            self.leader.clone(),
            self.maintenance_alive.clone(),
            self.service_cancel.clone(),
            self.periodic_jobs.clone(),
            self.in_flight.clone(),
            effective_storage.clone(),
        )
        .promote_interval(self.promote_interval);
        if let Some(interval) = self.heartbeat_rescue_interval {
            maintenance = maintenance.heartbeat_rescue_interval(interval);
        }
        if let Some(staleness) = self.heartbeat_staleness {
            maintenance = maintenance.heartbeat_staleness(staleness);
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
        if let Some(interval) = self.leader_check_interval {
            maintenance = maintenance.leader_check_interval(interval);
        }
        if let Some(retention) = self.completed_retention {
            maintenance = maintenance.completed_retention(retention);
        }
        if let Some(retention) = self.failed_retention {
            maintenance = maintenance.failed_retention(retention);
        }
        if let Some(retention) = self.descriptor_retention {
            maintenance = maintenance.descriptor_retention(retention);
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
        if let Some(interval) = self.queue_stats_interval {
            maintenance = maintenance.queue_stats_interval(interval);
        }
        if let Some(retention) = self.dlq_retention {
            maintenance = maintenance.dlq_retention(retention);
        }
        if let Some(batch_size) = self.dlq_cleanup_batch_size {
            maintenance = maintenance.dlq_cleanup_batch_size(batch_size);
        }
        maintenance = maintenance.dlq_policy(self.dlq_policy.clone());
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
                    effective_storage.clone(),
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
                    effective_storage.clone(),
                )
            };
            dispatcher_handles.push(tokio::spawn(async move {
                dispatcher.run().await;
            }));
        }

        self.publish_runtime_snapshot().await;

        let reporter = self.runtime_reporter_state();
        service_handles.push(tokio::spawn(async move {
            reporter.run().await;
        }));

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

        self.publish_runtime_snapshot().await;

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
        let maintenance_alive = self.maintenance_alive.load(Ordering::SeqCst);
        let shutting_down = self.dispatch_cancel.is_cancelled();
        let leader = self.leader.load(Ordering::SeqCst);
        let effective_storage = self.effective_storage.read().await.clone();
        let available_rows = if let Some(store) = effective_storage.queue_storage_store() {
            sqlx::query_as::<_, (String, i64)>(&format!(
                r#"
                SELECT ready.queue, count(*)::bigint AS available
                FROM {}.ready_entries AS ready
                JOIN {}.queue_claim_heads AS claims
                  ON claims.queue = ready.queue
                 AND claims.priority = ready.priority
                WHERE ready.lane_seq >= claims.claim_seq
                GROUP BY ready.queue
                "#,
                store.schema(),
                store.schema()
            ))
            .fetch_all(&self.pool)
            .await
            .unwrap_or_default()
        } else {
            sqlx::query_as::<_, (String, i64)>(
                r#"
                SELECT queue, count(*)::bigint AS available
                FROM awa.jobs_hot
                WHERE state = 'available'
                GROUP BY queue
                "#,
            )
            .fetch_all(&self.pool)
            .await
            .unwrap_or_default()
        };
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
            healthy: postgres_connected
                && poll_loop_alive
                && heartbeat_alive
                && maintenance_alive
                && !shutting_down,
            postgres_connected,
            poll_loop_alive,
            heartbeat_alive,
            maintenance_alive,
            shutting_down,
            leader,
            queues,
        }
    }
}

impl RuntimeReporterState {
    async fn storage_capability(&self) -> StorageCapability {
        if !self.queue_storage_capable {
            return StorageCapability::Canonical;
        }

        let effective_storage = self.effective_storage.read().await.clone();
        if matches!(effective_storage, RuntimeStorage::QueueStorage(_)) {
            return StorageCapability::QueueStorage;
        }

        match transition::status(&self.pool).await {
            Ok(status)
                if matches!(status.state.as_str(), "mixed_transition" | "active")
                    && status.active_engine == "queue_storage" =>
            {
                StorageCapability::CanonicalDrainOnly
            }
            Ok(_) => StorageCapability::QueueStorage,
            Err(err) => {
                warn!(
                    error = %err,
                    "Failed to resolve storage transition status for runtime snapshot"
                );
                StorageCapability::QueueStorage
            }
        }
    }

    fn queue_descriptor_hashes(&self) -> HashMap<String, String> {
        self.declared_queue_descriptors()
            .into_iter()
            .map(|named| (named.queue, named.descriptor.descriptor_hash()))
            .collect()
    }

    fn job_kind_descriptor_hashes(&self) -> HashMap<String, String> {
        self.declared_job_kind_descriptors()
            .into_iter()
            .map(|named| (named.kind, named.descriptor.descriptor_hash()))
            .collect()
    }

    fn declared_queue_descriptors(&self) -> Vec<NamedQueueDescriptor> {
        self.queues
            .iter()
            .map(|(queue, _)| NamedQueueDescriptor {
                queue: queue.clone(),
                descriptor: self
                    .queue_descriptors
                    .get(queue)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect()
    }

    fn declared_job_kind_descriptors(&self) -> Vec<NamedJobKindDescriptor> {
        let mut kinds = self.worker_kinds.clone();
        for kind in self.job_kind_descriptors.keys() {
            if !kinds.iter().any(|existing| existing == kind) {
                kinds.push(kind.clone());
            }
        }
        kinds.sort();
        kinds.dedup();

        kinds
            .into_iter()
            .map(|kind| NamedJobKindDescriptor {
                descriptor: self
                    .job_kind_descriptors
                    .get(&kind)
                    .cloned()
                    .unwrap_or_default(),
                kind,
            })
            .collect()
    }

    fn queue_snapshot(&self, queue: &str, config: &QueueConfig) -> QueueRuntimeSnapshot {
        let in_flight = self
            .queue_in_flight
            .get(queue)
            .map(|counter| counter.load(Ordering::SeqCst))
            .unwrap_or(0);

        let (mode, max_workers, min_workers, weight, overflow_held) =
            if let Some(overflow_pool) = &self.overflow_pool {
                (
                    QueueRuntimeMode::Weighted,
                    None,
                    Some(config.min_workers),
                    Some(config.weight),
                    Some(overflow_pool.held(queue)),
                )
            } else {
                (
                    QueueRuntimeMode::HardReserved,
                    Some(config.max_workers),
                    None,
                    None,
                    None,
                )
            };

        QueueRuntimeSnapshot {
            queue: queue.to_string(),
            in_flight,
            overflow_held,
            config: QueueRuntimeConfigSnapshot {
                mode,
                max_workers,
                min_workers,
                weight,
                global_max_workers: self.global_max_workers,
                poll_interval_ms: config.poll_interval.as_millis() as u64,
                deadline_duration_secs: config.deadline_duration.as_secs(),
                priority_aging_interval_secs: config.priority_aging_interval.as_secs(),
                rate_limit: config.rate_limit.as_ref().map(|rl| RateLimitSnapshot {
                    max_rate: rl.max_rate,
                    burst: rl.burst,
                }),
            },
        }
    }

    async fn snapshot_input(&self) -> RuntimeSnapshotInput {
        let postgres_connected = sqlx::query("SELECT 1").execute(&self.pool).await.is_ok();
        let poll_loop_alive = self
            .dispatcher_alive
            .values()
            .all(|alive| alive.load(Ordering::SeqCst));
        let heartbeat_alive = self.heartbeat_alive.load(Ordering::SeqCst);
        let maintenance_alive = self.maintenance_alive.load(Ordering::SeqCst);
        let shutting_down = self.dispatch_cancel.is_cancelled();
        let leader = self.leader.load(Ordering::SeqCst);
        let healthy = postgres_connected
            && poll_loop_alive
            && heartbeat_alive
            && maintenance_alive
            && !shutting_down;
        let storage_capability = self.storage_capability().await;
        let queues = self
            .queues
            .iter()
            .map(|(queue, config)| self.queue_snapshot(queue, config))
            .collect();

        RuntimeSnapshotInput {
            instance_id: self.instance_id,
            hostname: self.hostname.clone(),
            pid: self.pid,
            version: self.version.to_string(),
            storage_capability,
            started_at: self.started_at,
            snapshot_interval_ms: self.snapshot_interval.as_millis() as i64,
            healthy,
            postgres_connected,
            poll_loop_alive,
            heartbeat_alive,
            maintenance_alive,
            shutting_down,
            leader,
            global_max_workers: self.global_max_workers,
            queues,
            queue_descriptor_hashes: self.queue_descriptor_hashes(),
            job_kind_descriptor_hashes: self.job_kind_descriptor_hashes(),
        }
    }

    async fn publish_snapshot(&self) {
        let queue_descriptors = self.declared_queue_descriptors();
        let kind_descriptors = self.declared_job_kind_descriptors();

        if let Err(err) =
            admin::sync_queue_descriptors(&self.pool, &queue_descriptors, self.snapshot_interval)
                .await
        {
            warn!(error = %err, "Failed to sync queue descriptors");
        }
        if let Err(err) =
            admin::sync_job_kind_descriptors(&self.pool, &kind_descriptors, self.snapshot_interval)
                .await
        {
            warn!(error = %err, "Failed to sync job kind descriptors");
        }

        // Emit OTel info gauges for every declared descriptor. One series per
        // descriptor, value=1, with all descriptor fields as attributes. Panels
        // lift descriptor fields into existing metrics via a Prometheus label
        // join: `awa_job_completed_total * on(awa_job_queue) group_left(awa_queue_display_name) awa_queue_info`.
        for named in &queue_descriptors {
            self.metrics.record_queue_info(
                &named.queue,
                named.descriptor.display_name.as_deref(),
                named.descriptor.description.as_deref(),
                named.descriptor.owner.as_deref(),
                named.descriptor.docs_url.as_deref(),
                &named.descriptor.tags,
            );
        }
        for named in &kind_descriptors {
            self.metrics.record_job_kind_info(
                &named.kind,
                named.descriptor.display_name.as_deref(),
                named.descriptor.description.as_deref(),
                named.descriptor.owner.as_deref(),
                named.descriptor.docs_url.as_deref(),
                &named.descriptor.tags,
            );
        }

        let snapshot = self.snapshot_input().await;
        if let Err(err) = admin::upsert_runtime_snapshot(&self.pool, &snapshot).await {
            warn!(error = %err, "Failed to publish runtime snapshot");
        }

        if self.queue_storage_capable {
            match transition::status_report(&self.pool).await {
                Ok(report) => {
                    self.metrics.record_storage_state(&report.status);
                    self.metrics.record_storage_transition_ready(
                        "enter_mixed_transition",
                        report.can_enter_mixed_transition,
                    );
                    self.metrics
                        .record_storage_transition_ready("finalize", report.can_finalize);
                    self.metrics
                        .record_storage_canonical_live_backlog(report.canonical_live_backlog);

                    for capability in ["canonical", "canonical_drain_only", "queue_storage"] {
                        let count = report
                            .live_runtime_capability_counts
                            .get(capability)
                            .copied()
                            .unwrap_or(0) as i64;
                        self.metrics
                            .record_storage_live_runtime_capability(capability, count);
                    }

                    for (capability, count) in report.live_runtime_capability_counts {
                        if capability != "canonical"
                            && capability != "canonical_drain_only"
                            && capability != "queue_storage"
                        {
                            self.metrics
                                .record_storage_live_runtime_capability(&capability, count as i64);
                        }
                    }
                }
                Err(err) => {
                    warn!(error = %err, "Failed to publish storage transition metrics");
                }
            }
        }
    }

    async fn run(self) {
        let mut interval = tokio::time::interval(self.snapshot_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = self.dispatch_cancel.cancelled() => {
                    self.publish_snapshot().await;
                    break;
                }
                _ = interval.tick() => {
                    self.publish_snapshot().await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use awa_model::{migrations, storage, JobArgs, QueueStorage, QueueStorageConfig};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::PgPool;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{Duration, Instant};
    use tokio::sync::{oneshot, Notify};

    static TEST_MUTEX: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

    fn test_mutex() -> &'static tokio::sync::Mutex<()> {
        TEST_MUTEX.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    fn lazy_pool() -> PgPool {
        PgPoolOptions::new()
            .connect_lazy("postgres://postgres:test@localhost/awa_test")
            .expect("lazy pool should build")
    }

    fn base_database_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
    }

    fn replace_database_name(url: &str, database_name: &str) -> String {
        let (without_query, query_suffix) = match url.split_once('?') {
            Some((prefix, query)) => (prefix, Some(query)),
            None => (url, None),
        };
        let (base, _) = without_query
            .rsplit_once('/')
            .expect("database URL should include a database name");
        let mut out = format!("{base}/{database_name}");
        if let Some(query) = query_suffix {
            out.push('?');
            out.push_str(query);
        }
        out
    }

    fn database_url() -> String {
        std::env::var("DATABASE_URL_WORKER_CLIENT").unwrap_or_else(|_| {
            replace_database_name(&base_database_url(), "awa_test_worker_client")
        })
    }

    async fn ensure_database_exists(url: &str) {
        let database_name = url
            .split_once('?')
            .map(|(prefix, _)| prefix)
            .unwrap_or(url)
            .rsplit_once('/')
            .map(|(_, database_name)| database_name.to_string())
            .expect("database URL should include a database name");
        let admin_url = replace_database_name(url, "postgres");
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .expect("Failed to connect to admin database for client tests");
        let create_sql = format!("CREATE DATABASE {database_name}");
        match sqlx::query(&create_sql).execute(&admin_pool).await {
            Ok(_) => {}
            Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P04") => {}
            Err(err) => panic!("Failed to create client test database {database_name}: {err}"),
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
            .expect("Failed to connect to client test database")
    }

    async fn reset_schema(pool: &PgPool) {
        sqlx::raw_sql("DROP SCHEMA IF EXISTS awa CASCADE")
            .execute(pool)
            .await
            .expect("Failed to drop awa schema");
    }

    async fn apply_migrations_through(pool: &PgPool, version: i32) {
        for (_version, _desc, sql) in migrations::migration_sql_range(0, version) {
            sqlx::raw_sql(&sql).execute(pool).await.unwrap();
        }
    }

    async fn drop_queue_storage_schema(pool: &PgPool, schema: &str) {
        let sql = format!("DROP SCHEMA IF EXISTS {schema} CASCADE");
        sqlx::query(&sql)
            .execute(pool)
            .await
            .expect("Failed to drop queue storage schema");
    }

    async fn insert_available_job(pool: &PgPool, kind: &str, queue: &str) -> i64 {
        sqlx::query_scalar(
            r#"
            INSERT INTO awa.jobs (
                kind,
                queue,
                args,
                state,
                priority,
                max_attempts,
                run_at,
                metadata,
                tags
            )
            VALUES (
                $1,
                $2,
                '{}'::jsonb,
                'available'::awa.job_state,
                2,
                25,
                clock_timestamp(),
                '{}'::jsonb,
                '{}'::text[]
            )
            RETURNING id
            "#,
        )
        .bind(kind)
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("Failed to insert job")
    }

    async fn insert_canonical_available_job(pool: &PgPool, kind: &str, queue: &str) -> i64 {
        sqlx::query_scalar(
            r#"
            INSERT INTO awa.jobs_hot (
                kind,
                queue,
                args,
                state,
                priority,
                max_attempts,
                run_at,
                metadata,
                tags
            )
            VALUES (
                $1,
                $2,
                '{}'::jsonb,
                'available'::awa.job_state,
                2,
                25,
                clock_timestamp(),
                '{}'::jsonb,
                '{}'::text[]
            )
            RETURNING id
            "#,
        )
        .bind(kind)
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("Failed to insert canonical job")
    }

    async fn active_queue_storage_schema(pool: &PgPool) -> Option<String> {
        sqlx::query_scalar("SELECT awa.active_queue_storage_schema()")
            .fetch_one(pool)
            .await
            .expect("Failed to fetch active queue storage schema")
    }

    async fn wait_for_runtime_capability(
        pool: &PgPool,
        instance_id: Uuid,
        capability: StorageCapability,
        timeout: Duration,
    ) {
        let start = Instant::now();
        loop {
            let current: Option<String> = sqlx::query_scalar(
                "SELECT storage_capability FROM awa.runtime_instances WHERE instance_id = $1",
            )
            .bind(instance_id)
            .fetch_optional(pool)
            .await
            .expect("Failed to fetch runtime storage capability");
            if current.as_deref() == Some(capability.as_str()) {
                return;
            }
            assert!(
                start.elapsed() <= timeout,
                "Timed out waiting for runtime {instance_id} to report capability {}; last={current:?}",
                capability.as_str()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn expire_runtime_instance(pool: &PgPool, instance_id: Uuid) {
        sqlx::query(
            "UPDATE awa.runtime_instances SET last_seen_at = now() - interval '1 hour' WHERE instance_id = $1",
        )
        .bind(instance_id)
        .execute(pool)
        .await
        .expect("Failed to expire runtime instance");
    }

    async fn prepare_queue_storage_transition(pool: &PgPool, schema: &str) -> QueueStorage {
        let store = QueueStorage::new(QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        })
        .expect("Failed to build queue storage store");
        drop_queue_storage_schema(pool, schema).await;
        store
            .prepare_schema(pool)
            .await
            .expect("Failed to prepare queue storage schema");
        storage::prepare(
            pool,
            "queue_storage",
            serde_json::json!({ "schema": schema }),
        )
        .await
        .expect("Failed to prepare queue storage transition");
        store
    }

    async fn wait_for_state(pool: &PgPool, job_id: i64, state: &str, timeout: Duration) {
        let start = Instant::now();
        loop {
            let current: Option<String> = sqlx::query_scalar(
                "SELECT state::text FROM awa.jobs_hot WHERE id = $1 UNION ALL SELECT state::text FROM awa.scheduled_jobs WHERE id = $1 LIMIT 1",
            )
            .bind(job_id)
            .fetch_optional(pool)
            .await
            .expect("Failed to fetch canonical job state");
            if current.as_deref() == Some(state) {
                return;
            }
            assert!(
                start.elapsed() <= timeout,
                "Timed out waiting for job {job_id} to reach state {state}; last_state={current:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_queue_storage_done(
        pool: &PgPool,
        schema: &str,
        job_id: i64,
        timeout: Duration,
    ) {
        let sql = format!(
            "SELECT EXISTS(SELECT 1 FROM {schema}.done_entries WHERE job_id = $1 AND state = 'completed')"
        );
        let start = Instant::now();
        loop {
            let done: bool = sqlx::query_scalar(&sql)
                .bind(job_id)
                .fetch_one(pool)
                .await
                .expect("Failed to query queue storage terminal rows");
            if done {
                return;
            }
            assert!(
                start.elapsed() <= timeout,
                "Timed out waiting for queue storage job {job_id} to complete"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn force_canonical(mut builder: ClientBuilder) -> ClientBuilder {
        builder.storage = RuntimeStorage::Canonical;
        builder.storage_error = None;
        builder
    }

    #[tokio::test]
    async fn queue_storage_target_requires_prepared_schema() {
        let _guard = test_mutex().lock().await;
        let pool = setup_pool(4).await;
        let queue_storage_schema = "awa_cutover_target_requires_prepare";
        reset_schema(&pool).await;
        migrations::run(&pool)
            .await
            .expect("fresh 0.6 schema install should succeed");
        drop_queue_storage_schema(&pool, queue_storage_schema).await;

        storage::prepare(
            &pool,
            "queue_storage",
            serde_json::json!({ "schema": queue_storage_schema }),
        )
        .await
        .expect("Failed to prepare queue storage transition without schema");

        let client = Client::builder(pool.clone())
            .queue(
                "cutover",
                QueueConfig {
                    max_workers: 1,
                    poll_interval: Duration::from_millis(25),
                    ..QueueConfig::default()
                },
            )
            .queue_storage(
                QueueStorageConfig {
                    schema: queue_storage_schema.to_string(),
                    queue_slot_count: 4,
                    lease_slot_count: 2,
                    ..Default::default()
                },
                Duration::from_millis(1_000),
                Duration::from_millis(50),
            )
            .transition_role(TransitionWorkerRole::QueueStorageTarget)
            .register::<CutoverShortJob, _, _>(move |_args, _ctx| async move {
                Ok(JobResult::Completed)
            })
            .build()
            .expect("Failed to build queue-storage target client");

        let err = client
            .start()
            .await
            .expect_err("queue-storage target should refuse to start without prepared schema");
        match err {
            awa_model::AwaError::Validation(msg) => {
                assert!(
                    msg.contains("not prepared"),
                    "unexpected validation message: {msg}"
                );
            }
            other => panic!("expected Validation error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn queue_descriptor_requires_declared_queue() {
        let result = Client::builder(lazy_pool())
            .queue("default", QueueConfig::default())
            .queue_descriptor("billing", QueueDescriptor::new().display_name("Billing"))
            .build();

        assert!(matches!(
            result,
            Err(BuildError::QueueDescriptorWithoutQueue { queue }) if queue == "billing"
        ));
    }

    #[tokio::test]
    async fn queue_descriptor_allows_declared_queue() {
        let result = Client::builder(lazy_pool())
            .queue("billing", QueueConfig::default())
            .queue_descriptor("billing", QueueDescriptor::new().display_name("Billing"))
            .build();

        assert!(result.is_ok(), "descriptor for declared queue should build");
    }

    #[tokio::test]
    async fn job_kind_descriptor_allows_registered_kind() {
        #[derive(serde::Serialize, serde::Deserialize, awa_macros::JobArgs)]
        struct TestJob;

        let result = Client::builder(lazy_pool())
            .queue("default", QueueConfig::default())
            .register::<TestJob, _, _>(|_args, _ctx| async { Ok(JobResult::Completed) })
            .job_kind_descriptor::<TestJob>(JobKindDescriptor::new().display_name("Test job"))
            .build();

        assert!(
            result.is_ok(),
            "descriptor for registered kind should build"
        );
    }

    #[tokio::test]
    async fn dlq_cleanup_batch_size_must_be_positive() {
        let result = Client::builder(lazy_pool())
            .queue("default", QueueConfig::default())
            .dlq_cleanup_batch_size(0)
            .build();

        assert!(matches!(result, Err(BuildError::InvalidDlqBatchSize)));
    }

    #[tokio::test]
    async fn health_check_reads_available_from_active_queue_storage() {
        let _guard = test_mutex().lock().await;
        let pool = setup_pool(4).await;
        reset_schema(&pool).await;
        migrations::run(&pool)
            .await
            .expect("migrations should succeed");
        drop_queue_storage_schema(&pool, "awa_exp").await;

        let queue = "health_queue_storage";
        let client = Client::builder(pool.clone())
            .queue(queue, QueueConfig::default())
            .build()
            .expect("queue-storage health client should build");

        let store = client
            .storage
            .queue_storage_store()
            .expect("client should default to queue storage");
        store
            .install(&pool)
            .await
            .expect("queue storage install should succeed");

        insert_available_job(&pool, "cutover_short_job", queue).await;

        let health = client.health_check().await;
        let queue_health = health
            .queues
            .get(queue)
            .expect("queue should appear in health");
        assert_eq!(queue_health.available, 1);
    }

    #[derive(Clone, serde::Serialize, serde::Deserialize, awa_macros::JobArgs)]
    struct CutoverLongJob {}

    #[derive(Clone, serde::Serialize, serde::Deserialize, awa_macros::JobArgs)]
    struct CutoverShortJob {}

    #[tokio::test]
    async fn canonical_runtime_drains_in_flight_jobs_across_schema_upgrade_before_queue_storage_cutover(
    ) {
        let _guard = test_mutex().lock().await;
        let pool = setup_pool(8).await;
        let queue_storage_schema = "awa_cutover_runtime";
        reset_schema(&pool).await;
        drop_queue_storage_schema(&pool, queue_storage_schema).await;
        apply_migrations_through(&pool, 9).await;

        let long_started_flag = Arc::new(AtomicBool::new(false));
        let (long_started_tx_inner, long_started_rx) = oneshot::channel::<()>();
        let long_started_tx = Arc::new(Mutex::new(Some(long_started_tx_inner)));
        let long_release = Arc::new(Notify::new());
        let canonical_short_seen = Arc::new(AtomicUsize::new(0));
        let queue_storage_short_seen = Arc::new(AtomicUsize::new(0));

        let canonical_client = {
            let started = long_started_flag.clone();
            let started_tx = long_started_tx.clone();
            let release = long_release.clone();
            let canonical_short_seen = canonical_short_seen.clone();
            let builder = Client::builder(pool.clone())
                .queue(
                    "cutover",
                    QueueConfig {
                        max_workers: 2,
                        poll_interval: Duration::from_millis(25),
                        ..QueueConfig::default()
                    },
                )
                .register::<CutoverLongJob, _, _>(move |_args, _ctx| {
                    let started = started.clone();
                    let started_tx = started_tx.clone();
                    let release = release.clone();
                    async move {
                        started.store(true, Ordering::SeqCst);
                        if let Some(tx) =
                            started_tx.lock().expect("long-start mutex poisoned").take()
                        {
                            let _ = tx.send(());
                        }
                        release.notified().await;
                        Ok(JobResult::Completed)
                    }
                })
                .register::<CutoverShortJob, _, _>(move |_args, _ctx| {
                    let canonical_short_seen = canonical_short_seen.clone();
                    async move {
                        canonical_short_seen.fetch_add(1, Ordering::SeqCst);
                        Ok(JobResult::Completed)
                    }
                })
                .promote_interval(Duration::from_millis(25))
                .leader_election_interval(Duration::from_millis(100))
                .leader_check_interval(Duration::from_millis(50))
                .heartbeat_rescue_interval(Duration::from_millis(100))
                .deadline_rescue_interval(Duration::from_millis(100))
                .callback_rescue_interval(Duration::from_millis(100));
            force_canonical(builder)
                .build()
                .expect("Failed to build canonical client")
        };

        canonical_client
            .start()
            .await
            .expect("Failed to start canonical client");

        let long_id =
            insert_available_job(&pool, <CutoverLongJob as JobArgs>::kind(), "cutover").await;
        tokio::time::timeout(Duration::from_secs(5), long_started_rx)
            .await
            .expect("Timed out waiting for long canonical job to start")
            .expect("Long job start signal dropped");
        assert!(
            long_started_flag.load(Ordering::SeqCst),
            "long-running canonical job should be in flight before migration"
        );

        migrations::run(&pool)
            .await
            .expect("Schema upgrade from 0.5.x to 0.6 should succeed during canonical runtime");
        assert_eq!(
            active_queue_storage_schema(&pool).await,
            None,
            "schema upgrade alone must not activate queue storage"
        );

        let canonical_short_id =
            insert_available_job(&pool, <CutoverShortJob as JobArgs>::kind(), "cutover").await;
        let canonical_short_start = Instant::now();
        while canonical_short_seen.load(Ordering::SeqCst) == 0 {
            assert!(
                canonical_short_start.elapsed() <= Duration::from_secs(5),
                "canonical worker stopped processing new jobs after schema upgrade"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        wait_for_state(
            &pool,
            canonical_short_id,
            "completed",
            Duration::from_secs(5),
        )
        .await;

        long_release.notify_waiters();
        wait_for_state(&pool, long_id, "completed", Duration::from_secs(5)).await;
        canonical_client.shutdown(Duration::from_secs(5)).await;
        expire_runtime_instance(&pool, canonical_client.runtime_instance_id).await;

        let _store = prepare_queue_storage_transition(&pool, queue_storage_schema).await;
        assert_eq!(
            active_queue_storage_schema(&pool).await,
            None,
            "prepare alone must not activate queue storage routing"
        );

        let store_config = QueueStorageConfig {
            schema: queue_storage_schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        };
        let drain_only_client = {
            let queue_storage_short_seen = queue_storage_short_seen.clone();
            Client::builder(pool.clone())
                .queue(
                    "cutover",
                    QueueConfig {
                        max_workers: 2,
                        poll_interval: Duration::from_millis(25),
                        ..QueueConfig::default()
                    },
                )
                .queue_storage(
                    store_config.clone(),
                    Duration::from_millis(1_000),
                    Duration::from_millis(50),
                )
                .register::<CutoverShortJob, _, _>(move |_args, _ctx| {
                    let queue_storage_short_seen = queue_storage_short_seen.clone();
                    async move {
                        queue_storage_short_seen.fetch_add(1, Ordering::SeqCst);
                        Ok(JobResult::Completed)
                    }
                })
                .promote_interval(Duration::from_millis(25))
                .leader_election_interval(Duration::from_millis(100))
                .leader_check_interval(Duration::from_millis(50))
                .heartbeat_rescue_interval(Duration::from_millis(100))
                .deadline_rescue_interval(Duration::from_millis(100))
                .callback_rescue_interval(Duration::from_millis(100))
                .runtime_snapshot_interval(Duration::from_millis(100))
                .build()
                .expect("Failed to build queue storage client")
        };

        drain_only_client
            .start()
            .await
            .expect("Failed to start queue storage client");
        wait_for_runtime_capability(
            &pool,
            drain_only_client.runtime_instance_id,
            StorageCapability::QueueStorage,
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(
            active_queue_storage_schema(&pool).await,
            None,
            "prepared queue storage runtime must stay canonical until mixed transition"
        );

        let prepared_short_id =
            insert_available_job(&pool, <CutoverShortJob as JobArgs>::kind(), "cutover").await;
        let queue_storage_start = Instant::now();
        while queue_storage_short_seen.load(Ordering::SeqCst) == 0 {
            assert!(
                queue_storage_start.elapsed() <= Duration::from_secs(5),
                "queue-storage-capable runtime failed to process canonical work before mixed transition"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        wait_for_state(
            &pool,
            prepared_short_id,
            "completed",
            Duration::from_secs(5),
        )
        .await;

        storage::enter_mixed_transition(&pool)
            .await
            .expect("enter_mixed_transition should succeed once only 0.6 workers remain");
        assert_eq!(
            active_queue_storage_schema(&pool).await,
            Some(queue_storage_schema.to_string()),
            "mixed transition should activate queue storage routing"
        );
        wait_for_runtime_capability(
            &pool,
            drain_only_client.runtime_instance_id,
            StorageCapability::CanonicalDrainOnly,
            Duration::from_secs(5),
        )
        .await;

        let canonical_drain_id =
            insert_canonical_available_job(&pool, <CutoverShortJob as JobArgs>::kind(), "cutover")
                .await;
        wait_for_state(
            &pool,
            canonical_drain_id,
            "completed",
            Duration::from_secs(5),
        )
        .await;

        drain_only_client.shutdown(Duration::from_secs(5)).await;

        let queue_storage_client = {
            let queue_storage_short_seen = queue_storage_short_seen.clone();
            Client::builder(pool.clone())
                .queue(
                    "cutover",
                    QueueConfig {
                        max_workers: 2,
                        poll_interval: Duration::from_millis(25),
                        ..QueueConfig::default()
                    },
                )
                .queue_storage(
                    store_config.clone(),
                    Duration::from_millis(1_000),
                    Duration::from_millis(50),
                )
                .register::<CutoverShortJob, _, _>(move |_args, _ctx| {
                    let queue_storage_short_seen = queue_storage_short_seen.clone();
                    async move {
                        queue_storage_short_seen.fetch_add(1, Ordering::SeqCst);
                        Ok(JobResult::Completed)
                    }
                })
                .promote_interval(Duration::from_millis(25))
                .leader_election_interval(Duration::from_millis(100))
                .leader_check_interval(Duration::from_millis(50))
                .heartbeat_rescue_interval(Duration::from_millis(100))
                .deadline_rescue_interval(Duration::from_millis(100))
                .callback_rescue_interval(Duration::from_millis(100))
                .runtime_snapshot_interval(Duration::from_millis(100))
                .build()
                .expect("Failed to build post-transition queue storage client")
        };

        queue_storage_client
            .start()
            .await
            .expect("Failed to start post-transition queue storage client");
        wait_for_runtime_capability(
            &pool,
            queue_storage_client.runtime_instance_id,
            StorageCapability::QueueStorage,
            Duration::from_secs(5),
        )
        .await;

        let before_queue_storage = queue_storage_short_seen.load(Ordering::SeqCst);
        let queue_storage_job_id =
            insert_available_job(&pool, <CutoverShortJob as JobArgs>::kind(), "cutover").await;
        let queue_storage_start = Instant::now();
        while queue_storage_short_seen.load(Ordering::SeqCst) == before_queue_storage {
            assert!(
                queue_storage_start.elapsed() <= Duration::from_secs(5),
                "queue storage runtime failed to process new work after cutover"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        wait_for_queue_storage_done(
            &pool,
            queue_storage_schema,
            queue_storage_job_id,
            Duration::from_secs(5),
        )
        .await;

        queue_storage_client.shutdown(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn queue_storage_target_started_before_mixed_transition_processes_new_work_immediately() {
        let _guard = test_mutex().lock().await;
        let pool = setup_pool(8).await;
        let queue_storage_schema = "awa_cutover_target_runtime";
        reset_schema(&pool).await;
        migrations::run(&pool)
            .await
            .expect("fresh 0.6 schema install should succeed");
        drop_queue_storage_schema(&pool, queue_storage_schema).await;

        let canonical_seen = Arc::new(AtomicUsize::new(0));
        let queue_storage_seen = Arc::new(AtomicUsize::new(0));
        let store_config = QueueStorageConfig {
            schema: queue_storage_schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        };

        prepare_queue_storage_transition(&pool, queue_storage_schema).await;
        assert_eq!(
            active_queue_storage_schema(&pool).await,
            None,
            "prepare should not activate queue storage routing"
        );

        let auto_client = {
            let canonical_seen = canonical_seen.clone();
            Client::builder(pool.clone())
                .queue(
                    "cutover",
                    QueueConfig {
                        max_workers: 2,
                        poll_interval: Duration::from_millis(25),
                        ..QueueConfig::default()
                    },
                )
                .queue_storage(
                    store_config.clone(),
                    Duration::from_millis(1_000),
                    Duration::from_millis(50),
                )
                .register::<CutoverShortJob, _, _>(move |_args, _ctx| {
                    let canonical_seen = canonical_seen.clone();
                    async move {
                        canonical_seen.fetch_add(1, Ordering::SeqCst);
                        Ok(JobResult::Completed)
                    }
                })
                .promote_interval(Duration::from_millis(25))
                .leader_election_interval(Duration::from_millis(100))
                .leader_check_interval(Duration::from_millis(50))
                .heartbeat_rescue_interval(Duration::from_millis(100))
                .deadline_rescue_interval(Duration::from_millis(100))
                .callback_rescue_interval(Duration::from_millis(100))
                .runtime_snapshot_interval(Duration::from_millis(100))
                .build()
                .expect("Failed to build auto cutover client")
        };
        auto_client
            .start()
            .await
            .expect("Failed to start auto cutover client");

        let target_client = {
            let queue_storage_seen = queue_storage_seen.clone();
            Client::builder(pool.clone())
                .queue(
                    "cutover",
                    QueueConfig {
                        max_workers: 2,
                        poll_interval: Duration::from_millis(25),
                        ..QueueConfig::default()
                    },
                )
                .queue_storage(
                    store_config.clone(),
                    Duration::from_millis(1_000),
                    Duration::from_millis(50),
                )
                .transition_role(TransitionWorkerRole::QueueStorageTarget)
                .register::<CutoverShortJob, _, _>(move |_args, _ctx| {
                    let queue_storage_seen = queue_storage_seen.clone();
                    async move {
                        queue_storage_seen.fetch_add(1, Ordering::SeqCst);
                        Ok(JobResult::Completed)
                    }
                })
                .promote_interval(Duration::from_millis(25))
                .leader_election_interval(Duration::from_millis(100))
                .leader_check_interval(Duration::from_millis(50))
                .heartbeat_rescue_interval(Duration::from_millis(100))
                .deadline_rescue_interval(Duration::from_millis(100))
                .callback_rescue_interval(Duration::from_millis(100))
                .runtime_snapshot_interval(Duration::from_millis(100))
                .build()
                .expect("Failed to build queue-storage target client")
        };
        target_client
            .start()
            .await
            .expect("Failed to start queue-storage target client");

        wait_for_runtime_capability(
            &pool,
            auto_client.runtime_instance_id,
            StorageCapability::QueueStorage,
            Duration::from_secs(5),
        )
        .await;
        wait_for_runtime_capability(
            &pool,
            target_client.runtime_instance_id,
            StorageCapability::QueueStorage,
            Duration::from_secs(5),
        )
        .await;

        let canonical_job_id =
            insert_available_job(&pool, <CutoverShortJob as JobArgs>::kind(), "cutover").await;
        let canonical_start = Instant::now();
        while canonical_seen.load(Ordering::SeqCst) == 0 {
            assert!(
                canonical_start.elapsed() <= Duration::from_secs(5),
                "auto client failed to process canonical work before mixed transition"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        wait_for_state(&pool, canonical_job_id, "completed", Duration::from_secs(5)).await;
        assert_eq!(
            queue_storage_seen.load(Ordering::SeqCst),
            0,
            "queue-storage target should stay idle before routing flips"
        );

        storage::enter_mixed_transition(&pool)
            .await
            .expect("enter_mixed_transition should succeed with prepared 0.6 fleet");
        wait_for_runtime_capability(
            &pool,
            auto_client.runtime_instance_id,
            StorageCapability::CanonicalDrainOnly,
            Duration::from_secs(5),
        )
        .await;
        wait_for_runtime_capability(
            &pool,
            target_client.runtime_instance_id,
            StorageCapability::QueueStorage,
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(
            active_queue_storage_schema(&pool).await,
            Some(queue_storage_schema.to_string()),
            "mixed transition should activate queue storage routing"
        );

        let before_queue_storage = queue_storage_seen.load(Ordering::SeqCst);
        let queue_storage_job_id =
            insert_available_job(&pool, <CutoverShortJob as JobArgs>::kind(), "cutover").await;
        let queue_storage_start = Instant::now();
        while queue_storage_seen.load(Ordering::SeqCst) == before_queue_storage {
            assert!(
                queue_storage_start.elapsed() <= Duration::from_secs(5),
                "queue-storage target failed to process new work after routing flip"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        wait_for_queue_storage_done(
            &pool,
            queue_storage_schema,
            queue_storage_job_id,
            Duration::from_secs(5),
        )
        .await;

        let canonical_drain_id =
            insert_canonical_available_job(&pool, <CutoverShortJob as JobArgs>::kind(), "cutover")
                .await;
        wait_for_state(
            &pool,
            canonical_drain_id,
            "completed",
            Duration::from_secs(5),
        )
        .await;

        target_client.shutdown(Duration::from_secs(5)).await;
        auto_client.shutdown(Duration::from_secs(5)).await;
    }
}
