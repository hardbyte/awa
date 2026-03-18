use crate::dispatcher::{Dispatcher, QueueConfig};
use crate::executor::{BoxedWorker, JobError, JobExecutor, JobResult, Worker};
use crate::heartbeat::HeartbeatService;
use crate::maintenance::MaintenanceService;
use awa_model::{JobArgs, JobRow, PeriodicJob};
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Errors returned when building a worker client.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    #[error("at least one queue must be configured")]
    NoQueuesConfigured,
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
    pub max_workers: u32,
    pub available: u64,
}

/// Builder for the Awa worker client.
pub struct ClientBuilder {
    pool: PgPool,
    queues: Vec<(String, QueueConfig)>,
    workers: HashMap<String, BoxedWorker>,
    state: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    heartbeat_interval: Duration,
    periodic_jobs: Vec<PeriodicJob>,
}

impl ClientBuilder {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            queues: Vec::new(),
            workers: HashMap::new(),
            state: HashMap::new(),
            heartbeat_interval: Duration::from_secs(30),
            periodic_jobs: Vec::new(),
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
            periodic_jobs: Arc::new(self.periodic_jobs),
            cancel: CancellationToken::new(),
            handles: RwLock::new(Vec::new()),
            in_flight: Arc::new(RwLock::new(HashMap::new())),
            queue_in_flight,
            dispatcher_alive,
            heartbeat_alive: Arc::new(AtomicBool::new(false)),
            leader: Arc::new(AtomicBool::new(false)),
            metrics,
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
    periodic_jobs: Arc<Vec<PeriodicJob>>,
    cancel: CancellationToken,
    handles: RwLock<Vec<tokio::task::JoinHandle<()>>>,
    in_flight: Arc<RwLock<HashMap<i64, Arc<AtomicBool>>>>,
    queue_in_flight: Arc<HashMap<String, Arc<AtomicU32>>>,
    dispatcher_alive: Arc<HashMap<String, Arc<AtomicBool>>>,
    heartbeat_alive: Arc<AtomicBool>,
    leader: Arc<AtomicBool>,
    metrics: crate::metrics::AwaMetrics,
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

        // Create executor with metrics
        let executor = Arc::new(JobExecutor::new(
            self.pool.clone(),
            self.workers.clone(),
            self.in_flight.clone(),
            self.queue_in_flight.clone(),
            self.state.clone(),
            self.metrics.clone(),
        ));

        let mut handles = self.handles.write().await;

        // Start heartbeat service
        let heartbeat = HeartbeatService::new(
            self.pool.clone(),
            self.in_flight.clone(),
            self.heartbeat_interval,
            self.heartbeat_alive.clone(),
            self.cancel.clone(),
        );
        handles.push(tokio::spawn(async move {
            heartbeat.run().await;
        }));

        // Start maintenance service
        let maintenance = MaintenanceService::new(
            self.pool.clone(),
            self.leader.clone(),
            self.cancel.clone(),
            self.periodic_jobs.clone(),
        );
        handles.push(tokio::spawn(async move {
            maintenance.run().await;
        }));

        // Start a dispatcher per queue
        for (queue_name, config) in &self.queues {
            let dispatcher = Dispatcher::new(
                queue_name.clone(),
                config.clone(),
                self.pool.clone(),
                executor.clone(),
                self.in_flight.clone(),
                self.dispatcher_alive
                    .get(queue_name)
                    .cloned()
                    .unwrap_or_else(|| Arc::new(AtomicBool::new(false))),
                self.cancel.clone(),
            );
            handles.push(tokio::spawn(async move {
                dispatcher.run().await;
            }));
        }

        info!("Awa worker runtime started");
        Ok(())
    }

    /// Graceful shutdown with drain timeout.
    pub async fn shutdown(&self, timeout: Duration) {
        info!("Initiating graceful shutdown");

        // Signal all tasks to stop
        self.cancel.cancel();
        {
            let guard = self.in_flight.read().await;
            for flag in guard.values() {
                flag.store(true, Ordering::SeqCst);
            }
        }

        // Wait for handles with timeout
        let handles: Vec<_> = {
            let mut guard = self.handles.write().await;
            std::mem::take(&mut *guard)
        };

        let shutdown_future = async {
            for handle in handles {
                let _ = handle.await;
            }
        };

        if tokio::time::timeout(timeout, shutdown_future)
            .await
            .is_err()
        {
            warn!(
                timeout_secs = timeout.as_secs(),
                "Shutdown timeout exceeded, some tasks may not have completed"
            );
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
        let shutting_down = self.cancel.is_cancelled();
        let leader = self.leader.load(Ordering::SeqCst);
        let available_rows = sqlx::query_as::<_, (String, i64)>(
            r#"
            SELECT queue, count(*)::bigint AS available
            FROM awa.jobs
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
                (
                    queue.clone(),
                    QueueHealth {
                        in_flight,
                        max_workers: config.max_workers,
                        available,
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
