use crate::dispatcher::{Dispatcher, QueueConfig};
use crate::executor::{BoxedWorker, JobError, JobExecutor, JobResult, Worker};
use crate::heartbeat::HeartbeatService;
use crate::maintenance::MaintenanceService;
use awa_model::{JobArgs, JobRow};
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

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
}

impl ClientBuilder {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            queues: Vec::new(),
            workers: HashMap::new(),
            state: HashMap::new(),
            heartbeat_interval: Duration::from_secs(30),
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

    /// Build the client.
    pub fn build(self) -> Client {
        if self.queues.is_empty() {
            panic!("At least one queue must be configured");
        }

        let metrics = crate::metrics::AwaMetrics::from_global();

        Client {
            pool: self.pool,
            queues: self.queues,
            workers: Arc::new(self.workers),
            state: Arc::new(self.state),
            heartbeat_interval: self.heartbeat_interval,
            cancel: CancellationToken::new(),
            handles: RwLock::new(Vec::new()),
            metrics,
        }
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
    cancel: CancellationToken,
    handles: RwLock<Vec<tokio::task::JoinHandle<()>>>,
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

        let in_flight: Arc<RwLock<HashSet<i64>>> = Arc::new(RwLock::new(HashSet::new()));

        // Create executor with metrics
        let executor = Arc::new(JobExecutor::new(
            self.pool.clone(),
            self.workers.clone(),
            in_flight.clone(),
            self.state.clone(),
            self.metrics.clone(),
        ));

        let mut handles = self.handles.write().await;

        // Start heartbeat service
        let heartbeat = HeartbeatService::new(
            self.pool.clone(),
            in_flight.clone(),
            self.heartbeat_interval,
            self.cancel.clone(),
        );
        handles.push(tokio::spawn(async move {
            heartbeat.run().await;
        }));

        // Start maintenance service
        let maintenance = MaintenanceService::new(self.pool.clone(), self.cancel.clone());
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
                in_flight.clone(),
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

        let shutting_down = self.cancel.is_cancelled();

        HealthCheck {
            healthy: postgres_connected && !shutting_down,
            postgres_connected,
            poll_loop_alive: !shutting_down,
            heartbeat_alive: !shutting_down,
            shutting_down,
            leader: false,          // Would need to check advisory lock
            queues: HashMap::new(), // Would aggregate from dispatchers
        }
    }
}
