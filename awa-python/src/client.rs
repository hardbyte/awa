use crate::args::{derive_kind, get_type_class_name, serialize_args};
use crate::errors::{map_awa_error, map_connect_error, map_sqlx_error, state_error};
use crate::job::{py_to_json, PyJob};
use crate::transaction::{
    insert_raw_job, parse_run_at, parse_unique_opts, PySyncTransaction, PyTransaction,
};
use crate::worker::PythonWorker;
use awa_model::admin::{JobKindDescriptor, ListJobsFilter, QueueDescriptor};
use awa_model::{
    InsertOpts, InsertParams, JobState, PeriodicJob, QueueStorage, QueueStorageConfig,
};
use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

fn validate_timeout_seconds(timeout_seconds: f64) -> PyResult<Duration> {
    if !timeout_seconds.is_finite() || timeout_seconds.is_sign_negative() {
        return Err(map_awa_error(awa_model::AwaError::Validation(
            "timeout_seconds must be a finite, non-negative number".into(),
        )));
    }

    Ok(Duration::from_secs_f64(timeout_seconds))
}

fn build_queue_descriptor(
    py: Python<'_>,
    display_name: Option<String>,
    description: Option<String>,
    owner: Option<String>,
    docs_url: Option<String>,
    tags: Option<Vec<String>>,
    extra: Option<Py<PyAny>>,
) -> PyResult<QueueDescriptor> {
    let mut descriptor = QueueDescriptor::new();
    if let Some(value) = display_name {
        descriptor = descriptor.display_name(value);
    }
    if let Some(value) = description {
        descriptor = descriptor.description(value);
    }
    if let Some(value) = owner {
        descriptor = descriptor.owner(value);
    }
    if let Some(value) = docs_url {
        descriptor = descriptor.docs_url(value);
    }
    if let Some(value) = tags {
        descriptor = descriptor.tags(value);
    }
    if let Some(value) = extra {
        descriptor = descriptor.extra(py_to_json(py, value.bind(py))?);
    }
    Ok(descriptor)
}

fn build_job_kind_descriptor(
    py: Python<'_>,
    display_name: Option<String>,
    description: Option<String>,
    owner: Option<String>,
    docs_url: Option<String>,
    tags: Option<Vec<String>>,
    extra: Option<Py<PyAny>>,
) -> PyResult<JobKindDescriptor> {
    let mut descriptor = JobKindDescriptor::new();
    if let Some(value) = display_name {
        descriptor = descriptor.display_name(value);
    }
    if let Some(value) = description {
        descriptor = descriptor.description(value);
    }
    if let Some(value) = owner {
        descriptor = descriptor.owner(value);
    }
    if let Some(value) = docs_url {
        descriptor = descriptor.docs_url(value);
    }
    if let Some(value) = tags {
        descriptor = descriptor.tags(value);
    }
    if let Some(value) = extra {
        descriptor = descriptor.extra(py_to_json(py, value.bind(py))?);
    }
    Ok(descriptor)
}

/// Python result types for worker handlers.
#[pyclass(frozen, name = "RetryAfter", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyRetryAfter {
    #[pyo3(get)]
    pub seconds: f64,
}

#[pymethods]
impl PyRetryAfter {
    #[new]
    fn new(seconds: f64) -> Self {
        Self { seconds }
    }
}

#[pyclass(frozen, name = "Snooze", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PySnooze {
    #[pyo3(get)]
    pub seconds: f64,
}

#[pymethods]
impl PySnooze {
    #[new]
    fn new(seconds: f64) -> Self {
        Self { seconds }
    }
}

#[pyclass(frozen, name = "Cancel", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyCancel {
    #[pyo3(get)]
    pub reason: String,
}

#[pymethods]
impl PyCancel {
    #[new]
    #[pyo3(signature = (reason="cancelled by handler".to_string()))]
    fn new(reason: String) -> Self {
        Self { reason }
    }
}

/// Signal that the job should be parked to wait for an external callback.
///
/// Pass the token returned by `job.register_callback()`.
#[pyclass(frozen, name = "WaitForCallback", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyWaitForCallback {
    #[pyo3(get)]
    pub callback_id: String,
}

#[pymethods]
impl PyWaitForCallback {
    #[new]
    fn new(py: Python<'_>, token: Py<crate::job::PyCallbackToken>) -> Self {
        let token = token.bind(py).borrow();
        Self {
            callback_id: token.id.clone(),
        }
    }
}

/// Result of `resolve_callback`.
#[pyclass(frozen, name = "ResolveResult", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyResolveResult {
    /// One of "completed", "failed", "ignored".
    #[pyo3(get)]
    pub outcome: String,
    /// The job row (None when ignored).
    pub job: Option<PyJob>,
    /// Transformed payload (only for completed).
    pub payload_json: Option<serde_json::Value>,
    /// Why the callback was ignored (only for ignored).
    #[pyo3(get)]
    pub reason: Option<String>,
}

#[pymethods]
impl PyResolveResult {
    #[getter]
    fn job(&self, _py: Python<'_>) -> PyResult<Option<PyJob>> {
        Ok(self.job.clone())
    }

    #[getter]
    fn payload(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.payload_json {
            Some(v) => crate::job::json_to_py(py, v),
            None => Ok(py.None()),
        }
    }

    fn is_completed(&self) -> bool {
        self.outcome == "completed"
    }

    fn is_failed(&self) -> bool {
        self.outcome == "failed"
    }

    fn is_ignored(&self) -> bool {
        self.outcome == "ignored"
    }

    fn __repr__(&self) -> String {
        format!("ResolveResult(outcome='{}')", self.outcome)
    }
}

#[pyclass(frozen, name = "QueueHealth", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyQueueHealth {
    #[pyo3(get)]
    pub in_flight: u32,
    #[pyo3(get)]
    pub available: u64,
    /// Hard-reserved mode only: maximum workers for this queue.
    #[pyo3(get)]
    pub max_workers: Option<u32>,
    /// Weighted mode only: minimum guaranteed workers.
    #[pyo3(get)]
    pub min_workers: Option<u32>,
    /// Weighted mode only: queue weight for overflow allocation.
    #[pyo3(get)]
    pub weight: Option<u32>,
    /// Weighted mode only: current overflow permits held.
    #[pyo3(get)]
    pub overflow_held: Option<u32>,
}

#[pyclass(frozen, name = "QueueStat", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyQueueStat {
    #[pyo3(get)]
    pub queue: String,
    #[pyo3(get)]
    pub total_queued: i64,
    #[pyo3(get)]
    pub scheduled: i64,
    #[pyo3(get)]
    pub available: i64,
    #[pyo3(get)]
    pub retryable: i64,
    #[pyo3(get)]
    pub running: i64,
    #[pyo3(get)]
    pub failed: i64,
    #[pyo3(get)]
    pub waiting_external: i64,
    #[pyo3(get)]
    pub completed_last_hour: i64,
    #[pyo3(get)]
    pub lag_seconds: Option<f64>,
    #[pyo3(get)]
    pub paused: bool,
}

#[pymethods]
impl PyQueueStat {
    fn __repr__(&self) -> String {
        format!(
            "QueueStat(queue='{}', total_queued={}, available={}, running={}, failed={})",
            self.queue, self.total_queued, self.available, self.running, self.failed
        )
    }
}

#[pyclass(frozen, name = "HealthCheck", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyHealthCheck {
    #[pyo3(get)]
    pub healthy: bool,
    #[pyo3(get)]
    pub postgres_connected: bool,
    #[pyo3(get)]
    pub poll_loop_alive: bool,
    #[pyo3(get)]
    pub heartbeat_alive: bool,
    #[pyo3(get)]
    pub shutting_down: bool,
    #[pyo3(get)]
    pub leader: bool,
    queues: HashMap<String, PyQueueHealth>,
}

#[pymethods]
impl PyHealthCheck {
    #[getter]
    fn queues(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for (queue, health) in &self.queues {
            dict.set_item(queue, Py::new(py, health.clone())?)?;
        }
        Ok(dict.into_any().unbind())
    }
}

/// Worker registration entry.
pub struct WorkerEntry {
    pub kind: String,
    pub handler: Py<PyAny>,
    pub args_type: Py<PyAny>,
    pub queue: String,
    pub task_locals: pyo3_async_runtimes::TaskLocals,
}

impl Clone for WorkerEntry {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            kind: self.kind.clone(),
            handler: self.handler.clone_ref(py),
            args_type: self.args_type.clone_ref(py),
            queue: self.queue.clone(),
            task_locals: self.task_locals.clone(),
        })
    }
}

/// The main Python client.
#[pyclass(name = "Client")]
pub struct PyClient {
    pool: PgPool,
    workers: Arc<RwLock<HashMap<String, WorkerEntry>>>,
    periodic_jobs: Arc<Mutex<Vec<PeriodicJob>>>,
    queue_descriptors: Arc<Mutex<HashMap<String, QueueDescriptor>>>,
    job_kind_descriptors: Arc<Mutex<HashMap<String, JobKindDescriptor>>>,
    runtime: Arc<Mutex<Option<Arc<awa_worker::Client>>>>,
}

#[pymethods]
impl PyClient {
    #[new]
    #[pyo3(signature = (database_url, max_connections=10))]
    fn new(py: Python<'_>, database_url: String, max_connections: u32) -> PyResult<Self> {
        // Release the GIL during pool connect so other Python threads can
        // make progress. The block_on is unavoidable here (__init__ must be
        // sync) but is bounded by the 30s timeout.
        let pool = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                    tokio::time::timeout(
                        Duration::from_secs(30),
                        PgPoolOptions::new()
                            .max_connections(max_connections)
                            .acquire_timeout(Duration::from_secs(30))
                            .connect(&database_url),
                    )
                    .await
                    .map_err(|_| sqlx::Error::PoolTimedOut)?
                })
            })
            .map_err(map_connect_error)?;

        Ok(Self {
            pool,
            workers: Arc::new(RwLock::new(HashMap::new())),
            periodic_jobs: Arc::new(Mutex::new(Vec::new())),
            queue_descriptors: Arc::new(Mutex::new(HashMap::new())),
            job_kind_descriptors: Arc::new(Mutex::new(HashMap::new())),
            runtime: Arc::new(Mutex::new(None)),
        })
    }

    #[pyo3(signature = (args, *, kind=None, queue="default".to_string(), priority=2, max_attempts=25, tags=vec![], metadata=None, run_at=None, unique_opts=None))]
    #[allow(clippy::too_many_arguments)]
    fn insert<'py>(
        &self,
        py: Python<'py>,
        args: Py<PyAny>,
        kind: Option<String>,
        queue: String,
        priority: i16,
        max_attempts: i16,
        tags: Vec<String>,
        metadata: Option<Py<PyAny>>,
        run_at: Option<Py<PyAny>>,
        unique_opts: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (kind_str, args_json, metadata_json, run_at, unique) = Python::attach(|py| {
                let args_bound = args.bind(py);
                let kind_str = match kind {
                    Some(k) => k,
                    None => {
                        let class_name = get_type_class_name(args_bound.get_type().as_any())?;
                        derive_kind(&class_name)
                    }
                };
                let metadata_json = metadata
                    .as_ref()
                    .map(|value| py_to_json(py, value.bind(py)))
                    .transpose()?
                    .unwrap_or(serde_json::json!({}));
                let run_at = run_at
                    .as_ref()
                    .map(|value| parse_run_at(py, value.bind(py)))
                    .transpose()?;
                let unique = unique_opts
                    .as_ref()
                    .map(|value| parse_unique_opts(py, value.bind(py)))
                    .transpose()?;
                Ok::<_, PyErr>((
                    kind_str,
                    serialize_args(py, args_bound)?,
                    metadata_json,
                    run_at,
                    unique,
                ))
            })?;

            let row = insert_raw_job(
                &pool,
                &kind_str,
                &args_json,
                InsertOpts {
                    queue,
                    priority,
                    max_attempts,
                    run_at,
                    metadata: metadata_json,
                    tags,
                    unique,
                    ..Default::default()
                },
            )
            .await
            .map_err(map_awa_error)?;

            Ok(PyJob::from(row))
        })
    }

    /// Close the connection pool, releasing all database connections.
    ///
    /// Call after `shutdown()` to release connections, or call directly
    /// if the client was used only for queries (no workers started).
    /// If workers are running, call `shutdown()` first.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            pool.close().await;
            Ok(())
        })
    }

    fn migrate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        // migrations::run() is !Send (holds PoolConnection across awaits).
        // We bridge it via spawn_blocking + a current_thread runtime so the
        // asyncio event loop stays free and asyncio.wait_for() can cancel.
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build current_thread runtime for migrate");
                rt.block_on(awa_model::migrations::run(&pool))
            })
            .await
            .map_err(|e| state_error(format!("migration task failed: {e}")))?
            .map_err(map_awa_error)?;
            Ok(())
        })
    }

    fn install_queue_storage<'py>(
        &self,
        py: Python<'py>,
        schema: String,
        queue_slot_count: u32,
        lease_slot_count: u32,
        reset: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let store = QueueStorage::new(QueueStorageConfig {
                schema,
                queue_slot_count: queue_slot_count as usize,
                lease_slot_count: lease_slot_count as usize,
            })
            .map_err(map_awa_error)?;
            store.install(&pool).await.map_err(map_awa_error)?;
            if reset {
                store.reset(&pool).await.map_err(map_awa_error)?;
            }
            Ok(())
        })
    }

    fn transaction<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tx = pool.begin().await.map_err(map_sqlx_error)?;
            Ok(PyTransaction::new(tx))
        })
    }

    #[pyo3(signature = (args_type, *, kind=None, queue="default".to_string()))]
    fn worker(
        &self,
        py: Python<'_>,
        args_type: Py<PyAny>,
        kind: Option<String>,
        queue: String,
    ) -> PyResult<Py<PyAny>> {
        let workers = self.workers.clone();
        let kind_str = kind.unwrap_or_else(|| {
            Python::attach(|py| {
                let class_name = get_type_class_name(args_type.bind(py).as_any())
                    .unwrap_or_else(|_| "unknown".to_string());
                derive_kind(&class_name)
            })
        });

        let decorator = pyo3::types::PyCFunction::new_closure(
            py,
            None,
            None,
            move |args: &Bound<'_, pyo3::types::PyTuple>,
                  _kwargs: Option<&Bound<'_, PyDict>>|
                  -> PyResult<Py<PyAny>> {
                let py = args.py();
                let handler = args.get_item(0)?;
                let handler_py = handler.unbind();

                let entry = WorkerEntry {
                    kind: kind_str.clone(),
                    handler: handler_py.clone_ref(py),
                    args_type: args_type.clone_ref(py),
                    queue: queue.clone(),
                    task_locals: pyo3_async_runtimes::tokio::get_current_locals(py)?,
                };

                let workers = workers.clone();
                let kind = kind_str.clone();
                workers
                    .write()
                    .expect("workers lock poisoned")
                    .insert(kind, entry);

                Ok(handler_py)
            },
        )?;

        Ok(decorator.into_any().unbind())
    }

    /// Register a periodic (cron) job schedule.
    ///
    /// The schedule is synced to the database by the leader and evaluated
    /// every second to enqueue jobs when they're due.
    #[pyo3(signature = (name, cron_expr, args_type, args, *, timezone="UTC".to_string(), queue="default".to_string(), priority=2, max_attempts=25, tags=vec![], metadata=None))]
    #[allow(clippy::too_many_arguments)]
    fn periodic(
        &self,
        py: Python<'_>,
        name: String,
        cron_expr: String,
        args_type: Py<PyAny>,
        args: Py<PyAny>,
        timezone: String,
        queue: String,
        priority: i16,
        max_attempts: i16,
        tags: Vec<String>,
        metadata: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let args_bound = args.bind(py);
        let kind = {
            let class_name = get_type_class_name(args_type.bind(py).as_any())?;
            derive_kind(&class_name)
        };
        let args_json = serialize_args(py, args_bound)?;
        let metadata_json = metadata
            .as_ref()
            .map(|value| py_to_json(py, value.bind(py)))
            .transpose()?
            .unwrap_or(serde_json::json!({}));

        let periodic_job = PeriodicJob::builder(&name, &cron_expr)
            .timezone(&timezone)
            .queue(&queue)
            .priority(priority)
            .max_attempts(max_attempts)
            .tags(tags)
            .metadata(metadata_json)
            .build_raw(kind, args_json)
            .map_err(map_awa_error)?;

        self.periodic_jobs
            .lock()
            .expect("periodic_jobs mutex poisoned")
            .push(periodic_job);

        Ok(())
    }

    // Backs AsyncClient.queue_descriptor(). User-facing docs live on the
    // Python wrapper; the bridge only stashes the descriptor for use at
    // start() time.
    #[pyo3(signature = (
        queue,
        *,
        display_name=None,
        description=None,
        owner=None,
        docs_url=None,
        tags=None,
        extra=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn queue_descriptor(
        &self,
        py: Python<'_>,
        queue: String,
        display_name: Option<String>,
        description: Option<String>,
        owner: Option<String>,
        docs_url: Option<String>,
        tags: Option<Vec<String>>,
        extra: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        // Mutation after start() is a footgun: the runtime only reads
        // queue_descriptors during startup, so the late call would
        // silently have no effect. Fail loudly instead.
        if self
            .runtime
            .lock()
            .expect("runtime mutex poisoned")
            .is_some()
        {
            return Err(state_error(
                "queue_descriptor() must be called before start()",
            ));
        }
        let descriptor =
            build_queue_descriptor(py, display_name, description, owner, docs_url, tags, extra)?;
        self.queue_descriptors
            .lock()
            .expect("queue_descriptors mutex poisoned")
            .insert(queue, descriptor);
        Ok(())
    }

    // Backs AsyncClient.job_kind_descriptor(). See the Python wrapper for
    // user-facing semantics.
    #[pyo3(signature = (
        kind,
        *,
        display_name=None,
        description=None,
        owner=None,
        docs_url=None,
        tags=None,
        extra=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn job_kind_descriptor(
        &self,
        py: Python<'_>,
        kind: String,
        display_name: Option<String>,
        description: Option<String>,
        owner: Option<String>,
        docs_url: Option<String>,
        tags: Option<Vec<String>>,
        extra: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        if self
            .runtime
            .lock()
            .expect("runtime mutex poisoned")
            .is_some()
        {
            return Err(state_error(
                "job_kind_descriptor() must be called before start()",
            ));
        }
        let descriptor =
            build_job_kind_descriptor(py, display_name, description, owner, docs_url, tags, extra)?;
        self.job_kind_descriptors
            .lock()
            .expect("job_kind_descriptors mutex poisoned")
            .insert(kind, descriptor);
        Ok(())
    }

    fn retry<'py>(&self, py: Python<'py>, job_id: i64) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let row = awa_model::admin::retry(&pool, job_id)
                .await
                .map_err(map_awa_error)?;
            Ok(row.map(PyJob::from))
        })
    }

    fn cancel<'py>(&self, py: Python<'py>, job_id: i64) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let row = awa_model::admin::cancel(&pool, job_id)
                .await
                .map_err(map_awa_error)?;
            Ok(row.map(PyJob::from))
        })
    }

    /// Cancel a job by its unique key components (kind + optional queue/args/period).
    ///
    /// Reconstructs the BLAKE3 unique key from the same inputs used at insert
    /// time, then cancels the oldest matching non-terminal job. Returns the
    /// cancelled job, or None if no matching job was found.
    #[pyo3(signature = (kind, *, queue=None, args=None, period_bucket=None))]
    fn cancel_by_unique_key<'py>(
        &self,
        py: Python<'py>,
        kind: String,
        queue: Option<String>,
        args: Option<pyo3::Bound<'py, pyo3::types::PyAny>>,
        period_bucket: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let args_json: Option<serde_json::Value> = args
            .map(|a| pythonize::depythonize(&a))
            .transpose()
            .map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("Failed to serialize args: {e}"))
            })?;
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let row = awa_model::admin::cancel_by_unique_key(
                &pool,
                &kind,
                queue.as_deref(),
                args_json.as_ref(),
                period_bucket,
            )
            .await
            .map_err(map_awa_error)?;
            Ok(row.map(PyJob::from))
        })
    }

    #[pyo3(signature = (*, kind=None, queue=None))]
    fn retry_failed<'py>(
        &self,
        py: Python<'py>,
        kind: Option<String>,
        queue: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        match (&kind, &queue) {
            (Some(_), None) | (None, Some(_)) => {}
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Specify exactly one of kind or queue",
                ));
            }
        }
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let jobs = match (kind, queue) {
                (Some(kind), None) => awa_model::admin::retry_failed_by_kind(&pool, &kind).await,
                (None, Some(queue)) => awa_model::admin::retry_failed_by_queue(&pool, &queue).await,
                _ => unreachable!(),
            }
            .map_err(map_awa_error)?;

            Python::attach(|py| {
                let list = pyo3::types::PyList::empty(py);
                for job in jobs {
                    list.append(Py::new(py, PyJob::from(job))?)?;
                }
                Ok(list.unbind())
            })
        })
    }

    fn discard_failed<'py>(&self, py: Python<'py>, kind: String) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let count = awa_model::admin::discard_failed(&pool, &kind)
                .await
                .map_err(map_awa_error)?;
            Ok(count)
        })
    }

    #[pyo3(signature = (queue, paused_by=None))]
    fn pause_queue<'py>(
        &self,
        py: Python<'py>,
        queue: String,
        paused_by: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            awa_model::admin::pause_queue(&pool, &queue, paused_by.as_deref())
                .await
                .map_err(map_awa_error)?;
            Ok(())
        })
    }

    fn resume_queue<'py>(&self, py: Python<'py>, queue: String) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            awa_model::admin::resume_queue(&pool, &queue)
                .await
                .map_err(map_awa_error)?;
            Ok(())
        })
    }

    fn drain_queue<'py>(&self, py: Python<'py>, queue: String) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let count = awa_model::admin::drain_queue(&pool, &queue)
                .await
                .map_err(map_awa_error)?;
            Ok(count)
        })
    }

    /// Drain ALL dirty keys and recompute cached admin counters until the
    /// backlog is empty. Call before queue_stats() in tests without a
    /// maintenance leader to ensure the cache is fully fresh.
    fn flush_admin_metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            awa_model::admin::flush_dirty_admin_metadata(&pool)
                .await
                .map_err(map_awa_error)?;
            Ok(())
        })
    }

    fn flush_admin_metadata_sync(&self, py: Python<'_>) -> PyResult<()> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                awa_model::admin::flush_dirty_admin_metadata(&pool)
                    .await
                    .map_err(map_awa_error)?;
                Ok(())
            })
        })
    }

    fn queue_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stats = awa_model::admin::queue_overviews(&pool)
                .await
                .map_err(map_awa_error)?;
            Python::attach(|py| {
                let list = pyo3::types::PyList::empty(py);
                for stat in &stats {
                    list.append(Py::new(
                        py,
                        PyQueueStat {
                            queue: stat.queue.clone(),
                            total_queued: stat.total_queued,
                            scheduled: stat.scheduled,
                            available: stat.available,
                            retryable: stat.retryable,
                            running: stat.running,
                            failed: stat.failed,
                            waiting_external: stat.waiting_external,
                            completed_last_hour: stat.completed_last_hour,
                            lag_seconds: stat.lag_seconds,
                            paused: stat.paused,
                        },
                    )?)?;
                }
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (*, state=None, kind=None, queue=None, limit=100))]
    fn list_jobs<'py>(
        &self,
        py: Python<'py>,
        state: Option<String>,
        kind: Option<String>,
        queue: Option<String>,
        limit: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let parsed_state = state.as_deref().map(parse_job_state).transpose()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let filter = ListJobsFilter {
                state: parsed_state,
                kind,
                queue,
                limit: Some(limit),
                ..Default::default()
            };
            let jobs = awa_model::admin::list_jobs(&pool, &filter)
                .await
                .map_err(map_awa_error)?;
            Python::attach(|py| {
                let list = pyo3::types::PyList::empty(py);
                for job in jobs {
                    list.append(Py::new(py, PyJob::from(job))?)?;
                }
                Ok(list.unbind())
            })
        })
    }

    /// Get a single job by ID.
    fn get_job<'py>(&self, py: Python<'py>, job_id: i64) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let job = awa_model::admin::get_job(&pool, job_id)
                .await
                .map_err(map_awa_error)?;
            Ok(PyJob::from(job))
        })
    }

    /// List DLQ entries, optionally filtered by kind/queue/tag. `before_id`
    /// enables cursor pagination (pass the smallest id from the previous
    /// page). Rows are ordered by `dlq_at DESC, id DESC`.
    #[pyo3(signature = (*, kind=None, queue=None, tag=None, before_id=None, limit=100))]
    fn list_dlq<'py>(
        &self,
        py: Python<'py>,
        kind: Option<String>,
        queue: Option<String>,
        tag: Option<String>,
        before_id: Option<i64>,
        limit: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let filter = crate::dlq::build_filter(kind, queue, tag, before_id, Some(limit));
            let rows = awa_model::dlq::list_dlq(&pool, &filter)
                .await
                .map_err(map_awa_error)?;
            Python::attach(|py| {
                let list = pyo3::types::PyList::empty(py);
                for row in rows {
                    let entry = crate::dlq::dlq_row_to_entry(py, row)?;
                    list.append(Py::new(py, entry)?)?;
                }
                Ok(list.unbind())
            })
        })
    }

    /// Fetch a single DLQ entry by id. Returns `None` if the row isn't in the DLQ.
    fn get_dlq_job<'py>(&self, py: Python<'py>, job_id: i64) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let row = awa_model::dlq::get_dlq_job(&pool, job_id)
                .await
                .map_err(map_awa_error)?;
            Python::attach(|py| {
                Ok(match row {
                    Some(row) => crate::dlq::dlq_row_to_entry(py, row)?
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                    None => py.None(),
                })
            })
        })
    }

    /// Count DLQ rows, optionally filtered by queue.
    #[pyo3(signature = (*, queue=None))]
    fn dlq_depth<'py>(
        &self,
        py: Python<'py>,
        queue: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let count = awa_model::dlq::dlq_depth(&pool, queue.as_deref())
                .await
                .map_err(map_awa_error)?;
            Ok(count)
        })
    }

    /// DLQ row counts grouped by queue (descending).
    fn dlq_depth_by_queue<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = awa_model::dlq::dlq_depth_by_queue(&pool)
                .await
                .map_err(map_awa_error)?;
            Ok(rows)
        })
    }

    /// Retry a single DLQ'd job. Returns the revived PyJob or `None` if the
    /// DLQ row no longer exists.
    #[pyo3(signature = (job_id, *, run_at=None, priority=None, queue=None))]
    fn retry_from_dlq<'py>(
        &self,
        py: Python<'py>,
        job_id: i64,
        run_at: Option<DateTime<Utc>>,
        priority: Option<i16>,
        queue: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let opts = crate::dlq::build_retry_opts(run_at, priority, queue);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let job = awa_model::dlq::retry_from_dlq(&pool, job_id, &opts)
                .await
                .map_err(map_awa_error)?;
            if let Some(job) = job.as_ref() {
                awa_worker::AwaMetrics::from_global().record_dlq_retried(Some(&job.queue), 1);
            }
            Python::attach(|py| {
                Ok(match job {
                    Some(job) => PyJob::from(job).into_pyobject(py)?.into_any().unbind(),
                    None => py.None(),
                })
            })
        })
    }

    /// Bulk retry DLQ rows matching the filter. Returns the count of revived jobs.
    ///
    /// Requires at least one of `kind`, `queue`, or `tag` unless
    /// `allow_all=True`.
    #[pyo3(signature = (*, kind=None, queue=None, tag=None, allow_all=false))]
    fn bulk_retry_from_dlq<'py>(
        &self,
        py: Python<'py>,
        kind: Option<String>,
        queue: Option<String>,
        tag: Option<String>,
        allow_all: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let queue_attr = queue.clone();
            let filter = crate::dlq::build_filter(kind, queue, tag, None, None);
            let count = awa_model::dlq::bulk_retry_from_dlq(&pool, &filter, allow_all)
                .await
                .map_err(map_awa_error)?;
            if count > 0 {
                awa_worker::AwaMetrics::from_global()
                    .record_dlq_retried(queue_attr.as_deref(), count);
            }
            Ok(count)
        })
    }

    /// Move an already-failed job (in `jobs_hot`) into the DLQ. Returns the
    /// resulting DlqEntry, or `None` if the row wasn't in `failed` state.
    fn move_failed_to_dlq<'py>(
        &self,
        py: Python<'py>,
        job_id: i64,
        reason: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let row = awa_model::dlq::move_failed_to_dlq(&pool, job_id, &reason)
                .await
                .map_err(map_awa_error)?;
            Python::attach(|py| {
                Ok(match row {
                    Some(row) => crate::dlq::dlq_row_to_entry(py, row)?
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                    None => py.None(),
                })
            })
        })
    }

    /// Bulk-move failed jobs into the DLQ.
    ///
    /// Requires at least one of `kind` or `queue` unless `allow_all=True`.
    #[pyo3(signature = (*, kind=None, queue=None, reason="manual".to_string(), allow_all=false))]
    fn bulk_move_failed_to_dlq<'py>(
        &self,
        py: Python<'py>,
        kind: Option<String>,
        queue: Option<String>,
        reason: String,
        allow_all: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let count = awa_model::dlq::bulk_move_failed_to_dlq(
                &pool,
                kind.as_deref(),
                queue.as_deref(),
                &reason,
                allow_all,
            )
            .await
            .map_err(map_awa_error)?;
            Ok(count)
        })
    }

    /// Purge a single DLQ row. Returns `True` if the row was deleted.
    fn purge_dlq_job<'py>(&self, py: Python<'py>, job_id: i64) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let queue = awa_model::dlq::get_dlq_job(&pool, job_id)
                .await
                .map_err(map_awa_error)?
                .map(|row| row.job.queue);
            let deleted = awa_model::dlq::purge_dlq_job(&pool, job_id)
                .await
                .map_err(map_awa_error)?;
            if deleted {
                awa_worker::AwaMetrics::from_global().record_dlq_purged(queue.as_deref(), 1);
            }
            Ok(deleted)
        })
    }

    /// Bulk-purge DLQ rows matching the filter.
    ///
    /// Requires at least one of `kind`, `queue`, or `tag` unless
    /// `allow_all=True`.
    #[pyo3(signature = (*, kind=None, queue=None, tag=None, allow_all=false))]
    fn purge_dlq<'py>(
        &self,
        py: Python<'py>,
        kind: Option<String>,
        queue: Option<String>,
        tag: Option<String>,
        allow_all: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let queue_attr = queue.clone();
            let filter = crate::dlq::build_filter(kind, queue, tag, None, None);
            let count = awa_model::dlq::purge_dlq(&pool, &filter, allow_all)
                .await
                .map_err(map_awa_error)?;
            if count > 0 {
                awa_worker::AwaMetrics::from_global()
                    .record_dlq_purged(queue_attr.as_deref(), count);
            }
            Ok(count)
        })
    }

    fn health_check<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let runtime = self.runtime.lock().expect("runtime mutex poisoned").clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(runtime) = runtime {
                let health = runtime.health_check().await;
                return Ok(map_health_check(health));
            }

            let postgres_connected = sqlx::query("SELECT 1").execute(&pool).await.is_ok();
            Ok(PyHealthCheck {
                healthy: false,
                postgres_connected,
                poll_loop_alive: false,
                heartbeat_alive: false,
                shutting_down: false,
                leader: false,
                queues: HashMap::new(),
            })
        })
    }

    #[pyo3(signature = (queues=None, *, poll_interval_ms=200, global_max_workers=None, completed_retention_hours=None, failed_retention_hours=None, descriptor_retention_days=None, cleanup_batch_size=None, leader_election_interval_ms=None, heartbeat_interval_ms=None, promote_interval_ms=None, heartbeat_rescue_interval_ms=None, heartbeat_staleness_ms=None, deadline_rescue_interval_ms=None, callback_rescue_interval_ms=None, queue_storage_schema=None, queue_storage_queue_slot_count=16, queue_storage_lease_slot_count=8, queue_storage_queue_rotate_interval_ms=1000, queue_storage_lease_rotate_interval_ms=50))]
    #[allow(clippy::too_many_arguments)]
    fn start<'py>(
        &self,
        py: Python<'py>,
        queues: Option<Py<PyAny>>,
        poll_interval_ms: u64,
        global_max_workers: Option<u32>,
        completed_retention_hours: Option<f64>,
        failed_retention_hours: Option<f64>,
        descriptor_retention_days: Option<f64>,
        cleanup_batch_size: Option<i64>,
        leader_election_interval_ms: Option<u64>,
        heartbeat_interval_ms: Option<u64>,
        promote_interval_ms: Option<u64>,
        heartbeat_rescue_interval_ms: Option<u64>,
        heartbeat_staleness_ms: Option<u64>,
        deadline_rescue_interval_ms: Option<u64>,
        callback_rescue_interval_ms: Option<u64>,
        queue_storage_schema: Option<String>,
        queue_storage_queue_slot_count: u32,
        queue_storage_lease_slot_count: u32,
        queue_storage_queue_rotate_interval_ms: u64,
        queue_storage_lease_rotate_interval_ms: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        {
            let guard = self.runtime.lock().expect("runtime mutex poisoned");
            if guard.is_some() {
                return Err(state_error("worker runtime is already running"));
            }
        }

        let entries: Vec<_> = self
            .workers
            .read()
            .expect("workers lock poisoned")
            .values()
            .cloned()
            .collect();
        if entries.is_empty() {
            return Err(state_error(
                "register at least one worker before starting the runtime",
            ));
        }

        let parsed_configs = parse_queue_configs(py, queues.as_ref(), global_max_workers)?;
        let queue_configs = normalize_queue_configs(parsed_configs, &entries, global_max_workers)?;

        let mut builder = awa_worker::Client::builder(self.pool.clone());
        for config in &queue_configs {
            builder = builder.queue(
                config.name.clone(),
                awa_worker::QueueConfig {
                    max_workers: config.max_workers,
                    poll_interval: Duration::from_millis(poll_interval_ms),
                    rate_limit: config.rate_limit.clone(),
                    min_workers: config.min_workers,
                    weight: config.weight,
                    ..Default::default()
                },
            );
        }
        if let Some(global_max) = global_max_workers {
            builder = builder.global_max_workers(global_max);
        }
        if let Some(hours) = completed_retention_hours {
            if !hours.is_finite() || hours < 0.0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "completed_retention_hours must be a non-negative finite number",
                ));
            }
            builder = builder.completed_retention(Duration::from_secs_f64(hours * 3600.0));
        }
        if let Some(hours) = failed_retention_hours {
            if !hours.is_finite() || hours < 0.0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "failed_retention_hours must be a non-negative finite number",
                ));
            }
            builder = builder.failed_retention(Duration::from_secs_f64(hours * 3600.0));
        }
        if let Some(days) = descriptor_retention_days {
            if !days.is_finite() || days < 0.0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "descriptor_retention_days must be a non-negative finite number (0 disables)",
                ));
            }
            builder = builder.descriptor_retention(Duration::from_secs_f64(days * 86400.0));
        }
        if let Some(batch_size) = cleanup_batch_size {
            if batch_size <= 0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "cleanup_batch_size must be > 0",
                ));
            }
            builder = builder.cleanup_batch_size(batch_size);
        }

        // Apply per-queue retention overrides collected from queue config dicts
        let queue_overrides = parse_queue_retention_overrides(py, queues.as_ref())?;
        for (queue_name, policy) in queue_overrides {
            builder = builder.queue_retention(queue_name, policy);
        }

        if let Some(ms) = leader_election_interval_ms {
            builder = builder.leader_election_interval(Duration::from_millis(ms));
        }
        if let Some(ms) = heartbeat_interval_ms {
            builder = builder.heartbeat_interval(Duration::from_millis(ms));
        }
        if let Some(ms) = promote_interval_ms {
            builder = builder.promote_interval(Duration::from_millis(ms));
        }
        if let Some(ms) = heartbeat_rescue_interval_ms {
            builder = builder.heartbeat_rescue_interval(Duration::from_millis(ms));
        }
        if let Some(ms) = heartbeat_staleness_ms {
            builder = builder.heartbeat_staleness(Duration::from_millis(ms));
        }
        if let Some(ms) = deadline_rescue_interval_ms {
            builder = builder.deadline_rescue_interval(Duration::from_millis(ms));
        }
        if let Some(ms) = callback_rescue_interval_ms {
            builder = builder.callback_rescue_interval(Duration::from_millis(ms));
        }
        if queue_storage_queue_slot_count == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "queue_storage_queue_slot_count must be > 0",
            ));
        }
        if queue_storage_lease_slot_count == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "queue_storage_lease_slot_count must be > 0",
            ));
        }
        if queue_storage_queue_rotate_interval_ms == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "queue_storage_queue_rotate_interval_ms must be > 0",
            ));
        }
        if queue_storage_lease_rotate_interval_ms == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "queue_storage_lease_rotate_interval_ms must be > 0",
            ));
        }

        builder = builder.queue_storage(
            QueueStorageConfig {
                schema: queue_storage_schema.unwrap_or_else(|| QueueStorageConfig::default().schema),
                queue_slot_count: queue_storage_queue_slot_count as usize,
                lease_slot_count: queue_storage_lease_slot_count as usize,
            },
            Duration::from_millis(queue_storage_queue_rotate_interval_ms),
            Duration::from_millis(queue_storage_lease_rotate_interval_ms),
        );

        for entry in &entries {
            builder = builder.register_worker(PythonWorker::from_entry(entry));
        }

        // Register periodic jobs
        let periodic_jobs = self
            .periodic_jobs
            .lock()
            .expect("periodic_jobs mutex poisoned")
            .clone();
        for job in periodic_jobs {
            builder = builder.periodic(job);
        }

        // Attach descriptors. Using `job_kind_descriptor_kind` (by name) keeps
        // the Python surface symmetric with `queue_descriptor` and sidesteps
        // the typed T::kind() path, which requires Rust-side generics.
        let queue_descriptors = self
            .queue_descriptors
            .lock()
            .expect("queue_descriptors mutex poisoned")
            .clone();
        for (queue, descriptor) in queue_descriptors {
            builder = builder.queue_descriptor(queue, descriptor);
        }
        let job_kind_descriptors = self
            .job_kind_descriptors
            .lock()
            .expect("job_kind_descriptors mutex poisoned")
            .clone();
        for (kind, descriptor) in job_kind_descriptors {
            builder = builder.job_kind_descriptor_kind(kind, descriptor);
        }

        let runtime = Arc::new(builder.build().map_err(|e| state_error(e.to_string()))?);
        let runtime_clone = runtime.clone();
        let runtime_store = self.runtime.clone();
        // Store the runtime BEFORE starting so shutdown() can find it
        // even if called concurrently. If start() fails, remove it.
        *runtime_store.lock().expect("runtime mutex poisoned") = Some(runtime);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Err(e) = runtime_clone.start().await {
                runtime_store.lock().expect("runtime mutex poisoned").take();
                return Err(map_awa_error(e));
            }
            Ok(())
        })
    }

    #[pyo3(signature = (timeout_ms=2000))]
    fn shutdown<'py>(&self, py: Python<'py>, timeout_ms: u64) -> PyResult<Bound<'py, PyAny>> {
        let runtime = self.runtime.lock().expect("runtime mutex poisoned").take();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(runtime) = runtime {
                runtime.shutdown(Duration::from_millis(timeout_ms)).await;
            }
            Ok(())
        })
    }

    // ── External callback completion (async + sync) ─────────────────

    #[pyo3(signature = (callback_id, payload=None))]
    fn complete_external<'py>(
        &self,
        py: Python<'py>,
        callback_id: String,
        payload: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let payload_json = payload
            .as_ref()
            .map(|value| Python::attach(|py| py_to_json(py, value.bind(py))))
            .transpose()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uuid = uuid::Uuid::parse_str(&callback_id)
                .map_err(|e| map_awa_error(awa_model::AwaError::Validation(e.to_string())))?;
            let row = awa_model::admin::complete_external(&pool, uuid, payload_json, None)
                .await
                .map_err(map_awa_error)?;
            Ok(PyJob::from(row))
        })
    }

    fn fail_external<'py>(
        &self,
        py: Python<'py>,
        callback_id: String,
        error: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uuid = uuid::Uuid::parse_str(&callback_id)
                .map_err(|e| map_awa_error(awa_model::AwaError::Validation(e.to_string())))?;
            let row = awa_model::admin::fail_external(&pool, uuid, &error, None)
                .await
                .map_err(map_awa_error)?;
            Ok(PyJob::from(row))
        })
    }

    fn retry_external<'py>(
        &self,
        py: Python<'py>,
        callback_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uuid = uuid::Uuid::parse_str(&callback_id)
                .map_err(|e| map_awa_error(awa_model::AwaError::Validation(e.to_string())))?;
            let row = awa_model::admin::retry_external(&pool, uuid, None)
                .await
                .map_err(map_awa_error)?;
            Ok(PyJob::from(row))
        })
    }

    /// Resume a waiting job via external callback, returning it to running state.
    ///
    /// The handler resumes with the payload data. Use this for sequential
    /// callback patterns where the job needs to continue execution.
    #[pyo3(signature = (callback_id, payload=None))]
    fn resume_external<'py>(
        &self,
        py: Python<'py>,
        callback_id: String,
        payload: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let payload_json = payload
            .as_ref()
            .map(|value| Python::attach(|py| py_to_json(py, value.bind(py))))
            .transpose()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uuid = uuid::Uuid::parse_str(&callback_id)
                .map_err(|e| map_awa_error(awa_model::AwaError::Validation(e.to_string())))?;
            let row = awa_model::admin::resume_external(&pool, uuid, payload_json, None)
                .await
                .map_err(map_awa_error)?;
            Ok(PyJob::from(row))
        })
    }

    #[pyo3(signature = (callback_id, payload=None))]
    fn resume_external_sync(
        &self,
        py: Python<'_>,
        callback_id: String,
        payload: Option<Py<PyAny>>,
    ) -> PyResult<PyJob> {
        let pool = self.pool.clone();
        let payload_json = payload
            .as_ref()
            .map(|value| py_to_json(py, value.bind(py)))
            .transpose()?;
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let uuid = uuid::Uuid::parse_str(&callback_id)
                    .map_err(|e| map_awa_error(awa_model::AwaError::Validation(e.to_string())))?;
                let row = awa_model::admin::resume_external(&pool, uuid, payload_json, None)
                    .await
                    .map_err(map_awa_error)?;
                Ok(PyJob::from(row))
            })
        })
    }

    /// Reset the callback timeout for a long-running external operation.
    #[pyo3(signature = (callback_id, timeout_seconds=3600.0))]
    fn heartbeat_callback<'py>(
        &self,
        py: Python<'py>,
        callback_id: String,
        timeout_seconds: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uuid = uuid::Uuid::parse_str(&callback_id)
                .map_err(|e| map_awa_error(awa_model::AwaError::Validation(e.to_string())))?;
            let timeout = validate_timeout_seconds(timeout_seconds)?;
            let row = awa_model::admin::heartbeat_callback(&pool, uuid, timeout)
                .await
                .map_err(map_awa_error)?;
            Ok(PyJob::from(row))
        })
    }

    fn heartbeat_callback_sync(
        &self,
        py: Python<'_>,
        callback_id: String,
        timeout_seconds: Option<f64>,
    ) -> PyResult<PyJob> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let uuid = uuid::Uuid::parse_str(&callback_id)
                    .map_err(|e| map_awa_error(awa_model::AwaError::Validation(e.to_string())))?;
                let timeout = validate_timeout_seconds(timeout_seconds.unwrap_or(3600.0))?;
                let row = awa_model::admin::heartbeat_callback(&pool, uuid, timeout)
                    .await
                    .map_err(map_awa_error)?;
                Ok(PyJob::from(row))
            })
        })
    }

    #[pyo3(signature = (callback_id, payload=None))]
    fn complete_external_sync(
        &self,
        py: Python<'_>,
        callback_id: String,
        payload: Option<Py<PyAny>>,
    ) -> PyResult<PyJob> {
        let pool = self.pool.clone();
        let payload_json = payload
            .as_ref()
            .map(|value| py_to_json(py, value.bind(py)))
            .transpose()?;
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let uuid = uuid::Uuid::parse_str(&callback_id)
                    .map_err(|e| map_awa_error(awa_model::AwaError::Validation(e.to_string())))?;
                let row = awa_model::admin::complete_external(&pool, uuid, payload_json, None)
                    .await
                    .map_err(map_awa_error)?;
                Ok(PyJob::from(row))
            })
        })
    }

    fn fail_external_sync(
        &self,
        py: Python<'_>,
        callback_id: String,
        error: String,
    ) -> PyResult<PyJob> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let uuid = uuid::Uuid::parse_str(&callback_id)
                    .map_err(|e| map_awa_error(awa_model::AwaError::Validation(e.to_string())))?;
                let row = awa_model::admin::fail_external(&pool, uuid, &error, None)
                    .await
                    .map_err(map_awa_error)?;
                Ok(PyJob::from(row))
            })
        })
    }

    fn retry_external_sync(&self, py: Python<'_>, callback_id: String) -> PyResult<PyJob> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let uuid = uuid::Uuid::parse_str(&callback_id)
                    .map_err(|e| map_awa_error(awa_model::AwaError::Validation(e.to_string())))?;
                let row = awa_model::admin::retry_external(&pool, uuid, None)
                    .await
                    .map_err(map_awa_error)?;
                Ok(PyJob::from(row))
            })
        })
    }

    // ── Resolve callback (async + sync) ─────────────────────────────

    #[pyo3(signature = (callback_id, payload=None, default_action="ignore"))]
    fn resolve_callback<'py>(
        &self,
        py: Python<'py>,
        callback_id: String,
        payload: Option<Py<PyAny>>,
        default_action: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let payload_json = payload
            .as_ref()
            .map(|value| Python::attach(|py| py_to_json(py, value.bind(py))))
            .transpose()?;
        let action = parse_default_action(default_action)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uuid = uuid::Uuid::parse_str(&callback_id)
                .map_err(|e| map_awa_error(awa_model::AwaError::Validation(e.to_string())))?;
            let outcome =
                awa_model::admin::resolve_callback(&pool, uuid, payload_json, action, None)
                    .await
                    .map_err(map_awa_error)?;
            Ok(resolve_outcome_to_py(outcome))
        })
    }

    #[pyo3(signature = (callback_id, payload=None, default_action="ignore"))]
    fn resolve_callback_sync(
        &self,
        py: Python<'_>,
        callback_id: String,
        payload: Option<Py<PyAny>>,
        default_action: &str,
    ) -> PyResult<PyResolveResult> {
        let pool = self.pool.clone();
        let payload_json = payload
            .as_ref()
            .map(|value| py_to_json(py, value.bind(py)))
            .transpose()?;
        let action = parse_default_action(default_action)?;
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let uuid = uuid::Uuid::parse_str(&callback_id)
                    .map_err(|e| map_awa_error(awa_model::AwaError::Validation(e.to_string())))?;
                let outcome =
                    awa_model::admin::resolve_callback(&pool, uuid, payload_json, action, None)
                        .await
                        .map_err(map_awa_error)?;
                Ok(resolve_outcome_to_py(outcome))
            })
        })
    }

    // ── COPY batch insert (async + sync) ────────────────────────────

    #[pyo3(signature = (jobs, *, kind=None, queue="default".to_string(), priority=2, max_attempts=25, tags=vec![], metadata=None, run_at=None))]
    #[allow(clippy::too_many_arguments)]
    fn insert_many_copy<'py>(
        &self,
        py: Python<'py>,
        jobs: Vec<Py<PyAny>>,
        kind: Option<String>,
        queue: String,
        priority: i16,
        max_attempts: i16,
        tags: Vec<String>,
        metadata: Option<Py<PyAny>>,
        run_at: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let insert_params = prepare_insert_many_params(
            py,
            &jobs,
            kind,
            &queue,
            priority,
            max_attempts,
            &tags,
            metadata.as_ref(),
            run_at.as_ref(),
        )?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let results = awa_model::insert_many_copy_from_pool(&pool, &insert_params)
                .await
                .map_err(map_awa_error)?;
            Python::attach(|py| {
                let list = pyo3::types::PyList::empty(py);
                for row in results {
                    list.append(Py::new(py, PyJob::from(row))?)?;
                }
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (jobs, *, kind=None, queue="default".to_string(), priority=2, max_attempts=25, tags=vec![], metadata=None, run_at=None))]
    #[allow(clippy::too_many_arguments)]
    fn insert_many_copy_sync(
        &self,
        py: Python<'_>,
        jobs: Vec<Py<PyAny>>,
        kind: Option<String>,
        queue: String,
        priority: i16,
        max_attempts: i16,
        tags: Vec<String>,
        metadata: Option<Py<PyAny>>,
        run_at: Option<Py<PyAny>>,
    ) -> PyResult<Vec<PyJob>> {
        let pool = self.pool.clone();
        let insert_params = prepare_insert_many_params(
            py,
            &jobs,
            kind,
            &queue,
            priority,
            max_attempts,
            &tags,
            metadata.as_ref(),
            run_at.as_ref(),
        )?;

        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let results = awa_model::insert_many_copy_from_pool(&pool, &insert_params)
                    .await
                    .map_err(map_awa_error)?;
                Ok(results.into_iter().map(PyJob::from).collect())
            })
        })
    }

    // ── Sync counterparts ───────────────────────────────────────────

    #[pyo3(signature = (args, *, kind=None, queue="default".to_string(), priority=2, max_attempts=25, tags=vec![], metadata=None, run_at=None, unique_opts=None))]
    #[allow(clippy::too_many_arguments)]
    fn insert_sync(
        &self,
        py: Python<'_>,
        args: Py<PyAny>,
        kind: Option<String>,
        queue: String,
        priority: i16,
        max_attempts: i16,
        tags: Vec<String>,
        metadata: Option<Py<PyAny>>,
        run_at: Option<Py<PyAny>>,
        unique_opts: Option<Py<PyAny>>,
    ) -> PyResult<PyJob> {
        let pool = self.pool.clone();
        let args_bound = args.bind(py);
        let kind_str = match kind {
            Some(k) => k,
            None => {
                let class_name = get_type_class_name(args_bound.get_type().as_any())?;
                derive_kind(&class_name)
            }
        };
        let metadata_json = metadata
            .as_ref()
            .map(|value| py_to_json(py, value.bind(py)))
            .transpose()?
            .unwrap_or(serde_json::json!({}));
        let run_at_dt = run_at
            .as_ref()
            .map(|value| parse_run_at(py, value.bind(py)))
            .transpose()?;
        let args_json = serialize_args(py, args_bound)?;
        let unique = unique_opts
            .as_ref()
            .map(|value| parse_unique_opts(py, value.bind(py)))
            .transpose()?;

        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let row = insert_raw_job(
                    &pool,
                    &kind_str,
                    &args_json,
                    InsertOpts {
                        queue,
                        priority,
                        max_attempts,
                        run_at: run_at_dt,
                        metadata: metadata_json,
                        tags,
                        unique,
                        ..Default::default()
                    },
                )
                .await
                .map_err(map_awa_error)?;
                Ok(PyJob::from(row))
            })
        })
    }

    fn close_sync(&self, py: Python<'_>) -> PyResult<()> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                pool.close().await;
                Ok(())
            })
        })
    }

    fn migrate_sync(&self, py: Python<'_>) -> PyResult<()> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                awa_model::migrations::run(&pool)
                    .await
                    .map_err(map_awa_error)?;
                Ok(())
            })
        })
    }

    fn install_queue_storage_sync(
        &self,
        py: Python<'_>,
        schema: String,
        queue_slot_count: u32,
        lease_slot_count: u32,
        reset: bool,
    ) -> PyResult<()> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let store = QueueStorage::new(QueueStorageConfig {
                    schema,
                    queue_slot_count: queue_slot_count as usize,
                    lease_slot_count: lease_slot_count as usize,
                })
                .map_err(map_awa_error)?;
                store.install(&pool).await.map_err(map_awa_error)?;
                if reset {
                    store.reset(&pool).await.map_err(map_awa_error)?;
                }
                Ok(())
            })
        })
    }

    fn transaction_sync(&self, py: Python<'_>) -> PyResult<PySyncTransaction> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let tx = pool.begin().await.map_err(map_sqlx_error)?;
                Ok(PySyncTransaction::new(tx))
            })
        })
    }

    fn retry_sync(&self, py: Python<'_>, job_id: i64) -> PyResult<Option<PyJob>> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let row = awa_model::admin::retry(&pool, job_id)
                    .await
                    .map_err(map_awa_error)?;
                Ok(row.map(PyJob::from))
            })
        })
    }

    fn cancel_sync(&self, py: Python<'_>, job_id: i64) -> PyResult<Option<PyJob>> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let row = awa_model::admin::cancel(&pool, job_id)
                    .await
                    .map_err(map_awa_error)?;
                Ok(row.map(PyJob::from))
            })
        })
    }

    /// Cancel a job by its unique key components (sync version).
    #[pyo3(signature = (kind, *, queue=None, args=None, period_bucket=None))]
    fn cancel_by_unique_key_sync(
        &self,
        py: Python<'_>,
        kind: String,
        queue: Option<String>,
        args: Option<pyo3::Bound<'_, pyo3::types::PyAny>>,
        period_bucket: Option<i64>,
    ) -> PyResult<Option<PyJob>> {
        let args_json: Option<serde_json::Value> = args
            .map(|a| pythonize::depythonize(&a))
            .transpose()
            .map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("Failed to serialize args: {e}"))
            })?;
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let row = awa_model::admin::cancel_by_unique_key(
                    &pool,
                    &kind,
                    queue.as_deref(),
                    args_json.as_ref(),
                    period_bucket,
                )
                .await
                .map_err(map_awa_error)?;
                Ok(row.map(PyJob::from))
            })
        })
    }

    #[pyo3(signature = (*, kind=None, queue=None))]
    fn retry_failed_sync(
        &self,
        py: Python<'_>,
        kind: Option<String>,
        queue: Option<String>,
    ) -> PyResult<Vec<PyJob>> {
        match (&kind, &queue) {
            (Some(_), None) | (None, Some(_)) => {}
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Specify exactly one of kind or queue",
                ));
            }
        }
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let jobs = match (kind, queue) {
                    (Some(kind), None) => {
                        awa_model::admin::retry_failed_by_kind(&pool, &kind).await
                    }
                    (None, Some(queue)) => {
                        awa_model::admin::retry_failed_by_queue(&pool, &queue).await
                    }
                    _ => unreachable!(),
                }
                .map_err(map_awa_error)?;
                Ok(jobs.into_iter().map(PyJob::from).collect())
            })
        })
    }

    fn discard_failed_sync(&self, py: Python<'_>, kind: String) -> PyResult<u64> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let count = awa_model::admin::discard_failed(&pool, &kind)
                    .await
                    .map_err(map_awa_error)?;
                Ok(count)
            })
        })
    }

    #[pyo3(signature = (queue, paused_by=None))]
    fn pause_queue_sync(
        &self,
        py: Python<'_>,
        queue: String,
        paused_by: Option<String>,
    ) -> PyResult<()> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                awa_model::admin::pause_queue(&pool, &queue, paused_by.as_deref())
                    .await
                    .map_err(map_awa_error)?;
                Ok(())
            })
        })
    }

    fn resume_queue_sync(&self, py: Python<'_>, queue: String) -> PyResult<()> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                awa_model::admin::resume_queue(&pool, &queue)
                    .await
                    .map_err(map_awa_error)?;
                Ok(())
            })
        })
    }

    fn drain_queue_sync(&self, py: Python<'_>, queue: String) -> PyResult<u64> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let count = awa_model::admin::drain_queue(&pool, &queue)
                    .await
                    .map_err(map_awa_error)?;
                Ok(count)
            })
        })
    }

    fn queue_stats_sync(&self, py: Python<'_>) -> PyResult<Vec<PyQueueStat>> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let stats = awa_model::admin::queue_overviews(&pool)
                    .await
                    .map_err(map_awa_error)?;
                Ok(stats
                    .iter()
                    .map(|s| PyQueueStat {
                        queue: s.queue.clone(),
                        total_queued: s.total_queued,
                        scheduled: s.scheduled,
                        available: s.available,
                        retryable: s.retryable,
                        running: s.running,
                        failed: s.failed,
                        waiting_external: s.waiting_external,
                        completed_last_hour: s.completed_last_hour,
                        lag_seconds: s.lag_seconds,
                        paused: s.paused,
                    })
                    .collect())
            })
        })
    }

    #[pyo3(signature = (*, state=None, kind=None, queue=None, limit=100))]
    fn list_jobs_sync(
        &self,
        py: Python<'_>,
        state: Option<String>,
        kind: Option<String>,
        queue: Option<String>,
        limit: i64,
    ) -> PyResult<Vec<PyJob>> {
        let pool = self.pool.clone();
        let parsed_state = state.as_deref().map(parse_job_state).transpose()?;
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let filter = ListJobsFilter {
                    state: parsed_state,
                    kind,
                    queue,
                    limit: Some(limit),
                    ..Default::default()
                };
                let jobs = awa_model::admin::list_jobs(&pool, &filter)
                    .await
                    .map_err(map_awa_error)?;
                Ok(jobs.into_iter().map(PyJob::from).collect())
            })
        })
    }

    /// Get a single job by ID (sync).
    fn get_job_sync(&self, py: Python<'_>, job_id: i64) -> PyResult<PyJob> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let job = awa_model::admin::get_job(&pool, job_id)
                    .await
                    .map_err(map_awa_error)?;
                Ok(PyJob::from(job))
            })
        })
    }

    // --- DLQ sync versions -------------------------------------------------

    #[pyo3(signature = (*, kind=None, queue=None, tag=None, before_id=None, limit=100))]
    fn list_dlq_sync(
        &self,
        py: Python<'_>,
        kind: Option<String>,
        queue: Option<String>,
        tag: Option<String>,
        before_id: Option<i64>,
        limit: i64,
    ) -> PyResult<Vec<crate::dlq::PyDlqEntry>> {
        let pool = self.pool.clone();
        let filter = crate::dlq::build_filter(kind, queue, tag, before_id, Some(limit));
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let rows = awa_model::dlq::list_dlq(&pool, &filter)
                    .await
                    .map_err(map_awa_error)?;
                Python::attach(|py| {
                    rows.into_iter()
                        .map(|row| crate::dlq::dlq_row_to_entry(py, row))
                        .collect()
                })
            })
        })
    }

    fn get_dlq_job_sync(
        &self,
        py: Python<'_>,
        job_id: i64,
    ) -> PyResult<Option<crate::dlq::PyDlqEntry>> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let row = awa_model::dlq::get_dlq_job(&pool, job_id)
                    .await
                    .map_err(map_awa_error)?;
                Python::attach(|py| row.map(|r| crate::dlq::dlq_row_to_entry(py, r)).transpose())
            })
        })
    }

    #[pyo3(signature = (*, queue=None))]
    fn dlq_depth_sync(&self, py: Python<'_>, queue: Option<String>) -> PyResult<i64> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                awa_model::dlq::dlq_depth(&pool, queue.as_deref())
                    .await
                    .map_err(map_awa_error)
            })
        })
    }

    fn dlq_depth_by_queue_sync(&self, py: Python<'_>) -> PyResult<Vec<(String, i64)>> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                awa_model::dlq::dlq_depth_by_queue(&pool)
                    .await
                    .map_err(map_awa_error)
            })
        })
    }

    #[pyo3(signature = (job_id, *, run_at=None, priority=None, queue=None))]
    fn retry_from_dlq_sync(
        &self,
        py: Python<'_>,
        job_id: i64,
        run_at: Option<DateTime<Utc>>,
        priority: Option<i16>,
        queue: Option<String>,
    ) -> PyResult<Option<PyJob>> {
        let pool = self.pool.clone();
        let opts = crate::dlq::build_retry_opts(run_at, priority, queue);
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let job = awa_model::dlq::retry_from_dlq(&pool, job_id, &opts)
                    .await
                    .map_err(map_awa_error)?;
                if let Some(job) = job.as_ref() {
                    awa_worker::AwaMetrics::from_global().record_dlq_retried(Some(&job.queue), 1);
                }
                Ok(job.map(PyJob::from))
            })
        })
    }

    #[pyo3(signature = (*, kind=None, queue=None, tag=None, allow_all=false))]
    fn bulk_retry_from_dlq_sync(
        &self,
        py: Python<'_>,
        kind: Option<String>,
        queue: Option<String>,
        tag: Option<String>,
        allow_all: bool,
    ) -> PyResult<u64> {
        let pool = self.pool.clone();
        let queue_attr = queue.clone();
        let filter = crate::dlq::build_filter(kind, queue, tag, None, None);
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let count = awa_model::dlq::bulk_retry_from_dlq(&pool, &filter, allow_all)
                    .await
                    .map_err(map_awa_error)?;
                if count > 0 {
                    awa_worker::AwaMetrics::from_global()
                        .record_dlq_retried(queue_attr.as_deref(), count);
                }
                Ok(count)
            })
        })
    }

    fn move_failed_to_dlq_sync(
        &self,
        py: Python<'_>,
        job_id: i64,
        reason: String,
    ) -> PyResult<Option<crate::dlq::PyDlqEntry>> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let row = awa_model::dlq::move_failed_to_dlq(&pool, job_id, &reason)
                    .await
                    .map_err(map_awa_error)?;
                Python::attach(|py| row.map(|r| crate::dlq::dlq_row_to_entry(py, r)).transpose())
            })
        })
    }

    #[pyo3(signature = (*, kind=None, queue=None, reason="manual".to_string(), allow_all=false))]
    fn bulk_move_failed_to_dlq_sync(
        &self,
        py: Python<'_>,
        kind: Option<String>,
        queue: Option<String>,
        reason: String,
        allow_all: bool,
    ) -> PyResult<u64> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                awa_model::dlq::bulk_move_failed_to_dlq(
                    &pool,
                    kind.as_deref(),
                    queue.as_deref(),
                    &reason,
                    allow_all,
                )
                .await
                .map_err(map_awa_error)
            })
        })
    }

    fn purge_dlq_job_sync(&self, py: Python<'_>, job_id: i64) -> PyResult<bool> {
        let pool = self.pool.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let queue = awa_model::dlq::get_dlq_job(&pool, job_id)
                    .await
                    .map_err(map_awa_error)?
                    .map(|row| row.job.queue);
                let deleted = awa_model::dlq::purge_dlq_job(&pool, job_id)
                    .await
                    .map_err(map_awa_error)?;
                if deleted {
                    awa_worker::AwaMetrics::from_global().record_dlq_purged(queue.as_deref(), 1);
                }
                Ok(deleted)
            })
        })
    }

    #[pyo3(signature = (*, kind=None, queue=None, tag=None, allow_all=false))]
    fn purge_dlq_sync(
        &self,
        py: Python<'_>,
        kind: Option<String>,
        queue: Option<String>,
        tag: Option<String>,
        allow_all: bool,
    ) -> PyResult<u64> {
        let pool = self.pool.clone();
        let queue_attr = queue.clone();
        let filter = crate::dlq::build_filter(kind, queue, tag, None, None);
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let count = awa_model::dlq::purge_dlq(&pool, &filter, allow_all)
                    .await
                    .map_err(map_awa_error)?;
                if count > 0 {
                    awa_worker::AwaMetrics::from_global()
                        .record_dlq_purged(queue_attr.as_deref(), count);
                }
                Ok(count)
            })
        })
    }

    fn health_check_sync(&self, py: Python<'_>) -> PyResult<PyHealthCheck> {
        let pool = self.pool.clone();
        let runtime = self.runtime.lock().expect("runtime mutex poisoned").clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                if let Some(runtime) = runtime {
                    let health = runtime.health_check().await;
                    return Ok(map_health_check(health));
                }
                let postgres_connected = sqlx::query("SELECT 1").execute(&pool).await.is_ok();
                Ok(PyHealthCheck {
                    healthy: false,
                    postgres_connected,
                    poll_loop_alive: false,
                    heartbeat_alive: false,
                    shutting_down: false,
                    leader: false,
                    queues: HashMap::new(),
                })
            })
        })
    }
}

/// Convert a list of Python job args into InsertParams for the COPY path.
#[allow(clippy::too_many_arguments)]
fn prepare_insert_many_params(
    py: Python<'_>,
    jobs: &[Py<PyAny>],
    kind: Option<String>,
    queue: &str,
    priority: i16,
    max_attempts: i16,
    tags: &[String],
    metadata: Option<&Py<PyAny>>,
    run_at: Option<&Py<PyAny>>,
) -> PyResult<Vec<InsertParams>> {
    let metadata_json = metadata
        .map(|value| py_to_json(py, value.bind(py)))
        .transpose()?
        .unwrap_or(serde_json::json!({}));
    let run_at_dt = run_at
        .map(|value| parse_run_at(py, value.bind(py)))
        .transpose()?;

    jobs.iter()
        .map(|job| {
            let bound = job.bind(py);
            let kind_str = match &kind {
                Some(k) => k.clone(),
                None => {
                    let class_name = get_type_class_name(bound.get_type().as_any())?;
                    Ok::<_, PyErr>(derive_kind(&class_name))
                }?,
            };
            let args_json = serialize_args(py, bound)?;
            Ok(InsertParams {
                kind: kind_str,
                args: args_json,
                opts: InsertOpts {
                    queue: queue.to_string(),
                    priority,
                    max_attempts,
                    run_at: run_at_dt,
                    metadata: metadata_json.clone(),
                    tags: tags.to_vec(),
                    ..Default::default()
                },
            })
        })
        .collect()
}

/// Parsed queue configuration from Python input.
struct ParsedQueueConfig {
    name: String,
    max_workers: u32,
    min_workers: u32,
    weight: u32,
    rate_limit: Option<awa_worker::RateLimit>,
}

/// Parse queue configs from Python input (list of tuples or dicts).
fn parse_queue_configs(
    py: Python<'_>,
    queues: Option<&Py<PyAny>>,
    global_max_workers: Option<u32>,
) -> PyResult<Option<Vec<ParsedQueueConfig>>> {
    let queues = match queues {
        Some(q) => q,
        None => {
            if global_max_workers.is_some() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "weighted mode requires explicit queue configs (global_max_workers set but queues=None)",
                ));
            }
            return Ok(None);
        }
    };

    let bound = queues.bind(py);
    let list: Vec<Bound<'_, PyAny>> = bound.extract()?;
    let mut configs = Vec::new();

    for item in &list {
        // Try tuple form first: (name, max_workers)
        if let Ok(tuple) = item.extract::<(String, u32)>() {
            if global_max_workers.is_some() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "tuple queue config is not supported with global_max_workers; use dict form with min_workers/weight",
                ));
            }
            configs.push(ParsedQueueConfig {
                name: tuple.0,
                max_workers: tuple.1,
                min_workers: 0,
                weight: 1,
                rate_limit: None,
            });
            continue;
        }

        // Dict form
        let dict: &Bound<'_, PyDict> = item.cast().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(
                "queue config must be a (name, max_workers) tuple or a dict",
            )
        })?;

        let name: String = dict
            .get_item("name")?
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("queue config dict must have 'name' key")
            })?
            .extract()?;

        let has_max = dict.get_item("max_workers")?.is_some();
        let has_min = dict.get_item("min_workers")?.is_some();

        if has_max && has_min {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "use max_workers for hard-reserved mode or min_workers for weighted mode, not both",
            ));
        }

        let max_workers = if has_max {
            dict.get_item("max_workers")?.unwrap().extract()?
        } else if global_max_workers.is_none() && !has_min {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "max_workers required in hard-reserved mode (no global_max_workers set)",
            ));
        } else {
            50 // default, unused in weighted mode
        };

        let min_workers: u32 = dict
            .get_item("min_workers")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(0);

        let weight: u32 = dict
            .get_item("weight")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(1);

        if weight == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "weight must be > 0",
            ));
        }

        let rate_limit = if let Some(rl_val) = dict.get_item("rate_limit")? {
            if rl_val.is_none() {
                None
            } else {
                let (max_rate, burst): (f64, u32) = rl_val.extract().map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err(
                        "rate_limit must be a (max_rate: float, burst: int) tuple or None",
                    )
                })?;
                Some(awa_worker::RateLimit { max_rate, burst })
            }
        } else {
            None
        };

        configs.push(ParsedQueueConfig {
            name,
            max_workers,
            min_workers,
            weight,
            rate_limit,
        });
    }

    Ok(Some(configs))
}

fn normalize_queue_configs(
    parsed: Option<Vec<ParsedQueueConfig>>,
    entries: &[WorkerEntry],
    global_max_workers: Option<u32>,
) -> PyResult<Vec<ParsedQueueConfig>> {
    let configured = if let Some(configs) = parsed {
        configs
    } else {
        // Infer from registered workers
        let mut inferred = Vec::new();
        let mut seen = HashSet::new();
        for entry in entries {
            if seen.insert(entry.queue.clone()) {
                let default_max = if global_max_workers.is_some() { 50 } else { 10 };
                inferred.push(ParsedQueueConfig {
                    name: entry.queue.clone(),
                    max_workers: default_max,
                    min_workers: 0,
                    weight: 1,
                    rate_limit: None,
                });
            }
        }
        inferred
    };

    let configured_names: HashSet<_> = configured.iter().map(|c| c.name.clone()).collect();
    let missing: Vec<_> = entries
        .iter()
        .filter(|entry| !configured_names.contains(&entry.queue))
        .map(|entry| entry.queue.clone())
        .collect();
    if !missing.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "start() must configure the worker queues declared via @client.worker(..., queue=...): {}",
            missing.join(", ")
        )));
    }

    Ok(configured)
}

fn map_health_check(health: awa_worker::HealthCheck) -> PyHealthCheck {
    PyHealthCheck {
        healthy: health.healthy,
        postgres_connected: health.postgres_connected,
        poll_loop_alive: health.poll_loop_alive,
        heartbeat_alive: health.heartbeat_alive,
        shutting_down: health.shutting_down,
        leader: health.leader,
        queues: health
            .queues
            .into_iter()
            .map(|(queue, stats)| {
                let (max_workers, min_workers, weight, overflow_held) = match stats.capacity {
                    awa_worker::QueueCapacity::HardReserved { max_workers } => {
                        (Some(max_workers), None, None, None)
                    }
                    awa_worker::QueueCapacity::Weighted {
                        min_workers,
                        weight,
                        overflow_held,
                    } => (None, Some(min_workers), Some(weight), Some(overflow_held)),
                };
                (
                    queue,
                    PyQueueHealth {
                        in_flight: stats.in_flight,
                        available: stats.available,
                        max_workers,
                        min_workers,
                        weight,
                        overflow_held,
                    },
                )
            })
            .collect(),
    }
}

fn parse_job_state(value: &str) -> PyResult<JobState> {
    match value {
        "scheduled" => Ok(JobState::Scheduled),
        "available" => Ok(JobState::Available),
        "running" => Ok(JobState::Running),
        "completed" => Ok(JobState::Completed),
        "retryable" => Ok(JobState::Retryable),
        "failed" => Ok(JobState::Failed),
        "cancelled" => Ok(JobState::Cancelled),
        "waiting_external" => Ok(JobState::WaitingExternal),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unknown job state: {other}"
        ))),
    }
}

fn parse_default_action(value: &str) -> PyResult<awa_model::admin::DefaultAction> {
    match value {
        "complete" => Ok(awa_model::admin::DefaultAction::Complete),
        "fail" => Ok(awa_model::admin::DefaultAction::Fail),
        "ignore" => Ok(awa_model::admin::DefaultAction::Ignore),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unknown default_action: {other} (expected 'complete', 'fail', or 'ignore')"
        ))),
    }
}

fn resolve_outcome_to_py(outcome: awa_model::admin::ResolveOutcome) -> PyResolveResult {
    match outcome {
        awa_model::admin::ResolveOutcome::Completed { payload, job } => PyResolveResult {
            outcome: "completed".to_string(),
            job: Some(PyJob::from(job)),
            payload_json: payload,
            reason: None,
        },
        awa_model::admin::ResolveOutcome::Failed { job } => PyResolveResult {
            outcome: "failed".to_string(),
            job: Some(PyJob::from(job)),
            payload_json: None,
            reason: None,
        },
        awa_model::admin::ResolveOutcome::Ignored { reason } => PyResolveResult {
            outcome: "ignored".to_string(),
            job: None,
            payload_json: None,
            reason: Some(reason),
        },
    }
}

/// Parse per-queue retention overrides from the queue config dicts.
///
/// Looks for an optional `"retention"` key in each dict, expecting:
/// `{"completed_hours": float, "failed_hours": float}`
fn parse_queue_retention_overrides(
    py: Python<'_>,
    queues: Option<&Py<PyAny>>,
) -> PyResult<Vec<(String, awa_worker::RetentionPolicy)>> {
    let queues = match queues {
        Some(q) => q,
        None => return Ok(Vec::new()),
    };

    let bound = queues.bind(py);
    let list: Vec<Bound<'_, PyAny>> = match bound.extract() {
        Ok(l) => l,
        Err(_) => return Ok(Vec::new()),
    };

    let mut overrides = Vec::new();

    for item in &list {
        // Only dict-form configs can have retention
        let dict: &Bound<'_, PyDict> = match item.cast() {
            Ok(d) => d,
            Err(_) => continue,
        };

        let retention_val = match dict.get_item("retention")? {
            Some(v) if !v.is_none() => v,
            _ => continue,
        };

        let retention_dict: &Bound<'_, PyDict> = retention_val.cast().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(
                "retention must be a dict with 'completed_hours' and/or 'failed_hours' keys",
            )
        })?;

        let name: String = dict
            .get_item("name")?
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(
                    "queue config dict with retention must have 'name' key",
                )
            })?
            .extract()?;

        let default_policy = awa_worker::RetentionPolicy::default();
        let completed_hours: f64 = retention_dict
            .get_item("completed_hours")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(default_policy.completed.as_secs_f64() / 3600.0);
        if !completed_hours.is_finite() || completed_hours < 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "retention completed_hours must be a non-negative finite number",
            ));
        }
        let failed_hours: f64 = retention_dict
            .get_item("failed_hours")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(default_policy.failed.as_secs_f64() / 3600.0);
        if !failed_hours.is_finite() || failed_hours < 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "retention failed_hours must be a non-negative finite number",
            ));
        }

        overrides.push((
            name,
            awa_worker::RetentionPolicy {
                completed: Duration::from_secs_f64(completed_hours * 3600.0),
                failed: Duration::from_secs_f64(failed_hours * 3600.0),
                dlq: None,
            },
        ));
    }

    Ok(overrides)
}
