use crate::args::{derive_kind, get_type_class_name, serialize_args};
use crate::job::PyJob;
use crate::transaction::PyTransaction;
use awa_model::InsertOpts;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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

/// Worker registration entry (used when worker dispatch is started).
#[allow(dead_code)]
pub struct WorkerEntry {
    pub kind: String,
    pub handler: Py<PyAny>,
    pub args_type: Py<PyAny>,
    pub queue: String,
}

/// The main Python client.
#[pyclass(name = "Client")]
pub struct PyClient {
    pool: PgPool,
    workers: Arc<RwLock<HashMap<String, WorkerEntry>>>,
    cancel: tokio_util::sync::CancellationToken,
}

#[pymethods]
impl PyClient {
    /// Create a new client connected to the given database URL.
    #[new]
    #[pyo3(signature = (database_url, max_connections=10))]
    fn new(_py: Python<'_>, database_url: String, max_connections: u32) -> PyResult<Self> {
        let pool = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async {
                PgPoolOptions::new()
                    .max_connections(max_connections)
                    .connect(&database_url)
                    .await
            })
            .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(e.to_string()))?;

        Ok(Self {
            pool,
            workers: Arc::new(RwLock::new(HashMap::new())),
            cancel: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Insert a job. Args can be a dataclass, pydantic BaseModel, or dict.
    ///
    /// If args is a dataclass/BaseModel, kind is auto-derived from the class name.
    /// If args is a dict, kind must be provided.
    #[pyo3(signature = (args, *, kind=None, queue="default".to_string(), priority=2, max_attempts=25, tags=vec![], metadata=None))]
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (kind_str, args_json) = Python::attach(|py| {
                let args_bound = args.bind(py);
                let kind_str = match kind {
                    Some(k) => k,
                    None => {
                        let class_name = get_type_class_name(args_bound.get_type().as_any())?;
                        derive_kind(&class_name)
                    }
                };
                let args_json = serialize_args(py, args_bound)?;
                Ok::<_, PyErr>((kind_str, args_json))
            })?;

            let metadata_json = metadata
                .map(|m| {
                    Python::attach(|py| {
                        let m_bound = m.bind(py);
                        crate::job::py_to_json(py, m_bound)
                    })
                })
                .transpose()?
                .unwrap_or(serde_json::json!({}));

            let opts = InsertOpts {
                queue,
                priority,
                max_attempts,
                tags,
                metadata: metadata_json,
                ..Default::default()
            };

            // Use awa_model's insert_with with a manual approach since we have raw JSON
            let row = sqlx::query_as::<_, awa_model::JobRow>(
                r#"
                INSERT INTO awa.jobs (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
                VALUES ($1, $2, $3, 'available', $4, $5, now(), $6, $7)
                RETURNING *
                "#,
            )
            .bind(&kind_str)
            .bind(&opts.queue)
            .bind(&args_json)
            .bind(opts.priority)
            .bind(opts.max_attempts)
            .bind(&opts.metadata)
            .bind(&opts.tags)
            .fetch_one(&pool)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            Ok(PyJob::from(row))
        })
    }

    /// Run database migrations.
    fn migrate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            awa_model::migrations::run(&pool)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    /// Start a transaction for atomic operations.
    fn transaction<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tx = pool
                .begin()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTransaction::new(tx))
        })
    }

    /// Register a worker handler for a job type.
    ///
    /// Usage:
    ///   @client.worker(SendEmail, queue="email")
    ///   async def handle(job):
    ///       ...
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

        // Return a decorator using PyCFunction::new_closure
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
                };

                let workers = workers.clone();
                let kind = kind_str.clone();
                pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                    let mut guard = workers.write().await;
                    guard.insert(kind, entry);
                });

                Ok(handler_py)
            },
        )?;

        Ok(decorator.into_any().unbind())
    }

    /// Retry a failed or cancelled job.
    fn retry<'py>(&self, py: Python<'py>, job_id: i64) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let row = awa_model::admin::retry(&pool, job_id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(row.map(PyJob::from))
        })
    }

    /// Cancel a job.
    fn cancel<'py>(&self, py: Python<'py>, job_id: i64) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let row = awa_model::admin::cancel(&pool, job_id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(row.map(PyJob::from))
        })
    }

    /// Pause a queue.
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
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    /// Resume a queue.
    fn resume_queue<'py>(&self, py: Python<'py>, queue: String) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            awa_model::admin::resume_queue(&pool, &queue)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    /// Drain a queue (cancel all pending jobs).
    fn drain_queue<'py>(&self, py: Python<'py>, queue: String) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let count = awa_model::admin::drain_queue(&pool, &queue)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(count)
        })
    }

    /// Get queue statistics.
    fn queue_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stats = awa_model::admin::queue_stats(&pool)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Python::attach(|py| {
                let list = pyo3::types::PyList::empty(py);
                for stat in &stats {
                    let dict = PyDict::new(py);
                    dict.set_item("queue", &stat.queue)?;
                    dict.set_item("available", stat.available)?;
                    dict.set_item("running", stat.running)?;
                    dict.set_item("failed", stat.failed)?;
                    dict.set_item("completed_last_hour", stat.completed_last_hour)?;
                    dict.set_item("lag_seconds", stat.lag_seconds)?;
                    list.append(dict)?;
                }
                Ok(list.unbind())
            })
        })
    }

    /// Start the worker dispatch loop in the background.
    ///
    /// Spawns a background tokio task that polls registered queues, claims jobs,
    /// and dispatches to handler functions. Returns immediately.
    ///
    /// Call `client.shutdown()` to stop the loop.
    #[pyo3(signature = (queues, *, poll_interval_ms=200))]
    fn start(&self, _py: Python<'_>, queues: Vec<(String, u32)>, poll_interval_ms: u64) -> PyResult<()> {
        let pool = self.pool.clone();
        let workers = self.workers.clone();
        let cancel = self.cancel.clone();
        let poll_interval = std::time::Duration::from_millis(poll_interval_ms);

        pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
            if let Err(err) = crate::worker::run_workers(pool, workers, queues, cancel, poll_interval).await {
                tracing::error!(error = %err, "Worker dispatch loop failed");
            }
        });

        Ok(())
    }

    /// Gracefully shut down the worker dispatch loop.
    #[pyo3(signature = (timeout_ms=2000))]
    fn shutdown(&self, _py: Python<'_>, timeout_ms: u64) -> PyResult<()> {
        self.cancel.cancel();
        std::thread::sleep(std::time::Duration::from_millis(timeout_ms.min(5000)));
        Ok(())
    }
}
