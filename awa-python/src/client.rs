use crate::args::{derive_kind, get_type_class_name, serialize_args};
use crate::errors::{map_awa_error, map_connect_error, map_sqlx_error, state_error, validation_error};
use crate::job::{py_to_json, PyJob};
use crate::transaction::{insert_raw_job, parse_run_at, PyTransaction};
use crate::worker::PythonWorker;
use awa_model::admin::ListJobsFilter;
use awa_model::{InsertOpts, JobState, PeriodicJob};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
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

#[pyclass(frozen, name = "QueueHealth", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyQueueHealth {
    #[pyo3(get)]
    pub in_flight: u32,
    #[pyo3(get)]
    pub max_workers: u32,
    #[pyo3(get)]
    pub available: u64,
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
    runtime: Arc<Mutex<Option<Arc<awa_worker::Client>>>>,
}

#[pymethods]
impl PyClient {
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
            .map_err(map_connect_error)?;

        Ok(Self {
            pool,
            workers: Arc::new(RwLock::new(HashMap::new())),
            periodic_jobs: Arc::new(Mutex::new(Vec::new())),
            runtime: Arc::new(Mutex::new(None)),
        })
    }

    #[pyo3(signature = (args, *, kind=None, queue="default".to_string(), priority=2, max_attempts=25, tags=vec![], metadata=None, run_at=None))]
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (kind_str, args_json, metadata_json, run_at) = Python::attach(|py| {
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
                Ok::<_, PyErr>((
                    kind_str,
                    serialize_args(py, args_bound)?,
                    metadata_json,
                    run_at,
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
                    ..Default::default()
                },
            )
            .await
            .map_err(map_awa_error)?;

            Ok(PyJob::from(row))
        })
    }

    fn migrate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            awa_model::migrations::run(&pool)
                .await
                .map_err(map_awa_error)?;
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
                pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                    let mut guard = workers.write().await;
                    guard.insert(kind, entry);
                });

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

    fn queue_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stats = awa_model::admin::queue_stats(&pool)
                .await
                .map_err(map_awa_error)?;
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

    #[pyo3(signature = (queues=None, *, poll_interval_ms=200))]
    fn start(
        &self,
        _py: Python<'_>,
        queues: Option<Vec<(String, u32)>>,
        poll_interval_ms: u64,
    ) -> PyResult<()> {
        {
            let guard = self.runtime.lock().expect("runtime mutex poisoned");
            if guard.is_some() {
                return Err(state_error("worker runtime is already running"));
            }
        }

        let entries = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            self.workers
                .read()
                .await
                .values()
                .cloned()
                .collect::<Vec<_>>()
        });
        if entries.is_empty() {
            return Err(state_error(
                "register at least one worker before starting the runtime",
            ));
        }

        let queue_configs = normalize_queue_configs(queues, &entries)?;
        let mut builder = awa_worker::Client::builder(self.pool.clone());
        for (queue, max_workers) in &queue_configs {
            builder = builder.queue(
                queue.clone(),
                awa_worker::QueueConfig {
                    max_workers: *max_workers,
                    poll_interval: Duration::from_millis(poll_interval_ms),
                    ..Default::default()
                },
            );
        }
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

        let runtime = Arc::new(builder.build().map_err(|e| state_error(e.to_string()))?);
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(runtime.start())
            .map_err(map_awa_error)?;
        *self.runtime.lock().expect("runtime mutex poisoned") = Some(runtime);
        Ok(())
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
}

fn normalize_queue_configs(
    queues: Option<Vec<(String, u32)>>,
    entries: &[WorkerEntry],
) -> PyResult<Vec<(String, u32)>> {
    let configured = if let Some(queues) = queues {
        queues
    } else {
        let mut inferred = Vec::new();
        let mut seen = HashSet::new();
        for entry in entries {
            if seen.insert(entry.queue.clone()) {
                inferred.push((entry.queue.clone(), 10));
            }
        }
        inferred
    };

    let configured_names: HashSet<_> = configured.iter().map(|(queue, _)| queue.clone()).collect();
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
                (
                    queue,
                    PyQueueHealth {
                        in_flight: stats.in_flight,
                        max_workers: stats.max_workers,
                        available: stats.available,
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
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unknown job state: {other}"
        ))),
    }
}
