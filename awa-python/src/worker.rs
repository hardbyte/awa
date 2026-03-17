//! Python worker integration on top of the shared Rust runtime.

use crate::client::{PyCancel, PyRetryAfter, PySnooze, WorkerEntry};
use crate::job::json_to_py;
use awa_model::JobRow;
use awa_worker::{BuildError, Client, JobContext, JobError, JobResult, QueueConfig, Worker};
use pyo3::prelude::*;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// SAFETY: Extension module - interpreter is always initialized.
fn with_gil<F, R>(f: F) -> R
where
    F: for<'py> FnOnce(Python<'py>) -> R,
{
    unsafe { Python::attach_unchecked(f) }
}

pub async fn build_runtime_client(
    pool: PgPool,
    workers: Arc<RwLock<HashMap<String, WorkerEntry>>>,
    registered_kinds: Vec<String>,
    queues: Vec<(String, u32)>,
    poll_interval: Duration,
) -> PyResult<Client> {
    let mut builder = Client::builder(pool).state(PythonWorkerRegistry::new(workers));

    for (name, max_workers) in queues {
        builder = builder.queue(
            name,
            QueueConfig {
                max_workers,
                poll_interval,
                ..Default::default()
            },
        );
    }

    for kind in registered_kinds {
        builder = builder.register_worker(PythonWorker::new(kind));
    }

    builder.build().map_err(map_build_error)
}

fn map_build_error(error: BuildError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(error.to_string())
}

#[derive(Clone)]
struct PythonWorkerRegistry {
    workers: Arc<RwLock<HashMap<String, WorkerEntry>>>,
}

impl PythonWorkerRegistry {
    fn new(workers: Arc<RwLock<HashMap<String, WorkerEntry>>>) -> Self {
        Self { workers }
    }

    async fn lookup(&self, kind: &str) -> Option<WorkerEntry> {
        let guard = self.workers.read().await;
        guard.get(kind).map(clone_worker_entry)
    }
}

fn clone_worker_entry(entry: &WorkerEntry) -> WorkerEntry {
    with_gil(|py| WorkerEntry {
        kind: entry.kind.clone(),
        handler: entry.handler.clone_ref(py),
        args_type: entry.args_type.clone_ref(py),
        queue: entry.queue.clone(),
    })
}

struct PythonWorker {
    kind: &'static str,
}

impl PythonWorker {
    fn new(kind: String) -> Self {
        Self {
            kind: Box::leak(kind.into_boxed_str()),
        }
    }
}

#[async_trait::async_trait]
impl Worker for PythonWorker {
    fn kind(&self) -> &'static str {
        self.kind
    }

    async fn perform(&self, job_row: &JobRow, ctx: &JobContext) -> Result<JobResult, JobError> {
        let registry = ctx
            .extract::<PythonWorkerRegistry>()
            .ok_or_else(|| JobError::terminal("python worker registry missing"))?;

        let entry = registry.lookup(&job_row.kind).await.ok_or_else(|| {
            JobError::terminal(format!("unknown python job kind: {}", job_row.kind))
        })?;

        run_python_handler(entry, job_row, ctx)
    }
}

fn run_python_handler(
    entry: WorkerEntry,
    job_row: &JobRow,
    ctx: &JobContext,
) -> Result<JobResult, JobError> {
    let job = job_row.clone();
    let cancelled = ctx.is_cancelled();

    tokio::task::block_in_place(|| {
        with_gil(|py| {
            let args_json = json_to_py(py, &job.args)
                .map_err(|err| JobError::terminal(format!("args deserialization failed: {err}")))?;

            let args_inst = if entry
                .args_type
                .bind(py)
                .hasattr("model_validate")
                .unwrap_or(false)
            {
                entry
                    .args_type
                    .call_method1(py, "model_validate", (args_json.clone_ref(py),))
                    .map_err(classify_py_error)?
            } else if entry
                .args_type
                .bind(py)
                .hasattr("__dataclass_fields__")
                .unwrap_or(false)
            {
                match args_json.bind(py).cast::<pyo3::types::PyDict>() {
                    Ok(kwargs) => entry
                        .args_type
                        .call(py, (), Some(kwargs))
                        .map_err(classify_py_error)?,
                    Err(_) => args_json,
                }
            } else {
                args_json
            };

            let namespace = build_job_namespace(py, &job, args_inst, cancelled)?;
            let result = entry
                .handler
                .call1(py, (namespace,))
                .map_err(classify_py_error)?;

            let inspect = py.import("inspect").map_err(classify_py_error)?;
            let is_coroutine = inspect
                .call_method1("iscoroutine", (&result,))
                .and_then(|value| value.extract::<bool>())
                .map_err(classify_py_error)?;

            if is_coroutine {
                let asyncio = py.import("asyncio").map_err(classify_py_error)?;
                let runner = asyncio.getattr("run").map_err(classify_py_error)?;
                let awaited = runner.call1((result,)).map_err(classify_py_error)?;
                classify_return_value(awaited.unbind())
            } else {
                classify_return_value(result)
            }
        })
    })
}

fn build_job_namespace(
    py: Python<'_>,
    job_row: &JobRow,
    args_inst: Py<PyAny>,
    cancelled: bool,
) -> Result<Py<PyAny>, JobError> {
    let dict = pyo3::types::PyDict::new(py);
    dict.set_item("args", args_inst)
        .map_err(classify_py_error)?;
    dict.set_item("id", job_row.id).map_err(classify_py_error)?;
    dict.set_item("kind", &job_row.kind)
        .map_err(classify_py_error)?;
    dict.set_item("queue", &job_row.queue)
        .map_err(classify_py_error)?;
    dict.set_item("attempt", job_row.attempt)
        .map_err(classify_py_error)?;
    dict.set_item("max_attempts", job_row.max_attempts)
        .map_err(classify_py_error)?;
    dict.set_item("priority", job_row.priority)
        .map_err(classify_py_error)?;
    dict.set_item(
        "metadata",
        json_to_py(py, &job_row.metadata).map_err(classify_py_error)?,
    )
    .map_err(classify_py_error)?;
    dict.set_item("tags", &job_row.tags)
        .map_err(classify_py_error)?;
    dict.set_item(
        "deadline",
        job_row.deadline_at.map(|deadline| deadline.to_rfc3339()),
    )
    .map_err(classify_py_error)?;
    dict.set_item("is_cancelled", cancelled)
        .map_err(classify_py_error)?;

    py.import("types")
        .and_then(|module| module.getattr("SimpleNamespace"))
        .and_then(|cls| cls.call((), Some(&dict)))
        .map(|namespace| namespace.unbind())
        .map_err(classify_py_error)
}

fn classify_return_value(value: Py<PyAny>) -> Result<JobResult, JobError> {
    with_gil(|py| {
        let value = value.bind(py);

        if value.is_none() {
            Ok(JobResult::Completed)
        } else if value.is_instance_of::<PyRetryAfter>() {
            let seconds = value
                .getattr("seconds")
                .and_then(|v| v.extract::<f64>())
                .unwrap_or(60.0);
            Ok(JobResult::RetryAfter(Duration::from_secs_f64(seconds)))
        } else if value.is_instance_of::<PySnooze>() {
            let seconds = value
                .getattr("seconds")
                .and_then(|v| v.extract::<f64>())
                .unwrap_or(60.0);
            Ok(JobResult::Snooze(Duration::from_secs_f64(seconds)))
        } else if value.is_instance_of::<PyCancel>() {
            let reason = value
                .getattr("reason")
                .and_then(|v| v.extract::<String>())
                .unwrap_or_else(|_| "cancelled by handler".to_string());
            Ok(JobResult::Cancel(reason))
        } else {
            Ok(JobResult::Completed)
        }
    })
}

fn classify_py_error(err: PyErr) -> JobError {
    with_gil(|py| {
        let is_terminal = py
            .import("awa")
            .and_then(|module| module.getattr("TerminalError"))
            .map(|terminal| err.get_type(py).is_subclass(&terminal).unwrap_or(false))
            .unwrap_or(false);

        if is_terminal {
            JobError::terminal(err.value(py).to_string())
        } else {
            JobError::retryable(std::io::Error::other(err.value(py).to_string()))
        }
    })
}
