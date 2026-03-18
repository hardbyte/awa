use crate::client::{PyCancel, PyRetryAfter, PySnooze, WorkerEntry};
use crate::job::{json_to_py, PyJob};
use awa_model::JobRow;
use awa_worker::{JobContext, JobError, JobResult, Worker};
use pyo3::prelude::*;
use std::fmt;

#[derive(Debug)]
struct PythonHandlerError {
    message: String,
}

impl fmt::Display for PythonHandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PythonHandlerError {}

pub struct PythonWorker {
    kind: &'static str,
    handler: Py<PyAny>,
    args_type: Py<PyAny>,
    task_locals: pyo3_async_runtimes::TaskLocals,
}

impl PythonWorker {
    pub fn from_entry(entry: &WorkerEntry) -> Self {
        Python::attach(|py| Self {
            kind: Box::leak(entry.kind.clone().into_boxed_str()),
            handler: entry.handler.clone_ref(py),
            args_type: entry.args_type.clone_ref(py),
            task_locals: entry.task_locals.clone(),
        })
    }
}

#[async_trait::async_trait]
impl Worker for PythonWorker {
    fn kind(&self) -> &'static str {
        self.kind
    }

    async fn perform(&self, job_row: &JobRow, ctx: &JobContext) -> Result<JobResult, JobError> {
        let handler = Python::attach(|py| self.handler.clone_ref(py));
        let args_type = Python::attach(|py| self.args_type.clone_ref(py));
        let task_locals = self.task_locals.clone();
        let py_job = Python::attach(|py| build_dispatch_job(py, job_row.clone(), &args_type, ctx))
            .map_err(|err| classify_python_error(err, true))?;

        let future = Python::attach(|py| {
            let coro = handler.call1(py, (py_job,))?;
            pyo3_async_runtimes::into_future_with_locals(&task_locals, coro.into_bound(py))
        })
        .map_err(|err| classify_python_error(err, true))?;

        let value = future
            .await
            .map_err(|err| classify_python_error(err, false))?;

        classify_handler_result(value).map_err(|err| classify_python_error(err, true))
    }
}

fn build_dispatch_job(
    py: Python<'_>,
    job_row: JobRow,
    args_type: &Py<PyAny>,
    ctx: &JobContext,
) -> PyResult<Py<PyAny>> {
    let args_json = json_to_py(py, &job_row.args)?;
    let args_instance = if args_type.bind(py).hasattr("model_validate")? {
        args_type.call_method1(py, "model_validate", (args_json,))?
    } else if args_type.bind(py).hasattr("__dataclass_fields__")? {
        let kwargs = args_json
            .bind(py)
            .cast::<pyo3::types::PyDict>()
            .map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err("expected args to deserialize into a dict")
            })?;
        args_type.call(py, (), Some(kwargs))?
    } else {
        args_json
    };

    let job = PyJob::for_dispatch(job_row, args_instance, ctx.cancellation_flag());
    Ok(Py::new(py, job)?.into_bound(py).into_any().unbind())
}

fn classify_handler_result(value: Py<PyAny>) -> PyResult<JobResult> {
    Python::attach(|py| {
        let result = value.bind(py);
        if result.is_none() {
            Ok(JobResult::Completed)
        } else if result.is_instance_of::<PyRetryAfter>() {
            let seconds: f64 = result.getattr("seconds")?.extract()?;
            Ok(JobResult::RetryAfter(std::time::Duration::from_secs_f64(
                seconds.max(0.0),
            )))
        } else if result.is_instance_of::<PySnooze>() {
            let seconds: f64 = result.getattr("seconds")?.extract()?;
            Ok(JobResult::Snooze(std::time::Duration::from_secs_f64(
                seconds.max(0.0),
            )))
        } else if result.is_instance_of::<PyCancel>() {
            let reason: String = result.getattr("reason")?.extract()?;
            Ok(JobResult::Cancel(reason))
        } else {
            Ok(JobResult::Completed)
        }
    })
}

fn classify_python_error(err: PyErr, force_terminal: bool) -> JobError {
    Python::attach(|py| {
        let error_type = err
            .get_type(py)
            .qualname()
            .map(|name| name.to_string())
            .unwrap_or_else(|_| "UnknownPythonError".to_string());
        let error_message = err.value(py).to_string();
        let traceback = err
            .traceback(py)
            .and_then(|tb| tb.format().ok())
            .unwrap_or_default();
        let formatted = if traceback.is_empty() {
            format!("{error_type}: {error_message}")
        } else {
            format!("{error_type}: {error_message}\n{traceback}")
        };
        let is_terminal = force_terminal
            || py
                .import("awa")
                .and_then(|module| module.getattr("TerminalError"))
                .map(|terminal| err.get_type(py).is_subclass(&terminal).unwrap_or(false))
                .unwrap_or(false);

        if is_terminal {
            JobError::terminal(formatted)
        } else {
            JobError::retryable(PythonHandlerError { message: formatted })
        }
    })
}
