use awa_model::{JobRow, JobState};
use awa_worker::context::ProgressState;
use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Python representation of a callback token.
#[pyclass(frozen, name = "CallbackToken", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyCallbackToken {
    #[pyo3(get)]
    pub id: String,
}

#[pymethods]
impl PyCallbackToken {
    fn __repr__(&self) -> String {
        format!("CallbackToken(id='{}')", self.id)
    }
}

/// Python representation of a job state.
#[pyclass(frozen, eq, eq_int, name = "JobState", skip_from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyJobState {
    Scheduled,
    Available,
    Running,
    Completed,
    Retryable,
    Failed,
    Cancelled,
    WaitingExternal,
}

impl From<JobState> for PyJobState {
    fn from(state: JobState) -> Self {
        match state {
            JobState::Scheduled => PyJobState::Scheduled,
            JobState::Available => PyJobState::Available,
            JobState::Running => PyJobState::Running,
            JobState::Completed => PyJobState::Completed,
            JobState::Retryable => PyJobState::Retryable,
            JobState::Failed => PyJobState::Failed,
            JobState::Cancelled => PyJobState::Cancelled,
            JobState::WaitingExternal => PyJobState::WaitingExternal,
        }
    }
}

/// Python representation of a job row.
#[pyclass(frozen, name = "Job", skip_from_py_object)]
#[derive(Debug)]
pub struct PyJob {
    #[pyo3(get)]
    pub id: i64,
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub queue: String,
    #[pyo3(get)]
    pub state: PyJobState,
    #[pyo3(get)]
    pub priority: i16,
    #[pyo3(get)]
    pub attempt: i16,
    pub run_lease: i64,
    #[pyo3(get)]
    pub max_attempts: i16,
    #[pyo3(get)]
    pub tags: Vec<String>,
    /// Raw args as a Python dict
    pub args_json: serde_json::Value,
    /// Metadata as a Python dict
    pub metadata_json: serde_json::Value,
    args_override: Option<Py<PyAny>>,
    cancelled: Option<Arc<AtomicBool>>,
    /// Database pool for callback registration (only set during dispatch).
    pool: Option<PgPool>,
    /// Shared progress buffer for in-flight reporting (only set during dispatch).
    progress_buffer: Option<Arc<std::sync::Mutex<ProgressState>>>,
    /// Progress JSON from the job row (for queried jobs).
    progress_json: Option<serde_json::Value>,
    pub run_at: DateTime<Utc>,
    pub deadline_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub finalized_at: Option<DateTime<Utc>>,
}

impl Clone for PyJob {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            id: self.id,
            kind: self.kind.clone(),
            queue: self.queue.clone(),
            state: self.state,
            priority: self.priority,
            attempt: self.attempt,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            tags: self.tags.clone(),
            args_json: self.args_json.clone(),
            metadata_json: self.metadata_json.clone(),
            args_override: self.args_override.as_ref().map(|value| value.clone_ref(py)),
            cancelled: self.cancelled.clone(),
            pool: self.pool.clone(),
            progress_buffer: self.progress_buffer.clone(),
            progress_json: self.progress_json.clone(),
            run_at: self.run_at,
            deadline_at: self.deadline_at,
            created_at: self.created_at,
            finalized_at: self.finalized_at,
        })
    }
}

#[pymethods]
impl PyJob {
    #[getter]
    fn args(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(args) = &self.args_override {
            Ok(args.clone_ref(py))
        } else {
            json_to_py(py, &self.args_json)
        }
    }

    #[getter]
    fn metadata(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_to_py(py, &self.metadata_json)
    }

    #[getter]
    fn run_at(&self) -> String {
        self.run_at.to_rfc3339()
    }

    #[getter]
    fn deadline(&self) -> Option<String> {
        self.deadline_at.map(|d| d.to_rfc3339())
    }

    #[getter]
    fn created_at(&self) -> String {
        self.created_at.to_rfc3339()
    }

    #[getter]
    fn finalized_at(&self) -> Option<String> {
        self.finalized_at.map(|d| d.to_rfc3339())
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled
            .as_ref()
            .map(|flag| flag.load(Ordering::SeqCst))
            .unwrap_or(false)
    }

    /// Get progress as a Python dict, or None if no progress has been set.
    #[getter]
    fn progress(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // During dispatch, read from the live buffer
        if let Some(ref buffer) = self.progress_buffer {
            let guard = buffer.lock().expect("progress lock poisoned");
            match guard.latest() {
                Some(value) => return json_to_py(py, value),
                None => return Ok(py.None()),
            }
        }
        // For queried jobs, use the row data
        match &self.progress_json {
            Some(value) => json_to_py(py, value),
            None => Ok(py.None()),
        }
    }

    /// Set structured progress (0-100, optional message). Sync — writes to in-memory buffer.
    #[pyo3(signature = (percent, message=None))]
    fn set_progress(&self, percent: u8, message: Option<String>) -> PyResult<()> {
        let buffer = self.progress_buffer.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "set_progress is only available during job execution",
            )
        })?;

        let mut guard = buffer.lock().expect("progress lock poisoned");
        guard.set_progress(percent, message.as_deref());
        Ok(())
    }

    /// Shallow-merge keys into progress.metadata for checkpointing. Sync.
    fn update_metadata(&self, py: Python<'_>, updates: Py<PyAny>) -> PyResult<()> {
        let buffer = self.progress_buffer.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "update_metadata is only available during job execution",
            )
        })?;

        let updates_json = py_to_json(py, updates.bind(py))?;
        let obj = updates_json.as_object().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("update_metadata requires a dict")
        })?;

        let mut guard = buffer.lock().expect("progress lock poisoned");
        if !guard.merge_metadata(obj) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "progress.metadata is not a JSON object; cannot merge",
            ));
        }
        Ok(())
    }

    /// Force immediate flush of pending progress to DB.
    fn flush_progress<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        use crate::errors::map_awa_error;
        let pool = self.pool.clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "flush_progress is only available during job execution",
            )
        })?;
        let buffer = self.progress_buffer.clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "flush_progress is only available during job execution",
            )
        })?;
        let job_id = self.id;
        let run_lease = self.run_lease;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (snapshot, target_generation) = {
                let guard = buffer.lock().expect("progress lock poisoned");
                match guard.pending_snapshot() {
                    Some(pair) => pair,
                    None => return Ok(()),
                }
            };

            let result = sqlx::query(
                r#"
                UPDATE awa.jobs_hot
                SET progress = $2
                WHERE id = $1 AND state = 'running' AND run_lease = $3
                "#,
            )
            .bind(job_id)
            .bind(&snapshot)
            .bind(run_lease)
            .execute(&pool)
            .await
            .map_err(|e| map_awa_error(awa_model::AwaError::Database(e)))?;

            if result.rows_affected() == 0 {
                // Job was rescued/cancelled — not an error for the caller
                return Ok(());
            }

            let mut guard = buffer.lock().expect("progress lock poisoned");
            guard.ack(target_generation);

            Ok(())
        })
    }

    /// Register a callback for this job, writing the callback_id to the database
    /// immediately. Call this BEFORE sending the callback_id to the external system.
    ///
    /// Optional CEL expression kwargs enable automatic resolution via
    /// `resolve_callback`:
    /// - `filter`: gate expression (bool) — should this payload be processed?
    /// - `on_complete`: success condition (bool)
    /// - `on_fail`: failure condition (bool, evaluated before on_complete)
    /// - `transform`: reshape payload expression (any value)
    #[pyo3(signature = (timeout_seconds=3600.0, filter=None, on_complete=None, on_fail=None, transform=None))]
    fn register_callback<'py>(
        &self,
        py: Python<'py>,
        timeout_seconds: f64,
        filter: Option<String>,
        on_complete: Option<String>,
        on_fail: Option<String>,
        transform: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        use crate::errors::map_awa_error;
        let pool = self.pool.clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "register_callback is only available during job execution",
            )
        })?;
        if !timeout_seconds.is_finite() || timeout_seconds < 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "timeout_seconds must be a finite non-negative number",
            ));
        }
        let job_id = self.id;
        let run_lease = self.run_lease;
        let has_expressions =
            filter.is_some() || on_complete.is_some() || on_fail.is_some() || transform.is_some();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let timeout = std::time::Duration::from_secs_f64(timeout_seconds);
            let callback_id = if has_expressions {
                let config = awa_model::admin::CallbackConfig {
                    filter,
                    on_complete,
                    on_fail,
                    transform,
                };
                awa_model::admin::register_callback_with_config(
                    &pool, job_id, run_lease, timeout, &config,
                )
                .await
                .map_err(map_awa_error)?
            } else {
                awa_model::admin::register_callback(&pool, job_id, run_lease, timeout)
                    .await
                    .map_err(map_awa_error)?
            };
            Ok(PyCallbackToken {
                id: callback_id.to_string(),
            })
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Job(id={}, kind='{}', queue='{}', state={:?}, attempt={})",
            self.id, self.kind, self.queue, self.state, self.attempt
        )
    }
}

impl From<JobRow> for PyJob {
    fn from(row: JobRow) -> Self {
        let progress_json = row.progress.clone();
        PyJob {
            id: row.id,
            kind: row.kind,
            queue: row.queue,
            state: row.state.into(),
            priority: row.priority,
            attempt: row.attempt,
            run_lease: row.run_lease,
            max_attempts: row.max_attempts,
            tags: row.tags,
            args_json: row.args,
            metadata_json: row.metadata,
            args_override: None,
            cancelled: None,
            pool: None,
            progress_buffer: None,
            progress_json,
            run_at: row.run_at,
            deadline_at: row.deadline_at,
            created_at: row.created_at,
            finalized_at: row.finalized_at,
        }
    }
}

impl PyJob {
    pub fn for_dispatch(
        row: JobRow,
        args_override: Py<PyAny>,
        cancelled: Arc<AtomicBool>,
        pool: PgPool,
        progress_buffer: Arc<std::sync::Mutex<ProgressState>>,
    ) -> Self {
        let mut job = Self::from(row);
        job.args_override = Some(args_override);
        job.cancelled = Some(cancelled);
        job.pool = Some(pool);
        job.progress_buffer = Some(progress_buffer);
        job
    }
}

/// Convert serde_json::Value to a Python object.
pub fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b
            .into_pyobject(py)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
            .to_owned()
            .into_any()
            .unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
                    .into_any()
                    .unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
                    .into_any()
                    .unbind())
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s
            .into_pyobject(py)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
            .into_any()
            .unbind()),
        serde_json::Value::Array(arr) => {
            let list = pyo3::types::PyList::empty(py);
            for item in arr {
                list.append(json_to_py(py, item)?)?;
            }
            Ok(list.into_any().unbind())
        }
        serde_json::Value::Object(obj) => {
            let dict = pyo3::types::PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

/// Convert a Python object to serde_json::Value.
///
/// Recurses into lists and dicts with a depth limit to prevent stack overflow
/// from circular Python references.
pub fn py_to_json(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    py_to_json_inner(py, obj, 0)
}

const MAX_JSON_DEPTH: usize = 64;

#[allow(clippy::only_used_in_recursion)]
fn py_to_json_inner(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    depth: usize,
) -> PyResult<serde_json::Value> {
    if depth > MAX_JSON_DEPTH {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "JSON nesting too deep (max 64 levels)",
        ));
    }

    if obj.is_none() {
        Ok(serde_json::Value::Null)
    } else if let Ok(b) = obj.extract::<bool>() {
        Ok(serde_json::Value::Bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(serde_json::json!(i))
    } else if let Ok(f) = obj.extract::<f64>() {
        Ok(serde_json::json!(f))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(serde_json::Value::String(s))
    } else if let Ok(list) = obj.cast::<pyo3::types::PyList>() {
        let mut arr = Vec::new();
        for item in list.iter() {
            arr.push(py_to_json_inner(py, &item, depth + 1)?);
        }
        Ok(serde_json::Value::Array(arr))
    } else if let Ok(dict) = obj.cast::<pyo3::types::PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, py_to_json_inner(py, &v, depth + 1)?);
        }
        Ok(serde_json::Value::Object(map))
    } else {
        // Try to use __dict__ for dataclass-like objects
        if let Ok(dict) = obj.getattr("__dict__") {
            py_to_json_inner(py, &dict, depth + 1)
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "Cannot convert {} to JSON",
                obj.get_type().qualname()?
            )))
        }
    }
}
