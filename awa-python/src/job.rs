use awa_model::{JobRow, JobState};
use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
            max_attempts: self.max_attempts,
            tags: self.tags.clone(),
            args_json: self.args_json.clone(),
            metadata_json: self.metadata_json.clone(),
            args_override: self.args_override.as_ref().map(|value| value.clone_ref(py)),
            cancelled: self.cancelled.clone(),
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

    fn __repr__(&self) -> String {
        format!(
            "Job(id={}, kind='{}', queue='{}', state={:?}, attempt={})",
            self.id, self.kind, self.queue, self.state, self.attempt
        )
    }
}

impl From<JobRow> for PyJob {
    fn from(row: JobRow) -> Self {
        PyJob {
            id: row.id,
            kind: row.kind,
            queue: row.queue,
            state: row.state.into(),
            priority: row.priority,
            attempt: row.attempt,
            max_attempts: row.max_attempts,
            tags: row.tags,
            args_json: row.args,
            metadata_json: row.metadata,
            args_override: None,
            cancelled: None,
            run_at: row.run_at,
            deadline_at: row.deadline_at,
            created_at: row.created_at,
            finalized_at: row.finalized_at,
        }
    }
}

impl PyJob {
    pub fn for_dispatch(row: JobRow, args_override: Py<PyAny>, cancelled: Arc<AtomicBool>) -> Self {
        let mut job = Self::from(row);
        job.args_override = Some(args_override);
        job.cancelled = Some(cancelled);
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
