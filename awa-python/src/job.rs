use awa_model::{JobRow, JobState};
use chrono::{DateTime, Utc};
use pyo3::prelude::*;

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
#[derive(Debug, Clone)]
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
    pub run_at: DateTime<Utc>,
    pub deadline_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub finalized_at: Option<DateTime<Utc>>,
}

#[pymethods]
impl PyJob {
    #[getter]
    fn args(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_to_py(py, &self.args_json)
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
            run_at: row.run_at,
            deadline_at: row.deadline_at,
            created_at: row.created_at,
            finalized_at: row.finalized_at,
        }
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

#[allow(clippy::only_used_in_recursion)]
/// Convert a Python object to serde_json::Value.
pub fn py_to_json(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
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
            arr.push(py_to_json(py, &item)?);
        }
        Ok(serde_json::Value::Array(arr))
    } else if let Ok(dict) = obj.cast::<pyo3::types::PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, py_to_json(py, &v)?);
        }
        Ok(serde_json::Value::Object(map))
    } else {
        // Try to use __dict__ for dataclass-like objects
        if let Ok(dict) = obj.getattr("__dict__") {
            py_to_json(py, &dict)
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "Cannot convert {} to JSON",
                obj.get_type().qualname()?
            )))
        }
    }
}
