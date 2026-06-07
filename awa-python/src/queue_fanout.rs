use crate::errors::validation_error;
use crate::transaction::parse_ordering_key;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyType};

#[pyclass(frozen, name = "QueueFanout", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyQueueFanout {
    inner: awa_model::QueueFanout,
}

impl PyQueueFanout {
    fn from_inner(inner: awa_model::QueueFanout) -> Self {
        Self { inner }
    }

    fn positive_width(width: i64) -> PyResult<usize> {
        if width <= 0 {
            return Err(validation_error("queue fanout width must be > 0"));
        }
        Ok(width as usize)
    }

    fn non_negative_index(index: i64) -> PyResult<usize> {
        if index < 0 {
            return Err(validation_error("index must be >= 0"));
        }
        Ok(index as usize)
    }
}

fn map_fanout_error(err: awa_model::QueueFanoutError) -> PyErr {
    validation_error(err.to_string())
}

#[pymethods]
impl PyQueueFanout {
    #[new]
    #[pyo3(signature = (logical_queue, width))]
    fn new(logical_queue: String, width: i64) -> PyResult<Self> {
        let width = Self::positive_width(width)?;
        awa_model::QueueFanout::new(logical_queue, width)
            .map(Self::from_inner)
            .map_err(map_fanout_error)
    }

    #[classmethod]
    fn from_physical_queues(
        _cls: &Bound<'_, PyType>,
        logical_queue: String,
        physical_queues: Vec<String>,
    ) -> PyResult<Self> {
        awa_model::QueueFanout::from_physical_queues(logical_queue, physical_queues)
            .map(Self::from_inner)
            .map_err(map_fanout_error)
    }

    #[getter]
    fn logical_queue(&self) -> &str {
        self.inner.logical_queue()
    }

    #[getter]
    fn physical_queues(&self) -> Vec<String> {
        self.inner.physical_queues().to_vec()
    }

    #[getter]
    fn width(&self) -> usize {
        self.inner.width()
    }

    fn queue_for_key(&self, py: Python<'_>, key: Py<PyAny>) -> PyResult<String> {
        let key = parse_ordering_key(py, key.bind(py))?;
        Ok(self.inner.queue_for_key(&key).to_string())
    }

    fn queue_for_index(&self, index: i64) -> PyResult<String> {
        let index = Self::non_negative_index(index)?;
        Ok(self.inner.queue_for_index(index).to_string())
    }

    /// Return keyword arguments for inserting a job routed by key.
    ///
    /// The returned dict has `queue` and `ordering_key`, so it can be passed
    /// directly to `client.insert(..., **fanout.route_by_key(key))`.
    fn route_by_key(&self, py: Python<'_>, key: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let key = parse_ordering_key(py, key.bind(py))?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("queue", self.inner.queue_for_key(&key))?;
        kwargs.set_item("ordering_key", PyBytes::new(py, &key))?;
        Ok(kwargs.into_any().unbind())
    }

    /// Return keyword arguments for inserting a job routed by round-robin index.
    fn route_by_index(&self, py: Python<'_>, index: i64) -> PyResult<Py<PyAny>> {
        let index = Self::non_negative_index(index)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("queue", self.inner.queue_for_index(index))?;
        Ok(kwargs.into_any().unbind())
    }

    /// Expand the fanout into Python `start()` queue config dictionaries.
    ///
    /// Capacity arguments are named per physical queue because the returned
    /// config declares each physical queue independently.
    #[pyo3(signature = (max_workers_per_queue=None, *, min_workers_per_queue=None, weight=1, rate_limit_per_queue=None, priority_aging_interval_ms=None, deadline_duration_ms=None, claimers=None, claim_batch_size=None))]
    #[allow(clippy::too_many_arguments)]
    fn queue_configs(
        &self,
        py: Python<'_>,
        max_workers_per_queue: Option<u32>,
        min_workers_per_queue: Option<u32>,
        weight: u32,
        rate_limit_per_queue: Option<Py<PyAny>>,
        priority_aging_interval_ms: Option<u64>,
        deadline_duration_ms: Option<u64>,
        claimers: Option<u16>,
        claim_batch_size: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        match (max_workers_per_queue, min_workers_per_queue) {
            (Some(_), Some(_)) => {
                return Err(validation_error(
                    "use max_workers_per_queue or min_workers_per_queue, not both",
                ));
            }
            (None, None) => {
                return Err(validation_error(
                    "queue_configs requires max_workers_per_queue or min_workers_per_queue",
                ));
            }
            (Some(0), None) => {
                return Err(validation_error("max_workers_per_queue must be > 0"));
            }
            _ => {}
        }
        if weight == 0 {
            return Err(validation_error("weight must be > 0"));
        }
        if claimers == Some(0) {
            return Err(validation_error("claimers must be > 0"));
        }
        if claim_batch_size == Some(0) {
            return Err(validation_error("claim_batch_size must be > 0"));
        }

        let configs = PyList::empty(py);
        for queue in self.inner.physical_queues() {
            let config = PyDict::new(py);
            config.set_item("name", queue)?;
            if let Some(max_workers) = max_workers_per_queue {
                config.set_item("max_workers", max_workers)?;
            }
            if let Some(min_workers) = min_workers_per_queue {
                config.set_item("min_workers", min_workers)?;
                config.set_item("weight", weight)?;
            }
            if let Some(rate_limit) = rate_limit_per_queue.as_ref() {
                config.set_item("rate_limit", rate_limit.bind(py))?;
            }
            if let Some(value) = priority_aging_interval_ms {
                config.set_item("priority_aging_interval_ms", value)?;
            }
            if let Some(value) = deadline_duration_ms {
                config.set_item("deadline_duration_ms", value)?;
            }
            if let Some(value) = claimers {
                config.set_item("claimers", value)?;
            }
            if let Some(value) = claim_batch_size {
                config.set_item("claim_batch_size", value)?;
            }
            configs.append(config)?;
        }

        Ok(configs.into_any().unbind())
    }

    fn __len__(&self) -> usize {
        self.inner.width()
    }

    fn __repr__(&self) -> String {
        format!(
            "QueueFanout(logical_queue={:?}, width={})",
            self.inner.logical_queue(),
            self.inner.width()
        )
    }
}
