use crate::args::{derive_kind, get_type_class_name, serialize_args};
use crate::job::{json_to_py, py_to_json, PyJob};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sqlx::{Column, Postgres, Row, Transaction};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Python transaction wrapper for atomic enqueue.
///
/// Intentionally narrow: raw SQL with $1/$2 placeholders, dict results.
#[pyclass(name = "Transaction")]
pub struct PyTransaction {
    tx: Arc<Mutex<Option<Transaction<'static, Postgres>>>>,
}

impl PyTransaction {
    pub fn new(tx: Transaction<'static, Postgres>) -> Self {
        Self {
            tx: Arc::new(Mutex::new(Some(tx))),
        }
    }
}

#[pymethods]
impl PyTransaction {
    /// Execute a SQL query, returning the number of affected rows.
    fn execute<'py>(
        &self,
        py: Python<'py>,
        query: String,
        args: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let json_args: Vec<serde_json::Value> = Python::attach(|py| {
            args.iter()
                .map(|a| py_to_json(py, a.bind(py)))
                .collect::<PyResult<Vec<_>>>()
        })?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = guard
                .as_mut()
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Transaction already committed or rolled back"))?;

            let mut q = sqlx::query(&query);
            for arg in &json_args {
                q = bind_json_arg(q, arg);
            }

            let result = q
                .execute(&mut **tx_ref)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            Ok(result.rows_affected() as i64)
        })
    }

    /// Fetch one row as a dict.
    fn fetch_one<'py>(
        &self,
        py: Python<'py>,
        query: String,
        args: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let json_args: Vec<serde_json::Value> = Python::attach(|py| {
            args.iter()
                .map(|a| py_to_json(py, a.bind(py)))
                .collect::<PyResult<Vec<_>>>()
        })?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = guard
                .as_mut()
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Transaction already committed or rolled back"))?;

            let mut q = sqlx::query(&query);
            for arg in &json_args {
                q = bind_json_arg(q, arg);
            }

            let row = q
                .fetch_one(&mut **tx_ref)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            row_to_py_dict(&row)
        })
    }

    /// Insert a job within this transaction.
    #[pyo3(signature = (args, *, kind=None, queue="default".to_string(), priority=2, max_attempts=25, tags=vec![]))]
    fn insert<'py>(
        &self,
        py: Python<'py>,
        args: Py<PyAny>,
        kind: Option<String>,
        queue: String,
        priority: i16,
        max_attempts: i16,
        tags: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();

        let (kind_str, args_json) = Python::attach(|py| {
            let args_bound = args.bind(py);
            let kind_str = match kind {
                Some(k) => k,
                None => {
                    let class_name = get_type_class_name(&args_bound.get_type().as_any())?;
                    derive_kind(&class_name)
                }
            };
            let args_json = serialize_args(py, args_bound)?;
            Ok::<_, PyErr>((kind_str, args_json))
        })?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = guard
                .as_mut()
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Transaction already committed or rolled back"))?;

            let row = sqlx::query_as::<_, awa_model::JobRow>(
                r#"
                INSERT INTO awa.jobs (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
                VALUES ($1, $2, $3, 'available', $4, $5, now(), '{}', $6)
                RETURNING *
                "#,
            )
            .bind(&kind_str)
            .bind(&queue)
            .bind(&args_json)
            .bind(priority)
            .bind(max_attempts)
            .bind(&tags)
            .fetch_one(&mut **tx_ref)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            Ok(PyJob::from(row))
        })
    }

    /// Commit the transaction.
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx = guard.take().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Transaction already committed or rolled back",
                )
            })?;
            tx.commit()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    /// Async context manager entry.
    fn __aenter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        // Return a future that resolves to self
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(slf) })
    }

    /// Async context manager exit — commits on success, rolls back on exception.
    #[pyo3(signature = (exc_type, _exc_val, _exc_tb))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let has_exception = exc_type.is_some();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            if let Some(tx) = guard.take() {
                if has_exception {
                    let _ = tx.rollback().await;
                } else {
                    tx.commit()
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                }
            }
            Ok(false) // Don't suppress exceptions
        })
    }

    /// Roll back the transaction.
    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx = guard.take().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Transaction already committed or rolled back",
                )
            })?;
            tx.rollback()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }
}

/// Bind a serde_json::Value to a sqlx query.
fn bind_json_arg<'q>(
    query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    value: &'q serde_json::Value,
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    match value {
        serde_json::Value::Null => query.bind(None::<String>),
        serde_json::Value::Bool(b) => query.bind(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                query.bind(i)
            } else if let Some(f) = n.as_f64() {
                query.bind(f)
            } else {
                query.bind(n.to_string())
            }
        }
        serde_json::Value::String(s) => query.bind(s.as_str()),
        _ => query.bind(value),
    }
}

/// Convert a PgRow to a Python dict.
fn row_to_py_dict(row: &sqlx::postgres::PgRow) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let dict = PyDict::new(py);
        let columns = row.columns();
        for col in columns {
            let name = col.name();
            // Try common types
            if let Ok(val) = row.try_get::<String, _>(name) {
                dict.set_item(name, val)?;
            } else if let Ok(val) = row.try_get::<i64, _>(name) {
                dict.set_item(name, val)?;
            } else if let Ok(val) = row.try_get::<i32, _>(name) {
                dict.set_item(name, val)?;
            } else if let Ok(val) = row.try_get::<f64, _>(name) {
                dict.set_item(name, val)?;
            } else if let Ok(val) = row.try_get::<bool, _>(name) {
                dict.set_item(name, val)?;
            } else if let Ok(val) = row.try_get::<serde_json::Value, _>(name) {
                dict.set_item(name, json_to_py(py, &val)?)?;
            } else {
                dict.set_item(name, py.None())?;
            }
        }
        Ok(dict.into_any().unbind())
    })
}
