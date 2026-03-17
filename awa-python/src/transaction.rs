use crate::args::{derive_kind, get_type_class_name, serialize_args};
use crate::job::{json_to_py, py_to_json, PyJob};
use awa_model::{InsertOpts, JobRow, JobState};
use chrono::{DateTime, NaiveDateTime, Utc};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sqlx::{Column, PgExecutor, Postgres, Row, Transaction};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Python transaction wrapper for atomic enqueue.
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
    fn execute<'py>(
        &self,
        py: Python<'py>,
        query: String,
        args: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let json_args = to_json_args(py, &args)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = tx_ref(&mut guard)?;
            let result = bind_json_args(sqlx::query(&query), &json_args)
                .execute(&mut **tx_ref)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(result.rows_affected() as i64)
        })
    }

    fn fetch_one<'py>(
        &self,
        py: Python<'py>,
        query: String,
        args: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let json_args = to_json_args(py, &args)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = tx_ref(&mut guard)?;
            let row = bind_json_args(sqlx::query(&query), &json_args)
                .fetch_one(&mut **tx_ref)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            row_to_py_dict(&row)
        })
    }

    fn fetch_optional<'py>(
        &self,
        py: Python<'py>,
        query: String,
        args: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let json_args = to_json_args(py, &args)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = tx_ref(&mut guard)?;
            let row = bind_json_args(sqlx::query(&query), &json_args)
                .fetch_optional(&mut **tx_ref)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            match row {
                Some(row) => row_to_py_dict(&row),
                None => Ok(Python::attach(|py| py.None())),
            }
        })
    }

    fn fetch_all<'py>(
        &self,
        py: Python<'py>,
        query: String,
        args: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let json_args = to_json_args(py, &args)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = tx_ref(&mut guard)?;
            let rows = bind_json_args(sqlx::query(&query), &json_args)
                .fetch_all(&mut **tx_ref)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Python::attach(|py| {
                let list = pyo3::types::PyList::empty(py);
                for row in &rows {
                    list.append(row_to_py_dict(row)?)?;
                }
                Ok(list.unbind())
            })
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
        let tx = self.tx.clone();
        let prepared = Python::attach(|py| {
            prepare_insert(py, args.bind(py), kind, metadata.as_ref(), run_at.as_ref())
        })?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = tx_ref(&mut guard)?;
            let row = insert_raw_job(
                &mut **tx_ref,
                &prepared.kind,
                &prepared.args_json,
                InsertOpts {
                    queue,
                    priority,
                    max_attempts,
                    run_at: prepared.run_at,
                    metadata: prepared.metadata_json,
                    tags,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyJob::from(row))
        })
    }

    #[pyo3(signature = (jobs, *, kind=None, queue="default".to_string(), priority=2, max_attempts=25, tags=vec![], metadata=None, run_at=None))]
    #[allow(clippy::too_many_arguments)]
    fn insert_many<'py>(
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
        let tx = self.tx.clone();
        let prepared_jobs = Python::attach(|py| {
            jobs.iter()
                .map(|job| {
                    prepare_insert(
                        py,
                        job.bind(py),
                        kind.clone(),
                        metadata.as_ref(),
                        run_at.as_ref(),
                    )
                })
                .collect::<PyResult<Vec<_>>>()
        })?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = tx_ref(&mut guard)?;
            let mut inserted = Vec::with_capacity(prepared_jobs.len());
            for prepared in prepared_jobs {
                let row = insert_raw_job(
                    &mut **tx_ref,
                    &prepared.kind,
                    &prepared.args_json,
                    InsertOpts {
                        queue: queue.clone(),
                        priority,
                        max_attempts,
                        run_at: prepared.run_at,
                        metadata: prepared.metadata_json,
                        tags: tags.clone(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                inserted.push(PyJob::from(row));
            }
            Ok(inserted)
        })
    }

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

    fn __aenter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(slf) })
    }

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
            Ok(false)
        })
    }

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

struct PreparedInsert {
    kind: String,
    args_json: serde_json::Value,
    metadata_json: serde_json::Value,
    run_at: Option<DateTime<Utc>>,
}

fn prepare_insert(
    py: Python<'_>,
    args: &Bound<'_, PyAny>,
    kind: Option<String>,
    metadata: Option<&Py<PyAny>>,
    run_at: Option<&Py<PyAny>>,
) -> PyResult<PreparedInsert> {
    let kind = match kind {
        Some(kind) => kind,
        None => {
            let class_name = get_type_class_name(args.get_type().as_any())?;
            derive_kind(&class_name)
        }
    };
    let metadata_json = metadata
        .map(|value| py_to_json(py, value.bind(py)))
        .transpose()?
        .unwrap_or(serde_json::json!({}));
    let run_at = run_at
        .map(|value| parse_run_at(py, value.bind(py)))
        .transpose()?;

    Ok(PreparedInsert {
        kind,
        args_json: serialize_args(py, args)?,
        metadata_json,
        run_at,
    })
}

fn tx_ref<'a>(
    guard: &'a mut Option<Transaction<'static, Postgres>>,
) -> PyResult<&'a mut Transaction<'static, Postgres>> {
    guard.as_mut().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("Transaction already committed or rolled back")
    })
}

fn to_json_args(py: Python<'_>, args: &[Py<PyAny>]) -> PyResult<Vec<serde_json::Value>> {
    args.iter()
        .map(|arg| py_to_json(py, arg.bind(py)))
        .collect::<PyResult<Vec<_>>>()
}

pub fn parse_run_at(_py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<DateTime<Utc>> {
    if let Ok(s) = value.extract::<String>() {
        return parse_datetime_str(&s);
    }

    let iso = value.call_method0("isoformat")?.extract::<String>()?;
    parse_datetime_str(&iso)
}

fn parse_datetime_str(value: &str) -> PyResult<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.with_timezone(&Utc));
    }
    let naive = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
    Ok(DateTime::from_naive_utc_and_offset(naive, Utc))
}

pub async fn insert_raw_job<'e, E>(
    executor: E,
    kind: &str,
    args_json: &serde_json::Value,
    opts: InsertOpts,
) -> Result<JobRow, awa_model::AwaError>
where
    E: PgExecutor<'e>,
{
    let state = if opts.run_at.is_some() {
        JobState::Scheduled
    } else {
        JobState::Available
    };

    sqlx::query_as::<_, JobRow>(
        r#"
        INSERT INTO awa.jobs (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
        VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()), $8, $9)
        RETURNING *
        "#,
    )
    .bind(kind)
    .bind(&opts.queue)
    .bind(args_json)
    .bind(state)
    .bind(opts.priority)
    .bind(opts.max_attempts)
    .bind(opts.run_at)
    .bind(&opts.metadata)
    .bind(&opts.tags)
    .fetch_one(executor)
    .await
    .map_err(awa_model::AwaError::from)
}

pub fn bind_json_args<'q>(
    mut query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    args: &'q [serde_json::Value],
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    for arg in args {
        query = bind_json_arg(query, arg);
    }
    query
}

pub fn bind_json_arg<'q>(
    query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    value: &'q serde_json::Value,
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    match value {
        serde_json::Value::Null => query.bind(None::<serde_json::Value>),
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

pub fn row_to_py_dict(row: &sqlx::postgres::PgRow) -> PyResult<Py<PyAny>> {
    use sqlx::TypeInfo;

    Python::attach(|py| {
        let dict = PyDict::new(py);
        let columns = row.columns();
        for col in columns {
            let name = col.name();
            let type_name = col.type_info().name();

            match type_name {
                "BOOL" => {
                    let val: Option<bool> = row.try_get(name).ok();
                    dict.set_item(name, val)?;
                }
                "INT2" => {
                    let val: Option<i16> = row.try_get(name).ok();
                    dict.set_item(name, val)?;
                }
                "INT4" => {
                    let val: Option<i32> = row.try_get(name).ok();
                    dict.set_item(name, val)?;
                }
                "INT8" => {
                    let val: Option<i64> = row.try_get(name).ok();
                    dict.set_item(name, val)?;
                }
                "FLOAT4" => {
                    let val: Option<f32> = row.try_get(name).ok();
                    dict.set_item(name, val)?;
                }
                "FLOAT8" | "NUMERIC" => {
                    let val: Option<f64> = row.try_get(name).ok();
                    dict.set_item(name, val)?;
                }
                "TEXT" | "VARCHAR" | "CHAR" | "NAME" | "BPCHAR" => {
                    let val: Option<String> = row.try_get(name).ok();
                    dict.set_item(name, val)?;
                }
                "TIMESTAMPTZ" => {
                    let val: Option<chrono::DateTime<chrono::Utc>> = row.try_get(name).ok();
                    dict.set_item(name, val.map(|v| v.to_rfc3339()))?;
                }
                "TIMESTAMP" => {
                    let val: Option<chrono::NaiveDateTime> = row.try_get(name).ok();
                    dict.set_item(name, val.map(|v| v.to_string()))?;
                }
                "JSONB" | "JSON" => {
                    if let Ok(val) = row.try_get::<serde_json::Value, _>(name) {
                        dict.set_item(name, json_to_py(py, &val)?)?;
                    } else {
                        dict.set_item(name, py.None())?;
                    }
                }
                "BYTEA" => {
                    let val: Option<Vec<u8>> = row.try_get(name).ok();
                    dict.set_item(name, val)?;
                }
                "TEXT[]" => {
                    let val: Option<Vec<String>> = row.try_get(name).ok();
                    dict.set_item(name, val)?;
                }
                "JSONB[]" => {
                    if let Ok(val) = row.try_get::<Vec<serde_json::Value>, _>(name) {
                        let list = pyo3::types::PyList::empty(py);
                        for item in &val {
                            list.append(json_to_py(py, item)?)?;
                        }
                        dict.set_item(name, list)?;
                    } else {
                        dict.set_item(name, py.None())?;
                    }
                }
                _ => {
                    if let Ok(val) = row.try_get::<String, _>(name) {
                        dict.set_item(name, val)?;
                    } else {
                        tracing::warn!(
                            column = name,
                            pg_type = type_name,
                            "Unsupported column type in row_to_py_dict, returning None"
                        );
                        dict.set_item(name, py.None())?;
                    }
                }
            }
        }
        Ok(dict.into_any().unbind())
    })
}
