use crate::args::{derive_kind, get_type_class_name, serialize_args};
use crate::errors::{map_awa_error, map_sqlx_error, state_error, validation_error};
use crate::job::{json_to_py, py_to_json, PyJob};
use awa_model::unique::compute_unique_key;
use awa_model::{InsertOpts, JobRow, JobState, UniqueOpts};
use chrono::{DateTime, NaiveDateTime, Utc};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyTuple;
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
    #[pyo3(signature = (query, *args))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        query: String,
        args: &Bound<'_, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let json_args = to_json_args(py, args)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = tx_ref(&mut guard)?;
            let result = bind_json_args(sqlx::query(&query), &json_args)
                .execute(&mut **tx_ref)
                .await
                .map_err(map_sqlx_error)?;
            Ok(result.rows_affected() as i64)
        })
    }

    #[pyo3(signature = (query, *args))]
    fn fetch_one<'py>(
        &self,
        py: Python<'py>,
        query: String,
        args: &Bound<'_, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let json_args = to_json_args(py, args)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = tx_ref(&mut guard)?;
            let row = bind_json_args(sqlx::query(&query), &json_args)
                .fetch_one(&mut **tx_ref)
                .await
                .map_err(map_sqlx_error)?;
            row_to_py_dict(&row)
        })
    }

    #[pyo3(signature = (query, *args))]
    fn fetch_optional<'py>(
        &self,
        py: Python<'py>,
        query: String,
        args: &Bound<'_, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let json_args = to_json_args(py, args)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = tx_ref(&mut guard)?;
            let row = bind_json_args(sqlx::query(&query), &json_args)
                .fetch_optional(&mut **tx_ref)
                .await
                .map_err(map_sqlx_error)?;
            match row {
                Some(row) => row_to_py_dict(&row),
                None => Ok(Python::attach(|py| py.None())),
            }
        })
    }

    #[pyo3(signature = (query, *args))]
    fn fetch_all<'py>(
        &self,
        py: Python<'py>,
        query: String,
        args: &Bound<'_, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let json_args = to_json_args(py, args)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx_ref = tx_ref(&mut guard)?;
            let rows = bind_json_args(sqlx::query(&query), &json_args)
                .fetch_all(&mut **tx_ref)
                .await
                .map_err(map_sqlx_error)?;
            Python::attach(|py| {
                let list = pyo3::types::PyList::empty(py);
                for row in &rows {
                    list.append(row_to_py_dict(row)?)?;
                }
                Ok(list.unbind())
            })
        })
    }

    #[pyo3(signature = (args, *, kind=None, queue="default".to_string(), priority=2, max_attempts=25, tags=vec![], metadata=None, run_at=None, unique_opts=None))]
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
        unique_opts: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let prepared = Python::attach(|py| {
            prepare_insert(py, args.bind(py), kind, metadata.as_ref(), run_at.as_ref())
        })?;
        let unique = Python::attach(|py| {
            unique_opts
                .as_ref()
                .map(|value| parse_unique_opts(py, value.bind(py)))
                .transpose()
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
                    unique,
                    ..Default::default()
                },
            )
            .await
            .map_err(map_awa_error)?;
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
                .map_err(map_awa_error)?;
                inserted.push(PyJob::from(row));
            }
            Ok(inserted)
        })
    }

    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx = guard
                .take()
                .ok_or_else(|| state_error("Transaction already committed or rolled back"))?;
            tx.commit().await.map_err(map_sqlx_error)?;
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
                    tx.commit().await.map_err(map_sqlx_error)?;
                }
            }
            Ok(false)
        })
    }

    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = tx.lock().await;
            let tx = guard
                .take()
                .ok_or_else(|| state_error("Transaction already committed or rolled back"))?;
            tx.rollback().await.map_err(map_sqlx_error)?;
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
    guard
        .as_mut()
        .ok_or_else(|| state_error("Transaction already committed or rolled back"))
}

fn to_json_args(py: Python<'_>, args: &Bound<'_, PyTuple>) -> PyResult<Vec<serde_json::Value>> {
    args.iter()
        .map(|arg| py_to_json(py, &arg))
        .collect::<PyResult<Vec<_>>>()
}

pub fn parse_run_at(_py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<DateTime<Utc>> {
    if let Ok(s) = value.extract::<String>() {
        return parse_datetime_str(&s);
    }

    let iso = value
        .call_method0("isoformat")
        .map_err(|_| validation_error("run_at must be a datetime or RFC3339 string"))?
        .extract::<String>()
        .map_err(|_| validation_error("run_at must be a datetime or RFC3339 string"))?;
    parse_datetime_str(&iso)
}

fn parse_datetime_str(value: &str) -> PyResult<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.with_timezone(&Utc));
    }
    let naive = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
        .map_err(|err| validation_error(err.to_string()))?;
    Ok(DateTime::from_naive_utc_and_offset(naive, Utc))
}

/// Parse a Python dict into UniqueOpts.
///
/// Accepted keys: by_queue (bool), by_args (bool), by_period (int), states (int bitmask).
/// All keys are optional and default to UniqueOpts::default().
pub fn parse_unique_opts(_py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<UniqueOpts> {
    let dict = value
        .downcast::<PyDict>()
        .map_err(|_| validation_error("unique_opts must be a dict"))?;

    let mut opts = UniqueOpts::default();

    if let Some(val) = dict.get_item("by_queue")? {
        opts.by_queue = val.extract::<bool>()?;
    }
    if let Some(val) = dict.get_item("by_args")? {
        opts.by_args = val.extract::<bool>()?;
    }
    if let Some(val) = dict.get_item("by_period")? {
        opts.by_period = Some(val.extract::<i64>()?);
    }
    if let Some(val) = dict.get_item("states")? {
        opts.states = val.extract::<u8>()?;
    }

    Ok(opts)
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

    let unique_key = opts.unique.as_ref().map(|u| {
        compute_unique_key(
            kind,
            if u.by_queue { Some(&opts.queue) } else { None },
            if u.by_args { Some(args_json) } else { None },
            u.by_period,
        )
    });

    let unique_states: Option<String> = opts.unique.as_ref().map(|u| {
        let mut bits = String::with_capacity(8);
        for bit in 0..8 {
            bits.push(if u.states & (1 << bit) != 0 { '1' } else { '0' });
        }
        bits
    });

    sqlx::query_as::<_, JobRow>(
        r#"
        INSERT INTO awa.jobs (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags, unique_key, unique_states)
        VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()), $8, $9, $10, $11::bit(8))
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
    .bind(&unique_key)
    .bind(&unique_states)
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

/// Synchronous transaction wrapper for Django/Flask web handlers.
///
/// Provides the same operations as `Transaction` but uses `block_on()`
/// instead of async, and supports `__enter__`/`__exit__` (not async with).
#[pyclass(name = "SyncTransaction")]
pub struct PySyncTransaction {
    tx: Arc<Mutex<Option<Transaction<'static, Postgres>>>>,
}

impl PySyncTransaction {
    pub fn new(tx: Transaction<'static, Postgres>) -> Self {
        Self {
            tx: Arc::new(Mutex::new(Some(tx))),
        }
    }
}

fn sync_tx_ref<'a>(
    guard: &'a mut Option<Transaction<'static, Postgres>>,
) -> PyResult<&'a mut Transaction<'static, Postgres>> {
    guard
        .as_mut()
        .ok_or_else(|| state_error("Transaction already committed or rolled back"))
}

#[pymethods]
impl PySyncTransaction {
    #[pyo3(signature = (query, *args))]
    fn execute(&self, py: Python<'_>, query: String, args: &Bound<'_, PyTuple>) -> PyResult<i64> {
        let json_args = to_json_args(py, args)?;
        let tx = self.tx.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut guard = tx.lock().await;
                let tx_ref = sync_tx_ref(&mut guard)?;
                let result = bind_json_args(sqlx::query(&query), &json_args)
                    .execute(&mut **tx_ref)
                    .await
                    .map_err(map_sqlx_error)?;
                Ok(result.rows_affected() as i64)
            })
        })
    }

    #[pyo3(signature = (query, *args))]
    fn fetch_one(
        &self,
        py: Python<'_>,
        query: String,
        args: &Bound<'_, PyTuple>,
    ) -> PyResult<Py<PyAny>> {
        let json_args = to_json_args(py, args)?;
        let tx = self.tx.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut guard = tx.lock().await;
                let tx_ref = sync_tx_ref(&mut guard)?;
                let row = bind_json_args(sqlx::query(&query), &json_args)
                    .fetch_one(&mut **tx_ref)
                    .await
                    .map_err(map_sqlx_error)?;
                row_to_py_dict(&row)
            })
        })
    }

    #[pyo3(signature = (query, *args))]
    fn fetch_optional(
        &self,
        py: Python<'_>,
        query: String,
        args: &Bound<'_, PyTuple>,
    ) -> PyResult<Py<PyAny>> {
        let json_args = to_json_args(py, args)?;
        let tx = self.tx.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut guard = tx.lock().await;
                let tx_ref = sync_tx_ref(&mut guard)?;
                let row = bind_json_args(sqlx::query(&query), &json_args)
                    .fetch_optional(&mut **tx_ref)
                    .await
                    .map_err(map_sqlx_error)?;
                match row {
                    Some(row) => row_to_py_dict(&row),
                    None => Ok(Python::attach(|py| py.None())),
                }
            })
        })
    }

    #[pyo3(signature = (query, *args))]
    fn fetch_all(
        &self,
        py: Python<'_>,
        query: String,
        args: &Bound<'_, PyTuple>,
    ) -> PyResult<Py<PyAny>> {
        let json_args = to_json_args(py, args)?;
        let tx = self.tx.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut guard = tx.lock().await;
                let tx_ref = sync_tx_ref(&mut guard)?;
                let rows = bind_json_args(sqlx::query(&query), &json_args)
                    .fetch_all(&mut **tx_ref)
                    .await
                    .map_err(map_sqlx_error)?;
                Python::attach(|py| {
                    let list = pyo3::types::PyList::empty(py);
                    for row in &rows {
                        list.append(row_to_py_dict(row)?)?;
                    }
                    Ok(list.into_any().unbind())
                })
            })
        })
    }

    #[pyo3(signature = (args, *, kind=None, queue="default".to_string(), priority=2, max_attempts=25, tags=vec![], metadata=None, run_at=None, unique_opts=None))]
    #[allow(clippy::too_many_arguments)]
    fn insert(
        &self,
        py: Python<'_>,
        args: Py<PyAny>,
        kind: Option<String>,
        queue: String,
        priority: i16,
        max_attempts: i16,
        tags: Vec<String>,
        metadata: Option<Py<PyAny>>,
        run_at: Option<Py<PyAny>>,
        unique_opts: Option<Py<PyAny>>,
    ) -> PyResult<PyJob> {
        let prepared = prepare_insert(py, args.bind(py), kind, metadata.as_ref(), run_at.as_ref())?;
        let unique = unique_opts
            .as_ref()
            .map(|value| parse_unique_opts(py, value.bind(py)))
            .transpose()?;
        let tx = self.tx.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut guard = tx.lock().await;
                let tx_ref = sync_tx_ref(&mut guard)?;
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
                        unique,
                        ..Default::default()
                    },
                )
                .await
                .map_err(map_awa_error)?;
                Ok(PyJob::from(row))
            })
        })
    }

    #[pyo3(signature = (jobs, *, kind=None, queue="default".to_string(), priority=2, max_attempts=25, tags=vec![], metadata=None, run_at=None))]
    #[allow(clippy::too_many_arguments)]
    fn insert_many(
        &self,
        py: Python<'_>,
        jobs: Vec<Py<PyAny>>,
        kind: Option<String>,
        queue: String,
        priority: i16,
        max_attempts: i16,
        tags: Vec<String>,
        metadata: Option<Py<PyAny>>,
        run_at: Option<Py<PyAny>>,
    ) -> PyResult<Vec<PyJob>> {
        let prepared_jobs = jobs
            .iter()
            .map(|job| {
                prepare_insert(
                    py,
                    job.bind(py),
                    kind.clone(),
                    metadata.as_ref(),
                    run_at.as_ref(),
                )
            })
            .collect::<PyResult<Vec<_>>>()?;
        let tx = self.tx.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut guard = tx.lock().await;
                let tx_ref = sync_tx_ref(&mut guard)?;
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
                    .map_err(map_awa_error)?;
                    inserted.push(PyJob::from(row));
                }
                Ok(inserted)
            })
        })
    }

    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let tx = self.tx.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut guard = tx.lock().await;
                let tx = guard
                    .take()
                    .ok_or_else(|| state_error("Transaction already committed or rolled back"))?;
                tx.commit().await.map_err(map_sqlx_error)?;
                Ok(())
            })
        })
    }

    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let tx = self.tx.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut guard = tx.lock().await;
                let tx = guard
                    .take()
                    .ok_or_else(|| state_error("Transaction already committed or rolled back"))?;
                tx.rollback().await.map_err(map_sqlx_error)?;
                Ok(())
            })
        })
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (exc_type, _exc_val, _exc_tb))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        let has_exception = exc_type.is_some();
        let tx = self.tx.clone();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut guard = tx.lock().await;
                if let Some(tx) = guard.take() {
                    if has_exception {
                        let _ = tx.rollback().await;
                    } else {
                        tx.commit().await.map_err(map_sqlx_error)?;
                    }
                }
                Ok(false)
            })
        })
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
