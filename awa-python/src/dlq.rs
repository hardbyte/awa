//! Python bindings for the Dead Letter Queue (DLQ) APIs.
//!
//! DLQ rows share the `JobRow` schema plus `reason`, `dlq_at`, and
//! `original_run_lease`. We surface them as `DlqEntry` — a small wrapper
//! that composes a `Job` with the extra metadata columns rather than
//! duplicating every PyJob getter.

use awa_model::dlq::{DlqRow, ListDlqFilter, RetryFromDlqOpts};
use chrono::{DateTime, Utc};
use pyo3::prelude::*;

use crate::job::PyJob;

/// Python representation of a DLQ row: the job snapshot at the time of
/// DLQ entry plus the DLQ-specific metadata (reason, when, original lease).
#[pyclass(frozen, name = "DlqEntry", skip_from_py_object)]
pub struct PyDlqEntry {
    #[pyo3(get)]
    pub job: Py<PyJob>,
    #[pyo3(get)]
    pub reason: String,
    pub dlq_at: DateTime<Utc>,
    #[pyo3(get)]
    pub original_run_lease: i64,
}

impl Clone for PyDlqEntry {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            job: self.job.clone_ref(py),
            reason: self.reason.clone(),
            dlq_at: self.dlq_at,
            original_run_lease: self.original_run_lease,
        })
    }
}

#[pymethods]
impl PyDlqEntry {
    fn __repr__(&self, py: Python<'_>) -> String {
        let job = self.job.bind(py).borrow();
        format!(
            "DlqEntry(id={}, kind='{}', queue='{}', reason='{}', dlq_at={})",
            job.id, job.kind, job.queue, self.reason, self.dlq_at,
        )
    }

    #[getter]
    fn dlq_at(&self) -> DateTime<Utc> {
        self.dlq_at
    }
}

pub(crate) fn dlq_row_to_entry(py: Python<'_>, row: DlqRow) -> PyResult<PyDlqEntry> {
    let (job_row, meta) = row.into_parts();
    let py_job = Py::new(py, PyJob::from(job_row))?;
    Ok(PyDlqEntry {
        job: py_job,
        reason: meta.reason,
        dlq_at: meta.dlq_at,
        original_run_lease: meta.original_run_lease,
    })
}

pub(crate) fn build_filter(
    kind: Option<String>,
    queue: Option<String>,
    tag: Option<String>,
    before_id: Option<i64>,
    before_dlq_at: Option<DateTime<Utc>>,
    limit: Option<i64>,
) -> ListDlqFilter {
    ListDlqFilter {
        kind,
        queue,
        tag,
        before_id,
        before_dlq_at,
        limit,
    }
}

pub(crate) fn build_retry_opts(
    run_at: Option<DateTime<Utc>>,
    priority: Option<i16>,
    queue: Option<String>,
) -> RetryFromDlqOpts {
    RetryFromDlqOpts {
        run_at,
        priority,
        queue,
    }
}

/// Set `count`, `skipped` and `conflicts` on `err` from `run`, so a caller
/// that catches it knows what the run changed before it stopped.
fn with_progress(err: PyErr, run: &awa_model::BulkRun) -> PyErr {
    Python::attach(|py| {
        let value = err.value(py);
        // Setting attributes on an exception instance does not fail.
        let _ = value.setattr("count", run.count);
        let _ = value.setattr("skipped", run.skipped);
        let _ = value.setattr("conflicts", run.conflicts.clone());
    });
    err
}

/// Finish a bulk run for Python: the count of changed rows, or an exception
/// carrying the run's `count`, `skipped` and `conflicts`. The exception is
/// `UniqueConflict` naming the skipped jobs after every other row was
/// changed, `DatabaseError` if the run gave up on rows another transaction
/// held, or the mapped error of a batch that failed.
pub(crate) fn finish_bulk_run(
    result: Result<awa_model::BulkRun, awa_model::BulkError<awa_model::BulkRun>>,
    verb: &str,
) -> PyResult<u64> {
    let run = match result {
        Ok(run) => run,
        Err(err) => {
            return Err(with_progress(
                crate::errors::map_awa_error(err.error),
                &err.partial,
            ))
        }
    };
    if run.busy_timed_out {
        return Err(with_progress(
            crate::DatabaseError::new_err(format!(
                "{verb} {} row(s), then stopped: every remaining matching row stayed held by another transaction",
                run.count,
            )),
            &run,
        ));
    }
    if run.skipped > 0 {
        let ids: Vec<String> = run.conflicts.iter().map(i64::to_string).collect();
        return Err(with_progress(
            crate::UniqueConflict::new_err(format!(
                "{verb} {} row(s); skipped {} job(s) whose unique key a live job holds: {}",
                run.count,
                run.skipped,
                ids.join(", ")
            )),
            &run,
        ));
    }
    Ok(run.count)
}

/// Retry every matching DLQ row, `batch_size` rows per transaction
/// (`bulk::DEFAULT_BATCH_SIZE` when unset). `on_changed` sees the row count
/// of each batch that retried rows, as the batch commits.
pub(crate) async fn bulk_retry(
    pool: &sqlx::PgPool,
    filter: &ListDlqFilter,
    allow_all: bool,
    batch_size: Option<i64>,
    mut on_changed: impl FnMut(u64),
) -> PyResult<u64> {
    let options = awa_model::BulkRunOptions::default()
        .batch_size(batch_size.unwrap_or(awa_model::bulk::DEFAULT_BATCH_SIZE));
    let run = awa_model::bulk::run_batches(
        options,
        |size, cursor| {
            awa_model::dlq::bulk_retry_from_dlq_batch(pool, filter, allow_all, size, cursor)
        },
        |batch, _| {
            if batch.count > 0 {
                on_changed(batch.count);
            }
        },
    )
    .await;
    finish_bulk_run(run, "Retried")
}

fn batch_options(batch_size: Option<i64>) -> awa_model::BulkRunOptions {
    awa_model::BulkRunOptions::default()
        .batch_size(batch_size.unwrap_or(awa_model::bulk::DEFAULT_BATCH_SIZE))
}

/// Discard every failed job of `kind` in batches.
pub(crate) async fn discard_failed(
    pool: &sqlx::PgPool,
    kind: &str,
    batch_size: Option<i64>,
) -> PyResult<u64> {
    let run = awa_model::bulk::run_batches(
        batch_options(batch_size),
        |size, cursor| awa_model::admin::discard_failed_batch(pool, kind, size, cursor),
        |_, _| {},
    )
    .await;
    finish_bulk_run(run, "Discarded")
}

/// Drain `queue` in batches.
pub(crate) async fn drain_queue(
    pool: &sqlx::PgPool,
    queue: &str,
    batch_size: Option<i64>,
) -> PyResult<u64> {
    let run = awa_model::bulk::run_batches(
        batch_options(batch_size),
        |size, cursor| awa_model::admin::drain_queue_batch(pool, queue, size, cursor),
        |_, _| {},
    )
    .await;
    finish_bulk_run(run, "Drained")
}

/// Move failed jobs into the DLQ in batches. `on_changed` sees the row
/// count of each batch that moved rows, as the batch commits.
pub(crate) async fn bulk_move(
    pool: &sqlx::PgPool,
    kind: Option<&str>,
    queue: Option<&str>,
    reason: &str,
    allow_all: bool,
    batch_size: Option<i64>,
    mut on_changed: impl FnMut(u64),
) -> PyResult<u64> {
    let run = awa_model::bulk::run_batches(
        batch_options(batch_size),
        |size, cursor| {
            awa_model::dlq::bulk_move_failed_to_dlq_batch(
                pool, kind, queue, reason, allow_all, size, cursor,
            )
        },
        |batch, _| {
            if batch.count > 0 {
                on_changed(batch.count);
            }
        },
    )
    .await;
    finish_bulk_run(run, "Moved")
}

/// Purge matching DLQ rows in batches. `on_changed` sees the row count of
/// each batch that purged rows, as the batch commits.
pub(crate) async fn purge(
    pool: &sqlx::PgPool,
    filter: &ListDlqFilter,
    allow_all: bool,
    batch_size: Option<i64>,
    mut on_changed: impl FnMut(u64),
) -> PyResult<u64> {
    let run = awa_model::bulk::run_batches(
        batch_options(batch_size),
        |size, cursor| awa_model::dlq::purge_dlq_batch(pool, filter, allow_all, size, cursor),
        |batch, _| {
            if batch.count > 0 {
                on_changed(batch.count);
            }
        },
    )
    .await;
    finish_bulk_run(run, "Purged")
}

/// Retry every failed job of `kind` or in `queue` in batches.
pub(crate) async fn retry_failed(
    pool: &sqlx::PgPool,
    kind: Option<&str>,
    queue: Option<&str>,
    batch_size: i64,
) -> PyResult<u64> {
    let options = batch_options(Some(batch_size));
    let run = match (kind, queue) {
        (Some(kind), None) => {
            awa_model::bulk::run_batches(
                options,
                |size, cursor| {
                    awa_model::admin::retry_failed_by_kind_batch(pool, kind, size, cursor)
                },
                |_, _| {},
            )
            .await
        }
        (None, Some(queue)) => {
            awa_model::bulk::run_batches(
                options,
                |size, cursor| {
                    awa_model::admin::retry_failed_by_queue_batch(pool, queue, size, cursor)
                },
                |_, _| {},
            )
            .await
        }
        _ => unreachable!("validated by the caller"),
    };
    finish_bulk_run(run, "Retried")
}
