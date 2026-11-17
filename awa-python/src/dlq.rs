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

/// Finish a bulk run for Python: the count of changed rows, or
/// `UniqueConflict` naming the skipped jobs after every other row was
/// changed, or `DatabaseError` if the run gave up on rows another transaction
/// held.
pub(crate) fn finish_bulk_run(run: awa_model::BulkRun, verb: &str) -> PyResult<u64> {
    if run.busy_timed_out {
        return Err(crate::DatabaseError::new_err(format!(
            "{verb} {} row(s), then stopped: every remaining matching row has been held by another transaction for {}s",
            run.count,
            awa_model::bulk::DEFAULT_BUSY_TIMEOUT.as_secs()
        )));
    }
    if run.skipped > 0 {
        let ids: Vec<String> = run.conflicts.iter().map(i64::to_string).collect();
        return Err(crate::UniqueConflict::new_err(format!(
            "{verb} {} row(s); skipped {} job(s) whose unique key a live job holds: {}",
            run.count,
            run.skipped,
            ids.join(", ")
        )));
    }
    Ok(run.count)
}

/// Retry every matching DLQ row, `batch_size` rows per transaction
/// (`bulk::DEFAULT_BATCH_SIZE` when unset).
pub(crate) async fn bulk_retry(
    pool: &sqlx::PgPool,
    filter: &ListDlqFilter,
    allow_all: bool,
    batch_size: Option<i64>,
) -> PyResult<u64> {
    use crate::errors::map_awa_error;
    let options = awa_model::BulkRunOptions::default()
        .batch_size(batch_size.unwrap_or(awa_model::bulk::DEFAULT_BATCH_SIZE));
    let run = awa_model::bulk::run_batches(
        options,
        |size, cursor| {
            awa_model::dlq::bulk_retry_from_dlq_batch(pool, filter, allow_all, size, cursor)
        },
        |_, _| {},
    )
    .await
    .map_err(map_awa_error)?;
    finish_bulk_run(run, "Retried")
}
