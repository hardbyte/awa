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

/// Run a bulk DLQ retry: one transaction for every matching row, or up to
/// `batch_size` rows when it is set. A unique conflict in a batch raises
/// `UniqueConflict` naming the job.
pub(crate) async fn bulk_retry(
    pool: &sqlx::PgPool,
    filter: &ListDlqFilter,
    allow_all: bool,
    batch_size: Option<i64>,
) -> PyResult<u64> {
    use crate::errors::map_awa_error;
    let Some(batch_size) = batch_size else {
        return awa_model::dlq::bulk_retry_from_dlq(pool, filter, allow_all)
            .await
            .map_err(map_awa_error);
    };
    match awa_model::dlq::bulk_retry_from_dlq_batch(pool, filter, allow_all, batch_size)
        .await
        .map_err(map_awa_error)?
    {
        awa_model::DlqRetryBatch::Revived { count } => Ok(count),
        awa_model::DlqRetryBatch::UniqueConflict { job_id } => {
            Err(crate::UniqueConflict::new_err(format!(
                "DLQ job {job_id} conflicts with a live unique claim; its batch was not retried"
            )))
        }
    }
}
