use awa_model::{InsertOpts, JobRow, JobState, QueueStorage};
use awa_worker::context::ProgressState;
use awa_worker::{FollowUps, OutputSlot};
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

#[pymethods]
impl PyJobState {
    fn __str__(&self) -> &'static str {
        match self {
            PyJobState::Scheduled => "scheduled",
            PyJobState::Available => "available",
            PyJobState::Running => "running",
            PyJobState::Completed => "completed",
            PyJobState::Retryable => "retryable",
            PyJobState::Failed => "failed",
            PyJobState::Cancelled => "cancelled",
            PyJobState::WaitingExternal => "waiting_external",
        }
    }

    fn __repr__(&self) -> String {
        format!("JobState.{:?}", self)
    }
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
    /// Active queue-storage backend (only set during queue-storage dispatch).
    queue_storage: Option<Arc<QueueStorage>>,
    /// Shared progress buffer for in-flight reporting (only set during dispatch).
    progress_buffer: Option<Arc<std::sync::Mutex<ProgressState>>>,
    /// The attempt's follow-up staging buffer (ADR-046; only set during dispatch).
    follow_ups: Option<FollowUps>,
    /// The attempt's output slot (ADR-048; only set during dispatch).
    output_slot: Option<OutputSlot>,
    /// The completed attempt's output from the job row (for queried jobs).
    output_json: Option<serde_json::Value>,
    /// Progress JSON from the job row (for queried jobs).
    progress_json: Option<serde_json::Value>,
    /// Error history from the job row (array of {error, attempt, at} objects).
    errors_json: Option<Vec<serde_json::Value>>,
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
            queue_storage: self.queue_storage.clone(),
            progress_buffer: self.progress_buffer.clone(),
            follow_ups: self.follow_ups.clone(),
            output_slot: self.output_slot.clone(),
            output_json: self.output_json.clone(),
            progress_json: self.progress_json.clone(),
            errors_json: self.errors_json.clone(),
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

    /// W3C traceparent captured at the enqueue site, if any. Pass it to an
    /// OpenTelemetry propagator to continue the producer's trace from a
    /// Python handler.
    #[getter]
    fn traceparent(&self) -> Option<String> {
        awa_model::trace::traceparent_from_metadata(&self.metadata_json).map(str::to_owned)
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

    /// Stage a follow-up job that is enqueued in the same transaction as this
    /// attempt's completion, and discarded if the attempt ends any other way
    /// (ADR-046). Takes the same keyword arguments as `Transaction.insert`.
    ///
    /// The args are converted to JSON now. A request that cannot be staged
    /// raises `StagingError` and poisons the attempt: if the handler still
    /// returns normally, the attempt fails terminally.
    // Options are taken untyped and extracted below, so a value of the wrong
    // type or range poisons the attempt instead of raising before staging.
    #[pyo3(signature = (args, *, kind=None, queue=None, priority=None, max_attempts=None, tags=None, metadata=None, run_at=None, unique_opts=None, ordering_key=None))]
    #[allow(clippy::too_many_arguments)]
    fn enqueue_on_completion(
        &self,
        py: Python<'_>,
        args: Py<PyAny>,
        kind: Option<Py<PyAny>>,
        queue: Option<Py<PyAny>>,
        priority: Option<Py<PyAny>>,
        max_attempts: Option<Py<PyAny>>,
        tags: Option<Py<PyAny>>,
        metadata: Option<Py<PyAny>>,
        run_at: Option<Py<PyAny>>,
        unique_opts: Option<Py<PyAny>>,
        ordering_key: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let Some(follow_ups) = &self.follow_ups else {
            return Err(crate::StagingError::new_err(
                "follow-ups can only be staged on the job passed to a running handler",
            ));
        };
        // Convert everything first; a conversion failure poisons the attempt
        // like any other staging failure, so a handler that swallows it
        // cannot complete without the follow-up it asked for.
        let prepared = (|| -> PyResult<(String, serde_json::Value, InsertOpts)> {
            let args_bound = args.bind(py);
            let kind = match kind {
                Some(kind) => kind.extract::<String>(py)?,
                None => crate::args::derive_kind(&crate::args::get_type_class_name(
                    args_bound.get_type().as_any(),
                )?),
            };
            let opts = InsertOpts {
                queue: queue
                    .map(|value| value.extract::<String>(py))
                    .transpose()?
                    .unwrap_or_else(|| "default".to_string()),
                priority: priority
                    .map(|value| value.extract::<i16>(py))
                    .transpose()?
                    .unwrap_or(2),
                max_attempts: max_attempts
                    .map(|value| value.extract::<i16>(py))
                    .transpose()?
                    .unwrap_or(25),
                tags: tags
                    .map(|value| value.extract::<Vec<String>>(py))
                    .transpose()?
                    .unwrap_or_default(),
                run_at: run_at
                    .as_ref()
                    .map(|value| crate::transaction::parse_run_at(py, value.bind(py)))
                    .transpose()?,
                metadata: metadata
                    .as_ref()
                    .map(|value| py_to_json(py, value.bind(py)))
                    .transpose()?
                    .unwrap_or(serde_json::json!({})),
                unique: unique_opts
                    .as_ref()
                    .map(|value| crate::transaction::parse_unique_opts(py, value.bind(py)))
                    .transpose()?,
                ordering_key: ordering_key
                    .as_ref()
                    .map(|value| crate::transaction::parse_ordering_key(py, value.bind(py)))
                    .transpose()?,
                ..InsertOpts::default()
            };
            Ok((kind, crate::args::serialize_args(py, args_bound)?, opts))
        })();
        match prepared {
            Ok((kind, args_json, opts)) => follow_ups
                .enqueue_raw_on_completion(kind, args_json, opts)
                .map_err(|err| crate::StagingError::new_err(err.to_string())),
            Err(cause) => {
                let err = follow_ups.poison(awa_worker::StageError::Invalid(cause.to_string()));
                let staging = crate::StagingError::new_err(err.to_string());
                staging.set_cause(py, Some(cause));
                Err(staging)
            }
        }
    }

    /// Record this attempt's output (ADR-048), replacing any earlier one.
    ///
    /// The value is converted to JSON now: pydantic models are dumped in JSON
    /// mode, dataclasses and tuples are converted, and sets, non-finite
    /// floats, non-string keys and integers outside 64 bits are rejected.
    /// At most `MAX_OUTPUT_BYTES` of JSON text. `None` clears the output.
    /// A value that cannot be recorded raises `OutputError` and clears the
    /// output; it does not poison the attempt.
    fn set_output(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let Some(slot) = &self.output_slot else {
            return Err(crate::OutputError::new_err(
                "an output can only be recorded on the job passed to a running handler",
            ));
        };
        match output_to_json(py, value.bind(py), 0) {
            Ok(json) => slot
                .set_json(json)
                .map_err(|err| crate::OutputError::new_err(err.to_string())),
            Err(cause) => {
                let err = slot.fail(awa_worker::OutputError::Invalid(cause.to_string()));
                let output_error = crate::OutputError::new_err(err.to_string());
                output_error.set_cause(py, Some(cause));
                Err(output_error)
            }
        }
    }

    /// Remove the recorded output, so the attempt publishes none.
    fn clear_output(&self) {
        if let Some(slot) = &self.output_slot {
            slot.clear();
        }
    }

    /// During dispatch, the output recorded so far; on a job read from the
    /// database, the completed attempt's output (ADR-048), or None.
    #[getter]
    fn output(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value = match &self.output_slot {
            Some(slot) => slot.get(),
            None => self.output_json.clone(),
        };
        match value {
            Some(value) => json_to_py(py, &value),
            None => Ok(py.None()),
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled
            .as_ref()
            .map(|flag| flag.load(Ordering::SeqCst))
            .unwrap_or(false)
    }

    /// Error history — list of dicts with 'error', 'attempt', and 'at' keys.
    /// Empty list for jobs that haven't errored.
    #[getter]
    fn errors(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.errors_json {
            Some(errors) => {
                let list = pyo3::types::PyList::empty(py);
                for err in errors {
                    list.append(json_to_py(py, err)?)?;
                }
                Ok(list.into_any().unbind())
            }
            None => {
                let list = pyo3::types::PyList::empty(py);
                Ok(list.into_any().unbind())
            }
        }
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
        let queue_storage = self.queue_storage.clone();

        crate::async_bridge::future_into_py(py, async move {
            let (snapshot, target_generation) = {
                let guard = buffer.lock().expect("progress lock poisoned");
                match guard.pending_snapshot() {
                    Some(pair) => pair,
                    None => return Ok(()),
                }
            };

            if let Some(store) = queue_storage {
                store
                    .flush_progress(&pool, job_id, run_lease, snapshot.clone())
                    .await
                    .map_err(map_awa_error)?;
            } else if let Some(schema) = QueueStorage::active_schema(&pool)
                .await
                .map_err(map_awa_error)?
            {
                QueueStorage::from_existing_schema(schema)
                    .map_err(map_awa_error)?
                    .flush_progress(&pool, job_id, run_lease, snapshot.clone())
                    .await
                    .map_err(map_awa_error)?;
            } else {
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
            }

            let mut guard = buffer.lock().expect("progress lock poisoned");
            guard.ack(target_generation);

            Ok(())
        })
    }

    /// Synchronous variant of flush_progress for sync handlers (Django/Flask).
    fn flush_progress_sync(&self, py: Python<'_>) -> PyResult<()> {
        use crate::errors::map_awa_error;
        let pool = self.pool.clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "flush_progress_sync is only available during job execution",
            )
        })?;
        let buffer = self.progress_buffer.clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "flush_progress_sync is only available during job execution",
            )
        })?;
        let job_id = self.id;
        let run_lease = self.run_lease;
        let queue_storage = self.queue_storage.clone();

        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let (snapshot, target_generation) = {
                    let guard = buffer.lock().expect("progress lock poisoned");
                    match guard.pending_snapshot() {
                        Some(pair) => pair,
                        None => return Ok(()),
                    }
                };

                if let Some(store) = queue_storage {
                    store
                        .flush_progress(&pool, job_id, run_lease, snapshot.clone())
                        .await
                        .map_err(map_awa_error)?;
                } else if let Some(schema) = QueueStorage::active_schema(&pool)
                    .await
                    .map_err(map_awa_error)?
                {
                    QueueStorage::from_existing_schema(schema)
                        .map_err(map_awa_error)?
                        .flush_progress(&pool, job_id, run_lease, snapshot.clone())
                        .await
                        .map_err(map_awa_error)?;
                } else {
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
                        return Ok(());
                    }
                }

                let mut guard = buffer.lock().expect("progress lock poisoned");
                guard.ack(target_generation);

                Ok(())
            })
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

        crate::async_bridge::future_into_py(py, async move {
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

    /// Wait for an external callback to resolve, then resume with the payload.
    ///
    /// This enables sequential callbacks: register a callback, wait for it,
    /// process the result, register another, wait again — all within a single
    /// handler invocation.
    ///
    /// The handler's async task suspends while waiting. When
    /// ``resume_external(callback_id, payload)`` is called, this method
    /// returns the payload.
    fn wait_for_callback<'py>(
        &self,
        py: Python<'py>,
        token: PyRef<'_, PyCallbackToken>,
    ) -> PyResult<Bound<'py, PyAny>> {
        use crate::errors::map_awa_error;
        use awa_model::admin::{check_callback_state, enter_callback_wait, CallbackPollResult};

        let pool = self.pool.clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "wait_for_callback is only available during job execution",
            )
        })?;
        let cancelled = self.cancelled.clone();
        let job_id = self.id;
        let run_lease = self.run_lease;
        let token_id = token.id.clone();
        let token_uuid = uuid::Uuid::parse_str(&token_id).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid callback token UUID: {e}"))
        })?;

        crate::async_bridge::future_into_py(py, async move {
            let entered = enter_callback_wait(&pool, job_id, run_lease, token_uuid)
                .await
                .map_err(map_awa_error)?;

            if !entered {
                match check_callback_state(&pool, job_id, token_uuid)
                    .await
                    .map_err(map_awa_error)?
                {
                    CallbackPollResult::Resolved(payload) => {
                        return Python::attach(|py| json_to_py(py, &payload));
                    }
                    CallbackPollResult::Pending => { /* Already in waiting_external */ }
                    CallbackPollResult::Stale {
                        token,
                        current,
                        state,
                    } => {
                        return Err(map_awa_error(awa_model::AwaError::Validation(format!(
                            "wait_for_callback: token {token} is stale; current callback is {current} in state {state:?}"
                        ))));
                    }
                    CallbackPollResult::UnexpectedState { token, state } => {
                        return Err(map_awa_error(awa_model::AwaError::Validation(format!(
                            "wait_for_callback: job is not waiting on callback {token}; state={state:?}"
                        ))));
                    }
                    CallbackPollResult::NotFound => {
                        return Err(map_awa_error(awa_model::AwaError::Validation(
                            "job not found during callback wait".into(),
                        )));
                    }
                }
            }

            // Poll DB until the callback is resolved
            loop {
                if let Some(ref flag) = cancelled {
                    if flag.load(std::sync::atomic::Ordering::SeqCst) {
                        return Err(map_awa_error(awa_model::AwaError::Validation(
                            "job cancelled while waiting for callback".into(),
                        )));
                    }
                }

                match check_callback_state(&pool, job_id, token_uuid)
                    .await
                    .map_err(map_awa_error)?
                {
                    CallbackPollResult::Resolved(payload) => {
                        return Python::attach(|py| json_to_py(py, &payload));
                    }
                    CallbackPollResult::Pending => {
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                    CallbackPollResult::Stale {
                        token,
                        current,
                        state,
                    } => {
                        return Err(map_awa_error(awa_model::AwaError::Validation(format!(
                            "wait_for_callback: token {token} is stale; current callback is {current} in state {state:?}"
                        ))));
                    }
                    CallbackPollResult::UnexpectedState { token, state } => {
                        return Err(map_awa_error(awa_model::AwaError::Validation(format!(
                            "job left wait_for_callback unexpectedly for token {token}: state={state:?}"
                        ))));
                    }
                    CallbackPollResult::NotFound => {
                        return Err(map_awa_error(awa_model::AwaError::Validation(
                            "job not found during callback wait".into(),
                        )));
                    }
                }
            }
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
        let errors_json = row.errors.clone();
        let output_json = row.output.clone();
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
            queue_storage: None,
            progress_buffer: None,
            follow_ups: None,
            output_slot: None,
            output_json,
            progress_json,
            errors_json,
            run_at: row.run_at,
            deadline_at: row.deadline_at,
            created_at: row.created_at,
            finalized_at: row.finalized_at,
        }
    }
}

impl PyJob {
    #[allow(clippy::too_many_arguments)]
    pub fn for_dispatch(
        row: JobRow,
        args_override: Py<PyAny>,
        cancelled: Arc<AtomicBool>,
        pool: PgPool,
        queue_storage: Option<Arc<QueueStorage>>,
        progress_buffer: Arc<std::sync::Mutex<ProgressState>>,
        follow_ups: FollowUps,
        output_slot: OutputSlot,
    ) -> Self {
        let mut job = Self::from(row);
        job.args_override = Some(args_override);
        job.cancelled = Some(cancelled);
        job.pool = Some(pool);
        job.queue_storage = queue_storage;
        job.progress_buffer = Some(progress_buffer);
        job.follow_ups = Some(follow_ups);
        job.output_slot = Some(output_slot);
        job
    }
}

/// Convert a Python value to an output (ADR-048): pydantic models in JSON
/// mode, dataclasses at any depth, tuples as lists. Sets, non-finite floats,
/// non-string keys, integers outside 64 bits and other objects are rejected
/// rather than coerced.
fn output_to_json(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    depth: usize,
) -> PyResult<serde_json::Value> {
    use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
    let type_error = |message: String| pyo3::exceptions::PyTypeError::new_err(message);
    if depth > MAX_JSON_DEPTH {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "output nesting too deep (max 64 levels)",
        ));
    }
    if obj.is_none() {
        return Ok(serde_json::Value::Null);
    }
    if let Ok(b) = obj.cast::<PyBool>() {
        return Ok(serde_json::Value::Bool(b.is_true()));
    }
    if obj.cast::<PyInt>().is_ok() {
        if let Ok(i) = obj.extract::<i64>() {
            return Ok(serde_json::json!(i));
        }
        if let Ok(u) = obj.extract::<u64>() {
            return Ok(serde_json::json!(u));
        }
        return Err(pyo3::exceptions::PyValueError::new_err(
            "output integers must fit in 64 bits",
        ));
    }
    if let Ok(f) = obj.cast::<PyFloat>() {
        let value = f.value();
        return serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("output floats must be finite")
            });
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return Ok(serde_json::Value::String(s.to_str()?.to_owned()));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        return list
            .iter()
            .map(|item| output_to_json(py, &item, depth + 1))
            .collect::<PyResult<Vec<_>>>()
            .map(serde_json::Value::Array);
    }
    if let Ok(tuple) = obj.cast::<PyTuple>() {
        return tuple
            .iter()
            .map(|item| output_to_json(py, &item, depth + 1))
            .collect::<PyResult<Vec<_>>>()
            .map(serde_json::Value::Array);
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (key, value) in dict.iter() {
            let Ok(key) = key.cast::<PyString>() else {
                return Err(type_error(format!(
                    "output keys must be str, not {}",
                    key.get_type().qualname()?
                )));
            };
            map.insert(
                key.to_str()?.to_owned(),
                output_to_json(py, &value, depth + 1)?,
            );
        }
        return Ok(serde_json::Value::Object(map));
    }
    // A pydantic model (v2): dump in JSON mode so dates and the like become
    // strings, as pydantic would send them.
    if !obj.is_instance_of::<pyo3::types::PyType>() {
        if let Ok(model_dump) = obj.getattr("model_dump") {
            if model_dump.is_callable() {
                let kwargs = PyDict::new(py);
                kwargs.set_item("mode", "json")?;
                let dumped = model_dump.call((), Some(&kwargs))?;
                return output_to_json(py, &dumped, depth + 1);
            }
        }
        let dataclasses = py.import("dataclasses")?;
        if dataclasses
            .call_method1("is_dataclass", (obj,))?
            .is_truthy()?
        {
            let as_dict = dataclasses.call_method1("asdict", (obj,))?;
            return output_to_json(py, &as_dict, depth + 1);
        }
    }
    Err(type_error(format!(
        "{} is not JSON-serializable as an output",
        obj.get_type().qualname()?
    )))
}

/// Validate and normalize a value as `Job.set_output` would, returning the
/// JSON it would store. Used by the `awa.testing.HandlerJob` stand-in so its
/// rules cannot drift from the runtime's.
#[pyfunction]
#[pyo3(name = "_validate_output")]
pub fn validate_output(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
    match output_json(py, value.bind(py))? {
        Some(json) => json_to_py(py, &json),
        None => Ok(py.None()),
    }
}

/// Convert and validate a value as an output, raising `OutputError`. `None`
/// (or JSON null) means no output.
pub(crate) fn output_json(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<serde_json::Value>> {
    let json = output_to_json(py, value, 0).map_err(|cause| {
        let err = crate::OutputError::new_err(format!("output is not storable JSON: {cause}"));
        err.set_cause(py, Some(cause));
        err
    })?;
    awa_model::output::validate_output(json)
        .map_err(|err| crate::OutputError::new_err(err.to_string()))
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
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_pyobject(py)
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
