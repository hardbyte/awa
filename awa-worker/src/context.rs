pub use crate::runtime::ProgressState;
use awa_model::{AwaError, CallbackConfig, JobRow};
use sqlx::PgPool;
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Proof that this job registered an external callback in the database.
///
/// The public `id` can be sent to the external system. `#[non_exhaustive]`
/// keeps external callers from constructing this type directly.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CallbackGuard {
    pub id: uuid::Uuid,
}

impl CallbackGuard {
    fn new(id: uuid::Uuid) -> Self {
        Self { id }
    }

    /// Return the callback UUID persisted for this job.
    pub fn id(&self) -> uuid::Uuid {
        self.id
    }

    #[cfg(feature = "__python-bridge")]
    #[doc(hidden)]
    pub fn from_bridge_token(id: uuid::Uuid) -> Self {
        Self::new(id)
    }
}

#[doc(hidden)]
pub type CallbackToken = CallbackGuard;

/// Context passed to worker handlers during job execution.
///
/// Provides access to the job metadata, shared state (e.g., service dependencies),
/// callback registration, and structured progress reporting.
pub struct JobContext {
    /// The raw job row from the database.
    pub job: JobRow,
    /// Cancellation flag — set to true when shutdown or deadline is signalled.
    cancelled: Arc<AtomicBool>,
    /// Shared state map for dependency injection.
    state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
    /// Database pool for callback registration and progress flush.
    pool: PgPool,
    /// Shared progress buffer — written by handler, read by heartbeat service.
    progress: Arc<std::sync::Mutex<ProgressState>>,
}

impl JobContext {
    pub fn new(
        job: JobRow,
        cancelled: Arc<AtomicBool>,
        state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
        pool: PgPool,
        progress: Arc<std::sync::Mutex<ProgressState>>,
    ) -> Self {
        Self {
            job,
            cancelled,
            state,
            pool,
            progress,
        }
    }

    /// Check if this job's execution has been cancelled (shutdown or deadline).
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Clone the shared cancellation flag for language bridges.
    pub fn cancellation_flag(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }

    /// Signal cancellation for this job.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Extract a shared state value by type.
    ///
    /// State values are registered via `Client::builder().state(value)`.
    /// Register the concrete type you want to extract:
    ///
    /// ```ignore
    /// // Register with the type you'll extract:
    /// let deps = Arc::new(MyDeps::new());
    /// Client::builder(pool)
    ///     .state(deps.clone())  // stores Arc<MyDeps>
    ///     .build()?;
    ///
    /// // In handler — extract the same type:
    /// let deps = ctx.extract::<Arc<MyDeps>>().unwrap();
    /// ```
    pub fn extract<T: Any + Send + Sync + Clone>(&self) -> Option<T> {
        self.state
            .get(&std::any::TypeId::of::<T>())
            .and_then(|v| v.downcast_ref::<T>())
            .cloned()
    }

    /// Get a reference to the database pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Register a callback for this job, writing the callback_id to the database
    /// immediately.
    ///
    /// Call this BEFORE sending the callback_id to the external system to avoid
    /// the race condition where the external system fires before the DB knows
    /// about the callback.
    ///
    /// Returns a `CallbackGuard` whose `id` should be included in the URL or
    /// payload sent to the external system.
    pub async fn register_callback(&self, timeout: Duration) -> Result<CallbackGuard, AwaError> {
        let callback_id = awa_model::admin::register_callback(
            &self.pool,
            self.job.id,
            self.job.run_lease,
            timeout,
        )
        .await?;
        Ok(CallbackGuard::new(callback_id))
    }

    /// Register a callback with CEL expressions for automatic resolution.
    ///
    /// See [`CallbackConfig`] for expression semantics.
    pub async fn register_callback_with_config(
        &self,
        timeout: Duration,
        config: &CallbackConfig,
    ) -> Result<CallbackGuard, AwaError> {
        let callback_id = awa_model::admin::register_callback_with_config(
            &self.pool,
            self.job.id,
            self.job.run_lease,
            timeout,
            config,
        )
        .await?;
        Ok(CallbackGuard::new(callback_id))
    }

    /// Wait for an external callback to resolve, then resume with the payload.
    ///
    /// This enables sequential callbacks: the handler can register a callback,
    /// wait for it, process the result, register another callback, wait again,
    /// and so on — all within a single handler invocation.
    ///
    /// The handler's async task suspends while waiting. The job transitions to
    /// `waiting_external`. When `resume_external(callback_id, payload)` is
    /// called, the job transitions back to `running` and this method returns
    /// the payload.
    ///
    /// The handler holds its permit (worker slot) during the wait. For very
    /// long waits, consider whether the single-shot `WaitForCallback` pattern
    /// (which releases the permit) is more appropriate.
    ///
    /// ```ignore
    /// let token = ctx.register_callback(Duration::from_secs(3600)).await?;
    /// send_to_external_system(token.id());
    /// let payload = ctx.wait_for_callback(token).await?;
    /// // Handler resumes here with the external system's response
    /// ```
    pub async fn wait_for_callback(
        &self,
        guard: CallbackGuard,
    ) -> Result<serde_json::Value, AwaError> {
        let callback_id = guard.id();

        // Transition to waiting_external
        let result = sqlx::query(
            r#"
            UPDATE awa.jobs
            SET state = 'waiting_external',
                heartbeat_at = NULL,
                deadline_at = NULL
            WHERE id = $1 AND state = 'running' AND run_lease = $2 AND callback_id = $3
            "#,
        )
        .bind(self.job.id)
        .bind(self.job.run_lease)
        .bind(callback_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            let current: Option<(awa_model::JobState, Option<uuid::Uuid>, serde_json::Value)> =
                sqlx::query_as("SELECT state, callback_id, metadata FROM awa.jobs WHERE id = $1")
                    .bind(self.job.id)
                    .fetch_optional(&self.pool)
                    .await?;

            match current {
                Some((awa_model::JobState::Running, None, metadata))
                    if metadata.get("_awa_callback_result").is_some() =>
                {
                    return take_callback_payload(&self.pool, self.job.id, metadata).await;
                }
                Some((state, Some(current_callback_id), _))
                    if current_callback_id != callback_id =>
                {
                    return Err(AwaError::Validation(format!(
                        "wait_for_callback: token {callback_id} is stale; current callback is {current_callback_id} in state {state:?}"
                    )));
                }
                Some((awa_model::JobState::WaitingExternal, Some(_), _)) => {
                    // Already transitioned to waiting_external for this callback.
                }
                Some((state, _, _)) => {
                    return Err(AwaError::Validation(format!(
                        "wait_for_callback: job is not waiting on callback {callback_id}; state={state:?}"
                    )));
                }
                None => {
                    return Err(AwaError::Validation(
                        "job not found during callback wait".into(),
                    ));
                }
            }
        }

        // Poll DB until the callback is resolved (state changes from waiting_external).
        // resume_external sets state to 'running' and stores payload in metadata.
        loop {
            // Check for cancellation (shutdown, deadline)
            if self.is_cancelled() {
                return Err(AwaError::Validation(
                    "job cancelled while waiting for callback".into(),
                ));
            }

            let row: Option<(awa_model::JobState, Option<uuid::Uuid>, serde_json::Value)> =
                sqlx::query_as("SELECT state, callback_id, metadata FROM awa.jobs WHERE id = $1")
                    .bind(self.job.id)
                    .fetch_optional(&self.pool)
                    .await?;

            match row {
                Some((awa_model::JobState::Running, None, metadata))
                    if metadata.get("_awa_callback_result").is_some() =>
                {
                    return take_callback_payload(&self.pool, self.job.id, metadata).await;
                }
                Some((awa_model::JobState::WaitingExternal, Some(current_callback_id), _))
                    if current_callback_id == callback_id =>
                {
                    // Still waiting — sleep and poll again
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Some((state, Some(current_callback_id), _))
                    if current_callback_id != callback_id =>
                {
                    return Err(AwaError::Validation(format!(
                        "wait_for_callback: token {callback_id} is stale; current callback is {current_callback_id} in state {state:?}"
                    )));
                }
                Some((state, _, _)) => {
                    // Job was rescued, cancelled, or otherwise moved out of waiting
                    return Err(AwaError::Validation(format!(
                        "job left wait_for_callback unexpectedly for token {callback_id}: state={state:?}"
                    )));
                }
                None => {
                    return Err(AwaError::Validation(
                        "job not found during callback wait".into(),
                    ));
                }
            }
        }
    }

    /// Set structured progress (0-100 with message). Sync — writes to in-memory buffer.
    ///
    /// `percent` is clamped to 0-100. For progress without a message, pass `""`.
    pub fn set_progress(&self, percent: u8, message: &str) {
        let mut guard = self.progress.lock().expect("progress lock poisoned");
        guard.set_progress(percent, Some(message));
    }

    /// Shallow-merge keys into progress.metadata for checkpointing. Sync.
    ///
    /// `updates` must be a JSON object. Top-level keys overwrite; nested objects
    /// are replaced, not deep-merged.
    pub fn update_metadata(&self, updates: serde_json::Value) -> Result<(), AwaError> {
        let obj = updates
            .as_object()
            .ok_or_else(|| AwaError::Validation("update_metadata requires a JSON object".into()))?;

        let mut guard = self.progress.lock().expect("progress lock poisoned");
        if !guard.merge_metadata(obj) {
            return Err(AwaError::Validation(
                "progress.metadata is not a JSON object; cannot merge".into(),
            ));
        }
        Ok(())
    }

    /// Force immediate flush of pending progress to DB. For critical checkpoints.
    ///
    /// Does not return success until the progress has been durably written
    /// or the job is no longer in running state (rescued/cancelled).
    pub async fn flush_progress(&self) -> Result<(), AwaError> {
        let (snapshot, target_generation) = {
            let guard = self.progress.lock().expect("progress lock poisoned");
            match guard.pending_snapshot() {
                Some(pair) => pair,
                None => return Ok(()),
            }
        };

        let result = sqlx::query(
            r#"
            UPDATE awa.jobs_hot
            SET progress = $2
            WHERE id = $1 AND state = 'running' AND run_lease = $3
            "#,
        )
        .bind(self.job.id)
        .bind(&snapshot)
        .bind(self.job.run_lease)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            // Job was rescued/cancelled — not an error for the caller
            return Ok(());
        }

        let mut guard = self.progress.lock().expect("progress lock poisoned");
        guard.ack(target_generation);

        Ok(())
    }

    /// Get a clone of the shared progress state Arc (for Python bridge).
    pub fn progress_buffer(&self) -> Arc<std::sync::Mutex<ProgressState>> {
        self.progress.clone()
    }
}

async fn take_callback_payload(
    pool: &PgPool,
    job_id: i64,
    metadata: serde_json::Value,
) -> Result<serde_json::Value, AwaError> {
    let payload = metadata
        .get("_awa_callback_result")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    sqlx::query("UPDATE awa.jobs SET metadata = metadata - '_awa_callback_result' WHERE id = $1")
        .bind(job_id)
        .execute(pool)
        .await?;

    Ok(payload)
}
