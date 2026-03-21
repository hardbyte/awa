pub use crate::runtime::ProgressState;
use awa_model::{AwaError, CallbackConfig, JobRow};
use sqlx::PgPool;
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Token representing a registered callback for external webhook completion.
///
/// The `id` is a UUID that has been persisted to the database. Pass this ID
/// to the external system so it can call back to complete/fail/retry the job.
pub struct CallbackToken {
    pub id: uuid::Uuid,
}

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
    /// Returns a `CallbackToken` whose `id` should be included in the URL or
    /// payload sent to the external system.
    pub async fn register_callback(&self, timeout: Duration) -> Result<CallbackToken, AwaError> {
        let callback_id = awa_model::admin::register_callback(
            &self.pool,
            self.job.id,
            self.job.run_lease,
            timeout,
        )
        .await?;
        Ok(CallbackToken { id: callback_id })
    }

    /// Register a callback with CEL expressions for automatic resolution.
    ///
    /// See [`CallbackConfig`] for expression semantics.
    pub async fn register_callback_with_config(
        &self,
        timeout: Duration,
        config: &CallbackConfig,
    ) -> Result<CallbackToken, AwaError> {
        let callback_id = awa_model::admin::register_callback_with_config(
            &self.pool,
            self.job.id,
            self.job.run_lease,
            timeout,
            config,
        )
        .await?;
        Ok(CallbackToken { id: callback_id })
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
