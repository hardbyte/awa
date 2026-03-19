use awa_model::{AwaError, JobRow};
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
/// and callback registration for external webhook completion.
pub struct JobContext {
    /// The raw job row from the database.
    pub job: JobRow,
    /// Cancellation flag — set to true when shutdown or deadline is signalled.
    cancelled: Arc<AtomicBool>,
    /// Shared state map for dependency injection.
    state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
    /// Database pool for callback registration.
    pool: PgPool,
}

impl JobContext {
    pub fn new(
        job: JobRow,
        cancelled: Arc<AtomicBool>,
        state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
        pool: PgPool,
    ) -> Self {
        Self {
            job,
            cancelled,
            state,
            pool,
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
        let callback_id =
            awa_model::admin::register_callback(&self.pool, self.job.id, timeout).await?;
        Ok(CallbackToken { id: callback_id })
    }
}
