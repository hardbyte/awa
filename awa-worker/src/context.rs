use awa_model::JobRow;
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Context passed to worker handlers during job execution.
///
/// Provides access to the job metadata and shared state (e.g., service dependencies).
pub struct JobContext {
    /// The raw job row from the database.
    pub job: JobRow,
    /// Cancellation flag — set to true when shutdown or deadline is signalled.
    cancelled: Arc<AtomicBool>,
    /// Shared state map for dependency injection.
    state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
}

impl JobContext {
    pub fn new(
        job: JobRow,
        cancelled: Arc<AtomicBool>,
        state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
    ) -> Self {
        Self {
            job,
            cancelled,
            state,
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
}
