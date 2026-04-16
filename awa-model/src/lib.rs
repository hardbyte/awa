pub mod admin;
pub mod bridge;
pub mod cron;
pub mod dlq;
pub mod error;
pub mod insert;
pub mod job;
pub mod kind;
pub mod migrations;
pub mod unique;

// Re-exports for ergonomics
pub use admin::{
    CallbackConfig, DefaultAction, JobKindDescriptor, JobKindOverview, ListJobsFilter,
    QueueDescriptor, QueueOverview, QueueRuntimeConfigSnapshot, QueueRuntimeMode,
    QueueRuntimeSnapshot, QueueRuntimeSummary, RateLimitSnapshot, ResolveOutcome, RuntimeInstance,
    RuntimeOverview, RuntimeSnapshotInput, StateTimeseriesBucket,
};

/// Deprecated alias preserved for one release so existing downstream code
/// compiling against `awa_model::QueueStats` keeps building. New callers
/// should use [`QueueOverview`] directly — the renamed type carries
/// additional descriptor fields this alias predates.
#[deprecated(since = "0.5.4", note = "use `QueueOverview` instead")]
pub type QueueStats = QueueOverview;
pub use cron::{CronJobRow, PeriodicJob, PeriodicJobBuilder};
pub use dlq::{DlqRow, ListDlqFilter, RetryFromDlqOpts};
pub use error::AwaError;
pub use insert::{insert, insert_many, insert_many_copy, insert_many_copy_from_pool, insert_with};
pub use job::{InsertOpts, InsertParams, JobRow, JobState, UniqueOpts};

// Re-export the derive macro
pub use awa_macros::JobArgs;

/// Trait for typed job arguments.
///
/// Implement this trait (or use `#[derive(JobArgs)]`) to define a job type.
/// The `kind()` method returns the snake_case kind string that identifies
/// this job type across languages.
pub trait JobArgs: serde::Serialize {
    /// The kind string for this job type (e.g., "send_email").
    fn kind() -> &'static str
    where
        Self: Sized;

    /// Get the kind string for an instance.
    fn kind_str(&self) -> &'static str
    where
        Self: Sized,
    {
        Self::kind()
    }

    /// Serialize to JSON value.
    fn to_args(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}
