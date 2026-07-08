pub mod adapter;
pub mod admin;
pub mod batch_operations;
pub mod bridge;
pub mod callback_contract;
pub mod cron;
pub mod dlq;
pub mod error;
pub mod insert;
pub mod job;
pub mod kind;
pub mod migrations;
pub mod partitioned_queue;
pub mod queue_storage;
pub mod storage;
pub mod trace;
pub mod unique;

// Re-exports for ergonomics
pub use adapter::postgres::{prepare_job_insert, prepare_raw_job_insert, PreparedJobInsert};
pub use admin::{
    CallbackConfig, DefaultAction, JobKindDescriptor, JobKindOverview, ListJobsFilter,
    QueueDescriptor, QueueOverview, QueueRuntimeConfigSnapshot, QueueRuntimeMode,
    QueueRuntimeSnapshot, QueueRuntimeSummary, RateLimitSnapshot, ResolveOutcome,
    RetryFailedOutcome, RuntimeInstance, RuntimeOverview, RuntimeSnapshotInput,
    StateTimeseriesBucket, StorageCapability,
};
pub use batch_operations::{
    BatchOperation, BatchOperationFilter, BatchOperationKind, BatchOperationPreview,
    BatchOperationSpec, BatchOperationState, ListBatchOperationsFilter, SubmitBatchOperation,
};

/// Deprecated alias preserved for one release so existing downstream code
/// compiling against `awa_model::QueueStats` keeps building. New callers
/// should use [`QueueOverview`] directly — the renamed type carries
/// additional descriptor fields this alias predates.
#[deprecated(since = "0.5.4", note = "use `QueueOverview` instead")]
pub type QueueStats = QueueOverview;
pub use cron::{CronJobRow, CronMissedFirePolicy, PeriodicJob, PeriodicJobBuilder};
pub use dlq::{DlqMetadata, DlqRow, ListDlqFilter, RetryFromDlqOpts};
pub use error::{map_sqlx_error, AwaError};
pub use insert::{insert, insert_many, insert_many_copy, insert_many_copy_from_pool, insert_with};
pub use job::{InsertOpts, InsertParams, JobRow, JobState, UniqueOpts};
pub use partitioned_queue::{
    partition_for_ordering_key, partition_hash64, PartitionedQueue, PartitionedQueueError,
};
pub use queue_storage::{
    ClaimedEntry, ClaimedRuntimeJob, PruneOutcome, QueueCounts, QueueStorage, QueueStorageConfig,
    RotateOutcome, SkipReason, TerminalDeltaRollupOutcome,
};
pub use storage::StorageStatus;

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
