pub mod adapter;
pub mod admin;
// Internal to awa's crates; not part of the 0.6 public API.
#[doc(hidden)]
pub mod advisory;
pub mod batch_operations;
pub mod bridge;
pub mod bulk;
pub mod callback_contract;
pub mod cron;
pub mod dlq;
pub mod error;
pub mod insert;
// Internal to awa's crates; not part of the 0.6 public API.
#[doc(hidden)]
pub mod isolation;
pub mod job;
pub mod kind;
pub mod migrations;
pub mod partitioned_queue;
pub mod queue_storage;
mod read_transaction;
pub mod reschedule;
pub mod runtime_control;
pub mod storage;
mod storage_footprint_sql;
pub mod unique;
mod wakeup;

pub use wakeup::flush_wakeups;

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
pub use bulk::{BulkBatch, BulkCursor, BulkError, BulkIds, BulkRun, BulkRunOptions};
pub use cron::{CronJobRow, CronMissedFirePolicy, PeriodicJob, PeriodicJobBuilder};
pub use dlq::{DlqCount, DlqMetadata, DlqRow, ListDlqFilter, RetryFromDlqOpts};
pub use error::{map_sqlx_error, sqlx_error_type, AwaError};
pub use insert::{
    insert, insert_many, insert_many_copy, insert_many_copy_from_pool,
    insert_many_skip_unique_conflicts, insert_with,
};
pub use job::{InsertOpts, InsertParams, JobRow, JobState, UniqueOpts};
pub use partitioned_queue::{
    partition_for_ordering_key, partition_hash64, PartitionedQueue, PartitionedQueueError,
};
pub use queue_storage::{
    ClaimedEntry, ClaimedRuntimeJob, GatedRotateOutcome, PendingReadyLane, PromoteOutcome,
    PromotionLane, PruneOutcome, QueueCounts, QueueRingPin, QueueServingState, QueueStorage,
    QueueStorageConfig, RescueBatch, RescueBatchOptions, RescueScanCursor, RescueScanProgress,
    RescueSlotOrder, RotateOutcome, RuntimeBatchCompletion, SkipReason, SpentClaimHeadAdvance,
    SpentClaimHeadAdvances, SpentClaimHeadFailure, TerminalDeltaRollupOutcome,
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
