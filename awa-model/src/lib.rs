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
pub mod reschedule;
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
    ClaimedEntry, ClaimedRuntimeJob, PruneDurations, PruneOutcome, QueueCounts, QueueStorage,
    QueueStorageConfig, RingLedgerFoldOutcome, RotateOutcome, SkipReason,
    TerminalDeltaRollupOutcome, TerminalRollupFoldOutcome,
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

/// Accept dynamically assembled SQL after manual audit, for sqlx >= 0.9.
///
/// sqlx 0.9 only lets a query be built from `&'static str` unless the call
/// site asserts the text is safe. Awa assembles query text with `format!` in
/// order to name the configured schema and its partition children, so those
/// call sites opt in here.
///
/// # Invariant
///
/// Every use of this function satisfies both of:
///
/// 1. The only interpolated values are SQL identifiers that passed
///    `validate_ident` (`[a-z_][a-z0-9_]*` — see `queue_storage`), integers
///    derived from slot/shard arithmetic, and fixed text fragments chosen by
///    awa itself (for example an allow-listed table name, or a `state`
///    literal typed as `&'static str`).
/// 2. Every value that originates from a job, queue, filter, or API caller
///    travels as a bind parameter.
///
/// Schema names are validated both where they are configured
/// (`QueueStorage::new`) and where they are read back out of the database
/// (`QueueStorage::active_schema`), so no unchecked name reaches a `format!`.
///
/// sqlx cannot see any of that, which is why the assertion is manual. Keep
/// the invariant true: `grep audited_sql` is how the audit is re-run, so a
/// site that does not satisfy it belongs in [`caller_provided_sql`] instead.
///
/// One carve-out: `copy_in_raw` is not covered by sqlx's guard, so the two
/// dynamic `COPY ... FROM STDIN` statements in `queue_storage` hold the same
/// invariant by hand and are commented as such. A `grep audited_sql` audit
/// should sweep `copy_in_raw` alongside it.
///
/// Downstream callers assembling their *own* query text get no audit from
/// awa by using this: the name records that awa vouched for awa's SQL. Reach
/// for `sqlx::AssertSqlSafe` directly there, so the assertion reads as
/// belonging to whoever made it.
///
/// Note that the return type is a sqlx type, so this signature is tied to
/// awa's sqlx major version.
pub fn audited_sql(sql: impl Into<String>) -> sqlx::AssertSqlSafe<String> {
    sqlx::AssertSqlSafe(sql.into())
}

/// Pass through SQL that an external caller supplied verbatim, for sqlx >= 0.9.
///
/// This is the counterpart to [`audited_sql`] for awa's deliberate raw-SQL
/// APIs — the Python `Transaction.execute` / `fetch_*` family, whose whole
/// purpose is to run the caller's own statement inside awa's transaction.
/// The text is not awa's and awa makes no claim about it; the caller owns it
/// exactly as they would owning a `sqlx::query` call directly. Arguments are
/// still bound as parameters.
///
/// Kept separate from [`audited_sql`] on purpose: mixing the two would make
/// the audit invariant documented there unverifiable by inspection.
pub fn caller_provided_sql(sql: impl Into<String>) -> sqlx::AssertSqlSafe<String> {
    sqlx::AssertSqlSafe(sql.into())
}
