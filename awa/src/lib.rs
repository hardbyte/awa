//! Awa — Postgres-native background job queue for Rust and Python.
//!
//! This is the facade crate that re-exports the main types from awa-model,
//! awa-macros, and awa-worker for ergonomic usage.

// Re-export awa_model so the JobArgs derive macro can resolve its trait path
// through the facade crate (::awa::awa_model::JobArgs). Users only need to
// depend on `awa` — no separate `awa-model` dependency required.
#[doc(hidden)]
pub use awa_model;

// Re-export core model types (includes JobArgs derive macro via awa-model)
pub use awa_model::{
    self as model, adapter, admin, bridge, callback_contract, insert, insert_many,
    insert_many_copy, insert_many_copy_from_pool, insert_with, map_sqlx_error, migrations,
    prepare_job_insert, prepare_raw_job_insert, storage, AwaError, CallbackConfig, DefaultAction,
    DlqMetadata, DlqRow, InsertOpts, InsertParams, JobArgs, JobKindDescriptor, JobRow, JobState,
    ListDlqFilter, PreparedJobInsert, QueueDescriptor, QueueFanout, QueueFanoutError, QueueStorage,
    QueueStorageConfig, ResolveOutcome, RetryFromDlqOpts, StorageCapability, StorageStatus,
    UniqueOpts,
};

// Re-export worker runtime
pub use awa_worker::{
    self as worker, context::ProgressState, BuildError, CallbackGuard, CallbackToken, Client,
    ClientBuilder, CronMissedFirePolicy, EnqueueRequest, HealthCheck, JobContext, JobError,
    JobEvent, JobResult, PeriodicJob, PeriodicJobBuilder, QueueCapacity, QueueConfig, QueueHealth,
    RateLimit, RescueReason, RetentionPolicy, UntypedJobEvent, Worker,
};

#[cfg(feature = "http-worker")]
pub use awa_worker::{HttpWorker, HttpWorkerConfig, HttpWorkerMode};
