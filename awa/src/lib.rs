//! Awa — Postgres-native background job queue for Rust and Python.
//!
//! This is the facade crate that re-exports the main types from awa-model,
//! awa-macros, and awa-worker for ergonomic usage.

// Re-export core model types (includes JobArgs derive macro via awa-model)
pub use awa_model::{
    self as model, admin, insert, insert_many, insert_many_copy, insert_many_copy_from_pool,
    insert_with, migrations, AwaError, CallbackConfig, DefaultAction, InsertOpts, InsertParams,
    JobArgs, JobRow, JobState, ResolveOutcome, UniqueOpts,
};

// Re-export worker runtime
pub use awa_worker::{
    self as worker, context::ProgressState, BuildError, CallbackToken, Client, ClientBuilder,
    HealthCheck, JobContext, JobError, JobResult, PeriodicJob, PeriodicJobBuilder, QueueCapacity,
    QueueConfig, QueueHealth, RateLimit, Worker,
};
