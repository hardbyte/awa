//! Stable building blocks for external database adapters.
//!
//! These APIs let integration crates execute Awa-compatible inserts through
//! their own Postgres driver or transaction type while reusing Awa's canonical
//! validation, scheduling, uniqueness, and shard-routing preparation.

/// Postgres adapter helpers.
pub mod postgres {
    use crate::insert::POSTGRES_INSERT_JOB_SQL;

    pub use crate::insert::{prepare_job_insert, prepare_raw_job_insert, PreparedJobInsert};

    /// Canonical SQL for inserting one prepared Awa job through a Postgres
    /// driver that binds state and unique state masks as text.
    ///
    /// Bind parameters in the order exposed by [`PreparedJobInsert`]:
    /// kind, queue, args, state, priority, max_attempts, run_at, metadata,
    /// tags, unique_key, unique_states, ordering_key.
    pub const INSERT_JOB_SQL: &str = POSTGRES_INSERT_JOB_SQL;

    /// SQLSTATE for Postgres unique-violation errors.
    ///
    /// Adapters should map this to [`crate::AwaError::UniqueConflict`] when
    /// their driver exposes database error codes.
    pub const UNIQUE_VIOLATION_SQLSTATE: &str = "23505";
}
