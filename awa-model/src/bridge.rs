//! Transactional enqueue for non-sqlx Postgres stacks.
//!
//! The core [`crate::insert::insert`] and [`crate::insert::insert_with`]
//! functions require sqlx's `PgExecutor` trait. This module provides
//! feature-gated adapters so users of other Postgres libraries can insert
//! jobs within their existing transactions.
//!
//! All adapters share the same preparation logic in
//! [`crate::insert::prepare_row`] — validation, state determination, and
//! unique key computation happen exactly once, avoiding semantic drift
//! between drivers.
//!
//! # Available adapters
//!
//! | Feature | Module | Works with |
//! |---------|--------|------------|
//! | `tokio-postgres` | [`tokio_pg`] | `tokio_postgres::Client` and `Transaction` |

// ---------------------------------------------------------------------------
// tokio-postgres adapter
// ---------------------------------------------------------------------------

/// Adapter for inserting Awa jobs via `tokio_postgres` connections and
/// transactions.
///
/// All functions are bounded on [`tokio_postgres::GenericClient`], which is
/// implemented for `tokio_postgres::Client` and `tokio_postgres::Transaction`.
///
/// Pool wrappers like `deadpool_postgres::Client` typically deref to the
/// underlying `tokio_postgres::Client`. Call `.transaction()` on the wrapper
/// first, then pass the resulting `tokio_postgres::Transaction` to these
/// functions.
#[cfg(feature = "tokio-postgres")]
pub mod tokio_pg {
    use crate::error::AwaError;
    use crate::insert::{prepare_row, prepare_row_raw, PreparedRow};
    use crate::job::{InsertOpts, JobRow, JobState};
    use crate::JobArgs;
    use ::tokio_postgres::GenericClient;

    /// INSERT with `RETURNING *` plus `state::text` for enum parsing.
    ///
    /// Double-casts (`$4::text::awa.job_state`, `$11::text::bit(8)`) ensure
    /// tokio-postgres accepts plain TEXT parameters for the custom enum and
    /// bit columns. `state::text AS state_str` gives us a readable string
    /// alongside the full row.
    const INSERT_SQL: &str = r#"
    INSERT INTO awa.jobs
        (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags, unique_key, unique_states)
    VALUES
        ($1, $2, $3, $4::text::awa.job_state, $5::smallint, $6::smallint,
         COALESCE($7, now()), $8, $9::text[], $10, $11::text::bit(8))
    RETURNING *, state::text AS state_str
    "#;

    /// Insert a job with default options.
    ///
    /// Mirrors [`crate::insert::insert`] but works with any
    /// [`tokio_postgres::GenericClient`].
    pub async fn insert_job<C: GenericClient>(
        client: &C,
        args: &impl JobArgs,
    ) -> Result<JobRow, AwaError> {
        insert_job_with(client, args, InsertOpts::default()).await
    }

    /// Insert a job with custom options.
    ///
    /// Mirrors [`crate::insert::insert_with`] but works with any
    /// [`tokio_postgres::GenericClient`].
    pub async fn insert_job_with<C: GenericClient>(
        client: &C,
        args: &impl JobArgs,
        opts: InsertOpts,
    ) -> Result<JobRow, AwaError> {
        let prepared = prepare_row(args, opts)?;
        execute(client, &prepared).await
    }

    /// Insert a job from raw kind + JSON args + options.
    ///
    /// Use when you don't have a `JobArgs` impl — e.g. forwarding from
    /// a dynamic source or another language.
    pub async fn insert_job_raw<C: GenericClient>(
        client: &C,
        kind: String,
        args: serde_json::Value,
        opts: InsertOpts,
    ) -> Result<JobRow, AwaError> {
        let prepared = prepare_row_raw(kind, args, opts)?;
        execute(client, &prepared).await
    }

    async fn execute<C: GenericClient>(
        client: &C,
        prepared: &PreparedRow,
    ) -> Result<JobRow, AwaError> {
        let state_str = prepared.state.to_string();

        let row = client
            .query_one(
                INSERT_SQL,
                &[
                    &prepared.kind,
                    &prepared.queue,
                    &prepared.args,
                    &state_str,
                    &prepared.priority,
                    &prepared.max_attempts,
                    &prepared.run_at,
                    &prepared.metadata,
                    &prepared.tags,
                    &prepared.unique_key,
                    &prepared.unique_states,
                ],
            )
            .await
            .map_err(|err| {
                if let Some(db_err) = err.as_db_error() {
                    if db_err.code() == &::tokio_postgres::error::SqlState::UNIQUE_VIOLATION {
                        return AwaError::UniqueConflict {
                            constraint: db_err.constraint().map(|c| c.to_string()),
                        };
                    }
                }
                AwaError::TokioPg(err)
            })?;

        row_to_job_row(&row)
    }

    fn parse_state(state_str: &str) -> Result<JobState, AwaError> {
        match state_str {
            "scheduled" => Ok(JobState::Scheduled),
            "available" => Ok(JobState::Available),
            "running" => Ok(JobState::Running),
            "completed" => Ok(JobState::Completed),
            "retryable" => Ok(JobState::Retryable),
            "failed" => Ok(JobState::Failed),
            "cancelled" => Ok(JobState::Cancelled),
            "waiting_external" => Ok(JobState::WaitingExternal),
            other => Err(AwaError::Validation(format!(
                "unexpected job state from database: {other}"
            ))),
        }
    }

    /// Convert a `RETURNING *` row into a full [`JobRow`].
    fn row_to_job_row(row: &::tokio_postgres::Row) -> Result<JobRow, AwaError> {
        let state_str: String = row.get("state_str");

        Ok(JobRow {
            id: row.get("id"),
            kind: row.get("kind"),
            queue: row.get("queue"),
            args: row.get("args"),
            state: parse_state(&state_str)?,
            priority: row.get("priority"),
            attempt: row.get("attempt"),
            run_lease: row.get("run_lease"),
            max_attempts: row.get("max_attempts"),
            run_at: row.get("run_at"),
            heartbeat_at: row.get("heartbeat_at"),
            deadline_at: row.get("deadline_at"),
            attempted_at: row.get("attempted_at"),
            finalized_at: row.get("finalized_at"),
            created_at: row.get("created_at"),
            errors: None, // JSONB[] — tokio-postgres doesn't auto-deserialize Vec<Value>
            metadata: row.get("metadata"),
            tags: row.get("tags"),
            unique_key: row.get("unique_key"),
            unique_states: None, // BIT(8) — not directly mappable
            callback_id: row.get("callback_id"),
            callback_timeout_at: row.get("callback_timeout_at"),
            callback_filter: row.get("callback_filter"),
            callback_on_complete: row.get("callback_on_complete"),
            callback_on_fail: row.get("callback_on_fail"),
            callback_transform: row.get("callback_transform"),
            progress: row.get("progress"),
        })
    }
}
