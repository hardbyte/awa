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

        JobRow::try_from(&row)
    }

    fn parse_state(state_str: &str) -> Result<JobState, AwaError> {
        state_str
            .parse()
            .map_err(|e: String| AwaError::Validation(e))
    }

    /// Decode a column from a tokio-postgres row, mapping errors to
    /// [`AwaError::Validation`] instead of panicking.
    fn col<'a, T: ::tokio_postgres::types::FromSql<'a>>(
        row: &'a ::tokio_postgres::Row,
        name: &str,
    ) -> Result<T, AwaError> {
        row.try_get(name)
            .map_err(|e| AwaError::Validation(format!("failed to decode column {name}: {e}")))
    }

    impl TryFrom<&::tokio_postgres::Row> for JobRow {
        type Error = AwaError;

        /// Convert a `RETURNING *, state::text AS state_str` row into a
        /// full [`JobRow`].
        fn try_from(row: &::tokio_postgres::Row) -> Result<Self, Self::Error> {
            let state_str: String = col(row, "state_str")?;

            Ok(JobRow {
                id: col(row, "id")?,
                kind: col(row, "kind")?,
                queue: col(row, "queue")?,
                args: col(row, "args")?,
                state: parse_state(&state_str)?,
                priority: col(row, "priority")?,
                attempt: col(row, "attempt")?,
                run_lease: col(row, "run_lease")?,
                max_attempts: col(row, "max_attempts")?,
                run_at: col(row, "run_at")?,
                heartbeat_at: col(row, "heartbeat_at")?,
                deadline_at: col(row, "deadline_at")?,
                attempted_at: col(row, "attempted_at")?,
                finalized_at: col(row, "finalized_at")?,
                created_at: col(row, "created_at")?,
                errors: col(row, "errors")?,
                metadata: col(row, "metadata")?,
                tags: col(row, "tags")?,
                unique_key: col(row, "unique_key")?,
                unique_states: None, // BIT(8) — no direct tokio-postgres mapping
                callback_id: col(row, "callback_id")?,
                callback_timeout_at: col(row, "callback_timeout_at")?,
                callback_filter: col(row, "callback_filter")?,
                callback_on_complete: col(row, "callback_on_complete")?,
                callback_on_fail: col(row, "callback_on_fail")?,
                callback_transform: col(row, "callback_transform")?,
                progress: col(row, "progress")?,
            })
        }
    }
}
