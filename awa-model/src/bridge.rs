//! Transactional enqueue for non-sqlx Postgres stacks.
//!
//! The core [`crate::insert::insert`] and [`crate::insert::insert_with`]
//! functions require sqlx's `PgExecutor` trait. This module provides
//! feature-gated adapters so users of other Postgres libraries can insert
//! jobs within their existing transactions.
//!
//! All adapters share the same preparation logic in
//! [`crate::adapter::postgres::prepare_job_insert`] — validation, state
//! determination, unique key computation, and ordering-key propagation happen
//! exactly once, avoiding semantic drift between drivers.
//!
//! # Available adapters
//!
//! | Feature | Module | Works with |
//! |---------|--------|------------|
//! | `tokio-postgres` | [`tokio_pg`] | `tokio_postgres::Client` and `Transaction` |
//! | `sqlx09` | [`sqlx09`] | sqlx 0.9 `PgPool`, `PgConnection` and `Transaction` |

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
    use crate::adapter::postgres::{
        prepare_job_insert, prepare_raw_job_insert, PreparedJobInsert, INSERT_JOB_SQL,
    };
    use crate::error::AwaError;
    use crate::job::{InsertOpts, JobRow, JobState};
    use crate::JobArgs;
    use ::tokio_postgres::GenericClient;

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
        let prepared = prepare_job_insert(args, opts)?;
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
        let prepared = prepare_raw_job_insert(kind, args, opts)?;
        execute(client, &prepared).await
    }

    async fn execute<C: GenericClient>(
        client: &C,
        prepared: &PreparedJobInsert,
    ) -> Result<JobRow, AwaError> {
        let state_str = prepared.state_db_str();

        let row = client
            .query_one(
                INSERT_JOB_SQL,
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
                    &prepared.ordering_key,
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

// ---------------------------------------------------------------------------
// sqlx 0.9 adapter
// ---------------------------------------------------------------------------

/// Adapter for inserting Awa jobs through sqlx 0.9 executors.
///
/// Awa 0.6 is built on sqlx 0.8, so its native insert functions cannot join a
/// sqlx 0.9 transaction. These functions take any sqlx 0.9
/// [`PgExecutor`](::sqlx09::PgExecutor) — `&PgPool`, `&mut PgConnection` or
/// `&mut *transaction` — and run the same SQL as the native path.
///
/// The feature adds no runtime or TLS feature to sqlx 0.9; the application's
/// own sqlx 0.9 dependency selects them.
#[cfg(feature = "sqlx09")]
pub mod sqlx09 {
    use crate::adapter::postgres::{
        prepare_job_insert, prepare_raw_job_insert, PreparedJobInsert, INSERT_JOB_SQL,
    };
    use crate::error::AwaError;
    use crate::insert::{ordering_hash_text, precompute_rows, INSERT_MANY_ROW_FUNCTION_SQL};
    use crate::job::{InsertOpts, InsertParams, JobRow, JobState};
    use crate::JobArgs;
    use ::sqlx09::postgres::PgRow;
    use ::sqlx09::{PgExecutor, Row};

    /// Insert a job with default options.
    ///
    /// Mirrors [`crate::insert::insert`].
    pub async fn insert_job<'e, E: PgExecutor<'e>>(
        executor: E,
        args: &impl JobArgs,
    ) -> Result<JobRow, AwaError> {
        insert_job_with(executor, args, InsertOpts::default()).await
    }

    /// Insert a job with custom options.
    ///
    /// Mirrors [`crate::insert::insert_with`], including the mapping of a
    /// unique violation to [`AwaError::UniqueConflict`].
    pub async fn insert_job_with<'e, E: PgExecutor<'e>>(
        executor: E,
        args: &impl JobArgs,
        opts: InsertOpts,
    ) -> Result<JobRow, AwaError> {
        let prepared = prepare_job_insert(args, opts)?;
        execute(executor, &prepared).await
    }

    /// Insert a job from raw kind + JSON args + options.
    pub async fn insert_job_raw<'e, E: PgExecutor<'e>>(
        executor: E,
        kind: String,
        args: serde_json::Value,
        opts: InsertOpts,
    ) -> Result<JobRow, AwaError> {
        let prepared = prepare_raw_job_insert(kind, args, opts)?;
        execute(executor, &prepared).await
    }

    /// Insert several jobs in one statement.
    ///
    /// Mirrors [`crate::insert::insert_many`]: a unique violation fails the
    /// whole batch and is returned as [`AwaError::Sqlx09`].
    pub async fn insert_many<'e, E: PgExecutor<'e>>(
        executor: E,
        jobs: &[InsertParams],
    ) -> Result<Vec<JobRow>, AwaError> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }

        let rows: Vec<PreparedJobInsert> = precompute_rows(jobs)?;
        let inserted: Vec<PgRow> = ::sqlx09::query(INSERT_MANY_ROW_FUNCTION_SQL)
            .bind(rows.iter().map(|row| row.kind.as_str()).collect::<Vec<_>>())
            .bind(
                rows.iter()
                    .map(|row| row.queue.as_str())
                    .collect::<Vec<_>>(),
            )
            .bind(rows.iter().map(|row| &row.args).collect::<Vec<_>>())
            .bind(
                rows.iter()
                    .map(|row| row.state_db_str())
                    .collect::<Vec<_>>(),
            )
            .bind(rows.iter().map(|row| row.priority).collect::<Vec<_>>())
            .bind(rows.iter().map(|row| row.max_attempts).collect::<Vec<_>>())
            .bind(rows.iter().map(|row| row.run_at).collect::<Vec<_>>())
            .bind(rows.iter().map(|row| &row.metadata).collect::<Vec<_>>())
            .bind(
                rows.iter()
                    .map(|row| serde_json::json!(row.tags))
                    .collect::<Vec<_>>(),
            )
            .bind(
                rows.iter()
                    .map(|row| row.unique_key.as_deref())
                    .collect::<Vec<_>>(),
            )
            .bind(
                rows.iter()
                    .map(|row| row.unique_states.as_deref())
                    .collect::<Vec<_>>(),
            )
            .bind(
                rows.iter()
                    .map(|row| row.ordering_key.as_deref())
                    .collect::<Vec<_>>(),
            )
            .bind(rows.iter().map(ordering_hash_text).collect::<Vec<_>>())
            .fetch_all(executor)
            .await
            .map_err(AwaError::Sqlx09)?;

        inserted.iter().map(job_row).collect()
    }

    async fn execute<'e, E: PgExecutor<'e>>(
        executor: E,
        prepared: &PreparedJobInsert,
    ) -> Result<JobRow, AwaError> {
        let row: PgRow = ::sqlx09::query(INSERT_JOB_SQL)
            .bind(&prepared.kind)
            .bind(&prepared.queue)
            .bind(&prepared.args)
            .bind(prepared.state_db_str())
            .bind(prepared.priority)
            .bind(prepared.max_attempts)
            .bind(prepared.run_at)
            .bind(&prepared.metadata)
            .bind(&prepared.tags)
            .bind(&prepared.unique_key)
            .bind(&prepared.unique_states)
            .bind(&prepared.ordering_key)
            .fetch_one(executor)
            .await
            .map_err(map_insert_error)?;

        job_row(&row)
    }

    fn map_insert_error(err: ::sqlx09::Error) -> AwaError {
        if let ::sqlx09::Error::Database(ref database) = err {
            if database.code().as_deref()
                == Some(crate::adapter::postgres::UNIQUE_VIOLATION_SQLSTATE)
            {
                return AwaError::UniqueConflict {
                    constraint: database.constraint().map(|c| c.to_string()),
                };
            }
        }
        AwaError::Sqlx09(err)
    }

    /// Decode an `awa.jobs`-shaped row. `state` is an `awa.job_state` enum,
    /// whose wire format is its label, so it is read as text without sqlx's
    /// type check.
    fn job_row(row: &PgRow) -> Result<JobRow, AwaError> {
        let state_label: String = row.try_get_unchecked("state").map_err(AwaError::Sqlx09)?;
        let state: JobState = state_label.parse().map_err(AwaError::Validation)?;

        Ok(JobRow {
            id: get(row, "id")?,
            kind: get(row, "kind")?,
            queue: get(row, "queue")?,
            args: get(row, "args")?,
            state,
            priority: get(row, "priority")?,
            attempt: get(row, "attempt")?,
            run_lease: get(row, "run_lease")?,
            max_attempts: get(row, "max_attempts")?,
            run_at: get(row, "run_at")?,
            heartbeat_at: get(row, "heartbeat_at")?,
            deadline_at: get(row, "deadline_at")?,
            attempted_at: get(row, "attempted_at")?,
            finalized_at: get(row, "finalized_at")?,
            created_at: get(row, "created_at")?,
            errors: get(row, "errors")?,
            metadata: get(row, "metadata")?,
            tags: get(row, "tags")?,
            unique_key: get(row, "unique_key")?,
            unique_states: None,
            callback_id: get(row, "callback_id")?,
            callback_timeout_at: get(row, "callback_timeout_at")?,
            callback_filter: get(row, "callback_filter")?,
            callback_on_complete: get(row, "callback_on_complete")?,
            callback_on_fail: get(row, "callback_on_fail")?,
            callback_transform: get(row, "callback_transform")?,
            progress: get(row, "progress")?,
        })
    }

    fn get<'r, T>(row: &'r PgRow, name: &str) -> Result<T, AwaError>
    where
        T: ::sqlx09::Decode<'r, ::sqlx09::Postgres> + ::sqlx09::Type<::sqlx09::Postgres>,
    {
        row.try_get(name).map_err(AwaError::Sqlx09)
    }
}
