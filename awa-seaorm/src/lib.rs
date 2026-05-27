//! SeaORM integration helpers for Awa.
//!
//! This crate stays deliberately thin. Awa's core is SQLx/Postgres-native,
//! and a SeaORM `DatabaseConnection` already wraps a `sqlx::PgPool` — so for
//! building a client, running migrations, or reading job state you can reach
//! the pool directly (see [`pool`]) and use Awa's existing APIs unchanged.
//!
//! The one thing the pool *can't* give you is enqueueing a job on the same
//! connection as an in-flight SeaORM transaction: `get_postgres_connection_pool()`
//! hands back a separate pooled connection, so a job inserted through it would
//! commit independently of your ORM writes. The [`insert`], [`insert_with`],
//! and [`insert_raw`] functions here run Awa's canonical insert SQL through
//! SeaORM's [`ConnectionTrait`], so they bind to whatever you pass —
//! a `DatabaseConnection` *or* a `DatabaseTransaction` — letting you enqueue a
//! job atomically with the rest of a transaction. The returned row is decoded
//! with Awa's own `FromRow`, so it is identical to a job from `awa::insert`.
//!
//! Job administration and reads are not duplicated here: use the `awa::admin`
//! API on the pool from [`pool`].

use awa::adapter::postgres::{prepare_job_insert, prepare_raw_job_insert, PreparedJobInsert};
use awa::{map_sqlx_error, AwaError, Client, ClientBuilder, InsertOpts, JobArgs, JobRow};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbErr, Statement};
use sqlx::FromRow;
use sqlx::PgPool;
use std::sync::Arc;

/// Convenience methods for using a SeaORM connection with Awa.
pub trait SeaOrmAwaExt {
    /// Access the underlying PostgreSQL pool.
    fn awa_pool(&self) -> &PgPool;

    /// Build an Awa client from the SeaORM connection's pool.
    fn awa_client_builder(&self) -> ClientBuilder;
}

impl SeaOrmAwaExt for DatabaseConnection {
    fn awa_pool(&self) -> &PgPool {
        self.get_postgres_connection_pool()
    }

    fn awa_client_builder(&self) -> ClientBuilder {
        Client::builder(self.awa_pool().clone())
    }
}

/// Return the underlying PostgreSQL pool from a SeaORM connection.
pub fn pool(connection: &DatabaseConnection) -> &PgPool {
    connection.awa_pool()
}

/// Build an Awa client builder from a SeaORM connection.
pub fn client_builder(connection: &DatabaseConnection) -> ClientBuilder {
    connection.awa_client_builder()
}

/// Run Awa migrations on a SeaORM connection.
pub async fn migrate(connection: &DatabaseConnection) -> Result<(), AwaError> {
    awa::migrations::run(connection.awa_pool()).await
}

/// Enqueue a job using the type-provided job kind and serialized arguments.
///
/// Runs on the supplied SeaORM connection or transaction, so passing a
/// `&DatabaseTransaction` commits the job atomically with the rest of that
/// transaction.
pub async fn insert<C>(connection: &C, args: &impl JobArgs) -> Result<JobRow, AwaError>
where
    C: ConnectionTrait,
{
    insert_with(connection, args, InsertOpts::default()).await
}

/// Enqueue a job with explicit enqueue options.
///
/// Like [`insert`], this binds to the supplied connection or transaction.
pub async fn insert_with<C>(
    connection: &C,
    args: &impl JobArgs,
    opts: InsertOpts,
) -> Result<JobRow, AwaError>
where
    C: ConnectionTrait,
{
    let prepared = prepare_job_insert(args, opts)?;
    insert_prepared(connection, &prepared).await
}

/// Enqueue a job from a raw kind name and JSON-compatible arguments.
///
/// Like [`insert`], this binds to the supplied connection or transaction.
pub async fn insert_raw<C>(
    connection: &C,
    kind: impl Into<String>,
    args: impl Into<serde_json::Value>,
    opts: InsertOpts,
) -> Result<JobRow, AwaError>
where
    C: ConnectionTrait,
{
    let prepared = prepare_raw_job_insert(kind, args, opts)?;
    insert_prepared(connection, &prepared).await
}

async fn insert_prepared<C>(
    connection: &C,
    prepared: &PreparedJobInsert,
) -> Result<JobRow, AwaError>
where
    C: ConnectionTrait,
{
    let unique_key = prepared.unique_key().map(<[u8]>::to_vec);
    let unique_states = prepared.unique_states_bit_string().map(ToOwned::to_owned);
    let ordering_key = prepared.ordering_key().map(<[u8]>::to_vec);

    let statement = Statement::from_sql_and_values(
        connection.get_database_backend(),
        awa::adapter::postgres::INSERT_JOB_SQL,
        vec![
            prepared.kind().into(),
            prepared.queue().into(),
            prepared.args().clone().into(),
            prepared.state_db_str().into(),
            prepared.priority().into(),
            prepared.max_attempts().into(),
            prepared.run_at().into(),
            prepared.metadata().clone().into(),
            prepared.tags().to_vec().into(),
            unique_key.into(),
            unique_states.into(),
            ordering_key.into(),
        ],
    );

    let result = connection
        .query_one_raw(statement)
        .await
        .map_err(map_db_err)?
        .ok_or_else(|| {
            AwaError::Database(sqlx::Error::Protocol(
                "insert_job_compat returned no row".to_string(),
            ))
        })?;

    // SeaORM ran the query but `JobRow` already knows how to decode an
    // `awa.jobs` row, so reach the underlying sqlx row and reuse it rather
    // than maintaining a parallel column-by-column decoder.
    let row = result.try_as_pg_row().ok_or_else(|| {
        AwaError::Database(sqlx::Error::Protocol(
            "expected a PostgreSQL row from the insert".to_string(),
        ))
    })?;

    JobRow::from_row(row).map_err(AwaError::from)
}

/// Translate a SeaORM error into Awa's error type.
///
/// Where SeaORM carries the underlying sqlx error, route it through Awa's
/// canonical [`map_sqlx_error`] so the result — including unique-conflict
/// detection — is identical to the native `awa::insert` path. SeaORM mints a
/// fresh `Arc` per error, so the unwrap normally succeeds; the rare shared
/// case falls back to a database error.
fn map_db_err(err: DbErr) -> AwaError {
    match err {
        DbErr::Exec(sea_orm::RuntimeErr::SqlxError(err))
        | DbErr::Query(sea_orm::RuntimeErr::SqlxError(err))
        | DbErr::Conn(sea_orm::RuntimeErr::SqlxError(err)) => match Arc::try_unwrap(err) {
            Ok(err) => map_sqlx_error(err),
            Err(err) => AwaError::Database(sqlx::Error::Protocol(err.to_string())),
        },
        other => AwaError::Database(sqlx::Error::Protocol(other.to_string())),
    }
}
