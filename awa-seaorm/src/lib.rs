//! SeaORM integration helpers for Awa.
//!
//! This crate stays deliberately thin: it exposes the underlying
//! `sqlx::PgPool` from a SeaORM `DatabaseConnection`, then reuses Awa's
//! existing migration and insertion helpers on that pool.
//!
//! It does not add a new storage engine, and it does not replace the
//! existing `awa` sqlx API.

use awa::{AwaError, Client, ClientBuilder, InsertOpts, JobArgs, JobRow};
use sea_orm::DatabaseConnection;
use sqlx::PgPool;

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

/// Insert a job using Awa's existing sqlx path via the SeaORM connection pool.
pub async fn insert(
    connection: &DatabaseConnection,
    args: &impl JobArgs,
) -> Result<JobRow, AwaError> {
    awa::insert(connection.awa_pool(), args).await
}

/// Insert a job with custom options using the SeaORM connection pool.
pub async fn insert_with(
    connection: &DatabaseConnection,
    args: &impl JobArgs,
    opts: InsertOpts,
) -> Result<JobRow, AwaError> {
    awa::insert_with(connection.awa_pool(), args, opts).await
}
