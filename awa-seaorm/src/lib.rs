//! SeaORM integration helpers for Awa.
//!
//! This crate provides a SeaORM-first repository for Awa job administration
//! while preserving the original transactional insert helpers.

/// SeaORM entity definitions for the Awa schema.
pub mod entity;
mod mapping;
mod repo;
mod sql;

/// SeaORM-backed repository for job enqueue and lifecycle operations.
pub use repo::JobRepository;

use awa::{AwaError, Client, ClientBuilder, InsertOpts, JobArgs, JobRow};
use sea_orm::{ConnectionTrait, DatabaseConnection};
use sqlx::PgPool;

/// Convenience methods for using a SeaORM connection with Awa.
pub trait SeaOrmAwaExt {
    /// Access the underlying PostgreSQL pool.
    fn awa_pool(&self) -> &PgPool;

    /// Build an Awa client from the SeaORM connection's pool.
    fn awa_client_builder(&self) -> ClientBuilder;

    /// Build a SeaORM job repository.
    fn awa_jobs(&self) -> JobRepository<'_, Self>
    where
        Self: Sized + ConnectionTrait,
    {
        JobRepository::new(self)
    }
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

/// Insert a job using SeaORM's connection or transaction.
pub async fn insert<C>(connection: &C, args: &impl JobArgs) -> Result<JobRow, AwaError>
where
    C: ConnectionTrait,
{
    JobRepository::new(connection).insert(args).await
}

/// Insert a job with custom options using SeaORM's connection or transaction.
pub async fn insert_with<C>(
    connection: &C,
    args: &impl JobArgs,
    opts: InsertOpts,
) -> Result<JobRow, AwaError>
where
    C: ConnectionTrait,
{
    JobRepository::new(connection).insert_with(args, opts).await
}

/// Insert a job from raw kind + JSON args + options.
pub async fn insert_raw<C>(
    connection: &C,
    kind: impl Into<String>,
    args: impl Into<serde_json::Value>,
    opts: InsertOpts,
) -> Result<JobRow, AwaError>
where
    C: ConnectionTrait,
{
    JobRepository::new(connection)
        .insert_raw(kind, args, opts)
        .await
}
