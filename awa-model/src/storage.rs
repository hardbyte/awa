use crate::error::AwaError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgExecutor};

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, PartialEq)]
pub struct StorageStatus {
    pub current_engine: String,
    pub active_engine: String,
    pub prepared_engine: Option<String>,
    pub state: String,
    pub transition_epoch: i64,
    pub details: serde_json::Value,
    pub entered_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub finalized_at: Option<DateTime<Utc>>,
}

pub async fn status<'e, E>(executor: E) -> Result<StorageStatus, AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, StorageStatus>("SELECT * FROM awa.storage_status()")
        .fetch_one(executor)
        .await
        .map_err(AwaError::from)
}

pub async fn prepare<'e, E>(
    executor: E,
    engine: &str,
    details: serde_json::Value,
) -> Result<StorageStatus, AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, StorageStatus>("SELECT * FROM awa.storage_prepare($1, $2)")
        .bind(engine)
        .bind(details)
        .fetch_one(executor)
        .await
        .map_err(AwaError::from)
}

pub async fn abort<'e, E>(executor: E) -> Result<StorageStatus, AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, StorageStatus>("SELECT * FROM awa.storage_abort()")
        .fetch_one(executor)
        .await
        .map_err(AwaError::from)
}

pub async fn enter_mixed_transition<'e, E>(executor: E) -> Result<StorageStatus, AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, StorageStatus>("SELECT * FROM awa.storage_enter_mixed_transition()")
        .fetch_one(executor)
        .await
        .map_err(AwaError::from)
}

pub async fn finalize<'e, E>(executor: E) -> Result<StorageStatus, AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, StorageStatus>("SELECT * FROM awa.storage_finalize()")
        .fetch_one(executor)
        .await
        .map_err(AwaError::from)
}
