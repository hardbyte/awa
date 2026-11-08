//! Durable Phase 1 control. Mutations require explicit operator EXECUTE grants.
use crate::AwaError;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

pub const PRUNE_SCOPE: &str = "maintenance.prune";

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct RuntimeControl {
    pub scope: String,
    pub revision: i64,
    pub halt_id: Option<Uuid>,
    pub mode: String,
    pub reason: String,
    pub actor: String,
    pub updated_at: DateTime<Utc>,
}

pub async fn read(conn: &mut PgConnection) -> Result<RuntimeControl, sqlx::Error> {
    sqlx::query_as("SELECT scope, revision, halt_id, mode, reason, actor, updated_at FROM awa.runtime_controls WHERE scope = 'maintenance.prune'")
        .fetch_one(conn).await
}

pub async fn halt(
    pool: &PgPool,
    scope: &str,
    reason: &str,
    mode: &str,
) -> Result<RuntimeControl, AwaError> {
    Ok(
        sqlx::query_as("SELECT * FROM awa.halt_runtime_scope($1, $2, $3)")
            .bind(scope)
            .bind(reason)
            .bind(mode)
            .fetch_one(pool)
            .await?,
    )
}

pub async fn resume(pool: &PgPool, scope: &str, halt_id: Uuid) -> Result<RuntimeControl, AwaError> {
    Ok(
        sqlx::query_as("SELECT * FROM awa.resume_runtime_scope($1, $2)")
            .bind(scope)
            .bind(halt_id)
            .fetch_one(pool)
            .await?,
    )
}
