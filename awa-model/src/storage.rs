use crate::admin;
use crate::error::AwaError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgExecutor, PgPool};
use std::collections::BTreeMap;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageStatusReport {
    #[serde(flatten)]
    pub status: StorageStatus,
    pub canonical_live_backlog: i64,
    pub prepared_queue_storage_schema: Option<String>,
    pub prepared_schema_ready: bool,
    pub live_runtime_capability_counts: BTreeMap<String, usize>,
    pub can_enter_mixed_transition: bool,
    pub enter_mixed_transition_blockers: Vec<String>,
    pub can_finalize: bool,
    pub finalize_blockers: Vec<String>,
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

fn queue_storage_schema_from_status(status: &StorageStatus) -> Option<String> {
    let queue_storage_involved = status.prepared_engine.as_deref() == Some("queue_storage")
        || status.active_engine == "queue_storage";
    if !queue_storage_involved {
        return None;
    }
    Some(
        status
            .details
            .get("schema")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("awa")
            .to_string(),
    )
}

pub async fn queue_storage_schema_ready(pool: &PgPool, schema: &str) -> Result<bool, AwaError> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT
            to_regclass(format('%I.%I', $1, 'queue_ring_state')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'ready_entries')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'leases')) IS NOT NULL
        "#,
    )
    .bind(schema)
    .fetch_one(pool)
    .await
    .map_err(AwaError::from)
}

async fn canonical_live_backlog(pool: &PgPool) -> Result<i64, AwaError> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT
            COALESCE((
                SELECT count(*)::bigint
                FROM awa.jobs_hot
                WHERE state NOT IN ('completed', 'failed', 'cancelled')
            ), 0)
            +
            COALESCE((
                SELECT count(*)::bigint
                FROM awa.scheduled_jobs
            ), 0)
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(AwaError::from)
}

pub async fn status_report(pool: &PgPool) -> Result<StorageStatusReport, AwaError> {
    let status = status(pool).await?;
    let canonical_live_backlog = canonical_live_backlog(pool).await?;
    let prepared_queue_storage_schema = queue_storage_schema_from_status(&status);
    let prepared_schema_ready = match prepared_queue_storage_schema.as_deref() {
        Some(schema) => queue_storage_schema_ready(pool, schema).await?,
        None => false,
    };

    let instances = admin::list_runtime_instances(pool).await?;
    let mut live_runtime_capability_counts = BTreeMap::new();
    for instance in instances.into_iter().filter(|instance| !instance.stale) {
        *live_runtime_capability_counts
            .entry(instance.storage_capability.as_str().to_string())
            .or_insert(0) += 1;
    }

    let live_canonical = *live_runtime_capability_counts
        .get("canonical")
        .unwrap_or(&0);
    let live_queue_storage = *live_runtime_capability_counts
        .get("queue_storage")
        .unwrap_or(&0);
    let live_drain = *live_runtime_capability_counts
        .get("canonical_drain_only")
        .unwrap_or(&0);

    let mut enter_mixed_transition_blockers = Vec::new();
    if status.state != "prepared" {
        enter_mixed_transition_blockers
            .push(format!("state is '{}' (expected 'prepared')", status.state));
    }
    if status.prepared_engine.as_deref() != Some("queue_storage") {
        enter_mixed_transition_blockers.push(format!(
            "prepared_engine is {:?} (expected Some(\"queue_storage\"))",
            status.prepared_engine
        ));
    }
    if prepared_queue_storage_schema.is_some() && !prepared_schema_ready {
        enter_mixed_transition_blockers
            .push("prepared queue-storage schema is missing required tables".to_string());
    }
    if live_canonical > 0 {
        enter_mixed_transition_blockers.push(format!(
            "{live_canonical} canonical-only runtime(s) are still live"
        ));
    }
    if live_queue_storage == 0 {
        enter_mixed_transition_blockers
            .push("no live queue-storage-capable runtimes are reporting".to_string());
    }

    let mut finalize_blockers = Vec::new();
    if status.state != "mixed_transition" {
        finalize_blockers.push(format!(
            "state is '{}' (expected 'mixed_transition')",
            status.state
        ));
    }
    if status.prepared_engine.as_deref() != Some("queue_storage") {
        finalize_blockers.push(format!(
            "prepared_engine is {:?} (expected Some(\"queue_storage\"))",
            status.prepared_engine
        ));
    }
    if canonical_live_backlog > 0 {
        finalize_blockers.push(format!(
            "canonical live backlog is {canonical_live_backlog}"
        ));
    }
    if live_canonical + live_drain > 0 {
        finalize_blockers.push(format!(
            "{} canonical or drain-only runtime(s) are still live",
            live_canonical + live_drain
        ));
    }

    Ok(StorageStatusReport {
        status,
        canonical_live_backlog,
        prepared_queue_storage_schema,
        prepared_schema_ready,
        live_runtime_capability_counts,
        can_enter_mixed_transition: enter_mixed_transition_blockers.is_empty(),
        enter_mixed_transition_blockers,
        can_finalize: finalize_blockers.is_empty(),
        finalize_blockers,
    })
}
