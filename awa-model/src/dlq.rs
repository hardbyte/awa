//! Dead-letter queue helpers for the queue_storage backend.
//!
//! On this branch the DLQ is part of the queue_storage engine. The public API
//! resolves the active queue_storage schema from `awa.runtime_storage_backends`
//! and operates against that backend.

use crate::error::AwaError;
use crate::job::JobRow;
use crate::queue_storage::QueueStorage;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DlqMetadata {
    pub reason: String,
    pub dlq_at: DateTime<Utc>,
    pub original_run_lease: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqRow {
    pub job: JobRow,
    pub reason: String,
    pub dlq_at: DateTime<Utc>,
    pub original_run_lease: i64,
}

impl DlqRow {
    pub fn metadata(&self) -> DlqMetadata {
        DlqMetadata {
            reason: self.reason.clone(),
            dlq_at: self.dlq_at,
            original_run_lease: self.original_run_lease,
        }
    }

    pub fn into_parts(self) -> (JobRow, DlqMetadata) {
        let metadata = DlqMetadata {
            reason: self.reason,
            dlq_at: self.dlq_at,
            original_run_lease: self.original_run_lease,
        };
        (self.job, metadata)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListDlqFilter {
    pub kind: Option<String>,
    pub queue: Option<String>,
    pub tag: Option<String>,
    pub before_id: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetryFromDlqOpts {
    pub run_at: Option<DateTime<Utc>>,
    pub priority: Option<i16>,
    pub queue: Option<String>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct QueueStorageDlqRow {
    job_id: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    state: crate::JobState,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    run_at: DateTime<Utc>,
    attempted_at: Option<DateTime<Utc>>,
    finalized_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    payload: serde_json::Value,
    dlq_reason: String,
    dlq_at: DateTime<Utc>,
    original_run_lease: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueStoragePayload {
    #[serde(default)]
    metadata: serde_json::Value,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    errors: Vec<serde_json::Value>,
    #[serde(default)]
    progress: Option<serde_json::Value>,
}

impl Default for QueueStoragePayload {
    fn default() -> Self {
        Self {
            metadata: serde_json::json!({}),
            tags: Vec::new(),
            errors: Vec::new(),
            progress: None,
        }
    }
}

impl QueueStorageDlqRow {
    fn into_dlq_row(self) -> Result<DlqRow, AwaError> {
        let payload: QueueStoragePayload = serde_json::from_value(self.payload)?;
        Ok(DlqRow {
            job: JobRow {
                id: self.job_id,
                kind: self.kind,
                queue: self.queue,
                args: self.args,
                state: self.state,
                priority: self.priority,
                attempt: self.attempt,
                run_lease: self.run_lease,
                max_attempts: self.max_attempts,
                run_at: self.run_at,
                heartbeat_at: None,
                deadline_at: None,
                attempted_at: self.attempted_at,
                finalized_at: Some(self.finalized_at),
                created_at: self.created_at,
                errors: (!payload.errors.is_empty()).then_some(payload.errors),
                metadata: payload.metadata,
                tags: payload.tags,
                unique_key: self.unique_key,
                unique_states: None,
                callback_id: None,
                callback_timeout_at: None,
                callback_filter: None,
                callback_on_complete: None,
                callback_on_fail: None,
                callback_transform: None,
                progress: payload.progress,
            },
            reason: self.dlq_reason,
            dlq_at: self.dlq_at,
            original_run_lease: self.original_run_lease,
        })
    }
}

async fn active_queue_storage(pool: &PgPool) -> Result<QueueStorage, AwaError> {
    let schema = QueueStorage::active_schema(pool).await?.ok_or_else(|| {
        AwaError::Validation(
            "DLQ APIs currently require an active queue_storage backend".to_string(),
        )
    })?;
    QueueStorage::from_existing_schema(schema)
}

pub async fn move_failed_to_dlq(
    pool: &PgPool,
    job_id: i64,
    reason: &str,
) -> Result<Option<DlqRow>, AwaError> {
    let store = active_queue_storage(pool).await?;
    if store
        .move_failed_to_dlq(pool, job_id, reason)
        .await?
        .is_none()
    {
        return Ok(None);
    }
    get_dlq_job(pool, job_id).await
}

pub async fn list_dlq(pool: &PgPool, filter: &ListDlqFilter) -> Result<Vec<DlqRow>, AwaError> {
    let store = active_queue_storage(pool).await?;
    let schema = store.schema();
    let rows: Vec<QueueStorageDlqRow> = sqlx::query_as(&format!(
        r#"
        SELECT
            job_id,
            kind,
            queue,
            args,
            state,
            priority,
            attempt,
            run_lease,
            max_attempts,
            run_at,
            attempted_at,
            finalized_at,
            created_at,
            unique_key,
            payload,
            dlq_reason,
            dlq_at,
            original_run_lease
        FROM {schema}.dlq_entries
        WHERE ($1::text IS NULL OR kind = $1)
          AND ($2::text IS NULL OR queue = $2)
          AND ($3::text IS NULL OR payload -> 'tags' ? $3)
          AND ($4::bigint IS NULL OR job_id < $4)
        ORDER BY dlq_at DESC, job_id DESC
        LIMIT $5
        "#
    ))
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .bind(filter.before_id)
    .bind(filter.limit.unwrap_or(100))
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(QueueStorageDlqRow::into_dlq_row)
        .collect()
}

pub async fn get_dlq_job(pool: &PgPool, job_id: i64) -> Result<Option<DlqRow>, AwaError> {
    let store = active_queue_storage(pool).await?;
    let schema = store.schema();
    let row: Option<QueueStorageDlqRow> = sqlx::query_as(&format!(
        r#"
        SELECT
            job_id,
            kind,
            queue,
            args,
            state,
            priority,
            attempt,
            run_lease,
            max_attempts,
            run_at,
            attempted_at,
            finalized_at,
            created_at,
            unique_key,
            payload,
            dlq_reason,
            dlq_at,
            original_run_lease
        FROM {schema}.dlq_entries
        WHERE job_id = $1
        ORDER BY dlq_at DESC
        LIMIT 1
        "#
    ))
    .bind(job_id)
    .fetch_optional(pool)
    .await?;

    row.map(QueueStorageDlqRow::into_dlq_row).transpose()
}

pub async fn dlq_depth(pool: &PgPool, queue: Option<&str>) -> Result<i64, AwaError> {
    let store = active_queue_storage(pool).await?;
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT count(*)::bigint FROM {}.dlq_entries WHERE ($1::text IS NULL OR queue = $1)",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .map_err(Into::into)
}

pub async fn dlq_depth_by_queue(pool: &PgPool) -> Result<Vec<(String, i64)>, AwaError> {
    let store = active_queue_storage(pool).await?;
    sqlx::query_as::<_, (String, i64)>(&format!(
        r#"
        SELECT queue, count(*)::bigint
        FROM {}.dlq_entries
        GROUP BY queue
        ORDER BY count(*) DESC, queue ASC
        "#,
        store.schema()
    ))
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

pub async fn retry_from_dlq(
    pool: &PgPool,
    job_id: i64,
    opts: &RetryFromDlqOpts,
) -> Result<Option<JobRow>, AwaError> {
    let store = active_queue_storage(pool).await?;
    store.retry_from_dlq(pool, job_id, opts).await
}

pub async fn purge_dlq(pool: &PgPool, filter: &ListDlqFilter) -> Result<u64, AwaError> {
    let store = active_queue_storage(pool).await?;
    let result = sqlx::query(&format!(
        r#"
        DELETE FROM {}.dlq_entries
        WHERE ($1::text IS NULL OR kind = $1)
          AND ($2::text IS NULL OR queue = $2)
          AND ($3::text IS NULL OR payload -> 'tags' ? $3)
          AND ($4::bigint IS NULL OR job_id < $4)
        "#,
        store.schema()
    ))
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .bind(filter.before_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub async fn purge_dlq_job(pool: &PgPool, job_id: i64) -> Result<bool, AwaError> {
    let store = active_queue_storage(pool).await?;
    let result = sqlx::query(&format!(
        "DELETE FROM {}.dlq_entries WHERE job_id = $1",
        store.schema()
    ))
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}
