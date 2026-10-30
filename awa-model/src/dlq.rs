//! Dead-letter queue helpers for the queue_storage backend.
//!
//! The DLQ is part of the queue_storage engine. The public API resolves the
//! active queue_storage schema from `awa.runtime_storage_backends` and operates
//! against that backend.

use crate::audited_sql;
use crate::error::AwaError;
use crate::filter::{DlqPage, JobFilter};
use crate::job::JobRow;
use crate::job_query::{dlq_predicate, Bind, Predicate};
use crate::queue_storage::QueueStorage;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DlqMetadata {
    pub reason: String,
    pub dlq_at: DateTime<Utc>,
    pub original_run_lease: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqRow {
    #[serde(flatten)]
    pub job: JobRow,
    #[serde(rename = "dlq_reason")]
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

/// Superseded by [`JobFilter`] with [`DlqPage`] (ADR-049), which keeps
/// paging out of the filter that purge and bulk retry act on.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListDlqFilter {
    pub kind: Option<String>,
    pub queue: Option<String>,
    pub tag: Option<String>,
    pub before_id: Option<i64>,
    pub before_dlq_at: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
}

impl ListDlqFilter {
    /// The equivalent filter and page.
    pub fn split(&self) -> (JobFilter, DlqPage) {
        let filter = JobFilter {
            kind: self.kind.clone(),
            queue: self.queue.clone(),
            tags: self.tag.clone().into_iter().collect(),
            ..Default::default()
        };
        let page = DlqPage {
            before_dlq_at: self.before_dlq_at,
            before_id: self.before_id,
            limit: self.limit.unwrap_or(crate::filter::DEFAULT_PAGE_LIMIT),
        };
        (filter, page)
    }
}

/// Narrow `predicate` to rows before a `(dlq_at, id)` position, newest first.
fn before_position(
    predicate: &mut Predicate,
    before_dlq_at: Option<DateTime<Utc>>,
    before_id: Option<i64>,
) {
    match (before_dlq_at, before_id) {
        (None, None) => {}
        (None, Some(id)) => {
            let p = predicate.param(Bind::Int(id));
            predicate.conditions.push(format!("job_id < {p}"));
        }
        (Some(at), None) => {
            let p = predicate.param(Bind::Time(at));
            predicate.conditions.push(format!("dlq_at < {p}"));
        }
        (Some(at), Some(id)) => {
            let at = predicate.param(Bind::Time(at));
            let id = predicate.param(Bind::Int(id));
            predicate
                .conditions
                .push(format!("(dlq_at, job_id) < ({at}, {id})"));
        }
    }
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
    #[serde(default = "default_queue_storage_payload_metadata")]
    metadata: serde_json::Value,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    errors: Vec<serde_json::Value>,
    #[serde(default)]
    progress: Option<serde_json::Value>,
}

fn default_queue_storage_payload_metadata() -> serde_json::Value {
    serde_json::json!({})
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
                output: None,
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

/// Bulk-move failed terminal rows into the DLQ.
///
/// Returns the number of rows moved. Requires at least one of `kind` or
/// `queue` unless `allow_all` is `true`, which is an explicit opt-in to move
/// every failed row currently stored in queue_storage.
///
/// Preserves the source row's progress snapshot and error history so manual
/// operator moves do not discard the last known execution state.
pub async fn bulk_move_failed_to_dlq(
    pool: &PgPool,
    kind: Option<&str>,
    queue: Option<&str>,
    reason: &str,
    allow_all: bool,
) -> Result<u64, AwaError> {
    if !allow_all && kind.is_none() && queue.is_none() {
        return Err(AwaError::Validation(
            "bulk_move_failed_to_dlq requires at least one of kind or queue (or allow_all=true)"
                .into(),
        ));
    }

    let store = active_queue_storage(pool).await?;
    store
        .bulk_move_failed_to_dlq(pool, kind, queue, reason)
        .await
}

/// Dead-lettered jobs matching `filter`, newest first by `(dlq_at, id)`.
pub async fn find_dlq(
    pool: &PgPool,
    filter: &JobFilter,
    page: DlqPage,
) -> Result<Vec<DlqRow>, AwaError> {
    filter.validate()?;
    list_dlq_where(pool, filter, page).await
}

#[deprecated(
    since = "0.7.0",
    note = "use `find_dlq` with a `JobFilter` and `DlqPage`"
)]
pub async fn list_dlq(pool: &PgPool, filter: &ListDlqFilter) -> Result<Vec<DlqRow>, AwaError> {
    let (filter, page) = filter.split();
    list_dlq_where(pool, &filter, page).await
}

async fn list_dlq_where(
    pool: &PgPool,
    filter: &JobFilter,
    page: DlqPage,
) -> Result<Vec<DlqRow>, AwaError> {
    let store = active_queue_storage(pool).await?;
    let Some(mut predicate) = dlq_predicate(filter) else {
        return Ok(Vec::new());
    };
    before_position(&mut predicate, page.before_dlq_at, page.before_id);
    let limit = predicate.param(Bind::Int(page.clamped_limit()));
    let sql = format!(
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
        {where_clause}
        ORDER BY dlq_at DESC, job_id DESC
        LIMIT {limit}
        "#,
        schema = store.schema(),
        where_clause = predicate.where_clause(),
    );
    let rows: Vec<QueueStorageDlqRow> = predicate
        .bind_query_as(sqlx::query_as(audited_sql(sql)))
        .fetch_all(pool)
        .await?;

    rows.into_iter()
        .map(QueueStorageDlqRow::into_dlq_row)
        .collect()
}

pub async fn get_dlq_job(pool: &PgPool, job_id: i64) -> Result<Option<DlqRow>, AwaError> {
    let store = active_queue_storage(pool).await?;
    let schema = store.schema();
    let row: Option<QueueStorageDlqRow> = sqlx::query_as(audited_sql(format!(
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
    )))
    .bind(job_id)
    .fetch_optional(pool)
    .await?;

    row.map(QueueStorageDlqRow::into_dlq_row).transpose()
}

pub async fn dlq_depth(pool: &PgPool, queue: Option<&str>) -> Result<i64, AwaError> {
    let store = active_queue_storage(pool).await?;
    sqlx::query_scalar::<_, i64>(audited_sql(format!(
        "SELECT count(*)::bigint FROM {}.dlq_entries WHERE ($1::text IS NULL OR queue = $1)",
        store.schema()
    )))
    .bind(queue)
    .fetch_one(pool)
    .await
    .map_err(Into::into)
}

pub async fn dlq_depth_by_queue(pool: &PgPool) -> Result<Vec<(String, i64)>, AwaError> {
    let store = active_queue_storage(pool).await?;
    sqlx::query_as::<_, (String, i64)>(audited_sql(format!(
        r#"
        SELECT queue, count(*)::bigint
        FROM {}.dlq_entries
        GROUP BY queue
        ORDER BY count(*) DESC, queue ASC
        "#,
        store.schema()
    )))
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

/// Retry a single DLQ row back into live queue storage.
///
/// Atomic: deletes the DLQ row and inserts a fresh ready/deferred row in one
/// transaction. Resets `attempt = 0`, `run_lease = 0`, and clears any
/// per-attempt progress snapshot because a revived attempt starts from zero.
/// Error history is preserved in payload metadata for post-mortem visibility.
///
/// If the revived row conflicts with a live unique claim, this returns
/// [`AwaError::UniqueConflict`] and the DLQ row remains in place.
pub async fn retry_from_dlq(
    pool: &PgPool,
    job_id: i64,
    opts: &RetryFromDlqOpts,
) -> Result<Option<JobRow>, AwaError> {
    let store = active_queue_storage(pool).await?;
    store.retry_from_dlq(pool, job_id, opts).await
}

/// Refuse a bulk DLQ action whose filter would select the whole DLQ unless
/// the caller opted in. `states` and `dead_lettered` don't count: every DLQ
/// row is a dead-lettered failed job, so neither narrows it.
fn require_dlq_scope(filter: &JobFilter, allow_all: bool, action: &str) -> Result<(), AwaError> {
    let scoped = filter.kind.is_some()
        || filter.queue.is_some()
        || !filter.tags.is_empty()
        || filter.ids.as_ref().is_some_and(|ids| !ids.is_empty())
        || filter.created_at_gte.is_some()
        || filter.created_at_lt.is_some();
    if scoped || allow_all {
        return Ok(());
    }
    Err(AwaError::Validation(format!(
        "{action} needs a kind, queue, tag, ids or created_at bound (or allow_all=true)"
    )))
}

/// Bulk-retry DLQ rows matching the filter.
///
/// Requires a filter that narrows the DLQ unless `allow_all` is `true`. This
/// guard prevents an empty payload from reviving the entire DLQ by accident.
///
/// Like single-row retry, unique-claim conflicts abort the transaction and
/// leave the affected DLQ rows untouched.
pub async fn bulk_retry_from_dlq_matching(
    pool: &PgPool,
    filter: &JobFilter,
    allow_all: bool,
) -> Result<u64, AwaError> {
    filter.validate()?;
    require_dlq_scope(filter, allow_all, "bulk retry from the DLQ")?;
    let Some(predicate) = dlq_predicate(filter) else {
        return Ok(0);
    };
    let store = active_queue_storage(pool).await?;
    store.bulk_retry_from_dlq_where(pool, &predicate).await
}

#[deprecated(
    since = "0.7.0",
    note = "use `bulk_retry_from_dlq_matching` with a `JobFilter`"
)]
pub async fn bulk_retry_from_dlq(
    pool: &PgPool,
    filter: &ListDlqFilter,
    allow_all: bool,
) -> Result<u64, AwaError> {
    if !allow_all && filter.kind.is_none() && filter.queue.is_none() && filter.tag.is_none() {
        return Err(AwaError::Validation(
            "bulk_retry_from_dlq requires at least one of kind, queue, or tag (or allow_all=true)"
                .into(),
        ));
    }
    let (job_filter, page) = filter.split();
    let Some(mut predicate) = dlq_predicate(&job_filter) else {
        return Ok(0);
    };
    before_position(&mut predicate, page.before_dlq_at, page.before_id);
    let store = active_queue_storage(pool).await?;
    store.bulk_retry_from_dlq_where(pool, &predicate).await
}

/// Purge DLQ rows matching the filter.
///
/// Requires a filter that narrows the DLQ unless `allow_all` is `true`,
/// which is the explicit "yes, purge the whole DLQ" escape hatch for
/// operator tooling.
pub async fn purge_dlq_matching(
    pool: &PgPool,
    filter: &JobFilter,
    allow_all: bool,
) -> Result<u64, AwaError> {
    purge_dlq_matching_before(pool, filter, None, None, allow_all).await
}

/// [`purge_dlq_matching`], limited to rows before a `(dlq_at, id)` position
/// in `find_dlq`'s order: a page's last row, to purge everything older.
pub async fn purge_dlq_matching_before(
    pool: &PgPool,
    filter: &JobFilter,
    before_dlq_at: Option<DateTime<Utc>>,
    before_id: Option<i64>,
    allow_all: bool,
) -> Result<u64, AwaError> {
    filter.validate()?;
    require_dlq_scope(filter, allow_all, "purging the DLQ")?;
    let Some(mut predicate) = dlq_predicate(filter) else {
        return Ok(0);
    };
    before_position(&mut predicate, before_dlq_at, before_id);
    purge_where(pool, &predicate).await
}

#[deprecated(since = "0.7.0", note = "use `purge_dlq_matching` with a `JobFilter`")]
pub async fn purge_dlq(
    pool: &PgPool,
    filter: &ListDlqFilter,
    allow_all: bool,
) -> Result<u64, AwaError> {
    if !allow_all && filter.kind.is_none() && filter.queue.is_none() && filter.tag.is_none() {
        return Err(AwaError::Validation(
            "purge_dlq requires at least one of kind, queue, or tag (or allow_all=true)".into(),
        ));
    }
    let (job_filter, page) = filter.split();
    let Some(mut predicate) = dlq_predicate(&job_filter) else {
        return Ok(0);
    };
    before_position(&mut predicate, page.before_dlq_at, page.before_id);
    purge_where(pool, &predicate).await
}

async fn purge_where(pool: &PgPool, predicate: &Predicate) -> Result<u64, AwaError> {
    let store = active_queue_storage(pool).await?;
    let sql = format!(
        "DELETE FROM {}.dlq_entries {}",
        store.schema(),
        predicate.where_clause()
    );
    let result = predicate
        .bind_query(sqlx::query(audited_sql(sql)))
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

pub async fn purge_dlq_job(pool: &PgPool, job_id: i64) -> Result<bool, AwaError> {
    let store = active_queue_storage(pool).await?;
    let result = sqlx::query(audited_sql(format!(
        "DELETE FROM {}.dlq_entries WHERE job_id = $1",
        store.schema()
    )))
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Purge DLQ rows older than the configured retention horizon.
///
/// Deletes up to `batch_size` rows per call, ordered by oldest-first
/// `(dlq_at, job_id)`, optionally restricted to a single queue. Intended for
/// maintenance loops rather than interactive operator use.
pub async fn cleanup_dlq(
    pool: &PgPool,
    retention: Duration,
    batch_size: i64,
    queue: Option<&str>,
) -> Result<u64, AwaError> {
    let store = active_queue_storage(pool).await?;
    let retention_secs = retention.as_secs().min(i64::MAX as u64) as i64;
    let result = sqlx::query(audited_sql(format!(
        r#"
        DELETE FROM {}.dlq_entries
        WHERE job_id IN (
            SELECT job_id
            FROM {}.dlq_entries
            WHERE dlq_at < now() - make_interval(secs => $1::bigint)
              AND ($3::text IS NULL OR queue = $3)
            ORDER BY dlq_at ASC, job_id ASC
            LIMIT $2
        )
        "#,
        store.schema(),
        store.schema()
    )))
    .bind(retention_secs)
    .bind(batch_size)
    .bind(queue)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}
