//! Dead Letter Queue (DLQ) operations.
//!
//! Permanently-failed jobs are kept separate from the hot path so they do not
//! bloat the claim-path indexes or maintenance workload. The DLQ lives in
//! `awa.jobs_dlq`; it is never claimed by dispatchers.
//!
//! Moving a job to the DLQ is always atomic and lease-guarded (for terminal
//! failure paths) or state-guarded (for admin-initiated bulk moves) — see the
//! two SQL helpers `awa.move_to_dlq_guarded` and `awa.move_failed_to_dlq`
//! defined in migration v008.

use crate::error::AwaError;
use crate::job::{JobRow, JobState};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;
use sqlx::PgExecutor;

/// A row from `awa.jobs_dlq` — identical to `JobRow` plus DLQ-specific metadata.
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DlqRow {
    pub id: i64,
    pub kind: String,
    pub queue: String,
    pub args: serde_json::Value,
    pub state: JobState,
    pub priority: i16,
    pub attempt: i16,
    pub run_lease: i64,
    pub max_attempts: i16,
    pub run_at: DateTime<Utc>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub deadline_at: Option<DateTime<Utc>>,
    pub attempted_at: Option<DateTime<Utc>>,
    pub finalized_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub errors: Option<Vec<serde_json::Value>>,
    pub metadata: serde_json::Value,
    pub tags: Vec<String>,
    pub unique_key: Option<Vec<u8>>,
    /// Unique states bitmask — stored as BIT(8) in Postgres.
    /// Skipped in FromRow since it's only used by the DB-side unique index.
    #[sqlx(skip)]
    pub unique_states: Option<u8>,
    pub callback_id: Option<uuid::Uuid>,
    pub callback_timeout_at: Option<DateTime<Utc>>,
    pub callback_filter: Option<String>,
    pub callback_on_complete: Option<String>,
    pub callback_on_fail: Option<String>,
    pub callback_transform: Option<String>,
    pub progress: Option<serde_json::Value>,
    /// Why the job was moved to the DLQ (short operator-facing summary).
    pub dlq_reason: String,
    /// When the job entered the DLQ.
    pub dlq_at: DateTime<Utc>,
    /// Run lease of the terminal attempt at DLQ-entry time.
    pub original_run_lease: i64,
}

/// DLQ-specific metadata attached to a job when surfaced through admin APIs.
///
/// Separates the columns unique to `jobs_dlq` (reason, dlq_at, original_run_lease)
/// from the job row fields so admin views can present a DLQ'd job alongside a
/// live one without introducing a union type at every callsite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqMetadata {
    pub reason: String,
    pub dlq_at: DateTime<Utc>,
    pub original_run_lease: i64,
}

impl DlqRow {
    /// Split a DLQ row into its underlying JobRow and DLQ-specific metadata.
    ///
    /// Used by admin tools that want to present DLQ'd jobs through the same
    /// interfaces as live jobs while still exposing the DLQ metadata.
    pub fn into_parts(self) -> (JobRow, DlqMetadata) {
        let meta = DlqMetadata {
            reason: self.dlq_reason,
            dlq_at: self.dlq_at,
            original_run_lease: self.original_run_lease,
        };
        let job = JobRow {
            id: self.id,
            kind: self.kind,
            queue: self.queue,
            args: self.args,
            state: self.state,
            priority: self.priority,
            attempt: self.attempt,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            run_at: self.run_at,
            heartbeat_at: self.heartbeat_at,
            deadline_at: self.deadline_at,
            attempted_at: self.attempted_at,
            finalized_at: self.finalized_at,
            created_at: self.created_at,
            errors: self.errors,
            metadata: self.metadata,
            tags: self.tags,
            unique_key: self.unique_key,
            unique_states: self.unique_states,
            callback_id: self.callback_id,
            callback_timeout_at: self.callback_timeout_at,
            callback_filter: self.callback_filter,
            callback_on_complete: self.callback_on_complete,
            callback_on_fail: self.callback_on_fail,
            callback_transform: self.callback_transform,
            progress: self.progress,
        };
        (job, meta)
    }
}

/// Filter for listing DLQ rows.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListDlqFilter {
    pub kind: Option<String>,
    pub queue: Option<String>,
    pub tag: Option<String>,
    /// Pagination: only return rows with id < before_id. Results ordered by dlq_at DESC, id DESC.
    pub before_id: Option<i64>,
    pub limit: Option<i64>,
}

/// Move an already-failed job (living in `jobs_hot`) into the DLQ.
///
/// Use this for bulk admin moves where the caller does not own the run_lease
/// (e.g., moving historical failures). Guarded by `state = 'failed'` to avoid
/// racing with rescue/retry. Returns `None` if the job isn't in `failed` state
/// or doesn't exist.
pub async fn move_failed_to_dlq<'e, E>(
    executor: E,
    job_id: i64,
    reason: &str,
) -> Result<Option<DlqRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, DlqRow>("SELECT * FROM awa.move_failed_to_dlq($1, $2)")
        .bind(job_id)
        .bind(reason)
        .fetch_optional(executor)
        .await?;
    Ok(row)
}

/// Bulk-move all failed jobs matching the filter into the DLQ.
///
/// Returns the number of rows moved. Either `kind` or `queue` must be
/// specified.
///
/// Preserves the source row's `progress` column so operator-initiated moves
/// don't silently drop handler checkpoints.
pub async fn bulk_move_failed_to_dlq<'e, E>(
    executor: E,
    kind: Option<&str>,
    queue: Option<&str>,
    reason: &str,
) -> Result<u64, AwaError>
where
    E: PgExecutor<'e>,
{
    if kind.is_none() && queue.is_none() {
        return Err(AwaError::Validation(
            "bulk_move_failed_to_dlq requires at least one of kind or queue".into(),
        ));
    }
    let res = sqlx::query(
        r#"
        WITH moved AS (
            DELETE FROM awa.jobs_hot
            WHERE state = 'failed'
              AND ($1::text IS NULL OR kind = $1)
              AND ($2::text IS NULL OR queue = $2)
            RETURNING *
        )
        INSERT INTO awa.jobs_dlq (
            id, kind, queue, args, state, priority, attempt, max_attempts,
            run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
            created_at, errors, metadata, tags, unique_key, unique_states,
            callback_id, callback_timeout_at, callback_filter, callback_on_complete,
            callback_on_fail, callback_transform, run_lease, progress,
            dlq_reason, dlq_at, original_run_lease
        )
        SELECT
            id, kind, queue, args, 'failed'::awa.job_state, priority, attempt,
            max_attempts, run_at, NULL, NULL, attempted_at, COALESCE(finalized_at, now()),
            created_at, errors, metadata, tags, unique_key, unique_states,
            NULL, NULL, NULL, NULL, NULL, NULL, 0, progress,
            $3, now(), run_lease
        FROM moved
        "#,
    )
    .bind(kind)
    .bind(queue)
    .bind(reason)
    .execute(executor)
    .await?;
    Ok(res.rows_affected())
}

/// List DLQ rows matching the filter.
///
/// Sorted by `dlq_at DESC, id DESC` so the most recently DLQ'd jobs come first.
pub async fn list_dlq<'e, E>(executor: E, filter: &ListDlqFilter) -> Result<Vec<DlqRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let limit = filter.limit.unwrap_or(100);
    let rows = sqlx::query_as::<_, DlqRow>(
        r#"
        SELECT * FROM awa.jobs_dlq
        WHERE ($1::text IS NULL OR kind = $1)
          AND ($2::text IS NULL OR queue = $2)
          AND ($3::text IS NULL OR tags @> ARRAY[$3]::text[])
          AND ($4::bigint IS NULL OR id < $4)
        ORDER BY dlq_at DESC, id DESC
        LIMIT $5
        "#,
    )
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .bind(filter.before_id)
    .bind(limit)
    .fetch_all(executor)
    .await?;
    Ok(rows)
}

/// Get a single DLQ row by id.
pub async fn get_dlq_job<'e, E>(executor: E, job_id: i64) -> Result<Option<DlqRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, DlqRow>("SELECT * FROM awa.jobs_dlq WHERE id = $1")
        .bind(job_id)
        .fetch_optional(executor)
        .await?;
    Ok(row)
}

/// Count DLQ rows, optionally filtered by queue.
pub async fn dlq_depth<'e, E>(executor: E, queue: Option<&str>) -> Result<i64, AwaError>
where
    E: PgExecutor<'e>,
{
    let count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)::bigint
        FROM awa.jobs_dlq
        WHERE ($1::text IS NULL OR queue = $1)
        "#,
    )
    .bind(queue)
    .fetch_one(executor)
    .await?;
    Ok(count)
}

/// Count DLQ rows grouped by queue.
pub async fn dlq_depth_by_queue<'e, E>(executor: E) -> Result<Vec<(String, i64)>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"
        SELECT queue, count(*)::bigint
        FROM awa.jobs_dlq
        GROUP BY queue
        ORDER BY count(*) DESC
        "#,
    )
    .fetch_all(executor)
    .await?;
    Ok(rows)
}

/// Options for retrying a DLQ'd job.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetryFromDlqOpts {
    /// If set, the job is re-enqueued at this time (scheduled) instead of immediately.
    pub run_at: Option<DateTime<Utc>>,
    /// Optional priority override (1..=4).
    pub priority: Option<i16>,
    /// Optional queue override (rare — usually kept the same).
    pub queue: Option<String>,
}

/// Retry a single DLQ'd job by id.
///
/// Atomic: deletes from `jobs_dlq` and inserts a fresh row into `jobs_hot`
/// (or `scheduled_jobs` if `run_at` is in the future). Resets `attempt = 0`,
/// `run_lease = 0`, clears callback/heartbeat fields, discards any handler
/// progress snapshot (since the attempt counter restarts at zero), and
/// preserves the full error history for post-mortem visibility.
///
/// If the DLQ'd row carries a `unique_key` and a live job with the same key
/// already occupies `jobs_hot` / `scheduled_jobs`, the retry will fail with
/// [`AwaError::UniqueConflict`] — the row stays in the DLQ so the operator
/// can purge it or resolve the live job first.
///
/// Returns the revived `JobRow`, or `None` if the DLQ row was already removed.
pub async fn retry_from_dlq<'e, E>(
    executor: E,
    job_id: i64,
    opts: &RetryFromDlqOpts,
) -> Result<Option<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let scheduled = opts.run_at.map(|t| t > Utc::now()).unwrap_or(false);

    // The INSERT side fires the unique-claim trigger on `jobs_hot` /
    // `scheduled_jobs`, which can collide with a replacement job submitted
    // while this row was in the DLQ. Map SQLSTATE 23505 to
    // `UniqueConflict` so callers get a structured error instead of a raw
    // trigger-level constraint violation. The DLQ row stays put because
    // the enclosing CTE rolls back on error.
    if scheduled {
        sqlx::query_as::<_, JobRow>(
            r#"
            WITH moved AS (
                DELETE FROM awa.jobs_dlq WHERE id = $1 RETURNING *
            )
            INSERT INTO awa.scheduled_jobs (
                id, kind, queue, args, state, priority, attempt, max_attempts,
                run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
                created_at, errors, metadata, tags, unique_key, unique_states,
                callback_id, callback_timeout_at, callback_filter, callback_on_complete,
                callback_on_fail, callback_transform, run_lease, progress
            )
            SELECT
                id, kind, COALESCE($4, queue), args,
                'scheduled'::awa.job_state,
                COALESCE($3, priority),
                0,
                max_attempts,
                $2,
                NULL, NULL, NULL, NULL,
                created_at, errors, metadata, tags, unique_key, unique_states,
                NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL
            FROM moved
            RETURNING *
            "#,
        )
        .bind(job_id)
        .bind(opts.run_at)
        .bind(opts.priority)
        .bind(opts.queue.as_deref())
        .fetch_optional(executor)
        .await
        .map_err(crate::insert::map_sqlx_error)
    } else {
        sqlx::query_as::<_, JobRow>(
            r#"
            WITH moved AS (
                DELETE FROM awa.jobs_dlq WHERE id = $1 RETURNING *
            )
            INSERT INTO awa.jobs_hot (
                id, kind, queue, args, state, priority, attempt, max_attempts,
                run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
                created_at, errors, metadata, tags, unique_key, unique_states,
                callback_id, callback_timeout_at, callback_filter, callback_on_complete,
                callback_on_fail, callback_transform, run_lease, progress
            )
            SELECT
                id, kind, COALESCE($3, queue), args,
                'available'::awa.job_state,
                COALESCE($2, priority),
                0,
                max_attempts,
                now(),
                NULL, NULL, NULL, NULL,
                created_at, errors, metadata, tags, unique_key, unique_states,
                NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL
            FROM moved
            RETURNING *
            "#,
        )
        .bind(job_id)
        .bind(opts.priority)
        .bind(opts.queue.as_deref())
        .fetch_optional(executor)
        .await
        .map_err(crate::insert::map_sqlx_error)
    }
}

/// Bulk-retry DLQ rows matching a filter. Returns the count of revived jobs.
///
/// Requires at least one of `kind`, `queue`, or `tag` to be set unless
/// `allow_all` is `true`. This guard prevents an empty payload from
/// accidentally reviving the entire DLQ — a thundering-herd hazard that was
/// easy to trigger through the UI or CLI before the guard existed.
pub async fn bulk_retry_from_dlq<'e, E>(
    executor: E,
    filter: &ListDlqFilter,
    allow_all: bool,
) -> Result<u64, AwaError>
where
    E: PgExecutor<'e>,
{
    if !allow_all && filter.kind.is_none() && filter.queue.is_none() && filter.tag.is_none() {
        return Err(AwaError::Validation(
            "bulk_retry_from_dlq requires at least one of kind, queue, or tag (or allow_all=true)"
                .into(),
        ));
    }
    let res = sqlx::query(
        r#"
        WITH moved AS (
            DELETE FROM awa.jobs_dlq
            WHERE ($1::text IS NULL OR kind = $1)
              AND ($2::text IS NULL OR queue = $2)
              AND ($3::text IS NULL OR tags @> ARRAY[$3]::text[])
            RETURNING *
        )
        INSERT INTO awa.jobs_hot (
            id, kind, queue, args, state, priority, attempt, max_attempts,
            run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
            created_at, errors, metadata, tags, unique_key, unique_states,
            callback_id, callback_timeout_at, callback_filter, callback_on_complete,
            callback_on_fail, callback_transform, run_lease, progress
        )
        SELECT
            id, kind, queue, args, 'available'::awa.job_state, priority, 0,
            max_attempts, now(), NULL, NULL, NULL, NULL,
            created_at, errors, metadata, tags, unique_key, unique_states,
            NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL
        FROM moved
        "#,
    )
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .execute(executor)
    .await?;
    Ok(res.rows_affected())
}

/// Purge (delete) DLQ rows matching a filter. Returns the count of rows deleted.
///
/// Requires at least one of `kind`, `queue`, or `tag` unless `allow_all` is
/// `true`. The explicit opt-in avoids accidentally wiping the DLQ via an
/// empty filter from the UI or CLI.
pub async fn purge_dlq<'e, E>(
    executor: E,
    filter: &ListDlqFilter,
    allow_all: bool,
) -> Result<u64, AwaError>
where
    E: PgExecutor<'e>,
{
    if !allow_all && filter.kind.is_none() && filter.queue.is_none() && filter.tag.is_none() {
        return Err(AwaError::Validation(
            "purge_dlq requires at least one of kind, queue, or tag (or allow_all=true)".into(),
        ));
    }
    let res = sqlx::query(
        r#"
        DELETE FROM awa.jobs_dlq
        WHERE ($1::text IS NULL OR kind = $1)
          AND ($2::text IS NULL OR queue = $2)
          AND ($3::text IS NULL OR tags @> ARRAY[$3]::text[])
        "#,
    )
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .execute(executor)
    .await?;
    Ok(res.rows_affected())
}

/// Delete a single DLQ row by id. Returns true if the row was deleted.
pub async fn purge_dlq_job<'e, E>(executor: E, job_id: i64) -> Result<bool, AwaError>
where
    E: PgExecutor<'e>,
{
    let res = sqlx::query("DELETE FROM awa.jobs_dlq WHERE id = $1")
        .bind(job_id)
        .execute(executor)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Delete DLQ rows older than the given retention duration.
///
/// Limited per-call via `batch_size` to avoid long-running transactions.
/// Returns the number of rows deleted. When `queue` is `Some`, only rows for
/// that queue are considered — used to apply per-queue retention overrides
/// without leaking global retention to queues with explicit policies.
pub async fn cleanup_dlq<'e, E>(
    executor: E,
    retention: std::time::Duration,
    batch_size: i64,
    queue: Option<&str>,
) -> Result<u64, AwaError>
where
    E: PgExecutor<'e>,
{
    let retention_secs: i64 = retention.as_secs().min(i64::MAX as u64) as i64;
    let res = sqlx::query(
        r#"
        DELETE FROM awa.jobs_dlq
        WHERE id IN (
            SELECT id FROM awa.jobs_dlq
            WHERE dlq_at < now() - make_interval(secs => $1::bigint)
              AND ($3::text IS NULL OR queue = $3)
            LIMIT $2
        )
        "#,
    )
    .bind(retention_secs)
    .bind(batch_size)
    .bind(queue)
    .execute(executor)
    .await?;
    Ok(res.rows_affected())
}
