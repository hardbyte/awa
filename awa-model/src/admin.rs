use crate::error::AwaError;
use crate::job::{JobRow, JobState};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::PgExecutor;
use sqlx::PgPool;
use std::cmp::max;
use std::collections::HashMap;
use uuid::Uuid;

/// Retry a single failed, cancelled, or waiting_external job.
pub async fn retry<'e, E>(executor: E, job_id: i64) -> Result<Option<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'available', attempt = 0, run_at = now(),
            finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL,
            callback_id = NULL, callback_timeout_at = NULL,
            callback_filter = NULL, callback_on_complete = NULL,
            callback_on_fail = NULL, callback_transform = NULL
        WHERE id = $1 AND state IN ('failed', 'cancelled', 'waiting_external')
        RETURNING *
        "#,
    )
    .bind(job_id)
    .fetch_optional(executor)
    .await?
    .ok_or(AwaError::JobNotFound { id: job_id })
    .map(Some)
}

/// Cancel a single non-terminal job.
pub async fn cancel<'e, E>(executor: E, job_id: i64) -> Result<Option<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'cancelled', finalized_at = now(),
            callback_id = NULL, callback_timeout_at = NULL,
            callback_filter = NULL, callback_on_complete = NULL,
            callback_on_fail = NULL, callback_transform = NULL
        WHERE id = $1 AND state NOT IN ('completed', 'failed', 'cancelled')
        RETURNING *
        "#,
    )
    .bind(job_id)
    .fetch_optional(executor)
    .await?
    .ok_or(AwaError::JobNotFound { id: job_id })
    .map(Some)
}

/// Cancel a job by its unique key components.
///
/// Reconstructs the BLAKE3 unique key from the same inputs used at insert time
/// (kind, optional queue, optional args, optional period bucket), then cancels
/// the single oldest matching non-terminal job. Returns `None` if no matching
/// job was found (already completed, already cancelled, or never existed).
///
/// The parameters must match what was used at insert time: pass `queue` only if
/// the original `UniqueOpts` had `by_queue: true`, `args` only if `by_args: true`,
/// and `period_bucket` only if `by_period` was set. Mismatched components produce
/// a different hash and the job won't be found.
///
/// Only one job is cancelled per call (the oldest by `id`). This is intentional:
/// unique key enforcement uses a state bitmask, so multiple rows with the same
/// key can legally coexist (e.g., one `waiting_external` + one `available`).
/// Cancelling all of them in one shot would be surprising.
///
/// This is useful when the caller knows the job kind and args but not the job ID —
/// e.g., cancelling a scheduled reminder when the triggering condition is resolved.
///
/// # Implementation notes
///
/// Queries `jobs_hot` and `scheduled_jobs` directly rather than the `awa.jobs`
/// UNION ALL view, because PostgreSQL does not support `FOR UPDATE` on UNION
/// views. The CTE selects candidate IDs without row locks; blocking on a
/// concurrently-locked row (e.g., one being processed by a worker) happens
/// implicitly during the UPDATE phase via the writable view trigger. If the
/// worker completes the job before the UPDATE acquires the lock, the state
/// check (`NOT IN ('completed', 'failed', 'cancelled')`) causes the cancel
/// to no-op and return `None`.
///
/// The lookup scans `unique_key` on both physical tables without a dedicated
/// index. This is acceptable for low-volume use cases. For high-volume tables,
/// consider adding a partial index on `unique_key WHERE unique_key IS NOT NULL`
/// or routing through `job_unique_claims` (which is already indexed).
pub async fn cancel_by_unique_key<'e, E>(
    executor: E,
    kind: &str,
    queue: Option<&str>,
    args: Option<&serde_json::Value>,
    period_bucket: Option<i64>,
) -> Result<Option<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let unique_key = crate::unique::compute_unique_key(kind, queue, args, period_bucket);

    // Find the oldest matching job across both physical tables. CTE selects
    // candidate IDs without row locks; blocking on concurrently-locked rows
    // happens implicitly during the UPDATE via the writable view trigger.
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        WITH candidates AS (
            SELECT id FROM awa.jobs_hot
            WHERE unique_key = $1 AND state NOT IN ('completed', 'failed', 'cancelled')
            UNION ALL
            SELECT id FROM awa.scheduled_jobs
            WHERE unique_key = $1 AND state NOT IN ('completed', 'failed', 'cancelled')
            ORDER BY id ASC
            LIMIT 1
        )
        UPDATE awa.jobs
        SET state = 'cancelled', finalized_at = now(),
            callback_id = NULL, callback_timeout_at = NULL,
            callback_filter = NULL, callback_on_complete = NULL,
            callback_on_fail = NULL, callback_transform = NULL
        FROM candidates
        WHERE awa.jobs.id = candidates.id
        RETURNING awa.jobs.*
        "#,
    )
    .bind(&unique_key)
    .fetch_optional(executor)
    .await?;

    Ok(row)
}

/// Retry all failed jobs of a given kind.
pub async fn retry_failed_by_kind<'e, E>(executor: E, kind: &str) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'available', attempt = 0, run_at = now(),
            finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL
        WHERE kind = $1 AND state = 'failed'
        RETURNING *
        "#,
    )
    .bind(kind)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// Retry all failed jobs in a given queue.
pub async fn retry_failed_by_queue<'e, E>(executor: E, queue: &str) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'available', attempt = 0, run_at = now(),
            finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL
        WHERE queue = $1 AND state = 'failed'
        RETURNING *
        "#,
    )
    .bind(queue)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// Discard (delete) all failed jobs of a given kind.
pub async fn discard_failed<'e, E>(executor: E, kind: &str) -> Result<u64, AwaError>
where
    E: PgExecutor<'e>,
{
    let result = sqlx::query("DELETE FROM awa.jobs WHERE kind = $1 AND state = 'failed'")
        .bind(kind)
        .execute(executor)
        .await?;

    Ok(result.rows_affected())
}

/// Pause a queue. Affects all workers immediately.
pub async fn pause_queue<'e, E>(
    executor: E,
    queue: &str,
    paused_by: Option<&str>,
) -> Result<(), AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query(
        r#"
        INSERT INTO awa.queue_meta (queue, paused, paused_at, paused_by)
        VALUES ($1, TRUE, now(), $2)
        ON CONFLICT (queue) DO UPDATE SET paused = TRUE, paused_at = now(), paused_by = $2
        "#,
    )
    .bind(queue)
    .bind(paused_by)
    .execute(executor)
    .await?;

    Ok(())
}

/// Resume a paused queue.
pub async fn resume_queue<'e, E>(executor: E, queue: &str) -> Result<(), AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query("UPDATE awa.queue_meta SET paused = FALSE WHERE queue = $1")
        .bind(queue)
        .execute(executor)
        .await?;

    Ok(())
}

/// Drain a queue: cancel all non-running, non-terminal jobs.
pub async fn drain_queue<'e, E>(executor: E, queue: &str) -> Result<u64, AwaError>
where
    E: PgExecutor<'e>,
{
    let result = sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'cancelled', finalized_at = now(),
            callback_id = NULL, callback_timeout_at = NULL,
            callback_filter = NULL, callback_on_complete = NULL,
            callback_on_fail = NULL, callback_transform = NULL
        WHERE queue = $1 AND state IN ('available', 'scheduled', 'retryable', 'waiting_external')
        "#,
    )
    .bind(queue)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// Queue statistics.
#[derive(Debug, Clone, Serialize)]
pub struct QueueStats {
    pub queue: String,
    /// All non-terminal jobs for the queue, including running and waiting_external.
    pub total_queued: i64,
    pub scheduled: i64,
    pub available: i64,
    pub retryable: i64,
    pub running: i64,
    pub failed: i64,
    pub waiting_external: i64,
    pub completed_last_hour: i64,
    pub lag_seconds: Option<f64>,
    pub paused: bool,
}

/// Snapshot of a per-queue rate limit configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateLimitSnapshot {
    pub max_rate: f64,
    pub burst: u32,
}

/// Runtime concurrency mode for a queue.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum QueueRuntimeMode {
    HardReserved,
    Weighted,
}

/// Per-queue configuration published by a worker runtime instance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueueRuntimeConfigSnapshot {
    pub mode: QueueRuntimeMode,
    pub max_workers: Option<u32>,
    pub min_workers: Option<u32>,
    pub weight: Option<u32>,
    pub global_max_workers: Option<u32>,
    pub poll_interval_ms: u64,
    pub deadline_duration_secs: u64,
    pub priority_aging_interval_secs: u64,
    pub rate_limit: Option<RateLimitSnapshot>,
}

/// Runtime state for one queue on one worker instance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueueRuntimeSnapshot {
    pub queue: String,
    pub in_flight: u32,
    pub overflow_held: Option<u32>,
    pub config: QueueRuntimeConfigSnapshot,
}

/// Data written by a worker runtime into the observability snapshot table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSnapshotInput {
    pub instance_id: Uuid,
    pub hostname: Option<String>,
    pub pid: i32,
    pub version: String,
    pub started_at: DateTime<Utc>,
    pub snapshot_interval_ms: i64,
    pub healthy: bool,
    pub postgres_connected: bool,
    pub poll_loop_alive: bool,
    pub heartbeat_alive: bool,
    pub maintenance_alive: bool,
    pub shutting_down: bool,
    pub leader: bool,
    pub global_max_workers: Option<u32>,
    pub queues: Vec<QueueRuntimeSnapshot>,
}

/// A worker runtime instance as exposed through the admin API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeInstance {
    pub instance_id: Uuid,
    pub hostname: Option<String>,
    pub pid: i32,
    pub version: String,
    pub started_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub snapshot_interval_ms: i64,
    pub stale: bool,
    pub healthy: bool,
    pub postgres_connected: bool,
    pub poll_loop_alive: bool,
    pub heartbeat_alive: bool,
    pub maintenance_alive: bool,
    pub shutting_down: bool,
    pub leader: bool,
    pub global_max_workers: Option<u32>,
    pub queues: Vec<QueueRuntimeSnapshot>,
}

impl RuntimeInstance {
    fn stale_cutoff(interval_ms: i64) -> Duration {
        let interval_ms = max(interval_ms, 1_000);
        Duration::milliseconds(max(interval_ms.saturating_mul(3), 30_000))
    }

    fn from_db_row(row: RuntimeInstanceRow, now: DateTime<Utc>) -> Self {
        let stale = row.last_seen_at + Self::stale_cutoff(row.snapshot_interval_ms) < now;
        Self {
            instance_id: row.instance_id,
            hostname: row.hostname,
            pid: row.pid,
            version: row.version,
            started_at: row.started_at,
            last_seen_at: row.last_seen_at,
            snapshot_interval_ms: row.snapshot_interval_ms,
            stale,
            healthy: row.healthy,
            postgres_connected: row.postgres_connected,
            poll_loop_alive: row.poll_loop_alive,
            heartbeat_alive: row.heartbeat_alive,
            maintenance_alive: row.maintenance_alive,
            shutting_down: row.shutting_down,
            leader: row.leader,
            global_max_workers: row.global_max_workers.map(|v| v as u32),
            queues: row.queues.0,
        }
    }
}

/// Cluster-wide runtime overview.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeOverview {
    pub total_instances: usize,
    pub live_instances: usize,
    pub stale_instances: usize,
    pub healthy_instances: usize,
    pub leader_instances: usize,
    pub instances: Vec<RuntimeInstance>,
}

/// Queue-centric runtime/config summary aggregated across worker instances.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueRuntimeSummary {
    pub queue: String,
    pub instance_count: usize,
    pub live_instances: usize,
    pub stale_instances: usize,
    pub healthy_instances: usize,
    pub total_in_flight: u64,
    pub overflow_held_total: Option<u64>,
    pub config_mismatch: bool,
    pub config: Option<QueueRuntimeConfigSnapshot>,
}

#[derive(Debug, sqlx::FromRow)]
struct RuntimeInstanceRow {
    instance_id: Uuid,
    hostname: Option<String>,
    pid: i32,
    version: String,
    started_at: DateTime<Utc>,
    last_seen_at: DateTime<Utc>,
    snapshot_interval_ms: i64,
    healthy: bool,
    postgres_connected: bool,
    poll_loop_alive: bool,
    heartbeat_alive: bool,
    maintenance_alive: bool,
    shutting_down: bool,
    leader: bool,
    global_max_workers: Option<i32>,
    queues: Json<Vec<QueueRuntimeSnapshot>>,
}

/// Upsert a runtime observability snapshot for one worker instance.
pub async fn upsert_runtime_snapshot<'e, E>(
    executor: E,
    snapshot: &RuntimeSnapshotInput,
) -> Result<(), AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_instances (
            instance_id,
            hostname,
            pid,
            version,
            started_at,
            last_seen_at,
            snapshot_interval_ms,
            healthy,
            postgres_connected,
            poll_loop_alive,
            heartbeat_alive,
            maintenance_alive,
            shutting_down,
            leader,
            global_max_workers,
            queues
        )
        VALUES (
            $1, $2, $3, $4, $5, now(), $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
        )
        ON CONFLICT (instance_id) DO UPDATE SET
            hostname = EXCLUDED.hostname,
            pid = EXCLUDED.pid,
            version = EXCLUDED.version,
            started_at = EXCLUDED.started_at,
            last_seen_at = now(),
            snapshot_interval_ms = EXCLUDED.snapshot_interval_ms,
            healthy = EXCLUDED.healthy,
            postgres_connected = EXCLUDED.postgres_connected,
            poll_loop_alive = EXCLUDED.poll_loop_alive,
            heartbeat_alive = EXCLUDED.heartbeat_alive,
            maintenance_alive = EXCLUDED.maintenance_alive,
            shutting_down = EXCLUDED.shutting_down,
            leader = EXCLUDED.leader,
            global_max_workers = EXCLUDED.global_max_workers,
            queues = EXCLUDED.queues
        "#,
    )
    .bind(snapshot.instance_id)
    .bind(snapshot.hostname.as_deref())
    .bind(snapshot.pid)
    .bind(&snapshot.version)
    .bind(snapshot.started_at)
    .bind(snapshot.snapshot_interval_ms)
    .bind(snapshot.healthy)
    .bind(snapshot.postgres_connected)
    .bind(snapshot.poll_loop_alive)
    .bind(snapshot.heartbeat_alive)
    .bind(snapshot.maintenance_alive)
    .bind(snapshot.shutting_down)
    .bind(snapshot.leader)
    .bind(snapshot.global_max_workers.map(|v| v as i32))
    .bind(Json(&snapshot.queues))
    .execute(executor)
    .await?;

    Ok(())
}

/// Opportunistically delete long-stale runtime snapshot rows.
pub async fn cleanup_runtime_snapshots<'e, E>(
    executor: E,
    max_age: Duration,
) -> Result<u64, AwaError>
where
    E: PgExecutor<'e>,
{
    let seconds = max(max_age.num_seconds(), 1);
    let result = sqlx::query(
        "DELETE FROM awa.runtime_instances WHERE last_seen_at < now() - make_interval(secs => $1)",
    )
    .bind(seconds)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// List all runtime instances ordered with leader/live instances first.
pub async fn list_runtime_instances<'e, E>(executor: E) -> Result<Vec<RuntimeInstance>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_as::<_, RuntimeInstanceRow>(
        r#"
        SELECT
            instance_id,
            hostname,
            pid,
            version,
            started_at,
            last_seen_at,
            snapshot_interval_ms,
            healthy,
            postgres_connected,
            poll_loop_alive,
            heartbeat_alive,
            maintenance_alive,
            shutting_down,
            leader,
            global_max_workers,
            queues
        FROM awa.runtime_instances
        ORDER BY leader DESC, last_seen_at DESC, started_at DESC
        "#,
    )
    .fetch_all(executor)
    .await?;

    let now = Utc::now();
    Ok(rows
        .into_iter()
        .map(|row| RuntimeInstance::from_db_row(row, now))
        .collect())
}

/// Cluster runtime overview with instance list.
pub async fn runtime_overview<'e, E>(executor: E) -> Result<RuntimeOverview, AwaError>
where
    E: PgExecutor<'e>,
{
    let instances = list_runtime_instances(executor).await?;
    let total_instances = instances.len();
    let stale_instances = instances.iter().filter(|i| i.stale).count();
    let live_instances = total_instances.saturating_sub(stale_instances);
    let healthy_instances = instances.iter().filter(|i| !i.stale && i.healthy).count();
    let leader_instances = instances.iter().filter(|i| !i.stale && i.leader).count();

    Ok(RuntimeOverview {
        total_instances,
        live_instances,
        stale_instances,
        healthy_instances,
        leader_instances,
        instances,
    })
}

/// Queue runtime/config summary aggregated across worker snapshots.
pub async fn queue_runtime_summary<'e, E>(executor: E) -> Result<Vec<QueueRuntimeSummary>, AwaError>
where
    E: PgExecutor<'e>,
{
    let instances = list_runtime_instances(executor).await?;
    let mut by_queue: HashMap<String, Vec<(bool, bool, QueueRuntimeSnapshot)>> = HashMap::new();

    for instance in instances {
        let is_live = !instance.stale;
        let is_healthy = is_live && instance.healthy;
        for queue in instance.queues {
            by_queue
                .entry(queue.queue.clone())
                .or_default()
                .push((is_live, is_healthy, queue));
        }
    }

    let mut summaries: Vec<_> = by_queue
        .into_iter()
        .map(|(queue, entries)| {
            let instance_count = entries.len();
            let live_instances = entries.iter().filter(|(live, _, _)| *live).count();
            let stale_instances = instance_count.saturating_sub(live_instances);
            let healthy_instances = entries.iter().filter(|(_, healthy, _)| *healthy).count();
            let total_in_flight = entries
                .iter()
                .filter(|(live, _, _)| *live)
                .map(|(_, _, queue)| u64::from(queue.in_flight))
                .sum();

            let overflow_total: u64 = entries
                .iter()
                .filter(|(live, _, _)| *live)
                .filter_map(|(_, _, queue)| queue.overflow_held.map(u64::from))
                .sum();

            let live_configs: Vec<_> = entries
                .iter()
                .filter(|(live, _, _)| *live)
                .map(|(_, _, queue)| queue.config.clone())
                .collect();
            let config_candidates = if live_configs.is_empty() {
                entries
                    .iter()
                    .map(|(_, _, queue)| queue.config.clone())
                    .collect::<Vec<_>>()
            } else {
                live_configs
            };
            let config = config_candidates.first().cloned();
            let config_mismatch = config_candidates
                .iter()
                .skip(1)
                .any(|candidate| Some(candidate) != config.as_ref());

            QueueRuntimeSummary {
                queue,
                instance_count,
                live_instances,
                stale_instances,
                healthy_instances,
                total_in_flight,
                overflow_held_total: config
                    .as_ref()
                    .filter(|cfg| cfg.mode == QueueRuntimeMode::Weighted)
                    .map(|_| overflow_total),
                config_mismatch,
                config,
            }
        })
        .collect();

    summaries.sort_by(|a, b| a.queue.cmp(&b.queue));
    Ok(summaries)
}

/// Get statistics for all queues.
pub async fn queue_stats<'e, E>(executor: E) -> Result<Vec<QueueStats>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_as::<
        _,
        (
            String,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<f64>,
            bool,
        ),
    >(
        r#"
        WITH available_lag AS (
            SELECT
                queue,
                EXTRACT(EPOCH FROM (now() - min(run_at)))::float8 AS lag_seconds
            FROM awa.jobs_hot
            WHERE state = 'available'
            GROUP BY queue
        ),
        completed_recent AS (
            SELECT
                queue,
                count(*)::bigint AS completed_last_hour
            FROM awa.jobs_hot
            WHERE state = 'completed'
              AND finalized_at > now() - interval '1 hour'
            GROUP BY queue
        )
        SELECT
            qs.queue,
            qs.scheduled + qs.available + qs.running + qs.retryable + qs.waiting_external AS total_queued,
            qs.scheduled,
            qs.available,
            qs.retryable,
            qs.running,
            qs.failed,
            qs.waiting_external,
            COALESCE(cr.completed_last_hour, 0) AS completed_last_hour,
            al.lag_seconds,
            COALESCE(qm.paused, FALSE) AS paused
        FROM awa.queue_state_counts qs
        LEFT JOIN available_lag al ON al.queue = qs.queue
        LEFT JOIN completed_recent cr ON cr.queue = qs.queue
        LEFT JOIN awa.queue_meta qm ON qm.queue = qs.queue
        ORDER BY qs.queue
        "#,
    )
    .fetch_all(executor)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(
                queue,
                total_queued,
                scheduled,
                available,
                retryable,
                running,
                failed,
                waiting_external,
                completed_last_hour,
                lag_seconds,
                paused,
            )| QueueStats {
                queue,
                total_queued,
                scheduled,
                available,
                retryable,
                running,
                failed,
                waiting_external,
                completed_last_hour,
                lag_seconds,
                paused,
            },
        )
        .collect())
}

/// List jobs with optional filters.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ListJobsFilter {
    pub state: Option<JobState>,
    pub kind: Option<String>,
    pub queue: Option<String>,
    pub tag: Option<String>,
    pub before_id: Option<i64>,
    pub limit: Option<i64>,
}

/// List jobs matching the given filter.
pub async fn list_jobs<'e, E>(executor: E, filter: &ListJobsFilter) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let limit = filter.limit.unwrap_or(100);

    let rows = sqlx::query_as::<_, JobRow>(
        r#"
        SELECT * FROM awa.jobs
        WHERE ($1::awa.job_state IS NULL OR state = $1)
          AND ($2::text IS NULL OR kind = $2)
          AND ($3::text IS NULL OR queue = $3)
          AND ($4::text IS NULL OR tags @> ARRAY[$4]::text[])
          AND ($5::bigint IS NULL OR id < $5)
        ORDER BY id DESC
        LIMIT $6
        "#,
    )
    .bind(filter.state)
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .bind(filter.before_id)
    .bind(limit)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// Get a single job by ID.
pub async fn get_job<'e, E>(executor: E, job_id: i64) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, JobRow>("SELECT * FROM awa.jobs WHERE id = $1")
        .bind(job_id)
        .fetch_optional(executor)
        .await?;

    row.ok_or(AwaError::JobNotFound { id: job_id })
}

/// Count jobs grouped by state.
pub async fn state_counts<'e, E>(executor: E) -> Result<HashMap<JobState, i64>, AwaError>
where
    E: PgExecutor<'e>,
{
    // Single scan of queue_state_counts — sums all columns in one pass
    // then unpivots via VALUES join.
    let rows = sqlx::query_as::<_, (JobState, i64)>(
        r#"
        SELECT v.state, v.total FROM (
            SELECT
                COALESCE(sum(scheduled), 0)::bigint      AS scheduled,
                COALESCE(sum(available), 0)::bigint      AS available,
                COALESCE(sum(running), 0)::bigint        AS running,
                COALESCE(sum(completed), 0)::bigint      AS completed,
                COALESCE(sum(retryable), 0)::bigint      AS retryable,
                COALESCE(sum(failed), 0)::bigint         AS failed,
                COALESCE(sum(cancelled), 0)::bigint      AS cancelled,
                COALESCE(sum(waiting_external), 0)::bigint AS waiting_external
            FROM awa.queue_state_counts
        ) s,
        LATERAL (VALUES
            ('scheduled'::awa.job_state,        s.scheduled),
            ('available'::awa.job_state,        s.available),
            ('running'::awa.job_state,          s.running),
            ('completed'::awa.job_state,        s.completed),
            ('retryable'::awa.job_state,        s.retryable),
            ('failed'::awa.job_state,           s.failed),
            ('cancelled'::awa.job_state,        s.cancelled),
            ('waiting_external'::awa.job_state, s.waiting_external)
        ) AS v(state, total)
        "#,
    )
    .fetch_all(executor)
    .await?;

    Ok(rows.into_iter().collect())
}

/// Return all distinct job kinds.
pub async fn distinct_kinds<'e, E>(executor: E) -> Result<Vec<String>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT kind FROM awa.job_kind_catalog WHERE ref_count > 0 ORDER BY kind",
    )
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// Return all distinct queue names.
pub async fn distinct_queues<'e, E>(executor: E) -> Result<Vec<String>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT queue FROM awa.job_queue_catalog WHERE ref_count > 0 ORDER BY queue",
    )
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// Retry multiple jobs by ID. Only retries failed, cancelled, or waiting_external jobs.
pub async fn bulk_retry<'e, E>(executor: E, ids: &[i64]) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'available', attempt = 0, run_at = now(),
            finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL,
            callback_id = NULL, callback_timeout_at = NULL,
            callback_filter = NULL, callback_on_complete = NULL,
            callback_on_fail = NULL, callback_transform = NULL
        WHERE id = ANY($1) AND state IN ('failed', 'cancelled', 'waiting_external')
        RETURNING *
        "#,
    )
    .bind(ids)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// Cancel multiple jobs by ID. Only cancels non-terminal jobs.
pub async fn bulk_cancel<'e, E>(executor: E, ids: &[i64]) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'cancelled', finalized_at = now(),
            callback_id = NULL, callback_timeout_at = NULL,
            callback_filter = NULL, callback_on_complete = NULL,
            callback_on_fail = NULL, callback_transform = NULL
        WHERE id = ANY($1) AND state NOT IN ('completed', 'failed', 'cancelled')
        RETURNING *
        "#,
    )
    .bind(ids)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// A bucketed count of jobs by state over time.
#[derive(Debug, Clone, Serialize)]
pub struct StateTimeseriesBucket {
    pub bucket: chrono::DateTime<chrono::Utc>,
    pub state: JobState,
    pub count: i64,
}

/// Return time-bucketed state counts over the last N minutes.
pub async fn state_timeseries<'e, E>(
    executor: E,
    minutes: i32,
) -> Result<Vec<StateTimeseriesBucket>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>, JobState, i64)>(
        r#"
        SELECT
            date_trunc('minute', created_at) AS bucket,
            state,
            count(*) AS count
        FROM awa.jobs
        WHERE created_at >= now() - make_interval(mins => $1)
        GROUP BY bucket, state
        ORDER BY bucket
        "#,
    )
    .bind(minutes)
    .fetch_all(executor)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(bucket, state, count)| StateTimeseriesBucket {
            bucket,
            state,
            count,
        })
        .collect())
}

/// Register a callback for a running job, writing the callback_id and timeout
/// to the database immediately.
///
/// Call this BEFORE sending the callback_id to the external system to avoid
/// the race condition where the external system fires before the DB knows
/// about the callback.
///
/// Returns the generated callback UUID on success.
pub async fn register_callback<'e, E>(
    executor: E,
    job_id: i64,
    run_lease: i64,
    timeout: std::time::Duration,
) -> Result<Uuid, AwaError>
where
    E: PgExecutor<'e>,
{
    let callback_id = Uuid::new_v4();
    let timeout_secs = timeout.as_secs_f64();
    let result = sqlx::query(
        r#"UPDATE awa.jobs
           SET callback_id = $2,
               callback_timeout_at = now() + make_interval(secs => $3),
               callback_filter = NULL,
               callback_on_complete = NULL,
               callback_on_fail = NULL,
               callback_transform = NULL
           WHERE id = $1 AND state = 'running' AND run_lease = $4"#,
    )
    .bind(job_id)
    .bind(callback_id)
    .bind(timeout_secs)
    .bind(run_lease)
    .execute(executor)
    .await?;
    if result.rows_affected() == 0 {
        return Err(AwaError::Validation("job is not in running state".into()));
    }
    Ok(callback_id)
}

/// Complete a waiting job via external callback.
///
/// Accepts jobs in `waiting_external` or `running` state (race handling: the
/// external system may fire before the executor transitions to `waiting_external`).
///
/// When `resume` is `false` (default), the job transitions to `completed`.
/// When `resume` is `true`, the job transitions back to `running` with the
/// callback payload stored in metadata under `_awa_callback_result`. The
/// handler can then read the result and continue processing (sequential
/// callback pattern from ADR-016).
pub async fn complete_external<'e, E>(
    executor: E,
    callback_id: Uuid,
    payload: Option<serde_json::Value>,
    run_lease: Option<i64>,
) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    complete_external_inner(executor, callback_id, payload, run_lease, false).await
}

/// Complete a waiting job and resume the handler with the callback payload.
///
/// Like `complete_external`, but the job transitions to `running` instead of
/// `completed`, allowing the handler to continue with sequential callbacks.
/// The payload is stored in `metadata._awa_callback_result`.
pub async fn resume_external<'e, E>(
    executor: E,
    callback_id: Uuid,
    payload: Option<serde_json::Value>,
    run_lease: Option<i64>,
) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    complete_external_inner(executor, callback_id, payload, run_lease, true).await
}

async fn complete_external_inner<'e, E>(
    executor: E,
    callback_id: Uuid,
    payload: Option<serde_json::Value>,
    run_lease: Option<i64>,
    resume: bool,
) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = if resume {
        // Resume: transition to running, store payload, refresh heartbeat.
        // The handler is still alive and polling — it will detect the state change.
        let payload_json = payload.unwrap_or(serde_json::Value::Null);
        sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE awa.jobs
            SET state = 'running',
                callback_id = NULL,
                callback_timeout_at = NULL,
                callback_filter = NULL,
                callback_on_complete = NULL,
                callback_on_fail = NULL,
                callback_transform = NULL,
                heartbeat_at = now(),
                metadata = metadata || jsonb_build_object('_awa_callback_result', $3::jsonb)
            WHERE callback_id = $1 AND state IN ('waiting_external', 'running')
              AND ($2::bigint IS NULL OR run_lease = $2)
            RETURNING *
            "#,
        )
        .bind(callback_id)
        .bind(run_lease)
        .bind(&payload_json)
        .fetch_optional(executor)
        .await?
    } else {
        // Complete: terminal state, clear everything.
        sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE awa.jobs
            SET state = 'completed',
                finalized_at = now(),
                callback_id = NULL,
                callback_timeout_at = NULL,
                callback_filter = NULL,
                callback_on_complete = NULL,
                callback_on_fail = NULL,
                callback_transform = NULL,
                heartbeat_at = NULL,
                deadline_at = NULL,
                progress = NULL
            WHERE callback_id = $1 AND state IN ('waiting_external', 'running')
              AND ($2::bigint IS NULL OR run_lease = $2)
            RETURNING *
            "#,
        )
        .bind(callback_id)
        .bind(run_lease)
        .fetch_optional(executor)
        .await?
    };

    row.ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })
}

/// Fail a waiting job via external callback.
///
/// Records the error and transitions to `failed`.
pub async fn fail_external<'e, E>(
    executor: E,
    callback_id: Uuid,
    error: &str,
    run_lease: Option<i64>,
) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'failed',
            finalized_at = now(),
            callback_id = NULL,
            callback_timeout_at = NULL,
            callback_filter = NULL,
            callback_on_complete = NULL,
            callback_on_fail = NULL,
            callback_transform = NULL,
            heartbeat_at = NULL,
            deadline_at = NULL,
            errors = errors || jsonb_build_object(
                'error', $2::text,
                'attempt', attempt,
                'at', now()
            )::jsonb
        WHERE callback_id = $1 AND state IN ('waiting_external', 'running')
          AND ($3::bigint IS NULL OR run_lease = $3)
        RETURNING *
        "#,
    )
    .bind(callback_id)
    .bind(error)
    .bind(run_lease)
    .fetch_optional(executor)
    .await?;

    row.ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })
}

/// Retry a waiting job via external callback.
///
/// Resets to `available` with attempt = 0. The handler must be idempotent
/// with respect to the external call — a retry re-executes from scratch.
///
/// Only accepts `waiting_external` state — unlike complete/fail which are
/// terminal transitions, retry puts the job back to `available`. Allowing
/// retry from `running` would risk concurrent dispatch if the original
/// handler hasn't finished yet.
pub async fn retry_external<'e, E>(
    executor: E,
    callback_id: Uuid,
    run_lease: Option<i64>,
) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'available',
            attempt = 0,
            run_at = now(),
            finalized_at = NULL,
            callback_id = NULL,
            callback_timeout_at = NULL,
            callback_filter = NULL,
            callback_on_complete = NULL,
            callback_on_fail = NULL,
            callback_transform = NULL,
            heartbeat_at = NULL,
            deadline_at = NULL
        WHERE callback_id = $1 AND state = 'waiting_external'
          AND ($2::bigint IS NULL OR run_lease = $2)
        RETURNING *
        "#,
    )
    .bind(callback_id)
    .bind(run_lease)
    .fetch_optional(executor)
    .await?;

    row.ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })
}

/// Reset the callback timeout for a long-running external operation.
///
/// External systems call this periodically to signal "still working" without
/// completing the job. Resets `callback_timeout_at` to `now() + timeout`.
/// The job stays in `waiting_external`.
///
/// Returns the updated job row, or `CallbackNotFound` if the callback ID
/// doesn't match a waiting job.
pub async fn heartbeat_callback<'e, E>(
    executor: E,
    callback_id: Uuid,
    timeout: std::time::Duration,
) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    let timeout_secs = timeout.as_secs_f64();
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET callback_timeout_at = now() + make_interval(secs => $2)
        WHERE callback_id = $1 AND state = 'waiting_external'
        RETURNING *
        "#,
    )
    .bind(callback_id)
    .bind(timeout_secs)
    .fetch_optional(executor)
    .await?;

    row.ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })
}

/// Cancel (clear) a registered callback for a running job.
///
/// Best-effort cleanup: returns `Ok(true)` if a row was updated,
/// `Ok(false)` if no match (already resolved, rescued, or wrong lease).
/// Callers should not treat `false` as an error.
pub async fn cancel_callback<'e, E>(
    executor: E,
    job_id: i64,
    run_lease: i64,
) -> Result<bool, AwaError>
where
    E: PgExecutor<'e>,
{
    let result = sqlx::query(
        r#"
        UPDATE awa.jobs
        SET callback_id = NULL,
            callback_timeout_at = NULL,
            callback_filter = NULL,
            callback_on_complete = NULL,
            callback_on_fail = NULL,
            callback_transform = NULL
        WHERE id = $1 AND callback_id IS NOT NULL AND state = 'running' AND run_lease = $2
        "#,
    )
    .bind(job_id)
    .bind(run_lease)
    .execute(executor)
    .await?;

    Ok(result.rows_affected() > 0)
}

// ── Sequential callback wait helpers ─────────────────────────────────
//
// These functions extract the DB-interaction logic for `wait_for_callback`
// so that both the Rust `JobContext` and the Python bridge call the same
// code paths.

/// Result of a single poll iteration inside `wait_for_callback`.
#[derive(Debug)]
pub enum CallbackPollResult {
    /// The callback was resolved and the payload is ready.
    Resolved(serde_json::Value),
    /// Still waiting — caller should sleep and poll again.
    Pending,
    /// The callback token is stale (a different callback is current).
    Stale {
        token: Uuid,
        current: Uuid,
        state: JobState,
    },
    /// The job left the wait unexpectedly (rescued, cancelled, etc.).
    UnexpectedState { token: Uuid, state: JobState },
    /// The job was not found.
    NotFound,
}

/// Transition a running job to `waiting_external` for the given callback.
///
/// Returns `Ok(true)` if the transition succeeded, `Ok(false)` if the row
/// did not match (the caller should check for an early-resume race).
pub async fn enter_callback_wait(
    pool: &PgPool,
    job_id: i64,
    run_lease: i64,
    callback_id: Uuid,
) -> Result<bool, AwaError> {
    let result = sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'waiting_external',
            heartbeat_at = NULL,
            deadline_at = NULL
        WHERE id = $1 AND state = 'running' AND run_lease = $2 AND callback_id = $3
        "#,
    )
    .bind(job_id)
    .bind(run_lease)
    .bind(callback_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Check the current state of a job during callback wait.
///
/// Handles the early-resume race: if `resume_external` won the race before
/// the handler transitioned to `waiting_external`, the callback result is
/// already in metadata and this returns `Resolved`.
pub async fn check_callback_state(
    pool: &PgPool,
    job_id: i64,
    callback_id: Uuid,
) -> Result<CallbackPollResult, AwaError> {
    let row: Option<(JobState, Option<Uuid>, serde_json::Value)> =
        sqlx::query_as("SELECT state, callback_id, metadata FROM awa.jobs WHERE id = $1")
            .bind(job_id)
            .fetch_optional(pool)
            .await?;

    match row {
        Some((JobState::Running, None, metadata))
            if metadata.get("_awa_callback_result").is_some() =>
        {
            let payload = take_callback_payload(pool, job_id, metadata).await?;
            Ok(CallbackPollResult::Resolved(payload))
        }
        Some((state, Some(current_callback_id), _)) if current_callback_id != callback_id => {
            Ok(CallbackPollResult::Stale {
                token: callback_id,
                current: current_callback_id,
                state,
            })
        }
        Some((JobState::WaitingExternal, Some(current), _)) if current == callback_id => {
            Ok(CallbackPollResult::Pending)
        }
        Some((state, _, _)) => Ok(CallbackPollResult::UnexpectedState {
            token: callback_id,
            state,
        }),
        None => Ok(CallbackPollResult::NotFound),
    }
}

/// Extract the `_awa_callback_result` key from metadata and clean it up.
pub async fn take_callback_payload(
    pool: &PgPool,
    job_id: i64,
    metadata: serde_json::Value,
) -> Result<serde_json::Value, AwaError> {
    let payload = metadata
        .get("_awa_callback_result")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    sqlx::query("UPDATE awa.jobs SET metadata = metadata - '_awa_callback_result' WHERE id = $1")
        .bind(job_id)
        .execute(pool)
        .await?;

    Ok(payload)
}

// ── CEL callback expressions ──────────────────────────────────────────

/// Configuration for CEL callback expressions.
///
/// All fields are optional. When all are `None`, behaviour is identical to
/// the original `register_callback` (no expression evaluation).
#[derive(Debug, Clone, Default)]
pub struct CallbackConfig {
    /// Gate: should this payload be processed at all? Returns bool.
    pub filter: Option<String>,
    /// Does this payload indicate success? Returns bool.
    pub on_complete: Option<String>,
    /// Does this payload indicate failure? Returns bool. Evaluated before on_complete.
    pub on_fail: Option<String>,
    /// Reshape payload before returning to caller. Returns any Value.
    pub transform: Option<String>,
}

impl CallbackConfig {
    /// Returns true if no expressions are configured.
    pub fn is_empty(&self) -> bool {
        self.filter.is_none()
            && self.on_complete.is_none()
            && self.on_fail.is_none()
            && self.transform.is_none()
    }
}

/// What `resolve_callback` should do if no CEL conditions match or no
/// expressions are configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultAction {
    Complete,
    Fail,
    Ignore,
}

/// Outcome of `resolve_callback`.
#[derive(Debug)]
pub enum ResolveOutcome {
    Completed {
        payload: Option<serde_json::Value>,
        job: JobRow,
    },
    Failed {
        job: JobRow,
    },
    Ignored {
        reason: String,
    },
}

impl ResolveOutcome {
    pub fn is_completed(&self) -> bool {
        matches!(self, ResolveOutcome::Completed { .. })
    }
    pub fn is_failed(&self) -> bool {
        matches!(self, ResolveOutcome::Failed { .. })
    }
    pub fn is_ignored(&self) -> bool {
        matches!(self, ResolveOutcome::Ignored { .. })
    }
}

/// Register a callback with optional CEL expressions.
///
/// When expressions are provided and the `cel` feature is enabled, each
/// expression is trial-compiled at registration time so syntax errors are
/// caught early.
///
/// When the `cel` feature is disabled and any expression is non-None,
/// returns `AwaError::Validation`.
pub async fn register_callback_with_config<'e, E>(
    executor: E,
    job_id: i64,
    run_lease: i64,
    timeout: std::time::Duration,
    config: &CallbackConfig,
) -> Result<Uuid, AwaError>
where
    E: PgExecutor<'e>,
{
    // Validate CEL expressions at registration time: compile + check references
    #[cfg(feature = "cel")]
    {
        for (name, expr) in [
            ("filter", &config.filter),
            ("on_complete", &config.on_complete),
            ("on_fail", &config.on_fail),
            ("transform", &config.transform),
        ] {
            if let Some(src) = expr {
                let program = cel::Program::compile(src).map_err(|e| {
                    AwaError::Validation(format!("invalid CEL expression for {name}: {e}"))
                })?;

                // Reject undeclared variables — CEL only reports these at execution
                // time, so an expression like `missing == 1` would parse fine but
                // silently fall into the fail-open path at resolve time.
                let refs = program.references();
                let bad_vars: Vec<&str> = refs
                    .variables()
                    .into_iter()
                    .filter(|v| *v != "payload")
                    .collect();
                if !bad_vars.is_empty() {
                    return Err(AwaError::Validation(format!(
                        "CEL expression for {name} references undeclared variable(s): {}; \
                         only 'payload' is available",
                        bad_vars.join(", ")
                    )));
                }
            }
        }
    }

    #[cfg(not(feature = "cel"))]
    {
        if !config.is_empty() {
            return Err(AwaError::Validation(
                "CEL expressions require the 'cel' feature".into(),
            ));
        }
    }

    let callback_id = Uuid::new_v4();
    let timeout_secs = timeout.as_secs_f64();

    let result = sqlx::query(
        r#"UPDATE awa.jobs
           SET callback_id = $2,
               callback_timeout_at = now() + make_interval(secs => $3),
               callback_filter = $4,
               callback_on_complete = $5,
               callback_on_fail = $6,
               callback_transform = $7
           WHERE id = $1 AND state = 'running' AND run_lease = $8"#,
    )
    .bind(job_id)
    .bind(callback_id)
    .bind(timeout_secs)
    .bind(&config.filter)
    .bind(&config.on_complete)
    .bind(&config.on_fail)
    .bind(&config.transform)
    .bind(run_lease)
    .execute(executor)
    .await?;

    if result.rows_affected() == 0 {
        return Err(AwaError::Validation("job is not in running state".into()));
    }
    Ok(callback_id)
}

/// Internal action decided by CEL evaluation or default.
enum ResolveAction {
    Complete(Option<serde_json::Value>),
    Fail {
        error: String,
        expression: Option<String>,
    },
    Ignore(String),
}

/// Resolve a callback by evaluating CEL expressions against the payload.
///
/// Uses a transaction with `SELECT ... FOR UPDATE` for atomicity.
/// The `default_action` determines behaviour when no CEL conditions match
/// or no expressions are configured.
pub async fn resolve_callback(
    pool: &PgPool,
    callback_id: Uuid,
    payload: Option<serde_json::Value>,
    default_action: DefaultAction,
    run_lease: Option<i64>,
) -> Result<ResolveOutcome, AwaError> {
    let mut tx = pool.begin().await?;

    // Query jobs_hot directly (not the awa.jobs UNION ALL view) because
    // FOR UPDATE is not reliably supported on UNION views. Waiting_external
    // and running jobs are always in jobs_hot (the check constraint on
    // scheduled_jobs only allows scheduled/retryable).
    //
    // Accepts both 'waiting_external' and 'running' to handle the race where
    // a fast callback arrives before the executor transitions running ->
    // waiting_external (matching complete_external/fail_external behavior).
    let job = sqlx::query_as::<_, JobRow>(
        "SELECT * FROM awa.jobs_hot WHERE callback_id = $1
         AND state IN ('waiting_external', 'running')
         AND ($2::bigint IS NULL OR run_lease = $2)
         FOR UPDATE",
    )
    .bind(callback_id)
    .bind(run_lease)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })?;

    let action = evaluate_or_default(&job, &payload, default_action)?;

    match action {
        ResolveAction::Complete(transformed_payload) => {
            let completed_job = sqlx::query_as::<_, JobRow>(
                r#"
                UPDATE awa.jobs
                SET state = 'completed',
                    finalized_at = now(),
                    callback_id = NULL,
                    callback_timeout_at = NULL,
                    callback_filter = NULL,
                    callback_on_complete = NULL,
                    callback_on_fail = NULL,
                    callback_transform = NULL,
                    heartbeat_at = NULL,
                    deadline_at = NULL,
                    progress = NULL
                WHERE id = $1
                RETURNING *
                "#,
            )
            .bind(job.id)
            .fetch_one(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(ResolveOutcome::Completed {
                payload: transformed_payload,
                job: completed_job,
            })
        }
        ResolveAction::Fail { error, expression } => {
            let mut error_json = serde_json::json!({
                "error": error,
                "attempt": job.attempt,
                "at": chrono::Utc::now().to_rfc3339(),
            });
            if let Some(expr) = expression {
                error_json["expression"] = serde_json::Value::String(expr);
            }

            let failed_job = sqlx::query_as::<_, JobRow>(
                r#"
                UPDATE awa.jobs
                SET state = 'failed',
                    finalized_at = now(),
                    callback_id = NULL,
                    callback_timeout_at = NULL,
                    callback_filter = NULL,
                    callback_on_complete = NULL,
                    callback_on_fail = NULL,
                    callback_transform = NULL,
                    heartbeat_at = NULL,
                    deadline_at = NULL,
                    errors = errors || $2::jsonb
                WHERE id = $1
                RETURNING *
                "#,
            )
            .bind(job.id)
            .bind(error_json)
            .fetch_one(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(ResolveOutcome::Failed { job: failed_job })
        }
        ResolveAction::Ignore(reason) => {
            // No state change — dropping tx releases FOR UPDATE lock
            Ok(ResolveOutcome::Ignored { reason })
        }
    }
}

/// Evaluate CEL expressions or fall through to default_action.
fn evaluate_or_default(
    job: &JobRow,
    payload: &Option<serde_json::Value>,
    default_action: DefaultAction,
) -> Result<ResolveAction, AwaError> {
    let has_expressions = job.callback_filter.is_some()
        || job.callback_on_complete.is_some()
        || job.callback_on_fail.is_some()
        || job.callback_transform.is_some();

    if !has_expressions {
        return Ok(apply_default(default_action, payload));
    }

    #[cfg(feature = "cel")]
    {
        Ok(evaluate_cel(job, payload, default_action))
    }

    #[cfg(not(feature = "cel"))]
    {
        // Expressions are present but CEL feature is not enabled.
        // Return an error without mutating the job — it stays in waiting_external.
        let _ = (payload, default_action);
        Err(AwaError::Validation(
            "CEL expressions present but 'cel' feature is not enabled".into(),
        ))
    }
}

fn apply_default(
    default_action: DefaultAction,
    payload: &Option<serde_json::Value>,
) -> ResolveAction {
    match default_action {
        DefaultAction::Complete => ResolveAction::Complete(payload.clone()),
        DefaultAction::Fail => ResolveAction::Fail {
            error: "callback failed: default action".to_string(),
            expression: None,
        },
        DefaultAction::Ignore => {
            ResolveAction::Ignore("no expressions configured, default is ignore".to_string())
        }
    }
}

#[cfg(feature = "cel")]
fn evaluate_cel(
    job: &JobRow,
    payload: &Option<serde_json::Value>,
    default_action: DefaultAction,
) -> ResolveAction {
    let payload_value = payload.as_ref().cloned().unwrap_or(serde_json::Value::Null);

    // 1. Evaluate filter
    if let Some(filter_expr) = &job.callback_filter {
        match eval_bool(filter_expr, &payload_value, job.id, "filter") {
            Ok(true) => {} // pass through
            Ok(false) => {
                return ResolveAction::Ignore("filter expression returned false".to_string());
            }
            Err(_) => {
                // Fail-open: treat filter error as true (pass through)
            }
        }
    }

    // 2. Evaluate on_fail (before on_complete — fail takes precedence)
    if let Some(on_fail_expr) = &job.callback_on_fail {
        match eval_bool(on_fail_expr, &payload_value, job.id, "on_fail") {
            Ok(true) => {
                return ResolveAction::Fail {
                    error: "callback failed: on_fail expression matched".to_string(),
                    expression: Some(on_fail_expr.clone()),
                };
            }
            Ok(false) => {} // don't fail
            Err(_) => {
                // Fail-open: treat on_fail error as false (don't fail)
            }
        }
    }

    // 3. Evaluate on_complete
    if let Some(on_complete_expr) = &job.callback_on_complete {
        match eval_bool(on_complete_expr, &payload_value, job.id, "on_complete") {
            Ok(true) => {
                // Complete with optional transform
                let transformed = apply_transform(job, &payload_value);
                return ResolveAction::Complete(Some(transformed));
            }
            Ok(false) => {} // don't complete
            Err(_) => {
                // Fail-open: treat on_complete error as false (don't complete)
            }
        }
    }

    // 4. Neither condition matched → apply default_action
    apply_default(default_action, payload)
}

#[cfg(feature = "cel")]
fn eval_bool(
    expression: &str,
    payload_value: &serde_json::Value,
    job_id: i64,
    expression_name: &str,
) -> Result<bool, ()> {
    let program = match cel::Program::compile(expression) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                job_id,
                expression_name,
                expression,
                error = %e,
                "CEL compilation error during evaluation"
            );
            return Err(());
        }
    };

    let mut context = cel::Context::default();
    if let Err(e) = context.add_variable("payload", payload_value.clone()) {
        tracing::warn!(
            job_id,
            expression_name,
            error = %e,
            "Failed to add payload variable to CEL context"
        );
        return Err(());
    }

    match program.execute(&context) {
        Ok(cel::Value::Bool(b)) => Ok(b),
        Ok(other) => {
            tracing::warn!(
                job_id,
                expression_name,
                expression,
                result_type = ?other.type_of(),
                "CEL expression returned non-bool"
            );
            Err(())
        }
        Err(e) => {
            tracing::warn!(
                job_id,
                expression_name,
                expression,
                error = %e,
                "CEL execution error"
            );
            Err(())
        }
    }
}

#[cfg(feature = "cel")]
fn apply_transform(job: &JobRow, payload_value: &serde_json::Value) -> serde_json::Value {
    let transform_expr = match &job.callback_transform {
        Some(expr) => expr,
        None => return payload_value.clone(),
    };

    let program = match cel::Program::compile(transform_expr) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                job_id = job.id,
                expression = transform_expr,
                error = %e,
                "CEL transform compilation error, using original payload"
            );
            return payload_value.clone();
        }
    };

    let mut context = cel::Context::default();
    if let Err(e) = context.add_variable("payload", payload_value.clone()) {
        tracing::warn!(
            job_id = job.id,
            error = %e,
            "Failed to add payload variable for transform"
        );
        return payload_value.clone();
    }

    match program.execute(&context) {
        Ok(value) => match value.json() {
            Ok(json) => json,
            Err(e) => {
                tracing::warn!(
                    job_id = job.id,
                    expression = transform_expr,
                    error = %e,
                    "CEL transform result could not be converted to JSON, using original payload"
                );
                payload_value.clone()
            }
        },
        Err(e) => {
            tracing::warn!(
                job_id = job.id,
                expression = transform_expr,
                error = %e,
                "CEL transform execution error, using original payload"
            );
            payload_value.clone()
        }
    }
}

// ---------------------------------------------------------------------------
// Tick: stateless, bounded maintenance for serverless deployments
// ---------------------------------------------------------------------------

/// Result of a single `tick()` invocation.
#[derive(Debug, Clone, Default, Serialize)]
pub struct TickResult {
    /// Number of scheduled jobs promoted to available.
    pub promoted_scheduled: u64,
    /// Number of retryable jobs promoted to available.
    pub promoted_retryable: u64,
    /// Number of jobs rescued from stale heartbeats.
    pub rescued_heartbeat: u64,
    /// Number of jobs rescued from expired deadlines.
    pub rescued_deadline: u64,
    /// Number of jobs rescued from expired callback timeouts.
    pub rescued_callback: u64,
    /// Number of terminal jobs cleaned up.
    pub cleaned_up: u64,
}

/// Default batch limit for each tick step.
const TICK_BATCH_LIMIT: i64 = 500;

/// Run a single bounded maintenance pass.
///
/// This is the stateless equivalent of what `MaintenanceService` does on a
/// timer-driven loop with leader election. It is designed for serverless or
/// low-traffic deployments where no persistent worker process is running.
///
/// Each step is bounded by `TICK_BATCH_LIMIT` so the function completes in
/// predictable time. Call it on a schedule (e.g., every 1–5 minutes from
/// Cloud Scheduler, pg_cron, or a framework background task).
///
/// Unlike the full maintenance service, `tick()` does NOT:
/// - Hold an advisory lock (leader election)
/// - Signal in-flight job cancellation (no local worker)
/// - Evaluate cron schedules (use the persistent worker for cron)
/// - Publish OTel metrics
///
/// It DOES:
/// 1. Promote scheduled → available (bounded batch)
/// 2. Promote retryable → available (bounded batch)
/// 3. Rescue stale heartbeats (bounded batch)
/// 4. Rescue expired deadlines (bounded batch)
/// 5. Rescue expired callback timeouts (bounded batch)
/// 6. Clean up completed/failed jobs past retention (bounded batch)
pub async fn tick(pool: &PgPool) -> Result<TickResult, AwaError> {
    tick_with_options(pool, TickOptions::default()).await
}

/// Options for customizing a `tick()` invocation.
#[derive(Debug, Clone)]
pub struct TickOptions {
    /// Maximum jobs to process per step (default: 500).
    pub batch_limit: i64,
    /// Heartbeat staleness threshold (default: 90s).
    pub heartbeat_staleness_secs: u64,
    /// Retention for completed jobs (default: 24h).
    pub completed_retention_secs: u64,
    /// Retention for failed/cancelled jobs (default: 72h).
    pub failed_retention_secs: u64,
}

impl Default for TickOptions {
    fn default() -> Self {
        Self {
            batch_limit: TICK_BATCH_LIMIT,
            heartbeat_staleness_secs: 90,
            completed_retention_secs: 86400,
            failed_retention_secs: 259200,
        }
    }
}

/// Run a single bounded maintenance pass with custom options.
pub async fn tick_with_options(pool: &PgPool, opts: TickOptions) -> Result<TickResult, AwaError> {
    let mut result = TickResult::default();

    // 1. Promote scheduled → available
    result.promoted_scheduled = tick_promote(pool, "scheduled", opts.batch_limit).await?;

    // 2. Promote retryable → available
    result.promoted_retryable = tick_promote(pool, "retryable", opts.batch_limit).await?;

    // 3. Rescue stale heartbeats
    result.rescued_heartbeat =
        tick_rescue_heartbeats(pool, opts.heartbeat_staleness_secs, opts.batch_limit).await?;

    // 4. Rescue expired deadlines
    result.rescued_deadline = tick_rescue_deadlines(pool, opts.batch_limit).await?;

    // 5. Rescue expired callback timeouts
    result.rescued_callback = tick_rescue_callbacks(pool, opts.batch_limit).await?;

    // 6. Cleanup terminal jobs
    result.cleaned_up = tick_cleanup(
        pool,
        opts.completed_retention_secs,
        opts.failed_retention_secs,
        opts.batch_limit,
    )
    .await?;

    Ok(result)
}

/// Promote due jobs of a given state (scheduled or retryable) to available.
///
/// Uses the same literal-state-injection pattern as `MaintenanceService` to
/// ensure the Postgres planner can use the partial index on `(run_at, id)`.
async fn tick_promote(pool: &PgPool, state: &str, limit: i64) -> Result<u64, AwaError> {
    // Validate state to prevent SQL injection (only two valid values)
    assert!(
        state == "scheduled" || state == "retryable",
        "tick_promote: invalid state"
    );

    let sql = format!(
        r#"
        WITH due AS (
            DELETE FROM awa.scheduled_jobs
            WHERE id IN (
                SELECT id
                FROM awa.scheduled_jobs
                WHERE state = '{state}'::awa.job_state
                  AND run_at <= now()
                ORDER BY run_at ASC, id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
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
            id, kind, queue, args, 'available'::awa.job_state, priority,
            attempt, max_attempts, now(), NULL, NULL, attempted_at, finalized_at,
            created_at, errors, metadata, tags, unique_key, unique_states,
            NULL, NULL, NULL, NULL, NULL, NULL, run_lease, progress
        FROM due
        "#
    );

    let result = sqlx::query(&sql).bind(limit).execute(pool).await?;
    Ok(result.rows_affected())
}

async fn tick_rescue_heartbeats(
    pool: &PgPool,
    staleness_secs: u64,
    limit: i64,
) -> Result<u64, AwaError> {
    let staleness = format!("{staleness_secs} seconds");
    let result = sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'retryable',
            finalized_at = now(),
            heartbeat_at = NULL,
            deadline_at = NULL,
            callback_id = NULL,
            callback_timeout_at = NULL,
            callback_filter = NULL,
            callback_on_complete = NULL,
            callback_on_fail = NULL,
            callback_transform = NULL,
            errors = errors || jsonb_build_object(
                'error', 'heartbeat stale: worker presumed dead (via tick)',
                'attempt', attempt,
                'at', now()
            )::jsonb
        WHERE id IN (
            SELECT id FROM awa.jobs
            WHERE state = 'running'
              AND heartbeat_at < now() - $1::interval
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        "#,
    )
    .bind(&staleness)
    .bind(limit)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

async fn tick_rescue_deadlines(pool: &PgPool, limit: i64) -> Result<u64, AwaError> {
    let result = sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'retryable',
            finalized_at = now(),
            heartbeat_at = NULL,
            deadline_at = NULL,
            callback_id = NULL,
            callback_timeout_at = NULL,
            callback_filter = NULL,
            callback_on_complete = NULL,
            callback_on_fail = NULL,
            callback_transform = NULL,
            errors = errors || jsonb_build_object(
                'error', 'hard deadline exceeded (via tick)',
                'attempt', attempt,
                'at', now()
            )::jsonb
        WHERE id IN (
            SELECT id FROM awa.jobs
            WHERE state = 'running'
              AND deadline_at IS NOT NULL
              AND deadline_at < now()
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        "#,
    )
    .bind(limit)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

async fn tick_rescue_callbacks(pool: &PgPool, limit: i64) -> Result<u64, AwaError> {
    let result = sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = CASE WHEN attempt >= max_attempts THEN 'failed'::awa.job_state ELSE 'retryable'::awa.job_state END,
            finalized_at = now(),
            callback_id = NULL,
            callback_timeout_at = NULL,
            callback_filter = NULL,
            callback_on_complete = NULL,
            callback_on_fail = NULL,
            callback_transform = NULL,
            run_at = CASE WHEN attempt >= max_attempts THEN run_at
                     ELSE now() + awa.backoff_duration(attempt, max_attempts) END,
            errors = errors || jsonb_build_object(
                'error', 'callback timed out (via tick)',
                'attempt', attempt,
                'at', now()
            )::jsonb
        WHERE id IN (
            SELECT id FROM awa.jobs
            WHERE state = 'waiting_external'
              AND callback_timeout_at IS NOT NULL
              AND callback_timeout_at < now()
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        "#,
    )
    .bind(limit)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

async fn tick_cleanup(
    pool: &PgPool,
    completed_retention_secs: u64,
    failed_retention_secs: u64,
    limit: i64,
) -> Result<u64, AwaError> {
    let completed_retention = format!("{completed_retention_secs} seconds");
    let failed_retention = format!("{failed_retention_secs} seconds");
    let result = sqlx::query(
        r#"
        DELETE FROM awa.jobs_hot
        WHERE id IN (
            SELECT id FROM awa.jobs_hot
            WHERE (state = 'completed' AND finalized_at < now() - $1::interval)
               OR (state IN ('failed', 'cancelled') AND finalized_at < now() - $2::interval)
            LIMIT $3
        )
        "#,
    )
    .bind(&completed_retention)
    .bind(&failed_retention)
    .bind(limit)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}
