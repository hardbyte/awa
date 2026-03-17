use crate::error::AwaError;
use crate::job::{JobRow, JobState};
use sqlx::PgExecutor;

/// Retry a single failed or cancelled job.
pub async fn retry<'e, E>(executor: E, job_id: i64) -> Result<Option<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'available', attempt = 0, run_at = now(),
            finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL
        WHERE id = $1 AND state IN ('failed', 'cancelled')
        RETURNING *
        "#,
    )
    .bind(job_id)
    .fetch_optional(executor)
    .await?;

    match row {
        Some(row) => Ok(Some(row)),
        None => Err(AwaError::JobNotFound { id: job_id }),
    }
}

/// Cancel a single non-terminal job.
pub async fn cancel<'e, E>(executor: E, job_id: i64) -> Result<Option<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'cancelled', finalized_at = now()
        WHERE id = $1 AND state NOT IN ('completed', 'failed', 'cancelled')
        RETURNING *
        "#,
    )
    .bind(job_id)
    .fetch_optional(executor)
    .await?;

    match row {
        Some(row) => Ok(Some(row)),
        None => Err(AwaError::JobNotFound { id: job_id }),
    }
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
        UPDATE awa.jobs SET state = 'cancelled', finalized_at = now()
        WHERE queue = $1 AND state IN ('available', 'scheduled', 'retryable')
        "#,
    )
    .bind(queue)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// Queue statistics.
#[derive(Debug, Clone)]
pub struct QueueStats {
    pub queue: String,
    pub available: i64,
    pub running: i64,
    pub failed: i64,
    pub completed_last_hour: i64,
    pub lag_seconds: Option<f64>,
}

/// Get statistics for all queues.
pub async fn queue_stats<'e, E>(executor: E) -> Result<Vec<QueueStats>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_as::<_, (String, i64, i64, i64, i64, Option<f64>)>(
        r#"
        SELECT
            queue,
            count(*) FILTER (WHERE state = 'available') AS available,
            count(*) FILTER (WHERE state = 'running') AS running,
            count(*) FILTER (WHERE state = 'failed') AS failed,
            count(*) FILTER (WHERE state = 'completed'
                AND finalized_at > now() - interval '1 hour') AS completed_last_hour,
            EXTRACT(EPOCH FROM (now() - min(run_at) FILTER (WHERE state = 'available')))::float8 AS lag_seconds
        FROM awa.jobs
        GROUP BY queue
        "#,
    )
    .fetch_all(executor)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(queue, available, running, failed, completed_last_hour, lag_seconds)| QueueStats {
                queue,
                available,
                running,
                failed,
                completed_last_hour,
                lag_seconds,
            },
        )
        .collect())
}

/// List jobs with optional filters.
#[derive(Debug, Clone, Default)]
pub struct ListJobsFilter {
    pub state: Option<JobState>,
    pub kind: Option<String>,
    pub queue: Option<String>,
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
        ORDER BY id DESC
        LIMIT $4
        "#,
    )
    .bind(filter.state)
    .bind(&filter.kind)
    .bind(&filter.queue)
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
