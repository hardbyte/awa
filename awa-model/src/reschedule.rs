//! Canonical attempt re-scheduling (snooze, retry backoff, retry-after).
//!
//! #456: the historical canonical `running -> scheduled/retryable` move
//! while the cluster is canonical, and a move into the active queue-storage
//! schema's `deferred_jobs` once routing has flipped — so the canonical live
//! backlog converges during mixed transition even for handlers that snooze
//! on every run.
//!
//! Deliberately implemented as runtime SQL against the v040 schema rather
//! than a database function: the fix matters only on *unfinalized* clusters
//! mid-transition, exactly where the ADR-037 gate refuses `awa migrate`, so
//! it must work without a schema change (and stay backportable to 0.6.x,
//! whose migration numbering 0.7 already owns past v040).
//!
//! Stale-completion protection: the take of the canonical row is guarded by
//! `state = 'running' AND run_lease = <lease>` exactly like the historical
//! UPDATE, and the migrated successor gets a fresh job id — the old id no
//! longer exists in any canonical table, so a late completion from a rescued
//! attempt matches nothing and reports stale.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};

use crate::error::{map_sqlx_error, AwaError};
use crate::job::JobState;

/// What kind of re-schedule the completing attempt requested.
#[derive(Debug, Clone, Copy)]
pub enum Reschedule {
    /// `JobResult::Snooze` — back to `scheduled`, attempt not counted.
    Snooze { delay_secs: f64 },
    /// `JobResult::RetryAfter` — to `retryable` with a caller-chosen delay.
    RetryAfter { delay_secs: f64 },
    /// Retryable handler error — to `retryable` with DB-computed backoff.
    RetryBackoff,
}

impl Reschedule {
    fn next_state(&self) -> JobState {
        match self {
            Reschedule::Snooze { .. } => JobState::Scheduled,
            Reschedule::RetryAfter { .. } | Reschedule::RetryBackoff => JobState::Retryable,
        }
    }

    fn decrements_attempt(&self) -> bool {
        matches!(self, Reschedule::Snooze { .. })
    }

    /// `(delay_secs, use_backoff)` for the SQL run_at computation.
    fn delay_parameters(&self) -> (f64, bool) {
        match self {
            Reschedule::Snooze { delay_secs } | Reschedule::RetryAfter { delay_secs } => {
                (*delay_secs, false)
            }
            Reschedule::RetryBackoff => (0.0, true),
        }
    }
}

/// Result of a guarded re-schedule.
#[derive(Debug, Clone)]
pub enum RescheduleOutcome {
    /// The job moved back to the canonical deferred backlog under its
    /// existing id (cluster still canonical).
    Rescheduled {
        job_id: i64,
        run_at: DateTime<Utc>,
        attempt: i16,
    },
    /// Routing has flipped: the attempt was re-inserted as a queue-storage
    /// deferred job under a fresh id and left the canonical plane.
    Migrated {
        job_id: i64,
        run_at: DateTime<Utc>,
        attempt: i16,
    },
    /// The `state = 'running' AND run_lease = $n` guard matched nothing —
    /// the attempt was rescued or cancelled and this completion is stale.
    Stale,
}

/// The canonical row taken from `awa.jobs_hot`, with `unique_states`
/// projected to text (`JobRow` skips it in `FromRow`).
#[derive(sqlx::FromRow)]
struct TakenJob {
    kind: String,
    queue: String,
    args: serde_json::Value,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    attempted_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    errors: Option<Vec<serde_json::Value>>,
    metadata: serde_json::Value,
    tags: Vec<String>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
}

/// Re-schedule a running canonical attempt in its own transaction.
///
/// `error` is appended to the job's error list (retryable failures);
/// `progress` replaces the persisted progress snapshot, matching the
/// historical UPDATEs.
pub async fn reschedule_canonical_attempt(
    pool: &PgPool,
    job_id: i64,
    run_lease: i64,
    reschedule: Reschedule,
    error: Option<&serde_json::Value>,
    progress: Option<&serde_json::Value>,
) -> Result<RescheduleOutcome, AwaError> {
    let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
    let outcome =
        reschedule_canonical_attempt_tx(&mut tx, job_id, run_lease, reschedule, error, progress)
            .await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(outcome)
}

/// Transaction-composable form of [`reschedule_canonical_attempt`] for
/// callers that must commit follow-up work atomically with the re-schedule.
pub async fn reschedule_canonical_attempt_tx(
    conn: &mut PgConnection,
    job_id: i64,
    run_lease: i64,
    reschedule: Reschedule,
    error: Option<&serde_json::Value>,
    progress: Option<&serde_json::Value>,
) -> Result<RescheduleOutcome, AwaError> {
    // Lock the transition singleton FOR SHARE for the rest of this
    // transaction before deciding where the attempt goes. `storage_abort`,
    // `storage_enter_mixed_transition`, and `storage_finalize` all take
    // `FOR UPDATE` on this row, so the share lock serializes us against
    // them: without it, an abort could validate the queue-storage tables as
    // empty and restore canonical routing between our read and our insert,
    // stranding the job in a schema that is no longer active.
    //
    // `fetch_optional(...).flatten()` keeps the NULL-safe behaviour of
    // `active_queue_storage_schema()` when the singleton row is missing —
    // that resolves to canonical, same as a NULL result.
    let active_schema: Option<String> = sqlx::query_scalar(
        "SELECT awa.active_queue_storage_schema() \
         FROM awa.storage_transition_state WHERE singleton FOR SHARE",
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(map_sqlx_error)?
    .flatten();

    match active_schema {
        None => reschedule_canonical(conn, job_id, run_lease, reschedule, error, progress).await,
        Some(schema) => {
            migrate_to_queue_storage(
                conn, &schema, job_id, run_lease, reschedule, error, progress,
            )
            .await
        }
    }
}

/// Canonical routing: the same hot -> scheduled move the executor performed
/// historically. The jobs_hot/scheduled_jobs triggers keep unique claims and
/// admin dirty keys in sync.
async fn reschedule_canonical(
    conn: &mut PgConnection,
    job_id: i64,
    run_lease: i64,
    reschedule: Reschedule,
    error: Option<&serde_json::Value>,
    progress: Option<&serde_json::Value>,
) -> Result<RescheduleOutcome, AwaError> {
    let (delay_secs, use_backoff) = reschedule.delay_parameters();
    let row: Option<(i64, DateTime<Utc>, i16)> = sqlx::query_as(
        r#"
        WITH deleted AS (
            DELETE FROM awa.jobs_hot
            WHERE id = $1 AND state = 'running' AND run_lease = $2
            RETURNING *
        ), moved AS (
            INSERT INTO awa.scheduled_jobs (
                id, kind, queue, args, state, priority, attempt, max_attempts,
                run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
                created_at, errors, metadata, tags, unique_key, unique_states,
                callback_id, callback_timeout_at, callback_filter,
                callback_on_complete, callback_on_fail, callback_transform,
                run_lease, progress
            )
            SELECT
                id, kind, queue, args,
                $3::awa.job_state,
                priority,
                CASE WHEN $4 THEN attempt - 1 ELSE attempt END,
                max_attempts,
                CASE WHEN $5 THEN now() + awa.backoff_duration(attempt, max_attempts)
                     ELSE now() + make_interval(secs => $6) END,
                NULL, NULL, attempted_at,
                CASE WHEN $3::awa.job_state = 'retryable' THEN now() ELSE finalized_at END,
                created_at,
                CASE WHEN $7::jsonb IS NULL THEN errors ELSE errors || $7::jsonb END,
                metadata, tags, unique_key, unique_states,
                callback_id, callback_timeout_at, callback_filter,
                callback_on_complete, callback_on_fail, callback_transform,
                run_lease, $8
            FROM deleted
            RETURNING id, run_at, attempt
        )
        SELECT id, run_at, attempt FROM moved
        "#,
    )
    .bind(job_id)
    .bind(run_lease)
    .bind(reschedule.next_state())
    .bind(reschedule.decrements_attempt())
    .bind(use_backoff)
    .bind(delay_secs)
    .bind(error)
    .bind(progress)
    .fetch_optional(&mut *conn)
    .await
    .map_err(map_sqlx_error)?;

    Ok(match row {
        Some((job_id, run_at, attempt)) => RescheduleOutcome::Rescheduled {
            job_id,
            run_at,
            attempt,
        },
        None => RescheduleOutcome::Stale,
    })
}

/// Mixed transition / active: take the canonical row and re-insert the
/// attempt as a queue-storage deferred job under a fresh id.
async fn migrate_to_queue_storage(
    conn: &mut PgConnection,
    schema: &str,
    job_id: i64,
    run_lease: i64,
    reschedule: Reschedule,
    error: Option<&serde_json::Value>,
    progress: Option<&serde_json::Value>,
) -> Result<RescheduleOutcome, AwaError> {
    // The jobs_hot delete trigger releases the job's unique claim.
    let taken: Option<TakenJob> = sqlx::query_as(
        r#"
        DELETE FROM awa.jobs_hot
        WHERE id = $1 AND state = 'running' AND run_lease = $2
        RETURNING kind, queue, args, priority, attempt, run_lease, max_attempts,
                  attempted_at, created_at, errors, metadata, tags,
                  unique_key, unique_states::text AS unique_states
        "#,
    )
    .bind(job_id)
    .bind(run_lease)
    .fetch_optional(&mut *conn)
    .await
    .map_err(map_sqlx_error)?;

    let Some(job) = taken else {
        return Ok(RescheduleOutcome::Stale);
    };

    let (delay_secs, use_backoff) = reschedule.delay_parameters();
    let next_state = reschedule.next_state();
    let attempt = if reschedule.decrements_attempt() {
        job.attempt.saturating_sub(1)
    } else {
        job.attempt
    };

    // The id sequences differ between the canonical tables and a
    // queue-storage schema, so the successor must take a fresh id; reusing
    // the canonical id could collide with a future queue-storage insert.
    let (new_id, run_at): (i64, DateTime<Utc>) = sqlx::query_as(&format!(
        r#"
        SELECT nextval('{schema}.job_id_seq')::bigint,
               CASE WHEN $1 THEN now() + awa.backoff_duration($2::smallint, $3::smallint)
                    ELSE now() + make_interval(secs => $4) END
        "#
    ))
    .bind(use_backoff)
    .bind(job.attempt)
    .bind(job.max_attempts)
    .bind(delay_secs)
    .fetch_one(&mut *conn)
    .await
    .map_err(map_sqlx_error)?;

    let mut errors: Vec<serde_json::Value> = job.errors.clone().unwrap_or_default();
    if let Some(error) = error {
        errors.push(error.clone());
    }
    let payload = serde_json::json!({
        "metadata": job.metadata,
        "tags": job.tags,
        "errors": errors,
        "progress": progress,
    });

    // Claim the successor id. If a newer duplicate already holds the key
    // (possible only when 'running' is outside the job's unique_states mask,
    // so the delete above released nothing), the holder wins and the
    // successor proceeds unclaimed — consistent with the queue-storage
    // rescue path's duplicate handling.
    if let (Some(unique_key), Some(unique_states)) = (&job.unique_key, &job.unique_states) {
        let claims: bool =
            sqlx::query_scalar("SELECT awa.job_state_in_bitmask($1::bit(8), $2::awa.job_state)")
                .bind(unique_states)
                .bind(next_state)
                .fetch_one(&mut *conn)
                .await
                .map_err(map_sqlx_error)?;
        if claims {
            sqlx::query(
                "INSERT INTO awa.job_unique_claims (unique_key, job_id) \
                 VALUES ($1, $2) ON CONFLICT (unique_key) DO NOTHING",
            )
            .bind(unique_key)
            .bind(new_id)
            .execute(&mut *conn)
            .await
            .map_err(map_sqlx_error)?;
        }
    }

    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.deferred_jobs (
            job_id, kind, queue, args, state, priority, attempt, run_lease,
            max_attempts, run_at, attempted_at, finalized_at, created_at,
            unique_key, unique_states, payload
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::bit(8), $16)
        "#
    ))
    .bind(new_id)
    .bind(&job.kind)
    .bind(&job.queue)
    .bind(&job.args)
    .bind(next_state)
    .bind(job.priority)
    .bind(attempt)
    .bind(job.run_lease)
    .bind(job.max_attempts)
    .bind(run_at)
    .bind(job.attempted_at)
    .bind(if next_state == JobState::Retryable {
        Some(Utc::now())
    } else {
        None
    })
    .bind(job.created_at)
    .bind(&job.unique_key)
    .bind(&job.unique_states)
    .bind(&payload)
    .execute(&mut *conn)
    .await
    .map_err(map_sqlx_error)?;

    Ok(RescheduleOutcome::Migrated {
        job_id: new_id,
        run_at,
        attempt,
    })
}
