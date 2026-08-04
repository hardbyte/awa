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
use sqlx::{PgConnection, PgPool, Postgres, Transaction};

use crate::error::{map_sqlx_error, AwaError};
use crate::job::{JobRow, JobState};
use crate::queue_storage::QueueStorage;

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
    /// A newer duplicate acquired the unique claim while the canonical
    /// attempt was running. The attempted successor was recorded as a
    /// cancelled queue-storage terminal row and is not executable.
    CancelledDuplicate { job_id: i64 },
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
    tx: &mut Transaction<'_, Postgres>,
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
    .fetch_optional(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?
    .flatten();

    match active_schema {
        None => {
            reschedule_canonical(tx.as_mut(), job_id, run_lease, reschedule, error, progress).await
        }
        Some(schema) => {
            migrate_to_queue_storage(tx, &schema, job_id, run_lease, reschedule, error, progress)
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
    tx: &mut Transaction<'_, Postgres>,
    schema: &str,
    job_id: i64,
    run_lease: i64,
    reschedule: Reschedule,
    error: Option<&serde_json::Value>,
    progress: Option<&serde_json::Value>,
) -> Result<RescheduleOutcome, AwaError> {
    // The jobs_hot delete trigger releases the job's unique claim.
    //
    // Jobs carrying callback wiring are deliberately excluded: `deferred_jobs`
    // has no callback columns (queue storage keeps that state on the lease), so
    // migrating one would silently drop `callback_id` and the CEL expressions,
    // leaving no way to resume or resolve the callback. Those keep the
    // canonical write below — they are not the perpetual-snooze shape this
    // path exists for, and a callback job reaches a terminal state on its own.
    let taken: Option<TakenJob> = sqlx::query_as(
        r#"
        DELETE FROM awa.jobs_hot
        WHERE id = $1 AND state = 'running' AND run_lease = $2
          AND callback_id IS NULL
          AND callback_timeout_at IS NULL
          AND callback_filter IS NULL
          AND callback_on_complete IS NULL
          AND callback_on_fail IS NULL
          AND callback_transform IS NULL
        RETURNING kind, queue, args, priority, attempt, run_lease, max_attempts,
                  attempted_at, created_at, errors, metadata, tags,
                  unique_key, unique_states::text AS unique_states
        "#,
    )
    .bind(job_id)
    .bind(run_lease)
    .fetch_optional(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;

    let Some(job) = taken else {
        // Either the job carries callback wiring, or the run_lease guard did
        // not match. Fall through to the canonical write, which applies the
        // same guard: it re-schedules a callback-carrying job in place and
        // reports `Stale` for a genuinely lost attempt.
        return reschedule_canonical(tx.as_mut(), job_id, run_lease, reschedule, error, progress)
            .await;
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
    // `finalized_at` comes from the same statement as `run_at`: retention and
    // cleanup compare it against the database clock, so stamping it from the
    // worker would let clock skew shift those decisions.
    let (new_id, run_at, db_now): (i64, DateTime<Utc>, DateTime<Utc>) = sqlx::query_as(&format!(
        r#"
        SELECT nextval('{schema}.job_id_seq')::bigint,
               CASE WHEN $1 THEN now() + awa.backoff_duration($2::smallint, $3::smallint)
                    ELSE now() + make_interval(secs => $4) END,
               now()
        "#
    ))
    .bind(use_backoff)
    .bind(job.attempt)
    .bind(job.max_attempts)
    .bind(delay_secs)
    .fetch_one(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;

    let mut errors: Vec<serde_json::Value> = job.errors.clone().unwrap_or_default();
    if let Some(error) = error {
        errors.push(error.clone());
    }
    let successor = JobRow {
        id: new_id,
        kind: job.kind,
        queue: job.queue,
        args: job.args,
        state: next_state,
        priority: job.priority,
        attempt,
        run_lease: job.run_lease,
        max_attempts: job.max_attempts,
        run_at,
        heartbeat_at: None,
        deadline_at: None,
        attempted_at: job.attempted_at,
        finalized_at: (next_state == JobState::Retryable).then_some(db_now),
        created_at: job.created_at,
        errors: (!errors.is_empty()).then_some(errors),
        metadata: job.metadata,
        tags: job.tags,
        unique_key: job.unique_key,
        unique_states: None,
        callback_id: None,
        callback_timeout_at: None,
        callback_filter: None,
        callback_on_complete: None,
        callback_on_fail: None,
        callback_transform: None,
        progress: progress.cloned(),
    };
    let inserted = QueueStorage::from_existing_schema(schema)?
        .insert_migrated_deferred_or_cancel_duplicate_tx(
            tx,
            successor,
            job.unique_states,
            "rescheduled as duplicate: unique claim held by a newer job",
        )
        .await?;

    if inserted.state == JobState::Cancelled {
        Ok(RescheduleOutcome::CancelledDuplicate {
            job_id: inserted.id,
        })
    } else {
        Ok(RescheduleOutcome::Migrated {
            job_id: inserted.id,
            run_at: inserted.run_at,
            attempt: inserted.attempt,
        })
    }
}
