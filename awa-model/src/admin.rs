use crate::dlq::DlqMetadata;
use crate::error::AwaError;
use crate::job::{JobRow, JobState};
use crate::queue_storage::QueueStorage;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::Acquire;
use sqlx::PgExecutor;
use sqlx::PgPool;
use sqlx::Postgres;
use std::cmp::max;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobTimelineEvent {
    pub timestamp: DateTime<Utc>,
    pub label: String,
    pub state: Option<JobState>,
    pub detail: Option<String>,
    pub is_error: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobDumpSummary {
    pub original_priority: i16,
    pub can_retry: bool,
    pub can_cancel: bool,
    pub error_count: usize,
    pub latest_error: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDump {
    pub job: JobRow,
    pub summary: JobDumpSummary,
    pub timeline: Vec<JobTimelineEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dlq: Option<DlqMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunDumpSource {
    CurrentJobRow,
    ErrorHistory,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CallbackDump {
    pub callback_id: Option<Uuid>,
    pub callback_timeout_at: Option<DateTime<Utc>>,
    pub callback_filter: Option<String>,
    pub callback_on_complete: Option<String>,
    pub callback_on_fail: Option<String>,
    pub callback_transform: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunDump {
    pub job_id: i64,
    pub kind: String,
    pub queue: String,
    pub selected_attempt: i16,
    pub current_attempt: i16,
    pub current_run_lease: i64,
    pub selected_run_lease: Option<i64>,
    pub source: RunDumpSource,
    pub state: JobState,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub terminal: Option<bool>,
    pub progress: Option<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
    pub callback: Option<CallbackDump>,
    pub raw_error_entry: Option<serde_json::Value>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone)]
struct ErrorEntry {
    attempt: Option<i16>,
    error: Option<String>,
    at: Option<DateTime<Utc>>,
    terminal: bool,
    raw: serde_json::Value,
}

fn parse_error_entry(value: &serde_json::Value) -> Option<ErrorEntry> {
    let obj = value.as_object()?;
    let attempt = obj
        .get("attempt")
        .and_then(|v| v.as_i64())
        .and_then(|v| i16::try_from(v).ok());
    let error = obj
        .get("error")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned);
    let at = obj
        .get("at")
        .and_then(|v| v.as_str())
        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
        .map(|v| v.with_timezone(&Utc));
    let terminal = obj
        .get("terminal")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    Some(ErrorEntry {
        attempt,
        error,
        at,
        terminal,
        raw: value.clone(),
    })
}

fn original_priority(job: &JobRow) -> i16 {
    job.metadata
        .get("_awa_original_priority")
        .and_then(|v| v.as_i64())
        .and_then(|v| i16::try_from(v).ok())
        .unwrap_or(job.priority)
}

fn build_job_timeline(job: &JobRow) -> Vec<JobTimelineEvent> {
    let mut events = Vec::new();
    events.push(JobTimelineEvent {
        timestamp: job.created_at,
        label: "Created".to_string(),
        state: None,
        detail: None,
        is_error: false,
    });

    if let Some(errors) = &job.errors {
        for entry in errors.iter().filter_map(parse_error_entry) {
            if let Some(timestamp) = entry.at {
                let label = match entry.attempt {
                    Some(attempt) => format!("Attempt {attempt} failed"),
                    None => "Error".to_string(),
                };
                events.push(JobTimelineEvent {
                    timestamp,
                    label,
                    state: Some(JobState::Failed),
                    detail: entry.error,
                    is_error: true,
                });
            }
        }
    }

    if let Some(attempted_at) = job.attempted_at {
        let label = if job.state == JobState::WaitingExternal {
            format!("Attempt {} — waiting for callback", job.attempt)
        } else if job.state.is_terminal() {
            format!("Attempt {} started", job.attempt)
        } else {
            format!("Attempt {} running", job.attempt)
        };
        let state = if job.state.is_terminal() {
            Some(JobState::Running)
        } else {
            Some(job.state)
        };
        events.push(JobTimelineEvent {
            timestamp: attempted_at,
            label,
            state,
            detail: None,
            is_error: false,
        });
    }

    if let Some(finalized_at) = job.finalized_at {
        let label = match job.state {
            JobState::Completed => format!("Completed (attempt {})", job.attempt),
            JobState::Failed => format!(
                "Failed after {} attempt{}",
                job.attempt,
                if job.attempt == 1 { "" } else { "s" }
            ),
            JobState::Cancelled => "Cancelled".to_string(),
            _ => "Finalized".to_string(),
        };
        events.push(JobTimelineEvent {
            timestamp: finalized_at,
            label,
            state: Some(job.state),
            detail: None,
            is_error: false,
        });
    }

    events.sort_by_key(|event| event.timestamp);
    events
}

fn build_job_dump(job: JobRow, dlq: Option<DlqMetadata>) -> JobDump {
    let latest_error = job
        .errors
        .as_ref()
        .and_then(|errors| errors.last())
        .cloned();
    let summary = JobDumpSummary {
        original_priority: original_priority(&job),
        can_retry: dlq.is_none()
            && matches!(
                job.state,
                JobState::Failed | JobState::Cancelled | JobState::WaitingExternal
            ),
        can_cancel: dlq.is_none() && !job.state.is_terminal(),
        error_count: job.errors.as_ref().map(|errors| errors.len()).unwrap_or(0),
        latest_error,
    };
    let timeline = build_job_timeline(&job);
    JobDump {
        job,
        summary,
        timeline,
        dlq,
    }
}

fn callback_dump(job: &JobRow) -> Option<CallbackDump> {
    let callback = CallbackDump {
        callback_id: job.callback_id,
        callback_timeout_at: job.callback_timeout_at,
        callback_filter: job.callback_filter.clone(),
        callback_on_complete: job.callback_on_complete.clone(),
        callback_on_fail: job.callback_on_fail.clone(),
        callback_transform: job.callback_transform.clone(),
    };
    if callback.callback_id.is_none()
        && callback.callback_timeout_at.is_none()
        && callback.callback_filter.is_none()
        && callback.callback_on_complete.is_none()
        && callback.callback_on_fail.is_none()
        && callback.callback_transform.is_none()
    {
        None
    } else {
        Some(callback)
    }
}

async fn active_queue_storage(pool: &PgPool) -> Result<Option<QueueStorage>, AwaError> {
    QueueStorage::active_schema(pool)
        .await?
        .map(QueueStorage::from_existing_schema)
        .transpose()
}

/// Transaction-aware variant of [`active_queue_storage`] — read the active
/// queue-storage schema inside the caller's transaction so the engine
/// selection and the resulting state UPDATE commit (or roll back)
/// together. Required by the ADR-029 atomic callback resolution path so a
/// race between this read and a routing flip can never split the chosen
/// engine and the transition that follows.
async fn active_queue_storage_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<QueueStorage>, AwaError> {
    QueueStorage::active_schema_in_tx(tx)
        .await?
        .map(QueueStorage::from_existing_schema)
        .transpose()
}

/// Callback ownership can remain canonical while new jobs route to queue storage.
/// Lock transition state through the caller's transaction so finalize cannot
/// change the fallback boundary halfway through a callback operation (#462).
async fn callback_queue_storage_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    callback_id: Uuid,
) -> Result<Option<QueueStorage>, AwaError> {
    let state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM awa.storage_transition_state WHERE singleton FOR SHARE",
    )
    .fetch_optional(tx.as_mut())
    .await?;
    let Some(store) = active_queue_storage_in_tx(tx).await? else {
        return Ok(None);
    };
    if state.as_deref() == Some("mixed_transition")
        && store
            .callback_job_in_tx(tx, callback_id, None, false)
            .await?
            .is_none()
    {
        return Ok(None);
    }
    Ok(Some(store))
}

/// A drain handler keeps its canonical job ID across the routing flip. Job
/// IDs are global; migrated successors get fresh IDs, so an existing canonical
/// row identifies this handler's store without searching the queue view.
async fn callback_job_queue_storage_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job_id: i64,
) -> Result<Option<QueueStorage>, AwaError> {
    let state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM awa.storage_transition_state WHERE singleton FOR SHARE",
    )
    .fetch_optional(tx.as_mut())
    .await?;
    let Some(store) = active_queue_storage_in_tx(tx).await? else {
        return Ok(None);
    };
    if state.as_deref() == Some("mixed_transition") {
        let canonical: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM awa.jobs_hot WHERE id=$1)")
                .bind(job_id)
                .fetch_one(tx.as_mut())
                .await?;
        if canonical {
            return Ok(None);
        }
    }
    Ok(Some(store))
}

/// Every job a queue-storage schema holds, in the state it is in, as a
/// `current_jobs` CTE. Admin reads that can't afford to expand it use bounded
/// forms of their own; this is the definition they must agree with.
#[doc(hidden)]
pub fn queue_storage_current_jobs_cte(schema: &str) -> String {
    format!(
        r#"
        WITH current_available AS (
            SELECT
                ready.job_id,
                ready.kind,
                ready.queue,
                'available'::awa.job_state AS state,
                ready.created_at,
                ready.run_at,
                NULL::timestamptz AS finalized_at
            FROM {schema}.ready_entries AS ready
            JOIN {schema}.queue_claim_heads AS claims
              ON claims.queue = ready.queue
             AND claims.priority = ready.priority
             AND claims.enqueue_shard = ready.enqueue_shard
            WHERE ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.ready_tombstones AS tomb
                  WHERE tomb.ready_slot = ready.ready_slot
                    AND tomb.ready_generation = ready.ready_generation
                    AND tomb.queue = ready.queue
                    AND tomb.priority = ready.priority
                    AND tomb.enqueue_shard = ready.enqueue_shard
                    AND tomb.lane_seq = ready.lane_seq
              )
        ),
        current_jobs AS (
            SELECT job_id, kind, queue, state, created_at, run_at, finalized_at
            FROM current_available
            UNION ALL
            SELECT job_id, kind, queue, state, created_at, run_at, finalized_at
            FROM {schema}.deferred_jobs
            UNION ALL
            SELECT
                leases.job_id,
                ready.kind,
                leases.queue,
                leases.state,
                ready.created_at,
                ready.run_at,
                NULL::timestamptz AS finalized_at
            FROM {schema}.leases AS leases
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = leases.ready_slot
             AND ready.ready_generation = leases.ready_generation
             AND ready.queue = leases.queue
             -- Leases record the aged effective priority while the ready row
             -- keeps its lane priority, so match the attempt instead.
             AND ready.job_id = leases.job_id
             AND ready.run_lease + 1 = leases.run_lease
             AND ready.enqueue_shard = leases.enqueue_shard
             AND ready.lane_seq = leases.lane_seq
             AND NOT EXISTS (
                 SELECT 1 FROM {schema}.ready_tombstones AS tomb
                 WHERE tomb.ready_slot = ready.ready_slot
                   AND tomb.ready_generation = ready.ready_generation
                   AND tomb.queue = ready.queue
                   AND tomb.priority = ready.priority
                   AND tomb.enqueue_shard = ready.enqueue_shard
                   AND tomb.lane_seq = ready.lane_seq
             )
            UNION ALL
            -- Receipt claims that have not materialised into `leases` are
            -- running too (#246 / #416). They live on legacy row-local
            -- `lease_claims` or, by default, compact `lease_claim_batches`.
            -- Report both as `running`, anti-joined against every durable
            -- closure/terminal/materialised-lease shape. This mirrors
            -- QueueStorage::load_job (the batch-aware single-job admin read)
            -- and the canonical open-receipt shape in
            -- queue_storage::open_receipt_running_claims_sql; it is spelled
            -- out here because this view projects the full job row
            -- (kind/created_at/run_at from `ready_entries`), not just claim
            -- identity.
            SELECT
                claims.job_id,
                ready.kind,
                claims.queue,
                'running'::awa.job_state AS state,
                ready.created_at,
                ready.run_at,
                NULL::timestamptz AS finalized_at
            FROM {schema}.lease_claims AS claims
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = claims.ready_slot
             AND ready.ready_generation = claims.ready_generation
             AND ready.queue = claims.queue
             -- Claims keep the aged effective priority; match the ready attempt.
             AND ready.job_id = claims.job_id
             AND ready.run_lease + 1 = claims.run_lease
             AND ready.enqueue_shard = claims.enqueue_shard
             AND ready.lane_seq = claims.lane_seq
             -- Moving a ready row tombstones the old one and re-inserts the same
             -- attempt, which can reuse the old lane_seq on the new lane.
             AND NOT EXISTS (
                 SELECT 1 FROM {schema}.ready_tombstones AS tomb
                 WHERE tomb.ready_slot = ready.ready_slot
                   AND tomb.ready_generation = ready.ready_generation
                   AND tomb.queue = ready.queue
                   AND tomb.priority = ready.priority
                   AND tomb.enqueue_shard = ready.enqueue_shard
                   AND tomb.lane_seq = ready.lane_seq
             )
            WHERE claims.closed_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.lease_claim_closures AS cx
                  WHERE cx.claim_slot = claims.claim_slot
                    AND cx.job_id = claims.job_id
                    AND cx.run_lease = claims.run_lease
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.lease_claim_closure_batches AS cb
                  WHERE cb.receipt_ranges @> claims.receipt_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.leases AS lease
                  WHERE lease.job_id = claims.job_id
                    AND lease.run_lease = claims.run_lease
              )
              -- Terminal supersession counts as closed evidence even
              -- without an explicit closure row (mirrors the compact
              -- branch below and open_receipt_running_claims_sql);
              -- without these a legacy row-local claim would be listed
              -- as running AND terminal at once.
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.done_entries AS done
                  WHERE done.job_id = claims.job_id
                    AND done.run_lease = claims.run_lease
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.deferred_jobs AS deferred
                  WHERE deferred.job_id = claims.job_id
                    AND deferred.run_lease = claims.run_lease
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.dlq_entries AS dlq
                  WHERE dlq.job_id = claims.job_id
                    AND dlq.run_lease = claims.run_lease
              )
            UNION ALL
            SELECT
                items.job_id,
                ready.kind,
                batches.queue,
                'running'::awa.job_state AS state,
                ready.created_at,
                ready.run_at,
                NULL::timestamptz AS finalized_at
            FROM {schema}.lease_claim_batches AS batches
            CROSS JOIN LATERAL unnest(
                batches.job_ids,
                batches.run_leases,
                batches.receipt_ids,
                batches.lane_seqs
            ) AS items(job_id, run_lease, receipt_id, lane_seq)
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = batches.ready_slot
             AND ready.ready_generation = batches.ready_generation
             AND ready.queue = batches.queue
             AND ready.enqueue_shard = batches.enqueue_shard
             AND ready.lane_seq = items.lane_seq
             AND ready.job_id = items.job_id
             AND ready.run_lease + 1 = items.run_lease
             AND NOT EXISTS (
                 SELECT 1 FROM {schema}.ready_tombstones AS tomb
                 WHERE tomb.ready_slot = ready.ready_slot
                   AND tomb.ready_generation = ready.ready_generation
                   AND tomb.queue = ready.queue
                   AND tomb.priority = ready.priority
                   AND tomb.enqueue_shard = ready.enqueue_shard
                   AND tomb.lane_seq = ready.lane_seq
             )
            WHERE NOT EXISTS (
                  SELECT 1 FROM {schema}.lease_claim_closures AS cx
                  WHERE cx.claim_slot = batches.claim_slot
                    AND cx.job_id = items.job_id
                    AND cx.run_lease = items.run_lease
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.lease_claim_closure_batches AS cb
                  WHERE cb.claim_slot = batches.claim_slot
                    AND cb.receipt_ranges @> items.receipt_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.leases AS lease
                  WHERE lease.job_id = items.job_id
                    AND lease.run_lease = items.run_lease
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.done_entries AS done
                  WHERE done.job_id = items.job_id
                    AND done.run_lease = items.run_lease
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.deferred_jobs AS deferred
                  WHERE deferred.job_id = items.job_id
                    AND deferred.run_lease = items.run_lease
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.dlq_entries AS dlq
                  WHERE dlq.job_id = items.job_id
                    AND dlq.run_lease = items.run_lease
              )
            UNION ALL
            SELECT job_id, kind, queue, state, created_at, run_at, finalized_at
            FROM {schema}.terminal_jobs
            UNION ALL
            SELECT
                job_id,
                kind,
                queue,
                'failed'::awa.job_state AS state,
                created_at,
                run_at,
                finalized_at
            FROM {schema}.dlq_entries
        )
        "#
    )
}

/// Each queue's available rows, exactly, and the oldest of them, as
/// `(queue, available, oldest_run_at)`.
///
/// Each lane's unclaimed rows are found the way a claim finds them: through
/// `ready_segments` from the lane's claim cursor, then along the ready
/// primary key within each segment's ring slot. The ready key leads with the
/// ring slot, so a lane range without it cannot use the index and would scan
/// every retained ready row. The cost follows the backlog.
///
/// `oldest_run_at` is the earliest `run_at` among those rows, not the first
/// row's: a queue move or priority aging appends an older job behind newer
/// ones.
fn queue_storage_available_by_queue_sql(schema: &str) -> String {
    format!(
        r#"
        SELECT
            heads.queue,
            COALESCE(sum(lane.available), 0)::bigint AS available,
            min(lane.oldest_run_at) AS oldest_run_at
        FROM (
            SELECT
                claims.queue,
                claims.priority,
                claims.enqueue_shard,
                {schema}.sequence_next_value(claims.seq_name) AS claim_seq,
                {schema}.sequence_next_value(enqueues.seq_name) AS next_seq
            FROM {schema}.queue_claim_heads AS claims
            JOIN {schema}.queue_enqueue_heads AS enqueues
              ON enqueues.queue = claims.queue
             AND enqueues.priority = claims.priority
             AND enqueues.enqueue_shard = claims.enqueue_shard
        ) AS heads
        CROSS JOIN LATERAL (
            {lane_rows}
        ) AS lane
        GROUP BY heads.queue
        "#,
        lane_rows = queue_storage_available_lane_rows_sql(
            schema,
            "heads",
            "count(*)::bigint AS available, min(ready.run_at) AS oldest_run_at",
            "TRUE",
        ),
    )
}

/// A `SELECT {columns} FROM ready_segments JOIN ready_entries` over the
/// unclaimed, untombstoned rows of the lane in `heads` (which must carry
/// `queue`, `priority`, `enqueue_shard`, `claim_seq` and `next_seq`) that
/// satisfy `condition` on `ready`. See [`queue_storage_available_by_queue_sql`] for why it
/// walks segments.
fn queue_storage_available_lane_rows_sql(
    schema: &str,
    heads: &str,
    columns: &str,
    condition: &str,
) -> String {
    format!(
        r#"
            SELECT {columns}
            FROM (
                -- The lane's segments from its claim cursor, in lane order
                -- along the lane index. Without the order the planner can
                -- prefer walking every segment by ring slot.
                SELECT
                    segment.ready_slot,
                    segment.ready_generation,
                    GREATEST(segment.first_lane_seq, {heads}.claim_seq) AS from_seq,
                    LEAST(segment.next_lane_seq, {heads}.next_seq) AS to_seq
                FROM {schema}.ready_segments AS segment
                WHERE segment.queue = {heads}.queue
                  AND segment.priority = {heads}.priority
                  AND segment.enqueue_shard = {heads}.enqueue_shard
                  AND segment.next_lane_seq > {heads}.claim_seq
                  AND segment.first_lane_seq < {heads}.next_seq
                ORDER BY segment.next_lane_seq
            ) AS segment
            -- Each segment's rows along the ready key within its ring slot.
            -- OFFSET 0 keeps this a per-segment probe: flattened into a
            -- join, a filter such as a creation bound can lead the planner
            -- to scan every retained ready row and match segments after.
            CROSS JOIN LATERAL (
                SELECT ready.*
                FROM {schema}.ready_entries AS ready
                WHERE ready.ready_slot = segment.ready_slot
                  AND ready.ready_generation = segment.ready_generation
                  AND ready.queue = {heads}.queue
                  AND ready.priority = {heads}.priority
                  AND ready.enqueue_shard = {heads}.enqueue_shard
                  AND ready.lane_seq >= segment.from_seq
                  AND ready.lane_seq < segment.to_seq
                OFFSET 0
            ) AS ready
            WHERE ({condition})
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.ready_tombstones AS tomb
                  WHERE tomb.ready_slot = ready.ready_slot
                    AND tomb.ready_generation = ready.ready_generation
                    AND tomb.queue = ready.queue
                    AND tomb.priority = ready.priority
                    AND tomb.enqueue_shard = ready.enqueue_shard
                    AND tomb.lane_seq = ready.lane_seq
              )
        "#
    )
}

/// Per-queue counts for the admin queue views, ending in a `signals` CTE.
/// They come from the live-work tables the worker queue gauges read, never
/// from `terminal_jobs`, whose completion batches expand and join back to
/// `ready_entries`: the cost follows live work rather than retained history.
/// `available` is exact, and `completed_last_hour` comes from completion-batch
/// sizes and completed `done_entries` rows.
fn queue_storage_signals_cte(schema: &str) -> String {
    format!(
        r#"
        WITH head_signal AS (
            SELECT
                enqueues.queue,
                enqueues.priority,
                enqueues.enqueue_shard,
                {schema}.sequence_next_value(enqueues.seq_name) AS next_seq,
                {schema}.sequence_next_value(claims.seq_name) AS claim_seq
            FROM {schema}.queue_enqueue_heads AS enqueues
            JOIN {schema}.queue_claim_heads AS claims
              ON claims.queue = enqueues.queue
             AND claims.priority = enqueues.priority
             AND claims.enqueue_shard = enqueues.enqueue_shard
        ),
        queues AS (
            SELECT DISTINCT queue
            FROM (
                SELECT queue FROM awa.queue_meta
                UNION ALL
                SELECT queue FROM head_signal
                UNION ALL
                SELECT queue FROM {schema}.leases
                UNION ALL
                SELECT queue FROM {schema}.deferred_jobs
                UNION ALL
                SELECT queue FROM {schema}.queue_terminal_live_counts
                UNION ALL
                SELECT queue FROM {schema}.queue_terminal_rollups
                UNION ALL
                SELECT queue FROM {schema}.dlq_entries
            ) queues
        ),
        ready AS (
            {available}
        ),
        lag AS (
            -- The oldest available job, as before queue storage: it reads the
            -- same backlog rows the exact count already reads.
            SELECT
                queue,
                EXTRACT(EPOCH FROM clock_timestamp() - oldest_run_at)::double precision
                    AS lag_seconds
            FROM ready
            WHERE oldest_run_at IS NOT NULL
        ),
        leases AS (
            -- Receipt-held jobs are running without a `leases` row. A claim
            -- reaches `waiting_external` only once materialised into
            -- `leases`, so that count needs no claim leg.
            SELECT
                queue,
                sum(running)::bigint AS running,
                sum(waiting_external)::bigint AS waiting_external
            FROM (
                SELECT
                    queue,
                    count(*) FILTER (WHERE state = 'running')::bigint AS running,
                    count(*) FILTER (WHERE state = 'waiting_external')::bigint AS waiting_external
                FROM {schema}.leases
                GROUP BY queue
                UNION ALL
                SELECT queue, count(*)::bigint, 0::bigint
                FROM ({open_running}) AS open_claims
                GROUP BY queue
            ) AS live
            GROUP BY queue
        ),
        deferred AS (
            SELECT
                queue,
                count(*) FILTER (WHERE state = 'scheduled')::bigint AS scheduled,
                count(*) FILTER (WHERE state = 'retryable')::bigint AS retryable
            FROM {schema}.deferred_jobs
            GROUP BY queue
        ),
        terminal AS (
            SELECT queue, count(*)::bigint AS failed_done
            FROM {schema}.done_entries
            WHERE state = 'failed'
            GROUP BY queue
        ),
        dlq AS (
            SELECT queue, count(*)::bigint AS failed_dlq
            FROM {schema}.dlq_entries
            GROUP BY queue
        ),
        completed_recent AS (
            WITH recent_tombstones AS MATERIALIZED (
                SELECT ready_slot, ready_generation, queue, priority, enqueue_shard,
                       job_id, run_lease
                FROM {schema}.receipt_completion_tombstones
                WHERE deleted_at > clock_timestamp() - interval '1 hour'
            ),
            -- Recent batches in a lane with a recent tombstone: the only ones
            -- whose members need reading. Selected before expanding any
            -- member array, so retained batches are never expanded.
            tombstoned_batches AS MATERIALIZED (
                SELECT batch.queue, batch.ready_slot, batch.job_ids, batch.run_leases
                FROM {schema}.receipt_completion_batches AS batch
                WHERE batch.finalized_at > clock_timestamp() - interval '1 hour'
                  AND EXISTS (
                      SELECT 1 FROM recent_tombstones AS tomb
                      WHERE tomb.ready_slot = batch.ready_slot
                        AND tomb.ready_generation = batch.ready_generation
                        AND tomb.queue = batch.queue
                        AND tomb.priority = batch.priority
                        AND tomb.enqueue_shard = batch.enqueue_shard
                  )
            )
            SELECT queue, sum(completed)::bigint AS completed_last_hour
            FROM (
                -- A batch carries its queue and size, so it is counted
                -- without unnesting its members.
                SELECT queue, completed_count::bigint AS completed
                FROM {schema}.receipt_completion_batches
                WHERE finalized_at > clock_timestamp() - interval '1 hour'
                UNION ALL
                -- Less its members deleted or retried since, as
                -- `terminal_jobs` drops them. A tombstone on a member
                -- finalised within the hour is itself newer than that. Each
                -- tombstoned batch is expanded once and its members matched by
                -- hash, so the work follows the last hour's completions and
                -- deletions, never one batch scan per tombstone.
                SELECT batch.queue, -1::bigint
                FROM tombstoned_batches AS batch
                CROSS JOIN LATERAL unnest(batch.job_ids, batch.run_leases)
                    AS member(job_id, run_lease)
                JOIN recent_tombstones AS tomb
                  ON tomb.ready_slot = batch.ready_slot
                 AND tomb.job_id = member.job_id
                 AND tomb.run_lease = member.run_lease
                UNION ALL
                SELECT queue, 1::bigint
                FROM {schema}.done_entries
                WHERE state = 'completed'
                  AND finalized_at > clock_timestamp() - interval '1 hour'
            ) AS recent
            GROUP BY queue
        ),
        signals AS (
            SELECT
                queues.queue,
                COALESCE(ready.available, 0)::bigint AS available,
                COALESCE(leases.running, 0)::bigint AS running,
                COALESCE(leases.waiting_external, 0)::bigint AS waiting_external,
                COALESCE(deferred.scheduled, 0)::bigint AS scheduled,
                COALESCE(deferred.retryable, 0)::bigint AS retryable,
                (COALESCE(terminal.failed_done, 0) + COALESCE(dlq.failed_dlq, 0))::bigint AS failed,
                lag.lag_seconds,
                COALESCE(completed_recent.completed_last_hour, 0)::bigint AS completed_last_hour
            FROM queues
            LEFT JOIN ready ON ready.queue = queues.queue
            LEFT JOIN lag ON lag.queue = queues.queue
            LEFT JOIN leases ON leases.queue = queues.queue
            LEFT JOIN deferred ON deferred.queue = queues.queue
            LEFT JOIN terminal ON terminal.queue = queues.queue
            LEFT JOIN dlq ON dlq.queue = queues.queue
            LEFT JOIN completed_recent ON completed_recent.queue = queues.queue
        )
        "#,
        available = queue_storage_available_by_queue_sql(schema),
        open_running = crate::queue_storage::open_receipt_running_claims_sql(schema),
    )
}

/// Jobs created at or after `since` (a bind such as `$1`), each in its
/// current state, as a `recent_jobs` CTE of `created_at` and `state`.
///
/// It selects what `current_jobs` would with the same bound, but bounds each
/// source before any join: available rows are read per lane from its claim
/// cursor through its segments, and terminal rows only from done rows and completion batches
/// finalised since `since`, as a job finishes after it is created. `since`
/// must be a bind rather than an expression such as `clock_timestamp()`,
/// which Postgres can't push into a bound on `finalized_at`.
fn queue_storage_recent_jobs_cte(schema: &str, since: &str) -> String {
    let available = queue_storage_available_lane_rows_sql(
        schema,
        "head_signal",
        "ready.created_at",
        &format!("ready.created_at >= {since}"),
    );
    let open_running = crate::queue_storage::open_receipt_running_claims_sql(schema);
    format!(
        r#"
        WITH head_signal AS (
            SELECT
                claims.queue,
                claims.priority,
                claims.enqueue_shard,
                {schema}.sequence_next_value(claims.seq_name) AS claim_seq,
                {schema}.sequence_next_value(enqueues.seq_name) AS next_seq
            FROM {schema}.queue_claim_heads AS claims
            JOIN {schema}.queue_enqueue_heads AS enqueues
              ON enqueues.queue = claims.queue
             AND enqueues.priority = claims.priority
             AND enqueues.enqueue_shard = claims.enqueue_shard
        ),
        recent_jobs AS (
            SELECT ready.created_at, 'available'::awa.job_state AS state
            FROM head_signal
            CROSS JOIN LATERAL (
                {available}
            ) AS ready
            UNION ALL
            SELECT created_at, state
            FROM {schema}.deferred_jobs
            WHERE created_at >= {since}
            UNION ALL
            SELECT ready.created_at, leases.state
            FROM {schema}.leases AS leases
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = leases.ready_slot
             AND ready.ready_generation = leases.ready_generation
             AND ready.queue = leases.queue
             -- Leases record the aged effective priority while the ready row
             -- keeps its lane priority, so match the attempt instead.
             AND ready.job_id = leases.job_id
             AND ready.run_lease + 1 = leases.run_lease
             AND ready.enqueue_shard = leases.enqueue_shard
             AND ready.lane_seq = leases.lane_seq
             AND NOT EXISTS (
                 SELECT 1 FROM {schema}.ready_tombstones AS tomb
                 WHERE tomb.ready_slot = ready.ready_slot
                   AND tomb.ready_generation = ready.ready_generation
                   AND tomb.queue = ready.queue
                   AND tomb.priority = ready.priority
                   AND tomb.enqueue_shard = ready.enqueue_shard
                   AND tomb.lane_seq = ready.lane_seq
             )
            WHERE ready.created_at >= {since}
            UNION ALL
            -- A queue or priority move copies the row with its times, so
            -- any one of a job's ready rows gives its creation time.
            SELECT created.created_at, 'running'::awa.job_state
            FROM ({open_running}) AS open_claims
            CROSS JOIN LATERAL (
                SELECT ready.created_at
                FROM {schema}.ready_entries AS ready
                WHERE ready.job_id = open_claims.job_id
                LIMIT 1
            ) AS created
            WHERE created.created_at >= {since}
            UNION ALL
            -- What `terminal_jobs` holds, bounded the same way, with each
            -- row's creation time probed along the ready key. Read through
            -- the view, Postgres joins the window's members to a scan of
            -- every retained ready row.
            SELECT COALESCE(done.created_at, ready.created_at, done.finalized_at), done.state
            FROM {schema}.done_entries AS done
            LEFT JOIN LATERAL (
                SELECT ready.created_at
                FROM {schema}.ready_entries AS ready
                WHERE ready.ready_slot = done.ready_slot
                  AND ready.ready_generation = done.ready_generation
                  AND ready.queue = done.queue
                  AND ready.priority = done.priority
                  AND ready.enqueue_shard = done.enqueue_shard
                  AND ready.lane_seq = done.lane_seq
                OFFSET 0
            ) AS ready ON TRUE
            WHERE done.finalized_at >= {since}
              AND COALESCE(done.created_at, ready.created_at, done.finalized_at) >= {since}
            UNION ALL
            SELECT ready.created_at, 'completed'::awa.job_state
            FROM {schema}.receipt_completion_batches AS batch
            CROSS JOIN LATERAL unnest(batch.job_ids, batch.run_leases, batch.lane_seqs)
                AS item(job_id, run_lease, lane_seq)
            CROSS JOIN LATERAL (
                SELECT ready.created_at
                FROM {schema}.ready_entries AS ready
                WHERE ready.ready_slot = batch.ready_slot
                  AND ready.ready_generation = batch.ready_generation
                  AND ready.queue = batch.queue
                  AND ready.priority = batch.priority
                  AND ready.enqueue_shard = batch.enqueue_shard
                  AND ready.lane_seq = item.lane_seq
                  AND ready.job_id = item.job_id
                OFFSET 0
            ) AS ready
            WHERE batch.finalized_at >= {since}
              AND ready.created_at >= {since}
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.receipt_completion_tombstones AS tomb
                  WHERE tomb.ready_slot = batch.ready_slot
                    AND tomb.ready_generation = batch.ready_generation
                    AND tomb.job_id = item.job_id
                    AND tomb.run_lease = item.run_lease
              )
            UNION ALL
            SELECT created_at, 'failed'::awa.job_state
            FROM {schema}.dlq_entries
            WHERE created_at >= {since}
        )
        "#
    )
}

// Per-source kind/queue counts keep the same joins and predicates as the
// combined projection, while allowing each source's relation locks to end
// before another source opens a different set of retained partitions.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KindCountSource {
    Available,
    Leased,
    Deferred,
    Done,
    Receipts,
    Dlq,
}

// Generate only this source's relations: unused CTEs would still acquire
// relation locks during parse/plan before execution could discard them.
fn queue_storage_kind_count_source_sql(schema: &str, source: KindCountSource) -> String {
    let available = queue_storage_available_lane_rows_sql(schema, "heads", "ready.kind", "TRUE");
    match source {
        KindCountSource::Available => format!(
            r#"
        WITH heads AS MATERIALIZED (
            SELECT queue, priority, enqueue_shard, claim_seq, next_seq
            FROM unnest(
                $1::text[], $2::smallint[], $3::smallint[], $4::bigint[], $5::bigint[]
            ) AS captured(queue, priority, enqueue_shard, claim_seq, next_seq)
        )
            SELECT lane.kind, heads.queue, count(*)::bigint AS n, 0::bigint AS n_recent
            FROM heads
            CROSS JOIN LATERAL (
                {available}
            ) AS lane
            GROUP BY lane.kind, heads.queue
            "#
        ),
        KindCountSource::Leased => format!(
            r#"
            SELECT ready.kind, leases.queue, count(*)::bigint AS n, 0::bigint AS n_recent
            FROM {schema}.leases AS leases
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = leases.ready_slot
             AND ready.ready_generation = leases.ready_generation
             AND ready.queue = leases.queue
             -- Leases record the aged effective priority while the ready row
             -- keeps its lane priority, so match the attempt instead.
             AND ready.job_id = leases.job_id
             AND ready.run_lease + 1 = leases.run_lease
             AND ready.enqueue_shard = leases.enqueue_shard
             AND ready.lane_seq = leases.lane_seq
             AND NOT EXISTS (
                 SELECT 1 FROM {schema}.ready_tombstones AS tomb
                 WHERE tomb.ready_slot = ready.ready_slot
                   AND tomb.ready_generation = ready.ready_generation
                   AND tomb.queue = ready.queue
                   AND tomb.priority = ready.priority
                   AND tomb.enqueue_shard = ready.enqueue_shard
                   AND tomb.lane_seq = ready.lane_seq
             )
            GROUP BY ready.kind, leases.queue
            "#
        ),
        KindCountSource::Deferred => format!(
            r#"
            SELECT kind, queue, count(*)::bigint AS n, 0::bigint AS n_recent
            FROM {schema}.deferred_jobs
            GROUP BY kind, queue
            "#
        ),
        KindCountSource::Done => format!(
            r#"
            SELECT
                kind,
                queue,
                count(*)::bigint AS n,
                count(*) FILTER (
                    WHERE state = 'completed'
                      AND finalized_at > clock_timestamp() - interval '1 hour'
                )::bigint AS n_recent
            FROM {schema}.done_entries
            GROUP BY kind, queue
            "#
        ),
        KindCountSource::Receipts => format!(
            r#"
            SELECT
                ready.kind,
                batch.queue,
                count(*)::bigint AS n,
                count(*) FILTER (
                    WHERE batch.finalized_at > clock_timestamp() - interval '1 hour'
                )::bigint AS n_recent
            FROM {schema}.receipt_completion_batches AS batch
            CROSS JOIN LATERAL unnest(
                batch.job_ids,
                batch.run_leases,
                batch.lane_seqs
            ) AS item(job_id, run_lease, lane_seq)
            CROSS JOIN LATERAL (
                SELECT ready.kind
                FROM {schema}.ready_entries AS ready
                WHERE ready.ready_slot = batch.ready_slot
                  AND ready.ready_generation = batch.ready_generation
                  AND ready.queue = batch.queue
                  AND ready.priority = batch.priority
                  AND ready.enqueue_shard = batch.enqueue_shard
                  AND ready.lane_seq = item.lane_seq
                  AND ready.job_id = item.job_id
                -- Keep the full member key in the parameterized lookup,
                -- rather than scanning its retained lane for every member.
                OFFSET 0
            ) AS ready
            WHERE NOT EXISTS (
                SELECT 1 FROM {schema}.receipt_completion_tombstones AS tomb
                WHERE tomb.ready_slot = batch.ready_slot
                  AND tomb.ready_generation = batch.ready_generation
                  AND tomb.job_id = item.job_id
                  AND tomb.run_lease = item.run_lease
            )
            GROUP BY ready.kind, batch.queue
            "#
        ),
        KindCountSource::Dlq => format!(
            r#"
            SELECT kind, queue, count(*)::bigint AS n, 0::bigint AS n_recent
            FROM {schema}.dlq_entries
            GROUP BY kind, queue
            "#
        ),
    }
}

// Merge small (kind, queue) aggregates from separately locked read sources.
fn queue_storage_kind_counts_cte() -> &'static str {
    r#"
        kind_queue AS (
            SELECT kind, queue, sum(n)::bigint AS n, sum(n_recent)::bigint AS n_recent
            FROM unnest($1::text[], $2::text[], $3::bigint[], $4::bigint[])
                AS sources(kind, queue, n, n_recent)
            GROUP BY kind, queue
        ),
        kind_counts AS (
            SELECT
                kind,
                sum(n)::bigint AS job_count,
                count(*)::bigint AS queue_count,
                sum(n_recent)::bigint AS completed_last_hour
            FROM kind_queue
            GROUP BY kind
        )
    "#
}

async fn notify_cancellation_tx<'a>(
    tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    job_id: i64,
    run_lease: i64,
) -> Result<(), AwaError> {
    let payload = serde_json::json!({ "job_id": job_id, "run_lease": run_lease }).to_string();
    sqlx::query("SELECT pg_notify('awa:cancel', $1)")
        .bind(payload)
        .execute(tx.as_mut())
        .await?;
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JobCandidateSource {
    States,
    Receipts(usize),
}

/// Candidate job ids for [`list_jobs`] from one source: the newest `limit`
/// distinct ids below `cursor`, with the statement's binds in order. State
/// sources apply kind/state/queue filters. Receipt sources enumerate open
/// claims in one physical slot and apply kind/queue before their limit.
///
/// Each place a job can live is read and limited on its own, with only the
/// state legs that can hold the requested state. Known filters are written
/// into the statement rather than as `$n IS NULL OR` terms that a generic plan
/// can't drop. Read through
/// `current_jobs`, Postgres joins every retained completion to a scan of the
/// ready rows, because that join and the `UNION ALL` are each estimated at one
/// row. Available rows are found per lane as a claim finds them, and a
/// completion batch's members are ordered by id before the ready primary key
/// is probed for each, so a page of newest ids probes only as many as it
/// returns.
fn queue_storage_job_id_candidates_sql(
    schema: &str,
    filter: &ListJobsFilter,
    cursor: Option<i64>,
    limit: i64,
    source: JobCandidateSource,
) -> (String, Vec<CandidateBind>) {
    let mut binds = Vec::new();
    let mut bind = |value: CandidateBind| {
        binds.push(value);
        format!("${}", binds.len())
    };
    let state = filter
        .state
        .filter(|_| source == JobCandidateSource::States)
        .map(|state| bind(CandidateBind::State(state)));
    let kind = filter
        .kind
        .as_ref()
        .map(|kind| bind(CandidateBind::Text(kind.clone())));
    let queue = filter
        .queue
        .as_ref()
        .map(|queue| bind(CandidateBind::Text(queue.clone())));
    let before = cursor.map(|cursor| bind(CandidateBind::Int(cursor)));
    let limit = bind(CandidateBind::Int(limit));

    // `AND column = $n` for each filter that is set; `kind` and `queue` name
    // the columns, which differ by leg.
    let predicates = |state_column: Option<&str>,
                      kind_column: Option<&str>,
                      queue_column: &str,
                      id_column: &str| {
        let mut sql = String::new();
        if let (Some(param), Some(column)) = (&state, state_column) {
            sql.push_str(&format!(" AND {column} = {param}::awa.job_state"));
        }
        if let (Some(param), Some(column)) = (&kind, kind_column) {
            sql.push_str(&format!(" AND {column} = {param}"));
        }
        if let Some(param) = &queue {
            sql.push_str(&format!(" AND {queue_column} = {param}"));
        }
        if let Some(param) = &before {
            sql.push_str(&format!(" AND {id_column} < {param}::bigint"));
        }
        sql
    };
    let holds = |states: &[JobState]| {
        source == JobCandidateSource::States
            && filter
                .state
                .is_none_or(|requested| states.contains(&requested))
    };

    let mut legs: Vec<String> = Vec::new();
    let mut with = String::new();
    if holds(&[JobState::Available]) {
        let lane_rows = queue_storage_available_lane_rows_sql(
            schema,
            "heads",
            "ready.job_id",
            &format!(
                "TRUE{}",
                predicates(None, Some("ready.kind"), "ready.queue", "ready.job_id")
            ),
        );
        let head_queue = queue
            .as_ref()
            .map(|param| format!("WHERE claims.queue = {param}"))
            .unwrap_or_default();
        with = format!(
            r#"
            WITH heads AS MATERIALIZED (
                SELECT
                    claims.queue,
                    claims.priority,
                    claims.enqueue_shard,
                    {schema}.sequence_next_value(claims.seq_name) AS claim_seq,
                    {schema}.sequence_next_value(enqueues.seq_name) AS next_seq
                FROM {schema}.queue_claim_heads AS claims
                JOIN {schema}.queue_enqueue_heads AS enqueues
                  ON enqueues.queue = claims.queue
                 AND enqueues.priority = claims.priority
                 AND enqueues.enqueue_shard = claims.enqueue_shard
                {head_queue}
            )
            "#
        );
        legs.push(format!(
            r#"(
                SELECT DISTINCT lane.job_id
                FROM heads
                CROSS JOIN LATERAL ({lane_rows}) AS lane
                ORDER BY lane.job_id DESC
                LIMIT {limit}
            )"#
        ));
    }
    if holds(&[JobState::Scheduled, JobState::Retryable]) {
        let predicates = predicates(Some("state"), Some("kind"), "queue", "job_id");
        legs.push(format!(
            r#"(
                SELECT DISTINCT job_id
                FROM {schema}.deferred_jobs
                WHERE TRUE{predicates}
                ORDER BY job_id DESC
                LIMIT {limit}
            )"#
        ));
    }
    if holds(&[JobState::Running, JobState::WaitingExternal]) {
        let predicates = predicates(
            Some("leases.state"),
            Some("ready.kind"),
            "leases.queue",
            "leases.job_id",
        );
        legs.push(format!(
            r#"(
                SELECT DISTINCT leases.job_id
                FROM {schema}.leases AS leases
                JOIN {schema}.ready_entries AS ready
                  ON ready.ready_slot = leases.ready_slot
                 AND ready.ready_generation = leases.ready_generation
                 AND ready.queue = leases.queue
                 -- Leases record the aged effective priority while the ready row
                 -- keeps its lane priority, so match the attempt instead.
                 AND ready.job_id = leases.job_id
                 AND ready.run_lease + 1 = leases.run_lease
                 AND ready.enqueue_shard = leases.enqueue_shard
                 AND ready.lane_seq = leases.lane_seq
                 AND NOT EXISTS (
                     SELECT 1 FROM {schema}.ready_tombstones AS tomb
                     WHERE tomb.ready_slot = ready.ready_slot
                       AND tomb.ready_generation = ready.ready_generation
                       AND tomb.queue = ready.queue
                       AND tomb.priority = ready.priority
                       AND tomb.enqueue_shard = ready.enqueue_shard
                       AND tomb.lane_seq = ready.lane_seq
                 )
                WHERE TRUE{predicates}
                ORDER BY leases.job_id DESC
                LIMIT {limit}
            )"#
        ));
    }
    if let JobCandidateSource::Receipts(slot) = source {
        let open_running =
            crate::queue_storage::open_receipt_running_claims_sql_in_slot(schema, slot);
        let kind_join = if kind.is_some() {
            format!(
                r#"
                CROSS JOIN LATERAL (
                    SELECT ready.kind
                    FROM {schema}.ready_entries AS ready
                    WHERE ready.job_id = open_claims.job_id
                      AND ready.run_lease + 1 = open_claims.run_lease
                      AND ready.queue = open_claims.queue
                      AND NOT EXISTS (
                          SELECT 1 FROM {schema}.ready_tombstones AS tomb
                          WHERE tomb.ready_slot = ready.ready_slot
                            AND tomb.ready_generation = ready.ready_generation
                            AND tomb.queue = ready.queue
                            AND tomb.priority = ready.priority
                            AND tomb.enqueue_shard = ready.enqueue_shard
                            AND tomb.lane_seq = ready.lane_seq
                      )
                    LIMIT 1
                ) AS created
                "#
            )
        } else {
            String::new()
        };
        let predicates = predicates(
            None,
            Some("created.kind"),
            "open_claims.queue",
            "open_claims.job_id",
        );
        legs.push(format!(
            r#"(
                SELECT DISTINCT open_claims.job_id
                FROM ({open_running}) AS open_claims
                {kind_join}
                WHERE TRUE{predicates}
                ORDER BY open_claims.job_id DESC
                LIMIT {limit}
            )"#
        ));
    }
    if holds(&[JobState::Completed, JobState::Failed, JobState::Cancelled]) {
        let predicates = predicates(Some("state"), Some("kind"), "queue", "job_id");
        legs.push(format!(
            r#"(
                SELECT DISTINCT job_id
                FROM {schema}.done_entries
                WHERE TRUE{predicates}
                ORDER BY job_id DESC
                LIMIT {limit}
            )"#
        ));
    }
    if holds(&[JobState::Completed]) {
        let members = predicates(None, None, "batch.queue", "item.job_id");
        let kind = kind
            .as_ref()
            .map(|param| format!(" AND ready.kind = {param}"))
            .unwrap_or_default();
        legs.push(format!(
            r#"(
                SELECT DISTINCT member.job_id
                FROM (
                    SELECT
                        batch.ready_slot,
                        batch.ready_generation,
                        batch.queue,
                        batch.priority,
                        batch.enqueue_shard,
                        item.job_id,
                        item.run_lease,
                        item.lane_seq
                    FROM {schema}.receipt_completion_batches AS batch
                    CROSS JOIN LATERAL unnest(
                        batch.job_ids,
                        batch.run_leases,
                        batch.lane_seqs
                    ) AS item(job_id, run_lease, lane_seq)
                    WHERE TRUE{members}
                    ORDER BY item.job_id DESC
                    OFFSET 0
                ) AS member
                CROSS JOIN LATERAL (
                    SELECT 1
                    FROM {schema}.ready_entries AS ready
                    WHERE ready.ready_slot = member.ready_slot
                      AND ready.ready_generation = member.ready_generation
                      AND ready.queue = member.queue
                      AND ready.priority = member.priority
                      AND ready.enqueue_shard = member.enqueue_shard
                      AND ready.lane_seq = member.lane_seq
                      AND ready.job_id = member.job_id{kind}
                      AND NOT EXISTS (
                          SELECT 1 FROM {schema}.receipt_completion_tombstones AS tomb
                          WHERE tomb.ready_slot = member.ready_slot
                            AND tomb.ready_generation = member.ready_generation
                            AND tomb.job_id = member.job_id
                            AND tomb.run_lease = member.run_lease
                      )
                    LIMIT 1
                ) AS live
                ORDER BY member.job_id DESC
                LIMIT {limit}
            )"#
        ));
    }
    if holds(&[JobState::Failed]) {
        let predicates = predicates(None, Some("kind"), "queue", "job_id");
        legs.push(format!(
            r#"(
                SELECT DISTINCT job_id
                FROM {schema}.dlq_entries
                WHERE TRUE{predicates}
                ORDER BY job_id DESC
                LIMIT {limit}
            )"#
        ));
    }

    let sql = format!(
        r#"
        {with}
        SELECT DISTINCT job_id
        FROM ({legs}) AS candidates
        ORDER BY job_id DESC
        LIMIT {limit}
        "#,
        legs = legs.join(" UNION ALL "),
    );
    (sql, binds)
}

/// A value bound to the statement [`queue_storage_job_id_candidates_sql`]
/// returns.
#[derive(Debug)]
enum CandidateBind {
    State(JobState),
    Text(String),
    Int(i64),
}

async fn fetch_queue_storage_job_candidates(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &str,
    binds: Vec<CandidateBind>,
) -> Result<Vec<i64>, sqlx::Error> {
    sqlx::query("SAVEPOINT awa_list_candidates")
        .execute(tx.as_mut())
        .await?;
    let mut query = sqlx::query_scalar::<_, i64>(sql);
    for value in binds {
        query = match value {
            CandidateBind::State(state) => query.bind(state),
            CandidateBind::Text(text) => query.bind(text),
            CandidateBind::Int(int) => query.bind(int),
        };
    }
    let ids = query.fetch_all(tx.as_mut()).await?;
    // Keep the outer repeatable-read snapshot, but release this read phase's
    // relation locks. Destroy the savepoint too rather than stacking names.
    sqlx::query("ROLLBACK TO SAVEPOINT awa_list_candidates")
        .execute(tx.as_mut())
        .await?;
    sqlx::query("RELEASE SAVEPOINT awa_list_candidates")
        .execute(tx.as_mut())
        .await?;
    Ok(ids)
}

async fn list_queue_storage_jobs(
    store: &QueueStorage,
    pool: &PgPool,
    filter: &ListJobsFilter,
) -> Result<Vec<JobRow>, AwaError> {
    let limit = filter.limit.unwrap_or(100).clamp(1, 1000);
    let candidate_limit = if filter.tag.is_some() {
        limit.saturating_mul(10).min(5000)
    } else {
        limit.saturating_mul(3).min(2000)
    };

    let mut jobs = Vec::new();
    let mut cursor = filter.before_id;

    loop {
        // Every candidate phase sees the same MVCC snapshot. Each phase
        // releases its own read locks before the next one opens more tables.
        let mut begin_guard = crate::read_transaction::BeginGuard::acquire(pool).await?;
        let mut tx = sqlx::Connection::begin_with(
            &mut *begin_guard.connection,
            "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        )
        .await?;
        begin_guard.begin_pending = false;
        sqlx::query("SET LOCAL jit = off")
            .execute(tx.as_mut())
            .await?;
        let states = filter.state.map_or_else(
            || {
                vec![
                    JobState::Available,
                    JobState::Scheduled,
                    JobState::Retryable,
                    JobState::Running,
                    JobState::WaitingExternal,
                    JobState::Completed,
                    JobState::Failed,
                    JobState::Cancelled,
                ]
            },
            |state| vec![state],
        );
        let mut ids = BTreeSet::new();
        for state in states {
            let mut phase_filter = filter.clone();
            phase_filter.state = Some(state);
            let (sql, binds) = queue_storage_job_id_candidates_sql(
                store.schema(),
                &phase_filter,
                cursor,
                candidate_limit,
                JobCandidateSource::States,
            );
            ids.extend(fetch_queue_storage_job_candidates(&mut tx, &sql, binds).await?);
        }
        if filter.state.is_none_or(|state| state == JobState::Running) {
            for slot in queue_storage_populated_claim_slots(&mut tx, store).await? {
                let (sql, binds) = queue_storage_job_id_candidates_sql(
                    store.schema(),
                    filter,
                    cursor,
                    candidate_limit,
                    JobCandidateSource::Receipts(slot),
                );
                ids.extend(fetch_queue_storage_job_candidates(&mut tx, &sql, binds).await?);
            }
        }
        tx.commit().await?;
        drop(begin_guard);
        let ids: Vec<i64> = ids
            .into_iter()
            .rev()
            .take(candidate_limit as usize)
            .collect();
        if ids.is_empty() {
            break;
        }

        for job_id in &ids {
            let Some(job) = store.load_job(pool, *job_id).await? else {
                continue;
            };

            if let Some(state) = filter.state {
                if job.state != state {
                    continue;
                }
            }
            if let Some(kind) = &filter.kind {
                if &job.kind != kind {
                    continue;
                }
            }
            if let Some(queue) = &filter.queue {
                if &job.queue != queue {
                    continue;
                }
            }
            if let Some(tag) = &filter.tag {
                if !job.tags.iter().any(|job_tag| job_tag == tag) {
                    continue;
                }
            }

            jobs.push(job);
            if jobs.len() as i64 >= limit {
                break;
            }
        }

        if jobs.len() as i64 >= limit || ids.len() < candidate_limit as usize {
            break;
        }
        cursor = ids.last().copied();
    }

    jobs.sort_by_key(|job| std::cmp::Reverse(job.id));
    Ok(jobs)
}

/// Retry a single failed, cancelled, or waiting_external job.
pub async fn retry(pool: &PgPool, job_id: i64) -> Result<Option<JobRow>, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        return store
            .retry_job(pool, job_id)
            .await?
            .ok_or(AwaError::JobNotFound { id: job_id })
            .map(Some);
    }

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
    .fetch_optional(pool)
    .await?
    .ok_or(AwaError::JobNotFound { id: job_id })
    .map(Some)
}

/// Cancel a single non-terminal job.
pub async fn cancel(pool: &PgPool, job_id: i64) -> Result<Option<JobRow>, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        return store
            .cancel_job(pool, job_id)
            .await?
            .ok_or(AwaError::JobNotFound { id: job_id })
            .map(Some);
    }

    // Cancel the row and capture its prior state. If we moved it out
    // of `running` / `waiting_external`, NOTIFY listening workers so
    // the handler currently executing it can learn about the
    // cancellation and stop cleanly.
    let mut tx = pool.begin().await?;

    let prior_state: Option<JobState> =
        sqlx::query_scalar::<_, JobState>("SELECT state FROM awa.jobs WHERE id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_optional(tx.as_mut())
            .await?;

    let Some(prior_state) = prior_state else {
        tx.rollback().await.ok();
        return Err(AwaError::JobNotFound { id: job_id });
    };

    let job: Option<JobRow> = sqlx::query_as::<_, JobRow>(
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
    .fetch_optional(tx.as_mut())
    .await?;

    let Some(job) = job else {
        tx.rollback().await.ok();
        return Err(AwaError::JobNotFound { id: job_id });
    };

    if matches!(prior_state, JobState::Running | JobState::WaitingExternal) {
        notify_cancellation_tx(&mut tx, job.id, job.run_lease).await?;
    }

    tx.commit().await?;
    Ok(Some(job))
}

/// Transaction-participating variant of [`cancel`].
///
/// Runs the cancellation on the caller-provided connection `conn` rather than
/// acquiring its own from a pool. When `conn` is already inside a transaction —
/// the common case, and a `&mut Transaction` deref-coerces to `&mut PgConnection`
/// — the cancel commits or rolls back atomically with the caller's other work,
/// and the cooperative `awa:cancel` NOTIFY to a running worker fires only when the
/// caller commits. When `conn` is a standalone pooled connection the cancel runs
/// in its own transaction.
///
/// Internally the cancel runs in a nested transaction — a `SAVEPOINT` when `conn`
/// is already in a transaction — so a cancel error rolls back only the cancel
/// rather than poisoning the caller's transaction.
///
/// On the queue-storage engine this skips the best-effort claim-cursor advance
/// that [`cancel`] performs after its own commit (it cannot run inside the
/// caller's transaction); the derived queue depth may briefly over-count by one
/// until later committed rows on the lane are claimed. Counts are unaffected on
/// the canonical engine.
pub async fn cancel_tx(
    conn: &mut sqlx::PgConnection,
    job_id: i64,
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = conn.begin().await?;
    match cancel_in_tx(&mut tx, job_id).await {
        Ok(row) => {
            tx.commit().await?;
            Ok(row)
        }
        Err(err) => {
            tx.rollback().await.ok();
            Err(err)
        }
    }
}

/// Inner cancellation logic, run on an existing transaction. Shared by
/// [`cancel_tx`] and [`cancel_by_unique_key_tx`].
async fn cancel_in_tx<'a>(
    tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    job_id: i64,
) -> Result<Option<JobRow>, AwaError> {
    if let Some(store) = active_queue_storage_in_tx(tx).await? {
        return store
            .cancel_job_in_tx(tx, job_id)
            .await?
            .ok_or(AwaError::JobNotFound { id: job_id })
            .map(Some);
    }

    let prior_state: Option<JobState> =
        sqlx::query_scalar::<_, JobState>("SELECT state FROM awa.jobs WHERE id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_optional(tx.as_mut())
            .await?;

    let Some(prior_state) = prior_state else {
        return Err(AwaError::JobNotFound { id: job_id });
    };

    let job: Option<JobRow> = sqlx::query_as::<_, JobRow>(
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
    .fetch_optional(tx.as_mut())
    .await?;

    let Some(job) = job else {
        return Err(AwaError::JobNotFound { id: job_id });
    };

    if matches!(prior_state, JobState::Running | JobState::WaitingExternal) {
        notify_cancellation_tx(tx, job.id, job.run_lease).await?;
    }

    Ok(Some(job))
}

/// Queue-storage candidate lookup for a unique key: the oldest nonterminal
/// job holding key `$1`. Shared by [`cancel_by_unique_key`] and
/// [`cancel_by_unique_key_tx`] so the two cannot drift.
///
/// Each source is filtered by the key where it is read, so a missing key
/// costs no more than reading the live work:
/// - available rows are walked lane by lane through `ready_segments` from the
///   claim cursor, the way a claim finds them, and probed along the ready key
///   within each segment's ring slot. The ready key leads with the ring slot,
///   so a lane range without it scans every retained ready row, completed
///   jobs included;
/// - scheduled and retryable rows use the `deferred_jobs` unique-key index;
/// - running and waiting jobs are read from `leases` and from open receipt
///   claims that have not been materialised into `leases`.
///
/// Without a queue, the open receipt claims are read in full before the key
/// filter, so this lookup costs what the claim ring retains. With `scoped`
/// see [`scoped_unique_key_candidate_sql`], which starts from the key.
fn unique_key_candidate_sql(schema: &str, scoped: bool) -> String {
    if scoped {
        return scoped_unique_key_candidate_sql(schema);
    }
    let heads_scope = if scoped {
        "WHERE claims.queue = ANY($2)"
    } else {
        ""
    };
    let open_running = crate::queue_storage::open_receipt_running_claims_sql(schema);
    format!(
        r#"
        WITH heads AS (
            SELECT
                claims.queue,
                claims.priority,
                claims.enqueue_shard,
                {schema}.sequence_next_value(claims.seq_name) AS claim_seq,
                {schema}.sequence_next_value(enqueues.seq_name) AS next_seq
            FROM {schema}.queue_claim_heads AS claims
            JOIN {schema}.queue_enqueue_heads AS enqueues
              ON enqueues.queue = claims.queue
             AND enqueues.priority = claims.priority
             AND enqueues.enqueue_shard = claims.enqueue_shard
            {heads_scope}
        ),
        candidates AS (
            SELECT ready.job_id
            FROM heads
            CROSS JOIN LATERAL (
                SELECT
                    segment.ready_slot,
                    segment.ready_generation,
                    GREATEST(segment.first_lane_seq, heads.claim_seq) AS from_seq,
                    LEAST(segment.next_lane_seq, heads.next_seq) AS to_seq
                FROM {schema}.ready_segments AS segment
                WHERE segment.queue = heads.queue
                  AND segment.priority = heads.priority
                  AND segment.enqueue_shard = heads.enqueue_shard
                  AND segment.next_lane_seq > heads.claim_seq
                  AND segment.first_lane_seq < heads.next_seq
                ORDER BY segment.next_lane_seq
            ) AS segment
            -- OFFSET 0 keeps this a per-segment probe along the ready key;
            -- flattened, the key filter can lead the planner to scan every
            -- retained ready row instead.
            CROSS JOIN LATERAL (
                SELECT ready.*
                FROM {schema}.ready_entries AS ready
                WHERE ready.ready_slot = segment.ready_slot
                  AND ready.ready_generation = segment.ready_generation
                  AND ready.queue = heads.queue
                  AND ready.priority = heads.priority
                  AND ready.enqueue_shard = heads.enqueue_shard
                  AND ready.lane_seq >= segment.from_seq
                  AND ready.lane_seq < segment.to_seq
                  AND ready.unique_key = $1
                OFFSET 0
            ) AS ready
            WHERE NOT EXISTS (
                SELECT 1 FROM {schema}.ready_tombstones AS tomb
                WHERE tomb.ready_slot = ready.ready_slot
                  AND tomb.ready_generation = ready.ready_generation
                  AND tomb.queue = ready.queue
                  AND tomb.priority = ready.priority
                  AND tomb.enqueue_shard = ready.enqueue_shard
                  AND tomb.lane_seq = ready.lane_seq
            )
            UNION ALL
            SELECT job_id
            FROM {schema}.deferred_jobs
            WHERE unique_key = $1
            UNION ALL
            -- A running job's unique_key is stored on its ready_entries row;
            -- reach it through the lease's lane identity.
            SELECT leases.job_id
            FROM {schema}.leases AS leases
            JOIN LATERAL (
                SELECT 1
                FROM {schema}.ready_entries AS ready
                WHERE ready.ready_slot = leases.ready_slot
                  AND ready.ready_generation = leases.ready_generation
                  AND ready.queue = leases.queue
                  -- Leases record the aged effective priority while the ready row
                  -- keeps its lane priority, so match the attempt instead.
                  AND ready.job_id = leases.job_id
                  AND ready.run_lease + 1 = leases.run_lease
                  AND ready.enqueue_shard = leases.enqueue_shard
                  AND ready.lane_seq = leases.lane_seq
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.ready_tombstones AS tomb
                      WHERE tomb.ready_slot = ready.ready_slot
                        AND tomb.ready_generation = ready.ready_generation
                        AND tomb.queue = ready.queue
                        AND tomb.priority = ready.priority
                        AND tomb.enqueue_shard = ready.enqueue_shard
                        AND tomb.lane_seq = ready.lane_seq
                  )
                  AND ready.unique_key = $1
                OFFSET 0
            ) AS ready ON TRUE
            UNION ALL
            -- Running receipt claims not yet materialised into `leases`.
            -- This leg reads the claims still in the claim ring (closed claim
            -- batches are skipped whole) before the key filter, so its cost
            -- follows claim-ring retention, not completed-job history.
            SELECT open_claims.job_id
            FROM ({open_running}) AS open_claims
            JOIN LATERAL (
                SELECT 1
                FROM {schema}.ready_entries AS ready
                WHERE ready.job_id = open_claims.job_id
                  AND ready.unique_key = $1
                LIMIT 1
            ) AS ready ON TRUE
        )
        SELECT job_id
        FROM candidates
        ORDER BY job_id ASC
        LIMIT 1
        "#
    )
}

/// The queue-scoped candidate lookup: starts from the queue's ready rows that
/// hold the key, read through the lane index, then keeps the ones still live.
/// Its cost follows that queue's retained rows, not other queues' backlogs or
/// the claim ring, which a large backlog elsewhere can keep from pruning.
///
/// - available: at or past the lane's claim cursor and not tombstoned;
/// - scheduled and retryable: the `deferred_jobs` unique-key index;
/// - running or waiting: in `leases` or on an open receipt claim, both
///   checked by `job_id` for just these jobs.
///
/// `$2` holds the physical queues of the logical queue.
fn scoped_unique_key_candidate_sql(schema: &str) -> String {
    let open_running = crate::queue_storage::open_receipt_running_claims_sql_for(
        schema,
        Some("ARRAY(SELECT keyed.job_id FROM keyed)"),
    );
    format!(
        r#"
        WITH keyed AS MATERIALIZED (
            SELECT
                ready.job_id,
                ready.ready_slot,
                ready.ready_generation,
                ready.queue,
                ready.priority,
                ready.enqueue_shard,
                ready.lane_seq
            FROM {schema}.ready_entries AS ready
            WHERE ready.queue = ANY($2)
              AND ready.unique_key = $1
        ),
        candidates AS (
            SELECT keyed.job_id
            FROM keyed
            JOIN {schema}.queue_claim_heads AS claims
              ON claims.queue = keyed.queue
             AND claims.priority = keyed.priority
             AND claims.enqueue_shard = keyed.enqueue_shard
            WHERE keyed.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.ready_tombstones AS tomb
                  WHERE tomb.ready_slot = keyed.ready_slot
                    AND tomb.ready_generation = keyed.ready_generation
                    AND tomb.queue = keyed.queue
                    AND tomb.priority = keyed.priority
                    AND tomb.enqueue_shard = keyed.enqueue_shard
                    AND tomb.lane_seq = keyed.lane_seq
              )
            UNION ALL
            SELECT job_id
            FROM {schema}.deferred_jobs
            WHERE unique_key = $1
            UNION ALL
            -- By job id rather than lane: a job moved to another queue
            -- while available is leased on its new lane.
            SELECT keyed.job_id
            FROM keyed
            WHERE EXISTS (
                SELECT 1 FROM {schema}.leases AS lease
                WHERE lease.job_id = keyed.job_id
            )
            UNION ALL
            SELECT open_claims.job_id
            FROM ({open_running}) AS open_claims
        )
        SELECT job_id
        FROM candidates
        ORDER BY job_id ASC
        LIMIT 1
        "#
    )
}

/// The physical queues that hold a logical queue's lanes, read from the
/// schema's `queue_lanes`: the queue itself and its `queue#N` stripes. The
/// admin store is rebuilt from the schema name alone, so its configured
/// stripe count is the default, not the one the runtime enqueues with.
async fn persisted_physical_queues(
    conn: &mut sqlx::PgConnection,
    schema: &str,
    queue: &str,
) -> Result<Vec<String>, AwaError> {
    let mut queues: Vec<String> = sqlx::query_scalar(&format!(
        r#"
        SELECT DISTINCT lanes.queue
        FROM {schema}.queue_lanes AS lanes
        WHERE left(lanes.queue, char_length($1) + 1) = $1 || '#'
          AND substr(lanes.queue, char_length($1) + 2) ~ '^[0-9]+$'
        "#
    ))
    .bind(queue)
    .fetch_all(&mut *conn)
    .await?;
    queues.push(queue.to_string());
    Ok(queues)
}

/// The oldest nonterminal queue-storage job holding `unique_key`.
///
/// A queue is part of a key only when it was inserted with `by_queue`, so a
/// job holding a queue-scoped key was enqueued there: with a queue, its ready
/// rows are read from that queue alone. A job moved to another queue is
/// found through its original ready row while that row is retained, except
/// while it is available in the new queue; cancel it by id otherwise.
async fn queue_storage_unique_key_candidate(
    conn: &mut sqlx::PgConnection,
    store: &QueueStorage,
    unique_key: &[u8],
    queue: Option<&str>,
) -> Result<Option<i64>, AwaError> {
    let schema = store.schema();
    let Some(queue) = queue else {
        let sql = unique_key_candidate_sql(schema, false);
        return Ok(sqlx::query_scalar(&sql)
            .bind(unique_key)
            .fetch_optional(&mut *conn)
            .await?);
    };
    let queues = persisted_physical_queues(&mut *conn, schema, queue).await?;
    // Available, running and waiting jobs all have a ready row in the queue.
    // Without one, only a scheduled or retryable job can hold the key, and
    // the deferred index answers that alone; the full lookup spans enough
    // partitioned tables that planning it costs several milliseconds.
    // One statement, so a job promoted from deferred_jobs to a ready row
    // between the two reads cannot be missed by both.
    let (has_ready_rows, deferred): (bool, Option<i64>) = sqlx::query_as(&format!(
        "SELECT EXISTS (SELECT 1 FROM {schema}.ready_entries \
                        WHERE queue = ANY($2) AND unique_key = $1), \
                (SELECT job_id FROM {schema}.deferred_jobs WHERE unique_key = $1 \
                 ORDER BY job_id LIMIT 1)"
    ))
    .bind(unique_key)
    .bind(&queues)
    .fetch_one(&mut *conn)
    .await?;
    if !has_ready_rows {
        return Ok(deferred);
    }
    let sql = unique_key_candidate_sql(schema, true);
    Ok(sqlx::query_scalar(&sql)
        .bind(unique_key)
        .bind(&queues)
        .fetch_optional(&mut *conn)
        .await?)
}

/// Canonical (non queue-storage) candidate lookup for a unique key.
const UNIQUE_KEY_CANDIDATE_SQL_CANONICAL: &str = r#"
    WITH candidates AS (
        SELECT id FROM awa.jobs_hot
        WHERE unique_key = $1 AND state NOT IN ('completed', 'failed', 'cancelled')
        UNION ALL
        SELECT id FROM awa.scheduled_jobs
        WHERE unique_key = $1 AND state NOT IN ('completed', 'failed', 'cancelled')
        ORDER BY id ASC
        LIMIT 1
    )
    SELECT id
    FROM candidates
    "#;

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
/// views. The CTE selects the oldest candidate ID without row locks, then
/// delegates to `cancel(...)` so running jobs also emit the cooperative
/// in-flight cancellation notification. If the selected job completes before
/// `cancel(...)` locks it, the cancel becomes a no-op and returns `None`.
///
/// On the canonical engine the lookup scans `unique_key` on both physical
/// tables without a dedicated index. On queue storage, scheduled and
/// retryable rows are found by their unique-key index. With a `queue`, the
/// lookup reads that queue's ready rows (and its `queue#N` stripes') through
/// the lane index, retained ones included, and checks only the rows holding
/// the key for being available, leased or on an open receipt claim; its cost
/// follows that queue's own retained rows, not other queues' backlogs.
/// Without a `queue`, available rows are walked lane by lane from the claim
/// cursor and open receipt claims are read in full, so the cost follows what
/// the claim ring retains. A job moved to another queue while available is
/// not found by a lookup that names its original queue.
pub async fn cancel_by_unique_key(
    pool: &PgPool,
    kind: &str,
    queue: Option<&str>,
    args: Option<&serde_json::Value>,
    period_bucket: Option<i64>,
) -> Result<Option<JobRow>, AwaError> {
    let unique_key = crate::unique::compute_unique_key(kind, queue, args, period_bucket);

    if let Some(store) = active_queue_storage(pool).await? {
        let mut conn = pool.acquire().await?;
        let candidate =
            queue_storage_unique_key_candidate(&mut conn, &store, &unique_key, queue).await?;
        drop(conn);

        return match candidate {
            Some(job_id) => match cancel(pool, job_id).await {
                Err(AwaError::JobNotFound { .. }) => Ok(None),
                other => other,
            },
            None => Ok(None),
        };
    }

    // Find the oldest matching job across both physical tables, then route
    // through `cancel(...)` so running jobs emit the same cooperative
    // cancellation notification as ID-based admin cancels.
    let candidate: Option<i64> = sqlx::query_scalar(UNIQUE_KEY_CANDIDATE_SQL_CANONICAL)
        .bind(&unique_key)
        .fetch_optional(pool)
        .await?;

    match candidate {
        Some(job_id) => match cancel(pool, job_id).await {
            Ok(row) => Ok(row),
            Err(AwaError::JobNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        },
        None => Ok(None),
    }
}

/// Transaction-participating variant of [`cancel_by_unique_key`].
///
/// Resolves the candidate and cancels it on the caller-provided connection
/// `conn`, so the cancel is atomic with the caller's other work (and the
/// `awa:cancel` NOTIFY fires only on the caller's commit). A `&mut Transaction`
/// deref-coerces to `&mut PgConnection`. See [`cancel_tx`] for the
/// nested-transaction and queue-storage claim-cursor caveats.
pub async fn cancel_by_unique_key_tx(
    conn: &mut sqlx::PgConnection,
    kind: &str,
    queue: Option<&str>,
    args: Option<&serde_json::Value>,
    period_bucket: Option<i64>,
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = conn.begin().await?;
    match cancel_by_unique_key_in_tx(&mut tx, kind, queue, args, period_bucket).await {
        Ok(row) => {
            tx.commit().await?;
            Ok(row)
        }
        Err(err) => {
            tx.rollback().await.ok();
            Err(err)
        }
    }
}

async fn cancel_by_unique_key_in_tx<'a>(
    tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    kind: &str,
    queue: Option<&str>,
    args: Option<&serde_json::Value>,
    period_bucket: Option<i64>,
) -> Result<Option<JobRow>, AwaError> {
    let unique_key = crate::unique::compute_unique_key(kind, queue, args, period_bucket);

    if let Some(store) = active_queue_storage_in_tx(tx).await? {
        let candidate =
            queue_storage_unique_key_candidate(tx.as_mut(), &store, &unique_key, queue).await?;

        return match candidate {
            Some(job_id) => match cancel_in_tx(tx, job_id).await {
                Err(AwaError::JobNotFound { .. }) => Ok(None),
                other => other,
            },
            None => Ok(None),
        };
    }

    let candidate: Option<i64> = sqlx::query_scalar(UNIQUE_KEY_CANDIDATE_SQL_CANONICAL)
        .bind(&unique_key)
        .fetch_optional(tx.as_mut())
        .await?;

    match candidate {
        Some(job_id) => match cancel_in_tx(tx, job_id).await {
            Ok(row) => Ok(row),
            Err(AwaError::JobNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        },
        None => Ok(None),
    }
}

/// Result of a bulk failed-job retry ([`retry_failed_by_kind`] /
/// [`retry_failed_by_queue`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryFailedOutcome {
    /// Jobs actually transitioned back to runnable state.
    pub retried: Vec<JobRow>,
    /// Failed jobs matched by the scan that selected retry candidates.
    /// `matched - retried.len()` jobs raced to another state or were
    /// pruned between the scan and the retry.
    pub matched: u64,
    /// Cumulative count of failed rows pruned past the retention floor
    /// for the selected queue, summed over
    /// `queue_terminal_rollups` priorities. These rows can no longer be
    /// retried. `None` when the count cannot be scoped to the
    /// selection: kind-based retries (rollups carry no kind dimension)
    /// and the canonical (non queue-storage) backend.
    pub pruned_failed_count: Option<u64>,
}

/// Retry all failed jobs of a given kind.
pub async fn retry_failed_by_kind(
    pool: &PgPool,
    kind: &str,
) -> Result<RetryFailedOutcome, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        let sql = format!(
            r#"
            SELECT DISTINCT job_id
            FROM {schema}.done_entries
            WHERE kind = $1
              AND state = 'failed'
            ORDER BY job_id ASC
            "#,
            schema = store.schema()
        );
        let ids: Vec<i64> = sqlx::query_scalar(&sql).bind(kind).fetch_all(pool).await?;
        let (retried, matched) = store.retry_jobs_by_ids(pool, &ids).await?;
        return Ok(RetryFailedOutcome {
            retried,
            matched,
            pruned_failed_count: None,
        });
    }

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
    .fetch_all(pool)
    .await?;

    Ok(RetryFailedOutcome {
        matched: rows.len() as u64,
        retried: rows,
        pruned_failed_count: None,
    })
}

/// Retry all failed jobs in a given queue.
pub async fn retry_failed_by_queue(
    pool: &PgPool,
    queue: &str,
) -> Result<RetryFailedOutcome, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        let sql = format!(
            r#"
            SELECT DISTINCT job_id
            FROM {schema}.done_entries
            WHERE queue = $1
              AND state = 'failed'
            ORDER BY job_id ASC
            "#,
            schema = store.schema()
        );
        let ids: Vec<i64> = sqlx::query_scalar(&sql).bind(queue).fetch_all(pool).await?;
        let (retried, matched) = store.retry_jobs_by_ids(pool, &ids).await?;
        let pruned_failed_count = store.pruned_failed_count_for_queue(pool, queue).await?;
        return Ok(RetryFailedOutcome {
            retried,
            matched,
            pruned_failed_count: Some(pruned_failed_count),
        });
    }

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
    .fetch_all(pool)
    .await?;

    Ok(RetryFailedOutcome {
        matched: rows.len() as u64,
        retried: rows,
        pruned_failed_count: None,
    })
}

/// Discard (delete) all failed jobs of a given kind.
pub async fn discard_failed(pool: &PgPool, kind: &str) -> Result<u64, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        return store.discard_failed_by_kind(pool, kind).await;
    }

    let result = sqlx::query("DELETE FROM awa.jobs WHERE kind = $1 AND state = 'failed'")
        .bind(kind)
        .execute(pool)
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
///
/// On queue storage this also drains the queue's stripes: queues named
/// `<queue>#<digits>`. Jobs enqueued after the drain starts are left alone.
pub async fn drain_queue(pool: &PgPool, queue: &str) -> Result<u64, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        return store.drain_queue(pool, queue).await;
    }

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
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// Code-declared, operator-facing descriptor for a queue.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueueDescriptor {
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub docs_url: Option<String>,
    pub tags: Vec<String>,
    pub extra: serde_json::Value,
}

impl Default for QueueDescriptor {
    fn default() -> Self {
        Self {
            display_name: None,
            description: None,
            owner: None,
            docs_url: None,
            tags: Vec::new(),
            extra: serde_json::json!({}),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NamedQueueDescriptor {
    pub queue: String,
    #[serde(flatten)]
    pub descriptor: QueueDescriptor,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobKindDescriptor {
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub docs_url: Option<String>,
    pub tags: Vec<String>,
    pub extra: serde_json::Value,
}

impl Default for JobKindDescriptor {
    fn default() -> Self {
        Self {
            display_name: None,
            description: None,
            owner: None,
            docs_url: None,
            tags: Vec::new(),
            extra: serde_json::json!({}),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NamedJobKindDescriptor {
    pub kind: String,
    #[serde(flatten)]
    pub descriptor: JobKindDescriptor,
}

/// Columns bound per descriptor row. Must match the order used in
/// [`build_descriptor_upsert`] and the bind loop below.
const DESCRIPTOR_PARAMS_PER_ROW: u32 = 9;

/// Postgres caps bound parameters at 65535 per statement. 9 params × 7000 rows
/// = 63k, so chunk below that with a comfortable margin.
const DESCRIPTOR_BATCH_SIZE: usize = 5000;

/// Build a batched INSERT ... VALUES (...), (...), ... ON CONFLICT upsert for
/// a descriptor catalog. `table` is `awa.queue_descriptors` or
/// `awa.job_kind_descriptors`; `pk` is `queue` or `kind`.
fn build_descriptor_upsert(table: &str, pk: &str, count: usize) -> String {
    let mut query = format!(
        "INSERT INTO {table} (\
             {pk}, display_name, description, owner, docs_url, tags, extra, \
             descriptor_hash, sync_interval_ms, created_at, updated_at, last_seen_at\
         ) VALUES "
    );
    let mut param_index = 1u32;
    for i in 0..count {
        if i > 0 {
            query.push_str(", ");
        }
        query.push_str(&format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, now(), now(), now())",
            param_index,
            param_index + 1,
            param_index + 2,
            param_index + 3,
            param_index + 4,
            param_index + 5,
            param_index + 6,
            param_index + 7,
            param_index + 8,
        ));
        param_index += DESCRIPTOR_PARAMS_PER_ROW;
    }
    // updated_at only advances when the hash changes — so an untouched
    // descriptor keeps its original `updated_at` timestamp across ticks
    // but still gets a fresh `last_seen_at` for liveness.
    query.push_str(&format!(
        " ON CONFLICT ({pk}) DO UPDATE SET \
             display_name = EXCLUDED.display_name, \
             description = EXCLUDED.description, \
             owner = EXCLUDED.owner, \
             docs_url = EXCLUDED.docs_url, \
             tags = EXCLUDED.tags, \
             extra = EXCLUDED.extra, \
             descriptor_hash = EXCLUDED.descriptor_hash, \
             sync_interval_ms = EXCLUDED.sync_interval_ms, \
             updated_at = CASE \
                 WHEN {table}.descriptor_hash IS DISTINCT FROM EXCLUDED.descriptor_hash \
                 THEN now() ELSE {table}.updated_at \
             END, \
             last_seen_at = now()"
    ));
    query
}

/// Upsert queue descriptors declared by the worker runtime.
///
/// Descriptor rows are part of the control-plane catalog and intentionally
/// separate from mutable queue runtime state in `awa.queue_meta`.
///
/// Descriptors are upserted in batched multi-row statements — one round-trip
/// per [`DESCRIPTOR_BATCH_SIZE`] descriptors rather than one per descriptor.
/// For typical fleets (≤100 queues / ≤500 kinds) that collapses to a single
/// round-trip per sync call.
pub async fn sync_queue_descriptors(
    pool: &PgPool,
    descriptors: &[NamedQueueDescriptor],
    sync_interval: std::time::Duration,
) -> Result<(), AwaError> {
    if descriptors.is_empty() {
        return Ok(());
    }
    let sync_interval_ms = sync_interval.as_millis() as i64;

    for chunk in descriptors.chunks(DESCRIPTOR_BATCH_SIZE) {
        let hashes: Vec<String> = chunk
            .iter()
            .map(|named| named.descriptor.descriptor_hash())
            .collect();
        let sql = build_descriptor_upsert("awa.queue_descriptors", "queue", chunk.len());
        let mut query = sqlx::query(&sql);
        for (named, hash) in chunk.iter().zip(hashes.iter()) {
            query = query
                .bind(&named.queue)
                .bind(named.descriptor.display_name.as_deref())
                .bind(named.descriptor.description.as_deref())
                .bind(named.descriptor.owner.as_deref())
                .bind(named.descriptor.docs_url.as_deref())
                .bind(&named.descriptor.tags)
                .bind(&named.descriptor.extra)
                .bind(hash.as_str())
                .bind(sync_interval_ms);
        }
        query.execute(pool).await?;
    }

    Ok(())
}

/// Upsert job-kind descriptors declared by the worker runtime. Batched the
/// same way as [`sync_queue_descriptors`].
pub async fn sync_job_kind_descriptors(
    pool: &PgPool,
    descriptors: &[NamedJobKindDescriptor],
    sync_interval: std::time::Duration,
) -> Result<(), AwaError> {
    if descriptors.is_empty() {
        return Ok(());
    }
    let sync_interval_ms = sync_interval.as_millis() as i64;

    for chunk in descriptors.chunks(DESCRIPTOR_BATCH_SIZE) {
        let hashes: Vec<String> = chunk
            .iter()
            .map(|named| named.descriptor.descriptor_hash())
            .collect();
        let sql = build_descriptor_upsert("awa.job_kind_descriptors", "kind", chunk.len());
        let mut query = sqlx::query(&sql);
        for (named, hash) in chunk.iter().zip(hashes.iter()) {
            query = query
                .bind(&named.kind)
                .bind(named.descriptor.display_name.as_deref())
                .bind(named.descriptor.description.as_deref())
                .bind(named.descriptor.owner.as_deref())
                .bind(named.descriptor.docs_url.as_deref())
                .bind(&named.descriptor.tags)
                .bind(&named.descriptor.extra)
                .bind(hash.as_str())
                .bind(sync_interval_ms);
        }
        query.execute(pool).await?;
    }

    Ok(())
}

impl QueueDescriptor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn display_name(mut self, display_name: impl Into<String>) -> Self {
        self.display_name = Some(display_name.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    pub fn docs_url(mut self, docs_url: impl Into<String>) -> Self {
        self.docs_url = Some(docs_url.into());
        self
    }

    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags = tags.into_iter().map(Into::into).collect();
        self
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    pub fn extra(mut self, extra: serde_json::Value) -> Self {
        self.extra = extra;
        self
    }

    pub fn descriptor_hash(&self) -> String {
        let payload = serde_json::json!({
            "display_name": self.display_name,
            "description": self.description,
            "owner": self.owner,
            "docs_url": self.docs_url,
            "tags": self.tags,
            "extra": canonicalize_json(&self.extra),
        });
        let encoded = serde_json::to_vec(&payload)
            .expect("queue descriptor JSON serialization should not fail");
        hex::encode(blake3::hash(&encoded).as_bytes())
    }
}

impl JobKindDescriptor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn display_name(mut self, display_name: impl Into<String>) -> Self {
        self.display_name = Some(display_name.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    pub fn docs_url(mut self, docs_url: impl Into<String>) -> Self {
        self.docs_url = Some(docs_url.into());
        self
    }

    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags = tags.into_iter().map(Into::into).collect();
        self
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    pub fn extra(mut self, extra: serde_json::Value) -> Self {
        self.extra = extra;
        self
    }

    pub fn descriptor_hash(&self) -> String {
        let payload = serde_json::json!({
            "display_name": self.display_name,
            "description": self.description,
            "owner": self.owner,
            "docs_url": self.docs_url,
            "tags": self.tags,
            "extra": canonicalize_json(&self.extra),
        });
        let encoded = serde_json::to_vec(&payload)
            .expect("job kind descriptor JSON serialization should not fail");
        hex::encode(blake3::hash(&encoded).as_bytes())
    }
}

fn canonicalize_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().cloned().collect();
            keys.sort();
            let mut out = serde_json::Map::with_capacity(map.len());
            for key in keys {
                if let Some(child) = map.get(&key) {
                    out.insert(key, canonicalize_json(child));
                }
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonicalize_json).collect())
        }
        _ => value.clone(),
    }
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct QueueOverview {
    pub queue: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub docs_url: Option<String>,
    pub tags: Vec<String>,
    pub extra: serde_json::Value,
    pub descriptor_last_seen_at: Option<DateTime<Utc>>,
    pub descriptor_stale: bool,
    pub descriptor_mismatch: bool,
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

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct JobKindOverview {
    pub kind: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub docs_url: Option<String>,
    pub tags: Vec<String>,
    pub extra: serde_json::Value,
    pub descriptor_last_seen_at: Option<DateTime<Utc>>,
    pub descriptor_stale: bool,
    pub descriptor_mismatch: bool,
    pub job_count: i64,
    pub queue_count: i64,
    pub completed_last_hour: i64,
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
    #[serde(default)]
    pub claimers: Option<u16>,
    #[serde(default)]
    pub claim_batch_size: Option<usize>,
    #[serde(default)]
    pub dlq_enabled: Option<bool>,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StorageCapability {
    Canonical,
    CanonicalDrainOnly,
    QueueStorage,
}

impl StorageCapability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Canonical => "canonical",
            Self::CanonicalDrainOnly => "canonical_drain_only",
            Self::QueueStorage => "queue_storage",
        }
    }
}

impl fmt::Display for StorageCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StorageCapability {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "canonical" => Ok(Self::Canonical),
            "canonical_drain_only" => Ok(Self::CanonicalDrainOnly),
            "queue_storage" => Ok(Self::QueueStorage),
            _ => Err(value.to_string()),
        }
    }
}

/// Operator-selected execution role used during a `0.5.x → 0.6` storage
/// transition. Persisted on `awa.runtime_instances` so the SQL gate for
/// `enter_mixed_transition` can require a runtime that will actually
/// execute queue-storage work after routing flips, rather than only
/// inspecting the more permissive `storage_capability` snapshot
/// (auto-role runtimes report `queue_storage` while in
/// canonical/prepared but downgrade to `canonical_drain_only` post-flip).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TransitionRole {
    /// Default. Resolves storage at startup from `awa.storage_status()`
    /// and follows whatever the cluster is doing. Auto runtimes started
    /// before mixed_transition cannot satisfy the queue-storage executor
    /// gate; they're meant to drain canonical first and then have new
    /// auto runtimes spin up post-flip to take over queue_storage work.
    #[default]
    Auto,
    /// Stay on canonical execution regardless of transition state. Used
    /// for runtimes that should keep draining canonical even after
    /// routing flips.
    CanonicalDrain,
    /// Always execute queue-storage work, including pre-flip when state
    /// is `prepared`. At least one live runtime in this role is required
    /// before `awa.storage_enter_mixed_transition()` will succeed.
    QueueStorageTarget,
}

impl TransitionRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::CanonicalDrain => "canonical_drain",
            Self::QueueStorageTarget => "queue_storage_target",
        }
    }
}

impl fmt::Display for TransitionRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TransitionRole {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "auto" => Ok(Self::Auto),
            "canonical_drain" => Ok(Self::CanonicalDrain),
            "queue_storage_target" => Ok(Self::QueueStorageTarget),
            _ => Err(value.to_string()),
        }
    }
}

/// Data written by a worker runtime into the observability snapshot table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSnapshotInput {
    pub instance_id: Uuid,
    pub hostname: Option<String>,
    pub pid: i32,
    pub version: String,
    pub storage_capability: StorageCapability,
    #[serde(default)]
    pub transition_role: TransitionRole,
    pub started_at: DateTime<Utc>,
    pub snapshot_interval_ms: i64,
    pub healthy: bool,
    pub postgres_connected: bool,
    pub poll_loop_alive: bool,
    pub heartbeat_alive: bool,
    pub maintenance_alive: bool,
    pub shutting_down: bool,
    pub leader: bool,
    /// Whether this runtime contends for maintenance leadership. Recorded in
    /// `awa.runtime_maintenance_eligibility` only when false, and only once
    /// the `runtime_maintenance_eligibility` schema patch is applied.
    #[serde(default = "maintenance_leader_default")]
    pub maintenance_leader: bool,
    pub global_max_workers: Option<u32>,
    pub queues: Vec<QueueRuntimeSnapshot>,
    pub queue_descriptor_hashes: HashMap<String, String>,
    pub job_kind_descriptor_hashes: HashMap<String, String>,
}

fn maintenance_leader_default() -> bool {
    true
}

/// A worker runtime instance as exposed through the admin API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeInstance {
    pub instance_id: Uuid,
    pub hostname: Option<String>,
    pub pid: i32,
    pub version: String,
    pub storage_capability: StorageCapability,
    #[serde(default)]
    pub transition_role: TransitionRole,
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
    /// Whether this runtime may become maintenance leader. False only for a
    /// runtime started with `maintenance_leader(false)` on a database with
    /// the `runtime_maintenance_eligibility` schema patch; runtimes from
    /// before the option always contend, so they report true.
    #[serde(default = "maintenance_leader_default")]
    pub maintenance_leader: bool,
    pub global_max_workers: Option<u32>,
    pub queues: Vec<QueueRuntimeSnapshot>,
}

impl RuntimeInstance {
    /// Whether this runtime could take maintenance leadership now: it is
    /// live, not shutting down, and has not opted out.
    pub fn can_take_leadership(&self) -> bool {
        !self.stale && !self.shutting_down && self.maintenance_leader
    }

    fn stale_cutoff(interval_ms: i64) -> Duration {
        let interval_ms = max(interval_ms, 1_000);
        Duration::milliseconds(max(interval_ms.saturating_mul(3), 30_000))
    }

    fn from_db_row(row: RuntimeInstanceRow, now: DateTime<Utc>) -> Result<Self, AwaError> {
        let stale = row.last_seen_at + Self::stale_cutoff(row.snapshot_interval_ms) < now;
        let storage_capability =
            StorageCapability::from_str(&row.storage_capability).map_err(|value| {
                AwaError::Validation(format!(
                    "invalid storage capability in runtime_instances: {value}"
                ))
            })?;
        let transition_role = TransitionRole::from_str(&row.transition_role).map_err(|value| {
            AwaError::Validation(format!(
                "invalid transition_role in runtime_instances: {value}"
            ))
        })?;
        Ok(Self {
            instance_id: row.instance_id,
            hostname: row.hostname,
            pid: row.pid,
            version: row.version,
            storage_capability,
            transition_role,
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
            maintenance_leader: row.maintenance_leader,
            global_max_workers: row.global_max_workers.map(|v| v as u32),
            queues: row.queues.0,
        })
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
    /// Live runtimes, not shutting down, that may become maintenance leader.
    /// Zero while runtimes are live means none of them will take leadership.
    #[serde(default)]
    pub leader_eligible_instances: usize,
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

fn queue_runtime_configs_match(
    left: &QueueRuntimeConfigSnapshot,
    right: &QueueRuntimeConfigSnapshot,
) -> bool {
    left.mode == right.mode
        && left.max_workers == right.max_workers
        && left.min_workers == right.min_workers
        && left.weight == right.weight
        && left.global_max_workers == right.global_max_workers
        && left.poll_interval_ms == right.poll_interval_ms
        && left.deadline_duration_secs == right.deadline_duration_secs
        && left.priority_aging_interval_secs == right.priority_aging_interval_secs
        && left.rate_limit == right.rate_limit
        && (left.dlq_enabled.is_none()
            || right.dlq_enabled.is_none()
            || left.dlq_enabled == right.dlq_enabled)
}

fn representative_queue_runtime_config(
    configs: &[QueueRuntimeConfigSnapshot],
) -> Option<QueueRuntimeConfigSnapshot> {
    let first = configs.first()?;
    if first.dlq_enabled.is_some() {
        return Some(first.clone());
    }

    configs
        .iter()
        .find(|candidate| {
            candidate.dlq_enabled.is_some() && queue_runtime_configs_match(first, candidate)
        })
        .cloned()
        .or_else(|| Some(first.clone()))
}

#[derive(Debug, sqlx::FromRow)]
struct RuntimeInstanceRow {
    instance_id: Uuid,
    hostname: Option<String>,
    pid: i32,
    version: String,
    storage_capability: String,
    transition_role: String,
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
    maintenance_leader: bool,
    global_max_workers: Option<i32>,
    queues: Json<Vec<QueueRuntimeSnapshot>>,
}

/// Upsert a runtime observability snapshot for one worker instance.
///
/// A snapshot with `maintenance_leader: false` also records the opt-out in
/// `awa.runtime_maintenance_eligibility` when that table exists and the role
/// may insert into it; see [`maintenance_eligibility_reportable`].
pub async fn upsert_runtime_snapshot<'a, A>(
    conn: A,
    snapshot: &RuntimeSnapshotInput,
) -> Result<(), AwaError>
where
    A: Acquire<'a, Database = Postgres>,
{
    let mut conn = conn.acquire().await?;
    // Recorded before the snapshot row, so a reader never sees a new
    // opted-out runtime without its opt-out.
    if !snapshot.maintenance_leader && eligibility_table_usable(&mut conn, "INSERT").await? {
        sqlx::query(
            "INSERT INTO awa.runtime_maintenance_eligibility (instance_id, maintenance_leader) \
             VALUES ($1, false) ON CONFLICT (instance_id) DO NOTHING",
        )
        .bind(snapshot.instance_id)
        .execute(&mut *conn)
        .await?;
    }
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_instances (
            instance_id,
            hostname,
            pid,
            version,
            storage_capability,
            transition_role,
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
            queues,
            queue_descriptor_hashes,
            job_kind_descriptor_hashes
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, now(), $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
        )
        ON CONFLICT (instance_id) DO UPDATE SET
            hostname = EXCLUDED.hostname,
            pid = EXCLUDED.pid,
            version = EXCLUDED.version,
            storage_capability = EXCLUDED.storage_capability,
            transition_role = EXCLUDED.transition_role,
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
            queues = EXCLUDED.queues,
            queue_descriptor_hashes = EXCLUDED.queue_descriptor_hashes,
            job_kind_descriptor_hashes = EXCLUDED.job_kind_descriptor_hashes
        "#,
    )
    .bind(snapshot.instance_id)
    .bind(snapshot.hostname.as_deref())
    .bind(snapshot.pid)
    .bind(&snapshot.version)
    .bind(snapshot.storage_capability.as_str())
    .bind(snapshot.transition_role.as_str())
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
    .bind(Json(&snapshot.queue_descriptor_hashes))
    .bind(Json(&snapshot.job_kind_descriptor_hashes))
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// Whether `awa.runtime_maintenance_eligibility` exists and this role holds
/// `privilege` on it. The table comes from a schema patch, so a binary may
/// run against a database without it, or with a role that was never granted
/// it; both read as "every runtime is eligible".
pub(crate) async fn eligibility_table_usable(
    conn: &mut sqlx::PgConnection,
    privilege: &str,
) -> Result<bool, AwaError> {
    let usable: bool = sqlx::query_scalar(
        "SELECT to_regclass('awa.runtime_maintenance_eligibility') IS NOT NULL \
           AND has_table_privilege('awa.runtime_maintenance_eligibility', $1)",
    )
    .bind(privilege)
    .fetch_one(&mut *conn)
    .await?;
    Ok(usable)
}

/// SQL for whether the `awa.runtime_instances` row aliased `runtime` may lead,
/// for the tables this role can read.
pub(crate) async fn maintenance_leader_sql(
    conn: &mut sqlx::PgConnection,
) -> Result<&'static str, AwaError> {
    Ok(if eligibility_table_usable(conn, "SELECT").await? {
        "NOT EXISTS (SELECT 1 FROM awa.runtime_maintenance_eligibility AS eligibility \
                     WHERE eligibility.instance_id = runtime.instance_id \
                       AND NOT eligibility.maintenance_leader)"
    } else {
        "true"
    })
}

/// Whether a runtime that opts out of maintenance leadership can have that
/// opt-out recorded: the `runtime_maintenance_eligibility` schema patch is
/// applied and this role may insert into its table. When false the runtime
/// still never leads, but status and the admin API report it as eligible.
pub async fn maintenance_eligibility_reportable<'a, A>(conn: A) -> Result<bool, AwaError>
where
    A: Acquire<'a, Database = Postgres>,
{
    let mut conn = conn.acquire().await?;
    eligibility_table_usable(&mut conn, "INSERT").await
}

/// Delete eligibility rows whose runtime snapshot row has gone. Rows younger
/// than `min_age` are kept, so a runtime's opt-out recorded just before its
/// first snapshot is not swept. Returns the number of rows deleted, or zero
/// when the table is absent or this role may not delete from it.
pub async fn cleanup_runtime_maintenance_eligibility<'a, A>(
    conn: A,
    min_age: Duration,
) -> Result<u64, AwaError>
where
    A: Acquire<'a, Database = Postgres>,
{
    let mut conn = conn.acquire().await?;
    if !eligibility_table_usable(&mut conn, "DELETE").await? {
        return Ok(0);
    }
    let seconds = max(min_age.num_seconds(), 1);
    let result = sqlx::query(
        "DELETE FROM awa.runtime_maintenance_eligibility AS eligibility \
         WHERE eligibility.recorded_at < now() - make_interval(secs => $1) \
           AND NOT EXISTS (SELECT 1 FROM awa.runtime_instances AS runtime \
                           WHERE runtime.instance_id = eligibility.instance_id)",
    )
    .bind(seconds)
    .execute(&mut *conn)
    .await?;
    Ok(result.rows_affected())
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

/// Delete descriptor rows whose `last_seen_at` is older than `max_age`.
///
/// Intended to run on the maintenance leader's cleanup cycle — a descriptor
/// whose declaring code has been retired would otherwise linger in the
/// catalog forever, showing as permanently stale. Descriptors are
/// best-effort (no FK from `awa.jobs*`), so deletion is safe: if a worker
/// later re-declares the same queue / kind, the next sync recreates the
/// row from the declaration.
///
/// `table` must be `awa.queue_descriptors` or `awa.job_kind_descriptors`;
/// the caller is expected to dispatch both in turn.
pub async fn cleanup_stale_descriptors<'e, E>(
    executor: E,
    table: &str,
    max_age: Duration,
) -> Result<u64, AwaError>
where
    E: PgExecutor<'e>,
{
    if !matches!(table, "awa.queue_descriptors" | "awa.job_kind_descriptors") {
        return Err(AwaError::Validation(format!(
            "cleanup_stale_descriptors: unknown table {table:?}"
        )));
    }
    let seconds = max(max_age.num_seconds(), 1);
    // Table name is an authenticated literal from the match above — safe
    // to interpolate into the statement.
    let sql = format!("DELETE FROM {table} WHERE last_seen_at < now() - make_interval(secs => $1)");
    let result = sqlx::query(&sql).bind(seconds).execute(executor).await?;
    Ok(result.rows_affected())
}

/// List all runtime instances ordered with leader/live instances first.
pub async fn list_runtime_instances<'a, A>(conn: A) -> Result<Vec<RuntimeInstance>, AwaError>
where
    A: Acquire<'a, Database = Postgres>,
{
    let mut conn = conn.acquire().await?;
    let maintenance_leader = maintenance_leader_sql(&mut conn).await?;
    let rows = sqlx::query_as::<_, RuntimeInstanceRow>(&format!(
        r#"
        SELECT
            runtime.instance_id,
            runtime.hostname,
            runtime.pid,
            runtime.version,
            runtime.storage_capability,
            runtime.transition_role,
            runtime.started_at,
            runtime.last_seen_at,
            runtime.snapshot_interval_ms,
            runtime.healthy,
            runtime.postgres_connected,
            runtime.poll_loop_alive,
            runtime.heartbeat_alive,
            runtime.maintenance_alive,
            runtime.shutting_down,
            runtime.leader,
            {maintenance_leader} AS maintenance_leader,
            runtime.global_max_workers,
            runtime.queues
        FROM awa.runtime_instances AS runtime
        ORDER BY runtime.leader DESC, runtime.last_seen_at DESC, runtime.started_at DESC
        "#
    ))
    .fetch_all(&mut *conn)
    .await?;

    let now = Utc::now();
    rows.into_iter()
        .map(|row| RuntimeInstance::from_db_row(row, now))
        .collect()
}

/// Cluster runtime overview with instance list.
pub async fn runtime_overview<'a, A>(conn: A) -> Result<RuntimeOverview, AwaError>
where
    A: Acquire<'a, Database = Postgres>,
{
    let instances = list_runtime_instances(conn).await?;
    let total_instances = instances.len();
    let stale_instances = instances.iter().filter(|i| i.stale).count();
    let live_instances = total_instances.saturating_sub(stale_instances);
    let healthy_instances = instances.iter().filter(|i| !i.stale && i.healthy).count();
    let leader_instances = instances.iter().filter(|i| !i.stale && i.leader).count();
    let leader_eligible_instances = instances.iter().filter(|i| i.can_take_leadership()).count();

    Ok(RuntimeOverview {
        total_instances,
        live_instances,
        stale_instances,
        healthy_instances,
        leader_instances,
        leader_eligible_instances,
        instances,
    })
}

/// Queue runtime/config summary aggregated across worker snapshots.
pub async fn queue_runtime_summary<'a, A>(conn: A) -> Result<Vec<QueueRuntimeSummary>, AwaError>
where
    A: Acquire<'a, Database = Postgres>,
{
    let instances = list_runtime_instances(conn).await?;
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
            let config = representative_queue_runtime_config(&config_candidates);
            let config_mismatch = config_candidates.iter().skip(1).any(|candidate| {
                config
                    .as_ref()
                    .is_some_and(|cfg| !queue_runtime_configs_match(cfg, candidate))
            });

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

/// Get queue overviews for all known queues.
///
/// On queue storage, counts come from the live-work tables (see
/// `queue_storage_signals_cte`).
///
/// On the canonical engine it is a hybrid read: per-state counts come from the `queue_state_counts`
/// cache table (eventually consistent, ~2s lag), while `lag_seconds`
/// and `completed_last_hour` are computed live from `jobs_hot`.
///
/// The cache is kept fresh by the maintenance leader's dirty-key
/// recompute (~2s) and full reconciliation (~60s). Also warmed during
/// `migrate()`.
///
/// For exact cached counts in tests without a running maintenance
/// leader, call `flush_dirty_admin_metadata()` first.
pub async fn queue_overviews(pool: &PgPool) -> Result<Vec<QueueOverview>, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        // `queue_state_counts` isn't maintained on queue storage, and the
        // `current_jobs` view expands every retained completion, so counts
        // come from the live-work tables instead.
        let sql = format!(
            r#"
            {signals_cte},
            all_queues AS (
                SELECT queue FROM signals
                UNION
                SELECT queue FROM awa.queue_descriptors
                UNION
                SELECT queue FROM awa.queue_meta
                UNION
                SELECT DISTINCT descriptor.key AS queue
                FROM awa.runtime_instances runtime
                CROSS JOIN LATERAL jsonb_each_text(runtime.queue_descriptor_hashes) AS descriptor(key, value)
            ),
            live_queue_descriptor_variants AS (
                SELECT
                    descriptor.key AS queue,
                    count(DISTINCT descriptor.value)::bigint AS descriptor_variant_count
                FROM awa.runtime_instances runtime
                CROSS JOIN LATERAL jsonb_each_text(runtime.queue_descriptor_hashes) AS descriptor(key, value)
                WHERE runtime.last_seen_at + make_interval(
                    secs => GREATEST(((GREATEST(runtime.snapshot_interval_ms, 1000) / 1000) * 3)::int, 30)
                ) >= now()
                GROUP BY descriptor.key
            )
            SELECT
                q.queue,
                qd.display_name,
                qd.description,
                qd.owner,
                qd.docs_url,
                COALESCE(qd.tags, ARRAY[]::text[]) AS tags,
                COALESCE(qd.extra, '{{}}'::jsonb) AS extra,
                qd.last_seen_at AS descriptor_last_seen_at,
                CASE
                    WHEN qd.last_seen_at IS NULL THEN FALSE
                    ELSE qd.last_seen_at + make_interval(
                        secs => GREATEST(((COALESCE(qd.sync_interval_ms, 10000) / 1000) * 3)::int, 30)
                    ) < now()
                END AS descriptor_stale,
                COALESCE(qdv.descriptor_variant_count, 0) > 1 AS descriptor_mismatch,
                COALESCE(sg.scheduled + sg.available + sg.running + sg.retryable + sg.waiting_external, 0)
                    AS total_queued,
                COALESCE(sg.scheduled, 0) AS scheduled,
                COALESCE(sg.available, 0) AS available,
                COALESCE(sg.retryable, 0) AS retryable,
                COALESCE(sg.running, 0) AS running,
                COALESCE(sg.failed, 0) AS failed,
                COALESCE(sg.waiting_external, 0) AS waiting_external,
                COALESCE(sg.completed_last_hour, 0) AS completed_last_hour,
                sg.lag_seconds,
                COALESCE(qm.paused, FALSE) AS paused
            FROM all_queues q
            LEFT JOIN signals sg ON sg.queue = q.queue
            LEFT JOIN awa.queue_descriptors qd ON qd.queue = q.queue
            LEFT JOIN live_queue_descriptor_variants qdv ON qdv.queue = q.queue
            LEFT JOIN awa.queue_meta qm ON qm.queue = q.queue
            ORDER BY q.queue
            "#,
            signals_cte = queue_storage_signals_cte(store.schema())
        );

        // JIT compiles this statement for longer than it runs.
        let mut begin_guard = crate::read_transaction::BeginGuard::acquire(pool).await?;
        let mut tx = sqlx::Connection::begin(&mut *begin_guard.connection).await?;
        begin_guard.begin_pending = false;
        sqlx::query("SET LOCAL jit = off")
            .execute(tx.as_mut())
            .await?;
        let rows = sqlx::query_as::<_, QueueOverview>(&sql)
            .fetch_all(tx.as_mut())
            .await?;
        tx.commit().await?;
        drop(begin_guard);
        return Ok(rows);
    }

    let rows = sqlx::query_as::<_, QueueOverview>(
        r#"
        WITH all_queues AS (
            -- Union every source of queue-name knowledge so /queues never
            -- hides a queue that exists *somewhere* in the system:
            --   - queue_state_counts: has observed jobs
            --   - queue_descriptors:   declared by worker code
            --   - queue_meta:          pause state recorded by an operator
            --   - runtime_instances:   registered by a currently-running worker
            SELECT queue FROM awa.queue_state_counts
            UNION
            SELECT queue FROM awa.queue_descriptors
            UNION
            SELECT queue FROM awa.queue_meta
            UNION
            SELECT DISTINCT descriptor.key AS queue
            FROM awa.runtime_instances runtime
            CROSS JOIN LATERAL jsonb_each_text(runtime.queue_descriptor_hashes) AS descriptor(key, value)
        ),
        live_queue_descriptor_variants AS (
            SELECT
                descriptor.key AS queue,
                count(DISTINCT descriptor.value)::bigint AS descriptor_variant_count
            FROM awa.runtime_instances runtime
            CROSS JOIN LATERAL jsonb_each_text(runtime.queue_descriptor_hashes) AS descriptor(key, value)
            WHERE runtime.last_seen_at + make_interval(
                secs => GREATEST(((GREATEST(runtime.snapshot_interval_ms, 1000) / 1000) * 3)::int, 30)
            ) >= now()
            GROUP BY descriptor.key
        ),
        available_lag AS (
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
            q.queue,
            qd.display_name,
            qd.description,
            qd.owner,
            qd.docs_url,
            COALESCE(qd.tags, ARRAY[]::text[]) AS tags,
            COALESCE(qd.extra, '{}'::jsonb) AS extra,
            qd.last_seen_at AS descriptor_last_seen_at,
            CASE
                WHEN qd.last_seen_at IS NULL THEN FALSE
                ELSE qd.last_seen_at + make_interval(
                    secs => GREATEST(((COALESCE(qd.sync_interval_ms, 10000) / 1000) * 3)::int, 30)
                ) < now()
            END AS descriptor_stale,
            COALESCE(qdv.descriptor_variant_count, 0) > 1 AS descriptor_mismatch,
            COALESCE(qs.scheduled + qs.available + qs.running + qs.retryable + qs.waiting_external, 0) AS total_queued,
            COALESCE(qs.scheduled, 0) AS scheduled,
            COALESCE(qs.available, 0) AS available,
            COALESCE(qs.retryable, 0) AS retryable,
            COALESCE(qs.running, 0) AS running,
            COALESCE(qs.failed, 0) AS failed,
            COALESCE(qs.waiting_external, 0) AS waiting_external,
            COALESCE(cr.completed_last_hour, 0) AS completed_last_hour,
            al.lag_seconds,
            COALESCE(qm.paused, FALSE) AS paused
        FROM all_queues q
        LEFT JOIN awa.queue_state_counts qs ON qs.queue = q.queue
        LEFT JOIN awa.queue_descriptors qd ON qd.queue = q.queue
        LEFT JOIN live_queue_descriptor_variants qdv ON qdv.queue = q.queue
        LEFT JOIN available_lag al ON al.queue = q.queue
        LEFT JOIN completed_recent cr ON cr.queue = q.queue
        LEFT JOIN awa.queue_meta qm ON qm.queue = q.queue
        ORDER BY q.queue
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Get one queue overview by name.
pub async fn queue_overview(pool: &PgPool, queue: &str) -> Result<Option<QueueOverview>, AwaError> {
    let rows = queue_overviews(pool).await?;
    Ok(rows.into_iter().find(|row| row.queue == queue))
}

pub async fn queue_descriptors_for_names<'e, E>(
    executor: E,
    queues: &[String],
) -> Result<HashMap<String, QueueDescriptor>, AwaError>
where
    E: PgExecutor<'e>,
{
    if queues.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query_as::<
        _,
        (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Vec<String>,
            serde_json::Value,
        ),
    >(
        r#"
        SELECT
            queue,
            display_name,
            description,
            owner,
            docs_url,
            tags,
            extra
        FROM awa.queue_descriptors
        WHERE queue = ANY($1)
        "#,
    )
    .bind(queues)
    .fetch_all(executor)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(queue, display_name, description, owner, docs_url, tags, extra)| {
                (
                    queue,
                    QueueDescriptor {
                        display_name,
                        description,
                        owner,
                        docs_url,
                        tags,
                        extra,
                    },
                )
            },
        )
        .collect())
}

pub async fn job_kind_descriptors_for_names<'e, E>(
    executor: E,
    kinds: &[String],
) -> Result<HashMap<String, JobKindDescriptor>, AwaError>
where
    E: PgExecutor<'e>,
{
    if kinds.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query_as::<
        _,
        (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Vec<String>,
            serde_json::Value,
        ),
    >(
        r#"
        SELECT
            kind,
            display_name,
            description,
            owner,
            docs_url,
            tags,
            extra
        FROM awa.job_kind_descriptors
        WHERE kind = ANY($1)
        "#,
    )
    .bind(kinds)
    .fetch_all(executor)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(kind, display_name, description, owner, docs_url, tags, extra)| {
                (
                    kind,
                    JobKindDescriptor {
                        display_name,
                        description,
                        owner,
                        docs_url,
                        tags,
                        extra,
                    },
                )
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
pub async fn list_jobs(pool: &PgPool, filter: &ListJobsFilter) -> Result<Vec<JobRow>, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        return list_queue_storage_jobs(&store, pool, filter).await;
    }

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
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Get a single job by ID.
pub async fn get_job(pool: &PgPool, job_id: i64) -> Result<JobRow, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        let row = store.load_job(pool, job_id).await?;
        return row.ok_or(AwaError::JobNotFound { id: job_id });
    }

    let row = sqlx::query_as::<_, JobRow>("SELECT * FROM awa.jobs WHERE id = $1")
        .bind(job_id)
        .fetch_optional(pool)
        .await?;

    row.ok_or(AwaError::JobNotFound { id: job_id })
}

/// Fetch a job plus optional DLQ metadata for admin surfaces that need to
/// distinguish a DLQ row from a normal live or terminal row.
pub async fn get_job_with_source(
    pool: &PgPool,
    job_id: i64,
) -> Result<(JobRow, Option<DlqMetadata>), AwaError> {
    let job = get_job(pool, job_id).await?;
    let dlq = if active_queue_storage(pool).await?.is_some() {
        crate::dlq::get_dlq_job(pool, job_id)
            .await?
            .map(|row| row.metadata())
    } else {
        None
    };
    Ok((job, dlq))
}

/// Build a read-only inspection snapshot for one job.
pub async fn dump_job(pool: &PgPool, job_id: i64) -> Result<JobDump, AwaError> {
    let (job, dlq) = get_job_with_source(pool, job_id).await?;
    Ok(build_job_dump(job, dlq))
}

/// Build a read-only inspection snapshot for one attempt.
///
/// Awa does not currently persist a standalone runs table. The current attempt
/// is inspected from the live job row. Historical attempts are reconstructed
/// from the structured `errors[]` history.
pub async fn dump_run(
    pool: &PgPool,
    job_id: i64,
    attempt: Option<i16>,
) -> Result<RunDump, AwaError> {
    let job = get_job(pool, job_id).await?;
    let selected_attempt = attempt.unwrap_or(job.attempt);

    if selected_attempt < 0 {
        return Err(AwaError::Validation("attempt must be >= 0".to_string()));
    }

    if job.attempt == 0 && selected_attempt == 0 {
        return Ok(RunDump {
            job_id: job.id,
            kind: job.kind.clone(),
            queue: job.queue.clone(),
            selected_attempt,
            current_attempt: job.attempt,
            current_run_lease: job.run_lease,
            selected_run_lease: Some(job.run_lease),
            source: RunDumpSource::CurrentJobRow,
            state: job.state,
            started_at: job.attempted_at,
            finished_at: job.finalized_at,
            error: None,
            terminal: None,
            progress: job.progress.clone(),
            metadata: Some(job.metadata.clone()),
            callback: callback_dump(&job),
            raw_error_entry: None,
            notes: vec!["Job has not been claimed yet; attempt 0 is the pre-run snapshot.".into()],
        });
    }

    if selected_attempt == job.attempt {
        let latest_error = job
            .errors
            .as_ref()
            .into_iter()
            .flatten()
            .filter_map(parse_error_entry)
            .find(|entry| entry.attempt == Some(selected_attempt));
        return Ok(RunDump {
            job_id: job.id,
            kind: job.kind.clone(),
            queue: job.queue.clone(),
            selected_attempt,
            current_attempt: job.attempt,
            current_run_lease: job.run_lease,
            selected_run_lease: Some(job.run_lease),
            source: RunDumpSource::CurrentJobRow,
            state: job.state,
            started_at: job.attempted_at,
            finished_at: if job.state.is_terminal() {
                job.finalized_at
            } else {
                None
            },
            error: latest_error.as_ref().and_then(|entry| entry.error.clone()),
            terminal: latest_error.as_ref().map(|entry| entry.terminal),
            progress: job.progress.clone(),
            metadata: Some(job.metadata.clone()),
            callback: callback_dump(&job),
            raw_error_entry: latest_error.map(|entry| entry.raw),
            notes: vec![],
        });
    }

    if selected_attempt == 0 {
        return Err(AwaError::Validation(format!(
            "attempt {selected_attempt} is not available for job {job_id}; current attempt is {}",
            job.attempt
        )));
    }

    if job.attempt > 0 && selected_attempt > job.attempt {
        return Err(AwaError::Validation(format!(
            "attempt {selected_attempt} is not available for job {job_id}; current attempt is {}",
            job.attempt
        )));
    }

    let historical = job
        .errors
        .as_ref()
        .into_iter()
        .flatten()
        .filter_map(parse_error_entry)
        .find(|entry| entry.attempt == Some(selected_attempt))
        .ok_or_else(|| {
            AwaError::Validation(format!(
                "attempt {selected_attempt} is not present in the recorded error history for job {job_id}"
            ))
        })?;

    Ok(RunDump {
        job_id: job.id,
        kind: job.kind.clone(),
        queue: job.queue.clone(),
        selected_attempt,
        current_attempt: job.attempt,
        current_run_lease: job.run_lease,
        selected_run_lease: None,
        source: RunDumpSource::ErrorHistory,
        state: if historical.terminal {
            JobState::Failed
        } else {
            JobState::Retryable
        },
        started_at: None,
        finished_at: historical.at,
        error: historical.error,
        terminal: Some(historical.terminal),
        progress: None,
        metadata: None,
        callback: None,
        raw_error_entry: Some(historical.raw),
        notes: vec![
            "Historical attempts are reconstructed from errors[] because Awa does not persist a standalone runs table.".into(),
            "Only the current attempt has live progress, callback, and metadata fields.".into(),
        ],
    })
}

/// Count jobs grouped by state.
///
/// Reads from the `queue_state_counts` cache table.
pub async fn state_counts(pool: &PgPool) -> Result<HashMap<JobState, i64>, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        let sql = format!(
            r#"
            SELECT
                COALESCE((SELECT count(*)::bigint FROM {schema}.deferred_jobs WHERE state = 'scheduled'), 0) AS scheduled,
                COALESCE((SELECT sum(available)::bigint FROM ({available}) AS lanes), 0) AS available,
                COALESCE((SELECT count(*)::bigint FROM {schema}.leases WHERE state = 'running'), 0)
                  + COALESCE((SELECT count(*)::bigint FROM ({open_running}) AS open_running), 0) AS running,
                -- A completion batch carries its size, so completed jobs are
                -- counted without expanding batches through `terminal_jobs`.
                -- Tombstones mark batch members deleted or retried since.
                COALESCE((SELECT sum(completed_count)::bigint FROM {schema}.receipt_completion_batches), 0)
                  - COALESCE((SELECT count(*)::bigint FROM {schema}.receipt_completion_tombstones), 0)
                  + COALESCE((SELECT count(*)::bigint FROM {schema}.done_entries WHERE state = 'completed'), 0)
                  AS completed,
                COALESCE((SELECT count(*)::bigint FROM {schema}.deferred_jobs WHERE state = 'retryable'), 0) AS retryable,
                COALESCE((SELECT count(*)::bigint FROM {schema}.done_entries WHERE state = 'failed'), 0)
                  + COALESCE((SELECT count(*)::bigint FROM {schema}.dlq_entries), 0) AS failed,
                COALESCE((SELECT count(*)::bigint FROM {schema}.done_entries WHERE state = 'cancelled'), 0) AS cancelled,
                COALESCE((SELECT count(*)::bigint FROM {schema}.leases WHERE state = 'waiting_external'), 0) AS waiting_external
            "#,
            available = queue_storage_available_by_queue_sql(store.schema()),
            open_running = crate::queue_storage::open_receipt_running_claims_sql(store.schema()),
            schema = store.schema(),
        );

        let (
            scheduled,
            available,
            running,
            completed,
            retryable,
            failed,
            cancelled,
            waiting_external,
        ): (i64, i64, i64, i64, i64, i64, i64, i64) = {
            // As in `queue_overviews`: compiling costs more than running.
            let mut begin_guard = crate::read_transaction::BeginGuard::acquire(pool).await?;
            let mut tx = sqlx::Connection::begin(&mut *begin_guard.connection).await?;
            begin_guard.begin_pending = false;
            sqlx::query("SET LOCAL jit = off")
                .execute(tx.as_mut())
                .await?;
            let counts = sqlx::query_as(&sql).fetch_one(tx.as_mut()).await?;
            tx.commit().await?;
            drop(begin_guard);
            counts
        };

        return Ok(HashMap::from([
            (JobState::Scheduled, scheduled),
            (JobState::Available, available),
            (JobState::Running, running),
            (JobState::Completed, completed),
            (JobState::Retryable, retryable),
            (JobState::Failed, failed),
            (JobState::Cancelled, cancelled),
            (JobState::WaitingExternal, waiting_external),
        ]));
    }

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
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().collect())
}

#[derive(sqlx::FromRow)]
struct KindCountHead {
    queue: String,
    priority: i16,
    enqueue_shard: i16,
    claim_seq: i64,
    next_seq: i64,
}

// Sequence values do not obey MVCC. Capture them with the first read's
// snapshot instead of letting later claims advance them during receipt phases.
async fn queue_storage_kind_heads(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    store: &QueueStorage,
) -> Result<Vec<KindCountHead>, sqlx::Error> {
    let schema = store.schema();
    sqlx::query("SAVEPOINT awa_kind_heads")
        .execute(tx.as_mut())
        .await?;
    let heads = sqlx::query_as::<_, KindCountHead>(&format!(
        r#"
            SELECT
                claims.queue,
                claims.priority,
                claims.enqueue_shard,
                {schema}.sequence_next_value(claims.seq_name) AS claim_seq,
                {schema}.sequence_next_value(enqueues.seq_name) AS next_seq
            FROM {schema}.queue_claim_heads AS claims
            JOIN {schema}.queue_enqueue_heads AS enqueues
              ON enqueues.queue = claims.queue
             AND enqueues.priority = claims.priority
             AND enqueues.enqueue_shard = claims.enqueue_shard
    "#
    ))
    .fetch_all(tx.as_mut())
    .await?;
    sqlx::query("ROLLBACK TO SAVEPOINT awa_kind_heads")
        .execute(tx.as_mut())
        .await?;
    sqlx::query("RELEASE SAVEPOINT awa_kind_heads")
        .execute(tx.as_mut())
        .await?;
    Ok(heads)
}

async fn queue_storage_base_kind_counts(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    store: &QueueStorage,
    heads: Vec<KindCountHead>,
) -> Result<Vec<(String, String, i64, i64)>, sqlx::Error> {
    let mut head_queues = Vec::with_capacity(heads.len());
    let mut head_priorities = Vec::with_capacity(heads.len());
    let mut head_shards = Vec::with_capacity(heads.len());
    let mut claim_seqs = Vec::with_capacity(heads.len());
    let mut next_seqs = Vec::with_capacity(heads.len());
    for head in heads {
        head_queues.push(head.queue);
        head_priorities.push(head.priority);
        head_shards.push(head.enqueue_shard);
        claim_seqs.push(head.claim_seq);
        next_seqs.push(head.next_seq);
    }
    let mut counts = Vec::new();
    for source in [
        KindCountSource::Available,
        KindCountSource::Leased,
        KindCountSource::Deferred,
        KindCountSource::Done,
        KindCountSource::Receipts,
        KindCountSource::Dlq,
    ] {
        let sql = queue_storage_kind_count_source_sql(store.schema(), source);
        sqlx::query("SAVEPOINT awa_kind_source")
            .execute(tx.as_mut())
            .await?;
        let mut query = sqlx::query_as::<_, (String, String, i64, i64)>(&sql);
        if source == KindCountSource::Available {
            query = query
                .bind(&head_queues)
                .bind(&head_priorities)
                .bind(&head_shards)
                .bind(&claim_seqs)
                .bind(&next_seqs);
        }
        counts.extend(query.fetch_all(tx.as_mut()).await?);
        sqlx::query("ROLLBACK TO SAVEPOINT awa_kind_source")
            .execute(tx.as_mut())
            .await?;
        sqlx::query("RELEASE SAVEPOINT awa_kind_source")
            .execute(tx.as_mut())
            .await?;
    }
    Ok(counts)
}

// The result is bounded by the installed claim-ring slot count, not by
// retained job volume. Include all populated slots, including old generations.
async fn queue_storage_populated_claim_slots(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    store: &QueueStorage,
) -> Result<Vec<usize>, AwaError> {
    let schema = store.schema();
    sqlx::query("SAVEPOINT awa_claim_slots")
        .execute(tx.as_mut())
        .await?;
    let slots: Vec<i32> = sqlx::query_scalar(&format!(
        "SELECT claim_slot FROM {schema}.lease_claims WHERE closed_at IS NULL \
         UNION SELECT claim_slot FROM {schema}.lease_claim_batches ORDER BY claim_slot"
    ))
    .fetch_all(tx.as_mut())
    .await?;
    sqlx::query("ROLLBACK TO SAVEPOINT awa_claim_slots")
        .execute(tx.as_mut())
        .await?;
    sqlx::query("RELEASE SAVEPOINT awa_claim_slots")
        .execute(tx.as_mut())
        .await?;
    // A schema-only admin store carries the default slot count, so take the
    // installed slot numbers as they are rather than bounding them by it.
    slots
        .into_iter()
        .map(|slot| crate::queue_storage::ring_slot_number(slot, "claim"))
        .collect()
}

// Keep only per-kind, per-queue aggregates in Rust, not every claim identity.
async fn queue_storage_running_kind_counts(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    store: &QueueStorage,
) -> Result<Vec<(String, String, i64)>, AwaError> {
    let schema = store.schema();
    let slots = queue_storage_populated_claim_slots(tx, store).await?;
    let mut counts = Vec::new();
    for slot in slots {
        let open_running =
            crate::queue_storage::open_receipt_running_claims_sql_in_slot(schema, slot);
        sqlx::query("SAVEPOINT awa_kind_counts")
            .execute(tx.as_mut())
            .await?;
        let rows: Vec<(String, String, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT created.kind, open_claims.queue, count(*)::bigint
            FROM ({open_running}) AS open_claims
            CROSS JOIN LATERAL (
                SELECT ready.kind
                FROM {schema}.ready_entries AS ready
                WHERE ready.job_id = open_claims.job_id
                  AND ready.run_lease + 1 = open_claims.run_lease
                  AND ready.queue = open_claims.queue
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.ready_tombstones AS tomb
                      WHERE tomb.ready_slot = ready.ready_slot
                        AND tomb.ready_generation = ready.ready_generation
                        AND tomb.queue = ready.queue
                        AND tomb.priority = ready.priority
                        AND tomb.enqueue_shard = ready.enqueue_shard
                        AND tomb.lane_seq = ready.lane_seq
                  )
                LIMIT 1
            ) AS created
            GROUP BY created.kind, open_claims.queue
        "#
        ))
        .fetch_all(tx.as_mut())
        .await?;
        sqlx::query("ROLLBACK TO SAVEPOINT awa_kind_counts")
            .execute(tx.as_mut())
            .await?;
        sqlx::query("RELEASE SAVEPOINT awa_kind_counts")
            .execute(tx.as_mut())
            .await?;
        counts.extend(rows);
    }
    Ok(counts)
}

/// Get job-kind overviews for all known kinds.
pub async fn job_kind_overviews(pool: &PgPool) -> Result<Vec<JobKindOverview>, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        let sql = format!(
            r#"
            WITH {kind_counts_cte},
            all_kinds AS (
                SELECT kind FROM kind_counts
                UNION
                SELECT kind FROM awa.job_kind_descriptors
                UNION
                SELECT DISTINCT descriptor.key AS kind
                FROM awa.runtime_instances runtime
                CROSS JOIN LATERAL jsonb_each_text(runtime.job_kind_descriptor_hashes) AS descriptor(key, value)
            ),
            live_kind_descriptor_variants AS (
                SELECT
                    descriptor.key AS kind,
                    count(DISTINCT descriptor.value)::bigint AS descriptor_variant_count
                FROM awa.runtime_instances runtime
                CROSS JOIN LATERAL jsonb_each_text(runtime.job_kind_descriptor_hashes) AS descriptor(key, value)
                WHERE runtime.last_seen_at + make_interval(
                    secs => GREATEST(((GREATEST(runtime.snapshot_interval_ms, 1000) / 1000) * 3)::int, 30)
                ) >= now()
                GROUP BY descriptor.key
            )
            SELECT
                k.kind,
                kd.display_name,
                kd.description,
                kd.owner,
                kd.docs_url,
                COALESCE(kd.tags, ARRAY[]::text[]) AS tags,
                COALESCE(kd.extra, '{{}}'::jsonb) AS extra,
                kd.last_seen_at AS descriptor_last_seen_at,
                CASE
                    WHEN kd.last_seen_at IS NULL THEN FALSE
                    ELSE kd.last_seen_at + make_interval(
                        secs => GREATEST(((COALESCE(kd.sync_interval_ms, 10000) / 1000) * 3)::int, 30)
                    ) < now()
                END AS descriptor_stale,
                COALESCE(kdv.descriptor_variant_count, 0) > 1 AS descriptor_mismatch,
                COALESCE(kc.job_count, 0) AS job_count,
                COALESCE(kc.queue_count, 0) AS queue_count,
                COALESCE(kc.completed_last_hour, 0) AS completed_last_hour
            FROM all_kinds k
            LEFT JOIN kind_counts kc ON kc.kind = k.kind
            LEFT JOIN awa.job_kind_descriptors kd ON kd.kind = k.kind
            LEFT JOIN live_kind_descriptor_variants kdv ON kdv.kind = k.kind
            ORDER BY k.kind
            "#,
            kind_counts_cte = queue_storage_kind_counts_cte()
        );

        // As in `queue_overviews`: compiling costs more than running.
        let mut begin_guard = crate::read_transaction::BeginGuard::acquire(pool).await?;
        let mut tx = sqlx::Connection::begin_with(
            &mut *begin_guard.connection,
            "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        )
        .await?;
        begin_guard.begin_pending = false;
        sqlx::query("SET LOCAL jit = off")
            .execute(tx.as_mut())
            .await?;
        let heads = queue_storage_kind_heads(&mut tx, &store).await?;
        let mut sources = queue_storage_base_kind_counts(&mut tx, &store, heads).await?;
        sources.extend(
            queue_storage_running_kind_counts(&mut tx, &store)
                .await?
                .into_iter()
                .map(|(kind, queue, n)| (kind, queue, n, 0)),
        );
        let mut kinds = Vec::with_capacity(sources.len());
        let mut queues = Vec::with_capacity(sources.len());
        let mut counts = Vec::with_capacity(sources.len());
        let mut recent_counts = Vec::with_capacity(sources.len());
        for (kind, queue, count, recent) in sources {
            kinds.push(kind);
            queues.push(queue);
            counts.push(count);
            recent_counts.push(recent);
        }
        let rows = sqlx::query_as::<_, JobKindOverview>(&sql)
            .bind(kinds)
            .bind(queues)
            .bind(counts)
            .bind(recent_counts)
            .fetch_all(tx.as_mut())
            .await?;
        tx.commit().await?;
        drop(begin_guard);
        return Ok(rows);
    }

    let rows = sqlx::query_as::<_, JobKindOverview>(
        r#"
        WITH all_kinds AS (
            -- Union every source of kind-name knowledge so /kinds never
            -- hides a kind that exists *somewhere* in the system:
            --   - job_kind_catalog: has observed jobs
            --   - job_kind_descriptors: declared by worker code
            --   - runtime_instances: reported by a currently-running worker
            SELECT kind FROM awa.job_kind_catalog WHERE ref_count > 0
            UNION
            SELECT kind FROM awa.job_kind_descriptors
            UNION
            SELECT DISTINCT descriptor.key AS kind
            FROM awa.runtime_instances runtime
            CROSS JOIN LATERAL jsonb_each_text(runtime.job_kind_descriptor_hashes) AS descriptor(key, value)
        ),
        live_kind_descriptor_variants AS (
            SELECT
                descriptor.key AS kind,
                count(DISTINCT descriptor.value)::bigint AS descriptor_variant_count
            FROM awa.runtime_instances runtime
            CROSS JOIN LATERAL jsonb_each_text(runtime.job_kind_descriptor_hashes) AS descriptor(key, value)
            WHERE runtime.last_seen_at + make_interval(
                secs => GREATEST(((GREATEST(runtime.snapshot_interval_ms, 1000) / 1000) * 3)::int, 30)
            ) >= now()
            GROUP BY descriptor.key
        ),
        completed_recent AS (
            SELECT
                kind,
                count(*)::bigint AS completed_last_hour
            FROM awa.jobs_hot
            WHERE state = 'completed'
              AND finalized_at > now() - interval '1 hour'
            GROUP BY kind
        ),
        queue_counts AS (
            -- Restrict the per-kind queue fan-out to `jobs_hot` rather than
            -- the full `awa.jobs` view. jobs_hot is bounded by retention
            -- (default 24h completed / 72h failed) so the scan cost is
            -- tied to in-flight volume, not historical volume. The
            -- semantic this produces — "queues this kind is currently
            -- active on" — is the one admin surfaces actually care about;
            -- a kind that hasn't enqueued in 3 months shouldn't be
            -- counted as still spanning N queues.
            SELECT
                kind,
                count(DISTINCT queue)::bigint AS queue_count
            FROM awa.jobs_hot
            GROUP BY kind
        )
        SELECT
            k.kind,
            kd.display_name,
            kd.description,
            kd.owner,
            kd.docs_url,
            COALESCE(kd.tags, ARRAY[]::text[]) AS tags,
            COALESCE(kd.extra, '{}'::jsonb) AS extra,
            kd.last_seen_at AS descriptor_last_seen_at,
            CASE
                WHEN kd.last_seen_at IS NULL THEN FALSE
                ELSE kd.last_seen_at + make_interval(
                    secs => GREATEST(((COALESCE(kd.sync_interval_ms, 10000) / 1000) * 3)::int, 30)
                ) < now()
            END AS descriptor_stale,
            COALESCE(kdv.descriptor_variant_count, 0) > 1 AS descriptor_mismatch,
            COALESCE(kc.ref_count, 0) AS job_count,
            COALESCE(qc.queue_count, 0) AS queue_count,
            COALESCE(cr.completed_last_hour, 0) AS completed_last_hour
        FROM all_kinds k
        LEFT JOIN awa.job_kind_catalog kc ON kc.kind = k.kind
        LEFT JOIN awa.job_kind_descriptors kd ON kd.kind = k.kind
        LEFT JOIN live_kind_descriptor_variants kdv ON kdv.kind = k.kind
        LEFT JOIN queue_counts qc ON qc.kind = k.kind
        LEFT JOIN completed_recent cr ON cr.kind = k.kind
        ORDER BY k.kind
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn job_kind_overview(
    pool: &PgPool,
    kind: &str,
) -> Result<Option<JobKindOverview>, AwaError> {
    let rows = job_kind_overviews(pool).await?;
    Ok(rows.into_iter().find(|row| row.kind == kind))
}

/// Return all distinct job kinds.
///
/// Reads from the `job_kind_catalog` cache table.
pub async fn distinct_kinds(pool: &PgPool) -> Result<Vec<String>, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        let sql = format!(
            "{} \
             SELECT DISTINCT kind \
             FROM current_jobs \
             ORDER BY kind",
            queue_storage_current_jobs_cte(store.schema())
        );
        return sqlx::query_scalar(&sql)
            .fetch_all(pool)
            .await
            .map_err(AwaError::from);
    }

    let rows = sqlx::query_scalar::<_, String>(
        "SELECT kind FROM awa.job_kind_catalog WHERE ref_count > 0 ORDER BY kind",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Return all distinct queue names.
///
/// Reads from the `job_queue_catalog` cache table.
pub async fn distinct_queues(pool: &PgPool) -> Result<Vec<String>, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        let sql = format!(
            "{} \
             SELECT DISTINCT queue \
             FROM current_jobs \
             ORDER BY queue",
            queue_storage_current_jobs_cte(store.schema())
        );
        return sqlx::query_scalar(&sql)
            .fetch_all(pool)
            .await
            .map_err(AwaError::from);
    }

    let rows = sqlx::query_scalar::<_, String>(
        "SELECT queue FROM awa.job_queue_catalog WHERE ref_count > 0 ORDER BY queue",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Drain one batch of dirty keys and recompute exact cached rows.
/// Returns the number of dirty keys processed in this batch.
///
/// Called frequently by the maintenance leader (~2s). Uses per-queue
/// indexes for targeted recompute rather than full table scans.
pub async fn recompute_dirty_admin_metadata(pool: &PgPool) -> Result<i32, AwaError> {
    if active_queue_storage(pool).await?.is_some() {
        return Ok(0);
    }
    let count: i32 = sqlx::query_scalar("SELECT awa.recompute_dirty_admin_metadata(100)")
        .fetch_one(pool)
        .await?;
    Ok(count)
}

/// Drain ALL dirty keys until the backlog is empty.
///
/// Use in tests or admin tooling where you need the cache to be fully
/// consistent before reading. Each call to the underlying SQL function
/// acquires a blocking advisory lock, so concurrent callers serialize
/// rather than skip.
pub async fn flush_dirty_admin_metadata(pool: &PgPool) -> Result<i32, AwaError> {
    if active_queue_storage(pool).await?.is_some() {
        return Ok(0);
    }
    let mut total = 0i32;
    loop {
        let count: i32 = sqlx::query_scalar("SELECT awa.recompute_dirty_admin_metadata(100)")
            .fetch_one(pool)
            .await?;
        total += count;
        if count == 0 {
            break;
        }
    }
    Ok(total)
}

/// Full reconciliation of admin metadata counters from base tables.
///
/// Called infrequently by the maintenance leader (~60s) as a safety net
/// to correct any drift from skipped dirty keys. Also called during
/// migrate() to warm the cache.
pub async fn refresh_admin_metadata(pool: &PgPool) -> Result<(), AwaError> {
    if active_queue_storage(pool).await?.is_some() {
        return Ok(());
    }
    sqlx::query("SELECT awa.refresh_admin_metadata()")
        .execute(pool)
        .await?;
    Ok(())
}

/// Retry multiple jobs by ID. Only retries failed, cancelled, or waiting_external jobs.
pub async fn bulk_retry(pool: &PgPool, ids: &[i64]) -> Result<Vec<JobRow>, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        let (rows, _attempted) = store.retry_jobs_by_ids(pool, ids).await?;
        return Ok(rows);
    }

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
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Cancel multiple jobs by ID. Only cancels non-terminal jobs.
pub async fn bulk_cancel(pool: &PgPool, ids: &[i64]) -> Result<Vec<JobRow>, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        return store.cancel_jobs_by_ids(pool, ids).await;
    }

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
    .fetch_all(pool)
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
pub async fn state_timeseries(
    pool: &PgPool,
    minutes: i32,
) -> Result<Vec<StateTimeseriesBucket>, AwaError> {
    if let Some(store) = active_queue_storage(pool).await? {
        let sql = format!(
            "{} \
             SELECT \
                 date_trunc('minute', created_at) AS bucket, \
                 state, \
                 count(*) AS count \
             FROM recent_jobs \
             GROUP BY bucket, state \
             ORDER BY bucket",
            queue_storage_recent_jobs_cte(store.schema(), "$1")
        );
        let mut begin_guard = crate::read_transaction::BeginGuard::acquire(pool).await?;
        let mut tx = sqlx::Connection::begin(&mut *begin_guard.connection).await?;
        begin_guard.begin_pending = false;
        sqlx::query("SET LOCAL jit = off")
            .execute(tx.as_mut())
            .await?;
        // The window is measured on the database's clock, as on the canonical
        // engine, but bound as a value: Postgres won't push an expression
        // like `clock_timestamp()` into a bound on `finalized_at`.
        let since: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT clock_timestamp() - make_interval(mins => $1)")
                .bind(minutes)
                .fetch_one(tx.as_mut())
                .await?;
        let rows = sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>, JobState, i64)>(&sql)
            .bind(since)
            .fetch_all(tx.as_mut())
            .await?;
        tx.commit().await?;
        drop(begin_guard);

        return Ok(rows
            .into_iter()
            .map(|(bucket, state, count)| StateTimeseriesBucket {
                bucket,
                state,
                count,
            })
            .collect());
    }

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
    .fetch_all(pool)
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
pub async fn register_callback(
    pool: &PgPool,
    job_id: i64,
    run_lease: i64,
    timeout: std::time::Duration,
) -> Result<Uuid, AwaError> {
    let mut tx = pool.begin().await?;
    if let Some(store) = callback_job_queue_storage_in_tx(&mut tx, job_id).await? {
        tx.commit().await?;
        return store
            .register_callback(pool, job_id, run_lease, timeout)
            .await;
    }

    let callback_id = Uuid::new_v4();
    let timeout_secs = timeout.as_secs_f64();
    let result = sqlx::query(
        r#"UPDATE awa.jobs_hot
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
    .execute(tx.as_mut())
    .await?;
    if result.rows_affected() == 0 {
        return Err(AwaError::Validation("job is not in running state".into()));
    }
    tx.commit().await?;
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
/// callback pattern from ADR-021).
pub async fn complete_external(
    pool: &PgPool,
    callback_id: Uuid,
    payload: Option<serde_json::Value>,
    run_lease: Option<i64>,
) -> Result<JobRow, AwaError> {
    let mut tx = pool.begin().await?;
    let row = complete_external_in_tx(&mut tx, callback_id, payload, run_lease).await?;
    tx.commit().await?;
    Ok(row)
}

/// Transaction-aware variant of [`complete_external`] (ADR-029). Callers
/// that want to dispatch follow-up specs in the same transaction as the
/// callback completion should drive this directly and own the
/// `tx.commit()`. See [`crate::admin::resolve_callback_in_tx`] for the
/// policy-evaluating counterpart.
pub async fn complete_external_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    callback_id: Uuid,
    payload: Option<serde_json::Value>,
    run_lease: Option<i64>,
) -> Result<JobRow, AwaError> {
    complete_external_in_tx_inner(tx, callback_id, payload, run_lease, false).await
}

/// Complete a waiting job and resume the handler with the callback payload.
///
/// Like `complete_external`, but the job transitions to `running` instead of
/// `completed`, allowing the handler to continue with sequential callbacks.
/// The payload is stored in `metadata._awa_callback_result`.
pub async fn resume_external(
    pool: &PgPool,
    callback_id: Uuid,
    payload: Option<serde_json::Value>,
    run_lease: Option<i64>,
) -> Result<JobRow, AwaError> {
    let mut tx = pool.begin().await?;
    let row = resume_external_in_tx(&mut tx, callback_id, payload, run_lease).await?;
    tx.commit().await?;
    Ok(row)
}

/// Transaction-aware variant of [`resume_external`] (ADR-029). Resume is
/// not currently an ADR-029 outcome — the handler keeps running and will
/// emit its own completion event later — so callers that just need
/// payload delivery generally use [`resume_external`]. This `_in_tx`
/// shape exists for symmetry with the other callback transitions and so
/// applications can compose the resume `UPDATE` with their own
/// transaction-local writes.
pub async fn resume_external_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    callback_id: Uuid,
    payload: Option<serde_json::Value>,
    run_lease: Option<i64>,
) -> Result<JobRow, AwaError> {
    complete_external_in_tx_inner(tx, callback_id, payload, run_lease, true).await
}

async fn complete_external_in_tx_inner(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    callback_id: Uuid,
    payload: Option<serde_json::Value>,
    run_lease: Option<i64>,
    resume: bool,
) -> Result<JobRow, AwaError> {
    if let Some(store) = callback_queue_storage_in_tx(tx, callback_id).await? {
        return store
            .complete_external_in_tx(tx, callback_id, payload, run_lease, resume)
            .await;
    }

    let row = if resume {
        // Resume: transition to running, store payload, refresh heartbeat.
        // The handler is still alive and polling — it will detect the state change.
        let payload_json = payload.unwrap_or(serde_json::Value::Null);
        sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE awa.jobs_hot
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
        .fetch_optional(tx.as_mut())
        .await?
    } else {
        // Complete: terminal state, clear everything.
        sqlx::query_as::<_, JobRow>(
            r#"
            UPDATE awa.jobs_hot
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
        .fetch_optional(tx.as_mut())
        .await?
    };

    row.ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })
}

/// Fail a waiting job via external callback.
///
/// Records the error and transitions to `failed`.
pub async fn fail_external(
    pool: &PgPool,
    callback_id: Uuid,
    error: &str,
    run_lease: Option<i64>,
) -> Result<JobRow, AwaError> {
    let mut tx = pool.begin().await?;
    let row = fail_external_in_tx(&mut tx, callback_id, error, run_lease).await?;
    tx.commit().await?;
    Ok(row)
}

/// Transaction-aware variant of [`fail_external`] (ADR-029).
pub async fn fail_external_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    callback_id: Uuid,
    error: &str,
    run_lease: Option<i64>,
) -> Result<JobRow, AwaError> {
    if let Some(store) = callback_queue_storage_in_tx(tx, callback_id).await? {
        return store
            .fail_external_with_error_entry_in_tx(
                tx,
                callback_id,
                serde_json::json!({ "error": error }),
                run_lease,
            )
            .await;
    }

    let row = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs_hot
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
    .fetch_optional(tx.as_mut())
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
pub async fn retry_external(
    pool: &PgPool,
    callback_id: Uuid,
    run_lease: Option<i64>,
) -> Result<JobRow, AwaError> {
    let mut tx = pool.begin().await?;
    let row = retry_external_in_tx(&mut tx, callback_id, run_lease).await?;
    tx.commit().await?;
    Ok(row)
}

/// Transaction-aware variant of [`retry_external`] (ADR-029).
pub async fn retry_external_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    callback_id: Uuid,
    run_lease: Option<i64>,
) -> Result<JobRow, AwaError> {
    if let Some(store) = callback_queue_storage_in_tx(tx, callback_id).await? {
        return store.retry_external_in_tx(tx, callback_id, run_lease).await;
    }

    let row = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs_hot
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
    .fetch_optional(tx.as_mut())
    .await?;

    let row = row.ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })?;
    // jobs_hot only notifies on INSERT. This direct UPDATE must wake the
    // canonical dispatcher itself; PostgreSQL delivers it only on commit.
    sqlx::query("SELECT pg_notify('awa:' || $1, '')")
        .bind(&row.queue)
        .execute(tx.as_mut())
        .await?;
    Ok(row)
}

/// Reset the callback timeout for a long-running external operation.
///
/// External systems call this periodically to signal "still working" without
/// completing the job. Resets `callback_timeout_at` to `now() + timeout`.
/// The job stays in `waiting_external`.
///
/// Returns the updated job row, or `CallbackNotFound` if the callback ID
/// doesn't match a waiting job.
pub async fn heartbeat_callback(
    pool: &PgPool,
    callback_id: Uuid,
    timeout: std::time::Duration,
) -> Result<JobRow, AwaError> {
    let mut tx = pool.begin().await?;
    if let Some(store) = callback_queue_storage_in_tx(&mut tx, callback_id).await? {
        tx.commit().await?;
        return store.heartbeat_callback(pool, callback_id, timeout).await;
    }

    let timeout_secs = timeout.as_secs_f64();
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs_hot
        SET callback_timeout_at = now() + make_interval(secs => $2)
        WHERE callback_id = $1 AND state = 'waiting_external'
        RETURNING *
        "#,
    )
    .bind(callback_id)
    .bind(timeout_secs)
    .fetch_optional(tx.as_mut())
    .await?;

    tx.commit().await?;
    row.ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })
}

/// Cancel (clear) a registered callback for a running job.
///
/// Best-effort cleanup: returns `Ok(true)` if a row was updated,
/// `Ok(false)` if no match (already resolved, rescued, or wrong lease).
/// Callers should not treat `false` as an error.
pub async fn cancel_callback(pool: &PgPool, job_id: i64, run_lease: i64) -> Result<bool, AwaError> {
    let mut tx = pool.begin().await?;
    if let Some(store) = callback_job_queue_storage_in_tx(&mut tx, job_id).await? {
        tx.commit().await?;
        return store.cancel_callback(pool, job_id, run_lease).await;
    }

    let result = sqlx::query(
        r#"
        UPDATE awa.jobs_hot
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
    .execute(tx.as_mut())
    .await?;

    tx.commit().await?;
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
    let mut tx = pool.begin().await?;
    if let Some(store) = callback_job_queue_storage_in_tx(&mut tx, job_id).await? {
        tx.commit().await?;
        return store
            .enter_callback_wait(pool, job_id, run_lease, callback_id)
            .await;
    }

    let result = sqlx::query(
        r#"
        UPDATE awa.jobs_hot
        SET state = 'waiting_external',
            heartbeat_at = NULL,
            deadline_at = NULL
        WHERE id = $1 AND state = 'running' AND run_lease = $2 AND callback_id = $3
        "#,
    )
    .bind(job_id)
    .bind(run_lease)
    .bind(callback_id)
    .execute(tx.as_mut())
    .await?;

    tx.commit().await?;
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
    let mut tx = pool.begin().await?;
    if let Some(store) = callback_job_queue_storage_in_tx(&mut tx, job_id).await? {
        tx.commit().await?;
        return store.check_callback_state(pool, job_id, callback_id).await;
    }

    let row: Option<(JobState, Option<Uuid>, serde_json::Value)> = sqlx::query_as(
        "SELECT state, callback_id, metadata FROM awa.jobs_hot WHERE id = $1 FOR UPDATE",
    )
    .bind(job_id)
    .fetch_optional(tx.as_mut())
    .await?;

    let result = match row {
        Some((JobState::Running, None, metadata))
            if metadata.get("_awa_callback_result").is_some() =>
        {
            let payload = metadata
                .get("_awa_callback_result")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            sqlx::query(
                "UPDATE awa.jobs_hot SET metadata=metadata-'_awa_callback_result' WHERE id=$1",
            )
            .bind(job_id)
            .execute(tx.as_mut())
            .await?;
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
    };
    tx.commit().await?;
    result
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

    sqlx::query(
        "UPDATE awa.jobs_hot SET metadata = metadata - '_awa_callback_result' WHERE id = $1",
    )
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
pub async fn register_callback_with_config(
    pool: &PgPool,
    job_id: i64,
    run_lease: i64,
    timeout: std::time::Duration,
    config: &CallbackConfig,
) -> Result<Uuid, AwaError> {
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

    let mut tx = pool.begin().await?;
    if let Some(store) = callback_job_queue_storage_in_tx(&mut tx, job_id).await? {
        tx.commit().await?;
        return store
            .register_callback_with_config(pool, job_id, run_lease, timeout, config)
            .await;
    }

    let callback_id = Uuid::new_v4();
    let timeout_secs = timeout.as_secs_f64();

    let result = sqlx::query(
        r#"UPDATE awa.jobs_hot
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
    .execute(tx.as_mut())
    .await?;

    if result.rows_affected() == 0 {
        return Err(AwaError::Validation("job is not in running state".into()));
    }
    tx.commit().await?;
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
    let outcome =
        resolve_callback_in_tx(&mut tx, callback_id, payload, default_action, run_lease).await?;
    tx.commit().await?;
    Ok(outcome)
}

/// Transaction-aware variant of [`resolve_callback`] (ADR-029).
///
/// Performs callback lookup, CEL evaluation, and the chosen state
/// transition (Complete / Fail / Ignore) inside the caller-owned
/// transaction. The lookup takes a row lock — `SELECT ... FOR UPDATE` on
/// `awa.jobs_hot` for canonical storage; `FOR UPDATE OF lease` against
/// the active queue-storage `leases` table — so concurrent resolves are
/// serialised on the same `callback_id`.
///
/// Callers that want to dispatch ADR-029 follow-up specs in the same
/// transaction (e.g. `Client::resolve_callback`) drive this directly,
/// then dispatch specs against the returned [`ResolveOutcome`] before
/// committing.
pub async fn resolve_callback_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    callback_id: Uuid,
    payload: Option<serde_json::Value>,
    default_action: DefaultAction,
    run_lease: Option<i64>,
) -> Result<ResolveOutcome, AwaError> {
    if let Some(store) = callback_queue_storage_in_tx(tx, callback_id).await? {
        let job = store
            .callback_job_in_tx(tx, callback_id, run_lease, true)
            .await?
            .ok_or(AwaError::CallbackNotFound {
                callback_id: callback_id.to_string(),
            })?;

        let action = evaluate_or_default(&job, &payload, default_action)?;

        return match action {
            ResolveAction::Complete(transformed_payload) => {
                let completed_job = store
                    .complete_external_in_tx(tx, callback_id, None, run_lease, false)
                    .await?;
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

                let failed_job = store
                    .fail_external_with_error_entry_in_tx(tx, callback_id, error_json, run_lease)
                    .await?;
                Ok(ResolveOutcome::Failed { job: failed_job })
            }
            ResolveAction::Ignore(reason) => Ok(ResolveOutcome::Ignored { reason }),
        };
    }

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
    .fetch_optional(tx.as_mut())
    .await?
    .ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })?;

    let action = evaluate_or_default(&job, &payload, default_action)?;

    match action {
        ResolveAction::Complete(transformed_payload) => {
            let completed_job = sqlx::query_as::<_, JobRow>(
                r#"
                UPDATE awa.jobs_hot
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
            .fetch_one(tx.as_mut())
            .await?;

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
                UPDATE awa.jobs_hot
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
            .fetch_one(tx.as_mut())
            .await?;

            Ok(ResolveOutcome::Failed { job: failed_job })
        }
        ResolveAction::Ignore(reason) => {
            // No state change — caller's tx will commit cleanly without
            // changing the FOR UPDATE row, releasing the lock.
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
