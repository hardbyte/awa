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
    /// Historical samples of `canonical_live_backlog` anchored to the
    /// current `transition_epoch`. Only populated when the caller passes
    /// `?history=true` on `/api/storage`. The 0.6 server returns an empty
    /// slice because samples are accumulated client-side (no audit table
    /// — see issue #180); this field is kept additive so future versions
    /// that introduce server-side accumulation stay wire-compatible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<Vec<BacklogSample>>,
}

/// One sample of `canonical_live_backlog`, timestamped server-side. The UI
/// uses these to render an epoch-anchored sparkline showing the full
/// timeline of the current cutover (rather than a sliding window).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BacklogSample {
    pub at: DateTime<Utc>,
    pub canonical_live_backlog: i64,
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

/// Ring-cursor authority + fleet flip-readiness for one queue-storage
/// schema (#371 staged rolling upgrade). Mirrors `awa.ring_authority_status`.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow, PartialEq)]
pub struct RingAuthorityStatus {
    /// `"columns"` (compat; mixed 0.6.2/0.7 fleet safe) or `"ledger"`.
    pub authority: String,
    /// When the one-way flip to `ledger` happened (NULL until flipped, or
    /// set at install time for a fresh install that started in `ledger`).
    pub flipped_at: Option<DateTime<Utc>>,
    /// Fresh-heartbeat runtimes (within the freshness window).
    pub live_instances: i64,
    /// Fresh runtimes whose `binary_version` is >= the min flip version.
    pub flip_aware_instances: i64,
    /// Fresh runtimes NOT known to be flip-aware (NULL or old
    /// `binary_version`) — a manual flip refuses while this is > 0 unless
    /// forced.
    pub blocking_instances: i64,
}

/// Read the ring-cursor authority and flip-readiness for a schema.
pub async fn ring_authority_status<'e, E>(
    executor: E,
    schema: &str,
) -> Result<RingAuthorityStatus, AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, RingAuthorityStatus>("SELECT * FROM awa.ring_authority_status($1)")
        .bind(schema)
        .fetch_one(executor)
        .await
        .map_err(AwaError::from)
}

/// Flip a schema's ring-cursor authority `columns -> ledger` (one-way,
/// idempotent). Refuses unless `force` when a fresh-heartbeat runtime is not
/// known to be flip-aware. Returns the resulting authority (`"ledger"`).
pub async fn flip_ring_authority<'e, E>(
    executor: E,
    schema: &str,
    force: bool,
) -> Result<String, AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_scalar::<_, String>("SELECT awa.flip_ring_authority($1, $2)")
        .bind(schema)
        .bind(force)
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

/// Returns true only when every queue-storage substrate object the
/// runtime depends on is present. Workers and producers should treat
/// `false` as "schema not installed yet" and either wait or trigger
/// `QueueStorage::prepare_schema`.
///
/// The required objects (tables, the job-id sequence, the claim
/// function and its exact signature) originate from the v023 SQL helper,
/// called either by `awa migrate` for the default schema or by
/// [`QueueStorage::prepare_schema`](crate::QueueStorage::prepare_schema)
/// for custom schemas. If you change a required substrate object or the
/// `claim_ready_runtime` signature there, update this check at the same
/// time.
pub async fn queue_storage_schema_ready(pool: &PgPool, schema: &str) -> Result<bool, AwaError> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT
            to_regclass(format('%I.%I', $1, 'job_id_seq')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'lease_claim_receipt_id_seq')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'lease_claim_batch_id_seq')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'queue_ring_state')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'queue_ring_rotations')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'lease_ring_rotations')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'claim_ring_rotations')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'queue_terminal_rollup_deltas')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'ready_entries')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'ready_claim_attempt_batches')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'ready_tombstones')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'ready_segments')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'done_entries')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'receipt_completion_batches')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'receipt_completion_tombstones')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'queue_terminal_count_deltas')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'leases')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'deferred_jobs')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'lease_claims')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'lease_claim_batches')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'lease_claim_closures')) IS NOT NULL
            AND to_regclass(format('%I.%I', $1, 'lease_claim_closure_batches')) IS NOT NULL
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = $1
                  AND table_name = 'lease_claim_closure_batches'
                  AND column_name = 'receipt_ranges'
            )
            AND (
                SELECT count(*) = 3
                FROM information_schema.columns
                WHERE table_schema = $1
                  AND table_name = 'queue_claim_heads'
                  AND column_name = ANY(ARRAY[
                      'ready_segment_slot',
                      'ready_segment_generation',
                      'ready_segment_next_lane_seq'
                  ])
            )
            AND EXISTS (
                SELECT 1
                FROM pg_proc AS p
                JOIN pg_namespace AS n
                  ON n.oid = p.pronamespace
                WHERE n.nspname = $1
                  AND p.oid = to_regprocedure(
                      format('%I.%I(text,bigint,double precision,double precision)', $1, 'claim_ready_runtime')
                  )::oid
                  AND position('receipt_id' IN pg_get_function_result(p.oid)) > 0
                  AND position('claim_batch_id' IN pg_get_function_result(p.oid)) > 0
                  AND position('claim_batch_index' IN pg_get_function_result(p.oid)) > 0
            )
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
    let mut live_queue_storage_targets = 0usize;
    for instance in instances.into_iter().filter(|instance| !instance.stale) {
        *live_runtime_capability_counts
            .entry(instance.storage_capability.as_str().to_string())
            .or_insert(0) += 1;
        if instance.transition_role == admin::TransitionRole::QueueStorageTarget
            && instance.storage_capability == admin::StorageCapability::QueueStorage
        {
            live_queue_storage_targets += 1;
        }
    }

    let live_canonical = *live_runtime_capability_counts
        .get("canonical")
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
    if live_queue_storage_targets == 0 {
        enter_mixed_transition_blockers.push(
            "no live runtimes with transition_role=queue_storage_target are reporting; \
             auto-role runtimes downgrade to drain-only after the routing flip"
                .to_string(),
        );
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
        history: None,
    })
}
