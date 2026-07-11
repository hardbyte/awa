use crate::admin::{CallbackConfig, CallbackPollResult};
use crate::dlq::{ListDlqFilter, RetryFromDlqOpts};
use crate::error::AwaError;
use crate::insert::prepare_row_raw;
use crate::{InsertParams, JobRow, JobState};
use chrono::TimeDelta;
use chrono::{DateTime, Utc};
use sqlx::Acquire;
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_SCHEMA: &str = "awa";
const DEFAULT_QUEUE_SLOT_COUNT: usize = 16;
const DEFAULT_LEASE_SLOT_COUNT: usize = 8;
const DEFAULT_CLAIM_SLOT_COUNT: usize = 8;
const DEFAULT_QUEUE_STRIPE_COUNT: usize = 1;
const QUEUE_STRIPE_DELIMITER: &str = "#";
const COPY_NULL_SENTINEL: &str = "__AWA_NULL__";
const COPY_CHUNK_TARGET_BYTES: usize = 256 * 1024;
const TERMINAL_COUNTER_BUCKETS: i16 = 256;
const RECEIPT_RESCUE_BATCH_LIMIT: i64 = 500;
const RECEIPT_RESCUE_CURSOR_SCAN_LIMIT: i64 = 10_000;
const RECEIPT_DEADLINE_RESCUE_CURSOR_SCAN_LIMIT: i64 = 10_000;
const DEFAULT_PRUNE_LOCK_TIMEOUT: Duration = Duration::from_millis(10);

/// Portable 64-bit hash over raw ordering-key bytes.
///
/// This is intentionally simple enough to implement byte-for-byte in
/// `awa.insert_job_compat`, so SQL, Rust, and Python producers that
/// pass the same ordering-key bytes can derive the same routing facts.
pub fn ordering_key_hash64(ordering_key: &[u8]) -> u64 {
    let mut hash: u128 = 14_695_981_039_346_656_037;
    const PRIME: u128 = 1_099_511_628_211;
    const MASK: u128 = u64::MAX as u128;
    for byte in ordering_key {
        hash = hash.wrapping_mul(PRIME).wrapping_add(*byte as u128) & MASK;
    }
    hash as u64
}

/// Deterministically map an ordering key to a shard in `[0, shards)`.
///
/// Inputs sharing a key always produce the same shard, which is what
/// lets producers preserve FIFO within a key when the destination
/// queue is sharded. `shards <= 1` returns shard 0 unconditionally.
///
/// This deliberately remains the raw `ordering_key_hash64(key) % shards`
/// mapping used by SQL compatibility producers.
pub fn shard_for_ordering_key(ordering_key: &[u8], shards: i16) -> i16 {
    if shards <= 1 {
        return 0;
    }
    (ordering_key_hash64(ordering_key) % shards as u64) as i16
}

fn terminal_counter_bucket(job_id: i64) -> i16 {
    job_id.rem_euclid(TERMINAL_COUNTER_BUCKETS as i64) as i16
}

#[derive(Debug, Clone)]
pub struct QueueStorageConfig {
    pub schema: String,
    pub queue_slot_count: usize,
    pub lease_slot_count: usize,
    /// Number of child partitions the receipt ring splits
    /// `lease_claims`, `lease_claim_batches`, and receipt-closure evidence
    /// across (ADR-023).
    /// Mirrors `lease_slot_count`: a small fixed set of slots
    /// reclaimed by rotation + TRUNCATE rather than by row-level
    /// DELETE.
    pub claim_slot_count: usize,
    pub queue_stripe_count: usize,
    /// Use the receipt-plane short path: claim writes compact batches into
    /// `lease_claim_batches` for both zero-deadline and deadline-backed
    /// attempts (#246) — a batch shares one `claimed_at`, hence one
    /// `deadline_at`, and expired batches are swept by the compact batch
    /// deadline-rescue cursor. Row-local `lease_claims` evidence remains
    /// readable for legacy in-flight claims mid-upgrade. Compact claims
    /// can still materialize into `leases` when the worker later needs mutable
    /// attempt state. Successful compact
    /// completion writes durable terminal history through
    /// `receipt_completion_batches` and compact claim-local closure evidence
    /// in `lease_claim_closure_batches`, or falls back to `done_entries`, while
    /// non-success exits and cold terminal-delete paths materialize explicit
    /// closures in `lease_claim_closures`.
    /// Receipt claim evidence is reclaimed by claim-ring rotation +
    /// TRUNCATE.
    /// Default `true`.
    /// Set to `false` to force every claim through the legacy
    /// `leases` materialization path.
    pub lease_claim_receipts: bool,
}

impl Default for QueueStorageConfig {
    fn default() -> Self {
        Self {
            schema: DEFAULT_SCHEMA.to_string(),
            queue_slot_count: DEFAULT_QUEUE_SLOT_COUNT,
            lease_slot_count: DEFAULT_LEASE_SLOT_COUNT,
            claim_slot_count: DEFAULT_CLAIM_SLOT_COUNT,
            queue_stripe_count: DEFAULT_QUEUE_STRIPE_COUNT,
            lease_claim_receipts: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct ClaimedEntry {
    pub queue: String,
    pub priority: i16,
    pub lane_seq: i64,
    pub ready_slot: i32,
    pub ready_generation: i64,
    pub lease_slot: i32,
    pub lease_generation: i64,
    /// ADR-023: the `claim_slot` partition this attempt's receipt
    /// evidence landed in. Receipt-closure paths use this to co-locate
    /// explicit `lease_claim_closures`; compact successful receipt
    /// completions keep claim-local evidence in
    /// `lease_claim_closure_batches` instead.
    pub claim_slot: i32,
    /// Stable identity for immutable receipt claim evidence. Present for
    /// receipt-backed claims and used by the compact completion path to
    /// validate the exact claim attempt without row-locking it.
    pub receipt_id: Option<i64>,
    /// Compact claim batch row identity for receipt claims (zero-deadline
    /// and deadline-backed alike since #246). Row-local receipt claims
    /// leave this unset.
    pub claim_batch_id: Option<i64>,
    /// One-based item position inside `lease_claim_batches.*_ids` arrays.
    /// Present with `claim_batch_id` and used as an O(1) compact proof.
    pub claim_batch_index: Option<i32>,
    pub lease_claim_receipt: bool,
    /// The enqueue shard the row was claimed from. Routes the
    /// terminal `done_entries` write onto the correct shard's
    /// `(ready_slot, queue, priority, enqueue_shard, lane_seq)` key
    /// and is the join predicate for receipt and admin lookups that
    /// touch `queue_claim_heads` / `ready_entries` / `leases`.
    pub enqueue_shard: i16,
}

#[derive(Debug, Clone)]
pub struct ClaimedRuntimeJob {
    pub claim: ClaimedEntry,
    pub job: JobRow,
    pub unique_states: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct QueueClaimerLease {
    pub claimer_slot: i16,
    pub lease_epoch: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
struct QueueClaimerLeaseRow {
    claimer_slot: i16,
    lease_epoch: i64,
    last_claimed_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

impl QueueClaimerLeaseRow {
    fn lease(self) -> QueueClaimerLease {
        QueueClaimerLease {
            claimer_slot: self.claimer_slot,
            lease_epoch: self.lease_epoch,
        }
    }

    fn needs_refresh(
        self,
        now: DateTime<Utc>,
        lease_ttl: Duration,
        idle_threshold: Duration,
    ) -> bool {
        let Ok(idle_refresh_delta) = TimeDelta::from_std(idle_threshold / 2) else {
            return true;
        };
        let Ok(expiry_refresh_delta) = TimeDelta::from_std(lease_ttl / 2) else {
            return true;
        };

        self.last_claimed_at <= now - idle_refresh_delta
            || self.expires_at <= now + expiry_refresh_delta
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct QueueClaimerState {
    pub target_claimers: i16,
}

impl ClaimedRuntimeJob {
    fn into_done_row(self, finalized_at: DateTime<Utc>) -> Result<DoneJobRow, AwaError> {
        let payload = QueueStorage::payload_from_parts(
            self.job.metadata,
            self.job.tags,
            self.job.errors,
            None,
        )?;

        Ok(DoneJobRow {
            ready_slot: self.claim.ready_slot,
            ready_generation: self.claim.ready_generation,
            job_id: self.job.id,
            kind: self.job.kind,
            queue: self.job.queue,
            args: self.job.args,
            state: JobState::Completed,
            priority: self.claim.priority,
            attempt: self.job.attempt,
            run_lease: self.job.run_lease,
            max_attempts: self.job.max_attempts,
            lane_seq: self.claim.lane_seq,
            enqueue_shard: self.claim.enqueue_shard,
            run_at: self.job.run_at,
            attempted_at: self.job.attempted_at,
            finalized_at,
            created_at: self.job.created_at,
            unique_key: self.job.unique_key,
            unique_states: self.unique_states,
            payload,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueCounts {
    pub available: i64,
    pub running: i64,
    /// Count of rows in *any* terminal state (`completed`, `failed`, or
    /// `cancelled`) for the queue. The name reflects what the field
    /// actually counts: it is `{schema}.terminal_jobs` semantics, not
    /// `count(*) WHERE state = 'completed'`. Physical terminal facts may
    /// live in `done_entries` or compact receipt completion batches.
    /// The historical name `completed` was a misnomer —
    /// `queue_counts_exact` has always included failed and cancelled
    /// terminals; renamed in #290 along with the counter-backed read
    /// path.
    pub terminal: i64,
    /// Cumulative count of `failed` terminal rows pruned past the
    /// failed-retention floor, summed from
    /// `queue_terminal_rollups.pruned_failed_count`. These rows no
    /// longer exist in `done_entries` and cannot be retried.
    /// Monotonically non-decreasing — rollups never shrink.
    pub pruned_failed: i64,
}

/// Cheap available-only signal used by the dispatcher's claimer-sizing
/// control loop. Derives the count from enqueue and claim sequence cursors
/// summed over the queue's physical stripes — two PK reads per lane, O(few
/// rows) regardless of backlog size.
///
/// This is intentionally a separate type from [`QueueCounts`]: the
/// dispatcher claim hot path only consumes the available count, and
/// returning a `QueueCounts` with two perpetually-zero fields would
/// invite future code to read `.running` or `.terminal` and silently
/// get wrong answers. Code that legitimately needs the full counts
/// should call [`QueueStorage::queue_counts`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AvailableSignal {
    pub available: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotateOutcome {
    Rotated {
        slot: i32,
        generation: i64,
    },
    /// Target slot has live state; rotation deferred. `busy` carries a
    /// bounded per-table presence indicator observed at the gate (only fields
    /// relevant to the ring being rotated are populated).
    SkippedBusy {
        slot: i32,
        busy: BusyCounts,
    },
    /// Ring is idle: every child table of the *current* slot is empty, so
    /// there is nothing to seal and rotation was skipped without updating
    /// any ring-state row. Issue #371: timer-driven rotation of an idle
    /// ring UPDATEs the singleton ring-state row every tick, and under a
    /// pinned MVCC horizon each of those dead tuples stays unreclaimable.
    /// `slot` is the current slot, which stays open.
    SkippedIdle {
        slot: i32,
    },
}

/// Per-table presence observed at a rotation gate. Each non-zero value means
/// "this table was non-empty"; it is not an exact row count. Each ring populates
/// only the fields meaningful for it; unused fields stay zero. The
/// maintenance loop emits one OTel metric label per non-zero field so
/// dashboards can attribute "rotation pinned" to the responsible side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BusyCounts {
    /// Queue ring: rows in the next `ready_entries` child.
    pub queue_ready: i64,
    /// Queue ring: rows in the next `ready_claim_attempt_batches` child.
    pub queue_claim_attempt_batches: i64,
    /// Queue ring: rows in the next `done_entries` child.
    pub queue_done: i64,
    /// Queue ring: rows in the next `ready_tombstones` child.
    pub queue_tombstones: i64,
    /// Queue ring: rows in the next `ready_segments` child.
    pub queue_ready_segments: i64,
    /// Queue ring: rows in the next `receipt_completion_batches` child.
    pub queue_receipt_completion_batches: i64,
    /// Queue ring: rows in the next `receipt_completion_tombstones` child.
    pub queue_receipt_completion_tombstones: i64,
    /// Queue ring: rows in the next `queue_terminal_count_deltas` child.
    pub queue_terminal_deltas: i64,
    /// Lease ring: rows in the next `leases` child.
    pub leases: i64,
    /// Claim ring: rows in the next `lease_claims` child.
    pub claims: i64,
    /// Claim ring: rows in the next `lease_claim_closures` child.
    pub closures: i64,
    /// Claim ring: rows in the next compact closure-batch child.
    pub closure_batches: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneOutcome {
    Noop,
    Pruned {
        slot: i32,
        /// Failed terminal rows inside the failed-retention floor that
        /// the queue prune re-homed into the live `done_entries`
        /// segment instead of dropping. Always zero for the lease and
        /// claim rings.
        carried_failed_rows: u64,
    },
    /// Lock acquisition timed out (held-tx, lock contention).
    Blocked {
        slot: i32,
    },
    /// Target slot still has live state. `reason` discriminates which gate
    /// fired and `count` gives its magnitude.
    SkippedActive {
        slot: i32,
        reason: SkipReason,
        count: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TerminalDeltaRollupOutcome {
    pub rolled_slots: usize,
    pub delta_rows: i64,
    pub grouped_keys: i64,
    pub skipped_active_slots: usize,
    pub blocked_slots: usize,
    pub skipped_mvcc_pinned: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalDeltaSlotRollup {
    Empty,
    Rolled { delta_rows: i64, grouped_keys: i64 },
    SkippedActive,
    SkippedMvccPinned,
    Blocked,
}

/// Outcome of [`QueueStorage::fold_terminal_rollup_deltas`] (#371).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TerminalRollupFoldOutcome {
    /// Delta rows consumed (deleted) by the fold.
    pub folded_delta_rows: usize,
    /// Distinct (queue, priority) keys upserted into the rollups.
    pub folded_keys: usize,
    /// True when the fold stood down because another backend pinned the
    /// MVCC horizon; the delta rows stay visible to exact readers.
    pub skipped_mvcc_pinned: bool,
}

/// Outcome of [`QueueStorage::fold_ring_rotation_ledgers`] (#371).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RingLedgerFoldOutcome {
    /// Ledger rows trimmed across the queue, lease, and claim rings.
    pub trimmed_rows: u64,
    /// True when the fold stood down because another backend pinned the
    /// MVCC horizon; the ledgers keep their full history until the next
    /// clear-horizon tick.
    pub skipped_mvcc_pinned: bool,
}

/// Discriminator for [`PruneOutcome::SkippedActive`].
///
/// Multiple gates can fire `SkippedActive` for the same ring (e.g. queue
/// prune checks both `active_leases` and `pending_ready`). Carrying the
/// reason separately from `count` lets dashboards split out "ring saturated
/// because backlog never drained" from "leases lingering on prior
/// generation" without re-parsing log lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// Queue prune: leases on the prior generation persist.
    QueueActiveLeases,
    /// Queue prune: receipt claims still need same-slot terminal evidence.
    QueueUnclosedClaimRefs,
    /// Queue prune: ready rows without matching done or tombstone evidence.
    QueuePendingReady,
    /// Lease prune: target slot equals the current slot (rotator race).
    LeaseCurrent,
    /// Lease prune: pending leases on target slot.
    LeaseActive,
    /// Claim prune: target slot equals the current slot (rotator race).
    ClaimCurrent,
    /// Claim prune: open claims on target slot (no closure evidence).
    ClaimOpen,
}

impl SkipReason {
    /// Stable, low-cardinality label suitable for OTel metric attributes.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::QueueActiveLeases => "queue.active_leases",
            Self::QueueUnclosedClaimRefs => "queue.unclosed_claim_refs",
            Self::QueuePendingReady => "queue.pending_ready",
            Self::LeaseCurrent => "lease.current",
            Self::LeaseActive => "lease.active",
            Self::ClaimCurrent => "claim.current",
            Self::ClaimOpen => "claim.open",
        }
    }
}

fn map_sqlx_error(err: sqlx::Error) -> AwaError {
    if let sqlx::Error::Database(ref db_err) = err {
        if db_err.code().as_deref() == Some("23505") {
            return AwaError::UniqueConflict {
                constraint: db_err.constraint().map(|c| c.to_string()),
            };
        }
    }
    AwaError::Database(err)
}

fn is_lock_contention_error(err: &sqlx::Error) -> bool {
    matches!(
        err,
        sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("55P03")
    )
}

async fn set_prune_lock_timeout_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    timeout: Duration,
) -> Result<(), AwaError> {
    let millis = timeout.as_millis().max(1).min(i64::MAX as u128);
    let timeout = format!("{millis}ms");
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(timeout)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
    Ok(())
}

fn validate_ident(ident: &str) -> Result<(), AwaError> {
    let mut chars = ident.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() || first == '_' => {}
        _ => {
            return Err(AwaError::Validation(format!(
                "invalid SQL identifier: {ident}"
            )));
        }
    }

    if chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        Ok(())
    } else {
        Err(AwaError::Validation(format!(
            "invalid SQL identifier: {ident}"
        )))
    }
}

fn ready_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.ready_entries_{slot}")
}

fn ready_claim_attempt_batch_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.ready_claim_attempt_batches_{slot}")
}

fn done_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.done_entries_{slot}")
}

fn ready_tombstone_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.ready_tombstones_{slot}")
}

fn ready_segment_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.ready_segments_{slot}")
}

fn receipt_completion_batch_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.receipt_completion_batches_{slot}")
}

fn receipt_completion_tombstone_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.receipt_completion_tombstones_{slot}")
}

fn terminal_delta_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.queue_terminal_count_deltas_{slot}")
}

fn done_ready_join(schema: &str, done_alias: &str, ready_alias: &str) -> String {
    format!(
        r#"
            LEFT JOIN {schema}.ready_entries AS {ready_alias}
              ON {ready_alias}.ready_slot = {done_alias}.ready_slot
             AND {ready_alias}.ready_generation = {done_alias}.ready_generation
             AND {ready_alias}.queue = {done_alias}.queue
             AND {ready_alias}.priority = {done_alias}.priority
             AND {ready_alias}.enqueue_shard = {done_alias}.enqueue_shard
             AND {ready_alias}.lane_seq = {done_alias}.lane_seq
        "#
    )
}

fn done_row_projection(done_alias: &str, ready_alias: &str) -> String {
    format!(
        r#"
                {done_alias}.ready_slot,
                {done_alias}.ready_generation,
                {done_alias}.job_id,
                {done_alias}.kind,
                {done_alias}.queue,
                COALESCE({done_alias}.args, {ready_alias}.args, '{{}}'::jsonb) AS args,
                {done_alias}.state,
                {done_alias}.priority,
                {done_alias}.attempt,
                {done_alias}.run_lease,
                COALESCE({done_alias}.max_attempts, {ready_alias}.max_attempts, 25::smallint) AS max_attempts,
                {done_alias}.lane_seq,
                {done_alias}.enqueue_shard,
                COALESCE({done_alias}.run_at, {ready_alias}.run_at, {done_alias}.finalized_at) AS run_at,
                COALESCE({done_alias}.attempted_at, {ready_alias}.attempted_at) AS attempted_at,
                {done_alias}.finalized_at,
                COALESCE({done_alias}.created_at, {ready_alias}.created_at, {done_alias}.finalized_at) AS created_at,
                COALESCE({done_alias}.unique_key, {ready_alias}.unique_key) AS unique_key,
                COALESCE({done_alias}.unique_states, {ready_alias}.unique_states) AS unique_states,
                COALESCE({done_alias}.payload, {ready_alias}.payload, '{{}}'::jsonb) AS payload
        "#
    )
}

fn lease_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.leases_{slot}")
}

fn claim_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.lease_claims_{slot}")
}

fn claim_batch_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.lease_claim_batches_{slot}")
}

fn closure_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.lease_claim_closures_{slot}")
}

fn claim_closure_batch_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.lease_claim_closure_batches_{slot}")
}

fn ring_slot_index(slot: i32, slot_count: usize, ring: &str) -> Result<usize, AwaError> {
    let index = usize::try_from(slot).map_err(|_| {
        AwaError::Validation(format!(
            "invalid {ring} ring slot {slot}: slot must be non-negative"
        ))
    })?;
    if index >= slot_count {
        return Err(AwaError::Validation(format!(
            "invalid {ring} ring slot {slot}: configured slot count is {slot_count}"
        )));
    }
    Ok(index)
}

fn receipt_closed_evidence_sql(
    schema: &str,
    closure_rel: &str,
    closure_batch_rel: &str,
    claims_alias: &str,
) -> String {
    // `receipt_id` is allocated from a global sequence, so it identifies a
    // claim without a `claim_slot` predicate. Compact closure batches retain
    // the raw receipt array for audit/debugging, but hot membership checks use
    // the per-child GiST index on the derived multirange.
    format!(
        r#"
        (
            {claims_alias}.closed_at IS NOT NULL
            OR EXISTS (
                SELECT 1 FROM {closure_rel} AS closures
                WHERE closures.claim_slot = {claims_alias}.claim_slot
                  AND closures.job_id = {claims_alias}.job_id
                  AND closures.run_lease = {claims_alias}.run_lease
            )
            OR EXISTS (
                SELECT 1
                FROM {closure_batch_rel} AS closure_batches
                WHERE closure_batches.receipt_ranges @> {claims_alias}.receipt_id
            )
            OR EXISTS (
                SELECT 1 FROM {schema}.done_entries AS done
                WHERE done.job_id = {claims_alias}.job_id
                  AND done.run_lease = {claims_alias}.run_lease
            )
            OR EXISTS (
                SELECT 1 FROM {schema}.deferred_jobs AS deferred
                WHERE deferred.job_id = {claims_alias}.job_id
                  AND deferred.run_lease = {claims_alias}.run_lease
            )
            OR EXISTS (
                SELECT 1 FROM {schema}.dlq_entries AS dlq
                WHERE dlq.job_id = {claims_alias}.job_id
                  AND dlq.run_lease = {claims_alias}.run_lease
            )
        )
        "#
    )
}

fn busy_indicator(has_rows: bool) -> i64 {
    if has_rows {
        1
    } else {
        0
    }
}

async fn queue_prune_has_active_leases_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    schema: &str,
    slot: i32,
    generation: i64,
) -> Result<bool, AwaError> {
    sqlx::query_scalar(&format!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM {schema}.leases
            WHERE ready_slot = $1
              AND ready_generation = $2
            LIMIT 1
        )
        "#
    ))
    .bind(slot)
    .bind(generation)
    .fetch_one(tx.as_mut())
    .await
    .map_err(map_sqlx_error)
}

async fn queue_prune_has_pending_ready_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    schema: &str,
    ready_child: &str,
    generation: i64,
) -> Result<bool, AwaError> {
    sqlx::query_scalar(&format!(
        r#"
        WITH claim_cursors AS MATERIALIZED (
            SELECT
                queue,
                priority,
                enqueue_shard,
                {schema}.sequence_next_value(seq_name) AS claim_seq
            FROM {schema}.queue_claim_heads
        )
        SELECT EXISTS (
            SELECT 1
            FROM claim_cursors AS claims
            CROSS JOIN LATERAL (
                SELECT 1
                FROM {ready_child} AS ready
                WHERE ready.ready_generation = $1
                  AND ready.queue = claims.queue
                  AND ready.priority = claims.priority
                  AND ready.enqueue_shard = claims.enqueue_shard
                  AND ready.lane_seq >= claims.claim_seq
                LIMIT 1
            ) AS pending_ready
            LIMIT 1
        )
        "#
    ))
    .bind(generation)
    .fetch_one(tx.as_mut())
    .await
    .map_err(map_sqlx_error)
}

async fn queue_prune_has_unclosed_claim_refs_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    schema: &str,
    slot: i32,
    generation: i64,
) -> Result<bool, AwaError> {
    let count_proves_claim_refs_closed: bool = sqlx::query_scalar(&format!(
        r#"
        WITH claim_count AS (
            SELECT count(*)::bigint AS total
            FROM {schema}.lease_claims AS claims
            WHERE claims.ready_slot = $1
              AND claims.ready_generation = $2
        ),
        compact_claim_count AS (
            SELECT COALESCE(sum(claimed_count), 0)::bigint AS total
            FROM {schema}.lease_claim_batches AS batches
            WHERE batches.ready_slot = $1
              AND batches.ready_generation = $2
        ),
        explicit_count AS (
            SELECT count(*)::bigint AS total
            FROM {schema}.lease_claims AS claims
            JOIN {schema}.lease_claim_closures AS closures
              ON closures.claim_slot = claims.claim_slot
             AND closures.job_id = claims.job_id
             AND closures.run_lease = claims.run_lease
            WHERE claims.ready_slot = $1
              AND claims.ready_generation = $2
        ),
        compact_count AS (
            SELECT COALESCE(sum(closed_count), 0)::bigint AS total
            FROM {schema}.lease_claim_closure_batches AS batches
            WHERE batches.ready_slot = $1
              AND batches.ready_generation = $2
        )
        SELECT claim_count.total + compact_claim_count.total =
               explicit_count.total + compact_count.total
        FROM claim_count, compact_claim_count, explicit_count, compact_count
        "#
    ))
    .bind(slot)
    .bind(generation)
    .fetch_one(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;
    if count_proves_claim_refs_closed {
        return Ok(false);
    }

    // The exact anti-join over compact claim batches requires unnesting
    // retained claim history. Under a hot MVCC horizon that proof can become
    // more expensive than the prune it guards. A count mismatch is enough to
    // make truncate unsafe, so skip and try again after closures catch up.
    Ok(true)
}

async fn claim_prune_has_open_claims_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    _schema: &str,
    claim_child: &str,
    claim_batch_child: &str,
    closure_child: &str,
    closure_batch_child: &str,
) -> Result<bool, AwaError> {
    let count_proves_claims_closed: bool = sqlx::query_scalar(&format!(
        r#"
        WITH claim_count AS (
            SELECT count(*)::bigint AS total FROM {claim_child}
        ),
        compact_claim_count AS (
            SELECT COALESCE(sum(claimed_count), 0)::bigint AS total
            FROM {claim_batch_child}
        ),
        explicit_count AS (
            SELECT count(*)::bigint AS total FROM {closure_child}
        ),
        compact_count AS (
            SELECT COALESCE(sum(closed_count), 0)::bigint AS total
            FROM {closure_batch_child}
        )
        SELECT claim_count.total + compact_claim_count.total =
               explicit_count.total + compact_count.total
        FROM claim_count, compact_claim_count, explicit_count, compact_count
        "#
    ))
    .fetch_one(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;
    if count_proves_claims_closed {
        return Ok(false);
    }

    // A count mismatch means at least one claim is not proven closed by the
    // append-only closure ledgers. Returning SkippedActive is conservative and
    // avoids an unbounded anti-join over retained compact claim batches.
    Ok(true)
}

fn oldest_initialized_ring_slot(
    current_slot: i32,
    generation: i64,
    slot_count: i32,
) -> Option<(i32, i64)> {
    if slot_count <= 1 {
        return None;
    }

    let initialized_slots = (generation + 1).min(slot_count as i64) as i32;
    if initialized_slots <= 1 {
        return None;
    }

    let offset = initialized_slots - 1;
    let oldest_slot = (current_slot - offset).rem_euclid(slot_count);
    let oldest_generation = generation - offset as i64;
    if oldest_generation < 0 {
        return None;
    }

    Some((oldest_slot, oldest_generation))
}

/// Ring families that share the append-only rotation-ledger pattern
/// (#371). The current cursor of each ring is the max-generation row of
/// `{schema}.{ring}_ring_rotations`; the `{ring}_ring_state` singleton
/// holds only cold config (`slot_count`, plus the #290 trust marker on
/// the queue ring).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RingFamily {
    Queue,
    Lease,
    Claim,
}

impl RingFamily {
    fn name(self) -> &'static str {
        match self {
            RingFamily::Queue => "queue",
            RingFamily::Lease => "lease",
            RingFamily::Claim => "claim",
        }
    }

    fn ledger_relname(self) -> &'static str {
        match self {
            RingFamily::Queue => "queue_ring_rotations",
            RingFamily::Lease => "lease_ring_rotations",
            RingFamily::Claim => "claim_ring_rotations",
        }
    }

    fn state_relname(self) -> &'static str {
        match self {
            RingFamily::Queue => "queue_ring_state",
            RingFamily::Lease => "lease_ring_state",
            RingFamily::Claim => "claim_ring_state",
        }
    }

    /// Advisory-lock name serializing rotate ↔ prune ↔ delta-rollup for
    /// one ring. Replaces the pre-#371 `FOR UPDATE` on the ring-state
    /// singleton: the row is gone (cursor lives in the append-only
    /// ledger), but the mutual exclusion it provided is still required —
    /// on a fully-initialized ring the rotation target and the prune
    /// target are the same slot, and rotating into a slot mid-truncate
    /// would route fresh writes into a partition being wiped.
    fn rotation_lock_name(self, schema: &str) -> String {
        format!("awa.queue_storage.ring_rotation:{schema}:{}", self.name())
    }
}

/// Latest generation at which `slot` was the ring's open slot, derived
/// from the current cursor. Rotation advances the cursor slot and the
/// generation in lock-step by exactly one (`slot = generation mod
/// slot_count`, genesis at `(0, 0)`), so the ledger's max row determines
/// every older slot's last-open generation without per-slot state.
/// Returns `None` for slots the ring has not rotated through yet.
fn ring_slot_generation(
    current_slot: i32,
    generation: i64,
    slot_count: i32,
    slot: i32,
) -> Option<i64> {
    if slot_count <= 0 || slot < 0 || slot >= slot_count {
        return None;
    }
    let offset = (current_slot - slot).rem_euclid(slot_count) as i64;
    let slot_generation = generation - offset;
    (slot_generation >= 0).then_some(slot_generation)
}

/// All sealed (initialized, non-current) ring slots with their last-open
/// generations, ordered by generation ascending — the same order the
/// pre-#371 `queue_ring_slots` scan (`WHERE generation >= 0 AND slot <>
/// current ORDER BY generation`) produced.
fn initialized_sealed_ring_slots(
    current_slot: i32,
    generation: i64,
    slot_count: i32,
) -> Vec<(i32, i64)> {
    if slot_count <= 1 {
        return Vec::new();
    }
    let initialized_slots = (generation + 1).min(slot_count as i64) as i32;
    (1..initialized_slots)
        .rev()
        .map(|offset| {
            (
                (current_slot - offset).rem_euclid(slot_count),
                generation - offset as i64,
            )
        })
        .collect()
}

#[cfg(test)]
mod identifier_tests {
    use super::{validate_ident, QueueStorage, QueueStorageConfig};

    #[test]
    fn queue_storage_schema_identifiers_are_lowercase_unquoted_names() {
        for ident in ["awa", "awa_queue_storage", "_awa123"] {
            validate_ident(ident).expect("identifier should be accepted");
        }

        for ident in ["Awa", "awa-queue", "123awa", "awa.queue"] {
            assert!(
                validate_ident(ident).is_err(),
                "identifier should be rejected: {ident}"
            );
        }
    }

    #[test]
    fn default_queue_storage_schema_requires_default_physical_shape() {
        for config in [
            QueueStorageConfig {
                queue_slot_count: 32,
                ..Default::default()
            },
            QueueStorageConfig {
                lease_slot_count: 4,
                ..Default::default()
            },
            QueueStorageConfig {
                claim_slot_count: 4,
                ..Default::default()
            },
            QueueStorageConfig {
                lease_claim_receipts: false,
                ..Default::default()
            },
        ] {
            let err = QueueStorage::new(config).expect_err("default awa schema shape must reject");
            assert!(
                err.to_string()
                    .contains("default `awa` queue-storage schema"),
                "unexpected error: {err}"
            );
        }

        QueueStorage::new(QueueStorageConfig {
            schema: "awa_custom".to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            lease_claim_receipts: false,
            ..Default::default()
        })
        .expect("custom schema should allow custom physical shape");
    }
}

#[cfg(test)]
mod shard_routing_tests {
    use super::shard_for_ordering_key;
    use std::collections::HashSet;

    #[test]
    fn shards_le_one_collapse_to_zero() {
        assert_eq!(shard_for_ordering_key(b"customer-42", 1), 0);
        assert_eq!(shard_for_ordering_key(b"", 1), 0);
        assert_eq!(shard_for_ordering_key(b"customer-42", 0), 0);
    }

    #[test]
    fn same_key_lands_on_same_shard() {
        let key = b"customer-42";
        let first = shard_for_ordering_key(key, 8);
        for _ in 0..100 {
            assert_eq!(shard_for_ordering_key(key, 8), first);
        }
    }

    #[test]
    fn shard_is_within_range() {
        for n in 0..256u32 {
            let key = format!("order-{n}");
            let shard = shard_for_ordering_key(key.as_bytes(), 8);
            assert!((0..8).contains(&shard));
        }
    }

    #[test]
    fn distinct_keys_spread_across_shards() {
        let mut hit: HashSet<i16> = HashSet::new();
        for n in 0..1024u32 {
            let key = format!("order-{n}");
            hit.insert(shard_for_ordering_key(key.as_bytes(), 8));
        }
        assert_eq!(hit.len(), 8, "1024 distinct keys should cover all 8 shards");
    }
}

#[cfg(test)]
mod ring_slot_tests {
    use super::oldest_initialized_ring_slot;

    #[test]
    fn oldest_initialized_ring_slot_is_none_until_second_slot_exists() {
        assert_eq!(oldest_initialized_ring_slot(0, 0, 8), None);
    }

    #[test]
    fn oldest_initialized_ring_slot_tracks_partial_ring_startup() {
        assert_eq!(oldest_initialized_ring_slot(1, 1, 8), Some((0, 0)));
        assert_eq!(oldest_initialized_ring_slot(2, 2, 8), Some((0, 0)));
        assert_eq!(oldest_initialized_ring_slot(3, 3, 8), Some((0, 0)));
    }

    #[test]
    fn oldest_initialized_ring_slot_wraps_after_full_rotation() {
        assert_eq!(oldest_initialized_ring_slot(7, 7, 8), Some((0, 0)));
        assert_eq!(oldest_initialized_ring_slot(0, 8, 8), Some((1, 1)));
        assert_eq!(oldest_initialized_ring_slot(1, 9, 8), Some((2, 2)));
    }
}

#[cfg(test)]
mod ring_ledger_derivation_tests {
    use super::{
        initialized_sealed_ring_slots, oldest_initialized_ring_slot, ring_slot_generation,
    };

    /// Rotation advances slot and generation in lock-step from genesis
    /// (0, 0), so the cursor always satisfies `slot = generation mod
    /// slot_count` and each slot's last-open generation is derivable.
    #[test]
    fn cursor_slot_is_generation_mod_slot_count() {
        for slot_count in [2_i32, 4, 8, 16] {
            for generation in 0..(slot_count as i64 * 3) {
                let slot = (generation % slot_count as i64) as i32;
                assert_eq!(
                    ring_slot_generation(slot, generation, slot_count, slot),
                    Some(generation),
                    "current slot derives to the cursor generation"
                );
            }
        }
    }

    #[test]
    fn slot_generation_derives_older_slots() {
        // Cursor at slot 3, generation 19, slot_count 16: slot 3 opened
        // at 19, slot 2 at 18, ..., slot 0 at 16, slot 15 at 15, etc.
        assert_eq!(ring_slot_generation(3, 19, 16, 3), Some(19));
        assert_eq!(ring_slot_generation(3, 19, 16, 2), Some(18));
        assert_eq!(ring_slot_generation(3, 19, 16, 0), Some(16));
        assert_eq!(ring_slot_generation(3, 19, 16, 15), Some(15));
        assert_eq!(ring_slot_generation(3, 19, 16, 4), Some(4));
    }

    #[test]
    fn slot_generation_is_none_for_never_opened_slots() {
        // Cursor at (2, 2) in an 8-slot ring: slots 3..7 never opened.
        for slot in 3..8 {
            assert_eq!(ring_slot_generation(2, 2, 8, slot), None);
        }
        // Out-of-range slots are never derivable.
        assert_eq!(ring_slot_generation(2, 2, 8, -1), None);
        assert_eq!(ring_slot_generation(2, 2, 8, 8), None);
    }

    #[test]
    fn sealed_slots_match_slot_generation_and_ordering() {
        for slot_count in [2_i32, 4, 8, 16] {
            for generation in 0..(slot_count as i64 * 2 + 3) {
                let current_slot = (generation % slot_count as i64) as i32;
                let sealed = initialized_sealed_ring_slots(current_slot, generation, slot_count);
                // Ascending generations, all below the cursor.
                let mut previous = -1_i64;
                for &(slot, slot_generation) in &sealed {
                    assert!(slot_generation > previous);
                    assert!(slot_generation < generation);
                    assert_ne!(slot, current_slot);
                    assert_eq!(
                        ring_slot_generation(current_slot, generation, slot_count, slot),
                        Some(slot_generation)
                    );
                    previous = slot_generation;
                }
                // The oldest sealed slot agrees with the prune target.
                assert_eq!(
                    sealed.first().copied(),
                    oldest_initialized_ring_slot(current_slot, generation, slot_count)
                );
                // Ring keeps at most slot_count - 1 sealed slots.
                assert!(sealed.len() < slot_count as usize);
            }
        }
    }
}

#[cfg(test)]
mod claim_cursor_advance_tests {
    use super::{ClaimCursorAdvance, QueueStorage};

    fn advance(next_seq: i64, only_if_current: Option<i64>) -> ClaimCursorAdvance {
        ClaimCursorAdvance {
            queue: "queue".to_string(),
            priority: 2,
            enqueue_shard: 0,
            next_seq,
            only_if_current,
        }
    }

    #[test]
    fn normalize_claim_cursor_advances_sorts_conditional_lane_updates() {
        let normalized = QueueStorage::normalize_claim_cursor_advances(&[
            advance(7, Some(6)),
            advance(6, Some(5)),
            advance(8, Some(7)),
        ]);

        let ordered: Vec<(i64, i64)> = normalized
            .iter()
            .map(|advance| (advance.only_if_current.unwrap(), advance.next_seq))
            .collect();
        assert_eq!(ordered, vec![(5, 6), (6, 7), (7, 8)]);
    }

    #[test]
    fn normalize_claim_cursor_advances_coalesces_unconditional_lane_updates() {
        let normalized = QueueStorage::normalize_claim_cursor_advances(&[
            advance(3, None),
            advance(5, None),
            advance(4, Some(3)),
        ]);

        assert_eq!(normalized.len(), 1);
        assert_eq!(normalized[0].next_seq, 5);
        assert_eq!(normalized[0].only_if_current, None);
    }
}

fn default_payload_metadata() -> serde_json::Value {
    serde_json::json!({})
}

fn is_empty_json_object(value: &serde_json::Value) -> bool {
    value.as_object().is_some_and(serde_json::Map::is_empty)
}

fn is_compact_receipt_completion_metadata(value: &serde_json::Value) -> bool {
    let Some(metadata) = value.as_object() else {
        return false;
    };

    metadata
        .keys()
        .all(|key| key == "_awa_original_priority" || key == "_awa_original_queue")
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RuntimePayload {
    #[serde(
        default = "default_payload_metadata",
        skip_serializing_if = "is_empty_json_object"
    )]
    metadata: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    errors: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    progress: Option<serde_json::Value>,
}

impl Default for RuntimePayload {
    fn default() -> Self {
        Self {
            metadata: default_payload_metadata(),
            tags: Vec::new(),
            errors: Vec::new(),
            progress: None,
        }
    }
}

impl RuntimePayload {
    fn from_json(value: serde_json::Value) -> Result<Self, AwaError> {
        if value.is_null() {
            return Ok(Self::default());
        }
        let payload: Self = serde_json::from_value(value)?;
        if !payload.metadata.is_object() {
            return Err(AwaError::Validation(
                "queue storage payload metadata must be a JSON object".to_string(),
            ));
        }
        Ok(payload)
    }

    fn into_json(self) -> serde_json::Value {
        serde_json::to_value(self).expect("runtime payload serializes")
    }

    fn errors_option(&self) -> Option<Vec<serde_json::Value>> {
        (!self.errors.is_empty()).then(|| self.errors.clone())
    }

    fn push_error(&mut self, error: serde_json::Value) {
        self.errors.push(error);
    }

    fn set_progress(&mut self, progress: Option<serde_json::Value>) {
        self.progress = progress;
    }

    fn insert_callback_result(&mut self, payload: Option<serde_json::Value>) {
        let metadata = self
            .metadata
            .as_object_mut()
            .expect("runtime payload metadata object");
        metadata.insert(
            "_awa_callback_result".to_string(),
            payload.unwrap_or(serde_json::Value::Null),
        );
    }
}

#[cfg(test)]
mod runtime_payload_tests {
    use super::{
        is_compact_receipt_completion_metadata, storage_payload, terminal_storage_payload,
        RuntimePayload,
    };

    #[test]
    fn default_runtime_payload_serializes_compactly() {
        assert_eq!(
            RuntimePayload::default().into_json(),
            serde_json::json!({}),
            "default payloads should not write empty metadata/tags/errors/progress"
        );
        assert_eq!(
            storage_payload(&RuntimePayload::default().into_json()),
            None
        );
    }

    #[test]
    fn missing_runtime_payload_fields_round_trip_with_defaults() {
        let payload = RuntimePayload::from_json(serde_json::json!({})).unwrap();

        assert_eq!(payload.metadata, serde_json::json!({}));
        assert!(payload.tags.is_empty());
        assert!(payload.errors.is_empty());
        assert_eq!(payload.progress, None);
        assert_eq!(payload.into_json(), serde_json::json!({}));
    }

    #[test]
    fn null_runtime_payload_round_trips_with_defaults() {
        let payload = RuntimePayload::from_json(serde_json::Value::Null).unwrap();

        assert_eq!(payload.metadata, serde_json::json!({}));
        assert!(payload.tags.is_empty());
        assert!(payload.errors.is_empty());
        assert_eq!(payload.progress, None);
        assert_eq!(storage_payload(&payload.into_json()), None);
    }

    #[test]
    fn compact_receipt_completion_metadata_only_allows_awa_provenance() {
        assert!(is_compact_receipt_completion_metadata(&serde_json::json!(
            {}
        )));
        assert!(is_compact_receipt_completion_metadata(
            &serde_json::json!({ "_awa_original_priority": 4 })
        ));
        assert!(is_compact_receipt_completion_metadata(&serde_json::json!({
            "_awa_original_queue": "default",
            "_awa_original_priority": 4
        })));
        assert!(!is_compact_receipt_completion_metadata(
            &serde_json::json!({ "tenant": "acme" })
        ));
        assert!(!is_compact_receipt_completion_metadata(&serde_json::json!(
            null
        )));
    }

    #[test]
    fn legacy_expanded_runtime_payload_round_trips_to_compact_form() {
        let payload = RuntimePayload::from_json(serde_json::json!({
            "metadata": {},
            "tags": [],
            "errors": [],
            "progress": null
        }))
        .unwrap();

        assert_eq!(payload.metadata, serde_json::json!({}));
        assert!(payload.tags.is_empty());
        assert!(payload.errors.is_empty());
        assert_eq!(payload.progress, None);
        assert_eq!(payload.into_json(), serde_json::json!({}));
    }

    #[test]
    fn non_default_runtime_payload_fields_are_preserved() {
        let payload = RuntimePayload::from_json(serde_json::json!({
            "metadata": { "source": "test" },
            "tags": ["fast"],
            "errors": [{ "message": "boom" }],
            "progress": { "step": 1 }
        }))
        .unwrap();

        assert_eq!(
            payload.into_json(),
            serde_json::json!({
                "metadata": { "source": "test" },
                "tags": ["fast"],
                "errors": [{ "message": "boom" }],
                "progress": { "step": 1 }
            })
        );
    }

    #[test]
    fn unchanged_terminal_payload_elides_storage_copy() {
        let payload = serde_json::json!({
            "metadata": { "source": "test" },
            "tags": ["fast"]
        });

        assert_eq!(terminal_storage_payload(&payload, Some(&payload)), None);

        let changed = serde_json::json!({
            "metadata": { "source": "test" },
            "tags": ["fast"],
            "errors": [{ "message": "boom" }]
        });
        assert_eq!(
            terminal_storage_payload(&changed, Some(&payload)),
            Some(&changed)
        );
    }
}

fn unique_state_claims(unique_states: Option<&str>, state: JobState) -> bool {
    let Some(bitmask) = unique_states else {
        return false;
    };
    let idx = state.bit_position() as usize;
    bitmask.as_bytes().get(idx).is_some_and(|bit| *bit == b'1')
}

fn write_copy_field(buf: &mut Vec<u8>, value: &str) {
    if value.contains(',')
        || value.contains('"')
        || value.contains('\n')
        || value.contains('\r')
        || value.contains('\\')
        || value == COPY_NULL_SENTINEL
    {
        buf.push(b'"');
        for byte in value.bytes() {
            if byte == b'"' {
                buf.push(b'"');
            }
            buf.push(byte);
        }
        buf.push(b'"');
    } else {
        buf.extend_from_slice(value.as_bytes());
    }
}

fn write_copy_json(buf: &mut Vec<u8>, value: &serde_json::Value) {
    let json = serde_json::to_string(value).expect("JSON serialization should not fail");
    write_copy_field(buf, &json);
}

fn storage_payload(value: &serde_json::Value) -> Option<&serde_json::Value> {
    (!is_storage_payload_empty(value)).then_some(value)
}

fn terminal_storage_payload<'a>(
    value: &'a serde_json::Value,
    ready_payload: Option<&serde_json::Value>,
) -> Option<&'a serde_json::Value> {
    if is_storage_payload_empty(value) || ready_payload.is_some_and(|ready| ready == value) {
        None
    } else {
        Some(value)
    }
}

fn is_storage_payload_empty(value: &serde_json::Value) -> bool {
    value.is_null() || is_empty_json_object(value)
}

fn write_copy_storage_payload(buf: &mut Vec<u8>, value: &serde_json::Value) {
    match storage_payload(value) {
        Some(value) => write_copy_json(buf, value),
        None => buf.extend_from_slice(COPY_NULL_SENTINEL.as_bytes()),
    }
}

fn write_copy_datetime(buf: &mut Vec<u8>, value: DateTime<Utc>) {
    write_copy_field(buf, &value.to_rfc3339());
}

fn write_copy_optional_datetime(buf: &mut Vec<u8>, value: Option<DateTime<Utc>>) {
    match value {
        Some(value) => write_copy_datetime(buf, value),
        None => buf.extend_from_slice(COPY_NULL_SENTINEL.as_bytes()),
    }
}

fn write_copy_optional_bytes(buf: &mut Vec<u8>, value: &Option<Vec<u8>>) {
    match value {
        Some(bytes) => {
            let bytea_hex = format!("\\x{}", hex::encode(bytes));
            write_copy_field(buf, &bytea_hex);
        }
        None => buf.extend_from_slice(COPY_NULL_SENTINEL.as_bytes()),
    }
}

fn write_copy_optional_string(buf: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => write_copy_field(buf, value),
        None => buf.extend_from_slice(COPY_NULL_SENTINEL.as_bytes()),
    }
}

fn write_ready_copy_row(
    buf: &mut Vec<u8>,
    ready_slot: i32,
    ready_generation: i64,
    row: &RuntimeReadyInsert,
) {
    buf.extend_from_slice(ready_slot.to_string().as_bytes());
    buf.push(b',');
    buf.extend_from_slice(ready_generation.to_string().as_bytes());
    buf.push(b',');
    buf.extend_from_slice(row.job_id.to_string().as_bytes());
    buf.push(b',');
    write_copy_field(buf, &row.kind);
    buf.push(b',');
    write_copy_field(buf, &row.queue);
    buf.push(b',');
    write_copy_json(buf, &row.args);
    buf.push(b',');
    buf.extend_from_slice(row.priority.to_string().as_bytes());
    buf.push(b',');
    buf.extend_from_slice(row.attempt.to_string().as_bytes());
    buf.push(b',');
    buf.extend_from_slice(row.run_lease.to_string().as_bytes());
    buf.push(b',');
    buf.extend_from_slice(row.max_attempts.to_string().as_bytes());
    buf.push(b',');
    buf.extend_from_slice(row.lane_seq.to_string().as_bytes());
    buf.push(b',');
    buf.extend_from_slice(row.enqueue_shard.to_string().as_bytes());
    buf.push(b',');
    write_copy_datetime(buf, row.run_at);
    buf.push(b',');
    write_copy_optional_datetime(buf, row.attempted_at);
    buf.push(b',');
    write_copy_datetime(buf, row.created_at);
    buf.push(b',');
    write_copy_optional_bytes(buf, &row.unique_key);
    buf.push(b',');
    write_copy_optional_string(buf, row.unique_states.as_deref());
    buf.push(b',');
    write_copy_storage_payload(buf, &row.payload);
    buf.push(b'\n');
}

fn write_deferred_copy_row(buf: &mut Vec<u8>, row: &DeferredJobRow) {
    buf.extend_from_slice(row.job_id.to_string().as_bytes());
    buf.push(b',');
    write_copy_field(buf, &row.kind);
    buf.push(b',');
    write_copy_field(buf, &row.queue);
    buf.push(b',');
    write_copy_json(buf, &row.args);
    buf.push(b',');
    write_copy_field(buf, &row.state.to_string());
    buf.push(b',');
    buf.extend_from_slice(row.priority.to_string().as_bytes());
    buf.push(b',');
    buf.extend_from_slice(row.attempt.to_string().as_bytes());
    buf.push(b',');
    buf.extend_from_slice(row.run_lease.to_string().as_bytes());
    buf.push(b',');
    buf.extend_from_slice(row.max_attempts.to_string().as_bytes());
    buf.push(b',');
    write_copy_datetime(buf, row.run_at);
    buf.push(b',');
    write_copy_optional_datetime(buf, row.attempted_at);
    buf.push(b',');
    write_copy_optional_datetime(buf, row.finalized_at);
    buf.push(b',');
    write_copy_datetime(buf, row.created_at);
    buf.push(b',');
    write_copy_optional_bytes(buf, &row.unique_key);
    buf.push(b',');
    write_copy_optional_string(buf, row.unique_states.as_deref());
    buf.push(b',');
    write_copy_storage_payload(buf, &row.payload);
    buf.push(b'\n');
}

fn lifecycle_error(error: impl Into<String>, attempt: i16, terminal: bool) -> serde_json::Value {
    let mut value = serde_json::json!({
        "error": error.into(),
        "attempt": attempt,
        "at": Utc::now().to_rfc3339(),
    });
    if terminal {
        value["terminal"] = serde_json::Value::Bool(true);
    }
    value
}

fn transition_timestamp(job: &JobRow) -> DateTime<Utc> {
    job.finalized_at
        .or(job.heartbeat_at)
        .or(job.deadline_at)
        .or(job.attempted_at)
        .unwrap_or(job.run_at)
}

fn state_rank(state: JobState) -> u8 {
    match state {
        JobState::Running | JobState::WaitingExternal => 4,
        JobState::Retryable | JobState::Scheduled => 3,
        JobState::Available => 2,
        JobState::Completed | JobState::Failed | JobState::Cancelled => 1,
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ReadyJobRow {
    job_id: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    run_at: DateTime<Utc>,
    attempted_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    payload: serde_json::Value,
}

impl ReadyJobRow {
    fn into_job_row(self) -> Result<JobRow, AwaError> {
        let payload = RuntimePayload::from_json(self.payload)?;
        Ok(JobRow {
            id: self.job_id,
            kind: self.kind,
            queue: self.queue,
            args: self.args,
            state: JobState::Available,
            priority: self.priority,
            attempt: self.attempt,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            run_at: self.run_at,
            heartbeat_at: None,
            deadline_at: None,
            attempted_at: self.attempted_at,
            finalized_at: None,
            created_at: self.created_at,
            errors: payload.errors_option(),
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
        })
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ReadyTransitionRow {
    ready_slot: i32,
    ready_generation: i64,
    job_id: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    lane_seq: i64,
    enqueue_shard: i16,
    run_at: DateTime<Utc>,
    attempted_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
}

#[derive(Debug, Clone)]
struct ClaimCursorAdvance {
    queue: String,
    priority: i16,
    enqueue_shard: i16,
    next_seq: i64,
    only_if_current: Option<i64>,
}

type ClaimCursorLaneKey = (String, i16, i16);
type ConditionalClaimCursorAdvances = BTreeMap<i64, i64>;
type GroupedClaimCursorAdvances =
    BTreeMap<ClaimCursorLaneKey, (Option<i64>, ConditionalClaimCursorAdvances)>;
type TerminalCounterKey = (i32, i64, String, i16, i16, i16);

struct CancelJobTxResult {
    row: JobRow,
    claim_cursor_advance: Option<ClaimCursorAdvance>,
}

struct ReadyBatchMoveResult {
    moved: bool,
}

impl ReadyTransitionRow {
    fn into_existing_ready_row(
        self,
        queue: String,
        priority: i16,
        payload: serde_json::Value,
    ) -> ExistingReadyRow {
        ExistingReadyRow {
            job_id: self.job_id,
            kind: self.kind,
            queue,
            args: self.args,
            priority,
            attempt: self.attempt,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            run_at: self.run_at,
            attempted_at: self.attempted_at,
            created_at: self.created_at,
            unique_key: self.unique_key,
            unique_states: self.unique_states,
            payload,
        }
    }

    fn into_done_row(
        self,
        state: JobState,
        finalized_at: DateTime<Utc>,
        payload: serde_json::Value,
    ) -> DoneJobRow {
        DoneJobRow {
            ready_slot: self.ready_slot,
            ready_generation: self.ready_generation,
            job_id: self.job_id,
            kind: self.kind,
            queue: self.queue,
            args: self.args,
            state,
            priority: self.priority,
            attempt: self.attempt,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            lane_seq: self.lane_seq,
            enqueue_shard: self.enqueue_shard,
            run_at: self.run_at,
            attempted_at: self.attempted_at,
            finalized_at,
            created_at: self.created_at,
            unique_key: self.unique_key,
            unique_states: self.unique_states,
            payload,
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ReadyJobLeaseRow {
    ready_slot: i32,
    ready_generation: i64,
    lane_seq: i64,
    enqueue_shard: i16,
    lease_slot: i32,
    lease_generation: i64,
    claim_slot: i32,
    receipt_id: Option<i64>,
    claim_batch_id: Option<i64>,
    claim_batch_index: Option<i32>,
    job_id: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    lane_priority: i16,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    run_at: DateTime<Utc>,
    heartbeat_at: Option<DateTime<Utc>>,
    deadline_at: Option<DateTime<Utc>>,
    attempted_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
}

impl ReadyJobLeaseRow {
    fn claim_ref(&self, lease_claim_receipt: bool) -> ClaimedEntry {
        ClaimedEntry {
            queue: self.queue.clone(),
            priority: self.lane_priority,
            lane_seq: self.lane_seq,
            ready_slot: self.ready_slot,
            ready_generation: self.ready_generation,
            lease_slot: self.lease_slot,
            lease_generation: self.lease_generation,
            claim_slot: self.claim_slot,
            receipt_id: self.receipt_id,
            claim_batch_id: self.claim_batch_id,
            claim_batch_index: self.claim_batch_index,
            lease_claim_receipt,
            enqueue_shard: self.enqueue_shard,
        }
    }

    fn into_job_row(self) -> Result<JobRow, AwaError> {
        let mut payload = RuntimePayload::from_json(self.payload)?;
        if self.priority < self.lane_priority {
            let metadata = payload.metadata.as_object_mut().ok_or_else(|| {
                AwaError::Validation(
                    "queue storage payload metadata must be a JSON object".to_string(),
                )
            })?;
            metadata
                .entry("_awa_original_priority".to_string())
                .or_insert_with(|| serde_json::Value::from(i64::from(self.lane_priority)));
        }

        Ok(JobRow {
            id: self.job_id,
            kind: self.kind,
            queue: self.queue,
            args: self.args,
            state: JobState::Running,
            priority: self.priority,
            attempt: self.attempt,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            run_at: self.run_at,
            heartbeat_at: self.heartbeat_at,
            deadline_at: self.deadline_at,
            attempted_at: self.attempted_at,
            finalized_at: None,
            created_at: self.created_at,
            errors: payload.errors_option(),
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
        })
    }

    fn into_claimed_runtime_job(
        self,
        lease_claim_receipt: bool,
    ) -> Result<ClaimedRuntimeJob, AwaError> {
        let claim = self.claim_ref(lease_claim_receipt);
        let unique_states = self.unique_states.clone();
        let job = self.into_job_row()?;
        Ok(ClaimedRuntimeJob {
            claim,
            job,
            unique_states,
        })
    }
}

#[derive(Debug, Clone)]
struct RuntimeReadyRow {
    kind: String,
    queue: String,
    args: serde_json::Value,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    run_at: DateTime<Utc>,
    attempted_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
    /// Optional caller-supplied key. When the destination queue has
    /// `enqueue_shards > 1`, all rows sharing the same key route to
    /// the same shard so FIFO within the key is preserved. `None`
    /// falls back to the per-store rotor.
    ordering_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct RuntimeReadyInsert {
    job_id: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    run_at: DateTime<Utc>,
    attempted_at: Option<DateTime<Utc>>,
    lane_seq: i64,
    /// Enqueue shard the row belongs to. Selected per row by
    /// `shard_for_enqueue`; defaults to 0 when the queue's
    /// `enqueue_shards` is 1 (the default).
    enqueue_shard: i16,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
}

#[derive(Debug)]
struct ReadySegmentInsert {
    queue: String,
    priority: i16,
    enqueue_shard: i16,
    first_lane_seq: i64,
    next_lane_seq: i64,
    /// `run_at` for every row in this segment. Segment construction splits on
    /// `run_at` so claim-time priority aging stays exact after the claim cursor
    /// advances inside a multi-row segment.
    first_run_at: DateTime<Utc>,
}

#[cfg(test)]
mod ready_segment_tests {
    use super::{QueueStorage, RuntimeReadyInsert};
    use chrono::{Duration, TimeZone, Utc};

    fn ready_row(lane_seq: i64, run_at: chrono::DateTime<Utc>) -> RuntimeReadyInsert {
        RuntimeReadyInsert {
            job_id: lane_seq,
            kind: "segment_test".to_string(),
            queue: "segment-q".to_string(),
            args: serde_json::json!({}),
            priority: 2,
            attempt: 0,
            run_lease: 0,
            max_attempts: 25,
            run_at,
            attempted_at: None,
            lane_seq,
            enqueue_shard: 0,
            created_at: run_at,
            unique_key: None,
            unique_states: None,
            payload: serde_json::json!({}),
        }
    }

    #[test]
    fn ready_segments_split_on_run_at_boundaries() {
        let first = Utc
            .with_ymd_and_hms(2026, 6, 14, 12, 0, 0)
            .single()
            .expect("valid test timestamp");
        let second = first + Duration::seconds(1);
        let rows = vec![
            ready_row(1, first),
            ready_row(2, first),
            ready_row(3, second),
            ready_row(4, first),
        ];

        let segments = QueueStorage::ready_segments_from_rows(&rows);
        let ranges: Vec<_> = segments
            .iter()
            .map(|segment| {
                (
                    segment.first_lane_seq,
                    segment.next_lane_seq,
                    segment.first_run_at,
                )
            })
            .collect();

        assert_eq!(ranges, vec![(1, 3, first), (3, 4, second), (4, 5, first)]);
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct DoneJobRow {
    ready_slot: i32,
    ready_generation: i64,
    job_id: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    state: JobState,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    lane_seq: i64,
    /// Enqueue shard the row was claimed from. Part of the
    /// `done_entries` primary key so two shards' terminal rows at
    /// the same `(ready_slot, queue, priority, lane_seq)` do not
    /// collide.
    enqueue_shard: i16,
    run_at: DateTime<Utc>,
    attempted_at: Option<DateTime<Utc>>,
    finalized_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
}

impl DoneJobRow {
    fn into_job_row(self) -> Result<JobRow, AwaError> {
        let payload = RuntimePayload::from_json(self.payload)?;
        Ok(JobRow {
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
            errors: payload.errors_option(),
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
        })
    }

    fn into_dlq_row(self, dlq_reason: String, dlq_at: DateTime<Utc>) -> DlqJobRow {
        DlqJobRow {
            job_id: self.job_id,
            kind: self.kind,
            queue: self.queue,
            args: self.args,
            state: self.state,
            priority: self.priority,
            attempt: self.attempt,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            run_at: self.run_at,
            attempted_at: self.attempted_at,
            finalized_at: self.finalized_at,
            created_at: self.created_at,
            unique_key: self.unique_key,
            unique_states: self.unique_states,
            payload: self.payload,
            dlq_reason,
            dlq_at,
            original_run_lease: self.run_lease,
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct DlqJobRow {
    job_id: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    state: JobState,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    run_at: DateTime<Utc>,
    attempted_at: Option<DateTime<Utc>>,
    finalized_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
    dlq_reason: String,
    dlq_at: DateTime<Utc>,
    original_run_lease: i64,
}

impl DlqJobRow {
    fn into_job_row(self) -> Result<JobRow, AwaError> {
        let payload = RuntimePayload::from_json(self.payload)?;
        Ok(JobRow {
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
            errors: payload.errors_option(),
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
        })
    }

    fn into_retry_ready_row(
        self,
        queue: String,
        priority: i16,
        run_at: DateTime<Utc>,
        payload: serde_json::Value,
    ) -> ExistingReadyRow {
        ExistingReadyRow {
            job_id: self.job_id,
            kind: self.kind,
            queue,
            args: self.args,
            priority,
            attempt: 0,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            run_at,
            attempted_at: None,
            created_at: self.created_at,
            unique_key: self.unique_key,
            unique_states: self.unique_states,
            payload,
        }
    }

    fn into_retry_deferred_row(
        self,
        queue: String,
        priority: i16,
        run_at: DateTime<Utc>,
        payload: serde_json::Value,
    ) -> DeferredJobRow {
        DeferredJobRow {
            job_id: self.job_id,
            kind: self.kind,
            queue,
            args: self.args,
            state: JobState::Scheduled,
            priority,
            attempt: 0,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            run_at,
            attempted_at: None,
            finalized_at: None,
            created_at: self.created_at,
            unique_key: self.unique_key,
            unique_states: self.unique_states,
            payload,
        }
    }
}

#[derive(Debug, Clone)]
struct ExistingReadyRow {
    job_id: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    run_at: DateTime<Utc>,
    attempted_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct DeletedLeaseRow {
    ready_slot: i32,
    ready_generation: i64,
    job_id: i64,
    queue: String,
    state: JobState,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    lane_seq: i64,
    enqueue_shard: i16,
    attempted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ReadySnapshotRow {
    ready_slot: i32,
    ready_generation: i64,
    job_id: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    lane_seq: i64,
    enqueue_shard: i16,
    run_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct AttemptStateRow {
    job_id: i64,
    run_lease: i64,
    progress: Option<serde_json::Value>,
    callback_filter: Option<String>,
    callback_on_complete: Option<String>,
    callback_on_fail: Option<String>,
    callback_transform: Option<String>,
    callback_result: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
struct LeaseTransitionRow {
    ready_slot: i32,
    ready_generation: i64,
    job_id: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    state: JobState,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    lane_seq: i64,
    enqueue_shard: i16,
    run_at: DateTime<Utc>,
    attempted_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
    progress: Option<serde_json::Value>,
}

impl LeaseTransitionRow {
    fn into_done_row(
        self,
        state: JobState,
        finalized_at: DateTime<Utc>,
        payload: serde_json::Value,
    ) -> DoneJobRow {
        DoneJobRow {
            ready_slot: self.ready_slot,
            ready_generation: self.ready_generation,
            job_id: self.job_id,
            kind: self.kind,
            queue: self.queue,
            args: self.args,
            state,
            priority: self.priority,
            attempt: self.attempt,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            lane_seq: self.lane_seq,
            enqueue_shard: self.enqueue_shard,
            run_at: self.run_at,
            attempted_at: self.attempted_at,
            finalized_at,
            created_at: self.created_at,
            unique_key: self.unique_key,
            unique_states: self.unique_states,
            payload,
        }
    }

    fn into_deferred_row(
        self,
        state: JobState,
        run_at: DateTime<Utc>,
        finalized_at: Option<DateTime<Utc>>,
        payload: serde_json::Value,
    ) -> DeferredJobRow {
        DeferredJobRow {
            job_id: self.job_id,
            kind: self.kind,
            queue: self.queue,
            args: self.args,
            state,
            priority: self.priority,
            attempt: self.attempt,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            run_at,
            attempted_at: self.attempted_at,
            finalized_at,
            created_at: self.created_at,
            unique_key: self.unique_key,
            unique_states: self.unique_states,
            payload,
        }
    }

    fn into_ready_row(self, run_at: DateTime<Utc>, payload: serde_json::Value) -> ExistingReadyRow {
        ExistingReadyRow {
            job_id: self.job_id,
            kind: self.kind,
            queue: self.queue,
            args: self.args,
            priority: self.priority,
            attempt: self.attempt,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            run_at,
            attempted_at: self.attempted_at,
            created_at: self.created_at,
            unique_key: self.unique_key,
            unique_states: self.unique_states,
            payload,
        }
    }

    fn into_dlq_row(
        self,
        finalized_at: DateTime<Utc>,
        payload: serde_json::Value,
        dlq_reason: String,
        dlq_at: DateTime<Utc>,
    ) -> DlqJobRow {
        DlqJobRow {
            job_id: self.job_id,
            kind: self.kind,
            queue: self.queue,
            args: self.args,
            state: JobState::Failed,
            priority: self.priority,
            attempt: self.attempt,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            run_at: self.run_at,
            attempted_at: self.attempted_at,
            finalized_at,
            created_at: self.created_at,
            unique_key: self.unique_key,
            unique_states: self.unique_states,
            payload,
            dlq_reason,
            dlq_at,
            original_run_lease: self.run_lease,
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct LeaseJobRow {
    job_id: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    state: JobState,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    run_at: DateTime<Utc>,
    heartbeat_at: Option<DateTime<Utc>>,
    deadline_at: Option<DateTime<Utc>>,
    attempted_at: Option<DateTime<Utc>>,
    finalized_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    callback_id: Option<Uuid>,
    callback_timeout_at: Option<DateTime<Utc>>,
    callback_filter: Option<String>,
    callback_on_complete: Option<String>,
    callback_on_fail: Option<String>,
    callback_transform: Option<String>,
    payload: serde_json::Value,
    progress: Option<serde_json::Value>,
    callback_result: Option<serde_json::Value>,
}

impl LeaseJobRow {
    fn into_job_row(self) -> Result<JobRow, AwaError> {
        let payload = QueueStorage::materialize_runtime_payload(
            self.payload,
            self.progress,
            self.callback_result,
        )?;
        Ok(JobRow {
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
            heartbeat_at: self.heartbeat_at,
            deadline_at: self.deadline_at,
            attempted_at: self.attempted_at,
            finalized_at: self.finalized_at,
            created_at: self.created_at,
            errors: payload.errors_option(),
            metadata: payload.metadata,
            tags: payload.tags,
            unique_key: self.unique_key,
            unique_states: None,
            callback_id: self.callback_id,
            callback_timeout_at: self.callback_timeout_at,
            callback_filter: self.callback_filter,
            callback_on_complete: self.callback_on_complete,
            callback_on_fail: self.callback_on_fail,
            callback_transform: self.callback_transform,
            progress: payload.progress,
        })
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct DeferredJobRow {
    job_id: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    state: JobState,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    max_attempts: i16,
    run_at: DateTime<Utc>,
    attempted_at: Option<DateTime<Utc>>,
    finalized_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
}

impl DeferredJobRow {
    fn into_job_row(self) -> Result<JobRow, AwaError> {
        let payload = RuntimePayload::from_json(self.payload)?;
        Ok(JobRow {
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
            finalized_at: self.finalized_at,
            created_at: self.created_at,
            errors: payload.errors_option(),
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
        })
    }
}

/// Segmented queue storage backend.
///
/// Design goals:
/// - append-only queue segments in a rotated ring
/// - append-only completion segments keyed back to the queue segment
/// - a separate, faster rotating lease ring so delete churn is bounded by the
///   lease cycle rather than by queue retention
/// - hot mutable state restricted to queue cursors, narrow leases, and
///   optional per-attempt runtime state only when needed
///
#[derive(Debug)]
pub struct QueueStorage {
    config: QueueStorageConfig,
    next_stripe_probe: AtomicUsize,
    /// Per-store rotor that selects the enqueue shard for the next batch.
    /// Rotated once per `shard_for_enqueue` call so producers spread their
    /// writes across the per-`(queue, priority)` shard rows.
    shard_rotor: AtomicU16,
    /// Cache of `awa.queue_meta.enqueue_shards` per queue. Populated lazily
    /// on the first `shard_for_enqueue` call for a queue and invalidated by
    /// `reset()`. With the default `enqueue_shards = 1`, the cache holds 1
    /// and `pick_shard` returns 0 unconditionally.
    enqueue_shards_cache: Mutex<HashMap<String, i16>>,
    /// Lane-presence cache: `(physical_queue, priority, enqueue_shard)`
    /// triples whose three lane rows (queue_lanes, queue_enqueue_heads,
    /// queue_claim_heads) we have previously inserted. Skips the three
    /// `INSERT … ON CONFLICT DO NOTHING` round-trips on subsequent enqueue
    /// batches to a known lane/shard. Cleared on `reset()` because reset
    /// TRUNCATEs queue_lanes; `advance_enqueue_head` repairs a stale entry
    /// (head row gone after a rolled-back ensure_lane).
    ensured_lanes: Mutex<HashSet<(String, i16, i16)>>,
    prune_lock_timeout: Duration,
}

impl QueueStorage {
    pub fn new(config: QueueStorageConfig) -> Result<Self, AwaError> {
        if config.queue_slot_count < 4 {
            return Err(AwaError::Validation(
                "queue storage requires at least 4 queue slots".into(),
            ));
        }
        if config.lease_slot_count < 2 {
            return Err(AwaError::Validation(
                "queue storage requires at least 2 lease slots".into(),
            ));
        }
        if config.claim_slot_count < 2 {
            return Err(AwaError::Validation(
                "queue storage requires at least 2 claim slots".into(),
            ));
        }
        if config.queue_stripe_count == 0 {
            return Err(AwaError::Validation(
                "queue storage requires at least 1 queue stripe".into(),
            ));
        }
        if config.schema == DEFAULT_SCHEMA
            && (config.queue_slot_count != DEFAULT_QUEUE_SLOT_COUNT
                || config.lease_slot_count != DEFAULT_LEASE_SLOT_COUNT
                || config.claim_slot_count != DEFAULT_CLAIM_SLOT_COUNT
                || !config.lease_claim_receipts)
        {
            return Err(AwaError::Validation(
                "the default `awa` queue-storage schema must use the default slot counts and \
                 lease_claim_receipts=true"
                    .into(),
            ));
        }
        validate_ident(&config.schema)?;
        Ok(Self {
            config,
            next_stripe_probe: AtomicUsize::new(0),
            shard_rotor: AtomicU16::new(0),
            enqueue_shards_cache: Mutex::new(HashMap::new()),
            ensured_lanes: Mutex::new(HashSet::new()),
            prune_lock_timeout: DEFAULT_PRUNE_LOCK_TIMEOUT,
        })
    }

    #[doc(hidden)]
    pub fn with_prune_lock_timeout(mut self, timeout: Duration) -> Result<Self, AwaError> {
        if timeout.is_zero() {
            return Err(AwaError::Validation(
                "queue storage prune lock timeout must be greater than zero".into(),
            ));
        }
        self.prune_lock_timeout = timeout;
        Ok(self)
    }

    pub fn from_existing_schema(schema: impl Into<String>) -> Result<Self, AwaError> {
        Self::new(QueueStorageConfig {
            schema: schema.into(),
            ..Default::default()
        })
    }

    pub fn schema(&self) -> &str {
        &self.config.schema
    }

    pub fn slot_count(&self) -> usize {
        self.queue_slot_count()
    }

    pub fn queue_slot_count(&self) -> usize {
        self.config.queue_slot_count
    }

    pub fn lease_slot_count(&self) -> usize {
        self.config.lease_slot_count
    }

    pub fn claim_slot_count(&self) -> usize {
        self.config.claim_slot_count
    }

    pub fn queue_stripe_count(&self) -> usize {
        self.config.queue_stripe_count
    }

    pub fn lease_claim_receipts(&self) -> bool {
        self.config.lease_claim_receipts
    }

    fn uses_queue_striping(&self) -> bool {
        self.queue_stripe_count() > 1
    }

    fn is_physical_stripe_queue(&self, queue: &str) -> bool {
        self.uses_queue_striping()
            && queue
                .rsplit_once(QUEUE_STRIPE_DELIMITER)
                .is_some_and(|(_, suffix)| suffix.parse::<usize>().is_ok())
    }

    fn physical_queue_for_stripe(&self, queue: &str, stripe: usize) -> String {
        format!("{queue}{QUEUE_STRIPE_DELIMITER}{stripe}")
    }

    fn physical_queues_for_logical(&self, queue: &str) -> Vec<String> {
        if !self.uses_queue_striping() || self.is_physical_stripe_queue(queue) {
            return vec![queue.to_string()];
        }
        (0..self.queue_stripe_count())
            .map(|stripe| self.physical_queue_for_stripe(queue, stripe))
            .collect()
    }

    fn stripe_probe_start(&self, stripe_count: usize) -> usize {
        if stripe_count <= 1 {
            return 0;
        }
        self.next_stripe_probe.fetch_add(1, Ordering::Relaxed) % stripe_count
    }

    fn logical_queue_name<'a>(&self, queue: &'a str) -> &'a str {
        if !self.uses_queue_striping() {
            return queue;
        }
        queue
            .rsplit_once(QUEUE_STRIPE_DELIMITER)
            .and_then(|(prefix, suffix)| suffix.parse::<usize>().ok().map(|_| prefix))
            .unwrap_or(queue)
    }

    fn queue_stripe_for_enqueue(
        &self,
        queue: &str,
        unique_key: &Option<Vec<u8>>,
        salt: i64,
    ) -> String {
        if !self.uses_queue_striping() || self.is_physical_stripe_queue(queue) {
            return queue.to_string();
        }

        let stripe = if let Some(key) = unique_key {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            (hasher.finish() as usize) % self.queue_stripe_count()
        } else {
            salt.rem_euclid(self.queue_stripe_count() as i64) as usize
        };
        self.physical_queue_for_stripe(queue, stripe)
    }

    fn use_lease_claim_receipts_for_runtime(&self, _deadline_duration: Duration) -> bool {
        // Receipts mode supports per-claim deadlines via
        // `lease_claim_batches.deadline_at` (#246; rescued by
        // `rescue_expired_receipt_deadlines_tx`), so receipts is the
        // live path whenever the engine is configured for receipts —
        // the queue's `deadline_duration` no longer disqualifies it.
        self.lease_claim_receipts()
    }

    pub fn ready_child_relname(&self, slot: usize) -> String {
        format!("ready_entries_{slot}")
    }

    pub fn done_child_relname(&self, slot: usize) -> String {
        format!("done_entries_{slot}")
    }

    pub fn leases_relname(&self) -> &'static str {
        "leases"
    }

    pub fn lease_claims_relname(&self) -> &'static str {
        "lease_claims"
    }

    pub fn lease_claim_closures_relname(&self) -> &'static str {
        "lease_claim_closures"
    }

    pub fn leases_child_relname(&self, slot: usize) -> String {
        format!("leases_{slot}")
    }

    pub fn attempt_state_relname(&self) -> &'static str {
        "attempt_state"
    }

    pub async fn active_schema(pool: &PgPool) -> Result<Option<String>, AwaError> {
        sqlx::query_scalar(
            "SELECT schema_name FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'",
        )
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)
    }

    /// Transaction-aware variant of [`Self::active_schema`] — read the
    /// active queue-storage schema name inside the caller's transaction
    /// rather than acquiring a separate pool connection.
    pub async fn active_schema_in_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<Option<String>, AwaError> {
        sqlx::query_scalar(
            "SELECT schema_name FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'",
        )
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)
    }

    fn materialize_runtime_payload(
        payload: serde_json::Value,
        progress: Option<serde_json::Value>,
        callback_result: Option<serde_json::Value>,
    ) -> Result<RuntimePayload, AwaError> {
        let mut payload = RuntimePayload::from_json(payload)?;
        if let Some(progress) = progress {
            payload.set_progress(Some(progress));
        }
        if let Some(callback_result) = callback_result {
            payload.insert_callback_result(Some(callback_result));
        }
        Ok(payload)
    }

    fn payload_with_attempt_state(
        payload: serde_json::Value,
        progress: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, AwaError> {
        let mut payload = RuntimePayload::from_json(payload)?;
        if let Some(progress) = progress {
            payload.set_progress(Some(progress));
        }
        Ok(payload.into_json())
    }

    fn payload_from_parts(
        metadata: serde_json::Value,
        tags: Vec<String>,
        errors: Option<Vec<serde_json::Value>>,
        progress: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, AwaError> {
        Ok(RuntimePayload {
            metadata,
            tags,
            errors: errors.unwrap_or_default(),
            progress,
        }
        .into_json())
    }

    async fn sync_unique_claim<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
        unique_key: &Option<Vec<u8>>,
        unique_states: Option<&str>,
        old_state: Option<JobState>,
        new_state: Option<JobState>,
    ) -> Result<(), AwaError> {
        let old_claim = old_state.is_some_and(|state| unique_state_claims(unique_states, state));
        let new_claim = new_state.is_some_and(|state| unique_state_claims(unique_states, state));

        if old_claim && !new_claim {
            if let Some(key) = unique_key {
                sqlx::query(
                    "DELETE FROM awa.job_unique_claims WHERE unique_key = $1 AND job_id = $2",
                )
                .bind(key)
                .bind(job_id)
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            }
        }

        if new_claim && !old_claim {
            if let Some(key) = unique_key {
                let result = sqlx::query(
                    r#"
                    INSERT INTO awa.job_unique_claims (unique_key, job_id)
                    VALUES ($1, $2)
                    ON CONFLICT (unique_key)
                    DO UPDATE SET job_id = EXCLUDED.job_id
                    WHERE awa.job_unique_claims.job_id = EXCLUDED.job_id
                    "#,
                )
                .bind(key)
                .bind(job_id)
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                if result.rows_affected() == 0 {
                    return Err(AwaError::UniqueConflict {
                        constraint: Some("idx_awa_jobs_unique".to_string()),
                    });
                }
            }
        }

        Ok(())
    }

    async fn sync_enqueue_unique_claims<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        claims: Vec<(Vec<u8>, i64)>,
    ) -> Result<(), AwaError> {
        if claims.is_empty() {
            return Ok(());
        }

        let mut seen: HashSet<&[u8]> = HashSet::with_capacity(claims.len());
        for (key, _) in &claims {
            if !seen.insert(key.as_slice()) {
                return Err(AwaError::UniqueConflict {
                    constraint: Some("idx_awa_jobs_unique".to_string()),
                });
            }
        }

        let (keys, job_ids): (Vec<Vec<u8>>, Vec<i64>) = claims.into_iter().unzip();
        let (requested, applied): (i64, i64) = sqlx::query_as(
            r#"
            WITH input(unique_key, job_id) AS (
                SELECT * FROM unnest($1::bytea[], $2::bigint[])
            ),
            inserted AS (
                INSERT INTO awa.job_unique_claims (unique_key, job_id)
                SELECT unique_key, job_id FROM input
                ON CONFLICT (unique_key)
                DO UPDATE SET job_id = EXCLUDED.job_id
                WHERE awa.job_unique_claims.job_id = EXCLUDED.job_id
                RETURNING unique_key
            )
            SELECT
                (SELECT count(*)::bigint FROM input) AS requested,
                (SELECT count(*)::bigint FROM inserted) AS applied
            "#,
        )
        .bind(keys)
        .bind(job_ids)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if applied != requested {
            return Err(AwaError::UniqueConflict {
                constraint: Some("idx_awa_jobs_unique".to_string()),
            });
        }

        Ok(())
    }

    // Enqueue inserts have no prior storage state, so uniqueness only needs to
    // add claims for states included in the row's unique-state bitmask. State
    // transitions still use `sync_unique_claim`, which can release old claims.
    async fn sync_ready_enqueue_unique_claims<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[RuntimeReadyInsert],
    ) -> Result<(), AwaError> {
        let claims = rows
            .iter()
            .filter(|row| unique_state_claims(row.unique_states.as_deref(), JobState::Available))
            .filter_map(|row| row.unique_key.as_ref().map(|key| (key.clone(), row.job_id)))
            .collect();
        self.sync_enqueue_unique_claims(tx, claims).await
    }

    async fn sync_deferred_enqueue_unique_claims<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[DeferredJobRow],
    ) -> Result<(), AwaError> {
        let claims = rows
            .iter()
            .filter(|row| unique_state_claims(row.unique_states.as_deref(), row.state))
            .filter_map(|row| row.unique_key.as_ref().map(|key| (key.clone(), row.job_id)))
            .collect();
        self.sync_enqueue_unique_claims(tx, claims).await
    }

    /// Idempotently install the queue-storage substrate (schema, tables,
    /// indexes, functions) for this configuration.
    ///
    /// Runs entirely inside one transaction on a single pooled connection,
    /// guarded by `pg_advisory_xact_lock` so concurrent worker startups
    /// serialize cleanly rather than fighting over pool slots. The lock
    /// auto-releases on COMMIT/ROLLBACK.
    ///
    /// **Note on index builds:** the `CREATE INDEX IF NOT EXISTS` calls
    /// below are not `CONCURRENTLY` (Postgres bans `CONCURRENTLY` inside a
    /// transaction). On a fresh install that's a no-op cost. On a redeploy
    /// against a database that already has rows in the queue tables, each
    /// fresh-index build takes `ShareUpdateExclusiveLock` on its table for
    /// the duration of the build, and concurrent writers to that table
    /// queue up. For typical operator scenarios this is bounded by the
    /// `IF NOT EXISTS` guard — only the *new* indexes added by a runtime
    /// upgrade get built; the rest are no-ops.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.prepare_schema")]
    pub async fn prepare_schema(&self, pool: &PgPool) -> Result<(), AwaError> {
        let schema = self.schema();
        let install_lock_name = format!("awa.queue_storage.install:{schema}");
        let mut install_tx = pool.begin().await.map_err(map_sqlx_error)?;

        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(&install_lock_name)
            .execute(install_tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        let install_result = async {
            // Helper-owned DDL. The helper takes the same per-schema advisory
            // xact lock we just acquired (re-entrant on the same session) and
            // performs the entire forward-only substrate install: sequences,
            // ring-state singletons, partitioned ready/done/lease tables,
            // lane indexes, claim_ready_runtime() function, and seed rows.
            // Validation inside the helper rejects non-default configuration
            // against the default `awa` schema (see migration v023 and #308).
            //
            // What stays here (constraint 7 of #308 PR 1):
            //   * `open_receipt_claims` non-empty rejection + drop (ADR-023).
            //   * Legacy rename of `lease_claims` / `lease_claim_closures`
            //     before the helper creates the partitioned parents, plus the
            //     post-helper copy + drop of the renamed-aside rows.
            //   * `queue_count_snapshots` legacy drop.
            //
            // `awa.runtime_storage_backends` is owned by migrations (v012);
            // activation paths only seed/update its row. The helper does NOT
            // touch it and does NOT change storage-transition state, so a call
            // to `prepare_schema` remains activation-neutral.

            sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
                .execute(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

            // The hot path reads "currently open" by anti-joining the
            // partitioned `lease_claims` / `lease_claim_closures` pair, so
            // `open_receipt_claims` is unused (see ADR-023). Drop it on every
            // install. Refuse to drop a non-empty table — non-empty here
            // means an operator rolled forward from an older build that
            // still wrote rows we don't want to silently delete.
            let open_receipt_claims_exists: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = $1 AND c.relname = 'open_receipt_claims'
                )
                "#,
            )
            .bind(schema)
            .fetch_one(install_tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
            if open_receipt_claims_exists {
                let row_count: i64 = sqlx::query_scalar(&format!(
                    "SELECT count(*)::bigint FROM {schema}.open_receipt_claims"
                ))
                .fetch_one(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
                if row_count > 0 {
                    return Err(AwaError::Validation(format!(
                        "{schema}.open_receipt_claims has {row_count} rows but the runtime no \
                         longer reads or writes this table. Run the ADR-023 reverse migration \
                         (recreate from lease_claims minus durable closure evidence) to drain it, \
                         then re-run prepare_schema."
                    )));
                }
                sqlx::query(&format!(
                    "DROP TABLE IF EXISTS {schema}.open_receipt_claims CASCADE"
                ))
                .execute(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            }

            // Detect the current shape of the legacy claim/closure parents.
            // The partitioned parents have relkind 'p'; regular tables have
            // 'r' and need to be renamed aside so the helper can create the
            // partitioned parents under the canonical name. Copying the data
            // back happens after the helper returns, all inside this single
            // transaction so a crash leaves the schema in one of exactly two
            // states (pre-migration or post-migration).
            let lease_claims_relkind: Option<String> = sqlx::query_scalar(
                r#"
                SELECT c.relkind::text
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = 'lease_claims'
                "#,
            )
            .bind(schema)
            .fetch_optional(install_tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            let closures_relkind: Option<String> = sqlx::query_scalar(
                r#"
                SELECT c.relkind::text
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = 'lease_claim_closures'
                "#,
            )
            .bind(schema)
            .fetch_optional(install_tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            if lease_claims_relkind.as_deref() == Some("r") {
                sqlx::query(&format!(
                    "ALTER TABLE {schema}.lease_claims RENAME TO lease_claims_legacy"
                ))
                .execute(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            }
            if closures_relkind.as_deref() == Some("r") {
                sqlx::query(&format!(
                    "ALTER TABLE {schema}.lease_claim_closures RENAME TO lease_claim_closures_legacy"
                ))
                .execute(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            }

            // queue_count_snapshots was a staleness-cached counterpart of
            // queue_counts_exact. The dispatcher now derives the available
            // count directly from the head tables and nothing else needs the
            // snapshot. Drop it on every prepare_schema so an upgrade from an
            // older install reclaims the storage. Done before the helper
            // runs because the helper does not touch this legacy table.
            sqlx::query(&format!(
                "DROP TABLE IF EXISTS {schema}.queue_count_snapshots"
            ))
            .execute(install_tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            // The single forward-only DDL call. See the migration file
            // `awa-model/migrations/v023_install_queue_storage_substrate.sql`
            // for the function body. Default values match the constants in
            // this file (DEFAULT_QUEUE_SLOT_COUNT etc); the helper validates
            // that the default `awa` schema only ever gets default-shaped
            // installs.
            sqlx::query("SELECT awa.install_queue_storage_substrate($1, $2, $3, $4, $5)")
                .bind(schema)
                .bind(self.queue_slot_count() as i32)
                .bind(self.lease_slot_count() as i32)
                .bind(self.claim_slot_count() as i32)
                .bind(self.lease_claim_receipts())
                .execute(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

            // #169: apply receipt-plane fillfactor tunings. The install
            // helper above creates structure only; tunings live in
            // their own SQL function (see v024) and the orchestrator
            // here is the canonical place that composes them. This
            // means future perf knobs land additively in
            // `apply_receipt_plane_fillfactor` (or a sibling helper)
            // without touching the bigger install helper, and every
            // path that creates substrate — Rust prepare_schema, fresh
            // `awa migrate` (via v024's sweep), explicit operator call
            // against a custom schema — composes the same two pieces.
            sqlx::query("SELECT awa.apply_receipt_plane_fillfactor($1)")
                .bind(schema)
                .execute(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

            // Post-helper legacy fixups: copy any renamed-aside rows into the
            // newly partitioned parents created by the helper, then drop the
            // legacy table. ON CONFLICT DO NOTHING so a re-run after a
            // partial copy is idempotent. The outer transaction keeps the
            // copy + drop atomic so a crash between them leaves the schema
            // in one of exactly two states (pre or post migration); without
            // that the next `prepare_schema`'s ON CONFLICT would silently
            // mask the inconsistency.
            let lease_claims_legacy_exists: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = $1 AND c.relname = 'lease_claims_legacy'
                )
                "#,
            )
            .bind(schema)
            .fetch_one(install_tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            let closures_legacy_exists: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = $1 AND c.relname = 'lease_claim_closures_legacy'
                )
                "#,
            )
            .bind(schema)
            .fetch_one(install_tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            let legacy_claim_slot: Option<i32> =
                if lease_claims_legacy_exists || closures_legacy_exists {
                    Some(
                        sqlx::query_scalar(&format!(
                            "SELECT slot FROM {schema}.claim_ring_rotations ORDER BY generation DESC LIMIT 1"
                        ))
                        .fetch_one(install_tx.as_mut())
                        .await
                        .map_err(map_sqlx_error)?,
                    )
                } else {
                    None
                };

            if lease_claims_legacy_exists {
                sqlx::query(&format!(
                    "ALTER TABLE {schema}.lease_claims_legacy ADD COLUMN IF NOT EXISTS enqueue_shard SMALLINT NOT NULL DEFAULT 0"
                ))
                .execute(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
                sqlx::query(&format!(
                    "ALTER TABLE {schema}.lease_claims_legacy ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ"
                ))
                .execute(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                INSERT INTO {schema}.lease_claims (
                    claim_slot, job_id, run_lease, ready_slot, ready_generation,
                    queue, priority, attempt, max_attempts, lane_seq,
                    enqueue_shard, claimed_at, materialized_at, deadline_at
                )
                SELECT
                    $1,
                    job_id, run_lease, ready_slot, ready_generation,
                    queue, priority, attempt, max_attempts, lane_seq,
                    enqueue_shard, claimed_at, materialized_at, deadline_at
                FROM {schema}.lease_claims_legacy
                ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                "#
                ))
                .bind(legacy_claim_slot.expect("legacy claim slot should be present"))
                .execute(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    "DROP TABLE {schema}.lease_claims_legacy"
                ))
                .execute(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            }

            if closures_legacy_exists {
                sqlx::query(&format!(
                    r#"
                INSERT INTO {schema}.lease_claim_closures (
                    claim_slot, job_id, run_lease, outcome, closed_at
                )
                SELECT
                    $1,
                    job_id, run_lease, outcome, closed_at
                FROM {schema}.lease_claim_closures_legacy
                ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                "#
                ))
                .bind(legacy_claim_slot.expect("legacy claim slot should be present"))
                .execute(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    "DROP TABLE {schema}.lease_claim_closures_legacy"
                ))
                .execute(install_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            }

            Ok(())
        }
        .await;

        match install_result {
            Ok(()) => install_tx.commit().await.map_err(map_sqlx_error),
            Err(err) => {
                let _ = install_tx.rollback().await;
                Err(err)
            }
        }
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.activate_backend")]
    pub async fn activate_backend(&self, pool: &PgPool) -> Result<(), AwaError> {
        let schema = self.schema();
        let details = serde_json::json!({ "schema": schema });

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        // Mark queue storage active only after the full schema, partitions,
        // indexes, and helper functions are in place. The explicit install
        // helper is used by tests and queue-storage-only setups, so it must
        // flip both the migration-owned routing registry row and the storage
        // transition state.
        sqlx::query(
            r#"
            INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
            VALUES ('queue_storage', $1, now())
            ON CONFLICT (backend)
            DO UPDATE SET schema_name = EXCLUDED.schema_name, updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(schema)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let activation_result = sqlx::query(
            r#"
            UPDATE awa.storage_transition_state AS sts
            SET
                current_engine = 'queue_storage',
                prepared_engine = NULL,
                state = 'active',
                transition_epoch = CASE
                    WHEN sts.current_engine = 'queue_storage'
                     AND sts.prepared_engine IS NULL
                     AND sts.state = 'active'
                     AND sts.details = $1
                    THEN sts.transition_epoch
                    ELSE sts.transition_epoch + 1
                END,
                details = $1,
                entered_at = CASE
                    WHEN sts.current_engine = 'queue_storage'
                     AND sts.prepared_engine IS NULL
                     AND sts.state = 'active'
                     AND sts.details = $1
                    THEN sts.entered_at
                    ELSE now()
                END,
                updated_at = now(),
                finalized_at = CASE
                    WHEN sts.current_engine = 'queue_storage'
                     AND sts.prepared_engine IS NULL
                     AND sts.state = 'active'
                     AND sts.details = $1
                    THEN COALESCE(sts.finalized_at, now())
                    ELSE now()
                END
            WHERE sts.singleton
            "#,
        )
        .bind(details)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if activation_result.rows_affected() != 1 {
            return Err(AwaError::Validation(
                "queue storage activation requires the storage transition state row".into(),
            ));
        }

        tx.commit().await.map_err(map_sqlx_error)?;

        Ok(())
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.install")]
    pub async fn install(&self, pool: &PgPool) -> Result<(), AwaError> {
        self.prepare_schema(pool).await?;
        self.activate_backend(pool).await
    }

    pub async fn reset(&self, pool: &PgPool) -> Result<(), AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        // Drop any partial-migration leftover tables before the main
        // TRUNCATE. If `prepare_schema` crashed mid-migration, the
        // schema may contain `lease_claims_legacy` /
        // `lease_claim_closures_legacy` alongside the partitioned
        // parents. `reset()` must clean these out, otherwise the next
        // `prepare_schema()` runs the legacy migration again on top of
        // the freshly-emptied parent and silently re-inserts old rows.
        sqlx::query(&format!(
            "DROP TABLE IF EXISTS {schema}.lease_claims_legacy"
        ))
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            "DROP TABLE IF EXISTS {schema}.lease_claim_closures_legacy"
        ))
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            TRUNCATE
                {schema}.ready_entries,
                {schema}.ready_claim_attempt_batches,
                {schema}.ready_tombstones,
                {schema}.ready_segments,
                {schema}.done_entries,
                {schema}.receipt_completion_batches,
                {schema}.receipt_completion_tombstones,
                {schema}.dlq_entries,
                {schema}.leases,
                {schema}.lease_claims,
                {schema}.lease_claim_batches,
                {schema}.lease_claim_closures,
                {schema}.lease_claim_closure_batches,
                {schema}.attempt_state,
                {schema}.deferred_jobs,
                {schema}.queue_lanes,
                {schema}.queue_terminal_rollups,
                {schema}.queue_terminal_rollup_deltas,
                {schema}.queue_terminal_live_counts,
                {schema}.queue_terminal_count_deltas,
                {schema}.queue_claimer_leases,
                {schema}.queue_claimer_state,
                {schema}.queue_ring_slots,
                {schema}.lease_ring_slots,
                {schema}.claim_ring_slots,
                {schema}.queue_ring_rotations,
                {schema}.lease_ring_rotations,
                {schema}.claim_ring_rotations
            "#
        ))
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            "ALTER SEQUENCE {schema}.job_id_seq RESTART WITH 1"
        ))
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        for (family, slot_count) in [
            (RingFamily::Queue, self.queue_slot_count()),
            (RingFamily::Lease, self.lease_slot_count()),
            (RingFamily::Claim, self.claim_slot_count()),
        ] {
            let state = family.state_relname();
            sqlx::query(&format!(
                r#"
                UPDATE {schema}.{state}
                SET slot_count = $1
                WHERE singleton = TRUE
                "#
            ))
            .bind(slot_count as i32)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            // The rotation ledgers were truncated above; re-seed the
            // genesis cursor (slot 0, generation 0).
            let ledger = family.ledger_relname();
            sqlx::query(&format!(
                "INSERT INTO {schema}.{ledger} (generation, slot) VALUES (0, 0)"
            ))
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        for slot in 0..self.queue_slot_count() {
            sqlx::query(&format!(
                "INSERT INTO {schema}.queue_ring_slots (slot) VALUES ($1)"
            ))
            .bind(slot as i32)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        for slot in 0..self.lease_slot_count() {
            sqlx::query(&format!(
                "INSERT INTO {schema}.lease_ring_slots (slot) VALUES ($1)"
            ))
            .bind(slot as i32)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        for slot in 0..self.claim_slot_count() {
            sqlx::query(&format!(
                "INSERT INTO {schema}.claim_ring_slots (slot) VALUES ($1)"
            ))
            .bind(slot as i32)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        tx.commit().await.map_err(map_sqlx_error)?;

        // queue_lanes was TRUNCATEd above and queue_meta may have a new
        // shard configuration for the next round. Clear both caches so
        // the next ensure_lane / shard_for_enqueue calls re-observe DB
        // state.
        self.clear_lane_cache();
        self.enqueue_shards_cache
            .lock()
            .expect("enqueue_shards_cache poisoned")
            .clear();
        Ok(())
    }

    async fn ensure_lane<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        queue: &str,
        priority: i16,
        enqueue_shard: i16,
    ) -> Result<(), AwaError> {
        // Fast path: this store has previously written the three lane
        // rows for this `(queue, priority, shard)` triple. The cache is
        // optimistic: another transaction may have marked the lane before
        // commit, or the marking transaction may later roll back. Verify the
        // head row is visible in this transaction before trusting the cache.
        if self.lane_is_cached(queue, priority, enqueue_shard) {
            let schema = self.schema();
            let visible: bool = sqlx::query_scalar(&format!(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM {schema}.queue_enqueue_heads
                    WHERE queue = $1
                      AND priority = $2
                      AND enqueue_shard = $3
                )
                "#
            ))
            .bind(queue)
            .bind(priority)
            .bind(enqueue_shard)
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            if visible {
                return Ok(());
            }

            self.invalidate_cached_lane(queue, priority, enqueue_shard);
        }

        self.ensure_lane_inserts(tx, queue, priority, enqueue_shard)
            .await
    }

    /// Run the three lane-row inserts unconditionally and mark the
    /// `(queue, priority)` pair as cached on success. Skips the cache
    /// fast path so callers in the rollback-recovery path can force a
    /// re-insert without racing another transaction that has marked
    /// the lane but not yet committed its inserts.
    async fn ensure_lane_inserts<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        queue: &str,
        priority: i16,
        enqueue_shard: i16,
    ) -> Result<(), AwaError> {
        let schema = self.schema();
        let lane_lock_key = format!("{schema}:{queue}:{priority}:{enqueue_shard}");
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lane_lock_key)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.queue_lanes (queue, priority)
            VALUES ($1, $2)
            ON CONFLICT (queue, priority) DO NOTHING
            "#
        ))
        .bind(queue)
        .bind(priority)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.queue_enqueue_heads (queue, priority, enqueue_shard)
            VALUES ($1, $2, $3)
            ON CONFLICT (queue, priority, enqueue_shard) DO NOTHING
            "#
        ))
        .bind(queue)
        .bind(priority)
        .bind(enqueue_shard)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.queue_claim_heads (queue, priority, enqueue_shard)
            VALUES ($1, $2, $3)
            ON CONFLICT (queue, priority, enqueue_shard) DO NOTHING
            "#
        ))
        .bind(queue)
        .bind(priority)
        .bind(enqueue_shard)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            SELECT {schema}.ensure_lane_sequences($1, $2, $3)
            "#
        ))
        .bind(queue)
        .bind(priority)
        .bind(enqueue_shard)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        self.mark_lane_ensured(queue, priority, enqueue_shard);
        Ok(())
    }

    /// Pick the enqueue shard for a row on `queue`.
    ///
    /// Reads `awa.queue_meta.enqueue_shards` (default 1, cached
    /// in-process). With `enqueue_shards = 1` the result is always 0.
    /// With `enqueue_shards > 1`:
    ///
    /// - If `ordering_key` is `Some(k)`, the shard is a stable hash
    ///   of `k` modulo the shard count. Two rows sharing an
    ///   ordering key always land on the same shard, which preserves
    ///   FIFO within the key.
    /// - If `ordering_key` is `None`, the shard comes from the
    ///   per-store rotor, which spreads consecutive picks across
    ///   shards.
    async fn shard_for_enqueue(
        &self,
        pool_executor: impl sqlx::PgExecutor<'_>,
        queue: &str,
        ordering_key: Option<&[u8]>,
    ) -> Result<i16, AwaError> {
        if let Some(cached) = self
            .enqueue_shards_cache
            .lock()
            .expect("enqueue_shards_cache poisoned")
            .get(queue)
            .copied()
        {
            return Ok(self.pick_shard(cached, ordering_key));
        }

        let shards: i16 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(MAX(enqueue_shards), 1)::smallint
            FROM awa.queue_meta
            WHERE queue = $1
            "#,
        )
        .bind(queue)
        .fetch_one(pool_executor)
        .await
        .map_err(map_sqlx_error)?;

        let shards = shards.max(1);
        self.enqueue_shards_cache
            .lock()
            .expect("enqueue_shards_cache poisoned")
            .insert(queue.to_string(), shards);

        Ok(self.pick_shard(shards, ordering_key))
    }

    /// Map a row (or a no-key sub-batch) to its shard.
    ///
    /// At `shards <= 1` every row goes to shard 0. Otherwise the
    /// caller-supplied `ordering_key` hashes deterministically into
    /// `[0, shards)` via [`shard_for_ordering_key`]; an absent key
    /// advances the per-store rotor once and returns the next shard.
    /// Callers route a whole no-key sub-batch through a single
    /// `pick_shard(_, None)` so the rotor amortises across the batch
    /// rather than firing per row.
    fn pick_shard(&self, shards: i16, ordering_key: Option<&[u8]>) -> i16 {
        if shards <= 1 {
            return 0;
        }
        match ordering_key {
            Some(key) => shard_for_ordering_key(key, shards),
            None => {
                let raw = self.shard_rotor.fetch_add(1, Ordering::Relaxed) as i32;
                raw.rem_euclid(shards as i32) as i16
            }
        }
    }

    fn lane_is_cached(&self, queue: &str, priority: i16, enqueue_shard: i16) -> bool {
        let cache = self.ensured_lanes.lock().expect("ensured_lanes mutex");
        cache.contains(&(queue.to_string(), priority, enqueue_shard))
    }

    fn mark_lane_ensured(&self, queue: &str, priority: i16, enqueue_shard: i16) {
        self.ensured_lanes
            .lock()
            .expect("ensured_lanes mutex")
            .insert((queue.to_string(), priority, enqueue_shard));
    }

    fn invalidate_cached_lane(&self, queue: &str, priority: i16, enqueue_shard: i16) {
        self.ensured_lanes
            .lock()
            .expect("ensured_lanes mutex")
            .remove(&(queue.to_string(), priority, enqueue_shard));
    }

    fn clear_lane_cache(&self) {
        self.ensured_lanes
            .lock()
            .expect("ensured_lanes mutex")
            .clear();
    }

    // Reserve lane sequence numbers for a specific
    // `(queue, priority, shard)` triple and return the lane sequence at
    // which the caller's range starts. If the head row is missing —
    // typically because a previous ensure_lane ran inside a transaction
    // that ultimately rolled back, leaving a stale cache entry behind —
    // the cache entry is invalidated, ensure_lane is re-run, and the reserve
    // is retried exactly once. A second miss surfaces as
    // RowNotFound.
    async fn advance_enqueue_head<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        queue: &str,
        priority: i16,
        enqueue_shard: i16,
        count: i64,
    ) -> Result<i64, AwaError> {
        let schema = self.schema();
        let sql = format!(
            r#"
            SELECT {schema}.reserve_enqueue_seq($1, $2, $3, $4)
            FROM {schema}.queue_enqueue_heads
            WHERE queue = $1 AND priority = $2 AND enqueue_shard = $3
            "#
        );

        let maybe_start: Option<i64> = sqlx::query_scalar(&sql)
            .bind(queue)
            .bind(priority)
            .bind(enqueue_shard)
            .bind(count)
            .fetch_optional(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        if let Some(start) = maybe_start {
            return Ok(start);
        }

        // Recovery path: a prior ensure_lane marked the cache and
        // then rolled back, leaving the head row missing. Bypass the
        // cache fast path here and run the inserts unconditionally —
        // calling `ensure_lane` would re-take the fast path if
        // another concurrent transaction has re-marked the cache but
        // not yet committed its inserts, leaving us in the same
        // failure state.
        self.invalidate_cached_lane(queue, priority, enqueue_shard);
        self.ensure_lane_inserts(tx, queue, priority, enqueue_shard)
            .await?;
        let start: i64 = sqlx::query_scalar(&sql)
            .bind(queue)
            .bind(priority)
            .bind(enqueue_shard)
            .bind(count)
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        Ok(start)
    }

    /// Current ring cursor: the max-generation row of the ring's
    /// append-only rotation ledger (#371). Backward PK scan, O(1).
    async fn ring_cursor_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        family: RingFamily,
    ) -> Result<(i32, i64), AwaError> {
        let schema = self.schema();
        let ledger = family.ledger_relname();
        sqlx::query_as(&format!(
            r#"
            SELECT slot, generation
            FROM {schema}.{ledger}
            ORDER BY generation DESC
            LIMIT 1
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)
    }

    /// Ring width from the demoted cold-config singleton.
    async fn ring_slot_count_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        family: RingFamily,
    ) -> Result<i32, AwaError> {
        let schema = self.schema();
        let state = family.state_relname();
        sqlx::query_scalar(&format!(
            "SELECT slot_count FROM {schema}.{state} WHERE singleton = TRUE"
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)
    }

    /// Try to take the per-ring rotation lock (transaction-scoped
    /// advisory lock). Non-blocking: rotate/prune/rollup are periodic
    /// maintenance ticks, so under contention the caller skips the tick
    /// instead of queueing behind the holder.
    async fn try_ring_rotation_lock_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        family: RingFamily,
    ) -> Result<bool, AwaError> {
        sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(family.rotation_lock_name(self.schema()))
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)
    }

    /// Append the next rotation to the ring's ledger. The primary key on
    /// `generation` makes the insert a compare-and-swap: a conflict means
    /// another rotator observed the same cursor and won — the caller must
    /// treat `false` as a lost race, not retry with the same generation.
    async fn append_ring_rotation_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        family: RingFamily,
        next_generation: i64,
        next_slot: i32,
    ) -> Result<bool, AwaError> {
        let schema = self.schema();
        let ledger = family.ledger_relname();
        let inserted = sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.{ledger} (generation, slot)
            VALUES ($1, $2)
            ON CONFLICT (generation) DO NOTHING
            "#
        ))
        .bind(next_generation)
        .bind(next_slot)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(inserted.rows_affected() == 1)
    }

    async fn current_queue_ring<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    ) -> Result<(i32, i64), AwaError> {
        self.ring_cursor_tx(tx, RingFamily::Queue).await
    }

    async fn next_job_ids<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        count: usize,
    ) -> Result<Vec<i64>, AwaError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let query = format!(
            "SELECT nextval('{}')::bigint FROM generate_series(1, $1::int)",
            self.job_id_sequence()
        );

        sqlx::query_scalar(&query)
            .bind(count as i32)
            .fetch_all(tx.as_mut())
            .await
            .map_err(map_sqlx_error)
    }

    async fn current_timestamp_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    ) -> Result<DateTime<Utc>, AwaError> {
        sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)
    }

    async fn claim_ready_rows_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        queue: &str,
        max_batch: i64,
        deadline_duration: Duration,
        aging_interval: Duration,
    ) -> Result<Vec<ReadyJobLeaseRow>, AwaError> {
        let schema = self.schema();
        sqlx::query_as(&format!(
            r#"
            SELECT
                ready_slot,
                ready_generation,
                lane_seq,
                enqueue_shard,
                lease_slot,
                lease_generation,
                claim_slot,
                receipt_id,
                claim_batch_id,
                claim_batch_index,
                job_id,
                kind,
                queue,
                args,
                lane_priority,
                priority,
                attempt,
                run_lease,
                max_attempts,
                run_at,
                heartbeat_at,
                deadline_at,
                attempted_at,
                created_at,
                unique_key,
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload
            FROM {schema}.claim_ready_runtime($1, $2, $3, $4)
            "#
        ))
        .bind(queue)
        .bind(max_batch)
        .bind(deadline_duration.as_secs_f64())
        .bind(aging_interval.as_secs_f64())
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)
    }

    fn claim_cursor_advances(rows: &[ReadyJobLeaseRow]) -> Vec<ClaimCursorAdvance> {
        let mut next_by_lane: BTreeMap<ClaimCursorLaneKey, i64> = BTreeMap::new();
        for row in rows {
            let key = (row.queue.clone(), row.lane_priority, row.enqueue_shard);
            let next = row.lane_seq + 1;
            next_by_lane
                .entry(key)
                .and_modify(|current| *current = (*current).max(next))
                .or_insert(next);
        }

        next_by_lane
            .into_iter()
            .map(
                |((queue, priority, enqueue_shard), next_seq)| ClaimCursorAdvance {
                    queue,
                    priority,
                    enqueue_shard,
                    next_seq,
                    only_if_current: None,
                },
            )
            .collect()
    }

    fn normalize_claim_cursor_advances(advances: &[ClaimCursorAdvance]) -> Vec<ClaimCursorAdvance> {
        let mut grouped: GroupedClaimCursorAdvances = BTreeMap::new();

        for advance in advances {
            let key = (
                advance.queue.clone(),
                advance.priority,
                advance.enqueue_shard,
            );
            let (unconditional, conditional) = grouped.entry(key).or_default();
            if let Some(only_if_current) = advance.only_if_current {
                conditional
                    .entry(only_if_current)
                    .and_modify(|next| *next = (*next).max(advance.next_seq))
                    .or_insert(advance.next_seq);
            } else {
                *unconditional = Some(
                    unconditional
                        .map(|next| next.max(advance.next_seq))
                        .unwrap_or(advance.next_seq),
                );
            }
        }

        let mut normalized = Vec::with_capacity(advances.len());
        for ((queue, priority, enqueue_shard), (unconditional, conditional)) in grouped {
            if let Some(next_seq) = unconditional {
                normalized.push(ClaimCursorAdvance {
                    queue,
                    priority,
                    enqueue_shard,
                    next_seq,
                    only_if_current: None,
                });
                continue;
            }

            for (only_if_current, next_seq) in conditional {
                normalized.push(ClaimCursorAdvance {
                    queue: queue.clone(),
                    priority,
                    enqueue_shard,
                    next_seq,
                    only_if_current: Some(only_if_current),
                });
            }
        }

        normalized
    }

    async fn advance_claim_cursors(&self, pool: &PgPool, advances: &[ClaimCursorAdvance]) {
        let advances = Self::normalize_claim_cursor_advances(advances);
        // PostgreSQL sequence state is not rolled back with the surrounding
        // transaction. Keep claim cursors lagging rather than risking a cursor
        // that gets ahead of ready rows whose claim/cancel transaction aborts.
        for attempt in 1..=3 {
            match self.advance_claim_cursors_strict(pool, &advances).await {
                Ok(()) => return,
                Err(err) if attempt < 3 => {
                    tracing::warn!(
                        error = ?err,
                        lanes = advances.len(),
                        attempt,
                        "failed to advance queue-storage claim cursors after committed state change; retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(25 * attempt as u64)).await;
                }
                Err(err) => {
                    tracing::warn!(
                        error = ?err,
                        lanes = advances.len(),
                        attempts = attempt,
                        "failed to advance queue-storage claim cursors after committed state change"
                    );
                    return;
                }
            }
        }
    }

    async fn advance_claim_cursors_strict(
        &self,
        pool: &PgPool,
        advances: &[ClaimCursorAdvance],
    ) -> Result<(), AwaError> {
        if advances.is_empty() {
            return Ok(());
        }

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        for advance in advances {
            sqlx::query(&format!(
                r#"
                WITH head AS MATERIALIZED (
                    SELECT seq_name
                    FROM {schema}.queue_claim_heads
                    WHERE queue = $1
                      AND priority = $2
                      AND enqueue_shard = $3
                    FOR UPDATE
                )
                SELECT {schema}.set_sequence_next(seq_name, $4)
                FROM head
                WHERE $5::bigint IS NULL
                   OR {schema}.sequence_next_value(seq_name) = $5
                "#
            ))
            .bind(&advance.queue)
            .bind(advance.priority)
            .bind(advance.enqueue_shard)
            .bind(advance.next_seq)
            .bind(advance.only_if_current)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn execute_ready_inserts_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        ring: (i32, i64),
        rows: &[RuntimeReadyInsert],
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let schema = self.schema();
        let mut builder = QueryBuilder::<Postgres>::new(format!(
            "INSERT INTO {schema}.ready_entries (ready_slot, ready_generation, job_id, kind, queue, args, priority, attempt, run_lease, max_attempts, lane_seq, enqueue_shard, run_at, attempted_at, created_at, unique_key, unique_states, payload) "
        ));
        builder.push_values(rows.iter(), |mut b, row| {
            b.push_bind(ring.0)
                .push_bind(ring.1)
                .push_bind(row.job_id)
                .push_bind(&row.kind)
                .push_bind(&row.queue)
                .push_bind(&row.args)
                .push_bind(row.priority)
                .push_bind(row.attempt)
                .push_bind(row.run_lease)
                .push_bind(row.max_attempts)
                .push_bind(row.lane_seq)
                .push_bind(row.enqueue_shard)
                .push_bind(row.run_at)
                .push_bind(row.attempted_at)
                .push_bind(row.created_at)
                .push_bind(&row.unique_key)
                .push_bind(&row.unique_states)
                .push_bind(storage_payload(&row.payload));
        });
        builder
            .build()
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        Ok(rows.len())
    }

    fn ready_segments_from_rows(rows: &[RuntimeReadyInsert]) -> Vec<ReadySegmentInsert> {
        if rows.is_empty() {
            return Vec::new();
        }

        let mut segments = Vec::new();
        let mut start_index = 0usize;

        for index in 1..=rows.len() {
            let split = if index == rows.len() {
                true
            } else {
                let previous = &rows[index - 1];
                let current = &rows[index];
                previous.queue != current.queue
                    || previous.priority != current.priority
                    || previous.enqueue_shard != current.enqueue_shard
                    || previous.lane_seq + 1 != current.lane_seq
                    || previous.run_at != current.run_at
            };

            if split {
                let first = &rows[start_index];
                let last = &rows[index - 1];
                segments.push(ReadySegmentInsert {
                    queue: first.queue.clone(),
                    priority: first.priority,
                    enqueue_shard: first.enqueue_shard,
                    first_lane_seq: first.lane_seq,
                    next_lane_seq: last.lane_seq + 1,
                    first_run_at: first.run_at,
                });
                start_index = index;
            }
        }

        segments
    }

    async fn insert_ready_segments_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        ring: (i32, i64),
        rows: &[RuntimeReadyInsert],
    ) -> Result<usize, AwaError> {
        let segments = Self::ready_segments_from_rows(rows);
        if segments.is_empty() {
            return Ok(0);
        }

        let schema = self.schema();
        let mut builder = QueryBuilder::<Postgres>::new(format!(
            "INSERT INTO {schema}.ready_segments (ready_slot, ready_generation, queue, priority, enqueue_shard, first_lane_seq, next_lane_seq, first_run_at) "
        ));
        builder.push_values(segments.iter(), |mut b, segment| {
            b.push_bind(ring.0)
                .push_bind(ring.1)
                .push_bind(&segment.queue)
                .push_bind(segment.priority)
                .push_bind(segment.enqueue_shard)
                .push_bind(segment.first_lane_seq)
                .push_bind(segment.next_lane_seq)
                .push_bind(segment.first_run_at);
        });
        builder.push(
            " ON CONFLICT (ready_slot, ready_generation, queue, priority, enqueue_shard, first_lane_seq) DO NOTHING",
        );
        builder
            .build()
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        Ok(segments.len())
    }

    async fn execute_ready_copy_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        ring: (i32, i64),
        rows: &[RuntimeReadyInsert],
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let schema = self.schema();
        let copy_sql = format!(
            "COPY {schema}.ready_entries (ready_slot, ready_generation, job_id, kind, queue, args, priority, attempt, run_lease, max_attempts, lane_seq, enqueue_shard, run_at, attempted_at, created_at, unique_key, unique_states, payload) FROM STDIN WITH (FORMAT csv, NULL '{COPY_NULL_SENTINEL}')"
        );
        let mut copy_in = tx
            .as_mut()
            .copy_in_raw(&copy_sql)
            .await
            .map_err(map_sqlx_error)?;
        // 320 bytes/row is only a rough starting point; large JSON payloads
        // are bounded by chunked COPY sends below rather than by this reserve.
        let mut csv_buf = Vec::with_capacity(rows.len().min(1024) * 320);
        for row in rows {
            write_ready_copy_row(&mut csv_buf, ring.0, ring.1, row);
            if csv_buf.len() >= COPY_CHUNK_TARGET_BYTES {
                let chunk =
                    std::mem::replace(&mut csv_buf, Vec::with_capacity(COPY_CHUNK_TARGET_BYTES));
                copy_in.send(chunk).await.map_err(map_sqlx_error)?;
            }
        }
        if !csv_buf.is_empty() {
            copy_in.send(csv_buf).await.map_err(map_sqlx_error)?;
        }
        copy_in.finish().await.map_err(map_sqlx_error)?;

        Ok(rows.len())
    }

    async fn insert_ready_rows_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: Vec<RuntimeReadyRow>,
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let grouped = self.group_ready_rows_by_shard(tx, rows).await?;
        let total_rows: usize = grouped.values().map(Vec::len).sum();
        let job_ids = self.next_job_ids(tx, total_rows).await?;
        let mut job_id_iter = job_ids.into_iter();

        let mut ready_rows = Vec::with_capacity(total_rows);
        let mut lane_ranges = Vec::with_capacity(grouped.len());

        for ((queue, priority, enqueue_shard), lane_rows) in grouped {
            let range_start = ready_rows.len();

            for row in lane_rows {
                let job_id = job_id_iter.next().ok_or_else(|| {
                    AwaError::Validation("queue storage job id allocation underflow".to_string())
                })?;
                ready_rows.push(RuntimeReadyInsert {
                    job_id,
                    kind: row.kind,
                    queue: row.queue,
                    args: row.args,
                    priority: row.priority,
                    attempt: row.attempt,
                    run_lease: row.run_lease,
                    max_attempts: row.max_attempts,
                    run_at: row.run_at,
                    attempted_at: row.attempted_at,
                    lane_seq: 0,
                    enqueue_shard,
                    created_at: row.created_at,
                    unique_key: row.unique_key,
                    unique_states: row.unique_states,
                    payload: row.payload,
                });
            }
            lane_ranges.push((
                queue,
                priority,
                enqueue_shard,
                range_start,
                ready_rows.len(),
            ));
        }

        self.sync_ready_enqueue_unique_claims(tx, &ready_rows)
            .await?;
        for (queue, priority, enqueue_shard, range_start, range_end) in lane_ranges {
            self.ensure_lane(tx, &queue, priority, enqueue_shard)
                .await?;

            let count = (range_end - range_start) as i64;
            let start_seq = self
                .advance_enqueue_head(tx, &queue, priority, enqueue_shard, count)
                .await?;

            for (offset, row) in ready_rows[range_start..range_end].iter_mut().enumerate() {
                row.lane_seq = start_seq + offset as i64;
            }
        }
        let ring = self.current_queue_ring(tx).await?;
        self.execute_ready_inserts_tx(tx, ring, &ready_rows).await?;
        self.insert_ready_segments_tx(tx, ring, &ready_rows).await?;
        Ok(total_rows)
    }

    /// Re-group rows by `(queue, priority, enqueue_shard)` so each
    /// resulting bucket targets exactly one head row.
    ///
    /// The two routing modes are amortised differently:
    ///
    /// - **Rows with `ordering_key`** are hashed per row via
    ///   `shard_for_ordering_key`, so jobs that share a key always
    ///   land on the same shard. Each distinct key may produce its
    ///   own sub-bucket inside one batch.
    /// - **Rows without `ordering_key`** share **one rotor pick per
    ///   `(queue, priority)` per call**. A batch of 500 rotor-routed
    ///   rows produces one `advance_enqueue_head` UPDATE and one
    ///   INSERT, not 500. The rotor still advances per batch, so
    ///   successive batches spread across shards; the per-batch
    ///   amortisation is what makes `enqueue_shards > 1` net-faster
    ///   than `S = 1` at moderate concurrency.
    ///
    /// Mixing keyed and rotor rows in one call is supported — the
    /// keyed rows fan out by key while the rotor rows collapse into
    /// a single shard-sub-batch.
    async fn group_ready_rows_by_shard<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: Vec<RuntimeReadyRow>,
    ) -> Result<BTreeMap<(String, i16, i16), Vec<RuntimeReadyRow>>, AwaError> {
        // Partition by (queue, priority) first so the rotor pick for
        // the no-key sub-batch can amortise over every row that shares
        // its destination lane.
        let mut by_queue_priority: BTreeMap<(String, i16), Vec<RuntimeReadyRow>> = BTreeMap::new();
        for row in rows {
            by_queue_priority
                .entry((row.queue.clone(), row.priority))
                .or_default()
                .push(row);
        }

        let mut grouped: BTreeMap<(String, i16, i16), Vec<RuntimeReadyRow>> = BTreeMap::new();
        for ((queue, priority), bucket) in by_queue_priority {
            let mut rotor_rows: Vec<RuntimeReadyRow> = Vec::with_capacity(bucket.len());
            for row in bucket {
                if row.ordering_key.is_some() {
                    let shard = self
                        .shard_for_enqueue(tx.as_mut(), &queue, row.ordering_key.as_deref())
                        .await?;
                    grouped
                        .entry((queue.clone(), priority, shard))
                        .or_default()
                        .push(row);
                } else {
                    rotor_rows.push(row);
                }
            }
            if !rotor_rows.is_empty() {
                let shard = self.shard_for_enqueue(tx.as_mut(), &queue, None).await?;
                grouped
                    .entry((queue.clone(), priority, shard))
                    .or_default()
                    .extend(rotor_rows);
            }
        }
        Ok(grouped)
    }

    async fn insert_ready_rows_copy_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: Vec<RuntimeReadyRow>,
        job_ids: Vec<i64>,
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let grouped = self.group_ready_rows_by_shard(tx, rows).await?;

        let total_rows: usize = grouped.values().map(Vec::len).sum();
        if job_ids.len() != total_rows {
            return Err(AwaError::Validation(
                "queue storage job id allocation count mismatch".to_string(),
            ));
        }
        let mut job_id_iter = job_ids.into_iter();

        let mut ready_rows = Vec::with_capacity(total_rows);
        let mut lane_ranges = Vec::with_capacity(grouped.len());

        for ((queue, priority, enqueue_shard), lane_rows) in grouped {
            let range_start = ready_rows.len();

            for row in lane_rows {
                let job_id = job_id_iter.next().ok_or_else(|| {
                    AwaError::Validation("queue storage job id allocation underflow".to_string())
                })?;
                ready_rows.push(RuntimeReadyInsert {
                    job_id,
                    kind: row.kind,
                    queue: row.queue,
                    args: row.args,
                    priority: row.priority,
                    attempt: row.attempt,
                    run_lease: row.run_lease,
                    max_attempts: row.max_attempts,
                    run_at: row.run_at,
                    attempted_at: row.attempted_at,
                    lane_seq: 0,
                    enqueue_shard,
                    created_at: row.created_at,
                    unique_key: row.unique_key,
                    unique_states: row.unique_states,
                    payload: row.payload,
                });
            }
            lane_ranges.push((
                queue,
                priority,
                enqueue_shard,
                range_start,
                ready_rows.len(),
            ));
        }

        self.sync_ready_enqueue_unique_claims(tx, &ready_rows)
            .await?;
        for (queue, priority, enqueue_shard, range_start, range_end) in lane_ranges {
            self.ensure_lane(tx, &queue, priority, enqueue_shard)
                .await?;

            let count = (range_end - range_start) as i64;
            let start_seq = self
                .advance_enqueue_head(tx, &queue, priority, enqueue_shard, count)
                .await?;

            for (offset, row) in ready_rows[range_start..range_end].iter_mut().enumerate() {
                row.lane_seq = start_seq + offset as i64;
            }
        }
        let ring = self.current_queue_ring(tx).await?;
        self.execute_ready_copy_tx(tx, ring, &ready_rows).await?;
        self.insert_ready_segments_tx(tx, ring, &ready_rows).await?;
        Ok(total_rows)
    }

    async fn insert_existing_ready_rows_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: Vec<ExistingReadyRow>,
        old_state: Option<JobState>,
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut grouped: BTreeMap<(String, i16), Vec<ExistingReadyRow>> = BTreeMap::new();
        for row in rows {
            grouped
                .entry((row.queue.clone(), row.priority))
                .or_default()
                .push(row);
        }

        let total_rows: usize = grouped.values().map(Vec::len).sum();
        let mut ready_rows = Vec::with_capacity(total_rows);

        for ((queue, priority), lane_rows) in grouped {
            // Re-enqueue paths (retry-after, age-waiting, DLQ retry,
            // callback resume) do not carry the producer's original
            // `ordering_key`: the row came back from terminal storage
            // or from a deferred row, where the key was not retained.
            // Fall back to the rotor so the retry batch picks a shard
            // uniformly. Workloads that need ordering preserved across
            // retries must re-enqueue with the original key from the
            // application layer.
            let enqueue_shard = self.shard_for_enqueue(tx.as_mut(), &queue, None).await?;
            self.ensure_lane(tx, &queue, priority, enqueue_shard)
                .await?;

            let count = lane_rows.len() as i64;
            let start_seq = self
                .advance_enqueue_head(tx, &queue, priority, enqueue_shard, count)
                .await?;

            for (offset, row) in lane_rows.into_iter().enumerate() {
                self.sync_unique_claim(
                    tx,
                    row.job_id,
                    &row.unique_key,
                    row.unique_states.as_deref(),
                    old_state,
                    Some(JobState::Available),
                )
                .await?;
                ready_rows.push(RuntimeReadyInsert {
                    job_id: row.job_id,
                    kind: row.kind,
                    queue: row.queue,
                    args: row.args,
                    priority: row.priority,
                    attempt: row.attempt,
                    run_lease: row.run_lease,
                    max_attempts: row.max_attempts,
                    run_at: row.run_at,
                    attempted_at: row.attempted_at,
                    lane_seq: start_seq + offset as i64,
                    enqueue_shard,
                    created_at: row.created_at,
                    unique_key: row.unique_key,
                    unique_states: row.unique_states,
                    payload: row.payload,
                });
            }
        }

        let ring = self.current_queue_ring(tx).await?;
        self.execute_ready_inserts_tx(tx, ring, &ready_rows).await?;
        self.insert_ready_segments_tx(tx, ring, &ready_rows).await?;
        Ok(total_rows)
    }

    async fn insert_deferred_rows_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: Vec<DeferredJobRow>,
        old_state: Option<JobState>,
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        if old_state.is_none() {
            self.sync_deferred_enqueue_unique_claims(tx, &rows).await?;
        } else {
            for row in &rows {
                self.sync_unique_claim(
                    tx,
                    row.job_id,
                    &row.unique_key,
                    row.unique_states.as_deref(),
                    old_state,
                    Some(row.state),
                )
                .await?;
            }
        }

        let schema = self.schema();
        let mut builder = QueryBuilder::<Postgres>::new(format!(
            "INSERT INTO {schema}.deferred_jobs (job_id, kind, queue, args, state, priority, attempt, run_lease, max_attempts, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload) "
        ));
        builder.push_values(rows.iter(), |mut b, row| {
            b.push_bind(row.job_id)
                .push_bind(&row.kind)
                .push_bind(&row.queue)
                .push_bind(&row.args)
                .push_bind(row.state)
                .push_bind(row.priority)
                .push_bind(row.attempt)
                .push_bind(row.run_lease)
                .push_bind(row.max_attempts)
                .push_bind(row.run_at)
                .push_bind(row.attempted_at)
                .push_bind(row.finalized_at)
                .push_bind(row.created_at)
                .push_bind(&row.unique_key)
                .push_bind(&row.unique_states)
                .push_bind(storage_payload(&row.payload));
        });
        builder
            .build()
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        Ok(rows.len())
    }

    async fn insert_deferred_rows_copy_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: Vec<DeferredJobRow>,
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        self.sync_deferred_enqueue_unique_claims(tx, &rows).await?;

        let schema = self.schema();
        let copy_sql = format!(
            "COPY {schema}.deferred_jobs (job_id, kind, queue, args, state, priority, attempt, run_lease, max_attempts, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload) FROM STDIN WITH (FORMAT csv, NULL '{COPY_NULL_SENTINEL}')"
        );
        let mut copy_in = tx
            .as_mut()
            .copy_in_raw(&copy_sql)
            .await
            .map_err(map_sqlx_error)?;
        // 320 bytes/row is only a rough starting point; large JSON payloads
        // are bounded by chunked COPY sends below rather than by this reserve.
        let mut csv_buf = Vec::with_capacity(rows.len().min(1024) * 320);
        for row in &rows {
            write_deferred_copy_row(&mut csv_buf, row);
            if csv_buf.len() >= COPY_CHUNK_TARGET_BYTES {
                let chunk =
                    std::mem::replace(&mut csv_buf, Vec::with_capacity(COPY_CHUNK_TARGET_BYTES));
                copy_in.send(chunk).await.map_err(map_sqlx_error)?;
            }
        }
        if !csv_buf.is_empty() {
            copy_in.send(csv_buf).await.map_err(map_sqlx_error)?;
        }
        copy_in.finish().await.map_err(map_sqlx_error)?;

        Ok(rows.len())
    }

    async fn insert_done_rows_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[DoneJobRow],
        old_state: Option<JobState>,
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        for row in rows {
            self.sync_unique_claim(
                tx,
                row.job_id,
                &row.unique_key,
                row.unique_states.as_deref(),
                old_state,
                Some(row.state),
            )
            .await?;
        }

        let schema = self.schema();
        let keep_ready_backing = matches!(
            old_state,
            Some(JobState::Running | JobState::WaitingExternal)
        );
        let ready_payloads = if rows
            .iter()
            .any(|row| !is_storage_payload_empty(&row.payload))
        {
            self.ready_payloads_for_done_rows_tx(tx, rows).await?
        } else {
            HashMap::new()
        };
        let mut ordered_rows: Vec<&DoneJobRow> = rows.iter().collect();
        ordered_rows.sort_unstable_by_key(|row| {
            (
                row.ready_slot,
                row.ready_generation,
                row.queue.as_str(),
                row.priority,
                row.enqueue_shard,
                row.lane_seq,
                row.job_id,
            )
        });
        let (ready_backed, synthetic): (Vec<_>, Vec<_>) = ordered_rows
            .into_iter()
            .partition(|row| keep_ready_backing && row.lane_seq >= 0);

        if !ready_backed.is_empty() {
            let mut builder = QueryBuilder::<Postgres>::new(format!(
                "INSERT INTO {schema}.done_entries (ready_slot, ready_generation, job_id, kind, queue, state, priority, attempt, run_lease, lane_seq, enqueue_shard, attempted_at, finalized_at, payload) "
            ));
            builder.push_values(ready_backed, |mut b, row| {
                let ready_key = (
                    row.ready_slot,
                    row.ready_generation,
                    row.queue.as_str(),
                    row.priority,
                    row.enqueue_shard,
                    row.lane_seq,
                );
                let ready_payload = ready_payloads.get(&ready_key);
                b.push_bind(row.ready_slot)
                    .push_bind(row.ready_generation)
                    .push_bind(row.job_id)
                    .push_bind(&row.kind)
                    .push_bind(&row.queue)
                    .push_bind(row.state)
                    .push_bind(row.priority)
                    .push_bind(row.attempt)
                    .push_bind(row.run_lease)
                    .push_bind(row.lane_seq)
                    .push_bind(row.enqueue_shard)
                    .push_bind(row.attempted_at)
                    .push_bind(row.finalized_at)
                    .push_bind(terminal_storage_payload(&row.payload, ready_payload));
            });
            builder
                .build()
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
        }

        if !synthetic.is_empty() {
            let mut builder = QueryBuilder::<Postgres>::new(format!(
                "INSERT INTO {schema}.done_entries (ready_slot, ready_generation, job_id, kind, queue, args, state, priority, attempt, run_lease, max_attempts, lane_seq, enqueue_shard, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload) "
            ));
            builder.push_values(synthetic, |mut b, row| {
                let ready_key = (
                    row.ready_slot,
                    row.ready_generation,
                    row.queue.as_str(),
                    row.priority,
                    row.enqueue_shard,
                    row.lane_seq,
                );
                let ready_payload = ready_payloads.get(&ready_key);
                b.push_bind(row.ready_slot)
                    .push_bind(row.ready_generation)
                    .push_bind(row.job_id)
                    .push_bind(&row.kind)
                    .push_bind(&row.queue)
                    .push_bind(&row.args)
                    .push_bind(row.state)
                    .push_bind(row.priority)
                    .push_bind(row.attempt)
                    .push_bind(row.run_lease)
                    .push_bind(row.max_attempts)
                    .push_bind(row.lane_seq)
                    .push_bind(row.enqueue_shard)
                    .push_bind(row.run_at)
                    .push_bind(row.attempted_at)
                    .push_bind(row.finalized_at)
                    .push_bind(row.created_at)
                    .push_bind(&row.unique_key)
                    .push_bind(&row.unique_states)
                    .push_bind(terminal_storage_payload(&row.payload, ready_payload));
            });
            builder
                .build()
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
        }

        // Terminal counts stay exact without mutating the hot live-counter
        // rows on every completion. The hot path appends signed deltas; the
        // maintenance rollup folds them into queue_terminal_live_counts later.
        self.increment_live_terminal_counters_tx(tx, rows).await?;

        Ok(rows.len())
    }

    /// Append positive terminal-count deltas for newly inserted `done_entries`
    /// terminal rows.
    ///
    /// For done-entry terminal rows, the exact invariant is:
    ///
    /// `SUM(queue_terminal_live_counts) + SUM(queue_terminal_count_deltas)
    /// == count(*) FROM done_entries`.
    ///
    /// Compact receipt completions are already append-only batches, so
    /// `queue_counts_exact` counts retained `receipt_completion_batches`
    /// directly instead of adding another hot-path count-delta write.
    ///
    /// Maintenance asynchronously folds delta rows into the live-counter table
    /// and truncates the delta segment. The hot path therefore performs only
    /// append-only writes.
    async fn increment_live_terminal_counters_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[DoneJobRow],
    ) -> Result<(), AwaError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut by_group: BTreeMap<TerminalCounterKey, i64> = BTreeMap::new();
        for row in rows {
            let key = (
                row.ready_slot,
                row.ready_generation,
                row.queue.clone(),
                row.priority,
                row.enqueue_shard,
                terminal_counter_bucket(row.job_id),
            );
            *by_group.entry(key).or_insert(0) += 1;
        }
        self.append_terminal_count_deltas_tx(tx, by_group).await
    }

    /// Append negative terminal-count deltas for deleted `done_entries`
    /// terminal rows.
    ///
    /// This is used by retry-from-terminal, DLQ moves, discard paths, and the
    /// SQL compatibility delete function. The negative row may cancel a
    /// not-yet-rolled positive delta or reduce an already-folded live counter;
    /// exact reads sum both sources, so either order is correct.
    async fn decrement_live_terminal_counters_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[TerminalCounterKey],
    ) -> Result<(), AwaError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut by_group: BTreeMap<TerminalCounterKey, i64> = BTreeMap::new();
        for key in rows {
            *by_group.entry(key.clone()).or_insert(0) -= 1;
        }
        self.append_terminal_count_deltas_tx(tx, by_group).await
    }

    async fn append_terminal_count_deltas_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        by_group: BTreeMap<TerminalCounterKey, i64>,
    ) -> Result<(), AwaError> {
        if by_group.is_empty() {
            return Ok(());
        }

        let mut ready_slots: Vec<i32> = Vec::with_capacity(by_group.len());
        let mut ready_generations: Vec<i64> = Vec::with_capacity(by_group.len());
        let mut queues: Vec<String> = Vec::with_capacity(by_group.len());
        let mut priorities: Vec<i16> = Vec::with_capacity(by_group.len());
        let mut enqueue_shards: Vec<i16> = Vec::with_capacity(by_group.len());
        let mut counter_buckets: Vec<i16> = Vec::with_capacity(by_group.len());
        let mut deltas: Vec<i64> = Vec::with_capacity(by_group.len());
        for ((slot, generation, queue, prio, shard, bucket), delta) in by_group {
            if delta == 0 {
                continue;
            }
            ready_slots.push(slot);
            ready_generations.push(generation);
            queues.push(queue);
            priorities.push(prio);
            enqueue_shards.push(shard);
            counter_buckets.push(bucket);
            deltas.push(delta);
        }

        if deltas.is_empty() {
            return Ok(());
        }

        let schema = self.schema();
        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.queue_terminal_count_deltas (
                ready_slot,
                ready_generation,
                queue,
                priority,
                enqueue_shard,
                counter_bucket,
                terminal_delta
            )
            SELECT
                ready_slot,
                ready_generation,
                queue,
                priority,
                enqueue_shard,
                counter_bucket,
                terminal_delta
            FROM unnest(
                $1::int[],
                $2::bigint[],
                $3::text[],
                $4::smallint[],
                $5::smallint[],
                $6::smallint[],
                $7::bigint[]
            ) AS d(
                ready_slot,
                ready_generation,
                queue,
                priority,
                enqueue_shard,
                counter_bucket,
                terminal_delta
            )
            ORDER BY
                ready_slot,
                ready_generation,
                queue,
                priority,
                enqueue_shard,
                counter_bucket
            "#
        ))
        .bind(&ready_slots)
        .bind(&ready_generations)
        .bind(&queues)
        .bind(&priorities)
        .bind(&enqueue_shards)
        .bind(&counter_buckets)
        .bind(&deltas)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    /// Build the row-key vector for `decrement_live_terminal_counters_tx`
    /// from a slice of `DoneJobRow` (used by retry-from-terminal,
    /// DLQ move, and discard, all of which DELETE FROM done_entries
    /// with `RETURNING *` materialised into `DoneJobRow`).
    fn done_rows_to_counter_keys(rows: &[DoneJobRow]) -> Vec<TerminalCounterKey> {
        rows.iter()
            .map(|row| {
                (
                    row.ready_slot,
                    row.ready_generation,
                    row.queue.clone(),
                    row.priority,
                    row.enqueue_shard,
                    terminal_counter_bucket(row.job_id),
                )
            })
            .collect()
    }

    async fn ensure_terminal_removed_receipt_closures_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[DoneJobRow],
    ) -> Result<(), AwaError> {
        if rows.is_empty() || !self.lease_claim_receipts() {
            return Ok(());
        }

        let receipt_pairs: Vec<(i64, i64)> =
            rows.iter().map(|row| (row.job_id, row.run_lease)).collect();
        self.lock_receipt_attempts_tx(tx, &receipt_pairs).await?;

        let schema = self.schema();
        let job_ids: Vec<i64> = rows.iter().map(|row| row.job_id).collect();
        let run_leases: Vec<i64> = rows.iter().map(|row| row.run_lease).collect();
        sqlx::query(&format!(
            r#"
            WITH refs(job_id, run_lease) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[])
            ),
            inserted AS (
                INSERT INTO {schema}.lease_claim_closures (
                    claim_slot,
                    job_id,
                    run_lease,
                    outcome,
                    closed_at
                )
                SELECT
                    claims.claim_slot,
                    claims.job_id,
                    claims.run_lease,
                    'terminal_removed',
                    clock_timestamp()
                FROM {schema}.lease_claims AS claims
                JOIN refs
                  ON refs.job_id = claims.job_id
                 AND refs.run_lease = claims.run_lease
                ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                RETURNING claim_slot, job_id, run_lease, closed_at
            ),
            marked AS (
                UPDATE {schema}.lease_claims AS claims
                SET closed_at = COALESCE(claims.closed_at, inserted.closed_at)
                FROM inserted
                WHERE claims.claim_slot = inserted.claim_slot
                  AND claims.job_id = inserted.job_id
                  AND claims.run_lease = inserted.run_lease
                RETURNING claims.job_id
            )
            SELECT count(*) FROM marked
            "#
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn ready_payloads_for_done_rows_tx<'a, 'r>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &'r [DoneJobRow],
    ) -> Result<HashMap<(i32, i64, &'r str, i16, i16, i64), serde_json::Value>, AwaError> {
        if rows.is_empty() {
            return Ok(HashMap::new());
        }

        let schema = self.schema();
        let ready_slots: Vec<i32> = rows.iter().map(|row| row.ready_slot).collect();
        let ready_generations: Vec<i64> = rows.iter().map(|row| row.ready_generation).collect();
        let queues: Vec<&str> = rows.iter().map(|row| row.queue.as_str()).collect();
        let priorities: Vec<i16> = rows.iter().map(|row| row.priority).collect();
        let enqueue_shards: Vec<i16> = rows.iter().map(|row| row.enqueue_shard).collect();
        let lane_seqs: Vec<i64> = rows.iter().map(|row| row.lane_seq).collect();

        let payload_rows: Vec<(i32, i64, String, i16, i16, i64, serde_json::Value)> =
            sqlx::query_as(&format!(
                r#"
                WITH refs(ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq) AS (
                    SELECT * FROM unnest($1::int[], $2::bigint[], $3::text[], $4::smallint[], $5::smallint[], $6::bigint[])
                )
                SELECT
                    ready.ready_slot,
                    ready.ready_generation,
                    ready.queue,
                    ready.priority,
                    ready.enqueue_shard,
                    ready.lane_seq,
                    COALESCE(ready.payload, '{{}}'::jsonb) AS payload
                FROM refs
                JOIN {schema}.ready_entries AS ready
                  ON ready.ready_slot = refs.ready_slot
                 AND ready.ready_generation = refs.ready_generation
                 AND ready.queue = refs.queue
                 AND ready.priority = refs.priority
                 AND ready.enqueue_shard = refs.enqueue_shard
                 AND ready.lane_seq = refs.lane_seq
                "#
            ))
            .bind(&ready_slots)
            .bind(&ready_generations)
            .bind(&queues)
            .bind(&priorities)
            .bind(&enqueue_shards)
            .bind(&lane_seqs)
            .fetch_all(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        let mut payload_by_owned_key = HashMap::with_capacity(payload_rows.len());
        for (ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, payload) in
            payload_rows
        {
            payload_by_owned_key.insert(
                (
                    ready_slot,
                    ready_generation,
                    queue,
                    priority,
                    enqueue_shard,
                    lane_seq,
                ),
                payload,
            );
        }

        let mut payloads = HashMap::with_capacity(payload_by_owned_key.len());
        for row in rows {
            if let Some(payload) = payload_by_owned_key.remove(&(
                row.ready_slot,
                row.ready_generation,
                row.queue.clone(),
                row.priority,
                row.enqueue_shard,
                row.lane_seq,
            )) {
                payloads.insert(
                    (
                        row.ready_slot,
                        row.ready_generation,
                        row.queue.as_str(),
                        row.priority,
                        row.enqueue_shard,
                        row.lane_seq,
                    ),
                    payload,
                );
            }
        }
        Ok(payloads)
    }

    async fn insert_dlq_rows_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[DlqJobRow],
        old_state: Option<JobState>,
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        for row in rows {
            self.sync_unique_claim(
                tx,
                row.job_id,
                &row.unique_key,
                row.unique_states.as_deref(),
                old_state,
                Some(JobState::Failed),
            )
            .await?;
        }

        let schema = self.schema();
        let mut builder = QueryBuilder::<Postgres>::new(format!(
            "INSERT INTO {schema}.dlq_entries (job_id, kind, queue, args, state, priority, attempt, run_lease, max_attempts, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload, dlq_reason, dlq_at, original_run_lease) "
        ));
        builder.push_values(rows.iter(), |mut b, row| {
            b.push_bind(row.job_id)
                .push_bind(&row.kind)
                .push_bind(&row.queue)
                .push_bind(&row.args)
                .push_bind(row.state)
                .push_bind(row.priority)
                .push_bind(row.attempt)
                .push_bind(row.run_lease)
                .push_bind(row.max_attempts)
                .push_bind(row.run_at)
                .push_bind(row.attempted_at)
                .push_bind(row.finalized_at)
                .push_bind(row.created_at)
                .push_bind(&row.unique_key)
                .push_bind(&row.unique_states)
                .push_bind(storage_payload(&row.payload))
                .push_bind(&row.dlq_reason)
                .push_bind(row.dlq_at)
                .push_bind(row.original_run_lease);
        });
        builder
            .build()
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        Ok(rows.len())
    }

    /// Upsert pruned terminal counts into the permanent
    /// `queue_terminal_rollups` denormaliser, clamping both columns at
    /// zero. Since #371 this runs only from the horizon-gated fold
    /// ([`Self::fold_terminal_rollup_deltas`]) — the prune transaction
    /// appends delta rows instead, so a pinned MVCC horizon cannot pile
    /// up dead rollup versions. Keys are folded through a `BTreeMap`, so
    /// the upsert always touches (queue, priority) keys in deterministic
    /// order.
    async fn adjust_terminal_rollups_batch<'a, I>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        deltas: I,
    ) -> Result<(), AwaError>
    where
        I: IntoIterator<Item = (String, i16, i64, i64)>,
    {
        let mut grouped: BTreeMap<(String, i16), (i64, i64)> = BTreeMap::new();
        for (queue, priority, pruned_completed_delta, pruned_failed_delta) in deltas {
            if pruned_completed_delta == 0 && pruned_failed_delta == 0 {
                continue;
            }
            let entry = grouped.entry((queue, priority)).or_insert((0_i64, 0_i64));
            entry.0 += pruned_completed_delta;
            entry.1 += pruned_failed_delta;
        }

        if grouped.is_empty() {
            return Ok(());
        }

        let schema = self.schema();
        let mut queues = Vec::with_capacity(grouped.len());
        let mut priorities = Vec::with_capacity(grouped.len());
        let mut pruned_completed_deltas = Vec::with_capacity(grouped.len());
        let mut pruned_failed_deltas = Vec::with_capacity(grouped.len());

        for ((queue, priority), (pruned_completed_delta, pruned_failed_delta)) in grouped {
            queues.push(queue);
            priorities.push(priority);
            pruned_completed_deltas.push(pruned_completed_delta);
            pruned_failed_deltas.push(pruned_failed_delta);
        }

        sqlx::query(&format!(
            r#"
            WITH deltas(queue, priority, pruned_completed_delta, pruned_failed_delta) AS (
                SELECT *
                FROM unnest(
                    $1::text[],
                    $2::smallint[],
                    $3::bigint[],
                    $4::bigint[]
                )
            )
            INSERT INTO {schema}.queue_terminal_rollups AS rollups (
                queue,
                priority,
                pruned_completed_count,
                pruned_failed_count
            )
            SELECT
                deltas.queue,
                deltas.priority,
                deltas.pruned_completed_delta,
                deltas.pruned_failed_delta
            FROM deltas
            ON CONFLICT (queue, priority) DO UPDATE
            SET pruned_completed_count = GREATEST(
                    0,
                    rollups.pruned_completed_count + EXCLUDED.pruned_completed_count
                ),
                pruned_failed_count = GREATEST(
                    0,
                    rollups.pruned_failed_count + EXCLUDED.pruned_failed_count
                )
            "#
        ))
        .bind(&queues)
        .bind(&priorities)
        .bind(&pruned_completed_deltas)
        .bind(&pruned_failed_deltas)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    /// Append pruned terminal counts as delta rows (#371). Runs inside
    /// the prune transaction in place of the pre-#371 rollup upsert;
    /// deltas are folded into `queue_terminal_rollups` by
    /// [`Self::fold_terminal_rollup_deltas`] when the MVCC horizon is
    /// clear.
    async fn append_terminal_rollup_deltas_tx<'a, I>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        deltas: I,
    ) -> Result<(), AwaError>
    where
        I: IntoIterator<Item = (String, i16, i64, i64)>,
    {
        let mut grouped: BTreeMap<(String, i16), (i64, i64)> = BTreeMap::new();
        for (queue, priority, pruned_completed_delta, pruned_failed_delta) in deltas {
            if pruned_completed_delta == 0 && pruned_failed_delta == 0 {
                continue;
            }
            let entry = grouped.entry((queue, priority)).or_insert((0_i64, 0_i64));
            entry.0 += pruned_completed_delta;
            entry.1 += pruned_failed_delta;
        }

        if grouped.is_empty() {
            return Ok(());
        }

        let schema = self.schema();
        let mut queues = Vec::with_capacity(grouped.len());
        let mut priorities = Vec::with_capacity(grouped.len());
        let mut pruned_completed_deltas = Vec::with_capacity(grouped.len());
        let mut pruned_failed_deltas = Vec::with_capacity(grouped.len());

        for ((queue, priority), (pruned_completed_delta, pruned_failed_delta)) in grouped {
            queues.push(queue);
            priorities.push(priority);
            pruned_completed_deltas.push(pruned_completed_delta);
            pruned_failed_deltas.push(pruned_failed_delta);
        }

        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.queue_terminal_rollup_deltas (
                queue,
                priority,
                pruned_completed_delta,
                pruned_failed_delta
            )
            SELECT *
            FROM unnest(
                $1::text[],
                $2::smallint[],
                $3::bigint[],
                $4::bigint[]
            )
            "#
        ))
        .bind(&queues)
        .bind(&priorities)
        .bind(&pruned_completed_deltas)
        .bind(&pruned_failed_deltas)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn enqueue_runtime_rows(
        &self,
        pool: &PgPool,
        rows: Vec<RuntimeReadyRow>,
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let total_rows = self.insert_ready_rows_tx(&mut tx, rows.clone()).await?;

        let queues_to_notify: Vec<String> = rows.iter().map(|row| row.queue.clone()).collect();
        self.notify_queues_tx(&mut tx, queues_to_notify).await?;

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(total_rows)
    }

    pub async fn enqueue_batch(
        &self,
        pool: &PgPool,
        queue: &str,
        priority: i16,
        count: i64,
    ) -> Result<i64, AwaError> {
        if count <= 0 {
            return Ok(0);
        }

        let rows: Vec<_> = (0..count)
            .map(|seq| RuntimeReadyRow {
                kind: "bench_job".to_string(),
                queue: if self.uses_queue_striping() && !self.is_physical_stripe_queue(queue) {
                    self.physical_queue_for_stripe(
                        queue,
                        seq.rem_euclid(self.queue_stripe_count() as i64) as usize,
                    )
                } else {
                    queue.to_string()
                },
                args: serde_json::json!({ "seq": seq }),
                priority,
                attempt: 0,
                run_lease: 0,
                max_attempts: 25,
                run_at: Utc::now(),
                attempted_at: None,
                created_at: Utc::now(),
                unique_key: None,
                unique_states: None,
                payload: RuntimePayload::default().into_json(),
                ordering_key: None,
            })
            .collect();
        self.enqueue_runtime_rows(pool, rows)
            .await
            .map(|count| count as i64)
    }

    pub async fn enqueue_params_batch(
        &self,
        pool: &PgPool,
        jobs: &[InsertParams],
    ) -> Result<usize, AwaError> {
        if jobs.is_empty() {
            return Ok(0);
        }

        let now = Utc::now();
        let mut ready_rows = Vec::new();
        let mut deferred_rows = Vec::new();

        for (idx, job) in jobs.iter().enumerate() {
            let prepared = prepare_row_raw(job.kind.clone(), job.args.clone(), job.opts.clone())?;
            let payload = Self::payload_from_parts(prepared.metadata, prepared.tags, None, None)?;
            let queue =
                self.queue_stripe_for_enqueue(&prepared.queue, &prepared.unique_key, idx as i64);

            let ready_row = RuntimeReadyRow {
                kind: prepared.kind,
                queue: queue.clone(),
                args: prepared.args,
                priority: prepared.priority,
                attempt: 0,
                run_lease: 0,
                max_attempts: prepared.max_attempts,
                run_at: prepared.run_at.unwrap_or(now),
                attempted_at: None,
                created_at: now,
                unique_key: prepared.unique_key,
                unique_states: prepared.unique_states,
                payload: payload.clone(),
                ordering_key: prepared.ordering_key,
            };

            match prepared.state {
                JobState::Available => ready_rows.push(ready_row),
                JobState::Scheduled => deferred_rows.push(DeferredJobRow {
                    job_id: 0,
                    kind: ready_row.kind,
                    queue,
                    args: ready_row.args,
                    state: JobState::Scheduled,
                    priority: ready_row.priority,
                    attempt: ready_row.attempt,
                    run_lease: ready_row.run_lease,
                    max_attempts: ready_row.max_attempts,
                    run_at: ready_row.run_at,
                    attempted_at: ready_row.attempted_at,
                    finalized_at: None,
                    created_at: ready_row.created_at,
                    unique_key: ready_row.unique_key,
                    unique_states: ready_row.unique_states,
                    payload: payload.clone(),
                }),
                other => {
                    return Err(AwaError::Validation(format!(
                        "queue storage does not support initial state {other}"
                    )));
                }
            }
        }

        let queues_to_notify: Vec<String> =
            ready_rows.iter().map(|row| row.queue.clone()).collect();

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let mut total = 0usize;
        if !ready_rows.is_empty() {
            total += self
                .insert_ready_rows_tx(&mut tx, ready_rows.clone())
                .await?;
        }
        if !deferred_rows.is_empty() {
            let ids = self.next_job_ids(&mut tx, deferred_rows.len()).await?;
            let deferred_rows: Vec<_> = deferred_rows
                .into_iter()
                .zip(ids)
                .map(|(row, id)| DeferredJobRow { job_id: id, ..row })
                .collect();
            total += self
                .insert_deferred_rows_tx(&mut tx, deferred_rows, None)
                .await?;
        }

        self.notify_queues_tx(&mut tx, queues_to_notify).await?;

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(total)
    }

    /// Enqueue prepared jobs into queue storage using PostgreSQL COPY.
    ///
    /// This follows the same preparation, queue striping, lane sequencing,
    /// uniqueness, and notification semantics as [`Self::enqueue_params_batch`],
    /// but streams materialized rows directly into `ready_entries` and
    /// `deferred_jobs` instead of building multi-row `INSERT` statements.
    #[tracing::instrument(skip(self, pool, jobs), fields(job.count = jobs.len()), name = "queue_storage.enqueue_params_copy")]
    pub async fn enqueue_params_copy(
        &self,
        pool: &PgPool,
        jobs: &[InsertParams],
    ) -> Result<usize, AwaError> {
        if jobs.is_empty() {
            return Ok(0);
        }

        let now = Utc::now();
        let mut ready_rows = Vec::new();
        let mut deferred_rows = Vec::new();

        for (idx, job) in jobs.iter().enumerate() {
            let prepared = prepare_row_raw(job.kind.clone(), job.args.clone(), job.opts.clone())?;
            let payload = Self::payload_from_parts(prepared.metadata, prepared.tags, None, None)?;
            let queue =
                self.queue_stripe_for_enqueue(&prepared.queue, &prepared.unique_key, idx as i64);

            let ready_row = RuntimeReadyRow {
                kind: prepared.kind,
                queue: queue.clone(),
                args: prepared.args,
                priority: prepared.priority,
                attempt: 0,
                run_lease: 0,
                max_attempts: prepared.max_attempts,
                run_at: prepared.run_at.unwrap_or(now),
                attempted_at: None,
                created_at: now,
                unique_key: prepared.unique_key,
                unique_states: prepared.unique_states,
                payload: payload.clone(),
                ordering_key: prepared.ordering_key,
            };

            match prepared.state {
                JobState::Available => ready_rows.push(ready_row),
                JobState::Scheduled => deferred_rows.push(DeferredJobRow {
                    job_id: 0,
                    kind: ready_row.kind,
                    queue,
                    args: ready_row.args,
                    state: JobState::Scheduled,
                    priority: ready_row.priority,
                    attempt: ready_row.attempt,
                    run_lease: ready_row.run_lease,
                    max_attempts: ready_row.max_attempts,
                    run_at: ready_row.run_at,
                    attempted_at: ready_row.attempted_at,
                    finalized_at: None,
                    created_at: ready_row.created_at,
                    unique_key: ready_row.unique_key,
                    unique_states: ready_row.unique_states,
                    payload: payload.clone(),
                }),
                other => {
                    return Err(AwaError::Validation(format!(
                        "queue storage does not support initial state {other}"
                    )));
                }
            }
        }

        let queues_to_notify: Vec<String> =
            ready_rows.iter().map(|row| row.queue.clone()).collect();

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let mut total = 0usize;
        let job_ids = self
            .next_job_ids(&mut tx, ready_rows.len() + deferred_rows.len())
            .await?;
        let (ready_job_ids, deferred_job_ids) = job_ids.split_at(ready_rows.len());
        if !ready_rows.is_empty() {
            total += self
                .insert_ready_rows_copy_tx(&mut tx, ready_rows, ready_job_ids.to_vec())
                .await?;
        }
        if !deferred_rows.is_empty() {
            let deferred_rows: Vec<_> = deferred_rows
                .into_iter()
                .zip(deferred_job_ids.iter().copied())
                .map(|(row, id)| DeferredJobRow { job_id: id, ..row })
                .collect();
            total += self
                .insert_deferred_rows_copy_tx(&mut tx, deferred_rows)
                .await?;
        }

        self.notify_queues_tx(&mut tx, queues_to_notify).await?;

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(total)
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.claim_batch")]
    pub async fn claim_batch(
        &self,
        pool: &PgPool,
        queue: &str,
        max_batch: i64,
    ) -> Result<Vec<ClaimedEntry>, AwaError> {
        if max_batch <= 0 {
            return Ok(Vec::new());
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let mut claimed_rows = Vec::new();
        let stripe_queues = self.physical_queues_for_logical(queue);
        let start = self.stripe_probe_start(stripe_queues.len());
        for offset in 0..stripe_queues.len() {
            if claimed_rows.len() >= max_batch as usize {
                break;
            }
            let stripe_queue = &stripe_queues[(start + offset) % stripe_queues.len()];
            let remaining = max_batch - claimed_rows.len() as i64;
            claimed_rows.extend(
                self.claim_ready_rows_tx(
                    &mut tx,
                    stripe_queue,
                    remaining,
                    Duration::ZERO,
                    Duration::ZERO,
                )
                .await?,
            );
        }
        let claim_cursor_advances = Self::claim_cursor_advances(&claimed_rows);
        let claimed = claimed_rows
            .into_iter()
            .map(|row| row.claim_ref(self.lease_claim_receipts()))
            .collect();

        tx.commit().await.map_err(map_sqlx_error)?;
        self.advance_claim_cursors(pool, &claim_cursor_advances)
            .await;
        Ok(claimed)
    }

    #[tracing::instrument(skip(self, pool), fields(queue = %queue), name = "queue_storage.claim_runtime_batch")]
    pub async fn claim_runtime_batch(
        &self,
        pool: &PgPool,
        queue: &str,
        max_batch: i64,
        deadline_duration: Duration,
    ) -> Result<Vec<ClaimedRuntimeJob>, AwaError> {
        self.claim_runtime_batch_with_aging(
            pool,
            queue,
            max_batch,
            deadline_duration,
            Duration::ZERO,
        )
        .await
    }

    #[tracing::instrument(skip(self, pool), fields(queue = %queue), name = "queue_storage.claim_runtime_batch_with_aging")]
    pub async fn claim_runtime_batch_with_aging(
        &self,
        pool: &PgPool,
        queue: &str,
        max_batch: i64,
        deadline_duration: Duration,
        aging_interval: Duration,
    ) -> Result<Vec<ClaimedRuntimeJob>, AwaError> {
        if max_batch <= 0 {
            return Ok(Vec::new());
        }

        let stripe_queues = self.physical_queues_for_logical(queue);
        if stripe_queues.len() > 1 {
            let mut claimed = Vec::new();
            let start = self.stripe_probe_start(stripe_queues.len());
            for offset in 0..stripe_queues.len() {
                if claimed.len() >= max_batch as usize {
                    break;
                }
                let stripe_queue = &stripe_queues[(start + offset) % stripe_queues.len()];
                let remaining = max_batch - claimed.len() as i64;
                match self
                    .claim_runtime_batch_with_aging_physical(
                        pool,
                        stripe_queue,
                        remaining,
                        deadline_duration,
                        aging_interval,
                    )
                    .await
                {
                    Ok(stripe_claims) => claimed.extend(stripe_claims),
                    Err(err) if claimed.is_empty() => return Err(err),
                    Err(err) => {
                        tracing::warn!(
                            queue = %queue,
                            stripe_queue = %stripe_queue,
                            claimed = claimed.len(),
                            error = ?err,
                            "returning already-claimed runtime jobs after striped claim error"
                        );
                        break;
                    }
                }
            }
            return Ok(claimed);
        }

        self.claim_runtime_batch_with_aging_physical(
            pool,
            &stripe_queues[0],
            max_batch,
            deadline_duration,
            aging_interval,
        )
        .await
    }

    async fn claim_runtime_batch_with_aging_physical(
        &self,
        pool: &PgPool,
        queue: &str,
        max_batch: i64,
        deadline_duration: Duration,
        aging_interval: Duration,
    ) -> Result<Vec<ClaimedRuntimeJob>, AwaError> {
        if max_batch <= 0 {
            return Ok(Vec::new());
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let mut claimed = Vec::new();
        claimed.extend(
            self.claim_ready_rows_tx(&mut tx, queue, max_batch, deadline_duration, aging_interval)
                .await?,
        );

        for row in &claimed {
            self.sync_unique_claim(
                &mut tx,
                row.job_id,
                &row.unique_key,
                row.unique_states.as_deref(),
                Some(JobState::Available),
                Some(JobState::Running),
            )
            .await?;
        }

        let use_lease_claim_receipts = self.use_lease_claim_receipts_for_runtime(deadline_duration);
        if !use_lease_claim_receipts && deadline_duration.is_zero() {
            // Legacy zero-deadline claims have no heartbeat/deadline rescue
            // timestamp, so a post-commit conversion error would strand the
            // materialized lease indefinitely. Keep the old rollback semantics
            // for that path.
            let converted = claimed
                .iter()
                .cloned()
                .map(|row| row.into_claimed_runtime_job(use_lease_claim_receipts))
                .collect::<Result<Vec<_>, _>>()?;
            let claim_cursor_advances = Self::claim_cursor_advances(&claimed);
            tx.commit().await.map_err(map_sqlx_error)?;
            self.advance_claim_cursors(pool, &claim_cursor_advances)
                .await;
            return Ok(converted);
        }

        let claim_cursor_advances = Self::claim_cursor_advances(&claimed);

        // Release claim locks before doing Rust-side payload conversion; this
        // keeps the hot claim transaction focused on database state changes.
        tx.commit().await.map_err(map_sqlx_error)?;
        self.advance_claim_cursors(pool, &claim_cursor_advances)
            .await;

        claimed
            .into_iter()
            .map(|row| row.into_claimed_runtime_job(use_lease_claim_receipts))
            .collect()
    }

    #[tracing::instrument(skip(self, pool), fields(queue = %queue, instance_id = %instance_id), name = "queue_storage.acquire_queue_claimer")]
    pub async fn acquire_queue_claimer(
        &self,
        pool: &PgPool,
        queue: &str,
        instance_id: Uuid,
        max_claimers: i16,
        lease_ttl: Duration,
        idle_threshold: Duration,
    ) -> Result<Option<QueueClaimerLease>, AwaError> {
        Ok(self
            .acquire_queue_claimer_row(
                pool,
                queue,
                instance_id,
                max_claimers,
                lease_ttl,
                idle_threshold,
            )
            .await?
            .map(QueueClaimerLeaseRow::lease))
    }

    async fn acquire_queue_claimer_row(
        &self,
        pool: &PgPool,
        queue: &str,
        instance_id: Uuid,
        max_claimers: i16,
        lease_ttl: Duration,
        idle_threshold: Duration,
    ) -> Result<Option<QueueClaimerLeaseRow>, AwaError> {
        if max_claimers <= 0 {
            return Ok(None);
        }

        let schema = self.schema();
        let now = Utc::now();
        let expires_at = now
            + TimeDelta::from_std(lease_ttl)
                .map_err(|err| AwaError::Validation(format!("invalid claimer lease ttl: {err}")))?;
        let idle_cutoff = now
            - TimeDelta::from_std(idle_threshold).map_err(|err| {
                AwaError::Validation(format!("invalid claimer idle threshold: {err}"))
            })?;
        let probe_start = if max_claimers > 1 {
            ((instance_id.as_u128() ^ (now.timestamp_millis() as u128)) % (max_claimers as u128))
                as i16
        } else {
            0
        };

        if let Some(owned) = sqlx::query_as::<_, QueueClaimerLeaseRow>(&format!(
            r#"
            SELECT claimer_slot, lease_epoch, last_claimed_at, expires_at
            FROM {schema}.queue_claimer_leases
            WHERE queue = $1
              AND owner_instance_id = $2
              AND expires_at > $3
            ORDER BY claimer_slot
            LIMIT 1
            "#
        ))
        .bind(queue)
        .bind(instance_id)
        .bind(now)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?
        {
            return Ok(Some(owned));
        }

        for offset in 0..max_claimers {
            let slot = (probe_start + offset) % max_claimers;
            if let Some(updated) = sqlx::query_as::<_, QueueClaimerLeaseRow>(&format!(
                r#"
                UPDATE {schema}.queue_claimer_leases
                SET owner_instance_id = $3,
                    lease_epoch = CASE
                        WHEN owner_instance_id = $3 THEN lease_epoch
                        ELSE lease_epoch + 1
                    END,
                    leased_at = $4,
                    last_claimed_at = $4,
                    expires_at = $5
                WHERE queue = $1
                  AND claimer_slot = $2
                  AND (
                        owner_instance_id = $3
                     OR expires_at <= $4
                     OR last_claimed_at <= $6
                  )
                RETURNING claimer_slot, lease_epoch, last_claimed_at, expires_at
                "#
            ))
            .bind(queue)
            .bind(slot)
            .bind(instance_id)
            .bind(now)
            .bind(expires_at)
            .bind(idle_cutoff)
            .fetch_optional(pool)
            .await
            .map_err(map_sqlx_error)?
            {
                return Ok(Some(updated));
            }

            if let Some(inserted) = sqlx::query_as::<_, QueueClaimerLeaseRow>(&format!(
                r#"
                INSERT INTO {schema}.queue_claimer_leases (
                    queue,
                    claimer_slot,
                    owner_instance_id,
                    lease_epoch,
                    leased_at,
                    last_claimed_at,
                    expires_at
                )
                VALUES ($1, $2, $3, 0, $4, $4, $5)
                ON CONFLICT (queue, claimer_slot) DO NOTHING
                RETURNING claimer_slot, lease_epoch, last_claimed_at, expires_at
                "#
            ))
            .bind(queue)
            .bind(slot)
            .bind(instance_id)
            .bind(now)
            .bind(expires_at)
            .fetch_optional(pool)
            .await
            .map_err(map_sqlx_error)?
            {
                return Ok(Some(inserted));
            }
        }

        Ok(None)
    }

    #[tracing::instrument(skip(self, pool), fields(queue = %queue, instance_id = %instance_id, claimer_slot = lease.claimer_slot), name = "queue_storage.mark_queue_claimer_active")]
    pub async fn mark_queue_claimer_active(
        &self,
        pool: &PgPool,
        queue: &str,
        instance_id: Uuid,
        lease: QueueClaimerLease,
        lease_ttl: Duration,
    ) -> Result<bool, AwaError> {
        let schema = self.schema();
        let now = Utc::now();
        let expires_at = now
            + TimeDelta::from_std(lease_ttl)
                .map_err(|err| AwaError::Validation(format!("invalid claimer lease ttl: {err}")))?;

        let result = sqlx::query(&format!(
            r#"
            UPDATE {schema}.queue_claimer_leases
            SET last_claimed_at = $5,
                expires_at = $6
            WHERE queue = $1
              AND claimer_slot = $2
              AND owner_instance_id = $3
              AND lease_epoch = $4
            "#
        ))
        .bind(queue)
        .bind(lease.claimer_slot)
        .bind(instance_id)
        .bind(lease.lease_epoch)
        .bind(now)
        .bind(expires_at)
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(result.rows_affected() == 1)
    }

    fn desired_queue_claimer_target(
        &self,
        current_target: Option<i16>,
        signal: &AvailableSignal,
        max_claimers: i16,
    ) -> i16 {
        // The signal source counts sequence positions reserved for enqueue
        // but not yet claimed. It can over-count deleted or uncommitted
        // positions, but already-claimed rows are excluded by the claim cursor
        // advance.
        // `backlog` is retained as a separate name in the threshold
        // table to keep room for shape tweaks that diverge from
        // `available` later.
        let available = signal.available.max(0) as u64;
        let backlog = available;
        let current = current_target.unwrap_or(1).clamp(1, max_claimers.max(1));
        let max_four = 4.min(max_claimers.max(1));
        let max_two = 2.min(max_claimers.max(1));

        match current {
            4.. => {
                if available >= 32 || backlog >= 16 {
                    max_four
                } else if available >= 8 || backlog >= 4 {
                    max_two
                } else {
                    1
                }
            }
            2..=3 => {
                if available >= 128 || backlog >= 64 {
                    max_four
                } else if available >= 4 || backlog >= 2 {
                    max_two
                } else {
                    1
                }
            }
            _ => {
                if available >= 64 || backlog >= 32 {
                    max_four
                } else if available >= 8 || backlog >= 4 {
                    max_two
                } else {
                    1
                }
            }
        }
    }

    async fn queue_claimer_target(
        &self,
        pool: &PgPool,
        queue: &str,
        max_claimers: i16,
        control_interval: Duration,
    ) -> Result<i16, AwaError> {
        let schema = self.schema();
        let now = Utc::now();
        let stale_cutoff = now
            - TimeDelta::from_std(control_interval).map_err(|err| {
                AwaError::Validation(format!("invalid claimer control interval: {err}"))
            })?;

        if let Some(target) = sqlx::query_scalar::<_, i16>(&format!(
            r#"
            SELECT target_claimers
            FROM {schema}.queue_claimer_state
            WHERE queue = $1
              AND updated_at > $2
            "#
        ))
        .bind(queue)
        .bind(stale_cutoff)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?
        {
            return Ok(target.clamp(1, max_claimers.max(1)));
        }

        let current_target = sqlx::query_scalar::<_, i16>(&format!(
            r#"
            SELECT target_claimers
            FROM {schema}.queue_claimer_state
            WHERE queue = $1
            "#
        ))
        .bind(queue)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;

        let signal = self.queue_claimer_signal(pool, queue).await?;
        let desired = self.desired_queue_claimer_target(current_target, &signal, max_claimers);

        if let Some(updated) = sqlx::query_scalar::<_, i16>(&format!(
            r#"
            INSERT INTO {schema}.queue_claimer_state (queue, target_claimers, updated_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (queue) DO UPDATE
            SET target_claimers = EXCLUDED.target_claimers,
                updated_at = EXCLUDED.updated_at
            WHERE {schema}.queue_claimer_state.updated_at <= $4
            RETURNING target_claimers
            "#
        ))
        .bind(queue)
        .bind(desired)
        .bind(now)
        .bind(stale_cutoff)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?
        {
            return Ok(updated.clamp(1, max_claimers.max(1)));
        }

        Ok(current_target
            .unwrap_or(desired)
            .clamp(1, max_claimers.max(1)))
    }

    /// Cheap, dispatcher-grade available-count signal.
    ///
    /// Sums `enqueue_cursor - claim_cursor` across the queue's
    /// (queue, priority) lanes — one PK read into each of the two head tables per lane
    /// (typically ≤ 4 lanes per logical queue). The difference is an
    /// upper bound on the count of unclaimed ready rows: admin DELETEs
    /// of unclaimed jobs and in-flight enqueue reservations can leave a
    /// gap between `claim_cursor` and `enqueue_cursor`. The dispatcher
    /// tolerates this drift because the worst case is a wasted claim
    /// attempt that finds no committed rows.
    ///
    /// For an exact count, use [`Self::queue_counts_exact`], which
    /// scans `ready_entries`.
    async fn queue_claimer_signal(
        &self,
        pool: &PgPool,
        queue: &str,
    ) -> Result<AvailableSignal, AwaError> {
        let schema = self.schema();
        let queues = self.physical_queues_for_logical(queue);
        let available: i64 = sqlx::query_scalar(&format!(
            r#"
            SELECT COALESCE(
                sum(GREATEST(
                    {schema}.sequence_next_value(qe.seq_name)
                        - {schema}.sequence_next_value(qc.seq_name),
                    0
                )),
                0
            )::bigint
            FROM {schema}.queue_enqueue_heads AS qe
            JOIN {schema}.queue_claim_heads AS qc
              ON qc.queue = qe.queue
             AND qc.priority = qe.priority
             AND qc.enqueue_shard = qe.enqueue_shard
            WHERE qe.queue = ANY($1)
            "#
        ))
        .bind(&queues)
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(AvailableSignal { available })
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip(self, pool), fields(queue = %queue, instance_id = %instance_id), name = "queue_storage.claim_runtime_batch_with_aging_for_instance")]
    pub async fn claim_runtime_batch_with_aging_for_instance(
        &self,
        pool: &PgPool,
        queue: &str,
        max_batch: i64,
        deadline_duration: Duration,
        aging_interval: Duration,
        instance_id: Uuid,
        max_claimers: i16,
        lease_ttl: Duration,
        idle_threshold: Duration,
    ) -> Result<Vec<ClaimedRuntimeJob>, AwaError> {
        let target_claimers = self
            .queue_claimer_target(pool, queue, max_claimers, Duration::from_millis(500))
            .await?;

        let Some(lease) = self
            .acquire_queue_claimer_row(
                pool,
                queue,
                instance_id,
                target_claimers,
                lease_ttl,
                idle_threshold,
            )
            .await?
        else {
            return Ok(Vec::new());
        };

        let claimed = self
            .claim_runtime_batch_with_aging(
                pool,
                queue,
                max_batch,
                deadline_duration,
                aging_interval,
            )
            .await?;

        if !claimed.is_empty() && lease.needs_refresh(Utc::now(), lease_ttl, idle_threshold) {
            let _ = self
                .mark_queue_claimer_active(pool, queue, instance_id, lease.lease(), lease_ttl)
                .await?;
        }

        Ok(claimed)
    }

    #[tracing::instrument(skip(self, pool), fields(queue = %queue), name = "queue_storage.claim_job_batch")]
    pub async fn claim_job_batch(
        &self,
        pool: &PgPool,
        queue: &str,
        max_batch: i64,
        deadline_duration: Duration,
    ) -> Result<Vec<JobRow>, AwaError> {
        self.claim_runtime_batch(pool, queue, max_batch, deadline_duration)
            .await
            .map(|claimed| claimed.into_iter().map(|row| row.job).collect())
    }

    #[tracing::instrument(skip(self, pool, claimed), name = "queue_storage.complete_batch")]
    pub async fn complete_batch(
        &self,
        pool: &PgPool,
        claimed: &[ClaimedEntry],
    ) -> Result<usize, AwaError> {
        self.complete_claimed_batch(pool, claimed)
            .await
            .map(|updated| updated.len())
    }

    #[tracing::instrument(
        skip(self, pool, claimed),
        name = "queue_storage.complete_claimed_batch"
    )]
    pub async fn complete_claimed_batch(
        &self,
        pool: &PgPool,
        claimed: &[ClaimedEntry],
    ) -> Result<Vec<(i64, i64)>, AwaError> {
        if claimed.is_empty() {
            return Ok(Vec::new());
        }

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        let lease_slots: Vec<i32> = claimed.iter().map(|entry| entry.lease_slot).collect();
        let queues: Vec<String> = claimed.iter().map(|entry| entry.queue.clone()).collect();
        let priorities: Vec<i16> = claimed.iter().map(|entry| entry.priority).collect();
        let enqueue_shards: Vec<i16> = claimed.iter().map(|entry| entry.enqueue_shard).collect();
        let lane_seqs: Vec<i64> = claimed.iter().map(|entry| entry.lane_seq).collect();

        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            WITH completed(lease_slot, queue, priority, enqueue_shard, lane_seq) AS (
                SELECT * FROM unnest($1::int[], $2::text[], $3::smallint[], $4::smallint[], $5::bigint[])
            )
            DELETE FROM {schema}.leases AS leases
            USING completed
            WHERE leases.lease_slot = completed.lease_slot
              AND leases.queue = completed.queue
              AND leases.priority = completed.priority
              AND leases.enqueue_shard = completed.enqueue_shard
              AND leases.lane_seq = completed.lane_seq
            RETURNING
                leases.ready_slot,
                leases.ready_generation,
                leases.job_id,
                leases.queue,
                leases.state,
                leases.priority,
                leases.attempt,
                leases.run_lease,
                leases.max_attempts,
                leases.lane_seq,
                leases.enqueue_shard,
                leases.heartbeat_at,
                leases.deadline_at,
                leases.attempted_at,
                leases.callback_id,
                leases.callback_timeout_at
            "#
        ))
        .bind(&lease_slots)
        .bind(&queues)
        .bind(&priorities)
        .bind(&enqueue_shards)
        .bind(&lane_seqs)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if deleted.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let completed_pairs: Vec<(i64, i64)> = deleted
            .iter()
            .map(|row| (row.job_id, row.run_lease))
            .collect();
        self.close_receipt_pairs_tx(&mut tx, &completed_pairs, "completed")
            .await?;

        let moved = self.hydrate_deleted_leases_tx(&mut tx, deleted).await?;

        let finalized_at = Utc::now();
        let mut done_rows = Vec::with_capacity(moved.len());
        for entry in moved.iter().cloned() {
            let mut payload = RuntimePayload::from_json(Self::payload_with_attempt_state(
                entry.payload.clone(),
                entry.progress.clone(),
            )?)?;
            payload.set_progress(None);
            done_rows.push(entry.into_done_row(
                JobState::Completed,
                finalized_at,
                payload.into_json(),
            ));
        }

        self.insert_done_rows_tx(&mut tx, &done_rows, Some(JobState::Running))
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(moved
            .into_iter()
            .map(|entry| (entry.job_id, entry.run_lease))
            .collect())
    }

    fn receipt_fast_complete_candidate(entry: &ClaimedRuntimeJob) -> bool {
        entry.claim.lease_claim_receipt
            && entry.claim.receipt_id.is_some()
            && entry.job.unique_key.is_none()
            && is_compact_receipt_completion_metadata(&entry.job.metadata)
            && entry.job.tags.is_empty()
            && entry.job.errors.as_ref().is_none_or(Vec::is_empty)
    }

    async fn complete_receipt_runtime_batch_fast(
        &self,
        pool: &PgPool,
        claimed: &[ClaimedRuntimeJob],
    ) -> Result<Vec<(i64, i64)>, AwaError> {
        if claimed.is_empty() {
            return Ok(Vec::new());
        }

        let schema = self.schema();
        let finalized_at = Utc::now();
        let job_ids: Vec<i64> = claimed.iter().map(|entry| entry.job.id).collect();
        let run_leases: Vec<i64> = claimed.iter().map(|entry| entry.job.run_lease).collect();
        let mut by_partition: BTreeMap<usize, Vec<&ClaimedRuntimeJob>> = BTreeMap::new();
        for entry in claimed {
            let claim_slot_index =
                ring_slot_index(entry.claim.claim_slot, self.claim_slot_count(), "claim")?;
            by_partition
                .entry(claim_slot_index)
                .or_default()
                .push(entry);
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        // Without the per-job closure-row insert,
        // lease_claim_closure_batches become the idempotence evidence.
        // Serialize duplicate completion attempts before the main statement
        // so a waiter takes its statement snapshot after the first
        // transaction commits and sees that evidence.
        let receipt_pairs: Vec<(i64, i64)> = job_ids
            .iter()
            .zip(run_leases.iter())
            .map(|(job_id, run_lease)| (*job_id, *run_lease))
            .collect();
        if let Err(err) = self.lock_receipt_attempts_tx(&mut tx, &receipt_pairs).await {
            let _ = tx.rollback().await;
            return Err(err);
        }

        let mut rows = Vec::with_capacity(claimed.len());
        for (claim_slot_index, group) in by_partition {
            let claim_rel = claim_child_name(schema, claim_slot_index);
            let claim_batch_rel = claim_batch_child_name(schema, claim_slot_index);
            let closure_rel = closure_child_name(schema, claim_slot_index);
            let closure_batch_rel = claim_closure_batch_child_name(schema, claim_slot_index);
            let receipt_batch_rel = format!("{schema}.receipt_completion_batches");
            let closed_evidence =
                receipt_closed_evidence_sql(schema, &closure_rel, &closure_batch_rel, "claims");

            let claim_slots: Vec<i32> = group.iter().map(|entry| entry.claim.claim_slot).collect();
            let ready_slots: Vec<i32> = group.iter().map(|entry| entry.claim.ready_slot).collect();
            let ready_generations: Vec<i64> = group
                .iter()
                .map(|entry| entry.claim.ready_generation)
                .collect();
            let job_ids: Vec<i64> = group.iter().map(|entry| entry.job.id).collect();
            let queues: Vec<String> = group.iter().map(|entry| entry.job.queue.clone()).collect();
            let priorities: Vec<i16> = group.iter().map(|entry| entry.claim.priority).collect();
            let attempts: Vec<i16> = group.iter().map(|entry| entry.job.attempt).collect();
            let run_leases: Vec<i64> = group.iter().map(|entry| entry.job.run_lease).collect();
            let receipt_ids: Vec<i64> = group
                .iter()
                .map(|entry| {
                    entry
                        .claim
                        .receipt_id
                        .expect("receipt fast completion requires receipt_id")
                })
                .collect();
            let claim_batch_ids: Vec<Option<i64>> = group
                .iter()
                .map(|entry| entry.claim.claim_batch_id)
                .collect();
            let claim_batch_indices: Vec<Option<i32>> = group
                .iter()
                .map(|entry| entry.claim.claim_batch_index)
                .collect();
            let lane_seqs: Vec<i64> = group.iter().map(|entry| entry.claim.lane_seq).collect();
            let enqueue_shards: Vec<i16> = group
                .iter()
                .map(|entry| entry.claim.enqueue_shard)
                .collect();
            let attempted_ats: Vec<Option<DateTime<Utc>>> =
                group.iter().map(|entry| entry.job.attempted_at).collect();
            let finalized_ats: Vec<DateTime<Utc>> = vec![finalized_at; group.len()];

            let completed: Vec<(i64, i64)> = match sqlx::query_as(&format!(
                r#"
                WITH completed(
                    claim_slot,
                    ready_slot,
                    ready_generation,
                    job_id,
                    queue,
                    priority,
                    attempt,
                    run_lease,
                    receipt_id,
                    claim_batch_id,
                    claim_batch_index,
                    lane_seq,
                    enqueue_shard,
                    attempted_at,
                    finalized_at
                ) AS (
                    SELECT *
                    FROM unnest(
                        $1::int[],
                        $2::int[],
                        $3::bigint[],
                        $4::bigint[],
                        $5::text[],
                        $6::smallint[],
                        $7::smallint[],
                        $8::bigint[],
                        $9::bigint[],
                        $10::bigint[],
                        $11::int[],
                        $12::bigint[],
                        $13::smallint[],
                        $14::timestamptz[],
                        $15::timestamptz[]
                    )
                ),
                row_claim_refs AS (
                    SELECT claims.claim_slot, claims.job_id, claims.run_lease, claims.receipt_id
                    FROM {claim_rel} AS claims
                    JOIN completed
                      ON completed.claim_slot = claims.claim_slot
                     AND completed.job_id = claims.job_id
                     AND completed.run_lease = claims.run_lease
                     AND completed.receipt_id = claims.receipt_id
                     AND completed.claim_batch_id IS NULL
                    WHERE NOT {closed_evidence}
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {schema}.leases AS lease
                        WHERE lease.job_id = claims.job_id
                            AND lease.run_lease = claims.run_lease
                      )
                ),
                batch_claim_refs AS (
                    SELECT
                        claim_batches.claim_slot,
                        items.job_id,
                        items.run_lease,
                        items.receipt_id
                    FROM completed
                    JOIN {claim_batch_rel} AS claim_batches
                      ON claim_batches.claim_slot = completed.claim_slot
                     AND claim_batches.batch_id = completed.claim_batch_id
                    CROSS JOIN LATERAL (
                        SELECT
                            claim_batches.job_ids[completed.claim_batch_index] AS job_id,
                            claim_batches.run_leases[completed.claim_batch_index] AS run_lease,
                            claim_batches.receipt_ids[completed.claim_batch_index] AS receipt_id
                    ) AS items
                    WHERE completed.claim_batch_id IS NOT NULL
                      AND completed.claim_batch_index IS NOT NULL
                      AND completed.claim_batch_index BETWEEN 1 AND claim_batches.claimed_count
                      AND completed.job_id = items.job_id
                      AND completed.run_lease = items.run_lease
                      AND completed.receipt_id = items.receipt_id
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {closure_rel} AS closures
                          WHERE closures.claim_slot = claim_batches.claim_slot
                            AND closures.job_id = items.job_id
                            AND closures.run_lease = items.run_lease
                      )
                      -- Compact successful completion treats receipt closure
                      -- evidence as the same-attempt disposition fence. The
                      -- non-success paths write explicit closure rows before
                      -- moving the job to done/deferred/DLQ, so probing those
                      -- terminal ledgers here only adds hot-path work.
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {closure_batch_rel} AS closure_batches
                          WHERE closure_batches.receipt_ranges @> items.receipt_id
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {schema}.leases AS lease
                          WHERE lease.job_id = items.job_id
                            AND lease.run_lease = items.run_lease
                      )
                ),
                claim_refs AS (
                    SELECT claim_slot, job_id, run_lease, receipt_id FROM row_claim_refs
                    UNION ALL
                    SELECT claim_slot, job_id, run_lease, receipt_id FROM batch_claim_refs
                ),
                deleted_attempts AS (
                    DELETE FROM {schema}.attempt_state AS attempt
                    USING claim_refs
                    WHERE attempt.job_id = claim_refs.job_id
                      AND attempt.run_lease = claim_refs.run_lease
                    RETURNING attempt.job_id
                ),
                completed_rows AS (
                    SELECT completed.*
                    FROM completed
                    JOIN claim_refs
                      ON claim_refs.claim_slot = completed.claim_slot
                     AND claim_refs.job_id = completed.job_id
                     AND claim_refs.run_lease = completed.run_lease
                     AND claim_refs.receipt_id = completed.receipt_id
                ),
                claim_closure_batches AS (
                    INSERT INTO {closure_batch_rel} (
                        claim_slot,
                        ready_slot,
                        ready_generation,
                        outcome,
                        closed_count,
                        receipt_ids,
                        receipt_ranges,
                        closed_at
                    )
                    SELECT
                        completed.claim_slot,
                        completed.ready_slot,
                        completed.ready_generation,
                        'completed',
                        count(*)::int AS closed_count,
                        array_agg(completed.receipt_id ORDER BY completed.lane_seq, completed.job_id),
                        range_agg(int8range(completed.receipt_id, completed.receipt_id + 1, '[)') ORDER BY completed.receipt_id),
                        max(completed.finalized_at)
                    FROM completed_rows AS completed
                    GROUP BY
                        completed.claim_slot,
                        completed.ready_slot,
                        completed.ready_generation
                    RETURNING claim_slot, ready_slot, ready_generation, receipt_ids
                ),
                terminal AS (
                    INSERT INTO {receipt_batch_rel} (
                        ready_slot,
                        ready_generation,
                        claim_slot,
                        queue,
                        priority,
                        enqueue_shard,
                        completed_count,
                        job_ids,
                        run_leases,
                        lane_seqs,
                        attempts,
                        attempted_ats,
                        finalized_at
                    )
                    SELECT
                        completed.ready_slot,
                        completed.ready_generation,
                        completed.claim_slot,
                        completed.queue,
                        completed.priority,
                        completed.enqueue_shard,
                        count(*)::int AS completed_count,
                        array_agg(completed.job_id ORDER BY completed.lane_seq, completed.job_id),
                        array_agg(completed.run_lease ORDER BY completed.lane_seq, completed.job_id),
                        array_agg(completed.lane_seq ORDER BY completed.lane_seq, completed.job_id),
                        array_agg(completed.attempt ORDER BY completed.lane_seq, completed.job_id),
                        array_agg(completed.attempted_at ORDER BY completed.lane_seq, completed.job_id),
                        max(completed.finalized_at)
                    FROM completed_rows AS completed
                    GROUP BY
                        completed.ready_slot,
                        completed.ready_generation,
                        completed.claim_slot,
                        completed.queue,
                        completed.priority,
                        completed.enqueue_shard
                    RETURNING
                        ready_slot,
                        ready_generation,
                        claim_slot,
                        queue,
                        priority,
                        enqueue_shard,
                        job_ids,
                        run_leases
                )
                SELECT completed_rows.job_id, completed_rows.run_lease
                FROM completed_rows
                CROSS JOIN (SELECT count(*) FROM claim_closure_batches) AS closure_batch_write
                CROSS JOIN (SELECT count(*) FROM terminal) AS terminal_write
                "#
            ))
            .bind(&claim_slots)
            .bind(&ready_slots)
            .bind(&ready_generations)
            .bind(&job_ids)
            .bind(&queues)
            .bind(&priorities)
            .bind(&attempts)
            .bind(&run_leases)
            .bind(&receipt_ids)
            .bind(&claim_batch_ids)
            .bind(&claim_batch_indices)
            .bind(&lane_seqs)
            .bind(&enqueue_shards)
            .bind(&attempted_ats)
            .bind(&finalized_ats)
            .fetch_all(tx.as_mut())
            .await
            {
                Ok(rows) => rows,
                Err(err) => {
                    let _ = tx.rollback().await;
                    return Err(map_sqlx_error(err));
                }
            };

            rows.extend(completed);
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(rows)
    }

    #[tracing::instrument(
        skip(self, pool, claimed),
        name = "queue_storage.complete_runtime_batch"
    )]
    pub async fn complete_runtime_batch(
        &self,
        pool: &PgPool,
        claimed: &[ClaimedRuntimeJob],
    ) -> Result<Vec<(i64, i64)>, AwaError> {
        if claimed.is_empty() {
            return Ok(Vec::new());
        }

        if self.lease_claim_receipts() && claimed.iter().all(Self::receipt_fast_complete_candidate)
        {
            let mut updated = self
                .complete_receipt_runtime_batch_fast(pool, claimed)
                .await?;
            if updated.len() == claimed.len() {
                return Ok(updated);
            }

            let updated_pairs: BTreeSet<(i64, i64)> = updated.iter().copied().collect();
            let missed: Vec<_> = claimed
                .iter()
                .filter(|entry| !updated_pairs.contains(&(entry.job.id, entry.job.run_lease)))
                .cloned()
                .collect();
            if !missed.is_empty() {
                updated.extend(self.complete_runtime_batch_slow(pool, &missed).await?);
            }
            return Ok(updated);
        }

        self.complete_runtime_batch_slow(pool, claimed).await
    }

    async fn complete_runtime_batch_slow(
        &self,
        pool: &PgPool,
        claimed: &[ClaimedRuntimeJob],
    ) -> Result<Vec<(i64, i64)>, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self
            .complete_runtime_batch_slow_in_tx(&mut tx, claimed)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    /// Same as [`Self::complete_runtime_batch_slow`] but runs on the caller's
    /// transaction so additional writes — for example ADR-029 follow-up job
    /// inserts — can join the same commit. The caller is responsible for
    /// `commit()` / `rollback()`. Handles both receipt-claimed and
    /// materialised leases.
    pub async fn complete_runtime_batch_slow_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        claimed: &[ClaimedRuntimeJob],
    ) -> Result<Vec<(i64, i64)>, AwaError> {
        if claimed.is_empty() {
            return Ok(Vec::new());
        }

        let schema = self.schema();

        let claimed_map: BTreeMap<(i64, i64), ClaimedRuntimeJob> = claimed
            .iter()
            .cloned()
            .map(|entry| ((entry.job.id, entry.job.run_lease), entry))
            .collect();

        if self.lease_claim_receipts() {
            let (mut receipt_claimed, mut materialized_claimed): (Vec<_>, Vec<_>) = claimed
                .iter()
                .cloned()
                .partition(|entry| entry.claim.lease_claim_receipt);
            let mut updated_all = Vec::new();

            if !receipt_claimed.is_empty() {
                let receipt_pairs: Vec<(i64, i64)> = receipt_claimed
                    .iter()
                    .map(|entry| (entry.job.id, entry.job.run_lease))
                    .collect();
                self.lock_receipt_attempts_tx(tx, &receipt_pairs).await?;

                // claim_slot rides along on `ClaimedEntry`, so receipt
                // completion can validate exact claim evidence and route the
                // explicit completed closure without an extra lookup.
                let receipt_claim_slots: Vec<i32> = receipt_claimed
                    .iter()
                    .map(|entry| entry.claim.claim_slot)
                    .collect();
                let receipt_job_ids: Vec<i64> =
                    receipt_claimed.iter().map(|entry| entry.job.id).collect();
                let receipt_run_leases: Vec<i64> = receipt_claimed
                    .iter()
                    .map(|entry| entry.job.run_lease)
                    .collect();
                let receipt_receipt_ids: Vec<i64> = receipt_claimed
                    .iter()
                    .map(|entry| {
                        entry
                            .claim
                            .receipt_id
                            .expect("receipt-backed slow completion requires receipt_id")
                    })
                    .collect();
                let closure_rel = format!("{schema}.lease_claim_closures");
                let closure_batch_rel = format!("{schema}.lease_claim_closure_batches");
                let closed_evidence =
                    receipt_closed_evidence_sql(schema, &closure_rel, &closure_batch_rel, "claims");
                let updated: Vec<(i64, i64)> = sqlx::query_as(&format!(
                    r#"
                    WITH completed(claim_slot, job_id, run_lease, receipt_id) AS (
                        SELECT * FROM unnest($1::int[], $2::bigint[], $3::bigint[], $4::bigint[])
                    ),
                    locked_row_claims AS (
                        SELECT claims.claim_slot, claims.job_id, claims.run_lease
                        FROM {schema}.lease_claims AS claims
                        JOIN completed
                          ON completed.claim_slot = claims.claim_slot
                         AND completed.job_id = claims.job_id
                         AND completed.run_lease = claims.run_lease
                         AND completed.receipt_id = claims.receipt_id
                        WHERE NOT {closed_evidence}
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {schema}.leases AS lease
                              WHERE lease.job_id = claims.job_id
                                AND lease.run_lease = claims.run_lease
                              )
                        FOR UPDATE OF claims
                    ),
                    locked_batch_claims AS (
                        SELECT
                            claim_batches.claim_slot,
                            claim_batches.ready_slot,
                            claim_batches.ready_generation,
                            items.job_id,
                            items.run_lease,
                            items.receipt_id
                        FROM {schema}.lease_claim_batches AS claim_batches
                        CROSS JOIN LATERAL unnest(
                            claim_batches.job_ids,
                            claim_batches.run_leases,
                            claim_batches.receipt_ids
                        ) AS items(job_id, run_lease, receipt_id)
                        JOIN completed
                          ON completed.claim_slot = claim_batches.claim_slot
                         AND completed.job_id = items.job_id
                         AND completed.run_lease = items.run_lease
                         AND completed.receipt_id = items.receipt_id
                        WHERE NOT EXISTS (
                              SELECT 1
                              FROM {schema}.lease_claim_closures AS closures
                              WHERE closures.claim_slot = claim_batches.claim_slot
                                AND closures.job_id = items.job_id
                                AND closures.run_lease = items.run_lease
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {schema}.lease_claim_closure_batches AS closure_batches
                              WHERE closure_batches.claim_slot = claim_batches.claim_slot
                                AND closure_batches.receipt_ranges @> items.receipt_id
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {schema}.leases AS lease
                              WHERE lease.job_id = items.job_id
                                AND lease.run_lease = items.run_lease
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {schema}.done_entries AS done
                              WHERE done.job_id = items.job_id
                                AND done.run_lease = items.run_lease
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {schema}.deferred_jobs AS deferred
                              WHERE deferred.job_id = items.job_id
                                AND deferred.run_lease = items.run_lease
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {schema}.dlq_entries AS dlq
                              WHERE dlq.job_id = items.job_id
                                AND dlq.run_lease = items.run_lease
                          )
                        FOR UPDATE OF claim_batches
                    ),
                    locked_claims AS (
                        SELECT claim_slot, job_id, run_lease FROM locked_row_claims
                        UNION ALL
                        SELECT claim_slot, job_id, run_lease FROM locked_batch_claims
                    ),
                    deleted_attempts AS (
                        DELETE FROM {schema}.attempt_state AS attempt
                        USING locked_claims
                        WHERE attempt.job_id = locked_claims.job_id
                          AND attempt.run_lease = locked_claims.run_lease
                        RETURNING attempt.job_id
                    ),
                    -- Row-sourced (deadline lease) claims close into the
                    -- explicit ledger so the prune gates balance them
                    -- against the lease_claims row via the closure JOIN.
                    closed_claims AS (
                        INSERT INTO {schema}.lease_claim_closures (
                            claim_slot,
                            job_id,
                            run_lease,
                            outcome,
                            closed_at
                        )
                        SELECT
                            locked_row_claims.claim_slot,
                            locked_row_claims.job_id,
                            locked_row_claims.run_lease,
                            'completed',
                            clock_timestamp()
                        FROM locked_row_claims
                        ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                        RETURNING claim_slot, job_id, run_lease, closed_at
                    ),
                    -- Compact batch-sourced claims have no lease_claims row
                    -- to JOIN, so close them into the batch ledger that the
                    -- queue prune gate counts via compact_count.
                    closed_batches AS (
                        INSERT INTO {schema}.lease_claim_closure_batches (
                            claim_slot,
                            ready_slot,
                            ready_generation,
                            outcome,
                            closed_count,
                            receipt_ids,
                            receipt_ranges,
                            closed_at
                        )
                        SELECT
                            locked_batch_claims.claim_slot,
                            locked_batch_claims.ready_slot,
                            locked_batch_claims.ready_generation,
                            'completed',
                            count(*)::int,
                            array_agg(locked_batch_claims.receipt_id ORDER BY locked_batch_claims.receipt_id),
                            range_agg(int8range(locked_batch_claims.receipt_id, locked_batch_claims.receipt_id + 1, '[)') ORDER BY locked_batch_claims.receipt_id),
                            clock_timestamp()
                        FROM locked_batch_claims
                        GROUP BY
                            locked_batch_claims.claim_slot,
                            locked_batch_claims.ready_slot,
                            locked_batch_claims.ready_generation
                        RETURNING claim_slot
                    ),
                    marked_claims AS (
                        UPDATE {schema}.lease_claims AS claims
                        SET closed_at = COALESCE(claims.closed_at, closed_claims.closed_at)
                        FROM closed_claims
                        WHERE claims.claim_slot = closed_claims.claim_slot
                          AND claims.job_id = closed_claims.job_id
                          AND claims.run_lease = closed_claims.run_lease
                        RETURNING claims.job_id
                    ),
                    -- Force the batch insert to run and report the compact
                    -- claims it closed so the caller finalizes them too.
                    closed_batch_pairs AS (
                        SELECT locked_batch_claims.job_id, locked_batch_claims.run_lease
                        FROM locked_batch_claims
                        WHERE EXISTS (SELECT 1 FROM closed_batches)
                    )
                    SELECT job_id, run_lease
                    FROM closed_claims
                    UNION ALL
                    SELECT job_id, run_lease
                    FROM closed_batch_pairs
                    "#
                ))
                .bind(&receipt_claim_slots)
                .bind(&receipt_job_ids)
                .bind(&receipt_run_leases)
                .bind(&receipt_receipt_ids)
                .fetch_all(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                if !updated.is_empty() {
                    let finalized_at = Utc::now();
                    let mut done_rows = Vec::with_capacity(updated.len());
                    for (job_id, run_lease) in &updated {
                        if let Some(runtime_job) = claimed_map.get(&(*job_id, *run_lease)).cloned()
                        {
                            done_rows.push(runtime_job.into_done_row(finalized_at)?);
                        }
                    }

                    self.insert_done_rows_tx(tx, &done_rows, Some(JobState::Running))
                        .await?;
                    updated_all.extend(updated);
                }

                let updated_pairs: BTreeSet<(i64, i64)> = updated_all.iter().copied().collect();
                let mut escalated_receipts = Vec::new();
                for entry in receipt_claimed.drain(..) {
                    if !updated_pairs.contains(&(entry.job.id, entry.job.run_lease)) {
                        escalated_receipts.push(entry);
                    }
                }
                materialized_claimed.extend(escalated_receipts);
            }

            if !materialized_claimed.is_empty() {
                let ready_slots: Vec<i32> = materialized_claimed
                    .iter()
                    .map(|entry| entry.claim.ready_slot)
                    .collect();
                let ready_generations: Vec<i64> = materialized_claimed
                    .iter()
                    .map(|entry| entry.claim.ready_generation)
                    .collect();
                let job_ids: Vec<i64> = materialized_claimed
                    .iter()
                    .map(|entry| entry.job.id)
                    .collect();
                let queues: Vec<String> = materialized_claimed
                    .iter()
                    .map(|entry| entry.claim.queue.clone())
                    .collect();
                let priorities: Vec<i16> = materialized_claimed
                    .iter()
                    .map(|entry| entry.claim.priority)
                    .collect();
                let enqueue_shards: Vec<i16> = materialized_claimed
                    .iter()
                    .map(|entry| entry.claim.enqueue_shard)
                    .collect();
                let lane_seqs: Vec<i64> = materialized_claimed
                    .iter()
                    .map(|entry| entry.claim.lane_seq)
                    .collect();
                let run_leases: Vec<i64> = materialized_claimed
                    .iter()
                    .map(|entry| entry.job.run_lease)
                    .collect();

                // CTE-as-DML: delete the leases and the matching attempt_state
                // rows in one round-trip. Receipt claims may materialize a
                // lease after the original claim, so the materialized lease can
                // live in a newer lease slot than the claim carried. Match on
                // the stable ready-lane and attempt identity instead.
                let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
                    r#"
                    WITH completed(ready_slot, ready_generation, job_id, queue, priority, enqueue_shard, lane_seq, run_lease) AS (
                        SELECT * FROM unnest($1::int[], $2::bigint[], $3::bigint[], $4::text[], $5::smallint[], $6::smallint[], $7::bigint[], $8::bigint[])
                    ),
                    deleted AS (
                        DELETE FROM {schema}.leases AS leases
                        USING completed
                        WHERE leases.ready_slot = completed.ready_slot
                          AND leases.ready_generation = completed.ready_generation
                          AND leases.job_id = completed.job_id
                          AND leases.queue = completed.queue
                          AND leases.priority = completed.priority
                          AND leases.enqueue_shard = completed.enqueue_shard
                          AND leases.lane_seq = completed.lane_seq
                          AND leases.run_lease = completed.run_lease
                        RETURNING
                            leases.ready_slot,
                            leases.ready_generation,
                            leases.job_id,
                            leases.queue,
                            leases.state,
                            leases.priority,
                            leases.attempt,
                            leases.run_lease,
                            leases.max_attempts,
                            leases.lane_seq,
                            leases.enqueue_shard,
                            leases.heartbeat_at,
                            leases.deadline_at,
                            leases.attempted_at,
                            leases.callback_id,
                            leases.callback_timeout_at
                    ),
                    del_attempts AS (
                        DELETE FROM {schema}.attempt_state AS attempt
                        USING deleted
                        WHERE attempt.job_id = deleted.job_id
                          AND attempt.run_lease = deleted.run_lease
                        RETURNING attempt.job_id
                    )
                    SELECT
                        ready_slot,
                        ready_generation,
                        job_id,
                        queue,
                        state,
                        priority,
                        attempt,
                        run_lease,
                        max_attempts,
                        lane_seq,
                        enqueue_shard,
                        heartbeat_at,
                        deadline_at,
                        attempted_at,
                        callback_id,
                        callback_timeout_at
                    FROM deleted
                    "#
                ))
                .bind(&ready_slots)
                .bind(&ready_generations)
                .bind(&job_ids)
                .bind(&queues)
                .bind(&priorities)
                .bind(&enqueue_shards)
                .bind(&lane_seqs)
                .bind(&run_leases)
                .fetch_all(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                if !deleted.is_empty() {
                    let completed_pairs: Vec<(i64, i64)> = deleted
                        .iter()
                        .map(|row| (row.job_id, row.run_lease))
                        .collect();
                    self.close_receipt_pairs_tx(tx, &completed_pairs, "completed")
                        .await?;

                    let finalized_at = Utc::now();
                    let mut done_rows = Vec::with_capacity(deleted.len());
                    for deleted_row in deleted {
                        if let Some(runtime_job) = claimed_map
                            .get(&(deleted_row.job_id, deleted_row.run_lease))
                            .cloned()
                        {
                            done_rows.push(runtime_job.into_done_row(finalized_at)?);
                            updated_all.push((deleted_row.job_id, deleted_row.run_lease));
                        }
                    }

                    self.insert_done_rows_tx(tx, &done_rows, Some(JobState::Running))
                        .await?;
                }
            }

            return Ok(updated_all);
        }

        let lease_slots: Vec<i32> = claimed.iter().map(|entry| entry.claim.lease_slot).collect();
        let queues: Vec<String> = claimed
            .iter()
            .map(|entry| entry.claim.queue.clone())
            .collect();
        let priorities: Vec<i16> = claimed.iter().map(|entry| entry.claim.priority).collect();
        let enqueue_shards: Vec<i16> = claimed
            .iter()
            .map(|entry| entry.claim.enqueue_shard)
            .collect();
        let lane_seqs: Vec<i64> = claimed.iter().map(|entry| entry.claim.lane_seq).collect();
        let run_leases: Vec<i64> = claimed.iter().map(|entry| entry.job.run_lease).collect();

        // Single CTE-as-DML statement: delete the leases and the matching
        // attempt_state rows in one round-trip. The `deleted` CTE materialises
        // the lease deletion (so its RETURNING is observable to the
        // attempt-state delete and to the final SELECT), and `del_attempts`
        // hangs off it. Saves one round-trip per completion batch versus
        // issuing the attempt-state delete as a separate statement.
        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            WITH completed(lease_slot, queue, priority, enqueue_shard, lane_seq, run_lease) AS (
                SELECT * FROM unnest($1::int[], $2::text[], $3::smallint[], $4::smallint[], $5::bigint[], $6::bigint[])
            ),
            deleted AS (
                DELETE FROM {schema}.leases AS leases
                USING completed
                WHERE leases.lease_slot = completed.lease_slot
                  AND leases.queue = completed.queue
                  AND leases.priority = completed.priority
                  AND leases.enqueue_shard = completed.enqueue_shard
                  AND leases.lane_seq = completed.lane_seq
                  AND leases.run_lease = completed.run_lease
                RETURNING
                    leases.ready_slot,
                    leases.ready_generation,
                    leases.job_id,
                    leases.queue,
                    leases.state,
                    leases.priority,
                    leases.attempt,
                    leases.run_lease,
                    leases.max_attempts,
                    leases.lane_seq,
                    leases.enqueue_shard,
                    leases.heartbeat_at,
                    leases.deadline_at,
                    leases.attempted_at,
                    leases.callback_id,
                    leases.callback_timeout_at
            ),
            del_attempts AS (
                DELETE FROM {schema}.attempt_state AS attempt
                USING deleted
                WHERE attempt.job_id = deleted.job_id
                  AND attempt.run_lease = deleted.run_lease
                RETURNING attempt.job_id
            )
            SELECT
                ready_slot,
                ready_generation,
                job_id,
                queue,
                state,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                heartbeat_at,
                deadline_at,
                attempted_at,
                callback_id,
                callback_timeout_at
            FROM deleted
            "#
        ))
        .bind(&lease_slots)
        .bind(&queues)
        .bind(&priorities)
        .bind(&enqueue_shards)
        .bind(&lane_seqs)
        .bind(&run_leases)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if deleted.is_empty() {
            return Ok(Vec::new());
        }

        let completed_pairs: Vec<(i64, i64)> = deleted
            .iter()
            .map(|row| (row.job_id, row.run_lease))
            .collect();
        self.close_receipt_pairs_tx(tx, &completed_pairs, "completed")
            .await?;

        let finalized_at = Utc::now();
        let mut done_rows = Vec::with_capacity(deleted.len());
        let mut updated = Vec::with_capacity(deleted.len());
        for deleted_row in deleted {
            if let Some(runtime_job) = claimed_map
                .get(&(deleted_row.job_id, deleted_row.run_lease))
                .cloned()
            {
                done_rows.push(runtime_job.into_done_row(finalized_at)?);
                updated.push((deleted_row.job_id, deleted_row.run_lease));
            }
        }

        self.insert_done_rows_tx(tx, &done_rows, Some(JobState::Running))
            .await?;
        Ok(updated)
    }

    #[tracing::instrument(
        skip(self, pool, completions),
        name = "queue_storage.complete_job_batch_by_id"
    )]
    pub async fn complete_job_batch_by_id(
        &self,
        pool: &PgPool,
        completions: &[(i64, i64)],
    ) -> Result<Vec<(i64, i64)>, AwaError> {
        if completions.is_empty() {
            return Ok(Vec::new());
        }
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self
            .complete_job_batch_by_id_in_tx(&mut tx, completions)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    /// Same as [`Self::complete_job_batch_by_id`] but runs on the caller's
    /// transaction so additional writes — for example ADR-029 follow-up job
    /// inserts — can join the same commit. The caller is responsible for
    /// `commit()` / `rollback()`.
    pub async fn complete_job_batch_by_id_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        completions: &[(i64, i64)],
    ) -> Result<Vec<(i64, i64)>, AwaError> {
        if completions.is_empty() {
            return Ok(Vec::new());
        }

        let schema = self.schema();

        let job_ids: Vec<i64> = completions.iter().map(|(job_id, _)| *job_id).collect();
        let run_leases: Vec<i64> = completions
            .iter()
            .map(|(_, run_lease)| *run_lease)
            .collect();

        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            WITH completed(job_id, run_lease) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[])
            )
            DELETE FROM {schema}.leases AS leases
            USING completed
            WHERE leases.job_id = completed.job_id
              AND leases.run_lease = completed.run_lease
            RETURNING
                leases.ready_slot,
                leases.ready_generation,
                leases.job_id,
                leases.queue,
                leases.state,
                leases.priority,
                leases.attempt,
                leases.run_lease,
                leases.max_attempts,
                leases.lane_seq,
                leases.enqueue_shard,
                leases.heartbeat_at,
                leases.deadline_at,
                leases.attempted_at,
                leases.callback_id,
                leases.callback_timeout_at
            "#
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if deleted.is_empty() {
            return Ok(Vec::new());
        }

        let completed_pairs: Vec<(i64, i64)> = deleted
            .iter()
            .map(|row| (row.job_id, row.run_lease))
            .collect();
        self.close_receipt_pairs_tx(tx, &completed_pairs, "completed")
            .await?;

        let moved = self.hydrate_deleted_leases_tx(tx, deleted).await?;

        let finalized_at = Utc::now();
        let mut done_rows = Vec::with_capacity(moved.len());
        for entry in moved.iter().cloned() {
            let mut payload = RuntimePayload::from_json(Self::payload_with_attempt_state(
                entry.payload.clone(),
                entry.progress.clone(),
            )?)?;
            payload.set_progress(None);
            done_rows.push(entry.into_done_row(
                JobState::Completed,
                finalized_at,
                payload.into_json(),
            ));
        }

        self.insert_done_rows_tx(tx, &done_rows, Some(JobState::Running))
            .await?;
        Ok(moved
            .into_iter()
            .map(|entry| (entry.job_id, entry.run_lease))
            .collect())
    }

    /// Exact admin/UI-grade queue counts. Scans `ready_entries` rather
    /// than reading the head tables — slower, but unaffected by the
    /// transient gap between sequence reservations and committed,
    /// still-claimable ready rows. Use [`Self::queue_claimer_signal`] for the
    /// dispatcher hot path.
    ///
    /// The live-terminal portion reads retained compact receipt batches
    /// directly. Done-entry terminal rows read from folded
    /// `queue_terminal_live_counts` plus unrolled
    /// `queue_terminal_count_deltas` when [`Self::terminal_counter_trusted`]
    /// returns true, and fall back to a `count(*) FROM terminal_jobs` scan
    /// when not. The fallback exists for the rolling-upgrade window: older
    /// binaries may have written terminal rows without maintaining the
    /// counter/delta contract, so reads stay honest until the operator runs
    /// `awa storage rebuild-terminal-counters`.
    async fn queue_counts_exact(
        &self,
        pool: &PgPool,
        queue: &str,
    ) -> Result<QueueCounts, AwaError> {
        let schema = self.schema();
        let queues = self.physical_queues_for_logical(queue);
        let closure_rel = format!("{schema}.lease_claim_closures");
        let closure_batch_rel = format!("{schema}.lease_claim_closure_batches");
        let closed_evidence =
            receipt_closed_evidence_sql(schema, &closure_rel, &closure_batch_rel, "claims");
        let counter_trusted = self.terminal_counter_trusted(pool).await?;
        // The live-terminal CTE swaps between counter-fed and
        // scan-fed depending on trust. Build it as a string so the
        // outer query plan is otherwise identical between the two
        // paths.
        let live_terminal_cte = if counter_trusted {
            format!(
                "live_terminal AS (
                    SELECT GREATEST(
                        0,
                        COALESCE((
                            SELECT SUM(live_terminal_count)
                            FROM {schema}.queue_terminal_live_counts
                            WHERE queue = ANY($1)
                        ), 0)
                        +
                        COALESCE((
                            SELECT SUM(terminal_delta)
                            FROM {schema}.queue_terminal_count_deltas
                            WHERE queue = ANY($1)
                        ), 0)
                        +
                        COALESCE((
                            SELECT SUM(completed_count)
                            FROM {schema}.receipt_completion_batches
                            WHERE queue = ANY($1)
                        ), 0)
                        -
                        COALESCE((
                            SELECT count(*)::bigint
                            FROM {schema}.receipt_completion_tombstones
                            WHERE queue = ANY($1)
                        ), 0)
                    )::bigint AS terminal
                )"
            )
        } else {
            format!(
                "live_terminal AS (
                    SELECT count(*)::bigint AS terminal
                    FROM {schema}.terminal_jobs
                    WHERE queue = ANY($1)
                )"
            )
        };
        let row: (i64, i64, i64, i64) = sqlx::query_as(&format!(
            r#"
            WITH lane_counts AS (
                -- Exact count: a ready row is available iff its
                -- lane_seq has not yet been passed by the lane's
                -- claim sequence cursor. Each shard within a (queue,
                -- priority) lane carries its own sequence, so the
                -- join matches on shard too — otherwise a ready row
                -- in shard A could be incorrectly compared against
                -- shard B's claim cursor.
                SELECT COALESCE(count(*)::bigint, 0) AS available
                FROM {schema}.ready_entries AS ready
                JOIN {schema}.queue_claim_heads AS claims
                  ON claims.queue = ready.queue
                 AND claims.priority = ready.priority
                 AND claims.enqueue_shard = ready.enqueue_shard
                WHERE ready.queue = ANY($1)
                  AND ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.ready_tombstones AS tomb
                      WHERE tomb.queue = ready.queue
                        AND tomb.priority = ready.priority
                        AND tomb.enqueue_shard = ready.enqueue_shard
                        AND tomb.lane_seq = ready.lane_seq
                        AND tomb.ready_slot = ready.ready_slot
                        AND tomb.ready_generation = ready.ready_generation
                  )
            ),
            pruned_terminal AS (
                -- The GREATEST legacy dedupe applies to the completed
                -- column only: queue_lanes never carried a failed
                -- column, so failed counts come from the rollups alone.
                -- Unfolded prune deltas (#371) are added on top of the
                -- folded rollups so counts stay exact between folds.
                SELECT
                    COALESCE(
                        sum(
                            GREATEST(
                                COALESCE(lanes.pruned_completed_count, 0),
                                COALESCE(rollups.pruned_completed_count, 0)
                            )
                            + COALESCE(pending.pruned_completed_delta, 0)
                        ),
                        0
                    )::bigint AS completed,
                    COALESCE(
                        sum(
                            COALESCE(rollups.pruned_failed_count, 0)
                            + COALESCE(pending.pruned_failed_delta, 0)
                        ),
                        0
                    )::bigint AS failed
                FROM (
                    SELECT queue, priority, pruned_completed_count
                    FROM {schema}.queue_lanes
                    WHERE queue = ANY($1)
                ) AS lanes
                FULL OUTER JOIN (
                    SELECT queue, priority, pruned_completed_count, pruned_failed_count
                    FROM {schema}.queue_terminal_rollups
                    WHERE queue = ANY($1)
                ) AS rollups
                USING (queue, priority)
                FULL OUTER JOIN (
                    SELECT
                        queue,
                        priority,
                        sum(pruned_completed_delta)::bigint AS pruned_completed_delta,
                        sum(pruned_failed_delta)::bigint AS pruned_failed_delta
                    FROM {schema}.queue_terminal_rollup_deltas
                    WHERE queue = ANY($1)
                    GROUP BY queue, priority
                ) AS pending
                USING (queue, priority)
            ),
            live_running AS (
                SELECT (
                    COALESCE((
                        SELECT count(*)::bigint
                        FROM {schema}.leases
                        WHERE queue = ANY($1)
                          AND state = 'running'
                    ), 0)
                    +
                    -- Derive the receipt-backed running count from
                    -- lease_claims anti-joined with every durable
                    -- closure evidence shape.
                    COALESCE((
                        SELECT count(*)::bigint
                        FROM {schema}.lease_claims AS claims
                        WHERE claims.queue = ANY($1)
                          AND NOT {closed_evidence}
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {schema}.leases AS lease
	                              WHERE lease.job_id = claims.job_id
	                                AND lease.run_lease = claims.run_lease
	                          )
	                    ), 0)
	                    +
	                    -- Compact receipt claims live in
	                    -- lease_claim_batches until they complete or a cold
	                    -- path materializes them. Expand only for exact
	                    -- admin-grade counts.
	                    COALESCE((
	                        SELECT count(*)::bigint
	                        FROM {schema}.lease_claim_batches AS batches
	                        CROSS JOIN LATERAL unnest(
	                            batches.job_ids,
	                            batches.run_leases,
	                            batches.receipt_ids
	                        ) AS items(job_id, run_lease, receipt_id)
	                        WHERE batches.queue = ANY($1)
	                          AND NOT EXISTS (
	                              SELECT 1
	                              FROM {schema}.lease_claim_closures AS closures
	                              WHERE closures.claim_slot = batches.claim_slot
	                                AND closures.job_id = items.job_id
	                                AND closures.run_lease = items.run_lease
	                          )
	                          AND NOT EXISTS (
	                              SELECT 1
	                              FROM {schema}.lease_claim_closure_batches AS closure_batches
	                              WHERE closure_batches.claim_slot = batches.claim_slot
	                                AND closure_batches.receipt_ranges @> items.receipt_id
	                          )
	                          AND NOT EXISTS (
	                              SELECT 1
	                              FROM {schema}.leases AS lease
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
	                    ), 0)
	                )::bigint AS running
	            ),
            {live_terminal_cte}
            SELECT
                lane_counts.available,
                live_running.running,
                pruned_terminal.completed + pruned_terminal.failed
                    + live_terminal.terminal AS terminal,
                pruned_terminal.failed AS pruned_failed
            FROM lane_counts
            CROSS JOIN pruned_terminal
            CROSS JOIN live_running
            CROSS JOIN live_terminal
            "#
        ))
        .bind(&queues)
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        let (available, running, terminal, pruned_failed) = row;
        Ok(QueueCounts {
            available,
            running,
            terminal,
            pruned_failed,
        })
    }

    #[tracing::instrument(skip(self, pool), fields(queue = %queue), name = "queue_storage.queue_counts")]
    pub async fn queue_counts(&self, pool: &PgPool, queue: &str) -> Result<QueueCounts, AwaError> {
        self.queue_counts_exact(pool, queue).await
    }

    /// Index-only queue depth probe — for observability / depth-target
    /// throttling. Returns the same shape as [`Self::queue_counts`] but
    /// skips the table scans that [`Self::queue_counts_exact`] needs for
    /// exact terminal accounting:
    ///
    /// - **available** is the same as the dispatcher's claim signal:
    ///   `sum(GREATEST(enqueue_seq - claim_seq, 0))` over the shard
    ///   head tables. No scan of `ready_entries`. This is an upper
    ///   bound: admin DELETEs, committed gaps, and uncommitted enqueue
    ///   reservations can leave the enqueue sequence ahead of the actual
    ///   ready row count. Acceptable for depth-target throttling and
    ///   dashboards; not suitable for exact billing-style counts.
    /// - **running** matches [`Self::queue_counts`]'s strict definition:
    ///   `leases.state = 'running'` only. Receipt-plane claims that
    ///   have not yet materialised a lease row are omitted (the
    ///   exact path catches them via the `lease_claims` anti-join, but
    ///   that anti-join is what this fast variant exists to avoid).
    ///   `waiting_external` is *not* included — it's reported as part
    ///   of admin's parked-callback view, not running.
    /// - **terminal** is read from the persisted
    ///   `queue_terminal_rollups` denormaliser
    ///   (`pruned_completed_count + pruned_failed_count`).
    ///   Rows currently in `done_entries` or
    ///   `receipt_completion_batches` that have not yet rolled up are
    ///   not included. Strictly a lower bound; converges to the exact
    ///   count when rotation prunes the live queue segment. (The name
    ///   `terminal` is honest — this number counts `completed`,
    ///   `failed`, and `cancelled` terminal facts with the same
    ///   semantics as [`Self::queue_counts_exact`]; renamed from
    ///   `completed` in #290.)
    ///
    /// All three counters are O(num shards) lookups against small head
    /// tables and `leases` index probes. Use this for high-cadence
    /// pollers (admin dashboards, depth-target producers, soak
    /// observability); use [`Self::queue_counts`] for admin tooling
    /// that needs the exact terminal count.
    #[tracing::instrument(skip(self, pool), fields(queue = %queue), name = "queue_storage.queue_counts_fast")]
    pub async fn queue_counts_fast(
        &self,
        pool: &PgPool,
        queue: &str,
    ) -> Result<QueueCounts, AwaError> {
        let schema = self.schema();
        let queues = self.physical_queues_for_logical(queue);
        // available: dispatcher signal — already an index-only sum over
        // the (queue, priority, enqueue_shard) head tables.
        let available = self.queue_claimer_signal(pool, queue).await?.available;
        // running: leases.state = 'running' only, matching
        // queue_counts_exact's strict definition. Receipt-plane claims
        // that haven't materialised a lease row yet are documented as
        // omitted in the method-level doc.
        let running: i64 = sqlx::query_scalar(&format!(
            r#"
            SELECT COALESCE(count(*)::bigint, 0)
            FROM {schema}.leases
            WHERE queue = ANY($1)
              AND state = 'running'
            "#
        ))
        .bind(&queues)
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;
        // terminal: denormalised rollup plus unfolded prune deltas
        // (#371). Live (un-rolled-up) done_entries rows are excluded —
        // see method-level docs. The GREATEST legacy dedupe applies to
        // the completed column only: queue_lanes never carried a failed
        // column.
        let (pruned_completed, pruned_failed): (i64, i64) = sqlx::query_as(&format!(
            r#"
            SELECT
                COALESCE(sum(
                    GREATEST(
                        COALESCE(lanes.pruned_completed_count, 0),
                        COALESCE(rollups.pruned_completed_count, 0)
                    )
                    + COALESCE(pending.pruned_completed_delta, 0)
                ), 0)::bigint,
                COALESCE(sum(
                    COALESCE(rollups.pruned_failed_count, 0)
                    + COALESCE(pending.pruned_failed_delta, 0)
                ), 0)::bigint
            FROM (
                SELECT queue, priority, pruned_completed_count
                FROM {schema}.queue_lanes
                WHERE queue = ANY($1)
            ) AS lanes
            FULL OUTER JOIN (
                SELECT queue, priority, pruned_completed_count, pruned_failed_count
                FROM {schema}.queue_terminal_rollups
                WHERE queue = ANY($1)
            ) AS rollups
            USING (queue, priority)
            FULL OUTER JOIN (
                SELECT
                    queue,
                    priority,
                    sum(pruned_completed_delta)::bigint AS pruned_completed_delta,
                    sum(pruned_failed_delta)::bigint AS pruned_failed_delta
                FROM {schema}.queue_terminal_rollup_deltas
                WHERE queue = ANY($1)
                GROUP BY queue, priority
            ) AS pending
            USING (queue, priority)
            "#
        ))
        .bind(&queues)
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(QueueCounts {
            available,
            running,
            terminal: pruned_completed + pruned_failed,
            pruned_failed,
        })
    }

    /// Cumulative count of `failed` terminal rows pruned past the
    /// retention floor for a queue, summed over the queue's
    /// `queue_terminal_rollups` priorities (and stripes, for striped
    /// queues). Monotonic — rollups never decrease. These rows no
    /// longer exist in `done_entries` and cannot be retried.
    pub async fn pruned_failed_count_for_queue(
        &self,
        pool: &PgPool,
        queue: &str,
    ) -> Result<u64, AwaError> {
        let schema = self.schema();
        let queues = self.physical_queues_for_logical(queue);
        // Folded rollups plus unfolded prune deltas (#371), so the count
        // stays exact between maintenance folds.
        let pruned_failed: i64 = sqlx::query_scalar(&format!(
            r#"
            SELECT
                COALESCE((
                    SELECT sum(pruned_failed_count)
                    FROM {schema}.queue_terminal_rollups
                    WHERE queue = ANY($1)
                ), 0)::bigint
                +
                COALESCE((
                    SELECT sum(pruned_failed_delta)
                    FROM {schema}.queue_terminal_rollup_deltas
                    WHERE queue = ANY($1)
                ), 0)::bigint
            "#
        ))
        .bind(&queues)
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(pruned_failed.max(0) as u64)
    }

    async fn retry_job_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
    ) -> Result<Option<JobRow>, AwaError> {
        let schema = self.schema();
        let deleted_waiting: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE job_id = $1
              AND state = 'waiting_external'
            RETURNING
                ready_slot,
                ready_generation,
                job_id,
                queue,
                state,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                heartbeat_at,
                deadline_at,
                attempted_at,
                callback_id,
                callback_timeout_at
            "#
        ))
        .bind(job_id)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if !deleted_waiting.is_empty() {
            let waiting = self
                .hydrate_deleted_leases_tx(tx, deleted_waiting)
                .await?
                .into_iter()
                .next()
                .expect("deleted waiting lease");
            let ready_payload = Self::payload_with_attempt_state(
                waiting.payload.clone(),
                waiting.progress.clone(),
            )?;
            let ready_row = ExistingReadyRow {
                attempt: 0,
                run_at: Utc::now(),
                attempted_at: None,
                ..waiting.clone().into_ready_row(Utc::now(), ready_payload)
            };
            self.insert_existing_ready_rows_tx(tx, vec![ready_row.clone()], Some(waiting.state))
                .await?;
            self.notify_queues_tx(tx, std::iter::once(waiting.queue.clone()))
                .await?;
            return Ok(Some(
                ReadyJobRow {
                    job_id: ready_row.job_id,
                    kind: ready_row.kind,
                    queue: ready_row.queue,
                    args: ready_row.args,
                    priority: ready_row.priority,
                    attempt: ready_row.attempt,
                    run_lease: ready_row.run_lease,
                    max_attempts: ready_row.max_attempts,
                    run_at: ready_row.run_at,
                    attempted_at: ready_row.attempted_at,
                    created_at: ready_row.created_at,
                    unique_key: ready_row.unique_key,
                    payload: ready_row.payload,
                }
                .into_job_row()?,
            ));
        }

        let done_projection = done_row_projection("done", "ready");
        let ready_join = done_ready_join(schema, "done", "ready");
        let terminal: Option<DoneJobRow> = sqlx::query_as(&format!(
            r#"
            WITH deleted AS (
                DELETE FROM {schema}.done_entries
                WHERE (job_id, finalized_at) IN (
                SELECT job_id, finalized_at
                FROM {schema}.done_entries
                WHERE job_id = $1
                  AND state IN ('failed', 'cancelled')
                ORDER BY finalized_at DESC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
                RETURNING *
            )
            SELECT {done_projection}
            FROM deleted AS done
            {ready_join}
            "#
        ))
        .bind(job_id)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if let Some(terminal) = terminal {
            self.ensure_terminal_removed_receipt_closures_tx(tx, std::slice::from_ref(&terminal))
                .await?;
            // The DELETE FROM done_entries above removes one terminal row;
            // append a negative delta so exact counts stay in lockstep.
            self.decrement_live_terminal_counters_tx(
                tx,
                &Self::done_rows_to_counter_keys(std::slice::from_ref(&terminal)),
            )
            .await?;
            let ready_row = ExistingReadyRow {
                job_id: terminal.job_id,
                kind: terminal.kind,
                queue: terminal.queue.clone(),
                args: terminal.args,
                priority: terminal.priority,
                attempt: 0,
                run_lease: terminal.run_lease,
                max_attempts: terminal.max_attempts,
                run_at: Utc::now(),
                attempted_at: None,
                created_at: terminal.created_at,
                unique_key: terminal.unique_key,
                unique_states: terminal.unique_states,
                payload: terminal.payload,
            };
            self.insert_existing_ready_rows_tx(tx, vec![ready_row.clone()], Some(terminal.state))
                .await?;
            self.notify_queues_tx(tx, std::iter::once(terminal.queue.clone()))
                .await?;
            return Ok(Some(
                ReadyJobRow {
                    job_id: ready_row.job_id,
                    kind: ready_row.kind,
                    queue: ready_row.queue,
                    args: ready_row.args,
                    priority: ready_row.priority,
                    attempt: ready_row.attempt,
                    run_lease: ready_row.run_lease,
                    max_attempts: ready_row.max_attempts,
                    run_at: ready_row.run_at,
                    attempted_at: ready_row.attempted_at,
                    created_at: ready_row.created_at,
                    unique_key: ready_row.unique_key,
                    payload: ready_row.payload,
                }
                .into_job_row()?,
            ));
        }

        Ok(None)
    }

    pub async fn retry_job(&self, pool: &PgPool, job_id: i64) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let row = self.retry_job_tx(&mut tx, job_id).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(row)
    }

    /// Retry jobs by id inside a single transaction. Duplicate ids are
    /// collapsed so each job is attempted at most once. Returns the
    /// retried rows together with the number of unique ids attempted:
    /// ids whose terminal row raced to another state or was pruned
    /// between the caller's scan and this call are skipped, so
    /// `attempted - retried.len()` is the count of unique ids that were
    /// requested but not retried.
    pub async fn retry_jobs_by_ids(
        &self,
        pool: &PgPool,
        ids: &[i64],
    ) -> Result<(Vec<JobRow>, u64), AwaError> {
        let unique_ids: BTreeSet<i64> = ids.iter().copied().collect();
        if unique_ids.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let mut rows = Vec::with_capacity(unique_ids.len());
        for job_id in &unique_ids {
            if let Some(row) = self.retry_job_tx(&mut tx, *job_id).await? {
                rows.push(row);
            }
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok((rows, unique_ids.len() as u64))
    }

    /// Write a `<outcome>` closure row for any matching open receipt.
    /// Idempotent: no-op if no row-local claim or compact claim-batch item
    /// exists for the `(job_id, run_lease)` pair, or if a closure already exists. Used
    /// by the admin cancel path to keep the receipt plane consistent
    /// with the job's new terminal state so rescue doesn't revive it.
    ///
    /// `FOR UPDATE` on the inner SELECT serialises the closure write
    /// against `ensure_running_leases_from_receipts_tx`
    /// (which also takes `FOR UPDATE` on the same claim evidence) and
    /// against concurrent rescue / re-close paths that might race the
    /// same `(job_id, run_lease)`. Without it, materialization could
    /// see the claim evidence, decide to materialize, and a concurrent
    /// admin cancel could write the closure between materialization's
    /// SELECT and the lease INSERT — leaving a `running` lease for a
    /// closed claim that admin cancel believes is fully shut down.
    async fn close_receipt_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
        run_lease: i64,
        outcome: &str,
    ) -> Result<(), AwaError> {
        self.close_receipt_pairs_tx(tx, &[(job_id, run_lease)], outcome)
            .await
    }

    async fn close_receipt_pairs_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        pairs: &[(i64, i64)],
        outcome: &str,
    ) -> Result<(), AwaError> {
        if pairs.is_empty() || !self.lease_claim_receipts() {
            return Ok(());
        }

        let unique_pairs: BTreeSet<(i64, i64)> = pairs.iter().copied().collect();
        let unique_jobs: Vec<(i64, i64)> = unique_pairs.iter().copied().collect();
        self.lock_receipt_attempts_tx(tx, &unique_jobs).await?;

        let job_ids: Vec<i64> = unique_pairs.iter().map(|(job_id, _)| *job_id).collect();
        let run_leases: Vec<i64> = unique_pairs
            .iter()
            .map(|(_, run_lease)| *run_lease)
            .collect();
        let schema = self.schema();
        let closure_rel = format!("{schema}.lease_claim_closures");
        let closure_batch_rel = format!("{schema}.lease_claim_closure_batches");
        let closed_evidence =
            receipt_closed_evidence_sql(schema, &closure_rel, &closure_batch_rel, "claims");

        sqlx::query(&format!(
            r#"
            WITH refs(job_id, run_lease) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[])
            ),
            locked_claims AS (
                SELECT claims.claim_slot, claims.job_id, claims.run_lease
                FROM {schema}.lease_claims AS claims
                JOIN refs
                  ON refs.job_id = claims.job_id
                 AND refs.run_lease = claims.run_lease
                WHERE NOT {closed_evidence}
                FOR UPDATE OF claims
            ),
            locked_batch_claims AS (
                SELECT
                    claim_batches.claim_slot,
                    claim_batches.ready_slot,
                    claim_batches.ready_generation,
                    items.job_id,
                    items.run_lease,
                    items.receipt_id
                FROM {schema}.lease_claim_batches AS claim_batches
                CROSS JOIN LATERAL unnest(
                    claim_batches.job_ids,
                    claim_batches.run_leases,
                    claim_batches.receipt_ids
                ) AS items(job_id, run_lease, receipt_id)
                JOIN refs
                  ON refs.job_id = items.job_id
                 AND refs.run_lease = items.run_lease
                WHERE NOT EXISTS (
                      SELECT 1
                      FROM {schema}.lease_claim_closures AS closures
                      WHERE closures.claim_slot = claim_batches.claim_slot
                        AND closures.job_id = items.job_id
                        AND closures.run_lease = items.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.lease_claim_closure_batches AS closure_batches
                      WHERE closure_batches.claim_slot = claim_batches.claim_slot
                        AND closure_batches.receipt_ranges @> items.receipt_id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.done_entries AS done
                      WHERE done.job_id = items.job_id
                        AND done.run_lease = items.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.deferred_jobs AS deferred
                      WHERE deferred.job_id = items.job_id
                        AND deferred.run_lease = items.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.dlq_entries AS dlq
                      WHERE dlq.job_id = items.job_id
                        AND dlq.run_lease = items.run_lease
                  )
                FOR UPDATE OF claim_batches
            ),
            -- Row-sourced (deadline lease) claims keep their explicit
            -- closure: the queue/claim prune gates balance those against
            -- the lease_claims row via the closure JOIN. Compact
            -- batch-sourced claims have no lease_claims row to JOIN, so
            -- they are closed into lease_claim_closure_batches below.
            closing_claims AS (
                SELECT
                    locked_claims.claim_slot,
                    locked_claims.job_id,
                    locked_claims.run_lease,
                    $3 AS outcome,
                    clock_timestamp() AS closed_at
                FROM locked_claims
            ),
            inserted AS (
                INSERT INTO {schema}.lease_claim_closures (
                    claim_slot,
                    job_id,
                    run_lease,
                    outcome,
                    closed_at
                )
                SELECT
                    claim_slot,
                    job_id,
                    run_lease,
                    outcome,
                    closed_at
                FROM closing_claims
                ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                RETURNING claim_slot, job_id, run_lease, closed_at
            ),
            inserted_batches AS (
                INSERT INTO {schema}.lease_claim_closure_batches (
                    claim_slot,
                    ready_slot,
                    ready_generation,
                    outcome,
                    closed_count,
                    receipt_ids,
                    receipt_ranges,
                    closed_at
                )
                SELECT
                    locked_batch_claims.claim_slot,
                    locked_batch_claims.ready_slot,
                    locked_batch_claims.ready_generation,
                    $3,
                    count(*)::int,
                    array_agg(locked_batch_claims.receipt_id ORDER BY locked_batch_claims.receipt_id),
                    range_agg(int8range(locked_batch_claims.receipt_id, locked_batch_claims.receipt_id + 1, '[)') ORDER BY locked_batch_claims.receipt_id),
                    clock_timestamp()
                FROM locked_batch_claims
                GROUP BY
                    locked_batch_claims.claim_slot,
                    locked_batch_claims.ready_slot,
                    locked_batch_claims.ready_generation
                RETURNING claim_slot
            ),
            marked AS (
                UPDATE {schema}.lease_claims AS claims
                SET closed_at = COALESCE(claims.closed_at, inserted.closed_at)
                FROM inserted
                WHERE claims.claim_slot = inserted.claim_slot
                  AND claims.job_id = inserted.job_id
                  AND claims.run_lease = inserted.run_lease
                RETURNING claims.job_id
            )
            SELECT
                (SELECT count(*) FROM marked)
                + (SELECT count(*) FROM inserted_batches)
            "#
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .bind(outcome)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    /// Emit a `pg_notify('awa:cancel', ...)` inside the cancel
    /// transaction so any worker runtime currently executing this
    /// `(job_id, run_lease)` learns about the cancellation on commit
    /// and fires its in-flight cancel flag. Notifications are
    /// automatically discarded on rollback.
    async fn notify_cancellation_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
        run_lease: i64,
    ) -> Result<(), AwaError> {
        let payload = serde_json::json!({ "job_id": job_id, "run_lease": run_lease }).to_string();
        sqlx::query("SELECT pg_notify('awa:cancel', $1)")
            .bind(payload)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn cancel_job_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
    ) -> Result<Option<CancelJobTxResult>, AwaError> {
        let schema = self.schema();
        let ready: Option<ReadyTransitionRow> = sqlx::query_as(&format!(
            r#"
            WITH target AS (
                SELECT ready.*
                FROM {schema}.ready_entries AS ready
                JOIN {schema}.queue_claim_heads AS claims
                  ON claims.queue = ready.queue
                 AND claims.priority = ready.priority
                 AND claims.enqueue_shard = ready.enqueue_shard
                WHERE ready.job_id = $1
                  AND ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.ready_tombstones AS tomb
                      WHERE tomb.queue = ready.queue
                        AND tomb.priority = ready.priority
                        AND tomb.enqueue_shard = ready.enqueue_shard
                        AND tomb.lane_seq = ready.lane_seq
                        AND tomb.ready_slot = ready.ready_slot
                        AND tomb.ready_generation = ready.ready_generation
                  )
                ORDER BY ready.lane_seq DESC
                LIMIT 1
                FOR UPDATE OF ready SKIP LOCKED
            ),
            tombstone AS (
                INSERT INTO {schema}.ready_tombstones (
                    ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
                )
                SELECT ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
                FROM target
                ON CONFLICT DO NOTHING
            )
            SELECT
                ready_slot,
                ready_generation,
                job_id,
                kind,
                queue,
                args,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                run_at,
                attempted_at,
                created_at,
                unique_key,
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload
            FROM target
            "#
        ))
        .bind(job_id)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if let Some(ready) = ready {
            let done =
                ready
                    .clone()
                    .into_done_row(JobState::Cancelled, Utc::now(), ready.payload.clone());
            self.insert_done_rows_tx(tx, std::slice::from_ref(&done), Some(JobState::Available))
                .await?;
            // If the cancelled lane was *exactly* at the claim head,
            // advance the head past it so the derived cursor-difference count
            // drops by 1 immediately.
            // When other unclaimed lanes still sit between claim_seq
            // and the cancelled lane_seq, leave claim_seq alone —
            // advancing would skip past those still-claimable rows.
            // Non-head deletes can leave a bounded dispatcher over-count until
            // later committed rows on the lane are claimed.
            let claim_cursor_advance = ClaimCursorAdvance {
                queue: ready.queue.clone(),
                priority: ready.priority,
                enqueue_shard: ready.enqueue_shard,
                next_seq: ready.lane_seq + 1,
                only_if_current: Some(ready.lane_seq),
            };
            return Ok(Some(CancelJobTxResult {
                row: done.into_job_row()?,
                claim_cursor_advance: Some(claim_cursor_advance),
            }));
        }

        let deleted_lease: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE job_id = $1
              AND state IN ('running', 'waiting_external')
            RETURNING
                ready_slot,
                ready_generation,
                job_id,
                queue,
                state,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                heartbeat_at,
                deadline_at,
                attempted_at,
                callback_id,
                callback_timeout_at
            "#
        ))
        .bind(job_id)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if !deleted_lease.is_empty() {
            let lease = self
                .hydrate_deleted_leases_tx(tx, deleted_lease)
                .await?
                .into_iter()
                .next()
                .expect("deleted running lease");
            let done_payload =
                Self::payload_with_attempt_state(lease.payload.clone(), lease.progress.clone())?;
            let done = lease
                .clone()
                .into_done_row(JobState::Cancelled, Utc::now(), done_payload);
            self.insert_done_rows_tx(tx, std::slice::from_ref(&done), Some(lease.state))
                .await?;
            // Receipt-plane consistency: close any matching open
            // receipt so the ADR-023 anti-join no longer considers this
            // attempt live, and rescue doesn't try to revive it.
            self.close_receipt_tx(tx, lease.job_id, lease.run_lease, "cancelled")
                .await?;
            // Wake any worker currently executing this attempt.
            self.notify_cancellation_tx(tx, lease.job_id, lease.run_lease)
                .await?;
            return Ok(Some(CancelJobTxResult {
                row: done.into_job_row()?,
                claim_cursor_advance: None,
            }));
        }

        // ADR-023 receipt-only cancel: the job may be running on a
        // receipt-backed short path that never materialized a `leases`
        // row. Find it by anti-joining lease_claims with
        // lease_claim_closures, cancel it by writing a closure and a
        // done row, and notify listening workers.
        if self.lease_claim_receipts() {
            let closure_rel = format!("{schema}.lease_claim_closures");
            let closure_batch_rel = format!("{schema}.lease_claim_closure_batches");
            let closed_evidence =
                receipt_closed_evidence_sql(schema, &closure_rel, &closure_batch_rel, "claims");
            type ReceiptCancelRow = (
                i32,
                i64,
                i32,
                i64,
                String,
                i16,
                i16,
                i16,
                i64,
                DateTime<Utc>,
                i64,
                bool,
            );
            let receipt: Option<ReceiptCancelRow> = sqlx::query_as(&format!(
                r#"
                WITH row_receipt AS (
                    SELECT
                        claims.claim_slot,
                        claims.run_lease,
                        claims.ready_slot,
                        claims.ready_generation,
                        claims.queue,
                        claims.priority,
                        claims.attempt,
                        claims.max_attempts,
                        claims.lane_seq,
                        claims.claimed_at,
                        claims.receipt_id,
                        false AS compact_batch
                    FROM {schema}.lease_claims AS claims
                    WHERE claims.job_id = $1
                      AND NOT {closed_evidence}
                    ORDER BY claims.run_lease DESC
                    LIMIT 1
                    FOR UPDATE OF claims SKIP LOCKED
                ),
                batch_receipt AS (
                    SELECT
                        claim_batches.claim_slot,
                        items.run_lease,
                        claim_batches.ready_slot,
                        claim_batches.ready_generation,
                        claim_batches.queue,
                        claim_batches.priority,
                        items.attempt,
                        items.max_attempts,
                        items.lane_seq,
                        claim_batches.claimed_at,
                        items.receipt_id,
                        true AS compact_batch
                    FROM {schema}.lease_claim_batches AS claim_batches
                    CROSS JOIN LATERAL unnest(
                        claim_batches.job_ids,
                        claim_batches.run_leases,
                        claim_batches.receipt_ids,
                        claim_batches.lane_seqs,
                        claim_batches.attempts,
                        claim_batches.max_attempts
                    ) AS items(job_id, run_lease, receipt_id, lane_seq, attempt, max_attempts)
                    WHERE items.job_id = $1
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {schema}.lease_claim_closures AS closures
                          WHERE closures.claim_slot = claim_batches.claim_slot
                            AND closures.job_id = items.job_id
                            AND closures.run_lease = items.run_lease
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {schema}.lease_claim_closure_batches AS closure_batches
                          WHERE closure_batches.claim_slot = claim_batches.claim_slot
                            AND closure_batches.receipt_ranges @> items.receipt_id
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {schema}.leases AS lease
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
                    ORDER BY items.run_lease DESC
                    LIMIT 1
                    FOR UPDATE OF claim_batches SKIP LOCKED
                )
                SELECT *
                FROM (
                    SELECT * FROM row_receipt
                    UNION ALL
                    SELECT * FROM batch_receipt
                ) AS receipt
                ORDER BY run_lease DESC
                LIMIT 1
                "#
            ))
            .bind(job_id)
            .fetch_optional(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            if let Some((
                claim_slot,
                run_lease,
                ready_slot,
                ready_generation,
                queue,
                priority,
                attempt,
                max_attempts,
                lane_seq,
                claimed_at,
                receipt_id,
                compact_batch,
            )) = receipt
            {
                // Hydrate the ready row so we can synthesize the done
                // row with the original args/payload.
                let ready_match: Option<ReadyTransitionRow> = sqlx::query_as(&format!(
                    r#"
                    SELECT
                        ready_slot,
                        ready_generation,
                        job_id,
                        kind,
                        queue,
                        args,
                        priority,
                        attempt,
                        run_lease,
                        max_attempts,
                        lane_seq,
                        enqueue_shard,
                        run_at,
                        attempted_at,
                        created_at,
                        unique_key,
                        unique_states,
                        COALESCE(payload, '{{}}'::jsonb) AS payload
                    FROM {schema}.ready_entries
                    WHERE job_id = $1
                      AND ready_slot = $2
                      AND ready_generation = $3
                      AND queue = $4
                      AND lane_seq = $5
                    "#
                ))
                .bind(job_id)
                .bind(ready_slot)
                .bind(ready_generation)
                .bind(&queue)
                .bind(lane_seq)
                .fetch_optional(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                let Some(ready) = ready_match else {
                    // Shouldn't happen — the claim references a ready
                    // row. Fall through to the deferred / not-found
                    // branches.
                    return Ok(None);
                };

                let done = DoneJobRow {
                    ready_slot,
                    ready_generation,
                    job_id,
                    kind: ready.kind,
                    queue: queue.clone(),
                    args: ready.args,
                    state: JobState::Cancelled,
                    priority,
                    attempt,
                    run_lease,
                    max_attempts,
                    lane_seq,
                    enqueue_shard: ready.enqueue_shard,
                    run_at: ready.run_at,
                    attempted_at: Some(claimed_at),
                    finalized_at: Utc::now(),
                    created_at: ready.created_at,
                    unique_key: ready.unique_key,
                    unique_states: ready.unique_states,
                    payload: ready.payload,
                };
                self.insert_done_rows_tx(tx, std::slice::from_ref(&done), Some(JobState::Running))
                    .await?;
                // Write the closure into the same claim partition. A
                // compact batch-sourced claim has no lease_claims row, so
                // it closes into the batch ledger the queue prune gate
                // counts via compact_count; a deadline lease claim keeps
                // its explicit closure so the gate balances it against the
                // lease_claims row.
                if compact_batch {
                    sqlx::query(&format!(
                        r#"
                        INSERT INTO {schema}.lease_claim_closure_batches (
                            claim_slot,
                            ready_slot,
                            ready_generation,
                            outcome,
                            closed_count,
                            receipt_ids,
                            receipt_ranges,
                            closed_at
                        )
                        VALUES (
                            $1,
                            $2,
                            $3,
                            'cancelled',
                            1,
                            ARRAY[$4::bigint],
                            int8multirange(int8range($4::bigint, $4::bigint + 1, '[)')),
                            clock_timestamp()
                        )
                        "#
                    ))
                    .bind(claim_slot)
                    .bind(ready_slot)
                    .bind(ready_generation)
                    .bind(receipt_id)
                    .execute(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;
                } else {
                    sqlx::query(&format!(
                        r#"
                        WITH inserted AS (
                            INSERT INTO {schema}.lease_claim_closures (claim_slot, job_id, run_lease, outcome, closed_at)
                            VALUES ($1, $2, $3, 'cancelled', clock_timestamp())
                            ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                            RETURNING claim_slot, job_id, run_lease, closed_at
                        ),
                        marked AS (
                            UPDATE {schema}.lease_claims AS claims
                            SET closed_at = COALESCE(claims.closed_at, inserted.closed_at)
                            FROM inserted
                            WHERE claims.claim_slot = inserted.claim_slot
                              AND claims.job_id = inserted.job_id
                              AND claims.run_lease = inserted.run_lease
                            RETURNING claims.job_id
                        )
                        SELECT count(*) FROM marked
                        "#
                    ))
                    .bind(claim_slot)
                    .bind(job_id)
                    .bind(run_lease)
                    .execute(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;
                }
                // Defensive: between the leases DELETE at the top of
                // this function and the FOR UPDATE on claims above, a
                // concurrent `ensure_running_leases_from_receipts_tx`
                // can have materialized a `leases` row for this
                // (job_id, run_lease). Materialize and we both lock the
                // same claim evidence; whichever ran first commits, the
                // other replays under the new snapshot. If materialize
                // committed first, that lease is now an orphan pointing
                // at a job we're about to mark `cancelled`. Sweep it
                // defensively. If no race occurred this is a no-op.
                sqlx::query(&format!(
                    "DELETE FROM {schema}.leases WHERE job_id = $1 AND run_lease = $2"
                ))
                .bind(job_id)
                .bind(run_lease)
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
                self.notify_cancellation_tx(tx, job_id, run_lease).await?;
                return Ok(Some(CancelJobTxResult {
                    row: done.into_job_row()?,
                    claim_cursor_advance: None,
                }));
            }
        }

        let deferred: Option<DeferredJobRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.deferred_jobs
            WHERE job_id = $1
              AND state IN ('scheduled', 'retryable')
            RETURNING
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
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload
            "#
        ))
        .bind(job_id)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if let Some(deferred) = deferred {
            let (ready_slot, ready_generation) = self.current_queue_ring(tx).await?;
            // A deferred-cancel never observed a claim, so it has no
            // shard assignment to inherit. The synthesized terminal row
            // is parked on shard 0 with a synthetic negative `lane_seq`
            // that keeps the `done_entries` PK
            // `(ready_slot, queue, priority, enqueue_shard, lane_seq)`
            // unique. `ensure_lane` registers shard 0 for the queue so
            // the lane row exists regardless of producer activity.
            self.ensure_lane(tx, &deferred.queue, deferred.priority, 0)
                .await?;
            let done = DoneJobRow {
                ready_slot,
                ready_generation,
                job_id: deferred.job_id,
                kind: deferred.kind,
                queue: deferred.queue.clone(),
                args: deferred.args,
                state: JobState::Cancelled,
                priority: deferred.priority,
                attempt: deferred.attempt,
                run_lease: deferred.run_lease,
                max_attempts: deferred.max_attempts,
                lane_seq: -deferred.job_id,
                enqueue_shard: 0,
                run_at: deferred.run_at,
                attempted_at: deferred.attempted_at,
                finalized_at: Utc::now(),
                created_at: deferred.created_at,
                unique_key: deferred.unique_key,
                unique_states: deferred.unique_states,
                payload: deferred.payload,
            };
            self.insert_done_rows_tx(tx, std::slice::from_ref(&done), Some(deferred.state))
                .await?;
            return Ok(Some(CancelJobTxResult {
                row: done.into_job_row()?,
                claim_cursor_advance: None,
            }));
        }

        Ok(None)
    }

    pub async fn cancel_job(&self, pool: &PgPool, job_id: i64) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self.cancel_job_tx(&mut tx, job_id).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        if let Some(result) = result {
            if let Some(advance) = result.claim_cursor_advance.as_ref() {
                self.advance_claim_cursors(pool, std::slice::from_ref(advance))
                    .await;
            }
            Ok(Some(result.row))
        } else {
            Ok(None)
        }
    }

    /// Transaction-scoped cancel used by [`crate::admin::cancel_tx`]. Runs the
    /// full cancellation on the caller's transaction and returns the cancelled
    /// row. Unlike [`Self::cancel_job`] this does NOT perform the post-commit
    /// claim-cursor advance, so the derived queue depth may briefly over-count
    /// by one until later committed rows on the lane are claimed.
    pub(crate) async fn cancel_job_in_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
    ) -> Result<Option<JobRow>, AwaError> {
        Ok(self
            .cancel_job_tx(tx, job_id)
            .await?
            .map(|result| result.row))
    }

    pub async fn cancel_jobs_by_ids(
        &self,
        pool: &PgPool,
        ids: &[i64],
    ) -> Result<Vec<JobRow>, AwaError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let mut rows = Vec::with_capacity(ids.len());
        let mut claim_cursor_advances = Vec::new();
        for job_id in ids {
            if let Some(result) = self.cancel_job_tx(&mut tx, *job_id).await? {
                if let Some(advance) = result.claim_cursor_advance {
                    claim_cursor_advances.push(advance);
                }
                rows.push(result.row);
            }
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        self.advance_claim_cursors(pool, &claim_cursor_advances)
            .await;
        Ok(rows)
    }

    pub async fn set_priority(
        &self,
        pool: &PgPool,
        job_id: i64,
        priority: i16,
    ) -> Result<bool, AwaError> {
        if !(1..=4).contains(&priority) {
            return Err(AwaError::Validation(
                "priority must be between 1 and 4".to_string(),
            ));
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self.set_priority_tx(&mut tx, job_id, priority).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    pub async fn set_priority_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
        priority: i16,
    ) -> Result<bool, AwaError> {
        if !(1..=4).contains(&priority) {
            return Err(AwaError::Validation(
                "priority must be between 1 and 4".to_string(),
            ));
        }

        if self
            .update_deferred_batch_fields_tx(tx, job_id, None, Some(priority))
            .await?
        {
            return Ok(true);
        }

        let result = self
            .move_ready_batch_fields_tx(tx, job_id, None, Some(priority))
            .await?;
        Ok(result.moved)
    }

    pub async fn move_queue(
        &self,
        pool: &PgPool,
        job_id: i64,
        queue: &str,
        priority: Option<i16>,
    ) -> Result<bool, AwaError> {
        if queue.is_empty() || queue.len() > 200 {
            return Err(AwaError::Validation(
                "destination queue must be 1..=200 characters".to_string(),
            ));
        }
        if let Some(priority) = priority {
            if !(1..=4).contains(&priority) {
                return Err(AwaError::Validation(
                    "priority must be between 1 and 4".to_string(),
                ));
            }
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self.move_queue_tx(&mut tx, job_id, queue, priority).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    pub async fn move_queue_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
        queue: &str,
        priority: Option<i16>,
    ) -> Result<bool, AwaError> {
        if queue.is_empty() || queue.len() > 200 {
            return Err(AwaError::Validation(
                "destination queue must be 1..=200 characters".to_string(),
            ));
        }
        if let Some(priority) = priority {
            if !(1..=4).contains(&priority) {
                return Err(AwaError::Validation(
                    "priority must be between 1 and 4".to_string(),
                ));
            }
        }

        if self
            .update_deferred_batch_fields_tx(tx, job_id, Some(queue), priority)
            .await?
        {
            return Ok(true);
        }

        let result = self
            .move_ready_batch_fields_tx(tx, job_id, Some(queue), priority)
            .await?;
        Ok(result.moved)
    }

    async fn update_deferred_batch_fields_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
        queue: Option<&str>,
        priority: Option<i16>,
    ) -> Result<bool, AwaError> {
        let schema = self.schema();
        let row: Option<DeferredJobRow> = sqlx::query_as(&format!(
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
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload
            FROM {schema}.deferred_jobs
            WHERE job_id = $1
              AND state = 'scheduled'
            FOR UPDATE SKIP LOCKED
            "#
        ))
        .bind(job_id)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(row) = row else {
            return Ok(false);
        };

        let old_queue = row.queue.clone();
        let old_priority = row.priority;
        let requested_queue = queue.unwrap_or(&old_queue);
        let old_logical_queue = self.logical_queue_name(&old_queue).to_string();
        let new_queue = if queue.is_some()
            && requested_queue != old_queue
            && requested_queue != old_logical_queue
        {
            self.queue_stripe_for_enqueue(requested_queue, &row.unique_key, row.job_id)
        } else {
            old_queue.clone()
        };
        let new_priority = priority.unwrap_or(old_priority);
        if new_queue == old_queue && new_priority == old_priority {
            return Ok(false);
        }
        let mut payload = RuntimePayload::from_json(row.payload)?;
        let metadata = payload.metadata.as_object_mut().ok_or_else(|| {
            AwaError::Validation("queue storage payload metadata must be a JSON object".to_string())
        })?;
        if queue.is_some() {
            metadata
                .entry("_awa_original_queue".to_string())
                .or_insert_with(|| serde_json::Value::from(old_logical_queue));
        }
        if priority.is_some() {
            metadata
                .entry("_awa_original_priority".to_string())
                .or_insert_with(|| serde_json::Value::from(i64::from(old_priority)));
        }

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.deferred_jobs
            SET queue = $2,
                priority = $3,
                payload = $4
            WHERE job_id = $1
            "#
        ))
        .bind(job_id)
        .bind(new_queue)
        .bind(new_priority)
        .bind(storage_payload(&payload.into_json()))
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(true)
    }

    async fn move_ready_batch_fields_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
        queue: Option<&str>,
        priority: Option<i16>,
    ) -> Result<ReadyBatchMoveResult, AwaError> {
        let schema = self.schema();
        let ready: Option<ReadyTransitionRow> = sqlx::query_as(&format!(
            r#"
            WITH target AS (
                SELECT ready.*
                FROM {schema}.ready_entries AS ready
                JOIN {schema}.queue_claim_heads AS claims
                  ON claims.queue = ready.queue
                 AND claims.priority = ready.priority
                 AND claims.enqueue_shard = ready.enqueue_shard
                WHERE ready.job_id = $1
                  AND ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.ready_tombstones AS tomb
                      WHERE tomb.queue = ready.queue
                        AND tomb.priority = ready.priority
                        AND tomb.enqueue_shard = ready.enqueue_shard
                        AND tomb.lane_seq = ready.lane_seq
                        AND tomb.ready_slot = ready.ready_slot
                        AND tomb.ready_generation = ready.ready_generation
                  )
                ORDER BY ready.lane_seq DESC
                LIMIT 1
                FOR UPDATE OF ready SKIP LOCKED
            )
            SELECT
                ready_slot,
                ready_generation,
                job_id,
                kind,
                queue,
                args,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                run_at,
                attempted_at,
                created_at,
                unique_key,
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload
            FROM target
            "#
        ))
        .bind(job_id)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(ready) = ready else {
            return Ok(ReadyBatchMoveResult { moved: false });
        };

        let old_queue = ready.queue.clone();
        let old_priority = ready.priority;
        let requested_queue = queue.unwrap_or(&old_queue);
        let old_logical_queue = self.logical_queue_name(&old_queue).to_string();
        let new_queue = if queue.is_some()
            && requested_queue != old_queue
            && requested_queue != old_logical_queue
        {
            self.queue_stripe_for_enqueue(requested_queue, &ready.unique_key, ready.job_id)
        } else {
            old_queue.clone()
        };
        let new_priority = priority.unwrap_or(old_priority);
        if new_queue == old_queue && new_priority == old_priority {
            return Ok(ReadyBatchMoveResult { moved: false });
        }
        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.ready_tombstones (
                ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING
            "#
        ))
        .bind(ready.ready_slot)
        .bind(ready.ready_generation)
        .bind(&ready.queue)
        .bind(ready.priority)
        .bind(ready.enqueue_shard)
        .bind(ready.lane_seq)
        .bind(ready.job_id)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let mut payload = RuntimePayload::from_json(ready.payload.clone())?;
        let metadata = payload.metadata.as_object_mut().ok_or_else(|| {
            AwaError::Validation("queue storage payload metadata must be a JSON object".to_string())
        })?;
        if queue.is_some() {
            metadata
                .entry("_awa_original_queue".to_string())
                .or_insert_with(|| serde_json::Value::from(old_logical_queue));
        }
        if priority.is_some() {
            metadata
                .entry("_awa_original_priority".to_string())
                .or_insert_with(|| serde_json::Value::from(i64::from(old_priority)));
        }

        let notify_queue = new_queue.clone();
        let ready_row = ready.into_existing_ready_row(new_queue, new_priority, payload.into_json());
        self.insert_existing_ready_rows_tx(tx, vec![ready_row], Some(JobState::Available))
            .await?;
        self.notify_queues_tx(tx, std::iter::once(notify_queue))
            .await?;

        Ok(ReadyBatchMoveResult { moved: true })
    }

    pub async fn age_waiting_priorities(
        &self,
        pool: &PgPool,
        aging_interval: Duration,
        limit: i64,
    ) -> Result<Vec<i64>, AwaError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }

        let cutoff = Utc::now()
            - TimeDelta::from_std(aging_interval)
                .map_err(|err| AwaError::Validation(format!("invalid aging interval: {err}")))?;
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        let moved: Vec<ReadyTransitionRow> = sqlx::query_as(&format!(
            r#"
            WITH target AS (
                SELECT ready.*
                FROM {schema}.ready_entries AS ready
                JOIN {schema}.queue_claim_heads AS claims
                  ON claims.queue = ready.queue
                 AND claims.priority = ready.priority
                 AND claims.enqueue_shard = ready.enqueue_shard
                WHERE ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
                  AND ready.priority > 1
                  AND ready.run_at <= $1
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.ready_tombstones AS tomb
                      WHERE tomb.queue = ready.queue
                        AND tomb.priority = ready.priority
                        AND tomb.enqueue_shard = ready.enqueue_shard
                        AND tomb.lane_seq = ready.lane_seq
                        AND tomb.ready_slot = ready.ready_slot
                        AND tomb.ready_generation = ready.ready_generation
                  )
                ORDER BY ready.run_at ASC, ready.lane_seq ASC
                LIMIT $2
                FOR UPDATE OF ready SKIP LOCKED
            ),
            tombstones AS (
                INSERT INTO {schema}.ready_tombstones (
                    ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
                )
                SELECT ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
                FROM target
                ON CONFLICT DO NOTHING
            )
            SELECT
                ready_slot,
                ready_generation,
                job_id,
                kind,
                queue,
                args,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                run_at,
                attempted_at,
                created_at,
                unique_key,
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload
            FROM target
            "#
        ))
        .bind(cutoff)
        .bind(limit)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if moved.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let mut ids = Vec::with_capacity(moved.len());
        let mut queues = BTreeSet::new();
        let mut ready_rows = Vec::with_capacity(moved.len());

        for row in moved {
            ids.push(row.job_id);
            queues.insert(row.queue.clone());

            let mut payload = RuntimePayload::from_json(row.payload)?;
            let metadata = payload.metadata.as_object_mut().ok_or_else(|| {
                AwaError::Validation(
                    "queue storage payload metadata must be a JSON object".to_string(),
                )
            })?;
            metadata
                .entry("_awa_original_priority".to_string())
                .or_insert_with(|| serde_json::Value::from(i64::from(row.priority)));

            ready_rows.push(ExistingReadyRow {
                job_id: row.job_id,
                kind: row.kind,
                queue: row.queue,
                args: row.args,
                priority: row.priority - 1,
                attempt: row.attempt,
                run_lease: row.run_lease,
                max_attempts: row.max_attempts,
                run_at: row.run_at,
                attempted_at: row.attempted_at,
                created_at: row.created_at,
                unique_key: row.unique_key,
                unique_states: row.unique_states,
                payload: payload.into_json(),
            });
        }

        // Aging tombstones the source lane without moving its claim cursor,
        // then re-inserts on the target lane. The source lane can temporarily
        // over-count by `moved`; later claims advance over the tombstones.
        // The drift is bounded by aging rate × poll interval.
        self.insert_existing_ready_rows_tx(&mut tx, ready_rows, Some(JobState::Available))
            .await?;
        self.notify_queues_tx(&mut tx, queues).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(ids)
    }

    fn with_progress(
        payload: serde_json::Value,
        progress: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, AwaError> {
        let mut payload = RuntimePayload::from_json(payload)?;
        payload.set_progress(progress);
        Ok(payload.into_json())
    }

    async fn take_callback_result(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
    ) -> Result<serde_json::Value, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let mut row: Option<AttemptStateRow> = sqlx::query_as(&format!(
            r#"
            SELECT
                job_id,
                run_lease,
                progress,
                callback_filter,
                callback_on_complete,
                callback_on_fail,
                callback_transform,
                callback_result
            FROM {}
            WHERE job_id = $1
              AND run_lease = $2
            FOR UPDATE
            "#,
            self.attempt_state_table()
        ))
        .bind(job_id)
        .bind(run_lease)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(mut row) = row.take() else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(serde_json::Value::Null);
        };

        let result = row
            .callback_result
            .take()
            .unwrap_or(serde_json::Value::Null);

        if row.progress.is_none()
            && row.callback_filter.is_none()
            && row.callback_on_complete.is_none()
            && row.callback_on_fail.is_none()
            && row.callback_transform.is_none()
        {
            sqlx::query(&format!(
                "DELETE FROM {} WHERE job_id = $1 AND run_lease = $2",
                self.attempt_state_table()
            ))
            .bind(job_id)
            .bind(run_lease)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        } else {
            sqlx::query(&format!(
                "UPDATE {} SET callback_result = NULL, updated_at = clock_timestamp() WHERE job_id = $1 AND run_lease = $2",
                self.attempt_state_table()
            ))
            .bind(job_id)
            .bind(run_lease)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    async fn backoff_at_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        attempt: i16,
        max_attempts: i16,
    ) -> Result<DateTime<Utc>, AwaError> {
        sqlx::query_scalar("SELECT clock_timestamp() + awa.backoff_duration($1, $2)")
            .bind(attempt)
            .bind(max_attempts)
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)
    }

    async fn notify_queues_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        queues: impl IntoIterator<Item = String>,
    ) -> Result<(), AwaError> {
        // BTreeSet dedups so we never emit the same NOTIFY twice per
        // transaction. Multi-queue enqueues fold into a single round-trip via
        // `unnest($1::text[])` rather than one round-trip per channel.
        let channels: Vec<String> = queues
            .into_iter()
            .map(|queue| format!("awa:{}", self.logical_queue_name(&queue)))
            .collect::<BTreeSet<String>>()
            .into_iter()
            .collect();
        if channels.is_empty() {
            return Ok(());
        }
        sqlx::query("SELECT pg_notify(channel, '') FROM unnest($1::text[]) AS channel")
            .bind(&channels)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn lock_receipt_attempts_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        jobs: &[(i64, i64)],
    ) -> Result<(), AwaError> {
        if jobs.is_empty() {
            return Ok(());
        }

        let unique_jobs: BTreeSet<(i64, i64)> = jobs.iter().copied().collect();
        let job_ids: Vec<i64> = unique_jobs.iter().map(|(job_id, _)| *job_id).collect();
        let run_leases: Vec<i64> = unique_jobs
            .iter()
            .map(|(_, run_lease)| *run_lease)
            .collect();

        sqlx::query(
            r#"
            WITH locks AS MATERIALIZED (
                SELECT DISTINCT pg_catalog.hashtextextended(
                    format('awa.receipt.complete:%s:%s', job_id, run_lease),
                    0
                ) AS lock_key
                FROM unnest($1::bigint[], $2::bigint[]) AS input(job_id, run_lease)
                ORDER BY lock_key
            )
            SELECT pg_catalog.pg_advisory_xact_lock(lock_key)
            FROM locks
            "#,
        )
        .bind(&job_ids)
        .bind(&run_leases)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn ensure_running_leases_from_receipts_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        jobs: &[(i64, i64)],
    ) -> Result<usize, AwaError> {
        if jobs.is_empty() {
            return Ok(0);
        }

        self.lock_receipt_attempts_tx(tx, jobs).await?;

        let schema = self.schema();
        let closure_rel = format!("{schema}.lease_claim_closures");
        let closure_batch_rel = format!("{schema}.lease_claim_closure_batches");
        let closed_evidence =
            receipt_closed_evidence_sql(schema, &closure_rel, &closure_batch_rel, "claims");
        let job_ids: Vec<i64> = jobs.iter().map(|(job_id, _)| *job_id).collect();
        let run_leases: Vec<i64> = jobs.iter().map(|(_, run_lease)| *run_lease).collect();
        let inserted: i64 = sqlx::query_scalar(&format!(
            r#"
            WITH inflight(job_id, run_lease) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[])
            ),
            -- #371: MATERIALIZED pins the max-generation ledger read to a
            -- single evaluation. Without it PG18 inlines this one-reference
            -- CTE into the `CROSS JOIN lease_ring` below, and the planner
            -- re-runs the `ORDER BY generation DESC LIMIT 1` index descent
            -- once per materialized claim row (loops=N). The pre-ledger
            -- `lease_ring_state` singleton was a single cached heap read; the
            -- ledger descent cost scales with unfolded ledger depth, so the
            -- per-row re-execution regressed the completion hot path.
            lease_ring AS MATERIALIZED (
                SELECT slot AS lease_slot, generation AS lease_generation
                FROM {schema}.lease_ring_rotations
                ORDER BY generation DESC
                LIMIT 1
            ),
            row_claim_refs AS (
                -- Source claim metadata directly from the partitioned
                -- lease_claims table anti-joined against every durable
                -- closure evidence shape.
                SELECT
                    claims.claim_slot,
                    claims.job_id,
                    claims.run_lease,
                    claims.ready_slot,
                    claims.ready_generation,
                    claims.queue,
                    claims.priority,
                    claims.attempt,
                    claims.max_attempts,
                    claims.lane_seq,
                    claims.enqueue_shard,
                    claims.claimed_at,
                    claims.deadline_at
                FROM {schema}.lease_claims AS claims
                JOIN inflight
                  ON inflight.job_id = claims.job_id
                 AND inflight.run_lease = claims.run_lease
                WHERE NOT {closed_evidence}
                FOR UPDATE OF claims
            ),
            batch_claim_refs AS (
                SELECT
                    claim_batches.claim_slot,
                    items.job_id,
                    items.run_lease,
                    claim_batches.ready_slot,
                    claim_batches.ready_generation,
                    claim_batches.queue,
                    claim_batches.priority,
                    items.attempt,
                    items.max_attempts,
                    items.lane_seq,
                    claim_batches.enqueue_shard,
                    claim_batches.claimed_at,
                    claim_batches.deadline_at
                FROM {schema}.lease_claim_batches AS claim_batches
                CROSS JOIN LATERAL unnest(
                    claim_batches.job_ids,
                    claim_batches.run_leases,
                    claim_batches.receipt_ids,
                    claim_batches.lane_seqs,
                    claim_batches.attempts,
                    claim_batches.max_attempts
                ) AS items(job_id, run_lease, receipt_id, lane_seq, attempt, max_attempts)
                JOIN inflight
                  ON inflight.job_id = items.job_id
                 AND inflight.run_lease = items.run_lease
                WHERE NOT EXISTS (
                      SELECT 1
                      FROM {schema}.lease_claim_closures AS closures
                      WHERE closures.claim_slot = claim_batches.claim_slot
                        AND closures.job_id = items.job_id
                        AND closures.run_lease = items.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.lease_claim_closure_batches AS closure_batches
                      WHERE closure_batches.claim_slot = claim_batches.claim_slot
                        AND closure_batches.receipt_ranges @> items.receipt_id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.done_entries AS done
                      WHERE done.job_id = items.job_id
                        AND done.run_lease = items.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.deferred_jobs AS deferred
                      WHERE deferred.job_id = items.job_id
                        AND deferred.run_lease = items.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.dlq_entries AS dlq
                      WHERE dlq.job_id = items.job_id
                        AND dlq.run_lease = items.run_lease
                  )
                FOR UPDATE OF claim_batches
            ),
            claim_refs AS (
                SELECT * FROM row_claim_refs
                UNION ALL
                SELECT * FROM batch_claim_refs
            ),
            already_live AS (
                SELECT claim_refs.job_id, claim_refs.run_lease
                FROM claim_refs
                WHERE EXISTS (
                    SELECT 1
                    FROM {schema}.leases AS lease
                    WHERE lease.job_id = claim_refs.job_id
                      AND lease.run_lease = claim_refs.run_lease
                )
            ),
            inserted AS (
                INSERT INTO {schema}.leases (
                    lease_slot,
                    lease_generation,
                    ready_slot,
                    ready_generation,
                    job_id,
                    queue,
                    state,
                    priority,
                    attempt,
                    run_lease,
                    max_attempts,
                    lane_seq,
                    enqueue_shard,
                    heartbeat_at,
                    deadline_at,
                    attempted_at
                )
                SELECT
                    lease_ring.lease_slot,
                    lease_ring.lease_generation,
                    claim_refs.ready_slot,
                    claim_refs.ready_generation,
                    claim_refs.job_id,
                    claim_refs.queue,
                    'running'::awa.job_state,
                    claim_refs.priority,
                    claim_refs.attempt,
                    claim_refs.run_lease,
                    claim_refs.max_attempts,
                    claim_refs.lane_seq,
                    claim_refs.enqueue_shard,
                    clock_timestamp(),
                    -- Preserve the per-claim deadline so the lease-side
                    -- deadline rescue path picks up materialized claims
                    -- without an extra hop. NULL when receipts mode is
                    -- on with `deadline_duration = 0` (the short-job
                    -- shape that needs no deadline at all).
                    claim_refs.deadline_at,
                    claim_refs.claimed_at
                FROM claim_refs
                CROSS JOIN lease_ring
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {schema}.leases AS lease
                    WHERE lease.job_id = claim_refs.job_id
                      AND lease.run_lease = claim_refs.run_lease
                )
                RETURNING job_id, run_lease
            ),
            marked AS (
                UPDATE {schema}.lease_claims AS claims
                SET materialized_at = clock_timestamp()
                FROM (
                    SELECT job_id, run_lease FROM inserted
                    UNION
                    SELECT job_id, run_lease FROM already_live
                ) AS moved
                WHERE claims.job_id = moved.job_id
                  AND claims.run_lease = moved.run_lease
                RETURNING claims.job_id
            )
            SELECT count(*)::bigint
            FROM (
                SELECT job_id, run_lease FROM inserted
                UNION
                SELECT job_id, run_lease FROM already_live
            ) AS moved
            "#
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(inserted as usize)
    }

    async fn ensure_mutable_running_attempt_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
        run_lease: i64,
    ) -> Result<(), AwaError> {
        if self.lease_claim_receipts() {
            self.ensure_running_leases_from_receipts_tx(tx, &[(job_id, run_lease)])
                .await?;
        }
        Ok(())
    }

    async fn upsert_attempt_state_from_receipts_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        jobs: &[(i64, i64)],
    ) -> Result<usize, AwaError> {
        if jobs.is_empty() {
            return Ok(0);
        }

        self.lock_receipt_attempts_tx(tx, jobs).await?;

        let schema = self.schema();
        let closure_rel = format!("{schema}.lease_claim_closures");
        let closure_batch_rel = format!("{schema}.lease_claim_closure_batches");
        let closed_evidence =
            receipt_closed_evidence_sql(schema, &closure_rel, &closure_batch_rel, "claims");
        let job_ids: Vec<i64> = jobs.iter().map(|(job_id, _)| *job_id).collect();
        let run_leases: Vec<i64> = jobs.iter().map(|(_, run_lease)| *run_lease).collect();
        let updated: i64 = sqlx::query_scalar(&format!(
            r#"
            WITH inflight(job_id, run_lease) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[])
            ),
            row_claim_refs AS (
                -- Source open-claim identity from lease_claims
                -- anti-joined against every durable closure evidence
                -- shape.
                SELECT claims.job_id, claims.run_lease
                FROM {schema}.lease_claims AS claims
                JOIN inflight
                  ON inflight.job_id = claims.job_id
                 AND inflight.run_lease = claims.run_lease
                WHERE NOT {closed_evidence}
                FOR UPDATE OF claims
            ),
            batch_claim_refs AS (
                SELECT items.job_id, items.run_lease
                FROM {schema}.lease_claim_batches AS claim_batches
                CROSS JOIN LATERAL unnest(
                    claim_batches.job_ids,
                    claim_batches.run_leases,
                    claim_batches.receipt_ids
                ) AS items(job_id, run_lease, receipt_id)
                JOIN inflight
                  ON inflight.job_id = items.job_id
                 AND inflight.run_lease = items.run_lease
                WHERE NOT EXISTS (
                      SELECT 1
                      FROM {schema}.lease_claim_closures AS closures
                      WHERE closures.claim_slot = claim_batches.claim_slot
                        AND closures.job_id = items.job_id
                        AND closures.run_lease = items.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.lease_claim_closure_batches AS closure_batches
                      WHERE closure_batches.claim_slot = claim_batches.claim_slot
                        AND closure_batches.receipt_ranges @> items.receipt_id
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
                FOR UPDATE OF claim_batches
            ),
            claim_refs AS (
                SELECT * FROM row_claim_refs
                UNION ALL
                SELECT * FROM batch_claim_refs
            ),
            upserted AS (
                INSERT INTO {schema}.attempt_state (job_id, run_lease, heartbeat_at, updated_at)
                SELECT claim_refs.job_id, claim_refs.run_lease, clock_timestamp(), clock_timestamp()
                FROM claim_refs
                ON CONFLICT (job_id, run_lease)
                DO UPDATE SET
                    heartbeat_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                RETURNING job_id, run_lease
            ),
            marked AS (
                UPDATE {schema}.lease_claims AS claims
                SET materialized_at = COALESCE(claims.materialized_at, clock_timestamp())
                FROM row_claim_refs
                WHERE claims.job_id = row_claim_refs.job_id
                  AND claims.run_lease = row_claim_refs.run_lease
                RETURNING claims.job_id
            )
            SELECT count(*)::bigint FROM upserted
            "#
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(updated as usize)
    }

    async fn upsert_attempt_state_progress_from_receipts_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        jobs: &[(i64, i64, serde_json::Value)],
    ) -> Result<usize, AwaError> {
        if jobs.is_empty() {
            return Ok(0);
        }

        let receipt_jobs: Vec<(i64, i64)> = jobs
            .iter()
            .map(|(job_id, run_lease, _)| (*job_id, *run_lease))
            .collect();
        self.lock_receipt_attempts_tx(tx, &receipt_jobs).await?;

        let schema = self.schema();
        let closure_rel = format!("{schema}.lease_claim_closures");
        let closure_batch_rel = format!("{schema}.lease_claim_closure_batches");
        let closed_evidence =
            receipt_closed_evidence_sql(schema, &closure_rel, &closure_batch_rel, "claims");
        let job_ids: Vec<i64> = jobs.iter().map(|(job_id, _, _)| *job_id).collect();
        let run_leases: Vec<i64> = jobs.iter().map(|(_, run_lease, _)| *run_lease).collect();
        let progress: Vec<serde_json::Value> = jobs
            .iter()
            .map(|(_, _, progress)| progress.clone())
            .collect();
        let updated: i64 = sqlx::query_scalar(&format!(
            r#"
            WITH inflight(job_id, run_lease, progress) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::jsonb[])
            ),
            row_claim_refs AS (
                -- Same anti-join pattern as the heartbeat-only path
                -- above.
                SELECT claims.job_id, claims.run_lease, inflight.progress
                FROM {schema}.lease_claims AS claims
                JOIN inflight
                  ON inflight.job_id = claims.job_id
                 AND inflight.run_lease = claims.run_lease
                WHERE NOT {closed_evidence}
                FOR UPDATE OF claims
            ),
            batch_claim_refs AS (
                SELECT items.job_id, items.run_lease, inflight.progress
                FROM {schema}.lease_claim_batches AS claim_batches
                CROSS JOIN LATERAL unnest(
                    claim_batches.job_ids,
                    claim_batches.run_leases,
                    claim_batches.receipt_ids
                ) AS items(job_id, run_lease, receipt_id)
                JOIN inflight
                  ON inflight.job_id = items.job_id
                 AND inflight.run_lease = items.run_lease
                WHERE NOT EXISTS (
                      SELECT 1
                      FROM {schema}.lease_claim_closures AS closures
                      WHERE closures.claim_slot = claim_batches.claim_slot
                        AND closures.job_id = items.job_id
                        AND closures.run_lease = items.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.lease_claim_closure_batches AS closure_batches
                      WHERE closure_batches.claim_slot = claim_batches.claim_slot
                        AND closure_batches.receipt_ranges @> items.receipt_id
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
                FOR UPDATE OF claim_batches
            ),
            claim_refs AS (
                SELECT * FROM row_claim_refs
                UNION ALL
                SELECT * FROM batch_claim_refs
            ),
            upserted AS (
                INSERT INTO {schema}.attempt_state (
                    job_id,
                    run_lease,
                    heartbeat_at,
                    progress,
                    updated_at
                )
                SELECT
                    claim_refs.job_id,
                    claim_refs.run_lease,
                    clock_timestamp(),
                    claim_refs.progress,
                    clock_timestamp()
                FROM claim_refs
                ON CONFLICT (job_id, run_lease)
                DO UPDATE SET
                    heartbeat_at = clock_timestamp(),
                    progress = EXCLUDED.progress,
                    updated_at = clock_timestamp()
                RETURNING job_id, run_lease
            ),
            marked AS (
                UPDATE {schema}.lease_claims AS claims
                SET materialized_at = COALESCE(claims.materialized_at, clock_timestamp())
                FROM row_claim_refs
                WHERE claims.job_id = row_claim_refs.job_id
                  AND claims.run_lease = row_claim_refs.run_lease
                RETURNING claims.job_id
            )
            SELECT count(*)::bigint FROM upserted
            "#
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .bind(&progress)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(updated as usize)
    }

    async fn hydrate_deleted_leases_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        deleted: Vec<DeletedLeaseRow>,
    ) -> Result<Vec<LeaseTransitionRow>, AwaError> {
        if deleted.is_empty() {
            return Ok(Vec::new());
        }

        let schema = self.schema();
        let ready_slots: Vec<i32> = deleted.iter().map(|row| row.ready_slot).collect();
        let ready_generations: Vec<i64> = deleted.iter().map(|row| row.ready_generation).collect();
        let queues: Vec<String> = deleted.iter().map(|row| row.queue.clone()).collect();
        let enqueue_shards: Vec<i16> = deleted.iter().map(|row| row.enqueue_shard).collect();
        let lane_seqs: Vec<i64> = deleted.iter().map(|row| row.lane_seq).collect();
        let job_ids: Vec<i64> = deleted.iter().map(|row| row.job_id).collect();
        let run_leases: Vec<i64> = deleted.iter().map(|row| row.run_lease).collect();

        let ready_rows: Vec<ReadySnapshotRow> = sqlx::query_as(&format!(
            r#"
            WITH refs(ready_slot, ready_generation, queue, enqueue_shard, lane_seq, job_id) AS (
                SELECT * FROM unnest($1::int[], $2::bigint[], $3::text[], $4::smallint[], $5::bigint[], $6::bigint[])
            )
            SELECT
                ready.ready_slot,
                ready.ready_generation,
                ready.job_id,
                ready.kind,
                ready.queue,
                ready.args,
                ready.lane_seq,
                ready.enqueue_shard,
                ready.run_at,
                ready.created_at,
                ready.unique_key,
                ready.unique_states,
                COALESCE(ready.payload, '{{}}'::jsonb) AS payload
            FROM refs
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = refs.ready_slot
             AND ready.ready_generation = refs.ready_generation
             AND ready.queue = refs.queue
             AND ready.enqueue_shard = refs.enqueue_shard
             AND ready.lane_seq = refs.lane_seq
             AND ready.job_id = refs.job_id
            "#
        ))
        .bind(&ready_slots)
        .bind(&ready_generations)
        .bind(&queues)
        .bind(&enqueue_shards)
        .bind(&lane_seqs)
        .bind(&job_ids)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let attempt_rows: Vec<AttemptStateRow> = sqlx::query_as(&format!(
            r#"
            WITH refs(job_id, run_lease) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[])
            )
            DELETE FROM {schema}.attempt_state AS attempt
            USING refs
            WHERE attempt.job_id = refs.job_id
              AND attempt.run_lease = refs.run_lease
            RETURNING
                attempt.job_id,
                attempt.run_lease,
                attempt.progress,
                attempt.callback_filter,
                attempt.callback_on_complete,
                attempt.callback_on_fail,
                attempt.callback_transform,
                attempt.callback_result
            "#
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        // Hydrate runs as part of every rescue path that DELETE'd
        // a leases row (heartbeat / deadline / callback timeout
        // rescue, plus admin cancel of running attempts). For
        // receipt-backed attempts those leases came from
        // `ensure_running_leases_from_receipts_tx`, which leaves a
        // `lease_claims` row behind with `materialized_at` set. The
        // rescue itself closes the lease but never wrote a closure
        // for the original receipt, so the claim sat "open" until
        // partition prune — `load_job` and any
        // `lease_claims`-aware count then double-counted the
        // attempt as `running` even after it had moved to
        // retryable / failed / completed. Write the closure here so
        // the receipt plane mirrors the lease plane: when the lease
        // is gone, the receipt is gone too.
        sqlx::query(&format!(
            r#"
            WITH refs(job_id, run_lease) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[])
            ),
            row_claims AS (
                SELECT claims.claim_slot, claims.job_id, claims.run_lease,
                       'rescue', clock_timestamp()
                FROM {schema}.lease_claims AS claims
                JOIN refs
                  ON refs.job_id = claims.job_id
                 AND refs.run_lease = claims.run_lease
            ),
            -- Compact batch-sourced claims (including those materialized
            -- into `leases` without a lease_claims row) have no row to
            -- JOIN, so they close into the batch ledger the queue prune
            -- gate counts via compact_count. The double-close guards
            -- below keep a re-hydrated rescued receipt from writing a
            -- second batch closure.
            batch_claims AS (
                SELECT
                    claim_batches.claim_slot,
                    claim_batches.ready_slot,
                    claim_batches.ready_generation,
                    items.job_id,
                    items.run_lease,
                    items.receipt_id
                FROM {schema}.lease_claim_batches AS claim_batches
                CROSS JOIN LATERAL unnest(
                    claim_batches.job_ids,
                    claim_batches.run_leases,
                    claim_batches.receipt_ids
                ) AS items(job_id, run_lease, receipt_id)
                JOIN refs
                  ON refs.job_id = items.job_id
                 AND refs.run_lease = items.run_lease
                WHERE NOT EXISTS (
                      SELECT 1
                      FROM {schema}.lease_claim_closures AS closures
                      WHERE closures.claim_slot = claim_batches.claim_slot
                        AND closures.job_id = items.job_id
                        AND closures.run_lease = items.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.lease_claim_closure_batches AS closure_batches
                      WHERE closure_batches.claim_slot = claim_batches.claim_slot
                        AND closure_batches.receipt_ranges @> items.receipt_id
                  )
            ),
            inserted AS (
                INSERT INTO {schema}.lease_claim_closures
                    (claim_slot, job_id, run_lease, outcome, closed_at)
                SELECT * FROM row_claims
                ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                RETURNING claim_slot, job_id, run_lease, closed_at
            ),
            inserted_batches AS (
                INSERT INTO {schema}.lease_claim_closure_batches (
                    claim_slot,
                    ready_slot,
                    ready_generation,
                    outcome,
                    closed_count,
                    receipt_ids,
                    receipt_ranges,
                    closed_at
                )
                SELECT
                    batch_claims.claim_slot,
                    batch_claims.ready_slot,
                    batch_claims.ready_generation,
                    'rescue',
                    count(*)::int,
                    array_agg(batch_claims.receipt_id ORDER BY batch_claims.receipt_id),
                    range_agg(int8range(batch_claims.receipt_id, batch_claims.receipt_id + 1, '[)') ORDER BY batch_claims.receipt_id),
                    clock_timestamp()
                FROM batch_claims
                GROUP BY
                    batch_claims.claim_slot,
                    batch_claims.ready_slot,
                    batch_claims.ready_generation
                RETURNING claim_slot
            ),
            marked AS (
                UPDATE {schema}.lease_claims AS claims
                SET closed_at = COALESCE(claims.closed_at, inserted.closed_at)
                FROM inserted
                WHERE claims.claim_slot = inserted.claim_slot
                  AND claims.job_id = inserted.job_id
                  AND claims.run_lease = inserted.run_lease
                RETURNING claims.job_id
            )
            SELECT
                (SELECT count(*) FROM marked)
                + (SELECT count(*) FROM inserted_batches)
            "#
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let ready_map: BTreeMap<(i32, i64, String, i16, i64, i64), ReadySnapshotRow> = ready_rows
            .into_iter()
            .map(|row| {
                (
                    (
                        row.ready_slot,
                        row.ready_generation,
                        row.queue.clone(),
                        row.enqueue_shard,
                        row.lane_seq,
                        row.job_id,
                    ),
                    row,
                )
            })
            .collect();

        let attempt_map: BTreeMap<(i64, i64), AttemptStateRow> = attempt_rows
            .into_iter()
            .map(|row| ((row.job_id, row.run_lease), row))
            .collect();

        let mut hydrated = Vec::with_capacity(deleted.len());
        for deleted_row in deleted {
            let ready = ready_map
                .get(&(
                    deleted_row.ready_slot,
                    deleted_row.ready_generation,
                    deleted_row.queue.clone(),
                    deleted_row.enqueue_shard,
                    deleted_row.lane_seq,
                    deleted_row.job_id,
                ))
                .ok_or_else(|| {
                    AwaError::Validation(format!(
                        "queue storage ready row missing for deleted lease job {} run_lease {}",
                        deleted_row.job_id, deleted_row.run_lease
                    ))
                })?;
            let attempt = attempt_map.get(&(deleted_row.job_id, deleted_row.run_lease));

            hydrated.push(LeaseTransitionRow {
                ready_slot: deleted_row.ready_slot,
                ready_generation: deleted_row.ready_generation,
                job_id: deleted_row.job_id,
                kind: ready.kind.clone(),
                queue: ready.queue.clone(),
                args: ready.args.clone(),
                state: deleted_row.state,
                priority: deleted_row.priority,
                attempt: deleted_row.attempt,
                run_lease: deleted_row.run_lease,
                max_attempts: deleted_row.max_attempts,
                lane_seq: deleted_row.lane_seq,
                enqueue_shard: deleted_row.enqueue_shard,
                run_at: ready.run_at,
                attempted_at: deleted_row.attempted_at,
                created_at: ready.created_at,
                unique_key: ready.unique_key.clone(),
                unique_states: ready.unique_states.clone(),
                payload: ready.payload.clone(),
                progress: attempt.and_then(|row| row.progress.clone()),
            });
        }

        Ok(hydrated)
    }

    async fn close_open_receipt_claim_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
        run_lease: i64,
        outcome: &str,
    ) -> Result<Option<LeaseTransitionRow>, AwaError> {
        if !self.lease_claim_receipts() {
            return Ok(None);
        }

        self.lock_receipt_attempts_tx(tx, &[(job_id, run_lease)])
            .await?;

        let schema = self.schema();
        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            WITH row_target AS (
                -- Target is the open claim identified from the
                -- partitioned lease_claims table anti-joined against
                -- durable closure evidence.
                SELECT
                    claims.claim_slot,
                    NULL::bigint AS batch_id,
                    claims.receipt_id,
                    claims.ready_slot,
                    claims.ready_generation,
                    claims.job_id,
                    claims.queue,
                    'running'::awa.job_state AS state,
                    claims.priority,
                    claims.attempt,
                    claims.run_lease,
                    claims.max_attempts,
                    claims.lane_seq,
                    claims.enqueue_shard,
                    claims.claimed_at AS attempted_at
                FROM {schema}.lease_claims AS claims
                WHERE claims.job_id = $1
                  AND claims.run_lease = $2
                  AND claims.closed_at IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.lease_claim_closures AS closures
                      WHERE closures.claim_slot = claims.claim_slot
                        AND closures.job_id = claims.job_id
                        AND closures.run_lease = claims.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.lease_claim_closure_batches AS closure_batches
                      WHERE closure_batches.receipt_ranges @> claims.receipt_id
                  )
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
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.leases AS lease
                      WHERE lease.job_id = claims.job_id
                        AND lease.run_lease = claims.run_lease
                  )
                FOR UPDATE OF claims
            ),
            batch_target AS (
                SELECT
                    claim_batches.claim_slot,
                    claim_batches.batch_id,
                    items.receipt_id,
                    claim_batches.ready_slot,
                    claim_batches.ready_generation,
                    items.job_id,
                    claim_batches.queue,
                    'running'::awa.job_state AS state,
                    claim_batches.priority,
                    items.attempt,
                    items.run_lease,
                    items.max_attempts,
                    items.lane_seq,
                    claim_batches.enqueue_shard,
                    claim_batches.claimed_at AS attempted_at
                FROM {schema}.lease_claim_batches AS claim_batches
                CROSS JOIN LATERAL unnest(
                    claim_batches.job_ids,
                    claim_batches.run_leases,
                    claim_batches.receipt_ids,
                    claim_batches.lane_seqs,
                    claim_batches.attempts,
                    claim_batches.max_attempts
                ) AS items(job_id, run_lease, receipt_id, lane_seq, attempt, max_attempts)
                WHERE items.job_id = $1
                  AND items.run_lease = $2
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.lease_claim_closures AS closures
                      WHERE closures.claim_slot = claim_batches.claim_slot
                        AND closures.job_id = items.job_id
                        AND closures.run_lease = items.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {schema}.lease_claim_closure_batches AS closure_batches
                      WHERE closure_batches.claim_slot = claim_batches.claim_slot
                        AND closure_batches.receipt_ranges @> items.receipt_id
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
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.leases AS lease
                      WHERE lease.job_id = items.job_id
                        AND lease.run_lease = items.run_lease
                  )
                FOR UPDATE OF claim_batches
            ),
            target AS (
                SELECT * FROM row_target
                UNION ALL
                SELECT * FROM batch_target
                LIMIT 1
            ),
            -- A row target (batch_id IS NULL) is a deadline lease claim:
            -- close it explicitly so the prune gates balance it against
            -- the lease_claims row. A batch target has no lease_claims
            -- row to JOIN, so it closes into the batch ledger below.
            inserted AS (
                INSERT INTO {schema}.lease_claim_closures (claim_slot, job_id, run_lease, outcome, closed_at)
                SELECT target.claim_slot, target.job_id, target.run_lease, $3, clock_timestamp()
                FROM target
                WHERE target.batch_id IS NULL
                ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                RETURNING claim_slot, job_id, run_lease, closed_at
            ),
            inserted_batches AS (
                INSERT INTO {schema}.lease_claim_closure_batches (
                    claim_slot,
                    ready_slot,
                    ready_generation,
                    outcome,
                    closed_count,
                    receipt_ids,
                    receipt_ranges,
                    closed_at
                )
                SELECT
                    target.claim_slot,
                    target.ready_slot,
                    target.ready_generation,
                    $3,
                    count(*)::int,
                    array_agg(target.receipt_id ORDER BY target.receipt_id),
                    range_agg(int8range(target.receipt_id, target.receipt_id + 1, '[)') ORDER BY target.receipt_id),
                    clock_timestamp()
                FROM target
                WHERE target.batch_id IS NOT NULL
                GROUP BY target.claim_slot, target.ready_slot, target.ready_generation
                RETURNING claim_slot
            ),
            marked AS (
                UPDATE {schema}.lease_claims AS claims
                SET closed_at = COALESCE(claims.closed_at, inserted.closed_at)
                FROM inserted
                WHERE claims.claim_slot = inserted.claim_slot
                  AND claims.job_id = inserted.job_id
                  AND claims.run_lease = inserted.run_lease
                RETURNING claims.job_id
            ),
            -- The batch ledger has no job_id/run_lease column to RETURN,
            -- so derive the closed batch target from `target` gated on
            -- the batch insert having fired (target is LIMIT 1).
            closed_target AS (
                SELECT claim_slot, job_id, run_lease FROM inserted
                UNION ALL
                SELECT target.claim_slot, target.job_id, target.run_lease
                FROM target
                WHERE target.batch_id IS NOT NULL
                  AND EXISTS (SELECT 1 FROM inserted_batches)
            )
            SELECT
                target.ready_slot,
                target.ready_generation,
                target.job_id,
                target.queue,
                target.state,
                target.priority,
                target.attempt,
                target.run_lease,
                target.max_attempts,
                target.lane_seq,
                target.enqueue_shard,
                NULL::timestamptz AS heartbeat_at,
                NULL::timestamptz AS deadline_at,
                target.attempted_at,
                NULL::uuid AS callback_id,
                NULL::timestamptz AS callback_timeout_at
            FROM target
            JOIN closed_target
              ON closed_target.claim_slot = target.claim_slot
             AND closed_target.job_id = target.job_id
             AND closed_target.run_lease = target.run_lease
            "#
        ))
        .bind(job_id)
        .bind(run_lease)
        .bind(outcome)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if deleted.is_empty() {
            return Ok(None);
        }

        let moved = self.hydrate_deleted_leases_tx(tx, deleted).await?;
        Ok(moved.into_iter().next())
    }

    async fn take_running_attempt_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
        run_lease: i64,
        receipt_outcome: &str,
    ) -> Result<Option<LeaseTransitionRow>, AwaError> {
        if let Some(moved) = self
            .close_open_receipt_claim_tx(tx, job_id, run_lease, receipt_outcome)
            .await?
        {
            return Ok(Some(moved));
        }

        let schema = self.schema();
        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE job_id = $1
              AND run_lease = $2
              AND state = 'running'
            RETURNING
                ready_slot,
                ready_generation,
                job_id,
                queue,
                state,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                heartbeat_at,
                deadline_at,
                attempted_at,
                callback_id,
                callback_timeout_at
            "#
        ))
        .bind(job_id)
        .bind(run_lease)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if deleted.is_empty() {
            return Ok(None);
        }

        let moved = self.hydrate_deleted_leases_tx(tx, deleted).await?;
        Ok(moved.into_iter().next())
    }

    async fn rescue_stale_receipt_claims_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<DeletedLeaseRow>, AwaError> {
        let mut rescued = Vec::new();
        let mut remaining = RECEIPT_RESCUE_BATCH_LIMIT;
        let preferred_slot = self.oldest_initialized_claim_slot_tx(tx).await?;

        if let Some(slot) = preferred_slot {
            let mut slot_rescued = self
                .rescue_stale_receipt_claims_for_slot_tx(tx, slot, cutoff, remaining)
                .await?;
            remaining = remaining.saturating_sub(slot_rescued.len() as i64);
            rescued.append(&mut slot_rescued);
            if remaining == 0 {
                return Ok(rescued);
            }
        }

        for slot in 0..self.claim_slot_count() {
            let slot = slot as i32;
            if Some(slot) == preferred_slot {
                continue;
            }

            let mut slot_rescued = self
                .rescue_stale_receipt_claims_for_slot_tx(tx, slot, cutoff, remaining)
                .await?;
            remaining = remaining.saturating_sub(slot_rescued.len() as i64);
            rescued.append(&mut slot_rescued);

            if remaining == 0 {
                break;
            }
        }

        Ok(rescued)
    }

    async fn rescue_stale_receipt_claims_for_slot_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        slot: i32,
        cutoff: DateTime<Utc>,
        rescue_limit: i64,
    ) -> Result<Vec<DeletedLeaseRow>, AwaError> {
        let schema = self.schema();
        let claim_child = claim_child_name(schema, slot as usize);
        let claim_batch_child = claim_batch_child_name(schema, slot as usize);
        let closure_child = closure_child_name(schema, slot as usize);
        let closure_batch_child = claim_closure_batch_child_name(schema, slot as usize);
        let rescued: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            WITH cursor_row AS (
                SELECT
                    rescue_cursor_claimed_at,
                    rescue_cursor_job_id,
                    rescue_cursor_run_lease
                FROM {schema}.claim_ring_slots
                WHERE slot = $1
                FOR UPDATE
            ),
            claim_source AS MATERIALIZED (
                SELECT
                    claims.claim_slot,
                    NULL::bigint AS batch_id,
                    claims.ready_slot,
                    claims.ready_generation,
                    claims.job_id,
                    claims.queue,
                    claims.priority,
                    claims.attempt,
                    claims.run_lease,
                    claims.max_attempts,
                    claims.lane_seq,
                    claims.enqueue_shard,
                    claims.receipt_id,
                    claims.claimed_at,
                    claims.closed_at,
                    false AS compact_batch
                FROM {claim_child} AS claims
                WHERE claims.claim_slot = $1
                UNION ALL
                SELECT
                    claim_batches.claim_slot,
                    claim_batches.batch_id,
                    claim_batches.ready_slot,
                    claim_batches.ready_generation,
                    items.job_id,
                    claim_batches.queue,
                    claim_batches.priority,
                    items.attempt,
                    items.run_lease,
                    items.max_attempts,
                    items.lane_seq,
                    claim_batches.enqueue_shard,
                    items.receipt_id,
                    claim_batches.claimed_at,
                    NULL::timestamptz AS closed_at,
                    true AS compact_batch
                FROM {claim_batch_child} AS claim_batches
                CROSS JOIN LATERAL unnest(
                    claim_batches.job_ids,
                    claim_batches.run_leases,
                    claim_batches.receipt_ids,
                    claim_batches.lane_seqs,
                    claim_batches.attempts,
                    claim_batches.max_attempts
                ) AS items(job_id, run_lease, receipt_id, lane_seq, attempt, max_attempts)
                WHERE claim_batches.claim_slot = $1
            ),
            after_cursor AS MATERIALIZED (
                SELECT
                    claim_source.claim_slot,
                    claim_source.job_id,
                    claim_source.run_lease,
                    row_number() OVER (
                        ORDER BY claim_source.claimed_at, claim_source.job_id, claim_source.run_lease
                    ) AS rn
                FROM claim_source
                CROSS JOIN cursor_row
                WHERE claim_source.claimed_at < $2
                  AND (claim_source.claimed_at, claim_source.job_id, claim_source.run_lease)
                      > (
                          cursor_row.rescue_cursor_claimed_at,
                          cursor_row.rescue_cursor_job_id,
                          cursor_row.rescue_cursor_run_lease
                        )
                ORDER BY claim_source.claimed_at, claim_source.job_id, claim_source.run_lease
                LIMIT $3
            ),
            after_count AS (
                SELECT count(*)::bigint AS count FROM after_cursor
            ),
            before_cursor AS MATERIALIZED (
                SELECT
                    claim_source.claim_slot,
                    claim_source.job_id,
                    claim_source.run_lease,
                    after_count.count + row_number() OVER (
                        ORDER BY claim_source.claimed_at, claim_source.job_id, claim_source.run_lease
                    ) AS rn
                FROM claim_source
                CROSS JOIN cursor_row
                CROSS JOIN after_count
                WHERE after_count.count < $3
                  AND claim_source.claimed_at < $2
                  AND (claim_source.claimed_at, claim_source.job_id, claim_source.run_lease)
                      <= (
                          cursor_row.rescue_cursor_claimed_at,
                          cursor_row.rescue_cursor_job_id,
                          cursor_row.rescue_cursor_run_lease
                        )
                ORDER BY claim_source.claimed_at, claim_source.job_id, claim_source.run_lease
                LIMIT (SELECT GREATEST($3 - count, 0) FROM after_count)
            ),
            candidate_keys AS MATERIALIZED (
                SELECT claim_slot, job_id, run_lease, rn FROM after_cursor
                UNION ALL
                SELECT claim_slot, job_id, run_lease, rn FROM before_cursor
            ),
            candidates AS MATERIALIZED (
                SELECT
                    claim_source.claim_slot,
                    claim_source.batch_id,
                    claim_source.ready_slot,
                    claim_source.ready_generation,
                    claim_source.job_id,
                    claim_source.queue,
                    claim_source.priority,
                    claim_source.attempt,
                    claim_source.run_lease,
                    claim_source.max_attempts,
                    claim_source.lane_seq,
                    claim_source.enqueue_shard,
                    claim_source.receipt_id,
                    claim_source.claimed_at,
                    claim_source.closed_at,
                    claim_source.compact_batch,
                    COALESCE(attempt.heartbeat_at, claim_source.claimed_at) < $2 AS is_stale,
                    (
                        claim_source.closed_at IS NOT NULL
                        OR EXISTS (
                            SELECT 1 FROM {closure_child} AS closures
                            WHERE closures.claim_slot = claim_source.claim_slot
                              AND closures.job_id = claim_source.job_id
                              AND closures.run_lease = claim_source.run_lease
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM {closure_batch_child} AS closure_batches
                            WHERE closure_batches.receipt_ranges @> claim_source.receipt_id
                        )
                        OR EXISTS (
                            SELECT 1 FROM {schema}.done_entries AS done
                            WHERE done.job_id = claim_source.job_id
                              AND done.run_lease = claim_source.run_lease
                        )
                        OR EXISTS (
                            SELECT 1 FROM {schema}.deferred_jobs AS deferred
                            WHERE deferred.job_id = claim_source.job_id
                              AND deferred.run_lease = claim_source.run_lease
                        )
                        OR EXISTS (
                            SELECT 1 FROM {schema}.dlq_entries AS dlq
                            WHERE dlq.job_id = claim_source.job_id
                              AND dlq.run_lease = claim_source.run_lease
                        )
                    ) AS is_closed,
                    EXISTS (
                        SELECT 1 FROM {schema}.leases AS lease
                        WHERE lease.job_id = claim_source.job_id
                          AND lease.run_lease = claim_source.run_lease
                    ) AS is_lease_managed,
                    candidate_keys.rn
                FROM candidate_keys
                JOIN claim_source
                  ON claim_source.claim_slot = candidate_keys.claim_slot
                 AND claim_source.job_id = candidate_keys.job_id
                 AND claim_source.run_lease = candidate_keys.run_lease
                LEFT JOIN {schema}.attempt_state AS attempt
                  ON attempt.job_id = claim_source.job_id
                 AND attempt.run_lease = claim_source.run_lease
            ),
            stale_candidates AS (
                SELECT candidates.*
                FROM candidates
                WHERE NOT is_closed
                  AND NOT is_lease_managed
                  AND is_stale
                ORDER BY rn
                LIMIT $4
            ),
            stale_row_locked AS (
                SELECT stale_candidates.*
                FROM stale_candidates
                JOIN {claim_child} AS claims
                  ON claims.claim_slot = stale_candidates.claim_slot
                 AND claims.job_id = stale_candidates.job_id
                 AND claims.run_lease = stale_candidates.run_lease
                WHERE NOT stale_candidates.compact_batch
                  AND claims.closed_at IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM {closure_child} AS closures
                      WHERE closures.claim_slot = claims.claim_slot
                        AND closures.job_id = claims.job_id
                        AND closures.run_lease = claims.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {closure_batch_child} AS closure_batches
                      WHERE closure_batches.receipt_ranges @> claims.receipt_id
                  )
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
                  AND pg_catalog.pg_try_advisory_xact_lock(
                      pg_catalog.hashtextextended(
                          format('awa.receipt.complete:%s:%s', claims.job_id, claims.run_lease),
                          0
                      )
                  )
                  -- A claim that already materialized into `leases` is
                  -- on the lease-side heartbeat-rescue path (see
                  -- `rescue_stale_heartbeats`). Rescuing it again here
                  -- would write a second closure for an attempt the
                  -- runtime is still tracking via its lease row.
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.leases AS lease
	                      WHERE lease.job_id = claims.job_id
	                        AND lease.run_lease = claims.run_lease
	                  )
                FOR UPDATE OF claims SKIP LOCKED
            ),
            stale_batch_locked AS (
                SELECT stale_candidates.*
                FROM stale_candidates
                JOIN {claim_batch_child} AS claim_batches
                  ON claim_batches.claim_slot = stale_candidates.claim_slot
                 AND claim_batches.batch_id = stale_candidates.batch_id
                WHERE stale_candidates.compact_batch
                  AND NOT EXISTS (
                      SELECT 1 FROM {closure_child} AS closures
                      WHERE closures.claim_slot = stale_candidates.claim_slot
                        AND closures.job_id = stale_candidates.job_id
                        AND closures.run_lease = stale_candidates.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {closure_batch_child} AS closure_batches
                      WHERE closure_batches.receipt_ranges @> stale_candidates.receipt_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.done_entries AS done
                      WHERE done.job_id = stale_candidates.job_id
                        AND done.run_lease = stale_candidates.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.deferred_jobs AS deferred
                      WHERE deferred.job_id = stale_candidates.job_id
                        AND deferred.run_lease = stale_candidates.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.dlq_entries AS dlq
                      WHERE dlq.job_id = stale_candidates.job_id
                        AND dlq.run_lease = stale_candidates.run_lease
                  )
                  AND pg_catalog.pg_try_advisory_xact_lock(
                      pg_catalog.hashtextextended(
                          format('awa.receipt.complete:%s:%s', stale_candidates.job_id, stale_candidates.run_lease),
                          0
                      )
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.leases AS lease
                      WHERE lease.job_id = stale_candidates.job_id
                        AND lease.run_lease = stale_candidates.run_lease
                  )
                FOR UPDATE OF claim_batches SKIP LOCKED
            ),
            stale_locked AS (
                SELECT * FROM stale_row_locked
                UNION ALL
                SELECT * FROM stale_batch_locked
            ),
            -- Row-sourced (deadline lease) claims close explicitly so the
            -- prune gates balance them against the lease_claims row.
            -- Compact batch-sourced claims have no lease_claims row to
            -- JOIN, so they close into the batch ledger the queue prune
            -- gate counts via compact_count.
            inserted AS (
                INSERT INTO {schema}.lease_claim_closures (claim_slot, job_id, run_lease, outcome, closed_at)
                SELECT stale_locked.claim_slot, stale_locked.job_id, stale_locked.run_lease, 'rescued', clock_timestamp()
                FROM stale_locked
                WHERE NOT stale_locked.compact_batch
                ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                RETURNING claim_slot, job_id, run_lease, closed_at
            ),
            inserted_batches AS (
                INSERT INTO {schema}.lease_claim_closure_batches (
                    claim_slot,
                    ready_slot,
                    ready_generation,
                    outcome,
                    closed_count,
                    receipt_ids,
                    receipt_ranges,
                    closed_at
                )
                SELECT
                    stale_locked.claim_slot,
                    stale_locked.ready_slot,
                    stale_locked.ready_generation,
                    'rescued',
                    count(*)::int,
                    array_agg(stale_locked.receipt_id ORDER BY stale_locked.receipt_id),
                    range_agg(int8range(stale_locked.receipt_id, stale_locked.receipt_id + 1, '[)') ORDER BY stale_locked.receipt_id),
                    clock_timestamp()
                FROM stale_locked
                WHERE stale_locked.compact_batch
                GROUP BY
                    stale_locked.claim_slot,
                    stale_locked.ready_slot,
                    stale_locked.ready_generation
                RETURNING claim_slot
            ),
            closed_locked AS (
                SELECT claim_slot, job_id, run_lease FROM inserted
                UNION ALL
                SELECT stale_locked.claim_slot, stale_locked.job_id, stale_locked.run_lease
                FROM stale_locked
                WHERE stale_locked.compact_batch
                  AND EXISTS (SELECT 1 FROM inserted_batches)
            ),
            marked AS (
                UPDATE {schema}.lease_claims AS claims
                SET closed_at = COALESCE(claims.closed_at, inserted.closed_at)
                FROM inserted
                WHERE claims.claim_slot = inserted.claim_slot
                  AND claims.job_id = inserted.job_id
                  AND claims.run_lease = inserted.run_lease
                RETURNING claims.job_id
            ),
            annotated AS (
                SELECT
                    candidates.*,
                    (
                        candidates.is_closed
                        OR candidates.is_lease_managed
                        OR NOT candidates.is_stale
                        OR EXISTS (
                            SELECT 1 FROM closed_locked
                            WHERE closed_locked.claim_slot = candidates.claim_slot
                              AND closed_locked.job_id = candidates.job_id
                              AND closed_locked.run_lease = candidates.run_lease
                        )
                    ) AS advanceable
                FROM candidates
            ),
            bounded AS (
                SELECT
                    annotated.*,
                    min(CASE WHEN NOT annotated.advanceable THEN annotated.rn END) OVER () AS first_blocked_rn
                FROM annotated
            ),
            advance_target AS (
                SELECT claimed_at, job_id, run_lease
                FROM bounded
                WHERE first_blocked_rn IS NULL OR rn < first_blocked_rn
                ORDER BY rn DESC
                LIMIT 1
            ),
            advance_cursor AS (
                UPDATE {schema}.claim_ring_slots AS slots
                SET rescue_cursor_claimed_at = advance_target.claimed_at,
                    rescue_cursor_job_id = advance_target.job_id,
                    rescue_cursor_run_lease = advance_target.run_lease
                FROM advance_target
                WHERE slots.slot = $1
                RETURNING slots.slot
            ),
            cursor_advance AS (
                SELECT count(*) FROM advance_cursor
            )
            SELECT
                stale_locked.ready_slot,
                stale_locked.ready_generation,
                stale_locked.job_id,
                stale_locked.queue,
                'running'::awa.job_state AS state,
                stale_locked.priority,
                stale_locked.attempt,
                stale_locked.run_lease,
                stale_locked.max_attempts,
                stale_locked.lane_seq,
                stale_locked.enqueue_shard,
                stale_locked.claimed_at AS attempted_at
            FROM stale_locked
            JOIN closed_locked
              ON closed_locked.claim_slot = stale_locked.claim_slot
             AND closed_locked.job_id = stale_locked.job_id
             AND closed_locked.run_lease = stale_locked.run_lease
            CROSS JOIN cursor_advance
            "#
        ))
        .bind(slot)
        .bind(cutoff)
        .bind(RECEIPT_RESCUE_CURSOR_SCAN_LIMIT)
        .bind(rescue_limit)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(rescued)
    }

    /// Receipt-side counterpart to `rescue_expired_deadlines`: scans both
    /// receipt-claim representations for attempts whose per-claim
    /// `deadline_at` has passed but which still don't have a closure or a
    /// materialized lease row:
    ///
    /// * compact `lease_claim_batches` rows — the shape the claim path
    ///   writes for deadline-backed claims since #246; the batch carries
    ///   one shared `deadline_at`, and expired members are force-closed
    ///   into `lease_claim_closure_batches`;
    /// * row-local `lease_claims` rows — legacy in-flight claims written
    ///   before the compact-deadline upgrade; these keep the original
    ///   `lease_claim_closures` closure shape.
    ///
    /// Each match is returned for the maintenance caller to convert into
    /// a deferred / DLQ row, exactly as the lease-side path does.
    ///
    /// The anti-joins mirror `rescue_stale_receipt_claims_tx`'s
    /// disambiguation: a claim that has already materialized into
    /// `leases` is on the lease-side deadline-rescue path, and
    /// rescuing it here would double-close it.
    ///
    /// Unlike the lease-side scan, receipt deadline rescue walks each
    /// claim partition through a tiny cursor ordered by deadline (one
    /// cursor per representation). That keeps a long MVCC horizon from
    /// making every maintenance tick re-prove old successful receipt
    /// completions until claim prune can truncate the partition.
    async fn rescue_expired_receipt_deadlines_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    ) -> Result<Vec<DeletedLeaseRow>, AwaError> {
        let mut rescued = Vec::new();
        let mut remaining = RECEIPT_RESCUE_BATCH_LIMIT;
        let preferred_slot = self.oldest_initialized_claim_slot_tx(tx).await?;

        let mut slots: Vec<i32> = Vec::with_capacity(self.claim_slot_count() + 1);
        if let Some(slot) = preferred_slot {
            slots.push(slot);
        }
        for slot in 0..self.claim_slot_count() {
            let slot = slot as i32;
            if Some(slot) != preferred_slot {
                slots.push(slot);
            }
        }

        for slot in slots {
            let mut slot_rescued = self
                .rescue_expired_batch_deadlines_for_slot_tx(tx, slot, remaining)
                .await?;
            remaining = remaining.saturating_sub(slot_rescued.len() as i64);
            rescued.append(&mut slot_rescued);
            if remaining == 0 {
                break;
            }

            let mut slot_rescued = self
                .rescue_expired_receipt_deadlines_for_slot_tx(tx, slot, remaining)
                .await?;
            remaining = remaining.saturating_sub(slot_rescued.len() as i64);
            rescued.append(&mut slot_rescued);
            if remaining == 0 {
                break;
            }
        }

        Ok(rescued)
    }

    async fn oldest_initialized_claim_slot_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    ) -> Result<Option<i32>, AwaError> {
        let schema = self.schema();
        let preferred_slot = sqlx::query_as::<_, (i32, i64, i32)>(&format!(
            r#"
            SELECT ledger.slot, ledger.generation, state.slot_count
            FROM {schema}.claim_ring_state AS state
            CROSS JOIN LATERAL (
                SELECT slot, generation
                FROM {schema}.claim_ring_rotations
                ORDER BY generation DESC
                LIMIT 1
            ) AS ledger
            WHERE state.singleton = TRUE
            "#
        ))
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?
        .and_then(|(current_slot, generation, slot_count)| {
            oldest_initialized_ring_slot(current_slot, generation, slot_count)
                .map(|(slot, _generation)| slot)
                .filter(|slot| *slot >= 0 && (*slot as usize) < self.claim_slot_count())
        });

        Ok(preferred_slot)
    }

    async fn rescue_expired_receipt_deadlines_for_slot_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        slot: i32,
        rescue_limit: i64,
    ) -> Result<Vec<DeletedLeaseRow>, AwaError> {
        if rescue_limit <= 0 {
            return Ok(Vec::new());
        }

        let schema = self.schema();
        let claim_child = claim_child_name(schema, slot as usize);
        let closure_child = closure_child_name(schema, slot as usize);
        let closure_batch_child = claim_closure_batch_child_name(schema, slot as usize);
        let closed_evidence =
            receipt_closed_evidence_sql(schema, &closure_child, &closure_batch_child, "claims");
        let rescued: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            WITH cursor_row AS (
                SELECT
                    deadline_cursor_deadline_at,
                    deadline_cursor_job_id,
                    deadline_cursor_run_lease
                FROM {schema}.claim_ring_slots
                WHERE slot = $1
                FOR UPDATE
            ),
            after_cursor AS MATERIALIZED (
                SELECT
                    claims.claim_slot,
                    claims.job_id,
                    claims.run_lease,
                    row_number() OVER (
                        ORDER BY claims.deadline_at, claims.job_id, claims.run_lease
                    ) AS rn
                FROM {claim_child} AS claims
                CROSS JOIN cursor_row
                WHERE claims.claim_slot = $1
                  AND claims.deadline_at IS NOT NULL
                  AND claims.deadline_at < clock_timestamp()
                  AND (claims.deadline_at, claims.job_id, claims.run_lease)
                      > (
                          cursor_row.deadline_cursor_deadline_at,
                          cursor_row.deadline_cursor_job_id,
                          cursor_row.deadline_cursor_run_lease
                        )
                ORDER BY claims.deadline_at, claims.job_id, claims.run_lease
                LIMIT $2
            ),
            after_count AS (
                SELECT count(*)::bigint AS count FROM after_cursor
            ),
            before_cursor AS MATERIALIZED (
                SELECT
                    claims.claim_slot,
                    claims.job_id,
                    claims.run_lease,
                    after_count.count + row_number() OVER (
                        ORDER BY claims.deadline_at, claims.job_id, claims.run_lease
                    ) AS rn
                FROM {claim_child} AS claims
                CROSS JOIN cursor_row
                CROSS JOIN after_count
                WHERE after_count.count < $2
                  AND claims.claim_slot = $1
                  AND claims.deadline_at IS NOT NULL
                  AND claims.deadline_at < clock_timestamp()
                  AND (claims.deadline_at, claims.job_id, claims.run_lease)
                      <= (
                          cursor_row.deadline_cursor_deadline_at,
                          cursor_row.deadline_cursor_job_id,
                          cursor_row.deadline_cursor_run_lease
                        )
                ORDER BY claims.deadline_at, claims.job_id, claims.run_lease
                LIMIT (SELECT GREATEST($2 - count, 0) FROM after_count)
            ),
            candidate_keys AS MATERIALIZED (
                SELECT claim_slot, job_id, run_lease, rn FROM after_cursor
                UNION ALL
                SELECT claim_slot, job_id, run_lease, rn FROM before_cursor
            ),
            candidates AS MATERIALIZED (
                SELECT
                    claims.claim_slot,
                    claims.ready_slot,
                    claims.ready_generation,
                    claims.job_id,
                    claims.queue,
                    'running'::awa.job_state AS state,
                    claims.priority,
                    claims.attempt,
                    claims.run_lease,
                    claims.max_attempts,
                    claims.lane_seq,
                    claims.enqueue_shard,
                    claims.claimed_at,
                    claims.deadline_at,
                    claims.deadline_at < clock_timestamp() AS is_expired,
                    {closed_evidence} AS is_closed,
                    EXISTS (
                        SELECT 1 FROM {schema}.leases AS lease
                        WHERE lease.job_id = claims.job_id
                          AND lease.run_lease = claims.run_lease
                    ) AS is_lease_managed,
                    candidate_keys.rn
                FROM candidate_keys
                JOIN {claim_child} AS claims
                  ON claims.claim_slot = candidate_keys.claim_slot
                 AND claims.job_id = candidate_keys.job_id
                 AND claims.run_lease = candidate_keys.run_lease
            ),
            expired_candidates AS (
                SELECT candidates.*
                FROM candidates
                WHERE NOT is_closed
                  AND NOT is_lease_managed
                  AND is_expired
                ORDER BY rn
                LIMIT $3
            ),
            expired_locked AS (
                SELECT expired_candidates.*
                FROM expired_candidates
                JOIN {claim_child} AS claims
                  ON claims.claim_slot = expired_candidates.claim_slot
                 AND claims.job_id = expired_candidates.job_id
                 AND claims.run_lease = expired_candidates.run_lease
                WHERE NOT {closed_evidence}
                  AND claims.deadline_at < clock_timestamp()
                  AND pg_catalog.pg_try_advisory_xact_lock(
                      pg_catalog.hashtextextended(
                          format('awa.receipt.complete:%s:%s', claims.job_id, claims.run_lease),
                          0
                      )
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.leases AS lease
                      WHERE lease.job_id = claims.job_id
                        AND lease.run_lease = claims.run_lease
                  )
                FOR UPDATE OF claims SKIP LOCKED
            ),
            inserted AS (
                INSERT INTO {schema}.lease_claim_closures (claim_slot, job_id, run_lease, outcome, closed_at)
                SELECT
                    expired_locked.claim_slot,
                    expired_locked.job_id,
                    expired_locked.run_lease,
                    'deadline_expired',
                    clock_timestamp()
                FROM expired_locked
                ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                RETURNING claim_slot, job_id, run_lease, closed_at
            ),
            marked AS (
                UPDATE {schema}.lease_claims AS claims
                SET closed_at = COALESCE(claims.closed_at, inserted.closed_at)
                FROM inserted
                WHERE claims.claim_slot = inserted.claim_slot
                  AND claims.job_id = inserted.job_id
                  AND claims.run_lease = inserted.run_lease
                RETURNING claims.job_id
            ),
            annotated AS (
                SELECT
                    candidates.*,
                    (
                        candidates.is_closed
                        OR candidates.is_lease_managed
                        OR EXISTS (
                            SELECT 1 FROM inserted
                            WHERE inserted.claim_slot = candidates.claim_slot
                              AND inserted.job_id = candidates.job_id
                              AND inserted.run_lease = candidates.run_lease
                        )
                    ) AS advanceable
                FROM candidates
            ),
            bounded AS (
                SELECT
                    annotated.*,
                    min(CASE WHEN NOT annotated.advanceable THEN annotated.rn END) OVER () AS first_blocked_rn
                FROM annotated
            ),
            advance_target AS (
                SELECT deadline_at, job_id, run_lease
                FROM bounded
                WHERE first_blocked_rn IS NULL OR rn < first_blocked_rn
                ORDER BY rn DESC
                LIMIT 1
            ),
            advance_cursor AS (
                UPDATE {schema}.claim_ring_slots AS slots
                SET deadline_cursor_deadline_at = advance_target.deadline_at,
                    deadline_cursor_job_id = advance_target.job_id,
                    deadline_cursor_run_lease = advance_target.run_lease
                FROM advance_target
                WHERE slots.slot = $1
                RETURNING slots.slot
            ),
            cursor_advance AS (
                SELECT count(*) FROM advance_cursor
            )
            SELECT
                expired_locked.ready_slot,
                expired_locked.ready_generation,
                expired_locked.job_id,
                expired_locked.queue,
                expired_locked.state,
                expired_locked.priority,
                expired_locked.attempt,
                expired_locked.run_lease,
                expired_locked.max_attempts,
                expired_locked.lane_seq,
                expired_locked.enqueue_shard,
                expired_locked.claimed_at AS attempted_at
            FROM expired_locked
            JOIN inserted
              ON inserted.claim_slot = expired_locked.claim_slot
             AND inserted.job_id = expired_locked.job_id
             AND inserted.run_lease = expired_locked.run_lease
            CROSS JOIN cursor_advance
            "#
        ))
        .bind(slot)
        .bind(RECEIPT_DEADLINE_RESCUE_CURSOR_SCAN_LIMIT)
        .bind(rescue_limit)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(rescued)
    }

    /// Compact-batch counterpart of
    /// `rescue_expired_receipt_deadlines_for_slot_tx` (#246). Deadline-backed
    /// claims are written as one `lease_claim_batches` row per claimed batch
    /// with a single shared `deadline_at`, so the sweep cursor is
    /// `(deadline_at, batch_id)` on `claim_ring_slots.batch_deadline_cursor_*`
    /// and the candidate window includes only batches whose deadline has
    /// already passed (index-backed by the partial per-child
    /// `(deadline_at, batch_id) WHERE deadline_at IS NOT NULL` index).
    ///
    /// Expired batches expand into members; members WITHOUT durable closure
    /// evidence and WITHOUT a materialized lease are force-closed by
    /// appending a `lease_claim_closure_batches` row with outcome
    /// `'deadline_expired'` — the same closure shape compact claims use on
    /// every other exit path, so the claim/queue prune count proofs see the
    /// closure. Members that completed, materialized into `leases`
    /// (lease-side deadline rescue owns those), or already closed are
    /// skipped. The cursor advances past a batch only when every member is
    /// proven closed, lease-managed, or was closed by this transaction, so a
    /// partially-rescued batch (rescue-budget cutoff or a lost advisory-lock
    /// race) is revisited next tick.
    async fn rescue_expired_batch_deadlines_for_slot_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        slot: i32,
        rescue_limit: i64,
    ) -> Result<Vec<DeletedLeaseRow>, AwaError> {
        if rescue_limit <= 0 {
            return Ok(Vec::new());
        }

        let schema = self.schema();
        let claim_batch_child = claim_batch_child_name(schema, slot as usize);
        let closure_child = closure_child_name(schema, slot as usize);
        let closure_batch_child = claim_closure_batch_child_name(schema, slot as usize);
        let rescued: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            WITH cursor_row AS (
                SELECT
                    batch_deadline_cursor_deadline_at,
                    batch_deadline_cursor_batch_id
                FROM {schema}.claim_ring_slots
                WHERE slot = $1
                FOR UPDATE
            ),
            after_cursor AS MATERIALIZED (
                SELECT
                    claim_batches.claim_slot,
                    claim_batches.batch_id,
                    row_number() OVER (
                        ORDER BY claim_batches.deadline_at, claim_batches.batch_id
                    ) AS rn
                FROM {claim_batch_child} AS claim_batches
                CROSS JOIN cursor_row
                WHERE claim_batches.claim_slot = $1
                  AND claim_batches.deadline_at IS NOT NULL
                  AND claim_batches.deadline_at < clock_timestamp()
                  AND (claim_batches.deadline_at, claim_batches.batch_id)
                      > (
                          cursor_row.batch_deadline_cursor_deadline_at,
                          cursor_row.batch_deadline_cursor_batch_id
                        )
                ORDER BY claim_batches.deadline_at, claim_batches.batch_id
                LIMIT $2
            ),
            after_count AS (
                SELECT count(*)::bigint AS count FROM after_cursor
            ),
            before_cursor AS MATERIALIZED (
                SELECT
                    claim_batches.claim_slot,
                    claim_batches.batch_id,
                    after_count.count + row_number() OVER (
                        ORDER BY claim_batches.deadline_at, claim_batches.batch_id
                    ) AS rn
                FROM {claim_batch_child} AS claim_batches
                CROSS JOIN cursor_row
                CROSS JOIN after_count
                WHERE after_count.count < $2
                  AND claim_batches.claim_slot = $1
                  AND claim_batches.deadline_at IS NOT NULL
                  AND claim_batches.deadline_at < clock_timestamp()
                  AND (claim_batches.deadline_at, claim_batches.batch_id)
                      <= (
                          cursor_row.batch_deadline_cursor_deadline_at,
                          cursor_row.batch_deadline_cursor_batch_id
                        )
                ORDER BY claim_batches.deadline_at, claim_batches.batch_id
                LIMIT (SELECT GREATEST($2 - count, 0) FROM after_count)
            ),
            candidate_keys AS MATERIALIZED (
                SELECT claim_slot, batch_id, rn FROM after_cursor
                UNION ALL
                SELECT claim_slot, batch_id, rn FROM before_cursor
            ),
            -- Every member of a candidate batch, annotated with the same
            -- durable closure evidence and materialized-lease anti-joins the
            -- row-local deadline rescue uses. The batch deadline is shared,
            -- so every member of a candidate batch is expired by
            -- construction.
            members AS MATERIALIZED (
                SELECT
                    claim_batches.claim_slot,
                    claim_batches.batch_id,
                    claim_batches.ready_slot,
                    claim_batches.ready_generation,
                    items.job_id,
                    claim_batches.queue,
                    'running'::awa.job_state AS state,
                    claim_batches.priority,
                    items.attempt,
                    items.run_lease,
                    items.max_attempts,
                    items.lane_seq,
                    claim_batches.enqueue_shard,
                    items.receipt_id,
                    claim_batches.claimed_at,
                    claim_batches.deadline_at,
                    (
                        EXISTS (
                            SELECT 1 FROM {closure_child} AS closures
                            WHERE closures.claim_slot = claim_batches.claim_slot
                              AND closures.job_id = items.job_id
                              AND closures.run_lease = items.run_lease
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM {closure_batch_child} AS closure_batches
                            WHERE closure_batches.receipt_ranges @> items.receipt_id
                        )
                        OR EXISTS (
                            SELECT 1 FROM {schema}.done_entries AS done
                            WHERE done.job_id = items.job_id
                              AND done.run_lease = items.run_lease
                        )
                        OR EXISTS (
                            SELECT 1 FROM {schema}.deferred_jobs AS deferred
                            WHERE deferred.job_id = items.job_id
                              AND deferred.run_lease = items.run_lease
                        )
                        OR EXISTS (
                            SELECT 1 FROM {schema}.dlq_entries AS dlq
                            WHERE dlq.job_id = items.job_id
                              AND dlq.run_lease = items.run_lease
                        )
                    ) AS is_closed,
                    EXISTS (
                        SELECT 1 FROM {schema}.leases AS lease
                        WHERE lease.job_id = items.job_id
                          AND lease.run_lease = items.run_lease
                    ) AS is_lease_managed,
                    candidate_keys.rn
                FROM candidate_keys
                JOIN {claim_batch_child} AS claim_batches
                  ON claim_batches.claim_slot = candidate_keys.claim_slot
                 AND claim_batches.batch_id = candidate_keys.batch_id
                CROSS JOIN LATERAL unnest(
                    claim_batches.job_ids,
                    claim_batches.run_leases,
                    claim_batches.receipt_ids,
                    claim_batches.lane_seqs,
                    claim_batches.attempts,
                    claim_batches.max_attempts
                ) AS items(job_id, run_lease, receipt_id, lane_seq, attempt, max_attempts)
            ),
            expired_members AS (
                SELECT members.*
                FROM members
                WHERE NOT is_closed
                  AND NOT is_lease_managed
                ORDER BY rn, receipt_id
                LIMIT $3
            ),
            expired_locked AS (
                SELECT expired_members.*
                FROM expired_members
                JOIN {claim_batch_child} AS claim_batches
                  ON claim_batches.claim_slot = expired_members.claim_slot
                 AND claim_batches.batch_id = expired_members.batch_id
                WHERE claim_batches.deadline_at < clock_timestamp()
                  AND NOT EXISTS (
                      SELECT 1 FROM {closure_child} AS closures
                      WHERE closures.claim_slot = expired_members.claim_slot
                        AND closures.job_id = expired_members.job_id
                        AND closures.run_lease = expired_members.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {closure_batch_child} AS closure_batches
                      WHERE closure_batches.receipt_ranges @> expired_members.receipt_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.done_entries AS done
                      WHERE done.job_id = expired_members.job_id
                        AND done.run_lease = expired_members.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.deferred_jobs AS deferred
                      WHERE deferred.job_id = expired_members.job_id
                        AND deferred.run_lease = expired_members.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.dlq_entries AS dlq
                      WHERE dlq.job_id = expired_members.job_id
                        AND dlq.run_lease = expired_members.run_lease
                  )
                  AND pg_catalog.pg_try_advisory_xact_lock(
                      pg_catalog.hashtextextended(
                          format('awa.receipt.complete:%s:%s', expired_members.job_id, expired_members.run_lease),
                          0
                      )
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.leases AS lease
                      WHERE lease.job_id = expired_members.job_id
                        AND lease.run_lease = expired_members.run_lease
                  )
                FOR UPDATE OF claim_batches SKIP LOCKED
            ),
            -- Compact batch-sourced claims have no lease_claims row to mark,
            -- so they close into the batch closure ledger both prune count
            -- proofs read via compact closed_count totals.
            inserted_batches AS (
                INSERT INTO {closure_batch_child} (
                    claim_slot,
                    ready_slot,
                    ready_generation,
                    outcome,
                    closed_count,
                    receipt_ids,
                    receipt_ranges,
                    closed_at
                )
                SELECT
                    expired_locked.claim_slot,
                    expired_locked.ready_slot,
                    expired_locked.ready_generation,
                    'deadline_expired',
                    count(*)::int,
                    array_agg(expired_locked.receipt_id ORDER BY expired_locked.receipt_id),
                    range_agg(int8range(expired_locked.receipt_id, expired_locked.receipt_id + 1, '[)') ORDER BY expired_locked.receipt_id),
                    clock_timestamp()
                FROM expired_locked
                GROUP BY
                    expired_locked.claim_slot,
                    expired_locked.ready_slot,
                    expired_locked.ready_generation
                RETURNING claim_slot
            ),
            closed_locked AS (
                SELECT expired_locked.claim_slot, expired_locked.job_id, expired_locked.run_lease
                FROM expired_locked
                WHERE EXISTS (SELECT 1 FROM inserted_batches)
            ),
            annotated AS (
                SELECT
                    members.*,
                    (
                        members.is_closed
                        OR members.is_lease_managed
                        OR EXISTS (
                            SELECT 1 FROM closed_locked
                            WHERE closed_locked.claim_slot = members.claim_slot
                              AND closed_locked.job_id = members.job_id
                              AND closed_locked.run_lease = members.run_lease
                        )
                    ) AS advanceable
                FROM members
            ),
            -- The cursor may only advance past a batch when every one of its
            -- members is advanceable; a single open member (rescue-budget
            -- cutoff, lost advisory-lock race) blocks there so the batch is
            -- revisited.
            bounded AS (
                SELECT
                    annotated.*,
                    min(CASE WHEN NOT annotated.advanceable THEN annotated.rn END) OVER () AS first_blocked_rn
                FROM annotated
            ),
            advance_target AS (
                SELECT deadline_at, batch_id
                FROM bounded
                WHERE first_blocked_rn IS NULL OR rn < first_blocked_rn
                ORDER BY rn DESC
                LIMIT 1
            ),
            advance_cursor AS (
                UPDATE {schema}.claim_ring_slots AS slots
                SET batch_deadline_cursor_deadline_at = advance_target.deadline_at,
                    batch_deadline_cursor_batch_id = advance_target.batch_id
                FROM advance_target
                WHERE slots.slot = $1
                RETURNING slots.slot
            ),
            cursor_advance AS (
                SELECT count(*) FROM advance_cursor
            )
            SELECT
                expired_locked.ready_slot,
                expired_locked.ready_generation,
                expired_locked.job_id,
                expired_locked.queue,
                expired_locked.state,
                expired_locked.priority,
                expired_locked.attempt,
                expired_locked.run_lease,
                expired_locked.max_attempts,
                expired_locked.lane_seq,
                expired_locked.enqueue_shard,
                expired_locked.claimed_at AS attempted_at
            FROM expired_locked
            JOIN closed_locked
              ON closed_locked.claim_slot = expired_locked.claim_slot
             AND closed_locked.job_id = expired_locked.job_id
             AND closed_locked.run_lease = expired_locked.run_lease
            CROSS JOIN cursor_advance
            "#
        ))
        .bind(slot)
        .bind(RECEIPT_DEADLINE_RESCUE_CURSOR_SCAN_LIMIT)
        .bind(rescue_limit)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(rescued)
    }

    pub async fn load_job(&self, pool: &PgPool, job_id: i64) -> Result<Option<JobRow>, AwaError> {
        let schema = self.schema();
        let closure_rel = format!("{schema}.lease_claim_closures");
        let closure_batch_rel = format!("{schema}.lease_claim_closure_batches");
        let closed_evidence =
            receipt_closed_evidence_sql(schema, &closure_rel, &closure_batch_rel, "claims");
        let mut candidates = Vec::new();

        let ready_rows: Vec<ReadyJobRow> = sqlx::query_as(&format!(
            r#"
            SELECT
                job_id,
                kind,
                queue,
                args,
                priority,
                attempt,
                run_lease,
                max_attempts,
                run_at,
                attempted_at,
                created_at,
                unique_key,
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload
            FROM {schema}.ready_entries
            WHERE job_id = $1
            ORDER BY run_lease DESC, attempted_at DESC NULLS LAST, run_at DESC
            "#,
        ))
        .bind(job_id)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;
        for row in ready_rows {
            candidates.push(row.into_job_row()?);
        }

        let deferred_rows: Vec<DeferredJobRow> = sqlx::query_as(&format!(
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
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload
            FROM {schema}.deferred_jobs
            WHERE job_id = $1
            "#,
        ))
        .bind(job_id)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;
        for row in deferred_rows {
            candidates.push(row.into_job_row()?);
        }

        let lease_rows: Vec<LeaseJobRow> = sqlx::query_as(&format!(
            r#"
            SELECT
                lease.ready_slot,
                lease.ready_generation,
                lease.job_id,
                ready.kind,
                ready.queue,
                ready.args,
                lease.state,
                lease.priority,
                lease.attempt,
                lease.run_lease,
                lease.max_attempts,
                lease.lane_seq,
                ready.run_at,
                COALESCE(attempt.heartbeat_at, lease.heartbeat_at) AS heartbeat_at,
                lease.deadline_at,
                lease.attempted_at,
                NULL::timestamptz AS finalized_at,
                ready.created_at,
                ready.unique_key,
                ready.unique_states,
                lease.callback_id,
                lease.callback_timeout_at,
                attempt.callback_filter,
                attempt.callback_on_complete,
                attempt.callback_on_fail,
                attempt.callback_transform,
                COALESCE(ready.payload, '{{}}'::jsonb) AS payload,
                attempt.progress,
                attempt.callback_result
            FROM {schema}.leases AS lease
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = lease.ready_slot
             AND ready.ready_generation = lease.ready_generation
             AND ready.queue = lease.queue
             AND ready.priority = lease.priority
             AND ready.enqueue_shard = lease.enqueue_shard
             AND ready.lane_seq = lease.lane_seq
            LEFT JOIN {schema}.attempt_state AS attempt
              ON attempt.job_id = lease.job_id
             AND attempt.run_lease = lease.run_lease
            WHERE lease.job_id = $1
            ORDER BY lease.run_lease DESC
            "#,
        ))
        .bind(job_id)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;
        for row in lease_rows {
            candidates.push(row.into_job_row()?);
        }

        // Report receipt-backed attempts as running by anti-joining
        // lease_claims against every durable closure evidence shape.
        let lease_claim_rows: Vec<LeaseJobRow> = sqlx::query_as(&format!(
            r#"
            SELECT
                claims.ready_slot,
                claims.ready_generation,
                claims.job_id,
                ready.kind,
                ready.queue,
                ready.args,
                'running'::awa.job_state AS state,
                claims.priority,
                claims.attempt,
                claims.run_lease,
                claims.max_attempts,
                claims.lane_seq,
                ready.run_at,
                attempt.heartbeat_at,
                claims.deadline_at,
                claims.claimed_at AS attempted_at,
                NULL::timestamptz AS finalized_at,
                ready.created_at,
                ready.unique_key,
                ready.unique_states,
                NULL::uuid AS callback_id,
                NULL::timestamptz AS callback_timeout_at,
                attempt.callback_filter,
                attempt.callback_on_complete,
                attempt.callback_on_fail,
                attempt.callback_transform,
                COALESCE(ready.payload, '{{}}'::jsonb) AS payload,
                attempt.progress,
                attempt.callback_result
            FROM {schema}.lease_claims AS claims
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = claims.ready_slot
             AND ready.ready_generation = claims.ready_generation
             AND ready.queue = claims.queue
             AND ready.priority = claims.priority
             AND ready.enqueue_shard = claims.enqueue_shard
             AND ready.lane_seq = claims.lane_seq
            LEFT JOIN {schema}.attempt_state AS attempt
              ON attempt.job_id = claims.job_id
             AND attempt.run_lease = claims.run_lease
            WHERE claims.job_id = $1
              AND NOT {closed_evidence}
              -- Exclude claims that have already been materialized into
              -- leases — the lease-backed branch above already reports
              -- those.
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.leases AS lease
                  WHERE lease.job_id = claims.job_id
                    AND lease.run_lease = claims.run_lease
              )
            ORDER BY claims.run_lease DESC
            "#,
        ))
        .bind(job_id)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;
        for row in lease_claim_rows {
            candidates.push(row.into_job_row()?);
        }

        // Receipt claims are stored as compact batches. Expand
        // them only for this admin read, and report still-open items as
        // running until durable closure, terminal, or materialized-lease
        // evidence supersedes the claim.
        let lease_claim_batch_rows: Vec<LeaseJobRow> = sqlx::query_as(&format!(
            r#"
            SELECT
                claim_batches.ready_slot,
                claim_batches.ready_generation,
                items.job_id,
                ready.kind,
                ready.queue,
                ready.args,
                'running'::awa.job_state AS state,
                claim_batches.priority,
                items.attempt,
                items.run_lease,
                items.max_attempts,
                items.lane_seq,
                ready.run_at,
                attempt.heartbeat_at,
                claim_batches.deadline_at,
                claim_batches.claimed_at AS attempted_at,
                NULL::timestamptz AS finalized_at,
                ready.created_at,
                ready.unique_key,
                ready.unique_states,
                NULL::uuid AS callback_id,
                NULL::timestamptz AS callback_timeout_at,
                attempt.callback_filter,
                attempt.callback_on_complete,
                attempt.callback_on_fail,
                attempt.callback_transform,
                COALESCE(ready.payload, '{{}}'::jsonb) AS payload,
                attempt.progress,
                attempt.callback_result
            FROM {schema}.lease_claim_batches AS claim_batches
            CROSS JOIN LATERAL unnest(
                claim_batches.job_ids,
                claim_batches.run_leases,
                claim_batches.receipt_ids,
                claim_batches.lane_seqs,
                claim_batches.attempts,
                claim_batches.max_attempts
            ) AS items(job_id, run_lease, receipt_id, lane_seq, attempt, max_attempts)
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = claim_batches.ready_slot
             AND ready.ready_generation = claim_batches.ready_generation
             AND ready.queue = claim_batches.queue
             AND ready.enqueue_shard = claim_batches.enqueue_shard
             AND ready.lane_seq = items.lane_seq
             AND ready.job_id = items.job_id
            LEFT JOIN {schema}.attempt_state AS attempt
              ON attempt.job_id = items.job_id
             AND attempt.run_lease = items.run_lease
            WHERE items.job_id = $1
              AND NOT EXISTS (
                  SELECT 1
                  FROM {schema}.lease_claim_closures AS closures
                  WHERE closures.claim_slot = claim_batches.claim_slot
                    AND closures.job_id = items.job_id
                    AND closures.run_lease = items.run_lease
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM {schema}.lease_claim_closure_batches AS closure_batches
                  WHERE closure_batches.claim_slot = claim_batches.claim_slot
                    AND closure_batches.receipt_ranges @> items.receipt_id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM {schema}.leases AS lease
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
            ORDER BY items.run_lease DESC
            "#,
        ))
        .bind(job_id)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;
        for row in lease_claim_batch_rows {
            candidates.push(row.into_job_row()?);
        }

        let done_rows: Vec<DoneJobRow> = sqlx::query_as(&format!(
            r#"
            SELECT
                ready_slot,
                ready_generation,
                job_id,
                kind,
                queue,
                args,
                state,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                run_at,
                attempted_at,
                finalized_at,
                created_at,
                unique_key,
                unique_states,
                payload
            FROM {schema}.terminal_jobs AS done
            WHERE done.job_id = $1
            ORDER BY done.run_lease DESC, done.finalized_at DESC
            "#,
        ))
        .bind(job_id)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;
        for row in done_rows {
            candidates.push(row.into_job_row()?);
        }

        let dlq_rows: Vec<DlqJobRow> = sqlx::query_as(&format!(
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
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload,
                dlq_reason,
                dlq_at,
                original_run_lease
            FROM {schema}.dlq_entries
            WHERE job_id = $1
            ORDER BY dlq_at DESC
            "#,
        ))
        .bind(job_id)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;
        for row in dlq_rows {
            candidates.push(row.into_job_row()?);
        }

        Ok(candidates.into_iter().max_by_key(|job| {
            (
                job.run_lease,
                transition_timestamp(job),
                state_rank(job.state),
            )
        }))
    }

    pub async fn register_callback(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        timeout: Duration,
    ) -> Result<Uuid, AwaError> {
        let callback_id = Uuid::new_v4();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        self.ensure_mutable_running_attempt_tx(&mut tx, job_id, run_lease)
            .await?;
        let updated = sqlx::query(&format!(
            r#"
            UPDATE {}
            SET callback_id = $2,
                callback_timeout_at = clock_timestamp() + make_interval(secs => $3)
            WHERE job_id = $1
              AND state = 'running'
              AND run_lease = $4
            "#,
            self.leases_table()
        ))
        .bind(job_id)
        .bind(callback_id)
        .bind(timeout.as_secs_f64())
        .bind(run_lease)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if updated.rows_affected() == 0 {
            tx.rollback().await.map_err(map_sqlx_error)?;
            return Err(AwaError::Validation("job is not in running state".into()));
        }

        sqlx::query(&format!(
            r#"
            UPDATE {}
            SET callback_filter = NULL,
                callback_on_complete = NULL,
                callback_on_fail = NULL,
                callback_transform = NULL,
                updated_at = clock_timestamp()
            WHERE job_id = $1
              AND run_lease = $2
            "#,
            self.attempt_state_table()
        ))
        .bind(job_id)
        .bind(run_lease)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            DELETE FROM {}
            WHERE job_id = $1
              AND run_lease = $2
              AND progress IS NULL
              AND callback_result IS NULL
              AND callback_filter IS NULL
              AND callback_on_complete IS NULL
              AND callback_on_fail IS NULL
              AND callback_transform IS NULL
            "#,
            self.attempt_state_table()
        ))
        .bind(job_id)
        .bind(run_lease)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(callback_id)
    }

    pub async fn register_callback_with_config(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        timeout: Duration,
        config: &CallbackConfig,
    ) -> Result<Uuid, AwaError> {
        if config.is_empty() {
            return self
                .register_callback(pool, job_id, run_lease, timeout)
                .await;
        }

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
                    let references = program.references();
                    let bad_vars: Vec<String> = references
                        .variables()
                        .into_iter()
                        .filter(|v| *v != "payload")
                        .map(str::to_string)
                        .collect();
                    if !bad_vars.is_empty() {
                        return Err(AwaError::Validation(format!(
                            "CEL expression for {name} references undeclared variable(s): {}; only 'payload' is available",
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
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        self.ensure_mutable_running_attempt_tx(&mut tx, job_id, run_lease)
            .await?;
        let updated = sqlx::query(&format!(
            r#"
            UPDATE {}
            SET callback_id = $2,
                callback_timeout_at = clock_timestamp() + make_interval(secs => $3)
            WHERE job_id = $1
              AND state = 'running'
              AND run_lease = $4
            "#,
            self.leases_table()
        ))
        .bind(job_id)
        .bind(callback_id)
        .bind(timeout.as_secs_f64())
        .bind(run_lease)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if updated.rows_affected() == 0 {
            tx.rollback().await.map_err(map_sqlx_error)?;
            return Err(AwaError::Validation("job is not in running state".into()));
        }

        sqlx::query(&format!(
            r#"
            INSERT INTO {} (
                job_id,
                run_lease,
                callback_filter,
                callback_on_complete,
                callback_on_fail,
                callback_transform,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp())
            ON CONFLICT (job_id, run_lease)
            DO UPDATE SET
                callback_filter = EXCLUDED.callback_filter,
                callback_on_complete = EXCLUDED.callback_on_complete,
                callback_on_fail = EXCLUDED.callback_on_fail,
                callback_transform = EXCLUDED.callback_transform,
                updated_at = clock_timestamp()
            "#,
            self.attempt_state_table()
        ))
        .bind(job_id)
        .bind(run_lease)
        .bind(&config.filter)
        .bind(&config.on_complete)
        .bind(&config.on_fail)
        .bind(&config.transform)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(callback_id)
    }

    pub async fn cancel_callback(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
    ) -> Result<bool, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = sqlx::query(&format!(
            r#"
            UPDATE {}
            SET callback_id = NULL,
                callback_timeout_at = NULL
            WHERE job_id = $1
              AND callback_id IS NOT NULL
              AND state = 'running'
              AND run_lease = $2
            "#,
            self.leases_table()
        ))
        .bind(job_id)
        .bind(run_lease)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        if result.rows_affected() == 0 {
            tx.rollback().await.map_err(map_sqlx_error)?;
            return Ok(false);
        }

        sqlx::query(&format!(
            r#"
            UPDATE {}
            SET callback_filter = NULL,
                callback_on_complete = NULL,
                callback_on_fail = NULL,
                callback_transform = NULL,
                updated_at = clock_timestamp()
            WHERE job_id = $1
              AND run_lease = $2
            "#,
            self.attempt_state_table()
        ))
        .bind(job_id)
        .bind(run_lease)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            DELETE FROM {}
            WHERE job_id = $1
              AND run_lease = $2
              AND progress IS NULL
              AND callback_result IS NULL
              AND callback_filter IS NULL
              AND callback_on_complete IS NULL
              AND callback_on_fail IS NULL
              AND callback_transform IS NULL
            "#,
            self.attempt_state_table()
        ))
        .bind(job_id)
        .bind(run_lease)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(true)
    }

    /// Load the currently-active lease row for `job_id` (running or
    /// waiting_external) inside a caller-owned transaction. Used by ADR-029
    /// helpers that need the post-park snapshot — including the
    /// `callback_id` and `callback_timeout_at` written by
    /// `register_callback()` — without leaving the transaction that just
    /// performed the parking UPDATE.
    pub async fn load_active_lease_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        job_id: i64,
        run_lease: i64,
    ) -> Result<Option<JobRow>, AwaError> {
        let schema = self.schema();
        let row: Option<LeaseJobRow> = sqlx::query_as(&format!(
            r#"
            SELECT
                lease.ready_slot,
                lease.ready_generation,
                lease.job_id,
                ready.kind,
                ready.queue,
                ready.args,
                lease.state,
                lease.priority,
                lease.attempt,
                lease.run_lease,
                lease.max_attempts,
                lease.lane_seq,
                ready.run_at,
                COALESCE(attempt.heartbeat_at, lease.heartbeat_at) AS heartbeat_at,
                lease.deadline_at,
                lease.attempted_at,
                NULL::timestamptz AS finalized_at,
                ready.created_at,
                ready.unique_key,
                ready.unique_states,
                lease.callback_id,
                lease.callback_timeout_at,
                attempt.callback_filter,
                attempt.callback_on_complete,
                attempt.callback_on_fail,
                attempt.callback_transform,
                COALESCE(ready.payload, '{{}}'::jsonb) AS payload,
                attempt.progress,
                attempt.callback_result
            FROM {schema}.leases AS lease
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = lease.ready_slot
             AND ready.ready_generation = lease.ready_generation
             AND ready.queue = lease.queue
             AND ready.priority = lease.priority
             AND ready.enqueue_shard = lease.enqueue_shard
             AND ready.lane_seq = lease.lane_seq
            LEFT JOIN {schema}.attempt_state AS attempt
              ON attempt.job_id = lease.job_id
             AND attempt.run_lease = lease.run_lease
            WHERE lease.job_id = $1
              AND lease.run_lease = $2
            "#,
        ))
        .bind(job_id)
        .bind(run_lease)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        row.map(LeaseJobRow::into_job_row).transpose()
    }

    pub async fn enter_callback_wait(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        callback_id: Uuid,
    ) -> Result<bool, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let entered = self
            .enter_callback_wait_in_tx(&mut tx, job_id, run_lease, callback_id)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(entered)
    }

    /// Transaction-aware variant of [`Self::enter_callback_wait`] (ADR-029).
    /// Returns whether the row transitioned to `waiting_external`.
    pub async fn enter_callback_wait_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        job_id: i64,
        run_lease: i64,
        callback_id: Uuid,
    ) -> Result<bool, AwaError> {
        let result = sqlx::query(&format!(
            r#"
            UPDATE {}
            SET state = 'waiting_external',
                heartbeat_at = NULL,
                deadline_at = NULL
            WHERE job_id = $1
              AND state = 'running'
              AND run_lease = $2
              AND callback_id = $3
            "#,
            self.leases_table()
        ))
        .bind(job_id)
        .bind(run_lease)
        .bind(callback_id)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn check_callback_state(
        &self,
        pool: &PgPool,
        job_id: i64,
        callback_id: Uuid,
    ) -> Result<CallbackPollResult, AwaError> {
        let row: Option<(JobState, Option<Uuid>, i64, Option<serde_json::Value>)> =
            sqlx::query_as(&format!(
                r#"
                SELECT
                    lease.state,
                    lease.callback_id,
                    lease.run_lease,
                    attempt.callback_result
                FROM {} AS lease
                LEFT JOIN {} AS attempt
                  ON attempt.job_id = lease.job_id
                 AND attempt.run_lease = lease.run_lease
                WHERE lease.job_id = $1
                ORDER BY lease.run_lease DESC
                LIMIT 1
                "#,
                self.leases_table(),
                self.attempt_state_table()
            ))
            .bind(job_id)
            .fetch_optional(pool)
            .await
            .map_err(map_sqlx_error)?;

        match row {
            Some((JobState::Running, None, run_lease, Some(_))) => {
                let result = self.take_callback_result(pool, job_id, run_lease).await?;
                Ok(CallbackPollResult::Resolved(result))
            }
            Some((state, Some(current_callback_id), _, _))
                if current_callback_id != callback_id =>
            {
                Ok(CallbackPollResult::Stale {
                    token: callback_id,
                    current: current_callback_id,
                    state,
                })
            }
            Some((JobState::WaitingExternal, Some(current), _, _)) if current == callback_id => {
                Ok(CallbackPollResult::Pending)
            }
            Some((state, _, _, _)) => Ok(CallbackPollResult::UnexpectedState {
                token: callback_id,
                state,
            }),
            None => {
                if let Some(job) = self.load_job(pool, job_id).await? {
                    Ok(CallbackPollResult::UnexpectedState {
                        token: callback_id,
                        state: job.state,
                    })
                } else {
                    Ok(CallbackPollResult::NotFound)
                }
            }
        }
    }

    pub async fn callback_job(
        &self,
        pool: &PgPool,
        callback_id: Uuid,
        run_lease: Option<i64>,
    ) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self
            .callback_job_in_tx(&mut tx, callback_id, run_lease, false)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    /// Transaction-aware variant of [`Self::callback_job`] (ADR-029).
    /// When `for_update` is `true` the join's lease row is locked with
    /// `FOR UPDATE OF lease`, mirroring the canonical `resolve_callback`
    /// lookup that takes a row lock on `awa.jobs_hot` before evaluating
    /// the callback policy and committing the resulting transition.
    pub async fn callback_job_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        callback_id: Uuid,
        run_lease: Option<i64>,
        for_update: bool,
    ) -> Result<Option<JobRow>, AwaError> {
        let lock_clause = if for_update {
            "FOR UPDATE OF lease"
        } else {
            ""
        };
        let row: Option<LeaseJobRow> = sqlx::query_as(&format!(
            r#"
            SELECT
                lease.ready_slot,
                lease.ready_generation,
                lease.job_id,
                ready.kind,
                ready.queue,
                ready.args,
                lease.state,
                lease.priority,
                lease.attempt,
                lease.run_lease,
                lease.max_attempts,
                lease.lane_seq,
                ready.run_at,
                COALESCE(attempt.heartbeat_at, lease.heartbeat_at) AS heartbeat_at,
                lease.deadline_at,
                lease.attempted_at,
                NULL::timestamptz AS finalized_at,
                ready.created_at,
                ready.unique_key,
                ready.unique_states,
                lease.callback_id,
                lease.callback_timeout_at,
                attempt.callback_filter,
                attempt.callback_on_complete,
                attempt.callback_on_fail,
                attempt.callback_transform,
                COALESCE(ready.payload, '{{}}'::jsonb) AS payload,
                attempt.progress,
                attempt.callback_result
            FROM {} AS lease
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = lease.ready_slot
             AND ready.ready_generation = lease.ready_generation
             AND ready.queue = lease.queue
             AND ready.priority = lease.priority
             AND ready.enqueue_shard = lease.enqueue_shard
             AND ready.lane_seq = lease.lane_seq
            LEFT JOIN {schema}.attempt_state AS attempt
              ON attempt.job_id = lease.job_id
             AND attempt.run_lease = lease.run_lease
            WHERE lease.callback_id = $1
              AND lease.state IN ('waiting_external', 'running')
              AND ($2::bigint IS NULL OR lease.run_lease = $2)
            ORDER BY lease.run_lease DESC
            LIMIT 1
            {lock_clause}
            "#,
            self.leases_table(),
            schema = self.schema(),
        ))
        .bind(callback_id)
        .bind(run_lease)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        row.map(LeaseJobRow::into_job_row).transpose()
    }

    #[tracing::instrument(skip(self, pool, payload), name = "queue_storage.complete_external")]
    pub async fn complete_external(
        &self,
        pool: &PgPool,
        callback_id: Uuid,
        payload: Option<serde_json::Value>,
        run_lease: Option<i64>,
        resume: bool,
    ) -> Result<JobRow, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self
            .complete_external_in_tx(&mut tx, callback_id, payload, run_lease, resume)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    /// Transaction-aware variant of [`Self::complete_external`] (ADR-029).
    /// The non-resume path returns the post-completion `JobRow` directly
    /// from the `done_row` insert. The resume path returns the parked-row
    /// snapshot reloaded inside the same transaction via
    /// [`Self::load_active_lease_in_tx`] — i.e. it does not leave the
    /// caller's transaction to refresh state.
    pub async fn complete_external_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        callback_id: Uuid,
        payload: Option<serde_json::Value>,
        run_lease: Option<i64>,
        resume: bool,
    ) -> Result<JobRow, AwaError> {
        if resume {
            let resumed: Option<(i64, i64)> = sqlx::query_as(&format!(
                r#"
                UPDATE {}
                SET state = 'running',
                    callback_id = NULL,
                    callback_timeout_at = NULL,
                    heartbeat_at = clock_timestamp()
                WHERE callback_id = $1
                  AND state IN ('waiting_external', 'running')
                  AND ($2::bigint IS NULL OR run_lease = $2)
                RETURNING job_id, run_lease
                "#,
                self.leases_table()
            ))
            .bind(callback_id)
            .bind(run_lease)
            .fetch_optional(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            let Some((job_id, resumed_run_lease)) = resumed else {
                return Err(AwaError::CallbackNotFound {
                    callback_id: callback_id.to_string(),
                });
            };

            sqlx::query(&format!(
                r#"
                INSERT INTO {} (
                    job_id,
                    run_lease,
                    callback_filter,
                    callback_on_complete,
                    callback_on_fail,
                    callback_transform,
                    callback_result,
                    updated_at
                )
                VALUES ($1, $2, NULL, NULL, NULL, NULL, $3, clock_timestamp())
                ON CONFLICT (job_id, run_lease)
                DO UPDATE SET
                    callback_filter = NULL,
                    callback_on_complete = NULL,
                    callback_on_fail = NULL,
                    callback_transform = NULL,
                    callback_result = EXCLUDED.callback_result,
                    updated_at = clock_timestamp()
                "#,
                self.attempt_state_table()
            ))
            .bind(job_id)
            .bind(resumed_run_lease)
            .bind(payload.unwrap_or(serde_json::Value::Null))
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            return self
                .load_active_lease_in_tx(tx, job_id, resumed_run_lease)
                .await?
                .ok_or(AwaError::CallbackNotFound {
                    callback_id: callback_id.to_string(),
                });
        }

        let schema = self.schema();
        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE callback_id = $1
              AND state IN ('waiting_external', 'running')
              AND ($2::bigint IS NULL OR run_lease = $2)
            RETURNING
                ready_slot,
                ready_generation,
                job_id,
                queue,
                state,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                heartbeat_at,
                deadline_at,
                attempted_at,
                callback_id,
                callback_timeout_at
            "#
        ))
        .bind(callback_id)
        .bind(run_lease)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if deleted.is_empty() {
            return Err(AwaError::CallbackNotFound {
                callback_id: callback_id.to_string(),
            });
        }

        let completed_pairs: Vec<(i64, i64)> = deleted
            .iter()
            .map(|row| (row.job_id, row.run_lease))
            .collect();
        self.close_receipt_pairs_tx(tx, &completed_pairs, "completed")
            .await?;

        let moved = self.hydrate_deleted_leases_tx(tx, deleted).await?;
        let moved = moved.into_iter().next().expect("deleted callback lease");

        let mut payload = RuntimePayload::from_json(Self::payload_with_attempt_state(
            moved.payload.clone(),
            moved.progress.clone(),
        )?)?;
        payload.set_progress(None);
        let done_row =
            moved
                .clone()
                .into_done_row(JobState::Completed, Utc::now(), payload.into_json());
        self.insert_done_rows_tx(tx, std::slice::from_ref(&done_row), Some(moved.state))
            .await?;
        done_row.into_job_row()
    }

    pub async fn fail_external(
        &self,
        pool: &PgPool,
        callback_id: Uuid,
        error: &str,
        run_lease: Option<i64>,
    ) -> Result<JobRow, AwaError> {
        self.fail_external_with_error_entry(
            pool,
            callback_id,
            serde_json::json!({ "error": error }),
            run_lease,
        )
        .await
    }

    pub async fn fail_external_with_error_entry(
        &self,
        pool: &PgPool,
        callback_id: Uuid,
        error_entry: serde_json::Value,
        run_lease: Option<i64>,
    ) -> Result<JobRow, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self
            .fail_external_with_error_entry_in_tx(&mut tx, callback_id, error_entry, run_lease)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    /// Transaction-aware variant of [`Self::fail_external_with_error_entry`]
    /// (ADR-029).
    pub async fn fail_external_with_error_entry_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        callback_id: Uuid,
        error_entry: serde_json::Value,
        run_lease: Option<i64>,
    ) -> Result<JobRow, AwaError> {
        let schema = self.schema();
        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE callback_id = $1
              AND state IN ('waiting_external', 'running')
              AND ($2::bigint IS NULL OR run_lease = $2)
            RETURNING
                ready_slot,
                ready_generation,
                job_id,
                queue,
                state,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                heartbeat_at,
                deadline_at,
                attempted_at,
                callback_id,
                callback_timeout_at
            "#
        ))
        .bind(callback_id)
        .bind(run_lease)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if deleted.is_empty() {
            return Err(AwaError::CallbackNotFound {
                callback_id: callback_id.to_string(),
            });
        }

        let failed_pairs: Vec<(i64, i64)> = deleted
            .iter()
            .map(|row| (row.job_id, row.run_lease))
            .collect();
        self.close_receipt_pairs_tx(tx, &failed_pairs, "failed")
            .await?;

        let moved = self.hydrate_deleted_leases_tx(tx, deleted).await?;
        let moved = moved.into_iter().next().expect("deleted callback lease");

        let mut payload = RuntimePayload::from_json(Self::payload_with_attempt_state(
            moved.payload.clone(),
            moved.progress.clone(),
        )?)?;
        let mut error_entry = match error_entry {
            serde_json::Value::Object(map) => serde_json::Value::Object(map),
            other => serde_json::json!({ "error": other }),
        };
        let error_obj = error_entry
            .as_object_mut()
            .ok_or_else(|| AwaError::Validation("callback error entry must be an object".into()))?;
        error_obj
            .entry("attempt".to_string())
            .or_insert_with(|| serde_json::Value::from(i64::from(moved.attempt)));
        error_obj
            .entry("at".to_string())
            .or_insert_with(|| serde_json::Value::String(Utc::now().to_rfc3339()));
        error_obj
            .entry("terminal".to_string())
            .or_insert(serde_json::Value::Bool(true));
        payload.push_error(error_entry);
        let done_row =
            moved
                .clone()
                .into_done_row(JobState::Failed, Utc::now(), payload.into_json());
        self.insert_done_rows_tx(tx, std::slice::from_ref(&done_row), Some(moved.state))
            .await?;
        done_row.into_job_row()
    }

    pub async fn retry_external(
        &self,
        pool: &PgPool,
        callback_id: Uuid,
        run_lease: Option<i64>,
    ) -> Result<JobRow, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self
            .retry_external_in_tx(&mut tx, callback_id, run_lease)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    /// Transaction-aware variant of [`Self::retry_external`] (ADR-029).
    pub async fn retry_external_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        callback_id: Uuid,
        run_lease: Option<i64>,
    ) -> Result<JobRow, AwaError> {
        let schema = self.schema();
        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE callback_id = $1
              AND state = 'waiting_external'
              AND ($2::bigint IS NULL OR run_lease = $2)
            RETURNING
                ready_slot,
                ready_generation,
                job_id,
                queue,
                state,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                heartbeat_at,
                deadline_at,
                attempted_at,
                callback_id,
                callback_timeout_at
            "#
        ))
        .bind(callback_id)
        .bind(run_lease)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if deleted.is_empty() {
            return Err(AwaError::CallbackNotFound {
                callback_id: callback_id.to_string(),
            });
        }

        let retryable_pairs: Vec<(i64, i64)> = deleted
            .iter()
            .map(|row| (row.job_id, row.run_lease))
            .collect();
        self.close_receipt_pairs_tx(tx, &retryable_pairs, "retryable")
            .await?;

        let moved = self.hydrate_deleted_leases_tx(tx, deleted).await?;
        let moved = moved.into_iter().next().expect("deleted callback lease");

        let ready_payload =
            Self::payload_with_attempt_state(moved.payload.clone(), moved.progress.clone())?;

        let ready_row = ExistingReadyRow {
            attempt: 0,
            run_at: Utc::now(),
            ..moved.clone().into_ready_row(Utc::now(), ready_payload)
        };
        self.insert_existing_ready_rows_tx(tx, vec![ready_row.clone()], Some(moved.state))
            .await?;
        self.notify_queues_tx(tx, std::iter::once(moved.queue.clone()))
            .await?;
        ReadyJobRow {
            job_id: ready_row.job_id,
            kind: ready_row.kind,
            queue: ready_row.queue,
            args: ready_row.args,
            priority: ready_row.priority,
            attempt: ready_row.attempt,
            run_lease: ready_row.run_lease,
            max_attempts: ready_row.max_attempts,
            run_at: ready_row.run_at,
            attempted_at: ready_row.attempted_at,
            created_at: ready_row.created_at,
            unique_key: ready_row.unique_key,
            payload: ready_row.payload,
        }
        .into_job_row()
    }

    pub async fn heartbeat_callback(
        &self,
        pool: &PgPool,
        callback_id: Uuid,
        timeout: Duration,
    ) -> Result<JobRow, AwaError> {
        let updated: Option<(i64, i64)> = sqlx::query_as(&format!(
            r#"
            UPDATE {}
            SET callback_timeout_at = clock_timestamp() + make_interval(secs => $2)
            WHERE callback_id = $1
              AND state = 'waiting_external'
            RETURNING job_id, run_lease
            "#,
            self.leases_table()
        ))
        .bind(callback_id)
        .bind(timeout.as_secs_f64())
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;

        let Some((job_id, _run_lease)) = updated else {
            return Err(AwaError::CallbackNotFound {
                callback_id: callback_id.to_string(),
            });
        };

        self.load_job(pool, job_id)
            .await?
            .ok_or(AwaError::CallbackNotFound {
                callback_id: callback_id.to_string(),
            })
    }

    pub async fn flush_progress(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        progress: serde_json::Value,
    ) -> Result<(), AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        if self.lease_claim_receipts() {
            self.upsert_attempt_state_progress_from_receipts_tx(
                &mut tx,
                &[(job_id, run_lease, progress.clone())],
            )
            .await?;
        }
        sqlx::query(&format!(
            r#"
            INSERT INTO {} (job_id, run_lease, progress, updated_at)
            SELECT lease.job_id, lease.run_lease, $3, clock_timestamp()
            FROM {} AS lease
            WHERE lease.job_id = $1
              AND lease.run_lease = $2
              AND lease.state IN ('running', 'waiting_external')
            ON CONFLICT (job_id, run_lease)
            DO UPDATE SET
                progress = EXCLUDED.progress,
                updated_at = clock_timestamp()
            "#,
            self.attempt_state_table(),
            self.leases_table()
        ))
        .bind(job_id)
        .bind(run_lease)
        .bind(progress)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    pub async fn heartbeat_batch(
        &self,
        pool: &PgPool,
        jobs: &[(i64, i64)],
    ) -> Result<usize, AwaError> {
        if jobs.is_empty() {
            return Ok(0);
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let mut updated = 0_usize;
        if self.lease_claim_receipts() {
            // #169 B1: in receipts mode, attempt_state is the
            // authoritative heartbeat home. Skip the `UPDATE leases SET
            // heartbeat_at = ...` entirely — that write was the
            // dominant per-heartbeat non-HOT update on the partitioned
            // `leases` table (every state-indexed partial index pays 2
            // dead entries per write under sustained churn). Compat
            // reads in `LeaseJobRow` SELECTs and the `awa.jobs` view
            // COALESCE attempt_state.heartbeat_at first.
            updated += self
                .upsert_attempt_state_from_receipts_tx(&mut tx, jobs)
                .await?;
        } else {
            // Legacy non-receipts mode (custom schemas with
            // `lease_claim_receipts=FALSE`): the `leases.heartbeat_at`
            // write is still the heartbeat home, since there is no
            // upsert_attempt_state path firing.
            let job_ids: Vec<i64> = jobs.iter().map(|(job_id, _)| *job_id).collect();
            let run_leases: Vec<i64> = jobs.iter().map(|(_, run_lease)| *run_lease).collect();
            let result = sqlx::query(&format!(
                r#"
                WITH inflight AS (
                    SELECT * FROM unnest($1::bigint[], $2::bigint[]) AS v(job_id, run_lease)
                )
                UPDATE {table}
                SET heartbeat_at = clock_timestamp()
                FROM inflight
                WHERE {table}.job_id = inflight.job_id
                  AND {table}.run_lease = inflight.run_lease
                  AND {table}.state = 'running'
                "#,
                table = self.leases_table(),
            ))
            .bind(&job_ids)
            .bind(&run_leases)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
            updated += result.rows_affected() as usize;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(updated)
    }

    pub async fn heartbeat_progress_batch(
        &self,
        pool: &PgPool,
        jobs: &[(i64, i64, serde_json::Value)],
    ) -> Result<usize, AwaError> {
        if jobs.is_empty() {
            return Ok(0);
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let updated = if self.lease_claim_receipts() {
            // #169 B1: receipts mode is the only supported shape for
            // the default `awa` schema. attempt_state already carries
            // heartbeat_at + progress, so the `UPDATE leases SET
            // heartbeat_at` + nested `INSERT INTO attempt_state` CTE
            // is collapsed into the single attempt_state upsert. The
            // upsert sources open-claim identity from `lease_claims`
            // anti-joined against durable closure evidence so it picks
            // up every open claim regardless of whether
            // `materialize_claims` has fanned it out to `leases` yet.
            self.upsert_attempt_state_progress_from_receipts_tx(&mut tx, jobs)
                .await?
        } else {
            // Legacy non-receipts mode keeps the old CTE shape that
            // updates leases.heartbeat_at + leases.progress
            // (via attempt_state upsert) in a single round-trip.
            let schema = self.schema();
            let job_ids: Vec<i64> = jobs.iter().map(|(job_id, _, _)| *job_id).collect();
            let run_leases: Vec<i64> = jobs.iter().map(|(_, run_lease, _)| *run_lease).collect();
            let progress: Vec<serde_json::Value> =
                jobs.iter().map(|(_, _, value)| value.clone()).collect();
            let lease_updated: i64 = sqlx::query_scalar(&format!(
                r#"
                WITH inflight AS (
                    SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::jsonb[]) AS v(job_id, run_lease, progress)
                ),
                updated AS (
                    UPDATE {table} AS lease
                    SET heartbeat_at = clock_timestamp()
                    FROM inflight
                    WHERE lease.job_id = inflight.job_id
                      AND lease.run_lease = inflight.run_lease
                      AND lease.state = 'running'
                    RETURNING lease.job_id, lease.run_lease, inflight.progress
                ),
                upsert_attempt AS (
                    INSERT INTO {schema}.attempt_state (job_id, run_lease, progress, updated_at)
                    SELECT job_id, run_lease, progress, clock_timestamp()
                    FROM updated
                    ON CONFLICT (job_id, run_lease)
                    DO UPDATE SET
                        progress = EXCLUDED.progress,
                        updated_at = clock_timestamp()
                )
                SELECT count(*)::bigint FROM updated
                "#,
                table = self.leases_table(),
            ))
            .bind(&job_ids)
            .bind(&run_leases)
            .bind(&progress)
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
            lease_updated as usize
        };
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(updated)
    }

    pub async fn retry_after(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        retry_after: Duration,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self
            .retry_after_in_tx(&mut tx, job_id, run_lease, retry_after, progress)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    /// Transaction-aware variant of [`Self::retry_after`]. Caller owns the
    /// transaction lifecycle so the move can commit atomically alongside
    /// follow-up `INSERT`s (ADR-029).
    pub async fn retry_after_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        job_id: i64,
        run_lease: i64,
        retry_after: Duration,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let Some(moved) = self
            .take_running_attempt_tx(tx, job_id, run_lease, "retryable")
            .await?
        else {
            return Ok(None);
        };
        let now = self.current_timestamp_tx(tx).await?;

        let payload =
            Self::with_progress(moved.payload.clone(), progress.or(moved.progress.clone()))?;
        let deferred = moved.clone().into_deferred_row(
            JobState::Retryable,
            now + TimeDelta::from_std(retry_after).map_err(|err| {
                AwaError::Validation(format!("invalid retry_after duration: {err}"))
            })?,
            Some(now),
            payload,
        );
        self.insert_deferred_rows_tx(tx, vec![deferred.clone()], Some(moved.state))
            .await?;
        Ok(Some(deferred.into_job_row()?))
    }

    pub async fn snooze(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        snooze_for: Duration,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let Some(moved) = self
            .take_running_attempt_tx(&mut tx, job_id, run_lease, "scheduled")
            .await?
        else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };
        let now = self.current_timestamp_tx(&mut tx).await?;

        let payload =
            Self::with_progress(moved.payload.clone(), progress.or(moved.progress.clone()))?;
        let mut deferred = moved.clone().into_deferred_row(
            JobState::Scheduled,
            now + TimeDelta::from_std(snooze_for)
                .map_err(|err| AwaError::Validation(format!("invalid snooze duration: {err}")))?,
            None,
            payload,
        );
        deferred.attempt = deferred.attempt.saturating_sub(1);
        self.insert_deferred_rows_tx(&mut tx, vec![deferred.clone()], Some(moved.state))
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(deferred.into_job_row()?))
    }

    /// Rescue-path fallback for unique-claim conflicts: insert the rescued
    /// attempt's deferred successor, degrading to a cancelled terminal row
    /// when the job's unique claim is held by a newer duplicate — the claim
    /// holder wins, and one poisoned row must not abort the whole batched
    /// rescue transaction. The insert attempt runs inside a savepoint so the
    /// conflict leaves the outer transaction usable.
    async fn insert_rescued_deferred_or_cancel_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        row: LeaseTransitionRow,
        deferred: DeferredJobRow,
        duplicate_error: &str,
    ) -> Result<JobRow, AwaError> {
        let old_state = row.state;
        {
            let mut savepoint = tx.begin().await.map_err(map_sqlx_error)?;
            match self
                .insert_deferred_rows_tx(&mut savepoint, vec![deferred.clone()], Some(old_state))
                .await
            {
                Ok(_) => {
                    savepoint.commit().await.map_err(map_sqlx_error)?;
                    return deferred.into_job_row();
                }
                Err(AwaError::UniqueConflict { .. }) => {
                    savepoint.rollback().await.map_err(map_sqlx_error)?;
                }
                Err(err) => return Err(err),
            }
        }
        self.cancel_rescued_duplicate_tx(tx, row, deferred.payload, duplicate_error)
            .await
    }

    /// Same fallback for rescue arms that write a terminal row directly
    /// (callback timeout at max attempts): a `failed` insert that conflicts
    /// with a newer duplicate's claim becomes a cancellation instead.
    async fn insert_rescued_done_or_cancel_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        row: LeaseTransitionRow,
        done: DoneJobRow,
        duplicate_error: &str,
    ) -> Result<JobRow, AwaError> {
        let old_state = row.state;
        {
            let mut savepoint = tx.begin().await.map_err(map_sqlx_error)?;
            match self
                .insert_done_rows_tx(&mut savepoint, std::slice::from_ref(&done), Some(old_state))
                .await
            {
                Ok(_) => {
                    savepoint.commit().await.map_err(map_sqlx_error)?;
                    return done.into_job_row();
                }
                Err(AwaError::UniqueConflict { .. }) => {
                    savepoint.rollback().await.map_err(map_sqlx_error)?;
                }
                Err(err) => return Err(err),
            }
        }
        self.cancel_rescued_duplicate_tx(tx, row, done.payload, duplicate_error)
            .await
    }

    /// Write the cancelled terminal row for a rescue that lost its unique
    /// claim to a newer duplicate. `cancelled` normally sits outside the
    /// claiming mask so the insert is conflict-free; a mask that claims
    /// `cancelled` as well conflicts again, and then the row is written with
    /// its unique key stripped — the attempt held no claim (that is what
    /// made it conflict), so there is nothing to release, and losing the key
    /// on this terminal row beats aborting rescue for the whole cluster.
    async fn cancel_rescued_duplicate_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        row: LeaseTransitionRow,
        base_payload: serde_json::Value,
        duplicate_error: &str,
    ) -> Result<JobRow, AwaError> {
        let old_state = row.state;
        let mut payload = RuntimePayload::from_json(base_payload)?;
        payload.push_error(lifecycle_error(duplicate_error, row.attempt, true));
        let done = row.into_done_row(JobState::Cancelled, Utc::now(), payload.into_json());

        {
            let mut savepoint = tx.begin().await.map_err(map_sqlx_error)?;
            match self
                .insert_done_rows_tx(&mut savepoint, std::slice::from_ref(&done), Some(old_state))
                .await
            {
                Ok(_) => {
                    savepoint.commit().await.map_err(map_sqlx_error)?;
                    return done.into_job_row();
                }
                Err(AwaError::UniqueConflict { .. }) => {
                    savepoint.rollback().await.map_err(map_sqlx_error)?;
                }
                Err(err) => return Err(err),
            }
        }

        let mut stripped = done;
        stripped.unique_key = None;
        stripped.unique_states = None;
        self.insert_done_rows_tx(tx, std::slice::from_ref(&stripped), Some(old_state))
            .await?;
        stripped.into_job_row()
    }

    pub async fn cancel_running(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        reason: &str,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self
            .cancel_running_in_tx(&mut tx, job_id, run_lease, reason, progress)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    /// Transaction-aware variant of [`Self::cancel_running`] (ADR-029).
    pub async fn cancel_running_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        job_id: i64,
        run_lease: i64,
        reason: &str,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let Some(moved) = self
            .take_running_attempt_tx(tx, job_id, run_lease, "cancelled")
            .await?
        else {
            return Ok(None);
        };

        let mut payload = RuntimePayload::from_json(Self::with_progress(
            moved.payload.clone(),
            progress.or(moved.progress.clone()),
        )?)?;
        payload.push_error(lifecycle_error(
            format!("cancelled: {reason}"),
            moved.attempt,
            false,
        ));
        let done =
            moved
                .clone()
                .into_done_row(JobState::Cancelled, Utc::now(), payload.into_json());
        self.insert_done_rows_tx(tx, std::slice::from_ref(&done), Some(moved.state))
            .await?;
        Ok(Some(done.into_job_row()?))
    }

    pub async fn fail_terminal(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        error: &str,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self
            .fail_terminal_in_tx(&mut tx, job_id, run_lease, error, progress)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    /// Transaction-aware variant of [`Self::fail_terminal`] (ADR-029).
    pub async fn fail_terminal_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        job_id: i64,
        run_lease: i64,
        error: &str,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let Some(moved) = self
            .take_running_attempt_tx(tx, job_id, run_lease, "failed")
            .await?
        else {
            return Ok(None);
        };

        let mut payload = RuntimePayload::from_json(Self::with_progress(
            moved.payload.clone(),
            progress.or(moved.progress.clone()),
        )?)?;
        payload.push_error(lifecycle_error(error, moved.attempt, true));
        let done = moved
            .clone()
            .into_done_row(JobState::Failed, Utc::now(), payload.into_json());
        self.insert_done_rows_tx(tx, std::slice::from_ref(&done), Some(moved.state))
            .await?;
        Ok(Some(done.into_job_row()?))
    }

    pub async fn fail_to_dlq(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        dlq_reason: &str,
        error: &str,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self
            .fail_to_dlq_in_tx(&mut tx, job_id, run_lease, dlq_reason, error, progress)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    /// Transaction-aware variant of [`Self::fail_to_dlq`] (ADR-029).
    pub async fn fail_to_dlq_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        job_id: i64,
        run_lease: i64,
        dlq_reason: &str,
        error: &str,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let Some(moved) = self
            .take_running_attempt_tx(tx, job_id, run_lease, "dlq")
            .await?
        else {
            return Ok(None);
        };

        let finalized_at = Utc::now();
        let dlq_at = finalized_at;
        let mut payload = RuntimePayload::from_json(Self::with_progress(
            moved.payload.clone(),
            progress.or(moved.progress.clone()),
        )?)?;
        payload.push_error(lifecycle_error(error, moved.attempt, true));
        let dlq_row = moved.clone().into_dlq_row(
            finalized_at,
            payload.into_json(),
            dlq_reason.to_string(),
            dlq_at,
        );
        self.insert_dlq_rows_tx(tx, std::slice::from_ref(&dlq_row), Some(moved.state))
            .await?;
        Ok(Some(dlq_row.into_job_row()?))
    }

    pub async fn move_failed_to_dlq(
        &self,
        pool: &PgPool,
        job_id: i64,
        dlq_reason: &str,
    ) -> Result<Option<JobRow>, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let done_projection = done_row_projection("done", "ready");
        let ready_join = done_ready_join(schema, "done", "ready");
        let moved: Option<DoneJobRow> = sqlx::query_as(&format!(
            r#"
            WITH deleted AS (
                DELETE FROM {schema}.done_entries
                WHERE (job_id, finalized_at) IN (
                    SELECT job_id, finalized_at
                    FROM {schema}.done_entries
                    WHERE job_id = $1
                      AND state = 'failed'
                    ORDER BY finalized_at DESC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING *
            )
            SELECT {done_projection}
            FROM deleted AS done
            {ready_join}
            "#
        ))
        .bind(job_id)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(moved) = moved else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };

        self.ensure_terminal_removed_receipt_closures_tx(&mut tx, std::slice::from_ref(&moved))
            .await?;
        // DLQ inserts are outside done_entries, so the source terminal DELETE
        // appends a negative delta.
        self.decrement_live_terminal_counters_tx(
            &mut tx,
            &Self::done_rows_to_counter_keys(std::slice::from_ref(&moved)),
        )
        .await?;
        let dlq_row = moved
            .clone()
            .into_dlq_row(dlq_reason.to_string(), Utc::now());
        self.insert_dlq_rows_tx(&mut tx, std::slice::from_ref(&dlq_row), Some(moved.state))
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(dlq_row.into_job_row()?))
    }

    #[tracing::instrument(
        skip(self, pool, dlq_reason),
        fields(kind = ?kind, queue = ?queue),
        name = "queue_storage.bulk_move_failed_to_dlq"
    )]
    pub async fn bulk_move_failed_to_dlq(
        &self,
        pool: &PgPool,
        kind: Option<&str>,
        queue: Option<&str>,
        dlq_reason: &str,
    ) -> Result<u64, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let done_projection = done_row_projection("done", "ready");
        let ready_join = done_ready_join(schema, "done", "ready");
        let moved: Vec<DoneJobRow> = sqlx::query_as(&format!(
            r#"
            WITH deleted AS (
                DELETE FROM {schema}.done_entries
                WHERE state = 'failed'
                  AND ($1::text IS NULL OR kind = $1)
                  AND ($2::text IS NULL OR queue = $2)
                RETURNING *
            )
            SELECT {done_projection}
            FROM deleted AS done
            {ready_join}
            "#
        ))
        .bind(kind)
        .bind(queue)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if moved.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(0);
        }

        self.ensure_terminal_removed_receipt_closures_tx(&mut tx, &moved)
            .await?;
        // Bulk DLQ move deletes from done_entries. Append negative deltas by
        // the per-group magnitudes of the moved rows.
        self.decrement_live_terminal_counters_tx(&mut tx, &Self::done_rows_to_counter_keys(&moved))
            .await?;
        let dlq_at = Utc::now();
        let rows: Vec<DlqJobRow> = moved
            .into_iter()
            .map(|row| row.into_dlq_row(dlq_reason.to_string(), dlq_at))
            .collect();
        self.insert_dlq_rows_tx(&mut tx, &rows, Some(JobState::Failed))
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(rows.len() as u64)
    }

    pub async fn retry_from_dlq(
        &self,
        pool: &PgPool,
        job_id: i64,
        opts: &RetryFromDlqOpts,
    ) -> Result<Option<JobRow>, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Option<DlqJobRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.dlq_entries
            WHERE job_id = $1
            RETURNING
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
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload,
                dlq_reason,
                dlq_at,
                original_run_lease
            "#
        ))
        .bind(job_id)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(moved) = moved else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };

        let queue = opts.queue.clone().unwrap_or_else(|| moved.queue.clone());
        let priority = opts.priority.unwrap_or(moved.priority);
        let mut payload = RuntimePayload::from_json(moved.payload.clone())?;
        payload.set_progress(None);
        let payload = payload.into_json();

        if let Some(run_at) = opts.run_at.filter(|run_at| *run_at > Utc::now()) {
            let deferred = moved.into_retry_deferred_row(queue, priority, run_at, payload);
            self.insert_deferred_rows_tx(&mut tx, vec![deferred.clone()], Some(JobState::Failed))
                .await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Some(deferred.into_job_row()?));
        }

        let ready = moved.into_retry_ready_row(queue.clone(), priority, Utc::now(), payload);
        self.insert_existing_ready_rows_tx(&mut tx, vec![ready.clone()], Some(JobState::Failed))
            .await?;
        self.notify_queues_tx(&mut tx, std::iter::once(queue))
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(
            ReadyJobRow {
                job_id: ready.job_id,
                kind: ready.kind,
                queue: ready.queue,
                args: ready.args,
                priority: ready.priority,
                attempt: ready.attempt,
                run_lease: ready.run_lease,
                max_attempts: ready.max_attempts,
                run_at: ready.run_at,
                attempted_at: ready.attempted_at,
                created_at: ready.created_at,
                unique_key: ready.unique_key,
                payload: ready.payload,
            }
            .into_job_row()?,
        ))
    }

    #[tracing::instrument(
        skip(self, pool, filter),
        fields(kind = ?filter.kind, queue = ?filter.queue, tag = ?filter.tag),
        name = "queue_storage.bulk_retry_from_dlq"
    )]
    pub async fn bulk_retry_from_dlq(
        &self,
        pool: &PgPool,
        filter: &ListDlqFilter,
    ) -> Result<u64, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Vec<DlqJobRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.dlq_entries
            WHERE ($1::text IS NULL OR kind = $1)
              AND ($2::text IS NULL OR queue = $2)
              AND ($3::text IS NULL OR payload -> 'tags' ? $3)
              AND (
                  ($4::bigint IS NULL AND $5::timestamptz IS NULL)
                  OR ($4::bigint IS NOT NULL AND $5::timestamptz IS NULL AND job_id < $4)
                  OR ($4::bigint IS NULL AND $5::timestamptz IS NOT NULL AND dlq_at < $5)
                  OR (
                      $4::bigint IS NOT NULL
                      AND $5::timestamptz IS NOT NULL
                      AND (dlq_at, job_id) < ($5, $4)
                  )
              )
            RETURNING
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
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload,
                dlq_reason,
                dlq_at,
                original_run_lease
            "#
        ))
        .bind(&filter.kind)
        .bind(&filter.queue)
        .bind(&filter.tag)
        .bind(filter.before_id)
        .bind(filter.before_dlq_at)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if moved.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(0);
        }

        let run_at = Utc::now();
        let mut queues = BTreeSet::new();
        let mut ready_rows = Vec::with_capacity(moved.len());
        for moved_row in moved {
            let queue = moved_row.queue.clone();
            let priority = moved_row.priority;
            queues.insert(queue.clone());
            let mut payload = RuntimePayload::from_json(moved_row.payload.clone())?;
            payload.set_progress(None);
            ready_rows.push(moved_row.into_retry_ready_row(
                queue,
                priority,
                run_at,
                payload.into_json(),
            ));
        }

        let revived = ready_rows.len() as u64;
        self.insert_existing_ready_rows_tx(&mut tx, ready_rows, Some(JobState::Failed))
            .await?;
        self.notify_queues_tx(&mut tx, queues).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(revived)
    }

    pub async fn discard_failed_by_kind(&self, pool: &PgPool, kind: &str) -> Result<u64, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        let done_projection = done_row_projection("done", "ready");
        let ready_join = done_ready_join(schema, "done", "ready");
        let deleted_done: Vec<DoneJobRow> = sqlx::query_as(&format!(
            r#"
            WITH deleted AS (
                DELETE FROM {schema}.done_entries
                WHERE kind = $1
                  AND state = 'failed'
                RETURNING *
            )
            SELECT {done_projection}
            FROM deleted AS done
            {ready_join}
            "#
        ))
        .bind(kind)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let deleted_dlq: Vec<DlqJobRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.dlq_entries
            WHERE kind = $1
            RETURNING
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
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload,
                dlq_reason,
                dlq_at,
                original_run_lease
            "#
        ))
        .bind(kind)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        // Discard removes terminal `failed` rows from done_entries (and
        // `failed` DLQ rows from dlq_entries — exact terminal counts are keyed
        // on done_entries only). Append negative deltas by the per-group
        // magnitudes of `deleted_done`.
        self.ensure_terminal_removed_receipt_closures_tx(&mut tx, &deleted_done)
            .await?;
        self.decrement_live_terminal_counters_tx(
            &mut tx,
            &Self::done_rows_to_counter_keys(&deleted_done),
        )
        .await?;

        for row in &deleted_done {
            self.sync_unique_claim(
                &mut tx,
                row.job_id,
                &row.unique_key,
                row.unique_states.as_deref(),
                Some(row.state),
                None,
            )
            .await?;
        }

        for row in &deleted_dlq {
            self.sync_unique_claim(
                &mut tx,
                row.job_id,
                &row.unique_key,
                row.unique_states.as_deref(),
                Some(row.state),
                None,
            )
            .await?;
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok((deleted_done.len() + deleted_dlq.len()) as u64)
    }

    pub async fn fail_retryable(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        error: &str,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let result = self
            .fail_retryable_in_tx(&mut tx, job_id, run_lease, error, progress)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(result)
    }

    /// Transaction-aware variant of [`Self::fail_retryable`] (ADR-029).
    pub async fn fail_retryable_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        job_id: i64,
        run_lease: i64,
        error: &str,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let Some(moved) = self
            .take_running_attempt_tx(tx, job_id, run_lease, "retryable")
            .await?
        else {
            return Ok(None);
        };

        let mut payload = RuntimePayload::from_json(Self::with_progress(
            moved.payload.clone(),
            progress.or(moved.progress.clone()),
        )?)?;
        let exhausted = moved.attempt >= moved.max_attempts;
        payload.push_error(lifecycle_error(error, moved.attempt, exhausted));

        if exhausted {
            let done =
                moved
                    .clone()
                    .into_done_row(JobState::Failed, Utc::now(), payload.into_json());
            self.insert_done_rows_tx(tx, std::slice::from_ref(&done), Some(moved.state))
                .await?;
            return Ok(Some(done.into_job_row()?));
        }

        let deferred = moved.clone().into_deferred_row(
            JobState::Retryable,
            self.backoff_at_tx(tx, moved.attempt, moved.max_attempts)
                .await?,
            Some(Utc::now()),
            payload.into_json(),
        );
        self.insert_deferred_rows_tx(tx, vec![deferred.clone()], Some(moved.state))
            .await?;
        Ok(Some(deferred.into_job_row()?))
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.rescue_stale_heartbeats")]
    pub async fn rescue_stale_heartbeats(
        &self,
        pool: &PgPool,
        staleness: Duration,
    ) -> Result<Vec<JobRow>, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let cutoff = Utc::now()
            - TimeDelta::from_std(staleness)
                .map_err(|err| AwaError::Validation(format!("invalid staleness: {err}")))?;
        // #169 B1: the staleness predicate prefers
        // `attempt_state.heartbeat_at` (receipts-mode source of truth,
        // where heartbeat_batch writes go) and falls back to
        // `leases.heartbeat_at` (legacy non-receipts source). Two
        // cases this covers:
        //
        //   * Receipts mode, claim materialized into `leases` via
        //     callback registration / progress upsert / equivalent.
        //     The leases row was written once at materialize time and
        //     its `heartbeat_at` is stale by definition. The fresh
        //     value lives on `attempt_state.heartbeat_at`. COALESCE
        //     picks `attempt` so a healthy worker isn't falsely
        //     rescued — and a dead worker IS rescued, which the
        //     receipt-side path can't do (its anti-join below
        //     excludes materialized leases to avoid double-closure).
        //   * Legacy non-receipts mode: attempt_state.heartbeat_at is
        //     never written; COALESCE falls back to
        //     leases.heartbeat_at — same shape as pre-B1.
        //
        // The dropped `idx_state_hb` (v025) doesn't hurt this scan:
        // the planner can satisfy the `state='running'` prefix via the
        // surviving `(state, deadline_at)` or
        // `(state, callback_timeout_at)` indexes, followed by a heap
        // recheck of the COALESCE. Bounded by running-lease count and
        // called at 30s cadence — cheap.
        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases AS target
            WHERE (target.job_id, target.run_lease) IN (
                SELECT lease.job_id, lease.run_lease
                FROM {schema}.leases AS lease
                LEFT JOIN {schema}.attempt_state AS attempt
                  ON attempt.job_id = lease.job_id
                 AND attempt.run_lease = lease.run_lease
                WHERE lease.state = 'running'
                  AND COALESCE(attempt.heartbeat_at, lease.heartbeat_at) < $1
                ORDER BY COALESCE(attempt.heartbeat_at, lease.heartbeat_at) ASC
                LIMIT 500
                FOR UPDATE OF lease SKIP LOCKED
            )
            RETURNING
                ready_slot,
                ready_generation,
                job_id,
                queue,
                state,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                heartbeat_at,
                deadline_at,
                attempted_at,
                callback_id,
                callback_timeout_at
            "#
        ))
        .bind(cutoff)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let rescued_receipts = if self.lease_claim_receipts() {
            self.rescue_stale_receipt_claims_tx(&mut tx, cutoff).await?
        } else {
            Vec::new()
        };

        if deleted.is_empty() && rescued_receipts.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let moved_leases = self.hydrate_deleted_leases_tx(&mut tx, deleted).await?;
        let moved_receipts = self
            .hydrate_deleted_leases_tx(&mut tx, rescued_receipts)
            .await?;

        let mut rescued = Vec::with_capacity(moved_leases.len() + moved_receipts.len());
        for row in moved_leases {
            let mut payload = RuntimePayload::from_json(Self::payload_with_attempt_state(
                row.payload.clone(),
                row.progress.clone(),
            )?)?;
            payload.push_error(lifecycle_error(
                "heartbeat stale: worker presumed dead",
                row.attempt,
                false,
            ));
            let deferred = row.clone().into_deferred_row(
                JobState::Retryable,
                self.backoff_at_tx(&mut tx, row.attempt, row.max_attempts)
                    .await?,
                Some(Utc::now()),
                payload.into_json(),
            );
            let rescued_row = self
                .insert_rescued_deferred_or_cancel_tx(
                    &mut tx,
                    row,
                    deferred,
                    "rescued as duplicate: unique claim held by a newer job",
                )
                .await?;
            rescued.push(rescued_row);
        }
        for row in moved_receipts {
            let mut payload = RuntimePayload::from_json(Self::payload_with_attempt_state(
                row.payload.clone(),
                row.progress.clone(),
            )?)?;
            payload.push_error(lifecycle_error(
                "receipt claim stale: worker presumed dead",
                row.attempt,
                false,
            ));
            let deferred = row.clone().into_deferred_row(
                JobState::Retryable,
                self.backoff_at_tx(&mut tx, row.attempt, row.max_attempts)
                    .await?,
                Some(Utc::now()),
                payload.into_json(),
            );
            let rescued_row = self
                .insert_rescued_deferred_or_cancel_tx(
                    &mut tx,
                    row,
                    deferred,
                    "rescued as duplicate: unique claim held by a newer job",
                )
                .await?;
            rescued.push(rescued_row);
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(rescued)
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.rescue_expired_deadlines")]
    pub async fn rescue_expired_deadlines(&self, pool: &PgPool) -> Result<Vec<JobRow>, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE job_id IN (
                SELECT job_id
                FROM {schema}.leases
                WHERE state = 'running'
                  AND deadline_at IS NOT NULL
                  AND deadline_at < clock_timestamp()
                ORDER BY deadline_at ASC
                LIMIT 500
                FOR UPDATE SKIP LOCKED
            )
            RETURNING
                ready_slot,
                ready_generation,
                job_id,
                queue,
                state,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                heartbeat_at,
                deadline_at,
                attempted_at,
                callback_id,
                callback_timeout_at
            "#
        ))
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        // Receipts-mode short-path claims hold their deadline on
        // `lease_claim_batches.deadline_at` (compact batches, #246) or on
        // legacy row-local `lease_claims.deadline_at` rather than on a
        // `leases` row, so the receipt-plane needs its own scan; merge
        // all populations into one `moved` set so the maintenance caller
        // observes a single rescue batch per tick.
        let receipt_deleted = if self.lease_claim_receipts() {
            self.rescue_expired_receipt_deadlines_tx(&mut tx).await?
        } else {
            Vec::new()
        };

        if deleted.is_empty() && receipt_deleted.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let mut moved = self.hydrate_deleted_leases_tx(&mut tx, deleted).await?;
        moved.extend(
            self.hydrate_deleted_leases_tx(&mut tx, receipt_deleted)
                .await?,
        );

        let mut rescued = Vec::with_capacity(moved.len());
        for row in moved {
            let mut payload = RuntimePayload::from_json(Self::payload_with_attempt_state(
                row.payload.clone(),
                row.progress.clone(),
            )?)?;
            payload.push_error(lifecycle_error(
                "hard deadline exceeded",
                row.attempt,
                false,
            ));
            let deferred = row.clone().into_deferred_row(
                JobState::Retryable,
                self.backoff_at_tx(&mut tx, row.attempt, row.max_attempts)
                    .await?,
                Some(Utc::now()),
                payload.into_json(),
            );
            let rescued_row = self
                .insert_rescued_deferred_or_cancel_tx(
                    &mut tx,
                    row,
                    deferred,
                    "rescued as duplicate: unique claim held by a newer job",
                )
                .await?;
            rescued.push(rescued_row);
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(rescued)
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.rescue_expired_callbacks")]
    pub async fn rescue_expired_callbacks(&self, pool: &PgPool) -> Result<Vec<JobRow>, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE job_id IN (
                SELECT job_id
                FROM {schema}.leases
                WHERE state = 'waiting_external'
                  AND callback_timeout_at IS NOT NULL
                  AND callback_timeout_at < clock_timestamp()
                ORDER BY callback_timeout_at ASC
                LIMIT 500
                FOR UPDATE SKIP LOCKED
            )
            RETURNING
                ready_slot,
                ready_generation,
                job_id,
                queue,
                state,
                priority,
                attempt,
                run_lease,
                max_attempts,
                lane_seq,
                enqueue_shard,
                heartbeat_at,
                deadline_at,
                attempted_at,
                callback_id,
                callback_timeout_at
            "#
        ))
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if deleted.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let moved = self.hydrate_deleted_leases_tx(&mut tx, deleted).await?;

        let mut rescued = Vec::with_capacity(moved.len());
        for row in moved {
            let mut payload = RuntimePayload::from_json(Self::payload_with_attempt_state(
                row.payload.clone(),
                row.progress.clone(),
            )?)?;
            let exhausted = row.attempt >= row.max_attempts;
            payload.push_error(lifecycle_error(
                "callback timed out",
                row.attempt,
                exhausted,
            ));
            if exhausted {
                let done =
                    row.clone()
                        .into_done_row(JobState::Failed, Utc::now(), payload.into_json());
                let rescued_row = self
                    .insert_rescued_done_or_cancel_tx(
                        &mut tx,
                        row,
                        done,
                        "rescued as duplicate: unique claim held by a newer job",
                    )
                    .await?;
                rescued.push(rescued_row);
            } else {
                let deferred = row.clone().into_deferred_row(
                    JobState::Retryable,
                    self.backoff_at_tx(&mut tx, row.attempt, row.max_attempts)
                        .await?,
                    Some(Utc::now()),
                    payload.into_json(),
                );
                let rescued_row = self
                    .insert_rescued_deferred_or_cancel_tx(
                        &mut tx,
                        row,
                        deferred,
                        "rescued as duplicate: unique claim held by a newer job",
                    )
                    .await?;
                rescued.push(rescued_row);
            }
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(rescued)
    }

    pub async fn promote_due(
        &self,
        pool: &PgPool,
        state: JobState,
        batch_size: i64,
    ) -> Result<usize, AwaError> {
        if !matches!(state, JobState::Scheduled | JobState::Retryable) || batch_size <= 0 {
            return Ok(0);
        }

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Vec<DeferredJobRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.deferred_jobs
            WHERE job_id IN (
                SELECT job_id
                FROM {schema}.deferred_jobs
                WHERE state = $1
                  AND run_at <= clock_timestamp()
                  AND NOT EXISTS (
                      SELECT 1 FROM awa.queue_meta
                      WHERE queue = {schema}.deferred_jobs.queue AND paused = TRUE
                  )
                ORDER BY run_at ASC, priority ASC, job_id ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            RETURNING
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
                unique_states,
                COALESCE(payload, '{{}}'::jsonb) AS payload
            "#
        ))
        .bind(state)
        .bind(batch_size)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if moved.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(0);
        }

        let ready_rows: Vec<ExistingReadyRow> = moved
            .iter()
            .cloned()
            .map(|row| ExistingReadyRow {
                job_id: row.job_id,
                kind: row.kind,
                queue: row.queue,
                args: row.args,
                priority: row.priority,
                attempt: row.attempt,
                run_lease: row.run_lease,
                max_attempts: row.max_attempts,
                run_at: Utc::now(),
                attempted_at: row.attempted_at,
                created_at: row.created_at,
                unique_key: row.unique_key,
                unique_states: row.unique_states,
                payload: row.payload,
            })
            .collect();
        let queues = ready_rows
            .iter()
            .map(|row| row.queue.clone())
            .collect::<Vec<_>>();
        self.insert_existing_ready_rows_tx(&mut tx, ready_rows, Some(state))
            .await?;
        self.notify_queues_tx(&mut tx, queues).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(moved.len())
    }

    async fn relation_has_rows_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        relation: &str,
    ) -> Result<bool, AwaError> {
        sqlx::query_scalar(&format!("SELECT EXISTS (SELECT 1 FROM {relation} LIMIT 1)"))
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)
    }

    /// Idle-gate probe (#371): true when every relation in `relations` is
    /// empty. Short-circuits on the first non-empty relation, so under
    /// active traffic the extra cost over the existing busy gate is a
    /// single `EXISTS ... LIMIT 1` probe; when the ring really is idle all
    /// probes hit empty heaps. Same SQL shape (and lock strength —
    /// AccessShare only) as the per-table busy checks above.
    async fn relations_all_empty_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        relations: &[String],
    ) -> Result<bool, AwaError> {
        for relation in relations {
            if Self::relation_has_rows_tx(tx, relation).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.rotate")]
    pub async fn rotate(&self, pool: &PgPool) -> Result<RotateOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        // #371: the per-ring advisory lock replaces the pre-ledger
        // `FOR UPDATE` on the ring-state singleton for rotate ↔ prune
        // serialization. Non-blocking: rotation is a periodic tick, so
        // under contention we skip instead of queueing behind a prune.
        if !self
            .try_ring_rotation_lock_tx(&mut tx, RingFamily::Queue)
            .await?
        {
            let (current_slot, _) = self.ring_cursor_tx(&mut tx, RingFamily::Queue).await?;
            let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Queue).await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy {
                slot: (current_slot + 1).rem_euclid(slot_count),
                busy: BusyCounts::default(),
            });
        }

        let (current_slot, generation) = self.ring_cursor_tx(&mut tx, RingFamily::Queue).await?;
        let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Queue).await?;
        let state = (current_slot, generation, slot_count);

        let next_slot = (state.0 + 1).rem_euclid(state.2);
        let ready_busy =
            Self::relation_has_rows_tx(&mut tx, &ready_child_name(schema, next_slot as usize))
                .await?;
        let claim_attempt_batch_busy = Self::relation_has_rows_tx(
            &mut tx,
            &ready_claim_attempt_batch_child_name(schema, next_slot as usize),
        )
        .await?;
        let done_busy =
            Self::relation_has_rows_tx(&mut tx, &done_child_name(schema, next_slot as usize))
                .await?;
        let tombstone_busy = Self::relation_has_rows_tx(
            &mut tx,
            &ready_tombstone_child_name(schema, next_slot as usize),
        )
        .await?;
        let segment_busy = Self::relation_has_rows_tx(
            &mut tx,
            &ready_segment_child_name(schema, next_slot as usize),
        )
        .await?;
        let receipt_batch_busy = Self::relation_has_rows_tx(
            &mut tx,
            &receipt_completion_batch_child_name(schema, next_slot as usize),
        )
        .await?;
        let receipt_tombstone_busy = Self::relation_has_rows_tx(
            &mut tx,
            &receipt_completion_tombstone_child_name(schema, next_slot as usize),
        )
        .await?;
        let terminal_delta_busy = Self::relation_has_rows_tx(
            &mut tx,
            &terminal_delta_child_name(schema, next_slot as usize),
        )
        .await?;

        if ready_busy
            || claim_attempt_batch_busy
            || done_busy
            || tombstone_busy
            || segment_busy
            || receipt_batch_busy
            || receipt_tombstone_busy
            || terminal_delta_busy
        {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy {
                slot: next_slot,
                busy: BusyCounts {
                    queue_ready: busy_indicator(ready_busy),
                    queue_claim_attempt_batches: busy_indicator(claim_attempt_batch_busy),
                    queue_done: busy_indicator(done_busy),
                    queue_tombstones: busy_indicator(tombstone_busy),
                    queue_ready_segments: busy_indicator(segment_busy),
                    queue_receipt_completion_batches: busy_indicator(receipt_batch_busy),
                    queue_receipt_completion_tombstones: busy_indicator(receipt_tombstone_busy),
                    queue_terminal_deltas: busy_indicator(terminal_delta_busy),
                    ..Default::default()
                },
            });
        }

        // Idle gate (#371): the next slot is drained; if the *current*
        // slot's children are also empty, the ring received no writes
        // since this slot opened — there is nothing to seal, and the
        // ledger append below would only grow the rotation ledger with
        // rows that carry no sealed data. Skip without touching any row.
        //
        // Race note: a concurrent enqueue that commits into the current
        // slot right after this probe is benign. Rotation is enablement,
        // not obligation (the TLA+ rotate actions are guarded and carry
        // no fairness), so skipping only delays sealing that write by
        // one rotate tick — the next tick observes the row and rotates.
        // The skip never loses data and never leaves a non-empty slot
        // unsealed while traffic flows.
        let current_children = [
            ready_child_name(schema, state.0 as usize),
            ready_claim_attempt_batch_child_name(schema, state.0 as usize),
            done_child_name(schema, state.0 as usize),
            ready_tombstone_child_name(schema, state.0 as usize),
            ready_segment_child_name(schema, state.0 as usize),
            receipt_completion_batch_child_name(schema, state.0 as usize),
            receipt_completion_tombstone_child_name(schema, state.0 as usize),
            terminal_delta_child_name(schema, state.0 as usize),
        ];
        if Self::relations_all_empty_tx(&mut tx, &current_children).await? {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedIdle { slot: state.0 });
        }

        let next_generation = state.1 + 1;

        // Insert-conflict is the CAS: the advisory lock already excludes
        // well-behaved rotators, so a conflicting generation means some
        // writer bypassed the lock — treat it as a lost race, never as
        // retry-with-same-generation.
        if !self
            .append_ring_rotation_tx(&mut tx, RingFamily::Queue, next_generation, next_slot)
            .await?
        {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy {
                slot: next_slot,
                busy: BusyCounts::default(),
            });
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(RotateOutcome::Rotated {
            slot: next_slot,
            generation: next_generation,
        })
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.rotate_leases")]
    pub async fn rotate_leases(&self, pool: &PgPool) -> Result<RotateOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        // The per-ring advisory lock serialises with prune_oldest_leases
        // and parallel rotators (#371 — it replaces the pre-ledger
        // `FOR UPDATE` on the lease_ring_state singleton). Without it two
        // rotators can both pass the busy-check, both compute the same
        // next_slot, and the loser's ledger CAS wastes work.
        // `RotateLeasesPlan` in
        // `correctness/storage/AwaStorageLockOrder.tla` requires this
        // lock as the first acquired resource for the rotation tx.
        if !self
            .try_ring_rotation_lock_tx(&mut tx, RingFamily::Lease)
            .await?
        {
            let (current_slot, _) = self.ring_cursor_tx(&mut tx, RingFamily::Lease).await?;
            let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Lease).await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy {
                slot: (current_slot + 1).rem_euclid(slot_count),
                busy: BusyCounts::default(),
            });
        }

        let (current_slot, generation) = self.ring_cursor_tx(&mut tx, RingFamily::Lease).await?;
        let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Lease).await?;
        let state = (current_slot, generation, slot_count);

        let next_slot = (state.0 + 1).rem_euclid(state.2);
        let lease_busy =
            Self::relation_has_rows_tx(&mut tx, &lease_child_name(schema, next_slot as usize))
                .await?;

        if lease_busy {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy {
                slot: next_slot,
                busy: BusyCounts {
                    leases: busy_indicator(lease_busy),
                    ..Default::default()
                },
            });
        }

        // Idle gate (#371): if the current slot's lease child is empty
        // there is nothing to seal, and the ledger append below would
        // only grow the rotation ledger (the lease ring was the top
        // residual pinned-horizon dead-tuple accumulator at the
        // historical 250ms singleton-UPDATE cadence — ~14.4k dead tuples
        // per hour). See `rotate` for the race analysis: a lease that
        // lands after this probe is sealed one tick later; nothing is
        // lost.
        let current_children = [lease_child_name(schema, state.0 as usize)];
        if Self::relations_all_empty_tx(&mut tx, &current_children).await? {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedIdle { slot: state.0 });
        }

        let next_generation = state.1 + 1;

        if !self
            .append_ring_rotation_tx(&mut tx, RingFamily::Lease, next_generation, next_slot)
            .await?
        {
            // Another rotator beat us to the ledger CAS. Report the
            // bounded presence evidence we sampled before giving up.
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy {
                slot: next_slot,
                busy: BusyCounts {
                    leases: busy_indicator(lease_busy),
                    ..Default::default()
                },
            });
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(RotateOutcome::Rotated {
            slot: next_slot,
            generation: next_generation,
        })
    }

    /// Rebuild terminal counters from scratch by truncating folded live counts
    /// and pending deltas, then re-aggregating terminal history. Run this when:
    ///
    /// - upgrading from a pre-#290 fleet, where in-flight binaries wrote
    ///   terminal history without maintaining the counter,
    /// - after any incident that may have left the counter inconsistent
    ///   with terminal history, or
    /// - as a routine drift-recovery step before relying on counter-fed
    ///   reads for billing-grade accuracy.
    ///
    /// The rebuild is wrapped in an advisory lock so concurrent writers
    /// don't interleave new inserts mid-rebuild. The lock key is scoped
    /// to the queue-storage schema, so other schemas / other operations
    /// are unaffected. Writers that hit the lock will block briefly
    /// rather than fail.
    ///
    /// **Operator note:** this is best run on a quiesced fleet (workers
    /// paused or fully drained). Concurrent inserts during the rebuild
    /// will block on the lock; long-held locks can stall the fleet. The
    /// rebuild itself is O(rows in `{schema}.done_entries`). Compact receipt
    /// completions remain counted directly from retained
    /// `receipt_completion_batches` until queue prune folds them into
    /// permanent rollups.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.rebuild_terminal_counters")]
    pub async fn rebuild_terminal_counters(&self, pool: &PgPool) -> Result<i64, AwaError> {
        let schema = self.schema();
        let rebuild_lock_name = format!("awa.queue_storage.rebuild_terminal_counters:{schema}");
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(&rebuild_lock_name)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            "LOCK TABLE {schema}.done_entries, \
             {schema}.receipt_completion_batches, \
             {schema}.receipt_completion_tombstones, \
             {schema}.queue_terminal_count_deltas, \
             {schema}.queue_terminal_live_counts \
             IN ACCESS EXCLUSIVE MODE"
        ))
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            "TRUNCATE TABLE {schema}.queue_terminal_live_counts, {schema}.queue_terminal_count_deltas"
        ))
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let inserted: i64 = sqlx::query_scalar(&format!(
            r#"
            WITH inserted AS (
                INSERT INTO {schema}.queue_terminal_live_counts AS counts (
                    ready_slot, queue, priority, enqueue_shard, counter_bucket, live_terminal_count
                )
                SELECT
                    ready_slot,
                    queue,
                    priority,
                    enqueue_shard,
                    mod(
                        mod(job_id, {TERMINAL_COUNTER_BUCKETS}::bigint)
                            + {TERMINAL_COUNTER_BUCKETS}::bigint,
                        {TERMINAL_COUNTER_BUCKETS}::bigint
                    )::smallint AS counter_bucket,
                    count(*)::bigint
                FROM {schema}.done_entries
                GROUP BY ready_slot, queue, priority, enqueue_shard, counter_bucket
                RETURNING 1
            )
            SELECT COALESCE(count(*), 0)::bigint FROM inserted
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        // Flip the trust marker. From this point the read path
        // (queue_counts_exact) uses the counter for done-entry terminal rows;
        // before this call, it falls back to scanning terminal_jobs.
        sqlx::query(&format!(
            r#"
            UPDATE {schema}.queue_ring_state
            SET terminal_counter_trusted_at = now()
            WHERE singleton = TRUE
            "#
        ))
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(inserted)
    }

    /// Check whether the live terminal counter has been marked trusted
    /// for exact reads. Returns `true` for fresh installs (the trust
    /// marker is auto-set by `prepare_schema` when `done_entries` is
    /// empty) and after a successful
    /// [`Self::rebuild_terminal_counters`]; returns `false` on an
    /// existing install that has not yet been rebuilt under the new
    /// counter-aware code path.
    ///
    /// `queue_counts_exact` consults this to decide between the
    /// counter-fed fast path and the scan-based fallback. Single-row
    /// PK fetch; negligible cost per call.
    pub async fn terminal_counter_trusted(&self, pool: &PgPool) -> Result<bool, AwaError> {
        let schema = self.schema();
        let trusted: Option<bool> = sqlx::query_scalar(&format!(
            "SELECT terminal_counter_trusted_at IS NOT NULL \
             FROM {schema}.queue_ring_state WHERE singleton = TRUE"
        ))
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;
        // Missing row = pre-#290 schema that hasn't run the new
        // prepare_schema yet. Treat as untrusted; the scan path is
        // still correct.
        Ok(trusted.unwrap_or(false))
    }

    /// Fold append-only `done_entries` terminal-count deltas into
    /// `queue_terminal_live_counts` for sealed queue slots.
    ///
    /// `done_entries` terminal inserts and deletes append signed rows into
    /// `queue_terminal_count_deltas` instead of updating the live counter on
    /// the user-facing hot path. Compact receipt completions are counted from
    /// retained batch rows directly. Exact reads sum folded live counts plus
    /// pending deltas and retained compact batches, so this rollup can run
    /// asynchronously. It intentionally
    /// skips the current queue slot and any slot with active leases or open
    /// receipt claims; that keeps rollup away from the segment receiving hot
    /// completions and avoids racing future terminal deltas for the same
    /// ready generation. Rollup also stands down while another backend pins the
    /// MVCC horizon, leaving the append-only deltas visible to exact reads
    /// until the horizon clears.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.rollup_terminal_count_deltas")]
    pub async fn rollup_terminal_count_deltas(
        &self,
        pool: &PgPool,
        max_slots: usize,
    ) -> Result<TerminalDeltaRollupOutcome, AwaError> {
        if max_slots == 0 {
            return Ok(TerminalDeltaRollupOutcome::default());
        }

        let slots = self.terminal_delta_rollup_candidates(pool).await?;

        let mut outcome = TerminalDeltaRollupOutcome::default();
        for (slot, generation) in slots {
            if outcome.rolled_slots >= max_slots {
                break;
            }
            match self
                .rollup_terminal_count_delta_slot(pool, slot, generation)
                .await?
            {
                TerminalDeltaSlotRollup::Empty => {}
                TerminalDeltaSlotRollup::Rolled {
                    delta_rows,
                    grouped_keys,
                } => {
                    outcome.rolled_slots += 1;
                    outcome.delta_rows += delta_rows;
                    outcome.grouped_keys += grouped_keys;
                }
                TerminalDeltaSlotRollup::SkippedActive => {
                    outcome.skipped_active_slots += 1;
                }
                TerminalDeltaSlotRollup::SkippedMvccPinned => {
                    outcome.skipped_mvcc_pinned = true;
                    break;
                }
                TerminalDeltaSlotRollup::Blocked => {
                    outcome.blocked_slots += 1;
                }
            }
        }

        Ok(outcome)
    }

    async fn terminal_delta_rollup_mvcc_horizon_pinned_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
    ) -> Result<bool, AwaError> {
        let pinned: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND backend_type = 'client backend'
                  AND (
                      backend_xmin IS NOT NULL
                      OR (
                          backend_xid IS NOT NULL
                          AND state LIKE 'idle in transaction%'
                      )
                  )
            )
            "#,
        )
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(pinned)
    }

    async fn terminal_delta_rollup_candidates(
        &self,
        pool: &PgPool,
    ) -> Result<Vec<(i32, i64)>, AwaError> {
        let schema = self.schema();
        // Sealed slots and their last-open generations are derived from
        // the rotation-ledger cursor (#371); no per-slot generation rows
        // exist anymore.
        let (current_slot, generation, slot_count): (i32, i64, i32) = sqlx::query_as(&format!(
            r#"
                SELECT ledger.slot, ledger.generation, state.slot_count
                FROM {schema}.queue_ring_state AS state
                CROSS JOIN LATERAL (
                    SELECT slot, generation
                    FROM {schema}.queue_ring_rotations
                    ORDER BY generation DESC
                    LIMIT 1
                ) AS ledger
                WHERE state.singleton = TRUE
                "#
        ))
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;
        let sealed_slots = initialized_sealed_ring_slots(current_slot, generation, slot_count);

        let mut pending_slots = Vec::new();
        for (slot, generation) in sealed_slots {
            let Ok(slot_index) = usize::try_from(slot) else {
                continue;
            };
            let delta_child = terminal_delta_child_name(schema, slot_index);
            let has_pending: bool = sqlx::query_scalar(&format!(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM {delta_child}
                    WHERE ready_generation = $1
                    LIMIT 1
                )
                "#
            ))
            .bind(generation)
            .fetch_one(pool)
            .await
            .map_err(map_sqlx_error)?;
            if has_pending {
                pending_slots.push((slot, generation));
            }
        }

        Ok(pending_slots)
    }

    async fn rollup_terminal_count_delta_slot(
        &self,
        pool: &PgPool,
        slot: i32,
        generation: i64,
    ) -> Result<TerminalDeltaSlotRollup, AwaError> {
        let schema = self.schema();
        let delta_child = terminal_delta_child_name(schema, slot as usize);
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        // Ring rotation lock first (stabilises the cursor against rotate
        // and prune), then the slot row lock — the same order every
        // queue-ring maintenance tx uses (`RollupTerminalDeltasPlan` in
        // `correctness/storage/AwaStorageLockOrder.tla`).
        if !self
            .try_ring_rotation_lock_tx(&mut tx, RingFamily::Queue)
            .await?
        {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::Blocked);
        }

        let (current_slot, current_generation) =
            self.ring_cursor_tx(&mut tx, RingFamily::Queue).await?;
        let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Queue).await?;

        let slot_locked: Option<i32> = sqlx::query_scalar(&format!(
            r#"
            SELECT slot
            FROM {schema}.queue_ring_slots
            WHERE slot = $1
            FOR UPDATE
            "#
        ))
        .bind(slot)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if slot_locked.is_none() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::Empty);
        }

        if current_slot == slot {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::SkippedActive);
        }

        let Some(slot_generation) =
            ring_slot_generation(current_slot, current_generation, slot_count, slot)
        else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::Empty);
        };

        if slot_generation != generation {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::Empty);
        }

        if Self::terminal_delta_rollup_mvcc_horizon_pinned_tx(&mut tx).await? {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::SkippedMvccPinned);
        }

        let ready_child = ready_child_name(schema, slot as usize);
        let pending_ready: bool = sqlx::query_scalar(&format!(
            r#"
            WITH claim_cursors AS MATERIALIZED (
                SELECT
                    queue,
                    priority,
                    enqueue_shard,
                    {schema}.sequence_next_value(seq_name) AS claim_seq
                FROM {schema}.queue_claim_heads
            )
            SELECT EXISTS (
                SELECT 1
                FROM claim_cursors AS claims
                CROSS JOIN LATERAL (
                    SELECT 1
                    FROM {ready_child} AS ready
                    WHERE ready.ready_generation = $1
                      AND ready.queue = claims.queue
                      AND ready.priority = claims.priority
                      AND ready.enqueue_shard = claims.enqueue_shard
                      AND ready.lane_seq >= claims.claim_seq
                    LIMIT 1
                ) AS pending_ready
                LIMIT 1
            )
            "#
        ))
        .bind(generation)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if pending_ready {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::SkippedActive);
        }

        set_prune_lock_timeout_tx(&mut tx, self.prune_lock_timeout).await?;

        let lock_delta = sqlx::query(&format!(
            "LOCK TABLE {delta_child} IN ACCESS EXCLUSIVE MODE"
        ))
        .execute(tx.as_mut())
        .await;

        match lock_delta {
            Ok(_) => {}
            Err(err) if is_lock_contention_error(&err) => {
                let _ = tx.rollback().await;
                return Ok(TerminalDeltaSlotRollup::Blocked);
            }
            Err(err) => {
                let _ = tx.rollback().await;
                return Err(map_sqlx_error(err));
            }
        }

        let active_leases =
            queue_prune_has_active_leases_tx(&mut tx, schema, slot, generation).await?;

        if active_leases {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::SkippedActive);
        }

        let unclosed_claim_refs =
            queue_prune_has_unclosed_claim_refs_tx(&mut tx, schema, slot, generation).await?;

        if unclosed_claim_refs {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::SkippedActive);
        }

        let delta_rows: i64 = sqlx::query_scalar(&format!(
            r#"
            SELECT count(*)::bigint
            FROM {delta_child}
            WHERE ready_generation = $1
            "#
        ))
        .bind(generation)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if delta_rows == 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::Empty);
        }

        let grouped_keys: i64 = sqlx::query_scalar(&format!(
            r#"
            WITH grouped AS MATERIALIZED (
                SELECT
                    ready_slot,
                    queue,
                    priority,
                    enqueue_shard,
                    counter_bucket,
                    SUM(terminal_delta)::bigint AS delta
                FROM {delta_child}
                WHERE ready_generation = $1
                GROUP BY ready_slot, queue, priority, enqueue_shard, counter_bucket
                HAVING SUM(terminal_delta) <> 0
            ),
            updated AS (
                UPDATE {schema}.queue_terminal_live_counts AS counts
                SET live_terminal_count = GREATEST(0, counts.live_terminal_count + grouped.delta)
                FROM grouped
                WHERE counts.ready_slot = grouped.ready_slot
                  AND counts.queue = grouped.queue
                  AND counts.priority = grouped.priority
                  AND counts.enqueue_shard = grouped.enqueue_shard
                  AND counts.counter_bucket = grouped.counter_bucket
                RETURNING
                    counts.ready_slot,
                    counts.queue,
                    counts.priority,
                    counts.enqueue_shard,
                    counts.counter_bucket
            ),
            inserted AS (
                INSERT INTO {schema}.queue_terminal_live_counts AS counts (
                    ready_slot,
                    queue,
                    priority,
                    enqueue_shard,
                    counter_bucket,
                    live_terminal_count
                )
                SELECT
                    grouped.ready_slot,
                    grouped.queue,
                    grouped.priority,
                    grouped.enqueue_shard,
                    grouped.counter_bucket,
                    grouped.delta
                FROM grouped
                WHERE grouped.delta > 0
                  AND NOT EXISTS (
                      SELECT 1
                      FROM updated
                      WHERE updated.ready_slot = grouped.ready_slot
                        AND updated.queue = grouped.queue
                        AND updated.priority = grouped.priority
                        AND updated.enqueue_shard = grouped.enqueue_shard
                        AND updated.counter_bucket = grouped.counter_bucket
                  )
                ORDER BY ready_slot, queue, priority, enqueue_shard, counter_bucket
                ON CONFLICT (ready_slot, queue, priority, enqueue_shard, counter_bucket) DO UPDATE
                SET live_terminal_count =
                    GREATEST(0, counts.live_terminal_count + EXCLUDED.live_terminal_count)
                RETURNING 1
            )
            SELECT count(*)::bigint FROM grouped
            "#
        ))
        .bind(generation)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let truncate_delta = sqlx::query(&format!("TRUNCATE TABLE {delta_child}"))
            .execute(tx.as_mut())
            .await;

        match truncate_delta {
            Ok(_) => {
                tx.commit().await.map_err(map_sqlx_error)?;
                Ok(TerminalDeltaSlotRollup::Rolled {
                    delta_rows,
                    grouped_keys,
                })
            }
            Err(err) if is_lock_contention_error(&err) => {
                let _ = tx.rollback().await;
                Ok(TerminalDeltaSlotRollup::Blocked)
            }
            Err(err) => {
                let _ = tx.rollback().await;
                Err(map_sqlx_error(err))
            }
        }
    }

    /// Fold pending `queue_terminal_rollup_deltas` rows into the
    /// permanent `queue_terminal_rollups` denormaliser (#371).
    ///
    /// Queue prune appends per-(queue, priority) delta rows instead of
    /// upserting the rollups inside the prune transaction, so a pinned
    /// MVCC horizon cannot pile up dead rollup versions. This fold runs
    /// from the maintenance leader (piggybacking the 30s terminal-rollup
    /// tick), stands down while any other backend pins the MVCC horizon
    /// (the same guard the terminal-count delta rollup uses), and
    /// otherwise consumes the delta rows with `DELETE ... RETURNING`,
    /// aggregates them, and upserts the rollups in deterministic
    /// (queue, priority) order with the historical `GREATEST(0, ...)`
    /// clamp applied at fold time.
    ///
    /// Exact readers ([`Self::queue_counts`], [`Self::queue_counts_fast`],
    /// [`Self::pruned_failed_count_for_queue`]) add unfolded delta sums to
    /// their rollup reads, so counts stay exact regardless of fold timing.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.fold_terminal_rollup_deltas")]
    pub async fn fold_terminal_rollup_deltas(
        &self,
        pool: &PgPool,
    ) -> Result<TerminalRollupFoldOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        if Self::terminal_delta_rollup_mvcc_horizon_pinned_tx(&mut tx).await? {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalRollupFoldOutcome {
                skipped_mvcc_pinned: true,
                ..Default::default()
            });
        }

        let deltas: Vec<(String, i16, i64, i64)> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.queue_terminal_rollup_deltas
            RETURNING queue, priority, pruned_completed_delta, pruned_failed_delta
            "#
        ))
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if deltas.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalRollupFoldOutcome::default());
        }

        let folded_delta_rows = deltas.len();
        let folded_keys = deltas
            .iter()
            .map(|(queue, priority, _, _)| (queue.clone(), *priority))
            .collect::<std::collections::BTreeSet<_>>()
            .len();

        self.adjust_terminal_rollups_batch(&mut tx, deltas.into_iter())
            .await?;

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(TerminalRollupFoldOutcome {
            folded_delta_rows,
            folded_keys,
            skipped_mvcc_pinned: false,
        })
    }

    /// Trim the ring-rotation ledgers to one full wrap (#371).
    ///
    /// Rotation appends one ledger row per advance; this fold deletes
    /// rows older than `max_generation - (slot_count - 1)`, retaining one
    /// row per slot so every sealed slot's last-open generation stays
    /// directly readable from the ledger. Like the terminal-delta
    /// rollup, the fold stands down while another backend pins the MVCC
    /// horizon, so the deleted versions are immediately reclaimable when
    /// they are created.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.fold_ring_rotation_ledgers")]
    pub async fn fold_ring_rotation_ledgers(
        &self,
        pool: &PgPool,
    ) -> Result<RingLedgerFoldOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        if Self::terminal_delta_rollup_mvcc_horizon_pinned_tx(&mut tx).await? {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RingLedgerFoldOutcome {
                skipped_mvcc_pinned: true,
                ..Default::default()
            });
        }

        let mut trimmed_rows = 0_u64;
        for family in [RingFamily::Queue, RingFamily::Lease, RingFamily::Claim] {
            let ledger = family.ledger_relname();
            let state = family.state_relname();
            let deleted = sqlx::query(&format!(
                r#"
                DELETE FROM {schema}.{ledger}
                WHERE generation < (
                    SELECT max(generation) FROM {schema}.{ledger}
                ) - (
                    SELECT slot_count - 1 FROM {schema}.{state} WHERE singleton = TRUE
                )::bigint
                "#
            ))
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
            trimmed_rows += deleted.rows_affected();
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(RingLedgerFoldOutcome {
            trimmed_rows,
            skipped_mvcc_pinned: false,
        })
    }

    /// Prune the oldest sealed queue ring slot.
    ///
    /// `failed_retention` is the floor for `failed` terminal rows: rows
    /// with `finalized_at` inside the floor are re-homed into the live
    /// `done_entries` segment (still retryable) instead of being
    /// truncated with the slot. `Duration::ZERO` disables the floor and
    /// prunes every terminal row. Granularity is whole seconds,
    /// matching DLQ retention. The floor applies to `failed` only —
    /// `cancelled` is an explicit operator action and is always pruned.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.prune_oldest")]
    pub async fn prune_oldest(
        &self,
        pool: &PgPool,
        failed_retention: Duration,
    ) -> Result<PruneOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        // Ring rotation lock replaces the pre-#371 `FOR UPDATE` on the
        // queue_ring_state singleton: it keeps the cursor stable for the
        // whole prune so rotate cannot hand the slot being truncated out
        // to fresh writes.
        if !self
            .try_ring_rotation_lock_tx(&mut tx, RingFamily::Queue)
            .await?
        {
            let (current_slot, generation) =
                self.ring_cursor_tx(&mut tx, RingFamily::Queue).await?;
            let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Queue).await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(
                match oldest_initialized_ring_slot(current_slot, generation, slot_count) {
                    Some((slot, _)) => PruneOutcome::Blocked { slot },
                    None => PruneOutcome::Noop,
                },
            );
        }

        let (current_slot, current_generation) =
            self.ring_cursor_tx(&mut tx, RingFamily::Queue).await?;
        let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Queue).await?;
        let state = (current_slot,);

        let Some((slot, generation)) =
            oldest_initialized_ring_slot(current_slot, current_generation, slot_count)
        else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::Noop);
        };

        // Slot row lock: second resource in the queue-ring lock order
        // (ring advisory lock → slot row → child ACCESS EXCLUSIVE).
        let slot_locked: Option<i32> = sqlx::query_scalar(&format!(
            r#"
            SELECT slot
            FROM {schema}.queue_ring_slots
            WHERE slot = $1
            FOR UPDATE
            "#
        ))
        .bind(slot)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if slot_locked.is_none() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::Noop);
        };

        let ready_child = ready_child_name(schema, slot as usize);
        let claim_attempt_child = ready_claim_attempt_batch_child_name(schema, slot as usize);
        let done_child = done_child_name(schema, slot as usize);
        let tomb_child = ready_tombstone_child_name(schema, slot as usize);
        let segment_child = ready_segment_child_name(schema, slot as usize);
        let receipt_batch_child = receipt_completion_batch_child_name(schema, slot as usize);
        let receipt_tomb_child = receipt_completion_tombstone_child_name(schema, slot as usize);
        let delta_child = terminal_delta_child_name(schema, slot as usize);

        // Queue prune has three common skip gates. Check them before
        // taking ACCESS EXCLUSIVE on the ready/terminal child tables so
        // a known-busy slot does not block hot claim reads behind a
        // doomed truncate attempt. These are only MVCC snapshots; the
        // same gates are checked again after the exclusive locks.
        let active_leases =
            queue_prune_has_active_leases_tx(&mut tx, schema, slot, generation).await?;

        if active_leases {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::QueueActiveLeases,
                count: busy_indicator(active_leases),
            });
        }

        let pending =
            queue_prune_has_pending_ready_tx(&mut tx, schema, &ready_child, generation).await?;

        if pending {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::QueuePendingReady,
                count: busy_indicator(pending),
            });
        }

        let unclosed_claim_refs =
            queue_prune_has_unclosed_claim_refs_tx(&mut tx, schema, slot, generation).await?;

        if unclosed_claim_refs {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::QueueUnclosedClaimRefs,
                count: busy_indicator(unclosed_claim_refs),
            });
        }

        set_prune_lock_timeout_tx(&mut tx, self.prune_lock_timeout).await?;

        let lock_tables = sqlx::query(&format!(
            "LOCK TABLE {ready_child}, {claim_attempt_child}, {done_child}, {tomb_child}, {segment_child}, {receipt_batch_child}, {receipt_tomb_child}, {delta_child} IN ACCESS EXCLUSIVE MODE"
        ))
        .execute(tx.as_mut())
        .await;

        if let Err(err) = lock_tables {
            let _ = tx.rollback().await;
            if is_lock_contention_error(&err) {
                return Ok(PruneOutcome::Blocked { slot });
            }
            return Err(map_sqlx_error(err));
        }

        // The pre-lock gates are only an MVCC snapshot. If an in-flight
        // producer or claimer committed while the exclusive partition
        // lock waited, these post-lock gates see it before TRUNCATE.
        let active_leases =
            queue_prune_has_active_leases_tx(&mut tx, schema, slot, generation).await?;
        if active_leases {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::QueueActiveLeases,
                count: busy_indicator(active_leases),
            });
        }

        let pending =
            queue_prune_has_pending_ready_tx(&mut tx, schema, &ready_child, generation).await?;
        if pending {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::QueuePendingReady,
                count: busy_indicator(pending),
            });
        }

        let unclosed_claim_refs =
            queue_prune_has_unclosed_claim_refs_tx(&mut tx, schema, slot, generation).await?;
        if unclosed_claim_refs {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::QueueUnclosedClaimRefs,
                count: busy_indicator(unclosed_claim_refs),
            });
        }

        let failed_retention_secs = i64::try_from(failed_retention.as_secs()).unwrap_or(i64::MAX);
        let retention_floor = failed_retention_secs > 0;

        // #337: failed terminal rows still inside the retention floor
        // are re-homed into the live done segment before the TRUNCATE
        // so they stay retryable. The ring rotation lock is held above,
        // which serializes against rotate — the current slot (and its
        // generation, read from the ledger cursor) cannot move while
        // carried rows are re-inserted into it.
        let carried_failed_rows = if retention_floor {
            // Carried rows are widened to the self-contained synthetic
            // done-row shape (the COALESCE chain mirrors
            // `done_row_projection`) because the ready rows they would
            // otherwise hydrate from are truncated with this slot.
            // Deliberately no ON CONFLICT: lane_seq comes from
            // never-resetting sequences, so a PK collision means
            // corrupted terminal state and must abort the prune loudly
            // rather than silently drop a terminal fact.
            let carried = sqlx::query(&format!(
                r#"
                INSERT INTO {schema}.done_entries (
                    ready_slot, ready_generation, job_id, kind, queue,
                    args, state, priority, attempt, run_lease,
                    max_attempts, lane_seq, enqueue_shard, run_at,
                    attempted_at, finalized_at, created_at, unique_key,
                    unique_states, payload
                )
                SELECT
                    $2,
                    $3,
                    done.job_id,
                    done.kind,
                    done.queue,
                    COALESCE(done.args, ready.args, '{{}}'::jsonb),
                    done.state,
                    done.priority,
                    done.attempt,
                    done.run_lease,
                    COALESCE(done.max_attempts, ready.max_attempts, 25::smallint),
                    done.lane_seq,
                    done.enqueue_shard,
                    COALESCE(done.run_at, ready.run_at, done.finalized_at),
                    COALESCE(done.attempted_at, ready.attempted_at),
                    done.finalized_at,
                    COALESCE(done.created_at, ready.created_at, done.finalized_at),
                    COALESCE(done.unique_key, ready.unique_key),
                    COALESCE(done.unique_states, ready.unique_states),
                    COALESCE(done.payload, ready.payload, '{{}}'::jsonb)
                FROM {done_child} AS done
                LEFT JOIN {ready_child} AS ready
                  ON ready.ready_slot = done.ready_slot
                 AND ready.ready_generation = done.ready_generation
                 AND ready.queue = done.queue
                 AND ready.priority = done.priority
                 AND ready.enqueue_shard = done.enqueue_shard
                 AND ready.lane_seq = done.lane_seq
                WHERE done.state = 'failed'
                  AND done.finalized_at >= now() - make_interval(secs => $1::bigint)
                "#
            ))
            .bind(failed_retention_secs)
            .bind(state.0)
            .bind(current_generation)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?
            .rows_affected();

            if carried > 0 {
                // This slot's live counter rows and delta segment are
                // wiped below, so the carried rows' exact-count
                // evidence moves with them: re-append positive deltas
                // under the current slot, grouped exactly like the
                // completion path's delta append.
                sqlx::query(&format!(
                    r#"
                    INSERT INTO {schema}.queue_terminal_count_deltas (
                        ready_slot,
                        ready_generation,
                        queue,
                        priority,
                        enqueue_shard,
                        counter_bucket,
                        terminal_delta
                    )
                    SELECT
                        $2,
                        $3,
                        queue,
                        priority,
                        enqueue_shard,
                        mod(
                            mod(job_id, {TERMINAL_COUNTER_BUCKETS}::bigint)
                                + {TERMINAL_COUNTER_BUCKETS}::bigint,
                            {TERMINAL_COUNTER_BUCKETS}::bigint
                        )::smallint AS counter_bucket,
                        count(*)::bigint
                    FROM {done_child}
                    WHERE state = 'failed'
                      AND finalized_at >= now() - make_interval(secs => $1::bigint)
                    GROUP BY queue, priority, enqueue_shard, counter_bucket
                    ORDER BY queue, priority, enqueue_shard, counter_bucket
                    "#
                ))
                .bind(failed_retention_secs)
                .bind(state.0)
                .bind(current_generation)
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            }
            carried
        } else {
            0
        };

        // #290: scan the about-to-be-truncated partition for the rollup
        // fold. The rollup column is *permanent* state, so we MUST fold
        // from ground truth (the `{done_child}` partition itself), not
        // from `queue_terminal_live_counts` — the counter can drift
        // briefly during a rolling upgrade from a pre-counter binary
        // and folding drift into the rollup would bake it in forever.
        // The counter rows for this slot are still cleaned up after the
        // truncate; reads from the counter (queue_counts_exact in #305)
        // may transiently disagree with the rollup until the operator
        // runs `awa storage rebuild-terminal-counters`, but the rollup
        // itself stays authoritative. See PR #304 reviewer finding
        // "High: A1 can persist stale counter state before the
        // read-switch PR" for the trade-off.
        //
        // Carried failed rows are excluded from both columns: they are
        // still live in `done_entries`, so folding them into the
        // permanent rollup would double-count them.
        let pruned_terminal_counts: Vec<(String, i16, i64, i64)> = sqlx::query_as(&format!(
            r#"
            WITH done_counts AS (
                SELECT
                    queue,
                    priority,
                    (count(*) FILTER (WHERE state <> 'failed'))::bigint AS completed,
                    (count(*) FILTER (
                        WHERE state = 'failed'
                          AND (
                              $2::boolean IS FALSE
                              OR finalized_at < now() - make_interval(secs => $1::bigint)
                          )
                    ))::bigint AS failed
                FROM {done_child}
                GROUP BY queue, priority
            ),
            -- Scan the entire partition being truncated (all generations),
            -- mirroring done_counts' whole-child scan above. The rotate guard
            -- keeps a partition to one generation at prune time, so this is the
            -- same set today; counting the whole child keeps the rollup fold
            -- conservative even if that invariant is ever weakened (a stray
            -- generation's batches would otherwise be truncated without being
            -- folded into the rollup -> terminal-count undercount).
            batch_rows AS (
                SELECT
                    batch.ready_generation,
                    batch.queue,
                    batch.priority,
                    item.job_id,
                    item.run_lease
                FROM {receipt_batch_child} AS batch
                CROSS JOIN LATERAL unnest(
                    batch.job_ids,
                    batch.run_leases
                ) AS item(job_id, run_lease)
            ),
            batch_counts AS (
                SELECT
                    batch_rows.queue,
                    batch_rows.priority,
                    count(*)::bigint AS completed
                FROM batch_rows
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {receipt_tomb_child} AS tomb
                    WHERE tomb.ready_generation = batch_rows.ready_generation
                      AND tomb.job_id = batch_rows.job_id
                      AND tomb.run_lease = batch_rows.run_lease
                )
                GROUP BY batch_rows.queue, batch_rows.priority
            ),
            keys AS (
                SELECT queue, priority FROM done_counts
                UNION
                SELECT queue, priority FROM batch_counts
            )
            SELECT
                keys.queue,
                keys.priority,
                (
                    COALESCE(done_counts.completed, 0)
                    + COALESCE(batch_counts.completed, 0)
                )::bigint AS pruned_completed_count,
                COALESCE(done_counts.failed, 0)::bigint AS pruned_failed_count
            FROM keys
            LEFT JOIN done_counts USING (queue, priority)
            LEFT JOIN batch_counts USING (queue, priority)
            "#
        ))
        .bind(failed_retention_secs)
        .bind(retention_floor)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let truncate = sqlx::query(&format!(
            "TRUNCATE TABLE {ready_child}, {claim_attempt_child}, {done_child}, {tomb_child}, {segment_child}, {receipt_batch_child}, {receipt_tomb_child}, {delta_child}"
        ))
        .execute(tx.as_mut())
        .await;

        match truncate {
            Ok(_) => {
                if !pruned_terminal_counts.is_empty() {
                    // #371: append the pruned counts as delta rows instead
                    // of upserting `queue_terminal_rollups` here — the
                    // upsert under a pinned MVCC horizon left one
                    // unreclaimable dead rollup version per prune. The
                    // maintenance leader folds the deltas
                    // (`fold_terminal_rollup_deltas`) when the horizon is
                    // clear; exact readers add unfolded delta sums to
                    // their rollup reads.
                    self.append_terminal_rollup_deltas_tx(
                        &mut tx,
                        pruned_terminal_counts.into_iter(),
                    )
                    .await?;
                }

                // #290: the live counter rows for this slot are about to
                // be orphans (their underlying done_entries rows just got
                // truncated). Delete them in the same transaction. The
                // rollup fold above already captured ground-truth from
                // the partition scan; this just cleans up the counter
                // index entries so a future insert into a re-rotated
                // slot starts from zero.
                sqlx::query(&format!(
                    "DELETE FROM {schema}.queue_terminal_live_counts WHERE ready_slot = $1"
                ))
                .bind(slot)
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
                tx.commit().await.map_err(map_sqlx_error)?;
                if carried_failed_rows > 0 {
                    tracing::info!(
                        slot,
                        carried_failed_rows,
                        "Carried failed terminal rows inside the retention floor to the live done segment"
                    );
                }
                Ok(PruneOutcome::Pruned {
                    slot,
                    carried_failed_rows,
                })
            }
            Err(err) if is_lock_contention_error(&err) => {
                let _ = tx.rollback().await;
                Ok(PruneOutcome::Blocked { slot })
            }
            Err(err) => {
                let _ = tx.rollback().await;
                Err(map_sqlx_error(err))
            }
        }
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.prune_oldest_leases")]
    pub async fn prune_oldest_leases(&self, pool: &PgPool) -> Result<PruneOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        // `PruneLeasesPlan` in
        // `correctness/storage/AwaStorageLockOrder.tla` requires the
        // sequence lease-ring advisory lock →
        // `lease_ring_slots[slot] FOR UPDATE` → `ACCESS EXCLUSIVE` on
        // the child. The child lock is bounded by a short
        // transaction-local lock_timeout because pure NOWAIT can starve
        // under continuous parent-partition readers, while an unbounded
        // wait would put maintenance at the head of the relation-lock
        // queue indefinitely. Without these locks a concurrent
        // rotator can flip the cursor under the prune's liveness check
        // (current_slot recheck races the ledger CAS) and prune what
        // should be the active partition.
        if !self
            .try_ring_rotation_lock_tx(&mut tx, RingFamily::Lease)
            .await?
        {
            let (current_slot, generation) =
                self.ring_cursor_tx(&mut tx, RingFamily::Lease).await?;
            let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Lease).await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(
                match oldest_initialized_ring_slot(current_slot, generation, slot_count) {
                    Some((slot, _)) => PruneOutcome::Blocked { slot },
                    None => PruneOutcome::Noop,
                },
            );
        }

        let (current_slot, generation) = self.ring_cursor_tx(&mut tx, RingFamily::Lease).await?;
        let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Lease).await?;
        let state = (current_slot, generation, slot_count);

        let Some((slot, _generation)) = oldest_initialized_ring_slot(state.0, state.1, state.2)
        else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::Noop);
        };

        let slot_locked: Option<i32> = sqlx::query_scalar(&format!(
            r#"
            SELECT slot FROM {schema}.lease_ring_slots
            WHERE slot = $1
            FOR UPDATE
            "#
        ))
        .bind(slot)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if slot_locked.is_none() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::Noop);
        }

        let lease_child = lease_child_name(schema, slot as usize);

        set_prune_lock_timeout_tx(&mut tx, self.prune_lock_timeout).await?;

        let lock_table = sqlx::query(&format!(
            "LOCK TABLE {lease_child} IN ACCESS EXCLUSIVE MODE"
        ))
        .execute(tx.as_mut())
        .await;

        if let Err(err) = lock_table {
            let _ = tx.rollback().await;
            if is_lock_contention_error(&err) {
                return Ok(PruneOutcome::Blocked { slot });
            }
            return Err(map_sqlx_error(err));
        }

        // The ring rotation lock held above keeps the cursor stable, but
        // keep the explicit recheck to document the truncate
        // precondition.
        let (current_slot, _) = self.ring_cursor_tx(&mut tx, RingFamily::Lease).await?;

        if current_slot == slot {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::LeaseCurrent,
                count: 0,
            });
        }

        let active_leases = Self::relation_has_rows_tx(&mut tx, &lease_child).await?;

        if active_leases {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::LeaseActive,
                count: busy_indicator(active_leases),
            });
        }

        let truncate = sqlx::query(&format!("TRUNCATE TABLE {lease_child}"))
            .execute(tx.as_mut())
            .await;

        match truncate {
            Ok(_) => {
                tx.commit().await.map_err(map_sqlx_error)?;
                Ok(PruneOutcome::Pruned {
                    slot,
                    carried_failed_rows: 0,
                })
            }
            Err(err) if is_lock_contention_error(&err) => {
                let _ = tx.rollback().await;
                Ok(PruneOutcome::Blocked { slot })
            }
            Err(err) => {
                let _ = tx.rollback().await;
                Err(map_sqlx_error(err))
            }
        }
    }

    pub async fn vacuum_leases(&self, pool: &PgPool) -> Result<(), AwaError> {
        sqlx::query(&format!("VACUUM {}", self.leases_table()))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(())
    }

    /// ADR-023 claim-ring rotation. Parallel of `rotate_leases`.
    ///
    /// Advances the claim-ring cursor by appending to the
    /// `claim_ring_rotations` ledger (the generation PK insert is the
    /// compare-and-swap). Before flipping the cursor the target partition
    /// must be drained: the
    /// `lease_claims_<next>`, `lease_claim_batches_<next>`,
    /// `lease_claim_closures_<next>`, and
    /// `lease_claim_closure_batches_<next>` child tables
    /// must be empty. This is what the `rotate → prune → rotate` ring
    /// invariant requires — we only hand out a slot to new claims when a
    /// prior prune has truncated it.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.rotate_claims")]
    pub async fn rotate_claims(&self, pool: &PgPool) -> Result<RotateOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        // Per-ring advisory lock: serialises with prune_oldest_claims and
        // parallel rotators (#371 — replaces the pre-ledger `FOR UPDATE`
        // on the claim_ring_state singleton).
        if !self
            .try_ring_rotation_lock_tx(&mut tx, RingFamily::Claim)
            .await?
        {
            let (current_slot, _) = self.ring_cursor_tx(&mut tx, RingFamily::Claim).await?;
            let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Claim).await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy {
                slot: (current_slot + 1).rem_euclid(slot_count),
                busy: BusyCounts::default(),
            });
        }

        let (current_slot, generation) = self.ring_cursor_tx(&mut tx, RingFamily::Claim).await?;
        let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Claim).await?;
        let state = (current_slot, generation, slot_count);

        let next_slot = (state.0 + 1).rem_euclid(state.2);

        // Busy check: all children of the incoming slot must be empty.
        // A non-empty `lease_claims_<next>` means the previous lap's
        // prune hasn't run (or didn't complete); rotating anyway would
        // mix fresh claims with legacy rows and defeat the point of
        // partitioning. Non-empty closure children mean prune fell behind
        // on receipt-closure evidence specifically.
        let claim_busy =
            Self::relation_has_rows_tx(&mut tx, &claim_child_name(schema, next_slot as usize))
                .await?;
        let claim_batch_busy = Self::relation_has_rows_tx(
            &mut tx,
            &claim_batch_child_name(schema, next_slot as usize),
        )
        .await?;
        let closure_busy =
            Self::relation_has_rows_tx(&mut tx, &closure_child_name(schema, next_slot as usize))
                .await?;
        let closure_batch_busy = Self::relation_has_rows_tx(
            &mut tx,
            &claim_closure_batch_child_name(schema, next_slot as usize),
        )
        .await?;

        if claim_busy || claim_batch_busy || closure_busy || closure_batch_busy {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy {
                slot: next_slot,
                busy: BusyCounts {
                    claims: busy_indicator(claim_busy || claim_batch_busy),
                    closures: busy_indicator(closure_busy),
                    closure_batches: busy_indicator(closure_batch_busy),
                    ..Default::default()
                },
            });
        }

        // Idle gate (#371): if every child of the *current* slot is empty
        // there is no claim evidence to seal, and the ledger append below
        // would only grow the rotation ledger (the pre-ledger singleton
        // UPDATE accumulated ~3.6k dead tuples/hour under a pinned MVCC
        // horizon at the historical cadence). See `rotate` for the race
        // analysis: a claim that lands after this probe is sealed one
        // tick later; nothing is lost.
        let current_children = [
            claim_child_name(schema, state.0 as usize),
            claim_batch_child_name(schema, state.0 as usize),
            closure_child_name(schema, state.0 as usize),
            claim_closure_batch_child_name(schema, state.0 as usize),
        ];
        if Self::relations_all_empty_tx(&mut tx, &current_children).await? {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedIdle { slot: state.0 });
        }

        let next_generation = state.1 + 1;

        if !self
            .append_ring_rotation_tx(&mut tx, RingFamily::Claim, next_generation, next_slot)
            .await?
        {
            // Lost the ledger CAS race. Report the bounded presence
            // evidence we sampled before giving up.
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy {
                slot: next_slot,
                busy: BusyCounts {
                    claims: busy_indicator(claim_busy || claim_batch_busy),
                    closures: busy_indicator(closure_busy),
                    closure_batches: busy_indicator(closure_batch_busy),
                    ..Default::default()
                },
            });
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(RotateOutcome::Rotated {
            slot: next_slot,
            generation: next_generation,
        })
    }

    /// ADR-023 claim-ring prune. Parallel of `prune_oldest_leases`.
    ///
    /// Reclaims the oldest initialized (sealed) claim-ring slot by
    /// `TRUNCATE`-ing its `lease_claims_<slot>`,
    /// `lease_claim_batches_<slot>`, `lease_claim_closures_<slot>`, and
    /// `lease_claim_closure_batches_<slot>` children. Takes the full ADR-023
    /// lock sequence:
    ///
    /// 1. The claim-ring rotation advisory lock (serialises with rotate;
    ///    #371 — replaces the pre-ledger `FOR UPDATE` on the
    ///    `claim_ring_state` singleton).
    /// 2. `FOR UPDATE` on the target `claim_ring_slots` row.
    /// 3. Proves every claim in the sealed partition has closure evidence.
    /// 4. `LOCK TABLE ACCESS EXCLUSIVE` on all claim-ring children with a
    ///    short transaction-local `lock_timeout` (serialises with in-flight
    ///    claim/complete/rescue writers; gives up under sustained
    ///    contention).
    /// 5. Rechecks the slot is not the current one and that every
    ///    claim still has closure evidence.
    /// 6. `TRUNCATE` all claim-ring children in a single statement.
    ///
    /// The "every claim has closure evidence" precondition is what ADR-023
    /// calls `PartitionTruncateSafety`. If an open claim remains in the
    /// partition, prune returns `SkippedActive` and the claim has to
    /// drain by normal completion or be rescued by
    /// `rescue_stale_receipt_claims_tx` before prune will try again.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.prune_oldest_claims")]
    pub async fn prune_oldest_claims(&self, pool: &PgPool) -> Result<PruneOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        // Claim-ring advisory lock: first resource in `PruneClaimsPlan`
        // (`correctness/storage/AwaStorageLockOrder.tla`); serialises
        // with rotate_claims and parallel pruners.
        if !self
            .try_ring_rotation_lock_tx(&mut tx, RingFamily::Claim)
            .await?
        {
            let (current_slot, generation) =
                self.ring_cursor_tx(&mut tx, RingFamily::Claim).await?;
            let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Claim).await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(
                match oldest_initialized_ring_slot(current_slot, generation, slot_count) {
                    Some((slot, _)) => PruneOutcome::Blocked { slot },
                    None => PruneOutcome::Noop,
                },
            );
        }

        let (current_slot, generation) = self.ring_cursor_tx(&mut tx, RingFamily::Claim).await?;
        let slot_count = self.ring_slot_count_tx(&mut tx, RingFamily::Claim).await?;
        let state = (current_slot, generation, slot_count);

        let Some((slot, _generation)) = oldest_initialized_ring_slot(state.0, state.1, state.2)
        else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::Noop);
        };

        // Lock the slot row so concurrent rotate/prune observe the same
        // state machine transition.
        let slot_locked: Option<i32> = sqlx::query_scalar(&format!(
            r#"
            SELECT slot FROM {schema}.claim_ring_slots
            WHERE slot = $1
            FOR UPDATE
            "#
        ))
        .bind(slot)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if slot_locked.is_none() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::Noop);
        }

        let claim_child = claim_child_name(schema, slot as usize);
        let claim_batch_child = claim_batch_child_name(schema, slot as usize);
        let closure_child = closure_child_name(schema, slot as usize);
        let closure_batch_child = claim_closure_batch_child_name(schema, slot as usize);

        // Before taking ACCESS EXCLUSIVE on the child partitions, prove
        // the sealed slot is actually reclaimable. This optimistic proof
        // avoids blocking claim/complete traffic behind a prune attempt
        // that is already known to be doomed. A claimer that read the
        // old current slot before rotation may still commit while the
        // exclusive lock waits, so the open-claim proof is repeated once
        // the child locks are held.
        let open_claims = claim_prune_has_open_claims_tx(
            &mut tx,
            schema,
            &claim_child,
            &claim_batch_child,
            &closure_child,
            &closure_batch_child,
        )
        .await?;

        if open_claims {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::ClaimOpen,
                count: busy_indicator(open_claims),
            });
        }

        set_prune_lock_timeout_tx(&mut tx, self.prune_lock_timeout).await?;

        let lock_tables = sqlx::query(&format!(
            "LOCK TABLE {claim_child}, {claim_batch_child}, {closure_child}, {closure_batch_child} IN ACCESS EXCLUSIVE MODE"
        ))
        .execute(tx.as_mut())
        .await;

        if let Err(err) = lock_tables {
            let _ = tx.rollback().await;
            if is_lock_contention_error(&err) {
                return Ok(PruneOutcome::Blocked { slot });
            }
            return Err(map_sqlx_error(err));
        }

        // After taking ACCESS EXCLUSIVE, recheck that the slot is still
        // not the current one. The ring rotation lock should already
        // make this stable, but keeping the explicit gate documents the
        // truncate precondition.
        let (current_slot, _) = self.ring_cursor_tx(&mut tx, RingFamily::Claim).await?;

        if current_slot == slot {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::ClaimCurrent,
                count: 0,
            });
        }

        let open_claims = claim_prune_has_open_claims_tx(
            &mut tx,
            schema,
            &claim_child,
            &claim_batch_child,
            &closure_child,
            &closure_batch_child,
        )
        .await?;
        if open_claims {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::ClaimOpen,
                count: busy_indicator(open_claims),
            });
        }

        let truncate = sqlx::query(&format!(
            "TRUNCATE TABLE {claim_child}, {claim_batch_child}, {closure_child}, {closure_batch_child}"
        ))
        .execute(tx.as_mut())
        .await;

        match truncate {
            Ok(_) => {
                sqlx::query(&format!(
                    r#"
                    UPDATE {schema}.claim_ring_slots
                    SET rescue_cursor_claimed_at = '-infinity'::timestamptz,
                        rescue_cursor_job_id = 0,
                        rescue_cursor_run_lease = 0,
                        deadline_cursor_deadline_at = '-infinity'::timestamptz,
                        deadline_cursor_job_id = 0,
                        deadline_cursor_run_lease = 0,
                        batch_deadline_cursor_deadline_at = '-infinity'::timestamptz,
                        batch_deadline_cursor_batch_id = 0
                    WHERE slot = $1
                    "#
                ))
                .bind(slot)
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
                tx.commit().await.map_err(map_sqlx_error)?;
                Ok(PruneOutcome::Pruned {
                    slot,
                    carried_failed_rows: 0,
                })
            }
            Err(err) if is_lock_contention_error(&err) => {
                let _ = tx.rollback().await;
                Ok(PruneOutcome::Blocked { slot })
            }
            Err(err) => {
                let _ = tx.rollback().await;
                Err(map_sqlx_error(err))
            }
        }
    }

    fn job_id_sequence(&self) -> String {
        format!("{}.job_id_seq", self.schema())
    }

    fn leases_table(&self) -> String {
        format!("{}.{}", self.schema(), self.leases_relname())
    }

    fn attempt_state_table(&self) -> String {
        format!("{}.{}", self.schema(), self.attempt_state_relname())
    }
}
