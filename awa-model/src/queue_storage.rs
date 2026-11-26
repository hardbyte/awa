use crate::admin::{CallbackConfig, CallbackPollResult};
use crate::bulk::{self, BulkBatch, BulkCursor, BulkError, BulkIds};
use crate::dlq::{ListDlqFilter, RetryFromDlqOpts};
use crate::error::AwaError;
use crate::insert::prepare_row_raw;
use crate::isolation::BeginReadCommitted;
use crate::storage_footprint_sql::brin_fsm_empty_pages_sql;
use crate::{InsertParams, JobRow, JobState};
use chrono::TimeDelta;
use chrono::{DateTime, SubsecRound, Utc};
use sqlx::Acquire;
use sqlx::{FromRow, PgPool, Postgres, Row};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use uuid::Uuid;

const DEFAULT_SCHEMA: &str = "awa";
const DEFAULT_QUEUE_SLOT_COUNT: usize = 16;
const DEFAULT_LEASE_SLOT_COUNT: usize = 8;
const DEFAULT_CLAIM_SLOT_COUNT: usize = 8;
const DEFAULT_QUEUE_STRIPE_COUNT: usize = 1;
const QUEUE_STRIPE_DELIMITER: &str = "#";
const COPY_NULL_SENTINEL: &str = "__AWA_NULL__";
const COPY_CHUNK_TARGET_BYTES: usize = 256 * 1024;
/// Claim and claim-cursor transactions commit without waiting for the WAL
/// flush. A claim lost to a server crash leaves its job claimable again, and
/// any later synchronous commit flushes every earlier claim first.
const CLAIM_TRANSACTION_BEGIN: &str =
    crate::isolation::begin_read_committed_with!("SET LOCAL synchronous_commit = off");
const TERMINAL_COUNTER_BUCKETS: i16 = 256;
const RECEIPT_RESCUE_BATCH_LIMIT: i64 = 500;
const RESCUED_DUPLICATE_ERROR: &str = "rescued as duplicate: unique claim held by a newer job";
pub(crate) const SNOOZED_DUPLICATE_ERROR: &str =
    "snoozed as duplicate: unique claim held by a newer job";
pub(crate) const RETRIED_DUPLICATE_ERROR: &str =
    "retried as duplicate: unique claim held by a newer job";
pub(crate) const RESCHEDULED_DUPLICATE_ERROR: &str =
    "rescheduled as duplicate: unique claim held by a newer job";
/// The error a promotion writes when it cancels a due job as a duplicate.
#[doc(hidden)]
pub const PROMOTED_DUPLICATE_ERROR: &str =
    "promoted as duplicate: unique claim held by another job";
/// The errors canonical rescue writes when it cancels a stale, expired or
/// timed-out attempt as a duplicate.
#[doc(hidden)]
pub const RESCUED_STALE_DUPLICATE_ERROR: &str =
    "rescued as duplicate: heartbeat stale and unique claim held by a newer job";
#[doc(hidden)]
pub const RESCUED_EXPIRED_DUPLICATE_ERROR: &str =
    "rescued as duplicate: deadline expired and unique claim held by a newer job";
#[doc(hidden)]
pub const RESCUED_TIMED_OUT_DUPLICATE_ERROR: &str =
    "rescued as duplicate: callback timed out and unique claim held by a newer job";
const DRAIN_BATCH_SIZE: i64 = crate::bulk::DEFAULT_BATCH_SIZE;

/// Most jobs [`QueueStorage::cancel_jobs_in_batches`] cancels per
/// transaction. Cancelling by ID costs several milliseconds a job, most of
/// it planning the job's statements against the partitioned ring tables,
/// and the transaction holds the lanes' claim heads, so 1,000 jobs held them
/// for seconds; 250 keeps a chunk around two seconds.
pub const CANCEL_BY_ID_CHUNK: i64 = 250;

/// Columns of a `DlqJobRow` read from `dlq_entries`.
const DLQ_ROW_COLUMNS: &str = "job_id, kind, queue, args, state, priority, attempt, run_lease, \
     max_attempts, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, \
     COALESCE(payload, '{}'::jsonb) AS payload, dlq_reason, dlq_at, original_run_lease";

/// The `ListDlqFilter` predicate over `dlq_entries`, binding kind, queue,
/// tag, before_id and before_dlq_at as `$1` to `$5`.
pub(crate) const DLQ_FILTER_SQL: &str = r#"($1::text IS NULL OR kind = $1)
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
    )"#;
// The planner overestimates the drain statements enough to JIT-compile them,
// which takes seconds against milliseconds of execution.
const DRAIN_TRANSACTION_BEGIN: &str =
    crate::isolation::begin_read_committed_with!("SET LOCAL jit = off");
const DRAIN_CLAIM_CHECK_ROWS: i64 = 64;
/// How long a maintenance claim-cursor advance waits for any lock it does
/// not take with `SKIP LOCKED` or `NOWAIT`.
const CLAIM_CURSOR_ADVANCE_LOCK_TIMEOUT: Duration = Duration::from_millis(100);
// Rescue cursor scans are estimated far above their real cost, enough to
// JIT-compile them, which takes seconds against about 100 ms of execution.
const RESCUE_TRANSACTION_BEGIN: &str =
    crate::isolation::begin_read_committed_with!("SET LOCAL jit = off");
const RECEIPT_RESCUE_CURSOR_SCAN_LIMIT: i64 = 10_000;
const RECEIPT_DEADLINE_RESCUE_CURSOR_SCAN_LIMIT: i64 = 10_000;
const DEFAULT_PRUNE_LOCK_TIMEOUT: Duration = Duration::from_millis(10);

// Whether a ring child holds storage beyond the metadata an empty table keeps:
// any heap, TOAST, free-space-map or visibility-map page, or index pages beyond
// one btree/GiST page and three BRIN main-fork pages plus the BRIN FSM. Other
// index methods count from their first page.
const RECLAIMABLE_CHILD_STORAGE_SQL: &str = concat!(
    r#"
WITH heaps AS (
    SELECT oid, reltoastrelid FROM pg_class
    WHERE oid = $1::regclass AND relkind = 'r'
), heap_relations AS (
    SELECT oid FROM heaps
    UNION ALL
    SELECT reltoastrelid FROM heaps WHERE reltoastrelid <> 0
), storage_relations AS (
    SELECT oid, NULL::name AS amname FROM heap_relations
    UNION ALL
    SELECT i.indexrelid, am.amname FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    JOIN pg_am am ON am.oid = c.relam
    WHERE i.indrelid IN (SELECT oid FROM heap_relations)
)
SELECT EXISTS (
    SELECT 1 FROM storage_relations
    CROSS JOIN (SELECT current_setting('block_size')::bigint AS block_size) settings
    WHERE pg_relation_size(oid, 'fsm') > block_size *
          CASE amname WHEN 'brin' THEN "#,
    brin_fsm_empty_pages_sql!(),
    r#" ELSE 0 END
       OR pg_relation_size(oid, 'vm') > 0
       OR pg_relation_size(oid) > block_size *
          CASE amname WHEN 'btree' THEN 1 WHEN 'gist' THEN 1
                      WHEN 'brin' THEN 3 ELSE 0 END
)
"#
);

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

/// SplitMix64 finalizer over an [`ordering_key_hash64`] value, used to pick
/// a queue stripe.
fn stripe_hash(hash: u64) -> u64 {
    let mut z = hash;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
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
    /// Use the receipt-plane short path for zero-deadline jobs:
    /// claim writes compact batches into `lease_claim_batches` for the
    /// zero-deadline hot path, while deadline-backed attempts keep row-local
    /// evidence in `lease_claims` for indexed deadline rescue. Compact claims
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
    /// Compact claim batch row identity for zero-deadline receipt claims.
    /// Row-local receipt claims leave this unset.
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

/// Whether a batch completion waits for each attempt's completion lock or
/// leaves out attempts whose lock another transaction holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptLocks {
    Wait,
    Try,
}

/// The result of [`QueueStorage::complete_runtime_batch_isolating`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeBatchCompletion {
    /// `(job_id, run_lease)` of attempts this batch completed.
    pub updated: Vec<(i64, i64)>,
    /// `(job_id, run_lease)` of attempts left out because another
    /// transaction held their completion lock. They are neither completed
    /// nor stale: the caller completes each on its own.
    pub contended: Vec<(i64, i64)>,
}

#[derive(Debug, Clone)]
pub struct ClaimedRuntimeJob {
    pub claim: ClaimedEntry,
    pub job: JobRow,
    pub unique_states: Option<String>,
}

/// Identity of a compact receipt claim for heartbeat purposes: addresses
/// `lease_claim_batches` by primary key and the attempt by item index, so a
/// heartbeat does not have to scan every open claim batch. Built with
/// [`ClaimHeartbeatRef::from_claim`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClaimHeartbeatRef {
    pub job_id: i64,
    pub run_lease: i64,
    pub claim_slot: i32,
    pub claim_batch_id: i64,
    pub claim_batch_index: i32,
    pub receipt_id: i64,
}

impl ClaimHeartbeatRef {
    /// The heartbeat identity of a compact receipt claim, or `None` for
    /// legacy or row-local claims.
    pub fn from_claim(job_id: i64, run_lease: i64, claim: &ClaimedEntry) -> Option<Self> {
        if !claim.lease_claim_receipt {
            return None;
        }
        Some(Self {
            job_id,
            run_lease,
            claim_slot: claim.claim_slot,
            claim_batch_id: claim.claim_batch_id?,
            claim_batch_index: claim.claim_batch_index?,
            receipt_id: claim.receipt_id?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct QueueClaimerLease {
    pub claimer_slot: i16,
    pub lease_epoch: i64,
}

/// Sum of `enqueue cursor - claim cursor` over the lanes of the physical
/// queues bound as `$1`: the claimer signal (see
/// `QueueStorage::queue_claimer_signal`).
fn claimer_signal_sql(schema: &str) -> String {
    format!(
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
    )
}

/// How long a claim may reuse this instance's cached claimer lease before
/// re-reading the lease row and the queue's claimer target.
const CLAIMER_GATE_CACHE_TTL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy)]
struct CachedClaimerGate {
    checked_at: Instant,
    lease: QueueClaimerLeaseRow,
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
}

/// Outcome of an idle-gated rotation: [`QueueStorage::rotate_if_active`],
/// [`QueueStorage::rotate_leases_if_active`] or
/// [`QueueStorage::rotate_claims_if_active`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatedRotateOutcome {
    /// The ring held at least one row, so the rotation ran.
    Attempted(RotateOutcome),
    /// Every partition of the ring was empty. The rotation was not attempted
    /// and no ring-state row was locked or written; `slot` and `generation`
    /// are the unchanged cursor.
    SkippedIdle { slot: i32, generation: i64 },
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

/// Which claim slot a rescue batch visits first. A batch stops at its row
/// limit, so whichever slot it visits first gets most of a busy batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RescueSlotOrder {
    /// The oldest initialized claim slot, then the rest. The claim ring
    /// cannot rotate back into that slot until its claims are closed.
    #[default]
    OldestFirst,
    /// Every other slot, starting at this offset (modulo their count), then
    /// the oldest, so a long drain of the oldest slot does not starve the rest.
    OthersFirst(usize),
}

/// Options for one rescue batch ([`QueueStorage::rescue_stale_heartbeats_batch`]
/// and its deadline and callback counterparts).
#[derive(Debug, Clone, Default)]
pub struct RescueBatchOptions {
    pub slot_order: RescueSlotOrder,
    /// Claim slots to leave out, typically ones an earlier batch of the same
    /// pass found nothing in ([`RescueBatch::idle_slots`]).
    pub skip_slots: Vec<i32>,
    /// How many claims past its cursor each slot's stale-heartbeat or
    /// row-local deadline walk examines (default 10,000). Every claim
    /// examined costs its closure checks, so a batch that follows one that
    /// filled up on a dense backlog can examine far fewer.
    pub cursor_scan_limit: Option<usize>,
}

/// The outcome of one rescue batch, which runs in one transaction.
#[derive(Debug, Default)]
pub struct RescueBatch {
    pub rescued: Vec<JobRow>,
    /// A scan window or rescue quota was exhausted, so another bounded
    /// batch may find eligible attempts.
    pub saturated: bool,
    /// Claim slots whose bounded scans completed without eligible attempts.
    pub idle_slots: Vec<i32>,
    /// Last cursor reached by each bounded receipt scan. A caller continuing
    /// batches can stop a sweep once its cursor returns to its first window.
    pub scan_progress: Vec<RescueScanProgress>,
}

/// Cursor progress from a bounded receipt rescue scan.
#[derive(Debug, Clone)]
pub struct RescueScanProgress {
    pub slot: i32,
    /// One cursor per representation, in a stable order. A slot is drained
    /// only after every representation has completed its sweep.
    pub cursors: Vec<RescueScanCursor>,
}

/// Progress of one independently ordered receipt representation.
#[derive(Debug, Clone)]
pub struct RescueScanCursor {
    pub position: Option<(DateTime<Utc>, i64, i64)>,
    pub saturated: bool,
}

/// Receipt claims one rescue batch took, and what it found on the way.
#[derive(Default)]
struct ReceiptRescueBatch {
    rescued: Vec<DeletedLeaseRow>,
    saturated: bool,
    idle_slots: Vec<i32>,
    scan_progress: Vec<RescueScanProgress>,
}

type ReceiptRescueScan = (
    Vec<DeletedLeaseRow>,
    bool,
    Option<(DateTime<Utc>, i64, i64)>,
);
type ReceiptRescueScanRow = (
    sqlx::types::Json<Vec<DeletedLeaseRow>>,
    bool,
    Option<DateTime<Utc>>,
    Option<i64>,
    Option<i64>,
);

/// Where a rescue sends an exhausted attempt on a queue whose DLQ is enabled.
struct RescueDlq<'a> {
    reason: &'static str,
    enabled: &'a (dyn Fn(&str) -> bool + Sync),
}

/// Split rescued attempts by the state they left, keeping first-seen order,
/// because each terminal or deferred insert releases unique claims for one
/// previous state.
fn group_by_previous_state<T>(
    rows: Vec<(LeaseTransitionRow, T)>,
) -> Vec<(JobState, Vec<(LeaseTransitionRow, T)>)> {
    let mut groups: Vec<(JobState, Vec<(LeaseTransitionRow, T)>)> = Vec::new();
    for (row, settled) in rows {
        let state = row.state;
        match groups
            .iter_mut()
            .find(|(group_state, _)| *group_state == state)
        {
            Some((_, group)) => group.push((row, settled)),
            None => groups.push((state, vec![(row, settled)])),
        }
    }
    groups
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

/// A `(queue, priority)` enqueue lane as promotion addresses it. Promotion
/// picks the shard when it re-enqueues a row, so the shard is not part of it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PromotionLane {
    pub queue: String,
    pub priority: i16,
}

/// Outcome of [`QueueStorage::promote_due_lanes`] and
/// [`QueueStorage::promote_due_lane`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PromoteOutcome {
    /// Rows moved into the ready ring.
    pub promoted: usize,
    /// Due rows the batch selected, including those left deferred on a
    /// contended lane. A batch that selected its full size may have left
    /// further due rows behind.
    pub selected: usize,
    /// Lanes whose locks were held past `lock_timeout`, so their promotion
    /// transaction rolled back. Their rows stay deferred.
    pub contended: Vec<PromotionLane>,
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

/// A ready lane with rows at or past its claim cursor in the oldest sealed
/// queue ring slot, which keeps queue prune from reclaiming that slot. See
/// [`QueueStorage::queue_ring_pin`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PendingReadyLane {
    pub queue: String,
    pub priority: i16,
    pub enqueue_shard: i16,
    /// `lane_seq` of the lane's first row at or past its claim cursor.
    pub head_lane_seq: i64,
    /// `run_at` of that row: when it became ready.
    pub head_run_at: DateTime<Utc>,
    /// How long that row has been ready, by the database clock.
    pub head_age: Duration,
}

/// A lane whose claim cursor [`QueueStorage::advance_spent_claim_heads`]
/// moved over spent rows.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SpentClaimHeadAdvance {
    pub queue: String,
    pub priority: i16,
    pub enqueue_shard: i16,
    /// The lane's claim cursor before the advance.
    pub from_seq: i64,
    /// The lane's claim cursor after it: the lane's first unspent row, or
    /// one past the last spent row the advance read.
    pub to_seq: i64,
}

/// A lane [`QueueStorage::advance_spent_claim_heads`] could not advance
/// because of an error other than lock contention or a failed connection;
/// a failed connection fails the whole call.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SpentClaimHeadFailure {
    pub queue: String,
    pub priority: i16,
    pub enqueue_shard: i16,
    pub error: String,
}

/// What one [`QueueStorage::advance_spent_claim_heads`] call did, lane by
/// lane. A lane skipped for lock contention appears in neither list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SpentClaimHeadAdvances {
    pub advanced: Vec<SpentClaimHeadAdvance>,
    pub failed: Vec<SpentClaimHeadFailure>,
}

/// The oldest sealed queue ring slot and the lanes whose pending ready rows
/// keep queue prune from reclaiming it. See [`QueueStorage::queue_ring_pin`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct QueueRingPin {
    pub slot: i32,
    pub generation: i64,
    /// Oldest head first.
    pub lanes: Vec<PendingReadyLane>,
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

/// A failure of the connection itself, after which whether the statement
/// took effect is unknown: an I/O, TLS or protocol error, or SQLSTATE class 08.
fn is_connection_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::Protocol(_) => true,
        sqlx::Error::Database(db_err) => db_err.code().is_some_and(|code| code.starts_with("08")),
        _ => false,
    }
}

/// A lock timeout, or a deadlock in which the prune was the victim.
fn is_prune_contention_error(err: &sqlx::Error) -> bool {
    matches!(
        err,
        sqlx::Error::Database(db_err)
            if matches!(db_err.code().as_deref(), Some("55P03" | "40P01"))
    )
}

async fn set_lock_timeout_tx(
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

/// The longest single wait for prune's `ACCESS EXCLUSIVE` upgrade when the
/// prune lock timeout is shorter than this.
const PRUNE_LOCK_UPGRADE_CEILING: Duration = Duration::from_millis(320);

/// Partitions of a queue ring slot that queue prune locks in one statement.
const QUEUE_PRUNE_LOCKED_PARTITIONS: u32 = 8;

/// The longest single wait of [`acquire_prune_exclusive_lock_tx`] for `base`.
fn prune_lock_upgrade_ceiling(base: Duration) -> Duration {
    base.max(PRUNE_LOCK_UPGRADE_CEILING)
}

/// Take `ACCESS EXCLUSIVE` on `tables` for a prune's truncate.
///
/// A pending `ACCESS EXCLUSIVE` request stalls every new reader of the
/// tables, so each attempt waits a bounded time and then lets the readers
/// through before the next, longer attempt. The first attempt waits `base`
/// and each later one twice as long, up to [`prune_lock_upgrade_ceiling`].
/// A deadlock reported for the request counts as contention: PostgreSQL
/// raises it at once when a transaction that already reads one of the tables
/// is waiting to write a table the prune holds in `SHARE ROW EXCLUSIVE`. The
/// last failure is returned, and `lock_timeout` is back at `base` on success.
async fn acquire_prune_exclusive_lock_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tables: &str,
    base: Duration,
) -> Result<(), AwaError> {
    let ceiling = prune_lock_upgrade_ceiling(base);
    let mut timeout = base;
    loop {
        sqlx::query("SAVEPOINT awa_prune_lock_upgrade")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        set_lock_timeout_tx(tx, timeout).await?;
        let locked = sqlx::query(&format!("LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE"))
            .execute(tx.as_mut())
            .await;
        match locked {
            Ok(_) => {
                sqlx::query("RELEASE SAVEPOINT awa_prune_lock_upgrade")
                    .execute(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;
                return set_lock_timeout_tx(tx, base).await;
            }
            Err(err) if is_prune_contention_error(&err) && timeout < ceiling => {
                sqlx::query("ROLLBACK TO SAVEPOINT awa_prune_lock_upgrade")
                    .execute(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;
                tokio::time::sleep(base).await;
                timeout = timeout.saturating_mul(2).min(ceiling);
            }
            Err(err) => return Err(map_sqlx_error(err)),
        }
    }
}

/// Commit or roll back a maintenance transaction that has set the prune
/// `lock_timeout`.
///
/// The timeout applies to every later statement in the transaction, not only
/// the explicit `LOCK TABLE`: an insert, update or read of another relation can
/// also queue behind a conflicting lock (autovacuum's heap truncation takes
/// `ACCESS EXCLUSIVE`, for example). A lock timeout anywhere, or a deadlock
/// that PostgreSQL resolves by cancelling the prune, therefore rolls the whole
/// transaction back and reports `blocked`, so the caller backs off and retries
/// instead of surfacing an error.
async fn settle_prune_tx<T>(
    tx: sqlx::Transaction<'_, Postgres>,
    result: Result<T, AwaError>,
    blocked: T,
) -> Result<T, AwaError> {
    match result {
        Ok(outcome) => {
            tx.commit().await.map_err(map_sqlx_error)?;
            Ok(outcome)
        }
        Err(err) => {
            let _ = tx.rollback().await;
            match err {
                AwaError::Database(ref db_err) if is_prune_contention_error(db_err) => Ok(blocked),
                err => Err(err),
            }
        }
    }
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
    done_ready_join_from(
        done_alias,
        ready_alias,
        &format!("{schema}.ready_entries"),
        &format!("{schema}.ready_tombstones"),
    )
}

// Old writers could store effective priority in a narrow terminal row. Prefer
// the live body of that exact claimed attempt, while retaining same-lease,
// exact-lane bodies tombstoned by an available-job cancellation. Choose one
// body so moved source rows cannot multiply terminal mutations or counters.
fn done_ready_join_from(
    done_alias: &str,
    ready_alias: &str,
    ready_relation: &str,
    tombstone_relation: &str,
) -> String {
    format!(
        r#"
            LEFT JOIN LATERAL (
                SELECT candidate.*
                FROM (
                    SELECT body.*,
                        (
                            body.run_lease + 1 = {done_alias}.run_lease
                            AND NOT EXISTS (
                                SELECT 1 FROM {tombstone_relation} AS tomb
                                WHERE tomb.ready_slot = body.ready_slot
                                  AND tomb.ready_generation = body.ready_generation
                                  AND tomb.queue = body.queue
                                  AND tomb.priority = body.priority
                                  AND tomb.enqueue_shard = body.enqueue_shard
                                  AND tomb.lane_seq = body.lane_seq
                                  AND tomb.job_id = body.job_id
                            )
                        ) AS live_attempt_body
                    FROM {ready_relation} AS body
                    WHERE body.ready_slot = {done_alias}.ready_slot
                      AND body.ready_generation = {done_alias}.ready_generation
                      AND body.queue = {done_alias}.queue
                      AND body.enqueue_shard = {done_alias}.enqueue_shard
                      AND body.lane_seq = {done_alias}.lane_seq
                      AND body.job_id = {done_alias}.job_id
                      AND (body.run_lease = {done_alias}.run_lease
                           OR body.run_lease + 1 = {done_alias}.run_lease)
                ) AS candidate
                WHERE candidate.priority = {done_alias}.priority
                   OR candidate.live_attempt_body
                ORDER BY candidate.live_attempt_body DESC,
                         (candidate.priority = {done_alias}.priority) DESC,
                         candidate.priority
                LIMIT 1
            ) AS {ready_alias} ON TRUE
        "#
    )
}

fn done_payload_projection(done_alias: &str, ready_alias: &str) -> String {
    // A narrow old terminal payload can lack metadata from its live moved
    // body. Supply only metadata defaults; terminal errors/tags/progress and
    // explicit metadata overrides remain authoritative. Keep default {} elision.
    format!(
        r#"CASE
            WHEN {done_alias}.args IS NULL
             AND {ready_alias}.live_attempt_body
             AND COALESCE({ready_alias}.payload -> 'metadata', '{{}}'::jsonb) <> '{{}}'::jsonb
            THEN COALESCE({done_alias}.payload, {ready_alias}.payload, '{{}}'::jsonb)
                || jsonb_build_object('metadata',
                    COALESCE({ready_alias}.payload -> 'metadata', '{{}}'::jsonb)
                    || COALESCE({done_alias}.payload -> 'metadata', '{{}}'::jsonb))
            ELSE COALESCE({done_alias}.payload, {ready_alias}.payload, '{{}}'::jsonb)
        END"#
    )
}

fn done_row_projection(done_alias: &str, ready_alias: &str) -> String {
    let payload = done_payload_projection(done_alias, ready_alias);
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
                {payload} AS payload
        "#
    )
}

fn lease_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.leases_{slot}")
}

fn claim_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.lease_claims_{slot}")
}

/// SQL true when `column` names queue `param` or one of its stripes
/// (`queue#N`).
fn queue_and_stripes_sql(column: &str, param: &str) -> String {
    format!(
        "({column} = {param} \
         OR (left({column}, char_length({param}) + 1) = {param} || '{QUEUE_STRIPE_DELIMITER}' \
             AND substr({column}, char_length({param}) + 2) ~ '^[0-9]+$'))"
    )
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

/// The number of receipt claims a claim slot holds, row-local and compact.
/// Reads only the slot's claim partitions.
pub(crate) fn claim_slot_claimed_count_sql(schema: &str, slot: usize) -> String {
    format!(
        "SELECT (SELECT count(*) FROM {claims})::bigint \
              + (SELECT COALESCE(sum(claimed_count), 0) FROM {batches})::bigint",
        claims = claim_child_name(schema, slot),
        batches = claim_batch_child_name(schema, slot),
    )
}

/// The number of receipt claims the closure ledgers of a claim slot close,
/// row-local and compact. When it equals [`claim_slot_claimed_count_sql`],
/// every claim in the slot is closed: the proof claim prune makes before it
/// truncates the slot (see `claim_prune_has_open_claims_tx`). Reads only the
/// slot's closure partitions.
pub(crate) fn claim_slot_closed_count_sql(schema: &str, slot: usize) -> String {
    format!(
        "SELECT (SELECT count(*) FROM {closures})::bigint \
              + (SELECT COALESCE(sum(closed_count), 0) FROM {closure_batches})::bigint",
        closures = closure_child_name(schema, slot),
        closure_batches = claim_closure_batch_child_name(schema, slot),
    )
}

pub(crate) fn ring_slot_number(slot: i32, ring: &str) -> Result<usize, AwaError> {
    usize::try_from(slot).map_err(|_| {
        AwaError::Validation(format!(
            "invalid {ring} ring slot {slot}: slot must be non-negative"
        ))
    })
}

fn ring_slot_index(slot: i32, slot_count: usize, ring: &str) -> Result<usize, AwaError> {
    let index = ring_slot_number(slot, ring)?;
    if index >= slot_count {
        return Err(AwaError::Validation(format!(
            "invalid {ring} ring slot {slot}: configured slot count is {slot_count}"
        )));
    }
    Ok(index)
}

enum ReceiptEvidence {
    Closed,
    Open,
}

fn receipt_closed_evidence_sql(
    schema: &str,
    closure_rel: &str,
    closure_batch_rel: &str,
    claims_alias: &str,
) -> String {
    receipt_evidence_sql(
        schema,
        closure_rel,
        closure_batch_rel,
        claims_alias,
        ReceiptEvidence::Closed,
    )
}

fn receipt_evidence_sql(
    schema: &str,
    closure_rel: &str,
    closure_batch_rel: &str,
    claims_alias: &str,
    evidence: ReceiptEvidence,
) -> String {
    let (null_test, join, exists, range_fence) = match evidence {
        ReceiptEvidence::Closed => ("NOT NULL", "OR", "EXISTS", ""),
        ReceiptEvidence::Open => (
            "NULL",
            "AND",
            "NOT EXISTS",
            "
                -- Keep receipt membership correlated while the other closure
                -- exclusions can use full-identity anti joins.
                OFFSET 0",
        ),
    };
    // `receipt_id` is allocated from a global sequence and identifies a claim.
    // Closures retain that claim's slot; match it to prune unrelated partitions.
    // Compact closure batches retain the raw receipt array for audit/debugging,
    // but hot membership checks use the per-child GiST index on the multirange.
    format!(
        r#"
        (
            {claims_alias}.closed_at IS {null_test}
            {join} {exists} (
                SELECT 1 FROM {closure_rel} AS closures
                WHERE closures.claim_slot = {claims_alias}.claim_slot
                  AND closures.job_id = {claims_alias}.job_id
                  AND closures.run_lease = {claims_alias}.run_lease
            )
            {join} {exists} (
                SELECT 1
                FROM {closure_batch_rel} AS closure_batches
                WHERE closure_batches.claim_slot = {claims_alias}.claim_slot
                  AND closure_batches.receipt_ranges @> {claims_alias}.receipt_id{range_fence}
            )
            {join} {exists} (
                SELECT 1 FROM {schema}.done_entries AS done
                WHERE done.job_id = {claims_alias}.job_id
                  AND done.run_lease = {claims_alias}.run_lease
            )
            {join} {exists} (
                SELECT 1 FROM {schema}.deferred_jobs AS deferred
                WHERE deferred.job_id = {claims_alias}.job_id
                  AND deferred.run_lease = {claims_alias}.run_lease
            )
            {join} {exists} (
                SELECT 1 FROM {schema}.dlq_entries AS dlq
                WHERE dlq.job_id = {claims_alias}.job_id
                  AND dlq.run_lease = {claims_alias}.run_lease
            )
        )
        "#
    )
}

/// A subquery that yields the identity `(job_id, run_lease, queue)` of every
/// receipt claim currently open (i.e. `running`) but not yet materialised
/// into `leases`. Zero-deadline receipts use compact `lease_claim_batches`;
/// deadline-backed receipts retain row-local `lease_claims` for indexed
/// deadline rescue. The open set unions those row-local claims with the
/// unnested compact members, excluding every durable closure, terminal, and
/// materialised-lease evidence shape.
///
/// This is the canonical open-receipt-claim shape (mirrors `queue_counts`'s
/// `live_running` derivation). Compose it wherever a running enumeration must
/// see receipt claims: wrap in `(SELECT count(*) FROM (<sql>) …)`, join it, or
/// `UNION ALL` its rows into a `current_jobs`-style view. `waiting_external`
/// deliberately does NOT read this set — a claim can only reach
/// `waiting_external` after `register_callback` materialises it into `leases`
/// (see `enter_callback_wait_in_tx`), so those legs stay `leases`-only.
///
/// `leases` here is materialised-claim evidence; a claim present in `leases`
/// is reported by the caller's own `leases` leg, so it is excluded here to
/// avoid double counting.
pub(crate) fn open_receipt_running_claims_sql(schema: &str) -> String {
    open_receipt_running_claims_sql_for(schema, None)
}

/// [`open_receipt_running_claims_sql`] restricted to the jobs in `job_ids`,
/// an SQL expression yielding `bigint[]`. The restriction applies before the
/// closure and materialisation checks. Row-local claims are found through the
/// `job_id` index. Compact claim batches are found through `job_ids && {ids}`,
/// which uses a claim partition's GIN index on `job_ids` where it has one
/// (see `ensure_claim_batch_job_index_tx`) and otherwise reads every retained
/// batch header; only batches holding one of the jobs are unnested and checked.
pub(crate) fn open_receipt_running_claims_sql_for(schema: &str, job_ids: Option<&str>) -> String {
    open_receipt_running_claims_sql_from(schema, job_ids, None)
}

/// The same open-claim projection, scoped to one validated physical claim slot.
/// Ready and terminal history are deliberately not restricted to that slot.
pub(crate) fn open_receipt_running_claims_sql_in_slot(schema: &str, slot: usize) -> String {
    open_receipt_running_claims_sql_from(schema, None, Some(slot))
}

fn open_receipt_running_claims_sql_from(
    schema: &str,
    job_ids: Option<&str>,
    slot: Option<usize>,
) -> String {
    let (claims_rel, batches_rel, closure_rel, closure_batch_rel) = match slot {
        Some(slot) => (
            claim_child_name(schema, slot),
            claim_batch_child_name(schema, slot),
            closure_child_name(schema, slot),
            claim_closure_batch_child_name(schema, slot),
        ),
        None => (
            format!("{schema}.lease_claims"),
            format!("{schema}.lease_claim_batches"),
            format!("{schema}.lease_claim_closures"),
            format!("{schema}.lease_claim_closure_batches"),
        ),
    };
    let row_open = receipt_evidence_sql(
        schema,
        &closure_rel,
        &closure_batch_rel,
        "claims",
        ReceiptEvidence::Open,
    );
    let (row_filter, batch_filter) = match job_ids {
        Some(ids) => (
            format!("claims.job_id = ANY({ids}) AND "),
            format!("batches.job_ids && {ids} AND items.job_id = ANY({ids}) AND "),
        ),
        None => (String::new(), String::new()),
    };
    format!(
        r#"
        SELECT claims.job_id, claims.run_lease, claims.queue
        FROM {claims_rel} AS claims
        WHERE {row_filter}{row_open}
          AND NOT EXISTS (
              SELECT 1 FROM {schema}.leases AS lease
              WHERE lease.job_id = claims.job_id
                AND lease.run_lease = claims.run_lease
          )
        UNION ALL
        SELECT items.job_id, items.run_lease, batches.queue
        FROM {batches_rel} AS batches
        CROSS JOIN LATERAL unnest(
            batches.job_ids,
            batches.run_leases,
            batches.receipt_ids
        ) AS items(job_id, run_lease, receipt_id)
        -- Skips, before unnesting, a batch whose receipts closure batches
        -- have all closed: most retained batches, once their jobs finish.
        WHERE {batch_filter}NOT (
                  batches.receipt_ranges <@ COALESCE((
                      SELECT range_agg(closed)
                      FROM {closure_batch_rel} AS closure_batches
                      CROSS JOIN LATERAL unnest(closure_batches.receipt_ranges) AS closed
                      WHERE closure_batches.claim_slot = batches.claim_slot
                        AND closure_batches.receipt_ranges && batches.receipt_ranges
                  ), '{{}}'::int8multirange)
              )
          AND NOT EXISTS (
                  SELECT 1 FROM {closure_rel} AS closures
                  WHERE closures.claim_slot = batches.claim_slot
                    AND closures.job_id = items.job_id
                    AND closures.run_lease = items.run_lease
              )
          AND NOT EXISTS (
                  SELECT 1 FROM {closure_batch_rel} AS closure_batches
                  WHERE closure_batches.claim_slot = batches.claim_slot
                    AND closure_batches.receipt_ranges @> items.receipt_id
                  -- Keep the range lookup correlated: joining on claim_slot can
                  -- multiply receipt members by retained closure batches.
                  OFFSET 0
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

/// Whether a lease still points at a ready row in the slot. Leases of any
/// generation count, for the same reason as the pending-ready gate: a job a
/// REPEATABLE READ producer wrote into an older generation without the
/// `ring_generation_floor` patch (#847) can be claimed after the slot was
/// reused, and truncating the slot would delete its ready row under the
/// running attempt.
async fn queue_prune_has_active_leases_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    schema: &str,
    slot: i32,
) -> Result<bool, AwaError> {
    sqlx::query_scalar(&format!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM {schema}.leases
            WHERE ready_slot = $1
            LIMIT 1
        )
        "#
    ))
    .bind(slot)
    .fetch_one(tx.as_mut())
    .await
    .map_err(map_sqlx_error)
}

/// Whether the slot's ready partition holds a row at or after its lane's claim
/// cursor.
///
/// Rows of any generation count. Without the `ring_generation_floor` patch a
/// REPEATABLE READ or SERIALIZABLE producer could write a ready row into an
/// older generation than the slot's (#847); truncating the slot would delete
/// that unclaimed job, so it holds the slot instead.
async fn queue_prune_has_pending_ready_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    schema: &str,
    ready_child: &str,
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
                WHERE ready.queue = claims.queue
                  AND ready.priority = claims.priority
                  AND ready.enqueue_shard = claims.enqueue_shard
                  AND ready.lane_seq >= claims.claim_seq
                LIMIT 1
            ) AS pending_ready
            LIMIT 1
        )
        "#
    ))
    .fetch_one(tx.as_mut())
    .await
    .map_err(map_sqlx_error)
}

/// `claimed` without repeats of an attempt, keeping the first. Completion
/// writes one closure per input entry, and queue prune proves a slot's
/// claims closed by count, so an attempt passed twice would record two
/// closures for one claim and could hide another claim still open.
fn unique_attempts(claimed: &[ClaimedRuntimeJob]) -> std::borrow::Cow<'_, [ClaimedRuntimeJob]> {
    let mut seen = HashSet::with_capacity(claimed.len());
    if claimed
        .iter()
        .all(|entry| seen.insert((entry.job.id, entry.job.run_lease)))
    {
        return std::borrow::Cow::Borrowed(claimed);
    }
    let mut seen = HashSet::with_capacity(claimed.len());
    std::borrow::Cow::Owned(
        claimed
            .iter()
            .filter(|entry| seen.insert((entry.job.id, entry.job.run_lease)))
            .cloned()
            .collect(),
    )
}

/// Whether a claim on a ready row in the slot is not yet proven closed. Claims
/// of any generation count, as in [`queue_prune_has_active_leases_tx`].
async fn queue_prune_has_unclosed_claim_refs_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    schema: &str,
    slot: i32,
) -> Result<bool, AwaError> {
    let count_proves_claim_refs_closed: bool = sqlx::query_scalar(&format!(
        r#"
        WITH claim_count AS (
            SELECT count(*)::bigint AS total
            FROM {schema}.lease_claims AS claims
            WHERE claims.ready_slot = $1
        ),
        compact_claim_count AS (
            SELECT COALESCE(sum(claimed_count), 0)::bigint AS total
            FROM {schema}.lease_claim_batches AS batches
            WHERE batches.ready_slot = $1
        ),
        explicit_count AS (
            SELECT count(*)::bigint AS total
            FROM {schema}.lease_claims AS claims
            JOIN {schema}.lease_claim_closures AS closures
              ON closures.claim_slot = claims.claim_slot
             AND closures.job_id = claims.job_id
             AND closures.run_lease = claims.run_lease
            WHERE claims.ready_slot = $1
        ),
        compact_count AS (
            SELECT COALESCE(sum(closed_count), 0)::bigint AS total
            FROM {schema}.lease_claim_closure_batches AS batches
            WHERE batches.ready_slot = $1
        )
        SELECT claim_count.total + compact_claim_count.total =
               explicit_count.total + compact_count.total
        FROM claim_count, compact_claim_count, explicit_count, compact_count
        "#
    ))
    .bind(slot)
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
mod claim_cursor_advance_tests {
    use super::{ClaimCursorAdvance, QueueStorage};

    fn advance(next_seq: i64, only_if_current: Option<i64>) -> ClaimCursorAdvance {
        ClaimCursorAdvance {
            queue: "queue".to_string(),
            priority: 2,
            enqueue_shard: 0,
            next_seq,
            only_if_current,
            evidence: None,
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

/// The key of a duplicate-cancellation error entry that holds the job's
/// unique mask from before `cancelled` was cleared from it.
pub(crate) const DUPLICATE_CANCEL_MASK_KEY: &str = "duplicate_unique_states";

/// The messages of the error entries awa writes when it cancels a job as a
/// duplicate. A recorded mask is trusted only on an entry with one of them.
pub(crate) const DUPLICATE_CANCEL_ERRORS: [&str; 8] = [
    SNOOZED_DUPLICATE_ERROR,
    RETRIED_DUPLICATE_ERROR,
    RESCUED_DUPLICATE_ERROR,
    RESCHEDULED_DUPLICATE_ERROR,
    PROMOTED_DUPLICATE_ERROR,
    RESCUED_STALE_DUPLICATE_ERROR,
    RESCUED_EXPIRED_DUPLICATE_ERROR,
    RESCUED_TIMED_OUT_DUPLICATE_ERROR,
];

/// The messages releases before 0.6.19 wrote when they cancelled a duplicate
/// and stored it without its unique key.
const LEGACY_KEYLESS_DUPLICATE_ERRORS: [&str; 2] =
    [RESCUED_DUPLICATE_ERROR, RESCHEDULED_DUPLICATE_ERROR];

/// The unique mask a revived job takes. A row cancelled as a duplicate
/// carries its original mask under [`DUPLICATE_CANCEL_MASK_KEY`] in the
/// duplicate error entry, and gets it back. A cancelled duplicate that a
/// release before 0.6.19 stored without its unique key cannot be revived:
/// nothing would keep it from running beside the job that holds its key.
/// Neither can a recorded duplicate whose key is gone.
fn revived_unique_states(
    state: JobState,
    unique_key: Option<&[u8]>,
    unique_states: Option<String>,
    payload: &serde_json::Value,
) -> Result<Option<String>, AwaError> {
    if state != JobState::Cancelled {
        return Ok(unique_states);
    }
    let errors = payload
        .get("errors")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if let Some(original) = errors.iter().rev().find_map(|error| {
        let message = error.get("error").and_then(serde_json::Value::as_str)?;
        if !DUPLICATE_CANCEL_ERRORS.contains(&message) {
            return None;
        }
        error
            .get(DUPLICATE_CANCEL_MASK_KEY)
            .and_then(serde_json::Value::as_str)
    }) {
        if unique_key.is_none() {
            return Err(AwaError::UniqueConflict {
                constraint: Some("idx_awa_jobs_unique".to_string()),
            });
        }
        return Ok(Some(original.to_string()));
    }
    let legacy_keyless_duplicate = unique_key.is_none()
        && unique_states.is_none()
        && errors.last().is_some_and(|error| {
            error.get("terminal") == Some(&serde_json::Value::Bool(true))
                && error
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|message| LEGACY_KEYLESS_DUPLICATE_ERRORS.contains(&message))
        });
    if legacy_keyless_duplicate {
        return Err(AwaError::UniqueConflict {
            constraint: Some("idx_awa_jobs_unique".to_string()),
        });
    }
    Ok(unique_states)
}

/// `entry` without the duplicate cancellation mask key, which only awa
/// records.
pub(crate) fn without_duplicate_mask(entry: &serde_json::Value) -> serde_json::Value {
    let mut entry = entry.clone();
    if let Some(object) = entry.as_object_mut() {
        object.remove(DUPLICATE_CANCEL_MASK_KEY);
    }
    entry
}

/// A caller-supplied error entry as stored on a failed job, with the
/// attempt, time and terminal flag filled in when absent. The duplicate
/// cancellation mask key is dropped: only awa records it.
fn caller_error_entry(error_entry: serde_json::Value, attempt: i16) -> serde_json::Value {
    let mut error_obj = match error_entry {
        serde_json::Value::Object(map) => map,
        other => {
            let mut map = serde_json::Map::new();
            map.insert("error".to_string(), other);
            map
        }
    };
    error_obj.remove(DUPLICATE_CANCEL_MASK_KEY);
    error_obj
        .entry("attempt".to_string())
        .or_insert_with(|| serde_json::Value::from(i64::from(attempt)));
    error_obj
        .entry("at".to_string())
        .or_insert_with(|| serde_json::Value::String(Utc::now().to_rfc3339()));
    error_obj
        .entry("terminal".to_string())
        .or_insert(serde_json::Value::Bool(true));
    serde_json::Value::Object(error_obj)
}

/// Clear `cancelled` from `done`'s mask and record the original mask in its
/// last error entry, the duplicate cancellation.
fn clear_cancelled_claim(done: &mut DoneJobRow) -> Result<(), AwaError> {
    let Some(original) = done.unique_states.clone() else {
        return Ok(());
    };
    let mut payload = RuntimePayload::from_json(done.payload.clone())?;
    if let Some(entry) = payload.errors.last_mut() {
        entry[DUPLICATE_CANCEL_MASK_KEY] = serde_json::Value::String(original.clone());
    }
    done.payload = payload.into_json();
    done.unique_states = unique_states_without(Some(original), JobState::Cancelled);
    Ok(())
}

/// The cancelled done row for an attempt whose outcome's unique claim
/// another job holds, with `duplicate_error` as its last error entry, and
/// whether `cancelled` was cleared from its mask. The attempt held no claim,
/// since its outcome's claim conflicted. A mask that claims `cancelled`
/// would conflict again, so the row is written with `cancelled` cleared from
/// its mask and the original mask recorded in its error entry. It keeps its
/// unique key, so a later retry restores the mask and re-checks ownership.
fn duplicate_cancelled_row(
    row: LeaseTransitionRow,
    payload: serde_json::Value,
    duplicate_error: &str,
    finalized_at: DateTime<Utc>,
) -> Result<(DoneJobRow, bool), AwaError> {
    let attempt = row.attempt;
    let mut payload = RuntimePayload::from_json(payload)?;
    payload.push_error(lifecycle_error(duplicate_error, attempt, true));
    let mut done = row.into_done_row(JobState::Cancelled, finalized_at, payload.into_json());
    let cleared = unique_state_claims(done.unique_states.as_deref(), JobState::Cancelled);
    if cleared {
        clear_cancelled_claim(&mut done)?;
    }
    Ok((done, cleared))
}

/// `unique_states` with `state`'s bit cleared.
fn unique_states_without(unique_states: Option<String>, state: JobState) -> Option<String> {
    let mut bitmask = unique_states?.into_bytes();
    if let Some(bit) = bitmask.get_mut(state.bit_position() as usize) {
        *bit = b'0';
    }
    String::from_utf8(bitmask).ok()
}

/// A change to one job's row in `awa.job_unique_claims`: `(key, claim,
/// job_id)`, where `claim` takes the key for the job and otherwise releases
/// it. The tuple order is the order every transaction applies changes in:
/// unique key bytewise, a key's releases before its claims, then job ID.
type UniqueClaimChange<'k> = (&'k [u8], bool, i64);

/// The claim change a job moving from `old_state` to `new_state` makes, if
/// any. `None` is a state outside job storage.
fn unique_claim_change<'k>(
    job_id: i64,
    unique_key: Option<&'k [u8]>,
    unique_states: Option<&str>,
    old_state: Option<JobState>,
    new_state: Option<JobState>,
) -> Option<UniqueClaimChange<'k>> {
    let key = unique_key?;
    let old_claim = old_state.is_some_and(|state| unique_state_claims(unique_states, state));
    let new_claim = new_state.is_some_and(|state| unique_state_claims(unique_states, state));
    // A terminal row's mask can name its state while another job holds the
    // key, as for a cancelled duplicate, so reviving it re-checks ownership
    // through the claim upsert.
    let recheck = new_claim
        && old_state.is_some_and(|state| state.is_terminal())
        && !new_state.is_some_and(|state| state.is_terminal());
    (old_claim != new_claim || recheck).then_some((key, new_claim, job_id))
}

/// The claim changes writing `rows` as done rows makes.
fn done_claim_changes(
    rows: &[DoneJobRow],
    old_state: Option<JobState>,
) -> impl Iterator<Item = UniqueClaimChange<'_>> {
    rows.iter().filter_map(move |row| {
        unique_claim_change(
            row.job_id,
            row.unique_key.as_deref(),
            row.unique_states.as_deref(),
            old_state,
            Some(row.state),
        )
    })
}

/// The claim change writing `row` as a done row makes, owning its key.
fn owned_done_claim_change(
    row: &DoneJobRow,
    old_state: Option<JobState>,
) -> Option<(Vec<u8>, bool, i64)> {
    done_claim_changes(std::slice::from_ref(row), old_state)
        .next()
        .map(|(key, claim, job_id)| (key.to_vec(), claim, job_id))
}

/// The claim changes writing `rows` as DLQ rows makes. A DLQ row holds its
/// claim as `failed`.
fn dlq_claim_changes(
    rows: &[DlqJobRow],
    old_state: Option<JobState>,
) -> impl Iterator<Item = UniqueClaimChange<'_>> {
    rows.iter().filter_map(move |row| {
        unique_claim_change(
            row.job_id,
            row.unique_key.as_deref(),
            row.unique_states.as_deref(),
            old_state,
            Some(JobState::Failed),
        )
    })
}

/// The claim changes writing `rows` as deferred rows makes.
fn deferred_claim_changes(
    rows: &[DeferredJobRow],
    old_state: Option<JobState>,
) -> impl Iterator<Item = UniqueClaimChange<'_>> {
    rows.iter().filter_map(move |row| {
        unique_claim_change(
            row.job_id,
            row.unique_key.as_deref(),
            row.unique_states.as_deref(),
            old_state,
            Some(row.state),
        )
    })
}

/// The unique claims enqueuing `rows` takes.
fn deferred_enqueue_claims(rows: &[DeferredJobRow]) -> Vec<(Vec<u8>, i64)> {
    rows.iter()
        .filter(|row| unique_state_claims(row.unique_states.as_deref(), row.state))
        .filter_map(|row| row.unique_key.as_ref().map(|key| (key.clone(), row.job_id)))
        .collect()
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

fn terminal_payloads<'r>(
    rows: &[&'r DoneJobRow],
    ready_payloads: &HashMap<(i32, i64, &str, i16, i16, i64), serde_json::Value>,
) -> Vec<Option<&'r serde_json::Value>> {
    rows.iter()
        .map(|row| {
            let ready_key = (
                row.ready_slot,
                row.ready_generation,
                row.queue.as_str(),
                row.priority,
                row.enqueue_shard,
                row.lane_seq,
            );
            terminal_storage_payload(&row.payload, ready_payloads.get(&ready_key))
        })
        .collect()
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

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct ReadyRowIdentity {
    ready_slot: i32,
    ready_generation: i64,
    queue: String,
    priority: i16,
    enqueue_shard: i16,
    lane_seq: i64,
    job_id: i64,
    run_lease: i64,
}

/// Attempts a rescue pass leaves for a later pass, as `(job_id, run_lease)`.
struct BusyRetained {
    attempts: Vec<(i64, i64)>,
    /// The attempts stay unresolvable until their retained bodies are
    /// repaired, rather than until a lock is released.
    unresolvable: bool,
}

impl BusyRetained {
    fn busy(attempts: Vec<(i64, i64)>) -> Self {
        Self {
            attempts,
            unresolvable: false,
        }
    }

    /// Leaves the attempts out of the rest of the pass. Busy attempts shrink
    /// the pass's selection. Unresolvable attempts are replaced in it, so a
    /// backlog of them, which every pass meets first, cannot fill each batch
    /// and keep rescue from the attempts behind it.
    fn exclude(
        self,
        excluded: &mut BTreeSet<(i64, i64)>,
        selected: &mut Option<BTreeSet<(i64, i64)>>,
    ) -> Result<(), AwaError> {
        let previous = excluded.len();
        excluded.extend(self.attempts);
        if excluded.len() == previous {
            return Err(AwaError::Validation(
                "queue storage retained attempt retry made no progress".to_string(),
            ));
        }
        if self.unresolvable {
            *selected = None;
        }
        Ok(())
    }
}

/// A failure hydrating deleted leases. `Ambiguous` names the attempts, as
/// `(job_id, run_lease)`, whose retained ready bodies rank equally, so none
/// can be chosen for them.
enum HydrateError {
    Ambiguous(Vec<(i64, i64)>),
    Awa(AwaError),
}

impl From<AwaError> for HydrateError {
    fn from(err: AwaError) -> Self {
        Self::Awa(err)
    }
}

impl From<HydrateError> for AwaError {
    fn from(err: HydrateError) -> Self {
        match err {
            HydrateError::Ambiguous(attempts) => AwaError::Validation(match attempts.as_slice() {
                [] => "queue storage ready body ambiguous for deleted lease".to_string(),
                [(job_id, run_lease)] => format!(
                    "queue storage ready body ambiguous for deleted lease job {job_id} run_lease {run_lease}"
                ),
                [(job_id, run_lease), rest @ ..] => format!(
                    "queue storage ready body ambiguous for deleted lease job {job_id} run_lease {run_lease} and {} more",
                    rest.len()
                ),
            }),
            HydrateError::Awa(err) => err,
        }
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

/// A receipt-backed attempt found by a receipt-only cancel: claim slot, run
/// lease, ready slot, ready generation, queue, priority, attempt, max
/// attempts, lane position, claim time, receipt id and whether it is a compact
/// batch claim.
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

/// A lane whose claim cursor a spent-prefix advance moved.
#[derive(Debug, Clone, sqlx::FromRow)]
struct SpentPrefixAdvance {
    queue: String,
    priority: i16,
    enqueue_shard: i16,
    claim_seq: i64,
    next_seq: i64,
    /// Every row in the window was spent and the lane has rows past it.
    unfinished: bool,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct DrainLane {
    queue: String,
    priority: i16,
    enqueue_shard: i16,
    from_seq: i64,
    to_seq: i64,
}

#[derive(Debug, Clone)]
struct ClaimCursorAdvance {
    queue: String,
    priority: i16,
    enqueue_shard: i16,
    next_seq: i64,
    only_if_current: Option<i64>,
    /// Claim batch row that must exist for the advance to apply: the cursor
    /// only moves past a lane position whose claim is visible on the server
    /// the advance runs against.
    evidence: Option<ClaimEvidence>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClaimEvidence {
    ready_slot: i32,
    ready_generation: i64,
    lane_seq: i64,
    /// `(job_id, run_lease)` of a claim written straight into `leases`. A
    /// receipt claim, with or without a deadline, is proven by its
    /// `ready_claim_attempt_batches` row instead.
    legacy_attempt: Option<(i64, i64)>,
}

type ClaimCursorLaneKey = (String, i16, i16);
type ConditionalClaimCursorAdvances = BTreeMap<i64, i64>;
type GroupedClaimCursorAdvances = BTreeMap<
    ClaimCursorLaneKey,
    (
        Option<(i64, Option<ClaimEvidence>)>,
        ConditionalClaimCursorAdvances,
    ),
>;
type TerminalCounterKey = (i32, i64, String, i16, i16, i16);

struct CancelJobTxResult {
    row: JobRow,
    /// The cancellation's claim change, for the caller to apply with the
    /// rest of its transaction's.
    claim_change: Option<(Vec<u8>, bool, i64)>,
    claim_cursor_advance: Option<ClaimCursorAdvance>,
    /// Shard-0 lane of a cancelled deferred job, to be ensured after the
    /// transaction's unique claims change.
    lane: Option<(String, i16)>,
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

    #[sqlx::test(migrations = false)]
    async fn enqueue_batch_writes_more_segments_than_the_old_bind_limit(pool: sqlx::PgPool) {
        use super::{QueueStorageConfig, RuntimeReadyRow};

        crate::migrations::run(&pool).await.unwrap();
        let store = QueueStorage::new(QueueStorageConfig {
            schema: "awa_qs_segment_bind_limit".into(),
            ..Default::default()
        })
        .unwrap();
        store.install(&pool).await.unwrap();

        // Distinct microsecond timestamps guarantee one segment per row.
        // Reading the clock for every row can coalesce adjacent timestamps
        // and fail to exercise the old 8,191-segment bind limit.
        const JOBS: i64 = 9_000;
        let first_run_at = Utc::now() - Duration::seconds(1);
        let rows = (0..JOBS)
            .map(|seq| RuntimeReadyRow {
                kind: "segment_test".into(),
                queue: "segment-q".into(),
                args: serde_json::json!({"seq": seq}),
                priority: 2,
                attempt: 0,
                run_lease: 0,
                max_attempts: 25,
                run_at: first_run_at + Duration::microseconds(seq),
                attempted_at: None,
                created_at: first_run_at,
                unique_key: None,
                unique_states: None,
                payload: serde_json::json!({}),
                ordering_key: None,
            })
            .collect();
        assert_eq!(
            store.enqueue_runtime_rows(&pool, rows).await.unwrap(),
            JOBS as usize
        );
        let counts: (i64, i64) = sqlx::query_as(&format!(
            "SELECT (SELECT count(*) FROM {}.ready_entries), \
                    (SELECT count(*) FROM {}.ready_segments)",
            store.schema(),
            store.schema()
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(counts, (JOBS, JOBS));
        let claimed = store
            .claim_runtime_batch(&pool, "segment-q", 100, std::time::Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(claimed.len(), 100);
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

// Keep the stored terminal priority for decrementing its old counter key.
// Manual retry and terminal-to-DLQ moves recover the retained physical lane.
#[derive(Debug, sqlx::FromRow)]
struct HydratedTerminalRow {
    #[sqlx(flatten)]
    terminal: DoneJobRow,
    body_priority: i16,
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

/// The unique-claim fields of a deleted DLQ row.
#[derive(Debug, Clone, sqlx::FromRow)]
struct DeletedDlqRow {
    job_id: i64,
    state: JobState,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
}

const DELETED_DLQ_COLUMNS: &str = "job_id, state, unique_key, unique_states";

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

/// What one lane's promotion transaction did.
#[derive(Debug, Default)]
struct LanePromotion {
    promoted: usize,
    cancelled: Vec<JobRow>,
}

/// Which of a lane's due rows one promotion transaction takes.
#[derive(Debug, Clone, Copy)]
enum DueLaneRows<'a> {
    /// These rows, picked by a batch across lanes, where still due.
    Selected(&'a [i64]),
    /// The oldest due rows, up to this many.
    Oldest(i64),
}

#[derive(Debug, Clone, serde::Deserialize, sqlx::FromRow)]
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
    priority: i16,
    lane_seq: i64,
    enqueue_shard: i16,
    run_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
    run_lease: i64,
    live_body: bool,
}

type ReadyBodyKey = (i32, i64, String, i16, i64, i64, i64);
type ReadyBodyLaneKey = (i32, i64, String, i16, i64, i64, i64, i16);

fn select_claimed_ready_body<'a>(
    candidates: &'a [ReadySnapshotRow],
    claimed_lanes: &BTreeSet<ReadyBodyLaneKey>,
    reference: &DeletedLeaseRow,
) -> Result<(&'a ReadySnapshotRow, bool), HydrateError> {
    // Emitted physical-lane evidence wins over a live replacement.
    // Without it, retain the live-body compatibility fallback; it
    // cannot identify a legacy claim's original colliding lane.
    let emitted = |row: &ReadySnapshotRow| {
        claimed_lanes.contains(&(
            row.ready_slot,
            row.ready_generation,
            row.queue.clone(),
            row.enqueue_shard,
            row.lane_seq,
            row.job_id,
            row.run_lease + 1,
            row.priority,
        ))
    };
    let has_emitted = candidates.iter().any(&emitted);
    let rank = |row: &ReadySnapshotRow| {
        if has_emitted {
            emitted(row)
        } else {
            row.live_body
        }
    };
    let strongest = candidates
        .iter()
        .map(&rank)
        .max()
        .expect("ready body group is nonempty");
    let mut best = candidates.iter().filter(|row| rank(row) == strongest);
    let ready = best.next().expect("ready body rank has a candidate");
    if best.next().is_some() {
        return Err(HydrateError::Ambiguous(vec![(
            reference.job_id,
            reference.run_lease,
        )]));
    }
    Ok((ready, has_emitted))
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
    ready_priority: i16,
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
    self_contained_terminal: bool,
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
            // Terminal rows retain their ready-lane identity for body hydration.
            // The effective attempt priority is still used by deferred/DLQ rows.
            priority: self.ready_priority,
            attempt: self.attempt,
            run_lease: self.run_lease,
            max_attempts: self.max_attempts,
            // A competing live copy must not replace this recovered body
            // when later terminal reads resolve retained ready rows.
            lane_seq: if self.self_contained_terminal {
                -self.job_id
            } else {
                self.lane_seq
            },
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
    /// Stripe for the next enqueued job that has neither an ordering key
    /// nor a unique key. Randomly seeded so single-job producers spread
    /// across stripes instead of all starting at stripe 0.
    next_enqueue_stripe: AtomicUsize,
    /// Per-store rotor that selects the enqueue shard for the next batch.
    /// Rotated once per `shard_for_enqueue` call so producers spread their
    /// writes across the per-`(queue, priority)` shard rows.
    shard_rotor: AtomicU16,
    /// Cache of `awa.queue_meta.enqueue_shards` per queue. Populated lazily
    /// on the first `shard_for_enqueue` call for a queue and invalidated by
    /// `reset()`. With the default `enqueue_shards = 1`, the cache holds 1
    /// and `pick_shard` returns 0 unconditionally.
    enqueue_shards_cache: Mutex<HashMap<String, i16>>,
    /// This instance's claimer lease per queue, as last read or written, so a
    /// claim does not re-read the lease and claimer target on every call.
    claimer_gates: Mutex<HashMap<(String, Uuid), CachedClaimerGate>>,
    /// Whether `{schema}.provision_lane` (the `definer_lane_provisioning`
    /// schema patch) was callable the last time a lane was created. Only a
    /// positive answer is kept: lane creation is rare, and the probe rides on
    /// the lane-lock round trip.
    lane_provisioner_present: AtomicBool,
    /// Set once a provisioner failure has been logged.
    lane_provisioner_warned: AtomicBool,
    /// Set once `{schema}.enqueue_ring_cursor()` (the `ring_generation_floor`
    /// schema patch) was found callable. A negative answer is not kept: until
    /// the function appears, each enqueue checks again, so producers start
    /// using the floor with the first enqueue after the patch commits.
    enqueue_ring_cursor_present: AtomicBool,
    /// Set once a schema left without the floor after the patch was logged.
    enqueue_ring_cursor_warned: AtomicBool,
    prune_lock_timeout: Duration,
    /// Whether each sealed claim slot's claims were all closed when rescue
    /// last counted them; see [`Self::rescue_slot_order_tx`].
    claim_slot_proofs: Mutex<HashMap<i32, ClaimSlotProof>>,
}

/// Whether a sealed claim slot's claims were all closed, by the counts claim
/// prune proves before it truncates a slot, at its ring generation.
#[derive(Debug, Clone, Copy)]
struct ClaimSlotProof {
    generation: i64,
    closed: bool,
    proven_at: Instant,
}

/// How long rescue trusts a sealed claim slot's [`ClaimSlotProof`] before it
/// counts the slot again.
///
/// A claim reads the current claim slot without a lock, so a claim
/// transaction in flight when the ring rotates can still commit into the slot
/// just sealed. A slot proven closed before such a claim commits looks closed
/// to rescue until it is counted again, so the claim's rescue waits at most
/// this long. Prune does not use these proofs: it counts under its
/// `ACCESS EXCLUSIVE` lock, which waits for every such claim.
const CLAIM_SLOT_PROOF_TTL: Duration = Duration::from_secs(30);

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
            next_enqueue_stripe: AtomicUsize::new(Uuid::new_v4().as_u128() as usize),
            shard_rotor: AtomicU16::new(0),
            enqueue_shards_cache: Mutex::new(HashMap::new()),
            claimer_gates: Mutex::new(HashMap::new()),
            lane_provisioner_present: AtomicBool::new(false),
            lane_provisioner_warned: AtomicBool::new(false),
            enqueue_ring_cursor_present: AtomicBool::new(false),
            enqueue_ring_cursor_warned: AtomicBool::new(false),
            prune_lock_timeout: DEFAULT_PRUNE_LOCK_TIMEOUT,
            claim_slot_proofs: Mutex::new(HashMap::new()),
        })
    }

    /// Set the PostgreSQL `lock_timeout` applied to the exclusive-lock steps of
    /// ring prune (queue, lease and claim rings) and terminal-delta rollup.
    ///
    /// Each lock acquisition gets its own timeout; this does not bound the
    /// whole prune. Queue prune locks up to eight partitions, so its
    /// partition-lock
    /// waits alone can approach eight times this value. A lock timeout rolls
    /// back
    /// the prune and reports [`PruneOutcome::Blocked`]; the maintenance loop
    /// retries on a later tick. The default is 10 ms, which keeps prune from
    /// queueing behind readers and so from stalling claims and enqueues behind
    /// its pending
    /// `ACCESS EXCLUSIVE` request. Raise it when prune is persistently
    /// `Blocked` by short-lived readers of the ring partitions. Keep it well
    /// below `deadlock_timeout` (default 1 s) and any `statement_timeout`: a
    /// prune that waits as long as `deadlock_timeout` triggers deadlock
    /// detection, which may cancel a concurrent claim instead of the prune.
    /// The
    /// timeout is applied at millisecond granularity.
    ///
    /// Returns a validation error for a zero timeout or one above PostgreSQL's
    /// `lock_timeout` maximum (`i32::MAX` milliseconds, about 24.8 days).
    pub fn with_prune_lock_timeout(mut self, timeout: Duration) -> Result<Self, AwaError> {
        self.set_prune_lock_timeout(timeout)?;
        Ok(self)
    }

    /// In-place form of [`Self::with_prune_lock_timeout`].
    pub fn set_prune_lock_timeout(&mut self, timeout: Duration) -> Result<(), AwaError> {
        if timeout.is_zero() {
            return Err(AwaError::Validation(
                "queue storage prune lock timeout must be greater than zero".into(),
            ));
        }
        if timeout.as_millis() > i32::MAX as u128 {
            return Err(AwaError::Validation(format!(
                "queue storage prune lock timeout must not exceed {} ms",
                i32::MAX
            )));
        }
        self.prune_lock_timeout = timeout;
        Ok(())
    }

    /// The `lock_timeout` applied to prune's exclusive-lock steps.
    pub fn prune_lock_timeout(&self) -> Duration {
        self.prune_lock_timeout
    }

    /// The longest single lock wait of a prune.
    ///
    /// Queue and claim prune first wait [`Self::prune_lock_timeout`] for the
    /// `ACCESS EXCLUSIVE` lock they truncate under, then retry with twice the
    /// wait each time up to this value before reporting
    /// [`PruneOutcome::Blocked`]. It is 320 ms, or the prune lock timeout
    /// when that is longer.
    pub fn prune_lock_wait_ceiling(&self) -> Duration {
        prune_lock_upgrade_ceiling(self.prune_lock_timeout)
    }

    /// The longest a single prune lock statement can wait.
    ///
    /// `lock_timeout` applies to each relation a `LOCK TABLE` statement
    /// acquires, and queue prune locks eight partitions in one statement, so
    /// the statement can wait eight times [`Self::prune_lock_wait_ceiling`].
    /// A `statement_timeout` at or below this can cancel the prune before
    /// its lock timeout fires.
    pub fn prune_lock_statement_wait_ceiling(&self) -> Duration {
        self.prune_lock_wait_ceiling()
            .saturating_mul(QUEUE_PRUNE_LOCKED_PARTITIONS)
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

    /// Physical queues that hold jobs for `queue`: each stripe, then the
    /// unstriped queue itself. Producers that do not stripe (the SQL compat
    /// insert path, cron, a store configured with one stripe) write the
    /// unstriped queue, so a striped store reads it as well. The striped
    /// claim loops probe this list round-robin, so the unstriped queue
    /// takes one turn in `stripes + 1`.
    pub(crate) fn physical_queues_for_logical(&self, queue: &str) -> Vec<String> {
        if !self.uses_queue_striping() || self.is_physical_stripe_queue(queue) {
            return vec![queue.to_string()];
        }
        (0..self.queue_stripe_count())
            .map(|stripe| self.physical_queue_for_stripe(queue, stripe))
            .chain(std::iter::once(queue.to_string()))
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

    /// Physical stripe for a job enqueued to `queue`.
    ///
    /// A job with an `ordering_key` goes to the stripe its key hashes to,
    /// so jobs sharing a key share a stripe and keep their claim order;
    /// otherwise a `unique_key` picks the stripe the same way. The stripe
    /// uses [`stripe_hash`] of [`ordering_key_hash64`] and the enqueue
    /// shard ([`shard_for_ordering_key`]) uses the raw hash modulo the shard
    /// count, so the keys on one stripe still spread across every shard.
    /// A job with neither key takes `spread` when given (a job id),
    /// else the next value of the store's enqueue-stripe counter.
    fn queue_stripe_for_enqueue(
        &self,
        queue: &str,
        ordering_key: Option<&[u8]>,
        unique_key: Option<&[u8]>,
        spread: Option<i64>,
    ) -> String {
        if !self.uses_queue_striping() || self.is_physical_stripe_queue(queue) {
            return queue.to_string();
        }

        let stripes = self.queue_stripe_count();
        let stripe = match ordering_key.or(unique_key) {
            Some(key) => (stripe_hash(ordering_key_hash64(key)) % stripes as u64) as usize,
            None => match spread {
                Some(value) => value.rem_euclid(stripes as i64) as usize,
                None => self.next_enqueue_stripe.fetch_add(1, Ordering::Relaxed) % stripes,
            },
        };
        self.physical_queue_for_stripe(queue, stripe)
    }

    fn use_lease_claim_receipts_for_runtime(&self, _deadline_duration: Duration) -> bool {
        // Receipts mode now supports per-claim deadlines via
        // `lease_claims.deadline_at` (rescued by
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

    /// Apply `changes` to `awa.job_unique_claims` in one pass, in
    /// [`UniqueClaimChange`] order. A run of consecutive releases goes in one
    /// statement, and so does a run of consecutive claims. Each transaction
    /// changes its claims in a single such pass, before it reserves any lane.
    /// Returns the job IDs whose key another job holds; without
    /// `skip_conflicts` any such job is an error.
    async fn apply_unique_claim_changes_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        mut changes: Vec<UniqueClaimChange<'_>>,
        skip_conflicts: bool,
    ) -> Result<HashSet<i64>, AwaError> {
        let mut conflicts = HashSet::new();
        if changes.is_empty() {
            return Ok(conflicts);
        }
        changes.sort_unstable();
        changes.dedup();

        let contested: Vec<&[u8]> = changes
            .windows(2)
            .filter(|pair| pair[0].1 && pair[1].1 && pair[0].0 == pair[1].0)
            .map(|pair| pair[0].0)
            .collect();
        let owners: HashMap<Vec<u8>, i64> = if contested.is_empty() {
            HashMap::new()
        } else {
            sqlx::query_as::<_, (Vec<u8>, i64)>(
                "SELECT unique_key, job_id FROM awa.job_unique_claims WHERE unique_key = ANY($1)",
            )
            .bind(&contested)
            .fetch_all(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?
            .into_iter()
            .collect()
        };
        // Among jobs claiming the same key, the one that owns it wins;
        // otherwise the lowest job ID does.
        let mut winners: HashMap<&[u8], i64> = HashMap::new();
        for (key, claim, job_id) in &changes {
            if !*claim {
                continue;
            }
            let winner = winners.entry(*key).or_insert(*job_id);
            if owners.get(*key) == Some(job_id) {
                *winner = *job_id;
            }
        }

        let mut claiming: Vec<(&[u8], i64)> = Vec::new();
        let mut releasing: Vec<(&[u8], i64)> = Vec::new();
        for (key, claim, job_id) in changes {
            if claim {
                if winners.get(key) == Some(&job_id) {
                    self.release_unique_keys_tx(tx, &mut releasing).await?;
                    claiming.push((key, job_id));
                } else {
                    conflicts.insert(job_id);
                }
            } else {
                self.claim_unique_keys_tx(tx, &mut claiming, &mut conflicts)
                    .await?;
                releasing.push((key, job_id));
            }
        }
        self.claim_unique_keys_tx(tx, &mut claiming, &mut conflicts)
            .await?;
        self.release_unique_keys_tx(tx, &mut releasing).await?;

        if !skip_conflicts && !conflicts.is_empty() {
            return Err(AwaError::UniqueConflict {
                constraint: Some("idx_awa_jobs_unique".to_string()),
            });
        }
        Ok(conflicts)
    }

    /// Release the `pending` claims each job holds, locking them in the given
    /// order, and clear `pending`.
    async fn release_unique_keys_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        pending: &mut Vec<(&[u8], i64)>,
    ) -> Result<(), AwaError> {
        match pending.as_slice() {
            [] => return Ok(()),
            [(key, job_id)] => {
                sqlx::query(
                    "DELETE FROM awa.job_unique_claims WHERE unique_key = $1 AND job_id = $2",
                )
                .bind(key)
                .bind(job_id)
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            }
            _ => {
                let (keys, job_ids): (Vec<&[u8]>, Vec<i64>) = pending.iter().copied().unzip();
                // The locking clause applies after ORDER BY, so the CTE locks
                // the rows in key order; the DELETE only reaches rows the CTE
                // has already locked.
                sqlx::query(
                    r#"
                    WITH released AS MATERIALIZED (
                        SELECT claims.unique_key
                        FROM unnest($1::bytea[], $2::bigint[]) AS input(unique_key, job_id)
                        JOIN awa.job_unique_claims AS claims
                          ON claims.unique_key = input.unique_key
                         AND claims.job_id = input.job_id
                        ORDER BY claims.unique_key
                        FOR UPDATE OF claims
                    )
                    DELETE FROM awa.job_unique_claims AS claims
                    USING released
                    WHERE claims.unique_key = released.unique_key
                    "#,
                )
                .bind(&keys)
                .bind(&job_ids)
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            }
        }
        pending.clear();
        Ok(())
    }

    /// Insert enqueue-time unique claims in one statement, in unique-key
    /// order.
    async fn sync_enqueue_unique_claims<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        mut claims: Vec<(Vec<u8>, i64)>,
    ) -> Result<(), AwaError> {
        if claims.is_empty() {
            return Ok(());
        }

        claims.sort_unstable();
        if claims.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(AwaError::UniqueConflict {
                constraint: Some("idx_awa_jobs_unique".to_string()),
            });
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
    // transitions use `apply_unique_claim_changes_tx`, which can release old
    // claims.
    // `deferred_claims` are claimed in the same key-ordered statement, for a
    // batch whose deferred rows are written after its ready rows.
    async fn sync_ready_enqueue_unique_claims<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[RuntimeReadyInsert],
        mut deferred_claims: Vec<(Vec<u8>, i64)>,
    ) -> Result<(), AwaError> {
        deferred_claims.extend(
            rows.iter()
                .filter(|row| {
                    unique_state_claims(row.unique_states.as_deref(), JobState::Available)
                })
                .filter_map(|row| row.unique_key.as_ref().map(|key| (key.clone(), row.job_id))),
        );
        self.sync_enqueue_unique_claims(tx, deferred_claims).await
    }

    async fn sync_deferred_enqueue_unique_claims<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[DeferredJobRow],
    ) -> Result<(), AwaError> {
        self.sync_enqueue_unique_claims(tx, deferred_enqueue_claims(rows))
            .await
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
        let mut install_tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;

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
                            "SELECT current_slot FROM {schema}.claim_ring_state WHERE singleton"
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

        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;

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

    /// Empties the queue-storage schema: deletes every job, lease and DLQ
    /// entry, and restarts the ring, the generation floor and the job id
    /// sequence. A test fixture, not a recovery or maintenance operation.
    ///
    /// Stop every producer, worker and the maintenance leader using the
    /// schema, and let their transactions finish, before calling it: an
    /// enqueue or rotation that commits while it runs can leave a job in a
    /// ring generation the reset ring has not reached.
    pub async fn reset(&self, pool: &PgPool) -> Result<(), AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;

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
                {schema}.queue_terminal_live_counts,
                {schema}.queue_terminal_count_deltas,
                {schema}.queue_claimer_leases,
                {schema}.queue_claimer_state,
                {schema}.queue_ring_slots,
                {schema}.lease_ring_slots,
                {schema}.claim_ring_slots
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

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.queue_ring_state
            SET current_slot = 0,
                generation = 0,
                slot_count = $1
            WHERE singleton = TRUE
            "#
        ))
        .bind(self.queue_slot_count() as i32)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        // The ring restarts at generation 0, and so does its generation
        // floor (#847). The next enqueue or rotation raises it again.
        let has_generation_floor: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(format!("{schema}.queue_ring_generation_floor"))
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        if has_generation_floor {
            sqlx::query(&format!(
                "ALTER SEQUENCE {schema}.queue_ring_generation_floor RESTART WITH 0"
            ))
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.lease_ring_state
            SET current_slot = 0,
                generation = 0,
                slot_count = $1
            WHERE singleton = TRUE
            "#
        ))
        .bind(self.lease_slot_count() as i32)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.claim_ring_state
            SET current_slot = 0,
                generation = 0,
                slot_count = $1
            WHERE singleton = TRUE
            "#
        ))
        .bind(self.claim_slot_count() as i32)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        for slot in 0..self.queue_slot_count() {
            sqlx::query(&format!(
                r#"
                INSERT INTO {schema}.queue_ring_slots (slot, generation)
                VALUES ($1, $2)
                "#
            ))
            .bind(slot as i32)
            .bind(if slot == 0 { 0_i64 } else { -1_i64 })
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        for slot in 0..self.lease_slot_count() {
            sqlx::query(&format!(
                r#"
                INSERT INTO {schema}.lease_ring_slots (slot, generation)
                VALUES ($1, $2)
                "#
            ))
            .bind(slot as i32)
            .bind(if slot == 0 { 0_i64 } else { -1_i64 })
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        for slot in 0..self.claim_slot_count() {
            sqlx::query(&format!(
                r#"
                INSERT INTO {schema}.claim_ring_slots (slot, generation)
                VALUES ($1, $2)
                "#
            ))
            .bind(slot as i32)
            .bind(if slot == 0 { 0_i64 } else { -1_i64 })
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        tx.commit().await.map_err(map_sqlx_error)?;

        // queue_lanes and queue_claimer_leases were TRUNCATEd above and
        // queue_meta may have a new shard configuration for the next round.
        // Clear the caches so the next shard_for_enqueue / claim calls
        // re-observe DB state.
        self.enqueue_shards_cache
            .lock()
            .expect("enqueue_shards_cache poisoned")
            .clear();
        self.claimer_gates
            .lock()
            .expect("claimer gate cache poisoned")
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
        // A lane whose head row and lane row this transaction can see needs
        // no provisioning. `reset` truncates `queue_lanes` and keeps the heads.
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
            AND EXISTS (
                SELECT 1
                FROM {schema}.queue_lanes
                WHERE queue = $1
                  AND priority = $2
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

        self.ensure_lane_inserts(tx, queue, priority, enqueue_shard)
            .await
    }

    /// Provision every priority and shard of a queue ahead of its first
    /// enqueue. Once the `definer_lane_provisioning` schema patch is applied,
    /// any runtime role may run it: the lanes are created as the schema owner.
    /// This does not change enqueue routing or the configured shard count.
    pub async fn prepare_queue(
        &self,
        pool: &PgPool,
        queue: &str,
        enqueue_shards: i16,
    ) -> Result<(), AwaError> {
        if queue.is_empty() || !(1..=64).contains(&enqueue_shards) {
            return Err(AwaError::Validation(
                "prepare_queue requires a nonempty queue and 1..=64 enqueue shards".into(),
            ));
        }
        // Lane locks in name order, the order enqueue batches take them in.
        let mut physical_queues = self.physical_queues_for_logical(queue);
        physical_queues.sort();
        let mut transaction = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        for physical_queue in physical_queues {
            for priority in 1..=4 {
                for shard in 0..enqueue_shards {
                    self.ensure_lane_inserts(&mut transaction, &physical_queue, priority, shard)
                        .await?;
                }
            }
        }
        transaction.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    /// Run the three lane-row inserts unconditionally under the lane's
    /// provisioning lock.
    async fn ensure_lane_inserts<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        queue: &str,
        priority: i16,
        enqueue_shard: i16,
    ) -> Result<(), AwaError> {
        let schema = self.schema();
        // The lane row comes before the lane lock, the order every producer
        // takes them in: SQL inserts write the row, then take the lock in the
        // head-table trigger that provisions the lane's sequences.
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

        let lane_lock_key = format!("{schema}:{queue}:{priority}:{enqueue_shard}");
        let provisioner_callable = if self.lane_provisioner_present.load(Ordering::Relaxed) {
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(&lane_lock_key)
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            true
        } else {
            sqlx::query_scalar(
                r#"
                SELECT COALESCE(has_function_privilege(to_regprocedure($2), 'EXECUTE'), false)
                FROM (SELECT pg_advisory_xact_lock(hashtextextended($1, 0))) AS lane_lock
                "#,
            )
            .bind(&lane_lock_key)
            .bind(format!("{schema}.provision_lane(text,smallint,smallint)"))
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?
        };

        if provisioner_callable
            && self
                .provision_lane(tx, queue, priority, enqueue_shard)
                .await?
        {
            return Ok(());
        }

        // The head tables' BEFORE INSERT triggers run even for a row that
        // ON CONFLICT then skips, and they read and set the lane's sequence.
        // Skipping rows that exist keeps provisioning a live lane off a
        // sequence another role owns, such as one a 0.6.9 runtime created.
        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.queue_enqueue_heads (queue, priority, enqueue_shard)
            SELECT $1, $2, $3
            WHERE NOT EXISTS (
                SELECT 1 FROM {schema}.queue_enqueue_heads
                WHERE queue = $1 AND priority = $2 AND enqueue_shard = $3
            )
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
            SELECT $1, $2, $3
            WHERE NOT EXISTS (
                SELECT 1 FROM {schema}.queue_claim_heads
                WHERE queue = $1 AND priority = $2 AND enqueue_shard = $3
            )
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

        Ok(())
    }

    /// Create the lane through `{schema}.provision_lane`, which runs as the
    /// schema owner, inside a savepoint. Returns false, with the transaction
    /// as it was, when the function has gone (`42883`) or the schema owner
    /// may not write the lane (`42501`); the caller then writes the rows
    /// itself, which is what a binary that predates the function does.
    async fn provision_lane<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        queue: &str,
        priority: i16,
        enqueue_shard: i16,
    ) -> Result<bool, AwaError> {
        let schema = self.schema();
        sqlx::query("SAVEPOINT awa_provision_lane")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        let provisioned = sqlx::query(&format!("SELECT {schema}.provision_lane($1, $2, $3)"))
            .bind(queue)
            .bind(priority)
            .bind(enqueue_shard)
            .execute(tx.as_mut())
            .await;
        match provisioned {
            Ok(_) => {
                sqlx::query("RELEASE SAVEPOINT awa_provision_lane")
                    .execute(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;
                self.lane_provisioner_present.store(true, Ordering::Relaxed);
                Ok(true)
            }
            Err(sqlx::Error::Database(db_err))
                if matches!(db_err.code().as_deref(), Some("42883" | "42501")) =>
            {
                sqlx::query("ROLLBACK TO SAVEPOINT awa_provision_lane")
                    .execute(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;
                self.lane_provisioner_present
                    .store(false, Ordering::Relaxed);
                if !self.lane_provisioner_warned.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        schema,
                        code = db_err.code().as_deref().unwrap_or_default(),
                        error = %db_err.message(),
                        "Lane provisioning function failed; creating lane rows as the connected role"
                    );
                }
                Ok(false)
            }
            Err(err) => Err(map_sqlx_error(err)),
        }
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

    // Reserve lane sequence numbers for a specific
    // `(queue, priority, shard)` triple and return the lane sequence at
    // which the caller's range starts. If the head row is missing, the lane
    // rows are inserted again and the reserve is retried exactly once. A
    // second miss surfaces as RowNotFound.
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

        // Recovery path: the head row was removed after ensure_lane saw it.
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

    /// The queue-ring cursor that ready and terminal rows are written into.
    ///
    /// `{schema}.enqueue_ring_cursor()` (#847) returns the newer of the
    /// cursor this transaction can see and the schema's generation floor, and
    /// raises the floor to the former. Enqueues call this after reserving
    /// their lane positions, while they hold the lane locks, so a REPEATABLE
    /// READ or SERIALIZABLE producer whose snapshot predates a rotation still
    /// writes into a generation no older than any earlier enqueue in its lane.
    ///
    /// A schema without the `ring_generation_floor` patch, or a role that may
    /// not execute the function, reads the ring state as before.
    async fn current_queue_ring<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    ) -> Result<(i32, i64), AwaError> {
        let schema = self.schema();
        if self.enqueue_ring_cursor_callable(tx.as_mut()).await? {
            return sqlx::query_as(&format!(
                "SELECT slot, generation FROM {schema}.enqueue_ring_cursor()"
            ))
            .fetch_one(tx.as_mut())
            .await
            .map_err(|err| {
                // Gone or no longer executable since it was probed: probe
                // again next time rather than fail every enqueue.
                if let sqlx::Error::Database(db) = &err {
                    if matches!(db.code().as_deref(), Some("42883" | "42501")) {
                        self.enqueue_ring_cursor_present
                            .store(false, Ordering::Relaxed);
                    }
                }
                map_sqlx_error(err)
            });
        }
        sqlx::query_as(&format!(
            r#"
            SELECT current_slot, generation
            FROM {schema}.queue_ring_state
            WHERE singleton = TRUE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)
    }

    async fn enqueue_ring_cursor_callable<'e, E>(&self, executor: E) -> Result<bool, AwaError>
    where
        E: sqlx::PgExecutor<'e>,
    {
        if self.enqueue_ring_cursor_present.load(Ordering::Relaxed) {
            return Ok(true);
        }
        let (callable, patched): (bool, bool) = sqlx::query_as(
            "SELECT COALESCE(has_function_privilege(to_regprocedure($1), 'EXECUTE'), false), \
                    to_regprocedure('awa.refresh_ring_generation_floor_helper(text)') IS NOT NULL",
        )
        .bind(format!("{}.enqueue_ring_cursor()", self.schema()))
        .fetch_one(executor)
        .await
        .map_err(map_sqlx_error)?;
        if callable {
            self.enqueue_ring_cursor_present
                .store(true, Ordering::Relaxed);
        } else if patched
            && !self
                .enqueue_ring_cursor_warned
                .swap(true, Ordering::Relaxed)
        {
            // The patch is applied but this schema has no floor: it was
            // prepared by an installer that started before the patch, or the
            // patch could not install the floor here.
            tracing::warn!(
                schema = %self.schema(),
                "queue-storage schema has no generation floor although schema patch \
                 ring_generation_floor is applied; enqueues read the ring directly, which a \
                 REPEATABLE READ or SERIALIZABLE producer must not rely on. Run `awa migrate` \
                 again to install it."
            );
        }
        Ok(callable)
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
        let mut last_by_lane: BTreeMap<ClaimCursorLaneKey, &ReadyJobLeaseRow> = BTreeMap::new();
        for row in rows {
            let key = (row.queue.clone(), row.lane_priority, row.enqueue_shard);
            last_by_lane
                .entry(key)
                .and_modify(|current| {
                    if row.lane_seq > current.lane_seq {
                        *current = row;
                    }
                })
                .or_insert(row);
        }

        last_by_lane
            .into_iter()
            .map(
                |((queue, priority, enqueue_shard), last)| ClaimCursorAdvance {
                    queue,
                    priority,
                    enqueue_shard,
                    next_seq: last.lane_seq + 1,
                    only_if_current: None,
                    evidence: Some(ClaimEvidence {
                        ready_slot: last.ready_slot,
                        ready_generation: last.ready_generation,
                        lane_seq: last.lane_seq,
                        legacy_attempt: last
                            .receipt_id
                            .is_none()
                            .then_some((last.job_id, last.run_lease)),
                    }),
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
            } else if unconditional.is_none_or(|(next, _)| advance.next_seq > next) {
                *unconditional = Some((advance.next_seq, advance.evidence));
            }
        }

        let mut normalized = Vec::with_capacity(advances.len());
        for ((queue, priority, enqueue_shard), (unconditional, conditional)) in grouped {
            if let Some((next_seq, evidence)) = unconditional {
                normalized.push(ClaimCursorAdvance {
                    queue,
                    priority,
                    enqueue_shard,
                    next_seq,
                    only_if_current: None,
                    evidence,
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
                    evidence: None,
                });
            }
        }

        normalized
    }

    /// Replay the post-commit advance for an actual claim on the current server.
    #[doc(hidden)]
    pub async fn replay_claim_cursor_advance(
        &self,
        pool: &PgPool,
        claimed: &ClaimedRuntimeJob,
    ) -> Result<(), AwaError> {
        let claim = &claimed.claim;
        self.advance_claim_cursors_strict(
            pool,
            &[ClaimCursorAdvance {
                queue: claim.queue.clone(),
                priority: claim.priority,
                enqueue_shard: claim.enqueue_shard,
                next_seq: claim.lane_seq + 1,
                only_if_current: None,
                evidence: Some(ClaimEvidence {
                    ready_slot: claim.ready_slot,
                    ready_generation: claim.ready_generation,
                    lane_seq: claim.lane_seq,
                    legacy_attempt: claim
                        .receipt_id
                        .is_none()
                        .then_some((claimed.job.id, claimed.job.run_lease)),
                }),
            }],
        )
        .await
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
        let mut tx = pool
            .begin_with(CLAIM_TRANSACTION_BEGIN)
            .await
            .map_err(map_sqlx_error)?;
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
                WHERE ($5::bigint IS NULL OR {schema}.sequence_next_value(seq_name) = $5)
                  AND (
                      $6::int IS NULL
                      OR ($9::bigint IS NULL AND EXISTS (
                          SELECT 1
                          FROM {schema}.ready_claim_attempt_batches AS attempts
                          WHERE attempts.ready_slot = $6
                            AND attempts.ready_generation = $7
                            AND attempts.queue = $1
                            AND attempts.priority = $2
                            AND attempts.enqueue_shard = $3
                            AND attempts.first_lane_seq <= $8
                            AND attempts.next_lane_seq > $8
                            AND attempts.lane_ranges @> int8range($8, $8 + 1, '[)')
                      ))
                      OR EXISTS (
                          SELECT 1 FROM {schema}.leases AS leases
                          WHERE $9::bigint IS NOT NULL
                            AND leases.job_id = $9 AND leases.run_lease = $10
                            AND leases.ready_slot = $6 AND leases.ready_generation = $7
                            AND leases.queue = $1 AND leases.enqueue_shard = $3
                            AND leases.lane_seq = $8
                      )
                  )
                "#
            ))
            .bind(&advance.queue)
            .bind(advance.priority)
            .bind(advance.enqueue_shard)
            .bind(advance.next_seq)
            .bind(advance.only_if_current)
            .bind(advance.evidence.map(|evidence| evidence.ready_slot))
            .bind(advance.evidence.map(|evidence| evidence.ready_generation))
            .bind(advance.evidence.map(|evidence| evidence.lane_seq))
            .bind(
                advance
                    .evidence
                    .and_then(|evidence| evidence.legacy_attempt.map(|attempt| attempt.0)),
            )
            .bind(
                advance
                    .evidence
                    .and_then(|evidence| evidence.legacy_attempt.map(|attempt| attempt.1)),
            )
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
        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.ready_entries (ready_slot, ready_generation, job_id, kind, queue, args, priority, attempt, run_lease, max_attempts, lane_seq, enqueue_shard, run_at, attempted_at, created_at, unique_key, unique_states, payload)
            SELECT $1, $2, rows.*
            FROM unnest(
                $3::bigint[], $4::text[], $5::text[], $6::jsonb[], $7::smallint[],
                $8::smallint[], $9::bigint[], $10::smallint[], $11::bigint[], $12::smallint[],
                $13::timestamptz[], $14::timestamptz[], $15::timestamptz[], $16::bytea[],
                $17::text[], $18::jsonb[]
            ) AS rows
            "#
        ))
        .bind(ring.0)
        .bind(ring.1)
        .bind(rows.iter().map(|row| row.job_id).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.kind.as_str()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.queue.as_str()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| &row.args).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.priority).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.attempt).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.run_lease).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.max_attempts).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.lane_seq).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.enqueue_shard).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.run_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.attempted_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.created_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.unique_key.as_deref()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.unique_states.as_deref()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| storage_payload(&row.payload)).collect::<Vec<_>>())
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
        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.ready_segments (ready_slot, ready_generation, queue, priority, enqueue_shard, first_lane_seq, next_lane_seq, first_run_at)
            SELECT $1, $2, segments.*
            FROM unnest($3::text[], $4::smallint[], $5::smallint[], $6::bigint[], $7::bigint[], $8::timestamptz[]) AS segments
            ON CONFLICT (ready_slot, ready_generation, queue, priority, enqueue_shard, first_lane_seq) DO NOTHING
            "#
        ))
        .bind(ring.0)
        .bind(ring.1)
        .bind(segments.iter().map(|segment| segment.queue.as_str()).collect::<Vec<_>>())
        .bind(segments.iter().map(|segment| segment.priority).collect::<Vec<_>>())
        .bind(segments.iter().map(|segment| segment.enqueue_shard).collect::<Vec<_>>())
        .bind(segments.iter().map(|segment| segment.first_lane_seq).collect::<Vec<_>>())
        .bind(segments.iter().map(|segment| segment.next_lane_seq).collect::<Vec<_>>())
        .bind(segments.iter().map(|segment| segment.first_run_at).collect::<Vec<_>>())
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
        deferred_claims: Vec<(Vec<u8>, i64)>,
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

        self.sync_ready_enqueue_unique_claims(tx, &ready_rows, deferred_claims)
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
    ///
    /// The map's key order, `(queue, priority, shard)` ascending, is the
    /// order callers reserve lanes in. It is the canonical lane-lock order
    /// that `insert_many` and `insert_many_copy` also follow, so batches
    /// from either path cannot deadlock on each other's lane locks.
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
        deferred_claims: Vec<(Vec<u8>, i64)>,
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

        self.sync_ready_enqueue_unique_claims(tx, &ready_rows, deferred_claims)
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
        let rows = rows.into_iter().map(|row| (row, old_state)).collect();
        let (inserted, _) = self
            .insert_existing_ready_rows_inner_tx(tx, rows, false)
            .await?;
        Ok(inserted)
    }

    /// Re-enqueue `rows`, each paired with the state it leaves. Every row's
    /// unique claim is synced before any lane is reserved, the order every
    /// enqueue path takes them in. With `skip_conflicts`, a row whose unique
    /// key a live job holds gets no lane position and its job ID is returned.
    async fn insert_existing_ready_rows_inner_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: Vec<(ExistingReadyRow, Option<JobState>)>,
        skip_conflicts: bool,
    ) -> Result<(usize, Vec<i64>), AwaError> {
        if rows.is_empty() {
            return Ok((0, Vec::new()));
        }

        let conflicts = self
            .sync_reenqueue_unique_claims(tx, &rows, skip_conflicts)
            .await?;

        let mut claimed = Vec::with_capacity(rows.len());
        let mut skipped = Vec::with_capacity(conflicts.len());
        for (row, _) in rows {
            if conflicts.contains(&row.job_id) {
                skipped.push(row.job_id);
            } else {
                claimed.push(row);
            }
        }
        let inserted = self.insert_claimed_ready_rows_tx(tx, claimed).await?;
        Ok((inserted, skipped))
    }

    /// Reserve lane positions for `rows` and write them to the ready ring.
    /// Their unique claims must already be synced.
    async fn insert_claimed_ready_rows_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: Vec<ExistingReadyRow>,
    ) -> Result<usize, AwaError> {
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

        if ready_rows.is_empty() {
            return Ok(0);
        }
        let ring = self.current_queue_ring(tx).await?;
        self.execute_ready_inserts_tx(tx, ring, &ready_rows).await?;
        self.insert_ready_segments_tx(tx, ring, &ready_rows).await?;
        Ok(ready_rows.len())
    }

    /// Sync the unique claims of rows moving to `available` through
    /// [`Self::apply_unique_claim_changes_tx`].
    async fn sync_reenqueue_unique_claims<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[(ExistingReadyRow, Option<JobState>)],
        skip_conflicts: bool,
    ) -> Result<HashSet<i64>, AwaError> {
        let changes = rows
            .iter()
            .filter_map(|(row, old_state)| {
                unique_claim_change(
                    row.job_id,
                    row.unique_key.as_deref(),
                    row.unique_states.as_deref(),
                    *old_state,
                    Some(JobState::Available),
                )
            })
            .collect();
        self.apply_unique_claim_changes_tx(tx, changes, skip_conflicts)
            .await
    }

    /// Claim distinct `pending` keys in the given order, adding the job IDs
    /// whose key another job holds to `conflicts`, and clear `pending`.
    async fn claim_unique_keys_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        pending: &mut Vec<(&[u8], i64)>,
        conflicts: &mut HashSet<i64>,
    ) -> Result<(), AwaError> {
        if pending.is_empty() {
            return Ok(());
        }
        let (keys, job_ids): (Vec<&[u8]>, Vec<i64>) = pending.drain(..).unzip();
        let claimed: HashSet<i64> = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO awa.job_unique_claims (unique_key, job_id)
            SELECT input.unique_key, input.job_id
            FROM unnest($1::bytea[], $2::bigint[]) WITH ORDINALITY AS input(unique_key, job_id, ord)
            ORDER BY input.ord
            ON CONFLICT (unique_key)
            DO UPDATE SET job_id = EXCLUDED.job_id
            WHERE awa.job_unique_claims.job_id = EXCLUDED.job_id
            RETURNING job_id
            "#,
        )
        .bind(&keys)
        .bind(&job_ids)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?
        .into_iter()
        .collect();
        conflicts.extend(
            job_ids
                .into_iter()
                .filter(|job_id| !claimed.contains(job_id)),
        );
        Ok(())
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
            let changes = deferred_claim_changes(&rows, old_state).collect();
            self.apply_unique_claim_changes_tx(tx, changes, false)
                .await?;
        }

        self.write_deferred_rows_tx(tx, &rows).await
    }

    /// Insert `rows` into `deferred_jobs` without touching unique claims.
    async fn write_deferred_rows_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[DeferredJobRow],
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }
        let schema = self.schema();
        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.deferred_jobs (job_id, kind, queue, args, state, priority, attempt, run_lease, max_attempts, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload)
            SELECT job_id, kind, queue, args, state::awa.job_state, priority, attempt, run_lease, max_attempts, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload
            FROM unnest(
                $1::bigint[], $2::text[], $3::text[], $4::jsonb[], $5::text[], $6::smallint[],
                $7::smallint[], $8::bigint[], $9::smallint[], $10::timestamptz[], $11::timestamptz[],
                $12::timestamptz[], $13::timestamptz[], $14::bytea[], $15::text[], $16::jsonb[]
            ) AS rows (job_id, kind, queue, args, state, priority, attempt, run_lease, max_attempts, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload)
            "#
        ))
        .bind(rows.iter().map(|row| row.job_id).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.kind.as_str()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.queue.as_str()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| &row.args).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.state.as_str()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.priority).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.attempt).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.run_lease).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.max_attempts).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.run_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.attempted_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.finalized_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.created_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.unique_key.as_deref()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.unique_states.as_deref()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| storage_payload(&row.payload)).collect::<Vec<_>>())
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
        self.write_deferred_rows_copy_tx(tx, &rows).await
    }

    /// COPY `rows` into `deferred_jobs` without touching unique claims.
    async fn write_deferred_rows_copy_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[DeferredJobRow],
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }
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
        for row in rows {
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
        let changes = done_claim_changes(rows, old_state).collect();
        self.apply_unique_claim_changes_tx(tx, changes, false)
            .await?;
        self.write_done_rows_tx(tx, rows, old_state).await
    }

    /// Write `rows` into `done_entries` without touching unique claims.
    async fn write_done_rows_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[DoneJobRow],
        old_state: Option<JobState>,
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
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
            sqlx::query(&format!(
                r#"
                INSERT INTO {schema}.done_entries (ready_slot, ready_generation, job_id, kind, queue, state, priority, attempt, run_lease, lane_seq, enqueue_shard, attempted_at, finalized_at, payload)
                SELECT ready_slot, ready_generation, job_id, kind, queue, state::awa.job_state, priority, attempt, run_lease, lane_seq, enqueue_shard, attempted_at, finalized_at, payload
                FROM unnest(
                    $1::int[], $2::bigint[], $3::bigint[], $4::text[], $5::text[], $6::text[],
                    $7::smallint[], $8::smallint[], $9::bigint[], $10::bigint[], $11::smallint[],
                    $12::timestamptz[], $13::timestamptz[], $14::jsonb[]
                ) AS rows (ready_slot, ready_generation, job_id, kind, queue, state, priority, attempt, run_lease, lane_seq, enqueue_shard, attempted_at, finalized_at, payload)
                "#
            ))
            .bind(ready_backed.iter().map(|row| row.ready_slot).collect::<Vec<_>>())
            .bind(ready_backed.iter().map(|row| row.ready_generation).collect::<Vec<_>>())
            .bind(ready_backed.iter().map(|row| row.job_id).collect::<Vec<_>>())
            .bind(ready_backed.iter().map(|row| row.kind.as_str()).collect::<Vec<_>>())
            .bind(ready_backed.iter().map(|row| row.queue.as_str()).collect::<Vec<_>>())
            .bind(ready_backed.iter().map(|row| row.state.as_str()).collect::<Vec<_>>())
            .bind(ready_backed.iter().map(|row| row.priority).collect::<Vec<_>>())
            .bind(ready_backed.iter().map(|row| row.attempt).collect::<Vec<_>>())
            .bind(ready_backed.iter().map(|row| row.run_lease).collect::<Vec<_>>())
            .bind(ready_backed.iter().map(|row| row.lane_seq).collect::<Vec<_>>())
            .bind(ready_backed.iter().map(|row| row.enqueue_shard).collect::<Vec<_>>())
            .bind(ready_backed.iter().map(|row| row.attempted_at).collect::<Vec<_>>())
            .bind(ready_backed.iter().map(|row| row.finalized_at).collect::<Vec<_>>())
            .bind(terminal_payloads(&ready_backed, &ready_payloads))
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        if !synthetic.is_empty() {
            sqlx::query(&format!(
                r#"
                INSERT INTO {schema}.done_entries (ready_slot, ready_generation, job_id, kind, queue, args, state, priority, attempt, run_lease, max_attempts, lane_seq, enqueue_shard, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload)
                SELECT ready_slot, ready_generation, job_id, kind, queue, args, state::awa.job_state, priority, attempt, run_lease, max_attempts, lane_seq, enqueue_shard, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload
                FROM unnest(
                    $1::int[], $2::bigint[], $3::bigint[], $4::text[], $5::text[], $6::jsonb[],
                    $7::text[], $8::smallint[], $9::smallint[], $10::bigint[], $11::smallint[],
                    $12::bigint[], $13::smallint[], $14::timestamptz[], $15::timestamptz[],
                    $16::timestamptz[], $17::timestamptz[], $18::bytea[], $19::text[], $20::jsonb[]
                ) AS rows (ready_slot, ready_generation, job_id, kind, queue, args, state, priority, attempt, run_lease, max_attempts, lane_seq, enqueue_shard, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload)
                "#
            ))
            .bind(synthetic.iter().map(|row| row.ready_slot).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.ready_generation).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.job_id).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.kind.as_str()).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.queue.as_str()).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| &row.args).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.state.as_str()).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.priority).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.attempt).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.run_lease).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.max_attempts).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.lane_seq).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.enqueue_shard).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.run_at).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.attempted_at).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.finalized_at).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.created_at).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.unique_key.as_deref()).collect::<Vec<_>>())
            .bind(synthetic.iter().map(|row| row.unique_states.as_deref()).collect::<Vec<_>>())
            .bind(terminal_payloads(&synthetic, &ready_payloads))
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
        let changes = dlq_claim_changes(rows, old_state).collect();
        self.apply_unique_claim_changes_tx(tx, changes, false)
            .await?;
        self.write_dlq_rows_tx(tx, rows).await
    }

    /// Write `rows` into `dlq_entries` without touching unique claims.
    async fn write_dlq_rows_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[DlqJobRow],
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let schema = self.schema();
        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.dlq_entries (job_id, kind, queue, args, state, priority, attempt, run_lease, max_attempts, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload, dlq_reason, dlq_at, original_run_lease)
            SELECT job_id, kind, queue, args, state::awa.job_state, priority, attempt, run_lease, max_attempts, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload, dlq_reason, dlq_at, original_run_lease
            FROM unnest(
                $1::bigint[], $2::text[], $3::text[], $4::jsonb[], $5::text[], $6::smallint[],
                $7::smallint[], $8::bigint[], $9::smallint[], $10::timestamptz[], $11::timestamptz[],
                $12::timestamptz[], $13::timestamptz[], $14::bytea[], $15::text[], $16::jsonb[],
                $17::text[], $18::timestamptz[], $19::bigint[]
            ) AS rows (job_id, kind, queue, args, state, priority, attempt, run_lease, max_attempts, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload, dlq_reason, dlq_at, original_run_lease)
            "#
        ))
        .bind(rows.iter().map(|row| row.job_id).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.kind.as_str()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.queue.as_str()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| &row.args).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.state.as_str()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.priority).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.attempt).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.run_lease).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.max_attempts).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.run_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.attempted_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.finalized_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.created_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.unique_key.as_deref()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.unique_states.as_deref()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| storage_payload(&row.payload)).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.dlq_reason.as_str()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.dlq_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.original_run_lease).collect::<Vec<_>>())
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        Ok(rows.len())
    }

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

    async fn enqueue_runtime_rows(
        &self,
        pool: &PgPool,
        rows: Vec<RuntimeReadyRow>,
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let total_rows = self
            .insert_ready_rows_tx(&mut tx, rows.clone(), Vec::new())
            .await?;

        tx.commit().await.map_err(map_sqlx_error)?;
        self.wake_queues_after_commit(pool, rows.into_iter().map(|row| row.queue));
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

        for job in jobs {
            let prepared = prepare_row_raw(job.kind.clone(), job.args.clone(), job.opts.clone())?;
            let payload = Self::payload_from_parts(prepared.metadata, prepared.tags, None, None)?;
            let queue = self.queue_stripe_for_enqueue(
                &prepared.queue,
                prepared.ordering_key.as_deref(),
                prepared.unique_key.as_deref(),
                None,
            );

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

        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let mut total = 0usize;
        let ids = self.next_job_ids(&mut tx, deferred_rows.len()).await?;
        let deferred_rows: Vec<_> = deferred_rows
            .into_iter()
            .zip(ids)
            .map(|(row, id)| DeferredJobRow { job_id: id, ..row })
            .collect();
        // Every key of the batch is claimed in one key-ordered statement
        // before any lane is reserved.
        if ready_rows.is_empty() {
            total += self
                .insert_deferred_rows_tx(&mut tx, deferred_rows, None)
                .await?;
        } else {
            total += self
                .insert_ready_rows_tx(
                    &mut tx,
                    ready_rows.clone(),
                    deferred_enqueue_claims(&deferred_rows),
                )
                .await?;
            total += self.write_deferred_rows_tx(&mut tx, &deferred_rows).await?;
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        self.wake_queues_after_commit(pool, queues_to_notify);
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

        for job in jobs {
            let prepared = prepare_row_raw(job.kind.clone(), job.args.clone(), job.opts.clone())?;
            let payload = Self::payload_from_parts(prepared.metadata, prepared.tags, None, None)?;
            let queue = self.queue_stripe_for_enqueue(
                &prepared.queue,
                prepared.ordering_key.as_deref(),
                prepared.unique_key.as_deref(),
                None,
            );

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

        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let mut total = 0usize;
        let job_ids = self
            .next_job_ids(&mut tx, ready_rows.len() + deferred_rows.len())
            .await?;
        let (ready_job_ids, deferred_job_ids) = job_ids.split_at(ready_rows.len());
        let deferred_rows: Vec<_> = deferred_rows
            .into_iter()
            .zip(deferred_job_ids.iter().copied())
            .map(|(row, id)| DeferredJobRow { job_id: id, ..row })
            .collect();
        // As in `enqueue_params_batch`, every key is claimed in one
        // key-ordered statement before any lane is reserved.
        if ready_rows.is_empty() {
            total += self
                .insert_deferred_rows_copy_tx(&mut tx, deferred_rows)
                .await?;
        } else {
            total += self
                .insert_ready_rows_copy_tx(
                    &mut tx,
                    ready_rows,
                    ready_job_ids.to_vec(),
                    deferred_enqueue_claims(&deferred_rows),
                )
                .await?;
            total += self
                .write_deferred_rows_copy_tx(&mut tx, &deferred_rows)
                .await?;
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        self.wake_queues_after_commit(pool, queues_to_notify);
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

        let mut tx = pool
            .begin_with(CLAIM_TRANSACTION_BEGIN)
            .await
            .map_err(map_sqlx_error)?;
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

        let mut tx = pool
            .begin_with(CLAIM_TRANSACTION_BEGIN)
            .await
            .map_err(map_sqlx_error)?;
        let mut claimed = Vec::new();
        claimed.extend(
            self.claim_ready_rows_tx(&mut tx, queue, max_batch, deadline_duration, aging_interval)
                .await?,
        );

        let changes = claimed
            .iter()
            .filter_map(|row| {
                unique_claim_change(
                    row.job_id,
                    row.unique_key.as_deref(),
                    row.unique_states.as_deref(),
                    Some(JobState::Available),
                    Some(JobState::Running),
                )
            })
            .collect();
        self.apply_unique_claim_changes_tx(&mut tx, changes, false)
            .await?;

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

    #[doc(hidden)]
    pub fn cached_claimer_lease_count(&self) -> usize {
        self.claimer_gates
            .lock()
            .expect("claimer gate cache poisoned")
            .len()
    }

    fn cache_claimer_gate(&self, key: (String, Uuid), lease: QueueClaimerLeaseRow) {
        self.claimer_gates
            .lock()
            .expect("claimer gate cache poisoned")
            .insert(
                key,
                CachedClaimerGate {
                    checked_at: Instant::now(),
                    lease,
                },
            );
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

    /// The queue's shared claimer target, recomputed from the claimer signal
    /// once the stored value is older than `control_interval`.
    ///
    /// A recomputed target is written only when it differs from the stored
    /// one, and with a plain `UPDATE` whose `WHERE` excludes the stored value.
    /// Such an `UPDATE` takes no row lock on a row it skips, so steady-state
    /// refreshes from every instance neither write nor wait on each other.
    /// (`INSERT ... ON CONFLICT DO UPDATE ... WHERE` would lock the existing
    /// row even when its `WHERE` is false.) `updated_at` therefore records
    /// the last change, and a change stands for `control_interval` before
    /// anyone recomputes it.
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
        let clamp = |target: i16| target.clamp(1, max_claimers.max(1));

        let stored: Option<(i16, bool)> = sqlx::query_as(&format!(
            r#"
            SELECT target_claimers, updated_at > $2
            FROM {schema}.queue_claimer_state
            WHERE queue = $1
            "#
        ))
        .bind(queue)
        .bind(stale_cutoff)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;
        if let Some((target, true)) = stored {
            return Ok(clamp(target));
        }
        let current_target = stored.map(|(target, _)| target);

        let signal = self.queue_claimer_signal(pool, queue).await?;
        let desired = self.desired_queue_claimer_target(current_target, &signal, max_claimers);
        if current_target == Some(desired) {
            return Ok(clamp(desired));
        }

        let written = match current_target {
            None => sqlx::query_scalar::<_, i16>(&format!(
                r#"
                INSERT INTO {schema}.queue_claimer_state (queue, target_claimers, updated_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (queue) DO NOTHING
                RETURNING target_claimers
                "#
            ))
            .bind(queue)
            .bind(desired)
            .bind(now)
            .fetch_optional(pool)
            .await
            .map_err(map_sqlx_error)?,
            Some(_) => sqlx::query_scalar::<_, i16>(&format!(
                r#"
                UPDATE {schema}.queue_claimer_state
                SET target_claimers = $2,
                    updated_at = $3
                WHERE queue = $1
                  AND target_claimers IS DISTINCT FROM $2
                  AND updated_at <= $4
                RETURNING target_claimers
                "#
            ))
            .bind(queue)
            .bind(desired)
            .bind(now)
            .bind(stale_cutoff)
            .fetch_optional(pool)
            .await
            .map_err(map_sqlx_error)?,
        };

        // Another instance changed or created the row first; its target
        // stands until the next recompute.
        Ok(clamp(written.or(current_target).unwrap_or(desired)))
    }

    /// Whether a claim of `queue` could find a ready row, from the same
    /// head-table read as the claimer signal. `false` is exact: every lane's
    /// claim cursor has reached its enqueue cursor, so no ready row is
    /// committed or reserved. `true` is an upper bound and may be stale (see
    /// [`Self::queue_claimer_signal`]); the caller still claims to find out.
    pub async fn queue_may_have_ready(&self, pool: &PgPool, queue: &str) -> Result<bool, AwaError> {
        Ok(self.queue_claimer_signal(pool, queue).await?.available > 0)
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
        let available: i64 = sqlx::query_scalar(&claimer_signal_sql(schema))
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
        let gate_key = (queue.to_string(), instance_id);
        let cached_lease = self
            .claimer_gates
            .lock()
            .expect("claimer gate cache poisoned")
            .get(&gate_key)
            .filter(|gate| {
                gate.checked_at.elapsed() < CLAIMER_GATE_CACHE_TTL
                    && gate.lease.expires_at > Utc::now() + TimeDelta::seconds(1)
            })
            .map(|gate| gate.lease);
        let lease = match cached_lease {
            Some(lease) => lease,
            None => {
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
                    self.claimer_gates
                        .lock()
                        .expect("claimer gate cache poisoned")
                        .remove(&gate_key);
                    return Ok(Vec::new());
                };
                self.cache_claimer_gate(gate_key.clone(), lease);
                lease
            }
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

        let now = Utc::now();
        if !claimed.is_empty() && lease.needs_refresh(now, lease_ttl, idle_threshold) {
            // The claim has committed: these jobs are this runtime's whatever
            // happens to the lease refresh, so its failure must not fail the
            // call and strand them until rescue. Forget the cached lease
            // instead, so the next claim re-reads it.
            let refreshed = match self
                .mark_queue_claimer_active(pool, queue, instance_id, lease.lease(), lease_ttl)
                .await
            {
                Ok(true) => TimeDelta::from_std(lease_ttl)
                    .ok()
                    .map(|ttl| QueueClaimerLeaseRow {
                        last_claimed_at: now,
                        expires_at: now + ttl,
                        ..lease
                    }),
                Ok(false) => None,
                Err(err) => {
                    tracing::warn!(
                        error = ?err,
                        queue,
                        claimed = claimed.len(),
                        "failed to refresh the queue claimer lease after a committed claim; \
                         dispatching the claimed jobs"
                    );
                    None
                }
            };
            match refreshed {
                Some(refreshed) => self.cache_claimer_gate(gate_key, refreshed),
                None => {
                    self.claimer_gates
                        .lock()
                        .expect("claimer gate cache poisoned")
                        .remove(&gate_key);
                }
            }
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;

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
        locks: AttemptLocks,
    ) -> Result<RuntimeBatchCompletion, AwaError> {
        if claimed.is_empty() {
            return Ok(RuntimeBatchCompletion::default());
        }

        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let (held, contended) = match locks {
            AttemptLocks::Wait => (claimed.to_vec(), Vec::new()),
            AttemptLocks::Try => match self.try_lock_runtime_attempts_tx(&mut tx, claimed).await {
                Ok(split) => split,
                Err(err) => {
                    let _ = tx.rollback().await;
                    return Err(err);
                }
            },
        };
        if held.is_empty() {
            let _ = tx.rollback().await;
            return Ok(RuntimeBatchCompletion {
                updated: Vec::new(),
                contended,
            });
        }
        let claimed = held.as_slice();

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
        if let Err(err) = self
            .lock_receipt_attempt_keys_tx(&mut tx, &receipt_pairs)
            .await
        {
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
        Ok(RuntimeBatchCompletion {
            updated: rows,
            contended,
        })
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
        Ok(self
            .complete_runtime_batch_inner(pool, claimed, AttemptLocks::Wait)
            .await?
            .updated)
    }

    /// [`Self::complete_runtime_batch`] for a batch of unrelated attempts: an
    /// attempt whose completion lock another transaction holds is left out
    /// and returned in [`RuntimeBatchCompletion::contended`] instead of
    /// holding up the rest of the batch. An application that calls
    /// `admin::cancel_tx` inside its own transaction holds that lock until it
    /// commits. The caller completes contended attempts on their own.
    #[tracing::instrument(
        skip(self, pool, claimed),
        name = "queue_storage.complete_runtime_batch_isolating"
    )]
    pub async fn complete_runtime_batch_isolating(
        &self,
        pool: &PgPool,
        claimed: &[ClaimedRuntimeJob],
    ) -> Result<RuntimeBatchCompletion, AwaError> {
        self.complete_runtime_batch_inner(pool, claimed, AttemptLocks::Try)
            .await
    }

    async fn complete_runtime_batch_inner(
        &self,
        pool: &PgPool,
        claimed: &[ClaimedRuntimeJob],
        locks: AttemptLocks,
    ) -> Result<RuntimeBatchCompletion, AwaError> {
        if claimed.is_empty() {
            return Ok(RuntimeBatchCompletion::default());
        }
        let claimed = unique_attempts(claimed);
        let claimed = claimed.as_ref();

        if self.lease_claim_receipts() && claimed.iter().all(Self::receipt_fast_complete_candidate)
        {
            let mut outcome = self
                .complete_receipt_runtime_batch_fast(pool, claimed, locks)
                .await?;
            if outcome.updated.len() + outcome.contended.len() == claimed.len() {
                return Ok(outcome);
            }

            let settled: BTreeSet<(i64, i64)> = outcome
                .updated
                .iter()
                .chain(outcome.contended.iter())
                .copied()
                .collect();
            let missed: Vec<_> = claimed
                .iter()
                .filter(|entry| !settled.contains(&(entry.job.id, entry.job.run_lease)))
                .cloned()
                .collect();
            if !missed.is_empty() {
                let slow = self
                    .complete_runtime_batch_slow(pool, &missed, locks)
                    .await?;
                outcome.updated.extend(slow.updated);
                outcome.contended.extend(slow.contended);
            }
            return Ok(outcome);
        }

        self.complete_runtime_batch_slow(pool, claimed, locks).await
    }

    async fn complete_runtime_batch_slow(
        &self,
        pool: &PgPool,
        claimed: &[ClaimedRuntimeJob],
        locks: AttemptLocks,
    ) -> Result<RuntimeBatchCompletion, AwaError> {
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        // With `Try`, the attempts this transaction could lock stay locked to
        // commit; the slow path's own `lock_receipt_attempts_tx` then takes
        // them again, which does not wait on this transaction's own locks.
        let (claimed, contended) = match locks {
            AttemptLocks::Wait => (claimed.to_vec(), Vec::new()),
            AttemptLocks::Try if self.lease_claim_receipts() => {
                self.try_lock_runtime_attempts_tx(&mut tx, claimed).await?
            }
            AttemptLocks::Try => (claimed.to_vec(), Vec::new()),
        };
        let updated = self
            .complete_runtime_batch_slow_in_tx(&mut tx, &claimed)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(RuntimeBatchCompletion { updated, contended })
    }

    /// Try to take the completion lock of each attempt in `claimed` without
    /// waiting. Returns the attempts this transaction now holds and the
    /// `(job_id, run_lease)` pairs another transaction holds.
    async fn try_lock_runtime_attempts_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        claimed: &[ClaimedRuntimeJob],
    ) -> Result<(Vec<ClaimedRuntimeJob>, Vec<(i64, i64)>), AwaError> {
        let pairs: Vec<(i64, i64)> = claimed
            .iter()
            .map(|entry| (entry.job.id, entry.job.run_lease))
            .collect();
        let acquired: BTreeSet<(i64, i64)> = self
            .try_lock_receipt_attempt_keys_tx(tx, &pairs)
            .await?
            .into_iter()
            .collect();
        let contended: BTreeSet<(i64, i64)> = pairs
            .iter()
            .filter(|pair| !acquired.contains(pair))
            .copied()
            .collect();
        if contended.is_empty() {
            return Ok((claimed.to_vec(), Vec::new()));
        }
        let held = claimed
            .iter()
            .filter(|entry| !contended.contains(&(entry.job.id, entry.job.run_lease)))
            .cloned()
            .collect();
        Ok((held, contended.into_iter().collect()))
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
        let claimed = unique_attempts(claimed);
        let claimed = claimed.as_ref();

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
            // Both groups' done rows are written together, so their unique
            // claims change in one key-ordered pass.
            let mut done_rows = Vec::new();

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
                let receipt_claim_batch_ids: Vec<Option<i64>> = receipt_claimed
                    .iter()
                    .map(|entry| entry.claim.claim_batch_id)
                    .collect();
                let receipt_claim_batch_indices: Vec<Option<i32>> = receipt_claimed
                    .iter()
                    .map(|entry| entry.claim.claim_batch_index)
                    .collect();
                let closure_rel = format!("{schema}.lease_claim_closures");
                let closure_batch_rel = format!("{schema}.lease_claim_closure_batches");
                let closed_evidence =
                    receipt_closed_evidence_sql(schema, &closure_rel, &closure_batch_rel, "claims");
                let updated: Vec<(i64, i64)> = sqlx::query_as(&format!(
                    r#"
                    WITH completed(claim_slot, job_id, run_lease, receipt_id, claim_batch_id, claim_batch_index) AS (
                        SELECT * FROM unnest($1::int[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::int[])
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
                        FROM completed
                        JOIN {schema}.lease_claim_batches AS claim_batches
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
                .bind(&receipt_claim_batch_ids)
                .bind(&receipt_claim_batch_indices)
                .fetch_all(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                if !updated.is_empty() {
                    let finalized_at = Utc::now();
                    for (job_id, run_lease) in &updated {
                        if let Some(runtime_job) = claimed_map.get(&(*job_id, *run_lease)).cloned()
                        {
                            done_rows.push(runtime_job.into_done_row(finalized_at)?);
                        }
                    }
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
                    for deleted_row in deleted {
                        if let Some(runtime_job) = claimed_map
                            .get(&(deleted_row.job_id, deleted_row.run_lease))
                            .cloned()
                        {
                            done_rows.push(runtime_job.into_done_row(finalized_at)?);
                            updated_all.push((deleted_row.job_id, deleted_row.run_lease));
                        }
                    }
                }
            }

            self.insert_done_rows_tx(tx, &done_rows, Some(JobState::Running))
                .await?;
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
                SELECT
                    COALESCE(
                        sum(
                            GREATEST(
                                COALESCE(lanes.pruned_completed_count, 0),
                                COALESCE(rollups.pruned_completed_count, 0)
                            )
                        ),
                        0
                    )::bigint AS completed,
                    COALESCE(
                        sum(COALESCE(rollups.pruned_failed_count, 0)),
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
	                    -- Zero-deadline compact receipt claims live in
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
        let signal_sql = claimer_signal_sql(schema);
        // The three counts come from one statement and one snapshot.
        //
        // available: dispatcher signal — an index-only sum over the
        // (queue, priority, enqueue_shard) head tables.
        // running: leases.state = 'running' only, matching
        // queue_counts_exact's strict definition. Receipt-plane claims
        // that haven't materialised a lease row yet are documented as
        // omitted in the method-level doc.
        // terminal: denormalised rollup only. Live (un-rolled-up)
        // done_entries rows are excluded — see method-level docs. The
        // GREATEST legacy dedupe applies to the completed column only:
        // queue_lanes never carried a failed column.
        let (available, running, pruned_completed, pruned_failed): (i64, i64, i64, i64) =
            sqlx::query_as(&format!(
                r#"
            SELECT
                ({signal_sql}) AS available,
                (
                    SELECT COALESCE(count(*)::bigint, 0)
                    FROM {schema}.leases
                    WHERE queue = ANY($1)
                      AND state = 'running'
                ) AS running,
                terminal.pruned_completed,
                terminal.pruned_failed
            FROM (
                SELECT
                    COALESCE(sum(GREATEST(
                        COALESCE(lanes.pruned_completed_count, 0),
                        COALESCE(rollups.pruned_completed_count, 0)
                    )), 0)::bigint AS pruned_completed,
                    COALESCE(sum(COALESCE(rollups.pruned_failed_count, 0)), 0)::bigint
                        AS pruned_failed
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
            ) AS terminal
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
        let pruned_failed: i64 = sqlx::query_scalar(&format!(
            r#"
            SELECT COALESCE(sum(pruned_failed_count), 0)::bigint
            FROM {schema}.queue_terminal_rollups
            WHERE queue = ANY($1)
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
        let Some((ready_row, old_state)) = self.take_retry_row_tx(tx, job_id).await? else {
            return Ok(None);
        };
        let row = Self::retried_job_row(ready_row.clone())?;
        self.insert_existing_ready_rows_tx(tx, vec![ready_row], Some(old_state))
            .await?;
        Ok(Some(row))
    }

    fn retried_job_row(ready_row: ExistingReadyRow) -> Result<JobRow, AwaError> {
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

    /// Remove a waiting, failed or cancelled job from where it sits and
    /// return its ready row and the state it leaves, without enqueueing it.
    async fn take_retry_row_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
    ) -> Result<Option<(ExistingReadyRow, JobState)>, AwaError> {
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
            return Ok(Some((ready_row, waiting.state)));
        }

        let done_projection = done_row_projection("done", "ready");
        let ready_join = done_ready_join(schema, "done", "ready");
        let terminal: Option<HydratedTerminalRow> = sqlx::query_as(&format!(
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
            SELECT {done_projection}, COALESCE(ready.priority, done.priority) AS body_priority
            FROM deleted AS done
            {ready_join}
            "#
        ))
        .bind(job_id)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if let Some(hydrated) = terminal {
            let body_priority = hydrated.body_priority;
            let terminal = hydrated.terminal;
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
                priority: body_priority,
                attempt: 0,
                run_lease: terminal.run_lease,
                max_attempts: terminal.max_attempts,
                run_at: Utc::now(),
                attempted_at: None,
                created_at: terminal.created_at,
                unique_states: revived_unique_states(
                    terminal.state,
                    terminal.unique_key.as_deref(),
                    terminal.unique_states,
                    &terminal.payload,
                )?,
                unique_key: terminal.unique_key,
                payload: terminal.payload,
            };
            return Ok(Some((ready_row, terminal.state)));
        }

        Ok(None)
    }

    pub async fn retry_job(&self, pool: &PgPool, job_id: i64) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let row = self.retry_job_tx(&mut tx, job_id).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        if let Some(row) = &row {
            self.wake_queues_after_commit(pool, [row.queue.clone()]);
        }
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

        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let mut taken = Vec::with_capacity(unique_ids.len());
        for job_id in &unique_ids {
            if let Some((ready_row, old_state)) = self.take_retry_row_tx(&mut tx, *job_id).await? {
                taken.push((ready_row, Some(old_state)));
            }
        }
        let rows = taken
            .iter()
            .map(|(ready_row, _)| Self::retried_job_row(ready_row.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let queues: BTreeSet<String> = taken
            .iter()
            .map(|(ready_row, _)| ready_row.queue.clone())
            .collect();
        self.insert_existing_ready_rows_inner_tx(&mut tx, taken, false)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        self.wake_queues_after_commit(pool, queues);
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
                WHERE claim_batches.job_ids && $1::bigint[]
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

    /// SQL true when the ready row `alias` has been claimed. A claim records
    /// its attempt in its own transaction but moves the lane's claim cursor
    /// past the row only after commit, so a claimed row can still sit at or
    /// after the cursor; claims skip such rows on the same evidence. Receipt
    /// claims leave an attempt batch, and materialized claims a later attempt
    /// of the job, which is checked whatever this store's mode, since admin
    /// callers build the store with default configuration.
    fn ready_row_claimed_sql(&self, alias: &str) -> String {
        let schema = self.schema();
        let mut sql = self.ready_row_attempt_batch_sql(alias);
        for table in [
            "leases",
            "done_entries",
            "deferred_jobs",
            "dlq_entries",
            "ready_entries",
        ] {
            sql.push_str(&format!(
                " OR EXISTS (
                    SELECT 1 FROM {schema}.{table} AS attempt
                    WHERE attempt.job_id = {alias}.job_id
                      AND attempt.run_lease = {alias}.run_lease + 1
                )"
            ));
        }
        sql
    }

    /// SQL true when the ready row `alias` is covered by a claim-attempt
    /// batch, the only claim evidence `claim_ready_runtime` trusts in
    /// receipts mode.
    fn ready_row_attempt_batch_sql(&self, alias: &str) -> String {
        let schema = self.schema();
        format!(
            "EXISTS (
                SELECT 1 FROM {schema}.ready_claim_attempt_batches AS attempts
                WHERE attempts.ready_slot = {alias}.ready_slot
                  AND attempts.ready_generation = {alias}.ready_generation
                  AND attempts.queue = {alias}.queue
                  AND attempts.priority = {alias}.priority
                  AND attempts.enqueue_shard = {alias}.enqueue_shard
                  AND attempts.first_lane_seq <= {alias}.lane_seq
                  AND attempts.next_lane_seq > {alias}.lane_seq
                  AND attempts.lane_ranges @> int8range({alias}.lane_seq, {alias}.lane_seq + 1, '[)')
            )"
        )
    }

    /// SQL true when the ready row `alias` was claimed, by the evidence the
    /// schema's claim function trusts: claim-attempt batches with receipts,
    /// [`Self::ready_row_claimed_sql`] without.
    fn ready_row_claimed_for_mode_sql(&self, alias: &str, receipts: bool) -> String {
        if receipts {
            self.ready_row_attempt_batch_sql(alias)
        } else {
            self.ready_row_claimed_sql(alias)
        }
    }

    /// The row and batch receipt lookups behind a receipt-only cancel, each
    /// its own statement. A lookup that locks a claim another transaction has
    /// just rewritten rechecks it, and PostgreSQL 18 frees the statement's
    /// run-time partition pruning state when that lock runs out of rows; a
    /// later pruning pass in the same statement would read freed memory.
    fn receipt_cancel_lookup_sql(&self, lock: bool) -> [String; 2] {
        let schema = self.schema();
        let closure_rel = format!("{schema}.lease_claim_closures");
        let closure_batch_rel = format!("{schema}.lease_claim_closure_batches");
        let closed_evidence =
            receipt_closed_evidence_sql(schema, &closure_rel, &closure_batch_rel, "claims");
        let row_sql = format!(
            r#"
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
            "#
        );
        let batch_sql = format!(
            r#"
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
            WHERE claim_batches.job_ids @> ARRAY[$1::bigint]
              AND items.job_id = $1
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
            "#
        );
        if lock {
            [row_sql, batch_sql]
        } else {
            [
                row_sql.replace("FOR UPDATE OF claims SKIP LOCKED", ""),
                batch_sql.replace("FOR UPDATE OF claim_batches SKIP LOCKED", ""),
            ]
        }
    }

    /// The run lease of the job's open receipt-backed attempt, read without
    /// locking anything.
    async fn open_receipt_run_lease_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
    ) -> Result<Option<i64>, AwaError> {
        let mut run_lease = None;
        for sql in self.receipt_cancel_lookup_sql(false) {
            let found: Option<ReceiptCancelRow> = sqlx::query_as(&sql)
                .bind(job_id)
                .fetch_optional(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            run_lease = run_lease.max(found.map(|receipt| receipt.1));
        }
        Ok(run_lease)
    }

    /// Lock the job's open receipt-backed attempt, preferring the newer run
    /// lease when it has both a row and a batch receipt.
    async fn lock_open_receipt_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
    ) -> Result<Option<ReceiptCancelRow>, AwaError> {
        let mut newest: Option<ReceiptCancelRow> = None;
        for sql in self.receipt_cancel_lookup_sql(true) {
            let found: Option<ReceiptCancelRow> = sqlx::query_as(&sql)
                .bind(job_id)
                .fetch_optional(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            if let Some(found) = found {
                if newest.as_ref().is_none_or(|current| found.1 > current.1) {
                    newest = Some(found);
                }
            }
        }
        Ok(newest)
    }

    async fn cancel_materialized_job_lease_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
    ) -> Result<Option<CancelJobTxResult>, AwaError> {
        let schema = self.schema();
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
            let claim_change = owned_done_claim_change(&done, Some(lease.state));
            self.write_done_rows_tx(tx, std::slice::from_ref(&done), Some(lease.state))
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
                claim_change,
                claim_cursor_advance: None,
                lane: None,
            }));
        }

        Ok(None)
    }

    /// Cancel `job_id`, leaving its unique-claim change in the result for the
    /// caller to apply.
    async fn cancel_job_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_id: i64,
    ) -> Result<Option<CancelJobTxResult>, AwaError> {
        let schema = self.schema();
        self.lock_claim_heads_of_jobs_tx(tx, &[job_id]).await?;
        if let Some(result) = self.cancel_materialized_job_lease_tx(tx, job_id).await? {
            return Ok(Some(result));
        }

        if self.lease_claim_receipts() {
            if let Some(run_lease) = self.open_receipt_run_lease_tx(tx, job_id).await? {
                self.lock_receipt_attempt_keys_tx(tx, &[(job_id, run_lease)])
                    .await?;
                let receipt = self
                    .lock_open_receipt_tx(tx, job_id)
                    .await?
                    .filter(|receipt| receipt.1 == run_lease);
                if receipt.is_some() {
                    if let Some(lease) = self
                        .close_open_receipt_claim_tx(tx, job_id, run_lease, "cancelled")
                        .await?
                    {
                        let done_payload = Self::payload_with_attempt_state(
                            lease.payload.clone(),
                            lease.progress.clone(),
                        )?;
                        let done =
                            lease.into_done_row(JobState::Cancelled, Utc::now(), done_payload);
                        let claim_change = owned_done_claim_change(&done, Some(JobState::Running));
                        self.write_done_rows_tx(
                            tx,
                            std::slice::from_ref(&done),
                            Some(JobState::Running),
                        )
                        .await?;
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
                            claim_change,
                            claim_cursor_advance: None,
                            lane: None,
                        }));
                    }
                }
            }
            if let Some(result) = self.cancel_materialized_job_lease_tx(tx, job_id).await? {
                return Ok(Some(result));
            }
        }

        let claimed = self.ready_row_claimed_sql("ready");
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
                  AND NOT ({claimed})
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
            let claim_change = owned_done_claim_change(&done, Some(JobState::Available));
            self.write_done_rows_tx(tx, std::slice::from_ref(&done), Some(JobState::Available))
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
                evidence: None,
            };
            return Ok(Some(CancelJobTxResult {
                row: done.into_job_row()?,
                claim_change,
                claim_cursor_advance: Some(claim_cursor_advance),
                lane: None,
            }));
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
            // unique. The caller ensures shard 0 of the lane so the lane
            // row exists regardless of producer activity.
            let lane = (deferred.queue.clone(), deferred.priority);
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
            let claim_change = owned_done_claim_change(&done, Some(deferred.state));
            self.write_done_rows_tx(tx, std::slice::from_ref(&done), Some(deferred.state))
                .await?;
            return Ok(Some(CancelJobTxResult {
                row: done.into_job_row()?,
                claim_change,
                claim_cursor_advance: None,
                lane: Some(lane),
            }));
        }

        Ok(None)
    }

    pub async fn cancel_job(&self, pool: &PgPool, job_id: i64) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let result = self.cancel_job_tx(&mut tx, job_id).await?;
        self.apply_cancel_claim_changes_tx(&mut tx, result.as_slice())
            .await?;
        if let Some((queue, priority)) = result.as_ref().and_then(|result| result.lane.as_ref()) {
            self.ensure_lane(&mut tx, queue, *priority, 0).await?;
        }
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

    /// Apply the claim changes of `results` in one key-ordered pass.
    async fn apply_cancel_claim_changes_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        results: &[CancelJobTxResult],
    ) -> Result<(), AwaError> {
        let changes = results
            .iter()
            .filter_map(|result| result.claim_change.as_ref())
            .map(|(key, claim, job_id)| (key.as_slice(), *claim, *job_id))
            .collect();
        self.apply_unique_claim_changes_tx(tx, changes, false)
            .await?;
        Ok(())
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
        let Some(result) = self.cancel_job_tx(tx, job_id).await? else {
            return Ok(None);
        };
        self.apply_cancel_claim_changes_tx(tx, std::slice::from_ref(&result))
            .await?;
        if let Some((queue, priority)) = &result.lane {
            self.ensure_lane(tx, queue, *priority, 0).await?;
        }
        Ok(Some(result.row))
    }

    pub async fn cancel_jobs_by_ids(
        &self,
        pool: &PgPool,
        ids: &[i64],
    ) -> Result<Vec<JobRow>, AwaError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        self.lock_claim_heads_of_jobs_tx(&mut tx, ids).await?;
        if self.lease_claim_receipts() {
            // Completion batches take these locks in one statement, in key
            // order; taking them one job at a time could deadlock with one.
            let mut attempts = Vec::new();
            for job_id in ids {
                if let Some(run_lease) = self.open_receipt_run_lease_tx(&mut tx, *job_id).await? {
                    attempts.push((*job_id, run_lease));
                }
            }
            self.lock_receipt_attempt_keys_tx(&mut tx, &attempts)
                .await?;
        }
        let mut results = Vec::with_capacity(ids.len());
        for job_id in ids {
            results.extend(self.cancel_job_tx(&mut tx, *job_id).await?);
        }
        self.apply_cancel_claim_changes_tx(&mut tx, &results)
            .await?;
        let mut rows = Vec::with_capacity(results.len());
        let mut claim_cursor_advances = Vec::new();
        let mut lanes = BTreeSet::new();
        for result in results {
            if let Some(advance) = result.claim_cursor_advance {
                claim_cursor_advances.push(advance);
            }
            lanes.extend(result.lane);
            rows.push(result.row);
        }
        for (queue, priority) in &lanes {
            self.ensure_lane(&mut tx, queue, *priority, 0).await?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        self.advance_claim_cursors(pool, &claim_cursor_advances)
            .await;
        Ok(rows)
    }

    /// Cancel every available, scheduled, retryable and waiting job in
    /// `queue` and its stripes (`queue#N`), returning how many were cancelled.
    ///
    /// Jobs enqueued after the drain starts are left alone, and so are running
    /// jobs. Waiting jobs go through [`Self::cancel_jobs_by_ids`], then
    /// scheduled, retryable and available jobs are cancelled a chunk at a
    /// time, each chunk in its own transaction, so a large backlog costs a
    /// few statements per chunk rather than per job and holds row locks for
    /// one chunk at a time. Rows another transaction has locked are skipped.
    pub(crate) async fn drain_queue(&self, pool: &PgPool, queue: &str) -> Result<u64, AwaError> {
        self.drain_queue_in_batches(pool, queue, DRAIN_BATCH_SIZE)
            .await
    }

    /// [`Self::drain_queue`] with at most `batch_size` jobs per transaction.
    pub(crate) async fn drain_queue_in_batches(
        &self,
        pool: &PgPool,
        queue: &str,
        batch_size: i64,
    ) -> Result<u64, AwaError> {
        bulk::validate_batch_size(batch_size)?;
        let schema = self.schema();
        let job_id_bound: i64 = sqlx::query_scalar(&format!(
            "SELECT {schema}.sequence_next_value('job_id_seq')"
        ))
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        let scope = BulkCursor {
            before_job_id: job_id_bound,
            ..BulkCursor::default()
        };
        let mut drained = 0u64;
        // Waiting jobs first, then deferred, then ready: the order jobs move
        // between them. A waiting job whose callback times out becomes
        // retryable, and a deferred job is promoted to ready, so a job that
        // moves while the drain runs is still drained.
        let waiting: Vec<i64> = sqlx::query_scalar(&format!(
            r#"
            SELECT job_id
            FROM {schema}.leases
            WHERE {queues}
              AND state = 'waiting_external'
              AND job_id < $2
            ORDER BY job_id
            "#,
            queues = queue_and_stripes_sql("queue", "$1"),
        ))
        .bind(queue)
        .bind(job_id_bound)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;
        for chunk in waiting.chunks(batch_size as usize) {
            drained += self.cancel_jobs_by_ids(pool, chunk).await?.len() as u64;
        }

        let mut after_job_id = i64::MIN;
        loop {
            let mut tx = pool
                .begin_with(DRAIN_TRANSACTION_BEGIN)
                .await
                .map_err(map_sqlx_error)?;
            let (cancelled, last_job_id) = self
                .cancel_deferred_chunk_tx(&mut tx, queue, after_job_id, scope, batch_size)
                .await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            drained += cancelled as u64;
            if (cancelled as i64) < batch_size {
                break;
            }
            after_job_id = last_job_id;
        }

        // Read after the deferred pass, so jobs promoted while it ran are
        // drained from the ready lanes.
        let mut lanes = self.drain_lanes(pool, queue).await?;

        let drained_lanes = lanes.clone();
        while !lanes.is_empty() {
            let mut tx = pool
                .begin_with(DRAIN_TRANSACTION_BEGIN)
                .await
                .map_err(map_sqlx_error)?;
            self.lock_claim_heads_tx(&mut tx, &lanes).await?;
            let cancelled = self
                .cancel_available_chunk_tx(&mut tx, &mut lanes, scope, batch_size)
                .await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            drained += cancelled as u64;
            self.skip_spent_claim_prefixes_after_commit(pool, &lanes)
                .await;
            if (cancelled as i64) < batch_size {
                break;
            }
            lanes.retain(|lane| lane.from_seq < lane.to_seq);
        }
        self.skip_spent_claim_prefixes_after_commit(pool, &drained_lanes)
            .await;

        Ok(drained)
    }

    /// The lanes of `queue` and its stripes that hold ready rows past their
    /// claim cursor, from the claim cursor to the enqueue head.
    async fn drain_lanes(&self, pool: &PgPool, queue: &str) -> Result<Vec<DrainLane>, AwaError> {
        let schema = self.schema();
        let mut lanes: Vec<DrainLane> = sqlx::query_as(&format!(
            r#"
            SELECT
                claims.queue,
                claims.priority,
                claims.enqueue_shard,
                {schema}.sequence_next_value(claims.seq_name) AS from_seq,
                {schema}.sequence_next_value(enqueues.seq_name) AS to_seq
            FROM {schema}.queue_claim_heads AS claims
            JOIN {schema}.queue_enqueue_heads AS enqueues
              ON enqueues.queue = claims.queue
             AND enqueues.priority = claims.priority
             AND enqueues.enqueue_shard = claims.enqueue_shard
            WHERE {queues}
            "#,
            queues = queue_and_stripes_sql("claims.queue", "$1"),
        ))
        .bind(queue)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;
        lanes.retain(|lane| lane.from_seq < lane.to_seq);
        Ok(lanes)
    }

    /// Cancel up to `batch_size` of the waiting, scheduled, retryable and
    /// available jobs of `queue` and its stripes (`queue#N`), resuming at
    /// `cursor`, as one step of a drain run.
    ///
    /// The run takes waiting jobs, then scheduled and retryable jobs, then
    /// available jobs: the order jobs move between them (a waiting job whose
    /// callback times out becomes retryable, and a deferred job is promoted
    /// to ready), so a job that moves during the run is still drained. Jobs
    /// enqueued after the run started are left alone. The run never passes
    /// a scheduled, retryable or available job another transaction holds:
    /// when every remaining one is held, the batch reports `busy`.
    pub async fn drain_queue_batch(
        &self,
        pool: &PgPool,
        queue: &str,
        batch_size: i64,
        cursor: BulkCursor,
    ) -> Result<BulkBatch, AwaError> {
        bulk::validate_batch_size(batch_size)?;
        let schema = self.schema();
        let mut cursor = cursor;
        let mut fence = None;
        if cursor.before_job_id == 0 {
            let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
            fence = self.mark_bulk_run_tx(&mut tx, &mut cursor).await?;
            tx.commit().await.map_err(map_sqlx_error)?;
        }
        let left_above_sequence = self.drain_left_above(pool, fence, queue).await?;
        let next_pass = |cursor: BulkCursor, stage: u8| cursor.next_pass(stage);
        loop {
            let mut batch = BulkBatch::queue_storage(cursor);
            batch.left_above_sequence = left_above_sequence;
            match cursor.stage {
                0 => {
                    let waiting: Vec<i64> = sqlx::query_scalar(&format!(
                        r#"
                        SELECT lease.job_id
                        FROM {schema}.leases AS lease
                        WHERE {queues}
                          AND lease.state = 'waiting_external'
                          AND lease.job_id > $2
                          AND {scope}
                        ORDER BY lease.job_id
                        LIMIT $4
                        "#,
                        queues = queue_and_stripes_sql("lease.queue", "$1"),
                        scope = bulk::run_scope_sql("lease.job_id", "$3", "$5"),
                    ))
                    .bind(queue)
                    .bind(cursor.after_job_id)
                    .bind(cursor.before_job_id)
                    .bind(batch_size)
                    .bind(cursor.sequence_bound)
                    .fetch_all(pool)
                    .await
                    .map_err(map_sqlx_error)?;
                    let Some(last) = waiting.last().copied() else {
                        cursor = next_pass(cursor, 1);
                        continue;
                    };
                    batch.count = self.cancel_jobs_by_ids(pool, &waiting).await?.len() as u64;
                    batch.cursor.after_job_id = last;
                    if (waiting.len() as i64) < batch_size {
                        batch.cursor = next_pass(cursor, 1);
                    }
                    return Ok(batch);
                }
                1 => {
                    let mut tx = pool
                        .begin_with(DRAIN_TRANSACTION_BEGIN)
                        .await
                        .map_err(map_sqlx_error)?;
                    let (cancelled, last) = self
                        .cancel_deferred_chunk_tx(
                            &mut tx,
                            queue,
                            cursor.after_job_id,
                            cursor,
                            batch_size,
                        )
                        .await?;
                    // The lowest matching job left in the range after the
                    // chunk: one this chunk skipped because another
                    // transaction holds it, or one past the chunk's limit.
                    let first_left: Option<i64> = sqlx::query_scalar(&format!(
                        r#"
                        SELECT min(job_id)
                        FROM {schema}.deferred_jobs
                        WHERE {queues}
                          AND state IN ('scheduled', 'retryable')
                          AND job_id > $2
                          AND {scope}
                        "#,
                        queues = queue_and_stripes_sql("queue", "$1"),
                        scope = bulk::run_scope_sql("job_id", "$3", "$4"),
                    ))
                    .bind(queue)
                    .bind(cursor.after_job_id)
                    .bind(cursor.before_job_id)
                    .bind(cursor.sequence_bound)
                    .fetch_one(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;
                    tx.commit().await.map_err(map_sqlx_error)?;
                    batch.count = cancelled as u64;
                    let Some(first_left) = first_left else {
                        if cancelled == 0 {
                            cursor = next_pass(cursor, 2);
                            continue;
                        }
                        batch.cursor = next_pass(cursor, 2);
                        return Ok(batch);
                    };
                    // Never move past a held row.
                    let handled = if cancelled > 0 {
                        last
                    } else {
                        cursor.after_job_id
                    };
                    batch.cursor.after_job_id = handled.min(first_left - 1);
                    batch.busy = cancelled == 0;
                    return Ok(batch);
                }
                _ => {
                    let lanes = self.drain_lanes(pool, queue).await?;
                    if lanes.is_empty() {
                        batch.done = true;
                        return Ok(batch);
                    }
                    let mut tx = pool
                        .begin_with(DRAIN_TRANSACTION_BEGIN)
                        .await
                        .map_err(map_sqlx_error)?;
                    self.lock_claim_heads_tx(&mut tx, &lanes).await?;
                    let mut chunk_lanes = lanes.clone();
                    let cancelled = self
                        .cancel_available_chunk_tx(&mut tx, &mut chunk_lanes, cursor, batch_size)
                        .await?;
                    let remaining = (cancelled as i64) >= batch_size
                        || self
                            .drain_ready_remaining_tx(&mut tx, &lanes, cursor)
                            .await?;
                    tx.commit().await.map_err(map_sqlx_error)?;
                    batch.count = cancelled as u64;
                    self.skip_spent_claim_prefixes_after_commit(pool, &lanes)
                        .await;
                    batch.done = !remaining;
                    batch.busy = remaining && cancelled == 0;
                    return Ok(batch);
                }
            }
        }
    }

    /// Cancel up to `batch_size` scheduled and retryable jobs of
    /// `queue` with ids after `after_job_id` that belong to the run `scope`
    /// bounds, returning how many were cancelled and the largest cancelled
    /// id.
    async fn cancel_deferred_chunk_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        queue: &str,
        after_job_id: i64,
        scope: BulkCursor,
        batch_size: i64,
    ) -> Result<(usize, i64), AwaError> {
        let schema = self.schema();
        let deferred: Vec<DeferredJobRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.deferred_jobs
            WHERE job_id IN (
                SELECT job_id
                FROM {schema}.deferred_jobs
                WHERE {queues}
                  AND state IN ('scheduled', 'retryable')
                  AND job_id > $2
                  AND {scope}
                ORDER BY job_id
                LIMIT $4
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
            "#,
            queues = queue_and_stripes_sql("queue", "$1"),
            scope = bulk::run_scope_sql("job_id", "$3", "$5"),
        ))
        .bind(queue)
        .bind(after_job_id)
        .bind(scope.before_job_id)
        .bind(batch_size)
        .bind(scope.sequence_bound)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(last_job_id) = deferred.iter().map(|row| row.job_id).max() else {
            return Ok((0, after_job_id));
        };

        let cancelled = deferred.len();
        let (ready_slot, ready_generation) = self.current_queue_ring(tx).await?;
        let lanes: BTreeSet<(String, i16)> = deferred
            .iter()
            .map(|row| (row.queue.clone(), row.priority))
            .collect();

        let finalized_at = Utc::now();
        let mut by_state: HashMap<JobState, Vec<DoneJobRow>> = HashMap::new();
        for deferred in deferred {
            // Same shape as a single deferred cancel: shard 0 and a negative
            // lane_seq keep the done_entries key unique.
            let done = DoneJobRow {
                ready_slot,
                ready_generation,
                job_id: deferred.job_id,
                kind: deferred.kind,
                queue: deferred.queue,
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
                finalized_at,
                created_at: deferred.created_at,
                unique_key: deferred.unique_key,
                unique_states: deferred.unique_states,
                payload: deferred.payload,
            };
            by_state.entry(deferred.state).or_default().push(done);
        }
        let changes = by_state
            .iter()
            .flat_map(|(old_state, rows)| done_claim_changes(rows, Some(*old_state)))
            .collect();
        self.apply_unique_claim_changes_tx(tx, changes, false)
            .await?;
        for (old_state, rows) in &by_state {
            self.write_done_rows_tx(tx, rows, Some(*old_state)).await?;
        }
        for (queue, priority) in &lanes {
            self.ensure_lane(tx, queue, *priority, 0).await?;
        }

        Ok((cancelled, last_job_id))
    }

    /// Cancel up to `batch_size` available jobs from `lanes`,
    /// returning how many were cancelled and the claim-cursor advances that
    /// skip them.
    /// The `heads` and `target` CTEs that find a drain's unclaimed ready rows
    /// in `lanes` (bound as `$1` to `$5`), at most `$6` of them, with job IDs
    /// below `$7`. With `lock`, the rows are locked, skipping rows another
    /// transaction holds; without it, held rows are included, so a check
    /// that runs after a locking chunk finds the rows that chunk skipped.
    fn drain_ready_target_ctes(&self, lock: bool) -> String {
        let schema = self.schema();
        let claimed = self.ready_row_claimed_sql("ready");
        let scope = bulk::run_scope_sql("ready.job_id", "$7", "$8");
        let lock = if lock {
            "FOR UPDATE OF ready SKIP LOCKED"
        } else {
            ""
        };
        format!(
            r#"
            heads AS (
                SELECT
                    lanes.queue,
                    lanes.priority,
                    lanes.enqueue_shard,
                    GREATEST(lanes.from_seq, {schema}.sequence_next_value(claims.seq_name)) AS claim_seq,
                    lanes.to_seq AS next_seq
                FROM unnest($1::text[], $2::smallint[], $3::smallint[], $4::bigint[], $5::bigint[])
                    AS lanes(queue, priority, enqueue_shard, from_seq, to_seq)
                JOIN {schema}.queue_claim_heads AS claims
                  ON claims.queue = lanes.queue
                 AND claims.priority = lanes.priority
                 AND claims.enqueue_shard = lanes.enqueue_shard
            ),
            target AS (
                SELECT ready.*
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
                      AND {scope}
                      AND NOT (
                          EXISTS (
                              SELECT 1 FROM {schema}.ready_tombstones AS tomb
                              WHERE tomb.ready_slot = ready.ready_slot
                                AND tomb.ready_generation = ready.ready_generation
                                AND tomb.queue = ready.queue
                                AND tomb.priority = ready.priority
                                AND tomb.enqueue_shard = ready.enqueue_shard
                                AND tomb.lane_seq = ready.lane_seq
                          )
                          OR {claimed}
                      )
                    ORDER BY ready.lane_seq
                    {lock}
                ) AS ready
                LIMIT $6
            )
            "#
        )
    }

    /// Whether any unclaimed ready row of `lanes` that belongs to the run
    /// `scope` bounds remains, counting rows another transaction holds.
    async fn drain_ready_remaining_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        lanes: &[DrainLane],
        scope: BulkCursor,
    ) -> Result<bool, AwaError> {
        sqlx::query_scalar(&format!(
            "WITH {} SELECT EXISTS (SELECT 1 FROM target)",
            self.drain_ready_target_ctes(false)
        ))
        .bind(
            lanes
                .iter()
                .map(|lane| lane.queue.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(lanes.iter().map(|lane| lane.priority).collect::<Vec<_>>())
        .bind(
            lanes
                .iter()
                .map(|lane| lane.enqueue_shard)
                .collect::<Vec<_>>(),
        )
        .bind(lanes.iter().map(|lane| lane.from_seq).collect::<Vec<_>>())
        .bind(lanes.iter().map(|lane| lane.to_seq).collect::<Vec<_>>())
        .bind(1_i64)
        .bind(scope.before_job_id)
        .bind(scope.sequence_bound)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)
    }

    async fn cancel_available_chunk_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        lanes: &mut [DrainLane],
        scope: BulkCursor,
        batch_size: i64,
    ) -> Result<usize, AwaError> {
        let schema = self.schema();
        // Unclaimed rows are found the way a claim finds them: from each
        // lane's claim cursor through its segments, then along the ready key
        // within each segment's ring slot. The ready key leads with the ring
        // slot, so a lane range without it scans every retained ready row.
        // Rows are locked before the limit applies, so a row another
        // transaction holds does not shorten the chunk. The tombstone check
        // shares one NOT with the claim check: on its own, NOT EXISTS is
        // planned as an anti join that reads the lane's tombstones again for
        // every segment, and a lane enqueued one job at a time has a segment
        // per row. Only rows whose
        // tombstone this statement inserts are cancelled, so a concurrent
        // cancel of the same row cannot produce a second done row.
        let ready: Vec<ReadyTransitionRow> = sqlx::query_as(&format!(
            r#"
            WITH {ctes},
            tombstone AS (
                INSERT INTO {schema}.ready_tombstones (
                    ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
                )
                SELECT ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
                FROM target
                ON CONFLICT DO NOTHING
                RETURNING ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq
            )
            SELECT
                target.ready_slot,
                target.ready_generation,
                target.job_id,
                target.kind,
                target.queue,
                target.args,
                target.priority,
                target.attempt,
                target.run_lease,
                target.max_attempts,
                target.lane_seq,
                target.enqueue_shard,
                target.run_at,
                target.attempted_at,
                target.created_at,
                target.unique_key,
                target.unique_states,
                COALESCE(target.payload, '{{}}'::jsonb) AS payload
            FROM target
            JOIN tombstone
              ON tombstone.ready_slot = target.ready_slot
             AND tombstone.ready_generation = target.ready_generation
             AND tombstone.queue = target.queue
             AND tombstone.priority = target.priority
             AND tombstone.enqueue_shard = target.enqueue_shard
             AND tombstone.lane_seq = target.lane_seq
            "#,
            ctes = self.drain_ready_target_ctes(true),
        ))
        .bind(lanes.iter().map(|lane| lane.queue.as_str()).collect::<Vec<_>>())
        .bind(lanes.iter().map(|lane| lane.priority).collect::<Vec<_>>())
        .bind(lanes.iter().map(|lane| lane.enqueue_shard).collect::<Vec<_>>())
        .bind(lanes.iter().map(|lane| lane.from_seq).collect::<Vec<_>>())
        .bind(lanes.iter().map(|lane| lane.to_seq).collect::<Vec<_>>())
        .bind(batch_size)
        .bind(scope.before_job_id)
        .bind(scope.sequence_bound)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if ready.is_empty() {
            return Ok(0);
        }

        // The next chunk resumes after the rows this one cancelled, so each
        // lane is read once even where a claimed or locked row stays behind.
        for row in &ready {
            if let Some(lane) = lanes.iter_mut().find(|lane| {
                lane.queue == row.queue
                    && lane.priority == row.priority
                    && lane.enqueue_shard == row.enqueue_shard
            }) {
                lane.from_seq = lane.from_seq.max(row.lane_seq + 1);
            }
        }

        let finalized_at = Utc::now();
        let done_rows: Vec<DoneJobRow> = ready
            .into_iter()
            .map(|row| {
                let payload = row.payload.clone();
                row.into_done_row(JobState::Cancelled, finalized_at, payload)
            })
            .collect();
        self.insert_done_rows_tx(tx, &done_rows, Some(JobState::Available))
            .await?;

        Ok(done_rows.len())
    }

    /// Lock the claim heads of the lanes holding ready rows of `job_ids`, so
    /// the transaction's next statement sees any claim of those rows that was
    /// in progress. See [`Self::lock_claim_heads_tx`].
    async fn lock_claim_heads_of_jobs_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_ids: &[i64],
    ) -> Result<(), AwaError> {
        self.lock_claim_heads_of_jobs_with_wait_tx(tx, job_ids, false)
            .await
    }

    async fn lock_claim_heads_of_jobs_with_wait_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        job_ids: &[i64],
        nowait: bool,
    ) -> Result<(), AwaError> {
        let schema = self.schema();
        let wait_clause = if nowait { "NOWAIT" } else { "" };
        sqlx::query(&format!(
            r#"
            SELECT 1
            FROM {schema}.queue_claim_heads AS claims
            WHERE (claims.queue, claims.priority, claims.enqueue_shard) IN (
                SELECT ready.queue, ready.priority, ready.enqueue_shard
                FROM {schema}.ready_entries AS ready
                WHERE ready.job_id = ANY($1)
            )
            ORDER BY claims.queue COLLATE "C", claims.priority, claims.enqueue_shard
            FOR UPDATE OF claims {wait_clause}
            "#
        ))
        .bind(job_ids)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    /// Lock the claim heads of `lanes`. A claim holds its lane's head until
    /// it commits, so once this returns the transaction's next statement sees
    /// every claim on these lanes. Heads are locked in the byte order that
    /// cursor advances use.
    async fn lock_claim_heads_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        lanes: &[DrainLane],
    ) -> Result<(), AwaError> {
        let schema = self.schema();
        sqlx::query(&format!(
            r#"
            SELECT 1
            FROM {schema}.queue_claim_heads AS claims
            JOIN unnest($1::text[], $2::smallint[], $3::smallint[])
                AS lanes(queue, priority, enqueue_shard)
              ON claims.queue = lanes.queue
             AND claims.priority = lanes.priority
             AND claims.enqueue_shard = lanes.enqueue_shard
            ORDER BY claims.queue COLLATE "C", claims.priority, claims.enqueue_shard
            FOR UPDATE OF claims
            "#
        ))
        .bind(
            lanes
                .iter()
                .map(|lane| lane.queue.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(lanes.iter().map(|lane| lane.priority).collect::<Vec<_>>())
        .bind(
            lanes
                .iter()
                .map(|lane| lane.enqueue_shard)
                .collect::<Vec<_>>(),
        )
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    /// Move each lane's claim cursor over cancelled and claimed rows, up to
    /// the lane's `to_seq`. A claim crosses cancelled rows only one claim
    /// batch per poll, so without this the jobs behind a drained range wait
    /// for many polls before a worker reaches them.
    async fn skip_spent_claim_prefixes(
        &self,
        pool: &PgPool,
        lanes: &[DrainLane],
    ) -> Result<(), AwaError> {
        let claimed = self.ready_row_claimed_sql("ready");
        let sql = self.spent_claim_prefix_advance_sql(&claimed);
        let mut lanes = lanes.to_vec();
        while !lanes.is_empty() {
            let mut tx = pool
                .begin_with(DRAIN_TRANSACTION_BEGIN)
                .await
                .map_err(map_sqlx_error)?;
            self.lock_claim_heads_tx(&mut tx, &lanes).await?;
            let advanced = Self::advance_spent_claim_prefixes_tx(&mut tx, &sql, &lanes).await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            lanes.retain(|lane| {
                advanced.iter().any(|advance| {
                    advance.unfinished
                        && lane.queue == advance.queue
                        && lane.priority == advance.priority
                        && lane.enqueue_shard == advance.enqueue_shard
                })
            });
        }
        Ok(())
    }

    /// [`Self::skip_spent_claim_prefixes`] after the cancellations of
    /// `lanes` have committed. Moving the cursors only saves claims from
    /// crossing rows that are already spent, so a failure is logged, not
    /// returned: the caller must still see the committed count. The next
    /// drain batch moves the cursors over the same rows again, and a claim
    /// crosses them in any case.
    async fn skip_spent_claim_prefixes_after_commit(&self, pool: &PgPool, lanes: &[DrainLane]) {
        if let Err(err) = self.skip_spent_claim_prefixes(pool, lanes).await {
            tracing::warn!(
                error = ?err,
                lanes = lanes.len(),
                "failed to move queue-storage claim cursors over drained rows after the drain committed"
            );
        }
    }

    /// The statement behind [`Self::skip_spent_claim_prefixes`] and
    /// [`Self::advance_spent_claim_heads`], for lanes whose claim heads the
    /// transaction holds. `claimed` is the SQL predicate, over the ready row
    /// `ready`, that the row's attempt was already claimed.
    ///
    /// Each window is a range of positions, so a live row inside it is
    /// always seen. Untombstoned rows are checked for a claim only from the
    /// first, a few at a time: claims past the cursor are the few whose
    /// claimer has not yet moved it. The tombstone check is a column behind
    /// OFFSET 0, so it is not pushed down as a NOT EXISTS, for the reason
    /// given in `cancel_available_chunk_tx`. A gap before a visible row is an
    /// enqueue that rolled back: enqueues on a lane commit in position order.
    /// So each window starts at the lane's first visible row at or after the
    /// cursor, however many rolled-back positions lie before it, and a lane
    /// with no visible row there does not move. A lane's segments cover
    /// disjoint position ranges, so that row is the first one in the first
    /// segment, by `next_lane_seq`, that has a row at or after the cursor.
    /// The cursor moves to the first unclaimed checked row, else one past the
    /// last checked row when the check limit was reached, else one past the
    /// last visible row, and never moves back.
    fn spent_claim_prefix_advance_sql(&self, claimed: &str) -> String {
        let schema = self.schema();
        format!(
            r#"
                WITH cursors AS (
                    SELECT
                        lanes.queue,
                        lanes.priority,
                        lanes.enqueue_shard,
                        claims.seq_name,
                        {schema}.sequence_next_value(claims.seq_name) AS claim_seq,
                        lanes.to_seq
                    FROM unnest($1::text[], $2::smallint[], $3::smallint[], $4::bigint[])
                        AS lanes(queue, priority, enqueue_shard, to_seq)
                    JOIN {schema}.queue_claim_heads AS claims
                      ON claims.queue = lanes.queue
                     AND claims.priority = lanes.priority
                     AND claims.enqueue_shard = lanes.enqueue_shard
                ),
                windows AS (
                    SELECT
                        cursors.*,
                        LEAST(first_visible.lane_seq + $5, cursors.to_seq) AS window_end
                    FROM cursors
                    LEFT JOIN LATERAL (
                        SELECT head.lane_seq
                        FROM {schema}.ready_segments AS segment
                        CROSS JOIN LATERAL (
                            SELECT ready.lane_seq
                            FROM {schema}.ready_entries AS ready
                            WHERE ready.ready_slot = segment.ready_slot
                              AND ready.ready_generation = segment.ready_generation
                              AND ready.queue = cursors.queue
                              AND ready.priority = cursors.priority
                              AND ready.enqueue_shard = cursors.enqueue_shard
                              AND ready.lane_seq >= GREATEST(segment.first_lane_seq, cursors.claim_seq)
                              AND ready.lane_seq < segment.next_lane_seq
                            ORDER BY ready.lane_seq
                            LIMIT 1
                        ) AS head
                        WHERE segment.queue = cursors.queue
                          AND segment.priority = cursors.priority
                          AND segment.enqueue_shard = cursors.enqueue_shard
                          AND segment.next_lane_seq > cursors.claim_seq
                        ORDER BY segment.next_lane_seq
                        LIMIT 1
                    ) AS first_visible ON TRUE
                    WHERE first_visible.lane_seq IS NOT NULL
                ),
                scanned AS (
                    SELECT
                        windows.*,
                        unclaimed.first_live,
                        COALESCE(
                            unclaimed.first_live,
                            CASE
                                WHEN unclaimed.checked = $6 THEN unclaimed.last_checked + 1
                                ELSE visible.last_seen + 1
                            END
                        ) AS next_seq
                    FROM windows
                    CROSS JOIN LATERAL (
                        SELECT max(ready.lane_seq) AS last_seen
                        FROM {schema}.ready_segments AS segment
                        CROSS JOIN LATERAL (
                            SELECT ready.lane_seq
                            FROM {schema}.ready_entries AS ready
                            WHERE ready.ready_slot = segment.ready_slot
                              AND ready.ready_generation = segment.ready_generation
                              AND ready.queue = windows.queue
                              AND ready.priority = windows.priority
                              AND ready.enqueue_shard = windows.enqueue_shard
                              AND ready.lane_seq >= GREATEST(segment.first_lane_seq, windows.claim_seq)
                              AND ready.lane_seq < LEAST(segment.next_lane_seq, windows.window_end)
                            ORDER BY ready.lane_seq
                        ) AS ready
                        WHERE segment.queue = windows.queue
                          AND segment.priority = windows.priority
                          AND segment.enqueue_shard = windows.enqueue_shard
                          AND segment.next_lane_seq > windows.claim_seq
                          AND segment.first_lane_seq < windows.window_end
                    ) AS visible
                    CROSS JOIN LATERAL (
                        SELECT
                            min(ready.lane_seq) FILTER (WHERE NOT ({claimed})) AS first_live,
                            max(ready.lane_seq) AS last_checked,
                            count(*) AS checked
                        FROM (
                            SELECT ready.*
                            FROM {schema}.ready_segments AS segment
                            CROSS JOIN LATERAL (
                                SELECT
                                    ready.*,
                                    EXISTS (
                                        SELECT 1 FROM {schema}.ready_tombstones AS tomb
                                        WHERE tomb.ready_slot = ready.ready_slot
                                          AND tomb.ready_generation = ready.ready_generation
                                          AND tomb.queue = ready.queue
                                          AND tomb.priority = ready.priority
                                          AND tomb.enqueue_shard = ready.enqueue_shard
                                          AND tomb.lane_seq = ready.lane_seq
                                    ) AS tombstoned
                                FROM {schema}.ready_entries AS ready
                                WHERE ready.ready_slot = segment.ready_slot
                                  AND ready.ready_generation = segment.ready_generation
                                  AND ready.queue = windows.queue
                                  AND ready.priority = windows.priority
                                  AND ready.enqueue_shard = windows.enqueue_shard
                                  AND ready.lane_seq >= GREATEST(segment.first_lane_seq, windows.claim_seq)
                                  AND ready.lane_seq < LEAST(segment.next_lane_seq, windows.window_end)
                                ORDER BY ready.lane_seq
                                OFFSET 0
                            ) AS ready
                            WHERE segment.queue = windows.queue
                              AND segment.priority = windows.priority
                              AND segment.enqueue_shard = windows.enqueue_shard
                              AND segment.next_lane_seq > windows.claim_seq
                              AND segment.first_lane_seq < windows.window_end
                              AND NOT ready.tombstoned
                            ORDER BY ready.lane_seq
                            LIMIT $6
                        ) AS ready
                    ) AS unclaimed
                    WHERE windows.claim_seq < windows.to_seq
                ),
                advanced AS MATERIALIZED (
                    SELECT
                        scanned.*,
                        {schema}.set_sequence_next(scanned.seq_name, scanned.next_seq) AS moved
                    FROM scanned
                    WHERE scanned.next_seq > scanned.claim_seq
                )
                SELECT queue, priority, enqueue_shard, claim_seq, next_seq,
                       first_live IS NULL AND next_seq < to_seq AS unfinished
                FROM advanced
                "#
        )
    }

    /// Run [`Self::spent_claim_prefix_advance_sql`] for `lanes`, binding
    /// [`DRAIN_BATCH_SIZE`] as the window and [`DRAIN_CLAIM_CHECK_ROWS`] as
    /// the claim-check limit. Returns the lanes whose cursor moved.
    async fn advance_spent_claim_prefixes_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        sql: &str,
        lanes: &[DrainLane],
    ) -> Result<Vec<SpentPrefixAdvance>, AwaError> {
        sqlx::query_as(sql)
            .bind(
                lanes
                    .iter()
                    .map(|lane| lane.queue.as_str())
                    .collect::<Vec<_>>(),
            )
            .bind(lanes.iter().map(|lane| lane.priority).collect::<Vec<_>>())
            .bind(
                lanes
                    .iter()
                    .map(|lane| lane.enqueue_shard)
                    .collect::<Vec<_>>(),
            )
            .bind(lanes.iter().map(|lane| lane.to_seq).collect::<Vec<_>>())
            .bind(DRAIN_BATCH_SIZE)
            .bind(DRAIN_CLAIM_CHECK_ROWS)
            .fetch_all(tx.as_mut())
            .await
            .map_err(map_sqlx_error)
    }

    /// Move the claim cursors of `lanes` over the spent rows at their heads,
    /// outside any claim, so a lane whose head rows were cancelled, moved,
    /// reprioritised, or claimed without the cursor following, stops keeping
    /// queue prune from reclaiming the slot that holds them (#779).
    ///
    /// Only a lane whose head row, read without a row lock, is tombstoned or
    /// claimed is advanced, so a lane that is merely behind is left alone.
    /// Each such lane is advanced in its own transactions, at most
    /// `max_windows` windows of [`DRAIN_BATCH_SIZE`] positions per call. A
    /// transaction takes the lane's claim head with `SKIP LOCKED` and the
    /// queue-slot tables it reads with `NOWAIT`, in queue prune's lock order,
    /// under a short lock timeout: a lane a claim, cancel or move holds, or a
    /// table prune holds, is skipped for this call instead of waited for.
    ///
    /// The cursor moves only over committed tombstones and committed claim
    /// evidence of the kind the claim path trusts in this store's mode, and
    /// never past a row that is or may become claimable: see
    /// `correctness/storage/AwaMaintenanceCursorAdvance.tla`. Returns the
    /// lanes whose cursor moved.
    #[tracing::instrument(skip_all, name = "queue_storage.advance_spent_claim_heads")]
    pub async fn advance_spent_claim_heads(
        &self,
        pool: &PgPool,
        lanes: &[PendingReadyLane],
        max_windows: usize,
    ) -> Result<SpentClaimHeadAdvances, AwaError> {
        let mut outcome = SpentClaimHeadAdvances::default();
        if lanes.is_empty() || max_windows == 0 {
            return Ok(outcome);
        }
        let schema = self.schema();
        let mut probe = pool
            .begin_with(DRAIN_TRANSACTION_BEGIN)
            .await
            .map_err(map_sqlx_error)?;
        set_lock_timeout_tx(&mut probe, CLAIM_CURSOR_ADVANCE_LOCK_TIMEOUT).await?;
        let receipts = self.schema_uses_lease_claim_receipts_tx(&mut probe).await?;
        let claimed = self.ready_row_claimed_for_mode_sql("ready", receipts);
        let candidates: Result<Vec<(String, i16, i16)>, sqlx::Error> = sqlx::query_as(&format!(
            r#"
            WITH heads AS MATERIALIZED (
                SELECT
                    lanes.queue,
                    lanes.priority,
                    lanes.enqueue_shard,
                    {schema}.sequence_next_value(claims.seq_name) AS claim_seq
                FROM unnest($1::text[], $2::smallint[], $3::smallint[])
                    AS lanes(queue, priority, enqueue_shard)
                JOIN {schema}.queue_claim_heads AS claims
                  ON claims.queue = lanes.queue
                 AND claims.priority = lanes.priority
                 AND claims.enqueue_shard = lanes.enqueue_shard
            )
            SELECT heads.queue, heads.priority, heads.enqueue_shard
            FROM heads
            CROSS JOIN LATERAL (
                SELECT head.*
                FROM {schema}.ready_segments AS segment
                CROSS JOIN LATERAL (
                    SELECT
                        entry.ready_slot,
                        entry.ready_generation,
                        entry.queue,
                        entry.priority,
                        entry.enqueue_shard,
                        entry.lane_seq,
                        entry.job_id,
                        entry.run_lease
                    FROM {schema}.ready_entries AS entry
                    WHERE entry.ready_slot = segment.ready_slot
                      AND entry.ready_generation = segment.ready_generation
                      AND entry.queue = heads.queue
                      AND entry.priority = heads.priority
                      AND entry.enqueue_shard = heads.enqueue_shard
                      AND entry.lane_seq >= GREATEST(segment.first_lane_seq, heads.claim_seq)
                      AND entry.lane_seq < segment.next_lane_seq
                    ORDER BY entry.lane_seq
                    LIMIT 1
                ) AS head
                WHERE segment.queue = heads.queue
                  AND segment.priority = heads.priority
                  AND segment.enqueue_shard = heads.enqueue_shard
                  AND segment.next_lane_seq > heads.claim_seq
                ORDER BY segment.next_lane_seq
                LIMIT 1
            ) AS ready
            WHERE EXISTS (
                    SELECT 1 FROM {schema}.ready_tombstones AS tomb
                    WHERE tomb.ready_slot = ready.ready_slot
                      AND tomb.ready_generation = ready.ready_generation
                      AND tomb.queue = ready.queue
                      AND tomb.priority = ready.priority
                      AND tomb.enqueue_shard = ready.enqueue_shard
                      AND tomb.lane_seq = ready.lane_seq
                )
               OR {claimed}
            ORDER BY heads.queue COLLATE "C", heads.priority, heads.enqueue_shard
            "#
        ))
        .bind(
            lanes
                .iter()
                .map(|lane| lane.queue.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(lanes.iter().map(|lane| lane.priority).collect::<Vec<_>>())
        .bind(
            lanes
                .iter()
                .map(|lane| lane.enqueue_shard)
                .collect::<Vec<_>>(),
        )
        .fetch_all(probe.as_mut())
        .await;
        let candidates = match candidates {
            Ok(candidates) => candidates,
            Err(err) if is_prune_contention_error(&err) => {
                tracing::debug!(
                    error = %err,
                    "Skipped claim-cursor advances behind a locked queue-storage table"
                );
                return Ok(outcome);
            }
            Err(err) => return Err(map_sqlx_error(err)),
        };
        probe.commit().await.map_err(map_sqlx_error)?;

        let sql = self.spent_claim_prefix_advance_sql(&claimed);
        for (queue, priority, enqueue_shard) in candidates {
            let mut moved: Option<SpentClaimHeadAdvance> = None;
            for _ in 0..max_windows {
                let step = self
                    .advance_spent_claim_head_window(
                        pool,
                        &sql,
                        receipts,
                        &queue,
                        priority,
                        enqueue_shard,
                    )
                    .await;
                let step = match step {
                    Ok(step) => step,
                    Err(AwaError::Database(err)) if is_prune_contention_error(&err) => {
                        tracing::debug!(
                            queue = %queue,
                            priority,
                            enqueue_shard,
                            error = %err,
                            "Skipped a claim-cursor advance on a locked queue-storage table"
                        );
                        break;
                    }
                    Err(AwaError::Database(err)) if is_connection_error(&err) => {
                        return Err(AwaError::Database(err));
                    }
                    Err(err) => {
                        outcome.failed.push(SpentClaimHeadFailure {
                            queue: queue.clone(),
                            priority,
                            enqueue_shard,
                            error: err.to_string(),
                        });
                        break;
                    }
                };
                let Some(step) = step else {
                    break;
                };
                let unfinished = step.unfinished;
                match moved.as_mut() {
                    Some(lane) => lane.to_seq = step.next_seq,
                    None => {
                        moved = Some(SpentClaimHeadAdvance {
                            queue: queue.clone(),
                            priority,
                            enqueue_shard,
                            from_seq: step.claim_seq,
                            to_seq: step.next_seq,
                        })
                    }
                }
                if !unfinished {
                    break;
                }
            }
            outcome.advanced.extend(moved);
        }
        Ok(outcome)
    }

    /// Whether this store's schema claims with lease-claim receipts, read from
    /// its installed claim function the way the schema migrations read it.
    /// Falls back to this store's configuration when the function is missing.
    async fn schema_uses_lease_claim_receipts_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
    ) -> Result<bool, AwaError> {
        let receipts: Option<bool> = sqlx::query_scalar(
            r#"
            SELECT $1 = 'awa'
                OR position(
                    format('INSERT INTO %I.lease_claims', $1::text)
                    IN pg_get_functiondef(to_regprocedure(format(
                        '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
                        $1::text
                    )))
                ) > 0
            "#,
        )
        .bind(self.schema())
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(receipts.unwrap_or_else(|| self.lease_claim_receipts()))
    }

    /// One window of [`Self::advance_spent_claim_heads`] for one lane, in its
    /// own transaction. `None` when the lane's head is held by another
    /// transaction or the cursor did not move.
    async fn advance_spent_claim_head_window(
        &self,
        pool: &PgPool,
        sql: &str,
        receipts: bool,
        queue: &str,
        priority: i16,
        enqueue_shard: i16,
    ) -> Result<Option<SpentPrefixAdvance>, AwaError> {
        let schema = self.schema();
        let mut tx = pool
            .begin_with(DRAIN_TRANSACTION_BEGIN)
            .await
            .map_err(map_sqlx_error)?;
        set_lock_timeout_tx(&mut tx, CLAIM_CURSOR_ADVANCE_LOCK_TIMEOUT).await?;
        let locked: Option<i32> = sqlx::query_scalar(&format!(
            r#"
            SELECT 1
            FROM {schema}.queue_claim_heads
            WHERE queue = $1 AND priority = $2 AND enqueue_shard = $3
            FOR UPDATE SKIP LOCKED
            "#
        ))
        .bind(queue)
        .bind(priority)
        .bind(enqueue_shard)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        if locked.is_none() {
            tx.rollback().await.map_err(map_sqlx_error)?;
            return Ok(None);
        }
        // The tables the advance statement reads in the schema's claim mode,
        // locked up front without waiting: a table another transaction holds
        // exclusively ends this window instead of queueing behind it.
        let tables: &[&str] = if receipts {
            &[
                "ready_segments",
                "ready_entries",
                "ready_claim_attempt_batches",
                "ready_tombstones",
            ]
        } else {
            &[
                "leases",
                "ready_segments",
                "done_entries",
                "ready_entries",
                "ready_claim_attempt_batches",
                "ready_tombstones",
            ]
        };
        let tables = tables
            .iter()
            .map(|table| format!("{schema}.{table}"))
            .collect::<Vec<_>>()
            .join(", ");
        sqlx::query(&format!("LOCK TABLE {tables} IN ACCESS SHARE MODE NOWAIT"))
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        let to_seq: i64 = sqlx::query_scalar(&format!(
            r#"
            SELECT {schema}.sequence_next_value(seq_name)
            FROM {schema}.queue_enqueue_heads
            WHERE queue = $1 AND priority = $2 AND enqueue_shard = $3
            "#
        ))
        .bind(queue)
        .bind(priority)
        .bind(enqueue_shard)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        let lane = DrainLane {
            queue: queue.to_string(),
            priority,
            enqueue_shard,
            from_seq: 0,
            to_seq,
        };
        let advance =
            Self::advance_spent_claim_prefixes_tx(&mut tx, sql, std::slice::from_ref(&lane))
                .await?
                .into_iter()
                .next();
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(advance)
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

        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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

        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
            // Stored rows do not retain the ordering key.
            self.queue_stripe_for_enqueue(
                requested_queue,
                None,
                row.unique_key.as_deref(),
                Some(row.job_id),
            )
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
        self.lock_claim_heads_of_jobs_tx(tx, &[job_id]).await?;
        let claimed = self.ready_row_claimed_sql("ready");
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
                  AND NOT ({claimed})
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
            // Stored rows do not retain the ordering key.
            self.queue_stripe_for_enqueue(
                requested_queue,
                None,
                ready.unique_key.as_deref(),
                Some(ready.job_id),
            )
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let claimed = self.ready_row_claimed_sql("ready");

        // Claims commit before their cursor advance. Select only a bounded
        // set of identities before taking heads, without locking ready rows in
        // the reverse order to a claimer. A later statement rechecks these same
        // identities after every in-flight claim on their lanes has committed.
        let candidates: Vec<ReadyRowIdentity> = sqlx::query_as(&format!(
            r#"
            SELECT ready.ready_slot, ready.ready_generation, ready.queue,
                   ready.priority, ready.enqueue_shard, ready.lane_seq,
                   ready.job_id, ready.run_lease
            FROM {schema}.ready_entries AS ready
            JOIN {schema}.queue_claim_heads AS claims
              ON claims.queue = ready.queue
             AND claims.priority = ready.priority
             AND claims.enqueue_shard = ready.enqueue_shard
            WHERE ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
              AND ready.priority > 1
              AND ready.run_at <= $1
              AND NOT ({claimed})
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
            "#
        ))
        .bind(cutoff)
        .bind(limit)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        if candidates.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }
        let job_ids: Vec<i64> = candidates.iter().map(|row| row.job_id).collect();
        self.lock_claim_heads_of_jobs_tx(&mut tx, &job_ids).await?;

        let moved: Vec<ReadyTransitionRow> = sqlx::query_as(&format!(
            r#"
            WITH candidates(ready_slot, ready_generation, queue, priority,
                            enqueue_shard, lane_seq, job_id, run_lease) AS (
                SELECT * FROM unnest($3::int[], $4::bigint[], $5::text[], $6::smallint[],
                                    $7::smallint[], $8::bigint[], $9::bigint[], $10::bigint[])
            ), target AS (
                SELECT ready.*
                FROM candidates
                JOIN {schema}.ready_entries AS ready
                  ON ready.ready_slot = candidates.ready_slot
                 AND ready.ready_generation = candidates.ready_generation
                 AND ready.queue = candidates.queue
                 AND ready.priority = candidates.priority
                 AND ready.enqueue_shard = candidates.enqueue_shard
                 AND ready.lane_seq = candidates.lane_seq
                 AND ready.job_id = candidates.job_id
                 AND ready.run_lease = candidates.run_lease
                JOIN {schema}.queue_claim_heads AS claims
                  ON claims.queue = ready.queue
                 AND claims.priority = ready.priority
                 AND claims.enqueue_shard = ready.enqueue_shard
                WHERE ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
                  AND ready.priority > 1
                  AND ready.run_at <= $1
                  AND NOT ({claimed})
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
        .bind(candidates.iter().map(|row| row.ready_slot).collect::<Vec<_>>())
        .bind(candidates.iter().map(|row| row.ready_generation).collect::<Vec<_>>())
        .bind(candidates.iter().map(|row| row.queue.clone()).collect::<Vec<_>>())
        .bind(candidates.iter().map(|row| row.priority).collect::<Vec<_>>())
        .bind(candidates.iter().map(|row| row.enqueue_shard).collect::<Vec<_>>())
        .bind(candidates.iter().map(|row| row.lane_seq).collect::<Vec<_>>())
        .bind(&job_ids)
        .bind(candidates.iter().map(|row| row.run_lease).collect::<Vec<_>>())
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
        tx.commit().await.map_err(map_sqlx_error)?;
        self.wake_queues_after_commit(pool, queues);
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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

    /// Wake the claimers of `queues` once the caller's transaction has
    /// committed, coalesced per pool (see [`crate::wakeup`]). Paths that
    /// commit their own transaction use this; the public `*_tx` methods,
    /// whose caller commits, notify inside the transaction instead.
    fn wake_queues_after_commit(&self, pool: &PgPool, queues: impl IntoIterator<Item = String>) {
        crate::wakeup::wake_after_commit(
            pool,
            queues
                .into_iter()
                .map(|queue| format!("awa:{}", self.logical_queue_name(&queue))),
        );
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

    /// Take the attempts' completion locks, then lock their row-local claims.
    ///
    /// The statements that follow lock these claims again. A statement that
    /// locks a claim another transaction has just rewritten rechecks it, and
    /// PostgreSQL 18 frees that statement's run-time partition pruning state
    /// once the lock runs out of rows, so a later pruning pass in it reads
    /// freed memory. Locked here, any recheck happens in a statement that
    /// prunes nothing, and the later statements find the claims already held.
    async fn lock_receipt_attempts_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        jobs: &[(i64, i64)],
    ) -> Result<(), AwaError> {
        if jobs.is_empty() {
            return Ok(());
        }

        self.lock_receipt_attempt_keys_tx(tx, jobs).await?;

        let unique_jobs: BTreeSet<(i64, i64)> = jobs.iter().copied().collect();
        let job_ids: Vec<i64> = unique_jobs.iter().map(|(job_id, _)| *job_id).collect();
        let run_leases: Vec<i64> = unique_jobs
            .iter()
            .map(|(_, run_lease)| *run_lease)
            .collect();
        let schema = self.schema();
        sqlx::query(&format!(
            r#"
            SELECT 1
            FROM {schema}.lease_claims AS claims
            JOIN unnest($1::bigint[], $2::bigint[]) AS input(job_id, run_lease)
              ON input.job_id = claims.job_id
             AND input.run_lease = claims.run_lease
            ORDER BY claims.job_id, claims.run_lease
            FOR UPDATE OF claims
            "#
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    /// Serialize the receipt paths that complete or close the same attempts.
    async fn lock_receipt_attempt_keys_tx<'a>(
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

    /// Take the closers' attempt locks without waiting, returning the attempts
    /// locked. A heartbeat must not queue behind a closer: an attempt whose lock
    /// is held is being finalized or rescued right now, and rescue skips it for
    /// as long as the lock is held. Taking the locks in a statement of their own
    /// means the statement that then decides what is still open starts after
    /// every closer that held a lock first has committed (#614), so a heartbeat
    /// cannot recreate state for an attempt that has just been closed.
    async fn try_lock_receipt_attempt_keys_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        jobs: &[(i64, i64)],
    ) -> Result<Vec<(i64, i64)>, AwaError> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        let job_ids: Vec<i64> = jobs.iter().map(|(job_id, _)| *job_id).collect();
        let run_leases: Vec<i64> = jobs.iter().map(|(_, run_lease)| *run_lease).collect();
        sqlx::query_as::<_, (i64, i64)>(
            r#"
            WITH attempts AS MATERIALIZED (
                SELECT DISTINCT
                    input.job_id,
                    input.run_lease,
                    pg_catalog.hashtextextended(
                        format('awa.receipt.complete:%s:%s', input.job_id, input.run_lease),
                        0
                    ) AS lock_key
                FROM unnest($1::bigint[], $2::bigint[]) AS input(job_id, run_lease)
            )
            SELECT job_id, run_lease
            FROM attempts
            WHERE pg_catalog.pg_try_advisory_xact_lock(lock_key)
            "#,
        )
        .bind(&job_ids)
        .bind(&run_leases)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)
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
            lease_ring AS (
                SELECT current_slot AS lease_slot, generation AS lease_generation
                FROM {schema}.lease_ring_state
                WHERE singleton = TRUE
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
                WHERE claim_batches.job_ids && $1::bigint[]
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
                WHERE claim_batches.job_ids && $1::bigint[]
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

    /// With `skip_locked`, attempts whose lock another transaction holds are
    /// left out, as a heartbeat batch wants. Without it the call waits for
    /// each attempt's lock, so an explicit flush is never dropped.
    async fn upsert_attempt_state_progress_from_receipts_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        jobs: &[(i64, i64, serde_json::Value)],
        skip_locked: bool,
    ) -> Result<usize, AwaError> {
        if jobs.is_empty() {
            return Ok(0);
        }

        let receipt_jobs: Vec<(i64, i64)> = jobs
            .iter()
            .map(|(job_id, run_lease, _)| (*job_id, *run_lease))
            .collect();
        let locked: Option<HashSet<_>> = if skip_locked {
            Some(
                self.try_lock_receipt_attempt_keys_tx(tx, &receipt_jobs)
                    .await?
                    .into_iter()
                    .collect(),
            )
        } else {
            None
        };
        let jobs: Vec<_> = jobs
            .iter()
            .filter(|(job_id, run_lease, _)| {
                locked
                    .as_ref()
                    .is_none_or(|locked| locked.contains(&(*job_id, *run_lease)))
            })
            .collect();
        if jobs.is_empty() {
            return Ok(0);
        }
        let receipt_jobs: Vec<_> = jobs.iter().map(|(id, lease, _)| (*id, *lease)).collect();
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
                WHERE claim_batches.job_ids && $1::bigint[]
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

    async fn load_ready_snapshot_candidates_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        deleted: &[DeletedLeaseRow],
    ) -> Result<
        (
            BTreeMap<ReadyBodyKey, Vec<ReadySnapshotRow>>,
            BTreeSet<ReadyBodyLaneKey>,
        ),
        AwaError,
    > {
        let schema = self.schema();
        let ready_slots: Vec<i32> = deleted.iter().map(|row| row.ready_slot).collect();
        let ready_generations: Vec<i64> = deleted.iter().map(|row| row.ready_generation).collect();
        let queues: Vec<String> = deleted.iter().map(|row| row.queue.clone()).collect();
        let enqueue_shards: Vec<i16> = deleted.iter().map(|row| row.enqueue_shard).collect();
        let lane_seqs: Vec<i64> = deleted.iter().map(|row| row.lane_seq).collect();
        let job_ids: Vec<i64> = deleted.iter().map(|row| row.job_id).collect();
        let run_leases: Vec<i64> = deleted.iter().map(|row| row.run_lease).collect();

        // Resolve moved ready bodies without opening the whole tombstone ring.
        let slots = ready_slots
            .iter()
            .map(|&slot| ring_slot_number(slot, "queue"))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let tombstone_sources = slots
            .iter()
            .map(|&slot| format!("SELECT * FROM {}", ready_tombstone_child_name(schema, slot)))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let ready_rows: Vec<ReadySnapshotRow> = sqlx::query_as(&format!(
            r#"
            WITH refs(ready_slot, ready_generation, queue, enqueue_shard, lane_seq, job_id, run_lease) AS (
                SELECT * FROM unnest($1::int[], $2::bigint[], $3::text[], $4::smallint[], $5::bigint[], $6::bigint[], $7::bigint[])
            )
            SELECT
                ready.ready_slot,
                ready.ready_generation,
                ready.job_id,
                ready.kind,
                ready.queue,
                ready.args,
                ready.priority,
                ready.lane_seq,
                ready.enqueue_shard,
                ready.run_at,
                ready.created_at,
                ready.unique_key,
                ready.unique_states,
                COALESCE(ready.payload, '{{}}'::jsonb) AS payload,
                ready.run_lease,
                NOT EXISTS (
                    SELECT 1 FROM ({tombstone_sources}) AS tomb
                    WHERE tomb.ready_slot = ready.ready_slot
                      AND tomb.ready_generation = ready.ready_generation
                      AND tomb.queue = ready.queue
                      AND tomb.priority = ready.priority
                      AND tomb.enqueue_shard = ready.enqueue_shard
                      AND tomb.lane_seq = ready.lane_seq
                      AND tomb.job_id = ready.job_id
                ) AS live_body
            FROM refs
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = refs.ready_slot
             AND ready.ready_generation = refs.ready_generation
             AND ready.queue = refs.queue
             AND ready.enqueue_shard = refs.enqueue_shard
             AND ready.lane_seq = refs.lane_seq
             AND ready.job_id = refs.job_id
             AND ready.run_lease + 1 = refs.run_lease
            "#
        ))
        .bind(&ready_slots)
        .bind(&ready_generations)
        .bind(&queues)
        .bind(&enqueue_shards)
        .bind(&lane_seqs)
        .bind(&job_ids)
        .bind(&run_leases)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let mut ready_map: BTreeMap<ReadyBodyKey, Vec<ReadySnapshotRow>> = BTreeMap::new();
        for row in ready_rows {
            ready_map
                .entry((
                    row.ready_slot,
                    row.ready_generation,
                    row.queue.clone(),
                    row.enqueue_shard,
                    row.lane_seq,
                    row.job_id,
                    row.run_lease + 1,
                ))
                .or_default()
                .push(row);
        }

        let mut claimed_lanes: BTreeSet<ReadyBodyLaneKey> = BTreeSet::new();
        let ambiguous: Vec<&ReadySnapshotRow> = ready_map
            .values()
            .filter(|rows| rows.len() > 1)
            .flatten()
            .collect();
        if !ambiguous.is_empty() {
            // A unique body needs no extra relation lookup. Multiple bodies
            // need the emitted physical lane even if a replacement is live;
            // scope that evidence to the referenced ready slots.
            let attempt_sources = ambiguous
                .iter()
                .map(|row| row.ready_slot)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .map(|slot| ring_slot_number(slot, "queue"))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|slot| {
                    format!(
                        "SELECT * FROM {}",
                        ready_claim_attempt_batch_child_name(schema, slot)
                    )
                })
                .collect::<Vec<_>>()
                .join(" UNION ALL ");
            let lanes: Vec<ReadyBodyLaneKey> = sqlx::query_as(&format!(
                r#"
                WITH refs(ready_slot,ready_generation,queue,enqueue_shard,lane_seq,job_id,run_lease,priority) AS (
                    SELECT * FROM unnest($1::int[], $2::bigint[], $3::text[], $4::smallint[],
                                        $5::bigint[], $6::bigint[], $7::bigint[], $8::smallint[])
                )
                SELECT ready_slot,ready_generation,queue,enqueue_shard,lane_seq,job_id,run_lease,priority
                FROM refs
                WHERE EXISTS (
                    SELECT 1 FROM ({attempt_sources}) AS attempts
                    WHERE attempts.ready_slot = refs.ready_slot
                      AND attempts.ready_generation = refs.ready_generation
                      AND attempts.queue = refs.queue
                      AND attempts.priority = refs.priority
                      AND attempts.enqueue_shard = refs.enqueue_shard
                      AND attempts.first_lane_seq <= refs.lane_seq
                      AND attempts.next_lane_seq > refs.lane_seq
                      AND attempts.lane_ranges @> int8range(refs.lane_seq,refs.lane_seq + 1,'[)')
                )
                "#
            ))
            .bind(ambiguous.iter().map(|row| row.ready_slot).collect::<Vec<_>>())
            .bind(ambiguous.iter().map(|row| row.ready_generation).collect::<Vec<_>>())
            .bind(ambiguous.iter().map(|row| row.queue.clone()).collect::<Vec<_>>())
            .bind(ambiguous.iter().map(|row| row.enqueue_shard).collect::<Vec<_>>())
            .bind(ambiguous.iter().map(|row| row.lane_seq).collect::<Vec<_>>())
            .bind(ambiguous.iter().map(|row| row.job_id).collect::<Vec<_>>())
            .bind(ambiguous.iter().map(|row| row.run_lease + 1).collect::<Vec<_>>())
            .bind(ambiguous.iter().map(|row| row.priority).collect::<Vec<_>>())
            .fetch_all(tx.as_mut()).await.map_err(map_sqlx_error)?;
            claimed_lanes.extend(lanes);
        }

        Ok((ready_map, claimed_lanes))
    }

    async fn hydrate_deleted_leases_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        deleted: Vec<DeletedLeaseRow>,
    ) -> Result<Vec<LeaseTransitionRow>, AwaError> {
        let (hydrated, sources) = self
            .hydrate_deleted_leases_with_sources_tx(tx, deleted)
            .await?;
        if !sources.is_empty() {
            self.retire_retained_attempt_copies_tx(tx, &sources, false)
                .await?;
        }
        Ok(hydrated)
    }

    async fn try_hydrate_deleted_leases_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        deleted: Vec<DeletedLeaseRow>,
    ) -> Result<Result<Vec<LeaseTransitionRow>, BusyRetained>, AwaError> {
        let hydrated = async {
            let (hydrated, sources) = self
                .hydrate_deleted_leases_with_sources_tx(tx, deleted)
                .await?;
            if sources.is_empty() {
                return Ok(Ok(hydrated));
            }
            if let Some(busy) = self
                .retire_retained_attempt_copies_tx(tx, &sources, true)
                .await?
            {
                return Ok(Err(busy));
            }
            Ok(Ok(hydrated))
        }
        .await;
        match hydrated {
            Ok(hydrated) => Ok(hydrated),
            // One unresolvable attempt must not stop the rescue of the others.
            Err(HydrateError::Ambiguous(attempts)) => {
                for (job_id, run_lease) in &attempts {
                    tracing::error!(
                        schema = self.schema(),
                        job_id,
                        run_lease,
                        "Rescue skipped an attempt: queue storage ready body ambiguous for deleted lease; it stays running until its retained bodies are repaired"
                    );
                }
                Ok(Err(BusyRetained {
                    attempts,
                    unresolvable: true,
                }))
            }
            Err(HydrateError::Awa(err)) => Err(err),
        }
    }

    async fn hydrate_deleted_leases_with_sources_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        deleted: Vec<DeletedLeaseRow>,
    ) -> Result<(Vec<LeaseTransitionRow>, Vec<ReadyRowIdentity>), HydrateError> {
        if deleted.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let schema = self.schema();
        let job_ids: Vec<i64> = deleted.iter().map(|row| row.job_id).collect();
        let run_leases: Vec<i64> = deleted.iter().map(|row| row.run_lease).collect();
        let (ready_map, claimed_lanes) =
            self.load_ready_snapshot_candidates_tx(tx, &deleted).await?;
        // Report every ambiguous attempt at once, before any write, so a
        // caller that sets them aside needs one retry for all of them.
        let ambiguous: Vec<(i64, i64)> = deleted
            .iter()
            .filter(|row| {
                ready_map
                    .get(&(
                        row.ready_slot,
                        row.ready_generation,
                        row.queue.clone(),
                        row.enqueue_shard,
                        row.lane_seq,
                        row.job_id,
                        row.run_lease,
                    ))
                    .is_some_and(|candidates| {
                        matches!(
                            select_claimed_ready_body(candidates, &claimed_lanes, row),
                            Err(HydrateError::Ambiguous(_))
                        )
                    })
            })
            .map(|row| (row.job_id, row.run_lease))
            .collect();
        if !ambiguous.is_empty() {
            return Err(HydrateError::Ambiguous(ambiguous));
        }

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
                WHERE claim_batches.job_ids && $1::bigint[]
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

        let attempt_map: BTreeMap<(i64, i64), AttemptStateRow> = attempt_rows
            .into_iter()
            .map(|row| ((row.job_id, row.run_lease), row))
            .collect();

        let mut hydrated = Vec::with_capacity(deleted.len());
        let mut retained_sources = Vec::new();
        for deleted_row in deleted {
            let candidates = ready_map
                .get(&(
                    deleted_row.ready_slot,
                    deleted_row.ready_generation,
                    deleted_row.queue.clone(),
                    deleted_row.enqueue_shard,
                    deleted_row.lane_seq,
                    deleted_row.job_id,
                    deleted_row.run_lease,
                ))
                .ok_or_else(|| {
                    AwaError::Validation(format!(
                        "queue storage ready row missing for deleted lease job {} run_lease {}",
                        deleted_row.job_id, deleted_row.run_lease
                    ))
                })?;
            let (ready, has_emitted) =
                select_claimed_ready_body(candidates, &claimed_lanes, &deleted_row)?;
            if !ready.live_body {
                retained_sources.push(ReadyRowIdentity {
                    ready_slot: ready.ready_slot,
                    ready_generation: ready.ready_generation,
                    queue: ready.queue.clone(),
                    priority: ready.priority,
                    enqueue_shard: ready.enqueue_shard,
                    lane_seq: ready.lane_seq,
                    job_id: ready.job_id,
                    run_lease: ready.run_lease,
                });
            }
            // Claim-time aging adds this provenance to the returned JobRow,
            // not the immutable ready payload. Recovery must reconstruct the
            // same default when it reads that retained body after the claim.
            // Mutate the cloned JSON so unrelated payload fields survive.
            let mut payload = ready.payload.clone();
            if deleted_row.priority < ready.priority {
                let metadata = payload
                    .as_object_mut()
                    .ok_or_else(|| {
                        AwaError::Validation(
                            "queue storage payload must be a JSON object".to_string(),
                        )
                    })?
                    .entry("metadata")
                    .or_insert_with(|| serde_json::json!({}))
                    .as_object_mut()
                    .ok_or_else(|| {
                        AwaError::Validation(
                            "queue storage payload metadata must be a JSON object".to_string(),
                        )
                    })?;
                metadata
                    .entry("_awa_original_priority")
                    .or_insert_with(|| serde_json::Value::from(i64::from(ready.priority)));
            }
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
                ready_priority: ready.priority,
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
                payload,
                progress: attempt.and_then(|row| row.progress.clone()),
                self_contained_terminal: has_emitted
                    && !ready.live_body
                    && candidates.iter().any(|row| row.live_body),
            });
        }

        Ok((hydrated, retained_sources))
    }

    async fn retire_retained_attempt_copies_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        sources: &[ReadyRowIdentity],
        skip_busy: bool,
    ) -> Result<Option<BusyRetained>, HydrateError> {
        let schema = self.schema();
        let mut expected = BTreeMap::new();
        for source in sources {
            if expected
                .insert((source.job_id, source.run_lease), source)
                .is_some()
            {
                return Err(HydrateError::Ambiguous(vec![(
                    source.job_id,
                    source.run_lease + 1,
                )]));
            }
        }
        let job_ids: Vec<i64> = sources.iter().map(|row| row.job_id).collect();
        let run_leases: Vec<i64> = sources.iter().map(|row| row.run_lease).collect();
        if skip_busy {
            let busy = sqlx::query_as::<_, (i64, i64)>(&format!(
                r#"
                WITH refs(job_id, run_lease) AS (
                    SELECT * FROM unnest($1::bigint[], $2::bigint[])
                ), locked_heads AS MATERIALIZED (
                    SELECT heads.queue, heads.priority, heads.enqueue_shard
                    FROM {schema}.queue_claim_heads AS heads
                    WHERE EXISTS (
                        SELECT 1
                        FROM refs
                        JOIN {schema}.ready_entries AS ready
                          ON ready.job_id = refs.job_id AND ready.run_lease = refs.run_lease
                        WHERE ready.queue = heads.queue AND ready.priority = heads.priority
                          AND ready.enqueue_shard = heads.enqueue_shard
                    )
                    ORDER BY heads.queue COLLATE "C", heads.priority, heads.enqueue_shard
                    FOR UPDATE OF heads SKIP LOCKED
                )
                SELECT refs.job_id, refs.run_lease + 1
                FROM refs
                WHERE EXISTS (
                    SELECT 1
                    FROM {schema}.ready_entries AS ready
                    JOIN {schema}.queue_claim_heads AS heads
                      ON heads.queue = ready.queue AND heads.priority = ready.priority
                     AND heads.enqueue_shard = ready.enqueue_shard
                    WHERE ready.job_id = refs.job_id AND ready.run_lease = refs.run_lease
                      AND NOT EXISTS (
                          SELECT 1 FROM locked_heads
                          WHERE locked_heads.queue = heads.queue
                            AND locked_heads.priority = heads.priority
                            AND locked_heads.enqueue_shard = heads.enqueue_shard
                      )
                )
                "#
            ))
            .bind(&job_ids)
            .bind(&run_leases)
            .fetch_all(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
            if !busy.is_empty() {
                return Ok(Some(BusyRetained::busy(busy)));
            }
        } else {
            self.lock_claim_heads_of_jobs_with_wait_tx(tx, &job_ids, true)
                .await?;
        }
        let copies: Vec<ReadyRowIdentity> = sqlx::query_as(&format!(
            r#"
            WITH refs(job_id, run_lease) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[])
            )
            SELECT ready.ready_slot, ready.ready_generation, ready.queue, ready.priority,
                   ready.enqueue_shard, ready.lane_seq, ready.job_id, ready.run_lease
            FROM refs
            JOIN {schema}.ready_entries AS ready
              ON ready.job_id = refs.job_id AND ready.run_lease = refs.run_lease
            "#
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        let attempt_sources = copies
            .iter()
            .map(|row| ring_slot_number(row.ready_slot, "queue"))
            .collect::<Result<BTreeSet<_>, _>>()?
            .into_iter()
            .map(|slot| {
                format!(
                    "SELECT * FROM {}",
                    ready_claim_attempt_batch_child_name(schema, slot)
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        if copies.is_empty() {
            return Err(AwaError::Validation(
                "queue storage ready row missing for deleted lease during retirement".to_string(),
            )
            .into());
        }
        let slots: Vec<i32> = copies.iter().map(|row| row.ready_slot).collect();
        let generations: Vec<i64> = copies.iter().map(|row| row.ready_generation).collect();
        let queues: Vec<String> = copies.iter().map(|row| row.queue.clone()).collect();
        let priorities: Vec<i16> = copies.iter().map(|row| row.priority).collect();
        let shards: Vec<i16> = copies.iter().map(|row| row.enqueue_shard).collect();
        let sequences: Vec<i64> = copies.iter().map(|row| row.lane_seq).collect();
        let copy_job_ids: Vec<i64> = copies.iter().map(|row| row.job_id).collect();
        let copy_leases: Vec<i64> = copies.iter().map(|row| row.run_lease).collect();
        let emitted: Vec<ReadyRowIdentity> = sqlx::query_as(&format!(
            r#"
            WITH refs(ready_slot, ready_generation, queue, priority, enqueue_shard,
                      lane_seq, job_id, run_lease) AS (
                SELECT * FROM unnest($1::int[], $2::bigint[], $3::text[], $4::smallint[],
                                    $5::smallint[], $6::bigint[], $7::bigint[], $8::bigint[])
            )
            SELECT * FROM refs
            WHERE EXISTS (
                SELECT 1 FROM ({attempt_sources}) AS attempts
                WHERE attempts.ready_slot = refs.ready_slot
                  AND attempts.ready_generation = refs.ready_generation
                  AND attempts.queue = refs.queue AND attempts.priority = refs.priority
                  AND attempts.enqueue_shard = refs.enqueue_shard
                  AND attempts.first_lane_seq <= refs.lane_seq
                  AND attempts.next_lane_seq > refs.lane_seq
                  AND attempts.lane_ranges @> int8range(refs.lane_seq, refs.lane_seq + 1, '[)')
            )
            "#
        ))
        .bind(&slots)
        .bind(&generations)
        .bind(&queues)
        .bind(&priorities)
        .bind(&shards)
        .bind(&sequences)
        .bind(&copy_job_ids)
        .bind(&copy_leases)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        let mut seen = BTreeSet::new();
        for row in &emitted {
            let key = (row.job_id, row.run_lease);
            if !seen.insert(key) || expected.get(&key).copied() != Some(row) {
                return Err(HydrateError::Ambiguous(vec![(
                    row.job_id,
                    row.run_lease + 1,
                )]));
            }
        }
        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.ready_tombstones (
                ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
            )
            SELECT * FROM unnest($1::int[], $2::bigint[], $3::text[], $4::smallint[],
                                $5::smallint[], $6::bigint[], $7::bigint[])
            ON CONFLICT DO NOTHING
            "#
        ))
        .bind(&slots)
        .bind(&generations)
        .bind(&queues)
        .bind(&priorities)
        .bind(&shards)
        .bind(&sequences)
        .bind(&copy_job_ids)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(None)
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
                WHERE claim_batches.job_ids @> ARRAY[$1::bigint]
                  AND items.job_id = $1
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
        options: &RescueBatchOptions,
        excluded: &BTreeSet<(i64, i64)>,
        selected: Option<&BTreeSet<(i64, i64)>>,
        batch: &mut ReceiptRescueBatch,
    ) -> Result<(), AwaError> {
        let mut remaining = RECEIPT_RESCUE_BATCH_LIMIT;
        for slot in self.rescue_slot_order_tx(tx, options).await? {
            let (slot_rescued, scan_full, cursor) = self
                .rescue_stale_receipt_claims_for_slot_tx(
                    tx,
                    slot,
                    cutoff,
                    options
                        .cursor_scan_limit
                        .map_or(RECEIPT_RESCUE_CURSOR_SCAN_LIMIT, |limit| {
                            limit.max(1) as i64
                        }),
                    remaining,
                    excluded,
                    selected,
                )
                .await?;
            batch.saturated |= scan_full;
            if !scan_full && slot_rescued.is_empty() {
                batch.idle_slots.push(slot);
            }
            batch.scan_progress.push(RescueScanProgress {
                slot,
                cursors: vec![RescueScanCursor {
                    position: cursor,
                    saturated: scan_full,
                }],
            });
            remaining = remaining.saturating_sub(slot_rescued.len() as i64);
            batch.rescued.extend(slot_rescued);
            if remaining == 0 {
                batch.saturated = true;
                break;
            }
        }
        Ok(())
    }

    /// The claim slots one rescue batch visits, in order. Slots go from the
    /// oldest initialized one to the current one, so older claims come first.
    async fn rescue_slot_order_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        options: &RescueBatchOptions,
    ) -> Result<Vec<i32>, AwaError> {
        let (oldest, by_age) = self.claim_slots_by_age_tx(tx).await?;
        let others: Vec<i32> = by_age
            .into_iter()
            .filter(|slot| Some(*slot) != oldest)
            .collect();
        let mut slots = Vec::with_capacity(self.claim_slot_count());
        match options.slot_order {
            RescueSlotOrder::OldestFirst => {
                slots.extend(oldest);
                slots.extend(others);
            }
            RescueSlotOrder::OthersFirst(offset) => {
                if !others.is_empty() {
                    let start = offset % others.len();
                    slots.extend(others[start..].iter().chain(&others[..start]));
                }
                slots.extend(oldest);
            }
        }
        slots.retain(|slot| !options.skip_slots.contains(slot));
        let closed = self.closed_sealed_claim_slots_tx(tx, &slots).await?;
        slots.retain(|slot| !closed.contains(slot));
        Ok(slots)
    }

    /// The sealed slots among `slots` whose claims are all closed, so rescue
    /// has nothing to find in them.
    ///
    /// Each rotation leaves a sealed slot of every claim taken in it, most of
    /// them closed, and the slot stays until prune reclaims it. Rescue walks a
    /// slot's claims from a cursor, a bounded window per batch, so without
    /// this it walked the closed claims of every large sealed slot ahead of
    /// the slot holding a dead worker's claims, and found those minutes late.
    ///
    /// A slot is proven by counts, as claim prune proves it before truncating:
    /// claims in the slot against closures in its ledgers. Each count runs in
    /// a savepoint rolled back at once, so the slot's partitions are not held
    /// for the rest of the rescue transaction. A proof is kept for its slot's
    /// generation for [`CLAIM_SLOT_PROOF_TTL`]. The current slot is never
    /// skipped.
    async fn closed_sealed_claim_slots_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        slots: &[i32],
    ) -> Result<HashSet<i32>, AwaError> {
        let schema = self.schema();
        let current_slot: Option<i32> = sqlx::query_scalar(&format!(
            "SELECT current_slot FROM {schema}.claim_ring_state WHERE singleton = TRUE"
        ))
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        let generations: HashMap<i32, i64> = sqlx::query_as::<_, (i32, i64)>(&format!(
            "SELECT slot, generation FROM {schema}.claim_ring_slots WHERE slot = ANY($1)"
        ))
        .bind(slots)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?
        .into_iter()
        .collect();

        let mut closed = HashSet::new();
        for &slot in slots {
            if Some(slot) == current_slot {
                continue;
            }
            let Some(&generation) = generations.get(&slot) else {
                continue;
            };
            let cached = self
                .claim_slot_proofs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&slot)
                .copied()
                .filter(|proof| {
                    proof.generation == generation
                        && proof.proven_at.elapsed() < CLAIM_SLOT_PROOF_TTL
                });
            let is_closed = match cached {
                Some(proof) => proof.closed,
                None => {
                    let is_closed = self.claim_slot_proven_closed_tx(tx, slot).await?;
                    self.claim_slot_proofs
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(
                            slot,
                            ClaimSlotProof {
                                generation,
                                closed: is_closed,
                                proven_at: Instant::now(),
                            },
                        );
                    is_closed
                }
            };
            if is_closed {
                closed.insert(slot);
            }
        }
        Ok(closed)
    }

    /// Whether every claim in claim slot `slot` is closed, by counts: see
    /// [`claim_slot_closed_count_sql`]. A slot with no claims is closed
    /// without reading its closure partitions.
    async fn claim_slot_proven_closed_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        slot: i32,
    ) -> Result<bool, AwaError> {
        let schema = self.schema();
        let slot_index = ring_slot_number(slot, "claim")?;
        sqlx::query("SAVEPOINT awa_rescue_slot_proof")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        let proven = async {
            let claimed: i64 =
                sqlx::query_scalar(&claim_slot_claimed_count_sql(schema, slot_index))
                    .fetch_one(tx.as_mut())
                    .await?;
            if claimed == 0 {
                return Ok::<_, sqlx::Error>(true);
            }
            let closed: i64 = sqlx::query_scalar(&claim_slot_closed_count_sql(schema, slot_index))
                .fetch_one(tx.as_mut())
                .await?;
            Ok(claimed == closed)
        }
        .await;
        sqlx::query("ROLLBACK TO SAVEPOINT awa_rescue_slot_proof")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query("RELEASE SAVEPOINT awa_rescue_slot_proof")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        proven.map_err(map_sqlx_error)
    }

    #[allow(clippy::too_many_arguments)]
    async fn rescue_stale_receipt_claims_for_slot_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        slot: i32,
        cutoff: DateTime<Utc>,
        scan_limit: i64,
        rescue_limit: i64,
        excluded: &BTreeSet<(i64, i64)>,
        selected: Option<&BTreeSet<(i64, i64)>>,
    ) -> Result<ReceiptRescueScan, AwaError> {
        let (excluded_job_ids, excluded_run_leases): (Vec<_>, Vec<_>) =
            excluded.iter().copied().unzip();
        let (selected_job_ids, selected_run_leases): (Vec<i64>, Vec<i64>) = selected
            .map(|pairs| pairs.iter().copied().unzip())
            .unwrap_or_default();
        let schema = self.schema();
        let claim_child = claim_child_name(schema, slot as usize);
        let claim_batch_child = claim_batch_child_name(schema, slot as usize);
        let closure_child = closure_child_name(schema, slot as usize);
        let closure_batch_child = claim_closure_batch_child_name(schema, slot as usize);
        let candidate_ctes = format!(
            r#"
            cursor_row AS (
                SELECT
                    rescue_cursor_claimed_at,
                    rescue_cursor_job_id,
                    rescue_cursor_run_lease
                FROM {schema}.claim_ring_slots
                WHERE slot = $1
                FOR UPDATE
            ),
            -- The walk takes the first $3 claims after the cursor in
            -- (claimed_at, job_id, run_lease) order, then wraps to the
            -- first claims at or before it. Each side reads only the claims
            -- it can return: row-local claims through the cursor index, and
            -- compact batches (whose members share claimed_at) only up to
            -- the batch at which $3 members are certainly covered.
            after_batch_bound AS (
                SELECT min(running.claimed_at) AS claimed_at
                FROM (
                    SELECT
                        claim_batches.claimed_at,
                        sum(claim_batches.claimed_count) FILTER (
                            WHERE claim_batches.claimed_at > (SELECT rescue_cursor_claimed_at FROM cursor_row)
                        ) OVER (
                            ORDER BY claim_batches.claimed_at, claim_batches.batch_id
                        ) AS covered
                    FROM {claim_batch_child} AS claim_batches
                    WHERE claim_batches.claim_slot = $1
                      AND claim_batches.claimed_at < $2
                      AND claim_batches.claimed_at >= (SELECT rescue_cursor_claimed_at FROM cursor_row)
                ) AS running
                WHERE running.covered >= $3
            ),
            after_source AS (
                (
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
                      AND claims.claimed_at < $2
                      AND (claims.claimed_at, claims.job_id, claims.run_lease)
                          > (
                          (SELECT rescue_cursor_claimed_at FROM cursor_row),
                          (SELECT rescue_cursor_job_id FROM cursor_row),
                          (SELECT rescue_cursor_run_lease FROM cursor_row)
                        )
                    ORDER BY claims.claimed_at, claims.job_id, claims.run_lease
                    LIMIT $3
                )
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
                CROSS JOIN after_batch_bound
                CROSS JOIN LATERAL unnest(
                    claim_batches.job_ids,
                    claim_batches.run_leases,
                    claim_batches.receipt_ids,
                    claim_batches.lane_seqs,
                    claim_batches.attempts,
                    claim_batches.max_attempts
                ) AS items(job_id, run_lease, receipt_id, lane_seq, attempt, max_attempts)
                WHERE claim_batches.claim_slot = $1
                  AND claim_batches.claimed_at < $2
                  AND claim_batches.claimed_at >= (SELECT rescue_cursor_claimed_at FROM cursor_row)
                  AND claim_batches.claimed_at
                      <= COALESCE(after_batch_bound.claimed_at, 'infinity'::timestamptz)
                  AND (claim_batches.claimed_at, items.job_id, items.run_lease)
                      > (
                          (SELECT rescue_cursor_claimed_at FROM cursor_row),
                          (SELECT rescue_cursor_job_id FROM cursor_row),
                          (SELECT rescue_cursor_run_lease FROM cursor_row)
                        )
            ),
            after_cursor AS MATERIALIZED (
                SELECT
                    after_source.*,
                    row_number() OVER (
                        ORDER BY after_source.claimed_at, after_source.job_id, after_source.run_lease
                    ) AS rn
                FROM after_source
                ORDER BY after_source.claimed_at, after_source.job_id, after_source.run_lease
                LIMIT $3
            ),
            after_count AS MATERIALIZED (
                SELECT count(*)::bigint AS count FROM after_cursor
            ),
            before_batch_bound AS (
                SELECT min(running.claimed_at) AS claimed_at
                FROM (
                    SELECT
                        claim_batches.claimed_at,
                        sum(claim_batches.claimed_count) FILTER (
                            WHERE claim_batches.claimed_at < (SELECT rescue_cursor_claimed_at FROM cursor_row)
                        ) OVER (
                            ORDER BY claim_batches.claimed_at, claim_batches.batch_id
                        ) AS covered
                    FROM {claim_batch_child} AS claim_batches
                    WHERE claim_batches.claim_slot = $1
                      AND claim_batches.claimed_at < $2
                      AND claim_batches.claimed_at <= (SELECT rescue_cursor_claimed_at FROM cursor_row)
                ) AS running
                WHERE running.covered >= $3
            ),
            before_source AS (
                (
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
                    WHERE (SELECT count FROM after_count) < $3
                      AND claims.claim_slot = $1
                      AND claims.claimed_at < $2
                      AND (claims.claimed_at, claims.job_id, claims.run_lease)
                          <= (
                          (SELECT rescue_cursor_claimed_at FROM cursor_row),
                          (SELECT rescue_cursor_job_id FROM cursor_row),
                          (SELECT rescue_cursor_run_lease FROM cursor_row)
                        )
                    ORDER BY claims.claimed_at, claims.job_id, claims.run_lease
                    LIMIT $3
                )
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
                CROSS JOIN before_batch_bound
                CROSS JOIN LATERAL unnest(
                    claim_batches.job_ids,
                    claim_batches.run_leases,
                    claim_batches.receipt_ids,
                    claim_batches.lane_seqs,
                    claim_batches.attempts,
                    claim_batches.max_attempts
                ) AS items(job_id, run_lease, receipt_id, lane_seq, attempt, max_attempts)
                WHERE (SELECT count FROM after_count) < $3
                  AND claim_batches.claim_slot = $1
                  AND claim_batches.claimed_at < $2
                  AND claim_batches.claimed_at <= (SELECT rescue_cursor_claimed_at FROM cursor_row)
                  AND claim_batches.claimed_at
                      <= COALESCE(before_batch_bound.claimed_at, 'infinity'::timestamptz)
                  AND (claim_batches.claimed_at, items.job_id, items.run_lease)
                      <= (
                          (SELECT rescue_cursor_claimed_at FROM cursor_row),
                          (SELECT rescue_cursor_job_id FROM cursor_row),
                          (SELECT rescue_cursor_run_lease FROM cursor_row)
                        )
            ),
            before_cursor AS MATERIALIZED (
                SELECT
                    before_source.*,
                    after_count.count + row_number() OVER (
                        ORDER BY before_source.claimed_at, before_source.job_id, before_source.run_lease
                    ) AS rn
                FROM before_source
                CROSS JOIN after_count
                ORDER BY before_source.claimed_at, before_source.job_id, before_source.run_lease
                LIMIT (SELECT GREATEST($3 - count, 0) FROM after_count)
            ),
            candidate_rows AS MATERIALIZED (
                SELECT * FROM after_cursor
                UNION ALL
                SELECT * FROM before_cursor
            ),
            candidates AS MATERIALIZED (
                SELECT
                    candidate_rows.claim_slot,
                    candidate_rows.batch_id,
                    candidate_rows.ready_slot,
                    candidate_rows.ready_generation,
                    candidate_rows.job_id,
                    candidate_rows.queue,
                    candidate_rows.priority,
                    candidate_rows.attempt,
                    candidate_rows.run_lease,
                    candidate_rows.max_attempts,
                    candidate_rows.lane_seq,
                    candidate_rows.enqueue_shard,
                    candidate_rows.receipt_id,
                    candidate_rows.claimed_at,
                    candidate_rows.closed_at,
                    candidate_rows.compact_batch,
                    COALESCE(attempt.heartbeat_at, candidate_rows.claimed_at) < $2 AS is_stale,
                    (
                        candidate_rows.closed_at IS NOT NULL
                        OR EXISTS (
                            SELECT 1 FROM {closure_child} AS closures
                            WHERE closures.claim_slot = candidate_rows.claim_slot
                              AND closures.job_id = candidate_rows.job_id
                              AND closures.run_lease = candidate_rows.run_lease
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM {closure_batch_child} AS closure_batches
                            WHERE closure_batches.receipt_ranges @> candidate_rows.receipt_id
                        )
                        OR EXISTS (
                            SELECT 1 FROM {schema}.done_entries AS done
                            WHERE done.job_id = candidate_rows.job_id
                              AND done.run_lease = candidate_rows.run_lease
                        )
                        OR EXISTS (
                            SELECT 1 FROM {schema}.deferred_jobs AS deferred
                            WHERE deferred.job_id = candidate_rows.job_id
                              AND deferred.run_lease = candidate_rows.run_lease
                        )
                        OR EXISTS (
                            SELECT 1 FROM {schema}.dlq_entries AS dlq
                            WHERE dlq.job_id = candidate_rows.job_id
                              AND dlq.run_lease = candidate_rows.run_lease
                        )
                    ) AS is_closed,
                    EXISTS (
                        SELECT 1 FROM {schema}.leases AS lease
                        WHERE lease.job_id = candidate_rows.job_id
                          AND lease.run_lease = candidate_rows.run_lease
                    ) AS is_lease_managed,
                    candidate_rows.rn
                FROM candidate_rows
                LEFT JOIN {schema}.attempt_state AS attempt
                  ON attempt.job_id = candidate_rows.job_id
                 AND attempt.run_lease = candidate_rows.run_lease
            ),
            stale_candidates AS (
                SELECT candidates.*
                FROM candidates
                WHERE NOT is_closed
                  AND NOT is_lease_managed
                  AND is_stale
                  AND NOT EXISTS (
                      SELECT 1 FROM unnest($5::bigint[], $6::bigint[]) AS excluded(job_id, run_lease)
                      WHERE excluded.job_id = candidates.job_id AND excluded.run_lease = candidates.run_lease
                  )
                  AND ($7::boolean OR EXISTS (
                      SELECT 1 FROM unnest($8::bigint[], $9::bigint[]) AS selected(job_id, run_lease)
                      WHERE selected.job_id = candidates.job_id AND selected.run_lease = candidates.run_lease
                  ))
                ORDER BY rn
                LIMIT $4
            )"#
        );
        // Closers take the attempt's advisory lock before their statement
        // snapshot. Rescue takes the same locks in a statement of its own,
        // so the statement that decides and writes the closure starts after
        // any closer that held a lock first has committed and sees its
        // evidence. Only attempts locked here may be closed below.
        let (locked_job_ids, locked_run_leases): (Vec<i64>, Vec<i64>) =
            sqlx::query_as::<_, (i64, i64)>(&format!(
                r#"
                WITH {candidate_ctes}
                SELECT job_id, run_lease
                FROM stale_candidates
                WHERE pg_catalog.pg_try_advisory_xact_lock(
                    pg_catalog.hashtextextended(
                        format('awa.receipt.complete:%s:%s', job_id, run_lease),
                        0
                    )
                )
                "#
            ))
            .persistent(false)
            .bind(slot)
            .bind(cutoff)
            .bind(scan_limit)
            .bind(rescue_limit)
            .bind(&excluded_job_ids)
            .bind(&excluded_run_leases)
            .bind(selected.is_none())
            .bind(&selected_job_ids)
            .bind(&selected_run_leases)
            .fetch_all(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?
            .into_iter()
            .unzip();
        let (rescued, scan_full, cursor_at, cursor_job_id, cursor_run_lease): ReceiptRescueScanRow = sqlx::query_as(&format!(
            r#"
            WITH {candidate_ctes},
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
                  AND (claims.job_id, claims.run_lease) IN (
                      SELECT locked.job_id, locked.run_lease
                      FROM unnest($10::bigint[], $11::bigint[]) AS locked(job_id, run_lease)
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
                  AND (stale_candidates.job_id, stale_candidates.run_lease) IN (
                      SELECT locked.job_id, locked.run_lease
                      FROM unnest($10::bigint[], $11::bigint[]) AS locked(job_id, run_lease)
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
                UPDATE {claim_child} AS claims
                SET closed_at = COALESCE(claims.closed_at, inserted.closed_at)
                FROM inserted
                WHERE claims.job_id = inserted.job_id
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
                RETURNING slots.rescue_cursor_claimed_at, slots.rescue_cursor_job_id, slots.rescue_cursor_run_lease
            ),
            rescued_rows AS (
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
            )
            SELECT
                COALESCE((SELECT jsonb_agg(to_jsonb(rescued_rows)) FROM rescued_rows), '[]'::jsonb),
                (SELECT count(*) >= $3 FROM candidates),
                (SELECT rescue_cursor_claimed_at FROM advance_cursor),
                (SELECT rescue_cursor_job_id FROM advance_cursor),
                (SELECT rescue_cursor_run_lease FROM advance_cursor)
            "#
        ))
        .persistent(false)
        .bind(slot)
        .bind(cutoff)
        .bind(scan_limit)
        .bind(rescue_limit)
        .bind(&excluded_job_ids)
        .bind(&excluded_run_leases)
        .bind(selected.is_none())
        .bind(&selected_job_ids)
        .bind(&selected_run_leases)
        .bind(&locked_job_ids)
        .bind(&locked_run_leases)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        let scan_full = scan_full || rescued.0.len() as i64 >= rescue_limit;
        Ok((
            rescued.0,
            scan_full,
            cursor_at
                .zip(cursor_job_id)
                .zip(cursor_run_lease)
                .map(|((at, id), lease)| (at, id, lease)),
        ))
    }

    /// Receipt-side counterpart to `rescue_expired_deadlines`: scans
    /// `lease_claims` for rows whose per-claim `deadline_at` has
    /// passed but which still don't have a closure or a materialized
    /// lease row. Each match gets a `'deadline_expired'` closure
    /// written and is returned for the maintenance caller to convert
    /// into a deferred / DLQ row, exactly as the lease-side path does.
    ///
    /// The two anti-joins mirror `rescue_stale_receipt_claims_tx`'s
    /// disambiguation: a claim that has already materialized into
    /// `leases` is on the lease-side deadline-rescue path, and
    /// rescuing it here would double-close it.
    ///
    /// Unlike the lease-side scan, receipt deadline rescue walks each
    /// claim partition through a tiny cursor ordered by deadline. That
    /// keeps a long MVCC horizon from making every maintenance tick
    /// re-prove old successful receipt completions until claim prune
    /// can truncate the partition.
    async fn rescue_expired_receipt_deadlines_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        options: &RescueBatchOptions,
        excluded: &BTreeSet<(i64, i64)>,
        selected: Option<&BTreeSet<(i64, i64)>>,
        batch: &mut ReceiptRescueBatch,
    ) -> Result<(), AwaError> {
        let mut remaining = RECEIPT_RESCUE_BATCH_LIMIT;
        for slot in self.rescue_slot_order_tx(tx, options).await? {
            let (slot_rescued, scan_full, cursor) = self
                .rescue_expired_receipt_deadlines_for_slot_tx(
                    tx,
                    slot,
                    options
                        .cursor_scan_limit
                        .map_or(RECEIPT_DEADLINE_RESCUE_CURSOR_SCAN_LIMIT, |limit| {
                            limit.max(1) as i64
                        }),
                    remaining,
                    excluded,
                    selected,
                )
                .await?;
            batch.saturated |= scan_full;
            if !scan_full && slot_rescued.is_empty() {
                batch.idle_slots.push(slot);
            }
            batch.scan_progress.push(RescueScanProgress {
                slot,
                cursors: vec![RescueScanCursor {
                    position: cursor,
                    saturated: scan_full,
                }],
            });
            remaining = remaining.saturating_sub(slot_rescued.len() as i64);
            batch.rescued.extend(slot_rescued);
            if remaining == 0 {
                batch.saturated = true;
                break;
            }
        }
        Ok(())
    }

    /// The oldest initialized claim slot, and every slot ordered from it
    /// around the ring to the current one, then any not yet initialized.
    async fn claim_slots_by_age_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    ) -> Result<(Option<i32>, Vec<i32>), AwaError> {
        let schema = self.schema();
        let ring_cursor = sqlx::query_as::<_, (i32, i64, i32)>(&format!(
            r#"
            SELECT current_slot, generation, slot_count
            FROM {schema}.claim_ring_state
            WHERE singleton = TRUE
            "#
        ))
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        let slot_count = self.claim_slot_count() as i32;
        let Some((current_slot, generation, ring_slot_count)) = ring_cursor else {
            return Ok((None, (0..slot_count).collect()));
        };
        let oldest = oldest_initialized_ring_slot(current_slot, generation, ring_slot_count)
            .map(|(slot, _generation)| slot)
            .filter(|slot| (0..slot_count).contains(slot));
        let start = oldest.unwrap_or(current_slot).rem_euclid(slot_count);
        let mut by_age: Vec<i32> = Vec::with_capacity(slot_count as usize);
        let mut slot = start;
        loop {
            by_age.push(slot);
            if slot == current_slot.rem_euclid(slot_count) || by_age.len() == slot_count as usize {
                break;
            }
            slot = (slot + 1).rem_euclid(slot_count);
        }
        let uninitialized: Vec<i32> = (0..slot_count)
            .filter(|slot| !by_age.contains(slot))
            .collect();
        by_age.extend(uninitialized);
        Ok((oldest, by_age))
    }

    async fn rescue_expired_receipt_deadlines_for_slot_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        slot: i32,
        scan_limit: i64,
        rescue_limit: i64,
        excluded: &BTreeSet<(i64, i64)>,
        selected: Option<&BTreeSet<(i64, i64)>>,
    ) -> Result<ReceiptRescueScan, AwaError> {
        if rescue_limit <= 0 {
            return Ok((Vec::new(), false, None));
        }

        let (excluded_job_ids, excluded_run_leases): (Vec<_>, Vec<_>) =
            excluded.iter().copied().unzip();
        let (selected_job_ids, selected_run_leases): (Vec<i64>, Vec<i64>) = selected
            .map(|pairs| pairs.iter().copied().unzip())
            .unwrap_or_default();
        let schema = self.schema();
        let claim_child = claim_child_name(schema, slot as usize);
        let closure_child = closure_child_name(schema, slot as usize);
        let closure_batch_child = claim_closure_batch_child_name(schema, slot as usize);
        let closed_evidence =
            receipt_closed_evidence_sql(schema, &closure_child, &closure_batch_child, "claims");
        let candidate_ctes = format!(
            r#"
            cursor_row AS (
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
                WHERE claims.claim_slot = $1
                  AND claims.deadline_at IS NOT NULL
                  AND claims.deadline_at < clock_timestamp()
                  AND (claims.deadline_at, claims.job_id, claims.run_lease)
                      > (
                          (SELECT deadline_cursor_deadline_at FROM cursor_row),
                          (SELECT deadline_cursor_job_id FROM cursor_row),
                          (SELECT deadline_cursor_run_lease FROM cursor_row)
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
                CROSS JOIN after_count
                WHERE after_count.count < $2
                  AND claims.claim_slot = $1
                  AND claims.deadline_at IS NOT NULL
                  AND claims.deadline_at < clock_timestamp()
                  AND (claims.deadline_at, claims.job_id, claims.run_lease)
                      <= (
                          (SELECT deadline_cursor_deadline_at FROM cursor_row),
                          (SELECT deadline_cursor_job_id FROM cursor_row),
                          (SELECT deadline_cursor_run_lease FROM cursor_row)
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
                  AND NOT EXISTS (
                      SELECT 1 FROM unnest($4::bigint[], $5::bigint[]) AS excluded(job_id, run_lease)
                      WHERE excluded.job_id = candidates.job_id AND excluded.run_lease = candidates.run_lease
                  )
                  AND ($6::boolean OR EXISTS (
                      SELECT 1 FROM unnest($7::bigint[], $8::bigint[]) AS selected(job_id, run_lease)
                      WHERE selected.job_id = candidates.job_id AND selected.run_lease = candidates.run_lease
                  ))
                ORDER BY rn
                LIMIT $3
            )"#
        );
        // Lock before deciding; see `rescue_stale_receipt_claims_for_slot_tx`.
        let (locked_job_ids, locked_run_leases): (Vec<i64>, Vec<i64>) =
            sqlx::query_as::<_, (i64, i64)>(&format!(
                r#"
                WITH {candidate_ctes}
                SELECT job_id, run_lease
                FROM expired_candidates
                WHERE pg_catalog.pg_try_advisory_xact_lock(
                    pg_catalog.hashtextextended(
                        format('awa.receipt.complete:%s:%s', job_id, run_lease),
                        0
                    )
                )
                "#
            ))
            .persistent(false)
            .bind(slot)
            .bind(scan_limit)
            .bind(rescue_limit)
            .bind(&excluded_job_ids)
            .bind(&excluded_run_leases)
            .bind(selected.is_none())
            .bind(&selected_job_ids)
            .bind(&selected_run_leases)
            .fetch_all(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?
            .into_iter()
            .unzip();
        let (rescued, scan_full, cursor_at, cursor_job_id, cursor_run_lease): ReceiptRescueScanRow = sqlx::query_as(&format!(
            r#"
            WITH {candidate_ctes},
            expired_locked AS (
                SELECT expired_candidates.*
                FROM expired_candidates
                JOIN {claim_child} AS claims
                  ON claims.claim_slot = expired_candidates.claim_slot
                 AND claims.job_id = expired_candidates.job_id
                 AND claims.run_lease = expired_candidates.run_lease
                WHERE NOT {closed_evidence}
                  AND claims.deadline_at < clock_timestamp()
                  AND (claims.job_id, claims.run_lease) IN (
                      SELECT locked.job_id, locked.run_lease
                      FROM unnest($9::bigint[], $10::bigint[]) AS locked(job_id, run_lease)
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
                UPDATE {claim_child} AS claims
                SET closed_at = COALESCE(claims.closed_at, inserted.closed_at)
                FROM inserted
                WHERE claims.job_id = inserted.job_id
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
                RETURNING slots.deadline_cursor_deadline_at, slots.deadline_cursor_job_id, slots.deadline_cursor_run_lease
            ),
            rescued_rows AS (
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
            )
            SELECT
                COALESCE((SELECT jsonb_agg(to_jsonb(rescued_rows)) FROM rescued_rows), '[]'::jsonb),
                (SELECT count(*) >= $2 FROM candidates),
                (SELECT deadline_cursor_deadline_at FROM advance_cursor),
                (SELECT deadline_cursor_job_id FROM advance_cursor),
                (SELECT deadline_cursor_run_lease FROM advance_cursor)
            "#
        ))
        .persistent(false)
        .bind(slot)
        .bind(scan_limit)
        .bind(rescue_limit)
        .bind(&excluded_job_ids)
        .bind(&excluded_run_leases)
        .bind(selected.is_none())
        .bind(&selected_job_ids)
        .bind(&selected_run_leases)
        .bind(&locked_job_ids)
        .bind(&locked_run_leases)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        let scan_full = scan_full || rescued.0.len() as i64 >= rescue_limit;
        Ok((
            rescued.0,
            scan_full,
            cursor_at
                .zip(cursor_job_id)
                .zip(cursor_run_lease)
                .map(|((at, id), lease)| (at, id, lease)),
        ))
    }

    async fn active_job_read_row_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        row: sqlx::postgres::PgRow,
    ) -> Result<Option<JobRow>, AwaError> {
        let body_job_id: Option<i64> = row.try_get("body_job_id").map_err(map_sqlx_error)?;
        if body_job_id.is_some() {
            return LeaseJobRow::from_row(&row)
                .map_err(map_sqlx_error)?
                .into_job_row()
                .map(Some);
        }
        let reference = DeletedLeaseRow::from_row(&row).map_err(map_sqlx_error)?;
        let (ready_map, claimed_lanes) = self
            .load_ready_snapshot_candidates_tx(tx, std::slice::from_ref(&reference))
            .await?;
        let key = (
            reference.ready_slot,
            reference.ready_generation,
            reference.queue.clone(),
            reference.enqueue_shard,
            reference.lane_seq,
            reference.job_id,
            reference.run_lease,
        );
        let Some(candidates) = ready_map.get(&key) else {
            return Ok(None);
        };
        let (ready, _) = select_claimed_ready_body(candidates, &claimed_lanes, &reference)?;
        let mut payload = ready.payload.clone();
        if reference.priority < ready.priority {
            let metadata = payload
                .as_object_mut()
                .ok_or_else(|| {
                    AwaError::Validation("queue storage payload must be a JSON object".into())
                })?
                .entry("metadata")
                .or_insert_with(|| serde_json::json!({}))
                .as_object_mut()
                .ok_or_else(|| {
                    AwaError::Validation(
                        "queue storage payload metadata must be a JSON object".into(),
                    )
                })?;
            metadata
                .entry("_awa_original_priority")
                .or_insert_with(|| serde_json::Value::from(i64::from(ready.priority)));
        }
        LeaseJobRow {
            job_id: reference.job_id,
            kind: ready.kind.clone(),
            queue: ready.queue.clone(),
            args: ready.args.clone(),
            state: reference.state,
            priority: reference.priority,
            attempt: reference.attempt,
            run_lease: reference.run_lease,
            max_attempts: reference.max_attempts,
            run_at: ready.run_at,
            heartbeat_at: row.try_get("heartbeat_at").map_err(map_sqlx_error)?,
            deadline_at: row.try_get("deadline_at").map_err(map_sqlx_error)?,
            attempted_at: reference.attempted_at,
            finalized_at: None,
            created_at: ready.created_at,
            unique_key: ready.unique_key.clone(),
            callback_id: row.try_get("callback_id").map_err(map_sqlx_error)?,
            callback_timeout_at: row.try_get("callback_timeout_at").map_err(map_sqlx_error)?,
            callback_filter: row.try_get("callback_filter").map_err(map_sqlx_error)?,
            callback_on_complete: row
                .try_get("callback_on_complete")
                .map_err(map_sqlx_error)?,
            callback_on_fail: row.try_get("callback_on_fail").map_err(map_sqlx_error)?,
            callback_transform: row.try_get("callback_transform").map_err(map_sqlx_error)?,
            payload,
            progress: row.try_get("progress").map_err(map_sqlx_error)?,
            callback_result: row.try_get("callback_result").map_err(map_sqlx_error)?,
        }
        .into_job_row()
        .map(Some)
    }

    pub async fn load_job(&self, pool: &PgPool, job_id: i64) -> Result<Option<JobRow>, AwaError> {
        crate::read_transaction::read_until_untruncated(|guard| {
            self.load_job_in_snapshot(pool, job_id, guard)
        })
        .await
    }

    /// The job's newest row, and whether no ring partition was truncated
    /// while the read's snapshot was in use.
    async fn load_job_in_snapshot(
        &self,
        pool: &PgPool,
        job_id: i64,
        guard: crate::read_transaction::TruncationGuard,
    ) -> Result<(Option<JobRow>, bool), AwaError> {
        let schema = self.schema();
        let closure_rel = format!("{schema}.lease_claim_closures");
        let closure_batch_rel = format!("{schema}.lease_claim_closure_batches");
        let closed_evidence =
            receipt_closed_evidence_sql(schema, &closure_rel, &closure_batch_rel, "claims");
        let mut candidates = Vec::new();
        // A job moves between these relations as it is promoted, claimed,
        // completed or dead-lettered. Read them all from one snapshot so a
        // move between two reads cannot hide the job or surface a stale row.
        // Keep the located ready-body partitions locked until commit; later
        // read savepoints release unrelated relation locks after each leg.
        let mut begin_guard = crate::read_transaction::BeginGuard::acquire(pool)
            .await
            .map_err(map_sqlx_error)?;
        let snapshot = crate::read_transaction::GuardedSnapshot::before_begin(
            &mut begin_guard.connection,
            schema,
            guard,
        )
        .await
        .map_err(map_sqlx_error)?;
        let mut tx = sqlx::Connection::begin_with(
            &mut *begin_guard.connection,
            "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        )
        .await
        .map_err(map_sqlx_error)?;
        begin_guard.begin_pending = false;
        snapshot
            .after_begin(tx.as_mut(), schema)
            .await
            .map_err(map_sqlx_error)?;

        sqlx::query("SAVEPOINT awa_read_leg")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        let ready_slots: Vec<i32> = sqlx::query_scalar(&format!(
            "SELECT DISTINCT ready_slot FROM {schema}.ready_entries WHERE job_id = $1"
        ))
        .bind(job_id)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        sqlx::Executor::execute(
            tx.as_mut(),
            "ROLLBACK TO SAVEPOINT awa_read_leg; RELEASE SAVEPOINT awa_read_leg",
        )
        .await
        .map_err(map_sqlx_error)?;
        let ready_slots = ready_slots
            .into_iter()
            .map(|slot| ring_slot_number(slot, "queue"))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let child_sources = |tombstones: bool| {
            if ready_slots.is_empty() {
                let table = if tombstones {
                    "ready_tombstones"
                } else {
                    "ready_entries"
                };
                format!("SELECT * FROM ONLY {schema}.{table} WHERE FALSE")
            } else {
                ready_slots
                    .iter()
                    .map(|&slot| {
                        let child = if tombstones {
                            ready_tombstone_child_name(schema, slot)
                        } else {
                            ready_child_name(schema, slot)
                        };
                        format!("SELECT * FROM {child}")
                    })
                    .collect::<Vec<_>>()
                    .join(" UNION ALL ")
            }
        };
        let ready_sources = child_sources(false);
        let tombstone_sources = child_sources(true);
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
            FROM ({ready_sources}) AS ready
            WHERE ready.job_id = $1
              AND NOT EXISTS (
                  SELECT 1 FROM ({tombstone_sources}) AS tomb
                  WHERE tomb.ready_slot = ready.ready_slot
                    AND tomb.ready_generation = ready.ready_generation
                    AND tomb.queue = ready.queue
                    AND tomb.priority = ready.priority
                    AND tomb.enqueue_shard = ready.enqueue_shard
                    AND tomb.lane_seq = ready.lane_seq
              )
            ORDER BY run_lease DESC, attempted_at DESC NULLS LAST, run_at DESC
            "#,
        ))
        .bind(job_id)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        for row in ready_rows {
            candidates.push(row.into_job_row()?);
        }

        sqlx::query("SAVEPOINT awa_read_leg")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
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
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        sqlx::Executor::execute(
            tx.as_mut(),
            "ROLLBACK TO SAVEPOINT awa_read_leg; RELEASE SAVEPOINT awa_read_leg",
        )
        .await
        .map_err(map_sqlx_error)?;
        for row in deferred_rows {
            candidates.push(row.into_job_row()?);
        }

        sqlx::query("SAVEPOINT awa_read_leg")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        let lease_rows = sqlx::query(&format!(
            r#"
            SELECT
                ready.job_id AS body_job_id,
                lease.ready_slot,
                lease.ready_generation,
                lease.job_id,
                lease.enqueue_shard,
                ready.kind,
                lease.queue,
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
            LEFT JOIN ({ready_sources}) AS ready
              ON ready.ready_slot = lease.ready_slot
             AND ready.ready_generation = lease.ready_generation
             AND ready.queue = lease.queue
             -- Claims and leases record the aged effective priority while the
             -- ready row keeps its lane priority, so match the attempt instead.
             AND ready.job_id = lease.job_id
             AND ready.run_lease + 1 = lease.run_lease
             AND ready.enqueue_shard = lease.enqueue_shard
             AND ready.lane_seq = lease.lane_seq
             AND NOT EXISTS (
                 SELECT 1 FROM ({tombstone_sources}) AS tomb
                 WHERE tomb.ready_slot = ready.ready_slot
                   AND tomb.ready_generation = ready.ready_generation
                   AND tomb.queue = ready.queue
                   AND tomb.priority = ready.priority
                   AND tomb.enqueue_shard = ready.enqueue_shard
                   AND tomb.lane_seq = ready.lane_seq
             )
            LEFT JOIN {schema}.attempt_state AS attempt
              ON attempt.job_id = lease.job_id
             AND attempt.run_lease = lease.run_lease
            WHERE lease.job_id = $1
            ORDER BY lease.run_lease DESC
            "#,
        ))
        .bind(job_id)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        for row in lease_rows {
            if let Some(job) = self.active_job_read_row_tx(&mut tx, row).await? {
                candidates.push(job);
            }
        }

        sqlx::Executor::execute(
            tx.as_mut(),
            "ROLLBACK TO SAVEPOINT awa_read_leg; RELEASE SAVEPOINT awa_read_leg",
        )
        .await
        .map_err(map_sqlx_error)?;

        // Report receipt-backed attempts as running by anti-joining
        // lease_claims against every durable closure evidence shape.
        sqlx::query("SAVEPOINT awa_read_leg")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        let lease_claim_rows = sqlx::query(&format!(
            r#"
            SELECT
                ready.job_id AS body_job_id,
                claims.ready_slot,
                claims.ready_generation,
                claims.job_id,
                claims.enqueue_shard,
                ready.kind,
                claims.queue,
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
            LEFT JOIN ({ready_sources}) AS ready
              ON ready.ready_slot = claims.ready_slot
             AND ready.ready_generation = claims.ready_generation
             AND ready.queue = claims.queue
             -- Claims and leases record the aged effective priority while the
             -- ready row keeps its lane priority, so match the attempt instead.
             AND ready.job_id = claims.job_id
             AND ready.run_lease + 1 = claims.run_lease
             AND ready.enqueue_shard = claims.enqueue_shard
             AND ready.lane_seq = claims.lane_seq
             AND NOT EXISTS (
                 SELECT 1 FROM ({tombstone_sources}) AS tomb
                 WHERE tomb.ready_slot = ready.ready_slot
                   AND tomb.ready_generation = ready.ready_generation
                   AND tomb.queue = ready.queue
                   AND tomb.priority = ready.priority
                   AND tomb.enqueue_shard = ready.enqueue_shard
                   AND tomb.lane_seq = ready.lane_seq
             )
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
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        for row in lease_claim_rows {
            if let Some(job) = self.active_job_read_row_tx(&mut tx, row).await? {
                candidates.push(job);
            }
        }

        sqlx::Executor::execute(
            tx.as_mut(),
            "ROLLBACK TO SAVEPOINT awa_read_leg; RELEASE SAVEPOINT awa_read_leg",
        )
        .await
        .map_err(map_sqlx_error)?;

        // Zero-deadline receipt claims are stored as compact batches. Expand
        // them only for this admin read, and report still-open items as
        // running until durable closure, terminal, or materialized-lease
        // evidence supersedes the claim.
        sqlx::query("SAVEPOINT awa_read_leg")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        let lease_claim_batch_rows = sqlx::query(&format!(
            r#"
            SELECT
                ready.job_id AS body_job_id,
                claim_batches.ready_slot,
                claim_batches.ready_generation,
                items.job_id,
                claim_batches.enqueue_shard,
                ready.kind,
                claim_batches.queue,
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
            LEFT JOIN ({ready_sources}) AS ready
              ON ready.ready_slot = claim_batches.ready_slot
             AND ready.ready_generation = claim_batches.ready_generation
             AND ready.queue = claim_batches.queue
             AND ready.enqueue_shard = claim_batches.enqueue_shard
             AND ready.lane_seq = items.lane_seq
             AND ready.job_id = items.job_id
             AND ready.run_lease + 1 = items.run_lease
             AND NOT EXISTS (
                 SELECT 1 FROM ({tombstone_sources}) AS tomb
                 WHERE tomb.ready_slot = ready.ready_slot
                   AND tomb.ready_generation = ready.ready_generation
                   AND tomb.queue = ready.queue
                   AND tomb.priority = ready.priority
                   AND tomb.enqueue_shard = ready.enqueue_shard
                   AND tomb.lane_seq = ready.lane_seq
             )
            LEFT JOIN {schema}.attempt_state AS attempt
              ON attempt.job_id = items.job_id
             AND attempt.run_lease = items.run_lease
            WHERE claim_batches.job_ids @> ARRAY[$1::bigint]
              AND items.job_id = $1
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
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        for row in lease_claim_batch_rows {
            if let Some(job) = self.active_job_read_row_tx(&mut tx, row).await? {
                candidates.push(job);
            }
        }

        sqlx::Executor::execute(
            tx.as_mut(),
            "ROLLBACK TO SAVEPOINT awa_read_leg; RELEASE SAVEPOINT awa_read_leg",
        )
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query("SAVEPOINT awa_read_leg")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
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
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        sqlx::Executor::execute(
            tx.as_mut(),
            "ROLLBACK TO SAVEPOINT awa_read_leg; RELEASE SAVEPOINT awa_read_leg",
        )
        .await
        .map_err(map_sqlx_error)?;
        for row in done_rows {
            candidates.push(row.into_job_row()?);
        }

        sqlx::query("SAVEPOINT awa_read_leg")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
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
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        sqlx::Executor::execute(
            tx.as_mut(),
            "ROLLBACK TO SAVEPOINT awa_read_leg; RELEASE SAVEPOINT awa_read_leg",
        )
        .await
        .map_err(map_sqlx_error)?;
        for row in dlq_rows {
            candidates.push(row.into_job_row()?);
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        let untruncated = snapshot
            .untruncated(&mut begin_guard.connection, schema)
            .await
            .map_err(map_sqlx_error)?;
        drop(begin_guard);
        let job = candidates.into_iter().max_by_key(|job| {
            (
                job.run_lease,
                transition_timestamp(job),
                state_rank(job.state),
            )
        });
        Ok((job, untruncated))
    }

    pub async fn register_callback(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        timeout: Duration,
    ) -> Result<Uuid, AwaError> {
        let callback_id = Uuid::new_v4();
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let row = sqlx::query(&format!(
            r#"
            SELECT
                ready.job_id AS body_job_id,
                lease.ready_slot,
                lease.ready_generation,
                lease.job_id,
                lease.enqueue_shard,
                ready.kind,
                lease.queue,
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
            LEFT JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = lease.ready_slot
             AND ready.ready_generation = lease.ready_generation
             AND ready.queue = lease.queue
             -- Claims and leases record the aged effective priority while the
             -- ready row keeps its lane priority, so match the attempt instead.
             AND ready.job_id = lease.job_id
             AND ready.run_lease + 1 = lease.run_lease
             AND ready.enqueue_shard = lease.enqueue_shard
             AND ready.lane_seq = lease.lane_seq
             AND NOT EXISTS (
                 SELECT 1 FROM {schema}.ready_tombstones AS tomb
                 WHERE tomb.ready_slot = ready.ready_slot
                   AND tomb.ready_generation = ready.ready_generation
                   AND tomb.queue = ready.queue
                   AND tomb.priority = ready.priority
                   AND tomb.enqueue_shard = ready.enqueue_shard
                   AND tomb.lane_seq = ready.lane_seq
             )
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
        match row {
            Some(row) => self.active_job_read_row_tx(tx, row).await,
            None => Ok(None),
        }
    }

    pub async fn enter_callback_wait(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        callback_id: Uuid,
    ) -> Result<bool, AwaError> {
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let row = sqlx::query(&format!(
            r#"
            SELECT
                ready.job_id AS body_job_id,
                lease.ready_slot,
                lease.ready_generation,
                lease.job_id,
                lease.enqueue_shard,
                ready.kind,
                lease.queue,
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
            LEFT JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = lease.ready_slot
             AND ready.ready_generation = lease.ready_generation
             AND ready.queue = lease.queue
             -- Claims and leases record the aged effective priority while the
             -- ready row keeps its lane priority, so match the attempt instead.
             AND ready.job_id = lease.job_id
             AND ready.run_lease + 1 = lease.run_lease
             AND ready.enqueue_shard = lease.enqueue_shard
             AND ready.lane_seq = lease.lane_seq
             AND NOT EXISTS (
                 SELECT 1 FROM {schema}.ready_tombstones AS tomb
                 WHERE tomb.ready_slot = ready.ready_slot
                   AND tomb.ready_generation = ready.ready_generation
                   AND tomb.queue = ready.queue
                   AND tomb.priority = ready.priority
                   AND tomb.enqueue_shard = ready.enqueue_shard
                   AND tomb.lane_seq = ready.lane_seq
             )
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

        match row {
            Some(row) => self.active_job_read_row_tx(tx, row).await,
            None => Ok(None),
        }
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        payload.push_error(caller_error_entry(error_entry, moved.attempt));
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        if self.lease_claim_receipts() {
            self.upsert_attempt_state_progress_from_receipts_tx(
                &mut tx,
                &[(job_id, run_lease, progress.clone())],
                false,
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

    /// Heartbeat compact receipt claims by their batch identity. Each claim
    /// batch is addressed by primary key and the attempt by item index, with
    /// index-only closure probes and no batch row lock, so the cost is
    /// O(in-flight attempts) rather than O(open claim batches). Attempts that
    /// a closer holds right now are skipped rather than waited for.
    pub async fn heartbeat_claims_batch(
        &self,
        pool: &PgPool,
        claims: &[ClaimHeartbeatRef],
    ) -> Result<usize, AwaError> {
        if claims.is_empty() {
            return Ok(0);
        }
        let schema = self.schema();
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let pairs: Vec<(i64, i64)> = claims
            .iter()
            .map(|claim| (claim.job_id, claim.run_lease))
            .collect();
        let locked: HashSet<(i64, i64)> = self
            .try_lock_receipt_attempt_keys_tx(&mut tx, &pairs)
            .await?
            .into_iter()
            .collect();
        let claims: Vec<&ClaimHeartbeatRef> = claims
            .iter()
            .filter(|claim| locked.contains(&(claim.job_id, claim.run_lease)))
            .collect();
        if claims.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(0);
        }
        let job_ids: Vec<i64> = claims.iter().map(|claim| claim.job_id).collect();
        let run_leases: Vec<i64> = claims.iter().map(|claim| claim.run_lease).collect();
        let claim_slots: Vec<i32> = claims.iter().map(|claim| claim.claim_slot).collect();
        let batch_ids: Vec<i64> = claims.iter().map(|claim| claim.claim_batch_id).collect();
        let batch_indices: Vec<i32> = claims.iter().map(|claim| claim.claim_batch_index).collect();
        let receipt_ids: Vec<i64> = claims.iter().map(|claim| claim.receipt_id).collect();
        let updated: i64 = sqlx::query_scalar(&format!(
            r#"
            WITH inflight(job_id, run_lease, claim_slot, batch_id, batch_index, receipt_id) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::int[], $4::bigint[], $5::int[], $6::bigint[])
            ),
            open_claims AS (
                SELECT inflight.job_id, inflight.run_lease
                FROM inflight
                JOIN {schema}.lease_claim_batches AS claim_batches
                  ON claim_batches.claim_slot = inflight.claim_slot
                 AND claim_batches.batch_id = inflight.batch_id
                WHERE inflight.batch_index BETWEEN 1 AND claim_batches.claimed_count
                  AND claim_batches.job_ids[inflight.batch_index] = inflight.job_id
                  AND claim_batches.run_leases[inflight.batch_index] = inflight.run_lease
                  AND claim_batches.receipt_ids[inflight.batch_index] = inflight.receipt_id
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.lease_claim_closures AS closures
                      WHERE closures.claim_slot = inflight.claim_slot
                        AND closures.job_id = inflight.job_id
                        AND closures.run_lease = inflight.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.lease_claim_closure_batches AS closure_batches
                      WHERE closure_batches.claim_slot = inflight.claim_slot
                        AND closure_batches.receipt_ranges @> inflight.receipt_id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.done_entries AS done
                      WHERE done.job_id = inflight.job_id AND done.run_lease = inflight.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.deferred_jobs AS deferred
                      WHERE deferred.job_id = inflight.job_id AND deferred.run_lease = inflight.run_lease
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.dlq_entries AS dlq
                      WHERE dlq.job_id = inflight.job_id AND dlq.run_lease = inflight.run_lease
                  )
            ),
            upserted AS (
                INSERT INTO {schema}.attempt_state (job_id, run_lease, heartbeat_at, updated_at)
                SELECT open_claims.job_id, open_claims.run_lease, clock_timestamp(), clock_timestamp()
                FROM open_claims
                ON CONFLICT (job_id, run_lease)
                DO UPDATE SET
                    heartbeat_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                RETURNING job_id
            )
            SELECT count(*)::bigint FROM upserted
            "#
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .bind(&claim_slots)
        .bind(&batch_ids)
        .bind(&batch_indices)
        .bind(&receipt_ids)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(updated as usize)
    }

    /// Lock legacy lease rows without waiting behind a finalizer. Keep this
    /// separate from the updating statement: the latter must use a fresh
    /// snapshot and must not mix partition pruning with row locking (PG18).
    async fn lock_live_leases_for_heartbeat_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        jobs: &[(i64, i64)],
    ) -> Result<Vec<(i64, i64)>, AwaError> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        let job_ids: Vec<i64> = jobs.iter().map(|(job_id, _)| *job_id).collect();
        let run_leases: Vec<i64> = jobs.iter().map(|(_, run_lease)| *run_lease).collect();
        sqlx::query_as::<_, (i64, i64)>(&format!(
            r#"
            SELECT lease.job_id, lease.run_lease
            FROM {table} AS lease
            JOIN unnest($1::bigint[], $2::bigint[]) AS input(job_id, run_lease)
              ON lease.job_id = input.job_id AND lease.run_lease = input.run_lease
            WHERE lease.state = 'running'
            ORDER BY lease.job_id, lease.run_lease
            FOR UPDATE OF lease SKIP LOCKED
            "#,
            table = self.leases_table(),
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)
    }

    pub async fn heartbeat_batch(
        &self,
        pool: &PgPool,
        jobs: &[(i64, i64)],
    ) -> Result<usize, AwaError> {
        if jobs.is_empty() {
            return Ok(0);
        }

        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
            let locked = self.try_lock_receipt_attempt_keys_tx(&mut tx, jobs).await?;
            updated += self
                .upsert_attempt_state_from_receipts_tx(&mut tx, &locked)
                .await?;
        } else {
            // Legacy non-receipts mode (custom schemas with
            // `lease_claim_receipts=FALSE`): the `leases.heartbeat_at`
            // write is still the heartbeat home, since there is no
            // upsert_attempt_state path firing.
            let jobs = self
                .lock_live_leases_for_heartbeat_tx(&mut tx, jobs)
                .await?;
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

        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
            self.upsert_attempt_state_progress_from_receipts_tx(&mut tx, jobs, true)
                .await?
        } else {
            // Prelock independently so a finalizer cannot stall the entire
            // heartbeat pass. Skipped progress snapshots remain unacknowledged
            // in the worker and are retried on the next pass.
            let keys: Vec<_> = jobs.iter().map(|(id, lease, _)| (*id, *lease)).collect();
            let locked: HashSet<_> = self
                .lock_live_leases_for_heartbeat_tx(&mut tx, &keys)
                .await?
                .into_iter()
                .collect();
            let jobs: Vec<_> = jobs
                .iter()
                .filter(|(id, lease, _)| locked.contains(&(*id, *lease)))
                .collect();
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let row = self
            .insert_deferred_or_cancel_duplicate_tx(tx, moved, deferred, RETRIED_DUPLICATE_ERROR)
            .await?;
        Ok(Some(row))
    }

    pub async fn snooze(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        snooze_for: Duration,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let row = self
            .insert_deferred_or_cancel_duplicate_tx(
                &mut tx,
                moved,
                deferred,
                SNOOZED_DUPLICATE_ERROR,
            )
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(row))
    }

    /// Insert a canonical attempt's transition-time deferred successor,
    /// degrading to a cancelled terminal row if a newer duplicate already
    /// owns the unique claim. Canonical deletion releases any claim before
    /// this call, so the deferred insert must acquire from `None` even when
    /// the old `running` state was inside the mask.
    pub(crate) async fn insert_migrated_deferred_or_cancel_duplicate_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        row: JobRow,
        unique_states: Option<String>,
        duplicate_error: &str,
    ) -> Result<JobRow, AwaError> {
        let payload = Self::payload_from_parts(row.metadata, row.tags, row.errors, row.progress)?;
        let deferred = DeferredJobRow {
            job_id: row.id,
            kind: row.kind,
            queue: row.queue,
            args: row.args,
            state: row.state,
            priority: row.priority,
            attempt: row.attempt,
            run_lease: row.run_lease,
            max_attempts: row.max_attempts,
            run_at: row.run_at,
            attempted_at: row.attempted_at,
            finalized_at: row.finalized_at,
            created_at: row.created_at,
            unique_key: row.unique_key,
            unique_states,
            payload,
        };

        {
            // isolation: a savepoint, at the enclosing transaction's level
            let mut savepoint = tx.begin().await.map_err(map_sqlx_error)?;
            match self
                .insert_deferred_rows_tx(&mut savepoint, vec![deferred.clone()], None)
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

        let mut cancelled_payload = RuntimePayload::from_json(deferred.payload.clone())?;
        cancelled_payload.push_error(lifecycle_error(duplicate_error, deferred.attempt, true));
        let (ready_slot, ready_generation) = self.current_queue_ring(tx).await?;
        let mut done = DoneJobRow {
            ready_slot,
            ready_generation,
            job_id: deferred.job_id,
            kind: deferred.kind,
            queue: deferred.queue,
            args: deferred.args,
            state: JobState::Cancelled,
            priority: deferred.priority,
            attempt: deferred.attempt,
            run_lease: deferred.run_lease,
            max_attempts: deferred.max_attempts,
            // The successor never entered a ready lane. Mirror deferred-job
            // cancellation with a synthetic negative sequence on shard 0.
            lane_seq: -deferred.job_id,
            enqueue_shard: 0,
            run_at: deferred.run_at,
            attempted_at: deferred.attempted_at,
            finalized_at: self.current_timestamp_tx(tx).await?,
            created_at: deferred.created_at,
            unique_key: deferred.unique_key,
            unique_states: deferred.unique_states,
            payload: cancelled_payload.into_json(),
        };

        let claimed = {
            // isolation: a savepoint, at the enclosing transaction's level
            let mut savepoint = tx.begin().await.map_err(map_sqlx_error)?;
            match self
                .insert_done_rows_tx(&mut savepoint, std::slice::from_ref(&done), None)
                .await
            {
                Ok(_) => {
                    savepoint.commit().await.map_err(map_sqlx_error)?;
                    true
                }
                Err(AwaError::UniqueConflict { .. }) => {
                    savepoint.rollback().await.map_err(map_sqlx_error)?;
                    false
                }
                Err(err) => return Err(err),
            }
        };

        if !claimed {
            // If `cancelled` itself claims uniqueness, the newer holder still
            // wins. The row keeps its key, so a retry re-checks ownership.
            clear_cancelled_claim(&mut done)?;
            self.insert_done_rows_tx(tx, std::slice::from_ref(&done), None)
                .await?;
        }
        self.ensure_lane(tx, &done.queue, done.priority, 0).await?;
        done.into_job_row()
    }

    /// Write `deferred` as the successor of the attempt `row`, or, when
    /// another job holds its unique key, finish the attempt as a cancelled
    /// duplicate with `duplicate_error` instead: the claim holder wins. The
    /// claim change is applied in one pass before any row is written, as
    /// every queue-storage path does.
    async fn insert_deferred_or_cancel_duplicate_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        row: LeaseTransitionRow,
        deferred: DeferredJobRow,
        duplicate_error: &str,
    ) -> Result<JobRow, AwaError> {
        let changes =
            deferred_claim_changes(std::slice::from_ref(&deferred), Some(row.state)).collect();
        let conflicts = self
            .apply_unique_claim_changes_tx(tx, changes, true)
            .await?;
        if !conflicts.contains(&row.job_id) {
            self.write_deferred_rows_tx(tx, std::slice::from_ref(&deferred))
                .await?;
            return deferred.into_job_row();
        }
        let old_state = row.state;
        // The cancelled row takes the time the caller stamped the successor
        // with, truncated to what PostgreSQL stores so the returned row
        // matches the committed one.
        let finalized_at = deferred
            .finalized_at
            .unwrap_or_else(Utc::now)
            .trunc_subsecs(6);
        let (done, cleared) =
            duplicate_cancelled_row(row, deferred.payload, duplicate_error, finalized_at)?;
        // As in rescue: a row whose mask was cleared is written
        // self-contained, so it stores the mask it reports.
        let old_state = (!cleared).then_some(old_state);
        self.write_done_rows_tx(tx, std::slice::from_ref(&done), old_state)
            .await?;
        done.into_job_row()
    }

    /// Settle attempts that maintenance rescued (stale heartbeat, expired
    /// deadline or callback timeout): back to `retryable` with backoff while
    /// attempts remain, `failed` once the rescued attempt was the last. A job
    /// that kills or hangs its worker every time therefore stops after
    /// `max_attempts`, like one whose handler returns an error. Each batch
    /// pairs rescued attempts with the error recorded on them.
    ///
    /// An exhausted attempt on a queue whose DLQ is enabled goes straight to
    /// `dlq_entries` in this transaction, as `fail_to_dlq` does for a handler
    /// error, so there is no window in which it sits `failed` outside the DLQ.
    ///
    /// Every batch's unique claims change in one key-ordered pass. An attempt
    /// whose outcome claims a key a newer duplicate holds is cancelled
    /// instead: the claim holder wins, and one poisoned row must not abort
    /// the whole rescue transaction.
    async fn settle_rescued_attempts_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        batches: Vec<(Vec<LeaseTransitionRow>, &str)>,
        dlq: RescueDlq<'_>,
    ) -> Result<Vec<JobRow>, AwaError> {
        let rows: Vec<(LeaseTransitionRow, &str)> = batches
            .into_iter()
            .flat_map(|(rows, error)| rows.into_iter().map(move |row| (row, error)))
            .collect();
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let (retry_attempts, retry_max_attempts): (Vec<i16>, Vec<i16>) = rows
            .iter()
            .filter(|(row, _)| row.attempt < row.max_attempts)
            .map(|(row, _)| (row.attempt, row.max_attempts))
            .unzip();
        let mut retry_run_ats = self
            .backoffs_at_tx(tx, &retry_attempts, &retry_max_attempts)
            .await?
            .into_iter();

        // PostgreSQL keeps microseconds, so the rows returned match the rows
        // committed.
        let finalized_at = Utc::now().trunc_subsecs(6);
        let mut retries: Vec<(LeaseTransitionRow, DeferredJobRow)> = Vec::new();
        let mut failures: Vec<(LeaseTransitionRow, DoneJobRow)> = Vec::new();
        let mut dead_letters: Vec<(LeaseTransitionRow, DlqJobRow)> = Vec::new();
        for (row, error) in rows {
            let mut payload = RuntimePayload::from_json(Self::payload_with_attempt_state(
                row.payload.clone(),
                row.progress.clone(),
            )?)?;
            let exhausted = row.attempt >= row.max_attempts;
            payload.push_error(lifecycle_error(error, row.attempt, exhausted));
            if !exhausted {
                let run_at = retry_run_ats.next().ok_or_else(|| {
                    AwaError::Validation("rescue backoff missing for a retryable attempt".into())
                })?;
                let deferred = row.clone().into_deferred_row(
                    JobState::Retryable,
                    run_at,
                    Some(finalized_at),
                    payload.into_json(),
                );
                retries.push((row, deferred));
            } else if (dlq.enabled)(&row.queue) {
                let dead_letter = row.clone().into_dlq_row(
                    finalized_at,
                    payload.into_json(),
                    dlq.reason.to_string(),
                    finalized_at,
                );
                dead_letters.push((row, dead_letter));
            } else {
                let done =
                    row.clone()
                        .into_done_row(JobState::Failed, finalized_at, payload.into_json());
                failures.push((row, done));
            }
        }

        let mut changes = Vec::new();
        for (row, deferred) in &retries {
            changes.extend(deferred_claim_changes(
                std::slice::from_ref(deferred),
                Some(row.state),
            ));
        }
        for (row, done) in &failures {
            changes.extend(done_claim_changes(
                std::slice::from_ref(done),
                Some(row.state),
            ));
        }
        for (row, dead_letter) in &dead_letters {
            changes.extend(dlq_claim_changes(
                std::slice::from_ref(dead_letter),
                Some(row.state),
            ));
        }
        let conflicts = self
            .apply_unique_claim_changes_tx(tx, changes, true)
            .await?;

        let mut settled = Vec::with_capacity(retries.len() + failures.len() + dead_letters.len());
        let mut duplicates: Vec<(LeaseTransitionRow, (DoneJobRow, bool))> = Vec::new();
        let mut duplicate = |row: LeaseTransitionRow, payload: serde_json::Value| {
            let done = duplicate_cancelled_row(
                row.clone(),
                payload,
                RESCUED_DUPLICATE_ERROR,
                finalized_at,
            )?;
            duplicates.push((row, done));
            Ok::<_, AwaError>(())
        };
        let mut kept_retries = Vec::with_capacity(retries.len());
        for (row, deferred) in retries {
            if conflicts.contains(&row.job_id) {
                duplicate(row, deferred.payload)?;
            } else {
                kept_retries.push((row, deferred));
            }
        }
        let mut kept_failures = Vec::with_capacity(failures.len());
        for (row, done) in failures {
            if conflicts.contains(&row.job_id) {
                duplicate(row, done.payload)?;
            } else {
                kept_failures.push((row, done));
            }
        }
        let mut kept_dead_letters = Vec::with_capacity(dead_letters.len());
        for (row, dead_letter) in dead_letters {
            if conflicts.contains(&row.job_id) {
                duplicate(row, dead_letter.payload)?;
            } else {
                kept_dead_letters.push((row, dead_letter));
            }
        }

        let deferred: Vec<DeferredJobRow> = kept_retries
            .into_iter()
            .map(|(_, deferred)| deferred)
            .collect();
        self.write_deferred_rows_tx(tx, &deferred).await?;
        for deferred in deferred {
            settled.push(deferred.into_job_row()?);
        }
        for (old_state, group) in group_by_previous_state(kept_failures) {
            let done: Vec<DoneJobRow> = group.into_iter().map(|(_, done)| done).collect();
            self.write_done_rows_tx(tx, &done, Some(old_state)).await?;
            for done in done {
                settled.push(done.into_job_row()?);
            }
        }
        let dead_letters: Vec<DlqJobRow> = kept_dead_letters
            .into_iter()
            .map(|(_, dead_letter)| dead_letter)
            .collect();
        self.write_dlq_rows_tx(tx, &dead_letters).await?;
        for dead_letter in dead_letters {
            settled.push(dead_letter.into_job_row()?);
        }
        // A ready-backed done row takes its unique key and mask from the
        // retained ready row, so a duplicate whose mask was cleared is
        // written self-contained, as a rescheduled duplicate is, and stores
        // the mask it reports.
        for (old_state, group) in group_by_previous_state(duplicates) {
            let (cleared, kept): (Vec<_>, Vec<_>) = group
                .into_iter()
                .map(|(_, done)| done)
                .partition(|(_, cleared)| *cleared);
            let kept: Vec<DoneJobRow> = kept.into_iter().map(|(done, _)| done).collect();
            let cleared: Vec<DoneJobRow> = cleared.into_iter().map(|(done, _)| done).collect();
            self.write_done_rows_tx(tx, &kept, Some(old_state)).await?;
            self.write_done_rows_tx(tx, &cleared, None).await?;
            for done in kept.into_iter().chain(cleared) {
                settled.push(done.into_job_row()?);
            }
        }
        Ok(settled)
    }

    /// `clock_timestamp() + awa.backoff_duration(..)` for each attempt, in
    /// one round trip, in input order.
    async fn backoffs_at_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        attempts: &[i16],
        max_attempts: &[i16],
    ) -> Result<Vec<DateTime<Utc>>, AwaError> {
        if attempts.is_empty() {
            return Ok(Vec::new());
        }
        sqlx::query_scalar(
            r#"
            SELECT clock_timestamp() + awa.backoff_duration(attempts.attempt, attempts.max_attempts)
            FROM unnest($1::smallint[], $2::smallint[])
                WITH ORDINALITY AS attempts(attempt, max_attempts, ord)
            ORDER BY attempts.ord
            "#,
        )
        .bind(attempts)
        .bind(max_attempts)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)
    }

    pub async fn cancel_running(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        reason: &str,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let done_projection = done_row_projection("done", "ready");
        let ready_join = done_ready_join(schema, "done", "ready");
        let moved: Option<HydratedTerminalRow> = sqlx::query_as(&format!(
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
            SELECT {done_projection}, COALESCE(ready.priority, done.priority) AS body_priority
            FROM deleted AS done
            {ready_join}
            "#
        ))
        .bind(job_id)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(hydrated) = moved else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };
        let mut moved = hydrated.terminal;

        self.ensure_terminal_removed_receipt_closures_tx(&mut tx, std::slice::from_ref(&moved))
            .await?;
        // DLQ inserts are outside done_entries, so the source terminal DELETE
        // appends a negative delta.
        self.decrement_live_terminal_counters_tx(
            &mut tx,
            &Self::done_rows_to_counter_keys(std::slice::from_ref(&moved)),
        )
        .await?;
        moved.priority = hydrated.body_priority;
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let moved = self
            .move_failed_rows_to_dlq_tx(&mut tx, kind, queue, dlq_reason, None)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(moved)
    }

    /// Move failed done rows matching `kind` and `queue` into the DLQ,
    /// limited to `ids` when set.
    async fn move_failed_rows_to_dlq_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        kind: Option<&str>,
        queue: Option<&str>,
        dlq_reason: &str,
        ids: Option<&[i64]>,
    ) -> Result<u64, AwaError> {
        let schema = self.schema();
        let done_projection = done_row_projection("done", "ready");
        let ready_join = done_ready_join(schema, "done", "ready");
        let moved: Vec<HydratedTerminalRow> = sqlx::query_as(&format!(
            r#"
            WITH deleted AS (
                DELETE FROM {schema}.done_entries
                WHERE state = 'failed'
                  AND ($1::text IS NULL OR kind = $1)
                  AND ($2::text IS NULL OR queue = $2)
                  AND ($3::bigint[] IS NULL OR job_id = ANY($3))
                RETURNING *
            )
            SELECT {done_projection}, COALESCE(ready.priority, done.priority) AS body_priority
            FROM deleted AS done
            {ready_join}
            "#
        ))
        .bind(kind)
        .bind(queue)
        .bind(ids)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if moved.is_empty() {
            return Ok(0);
        }

        let (moved, body_priorities): (Vec<_>, Vec<_>) = moved
            .into_iter()
            .map(|row| (row.terminal, row.body_priority))
            .unzip();
        self.ensure_terminal_removed_receipt_closures_tx(tx, &moved)
            .await?;
        // Bulk DLQ move deletes from done_entries. Append negative deltas by
        // the per-group magnitudes of the moved rows.
        self.decrement_live_terminal_counters_tx(tx, &Self::done_rows_to_counter_keys(&moved))
            .await?;
        let dlq_at = Utc::now();
        let rows: Vec<DlqJobRow> = moved
            .into_iter()
            .zip(body_priorities)
            .map(|(mut row, priority)| {
                row.priority = priority;
                row.into_dlq_row(dlq_reason.to_string(), dlq_at)
            })
            .collect();
        self.insert_dlq_rows_tx(tx, &rows, Some(JobState::Failed))
            .await?;
        Ok(rows.len() as u64)
    }

    pub async fn retry_from_dlq(
        &self,
        pool: &PgPool,
        job_id: i64,
        opts: &RetryFromDlqOpts,
    ) -> Result<Option<JobRow>, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        tx.commit().await.map_err(map_sqlx_error)?;
        self.wake_queues_after_commit(pool, [queue]);
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        tx.commit().await.map_err(map_sqlx_error)?;
        self.wake_queues_after_commit(pool, queues);
        Ok(revived)
    }

    /// Retry up to `batch_size` DLQ rows matching the filter past `cursor`,
    /// in job ID order, in one transaction.
    ///
    /// Rows another transaction holds are not taken and the cursor stops
    /// before them; when no row can be taken the batch is `busy`. A row whose
    /// unique key a live job holds is skipped and reported, and stays in the
    /// DLQ. `filter.limit` is ignored.
    #[tracing::instrument(
        skip(self, pool, filter),
        fields(kind = ?filter.kind, queue = ?filter.queue, tag = ?filter.tag, batch_size),
        name = "queue_storage.bulk_retry_from_dlq_batch"
    )]
    pub async fn bulk_retry_from_dlq_batch(
        &self,
        pool: &PgPool,
        filter: &ListDlqFilter,
        batch_size: i64,
        cursor: BulkCursor,
    ) -> Result<BulkBatch, AwaError> {
        bulk::validate_batch_size(batch_size)?;
        let schema = self.schema();
        let mut cursor = cursor;
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let fence = self.mark_bulk_run_tx(&mut tx, &mut cursor).await?;
        let mut batch = BulkBatch::queue_storage(cursor);
        batch.left_above_sequence = self.dlq_left_above_tx(&mut tx, fence, filter).await?;
        let pick = self
            .pick_dlq_batch_tx(&mut tx, filter, batch_size, cursor)
            .await?;
        let mut woken = BTreeSet::new();
        if !pick.ids.is_empty() {
            let rows: Vec<DlqJobRow> = sqlx::query_as(&format!(
                r#"
                SELECT {DLQ_ROW_COLUMNS}
                FROM {schema}.dlq_entries
                WHERE job_id = ANY($1)
                ORDER BY job_id
                "#
            ))
            .bind(&pick.ids)
            .fetch_all(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            let run_at = Utc::now();
            let mut queues = BTreeSet::new();
            let mut ready_rows = Vec::with_capacity(rows.len());
            for row in rows {
                let queue = row.queue.clone();
                let priority = row.priority;
                queues.insert(queue.clone());
                let mut payload = RuntimePayload::from_json(row.payload.clone())?;
                payload.set_progress(None);
                ready_rows.push(row.into_retry_ready_row(
                    queue,
                    priority,
                    run_at,
                    payload.into_json(),
                ));
            }
            let picked: Vec<i64> = ready_rows.iter().map(|row| row.job_id).collect();
            let ready_rows = ready_rows
                .into_iter()
                .map(|row| (row, Some(JobState::Failed)))
                .collect();
            let (revived, skipped) = self
                .insert_existing_ready_rows_inner_tx(&mut tx, ready_rows, true)
                .await?;
            for job_id in &skipped {
                batch.skip(*job_id);
            }
            if revived > 0 {
                let revived_ids: Vec<i64> = picked
                    .into_iter()
                    .filter(|job_id| !skipped.contains(job_id))
                    .collect();
                sqlx::query(&format!(
                    "DELETE FROM {schema}.dlq_entries WHERE job_id = ANY($1)"
                ))
                .bind(&revived_ids)
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
                batch.count = revived as u64;
                woken = queues;
            }
        }
        pick.settle(&mut batch, None);
        tx.commit().await.map_err(map_sqlx_error)?;
        self.wake_queues_after_commit(pool, woken);
        Ok(batch)
    }

    /// On a run's first batch, fence the schema's job ID sequence (see
    /// [`bulk`]) past the highest job ID in any of the schema's job tables,
    /// such as one migrated from canonical storage and kept by a retry, and
    /// record the run's bound in `cursor`.
    /// Returns the fence on a first batch, `None` when the cursor resumes a
    /// run.
    async fn mark_bulk_run_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        cursor: &mut BulkCursor,
    ) -> Result<Option<bulk::Fence>, AwaError> {
        if cursor.before_job_id != 0 {
            return Ok(None);
        }
        let schema = self.schema();
        let fence = bulk::fence_sequence_tx(
            tx,
            &format!(
                r#"
                WITH sequence AS (
                    SELECT {schema}.sequence_next_value('job_id_seq') AS next
                )
                SELECT
                    sequence.next,
                    GREATEST(
                        sequence.next,
                        (SELECT max(job_id) + 1 FROM {schema}.ready_entries),
                        (SELECT max(job_id) + 1 FROM {schema}.deferred_jobs),
                        (SELECT max(job_id) + 1 FROM {schema}.leases),
                        (SELECT max(job_id) + 1 FROM {schema}.lease_claims),
                        (SELECT max(job_id) + 1 FROM {schema}.done_entries),
                        (SELECT max(job_id) + 1 FROM {schema}.dlq_entries)
                    )
                FROM sequence
                "#
            ),
            &format!("SELECT {schema}.set_sequence_next('job_id_seq', $1)"),
        )
        .await?;
        bulk::apply_fence(cursor, fence);
        Ok(Some(fence))
    }

    /// SQL counting the rows matching each `(relation, filter)` whose job
    /// ID lies in `bound..top`, the IDs a refused fence leaves above the run.
    fn left_above_sql(&self, passes: &[(&str, &str)], bound: &str, top: &str) -> String {
        let schema = self.schema();
        let counts: Vec<String> = passes
            .iter()
            .map(|(relation, filter)| {
                format!(
                    "({})",
                    bulk::count_between_sql(
                        &format!("{schema}.{relation}"),
                        "job_id",
                        filter,
                        bound,
                        top
                    )
                )
            })
            .collect();
        format!("SELECT {}", counts.join(" + "))
    }

    /// Matching DLQ rows a refused `fence` leaves above the run.
    async fn dlq_left_above_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        fence: Option<bulk::Fence>,
        filter: &ListDlqFilter,
    ) -> Result<u64, AwaError> {
        let Some(bulk::Fence::Refused { bound, top }) = fence else {
            return Ok(0);
        };
        let count: i64 = sqlx::query_scalar(&self.left_above_sql(
            &[("dlq_entries", DLQ_FILTER_SQL)],
            "$6",
            "$7",
        ))
        .bind(&filter.kind)
        .bind(&filter.queue)
        .bind(&filter.tag)
        .bind(filter.before_id)
        .bind(filter.before_dlq_at)
        .bind(bound)
        .bind(top)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(count as u64)
    }

    /// Matching failed done rows, and with `with_dlq` the kind's DLQ rows, a
    /// refused `fence` leaves above the run.
    async fn failed_left_above_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        fence: Option<bulk::Fence>,
        kind: Option<&str>,
        queue: Option<&str>,
        with_dlq: bool,
    ) -> Result<u64, AwaError> {
        let Some(bulk::Fence::Refused { bound, top }) = fence else {
            return Ok(0);
        };
        let mut passes = vec![(
            "done_entries",
            "state = 'failed' AND ($1::text IS NULL OR kind = $1) AND ($2::text IS NULL OR queue = $2)",
        )];
        if with_dlq {
            passes.push((
                "dlq_entries",
                "($1::text IS NULL OR kind = $1) AND ($2::text IS NULL OR queue = $2)",
            ));
        }
        let count: i64 = sqlx::query_scalar(&self.left_above_sql(&passes, "$3", "$4"))
            .bind(kind)
            .bind(queue)
            .bind(bound)
            .bind(top)
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        Ok(count as u64)
    }

    /// The queue's waiting, scheduled, retryable and unclaimed available
    /// jobs a refused `fence` leaves above the drain run.
    async fn drain_left_above(
        &self,
        pool: &PgPool,
        fence: Option<bulk::Fence>,
        queue: &str,
    ) -> Result<u64, AwaError> {
        let Some(bulk::Fence::Refused { bound, top }) = fence else {
            return Ok(0);
        };
        let schema = self.schema();
        let claimed = self.ready_row_claimed_sql("ready");
        let deferred_and_waiting = self.left_above_sql(
            &[
                (
                    "deferred_jobs",
                    &format!(
                        "{} AND state IN ('scheduled', 'retryable')",
                        queue_and_stripes_sql("queue", "$1")
                    ),
                ),
                (
                    "leases",
                    &format!(
                        "{} AND state = 'waiting_external'",
                        queue_and_stripes_sql("queue", "$1")
                    ),
                ),
            ],
            "$2",
            "$3",
        );
        let count: i64 = sqlx::query_scalar(&format!(
            r#"
            {deferred_and_waiting} + (
                SELECT count(*)
                FROM {schema}.ready_entries AS ready
                WHERE {queues}
                  AND ready.job_id >= $2
                  AND ready.job_id < $3
                  AND NOT (
                      EXISTS (
                          SELECT 1 FROM {schema}.ready_tombstones AS tomb
                          WHERE tomb.ready_slot = ready.ready_slot
                            AND tomb.ready_generation = ready.ready_generation
                            AND tomb.queue = ready.queue
                            AND tomb.priority = ready.priority
                            AND tomb.enqueue_shard = ready.enqueue_shard
                            AND tomb.lane_seq = ready.lane_seq
                      )
                      OR {claimed}
                  )
            )
            "#,
            queues = queue_and_stripes_sql("ready.queue", "$1"),
        ))
        .bind(queue)
        .bind(bound)
        .bind(top)
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(count as u64)
    }

    /// Lock the next DLQ rows matching `filter` in the cursor's range.
    async fn pick_dlq_batch_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        filter: &ListDlqFilter,
        batch_size: i64,
        cursor: BulkCursor,
    ) -> Result<bulk::Pick, AwaError> {
        let (picked, first_held, more): (Vec<i64>, Option<i64>, bool) =
            sqlx::query_as(&bulk::pick_sql(
                &format!("{}.dlq_entries", self.schema()),
                "job_id",
                DLQ_FILTER_SQL,
                "$6",
                ["$7", "$9"],
                "$8",
            ))
            .bind(&filter.kind)
            .bind(&filter.queue)
            .bind(&filter.tag)
            .bind(filter.before_id)
            .bind(filter.before_dlq_at)
            .bind(cursor.after_job_id)
            .bind(cursor.before_job_id)
            .bind(batch_size)
            .bind(cursor.sequence_bound)
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        Ok(bulk::Pick::new(picked, first_held, more))
    }

    /// Lock the next failed done rows matching `kind` and `queue` in the
    /// cursor's range.
    async fn pick_failed_done_batch_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        kind: Option<&str>,
        queue: Option<&str>,
        batch_size: i64,
        cursor: BulkCursor,
    ) -> Result<bulk::Pick, AwaError> {
        let (picked, first_held, more): (Vec<i64>, Option<i64>, bool) =
            sqlx::query_as(&bulk::pick_sql(
                &format!("{}.done_entries", self.schema()),
                "job_id",
                "state = 'failed' AND ($1::text IS NULL OR kind = $1) AND ($2::text IS NULL OR queue = $2)",
                "$3",
                ["$4", "$6"],
                "$5",
            ))
            .bind(kind)
            .bind(queue)
            .bind(cursor.after_job_id)
            .bind(cursor.before_job_id)
            .bind(batch_size)
            .bind(cursor.sequence_bound)
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        Ok(bulk::Pick::new(picked, first_held, more))
    }

    /// Retry `ids` in the open transaction, skipping any job whose retry
    /// would violate a unique claim. Each pass takes the jobs and claims all
    /// their keys, in key order, before any lane is reserved. A pass that
    /// meets a conflict, whether taking a job or claiming the keys, holds no
    /// lane yet; it is rolled back and the next pass leaves the conflicting
    /// jobs where they are.
    async fn retry_jobs_skipping_conflicts_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        ids: &[i64],
        skipped: &mut impl FnMut(i64),
    ) -> Result<Vec<JobRow>, AwaError> {
        let mut ids: Vec<i64> = ids.to_vec();
        loop {
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            sqlx::query("SAVEPOINT awa_bulk_batch")
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            let mut taken = Vec::with_capacity(ids.len());
            let mut take_conflict = None;
            for job_id in &ids {
                match self.take_retry_row_tx(tx, *job_id).await {
                    Ok(Some((ready_row, old_state))) => taken.push((ready_row, Some(old_state))),
                    Ok(None) => {}
                    Err(AwaError::UniqueConflict { .. }) => {
                        take_conflict = Some(*job_id);
                        break;
                    }
                    Err(err) => return Err(err),
                }
            }
            let conflicts = match take_conflict {
                Some(job_id) => HashSet::from([job_id]),
                None => self.sync_reenqueue_unique_claims(tx, &taken, true).await?,
            };
            if conflicts.is_empty() {
                let rows = taken
                    .iter()
                    .map(|(ready_row, _)| Self::retried_job_row(ready_row.clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                self.insert_existing_ready_rows_inner_tx(tx, taken, false)
                    .await?;
                sqlx::query("RELEASE SAVEPOINT awa_bulk_batch")
                    .execute(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;
                return Ok(rows);
            }
            sqlx::query("ROLLBACK TO SAVEPOINT awa_bulk_batch")
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            sqlx::query("RELEASE SAVEPOINT awa_bulk_batch")
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            ids.retain(|job_id| !conflicts.contains(job_id));
            conflicts.into_iter().for_each(&mut *skipped);
        }
    }

    /// Retry up to `batch_size` failed jobs matching `kind` and `queue` past
    /// `cursor`, in job ID order, in one transaction. A job whose retry would
    /// violate a unique claim is skipped and reported.
    pub async fn retry_failed_batch(
        &self,
        pool: &PgPool,
        kind: Option<&str>,
        queue: Option<&str>,
        batch_size: i64,
        cursor: BulkCursor,
    ) -> Result<BulkBatch, AwaError> {
        bulk::validate_batch_size(batch_size)?;
        let mut cursor = cursor;
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let fence = self.mark_bulk_run_tx(&mut tx, &mut cursor).await?;
        let mut batch = BulkBatch::queue_storage(cursor);
        batch.left_above_sequence = self
            .failed_left_above_tx(&mut tx, fence, kind, queue, false)
            .await?;
        let pick = self
            .pick_failed_done_batch_tx(&mut tx, kind, queue, batch_size, cursor)
            .await?;
        let mut skipped = Vec::new();
        let rows = self
            .retry_jobs_skipping_conflicts_tx(&mut tx, &pick.ids, &mut |job_id| {
                skipped.push(job_id)
            })
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        self.wake_queues_after_commit(pool, rows.iter().map(|row| row.queue.clone()));
        batch.count = rows.len() as u64;
        skipped.into_iter().for_each(|job_id| batch.skip(job_id));
        pick.settle(&mut batch, None);
        Ok(batch)
    }

    /// Retry `ids` in transactions of at most `batch_size` jobs, skipping and
    /// reporting jobs whose retry would violate a unique claim. When a chunk
    /// fails, the error carries the jobs earlier chunks committed.
    pub async fn retry_jobs_in_batches(
        &self,
        pool: &PgPool,
        ids: &[i64],
        batch_size: i64,
    ) -> Result<BulkIds, BulkError<BulkIds>> {
        let mut outcome = BulkIds::default();
        if let Err(error) = bulk::validate_batch_size(batch_size) {
            return Err(BulkError::new(outcome, error));
        }
        let unique_ids: Vec<i64> = ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        for (index, chunk) in unique_ids.chunks(batch_size as usize).enumerate() {
            if index > 0 {
                tokio::time::sleep(bulk::DEFAULT_QUEUE_STORAGE_PAUSE).await;
            }
            let mut skipped = Vec::new();
            let chunk_result = async {
                let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
                let rows = self
                    .retry_jobs_skipping_conflicts_tx(&mut tx, chunk, &mut |job_id| {
                        skipped.push(job_id)
                    })
                    .await?;
                tx.commit().await.map_err(map_sqlx_error)?;
                self.wake_queues_after_commit(pool, rows.iter().map(|row| row.queue.clone()));
                Ok::<_, AwaError>(rows)
            }
            .await;
            match chunk_result {
                Ok(rows) => {
                    outcome.jobs.extend(rows);
                    skipped.into_iter().for_each(|job_id| outcome.skip(job_id));
                }
                Err(error) => return Err(BulkError::new(outcome, error)),
            }
        }
        Ok(outcome)
    }

    /// Cancel `ids` in transactions of at most `batch_size` jobs, and at most
    /// [`CANCEL_BY_ID_CHUNK`] jobs, in job ID order. When a chunk fails, the
    /// error carries the jobs earlier chunks committed.
    pub async fn cancel_jobs_in_batches(
        &self,
        pool: &PgPool,
        ids: &[i64],
        batch_size: i64,
    ) -> Result<BulkIds, BulkError<BulkIds>> {
        let mut outcome = BulkIds::default();
        if let Err(error) = bulk::validate_batch_size(batch_size) {
            return Err(BulkError::new(outcome, error));
        }
        // Sorted and deduplicated, so concurrent calls lock claim heads and
        // rows in the same order.
        let unique_ids: Vec<i64> = ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let chunk_size = batch_size.min(CANCEL_BY_ID_CHUNK) as usize;
        for (index, chunk) in unique_ids.chunks(chunk_size).enumerate() {
            if index > 0 {
                tokio::time::sleep(bulk::DEFAULT_QUEUE_STORAGE_PAUSE).await;
            }
            match self.cancel_jobs_by_ids(pool, chunk).await {
                Ok(rows) => outcome.jobs.extend(rows),
                Err(error) => return Err(BulkError::new(outcome, error)),
            }
        }
        Ok(outcome)
    }

    /// Discard up to `batch_size` failed jobs of `kind` past `cursor`, in
    /// one transaction: failed done rows first, then DLQ rows.
    pub async fn discard_failed_batch(
        &self,
        pool: &PgPool,
        kind: &str,
        batch_size: i64,
        cursor: BulkCursor,
    ) -> Result<BulkBatch, AwaError> {
        bulk::validate_batch_size(batch_size)?;
        let mut cursor = cursor;
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let fence = self.mark_bulk_run_tx(&mut tx, &mut cursor).await?;
        let mut batch = BulkBatch::queue_storage(cursor);
        batch.left_above_sequence = self
            .failed_left_above_tx(&mut tx, fence, Some(kind), None, true)
            .await?;
        if cursor.stage == 0 {
            let pick = self
                .pick_failed_done_batch_tx(&mut tx, Some(kind), None, batch_size, cursor)
                .await?;
            if !pick.ids.is_empty() {
                batch.count = self
                    .discard_failed_rows_tx(&mut tx, kind, Some(&pick.ids), Some(&[]))
                    .await?;
            }
            pick.settle(&mut batch, Some(1));
        } else {
            let filter = ListDlqFilter {
                kind: Some(kind.to_string()),
                ..Default::default()
            };
            let pick = self
                .pick_dlq_batch_tx(&mut tx, &filter, batch_size, cursor)
                .await?;
            if !pick.ids.is_empty() {
                batch.count = self
                    .discard_failed_rows_tx(&mut tx, kind, Some(&[]), Some(&pick.ids))
                    .await?;
            }
            pick.settle(&mut batch, None);
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(batch)
    }

    /// Move up to `batch_size` failed jobs matching `kind` and `queue` past
    /// `cursor` into the DLQ, in one transaction.
    pub async fn bulk_move_failed_to_dlq_batch(
        &self,
        pool: &PgPool,
        kind: Option<&str>,
        queue: Option<&str>,
        dlq_reason: &str,
        batch_size: i64,
        cursor: BulkCursor,
    ) -> Result<BulkBatch, AwaError> {
        bulk::validate_batch_size(batch_size)?;
        let mut cursor = cursor;
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let fence = self.mark_bulk_run_tx(&mut tx, &mut cursor).await?;
        let mut batch = BulkBatch::queue_storage(cursor);
        batch.left_above_sequence = self
            .failed_left_above_tx(&mut tx, fence, kind, queue, false)
            .await?;
        let pick = self
            .pick_failed_done_batch_tx(&mut tx, kind, queue, batch_size, cursor)
            .await?;
        if !pick.ids.is_empty() {
            batch.count = self
                .move_failed_rows_to_dlq_tx(&mut tx, kind, queue, dlq_reason, Some(&pick.ids))
                .await?;
        }
        pick.settle(&mut batch, None);
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(batch)
    }

    /// Delete up to `batch_size` DLQ rows matching `filter` past `cursor`, in
    /// one transaction. `filter.limit` is ignored.
    pub async fn purge_dlq_batch(
        &self,
        pool: &PgPool,
        filter: &ListDlqFilter,
        batch_size: i64,
        cursor: BulkCursor,
    ) -> Result<BulkBatch, AwaError> {
        bulk::validate_batch_size(batch_size)?;
        let mut cursor = cursor;
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let fence = self.mark_bulk_run_tx(&mut tx, &mut cursor).await?;
        let mut batch = BulkBatch::queue_storage(cursor);
        batch.left_above_sequence = self.dlq_left_above_tx(&mut tx, fence, filter).await?;
        let pick = self
            .pick_dlq_batch_tx(&mut tx, filter, batch_size, cursor)
            .await?;
        if !pick.ids.is_empty() {
            batch.count = self.delete_dlq_rows_by_id_tx(&mut tx, &pick.ids).await?;
        }
        pick.settle(&mut batch, None);
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(batch)
    }

    /// Release the unique claims of DLQ rows `deleted` removed. A DLQ row is
    /// a failed job, and a unique key whose states include `failed` stays
    /// claimed while the row exists; deleting the row without this leaves
    /// a claim that blocks the key for a job that no longer exists. Claims
    /// are released in one pass in key order, whatever order the delete
    /// returned the rows in.
    async fn release_deleted_dlq_claims_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        deleted: &[DeletedDlqRow],
    ) -> Result<(), AwaError> {
        let changes = deleted
            .iter()
            .filter_map(|row| {
                unique_claim_change(
                    row.job_id,
                    row.unique_key.as_deref(),
                    row.unique_states.as_deref(),
                    Some(row.state),
                    None,
                )
            })
            .collect();
        self.apply_unique_claim_changes_tx(tx, changes, false)
            .await?;
        Ok(())
    }

    /// Delete the DLQ rows `job_ids` in the open transaction, releasing their
    /// unique claims, and return how many were deleted.
    pub(crate) async fn delete_dlq_rows_by_id_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        job_ids: &[i64],
    ) -> Result<u64, AwaError> {
        let deleted: Vec<DeletedDlqRow> = sqlx::query_as(&format!(
            "DELETE FROM {}.dlq_entries WHERE job_id = ANY($1) RETURNING {DELETED_DLQ_COLUMNS}",
            self.schema()
        ))
        .bind(job_ids)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        self.release_deleted_dlq_claims_tx(tx, &deleted).await?;
        Ok(deleted.len() as u64)
    }

    /// Delete every DLQ row matching `filter`, its cursor included, in one
    /// transaction, releasing their unique claims. Ignores `filter.limit`.
    pub async fn purge_dlq(&self, pool: &PgPool, filter: &ListDlqFilter) -> Result<u64, AwaError> {
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let deleted: Vec<DeletedDlqRow> = sqlx::query_as(&format!(
            "DELETE FROM {}.dlq_entries WHERE {DLQ_FILTER_SQL} RETURNING {DELETED_DLQ_COLUMNS}",
            self.schema()
        ))
        .bind(&filter.kind)
        .bind(&filter.queue)
        .bind(&filter.tag)
        .bind(filter.before_id)
        .bind(filter.before_dlq_at)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        self.release_deleted_dlq_claims_tx(&mut tx, &deleted)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(deleted.len() as u64)
    }

    /// Delete one DLQ row, releasing its unique claim.
    pub async fn purge_dlq_job(&self, pool: &PgPool, job_id: i64) -> Result<bool, AwaError> {
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let deleted = self.delete_dlq_rows_by_id_tx(&mut tx, &[job_id]).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(deleted > 0)
    }

    /// Delete up to `limit` DLQ rows older than `retention`, releasing their
    /// unique claims, and return how many were deleted. `queue` keeps the
    /// delete to one queue; `exclude_queues` leaves those queues alone.
    /// With `oldest_first`, rows go in `(dlq_at, job_id)` order.
    pub async fn delete_expired_dlq_rows(
        &self,
        pool: &PgPool,
        retention: std::time::Duration,
        limit: i64,
        queue: Option<&str>,
        exclude_queues: &[&str],
        oldest_first: bool,
    ) -> Result<u64, AwaError> {
        let schema = self.schema();
        let retention_secs = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
        let order = if oldest_first {
            "ORDER BY dlq_at ASC, job_id ASC"
        } else {
            ""
        };
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let deleted: Vec<DeletedDlqRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.dlq_entries
            WHERE job_id IN (
                SELECT job_id FROM {schema}.dlq_entries
                WHERE dlq_at < now() - make_interval(secs => $1::bigint)
                  AND ($3::text IS NULL OR queue = $3)
                  AND queue <> ALL($4::text[])
                {order}
                LIMIT $2
            )
            RETURNING {DELETED_DLQ_COLUMNS}
            "#
        ))
        .bind(retention_secs)
        .bind(limit)
        .bind(queue)
        .bind(exclude_queues)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        self.release_deleted_dlq_claims_tx(&mut tx, &deleted)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(deleted.len() as u64)
    }

    pub async fn discard_failed_by_kind(&self, pool: &PgPool, kind: &str) -> Result<u64, AwaError> {
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        let discarded = self
            .discard_failed_rows_tx(&mut tx, kind, None, None)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(discarded)
    }

    /// Discard failed done rows and DLQ rows of `kind`, limited to the given
    /// job IDs when they are set.
    async fn discard_failed_rows_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        kind: &str,
        done_ids: Option<&[i64]>,
        dlq_ids: Option<&[i64]>,
    ) -> Result<u64, AwaError> {
        let schema = self.schema();

        let done_projection = done_row_projection("done", "ready");
        let ready_join = done_ready_join(schema, "done", "ready");
        let deleted_done: Vec<DoneJobRow> = sqlx::query_as(&format!(
            r#"
            WITH deleted AS (
                DELETE FROM {schema}.done_entries
                WHERE kind = $1
                  AND state = 'failed'
                  AND ($2::bigint[] IS NULL OR job_id = ANY($2))
                RETURNING *
            )
            SELECT {done_projection}
            FROM deleted AS done
            {ready_join}
            "#
        ))
        .bind(kind)
        .bind(done_ids)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let deleted_dlq: Vec<DlqJobRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.dlq_entries
            WHERE kind = $1
              AND ($2::bigint[] IS NULL OR job_id = ANY($2))
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
        .bind(dlq_ids)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        // Discard removes terminal `failed` rows from done_entries (and
        // `failed` DLQ rows from dlq_entries — exact terminal counts are keyed
        // on done_entries only). Append negative deltas by the per-group
        // magnitudes of `deleted_done`.
        self.ensure_terminal_removed_receipt_closures_tx(tx, &deleted_done)
            .await?;
        self.decrement_live_terminal_counters_tx(
            tx,
            &Self::done_rows_to_counter_keys(&deleted_done),
        )
        .await?;

        let changes = deleted_done
            .iter()
            .map(|row| (row.job_id, &row.unique_key, &row.unique_states, row.state))
            .chain(
                deleted_dlq
                    .iter()
                    .map(|row| (row.job_id, &row.unique_key, &row.unique_states, row.state)),
            )
            .filter_map(|(job_id, key, states, state)| {
                unique_claim_change(job_id, key.as_deref(), states.as_deref(), Some(state), None)
            })
            .collect();
        self.apply_unique_claim_changes_tx(tx, changes, false)
            .await?;

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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
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
        let row = self
            .insert_deferred_or_cancel_duplicate_tx(tx, moved, deferred, RETRIED_DUPLICATE_ERROR)
            .await?;
        Ok(Some(row))
    }

    /// Rescue attempts whose heartbeat went stale. Exhausted attempts fail
    /// into `done_entries`; see [`Self::rescue_stale_heartbeats_with_dlq`]
    /// to dead-letter them instead.
    pub async fn rescue_stale_heartbeats(
        &self,
        pool: &PgPool,
        staleness: Duration,
    ) -> Result<Vec<JobRow>, AwaError> {
        self.rescue_stale_heartbeats_with_dlq(pool, staleness, &|_| false)
            .await
    }

    /// [`Self::rescue_stale_heartbeats`], moving an exhausted attempt into
    /// `dlq_entries` (reason `heartbeat_stale`) in the rescue transaction
    /// when `dlq_enabled` returns true for its queue.
    pub async fn rescue_stale_heartbeats_with_dlq(
        &self,
        pool: &PgPool,
        staleness: Duration,
        dlq_enabled: &(dyn Fn(&str) -> bool + Sync),
    ) -> Result<Vec<JobRow>, AwaError> {
        Ok(self
            .rescue_stale_heartbeats_batch(
                pool,
                staleness,
                dlq_enabled,
                &RescueBatchOptions::default(),
            )
            .await?
            .rescued)
    }

    /// One stale-heartbeat rescue batch in its own transaction: up to
    /// 500 lease rows and 500 receipt claims.
    #[tracing::instrument(
        skip(self, pool, dlq_enabled, options),
        name = "queue_storage.rescue_stale_heartbeats"
    )]
    pub async fn rescue_stale_heartbeats_batch(
        &self,
        pool: &PgPool,
        staleness: Duration,
        dlq_enabled: &(dyn Fn(&str) -> bool + Sync),
        options: &RescueBatchOptions,
    ) -> Result<RescueBatch, AwaError> {
        let schema = self.schema();
        let mut tx = pool
            .begin_with(RESCUE_TRANSACTION_BEGIN)
            .await
            .map_err(map_sqlx_error)?;
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
        let mut excluded = BTreeSet::new();
        let mut selected: Option<BTreeSet<(i64, i64)>> = None;
        loop {
            let (excluded_job_ids, excluded_run_leases): (Vec<_>, Vec<_>) =
                excluded.iter().copied().unzip();
            let (selected_job_ids, selected_run_leases): (Vec<i64>, Vec<i64>) = selected
                .as_ref()
                .map(|pairs| pairs.iter().copied().unzip())
                .unwrap_or_default();
            // isolation: a savepoint, at the enclosing transaction's level
            let mut attempt = tx.begin().await.map_err(map_sqlx_error)?;
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
                      AND NOT EXISTS (
                          SELECT 1 FROM unnest($2::bigint[], $3::bigint[]) AS excluded(job_id, run_lease)
                          WHERE excluded.job_id = lease.job_id AND excluded.run_lease = lease.run_lease
                      )
                      AND ($4::boolean OR EXISTS (
                          SELECT 1 FROM unnest($5::bigint[], $6::bigint[]) AS selected(job_id, run_lease)
                          WHERE selected.job_id = lease.job_id AND selected.run_lease = lease.run_lease
                      ))
                    ORDER BY COALESCE(attempt.heartbeat_at, lease.heartbeat_at) ASC
                    LIMIT $7
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
            .bind(&excluded_job_ids)
            .bind(&excluded_run_leases)
            .bind(selected.is_none())
            .bind(&selected_job_ids)
            .bind(&selected_run_leases)
            .bind(RECEIPT_RESCUE_BATCH_LIMIT)
            .fetch_all(attempt.as_mut())
            .await
            .map_err(map_sqlx_error)?;
            let mut saturated = deleted.len() as i64 >= RECEIPT_RESCUE_BATCH_LIMIT;

            let receipt_excluded: BTreeSet<_> = excluded
                .iter()
                .copied()
                .chain(deleted.iter().map(|row| (row.job_id, row.run_lease)))
                .collect();
            let mut receipts = ReceiptRescueBatch::default();
            if self.lease_claim_receipts() {
                self.rescue_stale_receipt_claims_tx(
                    &mut attempt,
                    cutoff,
                    options,
                    &receipt_excluded,
                    selected.as_ref(),
                    &mut receipts,
                )
                .await?;
            }
            saturated |= receipts.saturated;
            let ReceiptRescueBatch {
                rescued: rescued_receipts,
                idle_slots,
                scan_progress,
                ..
            } = receipts;

            selected.get_or_insert_with(|| {
                deleted
                    .iter()
                    .chain(rescued_receipts.iter())
                    .map(|row| (row.job_id, row.run_lease))
                    .collect()
            });

            if deleted.is_empty() && rescued_receipts.is_empty() {
                attempt.commit().await.map_err(map_sqlx_error)?;
                tx.commit().await.map_err(map_sqlx_error)?;
                return Ok(RescueBatch {
                    idle_slots,
                    scan_progress,
                    saturated,
                    ..RescueBatch::default()
                });
            }

            let moved_leases = match self
                .try_hydrate_deleted_leases_tx(&mut attempt, deleted)
                .await?
            {
                Ok(moved) => moved,
                Err(busy) => {
                    attempt.rollback().await.map_err(map_sqlx_error)?;
                    busy.exclude(&mut excluded, &mut selected)?;
                    continue;
                }
            };
            let moved_receipts = match self
                .try_hydrate_deleted_leases_tx(&mut attempt, rescued_receipts)
                .await?
            {
                Ok(moved) => moved,
                Err(busy) => {
                    attempt.rollback().await.map_err(map_sqlx_error)?;
                    busy.exclude(&mut excluded, &mut selected)?;
                    continue;
                }
            };

            let rescued = self
                .settle_rescued_attempts_tx(
                    &mut attempt,
                    vec![
                        (moved_leases, "heartbeat stale: worker presumed dead"),
                        (moved_receipts, "receipt claim stale: worker presumed dead"),
                    ],
                    RescueDlq {
                        reason: "heartbeat_stale",
                        enabled: dlq_enabled,
                    },
                )
                .await?;
            attempt.commit().await.map_err(map_sqlx_error)?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RescueBatch {
                rescued,
                saturated,
                idle_slots,
                scan_progress,
            });
        }
    }

    /// Rescue attempts past their hard deadline. Exhausted attempts fail
    /// into `done_entries`; see [`Self::rescue_expired_deadlines_with_dlq`]
    /// to dead-letter them instead.
    pub async fn rescue_expired_deadlines(&self, pool: &PgPool) -> Result<Vec<JobRow>, AwaError> {
        self.rescue_expired_deadlines_with_dlq(pool, &|_| false)
            .await
    }

    /// [`Self::rescue_expired_deadlines`], moving an exhausted attempt into
    /// `dlq_entries` (reason `deadline_exceeded`) in the rescue transaction
    /// when `dlq_enabled` returns true for its queue.
    pub async fn rescue_expired_deadlines_with_dlq(
        &self,
        pool: &PgPool,
        dlq_enabled: &(dyn Fn(&str) -> bool + Sync),
    ) -> Result<Vec<JobRow>, AwaError> {
        Ok(self
            .rescue_expired_deadlines_batch(pool, dlq_enabled, &RescueBatchOptions::default())
            .await?
            .rescued)
    }

    /// One deadline rescue batch in its own transaction: up to
    /// 500 lease rows and 500 receipt claims.
    #[tracing::instrument(
        skip(self, pool, dlq_enabled, options),
        name = "queue_storage.rescue_expired_deadlines"
    )]
    pub async fn rescue_expired_deadlines_batch(
        &self,
        pool: &PgPool,
        dlq_enabled: &(dyn Fn(&str) -> bool + Sync),
        options: &RescueBatchOptions,
    ) -> Result<RescueBatch, AwaError> {
        let schema = self.schema();
        let mut tx = pool
            .begin_with(RESCUE_TRANSACTION_BEGIN)
            .await
            .map_err(map_sqlx_error)?;
        let mut excluded = BTreeSet::new();
        let mut selected: Option<BTreeSet<(i64, i64)>> = None;
        loop {
            let (excluded_job_ids, excluded_run_leases): (Vec<_>, Vec<_>) =
                excluded.iter().copied().unzip();
            let (selected_job_ids, selected_run_leases): (Vec<i64>, Vec<i64>) = selected
                .as_ref()
                .map(|pairs| pairs.iter().copied().unzip())
                .unwrap_or_default();
            // isolation: a savepoint, at the enclosing transaction's level
            let mut attempt = tx.begin().await.map_err(map_sqlx_error)?;
            let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
                r#"
                DELETE FROM {schema}.leases
                WHERE (job_id, run_lease) IN (
                    SELECT job_id, run_lease
                    FROM {schema}.leases
                    WHERE state = 'running'
                      AND deadline_at IS NOT NULL
                      AND deadline_at < clock_timestamp()
                      AND NOT EXISTS (
                          SELECT 1 FROM unnest($1::bigint[], $2::bigint[]) AS excluded(job_id, run_lease)
                          WHERE excluded.job_id = leases.job_id AND excluded.run_lease = leases.run_lease
                      )
                      AND ($3::boolean OR EXISTS (
                          SELECT 1 FROM unnest($4::bigint[], $5::bigint[]) AS selected(job_id, run_lease)
                          WHERE selected.job_id = leases.job_id AND selected.run_lease = leases.run_lease
                      ))
                    ORDER BY deadline_at ASC
                    LIMIT $6
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
            .bind(&excluded_job_ids)
            .bind(&excluded_run_leases)
            .bind(selected.is_none())
            .bind(&selected_job_ids)
            .bind(&selected_run_leases)
            .bind(RECEIPT_RESCUE_BATCH_LIMIT)
            .fetch_all(attempt.as_mut())
            .await
            .map_err(map_sqlx_error)?;
            let mut saturated = deleted.len() as i64 >= RECEIPT_RESCUE_BATCH_LIMIT;

            // Receipts-mode short-path claims hold their deadline on
            // `lease_claims.deadline_at` rather than on a `leases` row, so
            // the receipt-plane needs its own scan; merge both populations
            // into one `moved` set so the maintenance caller observes a
            // single rescue batch per tick.
            let receipt_excluded: BTreeSet<_> = excluded
                .iter()
                .copied()
                .chain(deleted.iter().map(|row| (row.job_id, row.run_lease)))
                .collect();
            let mut receipts = ReceiptRescueBatch::default();
            if self.lease_claim_receipts() {
                self.rescue_expired_receipt_deadlines_tx(
                    &mut attempt,
                    options,
                    &receipt_excluded,
                    selected.as_ref(),
                    &mut receipts,
                )
                .await?;
            }
            saturated |= receipts.saturated;
            let ReceiptRescueBatch {
                rescued: receipt_deleted,
                idle_slots,
                scan_progress,
                ..
            } = receipts;

            selected.get_or_insert_with(|| {
                deleted
                    .iter()
                    .chain(receipt_deleted.iter())
                    .map(|row| (row.job_id, row.run_lease))
                    .collect()
            });

            if deleted.is_empty() && receipt_deleted.is_empty() {
                attempt.commit().await.map_err(map_sqlx_error)?;
                tx.commit().await.map_err(map_sqlx_error)?;
                return Ok(RescueBatch {
                    idle_slots,
                    scan_progress,
                    saturated,
                    ..RescueBatch::default()
                });
            }

            let mut moved = match self
                .try_hydrate_deleted_leases_tx(&mut attempt, deleted)
                .await?
            {
                Ok(moved) => moved,
                Err(busy) => {
                    attempt.rollback().await.map_err(map_sqlx_error)?;
                    busy.exclude(&mut excluded, &mut selected)?;
                    continue;
                }
            };
            match self
                .try_hydrate_deleted_leases_tx(&mut attempt, receipt_deleted)
                .await?
            {
                Ok(receipts_moved) => moved.extend(receipts_moved),
                Err(busy) => {
                    attempt.rollback().await.map_err(map_sqlx_error)?;
                    busy.exclude(&mut excluded, &mut selected)?;
                    continue;
                }
            }

            let rescued = self
                .settle_rescued_attempts_tx(
                    &mut attempt,
                    vec![(moved, "hard deadline exceeded")],
                    RescueDlq {
                        reason: "deadline_exceeded",
                        enabled: dlq_enabled,
                    },
                )
                .await?;
            attempt.commit().await.map_err(map_sqlx_error)?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RescueBatch {
                rescued,
                saturated,
                idle_slots,
                scan_progress,
            });
        }
    }

    /// Rescue attempts whose callback timed out. Exhausted attempts fail
    /// into `done_entries`; see [`Self::rescue_expired_callbacks_with_dlq`]
    /// to dead-letter them instead.
    pub async fn rescue_expired_callbacks(&self, pool: &PgPool) -> Result<Vec<JobRow>, AwaError> {
        self.rescue_expired_callbacks_with_dlq(pool, &|_| false)
            .await
    }

    /// [`Self::rescue_expired_callbacks`], moving an exhausted attempt into
    /// `dlq_entries` (reason `callback_timeout`) in the rescue transaction
    /// when `dlq_enabled` returns true for its queue.
    pub async fn rescue_expired_callbacks_with_dlq(
        &self,
        pool: &PgPool,
        dlq_enabled: &(dyn Fn(&str) -> bool + Sync),
    ) -> Result<Vec<JobRow>, AwaError> {
        Ok(self
            .rescue_expired_callbacks_batch(pool, dlq_enabled, &RescueBatchOptions::default())
            .await?
            .rescued)
    }

    /// One callback-timeout rescue batch in its own transaction: up to
    /// 500 attempts. The options do not apply: callback waits live in
    /// `leases`, not in the claim ring.
    #[tracing::instrument(
        skip(self, pool, dlq_enabled, _options),
        name = "queue_storage.rescue_expired_callbacks"
    )]
    pub async fn rescue_expired_callbacks_batch(
        &self,
        pool: &PgPool,
        dlq_enabled: &(dyn Fn(&str) -> bool + Sync),
        _options: &RescueBatchOptions,
    ) -> Result<RescueBatch, AwaError> {
        let schema = self.schema();
        let mut tx = pool
            .begin_with(RESCUE_TRANSACTION_BEGIN)
            .await
            .map_err(map_sqlx_error)?;
        let mut excluded = BTreeSet::new();
        let mut selected: Option<BTreeSet<(i64, i64)>> = None;
        loop {
            let (excluded_job_ids, excluded_run_leases): (Vec<_>, Vec<_>) =
                excluded.iter().copied().unzip();
            let (selected_job_ids, selected_run_leases): (Vec<i64>, Vec<i64>) = selected
                .as_ref()
                .map(|pairs| pairs.iter().copied().unzip())
                .unwrap_or_default();
            // isolation: a savepoint, at the enclosing transaction's level
            let mut attempt = tx.begin().await.map_err(map_sqlx_error)?;
            let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
                r#"
                DELETE FROM {schema}.leases
                WHERE (job_id, run_lease) IN (
                    SELECT job_id, run_lease
                    FROM {schema}.leases
                    WHERE state = 'waiting_external'
                      AND callback_timeout_at IS NOT NULL
                      AND callback_timeout_at < clock_timestamp()
                      AND NOT EXISTS (
                          SELECT 1 FROM unnest($1::bigint[], $2::bigint[]) AS excluded(job_id, run_lease)
                          WHERE excluded.job_id = leases.job_id AND excluded.run_lease = leases.run_lease
                      )
                      AND ($3::boolean OR EXISTS (
                          SELECT 1 FROM unnest($4::bigint[], $5::bigint[]) AS selected(job_id, run_lease)
                          WHERE selected.job_id = leases.job_id AND selected.run_lease = leases.run_lease
                      ))
                    ORDER BY callback_timeout_at ASC
                    LIMIT $6
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
            .bind(&excluded_job_ids)
            .bind(&excluded_run_leases)
            .bind(selected.is_none())
            .bind(&selected_job_ids)
            .bind(&selected_run_leases)
            .bind(RECEIPT_RESCUE_BATCH_LIMIT)
            .fetch_all(attempt.as_mut())
            .await
            .map_err(map_sqlx_error)?;
            let saturated = deleted.len() as i64 >= RECEIPT_RESCUE_BATCH_LIMIT;

            selected.get_or_insert_with(|| {
                deleted
                    .iter()
                    .map(|row| (row.job_id, row.run_lease))
                    .collect()
            });

            if deleted.is_empty() {
                attempt.commit().await.map_err(map_sqlx_error)?;
                tx.commit().await.map_err(map_sqlx_error)?;
                return Ok(RescueBatch::default());
            }

            let moved = match self
                .try_hydrate_deleted_leases_tx(&mut attempt, deleted)
                .await?
            {
                Ok(moved) => moved,
                Err(busy) => {
                    attempt.rollback().await.map_err(map_sqlx_error)?;
                    busy.exclude(&mut excluded, &mut selected)?;
                    continue;
                }
            };

            let rescued = self
                .settle_rescued_attempts_tx(
                    &mut attempt,
                    vec![(moved, "callback timed out")],
                    RescueDlq {
                        reason: "callback_timeout",
                        enabled: dlq_enabled,
                    },
                )
                .await?;
            attempt.commit().await.map_err(map_sqlx_error)?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RescueBatch {
                rescued,
                saturated,
                idle_slots: Vec::new(),
                scan_progress: Vec::new(),
            });
        }
    }

    /// Promote up to `batch_size` due `state` rows into the ready ring.
    /// Equivalent to [`Self::promote_due_lanes`] with no lock timeout and no
    /// skipped lanes, returning the number of rows promoted.
    pub async fn promote_due(
        &self,
        pool: &PgPool,
        state: JobState,
        batch_size: i64,
    ) -> Result<usize, AwaError> {
        Ok(self
            .promote_due_lanes(pool, state, batch_size, None, &[])
            .await?
            .promoted)
    }

    /// Select up to `batch_size` due `state` rows, oldest `run_at` first,
    /// outside the lanes in `skip`, and promote each lane's share in its own
    /// transaction.
    ///
    /// The selection skips rows another transaction has locked, such as a
    /// lane promotion still waiting for its lane, so they cannot fill every
    /// batch of a tick. It runs as its own statement, so its row locks end
    /// with it; `deferred_jobs` is not partitioned, so no run-time pruning
    /// meets those locks.
    ///
    /// Re-enqueueing a row takes its lane's enqueue reservation lock, which
    /// producers hold until they commit, so a lane under heavy enqueue load
    /// can keep its promotion waiting. A transaction per lane confines that
    /// wait to the lane's own rows. With `lane_lock_timeout`, each lane's
    /// transaction runs under that `lock_timeout`; a lane that hits it rolls
    /// back, keeps its rows deferred and is reported in
    /// [`PromoteOutcome::contended`], and the remaining lanes go ahead.
    ///
    /// A due row whose unique key another live job holds is cancelled as a
    /// duplicate in its lane's transaction instead of being promoted.
    pub async fn promote_due_lanes(
        &self,
        pool: &PgPool,
        state: JobState,
        batch_size: i64,
        lane_lock_timeout: Option<Duration>,
        skip: &[PromotionLane],
    ) -> Result<PromoteOutcome, AwaError> {
        self.promote_due_lanes_with_cancelled(
            pool,
            state,
            batch_size,
            lane_lock_timeout,
            skip,
            &mut Vec::new(),
        )
        .await
    }

    /// [`Self::promote_due_lanes`], also appending to `cancelled` the rows it
    /// cancelled as duplicates. Each lane commits on its own, so the rows of
    /// lanes that committed are appended even when a later lane returns an
    /// error.
    #[doc(hidden)]
    pub async fn promote_due_lanes_with_cancelled(
        &self,
        pool: &PgPool,
        state: JobState,
        batch_size: i64,
        lane_lock_timeout: Option<Duration>,
        skip: &[PromotionLane],
        cancelled: &mut Vec<JobRow>,
    ) -> Result<PromoteOutcome, AwaError> {
        if !matches!(state, JobState::Scheduled | JobState::Retryable) || batch_size <= 0 {
            return Ok(Default::default());
        }

        let schema = self.schema();
        let due: Vec<(i64, String, i16)> = sqlx::query_as(&format!(
            r#"
            SELECT job_id, queue, priority
            FROM {schema}.deferred_jobs AS deferred
            WHERE state = $1
              AND run_at <= clock_timestamp()
              AND NOT EXISTS (
                  SELECT 1 FROM awa.queue_meta
                  WHERE queue = deferred.queue AND paused = TRUE
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM unnest($3::text[], $4::smallint[]) AS skip (queue, priority)
                  WHERE skip.queue = deferred.queue AND skip.priority = deferred.priority
              )
            ORDER BY run_at ASC, priority ASC, job_id ASC
            LIMIT $2
            FOR UPDATE OF deferred SKIP LOCKED
            "#
        ))
        .bind(state)
        .bind(batch_size)
        .bind(
            skip.iter()
                .map(|lane| lane.queue.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(skip.iter().map(|lane| lane.priority).collect::<Vec<_>>())
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;

        let mut outcome = PromoteOutcome {
            selected: due.len(),
            ..PromoteOutcome::default()
        };
        let mut lanes: BTreeMap<PromotionLane, Vec<i64>> = BTreeMap::new();
        for (job_id, queue, priority) in due {
            lanes
                .entry(PromotionLane { queue, priority })
                .or_default()
                .push(job_id);
        }
        let mut promoted_queues = BTreeSet::new();
        for (lane, job_ids) in lanes {
            let promoted = self
                .promote_lane(
                    pool,
                    state,
                    &lane,
                    DueLaneRows::Selected(&job_ids),
                    lane_lock_timeout,
                )
                .await;
            match promoted {
                Ok(Some(promoted)) => {
                    outcome.promoted += promoted.promoted;
                    cancelled.extend(promoted.cancelled);
                    if promoted.promoted > 0 {
                        promoted_queues.insert(lane.queue);
                    }
                }
                Ok(None) => outcome.contended.push(lane),
                Err(err) => {
                    self.notify_promoted_queues(pool, promoted_queues);
                    return Err(err);
                }
            }
        }
        self.notify_promoted_queues(pool, promoted_queues);
        Ok(outcome)
    }

    /// Promote up to `batch_size` due `state` rows of one lane, oldest
    /// `run_at` first, in one transaction. With `lane_lock_timeout`, a
    /// transaction that hits it rolls back and the lane is returned in
    /// [`PromoteOutcome::contended`] with nothing selected.
    pub async fn promote_due_lane(
        &self,
        pool: &PgPool,
        state: JobState,
        lane: &PromotionLane,
        batch_size: i64,
        lane_lock_timeout: Option<Duration>,
    ) -> Result<PromoteOutcome, AwaError> {
        self.promote_due_lane_with_cancelled(
            pool,
            state,
            lane,
            batch_size,
            lane_lock_timeout,
            &mut Vec::new(),
        )
        .await
    }

    /// [`Self::promote_due_lane`], also appending to `cancelled` the rows it
    /// cancelled as duplicates.
    #[doc(hidden)]
    pub async fn promote_due_lane_with_cancelled(
        &self,
        pool: &PgPool,
        state: JobState,
        lane: &PromotionLane,
        batch_size: i64,
        lane_lock_timeout: Option<Duration>,
        cancelled: &mut Vec<JobRow>,
    ) -> Result<PromoteOutcome, AwaError> {
        if !matches!(state, JobState::Scheduled | JobState::Retryable) || batch_size <= 0 {
            return Ok(Default::default());
        }
        let promoted = self
            .promote_lane(
                pool,
                state,
                lane,
                DueLaneRows::Oldest(batch_size),
                lane_lock_timeout,
            )
            .await?;
        if promoted
            .as_ref()
            .is_some_and(|promoted| promoted.promoted > 0)
        {
            self.notify_promoted_queues(pool, [lane.queue.clone()]);
        }
        Ok(match promoted {
            Some(promoted) => {
                let selected = promoted.promoted + promoted.cancelled.len();
                cancelled.extend(promoted.cancelled);
                PromoteOutcome {
                    promoted: promoted.promoted,
                    selected,
                    contended: Vec::new(),
                }
            }
            None => PromoteOutcome {
                contended: vec![lane.clone()],
                ..PromoteOutcome::default()
            },
        })
    }

    /// One lane's promotion transaction. `None` when its locks hit
    /// `lock_timeout` and it rolled back.
    async fn promote_lane(
        &self,
        pool: &PgPool,
        state: JobState,
        lane: &PromotionLane,
        rows: DueLaneRows<'_>,
        lock_timeout: Option<Duration>,
    ) -> Result<Option<LanePromotion>, AwaError> {
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;
        match self
            .promote_lane_tx(&mut tx, state, lane, rows, lock_timeout)
            .await
        {
            Ok(promoted) => {
                tx.commit().await.map_err(map_sqlx_error)?;
                Ok(Some(promoted))
            }
            Err(AwaError::Database(err)) if is_lock_contention_error(&err) => {
                tx.rollback().await.map_err(map_sqlx_error)?;
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    async fn promote_lane_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        state: JobState,
        lane: &PromotionLane,
        rows: DueLaneRows<'_>,
        lock_timeout: Option<Duration>,
    ) -> Result<LanePromotion, AwaError> {
        if let Some(timeout) = lock_timeout {
            set_lock_timeout_tx(tx, timeout).await?;
        }

        let schema = self.schema();
        let (row_filter, limit) = match rows {
            DueLaneRows::Selected(_) => ("AND job_id = ANY($4)", ""),
            DueLaneRows::Oldest(_) => ("", "ORDER BY run_at ASC, job_id ASC LIMIT $4"),
        };
        let sql = format!(
            r#"
            DELETE FROM {schema}.deferred_jobs
            WHERE job_id IN (
                SELECT job_id
                FROM {schema}.deferred_jobs AS deferred
                WHERE state = $1
                  AND queue = $2
                  AND priority = $3
                  {row_filter}
                  AND run_at <= clock_timestamp()
                  AND NOT EXISTS (
                      SELECT 1 FROM awa.queue_meta
                      WHERE queue = deferred.queue AND paused = TRUE
                  )
                {limit}
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
        );
        let query = sqlx::query_as::<_, DeferredJobRow>(&sql)
            .bind(state)
            .bind(&lane.queue)
            .bind(lane.priority);
        let query = match rows {
            DueLaneRows::Selected(job_ids) => query.bind(job_ids),
            DueLaneRows::Oldest(batch_size) => query.bind(batch_size),
        };
        let mut moved: Vec<DeferredJobRow> =
            query.fetch_all(tx.as_mut()).await.map_err(map_sqlx_error)?;
        if moved.is_empty() {
            return Ok(LanePromotion::default());
        }
        // DELETE ... RETURNING has no order; lane sequence follows due order.
        moved.sort_unstable_by_key(|row| (row.run_at, row.job_id));

        let mut due_run_ats = Vec::with_capacity(moved.len());
        let rows: Vec<(ExistingReadyRow, Option<JobState>)> = moved
            .into_iter()
            .map(|row| {
                due_run_ats.push(row.run_at);
                let ready = ExistingReadyRow {
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
                };
                (ready, Some(state))
            })
            .collect();
        let conflicts = self.sync_reenqueue_unique_claims(tx, &rows, true).await?;
        if conflicts.is_empty() {
            let ready_rows = rows.into_iter().map(|(row, _)| row).collect();
            let promoted = self.insert_claimed_ready_rows_tx(tx, ready_rows).await?;
            return Ok(LanePromotion {
                promoted,
                cancelled: Vec::new(),
            });
        }

        let mut ready_rows = Vec::with_capacity(rows.len());
        let mut duplicates = Vec::with_capacity(conflicts.len());
        for ((row, _), due_run_at) in rows.into_iter().zip(due_run_ats) {
            if conflicts.contains(&row.job_id) {
                duplicates.push((row, due_run_at));
            } else {
                ready_rows.push(row);
            }
        }
        let cancelled = self
            .cancel_promoted_duplicates_tx(tx, state, lane, duplicates)
            .await?;
        let promoted = self.insert_claimed_ready_rows_tx(tx, ready_rows).await?;
        Ok(LanePromotion {
            promoted,
            cancelled,
        })
    }

    /// Cancel due rows whose promotion lost their unique key to another job.
    /// The cancelled row keeps its unique key with `cancelled` cleared from
    /// its mask and the original mask recorded in its error entry, so it
    /// never holds the key and a later retry restores the mask and re-checks
    /// ownership.
    async fn cancel_promoted_duplicates_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        state: JobState,
        lane: &PromotionLane,
        duplicates: Vec<(ExistingReadyRow, DateTime<Utc>)>,
    ) -> Result<Vec<JobRow>, AwaError> {
        let (ready_slot, ready_generation) = self.current_queue_ring(tx).await?;
        let finalized_at = Utc::now().trunc_subsecs(6);
        let done: Vec<DoneJobRow> = duplicates
            .into_iter()
            .map(|(row, due_run_at)| {
                let mut payload = RuntimePayload::from_json(row.payload)?;
                payload.push_error(lifecycle_error(PROMOTED_DUPLICATE_ERROR, row.attempt, true));
                let mut done = DoneJobRow {
                    ready_slot,
                    ready_generation,
                    job_id: row.job_id,
                    kind: row.kind,
                    queue: row.queue,
                    args: row.args,
                    state: JobState::Cancelled,
                    priority: row.priority,
                    attempt: row.attempt,
                    run_lease: row.run_lease,
                    max_attempts: row.max_attempts,
                    lane_seq: -row.job_id,
                    enqueue_shard: 0,
                    run_at: due_run_at,
                    attempted_at: row.attempted_at,
                    finalized_at,
                    created_at: row.created_at,
                    unique_key: row.unique_key,
                    unique_states: row.unique_states,
                    payload: payload.into_json(),
                };
                // The row keeps its key, so a retry restores the mask and
                // re-checks ownership; without `cancelled` in its mask the
                // cancelled row never holds the key.
                if unique_state_claims(done.unique_states.as_deref(), JobState::Cancelled) {
                    clear_cancelled_claim(&mut done)?;
                }
                Ok(done)
            })
            .collect::<Result<_, AwaError>>()?;
        self.ensure_lane(tx, &lane.queue, lane.priority, 0).await?;
        // No claim changes: the due row held no claim, since its promotion's
        // claim conflicted, and its cancelled state claims nothing. Writing
        // the rows directly keeps every claim change ahead of the lane locks.
        self.write_done_rows_tx(tx, &done, Some(state)).await?;
        done.into_iter().map(DoneJobRow::into_job_row).collect()
    }

    /// Wake claimers of queues whose promotion has committed.
    ///
    /// Promotion notifies after its lane transactions instead of inside them.
    /// A notifying transaction takes PostgreSQL's notify-queue lock at
    /// `COMMIT`, which every notifying transaction in the cluster waits its
    /// turn for, and a lane transaction would hold its lane's locks, and run
    /// under its lock timeout, while it waited.
    fn notify_promoted_queues(&self, pool: &PgPool, queues: impl IntoIterator<Item = String>) {
        self.wake_queues_after_commit(pool, queues);
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

    /// Returns `true` if any of `relations` has at least one row, checking in
    /// order and stopping at the first non-empty one. Used by the ring prune
    /// paths to prove an oldest sealed slot is empty before taking
    /// `ACCESS EXCLUSIVE` and issuing a destructive `TRUNCATE` on it.
    async fn any_relation_has_rows_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        relations: &[&str],
    ) -> Result<bool, AwaError> {
        for relation in relations {
            if Self::relation_has_rows_tx(tx, relation).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// TRUNCATE those of `children` that hold reclaimable storage. The caller
    /// must already hold ACCESS EXCLUSIVE on every child; a child it skips has
    /// no heap page, so it holds no row.
    async fn truncate_used_children_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        children: &[&str],
    ) -> Result<(), AwaError> {
        let mut targets: Vec<&str> = Vec::with_capacity(children.len());
        for child in children {
            let reclaimable: bool = sqlx::query_scalar(RECLAIMABLE_CHILD_STORAGE_SQL)
                .bind(child)
                .fetch_one(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            if reclaimable {
                targets.push(child);
            }
        }
        if !targets.is_empty() {
            sqlx::query(&format!("TRUNCATE TABLE {}", targets.join(", ")))
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
        }
        Ok(())
    }

    /// The ring's cursor when no partition of any table in `relations` holds a
    /// row, otherwise `None`. Runs in a read-only transaction of its own, which
    /// takes no row lock, assigns no transaction id and writes no WAL. It takes
    /// `ACCESS SHARE` on the partitions in `relations` order, which must be the
    /// order the ring's prune requests `ACCESS EXCLUSIVE`.
    async fn idle_ring_cursor(
        &self,
        conn: &mut sqlx::PgConnection,
        ring_state: &str,
        relations: &[String],
    ) -> Result<Option<(i32, i64)>, AwaError> {
        let schema = self.schema();
        let mut tx = crate::isolation::begin_read_committed_on(conn)
            .await
            .map_err(map_sqlx_error)?;
        let refs: Vec<&str> = relations.iter().map(String::as_str).collect();
        if Self::any_relation_has_rows_tx(&mut tx, &refs).await? {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        }
        let cursor: (i32, i64) = sqlx::query_as(&format!(
            "SELECT current_slot, generation FROM {schema}.{ring_state} WHERE singleton = TRUE"
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(cursor))
    }

    /// Every table of the queue ring, plus the per-slot terminal counters that
    /// queue prune deletes with a slot, in queue prune's lock order.
    fn queue_ring_relations(&self) -> Vec<String> {
        let schema = self.schema();
        [
            "ready_segments",
            "done_entries",
            "ready_entries",
            "ready_claim_attempt_batches",
            "ready_tombstones",
            "receipt_completion_batches",
            "receipt_completion_tombstones",
            "queue_terminal_count_deltas",
            "queue_terminal_live_counts",
        ]
        .iter()
        .map(|table| format!("{schema}.{table}"))
        .collect()
    }

    fn claim_ring_relations(&self) -> Vec<String> {
        let schema = self.schema();
        [
            "lease_claims",
            "lease_claim_batches",
            "lease_claim_closures",
            "lease_claim_closure_batches",
        ]
        .iter()
        .map(|table| format!("{schema}.{table}"))
        .collect()
    }

    /// [`Self::idle_ring_cursor`] on a connection taken from `pool` and
    /// returned before the caller rotates.
    async fn idle_ring_cursor_pooled(
        &self,
        pool: &PgPool,
        ring_state: &str,
        relations: &[String],
    ) -> Result<Option<(i32, i64)>, AwaError> {
        let mut conn = pool.acquire().await.map_err(map_sqlx_error)?;
        self.idle_ring_cursor(&mut conn, ring_state, relations)
            .await
    }

    /// [`Self::rotate`], skipped when every partition of the queue ring is
    /// empty.
    ///
    /// An empty ring has nothing to seal, and [`Self::prune_oldest`] on it
    /// returns [`PruneOutcome::Noop`], so skipping leaves the cursor where it
    /// is without stranding a row. Once any row lands the next call rotates
    /// as [`Self::rotate`] does.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.rotate_if_active")]
    pub async fn rotate_if_active(&self, pool: &PgPool) -> Result<GatedRotateOutcome, AwaError> {
        let relations = self.queue_ring_relations();
        let idle = self
            .idle_ring_cursor_pooled(pool, "queue_ring_state", &relations)
            .await?;
        self.finish_gated_rotate(idle, pool).await
    }

    /// [`Self::rotate_if_active`] with the emptiness check run on `check_conn`
    /// instead of a pooled connection. Rotation still uses `pool`.
    ///
    /// The check loads every ring partition's metadata into the session that
    /// runs it, so a caller that checks on one long-lived connection keeps
    /// that metadata out of the pool's sessions. On `Ok`, `check_conn` is not
    /// inside a transaction; on `Err` it may be, and the caller should
    /// discard it.
    #[tracing::instrument(skip(self, check_conn, pool), name = "queue_storage.rotate_if_active")]
    pub async fn rotate_if_active_on(
        &self,
        check_conn: &mut sqlx::PgConnection,
        pool: &PgPool,
    ) -> Result<GatedRotateOutcome, AwaError> {
        let relations = self.queue_ring_relations();
        let idle = self
            .idle_ring_cursor(check_conn, "queue_ring_state", &relations)
            .await?;
        self.finish_gated_rotate(idle, pool).await
    }

    async fn finish_gated_rotate(
        &self,
        idle: Option<(i32, i64)>,
        pool: &PgPool,
    ) -> Result<GatedRotateOutcome, AwaError> {
        match idle {
            Some((slot, generation)) => Ok(GatedRotateOutcome::SkippedIdle { slot, generation }),
            None => self.rotate(pool).await.map(GatedRotateOutcome::Attempted),
        }
    }

    /// [`Self::rotate_leases`], skipped when every lease partition is empty.
    /// See [`Self::rotate_if_active`].
    #[tracing::instrument(skip(self, pool), name = "queue_storage.rotate_leases_if_active")]
    pub async fn rotate_leases_if_active(
        &self,
        pool: &PgPool,
    ) -> Result<GatedRotateOutcome, AwaError> {
        let relations = [self.leases_table()];
        let idle = self
            .idle_ring_cursor_pooled(pool, "lease_ring_state", &relations)
            .await?;
        self.finish_gated_rotate_leases(idle, pool).await
    }

    /// [`Self::rotate_leases_if_active`] with the emptiness check run on
    /// `check_conn`. See [`Self::rotate_if_active_on`].
    #[tracing::instrument(
        skip(self, check_conn, pool),
        name = "queue_storage.rotate_leases_if_active"
    )]
    pub async fn rotate_leases_if_active_on(
        &self,
        check_conn: &mut sqlx::PgConnection,
        pool: &PgPool,
    ) -> Result<GatedRotateOutcome, AwaError> {
        let relations = [self.leases_table()];
        let idle = self
            .idle_ring_cursor(check_conn, "lease_ring_state", &relations)
            .await?;
        self.finish_gated_rotate_leases(idle, pool).await
    }

    async fn finish_gated_rotate_leases(
        &self,
        idle: Option<(i32, i64)>,
        pool: &PgPool,
    ) -> Result<GatedRotateOutcome, AwaError> {
        match idle {
            Some((slot, generation)) => Ok(GatedRotateOutcome::SkippedIdle { slot, generation }),
            None => self
                .rotate_leases(pool)
                .await
                .map(GatedRotateOutcome::Attempted),
        }
    }

    /// [`Self::rotate_claims`], skipped when every partition of the claim
    /// ring is empty. See [`Self::rotate_if_active`].
    #[tracing::instrument(skip(self, pool), name = "queue_storage.rotate_claims_if_active")]
    pub async fn rotate_claims_if_active(
        &self,
        pool: &PgPool,
    ) -> Result<GatedRotateOutcome, AwaError> {
        let relations = self.claim_ring_relations();
        let idle = self
            .idle_ring_cursor_pooled(pool, "claim_ring_state", &relations)
            .await?;
        self.finish_gated_rotate_claims(idle, pool).await
    }

    /// [`Self::rotate_claims_if_active`] with the emptiness check run on
    /// `check_conn`. See [`Self::rotate_if_active_on`].
    #[tracing::instrument(
        skip(self, check_conn, pool),
        name = "queue_storage.rotate_claims_if_active"
    )]
    pub async fn rotate_claims_if_active_on(
        &self,
        check_conn: &mut sqlx::PgConnection,
        pool: &PgPool,
    ) -> Result<GatedRotateOutcome, AwaError> {
        let relations = self.claim_ring_relations();
        let idle = self
            .idle_ring_cursor(check_conn, "claim_ring_state", &relations)
            .await?;
        self.finish_gated_rotate_claims(idle, pool).await
    }

    async fn finish_gated_rotate_claims(
        &self,
        idle: Option<(i32, i64)>,
        pool: &PgPool,
    ) -> Result<GatedRotateOutcome, AwaError> {
        match idle {
            Some((slot, generation)) => Ok(GatedRotateOutcome::SkippedIdle { slot, generation }),
            None => self
                .rotate_claims(pool)
                .await
                .map(GatedRotateOutcome::Attempted),
        }
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.rotate")]
    pub async fn rotate(&self, pool: &PgPool) -> Result<RotateOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;

        let state: (i32, i64, i32) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot, generation, slot_count
            FROM {schema}.queue_ring_state
            WHERE singleton = TRUE
            FOR UPDATE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

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

        let next_generation = state.1 + 1;

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.queue_ring_state
            SET current_slot = $1,
                generation = $2
            WHERE singleton = TRUE
            "#
        ))
        .bind(next_slot)
        .bind(next_generation)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.queue_ring_slots
            SET generation = $2
            WHERE slot = $1
            "#
        ))
        .bind(next_slot)
        .bind(next_generation)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;
        // Raise the generation floor to the generation just committed, so a
        // producer whose snapshot predates this rotation (or many) writes
        // into it rather than into a slot the ring may since have reused
        // (#847). Not inside the rotation transaction: a commit that failed
        // after the raise would leave the floor ahead of the ring. Producers
        // raise the floor themselves, so a failure here only leaves that case
        // open until the next producer or rotation.
        match self.enqueue_ring_cursor_callable(pool).await {
            Ok(true) => {
                if let Err(err) =
                    sqlx::query(&format!("SELECT FROM {schema}.enqueue_ring_cursor()"))
                        .execute(pool)
                        .await
                {
                    tracing::warn!(
                        schema,
                        generation = next_generation,
                        error = %err,
                        "Failed to raise the queue-ring generation floor after rotation"
                    );
                }
            }
            Ok(false) => {}
            Err(err) => tracing::warn!(
                schema,
                generation = next_generation,
                error = %err,
                "Failed to check for the queue-ring generation floor after rotation"
            ),
        }
        Ok(RotateOutcome::Rotated {
            slot: next_slot,
            generation: next_generation,
        })
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.rotate_leases")]
    pub async fn rotate_leases(&self, pool: &PgPool) -> Result<RotateOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;

        // FOR UPDATE serialises with prune_oldest_leases and parallel
        // rotators. Without it two rotators can both pass the busy-check,
        // both compute the same next_slot, and the loser's CAS update
        // wastes work. `RotateLeasesPlan` in
        // `correctness/storage/AwaStorageLockOrder.tla` requires this
        // lock as the first acquired resource for the rotation tx.
        let state: (i32, i64, i32) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot, generation, slot_count
            FROM {schema}.lease_ring_state
            WHERE singleton = TRUE
            FOR UPDATE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

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

        let next_generation = state.1 + 1;

        let rotated = sqlx::query(&format!(
            r#"
            UPDATE {schema}.lease_ring_state
            SET current_slot = $1,
                generation = $2
            WHERE singleton = TRUE
              AND current_slot = $3
              AND generation = $4
            "#
        ))
        .bind(next_slot)
        .bind(next_generation)
        .bind(state.0)
        .bind(state.1)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if rotated.rows_affected() == 0 {
            // Another rotator beat us to the CAS. Report the bounded
            // presence evidence we sampled before giving up.
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;

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

        let schema = self.schema();
        let current_slot: i32 = sqlx::query_scalar(&format!(
            r#"
            SELECT current_slot
            FROM {schema}.queue_ring_state
            WHERE singleton = TRUE
            "#
        ))
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        let slots = self
            .terminal_delta_rollup_candidates(pool, current_slot)
            .await?;

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
                    // The next slot would most likely wait out the same lock
                    // timeout, and the leader loop is blocked while it does.
                    outcome.blocked_slots += 1;
                    break;
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
        current_slot: i32,
    ) -> Result<Vec<(i32, i64)>, AwaError> {
        let schema = self.schema();
        let sealed_slots: Vec<(i32, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT slot, generation
            FROM {schema}.queue_ring_slots
            WHERE generation >= 0
              AND slot <> $1
            ORDER BY generation ASC, slot ASC
            "#
        ))
        .bind(current_slot)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;

        let mut pending_slots = Vec::new();
        for (slot, generation) in sealed_slots {
            let Ok(slot_index) = usize::try_from(slot) else {
                continue;
            };
            let delta_child = terminal_delta_child_name(schema, slot_index);
            // Deltas of any generation: the rollup folds every delta in the
            // slot before it truncates the slot's delta partition.
            let has_pending: bool = sqlx::query_scalar(&format!(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM {delta_child}
                    LIMIT 1
                )
                "#
            ))
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;

        let current_slot: i32 = sqlx::query_scalar(&format!(
            r#"
            SELECT current_slot
            FROM {schema}.queue_ring_state
            WHERE singleton = TRUE
            FOR UPDATE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let slot_generation: Option<i64> = sqlx::query_scalar(&format!(
            r#"
            SELECT generation
            FROM {schema}.queue_ring_slots
            WHERE slot = $1
            FOR UPDATE
            "#
        ))
        .bind(slot)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(slot_generation) = slot_generation else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::Empty);
        };

        if current_slot == slot {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::SkippedActive);
        }

        if slot_generation != generation {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::Empty);
        }

        if Self::terminal_delta_rollup_mvcc_horizon_pinned_tx(&mut tx).await? {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::SkippedMvccPinned);
        }

        let ready_child = ready_child_name(schema, slot as usize);
        let pending_ready = queue_prune_has_pending_ready_tx(&mut tx, schema, &ready_child).await?;

        if pending_ready {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(TerminalDeltaSlotRollup::SkippedActive);
        }

        let locked: Result<TerminalDeltaSlotRollup, AwaError> = async {
            set_lock_timeout_tx(&mut tx, self.prune_lock_timeout).await?;

            sqlx::query(&format!(
                "LOCK TABLE {delta_child} IN ACCESS EXCLUSIVE MODE"
            ))
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            let active_leases =
                queue_prune_has_active_leases_tx(&mut tx, schema, slot).await?;

            if active_leases {
                return Ok(TerminalDeltaSlotRollup::SkippedActive);
            }

            let unclosed_claim_refs =
                queue_prune_has_unclosed_claim_refs_tx(&mut tx, schema, slot).await?;

            if unclosed_claim_refs {
                return Ok(TerminalDeltaSlotRollup::SkippedActive);
            }

            // Every delta in the slot, whatever its generation: the TRUNCATE
            // below removes them all, and a job written into an older
            // generation without the `ring_generation_floor` patch (#847)
            // leaves its deltas here too.
            let delta_rows: i64 = sqlx::query_scalar(&format!(
                r#"
                SELECT count(*)::bigint
                FROM {delta_child}
                "#
            ))
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            if delta_rows == 0 {
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
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!("TRUNCATE TABLE {delta_child}"))
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
            Ok(TerminalDeltaSlotRollup::Rolled {
                delta_rows,
                grouped_keys,
            })
        }
        .await;

        settle_prune_tx(tx, locked, TerminalDeltaSlotRollup::Blocked).await
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;

        let state: (i32,) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot
            FROM {schema}.queue_ring_state
            WHERE singleton = TRUE
            FOR UPDATE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let target: Option<(i32, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT slot, generation
            FROM {schema}.queue_ring_slots
            WHERE generation >= 0
              AND slot <> $1
            ORDER BY generation ASC, slot ASC
            LIMIT 1
            FOR UPDATE
            "#
        ))
        .bind(state.0)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some((slot, _generation)) = target else {
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

        // Idle-prune churn guard: on an idle queue the rotation cursor keeps
        // sealing empty slots, so without this gate the prune would re-take
        // ACCESS EXCLUSIVE and re-issue TRUNCATE on an already-empty sealed
        // slot every maintenance tick — continuous relfilenode churn and
        // WAL-flushing commits with zero jobs in the system. That idle DDL is
        // near-free on local-SSD Postgres but saturates the IO path on
        // disaggregated-storage engines such as AlloyDB, inflating p99 latency
        // for co-located application queries. If every child partition this
        // prune would truncate is already empty there is nothing to reclaim,
        // so commit and report Noop instead of truncating. The slot is sealed
        // (slot <> current_slot) and its ring-state and slot rows are held FOR
        // UPDATE above, so the emptiness snapshot cannot race a writer.
        let slot_has_reclaimable_rows = Self::any_relation_has_rows_tx(
            &mut tx,
            &[
                &ready_child,
                &claim_attempt_child,
                &done_child,
                &tomb_child,
                &segment_child,
                &receipt_batch_child,
                &receipt_tomb_child,
                &delta_child,
            ],
        )
        .await?;
        if !slot_has_reclaimable_rows {
            // The destructive prune path also deletes this slot's
            // queue_terminal_live_counts rows alongside the TRUNCATE. Those
            // rows are a materialized per-slot aggregate of done_entries, so an
            // empty done partition normally means none exist. Counter drift
            // (possible during a rolling upgrade from a pre-counter binary) can
            // nonetheless leave orphan rows for a slot whose ring children are
            // already empty. Preserve that cleanup on the skip path so avoiding
            // the (no-op) TRUNCATE does not strand counter rows and let the
            // exact per-queue counters drift indefinitely. This is a cheap
            // 0-row DELETE in the steady state and needs no ACCESS EXCLUSIVE.
            sqlx::query(&format!(
                "DELETE FROM {schema}.queue_terminal_live_counts WHERE ready_slot = $1"
            ))
            .bind(slot)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::Noop);
        }

        // Beyond the emptiness gate above, queue prune has three liveness
        // skip gates. Check them before taking ACCESS EXCLUSIVE on the
        // ready/terminal child tables so a known-busy slot does not block hot
        // claim reads behind a doomed truncate attempt. These are only MVCC
        // snapshots; the same gates are checked again after the exclusive
        // locks.
        let active_leases = queue_prune_has_active_leases_tx(&mut tx, schema, slot).await?;

        if active_leases {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::QueueActiveLeases,
                count: busy_indicator(active_leases),
            });
        }

        let pending = queue_prune_has_pending_ready_tx(&mut tx, schema, &ready_child).await?;

        if pending {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::QueuePendingReady,
                count: busy_indicator(pending),
            });
        }

        let unclosed_claim_refs =
            queue_prune_has_unclosed_claim_refs_tx(&mut tx, schema, slot).await?;

        if unclosed_claim_refs {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive {
                slot,
                reason: SkipReason::QueueUnclosedClaimRefs,
                count: busy_indicator(unclosed_claim_refs),
            });
        }

        let locked: Result<PruneOutcome, AwaError> = async {
            set_lock_timeout_tx(&mut tx, self.prune_lock_timeout).await?;

            // Locks are requested in this order, and a statement that holds
            // a later child while it waits behind the request for an earlier
            // one deadlocks with prune until the lock timeout. Claims take
            // the segment child first, and retry, fail and snooze take the
            // done child before the ready child.
            let slot_children = format!(
                "{segment_child}, {done_child}, {ready_child}, {claim_attempt_child}, {tomb_child}, {receipt_batch_child}, {receipt_tomb_child}, {delta_child}"
            );
            // SHARE ROW EXCLUSIVE waits for every transaction that has
            // written to this slot and blocks new writers, while claims,
            // completions and admin reads keep reading the sealed children.
            // The gates and the scans below can therefore take as long as
            // the slot needs without stalling traffic; ACCESS EXCLUSIVE is
            // only requested once the truncate is the next statement.
            sqlx::query(&format!(
                "LOCK TABLE {slot_children} IN SHARE ROW EXCLUSIVE MODE"
            ))
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
            sqlx::query("SAVEPOINT awa_prune_slot_frozen")
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

            // The pre-lock gates are only an MVCC snapshot. Writers that
            // touched the slot have committed by the time the lock is
            // granted, so these gates see them.
            let active_leases =
                queue_prune_has_active_leases_tx(&mut tx, schema, slot).await?;
            if active_leases {
                return Ok(PruneOutcome::SkippedActive {
                    slot,
                    reason: SkipReason::QueueActiveLeases,
                    count: busy_indicator(active_leases),
                });
            }

            let pending =
                queue_prune_has_pending_ready_tx(&mut tx, schema, &ready_child).await?;
            if pending {
                return Ok(PruneOutcome::SkippedActive {
                    slot,
                    reason: SkipReason::QueuePendingReady,
                    count: busy_indicator(pending),
                });
            }

            let unclosed_claim_refs =
                queue_prune_has_unclosed_claim_refs_tx(&mut tx, schema, slot).await?;
            if unclosed_claim_refs {
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
            // so they stay retryable. The queue_ring_state row is held FOR
            // UPDATE above, which serializes against rotate — the current
            // slot cannot move while carried rows are re-inserted into it.
            let carried_failed_rows = if retention_floor {
                let (current_generation,): (i64,) = sqlx::query_as(&format!(
                    "SELECT generation FROM {schema}.queue_ring_slots WHERE slot = $1"
                ))
                .bind(state.0)
                .fetch_one(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                // Carried rows are widened to the self-contained synthetic
                // done-row shape (the COALESCE chain mirrors
                // `done_row_projection`) because the ready rows they would
                // otherwise hydrate from are truncated with this slot.
                // Deliberately no ON CONFLICT: lane_seq comes from
                // never-resetting sequences, so a PK collision means
                // corrupted terminal state and must abort the prune loudly
                // rather than silently drop a terminal fact.
                let carry_ready_join =
                    done_ready_join_from("done", "ready", &ready_child, &tomb_child);
                let carry_payload = done_payload_projection("done", "ready");
                let groups: Vec<(String, i16, i16, i16, i64)> = sqlx::query_as(&format!(
                    r#"
                    WITH carried AS (
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
                        COALESCE(ready.priority, done.priority),
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
                        {carry_payload}
                    FROM {done_child} AS done
                    {carry_ready_join}
                    WHERE done.state = 'failed'
                      AND done.finalized_at >= now() - make_interval(secs => $1::bigint)
                    RETURNING queue, priority, enqueue_shard, job_id
                    )
                    SELECT
                        queue,
                        priority,
                        enqueue_shard,
                        mod(
                            mod(job_id, {TERMINAL_COUNTER_BUCKETS}::bigint)
                                + {TERMINAL_COUNTER_BUCKETS}::bigint,
                            {TERMINAL_COUNTER_BUCKETS}::bigint
                        )::smallint AS counter_bucket,
                        count(*)::bigint
                    FROM carried
                    GROUP BY queue, priority, enqueue_shard, counter_bucket
                    ORDER BY queue, priority, enqueue_shard, counter_bucket
                    "#
                ))
                .bind(failed_retention_secs)
                .bind(state.0)
                .bind(current_generation)
                .fetch_all(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                // The replacement delta uses each inserted row's recovered
                // priority. The old slot's counter evidence is truncated below.
                let carried: u64 = groups.iter().map(|(_, _, _, _, count)| *count as u64).sum();
                let deltas = groups
                    .into_iter()
                    .map(|(queue, priority, shard, bucket, count)| {
                        ((state.0, current_generation, queue, priority, shard, bucket), count)
                    })
                    .collect();
                self.append_terminal_count_deltas_tx(&mut tx, deltas).await?;
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

            acquire_prune_exclusive_lock_tx(&mut tx, &slot_children, self.prune_lock_timeout).await?;

            // A receipt claim inserts its attempt batch into this slot, so
            // it cannot have started after the slot was frozen, and closures
            // only accumulate: the claim-reference proof above still holds.
            // A claim that writes a lease directly touches no child of the
            // slot, and one that read its ready row before the freeze can
            // commit until the upgrade is granted. These two gates see it.
            let active_leases =
                queue_prune_has_active_leases_tx(&mut tx, schema, slot).await?;
            let pending = !active_leases
                && queue_prune_has_pending_ready_tx(&mut tx, schema, &ready_child)
                    .await?;
            if active_leases || pending {
                // Drops the carried failed rows along with the slot locks.
                sqlx::query("ROLLBACK TO SAVEPOINT awa_prune_slot_frozen")
                    .execute(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;
                return Ok(PruneOutcome::SkippedActive {
                    slot,
                    reason: if active_leases {
                        SkipReason::QueueActiveLeases
                    } else {
                        SkipReason::QueuePendingReady
                    },
                    count: busy_indicator(true),
                });
            }

            Self::truncate_used_children_tx(
                &mut tx,
                &[
                    &segment_child,
                    &done_child,
                    &ready_child,
                    &claim_attempt_child,
                    &tomb_child,
                    &receipt_batch_child,
                    &receipt_tomb_child,
                    &delta_child,
                ],
            )
            .await?;

            if !pruned_terminal_counts.is_empty() {
                self.adjust_terminal_rollups_batch(&mut tx, pruned_terminal_counts.into_iter())
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
            Ok(PruneOutcome::Pruned {
                slot,
                carried_failed_rows,
            })
        }
        .await;

        let outcome = settle_prune_tx(tx, locked, PruneOutcome::Blocked { slot }).await?;
        if let PruneOutcome::Pruned {
            slot,
            carried_failed_rows,
        } = outcome
        {
            if carried_failed_rows > 0 {
                tracing::info!(
                    slot,
                    carried_failed_rows,
                    "Carried failed terminal rows inside the retention floor to the live done segment"
                );
            }
        }
        Ok(outcome)
    }

    /// The lanes keeping queue prune from reclaiming the oldest sealed queue
    /// ring slot because they hold ready rows at or past their claim cursor:
    /// the rows behind [`SkipReason::QueuePendingReady`]. `None` when there is
    /// no sealed slot or no such lane.
    ///
    /// Read-only and lock-free on the ring state. It probes each lane's head
    /// in the slot's ready partition, the same lanes the pending-ready gate
    /// probes, so it costs about as much as that gate.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.queue_ring_pin")]
    pub async fn queue_ring_pin(&self, pool: &PgPool) -> Result<Option<QueueRingPin>, AwaError> {
        let schema = self.schema();
        // One snapshot for the oldest sealed slot and the rows in it: the
        // row probe counts every generation in the slot, so a rotation that
        // reopened the slot between two statements would otherwise report
        // the new current generation's rows as pinning the old one.
        let mut tx = pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .await
            .map_err(map_sqlx_error)?;
        set_lock_timeout_tx(&mut tx, self.prune_lock_timeout).await?;

        let target: Option<(i32, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT slots.slot, slots.generation
            FROM {schema}.queue_ring_slots AS slots
            WHERE slots.generation >= 0
              AND slots.slot <> (
                  SELECT current_slot
                  FROM {schema}.queue_ring_state
                  WHERE singleton = TRUE
              )
            ORDER BY slots.generation ASC, slots.slot ASC
            LIMIT 1
            "#
        ))
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        let Some((slot, generation)) = target else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };

        let ready_child = ready_child_name(schema, slot as usize);
        let rows: Vec<(String, i16, i16, i64, DateTime<Utc>, f64)> = sqlx::query_as(&format!(
            r#"
            WITH claim_cursors AS MATERIALIZED (
                SELECT
                    queue,
                    priority,
                    enqueue_shard,
                    {schema}.sequence_next_value(seq_name) AS claim_seq
                FROM {schema}.queue_claim_heads
            )
            SELECT
                claims.queue,
                claims.priority,
                claims.enqueue_shard,
                head.lane_seq,
                head.run_at,
                GREATEST(EXTRACT(EPOCH FROM (clock_timestamp() - head.run_at)), 0)::float8
            FROM claim_cursors AS claims
            CROSS JOIN LATERAL (
                SELECT ready.lane_seq, ready.run_at
                FROM {ready_child} AS ready
                WHERE ready.queue = claims.queue
                  AND ready.priority = claims.priority
                  AND ready.enqueue_shard = claims.enqueue_shard
                  AND ready.lane_seq >= claims.claim_seq
                ORDER BY ready.lane_seq ASC
                LIMIT 1
            ) AS head
            ORDER BY head.run_at ASC, claims.queue, claims.priority, claims.enqueue_shard
            "#
        ))
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;

        if rows.is_empty() {
            return Ok(None);
        }
        let lanes = rows
            .into_iter()
            .map(
                |(queue, priority, enqueue_shard, head_lane_seq, head_run_at, age_secs)| {
                    PendingReadyLane {
                        queue,
                        priority,
                        enqueue_shard,
                        head_lane_seq,
                        head_run_at,
                        head_age: Duration::try_from_secs_f64(age_secs).unwrap_or_default(),
                    }
                },
            )
            .collect();
        Ok(Some(QueueRingPin {
            slot,
            generation,
            lanes,
        }))
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.prune_oldest_leases")]
    pub async fn prune_oldest_leases(&self, pool: &PgPool) -> Result<PruneOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;

        // `PruneLeasesPlan` in
        // `correctness/storage/AwaStorageLockOrder.tla` requires the
        // sequence `lease_ring_state FOR UPDATE` →
        // `lease_ring_slots[slot] FOR UPDATE` → `ACCESS EXCLUSIVE` on
        // the child. The child lock is bounded by a short
        // transaction-local lock_timeout because pure NOWAIT can starve
        // under continuous parent-partition readers, while an unbounded
        // wait would put maintenance at the head of the relation-lock
        // queue indefinitely. Without these locks a concurrent
        // rotator can flip the cursor under the prune's liveness check
        // (current_slot recheck races a CAS update) and prune what
        // should be the active partition.
        let state: (i32, i64, i32) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot, generation, slot_count
            FROM {schema}.lease_ring_state
            WHERE singleton = TRUE
            FOR UPDATE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

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

        // Idle-prune churn guard (see `prune_oldest`): skip the destructive
        // ACCESS EXCLUSIVE + TRUNCATE when the sealed lease slot is already
        // empty, turning a per-tick truncate of an empty slot into a Noop and
        // eliminating the relfilenode/WAL churn that disaggregated-storage
        // engines such as AlloyDB amplify into application p99 latency. The
        // slot is sealed and its ring-state and slot rows are held FOR UPDATE
        // above, so the emptiness snapshot cannot race a writer.
        if !Self::relation_has_rows_tx(&mut tx, &lease_child).await? {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::Noop);
        }

        let locked: Result<PruneOutcome, AwaError> = async {
            set_lock_timeout_tx(&mut tx, self.prune_lock_timeout).await?;

            sqlx::query(&format!(
                "LOCK TABLE {lease_child} IN ACCESS EXCLUSIVE MODE"
            ))
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            let current_slot: i32 = sqlx::query_scalar(&format!(
                r#"
                SELECT current_slot
                FROM {schema}.lease_ring_state
                WHERE singleton = TRUE
                "#
            ))
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            if current_slot == slot {
                return Ok(PruneOutcome::SkippedActive {
                    slot,
                    reason: SkipReason::LeaseCurrent,
                    count: 0,
                });
            }

            let active_leases = Self::relation_has_rows_tx(&mut tx, &lease_child).await?;

            if active_leases {
                return Ok(PruneOutcome::SkippedActive {
                    slot,
                    reason: SkipReason::LeaseActive,
                    count: busy_indicator(active_leases),
                });
            }

            Self::truncate_used_children_tx(&mut tx, &[&lease_child]).await?;
            Ok(PruneOutcome::Pruned {
                slot,
                carried_failed_rows: 0,
            })
        }
        .await;

        settle_prune_tx(tx, locked, PruneOutcome::Blocked { slot }).await
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
    /// Advances `claim_ring_state.current_slot` via compare-and-swap. Before
    /// flipping the cursor the target partition must be drained: the
    /// `lease_claims_<next>`, `lease_claim_batches_<next>`,
    /// `lease_claim_closures_<next>`, and
    /// `lease_claim_closure_batches_<next>` child tables
    /// must be empty. This is what the `rotate → prune → rotate` ring
    /// invariant requires — we only hand out a slot to new claims when a
    /// prior prune has truncated it.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.rotate_claims")]
    pub async fn rotate_claims(&self, pool: &PgPool) -> Result<RotateOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;

        let state: (i32, i64, i32) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot, generation, slot_count
            FROM {schema}.claim_ring_state
            WHERE singleton = TRUE
            FOR UPDATE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

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

        let next_generation = state.1 + 1;

        let rotated = sqlx::query(&format!(
            r#"
            UPDATE {schema}.claim_ring_state
            SET current_slot = $1,
                generation = $2
            WHERE singleton = TRUE
              AND current_slot = $3
              AND generation = $4
            "#
        ))
        .bind(next_slot)
        .bind(next_generation)
        .bind(state.0)
        .bind(state.1)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if rotated.rows_affected() == 0 {
            // Lost the CAS race. Report the bounded presence evidence we
            // sampled before giving up.
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
    /// 1. `FOR UPDATE` on `claim_ring_state` (serialises with rotate).
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
        let mut tx = pool.begin_read_committed().await.map_err(map_sqlx_error)?;

        let state: (i32, i64, i32) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot, generation, slot_count
            FROM {schema}.claim_ring_state
            WHERE singleton = TRUE
            FOR UPDATE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some((slot, _generation)) = oldest_initialized_ring_slot(state.0, state.1, state.2)
        else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::Noop);
        };

        // The ring state row locked above already serializes rotate and
        // prune. The slot row is only read here: rescue takes its table
        // locks on the slot's partitions and then locks this row for its
        // cursor, so holding the row while waiting for ACCESS EXCLUSIVE on
        // those partitions would deadlock with every rescue of the slot.
        // Prune locks the row once it holds the partitions.
        let slot_locked: Option<i32> = sqlx::query_scalar(&format!(
            r#"
            SELECT slot FROM {schema}.claim_ring_slots
            WHERE slot = $1
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

        // Idle-prune churn guard (see `prune_oldest`): if every claim-ring
        // child partition for this sealed slot is already empty there is
        // nothing to reclaim, so commit a Noop instead of re-truncating an
        // empty slot every maintenance tick, eliminating the relfilenode/WAL
        // churn that disaggregated-storage engines such as AlloyDB amplify
        // into application p99 latency. The slot is sealed and its ring-state
        // and slot rows are held FOR UPDATE above, so the emptiness snapshot
        // cannot race a writer.
        let slot_has_reclaimable_rows = Self::any_relation_has_rows_tx(
            &mut tx,
            &[
                &claim_child,
                &claim_batch_child,
                &closure_child,
                &closure_batch_child,
            ],
        )
        .await?;
        if !slot_has_reclaimable_rows {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::Noop);
        }

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

        let locked: Result<PruneOutcome, AwaError> = async {
            set_lock_timeout_tx(&mut tx, self.prune_lock_timeout).await?;

            let slot_children = format!(
                "{claim_child}, {claim_batch_child}, {closure_child}, {closure_batch_child}"
            );
            // Heartbeat, retry, fail and snooze read `lease_claims` and then
            // update it in the same transaction. A weaker lock held across
            // the proof below would let them read and then block their
            // update, and the request for ACCESS EXCLUSIVE would deadlock
            // with every one of them. Claim prune therefore takes
            // ACCESS EXCLUSIVE before the proof.
            acquire_prune_exclusive_lock_tx(&mut tx, &slot_children, self.prune_lock_timeout)
                .await?;

            sqlx::query(&format!(
                "SELECT slot FROM {schema}.claim_ring_slots WHERE slot = $1 FOR UPDATE"
            ))
            .bind(slot)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            let current_slot: i32 = sqlx::query_scalar(&format!(
                r#"
                SELECT current_slot FROM {schema}.claim_ring_state WHERE singleton = TRUE
                "#
            ))
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            if current_slot == slot {
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
                return Ok(PruneOutcome::SkippedActive {
                    slot,
                    reason: SkipReason::ClaimOpen,
                    count: busy_indicator(open_claims),
                });
            }

            Self::truncate_used_children_tx(
                &mut tx,
                &[
                    &claim_child,
                    &claim_batch_child,
                    &closure_child,
                    &closure_batch_child,
                ],
            )
            .await?;
            self.ensure_claim_batch_job_index_tx(&mut tx, slot).await?;
            sqlx::query(&format!(
                r#"
                UPDATE {schema}.claim_ring_slots
                SET rescue_cursor_claimed_at = '-infinity'::timestamptz,
                    rescue_cursor_job_id = 0,
                    rescue_cursor_run_lease = 0,
                    deadline_cursor_deadline_at = '-infinity'::timestamptz,
                    deadline_cursor_job_id = 0,
                    deadline_cursor_run_lease = 0
                WHERE slot = $1
                "#
            ))
            .bind(slot)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
            Ok(PruneOutcome::Pruned {
                slot,
                carried_failed_rows: 0,
            })
        }
        .await;

        settle_prune_tx(tx, locked, PruneOutcome::Blocked { slot }).await
    }

    /// Give `lease_claim_batches_<slot>` its GIN index on `job_ids` through
    /// `{schema}.ensure_claim_batch_job_index`. Claim prune calls this right
    /// after truncating the slot, while it holds the partition's
    /// `ACCESS EXCLUSIVE` lock, so the build covers an empty table and waits
    /// on nobody. A schema without the function (the `claim_batch_job_index`
    /// patch is not applied) or a role that may not execute it skips the
    /// step. An error inside the function is rolled back to a savepoint and
    /// logged; it never undoes the prune.
    async fn ensure_claim_batch_job_index_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        slot: i32,
    ) -> Result<(), AwaError> {
        let schema = self.schema();
        let callable: bool = sqlx::query_scalar(
            "SELECT COALESCE(has_function_privilege(to_regprocedure($1), 'EXECUTE'), false)",
        )
        .bind(format!("{schema}.ensure_claim_batch_job_index(integer)"))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        if !callable {
            tracing::debug!(
                schema,
                slot,
                "Claim batch job index function is absent or not executable; skipping"
            );
            return Ok(());
        }

        sqlx::query("SAVEPOINT awa_claim_batch_job_index")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        let indexed: Result<bool, sqlx::Error> =
            sqlx::query_scalar(&format!("SELECT {schema}.ensure_claim_batch_job_index($1)"))
                .bind(slot)
                .fetch_one(tx.as_mut())
                .await;
        match indexed {
            Ok(indexed) => {
                sqlx::query("RELEASE SAVEPOINT awa_claim_batch_job_index")
                    .execute(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;
                if !indexed {
                    tracing::debug!(schema, slot, "Claim batch partition was not indexed");
                }
            }
            Err(sqlx::Error::Database(db_err)) => {
                sqlx::query("ROLLBACK TO SAVEPOINT awa_claim_batch_job_index")
                    .execute(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;
                let code = db_err.code().map(|code| code.into_owned());
                if matches!(code.as_deref(), Some("42883" | "42501")) {
                    tracing::debug!(
                        schema,
                        slot,
                        code = code.as_deref().unwrap_or_default(),
                        error = %db_err.message(),
                        "Claim batch job index function unavailable; skipping"
                    );
                } else {
                    tracing::warn!(
                        schema,
                        slot,
                        code = code.as_deref().unwrap_or_default(),
                        error = %db_err.message(),
                        "Could not index claim batch partition by job id"
                    );
                }
            }
            Err(err) => return Err(map_sqlx_error(err)),
        }
        Ok(())
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

#[cfg(test)]
mod writer_bind_tests {
    /// Multi-row `VALUES` writers bind one parameter per column per row, so a
    /// large batch exceeds PostgreSQL's 65,535 bind-parameter limit. Writers
    /// send one array per column through `unnest` instead.
    #[test]
    fn queue_storage_writers_do_not_bind_per_row() {
        let needle = concat!(".push", "_values(");
        let offenders: Vec<usize> = include_str!("queue_storage.rs")
            .lines()
            .enumerate()
            .filter(|(_, line)| line.contains(needle))
            .map(|(index, _)| index + 1)
            .collect();
        assert!(
            offenders.is_empty(),
            "queue_storage.rs builds a per-row VALUES insert at lines {offenders:?}; \
             use INSERT ... SELECT FROM unnest(...) with one array bind per column"
        );
    }
}

#[cfg(test)]
mod revived_unique_states_tests {
    use super::*;

    fn payload(error: serde_json::Value) -> serde_json::Value {
        serde_json::json!({ "errors": [error] })
    }

    #[test]
    fn a_handler_cancel_reason_mentioning_duplicates_does_not_block_retry() {
        let reason = payload(serde_json::json!({
            "error": "remote job rejected as duplicate: unique claim held by another job",
            "attempt": 1,
            "terminal": true,
        }));
        assert_eq!(
            revived_unique_states(JobState::Cancelled, None, None, &reason).unwrap(),
            None
        );
    }

    #[test]
    fn a_keyless_legacy_duplicate_cancellation_is_refused() {
        for message in LEGACY_KEYLESS_DUPLICATE_ERRORS {
            let legacy = payload(serde_json::json!({
                "error": message,
                "attempt": 1,
                "terminal": true,
            }));
            assert!(matches!(
                revived_unique_states(JobState::Cancelled, None, None, &legacy),
                Err(AwaError::UniqueConflict { .. })
            ));
        }
    }

    #[test]
    fn a_mask_field_on_a_caller_error_entry_is_ignored() {
        let forged = payload(serde_json::json!({
            "error": "upstream failed",
            DUPLICATE_CANCEL_MASK_KEY: "11111111",
        }));
        assert_eq!(
            revived_unique_states(JobState::Cancelled, None, None, &forged).unwrap(),
            None
        );
    }

    #[test]
    fn a_caller_error_entry_cannot_record_a_duplicate_mask() {
        let entry = caller_error_entry(
            serde_json::json!({
                "error": SNOOZED_DUPLICATE_ERROR,
                DUPLICATE_CANCEL_MASK_KEY: "11111111",
            }),
            2,
        );
        assert!(entry.get(DUPLICATE_CANCEL_MASK_KEY).is_none());
        assert_eq!(entry["error"], SNOOZED_DUPLICATE_ERROR);
        assert_eq!(entry["attempt"], 2);
        assert_eq!(entry["terminal"], true);
        assert_eq!(
            revived_unique_states(JobState::Cancelled, None, None, &payload(entry)).unwrap(),
            None
        );
    }

    #[test]
    fn a_reschedule_error_cannot_record_a_duplicate_mask() {
        let entry = without_duplicate_mask(&serde_json::json!({
            "error": RETRIED_DUPLICATE_ERROR,
            DUPLICATE_CANCEL_MASK_KEY: "11111111",
        }));
        assert!(entry.get(DUPLICATE_CANCEL_MASK_KEY).is_none());
        assert_eq!(entry["error"], RETRIED_DUPLICATE_ERROR);
        assert_eq!(
            without_duplicate_mask(&serde_json::json!("plain")),
            serde_json::json!("plain")
        );
    }

    #[test]
    fn canonical_revive_sql_names_every_canonical_duplicate_message() {
        let admin = include_str!("admin.rs");
        for message in [
            SNOOZED_DUPLICATE_ERROR,
            RETRIED_DUPLICATE_ERROR,
            PROMOTED_DUPLICATE_ERROR,
            RESCUED_STALE_DUPLICATE_ERROR,
            RESCUED_EXPIRED_DUPLICATE_ERROR,
            RESCUED_TIMED_OUT_DUPLICATE_ERROR,
        ] {
            assert_eq!(
                admin.matches(&format!("'{message}'")).count(),
                2,
                "{message} in both canonical revive statements"
            );
        }
        assert!(admin.contains(&format!("entry->>'{DUPLICATE_CANCEL_MASK_KEY}'")));
    }

    #[test]
    fn a_recorded_mask_is_restored() {
        let recorded = payload(serde_json::json!({
            "error": SNOOZED_DUPLICATE_ERROR,
            "attempt": 1,
            "terminal": true,
            DUPLICATE_CANCEL_MASK_KEY: "11000010",
        }));
        assert_eq!(
            revived_unique_states(
                JobState::Cancelled,
                Some(b"key"),
                Some("11000000".to_string()),
                &recorded
            )
            .unwrap(),
            Some("11000010".to_string())
        );
    }
}
