use crate::admin::{CallbackConfig, CallbackPollResult};
use crate::dlq::{ListDlqFilter, RetryFromDlqOpts};
use crate::error::AwaError;
use crate::insert::prepare_row_raw;
use crate::{InsertParams, JobRow, JobState};
use chrono::TimeDelta;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_SCHEMA: &str = "awa_exp";
const DEFAULT_QUEUE_SLOT_COUNT: usize = 16;
const DEFAULT_LEASE_SLOT_COUNT: usize = 8;
const DEFAULT_CLAIM_SLOT_COUNT: usize = 8;
const DEFAULT_QUEUE_STRIPE_COUNT: usize = 1;
const QUEUE_STRIPE_DELIMITER: &str = "#";

#[derive(Debug, Clone)]
pub struct QueueStorageConfig {
    pub schema: String,
    pub queue_slot_count: usize,
    pub lease_slot_count: usize,
    /// Number of child partitions the receipt ring splits
    /// `lease_claims` / `lease_claim_closures` across (ADR-023).
    /// Mirrors `lease_slot_count`: a small fixed set of slots
    /// reclaimed by rotation + TRUNCATE rather than by row-level
    /// DELETE.
    pub claim_slot_count: usize,
    pub queue_stripe_count: usize,
    /// Use the receipt-plane short path for zero-deadline jobs:
    /// claim writes a row into `lease_claims` and completion writes
    /// a closure tombstone into `lease_claim_closures`, both
    /// reclaimed by claim-ring rotation + TRUNCATE. Default `true`.
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
    /// ADR-023: the `claim_slot` partition this attempt's
    /// `lease_claims` receipt landed in. The completion path uses this
    /// to target the matching `lease_claim_closures` partition when
    /// writing the closure tombstone.
    pub claim_slot: i32,
    pub lease_claim_receipt: bool,
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
            priority: self.job.priority,
            attempt: self.job.attempt,
            run_lease: self.job.run_lease,
            max_attempts: self.job.max_attempts,
            lane_seq: self.claim.lane_seq,
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
    pub completed: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotateOutcome {
    Rotated { slot: i32, generation: i64 },
    SkippedBusy { slot: i32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneOutcome {
    Noop,
    Pruned { slot: i32 },
    Blocked { slot: i32 },
    SkippedActive { slot: i32 },
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

fn validate_ident(ident: &str) -> Result<(), AwaError> {
    let mut chars = ident.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => {
            return Err(AwaError::Validation(format!(
                "invalid SQL identifier: {ident}"
            )));
        }
    }

    if chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
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

fn done_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.done_entries_{slot}")
}

fn lease_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.leases_{slot}")
}

fn claim_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.lease_claims_{slot}")
}

fn closure_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.lease_claim_closures_{slot}")
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

fn default_payload_metadata() -> serde_json::Value {
    serde_json::json!({})
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RuntimePayload {
    #[serde(default = "default_payload_metadata")]
    metadata: serde_json::Value,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    errors: Vec<serde_json::Value>,
    #[serde(default)]
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

fn unique_state_claims(unique_states: Option<&str>, state: JobState) -> bool {
    let Some(bitmask) = unique_states else {
        return false;
    };
    let idx = state.bit_position() as usize;
    bitmask.as_bytes().get(idx).is_some_and(|bit| *bit == b'1')
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
    run_at: DateTime<Utc>,
    attempted_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
}

impl ReadyTransitionRow {
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
    lease_slot: i32,
    lease_generation: i64,
    claim_slot: i32,
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
            lease_claim_receipt,
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
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
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
            run_lease: 0,
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
            run_lease: 0,
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
    attempted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ReadySnapshotRow {
    ready_slot: i32,
    ready_generation: i64,
    kind: String,
    queue: String,
    args: serde_json::Value,
    priority: i16,
    lane_seq: i64,
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
        validate_ident(&config.schema)?;
        Ok(Self {
            config,
            next_stripe_probe: AtomicUsize::new(0),
        })
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

    fn use_lease_claim_receipts_for_runtime(&self, deadline_duration: Duration) -> bool {
        self.lease_claim_receipts() && deadline_duration.is_zero()
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

    #[tracing::instrument(skip(self, pool), name = "queue_storage.prepare_schema")]
    pub async fn prepare_schema(&self, pool: &PgPool) -> Result<(), AwaError> {
        let schema = self.schema();
        let install_lock_name = format!("awa.queue_storage.install:{schema}");
        let mut install_lock_conn = pool.acquire().await.map_err(map_sqlx_error)?;

        sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 0))")
            .bind(&install_lock_name)
            .execute(install_lock_conn.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        let install_result = async {
            sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

            // The hot path reads "currently open" by anti-joining the
            // partitioned `lease_claims` / `lease_claim_closures`
            // pair, so `open_receipt_claims` is unused (see ADR-023).
            // Drop it on every install. Refuse to drop a non-empty
            // table — non-empty here means an operator rolled forward
            // from an older build that still wrote rows we don't want
            // to silently delete. Treat that as an error the operator
            // must resolve (typically by running the reverse-
            // migration recipe in ADR-023 and re-trying).
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
            .fetch_one(pool)
            .await
            .map_err(map_sqlx_error)?;
            if open_receipt_claims_exists {
                let row_count: i64 = sqlx::query_scalar(&format!(
                    "SELECT count(*)::bigint FROM {schema}.open_receipt_claims"
                ))
                .fetch_one(pool)
                .await
                .map_err(map_sqlx_error)?;
                if row_count > 0 {
                    return Err(AwaError::Validation(format!(
                        "{schema}.open_receipt_claims has {row_count} rows but the runtime no \
                         longer reads or writes this table. Run the ADR-023 reverse migration \
                         (recreate from lease_claims minus lease_claim_closures) to drain it, \
                         then re-run prepare_schema."
                    )));
                }
                sqlx::query(&format!(
                    "DROP TABLE IF EXISTS {schema}.open_receipt_claims CASCADE"
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }

            let claimed_cte = if self.lease_claim_receipts() {
                format!(
                    r#"
                    claim_ring AS (
                        SELECT current_slot AS claim_slot
                        FROM {schema}.claim_ring_state
                        WHERE singleton = TRUE
                    ),
                    claimed AS (
                        INSERT INTO {schema}.lease_claims AS claim_rows (
                            claim_slot,
                            job_id,
                            run_lease,
                            ready_slot,
                            ready_generation,
                            queue,
                            priority,
                            attempt,
                            max_attempts,
                            lane_seq
                        )
                        SELECT
                            claim_ring.claim_slot,
                            selected.job_id,
                            selected.run_lease + 1,
                            selected.ready_slot,
                            selected.ready_generation,
                            selected.queue,
                            selected.effective_priority,
                            selected.attempt + 1,
                            selected.max_attempts,
                            selected.lane_seq
                        FROM selected
                        CROSS JOIN claim_ring
                        RETURNING
                            claim_rows.claim_slot,
                            claim_rows.ready_slot,
                            claim_rows.ready_generation,
                            claim_rows.job_id,
                            claim_rows.queue,
                            claim_rows.priority,
                            claim_rows.lane_seq,
                            claim_rows.attempt,
                            claim_rows.run_lease,
                            claim_rows.max_attempts
                    )
                    -- The partitioned lease_claims row above is the
                    -- authoritative record of "currently open"; every
                    -- other receipt-plane query reads it anti-joined
                    -- with lease_claim_closures.
                    "#
                )
            } else {
                // Non-receipts path doesn't write to lease_claims, so
                // claim_slot is meaningless here. We still emit a
                // placeholder value so the outer SELECT can reference
                // `claimed.claim_slot` unconditionally.
                format!(
                    r#"
                    claimed AS (
                        INSERT INTO {schema}.leases AS lease_rows (
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
                            heartbeat_at,
                            deadline_at,
                            attempted_at
                        )
                        SELECT
                            lease_ring.lease_slot,
                            lease_ring.lease_generation,
                            selected.ready_slot,
                            selected.ready_generation,
                            selected.job_id,
                            selected.queue,
                            'running'::awa.job_state,
                            selected.effective_priority,
                            selected.attempt + 1,
                            selected.run_lease + 1,
                            selected.max_attempts,
                            selected.lane_seq,
                            clock_timestamp(),
                            clock_timestamp() + make_interval(secs => $6),
                            clock_timestamp()
                        FROM selected
                        CROSS JOIN lease_ring
                        RETURNING
                            0::int AS claim_slot,
                            lease_rows.ready_slot,
                            lease_rows.ready_generation,
                            lease_rows.lease_slot,
                            lease_rows.lease_generation,
                            lease_rows.queue,
                            lease_rows.priority,
                            lease_rows.lane_seq,
                            lease_rows.attempt,
                            lease_rows.run_lease,
                            lease_rows.max_attempts,
                            lease_rows.heartbeat_at,
                            lease_rows.deadline_at,
                            lease_rows.attempted_at
                    )
                    "#
                )
            };

            sqlx::query(&format!(
                r#"
                CREATE SEQUENCE IF NOT EXISTS {schema}.job_id_seq
                "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.queue_ring_state (
                singleton      BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                current_slot   INT NOT NULL,
                generation     BIGINT NOT NULL,
                slot_count     INT NOT NULL
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            // Singleton row is rewritten on every queue rotation. Columns being
            // updated (current_slot, generation) are not indexed, so a low
            // fillfactor keeps the update in-page as a HOT update and the
            // aggressive vacuum threshold reclaims the non-HOT line pointer
            // churn quickly.
            sqlx::query(&format!(
                r#"
            ALTER TABLE {schema}.queue_ring_state SET (
                fillfactor = 50,
                autovacuum_vacuum_scale_factor = 0.0,
                autovacuum_vacuum_threshold = 50,
                autovacuum_vacuum_cost_limit = 2000,
                autovacuum_vacuum_cost_delay = 2
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            INSERT INTO {schema}.queue_ring_state (singleton, current_slot, generation, slot_count)
            VALUES (TRUE, 0, 0, $1)
            ON CONFLICT (singleton) DO NOTHING
            "#
            ))
            .bind(self.queue_slot_count() as i32)
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.queue_ring_slots (
                slot        INT PRIMARY KEY,
                generation  BIGINT NOT NULL
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            ALTER TABLE {schema}.queue_ring_slots SET (
                fillfactor = 70,
                autovacuum_vacuum_scale_factor = 0.0,
                autovacuum_vacuum_threshold = 50,
                autovacuum_vacuum_cost_limit = 2000,
                autovacuum_vacuum_cost_delay = 2
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.lease_ring_state (
                singleton      BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                current_slot   INT NOT NULL,
                generation     BIGINT NOT NULL,
                slot_count     INT NOT NULL
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            ALTER TABLE {schema}.lease_ring_state SET (
                fillfactor = 50,
                autovacuum_vacuum_scale_factor = 0.0,
                autovacuum_vacuum_threshold = 50,
                autovacuum_vacuum_cost_limit = 2000,
                autovacuum_vacuum_cost_delay = 2
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            INSERT INTO {schema}.lease_ring_state (singleton, current_slot, generation, slot_count)
            VALUES (TRUE, 0, 0, $1)
            ON CONFLICT (singleton) DO NOTHING
            "#
            ))
            .bind(self.lease_slot_count() as i32)
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.lease_ring_slots (
                slot        INT PRIMARY KEY,
                generation  BIGINT NOT NULL
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            ALTER TABLE {schema}.lease_ring_slots SET (
                fillfactor = 70,
                autovacuum_vacuum_scale_factor = 0.0,
                autovacuum_vacuum_threshold = 50,
                autovacuum_vacuum_cost_limit = 2000,
                autovacuum_vacuum_cost_delay = 2
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            // ADR-023 claim-ring control plane. Mirrors lease_ring_state /
            // lease_ring_slots above. The current_slot is the partition that
            // new `lease_claims` receipts and `lease_claim_closures`
            // tombstones append into; rotate_claims advances it with a
            // compare-and-swap on (current_slot, generation); prune_oldest_claims
            // reclaims older partitions via TRUNCATE.
            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.claim_ring_state (
                singleton      BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                current_slot   INT NOT NULL,
                generation     BIGINT NOT NULL,
                slot_count     INT NOT NULL
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            ALTER TABLE {schema}.claim_ring_state SET (
                fillfactor = 50,
                autovacuum_vacuum_scale_factor = 0.0,
                autovacuum_vacuum_threshold = 50,
                autovacuum_vacuum_cost_limit = 2000,
                autovacuum_vacuum_cost_delay = 2
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            INSERT INTO {schema}.claim_ring_state (singleton, current_slot, generation, slot_count)
            VALUES (TRUE, 0, 0, $1)
            ON CONFLICT (singleton) DO NOTHING
            "#
            ))
            .bind(self.claim_slot_count() as i32)
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.claim_ring_slots (
                slot        INT PRIMARY KEY,
                generation  BIGINT NOT NULL
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            ALTER TABLE {schema}.claim_ring_slots SET (
                fillfactor = 70,
                autovacuum_vacuum_scale_factor = 0.0,
                autovacuum_vacuum_threshold = 50,
                autovacuum_vacuum_cost_limit = 2000,
                autovacuum_vacuum_cost_delay = 2
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.queue_lanes (
                queue           TEXT NOT NULL,
                priority        SMALLINT NOT NULL,
                next_seq        BIGINT NOT NULL DEFAULT 1,
                claim_seq       BIGINT NOT NULL DEFAULT 1,
                available_count BIGINT NOT NULL DEFAULT 0,
                pruned_completed_count BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (queue, priority)
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.queue_enqueue_heads (
                queue           TEXT NOT NULL,
                priority        SMALLINT NOT NULL,
                next_seq        BIGINT NOT NULL DEFAULT 1,
                PRIMARY KEY (queue, priority)
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            // Updated once per enqueue batch to advance next_seq. The primary
            // key is the only index, and next_seq is not part of it, so the
            // updates are 99.9% HOT. fillfactor=50 (matching the ring-state
            // singletons) reserves enough per-page slack that HOT updates
            // stay in-page during autovacuum-blocked windows; fillfactor=70
            // bloated the heap to ~90 pages for a single live row in a
            // 30-min run with idle-in-tx pressure.
            sqlx::query(&format!(
                r#"
            ALTER TABLE {schema}.queue_enqueue_heads SET (
                fillfactor = 50,
                autovacuum_vacuum_scale_factor = 0.0,
                autovacuum_vacuum_threshold = 200,
                autovacuum_vacuum_cost_limit = 2000,
                autovacuum_vacuum_cost_delay = 2
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.queue_claim_heads (
                queue           TEXT NOT NULL,
                priority        SMALLINT NOT NULL,
                claim_seq       BIGINT NOT NULL DEFAULT 1,
                PRIMARY KEY (queue, priority)
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            ALTER TABLE {schema}.queue_claim_heads SET (
                fillfactor = 50,
                autovacuum_vacuum_scale_factor = 0.0,
                autovacuum_vacuum_threshold = 200,
                autovacuum_vacuum_cost_limit = 2000,
                autovacuum_vacuum_cost_delay = 2
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.queue_terminal_rollups (
                queue                  TEXT NOT NULL,
                priority               SMALLINT NOT NULL,
                pruned_completed_count BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (queue, priority)
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.queue_count_snapshots (
                queue         TEXT PRIMARY KEY,
                available     BIGINT NOT NULL DEFAULT 0,
                running       BIGINT NOT NULL DEFAULT 0,
                completed     BIGINT NOT NULL DEFAULT 0,
                refreshed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.queue_claimer_leases (
                queue             TEXT NOT NULL,
                claimer_slot      SMALLINT NOT NULL,
                owner_instance_id UUID NOT NULL,
                lease_epoch       BIGINT NOT NULL DEFAULT 0,
                leased_at         TIMESTAMPTZ NOT NULL,
                last_claimed_at   TIMESTAMPTZ NOT NULL,
                expires_at        TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (queue, claimer_slot)
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.queue_claimer_state (
                queue            TEXT PRIMARY KEY,
                target_claimers  SMALLINT NOT NULL,
                updated_at       TIMESTAMPTZ NOT NULL
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE INDEX IF NOT EXISTS idx_{schema}_queue_claimer_leases_owner
                ON {schema}.queue_claimer_leases (queue, owner_instance_id, expires_at)
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            let mut backfill_tx = pool.begin().await.map_err(map_sqlx_error)?;
            sqlx::query(&format!(
                r#"
            INSERT INTO {schema}.queue_enqueue_heads AS heads (
                queue,
                priority,
                next_seq
            )
            SELECT
                queue,
                priority,
                next_seq
            FROM {schema}.queue_lanes
            ON CONFLICT (queue, priority) DO UPDATE
            SET next_seq = GREATEST(heads.next_seq, EXCLUDED.next_seq)
            "#
            ))
            .execute(backfill_tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            INSERT INTO {schema}.queue_claim_heads AS heads (
                queue,
                priority,
                claim_seq
            )
            SELECT
                queue,
                priority,
                claim_seq
            FROM {schema}.queue_lanes
            ON CONFLICT (queue, priority) DO UPDATE
            SET claim_seq = GREATEST(heads.claim_seq, EXCLUDED.claim_seq)
            "#
            ))
            .execute(backfill_tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            INSERT INTO {schema}.queue_terminal_rollups AS rollups (
                queue,
                priority,
                pruned_completed_count
            )
            SELECT
                queue,
                priority,
                pruned_completed_count
            FROM {schema}.queue_lanes
            WHERE pruned_completed_count > 0
            ON CONFLICT (queue, priority) DO UPDATE
            SET pruned_completed_count = GREATEST(
                rollups.pruned_completed_count,
                EXCLUDED.pruned_completed_count
            )
            "#
            ))
            .execute(backfill_tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            UPDATE {schema}.queue_lanes
            SET pruned_completed_count = 0
            WHERE pruned_completed_count > 0
            "#
            ))
            .execute(backfill_tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            backfill_tx.commit().await.map_err(map_sqlx_error)?;

            sqlx::query(
                r#"
            CREATE TABLE IF NOT EXISTS awa.runtime_storage_backends (
                backend     TEXT PRIMARY KEY,
                schema_name TEXT NOT NULL,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            "#,
            )
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.leases (
                lease_slot        INT NOT NULL,
                lease_generation  BIGINT NOT NULL,
                ready_slot        INT NOT NULL,
                ready_generation  BIGINT NOT NULL,
                job_id            BIGINT NOT NULL,
                queue             TEXT NOT NULL,
                state             awa.job_state NOT NULL DEFAULT 'running',
                priority          SMALLINT NOT NULL,
                attempt           SMALLINT NOT NULL DEFAULT 1,
                run_lease         BIGINT NOT NULL DEFAULT 1,
                max_attempts      SMALLINT NOT NULL DEFAULT 25,
                lane_seq          BIGINT NOT NULL,
                heartbeat_at      TIMESTAMPTZ,
                deadline_at       TIMESTAMPTZ,
                attempted_at      TIMESTAMPTZ,
                callback_id       UUID,
                callback_timeout_at TIMESTAMPTZ,
                PRIMARY KEY (lease_slot, queue, priority, lane_seq)
            ) PARTITION BY LIST (lease_slot)
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            // `lease_claims` and `lease_claim_closures` are partitioned by
            // `claim_slot` (see ADR-023). Fresh installs create the
            // partitioned parents directly; existing installs with regular
            // tables take the in-place migration path (rename → create
            // partitioned → copy → drop legacy). Idempotent: re-running on
            // an already-partitioned table is a no-op.
            let claim_slot_count = self.claim_slot_count();

            // Detect the current shape of lease_claims / lease_claim_closures.
            let lease_claims_relkind: Option<String> = sqlx::query_scalar(
                r#"
                SELECT c.relkind::text
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = 'lease_claims'
                "#,
            )
            .bind(schema)
            .fetch_optional(pool)
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
            .fetch_optional(pool)
            .await
            .map_err(map_sqlx_error)?;

            // Regular tables → rename aside before creating the partitioned
            // parent. Partitioned or absent → do nothing.
            if lease_claims_relkind.as_deref() == Some("r") {
                sqlx::query(&format!(
                    "ALTER TABLE {schema}.lease_claims RENAME TO lease_claims_legacy"
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }
            if closures_relkind.as_deref() == Some("r") {
                sqlx::query(&format!(
                    "ALTER TABLE {schema}.lease_claim_closures RENAME TO lease_claim_closures_legacy"
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.lease_claims (
                claim_slot        INT NOT NULL,
                job_id            BIGINT NOT NULL,
                run_lease         BIGINT NOT NULL,
                ready_slot        INT NOT NULL,
                ready_generation  BIGINT NOT NULL,
                queue             TEXT NOT NULL,
                priority          SMALLINT NOT NULL,
                attempt           SMALLINT NOT NULL,
                max_attempts      SMALLINT NOT NULL,
                lane_seq          BIGINT NOT NULL,
                claimed_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                materialized_at   TIMESTAMPTZ,
                PRIMARY KEY (claim_slot, job_id, run_lease)
            ) PARTITION BY LIST (claim_slot)
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            for slot in 0..claim_slot_count {
                sqlx::query(&format!(
                    r#"
                CREATE TABLE IF NOT EXISTS {} PARTITION OF {schema}.lease_claims
                FOR VALUES IN ({slot})
                "#,
                    claim_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }

            sqlx::query(&format!(
                r#"
            CREATE INDEX IF NOT EXISTS idx_{schema}_lease_claims_stale
                ON {schema}.lease_claims (materialized_at, claimed_at, job_id)
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            // Secondary index on (job_id, run_lease) for completion /
            // materialize / rescue paths that don't carry claim_slot in
            // hand. Propagates to every child partition.
            sqlx::query(&format!(
                r#"
            CREATE INDEX IF NOT EXISTS idx_{schema}_lease_claims_job_run
                ON {schema}.lease_claims (job_id, run_lease)
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            // In-place migration: move existing rows into the
            // partitioned parent. Every legacy row lands in the
            // current claim_slot — the ring rotates naturally and
            // existing receipts will close (or be force-rescued) on
            // their normal lifecycle. Guarded by EXISTS so fresh
            // installs skip it.
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
            .fetch_one(pool)
            .await
            .map_err(map_sqlx_error)?;

            if lease_claims_legacy_exists {
                // Wrap the copy + drop in a single transaction so a
                // crash between the two leaves the schema in one of
                // exactly two states: pre-migration (legacy still
                // there, partitioned parent empty) or post-migration
                // (legacy gone, partitioned parent populated).
                // Otherwise a crash window can leave both populated,
                // and the next prepare_schema's
                // `ON CONFLICT DO NOTHING` masks the inconsistency
                // without surfacing it.
                let mut migrate_tx = pool.begin().await.map_err(map_sqlx_error)?;
                sqlx::query(&format!(
                    r#"
                INSERT INTO {schema}.lease_claims (
                    claim_slot, job_id, run_lease, ready_slot, ready_generation,
                    queue, priority, attempt, max_attempts, lane_seq,
                    claimed_at, materialized_at
                )
                SELECT
                    (SELECT current_slot FROM {schema}.claim_ring_state WHERE singleton),
                    job_id, run_lease, ready_slot, ready_generation,
                    queue, priority, attempt, max_attempts, lane_seq,
                    claimed_at, materialized_at
                FROM {schema}.lease_claims_legacy
                ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                "#
                ))
                .execute(migrate_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    "DROP TABLE {schema}.lease_claims_legacy"
                ))
                .execute(migrate_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                migrate_tx.commit().await.map_err(map_sqlx_error)?;
            }

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.lease_claim_closures (
                claim_slot        INT NOT NULL,
                job_id            BIGINT NOT NULL,
                run_lease         BIGINT NOT NULL,
                outcome           TEXT NOT NULL,
                closed_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (claim_slot, job_id, run_lease)
            ) PARTITION BY LIST (claim_slot)
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            for slot in 0..claim_slot_count {
                sqlx::query(&format!(
                    r#"
                CREATE TABLE IF NOT EXISTS {} PARTITION OF {schema}.lease_claim_closures
                FOR VALUES IN ({slot})
                "#,
                    closure_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }

            // Secondary index on (job_id, run_lease) mirroring the one on
            // lease_claims — completion / rescue sites that don't have
            // claim_slot in hand still find closures via this index.
            sqlx::query(&format!(
                r#"
            CREATE INDEX IF NOT EXISTS idx_{schema}_lease_claim_closures_job_run
                ON {schema}.lease_claim_closures (job_id, run_lease)
            "#
            ))
            .execute(pool)
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
            .fetch_one(pool)
            .await
            .map_err(map_sqlx_error)?;

            if closures_legacy_exists {
                // See the matching `lease_claims_legacy` migration
                // above for the rationale: copy + drop must be atomic
                // so a crash leaves the schema either fully migrated
                // or fully not.
                let mut migrate_tx = pool.begin().await.map_err(map_sqlx_error)?;
                sqlx::query(&format!(
                    r#"
                INSERT INTO {schema}.lease_claim_closures (
                    claim_slot, job_id, run_lease, outcome, closed_at
                )
                SELECT
                    (SELECT current_slot FROM {schema}.claim_ring_state WHERE singleton),
                    job_id, run_lease, outcome, closed_at
                FROM {schema}.lease_claim_closures_legacy
                ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                "#
                ))
                .execute(migrate_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    "DROP TABLE {schema}.lease_claim_closures_legacy"
                ))
                .execute(migrate_tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                migrate_tx.commit().await.map_err(map_sqlx_error)?;
            }

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.attempt_state (
                job_id              BIGINT NOT NULL,
                run_lease           BIGINT NOT NULL,
                heartbeat_at        TIMESTAMPTZ,
                progress            JSONB,
                callback_filter     TEXT,
                callback_on_complete TEXT,
                callback_on_fail    TEXT,
                callback_transform  TEXT,
                callback_result     JSONB,
                updated_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (job_id, run_lease)
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            ALTER TABLE {schema}.attempt_state
                ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            // Upserted on every heartbeat, deleted on every completion. The PK
            // is the only index, and all mutable columns (heartbeat_at,
            // updated_at, progress, callback_*) are outside it, so a reduced
            // fillfactor keeps heartbeat UPDATEs HOT.
            sqlx::query(&format!(
                r#"
            ALTER TABLE {schema}.attempt_state SET (
                fillfactor = 80,
                autovacuum_vacuum_scale_factor = 0.0,
                autovacuum_vacuum_threshold = 200,
                autovacuum_vacuum_cost_limit = 2000,
                autovacuum_vacuum_cost_delay = 2
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.ready_entries (
                ready_slot        INT NOT NULL,
                ready_generation  BIGINT NOT NULL,
                job_id            BIGINT NOT NULL,
                kind              TEXT NOT NULL,
                queue             TEXT NOT NULL,
                args              JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                priority          SMALLINT NOT NULL,
                attempt           SMALLINT NOT NULL DEFAULT 0,
                run_lease         BIGINT NOT NULL DEFAULT 0,
                max_attempts      SMALLINT NOT NULL DEFAULT 25,
                lane_seq          BIGINT NOT NULL,
                run_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                attempted_at      TIMESTAMPTZ,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                unique_key        BYTEA,
                unique_states     TEXT,
                payload           JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                PRIMARY KEY (ready_slot, queue, priority, lane_seq)
            ) PARTITION BY LIST (ready_slot)
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.done_entries (
                ready_slot        INT NOT NULL,
                ready_generation  BIGINT NOT NULL,
                job_id            BIGINT NOT NULL,
                kind              TEXT NOT NULL,
                queue             TEXT NOT NULL,
                args              JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                state             awa.job_state NOT NULL DEFAULT 'completed',
                priority          SMALLINT NOT NULL,
                attempt           SMALLINT NOT NULL DEFAULT 1,
                run_lease         BIGINT NOT NULL DEFAULT 1,
                max_attempts      SMALLINT NOT NULL DEFAULT 25,
                lane_seq          BIGINT NOT NULL,
                run_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                attempted_at      TIMESTAMPTZ,
                finalized_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                unique_key        BYTEA,
                unique_states     TEXT,
                payload           JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                PRIMARY KEY (ready_slot, queue, priority, lane_seq)
            ) PARTITION BY LIST (ready_slot)
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.deferred_jobs (
                job_id            BIGINT PRIMARY KEY,
                kind              TEXT NOT NULL,
                queue             TEXT NOT NULL,
                args              JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                state             awa.job_state NOT NULL,
                priority          SMALLINT NOT NULL,
                attempt           SMALLINT NOT NULL DEFAULT 0,
                run_lease         BIGINT NOT NULL DEFAULT 0,
                max_attempts      SMALLINT NOT NULL DEFAULT 25,
                run_at            TIMESTAMPTZ NOT NULL,
                attempted_at      TIMESTAMPTZ,
                finalized_at      TIMESTAMPTZ,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                unique_key        BYTEA,
                unique_states     TEXT,
                payload           JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                CONSTRAINT deferred_jobs_state_check
                    CHECK (state IN ('scheduled', 'retryable'))
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE INDEX IF NOT EXISTS idx_{schema}_deferred_due
                ON {schema}.deferred_jobs (state, run_at, queue, priority, job_id)
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE INDEX IF NOT EXISTS idx_{schema}_deferred_job_unique
                ON {schema}.deferred_jobs (unique_key)
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE TABLE IF NOT EXISTS {schema}.dlq_entries (
                job_id            BIGINT PRIMARY KEY,
                kind              TEXT NOT NULL,
                queue             TEXT NOT NULL,
                args              JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                state             awa.job_state NOT NULL DEFAULT 'failed',
                priority          SMALLINT NOT NULL,
                attempt           SMALLINT NOT NULL DEFAULT 1,
                run_lease         BIGINT NOT NULL DEFAULT 1,
                max_attempts      SMALLINT NOT NULL DEFAULT 25,
                run_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                attempted_at      TIMESTAMPTZ,
                finalized_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                unique_key        BYTEA,
                unique_states     TEXT,
                payload           JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                dlq_reason        TEXT NOT NULL,
                dlq_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                original_run_lease BIGINT NOT NULL
            )
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
            CREATE INDEX IF NOT EXISTS idx_{schema}_dlq_queue_time
                ON {schema}.dlq_entries (queue, dlq_at DESC)
            "#
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            for slot in 0..self.queue_slot_count() {
                sqlx::query(&format!(
                    r#"
                CREATE TABLE IF NOT EXISTS {} PARTITION OF {schema}.ready_entries
                FOR VALUES IN ({slot})
                "#,
                    ready_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_ready_{slot}_lane
                    ON {} (queue, priority, lane_seq)
                "#,
                    ready_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_ready_{slot}_job
                    ON {} (job_id)
                "#,
                    ready_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                CREATE TABLE IF NOT EXISTS {} PARTITION OF {schema}.done_entries
                FOR VALUES IN ({slot})
                "#,
                    done_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_done_{slot}_lane
                    ON {} (queue, priority, lane_seq)
                "#,
                    done_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_done_{slot}_job
                    ON {} (job_id)
                "#,
                    done_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }

            for slot in 0..self.lease_slot_count() {
                sqlx::query(&format!(
                    r#"
                CREATE TABLE IF NOT EXISTS {} PARTITION OF {schema}.leases
                FOR VALUES IN ({slot})
                "#,
                    lease_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_leases_{slot}_lane
                    ON {} (queue, priority, lane_seq)
                "#,
                    lease_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_leases_{slot}_ready_ref
                    ON {} (ready_slot, ready_generation)
                "#,
                    lease_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_leases_{slot}_job
                    ON {} (job_id, run_lease)
                "#,
                    lease_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_leases_{slot}_callback
                    ON {} (callback_id)
                "#,
                    lease_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_leases_{slot}_state_hb
                    ON {} (state, heartbeat_at)
                "#,
                    lease_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_leases_{slot}_state_deadline
                    ON {} (state, deadline_at)
                "#,
                    lease_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;

                sqlx::query(&format!(
                    r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_leases_{slot}_state_callback_timeout
                    ON {} (state, callback_timeout_at)
                "#,
                    lease_child_name(schema, slot)
                ))
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }

            sqlx::query(&format!(
                r#"
            CREATE OR REPLACE FUNCTION {schema}.claim_ready_runtime(
                p_queue TEXT,
                p_max_batch BIGINT,
                p_deadline_secs DOUBLE PRECISION,
                p_aging_secs DOUBLE PRECISION
            )
            RETURNS TABLE(
                ready_slot INT,
                ready_generation BIGINT,
                lane_seq BIGINT,
                lease_slot INT,
                lease_generation BIGINT,
                claim_slot INT,
                job_id BIGINT,
                kind TEXT,
                queue TEXT,
                args JSONB,
                lane_priority SMALLINT,
                priority SMALLINT,
                attempt SMALLINT,
                run_lease BIGINT,
                max_attempts SMALLINT,
                run_at TIMESTAMPTZ,
                heartbeat_at TIMESTAMPTZ,
                deadline_at TIMESTAMPTZ,
                attempted_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ,
                unique_key BYTEA,
                unique_states TEXT,
                payload JSONB
            )
            LANGUAGE plpgsql
            SET search_path = pg_catalog, awa, public
            AS $func$
            DECLARE
                v_lane_priority SMALLINT;
                v_lane_claim_seq BIGINT;
                v_lane_next_seq BIGINT;
                v_claim_limit BIGINT;
                v_target_slot INT;
                v_target_generation BIGINT;
            BEGIN
                SELECT
                    claims.priority,
                    claims.claim_seq,
                    enqueues.next_seq
                INTO v_lane_priority, v_lane_claim_seq, v_lane_next_seq
                FROM {schema}.queue_claim_heads AS claims
                JOIN {schema}.queue_enqueue_heads AS enqueues
                  ON enqueues.queue = claims.queue
                 AND enqueues.priority = claims.priority
                JOIN LATERAL (
                    SELECT
                        ready.ready_slot,
                        ready.ready_generation,
                        ready.run_at,
                        CASE
                            WHEN p_aging_secs > 0 THEN GREATEST(
                                1,
                                claims.priority - FLOOR(
                                    EXTRACT(EPOCH FROM (clock_timestamp() - ready.run_at)) / p_aging_secs
                                )::smallint
                            )::smallint
                            ELSE claims.priority
                        END AS effective_priority
                    FROM {schema}.ready_entries AS ready
                    WHERE ready.queue = p_queue
                      AND ready.priority = claims.priority
                      AND ready.lane_seq >= claims.claim_seq
                    ORDER BY ready.lane_seq ASC
                    LIMIT 1
                ) AS candidate ON TRUE
                WHERE claims.queue = p_queue
                  AND NOT EXISTS (
                      SELECT 1
                      FROM awa.queue_meta AS meta
                      WHERE meta.queue = p_queue
                        AND meta.paused = TRUE
                  )
                  AND claims.claim_seq < enqueues.next_seq
                ORDER BY candidate.effective_priority ASC, candidate.run_at ASC, claims.priority ASC
                LIMIT 1
                FOR UPDATE OF claims SKIP LOCKED;

                IF NOT FOUND THEN
                    RETURN;
                END IF;

                SELECT ready.ready_slot, ready.ready_generation
                INTO v_target_slot, v_target_generation
                FROM {schema}.ready_entries AS ready
                WHERE ready.queue = p_queue
                  AND ready.priority = v_lane_priority
                  AND ready.lane_seq >= v_lane_claim_seq
                ORDER BY ready.lane_seq ASC
                LIMIT 1;

                IF NOT FOUND THEN
                    UPDATE {schema}.queue_claim_heads AS claims
                    SET claim_seq = GREATEST(claims.claim_seq, v_lane_next_seq)
                    WHERE claims.queue = p_queue
                      AND claims.priority = v_lane_priority;
                    RETURN;
                END IF;

                v_claim_limit := LEAST(GREATEST(v_lane_next_seq - v_lane_claim_seq, 0), p_max_batch);
                IF v_claim_limit <= 0 THEN
                    RETURN;
                END IF;

                RETURN QUERY
                WITH lease_ring AS (
                    SELECT current_slot AS lease_slot, generation AS lease_generation
                    FROM {schema}.lease_ring_state
                    WHERE singleton = TRUE
                ),
                selected AS (
                    SELECT
                        ready.ready_slot,
                        ready.ready_generation,
                        ready.job_id,
                        ready.kind,
                        ready.queue,
                        ready.args,
                        ready.priority AS lane_priority,
                        CASE
                            WHEN p_aging_secs > 0 THEN GREATEST(
                                1,
                                ready.priority - FLOOR(
                                    EXTRACT(EPOCH FROM (clock_timestamp() - ready.run_at)) / p_aging_secs
                                )::smallint
                            )::smallint
                            ELSE ready.priority
                        END AS effective_priority,
                        ready.attempt,
                        ready.run_lease,
                        ready.max_attempts,
                        ready.lane_seq,
                        ready.run_at,
                        ready.created_at,
                        ready.unique_key,
                        ready.unique_states,
                        ready.payload
                    FROM {schema}.ready_entries AS ready
                    WHERE ready.queue = p_queue
                      AND ready.priority = v_lane_priority
                      AND ready.ready_slot = v_target_slot
                      AND ready.ready_generation = v_target_generation
                      AND ready.lane_seq >= v_lane_claim_seq
                    ORDER BY ready.lane_seq ASC
                    LIMIT v_claim_limit
                ),
                advanced AS (
                    UPDATE {schema}.queue_claim_heads AS claims
                    SET claim_seq = COALESCE(
                            (SELECT max(selected.lane_seq) + 1 FROM selected),
                            claims.claim_seq
                        )
                    WHERE claims.queue = p_queue
                      AND claims.priority = v_lane_priority
                    RETURNING claims.priority
                ),
                {claimed_cte}
                SELECT
                    claimed.ready_slot,
                    claimed.ready_generation,
                    claimed.lane_seq,
                    lease_ring.lease_slot,
                    lease_ring.lease_generation,
                    claimed.claim_slot,
                    selected.job_id,
                    selected.kind,
                    selected.queue,
                    selected.args,
                    selected.lane_priority,
                    selected.effective_priority,
                    claimed.attempt,
                    claimed.run_lease,
                    claimed.max_attempts,
                    selected.run_at,
                    CASE
                        WHEN p_deadline_secs > 0 THEN clock_timestamp()
                        ELSE NULL::timestamptz
                    END AS heartbeat_at,
                    CASE
                        WHEN p_deadline_secs > 0 THEN clock_timestamp() + make_interval(secs => p_deadline_secs)
                        ELSE NULL::timestamptz
                    END AS deadline_at,
                    CASE
                        WHEN p_deadline_secs > 0 THEN clock_timestamp()
                        ELSE NULL::timestamptz
                    END AS attempted_at,
                    selected.created_at,
                    selected.unique_key,
                    selected.unique_states,
                    selected.payload
                FROM claimed
                CROSS JOIN lease_ring
                JOIN selected
                 ON selected.ready_slot = claimed.ready_slot
                 AND selected.ready_generation = claimed.ready_generation
                 AND selected.queue = claimed.queue
                 AND selected.effective_priority = claimed.priority
                 AND selected.lane_seq = claimed.lane_seq
                ORDER BY selected.lane_seq ASC;

                IF NOT FOUND THEN
                    UPDATE {schema}.queue_claim_heads AS claims
                    SET claim_seq = GREATEST(claims.claim_seq, lane_next_seq)
                    WHERE claims.queue = p_queue
                      AND claims.priority = lane_priority;
                END IF;
            END;
            $func$
            "#,
                claimed_cte = claimed_cte
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            for slot in 0..self.queue_slot_count() {
                sqlx::query(&format!(
                    r#"
                INSERT INTO {schema}.queue_ring_slots (slot, generation)
                VALUES ($1, $2)
                ON CONFLICT (slot) DO NOTHING
                "#
                ))
                .bind(slot as i32)
                .bind(if slot == 0 { 0_i64 } else { -1_i64 })
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }

            for slot in 0..self.lease_slot_count() {
                sqlx::query(&format!(
                    r#"
                    INSERT INTO {schema}.lease_ring_slots (slot, generation)
                    VALUES ($1, $2)
                    ON CONFLICT (slot) DO NOTHING
                    "#
                ))
                .bind(slot as i32)
                .bind(if slot == 0 { 0_i64 } else { -1_i64 })
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }

            for slot in 0..self.claim_slot_count() {
                sqlx::query(&format!(
                    r#"
                    INSERT INTO {schema}.claim_ring_slots (slot, generation)
                    VALUES ($1, $2)
                    ON CONFLICT (slot) DO NOTHING
                    "#
                ))
                .bind(slot as i32)
                .bind(if slot == 0 { 0_i64 } else { -1_i64 })
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }

            Ok(())
        }
        .await;

        let unlock_result = sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
            .bind(&install_lock_name)
            .execute(install_lock_conn.as_mut())
            .await
            .map(|_| ())
            .map_err(map_sqlx_error);

        match (install_result, unlock_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), Ok(())) => Err(err),
            (Ok(()), Err(err)) => Err(err),
            (Err(err), Err(_)) => Err(err),
        }
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.activate_backend")]
    pub async fn activate_backend(&self, pool: &PgPool) -> Result<(), AwaError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS awa.runtime_storage_backends (
                backend     TEXT PRIMARY KEY,
                schema_name TEXT NOT NULL,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            "#,
        )
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        let schema = self.schema();
        let details = serde_json::json!({ "schema": schema });

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        // Mark queue storage active only after the full schema, partitions,
        // indexes, and helper functions are in place. The explicit install
        // helper is used by tests and queue-storage-only setups, so it must
        // flip both the routing registry and the storage transition state.
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
                {schema}.done_entries,
                {schema}.dlq_entries,
                {schema}.leases,
                {schema}.lease_claims,
                {schema}.lease_claim_closures,
                {schema}.attempt_state,
                {schema}.deferred_jobs,
                {schema}.queue_lanes,
                {schema}.queue_terminal_rollups,
                {schema}.queue_count_snapshots,
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

        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn ensure_lane<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        queue: &str,
        priority: i16,
    ) -> Result<(), AwaError> {
        let schema = self.schema();
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
            INSERT INTO {schema}.queue_enqueue_heads (queue, priority)
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
            INSERT INTO {schema}.queue_claim_heads (queue, priority)
            VALUES ($1, $2)
            ON CONFLICT (queue, priority) DO NOTHING
            "#
        ))
        .bind(queue)
        .bind(priority)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn current_queue_ring<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    ) -> Result<(i32, i64), AwaError> {
        let schema = self.schema();
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
                lease_slot,
                lease_generation,
                claim_slot,
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
                payload
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

    async fn execute_ready_inserts_tx<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        rows: &[RuntimeReadyInsert],
    ) -> Result<usize, AwaError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let schema = self.schema();
        let ring = self.current_queue_ring(tx).await?;
        let mut builder = QueryBuilder::<Postgres>::new(format!(
            "INSERT INTO {schema}.ready_entries (ready_slot, ready_generation, job_id, kind, queue, args, priority, attempt, run_lease, max_attempts, lane_seq, run_at, attempted_at, created_at, unique_key, unique_states, payload) "
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
                .push_bind(row.run_at)
                .push_bind(row.attempted_at)
                .push_bind(row.created_at)
                .push_bind(&row.unique_key)
                .push_bind(&row.unique_states)
                .push_bind(&row.payload);
        });
        builder
            .build()
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

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

        let schema = self.schema();
        let mut grouped: BTreeMap<(String, i16), Vec<RuntimeReadyRow>> = BTreeMap::new();
        for row in rows {
            grouped
                .entry((row.queue.clone(), row.priority))
                .or_default()
                .push(row);
        }

        let total_rows: usize = grouped.values().map(Vec::len).sum();
        let job_ids = self.next_job_ids(tx, total_rows).await?;
        let mut job_id_iter = job_ids.into_iter();

        let mut ready_rows = Vec::with_capacity(total_rows);

        for ((queue, priority), lane_rows) in grouped {
            self.ensure_lane(tx, &queue, priority).await?;

            let count = lane_rows.len() as i64;
            let start_seq: i64 = sqlx::query_scalar(&format!(
                r#"
                UPDATE {schema}.queue_enqueue_heads
                SET next_seq = next_seq + $3
                WHERE queue = $1 AND priority = $2
                RETURNING next_seq - $3
                "#
            ))
            .bind(&queue)
            .bind(priority)
            .bind(count)
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            for (offset, row) in lane_rows.into_iter().enumerate() {
                let job_id = job_id_iter.next().ok_or_else(|| {
                    AwaError::Validation("queue storage job id allocation underflow".to_string())
                })?;
                self.sync_unique_claim(
                    tx,
                    job_id,
                    &row.unique_key,
                    row.unique_states.as_deref(),
                    None,
                    Some(JobState::Available),
                )
                .await?;
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
                    lane_seq: start_seq + offset as i64,
                    created_at: row.created_at,
                    unique_key: row.unique_key,
                    unique_states: row.unique_states,
                    payload: row.payload,
                });
            }
        }

        self.execute_ready_inserts_tx(tx, &ready_rows).await?;
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

        let schema = self.schema();
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
            self.ensure_lane(tx, &queue, priority).await?;

            let count = lane_rows.len() as i64;
            let start_seq: i64 = sqlx::query_scalar(&format!(
                r#"
                UPDATE {schema}.queue_enqueue_heads
                SET next_seq = next_seq + $3
                WHERE queue = $1 AND priority = $2
                RETURNING next_seq - $3
                "#
            ))
            .bind(&queue)
            .bind(priority)
            .bind(count)
            .fetch_one(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

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
                    created_at: row.created_at,
                    unique_key: row.unique_key,
                    unique_states: row.unique_states,
                    payload: row.payload,
                });
            }
        }

        self.execute_ready_inserts_tx(tx, &ready_rows).await?;
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
                .push_bind(&row.payload);
        });
        builder
            .build()
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

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
        let mut builder = QueryBuilder::<Postgres>::new(format!(
            "INSERT INTO {schema}.done_entries (ready_slot, ready_generation, job_id, kind, queue, args, state, priority, attempt, run_lease, max_attempts, lane_seq, run_at, attempted_at, finalized_at, created_at, unique_key, unique_states, payload) "
        ));
        builder.push_values(rows.iter(), |mut b, row| {
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
                .push_bind(row.run_at)
                .push_bind(row.attempted_at)
                .push_bind(row.finalized_at)
                .push_bind(row.created_at)
                .push_bind(&row.unique_key)
                .push_bind(&row.unique_states)
                .push_bind(&row.payload);
        });
        builder
            .build()
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        Ok(rows.len())
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
                .push_bind(&row.payload)
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

    async fn adjust_lane_counts<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        queue: &str,
        priority: i16,
        available_delta: i64,
        pruned_completed_delta: i64,
    ) -> Result<(), AwaError> {
        self.adjust_lane_counts_batch(
            tx,
            [(
                queue.to_string(),
                priority,
                available_delta,
                pruned_completed_delta,
            )],
        )
        .await
    }

    async fn adjust_lane_counts_batch<'a, I>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        deltas: I,
    ) -> Result<(), AwaError>
    where
        I: IntoIterator<Item = (String, i16, i64, i64)>,
    {
        let mut grouped: BTreeMap<(String, i16), i64> = BTreeMap::new();
        for (queue, priority, available_delta, pruned_completed_delta) in deltas {
            if available_delta == 0 && pruned_completed_delta == 0 {
                continue;
            }
            *grouped.entry((queue, priority)).or_insert(0_i64) += pruned_completed_delta;
        }

        if grouped.is_empty() {
            return Ok(());
        }

        let schema = self.schema();
        let mut queues = Vec::with_capacity(grouped.len());
        let mut priorities = Vec::with_capacity(grouped.len());
        let mut pruned_completed_deltas = Vec::with_capacity(grouped.len());

        for ((queue, priority), pruned_completed_delta) in grouped {
            queues.push(queue);
            priorities.push(priority);
            pruned_completed_deltas.push(pruned_completed_delta);
        }

        sqlx::query(&format!(
            r#"
            WITH deltas(queue, priority, pruned_completed_delta) AS (
                SELECT *
                FROM unnest(
                    $1::text[],
                    $2::smallint[],
                    $3::bigint[]
                )
            )
            UPDATE {schema}.queue_lanes
            SET pruned_completed_count = GREATEST(
                    0,
                    queue_lanes.pruned_completed_count + deltas.pruned_completed_delta
                )
            FROM deltas
            WHERE queue_lanes.queue = deltas.queue
              AND queue_lanes.priority = deltas.priority
            "#
        ))
        .bind(&queues)
        .bind(&priorities)
        .bind(&pruned_completed_deltas)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn adjust_terminal_rollups_batch<'a, I>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        deltas: I,
    ) -> Result<(), AwaError>
    where
        I: IntoIterator<Item = (String, i16, i64)>,
    {
        let mut grouped: BTreeMap<(String, i16), i64> = BTreeMap::new();
        for (queue, priority, pruned_completed_delta) in deltas {
            if pruned_completed_delta == 0 {
                continue;
            }
            *grouped.entry((queue, priority)).or_insert(0_i64) += pruned_completed_delta;
        }

        if grouped.is_empty() {
            return Ok(());
        }

        let schema = self.schema();
        let mut queues = Vec::with_capacity(grouped.len());
        let mut priorities = Vec::with_capacity(grouped.len());
        let mut pruned_completed_deltas = Vec::with_capacity(grouped.len());

        for ((queue, priority), pruned_completed_delta) in grouped {
            queues.push(queue);
            priorities.push(priority);
            pruned_completed_deltas.push(pruned_completed_delta);
        }

        sqlx::query(&format!(
            r#"
            WITH deltas(queue, priority, pruned_completed_delta) AS (
                SELECT *
                FROM unnest(
                    $1::text[],
                    $2::smallint[],
                    $3::bigint[]
                )
            )
            INSERT INTO {schema}.queue_terminal_rollups AS rollups (
                queue,
                priority,
                pruned_completed_count
            )
            SELECT
                deltas.queue,
                deltas.priority,
                deltas.pruned_completed_delta
            FROM deltas
            ON CONFLICT (queue, priority) DO UPDATE
            SET pruned_completed_count = GREATEST(
                0,
                rollups.pruned_completed_count + EXCLUDED.pruned_completed_count
            )
            "#
        ))
        .bind(&queues)
        .bind(&priorities)
        .bind(&pruned_completed_deltas)
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

        let queues_to_notify: BTreeSet<String> = rows
            .iter()
            .map(|row| self.logical_queue_name(&row.queue).to_string())
            .collect();
        for queue in queues_to_notify {
            sqlx::query("SELECT pg_notify($1, '')")
                .bind(format!("awa:{queue}"))
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
        }

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

        let queues_to_notify: BTreeSet<String> = ready_rows
            .into_iter()
            .map(|row| self.logical_queue_name(&row.queue).to_string())
            .collect();
        for queue in queues_to_notify {
            sqlx::query("SELECT pg_notify($1, '')")
                .bind(format!("awa:{queue}"))
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
        }

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
        let claimed = claimed_rows
            .into_iter()
            .map(|row| row.claim_ref(self.lease_claim_receipts()))
            .collect();

        tx.commit().await.map_err(map_sqlx_error)?;
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

        if self.lease_claim_receipts() && !deadline_duration.is_zero() {
            return Err(AwaError::Validation(
                "experimental lease-claim receipts require queue deadline_duration=0".to_string(),
            ));
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let mut claimed = Vec::new();
        let stripe_queues = self.physical_queues_for_logical(queue);
        let start = self.stripe_probe_start(stripe_queues.len());
        for offset in 0..stripe_queues.len() {
            if claimed.len() >= max_batch as usize {
                break;
            }
            let stripe_queue = &stripe_queues[(start + offset) % stripe_queues.len()];
            let remaining = max_batch - claimed.len() as i64;
            claimed.extend(
                self.claim_ready_rows_tx(
                    &mut tx,
                    stripe_queue,
                    remaining,
                    deadline_duration,
                    aging_interval,
                )
                .await?,
            );
        }

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

        tx.commit().await.map_err(map_sqlx_error)?;

        let use_lease_claim_receipts = self.use_lease_claim_receipts_for_runtime(deadline_duration);
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

        if let Some(owned) = sqlx::query_as::<_, QueueClaimerLease>(&format!(
            r#"
            SELECT claimer_slot, lease_epoch
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
            if let Some(updated) = sqlx::query_as::<_, QueueClaimerLease>(&format!(
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
                RETURNING claimer_slot, lease_epoch
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

            if let Some(inserted) = sqlx::query_as::<_, QueueClaimerLease>(&format!(
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
                RETURNING claimer_slot, lease_epoch
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
        counts: &QueueCounts,
        max_claimers: i16,
    ) -> i16 {
        let available = counts.available.max(0) as u64;
        let running = counts.running.max(0) as u64;
        let backlog = available.saturating_sub(running);
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
        queue_counts_max_age: Duration,
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

        let counts = self
            .queue_counts_cached(pool, queue, queue_counts_max_age)
            .await?;
        let desired = self.desired_queue_claimer_target(current_target, &counts, max_claimers);

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
        queue_counts_max_age: Duration,
    ) -> Result<Vec<ClaimedRuntimeJob>, AwaError> {
        let target_claimers = self
            .queue_claimer_target(
                pool,
                queue,
                max_claimers,
                queue_counts_max_age,
                Duration::from_millis(500),
            )
            .await?;

        let Some(lease) = self
            .acquire_queue_claimer(
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

        if !claimed.is_empty() {
            let _ = self
                .mark_queue_claimer_active(pool, queue, instance_id, lease, lease_ttl)
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
        let lane_seqs: Vec<i64> = claimed.iter().map(|entry| entry.lane_seq).collect();

        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            WITH completed(lease_slot, queue, priority, lane_seq) AS (
                SELECT * FROM unnest($1::int[], $2::text[], $3::smallint[], $4::bigint[])
            )
            DELETE FROM {schema}.leases AS leases
            USING completed
            WHERE leases.lease_slot = completed.lease_slot
              AND leases.queue = completed.queue
              AND leases.priority = completed.priority
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
        .bind(&lane_seqs)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if deleted.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

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

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

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
                // claim_slot rides along on `ClaimedEntry`, so the
                // closure INSERT can pass (claim_slot, job_id,
                // run_lease) triples directly and let Postgres route
                // to the correct partition without an extra lookup.
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
                let updated: Vec<(i64, i64)> = sqlx::query_as(&format!(
                    r#"
                    WITH completed(claim_slot, job_id, run_lease) AS (
                        SELECT * FROM unnest($1::int[], $2::bigint[], $3::bigint[])
                    ),
                    inserted AS (
                        INSERT INTO {schema}.lease_claim_closures (claim_slot, job_id, run_lease, outcome, closed_at)
                        SELECT completed.claim_slot, completed.job_id, completed.run_lease, 'completed', clock_timestamp()
                        FROM completed
                        ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                        RETURNING job_id, run_lease
                    )
                    SELECT job_id, run_lease
                    FROM inserted
                    "#
                ))
                .bind(&receipt_claim_slots)
                .bind(&receipt_job_ids)
                .bind(&receipt_run_leases)
                .fetch_all(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                if !updated.is_empty() {
                    let updated_job_ids: Vec<i64> =
                        updated.iter().map(|(job_id, _)| *job_id).collect();
                    let updated_run_leases: Vec<i64> =
                        updated.iter().map(|(_, run_lease)| *run_lease).collect();

                    sqlx::query(&format!(
                        r#"
                        WITH completed(job_id, run_lease) AS (
                            SELECT * FROM unnest($1::bigint[], $2::bigint[])
                        )
                        DELETE FROM {schema}.attempt_state AS attempt
                        USING completed
                        WHERE attempt.job_id = completed.job_id
                          AND attempt.run_lease = completed.run_lease
                        "#
                    ))
                    .bind(&updated_job_ids)
                    .bind(&updated_run_leases)
                    .execute(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;

                    let finalized_at = Utc::now();
                    let mut done_rows = Vec::with_capacity(updated.len());
                    for (job_id, run_lease) in &updated {
                        if let Some(runtime_job) = claimed_map.get(&(*job_id, *run_lease)).cloned()
                        {
                            done_rows.push(runtime_job.into_done_row(finalized_at)?);
                        }
                    }

                    self.insert_done_rows_tx(&mut tx, &done_rows, Some(JobState::Running))
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
                let lease_slots: Vec<i32> = materialized_claimed
                    .iter()
                    .map(|entry| entry.claim.lease_slot)
                    .collect();
                let queues: Vec<String> = materialized_claimed
                    .iter()
                    .map(|entry| entry.claim.queue.clone())
                    .collect();
                let priorities: Vec<i16> = materialized_claimed
                    .iter()
                    .map(|entry| entry.claim.priority)
                    .collect();
                let lane_seqs: Vec<i64> = materialized_claimed
                    .iter()
                    .map(|entry| entry.claim.lane_seq)
                    .collect();
                let run_leases: Vec<i64> = materialized_claimed
                    .iter()
                    .map(|entry| entry.job.run_lease)
                    .collect();

                let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
                    r#"
                    WITH completed(lease_slot, queue, priority, lane_seq, run_lease) AS (
                        SELECT * FROM unnest($1::int[], $2::text[], $3::smallint[], $4::bigint[], $5::bigint[])
                    )
                    DELETE FROM {schema}.leases AS leases
                    USING completed
                    WHERE leases.lease_slot = completed.lease_slot
                      AND leases.queue = completed.queue
                      AND leases.priority = completed.priority
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
                .bind(&lane_seqs)
                .bind(&run_leases)
                .fetch_all(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

                if !deleted.is_empty() {
                    let deleted_job_ids: Vec<i64> = deleted.iter().map(|row| row.job_id).collect();
                    let deleted_run_leases: Vec<i64> =
                        deleted.iter().map(|row| row.run_lease).collect();

                    sqlx::query(&format!(
                        r#"
                        WITH completed(job_id, run_lease) AS (
                            SELECT * FROM unnest($1::bigint[], $2::bigint[])
                        )
                        DELETE FROM {schema}.attempt_state AS attempt
                        USING completed
                        WHERE attempt.job_id = completed.job_id
                          AND attempt.run_lease = completed.run_lease
                        "#
                    ))
                    .bind(&deleted_job_ids)
                    .bind(&deleted_run_leases)
                    .execute(tx.as_mut())
                    .await
                    .map_err(map_sqlx_error)?;

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

                    self.insert_done_rows_tx(&mut tx, &done_rows, Some(JobState::Running))
                        .await?;
                }
            }

            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(updated_all);
        }

        let lease_slots: Vec<i32> = claimed.iter().map(|entry| entry.claim.lease_slot).collect();
        let queues: Vec<String> = claimed
            .iter()
            .map(|entry| entry.claim.queue.clone())
            .collect();
        let priorities: Vec<i16> = claimed.iter().map(|entry| entry.claim.priority).collect();
        let lane_seqs: Vec<i64> = claimed.iter().map(|entry| entry.claim.lane_seq).collect();
        let run_leases: Vec<i64> = claimed.iter().map(|entry| entry.job.run_lease).collect();

        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            WITH completed(lease_slot, queue, priority, lane_seq, run_lease) AS (
                SELECT * FROM unnest($1::int[], $2::text[], $3::smallint[], $4::bigint[], $5::bigint[])
            )
            DELETE FROM {schema}.leases AS leases
            USING completed
            WHERE leases.lease_slot = completed.lease_slot
              AND leases.queue = completed.queue
              AND leases.priority = completed.priority
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
        .bind(&lane_seqs)
        .bind(&run_leases)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if deleted.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let deleted_job_ids: Vec<i64> = deleted.iter().map(|row| row.job_id).collect();
        let deleted_run_leases: Vec<i64> = deleted.iter().map(|row| row.run_lease).collect();

        sqlx::query(&format!(
            r#"
            WITH completed(job_id, run_lease) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[])
            )
            DELETE FROM {schema}.attempt_state AS attempt
            USING completed
            WHERE attempt.job_id = completed.job_id
              AND attempt.run_lease = completed.run_lease
            "#
        ))
        .bind(&deleted_job_ids)
        .bind(&deleted_run_leases)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

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

        self.insert_done_rows_tx(&mut tx, &done_rows, Some(JobState::Running))
            .await?;

        tx.commit().await.map_err(map_sqlx_error)?;
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

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

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
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

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

    async fn queue_counts_exact(
        &self,
        pool: &PgPool,
        queue: &str,
    ) -> Result<QueueCounts, AwaError> {
        let schema = self.schema();
        let queues = self.physical_queues_for_logical(queue);
        let row: (i64, i64, i64) = sqlx::query_as(&format!(
            r#"
            WITH lane_counts AS (
                SELECT
                    count(*)::bigint AS available
                FROM {schema}.ready_entries AS ready
                JOIN {schema}.queue_claim_heads AS claims
                  ON claims.queue = ready.queue
                 AND claims.priority = ready.priority
                WHERE ready.queue = ANY($1)
                  AND ready.lane_seq >= claims.claim_seq
            ),
            pruned_terminal AS (
                SELECT COALESCE(
                    sum(
                        GREATEST(
                            COALESCE(lanes.pruned_completed_count, 0),
                            COALESCE(rollups.pruned_completed_count, 0)
                        )
                    ),
                    0
                )::bigint AS completed
                FROM (
                    SELECT queue, priority, pruned_completed_count
                    FROM {schema}.queue_lanes
                    WHERE queue = ANY($1)
                ) AS lanes
                FULL OUTER JOIN (
                    SELECT queue, priority, pruned_completed_count
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
                    -- lease_claims anti-joined with
                    -- lease_claim_closures.
                    COALESCE((
                        SELECT count(*)::bigint
                        FROM {schema}.lease_claims AS claims
                        WHERE claims.queue = ANY($1)
                          AND NOT EXISTS (
                              SELECT 1 FROM {schema}.lease_claim_closures AS closures
                              WHERE closures.claim_slot = claims.claim_slot
                                AND closures.job_id = claims.job_id
                                AND closures.run_lease = claims.run_lease
                          )
                    ), 0)
                )::bigint AS running
            ),
            live_terminal AS (
                SELECT count(*)::bigint AS completed
                FROM {schema}.done_entries
                WHERE queue = ANY($1)
            )
            SELECT
                lane_counts.available,
                live_running.running,
                pruned_terminal.completed + live_terminal.completed AS completed
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

        let (available, running, completed) = row;
        Ok(QueueCounts {
            available,
            running,
            completed,
        })
    }

    #[tracing::instrument(skip(self, pool), fields(queue = %queue), name = "queue_storage.queue_counts")]
    pub async fn queue_counts(&self, pool: &PgPool, queue: &str) -> Result<QueueCounts, AwaError> {
        self.queue_counts_exact(pool, queue).await
    }

    #[tracing::instrument(
        skip(self, pool),
        fields(queue = %queue, max_age_ms = max_age.as_millis()),
        name = "queue_storage.queue_counts_cached"
    )]
    pub async fn queue_counts_cached(
        &self,
        pool: &PgPool,
        queue: &str,
        max_age: Duration,
    ) -> Result<QueueCounts, AwaError> {
        if max_age.is_zero() {
            return self.queue_counts_exact(pool, queue).await;
        }

        let schema = self.schema();
        let snapshot: Option<(i64, i64, i64, DateTime<Utc>)> = sqlx::query_as(&format!(
            r#"
            SELECT available, running, completed, refreshed_at
            FROM {schema}.queue_count_snapshots
            WHERE queue = $1
            "#
        ))
        .bind(queue)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;

        let now = Utc::now();
        if let Some((available, running, completed, refreshed_at)) = snapshot {
            if now
                .signed_duration_since(refreshed_at)
                .to_std()
                .unwrap_or_default()
                < max_age
            {
                return Ok(QueueCounts {
                    available,
                    running,
                    completed,
                });
            }
        }

        let counts = self.queue_counts_exact(pool, queue).await?;
        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.queue_count_snapshots AS snapshots (
                queue,
                available,
                running,
                completed,
                refreshed_at
            )
            VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (queue) DO UPDATE
            SET
                available = EXCLUDED.available,
                running = EXCLUDED.running,
                completed = EXCLUDED.completed,
                refreshed_at = EXCLUDED.refreshed_at
            "#
        ))
        .bind(queue)
        .bind(counts.available)
        .bind(counts.running)
        .bind(counts.completed)
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(counts)
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
                run_lease: 0,
                run_at: Utc::now(),
                attempted_at: None,
                ..waiting.clone().into_ready_row(Utc::now(), ready_payload)
            };
            self.insert_existing_ready_rows_tx(tx, vec![ready_row.clone()], Some(waiting.state))
                .await?;
            self.adjust_lane_counts(tx, &waiting.queue, waiting.priority, 0, 0)
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

        let terminal: Option<DoneJobRow> = sqlx::query_as(&format!(
            r#"
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
            RETURNING
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
                run_at,
                attempted_at,
                finalized_at,
                created_at,
                unique_key,
                unique_states,
                payload
            "#
        ))
        .bind(job_id)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if let Some(terminal) = terminal {
            let ready_row = ExistingReadyRow {
                job_id: terminal.job_id,
                kind: terminal.kind,
                queue: terminal.queue.clone(),
                args: terminal.args,
                priority: terminal.priority,
                attempt: 0,
                run_lease: 0,
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
            self.adjust_lane_counts(tx, &terminal.queue, terminal.priority, 0, 0)
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

    pub async fn retry_jobs_by_ids(
        &self,
        pool: &PgPool,
        ids: &[i64],
    ) -> Result<Vec<JobRow>, AwaError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let mut rows = Vec::with_capacity(ids.len());
        for job_id in ids {
            if let Some(row) = self.retry_job_tx(&mut tx, *job_id).await? {
                rows.push(row);
            }
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(rows)
    }

    /// Write a `<outcome>` closure row for any matching open receipt.
    /// Idempotent: no-op if no lease_claims row exists for the
    /// `(job_id, run_lease)` pair, or if a closure already exists. Used
    /// by the admin cancel path to keep the receipt plane consistent
    /// with the job's new terminal state so rescue doesn't revive it.
    ///
    /// `FOR UPDATE` on the inner SELECT serialises the closure write
    /// against `ensure_running_leases_from_receipts_tx`
    /// (which also takes `FOR UPDATE OF claims` on the same row) and
    /// against concurrent rescue / re-close paths that might race the
    /// same `(job_id, run_lease)`. Without it, materialization could
    /// see the claim row, decide to materialize, and a concurrent
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
        let schema = self.schema();
        sqlx::query(&format!(
            r#"
            WITH locked_claim AS (
                SELECT claim_slot, job_id, run_lease
                FROM {schema}.lease_claims AS claims
                WHERE claims.job_id = $1 AND claims.run_lease = $2
                FOR UPDATE
            )
            INSERT INTO {schema}.lease_claim_closures (claim_slot, job_id, run_lease, outcome, closed_at)
            SELECT claim_slot, job_id, run_lease, $3, clock_timestamp()
            FROM locked_claim
            ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
            "#
        ))
        .bind(job_id)
        .bind(run_lease)
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
    ) -> Result<Option<JobRow>, AwaError> {
        let schema = self.schema();
        let ready: Option<ReadyTransitionRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.ready_entries
            WHERE (ready_slot, queue, priority, lane_seq) IN (
                SELECT
                    ready.ready_slot,
                    ready.queue,
                    ready.priority,
                    ready.lane_seq
                FROM {schema}.ready_entries AS ready
                JOIN {schema}.queue_claim_heads AS claims
                  ON claims.queue = ready.queue
                 AND claims.priority = ready.priority
                WHERE ready.job_id = $1
                  AND ready.lane_seq >= claims.claim_seq
                ORDER BY ready.lane_seq DESC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING
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
                run_at,
                attempted_at,
                created_at,
                unique_key,
                unique_states,
                payload
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
            return Ok(Some(done.into_job_row()?));
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
            self.adjust_lane_counts(tx, &lease.queue, lease.priority, 0, 0)
                .await?;
            // Receipt-plane consistency: close any matching open
            // receipt so the ADR-023 anti-join no longer considers this
            // attempt live, and rescue doesn't try to revive it.
            self.close_receipt_tx(tx, lease.job_id, lease.run_lease, "cancelled")
                .await?;
            // Wake any worker currently executing this attempt.
            self.notify_cancellation_tx(tx, lease.job_id, lease.run_lease)
                .await?;
            return Ok(Some(done.into_job_row()?));
        }

        // ADR-023 receipt-only cancel: the job may be running on a
        // receipt-backed short path that never materialized a `leases`
        // row. Find it by anti-joining lease_claims with
        // lease_claim_closures, cancel it by writing a closure and a
        // done row, and notify listening workers.
        if self.lease_claim_receipts() {
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
            );
            let receipt: Option<ReceiptCancelRow> = sqlx::query_as(&format!(
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
                        claims.claimed_at
                    FROM {schema}.lease_claims AS claims
                    WHERE claims.job_id = $1
                      AND NOT EXISTS (
                          SELECT 1 FROM {schema}.lease_claim_closures AS closures
                          WHERE closures.claim_slot = claims.claim_slot
                            AND closures.job_id = claims.job_id
                            AND closures.run_lease = claims.run_lease
                      )
                    ORDER BY claims.run_lease DESC
                    LIMIT 1
                    FOR UPDATE OF claims SKIP LOCKED
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
                        run_at,
                        attempted_at,
                        created_at,
                        unique_key,
                        unique_states,
                        payload
                    FROM {schema}.ready_entries
                    WHERE job_id = $1
                      AND ready_slot = $2
                      AND ready_generation = $3
                      AND queue = $4
                      AND priority = $5
                      AND lane_seq = $6
                    "#
                ))
                .bind(job_id)
                .bind(ready_slot)
                .bind(ready_generation)
                .bind(&queue)
                .bind(priority)
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
                // Write the closure row into the same claim partition.
                sqlx::query(&format!(
                    r#"
                    INSERT INTO {schema}.lease_claim_closures (claim_slot, job_id, run_lease, outcome, closed_at)
                    VALUES ($1, $2, $3, 'cancelled', clock_timestamp())
                    ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                    "#
                ))
                .bind(claim_slot)
                .bind(job_id)
                .bind(run_lease)
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
                // Defensive: between the leases DELETE at the top of
                // this function and the FOR UPDATE on claims above, a
                // concurrent `ensure_running_leases_from_receipts_tx`
                // can have materialized a `leases` row for this
                // (job_id, run_lease). Materialize and we both lock the
                // same claim row; whichever ran first commits, the
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
                self.adjust_lane_counts(tx, &queue, priority, 0, 0).await?;
                self.notify_cancellation_tx(tx, job_id, run_lease).await?;
                return Ok(Some(done.into_job_row()?));
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
                payload
            "#
        ))
        .bind(job_id)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if let Some(deferred) = deferred {
            let (ready_slot, ready_generation) = self.current_queue_ring(tx).await?;
            self.ensure_lane(tx, &deferred.queue, deferred.priority)
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
            self.adjust_lane_counts(tx, &done.queue, done.priority, 0, 0)
                .await?;
            return Ok(Some(done.into_job_row()?));
        }

        Ok(None)
    }

    pub async fn cancel_job(&self, pool: &PgPool, job_id: i64) -> Result<Option<JobRow>, AwaError> {
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let row = self.cancel_job_tx(&mut tx, job_id).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(row)
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
        for job_id in ids {
            if let Some(row) = self.cancel_job_tx(&mut tx, *job_id).await? {
                rows.push(row);
            }
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(rows)
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
            DELETE FROM {schema}.ready_entries
            WHERE (ready_slot, queue, priority, lane_seq) IN (
                SELECT
                    ready.ready_slot,
                    ready.queue,
                    ready.priority,
                    ready.lane_seq
                FROM {schema}.ready_entries AS ready
                JOIN {schema}.queue_claim_heads AS claims
                  ON claims.queue = ready.queue
                 AND claims.priority = ready.priority
                WHERE ready.lane_seq >= claims.claim_seq
                  AND ready.priority > 1
                  AND ready.run_at <= $1
                ORDER BY ready.run_at ASC, ready.lane_seq ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            RETURNING
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
                run_at,
                attempted_at,
                created_at,
                unique_key,
                unique_states,
                payload
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
        let queues: BTreeSet<String> = queues
            .into_iter()
            .map(|queue| self.logical_queue_name(&queue).to_string())
            .collect();
        for queue in queues {
            sqlx::query("SELECT pg_notify($1, '')")
                .bind(format!("awa:{queue}"))
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
        }
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

        let schema = self.schema();
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
            claim_refs AS (
                -- Source claim metadata directly from the partitioned
                -- lease_claims table anti-joined against
                -- lease_claim_closures.
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
                    claims.claimed_at
                FROM {schema}.lease_claims AS claims
                JOIN inflight
                  ON inflight.job_id = claims.job_id
                 AND inflight.run_lease = claims.run_lease
                WHERE NOT EXISTS (
                    SELECT 1 FROM {schema}.lease_claim_closures AS closures
                    WHERE closures.claim_slot = claims.claim_slot
                      AND closures.job_id = claims.job_id
                      AND closures.run_lease = claims.run_lease
                )
                FOR UPDATE OF claims
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
                    clock_timestamp(),
                    NULL::timestamptz,
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
            SELECT count(*)::bigint FROM marked
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

        let schema = self.schema();
        let job_ids: Vec<i64> = jobs.iter().map(|(job_id, _)| *job_id).collect();
        let run_leases: Vec<i64> = jobs.iter().map(|(_, run_lease)| *run_lease).collect();
        let updated: i64 = sqlx::query_scalar(&format!(
            r#"
            WITH inflight(job_id, run_lease) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[])
            ),
            claim_refs AS (
                -- Source open-claim identity from lease_claims
                -- anti-joined against lease_claim_closures.
                SELECT claims.job_id, claims.run_lease
                FROM {schema}.lease_claims AS claims
                JOIN inflight
                  ON inflight.job_id = claims.job_id
                 AND inflight.run_lease = claims.run_lease
                WHERE NOT EXISTS (
                    SELECT 1 FROM {schema}.lease_claim_closures AS closures
                    WHERE closures.claim_slot = claims.claim_slot
                      AND closures.job_id = claims.job_id
                      AND closures.run_lease = claims.run_lease
                )
                FOR UPDATE OF claims
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
                FROM claim_refs
                WHERE claims.job_id = claim_refs.job_id
                  AND claims.run_lease = claim_refs.run_lease
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

        let schema = self.schema();
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
            claim_refs AS (
                -- Same anti-join pattern as the heartbeat-only path
                -- above.
                SELECT claims.job_id, claims.run_lease, inflight.progress
                FROM {schema}.lease_claims AS claims
                JOIN inflight
                  ON inflight.job_id = claims.job_id
                 AND inflight.run_lease = claims.run_lease
                WHERE NOT EXISTS (
                    SELECT 1 FROM {schema}.lease_claim_closures AS closures
                    WHERE closures.claim_slot = claims.claim_slot
                      AND closures.job_id = claims.job_id
                      AND closures.run_lease = claims.run_lease
                )
                FOR UPDATE OF claims
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
                FROM claim_refs
                WHERE claims.job_id = claim_refs.job_id
                  AND claims.run_lease = claim_refs.run_lease
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
        let priorities: Vec<i16> = deleted.iter().map(|row| row.priority).collect();
        let lane_seqs: Vec<i64> = deleted.iter().map(|row| row.lane_seq).collect();
        let job_ids: Vec<i64> = deleted.iter().map(|row| row.job_id).collect();
        let run_leases: Vec<i64> = deleted.iter().map(|row| row.run_lease).collect();

        let ready_rows: Vec<ReadySnapshotRow> = sqlx::query_as(&format!(
            r#"
            WITH refs(ready_slot, ready_generation, queue, priority, lane_seq) AS (
                SELECT * FROM unnest($1::int[], $2::bigint[], $3::text[], $4::smallint[], $5::bigint[])
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
                ready.run_at,
                ready.created_at,
                ready.unique_key,
                ready.unique_states,
                ready.payload
            FROM refs
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = refs.ready_slot
             AND ready.ready_generation = refs.ready_generation
             AND ready.queue = refs.queue
             AND ready.priority = refs.priority
             AND ready.lane_seq = refs.lane_seq
            "#
        ))
        .bind(&ready_slots)
        .bind(&ready_generations)
        .bind(&queues)
        .bind(&priorities)
        .bind(&lane_seqs)
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

        let ready_map: BTreeMap<(i32, i64, String, i16, i64), ReadySnapshotRow> = ready_rows
            .into_iter()
            .map(|row| {
                (
                    (
                        row.ready_slot,
                        row.ready_generation,
                        row.queue.clone(),
                        row.priority,
                        row.lane_seq,
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
                    deleted_row.priority,
                    deleted_row.lane_seq,
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

        let schema = self.schema();
        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            WITH target AS (
                -- Target is the open claim identified from the
                -- partitioned lease_claims table anti-joined against
                -- lease_claim_closures.
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
                    claims.claimed_at AS attempted_at
                FROM {schema}.lease_claims AS claims
                WHERE claims.job_id = $1
                  AND claims.run_lease = $2
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.lease_claim_closures AS closures
                      WHERE closures.claim_slot = claims.claim_slot
                        AND closures.job_id = claims.job_id
                        AND closures.run_lease = claims.run_lease
                  )
                FOR UPDATE OF claims
            ),
            inserted AS (
                INSERT INTO {schema}.lease_claim_closures (claim_slot, job_id, run_lease, outcome, closed_at)
                SELECT target.claim_slot, target.job_id, target.run_lease, $3, clock_timestamp()
                FROM target
                ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                RETURNING job_id, run_lease
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
                NULL::timestamptz AS heartbeat_at,
                NULL::timestamptz AS deadline_at,
                target.attempted_at,
                NULL::uuid AS callback_id,
                NULL::timestamptz AS callback_timeout_at
            FROM target
            JOIN inserted
              ON inserted.job_id = target.job_id
             AND inserted.run_lease = target.run_lease
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
        let schema = self.schema();
        let rescued: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            WITH stale_claims AS (
                -- Rescue scans partitioned lease_claims anti-joined
                -- with lease_claim_closures.
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
                    claims.claimed_at AS attempted_at
                FROM {schema}.lease_claims AS claims
                LEFT JOIN {schema}.attempt_state AS attempt
                  ON attempt.job_id = claims.job_id
                 AND attempt.run_lease = claims.run_lease
                WHERE COALESCE(attempt.heartbeat_at, claims.claimed_at) < $1
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.lease_claim_closures AS closures
                      WHERE closures.claim_slot = claims.claim_slot
                        AND closures.job_id = claims.job_id
                        AND closures.run_lease = claims.run_lease
                  )
                  -- A claim that already materialized into `leases` is
                  -- on the lease-side heartbeat-rescue path (see
                  -- `rescue_stale_heartbeats`). Rescuing it again here
                  -- would write a second closure for an attempt the
                  -- runtime is still tracking via its lease row, and on
                  -- commit produce a double-failure transition. Mirror
                  -- the same anti-join `load_job` uses to disambiguate.
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.leases AS lease
                      WHERE lease.job_id = claims.job_id
                        AND lease.run_lease = claims.run_lease
                  )
                ORDER BY COALESCE(attempt.heartbeat_at, claims.claimed_at) ASC
                LIMIT 500
                FOR UPDATE OF claims SKIP LOCKED
            ),
            inserted AS (
                INSERT INTO {schema}.lease_claim_closures (claim_slot, job_id, run_lease, outcome, closed_at)
                SELECT stale_claims.claim_slot, stale_claims.job_id, stale_claims.run_lease, 'rescued', clock_timestamp()
                FROM stale_claims
                ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING
                RETURNING job_id, run_lease
            )
            SELECT
                stale_claims.ready_slot,
                stale_claims.ready_generation,
                stale_claims.job_id,
                stale_claims.queue,
                stale_claims.state,
                stale_claims.priority,
                stale_claims.attempt,
                stale_claims.run_lease,
                stale_claims.max_attempts,
                stale_claims.lane_seq,
                stale_claims.attempted_at
            FROM stale_claims
            JOIN inserted
              ON inserted.job_id = stale_claims.job_id
             AND inserted.run_lease = stale_claims.run_lease
            "#
        ))
        .bind(cutoff)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(rescued)
    }

    pub async fn load_job(&self, pool: &PgPool, job_id: i64) -> Result<Option<JobRow>, AwaError> {
        let schema = self.schema();
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
                payload
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
                payload
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
                lease.heartbeat_at,
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
                ready.payload,
                attempt.progress,
                attempt.callback_result
            FROM {schema}.leases AS lease
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = lease.ready_slot
             AND ready.ready_generation = lease.ready_generation
             AND ready.queue = lease.queue
             AND ready.priority = lease.priority
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
        // lease_claims against lease_claim_closures.
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
                NULL::timestamptz AS deadline_at,
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
                ready.payload,
                attempt.progress,
                attempt.callback_result
            FROM {schema}.lease_claims AS claims
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = claims.ready_slot
             AND ready.ready_generation = claims.ready_generation
             AND ready.queue = claims.queue
             AND ready.priority = claims.priority
             AND ready.lane_seq = claims.lane_seq
            LEFT JOIN {schema}.attempt_state AS attempt
              ON attempt.job_id = claims.job_id
             AND attempt.run_lease = claims.run_lease
            WHERE claims.job_id = $1
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.lease_claim_closures AS closures
                  WHERE closures.claim_slot = claims.claim_slot
                    AND closures.job_id = claims.job_id
                    AND closures.run_lease = claims.run_lease
              )
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
                run_at,
                attempted_at,
                finalized_at,
                created_at,
                unique_key,
                unique_states,
                payload
            FROM {schema}.done_entries
            WHERE job_id = $1
            ORDER BY run_lease DESC, finalized_at DESC
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
                payload,
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

    pub async fn enter_callback_wait(
        &self,
        pool: &PgPool,
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
        .execute(pool)
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
                lease.heartbeat_at,
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
                ready.payload,
                attempt.progress,
                attempt.callback_result
            FROM {} AS lease
            JOIN {schema}.ready_entries AS ready
              ON ready.ready_slot = lease.ready_slot
             AND ready.ready_generation = lease.ready_generation
             AND ready.queue = lease.queue
             AND ready.priority = lease.priority
             AND ready.lane_seq = lease.lane_seq
            LEFT JOIN {schema}.attempt_state AS attempt
              ON attempt.job_id = lease.job_id
             AND attempt.run_lease = lease.run_lease
            WHERE lease.callback_id = $1
              AND lease.state IN ('waiting_external', 'running')
              AND ($2::bigint IS NULL OR lease.run_lease = $2)
            ORDER BY lease.run_lease DESC
            LIMIT 1
            "#,
            self.leases_table(),
            schema = self.schema()
        ))
        .bind(callback_id)
        .bind(run_lease)
        .fetch_optional(pool)
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
        if resume {
            let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
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

            let Some((job_id, run_lease)) = resumed else {
                tx.commit().await.map_err(map_sqlx_error)?;
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
            .bind(run_lease)
            .bind(payload.unwrap_or(serde_json::Value::Null))
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

            tx.commit().await.map_err(map_sqlx_error)?;

            return self
                .load_job(pool, job_id)
                .await?
                .ok_or(AwaError::CallbackNotFound {
                    callback_id: callback_id.to_string(),
                });
        }

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
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
            tx.commit().await.map_err(map_sqlx_error)?;
            return Err(AwaError::CallbackNotFound {
                callback_id: callback_id.to_string(),
            });
        }

        let moved = self.hydrate_deleted_leases_tx(&mut tx, deleted).await?;
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
        self.insert_done_rows_tx(&mut tx, std::slice::from_ref(&done_row), Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, 0)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
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
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
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
            tx.commit().await.map_err(map_sqlx_error)?;
            return Err(AwaError::CallbackNotFound {
                callback_id: callback_id.to_string(),
            });
        }

        let moved = self.hydrate_deleted_leases_tx(&mut tx, deleted).await?;
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
        self.insert_done_rows_tx(&mut tx, std::slice::from_ref(&done_row), Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, 0)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        done_row.into_job_row()
    }

    pub async fn retry_external(
        &self,
        pool: &PgPool,
        callback_id: Uuid,
        run_lease: Option<i64>,
    ) -> Result<JobRow, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
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
            tx.commit().await.map_err(map_sqlx_error)?;
            return Err(AwaError::CallbackNotFound {
                callback_id: callback_id.to_string(),
            });
        }

        let moved = self.hydrate_deleted_leases_tx(&mut tx, deleted).await?;
        let moved = moved.into_iter().next().expect("deleted callback lease");

        let ready_payload =
            Self::payload_with_attempt_state(moved.payload.clone(), moved.progress.clone())?;

        let ready_row = ExistingReadyRow {
            attempt: 0,
            run_at: Utc::now(),
            ..moved.clone().into_ready_row(Utc::now(), ready_payload)
        };
        self.insert_existing_ready_rows_tx(&mut tx, vec![ready_row.clone()], Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, 0)
            .await?;
        self.notify_queues_tx(&mut tx, std::iter::once(moved.queue.clone()))
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
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

        let job_ids: Vec<i64> = jobs.iter().map(|(job_id, _)| *job_id).collect();
        let run_leases: Vec<i64> = jobs.iter().map(|(_, run_lease)| *run_lease).collect();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let mut updated = 0_usize;
        if self.lease_claim_receipts() {
            updated += self
                .upsert_attempt_state_from_receipts_tx(&mut tx, jobs)
                .await?;
        }
        let result = sqlx::query(&format!(
            r#"
            WITH inflight AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[]) AS v(job_id, run_lease)
            )
            UPDATE {}
            SET heartbeat_at = clock_timestamp()
            FROM inflight
            WHERE {}.job_id = inflight.job_id
              AND {}.run_lease = inflight.run_lease
              AND {}.state = 'running'
            "#,
            self.leases_table(),
            self.leases_table(),
            self.leases_table(),
            self.leases_table()
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(updated + result.rows_affected() as usize)
    }

    pub async fn heartbeat_progress_batch(
        &self,
        pool: &PgPool,
        jobs: &[(i64, i64, serde_json::Value)],
    ) -> Result<usize, AwaError> {
        if jobs.is_empty() {
            return Ok(0);
        }

        let schema = self.schema();
        let job_ids: Vec<i64> = jobs.iter().map(|(job_id, _, _)| *job_id).collect();
        let run_leases: Vec<i64> = jobs.iter().map(|(_, run_lease, _)| *run_lease).collect();
        let progress: Vec<serde_json::Value> =
            jobs.iter().map(|(_, _, value)| value.clone()).collect();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let mut updated = 0_usize;
        if self.lease_claim_receipts() {
            updated += self
                .upsert_attempt_state_progress_from_receipts_tx(&mut tx, jobs)
                .await?;
        }
        let lease_updated: i64 = sqlx::query_scalar(&format!(
            r#"
            WITH inflight AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::jsonb[]) AS v(job_id, run_lease, progress)
            ),
            updated AS (
                UPDATE {} AS lease
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
            self.leases_table()
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .bind(&progress)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(updated + lease_updated as usize)
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
        let Some(moved) = self
            .take_running_attempt_tx(&mut tx, job_id, run_lease, "retryable")
            .await?
        else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };
        let now = self.current_timestamp_tx(&mut tx).await?;

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
        self.insert_deferred_rows_tx(&mut tx, vec![deferred.clone()], Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, 0)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
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
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, 0)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(deferred.into_job_row()?))
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
        let Some(moved) = self
            .take_running_attempt_tx(&mut tx, job_id, run_lease, "cancelled")
            .await?
        else {
            tx.commit().await.map_err(map_sqlx_error)?;
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
        self.insert_done_rows_tx(&mut tx, std::slice::from_ref(&done), Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, 0)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
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
        let Some(moved) = self
            .take_running_attempt_tx(&mut tx, job_id, run_lease, "failed")
            .await?
        else {
            tx.commit().await.map_err(map_sqlx_error)?;
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
        self.insert_done_rows_tx(&mut tx, std::slice::from_ref(&done), Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, 0)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
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
        let Some(moved) = self
            .take_running_attempt_tx(&mut tx, job_id, run_lease, "dlq")
            .await?
        else {
            tx.commit().await.map_err(map_sqlx_error)?;
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
        self.insert_dlq_rows_tx(&mut tx, std::slice::from_ref(&dlq_row), Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, 0)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
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
        let moved: Option<DoneJobRow> = sqlx::query_as(&format!(
            r#"
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
            RETURNING
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
                run_at,
                attempted_at,
                finalized_at,
                created_at,
                unique_key,
                unique_states,
                payload
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

        let dlq_row = moved
            .clone()
            .into_dlq_row(dlq_reason.to_string(), Utc::now());
        self.insert_dlq_rows_tx(&mut tx, std::slice::from_ref(&dlq_row), Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, 0)
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
        let moved: Vec<DoneJobRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.done_entries
            WHERE state = 'failed'
              AND ($1::text IS NULL OR kind = $1)
              AND ($2::text IS NULL OR queue = $2)
            RETURNING
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
                run_at,
                attempted_at,
                finalized_at,
                created_at,
                unique_key,
                unique_states,
                payload
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
                payload,
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
                payload,
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

        let deleted_done: Vec<DoneJobRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.done_entries
            WHERE kind = $1
              AND state = 'failed'
            RETURNING
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
                run_at,
                attempted_at,
                finalized_at,
                created_at,
                unique_key,
                unique_states,
                payload
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
                payload,
                dlq_reason,
                dlq_at,
                original_run_lease
            "#
        ))
        .bind(kind)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

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
        let Some(moved) = self
            .take_running_attempt_tx(&mut tx, job_id, run_lease, "retryable")
            .await?
        else {
            tx.commit().await.map_err(map_sqlx_error)?;
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
            self.insert_done_rows_tx(&mut tx, std::slice::from_ref(&done), Some(moved.state))
                .await?;
            self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, 0)
                .await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Some(done.into_job_row()?));
        }

        let deferred = moved.clone().into_deferred_row(
            JobState::Retryable,
            self.backoff_at_tx(&mut tx, moved.attempt, moved.max_attempts)
                .await?,
            Some(Utc::now()),
            payload.into_json(),
        );
        self.insert_deferred_rows_tx(&mut tx, vec![deferred.clone()], Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, 0)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
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
        let deleted: Vec<DeletedLeaseRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE job_id IN (
                SELECT job_id
                FROM {schema}.leases
                WHERE state = 'running'
                  AND heartbeat_at < $1
                ORDER BY heartbeat_at ASC
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
            self.insert_deferred_rows_tx(&mut tx, vec![deferred.clone()], Some(row.state))
                .await?;
            self.adjust_lane_counts(&mut tx, &row.queue, row.priority, 0, 0)
                .await?;
            rescued.push(deferred.into_job_row()?);
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
            self.insert_deferred_rows_tx(&mut tx, vec![deferred.clone()], Some(row.state))
                .await?;
            self.adjust_lane_counts(&mut tx, &row.queue, row.priority, 0, 0)
                .await?;
            rescued.push(deferred.into_job_row()?);
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
            self.insert_deferred_rows_tx(&mut tx, vec![deferred.clone()], Some(row.state))
                .await?;
            self.adjust_lane_counts(&mut tx, &row.queue, row.priority, 0, 0)
                .await?;
            rescued.push(deferred.into_job_row()?);
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
                self.insert_done_rows_tx(&mut tx, std::slice::from_ref(&done), Some(row.state))
                    .await?;
                self.adjust_lane_counts(&mut tx, &row.queue, row.priority, 0, 0)
                    .await?;
                rescued.push(done.into_job_row()?);
            } else {
                let deferred = row.clone().into_deferred_row(
                    JobState::Retryable,
                    self.backoff_at_tx(&mut tx, row.attempt, row.max_attempts)
                        .await?,
                    Some(Utc::now()),
                    payload.into_json(),
                );
                self.insert_deferred_rows_tx(&mut tx, vec![deferred.clone()], Some(row.state))
                    .await?;
                self.adjust_lane_counts(&mut tx, &row.queue, row.priority, 0, 0)
                    .await?;
                rescued.push(deferred.into_job_row()?);
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
                payload
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

    #[tracing::instrument(skip(self, pool), name = "queue_storage.rotate")]
    pub async fn rotate(&self, pool: &PgPool) -> Result<RotateOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

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
        let ready_count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*)::bigint FROM {}",
            ready_child_name(schema, next_slot as usize)
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        let done_count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*)::bigint FROM {}",
            done_child_name(schema, next_slot as usize)
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if ready_count > 0 || done_count > 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy { slot: next_slot });
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
        Ok(RotateOutcome::Rotated {
            slot: next_slot,
            generation: next_generation,
        })
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.rotate_leases")]
    pub async fn rotate_leases(&self, pool: &PgPool) -> Result<RotateOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

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
        let lease_count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*)::bigint FROM {}",
            lease_child_name(schema, next_slot as usize)
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if lease_count > 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy { slot: next_slot });
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
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy { slot: next_slot });
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(RotateOutcome::Rotated {
            slot: next_slot,
            generation: next_generation,
        })
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.prune_oldest")]
    pub async fn prune_oldest(&self, pool: &PgPool) -> Result<PruneOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

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

        let Some((slot, generation)) = target else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::Noop);
        };

        let ready_child = ready_child_name(schema, slot as usize);
        let done_child = done_child_name(schema, slot as usize);

        sqlx::query("SET LOCAL lock_timeout = '50ms'")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        let lock_tables = sqlx::query(&format!(
            "LOCK TABLE {ready_child}, {done_child} IN ACCESS EXCLUSIVE MODE"
        ))
        .execute(tx.as_mut())
        .await;

        if lock_tables.is_err() {
            let _ = tx.rollback().await;
            return Ok(PruneOutcome::Blocked { slot });
        }

        let active_leases: i64 = sqlx::query_scalar(&format!(
            r#"
            SELECT count(*)::bigint
            FROM {schema}.leases
            WHERE ready_slot = $1
              AND ready_generation = $2
            "#
        ))
        .bind(slot)
        .bind(generation)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if active_leases > 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive { slot });
        }

        let pending: i64 = sqlx::query_scalar(&format!(
            r#"
            SELECT count(*)::bigint
            FROM {ready_child} AS ready
            LEFT JOIN {done_child} AS done
              ON done.ready_generation = ready.ready_generation
             AND done.queue = ready.queue
             AND done.priority = ready.priority
             AND done.lane_seq = ready.lane_seq
            WHERE done.lane_seq IS NULL
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if pending > 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive { slot });
        }

        let pruned_terminal_counts: Vec<(String, i16, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT queue, priority, count(*)::bigint AS pruned_count
            FROM {done_child}
            GROUP BY queue, priority
            "#
        ))
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let truncate = sqlx::query(&format!("TRUNCATE TABLE {ready_child}, {done_child}",))
            .execute(tx.as_mut())
            .await;

        match truncate {
            Ok(_) => {
                if !pruned_terminal_counts.is_empty() {
                    self.adjust_terminal_rollups_batch(&mut tx, pruned_terminal_counts.into_iter())
                        .await?;
                }
                tx.commit().await.map_err(map_sqlx_error)?;
                Ok(PruneOutcome::Pruned { slot })
            }
            Err(_) => {
                let _ = tx.rollback().await;
                Ok(PruneOutcome::Blocked { slot })
            }
        }
    }

    #[tracing::instrument(skip(self, pool), name = "queue_storage.prune_oldest_leases")]
    pub async fn prune_oldest_leases(&self, pool: &PgPool) -> Result<PruneOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        // `PruneLeasesPlan` in
        // `correctness/storage/AwaStorageLockOrder.tla` requires the
        // sequence `lease_ring_state FOR UPDATE` →
        // `lease_ring_slots[slot] FOR UPDATE` → `ACCESS EXCLUSIVE` on
        // the child. Without these locks a concurrent rotator can flip
        // the cursor under the prune's liveness check (current_slot
        // recheck races a CAS update) and prune what should be the
        // active partition.
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

        sqlx::query("SET LOCAL lock_timeout = '50ms'")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        let lock_table = sqlx::query(&format!(
            "LOCK TABLE {lease_child} IN ACCESS EXCLUSIVE MODE"
        ))
        .execute(tx.as_mut())
        .await;

        if lock_table.is_err() {
            let _ = tx.rollback().await;
            return Ok(PruneOutcome::Blocked { slot });
        }

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
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive { slot });
        }

        let active_leases: i64 =
            sqlx::query_scalar(&format!("SELECT count(*)::bigint FROM {lease_child}"))
                .fetch_one(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;

        if active_leases > 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive { slot });
        }

        let truncate = sqlx::query(&format!("TRUNCATE TABLE {lease_child}"))
            .execute(tx.as_mut())
            .await;

        match truncate {
            Ok(_) => {
                tx.commit().await.map_err(map_sqlx_error)?;
                Ok(PruneOutcome::Pruned { slot })
            }
            Err(_) => {
                let _ = tx.rollback().await;
                Ok(PruneOutcome::Blocked { slot })
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
    /// Advances `claim_ring_state.current_slot` via compare-and-swap. Before
    /// flipping the cursor the target partition must be drained: both the
    /// `lease_claims_<next>` and `lease_claim_closures_<next>` child tables
    /// must be empty. This is what the `rotate → prune → rotate` ring
    /// invariant requires — we only hand out a slot to new claims when a
    /// prior prune has truncated it.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.rotate_claims")]
    pub async fn rotate_claims(&self, pool: &PgPool) -> Result<RotateOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

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

        // Busy check: both children of the incoming slot must be empty.
        // A non-empty `lease_claims_<next>` means the previous lap's
        // prune hasn't run (or didn't complete); rotating anyway would
        // mix fresh claims with legacy rows and defeat the point of
        // partitioning. Non-empty `lease_claim_closures_<next>` means
        // prune fell behind on closures specifically.
        let claim_count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*)::bigint FROM {}",
            claim_child_name(schema, next_slot as usize)
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let closure_count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*)::bigint FROM {}",
            closure_child_name(schema, next_slot as usize)
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if claim_count > 0 || closure_count > 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy { slot: next_slot });
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
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy { slot: next_slot });
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
    /// `TRUNCATE`-ing both its `lease_claims_<slot>` and
    /// `lease_claim_closures_<slot>` children. Takes the full ADR-023
    /// lock sequence:
    ///
    /// 1. `FOR UPDATE` on `claim_ring_state` (serialises with rotate).
    /// 2. `FOR UPDATE` on the target `claim_ring_slots` row.
    /// 3. `SET LOCAL lock_timeout = '50ms'` then `LOCK TABLE ACCESS
    ///    EXCLUSIVE` on both children (serialises with in-flight
    ///    claim/complete/rescue writers; gives up gracefully under
    ///    contention).
    /// 4. Verifies the slot is not the current one, and that every
    ///    claim in the partition has a matching closure row.
    /// 5. `TRUNCATE` both children in a single statement.
    ///
    /// The "every claim has a closure" precondition is what ADR-023
    /// calls `PartitionTruncateSafety`. If an open claim remains in the
    /// partition, prune returns `SkippedActive` and the claim has to
    /// drain by normal completion or be rescued by
    /// `rescue_stale_receipt_claims_tx` before prune will try again.
    #[tracing::instrument(skip(self, pool), name = "queue_storage.prune_oldest_claims")]
    pub async fn prune_oldest_claims(&self, pool: &PgPool) -> Result<PruneOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

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
        let closure_child = closure_child_name(schema, slot as usize);

        sqlx::query("SET LOCAL lock_timeout = '50ms'")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        let lock_tables = sqlx::query(&format!(
            "LOCK TABLE {claim_child}, {closure_child} IN ACCESS EXCLUSIVE MODE"
        ))
        .execute(tx.as_mut())
        .await;

        if lock_tables.is_err() {
            let _ = tx.rollback().await;
            return Ok(PruneOutcome::Blocked { slot });
        }

        // After taking ACCESS EXCLUSIVE, recheck that the slot is not
        // the current one (rotate may have won the ring-state lock
        // earlier).
        let current_slot: i32 = sqlx::query_scalar(&format!(
            r#"
            SELECT current_slot FROM {schema}.claim_ring_state WHERE singleton = TRUE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if current_slot == slot {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive { slot });
        }

        // `PartitionTruncateSafety`: every claim in this partition must
        // have a matching closure. Any open claim means a worker is
        // still running (or a rescue hasn't fired yet); we bail and let
        // normal lifecycle drain the partition.
        let open_claims: i64 = sqlx::query_scalar(&format!(
            r#"
            SELECT count(*)::bigint
            FROM {claim_child} AS claims
            WHERE NOT EXISTS (
                SELECT 1 FROM {closure_child} AS closures
                WHERE closures.claim_slot = claims.claim_slot
                  AND closures.job_id = claims.job_id
                  AND closures.run_lease = claims.run_lease
            )
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if open_claims > 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PruneOutcome::SkippedActive { slot });
        }

        let truncate = sqlx::query(&format!("TRUNCATE TABLE {claim_child}, {closure_child}"))
            .execute(tx.as_mut())
            .await;

        match truncate {
            Ok(_) => {
                tx.commit().await.map_err(map_sqlx_error)?;
                Ok(PruneOutcome::Pruned { slot })
            }
            Err(_) => {
                let _ = tx.rollback().await;
                Ok(PruneOutcome::Blocked { slot })
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
