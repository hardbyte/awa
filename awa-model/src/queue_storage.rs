use crate::admin::{CallbackConfig, CallbackPollResult};
use crate::dlq::RetryFromDlqOpts;
use crate::error::AwaError;
use crate::insert::prepare_row_raw;
use crate::{InsertParams, JobRow, JobState};
use chrono::TimeDelta;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_SCHEMA: &str = "awa_exp";
const DEFAULT_QUEUE_SLOT_COUNT: usize = 16;
const DEFAULT_LEASE_SLOT_COUNT: usize = 8;

#[derive(Debug, Clone)]
pub struct QueueStorageConfig {
    pub schema: String,
    pub queue_slot_count: usize,
    pub lease_slot_count: usize,
}

impl Default for QueueStorageConfig {
    fn default() -> Self {
        Self {
            schema: DEFAULT_SCHEMA.to_string(),
            queue_slot_count: DEFAULT_QUEUE_SLOT_COUNT,
            lease_slot_count: DEFAULT_LEASE_SLOT_COUNT,
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

    fn take_callback_result(&mut self) -> Option<serde_json::Value> {
        self.metadata
            .as_object_mut()
            .expect("runtime payload metadata object")
            .remove("_awa_callback_result")
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
    unique_states: Option<String>,
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
struct ReadyJobLeaseRow {
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

impl ReadyJobLeaseRow {
    fn into_job_row(self, deadline_duration: Duration) -> Result<JobRow, AwaError> {
        let payload = RuntimePayload::from_json(self.payload)?;
        let deadline_delta = chrono::Duration::from_std(deadline_duration).map_err(|err| {
            AwaError::Validation(format!(
                "invalid queue storage deadline duration for claimed job {}: {err}",
                self.job_id
            ))
        })?;

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
            heartbeat_at: self.attempted_at,
            deadline_at: self.attempted_at.map(|ts| ts + deadline_delta),
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
    finalized_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    payload: serde_json::Value,
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
    heartbeat_at: Option<DateTime<Utc>>,
    deadline_at: Option<DateTime<Utc>>,
    attempted_at: Option<DateTime<Utc>>,
    finalized_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    unique_key: Option<Vec<u8>>,
    unique_states: Option<String>,
    callback_id: Option<Uuid>,
    callback_timeout_at: Option<DateTime<Utc>>,
    callback_filter: Option<String>,
    callback_on_complete: Option<String>,
    callback_on_fail: Option<String>,
    callback_transform: Option<String>,
    payload: serde_json::Value,
}

impl LeaseJobRow {
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
/// - hot mutable state restricted to queue cursors and counters
///
#[derive(Debug)]
pub struct QueueStorage {
    config: QueueStorageConfig,
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
        validate_ident(&config.schema)?;
        Ok(Self { config })
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

    pub fn ready_child_relname(&self, slot: usize) -> String {
        format!("ready_entries_{slot}")
    }

    pub fn done_child_relname(&self, slot: usize) -> String {
        format!("done_entries_{slot}")
    }

    pub fn leases_relname(&self) -> &'static str {
        "leases"
    }

    pub fn leases_child_relname(&self, slot: usize) -> String {
        format!("leases_{slot}")
    }

    pub async fn active_schema(pool: &PgPool) -> Result<Option<String>, AwaError> {
        sqlx::query_scalar(
            "SELECT schema_name FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'",
        )
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)
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

    pub async fn install(&self, pool: &PgPool) -> Result<(), AwaError> {
        let schema = self.schema();

        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

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
            CREATE TABLE IF NOT EXISTS {schema}.queue_lanes (
                queue           TEXT NOT NULL,
                priority        SMALLINT NOT NULL,
                next_seq        BIGINT NOT NULL DEFAULT 1,
                claim_seq       BIGINT NOT NULL DEFAULT 1,
                available_count BIGINT NOT NULL DEFAULT 0,
                running_count   BIGINT NOT NULL DEFAULT 0,
                completed_count BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (queue, priority)
            )
            "#
        ))
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

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

        sqlx::query(
            r#"
            INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
            VALUES ('queue_storage', $1, now())
            ON CONFLICT (backend)
            DO UPDATE SET schema_name = EXCLUDED.schema_name, updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(schema)
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
                kind              TEXT NOT NULL,
                queue             TEXT NOT NULL,
                args              JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                state             awa.job_state NOT NULL DEFAULT 'running',
                priority          SMALLINT NOT NULL,
                attempt           SMALLINT NOT NULL DEFAULT 1,
                run_lease         BIGINT NOT NULL DEFAULT 1,
                max_attempts      SMALLINT NOT NULL DEFAULT 25,
                lane_seq          BIGINT NOT NULL,
                run_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                heartbeat_at      TIMESTAMPTZ,
                deadline_at       TIMESTAMPTZ,
                attempted_at      TIMESTAMPTZ,
                finalized_at      TIMESTAMPTZ,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                unique_key        BYTEA,
                unique_states     TEXT,
                callback_id       UUID,
                callback_timeout_at TIMESTAMPTZ,
                callback_filter   TEXT,
                callback_on_complete TEXT,
                callback_on_fail  TEXT,
                callback_transform TEXT,
                payload           JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                PRIMARY KEY (lease_slot, queue, priority, lane_seq)
            ) PARTITION BY LIST (lease_slot)
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

        Ok(())
    }

    pub async fn reset(&self, pool: &PgPool) -> Result<(), AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            TRUNCATE
                {schema}.ready_entries,
                {schema}.done_entries,
                {schema}.dlq_entries,
                {schema}.leases,
                {schema}.deferred_jobs,
                {schema}.queue_lanes,
                {schema}.queue_ring_slots,
                {schema}.lease_ring_slots
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

    async fn current_lease_ring<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    ) -> Result<(i32, i64), AwaError> {
        let schema = self.schema();
        sqlx::query_as(&format!(
            r#"
            SELECT current_slot, generation
            FROM {schema}.lease_ring_state
            WHERE singleton = TRUE
            "#
        ))
        .fetch_one(tx.as_mut())
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
                UPDATE {schema}.queue_lanes
                SET next_seq = next_seq + $3,
                    available_count = available_count + $3
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
                UPDATE {schema}.queue_lanes
                SET next_seq = next_seq + $3,
                    available_count = available_count + $3
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
        running_delta: i64,
        completed_delta: i64,
    ) -> Result<(), AwaError> {
        let schema = self.schema();
        sqlx::query(&format!(
            r#"
            UPDATE {schema}.queue_lanes
            SET available_count = GREATEST(0, available_count + $3),
                running_count = GREATEST(0, running_count + $4),
                completed_count = GREATEST(0, completed_count + $5)
            WHERE queue = $1 AND priority = $2
            "#
        ))
        .bind(queue)
        .bind(priority)
        .bind(available_delta)
        .bind(running_delta)
        .bind(completed_delta)
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

        let queues_to_notify: BTreeSet<String> = rows.iter().map(|row| row.queue.clone()).collect();
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

        let rows = (0..count)
            .map(|seq| RuntimeReadyRow {
                kind: "bench_job".to_string(),
                queue: queue.to_string(),
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

        for job in jobs {
            let prepared = prepare_row_raw(job.kind.clone(), job.args.clone(), job.opts.clone())?;
            let payload = Self::payload_from_parts(prepared.metadata, prepared.tags, None, None)?;

            let ready_row = RuntimeReadyRow {
                kind: prepared.kind,
                queue: prepared.queue,
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
                    queue: ready_row.queue,
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
                .zip(ids.into_iter())
                .map(|(row, id)| DeferredJobRow { job_id: id, ..row })
                .collect();
            total += self
                .insert_deferred_rows_tx(&mut tx, deferred_rows, None)
                .await?;
        }

        let queues_to_notify: BTreeSet<String> =
            ready_rows.into_iter().map(|row| row.queue).collect();
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

    pub async fn claim_batch(
        &self,
        pool: &PgPool,
        queue: &str,
        max_batch: i64,
    ) -> Result<Vec<ClaimedEntry>, AwaError> {
        if max_batch <= 0 {
            return Ok(Vec::new());
        }

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        let lane: Option<(i16, i64, i64, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT priority, claim_seq, next_seq, available_count
            FROM {schema}.queue_lanes
            WHERE queue = $1
              AND NOT EXISTS (
                  SELECT 1 FROM awa.queue_meta
                  WHERE queue = $1 AND paused = TRUE
              )
              AND claim_seq < next_seq
              AND available_count > 0
            ORDER BY priority ASC
            LIMIT 1
            FOR UPDATE
            "#
        ))
        .bind(queue)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some((priority, claim_seq, next_seq, available_count)) = lane else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        };

        let reserve = (next_seq - claim_seq).min(available_count).min(max_batch);
        if reserve <= 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let start_seq = claim_seq;
        let end_seq = claim_seq + reserve;

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.queue_lanes
            SET claim_seq = $3,
                available_count = available_count - $4,
                running_count = running_count + $4
            WHERE queue = $1 AND priority = $2
            "#
        ))
        .bind(queue)
        .bind(priority)
        .bind(end_seq)
        .bind(reserve)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let lease_ring = self.current_lease_ring(&mut tx).await?;

        let claimed: Vec<ClaimedEntry> = sqlx::query_as(&format!(
            r#"
            INSERT INTO {schema}.leases (
                lease_slot,
                lease_generation,
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
                heartbeat_at,
                deadline_at,
                attempted_at,
                created_at,
                unique_key,
                unique_states,
                payload
            )
            SELECT
                $1,
                $2,
                ready.ready_slot,
                ready.ready_generation,
                ready.job_id,
                ready.kind,
                ready.queue,
                ready.args,
                'running'::awa.job_state,
                ready.priority,
                ready.attempt + 1,
                ready.run_lease + 1,
                ready.max_attempts,
                ready.lane_seq,
                clock_timestamp(),
                clock_timestamp(),
                clock_timestamp(),
                clock_timestamp(),
                ready.created_at,
                ready.unique_key,
                ready.unique_states,
                ready.payload
            FROM {schema}.ready_entries AS ready
            WHERE ready.queue = $3
              AND ready.priority = $4
              AND ready.lane_seq >= $5
              AND ready.lane_seq < $6
            ORDER BY ready.lane_seq ASC
            RETURNING
                queue,
                priority,
                lane_seq,
                ready_slot,
                ready_generation,
                lease_slot,
                lease_generation
            "#
        ))
        .bind(lease_ring.0)
        .bind(lease_ring.1)
        .bind(queue)
        .bind(priority)
        .bind(start_seq)
        .bind(end_seq)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if claimed.len() as i64 != reserve {
            return Err(AwaError::Validation(format!(
                "queue storage claim reservation mismatch: reserved {reserve}, claimed {}",
                claimed.len()
            )));
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(claimed)
    }

    pub async fn claim_job_batch(
        &self,
        pool: &PgPool,
        queue: &str,
        max_batch: i64,
        deadline_duration: Duration,
    ) -> Result<Vec<JobRow>, AwaError> {
        if max_batch <= 0 {
            return Ok(Vec::new());
        }

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        let lane: Option<(i16, i64, i64, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT priority, claim_seq, next_seq, available_count
            FROM {schema}.queue_lanes
            WHERE queue = $1
              AND NOT EXISTS (
                  SELECT 1 FROM awa.queue_meta
                  WHERE queue = $1 AND paused = TRUE
              )
              AND claim_seq < next_seq
              AND available_count > 0
            ORDER BY priority ASC
            LIMIT 1
            FOR UPDATE
            "#
        ))
        .bind(queue)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some((priority, claim_seq, next_seq, available_count)) = lane else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        };

        let reserve = (next_seq - claim_seq).min(available_count).min(max_batch);
        if reserve <= 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let start_seq = claim_seq;
        let end_seq = claim_seq + reserve;

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.queue_lanes
            SET claim_seq = $3,
                available_count = available_count - $4,
                running_count = running_count + $4
            WHERE queue = $1 AND priority = $2
            "#
        ))
        .bind(queue)
        .bind(priority)
        .bind(end_seq)
        .bind(reserve)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let lease_ring = self.current_lease_ring(&mut tx).await?;

        let claimed: Vec<ReadyJobLeaseRow> = sqlx::query_as(&format!(
            r#"
            INSERT INTO {schema}.leases (
                lease_slot,
                lease_generation,
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
                heartbeat_at,
                deadline_at,
                attempted_at,
                created_at,
                unique_key,
                unique_states,
                payload
            )
            SELECT
                $1,
                $2,
                ready.ready_slot,
                ready.ready_generation,
                ready.job_id,
                ready.kind,
                ready.queue,
                ready.args,
                'running'::awa.job_state,
                ready.priority,
                ready.attempt + 1,
                ready.run_lease + 1,
                ready.max_attempts,
                ready.lane_seq,
                clock_timestamp(),
                clock_timestamp(),
                clock_timestamp() + make_interval(secs => $7),
                clock_timestamp(),
                ready.created_at,
                ready.unique_key,
                ready.unique_states,
                ready.payload
            FROM {schema}.ready_entries AS ready
            WHERE ready.queue = $3
              AND ready.priority = $4
              AND ready.lane_seq >= $5
              AND ready.lane_seq < $6
            ORDER BY ready.lane_seq ASC
            RETURNING
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
            "#
        ))
        .bind(lease_ring.0)
        .bind(lease_ring.1)
        .bind(queue)
        .bind(priority)
        .bind(start_seq)
        .bind(end_seq)
        .bind(deadline_duration.as_secs_f64())
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if claimed.len() as i64 != reserve {
            return Err(AwaError::Validation(format!(
                "queue storage runtime claim reservation mismatch: reserved {reserve}, claimed {}",
                claimed.len()
            )));
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

        claimed
            .into_iter()
            .map(|row| row.into_job_row(deadline_duration))
            .collect()
    }

    pub async fn complete_batch(
        &self,
        pool: &PgPool,
        claimed: &[ClaimedEntry],
    ) -> Result<usize, AwaError> {
        if claimed.is_empty() {
            return Ok(0);
        }

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        let lease_slots: Vec<i32> = claimed.iter().map(|entry| entry.lease_slot).collect();
        let queues: Vec<String> = claimed.iter().map(|entry| entry.queue.clone()).collect();
        let priorities: Vec<i16> = claimed.iter().map(|entry| entry.priority).collect();
        let lane_seqs: Vec<i64> = claimed.iter().map(|entry| entry.lane_seq).collect();

        let moved: Vec<LeaseTransitionRow> = sqlx::query_as(&format!(
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
                leases.kind,
                leases.queue,
                leases.args,
                leases.state,
                leases.priority,
                leases.attempt,
                leases.run_lease,
                leases.max_attempts,
                leases.lane_seq,
                leases.run_at,
                leases.attempted_at,
                leases.finalized_at,
                leases.created_at,
                leases.unique_key,
                leases.unique_states
                ,leases.payload
            "#
        ))
        .bind(&lease_slots)
        .bind(&queues)
        .bind(&priorities)
        .bind(&lane_seqs)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if moved.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(0);
        }

        let finalized_at = Utc::now();
        let mut done_rows = Vec::with_capacity(moved.len());
        for entry in moved.iter().cloned() {
            let mut payload = RuntimePayload::from_json(entry.payload.clone())?;
            payload.set_progress(None);
            done_rows.push(entry.into_done_row(
                JobState::Completed,
                finalized_at,
                payload.into_json(),
            ));
        }

        self.insert_done_rows_tx(&mut tx, &done_rows, Some(JobState::Running))
            .await?;

        let mut per_lane: BTreeMap<(String, i16), i64> = BTreeMap::new();
        for entry in &moved {
            *per_lane
                .entry((entry.queue.clone(), entry.priority))
                .or_insert(0) += 1;
            self.sync_unique_claim(
                &mut tx,
                entry.job_id,
                &entry.unique_key,
                entry.unique_states.as_deref(),
                Some(JobState::Running),
                Some(JobState::Completed),
            )
            .await?;
        }

        for ((queue, priority), count) in per_lane {
            sqlx::query(&format!(
                r#"
                UPDATE {schema}.queue_lanes
                SET running_count = GREATEST(0, running_count - $3),
                    completed_count = completed_count + $3
                WHERE queue = $1 AND priority = $2
                "#
            ))
            .bind(&queue)
            .bind(priority)
            .bind(count)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(moved.len())
    }

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

        let moved: Vec<LeaseTransitionRow> = sqlx::query_as(&format!(
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
                leases.kind,
                leases.queue,
                leases.args,
                leases.state,
                leases.priority,
                leases.attempt,
                leases.run_lease,
                leases.max_attempts,
                leases.lane_seq,
                leases.run_at,
                leases.attempted_at,
                leases.finalized_at,
                leases.created_at,
                leases.unique_key,
                leases.unique_states
                ,leases.payload
            "#
        ))
        .bind(&job_ids)
        .bind(&run_leases)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if moved.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let finalized_at = Utc::now();
        let mut done_rows = Vec::with_capacity(moved.len());
        for entry in moved.iter().cloned() {
            let mut payload = RuntimePayload::from_json(entry.payload.clone())?;
            payload.set_progress(None);
            done_rows.push(entry.into_done_row(
                JobState::Completed,
                finalized_at,
                payload.into_json(),
            ));
        }

        self.insert_done_rows_tx(&mut tx, &done_rows, Some(JobState::Running))
            .await?;

        let mut per_lane: BTreeMap<(String, i16), i64> = BTreeMap::new();
        for entry in &moved {
            *per_lane
                .entry((entry.queue.clone(), entry.priority))
                .or_insert(0) += 1;
            self.sync_unique_claim(
                &mut tx,
                entry.job_id,
                &entry.unique_key,
                entry.unique_states.as_deref(),
                Some(JobState::Running),
                Some(JobState::Completed),
            )
            .await?;
        }

        for ((queue, priority), count) in per_lane {
            sqlx::query(&format!(
                r#"
                UPDATE {schema}.queue_lanes
                SET running_count = GREATEST(0, running_count - $3),
                    completed_count = completed_count + $3
                WHERE queue = $1 AND priority = $2
                "#
            ))
            .bind(&queue)
            .bind(priority)
            .bind(count)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(moved
            .into_iter()
            .map(|entry| (entry.job_id, entry.run_lease))
            .collect())
    }

    pub async fn queue_counts(&self, pool: &PgPool, queue: &str) -> Result<QueueCounts, AwaError> {
        let schema = self.schema();
        let row: Option<(i64, i64, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT
                COALESCE(sum(available_count), 0)::bigint AS available,
                COALESCE(sum(running_count), 0)::bigint AS running,
                COALESCE(sum(completed_count), 0)::bigint AS completed
            FROM {schema}.queue_lanes
            WHERE queue = $1
            "#
        ))
        .bind(queue)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;

        let (available, running, completed) = row.unwrap_or((0, 0, 0));
        Ok(QueueCounts {
            available,
            running,
            completed,
        })
    }

    fn with_progress(
        payload: serde_json::Value,
        progress: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, AwaError> {
        let mut payload = RuntimePayload::from_json(payload)?;
        payload.set_progress(progress);
        Ok(payload.into_json())
    }

    async fn take_callback_payload(
        &self,
        pool: &PgPool,
        job_id: i64,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, AwaError> {
        let mut payload = RuntimePayload::from_json(payload)?;
        let result = payload
            .take_callback_result()
            .unwrap_or(serde_json::Value::Null);

        sqlx::query(&format!(
            "UPDATE {} SET payload = $2 WHERE job_id = $1",
            self.leases_table()
        ))
        .bind(job_id)
        .bind(payload.into_json())
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

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
        let queues: BTreeSet<String> = queues.into_iter().collect();
        for queue in queues {
            sqlx::query("SELECT pg_notify($1, '')")
                .bind(format!("awa:{queue}"))
                .execute(tx.as_mut())
                .await
                .map_err(map_sqlx_error)?;
        }
        Ok(())
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
                heartbeat_at,
                deadline_at,
                attempted_at,
                finalized_at,
                created_at,
                unique_key,
                unique_states,
                callback_id,
                callback_timeout_at,
                callback_filter,
                callback_on_complete,
                callback_on_fail,
                callback_transform,
                payload
            FROM {schema}.leases
            WHERE job_id = $1
            ORDER BY run_lease DESC
            "#,
        ))
        .bind(job_id)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;
        for row in lease_rows {
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
        let updated = sqlx::query(&format!(
            r#"
            UPDATE {}
            SET callback_id = $2,
                callback_timeout_at = clock_timestamp() + make_interval(secs => $3),
                callback_filter = NULL,
                callback_on_complete = NULL,
                callback_on_fail = NULL,
                callback_transform = NULL
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
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        if updated.rows_affected() == 0 {
            return Err(AwaError::Validation("job is not in running state".into()));
        }
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
                    let bad_vars: Vec<&str> = program
                        .references()
                        .variables()
                        .into_iter()
                        .filter(|v| *v != "payload")
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
        let updated = sqlx::query(&format!(
            r#"
            UPDATE {}
            SET callback_id = $2,
                callback_timeout_at = clock_timestamp() + make_interval(secs => $3),
                callback_filter = $5,
                callback_on_complete = $6,
                callback_on_fail = $7,
                callback_transform = $8
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
        .bind(&config.filter)
        .bind(&config.on_complete)
        .bind(&config.on_fail)
        .bind(&config.transform)
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        if updated.rows_affected() == 0 {
            return Err(AwaError::Validation("job is not in running state".into()));
        }
        Ok(callback_id)
    }

    pub async fn cancel_callback(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
    ) -> Result<bool, AwaError> {
        let result = sqlx::query(&format!(
            r#"
            UPDATE {}
            SET callback_id = NULL,
                callback_timeout_at = NULL,
                callback_filter = NULL,
                callback_on_complete = NULL,
                callback_on_fail = NULL,
                callback_transform = NULL
            WHERE job_id = $1
              AND callback_id IS NOT NULL
              AND state = 'running'
              AND run_lease = $2
            "#,
            self.leases_table()
        ))
        .bind(job_id)
        .bind(run_lease)
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() > 0)
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
        let row: Option<(JobState, Option<Uuid>, serde_json::Value)> = sqlx::query_as(&format!(
            "SELECT state, callback_id, payload FROM {} WHERE job_id = $1 ORDER BY run_lease DESC LIMIT 1",
            self.leases_table()
        ))
        .bind(job_id)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;

        match row {
            Some((JobState::Running, None, payload)) => {
                let payload = RuntimePayload::from_json(payload)?;
                if payload.metadata.get("_awa_callback_result").is_some() {
                    let result = self
                        .take_callback_payload(pool, job_id, payload.into_json())
                        .await?;
                    Ok(CallbackPollResult::Resolved(result))
                } else {
                    Ok(CallbackPollResult::UnexpectedState {
                        token: callback_id,
                        state: JobState::Running,
                    })
                }
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

    pub async fn complete_external(
        &self,
        pool: &PgPool,
        callback_id: Uuid,
        payload: Option<serde_json::Value>,
        run_lease: Option<i64>,
        resume: bool,
    ) -> Result<JobRow, AwaError> {
        if resume {
            let row: Option<LeaseJobRow> = sqlx::query_as(&format!(
                r#"
                UPDATE {}
                SET state = 'running',
                    callback_id = NULL,
                    callback_timeout_at = NULL,
                    callback_filter = NULL,
                    callback_on_complete = NULL,
                    callback_on_fail = NULL,
                    callback_transform = NULL,
                    heartbeat_at = clock_timestamp(),
                    payload = jsonb_set(
                        payload,
                        '{{metadata,_awa_callback_result}}',
                        to_jsonb($3::jsonb),
                        true
                    )
                WHERE callback_id = $1
                  AND state IN ('waiting_external', 'running')
                  AND ($2::bigint IS NULL OR run_lease = $2)
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
                    heartbeat_at,
                    deadline_at,
                    attempted_at,
                    finalized_at,
                    created_at,
                    unique_key,
                    unique_states,
                    callback_id,
                    callback_timeout_at,
                    callback_filter,
                    callback_on_complete,
                    callback_on_fail,
                    callback_transform,
                    payload
                "#,
                self.leases_table()
            ))
            .bind(callback_id)
            .bind(run_lease)
            .bind(payload.unwrap_or(serde_json::Value::Null))
            .fetch_optional(pool)
            .await
            .map_err(map_sqlx_error)?;

            return row.map(LeaseJobRow::into_job_row).transpose()?.ok_or(
                AwaError::CallbackNotFound {
                    callback_id: callback_id.to_string(),
                },
            );
        }

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Option<LeaseTransitionRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE callback_id = $1
              AND state IN ('waiting_external', 'running')
              AND ($2::bigint IS NULL OR run_lease = $2)
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
        .bind(callback_id)
        .bind(run_lease)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(moved) = moved else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Err(AwaError::CallbackNotFound {
                callback_id: callback_id.to_string(),
            });
        };

        let mut payload = RuntimePayload::from_json(moved.payload.clone())?;
        payload.set_progress(None);
        let done_row =
            moved
                .clone()
                .into_done_row(JobState::Completed, Utc::now(), payload.into_json());
        self.insert_done_rows_tx(&mut tx, std::slice::from_ref(&done_row), Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, -1, 1)
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
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Option<LeaseTransitionRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE callback_id = $1
              AND state IN ('waiting_external', 'running')
              AND ($2::bigint IS NULL OR run_lease = $2)
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
        .bind(callback_id)
        .bind(run_lease)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(moved) = moved else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Err(AwaError::CallbackNotFound {
                callback_id: callback_id.to_string(),
            });
        };

        let mut payload = RuntimePayload::from_json(moved.payload.clone())?;
        payload.push_error(lifecycle_error(error, moved.attempt, true));
        let done_row =
            moved
                .clone()
                .into_done_row(JobState::Failed, Utc::now(), payload.into_json());
        self.insert_done_rows_tx(&mut tx, std::slice::from_ref(&done_row), Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, -1, 1)
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
        let moved: Option<LeaseTransitionRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE callback_id = $1
              AND state = 'waiting_external'
              AND ($2::bigint IS NULL OR run_lease = $2)
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
        .bind(callback_id)
        .bind(run_lease)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(moved) = moved else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Err(AwaError::CallbackNotFound {
                callback_id: callback_id.to_string(),
            });
        };

        let ready_row = ExistingReadyRow {
            attempt: 0,
            run_at: Utc::now(),
            ..moved
                .clone()
                .into_ready_row(Utc::now(), moved.payload.clone())
        };
        self.insert_existing_ready_rows_tx(&mut tx, vec![ready_row.clone()], Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, -1, 0)
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
            unique_states: ready_row.unique_states,
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
        let row: Option<LeaseJobRow> = sqlx::query_as(&format!(
            r#"
            UPDATE {}
            SET callback_timeout_at = clock_timestamp() + make_interval(secs => $2)
            WHERE callback_id = $1
              AND state = 'waiting_external'
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
                heartbeat_at,
                deadline_at,
                attempted_at,
                finalized_at,
                created_at,
                unique_key,
                unique_states,
                callback_id,
                callback_timeout_at,
                callback_filter,
                callback_on_complete,
                callback_on_fail,
                callback_transform,
                payload
            "#,
            self.leases_table()
        ))
        .bind(callback_id)
        .bind(timeout.as_secs_f64())
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;

        row.map(LeaseJobRow::into_job_row)
            .transpose()?
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
        sqlx::query(&format!(
            r#"
            UPDATE {}
            SET payload = jsonb_set(payload, '{{progress}}', to_jsonb($3::jsonb), true)
            WHERE job_id = $1
              AND run_lease = $2
              AND state IN ('running', 'waiting_external')
            "#,
            self.leases_table()
        ))
        .bind(job_id)
        .bind(run_lease)
        .bind(progress)
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;
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
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() as usize)
    }

    pub async fn heartbeat_progress_batch(
        &self,
        pool: &PgPool,
        jobs: &[(i64, i64, serde_json::Value)],
    ) -> Result<usize, AwaError> {
        if jobs.is_empty() {
            return Ok(0);
        }

        let job_ids: Vec<i64> = jobs.iter().map(|(job_id, _, _)| *job_id).collect();
        let run_leases: Vec<i64> = jobs.iter().map(|(_, run_lease, _)| *run_lease).collect();
        let progress: Vec<serde_json::Value> =
            jobs.iter().map(|(_, _, value)| value.clone()).collect();
        let result = sqlx::query(&format!(
            r#"
            WITH inflight AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::jsonb[]) AS v(job_id, run_lease, progress)
            )
            UPDATE {}
            SET heartbeat_at = clock_timestamp(),
                payload = jsonb_set(payload, '{{progress}}', to_jsonb(inflight.progress), true)
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
        .bind(&progress)
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() as usize)
    }

    pub async fn retry_after(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        retry_after: Duration,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Option<LeaseTransitionRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE job_id = $1
              AND run_lease = $2
              AND state = 'running'
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
        .bind(run_lease)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(moved) = moved else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };

        let payload = Self::with_progress(moved.payload.clone(), progress)?;
        let deferred = moved.clone().into_deferred_row(
            JobState::Retryable,
            Utc::now()
                + TimeDelta::from_std(retry_after).map_err(|err| {
                    AwaError::Validation(format!("invalid retry_after duration: {err}"))
                })?,
            Some(Utc::now()),
            payload,
        );
        self.insert_deferred_rows_tx(&mut tx, vec![deferred.clone()], Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, -1, 0)
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
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Option<LeaseTransitionRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE job_id = $1
              AND run_lease = $2
              AND state = 'running'
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
        .bind(run_lease)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(moved) = moved else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };

        let payload = Self::with_progress(moved.payload.clone(), progress)?;
        let mut deferred = moved.clone().into_deferred_row(
            JobState::Scheduled,
            Utc::now()
                + TimeDelta::from_std(snooze_for).map_err(|err| {
                    AwaError::Validation(format!("invalid snooze duration: {err}"))
                })?,
            None,
            payload,
        );
        deferred.attempt = deferred.attempt.saturating_sub(1);
        self.insert_deferred_rows_tx(&mut tx, vec![deferred.clone()], Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, -1, 0)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(deferred.into_job_row()?))
    }

    pub async fn cancel_running(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Option<LeaseTransitionRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE job_id = $1
              AND run_lease = $2
              AND state = 'running'
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
        .bind(run_lease)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(moved) = moved else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };

        let payload = Self::with_progress(moved.payload.clone(), progress)?;
        let done = moved
            .clone()
            .into_done_row(JobState::Cancelled, Utc::now(), payload);
        self.insert_done_rows_tx(&mut tx, std::slice::from_ref(&done), Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, -1, 1)
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
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Option<LeaseTransitionRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE job_id = $1
              AND run_lease = $2
              AND state = 'running'
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
        .bind(run_lease)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(moved) = moved else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };

        let mut payload =
            RuntimePayload::from_json(Self::with_progress(moved.payload.clone(), progress)?)?;
        payload.push_error(lifecycle_error(error, moved.attempt, true));
        let done = moved
            .clone()
            .into_done_row(JobState::Failed, Utc::now(), payload.into_json());
        self.insert_done_rows_tx(&mut tx, std::slice::from_ref(&done), Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, -1, 1)
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
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Option<LeaseTransitionRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE job_id = $1
              AND run_lease = $2
              AND state = 'running'
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
        .bind(run_lease)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(moved) = moved else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };

        let finalized_at = Utc::now();
        let dlq_at = finalized_at;
        let mut payload =
            RuntimePayload::from_json(Self::with_progress(moved.payload.clone(), progress)?)?;
        payload.push_error(lifecycle_error(error, moved.attempt, true));
        let dlq_row = moved.clone().into_dlq_row(
            finalized_at,
            payload.into_json(),
            dlq_reason.to_string(),
            dlq_at,
        );
        self.insert_dlq_rows_tx(&mut tx, std::slice::from_ref(&dlq_row), Some(moved.state))
            .await?;
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, -1, 0)
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
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, 0, -1)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(dlq_row.into_job_row()?))
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
                unique_states: ready.unique_states,
                payload: ready.payload,
            }
            .into_job_row()?,
        ))
    }

    pub async fn fail_retryable(
        &self,
        pool: &PgPool,
        job_id: i64,
        run_lease: i64,
        error: &str,
        progress: Option<serde_json::Value>,
    ) -> Result<Option<JobRow>, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Option<LeaseTransitionRow> = sqlx::query_as(&format!(
            r#"
            DELETE FROM {schema}.leases
            WHERE job_id = $1
              AND run_lease = $2
              AND state = 'running'
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
        .bind(run_lease)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some(moved) = moved else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };

        let mut payload =
            RuntimePayload::from_json(Self::with_progress(moved.payload.clone(), progress)?)?;
        let exhausted = moved.attempt >= moved.max_attempts;
        payload.push_error(lifecycle_error(error, moved.attempt, exhausted));

        if exhausted {
            let done =
                moved
                    .clone()
                    .into_done_row(JobState::Failed, Utc::now(), payload.into_json());
            self.insert_done_rows_tx(&mut tx, std::slice::from_ref(&done), Some(moved.state))
                .await?;
            self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, -1, 1)
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
        self.adjust_lane_counts(&mut tx, &moved.queue, moved.priority, 0, -1, 0)
            .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(deferred.into_job_row()?))
    }

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
        let moved: Vec<LeaseTransitionRow> = sqlx::query_as(&format!(
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
        .bind(cutoff)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if moved.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let mut rescued = Vec::with_capacity(moved.len());
        for row in moved {
            let mut payload = RuntimePayload::from_json(row.payload.clone())?;
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
            self.adjust_lane_counts(&mut tx, &row.queue, row.priority, 0, -1, 0)
                .await?;
            rescued.push(deferred.into_job_row()?);
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(rescued)
    }

    pub async fn rescue_expired_deadlines(&self, pool: &PgPool) -> Result<Vec<JobRow>, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Vec<LeaseTransitionRow> = sqlx::query_as(&format!(
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
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if moved.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let mut rescued = Vec::with_capacity(moved.len());
        for row in moved {
            let mut payload = RuntimePayload::from_json(row.payload.clone())?;
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
            self.adjust_lane_counts(&mut tx, &row.queue, row.priority, 0, -1, 0)
                .await?;
            rescued.push(deferred.into_job_row()?);
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(rescued)
    }

    pub async fn rescue_expired_callbacks(&self, pool: &PgPool) -> Result<Vec<JobRow>, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        let moved: Vec<LeaseTransitionRow> = sqlx::query_as(&format!(
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
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if moved.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let mut rescued = Vec::with_capacity(moved.len());
        for row in moved {
            let mut payload = RuntimePayload::from_json(row.payload.clone())?;
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
                self.adjust_lane_counts(&mut tx, &row.queue, row.priority, 0, -1, 1)
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
                self.adjust_lane_counts(&mut tx, &row.queue, row.priority, 0, -1, 0)
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

    pub async fn rotate_leases(&self, pool: &PgPool) -> Result<RotateOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

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

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.lease_ring_state
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
            UPDATE {schema}.lease_ring_slots
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

    pub async fn prune_oldest(&self, pool: &PgPool) -> Result<PruneOutcome, AwaError> {
        let schema = self.schema();

        let state: (i32,) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot
            FROM {schema}.queue_ring_state
            WHERE singleton = TRUE
            "#
        ))
        .fetch_one(pool)
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
            "#
        ))
        .bind(state.0)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;

        let Some((slot, generation)) = target else {
            return Ok(PruneOutcome::Noop);
        };

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
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        if active_leases > 0 {
            return Ok(PruneOutcome::SkippedActive { slot });
        }

        let pending: i64 = sqlx::query_scalar(&format!(
            r#"
            SELECT count(*)::bigint
            FROM {} AS ready
            LEFT JOIN {} AS done
              ON done.ready_generation = ready.ready_generation
             AND done.queue = ready.queue
             AND done.priority = ready.priority
             AND done.lane_seq = ready.lane_seq
            WHERE done.lane_seq IS NULL
            "#,
            ready_child_name(schema, slot as usize),
            done_child_name(schema, slot as usize),
        ))
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        if pending > 0 {
            return Ok(PruneOutcome::SkippedActive { slot });
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        sqlx::query("SET LOCAL lock_timeout = '50ms'")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        let truncate = sqlx::query(&format!(
            "TRUNCATE TABLE {}, {}",
            ready_child_name(schema, slot as usize),
            done_child_name(schema, slot as usize),
        ))
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

    pub async fn prune_oldest_leases(&self, pool: &PgPool) -> Result<PruneOutcome, AwaError> {
        let schema = self.schema();

        let state: (i32,) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot
            FROM {schema}.lease_ring_state
            WHERE singleton = TRUE
            "#
        ))
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        let target: Option<(i32, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT slot, generation
            FROM {schema}.lease_ring_slots
            WHERE generation >= 0
              AND slot <> $1
            ORDER BY generation ASC, slot ASC
            LIMIT 1
            "#
        ))
        .bind(state.0)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;

        let Some((slot, _generation)) = target else {
            return Ok(PruneOutcome::Noop);
        };

        let active_leases: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*)::bigint FROM {}",
            lease_child_name(schema, slot as usize),
        ))
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        if active_leases > 0 {
            return Ok(PruneOutcome::SkippedActive { slot });
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        sqlx::query("SET LOCAL lock_timeout = '50ms'")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        let truncate = sqlx::query(&format!(
            "TRUNCATE TABLE {}",
            lease_child_name(schema, slot as usize),
        ))
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

    fn job_id_sequence(&self) -> String {
        format!("{}.job_id_seq", self.schema())
    }

    fn leases_table(&self) -> String {
        format!("{}.{}", self.schema(), self.leases_relname())
    }
}
