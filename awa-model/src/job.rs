use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;
use std::fmt;

/// Job states in the lifecycle state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "awa.job_state", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Scheduled,
    Available,
    Running,
    Completed,
    Retryable,
    Failed,
    Cancelled,
    WaitingExternal,
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for JobState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "scheduled" => Ok(JobState::Scheduled),
            "available" => Ok(JobState::Available),
            "running" => Ok(JobState::Running),
            "completed" => Ok(JobState::Completed),
            "retryable" => Ok(JobState::Retryable),
            "failed" => Ok(JobState::Failed),
            "cancelled" => Ok(JobState::Cancelled),
            "waiting_external" => Ok(JobState::WaitingExternal),
            other => Err(format!("unknown job state: {other}")),
        }
    }
}

impl JobState {
    /// Canonical snake_case representation used by Postgres and JSON.
    pub const fn as_str(&self) -> &'static str {
        match self {
            JobState::Scheduled => "scheduled",
            JobState::Available => "available",
            JobState::Running => "running",
            JobState::Completed => "completed",
            JobState::Retryable => "retryable",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
            JobState::WaitingExternal => "waiting_external",
        }
    }

    /// Bit position for unique_states bitmask.
    pub fn bit_position(&self) -> u8 {
        match self {
            JobState::Scheduled => 0,
            JobState::Available => 1,
            JobState::Running => 2,
            JobState::Completed => 3,
            JobState::Retryable => 4,
            JobState::Failed => 5,
            JobState::Cancelled => 6,
            JobState::WaitingExternal => 7,
        }
    }

    /// Check if this state is terminal (no further transitions possible).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobState::Completed | JobState::Failed | JobState::Cancelled
        )
    }
}

/// A row from the `awa.jobs` table.
///
/// `#[non_exhaustive]` so that adding a field is not a Rust source break: code
/// outside awa reads the fields but cannot build the struct literally or
/// match it without `..`. A field added later also carries `#[sqlx(default)]`,
/// so `query_as::<_, JobRow>` over an explicit column list keeps working.
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobRow {
    pub id: i64,
    pub kind: String,
    pub queue: String,
    pub args: serde_json::Value,
    pub state: JobState,
    pub priority: i16,
    pub attempt: i16,
    pub run_lease: i64,
    pub max_attempts: i16,
    pub run_at: DateTime<Utc>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub deadline_at: Option<DateTime<Utc>>,
    pub attempted_at: Option<DateTime<Utc>>,
    pub finalized_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub errors: Option<Vec<serde_json::Value>>,
    pub metadata: serde_json::Value,
    pub tags: Vec<String>,
    pub unique_key: Option<Vec<u8>>,
    /// Unique states bitmask — stored as BIT(8) in Postgres.
    /// Skipped in FromRow since it's only used by the DB-side unique index.
    #[sqlx(skip)]
    pub unique_states: Option<u8>,
    /// Callback ID for external webhook completion.
    pub callback_id: Option<uuid::Uuid>,
    /// Deadline for callback timeout.
    pub callback_timeout_at: Option<DateTime<Utc>>,
    /// CEL filter expression for callback resolution.
    pub callback_filter: Option<String>,
    /// CEL expression: does the payload indicate completion?
    pub callback_on_complete: Option<String>,
    /// CEL expression: does the payload indicate failure?
    pub callback_on_fail: Option<String>,
    /// CEL expression to transform the payload before returning.
    pub callback_transform: Option<String>,
    /// Structured progress reported by the handler during execution.
    pub progress: Option<serde_json::Value>,
    /// The completed attempt's output (ADR-048, queue storage). `None` for a
    /// job that did not complete with one, and on list reads, which do not
    /// load it. A failed attempt's output is in its entry in `errors`.
    #[sqlx(default)]
    #[serde(default)]
    pub output: Option<serde_json::Value>,
}

impl JobRow {
    /// This job's completion output, decoded as the output type `T`
    /// declares (ADR-048). `Ok(None)` when it has none.
    ///
    /// Fails when the job is not of `T`'s kind, or when the stored value does
    /// not decode: another language or an earlier version may have written
    /// a different shape.
    pub fn output_as<T: crate::JobOutput>(&self) -> Result<Option<T::Output>, crate::AwaError> {
        self.decode_output::<T>(self.output.as_ref())
    }

    /// The output kept in this job's error entries, from the latest failed
    /// attempt that recorded one, decoded as `T` declares (ADR-048).
    pub fn error_output_as<T: crate::JobOutput>(
        &self,
    ) -> Result<Option<T::Output>, crate::AwaError> {
        let output = self.errors.iter().flatten().rev().find_map(|entry| {
            entry
                .get(crate::output::ERROR_ENTRY_OUTPUT_KEY)
                .filter(|output| !output.is_null())
        });
        self.decode_output::<T>(output)
    }

    fn decode_output<T: crate::JobOutput>(
        &self,
        output: Option<&serde_json::Value>,
    ) -> Result<Option<T::Output>, crate::AwaError> {
        if self.kind != T::kind() {
            return Err(crate::AwaError::Validation(format!(
                "job {} is of kind {}, not {}",
                self.id,
                self.kind,
                T::kind()
            )));
        }
        output
            .map(|output| serde_json::from_value(output.clone()))
            .transpose()
            .map_err(crate::AwaError::from)
    }
}

/// Options for inserting a job.
#[derive(Debug, Clone)]
pub struct InsertOpts {
    pub queue: String,
    pub priority: i16,
    pub max_attempts: i16,
    pub run_at: Option<DateTime<Utc>>,
    pub deadline_duration: Option<chrono::Duration>,
    pub metadata: serde_json::Value,
    pub tags: Vec<String>,
    pub unique: Option<UniqueOpts>,
    /// Routes the job to a specific enqueue shard when the destination
    /// queue has `enqueue_shards > 1`. Jobs sharing an `ordering_key`
    /// land on the same shard, which preserves FIFO within that key.
    /// `None` falls back to the per-store rotor that spreads batches
    /// across shards. Ignored at `enqueue_shards = 1`. See ADR-025.
    pub ordering_key: Option<Vec<u8>>,
}

impl Default for InsertOpts {
    fn default() -> Self {
        Self {
            queue: "default".to_string(),
            priority: 2,
            max_attempts: 25,
            run_at: None,
            deadline_duration: None,
            metadata: serde_json::json!({}),
            tags: Vec::new(),
            unique: None,
            ordering_key: None,
        }
    }
}

/// Uniqueness constraint options.
#[derive(Debug, Clone)]
pub struct UniqueOpts {
    /// Include queue in uniqueness calculation.
    pub by_queue: bool,
    /// Include args in uniqueness calculation.
    pub by_args: bool,
    /// Period bucket for time-based uniqueness (epoch seconds / period).
    pub by_period: Option<i64>,
    /// States in which uniqueness is enforced.
    /// Default: scheduled, available, running, completed, retryable (bits 0-4).
    pub states: u8,
}

impl Default for UniqueOpts {
    fn default() -> Self {
        Self {
            by_queue: false,
            by_args: true,
            by_period: None,
            // Default: bits 0-4 set (scheduled, available, running, completed, retryable)
            states: 0b0001_1111,
        }
    }
}

impl UniqueOpts {
    /// Convert the states bitmask to a BIT(8) representation for Postgres.
    pub fn states_bits(&self) -> Vec<u8> {
        vec![self.states]
    }
}

/// Parameters for bulk insert.
#[derive(Debug, Clone)]
pub struct InsertParams {
    pub kind: String,
    pub args: serde_json::Value,
    pub opts: InsertOpts,
}
