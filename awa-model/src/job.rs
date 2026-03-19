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
        match self {
            JobState::Scheduled => write!(f, "scheduled"),
            JobState::Available => write!(f, "available"),
            JobState::Running => write!(f, "running"),
            JobState::Completed => write!(f, "completed"),
            JobState::Retryable => write!(f, "retryable"),
            JobState::Failed => write!(f, "failed"),
            JobState::Cancelled => write!(f, "cancelled"),
            JobState::WaitingExternal => write!(f, "waiting_external"),
        }
    }
}

impl JobState {
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
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
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
