//! SeaORM entity models for the Awa database schema.

use sea_orm::{DeriveActiveEnum, EnumIter};

/// SeaORM representation of the PostgreSQL `awa.job_state` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "job_state",
    rename_all = "snake_case"
)]
pub enum JobState {
    /// Job is scheduled for a future run time.
    Scheduled,
    /// Job is ready to be claimed by a worker.
    Available,
    /// Job is currently leased by a worker.
    Running,
    /// Job finished successfully.
    Completed,
    /// Job is waiting for a retry.
    Retryable,
    /// Job exhausted retries or failed permanently.
    Failed,
    /// Job was cancelled before completion.
    Cancelled,
    /// Job is waiting for an external callback.
    WaitingExternal,
}

impl From<awa::JobState> for JobState {
    fn from(value: awa::JobState) -> Self {
        match value {
            awa::JobState::Scheduled => Self::Scheduled,
            awa::JobState::Available => Self::Available,
            awa::JobState::Running => Self::Running,
            awa::JobState::Completed => Self::Completed,
            awa::JobState::Retryable => Self::Retryable,
            awa::JobState::Failed => Self::Failed,
            awa::JobState::Cancelled => Self::Cancelled,
            awa::JobState::WaitingExternal => Self::WaitingExternal,
        }
    }
}

impl From<JobState> for awa::JobState {
    fn from(value: JobState) -> Self {
        match value {
            JobState::Scheduled => Self::Scheduled,
            JobState::Available => Self::Available,
            JobState::Running => Self::Running,
            JobState::Completed => Self::Completed,
            JobState::Retryable => Self::Retryable,
            JobState::Failed => Self::Failed,
            JobState::Cancelled => Self::Cancelled,
            JobState::WaitingExternal => Self::WaitingExternal,
        }
    }
}

/// Entity for the `awa.jobs` table.
pub mod jobs {
    use super::JobState;
    use sea_orm::entity::prelude::*;

    /// Row model for `awa.jobs`.
    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "awa", table_name = "jobs")]
    pub struct Model {
        /// Stable job id.
        #[sea_orm(primary_key)]
        pub id: i64,
        /// Worker-visible job kind.
        pub kind: String,
        /// Queue name.
        pub queue: String,
        /// JSON job arguments.
        pub args: Json,
        /// Current lifecycle state.
        pub state: JobState,
        /// Scheduling priority.
        pub priority: i16,
        /// Current attempt number.
        pub attempt: i16,
        /// Maximum attempts before permanent failure.
        pub max_attempts: i16,
        /// Time when the job becomes runnable.
        pub run_at: DateTimeUtc,
        /// Last worker heartbeat time.
        pub heartbeat_at: Option<DateTimeUtc>,
        /// Worker lease deadline.
        pub deadline_at: Option<DateTimeUtc>,
        /// Time when the current attempt started.
        pub attempted_at: Option<DateTimeUtc>,
        /// Terminal state timestamp.
        pub finalized_at: Option<DateTimeUtc>,
        /// Creation timestamp.
        pub created_at: DateTimeUtc,
        /// Captured error entries.
        pub errors: Option<Vec<Json>>,
        /// User and system metadata.
        pub metadata: Json,
        /// Searchable job tags.
        pub tags: Vec<String>,
        /// Binary unique key used by uniqueness constraints.
        pub unique_key: Option<Vec<u8>>,
        /// Bitset of states covered by the unique key.
        ///
        /// This field is ignored by the SeaORM model because repository queries
        /// map the PostgreSQL bit type through `unique_states::text`.
        #[sea_orm(ignore)]
        pub unique_states: Option<u8>,
        /// Active external callback token.
        pub callback_id: Option<Uuid>,
        /// External callback timeout.
        pub callback_timeout_at: Option<DateTimeUtc>,
        /// Optional CEL filter for callback resolution.
        pub callback_filter: Option<String>,
        /// Optional CEL expression for successful callback resolution.
        pub callback_on_complete: Option<String>,
        /// Optional CEL expression for failed callback resolution.
        pub callback_on_fail: Option<String>,
        /// Optional CEL expression for callback payload transformation.
        pub callback_transform: Option<String>,
        /// Lease generation for worker ownership checks.
        pub run_lease: i64,
        /// Worker-reported progress payload.
        pub progress: Option<Json>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

/// Entity for the `awa.queue_meta` table.
pub mod queue_meta {
    use sea_orm::entity::prelude::*;

    /// Row model for `awa.queue_meta`.
    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(schema_name = "awa", table_name = "queue_meta")]
    pub struct Model {
        /// Queue name.
        #[sea_orm(primary_key, auto_increment = false)]
        pub queue: String,
        /// Whether workers should stop claiming jobs from this queue.
        pub paused: bool,
        /// Timestamp when the queue was paused.
        pub paused_at: Option<DateTimeUtc>,
        /// Optional actor that paused the queue.
        pub paused_by: Option<String>,
        /// Number of enqueue shards configured for the queue.
        pub enqueue_shards: i16,
    }

    impl ActiveModelBehavior for ActiveModel {}
}
