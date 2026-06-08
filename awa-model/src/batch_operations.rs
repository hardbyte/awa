use crate::error::{map_sqlx_error, AwaError};
use crate::job::{JobRow, JobState};
use crate::queue_storage::QueueStorage;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use sqlx::{FromRow, Row};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

const DEFAULT_CHUNK_SIZE: i64 = 100;
const PREVIEW_SAMPLE_LIMIT: i64 = 10;
const SCAN_PAGE_SIZE: i64 = 10_000;
const BATCH_RUNNER_LOCK_KEY: i64 = 0x4157_415f_4241_5443;

fn default_retention_days() -> i64 {
    std::env::var("AWA_BATCH_OP_RETENTION_DAYS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|days| *days > 0)
        .unwrap_or(90)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchOperationKind {
    SetPriority,
    MoveQueue,
}

impl BatchOperationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::SetPriority => "set_priority",
            Self::MoveQueue => "move_queue",
        }
    }
}

impl TryFrom<&str> for BatchOperationKind {
    type Error = AwaError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "set_priority" => Ok(Self::SetPriority),
            "move_queue" => Ok(Self::MoveQueue),
            _ => Err(AwaError::Validation(format!(
                "unknown batch operation kind: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchOperationState {
    Pending,
    Scanning,
    Running,
    Cancelling,
    Completed,
    Cancelled,
    Failed,
}

impl TryFrom<&str> for BatchOperationState {
    type Error = AwaError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "pending" => Ok(Self::Pending),
            "scanning" => Ok(Self::Scanning),
            "running" => Ok(Self::Running),
            "cancelling" => Ok(Self::Cancelling),
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            "failed" => Ok(Self::Failed),
            _ => Err(AwaError::Validation(format!(
                "unknown batch operation state: {value}"
            ))),
        }
    }
}

impl BatchOperationState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Scanning => "scanning",
            Self::Running => "running",
            Self::Cancelling => "cancelling",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchOperationFilter {
    pub kind: Option<String>,
    pub queue: Option<String>,
    pub ids: Option<Vec<i64>>,
    pub tag: Option<String>,
    pub state: Option<JobState>,
    pub created_at_gte: Option<DateTime<Utc>>,
    pub created_at_lt: Option<DateTime<Utc>>,
}

impl BatchOperationFilter {
    pub fn is_empty(&self) -> bool {
        self.kind.is_none()
            && self.queue.is_none()
            && self.ids.as_ref().is_none_or(Vec::is_empty)
            && self.tag.is_none()
            && self.state.is_none()
            && self.created_at_gte.is_none()
            && self.created_at_lt.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "op_kind")]
pub enum BatchOperationSpec {
    SetPriority {
        priority: i16,
    },
    MoveQueue {
        queue: String,
        priority: Option<i16>,
    },
}

impl BatchOperationSpec {
    pub fn kind(&self) -> BatchOperationKind {
        match self {
            Self::SetPriority { .. } => BatchOperationKind::SetPriority,
            Self::MoveQueue { .. } => BatchOperationKind::MoveQueue,
        }
    }

    fn validate(&self) -> Result<(), AwaError> {
        match self {
            Self::SetPriority { priority } => validate_priority(*priority),
            Self::MoveQueue { queue, priority } => {
                if queue.is_empty() || queue.len() > 200 {
                    return Err(AwaError::Validation(
                        "destination queue must be 1..=200 characters".to_string(),
                    ));
                }
                if let Some(priority) = priority {
                    validate_priority(*priority)?;
                }
                Ok(())
            }
        }
    }
}

fn validate_priority(priority: i16) -> Result<(), AwaError> {
    if (1..=4).contains(&priority) {
        Ok(())
    } else {
        Err(AwaError::Validation(
            "priority must be between 1 and 4".to_string(),
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitBatchOperation {
    pub spec: BatchOperationSpec,
    pub filter: BatchOperationFilter,
    pub submitted_by: Option<String>,
    pub allow_all: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchOperation {
    pub id: Uuid,
    pub op_kind: BatchOperationKind,
    pub filter: Json<BatchOperationFilter>,
    pub spec: Json<BatchOperationSpec>,
    pub state: BatchOperationState,
    pub submitted_by: Option<String>,
    pub submitted_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finalized_at: Option<DateTime<Utc>>,
    pub cursor: Option<Json<BatchOperationCursor>>,
    pub total_matched: Option<i64>,
    pub processed: i64,
    pub skipped: i64,
    pub errored: i64,
    pub last_error: Option<String>,
    pub runner_instance: Option<Uuid>,
    pub retention_until: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl<'r> FromRow<'r, PgRow> for BatchOperation {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let op_kind: String = row.try_get("op_kind")?;
        let state: String = row.try_get("state")?;
        Ok(Self {
            id: row.try_get("id")?,
            op_kind: BatchOperationKind::try_from(op_kind.as_str()).map_err(|err| {
                sqlx::Error::ColumnDecode {
                    index: "op_kind".to_string(),
                    source: Box::new(err),
                }
            })?,
            filter: row.try_get("filter")?,
            spec: row.try_get("spec")?,
            state: BatchOperationState::try_from(state.as_str()).map_err(|err| {
                sqlx::Error::ColumnDecode {
                    index: "state".to_string(),
                    source: Box::new(err),
                }
            })?,
            submitted_by: row.try_get("submitted_by")?,
            submitted_at: row.try_get("submitted_at")?,
            started_at: row.try_get("started_at")?,
            finalized_at: row.try_get("finalized_at")?,
            cursor: row.try_get("cursor")?,
            total_matched: row.try_get("total_matched")?,
            processed: row.try_get("processed")?,
            skipped: row.try_get("skipped")?,
            errored: row.try_get("errored")?,
            last_error: row.try_get("last_error")?,
            runner_instance: row.try_get("runner_instance")?,
            retention_until: row.try_get("retention_until")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOperationCursor {
    pub after_job_id: i64,
    pub max_job_id: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchOperationPreview {
    pub total_matched: i64,
    pub sample: Vec<JobRow>,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct BatchOperationRunOutcome {
    pub claimed: bool,
    pub finalized: bool,
    pub processed: i64,
    pub skipped: i64,
    pub errored: i64,
}

#[derive(Debug, Clone, Default)]
pub struct ListBatchOperationsFilter {
    pub state: Option<BatchOperationState>,
    pub limit: Option<i64>,
}

pub async fn submit_batch_operation(
    pool: &PgPool,
    request: SubmitBatchOperation,
) -> Result<BatchOperation, AwaError> {
    request.spec.validate()?;
    validate_filter_for_spec(&request.spec, &request.filter)?;
    if request.filter.is_empty() && !request.allow_all {
        return Err(AwaError::Validation(
            "batch operation requires a filter or allow_all=true".to_string(),
        ));
    }
    let op_kind = request.spec.kind();
    let id = Uuid::new_v4();
    sqlx::query_as::<_, BatchOperation>(
        r#"
        INSERT INTO awa.batch_operations (id, op_kind, filter, spec, state, submitted_by)
        VALUES ($1, $2, $3, $4, 'pending', $5)
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(op_kind.as_str())
    .bind(Json(request.filter))
    .bind(Json(request.spec))
    .bind(request.submitted_by)
    .fetch_one(pool)
    .await
    .map_err(map_sqlx_error)
}

pub async fn list_batch_operations(
    pool: &PgPool,
    filter: &ListBatchOperationsFilter,
) -> Result<Vec<BatchOperation>, AwaError> {
    let limit = filter.limit.unwrap_or(100).clamp(1, 1000);
    sqlx::query_as::<_, BatchOperation>(
        r#"
        SELECT *
        FROM awa.batch_operations
        WHERE ($1::text IS NULL OR state = $1)
        ORDER BY submitted_at DESC
        LIMIT $2
        "#,
    )
    .bind(filter.state.map(BatchOperationState::as_str))
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(map_sqlx_error)
}

pub async fn get_batch_operation(pool: &PgPool, id: Uuid) -> Result<BatchOperation, AwaError> {
    sqlx::query_as::<_, BatchOperation>("SELECT * FROM awa.batch_operations WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?
        .ok_or_else(|| AwaError::Validation(format!("batch operation not found: {id}")))
}

pub async fn request_batch_operation_cancellation(
    pool: &PgPool,
    id: Uuid,
) -> Result<BatchOperation, AwaError> {
    sqlx::query_as::<_, BatchOperation>(
        r#"
        UPDATE awa.batch_operations
        SET state = 'cancelling', updated_at = clock_timestamp()
        WHERE id = $1 AND state IN ('pending', 'scanning', 'running')
        RETURNING *
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?
    .ok_or_else(|| AwaError::Validation(format!("batch operation cannot be cancelled: {id}")))
}

pub async fn preview_batch_operation(
    pool: &PgPool,
    spec: BatchOperationSpec,
    filter: BatchOperationFilter,
) -> Result<BatchOperationPreview, AwaError> {
    spec.validate()?;
    validate_filter_for_spec(&spec, &filter)?;
    let effective_filter = filter.clone();
    let total_matched = count_matching_jobs(pool, &effective_filter, None, None).await?;
    let sample =
        load_matching_jobs(pool, &effective_filter, 0, i64::MAX, PREVIEW_SAMPLE_LIMIT).await?;
    Ok(BatchOperationPreview {
        total_matched,
        sample,
    })
}

pub async fn run_one_batch_operation_chunk(
    pool: &PgPool,
    runner_instance: Uuid,
    chunk_size: i64,
) -> Result<BatchOperationRunOutcome, AwaError> {
    let mut lock_conn = pool.acquire().await.map_err(map_sqlx_error)?;
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(BATCH_RUNNER_LOCK_KEY)
        .fetch_one(&mut *lock_conn)
        .await
        .map_err(map_sqlx_error)?;
    if !locked {
        return Ok(BatchOperationRunOutcome::default());
    }

    let result = run_one_batch_operation_chunk_locked(pool, runner_instance, chunk_size).await;
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(BATCH_RUNNER_LOCK_KEY)
        .execute(&mut *lock_conn)
        .await;
    result
}

async fn run_one_batch_operation_chunk_locked(
    pool: &PgPool,
    runner_instance: Uuid,
    chunk_size: i64,
) -> Result<BatchOperationRunOutcome, AwaError> {
    let chunk_size = chunk_size.max(1);
    let Some(mut operation) = claim_next_operation(pool, runner_instance).await? else {
        return Ok(BatchOperationRunOutcome::default());
    };

    if operation.state == BatchOperationState::Cancelling {
        skip_pending_items(pool, operation.id).await?;
        finalize_operation(pool, operation.id, BatchOperationState::Cancelled, None).await?;
        return Ok(BatchOperationRunOutcome {
            claimed: true,
            finalized: true,
            ..Default::default()
        });
    }

    if operation.state == BatchOperationState::Scanning {
        operation = scan_operation(pool, operation).await?;
    }

    if operation.state != BatchOperationState::Running {
        return Ok(BatchOperationRunOutcome {
            claimed: true,
            finalized: matches!(
                operation.state,
                BatchOperationState::Completed
                    | BatchOperationState::Cancelled
                    | BatchOperationState::Failed
            ),
            ..Default::default()
        });
    }

    let job_ids = claim_pending_items(pool, operation.id, chunk_size).await?;

    if job_ids.is_empty() {
        finalize_operation(pool, operation.id, BatchOperationState::Completed, None).await?;
        return Ok(BatchOperationRunOutcome {
            claimed: true,
            finalized: true,
            ..Default::default()
        });
    }

    let mut processed = 0;
    let mut skipped = 0;
    let mut errored = 0;

    for job_id in job_ids {
        match apply_item(pool, operation.id, &operation.spec.0, job_id).await {
            Ok(ItemOutcome::Processed) => processed += 1,
            Ok(ItemOutcome::Skipped) => skipped += 1,
            Ok(ItemOutcome::Errored(_)) => {
                errored += 1;
            }
            Err(_) => {
                errored += 1;
            }
        }
    }

    let pending_count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)::bigint
        FROM awa.batch_operation_items
        WHERE operation_id = $1 AND state = 'pending'
        "#,
    )
    .bind(operation.id)
    .fetch_one(pool)
    .await
    .map_err(map_sqlx_error)?;

    let state_after_chunk: String =
        sqlx::query_scalar("SELECT state FROM awa.batch_operations WHERE id = $1")
            .bind(operation.id)
            .fetch_one(pool)
            .await
            .map_err(map_sqlx_error)?;

    let finalized = if pending_count == 0 {
        let final_state = if state_after_chunk == BatchOperationState::Cancelling.as_str() {
            skip_pending_items(pool, operation.id).await?;
            BatchOperationState::Cancelled
        } else {
            BatchOperationState::Completed
        };
        finalize_operation(pool, operation.id, final_state, None).await?;
        true
    } else {
        false
    };

    Ok(BatchOperationRunOutcome {
        claimed: true,
        finalized,
        processed,
        skipped,
        errored,
    })
}

pub async fn cleanup_expired_batch_operations(pool: &PgPool, limit: i64) -> Result<u64, AwaError> {
    let result = sqlx::query(
        r#"
        DELETE FROM awa.batch_operations
        WHERE id IN (
            SELECT id
            FROM awa.batch_operations
            WHERE retention_until IS NOT NULL
              AND retention_until < clock_timestamp()
              AND state IN ('completed', 'cancelled', 'failed')
            ORDER BY retention_until ASC
            LIMIT $1
        )
        "#,
    )
    .bind(limit.max(1))
    .execute(pool)
    .await
    .map_err(map_sqlx_error)?;
    Ok(result.rows_affected())
}

pub async fn purge_batch_operations_before(
    pool: &PgPool,
    before: DateTime<Utc>,
    limit: i64,
) -> Result<u64, AwaError> {
    let result = sqlx::query(
        r#"
        DELETE FROM awa.batch_operations
        WHERE id IN (
            SELECT id
            FROM awa.batch_operations
            WHERE finalized_at IS NOT NULL
              AND finalized_at < $1
              AND state IN ('completed', 'cancelled', 'failed')
            ORDER BY finalized_at ASC
            LIMIT $2
        )
        "#,
    )
    .bind(before)
    .bind(limit.max(1))
    .execute(pool)
    .await
    .map_err(map_sqlx_error)?;
    Ok(result.rows_affected())
}

pub async fn finalize_expired_batch_operation_retention(
    pool: &PgPool,
    id: Uuid,
    state: BatchOperationState,
    retention_days: i64,
    last_error: Option<String>,
) -> Result<(), AwaError> {
    sqlx::query(
        r#"
        UPDATE awa.batch_operations
        SET state = $2,
            finalized_at = COALESCE(finalized_at, clock_timestamp()),
            retention_until = COALESCE(retention_until, clock_timestamp() + ($3 * interval '1 day')),
            last_error = COALESCE($4, last_error),
            updated_at = clock_timestamp()
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(state.as_str())
    .bind(retention_days.max(1))
    .bind(last_error)
    .execute(pool)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn claim_next_operation(
    pool: &PgPool,
    runner_instance: Uuid,
) -> Result<Option<BatchOperation>, AwaError> {
    sqlx::query_as::<_, BatchOperation>(
        r#"
        WITH target AS (
            SELECT id, state
            FROM awa.batch_operations
            WHERE state IN ('pending', 'scanning', 'running', 'cancelling')
            ORDER BY submitted_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE awa.batch_operations AS op
        SET state = CASE WHEN target.state = 'pending' THEN 'scanning' ELSE op.state END,
            started_at = COALESCE(op.started_at, clock_timestamp()),
            runner_instance = $1,
            updated_at = clock_timestamp()
        FROM target
        WHERE op.id = target.id
        RETURNING op.*
        "#,
    )
    .bind(runner_instance)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)
}

async fn scan_operation(
    pool: &PgPool,
    operation: BatchOperation,
) -> Result<BatchOperation, AwaError> {
    validate_filter_for_spec(&operation.spec.0, &operation.filter.0)?;
    let effective_filter = operation.filter.0.clone();
    let max_job_id = max_matching_job_id(pool, &effective_filter)
        .await?
        .unwrap_or(0);
    let total_matched = if max_job_id == 0 {
        0
    } else {
        count_matching_jobs(pool, &effective_filter, Some(0), Some(max_job_id)).await?
    };
    let operation_id = operation.id;
    let mut after_job_id = 0;
    while after_job_id < max_job_id {
        let ids = load_matching_job_ids(
            pool,
            &effective_filter,
            after_job_id,
            max_job_id,
            SCAN_PAGE_SIZE,
        )
        .await?;
        let Some(last_job_id) = ids.last().copied() else {
            break;
        };
        after_job_id = last_job_id;

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        insert_operation_items(&mut tx, operation_id, &ids).await?;
        tx.commit().await.map_err(map_sqlx_error)?;

        let state: String =
            sqlx::query_scalar("SELECT state FROM awa.batch_operations WHERE id = $1")
                .bind(operation_id)
                .fetch_one(pool)
                .await
                .map_err(map_sqlx_error)?;
        if state == BatchOperationState::Cancelling.as_str() {
            return get_batch_operation(pool, operation_id).await;
        }
    }

    let operation = sqlx::query_as::<_, BatchOperation>(
        r#"
        UPDATE awa.batch_operations
        SET state = 'running',
            total_matched = $2,
            cursor = $3,
            updated_at = clock_timestamp()
        WHERE id = $1 AND state = 'scanning'
        RETURNING *
        "#,
    )
    .bind(operation_id)
    .bind(total_matched)
    .bind(Json(BatchOperationCursor {
        after_job_id: 0,
        max_job_id,
    }))
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;

    match operation {
        Some(operation) => Ok(operation),
        None => get_batch_operation(pool, operation_id).await,
    }
}

async fn finalize_operation(
    pool: &PgPool,
    id: Uuid,
    state: BatchOperationState,
    last_error: Option<String>,
) -> Result<(), AwaError> {
    sqlx::query(
        r#"
        UPDATE awa.batch_operations
        SET state = $2,
            finalized_at = COALESCE(finalized_at, clock_timestamp()),
            retention_until = COALESCE(retention_until, clock_timestamp() + ($3 * interval '1 day')),
            last_error = COALESCE($4, last_error),
            updated_at = clock_timestamp()
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(state.as_str())
    .bind(default_retention_days())
    .bind(last_error)
    .execute(pool)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn insert_operation_items(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: Uuid,
    ids: &[i64],
) -> Result<(), AwaError> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        r#"
        INSERT INTO awa.batch_operation_items (operation_id, job_id)
        SELECT $1, job_id
        FROM unnest($2::bigint[]) AS job_id
        ON CONFLICT (operation_id, job_id) DO NOTHING
        "#,
    )
    .bind(operation_id)
    .bind(ids)
    .execute(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn claim_pending_items(
    pool: &PgPool,
    operation_id: Uuid,
    limit: i64,
) -> Result<Vec<i64>, AwaError> {
    sqlx::query_scalar(
        r#"
        SELECT job_id
        FROM awa.batch_operation_items
        WHERE operation_id = $1 AND state = 'pending'
        ORDER BY job_id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(operation_id)
    .bind(limit.max(1))
    .fetch_all(pool)
    .await
    .map_err(map_sqlx_error)
}

async fn skip_pending_items(pool: &PgPool, operation_id: Uuid) -> Result<i64, AwaError> {
    let skipped: i64 = sqlx::query_scalar(
        r#"
        WITH skipped AS (
            UPDATE awa.batch_operation_items
            SET state = 'skipped', processed_at = clock_timestamp()
            WHERE operation_id = $1 AND state = 'pending'
            RETURNING job_id
        ), count_skipped AS (
            SELECT count(*)::bigint AS count FROM skipped
        )
        UPDATE awa.batch_operations
        SET skipped = skipped + count_skipped.count,
            updated_at = clock_timestamp()
        FROM count_skipped
        WHERE id = $1
        RETURNING count_skipped.count
        "#,
    )
    .bind(operation_id)
    .fetch_one(pool)
    .await
    .map_err(map_sqlx_error)?;
    Ok(skipped)
}

fn validate_filter_for_spec(
    spec: &BatchOperationSpec,
    filter: &BatchOperationFilter,
) -> Result<(), AwaError> {
    match spec {
        BatchOperationSpec::SetPriority { .. } | BatchOperationSpec::MoveQueue { .. } => {
            if let Some(state) = filter.state {
                if !matches!(state, JobState::Scheduled | JobState::Available) {
                    return Err(AwaError::Validation(format!(
                        "{} only supports available or scheduled state filters",
                        spec.kind().as_str()
                    )));
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
enum ItemOutcome {
    Processed,
    Skipped,
    Errored(String),
}

async fn apply_item(
    pool: &PgPool,
    operation_id: Uuid,
    spec: &BatchOperationSpec,
    job_id: i64,
) -> Result<ItemOutcome, AwaError> {
    let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
    let outcome = match apply_to_job_tx(&mut tx, spec, job_id).await {
        Ok(true) => ItemOutcome::Processed,
        Ok(false) => ItemOutcome::Skipped,
        Err(err) => {
            tx.rollback().await.map_err(map_sqlx_error)?;
            let outcome = ItemOutcome::Errored(format!("job_id={job_id}: {err}"));
            let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
            record_item_outcome_tx(&mut tx, operation_id, job_id, &outcome).await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(outcome);
        }
    };
    record_item_outcome_tx(&mut tx, operation_id, job_id, &outcome).await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(outcome)
}

async fn record_item_outcome_tx(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: Uuid,
    job_id: i64,
    outcome: &ItemOutcome,
) -> Result<(), AwaError> {
    let (state, error, processed_delta, skipped_delta, errored_delta) = match &outcome {
        ItemOutcome::Processed => ("processed", None, 1_i64, 0_i64, 0_i64),
        ItemOutcome::Skipped => ("skipped", None, 0_i64, 1_i64, 0_i64),
        ItemOutcome::Errored(error) => ("errored", Some(error.as_str()), 0_i64, 0_i64, 1_i64),
    };
    sqlx::query(
        r#"
        UPDATE awa.batch_operation_items
        SET state = $3,
            error = $4,
            processed_at = clock_timestamp()
        WHERE operation_id = $1 AND job_id = $2 AND state = 'pending'
        "#,
    )
    .bind(operation_id)
    .bind(job_id)
    .bind(state)
    .bind(error)
    .execute(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;
    sqlx::query(
        r#"
        UPDATE awa.batch_operations
        SET processed = processed + $2,
            skipped = skipped + $3,
            errored = errored + $4,
            last_error = COALESCE($5, last_error),
            updated_at = clock_timestamp()
        WHERE id = $1
        "#,
    )
    .bind(operation_id)
    .bind(processed_delta)
    .bind(skipped_delta)
    .bind(errored_delta)
    .bind(error)
    .execute(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn apply_to_job_tx(
    tx: &mut Transaction<'_, Postgres>,
    spec: &BatchOperationSpec,
    job_id: i64,
) -> Result<bool, AwaError> {
    if let Some(store) = QueueStorage::active_schema_in_tx(tx)
        .await?
        .map(QueueStorage::from_existing_schema)
        .transpose()?
    {
        return match spec {
            BatchOperationSpec::SetPriority { priority } => {
                store.set_priority_tx(tx, job_id, *priority).await
            }
            BatchOperationSpec::MoveQueue { queue, priority } => {
                store.move_queue_tx(tx, job_id, queue, *priority).await
            }
        };
    }

    match spec {
        BatchOperationSpec::SetPriority { priority } => {
            canonical_set_priority_tx(tx, job_id, *priority).await
        }
        BatchOperationSpec::MoveQueue { queue, priority } => {
            canonical_move_queue_tx(tx, job_id, queue, *priority).await
        }
    }
}

async fn canonical_set_priority_tx(
    tx: &mut Transaction<'_, Postgres>,
    job_id: i64,
    priority: i16,
) -> Result<bool, AwaError> {
    let result = sqlx::query(
        r#"
        UPDATE awa.jobs
        SET priority = $2,
            metadata = CASE
                WHEN metadata ? '_awa_original_priority' THEN metadata
                ELSE jsonb_set(metadata, '{_awa_original_priority}', to_jsonb(priority), true)
            END
        WHERE id = $1 AND state IN ('available', 'scheduled') AND priority <> $2
        "#,
    )
    .bind(job_id)
    .bind(priority)
    .execute(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;
    Ok(result.rows_affected() > 0)
}

async fn canonical_move_queue_tx(
    tx: &mut Transaction<'_, Postgres>,
    job_id: i64,
    queue: &str,
    priority: Option<i16>,
) -> Result<bool, AwaError> {
    let result = sqlx::query(
        r#"
        WITH target AS (
            SELECT id, queue, priority, metadata
            FROM awa.jobs
            WHERE id = $1 AND state IN ('available', 'scheduled')
              AND (queue <> $2 OR ($3::smallint IS NOT NULL AND priority <> $3))
            FOR UPDATE
        ), stamped AS (
            SELECT
                id,
                CASE
                    WHEN $3::smallint IS NULL OR metadata ? '_awa_original_priority' THEN metadata
                    ELSE jsonb_set(metadata, '{_awa_original_priority}', to_jsonb(priority), true)
                END AS with_priority,
                queue AS original_queue
            FROM target
        )
        UPDATE awa.jobs AS jobs
        SET queue = $2,
            priority = COALESCE($3, jobs.priority),
            metadata = CASE
                WHEN stamped.with_priority ? '_awa_original_queue' THEN stamped.with_priority
                ELSE jsonb_set(stamped.with_priority, '{_awa_original_queue}', to_jsonb(stamped.original_queue), true)
            END
        FROM stamped
        WHERE jobs.id = stamped.id
        "#,
    )
    .bind(job_id)
    .bind(queue)
    .bind(priority)
    .execute(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;
    Ok(result.rows_affected() > 0)
}

async fn count_matching_jobs(
    pool: &PgPool,
    filter: &BatchOperationFilter,
    after_job_id: Option<i64>,
    max_job_id: Option<i64>,
) -> Result<i64, AwaError> {
    if QueueStorage::active_schema(pool).await?.is_some() {
        count_matching_queue_storage_jobs(pool, filter, after_job_id, max_job_id).await
    } else {
        count_matching_canonical_jobs(pool, filter, after_job_id, max_job_id).await
    }
}

async fn max_matching_job_id(
    pool: &PgPool,
    filter: &BatchOperationFilter,
) -> Result<Option<i64>, AwaError> {
    if QueueStorage::active_schema(pool).await?.is_some() {
        max_matching_queue_storage_job_id(pool, filter).await
    } else {
        max_matching_canonical_job_id(pool, filter).await
    }
}

async fn max_matching_canonical_job_id(
    pool: &PgPool,
    filter: &BatchOperationFilter,
) -> Result<Option<i64>, AwaError> {
    sqlx::query_scalar(
        r#"
        SELECT max(id)
        FROM awa.jobs
        WHERE state IN ('available', 'scheduled')
          AND ($1::awa.job_state IS NULL OR state = $1)
          AND ($2::text IS NULL OR kind = $2)
          AND ($3::text IS NULL OR queue = $3)
          AND ($4::text IS NULL OR tags @> ARRAY[$4]::text[])
          AND ($5::bigint[] IS NULL OR id = ANY($5))
          AND ($6::timestamptz IS NULL OR created_at >= $6)
          AND ($7::timestamptz IS NULL OR created_at < $7)
        "#,
    )
    .bind(filter.state)
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .bind(filter.ids.as_deref())
    .bind(filter.created_at_gte)
    .bind(filter.created_at_lt)
    .fetch_one(pool)
    .await
    .map_err(map_sqlx_error)
}

async fn max_matching_queue_storage_job_id(
    pool: &PgPool,
    filter: &BatchOperationFilter,
) -> Result<Option<i64>, AwaError> {
    let schema = QueueStorage::active_schema(pool)
        .await?
        .ok_or_else(|| AwaError::Validation("queue storage is not active".to_string()))?;
    sqlx::query_scalar(&format!(
        r#"
        WITH candidates AS (
            SELECT
                ready.job_id AS id,
                ready.kind,
                ready.queue,
                'available'::awa.job_state AS state,
                ready.created_at,
                COALESCE(ready.payload, '{{}}'::jsonb) AS payload
            FROM {schema}.ready_entries AS ready
            JOIN {schema}.queue_claim_heads AS claims
              ON claims.queue = ready.queue
             AND claims.priority = ready.priority
             AND claims.enqueue_shard = ready.enqueue_shard
            WHERE ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.ready_tombstones AS tomb
                  WHERE tomb.queue = ready.queue
                    AND tomb.priority = ready.priority
                    AND tomb.enqueue_shard = ready.enqueue_shard
                    AND tomb.lane_seq = ready.lane_seq
                    AND tomb.ready_slot = ready.ready_slot
                    AND tomb.ready_generation = ready.ready_generation
              )
            UNION ALL
            SELECT
                deferred.job_id AS id,
                deferred.kind,
                deferred.queue,
                deferred.state,
                deferred.created_at,
                COALESCE(deferred.payload, '{{}}'::jsonb) AS payload
            FROM {schema}.deferred_jobs AS deferred
            WHERE deferred.state = 'scheduled'
        )
        SELECT max(id)
        FROM candidates
        WHERE ($1::awa.job_state IS NULL OR state = $1)
          AND ($2::text IS NULL OR kind = $2)
          AND ($3::text IS NULL OR queue = $3)
          AND ($4::text IS NULL OR COALESCE(ARRAY(SELECT jsonb_array_elements_text(payload->'tags')), ARRAY[]::text[]) @> ARRAY[$4]::text[])
          AND ($5::bigint[] IS NULL OR id = ANY($5))
          AND ($6::timestamptz IS NULL OR created_at >= $6)
          AND ($7::timestamptz IS NULL OR created_at < $7)
        "#
    ))
    .bind(filter.state)
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .bind(filter.ids.as_deref())
    .bind(filter.created_at_gte)
    .bind(filter.created_at_lt)
    .fetch_one(pool)
    .await
    .map_err(map_sqlx_error)
}

async fn load_matching_jobs(
    pool: &PgPool,
    filter: &BatchOperationFilter,
    after_job_id: i64,
    max_job_id: i64,
    limit: i64,
) -> Result<Vec<JobRow>, AwaError> {
    if let Some(store) = QueueStorage::active_schema(pool)
        .await?
        .map(QueueStorage::from_existing_schema)
        .transpose()?
    {
        load_matching_queue_storage_jobs(&store, pool, filter, after_job_id, max_job_id, limit)
            .await
    } else {
        load_matching_canonical_jobs(pool, filter, after_job_id, max_job_id, limit).await
    }
}

async fn load_matching_job_ids(
    pool: &PgPool,
    filter: &BatchOperationFilter,
    after_job_id: i64,
    max_job_id: i64,
    limit: i64,
) -> Result<Vec<i64>, AwaError> {
    if QueueStorage::active_schema(pool).await?.is_some() {
        load_matching_queue_storage_job_ids(pool, filter, after_job_id, max_job_id, limit).await
    } else {
        load_matching_canonical_job_ids(pool, filter, after_job_id, max_job_id, limit).await
    }
}

async fn load_matching_canonical_job_ids(
    pool: &PgPool,
    filter: &BatchOperationFilter,
    after_job_id: i64,
    max_job_id: i64,
    limit: i64,
) -> Result<Vec<i64>, AwaError> {
    sqlx::query_scalar(
        r#"
        SELECT id
        FROM awa.jobs
        WHERE state IN ('available', 'scheduled')
          AND ($1::awa.job_state IS NULL OR state = $1)
          AND ($2::text IS NULL OR kind = $2)
          AND ($3::text IS NULL OR queue = $3)
          AND ($4::text IS NULL OR tags @> ARRAY[$4]::text[])
          AND ($5::bigint[] IS NULL OR id = ANY($5))
          AND ($6::timestamptz IS NULL OR created_at >= $6)
          AND ($7::timestamptz IS NULL OR created_at < $7)
          AND id > $8
          AND id <= $9
        ORDER BY id ASC
        LIMIT $10
        "#,
    )
    .bind(filter.state)
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .bind(filter.ids.as_deref())
    .bind(filter.created_at_gte)
    .bind(filter.created_at_lt)
    .bind(after_job_id)
    .bind(max_job_id)
    .bind(limit.max(1))
    .fetch_all(pool)
    .await
    .map_err(map_sqlx_error)
}

async fn load_matching_queue_storage_job_ids(
    pool: &PgPool,
    filter: &BatchOperationFilter,
    after_job_id: i64,
    max_job_id: i64,
    limit: i64,
) -> Result<Vec<i64>, AwaError> {
    let schema = QueueStorage::active_schema(pool)
        .await?
        .ok_or_else(|| AwaError::Validation("queue storage is not active".to_string()))?;
    sqlx::query_scalar(&format!(
        r#"
        WITH candidates AS (
            SELECT
                ready.job_id AS id,
                ready.kind,
                ready.queue,
                'available'::awa.job_state AS state,
                ready.created_at,
                COALESCE(ready.payload, '{{}}'::jsonb) AS payload
            FROM {schema}.ready_entries AS ready
            JOIN {schema}.queue_claim_heads AS claims
              ON claims.queue = ready.queue
             AND claims.priority = ready.priority
             AND claims.enqueue_shard = ready.enqueue_shard
            WHERE ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.ready_tombstones AS tomb
                  WHERE tomb.queue = ready.queue
                    AND tomb.priority = ready.priority
                    AND tomb.enqueue_shard = ready.enqueue_shard
                    AND tomb.lane_seq = ready.lane_seq
                    AND tomb.ready_slot = ready.ready_slot
                    AND tomb.ready_generation = ready.ready_generation
              )
            UNION ALL
            SELECT
                deferred.job_id AS id,
                deferred.kind,
                deferred.queue,
                deferred.state,
                deferred.created_at,
                COALESCE(deferred.payload, '{{}}'::jsonb) AS payload
            FROM {schema}.deferred_jobs AS deferred
            WHERE deferred.state = 'scheduled'
        )
        SELECT id
        FROM candidates
        WHERE ($1::awa.job_state IS NULL OR state = $1)
          AND ($2::text IS NULL OR kind = $2)
          AND ($3::text IS NULL OR queue = $3)
          AND ($4::text IS NULL OR COALESCE(ARRAY(SELECT jsonb_array_elements_text(payload->'tags')), ARRAY[]::text[]) @> ARRAY[$4]::text[])
          AND ($5::bigint[] IS NULL OR id = ANY($5))
          AND ($6::timestamptz IS NULL OR created_at >= $6)
          AND ($7::timestamptz IS NULL OR created_at < $7)
          AND id > $8
          AND id <= $9
        ORDER BY id ASC
        LIMIT $10
        "#
    ))
    .bind(filter.state)
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .bind(filter.ids.as_deref())
    .bind(filter.created_at_gte)
    .bind(filter.created_at_lt)
    .bind(after_job_id)
    .bind(max_job_id)
    .bind(limit.max(1))
    .fetch_all(pool)
    .await
    .map_err(map_sqlx_error)
}

async fn count_matching_canonical_jobs(
    pool: &PgPool,
    filter: &BatchOperationFilter,
    after_job_id: Option<i64>,
    max_job_id: Option<i64>,
) -> Result<i64, AwaError> {
    sqlx::query_scalar(
        r#"
        SELECT count(*)::bigint
        FROM awa.jobs
        WHERE state IN ('available', 'scheduled')
          AND ($1::awa.job_state IS NULL OR state = $1)
          AND ($2::text IS NULL OR kind = $2)
          AND ($3::text IS NULL OR queue = $3)
          AND ($4::text IS NULL OR tags @> ARRAY[$4]::text[])
          AND ($5::bigint[] IS NULL OR id = ANY($5))
          AND ($6::timestamptz IS NULL OR created_at >= $6)
          AND ($7::timestamptz IS NULL OR created_at < $7)
          AND ($8::bigint IS NULL OR id > $8)
          AND ($9::bigint IS NULL OR id <= $9)
        "#,
    )
    .bind(filter.state)
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .bind(filter.ids.as_deref())
    .bind(filter.created_at_gte)
    .bind(filter.created_at_lt)
    .bind(after_job_id)
    .bind(max_job_id)
    .fetch_one(pool)
    .await
    .map_err(map_sqlx_error)
}

async fn load_matching_canonical_jobs(
    pool: &PgPool,
    filter: &BatchOperationFilter,
    after_job_id: i64,
    max_job_id: i64,
    limit: i64,
) -> Result<Vec<JobRow>, AwaError> {
    sqlx::query_as::<_, JobRow>(
        r#"
        SELECT *
        FROM awa.jobs
        WHERE state IN ('available', 'scheduled')
          AND ($1::awa.job_state IS NULL OR state = $1)
          AND ($2::text IS NULL OR kind = $2)
          AND ($3::text IS NULL OR queue = $3)
          AND ($4::text IS NULL OR tags @> ARRAY[$4]::text[])
          AND ($5::bigint[] IS NULL OR id = ANY($5))
          AND ($6::timestamptz IS NULL OR created_at >= $6)
          AND ($7::timestamptz IS NULL OR created_at < $7)
          AND id > $8
          AND id <= $9
        ORDER BY id ASC
        LIMIT $10
        "#,
    )
    .bind(filter.state)
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .bind(filter.ids.as_deref())
    .bind(filter.created_at_gte)
    .bind(filter.created_at_lt)
    .bind(after_job_id)
    .bind(max_job_id)
    .bind(limit.max(1))
    .fetch_all(pool)
    .await
    .map_err(map_sqlx_error)
}

async fn count_matching_queue_storage_jobs(
    pool: &PgPool,
    filter: &BatchOperationFilter,
    after_job_id: Option<i64>,
    max_job_id: Option<i64>,
) -> Result<i64, AwaError> {
    let schema = QueueStorage::active_schema(pool)
        .await?
        .ok_or_else(|| AwaError::Validation("queue storage is not active".to_string()))?;
    sqlx::query_scalar(&format!(
        r#"
        WITH candidates AS (
            SELECT
                ready.job_id AS id,
                ready.kind,
                ready.queue,
                'available'::awa.job_state AS state,
                ready.created_at,
                COALESCE(ready.payload, '{{}}'::jsonb) AS payload
            FROM {schema}.ready_entries AS ready
            JOIN {schema}.queue_claim_heads AS claims
              ON claims.queue = ready.queue
             AND claims.priority = ready.priority
             AND claims.enqueue_shard = ready.enqueue_shard
            WHERE ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.ready_tombstones AS tomb
                  WHERE tomb.queue = ready.queue
                    AND tomb.priority = ready.priority
                    AND tomb.enqueue_shard = ready.enqueue_shard
                    AND tomb.lane_seq = ready.lane_seq
                    AND tomb.ready_slot = ready.ready_slot
                    AND tomb.ready_generation = ready.ready_generation
              )
            UNION ALL
            SELECT
                deferred.job_id AS id,
                deferred.kind,
                deferred.queue,
                deferred.state,
                deferred.created_at,
                COALESCE(deferred.payload, '{{}}'::jsonb) AS payload
            FROM {schema}.deferred_jobs AS deferred
            WHERE deferred.state = 'scheduled'
        )
        SELECT count(*)::bigint
        FROM candidates
        WHERE ($1::awa.job_state IS NULL OR state = $1)
          AND ($2::text IS NULL OR kind = $2)
          AND ($3::text IS NULL OR queue = $3)
          AND ($4::text IS NULL OR COALESCE(ARRAY(SELECT jsonb_array_elements_text(payload->'tags')), ARRAY[]::text[]) @> ARRAY[$4]::text[])
          AND ($5::bigint[] IS NULL OR id = ANY($5))
          AND ($6::timestamptz IS NULL OR created_at >= $6)
          AND ($7::timestamptz IS NULL OR created_at < $7)
          AND ($8::bigint IS NULL OR id > $8)
          AND ($9::bigint IS NULL OR id <= $9)
        "#
    ))
    .bind(filter.state)
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .bind(filter.ids.as_deref())
    .bind(filter.created_at_gte)
    .bind(filter.created_at_lt)
    .bind(after_job_id)
    .bind(max_job_id)
    .fetch_one(pool)
    .await
    .map_err(map_sqlx_error)
}

async fn load_matching_queue_storage_jobs(
    _store: &QueueStorage,
    pool: &PgPool,
    filter: &BatchOperationFilter,
    after_job_id: i64,
    max_job_id: i64,
    limit: i64,
) -> Result<Vec<JobRow>, AwaError> {
    let schema = _store.schema();
    let ids: Vec<i64> = sqlx::query_scalar(&format!(
        r#"
        WITH candidates AS (
            SELECT
                ready.job_id AS id,
                ready.kind,
                ready.queue,
                'available'::awa.job_state AS state,
                ready.created_at,
                COALESCE(ready.payload, '{{}}'::jsonb) AS payload
            FROM {schema}.ready_entries AS ready
            JOIN {schema}.queue_claim_heads AS claims
              ON claims.queue = ready.queue
             AND claims.priority = ready.priority
             AND claims.enqueue_shard = ready.enqueue_shard
            WHERE ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.ready_tombstones AS tomb
                  WHERE tomb.queue = ready.queue
                    AND tomb.priority = ready.priority
                    AND tomb.enqueue_shard = ready.enqueue_shard
                    AND tomb.lane_seq = ready.lane_seq
                    AND tomb.ready_slot = ready.ready_slot
                    AND tomb.ready_generation = ready.ready_generation
              )
            UNION ALL
            SELECT
                deferred.job_id AS id,
                deferred.kind,
                deferred.queue,
                deferred.state,
                deferred.created_at,
                COALESCE(deferred.payload, '{{}}'::jsonb) AS payload
            FROM {schema}.deferred_jobs AS deferred
            WHERE deferred.state = 'scheduled'
        )
        SELECT id
        FROM candidates
        WHERE ($1::awa.job_state IS NULL OR state = $1)
          AND ($2::text IS NULL OR kind = $2)
          AND ($3::text IS NULL OR queue = $3)
          AND ($4::text IS NULL OR COALESCE(ARRAY(SELECT jsonb_array_elements_text(payload->'tags')), ARRAY[]::text[]) @> ARRAY[$4]::text[])
          AND ($5::bigint[] IS NULL OR id = ANY($5))
          AND ($6::timestamptz IS NULL OR created_at >= $6)
          AND ($7::timestamptz IS NULL OR created_at < $7)
          AND id > $8
          AND id <= $9
        ORDER BY id ASC
        LIMIT $10
        "#
    ))
    .bind(filter.state)
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(&filter.tag)
    .bind(filter.ids.as_deref())
    .bind(filter.created_at_gte)
    .bind(filter.created_at_lt)
    .bind(after_job_id)
    .bind(max_job_id)
    .bind(limit.max(1))
    .fetch_all(pool)
    .await
    .map_err(map_sqlx_error)?;

    let mut jobs = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(job) = _store.load_job(pool, id).await? {
            if matches_batch_filter(&job, filter, after_job_id, max_job_id) {
                jobs.push(job);
            }
        }
    }
    jobs.sort_by_key(|job| job.id);
    Ok(jobs)
}

fn matches_batch_filter(
    job: &JobRow,
    filter: &BatchOperationFilter,
    after_job_id: i64,
    max_job_id: i64,
) -> bool {
    if job.id <= after_job_id || job.id > max_job_id {
        return false;
    }
    if !matches!(job.state, JobState::Available | JobState::Scheduled) {
        return false;
    }
    if filter.state.is_some_and(|state| job.state != state) {
        return false;
    }
    if filter.kind.as_ref().is_some_and(|kind| &job.kind != kind) {
        return false;
    }
    if filter
        .queue
        .as_ref()
        .is_some_and(|queue| &job.queue != queue)
    {
        return false;
    }
    if filter
        .tag
        .as_ref()
        .is_some_and(|tag| !job.tags.iter().any(|job_tag| job_tag == tag))
    {
        return false;
    }
    if let Some(ids) = &filter.ids {
        if !ids.contains(&job.id) {
            return false;
        }
    }
    if filter
        .created_at_gte
        .is_some_and(|created_at_gte| job.created_at < created_at_gte)
    {
        return false;
    }
    if filter
        .created_at_lt
        .is_some_and(|created_at_lt| job.created_at >= created_at_lt)
    {
        return false;
    }
    true
}

pub async fn run_one_default_chunk(
    pool: &PgPool,
    runner_instance: Uuid,
) -> Result<BatchOperationRunOutcome, AwaError> {
    run_one_batch_operation_chunk(pool, runner_instance, DEFAULT_CHUNK_SIZE).await
}
