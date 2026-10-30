use crate::error::{map_sqlx_error, AwaError};
use crate::filter::JobFilter;
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

/// ADR-030's batch-operation filter, now the shared [`JobFilter`] (ADR-049).
pub type BatchOperationFilter = JobFilter;

/// States a batch operation can act on: both kinds move ready work.
const ELIGIBLE_STATES: [JobState; 2] = [JobState::Available, JobState::Scheduled];

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
    request.filter.validate()?;
    validate_filter_for_spec(&request.spec, &request.filter)?;
    // An empty canonical database lists no jobs, but nothing could run a
    // batch operation there.
    if crate::job_query::active_store(pool).await?.is_none() {
        return Err(AwaError::Validation(
            crate::job_query::CANONICAL_REFUSAL.to_string(),
        ));
    }
    if request.filter.uses_extended_fields() {
        crate::filter::require_fleet_for_extended_filters(pool).await?;
    }
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
    filter.validate()?;
    let total_matched = crate::job_query::count(pool, &filter, &ELIGIBLE_STATES, None)
        .await?
        .value();
    let sample = crate::job_query::find(
        pool,
        &filter,
        &ELIGIBLE_STATES,
        crate::job_query::Window::OldestFirst {
            after_id: 0,
            max_id: i64::MAX,
            limit: PREVIEW_SAMPLE_LIMIT,
        },
    )
    .await?;
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
    let filter = operation.filter.0.clone();
    let max_job_id = crate::job_query::max_id(pool, &filter, &ELIGIBLE_STATES)
        .await?
        .unwrap_or(0);
    // The candidates frozen into the item table. One whose job has moved on
    // by the time the runner reaches it is counted as skipped there.
    let mut total_matched: i64 = 0;
    let operation_id = operation.id;
    let mut after_job_id = 0;
    while after_job_id < max_job_id {
        let ids = crate::job_query::scan_ids(
            pool,
            &filter,
            &ELIGIBLE_STATES,
            after_job_id,
            max_job_id,
            SCAN_PAGE_SIZE,
        )
        .await?;
        let Some(last_job_id) = ids.last().copied() else {
            break;
        };
        after_job_id = last_job_id;
        total_matched += ids.len() as i64;

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
            if filter
                .states
                .iter()
                .any(|state| !ELIGIBLE_STATES.contains(state))
                || filter.dead_lettered == Some(true)
            {
                return Err(AwaError::Validation(format!(
                    "{} only supports available or scheduled state filters",
                    spec.kind().as_str()
                )));
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
    Err(AwaError::Validation(
        "batch operations require an active queue_storage backend; \
         the canonical engine is deprecated (ADR-037)"
            .to_string(),
    ))
}

pub async fn run_one_default_chunk(
    pool: &PgPool,
    runner_instance: Uuid,
) -> Result<BatchOperationRunOutcome, AwaError> {
    run_one_batch_operation_chunk(pool, runner_instance, DEFAULT_CHUNK_SIZE).await
}
