use crate::error::AwaError;
use crate::job::{JobRow, JobState};
use sqlx::PgExecutor;
use sqlx::PgPool;
use uuid::Uuid;

/// Retry a single failed, cancelled, or waiting_external job.
pub async fn retry<'e, E>(executor: E, job_id: i64) -> Result<Option<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'available', attempt = 0, run_at = now(),
            finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL,
            callback_id = NULL, callback_timeout_at = NULL,
            callback_filter = NULL, callback_on_complete = NULL,
            callback_on_fail = NULL, callback_transform = NULL
        WHERE id = $1 AND state IN ('failed', 'cancelled', 'waiting_external')
        RETURNING *
        "#,
    )
    .bind(job_id)
    .fetch_optional(executor)
    .await?
    .ok_or(AwaError::JobNotFound { id: job_id })
    .map(Some)
}

/// Cancel a single non-terminal job.
pub async fn cancel<'e, E>(executor: E, job_id: i64) -> Result<Option<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'cancelled', finalized_at = now(),
            callback_id = NULL, callback_timeout_at = NULL,
            callback_filter = NULL, callback_on_complete = NULL,
            callback_on_fail = NULL, callback_transform = NULL
        WHERE id = $1 AND state NOT IN ('completed', 'failed', 'cancelled')
        RETURNING *
        "#,
    )
    .bind(job_id)
    .fetch_optional(executor)
    .await?
    .ok_or(AwaError::JobNotFound { id: job_id })
    .map(Some)
}

/// Retry all failed jobs of a given kind.
pub async fn retry_failed_by_kind<'e, E>(executor: E, kind: &str) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'available', attempt = 0, run_at = now(),
            finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL
        WHERE kind = $1 AND state = 'failed'
        RETURNING *
        "#,
    )
    .bind(kind)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// Retry all failed jobs in a given queue.
pub async fn retry_failed_by_queue<'e, E>(executor: E, queue: &str) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'available', attempt = 0, run_at = now(),
            finalized_at = NULL, heartbeat_at = NULL, deadline_at = NULL
        WHERE queue = $1 AND state = 'failed'
        RETURNING *
        "#,
    )
    .bind(queue)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// Discard (delete) all failed jobs of a given kind.
pub async fn discard_failed<'e, E>(executor: E, kind: &str) -> Result<u64, AwaError>
where
    E: PgExecutor<'e>,
{
    let result = sqlx::query("DELETE FROM awa.jobs WHERE kind = $1 AND state = 'failed'")
        .bind(kind)
        .execute(executor)
        .await?;

    Ok(result.rows_affected())
}

/// Pause a queue. Affects all workers immediately.
pub async fn pause_queue<'e, E>(
    executor: E,
    queue: &str,
    paused_by: Option<&str>,
) -> Result<(), AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query(
        r#"
        INSERT INTO awa.queue_meta (queue, paused, paused_at, paused_by)
        VALUES ($1, TRUE, now(), $2)
        ON CONFLICT (queue) DO UPDATE SET paused = TRUE, paused_at = now(), paused_by = $2
        "#,
    )
    .bind(queue)
    .bind(paused_by)
    .execute(executor)
    .await?;

    Ok(())
}

/// Resume a paused queue.
pub async fn resume_queue<'e, E>(executor: E, queue: &str) -> Result<(), AwaError>
where
    E: PgExecutor<'e>,
{
    sqlx::query("UPDATE awa.queue_meta SET paused = FALSE WHERE queue = $1")
        .bind(queue)
        .execute(executor)
        .await?;

    Ok(())
}

/// Drain a queue: cancel all non-running, non-terminal jobs.
pub async fn drain_queue<'e, E>(executor: E, queue: &str) -> Result<u64, AwaError>
where
    E: PgExecutor<'e>,
{
    let result = sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'cancelled', finalized_at = now(),
            callback_id = NULL, callback_timeout_at = NULL,
            callback_filter = NULL, callback_on_complete = NULL,
            callback_on_fail = NULL, callback_transform = NULL
        WHERE queue = $1 AND state IN ('available', 'scheduled', 'retryable', 'waiting_external')
        "#,
    )
    .bind(queue)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

/// Queue statistics.
#[derive(Debug, Clone)]
pub struct QueueStats {
    pub queue: String,
    pub available: i64,
    pub running: i64,
    pub failed: i64,
    pub waiting_external: i64,
    pub completed_last_hour: i64,
    pub lag_seconds: Option<f64>,
}

/// Get statistics for all queues.
pub async fn queue_stats<'e, E>(executor: E) -> Result<Vec<QueueStats>, AwaError>
where
    E: PgExecutor<'e>,
{
    let rows = sqlx::query_as::<_, (String, i64, i64, i64, i64, i64, Option<f64>)>(
        r#"
        SELECT
            queue,
            count(*) FILTER (WHERE state = 'available') AS available,
            count(*) FILTER (WHERE state = 'running') AS running,
            count(*) FILTER (WHERE state = 'failed') AS failed,
            count(*) FILTER (WHERE state = 'waiting_external') AS waiting_external,
            count(*) FILTER (WHERE state = 'completed'
                AND finalized_at > now() - interval '1 hour') AS completed_last_hour,
            EXTRACT(EPOCH FROM (now() - min(run_at) FILTER (WHERE state = 'available')))::float8 AS lag_seconds
        FROM awa.jobs
        GROUP BY queue
        "#,
    )
    .fetch_all(executor)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(
                queue,
                available,
                running,
                failed,
                waiting_external,
                completed_last_hour,
                lag_seconds,
            )| QueueStats {
                queue,
                available,
                running,
                failed,
                waiting_external,
                completed_last_hour,
                lag_seconds,
            },
        )
        .collect())
}

/// List jobs with optional filters.
#[derive(Debug, Clone, Default)]
pub struct ListJobsFilter {
    pub state: Option<JobState>,
    pub kind: Option<String>,
    pub queue: Option<String>,
    pub limit: Option<i64>,
}

/// List jobs matching the given filter.
pub async fn list_jobs<'e, E>(executor: E, filter: &ListJobsFilter) -> Result<Vec<JobRow>, AwaError>
where
    E: PgExecutor<'e>,
{
    let limit = filter.limit.unwrap_or(100);

    let rows = sqlx::query_as::<_, JobRow>(
        r#"
        SELECT * FROM awa.jobs
        WHERE ($1::awa.job_state IS NULL OR state = $1)
          AND ($2::text IS NULL OR kind = $2)
          AND ($3::text IS NULL OR queue = $3)
        ORDER BY id DESC
        LIMIT $4
        "#,
    )
    .bind(filter.state)
    .bind(&filter.kind)
    .bind(&filter.queue)
    .bind(limit)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// Get a single job by ID.
pub async fn get_job<'e, E>(executor: E, job_id: i64) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, JobRow>("SELECT * FROM awa.jobs WHERE id = $1")
        .bind(job_id)
        .fetch_optional(executor)
        .await?;

    row.ok_or(AwaError::JobNotFound { id: job_id })
}

/// Register a callback for a running job, writing the callback_id and timeout
/// to the database immediately.
///
/// Call this BEFORE sending the callback_id to the external system to avoid
/// the race condition where the external system fires before the DB knows
/// about the callback.
///
/// Returns the generated callback UUID on success.
pub async fn register_callback<'e, E>(
    executor: E,
    job_id: i64,
    run_lease: i64,
    timeout: std::time::Duration,
) -> Result<Uuid, AwaError>
where
    E: PgExecutor<'e>,
{
    let callback_id = Uuid::new_v4();
    let timeout_secs = timeout.as_secs_f64();
    let result = sqlx::query(
        r#"UPDATE awa.jobs
           SET callback_id = $2,
               callback_timeout_at = now() + make_interval(secs => $3),
               callback_filter = NULL,
               callback_on_complete = NULL,
               callback_on_fail = NULL,
               callback_transform = NULL
           WHERE id = $1 AND state = 'running' AND run_lease = $4"#,
    )
    .bind(job_id)
    .bind(callback_id)
    .bind(timeout_secs)
    .bind(run_lease)
    .execute(executor)
    .await?;
    if result.rows_affected() == 0 {
        return Err(AwaError::Validation("job is not in running state".into()));
    }
    Ok(callback_id)
}

/// Complete a waiting job via external callback.
///
/// Accepts jobs in `waiting_external` or `running` state (race handling: the
/// external system may fire before the executor transitions to `waiting_external`).
///
/// The `payload` parameter is accepted but not stored on the job — it exists
/// for callers who want to pass callback data through and will be used by the
/// planned CEL expression filtering/transforms feature. Callers can process
/// the payload immediately or enqueue a follow-up job with it.
pub async fn complete_external<'e, E>(
    executor: E,
    callback_id: Uuid,
    _payload: Option<serde_json::Value>,
) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'completed',
            finalized_at = now(),
            callback_id = NULL,
            callback_timeout_at = NULL,
            callback_filter = NULL,
            callback_on_complete = NULL,
            callback_on_fail = NULL,
            callback_transform = NULL,
            heartbeat_at = NULL,
            deadline_at = NULL,
            progress = NULL
        WHERE callback_id = $1 AND state IN ('waiting_external', 'running')
        RETURNING *
        "#,
    )
    .bind(callback_id)
    .fetch_optional(executor)
    .await?;

    row.ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })
}

/// Fail a waiting job via external callback.
///
/// Records the error and transitions to `failed`.
pub async fn fail_external<'e, E>(
    executor: E,
    callback_id: Uuid,
    error: &str,
) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'failed',
            finalized_at = now(),
            callback_id = NULL,
            callback_timeout_at = NULL,
            callback_filter = NULL,
            callback_on_complete = NULL,
            callback_on_fail = NULL,
            callback_transform = NULL,
            heartbeat_at = NULL,
            deadline_at = NULL,
            errors = errors || jsonb_build_object(
                'error', $2::text,
                'attempt', attempt,
                'at', now()
            )::jsonb
        WHERE callback_id = $1 AND state IN ('waiting_external', 'running')
        RETURNING *
        "#,
    )
    .bind(callback_id)
    .bind(error)
    .fetch_optional(executor)
    .await?;

    row.ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })
}

/// Retry a waiting job via external callback.
///
/// Resets to `available` with attempt = 0. The handler must be idempotent
/// with respect to the external call — a retry re-executes from scratch.
///
/// Only accepts `waiting_external` state — unlike complete/fail which are
/// terminal transitions, retry puts the job back to `available`. Allowing
/// retry from `running` would risk concurrent dispatch if the original
/// handler hasn't finished yet.
pub async fn retry_external<'e, E>(executor: E, callback_id: Uuid) -> Result<JobRow, AwaError>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'available',
            attempt = 0,
            run_at = now(),
            finalized_at = NULL,
            callback_id = NULL,
            callback_timeout_at = NULL,
            callback_filter = NULL,
            callback_on_complete = NULL,
            callback_on_fail = NULL,
            callback_transform = NULL,
            heartbeat_at = NULL,
            deadline_at = NULL
        WHERE callback_id = $1 AND state = 'waiting_external'
        RETURNING *
        "#,
    )
    .bind(callback_id)
    .fetch_optional(executor)
    .await?;

    row.ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })
}

// ── CEL callback expressions ──────────────────────────────────────────

/// Configuration for CEL callback expressions.
///
/// All fields are optional. When all are `None`, behaviour is identical to
/// the original `register_callback` (no expression evaluation).
#[derive(Debug, Clone, Default)]
pub struct CallbackConfig {
    /// Gate: should this payload be processed at all? Returns bool.
    pub filter: Option<String>,
    /// Does this payload indicate success? Returns bool.
    pub on_complete: Option<String>,
    /// Does this payload indicate failure? Returns bool. Evaluated before on_complete.
    pub on_fail: Option<String>,
    /// Reshape payload before returning to caller. Returns any Value.
    pub transform: Option<String>,
}

impl CallbackConfig {
    /// Returns true if no expressions are configured.
    pub fn is_empty(&self) -> bool {
        self.filter.is_none()
            && self.on_complete.is_none()
            && self.on_fail.is_none()
            && self.transform.is_none()
    }
}

/// What `resolve_callback` should do if no CEL conditions match or no
/// expressions are configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultAction {
    Complete,
    Fail,
    Ignore,
}

/// Outcome of `resolve_callback`.
#[derive(Debug)]
pub enum ResolveOutcome {
    Completed {
        payload: Option<serde_json::Value>,
        job: JobRow,
    },
    Failed {
        job: JobRow,
    },
    Ignored {
        reason: String,
    },
}

impl ResolveOutcome {
    pub fn is_completed(&self) -> bool {
        matches!(self, ResolveOutcome::Completed { .. })
    }
    pub fn is_failed(&self) -> bool {
        matches!(self, ResolveOutcome::Failed { .. })
    }
    pub fn is_ignored(&self) -> bool {
        matches!(self, ResolveOutcome::Ignored { .. })
    }
}

/// Register a callback with optional CEL expressions.
///
/// When expressions are provided and the `cel` feature is enabled, each
/// expression is trial-compiled at registration time so syntax errors are
/// caught early.
///
/// When the `cel` feature is disabled and any expression is non-None,
/// returns `AwaError::Validation`.
pub async fn register_callback_with_config<'e, E>(
    executor: E,
    job_id: i64,
    run_lease: i64,
    timeout: std::time::Duration,
    config: &CallbackConfig,
) -> Result<Uuid, AwaError>
where
    E: PgExecutor<'e>,
{
    // Validate CEL expressions at registration time: compile + check references
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

                // Reject undeclared variables — CEL only reports these at execution
                // time, so an expression like `missing == 1` would parse fine but
                // silently fall into the fail-open path at resolve time.
                let refs = program.references();
                let bad_vars: Vec<&str> = refs
                    .variables()
                    .into_iter()
                    .filter(|v| *v != "payload")
                    .collect();
                if !bad_vars.is_empty() {
                    return Err(AwaError::Validation(format!(
                        "CEL expression for {name} references undeclared variable(s): {}; \
                         only 'payload' is available",
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
    let timeout_secs = timeout.as_secs_f64();

    let result = sqlx::query(
        r#"UPDATE awa.jobs
           SET callback_id = $2,
               callback_timeout_at = now() + make_interval(secs => $3),
               callback_filter = $4,
               callback_on_complete = $5,
               callback_on_fail = $6,
               callback_transform = $7
           WHERE id = $1 AND state = 'running' AND run_lease = $8"#,
    )
    .bind(job_id)
    .bind(callback_id)
    .bind(timeout_secs)
    .bind(&config.filter)
    .bind(&config.on_complete)
    .bind(&config.on_fail)
    .bind(&config.transform)
    .bind(run_lease)
    .execute(executor)
    .await?;

    if result.rows_affected() == 0 {
        return Err(AwaError::Validation("job is not in running state".into()));
    }
    Ok(callback_id)
}

/// Internal action decided by CEL evaluation or default.
enum ResolveAction {
    Complete(Option<serde_json::Value>),
    Fail {
        error: String,
        expression: Option<String>,
    },
    Ignore(String),
}

/// Resolve a callback by evaluating CEL expressions against the payload.
///
/// Uses a transaction with `SELECT ... FOR UPDATE` for atomicity.
/// The `default_action` determines behaviour when no CEL conditions match
/// or no expressions are configured.
pub async fn resolve_callback(
    pool: &PgPool,
    callback_id: Uuid,
    payload: Option<serde_json::Value>,
    default_action: DefaultAction,
) -> Result<ResolveOutcome, AwaError> {
    let mut tx = pool.begin().await?;

    let job = sqlx::query_as::<_, JobRow>(
        "SELECT * FROM awa.jobs WHERE callback_id = $1
         AND state = 'waiting_external'
         FOR UPDATE",
    )
    .bind(callback_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(AwaError::CallbackNotFound {
        callback_id: callback_id.to_string(),
    })?;

    let action = evaluate_or_default(&job, &payload, default_action)?;

    match action {
        ResolveAction::Complete(transformed_payload) => {
            let completed_job = sqlx::query_as::<_, JobRow>(
                r#"
                UPDATE awa.jobs
                SET state = 'completed',
                    finalized_at = now(),
                    callback_id = NULL,
                    callback_timeout_at = NULL,
                    callback_filter = NULL,
                    callback_on_complete = NULL,
                    callback_on_fail = NULL,
                    callback_transform = NULL,
                    heartbeat_at = NULL,
                    deadline_at = NULL,
                    progress = NULL
                WHERE id = $1
                RETURNING *
                "#,
            )
            .bind(job.id)
            .fetch_one(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(ResolveOutcome::Completed {
                payload: transformed_payload,
                job: completed_job,
            })
        }
        ResolveAction::Fail { error, expression } => {
            let mut error_json = serde_json::json!({
                "error": error,
                "attempt": job.attempt,
                "at": chrono::Utc::now().to_rfc3339(),
            });
            if let Some(expr) = expression {
                error_json["expression"] = serde_json::Value::String(expr);
            }

            let failed_job = sqlx::query_as::<_, JobRow>(
                r#"
                UPDATE awa.jobs
                SET state = 'failed',
                    finalized_at = now(),
                    callback_id = NULL,
                    callback_timeout_at = NULL,
                    callback_filter = NULL,
                    callback_on_complete = NULL,
                    callback_on_fail = NULL,
                    callback_transform = NULL,
                    heartbeat_at = NULL,
                    deadline_at = NULL,
                    errors = errors || $2::jsonb
                WHERE id = $1
                RETURNING *
                "#,
            )
            .bind(job.id)
            .bind(error_json)
            .fetch_one(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(ResolveOutcome::Failed { job: failed_job })
        }
        ResolveAction::Ignore(reason) => {
            // No state change — dropping tx releases FOR UPDATE lock
            Ok(ResolveOutcome::Ignored { reason })
        }
    }
}

/// Evaluate CEL expressions or fall through to default_action.
fn evaluate_or_default(
    job: &JobRow,
    payload: &Option<serde_json::Value>,
    default_action: DefaultAction,
) -> Result<ResolveAction, AwaError> {
    let has_expressions = job.callback_filter.is_some()
        || job.callback_on_complete.is_some()
        || job.callback_on_fail.is_some()
        || job.callback_transform.is_some();

    if !has_expressions {
        return Ok(apply_default(default_action, payload));
    }

    #[cfg(feature = "cel")]
    {
        Ok(evaluate_cel(job, payload, default_action))
    }

    #[cfg(not(feature = "cel"))]
    {
        // Expressions are present but CEL feature is not enabled.
        // Return an error without mutating the job — it stays in waiting_external.
        let _ = (payload, default_action);
        Err(AwaError::Validation(
            "CEL expressions present but 'cel' feature is not enabled".into(),
        ))
    }
}

fn apply_default(
    default_action: DefaultAction,
    payload: &Option<serde_json::Value>,
) -> ResolveAction {
    match default_action {
        DefaultAction::Complete => ResolveAction::Complete(payload.clone()),
        DefaultAction::Fail => ResolveAction::Fail {
            error: "callback failed: default action".to_string(),
            expression: None,
        },
        DefaultAction::Ignore => {
            ResolveAction::Ignore("no expressions configured, default is ignore".to_string())
        }
    }
}

#[cfg(feature = "cel")]
fn evaluate_cel(
    job: &JobRow,
    payload: &Option<serde_json::Value>,
    default_action: DefaultAction,
) -> ResolveAction {
    let payload_value = payload.as_ref().cloned().unwrap_or(serde_json::Value::Null);

    // 1. Evaluate filter
    if let Some(filter_expr) = &job.callback_filter {
        match eval_bool(filter_expr, &payload_value, job.id, "filter") {
            Ok(true) => {} // pass through
            Ok(false) => {
                return ResolveAction::Ignore("filter expression returned false".to_string());
            }
            Err(_) => {
                // Fail-open: treat filter error as true (pass through)
            }
        }
    }

    // 2. Evaluate on_fail (before on_complete — fail takes precedence)
    if let Some(on_fail_expr) = &job.callback_on_fail {
        match eval_bool(on_fail_expr, &payload_value, job.id, "on_fail") {
            Ok(true) => {
                return ResolveAction::Fail {
                    error: "callback failed: on_fail expression matched".to_string(),
                    expression: Some(on_fail_expr.clone()),
                };
            }
            Ok(false) => {} // don't fail
            Err(_) => {
                // Fail-open: treat on_fail error as false (don't fail)
            }
        }
    }

    // 3. Evaluate on_complete
    if let Some(on_complete_expr) = &job.callback_on_complete {
        match eval_bool(on_complete_expr, &payload_value, job.id, "on_complete") {
            Ok(true) => {
                // Complete with optional transform
                let transformed = apply_transform(job, &payload_value);
                return ResolveAction::Complete(Some(transformed));
            }
            Ok(false) => {} // don't complete
            Err(_) => {
                // Fail-open: treat on_complete error as false (don't complete)
            }
        }
    }

    // 4. Neither condition matched → apply default_action
    apply_default(default_action, payload)
}

#[cfg(feature = "cel")]
fn eval_bool(
    expression: &str,
    payload_value: &serde_json::Value,
    job_id: i64,
    expression_name: &str,
) -> Result<bool, ()> {
    let program = match cel::Program::compile(expression) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                job_id,
                expression_name,
                expression,
                error = %e,
                "CEL compilation error during evaluation"
            );
            return Err(());
        }
    };

    let mut context = cel::Context::default();
    if let Err(e) = context.add_variable("payload", payload_value.clone()) {
        tracing::warn!(
            job_id,
            expression_name,
            error = %e,
            "Failed to add payload variable to CEL context"
        );
        return Err(());
    }

    match program.execute(&context) {
        Ok(cel::Value::Bool(b)) => Ok(b),
        Ok(other) => {
            tracing::warn!(
                job_id,
                expression_name,
                expression,
                result_type = ?other.type_of(),
                "CEL expression returned non-bool"
            );
            Err(())
        }
        Err(e) => {
            tracing::warn!(
                job_id,
                expression_name,
                expression,
                error = %e,
                "CEL execution error"
            );
            Err(())
        }
    }
}

#[cfg(feature = "cel")]
fn apply_transform(job: &JobRow, payload_value: &serde_json::Value) -> serde_json::Value {
    let transform_expr = match &job.callback_transform {
        Some(expr) => expr,
        None => return payload_value.clone(),
    };

    let program = match cel::Program::compile(transform_expr) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                job_id = job.id,
                expression = transform_expr,
                error = %e,
                "CEL transform compilation error, using original payload"
            );
            return payload_value.clone();
        }
    };

    let mut context = cel::Context::default();
    if let Err(e) = context.add_variable("payload", payload_value.clone()) {
        tracing::warn!(
            job_id = job.id,
            error = %e,
            "Failed to add payload variable for transform"
        );
        return payload_value.clone();
    }

    match program.execute(&context) {
        Ok(value) => match value.json() {
            Ok(json) => json,
            Err(e) => {
                tracing::warn!(
                    job_id = job.id,
                    expression = transform_expr,
                    error = %e,
                    "CEL transform result could not be converted to JSON, using original payload"
                );
                payload_value.clone()
            }
        },
        Err(e) => {
            tracing::warn!(
                job_id = job.id,
                expression = transform_expr,
                error = %e,
                "CEL transform execution error, using original payload"
            );
            payload_value.clone()
        }
    }
}
