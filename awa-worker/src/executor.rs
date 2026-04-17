use crate::completion::CompletionBatcherHandle;
use crate::context::{CallbackGuard, JobContext};
use crate::events::{BoxedUntypedEventHandler, UntypedJobEvent};
use crate::runtime::{InFlightMap, InFlightState, ProgressState};
use awa_model::{AwaError, JobRow};
use sqlx::PgPool;
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, info_span, warn, Instrument};

/// Result of executing a job handler.
///
/// See also [`JobError`] for the error side — notably [`JobError::Retryable`]
/// provides error-driven retry with database-computed backoff, while
/// [`JobResult::RetryAfter`] is an explicit retry with caller-specified delay.
#[derive(Debug)]
pub enum JobResult {
    /// Job completed successfully.
    Completed,
    /// Job should be retried after the given duration. Increments attempt.
    RetryAfter(std::time::Duration),
    /// Job should be snoozed (re-available after duration). Does NOT increment attempt.
    Snooze(std::time::Duration),
    /// Job should be cancelled.
    Cancel(String),
    /// Job is waiting for an external callback (webhook completion).
    ///
    /// Obtain the required guard from `ctx.register_callback()` or
    /// `ctx.register_callback_with_config()`.
    WaitForCallback(CallbackGuard),
}

/// Error type for job handlers — any error is retryable unless it's terminal.
///
/// [`JobError::Retryable`] triggers retry with database-computed exponential backoff.
/// For explicit caller-controlled retry delay, return [`Ok(JobResult::RetryAfter)`] instead.
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    /// Retryable error — will be retried if attempts remain.
    #[error("{0}")]
    Retryable(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Terminal error — immediately fails the job regardless of remaining attempts.
    #[error("terminal: {0}")]
    Terminal(String),
}

impl JobError {
    /// Create a retryable error from any `std::error::Error`.
    pub fn retryable(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        JobError::Retryable(Box::new(err))
    }

    /// Create a retryable error from a display message.
    ///
    /// Use this with `anyhow::Error` or other types that implement `Display`
    /// but not `std::error::Error`:
    /// ```ignore
    /// Err(JobError::retryable_msg(format!("{err:#}")))
    /// // or with anyhow:
    /// Err(JobError::retryable_msg(err))
    /// ```
    pub fn retryable_msg(msg: impl std::fmt::Display) -> Self {
        JobError::Retryable(Box::new(DisplayError(msg.to_string())))
    }

    /// Create a terminal error — immediately fails the job.
    pub fn terminal(msg: impl Into<String>) -> Self {
        JobError::Terminal(msg.into())
    }
}

/// Wrapper to turn a Display string into a std::error::Error for retryable_msg.
#[derive(Debug)]
struct DisplayError(String);

impl std::fmt::Display for DisplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DisplayError {}

/// With the `anyhow` feature, `?` works directly in handlers:
/// ```ignore
/// async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
///     let data = fallible_thing().await?; // anyhow::Error → JobError::Retryable
///     Ok(JobResult::Completed)
/// }
/// ```
#[cfg(feature = "anyhow")]
impl From<anyhow::Error> for JobError {
    fn from(err: anyhow::Error) -> Self {
        JobError::retryable_msg(format!("{err:#}"))
    }
}

/// Worker trait — implement this for each job type.
///
/// # Handling permanent failure
///
/// When all retry attempts are exhausted, awa moves the job to `failed`.
/// To run cleanup logic (update external state, send notifications), check
/// the attempt count inside `perform`:
///
/// ```ignore
/// async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
///     match do_work(ctx).await {
///         Ok(()) => Ok(JobResult::Completed),
///         Err(err) if ctx.job.attempt >= ctx.job.max_attempts => {
///             // Last attempt — run cleanup before awa marks as failed
///             mark_permanently_failed(ctx.job.id).await;
///             Err(JobError::retryable(err))
///         }
///         Err(err) => Err(JobError::retryable(err)),
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait Worker: Send + Sync + 'static {
    /// The kind string for this worker (must match the job's kind).
    fn kind(&self) -> &'static str;

    /// Execute the job. Access the job row via `ctx.job`.
    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError>;
}

/// Type-erased worker wrapper for the registry.
pub(crate) type BoxedWorker = Box<dyn Worker>;

/// Result of a state-transition attempt in `complete_job`.
#[allow(clippy::large_enum_variant)]
enum CompletionOutcome {
    /// The DB update was applied; optionally carries a lifecycle event to dispatch.
    Applied { event: Option<UntypedJobEvent> },
    /// The job was already rescued/cancelled — stale completion, no event.
    IgnoredStale,
}

/// Per-queue DLQ policy resolved at `Client::start`.
///
/// `enabled_default` controls the fallback for queues without an explicit
/// override, and `overrides` holds per-queue decisions. When enabled for a
/// queue, the executor routes terminal failures (Terminal errors + exhausted
/// retries) through `awa.move_to_dlq_guarded` instead of updating the
/// failed state in `jobs_hot`.
#[derive(Debug, Clone, Default)]
pub(crate) struct DlqPolicy {
    pub enabled_default: bool,
    pub overrides: Arc<HashMap<String, bool>>,
}

impl DlqPolicy {
    pub fn enabled_for(&self, queue: &str) -> bool {
        self.overrides
            .get(queue)
            .copied()
            .unwrap_or(self.enabled_default)
    }
}

/// Manages job execution — spawns worker futures and tracks in-flight jobs.
pub struct JobExecutor {
    pool: PgPool,
    workers: Arc<HashMap<String, BoxedWorker>>,
    lifecycle_handlers: Arc<HashMap<String, Vec<BoxedUntypedEventHandler>>>,
    in_flight: InFlightMap,
    queue_in_flight: Arc<HashMap<String, Arc<AtomicU32>>>,
    state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
    metrics: crate::metrics::AwaMetrics,
    completion_batcher: CompletionBatcherHandle,
    dlq_policy: DlqPolicy,
}

impl JobExecutor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        pool: PgPool,
        workers: Arc<HashMap<String, BoxedWorker>>,
        lifecycle_handlers: Arc<HashMap<String, Vec<BoxedUntypedEventHandler>>>,
        in_flight: InFlightMap,
        queue_in_flight: Arc<HashMap<String, Arc<AtomicU32>>>,
        state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
        metrics: crate::metrics::AwaMetrics,
        completion_batcher: CompletionBatcherHandle,
        dlq_policy: DlqPolicy,
    ) -> Self {
        Self {
            pool,
            workers,
            lifecycle_handlers,
            in_flight,
            queue_in_flight,
            state,
            metrics,
            completion_batcher,
            dlq_policy,
        }
    }

    /// Build the future that executes a claimed job.
    ///
    /// The caller is responsible for spawning it onto the runtime.
    pub fn execute_task(
        &self,
        job: JobRow,
        cancel: Arc<AtomicBool>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let pool = self.pool.clone();
        let workers = self.workers.clone();
        let lifecycle_handlers = self.lifecycle_handlers.clone();
        let in_flight = self.in_flight.clone();
        let queue_in_flight = self.queue_in_flight.clone();
        let state = self.state.clone();
        let metrics = self.metrics.clone();
        let completion_batcher = self.completion_batcher.clone();
        let dlq_policy = self.dlq_policy.clone();
        let job_id = job.id;
        let job_run_lease = job.run_lease;
        let job_kind = job.kind.clone();
        let job_queue = job.queue.clone();

        let span = info_span!(
            "job.execute",
            job.id = job_id,
            job.kind = %job_kind,
            job.queue = %job_queue,
            job.attempt = job.attempt,
            otel.name = %format!("job.execute {}", job_kind),
            otel.status_code = tracing::field::Empty,
        );

        async move {
            // Seed progress from the persisted checkpoint (for retries/snoozes)
            let progress_state = Arc::new(std::sync::Mutex::new(ProgressState::new(
                job.progress.clone(),
            )));

            // Register as in-flight with cancel + progress
            let in_flight_state = InFlightState {
                cancel: cancel.clone(),
                progress: progress_state.clone(),
            };
            in_flight.insert((job_id, job_run_lease), in_flight_state);
            if let Some(counter) = queue_in_flight.get(&job_queue) {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            metrics.record_in_flight_change(&job_queue, 1);

            let start = std::time::Instant::now();
            let ctx = JobContext::new(
                job.clone(),
                cancel,
                state,
                pool.clone(),
                progress_state.clone(),
            );

            let result = match workers.get(&job.kind) {
                Some(worker) => worker.perform(&ctx).await,
                None => {
                    error!(kind = %job.kind, job_id, "No worker registered for job kind");
                    Err(JobError::Terminal(format!(
                        "unknown job kind: {}",
                        job.kind
                    )))
                }
            };

            let duration = start.elapsed();

            // Snapshot progress for state transition
            let progress_snapshot = {
                let guard = progress_state.lock().expect("progress lock poisoned");
                guard.clone_latest()
            };

            // Complete the job based on the result, then record metrics
            // only if the state transition actually happened (not stale).
            let has_lifecycle_handlers = lifecycle_handlers.contains_key(&job_kind);
            let dlq_enabled = dlq_policy.enabled_for(&job_queue);
            let outcome = complete_job(
                &pool,
                &job,
                &result,
                &completion_batcher,
                progress_snapshot,
                duration,
                has_lifecycle_handlers,
                dlq_enabled,
                &metrics,
            )
            .await;

            match &outcome {
                Ok(CompletionOutcome::Applied { .. }) => {
                    // State transition succeeded — record metrics
                    match &result {
                        Ok(JobResult::Completed) => {
                            metrics.record_job_completed(&job_kind, &job_queue, duration);
                        }
                        Ok(JobResult::RetryAfter(_)) => {
                            metrics.record_job_retried(&job_kind, &job_queue);
                        }
                        Ok(JobResult::Cancel(_)) => {
                            metrics.jobs_cancelled.add(
                                1,
                                &[
                                    opentelemetry::KeyValue::new("awa.job.kind", job_kind.clone()),
                                    opentelemetry::KeyValue::new(
                                        "awa.job.queue",
                                        job_queue.clone(),
                                    ),
                                ],
                            );
                        }
                        Ok(JobResult::Snooze(_)) => {} // Not a terminal outcome
                        Ok(JobResult::WaitForCallback(_)) => {
                            metrics.jobs_waiting_external.add(
                                1,
                                &[
                                    opentelemetry::KeyValue::new("awa.job.kind", job_kind.clone()),
                                    opentelemetry::KeyValue::new(
                                        "awa.job.queue",
                                        job_queue.clone(),
                                    ),
                                ],
                            );
                        }
                        Err(JobError::Terminal(_)) => {
                            metrics.record_job_failed(&job_kind, &job_queue, true);
                        }
                        Err(JobError::Retryable(_)) => {
                            metrics.record_job_retried(&job_kind, &job_queue);
                        }
                    }
                }
                Ok(CompletionOutcome::IgnoredStale) => {
                    // Job was already rescued/cancelled — no metrics
                }
                Err(err) => {
                    error!(job_id, error = %err, "Failed to complete job");
                }
            }

            // Remove from in-flight BEFORE dispatching lifecycle events.
            // This ensures a slow/hung handler doesn't hold the permit open,
            // block queue capacity, or delay graceful shutdown.
            in_flight.remove((job_id, job_run_lease));
            if let Some(counter) = queue_in_flight.get(&job_queue) {
                counter.fetch_sub(1, Ordering::SeqCst);
            }
            metrics.record_in_flight_change(&job_queue, -1);

            // Dispatch lifecycle event as a detached task — best effort,
            // does not block the executor or affect shutdown drain.
            if let Ok(CompletionOutcome::Applied {
                event: Some(event), ..
            }) = outcome
            {
                let handlers = lifecycle_handlers.clone();
                let kind = job_kind.clone();
                tokio::spawn(async move {
                    dispatch_lifecycle_event(&handlers, &kind, event).await;
                });
            }
        }
        .instrument(span)
    }
}

/// Update job state in the database based on handler result.
///
/// Returns a `CompletionOutcome` indicating whether the state transition was
/// applied (with an optional lifecycle event) or ignored as stale.
#[allow(clippy::too_many_arguments)]
async fn complete_job(
    pool: &PgPool,
    job: &JobRow,
    result: &Result<JobResult, JobError>,
    completion_batcher: &CompletionBatcherHandle,
    progress_snapshot: Option<serde_json::Value>,
    duration: Duration,
    needs_event: bool,
    dlq_enabled: bool,
    metrics: &crate::metrics::AwaMetrics,
) -> Result<CompletionOutcome, AwaError> {
    match result {
        Ok(JobResult::Completed) => {
            tracing::Span::current().record("otel.status_code", "OK");
            info!(job_id = job.id, kind = %job.kind, attempt = job.attempt, "Job completed");
            let result = match completion_batcher.complete(job.id, job.run_lease).await {
                Ok(updated) => updated,
                Err(err) => {
                    warn!(
                        job_id = job.id,
                        error = %err,
                        "Completion batch flush failed, falling back to direct finalize"
                    );
                    direct_complete_job(pool, job).await?
                }
            };
            if !result {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, completion ignored"
                );
                return Ok(CompletionOutcome::IgnoredStale);
            }
            if needs_event {
                let updated_job: JobRow = sqlx::query_as("SELECT * FROM awa.jobs WHERE id = $1")
                    .bind(job.id)
                    .fetch_one(pool)
                    .await?;
                Ok(CompletionOutcome::Applied {
                    event: Some(UntypedJobEvent::Completed {
                        job: updated_job,
                        duration,
                    }),
                })
            } else {
                Ok(CompletionOutcome::Applied { event: None })
            }
        }

        Ok(JobResult::RetryAfter(retry_duration)) => {
            let seconds = retry_duration.as_secs() as f64;
            info!(
                job_id = job.id,
                kind = %job.kind,
                retry_after_secs = seconds,
                "Job requested retry after duration"
            );
            let result = sqlx::query(
                r#"
                UPDATE awa.jobs
                SET state = 'retryable',
                    run_at = now() + make_interval(secs => $2),
                    finalized_at = now(),
                    progress = $4
                WHERE id = $1 AND state = 'running' AND run_lease = $3
                "#,
            )
            .bind(job.id)
            .bind(seconds)
            .bind(job.run_lease)
            .bind(&progress_snapshot)
            .execute(pool)
            .await?;
            if result.rows_affected() == 0 {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, retry ignored"
                );
                return Ok(CompletionOutcome::IgnoredStale);
            }
            if needs_event {
                let updated_job: JobRow = sqlx::query_as("SELECT * FROM awa.jobs WHERE id = $1")
                    .bind(job.id)
                    .fetch_one(pool)
                    .await?;
                Ok(CompletionOutcome::Applied {
                    event: Some(UntypedJobEvent::Retried {
                        job: updated_job.clone(),
                        error: String::new(),
                        attempt: updated_job.attempt,
                        next_run_at: updated_job.run_at,
                    }),
                })
            } else {
                Ok(CompletionOutcome::Applied { event: None })
            }
        }

        Ok(JobResult::Snooze(snooze_duration)) => {
            let seconds = snooze_duration.as_secs() as f64;
            info!(
                job_id = job.id,
                kind = %job.kind,
                snooze_secs = seconds,
                "Job snoozed (attempt not incremented)"
            );
            // Snooze: back to available with new run_at, decrement attempt
            // (since it was already incremented at claim time)
            let result = sqlx::query(
                r#"
                UPDATE awa.jobs
                SET state = 'scheduled',
                    run_at = now() + make_interval(secs => $2),
                    attempt = attempt - 1,
                    heartbeat_at = NULL,
                    deadline_at = NULL,
                    progress = $4
                WHERE id = $1 AND state = 'running' AND run_lease = $3
                "#,
            )
            .bind(job.id)
            .bind(seconds)
            .bind(job.run_lease)
            .bind(&progress_snapshot)
            .execute(pool)
            .await?;
            if result.rows_affected() == 0 {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, snooze ignored"
                );
                return Ok(CompletionOutcome::IgnoredStale);
            }
            // Snooze is not a terminal event — no lifecycle event
            Ok(CompletionOutcome::Applied { event: None })
        }

        Ok(JobResult::Cancel(reason)) => {
            info!(
                job_id = job.id,
                kind = %job.kind,
                reason = %reason,
                "Job cancelled by handler"
            );
            let result = sqlx::query(
                r#"
                UPDATE awa.jobs
                SET state = 'cancelled',
                    finalized_at = now(),
                    errors = errors || $2::jsonb,
                    progress = $4
                WHERE id = $1 AND state = 'running' AND run_lease = $3
                "#,
            )
            .bind(job.id)
            .bind(serde_json::json!({
                "error": format!("cancelled: {}", reason),
                "attempt": job.attempt,
                "at": chrono::Utc::now().to_rfc3339()
            }))
            .bind(job.run_lease)
            .bind(&progress_snapshot)
            .execute(pool)
            .await?;
            if result.rows_affected() == 0 {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, cancel ignored"
                );
                return Ok(CompletionOutcome::IgnoredStale);
            }
            if needs_event {
                let updated_job: JobRow = sqlx::query_as("SELECT * FROM awa.jobs WHERE id = $1")
                    .bind(job.id)
                    .fetch_one(pool)
                    .await?;
                Ok(CompletionOutcome::Applied {
                    event: Some(UntypedJobEvent::Cancelled {
                        job: updated_job,
                        reason: reason.clone(),
                    }),
                })
            } else {
                Ok(CompletionOutcome::Applied { event: None })
            }
        }

        Ok(JobResult::WaitForCallback(_guard)) => {
            info!(
                job_id = job.id,
                kind = %job.kind,
                "Job waiting for external callback"
            );
            // Transition to waiting_external. Requires callback_id to be set
            // (handler must have called register_callback).
            let result = sqlx::query(
                r#"
                UPDATE awa.jobs
                SET state = 'waiting_external',
                    heartbeat_at = NULL,
                    deadline_at = NULL,
                    progress = $3
                WHERE id = $1 AND state = 'running' AND run_lease = $2 AND callback_id IS NOT NULL
                "#,
            )
            .bind(job.id)
            .bind(job.run_lease)
            .bind(&progress_snapshot)
            .execute(pool)
            .await?;
            if result.rows_affected() == 0 {
                // Check if a racing callback already completed/failed the job,
                // or if the handler forgot to call register_callback.
                let current: Option<(awa_model::JobState, Option<uuid::Uuid>)> =
                    sqlx::query_as("SELECT state, callback_id FROM awa.jobs WHERE id = $1")
                        .bind(job.id)
                        .fetch_optional(pool)
                        .await?;
                match current {
                    Some((state, _)) if state.is_terminal() => {
                        // Racing callback already completed the job — all good
                        info!(
                            job_id = job.id,
                            state = %state,
                            "Job already completed by racing callback"
                        );
                        // No lifecycle event for wait-for-callback
                        return Ok(CompletionOutcome::Applied { event: None });
                    }
                    Some((_, None)) => {
                        // Still running but no callback_id — programming error
                        error!(
                            job_id = job.id,
                            "WaitForCallback returned without calling register_callback"
                        );
                        sqlx::query(
                            r#"
                            UPDATE awa.jobs
                            SET state = 'failed',
                                finalized_at = now(),
                                errors = errors || $2::jsonb
                            WHERE id = $1 AND state = 'running' AND run_lease = $3
                            "#,
                        )
                        .bind(job.id)
                        .bind(serde_json::json!({
                            "error": "WaitForCallback returned without calling register_callback",
                            "attempt": job.attempt,
                            "at": chrono::Utc::now().to_rfc3339(),
                            "terminal": true
                        }))
                        .bind(job.run_lease)
                        .execute(pool)
                        .await?;
                        // No lifecycle event for wait-for-callback
                        return Ok(CompletionOutcome::Applied { event: None });
                    }
                    _ => {
                        warn!(
                            job_id = job.id,
                            "Job already rescued/cancelled, wait-for-callback ignored"
                        );
                        return Ok(CompletionOutcome::IgnoredStale);
                    }
                }
            }
            // No lifecycle event for wait-for-callback
            Ok(CompletionOutcome::Applied { event: None })
        }

        Err(JobError::Terminal(msg)) => {
            tracing::Span::current().record("otel.status_code", "ERROR");
            error!(
                job_id = job.id,
                kind = %job.kind,
                error = %msg,
                "Job failed terminally"
            );
            let error_json = serde_json::json!({
                "error": msg.to_string(),
                "attempt": job.attempt,
                "at": chrono::Utc::now().to_rfc3339(),
                "terminal": true
            });
            apply_terminal_failure(
                pool,
                job,
                error_json,
                progress_snapshot.as_ref(),
                dlq_enabled,
                "terminal_error",
                msg,
                needs_event,
                metrics,
            )
            .await
        }

        Err(JobError::Retryable(err)) => {
            let error_msg = err.to_string();
            if job.attempt >= job.max_attempts {
                tracing::Span::current().record("otel.status_code", "ERROR");
                error!(
                    job_id = job.id,
                    kind = %job.kind,
                    attempt = job.attempt,
                    max_attempts = job.max_attempts,
                    error = %error_msg,
                    "Job failed (max attempts exhausted)"
                );
                let error_json = serde_json::json!({
                    "error": error_msg,
                    "attempt": job.attempt,
                    "at": chrono::Utc::now().to_rfc3339()
                });
                apply_terminal_failure(
                    pool,
                    job,
                    error_json,
                    progress_snapshot.as_ref(),
                    dlq_enabled,
                    "max_attempts_exhausted",
                    &error_msg,
                    needs_event,
                    metrics,
                )
                .await
            } else {
                warn!(
                    job_id = job.id,
                    kind = %job.kind,
                    attempt = job.attempt,
                    error = %error_msg,
                    "Job failed (will retry)"
                );
                // Use database-side backoff calculation
                let result = sqlx::query(
                    r#"
                    UPDATE awa.jobs
                    SET state = 'retryable',
                        run_at = now() + awa.backoff_duration($2, $3),
                        finalized_at = now(),
                        heartbeat_at = NULL,
                        deadline_at = NULL,
                        errors = errors || $4::jsonb,
                        progress = $6
                    WHERE id = $1 AND state = 'running' AND run_lease = $5
                    "#,
                )
                .bind(job.id)
                .bind(job.attempt)
                .bind(job.max_attempts)
                .bind(serde_json::json!({
                    "error": error_msg,
                    "attempt": job.attempt,
                    "at": chrono::Utc::now().to_rfc3339()
                }))
                .bind(job.run_lease)
                .bind(&progress_snapshot)
                .execute(pool)
                .await?;
                if result.rows_affected() == 0 {
                    warn!(
                        job_id = job.id,
                        "Job already rescued/cancelled, retry ignored"
                    );
                    return Ok(CompletionOutcome::IgnoredStale);
                }
                if needs_event {
                    let updated_job: JobRow =
                        sqlx::query_as("SELECT * FROM awa.jobs WHERE id = $1")
                            .bind(job.id)
                            .fetch_one(pool)
                            .await?;
                    Ok(CompletionOutcome::Applied {
                        event: Some(UntypedJobEvent::Retried {
                            job: updated_job.clone(),
                            error: error_msg,
                            attempt: job.attempt,
                            next_run_at: updated_job.run_at,
                        }),
                    })
                } else {
                    Ok(CompletionOutcome::Applied { event: None })
                }
            }
        }
    }
}

/// Dispatch a lifecycle event to all registered handlers for a job kind.
///
/// Handlers are called sequentially. Panics are caught and logged — a
/// misbehaving handler cannot crash the dispatch loop or lose events
/// for subsequent handlers.
async fn dispatch_lifecycle_event(
    handlers: &HashMap<String, Vec<BoxedUntypedEventHandler>>,
    kind: &str,
    event: UntypedJobEvent,
) {
    if let Some(handlers) = handlers.get(kind) {
        for handler in handlers {
            let handler = handler.clone();
            let event = event.clone();
            let result = tokio::spawn(async move {
                (handler)(event).await;
            })
            .await;
            if let Err(err) = result {
                tracing::warn!(
                    kind,
                    error = %err,
                    "Lifecycle event handler panicked"
                );
            }
        }
    }
}

/// Apply a terminal failure — either route the job into the DLQ (when enabled
/// for this queue) or mark it `failed` in place. Both paths are lease-guarded.
///
/// Stale outcomes (another rescue or cancel won the race) return
/// `CompletionOutcome::IgnoredStale` so dispatchers skip lifecycle events and
/// duplicate metrics.
#[allow(clippy::too_many_arguments)]
async fn apply_terminal_failure(
    pool: &PgPool,
    job: &JobRow,
    error_json: serde_json::Value,
    progress_snapshot: Option<&serde_json::Value>,
    dlq_enabled: bool,
    dlq_reason: &str,
    error_msg: &str,
    needs_event: bool,
    metrics: &crate::metrics::AwaMetrics,
) -> Result<CompletionOutcome, AwaError> {
    if dlq_enabled {
        // Atomic, lease-guarded move from jobs_hot to jobs_dlq. The SQL
        // function uses the same (state='running' AND run_lease=lease)
        // guard that the in-place UPDATE below uses, so concurrent rescue
        // wins the same way.
        let moved: Option<awa_model::dlq::DlqRow> =
            sqlx::query_as("SELECT * FROM awa.move_to_dlq_guarded($1, $2, $3, $4, $5)")
                .bind(job.id)
                .bind(job.run_lease)
                .bind(dlq_reason)
                .bind(&error_json)
                .bind(progress_snapshot)
                .fetch_optional(pool)
                .await?;

        let Some(dlq_row) = moved else {
            warn!(
                job_id = job.id,
                "Job already rescued/cancelled, DLQ move ignored"
            );
            return Ok(CompletionOutcome::IgnoredStale);
        };

        metrics.record_dlq_moved(&job.kind, &job.queue, dlq_reason);

        if needs_event {
            // Reconstruct a JobRow from the DLQ row so lifecycle handlers see
            // the post-finalisation state (run_lease=0, NULL heartbeat/deadline/
            // callbacks, updated errors + preserved progress) rather than the
            // pre-claim snapshot.
            let updated_job = JobRow {
                id: dlq_row.id,
                kind: dlq_row.kind,
                queue: dlq_row.queue,
                args: dlq_row.args,
                state: dlq_row.state,
                priority: dlq_row.priority,
                attempt: dlq_row.attempt,
                run_lease: dlq_row.run_lease,
                max_attempts: dlq_row.max_attempts,
                run_at: dlq_row.run_at,
                heartbeat_at: dlq_row.heartbeat_at,
                deadline_at: dlq_row.deadline_at,
                attempted_at: dlq_row.attempted_at,
                finalized_at: dlq_row.finalized_at,
                created_at: dlq_row.created_at,
                errors: dlq_row.errors,
                metadata: dlq_row.metadata,
                tags: dlq_row.tags,
                unique_key: dlq_row.unique_key,
                unique_states: dlq_row.unique_states,
                callback_id: dlq_row.callback_id,
                callback_timeout_at: dlq_row.callback_timeout_at,
                callback_filter: dlq_row.callback_filter,
                callback_on_complete: dlq_row.callback_on_complete,
                callback_on_fail: dlq_row.callback_on_fail,
                callback_transform: dlq_row.callback_transform,
                progress: dlq_row.progress,
            };
            return Ok(CompletionOutcome::Applied {
                event: Some(UntypedJobEvent::Exhausted {
                    job: updated_job,
                    error: error_msg.to_string(),
                    attempt: job.attempt,
                }),
            });
        }
        return Ok(CompletionOutcome::Applied { event: None });
    }

    let result = sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'failed',
            finalized_at = now(),
            errors = errors || $2::jsonb,
            progress = $4
        WHERE id = $1 AND state = 'running' AND run_lease = $3
        "#,
    )
    .bind(job.id)
    .bind(&error_json)
    .bind(job.run_lease)
    .bind(progress_snapshot)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        warn!(
            job_id = job.id,
            "Job already rescued/cancelled, terminal failure ignored"
        );
        return Ok(CompletionOutcome::IgnoredStale);
    }
    if needs_event {
        let updated_job: JobRow = sqlx::query_as("SELECT * FROM awa.jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(pool)
            .await?;
        return Ok(CompletionOutcome::Applied {
            event: Some(UntypedJobEvent::Exhausted {
                job: updated_job,
                error: error_msg.to_string(),
                attempt: job.attempt,
            }),
        });
    }
    Ok(CompletionOutcome::Applied { event: None })
}

async fn direct_complete_job(pool: &PgPool, job: &JobRow) -> Result<bool, AwaError> {
    let result = sqlx::query(
        r#"
        UPDATE awa.jobs_hot
        SET state = 'completed',
            finalized_at = now(),
            progress = NULL
        WHERE id = $1 AND state = 'running' AND run_lease = $2
        "#,
    )
    .bind(job.id)
    .bind(job.run_lease)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}
