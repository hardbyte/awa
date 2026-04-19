use crate::completion::CompletionBatcherHandle;
use crate::context::{CallbackGuard, JobContext};
use crate::events::{BoxedUntypedEventHandler, UntypedJobEvent};
use crate::runtime::{InFlightMap, InFlightState, ProgressState};
use crate::storage::{QueueStorageRuntime, RuntimeStorage};
use awa_model::{AwaError, ClaimedEntry, JobRow, JobState};
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

/// Per-queue DLQ policy resolved at `Client::start`.
#[derive(Debug, Clone, Default)]
pub struct DlqPolicy {
    pub enabled_default: bool,
    pub overrides: Arc<HashMap<String, bool>>,
}

impl DlqPolicy {
    pub fn new(enabled_default: bool, overrides: HashMap<String, bool>) -> Self {
        Self {
            enabled_default,
            overrides: Arc::new(overrides),
        }
    }

    pub fn enabled_for(&self, queue: &str) -> bool {
        self.overrides
            .get(queue)
            .copied()
            .unwrap_or(self.enabled_default)
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
    Applied {
        event: Option<UntypedJobEvent>,
        terminal: bool,
    },
    /// The job was already rescued/cancelled — stale completion, no event.
    IgnoredStale,
}

#[derive(Debug, Clone)]
pub(crate) struct DispatchedJob {
    pub job: JobRow,
    pub queue_storage_claim: Option<ClaimedEntry>,
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
    storage: RuntimeStorage,
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
        storage: RuntimeStorage,
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
            storage,
            dlq_policy,
        }
    }

    /// Build the future that executes a claimed job.
    ///
    /// The caller is responsible for spawning it onto the runtime.
    pub(crate) fn execute_task(
        &self,
        dispatched: DispatchedJob,
        cancel: Arc<AtomicBool>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let job = dispatched.job;
        let queue_storage_claim = dispatched.queue_storage_claim;
        let pool = self.pool.clone();
        let workers = self.workers.clone();
        let lifecycle_handlers = self.lifecycle_handlers.clone();
        let in_flight = self.in_flight.clone();
        let queue_in_flight = self.queue_in_flight.clone();
        let state = self.state.clone();
        let metrics = self.metrics.clone();
        let completion_batcher = self.completion_batcher.clone();
        let storage = self.storage.clone();
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
                storage.clone(),
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
                queue_storage_claim.as_ref(),
                &result,
                &completion_batcher,
                progress_snapshot,
                duration,
                has_lifecycle_handlers,
                &storage,
                dlq_enabled,
                &metrics,
            )
            .await;

            match &outcome {
                Ok(CompletionOutcome::Applied { terminal, .. }) => {
                    // State transition succeeded — record metrics. `terminal`
                    // is the source of truth for retry-vs-failure because
                    // JobError::Retryable can resolve to either path.
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
                            if *terminal {
                                metrics.record_job_failed(&job_kind, &job_queue, true);
                            } else {
                                metrics.jobs_waiting_external.add(
                                    1,
                                    &[
                                        opentelemetry::KeyValue::new(
                                            "awa.job.kind",
                                            job_kind.clone(),
                                        ),
                                        opentelemetry::KeyValue::new(
                                            "awa.job.queue",
                                            job_queue.clone(),
                                        ),
                                    ],
                                );
                            }
                        }
                        Err(JobError::Terminal(_)) => {
                            metrics.record_job_failed(&job_kind, &job_queue, true);
                        }
                        Err(JobError::Retryable(_)) => {
                            if *terminal {
                                metrics.record_job_failed(&job_kind, &job_queue, true);
                            } else {
                                metrics.record_job_retried(&job_kind, &job_queue);
                            }
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
async fn complete_job(
    pool: &PgPool,
    job: &JobRow,
    queue_storage_claim: Option<&ClaimedEntry>,
    result: &Result<JobResult, JobError>,
    completion_batcher: &CompletionBatcherHandle,
    progress_snapshot: Option<serde_json::Value>,
    duration: Duration,
    needs_event: bool,
    storage: &RuntimeStorage,
    dlq_enabled: bool,
    metrics: &crate::metrics::AwaMetrics,
) -> Result<CompletionOutcome, AwaError> {
    match storage {
        RuntimeStorage::Canonical => {
            complete_job_canonical(
                pool,
                job,
                result,
                completion_batcher,
                progress_snapshot,
                duration,
                needs_event,
                dlq_enabled,
                metrics,
            )
            .await
        }
        RuntimeStorage::QueueStorage(runtime) => {
            complete_job_queue_storage(
                runtime,
                pool,
                job,
                queue_storage_claim,
                result,
                completion_batcher,
                progress_snapshot,
                duration,
                needs_event,
                dlq_enabled,
                metrics,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn complete_job_canonical(
    pool: &PgPool,
    job: &JobRow,
    result: &Result<JobResult, JobError>,
    completion_batcher: &CompletionBatcherHandle,
    progress_snapshot: Option<serde_json::Value>,
    duration: Duration,
    needs_event: bool,
    _dlq_enabled: bool,
    _metrics: &crate::metrics::AwaMetrics,
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
                    terminal: false,
                })
            } else {
                Ok(CompletionOutcome::Applied {
                    event: None,
                    terminal: false,
                })
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
                    terminal: false,
                })
            } else {
                Ok(CompletionOutcome::Applied {
                    event: None,
                    terminal: false,
                })
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
            Ok(CompletionOutcome::Applied {
                event: None,
                terminal: false,
            })
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
                    terminal: false,
                })
            } else {
                Ok(CompletionOutcome::Applied {
                    event: None,
                    terminal: false,
                })
            }
        }

        Ok(JobResult::WaitForCallback(_guard)) => {
            info!(
                job_id = job.id,
                kind = %job.kind,
                "Job waiting for external callback"
            );
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
                let current: Option<(JobState, Option<uuid::Uuid>)> =
                    sqlx::query_as("SELECT state, callback_id FROM awa.jobs WHERE id = $1")
                        .bind(job.id)
                        .fetch_optional(pool)
                        .await?;
                match current {
                    Some((state, _)) if state.is_terminal() => {
                        info!(
                            job_id = job.id,
                            state = %state,
                            "Job already completed by racing callback"
                        );
                        return Ok(CompletionOutcome::Applied {
                            event: None,
                            terminal: false,
                        });
                    }
                    Some((_, None)) => {
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
                        return Ok(CompletionOutcome::Applied {
                            event: None,
                            terminal: true,
                        });
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
            Ok(CompletionOutcome::Applied {
                event: None,
                terminal: false,
            })
        }

        Err(JobError::Terminal(msg)) => {
            tracing::Span::current().record("otel.status_code", "ERROR");
            error!(
                job_id = job.id,
                kind = %job.kind,
                error = %msg,
                "Job failed terminally"
            );
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
            .bind(serde_json::json!({
                "error": msg.to_string(),
                "attempt": job.attempt,
                "at": chrono::Utc::now().to_rfc3339(),
                "terminal": true
            }))
            .bind(job.run_lease)
            .bind(&progress_snapshot)
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
                Ok(CompletionOutcome::Applied {
                    event: Some(UntypedJobEvent::Exhausted {
                        job: updated_job,
                        error: msg.clone(),
                        attempt: job.attempt,
                    }),
                    terminal: true,
                })
            } else {
                Ok(CompletionOutcome::Applied {
                    event: None,
                    terminal: true,
                })
            }
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
                        "Job already rescued/cancelled, failure ignored"
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
                        event: Some(UntypedJobEvent::Exhausted {
                            job: updated_job,
                            error: error_msg,
                            attempt: job.attempt,
                        }),
                        terminal: true,
                    })
                } else {
                    Ok(CompletionOutcome::Applied {
                        event: None,
                        terminal: true,
                    })
                }
            } else {
                warn!(
                    job_id = job.id,
                    kind = %job.kind,
                    attempt = job.attempt,
                    error = %error_msg,
                    "Job failed (will retry)"
                );
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
                        terminal: false,
                    })
                } else {
                    Ok(CompletionOutcome::Applied {
                        event: None,
                        terminal: false,
                    })
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn complete_job_queue_storage(
    runtime: &QueueStorageRuntime,
    pool: &PgPool,
    job: &JobRow,
    queue_storage_claim: Option<&ClaimedEntry>,
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
            let updated = match match queue_storage_claim {
                Some(claim) => {
                    completion_batcher
                        .complete_claimed(job.id, job.run_lease, claim.clone())
                        .await
                }
                None => completion_batcher.complete(job.id, job.run_lease).await,
            } {
                Ok(updated) => updated,
                Err(err) => {
                    warn!(
                        job_id = job.id,
                        error = %err,
                        "Completion batch flush failed, falling back to direct finalize"
                    );
                    direct_complete_job_queue_storage(runtime, pool, job, queue_storage_claim)
                        .await?
                }
            };
            if !updated {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, completion ignored"
                );
                return Ok(CompletionOutcome::IgnoredStale);
            }
            if needs_event {
                let updated_job =
                    runtime
                        .store
                        .load_job(pool, job.id)
                        .await?
                        .unwrap_or_else(|| {
                            let mut completed_job = job.clone();
                            completed_job.state = JobState::Completed;
                            completed_job.finalized_at = Some(chrono::Utc::now());
                            completed_job.progress = None;
                            completed_job
                        });
                Ok(CompletionOutcome::Applied {
                    event: Some(UntypedJobEvent::Completed {
                        job: updated_job,
                        duration,
                    }),
                    terminal: false,
                })
            } else {
                Ok(CompletionOutcome::Applied {
                    event: None,
                    terminal: false,
                })
            }
        }

        Ok(JobResult::RetryAfter(retry_duration)) => {
            info!(
                job_id = job.id,
                kind = %job.kind,
                retry_after_secs = retry_duration.as_secs_f64(),
                "Job requested retry after duration"
            );
            let Some(updated_job) = runtime
                .store
                .retry_after(
                    pool,
                    job.id,
                    job.run_lease,
                    *retry_duration,
                    progress_snapshot.clone(),
                )
                .await?
            else {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, retry ignored"
                );
                return Ok(CompletionOutcome::IgnoredStale);
            };
            if needs_event {
                Ok(CompletionOutcome::Applied {
                    event: Some(UntypedJobEvent::Retried {
                        job: updated_job.clone(),
                        error: String::new(),
                        attempt: updated_job.attempt,
                        next_run_at: updated_job.run_at,
                    }),
                    terminal: false,
                })
            } else {
                Ok(CompletionOutcome::Applied {
                    event: None,
                    terminal: false,
                })
            }
        }

        Ok(JobResult::Snooze(snooze_duration)) => {
            info!(
                job_id = job.id,
                kind = %job.kind,
                snooze_secs = snooze_duration.as_secs_f64(),
                "Job snoozed (attempt not incremented)"
            );
            let updated = runtime
                .store
                .snooze(
                    pool,
                    job.id,
                    job.run_lease,
                    *snooze_duration,
                    progress_snapshot.clone(),
                )
                .await?;
            if updated.is_none() {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, snooze ignored"
                );
                return Ok(CompletionOutcome::IgnoredStale);
            }
            Ok(CompletionOutcome::Applied {
                event: None,
                terminal: false,
            })
        }

        Ok(JobResult::Cancel(reason)) => {
            info!(
                job_id = job.id,
                kind = %job.kind,
                reason = %reason,
                "Job cancelled by handler"
            );
            let Some(updated_job) = runtime
                .store
                .cancel_running(pool, job.id, job.run_lease, progress_snapshot.clone())
                .await?
            else {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, cancel ignored"
                );
                return Ok(CompletionOutcome::IgnoredStale);
            };
            if needs_event {
                Ok(CompletionOutcome::Applied {
                    event: Some(UntypedJobEvent::Cancelled {
                        job: updated_job,
                        reason: reason.clone(),
                    }),
                    terminal: false,
                })
            } else {
                Ok(CompletionOutcome::Applied {
                    event: None,
                    terminal: false,
                })
            }
        }

        Ok(JobResult::WaitForCallback(guard)) => {
            info!(
                job_id = job.id,
                kind = %job.kind,
                "Job waiting for external callback"
            );
            let entered = runtime
                .store
                .enter_callback_wait(pool, job.id, job.run_lease, guard.id())
                .await?;
            if !entered {
                let current = runtime.store.load_job(pool, job.id).await?;
                match current {
                    Some(current) if current.state.is_terminal() => {
                        info!(
                            job_id = job.id,
                            state = %current.state,
                            "Job already completed by racing callback"
                        );
                        return Ok(CompletionOutcome::Applied {
                            event: None,
                            terminal: false,
                        });
                    }
                    Some(current)
                        if current.state == JobState::Running && current.callback_id.is_none() =>
                    {
                        error!(
                            job_id = job.id,
                            "WaitForCallback returned without calling register_callback"
                        );
                        let failed = if dlq_enabled {
                            let failed = runtime
                                .store
                                .fail_to_dlq(
                                    pool,
                                    job.id,
                                    job.run_lease,
                                    "wait_for_callback_contract_violation",
                                    "WaitForCallback returned without calling register_callback",
                                    progress_snapshot.clone(),
                                )
                                .await?;
                            if failed.is_some() {
                                metrics.record_dlq_moved(
                                    &job.kind,
                                    &job.queue,
                                    "wait_for_callback_contract_violation",
                                );
                            }
                            failed
                        } else {
                            runtime
                                .store
                                .fail_terminal(
                                    pool,
                                    job.id,
                                    job.run_lease,
                                    "WaitForCallback returned without calling register_callback",
                                    progress_snapshot.clone(),
                                )
                                .await?
                        };
                        if failed.is_none() {
                            return Ok(CompletionOutcome::IgnoredStale);
                        }
                        return Ok(CompletionOutcome::Applied {
                            event: None,
                            terminal: true,
                        });
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
            Ok(CompletionOutcome::Applied {
                event: None,
                terminal: false,
            })
        }

        Err(JobError::Terminal(msg)) => {
            tracing::Span::current().record("otel.status_code", "ERROR");
            error!(
                job_id = job.id,
                kind = %job.kind,
                error = %msg,
                "Job failed terminally"
            );
            let updated_job = if dlq_enabled {
                let moved = runtime
                    .store
                    .fail_to_dlq(
                        pool,
                        job.id,
                        job.run_lease,
                        "terminal_error",
                        msg,
                        progress_snapshot.clone(),
                    )
                    .await?;
                if moved.is_some() {
                    metrics.record_dlq_moved(&job.kind, &job.queue, "terminal_error");
                }
                moved
            } else {
                runtime
                    .store
                    .fail_terminal(pool, job.id, job.run_lease, msg, progress_snapshot.clone())
                    .await?
            };
            let Some(updated_job) = updated_job else {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, terminal failure ignored"
                );
                return Ok(CompletionOutcome::IgnoredStale);
            };
            if needs_event {
                Ok(CompletionOutcome::Applied {
                    event: Some(UntypedJobEvent::Exhausted {
                        job: updated_job,
                        error: msg.clone(),
                        attempt: job.attempt,
                    }),
                    terminal: true,
                })
            } else {
                Ok(CompletionOutcome::Applied {
                    event: None,
                    terminal: true,
                })
            }
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
                let updated_job = if dlq_enabled {
                    let moved = runtime
                        .store
                        .fail_to_dlq(
                            pool,
                            job.id,
                            job.run_lease,
                            "max_attempts_exhausted",
                            &error_msg,
                            progress_snapshot.clone(),
                        )
                        .await?;
                    if moved.is_some() {
                        metrics.record_dlq_moved(&job.kind, &job.queue, "max_attempts_exhausted");
                    }
                    moved
                } else {
                    runtime
                        .store
                        .fail_terminal(
                            pool,
                            job.id,
                            job.run_lease,
                            &error_msg,
                            progress_snapshot.clone(),
                        )
                        .await?
                };
                let Some(updated_job) = updated_job else {
                    warn!(
                        job_id = job.id,
                        "Job already rescued/cancelled, failure ignored"
                    );
                    return Ok(CompletionOutcome::IgnoredStale);
                };
                if needs_event {
                    Ok(CompletionOutcome::Applied {
                        event: Some(UntypedJobEvent::Exhausted {
                            job: updated_job,
                            error: error_msg,
                            attempt: job.attempt,
                        }),
                        terminal: true,
                    })
                } else {
                    Ok(CompletionOutcome::Applied {
                        event: None,
                        terminal: true,
                    })
                }
            } else {
                warn!(
                    job_id = job.id,
                    kind = %job.kind,
                    attempt = job.attempt,
                    error = %error_msg,
                    "Job failed (will retry)"
                );
                let Some(updated_job) = runtime
                    .store
                    .fail_retryable(
                        pool,
                        job.id,
                        job.run_lease,
                        &error_msg,
                        progress_snapshot.clone(),
                    )
                    .await?
                else {
                    warn!(
                        job_id = job.id,
                        "Job already rescued/cancelled, retry ignored"
                    );
                    return Ok(CompletionOutcome::IgnoredStale);
                };
                if needs_event {
                    Ok(CompletionOutcome::Applied {
                        event: Some(UntypedJobEvent::Retried {
                            job: updated_job.clone(),
                            error: error_msg,
                            attempt: job.attempt,
                            next_run_at: updated_job.run_at,
                        }),
                        terminal: false,
                    })
                } else {
                    Ok(CompletionOutcome::Applied {
                        event: None,
                        terminal: false,
                    })
                }
            }
        }
    }
}

async fn direct_complete_job_queue_storage(
    runtime: &QueueStorageRuntime,
    pool: &PgPool,
    job: &JobRow,
    queue_storage_claim: Option<&ClaimedEntry>,
) -> Result<bool, AwaError> {
    let updated = if let Some(claim) = queue_storage_claim {
        runtime.store.complete_claimed_batch(pool, std::slice::from_ref(claim)).await?
    } else {
        runtime
            .store
            .complete_job_batch_by_id(pool, &[(job.id, job.run_lease)])
            .await?
    };
    Ok(!updated.is_empty())
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
