use crate::completion::CompletionBatcherHandle;
use crate::context::{CallbackGuard, JobContext};
use crate::events::{BoxedUntypedEventHandler, UntypedJobEvent};
use crate::runtime::{InFlightMap, InFlightState, ProgressState};
use crate::storage::{QueueStorageRuntime, RuntimeStorage};
use awa_model::{AwaError, ClaimedEntry, ClaimedRuntimeJob, JobRow, JobState};
use sqlx::PgPool;
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, info_span, warn, Instrument};

/// Result of executing a job handler.
///
/// # Picking the right variant for re-runs
///
/// Three primitives can put the job back on the queue. They differ in
/// what they say about *why*:
///
/// | Primitive | Means | Increments `attempt` | Delay shape |
/// |-----------|-------|----------------------|-------------|
/// | [`JobError::Retryable`] | "this attempt failed; try again" | yes | DB-computed exponential backoff |
/// | [`JobResult::RetryAfter`] | "this attempt failed; try again after delay X" | yes | caller-specified |
/// | [`JobResult::Snooze`] | "this attempt didn't fail — it's just not time yet" | no | caller-specified |
///
/// Rule of thumb: if every "non-success" return is a "not yet" rather
/// than a failure (polling, waiting for an upstream signal, rate
/// limiting), use [`Snooze`] so `max_attempts` keeps its plain
/// meaning of bounding genuine failures. If the handler observed a
/// real failure and wants a specific retry delay rather than the
/// default exponential backoff, use [`RetryAfter`]. See
/// `awa/examples/poll_until_deadline.rs` for a deadline-bounded
/// polling example.
///
/// [`Snooze`]: JobResult::Snooze
/// [`RetryAfter`]: JobResult::RetryAfter
#[derive(Debug)]
pub enum JobResult {
    /// Job completed successfully.
    Completed,
    /// Job should be retried after the given duration. Increments
    /// `attempt`. Use when this attempt failed and you want a
    /// caller-specified delay instead of the default exponential
    /// backoff produced by [`JobError::Retryable`].
    RetryAfter(std::time::Duration),
    /// Job should be re-scheduled after the given duration without
    /// counting as a failed attempt. Use for polling-style waits
    /// where each "not yet" probe is normal — `max_attempts` should
    /// only bound genuine handler failures, not the polling cadence.
    Snooze(std::time::Duration),
    /// Job should be cancelled. Records the reason in the job's
    /// `errors` column and sets state to `cancelled` — no DLQ, no
    /// failure event. Use for graceful give-up (e.g. handler-side
    /// deadline expiry, user-requested abort).
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
/// For explicit caller-controlled retry delay, return `Ok(`[`JobResult::RetryAfter`]`)` instead.
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
    pub queue_storage_unique_states: Option<String>,
}

/// Manages job execution — spawns worker futures and tracks in-flight jobs.
pub struct JobExecutor {
    pool: PgPool,
    workers: Arc<HashMap<String, BoxedWorker>>,
    lifecycle_handlers: Arc<HashMap<String, Vec<BoxedUntypedEventHandler>>>,
    enqueue_specs: Arc<
        HashMap<
            crate::enqueue_specs::Outcome,
            HashMap<String, Vec<crate::enqueue_specs::BoxedEnqueueSpec>>,
        >,
    >,
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
        enqueue_specs: Arc<
            HashMap<
                crate::enqueue_specs::Outcome,
                HashMap<String, Vec<crate::enqueue_specs::BoxedEnqueueSpec>>,
            >,
        >,
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
            enqueue_specs,
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
        let queue_storage_unique_states = dispatched.queue_storage_unique_states;
        let pool = self.pool.clone();
        let workers = self.workers.clone();
        let lifecycle_handlers = self.lifecycle_handlers.clone();
        let enqueue_specs = self.enqueue_specs.clone();
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
            let has_lifecycle_handlers = lifecycle_handlers.contains_key(&job_kind);

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
                Some(worker) => {
                    if has_lifecycle_handlers {
                        let started_handlers = lifecycle_handlers.clone();
                        let started_kind = job_kind.clone();
                        let started_job = job.clone();
                        tokio::spawn(async move {
                            dispatch_lifecycle_event(
                                &started_handlers,
                                &started_kind,
                                UntypedJobEvent::Started { job: started_job },
                            )
                            .await;
                        });
                    }
                    worker.perform(&ctx).await
                }
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

            // Remove from in-flight immediately after the handler returns and
            // the progress snapshot is captured. This keeps local worker
            // capacity tied to active handler execution, not to the tail
            // latency of durable completion bookkeeping.
            in_flight.remove((job_id, job_run_lease));
            if let Some(counter) = queue_in_flight.get(&job_queue) {
                counter.fetch_sub(1, Ordering::SeqCst);
            }
            metrics.record_in_flight_change(&job_queue, -1);

            let dlq_enabled = dlq_policy.enabled_for(&job_queue);
            tokio::spawn(async move {
                let outcome = complete_job(
                    &pool,
                    &job,
                    queue_storage_claim.as_ref(),
                    queue_storage_unique_states.as_deref(),
                    &result,
                    &completion_batcher,
                    progress_snapshot,
                    duration,
                    has_lifecycle_handlers,
                    &enqueue_specs,
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
                            Ok(JobResult::Snooze(_)) => {}
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
                    Ok(CompletionOutcome::IgnoredStale) => {}
                    Err(err) => {
                        error!(job_id, error = %err, "Failed to complete job");
                    }
                }

                if let Ok(CompletionOutcome::Applied {
                    event: Some(event), ..
                }) = outcome
                {
                    dispatch_lifecycle_event(&lifecycle_handlers, &job_kind, event).await;
                }
            });
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
    queue_storage_claim: Option<&ClaimedEntry>,
    queue_storage_unique_states: Option<&str>,
    result: &Result<JobResult, JobError>,
    completion_batcher: &CompletionBatcherHandle,
    progress_snapshot: Option<serde_json::Value>,
    duration: Duration,
    needs_event: bool,
    enqueue_specs: &Arc<
        HashMap<
            crate::enqueue_specs::Outcome,
            HashMap<String, Vec<crate::enqueue_specs::BoxedEnqueueSpec>>,
        >,
    >,
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
                enqueue_specs,
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
                queue_storage_unique_states,
                result,
                completion_batcher,
                progress_snapshot,
                duration,
                needs_event,
                enqueue_specs,
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
    enqueue_specs: &Arc<
        HashMap<
            crate::enqueue_specs::Outcome,
            HashMap<String, Vec<crate::enqueue_specs::BoxedEnqueueSpec>>,
        >,
    >,
    _dlq_enabled: bool,
    _metrics: &crate::metrics::AwaMetrics,
) -> Result<CompletionOutcome, AwaError> {
    match result {
        Ok(JobResult::Completed) => {
            tracing::Span::current().record("otel.status_code", "OK");
            info!(job_id = job.id, kind = %job.kind, attempt = job.attempt, "Job completed");

            // ADR-029: when this kind has follow-up specs registered, drive
            // completion through a dedicated transaction so the UPDATE and
            // the follow-up INSERTs commit atomically. The batched path
            // can't carry per-job follow-ups, so we bypass it here.
            let kind_specs = enqueue_specs
                .get(&crate::enqueue_specs::Outcome::Completed)
                .and_then(|by_kind| by_kind.get(&job.kind))
                .cloned();
            if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                let outcome = complete_canonical_with_followups(pool, job, &specs).await?;
                return match outcome {
                    None => {
                        warn!(
                            job_id = job.id,
                            "Job already rescued/cancelled, completion ignored"
                        );
                        Ok(CompletionOutcome::IgnoredStale)
                    }
                    Some(updated_job) => {
                        let event = if needs_event {
                            Some(UntypedJobEvent::Completed {
                                job: updated_job,
                                duration,
                            })
                        } else {
                            None
                        };
                        Ok(CompletionOutcome::Applied {
                            event,
                            terminal: false,
                        })
                    }
                };
            }

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

            // ADR-029: caller-requested retry. The Retried OutcomeContext
            // carries an empty error string (parity with the event emitted
            // below) and the attempt / next_run_at read from the post-UPDATE
            // row.
            let kind_specs = enqueue_specs
                .get(&crate::enqueue_specs::Outcome::Retried)
                .and_then(|by_kind| by_kind.get(&job.kind))
                .cloned();
            if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                let result = retry_after_canonical_with_followups(
                    pool,
                    job,
                    seconds,
                    progress_snapshot.as_ref(),
                    &specs,
                )
                .await?;
                return match result {
                    None => {
                        warn!(
                            job_id = job.id,
                            "Job already rescued/cancelled, retry ignored"
                        );
                        Ok(CompletionOutcome::IgnoredStale)
                    }
                    Some(updated_job) => {
                        let event = if needs_event {
                            Some(UntypedJobEvent::Retried {
                                job: updated_job.clone(),
                                error: String::new(),
                                attempt: updated_job.attempt,
                                next_run_at: updated_job.run_at,
                            })
                        } else {
                            None
                        };
                        Ok(CompletionOutcome::Applied {
                            event,
                            terminal: false,
                        })
                    }
                };
            }

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
            tracing::Span::current().record("otel.status_code", "OK");
            info!(
                job_id = job.id,
                kind = %job.kind,
                reason = %reason,
                "Job cancelled by handler"
            );

            // ADR-029: when this kind has Cancelled specs registered, drive
            // the cancellation through a transaction so the UPDATE and the
            // follow-up `INSERT`s commit atomically.
            let kind_specs = enqueue_specs
                .get(&crate::enqueue_specs::Outcome::Cancelled)
                .and_then(|by_kind| by_kind.get(&job.kind))
                .cloned();
            let outcome_ctx = crate::enqueue_specs::OutcomeContext::Cancelled {
                reason: reason.clone(),
            };
            if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                let result = cancel_canonical_with_followups(
                    pool,
                    job,
                    reason,
                    progress_snapshot.as_ref(),
                    &specs,
                    &outcome_ctx,
                )
                .await?;
                return match result {
                    None => {
                        warn!(
                            job_id = job.id,
                            "Job already rescued/cancelled, cancel ignored"
                        );
                        Ok(CompletionOutcome::IgnoredStale)
                    }
                    Some(updated_job) => {
                        let event = if needs_event {
                            Some(UntypedJobEvent::Cancelled {
                                job: updated_job,
                                reason: reason.clone(),
                            })
                        } else {
                            None
                        };
                        Ok(CompletionOutcome::Applied {
                            event,
                            terminal: false,
                        })
                    }
                };
            }

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

            // ADR-029: WaitingForCallback follow-up enqueue. The triggering
            // UPDATE and the follow-up INSERTs commit in the same transaction.
            // Race / missing-callback paths below are unchanged and dispatch
            // no follow-ups because the row didn't actually park.
            let kind_specs = enqueue_specs
                .get(&crate::enqueue_specs::Outcome::WaitingForCallback)
                .and_then(|by_kind| by_kind.get(&job.kind))
                .cloned();
            if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                let parked =
                    park_canonical_with_followups(pool, job, progress_snapshot.as_ref(), &specs)
                        .await?;
                if let Some(parked_job) = parked {
                    let event = if needs_event {
                        Some(UntypedJobEvent::WaitingForCallback { job: parked_job })
                    } else {
                        None
                    };
                    return Ok(CompletionOutcome::Applied {
                        event,
                        terminal: false,
                    });
                }
                // Fall through to the existing rows_affected == 0 handling.
            }

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
                        let result = sqlx::query(
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
                        if result.rows_affected() == 0 {
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
            let event = if needs_event {
                let parked_job: JobRow = sqlx::query_as("SELECT * FROM awa.jobs WHERE id = $1")
                    .bind(job.id)
                    .fetch_one(pool)
                    .await?;
                Some(UntypedJobEvent::WaitingForCallback { job: parked_job })
            } else {
                None
            };
            Ok(CompletionOutcome::Applied {
                event,
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

            // ADR-029: terminal error counts as Exhausted (the error is
            // fatal; no further attempts will run).
            let kind_specs = enqueue_specs
                .get(&crate::enqueue_specs::Outcome::Exhausted)
                .and_then(|by_kind| by_kind.get(&job.kind))
                .cloned();
            if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                let result = terminal_canonical_with_followups(
                    pool,
                    job,
                    msg,
                    progress_snapshot.as_ref(),
                    &specs,
                )
                .await?;
                return match result {
                    None => {
                        warn!(
                            job_id = job.id,
                            "Job already rescued/cancelled, terminal failure ignored"
                        );
                        Ok(CompletionOutcome::IgnoredStale)
                    }
                    Some(updated_job) => {
                        let event = if needs_event {
                            Some(UntypedJobEvent::Exhausted {
                                job: updated_job,
                                error: msg.clone(),
                                attempt: job.attempt,
                            })
                        } else {
                            None
                        };
                        Ok(CompletionOutcome::Applied {
                            event,
                            terminal: true,
                        })
                    }
                };
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

                // ADR-029: retries exhausted -> Exhausted outcome.
                let kind_specs = enqueue_specs
                    .get(&crate::enqueue_specs::Outcome::Exhausted)
                    .and_then(|by_kind| by_kind.get(&job.kind))
                    .cloned();
                if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                    let result = exhaust_canonical_with_followups(
                        pool,
                        job,
                        &error_msg,
                        progress_snapshot.as_ref(),
                        &specs,
                    )
                    .await?;
                    return match result {
                        None => {
                            warn!(
                                job_id = job.id,
                                "Job already rescued/cancelled, failure ignored"
                            );
                            Ok(CompletionOutcome::IgnoredStale)
                        }
                        Some(updated_job) => {
                            let event = if needs_event {
                                Some(UntypedJobEvent::Exhausted {
                                    job: updated_job,
                                    error: error_msg,
                                    attempt: job.attempt,
                                })
                            } else {
                                None
                            };
                            Ok(CompletionOutcome::Applied {
                                event,
                                terminal: true,
                            })
                        }
                    };
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

                // ADR-029: retryable error with backoff -> Retried outcome.
                let kind_specs = enqueue_specs
                    .get(&crate::enqueue_specs::Outcome::Retried)
                    .and_then(|by_kind| by_kind.get(&job.kind))
                    .cloned();
                if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                    let result = retry_backoff_canonical_with_followups(
                        pool,
                        job,
                        &error_msg,
                        progress_snapshot.as_ref(),
                        &specs,
                    )
                    .await?;
                    return match result {
                        None => {
                            warn!(
                                job_id = job.id,
                                "Job already rescued/cancelled, retry ignored"
                            );
                            Ok(CompletionOutcome::IgnoredStale)
                        }
                        Some(updated_job) => {
                            let event = if needs_event {
                                Some(UntypedJobEvent::Retried {
                                    job: updated_job.clone(),
                                    error: error_msg,
                                    attempt: job.attempt,
                                    next_run_at: updated_job.run_at,
                                })
                            } else {
                                None
                            };
                            Ok(CompletionOutcome::Applied {
                                event,
                                terminal: false,
                            })
                        }
                    };
                }

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
    queue_storage_unique_states: Option<&str>,
    result: &Result<JobResult, JobError>,
    completion_batcher: &CompletionBatcherHandle,
    progress_snapshot: Option<serde_json::Value>,
    duration: Duration,
    needs_event: bool,
    enqueue_specs: &Arc<
        HashMap<
            crate::enqueue_specs::Outcome,
            HashMap<String, Vec<crate::enqueue_specs::BoxedEnqueueSpec>>,
        >,
    >,
    dlq_enabled: bool,
    metrics: &crate::metrics::AwaMetrics,
) -> Result<CompletionOutcome, AwaError> {
    match result {
        Ok(JobResult::Completed) => {
            tracing::Span::current().record("otel.status_code", "OK");
            info!(job_id = job.id, kind = %job.kind, attempt = job.attempt, "Job completed");

            // ADR-029: when this kind has follow-up specs registered, drive
            // completion through a dedicated transaction so the
            // receipt-plane / lease cleanup + done_entries append +
            // follow-up `INSERT`s commit atomically. The receipt-plane
            // fast-complete (ADR-023) can't carry per-job follow-ups, so we
            // bypass it for spec'd jobs and use the slow path's tx-aware
            // variant.
            let kind_specs = enqueue_specs
                .get(&crate::enqueue_specs::Outcome::Completed)
                .and_then(|by_kind| by_kind.get(&job.kind))
                .cloned();
            if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                let outcome = complete_queue_storage_with_followups(
                    runtime,
                    pool,
                    job,
                    queue_storage_claim,
                    queue_storage_unique_states,
                    &specs,
                )
                .await?;
                return match outcome {
                    None => {
                        warn!(
                            job_id = job.id,
                            "Job already rescued/cancelled, completion ignored"
                        );
                        Ok(CompletionOutcome::IgnoredStale)
                    }
                    Some(updated_job) => {
                        let event = if needs_event {
                            Some(UntypedJobEvent::Completed {
                                job: updated_job,
                                duration,
                            })
                        } else {
                            None
                        };
                        Ok(CompletionOutcome::Applied {
                            event,
                            terminal: false,
                        })
                    }
                };
            }

            let updated = match match queue_storage_claim {
                Some(claim) => {
                    completion_batcher
                        .complete_runtime_job(ClaimedRuntimeJob {
                            claim: claim.clone(),
                            job: job.clone(),
                            unique_states: queue_storage_unique_states
                                .map(std::string::ToString::to_string),
                        })
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
                    direct_complete_job_queue_storage(
                        runtime,
                        pool,
                        job,
                        queue_storage_claim,
                        queue_storage_unique_states,
                    )
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

            // ADR-029: caller-requested retry on queue storage.
            let kind_specs = enqueue_specs
                .get(&crate::enqueue_specs::Outcome::Retried)
                .and_then(|by_kind| by_kind.get(&job.kind))
                .cloned();
            if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                let result = retry_after_queue_storage_with_followups(
                    runtime,
                    pool,
                    job,
                    *retry_duration,
                    progress_snapshot.clone(),
                    &specs,
                )
                .await?;
                return match result {
                    None => {
                        warn!(
                            job_id = job.id,
                            "Job already rescued/cancelled, retry ignored"
                        );
                        Ok(CompletionOutcome::IgnoredStale)
                    }
                    Some(updated_job) => {
                        let event = if needs_event {
                            Some(UntypedJobEvent::Retried {
                                job: updated_job.clone(),
                                error: String::new(),
                                attempt: updated_job.attempt,
                                next_run_at: updated_job.run_at,
                            })
                        } else {
                            None
                        };
                        Ok(CompletionOutcome::Applied {
                            event,
                            terminal: false,
                        })
                    }
                };
            }

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

            // ADR-029: queue-storage Cancelled follow-ups.
            let kind_specs = enqueue_specs
                .get(&crate::enqueue_specs::Outcome::Cancelled)
                .and_then(|by_kind| by_kind.get(&job.kind))
                .cloned();
            if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                let result = cancel_queue_storage_with_followups(
                    runtime,
                    pool,
                    job,
                    reason,
                    progress_snapshot.clone(),
                    &specs,
                )
                .await?;
                return match result {
                    None => {
                        warn!(
                            job_id = job.id,
                            "Job already rescued/cancelled, cancel ignored"
                        );
                        Ok(CompletionOutcome::IgnoredStale)
                    }
                    Some(updated_job) => {
                        let event = if needs_event {
                            Some(UntypedJobEvent::Cancelled {
                                job: updated_job,
                                reason: reason.clone(),
                            })
                        } else {
                            None
                        };
                        Ok(CompletionOutcome::Applied {
                            event,
                            terminal: false,
                        })
                    }
                };
            }

            let Some(updated_job) = runtime
                .store
                .cancel_running(
                    pool,
                    job.id,
                    job.run_lease,
                    reason,
                    progress_snapshot.clone(),
                )
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

            // ADR-029: queue-storage WaitingForCallback follow-ups. Race /
            // missing-callback paths below remain unchanged — they dispatch
            // no follow-ups because the row didn't actually park.
            let kind_specs = enqueue_specs
                .get(&crate::enqueue_specs::Outcome::WaitingForCallback)
                .and_then(|by_kind| by_kind.get(&job.kind))
                .cloned();
            if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                let parked =
                    park_queue_storage_with_followups(runtime, pool, job, guard.id(), &specs)
                        .await?;
                if let Some(parked_job) = parked {
                    let event = if needs_event {
                        Some(UntypedJobEvent::WaitingForCallback { job: parked_job })
                    } else {
                        None
                    };
                    return Ok(CompletionOutcome::Applied {
                        event,
                        terminal: false,
                    });
                }
                // Fall through to existing rows_affected == 0 handling.
            }

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
            let event = if needs_event {
                let parked_job = runtime
                    .store
                    .load_job(pool, job.id)
                    .await?
                    .unwrap_or_else(|| {
                        let mut parked = job.clone();
                        parked.state = JobState::WaitingExternal;
                        parked
                    });
                Some(UntypedJobEvent::WaitingForCallback { job: parked_job })
            } else {
                None
            };
            Ok(CompletionOutcome::Applied {
                event,
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

            // ADR-029: queue-storage Exhausted follow-ups (Terminal counts).
            let kind_specs = enqueue_specs
                .get(&crate::enqueue_specs::Outcome::Exhausted)
                .and_then(|by_kind| by_kind.get(&job.kind))
                .cloned();
            if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                let result = fail_queue_storage_with_followups(
                    runtime,
                    pool,
                    job,
                    "terminal_error",
                    msg,
                    progress_snapshot.clone(),
                    dlq_enabled,
                    metrics,
                    &specs,
                )
                .await?;
                return match result {
                    None => {
                        warn!(
                            job_id = job.id,
                            "Job already rescued/cancelled, terminal failure ignored"
                        );
                        Ok(CompletionOutcome::IgnoredStale)
                    }
                    Some(updated_job) => {
                        let event = if needs_event {
                            Some(UntypedJobEvent::Exhausted {
                                job: updated_job,
                                error: msg.clone(),
                                attempt: job.attempt,
                            })
                        } else {
                            None
                        };
                        Ok(CompletionOutcome::Applied {
                            event,
                            terminal: true,
                        })
                    }
                };
            }

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

                // ADR-029: retries exhausted -> Exhausted outcome.
                let kind_specs = enqueue_specs
                    .get(&crate::enqueue_specs::Outcome::Exhausted)
                    .and_then(|by_kind| by_kind.get(&job.kind))
                    .cloned();
                if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                    let result = fail_queue_storage_with_followups(
                        runtime,
                        pool,
                        job,
                        "max_attempts_exhausted",
                        &error_msg,
                        progress_snapshot.clone(),
                        dlq_enabled,
                        metrics,
                        &specs,
                    )
                    .await?;
                    return match result {
                        None => {
                            warn!(
                                job_id = job.id,
                                "Job already rescued/cancelled, failure ignored"
                            );
                            Ok(CompletionOutcome::IgnoredStale)
                        }
                        Some(updated_job) => {
                            let event = if needs_event {
                                Some(UntypedJobEvent::Exhausted {
                                    job: updated_job,
                                    error: error_msg,
                                    attempt: job.attempt,
                                })
                            } else {
                                None
                            };
                            Ok(CompletionOutcome::Applied {
                                event,
                                terminal: true,
                            })
                        }
                    };
                }

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

                // ADR-029: retryable error with backoff -> Retried outcome.
                let kind_specs = enqueue_specs
                    .get(&crate::enqueue_specs::Outcome::Retried)
                    .and_then(|by_kind| by_kind.get(&job.kind))
                    .cloned();
                if let Some(specs) = kind_specs.filter(|s| !s.is_empty()) {
                    let result = retry_backoff_queue_storage_with_followups(
                        runtime,
                        pool,
                        job,
                        &error_msg,
                        progress_snapshot.clone(),
                        &specs,
                    )
                    .await?;
                    return match result {
                        None => {
                            warn!(
                                job_id = job.id,
                                "Job already rescued/cancelled, retry ignored"
                            );
                            Ok(CompletionOutcome::IgnoredStale)
                        }
                        Some(updated_job) => {
                            let event = if needs_event {
                                Some(UntypedJobEvent::Retried {
                                    job: updated_job.clone(),
                                    error: error_msg,
                                    attempt: job.attempt,
                                    next_run_at: updated_job.run_at,
                                })
                            } else {
                                None
                            };
                            Ok(CompletionOutcome::Applied {
                                event,
                                terminal: false,
                            })
                        }
                    };
                }

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

/// Complete a queue-storage job and run its registered follow-up enqueue
/// specs atomically with the completion (ADR-029).
///
/// Bypasses the receipt-plane fast-complete path and goes straight to the
/// tx-aware slow path so the lease/receipt cleanup, `done_entries` append,
/// and follow-up `INSERT`s all commit together. `complete_runtime_batch_slow`
/// already handles both receipt-claimed and materialised leases, so the
/// trade-off is purely losing receipt-plane fast-complete throughput for
/// spec'd jobs — acceptable since spec'd jobs do extra DB work anyway.
///
/// Returns:
/// - `Ok(None)` if the completion was stale — the lease was already rescued
///   or cancelled. The transaction is rolled back; no follow-ups emitted.
/// - `Ok(Some(updated_job))` if the completion committed; follow-ups have
///   been INSERTed in the same transaction. The returned row is the
///   post-completion snapshot, mirroring the fallback constructed elsewhere
///   in this file when `runtime.store.load_job` doesn't return one.
#[allow(clippy::explicit_auto_deref)]
async fn complete_queue_storage_with_followups(
    runtime: &QueueStorageRuntime,
    pool: &PgPool,
    job: &JobRow,
    queue_storage_claim: Option<&ClaimedEntry>,
    queue_storage_unique_states: Option<&str>,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let Some(claim) = queue_storage_claim else {
        // The slow path is keyed on `ClaimedEntry` (it needs `lease_slot`,
        // `lane_seq`, `claim_slot`, etc. to clean up receipt-plane and
        // materialised lease rows). The executor always passes a claim when
        // it dispatches a queue-storage job, so this branch is normally
        // unreachable; report as stale rather than silently dropping the
        // outcome.
        warn!(
            job_id = job.id,
            "queue-storage completion with follow-up specs but no claim — \
             treating as stale"
        );
        return Ok(None);
    };

    let runtime_job = ClaimedRuntimeJob {
        claim: claim.clone(),
        job: job.clone(),
        unique_states: queue_storage_unique_states.map(std::string::ToString::to_string),
    };

    let mut tx = pool.begin().await?;

    let updated = runtime
        .store
        .complete_runtime_batch_slow_in_tx(&mut tx, std::slice::from_ref(&runtime_job))
        .await?;

    if updated.is_empty() {
        tx.rollback().await?;
        return Ok(None);
    }

    // Synthesise the post-completion snapshot — the store has just moved the
    // lease into `done_entries` so a live SELECT would race with rotation.
    // The happy-path fields are exactly those `complete_job_queue_storage`
    // reconstructs when `load_job` returns None.
    let mut updated_job = job.clone();
    updated_job.state = JobState::Completed;
    updated_job.finalized_at = Some(chrono::Utc::now());
    updated_job.progress = None;

    crate::enqueue_specs::dispatch_specs_in_tx(&mut tx, &updated_job, specs, None).await?;

    tx.commit().await?;
    Ok(Some(updated_job))
}

/// Cancel a running queue-storage job and dispatch any Cancelled
/// follow-ups in the same transaction (ADR-029).
async fn cancel_queue_storage_with_followups(
    runtime: &QueueStorageRuntime,
    pool: &PgPool,
    job: &JobRow,
    reason: &str,
    progress_snapshot: Option<serde_json::Value>,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;
    let Some(updated_job) = runtime
        .store
        .cancel_running_in_tx(&mut tx, job.id, job.run_lease, reason, progress_snapshot)
        .await?
    else {
        tx.rollback().await?;
        return Ok(None);
    };
    let outcome_ctx = crate::enqueue_specs::OutcomeContext::Cancelled {
        reason: reason.to_string(),
    };
    crate::enqueue_specs::dispatch_specs_in_tx(&mut tx, &updated_job, specs, Some(&outcome_ctx))
        .await?;
    tx.commit().await?;
    Ok(Some(updated_job))
}

/// RetryAfter on queue-storage + Retried follow-ups, in the same tx.
async fn retry_after_queue_storage_with_followups(
    runtime: &QueueStorageRuntime,
    pool: &PgPool,
    job: &JobRow,
    retry_duration: Duration,
    progress_snapshot: Option<serde_json::Value>,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;
    let Some(updated_job) = runtime
        .store
        .retry_after_in_tx(
            &mut tx,
            job.id,
            job.run_lease,
            retry_duration,
            progress_snapshot,
        )
        .await?
    else {
        tx.rollback().await?;
        return Ok(None);
    };
    // Mirror the canonical `RetryAfter` event: empty error string.
    let outcome_ctx = crate::enqueue_specs::OutcomeContext::Retried {
        error: String::new(),
        attempt: updated_job.attempt,
        next_run_at: updated_job.run_at,
    };
    crate::enqueue_specs::dispatch_specs_in_tx(&mut tx, &updated_job, specs, Some(&outcome_ctx))
        .await?;
    tx.commit().await?;
    Ok(Some(updated_job))
}

/// Retryable error on queue-storage that backs off (attempt < max_attempts)
/// plus Retried follow-ups, in the same tx.
async fn retry_backoff_queue_storage_with_followups(
    runtime: &QueueStorageRuntime,
    pool: &PgPool,
    job: &JobRow,
    error_msg: &str,
    progress_snapshot: Option<serde_json::Value>,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;
    let Some(updated_job) = runtime
        .store
        .fail_retryable_in_tx(&mut tx, job.id, job.run_lease, error_msg, progress_snapshot)
        .await?
    else {
        tx.rollback().await?;
        return Ok(None);
    };
    let outcome_ctx = crate::enqueue_specs::OutcomeContext::Retried {
        error: error_msg.to_string(),
        attempt: job.attempt,
        next_run_at: updated_job.run_at,
    };
    crate::enqueue_specs::dispatch_specs_in_tx(&mut tx, &updated_job, specs, Some(&outcome_ctx))
        .await?;
    tx.commit().await?;
    Ok(Some(updated_job))
}

/// Terminal failure (or retries exhausted) on queue-storage + Exhausted
/// follow-ups, in the same tx. Honours DLQ routing if enabled.
#[allow(clippy::too_many_arguments)]
async fn fail_queue_storage_with_followups(
    runtime: &QueueStorageRuntime,
    pool: &PgPool,
    job: &JobRow,
    dlq_reason: &str,
    error_msg: &str,
    progress_snapshot: Option<serde_json::Value>,
    dlq_enabled: bool,
    metrics: &crate::metrics::AwaMetrics,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;
    // Track whether the row routed to DLQ so the metric records after a
    // successful commit only — a spec-INSERT failure rolls the whole tx
    // back, and a phantom DLQ count would lie about a transition that
    // didn't actually happen.
    let (updated_job, routed_to_dlq) = if dlq_enabled {
        let moved = runtime
            .store
            .fail_to_dlq_in_tx(
                &mut tx,
                job.id,
                job.run_lease,
                dlq_reason,
                error_msg,
                progress_snapshot,
            )
            .await?;
        let routed = moved.is_some();
        (moved, routed)
    } else {
        let moved = runtime
            .store
            .fail_terminal_in_tx(&mut tx, job.id, job.run_lease, error_msg, progress_snapshot)
            .await?;
        (moved, false)
    };
    let Some(updated_job) = updated_job else {
        tx.rollback().await?;
        return Ok(None);
    };
    let outcome_ctx = crate::enqueue_specs::OutcomeContext::Exhausted {
        error: error_msg.to_string(),
        attempt: job.attempt,
    };
    crate::enqueue_specs::dispatch_specs_in_tx(&mut tx, &updated_job, specs, Some(&outcome_ctx))
        .await?;
    tx.commit().await?;
    if routed_to_dlq {
        metrics.record_dlq_moved(&job.kind, &job.queue, dlq_reason);
    }
    Ok(Some(updated_job))
}

/// Park a queue-storage job in `waiting_external` and dispatch any
/// WaitingForCallback follow-ups in the same tx (ADR-029).
///
/// Returns `Ok(Some(parked))` if the row transitioned. `Ok(None)` if the
/// guarded UPDATE didn't match — caller falls back to the existing race /
/// missing-callback handling.
async fn park_queue_storage_with_followups(
    runtime: &QueueStorageRuntime,
    pool: &PgPool,
    job: &JobRow,
    callback_id: uuid::Uuid,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;
    let entered = runtime
        .store
        .enter_callback_wait_in_tx(&mut tx, job.id, job.run_lease, callback_id)
        .await?;
    if !entered {
        tx.rollback().await?;
        return Ok(None);
    }
    // The parked row lives in the leases table; constructing the post-park
    // snapshot from `job` matches the fallback `complete_job_queue_storage`
    // already uses when `load_job` doesn't return a row.
    let mut parked_job = job.clone();
    parked_job.state = JobState::WaitingExternal;
    parked_job.heartbeat_at = None;
    parked_job.deadline_at = None;
    crate::enqueue_specs::dispatch_specs_in_tx(
        &mut tx,
        &parked_job,
        specs,
        Some(&crate::enqueue_specs::OutcomeContext::WaitingForCallback),
    )
    .await?;
    tx.commit().await?;
    Ok(Some(parked_job))
}

async fn direct_complete_job_queue_storage(
    runtime: &QueueStorageRuntime,
    pool: &PgPool,
    job: &JobRow,
    queue_storage_claim: Option<&ClaimedEntry>,
    queue_storage_unique_states: Option<&str>,
) -> Result<bool, AwaError> {
    let updated = if let Some(claim) = queue_storage_claim {
        let runtime_job = ClaimedRuntimeJob {
            claim: claim.clone(),
            job: job.clone(),
            unique_states: queue_storage_unique_states.map(std::string::ToString::to_string),
        };
        runtime
            .store
            .complete_runtime_batch(pool, std::slice::from_ref(&runtime_job))
            .await?
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
pub(crate) async fn dispatch_lifecycle_event(
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

/// Complete a canonical-storage job and run its registered follow-up enqueue
/// specs atomically with the completion UPDATE (ADR-029).
///
/// Returns:
/// - `Ok(None)` if the completion was stale (`rows_affected == 0`) — the job
///   has already been rescued or cancelled, no follow-ups are emitted, no
///   event should fire.
/// - `Ok(Some(updated_job))` if the completion committed; follow-ups have
///   been INSERTed in the same transaction. The returned row is the
///   post-completion snapshot (state = `completed`, `finalized_at` set).
// The follow-up loop reborrows `&mut *tx` per spec invocation so the same
// transaction handle can be reused; clippy reads the `*tx` as a redundant
// deref but `fetch_one`'s Executor bound requires the inner connection.
#[allow(clippy::explicit_auto_deref)]
async fn complete_canonical_with_followups(
    pool: &PgPool,
    job: &JobRow,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;

    // `UPDATE ... RETURNING` against `awa.jobs_hot` directly (not the
    // `awa.jobs` compatibility view): in canonical-drain mode the view is
    // backed by the active queue-storage schema and would return RowNotFound
    // for a canonical row, rolling back the just-applied completion and
    // wedging the job. The hot table is the source of truth for canonical
    // attempts regardless of routing mode.
    let updated_job: Option<JobRow> = sqlx::query_as(
        r#"
        UPDATE awa.jobs_hot
        SET state = 'completed',
            finalized_at = now(),
            progress = NULL
        WHERE id = $1 AND state = 'running' AND run_lease = $2
        RETURNING *
        "#,
    )
    .bind(job.id)
    .bind(job.run_lease)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(updated_job) = updated_job else {
        // Stale: another writer already finalised this attempt. Drop the
        // transaction without emitting follow-ups.
        tx.rollback().await?;
        return Ok(None);
    };

    crate::enqueue_specs::dispatch_specs_in_tx(&mut tx, &updated_job, specs, None).await?;

    tx.commit().await?;
    Ok(Some(updated_job))
}

/// Cancelled-outcome equivalent: cancel the running canonical row and
/// dispatch any registered `Cancelled` follow-ups inside the same
/// transaction. Returns the post-update [`JobRow`] for event emission, or
/// `None` when the row was already finalised by another writer.
async fn cancel_canonical_with_followups(
    pool: &PgPool,
    job: &JobRow,
    reason: &str,
    progress_snapshot: Option<&serde_json::Value>,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
    outcome_ctx: &crate::enqueue_specs::OutcomeContext,
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;

    let updated_job: Option<JobRow> = sqlx::query_as(
        r#"
        UPDATE awa.jobs_hot
        SET state = 'cancelled',
            finalized_at = now(),
            errors = errors || $2::jsonb,
            progress = $4
        WHERE id = $1 AND state = 'running' AND run_lease = $3
        RETURNING *
        "#,
    )
    .bind(job.id)
    .bind(serde_json::json!({
        "error": format!("cancelled: {}", reason),
        "attempt": job.attempt,
        "at": chrono::Utc::now().to_rfc3339()
    }))
    .bind(job.run_lease)
    .bind(progress_snapshot)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(updated_job) = updated_job else {
        tx.rollback().await?;
        return Ok(None);
    };

    crate::enqueue_specs::dispatch_specs_in_tx(&mut tx, &updated_job, specs, Some(outcome_ctx))
        .await?;

    tx.commit().await?;
    Ok(Some(updated_job))
}

/// Park the running canonical row into `waiting_external` and dispatch any
/// registered `WaitingForCallback` follow-ups inside the same transaction.
/// Returns the parked [`JobRow`] for event emission, or `None` when the
/// guarded UPDATE matched zero rows (caller falls back to the existing race
/// / missing-callback handling).
async fn park_canonical_with_followups(
    pool: &PgPool,
    job: &JobRow,
    progress_snapshot: Option<&serde_json::Value>,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;

    let parked_job: Option<JobRow> = sqlx::query_as(
        r#"
        UPDATE awa.jobs_hot
        SET state = 'waiting_external',
            heartbeat_at = NULL,
            deadline_at = NULL,
            progress = $3
        WHERE id = $1 AND state = 'running' AND run_lease = $2 AND callback_id IS NOT NULL
        RETURNING *
        "#,
    )
    .bind(job.id)
    .bind(job.run_lease)
    .bind(progress_snapshot)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(parked_job) = parked_job else {
        tx.rollback().await?;
        return Ok(None);
    };

    crate::enqueue_specs::dispatch_specs_in_tx(
        &mut tx,
        &parked_job,
        specs,
        Some(&crate::enqueue_specs::OutcomeContext::WaitingForCallback),
    )
    .await?;

    tx.commit().await?;
    Ok(Some(parked_job))
}

/// Caller-requested retry (RetryAfter) + Retried follow-ups.
///
/// `retryable` is forbidden in `awa.jobs_hot` (CHECK constraint); the row
/// must move to `awa.scheduled_jobs`. We do the DELETE+INSERT explicitly
/// in a CTE so the move plus the follow-up enqueue commit atomically. This
/// mirrors what the `INSTEAD OF UPDATE` trigger on `awa.jobs` would do —
/// avoiding the view keeps us correct under canonical-drain mode, where the
/// view's trigger rejects UPDATEs while queue-storage is the active
/// engine.
async fn retry_after_canonical_with_followups(
    pool: &PgPool,
    job: &JobRow,
    seconds: f64,
    progress_snapshot: Option<&serde_json::Value>,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;

    let updated_job: Option<JobRow> = sqlx::query_as(
        r#"
        WITH deleted AS (
            DELETE FROM awa.jobs_hot
            WHERE id = $1 AND state = 'running' AND run_lease = $3
            RETURNING *
        )
        INSERT INTO awa.scheduled_jobs (
            id, kind, queue, args, state, priority, attempt, max_attempts,
            run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
            created_at, errors, metadata, tags, unique_key, unique_states,
            callback_id, callback_timeout_at, callback_filter,
            callback_on_complete, callback_on_fail, callback_transform,
            run_lease, progress
        )
        SELECT
            id, kind, queue, args,
            'retryable'::awa.job_state,
            priority, attempt, max_attempts,
            now() + make_interval(secs => $2),
            heartbeat_at, deadline_at, attempted_at,
            now(),
            created_at, errors, metadata, tags, unique_key, unique_states,
            callback_id, callback_timeout_at, callback_filter,
            callback_on_complete, callback_on_fail, callback_transform,
            run_lease, $4
        FROM deleted
        RETURNING *
        "#,
    )
    .bind(job.id)
    .bind(seconds)
    .bind(job.run_lease)
    .bind(progress_snapshot)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(updated_job) = updated_job else {
        tx.rollback().await?;
        return Ok(None);
    };

    let outcome_ctx = crate::enqueue_specs::OutcomeContext::Retried {
        error: String::new(),
        attempt: updated_job.attempt,
        next_run_at: updated_job.run_at,
    };
    crate::enqueue_specs::dispatch_specs_in_tx(&mut tx, &updated_job, specs, Some(&outcome_ctx))
        .await?;

    tx.commit().await?;
    Ok(Some(updated_job))
}

/// Terminal error -> failed + Exhausted follow-ups.
async fn terminal_canonical_with_followups(
    pool: &PgPool,
    job: &JobRow,
    msg: &str,
    progress_snapshot: Option<&serde_json::Value>,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;

    let updated_job: Option<JobRow> = sqlx::query_as(
        r#"
        UPDATE awa.jobs_hot
        SET state = 'failed',
            finalized_at = now(),
            errors = errors || $2::jsonb,
            progress = $4
        WHERE id = $1 AND state = 'running' AND run_lease = $3
        RETURNING *
        "#,
    )
    .bind(job.id)
    .bind(serde_json::json!({
        "error": msg,
        "attempt": job.attempt,
        "at": chrono::Utc::now().to_rfc3339(),
        "terminal": true
    }))
    .bind(job.run_lease)
    .bind(progress_snapshot)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(updated_job) = updated_job else {
        tx.rollback().await?;
        return Ok(None);
    };

    let outcome_ctx = crate::enqueue_specs::OutcomeContext::Exhausted {
        error: msg.to_string(),
        attempt: job.attempt,
    };
    crate::enqueue_specs::dispatch_specs_in_tx(&mut tx, &updated_job, specs, Some(&outcome_ctx))
        .await?;

    tx.commit().await?;
    Ok(Some(updated_job))
}

/// Retries exhausted (attempt >= max_attempts) -> failed + Exhausted
/// follow-ups. Differs from terminal in the error envelope (no `terminal`
/// marker since exhaustion is not a fatal-by-handler-assertion).
async fn exhaust_canonical_with_followups(
    pool: &PgPool,
    job: &JobRow,
    error_msg: &str,
    progress_snapshot: Option<&serde_json::Value>,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;

    let updated_job: Option<JobRow> = sqlx::query_as(
        r#"
        UPDATE awa.jobs_hot
        SET state = 'failed',
            finalized_at = now(),
            errors = errors || $2::jsonb,
            progress = $4
        WHERE id = $1 AND state = 'running' AND run_lease = $3
        RETURNING *
        "#,
    )
    .bind(job.id)
    .bind(serde_json::json!({
        "error": error_msg,
        "attempt": job.attempt,
        "at": chrono::Utc::now().to_rfc3339()
    }))
    .bind(job.run_lease)
    .bind(progress_snapshot)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(updated_job) = updated_job else {
        tx.rollback().await?;
        return Ok(None);
    };

    let outcome_ctx = crate::enqueue_specs::OutcomeContext::Exhausted {
        error: error_msg.to_string(),
        attempt: job.attempt,
    };
    crate::enqueue_specs::dispatch_specs_in_tx(&mut tx, &updated_job, specs, Some(&outcome_ctx))
        .await?;

    tx.commit().await?;
    Ok(Some(updated_job))
}

/// Retryable error within budget -> retryable + Retried follow-ups, using
/// the configured backoff function for run_at.
async fn retry_backoff_canonical_with_followups(
    pool: &PgPool,
    job: &JobRow,
    error_msg: &str,
    progress_snapshot: Option<&serde_json::Value>,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;

    let updated_job: Option<JobRow> = sqlx::query_as(
        r#"
        WITH deleted AS (
            DELETE FROM awa.jobs_hot
            WHERE id = $1 AND state = 'running' AND run_lease = $5
            RETURNING *
        )
        INSERT INTO awa.scheduled_jobs (
            id, kind, queue, args, state, priority, attempt, max_attempts,
            run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
            created_at, errors, metadata, tags, unique_key, unique_states,
            callback_id, callback_timeout_at, callback_filter,
            callback_on_complete, callback_on_fail, callback_transform,
            run_lease, progress
        )
        SELECT
            id, kind, queue, args,
            'retryable'::awa.job_state,
            priority, attempt, max_attempts,
            now() + awa.backoff_duration($2, $3),
            NULL, NULL,
            attempted_at,
            now(),
            created_at,
            errors || $4::jsonb,
            metadata, tags, unique_key, unique_states,
            callback_id, callback_timeout_at, callback_filter,
            callback_on_complete, callback_on_fail, callback_transform,
            run_lease, $6
        FROM deleted
        RETURNING *
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
    .bind(progress_snapshot)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(updated_job) = updated_job else {
        tx.rollback().await?;
        return Ok(None);
    };

    let outcome_ctx = crate::enqueue_specs::OutcomeContext::Retried {
        error: error_msg.to_string(),
        attempt: job.attempt,
        next_run_at: updated_job.run_at,
    };
    crate::enqueue_specs::dispatch_specs_in_tx(&mut tx, &updated_job, specs, Some(&outcome_ctx))
        .await?;

    tx.commit().await?;
    Ok(Some(updated_job))
}
