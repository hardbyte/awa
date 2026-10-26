use crate::completion::CompletionBatcherHandle;
use crate::context::{CallbackGuard, JobContext};
use crate::events::{BoxedUntypedEventHandler, UntypedJobEvent};
use crate::queue_counters::{QueueCounterMap, QueueCounters};
use crate::runtime::{InFlightMap, InFlightState, ProgressState};
use crate::storage::{QueueStorageRuntime, RuntimeStorage};
use awa_model::reschedule::{Reschedule, RescheduleOutcome};
use awa_model::{AwaError, ClaimedEntry, ClaimedRuntimeJob, JobRow, JobState};
use sqlx::PgPool;
use std::any::Any;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, info_span, warn, Instrument};

/// Result of executing a job handler.
///
/// # Picking the right variant for re-runs
///
/// Three primitives can put the job back on the queue. They differ in
/// what they say about *why*:
///
/// | Primitive | Means | Increments `attempt` | Delay shape | Bounded by `max_attempts` |
/// |-----------|-------|----------------------|-------------|---------------------------|
/// | [`JobError::Retryable`] | "this attempt failed; try again" | yes | DB-computed exponential backoff | yes |
/// | [`JobResult::RetryAfter`] | "this attempt failed; try again after delay X" | yes | caller-specified | yes |
/// | [`JobResult::Snooze`] | "this attempt didn't fail — it's just not time yet" | no | caller-specified | no |
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
    ///
    /// A retry requested on the final attempt (`attempt >= max_attempts`)
    /// exhausts the job: it fails into the DLQ exactly like a retryable
    /// error would. For indefinite rescheduling that must not consume the
    /// attempt budget, return [`Snooze`](JobResult::Snooze) instead.
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
    queue_counters: QueueCounterMap,
    state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
    metrics: crate::metrics::AwaMetrics,
    completion_batcher: CompletionBatcherHandle,
    storage: RuntimeStorage,
    dlq_policy: DlqPolicy,
    finalizers: TaskTracker,
    finalize_cancel: CancellationToken,
    finalize_capacity: Arc<Semaphore>,
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
        queue_counters: QueueCounterMap,
        state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
        metrics: crate::metrics::AwaMetrics,
        completion_batcher: CompletionBatcherHandle,
        storage: RuntimeStorage,
        dlq_policy: DlqPolicy,
        finalizers: TaskTracker,
        finalize_cancel: CancellationToken,
        runtime_worker_capacity: u32,
    ) -> Self {
        let finalize_capacity = Arc::new(Semaphore::new(
            completion_batcher.finalizer_capacity(runtime_worker_capacity),
        ));
        Self {
            pool,
            workers,
            lifecycle_handlers,
            enqueue_specs,
            in_flight,
            queue_in_flight,
            queue_counters,
            state,
            metrics,
            completion_batcher,
            storage,
            dlq_policy,
            finalizers,
            finalize_cancel,
            finalize_capacity,
        }
    }

    /// Counters for `queue`, if it is one this runtime serves.
    pub(crate) fn queue_counters(&self, queue: &str) -> Option<Arc<QueueCounters>> {
        self.queue_counters.get(queue).cloned()
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
        let finalizers = self.finalizers.clone();
        let finalize_cancel = self.finalize_cancel.clone();
        let finalize_capacity = self.finalize_capacity.clone();
        let job_id = job.id;
        let job_run_lease = job.run_lease;
        let job_kind = job.kind.clone();
        let job_queue = job.queue.clone();
        let counters = self.queue_counters(&job_queue);

        let span = info_span!(
            "job.execute",
            job.id = job_id,
            job.kind = %job_kind,
            job.queue = %job_queue,
            job.attempt = job.attempt,
            otel.name = %format!("job.execute {}", job_kind),
            otel.kind = "consumer",
            otel.status_code = tracing::field::Empty,
            messaging.system = "awa",
            messaging.destination.name = %job_queue,
            messaging.operation.r#type = "process",
            messaging.message.id = %job_id,
        );

        // Continue the producer's trace (#110): a traceparent captured at
        // enqueue (awa_model::trace) reconnects the execution span to it.
        // First attempts join it as a remote child — the enqueue→execute
        // flow reads as one trace. Retries instead start a fresh trace with
        // a LINK back, per OTel messaging guidance for deferred processing:
        // a job retried hours later would otherwise stretch the original
        // trace across its whole backoff schedule and inherit its sampling
        // decision. set_parent fails when no OpenTelemetry layer is
        // installed — the normal untraced case, so the error is dropped.
        if let Some(enqueue_site) = awa_model::trace::traceparent_from_metadata(&job.metadata)
            .and_then(awa_model::trace::parse_traceparent)
        {
            use opentelemetry::trace::TraceContextExt;
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            if job.attempt <= 1 {
                let _ = span.set_parent(
                    opentelemetry::Context::new().with_remote_span_context(enqueue_site),
                );
            } else {
                // Detach from the ambient dispatcher claim span so the
                // retry is a root span, then link back to the enqueue site.
                let _ = span.set_parent(opentelemetry::Context::new());
                span.add_link(enqueue_site);
            }
        }

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

            // Keep worker/heartbeat ownership while the bounded finalizer queue is full.
            let finalize_permit = tokio::select! {
                permit = finalize_capacity.acquire_owned() => Some(permit.expect("finalizer capacity is never closed")),
                _ = finalize_cancel.cancelled() => None,
            };

            in_flight.remove((job_id, job_run_lease));
            if let Some(counter) = queue_in_flight.get(&job_queue) {
                counter.fetch_sub(1, Ordering::SeqCst);
            }
            metrics.record_in_flight_change(&job_queue, -1);

            let Some(finalize_permit) = finalize_permit else {
                return;
            };
            let dlq_enabled = dlq_policy.enabled_for(&job_queue);
            finalizers.spawn(
                async move {
                    let event_kind = job_kind.clone();
                    let finalize = async move {
                        let outcome = finalize_with_retry(
                            |completion_path| complete_job(
                                &pool,
                                &job,
                                queue_storage_claim.as_ref(),
                                queue_storage_unique_states.as_deref(),
                                &result,
                                &completion_batcher,
                                completion_path,
                                progress_snapshot.clone(),
                                duration,
                                has_lifecycle_handlers,
                                &enqueue_specs,
                                &storage,
                                dlq_enabled,
                                &metrics,
                            ),
                            &metrics,
                            job_id,
                            &job_kind,
                            &job_queue,
                            COMPLETION_RECONNECT_WINDOW,
                        ).await;

                        match &outcome {
                            Ok(FinalizeAck {
                                outcome: CompletionOutcome::Applied { terminal, .. },
                                reconnect_retries,
                                elapsed,
                            }) => {
                                log_applied_outcome(&job, &result, *terminal);
                                if *reconnect_retries > 0 {
                                    info!(
                                        job_id,
                                        retries = reconnect_retries,
                                        elapsed_ms = elapsed.as_millis() as u64,
                                        "Durable finalization acknowledged after database unavailability"
                                    );
                                }
                                // State transition succeeded — record metrics. `terminal`
                                // is the source of truth for retry-vs-failure because
                                // JobError::Retryable can resolve to either path.
                                let counters = counters.as_deref();
                                match &result {
                                    Ok(JobResult::Completed) => {
                                        metrics.record_job_completed(&job_kind, &job_queue, duration);
                                        if let Some(counters) = counters { counters.record_completed(); }
                                    }
                                    Ok(JobResult::RetryAfter(_)) => {
                                        if *terminal {
                                            metrics.record_job_failed(&job_kind, &job_queue, true);
                                            if let Some(counters) = counters { counters.record_failed(); }
                                        } else {
                                            metrics.record_job_retried(&job_kind, &job_queue);
                                            if let Some(counters) = counters { counters.record_retried(); }
                                        }
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
                                            if let Some(counters) = counters { counters.record_failed(); }
                                        } else {
                                            if let Some(counters) = counters { counters.record_parked(); }
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
                                        if let Some(counters) = counters { counters.record_failed(); }
                                    }
                                    Err(JobError::Retryable(_)) => {
                                        if *terminal {
                                            metrics.record_job_failed(&job_kind, &job_queue, true);
                                            if let Some(counters) = counters { counters.record_failed(); }
                                        } else {
                                            metrics.record_job_retried(&job_kind, &job_queue);
                                            if let Some(counters) = counters { counters.record_retried(); }
                                        }
                                    }
                                }
                            }
                            Ok(FinalizeAck {
                                outcome: CompletionOutcome::IgnoredStale,
                                reconnect_retries,
                                ..
                            }) => {
                                if *reconnect_retries > 0 {
                                    info!(
                                        job_id,
                                        retries = reconnect_retries,
                                        "Finalization outcome unknown after database unavailability; an earlier attempt may have committed or another actor closed the lease"
                                    );
                                } else {
                                    debug!(job_id, "Attempt is no longer current; finalization ignored");
                                }
                            }
                            Err(err) => {
                                error!(job_id, error = %err, "Failed to complete job");
                            }
                        }

                        if let Ok(FinalizeAck {
                            outcome: CompletionOutcome::Applied { event: Some(event), .. }, ..
                        }) = outcome
                        {
                            Some(event)
                        } else {
                            None
                        }
                    };
                    let event = tokio::select! {
                        event = finalize => event,
                        _ = finalize_cancel.cancelled() => {
                            warn!(job_id, "Finalization cancelled after runtime drain");
                            None
                        },
                    };
                    drop(finalize_permit);
                    if let Some(event) = event {
                        tokio::select! {
                            _ = dispatch_lifecycle_event(&lifecycle_handlers, &event_kind, event) => {},
                            _ = finalize_cancel.cancelled() => {},
                        }
                    }
                }
                .in_current_span(),
            );
        }
        .instrument(span)
    }
}

/// How many times a finalize transaction is re-run after Postgres aborts it
/// to break a lock cycle before the failure is surfaced.
const COMPLETION_DEADLOCK_RETRIES: u32 = 3;

/// Short exponential backoff between deadlock retries (10ms, 20ms, 40ms, ...).
/// Postgres aborts only one party of a cycle, so the survivor has usually
/// committed by the time the retry runs.
fn completion_deadlock_backoff(retry: u32) -> Duration {
    Duration::from_millis(10u64 << retry.saturating_sub(1).min(4))
}

#[derive(Debug, thiserror::Error)]
enum FinalizeError {
    #[error(transparent)]
    Operation(#[from] AwaError),
    #[error("durable finalization was not acknowledged before its retry deadline")]
    Deadline,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompletionPath {
    Batched,
    Direct,
}

#[derive(Debug)]
struct FinalizeAck<T> {
    outcome: T,
    reconnect_retries: u32,
    elapsed: Duration,
}

async fn finalize_with_retry<T, F, Fut>(
    mut finalize: F,
    metrics: &crate::metrics::AwaMetrics,
    job_id: i64,
    kind: &str,
    queue: &str,
    window: Duration,
) -> Result<FinalizeAck<T>, FinalizeError>
where
    F: FnMut(CompletionPath) -> Fut,
    Fut: std::future::Future<Output = Result<T, AwaError>>,
{
    let started = tokio::time::Instant::now();
    let deadline = started + window;
    let mut deadlock_retries = 0;
    let mut reconnect_retries = 0;
    let mut completion_path = CompletionPath::Batched;
    loop {
        let outcome = tokio::time::timeout_at(deadline, finalize(completion_path))
            .await
            .map_err(|_| FinalizeError::Deadline)?;
        let error = match outcome {
            Ok(outcome) => {
                return Ok(FinalizeAck {
                    outcome,
                    reconnect_retries,
                    elapsed: started.elapsed(),
                });
            }
            Err(error) => error,
        };
        let delay = if error.is_deadlock() && deadlock_retries < COMPLETION_DEADLOCK_RETRIES {
            deadlock_retries += 1;
            completion_deadlock_backoff(deadlock_retries)
        } else if error.is_connection_lost() {
            reconnect_retries += 1;
            completion_reconnect_backoff(reconnect_retries)
        } else {
            return Err(error.into());
        };
        if deadlock_retries + reconnect_retries == 1 {
            warn!(job_id, error = %error, delay_ms = delay.as_millis() as u64, "Durable finalization failed; retrying");
        } else {
            debug!(job_id, error = %error, delay_ms = delay.as_millis() as u64, "Durable finalization failed; retrying");
        }
        tokio::time::timeout_at(deadline, tokio::time::sleep(delay))
            .await
            .map_err(|_| FinalizeError::Deadline)?;
        if error.is_connection_lost() {
            metrics.record_completion_reconnect_retry(kind, queue);
        } else {
            metrics.record_completion_deadlock_retry(kind, queue);
        }
        completion_path = CompletionPath::Direct;
    }
}

/// Reconnect retry budget measured from the first finalize attempt. Rescue
/// can win before this expires; the lease guard then rejects the old result.
const COMPLETION_RECONNECT_WINDOW: Duration = Duration::from_secs(120);

/// Equal jitter between half and all of an exponentially increasing delay,
/// capped at four seconds.
fn completion_reconnect_backoff(retry: u32) -> Duration {
    let ceiling_ms = 500u64 << retry.saturating_sub(1).min(3);
    let half_ms = ceiling_ms / 2;
    let random = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    Duration::from_millis(half_ms + random % (half_ms + 1))
}

fn log_applied_outcome(job: &JobRow, result: &Result<JobResult, JobError>, terminal: bool) {
    if terminal {
        tracing::Span::current().record("otel.status_code", "ERROR");
        let reason = match result {
            Err(error) => error.to_string(),
            Ok(JobResult::RetryAfter(delay)) => {
                format!("max_attempts exhausted (handler requested retry after {delay:?})")
            }
            Ok(JobResult::WaitForCallback(_)) => {
                "WaitForCallback returned without calling register_callback".into()
            }
            _ => "attempt exhausted".into(),
        };
        error!(job_id = job.id, kind = %job.kind, attempt = job.attempt, max_attempts = job.max_attempts, error = %reason, "Job failed terminally");
        return;
    }
    match result {
        Ok(JobResult::Completed) => {
            tracing::Span::current().record("otel.status_code", "OK");
            info!(job_id = job.id, kind = %job.kind, attempt = job.attempt, "Job completed");
        }
        Ok(JobResult::RetryAfter(delay)) => {
            info!(job_id = job.id, kind = %job.kind, retry_after_secs = delay.as_secs_f64(), "Job requested retry after duration");
        }
        Ok(JobResult::Snooze(delay)) => {
            info!(job_id = job.id, kind = %job.kind, snooze_secs = delay.as_secs_f64(), "Job snoozed");
        }
        Ok(JobResult::Cancel(reason)) => {
            info!(job_id = job.id, kind = %job.kind, reason = %reason, "Job cancelled");
        }
        Ok(JobResult::WaitForCallback(_)) => {
            info!(job_id = job.id, kind = %job.kind, "Callback wait finalization acknowledged");
        }
        Err(error) => {
            tracing::Span::current().record("otel.status_code", "ERROR");
            warn!(job_id = job.id, kind = %job.kind, attempt = job.attempt, error = %error, "Job failed; retry scheduled");
        }
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
    completion_path: CompletionPath,
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
                completion_path,
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
                completion_path,
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

type EnqueueSpecsByOutcome = HashMap<
    crate::enqueue_specs::Outcome,
    HashMap<String, Vec<crate::enqueue_specs::BoxedEnqueueSpec>>,
>;

/// Terminal exhaustion on the canonical engine: mark the job `failed`, append
/// the error, and emit the `Exhausted` outcome (with follow-ups when
/// registered). Shared by the retryable-error path and a [`JobResult::RetryAfter`]
/// returned on the final attempt — `max_attempts` bounds failed attempts
/// regardless of which primitive reported the failure.
async fn exhaust_canonical(
    pool: &PgPool,
    job: &JobRow,
    error_msg: String,
    enqueue_specs: &EnqueueSpecsByOutcome,
    progress_snapshot: &Option<serde_json::Value>,
    needs_event: bool,
) -> Result<CompletionOutcome, AwaError> {
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
            None => Ok(CompletionOutcome::IgnoredStale),
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

    // `awa.jobs_hot` directly, not the `awa.jobs` view: in canonical-drain
    // mode the view is backed by the active queue-storage schema, so a view
    // UPDATE silently misses the canonical row and the job wedges in
    // `running` (same rationale as `complete_canonical_with_followups`).
    // This applies to every canonical fast-path write and re-read below.
    let result = sqlx::query(
        r#"
        UPDATE awa.jobs_hot
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
    .bind(progress_snapshot)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Ok(CompletionOutcome::IgnoredStale);
    }
    if needs_event {
        let updated_job: JobRow = sqlx::query_as("SELECT * FROM awa.jobs_hot WHERE id = $1")
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
}

/// Terminal exhaustion on queue storage: fail into the DLQ when enabled
/// (reason `max_attempts_exhausted`), else fail terminally, and emit the
/// `Exhausted` outcome (with follow-ups when registered). Shared by the
/// retryable-error path and a [`JobResult::RetryAfter`] returned on the
/// final attempt.
#[allow(clippy::too_many_arguments)]
async fn exhaust_queue_storage(
    runtime: &QueueStorageRuntime,
    pool: &PgPool,
    job: &JobRow,
    queue_storage_claim: Option<&ClaimedEntry>,
    error_msg: String,
    enqueue_specs: &EnqueueSpecsByOutcome,
    progress_snapshot: Option<serde_json::Value>,
    dlq_enabled: bool,
    metrics: &crate::metrics::AwaMetrics,
    needs_event: bool,
) -> Result<CompletionOutcome, AwaError> {
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
            queue_storage_claim,
            "max_attempts_exhausted",
            &error_msg,
            progress_snapshot.clone(),
            dlq_enabled,
            metrics,
            &specs,
        )
        .await?;
        return match result {
            None => Ok(CompletionOutcome::IgnoredStale),
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
            .fail_to_dlq_with_claim(
                pool,
                job.id,
                job.run_lease,
                "max_attempts_exhausted",
                &error_msg,
                progress_snapshot.clone(),
                queue_storage_claim,
            )
            .await?;
        if moved.is_some() {
            metrics.record_dlq_moved(&job.kind, &job.queue, "max_attempts_exhausted");
        }
        moved
    } else {
        runtime
            .store
            .fail_terminal_with_claim(
                pool,
                job.id,
                job.run_lease,
                &error_msg,
                progress_snapshot.clone(),
                queue_storage_claim,
            )
            .await?
    };
    let Some(updated_job) = updated_job else {
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
}

#[allow(clippy::too_many_arguments)]
async fn complete_job_canonical(
    pool: &PgPool,
    job: &JobRow,
    result: &Result<JobResult, JobError>,
    completion_batcher: &CompletionBatcherHandle,
    completion_path: CompletionPath,
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
                    None => Ok(CompletionOutcome::IgnoredStale),
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

            let result = match completion_path {
                CompletionPath::Direct => direct_complete_job(pool, job).await?,
                CompletionPath::Batched => {
                    match completion_batcher.complete(job.id, job.run_lease).await {
                        Ok(updated) => updated,
                        Err(_) => direct_complete_job(pool, job).await?,
                    }
                }
            };
            if !result {
                return Ok(CompletionOutcome::IgnoredStale);
            }
            if needs_event {
                let updated_job: JobRow =
                    sqlx::query_as("SELECT * FROM awa.jobs_hot WHERE id = $1")
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
            // RetryAfter reports a failed attempt with a caller-chosen delay,
            // so a retry requested on the final attempt exhausts the job
            // exactly like a retryable error would.
            if job.attempt >= job.max_attempts {
                let error_msg = format!(
                    "max_attempts exhausted (handler requested retry after {retry_duration:?})"
                );
                return exhaust_canonical(
                    pool,
                    job,
                    error_msg,
                    enqueue_specs,
                    &progress_snapshot,
                    needs_event,
                )
                .await;
            }

            let seconds = retry_duration.as_secs() as f64;

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
                    None => Ok(CompletionOutcome::IgnoredStale),
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

            let outcome = awa_model::reschedule::reschedule_canonical_attempt(
                pool,
                job.id,
                job.run_lease,
                Reschedule::RetryAfter {
                    delay_secs: seconds,
                },
                None,
                progress_snapshot.as_ref(),
            )
            .await?;
            let (RescheduleOutcome::Rescheduled {
                job_id,
                run_at,
                attempt,
            }
            | RescheduleOutcome::Migrated {
                job_id,
                run_at,
                attempt,
            }) = outcome
            else {
                return Ok(CompletionOutcome::IgnoredStale);
            };
            if needs_event {
                let updated_job = rescheduled_event_row(
                    job,
                    JobState::Retryable,
                    job_id,
                    run_at,
                    attempt,
                    None,
                    progress_snapshot,
                );
                Ok(CompletionOutcome::Applied {
                    event: Some(UntypedJobEvent::Retried {
                        job: updated_job,
                        error: String::new(),
                        attempt,
                        next_run_at: run_at,
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
            let outcome = awa_model::reschedule::reschedule_canonical_attempt(
                pool,
                job.id,
                job.run_lease,
                Reschedule::Snooze {
                    delay_secs: seconds,
                },
                None,
                progress_snapshot.as_ref(),
            )
            .await?;
            if matches!(
                outcome,
                RescheduleOutcome::Stale | RescheduleOutcome::CancelledDuplicate { .. }
            ) {
                return Ok(CompletionOutcome::IgnoredStale);
            }
            Ok(CompletionOutcome::Applied {
                event: None,
                terminal: false,
            })
        }

        Ok(JobResult::Cancel(reason)) => {
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
                    None => Ok(CompletionOutcome::IgnoredStale),
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
                UPDATE awa.jobs_hot
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
                return Ok(CompletionOutcome::IgnoredStale);
            }
            if needs_event {
                let updated_job: JobRow =
                    sqlx::query_as("SELECT * FROM awa.jobs_hot WHERE id = $1")
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
                UPDATE awa.jobs_hot
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
                // A rescued row may have moved to `scheduled_jobs`, so the
                // race check reads both canonical tables.
                let current: Option<(JobState, Option<uuid::Uuid>)> = sqlx::query_as(
                    "SELECT state, callback_id FROM awa.jobs_hot WHERE id = $1 \
                     UNION ALL \
                     SELECT state, callback_id FROM awa.scheduled_jobs WHERE id = $1",
                )
                .bind(job.id)
                .fetch_optional(pool)
                .await?;
                match current {
                    Some((state, _)) if state.is_terminal() => {
                        return Ok(CompletionOutcome::Applied {
                            event: None,
                            terminal: false,
                        });
                    }
                    Some((_, None)) => {
                        let result = sqlx::query(
                            r#"
                            UPDATE awa.jobs_hot
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
                        return Ok(CompletionOutcome::IgnoredStale);
                    }
                }
            }
            let event = if needs_event {
                let parked_job: JobRow = sqlx::query_as("SELECT * FROM awa.jobs_hot WHERE id = $1")
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
                    None => Ok(CompletionOutcome::IgnoredStale),
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
                UPDATE awa.jobs_hot
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
                return Ok(CompletionOutcome::IgnoredStale);
            }
            if needs_event {
                let updated_job: JobRow =
                    sqlx::query_as("SELECT * FROM awa.jobs_hot WHERE id = $1")
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
                exhaust_canonical(
                    pool,
                    job,
                    error_msg,
                    enqueue_specs,
                    &progress_snapshot,
                    needs_event,
                )
                .await
            } else {
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
                        None => Ok(CompletionOutcome::IgnoredStale),
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

                let error_entry = serde_json::json!({
                    "error": error_msg,
                    "attempt": job.attempt,
                    "at": chrono::Utc::now().to_rfc3339()
                });
                let outcome = awa_model::reschedule::reschedule_canonical_attempt(
                    pool,
                    job.id,
                    job.run_lease,
                    Reschedule::RetryBackoff,
                    Some(&error_entry),
                    progress_snapshot.as_ref(),
                )
                .await?;
                let (RescheduleOutcome::Rescheduled {
                    job_id,
                    run_at,
                    attempt,
                }
                | RescheduleOutcome::Migrated {
                    job_id,
                    run_at,
                    attempt,
                }) = outcome
                else {
                    return Ok(CompletionOutcome::IgnoredStale);
                };
                if needs_event {
                    let updated_job = rescheduled_event_row(
                        job,
                        JobState::Retryable,
                        job_id,
                        run_at,
                        attempt,
                        Some(&error_entry),
                        progress_snapshot,
                    );
                    Ok(CompletionOutcome::Applied {
                        event: Some(UntypedJobEvent::Retried {
                            job: updated_job,
                            error: error_msg,
                            attempt: job.attempt,
                            next_run_at: run_at,
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

/// Synthesize the post-reschedule row for event emission. After a migrated
/// re-schedule the job lives in the queue-storage deferred backlog under a
/// new id, so re-reading `awa.jobs` by the old id would return nothing.
///
/// This must mirror every mutation the reschedule persisted, not just the
/// routing fields: hooks inspecting `JobEvent::Retried.job` read `errors` to
/// see the failure that triggered the retry, and `finalized_at` to see when
/// the attempt closed.
fn rescheduled_event_row(
    job: &JobRow,
    state: JobState,
    job_id: i64,
    run_at: chrono::DateTime<chrono::Utc>,
    attempt: i16,
    error: Option<&serde_json::Value>,
    progress: Option<serde_json::Value>,
) -> JobRow {
    let mut row = job.clone();
    row.id = job_id;
    row.state = state;
    row.run_at = run_at;
    row.attempt = attempt;
    row.heartbeat_at = None;
    row.deadline_at = None;
    row.progress = progress;
    if let Some(error) = error {
        row.errors.get_or_insert_with(Vec::new).push(error.clone());
    }
    if state == JobState::Retryable {
        row.finalized_at = Some(chrono::Utc::now());
    }
    row
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
    completion_path: CompletionPath,
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
                    None => Ok(CompletionOutcome::IgnoredStale),
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

            let updated = match completion_path {
                CompletionPath::Direct => {
                    direct_complete_job_queue_storage(
                        runtime,
                        pool,
                        job,
                        queue_storage_claim,
                        queue_storage_unique_states,
                    )
                    .await?
                }
                CompletionPath::Batched => match match queue_storage_claim {
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
                    Err(_) => {
                        direct_complete_job_queue_storage(
                            runtime,
                            pool,
                            job,
                            queue_storage_claim,
                            queue_storage_unique_states,
                        )
                        .await?
                    }
                },
            };
            if !updated {
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
            // RetryAfter reports a failed attempt with a caller-chosen delay,
            // so a retry requested on the final attempt exhausts the job
            // exactly like a retryable error would.
            if job.attempt >= job.max_attempts {
                let error_msg = format!(
                    "max_attempts exhausted (handler requested retry after {retry_duration:?})"
                );
                return exhaust_queue_storage(
                    runtime,
                    pool,
                    job,
                    queue_storage_claim,
                    error_msg,
                    enqueue_specs,
                    progress_snapshot.clone(),
                    dlq_enabled,
                    metrics,
                    needs_event,
                )
                .await;
            }

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
                    queue_storage_claim,
                    *retry_duration,
                    progress_snapshot.clone(),
                    &specs,
                )
                .await?;
                return match result {
                    None => Ok(CompletionOutcome::IgnoredStale),
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
                .retry_after_with_claim(
                    pool,
                    job.id,
                    job.run_lease,
                    *retry_duration,
                    progress_snapshot.clone(),
                    queue_storage_claim,
                )
                .await?
            else {
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
            let updated = runtime
                .store
                .snooze_with_claim(
                    pool,
                    job.id,
                    job.run_lease,
                    *snooze_duration,
                    progress_snapshot.clone(),
                    queue_storage_claim,
                )
                .await?;
            if updated.is_none() {
                return Ok(CompletionOutcome::IgnoredStale);
            }
            Ok(CompletionOutcome::Applied {
                event: None,
                terminal: false,
            })
        }

        Ok(JobResult::Cancel(reason)) => {
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
                    None => Ok(CompletionOutcome::IgnoredStale),
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
                        return Ok(CompletionOutcome::Applied {
                            event: None,
                            terminal: false,
                        });
                    }
                    Some(current)
                        if current.state == JobState::Running && current.callback_id.is_none() =>
                    {
                        let failed =
                            if dlq_enabled {
                                let failed = runtime
                                .store
                                .fail_to_dlq_with_claim(
                                    pool,
                                    job.id,
                                    job.run_lease,
                                    "wait_for_callback_contract_violation",
                                    "WaitForCallback returned without calling register_callback",
                                    progress_snapshot.clone(), queue_storage_claim)
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
                                .fail_terminal_with_claim(
                                    pool,
                                    job.id,
                                    job.run_lease,
                                    "WaitForCallback returned without calling register_callback",
                                    progress_snapshot.clone(), queue_storage_claim)
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
                    queue_storage_claim,
                    "terminal_error",
                    msg,
                    progress_snapshot.clone(),
                    dlq_enabled,
                    metrics,
                    &specs,
                )
                .await?;
                return match result {
                    None => Ok(CompletionOutcome::IgnoredStale),
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
                    .fail_to_dlq_with_claim(
                        pool,
                        job.id,
                        job.run_lease,
                        "terminal_error",
                        msg,
                        progress_snapshot.clone(),
                        queue_storage_claim,
                    )
                    .await?;
                if moved.is_some() {
                    metrics.record_dlq_moved(&job.kind, &job.queue, "terminal_error");
                }
                moved
            } else {
                runtime
                    .store
                    .fail_terminal_with_claim(
                        pool,
                        job.id,
                        job.run_lease,
                        msg,
                        progress_snapshot.clone(),
                        queue_storage_claim,
                    )
                    .await?
            };
            let Some(updated_job) = updated_job else {
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
                exhaust_queue_storage(
                    runtime,
                    pool,
                    job,
                    queue_storage_claim,
                    error_msg,
                    enqueue_specs,
                    progress_snapshot.clone(),
                    dlq_enabled,
                    metrics,
                    needs_event,
                )
                .await
            } else {
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
                        queue_storage_claim,
                        &error_msg,
                        progress_snapshot.clone(),
                        &specs,
                    )
                    .await?;
                    return match result {
                        None => Ok(CompletionOutcome::IgnoredStale),
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
                    .fail_retryable_with_claim(
                        pool,
                        job.id,
                        job.run_lease,
                        &error_msg,
                        progress_snapshot.clone(),
                        queue_storage_claim,
                    )
                    .await?
                else {
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
    queue_storage_claim: Option<&ClaimedEntry>,
    retry_duration: Duration,
    progress_snapshot: Option<serde_json::Value>,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;
    let Some(updated_job) = runtime
        .store
        .retry_after_in_tx_with_claim(
            &mut tx,
            job.id,
            job.run_lease,
            retry_duration,
            progress_snapshot,
            queue_storage_claim,
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
    queue_storage_claim: Option<&ClaimedEntry>,
    error_msg: &str,
    progress_snapshot: Option<serde_json::Value>,
    specs: &[crate::enqueue_specs::BoxedEnqueueSpec],
) -> Result<Option<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;
    let Some(updated_job) = runtime
        .store
        .fail_retryable_in_tx_with_claim(
            &mut tx,
            job.id,
            job.run_lease,
            error_msg,
            progress_snapshot,
            queue_storage_claim,
        )
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
    queue_storage_claim: Option<&ClaimedEntry>,
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
            .fail_to_dlq_in_tx_with_claim(
                &mut tx,
                job.id,
                job.run_lease,
                dlq_reason,
                error_msg,
                progress_snapshot,
                queue_storage_claim,
            )
            .await?;
        let routed = moved.is_some();
        (moved, routed)
    } else {
        let moved = runtime
            .store
            .fail_terminal_in_tx_with_claim(
                &mut tx,
                job.id,
                job.run_lease,
                error_msg,
                progress_snapshot,
                queue_storage_claim,
            )
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
    // Re-read the parked row inside the same transaction. The input
    // `job` is the snapshot the executor claimed earlier — it does not
    // carry `callback_id` / `callback_timeout_at` written by
    // `register_callback()` during the handler, and the parked-state
    // fields (state, heartbeat_at, deadline_at) only land after the
    // enter_callback_wait_in_tx UPDATE above. Fall back to a synthesised
    // snapshot only if the SELECT misses (shouldn't happen because the
    // UPDATE just matched, but defensive).
    let parked_job = match runtime
        .store
        .load_active_lease_in_tx(&mut tx, job.id, job.run_lease)
        .await?
    {
        Some(row) => row,
        None => {
            let mut parked = job.clone();
            parked.state = JobState::WaitingExternal;
            parked.heartbeat_at = None;
            parked.deadline_at = None;
            parked.callback_id = Some(callback_id);
            parked
        }
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
            let result = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
                (handler)(event).await;
            }))
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

    let outcome = awa_model::reschedule::reschedule_canonical_attempt_tx(
        &mut tx,
        job.id,
        job.run_lease,
        Reschedule::RetryAfter {
            delay_secs: seconds,
        },
        None,
        progress_snapshot,
    )
    .await?;
    let (job_id, run_at, attempt) = match outcome {
        RescheduleOutcome::Rescheduled {
            job_id,
            run_at,
            attempt,
        }
        | RescheduleOutcome::Migrated {
            job_id,
            run_at,
            attempt,
        } => (job_id, run_at, attempt),
        RescheduleOutcome::CancelledDuplicate { .. } => {
            tx.commit().await?;
            return Ok(None);
        }
        RescheduleOutcome::Stale => {
            tx.rollback().await?;
            return Ok(None);
        }
    };
    let updated_job = rescheduled_event_row(
        job,
        JobState::Retryable,
        job_id,
        run_at,
        attempt,
        None,
        progress_snapshot.cloned(),
    );

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

    let error_entry = serde_json::json!({
        "error": error_msg,
        "attempt": job.attempt,
        "at": chrono::Utc::now().to_rfc3339()
    });
    let outcome = awa_model::reschedule::reschedule_canonical_attempt_tx(
        &mut tx,
        job.id,
        job.run_lease,
        Reschedule::RetryBackoff,
        Some(&error_entry),
        progress_snapshot,
    )
    .await?;
    let (job_id, run_at, attempt) = match outcome {
        RescheduleOutcome::Rescheduled {
            job_id,
            run_at,
            attempt,
        }
        | RescheduleOutcome::Migrated {
            job_id,
            run_at,
            attempt,
        } => (job_id, run_at, attempt),
        RescheduleOutcome::CancelledDuplicate { .. } => {
            tx.commit().await?;
            return Ok(None);
        }
        RescheduleOutcome::Stale => {
            tx.rollback().await?;
            return Ok(None);
        }
    };
    let updated_job = rescheduled_event_row(
        job,
        JobState::Retryable,
        job_id,
        run_at,
        attempt,
        Some(&error_entry),
        progress_snapshot.cloned(),
    );

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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_metrics() -> crate::metrics::AwaMetrics {
        crate::metrics::AwaMetrics::new(&opentelemetry::global::meter("finalize-test"))
    }

    #[tokio::test]
    async fn cancelling_lifecycle_dispatch_drops_its_pending_hook() {
        struct NotifyOnDrop(Arc<tokio::sync::Notify>);

        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }

        let hook_started = Arc::new(tokio::sync::Notify::new());
        let hook_dropped = Arc::new(tokio::sync::Notify::new());
        let handler_started = hook_started.clone();
        let handler_dropped = hook_dropped.clone();
        let hook: BoxedUntypedEventHandler = Arc::new(move |_event| {
            let handler_started = handler_started.clone();
            let handler_dropped = handler_dropped.clone();
            Box::pin(async move {
                let _drop_notification = NotifyOnDrop(handler_dropped);
                handler_started.notify_one();
                std::future::pending::<()>().await;
            })
        });
        let now = chrono::Utc::now();
        let job = JobRow {
            id: 1,
            kind: "pending_hook".into(),
            queue: "test".into(),
            args: serde_json::json!({}),
            state: JobState::Completed,
            priority: 2,
            attempt: 1,
            run_lease: 1,
            max_attempts: 5,
            run_at: now,
            heartbeat_at: None,
            deadline_at: None,
            attempted_at: Some(now),
            finalized_at: Some(now),
            created_at: now,
            errors: None,
            metadata: serde_json::json!({}),
            tags: Vec::new(),
            unique_key: None,
            unique_states: None,
            callback_id: None,
            callback_timeout_at: None,
            callback_filter: None,
            callback_on_complete: None,
            callback_on_fail: None,
            callback_transform: None,
            progress: None,
        };
        let handlers = HashMap::from([("pending_hook".into(), vec![hook])]);
        let dispatch = tokio::spawn(async move {
            dispatch_lifecycle_event(
                &handlers,
                "pending_hook",
                UntypedJobEvent::Completed {
                    job,
                    duration: Duration::ZERO,
                },
            )
            .await;
        });
        tokio::time::timeout(Duration::from_secs(2), hook_started.notified())
            .await
            .expect("hook must be running before dispatch cancellation");
        dispatch.abort();
        assert!(dispatch.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), hook_dropped.notified())
            .await
            .expect("cancelling dispatch must drop the spawned hook future");
    }

    #[tokio::test(start_paused = true)]
    async fn finalize_deadline_bounds_an_unresponsive_attempt() {
        let started = tokio::time::Instant::now();
        let result = finalize_with_retry(
            |_| std::future::pending::<Result<(), AwaError>>(),
            &test_metrics(),
            1,
            "test",
            "test",
            Duration::from_secs(2),
        )
        .await;
        assert!(matches!(result, Err(FinalizeError::Deadline)));
        assert_eq!(started.elapsed(), Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn finalize_deadline_bounds_reconnect_backoff_and_retries() {
        let started = tokio::time::Instant::now();
        let calls = std::sync::atomic::AtomicU32::new(0);
        let result = finalize_with_retry(
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err::<(), _>(AwaError::Database(sqlx::Error::PoolTimedOut)))
            },
            &test_metrics(),
            1,
            "test",
            "test",
            Duration::from_secs(2),
        )
        .await;
        assert!(matches!(result, Err(FinalizeError::Deadline)));
        assert_eq!(started.elapsed(), Duration::from_secs(2));
        assert!((3..=4).contains(&calls.load(Ordering::SeqCst)));
    }

    #[tokio::test(start_paused = true)]
    async fn finalize_does_not_retry_statement_or_closed_pool_errors() {
        let calls = std::sync::atomic::AtomicU32::new(0);
        let result = finalize_with_retry(
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err::<(), _>(AwaError::Database(sqlx::Error::PoolClosed)))
            },
            &test_metrics(),
            1,
            "test",
            "test",
            Duration::from_secs(120),
        )
        .await;
        assert!(matches!(result, Err(FinalizeError::Operation(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    // Fresh partitioned-schema fixtures share PostgreSQL's cluster-wide lock table.
    static RECONNECT_DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test(start_paused = true)]
    async fn retries_bypass_batching_and_only_the_first_failure_warns() {
        use std::io::Write;
        use tracing::instrument::WithSubscriber;

        struct LogWriter(Arc<std::sync::Mutex<Vec<u8>>>);

        impl Write for LogWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let logs = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer_logs = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || LogWriter(writer_logs.clone()))
            .finish();
        let paths = std::sync::Mutex::new(Vec::new());
        let ack = finalize_with_retry(
            |path| {
                let mut paths = paths.lock().unwrap();
                paths.push(path);
                std::future::ready(if paths.len() <= 3 {
                    Err(connection_reset())
                } else {
                    Ok(42)
                })
            },
            &test_metrics(),
            1,
            "test",
            "test",
            Duration::from_secs(120),
        )
        .with_subscriber(subscriber)
        .await
        .unwrap();
        assert_eq!(ack.outcome, 42);
        assert_eq!(ack.reconnect_retries, 3);
        assert_eq!(
            *paths.lock().unwrap(),
            [
                CompletionPath::Batched,
                CompletionPath::Direct,
                CompletionPath::Direct,
                CompletionPath::Direct,
            ]
        );
        let logs = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
        let retry_lines: Vec<_> = logs
            .lines()
            .filter(|line| line.contains("Durable finalization failed; retrying"))
            .collect();
        assert_eq!(retry_lines.len(), 3, "{logs}");
        assert_eq!(
            retry_lines
                .iter()
                .filter(|line| line.contains("WARN"))
                .count(),
            1,
            "{logs}"
        );
        assert_eq!(
            retry_lines
                .iter()
                .filter(|line| line.contains("DEBUG"))
                .count(),
            2,
            "{logs}"
        );
    }

    #[sqlx::test(migrations = false)]
    async fn direct_finalization_does_not_wait_for_the_completion_batcher(pool: PgPool) {
        let _database_guard = RECONNECT_DB_LOCK.lock().await;
        awa_model::migrations::run(&pool).await.unwrap();
        let canonical_job = running_reconnect_job(&pool).await;
        let runtime = QueueStorageRuntime::new(
            awa_model::QueueStorageConfig::default(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
        runtime
            .store
            .enqueue_batch(&pool, "direct_retry", 2, 1)
            .await
            .unwrap();
        let runtime_job = runtime
            .store
            .claim_runtime_batch(&pool, "direct_retry", 1, Duration::from_secs(60))
            .await
            .unwrap()
            .pop()
            .unwrap();
        let metrics = test_metrics();
        for (job, claim, storage) in [
            (canonical_job, None, RuntimeStorage::Canonical),
            (
                runtime_job.job,
                Some(runtime_job.claim),
                RuntimeStorage::QueueStorage(runtime),
            ),
        ] {
            let (_unstarted_batcher, handle) = crate::completion::CompletionBatcher::new(
                pool.clone(),
                CancellationToken::new(),
                metrics.clone(),
                storage.clone(),
                1,
            );
            let outcome = tokio::time::timeout(
                Duration::from_secs(5),
                complete_job(
                    &pool,
                    &job,
                    claim.as_ref(),
                    None,
                    &Ok(JobResult::Completed),
                    &handle,
                    CompletionPath::Direct,
                    None,
                    Duration::ZERO,
                    false,
                    &Arc::new(HashMap::new()),
                    &storage,
                    false,
                    &metrics,
                ),
            )
            .await
            .expect("a direct retry must not wait on an unstarted batcher")
            .unwrap();
            assert!(matches!(outcome, CompletionOutcome::Applied { .. }));
            let state = match storage {
                RuntimeStorage::Canonical => sqlx::query_scalar::<_, JobState>(
                    "SELECT state FROM awa.jobs_hot WHERE id = $1",
                )
                .bind(job.id)
                .fetch_one(&pool)
                .await
                .unwrap(),
                RuntimeStorage::QueueStorage(runtime) => {
                    runtime
                        .store
                        .load_job(&pool, job.id)
                        .await
                        .unwrap()
                        .unwrap()
                        .state
                }
            };
            assert_eq!(state, JobState::Completed);
        }
    }

    struct ReconnectFollowup;

    impl crate::enqueue_specs::EnqueueFollowUp for ReconnectFollowup {
        fn run<'a>(
            &'a self,
            connection: &'a mut sqlx::PgConnection,
            job: &'a JobRow,
            _context: Option<&'a crate::enqueue_specs::OutcomeContext>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), AwaError>> + Send + 'a>>
        {
            Box::pin(async move {
                sqlx::query("INSERT INTO awa.jobs_hot (kind, queue, args) VALUES ('reconnect_child', 'reconnect_test', $1)")
                    .bind(serde_json::json!({"parent": job.id}))
                    .execute(connection).await?;
                Ok(())
            })
        }
    }

    async fn running_reconnect_job(pool: &PgPool) -> JobRow {
        sqlx::query_as("INSERT INTO awa.jobs_hot (kind, queue, args, state, run_lease, attempt, max_attempts) VALUES ('reconnect_parent', 'reconnect_test', '{}', 'running', 1, 1, 5) RETURNING *")
            .fetch_one(pool).await.unwrap()
    }

    fn reconnect_specs() -> Arc<EnqueueSpecsByOutcome> {
        let mut registry = HashMap::new();
        for outcome in [
            crate::enqueue_specs::Outcome::Completed,
            crate::enqueue_specs::Outcome::Retried,
        ] {
            let specs: Vec<crate::enqueue_specs::BoxedEnqueueSpec> =
                vec![Arc::new(ReconnectFollowup)];
            registry.insert(outcome, HashMap::from([("reconnect_parent".into(), specs)]));
        }
        Arc::new(registry)
    }

    fn connection_reset() -> AwaError {
        AwaError::Database(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "lost acknowledgement",
        )))
    }

    #[sqlx::test(migrations = false)]
    async fn reconnect_replays_committed_outcomes_without_duplicate_followups(pool: PgPool) {
        let _database_guard = RECONNECT_DB_LOCK.lock().await;
        awa_model::migrations::run(&pool).await.unwrap();
        let metrics = test_metrics();
        let specs = reconnect_specs();
        let (_, batcher) = crate::completion::CompletionBatcher::new(
            pool.clone(),
            CancellationToken::new(),
            metrics.clone(),
            RuntimeStorage::Canonical,
            1,
        );
        for result in [
            Ok(JobResult::Completed),
            Ok(JobResult::RetryAfter(Duration::from_secs(60))),
            Ok(JobResult::Snooze(Duration::from_secs(60))),
        ] {
            let job = running_reconnect_job(&pool).await;
            let calls = AtomicU32::new(0);
            let outcome = finalize_with_retry(
                |completion_path| {
                    let pool = &pool;
                    let job = &job;
                    let result = &result;
                    let batcher = &batcher;
                    let specs = &specs;
                    let metrics = &metrics;
                    let calls = &calls;
                    async move {
                        let outcome = complete_job_canonical(
                            pool,
                            job,
                            result,
                            batcher,
                            completion_path,
                            None,
                            Duration::ZERO,
                            false,
                            specs,
                            false,
                            metrics,
                        )
                        .await?;
                        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            assert!(matches!(outcome, CompletionOutcome::Applied { .. }));
                            Err(connection_reset())
                        } else {
                            Ok(outcome)
                        }
                    }
                },
                &metrics,
                job.id,
                &job.kind,
                &job.queue,
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert!(matches!(outcome.outcome, CompletionOutcome::IgnoredStale));
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            let children: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.jobs_hot WHERE kind = 'reconnect_child' AND args->>'parent' = $1")
                .bind(job.id.to_string()).fetch_one(&pool).await.unwrap();
            assert_eq!(
                children,
                if matches!(result, Ok(JobResult::Snooze(_))) {
                    0
                } else {
                    1
                }
            );
        }
    }

    #[sqlx::test(migrations = false)]
    async fn reconnect_saved_result_cannot_complete_a_newer_lease(pool: PgPool) {
        let _database_guard = RECONNECT_DB_LOCK.lock().await;
        awa_model::migrations::run(&pool).await.unwrap();
        let job = running_reconnect_job(&pool).await;
        let metrics = test_metrics();
        let specs = reconnect_specs();
        let (_, batcher) = crate::completion::CompletionBatcher::new(
            pool.clone(),
            CancellationToken::new(),
            metrics.clone(),
            RuntimeStorage::Canonical,
            1,
        );
        let calls = AtomicU32::new(0);
        let outcome = finalize_with_retry(
            |completion_path| {
                let pool = &pool;
                let job = &job;
                let batcher = &batcher;
                let specs = &specs;
                let metrics = &metrics;
                let calls = &calls;
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        sqlx::query("UPDATE awa.jobs_hot SET run_lease = run_lease + 1, attempt = attempt + 1 WHERE id = $1")
                            .bind(job.id).execute(pool).await.unwrap();
                        return Err(connection_reset());
                    }
                    complete_job_canonical(
                        pool, job, &Ok(JobResult::Completed), batcher, completion_path,
                        None, Duration::ZERO, false, specs, false, metrics,
                    ).await
                }
            },
            &metrics, job.id, &job.kind, &job.queue, Duration::from_secs(5),
        ).await.unwrap();
        assert!(matches!(outcome.outcome, CompletionOutcome::IgnoredStale));
        let current: JobRow = sqlx::query_as("SELECT * FROM awa.jobs_hot WHERE id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(current.run_lease, job.run_lease + 1);
        assert_eq!(current.state, JobState::Running);
        let children: i64 =
            sqlx::query_scalar("SELECT count(*) FROM awa.jobs_hot WHERE kind = 'reconnect_child'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(children, 0);
    }

    struct ReconnectWorker(Arc<AtomicU32>);

    #[async_trait::async_trait]
    impl Worker for ReconnectWorker {
        fn kind(&self) -> &'static str {
            "reconnect_parent"
        }
        async fn perform(&self, _context: &JobContext) -> Result<JobResult, JobError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(JobResult::Completed)
        }
    }

    #[sqlx::test(migrations = false)]
    async fn pending_outcome_hook_does_not_hold_durable_finalizer_capacity(pool: PgPool) {
        let _database_guard = RECONNECT_DB_LOCK.lock().await;
        awa_model::migrations::run(&pool).await.unwrap();
        let first = running_reconnect_job(&pool).await;
        let second = running_reconnect_job(&pool).await;
        let first_id = first.id;
        let second_id = second.id;
        let hook_started = Arc::new(tokio::sync::Notify::new());
        let second_completed = Arc::new(tokio::sync::Notify::new());
        let release_hook = CancellationToken::new();
        let handler_started = hook_started.clone();
        let handler_completed = second_completed.clone();
        let handler_release = release_hook.clone();
        let hook: BoxedUntypedEventHandler = Arc::new(move |event| {
            let handler_started = handler_started.clone();
            let handler_completed = handler_completed.clone();
            let handler_release = handler_release.clone();
            Box::pin(async move {
                if let UntypedJobEvent::Completed { job, .. } = event {
                    if job.id == first_id {
                        handler_started.notify_one();
                        handler_release.cancelled().await;
                    } else if job.id == second_id {
                        handler_completed.notify_one();
                    }
                }
            })
        });
        let metrics = test_metrics();
        let finalizers = TaskTracker::new();
        let cancel = CancellationToken::new();
        let (_batcher, handle) = crate::completion::CompletionBatcher::new(
            pool.clone(),
            cancel.clone(),
            metrics.clone(),
            RuntimeStorage::Canonical,
            1,
        );
        let workers: HashMap<String, BoxedWorker> = HashMap::from([(
            "reconnect_parent".into(),
            Box::new(ReconnectWorker(Arc::new(AtomicU32::new(0)))) as BoxedWorker,
        )]);
        let mut executor = JobExecutor::new(
            pool.clone(),
            Arc::new(workers),
            Arc::new(HashMap::from([("reconnect_parent".into(), vec![hook])])),
            reconnect_specs(),
            Arc::new(crate::runtime::InFlightRegistry::default()),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            metrics,
            handle,
            RuntimeStorage::Canonical,
            DlqPolicy::default(),
            finalizers.clone(),
            cancel,
            1,
        );
        executor.finalize_capacity = Arc::new(Semaphore::new(1));
        let dispatched = |job| DispatchedJob {
            job,
            queue_storage_claim: None,
            queue_storage_unique_states: None,
        };
        executor
            .execute_task(dispatched(first), Arc::new(AtomicBool::new(false)))
            .await;
        tokio::time::timeout(Duration::from_secs(2), hook_started.notified())
            .await
            .expect("first durable finalize must reach its outcome hook");
        tokio::time::timeout(
            Duration::from_secs(2),
            executor.execute_task(dispatched(second), Arc::new(AtomicBool::new(false))),
        )
        .await
        .expect("pending outcome hook must not consume the only finalize permit");
        tokio::time::timeout(Duration::from_secs(2), second_completed.notified())
            .await
            .expect("second durable finalize must finish while the first hook is pending");
        let states: Vec<String> =
            sqlx::query_scalar("SELECT state::text FROM awa.jobs_hot WHERE id = ANY($1)")
                .bind([first_id, second_id].as_slice())
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(states.len(), 2);
        assert!(states.iter().all(|state| state == "completed"));
        release_hook.cancel();
        finalizers.close();
        tokio::time::timeout(Duration::from_secs(2), finalizers.wait())
            .await
            .expect("tracked outcome hooks must drain after release");
    }

    #[sqlx::test(migrations = false)]
    async fn finalizer_capacity_backpressures_handlers_and_cancellation_releases_them(
        pool: PgPool,
    ) {
        let _database_guard = RECONNECT_DB_LOCK.lock().await;
        awa_model::migrations::run(&pool).await.unwrap();
        let first = running_reconnect_job(&pool).await;
        let second = running_reconnect_job(&pool).await;
        let metrics = test_metrics();
        let finalizers = TaskTracker::new();
        let cancel = CancellationToken::new();
        let in_flight = Arc::new(crate::runtime::InFlightRegistry::default());
        let runs = Arc::new(AtomicU32::new(0));
        // Keep the unstarted batcher alive: finalization cannot receive an acknowledgement.
        let (_batcher, handle) = crate::completion::CompletionBatcher::new(
            pool.clone(),
            cancel.clone(),
            metrics.clone(),
            RuntimeStorage::Canonical,
            1,
        );
        let workers: HashMap<String, BoxedWorker> = HashMap::from([(
            "reconnect_parent".into(),
            Box::new(ReconnectWorker(runs.clone())) as BoxedWorker,
        )]);
        let mut executor = JobExecutor::new(
            pool,
            Arc::new(workers),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            in_flight.clone(),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            metrics,
            handle,
            RuntimeStorage::Canonical,
            DlqPolicy::default(),
            finalizers.clone(),
            cancel.clone(),
            1,
        );
        executor.finalize_capacity = Arc::new(Semaphore::new(1));
        let dispatched = |job| DispatchedJob {
            job,
            queue_storage_claim: None,
            queue_storage_unique_states: None,
        };
        executor
            .execute_task(dispatched(first), Arc::new(AtomicBool::new(false)))
            .await;
        assert_eq!(finalizers.len(), 1);
        let second_key = (second.id, second.run_lease);
        let second_task = tokio::spawn(
            executor.execute_task(dispatched(second), Arc::new(AtomicBool::new(false))),
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while runs.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !second_task.is_finished(),
            "worker ownership must remain while finalizer capacity is full"
        );
        assert!(in_flight.get_cancel(second_key).is_some());
        assert_eq!(finalizers.len(), 1);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), second_task)
            .await
            .unwrap()
            .unwrap();
        finalizers.close();
        tokio::time::timeout(Duration::from_secs(2), finalizers.wait())
            .await
            .unwrap();
        assert!(in_flight.get_cancel(second_key).is_none());
        assert!(finalizers.is_empty());
    }

    #[sqlx::test(migrations = false)]
    async fn finalizers_fill_completion_pipeline_then_backpressure_handlers(pool: PgPool) {
        let _database_guard = RECONNECT_DB_LOCK.lock().await;
        awa_model::migrations::run(&pool).await.unwrap();
        let mut jobs = Vec::new();
        for _ in 0..12 {
            jobs.push(running_reconnect_job(&pool).await);
        }
        let job_ids: Vec<i64> = jobs.iter().map(|job| job.id).collect();
        let mut blocking_tx = pool.begin().await.unwrap();
        let blocking_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *blocking_tx)
            .await
            .unwrap();
        sqlx::query("SELECT id FROM awa.jobs_hot WHERE id = ANY($1) FOR UPDATE")
            .bind(&job_ids[..4])
            .fetch_all(&mut *blocking_tx)
            .await
            .unwrap();

        let metrics = test_metrics();
        let finalizers = TaskTracker::new();
        let cancel = CancellationToken::new();
        let _cancel_on_drop = cancel.clone().drop_guard();
        let in_flight = Arc::new(crate::runtime::InFlightRegistry::default());
        let runs = Arc::new(AtomicU32::new(0));
        let (batcher, handle) = crate::completion::CompletionBatcher::with_batching(
            pool.clone(),
            cancel.clone(),
            metrics.clone(),
            RuntimeStorage::Canonical,
            1,
            4,
            Duration::from_secs(3600),
        );
        let workers: HashMap<String, BoxedWorker> = HashMap::from([(
            "reconnect_parent".into(),
            Box::new(ReconnectWorker(runs.clone())) as BoxedWorker,
        )]);
        let executor = JobExecutor::new(
            pool.clone(),
            Arc::new(workers),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            in_flight.clone(),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            metrics,
            handle,
            RuntimeStorage::Canonical,
            DlqPolicy::default(),
            finalizers.clone(),
            cancel.clone(),
            1,
        );
        let dispatched = |job| DispatchedJob {
            job,
            queue_storage_claim: None,
            queue_storage_unique_states: None,
        };
        let mut jobs = jobs.into_iter();
        for job in jobs.by_ref().take(4) {
            tokio::time::timeout(
                Duration::from_secs(5),
                executor.execute_task(dispatched(job), Arc::new(AtomicBool::new(false))),
            )
            .await
            .expect("short handlers must fill a complete batch even with one worker");
        }
        let batcher_handles = batcher.spawn();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let blocked: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                     WHERE $1 = ANY(pg_blocking_pids(pid)))",
                )
                .bind(blocking_pid)
                .fetch_one(&pool)
                .await
                .unwrap();
                if blocked {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the full first batch must reach its blocked SQL flush");
        for job in jobs.by_ref().take(4) {
            tokio::time::timeout(
                Duration::from_secs(5),
                executor.execute_task(dispatched(job), Arc::new(AtomicBool::new(false))),
            )
            .await
            .expect("another complete batch must queue while the first flush is blocked");
        }
        let waiting_job = jobs.next().unwrap();
        let waiting_key = (waiting_job.id, waiting_job.run_lease);
        let waiting_task = tokio::spawn(
            executor.execute_task(dispatched(waiting_job), Arc::new(AtomicBool::new(false))),
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while runs.load(Ordering::SeqCst) != 9 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!waiting_task.is_finished());
        assert!(in_flight.get_cancel(waiting_key).is_some());
        assert_eq!(finalizers.len(), 8);

        blocking_tx.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), waiting_task)
            .await
            .unwrap()
            .unwrap();
        for job in jobs {
            executor
                .execute_task(dispatched(job), Arc::new(AtomicBool::new(false)))
                .await;
        }
        finalizers.close();
        tokio::time::timeout(Duration::from_secs(5), finalizers.wait())
            .await
            .expect("three full batches must persist and drain without the flush timer");
        let completed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.jobs_hot WHERE id = ANY($1) AND state = 'completed'",
        )
        .bind(&job_ids)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(completed, job_ids.len() as i64);
        assert_eq!(runs.load(Ordering::SeqCst), 12);
        assert!(in_flight.get_cancel(waiting_key).is_none());
        cancel.cancel();
        for handle in batcher_handles {
            tokio::time::timeout(Duration::from_secs(5), handle)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[test]
    fn reconnect_backoff_spreads_retries_within_bounded_equal_jitter() {
        for (retry, ceiling_ms) in [
            (1, 500),
            (2, 1000),
            (3, 2000),
            (4, 4000),
            (5, 4000),
            (40, 4000),
        ] {
            let samples: std::collections::HashSet<_> = (0..64)
                .map(|_| completion_reconnect_backoff(retry))
                .collect();
            assert!(
                samples.len() > 1,
                "retry {retry} must not use a fixed delay"
            );
            assert!(samples.iter().all(|delay| {
                *delay >= Duration::from_millis(ceiling_ms / 2)
                    && *delay <= Duration::from_millis(ceiling_ms)
            }));
        }
    }
}
