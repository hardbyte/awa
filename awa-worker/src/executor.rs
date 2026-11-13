use crate::completion::{CompletionBatcherHandle, CompletionError};
use crate::context::{CallbackGuard, JobContext};
use crate::events::{BoxedUntypedEventHandler, UntypedJobEvent};
use crate::runtime::{lock_progress, InFlightMap, InFlightState, ProgressState};
use crate::storage::{QueueStorageRuntime, RuntimeStorage};
use awa_model::reschedule::{Reschedule, RescheduleOutcome};
use awa_model::{AwaError, ClaimedEntry, ClaimedRuntimeJob, JobRow, JobState};
use sqlx::PgPool;
use std::any::Any;
use std::collections::HashMap;
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

/// Polls a handler future, turning a panic into an `Err` with its payload.
///
/// Without this a panicking handler unwinds out of the job task before
/// finalization: the job stays registered in-flight, heartbeats keep it
/// looking alive until its deadline, the attempt is never counted, and the
/// panic message is lost. Caught, a panic is an ordinary retryable failure.
struct CatchUnwind<F>(F);

impl<F: std::future::Future + Unpin> std::future::Future for CatchUnwind<F> {
    type Output = Result<F::Output, Box<dyn Any + Send>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // Unwind safety: the handler future is dropped after a panic and
        // never polled again, so no broken invariant inside it is observed.
        let inner = &mut self.0;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::pin::Pin::new(inner).poll(cx)
        })) {
            Ok(std::task::Poll::Ready(output)) => std::task::Poll::Ready(Ok(output)),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(payload) => std::task::Poll::Ready(Err(payload)),
        }
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string())
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
    /// How long the handler may run before the executor drops it, set past
    /// the queue's deadline so maintenance has rescued the attempt by then.
    /// `None` when the queue has no deadline.
    pub abandon_after: Option<Duration>,
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
        let abandon_after = dispatched.abandon_after;
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
                claim: queue_storage_claim.as_ref().and_then(|claim| {
                    awa_model::queue_storage::ClaimHeartbeatRef::from_claim(
                        job_id,
                        job_run_lease,
                        claim,
                    )
                }),
                finalizing: false,
            };
            in_flight.insert((job_id, job_run_lease), in_flight_state);
            let heartbeat_guard = storage.queue_storage().map(|_| FinalizationHeartbeatGuard {
                in_flight: in_flight.clone(),
                key: (job_id, job_run_lease),
            });
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

            let mut started_event = None;
            let result = match workers.get(&job.kind) {
                Some(worker) => {
                    if has_lifecycle_handlers {
                        let started_handlers = lifecycle_handlers.clone();
                        let started_kind = job_kind.clone();
                        let started_job = job.clone();
                        started_event = Some(tokio::spawn(async move {
                            dispatch_lifecycle_event(
                                &started_handlers,
                                &started_kind,
                                UntypedJobEvent::Started { job: started_job },
                            )
                            .await;
                        }));
                    }
                    let perform = CatchUnwind(worker.perform(&ctx));
                    let outcome = match abandon_after {
                        Some(limit) => tokio::time::timeout(limit, perform).await.ok(),
                        None => Some(perform.await),
                    };
                    match outcome {
                        Some(Ok(result)) => Some(result),
                        Some(Err(panic)) => {
                            let message = panic_message(panic.as_ref());
                            error!(kind = %job.kind, job_id, panic = %message, "Job handler panicked");
                            Some(Err(JobError::retryable_msg(format!(
                                "handler panicked: {message}"
                            ))))
                        }
                        // The deadline rescue has taken this attempt; a
                        // handler that ignored the cancel signal would
                        // otherwise hold its worker slot forever and starve
                        // the retry. Drop it and leave the outcome to rescue.
                        None => {
                            warn!(
                                kind = %job.kind,
                                job_id,
                                "Abandoned a handler still running well past its deadline"
                            );
                            None
                        }
                    }
                }
                None => {
                    error!(kind = %job.kind, job_id, "No worker registered for job kind");
                    Some(Err(JobError::Terminal(format!(
                        "unknown job kind: {}",
                        job.kind
                    ))))
                }
            };

            let duration = start.elapsed();

            // Snapshot progress for state transition
            let progress_snapshot = {
                let guard = lock_progress(&progress_state);
                guard.clone_latest()
            };

            // The handler's progress snapshot is final: the finalize settles it,
            // so heartbeats stop flushing progress for this attempt.
            in_flight.mark_finalizing((job_id, job_run_lease));
            // Keep worker capacity and heartbeats until a bounded finalizer slot
            // is available. Otherwise returning handlers can create an unbounded
            // set of tasks retaining job payloads while completion is stalled.
            let finalize_permit = match result {
                Some(_) => tokio::select! {
                    biased;
                    _ = finalize_cancel.cancelled() => None,
                    permit = finalize_capacity.acquire_owned() => {
                        Some(permit.expect("finalizer capacity is never closed"))
                    }
                },
                None => None,
            };

            if heartbeat_guard.is_none() {
                in_flight.remove((job_id, job_run_lease));
            }
            if let Some(counter) = queue_in_flight.get(&job_queue) {
                counter.fetch_sub(1, Ordering::SeqCst);
            }
            metrics.record_in_flight_change(&job_queue, -1);

            let (Some(result), Some(finalize_permit)) = (result, finalize_permit) else {
                return;
            };
            let dlq_enabled = dlq_policy.enabled_for(&job_queue);
            finalizers.spawn(async move {
                let outcome = finalize_with_retry(
                    || complete_job(
                        &pool,
                        &job,
                        queue_storage_claim.as_ref(),
                        queue_storage_unique_states.as_deref(),
                        &result,
                        &completion_batcher,
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
                    COMPLETION_LOCK_WINDOW,
                    storage.queue_storage().is_some(),
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
                                if *terminal {
                                    metrics.record_job_failed(&job_kind, &job_queue, true);
                                } else {
                                    metrics.record_job_retried(&job_kind, &job_queue);
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

                drop(heartbeat_guard);
                // Lifecycle hooks run under the finalizer permit, so a hook slower
                // than job throughput holds back claiming instead of accumulating
                // tasks that each retain a copy of the job.
                if let Some(started_event) = started_event {
                    let _ = started_event.await;
                }
                if let Ok(CompletionOutcome::Applied {
                    event: Some(event), ..
                }) = outcome
                {
                    dispatch_lifecycle_event(&lifecycle_handlers, &job_kind, event).await;
                }
                drop(finalize_permit);
            });
        }
        .instrument(span)
    }
}

struct FinalizationHeartbeatGuard {
    in_flight: InFlightMap,
    key: (i64, i64),
}

impl Drop for FinalizationHeartbeatGuard {
    fn drop(&mut self) {
        self.in_flight.remove(self.key);
    }
}

/// How many times a finalize transaction is re-run after Postgres aborts it
/// to break a lock cycle before the failure is surfaced.
const COMPLETION_DEADLOCK_RETRIES: u32 = 3;

fn completion_contention_backoff(retry: u32) -> Duration {
    Duration::from_millis(10u64 << retry.saturating_sub(1).min(4))
}

/// Why one finalization attempt did not finish.
#[derive(Debug, thiserror::Error)]
enum FinalizeAttemptError {
    #[error(transparent)]
    Operation(#[from] AwaError),
    /// Another transaction holds the attempt's completion lock. Nothing was
    /// written; the attempt is finalized again after a backoff.
    #[error("another transaction holds the attempt's completion lock")]
    LockHeld,
}

/// How long an attempt may wait on a held completion lock before the wait is
/// logged as a warning. The runtime's own heartbeat and progress flush hold
/// the lock for the length of one short transaction, which is not worth one.
const COMPLETION_LOCK_HELD_WARN_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug, thiserror::Error)]
enum FinalizeError {
    #[error(transparent)]
    Operation(#[from] AwaError),
    #[error("durable finalization lock-retry window expired")]
    Deadline,
}

#[allow(clippy::too_many_arguments)]
async fn finalize_with_retry<T, E, F, Fut>(
    mut finalize: F,
    metrics: &crate::metrics::AwaMetrics,
    job_id: i64,
    kind: &str,
    queue: &str,
    window: Duration,
    retry_lock_contention: bool,
) -> Result<T, FinalizeError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: Into<FinalizeAttemptError>,
{
    let started = tokio::time::Instant::now();
    let deadline = retry_lock_contention.then(|| started + window);
    let mut deadlock_retries = 0;
    let mut lock_retries = 0;
    let mut lock_held_retries = 0;
    let mut lock_held_warned = false;
    loop {
        if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            return Err(FinalizeError::Deadline);
        }
        let outcome = finalize().await;
        let error = match outcome {
            Ok(outcome) => return Ok(outcome),
            Err(error) => match error.into() {
                FinalizeAttemptError::Operation(error) => error,
                FinalizeAttemptError::LockHeld => {
                    lock_held_retries += 1;
                    let delay = completion_contention_backoff(lock_held_retries);
                    let waited = started.elapsed();
                    if waited >= COMPLETION_LOCK_HELD_WARN_AFTER && !lock_held_warned {
                        lock_held_warned = true;
                        warn!(
                            job_id,
                            waited_ms = waited.as_millis() as u64,
                            "Another transaction holds this attempt's completion lock; still retrying"
                        );
                    } else {
                        debug!(
                            job_id,
                            delay_ms = delay.as_millis() as u64,
                            "Another transaction holds this attempt's completion lock; retrying"
                        );
                    }
                    tokio::time::timeout_at(started + window, tokio::time::sleep(delay))
                        .await
                        .map_err(|_| FinalizeError::Deadline)?;
                    if tokio::time::Instant::now() >= started + window {
                        return Err(FinalizeError::Deadline);
                    }
                    continue;
                }
            },
        };
        let delay = if error.is_deadlock() && deadlock_retries < COMPLETION_DEADLOCK_RETRIES {
            deadlock_retries += 1;
            metrics.record_completion_deadlock_retry(kind, queue);
            let delay = completion_contention_backoff(deadlock_retries);
            warn!(
                job_id,
                retry = deadlock_retries,
                max_retries = COMPLETION_DEADLOCK_RETRIES,
                delay_ms = delay.as_millis() as u64,
                "Completion transaction aborted by the deadlock detector; retrying"
            );
            delay
        } else if retry_lock_contention && is_completion_lock_contention(&error) {
            lock_retries += 1;
            let delay = completion_contention_backoff(lock_retries);
            if lock_retries == 1 {
                warn!(job_id, error = %error, delay_ms = delay.as_millis() as u64, "Durable finalization failed; retrying");
            } else {
                debug!(job_id, error = %error, delay_ms = delay.as_millis() as u64, "Durable finalization failed; retrying");
            }
            delay
        } else {
            return Err(error.into());
        };
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, tokio::time::sleep(delay))
                .await
                .map_err(|_| FinalizeError::Deadline)?,
            None => tokio::time::sleep(delay).await,
        }
    }
}

/// Re-read a job whose transition has committed, for its lifecycle event.
/// The transition stands whatever the read returns, so a failed read is logged
/// and the caller builds the event from the worker's copy instead.
async fn reload_committed_job(
    runtime: &QueueStorageRuntime,
    pool: &PgPool,
    job_id: i64,
) -> Option<JobRow> {
    match runtime.store.load_job(pool, job_id).await {
        Ok(job) => job,
        Err(err) => {
            warn!(
                job_id,
                error = %err,
                "Could not re-read a committed job for its lifecycle event; using the worker's copy"
            );
            None
        }
    }
}

fn is_completion_lock_contention(error: &AwaError) -> bool {
    matches!(error, AwaError::Database(sqlx::Error::Database(database))
        if database.code().as_deref() == Some("55P03"))
}

const COMPLETION_LOCK_WINDOW: Duration = Duration::from_secs(120);

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
) -> Result<CompletionOutcome, FinalizeAttemptError> {
    match storage {
        RuntimeStorage::Canonical => complete_job_canonical(
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
        .map_err(Into::into),
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
        warn!(
            job_id = job.id,
            "Job already rescued/cancelled, failure ignored"
        );
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
    error_msg: String,
    enqueue_specs: &EnqueueSpecsByOutcome,
    progress_snapshot: Option<serde_json::Value>,
    dlq_enabled: bool,
    metrics: &crate::metrics::AwaMetrics,
    needs_event: bool,
) -> Result<CompletionOutcome, AwaError> {
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
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, retry ignored"
                );
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
            info!(
                job_id = job.id,
                kind = %job.kind,
                snooze_secs = seconds,
                "Job snoozed (attempt not incremented)"
            );
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

            // `awa.jobs_hot` directly, not the `awa.jobs` view: in canonical-drain
            // mode the view is backed by the active queue-storage schema, so a view
            // UPDATE silently misses the canonical row and the job wedges in
            // `running` (same rationale as `complete_canonical_with_followups`).
            // This applies to every canonical fast-path write and re-read here.
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
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, cancel ignored"
                );
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
                        warn!(
                            job_id = job.id,
                            "Job already rescued/cancelled, wait-for-callback ignored"
                        );
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
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, terminal failure ignored"
                );
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
                    warn!(
                        job_id = job.id,
                        "Job already rescued/cancelled, retry ignored"
                    );
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

/// The job as a read returns it after this attempt completes.
fn completed_event_row(job: &JobRow) -> JobRow {
    let mut row = job.clone();
    row.state = JobState::Completed;
    row.finalized_at = Some(chrono::Utc::now());
    row.progress = None;
    row.heartbeat_at = None;
    row.deadline_at = None;
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
) -> Result<CompletionOutcome, FinalizeAttemptError> {
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
                // Another transaction holds this attempt's completion lock.
                // Finalizing it directly would wait on that lock while
                // holding a pool connection; `finalize_with_retry` retries it
                // with backoff instead.
                Err(CompletionError::LockHeld { .. }) => {
                    return Err(FinalizeAttemptError::LockHeld)
                }
                Err(CompletionError::Storage(err)) if is_completion_lock_contention(&err) => {
                    return Err(err.into())
                }
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
                // The event is built from the worker's copy rather than
                // re-reading the job.
                Ok(CompletionOutcome::Applied {
                    event: Some(UntypedJobEvent::Completed {
                        job: completed_event_row(job),
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
                    error_msg,
                    enqueue_specs,
                    progress_snapshot.clone(),
                    dlq_enabled,
                    metrics,
                    needs_event,
                )
                .await
                .map_err(Into::into);
            }

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
                let parked_job = reload_committed_job(runtime, pool, job.id)
                    .await
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
                exhaust_queue_storage(
                    runtime,
                    pool,
                    job,
                    error_msg,
                    enqueue_specs,
                    progress_snapshot.clone(),
                    dlq_enabled,
                    metrics,
                    needs_event,
                )
                .await
                .map_err(Into::into)
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
    let updated_job = completed_event_row(job);

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
    use tokio_util::sync::CancellationToken;

    fn test_metrics() -> crate::metrics::AwaMetrics {
        crate::metrics::AwaMetrics::new(&opentelemetry::global::meter("finalize-test"))
    }

    #[derive(Debug)]
    struct FinalizeDatabaseError(&'static str);

    impl std::fmt::Display for FinalizeDatabaseError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "sqlstate {}", self.0)
        }
    }

    impl std::error::Error for FinalizeDatabaseError {}

    impl sqlx::error::DatabaseError for FinalizeDatabaseError {
        fn message(&self) -> &str {
            "finalize transaction rejected"
        }
        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some(std::borrow::Cow::Borrowed(self.0))
        }
        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn finalize_sqlstate(code: &'static str) -> AwaError {
        AwaError::Database(sqlx::Error::Database(Box::new(FinalizeDatabaseError(code))))
    }

    #[tokio::test(start_paused = true)]
    async fn finalize_lock_contention_preserves_the_sealed_outcome() {
        let calls = AtomicU32::new(0);
        let applied = AtomicU32::new(0);
        let sealed = serde_json::json!({
            "outcome": "retry_after", "delay_secs": 60,
            "progress": {"checkpoint": 7}, "followups": [{"id": 42}],
        });
        let outcome = finalize_with_retry(
            || {
                std::future::ready(if calls.fetch_add(1, Ordering::SeqCst) < 4 {
                    Err(finalize_sqlstate("55P03"))
                } else {
                    applied.fetch_add(1, Ordering::SeqCst);
                    Ok(sealed.clone())
                })
            },
            &test_metrics(),
            1,
            "test",
            "test",
            Duration::from_secs(2),
            true,
        )
        .await
        .expect("contention must retry the saved outcome");
        assert_eq!(outcome, sealed);
        assert_eq!(calls.load(Ordering::SeqCst), 5);
        assert_eq!(applied.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn finalize_lock_contention_keeps_the_absolute_deadline() {
        let calls = AtomicU32::new(0);
        let started = tokio::time::Instant::now();
        let result = finalize_with_retry(
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err::<(), _>(finalize_sqlstate("55P03")))
            },
            &test_metrics(),
            1,
            "test",
            "test",
            Duration::from_millis(100),
            true,
        )
        .await;
        assert!(matches!(result, Err(FinalizeError::Deadline)), "{result:?}");
        assert_eq!(started.elapsed(), Duration::from_millis(100));
        assert!((3..=5).contains(&calls.load(Ordering::SeqCst)));
    }

    #[tokio::test(start_paused = true)]
    async fn accepted_finalization_observes_its_acknowledgement_after_the_retry_window() {
        let calls = Arc::new(AtomicU32::new(0));
        let events = Arc::new(AtomicU32::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let (acknowledge, response) = tokio::sync::oneshot::channel::<UntypedJobEvent>();
        let observed = events.clone();
        let handler: BoxedUntypedEventHandler = Arc::new(move |event| {
            assert!(matches!(event, UntypedJobEvent::Completed { job, .. }
                if job.id == 1 && job.run_lease == 1));
            observed.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {})
        });
        let handlers = Arc::new(HashMap::from([("finalize_parent".into(), vec![handler])]));
        let attempts = calls.clone();
        let notification = started.clone();
        let task = tokio::spawn(async move {
            let mut response = Some(response);
            let outcome = finalize_with_retry(
                || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    let response = response.take().expect("accepted work is never restarted");
                    let notification = notification.clone();
                    async move {
                        notification.notify_one();
                        let event = response.await.map_err(|_| {
                            AwaError::Validation("completion acknowledgement dropped".into())
                        })?;
                        Ok::<_, AwaError>(CompletionOutcome::Applied {
                            event: Some(event),
                            terminal: false,
                        })
                    }
                },
                &test_metrics(),
                1,
                "finalize_parent",
                "test",
                Duration::from_millis(100),
                true,
            )
            .await?;
            let CompletionOutcome::Applied {
                event: Some(event), ..
            } = outcome
            else {
                panic!("accepted completion must retain its lifecycle event")
            };
            dispatch_lifecycle_event(&handlers, "finalize_parent", event).await;
            Ok::<(), FinalizeError>(())
        });
        started.notified().await;
        tokio::time::advance(Duration::from_millis(110)).await;
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "the retry window must not cancel accepted work"
        );
        let now = chrono::Utc::now();
        let job = serde_json::from_value(serde_json::json!({
            "id": 1, "kind": "finalize_parent", "queue": "test", "args": {},
            "state": "completed", "priority": 2, "attempt": 1, "run_lease": 1,
            "max_attempts": 5, "run_at": now, "attempted_at": now,
            "created_at": now, "finalized_at": now, "metadata": {}, "tags": [],
        }))
        .unwrap();
        assert!(acknowledge
            .send(UntypedJobEvent::Completed {
                job,
                duration: Duration::ZERO
            })
            .is_ok());
        task.await.unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(events.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn finalize_lock_contention_does_not_retry_unrelated_statement_errors() {
        for code in ["57014", "23505", "55P04"] {
            let calls = AtomicU32::new(0);
            let result = finalize_with_retry(
                || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Err::<(), _>(finalize_sqlstate(code)))
                },
                &test_metrics(),
                1,
                "test",
                "test",
                Duration::from_secs(2),
                true,
            )
            .await;
            assert!(
                matches!(result, Err(FinalizeError::Operation(_))),
                "{code}: {result:?}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1, "{code}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_held_completion_lock_is_retried_until_the_attempt_finalizes() {
        let calls = AtomicU32::new(0);
        let result = finalize_with_retry(
            || {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(if call < 3 {
                    Err(FinalizeAttemptError::LockHeld)
                } else {
                    Ok(call)
                })
            },
            &test_metrics(),
            1,
            "test",
            "test",
            Duration::from_secs(2),
            true,
        )
        .await;
        assert!(matches!(result, Ok(3)));
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn a_completion_lock_held_past_the_window_ends_finalization() {
        let calls = AtomicU32::new(0);
        let started = tokio::time::Instant::now();
        let result = finalize_with_retry(
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err::<(), _>(FinalizeAttemptError::LockHeld))
            },
            &test_metrics(),
            1,
            "test",
            "test",
            Duration::from_secs(2),
            true,
        )
        .await;
        assert!(matches!(result, Err(FinalizeError::Deadline)));
        assert_eq!(started.elapsed(), Duration::from_secs(2));
        assert!(calls.load(Ordering::SeqCst) > 5);
    }

    #[tokio::test(start_paused = true)]
    async fn canonical_finalization_does_not_retry_lock_contention() {
        let calls = AtomicU32::new(0);
        let result = finalize_with_retry(
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err::<(), _>(finalize_sqlstate("55P03")))
            },
            &test_metrics(),
            1,
            "test",
            "test",
            Duration::from_secs(2),
            false,
        )
        .await;
        assert!(matches!(result, Err(FinalizeError::Operation(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn canonical_finalization_preserves_three_deadlock_retries_without_a_deadline() {
        let calls = AtomicU32::new(0);
        let started = tokio::time::Instant::now();
        let result = finalize_with_retry(
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err::<(), _>(finalize_sqlstate("40P01")))
            },
            &test_metrics(),
            1,
            "test",
            "test",
            Duration::ZERO,
            false,
        )
        .await;
        assert!(matches!(result, Err(FinalizeError::Operation(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(started.elapsed(), Duration::from_millis(70));
    }

    struct FinalizeWorker;

    #[async_trait::async_trait]
    impl Worker for FinalizeWorker {
        fn kind(&self) -> &'static str {
            "finalize_parent"
        }

        async fn perform(&self, _context: &JobContext) -> Result<JobResult, JobError> {
            Ok(JobResult::Completed)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn queue_finalization_retains_heartbeat_without_holding_worker_capacity() {
        let metrics = test_metrics();
        let now = chrono::Utc::now();
        let job: JobRow = serde_json::from_value(serde_json::json!({
            "id": 1, "kind": "finalize_parent", "queue": "test", "args": {},
            "state": "running", "priority": 2, "attempt": 1, "run_lease": 1,
            "max_attempts": 5, "run_at": now, "attempted_at": now,
            "created_at": now, "metadata": {}, "tags": [],
        }))
        .unwrap();
        let key = (job.id, job.run_lease);
        let runtime = QueueStorageRuntime::new(
            awa_model::QueueStorageConfig::default(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
        let storage = RuntimeStorage::QueueStorage(runtime);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://postgres:test@localhost/awa_test")
            .unwrap();
        let finalizers = TaskTracker::new();
        let in_flight = Arc::new(crate::runtime::InFlightRegistry::default());
        let count = Arc::new(AtomicU32::new(0));
        let (unstarted, handle) = crate::completion::CompletionBatcher::new(
            pool.clone(),
            CancellationToken::new(),
            metrics.clone(),
            storage.clone(),
            1,
        );
        let executor = JobExecutor::new(
            pool.clone(),
            Arc::new(HashMap::from([(
                "finalize_parent".into(),
                Box::new(FinalizeWorker) as BoxedWorker,
            )])),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            in_flight.clone(),
            Arc::new(HashMap::from([("test".into(), count.clone())])),
            Arc::new(HashMap::new()),
            metrics,
            handle,
            storage,
            DlqPolicy::default(),
            finalizers.clone(),
            CancellationToken::new(),
            1,
        );
        executor
            .execute_task(
                DispatchedJob {
                    job,
                    queue_storage_claim: Some(ClaimedEntry {
                        queue: "test".into(),
                        priority: 2,
                        lane_seq: 1,
                        ready_slot: 0,
                        ready_generation: 0,
                        lease_slot: 0,
                        lease_generation: 0,
                        claim_slot: 0,
                        receipt_id: Some(1),
                        claim_batch_id: Some(1),
                        claim_batch_index: Some(1),
                        lease_claim_receipt: true,
                        enqueue_shard: 0,
                    }),
                    queue_storage_unique_states: None,
                    abandon_after: None,
                },
                Arc::new(AtomicBool::new(false)),
            )
            .await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "handler capacity must be released"
        );
        assert_eq!(finalizers.len(), 1);
        assert!(
            in_flight.get_cancel(key).is_some(),
            "heartbeat ownership must survive handler return"
        );
        tokio::task::yield_now().await;
        assert!(
            in_flight.get_cancel(key).is_some(),
            "pending acknowledgement must retain the lease"
        );
        tokio::time::advance(COMPLETION_LOCK_WINDOW + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(finalizers.len(), 1);
        assert!(
            in_flight.get_cancel(key).is_some(),
            "accepted completion must retain heartbeat ownership beyond the retry window"
        );
        pool.close().await;
        drop(unstarted);
        finalizers.close();
        tokio::time::timeout(Duration::from_secs(1), finalizers.wait())
            .await
            .unwrap();
        assert!(
            in_flight.get_cancel(key).is_none(),
            "settlement must unregister the exact lease"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn saturated_finalizer_capacity_retains_worker_until_shutdown() {
        for storage in [
            RuntimeStorage::Canonical,
            RuntimeStorage::QueueStorage(
                QueueStorageRuntime::new(
                    awa_model::QueueStorageConfig::default(),
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                )
                .unwrap(),
            ),
        ] {
            let now = chrono::Utc::now();
            let job: JobRow = serde_json::from_value(serde_json::json!({
                "id": 1, "kind": "finalize_parent", "queue": "test", "args": {},
                "state": "running", "priority": 2, "attempt": 1, "run_lease": 1,
                "max_attempts": 5, "run_at": now, "attempted_at": now,
                "created_at": now, "metadata": {}, "tags": [],
            }))
            .unwrap();
            let key = (job.id, job.run_lease);
            let pool = sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://postgres:test@localhost/awa_test")
                .unwrap();
            let metrics = test_metrics();
            let finalizers = TaskTracker::new();
            let finalize_cancel = CancellationToken::new();
            let in_flight = Arc::new(crate::runtime::InFlightRegistry::default());
            let count = Arc::new(AtomicU32::new(0));
            let (_unstarted, handle) = crate::completion::CompletionBatcher::new(
                pool.clone(),
                CancellationToken::new(),
                metrics.clone(),
                storage.clone(),
                1,
            );
            let executor = JobExecutor::new(
                pool.clone(),
                Arc::new(HashMap::from([(
                    "finalize_parent".into(),
                    Box::new(FinalizeWorker) as BoxedWorker,
                )])),
                Arc::new(HashMap::new()),
                Arc::new(HashMap::new()),
                in_flight.clone(),
                Arc::new(HashMap::from([("test".into(), count.clone())])),
                Arc::new(HashMap::new()),
                metrics,
                handle,
                storage,
                DlqPolicy::default(),
                finalizers.clone(),
                finalize_cancel.clone(),
                1,
            );
            // Occupy the entire budget without starting database work.
            let capacity = executor.finalize_capacity.clone();
            let held = capacity
                .clone()
                .acquire_many_owned(capacity.available_permits() as u32)
                .await
                .unwrap();
            let task = tokio::spawn(executor.execute_task(
                DispatchedJob {
                    job,
                    queue_storage_claim: None,
                    queue_storage_unique_states: None,
                    abandon_after: None,
                },
                Arc::new(AtomicBool::new(false)),
            ));
            tokio::task::yield_now().await;
            assert!(!task.is_finished());
            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert!(in_flight.get_cancel(key).is_some());
            assert!(finalizers.is_empty());

            // Cancelling background services at the drain deadline must release
            // the capacity waiter without spawning another finalizer.
            finalize_cancel.cancel();
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(count.load(Ordering::SeqCst), 0);
            assert!(in_flight.get_cancel(key).is_none());
            assert!(finalizers.is_empty());
            drop(held);
            pool.close().await;
        }
    }

    #[tokio::test]
    async fn cancelled_finalization_unregisters_only_its_exact_lease() {
        let in_flight = Arc::new(crate::runtime::InFlightRegistry::default());
        for key in [(1, 1), (1, 2)] {
            in_flight.insert(
                key,
                InFlightState {
                    cancel: Arc::new(AtomicBool::new(false)),
                    progress: Arc::new(std::sync::Mutex::new(ProgressState::new(None))),
                    claim: None,
                    finalizing: false,
                },
            );
        }
        let guard = FinalizationHeartbeatGuard {
            in_flight: in_flight.clone(),
            key: (1, 1),
        };
        let started = Arc::new(tokio::sync::Notify::new());
        let notification = started.clone();
        let task = tokio::spawn(async move {
            let _guard = guard;
            notification.notify_one();
            std::future::pending::<()>().await;
        });
        started.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(in_flight.get_cancel((1, 1)).is_none());
        assert!(in_flight.get_cancel((1, 2)).is_some());
    }
    struct RetainedFinalizeFollowup;

    impl crate::enqueue_specs::EnqueueFollowUp for RetainedFinalizeFollowup {
        fn run<'a>(
            &'a self,
            connection: &'a mut sqlx::PgConnection,
            job: &'a JobRow,
            _context: Option<&'a crate::enqueue_specs::OutcomeContext>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), AwaError>> + Send + 'a>>
        {
            Box::pin(async move {
                sqlx::query("INSERT INTO finalize_followups(parent) VALUES ($1)")
                    .bind(job.id)
                    .execute(connection)
                    .await?;
                Ok(())
            })
        }
    }

    #[sqlx::test(migrations = false)]
    async fn retained_head_contention_replays_the_worker_outcome_once(pool: PgPool) {
        awa_model::migrations::run(&pool).await.unwrap();
        sqlx::query("CREATE TABLE finalize_followups(parent bigint PRIMARY KEY)")
            .execute(&pool)
            .await
            .unwrap();
        let runtime = QueueStorageRuntime::new(
            awa_model::QueueStorageConfig {
                queue_stripe_count: 1,
                ..Default::default()
            },
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
        let queue = "retained_finalize";
        runtime.store.prepare_queue(&pool, queue, 1).await.unwrap();
        runtime
            .store
            .enqueue_params_batch(
                &pool,
                &[awa_model::job::InsertParams {
                    kind: "retained_finalize_parent".into(),
                    args: serde_json::json!({"body":"retained"}),
                    opts: awa_model::InsertOpts {
                        queue: queue.into(),
                        priority: 4,
                        max_attempts: 3,
                        metadata: serde_json::json!({"preserved":true}),
                        ..Default::default()
                    },
                }],
            )
            .await
            .unwrap();
        let claimed = runtime
            .store
            .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let pair = (claimed.job.id, claimed.job.run_lease);
        let schema = runtime.store.schema();
        let mut historical = pool.begin().await.unwrap();
        sqlx::query(&format!(
            "INSERT INTO {schema}.ready_tombstones
             (ready_slot,ready_generation,queue,priority,enqueue_shard,lane_seq,job_id)
             SELECT ready_slot,ready_generation,queue,priority,enqueue_shard,lane_seq,job_id
             FROM {schema}.ready_entries WHERE job_id=$1"
        ))
        .bind(pair.0)
        .execute(historical.as_mut())
        .await
        .unwrap();
        sqlx::query(&format!(
            "INSERT INTO {schema}.ready_entries
             (ready_slot,ready_generation,job_id,kind,queue,args,priority,attempt,run_lease,max_attempts,
              lane_seq,enqueue_shard,run_at,attempted_at,created_at,unique_key,unique_states,payload)
             SELECT ready_slot,ready_generation,job_id,kind,queue,args,3,attempt,run_lease,max_attempts,
              {schema}.reserve_enqueue_seq(queue,3::smallint,enqueue_shard,1::bigint),enqueue_shard,
              run_at,attempted_at,created_at,unique_key,unique_states,payload
             FROM {schema}.ready_entries WHERE job_id=$1"
        )).bind(pair.0).execute(historical.as_mut()).await.unwrap();
        sqlx::query(&format!(
            "INSERT INTO {schema}.ready_segments
             (ready_slot,ready_generation,queue,priority,enqueue_shard,first_lane_seq,next_lane_seq,first_run_at,created_at)
             SELECT ready_slot,ready_generation,queue,priority,enqueue_shard,lane_seq,lane_seq+1,run_at,created_at
             FROM {schema}.ready_entries WHERE job_id=$1 AND priority=3 ON CONFLICT DO NOTHING"
        )).bind(pair.0).execute(historical.as_mut()).await.unwrap();
        historical.commit().await.unwrap();
        let progress = serde_json::json!({"checkpoint":7});
        runtime
            .store
            .flush_progress(&pool, pair.0, pair.1, progress.clone())
            .await
            .unwrap();
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query(&format!(
            "SELECT 1 FROM {schema}.queue_claim_heads
             WHERE queue=$1 AND priority=3 AND enqueue_shard=0 FOR UPDATE"
        ))
        .bind(queue)
        .fetch_one(blocker.as_mut())
        .await
        .unwrap();
        let metrics = test_metrics();
        let storage = RuntimeStorage::QueueStorage(runtime.clone());
        let (_unstarted, handle) = crate::completion::CompletionBatcher::new(
            pool.clone(),
            CancellationToken::new(),
            metrics.clone(),
            storage.clone(),
            1,
        );
        let specs: Arc<EnqueueSpecsByOutcome> = Arc::new(HashMap::from([(
            crate::enqueue_specs::Outcome::Retried,
            HashMap::from([(
                "retained_finalize_parent".into(),
                vec![Arc::new(RetainedFinalizeFollowup) as crate::enqueue_specs::BoxedEnqueueSpec],
            )]),
        )]));
        let intended = Ok(JobResult::RetryAfter(Duration::from_secs(60)));
        let calls = AtomicU32::new(0);
        let busy = tokio::sync::Notify::new();
        let finalize = finalize_with_retry(
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                let (pool, job, claim, intended, progress, handle, specs, storage, metrics, busy) = (
                    &pool,
                    &claimed.job,
                    &claimed.claim,
                    &intended,
                    &progress,
                    &handle,
                    &specs,
                    &storage,
                    &metrics,
                    &busy,
                );
                async move {
                    let result = complete_job(
                        pool,
                        job,
                        Some(claim),
                        None,
                        intended,
                        handle,
                        Some(progress.clone()),
                        Duration::ZERO,
                        true,
                        specs,
                        storage,
                        false,
                        metrics,
                    )
                    .await;
                    if let Err(error) = &result {
                        assert!(
                            matches!(error, FinalizeAttemptError::Operation(error)
                                if is_completion_lock_contention(error)),
                            "expected actual held-head55P03: {error}"
                        );
                        busy.notify_one();
                    }
                    result
                }
            },
            &metrics,
            pair.0,
            &claimed.job.kind,
            queue,
            Duration::from_secs(5),
            true,
        );
        let release = async {
            tokio::time::timeout(Duration::from_secs(3), busy.notified())
                .await
                .expect("worker transition must encounter the held historical head");
            let active = runtime
                .store
                .load_job(&pool, pair.0)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(active.state, JobState::Running);
            assert_eq!(active.run_lease, pair.1);
            assert_eq!(active.progress, Some(progress.clone()));
            let effects: i64 = sqlx::query_scalar("SELECT count(*) FROM finalize_followups")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(effects, 0, "failed transaction cannot publish its followup");
            blocker.rollback().await.unwrap();
        };
        let (ack, ()) = tokio::join!(finalize, release);
        let ack = ack.expect("saved worker outcome must survive transient head contention");
        assert!(calls.load(Ordering::SeqCst) >= 2);
        let CompletionOutcome::Applied {
            event: Some(UntypedJobEvent::Retried { job, .. }),
            terminal: false,
        } = ack
        else {
            panic!("worker must acknowledge its intended retry outcome")
        };
        assert_eq!(job.id, pair.0);
        assert_eq!(job.run_lease, pair.1);
        assert_eq!(job.state, JobState::Retryable);
        assert_eq!(job.args, claimed.job.args);
        assert_eq!(job.max_attempts, claimed.job.max_attempts);
        assert_eq!(job.progress, Some(progress.clone()));
        assert_eq!(job.metadata["preserved"], true);
        let replay = complete_job(
            &pool,
            &claimed.job,
            Some(&claimed.claim),
            None,
            &intended,
            &handle,
            Some(progress),
            Duration::ZERO,
            true,
            &specs,
            &storage,
            false,
            &metrics,
        )
        .await
        .unwrap();
        assert!(matches!(replay, CompletionOutcome::IgnoredStale));
        let effects: i64 =
            sqlx::query_scalar("SELECT count(*) FROM finalize_followups WHERE parent=$1")
                .bind(pair.0)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            effects, 1,
            "the acknowledged outcome publishes exactly one followup"
        );
        let deferred:i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM {schema}.deferred_jobs WHERE job_id=$1 AND run_lease=$2 AND state='retryable'"
        )).bind(pair.0).bind(pair.1).fetch_one(&pool).await.unwrap();
        assert_eq!(deferred, 1);
    }
}
