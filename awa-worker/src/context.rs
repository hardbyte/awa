use crate::enqueue_specs::EnqueueRequest;
use crate::follow_ups::{FollowUps, StageError};
use crate::output::{OutputError, OutputSlot, TypedOutputSlot};
pub use crate::runtime::ProgressState;
use crate::storage::RuntimeStorage;
use awa_model::{AwaError, CallbackConfig, JobArgs, JobOutput, JobRow, QueueStorage};
use sqlx::PgPool;
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Proof that this job registered an external callback in the database.
///
/// The public `id` can be sent to the external system. `#[non_exhaustive]`
/// keeps external callers from constructing this type directly.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CallbackGuard {
    pub id: uuid::Uuid,
}

impl CallbackGuard {
    fn new(id: uuid::Uuid) -> Self {
        Self { id }
    }

    /// Return the callback UUID persisted for this job.
    pub fn id(&self) -> uuid::Uuid {
        self.id
    }

    #[cfg(feature = "__python-bridge")]
    #[doc(hidden)]
    pub fn from_bridge_token(id: uuid::Uuid) -> Self {
        Self::new(id)
    }
}

#[doc(hidden)]
pub type CallbackToken = CallbackGuard;

/// Context passed to worker handlers during job execution.
///
/// Provides access to the job metadata, shared state (e.g., service dependencies),
/// callback registration, and structured progress reporting.
pub struct JobContext {
    /// The raw job row from the database.
    pub job: JobRow,
    /// Cancellation flag — set to true when shutdown or deadline is signalled.
    cancelled: Arc<AtomicBool>,
    /// Shared state map for dependency injection.
    state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
    /// Database pool for callback registration and progress flush.
    pool: PgPool,
    /// Active runtime storage backend.
    storage: RuntimeStorage,
    /// Shared progress buffer — written by handler, read by heartbeat service.
    progress: Arc<std::sync::Mutex<ProgressState>>,
    /// Follow-ups staged for this attempt's completion (ADR-046).
    follow_ups: FollowUps,
    /// The output this attempt records (ADR-048).
    output: OutputSlot,
}

impl JobContext {
    pub(crate) fn new(
        job: JobRow,
        cancelled: Arc<AtomicBool>,
        state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
        pool: PgPool,
        storage: RuntimeStorage,
        progress: Arc<std::sync::Mutex<ProgressState>>,
    ) -> Self {
        Self {
            job,
            cancelled,
            state,
            pool,
            storage,
            progress,
            follow_ups: FollowUps::new(),
            output: OutputSlot::new(),
        }
    }

    #[doc(hidden)]
    pub fn new_for_testing(
        job: JobRow,
        cancelled: Arc<AtomicBool>,
        state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
        pool: PgPool,
        progress: Arc<std::sync::Mutex<ProgressState>>,
    ) -> Self {
        Self::new(
            job,
            cancelled,
            state,
            pool,
            RuntimeStorage::Canonical,
            progress,
        )
    }

    /// Check if this job's execution has been cancelled (shutdown or deadline).
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// W3C traceparent captured at the **enqueue site**, if any. The
    /// execution span already joins that trace automatically; this getter
    /// is for inspecting the stored context (or building span links).
    ///
    /// To propagate the trace onward from handler code (outgoing HTTP
    /// headers, child processes), use
    /// [`awa_model::trace::current_traceparent`] instead: handlers run
    /// inside the `job.execute` span, so the ambient context makes the
    /// downstream span a child of the execution — forwarding the enqueue
    /// context would make it a sibling.
    pub fn traceparent(&self) -> Option<&str> {
        awa_model::trace::traceparent_from_metadata(&self.job.metadata)
    }

    /// A cloneable handle for staging follow-up jobs on this attempt.
    ///
    /// Use it where the context cannot be borrowed, such as the `'static`
    /// future returned by a `ClientBuilder::register` closure:
    ///
    /// ```ignore
    /// .register::<StageA, _, _>(|args, ctx| {
    ///     let follow_ups = ctx.follow_ups();
    ///     async move {
    ///         let item_id = resolve_item(&args).await?;
    ///         follow_ups.enqueue_on_completion(
    ///             EnqueueRequest::new(StageB { item_id }).unique(UniqueOpts::default()),
    ///         )?;
    ///         Ok(JobResult::Completed)
    ///     }
    /// })
    /// ```
    pub fn follow_ups(&self) -> FollowUps {
        self.follow_ups.clone()
    }

    /// Stage a follow-up job that is enqueued in the same transaction as
    /// this attempt's completion, and discarded if the attempt ends any other
    /// way (ADR-046). Shorthand for `self.follow_ups().enqueue_on_completion(..)`.
    ///
    /// The args are serialized and the unique key computed now. A follow-up
    /// with unique options whose key already exists is skipped when the
    /// completion commits. A failed call poisons the attempt: returning
    /// `JobResult::Completed` afterwards fails it terminally.
    pub fn enqueue_on_completion<F: JobArgs>(
        &self,
        request: impl Into<EnqueueRequest<F>>,
    ) -> Result<(), StageError> {
        self.follow_ups.enqueue_on_completion(request)
    }

    /// Record this attempt's output (ADR-048), replacing any earlier one.
    ///
    /// The output is a JSON value of at most
    /// [`MAX_OUTPUT_BYTES`](crate::MAX_OUTPUT_BYTES), kept for people and
    /// tools to inspect: as the job's output if the attempt completes, or in
    /// its error entry if it fails. Nothing in Awa reads it, and another job
    /// must not either; pass values a follow-up needs in its args.
    ///
    /// The value is serialized and checked now. Recording JSON null clears
    /// the output, and so does a failed call. A failure does not fail the
    /// attempt: [`OutputError`] deliberately does not convert into a job
    /// error, so the handler decides whether a missing output matters.
    pub fn set_output<T: serde::Serialize + ?Sized>(&self, output: &T) -> Result<(), OutputError> {
        self.output.set(output)
    }

    /// Remove the recorded output, so the attempt publishes none.
    pub fn clear_output(&self) {
        self.output.clear();
    }

    /// The output recorded so far by this attempt.
    pub fn output(&self) -> Option<serde_json::Value> {
        self.output.get()
    }

    /// A cloneable handle to this attempt's output, for futures that cannot
    /// borrow the context, such as the `'static` future a
    /// `ClientBuilder::register` closure returns.
    pub fn output_slot(&self) -> OutputSlot {
        self.output.clone()
    }

    /// This attempt's output slot, typed by the output `T` declares with
    /// `#[awa(output = ...)]`. On a job that is not of kind `T`, every
    /// `set` on the slot fails with [`OutputError::Invalid`].
    pub fn typed_output<T: JobOutput>(&self) -> TypedOutputSlot<T::Output> {
        let mismatch = (self.job.kind != T::kind()).then(|| {
            format!(
                "job {} is of kind {}, not {}",
                self.job.id,
                self.job.kind,
                T::kind()
            )
        });
        TypedOutputSlot::new(self.output.clone(), mismatch)
    }

    /// Clone the shared cancellation flag for language bridges.
    pub fn cancellation_flag(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }

    /// Signal cancellation for this job.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Extract a shared state value by type.
    ///
    /// State values are registered via `Client::builder().state(value)`.
    /// Register the concrete type you want to extract:
    ///
    /// ```ignore
    /// // Register with the type you'll extract:
    /// let deps = Arc::new(MyDeps::new());
    /// Client::builder(pool)
    ///     .state(deps.clone())  // stores Arc<MyDeps>
    ///     .build()?;
    ///
    /// // In handler — extract the same type:
    /// let deps = ctx.extract::<Arc<MyDeps>>().unwrap();
    /// ```
    pub fn extract<T: Any + Send + Sync + Clone>(&self) -> Option<T> {
        self.state
            .get(&std::any::TypeId::of::<T>())
            .and_then(|v| v.downcast_ref::<T>())
            .cloned()
    }

    /// Get a reference to the database pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the active queue-storage backend for this job, if any.
    pub fn queue_storage_store(&self) -> Option<Arc<QueueStorage>> {
        self.storage.queue_storage_store()
    }

    /// Register a callback for this job, writing the callback_id to the database
    /// immediately.
    ///
    /// Call this BEFORE sending the callback_id to the external system to avoid
    /// the race condition where the external system fires before the DB knows
    /// about the callback.
    ///
    /// Returns a `CallbackGuard` whose `id` should be included in the URL or
    /// payload sent to the external system.
    pub async fn register_callback(&self, timeout: Duration) -> Result<CallbackGuard, AwaError> {
        let callback_id = match &self.storage {
            RuntimeStorage::Canonical => {
                awa_model::admin::register_callback(
                    &self.pool,
                    self.job.id,
                    self.job.run_lease,
                    timeout,
                )
                .await?
            }
            RuntimeStorage::QueueStorage(runtime) => {
                runtime
                    .store
                    .register_callback(&self.pool, self.job.id, self.job.run_lease, timeout)
                    .await?
            }
        };
        Ok(CallbackGuard::new(callback_id))
    }

    /// Register a callback with CEL expressions for automatic resolution.
    ///
    /// See [`CallbackConfig`] for expression semantics.
    pub async fn register_callback_with_config(
        &self,
        timeout: Duration,
        config: &CallbackConfig,
    ) -> Result<CallbackGuard, AwaError> {
        let callback_id = match &self.storage {
            RuntimeStorage::Canonical => {
                awa_model::admin::register_callback_with_config(
                    &self.pool,
                    self.job.id,
                    self.job.run_lease,
                    timeout,
                    config,
                )
                .await?
            }
            RuntimeStorage::QueueStorage(runtime) => {
                runtime
                    .store
                    .register_callback_with_config(
                        &self.pool,
                        self.job.id,
                        self.job.run_lease,
                        timeout,
                        config,
                    )
                    .await?
            }
        };
        Ok(CallbackGuard::new(callback_id))
    }

    /// Wait for an external callback to resolve, then resume with the payload.
    ///
    /// This enables sequential callbacks: the handler can register a callback,
    /// wait for it, process the result, register another callback, wait again,
    /// and so on — all within a single handler invocation.
    ///
    /// The handler's async task suspends while waiting. The job transitions to
    /// `waiting_external`. When `resume_external(callback_id, payload)` is
    /// called, the job transitions back to `running` and this method returns
    /// the payload.
    ///
    /// The handler holds its permit (worker slot) during the wait. For very
    /// long waits, consider whether the single-shot `WaitForCallback` pattern
    /// (which releases the permit) is more appropriate.
    ///
    /// ```ignore
    /// let token = ctx.register_callback(Duration::from_secs(3600)).await?;
    /// send_to_external_system(token.id());
    /// let payload = ctx.wait_for_callback(token).await?;
    /// // Handler resumes here with the external system's response
    /// ```
    pub async fn wait_for_callback(
        &self,
        guard: CallbackGuard,
    ) -> Result<serde_json::Value, AwaError> {
        use awa_model::admin::CallbackPollResult;

        let callback_id = guard.id();

        let entered = match &self.storage {
            RuntimeStorage::Canonical => {
                awa_model::admin::enter_callback_wait(
                    &self.pool,
                    self.job.id,
                    self.job.run_lease,
                    callback_id,
                )
                .await?
            }
            RuntimeStorage::QueueStorage(runtime) => {
                runtime
                    .store
                    .enter_callback_wait(&self.pool, self.job.id, self.job.run_lease, callback_id)
                    .await?
            }
        };

        let check_state = || async {
            match &self.storage {
                RuntimeStorage::Canonical => {
                    awa_model::admin::check_callback_state(&self.pool, self.job.id, callback_id)
                        .await
                }
                RuntimeStorage::QueueStorage(runtime) => {
                    runtime
                        .store
                        .check_callback_state(&self.pool, self.job.id, callback_id)
                        .await
                }
            }
        };

        if !entered {
            match check_state().await? {
                CallbackPollResult::Resolved(payload) => return Ok(payload),
                CallbackPollResult::Pending => { /* Already in waiting_external */ }
                CallbackPollResult::Stale {
                    token,
                    current,
                    state,
                } => {
                    return Err(AwaError::Validation(format!(
                        "wait_for_callback: token {token} is stale; current callback is {current} in state {state:?}"
                    )));
                }
                CallbackPollResult::UnexpectedState { token, state } => {
                    return Err(AwaError::Validation(format!(
                        "wait_for_callback: job is not waiting on callback {token}; state={state:?}"
                    )));
                }
                CallbackPollResult::NotFound => {
                    return Err(AwaError::Validation(
                        "job not found during callback wait".into(),
                    ));
                }
            }
        }

        loop {
            if self.is_cancelled() {
                return Err(AwaError::Validation(
                    "job cancelled while waiting for callback".into(),
                ));
            }

            match check_state().await? {
                CallbackPollResult::Resolved(payload) => return Ok(payload),
                CallbackPollResult::Pending => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                CallbackPollResult::Stale {
                    token,
                    current,
                    state,
                } => {
                    return Err(AwaError::Validation(format!(
                        "wait_for_callback: token {token} is stale; current callback is {current} in state {state:?}"
                    )));
                }
                CallbackPollResult::UnexpectedState { token, state } => {
                    return Err(AwaError::Validation(format!(
                        "job left wait_for_callback unexpectedly for token {token}: state={state:?}"
                    )));
                }
                CallbackPollResult::NotFound => {
                    return Err(AwaError::Validation(
                        "job not found during callback wait".into(),
                    ));
                }
            }
        }
    }

    /// Set structured progress (0-100 with message). Sync — writes to in-memory buffer.
    ///
    /// `percent` is clamped to 0-100. For progress without a message, pass `""`.
    pub fn set_progress(&self, percent: u8, message: &str) {
        let mut guard = self.progress.lock().expect("progress lock poisoned");
        guard.set_progress(percent, Some(message));
    }

    /// Shallow-merge keys into progress.metadata for checkpointing. Sync.
    ///
    /// `updates` must be a JSON object. Top-level keys overwrite; nested objects
    /// are replaced, not deep-merged.
    pub fn update_metadata(&self, updates: serde_json::Value) -> Result<(), AwaError> {
        let obj = updates
            .as_object()
            .ok_or_else(|| AwaError::Validation("update_metadata requires a JSON object".into()))?;

        let mut guard = self.progress.lock().expect("progress lock poisoned");
        if !guard.merge_metadata(obj) {
            return Err(AwaError::Validation(
                "progress.metadata is not a JSON object; cannot merge".into(),
            ));
        }
        Ok(())
    }

    /// Force immediate flush of pending progress to DB. For critical checkpoints.
    ///
    /// Does not return success until the progress has been durably written
    /// or the job is no longer in running state (rescued/cancelled).
    pub async fn flush_progress(&self) -> Result<(), AwaError> {
        let (snapshot, target_generation) = {
            let guard = self.progress.lock().expect("progress lock poisoned");
            match guard.pending_snapshot() {
                Some(pair) => pair,
                None => return Ok(()),
            }
        };

        match &self.storage {
            RuntimeStorage::Canonical => {
                let result = sqlx::query(
                    r#"
                    UPDATE awa.jobs_hot
                    SET progress = $2
                    WHERE id = $1 AND state = 'running' AND run_lease = $3
                    "#,
                )
                .bind(self.job.id)
                .bind(&snapshot)
                .bind(self.job.run_lease)
                .execute(&self.pool)
                .await?;

                if result.rows_affected() == 0 {
                    return Ok(());
                }
            }
            RuntimeStorage::QueueStorage(runtime) => {
                runtime
                    .store
                    .flush_progress(
                        &self.pool,
                        self.job.id,
                        self.job.run_lease,
                        snapshot.clone(),
                    )
                    .await?;
            }
        }

        let mut guard = self.progress.lock().expect("progress lock poisoned");
        guard.ack(target_generation);

        Ok(())
    }

    /// Get a clone of the shared progress state Arc (for Python bridge).
    pub fn progress_buffer(&self) -> Arc<std::sync::Mutex<ProgressState>> {
        self.progress.clone()
    }
}
