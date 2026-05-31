//! Transactional follow-up enqueue specs (ADR-029).
//!
//! A spec is a per-(outcome, kind) registration that fires when its
//! triggering state transition commits. Worker-driven outcomes dispatch
//! the follow-up `INSERT` in the same transaction as the state UPDATE
//! (atomic with the trigger). Callback-resolution and maintenance-rescue
//! outcomes dispatch in a separate transaction after the trigger
//! transaction commits (best-effort — a failed `INSERT` is logged and
//! the trigger stands). Either way, once the follow-up `INSERT` commits
//! the row is a regular Awa job: at-least-once, retried, DLQ-aware,
//! visible to admin tooling.
//!
//! Specs are type-erased here so the executor can dispatch them without
//! knowing the trigger or follow-up types statically. The user-facing
//! `ClientBuilder::on_*_enqueue` methods wrap their typed closures into
//! impls of [`EnqueueFollowUp`] and accumulate them in a two-level
//! `outcome -> kind -> specs` registry.

use awa_model::{insert_with, AwaError, InsertOpts, JobArgs, JobRow};
use serde::de::DeserializeOwned;
use sqlx::PgConnection;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

/// Description of a follow-up job to enqueue when an `on_*_enqueue` spec
/// fires. Carries the follow-up's `JobArgs` and the [`InsertOpts`] applied to
/// the `INSERT`. For the common "default opts" case, users can return the
/// `JobArgs` value directly from their closure — `EnqueueRequest::from(args)`
/// is invoked automatically.
#[derive(Debug, Clone)]
pub struct EnqueueRequest<F> {
    pub(crate) args: F,
    pub(crate) opts: InsertOpts,
}

impl<F: JobArgs> EnqueueRequest<F> {
    /// Build a request with default [`InsertOpts`].
    pub fn new(args: F) -> Self {
        Self {
            args,
            opts: InsertOpts::default(),
        }
    }

    /// Override the follow-up's queue.
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.opts.queue = queue.into();
        self
    }

    /// Override the follow-up's priority.
    pub fn priority(mut self, priority: i16) -> Self {
        self.opts.priority = priority;
        self
    }

    /// Override the follow-up's `max_attempts`.
    pub fn max_attempts(mut self, max_attempts: i16) -> Self {
        self.opts.max_attempts = max_attempts;
        self
    }

    /// Replace the follow-up's [`InsertOpts`] wholesale — useful for fields
    /// without dedicated builder methods (`metadata`, `tags`, `unique`,
    /// `run_at`, `deadline_duration`, `ordering_key`).
    pub fn with_opts(mut self, opts: InsertOpts) -> Self {
        self.opts = opts;
        self
    }
}

impl<F: JobArgs> From<F> for EnqueueRequest<F> {
    fn from(args: F) -> Self {
        Self::new(args)
    }
}

/// The outcome whose state-commit triggers a registered spec.
///
/// One spec is tied to exactly one outcome; the registry is keyed on this so
/// the executor can look up specs for the specific branch it just took
/// without filtering.
///
/// `Started` is intentionally excluded (see ADR-029): claim-time follow-up
/// enqueue would join the dispatcher's hot path and the durable-side-effect
/// use case for "job started" is uncommon. Observation belongs to hooks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Outcome {
    Completed,
    Retried,
    Exhausted,
    Cancelled,
    WaitingForCallback,
    /// Maintenance rescued the job (expired callback, stale heartbeat, or
    /// exceeded deadline). See [`crate::events::RescueReason`].
    Rescued,
}

/// Per-outcome runtime context handed to non-`Completed` follow-up closures
/// so they can specialise on outcome-specific fields (error / attempt /
/// reason / next_run_at). Variants mirror the corresponding
/// `UntypedJobEvent` variants in shape.
#[derive(Debug, Clone)]
pub enum OutcomeContext {
    Retried {
        error: String,
        attempt: i16,
        next_run_at: chrono::DateTime<chrono::Utc>,
    },
    Exhausted {
        error: String,
        attempt: i16,
    },
    Cancelled {
        reason: String,
    },
    WaitingForCallback,
    Rescued {
        reason: crate::events::RescueReason,
    },
}

/// Type-erased follow-up-enqueue spec for one (outcome, kind) pair.
///
/// `Completed` specs receive `outcome_context: None` (no extra context beyond
/// the JobRow). Non-Completed specs receive `Some(ctx)` with the matching
/// variant — the registry guarantees the variant matches because the spec is
/// registered under its outcome key.
pub(crate) trait EnqueueFollowUp: Send + Sync {
    fn run<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        job: &'a JobRow,
        outcome_context: Option<&'a OutcomeContext>,
    ) -> Pin<Box<dyn Future<Output = Result<(), AwaError>> + Send + 'a>>;
}

pub(crate) type BoxedEnqueueSpec = Arc<dyn EnqueueFollowUp + 'static>;

fn decode_trigger_args<T: DeserializeOwned>(job: &JobRow) -> Result<T, AwaError> {
    serde_json::from_value(job.args.clone()).map_err(|err| {
        AwaError::Validation(format!(
            "follow-up enqueue: failed to decode trigger args for kind {}: {err}",
            job.kind
        ))
    })
}

/// Spec for the `Completed` outcome. Captures a typed closure that maps the
/// trigger's deserialised args plus its post-completion `JobRow` to an
/// [`EnqueueRequest<F>`] describing the follow-up.
pub(crate) struct CompletedFollowUp<T, F, MakeFn> {
    pub(crate) make: MakeFn,
    pub(crate) _phantom: PhantomData<fn() -> (T, F)>,
}

impl<T, F, MakeFn> EnqueueFollowUp for CompletedFollowUp<T, F, MakeFn>
where
    T: JobArgs + DeserializeOwned + Send + Sync + 'static,
    F: JobArgs + Send + Sync + 'static,
    MakeFn: Fn(T, &JobRow) -> EnqueueRequest<F> + Send + Sync + 'static,
{
    fn run<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        job: &'a JobRow,
        _outcome_context: Option<&'a OutcomeContext>,
    ) -> Pin<Box<dyn Future<Output = Result<(), AwaError>> + Send + 'a>> {
        Box::pin(async move {
            let args: T = decode_trigger_args(job)?;
            let request = (self.make)(args, job);
            insert_with(&mut *conn, &request.args, request.opts).await?;
            Ok(())
        })
    }
}

/// Spec for the `Retried` outcome.
pub(crate) struct RetriedFollowUp<T, F, MakeFn> {
    pub(crate) make: MakeFn,
    pub(crate) _phantom: PhantomData<fn() -> (T, F)>,
}

impl<T, F, MakeFn> EnqueueFollowUp for RetriedFollowUp<T, F, MakeFn>
where
    T: JobArgs + DeserializeOwned + Send + Sync + 'static,
    F: JobArgs + Send + Sync + 'static,
    MakeFn: Fn(T, &JobRow, &str, i16, chrono::DateTime<chrono::Utc>) -> EnqueueRequest<F>
        + Send
        + Sync
        + 'static,
{
    fn run<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        job: &'a JobRow,
        outcome_context: Option<&'a OutcomeContext>,
    ) -> Pin<Box<dyn Future<Output = Result<(), AwaError>> + Send + 'a>> {
        Box::pin(async move {
            let Some(OutcomeContext::Retried {
                error,
                attempt,
                next_run_at,
            }) = outcome_context
            else {
                return Err(AwaError::Validation(
                    "RetriedFollowUp dispatched without a Retried OutcomeContext".into(),
                ));
            };
            let args: T = decode_trigger_args(job)?;
            let request = (self.make)(args, job, error, *attempt, *next_run_at);
            insert_with(&mut *conn, &request.args, request.opts).await?;
            Ok(())
        })
    }
}

/// Spec for the `Exhausted` outcome (retries-exhausted or terminal-error).
pub(crate) struct ExhaustedFollowUp<T, F, MakeFn> {
    pub(crate) make: MakeFn,
    pub(crate) _phantom: PhantomData<fn() -> (T, F)>,
}

impl<T, F, MakeFn> EnqueueFollowUp for ExhaustedFollowUp<T, F, MakeFn>
where
    T: JobArgs + DeserializeOwned + Send + Sync + 'static,
    F: JobArgs + Send + Sync + 'static,
    MakeFn: Fn(T, &JobRow, &str, i16) -> EnqueueRequest<F> + Send + Sync + 'static,
{
    fn run<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        job: &'a JobRow,
        outcome_context: Option<&'a OutcomeContext>,
    ) -> Pin<Box<dyn Future<Output = Result<(), AwaError>> + Send + 'a>> {
        Box::pin(async move {
            let Some(OutcomeContext::Exhausted { error, attempt }) = outcome_context else {
                return Err(AwaError::Validation(
                    "ExhaustedFollowUp dispatched without an Exhausted OutcomeContext".into(),
                ));
            };
            let args: T = decode_trigger_args(job)?;
            let request = (self.make)(args, job, error, *attempt);
            insert_with(&mut *conn, &request.args, request.opts).await?;
            Ok(())
        })
    }
}

/// Spec for the `Cancelled` outcome.
pub(crate) struct CancelledFollowUp<T, F, MakeFn> {
    pub(crate) make: MakeFn,
    pub(crate) _phantom: PhantomData<fn() -> (T, F)>,
}

impl<T, F, MakeFn> EnqueueFollowUp for CancelledFollowUp<T, F, MakeFn>
where
    T: JobArgs + DeserializeOwned + Send + Sync + 'static,
    F: JobArgs + Send + Sync + 'static,
    MakeFn: Fn(T, &JobRow, &str) -> EnqueueRequest<F> + Send + Sync + 'static,
{
    fn run<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        job: &'a JobRow,
        outcome_context: Option<&'a OutcomeContext>,
    ) -> Pin<Box<dyn Future<Output = Result<(), AwaError>> + Send + 'a>> {
        Box::pin(async move {
            let Some(OutcomeContext::Cancelled { reason }) = outcome_context else {
                return Err(AwaError::Validation(
                    "CancelledFollowUp dispatched without a Cancelled OutcomeContext".into(),
                ));
            };
            let args: T = decode_trigger_args(job)?;
            let request = (self.make)(args, job, reason);
            insert_with(&mut *conn, &request.args, request.opts).await?;
            Ok(())
        })
    }
}

/// Spec for the `WaitingForCallback` outcome.
pub(crate) struct WaitingForCallbackFollowUp<T, F, MakeFn> {
    pub(crate) make: MakeFn,
    pub(crate) _phantom: PhantomData<fn() -> (T, F)>,
}

impl<T, F, MakeFn> EnqueueFollowUp for WaitingForCallbackFollowUp<T, F, MakeFn>
where
    T: JobArgs + DeserializeOwned + Send + Sync + 'static,
    F: JobArgs + Send + Sync + 'static,
    MakeFn: Fn(T, &JobRow) -> EnqueueRequest<F> + Send + Sync + 'static,
{
    fn run<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        job: &'a JobRow,
        _outcome_context: Option<&'a OutcomeContext>,
    ) -> Pin<Box<dyn Future<Output = Result<(), AwaError>> + Send + 'a>> {
        Box::pin(async move {
            let args: T = decode_trigger_args(job)?;
            let request = (self.make)(args, job);
            insert_with(&mut *conn, &request.args, request.opts).await?;
            Ok(())
        })
    }
}

/// Spec for the `Rescued` outcome. `make` receives the trigger's args,
/// post-rescue `JobRow`, and the [`RescueReason`](crate::events::RescueReason).
pub(crate) struct RescuedFollowUp<T, F, MakeFn> {
    pub(crate) make: MakeFn,
    pub(crate) _phantom: PhantomData<fn() -> (T, F)>,
}

impl<T, F, MakeFn> EnqueueFollowUp for RescuedFollowUp<T, F, MakeFn>
where
    T: JobArgs + DeserializeOwned + Send + Sync + 'static,
    F: JobArgs + Send + Sync + 'static,
    MakeFn:
        Fn(T, &JobRow, crate::events::RescueReason) -> EnqueueRequest<F> + Send + Sync + 'static,
{
    fn run<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        job: &'a JobRow,
        outcome_context: Option<&'a OutcomeContext>,
    ) -> Pin<Box<dyn Future<Output = Result<(), AwaError>> + Send + 'a>> {
        Box::pin(async move {
            let Some(OutcomeContext::Rescued { reason }) = outcome_context else {
                return Err(AwaError::Validation(
                    "RescuedFollowUp dispatched without a Rescued OutcomeContext".into(),
                ));
            };
            let args: T = decode_trigger_args(job)?;
            let request = (self.make)(args, job, *reason);
            insert_with(&mut *conn, &request.args, request.opts).await?;
            Ok(())
        })
    }
}

/// Helper used by the executor (and other emission sites) to drive a list of
/// specs against a connection inside an already-open transaction.
pub(crate) async fn dispatch_specs_in_tx(
    conn: &mut PgConnection,
    job: &JobRow,
    specs: &[BoxedEnqueueSpec],
    outcome_context: Option<&OutcomeContext>,
) -> Result<(), AwaError> {
    for spec in specs {
        spec.run(conn, job, outcome_context).await?;
    }
    Ok(())
}
