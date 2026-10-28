//! Transactional follow-up enqueue specs (ADR-029).
//!
//! A spec is a per-(outcome, kind) registration that fires when its
//! triggering state transition commits. Worker-driven outcomes and
//! callback resolution via the worker `Client::*_external` APIs
//! dispatch the follow-up `INSERT` in the same transaction as the
//! state UPDATE (atomic with the trigger — a spec failure rolls the
//! trigger back so the caller / external sender can retry). Maintenance
//! rescue dispatches in a separate transaction after the rescue
//! commits (best-effort — a failed `INSERT` is logged and the rescue
//! stands). Either way, once the follow-up `INSERT` commits the row is
//! a regular Awa job: at-least-once, retried, DLQ-aware, visible to
//! admin tooling.
//!
//! Specs are type-erased here so the executor can dispatch them without
//! knowing the trigger or follow-up types statically. The user-facing
//! `ClientBuilder::on_*_enqueue` methods wrap their typed closures into
//! impls of [`EnqueueFollowUp`] and accumulate them in a two-level
//! `outcome -> kind -> specs` registry.

use awa_model::{
    insert_prepared, insert_with, map_sqlx_error, AwaError, InsertOpts, JobArgs, JobRow,
    PreparedJobInsert, UniqueOpts,
};
use serde::de::DeserializeOwned;
use sqlx::{Connection, PgConnection};
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

    /// Set the follow-up's unique options without replacing the rest of its
    /// [`InsertOpts`]. A follow-up whose unique key already exists is
    /// skipped and its trigger still commits.
    pub fn unique(mut self, unique: UniqueOpts) -> Self {
        self.opts.unique = Some(unique);
        self
    }

    /// Set the follow-up's metadata without replacing the rest of its
    /// [`InsertOpts`]. Metadata is outside the unique key, so it can carry
    /// context that must not affect deduplication.
    pub fn metadata(mut self, metadata: serde_json::Value) -> Self {
        self.opts.metadata = metadata;
        self
    }

    /// Replace the follow-up's [`InsertOpts`] wholesale — useful for fields
    /// without dedicated builder methods (`tags`, `run_at`,
    /// `deadline_duration`, `ordering_key`). It also replaces anything set
    /// earlier with `.queue()`, `.unique()` and the other builders.
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

/// Invoke a user-supplied `make` closure, converting any panic into an
/// `AwaError` so the spec dispatcher can roll back its transaction and
/// log the failure the same way it would for a returned error. Without
/// this, a panic in user code would unwind:
/// - on worker-driven outcomes, the executor's completion task (the
///   transition tx would still roll back via Drop, but the worker would
///   log a JoinError rather than the typed spec failure);
/// - on callback-resolution paths, the resolver task (after the
///   resolution itself has already committed) — observers and in-process
///   hooks downstream of the panic point never run;
/// - on rescue paths, the maintenance task — likewise dropping the
///   detached hook spawn for that rescue.
fn catch_make_panic<R>(
    kind: &str,
    f: impl FnOnce() -> R + std::panic::UnwindSafe,
) -> Result<R, AwaError> {
    std::panic::catch_unwind(f).map_err(|panic| {
        let detail = if let Some(msg) = panic.downcast_ref::<&'static str>() {
            (*msg).to_string()
        } else if let Some(msg) = panic.downcast_ref::<String>() {
            msg.clone()
        } else {
            "panic payload not a string".to_string()
        };
        AwaError::Validation(format!(
            "follow-up enqueue closure for kind {kind} panicked: {detail}"
        ))
    })
}

/// Insert one follow-up inside the trigger's transaction, under a savepoint.
///
/// A unique conflict means the follow-up already exists: the request carried
/// unique options precisely so that duplicates collapse, so the duplicate is
/// skipped and the trigger's transition still commits. Without the savepoint
/// the conflict would abort the whole transaction and roll the trigger back,
/// and every retry of the trigger would hit the same conflict until it ran
/// out of attempts. Any other error still fails the dispatch. Follow-ups
/// without unique options cannot conflict and skip the savepoint.
async fn insert_follow_up<F: JobArgs>(
    conn: &mut PgConnection,
    trigger: &JobRow,
    args: &F,
    opts: InsertOpts,
) -> Result<(), AwaError> {
    if opts.unique.is_none() {
        return insert_with(&mut *conn, args, opts).await.map(|_| ());
    }
    let mut savepoint = conn.begin().await.map_err(map_sqlx_error)?;
    let inserted = insert_with(&mut *savepoint, args, opts).await.map(|_| ());
    settle_follow_up_savepoint(savepoint, inserted, trigger, F::kind())
        .await
        .map(|_| ())
}

/// Insert one handler-staged follow-up (ADR-046) inside the completion
/// transaction, with the same duplicate handling as [`insert_follow_up`].
/// Returns whether the row was inserted rather than skipped as a duplicate.
async fn insert_staged_follow_up(
    conn: &mut PgConnection,
    trigger: &JobRow,
    row: &PreparedJobInsert,
) -> Result<bool, AwaError> {
    if row.unique_key().is_none() {
        return insert_prepared(&mut *conn, row).await.map(|_| true);
    }
    let mut savepoint = conn.begin().await.map_err(map_sqlx_error)?;
    let inserted = insert_prepared(&mut *savepoint, row).await.map(|_| ());
    settle_follow_up_savepoint(savepoint, inserted, trigger, row.kind()).await
}

/// Release the savepoint around a unique follow-up insert, or roll it back
/// and skip the follow-up when its key already exists. Returns whether the
/// follow-up was inserted.
async fn settle_follow_up_savepoint(
    savepoint: sqlx::Transaction<'_, sqlx::Postgres>,
    inserted: Result<(), AwaError>,
    trigger: &JobRow,
    follow_up_kind: &str,
) -> Result<bool, AwaError> {
    match inserted {
        Ok(()) => savepoint
            .commit()
            .await
            .map_err(map_sqlx_error)
            .map(|()| true),
        Err(AwaError::UniqueConflict { constraint })
            if is_duplicate_follow_up(constraint.as_deref()) =>
        {
            savepoint.rollback().await.map_err(map_sqlx_error)?;
            tracing::debug!(
                trigger_job_id = trigger.id,
                trigger_kind = %trigger.kind,
                follow_up_kind,
                constraint = constraint.as_deref().unwrap_or(""),
                "Follow-up already enqueued; skipping the duplicate"
            );
            Ok(false)
        }
        Err(err) => Err(err),
    }
}

/// The constraint a unique-key conflict reports on both storage engines.
/// Any other unique violation (a primary-key collision, say) is not a
/// duplicate of the follow-up and must still fail the dispatch.
const JOB_UNIQUE_KEY_CONSTRAINT: &str = "idx_awa_jobs_unique";

fn is_duplicate_follow_up(constraint: Option<&str>) -> bool {
    constraint == Some(JOB_UNIQUE_KEY_CONSTRAINT)
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
            let request = catch_make_panic(
                &job.kind,
                std::panic::AssertUnwindSafe(|| (self.make)(args, job)),
            )?;
            insert_follow_up(&mut *conn, job, &request.args, request.opts).await?;
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
            let request = catch_make_panic(
                &job.kind,
                std::panic::AssertUnwindSafe(|| {
                    (self.make)(args, job, error, *attempt, *next_run_at)
                }),
            )?;
            insert_follow_up(&mut *conn, job, &request.args, request.opts).await?;
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
            let request = catch_make_panic(
                &job.kind,
                std::panic::AssertUnwindSafe(|| (self.make)(args, job, error, *attempt)),
            )?;
            insert_follow_up(&mut *conn, job, &request.args, request.opts).await?;
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
            let request = catch_make_panic(
                &job.kind,
                std::panic::AssertUnwindSafe(|| (self.make)(args, job, reason)),
            )?;
            insert_follow_up(&mut *conn, job, &request.args, request.opts).await?;
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
            let request = catch_make_panic(
                &job.kind,
                std::panic::AssertUnwindSafe(|| (self.make)(args, job)),
            )?;
            insert_follow_up(&mut *conn, job, &request.args, request.opts).await?;
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
            let request = catch_make_panic(
                &job.kind,
                std::panic::AssertUnwindSafe(|| (self.make)(args, job, *reason)),
            )?;
            insert_follow_up(&mut *conn, job, &request.args, request.opts).await?;
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
    if specs.is_empty() {
        return Ok(());
    }
    // ADR-048: a spec never sees an output, so no follow-up is derived from
    // one. Values a follow-up needs travel in its args.
    let job = crate::output::without_outputs(job);
    for spec in specs {
        spec.run(conn, &job, outcome_context).await?;
    }
    Ok(())
}

/// Insert the follow-ups a handler staged (ADR-046), in staging order, inside
/// the attempt's completion transaction. Runs after the registered specs.
/// What a completion's staged follow-ups came to.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StagedInserts {
    pub(crate) inserted: u64,
    pub(crate) duplicates: u64,
}

pub(crate) async fn insert_staged_in_tx(
    conn: &mut PgConnection,
    job: &JobRow,
    staged: &[PreparedJobInsert],
) -> Result<StagedInserts, AwaError> {
    let mut counts = StagedInserts::default();
    for row in staged {
        if insert_staged_follow_up(conn, job, row).await? {
            counts.inserted += 1;
        } else {
            counts.duplicates += 1;
        }
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::is_duplicate_follow_up;

    #[test]
    fn only_the_unique_key_constraint_counts_as_a_duplicate() {
        assert!(is_duplicate_follow_up(Some("idx_awa_jobs_unique")));
        assert!(!is_duplicate_follow_up(Some("jobs_hot_pkey")));
        assert!(!is_duplicate_follow_up(None));
    }
}
