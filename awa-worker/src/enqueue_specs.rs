//! Transactional follow-up enqueue specs (ADR-029).
//!
//! A spec is a per-kind registration that, when its triggering state
//! transition commits, inserts a follow-up Awa job in the same database
//! transaction. The follow-up rides Awa's existing durability (at-least-once,
//! retries, DLQ, admin visibility), so the side effect cannot be lost between
//! the state commit and the hook dispatch the way an in-process hook can.
//!
//! Specs are type-erased here so the executor can dispatch them without
//! knowing the trigger or follow-up types statically. The user-facing
//! `ClientBuilder::on_*_enqueue` methods wrap their typed closures into
//! impls of [`EnqueueFollowUp`] and accumulate them in a per-kind map.

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

    /// Replace the follow-up's [`InsertOpts`] wholesale (for callers who need
    /// fields the builder methods above do not yet cover — `metadata`, `tags`,
    /// `unique`, `run_at`, `deadline_duration`, `ordering_key`).
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

/// Type-erased follow-up-enqueue spec for one (trigger kind, outcome) pair.
///
/// Implementors deserialise the trigger's args from the committed `JobRow`,
/// apply the user's closure to produce the follow-up's `JobArgs` and
/// [`InsertOpts`], then insert through the supplied executor — which the
/// caller scopes to the same transaction as the triggering state commit.
pub(crate) trait EnqueueFollowUp: Send + Sync {
    fn run<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        job: &'a JobRow,
    ) -> Pin<Box<dyn Future<Output = Result<(), AwaError>> + Send + 'a>>;
}

pub(crate) type BoxedEnqueueSpec = Arc<dyn EnqueueFollowUp + 'static>;

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
    ) -> Pin<Box<dyn Future<Output = Result<(), AwaError>> + Send + 'a>> {
        Box::pin(async move {
            // Deserialise the trigger args from the committed snapshot. If
            // they don't decode, treat it as a configuration error in the
            // hook rather than a job failure — the trigger has already
            // committed by the time this runs.
            let args: T = serde_json::from_value(job.args.clone()).map_err(|err| {
                AwaError::Validation(format!(
                    "follow-up enqueue: failed to decode trigger args for kind {}: {err}",
                    job.kind
                ))
            })?;
            let request = (self.make)(args, job);
            insert_with(&mut *conn, &request.args, request.opts).await?;
            Ok(())
        })
    }
}
