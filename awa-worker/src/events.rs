use awa_model::JobRow;
use chrono::{DateTime, Utc};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// Typed lifecycle event for a specific job argument type.
#[derive(Debug, Clone)]
pub enum JobEvent<T> {
    /// Job completed successfully.
    Completed {
        args: T,
        job: JobRow,
        duration: Duration,
    },
    /// Job failed but will be retried later.
    Retried {
        args: T,
        job: JobRow,
        error: String,
        attempt: i16,
        next_run_at: DateTime<Utc>,
    },
    /// Job exhausted its retry budget and moved to `failed`.
    Exhausted {
        args: T,
        job: JobRow,
        error: String,
        attempt: i16,
    },
    /// Job was cancelled by the handler.
    Cancelled {
        args: T,
        job: JobRow,
        reason: String,
    },
}

impl<T> JobEvent<T> {
    /// The job snapshot associated with this event.
    pub fn job(&self) -> &JobRow {
        match self {
            JobEvent::Completed { job, .. }
            | JobEvent::Retried { job, .. }
            | JobEvent::Exhausted { job, .. }
            | JobEvent::Cancelled { job, .. } => job,
        }
    }
}

/// Untyped lifecycle event keyed only by job kind.
#[derive(Debug, Clone)]
pub enum UntypedJobEvent {
    /// Job completed successfully.
    Completed { job: JobRow, duration: Duration },
    /// Job failed but will be retried later.
    Retried {
        job: JobRow,
        error: String,
        attempt: i16,
        next_run_at: DateTime<Utc>,
    },
    /// Job exhausted its retry budget and moved to `failed`.
    Exhausted {
        job: JobRow,
        error: String,
        attempt: i16,
    },
    /// Job was cancelled by the handler.
    Cancelled { job: JobRow, reason: String },
}

impl UntypedJobEvent {
    /// The job snapshot associated with this event.
    pub fn job(&self) -> &JobRow {
        match self {
            UntypedJobEvent::Completed { job, .. }
            | UntypedJobEvent::Retried { job, .. }
            | UntypedJobEvent::Exhausted { job, .. }
            | UntypedJobEvent::Cancelled { job, .. } => job,
        }
    }

    pub(crate) fn into_typed<T>(self, args: T) -> JobEvent<T> {
        match self {
            UntypedJobEvent::Completed { job, duration } => JobEvent::Completed {
                args,
                job,
                duration,
            },
            UntypedJobEvent::Retried {
                job,
                error,
                attempt,
                next_run_at,
            } => JobEvent::Retried {
                args,
                job,
                error,
                attempt,
                next_run_at,
            },
            UntypedJobEvent::Exhausted {
                job,
                error,
                attempt,
            } => JobEvent::Exhausted {
                args,
                job,
                error,
                attempt,
            },
            UntypedJobEvent::Cancelled { job, reason } => JobEvent::Cancelled { args, job, reason },
        }
    }
}

pub(crate) type BoxedLifecycleFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(crate) type BoxedUntypedEventHandler =
    Arc<dyn Fn(UntypedJobEvent) -> BoxedLifecycleFuture + Send + Sync + 'static>;
