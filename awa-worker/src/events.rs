use awa_model::JobRow;
use chrono::{DateTime, Utc};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// Typed lifecycle event for a specific job argument type.
#[derive(Debug, Clone)]
pub enum JobEvent<T> {
    /// Job execution started after the claim committed.
    Started { args: T, job: JobRow },
    /// Job parked waiting for an external callback. `job.callback_id` and
    /// `job.callback_timeout_at` identify the pending callback.
    WaitingForCallback { args: T, job: JobRow },
    /// Job completed successfully.
    ///
    /// For a callback-driven completion `duration` is [`Duration::ZERO`], since
    /// no handler ran during the parked phase.
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
    /// Job was rescued by maintenance — either its callback wait expired,
    /// its heartbeat went stale (presumed crashed worker), or its deadline
    /// was exceeded. `reason` identifies the rescue path. The post-rescue
    /// `job.state` is `retryable` when retries remain or `failed` when the
    /// attempt that was rescued exhausted its retry budget.
    Rescued {
        args: T,
        job: JobRow,
        reason: RescueReason,
    },
}

/// Why maintenance rescued the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RescueReason {
    /// The external callback's `callback_timeout_at` elapsed without resolution.
    ExpiredCallback,
    /// The job's heartbeat went stale (worker presumed crashed or stuck).
    StaleHeartbeat,
    /// The job's `deadline_at` elapsed.
    DeadlineExceeded,
}

impl RescueReason {
    /// Stable identifier suitable for logs / spec dispatch context.
    pub fn as_str(self) -> &'static str {
        match self {
            RescueReason::ExpiredCallback => "expired_callback",
            RescueReason::StaleHeartbeat => "stale_heartbeat",
            RescueReason::DeadlineExceeded => "deadline_exceeded",
        }
    }
}

impl<T> JobEvent<T> {
    /// The job snapshot associated with this event.
    pub fn job(&self) -> &JobRow {
        match self {
            JobEvent::Started { job, .. }
            | JobEvent::WaitingForCallback { job, .. }
            | JobEvent::Completed { job, .. }
            | JobEvent::Retried { job, .. }
            | JobEvent::Exhausted { job, .. }
            | JobEvent::Cancelled { job, .. }
            | JobEvent::Rescued { job, .. } => job,
        }
    }
}

/// Untyped lifecycle event keyed only by job kind.
#[derive(Debug, Clone)]
pub enum UntypedJobEvent {
    /// Job execution started after the claim committed.
    Started { job: JobRow },
    /// Job parked waiting for an external callback.
    WaitingForCallback { job: JobRow },
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
    /// Job was rescued by maintenance. See [`JobEvent::Rescued`].
    Rescued { job: JobRow, reason: RescueReason },
}

impl UntypedJobEvent {
    /// The job snapshot associated with this event.
    pub fn job(&self) -> &JobRow {
        match self {
            UntypedJobEvent::Started { job, .. }
            | UntypedJobEvent::WaitingForCallback { job, .. }
            | UntypedJobEvent::Completed { job, .. }
            | UntypedJobEvent::Retried { job, .. }
            | UntypedJobEvent::Exhausted { job, .. }
            | UntypedJobEvent::Cancelled { job, .. }
            | UntypedJobEvent::Rescued { job, .. } => job,
        }
    }

    pub(crate) fn into_typed<T>(self, args: T) -> JobEvent<T> {
        match self {
            UntypedJobEvent::Started { job } => JobEvent::Started { args, job },
            UntypedJobEvent::WaitingForCallback { job } => {
                JobEvent::WaitingForCallback { args, job }
            }
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
            UntypedJobEvent::Rescued { job, reason } => JobEvent::Rescued { args, job, reason },
        }
    }
}

pub(crate) type BoxedLifecycleFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(crate) type BoxedUntypedEventHandler =
    Arc<dyn Fn(UntypedJobEvent) -> BoxedLifecycleFuture + Send + Sync + 'static>;
