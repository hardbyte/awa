use crate::context::JobContext;
use awa_model::{AwaError, JobRow};
use sqlx::PgPool;
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, info_span, warn, Instrument};

/// Result of executing a job handler.
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
}

/// Error type for job handlers — any error is retryable unless it's terminal.
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
    pub fn retryable(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        JobError::Retryable(Box::new(err))
    }

    pub fn terminal(msg: impl Into<String>) -> Self {
        JobError::Terminal(msg.into())
    }
}

/// Worker trait — implement this for each job type.
#[async_trait::async_trait]
pub trait Worker: Send + Sync + 'static {
    /// The kind string for this worker (must match the job's kind).
    fn kind(&self) -> &'static str;

    /// Execute the job. The raw args JSON and context are provided.
    async fn perform(&self, job_row: &JobRow, ctx: &JobContext) -> Result<JobResult, JobError>;
}

/// Type-erased worker wrapper for the registry.
pub(crate) type BoxedWorker = Box<dyn Worker>;

/// Manages job execution — spawns worker futures and tracks in-flight jobs.
pub struct JobExecutor {
    pool: PgPool,
    workers: Arc<HashMap<String, BoxedWorker>>,
    in_flight: Arc<RwLock<HashMap<i64, Arc<AtomicBool>>>>,
    queue_in_flight: Arc<HashMap<String, Arc<AtomicU32>>>,
    state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
    metrics: crate::metrics::AwaMetrics,
}

impl JobExecutor {
    pub fn new(
        pool: PgPool,
        workers: Arc<HashMap<String, BoxedWorker>>,
        in_flight: Arc<RwLock<HashMap<i64, Arc<AtomicBool>>>>,
        queue_in_flight: Arc<HashMap<String, Arc<AtomicU32>>>,
        state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
        metrics: crate::metrics::AwaMetrics,
    ) -> Self {
        Self {
            pool,
            workers,
            in_flight,
            queue_in_flight,
            state,
            metrics,
        }
    }

    /// Execute a claimed job. Returns a JoinHandle for the spawned task.
    pub fn execute(&self, job: JobRow, cancel: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
        let pool = self.pool.clone();
        let workers = self.workers.clone();
        let in_flight = self.in_flight.clone();
        let queue_in_flight = self.queue_in_flight.clone();
        let state = self.state.clone();
        let metrics = self.metrics.clone();
        let job_id = job.id;
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

        tokio::spawn(
            async move {
                // Register as in-flight
                {
                    let mut guard = in_flight.write().await;
                    guard.insert(job_id, cancel.clone());
                }
                if let Some(counter) = queue_in_flight.get(&job_queue) {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                metrics.record_in_flight_change(&job_queue, 1);

                let start = std::time::Instant::now();
                let ctx = JobContext::new(job.clone(), cancel, state);

                let result = match workers.get(&job.kind) {
                    Some(worker) => worker.perform(&job, &ctx).await,
                    None => {
                        error!(kind = %job.kind, job_id, "No worker registered for job kind");
                        Err(JobError::Terminal(format!(
                            "unknown job kind: {}",
                            job.kind
                        )))
                    }
                };

                let duration = start.elapsed();

                // Complete the job based on the result, then record metrics
                // only if the state transition actually happened (not stale).
                match complete_job(&pool, &job, &result).await {
                    Ok(true) => {
                        // State transition succeeded — record metrics
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
                            Ok(JobResult::Snooze(_)) => {} // Not a terminal outcome
                            Err(JobError::Terminal(_)) => {
                                metrics.record_job_failed(&job_kind, &job_queue, true);
                            }
                            Err(JobError::Retryable(_)) => {
                                metrics.record_job_retried(&job_kind, &job_queue);
                            }
                        }
                    }
                    Ok(false) => {
                        // Job was already rescued/cancelled — no metrics
                    }
                    Err(err) => {
                        error!(job_id, error = %err, "Failed to complete job");
                    }
                }

                // Remove from in-flight
                {
                    let mut guard = in_flight.write().await;
                    guard.remove(&job_id);
                }
                if let Some(counter) = queue_in_flight.get(&job_queue) {
                    counter.fetch_sub(1, Ordering::SeqCst);
                }
                metrics.record_in_flight_change(&job_queue, -1);
            }
            .instrument(span),
        )
    }
}

/// Update job state in the database based on handler result.
///
/// Returns `true` if the state transition happened, `false` if the job was
/// already rescued/cancelled by maintenance (stale completion).
async fn complete_job(
    pool: &PgPool,
    job: &JobRow,
    result: &Result<JobResult, JobError>,
) -> Result<bool, AwaError> {
    match result {
        Ok(JobResult::Completed) => {
            tracing::Span::current().record("otel.status_code", "OK");
            info!(job_id = job.id, kind = %job.kind, attempt = job.attempt, "Job completed");
            let result = sqlx::query(
                "UPDATE awa.jobs SET state = 'completed', finalized_at = now() WHERE id = $1 AND state = 'running'",
            )
            .bind(job.id)
            .execute(pool)
            .await?;
            if result.rows_affected() == 0 {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, completion ignored"
                );
                return Ok(false);
            }
        }

        Ok(JobResult::RetryAfter(duration)) => {
            let seconds = duration.as_secs() as f64;
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
                    finalized_at = now()
                WHERE id = $1 AND state = 'running'
                "#,
            )
            .bind(job.id)
            .bind(seconds)
            .execute(pool)
            .await?;
            if result.rows_affected() == 0 {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, retry ignored"
                );
                return Ok(false);
            }
        }

        Ok(JobResult::Snooze(duration)) => {
            let seconds = duration.as_secs() as f64;
            info!(
                job_id = job.id,
                kind = %job.kind,
                snooze_secs = seconds,
                "Job snoozed (attempt not incremented)"
            );
            // Snooze: back to available with new run_at, decrement attempt
            // (since it was already incremented at claim time)
            let result = sqlx::query(
                r#"
                UPDATE awa.jobs
                SET state = 'scheduled',
                    run_at = now() + make_interval(secs => $2),
                    attempt = attempt - 1,
                    heartbeat_at = NULL,
                    deadline_at = NULL
                WHERE id = $1 AND state = 'running'
                "#,
            )
            .bind(job.id)
            .bind(seconds)
            .execute(pool)
            .await?;
            if result.rows_affected() == 0 {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, snooze ignored"
                );
                return Ok(false);
            }
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
                    errors = errors || $2::jsonb
                WHERE id = $1 AND state = 'running'
                "#,
            )
            .bind(job.id)
            .bind(serde_json::json!({
                "error": format!("cancelled: {}", reason),
                "attempt": job.attempt,
                "at": chrono::Utc::now().to_rfc3339()
            }))
            .execute(pool)
            .await?;
            if result.rows_affected() == 0 {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, cancel ignored"
                );
                return Ok(false);
            }
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
                    errors = errors || $2::jsonb
                WHERE id = $1 AND state = 'running'
                "#,
            )
            .bind(job.id)
            .bind(serde_json::json!({
                "error": msg.to_string(),
                "attempt": job.attempt,
                "at": chrono::Utc::now().to_rfc3339(),
                "terminal": true
            }))
            .execute(pool)
            .await?;
            if result.rows_affected() == 0 {
                warn!(
                    job_id = job.id,
                    "Job already rescued/cancelled, terminal failure ignored"
                );
                return Ok(false);
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
                        errors = errors || $2::jsonb
                    WHERE id = $1 AND state = 'running'
                    "#,
                )
                .bind(job.id)
                .bind(serde_json::json!({
                    "error": error_msg,
                    "attempt": job.attempt,
                    "at": chrono::Utc::now().to_rfc3339()
                }))
                .execute(pool)
                .await?;
                if result.rows_affected() == 0 {
                    warn!(
                        job_id = job.id,
                        "Job already rescued/cancelled, failure ignored"
                    );
                    return Ok(false);
                }
            } else {
                warn!(
                    job_id = job.id,
                    kind = %job.kind,
                    attempt = job.attempt,
                    error = %error_msg,
                    "Job failed (will retry)"
                );
                // Use database-side backoff calculation
                let result = sqlx::query(
                    r#"
                    UPDATE awa.jobs
                    SET state = 'retryable',
                        run_at = now() + awa.backoff_duration($2, $3),
                        finalized_at = now(),
                        heartbeat_at = NULL,
                        deadline_at = NULL,
                        errors = errors || $4::jsonb
                    WHERE id = $1 AND state = 'running'
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
                .execute(pool)
                .await?;
                if result.rows_affected() == 0 {
                    warn!(
                        job_id = job.id,
                        "Job already rescued/cancelled, retry ignored"
                    );
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}
