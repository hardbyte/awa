//! Test utilities for Awa job queue.
//!
//! Provides `TestClient` for integration testing of job handlers.

pub mod setup;

use awa_model::{AwaError, JobArgs, JobRow};
use awa_worker::context::ProgressState;
use awa_worker::{JobContext, JobError, JobResult, Worker};
use sqlx::PgPool;
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// Test client for working with jobs in tests.
///
/// Provides helper methods for inserting jobs and executing them synchronously.
pub struct TestClient {
    pool: PgPool,
}

impl TestClient {
    /// Create a test client from an existing pool.
    pub async fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Get the underlying pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Run migrations (call this in test setup).
    pub async fn migrate(&self) -> Result<(), AwaError> {
        awa_model::migrations::run(&self.pool).await?;
        crate::setup::reset_runtime_backend(&self.pool).await;
        Ok(())
    }

    /// Clean the awa schema (for test isolation).
    pub async fn clean(&self) -> Result<(), AwaError> {
        crate::setup::reset_runtime_backend(&self.pool).await;
        sqlx::query("DELETE FROM awa.jobs")
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM awa.queue_meta")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Insert a job.
    pub async fn insert(&self, args: &impl JobArgs) -> Result<JobRow, AwaError> {
        awa_model::insert(&self.pool, args).await
    }

    /// Claim and execute a single job of type T using the given worker.
    ///
    /// This overload does NOT filter by queue, so it may pick up jobs from any
    /// queue. Prefer `work_one_in_queue` for test isolation.
    pub async fn work_one<W: Worker>(&self, worker: &W) -> Result<WorkResult, AwaError> {
        self.work_one_in_queue(worker, None).await
    }

    /// Claim and execute a single job, optionally filtered by queue.
    ///
    /// Routes through the active storage engine: under queue storage it claims
    /// and records the outcome via the runtime store API; under canonical it
    /// drives the `awa.jobs` view directly.
    pub async fn work_one_in_queue<W: Worker>(
        &self,
        worker: &W,
        queue: Option<&str>,
    ) -> Result<WorkResult, AwaError> {
        if let Some(schema) =
            awa_model::queue_storage::QueueStorage::active_schema(&self.pool).await?
        {
            return self.work_one_queue_storage(worker, queue, &schema).await;
        }

        // Claim one job
        let jobs: Vec<JobRow> = sqlx::query_as::<_, JobRow>(
            r#"
            WITH claimed AS (
                SELECT id FROM awa.jobs
                WHERE state = 'available' AND kind = $1
                  AND ($2::text IS NULL OR queue = $2)
                ORDER BY run_at ASC, id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE awa.jobs
            SET state = 'running',
                attempt = attempt + 1,
                run_lease = run_lease + 1,
                attempted_at = now(),
                heartbeat_at = now(),
                deadline_at = now() + interval '5 minutes'
            FROM claimed
            WHERE awa.jobs.id = claimed.id
            RETURNING awa.jobs.*
            "#,
        )
        .bind(worker.kind())
        .bind(queue)
        .fetch_all(&self.pool)
        .await?;

        let job = match jobs.into_iter().next() {
            Some(job) => job,
            None => return Ok(WorkResult::NoJob),
        };

        let (result, progress_snapshot) = self.run_handler(&job, worker).await;

        // Update job state based on result
        match &result {
            Ok(JobResult::Completed) => {
                sqlx::query(
                    "UPDATE awa.jobs SET state = 'completed', finalized_at = now(), progress = NULL WHERE id = $1",
                )
                .bind(job.id)
                .execute(&self.pool)
                .await?;
                Ok(WorkResult::Completed(job))
            }
            Ok(JobResult::Cancel(reason)) => {
                sqlx::query(
                    "UPDATE awa.jobs SET state = 'cancelled', finalized_at = now(), progress = $2 WHERE id = $1",
                )
                .bind(job.id)
                .bind(&progress_snapshot)
                .execute(&self.pool)
                .await?;
                Ok(WorkResult::Cancelled(job, reason.clone()))
            }
            Ok(JobResult::RetryAfter(_)) | Err(JobError::Retryable(_)) => {
                // Both primitives report a failed attempt; the final attempt
                // exhausts the job, mirroring the real executor.
                if job.attempt >= job.max_attempts {
                    sqlx::query(
                        "UPDATE awa.jobs SET state = 'failed', finalized_at = now(), progress = $2 WHERE id = $1",
                    )
                    .bind(job.id)
                    .bind(&progress_snapshot)
                    .execute(&self.pool)
                    .await?;
                    return Ok(WorkResult::Failed(
                        job,
                        "max_attempts exhausted".to_string(),
                    ));
                }
                sqlx::query(
                    "UPDATE awa.jobs SET state = 'retryable', finalized_at = now(), progress = $2 WHERE id = $1",
                )
                .bind(job.id)
                .bind(&progress_snapshot)
                .execute(&self.pool)
                .await?;
                Ok(WorkResult::Retryable(job))
            }
            Ok(JobResult::Snooze(_)) => {
                sqlx::query(
                    "UPDATE awa.jobs SET state = 'available', attempt = attempt - 1, progress = $2 WHERE id = $1",
                )
                .bind(job.id)
                .bind(&progress_snapshot)
                .execute(&self.pool)
                .await?;
                Ok(WorkResult::Snoozed(job))
            }
            Ok(JobResult::WaitForCallback(_)) => {
                // Check if callback_id was registered
                let has_callback: Option<(Option<uuid::Uuid>,)> =
                    sqlx::query_as("SELECT callback_id FROM awa.jobs WHERE id = $1")
                        .bind(job.id)
                        .fetch_optional(&self.pool)
                        .await?;
                match has_callback {
                    Some((Some(_),)) => {
                        sqlx::query(
                            "UPDATE awa.jobs SET state = 'waiting_external', heartbeat_at = NULL, deadline_at = NULL, progress = $2 WHERE id = $1",
                        )
                        .bind(job.id)
                        .bind(&progress_snapshot)
                        .execute(&self.pool)
                        .await?;
                        let updated = self.get_job(job.id).await?;
                        Ok(WorkResult::WaitingExternal(updated))
                    }
                    _ => {
                        sqlx::query(
                            "UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1",
                        )
                        .bind(job.id)
                        .execute(&self.pool)
                        .await?;
                        Ok(WorkResult::Failed(
                            job,
                            "WaitForCallback returned without calling register_callback"
                                .to_string(),
                        ))
                    }
                }
            }
            Err(JobError::Terminal(msg)) => {
                sqlx::query(
                    "UPDATE awa.jobs SET state = 'failed', finalized_at = now(), progress = $2 WHERE id = $1",
                )
                .bind(job.id)
                .bind(&progress_snapshot)
                .execute(&self.pool)
                .await?;
                Ok(WorkResult::Failed(job, msg.clone()))
            }
        }
    }

    /// Build a testing `JobContext`, run the worker, and snapshot any progress
    /// it buffered. Shared by the canonical and queue-storage work paths.
    async fn run_handler<W: Worker>(
        &self,
        job: &JobRow,
        worker: &W,
    ) -> (Result<JobResult, JobError>, Option<serde_json::Value>) {
        let cancel = Arc::new(AtomicBool::new(false));
        let state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>> =
            Arc::new(HashMap::new());
        let progress = Arc::new(std::sync::Mutex::new(ProgressState::new(
            job.progress.clone(),
        )));
        let ctx = JobContext::new_for_testing(
            job.clone(),
            cancel,
            state,
            self.pool.clone(),
            progress.clone(),
        );
        let result = worker.perform(&ctx).await;
        let snapshot = {
            let guard = progress.lock().expect("progress lock poisoned");
            guard.clone_latest()
        };
        (result, snapshot)
    }

    /// Queue-storage variant of [`Self::work_one_in_queue`]: claim, run the
    /// handler, and record the outcome through the runtime store API rather
    /// than writing the `awa.jobs` view (which the engine rejects).
    async fn work_one_queue_storage<W: Worker>(
        &self,
        worker: &W,
        queue: Option<&str>,
        schema: &str,
    ) -> Result<WorkResult, AwaError> {
        use std::time::Duration;
        let store = awa_model::queue_storage::QueueStorage::from_existing_schema(schema)?;

        // The store claims per queue; when the caller did not pin one, resolve a
        // queue carrying a ready job of this worker's kind.
        let target_queue = match queue {
            Some(queue) => queue.to_string(),
            None => {
                let resolved: Option<String> = sqlx::query_scalar(&format!(
                    "SELECT queue FROM {schema}.ready_entries WHERE kind = $1 ORDER BY job_id ASC LIMIT 1"
                ))
                .bind(worker.kind())
                .fetch_optional(&self.pool)
                .await?;
                match resolved {
                    Some(queue) => queue,
                    None => return Ok(WorkResult::NoJob),
                }
            }
        };

        let claimed = store
            .claim_runtime_batch(&self.pool, &target_queue, 1, Duration::from_secs(60))
            .await?;
        let claimed = match claimed.into_iter().next() {
            Some(claimed) => claimed,
            None => return Ok(WorkResult::NoJob),
        };
        let job = claimed.job.clone();

        let (result, progress_snapshot) = self.run_handler(&job, worker).await;

        match &result {
            Ok(JobResult::Completed) => {
                store
                    .complete_runtime_batch(&self.pool, std::slice::from_ref(&claimed))
                    .await?;
                Ok(WorkResult::Completed(job))
            }
            Ok(JobResult::Cancel(reason)) => {
                store
                    .cancel_running(&self.pool, job.id, job.run_lease, reason, progress_snapshot)
                    .await?;
                Ok(WorkResult::Cancelled(job, reason.clone()))
            }
            Ok(JobResult::RetryAfter(delay)) => {
                // The final attempt exhausts the job, mirroring the real
                // executor.
                if job.attempt >= job.max_attempts {
                    store
                        .fail_terminal(
                            &self.pool,
                            job.id,
                            job.run_lease,
                            "max_attempts exhausted",
                            progress_snapshot,
                        )
                        .await?;
                    return Ok(WorkResult::Failed(
                        job,
                        "max_attempts exhausted".to_string(),
                    ));
                }
                store
                    .retry_after(&self.pool, job.id, job.run_lease, *delay, progress_snapshot)
                    .await?;
                Ok(WorkResult::Retryable(job))
            }
            Err(JobError::Retryable(_)) => {
                // The final attempt exhausts the job, mirroring the real
                // executor.
                if job.attempt >= job.max_attempts {
                    store
                        .fail_terminal(
                            &self.pool,
                            job.id,
                            job.run_lease,
                            "max_attempts exhausted",
                            progress_snapshot,
                        )
                        .await?;
                    return Ok(WorkResult::Failed(
                        job,
                        "max_attempts exhausted".to_string(),
                    ));
                }
                // A retryable error reschedules the job. Canonical leaves it in
                // a finalized `retryable` state (not re-claimable by a later
                // `work_one`); the store needs an explicit delay, so use a long
                // one rather than a near-immediate reschedule that would let the
                // same job be re-claimed within a test or accumulate across runs.
                store
                    .retry_after(
                        &self.pool,
                        job.id,
                        job.run_lease,
                        Duration::from_secs(3600),
                        progress_snapshot,
                    )
                    .await?;
                Ok(WorkResult::Retryable(job))
            }
            Ok(JobResult::Snooze(delay)) => {
                store
                    .snooze(&self.pool, job.id, job.run_lease, *delay, progress_snapshot)
                    .await?;
                Ok(WorkResult::Snoozed(job))
            }
            Ok(JobResult::WaitForCallback(_)) => {
                // The handler registers the callback via the context; park to
                // waiting_external when it did, otherwise fail like the
                // canonical path.
                match self.get_job(job.id).await?.callback_id {
                    Some(callback_id) => {
                        let entered = store
                            .enter_callback_wait(&self.pool, job.id, job.run_lease, callback_id)
                            .await?;
                        assert!(
                            entered,
                            "enter_callback_wait did not transition job {} to waiting_external",
                            job.id
                        );
                        Ok(WorkResult::WaitingExternal(self.get_job(job.id).await?))
                    }
                    None => {
                        let msg = "WaitForCallback returned without calling register_callback";
                        store
                            .fail_terminal(
                                &self.pool,
                                job.id,
                                job.run_lease,
                                msg,
                                progress_snapshot,
                            )
                            .await?;
                        Ok(WorkResult::Failed(job, msg.to_string()))
                    }
                }
            }
            Err(JobError::Terminal(msg)) => {
                store
                    .fail_terminal(&self.pool, job.id, job.run_lease, msg, progress_snapshot)
                    .await?;
                Ok(WorkResult::Failed(job, msg.clone()))
            }
        }
    }

    /// Get a job by ID.
    pub async fn get_job(&self, job_id: i64) -> Result<JobRow, AwaError> {
        awa_model::admin::get_job(&self.pool, job_id).await
    }
}

/// Result of `work_one`.
#[derive(Debug)]
pub enum WorkResult {
    /// No job was available.
    NoJob,
    /// Job completed successfully.
    Completed(JobRow),
    /// Job was retried.
    Retryable(JobRow),
    /// Job was snoozed.
    Snoozed(JobRow),
    /// Job was cancelled.
    Cancelled(JobRow, String),
    /// Job failed terminally.
    Failed(JobRow, String),
    /// Job is waiting for an external callback.
    WaitingExternal(JobRow),
}

impl WorkResult {
    pub fn is_completed(&self) -> bool {
        matches!(self, WorkResult::Completed(_))
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, WorkResult::Failed(_, _))
    }

    pub fn is_no_job(&self) -> bool {
        matches!(self, WorkResult::NoJob)
    }

    pub fn is_waiting_external(&self) -> bool {
        matches!(self, WorkResult::WaitingExternal(_))
    }
}
