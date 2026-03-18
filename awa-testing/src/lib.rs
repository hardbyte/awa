//! Test utilities for Awa job queue.
//!
//! Provides `TestClient` for integration testing of job handlers.

use awa_model::{AwaError, JobArgs, JobRow};
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
        awa_model::migrations::run(&self.pool).await
    }

    /// Clean the awa schema (for test isolation).
    pub async fn clean(&self) -> Result<(), AwaError> {
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
    pub async fn work_one_in_queue<W: Worker>(
        &self,
        worker: &W,
        queue: Option<&str>,
    ) -> Result<WorkResult, AwaError> {
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

        let cancel = Arc::new(AtomicBool::new(false));
        let state: Arc<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>> =
            Arc::new(HashMap::new());
        let ctx = JobContext::new(job.clone(), cancel, state);

        let result = worker.perform(&job, &ctx).await;

        // Update job state based on result
        match &result {
            Ok(JobResult::Completed) => {
                sqlx::query(
                    "UPDATE awa.jobs SET state = 'completed', finalized_at = now() WHERE id = $1",
                )
                .bind(job.id)
                .execute(&self.pool)
                .await?;
                Ok(WorkResult::Completed(job))
            }
            Ok(JobResult::Cancel(reason)) => {
                sqlx::query(
                    "UPDATE awa.jobs SET state = 'cancelled', finalized_at = now() WHERE id = $1",
                )
                .bind(job.id)
                .execute(&self.pool)
                .await?;
                Ok(WorkResult::Cancelled(job, reason.clone()))
            }
            Ok(JobResult::RetryAfter(_)) | Err(JobError::Retryable(_)) => {
                sqlx::query(
                    "UPDATE awa.jobs SET state = 'retryable', finalized_at = now() WHERE id = $1",
                )
                .bind(job.id)
                .execute(&self.pool)
                .await?;
                Ok(WorkResult::Retryable(job))
            }
            Ok(JobResult::Snooze(_)) => {
                sqlx::query(
                    "UPDATE awa.jobs SET state = 'available', attempt = attempt - 1 WHERE id = $1",
                )
                .bind(job.id)
                .execute(&self.pool)
                .await?;
                Ok(WorkResult::Snoozed(job))
            }
            Err(JobError::Terminal(msg)) => {
                sqlx::query(
                    "UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1",
                )
                .bind(job.id)
                .execute(&self.pool)
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
}
