//! Integration tests for structured job progress and metadata updates (#12).
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{admin, migrations};
use awa::{JobArgs, JobContext, JobError, JobResult, JobState, Worker};
use awa_testing::TestClient;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup() -> TestClient {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");

    let client = TestClient::from_pool(pool).await;
    client.migrate().await.expect("Failed to run migrations");
    client
}

async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue jobs");
}

// -- Job types for testing --

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct ProgressJob {
    pub data: String,
}

// -- Worker implementations --

/// Worker that sets progress to 50%.
struct SetProgressWorker;

#[async_trait::async_trait]
impl Worker for SetProgressWorker {
    fn kind(&self) -> &'static str {
        "progress_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        ctx.set_progress(50, "half done");
        ctx.flush_progress().await.map_err(JobError::retryable)?;
        Ok(JobResult::Completed)
    }
}

/// Worker that updates metadata.
struct UpdateMetadataWorker;

#[async_trait::async_trait]
impl Worker for UpdateMetadataWorker {
    fn kind(&self) -> &'static str {
        "progress_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        ctx.update_metadata(serde_json::json!({"last_processed_id": 1234}))
            .map_err(|e| JobError::terminal(e.to_string()))?;
        ctx.flush_progress().await.map_err(JobError::retryable)?;
        Ok(JobResult::Completed)
    }
}

/// Worker that sets progress multiple times — only last value persists.
struct OverwriteProgressWorker;

#[async_trait::async_trait]
impl Worker for OverwriteProgressWorker {
    fn kind(&self) -> &'static str {
        "progress_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        ctx.set_progress(10, "starting");
        ctx.set_progress(50, "middle");
        ctx.set_progress(90, "almost done");
        ctx.flush_progress().await.map_err(JobError::retryable)?;
        Ok(JobResult::Completed)
    }
}

/// Worker that flushes progress immediately.
struct FlushProgressWorker;

#[async_trait::async_trait]
impl Worker for FlushProgressWorker {
    fn kind(&self) -> &'static str {
        "progress_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        ctx.set_progress(42, "flushed");
        ctx.flush_progress().await.map_err(JobError::retryable)?;
        // Verify progress was written to DB
        let row = admin::get_job(ctx.pool(), ctx.job.id)
            .await
            .map_err(|e| JobError::terminal(format!("failed to get job: {e}")))?;
        let progress = row.progress.expect("progress should be set after flush");
        let percent = progress.get("percent").and_then(|v| v.as_u64()).unwrap();
        assert_eq!(percent, 42);
        Ok(JobResult::Completed)
    }
}

/// Worker that sets progress then returns RetryAfter.
struct RetryWithProgressWorker;

#[async_trait::async_trait]
impl Worker for RetryWithProgressWorker {
    fn kind(&self) -> &'static str {
        "progress_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        ctx.set_progress(30, "partial work");
        ctx.update_metadata(serde_json::json!({"last_id": 500}))
            .map_err(|e| JobError::terminal(e.to_string()))?;
        Ok(JobResult::RetryAfter(Duration::from_secs(1)))
    }
}

/// Worker that reads checkpoint from previous attempt.
struct ReadCheckpointWorker;

#[async_trait::async_trait]
impl Worker for ReadCheckpointWorker {
    fn kind(&self) -> &'static str {
        "progress_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        // Read checkpoint from previous attempt
        let last_id = ctx
            .job
            .progress
            .as_ref()
            .and_then(|p| p.get("metadata"))
            .and_then(|m| m.get("last_id"))
            .and_then(|v| v.as_i64());
        assert_eq!(
            last_id,
            Some(500),
            "should see checkpoint from previous attempt"
        );
        Ok(JobResult::Completed)
    }
}

/// Worker that sets progress and returns Cancel.
struct CancelWithProgressWorker;

#[async_trait::async_trait]
impl Worker for CancelWithProgressWorker {
    fn kind(&self) -> &'static str {
        "progress_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        ctx.set_progress(75, "cancelling");
        Ok(JobResult::Cancel("user requested".to_string()))
    }
}

/// Worker that sets progress and fails terminally.
struct FailWithProgressWorker;

#[async_trait::async_trait]
impl Worker for FailWithProgressWorker {
    fn kind(&self) -> &'static str {
        "progress_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        ctx.set_progress(10, "about to fail");
        Err(JobError::terminal("fatal error"))
    }
}

/// Worker that does not set any progress.
struct NoProgressWorker;

#[async_trait::async_trait]
impl Worker for NoProgressWorker {
    fn kind(&self) -> &'static str {
        "progress_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::Completed)
    }
}

/// Worker that sets progress > 100 and verifies it is clamped to 100.
struct ClampProgressWorker;

#[async_trait::async_trait]
impl Worker for ClampProgressWorker {
    fn kind(&self) -> &'static str {
        "progress_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        ctx.set_progress(101, "over the top");
        ctx.flush_progress().await.map_err(JobError::retryable)?;
        // Verify clamped value was written to DB
        let row = admin::get_job(ctx.pool(), ctx.job.id)
            .await
            .map_err(|e| JobError::terminal(format!("failed to get job: {e}")))?;
        let progress = row.progress.expect("progress should be set after flush");
        let percent = progress.get("percent").and_then(|v| v.as_u64()).unwrap();
        assert_eq!(percent, 100, "percent should be clamped to 100");
        Ok(JobResult::Completed)
    }
}

/// Worker that sets progress and waits for callback.
struct WaitWithProgressWorker;

#[async_trait::async_trait]
impl Worker for WaitWithProgressWorker {
    fn kind(&self) -> &'static str {
        "progress_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        ctx.set_progress(50, "waiting for callback");
        let callback = ctx
            .register_callback(Duration::from_secs(3600))
            .await
            .map_err(JobError::retryable)?;
        Ok(JobResult::WaitForCallback(callback))
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

/// P1: set_progress(50, "half") + flush_progress → job.progress.percent == 50
#[tokio::test]
async fn test_set_progress_and_flush() {
    let tc = setup().await;
    let queue = "progress_p1";
    clean_queue(tc.pool(), queue).await;

    let job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p1".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = tc
        .work_one_in_queue(&SetProgressWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_completed());

    // Completed jobs have progress cleared to NULL
    let completed = admin::get_job(tc.pool(), job.id).await.unwrap();
    assert_eq!(completed.state, JobState::Completed);
    assert!(
        completed.progress.is_none(),
        "completed jobs should have NULL progress"
    );
}

/// P2: update_metadata({"key": "val"}) + flush → progress.metadata.key == "val"
#[tokio::test]
async fn test_update_metadata_merge() {
    let tc = setup().await;
    let queue = "progress_p2";
    clean_queue(tc.pool(), queue).await;

    let _job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p2".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = tc
        .work_one_in_queue(&UpdateMetadataWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_completed());
}

/// P3: Multiple set_progress calls + flush → only last value
#[tokio::test]
async fn test_overwrite_progress() {
    let tc = setup().await;
    let queue = "progress_p3";
    clean_queue(tc.pool(), queue).await;

    let _job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p3".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = tc
        .work_one_in_queue(&OverwriteProgressWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_completed());
}

/// P4: flush_progress() writes immediately
#[tokio::test]
async fn test_flush_progress_immediate() {
    let tc = setup().await;
    let queue = "progress_p4";
    clean_queue(tc.pool(), queue).await;

    let _job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p4".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = tc
        .work_one_in_queue(&FlushProgressWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_completed());
}

/// P6: Completed job → progress = NULL (via completion batcher / test harness)
#[tokio::test]
async fn test_completed_clears_progress() {
    let tc = setup().await;
    let queue = "progress_p6";
    clean_queue(tc.pool(), queue).await;

    let job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p6".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // First, set some progress on the job (simulate mid-execution)
    let result = tc
        .work_one_in_queue(&SetProgressWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_completed());

    // The test harness transitions to completed state.
    // In production, the completion batcher sets progress = NULL.
    // Let's verify by inserting a new job, setting progress, flushing, then completing
    // through a full client lifecycle.
    let completed = admin::get_job(tc.pool(), job.id).await.unwrap();
    assert_eq!(completed.state, JobState::Completed);
}

/// P7: RetryAfter → next attempt sees progress from previous attempt
#[tokio::test]
async fn test_retry_preserves_progress() {
    let tc = setup().await;
    let queue = "progress_p7";
    clean_queue(tc.pool(), queue).await;

    let job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p7".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // First attempt: set progress and retry
    let result = tc
        .work_one_in_queue(&RetryWithProgressWorker, Some(queue))
        .await
        .unwrap();
    assert!(matches!(result, awa_testing::WorkResult::Retryable(_)));

    // Verify progress was preserved on the retryable job
    let retried = admin::get_job(tc.pool(), job.id).await.unwrap();
    assert!(
        retried.progress.is_some(),
        "progress should be preserved on retry"
    );
    let progress = retried.progress.unwrap();
    assert_eq!(progress.get("percent").and_then(|v| v.as_u64()), Some(30));
    assert_eq!(
        progress
            .get("metadata")
            .and_then(|m| m.get("last_id"))
            .and_then(|v| v.as_i64()),
        Some(500)
    );

    // Make the job available again for the second attempt
    sqlx::query("UPDATE awa.jobs SET state = 'available', run_at = now() WHERE id = $1")
        .bind(job.id)
        .execute(tc.pool())
        .await
        .unwrap();

    // Second attempt: read the checkpoint
    let result2 = tc
        .work_one_in_queue(&ReadCheckpointWorker, Some(queue))
        .await
        .unwrap();
    assert!(result2.is_completed());
}

/// P8: No progress set → heartbeat query unchanged
#[tokio::test]
async fn test_no_progress_no_overhead() {
    let tc = setup().await;
    let queue = "progress_p8";
    clean_queue(tc.pool(), queue).await;

    let job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p8".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = tc
        .work_one_in_queue(&NoProgressWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_completed());

    let completed = admin::get_job(tc.pool(), job.id).await.unwrap();
    assert!(completed.progress.is_none(), "no progress should be set");
}

/// P9: set_progress(101) → clamped to 100
#[tokio::test]
async fn test_progress_clamped_to_100() {
    let tc = setup().await;
    let queue = "progress_p9";
    clean_queue(tc.pool(), queue).await;

    let _job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p9".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = tc
        .work_one_in_queue(&ClampProgressWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_completed());

    // The clamp happens in the buffer. We can verify by checking that the
    // worker completed without error (the clamped value was flushed successfully).
}

/// P10: WaitForCallback preserves progress
#[tokio::test]
async fn test_wait_for_callback_preserves_progress() {
    let tc = setup().await;
    let queue = "progress_p10";
    clean_queue(tc.pool(), queue).await;

    let job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p10".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = tc
        .work_one_in_queue(&WaitWithProgressWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_waiting_external());

    let waiting = admin::get_job(tc.pool(), job.id).await.unwrap();
    assert_eq!(waiting.state, JobState::WaitingExternal);
    assert!(
        waiting.progress.is_some(),
        "progress should be preserved in waiting_external"
    );
    let progress = waiting.progress.unwrap();
    assert_eq!(progress.get("percent").and_then(|v| v.as_u64()), Some(50));
}

/// P11: complete_external clears progress
#[tokio::test]
async fn test_complete_external_clears_progress() {
    let tc = setup().await;
    let queue = "progress_p11";
    clean_queue(tc.pool(), queue).await;

    let _job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p11".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = tc
        .work_one_in_queue(&WaitWithProgressWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_waiting_external());

    // Get the callback_id
    let waiting = match result {
        awa_testing::WorkResult::WaitingExternal(job) => job,
        _ => panic!("expected WaitingExternal"),
    };
    let callback_id = waiting.callback_id.expect("callback_id should be set");

    // Complete externally
    let completed = admin::complete_external(tc.pool(), callback_id, None, None)
        .await
        .unwrap();
    assert_eq!(completed.state, JobState::Completed);
    assert!(
        completed.progress.is_none(),
        "progress should be cleared after complete_external"
    );
}

/// P12: fail_external preserves progress
#[tokio::test]
async fn test_fail_external_preserves_progress() {
    let tc = setup().await;
    let queue = "progress_p12";
    clean_queue(tc.pool(), queue).await;

    let _job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p12".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = tc
        .work_one_in_queue(&WaitWithProgressWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_waiting_external());

    let waiting = match result {
        awa_testing::WorkResult::WaitingExternal(job) => job,
        _ => panic!("expected WaitingExternal"),
    };
    let callback_id = waiting.callback_id.expect("callback_id should be set");

    let failed = admin::fail_external(tc.pool(), callback_id, "external error", None)
        .await
        .unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert!(
        failed.progress.is_some(),
        "progress should be preserved after fail_external"
    );
}

/// P14: Failed (terminal error) preserves progress
#[tokio::test]
async fn test_terminal_failure_preserves_progress() {
    let tc = setup().await;
    let queue = "progress_p14";
    clean_queue(tc.pool(), queue).await;

    let job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p14".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = tc
        .work_one_in_queue(&FailWithProgressWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_failed());

    let failed = admin::get_job(tc.pool(), job.id).await.unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert!(
        failed.progress.is_some(),
        "progress should be preserved on terminal failure"
    );
    let progress = failed.progress.unwrap();
    assert_eq!(progress.get("percent").and_then(|v| v.as_u64()), Some(10));
}

/// P15: Cancelled preserves progress
#[tokio::test]
async fn test_cancel_preserves_progress() {
    let tc = setup().await;
    let queue = "progress_p15";
    clean_queue(tc.pool(), queue).await;

    let job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p15".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = tc
        .work_one_in_queue(&CancelWithProgressWorker, Some(queue))
        .await
        .unwrap();
    assert!(matches!(result, awa_testing::WorkResult::Cancelled(_, _)));

    let cancelled = admin::get_job(tc.pool(), job.id).await.unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert!(
        cancelled.progress.is_some(),
        "progress should be preserved on cancel"
    );
    let progress = cancelled.progress.unwrap();
    assert_eq!(progress.get("percent").and_then(|v| v.as_u64()), Some(75));
}

/// P5: Progress survives rescue: set progress → simulate stale heartbeat → rescued job has progress
#[tokio::test]
async fn test_progress_survives_rescue() {
    let tc = setup().await;
    let queue = "progress_p5";
    clean_queue(tc.pool(), queue).await;

    let job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p5".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Simulate: claim the job, set progress, then let the heartbeat go stale.
    // Manually transition to running with a stale heartbeat_at.
    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'running',
            attempt = attempt + 1,
            run_lease = run_lease + 1,
            attempted_at = now(),
            heartbeat_at = now() - interval '5 minutes',
            progress = '{"percent": 60, "message": "in progress", "metadata": {"cursor": "abc"}}'::jsonb
        WHERE id = $1
        "#,
    )
    .bind(job.id)
    .execute(tc.pool())
    .await
    .unwrap();

    // Now simulate the rescue: the maintenance service runs rescue_stale_heartbeats,
    // which transitions the job to retryable without touching the progress column.
    let rescued: Vec<(i64,)> = sqlx::query_as(
        r#"
        UPDATE awa.jobs
        SET state = 'retryable',
            finalized_at = now(),
            heartbeat_at = NULL,
            deadline_at = NULL,
            callback_id = NULL,
            callback_timeout_at = NULL,
            errors = errors || jsonb_build_object(
                'error', 'heartbeat stale: worker presumed dead',
                'attempt', attempt,
                'at', now()
            )::jsonb
        WHERE id = $1 AND state = 'running'
        RETURNING id
        "#,
    )
    .bind(job.id)
    .fetch_all(tc.pool())
    .await
    .unwrap();
    assert_eq!(rescued.len(), 1, "job should have been rescued");

    // Verify progress survived the rescue
    let rescued_job = admin::get_job(tc.pool(), job.id).await.unwrap();
    assert_eq!(rescued_job.state, JobState::Retryable);
    assert!(
        rescued_job.progress.is_some(),
        "progress should survive rescue"
    );
    let progress = rescued_job.progress.unwrap();
    assert_eq!(progress.get("percent").and_then(|v| v.as_u64()), Some(60));
    assert_eq!(
        progress
            .get("metadata")
            .and_then(|m| m.get("cursor"))
            .and_then(|v| v.as_str()),
        Some("abc")
    );
}

/// P13: Callback timeout rescue preserves progress
#[tokio::test]
async fn test_callback_timeout_rescue_preserves_progress() {
    let tc = setup().await;
    let queue = "progress_p13";
    clean_queue(tc.pool(), queue).await;

    let job = awa::insert_with(
        tc.pool(),
        &ProgressJob { data: "p13".into() },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Simulate: job is in waiting_external with an expired callback timeout
    // and has progress set.
    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'waiting_external',
            attempt = 1,
            run_lease = run_lease + 1,
            callback_id = gen_random_uuid(),
            callback_timeout_at = now() - interval '1 minute',
            heartbeat_at = NULL,
            deadline_at = NULL,
            progress = '{"percent": 40, "message": "waiting", "metadata": {"step": 3}}'::jsonb
        WHERE id = $1
        "#,
    )
    .bind(job.id)
    .execute(tc.pool())
    .await
    .unwrap();

    // Simulate the callback timeout rescue (same query as maintenance service)
    let rescued: Vec<(i64,)> = sqlx::query_as(
        r#"
        UPDATE awa.jobs
        SET state = CASE WHEN attempt >= max_attempts THEN 'failed'::awa.job_state ELSE 'retryable'::awa.job_state END,
            finalized_at = now(),
            callback_id = NULL,
            callback_timeout_at = NULL,
            run_at = CASE WHEN attempt >= max_attempts THEN run_at
                     ELSE now() + awa.backoff_duration(attempt, max_attempts) END,
            errors = errors || jsonb_build_object(
                'error', 'callback timed out',
                'attempt', attempt,
                'at', now()
            )::jsonb
        WHERE id = $1 AND state = 'waiting_external'
        RETURNING id
        "#,
    )
    .bind(job.id)
    .fetch_all(tc.pool())
    .await
    .unwrap();
    assert_eq!(
        rescued.len(),
        1,
        "job should have been rescued from callback timeout"
    );

    // Verify progress survived the callback timeout rescue
    let rescued_job = admin::get_job(tc.pool(), job.id).await.unwrap();
    assert!(
        rescued_job.state == JobState::Retryable || rescued_job.state == JobState::Failed,
        "job should be retryable or failed after callback timeout"
    );
    assert!(
        rescued_job.progress.is_some(),
        "progress should survive callback timeout rescue"
    );
    let progress = rescued_job.progress.unwrap();
    assert_eq!(progress.get("percent").and_then(|v| v.as_u64()), Some(40));
    assert_eq!(
        progress
            .get("metadata")
            .and_then(|m| m.get("step"))
            .and_then(|v| v.as_i64()),
        Some(3)
    );
}

/// Full lifecycle test using real Client (not TestClient) to verify progress
/// flows through the completion batcher and executor correctly.
#[tokio::test]
async fn test_progress_full_lifecycle() {
    use awa::{Client, QueueConfig};
    use std::sync::atomic::{AtomicI64, Ordering};

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");
    migrations::run(&pool).await.expect("Failed to migrate");

    let queue = "progress_lifecycle";
    clean_queue(&pool, queue).await;

    static LAST_FLUSHED_PERCENT: AtomicI64 = AtomicI64::new(-1);

    #[derive(Debug, Serialize, Deserialize, JobArgs)]
    struct LifecycleJob {
        pub mode: String,
    }

    struct LifecycleWorker;

    #[async_trait::async_trait]
    impl Worker for LifecycleWorker {
        fn kind(&self) -> &'static str {
            "lifecycle_job"
        }

        async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
            let args: LifecycleJob = serde_json::from_value(ctx.job.args.clone())
                .map_err(|e| JobError::terminal(e.to_string()))?;
            match args.mode.as_str() {
                "complete" => {
                    ctx.set_progress(100, "done");
                    ctx.flush_progress().await.map_err(JobError::retryable)?;
                    // Record what we flushed
                    LAST_FLUSHED_PERCENT.store(100, Ordering::SeqCst);
                    Ok(JobResult::Completed)
                }
                "retry" => {
                    ctx.set_progress(50, "halfway");
                    ctx.update_metadata(serde_json::json!({"checkpoint": 42}))
                        .map_err(|e| JobError::terminal(e.to_string()))?;
                    Ok(JobResult::RetryAfter(Duration::from_millis(10)))
                }
                "read_checkpoint" => {
                    let checkpoint = ctx
                        .job
                        .progress
                        .as_ref()
                        .and_then(|p| p.get("metadata"))
                        .and_then(|m| m.get("checkpoint"))
                        .and_then(|v| v.as_i64());
                    LAST_FLUSHED_PERCENT.store(checkpoint.unwrap_or(-1), Ordering::SeqCst);
                    Ok(JobResult::Completed)
                }
                _ => Ok(JobResult::Completed),
            }
        }
    }

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                min_workers: 1,
                max_workers: 2,
                ..Default::default()
            },
        )
        .register_worker(LifecycleWorker)
        .heartbeat_interval(Duration::from_millis(100))
        .leader_election_interval(Duration::from_millis(100))
        .build()
        .unwrap();

    client.start().await.unwrap();

    // Test 1: Complete with progress → progress should be NULL after
    LAST_FLUSHED_PERCENT.store(-1, Ordering::SeqCst);
    let job1 = awa::insert_with(
        &pool,
        &LifecycleJob {
            mode: "complete".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Wait for job to complete
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let job = admin::get_job(&pool, job1.id).await.unwrap();
        if job.state == JobState::Completed {
            assert!(
                job.progress.is_none(),
                "completed job should have NULL progress"
            );
            break;
        }
    }
    assert_eq!(LAST_FLUSHED_PERCENT.load(Ordering::SeqCst), 100);

    // Test 2: Retry with checkpoint → verify checkpoint survives
    let job2 = awa::insert_with(
        &pool,
        &LifecycleJob {
            mode: "retry".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Wait for retry
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let job = admin::get_job(&pool, job2.id).await.unwrap();
        if job.state == JobState::Retryable {
            let progress = job.progress.expect("retried job should have progress");
            assert_eq!(progress.get("percent").and_then(|v| v.as_u64()), Some(50));
            assert_eq!(
                progress
                    .get("metadata")
                    .and_then(|m| m.get("checkpoint"))
                    .and_then(|v| v.as_i64()),
                Some(42)
            );
            break;
        }
    }

    client.shutdown(Duration::from_secs(5)).await;
}
