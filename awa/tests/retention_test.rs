//! Integration tests for configurable retention policies and cleanup.
//!
//! These tests must run sequentially because each starts a Client that
//! becomes leader and runs global cleanup — concurrent leaders with
//! different retention policies would interfere with each other's test data.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{insert_with, InsertOpts};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, RetentionPolicy, Worker};
use awa_testing::TestClient;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::sync::Mutex;

/// Serialize retention tests — each starts a Client that becomes leader and
/// runs global cleanup, so concurrent tests interfere with each other.
static RETENTION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue meta");
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct RetentionTestJob {
    pub value: String,
}

struct RetentionTestWorker;

#[async_trait::async_trait]
impl Worker for RetentionTestWorker {
    fn kind(&self) -> &'static str {
        "retention_test_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::Completed)
    }
}

/// Helper to insert a job and immediately set it to a terminal state
/// with a backdated finalized_at timestamp.
async fn insert_terminal_job(pool: &sqlx::PgPool, queue: &str, state: &str, age_secs: i64) -> i64 {
    let job = insert_with(
        pool,
        &RetentionTestJob {
            value: format!("{state}_job"),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .expect("Failed to insert job");

    // Move to terminal state with backdated finalized_at
    sqlx::query(&format!(
        "UPDATE awa.jobs SET state = '{state}'::awa.job_state, finalized_at = now() - interval '{age_secs} seconds' WHERE id = $1"
    ))
    .bind(job.id)
    .execute(pool)
    .await
    .expect("Failed to update job state");

    job.id
}

/// Verify that a job exists in the database.
async fn job_exists(pool: &sqlx::PgPool, job_id: i64) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM awa.jobs WHERE id = $1)")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .unwrap_or(false)
}

#[tokio::test]
async fn test_cleanup_respects_completed_retention() {
    let _guard = RETENTION_LOCK.lock().await;
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "retention_completed";
    clean_queue(pool, queue).await;

    // Insert a completed job older than 24h (default retention)
    let old_job_id = insert_terminal_job(pool, queue, "completed", 90_000).await;

    // Start a client with default retention, trigger cleanup, then shut down
    let client = Client::builder(pool.clone())
        .queue(queue, QueueConfig::default())
        .register_worker(RetentionTestWorker)
        .leader_election_interval(Duration::from_millis(100))
        .cleanup_interval(Duration::from_secs(1))
        .build()
        .unwrap();
    client.start().await.unwrap();

    // Give the maintenance leader time to run a cleanup cycle
    tokio::time::sleep(Duration::from_secs(3)).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert!(
        !job_exists(pool, old_job_id).await,
        "Old completed job should have been cleaned up"
    );
}

#[tokio::test]
async fn test_cleanup_preserves_recent_jobs() {
    let _guard = RETENTION_LOCK.lock().await;
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "retention_recent";
    clean_queue(pool, queue).await;

    // Insert a completed job that's only 1 hour old (within 24h default retention)
    let recent_job_id = insert_terminal_job(pool, queue, "completed", 3_600).await;

    let client = Client::builder(pool.clone())
        .queue(queue, QueueConfig::default())
        .register_worker(RetentionTestWorker)
        .leader_election_interval(Duration::from_millis(100))
        .cleanup_interval(Duration::from_secs(1))
        .build()
        .unwrap();
    client.start().await.unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert!(
        job_exists(pool, recent_job_id).await,
        "Recent completed job should NOT have been cleaned up"
    );
}

#[tokio::test]
async fn test_cleanup_batch_size_accepted() {
    let _guard = RETENTION_LOCK.lock().await;
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "retention_batch";
    clean_queue(pool, queue).await;

    // Insert an old completed job
    let old_job_id = insert_terminal_job(pool, queue, "completed", 90_000).await;

    // Verify that a custom batch_size is accepted and cleanup still works.
    // (Exact batch limiting is hard to assert in parallel CI — the advisory lock
    // means any concurrent maintenance leader may clean this queue.)
    let client = Client::builder(pool.clone())
        .queue(queue, QueueConfig::default())
        .register_worker(RetentionTestWorker)
        .leader_election_interval(Duration::from_millis(100))
        .cleanup_batch_size(2)
        .cleanup_interval(Duration::from_secs(1))
        .build()
        .unwrap();
    client.start().await.unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;
    client.shutdown(Duration::from_secs(2)).await;

    // The old job should be cleaned up (by this or another test's leader)
    assert!(
        !job_exists(pool, old_job_id).await,
        "Old completed job should have been cleaned up with custom batch_size"
    );
}

#[tokio::test]
async fn test_per_queue_retention_override() {
    let _guard = RETENTION_LOCK.lock().await;
    let test_client = setup().await;
    let pool = test_client.pool();
    let fast_queue = "retention_fast";
    let slow_queue = "retention_slow";
    clean_queue(pool, fast_queue).await;
    clean_queue(pool, slow_queue).await;

    // Insert a 2-hour-old completed job in each queue
    let fast_job_id = insert_terminal_job(pool, fast_queue, "completed", 7_200).await;
    let slow_job_id = insert_terminal_job(pool, slow_queue, "completed", 7_200).await;

    // fast_queue: 1h retention (job should be deleted)
    // slow_queue: uses default 24h retention (job should survive)
    let client = Client::builder(pool.clone())
        .queue(fast_queue, QueueConfig::default())
        .queue(slow_queue, QueueConfig::default())
        .register_worker(RetentionTestWorker)
        .leader_election_interval(Duration::from_millis(100))
        .cleanup_interval(Duration::from_secs(1))
        .queue_retention(
            fast_queue,
            RetentionPolicy {
                completed: Duration::from_secs(3600), // 1h
                failed: Duration::from_secs(3600),
                dlq: None,
            },
        )
        .build()
        .unwrap();
    client.start().await.unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert!(
        !job_exists(pool, fast_job_id).await,
        "Job in fast-retention queue should have been cleaned up"
    );
    assert!(
        job_exists(pool, slow_job_id).await,
        "Job in default-retention queue should NOT have been cleaned up"
    );
}

#[tokio::test]
async fn test_cleanup_targets_jobs_hot_directly() {
    let _guard = RETENTION_LOCK.lock().await;
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "retention_hot_target";
    clean_queue(pool, queue).await;

    // Insert an old completed job
    let job_id = insert_terminal_job(pool, queue, "completed", 90_000).await;

    // Verify the job is in jobs_hot (completed jobs live there, not in scheduled_jobs)
    let in_hot: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM awa.jobs_hot WHERE id = $1)")
            .bind(job_id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert!(in_hot, "Completed job should be in jobs_hot");

    let client = Client::builder(pool.clone())
        .queue(queue, QueueConfig::default())
        .register_worker(RetentionTestWorker)
        .leader_election_interval(Duration::from_millis(100))
        .cleanup_interval(Duration::from_secs(1))
        .build()
        .unwrap();
    client.start().await.unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;
    client.shutdown(Duration::from_secs(2)).await;

    // Verify it was deleted from jobs_hot directly
    let still_in_hot: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM awa.jobs_hot WHERE id = $1)")
            .bind(job_id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert!(
        !still_in_hot,
        "Old completed job should have been deleted from jobs_hot"
    );
}
