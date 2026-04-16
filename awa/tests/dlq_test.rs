//! Integration tests for the Dead Letter Queue (DLQ).
//!
//! Covers: SQL-level atomicity of `move_to_dlq_guarded`, stale-lease rejection,
//! retry-from-DLQ re-enqueue, bulk admin moves, purge, and the executor-level
//! routing decision when a queue has DLQ enabled.

use awa::model::dlq::{self, ListDlqFilter, RetryFromDlqOpts};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
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
    sqlx::query("DELETE FROM awa.jobs_dlq WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean DLQ");
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean jobs");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue_meta");
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct DlqTestJob {
    pub value: String,
}

struct AlwaysFailTerminalWorker;

#[async_trait::async_trait]
impl Worker for AlwaysFailTerminalWorker {
    fn kind(&self) -> &'static str {
        "dlq_test_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Err(JobError::Terminal("boom".into()))
    }
}

/// Guarded move: a running job finalised with a matching lease lands in DLQ
/// and leaves `jobs_hot` empty for that job id.
#[tokio::test]
async fn test_move_to_dlq_guarded_happy_path() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_guarded_happy";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "hello".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Simulate claim: transition to running and bump run_lease.
    sqlx::query(
        r#"UPDATE awa.jobs_hot SET state='running', run_lease=42, attempt=1,
               attempted_at=now(), heartbeat_at=now() WHERE id=$1"#,
    )
    .bind(job.id)
    .execute(pool)
    .await
    .unwrap();

    let dlq_row: Option<awa::DlqRow> = sqlx::query_as(
        "SELECT * FROM awa.move_to_dlq_guarded($1, $2, $3, $4::jsonb)",
    )
    .bind(job.id)
    .bind(42_i64)
    .bind("terminal_error")
    .bind(serde_json::json!({"error": "boom", "attempt": 1, "terminal": true}))
    .fetch_optional(pool)
    .await
    .unwrap();

    let dlq_row = dlq_row.expect("DLQ move should have succeeded");
    assert_eq!(dlq_row.id, job.id);
    assert_eq!(dlq_row.dlq_reason, "terminal_error");
    assert_eq!(dlq_row.original_run_lease, 42);

    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM awa.jobs_hot WHERE id = $1")
            .bind(job.id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(remaining, 0, "jobs_hot should no longer hold the job");
}

/// Guarded move: stale lease (the row has been re-claimed) results in 0 rows
/// affected and the job row is untouched.
#[tokio::test]
async fn test_move_to_dlq_guarded_rejects_stale_lease() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_guarded_stale";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "stale".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    sqlx::query(
        r#"UPDATE awa.jobs_hot SET state='running', run_lease=7 WHERE id=$1"#,
    )
    .bind(job.id)
    .execute(pool)
    .await
    .unwrap();

    // Call with the wrong lease (simulating a rescue that bumped it in the meantime)
    let dlq_row: Option<awa::DlqRow> = sqlx::query_as(
        "SELECT * FROM awa.move_to_dlq_guarded($1, $2, $3, $4::jsonb)",
    )
    .bind(job.id)
    .bind(99_i64) // wrong
    .bind("should_not_move")
    .bind(serde_json::json!({}))
    .fetch_optional(pool)
    .await
    .unwrap();

    assert!(dlq_row.is_none(), "stale lease should reject the move");

    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM awa.jobs_hot WHERE id = $1")
            .bind(job.id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(
        remaining, 1,
        "jobs_hot row must still exist after stale rejection"
    );
}

/// Admin bulk-move: failed jobs in jobs_hot can be relocated into the DLQ.
#[tokio::test]
async fn test_bulk_move_failed_to_dlq() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_bulk_move";
    clean_queue(pool, queue).await;

    for i in 0..3 {
        let job = awa::model::insert_with(
            pool,
            &DlqTestJob {
                value: format!("j{i}"),
            },
            awa::InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        sqlx::query("UPDATE awa.jobs_hot SET state='failed', finalized_at=now() WHERE id=$1")
            .bind(job.id)
            .execute(pool)
            .await
            .unwrap();
    }

    let moved =
        dlq::bulk_move_failed_to_dlq(pool, None, Some(queue), "ops_archived")
            .await
            .unwrap();
    assert_eq!(moved, 3);

    let depth = dlq::dlq_depth(pool, Some(queue)).await.unwrap();
    assert_eq!(depth, 3);

    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM awa.jobs_hot WHERE queue=$1 AND state='failed'",
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(remaining, 0);
}

/// Retry from DLQ: a DLQ'd job re-enters jobs_hot as available with attempt=0.
#[tokio::test]
async fn test_retry_from_dlq_revives_job() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_retry";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "revive".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE awa.jobs_hot SET state='failed', attempt=5, finalized_at=now() WHERE id=$1")
        .bind(job.id)
        .execute(pool)
        .await
        .unwrap();
    dlq::move_failed_to_dlq(pool, job.id, "reason").await.unwrap();

    let opts = RetryFromDlqOpts::default();
    let revived = dlq::retry_from_dlq(pool, job.id, &opts)
        .await
        .unwrap()
        .expect("retry should return the revived row");
    assert_eq!(revived.id, job.id);
    assert_eq!(revived.attempt, 0);
    assert_eq!(revived.run_lease, 0);
    assert_eq!(revived.state.to_string(), "available");

    let depth_after = dlq::dlq_depth(pool, Some(queue)).await.unwrap();
    assert_eq!(depth_after, 0);
}

/// Retry from DLQ with a future run_at schedules the job instead of making it available.
#[tokio::test]
async fn test_retry_from_dlq_with_future_run_at_schedules() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_retry_scheduled";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "later".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE awa.jobs_hot SET state='failed', finalized_at=now() WHERE id=$1")
        .bind(job.id)
        .execute(pool)
        .await
        .unwrap();
    dlq::move_failed_to_dlq(pool, job.id, "will_retry").await.unwrap();

    let opts = RetryFromDlqOpts {
        run_at: Some(chrono::Utc::now() + chrono::Duration::minutes(10)),
        ..Default::default()
    };
    let revived = dlq::retry_from_dlq(pool, job.id, &opts)
        .await
        .unwrap()
        .expect("retry should return the revived row");
    assert_eq!(revived.state.to_string(), "scheduled");
}

/// Purge DLQ removes matching rows.
#[tokio::test]
async fn test_purge_dlq() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_purge";
    clean_queue(pool, queue).await;

    for i in 0..4 {
        let job = awa::model::insert_with(
            pool,
            &DlqTestJob {
                value: format!("p{i}"),
            },
            awa::InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        sqlx::query("UPDATE awa.jobs_hot SET state='failed', finalized_at=now() WHERE id=$1")
            .bind(job.id)
            .execute(pool)
            .await
            .unwrap();
    }
    dlq::bulk_move_failed_to_dlq(pool, None, Some(queue), "t")
        .await
        .unwrap();
    assert_eq!(dlq::dlq_depth(pool, Some(queue)).await.unwrap(), 4);

    let deleted = dlq::purge_dlq(
        pool,
        &ListDlqFilter {
            queue: Some(queue.into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(deleted, 4);
    assert_eq!(dlq::dlq_depth(pool, Some(queue)).await.unwrap(), 0);
}

/// End-to-end: a queue with DLQ enabled routes a terminal failure through the
/// executor into jobs_dlq instead of leaving the row in jobs_hot.
#[tokio::test]
async fn test_executor_routes_terminal_failure_to_dlq_when_enabled() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_executor_enabled";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "end2end".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            max_attempts: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let client = Client::builder(pool.clone())
        .queue(queue, QueueConfig::default())
        .register_worker(AlwaysFailTerminalWorker)
        .dlq_enabled_by_default(true)
        .leader_election_interval(Duration::from_millis(100))
        .cleanup_interval(Duration::from_secs(3600))
        .heartbeat_interval(Duration::from_secs(60))
        .build()
        .unwrap();
    client.start().await.unwrap();
    // Give the dispatcher time to claim and execute
    tokio::time::sleep(Duration::from_secs(2)).await;
    client.shutdown(Duration::from_secs(3)).await;

    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM awa.jobs_hot WHERE id=$1")
            .bind(job.id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(remaining, 0, "terminal failure should leave jobs_hot empty");

    let dlq_row = dlq::get_dlq_job(pool, job.id).await.unwrap();
    assert!(
        dlq_row.is_some(),
        "terminal failure should land in DLQ when enabled"
    );
    let dlq_row = dlq_row.unwrap();
    assert_eq!(dlq_row.dlq_reason, "terminal_error");
    assert!(dlq_row.errors.as_ref().map(|e| !e.is_empty()).unwrap_or(false));
}

/// Inverse: without DLQ enabled, a terminal failure stays in jobs_hot as before.
#[tokio::test]
async fn test_executor_keeps_terminal_failure_in_hot_when_disabled() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_executor_disabled";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "legacy".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            max_attempts: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let client = Client::builder(pool.clone())
        .queue(queue, QueueConfig::default())
        .register_worker(AlwaysFailTerminalWorker)
        .leader_election_interval(Duration::from_millis(100))
        .cleanup_interval(Duration::from_secs(3600))
        .heartbeat_interval(Duration::from_secs(60))
        .build()
        .unwrap();
    client.start().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    client.shutdown(Duration::from_secs(3)).await;

    let row: Option<(String,)> =
        sqlx::query_as("SELECT state::text FROM awa.jobs_hot WHERE id=$1")
            .bind(job.id)
            .fetch_optional(pool)
            .await
            .unwrap();
    assert_eq!(row.map(|r| r.0), Some("failed".to_string()));
    assert!(dlq::get_dlq_job(pool, job.id).await.unwrap().is_none());
}
