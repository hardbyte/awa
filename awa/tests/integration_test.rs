//! Integration tests for Awa — requires a running Postgres instance.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{admin, insert_many, insert_with, migrations, InsertOpts, UniqueOpts};
use awa::{
    AwaError, BuildError, Client, JobArgs, JobContext, JobError, JobResult, JobState, QueueConfig,
    RateLimit, Worker,
};
use awa_testing::{TestClient, WorkResult};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup_with_connections(max_connections: u32) -> TestClient {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");

    let client = TestClient::from_pool(pool).await;
    client.migrate().await.expect("Failed to run migrations");
    // No global cleanup — each test uses a unique queue name for isolation.
    client
}

async fn setup() -> TestClient {
    setup_with_connections(2).await
}

/// Clean only jobs and queue_meta for a specific queue.
/// Call this at the start of each test to remove leftovers from previous runs.
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

async fn wait_for_runtime_snapshot(
    pool: &sqlx::PgPool,
    expected_queue: &str,
) -> admin::RuntimeOverview {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let overview = admin::runtime_overview(pool).await.unwrap();
        if overview.instances.iter().any(|instance| {
            instance
                .queues
                .iter()
                .any(|queue| queue.queue == expected_queue)
        }) {
            return overview;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for runtime snapshot for queue {expected_queue}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// -- Job types for testing --

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SendEmail {
    pub to: String,
    pub subject: String,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct ProcessPayment {
    pub order_id: i64,
    pub amount_cents: i64,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
#[awa(kind = "custom_job_kind")]
struct CustomKindJob {
    pub data: String,
}

// -- Worker implementations --

struct SendEmailWorker;

#[async_trait::async_trait]
impl Worker for SendEmailWorker {
    fn kind(&self) -> &'static str {
        "send_email"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: SendEmail = serde_json::from_value(ctx.job.args.clone())
            .map_err(|e| JobError::Terminal(e.to_string()))?;
        assert!(!args.to.is_empty());
        Ok(JobResult::Completed)
    }
}

struct FailingWorker;

#[async_trait::async_trait]
impl Worker for FailingWorker {
    fn kind(&self) -> &'static str {
        "process_payment"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Err(JobError::retryable(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "payment gateway unavailable",
        )))
    }
}

// -- Tests --

#[tokio::test]
async fn test_migrations() {
    let client = setup().await;
    let version = migrations::current_version(client.pool()).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);
}

#[tokio::test]
async fn test_insert_and_retrieve() {
    let client = setup().await;
    let queue = "integ_insert_and_retrieve";
    clean_queue(client.pool(), queue).await;

    let job = insert_with(
        client.pool(),
        &SendEmail {
            to: "alice@example.com".into(),
            subject: "Welcome".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(job.kind, "send_email");
    assert_eq!(job.queue, queue);
    assert_eq!(job.state, JobState::Available);
    assert_eq!(job.priority, 2);
    assert_eq!(job.attempt, 0);
    assert_eq!(job.max_attempts, 25);

    let args: SendEmail = serde_json::from_value(job.args).unwrap();
    assert_eq!(args.to, "alice@example.com");
    assert_eq!(args.subject, "Welcome");
}

#[tokio::test]
async fn test_insert_with_custom_opts() {
    let client = setup().await;
    let queue = "integ_custom_opts";
    clean_queue(client.pool(), queue).await;

    let job = insert_with(
        client.pool(),
        &SendEmail {
            to: "bob@example.com".into(),
            subject: "Alert".into(),
        },
        InsertOpts {
            queue: queue.into(),
            priority: 1,
            max_attempts: 3,
            tags: vec!["urgent".into(), "email".into()],
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(job.queue, queue);
    assert_eq!(job.priority, 1);
    assert_eq!(job.max_attempts, 3);
    assert_eq!(job.tags, vec!["urgent", "email"]);
}

#[tokio::test]
async fn test_insert_with_future_run_at() {
    let client = setup().await;
    let queue = "integ_future_run_at";
    clean_queue(client.pool(), queue).await;

    let future_time = chrono::Utc::now() + chrono::Duration::hours(1);
    let job = insert_with(
        client.pool(),
        &SendEmail {
            to: "future@example.com".into(),
            subject: "Later".into(),
        },
        InsertOpts {
            queue: queue.into(),
            run_at: Some(future_time),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(job.state, JobState::Scheduled);
}

#[tokio::test]
async fn test_insert_many() {
    let client = setup().await;
    let queue = "integ_insert_many";
    clean_queue(client.pool(), queue).await;

    let jobs_params = vec![
        awa::model::insert::params_with(
            &SendEmail {
                to: "a@b.com".into(),
                subject: "One".into(),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .unwrap(),
        awa::model::insert::params_with(
            &SendEmail {
                to: "c@d.com".into(),
                subject: "Two".into(),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .unwrap(),
    ];

    let jobs = insert_many(client.pool(), &jobs_params).await.unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].kind, "send_email");
    assert_eq!(jobs[1].kind, "send_email");
    assert!(jobs.iter().all(|j| j.queue == queue));
}

#[tokio::test]
async fn test_custom_kind() {
    let client = setup().await;
    let queue = "integ_custom_kind";
    clean_queue(client.pool(), queue).await;

    let job = insert_with(
        client.pool(),
        &CustomKindJob {
            data: "test".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(job.kind, "custom_job_kind");
    assert_eq!(job.queue, queue);
}

#[tokio::test]
async fn test_kind_derivation() {
    assert_eq!(SendEmail::kind(), "send_email");
    assert_eq!(ProcessPayment::kind(), "process_payment");
    assert_eq!(CustomKindJob::kind(), "custom_job_kind");
}

#[tokio::test]
async fn test_work_one_completed() {
    let client = setup().await;
    let queue = "integ_work_one_completed";
    clean_queue(client.pool(), queue).await;

    insert_with(
        client.pool(),
        &SendEmail {
            to: "worker@example.com".into(),
            subject: "Test".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let worker = SendEmailWorker;
    let result = client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();
    assert!(
        result.is_completed(),
        "Expected completed, got: {:?}",
        result
    );
}

#[tokio::test]
async fn test_work_one_retryable() {
    let client = setup().await;
    let queue = "integ_work_one_retryable";
    clean_queue(client.pool(), queue).await;

    insert_with(
        client.pool(),
        &ProcessPayment {
            order_id: 42,
            amount_cents: 9999,
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let worker = FailingWorker;
    let result = client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();
    assert!(
        matches!(result, WorkResult::Retryable(_)),
        "Expected retryable, got: {:?}",
        result
    );
}

#[tokio::test]
async fn test_work_one_no_job() {
    let client = setup().await;
    let queue = "integ_work_one_no_job";
    clean_queue(client.pool(), queue).await;

    let worker = SendEmailWorker;
    let result = client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_no_job());
}

#[tokio::test]
async fn test_admin_retry() {
    let client = setup().await;
    let queue = "integ_admin_retry";
    clean_queue(client.pool(), queue).await;

    let job = insert_with(
        client.pool(),
        &ProcessPayment {
            order_id: 1,
            amount_cents: 100,
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Set to failed directly
    sqlx::query("UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1")
        .bind(job.id)
        .execute(client.pool())
        .await
        .unwrap();

    // Retry
    let retried = admin::retry(client.pool(), job.id).await.unwrap();
    assert!(retried.is_some());

    let retried_job = client.get_job(job.id).await.unwrap();
    assert_eq!(retried_job.state, JobState::Available);
    assert_eq!(retried_job.attempt, 0);
}

#[tokio::test]
async fn test_admin_cancel() {
    let client = setup().await;
    let queue = "integ_admin_cancel";
    clean_queue(client.pool(), queue).await;

    let job = insert_with(
        client.pool(),
        &SendEmail {
            to: "cancel@example.com".into(),
            subject: "Cancel me".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    admin::cancel(client.pool(), job.id).await.unwrap();

    let cancelled = client.get_job(job.id).await.unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
}

#[tokio::test]
async fn test_admin_pause_resume_queue() {
    let client = setup().await;
    let queue = "integ_pause_resume";
    clean_queue(client.pool(), queue).await;

    admin::pause_queue(client.pool(), queue, Some("test"))
        .await
        .unwrap();

    let is_paused: bool = sqlx::query_scalar("SELECT paused FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .fetch_one(client.pool())
        .await
        .unwrap();
    assert!(is_paused);

    admin::resume_queue(client.pool(), queue).await.unwrap();

    let is_paused: bool = sqlx::query_scalar("SELECT paused FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .fetch_one(client.pool())
        .await
        .unwrap();
    assert!(!is_paused);
}

#[tokio::test]
async fn test_admin_drain_queue() {
    let client = setup().await;
    let queue = "integ_drain_queue";
    clean_queue(client.pool(), queue).await;

    for i in 0..5 {
        insert_with(
            client.pool(),
            &SendEmail {
                to: format!("drain{i}@example.com"),
                subject: "Drain test".into(),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    let drained = admin::drain_queue(client.pool(), queue).await.unwrap();
    assert_eq!(drained, 5);

    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'available'",
    )
    .bind(queue)
    .fetch_one(client.pool())
    .await
    .unwrap();
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn test_admin_queue_stats() {
    let client = setup().await;
    let queue = "integ_queue_stats";
    clean_queue(client.pool(), queue).await;

    for _ in 0..3 {
        insert_with(
            client.pool(),
            &SendEmail {
                to: "stats@example.com".into(),
                subject: "Stats test".into(),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    let stats = admin::queue_stats(client.pool()).await.unwrap();
    let stat = stats.iter().find(|s| s.queue == queue).unwrap();
    assert_eq!(stat.available, 3);
}

#[tokio::test]
async fn test_admin_list_jobs() {
    let client = setup().await;
    let queue = "integ_list_jobs";
    clean_queue(client.pool(), queue).await;

    insert_with(
        client.pool(),
        &SendEmail {
            to: "list@example.com".into(),
            subject: "List test".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let filter = admin::ListJobsFilter {
        state: Some(JobState::Available),
        queue: Some(queue.to_string()),
        ..Default::default()
    };
    let jobs = admin::list_jobs(client.pool(), &filter).await.unwrap();
    assert!(!jobs.is_empty());
    assert!(jobs.iter().all(|j| j.state == JobState::Available));
    assert!(jobs.iter().all(|j| j.queue == queue));
}

#[tokio::test]
async fn test_admin_runtime_observability_snapshot() {
    let client = setup_with_connections(8).await;
    let queue = "integ_runtime_observability";
    clean_queue(client.pool(), queue).await;

    // Clean runtime snapshots from previous runs of this test (scoped to our queue)
    sqlx::query("DELETE FROM awa.runtime_instances WHERE queues @> $1::jsonb")
        .bind(serde_json::json!([{"queue": queue}]))
        .execute(client.pool())
        .await
        .expect("Failed to clean runtime_instances for test queue");

    let runtime = Client::builder(client.pool().clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 7,
                min_workers: 2,
                weight: 3,
                rate_limit: Some(RateLimit {
                    max_rate: 12.5,
                    burst: 25,
                }),
                ..Default::default()
            },
        )
        .register_worker(SendEmailWorker)
        .global_max_workers(16)
        .runtime_snapshot_interval(Duration::from_millis(25))
        .build()
        .unwrap();

    runtime.start().await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let overview = loop {
        let overview = wait_for_runtime_snapshot(client.pool(), queue).await;
        if overview
            .instances
            .iter()
            .any(|instance| instance.maintenance_alive)
        {
            break overview;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for maintenance loop to become healthy"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // Find the instance serving our queue (other tests may leave stale snapshots)
    let instance = overview
        .instances
        .iter()
        .find(|i| i.queues.iter().any(|q| q.queue == queue))
        .expect("expected an instance serving our queue");
    assert!(!instance.stale);
    assert!(instance.maintenance_alive);
    let queue_snapshot = instance.queues.iter().find(|q| q.queue == queue).unwrap();
    assert_eq!(
        queue_snapshot.config.mode,
        admin::QueueRuntimeMode::Weighted
    );
    assert_eq!(queue_snapshot.config.min_workers, Some(2));
    assert_eq!(queue_snapshot.config.weight, Some(3));
    assert_eq!(queue_snapshot.config.global_max_workers, Some(16));
    let rate_limit = queue_snapshot.config.rate_limit.as_ref().unwrap();
    assert_eq!(rate_limit.max_rate, 12.5);
    assert_eq!(rate_limit.burst, 25);

    let queue_runtime = admin::queue_runtime_summary(client.pool()).await.unwrap();
    let summary = queue_runtime.iter().find(|q| q.queue == queue).unwrap();
    assert_eq!(summary.instance_count, 1);
    assert_eq!(summary.live_instances, 1);
    assert_eq!(summary.healthy_instances, 1);
    assert!(!summary.config_mismatch);
    assert_eq!(
        summary.config.as_ref().map(|cfg| cfg.mode),
        Some(admin::QueueRuntimeMode::Weighted)
    );

    runtime.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn test_runtime_overview_empty_state() {
    let client = setup().await;
    // Ensure a clean table for this test (scoped: delete only instances with no queues
    // or with a dummy queue name that won't collide with other tests)
    let dummy_queue = "integ_empty_state_probe";
    sqlx::query("DELETE FROM awa.runtime_instances WHERE queues @> $1::jsonb")
        .bind(serde_json::json!([{ "queue": dummy_queue }]))
        .execute(client.pool())
        .await
        .unwrap();

    // An overview with no snapshots for any queue should return zeroes
    // Note: there may be snapshots from other tests, so we check that the API succeeds
    // and returns valid data structure
    let overview = admin::runtime_overview(client.pool()).await.unwrap();
    // Structural assertions
    assert!(overview.live_instances <= overview.total_instances);
    assert!(overview.stale_instances <= overview.total_instances);

    // queue_runtime_summary on a queue with no instances should be empty for that queue
    let summaries = admin::queue_runtime_summary(client.pool()).await.unwrap();
    let dummy_summary = summaries.iter().find(|s| s.queue == dummy_queue);
    assert!(
        dummy_summary.is_none(),
        "no summary expected for non-existent queue"
    );
}

#[tokio::test]
async fn test_cleanup_runtime_snapshots_preserves_fresh() {
    use chrono::{TimeDelta, Utc};
    use uuid::Uuid;

    let client = setup().await;

    let fresh_id = Uuid::new_v4();
    let stale_id = Uuid::new_v4();

    // Insert a "fresh" snapshot (last_seen_at = now, via the upsert)
    let fresh_snapshot = admin::RuntimeSnapshotInput {
        instance_id: fresh_id,
        hostname: Some("fresh-host".into()),
        pid: 1,
        version: "test".into(),
        started_at: Utc::now(),
        snapshot_interval_ms: 10_000,
        healthy: true,
        postgres_connected: true,
        poll_loop_alive: true,
        heartbeat_alive: true,
        maintenance_alive: true,
        shutting_down: false,
        leader: false,
        global_max_workers: None,
        queues: vec![],
    };
    admin::upsert_runtime_snapshot(client.pool(), &fresh_snapshot)
        .await
        .unwrap();

    // Insert a "stale" snapshot by directly inserting with an old last_seen_at
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_instances (
            instance_id, hostname, pid, version, started_at, last_seen_at,
            snapshot_interval_ms, healthy, postgres_connected, poll_loop_alive,
            heartbeat_alive, maintenance_alive, shutting_down, leader,
            global_max_workers, queues
        ) VALUES (
            $1, 'stale-host', 2, 'test', now(), now() - interval '48 hours',
            10000, true, true, true, true, false, false, false, NULL, '[]'::jsonb
        )
        "#,
    )
    .bind(stale_id)
    .execute(client.pool())
    .await
    .unwrap();

    // Run cleanup with 24h threshold — should delete stale but keep fresh
    let deleted =
        admin::cleanup_runtime_snapshots(client.pool(), TimeDelta::try_hours(24).unwrap())
            .await
            .unwrap();
    assert!(
        deleted >= 1,
        "expected at least the stale snapshot to be deleted"
    );

    // Verify fresh snapshot still exists
    let instances = admin::list_runtime_instances(client.pool()).await.unwrap();
    assert!(
        instances.iter().any(|i| i.instance_id == fresh_id),
        "fresh snapshot should not be deleted"
    );
    assert!(
        !instances.iter().any(|i| i.instance_id == stale_id),
        "stale snapshot should be deleted"
    );

    // Cleanup: remove our test data
    sqlx::query("DELETE FROM awa.runtime_instances WHERE instance_id = $1")
        .bind(fresh_id)
        .execute(client.pool())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_transactional_insert() {
    let client = setup().await;
    let queue = "integ_tx_insert";
    clean_queue(client.pool(), queue).await;

    let mut tx = client.pool().begin().await.unwrap();

    let job = insert_with(
        &mut *tx,
        &SendEmail {
            to: "tx@example.com".into(),
            subject: "Transactional".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    tx.commit().await.unwrap();

    let after_commit = client.get_job(job.id).await.unwrap();
    assert_eq!(after_commit.kind, "send_email");
    assert_eq!(after_commit.queue, queue);
}

#[tokio::test]
async fn test_unique_conflict_rejects_duplicate() {
    let client = setup().await;
    let queue = "integ_unique_conflict";
    clean_queue(client.pool(), queue).await;

    let opts = InsertOpts {
        queue: queue.into(),
        unique: Some(UniqueOpts {
            by_queue: true,
            ..UniqueOpts::default()
        }),
        ..Default::default()
    };

    // First insert should succeed
    let job1 = insert_with(
        client.pool(),
        &SendEmail {
            to: "unique@example.com".into(),
            subject: "First".into(),
        },
        opts.clone(),
    )
    .await
    .unwrap();

    assert!(job1.unique_key.is_some());
    assert_eq!(job1.state, JobState::Available);

    // Second insert with the same kind + args should be rejected as a unique conflict
    let result = insert_with(
        client.pool(),
        &SendEmail {
            to: "unique@example.com".into(),
            subject: "First".into(),
        },
        opts.clone(),
    )
    .await;

    assert!(
        matches!(result, Err(AwaError::UniqueConflict { .. })),
        "Expected UniqueConflict error, got: {:?}",
        result
    );
}

#[tokio::test]
async fn test_unique_key_insert() {
    let client = setup().await;
    let queue = "integ_unique_key";
    clean_queue(client.pool(), queue).await;

    let opts = InsertOpts {
        queue: queue.into(),
        unique: Some(UniqueOpts {
            by_queue: true,
            ..UniqueOpts::default()
        }),
        ..Default::default()
    };

    let job1 = insert_with(
        client.pool(),
        &SendEmail {
            to: "unique@example.com".into(),
            subject: "First".into(),
        },
        opts.clone(),
    )
    .await
    .unwrap();

    assert!(job1.unique_key.is_some());
}

#[tokio::test]
async fn test_job_state_lifecycle() {
    let client = setup().await;
    let queue = "integ_lifecycle";
    clean_queue(client.pool(), queue).await;

    // Insert -> available
    let job = insert_with(
        client.pool(),
        &SendEmail {
            to: "lifecycle@example.com".into(),
            subject: "Lifecycle".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(job.state, JobState::Available);

    // Work -> completed
    let worker = SendEmailWorker;
    let result = client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_completed());

    // Verify final state
    let final_job = client.get_job(job.id).await.unwrap();
    assert_eq!(final_job.state, JobState::Completed);
    assert_eq!(final_job.attempt, 1);
    assert!(final_job.finalized_at.is_some());
}

#[tokio::test]
async fn test_builder_requires_at_least_one_queue() {
    let client = setup().await;

    let result = Client::builder(client.pool().clone()).build();

    assert!(matches!(result, Err(BuildError::NoQueuesConfigured)));
}
