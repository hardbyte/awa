//! Integration tests for Awa — requires a running Postgres instance.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{admin, insert_many, insert_with, migrations, InsertOpts, UniqueOpts};
use awa::{
    AwaError, BuildError, Client, JobArgs, JobContext, JobError, JobResult, JobRow, JobState,
    Worker,
};
use awa_testing::{TestClient, WorkResult};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup() -> TestClient {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");

    let client = TestClient::from_pool(pool).await;
    client.migrate().await.expect("Failed to run migrations");
    // No global cleanup — each test uses a unique queue name for isolation.
    client
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

    async fn perform(&self, job_row: &JobRow, _ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: SendEmail = serde_json::from_value(job_row.args.clone())
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

    async fn perform(&self, _job_row: &JobRow, _ctx: &JobContext) -> Result<JobResult, JobError> {
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
