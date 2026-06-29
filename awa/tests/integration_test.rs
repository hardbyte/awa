//! Integration tests for Awa — requires a running Postgres instance.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{
    admin, batch_operations, insert_many, insert_with, migrations, BatchOperationFilter,
    BatchOperationSpec, InsertOpts, SubmitBatchOperation, UniqueOpts,
};
use awa::{
    AwaError, BuildError, Client, InsertParams, JobArgs, JobContext, JobError, JobResult, JobState,
    QueueConfig, RateLimit, Worker,
};
use awa_testing::{TestClient, WorkResult};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgListener, PgPoolOptions};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;

fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

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

/// Clean only jobs, queue_meta, and admin caches for a specific queue.
/// Call this at the start of each test to remove leftovers from previous runs.
/// Explicitly deletes the queue_state_counts row to prevent accumulated cache
/// drift from affecting assertions (the DELETE trigger should handle this, but
/// concurrent test runs against a shared DB can cause small delta errors that
/// compound over time).
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
    sqlx::query("DELETE FROM awa.queue_state_counts WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue state counts");
}

async fn run_batch_operation_to_completion(
    pool: &sqlx::PgPool,
    operation_id: uuid::Uuid,
) -> batch_operations::BatchOperation {
    let runner = uuid::Uuid::new_v4();
    for _ in 0..100 {
        let outcome = batch_operations::run_one_batch_operation_chunk(pool, runner, 10)
            .await
            .expect("run batch operation chunk");
        if outcome.finalized || !outcome.claimed {
            break;
        }
    }
    batch_operations::get_batch_operation(pool, operation_id)
        .await
        .expect("get batch operation")
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
async fn test_batch_operation_set_priority_and_move_queue_canonical() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    let source_queue = "integ_batch_ops_source";
    let dest_queue = "integ_batch_ops_dest";
    clean_queue(client.pool(), source_queue).await;
    clean_queue(client.pool(), dest_queue).await;
    sqlx::query("DELETE FROM awa.batch_operations")
        .execute(client.pool())
        .await
        .expect("clean batch operations");

    let job = insert_with(
        client.pool(),
        &SendEmail {
            to: "ops@example.com".into(),
            subject: "Escalate".into(),
        },
        InsertOpts {
            queue: source_queue.into(),
            priority: 4,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let preview = batch_operations::preview_batch_operation(
        client.pool(),
        BatchOperationSpec::SetPriority { priority: 1 },
        BatchOperationFilter {
            queue: Some(source_queue.to_string()),
            ..Default::default()
        },
    )
    .await
    .expect("preview set priority");
    assert_eq!(preview.total_matched, 1);

    let operation = batch_operations::submit_batch_operation(
        client.pool(),
        SubmitBatchOperation {
            spec: BatchOperationSpec::SetPriority { priority: 1 },
            filter: BatchOperationFilter {
                queue: Some(source_queue.to_string()),
                ..Default::default()
            },
            submitted_by: Some("test".to_string()),
            allow_all: false,
        },
    )
    .await
    .expect("submit set priority");
    let completed = run_batch_operation_to_completion(client.pool(), operation.id).await;
    assert_eq!(
        completed.state,
        batch_operations::BatchOperationState::Completed
    );
    assert_eq!(completed.processed, 1);

    let updated = admin::get_job(client.pool(), job.id).await.unwrap();
    assert_eq!(updated.priority, 1);
    assert_eq!(
        updated.metadata.get("_awa_original_priority"),
        Some(&serde_json::json!(4))
    );

    let operation = batch_operations::submit_batch_operation(
        client.pool(),
        SubmitBatchOperation {
            spec: BatchOperationSpec::MoveQueue {
                queue: dest_queue.to_string(),
                priority: Some(2),
            },
            filter: BatchOperationFilter {
                ids: Some(vec![job.id]),
                ..Default::default()
            },
            submitted_by: Some("test".to_string()),
            allow_all: false,
        },
    )
    .await
    .expect("submit move queue");
    let completed = run_batch_operation_to_completion(client.pool(), operation.id).await;
    assert_eq!(
        completed.state,
        batch_operations::BatchOperationState::Completed
    );

    let moved = admin::get_job(client.pool(), job.id).await.unwrap();
    assert_eq!(moved.queue, dest_queue);
    assert_eq!(moved.priority, 2);
    assert_eq!(
        moved.metadata.get("_awa_original_queue"),
        Some(&serde_json::json!(source_queue))
    );
}

#[tokio::test]
async fn test_batch_operation_rejects_ineligible_state_filter() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    sqlx::query("DELETE FROM awa.batch_operations")
        .execute(client.pool())
        .await
        .expect("clean batch operations");

    let err = batch_operations::submit_batch_operation(
        client.pool(),
        SubmitBatchOperation {
            spec: BatchOperationSpec::SetPriority { priority: 1 },
            filter: BatchOperationFilter {
                state: Some(JobState::Failed),
                ..Default::default()
            },
            submitted_by: Some("test".to_string()),
            allow_all: false,
        },
    )
    .await
    .expect_err("failed-state filter must be rejected, not broadened");
    assert!(err
        .to_string()
        .contains("only supports available or scheduled"));
}

#[tokio::test]
async fn test_batch_operation_cancellation_marks_remaining_items_skipped() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    let queue = "integ_batch_cancel";
    clean_queue(client.pool(), queue).await;
    sqlx::query("DELETE FROM awa.batch_operations")
        .execute(client.pool())
        .await
        .expect("clean batch operations");

    for id in 0..3 {
        insert_with(
            client.pool(),
            &SendEmail {
                to: format!("user{id}@example.com"),
                subject: "Cancel batch".into(),
            },
            InsertOpts {
                queue: queue.into(),
                priority: 4,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    let operation = batch_operations::submit_batch_operation(
        client.pool(),
        SubmitBatchOperation {
            spec: BatchOperationSpec::SetPriority { priority: 1 },
            filter: BatchOperationFilter {
                queue: Some(queue.to_string()),
                ..Default::default()
            },
            submitted_by: Some("test".to_string()),
            allow_all: false,
        },
    )
    .await
    .expect("submit batch op");

    let runner = uuid::Uuid::new_v4();
    let first = batch_operations::run_one_batch_operation_chunk(client.pool(), runner, 1)
        .await
        .expect("first chunk");
    assert_eq!(first.processed, 1);
    batch_operations::request_batch_operation_cancellation(client.pool(), operation.id)
        .await
        .expect("request cancellation");
    let second = batch_operations::run_one_batch_operation_chunk(client.pool(), runner, 10)
        .await
        .expect("cancellation chunk");
    assert!(second.finalized);

    let operation = batch_operations::get_batch_operation(client.pool(), operation.id)
        .await
        .expect("get operation");
    assert_eq!(
        operation.state,
        batch_operations::BatchOperationState::Cancelled
    );
    assert_eq!(operation.total_matched, Some(3));
    assert_eq!(operation.processed, 1);
    assert_eq!(operation.skipped, 2);
}

#[tokio::test]
async fn test_maintenance_leader_processes_batch_operations() {
    let _guard = test_lock().lock().await;
    let client = setup_with_connections(8).await;
    let queue = "integ_batch_maintenance";
    clean_queue(client.pool(), queue).await;
    sqlx::query("DELETE FROM awa.batch_operations")
        .execute(client.pool())
        .await
        .expect("clean batch operations");

    let job = insert_with(
        client.pool(),
        &SendEmail {
            to: "maint@example.com".into(),
            subject: "Maintenance".into(),
        },
        InsertOpts {
            queue: queue.into(),
            priority: 4,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let operation = batch_operations::submit_batch_operation(
        client.pool(),
        SubmitBatchOperation {
            spec: BatchOperationSpec::SetPriority { priority: 1 },
            filter: BatchOperationFilter {
                ids: Some(vec![job.id]),
                ..Default::default()
            },
            submitted_by: Some("test".to_string()),
            allow_all: false,
        },
    )
    .await
    .expect("submit batch op");

    let runtime = Client::builder(client.pool().clone())
        .queue("integ_batch_maintenance_idle", QueueConfig::default())
        .build()
        .unwrap();
    runtime.start().await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        let operation = batch_operations::get_batch_operation(client.pool(), operation.id)
            .await
            .expect("get operation");
        if operation.state == batch_operations::BatchOperationState::Completed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "maintenance did not complete batch operation in time: {operation:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    runtime.shutdown(Duration::from_secs(1)).await;

    let updated = admin::get_job(client.pool(), job.id).await.unwrap();
    assert_eq!(updated.priority, 1);
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
async fn test_insert_many_updates_admin_metadata_for_direct_paths() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    let hot_queue = "integ_insert_many_admin_hot";
    let scheduled_queue = "integ_insert_many_admin_scheduled";
    let hot_kind = "integ_insert_many_admin_hot_kind";
    let scheduled_kind = "integ_insert_many_admin_scheduled_kind";

    clean_queue(client.pool(), hot_queue).await;
    clean_queue(client.pool(), scheduled_queue).await;
    sqlx::query("DELETE FROM awa.jobs WHERE kind = ANY($1)")
        .bind(vec![hot_kind, scheduled_kind])
        .execute(client.pool())
        .await
        .unwrap();

    let hot_jobs = vec![
        InsertParams {
            kind: hot_kind.to_string(),
            args: serde_json::json!({"seq": 1}),
            opts: InsertOpts {
                queue: hot_queue.to_string(),
                ..Default::default()
            },
        },
        InsertParams {
            kind: hot_kind.to_string(),
            args: serde_json::json!({"seq": 2}),
            opts: InsertOpts {
                queue: hot_queue.to_string(),
                ..Default::default()
            },
        },
    ];
    insert_many(client.pool(), &hot_jobs).await.unwrap();

    let scheduled_jobs = vec![
        InsertParams {
            kind: scheduled_kind.to_string(),
            args: serde_json::json!({"seq": 3}),
            opts: InsertOpts {
                queue: scheduled_queue.to_string(),
                run_at: Some(chrono::Utc::now() + chrono::Duration::minutes(30)),
                ..Default::default()
            },
        },
        InsertParams {
            kind: scheduled_kind.to_string(),
            args: serde_json::json!({"seq": 4}),
            opts: InsertOpts {
                queue: scheduled_queue.to_string(),
                run_at: Some(chrono::Utc::now() + chrono::Duration::minutes(45)),
                ..Default::default()
            },
        },
    ];
    insert_many(client.pool(), &scheduled_jobs).await.unwrap();

    admin::flush_dirty_admin_metadata(client.pool())
        .await
        .unwrap();
    let stats = admin::queue_overviews(client.pool()).await.unwrap();
    let hot_stats = stats.iter().find(|stat| stat.queue == hot_queue).unwrap();
    assert_eq!(hot_stats.available, 2);
    assert_eq!(hot_stats.total_queued, 2);

    let scheduled_stats = stats
        .iter()
        .find(|stat| stat.queue == scheduled_queue)
        .unwrap();
    assert_eq!(scheduled_stats.scheduled, 2);
    assert_eq!(scheduled_stats.total_queued, 2);

    let kinds = admin::distinct_kinds(client.pool()).await.unwrap();
    assert!(kinds.contains(&hot_kind.to_string()));
    assert!(kinds.contains(&scheduled_kind.to_string()));

    let queues = admin::distinct_queues(client.pool()).await.unwrap();
    assert!(queues.contains(&hot_queue.to_string()));
    assert!(queues.contains(&scheduled_queue.to_string()));
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
    let _guard = test_lock().lock().await;
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
    let _guard = test_lock().lock().await;
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
async fn test_admin_cancel_by_unique_key_emits_canonical_running_cancel_notification() {
    let _guard = test_lock().lock().await;
    let client = setup_with_connections(4).await;
    let queue = "integ_cancel_by_unique_key_notify";
    awa_testing::setup::reset_runtime_backend(client.pool()).await;
    clean_queue(client.pool(), queue).await;

    let mut listener = PgListener::connect_with(client.pool()).await.unwrap();
    listener.listen("awa:cancel").await.unwrap();

    let args = SendEmail {
        to: "cancel-by-key-running@example.com".into(),
        subject: "Cancel running by key".into(),
    };
    let job = insert_with(
        client.pool(),
        &args,
        InsertOpts {
            queue: queue.into(),
            unique: Some(UniqueOpts {
                by_queue: true,
                by_args: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    sqlx::query(
        "UPDATE awa.jobs \
         SET state = 'running', attempt = 1, run_lease = 42, attempted_at = now(), heartbeat_at = now() \
         WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    let cancelled = admin::cancel_by_unique_key(
        client.pool(),
        SendEmail::kind(),
        Some(queue),
        Some(&serde_json::to_value(&args).unwrap()),
        None,
    )
    .await
    .unwrap()
    .expect("running job should be cancelled by unique key");
    assert_eq!(cancelled.id, job.id);

    let notification = tokio::time::timeout(Duration::from_secs(3), listener.recv())
        .await
        .expect("cancel notification should be emitted")
        .expect("cancel listener should receive notification");
    let payload: serde_json::Value = serde_json::from_str(notification.payload()).unwrap();
    assert_eq!(payload["job_id"].as_i64(), Some(job.id));
    assert_eq!(payload["run_lease"].as_i64(), Some(42));
}

#[tokio::test]
async fn test_admin_cancel_by_unique_key() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    let queue = "integ_cancel_by_unique_key";
    clean_queue(client.pool(), queue).await;

    let args = SendEmail {
        to: "cancel-by-key@example.com".into(),
        subject: "Cancel me by key".into(),
    };

    let job = insert_with(
        client.pool(),
        &args,
        InsertOpts {
            queue: queue.into(),
            unique: Some(UniqueOpts {
                by_queue: true,
                by_args: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Cancel using the same kind + queue + args that were used to insert
    let cancelled = admin::cancel_by_unique_key(
        client.pool(),
        SendEmail::kind(),
        Some(queue),
        Some(&serde_json::to_value(&args).unwrap()),
        None,
    )
    .await
    .unwrap();

    assert!(cancelled.is_some(), "should find and cancel the job");
    assert_eq!(cancelled.unwrap().id, job.id);

    let fetched = client.get_job(job.id).await.unwrap();
    assert_eq!(fetched.state, JobState::Cancelled);
}

#[tokio::test]
async fn test_admin_cancel_by_unique_key_tx_is_transactional() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    let queue = "integ_cancel_by_unique_key_tx";
    clean_queue(client.pool(), queue).await;

    let args = SendEmail {
        to: "cancel-by-key-tx@example.com".into(),
        subject: "Cancel me by key in a tx".into(),
    };
    let job = insert_with(
        client.pool(),
        &args,
        InsertOpts {
            queue: queue.into(),
            unique: Some(UniqueOpts {
                by_queue: true,
                by_args: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let args_value = serde_json::to_value(&args).unwrap();

    // Rolled-back transaction, passing a `&mut Transaction` (which deref-coerces
    // to `&mut PgConnection`): the cancel resolves the job but must NOT persist.
    let mut tx = client.pool().begin().await.unwrap();
    let cancelled = admin::cancel_by_unique_key_tx(
        &mut tx,
        SendEmail::kind(),
        Some(queue),
        Some(&args_value),
        None,
    )
    .await
    .unwrap();
    assert_eq!(cancelled.expect("tx cancel should find the job").id, job.id);
    tx.rollback().await.unwrap();

    let after_rollback = client.get_job(job.id).await.unwrap();
    assert_ne!(
        after_rollback.state,
        JobState::Cancelled,
        "a rolled-back cancel_by_unique_key_tx must leave the job uncancelled"
    );

    // Committed transaction, passing an explicit `&mut PgConnection` — exactly
    // how a `transact!`-style helper hands a connection (not a `Transaction`) to
    // its body.
    let mut tx = client.pool().begin().await.unwrap();
    let conn: &mut sqlx::PgConnection = &mut tx;
    let cancelled = admin::cancel_by_unique_key_tx(
        conn,
        SendEmail::kind(),
        Some(queue),
        Some(&args_value),
        None,
    )
    .await
    .unwrap();
    assert_eq!(cancelled.expect("tx cancel should find the job").id, job.id);
    tx.commit().await.unwrap();

    let after_commit = client.get_job(job.id).await.unwrap();
    assert_eq!(after_commit.state, JobState::Cancelled);
}

#[tokio::test]
async fn test_admin_cancel_by_unique_key_returns_none_when_not_found() {
    let _guard = test_lock().lock().await;
    let client = setup().await;

    let result = admin::cancel_by_unique_key(
        client.pool(),
        "nonexistent_kind",
        Some("nonexistent_queue"),
        Some(&serde_json::json!({"id": "does-not-exist"})),
        None,
    )
    .await
    .unwrap();

    assert!(result.is_none(), "should return None for non-existent job");
}

#[tokio::test]
async fn test_admin_cancel_by_unique_key_noop_when_already_completed() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    let queue = "integ_cancel_by_key_completed";
    clean_queue(client.pool(), queue).await;

    let args = SendEmail {
        to: "completed@example.com".into(),
        subject: "Already done".into(),
    };

    let job = insert_with(
        client.pool(),
        &args,
        InsertOpts {
            queue: queue.into(),
            unique: Some(UniqueOpts {
                by_queue: true,
                by_args: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Manually mark as completed
    sqlx::query("UPDATE awa.jobs SET state = 'completed', finalized_at = now() WHERE id = $1")
        .bind(job.id)
        .execute(client.pool())
        .await
        .unwrap();

    // Cancel should return None — job is already terminal
    let result = admin::cancel_by_unique_key(
        client.pool(),
        SendEmail::kind(),
        Some(queue),
        Some(&serde_json::to_value(&args).unwrap()),
        None,
    )
    .await
    .unwrap();

    assert!(
        result.is_none(),
        "should not cancel an already-completed job"
    );
}

#[tokio::test]
async fn test_admin_cancel_by_unique_key_scheduled_job() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    let queue = "integ_cancel_by_key_scheduled";
    clean_queue(client.pool(), queue).await;

    let args = SendEmail {
        to: "scheduled@example.com".into(),
        subject: "Future job".into(),
    };

    // Insert with run_at in the future — goes to scheduled_jobs (cold table)
    let job = insert_with(
        client.pool(),
        &args,
        InsertOpts {
            queue: queue.into(),
            run_at: Some(chrono::Utc::now() + chrono::TimeDelta::hours(24)),
            unique: Some(UniqueOpts {
                by_queue: true,
                by_args: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Verify it's in scheduled state (cold table)
    let fetched = client.get_job(job.id).await.unwrap();
    assert_eq!(fetched.state, JobState::Scheduled);

    // Cancel by unique key should find it in the cold table
    let cancelled = admin::cancel_by_unique_key(
        client.pool(),
        SendEmail::kind(),
        Some(queue),
        Some(&serde_json::to_value(&args).unwrap()),
        None,
    )
    .await
    .unwrap();

    assert!(cancelled.is_some(), "should cancel job in scheduled_jobs");
    assert_eq!(cancelled.unwrap().id, job.id);

    let fetched = client.get_job(job.id).await.unwrap();
    assert_eq!(fetched.state, JobState::Cancelled);
}

#[tokio::test]
async fn test_admin_cancel_by_unique_key_cancels_oldest_when_multiple_exist() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    let queue = "integ_cancel_by_key_multi";
    clean_queue(client.pool(), queue).await;

    let args = SendEmail {
        to: "multi@example.com".into(),
        subject: "Duplicate key".into(),
    };
    let args_json = serde_json::to_value(&args).unwrap();

    // Insert first job with unique key
    let job1 = insert_with(
        client.pool(),
        &args,
        InsertOpts {
            queue: queue.into(),
            unique: Some(UniqueOpts {
                by_queue: true,
                by_args: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Move first job to waiting_external (excluded from default unique_states bitmask)
    // so a second job with the same key can be inserted
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempted_at = now(), deadline_at = now() + interval '5 min' WHERE id = $1",
    )
    .bind(job1.id)
    .execute(client.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE awa.jobs SET state = 'waiting_external', callback_id = gen_random_uuid(), callback_timeout_at = now() + interval '1 hour' WHERE id = $1",
    )
    .bind(job1.id)
    .execute(client.pool())
    .await
    .unwrap();

    // Insert second job — should succeed since waiting_external is outside default unique_states
    let job2 = insert_with(
        client.pool(),
        &args,
        InsertOpts {
            queue: queue.into(),
            unique: Some(UniqueOpts {
                by_queue: true,
                by_args: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_ne!(job1.id, job2.id, "should be two distinct jobs");

    // Cancel by unique key — should cancel only the oldest (job1)
    let cancelled = admin::cancel_by_unique_key(
        client.pool(),
        SendEmail::kind(),
        Some(queue),
        Some(&args_json),
        None,
    )
    .await
    .unwrap();

    assert!(cancelled.is_some());
    assert_eq!(
        cancelled.unwrap().id,
        job1.id,
        "should cancel the oldest job"
    );

    // job2 should still be available
    let job2_fetched = client.get_job(job2.id).await.unwrap();
    assert_eq!(job2_fetched.state, JobState::Available);
}

#[tokio::test]
async fn test_admin_cancel_by_unique_key_mismatched_queue_returns_none() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    let queue = "integ_cancel_by_key_mismatch";
    clean_queue(client.pool(), queue).await;

    let args = SendEmail {
        to: "mismatch@example.com".into(),
        subject: "Wrong queue".into(),
    };

    insert_with(
        client.pool(),
        &args,
        InsertOpts {
            queue: queue.into(),
            unique: Some(UniqueOpts {
                by_queue: true,
                by_args: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Cancel with wrong queue — different hash, should not find the job
    let result = admin::cancel_by_unique_key(
        client.pool(),
        SendEmail::kind(),
        Some("wrong_queue"),
        Some(&serde_json::to_value(&args).unwrap()),
        None,
    )
    .await
    .unwrap();

    assert!(
        result.is_none(),
        "mismatched queue should produce different hash"
    );
}

/// Force the engine onto the queue-storage substrate the way a fresh install
/// auto-finalizes onto it — the inverse of `awa_testing::reset_runtime_backend`.
/// The substrate already exists in the `awa` schema (installed by the
/// migrations); flipping the transition state and registering the backend is
/// enough for the admin paths to route there.
async fn activate_queue_storage(pool: &sqlx::PgPool) {
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET state = 'active',
            current_engine = 'queue_storage',
            prepared_engine = NULL,
            details = jsonb_build_object('schema', 'awa'),
            transition_epoch = transition_epoch + 1,
            updated_at = now(),
            finalized_at = now()
        WHERE singleton
        "#,
    )
    .execute(&mut *tx)
    .await
    .expect("activate queue-storage transition state");
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
        VALUES ('queue_storage', 'awa', now())
        ON CONFLICT (backend) DO UPDATE
        SET schema_name = EXCLUDED.schema_name, updated_at = EXCLUDED.updated_at
        "#,
    )
    .execute(&mut *tx)
    .await
    .expect("register queue-storage backend");
    tx.commit().await.unwrap();
}

/// `cancel_by_unique_key` and its `_tx` variant must run against the
/// queue-storage substrate, not only the canonical tables. The queue-storage
/// candidate query spans the available, deferred, and running-job lookups (the
/// running one reaches `unique_key` through a `leases → ready_entries` join), so
/// it must execute and return `Ok(None)` when nothing matches the key.
#[tokio::test]
async fn test_admin_cancel_by_unique_key_queue_storage_active() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    activate_queue_storage(client.pool()).await;

    let pool_result = admin::cancel_by_unique_key(
        client.pool(),
        SendEmail::kind(),
        Some("integ_cancel_by_unique_key_qs"),
        None,
        None,
    )
    .await;

    let mut tx = client.pool().begin().await.unwrap();
    let tx_result = admin::cancel_by_unique_key_tx(
        &mut tx,
        SendEmail::kind(),
        Some("integ_cancel_by_unique_key_qs"),
        None,
        None,
    )
    .await;
    tx.rollback().await.unwrap();

    // Restore the canonical engine before asserting so a failed assertion can't
    // leak queue-storage state into other tests sharing the database.
    awa_testing::setup::reset_runtime_backend(client.pool()).await;

    assert!(
        matches!(pool_result, Ok(None)),
        "queue-storage cancel_by_unique_key must not error when nothing matches; got {pool_result:?}"
    );
    assert!(
        matches!(tx_result, Ok(None)),
        "queue-storage cancel_by_unique_key_tx must not error when nothing matches; got {tx_result:?}"
    );
}

#[tokio::test]
async fn test_admin_pause_resume_queue() {
    let _guard = test_lock().lock().await;
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
    let _guard = test_lock().lock().await;
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
    let _guard = test_lock().lock().await;
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

    insert_with(
        client.pool(),
        &SendEmail {
            to: "scheduled@example.com".into(),
            subject: "Scheduled stats test".into(),
        },
        InsertOpts {
            queue: queue.into(),
            run_at: Some(chrono::Utc::now() + chrono::Duration::minutes(30)),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO awa.jobs (kind, queue, args, state, run_at) VALUES ('send_email', $1, '{}'::jsonb, 'retryable', now() + interval '10 minutes')",
    )
    .bind(queue)
    .execute(client.pool())
    .await
    .unwrap();

    admin::flush_dirty_admin_metadata(client.pool())
        .await
        .unwrap();
    let stats = admin::queue_overviews(client.pool()).await.unwrap();
    let stat = stats.iter().find(|s| s.queue == queue).unwrap();
    assert_eq!(stat.available, 3);
    assert_eq!(stat.scheduled, 1);
    assert_eq!(stat.retryable, 1);
    assert_eq!(stat.total_queued, 5);
}

#[tokio::test]
async fn test_admin_metadata_caches_track_state_and_catalog_changes() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    let queue_a = "integ_admin_meta_a";
    let queue_b = "integ_admin_meta_b";
    let kind_a = "integ_admin_meta_available_kind";
    let kind_b = "integ_admin_meta_scheduled_kind";
    let kind_c = "integ_admin_meta_waiting_kind";
    let our_queues: &[&str] = &[queue_a, queue_b];
    let our_kinds: &[&str] = &[kind_a, kind_b, kind_c];

    // Run the entire test on a single pooled connection inside one
    // transaction that holds the admin-metadata advisory lock (1098018130).
    // Why: that lock is what `refresh_admin_metadata()` and
    // `recompute_dirty_admin_metadata()` acquire before touching the
    // cache tables. While we hold it, no maintenance leader running in a
    // parallel test binary can take a snapshot of `jobs_hot` before our
    // INSERTs commit and then overwrite `queue_state_counts` with stale
    // counts between our recompute and our read. Doing both the mutation
    // and the cache read in the same transaction also means the recompute
    // call sees our pending dirty markers and our reads see the upsert it
    // just performed — no cross-connection visibility races.
    let mut tx = client.pool().begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(1098018130)")
        .execute(&mut *tx)
        .await
        .unwrap();

    // Wipe any leftovers from previous runs of this test (jobs, pause
    // flags, cache rows, dirty markers) for our unique queue/kind names.
    sqlx::query("DELETE FROM awa.jobs WHERE queue = ANY($1) OR kind = ANY($2)")
        .bind(our_queues)
        .bind(our_kinds)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = ANY($1)")
        .bind(our_queues)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM awa.queue_state_counts WHERE queue = ANY($1)")
        .bind(our_queues)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM awa.job_kind_catalog WHERE kind = ANY($1)")
        .bind(our_kinds)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM awa.job_queue_catalog WHERE queue = ANY($1)")
        .bind(our_queues)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM awa.admin_dirty_queues WHERE queue = ANY($1)")
        .bind(our_queues)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM awa.admin_dirty_kinds WHERE kind = ANY($1)")
        .bind(our_kinds)
        .execute(&mut *tx)
        .await
        .unwrap();

    // ── Phase 1: insert 3 jobs across two queues, drain, verify caches ──
    sqlx::query(
        r#"
        INSERT INTO awa.jobs (kind, queue, args, state, run_at)
        VALUES
            ($1, $2, '{}'::jsonb, 'available', now()),
            ($3, $2, '{}'::jsonb, 'scheduled', now() + interval '30 minutes'),
            ($4, $5, '{}'::jsonb, 'waiting_external', now())
        "#,
    )
    .bind(kind_a)
    .bind(queue_a)
    .bind(kind_b)
    .bind(kind_c)
    .bind(queue_b)
    .execute(&mut *tx)
    .await
    .unwrap();

    drain_dirty_for(&mut tx, our_queues, our_kinds).await;

    let (a_total, a_avail, a_sched) = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT scheduled + available + running + retryable + waiting_external,
                available,
                scheduled
         FROM awa.queue_state_counts WHERE queue = $1",
    )
    .bind(queue_a)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(a_total, 2);
    assert_eq!(a_avail, 1);
    assert_eq!(a_sched, 1);

    let (b_total, b_waiting) = sqlx::query_as::<_, (i64, i64)>(
        "SELECT scheduled + available + running + retryable + waiting_external,
                waiting_external
         FROM awa.queue_state_counts WHERE queue = $1",
    )
    .bind(queue_b)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(b_total, 1);
    assert_eq!(b_waiting, 1);

    let kinds_present: Vec<String> = sqlx::query_scalar(
        "SELECT kind FROM awa.job_kind_catalog WHERE kind = ANY($1) AND ref_count > 0",
    )
    .bind(our_kinds)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert!(kinds_present.contains(&kind_a.to_string()));
    assert!(kinds_present.contains(&kind_b.to_string()));
    assert!(kinds_present.contains(&kind_c.to_string()));

    let queues_present: Vec<String> = sqlx::query_scalar(
        "SELECT queue FROM awa.job_queue_catalog WHERE queue = ANY($1) AND ref_count > 0",
    )
    .bind(our_queues)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert!(queues_present.contains(&queue_a.to_string()));
    assert!(queues_present.contains(&queue_b.to_string()));

    // ── Phase 2: promote kind_b from scheduled to available ─────────────
    sqlx::query("UPDATE awa.jobs SET state = 'available', run_at = now() WHERE kind = $1")
        .bind(kind_b)
        .execute(&mut *tx)
        .await
        .unwrap();
    drain_dirty_for(&mut tx, our_queues, our_kinds).await;
    let (a_avail_after, a_sched_after) = sqlx::query_as::<_, (i64, i64)>(
        "SELECT available, scheduled FROM awa.queue_state_counts WHERE queue = $1",
    )
    .bind(queue_a)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(a_avail_after, 2, "kind_b should now be available");
    assert_eq!(a_sched_after, 0, "no scheduled jobs should remain");

    // ── Phase 3: delete kind_c (only job in queue_b); cache should purge ─
    sqlx::query("DELETE FROM awa.jobs WHERE kind = $1")
        .bind(kind_c)
        .execute(&mut *tx)
        .await
        .unwrap();
    drain_dirty_for(&mut tx, our_queues, our_kinds).await;

    let queue_b_row: Option<i32> =
        sqlx::query_scalar("SELECT 1 FROM awa.queue_state_counts WHERE queue = $1")
            .bind(queue_b)
            .fetch_optional(&mut *tx)
            .await
            .unwrap();
    assert!(
        queue_b_row.is_none(),
        "queue_b should be purged from queue_state_counts"
    );

    let kind_c_row: Option<i32> =
        sqlx::query_scalar("SELECT 1 FROM awa.job_kind_catalog WHERE kind = $1 AND ref_count > 0")
            .bind(kind_c)
            .fetch_optional(&mut *tx)
            .await
            .unwrap();
    assert!(
        kind_c_row.is_none(),
        "kind_c should be purged from job_kind_catalog"
    );

    let queue_b_catalog_row: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM awa.job_queue_catalog WHERE queue = $1 AND ref_count > 0",
    )
    .bind(queue_b)
    .fetch_optional(&mut *tx)
    .await
    .unwrap();
    assert!(
        queue_b_catalog_row.is_none(),
        "queue_b should be purged from job_queue_catalog"
    );

    tx.commit().await.unwrap();
}

/// Drive the production `recompute_dirty_admin_metadata()` SQL function
/// until our test's queues and kinds have no remaining dirty markers, so
/// the cache tables reflect every mutation we just made.
///
/// Concurrent test binaries hitting the shared database can keep adding
/// dirty markers for *their* queues/kinds while we loop, but
/// `recompute_dirty_admin_metadata` orders by `touched_at` and processes
/// 1000 entries per call, so each iteration makes forward progress and a
/// bounded number of iterations is enough in practice. We cap the loop so
/// a wedged drain reports a clear failure instead of hanging the test.
async fn drain_dirty_for(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    queues: &[&str],
    kinds: &[&str],
) {
    for _ in 0..100 {
        sqlx::query("SELECT awa.recompute_dirty_admin_metadata(1000)")
            .execute(&mut **tx)
            .await
            .unwrap();
        let remaining: i64 = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM awa.admin_dirty_queues WHERE queue = ANY($1))
                  + (SELECT count(*) FROM awa.admin_dirty_kinds WHERE kind = ANY($2))",
        )
        .bind(queues)
        .bind(kinds)
        .fetch_one(&mut **tx)
        .await
        .unwrap();
        if remaining == 0 {
            return;
        }
    }
    panic!("admin dirty markers for test queues/kinds were not drained after 100 iterations");
}

#[tokio::test]
async fn test_admin_metadata_tracks_scheduled_promotion_path() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    let queue = "integ_admin_meta_promote";
    let kind = "integ_admin_meta_promote_kind";

    clean_queue(client.pool(), queue).await;
    sqlx::query("DELETE FROM awa.jobs WHERE kind = $1")
        .bind(kind)
        .execute(client.pool())
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO awa.jobs (kind, queue, args, state, run_at) VALUES ($1, $2, '{}'::jsonb, 'scheduled', now() - interval '1 minute')",
    )
    .bind(kind)
    .bind(queue)
    .execute(client.pool())
    .await
    .unwrap();

    admin::flush_dirty_admin_metadata(client.pool())
        .await
        .unwrap();
    let before = admin::queue_overviews(client.pool()).await.unwrap();
    let before = before.iter().find(|stat| stat.queue == queue).unwrap();
    assert_eq!(before.scheduled, 1);
    assert_eq!(before.available, 0);
    assert_eq!(before.total_queued, 1);

    let promoted: i64 = sqlx::query_scalar(
        r#"
        WITH due AS (
            DELETE FROM awa.scheduled_jobs
            WHERE id IN (
                SELECT id
                FROM awa.scheduled_jobs
                WHERE queue = $1
                  AND state = 'scheduled'
                  AND run_at <= now()
                ORDER BY run_at ASC, id ASC
                LIMIT 32
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
        ),
        promoted AS (
            INSERT INTO awa.jobs_hot (
                id, kind, queue, args, state, priority, attempt, max_attempts,
                run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
                created_at, errors, metadata, tags, unique_key, unique_states,
                callback_id, callback_timeout_at, callback_filter, callback_on_complete,
                callback_on_fail, callback_transform, run_lease, progress
            )
            SELECT
                id,
                kind,
                queue,
                args,
                'available'::awa.job_state,
                priority,
                attempt,
                max_attempts,
                now(),
                NULL,
                NULL,
                attempted_at,
                finalized_at,
                created_at,
                errors,
                metadata,
                tags,
                unique_key,
                unique_states,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                run_lease,
                progress
            FROM due
            RETURNING id
        )
        SELECT count(*)::bigint FROM promoted
        "#,
    )
    .bind(queue)
    .fetch_one(client.pool())
    .await
    .unwrap();
    assert_eq!(promoted, 1);

    admin::flush_dirty_admin_metadata(client.pool())
        .await
        .unwrap();
    let after = admin::queue_overviews(client.pool()).await.unwrap();
    let after = after.iter().find(|stat| stat.queue == queue).unwrap();
    assert_eq!(after.scheduled, 0);
    assert_eq!(after.available, 1);
    assert_eq!(after.total_queued, 1);
}

/// Heartbeat and progress-only UPDATEs must NOT dirty queues or kinds.
/// The dirty-key UPDATE trigger filters via JOIN on old_rows/new_rows,
/// only inserting dirty keys when queue, kind, or state actually changed.
#[tokio::test]
async fn test_heartbeat_progress_updates_do_not_dirty_admin_metadata() {
    let _guard = test_lock().lock().await;
    let client = setup().await;
    let queue = "integ_no_dirty_heartbeat";
    clean_queue(client.pool(), queue).await;

    // Insert and claim a job so it's in 'running' state
    insert_with(
        client.pool(),
        &SendEmail {
            to: "hb@example.com".into(),
            subject: "HB".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE awa.jobs_hot SET state = 'running', attempt = 1, heartbeat_at = now() WHERE queue = $1",
    )
    .bind(queue)
    .execute(client.pool())
    .await
    .unwrap();

    // Flush dirty keys from the insert + state change above
    admin::flush_dirty_admin_metadata(client.pool())
        .await
        .unwrap();

    // Clear the dirty tables completely
    sqlx::query("DELETE FROM awa.admin_dirty_queues")
        .execute(client.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM awa.admin_dirty_kinds")
        .execute(client.pool())
        .await
        .unwrap();

    // Heartbeat-only update (no state/queue/kind change)
    sqlx::query("UPDATE awa.jobs_hot SET heartbeat_at = now() WHERE queue = $1")
        .bind(queue)
        .execute(client.pool())
        .await
        .unwrap();

    // Progress-only update
    sqlx::query("UPDATE awa.jobs_hot SET progress = '{\"percent\": 50}'::jsonb WHERE queue = $1")
        .bind(queue)
        .execute(client.pool())
        .await
        .unwrap();

    // Verify no dirty key was created for THIS queue.
    // We check by queue name (unique to this test) rather than count(*)
    // because concurrent parallel tests may dirty other queues/kinds.
    let dirty_for_queue: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.admin_dirty_queues WHERE queue = $1")
            .bind(queue)
            .fetch_one(client.pool())
            .await
            .unwrap();
    assert_eq!(
        dirty_for_queue, 0,
        "heartbeat/progress should not dirty this queue"
    );
}

/// flush_dirty_admin_metadata() must drain the entire backlog, not just
/// one 100-key batch.
#[tokio::test]
async fn test_flush_dirty_admin_metadata_drains_full_backlog() {
    let _guard = test_lock().lock().await;
    let client = setup().await;

    // Clean up
    sqlx::query("DELETE FROM awa.jobs_hot WHERE queue LIKE 'integ_flush_backlog_%'")
        .execute(client.pool())
        .await
        .unwrap();

    // Insert jobs across >100 distinct queues to create a backlog larger than one batch
    for i in 0..120 {
        let queue = format!("integ_flush_backlog_{i:03}");
        insert_with(
            client.pool(),
            &SendEmail {
                to: "flush@example.com".into(),
                subject: format!("Flush {i}"),
            },
            InsertOpts {
                queue,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    // Flush all dirty keys. We don't assert on the count because a
    // concurrent migrate() or parallel test may have already drained
    // them via refresh_admin_metadata(). The important assertion is
    // that the cache is correct afterward.
    admin::flush_dirty_admin_metadata(client.pool())
        .await
        .unwrap();

    // Scope the post-flush check to this test's queues. Parallel test
    // binaries share the database and can write fresh dirty markers between
    // our flush and this assertion, so a global count(*) is racy.
    let dirty_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.admin_dirty_queues WHERE queue LIKE 'integ_flush_backlog_%'",
    )
    .fetch_one(client.pool())
    .await
    .unwrap();
    assert_eq!(
        dirty_after, 0,
        "all dirty queues for this test should be drained"
    );

    // Verify the cache is accurate for a sample queue
    let stats = admin::queue_overviews(client.pool()).await.unwrap();
    let stat = stats
        .iter()
        .find(|s| s.queue == "integ_flush_backlog_000")
        .unwrap();
    assert_eq!(stat.available, 1);
}

#[tokio::test]
async fn test_admin_list_jobs() {
    let _guard = test_lock().lock().await;
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
    let _guard = test_lock().lock().await;
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
    let instance = loop {
        let overview = wait_for_runtime_snapshot(client.pool(), queue).await;
        if let Some(instance) = overview.instances.iter().find(|instance| {
            !instance.stale
                && instance.maintenance_alive
                && instance.queues.iter().any(|q| q.queue == queue)
        }) {
            break instance.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for queue-scoped maintenance loop to become healthy"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
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
        storage_capability: admin::StorageCapability::Canonical,
        transition_role: admin::TransitionRole::Auto,
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
        queue_descriptor_hashes: std::collections::HashMap::new(),
        job_kind_descriptor_hashes: std::collections::HashMap::new(),
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
    assert_eq!(
        instances
            .iter()
            .find(|i| i.instance_id == fresh_id)
            .map(|i| i.storage_capability.as_str()),
        Some("canonical")
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

#[tokio::test]
async fn test_null_byte_in_args_rejected_with_validation_error() {
    let client = setup().await;
    let queue = "integ_null_byte";

    let result = insert_with(
        client.pool(),
        &SendEmail {
            to: "test\x00@example.com".into(),
            subject: "Hello".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await;

    assert!(result.is_err(), "Null byte in args should be rejected");
    let err = result.unwrap_err();
    assert!(
        matches!(err, AwaError::Validation(_)),
        "Expected Validation error, got: {err:?}"
    );
    assert!(
        err.to_string().contains("null bytes"),
        "Error should mention null bytes, got: {err}"
    );
}

#[tokio::test]
async fn test_null_byte_in_metadata_rejected() {
    let client = setup().await;
    let queue = "integ_null_byte_meta";

    let result = insert_with(
        client.pool(),
        &SendEmail {
            to: "alice@example.com".into(),
            subject: "Hello".into(),
        },
        InsertOpts {
            queue: queue.into(),
            metadata: serde_json::json!({"key": "value\x00with_null"}),
            ..Default::default()
        },
    )
    .await;

    assert!(result.is_err(), "Null byte in metadata should be rejected");
    assert!(matches!(result.unwrap_err(), AwaError::Validation(_)));
}
