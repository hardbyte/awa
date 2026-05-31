//! ADR-029 integration tests: durable follow-up jobs enqueued in the same
//! transaction as a triggering state commit.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{admin, migrations};
use awa::{Client, JobArgs, JobResult, JobState, QueueConfig};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::Semaphore;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup_pool_canonical() -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url())
        .await
        .expect("connect to test database");
    sqlx::query("DROP SCHEMA IF EXISTS awa CASCADE")
        .execute(&pool)
        .await
        .expect("drop awa schema");
    migrations::run(&pool).await.expect("run migrations");
    pool
}

fn test_gate() -> Arc<Semaphore> {
    static GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    GATE.get_or_init(|| Arc::new(Semaphore::new(1))).clone()
}

async fn wait_for_state(pool: &sqlx::PgPool, job_id: i64, state: JobState) -> awa::JobRow {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(job) = admin::get_job(pool, job_id).await {
            if job.state == state {
                return job;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for job {job_id} to reach {state:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn first_follow_up(pool: &sqlx::PgPool, kind: &str) -> Option<awa::JobRow> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row: Option<awa::JobRow> =
            sqlx::query_as("SELECT * FROM awa.jobs WHERE kind = $1 ORDER BY id ASC LIMIT 1")
                .bind(kind)
                .fetch_optional(pool)
                .await
                .expect("query follow-up");
        if row.is_some() {
            return row;
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct EnqueueTrigger {
    user_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct EnqueueFollowUp {
    user_id: i64,
    triggered_by_job_id: i64,
}

#[tokio::test]
async fn on_completed_enqueue_inserts_follow_up_atomically() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    let queue = "enqueue_spec_trigger";

    let client = Client::builder(pool.clone())
        .canonical_storage()
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<EnqueueTrigger, _, _>(|_args, _ctx| async move { Ok(JobResult::Completed) })
        // The spec under test: when EnqueueTrigger completes, enqueue an
        // EnqueueFollowUp in the same transaction.
        .on_completed_enqueue::<EnqueueTrigger, EnqueueFollowUp, _>(|args, job| EnqueueFollowUp {
            user_id: args.user_id,
            triggered_by_job_id: job.id,
        })
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &EnqueueTrigger { user_id: 42 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let completed = wait_for_state(&pool, trigger_job.id, JobState::Completed).await;
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("follow-up job should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(completed.state, JobState::Completed);

    // Decode the follow-up's args and check the make-args closure ran with
    // the trigger's args and post-completion JobRow.
    let follow_args: EnqueueFollowUp =
        serde_json::from_value(follow_up.args.clone()).expect("decode follow-up args");
    assert_eq!(follow_args.user_id, 42);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
    assert_eq!(follow_up.state, JobState::Available);
}

#[tokio::test]
async fn on_completed_enqueue_under_queue_storage_fails_fast() {
    use awa::model::queue_storage::{QueueStorage, QueueStorageConfig};

    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    QueueStorage::new(QueueStorageConfig::default())
        .expect("build queue storage")
        .install(&pool)
        .await
        .expect("install queue storage");

    let client = Client::builder(pool.clone())
        .queue(
            "enqueue_spec_queue_storage",
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<EnqueueTrigger, _, _>(|_args, _ctx| async move { Ok(JobResult::Completed) })
        .on_completed_enqueue::<EnqueueTrigger, EnqueueFollowUp, _>(|args, _job| EnqueueFollowUp {
            user_id: args.user_id,
            triggered_by_job_id: 0,
        })
        .build()
        .unwrap();

    // ADR-029 queue-storage wiring is a deferred follow-up. Until it lands,
    // start() must refuse loud so callers can't ship code that silently
    // never enqueues follow-ups.
    let err = client.start().await.expect_err("start should fail fast");
    let msg = err.to_string();
    assert!(
        msg.contains("queue storage") && msg.contains("canonical_storage"),
        "unexpected error message: {msg}"
    );
}
