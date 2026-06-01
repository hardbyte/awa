//! ADR-029 integration tests: durable follow-up jobs enqueued in the same
//! transaction as a triggering state commit.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{admin, migrations};
use awa::{Client, EnqueueRequest, JobArgs, JobResult, JobState, QueueConfig};
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

/// A panicking `on_*_enqueue` closure must not unwind the executor or
/// leave the trigger committed when the spec dispatch ran in the
/// finalization transaction. The dispatch catches the panic, converts
/// it to an AwaError, rolls back the tx (so the trigger stays in
/// `running` and the heartbeat-rescue will pick it up), and the worker
/// keeps draining other jobs.
#[tokio::test]
async fn panicking_on_completed_enqueue_closure_is_caught_and_rolls_back() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    let queue = "enqueue_spec_panic_trigger";

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
        .on_completed_enqueue::<EnqueueTrigger, EnqueueFollowUp, _>(|_args, _job| {
            panic!("user closure exploded");
        })
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &EnqueueTrigger { user_id: 999 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    // Give the executor enough time to claim, perform, and attempt
    // completion (which rolls back due to the panic).
    tokio::time::sleep(Duration::from_millis(500)).await;
    client.shutdown(Duration::from_secs(2)).await;

    let trigger_after = admin::get_job(&pool, trigger_job.id).await.unwrap();
    // Trigger must not be `completed` — the completion tx rolled back.
    assert_ne!(
        trigger_after.state,
        JobState::Completed,
        "panic in on_completed_enqueue closure leaked through and committed the trigger"
    );
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind()).await;
    assert!(
        follow_up.is_none(),
        "panicking closure must not have inserted a follow-up"
    );
}

#[tokio::test]
async fn on_completed_enqueue_routes_follow_up_to_custom_queue() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    let trigger_queue = "enqueue_spec_custom_trigger";
    let follow_queue = "enqueue_spec_custom_follow";

    let follow_queue_for_closure = follow_queue.to_string();
    let client = Client::builder(pool.clone())
        .canonical_storage()
        .queue(
            trigger_queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<EnqueueTrigger, _, _>(|_args, _ctx| async move { Ok(JobResult::Completed) })
        .on_completed_enqueue_with::<EnqueueTrigger, EnqueueFollowUp, _>(move |args, job| {
            EnqueueRequest::new(EnqueueFollowUp {
                user_id: args.user_id,
                triggered_by_job_id: job.id,
            })
            .queue(follow_queue_for_closure.clone())
            .priority(1)
        })
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &EnqueueTrigger { user_id: 7 },
        awa::InsertOpts {
            queue: trigger_queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    wait_for_state(&pool, trigger_job.id, JobState::Completed).await;
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("follow-up job should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(follow_up.queue, follow_queue);
    assert_eq!(follow_up.priority, 1);
}

async fn setup_pool_queue_storage() -> sqlx::PgPool {
    use awa::model::queue_storage::{QueueStorage, QueueStorageConfig};
    let pool = setup_pool_canonical().await;
    QueueStorage::new(QueueStorageConfig::default())
        .expect("build queue storage")
        .install(&pool)
        .await
        .expect("install queue storage");
    pool
}

#[tokio::test]
async fn on_cancelled_enqueue_inserts_follow_up_atomically() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    let queue = "enqueue_spec_cancel_trigger";

    let client = Client::builder(pool.clone())
        .canonical_storage()
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<EnqueueTrigger, _, _>(|_args, _ctx| async move {
            Ok(JobResult::Cancel("user requested".to_string()))
        })
        .on_cancelled_enqueue::<EnqueueTrigger, EnqueueFollowUp, _>(|args, job, _reason| {
            EnqueueFollowUp {
                user_id: args.user_id,
                triggered_by_job_id: job.id,
            }
        })
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &EnqueueTrigger { user_id: 11 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    wait_for_state(&pool, trigger_job.id, JobState::Cancelled).await;
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("cancelled follow-up should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp = serde_json::from_value(follow_up.args.clone()).unwrap();
    assert_eq!(follow_args.user_id, 11);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct ExhaustTrigger {
    user_id: i64,
}

#[tokio::test]
async fn on_exhausted_enqueue_inserts_follow_up_atomically() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    let queue = "enqueue_spec_exhaust_trigger";

    let client = Client::builder(pool.clone())
        .canonical_storage()
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<ExhaustTrigger, _, _>(|_args, _ctx| async move {
            Err(awa::JobError::Terminal("permanent failure".to_string()))
        })
        .on_exhausted_enqueue::<ExhaustTrigger, EnqueueFollowUp, _>(
            |args, job, _error, _attempt| EnqueueFollowUp {
                user_id: args.user_id,
                triggered_by_job_id: job.id,
            },
        )
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &ExhaustTrigger { user_id: 22 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    wait_for_state(&pool, trigger_job.id, JobState::Failed).await;
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("exhausted follow-up should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp = serde_json::from_value(follow_up.args.clone()).unwrap();
    assert_eq!(follow_args.user_id, 22);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct RetryTrigger {
    user_id: i64,
}

#[tokio::test]
async fn on_retried_enqueue_inserts_follow_up_atomically() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    let queue = "enqueue_spec_retry_trigger";

    let client = Client::builder(pool.clone())
        .canonical_storage()
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        // Always fail with retryable; max_attempts = 1 means the first
        // failure should immediately Exhaust, so we set max_attempts = 5 to
        // get a clean Retried event on the first run.
        .register::<RetryTrigger, _, _>(|_args, _ctx| async move {
            Err(awa::JobError::Retryable("flaky".into()))
        })
        .on_retried_enqueue::<RetryTrigger, EnqueueFollowUp, _>(
            |args, job, _error, _attempt, _next_run_at| EnqueueFollowUp {
                user_id: args.user_id,
                triggered_by_job_id: job.id,
            },
        )
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &RetryTrigger { user_id: 33 },
        awa::InsertOpts {
            queue: queue.to_string(),
            max_attempts: 5,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    // The trigger may cycle through Retryable multiple times. We just need
    // at least one follow-up to land.
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("retried follow-up should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp = serde_json::from_value(follow_up.args.clone()).unwrap();
    assert_eq!(follow_args.user_id, 33);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct WaitTrigger {
    user_id: i64,
}

struct WaitWorker;

#[async_trait::async_trait]
impl awa::Worker for WaitWorker {
    fn kind(&self) -> &'static str {
        WaitTrigger::kind()
    }
    async fn perform(&self, ctx: &awa::JobContext) -> Result<awa::JobResult, awa::JobError> {
        let guard = ctx
            .register_callback(Duration::from_secs(60))
            .await
            .map_err(awa::JobError::retryable)?;
        Ok(JobResult::WaitForCallback(guard))
    }
}

#[tokio::test]
async fn on_waiting_for_callback_enqueue_inserts_follow_up_atomically() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    let queue = "enqueue_spec_wait_trigger";

    let seen_callback_id: Arc<std::sync::Mutex<Option<uuid::Uuid>>> =
        Arc::new(std::sync::Mutex::new(None));
    let seen_callback_id_for_closure = seen_callback_id.clone();

    let client = Client::builder(pool.clone())
        .canonical_storage()
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register_worker(WaitWorker)
        .on_waiting_for_callback_enqueue::<WaitTrigger, EnqueueFollowUp, _>(move |args, job| {
            *seen_callback_id_for_closure.lock().unwrap() = job.callback_id;
            EnqueueFollowUp {
                user_id: args.user_id,
                triggered_by_job_id: job.id,
            }
        })
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &WaitTrigger { user_id: 44 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    wait_for_state(&pool, trigger_job.id, JobState::WaitingExternal).await;
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("waiting-for-callback follow-up should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp = serde_json::from_value(follow_up.args.clone()).unwrap();
    assert_eq!(follow_args.user_id, 44);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
    let captured = seen_callback_id
        .lock()
        .unwrap()
        .expect("closure should have observed a callback_id on the parked JobRow");
    let parked = admin::get_job(&pool, trigger_job.id).await.unwrap();
    assert_eq!(Some(captured), parked.callback_id);
}

#[tokio::test]
async fn on_completed_enqueue_inserts_follow_up_under_queue_storage() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_queue_storage().await;
    let queue = "enqueue_spec_qs_trigger";

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<EnqueueTrigger, _, _>(|_args, _ctx| async move { Ok(JobResult::Completed) })
        .on_completed_enqueue::<EnqueueTrigger, EnqueueFollowUp, _>(|args, job| EnqueueFollowUp {
            user_id: args.user_id,
            triggered_by_job_id: job.id,
        })
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &EnqueueTrigger { user_id: 99 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    wait_for_state(&pool, trigger_job.id, JobState::Completed).await;
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("follow-up should be enqueued under queue storage");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp =
        serde_json::from_value(follow_up.args.clone()).expect("decode follow-up args");
    assert_eq!(follow_args.user_id, 99);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
}

#[tokio::test]
async fn on_cancelled_enqueue_inserts_follow_up_under_queue_storage() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_queue_storage().await;
    let queue = "enqueue_spec_qs_cancel_trigger";

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<EnqueueTrigger, _, _>(|_args, _ctx| async move {
            Ok(JobResult::Cancel("user requested".to_string()))
        })
        .on_cancelled_enqueue::<EnqueueTrigger, EnqueueFollowUp, _>(|args, job, _reason| {
            EnqueueFollowUp {
                user_id: args.user_id,
                triggered_by_job_id: job.id,
            }
        })
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &EnqueueTrigger { user_id: 111 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    wait_for_state(&pool, trigger_job.id, JobState::Cancelled).await;
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("queue-storage cancelled follow-up should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp = serde_json::from_value(follow_up.args.clone()).unwrap();
    assert_eq!(follow_args.user_id, 111);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
}

#[tokio::test]
async fn on_exhausted_enqueue_inserts_follow_up_under_queue_storage() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_queue_storage().await;
    let queue = "enqueue_spec_qs_exhaust_trigger";

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<ExhaustTrigger, _, _>(|_args, _ctx| async move {
            Err(awa::JobError::Terminal("permanent failure".to_string()))
        })
        .on_exhausted_enqueue::<ExhaustTrigger, EnqueueFollowUp, _>(
            |args, job, _error, _attempt| EnqueueFollowUp {
                user_id: args.user_id,
                triggered_by_job_id: job.id,
            },
        )
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &ExhaustTrigger { user_id: 222 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    wait_for_state(&pool, trigger_job.id, JobState::Failed).await;
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("queue-storage exhausted follow-up should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp = serde_json::from_value(follow_up.args.clone()).unwrap();
    assert_eq!(follow_args.user_id, 222);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
}

#[tokio::test]
async fn on_retried_enqueue_inserts_follow_up_under_queue_storage() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_queue_storage().await;
    let queue = "enqueue_spec_qs_retry_trigger";

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<RetryTrigger, _, _>(|_args, _ctx| async move {
            Err(awa::JobError::Retryable("flaky".into()))
        })
        .on_retried_enqueue::<RetryTrigger, EnqueueFollowUp, _>(
            |args, job, _error, _attempt, _next_run_at| EnqueueFollowUp {
                user_id: args.user_id,
                triggered_by_job_id: job.id,
            },
        )
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &RetryTrigger { user_id: 333 },
        awa::InsertOpts {
            queue: queue.to_string(),
            max_attempts: 5,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("queue-storage retried follow-up should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp = serde_json::from_value(follow_up.args.clone()).unwrap();
    assert_eq!(follow_args.user_id, 333);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
}

#[tokio::test]
async fn on_waiting_for_callback_enqueue_inserts_follow_up_under_queue_storage() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_queue_storage().await;
    let queue = "enqueue_spec_qs_wait_trigger";

    // Capture the parked JobRow the closure sees so we can assert that
    // `register_callback()`'s writes (callback_id, callback_timeout_at)
    // are visible — the dispatcher must reload the row from the leases
    // table after `enter_callback_wait_in_tx`, not reuse the pre-park
    // snapshot the executor claimed.
    let seen_callback_id: Arc<std::sync::Mutex<Option<uuid::Uuid>>> =
        Arc::new(std::sync::Mutex::new(None));
    let seen_callback_id_for_closure = seen_callback_id.clone();

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register_worker(WaitWorker)
        .on_waiting_for_callback_enqueue::<WaitTrigger, EnqueueFollowUp, _>(move |args, job| {
            *seen_callback_id_for_closure.lock().unwrap() = job.callback_id;
            EnqueueFollowUp {
                user_id: args.user_id,
                triggered_by_job_id: job.id,
            }
        })
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &WaitTrigger { user_id: 444 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    wait_for_state(&pool, trigger_job.id, JobState::WaitingExternal).await;
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("queue-storage waiting-for-callback follow-up should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp = serde_json::from_value(follow_up.args.clone()).unwrap();
    assert_eq!(follow_args.user_id, 444);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
    let captured = seen_callback_id
        .lock()
        .unwrap()
        .expect("closure should have observed a callback_id on the parked JobRow");
    let parked = admin::get_job(&pool, trigger_job.id).await.unwrap();
    assert_eq!(
        Some(captured),
        parked.callback_id,
        "closure-visible callback_id must match the one written by register_callback()"
    );
}

#[tokio::test]
async fn complete_external_dispatches_completed_followup() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    let queue = "enqueue_spec_cb_complete";

    let client = Client::builder(pool.clone())
        .canonical_storage()
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register_worker(WaitWorker)
        .on_completed_enqueue::<WaitTrigger, EnqueueFollowUp, _>(|args, job| EnqueueFollowUp {
            user_id: args.user_id,
            triggered_by_job_id: job.id,
        })
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &WaitTrigger { user_id: 501 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let parked = wait_for_state(&pool, trigger_job.id, JobState::WaitingExternal).await;
    let callback_id = parked.callback_id.expect("callback id set on parked job");

    let _completed = client
        .complete_external(callback_id, None, None)
        .await
        .expect("complete_external");

    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("callback Completed follow-up should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp = serde_json::from_value(follow_up.args.clone()).unwrap();
    assert_eq!(follow_args.user_id, 501);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
}

#[tokio::test]
async fn fail_external_dispatches_exhausted_followup() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    let queue = "enqueue_spec_cb_fail";

    let client = Client::builder(pool.clone())
        .canonical_storage()
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register_worker(WaitWorker)
        .on_exhausted_enqueue::<WaitTrigger, EnqueueFollowUp, _>(|args, job, _error, _attempt| {
            EnqueueFollowUp {
                user_id: args.user_id,
                triggered_by_job_id: job.id,
            }
        })
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &WaitTrigger { user_id: 502 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let parked = wait_for_state(&pool, trigger_job.id, JobState::WaitingExternal).await;
    let callback_id = parked.callback_id.expect("callback id");

    let _failed = client
        .fail_external(callback_id, "external rejected", None)
        .await
        .expect("fail_external");

    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("callback Exhausted follow-up should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp = serde_json::from_value(follow_up.args.clone()).unwrap();
    assert_eq!(follow_args.user_id, 502);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
}

#[tokio::test]
async fn retry_external_dispatches_retried_followup() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    let queue = "enqueue_spec_cb_retry";

    let client = Client::builder(pool.clone())
        .canonical_storage()
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register_worker(WaitWorker)
        .on_retried_enqueue::<WaitTrigger, EnqueueFollowUp, _>(
            |args, job, _error, _attempt, _next_run_at| EnqueueFollowUp {
                user_id: args.user_id,
                triggered_by_job_id: job.id,
            },
        )
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &WaitTrigger { user_id: 503 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let parked = wait_for_state(&pool, trigger_job.id, JobState::WaitingExternal).await;
    let callback_id = parked.callback_id.expect("callback id");

    let _retried = client
        .retry_external(callback_id, None)
        .await
        .expect("retry_external");

    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("callback Retried follow-up should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp = serde_json::from_value(follow_up.args.clone()).unwrap();
    assert_eq!(follow_args.user_id, 503);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct ExpiringTrigger {
    user_id: i64,
}

struct ShortCallbackWorker;

#[async_trait::async_trait]
impl awa::Worker for ShortCallbackWorker {
    fn kind(&self) -> &'static str {
        ExpiringTrigger::kind()
    }
    async fn perform(&self, ctx: &awa::JobContext) -> Result<awa::JobResult, awa::JobError> {
        // Very short callback timeout so maintenance rescues it quickly.
        let guard = ctx
            .register_callback(Duration::from_millis(100))
            .await
            .map_err(awa::JobError::retryable)?;
        Ok(JobResult::WaitForCallback(guard))
    }
}

#[tokio::test]
async fn on_rescued_enqueue_dispatches_followup_on_expired_callback() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    let queue = "enqueue_spec_rescued_trigger";

    let client = Client::builder(pool.clone())
        .canonical_storage()
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .callback_rescue_interval(Duration::from_millis(100))
        .register_worker(ShortCallbackWorker)
        .on_rescued_enqueue::<ExpiringTrigger, EnqueueFollowUp, _>(|args, job, _reason| {
            EnqueueFollowUp {
                user_id: args.user_id,
                triggered_by_job_id: job.id,
            }
        })
        .build()
        .unwrap();

    let trigger_job = awa::insert_with(
        &pool,
        &ExpiringTrigger { user_id: 601 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let follow_up = first_follow_up(&pool, EnqueueFollowUp::kind())
        .await
        .expect("rescued follow-up should be enqueued");
    client.shutdown(Duration::from_secs(2)).await;

    let follow_args: EnqueueFollowUp = serde_json::from_value(follow_up.args.clone()).unwrap();
    assert_eq!(follow_args.user_id, 601);
    assert_eq!(follow_args.triggered_by_job_id, trigger_job.id);
}
