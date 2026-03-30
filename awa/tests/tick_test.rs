//! Integration tests for the `tick()` stateless maintenance function.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{admin, insert_with, migrations, InsertOpts};
use awa::{JobArgs, JobState};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::OnceLock;
use tokio::sync::Mutex;

fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup() -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");

    migrations::run(&pool).await.expect("Failed to migrate");
    pool
}

async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue");
    sqlx::query("DELETE FROM awa.queue_state_counts WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean state counts");
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct TickTestJob {
    value: i32,
}

#[tokio::test]
async fn tick_promotes_scheduled_jobs() {
    let _lock = test_lock().lock().await;
    let pool = setup().await;
    let queue = "tick_promote_test";
    clean_queue(&pool, queue).await;

    // Insert a job scheduled in the past (should be promoted)
    let opts = InsertOpts {
        queue: queue.to_string(),
        run_at: Some(chrono::Utc::now() - chrono::Duration::seconds(10)),
        ..Default::default()
    };
    insert_with(&pool, &TickTestJob { value: 1 }, opts)
        .await
        .expect("insert failed");

    // Run tick
    let result = admin::tick(&pool).await.expect("tick failed");
    assert!(
        result.promoted_scheduled >= 1,
        "expected at least 1 promotion, got {}",
        result.promoted_scheduled
    );

    // Verify the job is now available in the hot table
    let jobs = admin::list_jobs(
        &pool,
        &admin::ListJobsFilter {
            queue: Some(queue.to_string()),
            state: Some(JobState::Available),
            limit: Some(10),
            ..Default::default()
        },
    )
    .await
    .expect("list failed");
    assert!(
        !jobs.is_empty(),
        "expected available job after tick promotion"
    );
}

#[tokio::test]
async fn tick_rescues_stale_heartbeats() {
    let _lock = test_lock().lock().await;
    let pool = setup().await;
    let queue = "tick_rescue_hb_test";
    clean_queue(&pool, queue).await;

    // Insert a job
    let opts = InsertOpts {
        queue: queue.to_string(),
        ..Default::default()
    };
    let job = insert_with(&pool, &TickTestJob { value: 2 }, opts)
        .await
        .expect("insert failed");

    // Manually transition to running with a very stale heartbeat
    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'running',
            attempt = 1,
            heartbeat_at = now() - interval '5 minutes'
        WHERE id = $1
        "#,
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .expect("failed to set running state");

    // Run tick with short staleness threshold
    let tick_opts = admin::TickOptions {
        heartbeat_staleness_secs: 60, // 1 minute, well under the 5-minute stale heartbeat
        ..Default::default()
    };
    let result = admin::tick_with_options(&pool, tick_opts)
        .await
        .expect("tick failed");
    assert!(
        result.rescued_heartbeat >= 1,
        "expected at least 1 heartbeat rescue"
    );

    // Job should now be retryable
    let updated = admin::get_job(&pool, job.id).await.expect("get failed");
    assert_eq!(
        updated.state,
        JobState::Retryable,
        "expected retryable after rescue"
    );
}

#[tokio::test]
async fn tick_rescues_expired_deadlines() {
    let _lock = test_lock().lock().await;
    let pool = setup().await;
    let queue = "tick_rescue_dl_test";
    clean_queue(&pool, queue).await;

    let opts = InsertOpts {
        queue: queue.to_string(),
        ..Default::default()
    };
    let job = insert_with(&pool, &TickTestJob { value: 3 }, opts)
        .await
        .expect("insert failed");

    // Manually transition to running with an expired deadline
    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'running',
            attempt = 1,
            heartbeat_at = now(),
            deadline_at = now() - interval '1 minute'
        WHERE id = $1
        "#,
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .expect("failed to set running state");

    let result = admin::tick(&pool).await.expect("tick failed");
    assert!(
        result.rescued_deadline >= 1,
        "expected at least 1 deadline rescue"
    );

    let updated = admin::get_job(&pool, job.id).await.expect("get failed");
    assert_eq!(
        updated.state,
        JobState::Retryable,
        "expected retryable after deadline rescue"
    );
}

#[tokio::test]
async fn tick_rescues_expired_callbacks() {
    let _lock = test_lock().lock().await;
    let pool = setup().await;
    let queue = "tick_rescue_cb_test";
    clean_queue(&pool, queue).await;

    let opts = InsertOpts {
        queue: queue.to_string(),
        ..Default::default()
    };
    let job = insert_with(&pool, &TickTestJob { value: 4 }, opts)
        .await
        .expect("insert failed");

    // Manually transition to waiting_external with expired callback timeout
    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'waiting_external',
            attempt = 1,
            callback_id = gen_random_uuid(),
            callback_timeout_at = now() - interval '1 minute'
        WHERE id = $1
        "#,
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .expect("failed to set waiting_external state");

    let result = admin::tick(&pool).await.expect("tick failed");
    assert!(
        result.rescued_callback >= 1,
        "expected at least 1 callback rescue"
    );

    let updated = admin::get_job(&pool, job.id).await.expect("get failed");
    assert!(
        updated.state == JobState::Retryable || updated.state == JobState::Failed,
        "expected retryable or failed after callback rescue, got {:?}",
        updated.state,
    );
}

#[tokio::test]
async fn tick_noop_when_nothing_to_do() {
    let _lock = test_lock().lock().await;
    let pool = setup().await;
    let queue = "tick_noop_test";
    clean_queue(&pool, queue).await;

    let result = admin::tick(&pool).await.expect("tick failed");
    assert_eq!(result.promoted_scheduled, 0);
    assert_eq!(result.promoted_retryable, 0);
    assert_eq!(result.rescued_heartbeat, 0);
    assert_eq!(result.rescued_deadline, 0);
    assert_eq!(result.rescued_callback, 0);
}
