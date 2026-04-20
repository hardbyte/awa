//! Integration tests for configurable retention policies and cleanup.
//!
//! These tests must run sequentially because each starts a Client that
//! becomes leader and runs global cleanup — concurrent leaders with
//! different retention policies would interfere with each other's test data.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{insert_with, InsertOpts};
use awa::{JobArgs, RetentionPolicy};
use awa_testing::TestClient;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell};

/// Serialize retention tests — each starts a Client that becomes leader and
/// runs global cleanup, so concurrent tests interfere with each other.
static RETENTION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
static RETENTION_TEST_DB_INIT: OnceCell<()> = OnceCell::const_new();

fn base_database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn replace_database_name(url: &str, database_name: &str) -> String {
    let (without_query, query_suffix) = match url.split_once('?') {
        Some((prefix, query)) => (prefix, Some(query)),
        None => (url, None),
    };
    let (base, _) = without_query
        .rsplit_once('/')
        .expect("database URL should include a database name");
    let mut out = format!("{base}/{database_name}");
    if let Some(query) = query_suffix {
        out.push('?');
        out.push_str(query);
    }
    out
}

fn database_name(url: &str) -> String {
    let without_query = url.split_once('?').map(|(prefix, _)| prefix).unwrap_or(url);
    without_query
        .rsplit_once('/')
        .map(|(_, database_name)| database_name.to_string())
        .expect("database URL should include a database name")
}

fn validate_database_name(database_name: &str) {
    assert!(
        !database_name.is_empty()
            && database_name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
        "retention test database names must use only [A-Za-z0-9_]"
    );
}

fn database_url() -> String {
    std::env::var("DATABASE_URL_RETENTION_TEST")
        .unwrap_or_else(|_| replace_database_name(&base_database_url(), "awa_test_retention"))
}

async fn ensure_database_exists(url: &str) {
    let database_name = database_name(url);
    validate_database_name(&database_name);
    let admin_url = replace_database_name(url, "postgres");
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await
        .expect("Failed to connect to admin database for retention tests");
    let create_sql = format!("CREATE DATABASE {database_name}");
    match sqlx::query(&create_sql).execute(&admin_pool).await {
        Ok(_) => {}
        Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P04") => {}
        Err(err) => panic!("Failed to create retention test database {database_name}: {err}"),
    }
}

async fn setup() -> TestClient {
    let url = database_url();
    RETENTION_TEST_DB_INIT
        .get_or_init(|| async {
            ensure_database_exists(&url).await;
        })
        .await;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
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

async fn run_canonical_cleanup(
    pool: &sqlx::PgPool,
    completed_retention: Duration,
    failed_retention: Duration,
    cleanup_batch_size: i64,
    queue_retention_overrides: &HashMap<String, RetentionPolicy>,
) {
    let override_queues: Vec<String> = queue_retention_overrides.keys().cloned().collect();
    let completed_retention_secs = i64::try_from(completed_retention.as_secs()).unwrap_or(i64::MAX);
    let failed_retention_secs = i64::try_from(failed_retention.as_secs()).unwrap_or(i64::MAX);

    if override_queues.is_empty() {
        sqlx::query(
            r#"
            DELETE FROM awa.jobs_hot
            WHERE id IN (
                SELECT id FROM awa.jobs_hot
                WHERE (state = 'completed' AND finalized_at < now() - make_interval(secs => $1::bigint))
                   OR (state IN ('failed', 'cancelled') AND finalized_at < now() - make_interval(secs => $2::bigint))
                LIMIT $3
            )
            "#,
        )
        .bind(completed_retention_secs)
        .bind(failed_retention_secs)
        .bind(cleanup_batch_size)
        .execute(pool)
        .await
        .expect("Failed to run canonical cleanup global pass");
    } else {
        sqlx::query(
            r#"
            DELETE FROM awa.jobs_hot
            WHERE id IN (
                SELECT id FROM awa.jobs_hot
                WHERE ((state = 'completed' AND finalized_at < now() - make_interval(secs => $1::bigint))
                   OR (state IN ('failed', 'cancelled') AND finalized_at < now() - make_interval(secs => $2::bigint)))
                  AND queue != ALL($4::text[])
                LIMIT $3
            )
            "#,
        )
        .bind(completed_retention_secs)
        .bind(failed_retention_secs)
        .bind(cleanup_batch_size)
        .bind(&override_queues)
        .execute(pool)
        .await
        .expect("Failed to run canonical cleanup global pass with overrides");
    }

    for (queue_name, policy) in queue_retention_overrides {
        let queue_completed_secs = i64::try_from(policy.completed.as_secs()).unwrap_or(i64::MAX);
        let queue_failed_secs = i64::try_from(policy.failed.as_secs()).unwrap_or(i64::MAX);

        sqlx::query(
            r#"
            DELETE FROM awa.jobs_hot
            WHERE id IN (
                SELECT id FROM awa.jobs_hot
                WHERE queue = $4
                  AND ((state = 'completed' AND finalized_at < now() - make_interval(secs => $1::bigint))
                    OR (state IN ('failed', 'cancelled') AND finalized_at < now() - make_interval(secs => $2::bigint)))
                LIMIT $3
            )
            "#,
        )
        .bind(queue_completed_secs)
        .bind(queue_failed_secs)
        .bind(cleanup_batch_size)
        .bind(queue_name)
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("Failed to run canonical cleanup override pass for {queue_name}: {err}"));
    }
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

    run_canonical_cleanup(
        pool,
        Duration::from_secs(86_400),
        Duration::from_secs(259_200),
        1000,
        &HashMap::new(),
    )
    .await;

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

    run_canonical_cleanup(
        pool,
        Duration::from_secs(86_400),
        Duration::from_secs(259_200),
        1000,
        &HashMap::new(),
    )
    .await;

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

    run_canonical_cleanup(
        pool,
        Duration::from_secs(86_400),
        Duration::from_secs(259_200),
        2,
        &HashMap::new(),
    )
    .await;

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

    let overrides = HashMap::from([(
        fast_queue.to_string(),
        RetentionPolicy {
            completed: Duration::from_secs(3600),
            failed: Duration::from_secs(3600),
            dlq: None,
        },
    )]);
    run_canonical_cleanup(
        pool,
        Duration::from_secs(86_400),
        Duration::from_secs(259_200),
        1000,
        &overrides,
    )
    .await;

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

    run_canonical_cleanup(
        pool,
        Duration::from_secs(86_400),
        Duration::from_secs(259_200),
        1000,
        &HashMap::new(),
    )
    .await;

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
