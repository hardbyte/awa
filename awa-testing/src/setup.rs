//! Common test setup utilities for Awa integration tests.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;

/// Default database URL for test runs.
pub fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

/// Database URL with a custom application_name parameter appended.
pub fn database_url_with_app_name(app_name: &str) -> String {
    let mut url = database_url();
    let sep = if url.contains('?') { '&' } else { '?' };
    url.push(sep);
    url.push_str("application_name=");
    url.push_str(app_name);
    url
}

/// Create a connection pool.
pub async fn pool(max_connections: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database")
}

/// Create a connection pool with a custom database URL.
pub async fn pool_with_url(url: &str, max_connections: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await
        .expect("Failed to connect to database")
}

/// Create a pool, run migrations, and return it.
pub async fn setup(max_connections: u32) -> PgPool {
    let pool = pool(max_connections).await;
    awa_model::migrations::run(&pool)
        .await
        .expect("Failed to run migrations");
    reset_runtime_backend(&pool).await;
    pool
}

/// Initialise the active storage engine to the one selected by the
/// `AWA_TEST_ENGINE` env var: `canonical` (default) or `queue_storage`. awa's
/// caller-facing contract is engine-invariant, so running the same suite under
/// both values exercises both backends. Tests that pin a specific engine call
/// [`reset_to_canonical`] / [`activate_queue_storage`] directly.
pub async fn reset_runtime_backend(pool: &PgPool) {
    match std::env::var("AWA_TEST_ENGINE").as_deref() {
        Ok("queue_storage") => activate_queue_storage(pool).await,
        _ => reset_to_canonical(pool).await,
    }
}

/// Force the canonical engine and clear any queue-storage runtime registration.
pub async fn reset_to_canonical(pool: &PgPool) {
    let mut tx = pool
        .begin()
        .await
        .expect("Failed to start runtime backend reset transaction");

    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET current_engine = 'canonical',
            prepared_engine = NULL,
            state = 'canonical',
            transition_epoch = transition_epoch + 1,
            details = '{}'::jsonb,
            updated_at = now(),
            finalized_at = NULL
        WHERE singleton
        "#,
    )
    .execute(&mut *tx)
    .await
    .expect("Failed to reset storage transition state for test setup");
    sqlx::query("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        .execute(&mut *tx)
        .await
        .expect("Failed to reset active runtime backend for test setup");
    sqlx::query("DELETE FROM awa.runtime_instances")
        .execute(&mut *tx)
        .await
        .expect("Failed to reset runtime instances for test setup");
    tx.commit()
        .await
        .expect("Failed to commit runtime backend reset transaction");
}

/// Activate the queue-storage engine against the substrate installed in the
/// `awa` schema (mirrors a fresh-install auto-finalize).
pub async fn activate_queue_storage(pool: &PgPool) {
    let mut tx = pool
        .begin()
        .await
        .expect("Failed to start queue-storage activation transaction");

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
    .expect("Failed to activate queue-storage transition state for test setup");
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
    .expect("Failed to register queue-storage backend for test setup");
    sqlx::query("DELETE FROM awa.runtime_instances")
        .execute(&mut *tx)
        .await
        .expect("Failed to reset runtime instances for test setup");
    tx.commit()
        .await
        .expect("Failed to commit queue-storage activation transaction");
}

/// The storage engine the suite is running against, selected by `AWA_TEST_ENGINE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestEngine {
    Canonical,
    QueueStorage,
}

/// The engine selected for this test run (`canonical` unless `AWA_TEST_ENGINE=queue_storage`).
pub fn test_engine() -> TestEngine {
    match std::env::var("AWA_TEST_ENGINE").as_deref() {
        Ok("queue_storage") => TestEngine::QueueStorage,
        _ => TestEngine::Canonical,
    }
}

/// Guard for tests whose assertions are specific to the canonical engine —
/// e.g. admin-metadata caches maintained by canonical row triggers, or the
/// canonical running-cancel notification. Returns `true` (with a skip notice)
/// when the run is under another engine, so the test can early-return:
///
/// ```ignore
/// if awa_testing::setup::skip_unless_canonical("my_test") { return; }
/// ```
pub fn skip_unless_canonical(test: &str) -> bool {
    if test_engine() != TestEngine::Canonical {
        eprintln!(
            "[skip] {test}: canonical-only assertions; running under AWA_TEST_ENGINE=queue_storage"
        );
        true
    } else {
        false
    }
}

/// Delete all jobs, queue metadata, and admin caches for a specific queue.
///
/// Explicitly deletes the `queue_state_counts` row to prevent accumulated
/// cache drift from affecting assertions. The DELETE trigger normally handles
/// this, but concurrent test runs against a shared DB can cause small delta
/// errors that compound over time.
pub async fn clean_queue(pool: &PgPool, queue: &str) {
    match awa_model::queue_storage::QueueStorage::active_schema(pool)
        .await
        .expect("active_schema lookup for clean_queue")
    {
        Some(schema) => clean_queue_substrate(pool, &schema, queue).await,
        None => {
            sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
                .bind(queue)
                .execute(pool)
                .await
                .expect("Failed to clean queue jobs");
        }
    }
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

/// Drain every job-bearing plane of the queue-storage substrate for one queue
/// and release its unique claims, so a queue name can be reused across tests
/// and re-runs against the same database. The canonical `awa.jobs` view only
/// surfaces a queue's head under queue storage, so a single view `DELETE`
/// leaves the rest of the lane (and its `job_unique_claims`) behind.
async fn clean_queue_substrate(pool: &PgPool, schema: &str, queue: &str) {
    // Release unique claims first, while the rows that carry the job ids exist.
    sqlx::query(&format!(
        "DELETE FROM awa.job_unique_claims WHERE job_id IN (
             SELECT job_id FROM {schema}.ready_entries WHERE queue = $1
             UNION SELECT job_id FROM {schema}.deferred_jobs WHERE queue = $1
             UNION SELECT job_id FROM {schema}.leases WHERE queue = $1
             UNION SELECT job_id FROM {schema}.done_entries WHERE queue = $1
             UNION SELECT job_id FROM {schema}.dlq_entries WHERE queue = $1)"
    ))
    .bind(queue)
    .execute(pool)
    .await
    .expect("Failed to release queue unique claims");

    for plane in [
        "ready_entries",
        "deferred_jobs",
        "leases",
        "done_entries",
        "dlq_entries",
    ] {
        sqlx::query(&format!("DELETE FROM {schema}.{plane} WHERE queue = $1"))
            .bind(queue)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("Failed to clean {plane} for queue {queue}: {err}"));
    }
}

/// Query job state counts for a queue, returning a map of state -> count.
pub async fn queue_state_counts(pool: &PgPool, queue: &str) -> HashMap<String, i64> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"
        SELECT state::text, count(*)::bigint
        FROM awa.jobs
        WHERE queue = $1
        GROUP BY state
        "#,
    )
    .bind(queue)
    .fetch_all(pool)
    .await
    .expect("Failed to query state counts");

    rows.into_iter().collect()
}

/// Extract a count for a given state from a state-counts map.
pub fn state_count(counts: &HashMap<String, i64>, state: &str) -> i64 {
    counts.get(state).copied().unwrap_or(0)
}

/// Poll queue state counts until a predicate is satisfied, or panic on timeout.
pub async fn wait_for_counts(
    pool: &PgPool,
    queue: &str,
    predicate: impl Fn(&HashMap<String, i64>) -> bool,
    timeout: Duration,
) -> HashMap<String, i64> {
    let start = std::time::Instant::now();
    loop {
        let counts = queue_state_counts(pool, queue).await;
        if predicate(&counts) {
            return counts;
        }
        assert!(
            start.elapsed() < timeout,
            "Timed out waiting for queue {queue} counts; last counts: {counts:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
