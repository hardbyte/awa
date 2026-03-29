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
    pool
}

/// Delete all jobs, queue metadata, and admin caches for a specific queue.
///
/// Explicitly deletes the `queue_state_counts` row to prevent accumulated
/// cache drift from affecting assertions. The DELETE trigger normally handles
/// this, but concurrent test runs against a shared DB can cause small delta
/// errors that compound over time.
pub async fn clean_queue(pool: &PgPool, queue: &str) {
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
