//! ADR-038 hot-reload regression: per-queue runtime overrides apply to a
//! RUNNING worker (no restart), and clearing them reverts to the builder
//! configuration. The per-attempt deadline is the observable: on the
//! canonical engine `deadline_at - attempted_at` of a claimed job reads the
//! effective `deadline_duration` back out of the database.
//!
//! Sets `AWA_OVERRIDE_REFRESH_MS` before building the client so the test
//! pins the behavior, not the production 10s cadence.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::{Client, JobArgs, JobResult, QueueConfig};
use awa_model::admin::{
    clear_queue_runtime_overrides, set_queue_runtime_overrides, QueueRuntimeOverrides,
};
use awa_model::{insert_with, migrations, InsertOpts};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

const QUEUE: &str = "override_live";
const BASE_DEADLINE_SECS: u64 = 30;
const OVERRIDE_DEADLINE_MS: i64 = 5_000;

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct ParkedJob {
    pub n: i64,
}

async fn setup() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&database_url())
        .await
        .expect("Failed to connect — is Postgres running?");
    migrations::run(&pool).await.expect("Failed to migrate");
    awa_testing::setup::reset_runtime_backend(&pool).await;
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(QUEUE)
        .execute(&pool)
        .await
        .expect("queue cleanup");
    clear_queue_runtime_overrides(&pool, QUEUE)
        .await
        .expect("override cleanup");
    // Pin the canonical engine before the worker starts: on an empty, quiet
    // database `client.start()` auto-finalizes straight to queue storage
    // (`awa.storage_auto_finalize_if_fresh`), which would put this test's
    // canonical-view probes on the wrong plane. Any canonical job blocks
    // that door.
    sqlx::query("DELETE FROM awa.jobs WHERE queue = 'override_pin'")
        .execute(&pool)
        .await
        .expect("pin cleanup");
    insert_with(
        &pool,
        &ParkedJob { n: 0 },
        InsertOpts {
            queue: "override_pin".into(),
            ..Default::default()
        },
    )
    .await
    .expect("pin insert should succeed");
    pool
}

/// Insert a job, wait for it to be claimed, and return its effective
/// deadline window in milliseconds (`deadline_at - attempted_at`).
async fn claimed_deadline_window_ms(pool: &PgPool, n: i64) -> i64 {
    let id = insert_with(
        pool,
        &ParkedJob { n },
        InsertOpts {
            queue: QUEUE.into(),
            ..Default::default()
        },
    )
    .await
    .expect("insert should succeed")
    .id;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let window: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT (EXTRACT(EPOCH FROM (deadline_at - attempted_at)) * 1000)::bigint
            FROM awa.jobs_hot
            WHERE id = $1 AND state = 'running' AND deadline_at IS NOT NULL
            "#,
        )
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("deadline probe");
        if let Some(window) = window {
            return window;
        }
        if tokio::time::Instant::now() > deadline {
            let diag: Vec<(i64, String, Option<String>)> = sqlx::query_as(
                "SELECT id, state::text, queue FROM awa.jobs WHERE id = $1 OR queue = 'override_live'",
            )
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            panic!("job {id} was never claimed; visible rows: {diag:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn assert_close(actual_ms: i64, expected_ms: i64, context: &str) {
    let drift = (actual_ms - expected_ms).abs();
    assert!(
        drift < 1_500,
        "{context}: deadline window {actual_ms}ms should be ~{expected_ms}ms"
    );
}

#[tokio::test]
async fn test_overrides_apply_and_revert_without_restart() {
    if awa_testing::setup::skip_unless_canonical("test_overrides_apply_and_revert_without_restart")
    {
        return;
    }
    // Must precede the client build: the dispatcher caches the cadence.
    std::env::set_var("AWA_OVERRIDE_REFRESH_MS", "300");
    let pool = setup().await;

    let client = Client::builder(pool.clone())
        .queue(
            QUEUE,
            QueueConfig {
                max_workers: 4,
                deadline_duration: Duration::from_secs(BASE_DEADLINE_SECS),
                poll_interval: Duration::from_millis(100),
                ..QueueConfig::default()
            },
        )
        .register::<ParkedJob, _, _>(|_args: ParkedJob, _ctx| async move {
            std::future::pending::<()>().await;
            Ok(JobResult::Completed)
        })
        .build()
        .expect("client should build");
    client.start().await.expect("client should start");

    // Baseline: builder-configured deadline.
    let window = claimed_deadline_window_ms(&pool, 1).await;
    assert_close(window, (BASE_DEADLINE_SECS * 1000) as i64, "baseline");

    // Override applies to the running worker within the refresh cadence.
    set_queue_runtime_overrides(
        &pool,
        QUEUE,
        &QueueRuntimeOverrides {
            deadline_ms: Some(OVERRIDE_DEADLINE_MS),
            ..Default::default()
        },
    )
    .await
    .expect("set overrides");
    tokio::time::sleep(Duration::from_millis(900)).await;
    let window = claimed_deadline_window_ms(&pool, 2).await;
    assert_close(window, OVERRIDE_DEADLINE_MS, "override applied live");

    // Clearing reverts to the builder value — also live.
    clear_queue_runtime_overrides(&pool, QUEUE)
        .await
        .expect("clear overrides");
    tokio::time::sleep(Duration::from_millis(900)).await;
    let window = claimed_deadline_window_ms(&pool, 3).await;
    assert_close(
        window,
        (BASE_DEADLINE_SECS * 1000) as i64,
        "revert on clear",
    );

    client.shutdown(Duration::from_secs(5)).await;
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1 OR queue = 'override_pin'")
        .bind(QUEUE)
        .execute(&pool)
        .await
        .expect("queue cleanup");
}
