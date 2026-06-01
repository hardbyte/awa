//! Integration tests for `awa storage finalize --check` / `--wait`.
//!
//! These tests drive the compiled `awa` binary against a dedicated
//! Postgres database (`awa_finalize_cli_test`) so they don't have to
//! contend with the shared `awa_test` storage-transition singleton
//! row that the rest of the integration suite mutates. A
//! process-local mutex plus an advisory lock keeps the tests in this
//! file from racing against each other when cargo runs them
//! concurrently.

use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

use assert_cmd::Command;
use awa_model::{migrations, storage, QueueStorage};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
use tokio::sync::Mutex;
use uuid::Uuid;

const TEST_DB_NAME: &str = "awa_finalize_cli_test";
const FINALIZE_TEST_LOCK_KEY: i64 = 0x6177616669636c69; // "awafinli" + slop

static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

fn test_mutex() -> &'static Mutex<()> {
    TEST_MUTEX.get_or_init(|| Mutex::new(()))
}

struct TestGuard {
    _local: tokio::sync::MutexGuard<'static, ()>,
    _conn: PgConnection,
}

fn base_database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn replace_database_name(url: &str, db_name: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (url, None),
    };
    let (prefix, _old_db) = base
        .rsplit_once('/')
        .expect("DATABASE_URL must include a database name");
    let mut out = format!("{prefix}/{db_name}");
    if let Some(query) = query {
        out.push('?');
        out.push_str(query);
    }
    out
}

fn test_database_url() -> String {
    replace_database_name(&base_database_url(), TEST_DB_NAME)
}

fn admin_database_url() -> String {
    replace_database_name(&base_database_url(), "postgres")
}

async fn ensure_test_database() {
    let mut admin = PgConnection::connect(&admin_database_url())
        .await
        .expect("connect admin db");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(TEST_DB_NAME)
            .fetch_one(&mut admin)
            .await
            .expect("check db existence");
    if !exists {
        // CREATE DATABASE does not support bind parameters.
        sqlx::raw_sql(&format!("CREATE DATABASE {TEST_DB_NAME}"))
            .execute(&mut admin)
            .await
            .expect("create finalize cli test db");
    }
}

async fn acquire_guard() -> TestGuard {
    let local = test_mutex().lock().await;
    ensure_test_database().await;
    let mut conn = PgConnection::connect(&test_database_url())
        .await
        .expect("lock conn");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(FINALIZE_TEST_LOCK_KEY)
        .execute(&mut conn)
        .await
        .expect("acquire advisory lock");
    TestGuard {
        _local: local,
        _conn: conn,
    }
}

async fn pool() -> PgPool {
    ensure_test_database().await;
    let opts = PgConnectOptions::from_str(&test_database_url()).expect("parse test db url");
    PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(opts)
        .await
        .expect("connect test db")
}

async fn reset_schema(pool: &PgPool) {
    sqlx::raw_sql("DROP SCHEMA IF EXISTS awa CASCADE")
        .execute(pool)
        .await
        .expect("drop awa schema");
    // Also clean up any prepared queue-storage schemas this test
    // might have left over from a previous run.
    sqlx::raw_sql("DROP SCHEMA IF EXISTS awa_finalize_cli_qs CASCADE")
        .execute(pool)
        .await
        .expect("drop queue storage schema");
}

async fn prepare_queue_storage_schema(pool: &PgPool, schema: &str) {
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(pool)
        .await
        .expect("drop qs schema");
    let store = QueueStorage::from_existing_schema(schema).expect("qs schema validates");
    store.prepare_schema(pool).await.expect("prepare qs schema");
}

async fn insert_runtime_instance(pool: &PgPool, capability: &str, transition_role: &str) {
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_instances (
            instance_id, hostname, pid, version,
            started_at, last_seen_at, snapshot_interval_ms,
            healthy, postgres_connected, poll_loop_alive,
            heartbeat_alive, maintenance_alive, shutting_down,
            leader, global_max_workers, queues,
            storage_capability, transition_role
        )
        VALUES (
            $1, 'finalize-cli-test-host', 4242, '0.6.0-test',
            now() - interval '1 minute', now(), 10000,
            TRUE, TRUE, TRUE,
            TRUE, TRUE, FALSE,
            TRUE, NULL, '[]'::jsonb,
            $2, $3
        )
        ON CONFLICT (instance_id) DO UPDATE
            SET last_seen_at = EXCLUDED.last_seen_at,
                storage_capability = EXCLUDED.storage_capability,
                transition_role = EXCLUDED.transition_role
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(capability)
    .bind(transition_role)
    .execute(pool)
    .await
    .expect("insert runtime instance");
}

fn run_cli(args: &[&str]) -> Command {
    let mut command = Command::cargo_bin("awa").expect("awa binary should build");
    command
        .env("DATABASE_URL", test_database_url())
        // Keep tracing chatter out of stderr unless the test wants it.
        .env("RUST_LOG", "warn")
        .args(args);
    command
}

// ── --check ──────────────────────────────────────────────────────

#[tokio::test]
async fn check_on_fresh_canonical_state_reports_blocked() {
    // A fresh install sits in `state=canonical`; finalize is blocked
    // because `state != mixed_transition`. `--check` should exit 2
    // with a useful summary and NOT mutate state.
    let _guard = acquire_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.expect("migrate");

    let assert = run_cli(&["storage", "finalize", "--check"])
        .assert()
        .code(2);
    let stderr = String::from_utf8(assert.get_output().stderr.clone()).expect("stderr utf8");
    assert!(
        stderr.contains("blocked"),
        "expected blocked summary, got: {stderr}"
    );
    assert!(
        stderr.contains("expected 'mixed_transition'"),
        "expected state blocker in summary, got: {stderr}"
    );

    // No state change.
    let status = storage::status(&pool).await.expect("status");
    assert_eq!(status.state, "canonical");
}

#[tokio::test]
async fn check_when_ready_exits_zero_without_finalizing() {
    let _guard = acquire_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.expect("migrate");

    let schema = "awa_finalize_cli_qs";
    prepare_queue_storage_schema(&pool, schema).await;
    storage::prepare(
        &pool,
        "queue_storage",
        serde_json::json!({ "schema": schema }),
    )
    .await
    .expect("storage prepare");
    insert_runtime_instance(&pool, "queue_storage", "queue_storage_target").await;
    storage::enter_mixed_transition(&pool)
        .await
        .expect("enter mixed transition");

    // mixed_transition with no canonical-only / drain-only runtimes
    // and an empty hot backlog → finalize is ready.
    let report = storage::status_report(&pool).await.expect("report");
    assert!(
        report.can_finalize,
        "test setup should reach can_finalize; blockers: {:?}",
        report.finalize_blockers
    );

    run_cli(&["storage", "finalize", "--check"])
        .assert()
        .success();

    // Crucially, --check must NOT advance state.
    let status = storage::status(&pool).await.expect("status");
    assert_eq!(status.state, "mixed_transition");
}

// ── --wait ───────────────────────────────────────────────────────

#[tokio::test]
async fn wait_finalizes_immediately_when_ready() {
    let _guard = acquire_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.expect("migrate");

    let schema = "awa_finalize_cli_qs";
    prepare_queue_storage_schema(&pool, schema).await;
    storage::prepare(
        &pool,
        "queue_storage",
        serde_json::json!({ "schema": schema }),
    )
    .await
    .expect("prepare");
    insert_runtime_instance(&pool, "queue_storage", "queue_storage_target").await;
    storage::enter_mixed_transition(&pool)
        .await
        .expect("enter mixed transition");

    // Cap the wait so the test can't hang if something regresses.
    run_cli(&["storage", "finalize", "--wait=30s"])
        .timeout(Duration::from_secs(60))
        .assert()
        .success();

    let status = storage::status(&pool).await.expect("status");
    assert_eq!(status.state, "active");
    assert_eq!(status.current_engine, "queue_storage");
}

#[tokio::test]
async fn wait_times_out_with_blocker() {
    // mixed_transition + a hot-backlog row that never goes away →
    // `--wait` should poll, observe the persistent blocker, hit the
    // duration cap, and exit 2 with the blocker described in stderr.
    let _guard = acquire_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.expect("migrate");

    let schema = "awa_finalize_cli_qs";
    prepare_queue_storage_schema(&pool, schema).await;
    storage::prepare(
        &pool,
        "queue_storage",
        serde_json::json!({ "schema": schema }),
    )
    .await
    .expect("prepare");
    insert_runtime_instance(&pool, "queue_storage", "queue_storage_target").await;
    storage::enter_mixed_transition(&pool)
        .await
        .expect("enter mixed transition");

    // Plant a non-terminal canonical row so canonical_live_backlog > 0.
    sqlx::query(
        "INSERT INTO awa.jobs_hot (kind, queue, args, state, priority) \
         VALUES ('finalize_blocker', 'cli_finalize_wait', '{}'::jsonb, 'available', 2)",
    )
    .execute(&pool)
    .await
    .expect("insert backlog row");

    let started = std::time::Instant::now();
    let assert = run_cli(&["storage", "finalize", "--wait=2s"])
        .timeout(Duration::from_secs(30))
        .assert()
        .code(2);
    // Sanity-check the wait actually waited (within rounding).
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_secs(2),
        "expected at least 2s of waiting, got {elapsed:?}"
    );

    let stderr = String::from_utf8(assert.get_output().stderr.clone()).expect("stderr utf8");
    assert!(
        stderr.contains("timed out"),
        "expected timeout summary, got: {stderr}"
    );
    assert!(
        stderr.contains("canonical live backlog"),
        "expected backlog blocker in summary, got: {stderr}"
    );

    // State should still be mixed_transition (finalize never ran).
    let status = storage::status(&pool).await.expect("status");
    assert_eq!(status.state, "mixed_transition");

    // Cleanup so a later run starts fresh.
    sqlx::query("DELETE FROM awa.jobs_hot WHERE queue = 'cli_finalize_wait'")
        .execute(&pool)
        .await
        .expect("cleanup backlog");
}
