//! Worker health/readiness listener tests (#368).
//!
//! Uses a dedicated `awa_health_test` database: these tests delete
//! `schema_version` rows and refuse connections to fault-inject, which must
//! not race the shared integration database. For the same reason the tests
//! serialize against each other — a process-local mutex plus a Postgres
//! advisory lock (so process-per-test runners stay safe too).
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::{Client, JobArgs, JobResult, QueueConfig};
use awa_model::{insert_with, migrations, InsertOpts};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;

static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
const HEALTH_TEST_LOCK_KEY: i64 = 0x6177_6168_6c74_6831; // "awahlth1"

fn test_mutex() -> &'static Mutex<()> {
    TEST_MUTEX.get_or_init(|| Mutex::new(()))
}

struct HealthTestGuard {
    _local: tokio::sync::MutexGuard<'static, ()>,
    _conn: PgConnection,
}

async fn acquire_health_guard() -> HealthTestGuard {
    let local = test_mutex().lock().await;
    ensure_health_database().await;
    let mut conn = PgConnection::connect(&health_database_url())
        .await
        .expect("Failed to open health test lock connection");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(HEALTH_TEST_LOCK_KEY)
        .execute(&mut conn)
        .await
        .expect("Failed to acquire health test advisory lock");
    HealthTestGuard {
        _local: local,
        _conn: conn,
    }
}

fn database_url() -> String {
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

fn health_database_url() -> String {
    replace_database_name(&database_url(), "awa_health_test")
}

async fn ensure_health_database() {
    let admin_url = replace_database_name(&database_url(), "postgres");
    let mut admin = PgConnection::connect(&admin_url)
        .await
        .expect("Failed to connect to admin database");
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'awa_health_test')",
    )
    .fetch_one(&mut admin)
    .await
    .expect("Failed to check health database existence");
    if !exists {
        // Tolerate the duplicate-database race: another test process can
        // create it between the existence check and this statement.
        if let Err(err) = sqlx::raw_sql("CREATE DATABASE awa_health_test")
            .execute(&mut admin)
            .await
        {
            let duplicate = matches!(
                &err,
                sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("42P04")
            );
            assert!(duplicate, "Failed to create health test database: {err}");
        }
    }
}

async fn setup() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&health_database_url())
        .await
        .expect("Failed to connect to health test database — is Postgres running?");
    migrations::run(&pool).await.expect("Failed to migrate");
    pool
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SlowJob {
    pub sleep_ms: u64,
}

fn ephemeral_addr() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

async fn build_and_start(pool: &PgPool, queue: &str) -> Client {
    let client = Client::builder(pool.clone())
        .queue(queue, QueueConfig::default())
        .register::<SlowJob, _, _>(|args: SlowJob, _ctx| async move {
            tokio::time::sleep(Duration::from_millis(args.sleep_ms)).await;
            Ok(JobResult::Completed)
        })
        .health_addr(ephemeral_addr())
        .build()
        .expect("client should build");
    client.start().await.expect("client should start");
    client
}

async fn get(addr: SocketAddr, path: &str) -> (u16, serde_json::Value) {
    let response = reqwest::get(format!("http://{addr}{path}"))
        .await
        .expect("health request should connect");
    let status = response.status().as_u16();
    let body: serde_json::Value = response.json().await.expect("health body should be JSON");
    (status, body)
}

/// Poll /readyz until it reports the expected `ready` value (dispatchers and
/// services flip their liveness flags asynchronously after start()).
async fn wait_for_ready(addr: SocketAddr, expected: bool) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let (status, body) = get(addr, "/readyz").await;
        let ready = body["ready"].as_bool().unwrap_or(false);
        if ready == expected {
            assert_eq!(status, if expected { 200 } else { 503 });
            return body;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("timed out waiting for ready={expected}; last body: {body}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn test_healthz_and_readyz_report_running_worker() {
    let _guard = acquire_health_guard().await;
    let pool = setup().await;
    let client = build_and_start(&pool, "health_baseline").await;
    let addr = client
        .health_listener_addr()
        .expect("listener address should be bound after start()");

    let body = wait_for_ready(addr, true).await;
    assert_eq!(body["postgres_connected"], true);
    assert_eq!(body["schema_compatible"], true);
    assert_eq!(body["poll_loop_alive"], true);
    assert_eq!(body["heartbeat_alive"], true);
    assert_eq!(body["maintenance_alive"], true);
    assert_eq!(body["shutting_down"], false);
    assert_eq!(body["expected_schema_version"], migrations::CURRENT_VERSION);

    let (status, body) = get(addr, "/healthz").await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "ok");

    // Unknown path and non-GET are rejected, not silently 200.
    let (status, _) = get(addr, "/nope").await;
    assert_eq!(status, 404);
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/readyz"))
        .send()
        .await
        .expect("POST should connect");
    assert_eq!(response.status().as_u16(), 405);

    client.shutdown(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn test_readyz_flips_503_on_schema_mismatch() {
    let _guard = acquire_health_guard().await;
    let pool = setup().await;
    let client = build_and_start(&pool, "health_schema").await;
    let addr = client.health_listener_addr().expect("listener address");
    wait_for_ready(addr, true).await;

    // Simulate a binary that is newer than the schema: remove the newest
    // schema_version row so the DB reports an older version.
    sqlx::query("DELETE FROM awa.schema_version WHERE version = $1")
        .bind(migrations::CURRENT_VERSION)
        .execute(&pool)
        .await
        .unwrap();

    let body = wait_for_ready(addr, false).await;
    assert_eq!(body["schema_compatible"], false);
    assert_eq!(body["postgres_connected"], true);

    sqlx::query("INSERT INTO awa.schema_version (version, description) VALUES ($1, 'restored by health test')")
        .bind(migrations::CURRENT_VERSION)
        .execute(&pool)
        .await
        .unwrap();
    wait_for_ready(addr, true).await;

    client.shutdown(Duration::from_secs(10)).await;
}

/// The supported rolling-deploy skew: a schema NEWER than the binary must
/// stay ready — and, critically, probing it must not mutate
/// `schema_version`. (`current_version` rewrites rows when its legacy
/// heuristic fires, and a future version triggers that heuristic; probes
/// use the read-only variant.)
#[tokio::test]
async fn test_readyz_stays_ready_on_newer_schema_without_mutating() {
    let _guard = acquire_health_guard().await;
    let pool = setup().await;
    let client = build_and_start(&pool, "health_newer_schema").await;
    let addr = client.health_listener_addr().expect("listener address");
    wait_for_ready(addr, true).await;

    sqlx::query(
        "INSERT INTO awa.schema_version (version, description) VALUES ($1, 'future version for health test')",
    )
    .bind(migrations::CURRENT_VERSION + 1)
    .execute(&pool)
    .await
    .unwrap();
    let rows_before: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();

    // Still ready, reporting the newer version...
    let body = wait_for_ready(addr, true).await;
    assert_eq!(body["schema_compatible"], true);
    assert_eq!(body["schema_version"], migrations::CURRENT_VERSION + 1);

    // ...and several probe cycles later the version table is untouched.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = get(addr, "/readyz").await;
    let rows_after: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        rows_before, rows_after,
        "readiness probes must never rewrite schema_version"
    );

    sqlx::query("DELETE FROM awa.schema_version WHERE version = $1")
        .bind(migrations::CURRENT_VERSION + 1)
        .execute(&pool)
        .await
        .unwrap();
    client.shutdown(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn test_readyz_reports_shutting_down_during_drain() {
    let _guard = acquire_health_guard().await;
    let pool = setup().await;
    let queue = "health_drain";
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(&pool)
        .await
        .unwrap();

    // The worker runs in-process, so the handler can signal the moment its
    // job starts executing — the compat view is not a reliable way to
    // observe `running` under queue storage.
    let started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started_flag = started.clone();
    let client = Client::builder(pool.clone())
        .queue(queue, QueueConfig::default())
        .register::<SlowJob, _, _>(move |args: SlowJob, _ctx| {
            let started = started_flag.clone();
            async move {
                started.store(true, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(args.sleep_ms)).await;
                Ok(JobResult::Completed)
            }
        })
        .health_addr(ephemeral_addr())
        .build()
        .expect("client should build");
    client.start().await.expect("client should start");
    let addr = client.health_listener_addr().expect("listener address");
    wait_for_ready(addr, true).await;

    // Park a slow job so the drain phase stays open long enough to probe.
    insert_with(
        &pool,
        &SlowJob { sleep_ms: 4000 },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Wait until the job is executing before starting the shutdown.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !started.load(std::sync::atomic::Ordering::SeqCst) {
        if tokio::time::Instant::now() > deadline {
            panic!("slow job was never claimed");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let shutdown = {
        let client = std::sync::Arc::new(client);
        let handle = client.clone();
        tokio::spawn(async move { handle.shutdown(Duration::from_secs(15)).await });
        client
    };

    // While draining, readiness must report shutting_down and 503 —
    // infrastructure should stop routing to this instance, but the
    // liveness endpoint must keep answering 200 so it is not killed.
    let body = wait_for_ready(addr, false).await;
    assert_eq!(body["shutting_down"], true);
    let (status, _) = get(addr, "/healthz").await;
    assert_eq!(status, 200);

    // After the drain completes the listener goes away with the runtime.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if reqwest::get(format!("http://{addr}/healthz"))
            .await
            .is_err()
        {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("health listener still answering after shutdown");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(shutdown);
}

#[tokio::test]
async fn test_readyz_flips_503_when_database_unreachable() {
    let _guard = acquire_health_guard().await;
    let pool = setup().await;
    let client = build_and_start(&pool, "health_db_down").await;
    let addr = client.health_listener_addr().expect("listener address");
    wait_for_ready(addr, true).await;

    // Container-free stand-in for a database outage: refuse new connections
    // to the test database and kill every existing backend. Pooled
    // connections die, reconnects are refused, so the probe's acquire fails.
    // (Closing the pool instead would deadlock — the runtime's LISTEN
    // connection never checks back in.)
    let admin_url = replace_database_name(&database_url(), "postgres");
    let mut admin = PgConnection::connect(&admin_url)
        .await
        .expect("admin connection for outage injection");
    sqlx::raw_sql("ALTER DATABASE awa_health_test WITH ALLOW_CONNECTIONS false")
        .execute(&mut admin)
        .await
        .expect("connection refusal should apply");
    sqlx::raw_sql(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = 'awa_health_test' AND pid <> pg_backend_pid()",
    )
    .execute(&mut admin)
    .await
    .expect("backend termination should apply");

    let body = wait_for_ready(addr, false).await;
    assert_eq!(body["postgres_connected"], false);

    // Restore the database for the remaining tests, then let the runtime
    // recover before shutting down cleanly.
    sqlx::raw_sql("ALTER DATABASE awa_health_test WITH ALLOW_CONNECTIONS true")
        .execute(&mut admin)
        .await
        .expect("connection restore should apply");
    wait_for_ready(addr, true).await;
    client.shutdown(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn test_invalid_health_addr_env_is_a_build_error() {
    // Env mutation is process-global; this is the only test that touches
    // AWA_HEALTH_ADDR, and no other test reads it (they configure the
    // builder explicitly).
    let _guard = acquire_health_guard().await;
    let pool = setup().await;
    std::env::set_var("AWA_HEALTH_ADDR", "not-an-address");
    let result = Client::builder(pool.clone())
        .queue("health_env", QueueConfig::default())
        .register::<SlowJob, _, _>(|_args: SlowJob, _ctx| async move { Ok(JobResult::Completed) })
        .build();
    std::env::remove_var("AWA_HEALTH_ADDR");
    let err = result
        .err()
        .expect("invalid AWA_HEALTH_ADDR must not build");
    assert!(
        err.to_string().contains("AWA_HEALTH_ADDR"),
        "unexpected error: {err}"
    );
}
