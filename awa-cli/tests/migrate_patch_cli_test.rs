//! `awa migrate` against a 0.6.9-shaped schema: range flags are refused on the
//! apply path, an interrupted patch leaves nothing behind and a retry
//! completes it, and `--sql` output applies all or nothing under psql.
//!
//! Each test gets its own database. The pre-patch shape is a v40 schema whose
//! queue-storage substrates (`awa` and a custom schema) carry the 0.6.9 lane
//! helper and no `awa.refresh_lane_sequence_helpers`.

use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use awa_model::{migrations, QueueStorage, QueueStorageConfig};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};

const CUSTOM_SCHEMA: &str = "custom_lanes";
const OLD_HELPER: &str =
    include_str!("../../awa-model/tests/fixtures/v06_ensure_lane_sequences.sql");

fn base_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn url_for(database: &str) -> String {
    let base = base_url();
    let (base, query) = match base.split_once('?') {
        Some((base, query)) => (base.to_string(), Some(query.to_string())),
        None => (base, None),
    };
    let (prefix, _) = base
        .rsplit_once('/')
        .expect("DATABASE_URL names a database");
    match query {
        Some(query) => format!("{prefix}/{database}?{query}"),
        None => format!("{prefix}/{database}"),
    }
}

struct TestDatabase {
    name: String,
    url: String,
}

impl TestDatabase {
    async fn create() -> Self {
        let name = format!("awa_migrate_cli_{}", uuid::Uuid::new_v4().simple());
        let mut admin = PgConnection::connect(&url_for("postgres")).await.unwrap();
        sqlx::raw_sql(&format!("CREATE DATABASE {name}"))
            .execute(&mut admin)
            .await
            .unwrap();
        let url = url_for(&name);
        Self { name, url }
    }

    async fn pool(&self) -> PgPool {
        PgPoolOptions::new()
            .max_connections(3)
            .connect(&self.url)
            .await
            .unwrap()
    }

    async fn drop(self) {
        let mut admin = PgConnection::connect(&url_for("postgres")).await.unwrap();
        sqlx::raw_sql(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            self.name
        ))
        .execute(&mut admin)
        .await
        .unwrap();
    }
}

/// A v40 schema as 0.6.9 left it: both substrates have the old lane helper
/// and the helper refresh function does not exist.
async fn make_pre_patch(pool: &PgPool) {
    migrations::run(pool).await.unwrap();
    QueueStorage::new(QueueStorageConfig {
        schema: CUSTOM_SCHEMA.into(),
        ..Default::default()
    })
    .unwrap()
    .prepare_schema(pool)
    .await
    .unwrap();
    for schema in ["awa", CUSTOM_SCHEMA] {
        sqlx::raw_sql(&OLD_HELPER.replace("__schema__", schema))
            .execute(pool)
            .await
            .unwrap();
    }
    sqlx::raw_sql("DROP FUNCTION awa.refresh_lane_sequence_helpers(text)")
        .execute(pool)
        .await
        .unwrap();
    // Stand-in for the 0.6.9 installer: same signature, no helper refresh.
    sqlx::raw_sql(
        r#"
        DO $$
        DECLARE v_args TEXT;
        BEGIN
            SELECT pg_get_function_arguments(p.oid) INTO STRICT v_args
            FROM pg_proc AS p
            WHERE p.pronamespace = 'awa'::regnamespace
              AND p.proname = 'install_queue_storage_substrate';
            EXECUTE format(
                'CREATE OR REPLACE FUNCTION awa.install_queue_storage_substrate(%s) '
                'RETURNS VOID LANGUAGE plpgsql AS $body$ BEGIN '
                'RAISE EXCEPTION ''pre-0.6.10 installer''; END $body$',
                v_args
            );
        END
        $$;
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(
        lane_patch(pool).await.needs_repair,
        vec![
            "awa.install_queue_storage_substrate".to_string(),
            "awa.refresh_lane_sequence_helpers".to_string(),
            "lane helpers in schema awa".to_string(),
            format!("lane helpers in schema {CUSTOM_SCHEMA}"),
        ]
    );
}

async fn lane_patch(pool: &PgPool) -> migrations::SchemaPatchStatus {
    migrations::schema_patch_status(pool)
        .await
        .unwrap()
        .into_iter()
        .find(|status| status.name == "prepared_lane_sequences")
        .unwrap()
}

fn awa(database_url: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_awa"))
        .arg("--database-url")
        .arg(database_url)
        .args(args)
        .env("RUST_LOG", "error")
        .output()
        .expect("run awa")
}

/// An advisory lock the patch takes on the `awa` substrate, held in an open
/// transaction so `awa migrate` blocks inside the patch.
async fn hold_install_lock(url: &str) -> PgConnection {
    let mut conn = PgConnection::connect(url).await.unwrap();
    sqlx::raw_sql(
        "BEGIN; SELECT pg_advisory_xact_lock(hashtextextended('awa.queue_storage.install:awa', 0));",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    conn
}

async fn wait_for_blocked_backend(pool: &PgPool, holder_pid: i32) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let pid: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity \
             WHERE datname = current_database() AND pid <> $1 AND pid <> pg_backend_pid() \
               AND wait_event_type = 'Lock' AND wait_event = 'advisory'",
        )
        .bind(holder_pid)
        .fetch_optional(pool)
        .await
        .unwrap();
        if let Some(pid) = pid {
            return pid;
        }
        assert!(
            Instant::now() < deadline,
            "awa migrate never blocked on the install lock"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn migrate_refuses_range_flags_on_the_apply_path() {
    let db = TestDatabase::create().await;
    for args in [
        &["migrate", "--to", "20"][..],
        &["migrate", "--from", "39"][..],
        &["migrate", "--version", "3"][..],
    ] {
        let output = awa(&db.url, args);
        assert_eq!(output.status.code(), Some(1), "{args:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("select a range to render, not a range to apply"),
            "{args:?}: {stderr}"
        );
    }
    let pool = db.pool().await;
    let has_schema: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'awa')")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!has_schema, "a refused apply must not touch the database");
    pool.close().await;
    db.drop().await;
}

#[tokio::test]
async fn interrupted_patch_leaves_nothing_and_a_retry_applies_it() {
    let db = TestDatabase::create().await;
    let pool = db.pool().await;
    make_pre_patch(&pool).await;
    let routing_before: (String, String, i64) = sqlx::query_as(
        "SELECT state, current_engine, transition_epoch FROM awa.storage_transition_state",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let mut holder = hold_install_lock(&db.url).await;
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut holder)
        .await
        .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_awa"))
        .arg("--database-url")
        .arg(&db.url)
        .arg("migrate")
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // By the time the patch waits for the substrate lock it has replaced the
    // installer and created the refresh function; ending its backend must
    // undo both.
    let blocked = wait_for_blocked_backend(&pool, holder_pid).await;
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(blocked)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(terminated);
    let output = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(!output.status.success(), "an interrupted migrate must fail");
    sqlx::raw_sql("ROLLBACK")
        .execute(&mut holder)
        .await
        .unwrap();
    holder.close().await.unwrap();

    // Nothing of the patch survives: installer, refresh function and both
    // substrates' helpers are as before.
    let status = lane_patch(&pool).await;
    assert!(!status.applied);
    assert_eq!(status.needs_repair.len(), 4, "{:?}", status.needs_repair);

    let output = awa(&db.url, &["migrate"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("Schema: v40 (unchanged)"), "{stdout}");
    assert!(
        stdout.contains("Schema patch prepared_lane_sequences: applied"),
        "{stdout}"
    );
    assert!(stdout.contains("lane helpers in schema awa"), "{stdout}");
    assert!(
        stdout.contains(&format!("lane helpers in schema {CUSTOM_SCHEMA}")),
        "{stdout}"
    );
    assert!(
        stdout.contains("Storage routing: unchanged (state canonical, engine canonical"),
        "{stdout}"
    );
    assert!(lane_patch(&pool).await.applied);
    let routing_after: (String, String, i64) = sqlx::query_as(
        "SELECT state, current_engine, transition_epoch FROM awa.storage_transition_state",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(routing_before, routing_after);

    // A second run has nothing left to do.
    let output = awa(&db.url, &["migrate"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(
        stdout.contains("Schema patch prepared_lane_sequences: already present"),
        "{stdout}"
    );
    pool.close().await;
    db.drop().await;
}

fn psql(database_url: &str, sql: &str, lock_timeout: Option<&str>) -> Output {
    use std::io::Write;
    let mut command = Command::new("psql");
    command
        .arg(database_url)
        .args(["-X", "-q", "-v", "ON_ERROR_STOP=1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(timeout) = lock_timeout {
        command.env("PGOPTIONS", format!("-c lock_timeout={timeout}"));
    }
    let mut child = command
        .spawn()
        .expect("psql must be on PATH for the exported-SQL test");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(sql.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[tokio::test]
async fn exported_sql_applies_all_or_nothing() {
    let db = TestDatabase::create().await;
    let pool = db.pool().await;
    make_pre_patch(&pool).await;

    let output = awa(&db.url, &["migrate", "--sql", "--pending"]);
    assert!(output.status.success());
    let sql = String::from_utf8(output.stdout).unwrap();
    assert!(
        sql.contains("-v ON_ERROR_STOP=1"),
        "the header names ON_ERROR_STOP"
    );
    let body: Vec<&str> = sql
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with("--"))
        .collect();
    assert_eq!(body.first(), Some(&"BEGIN;"));
    assert_eq!(
        body.get(1).copied(),
        Some(
            format!(
                "SELECT pg_advisory_xact_lock({});",
                migrations::MIGRATION_LOCK_KEY
            )
            .as_str()
        )
    );
    assert_eq!(body.last(), Some(&"COMMIT;"));
    assert!(sql.contains("-- Schema patch R__prepared_lane_sequences"));

    let unwrapped = awa(
        &db.url,
        &["migrate", "--sql", "--pending", "--no-transaction"],
    );
    let unwrapped = String::from_utf8(unwrapped.stdout).unwrap();
    assert!(!unwrapped.contains("BEGIN;") && !unwrapped.contains("pg_advisory_xact_lock(4"));

    // A lock timeout inside the patch fails the script; ON_ERROR_STOP stops
    // psql before COMMIT, so nothing applies.
    let mut holder = hold_install_lock(&db.url).await;
    let output = psql(&db.url, &sql, Some("500ms"));
    sqlx::raw_sql("ROLLBACK")
        .execute(&mut holder)
        .await
        .unwrap();
    holder.close().await.unwrap();
    assert!(
        !output.status.success(),
        "psql must fail: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let refresh_exists: bool = sqlx::query_scalar(
        "SELECT to_regprocedure('awa.refresh_lane_sequence_helpers(text)') IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        !refresh_exists,
        "a failed script must leave nothing applied"
    );
    assert_eq!(lane_patch(&pool).await.needs_repair.len(), 4);

    // The same script applies cleanly once the lock is free.
    let output = psql(&db.url, &sql, None);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(lane_patch(&pool).await.applied);
    assert!(migrations::pending_schema_patches(&pool)
        .await
        .unwrap()
        .is_empty());
    pool.close().await;
    db.drop().await;
}
