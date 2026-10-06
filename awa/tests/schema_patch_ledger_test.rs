//! Schema patch ledger and runner (ADR-046).
//!
//! Requires a running Postgres instance:
//! `DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test`.

use awa::model::migrations::{
    self, apply_schema_patches_conn, pending_schema_patches_conn, schema_patch_script, SchemaPatch,
    SCHEMA_PATCHES, SCHEMA_PATCH_LEDGER_DDL,
};
use awa_testing::setup;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::str::FromStr;

/// A harmless patch that only this test knows about.
const PROBE_PATCH: SchemaPatch = SchemaPatch {
    name: "test_probe_patch",
    description: "Adds a table the ledger test can look for",
    sql: "CREATE TABLE IF NOT EXISTS awa.schema_patch_probe_test (id INT PRIMARY KEY);",
    applied_probe: Some("SELECT to_regclass('awa.schema_patch_probe_test') IS NOT NULL"),
};

async fn dedicated_pool(name: &str) -> PgPool {
    let base = PgConnectOptions::from_str(&setup::database_url()).expect("DATABASE_URL parses");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(base.clone().database("postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    match sqlx::query(awa::audited_sql(format!("CREATE DATABASE {name}")))
        .execute(&admin)
        .await
    {
        Ok(_) => {}
        Err(sqlx::Error::Database(err)) if err.code().as_deref() == Some("42P04") => {}
        Err(err) => panic!("create database {name}: {err}"),
    }
    admin.close().await;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(base.database(name))
        .await
        .expect("connect to the dedicated database");
    sqlx::raw_sql("DROP SCHEMA IF EXISTS awa CASCADE")
        .execute(&pool)
        .await
        .unwrap();
    migrations::run(&pool).await.unwrap();
    pool
}

async fn ledger_rows(pool: &PgPool) -> Vec<(String, String)> {
    sqlx::query_as("SELECT name, applied_by FROM awa.schema_patches ORDER BY name")
        .fetch_all(pool)
        .await
        .unwrap()
}

/// v048 creates the ledger and records the patches v046 and v047 carry, so a
/// database that arrives from 0.6.x (patches applied, no ledger) and a fresh
/// install look the same. Nothing is pending on a current schema.
#[tokio::test]
async fn v048_seeds_the_ledger_with_the_carried_patches() {
    let pool = dedicated_pool("awa_test_patch_ledger_seed").await;
    assert_eq!(
        ledger_rows(&pool).await,
        vec![
            (
                "prepared_lane_sequences".to_string(),
                "migration v047".to_string()
            ),
            (
                "wait_free_dirty_marks".to_string(),
                "migration v046".to_string()
            ),
        ]
    );
    assert!(migrations::pending_schema_patches(&pool)
        .await
        .unwrap()
        .is_empty());
    assert!(
        SCHEMA_PATCHES.is_empty(),
        "main carries patches as numbered migrations"
    );
    pool.close().await;
}

/// The runner applies a pending patch once, records who applied it, and
/// treats the recorded patch as done on the next pass.
#[tokio::test]
async fn runner_applies_and_records_a_patch_once() {
    let pool = dedicated_pool("awa_test_patch_ledger_apply").await;
    let mut conn = pool.acquire().await.unwrap();

    let pending = pending_schema_patches_conn(&mut conn, &[PROBE_PATCH])
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);

    let applied = apply_schema_patches_conn(&mut conn, &[PROBE_PATCH])
        .await
        .unwrap();
    assert_eq!(applied, vec!["test_probe_patch"]);
    let exists: bool =
        sqlx::query_scalar("SELECT to_regclass('awa.schema_patch_probe_test') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert!(exists);
    let rows = ledger_rows(&pool).await;
    assert!(rows
        .iter()
        .any(|(name, by)| name == "test_probe_patch" && by.starts_with("awa ")));

    assert!(pending_schema_patches_conn(&mut conn, &[PROBE_PATCH])
        .await
        .unwrap()
        .is_empty());
    let applied_again = apply_schema_patches_conn(&mut conn, &[PROBE_PATCH])
        .await
        .unwrap();
    assert!(applied_again.is_empty());
    drop(conn);
    pool.close().await;
}

/// A database patched before the ledger existed is recognised through the
/// patch's probe: the ledger row is back-filled and the SQL is not re-run.
#[tokio::test]
async fn probe_backfills_the_ledger_for_pre_ledger_patches() {
    let pool = dedicated_pool("awa_test_patch_ledger_probe").await;
    let mut conn = pool.acquire().await.unwrap();
    sqlx::raw_sql(PROBE_PATCH.sql)
        .execute(&mut *conn)
        .await
        .unwrap();

    let pending = pending_schema_patches_conn(&mut conn, &[PROBE_PATCH])
        .await
        .unwrap();
    assert!(pending.is_empty());
    let rows = ledger_rows(&pool).await;
    assert!(rows
        .iter()
        .any(|(name, by)| name == "test_probe_patch" && by == "probe back-fill"));
    drop(conn);
    pool.close().await;
}

/// The exported script is self-recording: applying it with a plain SQL
/// runner leaves the same ledger row the built-in runner would, and applying
/// it twice is a no-op.
#[tokio::test]
async fn exported_patch_script_records_itself() {
    let pool = dedicated_pool("awa_test_patch_ledger_script").await;
    let script = schema_patch_script(&PROBE_PATCH, "sql export");
    assert!(script.contains(SCHEMA_PATCH_LEDGER_DDL));
    for _ in 0..2 {
        sqlx::raw_sql(awa::audited_sql(script.clone()))
            .execute(&pool)
            .await
            .unwrap();
    }
    let rows = ledger_rows(&pool).await;
    assert_eq!(
        rows.iter()
            .filter(|(name, _)| name == "test_probe_patch")
            .count(),
        1
    );
    assert!(rows
        .iter()
        .any(|(name, by)| name == "test_probe_patch" && by == "sql export"));
    pool.close().await;
}
