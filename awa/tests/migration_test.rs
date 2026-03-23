//! Migration tests: step-through upgrade, data survival, idempotency,
//! and migration_sql() consistency.
//!
//! These tests use a dedicated test schema to avoid interfering with
//! other tests that call `migrations::run()` on the shared database.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::migrations;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&database_url())
        .await
        .expect("Failed to connect to database — is Postgres running?")
}

/// Drop and recreate the awa schema for a clean migration test.
async fn reset_schema(pool: &PgPool) {
    sqlx::raw_sql("DROP SCHEMA IF EXISTS awa CASCADE")
        .execute(pool)
        .await
        .expect("Failed to drop schema");
}

// ── Fresh install ────────────────────────────────────────────────

#[tokio::test]
async fn test_fresh_install_reaches_current_version() {
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);
}

// ── Idempotency ──────────────────────────────────────────────────

#[tokio::test]
async fn test_migrations_are_idempotent() {
    let pool = pool().await;
    reset_schema(&pool).await;

    // Run three times
    migrations::run(&pool).await.unwrap();
    migrations::run(&pool).await.unwrap();
    migrations::run(&pool).await.unwrap();

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);
}

// ── Step-through upgrade with data survival ──────────────────────

#[tokio::test]
async fn test_step_through_upgrade_preserves_data() {
    let pool = pool().await;
    reset_schema(&pool).await;

    // Step 1: Apply only V1 (canonical schema)
    let v1_sql = migrations::migration_sql();
    let (v1_version, _, v1_up) = &v1_sql[0];
    assert_eq!(*v1_version, 1);
    sqlx::raw_sql(v1_up).execute(&pool).await.unwrap();

    // Verify V1 schema exists
    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, 1);

    // Insert test data into V1 schema
    sqlx::raw_sql(
        r#"
        INSERT INTO awa.jobs (kind, queue, args, state, priority)
        VALUES ('test_job', 'migration_test', '{"key": "value"}'::jsonb, 'available', 2);

        INSERT INTO awa.cron_jobs (name, cron_expr, kind, queue)
        VALUES ('test_cron', '* * * * *', 'test_job', 'migration_test');

        INSERT INTO awa.queue_meta (queue, paused) VALUES ('migration_test', false);
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    // Step 2: Run full migrations (should apply V2 + V3)
    migrations::run(&pool).await.unwrap();

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);

    // Verify data survived
    let job_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE queue = 'migration_test'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(job_count, 1, "Job should survive migration");

    let cron_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.cron_jobs WHERE name = 'test_cron'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(cron_count, 1, "Cron schedule should survive migration");

    // Verify new tables from V2 exist
    let has_runtime: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'runtime_instances')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(has_runtime, "runtime_instances table should exist after V2");

    // Verify new column from V3 exists
    let has_maintenance_alive: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'awa' AND table_name = 'runtime_instances' AND column_name = 'maintenance_alive')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        has_maintenance_alive,
        "maintenance_alive column should exist after V3"
    );

    // Clean up
    sqlx::raw_sql("DELETE FROM awa.jobs WHERE queue = 'migration_test'; DELETE FROM awa.cron_jobs WHERE name = 'test_cron'; DELETE FROM awa.queue_meta WHERE queue = 'migration_test'")
        .execute(&pool)
        .await
        .unwrap();
}

// ── migration_sql() consistency ──────────────────────────────────

#[tokio::test]
async fn test_migration_sql_matches_run() {
    let pool = pool().await;

    // Apply via run()
    reset_schema(&pool).await;
    migrations::run(&pool).await.unwrap();

    // Capture the resulting table list
    let tables_from_run: Vec<String> = sqlx::query_scalar(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'awa' ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    // Apply via migration_sql() on a fresh schema
    reset_schema(&pool).await;
    for (_version, _desc, sql) in migrations::migration_sql() {
        sqlx::raw_sql(&sql).execute(&pool).await.unwrap();
    }

    // Capture the resulting table list
    let tables_from_sql: Vec<String> = sqlx::query_scalar(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'awa' ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        tables_from_run, tables_from_sql,
        "migration_sql() should produce the same schema as run()"
    );

    // Restore for other tests
    migrations::run(&pool).await.unwrap();
}

// ── Legacy version upgrade (0.3.x → 0.4.x) ─────────────────────

#[tokio::test]
async fn test_legacy_version_upgrade() {
    let pool = pool().await;
    reset_schema(&pool).await;

    // Simulate a 0.3.x database: apply V1 schema but insert legacy version rows (3, 4, 5)
    let v1_sql = &migrations::migration_sql()[0].2;
    sqlx::raw_sql(v1_sql).execute(&pool).await.unwrap();

    // Replace the V1 version row with legacy numbering
    sqlx::raw_sql(
        r#"
        DELETE FROM awa.schema_version;
        INSERT INTO awa.schema_version (version, description) VALUES (3, 'Legacy V3');
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    // Apply V2 + V3 SQL manually with legacy numbering
    let v2_sql = &migrations::migration_sql()[1].2;
    let v3_sql = &migrations::migration_sql()[2].2;
    sqlx::raw_sql(v2_sql).execute(&pool).await.unwrap();
    sqlx::raw_sql(v3_sql).execute(&pool).await.unwrap();

    // Replace version rows with legacy numbering
    sqlx::raw_sql(
        r#"
        DELETE FROM awa.schema_version WHERE version IN (2, 3);
        INSERT INTO awa.schema_version (version, description) VALUES (4, 'Legacy V4');
        INSERT INTO awa.schema_version (version, description) VALUES (5, 'Legacy V5');
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    // Now run() should detect legacy versioning and normalize
    migrations::run(&pool).await.unwrap();

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(
        version,
        migrations::CURRENT_VERSION,
        "Legacy version should be normalized to current"
    );

    // Restore
    migrations::run(&pool).await.unwrap();
}
