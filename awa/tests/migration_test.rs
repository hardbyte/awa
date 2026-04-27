//! Migration tests: step-through upgrade, data survival, idempotency,
//! and migration_sql() consistency.
//!
//! **Must run with `--test-threads=1`** — these tests drop and recreate
//! the `awa` schema, which would break concurrent tests.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{insert_many, insert_many_copy_from_pool, migrations, storage, QueueStorage};
use awa::{InsertOpts, InsertParams, JobArgs, UniqueOpts};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
use std::sync::OnceLock;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Serialize all migration tests to prevent parallel schema drops.
static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
const MIGRATION_TEST_LOCK_KEY: i64 = 0x6177616d69677231;

fn test_mutex() -> &'static Mutex<()> {
    TEST_MUTEX.get_or_init(|| Mutex::new(()))
}

struct MigrationTestGuard {
    _local: tokio::sync::MutexGuard<'static, ()>,
    _conn: PgConnection,
}

async fn acquire_migration_guard() -> MigrationTestGuard {
    let local = test_mutex().lock().await;
    ensure_migration_database().await;
    let mut conn = PgConnection::connect(&migration_database_url())
        .await
        .expect("Failed to open migration test lock connection");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(MIGRATION_TEST_LOCK_KEY)
        .execute(&mut conn)
        .await
        .expect("Failed to acquire migration test advisory lock");
    MigrationTestGuard {
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

fn migration_database_url() -> String {
    replace_database_name(&database_url(), "awa_migration_test")
}

fn admin_database_url() -> String {
    replace_database_name(&database_url(), "postgres")
}

async fn ensure_migration_database() {
    let mut admin = PgConnection::connect(&admin_database_url())
        .await
        .expect("Failed to connect to admin database");
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'awa_migration_test')",
    )
    .fetch_one(&mut admin)
    .await
    .expect("Failed to check migration database existence");
    if !exists {
        sqlx::raw_sql("CREATE DATABASE awa_migration_test")
            .execute(&mut admin)
            .await
            .expect("Failed to create migration test database");
    }
}

async fn pool() -> PgPool {
    ensure_migration_database().await;
    PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&migration_database_url())
        .await
        .expect("Failed to connect to migration test database — is Postgres running?")
}

/// Drop and recreate the awa schema for a clean migration test.
async fn reset_schema(pool: &PgPool) {
    sqlx::raw_sql("DROP SCHEMA IF EXISTS awa CASCADE")
        .execute(pool)
        .await
        .expect("Failed to drop schema");
}

fn sqlstate_from_awa_error(err: &awa::AwaError) -> Option<String> {
    match err {
        awa::AwaError::Database(sqlx::Error::Database(db_err)) => {
            db_err.code().map(|code| code.to_string())
        }
        _ => None,
    }
}

async fn simulate_non_canonical_compat_routing(pool: &PgPool) {
    sqlx::raw_sql(
        r#"
        UPDATE awa.storage_transition_state
        SET prepared_engine = 'queue_storage',
            state = 'active',
            transition_epoch = transition_epoch + 1,
            details = '{"schema":"awa_queue_storage"}'::jsonb,
            updated_at = now(),
            finalized_at = now()
        WHERE singleton;

        CREATE OR REPLACE FUNCTION awa.insert_job_compat(
            p_kind TEXT,
            p_queue TEXT DEFAULT 'default',
            p_args JSONB DEFAULT '{}'::jsonb,
            p_state awa.job_state DEFAULT 'available',
            p_priority SMALLINT DEFAULT 2,
            p_max_attempts SMALLINT DEFAULT 25,
            p_run_at TIMESTAMPTZ DEFAULT NULL,
            p_metadata JSONB DEFAULT '{}'::jsonb,
            p_tags TEXT[] DEFAULT ARRAY[]::TEXT[],
            p_unique_key BYTEA DEFAULT NULL,
            p_unique_states BIT(8) DEFAULT NULL
        )
        RETURNS awa.jobs
        LANGUAGE sql
        SET search_path = pg_catalog, awa
        AS $$
            INSERT INTO awa.jobs (
                kind,
                queue,
                args,
                state,
                priority,
                max_attempts,
                run_at,
                metadata,
                tags,
                unique_key,
                unique_states
            )
            VALUES (
                p_kind,
                p_queue,
                p_args,
                p_state,
                p_priority,
                p_max_attempts,
                p_run_at,
                p_metadata,
                p_tags,
                p_unique_key,
                p_unique_states
            )
            RETURNING *
        $$;
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn install_queue_storage_backend(pool: &PgPool, schema: &str) {
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(pool)
        .await
        .expect("queue storage test schema should drop cleanly");

    let store =
        QueueStorage::from_existing_schema(schema).expect("queue storage schema should validate");
    store
        .install(pool)
        .await
        .expect("queue storage install should succeed");
}

async fn prepare_queue_storage_schema(pool: &PgPool, schema: &str) {
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(pool)
        .await
        .expect("queue storage test schema should drop cleanly");

    let store =
        QueueStorage::from_existing_schema(schema).expect("queue storage schema should validate");
    store
        .prepare_schema(pool)
        .await
        .expect("queue storage schema preparation should succeed");
}

async fn active_queue_storage_schema(pool: &PgPool) -> Option<String> {
    sqlx::query_scalar("SELECT awa.active_queue_storage_schema()")
        .fetch_one(pool)
        .await
        .expect("active queue storage schema query should succeed")
}

async fn relation_exists(pool: &PgPool, qualified_relname: &str) -> bool {
    sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(qualified_relname)
        .fetch_one(pool)
        .await
        .expect("relation existence query should succeed")
}

async fn insert_runtime_instance(pool: &PgPool, capability: &str) {
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_instances (
            instance_id,
            hostname,
            pid,
            version,
            started_at,
            last_seen_at,
            snapshot_interval_ms,
            healthy,
            postgres_connected,
            poll_loop_alive,
            heartbeat_alive,
            maintenance_alive,
            shutting_down,
            leader,
            global_max_workers,
            queues,
            storage_capability
        )
        VALUES (
            $1, 'test-host', 4242, '0.6.0-test',
            now() - interval '1 minute',
            now(),
            10000,
            TRUE,
            TRUE,
            TRUE,
            TRUE,
            TRUE,
            FALSE,
            TRUE,
            NULL,
            '[]'::jsonb,
            $2
        )
        ON CONFLICT (instance_id)
        DO UPDATE SET
            last_seen_at = EXCLUDED.last_seen_at,
            storage_capability = EXCLUDED.storage_capability
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(capability)
    .execute(pool)
    .await
    .expect("runtime instance insert should succeed");
}

// ── Fresh install ────────────────────────────────────────────────

#[tokio::test]
async fn test_fresh_install_reaches_current_version() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);
}

// ── Idempotency ──────────────────────────────────────────────────

#[tokio::test]
async fn test_migrations_are_idempotent() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    migrations::run(&pool).await.unwrap();
    migrations::run(&pool).await.unwrap();

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);
}

// ── Step-through upgrade with data survival ──────────────────────

#[tokio::test]
async fn test_step_through_upgrade_preserves_data() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    let v1_sql = migrations::migration_sql();
    let (v1_version, _, v1_up) = &v1_sql[0];
    assert_eq!(*v1_version, 1);
    sqlx::raw_sql(v1_up).execute(&pool).await.unwrap();

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, 1);

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

    // Step 2: Run full migrations (should apply V2 + V3 + V4)
    migrations::run(&pool).await.unwrap();

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);

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

    let has_runtime: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'runtime_instances')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(has_runtime, "runtime_instances table should exist after V2");

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

    let has_queue_state_counts: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'queue_state_counts')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        has_queue_state_counts,
        "queue_state_counts table should exist after V4"
    );

    let has_storage_transition_state: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'storage_transition_state')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        has_storage_transition_state,
        "storage_transition_state should exist after V10"
    );

    let has_storage_capability: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'awa' AND table_name = 'runtime_instances' AND column_name = 'storage_capability')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        has_storage_capability,
        "runtime_instances.storage_capability should exist after V10"
    );

    let available_count: i64 = sqlx::query_scalar(
        "SELECT available FROM awa.queue_state_counts WHERE queue = 'migration_test'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        available_count, 1,
        "V4 backfill should capture existing jobs"
    );

    sqlx::raw_sql("DELETE FROM awa.jobs WHERE queue = 'migration_test'; DELETE FROM awa.cron_jobs WHERE name = 'test_cron'; DELETE FROM awa.queue_meta WHERE queue = 'migration_test'")
        .execute(&pool)
        .await
        .unwrap();
}

// ── migration_sql() consistency ──────────────────────────────────

#[tokio::test]
async fn test_migration_sql_matches_run() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;

    reset_schema(&pool).await;
    migrations::run(&pool).await.unwrap();

    let tables_from_run: Vec<String> = sqlx::query_scalar(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'awa' ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    reset_schema(&pool).await;
    for (_version, _desc, sql) in migrations::migration_sql() {
        sqlx::raw_sql(&sql).execute(&pool).await.unwrap();
    }

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

    migrations::run(&pool).await.unwrap();
}

// ── Legacy version upgrade (0.3.x → 0.4.x) ─────────────────────

#[tokio::test]
async fn test_legacy_version_upgrade() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    let v1_sql = &migrations::migration_sql()[0].2;
    sqlx::raw_sql(v1_sql).execute(&pool).await.unwrap();

    sqlx::raw_sql(
        r#"
        DELETE FROM awa.schema_version;
        INSERT INTO awa.schema_version (version, description) VALUES (3, 'Legacy V3');
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let v2_sql = &migrations::migration_sql()[1].2;
    let v3_sql = &migrations::migration_sql()[2].2;
    sqlx::raw_sql(v2_sql).execute(&pool).await.unwrap();
    sqlx::raw_sql(v3_sql).execute(&pool).await.unwrap();

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

    migrations::run(&pool).await.unwrap();

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(
        version,
        migrations::CURRENT_VERSION,
        "Legacy version should be normalized to current"
    );

    let max_version: i32 =
        sqlx::query_scalar::<_, i32>("SELECT MAX(version) FROM awa.schema_version")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        max_version,
        migrations::CURRENT_VERSION,
        "Legacy version rows should be cleaned up, MAX should be {}",
        migrations::CURRENT_VERSION
    );

    let has_queue_state_counts: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'queue_state_counts')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        has_queue_state_counts,
        "V4 should be applied after normalization"
    );

    migrations::run(&pool).await.unwrap();
}

// ── migration_sql_range() selection ──────────────────────────────

#[tokio::test]
async fn test_migration_sql_range_produces_valid_schema() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    // Apply only V1+V2 via range, then verify V2 artifacts exist but V3+ don't.
    for (_version, _desc, sql) in migrations::migration_sql_range(0, 2) {
        sqlx::raw_sql(&sql).execute(&pool).await.unwrap();
    }

    let has_runtime: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'runtime_instances')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(has_runtime, "V2 should create runtime_instances");

    let has_maintenance: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'awa' AND table_name = 'runtime_instances' AND column_name = 'maintenance_alive')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!has_maintenance, "V3 should not be applied yet");

    // Now apply V3+V4 via range and verify.
    for (_version, _desc, sql) in migrations::migration_sql_range(2, migrations::CURRENT_VERSION) {
        sqlx::raw_sql(&sql).execute(&pool).await.unwrap();
    }

    let has_maintenance: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'awa' AND table_name = 'runtime_instances' AND column_name = 'maintenance_alive')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(has_maintenance, "V3 should be applied now");

    let has_admin: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'queue_state_counts')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(has_admin, "V4 should be applied now");

    let has_storage_transition_state: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'storage_transition_state')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        has_storage_transition_state,
        "V10 should create storage_transition_state"
    );

    // Full run() should still succeed (idempotent).
    migrations::run(&pool).await.unwrap();
}

#[tokio::test]
async fn test_storage_prepare_keeps_canonical_routing() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let status = storage::status(&pool).await.unwrap();
    assert_eq!(status.current_engine, "canonical");
    assert_eq!(status.active_engine, "canonical");
    assert_eq!(status.state, "canonical");
    assert_eq!(status.prepared_engine, None);

    let prepared = storage::prepare(
        &pool,
        "queue_storage",
        serde_json::json!({
            "schema": "awa_queue_storage"
        }),
    )
    .await
    .unwrap();
    assert_eq!(prepared.current_engine, "canonical");
    assert_eq!(prepared.active_engine, "canonical");
    assert_eq!(prepared.prepared_engine.as_deref(), Some("queue_storage"));
    assert_eq!(prepared.state, "prepared");
    assert_eq!(
        prepared.details,
        serde_json::json!({"schema": "awa_queue_storage"})
    );

    let inserted: awa::JobRow = sqlx::query_as(
        r#"
        SELECT *
        FROM awa.insert_job_compat(
            'storage_prepare_test',
            'prep_queue',
            '{}'::jsonb,
            'available'::awa.job_state,
            2::smallint,
            25::smallint,
            NULL::timestamptz,
            '{}'::jsonb,
            ARRAY[]::text[],
            NULL::bytea,
            NULL::bit(8)
        )
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(inserted.queue, "prep_queue");

    let hot_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.jobs_hot WHERE queue = 'prep_queue'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        hot_count, 1,
        "prepared state should still write canonical rows"
    );

    let aborted = storage::abort(&pool).await.unwrap();
    assert_eq!(aborted.state, "canonical");
    assert_eq!(aborted.active_engine, "canonical");
    assert_eq!(aborted.prepared_engine, None);
}

#[tokio::test]
async fn test_prepare_queue_storage_schema_does_not_activate_routing() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    prepare_queue_storage_schema(&pool, "awa_queue_storage_prepared").await;

    assert_eq!(
        active_queue_storage_schema(&pool).await,
        None,
        "schema preparation alone must not activate queue storage routing"
    );

    let status = storage::status(&pool).await.unwrap();
    assert_eq!(status.current_engine, "canonical");
    assert_eq!(status.active_engine, "canonical");
    assert_eq!(status.state, "canonical");

    assert!(
        relation_exists(&pool, "awa_queue_storage_prepared.ready_entries").await,
        "prepared queue-storage schema should materialize ready_entries"
    );
    assert!(
        relation_exists(&pool, "awa_queue_storage_prepared.leases").await,
        "prepared queue-storage schema should materialize leases"
    );
}

#[tokio::test]
async fn test_install_queue_storage_backend_activates_routing_and_state() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    install_queue_storage_backend(&pool, "awa_queue_storage_active").await;

    assert_eq!(
        active_queue_storage_schema(&pool).await.as_deref(),
        Some("awa_queue_storage_active"),
        "install helper should activate queue-storage routing immediately"
    );

    let status = storage::status(&pool).await.unwrap();
    assert_eq!(status.current_engine, "queue_storage");
    assert_eq!(status.active_engine, "queue_storage");
    assert_eq!(status.prepared_engine, None);
    assert_eq!(status.state, "active");
    assert_eq!(
        status.details,
        serde_json::json!({"schema": "awa_queue_storage_active"})
    );

    let inserted: awa::JobRow = sqlx::query_as(
        r#"
        SELECT *
        FROM awa.insert_job_compat(
            'install_queue_storage_test',
            'install_queue',
            '{}'::jsonb,
            'available'::awa.job_state,
            2::smallint,
            25::smallint,
            NULL::timestamptz,
            '{}'::jsonb,
            ARRAY[]::text[],
            NULL::bytea,
            NULL::bit(8)
        )
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(inserted.queue, "install_queue");

    let hot_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.jobs_hot WHERE queue = 'install_queue'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(hot_count, 0, "active queue storage should bypass jobs_hot");

    let queue_storage_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa_queue_storage_active.ready_entries WHERE queue = 'install_queue'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(queue_storage_count, 1);
}

#[tokio::test]
async fn test_storage_prepare_rejects_current_engine() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let err = storage::prepare(&pool, "canonical", serde_json::json!({}))
        .await
        .unwrap_err();
    assert_eq!(sqlstate_from_awa_error(&err).as_deref(), Some("22023"));
    assert!(
        err.to_string().contains("already active"),
        "expected already-active error, got {err}"
    );
}

#[tokio::test]
async fn test_storage_prepare_same_details_is_idempotent() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let details = serde_json::json!({
        "schema": "awa_queue_storage"
    });
    let prepared = storage::prepare(&pool, "queue_storage", details.clone())
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let prepared_again = storage::prepare(&pool, "queue_storage", details)
        .await
        .unwrap();
    assert_eq!(prepared_again.state, "prepared");
    assert_eq!(prepared_again.transition_epoch, prepared.transition_epoch);
    assert_eq!(prepared_again.entered_at, prepared.entered_at);
}

#[tokio::test]
async fn test_storage_abort_from_canonical_is_noop() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let before = storage::status(&pool).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let after = storage::abort(&pool).await.unwrap();

    assert_eq!(after.state, "canonical");
    assert_eq!(after.transition_epoch, before.transition_epoch);
    assert_eq!(after.entered_at, before.entered_at);
}

#[tokio::test]
async fn test_storage_abort_from_empty_mixed_transition_returns_to_canonical() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    prepare_queue_storage_schema(&pool, "awa_queue_storage").await;

    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET prepared_engine = 'queue_storage',
            state = 'mixed_transition',
            transition_epoch = transition_epoch + 1,
            details = '{"schema":"awa_queue_storage"}'::jsonb,
            entered_at = now(),
            updated_at = now(),
            finalized_at = NULL
        WHERE singleton
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at) VALUES ('queue_storage', 'awa_queue_storage', now())",
    )
    .execute(&pool)
    .await
    .unwrap();

    let before = storage::status(&pool).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let after = storage::abort(&pool).await.unwrap();

    assert_eq!(after.state, "canonical");
    assert_eq!(after.active_engine, "canonical");
    assert_eq!(after.prepared_engine, None);
    assert_eq!(after.transition_epoch, before.transition_epoch + 1);
    assert!(after.entered_at > before.entered_at);
    assert_eq!(active_queue_storage_schema(&pool).await, None);
}

#[tokio::test]
async fn test_storage_abort_from_mixed_transition_rejects_live_queue_storage_runtimes() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    prepare_queue_storage_schema(&pool, "awa_queue_storage").await;

    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET prepared_engine = 'queue_storage',
            state = 'mixed_transition',
            transition_epoch = transition_epoch + 1,
            details = '{"schema":"awa_queue_storage"}'::jsonb,
            entered_at = now(),
            updated_at = now(),
            finalized_at = NULL
        WHERE singleton
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at) VALUES ('queue_storage', 'awa_queue_storage', now())",
    )
    .execute(&pool)
    .await
    .unwrap();

    insert_runtime_instance(&pool, "queue_storage").await;

    let err = storage::abort(&pool).await.unwrap_err();
    assert_eq!(sqlstate_from_awa_error(&err).as_deref(), Some("55000"));
    assert!(
        err.to_string().contains("queue-storage runtime"),
        "got {err}"
    );
}

#[tokio::test]
async fn test_storage_abort_from_mixed_transition_rejects_queue_storage_rows() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    prepare_queue_storage_schema(&pool, "awa_queue_storage").await;

    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET prepared_engine = 'queue_storage',
            state = 'mixed_transition',
            transition_epoch = transition_epoch + 1,
            details = '{"schema":"awa_queue_storage"}'::jsonb,
            entered_at = now(),
            updated_at = now(),
            finalized_at = NULL
        WHERE singleton
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at) VALUES ('queue_storage', 'awa_queue_storage', now())",
    )
    .execute(&pool)
    .await
    .unwrap();

    let _: awa::JobRow = sqlx::query_as(
        r#"
        SELECT *
        FROM awa.insert_job_compat(
            'abort_mixed_transition_test',
            'abort_mixed_transition_queue',
            '{}'::jsonb,
            'available'::awa.job_state,
            2::smallint,
            25::smallint,
            NULL::timestamptz,
            '{}'::jsonb,
            ARRAY[]::text[],
            NULL::bytea,
            NULL::bit(8)
        )
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let err = storage::abort(&pool).await.unwrap_err();
    assert_eq!(sqlstate_from_awa_error(&err).as_deref(), Some("55000"));
    assert!(
        err.to_string().contains("queue storage contains"),
        "got {err}"
    );
}

#[tokio::test]
async fn test_storage_enter_mixed_transition_requires_prepared_queue_storage_runtime() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    prepare_queue_storage_schema(&pool, "awa_enter_mixed").await;

    let err = storage::enter_mixed_transition(&pool).await.unwrap_err();
    assert_eq!(sqlstate_from_awa_error(&err).as_deref(), Some("55000"));
    assert!(err.to_string().contains("prepared state"), "got {err}");

    storage::prepare(
        &pool,
        "queue_storage",
        serde_json::json!({"schema": "awa_enter_mixed"}),
    )
    .await
    .unwrap();

    let err = storage::enter_mixed_transition(&pool).await.unwrap_err();
    assert_eq!(sqlstate_from_awa_error(&err).as_deref(), Some("55000"));
    assert!(
        err.to_string().contains("queue-storage-capable runtime"),
        "got {err}"
    );

    insert_runtime_instance(&pool, "canonical").await;
    let err = storage::enter_mixed_transition(&pool).await.unwrap_err();
    assert_eq!(sqlstate_from_awa_error(&err).as_deref(), Some("55000"));
    assert!(
        err.to_string().contains("canonical-only runtime"),
        "got {err}"
    );

    sqlx::query("DELETE FROM awa.runtime_instances")
        .execute(&pool)
        .await
        .unwrap();
    insert_runtime_instance(&pool, "queue_storage").await;

    let status = storage::enter_mixed_transition(&pool).await.unwrap();
    assert_eq!(status.state, "mixed_transition");
    assert_eq!(status.current_engine, "canonical");
    assert_eq!(status.active_engine, "queue_storage");
    assert_eq!(status.prepared_engine.as_deref(), Some("queue_storage"));
    assert_eq!(
        active_queue_storage_schema(&pool).await,
        Some("awa_enter_mixed".to_string())
    );
}

#[tokio::test]
async fn test_storage_finalize_requires_empty_backlog_and_no_drain_runtimes() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    let schema = "awa_finalize_transition";
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    prepare_queue_storage_schema(&pool, schema).await;

    let err = storage::finalize(&pool).await.unwrap_err();
    assert_eq!(sqlstate_from_awa_error(&err).as_deref(), Some("55000"));
    assert!(err.to_string().contains("mixed_transition"), "got {err}");

    storage::prepare(
        &pool,
        "queue_storage",
        serde_json::json!({ "schema": schema }),
    )
    .await
    .unwrap();
    insert_runtime_instance(&pool, "queue_storage").await;
    storage::enter_mixed_transition(&pool).await.unwrap();

    sqlx::query(
        "INSERT INTO awa.jobs_hot (kind, queue, args, state, priority) VALUES ('finalize_backlog_job', 'finalize_queue', '{}'::jsonb, 'available', 2)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let err = storage::finalize(&pool).await.unwrap_err();
    assert_eq!(sqlstate_from_awa_error(&err).as_deref(), Some("55000"));
    assert!(
        err.to_string().contains("canonical live backlog"),
        "got {err}"
    );

    sqlx::query("DELETE FROM awa.jobs_hot WHERE queue = 'finalize_queue'")
        .execute(&pool)
        .await
        .unwrap();
    insert_runtime_instance(&pool, "canonical_drain_only").await;

    let err = storage::finalize(&pool).await.unwrap_err();
    assert_eq!(sqlstate_from_awa_error(&err).as_deref(), Some("55000"));
    assert!(err.to_string().contains("drain-only runtime"), "got {err}");

    sqlx::query(
        "DELETE FROM awa.runtime_instances WHERE storage_capability = 'canonical_drain_only'",
    )
    .execute(&pool)
    .await
    .unwrap();

    let status = storage::finalize(&pool).await.unwrap();
    assert_eq!(status.state, "active");
    assert_eq!(status.current_engine, "queue_storage");
    assert_eq!(status.active_engine, "queue_storage");
    assert_eq!(status.prepared_engine, None);
    assert!(status.finalized_at.is_some());
}

#[tokio::test]
async fn test_storage_status_report_surfaces_enter_mixed_transition_readiness() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    let schema = "awa_status_report_enter";
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    prepare_queue_storage_schema(&pool, schema).await;
    storage::prepare(
        &pool,
        "queue_storage",
        serde_json::json!({ "schema": schema }),
    )
    .await
    .unwrap();
    insert_runtime_instance(&pool, "queue_storage").await;

    let report = storage::status_report(&pool).await.unwrap();
    assert_eq!(report.status.state, "prepared");
    assert_eq!(
        report.prepared_queue_storage_schema.as_deref(),
        Some(schema)
    );
    assert!(report.prepared_schema_ready);
    assert_eq!(
        report.live_runtime_capability_counts.get("queue_storage"),
        Some(&1)
    );
    assert!(report.can_enter_mixed_transition);
    assert!(report.enter_mixed_transition_blockers.is_empty());
    assert!(!report.can_finalize);

    insert_runtime_instance(&pool, "canonical").await;
    let blocked = storage::status_report(&pool).await.unwrap();
    assert!(!blocked.can_enter_mixed_transition);
    assert!(
        blocked
            .enter_mixed_transition_blockers
            .iter()
            .any(|reason| reason.contains("canonical-only runtime")),
        "{:?}",
        blocked.enter_mixed_transition_blockers
    );
}

#[tokio::test]
async fn test_storage_status_report_surfaces_finalize_blockers() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    let schema = "awa_status_report_finalize";
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    prepare_queue_storage_schema(&pool, schema).await;
    storage::prepare(
        &pool,
        "queue_storage",
        serde_json::json!({ "schema": schema }),
    )
    .await
    .unwrap();
    insert_runtime_instance(&pool, "queue_storage").await;
    storage::enter_mixed_transition(&pool).await.unwrap();

    sqlx::query(
        "INSERT INTO awa.jobs_hot (kind, queue, args, state, priority) VALUES ('report_backlog_job', 'report_queue', '{}'::jsonb, 'available', 2)",
    )
    .execute(&pool)
    .await
    .unwrap();
    insert_runtime_instance(&pool, "canonical_drain_only").await;

    let report = storage::status_report(&pool).await.unwrap();
    assert_eq!(report.status.state, "mixed_transition");
    assert!(!report.can_finalize);
    assert_eq!(report.canonical_live_backlog, 1);
    assert!(
        report
            .finalize_blockers
            .iter()
            .any(|reason| reason.contains("canonical live backlog is 1")),
        "{:?}",
        report.finalize_blockers
    );
    assert!(
        report
            .finalize_blockers
            .iter()
            .any(|reason| reason.contains("drain-only runtime")),
        "{:?}",
        report.finalize_blockers
    );
}

#[tokio::test]
async fn test_mixed_transition_routes_compat_producers_into_queue_storage() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    let schema = "awa_migration_mixed_transition";
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    install_queue_storage_backend(&pool, schema).await;

    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET prepared_engine = 'queue_storage',
            state = 'mixed_transition',
            transition_epoch = transition_epoch + 1,
            details = $1::jsonb,
            updated_at = now(),
            finalized_at = NULL
        WHERE singleton
        "#,
    )
    .bind(format!(r#"{{"schema":"{schema}"}}"#))
    .execute(&pool)
    .await
    .unwrap();

    let compat_row = sqlx::query_as::<_, awa::JobRow>(
        r#"
        SELECT *
        FROM awa.insert_job_compat(
            'compat_mixed_transition_test',
            'compat_mixed_transition_queue',
            '{}'::jsonb,
            'available'::awa.job_state,
            2::smallint,
            25::smallint,
            NULL::timestamptz,
            '{}'::jsonb,
            ARRAY[]::text[],
            NULL::bytea,
            NULL::bit(8)
        )
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(compat_row.kind, "compat_mixed_transition_test");
    assert_eq!(compat_row.queue, "compat_mixed_transition_queue");

    let batch_rows = insert_many(
        &pool,
        &[InsertParams {
            kind: "compat_mixed_transition_batch".to_string(),
            args: serde_json::json!({"seq": 1}),
            opts: InsertOpts {
                queue: "compat_mixed_transition_batch".to_string(),
                ..Default::default()
            },
        }],
    )
    .await
    .unwrap();
    assert_eq!(batch_rows.len(), 1);
    assert_eq!(batch_rows[0].kind, "compat_mixed_transition_batch");

    let copy_rows = insert_many_copy_from_pool(
        &pool,
        &[InsertParams {
            kind: "compat_mixed_transition_copy".to_string(),
            args: serde_json::json!({"seq": 2}),
            opts: InsertOpts {
                queue: "compat_mixed_transition_copy".to_string(),
                unique: Some(UniqueOpts {
                    by_args: true,
                    by_queue: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        }],
    )
    .await
    .unwrap();
    assert_eq!(copy_rows.len(), 1);
    assert_eq!(copy_rows[0].kind, "compat_mixed_transition_copy");
}

#[tokio::test]
async fn test_insert_job_compat_routes_under_active_queue_storage_engine() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    let schema = "awa_migration_active_queue_storage";
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    install_queue_storage_backend(&pool, schema).await;

    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET prepared_engine = 'queue_storage',
            state = 'active',
            transition_epoch = transition_epoch + 1,
            details = $1::jsonb,
            updated_at = now(),
            finalized_at = now()
        WHERE singleton
        "#,
    )
    .bind(format!(r#"{{"schema":"{schema}"}}"#))
    .execute(&pool)
    .await
    .unwrap();

    let row = sqlx::query_as::<_, awa::JobRow>(
        r#"
        SELECT *
        FROM awa.insert_job_compat(
            'compat_refusal_test',
            'compat_refusal_queue',
            '{}'::jsonb,
            'available'::awa.job_state,
            2::smallint,
            25::smallint,
            NULL::timestamptz,
            '{}'::jsonb,
            ARRAY[]::text[],
            NULL::bytea,
            NULL::bit(8)
        )
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.kind, "compat_refusal_test");
    assert_eq!(row.queue, "compat_refusal_queue");
    assert_eq!(row.state, awa::JobState::Available);
}

#[tokio::test]
async fn test_insert_many_uses_insert_job_compat_after_non_canonical_activation() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    simulate_non_canonical_compat_routing(&pool).await;

    let queue = "compat_insert_many";
    let jobs = vec![
        InsertParams {
            kind: "compat_batch_job".to_string(),
            args: serde_json::json!({"seq": 1}),
            opts: InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        },
        InsertParams {
            kind: "compat_batch_job".to_string(),
            args: serde_json::json!({"seq": 2}),
            opts: InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        },
    ];

    let inserted = insert_many(&pool, &jobs).await.unwrap();
    assert_eq!(inserted.len(), 2);

    let stored: Vec<i64> = sqlx::query_scalar(
        "SELECT (args->>'seq')::bigint FROM awa.jobs WHERE queue = $1 ORDER BY 1",
    )
    .bind(queue)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(stored, vec![1, 2]);
}

#[tokio::test]
async fn test_insert_many_copy_uses_insert_job_compat_after_non_canonical_activation() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    simulate_non_canonical_compat_routing(&pool).await;

    let queue = "compat_insert_many_copy";
    let jobs = vec![
        InsertParams {
            kind: "compat_copy_job".to_string(),
            args: serde_json::json!({"seq": 10}),
            opts: InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        },
        InsertParams {
            kind: "compat_copy_job".to_string(),
            args: serde_json::json!({"seq": 11}),
            opts: InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        },
    ];

    let inserted = insert_many_copy_from_pool(&pool, &jobs).await.unwrap();
    assert_eq!(inserted.len(), 2);

    let stored: Vec<i64> = sqlx::query_scalar(
        "SELECT (args->>'seq')::bigint FROM awa.jobs WHERE queue = $1 ORDER BY 1",
    )
    .bind(queue)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(stored, vec![10, 11]);
}

#[tokio::test]
async fn test_insert_many_copy_preserves_unique_skip_after_non_canonical_activation() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    simulate_non_canonical_compat_routing(&pool).await;

    let queue = "compat_copy_unique";
    let unique_opts = InsertOpts {
        queue: queue.to_string(),
        unique: Some(UniqueOpts {
            by_args: true,
            by_queue: true,
            ..Default::default()
        }),
        ..Default::default()
    };

    let first_batch = vec![
        InsertParams {
            kind: "compat_unique_job".to_string(),
            args: serde_json::json!({"id": 1}),
            opts: unique_opts.clone(),
        },
        InsertParams {
            kind: "compat_unique_job".to_string(),
            args: serde_json::json!({"id": 2}),
            opts: unique_opts.clone(),
        },
    ];
    let first_inserted = insert_many_copy_from_pool(&pool, &first_batch)
        .await
        .unwrap();
    assert_eq!(first_inserted.len(), 2);

    let second_batch = vec![
        InsertParams {
            kind: "compat_unique_job".to_string(),
            args: serde_json::json!({"id": 1}),
            opts: unique_opts.clone(),
        },
        InsertParams {
            kind: "compat_unique_job".to_string(),
            args: serde_json::json!({"id": 3}),
            opts: unique_opts,
        },
    ];
    let second_inserted = insert_many_copy_from_pool(&pool, &second_batch)
        .await
        .unwrap();
    assert_eq!(second_inserted.len(), 1);
    assert_eq!(second_inserted[0].args["id"], 3);

    let stored: Vec<i64> = sqlx::query_scalar(
        "SELECT (args->>'id')::bigint FROM awa.jobs WHERE queue = $1 ORDER BY 1",
    )
    .bind(queue)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(stored, vec![1, 2, 3]);
}

// ── v011 self-heal regression tests ──────────────
//
// The 0.5.5 release shipped v010, which seeded the singleton row in
// awa.storage_transition_state via `INSERT … ON CONFLICT DO NOTHING`.
// The row could go missing in the wild — e.g. test fixtures that truncate
// awa tables between runs, partial v010 application that wrote the table
// but not the seed row, or logical dump/restore that omitted the table.
// When the row was missing, awa.active_storage_engine() returned NULL,
// which surfaced two user-visible bugs:
//
//   1. Single inserts (via awa.insert_job_compat) raised
//      `storage engine "<NULL>" is not writable in this release`.
//   2. insert_many silently inserted zero rows (NULL fails both
//      `engine = 'canonical'` and `engine <> 'canonical'`).
//
// v011 makes active_storage_engine() coalesce a missing/blank row to
// 'canonical', re-seeds the singleton, and hardens the assertion. The
// tests below exercise the two failure modes.

#[derive(Debug, Serialize, Deserialize, JobArgs)]
#[awa(kind = "self_heal_single_test")]
struct SelfHealSingleJob {
    pub value: String,
}

async fn delete_storage_transition_singleton(pool: &PgPool) {
    let result = sqlx::query("DELETE FROM awa.storage_transition_state WHERE singleton")
        .execute(pool)
        .await
        .expect("failed to delete storage_transition_state singleton");
    assert_eq!(
        result.rows_affected(),
        1,
        "expected to delete exactly one singleton row; the v010 seed must run before this helper or the test is not exercising the missing-row path"
    );
}

#[tokio::test]
async fn test_active_storage_engine_defaults_to_canonical_when_singleton_missing() {
    let _guard = test_mutex().lock().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    delete_storage_transition_singleton(&pool).await;

    let active: String = sqlx::query_scalar("SELECT awa.active_storage_engine()")
        .fetch_one(&pool)
        .await
        .expect("active_storage_engine should be NULL-safe after v011");
    assert_eq!(active, "canonical");

    let assert_ok: bool = sqlx::query_scalar("SELECT awa.assert_writable_canonical_storage()")
        .fetch_one(&pool)
        .await
        .expect("assert_writable_canonical_storage should treat missing singleton as canonical");
    assert!(assert_ok);
}

#[tokio::test]
async fn test_inserts_succeed_when_singleton_missing() {
    let _guard = test_mutex().lock().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    delete_storage_transition_singleton(&pool).await;

    // Single insert path goes through awa.insert_job_compat, which calls
    // assert_writable_canonical_storage. Pre-v011 this raised <NULL>.
    let single = awa::model::insert(
        &pool,
        &SelfHealSingleJob {
            value: "self_heal_single".to_string(),
        },
    )
    .await
    .expect("single insert must not fail when singleton row is missing");
    assert_eq!(single.kind, "self_heal_single_test");

    // insert_many uses the multi-row CTE that branches on the engine value.
    // Pre-v011 a NULL engine made both branches false and silently inserted
    // zero rows.
    let queue = "self_heal_many";
    let jobs = vec![
        InsertParams {
            kind: "self_heal_batch".to_string(),
            args: serde_json::json!({"seq": 1}),
            opts: InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        },
        InsertParams {
            kind: "self_heal_batch".to_string(),
            args: serde_json::json!({"seq": 2}),
            opts: InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        },
    ];
    let inserted = insert_many(&pool, &jobs)
        .await
        .expect("insert_many must not silently drop rows when singleton row is missing");
    assert_eq!(inserted.len(), 2);

    // insert_many_copy reads active_storage_engine() into a Rust String,
    // which would error on NULL pre-v011.
    let copy_queue = "self_heal_copy";
    let copy_jobs = vec![InsertParams {
        kind: "self_heal_copy_job".to_string(),
        args: serde_json::json!({"seq": 100}),
        opts: InsertOpts {
            queue: copy_queue.to_string(),
            ..Default::default()
        },
    }];
    let copy_inserted = insert_many_copy_from_pool(&pool, &copy_jobs)
        .await
        .expect("insert_many_copy must not fail when singleton row is missing");
    assert_eq!(copy_inserted.len(), 1);
}

#[tokio::test]
async fn test_v011_reseeds_singleton_when_upgrading_from_v010() {
    let _guard = test_mutex().lock().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    // Simulate a database that received v010 but lost its singleton row
    // and is being re-migrated from v10 → current. Roll the recorded
    // schema_version back to 10 so v011 re-runs.
    delete_storage_transition_singleton(&pool).await;
    sqlx::query("DELETE FROM awa.schema_version WHERE version > 10")
        .execute(&pool)
        .await
        .unwrap();

    migrations::run(&pool).await.unwrap();

    let row_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.storage_transition_state WHERE singleton")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        row_count, 1,
        "v011 should re-seed the singleton row when missing"
    );

    let status = storage::status(&pool)
        .await
        .expect("storage_status should succeed after re-seed");
    assert_eq!(status.current_engine, "canonical");
    assert_eq!(status.active_engine, "canonical");
    assert_eq!(status.state, "canonical");
}

// ── Legacy V3-only upgrade (0.3.0 exact, no V4/V5) ──────────────

#[tokio::test]
async fn test_legacy_v3_only_upgrade() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    let v1_sql = &migrations::migration_sql()[0].2;
    sqlx::raw_sql(v1_sql).execute(&pool).await.unwrap();

    sqlx::raw_sql(
        r#"
        DELETE FROM awa.schema_version;
        INSERT INTO awa.schema_version (version, description) VALUES (3, 'Legacy V3 only');
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let has_runtime: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'runtime_instances')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        !has_runtime,
        "runtime_instances should not exist in legacy V3"
    );

    migrations::run(&pool).await.unwrap();

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(
        version,
        migrations::CURRENT_VERSION,
        "Legacy V3-only should upgrade to current"
    );

    let has_runtime: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'runtime_instances')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(has_runtime, "runtime_instances should exist after upgrade");

    let has_col: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = 'awa' AND table_name = 'runtime_instances' AND column_name = 'maintenance_alive')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(has_col, "maintenance_alive should exist after upgrade");

    let has_queue_state_counts: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'queue_state_counts')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        has_queue_state_counts,
        "queue_state_counts should exist after upgrade"
    );

    migrations::run(&pool).await.unwrap();
}

/// Auto-finalize fast-path for fresh installs.
///
/// When state=canonical, prepared_engine=NULL, no canonical jobs ever, and
/// no live workers, awa.storage_auto_finalize_if_fresh skips the staged
/// transition and lands directly in active. This is the entry point that
/// closes the 0.6 fresh-install UX gap (issue #197).
#[tokio::test]
async fn test_auto_finalize_promotes_fresh_install() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let baseline = storage::status(&pool).await.unwrap();
    assert_eq!(baseline.state, "canonical");
    assert_eq!(baseline.prepared_engine, None);

    let promoted: bool = sqlx::query_scalar("SELECT awa.storage_auto_finalize_if_fresh($1)")
        .bind("awa")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(promoted, "fresh DB should promote on first call");

    let after = storage::status(&pool).await.unwrap();
    assert_eq!(after.state, "active");
    assert_eq!(after.current_engine, "queue_storage");
    assert_eq!(after.active_engine, "queue_storage");
    assert_eq!(after.prepared_engine, None);
    let auto_finalized = after
        .details
        .get("auto_finalized")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(
        auto_finalized,
        "auto_finalized=true should be in details: {:?}",
        after.details
    );

    // Idempotent: second call from active state returns false.
    let promoted_again: bool = sqlx::query_scalar("SELECT awa.storage_auto_finalize_if_fresh($1)")
        .bind("awa")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        !promoted_again,
        "auto-finalize from active should be a no-op"
    );
}

/// Auto-finalize must refuse to skip the staged transition once any
/// canonical work has touched the DB. Operators with existing 0.5.x
/// data must go through prepare → enter-mixed-transition → finalize so
/// canonical drain is observable.
#[tokio::test]
async fn test_auto_finalize_refuses_when_canonical_jobs_exist() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    sqlx::query(
        "INSERT INTO awa.jobs_hot (kind, queue, args, state, priority) \
         VALUES ('legacy_job', 'legacy_queue', '{}'::jsonb, 'completed', 2)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let promoted: bool = sqlx::query_scalar("SELECT awa.storage_auto_finalize_if_fresh($1)")
        .bind("awa")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!promoted, "DB with canonical jobs should not auto-finalize");

    let after = storage::status(&pool).await.unwrap();
    assert_eq!(after.state, "canonical");
}

/// Auto-finalize must respect an in-progress staged transition. If an
/// operator has already run `storage prepare`, the prepared_engine is
/// set and auto-finalize must defer to the staged path.
#[tokio::test]
async fn test_auto_finalize_refuses_when_prepared_engine_is_set() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    storage::prepare(
        &pool,
        "queue_storage",
        serde_json::json!({ "schema": "awa" }),
    )
    .await
    .unwrap();

    let promoted: bool = sqlx::query_scalar("SELECT awa.storage_auto_finalize_if_fresh($1)")
        .bind("awa")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        !promoted,
        "DB with explicit prepared_engine should not auto-finalize"
    );

    let after = storage::status(&pool).await.unwrap();
    assert_eq!(after.state, "prepared");
    assert_eq!(after.prepared_engine.as_deref(), Some("queue_storage"));
}

/// Auto-finalize must refuse when a worker is already heartbeating;
/// this prevents quietly skipping the canonical-drain interlock if a
/// worker fleet is already live during what was thought to be a fresh
/// install.
#[tokio::test]
async fn test_auto_finalize_refuses_when_runtime_is_live() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    insert_runtime_instance(&pool, "canonical").await;

    let promoted: bool = sqlx::query_scalar("SELECT awa.storage_auto_finalize_if_fresh($1)")
        .bind("awa")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        !promoted,
        "DB with live runtime instances should not auto-finalize"
    );

    let after = storage::status(&pool).await.unwrap();
    assert_eq!(after.state, "canonical");
}
