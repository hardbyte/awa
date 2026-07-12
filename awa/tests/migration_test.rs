//! Migration tests: step-through upgrade, data survival, idempotency,
//! and migration_sql() consistency.
//!
//! Tests serialize through a Postgres advisory lock (plus a process-local
//! mutex), so they are safe under parallel threads and under per-test
//! processes — CI shards this binary with `cargo nextest --partition`.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{insert_many, insert_many_copy_from_pool, migrations, storage, QueueStorage};
use awa::{InsertOpts, InsertParams, JobArgs, UniqueOpts};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
use std::str::FromStr;
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
        // Tolerate the duplicate-database race: under per-test processes
        // (cargo-nextest CI shards) another test can create it between the
        // existence check and this statement.
        if let Err(err) = sqlx::raw_sql("CREATE DATABASE awa_migration_test")
            .execute(&mut admin)
            .await
        {
            let duplicate = matches!(
                &err,
                sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("42P04")
            );
            assert!(duplicate, "Failed to create migration test database: {err}");
        }
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
    sqlx::query("DROP SCHEMA IF EXISTS awa_queue_storage CASCADE")
        .execute(pool)
        .await
        .expect("compat routing schema should drop cleanly");

    let store = QueueStorage::from_existing_schema("awa_queue_storage")
        .expect("compat routing queue storage schema should validate");
    store
        .prepare_schema(pool)
        .await
        .expect("compat routing queue storage schema should prepare cleanly");

    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET current_engine = 'queue_storage',
            prepared_engine = NULL,
            state = 'active',
            transition_epoch = transition_epoch + 1,
            details = '{"schema":"awa_queue_storage"}'::jsonb,
            updated_at = now(),
            finalized_at = now()
        WHERE singleton
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

fn assert_safe_generated_role_name(role: &str) {
    assert!(
        role.chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_'),
        "generated test role contains unsafe characters: {role}"
    );
}

async fn create_login_role(pool: &PgPool, role: &str) {
    assert_safe_generated_role_name(role);
    sqlx::query(&format!("DROP ROLE IF EXISTS {role}"))
        .execute(pool)
        .await
        .expect("test role should be dropped before create");
    sqlx::query(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'awa_test_password'"
    ))
    .execute(pool)
    .await
    .expect("test role should be created");
}

async fn drop_login_role(pool: &PgPool, role: &str) {
    assert_safe_generated_role_name(role);
    let _ = sqlx::query(&format!("DROP ROLE IF EXISTS {role}"))
        .execute(pool)
        .await;
}

async fn grant_runtime_privileges(pool: &PgPool, role: &str, include_truncate: bool) {
    assert_safe_generated_role_name(role);
    let table_privileges = if include_truncate {
        "SELECT, INSERT, UPDATE, DELETE, TRUNCATE"
    } else {
        "SELECT, INSERT, UPDATE, DELETE"
    };
    sqlx::raw_sql(&format!(
        r#"
        GRANT CONNECT ON DATABASE awa_migration_test TO {role};
        GRANT USAGE ON SCHEMA awa TO {role};
        GRANT USAGE, SELECT ON SEQUENCE awa.jobs_id_seq TO {role};
        GRANT {table_privileges} ON ALL TABLES IN SCHEMA awa TO {role};
        GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa TO {role};
        REVOKE EXECUTE ON FUNCTION awa.install_queue_storage_substrate(TEXT, INT, INT, INT, BOOLEAN) FROM {role};
        "#
    ))
    .execute(pool)
    .await
    .expect("runtime grants should apply");
}

async fn runtime_pool_for_role(role: &str) -> PgPool {
    assert_safe_generated_role_name(role);
    let options = PgConnectOptions::from_str(&migration_database_url())
        .expect("migration database URL should parse")
        .username(role)
        .password("awa_test_password");
    PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect_with(options)
        .await
        .expect("runtime role should connect")
}

async fn relation_exists(pool: &PgPool, qualified_relname: &str) -> bool {
    sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(qualified_relname)
        .fetch_one(pool)
        .await
        .expect("relation existence query should succeed")
}

async fn column_exists(pool: &PgPool, schema: &str, table: &str, column: &str) -> bool {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = $2
              AND column_name = $3
        )
        "#,
    )
    .bind(schema)
    .bind(table)
    .bind(column)
    .fetch_one(pool)
    .await
    .expect("column existence query should succeed")
}

async fn insert_runtime_instance(pool: &PgPool, capability: &str) {
    // Default each capability to a coherent operator-role pairing so existing
    // tests don't accidentally create invalid states. Tests that exercise the
    // pre-flip auto-role witness call `insert_runtime_instance_with_role`
    // directly.
    let role = match capability {
        "canonical" => "auto",
        "canonical_drain_only" => "canonical_drain",
        "queue_storage" => "queue_storage_target",
        _ => "auto",
    };
    insert_runtime_instance_with_role(pool, capability, role).await;
}

async fn insert_runtime_instance_with_role(pool: &PgPool, capability: &str, transition_role: &str) {
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
            storage_capability,
            transition_role
        )
        VALUES (
            $1, 'test-host', 4242, '0.6.2',
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
            $2,
            $3
        )
        ON CONFLICT (instance_id)
        DO UPDATE SET
            last_seen_at = EXCLUDED.last_seen_at,
            storage_capability = EXCLUDED.storage_capability,
            transition_role = EXCLUDED.transition_role
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(capability)
    .bind(transition_role)
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
    assert!(
        relation_exists(&pool, "awa.runtime_storage_backends").await,
        "runtime_storage_backends should be migration-owned"
    );
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

// ── 0.7 migrate gate (#370 / ADR-037) ────────────────────────────

/// Rewind `schema_version` so `run()` sees pending migrations again. The
/// re-applied migrations are idempotent by policy, so this only re-runs SQL
/// that is safe to repeat.
async fn rewind_schema_version(pool: &PgPool, from_version: i32) {
    sqlx::query("DELETE FROM awa.schema_version WHERE version >= $1")
        .bind(from_version)
        .execute(pool)
        .await
        .expect("schema_version rewind should succeed");
}

fn assert_storage_not_finalized(err: &awa::AwaError, expected_state: &str) {
    assert!(
        matches!(
            err,
            awa::AwaError::StorageNotFinalized { state } if state == expected_state
        ),
        "expected StorageNotFinalized with state '{expected_state}', got: {err}"
    );
}

#[tokio::test]
async fn test_migrate_gate_allows_effectively_fresh_canonical() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    rewind_schema_version(&pool, migrations::CURRENT_VERSION).await;

    // Canonical state, no prepared engine, no jobs, no live runtimes —
    // the same conditions awa.storage_auto_finalize_if_fresh accepts.
    migrations::run(&pool)
        .await
        .expect("effectively-fresh canonical cluster should migrate");
    assert_eq!(
        migrations::current_version(&pool).await.unwrap(),
        migrations::CURRENT_VERSION
    );
}

#[tokio::test]
async fn test_migrate_gate_allows_active_cluster() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    simulate_non_canonical_compat_routing(&pool).await;
    insert_runtime_instance(&pool, "queue_storage").await;
    // Rewind one version so the pending range crosses v042. The live runtime
    // reports the 0.6.2 compatibility floor, so migration remains rolling.
    rewind_schema_version(&pool, migrations::CURRENT_VERSION).await;

    migrations::run(&pool)
        .await
        .expect("finalized cluster with a live 0.6.2 runtime should migrate");
    assert_eq!(
        migrations::current_version(&pool).await.unwrap(),
        migrations::CURRENT_VERSION
    );
}

#[tokio::test]
async fn test_migrate_gate_refuses_canonical_cluster_with_jobs() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    sqlx::raw_sql(
        r#"
        SELECT awa.insert_job_compat(
            'gate_test_job', 'gate_queue', '{}'::jsonb,
            'available'::awa.job_state, 2::smallint, 25::smallint,
            NULL::timestamptz, '{}'::jsonb, ARRAY[]::text[],
            NULL::bytea, NULL::bit(8)
        )
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    rewind_schema_version(&pool, migrations::CURRENT_VERSION).await;

    let err = migrations::run(&pool).await.unwrap_err();
    assert_storage_not_finalized(&err, "canonical");
}

#[tokio::test]
async fn test_migrate_gate_refuses_prepared_state() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    storage::prepare(
        &pool,
        "queue_storage",
        serde_json::json!({"schema": "awa_queue_storage"}),
    )
    .await
    .unwrap();
    rewind_schema_version(&pool, migrations::CURRENT_VERSION).await;

    let err = migrations::run(&pool).await.unwrap_err();
    assert_storage_not_finalized(&err, "prepared");

    storage::abort(&pool).await.unwrap();
}

#[tokio::test]
async fn test_migrate_gate_refuses_mixed_transition_state() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    sqlx::raw_sql(
        r#"
        UPDATE awa.storage_transition_state
        SET state = 'mixed_transition',
            prepared_engine = 'queue_storage',
            details = '{"schema":"awa_queue_storage"}'::jsonb,
            transition_epoch = transition_epoch + 1,
            updated_at = now()
        WHERE singleton
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    rewind_schema_version(&pool, migrations::CURRENT_VERSION).await;

    let err = migrations::run(&pool).await.unwrap_err();
    assert_storage_not_finalized(&err, "mixed_transition");
}

#[tokio::test]
async fn test_migrate_gate_refuses_recently_live_canonical_runtime() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    insert_runtime_instance(&pool, "canonical").await;
    rewind_schema_version(&pool, migrations::CURRENT_VERSION).await;

    let err = migrations::run(&pool).await.unwrap_err();
    assert_storage_not_finalized(&err, "canonical");

    // Once the runtime's heartbeat is stale the cluster counts as fresh
    // again — matching the auto-finalize 90-second window.
    sqlx::raw_sql("UPDATE awa.runtime_instances SET last_seen_at = now() - interval '10 minutes'")
        .execute(&pool)
        .await
        .unwrap();
    migrations::run(&pool)
        .await
        .expect("stale runtimes should not hold the migrate gate closed");
}

#[tokio::test]
async fn test_documented_runtime_grants_cover_admin_refresh_truncate() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let runtime_without_truncate = format!("awa_runtime_no_truncate_{suffix}");
    let runtime_with_truncate = format!("awa_runtime_with_truncate_{suffix}");

    create_login_role(&pool, &runtime_without_truncate).await;
    create_login_role(&pool, &runtime_with_truncate).await;

    grant_runtime_privileges(&pool, &runtime_without_truncate, false).await;
    grant_runtime_privileges(&pool, &runtime_with_truncate, true).await;

    let old_doc_pool = runtime_pool_for_role(&runtime_without_truncate).await;
    let err = awa::model::admin::refresh_admin_metadata(&old_doc_pool)
        .await
        .expect_err("runtime grants without TRUNCATE should fail during metadata refresh");
    assert_eq!(
        sqlstate_from_awa_error(&err).as_deref(),
        Some("42501"),
        "missing TRUNCATE should surface as insufficient_privilege"
    );
    old_doc_pool.close().await;

    let documented_pool = runtime_pool_for_role(&runtime_with_truncate).await;
    awa::model::admin::refresh_admin_metadata(&documented_pool)
        .await
        .expect("documented runtime grants should allow metadata refresh");
    documented_pool.close().await;

    drop_login_role(&pool, &runtime_without_truncate).await;
    drop_login_role(&pool, &runtime_with_truncate).await;
}

#[tokio::test]
async fn test_install_queue_storage_substrate_is_not_public_executable() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.unwrap();

    let public_can_execute: bool = sqlx::query_scalar(
        "SELECT has_function_privilege('public', 'awa.install_queue_storage_substrate(text,integer,integer,integer,boolean)', 'EXECUTE')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        !public_can_execute,
        "queue-storage substrate installer must not be executable by PUBLIC"
    );
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

    // Step 2: the 0.7 migrate gate (#370 / ADR-037) refuses run() while
    // canonical jobs exist on an unfinalized cluster — the operator is
    // expected to finish the staged transition on a 0.6 binary first.
    let gate_err = migrations::run(&pool).await.unwrap_err();
    assert!(
        matches!(
            &gate_err,
            awa::AwaError::StorageNotFinalized { state } if state == "canonical"
        ),
        "expected StorageNotFinalized for canonical cluster with jobs, got: {gate_err}"
    );

    // Step 3: apply the remaining migrations the way a 0.6-line binary
    // would (raw migration SQL) and verify the seeded data survives.
    for (_version, _desc, sql) in migrations::migration_sql_range(1, migrations::CURRENT_VERSION) {
        sqlx::raw_sql(&sql).execute(&pool).await.unwrap();
    }

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);

    // With no pending migrations the gate is not consulted: run() stays a
    // no-op success on an up-to-date cluster whatever its transition state.
    migrations::run(&pool).await.unwrap();

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

#[tokio::test]
async fn test_v023_migrates_legacy_default_queue_storage_tables() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    for (_version, _desc, sql) in migrations::migration_sql_range(0, 22) {
        sqlx::raw_sql(&sql).execute(&pool).await.unwrap();
    }

    sqlx::raw_sql(
        r#"
        CREATE TABLE awa.open_receipt_claims (job_id bigint);
        CREATE TABLE awa.queue_count_snapshots (queue text);
        CREATE TABLE awa.lease_claims (
            job_id           BIGINT NOT NULL,
            run_lease        BIGINT NOT NULL,
            ready_slot       INT NOT NULL,
            ready_generation BIGINT NOT NULL,
            queue            TEXT NOT NULL,
            priority         SMALLINT NOT NULL,
            attempt          SMALLINT NOT NULL,
            max_attempts     SMALLINT NOT NULL,
            lane_seq         BIGINT NOT NULL,
            enqueue_shard    SMALLINT NOT NULL DEFAULT 0,
            claimed_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            materialized_at  TIMESTAMPTZ,
            deadline_at      TIMESTAMPTZ
        );
        CREATE TABLE awa.lease_claim_closures (
            job_id    BIGINT NOT NULL,
            run_lease BIGINT NOT NULL,
            outcome   TEXT NOT NULL,
            closed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        );

        INSERT INTO awa.lease_claims (
            job_id, run_lease, ready_slot, ready_generation, queue, priority,
            attempt, max_attempts, lane_seq, enqueue_shard, claimed_at,
            materialized_at, deadline_at
        )
        SELECT
            gs, 1, 0, 0, 'legacy_default', 2::smallint,
            0::smallint, 25::smallint, gs, (gs % 2)::smallint,
            clock_timestamp(), NULL::timestamptz,
            TIMESTAMPTZ '2030-01-01 00:00:00+00'
        FROM generate_series(1, 5) AS gs;

        INSERT INTO awa.lease_claim_closures (job_id, run_lease, outcome, closed_at)
        SELECT gs, 1, 'completed', clock_timestamp()
        FROM generate_series(1, 2) AS gs;
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    migrations::run(&pool).await.unwrap();

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);

    let lease_claims_relkind: String = sqlx::query_scalar(
        r#"
        SELECT c.relkind::text
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = 'awa'
          AND c.relname = 'lease_claims'
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        lease_claims_relkind, "p",
        "v023 should rebuild legacy lease_claims as a partitioned parent"
    );

    let migrated_claims: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.lease_claims WHERE queue = 'legacy_default'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(migrated_claims, 5);

    let preserved_claim_shape: (i16, bool) = sqlx::query_as(
        r#"
        SELECT enqueue_shard,
               deadline_at = TIMESTAMPTZ '2030-01-01 00:00:00+00'
        FROM awa.lease_claims
        WHERE queue = 'legacy_default'
          AND job_id = 3
          AND run_lease = 1
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(preserved_claim_shape, (1, true));

    let migrated_closures: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.lease_claim_closures WHERE outcome = 'completed'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(migrated_closures, 2);

    for relname in [
        "awa.lease_claims_legacy",
        "awa.lease_claim_closures_legacy",
        "awa.open_receipt_claims",
        "awa.queue_count_snapshots",
    ] {
        assert!(
            !relation_exists(&pool, relname).await,
            "{relname} should be removed by the v023 default-schema cleanup"
        );
    }
}

#[tokio::test]
async fn test_v027_rebuckets_existing_terminal_live_counts() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    for (_version, _desc, sql) in migrations::migration_sql_range(0, 26) {
        sqlx::raw_sql(&sql).execute(&pool).await.unwrap();
    }

    sqlx::raw_sql(
        r#"
        INSERT INTO awa.done_entries (
            ready_slot, ready_generation, job_id, kind, queue, state,
            priority, attempt, run_lease, lane_seq, enqueue_shard,
            attempted_at, finalized_at, payload
        ) VALUES
            (0, 0, 1001, 'migration_job', 'v27_rebucket', 'completed'::awa.job_state,
             2::smallint, 1::smallint, 1::bigint, 1::bigint, 0::smallint,
             now(), now(), '{}'::jsonb),
            (0, 0, 1002, 'migration_job', 'v27_rebucket', 'completed'::awa.job_state,
             2::smallint, 1::smallint, 1::bigint, 2::bigint, 0::smallint,
             now(), now(), '{}'::jsonb);

        TRUNCATE TABLE awa.queue_terminal_live_counts;
        INSERT INTO awa.queue_terminal_live_counts (
            ready_slot, queue, priority, enqueue_shard, counter_bucket, live_terminal_count
        ) VALUES (
            0, 'v27_rebucket', 2::smallint, 0::smallint, 0::smallint, 2::bigint
        );

        UPDATE awa.queue_ring_state
        SET terminal_counter_trusted_at = now()
        WHERE singleton = TRUE;
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    migrations::run(&pool).await.unwrap();

    let buckets: Vec<(i16, i64)> = sqlx::query_as(
        r#"
        SELECT counter_bucket, live_terminal_count
        FROM awa.queue_terminal_live_counts
        WHERE queue = 'v27_rebucket'
        ORDER BY counter_bucket
        "#,
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        buckets,
        vec![(233_i16, 1), (234_i16, 1)],
        "v27 should rebuild existing terminal live counters into job_id buckets"
    );

    let trusted: bool = sqlx::query_scalar(
        "SELECT terminal_counter_trusted_at IS NOT NULL FROM awa.queue_ring_state WHERE singleton = TRUE",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        trusted,
        "v27 rebuild should leave the rebuilt counter trusted"
    );
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
        relation_exists(&pool, "awa_queue_storage_prepared.ready_tombstones").await,
        "prepared queue-storage schema should materialize ready_tombstones"
    );
    assert!(
        relation_exists(
            &pool,
            "awa_queue_storage_prepared.queue_terminal_count_deltas"
        )
        .await,
        "prepared queue-storage schema should materialize queue_terminal_count_deltas"
    );
    let has_delta_nonzero_constraint: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM pg_constraint AS c
            JOIN pg_class AS t ON t.oid = c.conrelid
            JOIN pg_namespace AS n ON n.oid = t.relnamespace
            WHERE n.nspname = 'awa_queue_storage_prepared'
              AND t.relname = 'queue_terminal_count_deltas'
              AND c.conname = 'queue_terminal_count_deltas_nonzero'
        )
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("terminal delta constraint probe should succeed");
    assert!(
        has_delta_nonzero_constraint,
        "queue_terminal_count_deltas should reject no-op zero deltas"
    );
    let has_done_failed_index: bool = sqlx::query_scalar(
        "SELECT to_regclass('awa_queue_storage_prepared.idx_awa_queue_storage_prepared_done_0_failed_queue') IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("failed done_entries index probe should succeed");
    assert!(
        has_done_failed_index,
        "done_entries failed-row metric probe should have a partial index"
    );
    let has_receipt_batch_lane_index: bool = sqlx::query_scalar(
        "SELECT to_regclass('awa_queue_storage_prepared.idx_awa_queue_storage_prepared_receipt_completion_batches_0_lane') IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("receipt completion batch lane index probe should succeed");
    assert!(
        has_receipt_batch_lane_index,
        "receipt_completion_batches should keep the segment-routing index"
    );
    let has_receipt_batch_job_ids_gin: bool = sqlx::query_scalar(
        "SELECT to_regclass('awa_queue_storage_prepared.idx_awa_queue_storage_prepared_receipt_completion_batches_0_job_ids') IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("receipt completion batch job_ids index probe should succeed");
    assert!(
        !has_receipt_batch_job_ids_gin,
        "receipt_completion_batches must not maintain a hot-path GIN index for cold job-id deletes"
    );
    assert!(
        relation_exists(&pool, "awa_queue_storage_prepared.leases").await,
        "prepared queue-storage schema should materialize leases"
    );
}

#[tokio::test]
async fn test_v031_backfills_queue_storage_failed_done_metric_index() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let schema = "awa_queue_storage_v031_index";
    prepare_queue_storage_schema(&pool, schema).await;

    let index_name = format!("idx_{schema}_done_0_failed_queue");
    sqlx::query(&format!("DROP INDEX IF EXISTS {schema}.{index_name}"))
        .execute(&pool)
        .await
        .expect("failed done_entries test index should drop cleanly");

    sqlx::raw_sql(
        r#"
        DROP SCHEMA IF EXISTS awa_queue_storage_v031_partial CASCADE;
        CREATE SCHEMA awa_queue_storage_v031_partial;
        CREATE TABLE awa_queue_storage_v031_partial.queue_ring_state (
            singleton BOOLEAN PRIMARY KEY,
            slot_count INT NOT NULL
        );
        INSERT INTO awa_queue_storage_v031_partial.queue_ring_state
            (singleton, slot_count)
        VALUES (TRUE, 1);
        CREATE TABLE awa_queue_storage_v031_partial.done_entries (
            ready_slot INT NOT NULL,
            queue TEXT NOT NULL
        ) PARTITION BY LIST (ready_slot);
        CREATE TABLE awa_queue_storage_v031_partial.done_entries_0
            PARTITION OF awa_queue_storage_v031_partial.done_entries
            FOR VALUES IN (0);
        "#,
    )
    .execute(&pool)
    .await
    .expect("partial queue-storage probe schema should be creatable");

    sqlx::query("DELETE FROM awa.schema_version WHERE version >= 31")
        .execute(&pool)
        .await
        .expect("schema_version rewind should succeed");

    migrations::run(&pool)
        .await
        .expect("v031 should rerun cleanly");

    let has_done_failed_index: bool = sqlx::query_scalar(&format!(
        "SELECT to_regclass('{schema}.{index_name}') IS NOT NULL"
    ))
    .fetch_one(&pool)
    .await
    .expect("failed done_entries index probe should succeed");
    assert!(
        has_done_failed_index,
        "v031 must backfill failed-row metric indexes for existing queue-storage schemas"
    );

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);
}

/// v041 (#246) refreshes every installed queue-storage schema so the compact
/// deadline-claim machinery reaches schemas prepared before the upgrade: the
/// per-slot batch deadline-rescue cursor columns on `claim_ring_slots` and
/// the partial `(deadline_at, batch_id)` sweep index on each
/// `lease_claim_batches` child. Simulate a pre-v041 schema by stripping both,
/// rewind `schema_version`, and prove `run()` reinstalls them via the v023
/// install helper the migration re-invokes.
#[tokio::test]
async fn test_v041_refreshes_compact_deadline_cursors_and_index() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let schema = "awa_queue_storage_v041_deadline";
    prepare_queue_storage_schema(&pool, schema).await;

    // Strip the v041 additions to mimic a schema installed before #246.
    sqlx::raw_sql(&format!(
        r#"
        ALTER TABLE {schema}.claim_ring_slots
            DROP COLUMN IF EXISTS batch_deadline_cursor_deadline_at,
            DROP COLUMN IF EXISTS batch_deadline_cursor_batch_id;
        DROP INDEX IF EXISTS {schema}.idx_{schema}_lease_claim_batches_0_deadline_cursor;
        "#
    ))
    .execute(&pool)
    .await
    .expect("stripping v041 additions should succeed");

    let has_cursor_before = column_exists(
        &pool,
        schema,
        "claim_ring_slots",
        "batch_deadline_cursor_batch_id",
    )
    .await;
    assert!(
        !has_cursor_before,
        "precondition: the batch deadline cursor column must be absent before v041 reruns"
    );

    sqlx::query("DELETE FROM awa.schema_version WHERE version >= 41")
        .execute(&pool)
        .await
        .expect("schema_version rewind should succeed");

    migrations::run(&pool)
        .await
        .expect("v041 should rerun cleanly");

    let has_cursor_deadline = column_exists(
        &pool,
        schema,
        "claim_ring_slots",
        "batch_deadline_cursor_deadline_at",
    )
    .await;
    let has_cursor_batch = column_exists(
        &pool,
        schema,
        "claim_ring_slots",
        "batch_deadline_cursor_batch_id",
    )
    .await;
    assert!(
        has_cursor_deadline && has_cursor_batch,
        "v041 must backfill the batch deadline-rescue cursor columns on existing schemas"
    );

    // The migration contract covers EVERY lease_claim_batches child, not
    // just child 0 — enumerate the installed children and assert the
    // partial sweep index (deadline_at IS NOT NULL, so zero-deadline
    // traffic never maintains entries) exists on each.
    let child_slots: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT substring(tablename FROM 'lease_claim_batches_(\\d+)$')
         FROM pg_tables
         WHERE schemaname = '{schema}'
           AND tablename ~ '^lease_claim_batches_\\d+$'
         ORDER BY 1"
    ))
    .fetch_all(&pool)
    .await
    .expect("enumerate lease_claim_batches children");
    assert!(
        !child_slots.is_empty(),
        "expected at least one lease_claim_batches child partition"
    );
    for slot in &child_slots {
        let index_name = format!("idx_{schema}_lease_claim_batches_{slot}_deadline_cursor");
        // Resolve by OID (relnames are truncated to 63 chars) and read the
        // definition in one probe; NULL means the index is missing.
        let index_def: Option<String> = sqlx::query_scalar(&format!(
            "SELECT pg_get_indexdef(to_regclass('{schema}.{index_name}')::oid)"
        ))
        .fetch_one(&pool)
        .await
        .expect("index definition probe should succeed");
        let index_def = index_def.unwrap_or_else(|| {
            panic!(
                "v041 must backfill the partial batch-deadline sweep index on                  lease_claim_batches child {slot}"
            )
        });
        assert!(
            index_def.contains("deadline_at IS NOT NULL"),
            "child {slot}: the batch-deadline index must be partial on              deadline_at IS NOT NULL, got: {index_def}"
        );
    }

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);

    // Clean up so a later `migrations::run` (this or another test) does not
    // reinstall this schema through the discovery loops. With the v042
    // ledger loop now running alongside the v041 compact-deadline loop, a
    // leaked probe schema multiplies the single-tx lock footprint of the
    // reinstall and can trip max_locks_per_transaction under concurrency.
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("compact-deadline probe schema should drop cleanly");
}

async fn rollups_failed_column_exists(pool: &PgPool, schema: &str) -> bool {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = 'queue_terminal_rollups'
              AND column_name = 'pruned_failed_count'
        )
        "#,
    )
    .bind(schema)
    .fetch_one(pool)
    .await
    .expect("pruned_failed_count column probe should succeed")
}

#[tokio::test]
async fn test_v032_backfills_queue_storage_pruned_failed_rollup_column() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let schema = "awa_queue_storage_v032_rollups";
    prepare_queue_storage_schema(&pool, schema).await;

    // Simulate a substrate prepared by a pre-v032 binary.
    sqlx::query(&format!(
        "ALTER TABLE {schema}.queue_terminal_rollups DROP COLUMN pruned_failed_count"
    ))
    .execute(&pool)
    .await
    .expect("pruned_failed_count test column should drop cleanly");

    // A look-alike schema without the claim function must be skipped by
    // the substrate discovery filter.
    sqlx::raw_sql(
        r#"
        DROP SCHEMA IF EXISTS awa_queue_storage_v032_partial CASCADE;
        CREATE SCHEMA awa_queue_storage_v032_partial;
        CREATE TABLE awa_queue_storage_v032_partial.queue_ring_state (
            singleton BOOLEAN PRIMARY KEY,
            slot_count INT NOT NULL
        );
        CREATE TABLE awa_queue_storage_v032_partial.queue_terminal_rollups (
            queue TEXT NOT NULL,
            priority SMALLINT NOT NULL,
            pruned_completed_count BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (queue, priority)
        );
        "#,
    )
    .execute(&pool)
    .await
    .expect("partial queue-storage probe schema should be creatable");

    sqlx::query("DELETE FROM awa.schema_version WHERE version >= 32")
        .execute(&pool)
        .await
        .expect("schema_version rewind should succeed");

    migrations::run(&pool)
        .await
        .expect("v032 should rerun cleanly");

    assert!(
        rollups_failed_column_exists(&pool, schema).await,
        "v032 must backfill pruned_failed_count for existing queue-storage schemas"
    );
    assert!(
        !rollups_failed_column_exists(&pool, "awa_queue_storage_v032_partial").await,
        "v032 must skip schemas that are not real queue-storage substrates"
    );

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);
}

async fn ring_state_cursor_columns_exist(pool: &PgPool, schema: &str) -> bool {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = 'queue_ring_state'
              AND column_name = 'current_slot'
        )
        "#,
    )
    .bind(schema)
    .fetch_one(pool)
    .await
    .expect("current_slot column probe should succeed")
}

/// #371 v042 STAGED UPGRADE: an upgrade from a pre-v042 substrate must
/// create + seed the append-only rotation ledgers from the legacy
/// `current_slot` / `generation` singleton cursor WITHOUT dropping those
/// columns (the migration is additive — compat columns are retained), start
/// the schema in `columns` authority, and route a post-upgrade cursor read
/// off the compat columns exactly. Also covers the dev-shape v042 where the
/// columns were previously dropped: v042 must re-ADD and re-seed them from
/// the ledger (the inverse seed).
#[tokio::test]
async fn test_v042_expand_only_restores_compat_columns_and_seeds_ledger() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    let schema = "awa_queue_storage_v042_ledger";
    // Drop any leftover from a prior interrupted run BEFORE migrating: the
    // v042 discovery loop reinstalls every existing queue-storage schema,
    // and reinstalling a stale copy here only wastes a large single-tx
    // lock footprint (and can trip max_locks_per_transaction).
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("leftover ledger probe schema should drop cleanly");

    migrations::run(&pool).await.unwrap();

    prepare_queue_storage_schema(&pool, schema).await;

    // Downgrade the prepared (post-v042) substrate back to a pre-v042 (0.6-
    // shaped) substrate: drop the ledgers, the delta table, the authority
    // control row, and the ring_cursor resolver, and restore the mutable
    // cursor columns on each singleton. Seed the queue cursor at a
    // non-genesis position (current_slot = 3, generation = 19) so the ledger
    // seed is observable.
    sqlx::raw_sql(&format!(
        r#"
        DROP TABLE {schema}.queue_ring_rotations;
        DROP TABLE {schema}.lease_ring_rotations;
        DROP TABLE {schema}.claim_ring_rotations;
        DROP TABLE {schema}.queue_terminal_rollup_deltas;
        DROP TABLE {schema}.ring_cursor_authority;
        DROP FUNCTION {schema}.ring_cursor(text);

        ALTER TABLE {schema}.queue_ring_state
            ALTER COLUMN current_slot SET NOT NULL,
            ALTER COLUMN generation   SET NOT NULL;
        ALTER TABLE {schema}.lease_ring_state
            ALTER COLUMN current_slot SET NOT NULL,
            ALTER COLUMN generation   SET NOT NULL;
        ALTER TABLE {schema}.claim_ring_state
            ALTER COLUMN current_slot SET NOT NULL,
            ALTER COLUMN generation   SET NOT NULL;

        -- Stable 0.6 schemas carry NOT NULL per-slot generations without a
        -- default. INSERT(slot) ... ON CONFLICT still validates the proposed
        -- row before conflict detection, so v042's v023 reapply must provide
        -- an explicit generation even when every slot already exists.
        ALTER TABLE {schema}.queue_ring_slots ALTER COLUMN generation DROP DEFAULT;
        ALTER TABLE {schema}.lease_ring_slots ALTER COLUMN generation DROP DEFAULT;
        ALTER TABLE {schema}.claim_ring_slots ALTER COLUMN generation DROP DEFAULT;

        UPDATE {schema}.queue_ring_state SET current_slot = 3, generation = 19 WHERE singleton;
        "#
    ))
    .execute(&pool)
    .await
    .expect("downgrade to pre-v042 substrate shape should succeed");

    assert!(
        ring_state_cursor_columns_exist(&pool, schema).await,
        "downgraded substrate should carry the legacy cursor columns"
    );

    // Rerun the migrations: v042's discovery loop reapplies the v023 install
    // helper against this schema, seeding the ledger from the columns and
    // installing the authority control (which defaults to 'columns' because
    // this is an upgrade, not a fresh install).
    sqlx::query("DELETE FROM awa.schema_version WHERE version >= 42")
        .execute(&pool)
        .await
        .expect("schema_version rewind should succeed");

    migrations::run(&pool)
        .await
        .expect("v042 should rerun cleanly");

    // The compat cursor columns are RETAINED (additive migration).
    assert!(
        ring_state_cursor_columns_exist(&pool, schema).await,
        "v042 must retain the compat current_slot/generation cursor columns"
    );

    // Authority defaults to 'columns' on upgrade.
    let authority: String = sqlx::query_scalar(&format!(
        "SELECT authority FROM {schema}.ring_cursor_authority WHERE singleton"
    ))
    .fetch_one(&pool)
    .await
    .expect("authority row should exist after upgrade");
    assert_eq!(
        authority, "columns",
        "an upgrade starts in compat authority"
    );

    // The queue ledger was seeded from the legacy cursor exactly: the
    // current row carries (generation = 19, slot = 3).
    let (ledger_slot, ledger_gen): (i32, i64) = sqlx::query_as(&format!(
        "SELECT slot, generation FROM {schema}.queue_ring_rotations \
         ORDER BY generation DESC LIMIT 1"
    ))
    .fetch_one(&pool)
    .await
    .expect("queue ledger cursor should be readable after upgrade");
    assert_eq!(
        (ledger_slot, ledger_gen),
        (3, 19),
        "v042 must seed the queue ledger from the legacy cursor"
    );

    // The authority resolver returns the compat columns (== ledger here).
    let (cursor_slot, cursor_gen): (i32, i64) = sqlx::query_as(&format!(
        "SELECT slot, generation FROM {schema}.ring_cursor('queue')"
    ))
    .fetch_one(&pool)
    .await
    .expect("ring_cursor('queue') should resolve after upgrade");
    assert_eq!(
        (cursor_slot, cursor_gen),
        (3, 19),
        "ring_cursor must resolve the compat cursor in columns authority"
    );

    // The delta landing table exists again and the schema reports ready.
    assert!(
        storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable after upgrade"),
        "upgraded substrate must be reported ready (ledgers + delta table present)"
    );

    // A claim after upgrade routes off the resolved cursor without error.
    let claimed: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {schema}.claim_ready_runtime(\
         'v042_upgrade_q'::text, 8::bigint, 0::double precision, 0::double precision)"
    ))
    .fetch_one(&pool)
    .await
    .expect("claim_ready_runtime must route off the resolved cursor after upgrade");
    assert_eq!(claimed, 0, "no ready work was seeded for the upgrade probe");

    // ---- Dev-shape v042 (columns previously DROPPED) -> inverse seed. ----
    // Simulate the unreleased shipped-v042 shape: drop the compat columns,
    // leaving only the ledger. Rerun v042; it must re-ADD the columns seeded
    // FROM the ledger max (3, 19) and keep authority 'columns'.
    sqlx::raw_sql(&format!(
        r#"
        DROP TRIGGER reject_compat_ring_cursor_update_after_flip ON {schema}.queue_ring_state;
        DROP TRIGGER reject_compat_ring_cursor_update_after_flip ON {schema}.lease_ring_state;
        DROP TRIGGER reject_compat_ring_cursor_update_after_flip ON {schema}.claim_ring_state;
        ALTER TABLE {schema}.queue_ring_state DROP COLUMN current_slot, DROP COLUMN generation;
        ALTER TABLE {schema}.lease_ring_state DROP COLUMN current_slot, DROP COLUMN generation;
        ALTER TABLE {schema}.claim_ring_state DROP COLUMN current_slot, DROP COLUMN generation;
        "#
    ))
    .execute(&pool)
    .await
    .expect("dev-shape column drop should succeed");
    sqlx::query("DELETE FROM awa.schema_version WHERE version >= 42")
        .execute(&pool)
        .await
        .expect("schema_version rewind should succeed");
    migrations::run(&pool)
        .await
        .expect("v042 should rerun cleanly against the dev shape");

    let (restored_slot, restored_gen): (i32, i64) = sqlx::query_as(&format!(
        "SELECT current_slot, generation FROM {schema}.queue_ring_state WHERE singleton"
    ))
    .fetch_one(&pool)
    .await
    .expect("columns should be restored from the ledger");
    assert_eq!(
        (restored_slot, restored_gen),
        (3, 19),
        "v042 must re-ADD the compat columns seeded from the ledger max (inverse seed)"
    );

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);

    // Clean up so a later `migrations::run` (this or another test) does not
    // reinstall this schema through the v042 discovery loop.
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("ledger probe schema should drop cleanly");
}

/// #371 v042 staged upgrade: a FRESH queue-storage install starts directly
/// in `ledger` authority (no old binary can exist to read the compat
/// columns), while the default `awa` schema installed by `awa migrate` also
/// resolves through the authority machinery.
#[tokio::test]
async fn test_v042_fresh_install_starts_in_ledger_authority() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    let schema = "awa_queue_storage_v042_fresh";
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("leftover fresh probe schema should drop cleanly");

    migrations::run(&pool).await.unwrap();

    // Fresh install of a custom schema (never existed before).
    prepare_queue_storage_schema(&pool, schema).await;

    let authority: String = sqlx::query_scalar(&format!(
        "SELECT authority FROM {schema}.ring_cursor_authority WHERE singleton"
    ))
    .fetch_one(&pool)
    .await
    .expect("authority row should exist on a fresh install");
    assert_eq!(
        authority, "ledger",
        "a fresh install starts directly in ledger authority"
    );

    // ring_cursor resolves the genesis ledger cursor (0, 0).
    let (slot, generation): (i32, i64) = sqlx::query_as(&format!(
        "SELECT slot, generation FROM {schema}.ring_cursor('queue')"
    ))
    .fetch_one(&pool)
    .await
    .expect("ring_cursor('queue') should resolve on a fresh install");
    assert_eq!((slot, generation), (0, 0), "fresh genesis cursor is (0, 0)");

    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("fresh probe schema should drop cleanly");
}

#[tokio::test]
async fn test_queue_storage_schema_ready_requires_sequence_and_claim_function() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let schema = "awa_queue_storage_ready_probe";
    prepare_queue_storage_schema(&pool, schema).await;

    assert!(
        storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable"),
        "freshly prepared queue-storage schema should be ready"
    );

    sqlx::query(&format!("DROP SEQUENCE {schema}.job_id_seq CASCADE"))
        .execute(&pool)
        .await
        .expect("test sequence drop should succeed");

    assert!(
        !storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable after sequence drop"),
        "schema without job_id_seq must not be reported as ready"
    );

    prepare_queue_storage_schema(&pool, schema).await;
    sqlx::query(&format!(
        "DROP SEQUENCE {schema}.lease_claim_receipt_id_seq CASCADE"
    ))
    .execute(&pool)
    .await
    .expect("test receipt sequence drop should succeed");

    assert!(
        !storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable after receipt sequence drop"),
        "schema without lease_claim_receipt_id_seq must not be reported as ready"
    );

    prepare_queue_storage_schema(&pool, schema).await;
    sqlx::query(&format!(
        "DROP SEQUENCE {schema}.lease_claim_batch_id_seq CASCADE"
    ))
    .execute(&pool)
    .await
    .expect("test compact claim batch sequence drop should succeed");

    assert!(
        !storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable after compact claim sequence drop"),
        "schema without lease_claim_batch_id_seq must not be reported as ready"
    );

    prepare_queue_storage_schema(&pool, schema).await;
    sqlx::query(&format!("DROP TABLE {schema}.ready_tombstones CASCADE"))
        .execute(&pool)
        .await
        .expect("test ready_tombstones drop should succeed");

    assert!(
        !storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable after tombstone table drop"),
        "schema without ready_tombstones must not be reported as ready"
    );

    prepare_queue_storage_schema(&pool, schema).await;
    sqlx::query(&format!(
        "DROP TABLE {schema}.receipt_completion_batches CASCADE"
    ))
    .execute(&pool)
    .await
    .expect("test receipt_completion_batches drop should succeed");

    assert!(
        !storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable after compact batch table drop"),
        "schema without receipt_completion_batches must not be reported as ready"
    );

    prepare_queue_storage_schema(&pool, schema).await;
    sqlx::query(&format!("DROP TABLE {schema}.lease_claim_batches CASCADE"))
        .execute(&pool)
        .await
        .expect("test lease_claim_batches drop should succeed");

    assert!(
        !storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable after compact claim batch table drop"),
        "schema without lease_claim_batches must not be reported as ready"
    );

    prepare_queue_storage_schema(&pool, schema).await;
    sqlx::query(&format!(
        "DROP TABLE {schema}.receipt_completion_tombstones CASCADE"
    ))
    .execute(&pool)
    .await
    .expect("test receipt_completion_tombstones drop should succeed");

    assert!(
        !storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable after compact tombstone table drop"),
        "schema without receipt_completion_tombstones must not be reported as ready"
    );

    prepare_queue_storage_schema(&pool, schema).await;
    sqlx::query(&format!(
        "DROP TABLE {schema}.queue_terminal_count_deltas CASCADE"
    ))
    .execute(&pool)
    .await
    .expect("test queue_terminal_count_deltas drop should succeed");

    assert!(
        !storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable after terminal delta table drop"),
        "schema without queue_terminal_count_deltas must not be reported as ready"
    );

    prepare_queue_storage_schema(&pool, schema).await;
    sqlx::query(&format!(
        "ALTER TABLE {schema}.lease_claim_closure_batches DROP COLUMN receipt_ranges"
    ))
    .execute(&pool)
    .await
    .expect("test receipt_ranges drop should succeed");

    assert!(
        !storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable after receipt_ranges drop"),
        "schema without receipt_ranges must not be reported as ready"
    );

    prepare_queue_storage_schema(&pool, schema).await;
    sqlx::query(&format!(
        "DROP FUNCTION {schema}.claim_ready_runtime(text, bigint, double precision, double precision)"
    ))
        .execute(&pool)
        .await
        .expect("test claim function drop should succeed");

    assert!(
        !storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable after function drop"),
        "schema without claim_ready_runtime must not be reported as ready"
    );

    prepare_queue_storage_schema(&pool, schema).await;
    sqlx::query(&format!(
        "DROP FUNCTION {schema}.claim_ready_runtime(text, bigint, double precision, double precision)"
    ))
        .execute(&pool)
        .await
        .expect("test claim function drop before stub should succeed");
    sqlx::query(&format!(
        r#"
        CREATE FUNCTION {schema}.claim_ready_runtime(
            p_queue TEXT,
            p_max_batch BIGINT,
            p_deadline_secs DOUBLE PRECISION,
            p_aging_secs DOUBLE PRECISION
        )
        RETURNS TABLE(receipt_id BIGINT)
        LANGUAGE sql
        AS $$
            SELECT NULL::bigint WHERE FALSE
        $$;
        "#
    ))
    .execute(&pool)
    .await
    .expect("test stale claim function create should succeed");

    assert!(
        !storage::queue_storage_schema_ready(&pool, schema)
            .await
            .expect("schema readiness should be queryable after stale function replacement"),
        "schema with old claim_ready_runtime return shape must not be reported as ready"
    );
}

#[tokio::test]
async fn test_prepare_schema_preserves_trusted_terminal_counter_marker_on_current_shape() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let schema = "awa_queue_storage_trusted_marker";
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("queue storage test schema should drop cleanly");
    let store =
        QueueStorage::from_existing_schema(schema).expect("queue storage schema should validate");
    store
        .prepare_schema(&pool)
        .await
        .expect("queue storage schema preparation should succeed");

    sqlx::raw_sql(&format!(
        r#"
        INSERT INTO {schema}.done_entries (
            ready_slot, ready_generation, job_id, kind, queue, state,
            priority, attempt, run_lease, lane_seq, enqueue_shard,
            attempted_at, finalized_at, payload
        )
        VALUES (
            0, 1, 9100001, 'migration_test_job', 'trusted_marker',
            'completed'::awa.job_state, 2::smallint, 1::smallint,
            1::bigint, 1::bigint, 0::smallint, now(), now(), '{{}}'::jsonb
        );

        INSERT INTO {schema}.queue_terminal_live_counts (
            ready_slot, queue, priority, enqueue_shard, counter_bucket, live_terminal_count
        )
        VALUES (0, 'trusted_marker', 2::smallint, 0::smallint, 1::smallint, 1);

        UPDATE {schema}.queue_ring_state
        SET terminal_counter_trusted_at = now()
        WHERE singleton = TRUE;
        "#
    ))
    .execute(&pool)
    .await
    .expect("seed current-shape terminal counters");

    store
        .prepare_schema(&pool)
        .await
        .expect("idempotent prepare_schema should succeed");

    let trusted: bool = sqlx::query_scalar(&format!(
        "SELECT terminal_counter_trusted_at IS NOT NULL \
         FROM {schema}.queue_ring_state WHERE singleton = TRUE"
    ))
    .fetch_one(&pool)
    .await
    .expect("trust marker query should succeed");

    assert!(
        trusted,
        "idempotent prepare_schema must not clear a trusted current-shape terminal counter"
    );
}

#[tokio::test]
async fn test_v030_preserves_untrusted_terminal_counter_marker_on_empty_schema() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let schema = "awa_queue_storage_untrusted_marker";
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("queue storage test schema should drop cleanly");
    let store =
        QueueStorage::from_existing_schema(schema).expect("queue storage schema should validate");
    store
        .prepare_schema(&pool)
        .await
        .expect("queue storage schema preparation should succeed");

    sqlx::query(&format!(
        "UPDATE {schema}.queue_ring_state \
         SET terminal_counter_trusted_at = NULL \
         WHERE singleton = TRUE"
    ))
    .execute(&pool)
    .await
    .expect("clear trust marker");

    for (_version, _desc, sql) in migrations::migration_sql_range(29, 30) {
        sqlx::raw_sql(&sql)
            .execute(&pool)
            .await
            .expect("v030 migration should rerun cleanly");
    }

    let trusted: bool = sqlx::query_scalar(&format!(
        "SELECT terminal_counter_trusted_at IS NOT NULL \
         FROM {schema}.queue_ring_state WHERE singleton = TRUE"
    ))
    .fetch_one(&pool)
    .await
    .expect("trust marker query should succeed");

    assert!(
        !trusted,
        "v030 must not re-trust a queue-storage schema whose marker was cleared before upgrade"
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
async fn test_active_queue_storage_schema_survives_missing_backend_marker() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    install_queue_storage_backend(&pool, "awa_queue_storage_missing_marker").await;
    sqlx::query("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(
        active_queue_storage_schema(&pool).await.as_deref(),
        Some("awa_queue_storage_missing_marker"),
        "active transition state should keep queue-storage routing authoritative"
    );

    sqlx::query_as::<_, awa::JobRow>(
        r#"
        SELECT *
        FROM awa.insert_job_compat(
            'missing_marker_routing_test',
            'missing_marker_queue',
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

    let hot_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs_hot WHERE queue = 'missing_marker_queue'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(hot_count, 0, "active queue storage should bypass jobs_hot");

    let queue_storage_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa_queue_storage_missing_marker.ready_entries WHERE queue = 'missing_marker_queue'",
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
async fn test_storage_abort_from_mixed_transition_rejects_compact_terminal_rows() {
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

    sqlx::query(
        r#"
        INSERT INTO awa_queue_storage.receipt_completion_batches (
            ready_slot,
            ready_generation,
            claim_slot,
            queue,
            priority,
            enqueue_shard,
            completed_count,
            job_ids,
            run_leases,
            lane_seqs,
            attempts,
            attempted_ats,
            finalized_at
        )
        VALUES (
            0,
            1,
            0,
            'abort_compact_terminal_queue',
            2,
            0,
            1,
            ARRAY[9000001]::bigint[],
            ARRAY[1]::bigint[],
            ARRAY[1]::bigint[],
            ARRAY[1]::smallint[],
            ARRAY[now()]::timestamptz[],
            now()
        )
        "#,
    )
    .execute(&pool)
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
        err.to_string().contains("queue_storage_target"),
        "got {err}"
    );

    insert_runtime_instance_with_role(&pool, "canonical", "auto").await;
    let err = storage::enter_mixed_transition(&pool).await.unwrap_err();
    assert_eq!(sqlstate_from_awa_error(&err).as_deref(), Some("55000"));
    assert!(
        err.to_string().contains("canonical-only runtime"),
        "got {err}"
    );

    // Auto-role 0.6 runtime that *currently* reports queue-storage capability.
    // This is the witness from `AwaStorageTransitionCurrentGate.cfg`: pre-flip
    // the auto runtime claims `queue_storage`, but its effective storage
    // resolved to canonical at startup, so it will downgrade to
    // canonical_drain_only after the routing flip and leave the cluster with
    // no queue-storage executor. The gate must reject this.
    sqlx::query("DELETE FROM awa.runtime_instances")
        .execute(&pool)
        .await
        .unwrap();
    insert_runtime_instance_with_role(&pool, "queue_storage", "auto").await;
    let err = storage::enter_mixed_transition(&pool).await.unwrap_err();
    assert_eq!(sqlstate_from_awa_error(&err).as_deref(), Some("55000"));
    assert!(
        err.to_string().contains("queue_storage_target"),
        "got {err}"
    );

    // Add an explicit queue_storage_target runtime alongside the auto one;
    // the gate now passes because there's a runtime that will keep executing
    // queue_storage work after the routing flip.
    insert_runtime_instance_with_role(&pool, "queue_storage", "queue_storage_target").await;

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
    assert!(
        row.id > 0,
        "insert_job_compat must return the queue-storage job id"
    );
    assert_eq!(row.kind, "compat_refusal_test");
    assert_eq!(row.queue, "compat_refusal_queue");
    assert_eq!(row.state, awa::JobState::Available);

    let lane_seq: i64 = sqlx::query_scalar(&format!(
        "SELECT lane_seq FROM {schema}.ready_entries WHERE job_id = $1"
    ))
    .bind(row.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        lane_seq, 1,
        "insert_job_compat must reserve queue-storage lanes through the sequence allocator"
    );

    let ready_segments: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {schema}.ready_segments WHERE queue = $1"
    ))
    .bind("compat_refusal_queue")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        ready_segments, 1,
        "insert_job_compat must append ready segment metadata with the ready row"
    );

    let ready_segments_partitioned: bool = sqlx::query_scalar(
        r#"
        SELECT c.relkind = 'p'
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relname = 'ready_segments'
        "#,
    )
    .bind(schema)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        ready_segments_partitioned,
        "ready_segments must be a partitioned queue-ring parent"
    );

    let ready_segment_child_exists: bool = sqlx::query_scalar(
        "SELECT to_regclass(format('%I.%I', $1, 'ready_segments_0')) IS NOT NULL",
    )
    .bind(schema)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        ready_segment_child_exists,
        "ready_segments_0 child must exist for queue-ring prune"
    );

    let claim_head_cache_columns: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)::bigint
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'queue_claim_heads'
          AND column_name = ANY($2)
        "#,
    )
    .bind(schema)
    .bind([
        "ready_segment_slot",
        "ready_segment_generation",
        "ready_segment_next_lane_seq",
    ])
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        claim_head_cache_columns, 3,
        "queue_claim_heads ready-segment columns are retained (additive-only migration policy) \
         even though claim_ready_runtime no longer reads or writes them; dropping them is \
         deferred to a major version"
    );
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

// ── Concurrency & cancellation safety (migration advisory lock) ───

/// Build a dedicated pool with a specific connection ceiling — the shared
/// `pool()` caps at 2, too few to genuinely race several runners at once.
async fn pool_with_max_connections(max: u32) -> PgPool {
    ensure_migration_database().await;
    PgPoolOptions::new()
        .max_connections(max)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&migration_database_url())
        .await
        .expect("Failed to connect to migration test database — is Postgres running?")
}

/// Race N concurrent `run()` calls against a fresh database and assert they
/// converge: every run succeeds, the schema reaches CURRENT_VERSION, and each
/// migration version is recorded exactly once. Without the migration advisory
/// lock the interleaved DDL would collide ("relation already exists", "tuple
/// concurrently updated"); the lock serializes the runners so all converge on
/// a single application.
#[tokio::test]
async fn test_concurrent_runs_apply_once_and_converge() {
    let _guard = acquire_migration_guard().await;

    const RACERS: usize = 8;
    let pool = pool_with_max_connections(RACERS as u32 + 2).await;
    reset_schema(&pool).await;

    // `migrations::run` is `!Send` (it holds a pooled connection / transaction
    // across awaits), so the racers run on a LocalSet rather than tokio::spawn.
    // Cooperative scheduling is enough to exercise lock contention: while one
    // run holds the lock and awaits Postgres, the others park inside
    // pg_advisory_xact_lock.
    let local = tokio::task::LocalSet::new();
    let results: Vec<Result<(), awa::AwaError>> = local
        .run_until(async {
            let mut handles = Vec::with_capacity(RACERS);
            for _ in 0..RACERS {
                let pool = pool.clone();
                handles.push(tokio::task::spawn_local(async move {
                    migrations::run(&pool).await
                }));
            }
            let mut out = Vec::with_capacity(RACERS);
            for handle in handles {
                out.push(handle.await.expect("run task should not panic"));
            }
            out
        })
        .await;

    for (index, result) in results.iter().enumerate() {
        if let Err(err) = result {
            panic!("concurrent run {index} failed: {err}");
        }
    }

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);

    // No version applied more than once — a torn/interleaved application would
    // leave duplicate rows (or would have errored above).
    let duplicate_versions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT version FROM awa.schema_version GROUP BY version HAVING count(*) > 1) AS d",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(duplicate_versions, 0, "schema_version has duplicate rows");

    // Exactly one row per known migration (v008 is a reserved gap, so this is
    // the migration count, not CURRENT_VERSION).
    let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        recorded,
        migrations::migration_sql().len() as i64,
        "every migration version should be recorded exactly once"
    );

    pool.close().await;
}

/// A `run()` cancelled while it holds the migration lock must not strand the
/// lock. Because the lock is transaction-scoped, dropping the future rolls the
/// transaction back and frees the lock, so a subsequent `run()` proceeds
/// promptly. With the old session-scoped lock the cancelled run left the lock
/// held on the pooled connection and every later migrate blocked until that
/// connection happened to close — this test would then hang on the final run.
#[tokio::test]
async fn test_cancelled_run_does_not_strand_migration_lock() {
    let _guard = acquire_migration_guard().await;

    let pool = pool_with_max_connections(4).await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.expect("initial migrate");

    // Hold ACCESS EXCLUSIVE on awa.schema_version so a racing run parks *while
    // holding the migration advisory lock*: run() acquires the xact lock, then
    // blocks on its early `SELECT MAX(version) FROM awa.schema_version`.
    let mut blocker = PgConnection::connect(&migration_database_url())
        .await
        .expect("blocker connection should open");
    let mut blocker_tx = blocker.begin().await.expect("blocker begin");
    sqlx::raw_sql("LOCK TABLE awa.schema_version IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker_tx)
        .await
        .expect("blocker should lock schema_version");

    // Start a run and cancel it: the timeout drops the future while it is
    // parked on the held table lock, holding the migration advisory lock.
    let cancelled = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        migrations::run(&pool),
    )
    .await;
    assert!(
        cancelled.is_err(),
        "run should still be blocked on the held table lock when the timeout fires"
    );

    // Release the table lock. The cancelled run's rollback must have already
    // freed the migration advisory lock.
    blocker_tx.rollback().await.expect("blocker rollback");
    drop(blocker);

    // A fresh run must acquire the migration lock and finish. If the cancelled
    // run had stranded a session-scoped lock, this would block until the
    // timeout and fail.
    tokio::time::timeout(std::time::Duration::from_secs(30), migrations::run(&pool))
        .await
        .expect("subsequent run must not block on a stranded migration lock")
        .expect("subsequent migrate should succeed");

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);

    pool.close().await;
}

// ── #392: fail safe on a schema newer than the binary ────────────

/// A schema recorded *newer* than this binary must be refused loudly and left
/// untouched. Before this guard, `current_version`'s legacy heuristic
/// misclassified a future version (e.g. v42 → 4) and destructively rewrote
/// `awa.schema_version` (DELETE version >= 3, re-seed), then re-applied
/// migrations onto the newer physical layout and crashed mid-way, leaving
/// split-brain metadata (#392). Assert: `SchemaNewerThanBinary`, and zero
/// mutation of `schema_version` (count + max unchanged).
#[tokio::test]
async fn test_migrate_refuses_schema_newer_than_binary() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    // Bring the schema to CURRENT_VERSION the normal way.
    migrations::run(&pool).await.expect("initial migrate");

    // Record a version one past what this binary knows — the supported
    // rolling-deploy skew (older binary, newer schema).
    let future_version = migrations::CURRENT_VERSION + 1;
    sqlx::query("INSERT INTO awa.schema_version (version, description) VALUES ($1, $2)")
        .bind(future_version)
        .bind("future migration from a newer binary")
        .execute(&pool)
        .await
        .expect("seed future schema_version row");

    let (count_before, max_before): (i64, i32) =
        sqlx::query_as("SELECT count(*), MAX(version) FROM awa.schema_version")
            .fetch_one(&pool)
            .await
            .unwrap();

    let err = migrations::run(&pool).await.unwrap_err();
    assert!(
        matches!(
            &err,
            awa::AwaError::SchemaNewerThanBinary { found, supported }
                if *found == future_version && *supported == migrations::CURRENT_VERSION
        ),
        "expected SchemaNewerThanBinary, got: {err}"
    );

    // No writes on the refusal path: history intact, still points at the
    // future version.
    let (count_after, max_after): (i64, i32) =
        sqlx::query_as("SELECT count(*), MAX(version) FROM awa.schema_version")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count_before, count_after,
        "refusal must not add/remove schema_version rows"
    );
    assert_eq!(
        max_before, max_after,
        "refusal must not rewrite the max schema version"
    );
    assert_eq!(max_after, future_version);

    // The read-only probe agrees and also stays non-mutating.
    let probed = migrations::current_version_readonly(&pool).await.unwrap();
    assert_eq!(probed, future_version);

    pool.close().await;
}

/// The newer-schema guard must not disturb ordinary paths: a fresh migrate
/// reaches CURRENT_VERSION, a re-run is a clean no-op, and a schema one version
/// *behind* still upgrades.
#[tokio::test]
async fn test_newer_schema_guard_leaves_normal_paths_untouched() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    // Fresh install → CURRENT_VERSION.
    migrations::run(&pool).await.expect("fresh migrate");
    assert_eq!(
        migrations::current_version(&pool).await.unwrap(),
        migrations::CURRENT_VERSION
    );

    // Re-run at CURRENT_VERSION is a no-op success.
    migrations::run(&pool).await.expect("no-op re-run");
    assert_eq!(
        migrations::current_version(&pool).await.unwrap(),
        migrations::CURRENT_VERSION
    );

    // A schema exactly one behind still upgrades cleanly (the guard only
    // triggers strictly above CURRENT_VERSION).
    sqlx::query("DELETE FROM awa.schema_version WHERE version = $1")
        .bind(migrations::CURRENT_VERSION)
        .execute(&pool)
        .await
        .expect("drop the newest version row to simulate a one-behind schema");
    assert_eq!(
        migrations::current_version(&pool).await.unwrap(),
        migrations::CURRENT_VERSION - 1
    );
    migrations::run(&pool)
        .await
        .expect("upgrade one-behind schema");
    assert_eq!(
        migrations::current_version(&pool).await.unwrap(),
        migrations::CURRENT_VERSION
    );

    pool.close().await;
}

// ── Exclusive-window pre-flight live-runtime check ───────────────
//
// The strict mechanism remains available for a future migration with no
// rolling-compatible shape. No current migration is flagged; these tests pin
// its message and heartbeat-staleness behavior directly.

/// The pre-flight refusal names the migration, lists the live runtimes, and
/// points at the override. The gate keys on `last_seen_at` staleness, not
/// `healthy` — a hard-killed (`kill -9`) worker leaves `healthy = true` forever.
#[tokio::test]
async fn test_exclusive_window_preflight_refuses_with_live_runtime() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.expect("migrate to head");

    // A heartbeat inside the window — healthy is TRUE, matching a still-running
    // worker (also what a kill -9 leaves behind).
    insert_runtime_instance_with_role(&pool, "queue_storage", "queue_storage_target").await;

    let err = migrations::check_exclusive_window_preflight(&pool, migrations::CURRENT_VERSION)
        .await
        .expect_err("a live runtime must trip the exclusive-window pre-flight");
    assert!(
        matches!(
            &err,
            awa::AwaError::LiveRuntimesRequireExclusiveWindow { migration_version, count, .. }
                if *migration_version == migrations::CURRENT_VERSION && *count >= 1
        ),
        "expected LiveRuntimesRequireExclusiveWindow naming v{}, got: {err}",
        migrations::CURRENT_VERSION
    );
    // Migration-agnostic message that names the version and the escape hatch.
    let msg = err.to_string();
    assert!(
        msg.contains(&format!("migration v{}", migrations::CURRENT_VERSION))
            && msg.contains("--allow-live-runtimes"),
        "message should name the migration and the override: {msg}"
    );

    pool.close().await;
}

/// A stale heartbeat (older than the window) is not "live": the pre-flight passes.
#[tokio::test]
async fn test_exclusive_window_preflight_passes_with_stale_runtime() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.expect("migrate to head");

    insert_runtime_instance_with_role(&pool, "queue_storage", "queue_storage_target").await;
    sqlx::raw_sql("UPDATE awa.runtime_instances SET last_seen_at = now() - interval '10 minutes'")
        .execute(&pool)
        .await
        .expect("age the heartbeat");

    migrations::check_exclusive_window_preflight(&pool, migrations::CURRENT_VERSION)
        .await
        .expect("stale runtimes must not trip the pre-flight");

    pool.close().await;
}

/// `--allow-live-runtimes` skips the version-floor pre-flight even when a live
/// runtime reports a version below the v042 compatibility floor.
#[tokio::test]
async fn test_runtime_version_floor_override_allows_old_live_runtime() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.expect("migrate to head");
    // Finalize (so the ADR-037 gate passes), add a live worker, then rewind so
    // a real pending range crosses v042 with the worker heartbeating.
    simulate_non_canonical_compat_routing(&pool).await;
    insert_runtime_instance_with_role(&pool, "queue_storage", "queue_storage_target").await;
    sqlx::query("UPDATE awa.runtime_instances SET version = '0.6.1'")
        .execute(&pool)
        .await
        .expect("set runtime below v042 floor");
    rewind_schema_version(&pool, migrations::CURRENT_VERSION).await;

    let options = migrations::MigrateOptions {
        allow_live_runtimes: true,
    };
    migrations::run_with_options(&pool, options)
        .await
        .expect("override must skip the live-runtime pre-flight");
    assert_eq!(
        migrations::current_version(&pool).await.unwrap(),
        migrations::CURRENT_VERSION
    );

    pool.close().await;
}

#[tokio::test]
async fn test_runtime_version_floor_refuses_old_or_unparseable_live_runtime() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.expect("migrate to head");
    simulate_non_canonical_compat_routing(&pool).await;
    insert_runtime_instance_with_role(&pool, "queue_storage", "queue_storage_target").await;

    for reported in ["0.6.1", "unknown"] {
        sqlx::query("UPDATE awa.runtime_instances SET version = $1, last_seen_at = now()")
            .bind(reported)
            .execute(&pool)
            .await
            .expect("set incompatible runtime version");
        rewind_schema_version(&pool, migrations::CURRENT_VERSION).await;

        let err = migrations::run(&pool)
            .await
            .expect_err("incompatible live runtime must block v042");
        assert!(
            matches!(
                &err,
                awa::AwaError::RuntimeVersionFloorNotMet {
                    migration_version: 42,
                    minimum_version: "0.6.2",
                    count: 1,
                    ..
                }
            ),
            "expected v042 runtime-version-floor refusal for {reported:?}, got: {err}"
        );
        assert!(
            err.to_string().contains(reported),
            "refusal should identify the reported version: {err}"
        );
    }

    pool.close().await;
}

#[tokio::test]
async fn test_runtime_version_floor_ignores_stale_old_runtime() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.expect("migrate to head");
    simulate_non_canonical_compat_routing(&pool).await;
    insert_runtime_instance_with_role(&pool, "queue_storage", "queue_storage_target").await;
    sqlx::raw_sql(
        "UPDATE awa.runtime_instances \
         SET version = '0.6.1', last_seen_at = now() - interval '10 minutes'",
    )
    .execute(&pool)
    .await
    .expect("age runtime below v042 floor");
    rewind_schema_version(&pool, migrations::CURRENT_VERSION).await;

    migrations::run(&pool)
        .await
        .expect("stale runtimes do not block the migration floor");

    pool.close().await;
}

/// A range that crosses no flagged migration (already at head) must not consult
/// the pre-flight — a live worker is fine for a no-op run.
#[tokio::test]
async fn test_exclusive_window_skipped_for_noncrossing_range() {
    let _guard = acquire_migration_guard().await;
    let pool = pool().await;
    reset_schema(&pool).await;
    migrations::run(&pool).await.expect("migrate to head");
    assert_eq!(
        migrations::current_version(&pool).await.unwrap(),
        migrations::CURRENT_VERSION
    );

    // A live worker present, but there is nothing pending to cross — the gate
    // must not fire on an up-to-date schema.
    insert_runtime_instance_with_role(&pool, "queue_storage", "queue_storage_target").await;
    migrations::run(&pool)
        .await
        .expect("no-op run must not consult the exclusive-window pre-flight");
    assert_eq!(
        migrations::current_version(&pool).await.unwrap(),
        migrations::CURRENT_VERSION
    );

    pool.close().await;
}
