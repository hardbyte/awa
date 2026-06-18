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
