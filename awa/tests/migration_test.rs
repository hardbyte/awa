//! Migration tests: step-through upgrade, data survival, idempotency,
//! and migration_sql() consistency.
//!
//! **Must run with `--test-threads=1`** — these tests drop and recreate
//! the `awa` schema, which would break concurrent tests.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{insert_many, insert_many_copy_from_pool, migrations, storage};
use awa::{InsertOpts, InsertParams, UniqueOpts};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::OnceLock;
use tokio::sync::Mutex;

/// Serialize all migration tests to prevent parallel schema drops.
static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

fn test_mutex() -> &'static Mutex<()> {
    TEST_MUTEX.get_or_init(|| Mutex::new(()))
}

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

fn sqlstate_from_awa_error(err: &awa::AwaError) -> Option<String> {
    match err {
        awa::AwaError::Database(sqlx::Error::Database(db_err)) => {
            db_err.code().map(|code| code.to_string())
        }
        _ => None,
    }
}

fn sqlstate_from_sqlx_error(err: &sqlx::Error) -> Option<String> {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().map(|code| code.to_string()),
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

// ── Fresh install ────────────────────────────────────────────────

#[tokio::test]
async fn test_fresh_install_reaches_current_version() {
    let _guard = test_mutex().lock().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();
    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);
}

// ── Idempotency ──────────────────────────────────────────────────

#[tokio::test]
async fn test_migrations_are_idempotent() {
    let _guard = test_mutex().lock().await;
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
    let _guard = test_mutex().lock().await;
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
    let _guard = test_mutex().lock().await;
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
    let _guard = test_mutex().lock().await;
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
    let _guard = test_mutex().lock().await;
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
    let _guard = test_mutex().lock().await;
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
async fn test_storage_prepare_rejects_current_engine() {
    let _guard = test_mutex().lock().await;
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
    let _guard = test_mutex().lock().await;
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
    let _guard = test_mutex().lock().await;
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
async fn test_storage_abort_from_mixed_transition_returns_to_canonical() {
    let _guard = test_mutex().lock().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

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

    let before = storage::status(&pool).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let after = storage::abort(&pool).await.unwrap();

    assert_eq!(after.state, "canonical");
    assert_eq!(after.active_engine, "canonical");
    assert_eq!(after.prepared_engine, None);
    assert_eq!(after.transition_epoch, before.transition_epoch + 1);
    assert!(after.entered_at > before.entered_at);
}

#[tokio::test]
async fn test_storage_enter_mixed_transition_stub_requires_0_6() {
    let _guard = test_mutex().lock().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let err = sqlx::query("SELECT * FROM awa.storage_enter_mixed_transition()")
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(sqlstate_from_sqlx_error(&err).as_deref(), Some("55000"));
    assert!(
        err.to_string().contains("requires 0.6"),
        "expected 0.6 guard error, got {err}"
    );
}

#[tokio::test]
async fn test_storage_finalize_stub_requires_0_6() {
    let _guard = test_mutex().lock().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    let err = sqlx::query("SELECT * FROM awa.storage_finalize()")
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(sqlstate_from_sqlx_error(&err).as_deref(), Some("55000"));
    assert!(
        err.to_string().contains("requires 0.6"),
        "expected 0.6 guard error, got {err}"
    );
}

#[tokio::test]
async fn test_mixed_transition_fails_safe_for_0_5_producers() {
    let _guard = test_mutex().lock().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET prepared_engine = 'queue_storage',
            state = 'mixed_transition',
            transition_epoch = transition_epoch + 1,
            details = '{"schema":"awa_queue_storage"}'::jsonb,
            updated_at = now(),
            finalized_at = NULL
        WHERE singleton
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let compat_err = sqlx::query_as::<_, awa::JobRow>(
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
    .unwrap_err();
    assert_eq!(
        sqlstate_from_sqlx_error(&compat_err).as_deref(),
        Some("55000")
    );

    let batch_err = insert_many(
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
    .unwrap_err();
    assert_eq!(
        sqlstate_from_awa_error(&batch_err).as_deref(),
        Some("55000")
    );

    let copy_err = insert_many_copy_from_pool(
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
    .unwrap_err();
    assert_eq!(sqlstate_from_awa_error(&copy_err).as_deref(), Some("55000"));
}

#[tokio::test]
async fn test_insert_job_compat_refuses_non_canonical_active_engine() {
    let _guard = test_mutex().lock().await;
    let pool = pool().await;
    reset_schema(&pool).await;

    migrations::run(&pool).await.unwrap();

    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET prepared_engine = 'queue_storage',
            state = 'active',
            transition_epoch = transition_epoch + 1,
            details = '{"schema":"awa_queue_storage"}'::jsonb,
            updated_at = now(),
            finalized_at = now()
        WHERE singleton
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let err = sqlx::query_as::<_, awa::JobRow>(
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
    .unwrap_err();

    match err {
        sqlx::Error::Database(db_err) => {
            assert_eq!(db_err.code().as_deref(), Some("55000"));
            assert!(
                db_err.message().contains("not writable"),
                "expected non-writable error, got {}",
                db_err.message()
            );
        }
        other => panic!("expected sqlx database error, got {other:?}"),
    }
}

#[tokio::test]
async fn test_insert_many_uses_insert_job_compat_after_non_canonical_activation() {
    let _guard = test_mutex().lock().await;
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
    let _guard = test_mutex().lock().await;
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
    let _guard = test_mutex().lock().await;
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

// ── Legacy V3-only upgrade (0.3.0 exact, no V4/V5) ──────────────

#[tokio::test]
async fn test_legacy_v3_only_upgrade() {
    let _guard = test_mutex().lock().await;
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
