//! Proves an external operator can drive a canonical → queue-storage
//! transition using only SQL function calls — no Rust code path needs
//! to run any DDL, and no Rust wrapper needs to be involved beyond
//! `awa migrate` (which is itself just a SQL applier).
//!
//! This is the "external migration tooling" contract that
//! `docs/queue-storage-substrate.md` describes. The substrate DDL
//! ships in the migration set (v023 calls
//! `awa.install_queue_storage_substrate('awa')`), and the staged
//! transition is driven by the SQL functions defined in v010/v013/v014.
//! Tests below call those functions directly via raw SQL.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::LazyLock;
use tokio::sync::Mutex;
use uuid::Uuid;

// Storage-transition state is a singleton — serialise tests that
// mutate it so they don't stomp each other when run with `--test-threads`.
static TRANSITION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn migrated_pool() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url())
        .await
        .expect("connect");
    awa_model::migrations::run(&pool)
        .await
        .expect("run migrations");
    pool
}

async fn reset_transition_state(pool: &PgPool) {
    let mut tx = pool.begin().await.expect("begin reset tx");
    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET current_engine = 'canonical',
            prepared_engine = NULL,
            state = 'canonical',
            transition_epoch = transition_epoch + 1,
            details = '{}'::jsonb,
            updated_at = now(),
            finalized_at = NULL
        WHERE singleton
        "#,
    )
    .execute(&mut *tx)
    .await
    .expect("reset transition state");
    sqlx::query("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        .execute(&mut *tx)
        .await
        .expect("clear runtime_storage_backends");
    sqlx::query("DELETE FROM awa.runtime_instances")
        .execute(&mut *tx)
        .await
        .expect("clear runtime_instances");
    tx.commit().await.expect("commit reset tx");
}

async fn read_status(pool: &PgPool) -> (String, String, Option<String>) {
    let row: (String, String, Option<String>) =
        sqlx::query_as("SELECT state, current_engine, prepared_engine FROM awa.storage_status()")
            .fetch_one(pool)
            .await
            .expect("read storage_status");
    row
}

/// Stamp a synthetic `queue_storage_target` runtime row so the
/// mixed-transition gate (`storage_enter_mixed_transition`) sees an
/// executor ready to take queue-storage work after the routing flip.
/// In a real upgrade the operator brings up a worker with
/// `transition_role=queue_storage_target`; for an SQL-only test we
/// synthesise the row directly.
async fn stamp_queue_storage_target_runtime(pool: &PgPool) -> Uuid {
    let instance_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_instances (
            instance_id,
            hostname,
            pid,
            version,
            storage_capability,
            transition_role,
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
            queue_descriptor_hashes,
            job_kind_descriptor_hashes
        )
        VALUES (
            $1, 'sql-only-upgrade-test', 0, '0.0.0-test',
            'queue_storage', 'queue_storage_target',
            now(), now(), 5000, TRUE, TRUE, TRUE, TRUE, TRUE,
            FALSE, FALSE, 1,
            '[]'::jsonb, '[]'::jsonb, '[]'::jsonb
        )
        "#,
    )
    .bind(instance_id)
    .execute(pool)
    .await
    .expect("stamp queue_storage_target runtime");
    instance_id
}

/// Upgrade-from-canonical path: existing deployment has canonical jobs,
/// so `storage_auto_finalize_if_fresh` returns FALSE and the operator
/// must drive the staged transition. This test calls the SQL functions
/// directly — no Rust wrappers — to prove external migration tooling can
/// orchestrate the upgrade without any worker DDL.
#[tokio::test]
async fn external_tooling_can_upgrade_canonical_to_queue_storage_via_sql() {
    let _guard = TRANSITION_LOCK.lock().await;
    let pool = migrated_pool().await;
    reset_transition_state(&pool).await;

    // Defeat auto-finalize: ensure there's at least one canonical row
    // so `storage_auto_finalize_if_fresh` would refuse to short-circuit.
    sqlx::query(
        "INSERT INTO awa.jobs_hot (kind, queue, args) \
         VALUES ('sql_only_upgrade_marker', 'sql_only_upgrade', '{}'::jsonb)",
    )
    .execute(&pool)
    .await
    .expect("insert canonical marker");

    let target_instance = stamp_queue_storage_target_runtime(&pool).await;

    // Drive the staged transition via raw SQL — no awa_model::storage
    // wrappers, no Rust DDL. This mirrors what an external migration
    // tool would do as a post-DDL hook.
    sqlx::query("SELECT awa.storage_prepare($1, $2)")
        .bind("queue_storage")
        .bind(serde_json::json!({"schema": "awa"}))
        .execute(&pool)
        .await
        .expect("storage_prepare via raw SQL");

    let (state_after_prepare, _, prepared_after_prepare) = read_status(&pool).await;
    assert_eq!(state_after_prepare, "prepared");
    assert_eq!(prepared_after_prepare.as_deref(), Some("queue_storage"));

    sqlx::query("SELECT awa.storage_enter_mixed_transition()")
        .execute(&pool)
        .await
        .expect("storage_enter_mixed_transition via raw SQL");

    let (state_after_enter, _, _) = read_status(&pool).await;
    assert_eq!(state_after_enter, "mixed_transition");

    // `storage_finalize` refuses to advance while canonical live work
    // remains. In a real upgrade the operator waits in mixed_transition
    // for workers to drain; for the test we delete the marker to
    // simulate that drain.
    sqlx::query("DELETE FROM awa.jobs_hot WHERE queue = 'sql_only_upgrade'")
        .execute(&pool)
        .await
        .expect("simulate canonical drain");

    sqlx::query("SELECT awa.storage_finalize()")
        .execute(&pool)
        .await
        .expect("storage_finalize via raw SQL");

    let (final_state, final_engine, _) = read_status(&pool).await;
    assert_eq!(final_state, "active");
    assert_eq!(final_engine, "queue_storage");

    // Cleanup so other tests aren't poisoned.
    sqlx::query("DELETE FROM awa.runtime_instances WHERE instance_id = $1")
        .bind(target_instance)
        .execute(&pool)
        .await
        .expect("cleanup runtime row");
    reset_transition_state(&pool).await;
}

/// Fresh-install path: empty `awa.jobs` and no live runtimes means
/// `storage_auto_finalize_if_fresh` is allowed to jump straight from
/// canonical to active. The function is `GRANT EXECUTE ... TO PUBLIC`
/// (v013) so any caller — including a migration tool running as a
/// non-owner role — can invoke it. This test calls it via raw SQL with
/// no Rust runtime involvement.
#[tokio::test]
async fn external_tooling_can_finalize_fresh_install_via_sql() {
    let _guard = TRANSITION_LOCK.lock().await;
    let pool = migrated_pool().await;
    reset_transition_state(&pool).await;
    sqlx::query("DELETE FROM awa.jobs_hot")
        .execute(&pool)
        .await
        .expect("clear canonical jobs for fresh-install scenario");

    let promoted: bool = sqlx::query_scalar("SELECT awa.storage_auto_finalize_if_fresh($1)")
        .bind("awa")
        .fetch_one(&pool)
        .await
        .expect("auto-finalize via raw SQL");
    assert!(promoted, "fresh install should auto-finalize to active");

    let (state, engine, _) = read_status(&pool).await;
    assert_eq!(state, "active");
    assert_eq!(engine, "queue_storage");

    reset_transition_state(&pool).await;
}
