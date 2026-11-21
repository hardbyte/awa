//! Migration v062 (#847): the queue-ring generation floor.

use awa_model::{audited_sql, migrations, QueueStorage, QueueStorageConfig};
use sqlx::PgPool;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const CUSTOM: &str = "floor_custom";
const COMPAT_FUNCTIONS: [&str; 2] = ["insert_job_compat", "insert_jobs_compat_batch"];

async fn compat_function(pool: &PgPool, name: &str) -> Result<(i64, String), sqlx::Error> {
    sqlx::query_as(
        "SELECT p.oid::bigint, pg_get_functiondef(p.oid)
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'awa' AND p.proname = $1",
    )
    .bind(name)
    .fetch_one(pool)
    .await
}

async fn compat_functions(pool: &PgPool) -> Result<Vec<(i64, String)>, sqlx::Error> {
    let mut functions = Vec::new();
    for name in COMPAT_FUNCTIONS {
        functions.push(compat_function(pool, name).await?);
    }
    Ok(functions)
}

async fn apply(pool: &PgPool, from: i32, to: i32) -> TestResult {
    for (_, _, sql) in migrations::migration_sql_range(from, to) {
        sqlx::raw_sql(audited_sql(sql)).execute(pool).await?;
    }
    Ok(())
}

/// The floor's last value, or `None` before its first raise.
async fn floor(pool: &PgPool, schema: &str) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar(audited_sql(format!(
        "SELECT pg_sequence_last_value('{schema}.queue_ring_generation_floor'::regclass)"
    )))
    .fetch_one(pool)
    .await
}

/// Removes the floor from `schema`, as before v062.
async fn drop_floor(pool: &PgPool, schema: &str) -> TestResult {
    sqlx::raw_sql(audited_sql(format!(
        "DROP FUNCTION {schema}.enqueue_ring_cursor();
         DROP SEQUENCE {schema}.queue_ring_generation_floor;"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// Moves the queue ring of an empty schema to `generation` by appending
/// rotation-ledger rows.
async fn rotate_to(pool: &PgPool, schema: &str, generation: i64, slots: i64) -> TestResult {
    sqlx::query(audited_sql(format!(
        "INSERT INTO {schema}.queue_ring_rotations (generation, slot)
         SELECT g, g % $2 FROM generate_series(
             (SELECT max(generation) + 1 FROM {schema}.queue_ring_rotations), $1
         ) AS g"
    )))
    .bind(generation)
    .bind(slots)
    .execute(pool)
    .await?;
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn v062_installs_the_floor_and_rewrites_the_compat_producers(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    let store = QueueStorage::new(QueueStorageConfig {
        schema: CUSTOM.into(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })?;
    store.install(&pool).await?;
    for schema in ["awa", CUSTOM] {
        assert_eq!(floor(&pool, schema).await?, Some(0), "{schema}: seeded");
    }

    let installed = compat_functions(&pool).await?;
    for (_, definition) in &installed {
        assert!(definition.contains("enqueue_ring_cursor()"));
    }

    // Back to v061: the released compat producers, and no floor.
    apply(&pool, 17, 18).await?;
    apply(&pool, 56, 57).await?;
    for schema in ["awa", CUSTOM] {
        drop_floor(&pool, schema).await?;
    }
    let released = compat_functions(&pool).await?;
    for (_, definition) in &released {
        assert!(!definition.contains("enqueue_ring_cursor()"));
    }

    // A binary from this release on a schema before v062 reads the cursor as
    // before, through the compat producers and its own enqueue path alike.
    rotate_to(&pool, CUSTOM, 6, 4).await?;
    let fallback_store = QueueStorage::new(QueueStorageConfig {
        schema: CUSTOM.into(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })?;
    fallback_store
        .enqueue_batch(&pool, "fallback", 2, 1)
        .await?;
    let written: (i32, i64) = sqlx::query_as(audited_sql(format!(
        "SELECT ready_slot, ready_generation FROM {CUSTOM}.ready_entries WHERE queue = 'fallback'"
    )))
    .fetch_one(&pool)
    .await?;
    assert_eq!(written, (2, 6));

    apply(&pool, 61, 62).await?;
    let migrated = compat_functions(&pool).await?;
    for ((released_oid, _), (migrated_oid, migrated_definition)) in released.iter().zip(&migrated) {
        assert_eq!(released_oid, migrated_oid, "replaced in place");
        assert!(migrated_definition.contains("enqueue_ring_cursor()"));
    }
    // A fresh install runs v018 and v057 and then v062, so the bodies match.
    assert_eq!(
        migrated.iter().map(|(_, d)| d).collect::<Vec<_>>(),
        installed.iter().map(|(_, d)| d).collect::<Vec<_>>(),
        "v062 rewrites the released producers to the fresh-install bodies"
    );
    assert_eq!(floor(&pool, "awa").await?, Some(0));
    assert_eq!(
        floor(&pool, CUSTOM).await?,
        Some(6),
        "seeded from the cursor"
    );

    for schema in ["awa", CUSTOM] {
        let (definer, owned_by_ring_owner, public_execute, sequence_owned): (
            bool,
            bool,
            bool,
            bool,
        ) = sqlx::query_as(audited_sql(format!(
            "SELECT p.prosecdef,
                        p.proowner = state.relowner,
                        has_function_privilege('public', p.oid, 'EXECUTE'),
                        seq.relowner = state.relowner
                 FROM pg_proc AS p,
                      pg_class AS state,
                      pg_class AS seq
                 WHERE p.oid = '{schema}.enqueue_ring_cursor()'::regprocedure
                   AND state.oid = '{schema}.queue_ring_state'::regclass
                   AND seq.oid = '{schema}.queue_ring_generation_floor'::regclass"
        )))
        .fetch_one(&pool)
        .await?;
        assert!(definer, "{schema}: SECURITY DEFINER");
        assert!(owned_by_ring_owner, "{schema}: owned by the ring's owner");
        assert!(public_execute, "{schema}: PUBLIC may execute it");
        assert!(sequence_owned, "{schema}: floor owned by the ring's owner");
    }

    apply(&pool, 61, 62).await?;
    assert_eq!(
        compat_functions(&pool).await?,
        migrated,
        "replaying v062 changes nothing"
    );
    assert_eq!(floor(&pool, CUSTOM).await?, Some(6));

    // A snapshot that still sees generation 6 gets the floor's generation and
    // its slot, and cannot lower the floor.
    let cursor_sql = format!("SELECT slot, generation FROM {CUSTOM}.enqueue_ring_cursor()");
    let mut stale = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *stale)
        .await?;
    sqlx::query("SELECT 1").execute(&mut *stale).await?;
    rotate_to(&pool, CUSTOM, 9, 4).await?;
    let fresh: (i32, i64) = sqlx::query_as(audited_sql(cursor_sql.clone()))
        .fetch_one(&pool)
        .await?;
    assert_eq!(fresh, (1, 9));
    let seen: i64 = sqlx::query_scalar(audited_sql(format!(
        "SELECT generation FROM {CUSTOM}.ring_cursor('queue')"
    )))
    .fetch_one(&mut *stale)
    .await?;
    assert_eq!(seen, 6, "the snapshot predates the rotations");
    let stale_cursor: (i32, i64) = sqlx::query_as(audited_sql(cursor_sql))
        .fetch_one(&mut *stale)
        .await?;
    stale.rollback().await?;
    assert_eq!(stale_cursor, (1, 9));
    assert_eq!(floor(&pool, CUSTOM).await?, Some(9));
    Ok(())
}

/// A runtime role granted what it needed at v061, with no default privileges,
/// keeps enqueueing after v062 and raises the floor through
/// `enqueue_ring_cursor()`, without any privilege on the floor itself.
#[sqlx::test(migrations = false)]
async fn v062_needs_no_new_runtime_grants(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    drop_floor(&pool, "awa").await?;

    let runtime = format!("floor_runtime_{}", uuid::Uuid::new_v4().simple());
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    sqlx::raw_sql(audited_sql(format!(
        "CREATE ROLE {runtime} LOGIN PASSWORD 'floor_test';
         GRANT CONNECT ON DATABASE \"{database}\" TO {runtime};
         GRANT USAGE ON SCHEMA awa TO {runtime};
         GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA awa TO {runtime};
         GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA awa TO {runtime};
         GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa TO {runtime};"
    )))
    .execute(&pool)
    .await?;
    let result = async {
        apply(&pool, 61, 62).await?;
        rotate_to(&pool, "awa", 3, 16).await?;

        let floor_privilege: bool = sqlx::query_scalar(
            "SELECT has_sequence_privilege($1, 'awa.queue_ring_generation_floor', 'SELECT')
                 OR has_sequence_privilege($1, 'awa.queue_ring_generation_floor', 'UPDATE')",
        )
        .bind(&runtime)
        .fetch_one(&pool)
        .await?;
        assert!(!floor_privilege, "the runtime has no grant on the floor");

        let runtime_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                pool.connect_options()
                    .as_ref()
                    .clone()
                    .username(&runtime)
                    .password("floor_test"),
            )
            .await?;
        let store = QueueStorage::new(QueueStorageConfig::default())?;
        store
            .enqueue_batch(&runtime_pool, "runtime_queue", 2, 1)
            .await?;
        runtime_pool.close().await;

        let written: (i32, i64) = sqlx::query_as(
            "SELECT ready_slot, ready_generation FROM awa.ready_entries
             WHERE queue = 'runtime_queue'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(written, (3, 3));
        assert_eq!(floor(&pool, "awa").await?, Some(3), "the runtime raised it");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    sqlx::raw_sql(audited_sql(format!(
        "DROP OWNED BY {runtime}; DROP ROLE {runtime};"
    )))
    .execute(&pool)
    .await?;
    result
}

/// A migration role that can neither hand `enqueue_ring_cursor()` to the
/// ring's owner nor read the ring itself leaves the function out, with a
/// warning, rather than install one that would fail every enqueue.
#[sqlx::test(migrations = false)]
async fn floor_helper_leaves_out_a_function_its_owner_could_not_run(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    drop_floor(&pool, "awa").await?;

    let migrator = format!("floor_migrator_{}", uuid::Uuid::new_v4().simple());
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    sqlx::raw_sql(audited_sql(format!(
        "CREATE ROLE {migrator} LOGIN NOSUPERUSER PASSWORD 'floor_test';
         GRANT CONNECT ON DATABASE \"{database}\" TO {migrator};
         GRANT USAGE, CREATE ON SCHEMA awa TO {migrator};
         GRANT EXECUTE ON FUNCTION awa.refresh_ring_generation_floor_helper(text) TO {migrator};"
    )))
    .execute(&pool)
    .await?;
    let result = async {
        let migrator_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                pool.connect_options()
                    .as_ref()
                    .clone()
                    .username(&migrator)
                    .password("floor_test"),
            )
            .await?;
        sqlx::query("SELECT awa.refresh_ring_generation_floor_helper('awa')")
            .execute(&migrator_pool)
            .await?;
        migrator_pool.close().await;

        let present: bool =
            sqlx::query_scalar("SELECT to_regprocedure('awa.enqueue_ring_cursor()') IS NOT NULL")
                .fetch_one(&pool)
                .await?;
        assert!(!present, "the function is left out");

        let store = QueueStorage::new(QueueStorageConfig::default())?;
        store.enqueue_batch(&pool, "fallback_queue", 2, 1).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    sqlx::raw_sql(audited_sql(format!(
        "DROP OWNED BY {migrator}; DROP ROLE {migrator};"
    )))
    .execute(&pool)
    .await?;
    result
}
