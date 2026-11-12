//! `insert_many` and `insert_many_copy` against a schema without
//! `awa.insert_jobs_compat_batch` (migration v055), and the migrations that
//! install it (v055) and replace its body (v057).
//!
//! A binary may run ahead of `awa migrate`. Generic `insert_many` keeps using
//! the per-row function; COPY probes its own connection and uses the batch
//! function when it is installed and callable. Different databases in one
//! process can differ in whether they have the function without aborting their
//! caller's transactions.
//!
//! Requires a running Postgres instance and a role that may create databases
//! and roles.
//! Run with: `cargo test --package awa --test insert_many_schema_fallback_test`

use awa::audited_sql;
use awa::model::{
    insert_many, insert_many_copy, insert_many_copy_from_pool, insert_many_skip_unique_conflicts,
    migrations, AwaError,
};
use awa::{InsertOpts, InsertParams, UniqueOpts};
use awa_testing::setup;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

const BATCH_FUNCTION: &str = "insert_jobs_compat_batch";
const BATCH_FUNCTION_VERSION: i32 = 55;
const BOUNDED_WAIT_VERSION: i32 = 57;
const DROP_BATCH_FUNCTION: &str = "DROP FUNCTION awa.insert_jobs_compat_batch(text[], text[], \
     jsonb[], text[], smallint[], smallint[], timestamptz[], jsonb[], jsonb[], bytea[], text[], \
     text[], boolean)";

/// A migrated, queue-storage database of its own, dropped by [`drop`](Self::drop).
struct ScratchDatabase {
    name: String,
    pool: PgPool,
}

impl ScratchDatabase {
    async fn create() -> Self {
        let name = format!("awa_batchfn_{}", &Uuid::new_v4().simple().to_string()[..12]);
        let admin = admin_pool().await;
        sqlx::query(audited_sql(format!("CREATE DATABASE {name}")))
            .execute(&admin)
            .await
            .expect("create scratch database");
        admin.close().await;

        let options = PgConnectOptions::from_str(&setup::database_url())
            .expect("DATABASE_URL parses")
            .database(&name);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .expect("connect scratch database");
        migrations::run(&pool).await.expect("migrate");
        setup::activate_queue_storage(&pool).await;
        Self { name, pool }
    }

    async fn drop(self) {
        self.pool.close().await;
        let admin = admin_pool().await;
        sqlx::query(audited_sql(format!(
            "DROP DATABASE {} WITH (FORCE)",
            self.name
        )))
        .execute(&admin)
        .await
        .expect("drop scratch database");
        admin.close().await;
    }
}

async fn admin_pool() -> PgPool {
    let options = PgConnectOptions::from_str(&setup::database_url())
        .expect("DATABASE_URL parses")
        .database("postgres");
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("connect to the postgres maintenance database")
}

fn jobs(queue: &str, range: std::ops::Range<i64>, unique: bool) -> Vec<InsertParams> {
    range
        .map(|seq| InsertParams {
            kind: "schema_fallback_job".to_string(),
            args: serde_json::json!({ "seq": seq }),
            opts: InsertOpts {
                queue: queue.to_string(),
                unique: unique.then(|| UniqueOpts {
                    by_queue: true,
                    by_args: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        })
        .collect()
}

async fn segments(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*)::bigint FROM awa.ready_segments WHERE queue = $1")
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("count segments")
}

async fn ready_seqs(pool: &PgPool, queue: &str) -> Vec<i64> {
    sqlx::query_scalar(
        "SELECT (args->>'seq')::bigint FROM awa.ready_entries WHERE queue = $1 ORDER BY lane_seq",
    )
    .bind(queue)
    .fetch_all(pool)
    .await
    .expect("read ready rows")
}

fn seqs(rows: &[awa::JobRow]) -> Vec<i64> {
    rows.iter()
        .map(|row| row.args["seq"].as_i64().expect("seq"))
        .collect()
}

async fn has_batch_function(pool: &PgPool) -> bool {
    sqlx::query_scalar(
        "SELECT to_regprocedure('awa.insert_jobs_compat_batch(text[],text[],jsonb[],text[],\
         smallint[],smallint[],timestamptz[],jsonb[],jsonb[],bytea[],text[],text[],boolean)') \
         IS NOT NULL",
    )
    .fetch_one(pool)
    .await
    .expect("probe the batch function")
}

/// The database as a schema before v055 left it.
async fn roll_back_to_before_batch_function(pool: &PgPool) {
    sqlx::query(DROP_BATCH_FUNCTION)
        .execute(pool)
        .await
        .expect("drop batch function");
    sqlx::query("DELETE FROM awa.schema_version WHERE version >= $1")
        .bind(BATCH_FUNCTION_VERSION)
        .execute(pool)
        .await
        .expect("roll the schema version back");
}

#[tokio::test]
async fn binary_ahead_of_the_migration_falls_back_and_recovers() {
    let db = ScratchDatabase::create().await;
    let pool = &db.pool;
    assert!(has_batch_function(pool).await);

    roll_back_to_before_batch_function(pool).await;

    let first = insert_many(pool, &jobs("fallback_a", 0..3, false))
        .await
        .expect("insert_many without the batch function");
    assert_eq!(seqs(&first), vec![0, 1, 2]);
    assert_eq!(segments(pool, "fallback_a").await, 3, "per-row path");
    let copied = insert_many_copy_from_pool(pool, &jobs("fallback_a", 3..6, false))
        .await
        .expect("insert_many_copy without the batch function");
    assert_eq!(seqs(&copied), vec![3, 4, 5]);
    assert_eq!(segments(pool, "fallback_a").await, 6, "COPY stays per-row");
    assert_eq!(ready_seqs(pool, "fallback_a").await, vec![0, 1, 2, 3, 4, 5]);

    // Unique handling on the per-row path: a duplicate fails `insert_many`
    // and is skipped by `insert_many_copy`.
    insert_many(pool, &jobs("fallback_u", 0..2, true))
        .await
        .expect("seed unique jobs");
    let duplicate = insert_many(pool, &jobs("fallback_u", 1..3, true))
        .await
        .expect_err("a unique conflict fails the batch");
    assert!(
        matches!(&duplicate, AwaError::Database(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23505")),
        "{duplicate:?}"
    );
    let copied = insert_many_copy_from_pool(pool, &jobs("fallback_u", 1..4, true))
        .await
        .expect("unique insert_many_copy without the batch function");
    assert_eq!(seqs(&copied), vec![2, 3], "duplicate skipped");

    // The skip API needs the function and says so.
    let skipped = insert_many_skip_unique_conflicts(pool, &jobs("fallback_u", 0..1, true))
        .await
        .expect_err("needs the batch function");
    assert!(
        matches!(&skipped, AwaError::Validation(message) if message.contains(BATCH_FUNCTION)),
        "{skipped:?}"
    );

    migrations::run(pool).await.expect("migrate");
    assert!(has_batch_function(pool).await);

    // Generic insert_many stays on the per-row path; COPY picks the batch
    // function on its next call, on the same pool.
    insert_many(pool, &jobs("fallback_b", 0..3, false))
        .await
        .expect("generic insert after the migration");
    assert_eq!(segments(pool, "fallback_b").await, 3, "per-row path");
    let copied = insert_many_copy_from_pool(pool, &jobs("fallback_b", 3..6, false))
        .await
        .expect("COPY after the migration");
    assert_eq!(seqs(&copied), vec![3, 4, 5]);
    assert_eq!(
        segments(pool, "fallback_b").await,
        4,
        "COPY writes its batch as one segment"
    );
    assert_eq!(ready_seqs(pool, "fallback_b").await, vec![0, 1, 2, 3, 4, 5]);

    let copied = insert_many_copy_from_pool(pool, &jobs("fallback_u", 2..6, true))
        .await
        .expect("insert_many_copy with the batch function");
    assert_eq!(seqs(&copied), vec![4, 5], "duplicates skipped in one pass");
    let skipped = insert_many_skip_unique_conflicts(pool, &jobs("fallback_u", 0..8, true))
        .await
        .expect("skip with the batch function");
    assert_eq!(seqs(&skipped), vec![6, 7]);

    // Removing the function after a successful batch does not poison the next
    // generic or COPY call with a stale answer.
    sqlx::query(DROP_BATCH_FUNCTION)
        .execute(pool)
        .await
        .expect("drop batch function again");
    let mut tx = pool.begin().await.expect("caller transaction");
    insert_many(&mut *tx, &jobs("fallback_c", 0..3, false))
        .await
        .expect("generic insert still works");
    insert_many_copy(&mut tx, &jobs("fallback_c", 3..6, false))
        .await
        .expect("COPY re-probes its own connection");
    tx.commit()
        .await
        .expect("caller transaction remains usable");
    assert_eq!(segments(pool, "fallback_c").await, 6, "per-row paths");

    db.drop().await;
}

/// What one database says about the batch function never selects SQL for
/// another, and both paths join the caller's transaction, including its
/// rollback.
#[tokio::test]
async fn mixed_batch_function_databases_do_not_abort_caller_transactions() {
    let with_function = ScratchDatabase::create().await;
    let without_function = ScratchDatabase::create().await;
    for db in [&with_function, &without_function] {
        sqlx::query("CREATE TABLE application_writes (id bigint PRIMARY KEY)")
            .execute(&db.pool)
            .await
            .expect("create application table");
    }
    roll_back_to_before_batch_function(&without_function.pool).await;

    insert_many_skip_unique_conflicts(&with_function.pool, &jobs("mixed_warm", 0..2, true))
        .await
        .expect("batch API on the database with the function");

    for (index, db) in [&with_function, &without_function, &with_function]
        .into_iter()
        .enumerate()
    {
        let pool = &db.pool;
        let queue = format!("mixed_database_{index}");
        let mut tx = pool.begin().await.expect("caller transaction");
        sqlx::query("INSERT INTO application_writes (id) VALUES ($1)")
            .bind(index as i64)
            .execute(&mut *tx)
            .await
            .expect("application write");
        let inserted = insert_many(&mut *tx, &jobs(&queue, 0..3, true))
            .await
            .expect("generic insert must use SQL every schema supports");
        assert_eq!(seqs(&inserted), vec![0, 1, 2]);
        let copied = insert_many_copy(&mut tx, &jobs(&queue, 2..5, true))
            .await
            .expect("COPY must probe this database");
        assert_eq!(seqs(&copied), vec![3, 4], "duplicate skipped");
        let written: i64 =
            sqlx::query_scalar("SELECT count(*) FROM application_writes WHERE id = $1")
                .bind(index as i64)
                .fetch_one(&mut *tx)
                .await
                .expect("caller transaction was not aborted");
        assert_eq!(written, 1);
        tx.commit()
            .await
            .expect("commit application and jobs together");
        assert_eq!(ready_seqs(pool, &queue).await, vec![0, 1, 2, 3, 4]);

        let mut tx = pool.begin().await.expect("rollback transaction");
        sqlx::query("INSERT INTO application_writes (id) VALUES ($1)")
            .bind(index as i64 + 100)
            .execute(&mut *tx)
            .await
            .expect("application write to roll back");
        insert_many(&mut *tx, &jobs(&queue, 5..7, true))
            .await
            .expect("generic insert to roll back");
        insert_many_copy(&mut tx, &jobs(&queue, 7..9, true))
            .await
            .expect("COPY to roll back");
        tx.rollback().await.expect("roll back caller transaction");
        assert_eq!(ready_seqs(pool, &queue).await, vec![0, 1, 2, 3, 4]);
        let rolled_back: i64 =
            sqlx::query_scalar("SELECT count(*) FROM application_writes WHERE id = $1")
                .bind(index as i64 + 100)
                .fetch_one(pool)
                .await
                .expect("check application rollback");
        assert_eq!(rolled_back, 0);
    }

    with_function.drop().await;
    without_function.drop().await;
}

/// The exported v055 SQL, as an external runner applies it: twice in one
/// explicit transaction, then over an installed function.
#[tokio::test]
async fn exported_migration_reapplies_in_one_transaction() {
    let db = ScratchDatabase::create().await;
    let pool = &db.pool;
    let (_, _, sql) =
        migrations::migration_sql_range(BATCH_FUNCTION_VERSION - 1, BATCH_FUNCTION_VERSION)
            .pop()
            .expect("v055 is a migration");

    roll_back_to_before_batch_function(pool).await;
    sqlx::raw_sql(audited_sql(format!("BEGIN;\n{sql}\n{sql}\nCOMMIT;")))
        .execute(pool)
        .await
        .expect("apply v055 twice in one transaction");
    sqlx::raw_sql(audited_sql(sql))
        .execute(pool)
        .await
        .expect("apply over an installed function");

    assert!(has_batch_function(pool).await);
    let recorded: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.schema_version WHERE version = $1")
            .bind(BATCH_FUNCTION_VERSION)
            .fetch_one(pool)
            .await
            .expect("read schema versions");
    assert_eq!(recorded, 1);
    let copied = insert_many_copy_from_pool(pool, &jobs("exported", 0..4, false))
        .await
        .expect("COPY through the exported migration");
    assert_eq!(seqs(&copied), vec![0, 1, 2, 3]);
    assert_eq!(segments(pool, "exported").await, 1);

    db.drop().await;
}

/// TRUE when the installed batch function has the body v057 installs, which
/// bounds its lock waits and applies the caller's `lock_timeout` to each.
async fn batch_function_gives_way(pool: &PgPool) -> bool {
    sqlx::query_scalar(
        "SELECT p.prosrc ~ 'v_bounded_until := clock_timestamp\\(\\) \\+ 4 \\* v_wait_budget' \
                AND p.prosrc ~ 'v_caller_wait_ms BETWEEN 1 AND v_wait_ms' \
         FROM pg_proc AS p WHERE p.oid = to_regprocedure(\
         'awa.insert_jobs_compat_batch(text[],text[],jsonb[],text[],smallint[],smallint[],\
         timestamptz[],jsonb[],jsonb[],bytea[],text[],text[],boolean)')",
    )
    .fetch_one(pool)
    .await
    .expect("read the batch function body")
}

/// The database as v056 left it: the batch function has v055's body, which
/// waited for a lane without bound while holding the batch's unique claims.
async fn roll_back_to_the_unbounded_batch_function(pool: &PgPool) {
    let (_, _, v055) =
        migrations::migration_sql_range(BATCH_FUNCTION_VERSION - 1, BATCH_FUNCTION_VERSION)
            .pop()
            .expect("v055 is a migration");
    sqlx::raw_sql(audited_sql(v055))
        .execute(pool)
        .await
        .expect("install v055's body");
    sqlx::query("DELETE FROM awa.schema_version WHERE version >= $1")
        .bind(BOUNDED_WAIT_VERSION)
        .execute(pool)
        .await
        .expect("roll the schema version back");
}

/// v057 replaces v055's body of the batch function: through `awa migrate`,
/// and through the exported SQL applied twice in one explicit transaction and
/// then over the installed function.
#[tokio::test]
async fn migration_replaces_the_unbounded_batch_function_body() {
    let db = ScratchDatabase::create().await;
    let pool = &db.pool;
    assert!(batch_function_gives_way(pool).await);

    roll_back_to_the_unbounded_batch_function(pool).await;
    assert!(!batch_function_gives_way(pool).await);
    migrations::run(pool).await.expect("migrate");
    assert!(batch_function_gives_way(pool).await);
    assert_eq!(
        migrations::current_version(pool).await.expect("version"),
        migrations::CURRENT_VERSION
    );

    let (_, _, sql) =
        migrations::migration_sql_range(BOUNDED_WAIT_VERSION - 1, BOUNDED_WAIT_VERSION)
            .pop()
            .expect("v057 is a migration");
    roll_back_to_the_unbounded_batch_function(pool).await;
    sqlx::raw_sql(audited_sql(format!("BEGIN;\n{sql}\n{sql}\nCOMMIT;")))
        .execute(pool)
        .await
        .expect("apply v057 twice in one transaction");
    sqlx::raw_sql(audited_sql(sql))
        .execute(pool)
        .await
        .expect("apply over the installed function");
    assert!(batch_function_gives_way(pool).await);
    let recorded: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.schema_version WHERE version = $1")
            .bind(BOUNDED_WAIT_VERSION)
            .fetch_one(pool)
            .await
            .expect("read schema versions");
    assert_eq!(recorded, 1);
    let copied = insert_many_copy_from_pool(pool, &jobs("bounded", 0..4, true))
        .await
        .expect("COPY through the exported migration");
    assert_eq!(seqs(&copied), vec![0, 1, 2, 3]);
    assert_eq!(segments(pool, "bounded").await, 1);

    db.drop().await;
}

/// A role that may not execute the batch function keeps COPY on the per-row
/// path instead of failing it with `permission denied`.
#[tokio::test]
async fn copy_without_execute_on_the_batch_function_stays_per_row() {
    let db = ScratchDatabase::create().await;
    let pool = &db.pool;
    // Provision the lane first, so the role needs no sequence DDL and the
    // grants below cover its sequences.
    insert_many(pool, &jobs("no_execute", 0..1, false))
        .await
        .expect("provision lane");
    let role = format!(
        "awa_batchfn_role_{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    sqlx::raw_sql(audited_sql(format!(
        "CREATE ROLE {role} NOLOGIN;
         GRANT TEMP ON DATABASE {database} TO {role};
         GRANT USAGE ON SCHEMA awa TO {role};
         GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA awa TO {role};
         GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA awa TO {role};
         GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa TO {role};
         REVOKE EXECUTE ON FUNCTION awa.insert_jobs_compat_batch(text[], text[], jsonb[], text[],
             smallint[], smallint[], timestamptz[], jsonb[], jsonb[], bytea[], text[], text[], boolean)
             FROM PUBLIC, {role};",
        database = db.name,
    )))
    .execute(pool)
    .await
    .expect("create a role without EXECUTE on the batch function");

    // The role is cluster-wide, so it is dropped even when an assertion fails.
    let outcome = tokio::spawn({
        let pool = db.pool.clone();
        let role = role.clone();
        async move {
            let pool = &pool;
            let mut tx = pool.begin().await.expect("begin");
            sqlx::query(audited_sql(format!("SET LOCAL ROLE {role}")))
                .execute(&mut *tx)
                .await
                .expect("become the restricted role");
            let copied = insert_many_copy(&mut tx, &jobs("no_execute", 1..4, true))
                .await
                .expect("COPY without EXECUTE on the batch function");
            assert_eq!(seqs(&copied), vec![1, 2, 3]);
            let skipped = insert_many_skip_unique_conflicts(&mut *tx, &jobs("no_execute", 4..5, true))
                .await
                .expect_err("the skip API calls the function directly");
            assert!(
                matches!(&skipped, AwaError::Database(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42501")),
                "{skipped:?}"
            );
            tx.rollback().await.expect("roll back");

            let mut tx = pool.begin().await.expect("begin");
            sqlx::query(audited_sql(format!("SET LOCAL ROLE {role}")))
                .execute(&mut *tx)
                .await
                .expect("become the restricted role");
            insert_many_copy(&mut tx, &jobs("no_execute", 1..4, true))
                .await
                .expect("COPY without EXECUTE on the batch function");
            tx.commit().await.expect("commit");
            assert_eq!(ready_seqs(pool, "no_execute").await, vec![0, 1, 2, 3]);
            assert_eq!(segments(pool, "no_execute").await, 4, "per-row path");

        }
    })
    .await;

    db.drop().await;
    let admin = admin_pool().await;
    sqlx::query(audited_sql(format!("DROP ROLE {role}")))
        .execute(&admin)
        .await
        .expect("drop role");
    admin.close().await;
    if let Err(failure) = outcome {
        std::panic::resume_unwind(failure.into_panic());
    }
}
