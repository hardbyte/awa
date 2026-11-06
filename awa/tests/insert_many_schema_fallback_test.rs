//! `insert_many` and `insert_many_copy` against a schema without
//! `awa.insert_jobs_compat_batch`.
//!
//! A binary may run ahead of migration v053. Generic `insert_many` keeps using
//! the per-row function; COPY probes its own connection and uses the batch
//! function when installed. Different databases in one process can have
//! different schema versions without aborting their caller's transactions.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test insert_many_schema_fallback_test`

use awa::audited_sql;
use awa::model::{
    insert_many, insert_many_copy, insert_many_copy_from_pool, insert_many_skip_unique_conflicts,
    migrations, AwaError,
};
use awa::{InsertOpts, InsertParams, UniqueOpts};
use awa_testing::setup;
use sqlx::PgPool;
use uuid::Uuid;

const DROP_BATCH_FUNCTION: &str = "DROP FUNCTION awa.insert_jobs_compat_batch(text[], text[], \
     jsonb[], text[], smallint[], smallint[], timestamptz[], jsonb[], jsonb[], bytea[], text[], \
     text[], boolean)";
const MIGRATION_V053: &str =
    include_str!("../../awa-model/migrations/v053_insert_jobs_batch_compat.sql");

fn database_url_for(database: &str) -> String {
    let url = setup::database_url();
    let (without_query, query) = match url.split_once('?') {
        Some((prefix, query)) => (prefix, Some(query)),
        None => (url.as_str(), None),
    };
    let (base, _) = without_query
        .rsplit_once('/')
        .expect("database name in URL");
    match query {
        Some(query) => format!("{base}/{database}?{query}"),
        None => format!("{base}/{database}"),
    }
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

#[tokio::test]
async fn binary_ahead_of_the_migration_falls_back_and_recovers() {
    let admin = setup::pool(2).await;
    let database = format!("awa_batchfn_{}", &Uuid::new_v4().simple().to_string()[..12]);
    sqlx::query(audited_sql(format!("CREATE DATABASE {database}")))
        .execute(&admin)
        .await
        .expect("create scratch database");

    let pool = setup::pool_with_url(&database_url_for(&database), 4).await;
    migrations::run(&pool).await.expect("migrate");
    setup::activate_queue_storage(&pool).await;

    // A schema from before v053.
    sqlx::query(DROP_BATCH_FUNCTION)
        .execute(&pool)
        .await
        .expect("drop batch function");

    let first = insert_many(&pool, &jobs("fallback_a", 0..3, false))
        .await
        .expect("insert_many without the batch function");
    assert_eq!(seqs(&first), vec![0, 1, 2]);
    assert_eq!(segments(&pool, "fallback_a").await, 3, "per-row path");
    insert_many(&pool, &jobs("fallback_a", 3..6, false))
        .await
        .expect("a second call without the function");
    assert_eq!(segments(&pool, "fallback_a").await, 6, "still per-row");
    assert_eq!(
        ready_seqs(&pool, "fallback_a").await,
        vec![0, 1, 2, 3, 4, 5]
    );

    // Unique handling on the per-row path: a duplicate fails `insert_many`
    // and is skipped by `insert_many_copy`.
    insert_many(&pool, &jobs("fallback_u", 0..2, true))
        .await
        .expect("seed unique jobs");
    let duplicate = insert_many(&pool, &jobs("fallback_u", 1..3, true))
        .await
        .expect_err("a unique conflict fails the batch");
    assert!(
        matches!(&duplicate, AwaError::Database(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23505")),
        "{duplicate:?}"
    );
    let copied = insert_many_copy_from_pool(&pool, &jobs("fallback_u", 1..4, true))
        .await
        .expect("insert_many_copy without the batch function");
    assert_eq!(seqs(&copied), vec![2, 3], "duplicate skipped");

    // The skip API is new with v053 and says so.
    let skipped = insert_many_skip_unique_conflicts(&pool, &jobs("fallback_u", 0..1, true))
        .await
        .expect_err("needs the migration");
    assert!(
        matches!(&skipped, AwaError::Validation(message) if message.contains("v053")),
        "{skipped:?}"
    );

    // Generic insert_many stays on the per-row path when the migration runs;
    // COPY chooses the batch function on its next call.
    sqlx::raw_sql(MIGRATION_V053)
        .execute(&pool)
        .await
        .expect("apply v053");
    insert_many(&pool, &jobs("fallback_b", 0..3, false))
        .await
        .expect("generic insert after migration");
    assert_eq!(segments(&pool, "fallback_b").await, 3, "per-row path");
    insert_many(&pool, &jobs("fallback_b", 3..6, false))
        .await
        .expect("second generic insert after migration");
    assert_eq!(
        segments(&pool, "fallback_b").await,
        6,
        "generic insert_many keeps its schema-independent per-row path"
    );
    assert_eq!(
        ready_seqs(&pool, "fallback_b").await,
        vec![0, 1, 2, 3, 4, 5]
    );

    let copied = insert_many_copy_from_pool(&pool, &jobs("fallback_u", 2..6, true))
        .await
        .expect("insert_many_copy with the batch function");
    assert_eq!(seqs(&copied), vec![4, 5], "duplicates skipped in one pass");
    let skipped = insert_many_skip_unique_conflicts(&pool, &jobs("fallback_u", 0..8, true))
        .await
        .expect("skip with the batch function");
    assert_eq!(seqs(&skipped), vec![6, 7]);

    // Removing the function after a successful batch does not poison the next
    // generic or COPY call with a stale process-wide answer.
    sqlx::query(DROP_BATCH_FUNCTION)
        .execute(&pool)
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
    assert_eq!(segments(&pool, "fallback_c").await, 6, "per-row paths");

    pool.close().await;
    sqlx::query(audited_sql(format!(
        "DROP DATABASE {database} WITH (FORCE)"
    )))
    .execute(&admin)
    .await
    .expect("drop scratch database");
}

/// Availability learned from one database must never select SQL for another
/// database. Both paths join the caller's transaction, including its rollback.
#[tokio::test]
async fn mixed_schema_databases_do_not_abort_caller_transactions() {
    let current = setup::TestDatabase::canonical().await;
    let older = setup::TestDatabase::canonical().await;
    for db in [&current, &older] {
        setup::activate_queue_storage(db.pool()).await;
        sqlx::query("CREATE TABLE application_writes (id bigint PRIMARY KEY)")
            .execute(db.pool())
            .await
            .expect("create application table");
    }
    sqlx::query(DROP_BATCH_FUNCTION)
        .execute(older.pool())
        .await
        .expect("older database has no batch function");

    // This call previously seeded the shared cache with Present.
    insert_many_skip_unique_conflicts(current.pool(), &jobs("mixed_warm", 0..2, true))
        .await
        .expect("batch API on current schema");

    for (index, db) in [&current, &older, &current].into_iter().enumerate() {
        let pool = db.pool();
        let queue = format!("mixed_database_{index}");
        let mut tx = pool.begin().await.expect("caller transaction");
        sqlx::query("INSERT INTO application_writes (id) VALUES ($1)")
            .bind(index as i64)
            .execute(&mut *tx)
            .await
            .expect("application write");
        let inserted = insert_many(&mut *tx, &jobs(&queue, 0..3, true))
            .await
            .expect("generic insert must use this database's supported SQL");
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
}
