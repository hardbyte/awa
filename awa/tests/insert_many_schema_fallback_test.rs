//! `insert_many` and `insert_many_copy` against a schema without
//! `awa.insert_jobs_compat_batch`.
//!
//! A binary may run ahead of migration v053. It must keep inserting through
//! the per-row function, start using the batch function once it exists, and
//! recover when the function goes away again. The path a call took is read
//! from `ready_segments`: the per-row function writes one segment per row, the
//! batch function one per lane run.
//!
//! The process-wide answer about the function is part of what is tested, so
//! this binary has one test, against a database of its own.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test insert_many_schema_fallback_test`

use awa::audited_sql;
use awa::model::{
    insert_many, insert_many_copy_from_pool, insert_many_skip_unique_conflicts, migrations,
    AwaError,
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

    // The migration runs while the process is up: the next call still takes
    // the per-row path, sees the function, and the one after uses it.
    sqlx::raw_sql(MIGRATION_V053)
        .execute(&pool)
        .await
        .expect("apply v053");
    insert_many(&pool, &jobs("fallback_b", 0..3, false))
        .await
        .expect("call that finds the function");
    assert_eq!(segments(&pool, "fallback_b").await, 3, "per-row path");
    insert_many(&pool, &jobs("fallback_b", 3..6, false))
        .await
        .expect("call through the batch function");
    assert_eq!(
        segments(&pool, "fallback_b").await,
        4,
        "the batch function wrote one segment for its run"
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

    // The function goes away under a process that has seen it: one call fails
    // with `undefined_function`, and the next probes again and succeeds.
    sqlx::query(DROP_BATCH_FUNCTION)
        .execute(&pool)
        .await
        .expect("drop batch function again");
    let failed = insert_many(&pool, &jobs("fallback_c", 0..3, false))
        .await
        .expect_err("the cached function is gone");
    assert!(
        matches!(&failed, AwaError::Database(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42883")),
        "{failed:?}"
    );
    assert!(ready_seqs(&pool, "fallback_c").await.is_empty());
    insert_many(&pool, &jobs("fallback_c", 0..3, false))
        .await
        .expect("the call after the failure falls back");
    assert_eq!(segments(&pool, "fallback_c").await, 3, "per-row path");

    pool.close().await;
    sqlx::query(audited_sql(format!(
        "DROP DATABASE {database} WITH (FORCE)"
    )))
    .execute(&admin)
    .await
    .expect("drop scratch database");
}
