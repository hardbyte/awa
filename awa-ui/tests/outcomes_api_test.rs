//! `GET /api/stats/outcomes`: dead letters per bucket, by queue and kind and
//! across bucket boundaries; terminal outcomes reported with no coverage on
//! either engine, without reading terminal rows; and parameter validation.

use awa_model::queue_storage::{PruneOutcome, QueueStorage, RotateOutcome};
use awa_testing::setup::TestDatabase;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Duration as TimeDelta, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use std::time::Duration;
use tower::util::ServiceExt;

#[derive(Debug, Deserialize)]
struct Outcomes {
    covered_since: Option<DateTime<Utc>>,
    dead_lettered_covered_since: DateTime<Utc>,
    buckets: Vec<Bucket>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
struct Bucket {
    bucket: DateTime<Utc>,
    completed: Option<i64>,
    failed: Option<i64>,
    cancelled: Option<i64>,
    dead_lettered: Option<i64>,
}

fn dead_letters(outcomes: &Outcomes) -> Vec<i64> {
    outcomes
        .buckets
        .iter()
        .map(|b| b.dead_lettered.expect("dead letters covered"))
        .collect()
}

/// The shape every response keeps: every bucket of the window, evenly
/// spaced, terminal series reserved (null, with no coverage) and dead letters
/// counted from the window start.
fn assert_shape(uri: &str, outcomes: &Outcomes) {
    assert_eq!(outcomes.covered_since, None, "{uri}");
    let first = outcomes.buckets.first().expect("buckets");
    assert_eq!(outcomes.dead_lettered_covered_since, first.bucket, "{uri}");
    for pair in outcomes.buckets.windows(2) {
        assert_eq!(
            pair[1].bucket - pair[0].bucket,
            outcomes.buckets[1].bucket - outcomes.buckets[0].bucket,
            "{uri}"
        );
    }
    for bucket in &outcomes.buckets {
        assert_eq!(
            (bucket.completed, bucket.failed, bucket.cancelled),
            (None, None, None),
            "{uri}: {bucket:?}"
        );
        assert!(bucket.dead_lettered.is_some(), "{uri}: {bucket:?}");
    }
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, body.to_vec())
}

async fn outcomes(app: &axum::Router, uri: &str) -> Outcomes {
    let (status, body) = get(app, uri).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{uri}: {}",
        String::from_utf8_lossy(&body)
    );
    let outcomes = serde_json::from_slice(&body).unwrap();
    assert_shape(uri, &outcomes);
    outcomes
}

async fn insert(pool: &PgPool, kind: &str, queue: &str, count: usize) -> Vec<i64> {
    let params: Vec<_> = (0..count)
        .map(|_| awa_model::InsertParams {
            kind: kind.to_string(),
            args: serde_json::json!({}),
            opts: awa_model::InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        })
        .collect();
    awa_model::insert_many(pool, &params)
        .await
        .unwrap()
        .into_iter()
        .map(|job| job.id)
        .collect()
}

#[tokio::test]
async fn invalid_parameters_are_refused() {
    let db = TestDatabase::canonical().await;
    let app = awa_ui::router(db.pool().clone(), Duration::ZERO)
        .await
        .unwrap();
    for query in [
        "minutes=0",
        "minutes=1441",
        "minutes=abc",
        "bucket=7s",
        "bucket=60",
        "minutes=90&bucket=3600s",
        "queue=",
        "kind=",
        "state=failed",
        "limit=10",
    ] {
        let uri = format!("/api/stats/outcomes?{query}");
        let (status, body) = get(&app, &uri).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{uri}: {}",
            String::from_utf8_lossy(&body)
        );
    }
    let defaults = outcomes(&app, "/api/stats/outcomes").await;
    assert_eq!(defaults.buckets.len(), 60);
    let day = outcomes(&app, "/api/stats/outcomes?minutes=1440&bucket=3600s").await;
    assert_eq!(day.buckets.len(), 24);
}

async fn queue_storage() -> (TestDatabase, QueueStorage) {
    let db = TestDatabase::queue_storage().await;
    let schema = QueueStorage::active_schema(db.pool())
        .await
        .unwrap()
        .expect("queue storage is active");
    let store = QueueStorage::from_existing_schema(schema).unwrap();
    (db, store)
}

/// Claim every job on `queue`, then complete the first `completed`, fail the
/// next `failed` and dead-letter the next `dead`.
async fn work_queue(
    pool: &PgPool,
    store: &QueueStorage,
    queue: &str,
    completed: usize,
    failed: usize,
    dead: usize,
) {
    let claims = store
        .claim_runtime_batch(pool, queue, 100, Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(claims.len(), completed + failed + dead, "{queue}");
    let (done, rest) = claims.split_at(completed);
    if !done.is_empty() {
        store.complete_runtime_batch(pool, done).await.unwrap();
    }
    let (fail, dlq) = rest.split_at(failed);
    for claim in fail {
        store
            .fail_terminal(pool, claim.job.id, claim.job.run_lease, "boom", None)
            .await
            .unwrap()
            .expect("failed");
    }
    for claim in dlq {
        store
            .fail_to_dlq(
                pool,
                claim.job.id,
                claim.job.run_lease,
                "poison",
                "boom",
                None,
            )
            .await
            .unwrap()
            .expect("dead-lettered");
    }
}

/// Tuples read from the terminal-row tables (done entries and completion
/// batches, partitions included) by the backend of `pool`, which must have a
/// single connection.
async fn terminal_reads(pool: &PgPool) -> i64 {
    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query_scalar(
        "SELECT COALESCE(sum(COALESCE(seq_tup_read, 0) + COALESCE(idx_tup_fetch, 0)), 0)::bigint \
         FROM pg_stat_user_tables WHERE schemaname = 'awa' \
           AND (relname ~ '^done_entries' OR relname ~ '^receipt_completion_')",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn queue_storage_counts_dead_letters_and_reports_no_terminal_coverage() {
    let (db, store) = queue_storage().await;
    let pool = db.pool().clone();

    insert(&pool, "send", "qs_a", 6).await;
    work_queue(&pool, &store, "qs_a", 3, 2, 1).await;
    insert(&pool, "sync", "qs_b", 4).await;
    work_queue(&pool, &store, "qs_b", 1, 1, 2).await;
    for id in insert(&pool, "sync", "qs_b", 2).await {
        store
            .cancel_job(&pool, id)
            .await
            .unwrap()
            .expect("cancelled");
    }
    // A dead letter from before the window is not counted.
    insert(&pool, "old", "qs_a", 1).await;
    work_queue(&pool, &store, "qs_a", 0, 0, 1).await;
    sqlx::query(
        "UPDATE awa.dlq_entries SET dlq_at = now() - interval '3 hours' WHERE kind = 'old'",
    )
    .execute(&pool)
    .await
    .unwrap();

    let single = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::raw_sql("SET stats_fetch_consistency = 'none'")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&db.url())
        .await
        .unwrap();
    let app = awa_ui::router(single.clone(), Duration::ZERO)
        .await
        .unwrap();
    let reads_before = terminal_reads(&single).await;
    for (query, expected) in [
        ("", 3),
        ("&queue=qs_a", 1),
        ("&queue=qs_b", 2),
        ("&kind=send", 1),
        ("&kind=sync", 2),
        ("&kind=old", 0),
        ("&queue=qs_a&kind=sync", 0),
        ("&queue=qs_b&kind=sync", 2),
        ("&queue=missing", 0),
    ] {
        let uri = format!("/api/stats/outcomes?minutes=60&bucket=60s{query}");
        let result = outcomes(&app, &uri).await;
        assert_eq!(result.buckets.len(), 60, "{uri}");
        assert_eq!(dead_letters(&result).iter().sum::<i64>(), expected, "{uri}");
    }
    let day = outcomes(&app, "/api/stats/outcomes?minutes=1440&bucket=3600s").await;
    assert_eq!(day.buckets.len(), 24);
    assert_eq!(dead_letters(&day).iter().sum::<i64>(), 4);
    // Terminal rows are not read at all.
    assert_eq!(terminal_reads(&single).await - reads_before, 0);
    single.close().await;
}

#[tokio::test]
async fn queue_storage_dead_letters_split_at_bucket_boundaries() {
    let (db, store) = queue_storage().await;
    let pool = db.pool().clone();
    let app = awa_ui::router(pool.clone(), Duration::ZERO).await.unwrap();
    insert(&pool, "send", "edge_q", 4).await;
    work_queue(&pool, &store, "edge_q", 0, 0, 4).await;
    let ids: Vec<i64> = sqlx::query_scalar("SELECT job_id FROM awa.dlq_entries ORDER BY job_id")
        .fetch_all(&pool)
        .await
        .unwrap();
    let uri = "/api/stats/outcomes?minutes=30&bucket=300s";

    // Every dead letter is placed relative to the buckets read in the same
    // attempt; a boundary passing between the reads moves the window, and the
    // attempt is retried.
    for _ in 0..3 {
        let before = outcomes(&app, uri).await;
        assert_eq!(before.buckets.len(), 6);
        let start = |n: usize| before.buckets[n].bucket;
        let micro = TimeDelta::microseconds(1);
        for (id, at) in ids
            .iter()
            .zip([start(0), start(1) - micro, start(0) - micro, start(5)])
        {
            sqlx::query("UPDATE awa.dlq_entries SET dlq_at = $2 WHERE job_id = $1")
                .bind(id)
                .bind(at)
                .execute(&pool)
                .await
                .unwrap();
        }
        let after = outcomes(&app, uri).await;
        if after.buckets[0].bucket != before.buckets[0].bucket {
            continue;
        }
        assert_eq!(dead_letters(&after), vec![2, 0, 0, 0, 0, 1]);
        return;
    }
    panic!("the window moved on every attempt");
}

#[tokio::test]
async fn ring_prune_leaves_dead_letters_counted() {
    let (db, store) = queue_storage().await;
    let pool = db.pool().clone();
    let app = awa_ui::router(pool.clone(), Duration::ZERO).await.unwrap();
    let uri = "/api/stats/outcomes?minutes=60&bucket=60s";
    insert(&pool, "send", "ring_q", 4).await;
    work_queue(&pool, &store, "ring_q", 2, 0, 2).await;
    assert_eq!(
        dead_letters(&outcomes(&app, uri).await).iter().sum::<i64>(),
        2
    );

    assert!(matches!(
        store.rotate(&pool).await.unwrap(),
        RotateOutcome::Rotated { generation: 1, .. }
    ));
    assert!(matches!(
        store.prune_oldest(&pool, Duration::ZERO).await.unwrap(),
        PruneOutcome::Pruned { generation: 0, .. }
    ));
    let pruned = outcomes(&app, uri).await;
    assert_eq!(pruned.buckets.len(), 60);
    assert_eq!(dead_letters(&pruned).iter().sum::<i64>(), 2);
}

/// Reads of `awa.jobs_hot` by the backend of `pool`, which must have a single
/// connection: `(sequential scans, tuples read)`.
async fn jobs_hot_reads(pool: &PgPool) -> (i64, i64) {
    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query_as(
        "SELECT COALESCE(seq_scan, 0)::bigint, \
                (COALESCE(seq_tup_read, 0) + COALESCE(idx_tup_fetch, 0))::bigint \
         FROM pg_stat_user_tables WHERE schemaname = 'awa' AND relname = 'jobs_hot'",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

/// The canonical engine's cleanup removes terminal rows past a retention
/// the database does not record, so none of its counts can be told complete:
/// the read reports no terminal coverage, and does not read `awa.jobs_hot`.
#[tokio::test]
async fn canonical_reports_no_terminal_coverage_and_reads_nothing() {
    let db = TestDatabase::canonical().await;
    // Retained terminal rows of every kind inside the window, as a short
    // retention would leave them: none of them may be reported.
    sqlx::raw_sql(
        "INSERT INTO awa.jobs_hot (kind, queue, state, attempt, run_at, created_at, finalized_at) \
         SELECT 'k' || (g % 3), 'q' || (g % 2), \
                (ARRAY['completed', 'failed', 'cancelled'])[1 + g % 3]::awa.job_state, 1, at, at, at \
         FROM generate_series(1, 3000) AS g, \
              LATERAL (SELECT now() - (g % 3600) * interval '1 second' AS at) AS t",
    )
    .execute(db.pool())
    .await
    .unwrap();

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::raw_sql("SET stats_fetch_consistency = 'none'")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&db.url())
        .await
        .unwrap();
    let app = awa_ui::router(pool.clone(), Duration::ZERO).await.unwrap();
    let (scans_before, tuples_before) = jobs_hot_reads(&pool).await;
    for (query, count) in [
        ("minutes=60&bucket=60s", 60),
        ("minutes=120&bucket=300s&queue=q0", 24),
        ("minutes=1440&bucket=3600s&kind=k1", 24),
        ("minutes=30&bucket=900s&queue=q1&kind=k2", 2),
    ] {
        let uri = format!("/api/stats/outcomes?{query}");
        let result = outcomes(&app, &uri).await;
        assert_eq!(result.covered_since, None, "{uri}");
        assert_eq!(result.buckets.len(), count, "{uri}");
        assert_eq!(
            result.dead_lettered_covered_since, result.buckets[0].bucket,
            "{uri}"
        );
        for bucket in &result.buckets {
            assert_eq!(bucket.completed, None, "{uri}");
            assert_eq!(bucket.failed, None, "{uri}");
            assert_eq!(bucket.cancelled, None, "{uri}");
            assert_eq!(bucket.dead_lettered, Some(0), "{uri}");
        }
        for pair in result.buckets.windows(2) {
            assert!(pair[1].bucket > pair[0].bucket, "{uri}");
        }
    }
    let (scans_after, tuples_after) = jobs_hot_reads(&pool).await;
    pool.close().await;
    assert_eq!(scans_after - scans_before, 0, "awa.jobs_hot was scanned");
    assert_eq!(tuples_after - tuples_before, 0, "awa.jobs_hot was read");
}
