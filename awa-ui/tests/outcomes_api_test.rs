//! `GET /api/stats/outcomes`: bucket boundaries, the queue
//! and kind filters, `covered_since` after queue-ring prune, the dead-letter
//! series' own coverage, no terminal coverage on the canonical engine, and
//! parameter validation.

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

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
struct Bucket {
    bucket: DateTime<Utc>,
    completed: Option<i64>,
    failed: Option<i64>,
    cancelled: Option<i64>,
    dead_lettered: Option<i64>,
}

impl Bucket {
    /// Every series' count, for a bucket both coverages include.
    fn counts(&self) -> (i64, i64, i64, i64) {
        (
            self.completed.expect("completed covered"),
            self.failed.expect("failed covered"),
            self.cancelled.expect("cancelled covered"),
            self.dead_lettered.expect("dead_lettered covered"),
        )
    }
}

/// Each series summed over the buckets it covers.
fn totals(outcomes: &Outcomes) -> (i64, i64, i64, i64) {
    outcomes.buckets.iter().fold((0, 0, 0, 0), |sum, b| {
        (
            sum.0 + b.completed.unwrap_or(0),
            sum.1 + b.failed.unwrap_or(0),
            sum.2 + b.cancelled.unwrap_or(0),
            sum.3 + b.dead_lettered.unwrap_or(0),
        )
    })
}

/// The shape every response keeps: buckets from the earlier coverage time
/// on, each series counted exactly in the buckets its coverage includes.
fn assert_coverage(uri: &str, outcomes: &Outcomes) {
    let first = outcomes
        .covered_since
        .map_or(outcomes.dead_lettered_covered_since, |since| {
            since.min(outcomes.dead_lettered_covered_since)
        });
    if let Some(bucket) = outcomes.buckets.first() {
        assert!(bucket.bucket >= first, "{uri}: bucket before coverage");
        assert!(
            bucket.bucket - first < TimeDelta::seconds(3600),
            "{uri}: covered buckets left out"
        );
    }
    for bucket in &outcomes.buckets {
        let terminal = outcomes
            .covered_since
            .is_some_and(|since| bucket.bucket >= since);
        for value in [bucket.completed, bucket.failed, bucket.cancelled] {
            assert_eq!(value.is_some(), terminal, "{uri}: {bucket:?}");
        }
        assert_eq!(
            bucket.dead_lettered.is_some(),
            bucket.bucket >= outcomes.dead_lettered_covered_since,
            "{uri}: {bucket:?}"
        );
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
    assert_coverage(uri, &outcomes);
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

#[tokio::test]
async fn queue_storage_counts_every_terminal_source_and_filter() {
    let (db, store) = queue_storage().await;
    let pool = db.pool().clone();
    let app = awa_ui::router(pool.clone(), Duration::ZERO).await.unwrap();

    // A dead letter on each queue from before the window, so dead-letter
    // coverage spans the window.
    for queue in ["qs_a", "qs_b"] {
        insert(&pool, "old", queue, 1).await;
        work_queue(&pool, &store, queue, 0, 0, 1).await;
    }
    sqlx::query("UPDATE awa.dlq_entries SET dlq_at = now() - interval '3 hours'")
        .execute(&pool)
        .await
        .unwrap();

    insert(&pool, "send", "qs_a", 6).await;
    work_queue(&pool, &store, "qs_a", 3, 2, 1).await;
    insert(&pool, "sync", "qs_b", 4).await;
    work_queue(&pool, &store, "qs_b", 1, 1, 2).await;
    let cancelled = insert(&pool, "sync", "qs_b", 2).await;
    for id in cancelled {
        store
            .cancel_job(&pool, id)
            .await
            .unwrap()
            .expect("cancelled");
    }

    for (query, expected) in [
        ("", (4, 3, 2, 3)),
        ("&queue=qs_a", (3, 2, 0, 1)),
        ("&queue=qs_b", (1, 1, 2, 2)),
        ("&kind=send", (3, 2, 0, 1)),
        ("&kind=sync", (1, 1, 2, 2)),
        ("&queue=qs_a&kind=sync", (0, 0, 0, 0)),
        ("&queue=qs_b&kind=sync", (1, 1, 2, 2)),
    ] {
        let uri = format!("/api/stats/outcomes?minutes=60&bucket=60s{query}");
        let result = outcomes(&app, &uri).await;
        assert_eq!(result.buckets.len(), 60, "{uri}");
        assert_eq!(
            result.covered_since,
            Some(result.buckets[0].bucket),
            "{uri}"
        );
        assert_eq!(totals(&result), expected, "{uri}");
    }

    // Each source is bucketed by its own finalisation time.
    let uri = "/api/stats/outcomes?minutes=60&bucket=300s";
    for _ in 0..3 {
        let before = outcomes(&app, uri).await;
        let start = |n: usize| before.buckets[n].bucket;
        let micro = TimeDelta::microseconds(1);
        for (sql, at) in [
            (
                "UPDATE awa.receipt_completion_batches SET finalized_at = $1 WHERE queue = 'qs_a'",
                start(0),
            ),
            (
                "UPDATE awa.done_entries SET finalized_at = $1 \
                 WHERE queue = 'qs_a' AND state = 'failed'",
                start(1) - micro,
            ),
            (
                "UPDATE awa.done_entries SET finalized_at = $1 \
                 WHERE queue = 'qs_b' AND state = 'cancelled'",
                start(0) - micro,
            ),
            (
                "UPDATE awa.dlq_entries SET dlq_at = $1 WHERE queue = 'qs_b' AND kind <> 'old'",
                start(4),
            ),
        ] {
            sqlx::query(sql).bind(at).execute(&pool).await.unwrap();
        }
        let after = outcomes(&app, uri).await;
        if after.buckets[0].bucket != before.buckets[0].bucket {
            continue;
        }
        assert_eq!(after.buckets[0].counts(), (3, 2, 0, 0));
        assert_eq!(after.buckets[4].counts(), (0, 0, 0, 2));
        // qs_b's completion, failure and qs_a's dead letter stay current;
        // the cancellations fell out of the window.
        assert_eq!(after.buckets[11].counts(), (1, 1, 0, 1));
        assert_eq!(totals(&after), (4, 3, 0, 3));
        return;
    }
    panic!("the window moved on every attempt");
}

async fn queue_rotated_at(pool: &PgPool, generation: i64) -> DateTime<Utc> {
    sqlx::query_scalar("SELECT rotated_at FROM awa.queue_ring_rotations WHERE generation = $1")
        .bind(generation)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn queue_storage_covered_since_follows_ring_prune() {
    let (db, store) = queue_storage().await;
    let pool = db.pool().clone();
    let app = awa_ui::router(pool.clone(), Duration::ZERO).await.unwrap();
    let uri = "/api/stats/outcomes?minutes=60&bucket=60s";

    insert(&pool, "send", "ring_q", 3).await;
    work_queue(&pool, &store, "ring_q", 2, 1, 0).await;

    // Generation 0: nothing can have been pruned.
    let fresh = outcomes(&app, uri).await;
    assert_eq!(fresh.buckets.len(), 60);
    assert_eq!(fresh.covered_since, Some(fresh.buckets[0].bucket));
    assert_eq!(totals(&fresh), (2, 1, 0, 0));

    // Seal generation 0 and prune it.
    assert!(matches!(
        store.rotate(&pool).await.unwrap(),
        RotateOutcome::Rotated { generation: 1, .. }
    ));
    assert!(matches!(
        store.prune_oldest(&pool, Duration::ZERO).await.unwrap(),
        PruneOutcome::Pruned { generation: 0, .. }
    ));
    let opened = queue_rotated_at(&pool, 1).await;
    let pruned = outcomes(&app, uri).await;
    assert_eq!(pruned.covered_since, Some(opened));
    // No dead letter is retained, so that series covers the window, and
    // every terminal count before the horizon is unknown, not zero.
    assert_eq!(pruned.dead_lettered_covered_since, pruned.buckets[0].bucket);
    assert_eq!(pruned.buckets.len(), 60);
    assert!(pruned
        .buckets
        .iter()
        .filter(|b| b.bucket < opened)
        .all(|b| b.completed.is_none() && b.dead_lettered == Some(0)));
    assert_eq!(totals(&pruned), (0, 0, 0, 0));

    // Work in generation 1 is counted from the horizon on.
    insert(&pool, "send", "ring_q", 1).await;
    work_queue(&pool, &store, "ring_q", 1, 0, 0).await;
    sqlx::query(
        "UPDATE awa.queue_ring_rotations SET rotated_at = now() - interval '20 minutes' \
         WHERE generation = 1",
    )
    .execute(&pool)
    .await
    .unwrap();
    let opened = queue_rotated_at(&pool, 1).await;
    let after = outcomes(&app, uri).await;
    assert_eq!(after.covered_since, Some(opened));
    let terminal: Vec<_> = after
        .buckets
        .iter()
        .filter(|b| b.completed.is_some())
        .collect();
    assert!(terminal[0].bucket - opened < TimeDelta::seconds(60));
    assert!((19..=20).contains(&terminal.len()));
    assert_eq!(totals(&after), (1, 0, 0, 0));
}

#[tokio::test]
async fn queue_storage_dead_letters_keep_their_own_coverage() {
    let (db, store) = queue_storage().await;
    let pool = db.pool().clone();
    let app = awa_ui::router(pool.clone(), Duration::ZERO).await.unwrap();
    let uri = "/api/stats/outcomes?minutes=60&bucket=60s";

    insert(&pool, "send", "dl_q", 5).await;
    work_queue(&pool, &store, "dl_q", 2, 0, 3).await;
    // Ring prune reclaims the completions, not the dead letters.
    assert!(matches!(
        store.rotate(&pool).await.unwrap(),
        RotateOutcome::Rotated { generation: 1, .. }
    ));
    assert!(matches!(
        store.prune_oldest(&pool, Duration::ZERO).await.unwrap(),
        PruneOutcome::Pruned { generation: 0, .. }
    ));
    let job_ids: Vec<i64> =
        sqlx::query_scalar("SELECT job_id FROM awa.dlq_entries ORDER BY job_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    let backdate = |id: i64, interval: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE awa.dlq_entries SET dlq_at = now() - interval '{interval}' WHERE job_id = $1"
            )))
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        }
    };
    backdate(job_ids[0], "50 minutes").await;
    backdate(job_ids[1], "10 minutes").await;

    // The oldest retained dead letter bounds the series: an earlier bucket
    // could have held dead letters that DLQ cleanup has since removed.
    let oldest: DateTime<Utc> = sqlx::query_scalar("SELECT min(dlq_at) FROM awa.dlq_entries")
        .fetch_one(&pool)
        .await
        .unwrap();
    let opened = queue_rotated_at(&pool, 1).await;
    let result = outcomes(&app, uri).await;
    assert_eq!(result.covered_since, Some(opened));
    assert_eq!(result.dead_lettered_covered_since, oldest);
    assert!(result.buckets[0].bucket - oldest < TimeDelta::seconds(60));
    // The oldest's own bucket starts before it, so only the other two count.
    assert_eq!(totals(&result), (0, 0, 0, 2));
    assert!(result.buckets.len() >= 49);

    // A dead letter older than the window leaves the whole window covered.
    backdate(job_ids[0], "3 hours").await;
    let result = outcomes(&app, uri).await;
    assert_eq!(result.buckets.len(), 60);
    assert_eq!(result.dead_lettered_covered_since, result.buckets[0].bucket);
    assert_eq!(totals(&result), (0, 0, 0, 2));
    // Per queue as well; another queue's dead letters do not bound it.
    let other = outcomes(&app, &format!("{uri}&queue=other_q")).await;
    assert_eq!(other.dead_lettered_covered_since, other.buckets[0].bucket);
    assert_eq!(totals(&other), (0, 0, 0, 0));
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
