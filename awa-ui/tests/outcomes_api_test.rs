//! `GET /api/stats/outcomes` on both engines: bucket boundaries, the queue
//! and kind filters, `covered_since` after queue-ring prune, the dead-letter
//! series' own coverage, and parameter validation.

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
    covered_since: DateTime<Utc>,
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
        .min(outcomes.dead_lettered_covered_since);
    if let Some(bucket) = outcomes.buckets.first() {
        assert!(bucket.bucket >= first, "{uri}: bucket before coverage");
        assert!(
            bucket.bucket - first < TimeDelta::seconds(3600),
            "{uri}: covered buckets left out"
        );
    }
    for bucket in &outcomes.buckets {
        let terminal = bucket.bucket >= outcomes.covered_since;
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

/// Finalise canonical job `id` in `state` at `at`.
async fn finalize_canonical(pool: &PgPool, id: i64, state: &str, at: DateTime<Utc>) {
    sqlx::query(
        "UPDATE awa.jobs_hot SET state = $2::awa.job_state, finalized_at = $3, attempt = 1 \
         WHERE id = $1",
    )
    .bind(id)
    .bind(state)
    .bind(at)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn canonical_buckets_split_at_their_boundaries() {
    let db = TestDatabase::canonical().await;
    let pool = db.pool().clone();
    let app = awa_ui::router(pool.clone(), Duration::ZERO).await.unwrap();
    let uri = "/api/stats/outcomes?minutes=30&bucket=300s";
    let ids = insert(&pool, "outcome_job", "outcome_q", 6).await;

    // A bucket boundary can pass between the reads; retry from the start
    // when the window moved.
    for _ in 0..3 {
        let before = outcomes(&app, uri).await;
        assert_eq!(before.buckets.len(), 6);
        assert_eq!(before.covered_since, before.buckets[0].bucket);
        let start = |n: usize| before.buckets[n].bucket;
        let step = TimeDelta::seconds(300);
        for pair in before.buckets.windows(2) {
            assert_eq!(pair[1].bucket - pair[0].bucket, step);
        }
        let micro = TimeDelta::microseconds(1);
        finalize_canonical(&pool, ids[0], "completed", start(0)).await;
        finalize_canonical(&pool, ids[1], "completed", start(1) - micro).await;
        finalize_canonical(&pool, ids[2], "completed", start(0) - micro).await;
        finalize_canonical(&pool, ids[3], "failed", start(5)).await;
        finalize_canonical(&pool, ids[4], "cancelled", start(2) + TimeDelta::seconds(1)).await;
        finalize_canonical(&pool, ids[5], "failed", start(2)).await;

        let after = outcomes(&app, uri).await;
        if after.buckets[0].bucket != before.buckets[0].bucket {
            continue;
        }
        let counts: Vec<_> = after.buckets.iter().map(Bucket::counts).collect();
        assert_eq!(
            counts,
            vec![
                (2, 0, 0, 0),
                (0, 0, 0, 0),
                (0, 1, 1, 0),
                (0, 0, 0, 0),
                (0, 0, 0, 0),
                (0, 1, 0, 0),
            ]
        );
        assert_eq!(after.covered_since, after.buckets[0].bucket);
        return;
    }
    panic!("the window moved on every attempt");
}

#[tokio::test]
async fn canonical_counts_follow_the_queue_and_kind_filters() {
    let db = TestDatabase::canonical().await;
    let pool = db.pool().clone();
    let app = awa_ui::router(pool.clone(), Duration::ZERO).await.unwrap();
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT now()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let recent = now - TimeDelta::seconds(5);

    let a_send = insert(&pool, "send", "out_a", 4).await;
    let a_sync = insert(&pool, "sync", "out_a", 2).await;
    let b_send = insert(&pool, "send", "out_b", 3).await;
    for (n, id) in a_send.iter().enumerate() {
        let state = if n < 3 { "completed" } else { "failed" };
        finalize_canonical(&pool, *id, state, recent).await;
    }
    for id in &a_sync {
        finalize_canonical(&pool, *id, "cancelled", recent).await;
    }
    for (n, id) in b_send.iter().enumerate() {
        let state = if n == 0 { "completed" } else { "failed" };
        finalize_canonical(&pool, *id, state, recent).await;
    }
    // Outside a one-hour window.
    let old = insert(&pool, "send", "out_a", 1).await;
    finalize_canonical(&pool, old[0], "failed", now - TimeDelta::hours(2)).await;

    for (query, expected) in [
        ("", (4, 3, 2, 0)),
        ("&queue=out_a", (3, 1, 2, 0)),
        ("&queue=out_b", (1, 2, 0, 0)),
        ("&kind=send", (4, 3, 0, 0)),
        ("&kind=sync", (0, 0, 2, 0)),
        ("&queue=out_a&kind=send", (3, 1, 0, 0)),
        ("&queue=out_b&kind=sync", (0, 0, 0, 0)),
        ("&queue=missing", (0, 0, 0, 0)),
    ] {
        let uri = format!("/api/stats/outcomes?minutes=60&bucket=60s{query}");
        let result = outcomes(&app, &uri).await;
        assert_eq!(result.buckets.len(), 60, "{uri}");
        assert_eq!(totals(&result), expected, "{uri}");
    }
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
        assert_eq!(result.covered_since, result.buckets[0].bucket, "{uri}");
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
    assert_eq!(fresh.covered_since, fresh.buckets[0].bucket);
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
    assert_eq!(pruned.covered_since, opened);
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
    assert_eq!(after.covered_since, opened);
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
    assert_eq!(result.covered_since, opened);
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

/// The canonical read finds completed rows along the window and failed or
/// cancelled ones along their state, never by scanning retained history.
#[tokio::test]
async fn canonical_outcomes_read_along_indexes() {
    let db = TestDatabase::canonical().await;
    sqlx::raw_sql(
        "INSERT INTO awa.jobs_hot (kind, queue, state, attempt, run_at, created_at, finalized_at) \
         SELECT 'k' || (g % 5), 'q' || (g % 8), 'completed', 1, at, at, at \
         FROM generate_series(1, 60000) AS g, \
              LATERAL (SELECT now() - (g % 86400) * interval '1 second' AS at) AS t; \
         INSERT INTO awa.jobs_hot (kind, queue, state, attempt, run_at, created_at, finalized_at) \
         SELECT 'k' || (g % 5), 'q' || (g % 8), \
                (CASE WHEN g % 4 = 0 THEN 'cancelled' ELSE 'failed' END)::awa.job_state, 1, at, at, at \
         FROM generate_series(1, 3000) AS g, \
              LATERAL (SELECT now() - (g * 80) * interval '1 second' AS at) AS t; \
         INSERT INTO awa.jobs_hot (kind, queue, state, run_at) \
         SELECT 'k' || (g % 5), 'q' || (g % 8), 'available', now() \
         FROM generate_series(1, 20000) AS g",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("VACUUM (ANALYZE) awa.jobs_hot")
        .execute(db.pool())
        .await
        .unwrap();
    let in_window: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs_hot \
         WHERE state = 'completed' AND finalized_at >= now() - interval '2 hours 1 minute'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::raw_sql(
                    "SET max_parallel_workers_per_gather = 0; \
                     SET stats_fetch_consistency = 'none'",
                )
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
    let result = outcomes(&app, "/api/stats/outcomes?minutes=120&bucket=60s").await;
    let (scans_after, tuples_after) = jobs_hot_reads(&pool).await;
    pool.close().await;

    assert_eq!(result.buckets.len(), 120);
    assert!(totals(&result).0 > 0);
    assert_eq!(scans_after - scans_before, 0, "awa.jobs_hot was scanned");
    // At most the window's completions and every retained failure or
    // cancellation, with room for the plan's own probes.
    let budget = in_window + 3000 + 500;
    let read = tuples_after - tuples_before;
    assert!(read <= budget, "read {read} tuples, budget {budget}");
}
