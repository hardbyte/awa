//! Bounded bulk DLQ retry: `dlq::bulk_retry_from_dlq_batch`.
//!
//! Each call changes at most `batch_size` rows in one transaction, walks the
//! DLQ in job ID order, skips and reports unique conflicts, and reports
//! `busy` rather than passing a row another transaction holds.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test dlq_retry_batch_test -- --nocapture`

use awa::model::bulk::{run_batches, BulkBatch, BulkCursor, BulkRunOptions};
use awa::model::{dlq, AwaError, ListDlqFilter, RetryFromDlqOpts};
use awa_testing::setup;
use sqlx::PgPool;
use std::collections::BTreeSet;
use std::sync::LazyLock;
use tokio::sync::Mutex;
use uuid::Uuid;

static GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn queue_storage_pool() -> PgPool {
    let pool = setup::setup(10).await;
    setup::activate_queue_storage(&pool).await;
    pool
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", &Uuid::new_v4().simple().to_string()[..12])
}

struct Seed<'a> {
    kind: &'a str,
    queue: &'a str,
    dlq_age_secs: i64,
    tag: Option<&'a str>,
    unique_key: Option<&'a [u8]>,
}

async fn seed(pool: &PgPool, row: Seed<'_>) -> i64 {
    let payload = match row.tag {
        Some(tag) => serde_json::json!({ "tags": [tag] }),
        None => serde_json::json!({}),
    };
    sqlx::query_scalar(
        r#"
        INSERT INTO awa.dlq_entries (
            job_id, kind, queue, priority, dlq_reason, original_run_lease,
            dlq_at, payload, unique_key, unique_states
        )
        VALUES (
            nextval('awa.job_id_seq'), $1, $2, 2, 'seeded', 1,
            now() - make_interval(secs => $3), $4, $5,
            CASE WHEN $5::bytea IS NULL THEN NULL ELSE '11000000' END
        )
        RETURNING job_id
        "#,
    )
    .bind(row.kind)
    .bind(row.queue)
    .bind(row.dlq_age_secs as f64)
    .bind(payload)
    .bind(row.unique_key)
    .fetch_one(pool)
    .await
    .expect("seed dlq row")
}

async fn seed_plain(pool: &PgPool, kind: &str, queue: &str, dlq_age_secs: i64) -> i64 {
    seed(
        pool,
        Seed {
            kind,
            queue,
            dlq_age_secs,
            tag: None,
            unique_key: None,
        },
    )
    .await
}

async fn in_dlq(pool: &PgPool, ids: &[i64]) -> BTreeSet<i64> {
    sqlx::query_scalar::<_, i64>("SELECT job_id FROM awa.dlq_entries WHERE job_id = ANY($1)")
        .bind(ids)
        .fetch_all(pool)
        .await
        .expect("dlq rows")
        .into_iter()
        .collect()
}

async fn ready(pool: &PgPool, ids: &[i64]) -> Vec<i64> {
    sqlx::query_scalar("SELECT job_id FROM awa.ready_entries WHERE job_id = ANY($1)")
        .bind(ids)
        .fetch_all(pool)
        .await
        .expect("ready rows")
}

async fn batch(
    pool: &PgPool,
    filter: &ListDlqFilter,
    batch_size: i64,
    cursor: BulkCursor,
) -> BulkBatch {
    dlq::bulk_retry_from_dlq_batch(pool, filter, false, batch_size, cursor)
        .await
        .expect("bulk retry batch")
}

async fn hold(pool: &PgPool, ids: &[i64]) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut holder = pool.begin().await.expect("begin holder");
    sqlx::query("SELECT job_id FROM awa.dlq_entries WHERE job_id = ANY($1) FOR UPDATE")
        .bind(ids)
        .fetch_all(&mut *holder)
        .await
        .expect("hold rows");
    holder
}

fn by_queue(queue: &str) -> ListDlqFilter {
    ListDlqFilter {
        queue: Some(queue.to_string()),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_batch_takes_at_most_batch_size_rows_in_job_id_order() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("dlq_batch_bound");
    let mut ids = Vec::new();
    for age in [10, 50, 30, 40, 20] {
        ids.push(seed_plain(&pool, "dlq_batch_job", &queue, age).await);
    }
    let filter = ListDlqFilter {
        limit: Some(1),
        ..by_queue(&queue)
    };

    let first = batch(&pool, &filter, 2, BulkCursor::default()).await;
    assert_eq!((first.count, first.done, first.busy), (2, false, false));
    assert_eq!(first.cursor.after_job_id, ids[1]);
    let mut revived = ready(&pool, &ids).await;
    revived.sort_unstable();
    assert_eq!(revived, vec![ids[0], ids[1]]);
    assert_eq!(in_dlq(&pool, &ids).await.len(), 3);

    let second = batch(&pool, &filter, 10, first.cursor).await;
    assert_eq!((second.count, second.done), (3, true));
    assert!(in_dlq(&pool, &ids).await.is_empty());
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn kind_and_tag_filters_limit_what_a_batch_revives() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("dlq_batch_filters");
    let kind_a = unique_name("dlq_batch_a");
    let kind_b = unique_name("dlq_batch_b");
    let tag = unique_name("dlq_batch_tag");
    let a = seed_plain(&pool, &kind_a, &queue, 30).await;
    let b = seed_plain(&pool, &kind_b, &queue, 20).await;
    let tagged = seed(
        &pool,
        Seed {
            kind: &kind_b,
            queue: &queue,
            dlq_age_secs: 10,
            tag: Some(&tag),
            unique_key: None,
        },
    )
    .await;
    let all = [a, b, tagged];

    let by_kind = ListDlqFilter {
        kind: Some(kind_a.clone()),
        ..by_queue(&queue)
    };
    assert_eq!(
        batch(&pool, &by_kind, 10, BulkCursor::default())
            .await
            .count,
        1
    );
    assert_eq!(ready(&pool, &all).await, vec![a]);

    let by_tag = ListDlqFilter {
        tag: Some(tag.clone()),
        ..Default::default()
    };
    assert_eq!(
        batch(&pool, &by_tag, 10, BulkCursor::default()).await.count,
        1
    );
    assert_eq!(in_dlq(&pool, &all).await, BTreeSet::from([b]));
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_runs_revive_each_row_exactly_once() {
    const ROWS: i64 = 300;
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("dlq_batch_concurrent");
    let ids: Vec<i64> = sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries (job_id, kind, queue, priority, dlq_reason, original_run_lease) \
         SELECT nextval('awa.job_id_seq'), 'dlq_batch_job', $1, 2, 'seeded', 1 \
         FROM generate_series(1, $2) RETURNING job_id",
    )
    .bind(&queue)
    .bind(ROWS)
    .fetch_all(&pool)
    .await
    .expect("seed dlq rows");

    let mut runs = Vec::new();
    for _ in 0..4 {
        let pool = pool.clone();
        let filter = by_queue(&queue);
        runs.push(tokio::spawn(async move {
            run_batches(
                BulkRunOptions::default().batch_size(7),
                |size, cursor| dlq::bulk_retry_from_dlq_batch(&pool, &filter, false, size, cursor),
                |batch, _| assert!(batch.count <= 7),
            )
            .await
            .expect("run")
        }));
    }
    let mut total = 0;
    for run in runs {
        let run = run.await.expect("run task");
        assert!(run.complete, "{run:?}");
        total += run.count;
    }

    assert_eq!(total, ROWS as u64);
    let revived = ready(&pool, &ids).await;
    assert_eq!(revived.len(), ROWS as usize);
    assert_eq!(
        revived.iter().collect::<BTreeSet<_>>().len(),
        ROWS as usize,
        "a row was revived twice"
    );
    assert!(in_dlq(&pool, &ids).await.is_empty());
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn a_unique_conflict_is_skipped_reported_and_not_retried_again() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("dlq_batch_conflict");
    let key = format!("dlq-batch-{queue}").into_bytes();
    let first = seed_plain(&pool, "dlq_batch_job", &queue, 30).await;
    let keyed = seed(
        &pool,
        Seed {
            kind: "dlq_batch_job",
            queue: &queue,
            dlq_age_secs: 20,
            tag: None,
            unique_key: Some(&key),
        },
    )
    .await;
    let last = seed_plain(&pool, "dlq_batch_job", &queue, 10).await;
    sqlx::query("INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, $2)")
        .bind(&key)
        .bind(i64::MAX - 2)
        .execute(&pool)
        .await
        .expect("live job holds the key");

    let result = batch(&pool, &by_queue(&queue), 10, BulkCursor::default()).await;
    assert_eq!(
        (
            result.count,
            result.skipped,
            result.conflicts.clone(),
            result.done
        ),
        (2, 1, vec![keyed], true)
    );
    assert_eq!(
        in_dlq(&pool, &[first, keyed, last]).await,
        BTreeSet::from([keyed])
    );
    let holder: i64 =
        sqlx::query_scalar("SELECT job_id FROM awa.job_unique_claims WHERE unique_key = $1")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .expect("claim");
    assert_eq!(holder, i64::MAX - 2, "the live job keeps its key");

    let again = batch(&pool, &by_queue(&queue), 10, result.cursor).await;
    assert_eq!((again.count, again.skipped, again.done), (0, 0, true));

    sqlx::query("DELETE FROM awa.job_unique_claims WHERE unique_key = $1")
        .bind(&key)
        .execute(&pool)
        .await
        .expect("drop claim");
    setup::clean_queue(&pool, &queue).await;
}

async fn next_lane_seq(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT awa.sequence_next_value(seq_name) FROM awa.queue_enqueue_heads \
         WHERE queue = $1 AND priority = 2 AND enqueue_shard = 0",
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("next lane sequence")
}

async fn backend_pid(tx: &mut sqlx::Transaction<'static, sqlx::Postgres>) -> i32 {
    sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut **tx)
        .await
        .expect("backend pid")
}

/// Wait until `count` backends wait on `pid`, directly or through another
/// waiter.
async fn wait_for_waiters(pool: &PgPool, pid: i32, count: i64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "WITH RECURSIVE waiters(pid) AS (
                 SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))
                 UNION
                 SELECT activity.pid FROM pg_stat_activity AS activity
                 JOIN waiters ON waiters.pid = ANY(pg_blocking_pids(activity.pid))
             )
             SELECT count(*) FROM waiters",
        )
        .bind(pid)
        .fetch_one(pool)
        .await
        .expect("read lock waits");
        if waiting >= count {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{count} backends should wait on {pid}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// A row whose key a live job holds is left out before the lane is reserved,
/// so the lane advances only by the rows the batch revives.
#[tokio::test]
async fn a_conflicting_row_gets_no_lane_position() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("dlq_batch_no_gap");
    let key = format!("dlq-batch-{queue}").into_bytes();
    seed_plain(&pool, "dlq_batch_job", &queue, 40).await;
    let warmed = batch(&pool, &by_queue(&queue), 10, BulkCursor::default()).await;
    assert_eq!(warmed.count, 1, "the first batch provisions the lane");
    let keyed = seed(
        &pool,
        Seed {
            kind: "dlq_batch_job",
            queue: &queue,
            dlq_age_secs: 30,
            tag: None,
            unique_key: Some(&key),
        },
    )
    .await;
    let plain = seed_plain(&pool, "dlq_batch_job", &queue, 20).await;
    sqlx::query("INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, $2)")
        .bind(&key)
        .bind(i64::MAX - 3)
        .execute(&pool)
        .await
        .expect("live job holds the key");
    let before = next_lane_seq(&pool, &queue).await;

    let result = batch(&pool, &by_queue(&queue), 10, warmed.cursor).await;
    assert_eq!((result.count, result.conflicts.clone()), (1, vec![keyed]));
    let plain_seq: i64 =
        sqlx::query_scalar("SELECT lane_seq FROM awa.ready_entries WHERE job_id = $1")
            .bind(plain)
            .fetch_one(&pool)
            .await
            .expect("revived row");
    assert_eq!(plain_seq, before);
    assert_eq!(
        next_lane_seq(&pool, &queue).await,
        before + 1,
        "no lane gap"
    );

    sqlx::query("DELETE FROM awa.job_unique_claims WHERE unique_key = $1")
        .bind(&key)
        .execute(&pool)
        .await
        .expect("drop claim");
    setup::clean_queue(&pool, &queue).await;
}

/// Batches claim their keys in key order whatever order their rows come in,
/// so two batches over the same keys queue behind each other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batches_over_the_same_keys_do_not_deadlock() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("dlq_batch_key_order");
    let low = format!("dlq-batch-a-{queue}").into_bytes();
    let high = format!("dlq-batch-b-{queue}").into_bytes();
    seed_plain(&pool, "dlq_batch_job", &queue, 60).await;
    let warmed = batch(&pool, &by_queue(&queue), 10, BulkCursor::default()).await;
    assert_eq!(warmed.count, 1, "the first batch provisions the lane");
    let mut first = Vec::new();
    for key in [&low, &high] {
        first.push(
            seed(
                &pool,
                Seed {
                    kind: "dlq_batch_first",
                    queue: &queue,
                    dlq_age_secs: 50,
                    tag: None,
                    unique_key: Some(key),
                },
            )
            .await,
        );
    }
    let mut second = Vec::new();
    for key in [&high, &low] {
        second.push(
            seed(
                &pool,
                Seed {
                    kind: "dlq_batch_second",
                    queue: &queue,
                    dlq_age_secs: 40,
                    tag: None,
                    unique_key: Some(key),
                },
            )
            .await,
        );
    }

    let mut gate = pool.begin().await.expect("begin gate");
    let gate_pid = backend_pid(&mut gate).await;
    sqlx::query("INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, $2)")
        .bind(&high)
        .bind(i64::MAX - 4)
        .execute(&mut *gate)
        .await
        .expect("gate holds the high key");

    let run = |kind: &'static str| {
        let pool = pool.clone();
        let filter = ListDlqFilter {
            kind: Some(kind.to_string()),
            queue: Some(queue.clone()),
            ..Default::default()
        };
        tokio::spawn(async move {
            dlq::bulk_retry_from_dlq_batch(&pool, &filter, false, 10, BulkCursor::default()).await
        })
    };
    let first_run = run("dlq_batch_first");
    wait_for_waiters(&pool, gate_pid, 1).await;
    let second_run = run("dlq_batch_second");
    wait_for_waiters(&pool, gate_pid, 2).await;
    gate.rollback().await.expect("release gate");

    let first_result = first_run.await.expect("join").expect("first batch");
    let second_result = second_run.await.expect("join").expect("second batch");
    assert_eq!(first_result.count, 2);
    assert_eq!(
        (second_result.count, second_result.conflicts.clone()),
        (0, second.clone())
    );
    assert_eq!(ready(&pool, &first).await.len(), 2);
    assert_eq!(
        in_dlq(&pool, &second).await,
        second.iter().copied().collect()
    );

    sqlx::query("DELETE FROM awa.job_unique_claims WHERE unique_key = ANY($1)")
        .bind(vec![low.clone(), high.clone()])
        .execute(&pool)
        .await
        .expect("drop claims");
    setup::clean_queue(&pool, &queue).await;
}

/// A batch paused on the lane holds its claim, and a single retry of the
/// same key waits on the claim without holding the lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_and_a_single_retry_of_one_key_do_not_deadlock() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("dlq_batch_vs_single");
    let key = format!("dlq-batch-{queue}").into_bytes();
    seed_plain(&pool, "dlq_batch_job", &queue, 60).await;
    let warmed = batch(&pool, &by_queue(&queue), 10, BulkCursor::default()).await;
    assert_eq!(warmed.count, 1, "the first batch provisions the lane");
    let batched = seed(
        &pool,
        Seed {
            kind: "dlq_batch_job",
            queue: &queue,
            dlq_age_secs: 50,
            tag: None,
            unique_key: Some(&key),
        },
    )
    .await;
    let single = seed(
        &pool,
        Seed {
            kind: "dlq_single_job",
            queue: &queue,
            dlq_age_secs: 40,
            tag: None,
            unique_key: Some(&key),
        },
    )
    .await;

    let mut gate = pool.begin().await.expect("begin gate");
    let gate_pid = backend_pid(&mut gate).await;
    sqlx::query("SELECT awa.reserve_enqueue_seq($1, 2::smallint, 0::smallint, 1)")
        .bind(&queue)
        .execute(&mut *gate)
        .await
        .expect("gate holds the lane");

    let batch_run = tokio::spawn({
        let pool = pool.clone();
        let filter = ListDlqFilter {
            kind: Some("dlq_batch_job".to_string()),
            queue: Some(queue.clone()),
            ..Default::default()
        };
        async move { dlq::bulk_retry_from_dlq_batch(&pool, &filter, false, 10, warmed.cursor).await }
    });
    wait_for_waiters(&pool, gate_pid, 1).await;
    let single_run = tokio::spawn({
        let pool = pool.clone();
        async move { dlq::retry_from_dlq(&pool, single, &RetryFromDlqOpts::default()).await }
    });
    wait_for_waiters(&pool, gate_pid, 2).await;
    gate.commit().await.expect("release gate");

    let batch_result = batch_run.await.expect("join").expect("batch");
    let single_result = single_run.await.expect("join");
    assert_eq!(batch_result.count, 1);
    assert!(
        matches!(single_result, Err(AwaError::UniqueConflict { .. })),
        "{single_result:?}"
    );
    assert_eq!(ready(&pool, &[batched]).await, vec![batched]);
    assert_eq!(in_dlq(&pool, &[single]).await, BTreeSet::from([single]));

    sqlx::query("DELETE FROM awa.job_unique_claims WHERE unique_key = $1")
        .bind(&key)
        .execute(&pool)
        .await
        .expect("drop claim");
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn a_batch_is_busy_not_done_while_every_matching_row_is_held() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("dlq_batch_busy");
    let mut ids = Vec::new();
    for age in [20, 10] {
        ids.push(seed_plain(&pool, "dlq_batch_job", &queue, age).await);
    }

    let holder = hold(&pool, &ids).await;
    let held = batch(&pool, &by_queue(&queue), 10, BulkCursor::default()).await;
    assert_eq!((held.count, held.busy, held.done), (0, true, false));
    assert_eq!((held.cursor.stage, held.cursor.after_job_id), (0, 0));
    holder.rollback().await.expect("release rows");

    let released = batch(&pool, &by_queue(&queue), 10, held.cursor).await;
    assert_eq!((released.count, released.done), (2, true));
    assert!(in_dlq(&pool, &ids).await.is_empty());
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn a_held_row_is_never_passed_by_the_cursor() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("dlq_batch_held_first");
    let mut ids = Vec::new();
    for age in [30, 20, 10] {
        ids.push(seed_plain(&pool, "dlq_batch_job", &queue, age).await);
    }

    let holder = hold(&pool, &ids[..1]).await;
    let held = batch(&pool, &by_queue(&queue), 10, BulkCursor::default()).await;
    assert_eq!((held.count, held.busy, held.done), (0, true, false));
    assert_eq!(held.cursor.after_job_id, 0);
    assert_eq!(in_dlq(&pool, &ids).await.len(), 3);
    holder.rollback().await.expect("release row");

    let released = batch(&pool, &by_queue(&queue), 10, held.cursor).await;
    assert_eq!((released.count, released.done), (3, true));
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn batches_need_a_scope_and_a_positive_batch_size() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let unscoped = dlq::bulk_retry_from_dlq_batch(
        &pool,
        &ListDlqFilter::default(),
        false,
        10,
        BulkCursor::default(),
    )
    .await
    .expect_err("unscoped batch");
    assert!(matches!(unscoped, AwaError::Validation(_)), "{unscoped:?}");
    let zero = dlq::bulk_retry_from_dlq_batch(
        &pool,
        &by_queue("dlq_batch_zero"),
        false,
        0,
        BulkCursor::default(),
    )
    .await
    .expect_err("zero batch size");
    assert!(matches!(zero, AwaError::Validation(_)), "{zero:?}");
}
