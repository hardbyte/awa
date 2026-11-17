//! Bounded bulk DLQ retry: `dlq::bulk_retry_from_dlq_batch`.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test dlq_retry_batch_test -- --nocapture`

use awa::model::{dlq, AwaError, DlqRetryBatch, ListDlqFilter};
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

async fn batch(pool: &PgPool, filter: &ListDlqFilter, limit: i64) -> DlqRetryBatch {
    dlq::bulk_retry_from_dlq_batch(pool, filter, false, limit)
        .await
        .expect("bulk retry batch")
}

fn by_queue(queue: &str) -> ListDlqFilter {
    ListDlqFilter {
        queue: Some(queue.to_string()),
        ..Default::default()
    }
}

#[tokio::test]
async fn queue_batches_take_the_oldest_dead_letters_first() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("dlq_batch_queue_order");
    // Ids ascend while dlq_at ages descend, so the two orders disagree.
    let ages = [10, 50, 30, 40, 20];
    let mut ids = Vec::new();
    for age in ages {
        ids.push(seed_plain(&pool, "dlq_batch_job", &queue, age).await);
    }
    let filter = ListDlqFilter {
        limit: Some(1),
        ..by_queue(&queue)
    };

    assert_eq!(
        batch(&pool, &filter, 2).await,
        DlqRetryBatch::Revived { count: 2 }
    );
    let mut revived = ready(&pool, &ids).await;
    revived.sort_unstable();
    assert_eq!(revived, vec![ids[1], ids[3]], "ages 50 and 40 go first");

    assert_eq!(
        batch(&pool, &filter, 10).await,
        DlqRetryBatch::Revived { count: 3 }
    );
    assert!(in_dlq(&pool, &ids).await.is_empty());
    assert_eq!(
        batch(&pool, &filter, 10).await,
        DlqRetryBatch::Revived { count: 0 }
    );
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn batches_without_a_queue_take_the_lowest_job_ids_first() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("dlq_batch_id_order");
    let kind = unique_name("dlq_batch_kind");
    let mut ids = Vec::new();
    for age in [10, 20, 30, 40] {
        ids.push(seed_plain(&pool, &kind, &queue, age).await);
    }
    let filter = ListDlqFilter {
        kind: Some(kind.clone()),
        ..Default::default()
    };

    assert_eq!(
        batch(&pool, &filter, 2).await,
        DlqRetryBatch::Revived { count: 2 }
    );
    let mut revived = ready(&pool, &ids).await;
    revived.sort_unstable();
    assert_eq!(revived, vec![ids[0], ids[1]]);
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
        batch(&pool, &by_kind, 10).await,
        DlqRetryBatch::Revived { count: 1 }
    );
    assert_eq!(ready(&pool, &all).await, vec![a]);

    let by_tag = ListDlqFilter {
        tag: Some(tag.clone()),
        ..Default::default()
    };
    assert_eq!(
        batch(&pool, &by_tag, 10).await,
        DlqRetryBatch::Revived { count: 1 }
    );
    assert_eq!(in_dlq(&pool, &all).await, BTreeSet::from([b]));
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_batches_revive_each_row_exactly_once() {
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

    let mut drains = Vec::new();
    for _ in 0..4 {
        let pool = pool.clone();
        let filter = by_queue(&queue);
        drains.push(tokio::spawn(async move {
            let mut revived = 0;
            loop {
                match batch(&pool, &filter, 7).await {
                    DlqRetryBatch::Revived { count: 0 } => return revived,
                    DlqRetryBatch::Revived { count } => revived += count,
                    DlqRetryBatch::Busy => {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
        }));
    }
    let mut total = 0;
    for drain in drains {
        total += drain.await.expect("drain task");
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
async fn a_unique_conflict_rolls_back_its_batch_and_names_the_row() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("dlq_batch_conflict");
    let key = format!("dlq-batch-{queue}").into_bytes();
    let plain = seed_plain(&pool, "dlq_batch_job", &queue, 30).await;
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
    sqlx::query("INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, $2)")
        .bind(&key)
        .bind(i64::MAX - 2)
        .execute(&pool)
        .await
        .expect("live job holds the key");

    assert_eq!(
        batch(&pool, &by_queue(&queue), 10).await,
        DlqRetryBatch::UniqueConflict { job_id: keyed }
    );
    assert_eq!(in_dlq(&pool, &[plain, keyed]).await.len(), 2);
    assert!(ready(&pool, &[plain, keyed]).await.is_empty());

    sqlx::query("DELETE FROM awa.job_unique_claims WHERE unique_key = $1")
        .bind(&key)
        .execute(&pool)
        .await
        .expect("release the key");
    assert_eq!(
        batch(&pool, &by_queue(&queue), 10).await,
        DlqRetryBatch::Revived { count: 2 }
    );
    let holder: i64 =
        sqlx::query_scalar("SELECT job_id FROM awa.job_unique_claims WHERE unique_key = $1")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .expect("revived row holds its key");
    assert_eq!(holder, keyed);

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

    let mut holder = pool.begin().await.expect("begin holder");
    sqlx::query("SELECT job_id FROM awa.dlq_entries WHERE queue = $1 FOR UPDATE")
        .bind(&queue)
        .fetch_all(&mut *holder)
        .await
        .expect("hold every matching row");
    assert_eq!(
        batch(&pool, &by_queue(&queue), 10).await,
        DlqRetryBatch::Busy
    );
    holder.rollback().await.expect("release rows");

    assert_eq!(
        batch(&pool, &by_queue(&queue), 10).await,
        DlqRetryBatch::Revived { count: 2 }
    );
    assert!(in_dlq(&pool, &ids).await.is_empty());
    assert_eq!(
        batch(&pool, &by_queue(&queue), 10).await,
        DlqRetryBatch::Revived { count: 0 }
    );
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn batches_need_a_scope_and_a_positive_limit() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let unscoped = dlq::bulk_retry_from_dlq_batch(&pool, &ListDlqFilter::default(), false, 10)
        .await
        .expect_err("unscoped batch");
    assert!(matches!(unscoped, AwaError::Validation(_)), "{unscoped:?}");
    let zero = dlq::bulk_retry_from_dlq_batch(&pool, &by_queue("dlq_batch_zero"), false, 0)
        .await
        .expect_err("zero limit");
    assert!(matches!(zero, AwaError::Validation(_)), "{zero:?}");
}
