//! Unique-claim and lane lock order between fresh enqueues and re-enqueues.
//!
//! A fresh enqueue claims its job's unique key and then reserves the job's
//! lane, holding both until it commits. A re-enqueue of a job with the same
//! key into the same lane must take them in the same order, or each waits for
//! the other and PostgreSQL aborts one with a deadlock (40P01).
//!
//! Each test drives the interleaving deterministically. A holder transaction
//! takes a fresh enqueue's first lock, the claim of the key, with the insert
//! `awa.insert_job_compat` makes. The re-enqueue starts and is observed
//! waiting on the holder through `pg_blocking_pids`. Only then does the holder
//! take its second lock, the lane reservation, under a `lock_timeout` longer
//! than `deadlock_timeout`, and commit.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test unique_claim_lane_lock_order_test -- --nocapture`

mod ci_timing;

use awa::model::queue_storage::PromotionLane;
use awa::model::{admin, dlq, BulkCursor, ListDlqFilter};
use awa::{insert_many, insert_many_copy_from_pool, UniqueOpts};
use awa::{AwaError, InsertOpts, InsertParams, JobState, QueueStorage, QueueStorageConfig};
use awa_testing::setup;
use sqlx::PgPool;
use std::future::Future;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

static GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

const PRIORITY: i16 = 2;

async fn queue_storage_pool() -> PgPool {
    let pool = setup::setup(10).await;
    setup::activate_queue_storage(&pool).await;
    pool
}

fn unique_queue(prefix: &str) -> String {
    format!("{prefix}_{}", &Uuid::new_v4().simple().to_string()[..12])
}

/// Provision the lane in committed state, so the transactions under test
/// contend on the reservation lock and the claim alone.
async fn provision_lane(pool: &PgPool, queue: &str) {
    QueueStorage::new(QueueStorageConfig::default())
        .expect("queue storage")
        .enqueue_batch(pool, queue, PRIORITY, 1)
        .await
        .expect("provision lane");
}

/// A fresh enqueue paused between its claim and its lane reservation.
struct FreshEnqueue {
    tx: sqlx::Transaction<'static, sqlx::Postgres>,
    pid: i32,
}

impl FreshEnqueue {
    async fn claim(pool: &PgPool, key: &[u8]) -> Self {
        let mut tx = pool.begin().await.expect("begin fresh enqueue");
        let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *tx)
            .await
            .expect("backend pid");
        sqlx::query(
            "INSERT INTO awa.job_unique_claims (unique_key, job_id) \
             VALUES ($1, nextval('awa.job_id_seq'))",
        )
        .bind(key)
        .execute(&mut *tx)
        .await
        .expect("claim the key");
        Self { tx, pid }
    }

    async fn wait_for_waiter(&self, pool: &PgPool) {
        let deadline = Instant::now() + ci_timing::scaled_timeout(Duration::from_secs(10));
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE $1 = ANY(pg_blocking_pids(pid))
                 )",
            )
            .bind(self.pid)
            .fetch_one(pool)
            .await
            .expect("read lock waits");
            if waiting {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the re-enqueue never waited on the fresh enqueue's claim"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn reserve_and_commit(mut self, queue: &str) -> Result<(), sqlx::Error> {
        sqlx::query("SET LOCAL lock_timeout = '5s'")
            .execute(&mut *self.tx)
            .await?;
        sqlx::query("SELECT awa.reserve_enqueue_seq($1, $2, 0::smallint, 1)")
            .bind(queue)
            .bind(PRIORITY)
            .execute(&mut *self.tx)
            .await?;
        self.tx.commit().await
    }
}

/// Run `reenqueue` against a fresh enqueue of `key` into `queue`'s lane and
/// return its result once both have finished.
async fn race_fresh_enqueue<T, F>(pool: &PgPool, queue: &str, key: &[u8], reenqueue: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let fresh = FreshEnqueue::claim(pool, key).await;
    let reenqueue = tokio::spawn(reenqueue);
    fresh.wait_for_waiter(pool).await;
    fresh
        .reserve_and_commit(queue)
        .await
        .expect("the fresh enqueue reserves the lane and commits");
    tokio::time::timeout(
        ci_timing::scaled_timeout(Duration::from_secs(10)),
        reenqueue,
    )
    .await
    .expect("the re-enqueue finishes")
    .expect("join re-enqueue")
}

fn assert_unique_conflict<T: std::fmt::Debug>(result: Result<T, AwaError>, operation: &str) {
    assert!(
        matches!(result, Err(AwaError::UniqueConflict { .. })),
        "{operation} should lose the key to the fresh enqueue, got {result:?}"
    );
}

async fn drop_claim(pool: &PgPool, key: &[u8]) {
    sqlx::query("DELETE FROM awa.job_unique_claims WHERE unique_key = $1")
        .bind(key)
        .execute(pool)
        .await
        .expect("drop claim");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dlq_batch_retry_and_fresh_enqueue_of_one_key_do_not_deadlock() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_lane_dlq_batch");
    let key = format!("claim-lane-{queue}").into_bytes();
    provision_lane(&pool, &queue).await;
    let keyed: i64 = sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries (
             job_id, kind, queue, priority, dlq_reason, original_run_lease,
             unique_key, unique_states
         )
         VALUES (nextval('awa.job_id_seq'), 'claim_lane_job', $1, $2, 'seeded', 1, $3, '01000000')
         RETURNING job_id",
    )
    .bind(&queue)
    .bind(PRIORITY)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("seed dlq row");

    let filter = ListDlqFilter {
        queue: Some(queue.clone()),
        ..Default::default()
    };
    let result = race_fresh_enqueue(&pool, &queue, &key, {
        let pool = pool.clone();
        async move {
            dlq::bulk_retry_from_dlq_batch(&pool, &filter, false, 10, BulkCursor::default()).await
        }
    })
    .await
    .expect("the batch skips the row instead of failing");
    assert_eq!(
        (result.count, result.skipped, result.conflicts.clone()),
        (0, 1, vec![keyed])
    );
    let still_dead: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM awa.dlq_entries WHERE job_id = $1)")
            .bind(keyed)
            .fetch_one(&pool)
            .await
            .expect("dlq row");
    assert!(still_dead, "the conflicting row stays in the DLQ");

    drop_claim(&pool, &key).await;
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_retry_and_fresh_enqueue_of_one_key_do_not_deadlock() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_lane_admin_retry");
    provision_lane(&pool, &queue).await;
    let job = insert_many(
        &pool,
        &[InsertParams {
            kind: "claim_lane_job".to_string(),
            args: serde_json::json!({ "queue": queue }),
            opts: InsertOpts {
                queue: queue.clone(),
                priority: PRIORITY,
                unique: Some(UniqueOpts {
                    by_queue: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        }],
    )
    .await
    .expect("insert unique job")
    .pop()
    .expect("inserted job");
    let key = job.unique_key.clone().expect("unique key");
    admin::cancel(&pool, job.id)
        .await
        .expect("cancel")
        .expect("cancelled job");

    let result = race_fresh_enqueue(&pool, &queue, &key, {
        let pool = pool.clone();
        async move { admin::retry(&pool, job.id).await }
    })
    .await;
    assert_unique_conflict(result, "admin::retry");
    let state: JobState = sqlx::query_scalar("SELECT state FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .expect("job state");
    assert_eq!(state, JobState::Cancelled);

    drop_claim(&pool, &key).await;
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_and_fresh_enqueue_of_one_key_do_not_deadlock() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_lane_promotion");
    let key = format!("claim-lane-{queue}").into_bytes();
    provision_lane(&pool, &queue).await;
    let scheduled: i64 = sqlx::query_scalar(
        "INSERT INTO awa.deferred_jobs (
             job_id, kind, queue, args, state, priority, attempt, run_lease,
             max_attempts, run_at, created_at, unique_key, unique_states, payload
         )
         VALUES (
             nextval('awa.job_id_seq'), 'claim_lane_job', $1, '{}', 'scheduled', $2, 0, 0,
             25, now() - interval '1 minute', now(), $3, '01000000', '{}'
         )
         RETURNING job_id",
    )
    .bind(&queue)
    .bind(PRIORITY)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("seed scheduled job");

    let lane = PromotionLane {
        queue: queue.clone(),
        priority: PRIORITY,
    };
    let result = race_fresh_enqueue(&pool, &queue, &key, {
        let pool = pool.clone();
        async move {
            QueueStorage::new(QueueStorageConfig::default())
                .expect("queue storage")
                .promote_due_lane(&pool, JobState::Scheduled, &lane, 10, None)
                .await
        }
    })
    .await;
    assert_unique_conflict(result, "promotion");
    let still_scheduled: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM awa.deferred_jobs WHERE job_id = $1)")
            .bind(scheduled)
            .fetch_one(&pool)
            .await
            .expect("deferred row");
    assert!(still_scheduled, "the job stays scheduled");

    drop_claim(&pool, &key).await;
    setup::clean_queue(&pool, &queue).await;
}

fn unique_insert(queue: &str, n: i64) -> InsertParams {
    InsertParams {
        kind: "claim_lane_job".to_string(),
        args: serde_json::json!({ "queue": queue, "n": n }),
        opts: InsertOpts {
            queue: queue.to_string(),
            priority: PRIORITY,
            unique: Some(UniqueOpts::default()),
            ..Default::default()
        },
    }
}

fn unique_key_of(params: &InsertParams) -> Vec<u8> {
    awa::model::prepare_raw_job_insert(
        params.kind.clone(),
        params.args.clone(),
        params.opts.clone(),
    )
    .expect("prepare insert")
    .unique_key()
    .expect("unique key")
    .to_vec()
}

/// `insert_many` reserves its rows' lane before it claims the second row's
/// key unless it takes every key first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_many_and_fresh_enqueue_of_one_key_do_not_deadlock() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_lane_insert_many");
    provision_lane(&pool, &queue).await;
    let rows = vec![unique_insert(&queue, 1), unique_insert(&queue, 2)];
    let first_key = unique_key_of(&rows[0]);
    let key = unique_key_of(&rows[1]);

    let result = race_fresh_enqueue(&pool, &queue, &key, {
        let pool = pool.clone();
        async move { insert_many(&pool, &rows).await }
    })
    .await;
    match &result {
        Err(AwaError::Database(sqlx::Error::Database(err))) => assert_eq!(
            err.code().as_deref(),
            Some("23505"),
            "insert_many should lose the key to the fresh enqueue, got {err:?}"
        ),
        other => panic!("insert_many should lose the key to the fresh enqueue, got {other:?}"),
    }
    let inserted: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND kind = 'claim_lane_job'",
    )
    .bind(&queue)
    .fetch_one(&pool)
    .await
    .expect("count jobs");
    assert_eq!(inserted, 0, "the failed insert_many leaves no job");
    let first_claimed: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM awa.job_unique_claims WHERE unique_key = $1)",
    )
    .bind(&first_key)
    .fetch_one(&pool)
    .await
    .expect("first claim");
    assert!(!first_claimed, "the failed insert_many leaves no claim");

    drop_claim(&pool, &key).await;
    setup::clean_queue(&pool, &queue).await;
}

/// Without a conflict, `insert_many` inserts every unique row, holding each
/// row's own claim and no placeholder.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_many_claims_each_unique_row_once() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_lane_insert_many_ok");
    let rows: Vec<InsertParams> = (0..4).map(|n| unique_insert(&queue, n)).collect();
    let inserted = insert_many(&pool, &rows).await.expect("insert_many");
    assert_eq!(inserted.len(), rows.len());
    for (row, job) in rows.iter().zip(&inserted) {
        let key = unique_key_of(row);
        assert_eq!(job.unique_key.as_deref(), Some(key.as_slice()));
        let holders: Vec<i64> =
            sqlx::query_scalar("SELECT job_id FROM awa.job_unique_claims WHERE unique_key = $1")
                .bind(&key)
                .fetch_all(&pool)
                .await
                .expect("claim holders");
        assert_eq!(holders, vec![job.id]);
    }
    let duplicate = insert_many(&pool, &[unique_insert(&queue, 9), unique_insert(&queue, 0)]).await;
    assert!(
        matches!(&duplicate, Err(AwaError::Database(sqlx::Error::Database(err))) if err.code().as_deref() == Some("23505")),
        "{duplicate:?}"
    );
    let placeholders: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.job_unique_claims WHERE job_id = 0")
            .fetch_one(&pool)
            .await
            .expect("placeholder claims");
    assert_eq!(placeholders, 0);

    for row in &rows {
        drop_claim(&pool, &unique_key_of(row)).await;
    }
    setup::clean_queue(&pool, &queue).await;
}

const BATCH_FUNCTION: &str = "awa.insert_jobs_compat_batch(text[],text[],jsonb[],text[],smallint[],smallint[],timestamptz[],jsonb[],jsonb[],bytea[],text[],text[],boolean)";

/// `insert_many_copy` on a schema without the batch function inserts a row
/// at a time, and takes every key before the first row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn row_by_row_copy_and_fresh_enqueue_of_one_key_do_not_deadlock() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_lane_copy_rows");
    provision_lane(&pool, &queue).await;
    let rows = vec![unique_insert(&queue, 1), unique_insert(&queue, 2)];
    let key = unique_key_of(&rows[1]);
    let hidden = BATCH_FUNCTION.replacen(
        "insert_jobs_compat_batch",
        "insert_jobs_compat_batch_hidden",
        1,
    );
    sqlx::query(&format!("DROP FUNCTION IF EXISTS {hidden}"))
        .execute(&pool)
        .await
        .expect("drop a batch function an earlier run left hidden");

    sqlx::query(&format!(
        "ALTER FUNCTION {BATCH_FUNCTION} RENAME TO insert_jobs_compat_batch_hidden"
    ))
    .execute(&pool)
    .await
    .expect("hide the batch function");
    let result = race_fresh_enqueue(&pool, &queue, &key, {
        let pool = pool.clone();
        let rows = rows.clone();
        async move { insert_many_copy_from_pool(&pool, &rows).await }
    })
    .await;
    sqlx::query(&format!(
        "ALTER FUNCTION {hidden} RENAME TO insert_jobs_compat_batch"
    ))
    .execute(&pool)
    .await
    .expect("restore the batch function");

    let inserted = result.expect("the copy skips the row whose key the fresh enqueue took");
    assert_eq!(inserted.len(), 1, "{inserted:?}");
    assert_eq!(
        inserted[0].unique_key.as_deref(),
        Some(unique_key_of(&rows[0]).as_slice())
    );

    drop_claim(&pool, &unique_key_of(&rows[0])).await;
    drop_claim(&pool, &key).await;
    setup::clean_queue(&pool, &queue).await;
}
