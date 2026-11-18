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

    async fn claim_and_commit(mut self, key: &[u8]) -> Result<(), sqlx::Error> {
        sqlx::query("SET LOCAL lock_timeout = '5s'")
            .execute(&mut *self.tx)
            .await?;
        sqlx::query(
            "INSERT INTO awa.job_unique_claims (unique_key, job_id) \
             VALUES ($1, nextval('awa.job_id_seq'))",
        )
        .bind(key)
        .execute(&mut *self.tx)
        .await?;
        self.tx.commit().await
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

/// A cancelled job whose unique mask names `cancelled` and `available` reads
/// as holding its key, but another job may hold it, as after a duplicate
/// cancellation. Retrying it must check ownership, not assume it.
#[tokio::test]
async fn retry_of_a_terminal_row_rechecks_key_ownership() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_lane_terminal_retry");
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
                    // scheduled | available | retryable | cancelled
                    states: 0b0101_0011,
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
    let holder = i64::MAX - 7;
    sqlx::query("UPDATE awa.job_unique_claims SET job_id = $2 WHERE unique_key = $1")
        .bind(&key)
        .bind(holder)
        .execute(&pool)
        .await
        .expect("another job holds the key");

    assert_unique_conflict(admin::retry(&pool, job.id).await, "admin::retry");
    let state: JobState = sqlx::query_scalar("SELECT state FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .expect("job state");
    assert_eq!(state, JobState::Cancelled);
    let owner: i64 =
        sqlx::query_scalar("SELECT job_id FROM awa.job_unique_claims WHERE unique_key = $1")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .expect("claim");
    assert_eq!(owner, holder, "the other job keeps its key");

    sqlx::query("UPDATE awa.job_unique_claims SET job_id = $2 WHERE unique_key = $1")
        .bind(&key)
        .bind(job.id)
        .execute(&pool)
        .await
        .expect("the job owns its key again");
    admin::retry(&pool, job.id)
        .await
        .expect("retry of the key's owner succeeds");

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

/// A batch with a ready job and a scheduled unique job claims the scheduled
/// job's key before reserving the ready job's lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_batch_claims_scheduled_keys_before_reserving_lanes() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    for copy in [false, true] {
        let queue = unique_queue("claim_lane_mixed_batch");
        let key =
            awa::model::unique::compute_unique_key("claim_lane_job", Some(&queue), None, None);
        provision_lane(&pool, &queue).await;
        let jobs = vec![
            InsertParams {
                kind: "claim_lane_ready".to_string(),
                args: serde_json::json!({}),
                opts: InsertOpts {
                    queue: queue.clone(),
                    priority: PRIORITY,
                    ..Default::default()
                },
            },
            InsertParams {
                kind: "claim_lane_job".to_string(),
                args: serde_json::json!({}),
                opts: InsertOpts {
                    queue: queue.clone(),
                    priority: PRIORITY,
                    run_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
                    unique: Some(UniqueOpts {
                        by_queue: true,
                        by_args: false,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            },
        ];

        let result = race_fresh_enqueue(&pool, &queue, &key, {
            let pool = pool.clone();
            async move {
                let storage =
                    QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
                if copy {
                    storage.enqueue_params_copy(&pool, &jobs).await
                } else {
                    storage.enqueue_params_batch(&pool, &jobs).await
                }
            }
        })
        .await;
        assert_unique_conflict(
            result,
            if copy {
                "enqueue_params_copy"
            } else {
                "enqueue_params_batch"
            },
        );

        drop_claim(&pool, &key).await;
        setup::clean_queue(&pool, &queue).await;
    }
}

/// Of two DLQ rows with one unique key, the one that owns the key is retried
/// even when the other has the lower job ID.
#[tokio::test]
async fn dlq_batch_retry_keeps_the_key_owner_among_same_key_rows() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_lane_dlq_owner");
    let key = format!("claim-lane-{queue}").into_bytes();
    let mut ids = Vec::new();
    // available only, then failed | available
    for mask in ["01000000", "01000100"] {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO awa.dlq_entries (
                 job_id, kind, queue, priority, dlq_reason, original_run_lease,
                 unique_key, unique_states
             )
             VALUES (nextval('awa.job_id_seq'), 'claim_lane_job', $1, $2, 'seeded', 1, $3, $4::bit(8))
             RETURNING job_id",
        )
        .bind(&queue)
        .bind(PRIORITY)
        .bind(&key)
        .bind(mask)
        .fetch_one(&pool)
        .await
        .expect("seed dlq row");
        ids.push(id);
    }
    let (other, owner) = (ids[0], ids[1]);
    sqlx::query("INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, $2)")
        .bind(&key)
        .bind(owner)
        .execute(&pool)
        .await
        .expect("the failed row owns the key");

    let filter = ListDlqFilter {
        queue: Some(queue.clone()),
        ..Default::default()
    };
    let result = dlq::bulk_retry_from_dlq_batch(&pool, &filter, false, 10, BulkCursor::default())
        .await
        .expect("batch retry");
    assert_eq!(
        (result.count, result.skipped, result.conflicts.clone()),
        (1, 1, vec![other])
    );
    let claim: i64 =
        sqlx::query_scalar("SELECT job_id FROM awa.job_unique_claims WHERE unique_key = $1")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .expect("claim");
    assert_eq!(claim, owner);

    drop_claim(&pool, &key).await;
    setup::clean_queue(&pool, &queue).await;
}

/// A batch with a ready job on key A and a scheduled job on key B, A < B,
/// claims A before B, as every other path does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_batch_claims_ready_and_scheduled_keys_in_one_order() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    for copy in [false, true] {
        let queue = unique_queue("claim_lane_mixed_keys");
        let key_of =
            |kind: &str| awa::model::unique::compute_unique_key(kind, Some(&queue), None, None);
        let (mut ready_kind, mut scheduled_kind) = ("claim_lane_key_one", "claim_lane_key_two");
        if key_of(ready_kind) > key_of(scheduled_kind) {
            std::mem::swap(&mut ready_kind, &mut scheduled_kind);
        }
        let (first, second) = (key_of(ready_kind), key_of(scheduled_kind));
        let unique = || {
            Some(UniqueOpts {
                by_queue: true,
                by_args: false,
                ..Default::default()
            })
        };
        let jobs = vec![
            InsertParams {
                kind: ready_kind.to_string(),
                args: serde_json::json!({}),
                opts: InsertOpts {
                    queue: queue.clone(),
                    priority: PRIORITY,
                    unique: unique(),
                    ..Default::default()
                },
            },
            InsertParams {
                kind: scheduled_kind.to_string(),
                args: serde_json::json!({}),
                opts: InsertOpts {
                    queue: queue.clone(),
                    priority: PRIORITY,
                    run_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
                    unique: unique(),
                    ..Default::default()
                },
            },
        ];

        let holder = FreshEnqueue::claim(&pool, &first).await;
        let batch = tokio::spawn({
            let pool = pool.clone();
            async move {
                let storage =
                    QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
                if copy {
                    storage.enqueue_params_copy(&pool, &jobs).await
                } else {
                    storage.enqueue_params_batch(&pool, &jobs).await
                }
            }
        });
        holder.wait_for_waiter(&pool).await;
        holder
            .claim_and_commit(&second)
            .await
            .expect("the holder claims the second key and commits");
        let result =
            tokio::time::timeout(ci_timing::scaled_timeout(Duration::from_secs(10)), batch)
                .await
                .expect("the batch finishes")
                .expect("join batch");
        assert_unique_conflict(
            result,
            if copy {
                "enqueue_params_copy"
            } else {
                "enqueue_params_batch"
            },
        );

        drop_claim(&pool, &first).await;
        drop_claim(&pool, &second).await;
        setup::clean_queue(&pool, &queue).await;
    }
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

fn plain_insert(queue: &str, n: i64) -> InsertParams {
    let mut params = unique_insert(queue, n);
    params.opts.unique = None;
    params
}

async fn backend_pid(tx: &mut sqlx::Transaction<'static, sqlx::Postgres>) -> i32 {
    sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut **tx)
        .await
        .expect("backend pid")
}

/// Whether any backend waits on `holder`.
async fn waits_on(pool: &PgPool, holder: i32) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pg_stat_activity
             WHERE $1 = ANY(pg_blocking_pids(pid))
         )",
    )
    .bind(holder)
    .fetch_one(pool)
    .await
    .expect("read lock waits")
}

/// The interleaving of review comment 4610275002. A batch holds `lanes[0]`
/// and waits for `lanes[1]`, which a blocker holds, before it reaches its row
/// in `lanes[2]` whose key another job holds. The holder then releases the
/// key, a fresh enqueue claims it and waits for `lanes[0]`, and the blocker
/// lets go. A batch that tries the key now waits for the fresh enqueue while
/// holding the lane it waits for, and PostgreSQL aborts one with 40P01.
///
/// Returns the batch's result, whether it ever waited on the blocker, and
/// the fresh enqueue's result when it ran.
async fn held_key_released_mid_batch<T, F>(
    pool: &PgPool,
    lanes: &[String; 3],
    key: &[u8],
    batch: F,
) -> (T, bool, Option<Result<(), sqlx::Error>>)
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    // The key's committed holder.
    sqlx::query(
        "INSERT INTO awa.job_unique_claims (unique_key, job_id) \
         VALUES ($1, nextval('awa.job_id_seq'))",
    )
    .bind(key)
    .execute(pool)
    .await
    .expect("holder claims the key");

    let mut blocker = pool.begin().await.expect("begin blocker");
    let blocker_pid = backend_pid(&mut blocker).await;
    sqlx::query("SELECT awa.reserve_enqueue_seq($1, $2, 0::smallint, 1)")
        .bind(&lanes[1])
        .bind(PRIORITY)
        .execute(&mut *blocker)
        .await
        .expect("blocker reserves the middle lane");

    let batch = tokio::spawn(batch);
    let deadline = Instant::now() + ci_timing::scaled_timeout(Duration::from_secs(10));
    let mut waited = false;
    while !batch.is_finished() {
        if waits_on(pool, blocker_pid).await {
            waited = true;
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the batch neither finished nor waited"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut fresh_result = None;
    if waited {
        drop_claim(pool, key).await;
        let fresh = FreshEnqueue::claim(pool, key).await;
        let fresh_pid = fresh.pid;
        let first_lane = lanes[0].clone();
        let fresh = tokio::spawn(async move { fresh.reserve_and_commit(&first_lane).await });
        let deadline = Instant::now() + ci_timing::scaled_timeout(Duration::from_secs(10));
        while !fresh.is_finished() && !waits_on_any(pool, fresh_pid).await {
            assert!(
                Instant::now() < deadline,
                "the fresh enqueue neither finished nor waited"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        blocker.rollback().await.expect("blocker lets go");
        fresh_result = Some(
            tokio::time::timeout(ci_timing::scaled_timeout(Duration::from_secs(15)), fresh)
                .await
                .expect("the fresh enqueue finishes")
                .expect("join fresh enqueue"),
        );
    } else {
        blocker.rollback().await.expect("blocker lets go");
    }
    let result = tokio::time::timeout(ci_timing::scaled_timeout(Duration::from_secs(15)), batch)
        .await
        .expect("the batch finishes")
        .expect("join batch");
    (result, waited, fresh_result)
}

async fn waits_on_any(pool: &PgPool, waiter: i32) -> bool {
    sqlx::query_scalar("SELECT cardinality(pg_blocking_pids($1)) > 0")
        .bind(waiter)
        .fetch_one(pool)
        .await
        .expect("read lock waits")
}

fn is_deadlock<T: std::fmt::Debug>(result: &Result<T, AwaError>) -> bool {
    matches!(result, Err(AwaError::Database(sqlx::Error::Database(err))) if err.code().as_deref() == Some("40P01"))
}

fn held_key_lanes() -> [String; 3] {
    let base = unique_queue("claim_lane_held");
    [
        format!("{base}_a"),
        format!("{base}_b"),
        format!("{base}_c"),
    ]
}

/// A key a committed job holds fails a multi-row `insert_many` before it
/// reserves any lane, so the holder releasing the key mid-batch cannot leave
/// the batch waiting for the key while it holds a lane (review comment
/// 4610275002).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_many_fails_on_a_held_key_before_any_lane() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let lanes = held_key_lanes();
    for lane in &lanes {
        provision_lane(&pool, lane).await;
    }
    let rows = vec![
        plain_insert(&lanes[0], 1),
        plain_insert(&lanes[1], 2),
        unique_insert(&lanes[2], 3),
    ];
    let key = unique_key_of(&rows[2]);

    let (result, waited, fresh) = held_key_released_mid_batch(&pool, &lanes, &key, {
        let pool = pool.clone();
        async move { insert_many(&pool, &rows).await }
    })
    .await;
    assert!(!is_deadlock(&result), "insert_many deadlocked: {result:?}");
    assert!(
        !matches!(&fresh, Some(Err(err)) if err.as_database_error().and_then(|err| err.code()).as_deref() == Some("40P01")),
        "the fresh enqueue deadlocked: {fresh:?}"
    );
    assert!(
        matches!(&result, Err(AwaError::Database(sqlx::Error::Database(err))) if err.code().as_deref() == Some("23505") && err.constraint() == Some("idx_awa_jobs_unique")),
        "insert_many fails on the held key: {result:?}"
    );
    assert!(!waited, "insert_many fails before it reserves a lane");

    drop_claim(&pool, &key).await;
    for lane in &lanes {
        setup::clean_queue(&pool, lane).await;
    }
}

/// The row-by-row `insert_many_copy` fallback skips a row whose key a
/// committed job holds when it takes its keys, instead of trying it after it
/// has reserved lanes (review comment 4610275002).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn row_by_row_copy_skips_a_held_key_without_waiting_for_it() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let lanes = held_key_lanes();
    for lane in &lanes {
        provision_lane(&pool, lane).await;
    }
    let rows = vec![
        plain_insert(&lanes[0], 1),
        plain_insert(&lanes[1], 2),
        unique_insert(&lanes[2], 3),
    ];
    let key = unique_key_of(&rows[2]);
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

    let (result, waited, fresh) = held_key_released_mid_batch(&pool, &lanes, &key, {
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

    assert!(
        waited,
        "the copy reserves its first lane and waits for the second"
    );
    assert!(!is_deadlock(&result), "the copy deadlocked: {result:?}");
    let fresh = fresh.expect("the fresh enqueue ran");
    assert!(fresh.is_ok(), "the fresh enqueue commits: {fresh:?}");
    let inserted = result.expect("the copy skips the held key's row");
    assert_eq!(
        inserted
            .iter()
            .map(|job| job.queue.as_str())
            .collect::<Vec<_>>(),
        vec![lanes[0].as_str(), lanes[1].as_str()]
    );

    drop_claim(&pool, &key).await;
    for lane in &lanes {
        setup::clean_queue(&pool, lane).await;
    }
}
