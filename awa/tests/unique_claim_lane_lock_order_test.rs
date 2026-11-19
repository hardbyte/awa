//! Unique-claim and lane lock order.
//!
//! A fresh enqueue claims its jobs' unique keys in key order and then reserves
//! their lanes, holding every lock until it commits. Every other transaction
//! that changes unique claims, by claiming or releasing them, must take them
//! in the same order and before any lane, or two transactions each wait for
//! the other and PostgreSQL aborts one with a deadlock (40P01). A claim
//! upsert locks a conflicting row even when it leaves the row unchanged, so
//! it waits on an uncommitted release of that row as it would on a claim.
//!
//! Each test drives the interleaving deterministically. A holder transaction
//! takes a fresh enqueue's first lock, the claim of a key. The operation under
//! test starts and is observed waiting on the holder through
//! `pg_blocking_pids`. Only then does the holder take its second lock, a lane
//! reservation or the claim of a later key, under a `lock_timeout` longer than
//! `deadlock_timeout`, and commit.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test unique_claim_lane_lock_order_test -- --nocapture`

mod ci_timing;

use awa::model::queue_storage::{ClaimedRuntimeJob, PromotionLane};
use awa::model::{admin, dlq, BulkCursor, ListDlqFilter};
use awa::{insert_many, insert_many_copy_from_pool, JobRow, UniqueOpts};
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

/// The claim upsert a fresh enqueue makes.
const UPSERT_CLAIM: &str = "INSERT INTO awa.job_unique_claims (unique_key, job_id) \
     VALUES ($1, nextval('awa.job_id_seq')) \
     ON CONFLICT (unique_key) DO UPDATE SET job_id = EXCLUDED.job_id \
     WHERE awa.job_unique_claims.job_id = EXCLUDED.job_id";

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

    /// A fresh enqueue that found `key` claimed by a live job: its claim
    /// upsert holds the existing row locked and leaves it unchanged.
    async fn upsert(pool: &PgPool, key: &[u8]) -> Self {
        let mut tx = pool.begin().await.expect("begin fresh enqueue");
        let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *tx)
            .await
            .expect("backend pid");
        let claimed = sqlx::query(UPSERT_CLAIM)
            .bind(key)
            .execute(&mut *tx)
            .await
            .expect("upsert the claim");
        assert_eq!(claimed.rows_affected(), 0, "a live job holds the key");
        Self { tx, pid }
    }

    async fn upsert_and_commit(mut self, keys: &[Vec<u8>]) -> Result<(), sqlx::Error> {
        sqlx::query("SET LOCAL lock_timeout = '5s'")
            .execute(&mut *self.tx)
            .await?;
        for key in keys {
            sqlx::query(UPSERT_CLAIM)
                .bind(key)
                .execute(&mut *self.tx)
                .await?;
        }
        self.tx.commit().await
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
                "the operation never waited on the fresh enqueue's claim"
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
    .await
    .expect("promotion cancels the duplicate instead of failing");
    assert_eq!(result.promoted, 0, "{result:?}");
    assert_eq!(result.selected, 1, "{result:?}");
    let state: String = sqlx::query_scalar("SELECT state::text FROM awa.jobs WHERE id = $1")
        .bind(scheduled)
        .fetch_one(&pool)
        .await
        .expect("job state");
    assert_eq!(state, "cancelled", "the job loses the key and is cancelled");

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

// Release order. A transaction that changes the claims of several jobs, by
// cancelling, completing, failing, rescuing, claiming or discarding them, must
// change them in key order. Each test gives JOBS jobs keys K1 < ... < Kn and
// inserts them from the largest key down, while a fresh enqueue of K1..Kn
// holds K1. An operation that reaches any other key before K1 deadlocks with
// the enqueue once it takes the rest. Paths that read their rows in heap or
// plan order run several rounds.

/// Jobs per round.
const JOBS: usize = 4;
/// Rounds for paths whose row order follows heap or plan order.
const ROUNDS: usize = 4;

/// States a unique job claims its key in.
const DEFAULT_STATES: u8 = 0b0001_1111;
const AVAILABLE_ONLY: u8 = 0b0000_0010;
const AVAILABLE_RUNNING: u8 = 0b0000_0110;
const AVAILABLE_RUNNING_FAILED: u8 = 0b0010_0110;

/// JOBS job kinds of `queue` with their unique keys, in key order.
fn kinds_in_key_order(queue: &str) -> Vec<(String, Vec<u8>)> {
    let mut kinds: Vec<_> = (0..JOBS)
        .map(|n| {
            let kind = format!("claim_order_{n}");
            let key = awa::model::unique::compute_unique_key(&kind, Some(queue), None, None);
            (kind, key)
        })
        .collect();
    kinds.sort_by(|left, right| left.1.cmp(&right.1));
    kinds
}

async fn insert_unique(
    pool: &PgPool,
    queue: &str,
    kind: &str,
    states: u8,
    deferred: bool,
    max_attempts: i16,
) -> JobRow {
    insert_many(
        pool,
        &[InsertParams {
            kind: kind.to_string(),
            args: serde_json::json!({}),
            opts: InsertOpts {
                queue: queue.to_string(),
                priority: PRIORITY,
                max_attempts,
                run_at: deferred.then(|| chrono::Utc::now() + chrono::Duration::hours(1)),
                unique: Some(UniqueOpts {
                    by_queue: true,
                    by_args: false,
                    states,
                    ..Default::default()
                }),
                ..Default::default()
            },
        }],
    )
    .await
    .expect("insert unique job")
    .pop()
    .expect("inserted job")
}

/// Insert a job per key of `queue`, from the largest key down, and return
/// the keys in key order with the jobs holding them. `deferred(n)` says
/// whether the job with the n-th smallest key is scheduled.
async fn insert_keyed_jobs(
    pool: &PgPool,
    queue: &str,
    states: u8,
    max_attempts: i16,
    deferred: impl Fn(usize) -> bool,
) -> (Vec<Vec<u8>>, Vec<JobRow>) {
    let kinds = kinds_in_key_order(queue);
    let mut jobs = Vec::with_capacity(kinds.len());
    for (n, (kind, _)) in kinds.iter().enumerate().rev() {
        jobs.push(insert_unique(pool, queue, kind, states, deferred(n), max_attempts).await);
    }
    jobs.reverse();
    (kinds.into_iter().map(|(_, key)| key).collect(), jobs)
}

/// Claim every available job of `queue`, in lane order.
async fn claim_all(pool: &PgPool, queue: &str, deadline: Duration) -> Vec<ClaimedRuntimeJob> {
    let claimed = QueueStorage::new(QueueStorageConfig::default())
        .expect("queue storage")
        .claim_runtime_batch(pool, queue, 10, deadline)
        .await
        .expect("claim jobs");
    assert_eq!(claimed.len(), JOBS, "every job is claimed");
    claimed
}

/// Run `operation` against a fresh enqueue that holds the claim of `keys[0]`
/// and then takes the rest of `keys` in order, and return its result once
/// both have finished.
async fn race_sorted_enqueue<T, F>(pool: &PgPool, keys: &[Vec<u8>], operation: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let holder = FreshEnqueue::upsert(pool, &keys[0]).await;
    let operation = tokio::spawn(operation);
    holder.wait_for_waiter(pool).await;
    holder
        .upsert_and_commit(&keys[1..])
        .await
        .expect("the fresh enqueue takes every key and commits");
    tokio::time::timeout(
        ci_timing::scaled_timeout(Duration::from_secs(10)),
        operation,
    )
    .await
    .expect("the operation finishes")
    .expect("join operation")
}

async fn claim_holders(pool: &PgPool, keys: &[Vec<u8>]) -> Vec<Option<i64>> {
    let mut holders = Vec::with_capacity(keys.len());
    for key in keys {
        holders.push(
            sqlx::query_scalar("SELECT job_id FROM awa.job_unique_claims WHERE unique_key = $1")
                .bind(key)
                .fetch_optional(pool)
                .await
                .expect("read claim"),
        );
    }
    holders
}

async fn job_states(pool: &PgPool, jobs: &[JobRow]) -> Vec<JobState> {
    let mut states = Vec::with_capacity(jobs.len());
    for job in jobs {
        states.push(
            sqlx::query_scalar("SELECT state FROM awa.jobs WHERE id = $1")
                .bind(job.id)
                .fetch_one(pool)
                .await
                .expect("job state"),
        );
    }
    states
}

fn ids_largest_key_first(jobs: &[JobRow]) -> Vec<i64> {
    jobs.iter().rev().map(|job| job.id).collect()
}

#[derive(Clone, Copy, Debug)]
enum CancelShape {
    Available,
    Scheduled,
    Mixed,
    Running,
}

/// A bulk cancel releases its jobs' keys in key order, not the order of the
/// IDs it is given.
async fn bulk_cancel_releases_keys_in_key_order(shape: CancelShape) {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_order_cancel");
    let (keys, jobs) = insert_keyed_jobs(&pool, &queue, DEFAULT_STATES, 25, |n| match shape {
        CancelShape::Available | CancelShape::Running => false,
        CancelShape::Scheduled => true,
        CancelShape::Mixed => n % 2 == 0,
    })
    .await;
    if matches!(shape, CancelShape::Running) {
        claim_all(&pool, &queue, Duration::from_secs(300)).await;
    }

    let cancelled = race_sorted_enqueue(&pool, &keys, {
        let pool = pool.clone();
        let ids = ids_largest_key_first(&jobs);
        async move { admin::bulk_cancel(&pool, &ids).await }
    })
    .await
    .unwrap_or_else(|err| panic!("{shape:?} bulk cancel failed: {err:?}"));
    assert_eq!(cancelled.len(), JOBS, "{shape:?}");
    assert_eq!(
        job_states(&pool, &jobs).await,
        [JobState::Cancelled; JOBS],
        "{shape:?}"
    );
    assert_eq!(
        claim_holders(&pool, &keys).await,
        [None; JOBS],
        "{shape:?}: the cancels release every key"
    );

    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_cancel_of_available_jobs_releases_keys_in_key_order() {
    bulk_cancel_releases_keys_in_key_order(CancelShape::Available).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_cancel_of_scheduled_jobs_releases_keys_in_key_order() {
    bulk_cancel_releases_keys_in_key_order(CancelShape::Scheduled).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_cancel_of_available_and_scheduled_jobs_releases_keys_in_key_order() {
    bulk_cancel_releases_keys_in_key_order(CancelShape::Mixed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_cancel_of_running_jobs_releases_keys_in_key_order() {
    bulk_cancel_releases_keys_in_key_order(CancelShape::Running).await;
}

/// Each drain chunk releases its jobs' keys in key order, not job ID or heap
/// order.
async fn drain_releases_keys_in_key_order(deferred: bool) {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    for round in 0..ROUNDS {
        let queue = unique_queue("claim_order_drain");
        let (keys, jobs) = insert_keyed_jobs(&pool, &queue, DEFAULT_STATES, 25, |_| deferred).await;

        let drained = race_sorted_enqueue(&pool, &keys, {
            let pool = pool.clone();
            let queue = queue.clone();
            async move { admin::drain_queue(&pool, &queue).await }
        })
        .await
        .unwrap_or_else(|err| {
            panic!("drain (deferred: {deferred}, round {round}) failed: {err:?}")
        });
        assert_eq!(drained, JOBS as u64);
        assert_eq!(job_states(&pool, &jobs).await, [JobState::Cancelled; JOBS]);
        assert_eq!(claim_holders(&pool, &keys).await, [None; JOBS]);

        setup::clean_queue(&pool, &queue).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_of_scheduled_jobs_releases_keys_in_key_order() {
    drain_releases_keys_in_key_order(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_of_available_jobs_releases_keys_in_key_order() {
    drain_releases_keys_in_key_order(false).await;
}

/// A completion batch releases its jobs' keys in key order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_completion_releases_keys_in_key_order() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_order_complete");
    let (keys, jobs) = insert_keyed_jobs(&pool, &queue, AVAILABLE_RUNNING, 25, |_| false).await;
    let mut claimed = claim_all(&pool, &queue, Duration::from_secs(300)).await;
    let ids = ids_largest_key_first(&jobs);
    claimed.sort_by_key(|entry| ids.iter().position(|id| *id == entry.job.id));

    let completed = race_sorted_enqueue(&pool, &keys, {
        let pool = pool.clone();
        async move {
            QueueStorage::new(QueueStorageConfig::default())
                .expect("queue storage")
                .complete_runtime_batch(&pool, &claimed)
                .await
        }
    })
    .await
    .expect("the completion batch commits");
    assert_eq!(completed.len(), JOBS);
    assert_eq!(job_states(&pool, &jobs).await, [JobState::Completed; JOBS]);
    assert_eq!(claim_holders(&pool, &keys).await, [None; JOBS]);

    setup::clean_queue(&pool, &queue).await;
}

/// A claim batch whose jobs' masks leave out `running` releases their keys
/// in key order, not lane order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn claim_batch_releases_keys_in_key_order() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_order_claim");
    let (keys, jobs) = insert_keyed_jobs(&pool, &queue, AVAILABLE_ONLY, 25, |_| false).await;

    let claimed = race_sorted_enqueue(&pool, &keys, {
        let pool = pool.clone();
        let queue = queue.clone();
        async move {
            QueueStorage::new(QueueStorageConfig::default())
                .expect("queue storage")
                .claim_runtime_batch(&pool, &queue, 10, Duration::from_secs(300))
                .await
        }
    })
    .await
    .expect("the claim batch commits");
    let mut claimed: Vec<i64> = claimed.iter().map(|entry| entry.job.id).collect();
    claimed.sort_unstable();
    let mut ids: Vec<i64> = jobs.iter().map(|job| job.id).collect();
    ids.sort_unstable();
    assert_eq!(claimed, ids);
    assert_eq!(claim_holders(&pool, &keys).await, [None; JOBS]);
    admin::bulk_cancel(&pool, &ids)
        .await
        .expect("cancel the running jobs");

    setup::clean_queue(&pool, &queue).await;
}

/// A rescue batch releases its jobs' keys in key order, whether the rescued
/// attempts retry or dead-letter.
async fn rescue_releases_keys_in_key_order(to_dlq: bool) {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    for round in 0..ROUNDS {
        let queue = unique_queue("claim_order_rescue");
        let max_attempts = if to_dlq { 1 } else { 25 };
        let (keys, jobs) =
            insert_keyed_jobs(&pool, &queue, AVAILABLE_RUNNING, max_attempts, |_| false).await;
        claim_all(&pool, &queue, Duration::from_millis(1)).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let rescued = race_sorted_enqueue(&pool, &keys, {
            let pool = pool.clone();
            let queue = queue.clone();
            async move {
                QueueStorage::new(QueueStorageConfig::default())
                    .expect("queue storage")
                    .rescue_expired_deadlines_with_dlq(&pool, &move |name| to_dlq && name == queue)
                    .await
            }
        })
        .await
        .unwrap_or_else(|err| panic!("rescue (to DLQ: {to_dlq}, round {round}) failed: {err:?}"));
        let settled = if to_dlq {
            JobState::Failed
        } else {
            JobState::Retryable
        };
        let rescued: Vec<JobState> = rescued
            .iter()
            .filter(|row| jobs.iter().any(|job| job.id == row.id))
            .map(|row| row.state)
            .collect();
        assert_eq!(rescued, [settled; JOBS], "to DLQ: {to_dlq}");
        let ids: Vec<i64> = jobs.iter().map(|job| job.id).collect();
        let dead_letters: i64 =
            sqlx::query_scalar("SELECT count(*) FROM awa.dlq_entries WHERE job_id = ANY($1)")
                .bind(&ids)
                .fetch_one(&pool)
                .await
                .expect("count dlq rows");
        assert_eq!(dead_letters, if to_dlq { JOBS as i64 } else { 0 });
        assert_eq!(claim_holders(&pool, &keys).await, [None; JOBS]);

        setup::clean_queue(&pool, &queue).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rescue_to_retryable_releases_keys_in_key_order() {
    rescue_releases_keys_in_key_order(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rescue_to_dlq_releases_keys_in_key_order() {
    rescue_releases_keys_in_key_order(true).await;
}

/// Discarding failed jobs releases the keys of failed rows and DLQ rows in
/// one key-ordered pass, though it deletes the failed rows first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discard_failed_releases_keys_in_key_order() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_order_discard");
    let kind = format!("claim_order_discard_{queue}");
    let storage = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");

    // A failed row holding the larger key.
    let failed = insert_unique(&pool, &queue, &kind, AVAILABLE_RUNNING_FAILED, false, 25).await;
    let larger = failed.unique_key.clone().expect("unique key");
    let claimed = storage
        .claim_runtime_batch(&pool, &queue, 10, Duration::from_secs(300))
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    storage
        .fail_terminal(&pool, failed.id, claimed[0].job.run_lease, "boom", None)
        .await
        .expect("fail")
        .expect("failed job");

    // A DLQ row holding a proper prefix of that key, and so the smaller key.
    let smaller = larger[..larger.len() - 1].to_vec();
    let dead: i64 = sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries (
             job_id, kind, queue, priority, dlq_reason, original_run_lease,
             unique_key, unique_states
         )
         VALUES (nextval('awa.job_id_seq'), $1, $2, $3, 'seeded', 1, $4, '01100100')
         RETURNING job_id",
    )
    .bind(&kind)
    .bind(&queue)
    .bind(PRIORITY)
    .bind(&smaller)
    .fetch_one(&pool)
    .await
    .expect("seed dlq row");
    sqlx::query("INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, $2)")
        .bind(&smaller)
        .bind(dead)
        .execute(&pool)
        .await
        .expect("the DLQ row holds its key");
    let keys = vec![smaller, larger];
    assert_eq!(
        claim_holders(&pool, &keys).await,
        [Some(dead), Some(failed.id)]
    );

    let discarded = race_sorted_enqueue(&pool, &keys, {
        let pool = pool.clone();
        let kind = kind.clone();
        async move { admin::discard_failed(&pool, &kind).await }
    })
    .await
    .expect("the discard commits");
    assert_eq!(discarded, 2);
    assert_eq!(claim_holders(&pool, &keys).await, [None, None]);

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

/// Insert two unique jobs with distinct keys on `queue`'s lane and fail them
/// with their claims released, as a failed job whose mask omits `failed`.
async fn seed_failed_unique_pair(pool: &PgPool, queue: &str) -> [(i64, Vec<u8>); 2] {
    let params: Vec<InsertParams> = ["claim_lane_retry_a", "claim_lane_retry_b"]
        .into_iter()
        .map(|kind| InsertParams {
            kind: kind.to_string(),
            args: serde_json::json!({}),
            opts: InsertOpts {
                queue: queue.to_string(),
                priority: PRIORITY,
                unique: Some(UniqueOpts {
                    by_queue: true,
                    by_args: false,
                    ..Default::default()
                }),
                ..Default::default()
            },
        })
        .collect();
    let jobs = insert_many(pool, &params)
        .await
        .expect("insert unique jobs");
    let ids: Vec<i64> = jobs.iter().map(|job| job.id).collect();
    sqlx::query(
        r#"
        WITH moved AS (
            DELETE FROM awa.ready_entries
            WHERE job_id = ANY($1)
            RETURNING ready_slot, ready_generation, job_id, kind, queue, args,
                      priority, attempt, run_lease, max_attempts, lane_seq, run_at,
                      attempted_at, created_at, unique_key, unique_states, payload
        )
        INSERT INTO awa.done_entries (
            ready_slot, ready_generation, job_id, kind, queue, args, state,
            priority, attempt, run_lease, max_attempts, lane_seq, run_at,
            attempted_at, finalized_at, created_at, unique_key, unique_states, payload
        )
        SELECT ready_slot, ready_generation, job_id, kind, queue, args,
               'failed'::awa.job_state, priority, 1, run_lease,
               max_attempts, lane_seq, run_at, now(), now(),
               created_at, unique_key, unique_states, payload
        FROM moved
        "#,
    )
    .bind(&ids)
    .execute(pool)
    .await
    .expect("fail the jobs");
    sqlx::query("DELETE FROM awa.job_unique_claims WHERE job_id = ANY($1)")
        .bind(&ids)
        .execute(pool)
        .await
        .expect("release the failed jobs' claims");
    let mut seeded = jobs
        .into_iter()
        .map(|job| (job.id, job.unique_key.expect("unique key")));
    let first = seeded.next().expect("first job");
    let second = seeded.next().expect("second job");
    assert!(first.0 < second.0, "jobs are retried in job ID order");
    [first, second]
}

async fn assert_left_failed(pool: &PgPool, job_id: i64) {
    let state: JobState = sqlx::query_scalar("SELECT state FROM awa.jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .expect("job state");
    assert_eq!(state, JobState::Failed, "the conflicting job stays failed");
}

/// A bounded retry of failed jobs A and B on one lane, while a fresh enqueue
/// holds B's key and then reserves the lane, claims both keys before it
/// reserves the lane, so it loses B's key instead of deadlocking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_retry_by_queue_claims_every_key_before_reserving_the_lane() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_lane_retry_failed");
    provision_lane(&pool, &queue).await;
    let [(first, _), (second, second_key)] = seed_failed_unique_pair(&pool, &queue).await;
    let batch =
        race_fresh_enqueue(&pool, &queue, &second_key, {
            let pool = pool.clone();
            let queue = queue.clone();
            async move {
                admin::retry_failed_by_queue_batch(&pool, &queue, 10, BulkCursor::default()).await
            }
        })
        .await
        .expect("retry_failed_batch skips the conflicting job");
    assert_eq!(
        (batch.count, batch.skipped, batch.conflicts.clone()),
        (1, 1, vec![second])
    );
    let state: JobState = sqlx::query_scalar("SELECT state FROM awa.jobs WHERE id = $1")
        .bind(first)
        .fetch_one(&pool)
        .await
        .expect("job state");
    assert_eq!(state, JobState::Available);
    assert_left_failed(&pool, second).await;
    drop_claim(&pool, &second_key).await;
    setup::clean_queue(&pool, &queue).await;
}

/// [`bounded_retry_by_queue_claims_every_key_before_reserving_the_lane`] for
/// a bounded retry of explicit job IDs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_retry_by_ids_claims_every_key_before_reserving_the_lane() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("claim_lane_retry_ids");
    provision_lane(&pool, &queue).await;
    let [(first, _), (second, second_key)] = seed_failed_unique_pair(&pool, &queue).await;
    let outcome = race_fresh_enqueue(&pool, &queue, &second_key, {
        let pool = pool.clone();
        async move { admin::bulk_retry_in_batches(&pool, &[first, second], 10).await }
    })
    .await
    .expect("retry_jobs_in_batches skips the conflicting job");
    assert_eq!(
        (
            outcome.jobs.iter().map(|job| job.id).collect::<Vec<_>>(),
            outcome.skipped,
            outcome.conflicts.clone()
        ),
        (vec![first], 1, vec![second])
    );
    assert_left_failed(&pool, second).await;
    drop_claim(&pool, &second_key).await;
    setup::clean_queue(&pool, &queue).await;
}

/// Seed a DLQ row in `queue` whose failed-only mask keeps `key` claimed,
/// and its claim.
async fn seed_dlq_key_owner(pool: &PgPool, queue: &str, key: &[u8]) -> i64 {
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries (
             job_id, kind, queue, priority, dlq_reason, original_run_lease,
             dlq_at, unique_key, unique_states
         )
         VALUES (nextval('awa.job_id_seq'), 'claim_lane_job', $1, $2, 'seeded', 1,
                 now() - interval '1 day', $3, '00000100')
         RETURNING job_id",
    )
    .bind(queue)
    .bind(PRIORITY)
    .bind(key)
    .fetch_one(pool)
    .await
    .expect("seed dlq row");
    sqlx::query("INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, $2)")
        .bind(key)
        .bind(id)
        .execute(pool)
        .await
        .expect("the dlq row owns its key");
    id
}

/// Deleting DLQ rows releases their claims in key order, as every claim
/// path takes keys. The owner of the higher key B is the lower job ID, so
/// the delete returns it first. A holder locks the claim of the lower key A;
/// the purge, observed waiting on it, must not yet hold B's claim, which the
/// holder then takes before it commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dlq_deletes_release_claims_in_key_order() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    for purge in ["purge_dlq_batch", "purge_dlq", "cleanup_dlq"] {
        let queue = unique_queue("claim_order_dlq_delete");
        let low = format!("claim-order-{queue}-a").into_bytes();
        let high = format!("claim-order-{queue}-b").into_bytes();
        seed_dlq_key_owner(&pool, &queue, &high).await;
        seed_dlq_key_owner(&pool, &queue, &low).await;

        let mut holder = pool.begin().await.expect("begin holder");
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *holder)
            .await
            .expect("backend pid");
        sqlx::query("SELECT 1 FROM awa.job_unique_claims WHERE unique_key = $1 FOR UPDATE")
            .bind(&low)
            .fetch_all(&mut *holder)
            .await
            .expect("hold the low key's claim");
        let deletes = tokio::spawn({
            let pool = pool.clone();
            let queue = queue.clone();
            async move {
                let filter = ListDlqFilter {
                    queue: Some(queue.clone()),
                    ..Default::default()
                };
                match purge {
                    "purge_dlq_batch" => {
                        dlq::purge_dlq_batch(&pool, &filter, false, 10, BulkCursor::default())
                            .await
                            .map(|batch| batch.count)
                    }
                    "purge_dlq" => dlq::purge_dlq(&pool, &filter, false).await,
                    _ => dlq::cleanup_dlq(&pool, Duration::from_secs(3600), 10, Some(&queue)).await,
                }
            }
        });
        let held = FreshEnqueue { tx: holder, pid };
        held.wait_for_waiter(&pool).await;
        let mut holder = held.tx;
        sqlx::query("SET LOCAL lock_timeout = '5s'")
            .execute(&mut *holder)
            .await
            .expect("lock timeout");
        sqlx::query("UPDATE awa.job_unique_claims SET job_id = job_id WHERE unique_key = $1")
            .bind(&high)
            .execute(&mut *holder)
            .await
            .unwrap_or_else(|err| panic!("{purge}: the holder takes the high key: {err}"));
        holder.commit().await.expect("commit holder");
        let deleted =
            tokio::time::timeout(ci_timing::scaled_timeout(Duration::from_secs(10)), deletes)
                .await
                .expect("the delete finishes")
                .expect("join delete");
        assert_eq!(deleted.expect(purge), 2, "{purge}");
        let left: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.job_unique_claims WHERE unique_key = ANY($1)",
        )
        .bind(vec![low.clone(), high.clone()])
        .fetch_one(&pool)
        .await
        .expect("claims");
        assert_eq!(left, 0, "{purge} releases both claims");
        setup::clean_queue(&pool, &queue).await;
    }
}
