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
use awa::model::{admin, dlq, JobFilter};
use awa::{insert_many, JobRow, UniqueOpts};
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

/// A bulk DLQ retry claims the row's key before it reserves the lane, so a
/// fresh enqueue holding the key commits and the retry fails with a unique
/// conflict instead of a deadlock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dlq_bulk_retry_and_fresh_enqueue_of_one_key_do_not_deadlock() {
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

    let filter = JobFilter::new().queue(queue.clone());
    let result = race_fresh_enqueue(&pool, &queue, &key, {
        let pool = pool.clone();
        async move { dlq::bulk_retry_from_dlq_matching(&pool, &filter, false).await }
    })
    .await;
    assert_unique_conflict(result, "bulk DLQ retry");
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

/// Of two DLQ rows with one unique key, the one that owns the key keeps it
/// even when the other has the lower job ID: the bulk retry, which retries
/// every row or none, refuses the other row and changes nothing.
#[tokio::test]
async fn dlq_bulk_retry_keeps_the_key_owner_among_same_key_rows() {
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

    let filter = JobFilter::new().queue(queue.clone());
    let result = dlq::bulk_retry_from_dlq_matching(&pool, &filter, false).await;
    assert!(
        matches!(result, Err(AwaError::UniqueConflict { .. })),
        "the row that does not own the key conflicts, got {result:?}"
    );
    let still_dead: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.dlq_entries WHERE job_id = ANY($1)")
            .bind(vec![other, owner])
            .fetch_one(&pool)
            .await
            .expect("dlq rows");
    assert_eq!(still_dead, 2, "the failed bulk retry leaves both rows");
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
