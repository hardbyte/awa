//! Lane-lock ordering between concurrent enqueuers on queue storage.
//!
//! Every enqueue holds a transaction-scoped advisory lock per
//! `(queue, priority, shard)` lane until it commits. Two transactions that
//! enqueue into the same lanes in opposite orders deadlock, so every batch
//! enqueue must take its lane locks in one canonical order: `(queue, priority,
//! shard)` ascending, with queue names compared bytewise.
//!
//! The deadlock is driven deterministically: a gate lock stalls the
//! `QueueStorage` batch between its first and second lane, a second producer
//! with the lanes in the opposite input order is then observed waiting on the
//! first lane's lock through `pg_locks`, and only then is the gate released.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test enqueue_lane_lock_order_test -- --nocapture`

use awa::model::{
    insert_many, insert_many_copy_from_pool, AwaError, QueueStorage, QueueStorageConfig,
};
use awa::{InsertOpts, InsertParams, JobRow, UniqueOpts};
use awa_testing::setup;
use sqlx::PgPool;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

static LANE_LOCK_TEST_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

const WAIT_DEADLINE: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy)]
enum Producer {
    InsertMany,
    Copy,
    CopyLarge,
    CopyUnique,
}

impl Producer {
    fn jobs_per_queue(self) -> usize {
        match self {
            Producer::CopyLarge => 1100,
            _ => 3,
        }
    }

    async fn run(self, pool: &PgPool, jobs: &[InsertParams]) -> Result<Vec<JobRow>, AwaError> {
        match self {
            Producer::InsertMany => insert_many(pool, jobs).await,
            Producer::Copy | Producer::CopyLarge | Producer::CopyUnique => {
                insert_many_copy_from_pool(pool, jobs).await
            }
        }
    }
}

async fn setup_queue_storage() -> (PgPool, QueueStorageConfig) {
    let pool = setup::setup(10).await;
    setup::activate_queue_storage(&pool).await;
    (pool, QueueStorageConfig::default())
}

fn job(queue: &str, seq: usize, unique: bool) -> InsertParams {
    InsertParams {
        kind: "lane_lock_order_job".to_string(),
        args: serde_json::json!({ "queue": queue, "seq": seq }),
        opts: InsertOpts {
            queue: queue.to_string(),
            unique: unique.then(|| UniqueOpts {
                by_queue: true,
                by_args: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    }
}

fn jobs_for(producer: Producer, queues: &[&str]) -> Vec<InsertParams> {
    let unique = matches!(producer, Producer::CopyUnique);
    queues
        .iter()
        .flat_map(|queue| (0..producer.jobs_per_queue()).map(move |seq| job(queue, seq, unique)))
        .collect()
}

/// Key of the advisory lock `reserve_enqueue_seq` holds for a lane until the
/// enqueue transaction ends.
async fn reservation_lock_key(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar(
        r#"
        SELECT hashtextextended(
            format('%I.%I', 'awa', awa.queue_lane_sequence_name('queue_enqueue_seq', $1, 2::smallint, 0::smallint)),
            0
        )
        "#,
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("derive reservation lock key")
}

/// Key of the advisory lock `QueueStorage` takes while it provisions a lane.
async fn provisioning_lock_key(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar("SELECT hashtextextended($1, 0)")
        .bind(format!("awa:{queue}:2:0"))
        .fetch_one(pool)
        .await
        .expect("derive provisioning lock key")
}

async fn wait_for_advisory_waiter(pool: &PgPool, key: i64) {
    let started = Instant::now();
    loop {
        let waiting: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_locks
                WHERE locktype = 'advisory'
                  AND NOT granted
                  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND ((classid::bigint << 32) | objid::bigint) = $1
            )
            "#,
        )
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("poll pg_locks");
        if waiting {
            return;
        }
        assert!(
            started.elapsed() < WAIT_DEADLINE,
            "no backend started waiting for advisory lock {key}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Provision both lanes in committed state so the racing transactions contend
/// on the reservation locks alone, not on uncommitted lane rows.
async fn provision_lanes(pool: &PgPool, config: &QueueStorageConfig, queues: &[&str]) {
    let store = QueueStorage::new(config.clone()).expect("create queue storage");
    for queue in queues {
        store
            .enqueue_params_batch(pool, &[job(queue, 0, false)])
            .await
            .expect("provision lane");
    }
}

async fn assert_producer_does_not_deadlock_with_queue_storage_batch(producer: Producer) {
    let _guard = LANE_LOCK_TEST_GUARD.lock().await;
    let (pool, config) = setup_queue_storage().await;

    let suffix = Uuid::new_v4().simple().to_string();
    let first_queue = format!("lock_order_a_{suffix}");
    let second_queue = format!("lock_order_b_{suffix}");
    provision_lanes(&pool, &config, &[&first_queue, &second_queue]).await;

    let first_reservation = reservation_lock_key(&pool, &first_queue).await;
    let second_provisioning = provisioning_lock_key(&pool, &second_queue).await;

    // The gate stalls the QueueStorage batch after it has reserved its first
    // lane and before it reaches its second.
    let mut gate = pool.begin().await.expect("begin gate transaction");
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(second_provisioning)
        .execute(&mut *gate)
        .await
        .expect("take gate lock");

    // A fresh store has an empty lane cache, so it provisions each lane under
    // the lock the gate holds.
    let storage_batch = {
        let pool = pool.clone();
        let config = config.clone();
        let jobs = jobs_for(Producer::InsertMany, &[&first_queue, &second_queue]);
        tokio::spawn(async move {
            let store = QueueStorage::new(config).expect("create queue storage");
            store.enqueue_params_batch(&pool, &jobs).await
        })
    };
    wait_for_advisory_waiter(&pool, second_provisioning).await;

    // The competing producer lists the lanes in the opposite order, and so
    // reaches for the first lane's reservation lock, which the batch holds.
    let competing_jobs = jobs_for(producer, &[&second_queue, &first_queue]);
    let expected_queues: Vec<String> = competing_jobs
        .iter()
        .map(|params| params.opts.queue.clone())
        .collect();
    let competing = {
        let pool = pool.clone();
        tokio::spawn(async move { producer.run(&pool, &competing_jobs).await })
    };
    wait_for_advisory_waiter(&pool, first_reservation).await;

    gate.commit().await.expect("release gate");

    let storage_result = tokio::time::timeout(WAIT_DEADLINE, storage_batch)
        .await
        .expect("storage batch finished")
        .expect("storage batch task");
    let competing_result = tokio::time::timeout(WAIT_DEADLINE, competing)
        .await
        .expect("competing producer finished")
        .expect("competing producer task");

    storage_result.expect("QueueStorage batch must not be chosen as a deadlock victim");
    let inserted =
        competing_result.expect("competing producer must not be chosen as a deadlock victim");

    // Lane ordering must not leak into the result: rows come back in input order.
    let returned_queues: Vec<String> = inserted.iter().map(|row| row.queue.clone()).collect();
    assert_eq!(returned_queues, expected_queues);
    let returned_seqs: Vec<i64> = inserted
        .iter()
        .map(|row| row.args["seq"].as_i64().expect("seq arg"))
        .collect();
    let expected_seqs: Vec<i64> = (0..2)
        .flat_map(|_| 0..producer.jobs_per_queue() as i64)
        .collect();
    assert_eq!(returned_seqs, expected_seqs);

    for queue in [&first_queue, &second_queue] {
        let stored: i64 =
            sqlx::query_scalar("SELECT count(*) FROM awa.ready_entries WHERE queue = $1")
                .bind(queue)
                .fetch_one(&pool)
                .await
                .expect("count stored jobs");
        // One provisioning job, the QueueStorage batch's, and the competing
        // producer's own.
        assert_eq!(
            stored as usize,
            1 + Producer::InsertMany.jobs_per_queue() + producer.jobs_per_queue(),
            "queue {queue}"
        );
    }
}

#[tokio::test]
async fn insert_many_takes_lane_locks_in_canonical_order() {
    assert_producer_does_not_deadlock_with_queue_storage_batch(Producer::InsertMany).await;
}

#[tokio::test]
async fn insert_many_copy_takes_lane_locks_in_canonical_order() {
    assert_producer_does_not_deadlock_with_queue_storage_batch(Producer::Copy).await;
}

#[tokio::test]
async fn insert_many_copy_csv_takes_lane_locks_in_canonical_order() {
    assert_producer_does_not_deadlock_with_queue_storage_batch(Producer::CopyLarge).await;
}

#[tokio::test]
async fn insert_many_copy_unique_takes_lane_locks_in_canonical_order() {
    assert_producer_does_not_deadlock_with_queue_storage_batch(Producer::CopyUnique).await;
}

/// Ordering keys that route to each shard of a `shards`-way queue.
fn ordering_key_per_shard(shards: i16) -> Vec<Vec<u8>> {
    let mut keys: Vec<Option<Vec<u8>>> = vec![None; shards as usize];
    for candidate in 0..100_000u32 {
        let key = format!("lane-order-key-{candidate}").into_bytes();
        let shard = awa::model::queue_storage::shard_for_ordering_key(&key, shards) as usize;
        keys[shard].get_or_insert(key);
        if keys.iter().all(Option::is_some) {
            break;
        }
    }
    keys.into_iter()
        .map(|key| key.expect("a key for every shard"))
        .collect()
}

async fn assert_batch_reserves_lanes_in_canonical_order(producer: Producer) {
    let _guard = LANE_LOCK_TEST_GUARD.lock().await;
    let (pool, _config) = setup_queue_storage().await;

    let suffix = Uuid::new_v4().simple().to_string();
    // The queue names sort opposite to the order they first appear in the input,
    // and `Z` sorts before `a` bytewise but after it in most locales.
    let queues = [
        format!("lane_order_b_{suffix}"),
        format!("lane_order_Z_{suffix}"),
        format!("lane_order_a_{suffix}"),
    ];
    let shards = 4_i16;
    for queue in &queues {
        sqlx::query(
            "INSERT INTO awa.queue_meta (queue, enqueue_shards) VALUES ($1, $2)
             ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards",
        )
        .bind(queue)
        .bind(shards)
        .execute(&pool)
        .await
        .expect("configure shards");
    }

    let keys = ordering_key_per_shard(shards);
    let mut jobs = Vec::new();
    let rounds = match producer {
        Producer::CopyLarge => 700,
        _ => 12,
    };
    for round in 0..rounds {
        for queue in &queues {
            let mut params = job(queue, round, false);
            params.opts.priority = if round % 5 == 0 { 1 } else { 2 };
            // Keyed rows spread over every shard; every third row has no key.
            if round % 3 != 0 {
                params.opts.ordering_key = Some(keys[(round + queue.len()) % keys.len()].clone());
            }
            jobs.push(params);
        }
    }

    let inserted = producer.run(&pool, &jobs).await.expect("insert batch");
    assert_eq!(inserted.len(), jobs.len());
    for (row, params) in inserted.iter().zip(&jobs) {
        assert_eq!(row.queue, params.opts.queue);
        assert_eq!(row.args, params.args);
    }

    // Job ids are allocated as each row reaches `insert_job_compat`, so id
    // order is lock order.
    let lane_by_job: std::collections::HashMap<i64, (String, i16, i16)> =
        sqlx::query_as::<_, (i64, String, i16, i16)>(
            "SELECT job_id, queue, priority, enqueue_shard FROM awa.ready_entries WHERE job_id = ANY($1)",
        )
        .bind(inserted.iter().map(|row| row.id).collect::<Vec<_>>())
        .fetch_all(&pool)
        .await
        .expect("read lanes")
        .into_iter()
        .map(|(job_id, queue, priority, shard)| (job_id, (queue, priority, shard)))
        .collect();
    assert_eq!(lane_by_job.len(), jobs.len());

    let mut by_lock_order: Vec<(i64, usize)> = inserted
        .iter()
        .enumerate()
        .map(|(position, row)| (row.id, position))
        .collect();
    by_lock_order.sort_unstable();
    let lock_order: Vec<(&(String, i16, i16), usize)> = by_lock_order
        .iter()
        .map(|(job_id, position)| (&lane_by_job[job_id], *position))
        .collect();
    for pair in lock_order.windows(2) {
        let (lane, position) = pair[0];
        let (next_lane, next_position) = pair[1];
        assert!(
            lane.0.as_bytes() < next_lane.0.as_bytes()
                || (lane.0 == next_lane.0 && (lane.1, lane.2) < (next_lane.1, next_lane.2))
                || (lane == next_lane && position < next_position),
            "lane {lane:?} (input {position}) was locked before lane {next_lane:?} (input {next_position})"
        );
    }
}

#[tokio::test]
async fn insert_many_reserves_sharded_lanes_in_canonical_order() {
    assert_batch_reserves_lanes_in_canonical_order(Producer::InsertMany).await;
}

#[tokio::test]
async fn insert_many_copy_reserves_sharded_lanes_in_canonical_order() {
    assert_batch_reserves_lanes_in_canonical_order(Producer::Copy).await;
}

#[tokio::test]
async fn insert_many_copy_csv_reserves_sharded_lanes_in_canonical_order() {
    assert_batch_reserves_lanes_in_canonical_order(Producer::CopyLarge).await;
}
