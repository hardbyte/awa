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

use awa::audited_sql;
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
    CopySmall,
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
            Producer::CopySmall | Producer::CopyLarge | Producer::CopyUnique => {
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
        let stored: i64 = sqlx::query_scalar(audited_sql(
            "SELECT count(*) FROM awa.ready_entries WHERE queue = $1".to_string(),
        ))
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
    assert_producer_does_not_deadlock_with_queue_storage_batch(Producer::CopySmall).await;
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

/// Key of the advisory lock `reserve_enqueue_seq` holds for one lane.
async fn lane_reservation_lock_key(pool: &PgPool, queue: &str, priority: i16, shard: i16) -> i64 {
    sqlx::query_scalar(
        r#"
        SELECT hashtextextended(
            format('%I.%I', 'awa', awa.queue_lane_sequence_name('queue_enqueue_seq', $1, $2, $3)),
            0
        )
        "#,
    )
    .bind(queue)
    .bind(priority)
    .bind(shard)
    .fetch_one(pool)
    .await
    .expect("derive lane reservation lock key")
}

/// The backend waiting for the advisory lock `key`, once there is one.
async fn advisory_waiter_pid(pool: &PgPool, key: i64) -> i32 {
    let started = Instant::now();
    loop {
        let pid: Option<i32> = sqlx::query_scalar(
            r#"
            SELECT pid
            FROM pg_locks
            WHERE locktype = 'advisory'
              AND NOT granted
              AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND ((classid::bigint << 32) | objid::bigint) = $1
            LIMIT 1
            "#,
        )
        .bind(key)
        .fetch_optional(pool)
        .await
        .expect("poll pg_locks");
        if let Some(pid) = pid {
            return pid;
        }
        assert!(
            started.elapsed() < WAIT_DEADLINE,
            "no backend started waiting for advisory lock {key}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn granted_advisory_keys(pool: &PgPool, pid: i32) -> std::collections::BTreeSet<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT (classid::bigint << 32) | objid::bigint
        FROM pg_locks
        WHERE locktype = 'advisory' AND granted AND pid = $1
        "#,
    )
    .bind(pid)
    .fetch_all(pool)
    .await
    .expect("read granted advisory locks")
    .into_iter()
    .collect()
}

/// A batch over sharded lanes reserves them in `(queue, priority, shard)`
/// order, whatever order its rows arrive in. A gate holds one lane in the
/// middle of that order; the producer must be parked on it holding exactly the
/// lanes before it.
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
    let mut lanes = std::collections::BTreeSet::new();
    let rounds = match producer {
        Producer::CopyLarge => 700,
        _ => 12,
    };
    for round in 0..rounds {
        for queue in &queues {
            let mut params = job(queue, round, false);
            params.opts.priority = if round % 5 == 0 { 1 } else { 2 };
            // Every row is keyed, so its shard, and so its lane, is known here.
            let shard = (round + queue.len()) % keys.len();
            params.opts.ordering_key = Some(keys[shard].clone());
            lanes.insert((
                queue.as_bytes().to_vec(),
                params.opts.priority,
                shard as i16,
                queue.clone(),
            ));
            jobs.push(params);
        }
    }
    assert!(lanes.len() >= 6, "the batch must span many lanes");

    let canonical: Vec<(String, i16, i16)> = lanes
        .into_iter()
        .map(|(_, priority, shard, queue)| (queue, priority, shard))
        .collect();
    let mut canonical_keys = Vec::new();
    for (queue, priority, shard) in &canonical {
        canonical_keys.push(lane_reservation_lock_key(&pool, queue, *priority, *shard).await);
    }

    // Provision the lanes so the gate lock is the only thing the producer waits for.
    for (index, queue) in queues.iter().enumerate() {
        let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
        for (priority, shard) in [
            (1, 0),
            (1, 1),
            (1, 2),
            (1, 3),
            (2, 0),
            (2, 1),
            (2, 2),
            (2, 3),
        ] {
            let mut params = job(queue, 10_000 + index, false);
            params.opts.priority = priority;
            params.opts.ordering_key = Some(keys[shard as usize].clone());
            store
                .enqueue_params_batch(&pool, &[params])
                .await
                .expect("provision lane");
        }
    }

    let gated = canonical.len() / 2;
    let mut gate = pool.begin().await.expect("begin gate transaction");
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(canonical_keys[gated])
        .execute(&mut *gate)
        .await
        .expect("take gate lock");

    let competing = {
        let pool = pool.clone();
        let jobs = jobs.clone();
        tokio::spawn(async move { producer.run(&pool, &jobs).await })
    };
    let waiter = advisory_waiter_pid(&pool, canonical_keys[gated]).await;
    let held = granted_advisory_keys(&pool, waiter).await;
    let expected: std::collections::BTreeSet<i64> =
        canonical_keys[..gated].iter().copied().collect();
    assert_eq!(
        held,
        expected,
        "a batch parked on lane {gated} of {} must hold the lanes before it and no later one",
        canonical.len()
    );

    gate.commit().await.expect("release gate");
    let inserted = tokio::time::timeout(WAIT_DEADLINE, competing)
        .await
        .expect("producer finished")
        .expect("producer task")
        .expect("insert batch");

    // Lane ordering must not leak into the result: rows come back in input order.
    assert_eq!(inserted.len(), jobs.len());
    for (row, params) in inserted.iter().zip(&jobs) {
        assert_eq!(row.queue, params.opts.queue);
        assert_eq!(row.args, params.args);
    }

    // Within a lane, lane_seq follows input order.
    let lane_by_job: std::collections::HashMap<i64, (String, i16, i16, i64)> =
        sqlx::query_as::<_, (i64, String, i16, i16, i64)>(
            "SELECT job_id, queue, priority, enqueue_shard, lane_seq FROM awa.ready_entries WHERE job_id = ANY($1)",
        )
        .bind(inserted.iter().map(|row| row.id).collect::<Vec<_>>())
        .fetch_all(&pool)
        .await
        .expect("read lanes")
        .into_iter()
        .map(|(job_id, queue, priority, shard, lane_seq)| {
            (job_id, (queue, priority, shard, lane_seq))
        })
        .collect();
    assert_eq!(lane_by_job.len(), jobs.len());
    let mut last_seq_by_lane: std::collections::HashMap<(String, i16, i16), i64> =
        std::collections::HashMap::new();
    for row in &inserted {
        let (queue, priority, shard, lane_seq) = lane_by_job[&row.id].clone();
        if let Some(previous) = last_seq_by_lane.insert((queue, priority, shard), lane_seq) {
            assert_eq!(lane_seq, previous + 1, "lane_seq must follow input order");
        }
    }
}

#[tokio::test]
async fn insert_many_reserves_sharded_lanes_in_canonical_order() {
    assert_batch_reserves_lanes_in_canonical_order(Producer::InsertMany).await;
}

#[tokio::test]
async fn insert_many_copy_reserves_sharded_lanes_in_canonical_order() {
    assert_batch_reserves_lanes_in_canonical_order(Producer::CopySmall).await;
}

#[tokio::test]
async fn insert_many_copy_csv_reserves_sharded_lanes_in_canonical_order() {
    assert_batch_reserves_lanes_in_canonical_order(Producer::CopyLarge).await;
}
