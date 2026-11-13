//! Queue-stripe routing and claim coverage (#749).
//!
//! With `queue_stripe_count > 1` a logical queue `q` is stored as the
//! physical queues `q#0` … `q#N-1`. These tests pin down that:
//!
//! - jobs written to the unstriped queue `q` (the SQL compat insert path,
//!   or a producer configured with one stripe) are claimed and counted by a
//!   striped store;
//! - jobs that share an `ordering_key` land on one stripe and are claimed
//!   in insertion order;
//! - single-job inserts without a key spread across stripes;
//! - keys routed to one stripe still use every enqueue shard.

use awa_model::{
    cron, insert, migrations, storage, InsertOpts, InsertParams, JobArgs, PeriodicJob,
    QueueStorage, QueueStorageConfig,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::time::Duration;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn connect() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url())
        .await
        .expect("connect");
    migrations::run(&pool).await.expect("run migrations");
    pool
}

/// A freshly created database on the `DATABASE_URL` server, for a test that
/// changes the database-wide active storage engine. Returns the test pool and
/// a pool on the original database, which drops the test database with
/// [`drop_dedicated_database`].
async fn dedicated_database(name: &str) -> (PgPool, PgPool) {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url())
        .await
        .expect("connect");
    sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .expect("drop dedicated database");
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .expect("create dedicated database");
    let options = PgConnectOptions::from_str(&database_url())
        .expect("parse DATABASE_URL")
        .database(name);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .expect("connect to dedicated database");
    migrations::run(&pool).await.expect("run migrations");
    (pool, admin)
}

async fn drop_dedicated_database(pool: PgPool, admin: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .expect("drop dedicated database");
    admin.close().await;
}

fn config(schema: &str, stripes: usize) -> QueueStorageConfig {
    QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        queue_stripe_count: stripes,
        ..Default::default()
    }
}

async fn fresh_store(pool: &PgPool, schema: &str, stripes: usize) -> QueueStorage {
    let store = QueueStorage::new(config(schema, stripes)).expect("create queue storage");
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(pool)
        .await
        .expect("drop queue storage schema");
    store.prepare_schema(pool).await.expect("prepare schema");
    store.reset(pool).await.expect("reset queue storage");
    store
}

/// Make `schema` the active queue-storage engine, so the SQL compat insert
/// path (`awa.insert_job_compat`) writes into it.
async fn activate_queue_storage(pool: &PgPool, schema: &str) {
    storage::prepare(
        pool,
        "queue_storage",
        serde_json::json!({ "schema": schema }),
    )
    .await
    .expect("prepare queue storage transition");
    let gate = uuid::Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO awa.runtime_instances (
            instance_id, hostname, pid, version, storage_capability, transition_role,
            started_at, last_seen_at, snapshot_interval_ms, healthy, postgres_connected,
            poll_loop_alive, heartbeat_alive, maintenance_alive, shutting_down, leader,
            global_max_workers, queues, queue_descriptor_hashes, job_kind_descriptor_hashes)
        VALUES ($1, 'stripe-routing-test', 1, 'test', 'queue_storage', 'queue_storage_target',
            now(), now(), 10000, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE, TRUE, NULL,
            '[]'::jsonb, '{}'::jsonb, '{}'::jsonb)"#,
    )
    .bind(gate)
    .execute(pool)
    .await
    .expect("register queue-storage runtime");
    storage::enter_mixed_transition(pool)
        .await
        .expect("enter mixed transition");
    storage::finalize(pool).await.expect("finalize transition");
    sqlx::query("DELETE FROM awa.runtime_instances WHERE instance_id = $1")
        .bind(gate)
        .execute(pool)
        .await
        .expect("remove runtime instance");
}

fn job(queue: &str, seq: i64, ordering_key: Option<&str>) -> InsertParams {
    InsertParams {
        kind: "stripe_routing".into(),
        args: serde_json::json!({ "seq": seq }),
        opts: InsertOpts {
            queue: queue.to_string(),
            ordering_key: ordering_key.map(|key| key.as_bytes().to_vec()),
            ..Default::default()
        },
    }
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct StripeRoutingArgs {
    seq: i64,
}

/// `(seq, physical queue, enqueue_shard)` for every ready row, by `seq`.
async fn placement(pool: &PgPool, schema: &str) -> Vec<(i64, String, i16)> {
    sqlx::query_as(&format!(
        "SELECT (args->>'seq')::bigint, queue, enqueue_shard \
         FROM {schema}.ready_entries ORDER BY (args->>'seq')::bigint"
    ))
    .fetch_all(pool)
    .await
    .expect("read placement")
}

/// Claim until the queue is empty, returning `(seq, physical queue)` in claim
/// order.
async fn drain(
    pool: &PgPool,
    store: &QueueStorage,
    queue: &str,
    max_batch: i64,
) -> Vec<(i64, String)> {
    let mut claimed = Vec::new();
    for _ in 0..1_000 {
        let batch = store
            .claim_runtime_batch(pool, queue, max_batch, Duration::ZERO)
            .await
            .expect("claim");
        if batch.is_empty() {
            return claimed;
        }
        claimed.extend(batch.into_iter().map(|entry| {
            (
                entry.job.args["seq"].as_i64().expect("seq"),
                entry.job.queue.clone(),
            )
        }));
    }
    panic!("queue {queue} did not drain");
}

#[tokio::test]
async fn sql_path_jobs_are_claimed_by_a_striped_worker() {
    let database = "awa_stripe_routing_sql";
    let (pool, admin) = dedicated_database(database).await;
    let schema = "stripe_routing_sql";
    let store = fresh_store(&pool, schema, 2).await;
    activate_queue_storage(&pool, schema).await;
    let queue = "stripe_sql";

    insert::insert_with(
        &pool,
        &StripeRoutingArgs { seq: 1 },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .expect("insert_with");
    insert::insert_many(&pool, &[job(queue, 2, Some("K"))])
        .await
        .expect("single-row insert_many");
    insert::insert_many(
        &pool,
        &[
            job(queue, 3, None),
            job(queue, 4, None),
            job(queue, 5, Some("K")),
        ],
    )
    .await
    .expect("multi-row insert_many");
    let mut tx = pool.begin().await.expect("begin");
    insert::insert_many_copy(&mut tx, &[job(queue, 6, None), job(queue, 7, None)])
        .await
        .expect("transactional insert_many_copy");
    tx.commit().await.expect("commit");
    insert::insert_many_copy_from_pool(&pool, &[job(queue, 8, None), job(queue, 9, None)])
        .await
        .expect("insert_many_copy_from_pool");

    let periodic = PeriodicJob::builder("stripe_routing_cron", "* * * * *")
        .queue(queue)
        .build_raw("stripe_routing".into(), serde_json::json!({ "seq": 10 }))
        .expect("build periodic job");
    cron::upsert_cron_job(&pool, &periodic)
        .await
        .expect("upsert cron job");
    cron::trigger_cron_job(&pool, "stripe_routing_cron")
        .await
        .expect("trigger cron job");

    let rows = placement(&pool, schema).await;
    assert_eq!(
        rows.len(),
        10,
        "every SQL-path insert reached queue storage"
    );
    assert!(
        rows.iter().all(|(_, physical, _)| physical == queue),
        "the SQL compat path writes the unstriped logical queue: {rows:?}"
    );

    let counts = store
        .queue_counts(&pool, queue)
        .await
        .expect("queue counts");
    assert_eq!(
        counts.available, 10,
        "a striped store counts jobs in the unstriped queue"
    );

    // The query the CHANGELOG gives operators for finding these jobs.
    let unstriped: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT queue, state::text, count(*) FROM awa.jobs \
         WHERE state IN ('available', 'scheduled', 'retryable') AND strpos(queue, '#') = 0 \
         GROUP BY queue, state",
    )
    .fetch_all(&pool)
    .await
    .expect("list jobs in unstriped queues");
    assert_eq!(
        unstriped,
        vec![(queue.to_string(), "available".to_string(), 10)]
    );

    let claimed = drain(&pool, &store, queue, 4).await;
    let seqs: BTreeSet<i64> = claimed.iter().map(|(seq, _)| *seq).collect();
    assert_eq!(
        seqs,
        (1..=10).collect::<BTreeSet<_>>(),
        "a striped worker claims every SQL-path job: {claimed:?}"
    );

    drop_dedicated_database(pool, admin, database).await;
}

#[tokio::test]
async fn unstriped_producer_and_striped_worker_share_a_queue() {
    let pool = connect().await;
    let schema = "stripe_routing_mixed";
    let worker = fresh_store(&pool, schema, 2).await;
    let producer = QueueStorage::new(config(schema, 1)).expect("one-stripe producer");
    let queue = "stripe_mixed";

    worker
        .prepare_queue(&pool, queue, 1)
        .await
        .expect("prepare queue");
    let lanes: BTreeSet<String> = sqlx::query_scalar::<_, String>(&format!(
        "SELECT DISTINCT queue FROM {schema}.queue_lanes WHERE queue LIKE 'stripe\\_mixed%'"
    ))
    .fetch_all(&pool)
    .await
    .expect("read lanes")
    .into_iter()
    .collect();
    assert_eq!(
        lanes,
        BTreeSet::from([
            queue.to_string(),
            format!("{queue}#0"),
            format!("{queue}#1"),
        ]),
        "prepare_queue provisions the unstriped queue and every stripe"
    );

    producer
        .enqueue_params_batch(&pool, &[job(queue, 1, None), job(queue, 2, Some("K"))])
        .await
        .expect("one-stripe enqueue");
    producer
        .enqueue_params_copy(&pool, &[job(queue, 3, None)])
        .await
        .expect("one-stripe copy enqueue");
    worker
        .enqueue_params_batch(&pool, &[job(queue, 4, None), job(queue, 5, None)])
        .await
        .expect("two-stripe enqueue");

    let counts = worker.queue_counts(&pool, queue).await.expect("counts");
    assert_eq!(counts.available, 5);

    let claimed = drain(&pool, &worker, queue, 1).await;
    let seqs: BTreeSet<i64> = claimed.iter().map(|(seq, _)| *seq).collect();
    assert_eq!(
        seqs,
        (1..=5).collect::<BTreeSet<_>>(),
        "the two-stripe worker claims jobs from both producers: {claimed:?}"
    );
}

#[tokio::test]
async fn ordering_key_pins_a_key_to_one_stripe() {
    let pool = connect().await;
    for (name, use_copy) in [("batch", false), ("copy", true)] {
        let schema = format!("stripe_routing_key_{name}");
        let store = fresh_store(&pool, &schema, 4).await;
        let queue = "stripe_key";
        let enqueue = |jobs: Vec<InsertParams>| {
            let store = &store;
            let pool = &pool;
            async move {
                if use_copy {
                    store.enqueue_params_copy(pool, &jobs).await
                } else {
                    store.enqueue_params_batch(pool, &jobs).await
                }
                .expect("enqueue");
            }
        };
        for seq in 1..=4 {
            enqueue(vec![job(queue, seq, Some("K"))]).await;
        }
        enqueue((5..=12).map(|seq| job(queue, seq, Some("K"))).collect()).await;
        enqueue(vec![
            job(queue, 100, Some("OTHER")),
            job(queue, 13, Some("K")),
        ])
        .await;

        let rows = placement(&pool, &schema).await;
        let key_stripes: BTreeSet<&str> = rows
            .iter()
            .filter(|(seq, _, _)| *seq != 100)
            .map(|(_, physical, _)| physical.as_str())
            .collect();
        assert_eq!(
            key_stripes.len(),
            1,
            "enqueue_params_{name}: every job with key K is on one stripe: {rows:?}"
        );
    }
}

#[tokio::test]
async fn ordering_key_claim_order_survives_striping() {
    let pool = connect().await;
    let queue = "stripe_order";
    for rotor_advance in 0..3 {
        let schema = format!("stripe_routing_order_{rotor_advance}");
        let store = fresh_store(&pool, &schema, 2).await;
        store
            .enqueue_params_batch(&pool, &[job(queue, 1, Some("K"))])
            .await
            .expect("enqueue K1");
        store
            .enqueue_params_batch(
                &pool,
                &[job(queue, 100, Some("OTHER")), job(queue, 2, Some("K"))],
            )
            .await
            .expect("enqueue OTHER, K2");
        store
            .enqueue_params_batch(&pool, &[job(queue, 3, Some("K")), job(queue, 4, Some("K"))])
            .await
            .expect("enqueue K3, K4");

        // Each claim on an empty queue moves the stripe probe start by one.
        for _ in 0..rotor_advance {
            store
                .claim_runtime_batch(&pool, "stripe_order_empty", 1, Duration::ZERO)
                .await
                .expect("advance probe start");
        }
        let claimed = drain(&pool, &store, queue, 1).await;
        let key_order: Vec<i64> = claimed
            .iter()
            .map(|(seq, _)| *seq)
            .filter(|seq| *seq != 100)
            .collect();
        assert_eq!(
            key_order,
            vec![1, 2, 3, 4],
            "key K is claimed in insertion order (probe start advanced {rotor_advance}): {claimed:?}"
        );
    }
}

#[tokio::test]
async fn single_job_inserts_spread_across_stripes() {
    let pool = connect().await;
    let schema = "stripe_routing_spread";
    let store = fresh_store(&pool, schema, 4).await;
    let queue = "stripe_spread";
    for seq in 0..40 {
        store
            .enqueue_params_batch(&pool, &[job(queue, seq, None)])
            .await
            .expect("single enqueue");
    }
    for seq in 40..80 {
        store
            .enqueue_params_copy(&pool, &[job(queue, seq, None)])
            .await
            .expect("single copy enqueue");
    }
    let per_stripe: Vec<(String, i64)> = sqlx::query_as(&format!(
        "SELECT queue, count(*) FROM {schema}.ready_entries GROUP BY queue ORDER BY queue"
    ))
    .fetch_all(&pool)
    .await
    .expect("count per stripe");
    assert_eq!(
        per_stripe,
        (0..4)
            .map(|stripe| (format!("{queue}#{stripe}"), 20))
            .collect::<Vec<_>>(),
        "single-job inserts are spread evenly across the four stripes"
    );
}

#[tokio::test]
async fn short_ordering_keys_spread_across_stripes() {
    let pool = connect().await;
    let schema = "stripe_routing_short_keys";
    let store = fresh_store(&pool, schema, 4).await;
    let queue = "stripe_short_keys";
    let jobs: Vec<_> = (0..200)
        .map(|seq| job(queue, seq, Some(&seq.to_string())))
        .collect();
    store
        .enqueue_params_batch(&pool, &jobs)
        .await
        .expect("keyed enqueue");

    let mut per_stripe: BTreeMap<String, usize> = BTreeMap::new();
    let mut single_byte_stripes = BTreeSet::new();
    for (seq, physical, _) in placement(&pool, schema).await {
        if seq < 10 {
            single_byte_stripes.insert(physical.clone());
        }
        *per_stripe.entry(physical).or_default() += 1;
    }
    assert_eq!(
        per_stripe.keys().cloned().collect::<Vec<_>>(),
        (0..4)
            .map(|stripe| format!("{queue}#{stripe}"))
            .collect::<Vec<_>>(),
        "decimal keys use every stripe"
    );
    for (stripe, count) in &per_stripe {
        assert!(
            (35..=65).contains(count),
            "{stripe} holds {count} of 200 decimal keys"
        );
    }
    assert!(
        single_byte_stripes.len() > 1,
        "single-byte keys use more than one stripe: {single_byte_stripes:?}"
    );
}

#[tokio::test]
async fn equal_length_prefixed_keys_spread_across_stripes() {
    let pool = connect().await;
    let schema = "stripe_routing_prefixed_keys";
    let store = fresh_store(&pool, schema, 4).await;
    let queue = "stripe_prefixed_keys";
    let jobs: Vec<_> = (1..=100)
        .map(|seq| job(queue, seq, Some(&format!("tenant-{seq}"))))
        .collect();
    store
        .enqueue_params_batch(&pool, &jobs)
        .await
        .expect("keyed enqueue");

    let mut per_stripe: BTreeMap<String, usize> = BTreeMap::new();
    for (_, physical, _) in placement(&pool, schema).await {
        *per_stripe.entry(physical).or_default() += 1;
    }
    assert_eq!(
        per_stripe.len(),
        4,
        "prefixed keys use every stripe: {per_stripe:?}"
    );
    for (stripe, count) in &per_stripe {
        assert!(
            (15..=35).contains(count),
            "{stripe} holds {count} of 100 prefixed keys"
        );
    }
}

#[tokio::test]
async fn keys_on_one_stripe_still_use_every_enqueue_shard() {
    let pool = connect().await;
    let schema = "stripe_routing_shards";
    let store = fresh_store(&pool, schema, 2).await;
    let queue = "stripe_shards";
    for physical in [
        queue.to_string(),
        format!("{queue}#0"),
        format!("{queue}#1"),
    ] {
        sqlx::query(
            "INSERT INTO awa.queue_meta (queue, enqueue_shards) VALUES ($1, 4) \
             ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards",
        )
        .bind(&physical)
        .execute(&pool)
        .await
        .expect("configure enqueue shards");
    }

    let jobs: Vec<_> = (0..64)
        .map(|seq| job(queue, seq, Some(&format!("key-{seq}"))))
        .collect();
    for chunk in jobs.chunks(1) {
        store
            .enqueue_params_batch(&pool, chunk)
            .await
            .expect("keyed enqueue");
    }

    let mut shards_by_stripe: BTreeMap<String, BTreeSet<i16>> = BTreeMap::new();
    for (_, physical, shard) in placement(&pool, schema).await {
        shards_by_stripe.entry(physical).or_default().insert(shard);
    }
    let all_shards: BTreeSet<i16> = (0..4).collect();
    assert_eq!(
        shards_by_stripe.keys().cloned().collect::<Vec<_>>(),
        vec![format!("{queue}#0"), format!("{queue}#1")],
        "keyed jobs use both stripes"
    );
    for (stripe, shards) in &shards_by_stripe {
        assert_eq!(
            shards, &all_shards,
            "keys routed to {stripe} use every enqueue shard"
        );
    }

    sqlx::query("DELETE FROM awa.queue_meta WHERE queue LIKE 'stripe\\_shards%'")
        .execute(&pool)
        .await
        .expect("clean queue_meta");
}
