//! Lane generation order under non-READ COMMITTED producers (#847).
//!
//! Every enqueue reserves its lane position through `reserve_enqueue_seq`,
//! which waits on the lane's transaction advisory lock, and then reads the
//! queue ring cursor to choose the `(ready_slot, ready_generation)` its rows go
//! into. Under REPEATABLE READ or SERIALIZABLE that cursor read uses the
//! transaction snapshot, which can predate a ring rotation, so a later lane
//! position can land in an older generation than an earlier one.
//!
//! These tests build that layout through the real producer entry points and
//! then assert the queue-storage invariant: every committed job is claimed,
//! and prune never reclaims an unclaimed one. A producer may instead fail with
//! a retryable serialization failure (SQLSTATE 40001), which the test retries
//! the way an application retry loop would.

use awa_model::{
    audited_sql, insert, migrations, storage, AwaError, InsertOpts, InsertParams, QueueStorage,
    QueueStorageConfig,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::BTreeMap;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const SCHEMA: &str = "gen_order";
const QUEUE: &str = "gen_order_q";

#[derive(Clone, Copy, Debug)]
enum Isolation {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl Isolation {
    fn sql(self) -> &'static str {
        match self {
            Isolation::ReadCommitted => "READ COMMITTED",
            Isolation::RepeatableRead => "REPEATABLE READ",
            Isolation::Serializable => "SERIALIZABLE",
        }
    }
}

/// Which producer entry point writes the stale-snapshot job.
#[derive(Clone, Copy, Debug)]
enum Producer {
    /// `insert::insert_many`, i.e. `awa.insert_job_compat`.
    InsertJobCompat,
    /// `insert::insert_many_copy`, i.e. `awa.insert_jobs_compat_batch`.
    InsertJobsCompatBatch,
}

fn config() -> QueueStorageConfig {
    QueueStorageConfig {
        schema: SCHEMA.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    }
}

/// Install a fresh queue-storage schema and make it the active engine, so the
/// SQL compat producers (`awa.insert_job_compat`, `awa.insert_jobs_compat_batch`)
/// write into it.
async fn setup(pool: &PgPool) -> Result<QueueStorage, Box<dyn std::error::Error>> {
    migrations::run(pool).await?;
    let store = QueueStorage::new(config())?;
    store.prepare_schema(pool).await?;
    store.reset(pool).await?;
    storage::prepare(
        pool,
        "queue_storage",
        serde_json::json!({ "schema": SCHEMA }),
    )
    .await?;
    let gate = uuid::Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO awa.runtime_instances (
            instance_id, hostname, pid, version, storage_capability, transition_role,
            started_at, last_seen_at, snapshot_interval_ms, healthy, postgres_connected,
            poll_loop_alive, heartbeat_alive, maintenance_alive, shutting_down, leader,
            global_max_workers, queues, queue_descriptor_hashes, job_kind_descriptor_hashes)
        VALUES ($1, 'gen-order-test', 1, 'test', 'queue_storage', 'queue_storage_target',
            now(), now(), 10000, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE, TRUE, NULL,
            '[]'::jsonb, '{}'::jsonb, '{}'::jsonb)"#,
    )
    .bind(gate)
    .execute(pool)
    .await?;
    storage::enter_mixed_transition(pool).await?;
    storage::finalize(pool).await?;
    sqlx::query("DELETE FROM awa.runtime_instances WHERE instance_id = $1")
        .bind(gate)
        .execute(pool)
        .await?;
    Ok(store)
}

fn job(label: &str) -> InsertParams {
    InsertParams {
        kind: "gen_order".into(),
        args: serde_json::json!({ "label": label }),
        opts: InsertOpts {
            queue: QUEUE.to_string(),
            ..Default::default()
        },
    }
}

async fn produce(
    conn: &mut sqlx::PgConnection,
    producer: Producer,
    label: &str,
) -> Result<i64, AwaError> {
    let rows = match producer {
        Producer::InsertJobCompat => insert::insert_many(&mut *conn, &[job(label)]).await?,
        Producer::InsertJobsCompatBatch => insert::insert_many_copy(conn, &[job(label)]).await?,
    };
    Ok(rows[0].id)
}

fn is_serialization_failure(err: &AwaError) -> bool {
    match err {
        AwaError::Database(sqlx::Error::Database(db)) => db.code().as_deref() == Some("40001"),
        _ => false,
    }
}

/// `label -> (lane_seq, ready_slot, ready_generation, job_id)` for every
/// ready row.
async fn placement(pool: &PgPool) -> Result<BTreeMap<String, (i64, i32, i64, i64)>, sqlx::Error> {
    let rows: Vec<(String, i64, i32, i64, i64)> = sqlx::query_as(audited_sql(format!(
        "SELECT args->>'label', lane_seq, ready_slot, ready_generation, job_id
         FROM {SCHEMA}.ready_entries WHERE queue = $1 ORDER BY lane_seq"
    )))
    .bind(QUEUE)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(label, lane, slot, generation, id)| (label, (lane, slot, generation, id)))
        .collect())
}

async fn claim_cursor(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(audited_sql(format!(
        "SELECT {SCHEMA}.sequence_next_value(seq_name) FROM {SCHEMA}.queue_claim_heads
         WHERE queue = $1"
    )))
    .bind(QUEUE)
    .fetch_one(pool)
    .await
}

/// Claim and complete until a claim returns nothing; the labels claimed, in
/// claim order.
async fn claim_and_complete_all(
    pool: &PgPool,
    store: &QueueStorage,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut claimed = Vec::new();
    for _ in 0..20 {
        let batch = store
            .claim_runtime_batch(pool, QUEUE, 10, Duration::ZERO)
            .await?;
        if batch.is_empty() {
            break;
        }
        claimed.extend(
            batch
                .iter()
                .map(|entry| entry.job.args["label"].as_str().unwrap().to_string()),
        );
        store.complete_runtime_batch(pool, &batch).await?;
    }
    Ok(claimed)
}

async fn begin_with(
    pool: &PgPool,
    isolation: Isolation,
) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(audited_sql(format!(
        "SET TRANSACTION ISOLATION LEVEL {}",
        isolation.sql()
    )))
    .execute(&mut *tx)
    .await?;
    Ok(tx)
}

struct Outcome {
    placement: BTreeMap<String, (i64, i32, i64, i64)>,
    claimed: Vec<String>,
    cursor_after_claims: i64,
    serialization_retries: usize,
    prune: Vec<String>,
    surviving_after_prune: Vec<String>,
    /// `label -> state` from `QueueStorage::load_job` after prune, or
    /// "missing".
    states_after_prune: BTreeMap<String, String>,
}

/// The #847 interleaving:
///
/// ```text
/// z (RC):  enqueue, claim, complete       -- lane 1, generation 0
/// T1 (iso): BEGIN; SELECT 1               -- snapshot: ring at generation 0
///          rotate                         -- ring at generation 1
/// a (RC):  enqueue                        -- lane 2, generation 1
/// T1:      enqueue b; COMMIT              -- lane 3, generation 0 (stale)
/// c (RC):  enqueue                        -- lane 4, generation 1
/// ```
///
/// then claims and completes until the queue looks empty, and prunes the
/// oldest slot. A serialization failure on T1 is retried in a fresh
/// transaction of the same isolation.
async fn early_snapshot_scenario(
    pool: &PgPool,
    store: &QueueStorage,
    isolation: Isolation,
    producer: Producer,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let serialization_retries =
        build_early_snapshot_layout(pool, store, isolation, producer).await?;
    finish(pool, store, serialization_retries).await
}

/// Builds the layout of [`early_snapshot_scenario`] and returns how many
/// serialization failures the stale producer retried.
async fn build_early_snapshot_layout(
    pool: &PgPool,
    store: &QueueStorage,
    isolation: Isolation,
    producer: Producer,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut conn = pool.acquire().await?;
    produce(&mut conn, Producer::InsertJobCompat, "z").await?;
    drop(conn);
    assert_eq!(claim_and_complete_all(pool, store).await?, vec!["z"]);

    let mut t1 = begin_with(pool, isolation).await?;
    sqlx::query("SELECT 1").execute(&mut *t1).await?;

    let rotated = store.rotate(pool).await?;
    eprintln!("rotate: {rotated:?}");

    let mut conn = pool.acquire().await?;
    produce(&mut conn, Producer::InsertJobCompat, "a").await?;
    drop(conn);

    let mut serialization_retries = 0;
    loop {
        match produce(&mut t1, producer, "b").await {
            Ok(_) => match t1.commit().await {
                Ok(()) => break,
                Err(err) => {
                    let err = AwaError::Database(err);
                    if is_serialization_failure(&err) && serialization_retries < 3 {
                        serialization_retries += 1;
                        eprintln!("b: commit failed with 40001, retrying: {err}");
                        t1 = begin_with(pool, isolation).await?;
                        continue;
                    }
                    return Err(err.into());
                }
            },
            Err(err) if is_serialization_failure(&err) && serialization_retries < 3 => {
                serialization_retries += 1;
                eprintln!("b: enqueue failed with 40001, retrying: {err}");
                t1.rollback().await?;
                t1 = begin_with(pool, isolation).await?;
            }
            Err(err) => return Err(err.into()),
        }
    }

    let mut conn = pool.acquire().await?;
    produce(&mut conn, Producer::InsertJobCompat, "c").await?;
    drop(conn);

    Ok(serialization_retries)
}

/// Claims, completes, prunes and reports.
async fn finish(
    pool: &PgPool,
    store: &QueueStorage,
    serialization_retries: usize,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let placement = placement(pool).await?;
    let claimed = claim_and_complete_all(pool, store).await?;
    let cursor_after_claims = claim_cursor(pool).await?;

    let mut prune = Vec::new();
    for _ in 0..3 {
        let outcome = store.prune_oldest(pool, Duration::ZERO).await?;
        prune.push(format!("{outcome:?}"));
    }
    let surviving_after_prune: Vec<String> = sqlx::query_scalar(audited_sql(format!(
        "SELECT args->>'label' FROM {SCHEMA}.ready_entries WHERE queue = $1 ORDER BY lane_seq"
    )))
    .bind(QUEUE)
    .fetch_all(pool)
    .await?;
    let mut states_after_prune = BTreeMap::new();
    for (label, (_, _, _, id)) in &placement {
        let state = match store.load_job(pool, *id).await? {
            Some(job) => format!("{:?}", job.state),
            None => "missing".to_string(),
        };
        states_after_prune.insert(label.clone(), state);
    }

    Ok(Outcome {
        placement,
        claimed,
        cursor_after_claims,
        serialization_retries,
        prune,
        surviving_after_prune,
        states_after_prune,
    })
}

fn report_and_check(name: &str, outcome: &Outcome) {
    eprintln!("== {name}");
    eprintln!("placement (label -> lane_seq, slot, generation):");
    for (label, (lane, slot, generation, _)) in &outcome.placement {
        eprintln!("  {label}: lane {lane}, slot {slot}, generation {generation}");
    }
    eprintln!("claimed after the setup job: {:?}", outcome.claimed);
    eprintln!("claim cursor after claims: {}", outcome.cursor_after_claims);
    eprintln!("serialization retries: {}", outcome.serialization_retries);
    eprintln!("prune: {:?}", outcome.prune);
    eprintln!(
        "ready rows left after prune: {:?}",
        outcome.surviving_after_prune
    );
    eprintln!("job states after prune: {:?}", outcome.states_after_prune);

    // A completed job's rows go with its pruned slot; an unclaimed job's must
    // not.
    let lost: Vec<&String> = outcome
        .states_after_prune
        .iter()
        .filter(|(label, state)| {
            *state == "missing" && *label != "z" && !outcome.claimed.contains(label)
        })
        .map(|(label, _)| label)
        .collect();
    assert!(
        lost.is_empty(),
        "{name}: prune deleted never-claimed jobs {lost:?}"
    );

    let mut claimed = outcome.claimed.clone();
    claimed.sort();
    assert_eq!(
        claimed,
        vec!["a", "b", "c"],
        "{name}: every committed job is claimed"
    );

    // Generations never go backwards in lane order.
    let mut by_lane: Vec<(i64, i64)> = outcome
        .placement
        .values()
        .map(|(lane, _, generation, _)| (*lane, *generation))
        .collect();
    by_lane.sort();
    assert!(
        by_lane.windows(2).all(|pair| pair[0].1 <= pair[1].1),
        "{name}: generations are non-decreasing in lane order: {by_lane:?}"
    );
}

#[sqlx::test(migrations = false)]
async fn read_committed_producer_keeps_lane_generation_order(pool: PgPool) -> TestResult {
    let store = setup(&pool).await?;
    let outcome = early_snapshot_scenario(
        &pool,
        &store,
        Isolation::ReadCommitted,
        Producer::InsertJobCompat,
    )
    .await?;
    report_and_check("read committed, insert_job_compat", &outcome);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn repeatable_read_insert_job_compat_keeps_every_job(pool: PgPool) -> TestResult {
    let store = setup(&pool).await?;
    let outcome = early_snapshot_scenario(
        &pool,
        &store,
        Isolation::RepeatableRead,
        Producer::InsertJobCompat,
    )
    .await?;
    report_and_check("repeatable read, insert_job_compat", &outcome);
    Ok(())
}

/// Puts the schema back in compat (`columns`) ring authority, as an upgraded
/// 0.6 schema starts, so the queue-ring cursor is the mutable singleton row
/// rotation updates in place.
async fn use_columns_authority(pool: &PgPool) -> TestResult {
    sqlx::query(audited_sql(format!(
        "UPDATE {SCHEMA}.ring_cursor_authority SET authority = 'columns', flipped_at = NULL"
    )))
    .execute(pool)
    .await?;
    // A fresh install poisons the compat columns; restore them from the
    // ledgers.
    for ring in ["queue", "lease", "claim"] {
        sqlx::raw_sql(audited_sql(format!(
            "UPDATE {SCHEMA}.{ring}_ring_state AS s
             SET current_slot = l.slot, generation = l.generation
             FROM (SELECT slot, generation FROM {SCHEMA}.{ring}_ring_rotations
                   ORDER BY generation DESC LIMIT 1) AS l
             WHERE s.singleton;
             UPDATE {SCHEMA}.{ring}_ring_slots AS slots
             SET generation = CASE WHEN slots.slot = s.current_slot THEN s.generation ELSE -1 END
             FROM {SCHEMA}.{ring}_ring_state AS s WHERE s.singleton;"
        )))
        .execute(pool)
        .await?;
    }
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn repeatable_read_insert_job_compat_keeps_every_job_columns_authority(
    pool: PgPool,
) -> TestResult {
    let store = setup(&pool).await?;
    use_columns_authority(&pool).await?;
    let outcome = early_snapshot_scenario(
        &pool,
        &store,
        Isolation::RepeatableRead,
        Producer::InsertJobCompat,
    )
    .await?;
    report_and_check(
        "repeatable read, insert_job_compat, columns authority",
        &outcome,
    );
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn serializable_insert_job_compat_keeps_every_job(pool: PgPool) -> TestResult {
    let store = setup(&pool).await?;
    let outcome = early_snapshot_scenario(
        &pool,
        &store,
        Isolation::Serializable,
        Producer::InsertJobCompat,
    )
    .await?;
    report_and_check("serializable, insert_job_compat", &outcome);
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn repeatable_read_insert_jobs_compat_batch_keeps_every_job(pool: PgPool) -> TestResult {
    let store = setup(&pool).await?;
    let outcome = early_snapshot_scenario(
        &pool,
        &store,
        Isolation::RepeatableRead,
        Producer::InsertJobsCompatBatch,
    )
    .await?;
    report_and_check("repeatable read, insert_jobs_compat_batch", &outcome);
    Ok(())
}

/// Waits until some backend is blocked on an advisory lock.
async fn wait_for_advisory_waiter(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..500 {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
             WHERE datname = current_database()
               AND wait_event_type = 'Lock' AND wait_event = 'advisory')",
        )
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err("no producer blocked on the lane lock".into())
}

/// The same layout with no early statement: the stale producer's snapshot is
/// taken by the enqueue statement itself, before it waits for the lane lock.
/// The producer is the Rust `QueueStorage::enqueue_params_batch`, on a pool
/// whose sessions default to REPEATABLE READ
/// (`default_transaction_isolation`), so `pool.begin()` inside the store is
/// REPEATABLE READ.
///
/// ```text
/// z (RC):  enqueue, claim, complete       -- lane 1, generation 0
/// H (RC):  BEGIN; reserve_enqueue_seq     -- holds the lane lock (lane 2)
/// P (RR):  enqueue_params_batch(b)        -- snapshot at generation 0, waits
///          rotate                         -- ring at generation 1
/// H:       enqueue a; COMMIT              -- lane 3, generation 1
/// P:       (lock granted)                 -- lane 4, generation 0 (stale)
/// c (RC):  enqueue                        -- lane 5, generation 1
/// ```
#[sqlx::test(migrations = false)]
async fn repeatable_read_rust_enqueue_after_lock_wait_keeps_every_job(pool: PgPool) -> TestResult {
    let store = setup(&pool).await?;
    let rr_options: PgConnectOptions = pool
        .connect_options()
        .as_ref()
        .clone()
        .options([("default_transaction_isolation", "repeatable read")]);
    let rr_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(rr_options)
        .await?;

    let mut conn = pool.acquire().await?;
    produce(&mut conn, Producer::InsertJobCompat, "z").await?;
    drop(conn);
    assert_eq!(claim_and_complete_all(&pool, &store).await?, vec!["z"]);

    let mut holder = pool.begin().await?;
    sqlx::query(audited_sql(format!(
        "SELECT {SCHEMA}.reserve_enqueue_seq($1, 2::smallint, 0::smallint, 1)"
    )))
    .bind(QUEUE)
    .execute(&mut *holder)
    .await?;

    let rr_store = QueueStorage::new(config())?;
    let producer = tokio::spawn({
        let rr_pool = rr_pool.clone();
        async move {
            let mut retries = 0;
            loop {
                match rr_store.enqueue_params_batch(&rr_pool, &[job("b")]).await {
                    Ok(_) => return Ok::<usize, AwaError>(retries),
                    Err(err) if is_serialization_failure(&err) && retries < 3 => {
                        eprintln!("b: enqueue failed with 40001, retrying: {err}");
                        retries += 1;
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    });
    wait_for_advisory_waiter(&pool).await?;

    let rotated = store.rotate(&pool).await?;
    eprintln!("rotate: {rotated:?}");
    produce(&mut holder, Producer::InsertJobCompat, "a").await?;
    holder.commit().await?;

    let serialization_retries = producer.await??;

    let mut conn = pool.acquire().await?;
    produce(&mut conn, Producer::InsertJobCompat, "c").await?;
    drop(conn);

    let outcome = finish(&pool, &store, serialization_retries).await?;
    report_and_check(
        "repeatable read session default, Rust enqueue_params_batch after a lane-lock wait",
        &outcome,
    );
    Ok(())
}

const OTHER_QUEUE: &str = "gen_order_other";

/// Rotates the queue ring until its generation is `to_generation`, pruning
/// the oldest slot before each rotation, and returns the generation reached:
/// lower when a slot that still holds unclaimed work stops the ring. A filler
/// job in another queue, claimed and completed at once, keeps the ring from
/// idling.
async fn advance_ring(
    pool: &PgPool,
    store: &QueueStorage,
    to_generation: i64,
) -> Result<i64, Box<dyn std::error::Error>> {
    loop {
        let generation: i64 = sqlx::query_scalar(audited_sql(format!(
            "SELECT generation FROM {SCHEMA}.ring_cursor('queue')"
        )))
        .fetch_one(pool)
        .await?;
        if generation >= to_generation {
            return Ok(generation);
        }
        let mut filler = job("filler");
        filler.opts.queue = OTHER_QUEUE.to_string();
        insert::insert_many(pool, &[filler]).await?;
        loop {
            let batch = store
                .claim_runtime_batch(pool, OTHER_QUEUE, 10, Duration::ZERO)
                .await?;
            if batch.is_empty() {
                break;
            }
            store.complete_runtime_batch(pool, &batch).await?;
        }
        let pruned = store.prune_oldest(pool, Duration::ZERO).await?;
        let rotated = store.rotate(pool).await?;
        eprintln!("generation {generation}: prune {pruned:?}; rotate {rotated:?}");
        if !matches!(rotated, awa_model::RotateOutcome::Rotated { .. }) {
            return Ok(generation);
        }
    }
}

/// A snapshot older than a whole ring cycle: the producer writes into a
/// generation whose slot has since been pruned and reused. The lane order is
/// fine (the job is alone in its lane), but prune checks the slot's current
/// generation only, so the job is deleted before any worker claims it, for
/// example while its queue is paused.
#[sqlx::test(migrations = false)]
async fn repeatable_read_snapshot_older_than_the_ring_keeps_the_job(pool: PgPool) -> TestResult {
    let store = setup(&pool).await?;

    let mut t1 = begin_with(&pool, Isolation::RepeatableRead).await?;
    sqlx::query("SELECT 1").execute(&mut *t1).await?;
    // Four slots: generation 4 reuses slot 0, which held generation 0.
    assert_eq!(advance_ring(&pool, &store, 4).await?, 4);

    let mut serialization_retries = 0;
    let b_id = loop {
        match produce(&mut t1, Producer::InsertJobCompat, "b").await {
            Ok(id) => {
                t1.commit().await?;
                break id;
            }
            Err(err) if is_serialization_failure(&err) && serialization_retries < 3 => {
                serialization_retries += 1;
                eprintln!("b: enqueue failed with 40001, retrying: {err}");
                t1.rollback().await?;
                t1 = begin_with(&pool, Isolation::RepeatableRead).await?;
            }
            Err(err) => return Err(err.into()),
        }
    };
    eprintln!("placement: {:?}", placement(&pool).await?);

    // Nobody claims QUEUE while the ring turns once more. Rotation may stop
    // at b's slot while b is unclaimed in it, which is correct.
    let reached = advance_ring(&pool, &store, 8).await?;
    eprintln!("ring reached generation {reached}");
    let state = store
        .load_job(&pool, b_id)
        .await?
        .map(|job| format!("{:?}", job.state));
    eprintln!("b after another ring cycle: {state:?}");
    let claimed = claim_and_complete_all(&pool, &store).await?;
    eprintln!("claimed: {claimed:?}");
    assert_eq!(state.as_deref(), Some("Available"), "b survives prune");
    assert_eq!(claimed, vec!["b"], "b is claimed");
    Ok(())
}

/// After `QueueStorage::reset` the ring is back at generation 0, and so is
/// where enqueues write.
#[sqlx::test(migrations = false)]
async fn enqueue_after_reset_writes_generation_zero(pool: PgPool) -> TestResult {
    let store = setup(&pool).await?;
    assert_eq!(advance_ring(&pool, &store, 3).await?, 3);
    store.reset(&pool).await?;
    let mut conn = pool.acquire().await?;
    produce(&mut conn, Producer::InsertJobCompat, "after_reset").await?;
    drop(conn);
    let placement = placement(&pool).await?;
    assert_eq!(placement["after_reset"].2, 0, "{placement:?}");
    Ok(())
}

/// Removes the generation floor, so producers write where they did before
/// v062, and returns a store that has not yet seen the floor.
async fn without_generation_floor(
    pool: &PgPool,
) -> Result<QueueStorage, Box<dyn std::error::Error>> {
    sqlx::raw_sql(audited_sql(format!(
        "DROP FUNCTION {SCHEMA}.enqueue_ring_cursor();
         DROP SEQUENCE {SCHEMA}.queue_ring_generation_floor;"
    )))
    .execute(pool)
    .await?;
    Ok(QueueStorage::new(config())?)
}

/// A lane a REPEATABLE READ producer inverted before v062: b's later lane
/// position is in the older generation. `admin::drain_queue` moves the claim
/// cursor with a position scan across every segment of the lane, whatever its
/// generation, so it finds and cancels b too.
#[sqlx::test(migrations = false)]
async fn drain_cancels_a_job_in_an_older_generation(pool: PgPool) -> TestResult {
    setup(&pool).await?;
    let store = without_generation_floor(&pool).await?;
    build_early_snapshot_layout(
        &pool,
        &store,
        Isolation::RepeatableRead,
        Producer::InsertJobCompat,
    )
    .await?;
    let placement = placement(&pool).await?;
    eprintln!("placement: {placement:?}");
    assert!(
        placement["b"].2 < placement["a"].2,
        "b is in the older generation: {placement:?}"
    );
    let drained = awa_model::admin::drain_queue(&pool, QUEUE).await?;
    let mut states = BTreeMap::new();
    for (label, (_, _, _, id)) in &placement {
        let job = store.load_job(&pool, *id).await?.expect("job");
        states.insert(label.clone(), format!("{:?}", job.state));
    }
    eprintln!("drained {drained}: {states:?}");
    assert_eq!(drained, 3, "a, b and c are drained");
    assert_eq!(states["b"], "Cancelled");
    let next_seq: i64 = sqlx::query_scalar(audited_sql(format!(
        "SELECT {SCHEMA}.sequence_next_value(seq_name) FROM {SCHEMA}.queue_enqueue_heads
         WHERE queue = $1"
    )))
    .bind(QUEUE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        claim_cursor(&pool).await?,
        next_seq,
        "the cursor ends past c"
    );
    assert!(claim_and_complete_all(&pool, &store).await?.is_empty());
    Ok(())
}

/// Before v062 a snapshot older than a whole ring cycle wrote b into a slot the
/// ring had since reused, under the slot's old generation. Prune counts b as
/// pending whatever its generation, so the ring stops at b's slot instead of
/// deleting b.
#[sqlx::test(migrations = false)]
async fn prune_keeps_an_unclaimed_job_in_an_older_generation(pool: PgPool) -> TestResult {
    setup(&pool).await?;
    let store = without_generation_floor(&pool).await?;

    let mut t1 = begin_with(&pool, Isolation::RepeatableRead).await?;
    sqlx::query("SELECT 1").execute(&mut *t1).await?;
    assert_eq!(advance_ring(&pool, &store, 4).await?, 4);
    let b_id = produce(&mut t1, Producer::InsertJobCompat, "b").await?;
    t1.commit().await?;
    let placement = placement(&pool).await?;
    eprintln!("placement: {placement:?}");
    assert_eq!(
        (placement["b"].1, placement["b"].2),
        (0, 0),
        "b is in generation 0, in slot 0 that generation 4 now holds"
    );

    let reached = advance_ring(&pool, &store, 8).await?;
    eprintln!("ring reached generation {reached}");
    assert!(reached < 8, "the ring stops at b's slot");
    let state = store
        .load_job(&pool, b_id)
        .await?
        .map(|job| format!("{:?}", job.state));
    assert_eq!(state.as_deref(), Some("Available"), "b survives prune");
    let pin = store.queue_ring_pin(&pool).await?.expect("b pins the ring");
    assert_eq!(pin.slot, 0);
    assert_eq!(pin.lanes.len(), 1, "{pin:?}");
    Ok(())
}
