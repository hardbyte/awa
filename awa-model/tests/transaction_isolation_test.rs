//! awa's own transactions run at READ COMMITTED whatever the session's
//! default isolation level (#847).

use awa_model::isolation::BeginReadCommitted;
use awa_model::{
    insert, migrations, InsertOpts, JobArgs, PruneOutcome, QueueStorage, QueueStorageConfig,
    RotateOutcome, SkipReason,
};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};

type Error = Box<dyn std::error::Error>;
type TestResult = Result<(), Error>;

const QUEUE: &str = "rc_prune";

#[derive(Serialize)]
struct PruneProbe {
    n: i64,
}

impl JobArgs for PruneProbe {
    fn kind() -> &'static str {
        "prune_probe"
    }
}

/// A pool on the same database whose connections default to `isolation`, as
/// a role or database `default_transaction_isolation` would make them.
async fn pool_defaulting_to(pool: &PgPool, isolation: &'static str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(2)
        .after_connect(move |conn, _| {
            Box::pin(async move {
                sqlx::query("SELECT set_config('default_transaction_isolation', $1, false)")
                    .bind(isolation)
                    .execute(conn)
                    .await
                    .map(drop)
            })
        })
        .connect_with(pool.connect_options().as_ref().clone())
        .await
}

#[sqlx::test(migrations = false)]
async fn own_transactions_ignore_a_repeatable_read_default(pool: PgPool) -> TestResult {
    let repeatable_read = pool_defaulting_to(&pool, "repeatable read").await?;

    let mut inherited = repeatable_read.begin().await?;
    let level: String = sqlx::query_scalar("SELECT current_setting('transaction_isolation')")
        .fetch_one(&mut *inherited)
        .await?;
    inherited.rollback().await?;
    assert_eq!(level, "repeatable read", "begin() inherits the default");

    let mut own = repeatable_read.begin_read_committed().await?;
    let level: String = sqlx::query_scalar("SELECT current_setting('transaction_isolation')")
        .fetch_one(&mut *own)
        .await?;
    own.rollback().await?;
    assert_eq!(level, "read committed");
    Ok(())
}

/// A queue-storage schema whose ring slot 0 holds one completed job, so queue
/// prune of slot 0 gets past its emptiness check to the lock.
async fn store_with_completed_job(pool: &PgPool, schema: &str) -> Result<QueueStorage, Error> {
    migrations::run(pool).await?;
    let store = QueueStorage::new(QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        lease_claim_receipts: false,
        ..Default::default()
    })
    .and_then(|store| store.with_prune_lock_timeout(Duration::from_secs(30)))?;
    store.install(pool).await?;

    insert::insert_with(
        pool,
        &PruneProbe { n: 1 },
        InsertOpts {
            queue: QUEUE.to_string(),
            ..Default::default()
        },
    )
    .await?;
    let claimed = store
        .claim_runtime_batch(pool, QUEUE, 1, Duration::from_secs(60))
        .await?;
    assert_eq!(claimed.len(), 1, "the seed job is claimed");
    let completed = store.complete_runtime_batch(pool, &claimed).await?;
    assert_eq!(completed.len(), 1, "the seed job completes");
    Ok(store)
}

/// Whether a backend is waiting for a lock on a table in `schema`.
async fn lock_waiter_in(pool: &PgPool, schema: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
             WHERE NOT l.granted
               AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
               AND c.relnamespace = $1::regnamespace
         )",
    )
    .bind(schema)
    .fetch_one(pool)
    .await
}

/// A producer commits a job into ring slot 0 after the ring has moved on to
/// slot 1 and while queue prune of slot 0, running on `maintenance`, waits
/// for the slot's lock. Returns prune's outcome and whether the job survived.
async fn prune_racing_a_producer(
    pool: &PgPool,
    maintenance: &PgPool,
    schema: &str,
) -> Result<(PruneOutcome, bool), Error> {
    let store = Arc::new(store_with_completed_job(pool, schema).await?);

    // The producer enqueues through the compatibility insert, which reads the
    // ring cursor, still slot 0, and holds its transaction open.
    let mut producer = pool.begin_read_committed().await?;
    let job = insert::insert_with(
        &mut *producer,
        &PruneProbe { n: 2 },
        InsertOpts {
            queue: QUEUE.to_string(),
            ..Default::default()
        },
    )
    .await?;
    let placed: i32 = sqlx::query_scalar(&format!(
        "SELECT ready_slot FROM {schema}.ready_entries WHERE job_id = $1"
    ))
    .bind(job.id)
    .fetch_one(&mut *producer)
    .await?;
    assert_eq!(placed, 0, "the producer writes into slot 0");

    // Seal slot 0.
    let rotated = store.rotate(pool).await?;
    assert!(
        matches!(rotated, RotateOutcome::Rotated { slot: 1, .. }),
        "unexpected rotate outcome: {rotated:?}"
    );

    // Prune of slot 0 sees the completed seed job, not the producer's
    // uncommitted row, and waits for the producer to release slot 0's
    // children.
    let prune = tokio::spawn({
        let store = store.clone();
        let maintenance = maintenance.clone();
        async move { store.prune_oldest(&maintenance, Duration::ZERO).await }
    });
    let started = Instant::now();
    while !lock_waiter_in(pool, schema).await? {
        assert!(
            !prune.is_finished(),
            "prune finished without waiting for the lock"
        );
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "prune never waited for the slot's lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    producer.commit().await?;
    let outcome = prune.await??;

    let survived: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS (SELECT 1 FROM {schema}.ready_entries WHERE job_id = $1)"
    ))
    .bind(job.id)
    .fetch_one(pool)
    .await?;
    Ok((outcome, survived))
}

fn assert_kept_pending_job(outcome: PruneOutcome, survived: bool) {
    assert!(
        matches!(
            outcome,
            PruneOutcome::SkippedActive {
                slot: 0,
                reason: SkipReason::QueuePendingReady,
                ..
            }
        ),
        "prune must see the job committed while it waited, got {outcome:?}"
    );
    assert!(survived, "the committed job must survive the prune");
}

/// The reviewer's case for #847: with a maintenance pool whose connections
/// default to REPEATABLE READ, prune's checks after the lock wait used the
/// snapshot taken before it, reported `Pruned` and deleted the job the
/// producer committed while prune waited.
#[sqlx::test(migrations = false)]
async fn prune_on_a_repeatable_read_pool_keeps_a_job_committed_during_its_lock_wait(
    pool: PgPool,
) -> TestResult {
    let repeatable_read = pool_defaulting_to(&pool, "repeatable read").await?;
    let (outcome, survived) =
        prune_racing_a_producer(&pool, &repeatable_read, "awa_rc_prune_rr").await?;
    assert_kept_pending_job(outcome, survived);
    Ok(())
}

/// The same race on a READ COMMITTED maintenance pool.
#[sqlx::test(migrations = false)]
async fn prune_on_a_read_committed_pool_keeps_a_job_committed_during_its_lock_wait(
    pool: PgPool,
) -> TestResult {
    let read_committed = pool_defaulting_to(&pool, "read committed").await?;
    let (outcome, survived) =
        prune_racing_a_producer(&pool, &read_committed, "awa_rc_prune_rc").await?;
    assert_kept_pending_job(outcome, survived);
    Ok(())
}
