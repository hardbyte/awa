//! A job keeps its enqueue shard through deferred and DLQ storage (#340,
//! migration v064).
//!
//! Each test routes jobs with ordering keys that select different shards,
//! moves them through retry, the DLQ or a schedule, and checks the shard the
//! next claim hands them out from. Before shards were retained, every job a
//! promotion or DLQ retry returned to a lane took one rotor-chosen shard, so
//! jobs with different keys ended up together.

use awa_model::{
    audited_sql, migrations, ClaimedRuntimeJob, InsertOpts, InsertParams, JobState, QueueStorage,
    QueueStorageConfig, RetryFromDlqOpts,
};
use chrono::Utc;
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const QUEUE: &str = "retained_shard";
const SHARDS: i16 = 8;
const LEASE: Duration = Duration::from_secs(60);

fn config(schema: &str) -> QueueStorageConfig {
    QueueStorageConfig {
        schema: schema.into(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    }
}

async fn installed_store(
    pool: &PgPool,
    schema: &str,
) -> Result<QueueStorage, Box<dyn std::error::Error>> {
    migrations::run(pool).await?;
    let store = QueueStorage::new(config(schema))?;
    store.install(pool).await?;
    set_enqueue_shards(pool, SHARDS).await?;
    Ok(store)
}

async fn set_enqueue_shards(pool: &PgPool, shards: i16) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO awa.queue_meta (queue, enqueue_shards) VALUES ($1, $2)
         ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards",
    )
    .bind(QUEUE)
    .bind(shards)
    .execute(pool)
    .await?;
    Ok(())
}

/// Ordering keys that select four different shards at [`SHARDS`].
fn keys_on_distinct_shards() -> Vec<(Vec<u8>, i16)> {
    let mut by_shard = BTreeMap::new();
    for n in 0.. {
        let key = format!("customer-{n}").into_bytes();
        let shard = awa_model::queue_storage::shard_for_ordering_key(&key, SHARDS);
        by_shard.entry(shard).or_insert(key);
        if by_shard.len() == 4 {
            break;
        }
    }
    by_shard
        .into_iter()
        .map(|(shard, key)| (key, shard))
        .collect()
}

fn keyed_params(key: &[u8], run_at: Option<chrono::DateTime<Utc>>) -> InsertParams {
    InsertParams {
        kind: "retained_shard_probe".into(),
        args: serde_json::json!({}),
        opts: InsertOpts {
            queue: QUEUE.into(),
            ordering_key: Some(key.to_vec()),
            run_at,
            ..Default::default()
        },
    }
}

/// Claim `expected` jobs and return each one's shard by job id. A claim
/// takes one lane, so jobs on different shards need a claim each.
async fn claim_all(
    pool: &PgPool,
    store: &QueueStorage,
    expected: usize,
) -> Result<(Vec<ClaimedRuntimeJob>, BTreeMap<i64, i16>), Box<dyn std::error::Error>> {
    let mut claimed = Vec::new();
    for _ in 0..expected * 4 {
        if claimed.len() >= expected {
            break;
        }
        claimed.extend(store.claim_runtime_batch(pool, QUEUE, 100, LEASE).await?);
    }
    assert_eq!(claimed.len(), expected, "every job is claimable");
    let shards = claimed
        .iter()
        .map(|job| (job.job.id, job.claim.enqueue_shard))
        .collect();
    Ok((claimed, shards))
}

async fn retained_shards(
    pool: &PgPool,
    schema: &str,
    table: &str,
) -> Result<BTreeMap<i64, Option<i16>>, sqlx::Error> {
    let rows: Vec<(i64, Option<i16>)> = sqlx::query_as(audited_sql(format!(
        "SELECT job_id, enqueue_shard FROM {schema}.{table} ORDER BY job_id"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// Enqueue one job per key and claim them, returning the shard each key
/// routed its job to.
async fn enqueue_and_claim(
    pool: &PgPool,
    store: &QueueStorage,
) -> Result<(Vec<ClaimedRuntimeJob>, BTreeMap<i64, i16>), Box<dyn std::error::Error>> {
    let keys = keys_on_distinct_shards();
    let params: Vec<_> = keys
        .iter()
        .map(|(key, _)| keyed_params(key, None))
        .collect();
    store.enqueue_params_batch(pool, &params).await?;
    let (claimed, shards) = claim_all(pool, store, keys.len()).await?;
    let mut routed: Vec<i16> = shards.values().copied().collect();
    let mut expected: Vec<i16> = keys.iter().map(|(_, shard)| *shard).collect();
    routed.sort_unstable();
    expected.sort_unstable();
    assert_eq!(routed, expected, "each key routes to its own shard");
    Ok((claimed, shards))
}

async fn retry_all(
    pool: &PgPool,
    store: &QueueStorage,
    claimed: &[ClaimedRuntimeJob],
) -> TestResult {
    for job in claimed {
        store
            .retry_after(pool, job.job.id, job.job.run_lease, Duration::ZERO, None)
            .await?
            .expect("the claimed attempt retries");
    }
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn retries_and_dlq_retries_return_to_the_claimed_shard(pool: PgPool) -> TestResult {
    let schema = "retained_shard_cycle";
    let store = installed_store(&pool, schema).await?;
    let (claimed, original) = enqueue_and_claim(&pool, &store).await?;

    retry_all(&pool, &store, &claimed).await?;
    let deferred = retained_shards(&pool, schema, "deferred_jobs").await?;
    let expected: BTreeMap<i64, Option<i16>> = original
        .iter()
        .map(|(&id, &shard)| (id, Some(shard)))
        .collect();
    assert_eq!(deferred, expected, "retryable rows keep the claimed shard");

    store.promote_due(&pool, JobState::Retryable, 100).await?;
    let (claimed, retried) = claim_all(&pool, &store, original.len()).await?;
    assert_eq!(retried, original, "a promoted retry goes back to its shard");

    for job in &claimed {
        store
            .fail_to_dlq(
                &pool,
                job.job.id,
                job.job.run_lease,
                "probe",
                "probe failure",
                None,
            )
            .await?
            .expect("the claimed attempt moves to the DLQ");
    }
    let dead = retained_shards(&pool, schema, "dlq_entries").await?;
    assert_eq!(dead, expected, "DLQ rows keep the claimed shard");

    for &id in original.keys() {
        store
            .retry_from_dlq(&pool, id, &RetryFromDlqOpts::default())
            .await?
            .expect("the DLQ row is retried");
    }
    let (claimed, revived) = claim_all(&pool, &store, original.len()).await?;
    assert_eq!(revived, original, "a DLQ retry goes back to its shard");

    // A narrower queue wraps the retained shard rather than taking the rotor,
    // so jobs that shared a shard still share one. A fresh store has no
    // cached width.
    set_enqueue_shards(&pool, 3).await?;
    let narrowed = QueueStorage::new(config(schema))?;
    retry_all(&pool, &narrowed, &claimed).await?;
    narrowed
        .promote_due(&pool, JobState::Retryable, 100)
        .await?;
    let (_, wrapped) = claim_all(&pool, &narrowed, original.len()).await?;
    let expected_wrapped: BTreeMap<i64, i16> = original
        .iter()
        .map(|(&id, &shard)| (id, shard % 3))
        .collect();
    assert_eq!(
        wrapped, expected_wrapped,
        "a retained shard wraps to the queue's width"
    );
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn scheduled_keyed_jobs_keep_their_key_shard(pool: PgPool) -> TestResult {
    let schema = "retained_shard_scheduled";
    let store = installed_store(&pool, schema).await?;
    let keys = keys_on_distinct_shards();
    let run_at = Utc::now() + chrono::Duration::milliseconds(300);
    let params: Vec<_> = keys
        .iter()
        .map(|(key, _)| keyed_params(key, Some(run_at)))
        .collect();
    store.enqueue_params_batch(&pool, &params).await?;

    let deferred = retained_shards(&pool, schema, "deferred_jobs").await?;
    let mut stored: Vec<Option<i16>> = deferred.values().copied().collect();
    let mut expected: Vec<Option<i16>> = keys.iter().map(|(_, shard)| Some(*shard)).collect();
    stored.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        stored, expected,
        "a scheduled keyed job keeps its key's shard"
    );

    tokio::time::sleep(Duration::from_millis(400)).await;
    store.promote_due(&pool, JobState::Scheduled, 100).await?;
    let (_, promoted) = claim_all(&pool, &store, keys.len()).await?;
    let promoted: BTreeMap<i64, Option<i16>> = promoted
        .into_iter()
        .map(|(id, shard)| (id, Some(shard)))
        .collect();
    assert_eq!(promoted, deferred, "a promoted job goes to its key's shard");
    Ok(())
}

/// A binary that retains shards runs against a schema without v064's
/// column, and starts retaining shards once the column appears without
/// being restarted.
#[sqlx::test(migrations = false)]
async fn runs_before_the_migration_and_retains_after_it(pool: PgPool) -> TestResult {
    let schema = "retained_shard_pre_v064";
    migrations::run(&pool).await?;
    let installer = QueueStorage::new(config(schema))?;
    installer.install(&pool).await?;
    set_enqueue_shards(&pool, SHARDS).await?;
    sqlx::raw_sql(audited_sql(format!(
        "ALTER TABLE {schema}.deferred_jobs DROP COLUMN enqueue_shard;
         ALTER TABLE {schema}.dlq_entries DROP COLUMN enqueue_shard;"
    )))
    .execute(&pool)
    .await?;

    let store = QueueStorage::new(config(schema))?;
    let (claimed, original) = enqueue_and_claim(&pool, &store).await?;
    retry_all(&pool, &store, &claimed).await?;
    store.promote_due(&pool, JobState::Retryable, 100).await?;
    let (claimed, _) = claim_all(&pool, &store, original.len()).await?;
    for job in &claimed {
        store
            .fail_to_dlq(
                &pool,
                job.job.id,
                job.job.run_lease,
                "probe",
                "probe failure",
                None,
            )
            .await?
            .expect("the claimed attempt moves to the DLQ");
    }
    for &id in original.keys() {
        store
            .retry_from_dlq(&pool, id, &RetryFromDlqOpts::default())
            .await?
            .expect("the DLQ row is retried");
    }
    let (claimed, _) = claim_all(&pool, &store, original.len()).await?;

    // What v064 runs for each queue-storage schema.
    sqlx::query("SELECT awa.install_queue_storage_retained_shard($1)")
        .bind(schema)
        .execute(&pool)
        .await?;
    let current: BTreeMap<i64, i16> = claimed
        .iter()
        .map(|job| (job.job.id, job.claim.enqueue_shard))
        .collect();
    retry_all(&pool, &store, &claimed).await?;
    let deferred = retained_shards(&pool, schema, "deferred_jobs").await?;
    let expected: BTreeMap<i64, Option<i16>> = current
        .iter()
        .map(|(&id, &shard)| (id, Some(shard)))
        .collect();
    assert_eq!(
        deferred, expected,
        "the same store retains shards after v064"
    );
    store.promote_due(&pool, JobState::Retryable, 100).await?;
    let (_, retried) = claim_all(&pool, &store, original.len()).await?;
    assert_eq!(retried, current, "a promoted retry goes back to its shard");
    Ok(())
}
