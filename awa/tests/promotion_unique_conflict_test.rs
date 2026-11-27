//! A due scheduled or retryable job whose unique key another live job holds
//! must not block promotion. The duplicate is cancelled and the other due
//! jobs of the same batch are promoted in the same pass, on both engines.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::audited_sql;
use awa::model::QueueStorage;
use awa::worker::maintenance::QueueStoragePromoter;
use awa::worker::AwaMetrics;
use awa::{Client, JobArgs, JobEvent, JobResult, QueueConfig, UniqueOpts};
use awa::{JobState, QueueStorageConfig};
use awa_model::{admin, insert_many, insert_with, migrations, AwaError, InsertOpts, InsertParams};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
const PROMOTION_TEST_LOCK_KEY: i64 = 0x6177_6170_726f_6d31;

struct EngineGuard {
    _local: tokio::sync::MutexGuard<'static, ()>,
    _conn: PgConnection,
}

async fn acquire_engine_guard() -> EngineGuard {
    let local = TEST_MUTEX.get_or_init(|| Mutex::new(())).lock().await;
    let mut conn = PgConnection::connect(&database_url())
        .await
        .expect("Failed to open test lock connection");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(PROMOTION_TEST_LOCK_KEY)
        .execute(&mut conn)
        .await
        .expect("Failed to acquire test advisory lock");
    EngineGuard {
        _local: local,
        _conn: conn,
    }
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn connect() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&database_url())
        .await
        .expect("Failed to connect — is Postgres running?");
    migrations::run(&pool).await.expect("Failed to migrate");
    pool
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct UcPromoteJob {
    pub n: i64,
}

/// Unique while `available` only, so a scheduled or retryable job holds no
/// claim until promotion.
const AVAILABLE_ONLY_STATES: u8 = 1 << 1;
/// Unique while `available` or `cancelled`. A promotion duplicate with this
/// mask must not hold the key once cancelled, and must keep it for a retry.
const AVAILABLE_AND_CANCELLED_STATES: u8 = (1 << 1) | (1 << 6);

const CONSUMED_QUEUE: &str = "uc_promote";
/// Nothing consumes this queue, so promoted jobs stay `available` and the
/// claim holder keeps its claim.
const PARKED_QUEUE: &str = "uc_promote_parked";

async fn clean(pool: &PgPool) {
    for queue in [CONSUMED_QUEUE, PARKED_QUEUE] {
        sqlx::query("DELETE FROM awa.jobs WHERE queue LIKE $1 || '%'")
            .bind(queue)
            .execute(pool)
            .await
            .expect("queue cleanup");
    }
    sqlx::query(
        "DELETE FROM awa.job_unique_claims AS c WHERE NOT EXISTS (SELECT 1 FROM awa.jobs AS j WHERE j.id = c.job_id)",
    )
    .execute(pool)
    .await
    .expect("stale claim cleanup");
}

async fn insert(
    pool: &PgPool,
    queue: &str,
    n: i64,
    unique: Option<u8>,
    delay: Option<Duration>,
) -> i64 {
    insert_with(
        pool,
        &UcPromoteJob { n },
        InsertOpts {
            queue: queue.into(),
            run_at: delay.map(|delay| Utc::now() + delay),
            unique: unique.map(|states| UniqueOpts {
                states,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .expect("insert should succeed")
    .id
}

async fn job_state(pool: &PgPool, id: i64) -> String {
    sqlx::query_scalar("SELECT state::text FROM awa.jobs WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("state probe")
}

async fn claim_holder(pool: &PgPool, id: i64) -> Vec<i64> {
    sqlx::query_scalar(
        r#"
        SELECT c.job_id
        FROM awa.job_unique_claims AS c
        WHERE c.unique_key = (SELECT unique_key FROM awa.jobs WHERE id = $1)
        ORDER BY c.job_id
        "#,
    )
    .bind(id)
    .fetch_all(pool)
    .await
    .expect("claim probe")
}

fn is_unique_conflict(err: &AwaError) -> bool {
    match err {
        AwaError::UniqueConflict { .. } => true,
        AwaError::Database(sqlx::Error::Database(db_err)) => {
            db_err.code().as_deref() == Some("23505")
        }
        _ => false,
    }
}

fn client_builder(pool: &PgPool, queue_storage: bool) -> awa::ClientBuilder {
    let builder = Client::builder(pool.clone());
    if queue_storage {
        builder.queue_storage(
            QueueStorageConfig::default(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
    } else {
        builder
    }
}

/// Runs a client on the parked queue until the holder completes. `completed`
/// is in no test mask, so the holder releases the key.
async fn complete_holder(pool: &PgPool, queue: &str, holder: i64, queue_storage: bool) {
    let client = client_builder(pool, queue_storage)
        .queue(queue, QueueConfig::default())
        .register::<UcPromoteJob, _, _>(|_args: UcPromoteJob, _ctx| async move {
            Ok(JobResult::Completed)
        })
        .build()
        .expect("client should build");
    client.start().await.expect("client should start");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let state: Option<String> =
            sqlx::query_scalar("SELECT state::text FROM awa.jobs WHERE id = $1")
                .bind(holder)
                .fetch_optional(pool)
                .await
                .expect("holder probe");
        // Queue storage does not list a completed job in `awa.jobs`.
        if state.as_deref().is_none_or(|state| state == "completed") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the holder did not complete: {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    client.shutdown(Duration::from_secs(5)).await;
}

async fn run_case(pool: &PgPool, states: u8, queue_storage: bool) {
    clean(pool).await;
    // A lane of its own: on queue storage a deleted job's ready entry stays
    // in its lane until the claimer passes it, and earlier tests park
    // thousands of rows in the shared queue.
    let parked = format!("{PARKED_QUEUE}_{}", uuid::Uuid::new_v4().simple());
    let due_in = Duration::from_secs(1);
    let duplicate = insert(pool, &parked, 1, Some(states), Some(due_in)).await;
    let other = insert(pool, &parked, 2, None, Some(due_in)).await;
    let holder = insert(pool, &parked, 1, Some(states), None).await;
    assert_eq!(job_state(pool, duplicate).await, "scheduled");
    assert_eq!(job_state(pool, holder).await, "available");

    let (events, mut cancelled_events) = mpsc::unbounded_channel();
    let client = client_builder(pool, queue_storage)
        .queue(CONSUMED_QUEUE, QueueConfig::default())
        .register::<UcPromoteJob, _, _>(|_args: UcPromoteJob, _ctx| async move {
            Ok(JobResult::Completed)
        })
        .on_event::<UcPromoteJob, _, _>(move |event| {
            let events = events.clone();
            async move {
                if let JobEvent::Cancelled { job, reason, .. } = event {
                    let _ = events.send((job.id, reason));
                }
            }
        })
        .promote_interval(Duration::from_millis(100))
        .build()
        .expect("client should build");
    client.start().await.expect("client should start");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let d = job_state(pool, duplicate).await;
        let o = job_state(pool, other).await;
        if d != "scheduled" && o != "scheduled" {
            assert_eq!(d, "cancelled", "the duplicate is cancelled");
            assert_eq!(o, "available", "the other due job is promoted");
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("promotion did not converge: duplicate={d}, other={o}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let errors: String = sqlx::query_scalar("SELECT errors::text FROM awa.jobs WHERE id = $1")
        .bind(duplicate)
        .fetch_one(pool)
        .await
        .expect("errors probe");
    assert!(
        errors.contains("promoted as duplicate: unique claim held by another job"),
        "{errors}"
    );
    assert_eq!(job_state(pool, holder).await, "available");
    assert_eq!(claim_holder(pool, holder).await, vec![holder]);

    let (event_job, reason) =
        tokio::time::timeout(Duration::from_secs(10), cancelled_events.recv())
            .await
            .expect("a cancelled lifecycle event")
            .expect("event channel open");
    assert_eq!(event_job, duplicate);
    assert!(reason.contains("promoted as duplicate"), "{reason}");

    client.shutdown(Duration::from_secs(5)).await;

    // Retrying the cancelled duplicate while the holder owns the key
    // conflicts and leaves the holder's claim in place.
    let err = admin::retry(pool, duplicate)
        .await
        .expect_err("retry while the holder owns the key must conflict");
    assert!(is_unique_conflict(&err), "{err:?}");
    assert_eq!(job_state(pool, duplicate).await, "cancelled");
    assert_eq!(claim_holder(pool, holder).await, vec![holder]);

    // It conflicts because the cancelled row kept its key; `cancelled` is
    // cleared from its mask so the cancelled row never held the claim.
    let (key, mask): (Option<Vec<u8>>, Option<String>) =
        sqlx::query_as("SELECT unique_key, unique_states::text FROM awa.jobs WHERE id = $1")
            .bind(duplicate)
            .fetch_one(pool)
            .await
            .expect("cancelled row probe");
    assert!(
        key.is_some(),
        "the cancelled duplicate keeps its unique key"
    );
    assert_eq!(
        mask.as_deref().and_then(|mask| mask.chars().nth(6)),
        Some('0'),
        "the cancelled duplicate's mask does not claim cancelled: {mask:?}"
    );

    // Once the holder releases the key, the retry succeeds and claims it.
    complete_holder(pool, &parked, holder, queue_storage).await;
    admin::retry(pool, duplicate)
        .await
        .expect("retry after the holder leaves")
        .expect("retried row");
    assert_eq!(job_state(pool, duplicate).await, "available");
    assert_eq!(claim_holder(pool, duplicate).await, vec![duplicate]);

    // The retry restored the original mask: cancelling the job again keeps
    // its key exactly when that mask claims `cancelled`.
    admin::cancel(pool, duplicate)
        .await
        .expect("cancel the retried job")
        .expect("cancelled row");
    let claims_cancelled = states & (1 << JobState::Cancelled.bit_position()) != 0;
    assert_eq!(
        claim_holder(pool, duplicate).await,
        if claims_cancelled {
            vec![duplicate]
        } else {
            Vec::new()
        },
        "a cancelled job keeps its key exactly when its mask claims cancelled"
    );

    clean(pool).await;
}

#[tokio::test]
async fn test_canonical_promotion_cancels_a_unique_duplicate_and_promotes_the_rest() {
    if awa_testing::setup::skip_unless_canonical(
        "test_canonical_promotion_cancels_a_unique_duplicate_and_promotes_the_rest",
    ) {
        return;
    }
    let _guard = acquire_engine_guard().await;
    let pool = connect().await;
    awa_testing::setup::reset_runtime_backend(&pool).await;
    run_case(&pool, AVAILABLE_ONLY_STATES, false).await;
}

#[tokio::test]
async fn test_canonical_promotion_duplicate_whose_mask_claims_cancelled_keeps_its_key() {
    if awa_testing::setup::skip_unless_canonical(
        "test_canonical_promotion_duplicate_whose_mask_claims_cancelled_keeps_its_key",
    ) {
        return;
    }
    let _guard = acquire_engine_guard().await;
    let pool = connect().await;
    awa_testing::setup::reset_runtime_backend(&pool).await;
    run_case(&pool, AVAILABLE_AND_CANCELLED_STATES, false).await;
}

#[tokio::test]
async fn test_queue_storage_promotion_cancels_a_unique_duplicate_and_promotes_the_rest() {
    let _guard = acquire_engine_guard().await;
    let pool = connect().await;
    awa_testing::setup::activate_queue_storage(&pool).await;
    run_case(&pool, AVAILABLE_ONLY_STATES, true).await;
    awa_testing::setup::reset_runtime_backend(&pool).await;
}

#[tokio::test]
async fn test_queue_storage_promotion_duplicate_whose_mask_claims_cancelled_keeps_its_key() {
    let _guard = acquire_engine_guard().await;
    let pool = connect().await;
    awa_testing::setup::activate_queue_storage(&pool).await;
    run_case(&pool, AVAILABLE_AND_CANCELLED_STATES, true).await;
    awa_testing::setup::reset_runtime_backend(&pool).await;
}

/// More due rows than one promotion batch takes.
const BACKLOG: i64 = 4_200;

/// A conflict in the first batch of a backlog larger than one batch still
/// lets one promotion pass promote the whole backlog. The canonical
/// counterpart is a unit test of the maintenance service.
#[tokio::test]
async fn test_queue_storage_promotion_pass_continues_past_a_batch_with_a_duplicate() {
    let _guard = acquire_engine_guard().await;
    let pool = connect().await;
    awa_testing::setup::activate_queue_storage(&pool).await;
    clean(&pool).await;

    let due = Utc::now() - chrono::Duration::seconds(60);
    let unique = Some(UniqueOpts {
        states: AVAILABLE_ONLY_STATES,
        ..Default::default()
    });
    let job = |n: i64, run_at, unique| InsertParams {
        kind: UcPromoteJob { n }.kind_str().to_string(),
        args: serde_json::json!({ "n": n }),
        opts: InsertOpts {
            queue: PARKED_QUEUE.into(),
            run_at,
            unique,
            ..Default::default()
        },
    };
    let duplicate = insert_many(
        &pool,
        &[job(
            1_000_000,
            Some(due - chrono::Duration::seconds(1)),
            unique.clone(),
        )],
    )
    .await
    .expect("insert duplicate")[0]
        .id;
    let holder = insert_many(&pool, &[job(1_000_000, None, unique)])
        .await
        .expect("insert holder")[0]
        .id;
    let backlog: Vec<InsertParams> = (1..=BACKLOG).map(|n| job(n, Some(due), None)).collect();
    for chunk in backlog.chunks(1_000) {
        insert_many(&pool, chunk).await.expect("insert backlog");
    }

    let store = Arc::new(QueueStorage::new(QueueStorageConfig::default()).expect("queue storage"));
    let promoter = QueueStoragePromoter::new(pool.clone(), store, AwaMetrics::from_global());
    let promoted = promoter
        .promote_due(JobState::Scheduled, &CancellationToken::new())
        .await
        .expect("one promotion pass");
    assert_eq!(promoted as i64, BACKLOG);
    let cancelled = promoter.take_cancelled();
    assert_eq!(
        cancelled.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![duplicate]
    );

    let scheduled: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'scheduled'",
    )
    .bind(PARKED_QUEUE)
    .fetch_one(&pool)
    .await
    .expect("scheduled count");
    assert_eq!(scheduled, 0);
    assert_eq!(job_state(&pool, duplicate).await, "cancelled");
    assert_eq!(job_state(&pool, holder).await, "available");

    clean(&pool).await;
    awa_testing::setup::reset_runtime_backend(&pool).await;
}

/// Each lane of a promotion batch commits on its own. When a later lane
/// fails, the duplicates an earlier lane already cancelled are still handed to
/// the promoter, so their `Cancelled` events and metric are not lost.
#[tokio::test]
async fn test_queue_storage_promotion_keeps_cancellations_of_lanes_before_a_failed_lane() {
    let _guard = acquire_engine_guard().await;
    let pool = connect().await;
    awa_testing::setup::activate_queue_storage(&pool).await;
    clean(&pool).await;

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    // Lanes promote in queue order, so lane A commits before lane B fails.
    let lane_a = format!("{PARKED_QUEUE}_a_{suffix}");
    let lane_b = format!("{PARKED_QUEUE}_b_{suffix}");
    let due_in = Some(Duration::from_secs(1));
    let duplicate = insert(&pool, &lane_a, 1, Some(AVAILABLE_ONLY_STATES), due_in).await;
    let holder = insert(&pool, &lane_a, 1, Some(AVAILABLE_ONLY_STATES), None).await;
    insert(&pool, &lane_b, 2, None, due_in).await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    // Lane B's promotion fails with an error that is not a lock timeout.
    sqlx::raw_sql(audited_sql(format!(
        r#"
        CREATE OR REPLACE FUNCTION public.uc_promote_fail_lane_b() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'lane B fails';
        END $$;
        CREATE TRIGGER uc_promote_fail_lane_b
            BEFORE INSERT ON awa.ready_entries
            FOR EACH ROW WHEN (NEW.queue = '{lane_b}')
            EXECUTE FUNCTION public.uc_promote_fail_lane_b();
        "#
    )))
    .execute(&pool)
    .await
    .expect("install the failing trigger");

    let store = Arc::new(QueueStorage::new(QueueStorageConfig::default()).expect("queue storage"));
    let promoter = QueueStoragePromoter::new(pool.clone(), store, AwaMetrics::from_global());
    let result = promoter
        .promote_due(JobState::Scheduled, &CancellationToken::new())
        .await;
    let cancelled = promoter.take_cancelled();

    sqlx::raw_sql(
        "DROP TRIGGER uc_promote_fail_lane_b ON awa.ready_entries; DROP FUNCTION public.uc_promote_fail_lane_b()",
    )
    .execute(&pool)
    .await
    .expect("drop the failing trigger");

    assert!(result.is_err(), "lane B fails the pass: {result:?}");
    assert_eq!(job_state(&pool, duplicate).await, "cancelled");
    assert_eq!(
        cancelled.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![duplicate],
        "lane A's committed cancellation reaches the promoter"
    );
    assert_eq!(job_state(&pool, holder).await, "available");

    clean(&pool).await;
    awa_testing::setup::reset_runtime_backend(&pool).await;
}
