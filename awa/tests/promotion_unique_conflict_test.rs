//! A due scheduled or retryable job whose unique key another live job holds
//! must not block promotion. The duplicate is cancelled and the other due
//! jobs of the same batch are promoted in the same pass, on both engines.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::{Client, JobArgs, JobEvent, JobResult, QueueConfig, UniqueOpts};
use awa_model::{insert_with, migrations, InsertOpts};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

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

const CONSUMED_QUEUE: &str = "uc_promote";
/// Nothing consumes this queue, so promoted jobs stay `available` and the
/// claim holder keeps its claim.
const PARKED_QUEUE: &str = "uc_promote_parked";

async fn clean(pool: &PgPool) {
    for queue in [CONSUMED_QUEUE, PARKED_QUEUE] {
        sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
            .bind(queue)
            .execute(pool)
            .await
            .expect("queue cleanup");
    }
}

async fn insert(pool: &PgPool, n: i64, unique: bool, delay: Option<Duration>) -> i64 {
    insert_with(
        pool,
        &UcPromoteJob { n },
        InsertOpts {
            queue: PARKED_QUEUE.into(),
            run_at: delay.map(|delay| Utc::now() + delay),
            unique: unique.then(|| UniqueOpts {
                states: AVAILABLE_ONLY_STATES,
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

async fn run_case(pool: &PgPool) {
    clean(pool).await;
    let due_in = Duration::from_secs(1);
    let duplicate = insert(pool, 1, true, Some(due_in)).await;
    let other = insert(pool, 2, false, Some(due_in)).await;
    let holder = insert(pool, 1, true, None).await;
    assert_eq!(job_state(pool, duplicate).await, "scheduled");
    assert_eq!(job_state(pool, holder).await, "available");

    let (events, mut cancelled_events) = mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
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
    run_case(&pool).await;
}

#[tokio::test]
async fn test_queue_storage_promotion_cancels_a_unique_duplicate_and_promotes_the_rest() {
    let _guard = acquire_engine_guard().await;
    let pool = connect().await;
    awa_testing::setup::activate_queue_storage(&pool).await;
    run_case(&pool).await;
    awa_testing::setup::reset_runtime_backend(&pool).await;
}
