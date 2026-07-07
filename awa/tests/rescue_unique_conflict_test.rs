//! #388 regression: a single unique-claim conflict must not wedge the
//! canonical rescue sweeps.
//!
//! Scenario: a unique job whose `unique_states` mask excludes `running`
//! gets stuck running (no claim held), a newer duplicate is legitimately
//! enqueued and takes the claim, and the batched `running -> retryable`
//! rescue then trips `idx_awa_jobs_unique`. Before the fix the whole
//! 500-row rescue batch aborted every tick, starving rescue for every
//! other stuck job. The fix retries row-at-a-time: clean rows rescue,
//! the conflicted row is cancelled (the claim holder wins).
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::{Client, JobArgs, JobResult, QueueConfig, UniqueOpts};
use awa_model::{insert_with, migrations, InsertOpts, JobState};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&database_url())
        .await
        .expect("Failed to connect — is Postgres running?");
    migrations::run(&pool).await.expect("Failed to migrate");
    awa_testing::setup::reset_runtime_backend(&pool).await;
    pool
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct UcRescueJob {
    pub n: i64,
}

/// `scheduled | available | retryable` — the "unique while pending" mask
/// that drops the claim while running. This is the configuration that
/// produced the production wedge.
const PENDING_ONLY_STATES: u8 = 0b0001_0011;

const CONSUMED_QUEUE: &str = "uc_rescue";
/// The duplicate lives on a queue no worker consumes, so it keeps holding
/// the unique claim for the whole test instead of being claimed (claiming
/// would release the claim under this mask and hide the conflict).
const PARKED_QUEUE: &str = "uc_rescue_parked";

async fn clean(pool: &PgPool) {
    for queue in [CONSUMED_QUEUE, PARKED_QUEUE] {
        sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
            .bind(queue)
            .execute(pool)
            .await
            .expect("queue cleanup");
    }
}

async fn insert_unique(pool: &PgPool, queue: &str, n: i64) -> i64 {
    let row = insert_with(
        pool,
        &UcRescueJob { n },
        InsertOpts {
            queue: queue.into(),
            unique: Some(UniqueOpts {
                by_queue: false,
                by_args: true,
                by_period: None,
                states: PENDING_ONLY_STATES,
            }),
            ..Default::default()
        },
    )
    .await
    .expect("unique insert should succeed");
    row.id
}

async fn job_state(pool: &PgPool, id: i64) -> String {
    sqlx::query_scalar("SELECT state::text FROM awa.jobs WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("state probe")
}

#[tokio::test]
async fn test_rescue_survives_unique_claim_conflict() {
    if awa_testing::setup::skip_unless_canonical("test_rescue_survives_unique_claim_conflict") {
        return;
    }
    let pool = setup().await;
    clean(&pool).await;

    // Job A: unique, pending-only mask, will be stranded in `running`.
    let job_a = insert_unique(&pool, CONSUMED_QUEUE, 1).await;

    // Job C: no uniqueness — the innocent stuck job the old behavior starved.
    let job_c = insert_with(
        &pool,
        &UcRescueJob { n: 2 },
        InsertOpts {
            queue: CONSUMED_QUEUE.into(),
            ..Default::default()
        },
    )
    .await
    .expect("plain insert should succeed")
    .id;

    // Strand both in `running` with a stale heartbeat, as a crashed worker
    // would. The transition fires the claim trigger: A's mask excludes
    // `running`, so A's claim is released here.
    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'running',
            attempt = 1,
            heartbeat_at = now() - interval '1 hour'
        WHERE id IN ($1, $2)
        "#,
    )
    .bind(job_a)
    .bind(job_c)
    .execute(&pool)
    .await
    .expect("stranding update");

    // Job B: same unique key as A (same kind/args, by_queue = false), on a
    // queue nothing consumes. Legitimate under the mask — and it now holds
    // the claim, poisoning A's rescue transition.
    let job_b = insert_unique(&pool, PARKED_QUEUE, 1).await;

    let client = Client::builder(pool.clone())
        .queue(CONSUMED_QUEUE, QueueConfig::default())
        .register::<UcRescueJob, _, _>(|_args: UcRescueJob, _ctx| async move {
            Ok(JobResult::Completed)
        })
        .heartbeat_rescue_interval(Duration::from_millis(500))
        .heartbeat_staleness(Duration::from_secs(2))
        .build()
        .expect("client should build");
    client.start().await.expect("client should start");

    // The old behavior never converges: the batched rescue aborts on A's
    // conflict every tick, so C stays `running` forever. The fix rescues C
    // (which then re-runs to completion) and cancels A.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let a = job_state(&pool, job_a).await;
        let c = job_state(&pool, job_c).await;
        if a == "cancelled" && c == "completed" {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("rescue did not converge: A={a} (want cancelled), C={c} (want completed)");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // A carries an operator-legible error entry naming the duplicate.
    let rendered: String = sqlx::query_scalar("SELECT errors::text FROM awa.jobs WHERE id = $1")
        .bind(job_a)
        .fetch_one(&pool)
        .await
        .expect("errors probe");
    assert!(
        rendered.contains("rescued as duplicate"),
        "cancelled job should record why: {rendered}"
    );

    // The claim holder was untouched: still queued, still owning the key.
    assert_eq!(job_state(&pool, job_b).await, "available");
    let holder: i64 = sqlx::query_scalar(
        "SELECT c.job_id FROM awa.job_unique_claims c JOIN awa.jobs j ON j.unique_key = c.unique_key WHERE j.id = $1",
    )
    .bind(job_b)
    .fetch_one(&pool)
    .await
    .expect("claim probe");
    assert_eq!(holder, job_b, "duplicate keeps the unique claim");

    // The cancelled job is a real terminal: not claimable, not counted live.
    assert_eq!(job_state(&pool, job_a).await, "cancelled");
    let _ = JobState::Cancelled; // states covered by this test's contract

    client.shutdown(Duration::from_secs(10)).await;
    clean(&pool).await;
}
