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
use sqlx::postgres::{PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;

// Both tests flip the cluster-wide storage engine, so they must never run
// concurrently — process-local mutex for in-binary threads plus a Postgres
// advisory lock for process-per-test runners.
static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
const RESCUE_TEST_LOCK_KEY: i64 = 0x6177_6172_6573_6331; // "awaresc1"

struct RescueTestGuard {
    _local: tokio::sync::MutexGuard<'static, ()>,
    _conn: PgConnection,
}

async fn acquire_rescue_guard() -> RescueTestGuard {
    let local = TEST_MUTEX.get_or_init(|| Mutex::new(())).lock().await;
    let mut conn = PgConnection::connect(&database_url())
        .await
        .expect("Failed to open rescue test lock connection");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(RESCUE_TEST_LOCK_KEY)
        .execute(&mut conn)
        .await
        .expect("Failed to acquire rescue test advisory lock");
    RescueTestGuard {
        _local: local,
        _conn: conn,
    }
}

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
    clean_queues(pool, &[CONSUMED_QUEUE, PARKED_QUEUE]).await;
}

async fn clean_queues(pool: &PgPool, queues: &[&str]) {
    for queue in queues {
        sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
            .bind(queue)
            .execute(pool)
            .await
            .expect("queue cleanup");
    }
}

async fn insert_unique(pool: &PgPool, queue: &str, n: i64) -> i64 {
    insert_unique_with(pool, queue, n, PENDING_ONLY_STATES).await
}

async fn insert_unique_with(pool: &PgPool, queue: &str, n: i64, states: u8) -> i64 {
    let row = insert_with(
        pool,
        &UcRescueJob { n },
        InsertOpts {
            queue: queue.into(),
            unique: Some(UniqueOpts {
                by_queue: false,
                by_args: true,
                by_period: None,
                states,
            }),
            ..Default::default()
        },
    )
    .await
    .expect("unique insert should succeed");
    row.id
}

async fn job_state(pool: &PgPool, id: i64) -> String {
    job_state_opt(pool, id)
        .await
        .expect("job should be visible in awa.jobs")
}

/// Under queue storage a claimed (running) job is not visible through the
/// `awa.jobs` compatibility view, so probes that can race a claim read an
/// Option instead of assuming presence.
async fn job_state_opt(pool: &PgPool, id: i64) -> Option<String> {
    sqlx::query_scalar("SELECT state::text FROM awa.jobs WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("state probe")
}

#[tokio::test]
async fn test_rescue_survives_unique_claim_conflict() {
    if awa_testing::setup::skip_unless_canonical("test_rescue_survives_unique_claim_conflict") {
        return;
    }
    let _guard = acquire_rescue_guard().await;
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

/// The canonical rescue cancels a duplicate whose mask also claims
/// `cancelled`: the cancelled row keeps its key with `cancelled` cleared
/// from its mask, records the original mask, and an admin retry restores it
/// once the key is free.
#[tokio::test]
async fn test_canonical_rescue_duplicate_with_cancelled_claiming_mask_keeps_its_key() {
    if awa_testing::setup::skip_unless_canonical(
        "test_canonical_rescue_duplicate_with_cancelled_claiming_mask_keeps_its_key",
    ) {
        return;
    }
    let _guard = acquire_rescue_guard().await;
    let pool = setup().await;
    clean(&pool).await;
    let states = PENDING_ONLY_STATES | 0b0100_0000;

    let job_a = insert_unique_with(&pool, CONSUMED_QUEUE, 21, states).await;
    let original: String =
        sqlx::query_scalar("SELECT unique_states::text FROM awa.jobs WHERE id = $1")
            .bind(job_a)
            .fetch_one(&pool)
            .await
            .expect("original mask");
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, \
         heartbeat_at = now() - interval '1 hour' WHERE id = $1",
    )
    .bind(job_a)
    .execute(&pool)
    .await
    .expect("stranding update");
    let job_b = insert_unique_with(&pool, PARKED_QUEUE, 21, states).await;

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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let a = job_state(&pool, job_a).await;
        if a == "cancelled" {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("rescue did not cancel the duplicate: A={a}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    client.shutdown(Duration::from_secs(10)).await;

    let (stored, recorded): (String, Option<String>) = sqlx::query_as(
        "SELECT unique_states::text, errors[array_length(errors, 1)]->>'duplicate_unique_states' \
         FROM awa.jobs WHERE id = $1",
    )
    .bind(job_a)
    .fetch_one(&pool)
    .await
    .expect("cancelled row");
    assert_eq!(recorded.as_deref(), Some(original.as_str()));
    assert_eq!(&stored[6..7], "0", "cancelled is cleared from {stored}");
    let holder: i64 = sqlx::query_scalar(
        "SELECT c.job_id FROM awa.job_unique_claims c JOIN awa.jobs j ON j.unique_key = c.unique_key WHERE j.id = $1",
    )
    .bind(job_b)
    .fetch_one(&pool)
    .await
    .expect("claim probe");
    assert_eq!(holder, job_b, "the newer job keeps the key");

    let refused = awa_model::admin::retry(&pool, job_a).await;
    assert!(
        match &refused {
            Err(awa_model::AwaError::Database(sqlx::Error::Database(db))) => {
                db.code().as_deref() == Some("23505")
                    && db.constraint() == Some("idx_awa_jobs_unique")
            }
            _ => false,
        },
        "a retry is refused on the unique claim while the newer job holds the key: {refused:?}"
    );
    let state: String = sqlx::query_scalar("SELECT state::text FROM awa.jobs WHERE id = $1")
        .bind(job_a)
        .fetch_one(&pool)
        .await
        .expect("state probe");
    assert_eq!(
        state, "cancelled",
        "the refused retry leaves the job cancelled"
    );
    sqlx::query("DELETE FROM awa.jobs WHERE id = $1")
        .bind(job_b)
        .execute(&pool)
        .await
        .expect("remove the holder");
    awa_model::admin::retry(&pool, job_a)
        .await
        .expect("retry once the key is free");
    let revived: String =
        sqlx::query_scalar("SELECT unique_states::text FROM awa.jobs WHERE id = $1")
            .bind(job_a)
            .fetch_one(&pool)
            .await
            .expect("revived mask");
    assert_eq!(revived, original, "the retry restores the recorded mask");
    clean(&pool).await;
}

/// The queue-storage engine has the same wedge shape through a different
/// door: rescue re-inserts each rescued job as a `retryable` deferred row
/// inside one batch transaction, and the claim upsert refuses the re-entry
/// while a duplicate holds the claim. This test strands the jobs
/// live (parked handlers with a heartbeat-staleness shorter than the
/// heartbeat interval) instead of via raw SQL, so it exercises the real
/// claim/lease plane end to end.
#[tokio::test]
async fn test_rescue_survives_unique_claim_conflict_queue_storage() {
    rescue_survives_unique_claim_conflict_queue_storage(
        PENDING_ONLY_STATES,
        CONSUMED_QUEUE,
        PARKED_QUEUE,
    )
    .await;
}

/// The mask also claims `cancelled`, so the cancelled duplicate is written
/// with `cancelled` cleared from its mask. It keeps its unique key, and a
/// retry conflicts while the duplicate holds it.
#[tokio::test]
async fn test_rescue_duplicate_with_cancelled_claiming_mask_keeps_its_key() {
    rescue_survives_unique_claim_conflict_queue_storage(
        PENDING_ONLY_STATES | 0b0100_0000,
        "uc_rescue_cancelled_mask",
        "uc_rescue_cancelled_mask_parked",
    )
    .await;
}

async fn rescue_survives_unique_claim_conflict_queue_storage(
    states: u8,
    consumed: &str,
    parked: &str,
) {
    let _guard = acquire_rescue_guard().await;
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&database_url())
        .await
        .expect("Failed to connect — is Postgres running?");
    migrations::run(&pool).await.expect("Failed to migrate");
    awa_testing::setup::activate_queue_storage(&pool).await;
    clean_queues(&pool, &[consumed, parked]).await;

    // Job A: unique, pending-only mask — the claim is released the moment
    // it is claimed. Job C: no uniqueness, the innocent stuck job.
    let job_a = insert_unique_with(&pool, consumed, 11, states).await;
    let job_c = insert_with(
        &pool,
        &UcRescueJob { n: 12 },
        InsertOpts {
            queue: consumed.into(),
            ..Default::default()
        },
    )
    .await
    .expect("plain insert should succeed")
    .id;

    // Handlers park forever; heartbeat_staleness (2s) is far below the
    // heartbeat refresh interval (30s default), so every claimed job goes
    // heartbeat-stale while genuinely running — a live stand-in for a
    // crashed worker.
    let client = Client::builder(pool.clone())
        .queue(
            consumed,
            QueueConfig {
                max_workers: 4,
                ..QueueConfig::default()
            },
        )
        .register::<UcRescueJob, _, _>(|_args: UcRescueJob, _ctx| async move {
            std::future::pending::<()>().await;
            Ok(JobResult::Completed)
        })
        .heartbeat_rescue_interval(Duration::from_millis(500))
        .heartbeat_staleness(Duration::from_secs(2))
        .build()
        .expect("client should build");
    client.start().await.expect("client should start");

    // Wait for A to be claimed — under queue storage a running job drops
    // out of the compat view, and its claim is released by the pending-only
    // mask at that moment. Then park the duplicate on an unconsumed queue
    // so it takes and keeps the claim.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match job_state_opt(&pool, job_a).await.as_deref() {
            None | Some("running") => break,
            _ => {}
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "job A was never claimed: {:?}",
                job_state_opt(&pool, job_a).await
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let job_b = insert_unique_with(&pool, parked, 11, states).await;

    // Old behavior: the whole rescue batch aborts on A's conflict and C is
    // never rescued. Fixed behavior: A is cancelled (claim holder wins) and
    // C keeps being rescued — its attempt count advances. C oscillates
    // between running (invisible in the view) and retryable, so remember
    // the highest attempt seen.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut c_attempt_seen: i16 = 0;
    loop {
        let a = job_state_opt(&pool, job_a).await;
        if let Some(attempt) =
            sqlx::query_scalar::<_, i16>("SELECT attempt FROM awa.jobs WHERE id = $1")
                .bind(job_c)
                .fetch_optional(&pool)
                .await
                .expect("attempt probe")
        {
            c_attempt_seen = c_attempt_seen.max(attempt);
        }
        if a.as_deref() == Some("cancelled") && c_attempt_seen >= 1 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "QS rescue did not converge: A={a:?} (want cancelled), C attempt seen={c_attempt_seen}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let rendered: String = sqlx::query_scalar("SELECT errors::text FROM awa.jobs WHERE id = $1")
        .bind(job_a)
        .fetch_one(&pool)
        .await
        .expect("errors probe");
    assert!(
        rendered.contains("rescued as duplicate"),
        "cancelled job should record why: {rendered}"
    );
    assert_eq!(job_state(&pool, job_b).await, "available");

    let key: Option<Vec<u8>> = sqlx::query_scalar("SELECT unique_key FROM awa.jobs WHERE id = $1")
        .bind(job_a)
        .fetch_one(&pool)
        .await
        .expect("unique key probe");
    assert!(
        key.is_some(),
        "the cancelled duplicate keeps its unique key"
    );
    let retried = awa::model::admin::retry(&pool, job_a).await;
    assert!(
        matches!(retried, Err(awa::AwaError::UniqueConflict { .. })),
        "retry while the duplicate holds the key must conflict, got {retried:?}"
    );
    assert_eq!(job_state(&pool, job_b).await, "available");
    sqlx::query("DELETE FROM awa.jobs WHERE id = $1")
        .bind(job_b)
        .execute(&pool)
        .await
        .expect("delete the duplicate");
    let retried = awa::model::admin::retry(&pool, job_a).await;
    assert!(
        matches!(retried, Ok(Some(_))),
        "retry once the key is free must succeed, got {retried:?}"
    );

    client.shutdown(Duration::from_secs(5)).await;
    clean_queues(&pool, &[consumed, parked]).await;
    awa_testing::setup::reset_runtime_backend(&pool).await;
}

/// A rescued attempt whose mask claims `cancelled` as well as `retryable`,
/// but not `running`, loses its key to a newer duplicate while it runs. The
/// rescue cancels it as a duplicate; writing it `cancelled` with its full
/// mask would conflict again. The committed row keeps its unique key, has
/// `cancelled` cleared from its mask, and records the original mask in the
/// duplicate error entry, and the row rescue returns is the committed row.
/// An admin retry restores the mask and re-checks ownership: refused while
/// the duplicate holds the key, granted once it lets go.
#[tokio::test]
async fn test_rescued_duplicate_with_a_cancelled_mask_keeps_its_key_queue_storage() {
    use awa::model::admin;
    use awa::{insert_many, AwaError, InsertParams, QueueStorage, QueueStorageConfig};

    let _guard = acquire_rescue_guard().await;
    let pool = awa_testing::setup::setup(10).await;
    awa_testing::setup::activate_queue_storage(&pool).await;
    let queue = format!(
        "uc_rescue_cancelled_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let storage = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
    let insert = |pool: PgPool, queue: String| async move {
        insert_many(
            &pool,
            &[InsertParams {
                kind: "uc_rescue_cancelled".to_string(),
                args: serde_json::json!({}),
                opts: InsertOpts {
                    queue,
                    unique: Some(UniqueOpts {
                        by_queue: true,
                        by_args: false,
                        // available | retryable | cancelled, not running
                        states: 0b0101_0010,
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
    };
    let mask = |pool: PgPool, id: i64| async move {
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT unique_states::text FROM awa.jobs WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .expect("read mask")
    };
    let holder = |pool: PgPool, key: Vec<u8>| async move {
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT (SELECT job_id FROM awa.job_unique_claims WHERE unique_key = $1)",
        )
        .bind(key)
        .fetch_one(&pool)
        .await
        .expect("read claim")
    };

    let old = insert(pool.clone(), queue.clone()).await;
    let key = old.unique_key.clone().expect("unique key");
    let original = mask(pool.clone(), old.id).await.expect("mask");
    let mut cleared = original.clone().into_bytes();
    cleared[JobState::Cancelled.bit_position() as usize] = b'0';
    let cleared = String::from_utf8(cleared).expect("mask text");
    assert_ne!(original, cleared, "the mask claims cancelled");

    // Claiming releases the key, since the mask leaves out `running`; a
    // newer duplicate then takes it.
    let claimed = storage
        .claim_runtime_batch(&pool, &queue, 10, Duration::from_millis(1))
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    assert_eq!(holder(pool.clone(), key.clone()).await, None);
    let newer = insert(pool.clone(), queue.clone()).await;
    assert_eq!(holder(pool.clone(), key.clone()).await, Some(newer.id));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let rescued = storage
        .rescue_expired_deadlines(&pool)
        .await
        .expect("rescue");
    let returned = rescued
        .into_iter()
        .find(|row| row.id == old.id)
        .expect("rescue returns the attempt");
    assert_eq!(returned.state, JobState::Cancelled);

    let committed = admin::get_job(&pool, old.id).await.expect("committed row");
    assert_eq!(
        serde_json::to_value(&returned).expect("returned row"),
        serde_json::to_value(&committed).expect("committed row"),
        "rescue returns the row it committed"
    );
    assert_eq!(committed.unique_key.as_deref(), Some(key.as_slice()));
    assert_eq!(mask(pool.clone(), old.id).await, Some(cleared.clone()));
    let stored: (Option<Vec<u8>>, Option<String>) =
        sqlx::query_as("SELECT unique_key, unique_states FROM awa.done_entries WHERE job_id = $1")
            .bind(old.id)
            .fetch_one(&pool)
            .await
            .expect("stored done row");
    assert_eq!(
        stored,
        (Some(key.clone()), Some(cleared)),
        "the done row stores its key and mask"
    );
    let last_error = committed
        .errors
        .as_ref()
        .and_then(|errors| errors.last())
        .expect("duplicate error entry");
    assert_eq!(
        last_error["error"],
        "rescued as duplicate: unique claim held by a newer job"
    );
    assert_eq!(last_error["duplicate_unique_states"], original.as_str());
    assert_eq!(holder(pool.clone(), key.clone()).await, Some(newer.id));

    // Retry while the duplicate holds the key: refused, nothing changes.
    assert!(
        matches!(
            admin::retry(&pool, old.id).await,
            Err(AwaError::UniqueConflict { .. })
        ),
        "retry while the duplicate holds the key"
    );
    let unchanged = admin::get_job(&pool, old.id).await.expect("row");
    assert_eq!(unchanged.state, JobState::Cancelled);
    assert_eq!(holder(pool.clone(), key.clone()).await, Some(newer.id));

    // The duplicate lets go when it is claimed; the retry then takes the
    // key with the original mask.
    let claimed = storage
        .claim_runtime_batch(&pool, &queue, 10, Duration::from_secs(300))
        .await
        .expect("claim the duplicate");
    assert_eq!(
        claimed.iter().map(|job| job.job.id).collect::<Vec<_>>(),
        [newer.id]
    );
    assert_eq!(holder(pool.clone(), key.clone()).await, None);
    let revived = admin::retry(&pool, old.id)
        .await
        .expect("retry once the key is free")
        .expect("revived row");
    assert_eq!(revived.state, JobState::Available);
    assert_eq!(mask(pool.clone(), old.id).await, Some(original));
    assert_eq!(holder(pool.clone(), key.clone()).await, Some(old.id));

    // Close the duplicate's open receipt claim before the queue is cleaned,
    // so no later rescue meets a claim without its ready row.
    storage
        .complete_runtime_batch(&pool, &claimed)
        .await
        .expect("complete the duplicate");
    awa_testing::setup::clean_queue(&pool, &queue).await;
    sqlx::query("DELETE FROM awa.job_unique_claims WHERE unique_key = $1")
        .bind(&key)
        .execute(&pool)
        .await
        .expect("drop claim");
    awa_testing::setup::reset_runtime_backend(&pool).await;
}
