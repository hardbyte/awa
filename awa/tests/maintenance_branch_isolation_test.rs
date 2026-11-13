//! #303: one maintenance branch waiting on a lock must not stop the leader's
//! user-visible branches (promotion and rescue).
//!
//! The leader syncs its declared periodic jobs into `awa.cron_jobs` as soon
//! as it takes leadership. An upsert on a cron row that another session has
//! locked waits on that lock. These tests hold such a lock and measure how
//! long a due scheduled job then waits to be promoted, whether an attempt
//! past its deadline is still rescued, whether stored schedules keep firing,
//! and whether losing the leader lock still stops promotion.

#[path = "ci_timing.rs"]
mod ci_timing;

use awa::model::{QueueStorage, QueueStorageConfig};
use awa::{Client, InsertOpts, JobArgs, JobResult, JobState, PeriodicJob, QueueConfig};
use awa_testing::setup::TestDatabase;
use ci_timing::scaled_timeout;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::{Duration, Instant};

const QUEUE: &str = "branch_isolation";
const CRON: &str = "branch_isolation_nightly";
/// Declared after `CRON`, so a leader's startup sync reaches it only after
/// the locked `CRON` row, while its row already exists from an earlier run.
const EVERY_SECOND: &str = "branch_isolation_every_second";
const LEADER_LOCK_KEY: i64 = 0x4157_415f_4d41_494e;

/// Completes at once, unless `overrun` is set: then its first attempt
/// outlives its deadline, so only deadline rescue lets a second attempt run.
#[derive(Serialize, Deserialize, JobArgs)]
struct IsolationProbe {
    #[serde(default)]
    overrun: bool,
}

/// A runtime on its own pool, so the test's own reads and the lock holder
/// never compete with it for connections.
async fn client(pool: &PgPool) -> Client {
    let runtime_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    Client::builder(runtime_pool)
        .queue_storage(
            QueueStorageConfig::default(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .queue(
            QUEUE,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(20),
                // Queue storage takes each attempt's deadline from the queue.
                deadline_duration: Duration::from_millis(500),
                ..Default::default()
            },
        )
        .register::<IsolationProbe, _, _>(|args: IsolationProbe, ctx: &awa::JobContext| {
            let first_attempt = ctx.job.attempt <= 1;
            async move {
                if args.overrun && first_attempt {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                Ok(JobResult::Completed)
            }
        })
        .periodic(
            PeriodicJob::builder(CRON, "0 3 * * *")
                .queue(QUEUE)
                .build(&IsolationProbe { overrun: false })
                .unwrap(),
        )
        .periodic(
            PeriodicJob::builder(EVERY_SECOND, "* * * * * *")
                .queue(QUEUE)
                .build(&IsolationProbe { overrun: false })
                .unwrap(),
        )
        .leader_election_interval(Duration::from_millis(50))
        .leader_check_interval(Duration::from_millis(100))
        .promote_interval(Duration::from_millis(50))
        .deadline_rescue_interval(Duration::from_millis(200))
        .runtime_snapshot_interval(Duration::from_millis(100))
        .build()
        .unwrap()
}

/// A leader whose startup cron sync is waiting on a `CRON` row that the
/// returned transaction holds `FOR UPDATE`.
async fn leader_with_blocked_cron_sync(
    pool: &PgPool,
) -> (Client, sqlx::Transaction<'static, sqlx::Postgres>) {
    // Register both schedules once, then lock `CRON`'s row so the next
    // leader's startup sync waits on it.
    let first = client(pool).await;
    first.start().await.unwrap();
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    loop {
        let synced: i64 =
            sqlx::query_scalar("SELECT count(*) FROM awa.cron_jobs WHERE name IN ($1, $2)")
                .bind(CRON)
                .bind(EVERY_SECOND)
                .fetch_one(pool)
                .await
                .unwrap();
        if synced == 2 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "cron never synced");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    first.shutdown(Duration::from_secs(5)).await;

    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM awa.cron_jobs WHERE name = $1 FOR UPDATE")
        .bind(CRON)
        .execute(&mut *holder)
        .await
        .unwrap();

    let leader = client(pool).await;
    leader.start().await.unwrap();
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    while !leader.health_check().await.leader {
        assert!(
            tokio::time::Instant::now() < deadline,
            "never became leader"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The startup sync is waiting on the locked row.
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                            WHERE datname = current_database()
                              AND wait_event_type = 'Lock'
                              AND query LIKE '%cron_jobs%')",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if waiting {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the startup cron sync never waited on the locked row"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (leader, holder)
}

#[tokio::test]
async fn a_blocked_cron_sync_does_not_delay_promotion_or_rescue() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let (leader, holder) = leader_with_blocked_cron_sync(&pool).await;
    let store = QueueStorage::new(QueueStorageConfig::default()).unwrap();
    let scheduled = awa::insert_with(
        &pool,
        &IsolationProbe { overrun: false },
        InsertOpts {
            queue: QUEUE.into(),
            run_at: Some(chrono::Utc::now() + chrono::Duration::milliseconds(200)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let overrun = awa::insert_with(
        &pool,
        &IsolationProbe { overrun: true },
        InsertOpts {
            queue: QUEUE.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Promotion runs every 50 ms and deadline rescue every 200 ms; a rescued
    // attempt is retried after about 2 s. Generous, but far less than the
    // time the lock is held.
    let started = Instant::now();
    let finished = tokio::time::timeout(scaled_timeout(Duration::from_secs(15)), async {
        for id in [scheduled.id, overrun.id] {
            loop {
                let state = store.load_job(&pool, id).await.unwrap().unwrap().state;
                if state == JobState::Completed {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    })
    .await;
    let waited = started.elapsed();
    let mut states = Vec::new();
    for id in [scheduled.id, overrun.id] {
        let job = store.load_job(&pool, id).await.unwrap().unwrap();
        states.push(format!("job {id}: {:?} attempt {}", job.state, job.attempt));
    }

    // Shutdown waits for the branch in flight, so release the lock first.
    holder.rollback().await.unwrap();
    leader.shutdown(Duration::from_secs(5)).await;

    assert!(
        finished.is_ok(),
        "with the cron sync blocked, promotion and deadline rescue made no progress for {waited:?}: {states:?}"
    );
}

/// The cron evaluator runs on its own task and starts before the startup
/// sync, so schedules already stored keep firing while the sync waits.
#[tokio::test]
async fn stored_schedules_fire_while_the_startup_sync_waits() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let (leader, holder) = leader_with_blocked_cron_sync(&pool).await;
    let since = chrono::Utc::now();
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    let fired = loop {
        let fired: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM awa.cron_jobs
                            WHERE name = $1 AND last_enqueued_at > $2)",
        )
        .bind(EVERY_SECOND)
        .bind(since)
        .fetch_one(&pool)
        .await
        .unwrap();
        if fired || tokio::time::Instant::now() >= deadline {
            break fired;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    drop(holder);
    leader.shutdown(Duration::from_secs(5)).await;
    assert!(
        fired,
        "an every-second schedule did not fire while the startup sync waited"
    );
}

/// Losing the leader lock while the hygiene loop is stuck must still be
/// noticed and must fence recovery: nothing is promoted while no session
/// holds the lock.
#[tokio::test]
async fn losing_leadership_while_hygiene_is_stuck_stops_promotion() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let (leader, holder) = leader_with_blocked_cron_sync(&pool).await;
    let holder_pid: i32 = sqlx::query_scalar(
        "SELECT l.pid FROM pg_locks l
         WHERE l.locktype = 'advisory' AND l.granted
           AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
           AND ((l.classid::bigint << 32) | l.objid::bigint) = $1",
    )
    .bind(LEADER_LOCK_KEY)
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(holder_pid)
        .execute(&pool)
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    while leader.health_check().await.leader {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the runtime kept reporting leadership after its lock session ended"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let store = QueueStorage::new(QueueStorageConfig::default()).unwrap();
    let scheduled = awa::insert_with(
        &pool,
        &IsolationProbe { overrun: false },
        InsertOpts {
            queue: QUEUE.into(),
            run_at: Some(chrono::Utc::now() + chrono::Duration::milliseconds(100)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // Promotion runs every 50 ms while leading. Watch for 2 s: whenever no
    // session holds the lock, the job must not have left `scheduled`.
    let window = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut promoted_while_unled = None;
    while tokio::time::Instant::now() < window {
        let lock_held: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_locks l
                            WHERE l.locktype = 'advisory' AND l.granted
                              AND l.database = (SELECT oid FROM pg_database
                                                WHERE datname = current_database())
                              AND ((l.classid::bigint << 32) | l.objid::bigint) = $1)",
        )
        .bind(LEADER_LOCK_KEY)
        .fetch_one(&pool)
        .await
        .unwrap();
        let state = store
            .load_job(&pool, scheduled.id)
            .await
            .unwrap()
            .unwrap()
            .state;
        if !lock_held && state != JobState::Scheduled {
            promoted_while_unled = Some(state);
            break;
        }
        if lock_held {
            // Leadership came back; fencing is no longer what is observed.
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    drop(holder);
    leader.shutdown(Duration::from_secs(5)).await;
    assert_eq!(
        promoted_while_unled, None,
        "a job was promoted while no session held the leader lock"
    );
}
