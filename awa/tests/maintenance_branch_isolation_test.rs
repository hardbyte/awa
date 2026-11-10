//! #303: one maintenance branch waiting on a lock must not stop the leader's
//! user-visible branches (promotion and rescue).
//!
//! The leader syncs its declared periodic jobs into `awa.cron_jobs` as soon
//! as it takes leadership. An upsert on a cron row that another session has
//! locked waits on that lock. These tests hold such a lock and measure how
//! long a due scheduled job then waits to be promoted, whether an attempt
//! past its deadline is still rescued, and whether shutdown stays bounded.

mod ci_timing;

use awa::model::{migrations, QueueStorage};
use awa::{
    Client, InsertOpts, JobArgs, JobResult, JobState, PeriodicJob, QueueConfig, QueueStorageConfig,
};
use ci_timing::scaled_timeout;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::{Duration, Instant};

const QUEUE: &str = "branch_isolation";
const CRON: &str = "branch_isolation_nightly";

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
        .leader_election_interval(Duration::from_millis(50))
        .leader_check_interval(Duration::from_millis(100))
        .promote_interval(Duration::from_millis(50))
        .deadline_rescue_interval(Duration::from_millis(200))
        .runtime_snapshot_interval(Duration::from_millis(100))
        .build()
        .unwrap()
}

#[sqlx::test(migrations = false)]
async fn a_blocked_cron_sync_does_not_delay_promotion_or_rescue(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    // Register the schedule once, then lock its row so the next leader's
    // startup sync waits on it.
    let first = client(&pool).await;
    first.start().await.unwrap();
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    loop {
        let synced: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM awa.cron_jobs WHERE name = $1)")
                .bind(CRON)
                .fetch_one(&pool)
                .await
                .unwrap();
        if synced {
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

    let leader = client(&pool).await;
    leader.start().await.unwrap();
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    while !leader.health_check().await.leader {
        assert!(
            tokio::time::Instant::now() < deadline,
            "never became leader"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

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

    // The cron sync is still waiting on the lock. Shutdown must not wait on it
    // without bound: 5 s drain, 5 s branch grace, then margin.
    let shutdown_started = Instant::now();
    let shutdown = tokio::time::timeout(
        scaled_timeout(Duration::from_secs(20)),
        leader.shutdown(Duration::from_secs(5)),
    )
    .await;
    let shutdown_took = shutdown_started.elapsed();
    holder.rollback().await.unwrap();

    assert!(
        finished.is_ok(),
        "with the cron sync blocked, promotion and deadline rescue made no progress for {waited:?}: {states:?}"
    );
    assert!(
        shutdown.is_ok(),
        "shutdown waited {shutdown_took:?} on the blocked cron sync"
    );
}
