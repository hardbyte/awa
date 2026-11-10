//! One attempt whose finalize waits on a lock must not hold back the
//! finalizes of unrelated jobs.
//!
//! `admin::cancel_tx` run inside an application's transaction holds the
//! cancelled attempt's completion lock until that transaction commits. If the
//! attempt's handler returns `Completed` meanwhile, its completion waits on
//! that lock. These tests check that completions of jobs in other queues,
//! which share only the runtime's completion pipeline with it, still finish.

mod ci_timing;

use awa::model::{admin, migrations, QueueStorage};
use awa::{Client, InsertOpts, JobArgs, JobResult, JobState, QueueConfig, QueueStorageConfig};
use ci_timing::scaled_timeout;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

const HELD_QUEUE: &str = "finalize_isolation_held";
const BYSTANDER_QUEUE: &str = "finalize_isolation_bystander";
const BYSTANDERS: i64 = 50;

/// Waits for a permit before completing, so the test decides when its
/// attempt finalizes.
#[derive(Serialize, Deserialize, JobArgs)]
struct GatedJob {}

#[derive(Serialize, Deserialize, JobArgs)]
struct BystanderJob {
    n: i64,
}

async fn wait_for<F, Fut>(what: &str, timeout: Duration, mut done: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if done().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {timeout:?} waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn state(store: &QueueStorage, pool: &PgPool, id: i64) -> Option<JobState> {
    store.load_job(pool, id).await.unwrap().map(|job| job.state)
}

#[sqlx::test(migrations = false)]
async fn a_held_attempt_lock_does_not_stall_other_completions(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    let store = QueueStorage::new(QueueStorageConfig::default()).unwrap();

    let gate = Arc::new(Semaphore::new(0));
    let runtime_pool = PgPoolOptions::new()
        .max_connections(20)
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let handler_gate = gate.clone();
    let client = Client::builder(runtime_pool)
        .queue(
            HELD_QUEUE,
            QueueConfig {
                max_workers: 1,
                poll_interval: Duration::from_millis(20),
                ..Default::default()
            },
        )
        .queue(
            BYSTANDER_QUEUE,
            QueueConfig {
                max_workers: 8,
                poll_interval: Duration::from_millis(20),
                ..Default::default()
            },
        )
        .register::<GatedJob, _, _>(move |_args, _ctx| {
            let gate = handler_gate.clone();
            async move {
                gate.acquire().await.unwrap().forget();
                Ok(JobResult::Completed)
            }
        })
        .register::<BystanderJob, _, _>(|_args, _ctx| async move { Ok(JobResult::Completed) })
        .leader_election_interval(Duration::from_millis(50))
        .runtime_snapshot_interval(Duration::from_millis(100))
        .build()
        .unwrap();
    client.start().await.unwrap();

    let held = awa::insert_with(
        &pool,
        &GatedJob {},
        InsertOpts {
            queue: HELD_QUEUE.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    wait_for(
        "the gated job to start",
        scaled_timeout(Duration::from_secs(10)),
        || async { state(&store, &pool, held.id).await == Some(JobState::Running) },
    )
    .await;

    // An application cancels the running job inside its own transaction and
    // has not committed yet.
    let mut app = pool.begin().await.unwrap();
    admin::cancel_tx(&mut *app, held.id)
        .await
        .expect("cancel inside the application's transaction");

    // The handler returns; its completion now waits on the held lock.
    gate.add_permits(1);
    wait_for(
        "the held job's completion to wait on the application's lock",
        scaled_timeout(Duration::from_secs(10)),
        || async {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_locks
                                WHERE locktype = 'advisory' AND NOT granted
                                  AND database = (SELECT oid FROM pg_database
                                                  WHERE datname = current_database()))",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        },
    )
    .await;

    // Jobs in another queue complete while that completion waits.
    let mut bystanders = Vec::new();
    for n in 0..BYSTANDERS {
        let job = awa::insert_with(
            &pool,
            &BystanderJob { n },
            InsertOpts {
                queue: BYSTANDER_QUEUE.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        bystanders.push(job.id);
    }
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    let mut completed = 0;
    while tokio::time::Instant::now() < deadline {
        completed = 0;
        for id in &bystanders {
            if state(&store, &pool, *id).await == Some(JobState::Completed) {
                completed += 1;
            }
        }
        if completed == bystanders.len() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let held_state_while_open = state(&store, &pool, held.id).await;

    // The application commits: the cancel wins, and nothing is left behind.
    app.commit().await.unwrap();
    wait_for(
        "the held job to settle after the application committed",
        scaled_timeout(Duration::from_secs(10)),
        || async { state(&store, &pool, held.id).await == Some(JobState::Cancelled) },
    )
    .await;
    client.shutdown(Duration::from_secs(5)).await;

    assert_eq!(
        completed,
        bystanders.len(),
        "only {completed} of {} jobs in another queue completed while one attempt's completion \
         waited on an application's lock (held job: {held_state_while_open:?})",
        bystanders.len()
    );
}
