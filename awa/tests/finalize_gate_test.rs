//! A retry storm on queue storage stays within the runtime's finalize gate.
//!
//! Every handler returns a retryable error, so every finalize is a direct
//! transaction. The test holds a `SHARE` lock on `deferred_jobs` so those
//! transactions block at their insert, which makes each open finalize
//! transaction visible in `pg_locks` and keeps the rest waiting for a gate
//! permit for longer than the heartbeat staleness window.

mod ci_timing;

use awa::model::{migrations, QueueStorage};
use awa::{Client, InsertOpts, JobArgs, JobError, JobState, QueueConfig, QueueStorageConfig};
use ci_timing::{contention_floor, scaled_staleness, scaled_timeout};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const QUEUE: &str = "finalize_gate_storm";
const JOBS: i64 = 300;
const WORKERS: u32 = 200;
const POOL_SIZE: u32 = 10;
/// The default gate for a pool of `POOL_SIZE`: a quarter, clamped to 4..=64.
const GATE_SIZE: i64 = 4;
const OUTAGE: &str = "downstream outage";

#[derive(Serialize, Deserialize, JobArgs)]
struct StormJob {
    n: i64,
}

async fn open_finalize_transactions(pool: &PgPool, deferred: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(DISTINCT pid) FROM pg_locks \
         WHERE relation = $1::regclass AND mode = 'RowExclusiveLock' \
           AND pid <> pg_backend_pid()",
    )
    .bind(deferred)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[sqlx::test(migrations = false)]
async fn a_retry_storm_stays_within_the_finalize_gate(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    let store = QueueStorage::new(QueueStorageConfig::default()).unwrap();
    let deferred = format!("{}.deferred_jobs", store.schema());

    let runs: Arc<Mutex<HashMap<i64, u32>>> = Arc::new(Mutex::new(HashMap::new()));
    let runtime_pool = PgPoolOptions::new()
        .max_connections(POOL_SIZE)
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let staleness = scaled_staleness(Duration::from_secs(2));
    let handler_runs = runs.clone();
    let client = Client::builder(runtime_pool)
        .queue(
            QUEUE,
            QueueConfig {
                max_workers: WORKERS,
                poll_interval: Duration::from_millis(20),
                ..Default::default()
            },
        )
        .register::<StormJob, _, _>(move |args, _ctx| {
            let runs = handler_runs.clone();
            async move {
                *runs.lock().unwrap().entry(args.n).or_default() += 1;
                Err(JobError::retryable_msg(OUTAGE))
            }
        })
        .heartbeat_interval(Duration::from_millis(200))
        .heartbeat_staleness(staleness)
        .heartbeat_rescue_interval(Duration::from_millis(250))
        // Retried jobs stay deferred for the whole test.
        .promote_interval(Duration::from_secs(3600))
        .leader_election_interval(Duration::from_millis(50))
        .build()
        .unwrap();

    // Starting on an empty database makes queue storage the active engine.
    client.start().await.unwrap();

    // Finalize transactions block at their deferred_jobs insert while this
    // lock is held; heartbeats and claims do not write that table.
    let mut stall = pool.begin().await.unwrap();
    sqlx::query(&format!("LOCK TABLE {deferred} IN SHARE MODE"))
        .execute(stall.as_mut())
        .await
        .unwrap();

    let mut ids = Vec::new();
    for n in 0..JOBS {
        let job = awa::insert_with(
            &pool,
            &StormJob { n },
            InsertOpts {
                queue: QUEUE.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        ids.push(job.id);
    }

    let mut max_open = 0;
    let all_ran = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(30));
    while runs.lock().unwrap().len() < JOBS as usize {
        assert!(
            tokio::time::Instant::now() < all_ran,
            "only {} of {JOBS} handlers ran",
            runs.lock().unwrap().len()
        );
        max_open = max_open.max(open_finalize_transactions(&pool, &deferred).await);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Every handler has returned; keep the finalizes waiting for longer than
    // the staleness window so only heartbeats keep their attempts alive.
    let hold_until = tokio::time::Instant::now() + staleness + Duration::from_secs(2);
    while tokio::time::Instant::now() < hold_until {
        max_open = max_open.max(open_finalize_transactions(&pool, &deferred).await);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stall.commit().await.unwrap();

    let settled = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(30));
    loop {
        max_open = max_open.max(open_finalize_transactions(&pool, &deferred).await);
        let retryable: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.jobs WHERE id = ANY($1) AND state = 'retryable'",
        )
        .bind(&ids)
        .fetch_one(&pool)
        .await
        .unwrap();
        if retryable == JOBS {
            break;
        }
        assert!(
            tokio::time::Instant::now() < settled,
            "only {retryable} of {JOBS} jobs reached retryable"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    client.shutdown(Duration::from_secs(5)).await;

    eprintln!("most direct finalize transactions open at once: {max_open}");
    assert!(
        max_open <= GATE_SIZE,
        "{max_open} direct finalize transactions were open at once; the gate allows {GATE_SIZE}"
    );
    assert!(
        max_open >= contention_floor(1),
        "no direct finalize transaction was observed"
    );

    for id in &ids {
        let job = store.load_job(&pool, *id).await.unwrap().unwrap();
        assert_eq!(job.state, JobState::Retryable, "job {id}");
        assert_eq!(job.attempt, 1, "job {id} was run more than once");
        let errors = job.errors.unwrap_or_default();
        assert_eq!(
            errors.len(),
            1,
            "job {id} has errors from more than its own attempt (rescued?): {errors:?}"
        );
        assert_eq!(errors[0]["error"], OUTAGE, "job {id}: {errors:?}");
    }
    let runs = runs.lock().unwrap();
    assert_eq!(runs.len(), JOBS as usize);
    assert!(
        runs.values().all(|count| *count == 1),
        "a handler ran more than once"
    );
}
