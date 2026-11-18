//! With `queue_stripe_count > 1` a logical queue `q` is stored and claimed as
//! physical stripes `q#0` … `q#N-1`. Per-queue client settings are keyed by
//! the logical name, so they must apply to jobs claimed from its stripes.

mod ci_timing;

use awa::model::{migrations, InsertOpts, QueueStorage, QueueStorageConfig};
use awa::{Client, JobArgs, JobError, QueueConfig, RetentionPolicy};
use ci_timing::scaled_timeout;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const QUEUE: &str = "striped_dlq_override";
const JOBS: i64 = 8;

#[derive(Serialize, Deserialize, JobArgs)]
struct FailingJob {
    n: i64,
}

#[sqlx::test(migrations = false)]
async fn queue_dlq_override_applies_to_jobs_claimed_from_stripes(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    let config = QueueStorageConfig {
        queue_stripe_count: 2,
        ..Default::default()
    };
    let store = QueueStorage::new(config.clone()).unwrap();
    let client = Client::builder(pool.clone())
        .queue_storage(
            config,
            Duration::from_millis(500),
            Duration::from_millis(500),
        )
        .queue(
            QUEUE,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(20),
                ..Default::default()
            },
        )
        .dlq_enabled_by_default(false)
        .queue_dlq_enabled(QUEUE, true)
        .register::<FailingJob, _, _>(|_args, _ctx| async move {
            Err(JobError::terminal("always fails"))
        })
        .build()
        .unwrap();
    client.start().await.unwrap();

    let params: Vec<_> = (0..JOBS)
        .map(|n| awa::model::job::InsertParams {
            kind: "failing_job".into(),
            args: serde_json::json!({ "n": n }),
            opts: InsertOpts {
                queue: QUEUE.into(),
                max_attempts: 1,
                ..Default::default()
            },
        })
        .collect();
    store.enqueue_params_batch(&pool, &params).await.unwrap();
    let striped: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.ready_entries WHERE queue LIKE $1 || '#%'")
            .bind(QUEUE)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(striped, JOBS, "the jobs must be enqueued to the stripes");

    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(20));
    let (dead_lettered, failed) = loop {
        let dead_lettered: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.dlq_entries WHERE queue = $1 OR queue LIKE $1 || '#%'",
        )
        .bind(QUEUE)
        .fetch_one(&pool)
        .await
        .unwrap();
        let failed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.done_entries
             WHERE (queue = $1 OR queue LIKE $1 || '#%') AND state = 'failed'",
        )
        .bind(QUEUE)
        .fetch_one(&pool)
        .await
        .unwrap();
        if dead_lettered + failed >= JOBS || tokio::time::Instant::now() >= deadline {
            break (dead_lettered, failed);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    client.shutdown(Duration::from_secs(5)).await;

    assert_eq!(
        (dead_lettered, failed),
        (JOBS, 0),
        "queue_dlq_enabled({QUEUE:?}, true) must send its striped jobs to the DLQ"
    );
}

async fn dlq_rows(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM awa.dlq_entries WHERE queue = $1 OR queue LIKE $1 || '#%'",
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[sqlx::test(migrations = false)]
async fn queue_dlq_retention_override_applies_to_rows_from_stripes(pool: PgPool) {
    const KEPT: &str = "striped_dlq_retention_kept";
    const PURGED: &str = "striped_dlq_retention_purged";
    migrations::run(&pool).await.unwrap();
    let config = QueueStorageConfig {
        queue_stripe_count: 2,
        ..Default::default()
    };
    let store = QueueStorage::new(config.clone()).unwrap();
    let day = Duration::from_secs(86_400);
    let runs = Arc::new(AtomicI64::new(0));
    let handler_runs = runs.clone();
    let mut builder = Client::builder(pool.clone())
        .queue_storage(
            config,
            Duration::from_millis(500),
            Duration::from_millis(500),
        )
        .dlq_enabled_by_default(true)
        .dlq_retention(Duration::from_millis(1))
        .cleanup_interval(Duration::from_millis(100))
        .queue_retention(
            KEPT,
            RetentionPolicy {
                completed: day,
                failed: day,
                dlq: Some(day),
            },
        )
        .register::<FailingJob, _, _>(move |_args, _ctx| {
            let runs = handler_runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Err(JobError::terminal("always fails"))
            }
        });
    for queue in [KEPT, PURGED] {
        builder = builder.queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(20),
                ..Default::default()
            },
        );
    }
    let client = builder.build().unwrap();
    client.start().await.unwrap();

    for queue in [KEPT, PURGED] {
        let params: Vec<_> = (0..JOBS)
            .map(|n| awa::model::job::InsertParams {
                kind: "failing_job".into(),
                args: serde_json::json!({ "n": n }),
                opts: InsertOpts {
                    queue: queue.into(),
                    max_attempts: 1,
                    ..Default::default()
                },
            })
            .collect();
        store.enqueue_params_batch(&pool, &params).await.unwrap();
    }

    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(20));
    while runs.load(Ordering::SeqCst) < 2 * JOBS {
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {} of {} handlers ran",
            runs.load(Ordering::SeqCst),
            2 * JOBS
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Let the finalizes write the DLQ rows and several cleanup passes run.
    let settle = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(2));
    while tokio::time::Instant::now() < settle {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        dlq_rows(&pool, PURGED).await,
        0,
        "the global one-millisecond DLQ retention must purge {PURGED:?}"
    );
    let kept = dlq_rows(&pool, KEPT).await;
    client.shutdown(Duration::from_secs(5)).await;

    assert_eq!(
        kept, JOBS,
        "a one-day DLQ retention override on {KEPT:?} must keep its striped DLQ rows"
    );
}
