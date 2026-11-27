//! With `queue_stripe_count > 1` a logical queue `q` is stored and claimed as
//! physical stripes `q#0` … `q#N-1`. Per-queue client settings are keyed by
//! the logical name, so they must apply to jobs claimed from its stripes.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

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

fn failing_jobs(queue: &str) -> Vec<awa::model::job::InsertParams> {
    (0..JOBS)
        .map(|n| awa::model::job::InsertParams {
            kind: "failing_job".into(),
            args: serde_json::json!({ "n": n }),
            opts: InsertOpts {
                queue: queue.into(),
                max_attempts: 1,
                ..Default::default()
            },
        })
        .collect()
}

async fn wait_for_runs(runs: &AtomicI64, expected: i64) {
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(20));
    while runs.load(Ordering::SeqCst) < expected {
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {} of {expected} handlers ran",
            runs.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Let the finalizes write the DLQ rows and several cleanup passes run.
    let settle = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(2));
    while tokio::time::Instant::now() < settle {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn exact_dlq_rows(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM awa.dlq_entries WHERE queue = $1")
        .bind(queue)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Without striping, `lit#0` is a queue of its own: `lit`'s DLQ setting and
/// DLQ retention do not apply to it.
#[sqlx::test(migrations = false)]
async fn an_unstriped_queue_named_like_a_stripe_keeps_its_own_dlq_settings(pool: PgPool) {
    const LOGICAL: &str = "lit";
    const LITERAL: &str = "lit#0";
    migrations::run(&pool).await.unwrap();
    let config = QueueStorageConfig::default();
    assert_eq!(config.queue_stripe_count, 1);
    let store = QueueStorage::new(config.clone()).unwrap();
    let runs = Arc::new(AtomicI64::new(0));
    let handler_runs = runs.clone();
    let client = Client::builder(pool.clone())
        .queue_storage(
            config,
            Duration::from_millis(500),
            Duration::from_millis(500),
        )
        .queue(
            LITERAL,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(20),
                ..Default::default()
            },
        )
        .dlq_enabled_by_default(true)
        .queue_dlq_enabled(LOGICAL, false)
        .dlq_retention(Duration::from_secs(86_400))
        .cleanup_interval(Duration::from_millis(100))
        .queue_retention(
            LOGICAL,
            RetentionPolicy {
                completed: Duration::from_secs(86_400),
                failed: Duration::from_secs(86_400),
                dlq: Some(Duration::from_millis(1)),
            },
        )
        .register::<FailingJob, _, _>(move |_args, _ctx| {
            let runs = handler_runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Err(JobError::terminal("always fails"))
            }
        })
        .build()
        .unwrap();
    client.start().await.unwrap();
    store
        .enqueue_params_batch(&pool, &failing_jobs(LITERAL))
        .await
        .unwrap();
    wait_for_runs(&runs, JOBS).await;
    let kept = exact_dlq_rows(&pool, LITERAL).await;
    client.shutdown(Duration::from_secs(5)).await;

    assert_eq!(
        kept, JOBS,
        "{LITERAL:?} must keep DLQ on and the global retention, not {LOGICAL:?}'s"
    );
}

/// A stripe's own DLQ retention override wins over its logical queue's.
#[sqlx::test(migrations = false)]
async fn a_stripe_retention_override_wins_over_its_logical_queue(pool: PgPool) {
    const QUEUE: &str = "stripe_retention_exact";
    let kept_stripe = format!("{QUEUE}#0");
    let purged_stripe = format!("{QUEUE}#1");
    migrations::run(&pool).await.unwrap();
    let config = QueueStorageConfig {
        queue_stripe_count: 2,
        ..Default::default()
    };
    let store = QueueStorage::new(config.clone()).unwrap();
    let day = Duration::from_secs(86_400);
    let runs = Arc::new(AtomicI64::new(0));
    let per_stripe = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let (handler_runs, handler_queues) = (runs.clone(), per_stripe.clone());
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
        .dlq_enabled_by_default(true)
        .dlq_retention(day)
        .cleanup_interval(Duration::from_millis(100))
        .queue_retention(
            QUEUE,
            RetentionPolicy {
                completed: day,
                failed: day,
                dlq: Some(Duration::from_millis(1)),
            },
        )
        .queue_retention(
            kept_stripe.clone(),
            RetentionPolicy {
                completed: day,
                failed: day,
                dlq: Some(day),
            },
        )
        .register::<FailingJob, _, _>(move |_args, ctx| {
            let (runs, queues) = (handler_runs.clone(), handler_queues.clone());
            let queue = ctx.job.queue.clone();
            async move {
                queues.lock().unwrap().push(queue);
                runs.fetch_add(1, Ordering::SeqCst);
                Err(JobError::terminal("always fails"))
            }
        })
        .build()
        .unwrap();
    client.start().await.unwrap();
    store
        .enqueue_params_batch(&pool, &failing_jobs(QUEUE))
        .await
        .unwrap();
    wait_for_runs(&runs, JOBS).await;
    let on_kept = per_stripe
        .lock()
        .unwrap()
        .iter()
        .filter(|queue| **queue == kept_stripe)
        .count() as i64;
    let kept = exact_dlq_rows(&pool, &kept_stripe).await;
    let purged = exact_dlq_rows(&pool, &purged_stripe).await;
    client.shutdown(Duration::from_secs(5)).await;

    assert!(
        on_kept > 0 && on_kept < JOBS,
        "both stripes must run jobs: {on_kept} of {JOBS} on {kept_stripe}"
    );
    assert_eq!(purged, 0, "{purged_stripe:?} takes {QUEUE:?}'s retention");
    assert_eq!(
        kept, on_kept,
        "{kept_stripe:?}'s one-day override must keep its DLQ rows"
    );
}

/// Insert a dead-lettered row for `queue`, dead-lettered `age_days` ago.
async fn backdated_dlq_row(pool: &PgPool, job_id: i64, queue: &str, age_days: i64) {
    sqlx::query(
        "INSERT INTO awa.dlq_entries
             (job_id, kind, queue, priority, dlq_reason, original_run_lease, dlq_at)
         VALUES ($1, 'failing_job', $2, 2, 'test', 1, now() - make_interval(days => $3::int))",
    )
    .bind(job_id)
    .bind(queue)
    .bind(age_days as i32)
    .execute(pool)
    .await
    .unwrap();
}

/// While striping is on, DLQ retention treats `q#N` as a stripe of `q`
/// exactly when the runtime does: `N` parses as `usize`. Stripes left over
/// from a larger former stripe count still take `q`'s retention, and a
/// suffix too large for `usize` names an independent queue that takes the
/// global retention.
#[sqlx::test(migrations = false)]
async fn dlq_retention_uses_the_runtime_stripe_identity(pool: PgPool) {
    // `short` has a 1-day override, `long` a 60-day one; the global
    // retention is 30 days.
    const SHORT: &str = "dlq_ident_short";
    const LONG: &str = "dlq_ident_long";
    let too_big = "999999999999999999999999999999";
    let usize_max = usize::MAX.to_string();
    let past_usize_max = (u128::try_from(usize::MAX).unwrap() + 1).to_string();
    // The runtime's rule, which the rows below rely on.
    assert_eq!("+1".parse::<usize>(), Ok(1));
    assert!(past_usize_max.parse::<usize>().is_err());
    // (queue, age in days, kept)
    let rows: Vec<(String, i64, bool)> = vec![
        // Current and retained stripes take the logical queue's override.
        (format!("{SHORT}#0"), 2, false),
        (format!("{SHORT}#5"), 2, false),
        (format!("{SHORT}#+1"), 2, false),
        (format!("{SHORT}#{usize_max}"), 2, false),
        (format!("{LONG}#5"), 40, true),
        // Independent queues take the global retention.
        (format!("{SHORT}#{too_big}"), 2, true),
        (format!("{SHORT}#{past_usize_max}"), 2, true),
        (format!("{SHORT}#x1"), 2, true),
        (format!("{LONG}#{too_big}"), 40, false),
    ];
    migrations::run(&pool).await.unwrap();
    for (n, (queue, age, _)) in rows.iter().enumerate() {
        backdated_dlq_row(&pool, 9_000_000 + n as i64, queue, *age).await;
    }
    let config = QueueStorageConfig {
        queue_stripe_count: 2,
        ..Default::default()
    };
    let day = Duration::from_secs(86_400);
    let client = Client::builder(pool.clone())
        .queue_storage(
            config,
            Duration::from_millis(500),
            Duration::from_millis(500),
        )
        .queue(SHORT, QueueConfig::default())
        .dlq_retention(30 * day)
        .cleanup_interval(Duration::from_millis(100))
        .queue_retention(
            SHORT,
            RetentionPolicy {
                completed: day,
                failed: day,
                dlq: Some(day),
            },
        )
        .queue_retention(
            LONG,
            RetentionPolicy {
                completed: day,
                failed: day,
                dlq: Some(60 * day),
            },
        )
        .register::<FailingJob, _, _>(|_args, _ctx| async move {
            Err(JobError::terminal("always fails"))
        })
        .build()
        .unwrap();
    client.start().await.unwrap();

    let expected_kept = rows.iter().filter(|(_, _, kept)| *kept).count() as i64;
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(20));
    loop {
        let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.dlq_entries")
            .fetch_one(&pool)
            .await
            .unwrap();
        if remaining <= expected_kept || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // One more cleanup pass, so a row wrongly purged late is caught too.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut wrong = Vec::new();
    for (queue, age, kept) in &rows {
        let present = exact_dlq_rows(&pool, queue).await == 1;
        if present != *kept {
            wrong.push(format!(
                "{queue:?} ({age}d): kept={present}, expected {kept}"
            ));
        }
    }
    client.shutdown(Duration::from_secs(5)).await;
    assert!(wrong.is_empty(), "wrong DLQ retention: {wrong:#?}");
}
