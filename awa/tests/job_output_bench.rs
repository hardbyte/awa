//! ADR-048 cost check for retained job output on queue storage.
//!
//! Drains the same batch of jobs under each output pattern and reports
//! throughput, WAL bytes per job, how many jobs each compact batch row ended
//! up holding, and `get_job` latency for completed jobs. Ignored by default;
//! run with:
//!
//! ```sh
//! DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
//!   cargo test -p awa --test job_output_bench -- --ignored --nocapture
//! ```
//!
//! `OUTPUT_BENCH_JOBS` overrides the batch size (default 4000) and
//! `OUTPUT_BENCH_SCENARIOS` picks scenarios by index (for example `0,2`).
//! WAL is measured across the whole drain, so it includes claims and
//! completions; compare the rows with each other.

use awa::{Client, JobArgs, JobResult, QueueConfig, MAX_OUTPUT_BYTES};
use awa_testing::setup::TestDatabase;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct OutputBench {
    n: i64,
}

#[derive(Debug, Clone, Copy)]
enum Scenario {
    /// No output: the compact baseline.
    Baseline,
    /// A 100-byte object on every job.
    Small,
    /// A string just under the 4 KiB cap on every job.
    LargeString,
    /// An array of small integers just under the cap: the worst jsonb
    /// expansion.
    LargeNumericArray,
    /// Every attempt but the last fails with a 4 KiB output, five attempts.
    RetryStorm,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Scenario::Baseline => "baseline (no output)",
            Scenario::Small => "100 B output, every job",
            Scenario::LargeString => "4 KiB string, every job",
            Scenario::LargeNumericArray => "4 KiB numeric array, every job",
            Scenario::RetryStorm => "4 KiB output on 4 failed attempts",
        }
    }

    fn output(self, n: i64) -> Option<Value> {
        match self {
            Scenario::Baseline => None,
            Scenario::Small => Some(json!({
                "n": n,
                "status": "synced",
                "updated": 42,
                "skipped": 3,
                "source": "inventory-feed-7",
            })),
            Scenario::LargeString | Scenario::RetryStorm => {
                Some(json!("x".repeat(MAX_OUTPUT_BYTES - 64)))
            }
            Scenario::LargeNumericArray => {
                Some(Value::from(vec![1u8; (MAX_OUTPUT_BYTES - 64) / 2]))
            }
        }
    }
}

struct Measured {
    drain: Duration,
    wal_per_job: f64,
    jobs_per_batch_row: f64,
    get_job_p50: Duration,
    get_job_p95: Duration,
}

async fn wal_lsn(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn run(scenario: Scenario, jobs: i64) -> Measured {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "output_bench";
    let attempts: i16 = if matches!(scenario, Scenario::RetryStorm) {
        5
    } else {
        1
    };
    let returned = Arc::new(AtomicI64::new(0));
    let handler_returned = returned.clone();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 64,
                poll_interval: Duration::from_millis(10),
                ..Default::default()
            },
        )
        .register::<OutputBench, _, _>(move |args, ctx| {
            let output = ctx.output_slot();
            let attempt = ctx.job.attempt;
            let max_attempts = ctx.job.max_attempts;
            let returned = handler_returned.clone();
            async move {
                if let Some(value) = scenario.output(args.n) {
                    output.set(&value).unwrap();
                }
                if attempt < max_attempts {
                    return Ok(JobResult::RetryAfter(Duration::from_millis(1)));
                }
                returned.fetch_add(1, Ordering::Relaxed);
                Ok(JobResult::Completed)
            }
        })
        .build()
        .unwrap();

    let params: Vec<awa::InsertParams> = (0..jobs)
        .map(|n| awa::InsertParams {
            kind: OutputBench::kind().into(),
            args: json!({ "n": n }),
            opts: awa::InsertOpts {
                queue: queue.into(),
                max_attempts: attempts,
                ..Default::default()
            },
        })
        .collect();
    for chunk in params.chunks(500) {
        awa::insert_many(&pool, chunk).await.unwrap();
    }

    let wal_start = wal_lsn(&pool).await;
    let started = Instant::now();
    client.start().await.unwrap();
    while returned.load(Ordering::Relaxed) < jobs {
        assert!(
            started.elapsed() < Duration::from_secs(900),
            "{}: only {} of {jobs} jobs completed",
            scenario.name(),
            returned.load(Ordering::Relaxed)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let drain = started.elapsed();
    client.shutdown(Duration::from_secs(30)).await;
    let wal_end = wal_lsn(&pool).await;

    let completed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.terminal_jobs WHERE kind = $1 AND state = 'completed'",
    )
    .bind(OutputBench::kind())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(completed, jobs, "{}: every job completed", scenario.name());
    let jobs_per_batch_row: f64 = sqlx::query_scalar(
        "SELECT COALESCE(avg(completed_count), 0)::float8 FROM awa.receipt_completion_batches",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // Read back a spread of completed jobs, as the job detail view does.
    let ids: Vec<i64> = sqlx::query_scalar(
        "SELECT job_id FROM awa.terminal_jobs WHERE kind = $1 ORDER BY job_id",
    )
    .bind(OutputBench::kind())
    .fetch_all(&pool)
    .await
    .unwrap();
    let step = (ids.len() / 50).max(1);
    let mut latencies = Vec::new();
    for id in ids.iter().step_by(step) {
        let read = Instant::now();
        let job = awa::model::admin::get_job(&pool, *id).await.unwrap();
        latencies.push(read.elapsed());
        assert_eq!(
            job.output.is_some(),
            scenario.output(0).is_some(),
            "{}: job {id}",
            scenario.name()
        );
    }
    latencies.sort();
    Measured {
        drain,
        wal_per_job: (wal_end - wal_start) as f64 / jobs as f64,
        jobs_per_batch_row,
        get_job_p50: latencies[latencies.len() / 2],
        get_job_p95: latencies[latencies.len() * 95 / 100],
    }
}

#[tokio::test]
#[ignore = "cost benchmark; run explicitly with --ignored"]
async fn job_output_cost() {
    let jobs: i64 = std::env::var("OUTPUT_BENCH_JOBS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(4000);
    let all = [
        Scenario::Baseline,
        Scenario::Small,
        Scenario::LargeString,
        Scenario::LargeNumericArray,
        Scenario::RetryStorm,
    ];
    let scenarios: Vec<Scenario> = match std::env::var("OUTPUT_BENCH_SCENARIOS") {
        Ok(picked) => picked
            .split(',')
            .filter_map(|index| index.trim().parse::<usize>().ok())
            .filter_map(|index| all.get(index).copied())
            .collect(),
        Err(_) => all.to_vec(),
    };
    println!("\n{jobs} jobs per scenario, queue storage, 64 workers");
    println!(
        "{:<36} {:>8} {:>9} {:>10} {:>12} {:>12} {:>12}",
        "scenario", "drain s", "jobs/s", "WAL B/job", "jobs/batch", "get p50", "get p95"
    );
    for scenario in scenarios {
        let measured = run(scenario, jobs).await;
        println!(
            "{:<36} {:>8.2} {:>9.0} {:>10.0} {:>12.1} {:>12?} {:>12?}",
            scenario.name(),
            measured.drain.as_secs_f64(),
            jobs as f64 / measured.drain.as_secs_f64(),
            measured.wal_per_job,
            measured.jobs_per_batch_row,
            measured.get_job_p50,
            measured.get_job_p95,
        );
    }
}
