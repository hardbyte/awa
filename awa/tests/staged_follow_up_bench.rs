//! ADR-046 scale and throughput check for handler-staged follow-ups.
//!
//! Drains the same batch of trigger jobs under each staging pattern and
//! reports throughput, so the cost of staging can be compared
//! with no follow-ups and with a registered `Completed` spec. Ignored by
//! default; run with:
//!
//! ```sh
//! DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
//!   cargo test -p awa --test staged_follow_up_bench -- --ignored --nocapture
//! ```
//!
//! `STAGED_BENCH_JOBS` overrides the batch size (default 2000),
//! `STAGED_BENCH_ENGINE=canonical` runs on the canonical engine, and
//! `STAGED_BENCH_SCENARIOS` picks scenarios by index (for example `0,2`).
//!
//! Progress is counted in process as handlers return, not by polling
//! `awa.jobs`: on queue storage that count takes seconds and would swamp the
//! measurement. A handler keeps its worker slot until a finalizer slot is
//! free, so slow completions still throttle the count. One query at the end
//! confirms every trigger committed.

use awa::{Client, EnqueueRequest, JobArgs, JobResult, QueueConfig, UniqueOpts};
use awa_testing::setup::TestDatabase;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct BenchTrigger {
    n: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct BenchFollowUp {
    key: i64,
    slot: i64,
}

#[derive(Debug, Clone, Copy)]
enum Scenario {
    /// No follow-ups: the batched completion path.
    Baseline,
    /// A registered `Completed` spec: every job takes the per-job path.
    RegisteredSpec,
    /// One job in a hundred stages one follow-up.
    StageOnePercent,
    /// Every job stages one follow-up.
    StageEvery,
    /// Every job stages one unique follow-up over 10 keys: mostly skipped
    /// duplicates.
    StageUniqueHeavyDedup,
    /// Every job stages the per-attempt maximum.
    StageAtLimit,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Scenario::Baseline => "baseline (no follow-ups)",
            Scenario::RegisteredSpec => "registered Completed spec",
            Scenario::StageOnePercent => "stage 1 follow-up, 1% of jobs",
            Scenario::StageEvery => "stage 1 follow-up, every job",
            Scenario::StageUniqueHeavyDedup => "stage 1 unique follow-up, 10 keys",
            Scenario::StageAtLimit => "stage 16 follow-ups, every job",
        }
    }
}

async fn run(scenario: Scenario, jobs: i64, canonical: bool) -> (Duration, i64) {
    let db = if canonical {
        TestDatabase::canonical().await
    } else {
        TestDatabase::queue_storage().await
    };
    let pool = db.pool().clone();
    let queue = "staged_bench";

    let mut builder = Client::builder(pool.clone());
    if canonical {
        builder = builder.canonical_storage();
    }
    let returned = Arc::new(AtomicI64::new(0));
    let handler_returned = returned.clone();
    let mut builder = builder.queue(
        queue,
        QueueConfig {
            max_workers: 64,
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
    );
    builder = builder.register::<BenchTrigger, _, _>(move |args, ctx| {
        let follow_ups = ctx.follow_ups();
        let returned = handler_returned.clone();
        async move {
            returned.fetch_add(1, Ordering::Relaxed);
            let staged = match scenario {
                Scenario::Baseline | Scenario::RegisteredSpec => 0,
                Scenario::StageOnePercent => i64::from(args.n % 100 == 0),
                Scenario::StageEvery | Scenario::StageUniqueHeavyDedup => 1,
                Scenario::StageAtLimit => awa::MAX_STAGED_FOLLOW_UPS as i64,
            };
            for slot in 0..staged {
                let request = if matches!(scenario, Scenario::StageUniqueHeavyDedup) {
                    EnqueueRequest::new(BenchFollowUp {
                        key: args.n % 10,
                        slot: 0,
                    })
                    .unique(UniqueOpts::default())
                } else {
                    EnqueueRequest::new(BenchFollowUp { key: args.n, slot })
                };
                follow_ups.enqueue_on_completion(request.queue("staged_bench_follow_ups"))?;
            }
            Ok(JobResult::Completed)
        }
    });
    if matches!(scenario, Scenario::RegisteredSpec) {
        builder =
            builder.on_completed_enqueue_with::<BenchTrigger, BenchFollowUp, _>(|args, _job| {
                EnqueueRequest::new(BenchFollowUp {
                    key: args.n,
                    slot: 0,
                })
                .queue("staged_bench_follow_ups")
            });
    }
    let client = builder.build().unwrap();

    let triggers: Vec<awa::InsertParams> = (0..jobs)
        .map(|n| awa::InsertParams {
            kind: BenchTrigger::kind().into(),
            args: serde_json::json!({ "n": n }),
            opts: awa::InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        })
        .collect();
    for chunk in triggers.chunks(500) {
        awa::insert_many(&pool, chunk).await.unwrap();
    }

    let started = Instant::now();
    client.start().await.unwrap();
    while returned.load(Ordering::Relaxed) < jobs {
        assert!(
            started.elapsed() < Duration::from_secs(600),
            "{}: only {} of {jobs} handlers returned",
            scenario.name(),
            returned.load(Ordering::Relaxed)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let elapsed = started.elapsed();
    client.shutdown(Duration::from_secs(30)).await;
    let completed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE kind = $1 AND state = 'completed'")
            .bind(BenchTrigger::kind())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        completed,
        jobs,
        "{}: every trigger committed",
        scenario.name()
    );
    let follow_ups: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE kind = $1")
        .bind(BenchFollowUp::kind())
        .fetch_one(&pool)
        .await
        .unwrap();
    (elapsed, follow_ups)
}

#[tokio::test]
#[ignore = "throughput benchmark; run explicitly with --ignored"]
async fn staged_follow_up_throughput() {
    let jobs: i64 = std::env::var("STAGED_BENCH_JOBS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2000);
    let canonical = std::env::var("STAGED_BENCH_ENGINE").as_deref() == Ok("canonical");
    let all = [
        Scenario::Baseline,
        Scenario::RegisteredSpec,
        Scenario::StageOnePercent,
        Scenario::StageEvery,
        Scenario::StageUniqueHeavyDedup,
        Scenario::StageAtLimit,
    ];
    let scenarios: Vec<Scenario> = match std::env::var("STAGED_BENCH_SCENARIOS") {
        Ok(picked) => picked
            .split(',')
            .filter_map(|index| index.trim().parse::<usize>().ok())
            .filter_map(|index| all.get(index).copied())
            .collect(),
        Err(_) => all.to_vec(),
    };
    let engine = if canonical {
        "canonical"
    } else {
        "queue storage"
    };
    println!("\n{jobs} trigger jobs per scenario, {engine}, 64 workers");
    println!(
        "{:<38} {:>10} {:>12} {:>12}",
        "scenario", "drain s", "jobs/s", "follow-ups"
    );
    for scenario in scenarios {
        let (elapsed, follow_ups) = run(scenario, jobs, canonical).await;
        println!(
            "{:<38} {:>10.2} {:>12.0} {:>12}",
            scenario.name(),
            elapsed.as_secs_f64(),
            jobs as f64 / elapsed.as_secs_f64(),
            follow_ups
        );
        let expected = match scenario {
            Scenario::Baseline => 0,
            Scenario::RegisteredSpec | Scenario::StageEvery => jobs,
            Scenario::StageOnePercent => (jobs + 99) / 100,
            Scenario::StageUniqueHeavyDedup => 10,
            Scenario::StageAtLimit => jobs * awa::MAX_STAGED_FOLLOW_UPS as i64,
        };
        assert_eq!(follow_ups, expected, "{}", scenario.name());
    }
}
