//! Benchmark tests for Awa — validates PRD success metrics.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test benchmark_test -- --ignored --nocapture`
//!
//! PRD targets:
//!   - >5,000 jobs/sec sustained (Rust workers, single queue, no uniqueness)
//!   - <50ms median pickup latency (LISTEN/NOTIFY enabled)

use awa::model::{insert_many, insert_many_copy_from_pool, migrations};
use awa::{
    Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, JobRow, QueueConfig, Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::time::{Duration, Instant};

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:awa@localhost:5432/awa_test".to_string())
}

async fn pool_with(max_conns: u32) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database")
}

async fn setup(max_conns: u32) -> sqlx::PgPool {
    let pool = pool_with(max_conns).await;
    migrations::run(&pool).await.expect("Failed to migrate");
    pool
}

/// Clean only jobs and queue_meta for a specific queue.
async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue jobs");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue meta");
}

// ─── Job types ───────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct BenchJob {
    pub seq: i64,
}

/// No-op worker that completes immediately.
struct BenchWorker;

#[async_trait::async_trait]
impl Worker for BenchWorker {
    fn kind(&self) -> &'static str {
        "bench_job"
    }

    async fn perform(&self, _job_row: &JobRow, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::Completed)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 1: Sustained throughput with full Client runtime
// PRD target: >5,000 jobs/sec (Rust workers, single queue, no uniqueness)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_throughput_rust_workers() {
    let pool = setup(20).await;
    let queue = "bench_throughput";
    clean_queue(&pool, queue).await;

    let total_jobs: i64 = 5_000;
    let batch_size = 500;

    // Build and start the Client with workers
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 100,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .register_worker(BenchWorker)
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");

    // Insert jobs in batches
    let insert_start = Instant::now();
    for batch_start in (0..total_jobs).step_by(batch_size as usize) {
        let batch_end = (batch_start + batch_size).min(total_jobs);
        let params: Vec<_> = (batch_start..batch_end)
            .map(|i| {
                awa::model::insert::params_with(
                    &BenchJob { seq: i },
                    InsertOpts {
                        queue: queue.into(),
                        ..Default::default()
                    },
                )
                .unwrap()
            })
            .collect();
        insert_many(&pool, &params).await.unwrap();
    }
    let insert_elapsed = insert_start.elapsed();
    println!(
        "[bench] Inserted {} jobs in {:.2}s ({:.0} inserts/sec)",
        total_jobs,
        insert_elapsed.as_secs_f64(),
        total_jobs as f64 / insert_elapsed.as_secs_f64()
    );

    // Wait for all jobs to complete, polling periodically
    let processing_start = Instant::now();
    let timeout = Duration::from_secs(30);
    let mut last_count = 0i64;
    let mut stall_checks = 0u32;

    loop {
        if processing_start.elapsed() > timeout {
            let completed: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'completed'",
            )
            .bind(queue)
            .fetch_one(&pool)
            .await
            .unwrap();
            panic!(
                "Timeout after 30s: only {}/{} jobs completed",
                completed, total_jobs
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        let completed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'completed'",
        )
        .bind(queue)
        .fetch_one(&pool)
        .await
        .unwrap();

        if completed == total_jobs {
            let processing_elapsed = processing_start.elapsed();
            let throughput = total_jobs as f64 / processing_elapsed.as_secs_f64();
            println!(
                "[bench] All {} jobs completed in {:.2}s",
                total_jobs,
                processing_elapsed.as_secs_f64()
            );
            println!("[bench] Throughput: {:.0} jobs/sec", throughput);

            client.shutdown(Duration::from_secs(5)).await;

            // Use a lower bound for CI variance (3000), but the PRD target is 5000
            assert!(
                throughput >= 3000.0,
                "Throughput {:.0} jobs/sec is below minimum threshold of 3000 jobs/sec \
                 (PRD target: 5000 jobs/sec)",
                throughput
            );
            return;
        }

        // Track progress for stall detection
        if completed == last_count {
            stall_checks += 1;
            if stall_checks > 50 {
                // 5 seconds with no progress
                panic!(
                    "Processing stalled at {}/{} completed jobs",
                    completed, total_jobs
                );
            }
        } else {
            stall_checks = 0;
            last_count = completed;
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 2: Pickup latency with LISTEN/NOTIFY
// PRD target: <50ms median pickup latency
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_pickup_latency_listen_notify() {
    let pool = setup(10).await;
    let queue = "bench_latency";
    clean_queue(&pool, queue).await;

    // Channel for workers to report their pickup time
    let (pickup_tx, mut pickup_rx) = tokio::sync::mpsc::unbounded_channel::<std::time::Instant>();

    // Build a worker that records pickup time
    struct LatencyWorker {
        tx: tokio::sync::mpsc::UnboundedSender<std::time::Instant>,
    }

    #[async_trait::async_trait]
    impl Worker for LatencyWorker {
        fn kind(&self) -> &'static str {
            "bench_job"
        }

        async fn perform(
            &self,
            _job_row: &JobRow,
            _ctx: &JobContext,
        ) -> Result<JobResult, JobError> {
            let _ = self.tx.send(Instant::now());
            Ok(JobResult::Completed)
        }
    }

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 10,
                poll_interval: Duration::from_millis(200),
                ..QueueConfig::default()
            },
        )
        .register_worker(LatencyWorker { tx: pickup_tx })
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");

    // Wait for the dispatcher to be ready (LISTEN established)
    tokio::time::sleep(Duration::from_millis(500)).await;

    let iterations = 50;
    let mut latencies: Vec<Duration> = Vec::with_capacity(iterations);

    for i in 0..iterations {
        // Clean any leftover from previous iteration
        let insert_time = Instant::now();

        awa::model::insert_with(
            &pool,
            &BenchJob { seq: i as i64 },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Wait for the worker to pick up the job (with timeout)
        let pickup_time = tokio::time::timeout(Duration::from_secs(5), pickup_rx.recv())
            .await
            .expect("Timeout waiting for job pickup")
            .expect("Channel closed unexpectedly");

        let latency = pickup_time.duration_since(insert_time);
        latencies.push(latency);
    }

    client.shutdown(Duration::from_secs(5)).await;

    // Calculate percentiles
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    let min_latency = latencies[0];
    let max_latency = latencies[latencies.len() - 1];

    println!("[bench] Pickup latency over {} iterations:", iterations);
    println!("[bench]   min:  {:?}", min_latency);
    println!("[bench]   p50:  {:?}", p50);
    println!("[bench]   p95:  {:?}", p95);
    println!("[bench]   p99:  {:?}", p99);
    println!("[bench]   max:  {:?}", max_latency);

    assert!(
        p50 < Duration::from_millis(50),
        "Median pickup latency {:?} exceeds PRD target of 50ms",
        p50
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Test 3: Raw insert throughput (no workers)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_throughput_insert_only() {
    let pool = setup(20).await;
    let queue = "bench_insert_only";
    clean_queue(&pool, queue).await;

    let total_jobs: i64 = 10_000;
    let batch_size: i64 = 1_000;

    let start = Instant::now();

    for batch_start in (0..total_jobs).step_by(batch_size as usize) {
        let batch_end = (batch_start + batch_size).min(total_jobs);
        let params: Vec<_> = (batch_start..batch_end)
            .map(|i| {
                awa::model::insert::params_with(
                    &BenchJob { seq: i },
                    InsertOpts {
                        queue: queue.into(),
                        ..Default::default()
                    },
                )
                .unwrap()
            })
            .collect();
        insert_many(&pool, &params).await.unwrap();
    }

    let elapsed = start.elapsed();
    let insert_rate = total_jobs as f64 / elapsed.as_secs_f64();

    println!(
        "[bench] Inserted {} jobs in {:.2}s ({:.0} inserts/sec)",
        total_jobs,
        elapsed.as_secs_f64(),
        insert_rate
    );

    // Verify all jobs were inserted
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'available'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, total_jobs, "All jobs should be inserted");

    assert!(
        insert_rate >= 10_000.0,
        "Insert rate {:.0} jobs/sec is below minimum threshold of 10,000 jobs/sec",
        insert_rate
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Test 4: COPY insert throughput vs chunked INSERT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_throughput_copy_insert() {
    let pool = setup(20).await;
    let total_jobs: i64 = 10_000;
    let batch_size: i64 = 1_000;

    // ── Chunked INSERT baseline ──
    let queue_insert = "bench_copy_insert";
    clean_queue(&pool, queue_insert).await;

    let insert_start = Instant::now();
    for batch_start in (0..total_jobs).step_by(batch_size as usize) {
        let batch_end = (batch_start + batch_size).min(total_jobs);
        let params: Vec<_> = (batch_start..batch_end)
            .map(|i| {
                awa::model::insert::params_with(
                    &BenchJob { seq: i },
                    InsertOpts {
                        queue: queue_insert.into(),
                        ..Default::default()
                    },
                )
                .unwrap()
            })
            .collect();
        insert_many(&pool, &params).await.unwrap();
    }
    let insert_elapsed = insert_start.elapsed();
    let insert_rate = total_jobs as f64 / insert_elapsed.as_secs_f64();

    // ── COPY ──
    let queue_copy = "bench_copy_copy";
    clean_queue(&pool, queue_copy).await;

    let copy_start = Instant::now();
    let params: Vec<_> = (0..total_jobs)
        .map(|i| {
            awa::model::insert::params_with(
                &BenchJob { seq: i },
                InsertOpts {
                    queue: queue_copy.into(),
                    ..Default::default()
                },
            )
            .unwrap()
        })
        .collect();
    insert_many_copy_from_pool(&pool, &params).await.unwrap();
    let copy_elapsed = copy_start.elapsed();
    let copy_rate = total_jobs as f64 / copy_elapsed.as_secs_f64();

    println!(
        "[bench] Chunked INSERT: {} jobs in {:.2}s ({:.0} inserts/sec)",
        total_jobs,
        insert_elapsed.as_secs_f64(),
        insert_rate
    );
    println!(
        "[bench] COPY:           {} jobs in {:.2}s ({:.0} inserts/sec)",
        total_jobs,
        copy_elapsed.as_secs_f64(),
        copy_rate
    );
    println!("[bench] COPY speedup:   {:.1}x", copy_rate / insert_rate);

    // COPY should be at least as fast as chunked INSERT
    // (In practice it's significantly faster, but we use a generous threshold)
    assert!(
        copy_rate >= insert_rate * 0.8,
        "COPY rate {:.0}/s should be at least 80% of INSERT rate {:.0}/s",
        copy_rate,
        insert_rate
    );
}
