//! Concurrent multi-queue lifecycle benchmark.
//!
//! Measures system throughput when multiple producers insert into multiple
//! queues while workers consume across all of them simultaneously. This is
//! the "system under realistic load" benchmark — closer to production
//! behavior than single-queue hot-path or scheduled-only benchmarks.
//!
//! Run with:
//! ```
//! DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
//!   cargo test --release --package awa --test concurrent_lifecycle_test \
//!   -- --ignored --nocapture
//! ```

mod bench_output;

use async_trait::async_trait;
use awa::model::{insert_many, migrations};
use awa::{Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use bench_output::{BenchMetrics, BenchThroughput, BenchmarkResult, SCHEMA_VERSION};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup(max_conns: u32) -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");
    migrations::run(&pool).await.expect("Failed to migrate");
    pool
}

async fn reset_runtime_state(pool: &sqlx::PgPool) {
    sqlx::query(
        "TRUNCATE awa.jobs_hot, awa.scheduled_jobs, awa.queue_meta, awa.job_unique_claims RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("Failed to reset runtime state");
}

// ─── Job types: one per "service domain" ─────────────────────────────

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct EmailJob {
    seq: i64,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct PaymentJob {
    seq: i64,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct AnalyticsJob {
    seq: i64,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct WebhookJob {
    seq: i64,
}

// ─── Workers: no-op handlers that just count ─────────────────────────

struct CountingWorker {
    kind: &'static str,
    counter: Arc<AtomicU64>,
}

#[async_trait]
impl Worker for CountingWorker {
    fn kind(&self) -> &'static str {
        self.kind
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        self.counter.fetch_add(1, Ordering::Relaxed);
        Ok(JobResult::Completed)
    }
}

// ─── Queue configuration ─────────────────────────────────────────────

const QUEUES: &[(&str, u32)] = &[
    ("lifecycle_email", 32),
    ("lifecycle_payments", 16),
    ("lifecycle_analytics", 64),
    ("lifecycle_webhooks", 16),
];

fn queue_for_kind(kind: &str) -> &str {
    match kind {
        "email_job" => "lifecycle_email",
        "payment_job" => "lifecycle_payments",
        "analytics_job" => "lifecycle_analytics",
        "webhook_job" => "lifecycle_webhooks",
        _ => "lifecycle_email",
    }
}

// ─── Producer task ───────────────────────────────────────────────────

async fn producer_task(
    pool: sqlx::PgPool,
    kind_name: &'static str,
    queue: &'static str,
    total_jobs: i64,
    batch_size: usize,
    produced: Arc<AtomicU64>,
) {
    let mut seq = 0i64;
    while seq < total_jobs {
        let batch_end = (seq + batch_size as i64).min(total_jobs);
        let params: Vec<_> = (seq..batch_end)
            .map(|i| {
                // Build InsertParams for the appropriate kind
                let opts = InsertOpts {
                    queue: queue.to_string(),
                    ..Default::default()
                };
                match kind_name {
                    "email_job" => {
                        awa::model::insert::params_with(&EmailJob { seq: i }, opts).unwrap()
                    }
                    "payment_job" => {
                        awa::model::insert::params_with(&PaymentJob { seq: i }, opts).unwrap()
                    }
                    "analytics_job" => {
                        awa::model::insert::params_with(&AnalyticsJob { seq: i }, opts).unwrap()
                    }
                    "webhook_job" => {
                        awa::model::insert::params_with(&WebhookJob { seq: i }, opts).unwrap()
                    }
                    _ => unreachable!(),
                }
            })
            .collect();

        insert_many(&pool, &params).await.unwrap();
        produced.fetch_add((batch_end - seq) as u64, Ordering::Relaxed);
        seq = batch_end;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Benchmark: concurrent producers + consumers, full lifecycle
// ═══════════════════════════════════════════════════════════════════════

/// Configures one run of the concurrent lifecycle benchmark.
struct LifecycleConfig {
    name: &'static str,
    /// Jobs per kind (4 kinds, so total = jobs_per_kind * 4)
    jobs_per_kind: i64,
    /// Insert batch size per producer
    producer_batch_size: usize,
    /// Number of concurrent producer tasks per kind
    producers_per_kind: usize,
    /// Measurement window after all jobs are inserted
    window_secs: u64,
}

async fn run_lifecycle_benchmark(pool: &sqlx::PgPool, config: &LifecycleConfig) {
    reset_runtime_state(pool).await;

    let kinds: &[&str] = &["email_job", "payment_job", "analytics_job", "webhook_job"];
    let total_jobs = config.jobs_per_kind * kinds.len() as i64;

    // Counters
    let produced = Arc::new(AtomicU64::new(0));
    let email_count = Arc::new(AtomicU64::new(0));
    let payment_count = Arc::new(AtomicU64::new(0));
    let analytics_count = Arc::new(AtomicU64::new(0));
    let webhook_count = Arc::new(AtomicU64::new(0));

    // Build client with all queues and workers
    let client = Client::builder(pool.clone())
        .queue(
            "lifecycle_email",
            QueueConfig {
                max_workers: 32,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .queue(
            "lifecycle_payments",
            QueueConfig {
                max_workers: 16,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .queue(
            "lifecycle_analytics",
            QueueConfig {
                max_workers: 64,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .queue(
            "lifecycle_webhooks",
            QueueConfig {
                max_workers: 16,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .register_worker(CountingWorker {
            kind: "email_job",
            counter: email_count.clone(),
        })
        .register_worker(CountingWorker {
            kind: "payment_job",
            counter: payment_count.clone(),
        })
        .register_worker(CountingWorker {
            kind: "analytics_job",
            counter: analytics_count.clone(),
        })
        .register_worker(CountingWorker {
            kind: "webhook_job",
            counter: webhook_count.clone(),
        })
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");

    // Start producers — multiple tasks per kind, all inserting concurrently
    let started = Instant::now();
    let mut producer_handles = Vec::new();
    let batch_size = config.producer_batch_size;
    let producers_per_kind = config.producers_per_kind;
    let jobs_per_kind = config.jobs_per_kind;

    for kind in kinds {
        let queue: &'static str = queue_for_kind(kind);
        let jobs_per_producer = jobs_per_kind / producers_per_kind as i64;
        for _ in 0..producers_per_kind {
            let pool_clone = pool.clone();
            let produced_clone = produced.clone();
            let kind_static: &'static str = kind;
            producer_handles.push(tokio::spawn(async move {
                producer_task(
                    pool_clone,
                    kind_static,
                    queue,
                    jobs_per_producer,
                    batch_size,
                    produced_clone,
                )
                .await;
            }));
        }
    }

    // Wait for all producers to finish
    for handle in producer_handles {
        handle.await.expect("Producer task panicked");
    }
    let insert_elapsed = started.elapsed();
    let insert_rate = produced.load(Ordering::Relaxed) as f64 / insert_elapsed.as_secs_f64();

    println!(
        "[lifecycle] {} insert phase: {} jobs in {:.2}s ({:.0} inserts/s) across {} producers",
        config.name,
        produced.load(Ordering::Relaxed),
        insert_elapsed.as_secs_f64(),
        insert_rate,
        config.producers_per_kind * kinds.len(),
    );

    // Wait for all jobs to complete (or timeout)
    let drain_start = Instant::now();
    let timeout = Duration::from_secs(config.window_secs);
    let deadline = drain_start + timeout;
    loop {
        let completed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.jobs_hot WHERE state = 'completed' AND queue LIKE 'lifecycle_%'",
        )
        .fetch_one(pool)
        .await
        .unwrap();

        if completed >= total_jobs {
            break;
        }
        if Instant::now() >= deadline {
            let running: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM awa.jobs_hot WHERE state IN ('available', 'running') AND queue LIKE 'lifecycle_%'",
            )
            .fetch_one(pool)
            .await
            .unwrap();
            println!(
                "[lifecycle] {} timed out: {}/{} completed, {} in-flight",
                config.name, completed, total_jobs, running
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let total_elapsed = started.elapsed();
    let drain_elapsed = drain_start.elapsed();

    client.shutdown(Duration::from_secs(5)).await;

    let handler_total = email_count.load(Ordering::Relaxed)
        + payment_count.load(Ordering::Relaxed)
        + analytics_count.load(Ordering::Relaxed)
        + webhook_count.load(Ordering::Relaxed);

    let end_to_end_rate = handler_total as f64 / total_elapsed.as_secs_f64();
    let drain_rate = handler_total as f64 / drain_elapsed.as_secs_f64();

    // Per-queue breakdown
    println!(
        "[lifecycle] {} complete: {} jobs in {:.2}s ({:.0} end-to-end/s, {:.0} drain/s)",
        config.name,
        handler_total,
        total_elapsed.as_secs_f64(),
        end_to_end_rate,
        drain_rate,
    );
    println!(
        "[lifecycle] {} per-kind: email={} payment={} analytics={} webhook={}",
        config.name,
        email_count.load(Ordering::Relaxed),
        payment_count.load(Ordering::Relaxed),
        analytics_count.load(Ordering::Relaxed),
        webhook_count.load(Ordering::Relaxed),
    );
    println!(
        "[lifecycle] {} timing: insert={:.2}s drain={:.2}s total={:.2}s",
        config.name,
        insert_elapsed.as_secs_f64(),
        drain_elapsed.as_secs_f64(),
        total_elapsed.as_secs_f64(),
    );

    // JSONL output
    let mut outcomes = HashMap::new();
    outcomes.insert("email".to_string(), email_count.load(Ordering::Relaxed));
    outcomes.insert("payment".to_string(), payment_count.load(Ordering::Relaxed));
    outcomes.insert(
        "analytics".to_string(),
        analytics_count.load(Ordering::Relaxed),
    );
    outcomes.insert("webhook".to_string(), webhook_count.load(Ordering::Relaxed));

    BenchmarkResult {
        schema_version: SCHEMA_VERSION,
        scenario: config.name.to_string(),
        language: "rust".to_string(),
        seeded: total_jobs as u64,
        metrics: BenchMetrics {
            throughput: Some(BenchThroughput {
                handler_per_s: drain_rate,
                db_finalized_per_s: end_to_end_rate,
            }),
            enqueue_per_s: Some(insert_rate),
            drain_time_s: Some(drain_elapsed.as_secs_f64()),
            latency_ms: None,
            rescue: None,
        },
        outcomes,
        metadata: Some(serde_json::json!({
            "jobs_per_kind": config.jobs_per_kind,
            "producers_per_kind": config.producers_per_kind,
            "producer_batch_size": config.producer_batch_size,
            "queues": QUEUES.iter().map(|(q, w)| format!("{q}:{w}")).collect::<Vec<_>>(),
            "total_workers": QUEUES.iter().map(|(_, w)| w).sum::<u32>(),
            "insert_time_s": insert_elapsed.as_secs_f64(),
        })),
    }
    .emit();
}

// ═══════════════════════════════════════════════════════════════════════
// Test cases
// ═══════════════════════════════════════════════════════════════════════

/// Small scale: 4 queues × 2.5k jobs = 10k total, 4 producers.
/// Quick validation that the concurrent setup works.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_concurrent_lifecycle_10k() {
    let pool = setup(30).await;
    run_lifecycle_benchmark(
        &pool,
        &LifecycleConfig {
            name: "lifecycle_10k",
            jobs_per_kind: 2_500,
            producer_batch_size: 500,
            producers_per_kind: 1,
            window_secs: 30,
        },
    )
    .await;
}

/// Pre-seeded drain: 4 queues × 2k jobs = 8k total, no concurrent producers.
/// Measures pure multi-queue consumption throughput without insert contention.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_concurrent_drain_8k() {
    let pool = setup(30).await;
    reset_runtime_state(&pool).await;

    let exporter = opentelemetry_sdk::metrics::InMemoryMetricExporter::default();
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let kinds: &[(&str, &str)] = &[
        ("email_job", "lifecycle_email"),
        ("payment_job", "lifecycle_payments"),
        ("analytics_job", "lifecycle_analytics"),
        ("webhook_job", "lifecycle_webhooks"),
    ];
    let per_kind: i64 = 2_000;
    let total = per_kind * kinds.len() as i64;

    // Pre-seed all jobs
    let seed_start = Instant::now();
    for (kind_name, queue) in kinds {
        let params: Vec<_> = (0..per_kind)
            .map(|i| {
                let opts = InsertOpts {
                    queue: queue.to_string(),
                    ..Default::default()
                };
                match *kind_name {
                    "email_job" => {
                        awa::model::insert::params_with(&EmailJob { seq: i }, opts).unwrap()
                    }
                    "payment_job" => {
                        awa::model::insert::params_with(&PaymentJob { seq: i }, opts).unwrap()
                    }
                    "analytics_job" => {
                        awa::model::insert::params_with(&AnalyticsJob { seq: i }, opts).unwrap()
                    }
                    "webhook_job" => {
                        awa::model::insert::params_with(&WebhookJob { seq: i }, opts).unwrap()
                    }
                    _ => unreachable!(),
                }
            })
            .collect();
        // Insert in batches
        for chunk in params.chunks(1000) {
            insert_many(&pool, chunk).await.unwrap();
        }
    }
    let seed_elapsed = seed_start.elapsed();
    println!(
        "[lifecycle] drain_20k: seeded {total} jobs in {:.2}s ({:.0}/s)",
        seed_elapsed.as_secs_f64(),
        total as f64 / seed_elapsed.as_secs_f64()
    );

    // Counters
    let email_count = Arc::new(AtomicU64::new(0));
    let payment_count = Arc::new(AtomicU64::new(0));
    let analytics_count = Arc::new(AtomicU64::new(0));
    let webhook_count = Arc::new(AtomicU64::new(0));

    let client = Client::builder(pool.clone())
        .queue(
            "lifecycle_email",
            QueueConfig {
                max_workers: 32,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .queue(
            "lifecycle_payments",
            QueueConfig {
                max_workers: 16,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .queue(
            "lifecycle_analytics",
            QueueConfig {
                max_workers: 64,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .queue(
            "lifecycle_webhooks",
            QueueConfig {
                max_workers: 16,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .register_worker(CountingWorker {
            kind: "email_job",
            counter: email_count.clone(),
        })
        .register_worker(CountingWorker {
            kind: "payment_job",
            counter: payment_count.clone(),
        })
        .register_worker(CountingWorker {
            kind: "analytics_job",
            counter: analytics_count.clone(),
        })
        .register_worker(CountingWorker {
            kind: "webhook_job",
            counter: webhook_count.clone(),
        })
        .build()
        .expect("Failed to build client");

    let started = Instant::now();
    client.start().await.expect("Failed to start client");

    // Wait for all jobs to drain
    let timeout = Duration::from_secs(60);
    let deadline = Instant::now() + timeout;
    loop {
        let handler_total = email_count.load(Ordering::Relaxed)
            + payment_count.load(Ordering::Relaxed)
            + analytics_count.load(Ordering::Relaxed)
            + webhook_count.load(Ordering::Relaxed);
        if handler_total >= total as u64 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Timed out: {handler_total}/{total} completed"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let drain_elapsed = started.elapsed();
    client.shutdown(Duration::from_secs(5)).await;

    meter_provider
        .force_flush()
        .expect("Failed to flush metrics");
    let resource_metrics = exporter
        .get_finished_metrics()
        .expect("Failed to get metrics");

    // Print claim and completion metrics for diagnosis
    for rm in &resource_metrics {
        for scope_metrics in &rm.scope_metrics {
            for metric in &scope_metrics.metrics {
                if metric.name.starts_with("awa.dispatch")
                    || metric.name.starts_with("awa.completion")
                {
                    if let Some(sum) = metric
                        .data
                        .as_any()
                        .downcast_ref::<opentelemetry_sdk::metrics::data::Sum<u64>>()
                    {
                        let total: u64 = sum.data_points.iter().map(|dp| dp.value).sum();
                        println!("[metrics] {}: total={total}", metric.name);
                    }
                    if let Some(hist) = metric
                        .data
                        .as_any()
                        .downcast_ref::<opentelemetry_sdk::metrics::data::Histogram<f64>>(
                    ) {
                        let mut count = 0u64;
                        let mut sum = 0.0f64;
                        let mut max = 0.0f64;
                        for dp in &hist.data_points {
                            count += dp.count;
                            sum += dp.sum;
                            if let Some(m) = dp.max {
                                max = max.max(m);
                            }
                        }
                        let mean = if count > 0 { sum / count as f64 } else { 0.0 };
                        println!(
                            "[metrics] {}: count={count} mean_ms={:.3} max_ms={:.3}",
                            metric.name,
                            mean * 1000.0,
                            max * 1000.0
                        );
                    }
                    if let Some(hist) = metric
                        .data
                        .as_any()
                        .downcast_ref::<opentelemetry_sdk::metrics::data::Histogram<u64>>(
                    ) {
                        let mut count = 0u64;
                        let mut sum = 0u64;
                        let mut max = 0u64;
                        for dp in &hist.data_points {
                            count += dp.count;
                            sum += dp.sum;
                            if let Some(m) = dp.max {
                                max = max.max(m);
                            }
                        }
                        let mean = if count > 0 {
                            sum as f64 / count as f64
                        } else {
                            0.0
                        };
                        println!(
                            "[metrics] {}: count={count} mean={mean:.1} max={max}",
                            metric.name
                        );
                    }
                }
            }
        }
    }
    let _ = meter_provider.shutdown();

    let handler_total = email_count.load(Ordering::Relaxed)
        + payment_count.load(Ordering::Relaxed)
        + analytics_count.load(Ordering::Relaxed)
        + webhook_count.load(Ordering::Relaxed);
    let drain_rate = handler_total as f64 / drain_elapsed.as_secs_f64();

    println!(
        "[lifecycle] drain: {handler_total} jobs drained in {:.2}s ({drain_rate:.0}/s)",
        drain_elapsed.as_secs_f64()
    );
    println!(
        "[lifecycle] drain per-kind: email={} payment={} analytics={} webhook={}",
        email_count.load(Ordering::Relaxed),
        payment_count.load(Ordering::Relaxed),
        analytics_count.load(Ordering::Relaxed),
        webhook_count.load(Ordering::Relaxed),
    );

    BenchmarkResult {
        schema_version: SCHEMA_VERSION,
        scenario: "drain_20k_4queue".to_string(),
        language: "rust".to_string(),
        seeded: total as u64,
        metrics: BenchMetrics {
            throughput: Some(BenchThroughput {
                handler_per_s: drain_rate,
                db_finalized_per_s: drain_rate,
            }),
            enqueue_per_s: Some(total as f64 / seed_elapsed.as_secs_f64()),
            drain_time_s: Some(drain_elapsed.as_secs_f64()),
            latency_ms: None,
            rescue: None,
        },
        outcomes: {
            let mut m = HashMap::new();
            m.insert("email".to_string(), email_count.load(Ordering::Relaxed));
            m.insert("payment".to_string(), payment_count.load(Ordering::Relaxed));
            m.insert(
                "analytics".to_string(),
                analytics_count.load(Ordering::Relaxed),
            );
            m.insert("webhook".to_string(), webhook_count.load(Ordering::Relaxed));
            m
        },
        metadata: Some(serde_json::json!({
            "jobs_per_kind": per_kind,
            "queues": QUEUES.iter().map(|(q, w)| format!("{q}:{w}")).collect::<Vec<_>>(),
            "total_workers": QUEUES.iter().map(|(_, w)| w).sum::<u32>(),
        })),
    }
    .emit();
}

/// Medium scale: 4 queues × 10k jobs = 40k total, 8 producers (2 per kind).
/// Exercises concurrent insert contention + dispatch saturation.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_concurrent_lifecycle_40k() {
    let pool = setup(30).await;
    run_lifecycle_benchmark(
        &pool,
        &LifecycleConfig {
            name: "lifecycle_40k",
            jobs_per_kind: 10_000,
            producer_batch_size: 500,
            producers_per_kind: 2,
            window_secs: 60,
        },
    )
    .await;
}

/// Large scale: 4 queues × 25k jobs = 100k total, 16 producers (4 per kind).
/// Finds the system throughput ceiling with 128 workers across 4 queues.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_concurrent_lifecycle_100k() {
    let pool = setup(40).await;
    run_lifecycle_benchmark(
        &pool,
        &LifecycleConfig {
            name: "lifecycle_100k",
            jobs_per_kind: 25_000,
            producer_batch_size: 1_000,
            producers_per_kind: 4,
            window_secs: 120,
        },
    )
    .await;
}
