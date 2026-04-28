//! Awa portable benchmark adapter.
//!
//! Runs standardised benchmark scenarios and outputs JSON results.
//! Usage: awa-bench [--scenario <name>] [--job-count N] [--worker-count N]
//!
//! Env:
//! - `DATABASE_URL` (required)
//! - `QUEUE_STORAGE_SCHEMA` — queue_storage schema name (default `awa`)
//! - `QUEUE_STRIPE_COUNT` — logical queue stripe count (default `1`)
//! - `QUEUE_SLOT_COUNT` / `LEASE_SLOT_COUNT` / `CLAIM_SLOT_COUNT` — slot sizing (defaults 16 / 8 / 8)
//! - `QUEUE_ROTATE_MS` / `LEASE_ROTATE_MS` — rotate intervals (defaults 1000 / 50)
//!
//! The adapter defaults to the vacuum-aware queue_storage subsystem but can
//! also run the canonical engine for apples-to-apples comparisons via
//! `AWA_STORAGE_ENGINE=canonical`. Queue-storage scenarios call
//! `QueueStorage::install` + `QueueStorage::reset` to ensure a clean slot set
//! before measuring.

mod long_horizon;

use async_trait::async_trait;
use awa_macros::JobArgs;
use awa_model::{insert, migrations, InsertOpts, QueueStorage, QueueStorageConfig};
use awa_worker::{
    Client, JobContext, JobError, JobResult, QueueConfig, TransitionWorkerRole, Worker,
};
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::{Duration, Instant};

// ── Queue-storage configuration ───────────────────────────────────

pub(crate) fn queue_storage_config_with_receipts_default(
    receipts_default: bool,
) -> QueueStorageConfig {
    fn parse_bool(value: &str) -> bool {
        matches!(value, "1" | "true" | "TRUE" | "yes" | "on")
    }
    let lease_claim_receipts = match std::env::var("LEASE_CLAIM_RECEIPTS") {
        Ok(value) => parse_bool(value.as_str()),
        Err(_) => receipts_default,
    };
    QueueStorageConfig {
        schema: std::env::var("QUEUE_STORAGE_SCHEMA").unwrap_or_else(|_| "awa".into()),
        queue_slot_count: std::env::var("QUEUE_SLOT_COUNT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(16),
        lease_slot_count: std::env::var("LEASE_SLOT_COUNT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8),
        claim_slot_count: std::env::var("CLAIM_SLOT_COUNT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8),
        queue_stripe_count: std::env::var("QUEUE_STRIPE_COUNT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1),
        lease_claim_receipts,
    }
}

fn queue_storage_config() -> QueueStorageConfig {
    queue_storage_config_with_receipts_default(true)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageEngineMode {
    Canonical,
    QueueStorage,
}

pub(crate) fn storage_engine_mode() -> StorageEngineMode {
    match std::env::var("AWA_STORAGE_ENGINE")
        .unwrap_or_else(|_| "queue_storage".to_string())
        .as_str()
    {
        "canonical" => StorageEngineMode::Canonical,
        _ => StorageEngineMode::QueueStorage,
    }
}

pub(crate) fn bench_system_name() -> String {
    std::env::var("AWA_BENCH_SYSTEM").unwrap_or_else(|_| "awa".to_string())
}

fn queue_rotate_interval() -> Duration {
    Duration::from_millis(
        std::env::var("QUEUE_ROTATE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000),
    )
}

fn lease_rotate_interval() -> Duration {
    Duration::from_millis(
        std::env::var("LEASE_ROTATE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50),
    )
}

fn maybe_install_otlp_metrics() -> Option<SdkMeterProvider> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok()?;
    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "awa-portable-bench".to_string());
    let export_interval_ms = std::env::var("OTEL_EXPORT_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1000);

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .expect("failed to build OTLP metric exporter");
    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_millis(export_interval_ms))
        .build();
    let resource = Resource::builder().with_service_name(service_name).build();
    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();
    global::set_meter_provider(provider.clone());
    Some(provider)
}

/// Migrate the canonical schema, then install + reset the queue_storage
/// backend so inserts through `awa.jobs` route into it and slot state
/// starts from a clean baseline for each benchmark run.
async fn prepare_queue_storage(pool: &PgPool) -> (QueueStorage, QueueStorageConfig) {
    prepare_queue_storage_with_config(pool, queue_storage_config(), true).await
}

pub(crate) async fn prepare_canonical(pool: &PgPool) {
    migrations::run(pool).await.unwrap();
}

pub(crate) async fn prepare_queue_storage_with_config(
    pool: &PgPool,
    config: QueueStorageConfig,
    reset: bool,
) -> (QueueStorage, QueueStorageConfig) {
    migrations::run(pool).await.unwrap();
    let store = QueueStorage::new(config.clone()).expect("Invalid QueueStorageConfig");
    store
        .install(pool)
        .await
        .expect("Failed to install queue_storage backend");
    if reset {
        store
            .reset(pool)
            .await
            .expect("Failed to reset queue_storage state");
    }
    (store, config)
}

// ── Job type ──────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct BenchJob {
    pub seq: i64,
}

struct NoopWorker;

#[async_trait]
impl Worker for NoopWorker {
    fn kind(&self) -> &'static str {
        "bench_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::Completed)
    }
}

struct ChaosWorker {
    job_duration: Duration,
}

#[async_trait]
impl Worker for ChaosWorker {
    fn kind(&self) -> &'static str {
        "chaos_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        tokio::time::sleep(self.job_duration).await;
        Ok(JobResult::Completed)
    }
}

// ── Helpers ───────────────────────────────────────────────────────

fn database_url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be set")
}

async fn create_pool(max_connections: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database")
}

async fn clean_queue(pool: &PgPool, queue_name: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue_name)
        .execute(pool)
        .await
        .expect("Failed to clean jobs");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue_name)
        .execute(pool)
        .await
        .expect("Failed to clean queue_meta");
}

async fn count_by_state(
    store: &QueueStorage,
    pool: &PgPool,
    queue_name: &str,
) -> HashMap<String, i64> {
    let schema = store.schema();
    let rows: Vec<(String, i64)> = sqlx::query_as(&format!(
        r#"
        SELECT state, sum(count)::bigint AS count
        FROM (
            SELECT 'available'::text AS state, count(*)::bigint AS count
            FROM {schema}.ready_entries
            WHERE queue = $1

            UNION ALL

            SELECT state::text AS state, count(*)::bigint AS count
            FROM {schema}.leases
            WHERE queue = $1
            GROUP BY state

            UNION ALL

            SELECT 'running'::text AS state, count(*)::bigint AS count
            FROM {schema}.lease_claims AS claims
            WHERE queue = $1
              AND claims.materialized_at IS NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM {schema}.lease_claim_closures AS closures
                  WHERE closures.job_id = claims.job_id
                    AND closures.run_lease = claims.run_lease
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM {schema}.leases AS lease
                  WHERE lease.job_id = claims.job_id
                    AND lease.run_lease = claims.run_lease
              )

            UNION ALL

            SELECT state::text AS state, count(*)::bigint AS count
            FROM {schema}.deferred_jobs
            WHERE queue = $1
            GROUP BY state

            UNION ALL

            SELECT state::text AS state, count(*)::bigint AS count
            FROM {schema}.done_entries
            WHERE queue = $1
            GROUP BY state

            UNION ALL

            SELECT 'dlq'::text AS state, count(*)::bigint AS count
            FROM {schema}.dlq_entries
            WHERE queue = $1
        ) counts
        GROUP BY state
        "#,
    ))
    .bind(queue_name)
    .fetch_all(pool)
    .await
    .expect("Failed to query queue_storage state counts");
    rows.into_iter().collect()
}

async fn wait_for_completion(
    store: &QueueStorage,
    pool: &PgPool,
    queue_name: &str,
    expected: i64,
    timeout: Duration,
) {
    let start = Instant::now();
    loop {
        let counts = store
            .queue_counts(pool, queue_name)
            .await
            .expect("Failed to query queue_storage queue counts");
        if counts.completed >= expected {
            return;
        }
        if start.elapsed() > timeout {
            let state_counts = count_by_state(store, pool, queue_name).await;
            panic!(
                "Timeout after {:?}: {}/{} completed, state counts: {:?}",
                timeout, counts.completed, expected, state_counts
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn enqueue_batch(
    store: &QueueStorage,
    pool: &PgPool,
    queue_name: &str,
    count: i64,
    _use_copy: bool,
) {
    let batch_size = 500;
    for batch_start in (0..count).step_by(batch_size as usize) {
        let batch_end = (batch_start + batch_size).min(count);
        let params: Vec<_> = (batch_start..batch_end)
            .map(|i| {
                insert::params_with(
                    &BenchJob { seq: i },
                    InsertOpts {
                        queue: queue_name.into(),
                        ..Default::default()
                    },
                )
                .unwrap()
            })
            .collect();
        store.enqueue_params_batch(pool, &params).await.unwrap();
    }
}

fn build_client(
    pool: PgPool,
    queue_name: &str,
    max_workers: u32,
    storage: QueueStorageConfig,
) -> Client {
    Client::builder(pool)
        .queue(
            queue_name,
            QueueConfig {
                max_workers,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .queue_storage(storage, queue_rotate_interval(), lease_rotate_interval())
        .transition_role(TransitionWorkerRole::QueueStorageTarget)
        .register_worker(NoopWorker)
        .build()
        .expect("Failed to build client")
}

// ── Scenarios ─────────────────────────────────────────────────────

#[derive(Serialize)]
struct BenchmarkResult {
    system: String,
    scenario: String,
    config: serde_json::Value,
    results: serde_json::Value,
}

/// Scenario 1: Enqueue throughput — insert N jobs as fast as possible.
async fn scenario_enqueue_throughput(job_count: i64) -> BenchmarkResult {
    let pool = create_pool(20).await;
    let (store, _config) = prepare_queue_storage(&pool).await;
    let queue = "awa_enqueue_bench";

    let start = Instant::now();
    enqueue_batch(&store, &pool, queue, job_count, true).await;
    let elapsed = start.elapsed();

    let jobs_per_sec = job_count as f64 / elapsed.as_secs_f64();
    clean_queue(&pool, queue).await;

    BenchmarkResult {
        system: "awa".into(),
        scenario: "enqueue_throughput".into(),
        config: serde_json::json!({ "job_count": job_count }),
        results: serde_json::json!({
            "duration_ms": elapsed.as_millis(),
            "jobs_per_sec": jobs_per_sec,
        }),
    }
}

/// Scenario 2: Worker throughput — enqueue N jobs, then drain with workers.
async fn scenario_worker_throughput(job_count: i64, worker_count: u32) -> BenchmarkResult {
    let pool = create_pool(20).await;
    let (store, config) = prepare_queue_storage(&pool).await;
    let queue = "awa_worker_bench";

    // Pre-enqueue all jobs
    enqueue_batch(&store, &pool, queue, job_count, true).await;

    // Start workers and measure drain time
    let client = build_client(pool.clone(), queue, worker_count, config);
    let start = Instant::now();
    client.start().await.expect("Failed to start client");

    wait_for_completion(&store, &pool, queue, job_count, Duration::from_secs(120)).await;
    let elapsed = start.elapsed();

    client.shutdown(Duration::from_secs(5)).await;

    let jobs_per_sec = job_count as f64 / elapsed.as_secs_f64();
    clean_queue(&pool, queue).await;

    BenchmarkResult {
        system: "awa".into(),
        scenario: "worker_throughput".into(),
        config: serde_json::json!({
            "job_count": job_count,
            "worker_count": worker_count,
        }),
        results: serde_json::json!({
            "duration_ms": elapsed.as_millis(),
            "jobs_per_sec": jobs_per_sec,
        }),
    }
}

/// Scenario 3: Pickup latency — enqueue one job at a time to an idle queue.
async fn scenario_pickup_latency(iterations: i64, worker_count: u32) -> BenchmarkResult {
    let pool = create_pool(20).await;
    let (store, config) = prepare_queue_storage(&pool).await;
    let queue = "awa_latency_bench";

    let client = build_client(pool.clone(), queue, worker_count, config);
    client.start().await.expect("Failed to start client");

    // Let the client stabilise
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut latencies_us: Vec<u64> = Vec::with_capacity(iterations as usize);

    for i in 0..iterations {
        let insert_time = Instant::now();
        let params = [insert::params_with(
            &BenchJob { seq: i },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .unwrap()];
        store.enqueue_params_batch(&pool, &params).await.unwrap();

        // Observe cumulative completed count instead of polling one job row.
        // Queue-storage prune can rotate completed rows away, but queue_counts()
        // preserves the terminal rollup.
        wait_for_completion(&store, &pool, queue, i + 1, Duration::from_secs(10)).await;
        latencies_us.push(insert_time.elapsed().as_micros() as u64);
    }

    client.shutdown(Duration::from_secs(5)).await;

    latencies_us.sort();
    let len = latencies_us.len();
    let p50 = latencies_us[len / 2];
    let p95 = latencies_us[(len as f64 * 0.95) as usize];
    let p99 = latencies_us[(len as f64 * 0.99) as usize];
    let mean = latencies_us.iter().sum::<u64>() as f64 / len as f64;

    clean_queue(&pool, queue).await;

    BenchmarkResult {
        system: "awa".into(),
        scenario: "pickup_latency".into(),
        config: serde_json::json!({
            "iterations": iterations,
            "worker_count": worker_count,
        }),
        results: serde_json::json!({
            "mean_us": mean,
            "p50_us": p50,
            "p95_us": p95,
            "p99_us": p99,
        }),
    }
}

/// Scenario: migrate_only — connect, run migrations + install the queue_storage
/// backend, exit. No workers. Used by chaos harnesses that need a warm schema
/// before they start their own inserts.
async fn scenario_migrate_only() {
    let pool = create_pool(5).await;
    prepare_queue_storage(&pool).await;
    eprintln!("[awa] Migrations + queue_storage install complete.");
}

/// Scenario: worker_only — connect, run migrations, start a ChaosWorker client, block until killed.
async fn scenario_worker_only() {
    let max_connections: u32 = std::env::var("MAX_CONNECTIONS")
        .unwrap_or_else(|_| "20".into())
        .parse()
        .expect("MAX_CONNECTIONS must be an integer");

    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");
    let (_store, config) = prepare_queue_storage(&pool).await;

    let worker_count: u32 = std::env::var("WORKER_COUNT")
        .unwrap_or_else(|_| "10".into())
        .parse()
        .expect("WORKER_COUNT must be an integer");

    let job_duration_ms: u64 = std::env::var("JOB_DURATION_MS")
        .unwrap_or_else(|_| "30000".into())
        .parse()
        .expect("JOB_DURATION_MS must be an integer");

    let heartbeat_staleness_secs: u64 = std::env::var("HEARTBEAT_STALENESS_SECS")
        .unwrap_or_else(|_| "15".into())
        .parse()
        .expect("HEARTBEAT_STALENESS_SECS must be an integer");

    let rescue_interval_secs: u64 = std::env::var("RESCUE_INTERVAL_SECS")
        .unwrap_or_else(|_| "5".into())
        .parse()
        .expect("RESCUE_INTERVAL_SECS must be an integer");

    let chaos_worker = ChaosWorker {
        job_duration: Duration::from_millis(job_duration_ms),
    };

    let client = Client::builder(pool)
        .queue(
            "chaos",
            QueueConfig {
                max_workers: worker_count,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .queue_storage(config, queue_rotate_interval(), lease_rotate_interval())
        .heartbeat_interval(Duration::from_secs(5))
        .heartbeat_staleness(Duration::from_secs(heartbeat_staleness_secs))
        .heartbeat_rescue_interval(Duration::from_secs(rescue_interval_secs))
        .register_worker(chaos_worker)
        .build()
        .expect("Failed to build chaos client");

    client.start().await.expect("Failed to start chaos client");
    eprintln!(
        "[awa] worker_only: started with {} workers, job_duration={}ms. Blocking until killed.",
        worker_count, job_duration_ms
    );

    // Block until SIGKILL or ctrl-c — the orchestrator will kill us.
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl_c");
    eprintln!("[awa] worker_only: received signal, exiting.");
}

// ── Main ──────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();
    let _meter_provider = maybe_install_otlp_metrics();

    let scenario = std::env::var("SCENARIO").unwrap_or_else(|_| "all".into());
    let job_count: i64 = std::env::var("JOB_COUNT")
        .unwrap_or_else(|_| "10000".into())
        .parse()
        .expect("JOB_COUNT must be an integer");
    let worker_count: u32 = std::env::var("WORKER_COUNT")
        .unwrap_or_else(|_| "50".into())
        .parse()
        .expect("WORKER_COUNT must be an integer");
    let latency_iterations: i64 = std::env::var("LATENCY_ITERATIONS")
        .unwrap_or_else(|_| "100".into())
        .parse()
        .expect("LATENCY_ITERATIONS must be an integer");

    let mut results = Vec::new();

    if scenario == "all" || scenario == "enqueue_throughput" {
        eprintln!("[awa] Running enqueue_throughput...");
        results.push(scenario_enqueue_throughput(job_count).await);
    }
    if scenario == "all" || scenario == "worker_throughput" {
        eprintln!("[awa] Running worker_throughput...");
        results.push(scenario_worker_throughput(job_count, worker_count).await);
    }
    if scenario == "all" || scenario == "pickup_latency" {
        eprintln!("[awa] Running pickup_latency...");
        results.push(scenario_pickup_latency(latency_iterations, worker_count).await);
    }

    if scenario == "migrate_only" {
        scenario_migrate_only().await;
        return;
    }

    if scenario == "worker_only" {
        scenario_worker_only().await;
        return;
    }

    if scenario == "long_horizon" {
        long_horizon::run().await;
        return;
    }

    // Output results as JSON array to stdout
    println!("{}", serde_json::to_string_pretty(&results).unwrap());
}
