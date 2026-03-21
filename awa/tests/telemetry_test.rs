//! OTLP integration test — validates that AWA metrics reach an external
//! Prometheus-compatible collector via OTLP gRPC export.
//!
//! Requires:
//! - A running Postgres instance (DATABASE_URL)
//! - A running OTLP collector with Prometheus query API (e.g. grafana/otel-lgtm)
//!
//! Marked `#[ignore]` — only runs when explicitly requested:
//!   cargo test -p awa --test telemetry_test -- --ignored --nocapture
//!
//! See docs/test-plan.md for local setup instructions.

use awa::model::{insert_with, migrations, InsertOpts};
use awa::{Client, JobArgs, JobResult, QueueConfig};
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

// ── Helpers ──────────────────────────────────────────────────────────

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn otlp_endpoint() -> String {
    std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string())
}

fn prometheus_url() -> String {
    std::env::var("PROMETHEUS_URL").unwrap_or_else(|_| "http://localhost:9090".to_string())
}

async fn setup_pool() -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");
    migrations::run(&pool).await.expect("Failed to migrate");
    pool
}

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

// ── Job type ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct TelemetryJob {
    pub value: String,
}

// ── Prometheus query helpers ─────────────────────────────────────────

/// Response shape for Prometheus instant query API.
#[derive(Debug, Deserialize)]
struct PromResponse {
    status: String,
    data: PromData,
}

#[derive(Debug, Deserialize)]
struct PromData {
    result: Vec<PromResult>,
}

#[derive(Debug, Deserialize)]
struct PromResult {
    value: (f64, String),
}

/// Query Prometheus and return the first result's value, or None.
async fn prom_query(client: &reqwest::Client, metric: &str) -> Option<f64> {
    let url = format!("{}/api/v1/query", prometheus_url());
    let resp = client
        .get(&url)
        .query(&[("query", metric)])
        .send()
        .await
        .ok()?;

    let body: PromResponse = resp.json().await.ok()?;
    if body.status != "success" {
        return None;
    }
    body.data
        .result
        .first()
        .and_then(|r| r.value.1.parse::<f64>().ok())
}

/// Retry a Prometheus query until it returns a value >= threshold or timeout.
async fn wait_for_metric(
    client: &reqwest::Client,
    metric: &str,
    min_value: f64,
    timeout: Duration,
) -> f64 {
    let start = std::time::Instant::now();
    loop {
        if let Some(value) = prom_query(client, metric).await {
            if value >= min_value {
                return value;
            }
            eprintln!(
                "  {metric} = {value} (waiting for >= {min_value}), elapsed {:?}",
                start.elapsed()
            );
        } else {
            eprintln!("  {metric} not found yet, elapsed {:?}", start.elapsed());
        }

        if start.elapsed() > timeout {
            panic!("Timed out waiting for {metric} >= {min_value} after {timeout:?}");
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// ── Test ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_otlp_metrics_reach_prometheus() {
    let pool = setup_pool().await;
    let queue = "telemetry_otlp_test";
    clean_queue(&pool, queue).await;

    // 1. Configure OTLP metric exporter targeting the collector's gRPC endpoint.
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint())
        .build()
        .expect("Failed to build OTLP metric exporter");

    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(1))
        .build();

    let resource = Resource::builder()
        .with_service_name("awa-telemetry-test")
        .build();

    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();

    // 2. Set as global meter provider so AwaMetrics::from_global() uses it.
    global::set_meter_provider(meter_provider.clone());

    // 3. Build + start Client with a worker.
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .register::<TelemetryJob, _, _>(|_args, _ctx| async { Ok(JobResult::Completed) })
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");

    // 4. Insert jobs and wait for completion.
    let num_jobs = 3;
    for i in 0..num_jobs {
        insert_with(
            &pool,
            &TelemetryJob {
                value: format!("otlp-test-{i}"),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert job");
    }

    // Wait for all jobs to complete by polling the DB.
    let start = std::time::Instant::now();
    loop {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM awa.jobs WHERE queue = $1 AND state = 'completed'",
        )
        .bind(queue)
        .fetch_one(&pool)
        .await
        .expect("Failed to query completed count");

        if count >= num_jobs {
            eprintln!("All {num_jobs} jobs completed");
            break;
        }

        if start.elapsed() > Duration::from_secs(30) {
            panic!("Timed out waiting for jobs to complete; only {count}/{num_jobs} completed");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 5. Shutdown client + flush meter provider so metrics are exported.
    client.shutdown(Duration::from_secs(5)).await;
    meter_provider
        .force_flush()
        .expect("Failed to flush meter provider");

    // Give the collector a moment to process + scrape.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 6. Query Prometheus HTTP API for AWA metrics.
    let http = reqwest::Client::new();
    let timeout = Duration::from_secs(60);

    eprintln!("Querying Prometheus for awa metrics...");

    // OTel metric names (e.g. awa.job.completed) are translated by the
    // Prometheus exporter: dots → underscores, counter → _total suffix,
    // unit "s" → _seconds suffix. Annotation units like {job} are dropped.
    let completed = wait_for_metric(&http, "awa_job_completed_total", 1.0, timeout).await;
    eprintln!("  awa.job.completed = {completed}");

    let claimed = wait_for_metric(&http, "awa_job_claimed_total", 1.0, timeout).await;
    eprintln!("  awa.job.claimed = {claimed}");

    // awa.dispatch.claim_batches — reliably fires during job execution
    // (heartbeat has a 30s default interval so may not fire in a fast test)
    let claim_batches =
        wait_for_metric(&http, "awa_dispatch_claim_batches_total", 1.0, timeout).await;
    eprintln!("  awa.dispatch.claim_batches = {claim_batches}");

    // Histogram awa.job.duration (unit: s) → awa_job_duration_seconds_count
    let duration_count =
        wait_for_metric(&http, "awa_job_duration_seconds_count", 1.0, timeout).await;
    eprintln!("  awa.job.duration count = {duration_count}");

    // 7. Assertions (wait_for_metric already panics on timeout, but
    //    let's be explicit about what we expected).
    assert!(
        completed >= 1.0,
        "Expected awa.job.completed >= 1, got {completed}"
    );
    assert!(
        claimed >= 1.0,
        "Expected awa.job.claimed >= 1, got {claimed}"
    );
    assert!(
        claim_batches >= 1.0,
        "Expected awa.dispatch.claim_batches >= 1, got {claim_batches}"
    );
    assert!(
        duration_count >= 1.0,
        "Expected awa.job.duration count >= 1, got {duration_count}"
    );

    // Clean up.
    let _ = meter_provider.shutdown();
    eprintln!("Telemetry OTLP integration test passed!");
}
