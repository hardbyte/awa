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

use async_trait::async_trait;
use awa::model::{insert_with, migrations, InsertOpts};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

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

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct FailureModeTelemetryJob {
    mode: String,
}

struct FailureModeWorker;

#[async_trait]
impl Worker for FailureModeWorker {
    fn kind(&self) -> &'static str {
        "failure_mode_telemetry_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: FailureModeTelemetryJob =
            serde_json::from_value(ctx.job.args.clone()).map_err(JobError::retryable)?;

        match args.mode.as_str() {
            "complete" => Ok(JobResult::Completed),
            "terminal_fail" => Err(JobError::terminal("intentional telemetry test failure")),
            "retry_once" => {
                if ctx.job.attempt == 1 {
                    // The test backdates run_at after the rows enter retryable,
                    // so retry timing never depends on CI scheduling.
                    Ok(JobResult::RetryAfter(Duration::from_secs(3600)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "callback_timeout" => {
                if ctx.job.attempt == 1 {
                    // Keep the callback parked until the test backdates
                    // callback_timeout_at after verifying waiting_external rows.
                    let callback = ctx
                        .register_callback(Duration::from_secs(3600))
                        .await
                        .map_err(JobError::retryable)?;
                    Ok(JobResult::WaitForCallback(callback))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            other => Err(JobError::terminal(format!(
                "unknown telemetry test mode: {other}"
            ))),
        }
    }
}

// ── OTLP + Prometheus helpers ───────────────────────────────────────

fn build_otlp_meter_provider(endpoint: &str, service_name: &str) -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .expect("Failed to build OTLP metric exporter");

    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(1))
        .build();

    let resource = Resource::builder()
        .with_service_name(service_name.to_owned())
        .build();

    SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build()
}

async fn wait_for_job_count(pool: &sqlx::PgPool, queue: &str, state: &str, min: i64) {
    let start = std::time::Instant::now();
    loop {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM awa.jobs WHERE queue = $1 AND state = $2::awa.job_state",
        )
        .bind(queue)
        .bind(state)
        .fetch_one(pool)
        .await
        .expect("Failed to query job count");

        if count >= min {
            return;
        }

        if start.elapsed() > Duration::from_secs(30) {
            panic!("Timed out waiting for {min} {state} jobs in queue {queue}; only {count} found");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_leader(client: &Client, timeout: Duration) {
    let start = std::time::Instant::now();
    loop {
        if client.health_check().await.leader {
            return;
        }
        if start.elapsed() > timeout {
            panic!("Timed out waiting for single telemetry client to become leader");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Start a TCP proxy that forwards traffic to target_addr.
/// Aborting the returned handle kills the proxy, severing all connections.
async fn start_tcp_proxy(target_addr: &str) -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind TCP proxy listener");
    let port = listener.local_addr().unwrap().port();
    let target = target_addr.to_string();
    let handle = tokio::spawn(async move {
        while let Ok((mut client_stream, _)) = listener.accept().await {
            let target = target.clone();
            tokio::spawn(async move {
                if let Ok(mut server_stream) = TcpStream::connect(&target).await {
                    let _ =
                        tokio::io::copy_bidirectional(&mut client_stream, &mut server_stream).await;
                }
            });
        }
    });
    (port, handle)
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

/// Query Prometheus and return the sum across all returned series, or None.
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
    let mut total = 0.0;
    let mut found = false;
    for result in body.data.result {
        if let Ok(value) = result.value.1.parse::<f64>() {
            total += value;
            found = true;
        }
    }
    found.then_some(total)
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

/// Validates that failure-path metrics (failed, retried, rescues) reach Prometheus
/// via the full OTLP gRPC export pipeline.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_failure_path_metrics_reach_prometheus() {
    let pool = setup_pool().await;
    let queue = "telemetry_failure_path";
    clean_queue(&pool, queue).await;

    // 1. Configure OTLP exporter and set as global.
    let meter_provider = build_otlp_meter_provider(&otlp_endpoint(), "awa-failure-path-test");
    global::set_meter_provider(meter_provider.clone());

    // 2. Build client with fast maintenance intervals, but keep retry/callback
    // transitions under explicit DB control to avoid CI timing flakes (#67).
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .heartbeat_interval(Duration::from_millis(50))
        .promote_interval(Duration::from_millis(50))
        .callback_rescue_interval(Duration::from_millis(150))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .register_worker(FailureModeWorker)
        .build()
        .expect("Failed to build failure-path client");

    client
        .start()
        .await
        .expect("Failed to start failure-path client");
    wait_for_leader(&client, Duration::from_secs(5)).await;

    // 3. Insert jobs across failure modes.
    let modes = [
        ("complete", 3, 3),         // 3 jobs, max_attempts 3
        ("terminal_fail", 2, 1),    // 2 jobs, max_attempts 1 → immediate terminal
        ("callback_timeout", 2, 3), // 2 jobs, max_attempts 3 → callback rescue then complete
        ("retry_once", 2, 3),       // 2 jobs, max_attempts 3 → retry then complete
    ];

    for (mode, count, max_attempts) in modes {
        for i in 0..count {
            insert_with(
                &pool,
                &FailureModeTelemetryJob {
                    mode: mode.to_string(),
                },
                InsertOpts {
                    queue: queue.into(),
                    max_attempts,
                    ..Default::default()
                },
            )
            .await
            .unwrap_or_else(|_| panic!("Failed to insert {mode} job {i}"));
        }
    }

    // 4. Wait for the stable intermediate states, then force the timed
    // transitions from the database so the test doesn't rely on wall clock.
    wait_for_job_count(&pool, queue, "completed", 3).await;
    wait_for_job_count(&pool, queue, "failed", 2).await;
    wait_for_job_count(&pool, queue, "waiting_external", 2).await;
    wait_for_job_count(&pool, queue, "retryable", 2).await;

    sqlx::query(
        "UPDATE awa.jobs SET callback_timeout_at = now() - interval '1 second' \
         WHERE queue = $1 AND state = 'waiting_external'",
    )
    .bind(queue)
    .execute(&pool)
    .await
    .expect("Failed to backdate callback_timeout_at");

    sqlx::query(
        "UPDATE awa.jobs SET run_at = now() - interval '1 second' \
         WHERE queue = $1 AND state = 'retryable'",
    )
    .bind(queue)
    .execute(&pool)
    .await
    .expect("Failed to backdate retryable run_at");

    // 5. Wait for terminal states: 7 completed (3 + 2 callback + 2 retry) + 2 failed.
    let expected_completed = 7_i64;
    let expected_failed = 2_i64;

    wait_for_job_count(&pool, queue, "completed", expected_completed).await;
    wait_for_job_count(&pool, queue, "failed", expected_failed).await;

    // 6. Shutdown + flush to push metrics to the collector.
    client.shutdown(Duration::from_secs(5)).await;
    meter_provider
        .force_flush()
        .expect("Failed to flush meter provider");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 7. Query Prometheus for failure-path metrics.
    let http = reqwest::Client::new();
    let timeout = Duration::from_secs(60);

    eprintln!("Querying Prometheus for failure-path metrics...");

    let completed = wait_for_metric(
        &http,
        "awa_job_completed_total",
        expected_completed as f64,
        timeout,
    )
    .await;
    eprintln!("  awa_job_completed_total = {completed}");

    let failed = wait_for_metric(
        &http,
        "awa_job_failed_total",
        expected_failed as f64,
        timeout,
    )
    .await;
    eprintln!("  awa_job_failed_total = {failed}");

    let retried = wait_for_metric(&http, "awa_job_retried_total", 2.0, timeout).await;
    eprintln!("  awa_job_retried_total = {retried}");

    let rescues = wait_for_metric(&http, "awa_maintenance_rescues_total", 2.0, timeout).await;
    eprintln!("  awa_maintenance_rescues_total = {rescues}");

    let claimed = wait_for_metric(&http, "awa_job_claimed_total", 9.0, timeout).await;
    eprintln!("  awa_job_claimed_total = {claimed}");

    let duration_count =
        wait_for_metric(&http, "awa_job_duration_seconds_count", 5.0, timeout).await;
    eprintln!("  awa_job_duration_seconds_count = {duration_count}");

    assert!(completed >= expected_completed as f64);
    assert!(failed >= expected_failed as f64);
    assert!(retried >= 2.0, "Expected retried >= 2, got {retried}");
    assert!(rescues >= 2.0, "Expected rescues >= 2, got {rescues}");
    assert!(claimed >= 9.0, "Expected claimed >= 9, got {claimed}");
    assert!(
        duration_count >= 5.0,
        "Expected duration count >= 5, got {duration_count}"
    );

    let _ = meter_provider.shutdown();
    eprintln!("Failure-path OTLP telemetry test passed!");
}

/// Validates that job processing is unaffected when the OTLP collector dies mid-flight.
///
/// Uses an in-process TCP proxy to the real collector. Phase 1 verifies metrics
/// flow through the proxy. Phase 2 kills the proxy (simulating collector death)
/// and asserts jobs still complete and health checks pass.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_collector_death_does_not_block_job_processing() {
    let pool = setup_pool().await;
    let queue = "telemetry_collector_death";
    clean_queue(&pool, queue).await;

    // 1. Start TCP proxy forwarding to the real OTLP collector.
    let otlp_target = otlp_endpoint()
        .strip_prefix("http://")
        .unwrap_or("localhost:4317")
        .to_string();
    let (proxy_port, proxy_handle) = start_tcp_proxy(&otlp_target).await;
    let proxy_endpoint = format!("http://127.0.0.1:{proxy_port}");
    eprintln!("TCP proxy listening on {proxy_endpoint} → {otlp_target}");

    // 2. Configure OTLP exporter through the proxy.
    let meter_provider = build_otlp_meter_provider(&proxy_endpoint, "awa-collector-death-test");
    global::set_meter_provider(meter_provider.clone());

    // 3. Build + start client.
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .register::<TelemetryJob, _, _>(|_args, _ctx| async { Ok(JobResult::Completed) })
        .build()
        .expect("Failed to build collector-death client");

    client
        .start()
        .await
        .expect("Failed to start collector-death client");

    // ── Phase 1: collector alive ──
    let phase1_jobs = 5_i64;
    for i in 0..phase1_jobs {
        insert_with(
            &pool,
            &TelemetryJob {
                value: format!("alive-{i}"),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert phase-1 job");
    }

    wait_for_job_count(&pool, queue, "completed", phase1_jobs).await;
    eprintln!("Phase 1: {phase1_jobs} jobs completed with live collector");

    // Flush to ensure at least one export went through the proxy.
    meter_provider
        .force_flush()
        .expect("Failed to flush meter provider");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify the pipeline was live by checking Prometheus.
    let http = reqwest::Client::new();
    let completed = wait_for_metric(
        &http,
        "awa_job_completed_total",
        1.0,
        Duration::from_secs(30),
    )
    .await;
    eprintln!("Phase 1: Prometheus confirms awa_job_completed_total = {completed}");

    // ── Phase 2: kill the collector proxy ──
    proxy_handle.abort();
    eprintln!("Phase 2: TCP proxy killed — OTLP collector is now unreachable");

    // Insert more jobs while the collector is dead.
    let phase2_jobs = 5_i64;
    for i in 0..phase2_jobs {
        insert_with(
            &pool,
            &TelemetryJob {
                value: format!("dead-{i}"),
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert phase-2 job");
    }

    // Jobs must still complete — the dead collector must not block processing.
    wait_for_job_count(&pool, queue, "completed", phase1_jobs + phase2_jobs).await;
    eprintln!(
        "Phase 2: all {} jobs completed with dead collector",
        phase1_jobs + phase2_jobs
    );

    // Health check: the runtime loops must still be alive.
    let health = client.health_check().await;
    assert!(
        health.poll_loop_alive,
        "Dispatch loop should still be alive after collector death"
    );
    assert!(
        health.heartbeat_alive,
        "Heartbeat loop should still be alive after collector death"
    );

    client.shutdown(Duration::from_secs(5)).await;
    let _ = meter_provider.shutdown();
    eprintln!("Collector-death resilience test passed!");
}
