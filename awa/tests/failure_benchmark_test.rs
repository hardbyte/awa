//! Failure-mode benchmark tests for Awa.
//!
//! These benchmarks measure throughput, drain time, and recovery behaviour
//! when a configurable percentage of jobs fail, retry, hang, or trigger
//! callback timeouts.
//!
//! Run with:
//! ```
//! DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
//!   cargo test --package awa --test failure_benchmark_test -- --ignored --nocapture
//! ```
//!
//! Each test emits human-readable summaries and one @@BENCH_JSON@@ JSONL
//! record per scenario using the shared schema (schema_version=1).

mod bench_output;

use async_trait::async_trait;
use awa::model::migrations;
use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use bench_output::{BenchMetrics, BenchRescue, BenchThroughput, BenchmarkResult, SCHEMA_VERSION};
use opentelemetry_sdk::metrics::data::Sum;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
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

async fn queue_state_counts(pool: &sqlx::PgPool, queue: &str) -> HashMap<String, i64> {
    // Query both hot and scheduled tables to capture the full picture —
    // retried jobs may temporarily live in scheduled_jobs before promotion.
    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"
        SELECT state, sum(cnt)::bigint
        FROM (
            SELECT state::text AS state, count(*) AS cnt
            FROM awa.jobs_hot WHERE queue = $1 GROUP BY state
            UNION ALL
            SELECT state::text AS state, count(*) AS cnt
            FROM awa.scheduled_jobs WHERE queue = $1 GROUP BY state
        ) combined
        GROUP BY state
        "#,
    )
    .bind(queue)
    .fetch_all(pool)
    .await
    .expect("Failed to query state counts");
    rows.into_iter().collect()
}

fn sum_counter_metric(
    resource_metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
) -> u64 {
    let mut total = 0;
    for rm in resource_metrics {
        for scope_metrics in &rm.scope_metrics {
            for metric in &scope_metrics.metrics {
                if metric.name == name {
                    if let Some(sum) = metric.data.as_any().downcast_ref::<Sum<u64>>() {
                        total += sum.data_points.iter().map(|dp| dp.value).sum::<u64>();
                    }
                }
            }
        }
    }
    total
}

fn sum_counter_metric_with_attribute(
    resource_metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
    attr_name: &str,
    attr_value: &str,
) -> u64 {
    let mut total = 0;
    for rm in resource_metrics {
        for scope_metrics in &rm.scope_metrics {
            for metric in &scope_metrics.metrics {
                if metric.name != name {
                    continue;
                }
                if let Some(sum) = metric.data.as_any().downcast_ref::<Sum<u64>>() {
                    total += sum
                        .data_points
                        .iter()
                        .filter(|dp| {
                            dp.attributes.iter().any(|kv| {
                                kv.key.as_str() == attr_name && kv.value.as_str() == attr_value
                            })
                        })
                        .map(|dp| dp.value)
                        .sum::<u64>();
                }
            }
        }
    }
    total
}

// ─── Job types ───────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct FailureBenchJob {
    seq: i64,
    mode: String,
}

/// Worker that handles failure benchmark jobs deterministically.
struct FailureBenchWorker {
    handler_count: Arc<AtomicU64>,
}

#[async_trait]
impl Worker for FailureBenchWorker {
    fn kind(&self) -> &'static str {
        "failure_bench_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        self.handler_count.fetch_add(1, Ordering::Relaxed);

        let args: FailureBenchJob = serde_json::from_value(ctx.job.args.clone())
            .map_err(|err| JobError::terminal(format!("failed to decode args: {err}")))?;

        match args.mode.as_str() {
            "complete" => Ok(JobResult::Completed),
            "terminal" => Err(JobError::terminal("intentional benchmark failure")),
            "retryable" => {
                if ctx.job.attempt == 1 {
                    Ok(JobResult::RetryAfter(Duration::from_millis(50)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "callback_timeout" => {
                if ctx.job.attempt == 1 {
                    let callback = ctx
                        .register_callback(Duration::from_millis(300))
                        .await
                        .map_err(JobError::retryable)?;
                    Ok(JobResult::WaitForCallback(callback))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "deadline_hang" => {
                if ctx.job.attempt == 1 {
                    // Set a very short deadline so rescue fires quickly
                    sqlx::query(
                        "UPDATE awa.jobs_hot SET deadline_at = now() + make_interval(secs => $2) WHERE id = $1 AND run_lease = $3",
                    )
                    .bind(ctx.job.id)
                    .bind(0.2_f64)
                    .bind(ctx.job.run_lease)
                    .execute(ctx.pool())
                    .await
                    .map_err(JobError::retryable)?;

                    // Spin until cancelled
                    for _ in 0..200 {
                        if ctx.is_cancelled() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Ok(JobResult::RetryAfter(Duration::from_millis(50)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "snooze_once" => {
                // Snooze decrements attempt, so we can't use attempt to detect
                // "first time". Instead check if metadata has a snooze marker.
                let already_snoozed = ctx.job.metadata.get("snoozed").is_some();
                if !already_snoozed {
                    // Set the marker via a direct SQL update before snoozing
                    sqlx::query(
                        "UPDATE awa.jobs_hot SET metadata = metadata || '{\"snoozed\":true}'::jsonb WHERE id = $1 AND run_lease = $2",
                    )
                    .bind(ctx.job.id)
                    .bind(ctx.job.run_lease)
                    .execute(ctx.pool())
                    .await
                    .map_err(JobError::retryable)?;
                    Ok(JobResult::Snooze(Duration::from_millis(100)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            other => Err(JobError::terminal(format!(
                "unknown benchmark mode: {other}"
            ))),
        }
    }
}

// ─── Scenario runner ─────────────────────────────────────────────────

struct ScenarioConfig {
    name: String,
    total_jobs: i64,
    /// Maps mode -> count
    mode_distribution: Vec<(String, i64)>,
    max_workers: u32,
}

async fn run_scenario(pool: &sqlx::PgPool, config: &ScenarioConfig) {
    let queue = format!("bench_fail_{}", config.name);
    reset_runtime_state(pool).await;

    // Seed jobs per mode
    let mut seq: i64 = 0;
    for (mode, count) in &config.mode_distribution {
        if *count == 0 {
            continue;
        }
        sqlx::query(
            r#"
            INSERT INTO awa.jobs_hot
                (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
            SELECT
                'failure_bench_job',
                $1,
                jsonb_build_object('seq', $2 + g, 'mode', $3),
                'available'::awa.job_state,
                2,
                5,
                now(),
                '{}'::jsonb,
                '{}'::text[]
            FROM generate_series(1, $4) AS g
            "#,
        )
        .bind(&queue)
        .bind(seq)
        .bind(mode)
        .bind(*count)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("Failed to seed {mode} jobs: {e}"));
        seq += count;
    }

    // Set up metrics
    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let handler_count = Arc::new(AtomicU64::new(0));
    let client = Client::builder(pool.clone())
        .queue(
            &queue,
            QueueConfig {
                max_workers: config.max_workers,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .heartbeat_interval(Duration::from_millis(50))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(100))
        .promote_interval(Duration::from_millis(50))
        .leader_election_interval(Duration::from_millis(100))
        .register_worker(FailureBenchWorker {
            handler_count: handler_count.clone(),
        })
        .build()
        .expect("Failed to build client");

    let started = Instant::now();
    client.start().await.expect("Failed to start client");

    // Wait for all jobs to reach terminal states
    // Scenarios with deadline_hang or callback_timeout need extra time for
    // rescue cycles; 180s is generous enough for the full mixed matrix.
    let timeout = Duration::from_secs(180);
    let deadline = Instant::now() + timeout;
    loop {
        let counts = queue_state_counts(pool, &queue).await;
        let in_flight = counts.get("available").copied().unwrap_or(0)
            + counts.get("running").copied().unwrap_or(0)
            + counts.get("retryable").copied().unwrap_or(0)
            + counts.get("scheduled").copied().unwrap_or(0)
            + counts.get("waiting_external").copied().unwrap_or(0);
        if in_flight == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Timed out waiting for {} jobs to settle. In-flight: {in_flight}. Counts: {counts:?}",
            config.name
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let drain_time = started.elapsed();
    client.shutdown(Duration::from_secs(5)).await;

    // Collect metrics
    meter_provider
        .force_flush()
        .expect("Failed to flush metrics");
    let resource_metrics = exporter
        .get_finished_metrics()
        .expect("Failed to get finished metrics");

    let final_counts = queue_state_counts(pool, &queue).await;
    let completed = final_counts.get("completed").copied().unwrap_or(0) as u64;
    let failed = final_counts.get("failed").copied().unwrap_or(0) as u64;
    let cancelled = final_counts.get("cancelled").copied().unwrap_or(0) as u64;
    let handler_total = handler_count.load(Ordering::Relaxed);
    let finalized_total = completed + failed + cancelled;

    let handler_per_s = handler_total as f64 / drain_time.as_secs_f64();
    let db_per_s = finalized_total as f64 / drain_time.as_secs_f64();

    let rescues = sum_counter_metric(&resource_metrics, "awa.maintenance.rescues");
    let callback_timeouts = sum_counter_metric(&resource_metrics, "awa.job.waiting_external");
    let deadline_rescued = sum_counter_metric_with_attribute(
        &resource_metrics,
        "awa.maintenance.rescues",
        "awa.rescue.kind",
        "deadline",
    );

    // Human-readable output
    println!(
        "[failure-bench] scenario={} total={} drain={:.2}s handler={:.0}/s db_finalized={:.0}/s completed={} failed={} cancelled={} rescues={}",
        config.name, config.total_jobs, drain_time.as_secs_f64(),
        handler_per_s, db_per_s,
        completed, failed, cancelled, rescues
    );

    // JSONL output
    let mut outcomes: HashMap<String, u64> = HashMap::new();
    for (state, count) in &final_counts {
        if *count > 0 {
            outcomes.insert(state.clone(), *count as u64);
        }
    }

    let rescue_metrics = if rescues > 0 || callback_timeouts > 0 || deadline_rescued > 0 {
        Some(BenchRescue {
            deadline_rescued: if deadline_rescued > 0 {
                Some(deadline_rescued)
            } else {
                None
            },
            callback_timeouts: if callback_timeouts > 0 {
                Some(callback_timeouts)
            } else {
                None
            },
            heartbeat_rescued: None,
        })
    } else {
        None
    };

    BenchmarkResult {
        schema_version: SCHEMA_VERSION,
        scenario: config.name.clone(),
        language: "rust".to_string(),
        seeded: config.total_jobs as u64,
        metrics: BenchMetrics {
            throughput: Some(BenchThroughput {
                handler_per_s,
                db_finalized_per_s: db_per_s,
            }),
            enqueue_per_s: None,
            drain_time_s: Some(drain_time.as_secs_f64()),
            latency_ms: None,
            rescue: rescue_metrics,
        },
        outcomes,
        metadata: Some(serde_json::json!({
            "max_workers": config.max_workers,
            "mode_distribution": config.mode_distribution
                .iter()
                .map(|(m, c)| format!("{m}:{c}"))
                .collect::<Vec<_>>(),
        })),
    }
    .emit();

    let _ = meter_provider.shutdown();
}

fn scenario(
    name: &str,
    total: i64,
    failure_pct: i64,
    mode: &str,
    max_workers: u32,
) -> ScenarioConfig {
    let failure_count = total * failure_pct / 100;
    let success_count = total - failure_count;
    ScenarioConfig {
        name: name.to_string(),
        total_jobs: total,
        mode_distribution: vec![
            ("complete".to_string(), success_count),
            (mode.to_string(), failure_count),
        ],
        max_workers,
    }
}

fn mixed_scenario(name: &str, total: i64, max_workers: u32) -> ScenarioConfig {
    // Split evenly: 50% success, then equal parts of each failure mode
    let success_count = total / 2;
    let failure_count = total - success_count;
    let per_mode = failure_count / 5;
    let remainder = failure_count - per_mode * 5;
    ScenarioConfig {
        name: name.to_string(),
        total_jobs: total,
        mode_distribution: vec![
            ("complete".to_string(), success_count + remainder),
            ("terminal".to_string(), per_mode),
            ("retryable".to_string(), per_mode),
            ("callback_timeout".to_string(), per_mode),
            ("deadline_hang".to_string(), per_mode),
            ("snooze_once".to_string(), per_mode),
        ],
        max_workers,
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Individual benchmark tests
// ═══════════════════════════════════════════════════════════════════════

const TOTAL: i64 = 5_000;
const WORKERS: u32 = 64;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_failure_bench_terminal_1pct() {
    let pool = setup(20).await;
    run_scenario(
        &pool,
        &scenario("terminal_1pct", TOTAL, 1, "terminal", WORKERS),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_failure_bench_terminal_10pct() {
    let pool = setup(20).await;
    run_scenario(
        &pool,
        &scenario("terminal_10pct", TOTAL, 10, "terminal", WORKERS),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_failure_bench_terminal_50pct() {
    let pool = setup(20).await;
    run_scenario(
        &pool,
        &scenario("terminal_50pct", TOTAL, 50, "terminal", WORKERS),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_failure_bench_retryable_1pct() {
    let pool = setup(20).await;
    run_scenario(
        &pool,
        &scenario("retryable_1pct", TOTAL, 1, "retryable", WORKERS),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_failure_bench_retryable_10pct() {
    let pool = setup(20).await;
    run_scenario(
        &pool,
        &scenario("retryable_10pct", TOTAL, 10, "retryable", WORKERS),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_failure_bench_retryable_50pct() {
    let pool = setup(20).await;
    run_scenario(
        &pool,
        &scenario("retryable_50pct", TOTAL, 50, "retryable", WORKERS),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_failure_bench_callback_timeout_10pct() {
    let pool = setup(20).await;
    run_scenario(
        &pool,
        &scenario(
            "callback_timeout_10pct",
            TOTAL,
            10,
            "callback_timeout",
            WORKERS,
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_failure_bench_deadline_hang_10pct() {
    let pool = setup(20).await;
    run_scenario(
        &pool,
        &scenario("deadline_hang_10pct", TOTAL, 10, "deadline_hang", WORKERS),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_failure_bench_snooze_once_10pct() {
    let pool = setup(20).await;
    run_scenario(
        &pool,
        &scenario("snooze_once_10pct", TOTAL, 10, "snooze_once", WORKERS),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_failure_bench_mixed() {
    let pool = setup(20).await;
    run_scenario(&pool, &mixed_scenario("mixed_all_modes", TOTAL, WORKERS)).await;
}

/// Run the full failure benchmark matrix in one go.
/// Useful for local comparison runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_failure_bench_full_matrix() {
    let pool = setup(20).await;

    let scenarios = vec![
        scenario("terminal_1pct", TOTAL, 1, "terminal", WORKERS),
        scenario("terminal_10pct", TOTAL, 10, "terminal", WORKERS),
        scenario("terminal_50pct", TOTAL, 50, "terminal", WORKERS),
        scenario("retryable_1pct", TOTAL, 1, "retryable", WORKERS),
        scenario("retryable_10pct", TOTAL, 10, "retryable", WORKERS),
        scenario("retryable_50pct", TOTAL, 50, "retryable", WORKERS),
        scenario(
            "callback_timeout_10pct",
            TOTAL,
            10,
            "callback_timeout",
            WORKERS,
        ),
        scenario("deadline_hang_10pct", TOTAL, 10, "deadline_hang", WORKERS),
        scenario("snooze_once_10pct", TOTAL, 10, "snooze_once", WORKERS),
        mixed_scenario("mixed_all_modes", TOTAL, WORKERS),
    ];

    for s in &scenarios {
        run_scenario(&pool, s).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Stale-heartbeat rescue benchmark
//
// Seeds N jobs directly in "running" state with backdated heartbeat_at,
// then starts a client with aggressive heartbeat rescue intervals and
// measures how quickly all jobs are rescued and re-completed.
//
// This simulates the scenario where a worker node dies without draining.
// No subprocess management needed — we manipulate the DB directly.
// ═══════════════════════════════════════════════════════════════════════

/// No-op worker that completes immediately — used for rescued jobs on attempt 2+.
struct RescueBenchWorker {
    handler_count: Arc<AtomicU64>,
}

#[async_trait]
impl Worker for RescueBenchWorker {
    fn kind(&self) -> &'static str {
        "rescue_bench_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        self.handler_count.fetch_add(1, Ordering::Relaxed);
        Ok(JobResult::Completed)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_failure_bench_stale_heartbeat_rescue() {
    let pool = setup(20).await;
    let queue = "bench_stale_rescue";
    reset_runtime_state(&pool).await;

    let total_stale: i64 = 500;

    // Seed jobs directly in "running" state with a stale heartbeat.
    // run_lease is set to a value that won't match any real worker, ensuring
    // these look like orphaned jobs from a dead node.
    sqlx::query(
        r#"
        INSERT INTO awa.jobs_hot
            (kind, queue, args, state, priority, max_attempts, attempt,
             run_at, heartbeat_at, attempted_at, run_lease, metadata, tags)
        SELECT
            'rescue_bench_job',
            $1,
            jsonb_build_object('seq', g),
            'running'::awa.job_state,
            2,
            5,
            1,
            now() - interval '1 minute',
            now() - interval '10 minutes',
            now() - interval '1 minute',
            -1,
            '{}'::jsonb,
            '{}'::text[]
        FROM generate_series(1, $2) AS g
        "#,
    )
    .bind(queue)
    .bind(total_stale)
    .execute(&pool)
    .await
    .expect("Failed to seed stale running jobs");

    // Verify they're seeded correctly
    let running: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs_hot WHERE queue = $1 AND state = 'running'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(running, total_stale, "All seeded jobs should be running");

    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let handler_count = Arc::new(AtomicU64::new(0));
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 64,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        // Aggressive rescue intervals for benchmark
        .heartbeat_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .register_worker(RescueBenchWorker {
            handler_count: handler_count.clone(),
        })
        .build()
        .expect("Failed to build rescue benchmark client");

    let started = Instant::now();
    client.start().await.expect("Failed to start client");

    // Wait for all jobs to reach completed
    let timeout = Duration::from_secs(60);
    let deadline = Instant::now() + timeout;
    loop {
        let counts = queue_state_counts(&pool, queue).await;
        let completed = counts.get("completed").copied().unwrap_or(0);
        let still_running = counts.get("running").copied().unwrap_or(0);
        let retryable = counts.get("retryable").copied().unwrap_or(0);
        if completed == total_stale {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Timed out: completed={completed} running={still_running} retryable={retryable}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let rescue_time = started.elapsed();
    client.shutdown(Duration::from_secs(5)).await;

    meter_provider
        .force_flush()
        .expect("Failed to flush metrics");
    let resource_metrics = exporter
        .get_finished_metrics()
        .expect("Failed to get metrics");

    let rescues = sum_counter_metric(&resource_metrics, "awa.maintenance.rescues");
    let handler_total = handler_count.load(Ordering::Relaxed);

    println!(
        "[stale-rescue] stale_jobs={} rescue_time={:.2}s rescued={} handler_completed={} rate={:.0}/s",
        total_stale,
        rescue_time.as_secs_f64(),
        rescues,
        handler_total,
        total_stale as f64 / rescue_time.as_secs_f64()
    );

    let mut outcomes: HashMap<String, u64> = HashMap::new();
    outcomes.insert("completed".to_string(), total_stale as u64);

    BenchmarkResult {
        schema_version: SCHEMA_VERSION,
        scenario: "stale_heartbeat_rescue".to_string(),
        language: "rust".to_string(),
        seeded: total_stale as u64,
        metrics: BenchMetrics {
            throughput: Some(BenchThroughput {
                handler_per_s: handler_total as f64 / rescue_time.as_secs_f64(),
                db_finalized_per_s: total_stale as f64 / rescue_time.as_secs_f64(),
            }),
            enqueue_per_s: None,
            drain_time_s: Some(rescue_time.as_secs_f64()),
            latency_ms: None,
            rescue: Some(BenchRescue {
                heartbeat_rescued: Some(rescues),
                deadline_rescued: None,
                callback_timeouts: None,
            }),
        },
        outcomes,
        metadata: Some(serde_json::json!({
            "stale_age_minutes": 10,
            "rescue_interval_ms": 100,
        })),
    }
    .emit();

    let _ = meter_provider.shutdown();
}
