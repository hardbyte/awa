//! Local benchmark tests for throughput and scheduling behavior.
//!
//! Run with:
//! `DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --package awa --test scheduling_benchmark_test -- --ignored --nocapture`

use awa::model::{insert_many, migrations};
use awa::{
    Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, JobRow, PeriodicJob, QueueConfig,
    Worker,
};
use chrono::{DateTime, Utc};
use opentelemetry_sdk::metrics::data::{Histogram, Sum};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::time::{Duration, Instant};

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
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

async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs_hot WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean hot queue jobs");
    sqlx::query("DELETE FROM awa.scheduled_jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean scheduled queue jobs");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue meta");
}

async fn reset_runtime_state(pool: &sqlx::PgPool) {
    sqlx::query(
        "TRUNCATE awa.jobs_hot, awa.scheduled_jobs, awa.queue_meta, awa.job_unique_claims RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("Failed to reset runtime benchmark state");
}

async fn clean_cron_names(pool: &sqlx::PgPool, names: &[String]) {
    for name in names {
        sqlx::query("DELETE FROM awa.cron_jobs WHERE name = $1")
            .bind(name)
            .execute(pool)
            .await
            .expect("Failed to clean cron job");
    }
}

async fn wait_for_leader(client: &Client, timeout: Duration) {
    let start = Instant::now();
    loop {
        let health = client.health_check().await;
        if health.leader && health.poll_loop_alive && health.heartbeat_alive {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "Timed out waiting for leader/poll loop readiness"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_dispatch(client: &Client, timeout: Duration) {
    let start = Instant::now();
    loop {
        let health = client.health_check().await;
        if health.poll_loop_alive && health.heartbeat_alive {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "Timed out waiting for dispatcher/heartbeat readiness"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn install_in_memory_metrics() -> (InMemoryMetricExporter, SdkMeterProvider) {
    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());
    (exporter, meter_provider)
}

fn find_metric<'a>(
    resource_metrics: &'a [opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
) -> Vec<&'a opentelemetry_sdk::metrics::data::Metric> {
    let mut out = Vec::new();
    for rm in resource_metrics {
        for scope_metrics in &rm.scope_metrics {
            for metric in &scope_metrics.metrics {
                if metric.name == name {
                    out.push(metric);
                }
            }
        }
    }
    out
}

fn print_counter_metric(
    resource_metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
) {
    let total: u64 = find_metric(resource_metrics, name)
        .into_iter()
        .filter_map(|metric| metric.data.as_any().downcast_ref::<Sum<u64>>())
        .flat_map(|sum| sum.data_points.iter())
        .map(|dp| dp.value)
        .sum();
    println!("[metrics] {name}: total={total}");
}

fn print_histogram_metric_u64(
    resource_metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
) {
    let mut count = 0u64;
    let mut sum = 0u64;
    let mut max = 0u64;
    for metric in find_metric(resource_metrics, name) {
        if let Some(hist) = metric.data.as_any().downcast_ref::<Histogram<u64>>() {
            for dp in &hist.data_points {
                count += dp.count;
                sum += dp.sum;
                if let Some(dp_max) = dp.max {
                    max = max.max(dp_max);
                }
            }
        }
    }
    let mean = if count == 0 {
        0.0
    } else {
        sum as f64 / count as f64
    };
    println!("[metrics] {name}: count={count} mean={mean:.1} max={max}");
}

fn print_histogram_metric_f64(
    resource_metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
) {
    let mut count = 0u64;
    let mut sum = 0.0f64;
    let mut max = 0.0f64;
    for metric in find_metric(resource_metrics, name) {
        if let Some(hist) = metric.data.as_any().downcast_ref::<Histogram<f64>>() {
            for dp in &hist.data_points {
                count += dp.count;
                sum += dp.sum;
                if let Some(dp_max) = dp.max {
                    max = max.max(dp_max);
                }
            }
        }
    }
    let mean = if count == 0 { 0.0 } else { sum / count as f64 };
    println!(
        "[metrics] {name}: count={count} mean_ms={:.3} max_ms={:.3}",
        mean * 1000.0,
        max * 1000.0
    );
}

fn print_runtime_metrics(resource_metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics]) {
    print_counter_metric(resource_metrics, "awa.jobs.claimed");
    print_counter_metric(resource_metrics, "awa.dispatch.claim_batches");
    print_histogram_metric_u64(resource_metrics, "awa.dispatch.claim_batch_size");
    print_histogram_metric_f64(resource_metrics, "awa.dispatch.claim_duration");
    print_counter_metric(resource_metrics, "awa.completion.flushes");
    print_histogram_metric_u64(resource_metrics, "awa.completion.flush_batch_size");
    print_histogram_metric_f64(resource_metrics, "awa.completion.flush_duration");
    print_counter_metric(resource_metrics, "awa.maintenance.promote_batches");
    print_histogram_metric_u64(resource_metrics, "awa.maintenance.promote_batch_size");
    print_histogram_metric_f64(resource_metrics, "awa.maintenance.promote_duration");
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    assert!(!sorted.is_empty());
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn print_latency_summary(label: &str, mut values: Vec<Duration>) {
    values.sort();
    let min = values[0];
    let p50 = percentile(&values, 0.50);
    let p95 = percentile(&values, 0.95);
    let p99 = percentile(&values, 0.99);
    let max = values[values.len() - 1];

    println!("[sched] {label}:");
    println!("[sched]   count: {}", values.len());
    println!("[sched]   min:   {:.1} ms", min.as_secs_f64() * 1000.0);
    println!("[sched]   p50:   {:.1} ms", p50.as_secs_f64() * 1000.0);
    println!("[sched]   p95:   {:.1} ms", p95.as_secs_f64() * 1000.0);
    println!("[sched]   p99:   {:.1} ms", p99.as_secs_f64() * 1000.0);
    println!("[sched]   max:   {:.1} ms", max.as_secs_f64() * 1000.0);
}

fn parse_fire_time(raw: &str) -> DateTime<Utc> {
    chrono::DateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f%#z")
        .unwrap_or_else(|_| panic!("Failed to parse cron_fire_time: {raw}"))
        .with_timezone(&Utc)
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct TimingJob {
    seq: i64,
}

#[derive(Debug, Clone)]
struct PickupEvent {
    id: i64,
    picked_at: DateTime<Utc>,
    _seq: i64,
    steady_slot: Option<i64>,
    cron_name: Option<String>,
    cron_fire_time: Option<String>,
}

struct TimingWorker {
    tx: tokio::sync::mpsc::UnboundedSender<PickupEvent>,
}

#[async_trait::async_trait]
impl Worker for TimingWorker {
    fn kind(&self) -> &'static str {
        "timing_job"
    }

    async fn perform(&self, job: &JobRow, _ctx: &JobContext) -> Result<JobResult, JobError> {
        let seq = job
            .args
            .get("seq")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let steady_slot = job.metadata.get("steady_slot").and_then(|v| v.as_i64());
        let cron_name = job
            .metadata
            .get("cron_name")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned);
        let cron_fire_time = job
            .metadata
            .get("cron_fire_time")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned);

        let _ = self.tx.send(PickupEvent {
            id: job.id,
            picked_at: Utc::now(),
            _seq: seq,
            steady_slot,
            cron_name,
            cron_fire_time,
        });
        Ok(JobResult::Completed)
    }
}

async fn recv_n(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<PickupEvent>,
    n: usize,
    timeout: Duration,
) -> Vec<PickupEvent> {
    let deadline = Instant::now() + timeout;
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let now = Instant::now();
        assert!(now < deadline, "Timed out waiting for {n} pickup events");
        let remaining = deadline.saturating_duration_since(now);
        let item = tokio::time::timeout(remaining, rx.recv())
            .await
            .expect("Timed out waiting for pickup event")
            .expect("Pickup channel closed unexpectedly");
        out.push(item);
    }
    out
}

async fn recv_until(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<PickupEvent>,
    duration: Duration,
) -> Vec<PickupEvent> {
    let deadline = Instant::now() + duration;
    let mut out = Vec::new();
    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(now);
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(item)) => out.push(item),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_runtime_throughput_worker_sweep() {
    let pool = setup(20).await;
    let queue = "bench_worker_sweep";
    clean_queue(&pool, queue).await;

    let configs = [10u32, 25, 50, 100, 200];
    let total_jobs: i64 = 5_000;
    let batch_size = 500usize;

    for max_workers in configs {
        clean_queue(&pool, queue).await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let client = Client::builder(pool.clone())
            .queue(
                queue,
                QueueConfig {
                    max_workers,
                    poll_interval: Duration::from_millis(50),
                    ..QueueConfig::default()
                },
            )
            .register_worker(TimingWorker { tx })
            .build()
            .expect("Failed to build client");

        client.start().await.expect("Failed to start client");
        wait_for_dispatch(&client, Duration::from_secs(5)).await;

        let insert_start = Instant::now();
        for batch_start in (0..total_jobs as usize).step_by(batch_size) {
            let batch_end = (batch_start + batch_size).min(total_jobs as usize);
            let params: Vec<_> = (batch_start..batch_end)
                .map(|i| {
                    awa::model::insert::params_with(
                        &TimingJob { seq: i as i64 },
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

        let processing_start = Instant::now();
        let _events = recv_n(&mut rx, total_jobs as usize, Duration::from_secs(30)).await;
        let processing_elapsed = processing_start.elapsed();
        let total_elapsed = insert_start.elapsed();

        println!(
            "[throughput] max_workers={max_workers:>3} insert_rate={:.0}/s pickup_rate={:.0}/s end_to_end={:.0}/s",
            total_jobs as f64 / insert_elapsed.as_secs_f64(),
            total_jobs as f64 / processing_elapsed.as_secs_f64(),
            total_jobs as f64 / total_elapsed.as_secs_f64(),
        );

        client.shutdown(Duration::from_secs(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_runtime_completion_gap() {
    let pool = setup(20).await;
    let queue = "bench_completion_gap";
    clean_queue(&pool, queue).await;

    let total_jobs: i64 = 5_000;
    let batch_size = 500usize;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 200,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .register_worker(TimingWorker { tx })
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");
    wait_for_dispatch(&client, Duration::from_secs(5)).await;

    let insert_start = Instant::now();
    for batch_start in (0..total_jobs as usize).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(total_jobs as usize);
        let params: Vec<_> = (batch_start..batch_end)
            .map(|i| {
                awa::model::insert::params_with(
                    &TimingJob { seq: i as i64 },
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

    let handler_events = recv_n(&mut rx, total_jobs as usize, Duration::from_secs(30)).await;
    let handler_elapsed = insert_start.elapsed();

    let completion_start = Instant::now();
    loop {
        let completed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'completed'",
        )
        .bind(queue)
        .fetch_one(&pool)
        .await
        .unwrap();

        if completed == total_jobs {
            break;
        }
        assert!(
            completion_start.elapsed() < Duration::from_secs(30),
            "Timed out waiting for completed rows after handlers returned"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let total_elapsed = insert_start.elapsed();
    let completion_lag = total_elapsed.saturating_sub(handler_elapsed);

    println!(
        "[completion-gap] handler_returned={} in {:.3}s ({:.0}/s)",
        handler_events.len(),
        handler_elapsed.as_secs_f64(),
        handler_events.len() as f64 / handler_elapsed.as_secs_f64()
    );
    println!(
        "[completion-gap] db_completed={} in {:.3}s ({:.0}/s)",
        total_jobs,
        total_elapsed.as_secs_f64(),
        total_jobs as f64 / total_elapsed.as_secs_f64()
    );
    println!(
        "[completion-gap] post-return completion lag: {:.1} ms total ({:.1} us/job)",
        completion_lag.as_secs_f64() * 1000.0,
        completion_lag.as_secs_f64() * 1_000_000.0 / total_jobs as f64
    );

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_runtime_sustained_hot_path() {
    run_runtime_sustained_hot_path(20).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_runtime_sustained_hot_path_pool_100() {
    run_runtime_sustained_hot_path(100).await;
}

async fn run_runtime_sustained_hot_path(max_conns: u32) {
    let pool = setup(max_conns).await;
    let queue = if max_conns == 20 {
        "bench_hot_steady"
    } else {
        "bench_hot_steady_pool100"
    };
    // This benchmark is meant to measure the hot execution path only. Reset the
    // whole runtime state so global deferred backlogs from earlier tests do not
    // distort maintenance activity in the background.
    reset_runtime_state(&pool).await;
    clean_queue(&pool, queue).await;
    let (exporter, meter_provider) = install_in_memory_metrics();

    let total_jobs: i64 = 200_000;
    let warmup = Duration::from_secs(2);
    let window = Duration::from_secs(10);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    sqlx::query(
        r#"
        INSERT INTO awa.jobs_hot (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
        SELECT
            'timing_job',
            $1,
            jsonb_build_object('seq', g),
            'available'::awa.job_state,
            2,
            25,
            now(),
            '{}'::jsonb,
            '{}'::text[]
        FROM generate_series(1, $2) AS g
        "#,
    )
    .bind(queue)
    .bind(total_jobs)
    .execute(&pool)
    .await
    .expect("Failed to seed hot jobs");

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 256,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .register_worker(TimingWorker { tx })
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");
    wait_for_dispatch(&client, Duration::from_secs(5)).await;

    tokio::time::sleep(warmup).await;

    while rx.try_recv().is_ok() {}

    let completed_before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs_hot WHERE queue = $1 AND state = 'completed'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();

    let events = recv_until(&mut rx, window).await;

    let completed_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs_hot WHERE queue = $1 AND state = 'completed'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();
    let remaining_hot: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs_hot WHERE queue = $1 AND state IN ('available', 'running')",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();

    println!(
        "[steady-hot] warmup={:.1}s window={:.1}s handler_returned={} ({:.0}/s) db_completed_delta={} ({:.0}/s) remaining_hot={}",
        warmup.as_secs_f64(),
        window.as_secs_f64(),
        events.len(),
        events.len() as f64 / window.as_secs_f64(),
        completed_after - completed_before,
        (completed_after - completed_before) as f64 / window.as_secs_f64(),
        remaining_hot
    );
    println!("[steady-hot] pool_max_connections={max_conns}");

    client.shutdown(Duration::from_secs(5)).await;
    meter_provider
        .force_flush()
        .expect("Failed to flush metrics");
    let resource_metrics = exporter
        .get_finished_metrics()
        .expect("Failed to get finished metrics");
    print_runtime_metrics(&resource_metrics);
    let _ = meter_provider.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_scheduled_delay_jitter_sweep() {
    let pool = setup(20).await;
    let queue = "bench_scheduled_jitter";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 20,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .leader_election_interval(Duration::from_millis(200))
        .register_worker(TimingWorker { tx })
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");
    wait_for_leader(&client, Duration::from_secs(5)).await;

    let delays_ms = [100i64, 500, 1_000, 2_000];
    let samples_per_delay = 10usize;
    let mut run_at_by_id = HashMap::new();
    let mut delay_by_id = HashMap::new();

    for i in 0..(delays_ms.len() * samples_per_delay) {
        let delay_ms = delays_ms[i % delays_ms.len()];
        let run_at = Utc::now() + chrono::Duration::milliseconds(delay_ms);
        let job = awa::model::insert_with(
            &pool,
            &TimingJob { seq: i as i64 },
            InsertOpts {
                queue: queue.into(),
                run_at: Some(run_at),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        run_at_by_id.insert(job.id, run_at);
        delay_by_id.insert(job.id, delay_ms);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let events = recv_n(
        &mut rx,
        delays_ms.len() * samples_per_delay,
        Duration::from_secs(45),
    )
    .await;

    let mut late_by_delay: HashMap<i64, Vec<Duration>> = HashMap::new();
    for event in events {
        let scheduled_for = run_at_by_id[&event.id];
        let lateness = (event.picked_at - scheduled_for)
            .to_std()
            .unwrap_or_else(|_| Duration::from_secs(0));
        let original_delay = delay_by_id[&event.id];
        late_by_delay
            .entry(original_delay)
            .or_default()
            .push(lateness);
    }

    for delay_ms in delays_ms {
        let values = late_by_delay
            .remove(&delay_ms)
            .unwrap_or_else(|| panic!("Missing results for delay {delay_ms}ms"));
        print_latency_summary(&format!("scheduled run_at+{delay_ms}ms"), values);
    }

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_cron_seconds_timing_and_miss_rate() {
    let pool = setup(20).await;
    let queue = "bench_cron_seconds";
    clean_queue(&pool, queue).await;

    let cron_names: Vec<String> = (0..10).map(|i| format!("bench_cron_seconds_{i}")).collect();
    clean_cron_names(&pool, &cron_names).await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut builder = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 50,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .leader_election_interval(Duration::from_millis(200))
        .register_worker(TimingWorker { tx });

    let periodic_template = match PeriodicJob::builder("bench_cron_probe", "*/1 * * * * *")
        .queue(queue)
        .build_raw("timing_job".to_string(), serde_json::json!({}))
    {
        Ok(job) => job,
        Err(err) => {
            println!(
                "[cron] sub-minute periodic scheduling is not supported by the current API: {}",
                err
            );
            return;
        }
    };

    for name in &cron_names {
        let mut job = periodic_template.clone();
        job.name = name.clone();
        builder = builder.periodic(job);
    }

    let client = builder.build().expect("Failed to build client");
    client.start().await.expect("Failed to start client");
    wait_for_leader(&client, Duration::from_secs(5)).await;

    let window = Duration::from_secs(8);
    tokio::time::sleep(window).await;

    let expected_min = cron_names.len() * 6;
    let available = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND metadata ? 'cron_name'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap() as usize;

    let events = recv_n(&mut rx, available, Duration::from_secs(15)).await;
    let received = events.len();

    let mut lateness = Vec::new();
    let mut per_name = HashMap::<String, usize>::new();
    for event in events {
        let name = event.cron_name.expect("cron event missing cron_name");
        *per_name.entry(name).or_default() += 1;

        let fire_time = parse_fire_time(
            event
                .cron_fire_time
                .as_deref()
                .expect("cron event missing cron_fire_time"),
        );
        lateness.push(
            (event.picked_at - fire_time)
                .to_std()
                .unwrap_or_else(|_| Duration::from_secs(0)),
        );
    }

    println!(
        "[cron] schedules={} window={:.1}s jobs_seen={} jobs_picked={} expected_min={}",
        cron_names.len(),
        window.as_secs_f64(),
        available,
        received,
        expected_min
    );
    for name in &cron_names {
        println!(
            "[cron]   {name}: {} fires",
            per_name.get(name).copied().unwrap_or(0)
        );
    }
    print_latency_summary("cron */1 * * * * * pickup lateness", lateness);

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_scheduled_frontier_10m_due_1k() {
    run_scheduled_frontier_benchmark("bench_frontier_10m", 10_000_000, 1_000).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_scheduled_frontier_2m_due_1k_completion_gap() {
    run_scheduled_frontier_benchmark("bench_frontier_2m", 2_000_000, 1_000).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_scheduled_steady_10m_due_1k_per_sec() {
    run_scheduled_steady_benchmark("bench_steady_10m", 10_000_000, 1_000, 10).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore]
async fn test_scheduled_steady_10m_due_1k_per_sec_rt16() {
    run_scheduled_steady_benchmark("bench_steady_10m_rt16", 10_000_000, 1_000, 10).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_scheduled_steady_2m_due_4k_per_sec() {
    run_scheduled_steady_benchmark("bench_steady_2m_4k", 2_000_000, 4_000, 10).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_scheduled_steady_10m_due_6k_per_sec() {
    run_scheduled_steady_benchmark("bench_steady_10m_6k", 10_000_000, 6_000, 10).await;
}

async fn run_scheduled_frontier_benchmark(queue: &str, total_jobs: i64, due_now: i64) {
    let pool = setup(20).await;
    clean_queue(&pool, queue).await;

    let seed_start = Instant::now();
    sqlx::query(
        r#"
        INSERT INTO awa.scheduled_jobs (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
        SELECT
            'timing_job',
            $1,
            jsonb_build_object('seq', g),
            'scheduled'::awa.job_state,
            2,
            25,
            CASE
                WHEN g <= $2 THEN now() - interval '1 second'
                ELSE now() + make_interval(secs => g)
            END,
            '{}'::jsonb,
            '{}'::text[]
        FROM generate_series(1, $3) AS g
        "#,
    )
    .bind(queue)
    .bind(due_now)
    .bind(total_jobs)
    .execute(&pool)
    .await
    .expect("Failed to seed 10M scheduled rows");
    let seed_elapsed = seed_start.elapsed();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 256,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .leader_election_interval(Duration::from_millis(200))
        .promote_interval(Duration::from_millis(250))
        .register_worker(TimingWorker { tx })
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");
    wait_for_leader(&client, Duration::from_secs(5)).await;

    let processing_start = Instant::now();
    let processing_started_at = Utc::now();
    let events = recv_n(&mut rx, due_now as usize, Duration::from_secs(30)).await;
    let handler_elapsed = processing_start.elapsed();
    let first_pickup = events
        .iter()
        .map(|e| e.picked_at)
        .min()
        .expect("Expected at least one event");
    let last_pickup = events
        .iter()
        .map(|e| e.picked_at)
        .max()
        .expect("Expected at least one event");
    let first_latency = (first_pickup - processing_started_at)
        .to_std()
        .unwrap_or_else(|_| Duration::from_secs(0));
    let last_latency = (last_pickup - processing_started_at)
        .to_std()
        .unwrap_or_else(|_| Duration::from_secs(0));

    let completion_start = Instant::now();
    let completed: i64 = loop {
        let completed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.jobs_hot WHERE queue = $1 AND state = 'completed'",
        )
        .bind(queue)
        .fetch_one(&pool)
        .await
        .unwrap();
        if completed >= due_now {
            break completed;
        }
        assert!(
            completion_start.elapsed() < Duration::from_secs(30),
            "Timed out waiting for completed rows after due jobs were picked up"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let total_elapsed = processing_start.elapsed();
    let completion_lag = total_elapsed.saturating_sub(handler_elapsed);
    let scheduled_remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.scheduled_jobs WHERE queue = $1 AND state = 'scheduled'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();

    println!(
        "[frontier] seeded={} due_now={} seed_time={:.2}s",
        total_jobs,
        due_now,
        seed_elapsed.as_secs_f64()
    );
    println!(
        "[frontier] handler_returned={} in {:.3}s ({:.0}/s)",
        events.len(),
        handler_elapsed.as_secs_f64(),
        events.len() as f64 / handler_elapsed.as_secs_f64()
    );
    println!(
        "[frontier] db_completed={} in {:.3}s ({:.0}/s)",
        completed,
        total_elapsed.as_secs_f64(),
        completed as f64 / total_elapsed.as_secs_f64()
    );
    println!(
        "[frontier] first_pickup={:.1}ms last_pickup={:.1}ms completion_lag={:.1}ms scheduled_remaining={}",
        first_latency.as_secs_f64() * 1000.0,
        last_latency.as_secs_f64() * 1000.0,
        completion_lag.as_secs_f64() * 1000.0,
        scheduled_remaining
    );

    client.shutdown(Duration::from_secs(5)).await;
}

async fn run_scheduled_steady_benchmark(
    queue: &str,
    total_jobs: i64,
    due_rate: i64,
    window_secs: i64,
) {
    let pool = setup(20).await;
    // This benchmark measures one deferred frontier at scale. Reset the whole
    // runtime state so global promotion work from earlier tests does not skew
    // the result.
    reset_runtime_state(&pool).await;
    clean_queue(&pool, queue).await;
    let (exporter, meter_provider) = install_in_memory_metrics();

    let seed_start = Instant::now();
    sqlx::query(
        r#"
        INSERT INTO awa.scheduled_jobs (kind, queue, args, state, priority, max_attempts, run_at, metadata, tags)
        SELECT
            'timing_job',
            $1,
            jsonb_build_object('seq', g),
            'scheduled'::awa.job_state,
            2,
            25,
            now() + interval '365 days' + make_interval(secs => g),
            '{}'::jsonb,
            '{}'::text[]
        FROM generate_series(1, $2) AS g
        "#,
    )
    .bind(queue)
    .bind(total_jobs)
    .execute(&pool)
    .await
    .expect("Failed to seed scheduled backlog");
    let seed_elapsed = seed_start.elapsed();

    let due_rows = due_rate * window_secs;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 256,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .leader_election_interval(Duration::from_millis(200))
        .promote_interval(Duration::from_millis(250))
        .register_worker(TimingWorker { tx })
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");
    wait_for_leader(&client, Duration::from_secs(5)).await;

    sqlx::query(
        r#"
        WITH target AS (
            SELECT id, row_number() OVER (ORDER BY id) AS rn
            FROM (
                SELECT id
                FROM awa.scheduled_jobs
                WHERE queue = $1
                  AND state = 'scheduled'
                ORDER BY id ASC
                LIMIT $2
            ) picked
        )
        UPDATE awa.scheduled_jobs AS jobs
        SET run_at = now() + make_interval(secs => (((target.rn - 1) / $3) + 1)),
            metadata = jsonb_set(
                COALESCE(jobs.metadata, '{}'::jsonb),
                '{steady_slot}',
                to_jsonb((((target.rn - 1) / $3) + 1)),
                true
            )
        FROM target
        WHERE jobs.id = target.id
        "#,
    )
    .bind(queue)
    .bind(due_rows)
    .bind(due_rate)
    .execute(&pool)
    .await
    .expect("Failed to prime steady due schedule");

    let schedule_started_at = Utc::now();
    let mut completed_prev = 0i64;
    let mut completed_by_second = Vec::new();
    for second in 1..=window_secs {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let completed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.jobs_hot WHERE queue = $1 AND state = 'completed'",
        )
        .bind(queue)
        .fetch_one(&pool)
        .await
        .unwrap();
        completed_by_second.push((second, completed - completed_prev));
        completed_prev = completed;
    }

    let events = recv_until(&mut rx, Duration::from_secs(window_secs as u64 + 5)).await;

    let scheduled_remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.scheduled_jobs WHERE queue = $1 AND state = 'scheduled'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();

    let mut lateness = Vec::new();
    for event in &events {
        if let Some(slot) = event.steady_slot {
            let scheduled_for = schedule_started_at + chrono::Duration::seconds(slot);
            lateness.push(
                (event.picked_at - scheduled_for)
                    .to_std()
                    .unwrap_or_else(|_| Duration::from_secs(0)),
            );
        }
    }

    println!(
        "[steady-scheduled] seeded={} due_rate={}/s window={}s seed_time={:.2}s picked_total={} completed_total={} scheduled_remaining={}",
        total_jobs,
        due_rate,
        window_secs,
        seed_elapsed.as_secs_f64(),
        events.len(),
        completed_prev,
        scheduled_remaining
    );
    for (second, delta) in &completed_by_second {
        println!(
            "[steady-scheduled] second={:>2} completed_delta={} ({:.0}% of target)",
            second,
            delta,
            (*delta as f64 / due_rate as f64) * 100.0
        );
    }
    if !lateness.is_empty() {
        print_latency_summary("steady scheduled pickup lateness", lateness);
    }

    client.shutdown(Duration::from_secs(5)).await;
    meter_provider
        .force_flush()
        .expect("Failed to flush metrics");
    let resource_metrics = exporter
        .get_finished_metrics()
        .expect("Failed to get finished metrics");
    print_runtime_metrics(&resource_metrics);
    let _ = meter_provider.shutdown();
}
