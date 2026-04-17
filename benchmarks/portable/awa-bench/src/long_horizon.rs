//! Long-horizon scenario: fixed-rate producer + steady consumer, with rolling
//! JSONL telemetry samples emitted every `SAMPLE_EVERY_S`.
//!
//! Contract: benchmarks/portable/CONTRIBUTING_ADAPTERS.md
//!
//! Startup: emits one descriptor record, then runs indefinitely. SIGTERM
//! flushes pending samples and exits 0 within 5s.

use async_trait::async_trait;
use awa_macros::JobArgs;
use awa_model::{insert_with, migrations, InsertOpts};
use awa_worker::{Client, JobContext, JobError, JobResult, QueueConfig, Worker};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::{interval_at, MissedTickBehavior};

#[derive(Debug, Serialize, Deserialize, JobArgs)]
pub struct LongHorizonJob {
    pub seq: i64,
    /// Arbitrary filler so jobs approximate the declared payload size.
    pub padding: String,
}

/// Shared, bounded-memory rolling window of sample events for percentiles.
struct LatencyWindow {
    /// (captured_at, latency_ms) ring buffer — enforces the 30s window when
    /// samples are consumed.
    events: VecDeque<(Instant, f64)>,
    /// Pre-allocated histogram we fold the ring into at sample time.
    hist: Histogram<u64>,
}

impl LatencyWindow {
    fn new() -> Self {
        // 3 sig figs, up to ~60s, per HDR best practice.
        Self {
            events: VecDeque::with_capacity(4096),
            hist: Histogram::<u64>::new_with_bounds(1, 60_000 * 1_000, 3).unwrap(),
        }
    }

    fn record(&mut self, latency_ms: f64) {
        self.events.push_back((Instant::now(), latency_ms));
    }

    /// Snapshot p50/p95/p99 across the trailing `window`.
    fn snapshot(&mut self, window: Duration) -> Option<(f64, f64, f64, usize)> {
        let cutoff = Instant::now() - window;
        while let Some(&(t, _)) = self.events.front() {
            if t < cutoff {
                self.events.pop_front();
            } else {
                break;
            }
        }
        if self.events.is_empty() {
            return None;
        }
        self.hist.reset();
        for &(_, v) in &self.events {
            // Microseconds of precision.
            let _ = self.hist.record((v * 1_000.0).round().max(1.0) as u64);
        }
        let p50 = self.hist.value_at_quantile(0.50) as f64 / 1_000.0;
        let p95 = self.hist.value_at_quantile(0.95) as f64 / 1_000.0;
        let p99 = self.hist.value_at_quantile(0.99) as f64 / 1_000.0;
        Some((p50, p95, p99, self.events.len()))
    }
}

struct LongHorizonWorker {
    work_ms: u64,
    latencies: Arc<Mutex<LatencyWindow>>,
    completed_counter: Arc<AtomicU64>,
}

#[async_trait]
impl Worker for LongHorizonWorker {
    fn kind(&self) -> &'static str {
        "long_horizon_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        // Pickup latency: harness cares about "claim" latency — the time from
        // insert to when the worker began executing this job. Use the job's
        // created_at as the zero point.
        let now = chrono::Utc::now();
        let claim_latency_ms = (now - ctx.job.created_at).num_milliseconds().max(0) as f64;
        self.latencies.lock().await.record(claim_latency_ms);
        if self.work_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.work_ms)).await;
        }
        self.completed_counter.fetch_add(1, Ordering::Relaxed);
        Ok(JobResult::Completed)
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn now_iso_ms() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs() as i64;
    let millis = now.subsec_millis();
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, millis * 1_000_000)
        .unwrap_or_else(chrono::Utc::now);
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn emit(record: serde_json::Value) {
    // Single println per sample: tailer is line-buffered.
    println!("{}", record);
}

fn emit_descriptor(db_name: &str) {
    emit(json!({
        "kind": "descriptor",
        "system": "awa",
        "event_tables": [
            "awa.jobs_hot",
            "awa.scheduled_jobs",
            "awa.jobs_dlq",
            "awa.job_unique_claims"
        ],
        "extensions": [],
        "version": env!("CARGO_PKG_VERSION"),
        "schema_version": env_string("AWA_SCHEMA_VERSION", "current"),
        "db_name": db_name,
        "started_at": now_iso_ms(),
    }));
}

pub async fn run() {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let producer_rate = env_u64("PRODUCER_RATE", 800);
    let worker_count = env_u32("WORKER_COUNT", 32);
    let payload_bytes = env_u64("JOB_PAYLOAD_BYTES", 256);
    let work_ms = env_u64("JOB_WORK_MS", 1);
    let sample_every_s = env_u64("SAMPLE_EVERY_S", 10);
    let max_connections = env_u32("MAX_CONNECTIONS", 40);
    let db_name = database_url
        .rsplit('/')
        .next()
        .unwrap_or("awa_bench")
        .to_string();

    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");
    migrations::run(&pool)
        .await
        .expect("Failed to run migrations");
    emit_descriptor(&db_name);

    let queue_name = "awa_longhorizon_bench";
    // No clean here — the bench harness starts from a fresh PG, so existing
    // rows are not an issue. For --fast we do see stale rows across runs;
    // that's acceptable for dev iteration.

    let latencies = Arc::new(Mutex::new(LatencyWindow::new()));
    let completed = Arc::new(AtomicU64::new(0));
    let enqueued = Arc::new(AtomicU64::new(0));
    let queue_depth = Arc::new(AtomicU64::new(0));

    let worker = LongHorizonWorker {
        work_ms,
        latencies: Arc::clone(&latencies),
        completed_counter: Arc::clone(&completed),
    };

    let client = Client::builder(pool.clone())
        .queue(
            queue_name,
            QueueConfig {
                max_workers: worker_count,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .register_worker(worker)
        .build()
        .expect("Failed to build client");
    client.start().await.expect("Failed to start client");

    let shutdown = Arc::new(AtomicBool::new(false));

    // ── Producer ────────────────────────────────────────────────────
    let producer_pool = pool.clone();
    let producer_shutdown = Arc::clone(&shutdown);
    let producer_enqueued = Arc::clone(&enqueued);
    let padding = "x".repeat(payload_bytes.saturating_sub(32) as usize);
    let producer_handle = tokio::spawn(async move {
        // Convert rate to a uniform tick. Bursty patterns skew tail latency
        // measurement; prefer a smooth drip.
        let period_ns = if producer_rate > 0 {
            1_000_000_000 / producer_rate
        } else {
            return;
        };
        let mut ticker = interval_at(tokio::time::Instant::now(), Duration::from_nanos(period_ns));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut seq: i64 = 0;
        while !producer_shutdown.load(Ordering::Relaxed) {
            ticker.tick().await;
            let args = LongHorizonJob {
                seq,
                padding: padding.clone(),
            };
            let res = insert_with(
                &producer_pool,
                &args,
                InsertOpts {
                    queue: queue_name.into(),
                    ..Default::default()
                },
            )
            .await;
            match res {
                Ok(_) => {
                    producer_enqueued.fetch_add(1, Ordering::Relaxed);
                    seq += 1;
                }
                Err(err) => {
                    eprintln!("[awa] producer insert failed: {err}");
                }
            }
        }
    });

    // ── Queue depth poller ──────────────────────────────────────────
    let depth_pool = pool.clone();
    let depth_shutdown = Arc::clone(&shutdown);
    let depth_handle = {
        let queue_depth = Arc::clone(&queue_depth);
        tokio::spawn(async move {
            while !depth_shutdown.load(Ordering::Relaxed) {
                let row: Result<(i64,), _> = sqlx::query_as(
                    "SELECT count(*) FROM awa.jobs_hot \
                     WHERE queue = $1 AND state = 'available'",
                )
                .bind(queue_name)
                .fetch_one(&depth_pool)
                .await;
                if let Ok((n,)) = row {
                    queue_depth.store(n as u64, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    };

    // ── Sample emitter ──────────────────────────────────────────────
    let sample_shutdown = Arc::clone(&shutdown);
    let sample_enqueued = Arc::clone(&enqueued);
    let sample_completed = Arc::clone(&completed);
    let sample_depth = Arc::clone(&queue_depth);
    let sample_latencies = Arc::clone(&latencies);
    let sample_handle = tokio::spawn(async move {
        // Align first tick to the next wall-clock `sample_every_s` boundary.
        let now_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let next_boundary_s = ((now_epoch / sample_every_s) + 1) * sample_every_s;
        let sleep_for = next_boundary_s.saturating_sub(now_epoch);
        tokio::time::sleep(Duration::from_secs(sleep_for)).await;

        let mut last_enqueued: u64 = 0;
        let mut last_completed: u64 = 0;
        let mut last_tick = Instant::now();
        let mut ticker = interval_at(
            tokio::time::Instant::now(),
            Duration::from_secs(sample_every_s),
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        while !sample_shutdown.load(Ordering::Relaxed) {
            ticker.tick().await;
            let now = Instant::now();
            let dt = now.duration_since(last_tick).as_secs_f64().max(0.001);
            last_tick = now;

            let enq = sample_enqueued.load(Ordering::Relaxed);
            let cmp = sample_completed.load(Ordering::Relaxed);
            let enq_rate = (enq - last_enqueued) as f64 / dt;
            let cmp_rate = (cmp - last_completed) as f64 / dt;
            last_enqueued = enq;
            last_completed = cmp;

            let window = Duration::from_secs(30);
            let (p50, p95, p99) = {
                let mut guard = sample_latencies.lock().await;
                guard
                    .snapshot(window)
                    .map(|(a, b, c, _)| (a, b, c))
                    .unwrap_or((0.0, 0.0, 0.0))
            };
            let depth = sample_depth.load(Ordering::Relaxed) as f64;
            let ts = now_iso_ms();

            for (metric, value, window_s) in [
                ("claim_p50_ms", p50, 30.0),
                ("claim_p95_ms", p95, 30.0),
                ("claim_p99_ms", p99, 30.0),
                ("enqueue_rate", enq_rate, sample_every_s as f64),
                ("completion_rate", cmp_rate, sample_every_s as f64),
                ("queue_depth", depth, 0.0),
            ] {
                emit(json!({
                    "t": ts,
                    "system": "awa",
                    "kind": "adapter",
                    "subject_kind": "adapter",
                    "subject": "",
                    "metric": metric,
                    "value": value,
                    "window_s": window_s,
                }));
            }
        }
    });

    // ── Wait for SIGTERM / ctrl-c ───────────────────────────────────
    let sig_handler = tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install signal handler");
    });
    let term_handler = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            match signal(SignalKind::terminate()) {
                Ok(mut term) => {
                    let _ = term.recv().await;
                }
                Err(_) => {
                    std::future::pending::<()>().await;
                }
            }
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<()>().await;
        }
    });

    tokio::select! {
        _ = sig_handler => {},
        _ = term_handler => {},
    }
    eprintln!("[awa] long_horizon: shutdown signal received");
    shutdown.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        let _ = producer_handle.await;
        let _ = depth_handle.await;
        let _ = sample_handle.await;
    })
    .await;
    client.shutdown(Duration::from_secs(5)).await;
}
