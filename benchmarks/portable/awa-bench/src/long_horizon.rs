//! Long-horizon scenario: fixed-rate producer + steady consumer, with rolling
//! JSONL telemetry samples emitted every `SAMPLE_EVERY_S`.
//!
//! Contract: benchmarks/portable/CONTRIBUTING_ADAPTERS.md
//!
//! Startup: emits one descriptor record, then runs indefinitely. SIGTERM
//! flushes pending samples and exits 0 within 5s.

use async_trait::async_trait;
use awa_macros::JobArgs;
use awa_model::{insert, insert_many_copy_from_pool, InsertParams, QueueStorage};
use awa_worker::{
    Client, JobContext, JobError, JobResult, QueueConfig, TransitionWorkerRole, Worker,
};
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
    /// Wall-clock producer timestamp in unix milliseconds. Used for
    /// subscriber and end-to-end latency.
    pub produced_at_ms: i64,
    /// Synthetic semantics knob for the Awa-only retry benchmark: fail once
    /// on the first attempt, then succeed on retry.
    pub fail_first_attempt: bool,
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
    subscriber_latencies: Arc<Mutex<LatencyWindow>>,
    end_to_end_latencies: Arc<Mutex<LatencyWindow>>,
    completed_counter: Arc<AtomicU64>,
    retryable_failures_counter: Arc<AtomicU64>,
    completed_priority_1_counter: Arc<AtomicU64>,
    completed_priority_4_counter: Arc<AtomicU64>,
    completed_original_priority_1_counter: Arc<AtomicU64>,
    completed_original_priority_4_counter: Arc<AtomicU64>,
    aged_completion_counter: Arc<AtomicU64>,
}

#[async_trait]
impl Worker for LongHorizonWorker {
    fn kind(&self) -> &'static str {
        "long_horizon_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: LongHorizonJob = serde_json::from_value(ctx.job.args.clone())
            .map_err(|err| JobError::Terminal(format!("failed to deserialize args: {err}")))?;
        if args.fail_first_attempt && ctx.job.attempt == 1 {
            self.retryable_failures_counter
                .fetch_add(1, Ordering::Relaxed);
            return Err(JobError::retryable_msg(
                "synthetic first-attempt retryable failure",
            ));
        }
        let subscriber_latency_ms = (now_epoch_ms() - args.produced_at_ms).max(0) as f64;
        self.subscriber_latencies
            .lock()
            .await
            .record(subscriber_latency_ms);
        if self.work_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.work_ms)).await;
        }
        let end_to_end_latency_ms = (now_epoch_ms() - args.produced_at_ms).max(0) as f64;
        self.end_to_end_latencies
            .lock()
            .await
            .record(end_to_end_latency_ms);
        self.completed_counter.fetch_add(1, Ordering::Relaxed);
        match ctx.job.priority {
            1 => {
                self.completed_priority_1_counter
                    .fetch_add(1, Ordering::Relaxed);
            }
            4 => {
                self.completed_priority_4_counter
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        let original_priority = ctx
            .job
            .metadata
            .get("_awa_original_priority")
            .and_then(|value| value.as_i64())
            .and_then(|value| i16::try_from(value).ok())
            .unwrap_or(ctx.job.priority);
        match original_priority {
            1 => {
                self.completed_original_priority_1_counter
                    .fetch_add(1, Ordering::Relaxed);
            }
            4 => {
                self.completed_original_priority_4_counter
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        if original_priority != ctx.job.priority {
            self.aged_completion_counter.fetch_add(1, Ordering::Relaxed);
        }
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

fn env_u16(name: &str, default: u16) -> u16 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn priority_aging_interval() -> Duration {
    Duration::from_millis(
        std::env::var("PRIORITY_AGING_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60_000),
    )
}

fn parse_priority_pattern() -> Vec<i16> {
    let raw = env_string("JOB_PRIORITY_PATTERN", "2");
    let parsed: Vec<i16> = raw
        .split(',')
        .filter_map(|part| {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                return None;
            }
            trimmed.parse::<i16>().ok().filter(|p| (1..=4).contains(p))
        })
        .collect();
    if parsed.is_empty() {
        vec![2]
    } else {
        parsed
    }
}

fn read_producer_rate(default: u64) -> u64 {
    let Some(path) = std::env::var("PRODUCER_RATE_CONTROL_FILE").ok() else {
        return default;
    };
    std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .map(|value| value.max(0.0) as u64)
        .unwrap_or(default)
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

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn emit(record: serde_json::Value) {
    // Single println per sample: tailer is line-buffered.
    println!("{}", record);
}

fn instance_id() -> u32 {
    env_u32("BENCH_INSTANCE_ID", 0)
}

fn producer_enabled() -> bool {
    if env_u32("PRODUCER_ONLY_INSTANCE_ZERO", 0) == 0 {
        true
    } else {
        instance_id() == 0
    }
}

fn observer_enabled() -> bool {
    instance_id() == 0
}

fn build_batch_params(
    queue_name: &str,
    next_seq: &mut i64,
    batch_size: usize,
    priority_pattern: &[i16],
    fail_first_mod: i64,
    max_attempts: i16,
    padding: &str,
) -> Vec<InsertParams> {
    let mut params = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        let seq = *next_seq;
        let priority = priority_pattern[seq.rem_euclid(priority_pattern.len() as i64) as usize];
        let args = LongHorizonJob {
            seq,
            produced_at_ms: now_epoch_ms(),
            fail_first_attempt: fail_first_mod > 0 && seq.rem_euclid(fail_first_mod) == 0,
            padding: padding.to_owned(),
        };
        params.push(
            insert::params_with(
                &args,
                awa_model::InsertOpts {
                    queue: queue_name.into(),
                    priority,
                    max_attempts,
                    ..Default::default()
                },
            )
            .expect("failed to build queue storage params"),
        );
        *next_seq += 1;
    }
    params
}

fn queue_storage_event_tables(
    schema: &str,
    queue_slot_count: usize,
    lease_slot_count: usize,
    claim_slot_count: usize,
) -> Vec<String> {
    // Only register child partitions and unpartitioned tables.
    // pgstattuple cannot operate on partitioned parents
    // (`lease_claims`, `lease_claim_closures`, `ready_entries`,
    // `done_entries`, `leases`), so registering parents would just
    // spam the collector with errors and never produce a row.
    // `deferred_jobs`, `dlq_entries`, `attempt_state`, and the
    // queue/lease/claim ring state/slot tables are unpartitioned —
    // pgstattuple works on them directly.
    let mut tables = vec![
        format!("{schema}.queue_ring_state"),
        format!("{schema}.queue_ring_slots"),
        format!("{schema}.lease_ring_state"),
        format!("{schema}.lease_ring_slots"),
        format!("{schema}.claim_ring_state"),
        format!("{schema}.claim_ring_slots"),
        format!("{schema}.queue_lanes"),
        format!("{schema}.attempt_state"),
        format!("{schema}.deferred_jobs"),
        format!("{schema}.dlq_entries"),
    ];
    for slot in 0..queue_slot_count {
        tables.push(format!("{schema}.ready_entries_{slot}"));
        tables.push(format!("{schema}.done_entries_{slot}"));
    }
    for slot in 0..lease_slot_count {
        tables.push(format!("{schema}.leases_{slot}"));
    }
    for slot in 0..claim_slot_count {
        tables.push(format!("{schema}.lease_claims_{slot}"));
        tables.push(format!("{schema}.lease_claim_closures_{slot}"));
    }
    tables
}

fn canonical_event_tables() -> Vec<String> {
    vec![
        "awa.jobs_hot".to_string(),
        "awa.scheduled_jobs".to_string(),
        "awa.queue_state_counts".to_string(),
    ]
}

fn emit_descriptor(system: &str, db_name: &str, event_tables: Vec<String>) {
    emit(json!({
        "kind": "descriptor",
        "system": system,
        "instance_id": instance_id(),
        "event_tables": event_tables,
        "event_indexes": [],
        "extensions": [],
        "version": env!("CARGO_PKG_VERSION"),
        "schema_version": env_string("AWA_SCHEMA_VERSION", "current"),
        "db_name": db_name,
        "started_at": now_iso_ms(),
    }));
}

async fn poll_canonical_depths(
    pool: &sqlx::PgPool,
    queue_name: &str,
) -> Result<(u64, u64, u64, u64), sqlx::Error> {
    let (available, running, retryable, scheduled): (i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
            COALESCE((SELECT count(*)::bigint FROM awa.jobs_hot WHERE queue = $1 AND state = 'available'), 0) AS available,
            COALESCE((SELECT count(*)::bigint FROM awa.jobs_hot WHERE queue = $1 AND state = 'running'), 0) AS running,
            COALESCE((SELECT count(*)::bigint FROM awa.scheduled_jobs WHERE queue = $1 AND state = 'retryable'), 0) AS retryable,
            COALESCE((SELECT count(*)::bigint FROM awa.scheduled_jobs WHERE queue = $1 AND state = 'scheduled'), 0) AS scheduled
        "#,
    )
    .bind(queue_name)
    .fetch_one(pool)
    .await?;
    Ok((
        available.max(0) as u64,
        running.max(0) as u64,
        retryable.max(0) as u64,
        scheduled.max(0) as u64,
    ))
}

pub async fn run() {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let producer_rate = env_u64("PRODUCER_RATE", 800);
    let producer_mode = env_string("PRODUCER_MODE", "fixed");
    let target_depth = env_u64("TARGET_DEPTH", 1000);
    let worker_count = env_u32("WORKER_COUNT", 32);
    let payload_bytes = env_u64("JOB_PAYLOAD_BYTES", 256);
    let work_ms = env_u64("JOB_WORK_MS", 1);
    let fail_first_mod = env_u64("JOB_FAIL_FIRST_MOD", 0) as i64;
    let max_attempts = env_u16("JOB_MAX_ATTEMPTS", 25) as i16;
    let priority_pattern = parse_priority_pattern();
    let sample_every_s = env_u64("SAMPLE_EVERY_S", 10);
    let latency_window = Duration::from_millis(env_u64("LATENCY_WINDOW_MS", 30_000).max(1));
    let producer_batch_ms = env_u64("PRODUCER_BATCH_MS", 25).max(1);
    let producer_batch_max = env_u64("PRODUCER_BATCH_MAX", 128).max(1) as usize;
    // Per-replica connection pool. Sized to cover the steady-state
    // demand: 8 workers × concurrent in-flight + dispatcher + 4 batcher
    // shards + heartbeat + maintenance + producer + depth poller +
    // sampler. Under-sizing surfaces as `pool timed out` errors and a
    // memory build-up because spawned `complete_job` futures pile up
    // holding their captured (job, result, progress_snapshot) state.
    let default_max_connections = (worker_count.saturating_mul(4)).saturating_add(48).max(80);
    let max_connections = env_u32("MAX_CONNECTIONS", default_max_connections);
    let db_name = database_url
        .rsplit('/')
        .next()
        .unwrap_or("awa_bench")
        .to_string();
    let system_name = super::bench_system_name();
    let storage_engine = super::storage_engine_mode();

    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");
    let queue_storage = match storage_engine {
        super::StorageEngineMode::QueueStorage => {
            let storage = super::queue_storage_config_with_receipts_default(true);
            let (store, storage) =
                super::prepare_queue_storage_with_config(&pool, storage, false).await;
            emit_descriptor(
                &system_name,
                &db_name,
                queue_storage_event_tables(
                    store.schema(),
                    store.queue_slot_count(),
                    store.lease_slot_count(),
                    store.claim_slot_count(),
                ),
            );
            Some((store, storage))
        }
        super::StorageEngineMode::Canonical => {
            super::prepare_canonical(&pool).await;
            emit_descriptor(&system_name, &db_name, canonical_event_tables());
            None
        }
    };
    let use_lease_claim_receipts = queue_storage
        .as_ref()
        .map(|(_, storage)| storage.lease_claim_receipts)
        .unwrap_or(false);

    let queue_name = "awa_longhorizon_bench";
    // No clean here — the bench harness starts from a fresh PG, so existing
    // rows are not an issue. For --fast we do see stale rows across runs;
    // that's acceptable for dev iteration.

    let producer_call_latencies = Arc::new(Mutex::new(LatencyWindow::new()));
    let producer_latencies = Arc::new(Mutex::new(LatencyWindow::new()));
    let subscriber_latencies = Arc::new(Mutex::new(LatencyWindow::new()));
    let end_to_end_latencies = Arc::new(Mutex::new(LatencyWindow::new()));
    let completed = Arc::new(AtomicU64::new(0));
    let retryable_failures = Arc::new(AtomicU64::new(0));
    let completed_priority_1 = Arc::new(AtomicU64::new(0));
    let completed_priority_4 = Arc::new(AtomicU64::new(0));
    let completed_original_priority_1 = Arc::new(AtomicU64::new(0));
    let completed_original_priority_4 = Arc::new(AtomicU64::new(0));
    let aged_completions = Arc::new(AtomicU64::new(0));
    let enqueued = Arc::new(AtomicU64::new(0));
    let queue_depth = Arc::new(AtomicU64::new(0));
    let running_depth = Arc::new(AtomicU64::new(0));
    let retryable_depth = Arc::new(AtomicU64::new(0));
    let scheduled_depth = Arc::new(AtomicU64::new(0));
    let producer_target_rate = Arc::new(AtomicU64::new(producer_rate));

    let worker = LongHorizonWorker {
        work_ms,
        subscriber_latencies: Arc::clone(&subscriber_latencies),
        end_to_end_latencies: Arc::clone(&end_to_end_latencies),
        completed_counter: Arc::clone(&completed),
        retryable_failures_counter: Arc::clone(&retryable_failures),
        completed_priority_1_counter: Arc::clone(&completed_priority_1),
        completed_priority_4_counter: Arc::clone(&completed_priority_4),
        completed_original_priority_1_counter: Arc::clone(&completed_original_priority_1),
        completed_original_priority_4_counter: Arc::clone(&completed_original_priority_4),
        aged_completion_counter: Arc::clone(&aged_completions),
    };

    let client_builder = Client::builder(pool.clone())
        .priority_aging_interval(priority_aging_interval())
        .queue(
            queue_name,
            QueueConfig {
                max_workers: worker_count,
                poll_interval: Duration::from_millis(50),
                deadline_duration: if use_lease_claim_receipts {
                    Duration::ZERO
                } else {
                    QueueConfig::default().deadline_duration
                },
                priority_aging_interval: priority_aging_interval(),
                ..QueueConfig::default()
            },
        )
        // The portable bench is measuring queue behavior, not the cost of
        // refreshing runtime/admin snapshots every few seconds.
        .queue_stats_interval(Duration::from_secs(300))
        .runtime_snapshot_interval(Duration::from_secs(300))
        .register_worker(worker);
    let client = match &queue_storage {
        Some((_, storage)) => client_builder
            .queue_storage(
                storage.clone(),
                super::queue_rotate_interval(),
                super::lease_rotate_interval(),
            )
            .transition_role(TransitionWorkerRole::QueueStorageTarget)
            .build()
            .expect("Failed to build client"),
        None => client_builder
            .canonical_storage()
            .build()
            .expect("Failed to build client"),
    };
    client.start().await.expect("Failed to start client");

    let shutdown = Arc::new(AtomicBool::new(false));

    // ── Producer ────────────────────────────────────────────────────
    let producer_pool = pool.clone();
    let producer_shutdown = Arc::clone(&shutdown);
    let producer_enqueued = Arc::clone(&enqueued);
    let producer_queue_depth = Arc::clone(&queue_depth);
    let producer_target_rate_metric = Arc::clone(&producer_target_rate);
    let producer_call_latencies_window = Arc::clone(&producer_call_latencies);
    let producer_latencies_window = Arc::clone(&producer_latencies);
    let padding = "x".repeat(payload_bytes.saturating_sub(32) as usize);
    let producer_priority_pattern = priority_pattern.clone();
    // Build the QueueStorage handle once outside the producer loop;
    // the handle is conceptually pool-scoped, not batch-scoped, and
    // re-creating it per batch would be pure allocator pressure at
    // high producer rates.
    let producer_store = queue_storage.as_ref().map(|(_, storage)| {
        QueueStorage::new(storage.clone()).expect("Invalid QueueStorageConfig")
    });
    let producer_handle = tokio::spawn(async move {
        if !producer_enabled() {
            while !producer_shutdown.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            return;
        }
        let mut seq: i64 = 0;
        let mut fixed_rate_credit = 0.0_f64;
        let mut next_tick = tokio::time::Instant::now();
        while !producer_shutdown.load(Ordering::Relaxed) {
            let batch_size = if producer_mode == "depth-target" {
                producer_target_rate_metric.store(0, Ordering::Relaxed);
                let depth = producer_queue_depth.load(Ordering::Relaxed);
                if depth >= target_depth {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                next_tick = tokio::time::Instant::now();
                ((target_depth - depth) as usize).clamp(1, producer_batch_max)
            } else {
                let current_rate = read_producer_rate(producer_rate);
                producer_target_rate_metric.store(current_rate, Ordering::Relaxed);
                if current_rate == 0 {
                    fixed_rate_credit = 0.0;
                    next_tick = tokio::time::Instant::now();
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                let period = Duration::from_millis(producer_batch_ms);
                next_tick = std::cmp::max(next_tick + period, tokio::time::Instant::now());
                tokio::time::sleep_until(next_tick).await;
                fixed_rate_credit += current_rate as f64 * (producer_batch_ms as f64 / 1_000.0);
                let whole = fixed_rate_credit.floor() as usize;
                if whole == 0 {
                    continue;
                }
                fixed_rate_credit -= whole as f64;
                whole.min(producer_batch_max)
            };

            let mut next_seq = seq;
            let params = build_batch_params(
                queue_name,
                &mut next_seq,
                batch_size,
                &producer_priority_pattern,
                fail_first_mod,
                max_attempts,
                &padding,
            );
            let insert_start = Instant::now();
            let res = match storage_engine {
                super::StorageEngineMode::QueueStorage => {
                    let store = producer_store
                        .as_ref()
                        .expect("queue storage config missing");
                    store
                        .enqueue_params_batch(&producer_pool, &params)
                        .await
                }
                super::StorageEngineMode::Canonical => {
                    insert_many_copy_from_pool(&producer_pool, &params)
                        .await
                        .map(|rows| rows.len())
                }
            };
            match res {
                Ok(_) => {
                    let latency_ms = insert_start.elapsed().as_secs_f64() * 1_000.0;
                    let effective_per_message_ms = latency_ms / batch_size as f64;
                    producer_call_latencies_window
                        .lock()
                        .await
                        .record(latency_ms);
                    let mut guard = producer_latencies_window.lock().await;
                    for _ in 0..batch_size {
                        guard.record(effective_per_message_ms);
                    }
                    drop(guard);
                    producer_enqueued.fetch_add(batch_size as u64, Ordering::Relaxed);
                    seq = next_seq;
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
    // For short phases, a long cache TTL makes the observer report stale
    // running/available counts long after the queue has drained. Keep the
    // default tied to the sampling cadence, but allow overrides for profiling.
    let queue_count_max_age = Duration::from_millis(
        std::env::var("QUEUE_COUNT_MAX_AGE_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(sample_every_s.saturating_mul(1_000)),
    );
    let depth_handle = {
        let queue_depth = Arc::clone(&queue_depth);
        let running_depth = Arc::clone(&running_depth);
        let retryable_depth = Arc::clone(&retryable_depth);
        let scheduled_depth = Arc::clone(&scheduled_depth);
        let depth_storage = queue_storage.as_ref().map(|(_, storage)| storage.clone());
        tokio::spawn(async move {
            if !observer_enabled() {
                while !depth_shutdown.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                return;
            }
            while !depth_shutdown.load(Ordering::Relaxed) {
                match storage_engine {
                    super::StorageEngineMode::QueueStorage => {
                        let depth_store = QueueStorage::new(
                            depth_storage.clone().expect("queue storage config missing"),
                        )
                        .expect("Invalid QueueStorageConfig");
                        match depth_store
                            .queue_counts_cached(&depth_pool, queue_name, queue_count_max_age)
                            .await
                        {
                            Ok(counts) => {
                                queue_depth.store(counts.available as u64, Ordering::Relaxed);
                                running_depth.store(counts.running as u64, Ordering::Relaxed);
                                match sqlx::query_as::<_, (i64, i64)>(&format!(
                                    r#"
                                    SELECT
                                        COALESCE(sum(CASE WHEN state = 'retryable' THEN 1 ELSE 0 END), 0)::bigint AS retryable,
                                        COALESCE(sum(CASE WHEN state = 'scheduled' THEN 1 ELSE 0 END), 0)::bigint AS scheduled
                                    FROM {}.deferred_jobs
                                    WHERE queue = $1
                                    "#,
                                    depth_store.schema()
                                ))
                                .bind(queue_name)
                                .fetch_one(&depth_pool)
                                .await
                                {
                                    Ok((retryable, scheduled)) => {
                                        retryable_depth.store(retryable as u64, Ordering::Relaxed);
                                        scheduled_depth.store(scheduled as u64, Ordering::Relaxed);
                                    }
                                    Err(err) => {
                                        eprintln!("[awa] deferred depth poll failed: {err}");
                                    }
                                }
                            }
                            Err(err) => {
                                eprintln!("[awa] queue depth poll failed: {err}");
                            }
                        }
                    }
                    super::StorageEngineMode::Canonical => {
                        match poll_canonical_depths(&depth_pool, queue_name).await {
                            Ok((available, running, retryable, scheduled)) => {
                                queue_depth.store(available, Ordering::Relaxed);
                                running_depth.store(running, Ordering::Relaxed);
                                retryable_depth.store(retryable, Ordering::Relaxed);
                                scheduled_depth.store(scheduled, Ordering::Relaxed);
                            }
                            Err(err) => {
                                eprintln!("[awa] canonical queue depth poll failed: {err}");
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    };

    // ── Sample emitter ──────────────────────────────────────────────
    let sample_shutdown = Arc::clone(&shutdown);
    let sample_enqueued = Arc::clone(&enqueued);
    let sample_completed = Arc::clone(&completed);
    let sample_retryable_failures = Arc::clone(&retryable_failures);
    let sample_completed_priority_1 = Arc::clone(&completed_priority_1);
    let sample_completed_priority_4 = Arc::clone(&completed_priority_4);
    let sample_completed_original_priority_1 = Arc::clone(&completed_original_priority_1);
    let sample_completed_original_priority_4 = Arc::clone(&completed_original_priority_4);
    let sample_aged_completions = Arc::clone(&aged_completions);
    let sample_depth = Arc::clone(&queue_depth);
    let sample_running_depth = Arc::clone(&running_depth);
    let sample_retryable_depth = Arc::clone(&retryable_depth);
    let sample_scheduled_depth = Arc::clone(&scheduled_depth);
    let sample_producer_call_latencies = Arc::clone(&producer_call_latencies);
    let sample_producer_latencies = Arc::clone(&producer_latencies);
    let sample_subscriber_latencies = Arc::clone(&subscriber_latencies);
    let sample_end_to_end_latencies = Arc::clone(&end_to_end_latencies);
    let sample_target_rate = Arc::clone(&producer_target_rate);
    let sample_handle = tokio::spawn(async move {
        // Align first tick to the next wall-clock `sample_every_s` boundary.
        let now_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let next_boundary_s = ((now_epoch / sample_every_s) + 1) * sample_every_s;
        let sleep_for = next_boundary_s.saturating_sub(now_epoch);
        tokio::time::sleep(Duration::from_secs(sleep_for)).await;

        let mut last_enqueued: u64 = sample_enqueued.load(Ordering::Relaxed);
        let mut last_completed: u64 = sample_completed.load(Ordering::Relaxed);
        let mut last_retryable_failures: u64 =
            sample_retryable_failures.load(Ordering::Relaxed);
        let mut last_completed_priority_1: u64 =
            sample_completed_priority_1.load(Ordering::Relaxed);
        let mut last_completed_priority_4: u64 =
            sample_completed_priority_4.load(Ordering::Relaxed);
        let mut last_completed_original_priority_1: u64 =
            sample_completed_original_priority_1.load(Ordering::Relaxed);
        let mut last_completed_original_priority_4: u64 =
            sample_completed_original_priority_4.load(Ordering::Relaxed);
        let mut last_aged_completions: u64 =
            sample_aged_completions.load(Ordering::Relaxed);
        let mut last_tick = Instant::now();
        let mut ticker = interval_at(
            tokio::time::Instant::now() + Duration::from_secs(sample_every_s),
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
            let retryable = sample_retryable_failures.load(Ordering::Relaxed);
            let completed_p1 = sample_completed_priority_1.load(Ordering::Relaxed);
            let completed_p4 = sample_completed_priority_4.load(Ordering::Relaxed);
            let completed_original_p1 =
                sample_completed_original_priority_1.load(Ordering::Relaxed);
            let completed_original_p4 =
                sample_completed_original_priority_4.load(Ordering::Relaxed);
            let aged_completions = sample_aged_completions.load(Ordering::Relaxed);
            let enq_rate = (enq - last_enqueued) as f64 / dt;
            let cmp_rate = (cmp - last_completed) as f64 / dt;
            let retryable_rate = (retryable - last_retryable_failures) as f64 / dt;
            let completed_p1_rate =
                (completed_p1 - last_completed_priority_1) as f64 / dt;
            let completed_p4_rate =
                (completed_p4 - last_completed_priority_4) as f64 / dt;
            let completed_original_p1_rate =
                (completed_original_p1 - last_completed_original_priority_1) as f64 / dt;
            let completed_original_p4_rate =
                (completed_original_p4 - last_completed_original_priority_4) as f64 / dt;
            let aged_completion_rate =
                (aged_completions - last_aged_completions) as f64 / dt;
            last_enqueued = enq;
            last_completed = cmp;
            last_retryable_failures = retryable;
            last_completed_priority_1 = completed_p1;
            last_completed_priority_4 = completed_p4;
            last_completed_original_priority_1 = completed_original_p1;
            last_completed_original_priority_4 = completed_original_p4;
            last_aged_completions = aged_completions;

            let window = latency_window;
            let latency_window_s = latency_window.as_secs_f64();
            let (producer_call_p50, producer_call_p95, producer_call_p99) = {
                let mut guard = sample_producer_call_latencies.lock().await;
                guard
                    .snapshot(window)
                    .map(|(a, b, c, _)| (a, b, c))
                    .unwrap_or((0.0, 0.0, 0.0))
            };
            let (producer_p50, producer_p95, producer_p99) = {
                let mut guard = sample_producer_latencies.lock().await;
                guard
                    .snapshot(window)
                    .map(|(a, b, c, _)| (a, b, c))
                    .unwrap_or((0.0, 0.0, 0.0))
            };
            let (subscriber_p50, subscriber_p95, subscriber_p99) = {
                let mut guard = sample_subscriber_latencies.lock().await;
                guard
                    .snapshot(window)
                    .map(|(a, b, c, _)| (a, b, c))
                    .unwrap_or((0.0, 0.0, 0.0))
            };
            let (end_to_end_p50, end_to_end_p95, end_to_end_p99) = {
                let mut guard = sample_end_to_end_latencies.lock().await;
                guard
                    .snapshot(window)
                    .map(|(a, b, c, _)| (a, b, c))
                    .unwrap_or((0.0, 0.0, 0.0))
            };
            let depth = sample_depth.load(Ordering::Relaxed) as f64;
            let running_depth = sample_running_depth.load(Ordering::Relaxed) as f64;
            let retryable_depth = sample_retryable_depth.load(Ordering::Relaxed) as f64;
            let scheduled_depth = sample_scheduled_depth.load(Ordering::Relaxed) as f64;
            let total_backlog = depth + running_depth + retryable_depth + scheduled_depth;
            let target_rate = sample_target_rate.load(Ordering::Relaxed) as f64;
            let ts = now_iso_ms();

            for (metric, value, window_s) in [
                ("producer_call_p50_ms", producer_call_p50, latency_window_s),
                ("producer_call_p95_ms", producer_call_p95, latency_window_s),
                ("producer_call_p99_ms", producer_call_p99, latency_window_s),
                ("producer_p50_ms", producer_p50, latency_window_s),
                ("producer_p95_ms", producer_p95, latency_window_s),
                ("producer_p99_ms", producer_p99, latency_window_s),
                ("subscriber_p50_ms", subscriber_p50, latency_window_s),
                ("subscriber_p95_ms", subscriber_p95, latency_window_s),
                ("subscriber_p99_ms", subscriber_p99, latency_window_s),
                ("end_to_end_p50_ms", end_to_end_p50, latency_window_s),
                ("end_to_end_p95_ms", end_to_end_p95, latency_window_s),
                ("end_to_end_p99_ms", end_to_end_p99, latency_window_s),
                ("claim_p50_ms", subscriber_p50, latency_window_s),
                ("claim_p95_ms", subscriber_p95, latency_window_s),
                ("claim_p99_ms", subscriber_p99, latency_window_s),
                ("enqueue_rate", enq_rate, sample_every_s as f64),
                ("completion_rate", cmp_rate, sample_every_s as f64),
                ("retryable_failure_rate", retryable_rate, sample_every_s as f64),
                ("completed_priority_1_rate", completed_p1_rate, sample_every_s as f64),
                ("completed_priority_4_rate", completed_p4_rate, sample_every_s as f64),
                (
                    "completed_original_priority_1_rate",
                    completed_original_p1_rate,
                    sample_every_s as f64,
                ),
                (
                    "completed_original_priority_4_rate",
                    completed_original_p4_rate,
                    sample_every_s as f64,
                ),
                ("aged_completion_rate", aged_completion_rate, sample_every_s as f64),
            ] {
                emit(json!({
                    "t": ts,
                    "system": system_name,
                    "instance_id": instance_id(),
                    "kind": "adapter",
                    "subject_kind": "adapter",
                    "subject": "",
                    "metric": metric,
                    "value": value,
                    "window_s": window_s,
                }));
            }
            if observer_enabled() {
                for (metric, value, window_s) in [
                    ("queue_depth", depth, 0.0),
                    ("running_depth", running_depth, 0.0),
                    ("retryable_depth", retryable_depth, 0.0),
                    ("scheduled_depth", scheduled_depth, 0.0),
                    ("total_backlog", total_backlog, 0.0),
                    ("producer_target_rate", target_rate, 0.0),
                ] {
                    emit(json!({
                        "t": ts,
                        "system": system_name,
                        "instance_id": instance_id(),
                        "kind": "adapter",
                        "subject_kind": "adapter",
                        "subject": system_name,
                        "metric": metric,
                        "value": value,
                        "window_s": window_s,
                    }));
                }
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
    let _ = tokio::time::timeout(
        Duration::from_secs(3),
        client.shutdown(Duration::from_secs(3)),
    )
    .await;
}
