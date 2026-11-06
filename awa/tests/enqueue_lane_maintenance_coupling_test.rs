//! How a saturated enqueue lane affects the maintenance leader.
//!
//! Promotion of scheduled and retryable jobs re-enqueues them into their lane,
//! so it queues on the same per-lane advisory lock as producers. A promotion
//! batch is one transaction across every queue it touches, and the leader runs
//! its branches (promotion, rescue, rotation, ...) one after another on a
//! single task, so a batch parked behind a hot lane holds up the due jobs of
//! other queues in that batch and every other maintenance branch.
//!
//! `test_maintenance_under_hot_lane` measures, with and without concurrent
//! producers on one hot lane:
//!   * promotion batch latency and how long a backlog on the hot queue takes
//!     to drain,
//!   * promotion lag on an unrelated queue (time from a job becoming due to
//!     it being visible as ready), with the promotion loop shaped like the
//!     leader's (a tick, then batches until one comes back short),
//!   * heartbeat rescue of abandoned claims on the hot queue: how long each run
//!     takes and how late it starts once its tick is due (1s here, so a
//!     blocked promotion shows up as a late start).
//!
//! Tunables (optional):
//!   AWA_LANE_MAINT_PRODUCERS   producers on the hot lane, default 16
//!   AWA_LANE_MAINT_BACKLOG     due scheduled jobs on the hot queue, default 24576
//!   AWA_LANE_MAINT_RESCUE      abandoned claims on the hot queue, default 6000
//!
//! Requires a running Postgres instance.
//! Run with:
//!   cargo test --release --package awa --test enqueue_lane_maintenance_coupling_test \
//!       -- --ignored --nocapture --test-threads=1

use awa::model::{insert_many, QueueStorage};
use awa::{InsertOpts, InsertParams, JobState, QueueStorageConfig};
use awa_testing::setup;
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const PROMOTE_BATCH: i64 = 4_096;
const PROMOTE_TICK: Duration = Duration::from_millis(250);
const PROBE_PERIOD: Duration = Duration::from_millis(100);
const RESCUE_TICK: Duration = Duration::from_secs(1);

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn percentile(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).expect("finite"));
    sorted[((sorted.len() as f64 - 1.0) * quantile).round() as usize]
}

fn params(
    queue: &str,
    tag: &str,
    n: u64,
    run_at: Option<chrono::DateTime<chrono::Utc>>,
) -> InsertParams {
    InsertParams {
        kind: "lane_maint_job".to_string(),
        args: serde_json::json!({ "tag": tag, "n": n }),
        opts: InsertOpts {
            queue: queue.to_string(),
            run_at,
            ..Default::default()
        },
    }
}

async fn seed_due_backlog(pool: &PgPool, queue: &str, count: usize) {
    let due = chrono::Utc::now() - chrono::Duration::seconds(60);
    let mut inserted = 0usize;
    while inserted < count {
        let chunk = (count - inserted).min(2_000);
        let jobs: Vec<InsertParams> = (0..chunk)
            .map(|offset| params(queue, "backlog", (inserted + offset) as u64, Some(due)))
            .collect();
        insert_many(pool, &jobs).await.expect("seed backlog");
        inserted += chunk;
    }
}

async fn deferred_due(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*)::bigint FROM awa.deferred_jobs WHERE queue = $1 AND state = 'scheduled'",
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("count deferred")
}

struct Producers {
    stop: Arc<AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<u64>>,
}

impl Producers {
    fn start(pool: &PgPool, queue: &str, count: usize) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let tag = Uuid::new_v4().simple().to_string();
        let tasks = (0..count)
            .map(|producer| {
                let pool = pool.clone();
                let stop = Arc::clone(&stop);
                let queue = queue.to_string();
                let tag = tag.clone();
                tokio::spawn(async move {
                    let mut jobs_inserted = 0u64;
                    let mut counter = (producer as u64) << 40;
                    while !stop.load(Ordering::Relaxed) {
                        let jobs: Vec<InsertParams> = (0..185)
                            .map(|offset| params(&queue, &tag, counter + offset, None))
                            .collect();
                        counter += 185;
                        insert_many(&pool, &jobs).await.expect("producer insert");
                        jobs_inserted += 185;
                    }
                    jobs_inserted
                })
            })
            .collect();
        Self { stop, tasks }
    }

    async fn stop(self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        let mut total = 0;
        for task in self.tasks {
            total += task.await.expect("producer task");
        }
        total
    }
}

#[derive(Default)]
struct LeaderReport {
    /// Promotion batches that moved a full share of the backlog.
    batch_ms: Vec<f64>,
    drain_s: f64,
    probe_lag_ms: Vec<f64>,
    /// How long after its tick each heartbeat-rescue run started, and how long
    /// the run itself took.
    rescue_start_delay_ms: Vec<f64>,
    rescue_run_ms: Vec<f64>,
}

/// A leader loop shaped like the maintenance leader's: one task selecting over
/// a promotion timer (250ms; on each tick batches of up to 4096 until one is
/// short) and a heartbeat-rescue timer, running each branch to completion
/// before taking the next tick. Runs until the hot queue's backlog is gone and
/// `min_window` has passed, while a probe feeds one due job per 100ms into an
/// unrelated queue and times how long each takes to become ready.
async fn run_leader(
    pool: &PgPool,
    store: &QueueStorage,
    hot: &str,
    other: &str,
    min_window: Duration,
) -> LeaderReport {
    let mut report = LeaderReport::default();
    let started = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));

    let probe = {
        let pool = pool.clone();
        let other = other.to_string();
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut lags = Vec::new();
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                // Older than the hot backlog, so the probe is always in the
                // first promotion batch rather than behind the backlog's
                // run_at order.
                let due_at = chrono::Utc::now() - chrono::Duration::seconds(120);
                insert_many(&pool, &[params(&other, "probe", n, Some(due_at))])
                    .await
                    .expect("insert probe");
                let inserted = Instant::now();
                loop {
                    let visible: bool = sqlx::query_scalar(
                        "SELECT EXISTS (SELECT 1 FROM awa.ready_entries
                         WHERE queue = $1 AND args->>'tag' = 'probe' AND (args->>'n')::bigint = $2)",
                    )
                    .bind(&other)
                    .bind(n as i64)
                    .fetch_one(&pool)
                    .await
                    .expect("probe visibility");
                    if visible {
                        lags.push(inserted.elapsed().as_secs_f64() * 1e3);
                        break;
                    }
                    if stop.load(Ordering::Relaxed) {
                        return lags;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                tokio::time::sleep(PROBE_PERIOD).await;
            }
            lags
        })
    };

    let mut promote_timer = tokio::time::interval(PROMOTE_TICK);
    promote_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut rescue_timer = tokio::time::interval(RESCUE_TICK);
    rescue_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    promote_timer.tick().await;
    rescue_timer.tick().await;

    let mut backlog_gone_at: Option<Instant> = None;
    loop {
        tokio::select! {
            tick = promote_timer.tick() => {
                let _ = tick;
                loop {
                    let batch_started = Instant::now();
                    let promoted = store
                        .promote_due(pool, JobState::Scheduled, PROMOTE_BATCH)
                        .await
                        .expect("promote_due");
                    if promoted >= 1_000 {
                        report.batch_ms.push(batch_started.elapsed().as_secs_f64() * 1e3);
                    }
                    if (promoted as i64) < PROMOTE_BATCH {
                        break;
                    }
                }
                if backlog_gone_at.is_none() && deferred_due(pool, hot).await == 0 {
                    backlog_gone_at = Some(Instant::now());
                }
                if backlog_gone_at.is_some() && started.elapsed() >= min_window {
                    break;
                }
            }
            tick = rescue_timer.tick() => {
                let delay = tokio::time::Instant::now().saturating_duration_since(tick);
                let run_started = Instant::now();
                let rescued = store
                    .rescue_stale_heartbeats(pool, Duration::ZERO)
                    .await
                    .expect("rescue_stale_heartbeats");
                if !rescued.is_empty() {
                    report.rescue_start_delay_ms.push(delay.as_secs_f64() * 1e3);
                    report.rescue_run_ms.push(run_started.elapsed().as_secs_f64() * 1e3);
                }
            }
        }
    }
    report.drain_s = backlog_gone_at
        .expect("backlog drained")
        .duration_since(started)
        .as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    report.probe_lag_ms = probe.await.expect("probe task");
    report
}

fn max_of(values: &[f64]) -> f64 {
    values.iter().copied().fold(0.0, f64::max)
}

fn print_leader(label: &str, report: &LeaderReport) {
    println!(
        "[lane-maint] {label} promote batch ms p50={:.0} max={:.0} ({} batches) | hot backlog \
         drained in {:.2}s | unrelated-queue promotion lag ms p50={:.0} p99={:.0} max={:.0} ({} probes) \
         | rescue ran {} times, run ms p50={:.0} max={:.0}, started late by ms p50={:.0} max={:.0}",
        percentile(&report.batch_ms, 0.5),
        max_of(&report.batch_ms),
        report.batch_ms.len(),
        report.drain_s,
        percentile(&report.probe_lag_ms, 0.5),
        percentile(&report.probe_lag_ms, 0.99),
        max_of(&report.probe_lag_ms),
        report.probe_lag_ms.len(),
        report.rescue_run_ms.len(),
        percentile(&report.rescue_run_ms, 0.5),
        max_of(&report.rescue_run_ms),
        percentile(&report.rescue_start_delay_ms, 0.5),
        max_of(&report.rescue_start_delay_ms),
    );
}

/// Enqueue `count` jobs on `queue` and claim every one of them without ever
/// completing or heartbeating, so each becomes an abandoned attempt.
async fn seed_abandoned_claims(pool: &PgPool, store: &QueueStorage, queue: &str, count: usize) {
    let tag = Uuid::new_v4().simple().to_string();
    let mut enqueued = 0usize;
    while enqueued < count {
        let chunk = (count - enqueued).min(2_000);
        let jobs: Vec<InsertParams> = (0..chunk)
            .map(|offset| params(queue, &tag, (enqueued + offset) as u64, None))
            .collect();
        store
            .enqueue_params_batch(pool, &jobs)
            .await
            .expect("enqueue claimable jobs");
        enqueued += chunk;
    }
    let mut claimed = 0usize;
    while claimed < count {
        let batch = store
            .claim_runtime_batch(pool, queue, 500, Duration::from_secs(3_600))
            .await
            .expect("claim batch");
        assert!(!batch.is_empty(), "ran out of jobs to claim");
        claimed += batch.len();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_maintenance_under_hot_lane() {
    let producers = env_usize("AWA_LANE_MAINT_PRODUCERS", 16);
    let backlog = env_usize("AWA_LANE_MAINT_BACKLOG", 24_576);
    let rescue_claims = env_usize("AWA_LANE_MAINT_RESCUE", 6_000);

    let pool = setup::setup(producers as u32 + 12).await;
    setup::activate_queue_storage(&pool).await;
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");

    for (label, producer_count) in [("idle lane   ", 0usize), ("hot lane    ", producers)] {
        let suffix = Uuid::new_v4().simple().to_string();
        let hot = format!("lane_maint_hot_{}", &suffix[..10]);
        let other = format!("lane_maint_other_{}", &suffix[..10]);

        // Warm both lanes so lane provisioning is not measured.
        insert_many(
            &pool,
            &[
                params(&hot, "warm", 0, None),
                params(&other, "warm", 0, None),
            ],
        )
        .await
        .expect("warm lanes");

        seed_due_backlog(&pool, &hot, backlog).await;
        seed_abandoned_claims(&pool, &store, &hot, rescue_claims).await;

        let running = (producer_count > 0).then(|| Producers::start(&pool, &hot, producer_count));
        tokio::time::sleep(Duration::from_millis(1_500)).await;

        let report = run_leader(&pool, &store, &hot, &other, Duration::from_secs(12)).await;
        print_leader(label, &report);

        if let Some(running) = running {
            let produced = running.stop().await;
            println!("[lane-maint] {label} producers inserted {produced} jobs meanwhile");
        }
    }
}

/// The mechanism without producer load: one transaction holds the hot lane's
/// reservation lock for a fixed time while the leader loop runs. Promotion of
/// a batch that contains hot-queue rows waits for it, and takes the unrelated
/// queue's due job in the same batch and the leader's other branches with it.
/// Heartbeat rescue called directly does not wait for the lane at all: it
/// writes retryable rows to the deferred table, which promotion later moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_leader_behind_a_held_lane_lock() {
    let hold = Duration::from_millis(env_usize("AWA_LANE_MAINT_HOLD_MS", 3_000) as u64);
    let pool = setup::setup(16).await;
    setup::activate_queue_storage(&pool).await;
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");

    let suffix = Uuid::new_v4().simple().to_string();
    let hot = format!("lane_maint_hot_{}", &suffix[..10]);
    let other = format!("lane_maint_other_{}", &suffix[..10]);
    insert_many(
        &pool,
        &[
            params(&hot, "warm", 0, None),
            params(&other, "warm", 0, None),
        ],
    )
    .await
    .expect("warm lanes");
    seed_due_backlog(&pool, &hot, 8_192).await;
    seed_abandoned_claims(&pool, &store, &hot, 3_000).await;

    let key: i64 = sqlx::query_scalar(
        "SELECT hashtextextended(
            format('%I.%I', 'awa', awa.queue_lane_sequence_name('queue_enqueue_seq', $1, 2::smallint, 0::smallint)),
            0)",
    )
    .bind(&hot)
    .fetch_one(&pool)
    .await
    .expect("derive lane lock key");
    let mut gate = pool.begin().await.expect("begin gate");
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *gate)
        .await
        .expect("take lane lock");

    let rescue_started = Instant::now();
    let rescued = store
        .rescue_stale_heartbeats(&pool, Duration::ZERO)
        .await
        .expect("rescue with the lane lock held");
    println!(
        "[lane-maint] rescue of {} claims with the hot lane lock held took {:.0}ms",
        rescued.len(),
        rescue_started.elapsed().as_secs_f64() * 1e3
    );
    let remaining_hold = hold.saturating_sub(rescue_started.elapsed());

    let release = tokio::spawn(async move {
        tokio::time::sleep(remaining_hold).await;
        gate.commit().await.expect("release lane lock");
    });
    let report = run_leader(&pool, &store, &hot, &other, Duration::from_secs(9)).await;
    release.await.expect("release task");
    print_leader(&format!("lane held {}ms", hold.as_millis()), &report);
}
