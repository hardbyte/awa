//! A runtime holding tens of thousands of receipt claims dies without a
//! graceful shutdown. Maintenance must rescue the whole backlog in bounded
//! time, the claim ring must resume rotating, a runtime that stays alive
//! must never have a claim rescued, and every rescued attempt must count
//! (#627).

#[path = "ci_timing.rs"]
mod ci_timing;

use awa::model::{migrations, QueueStorage, QueueStorageConfig};
use awa::{Client, InsertOpts, JobArgs, JobEvent, JobResult, QueueConfig};
use ci_timing::{scaled_staleness, scaled_timeout};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const ABANDONED: i64 = 50_000;
const ABANDONED_CHUNK: i64 = 10_000;
/// Every tenth abandoned job has one attempt, so its rescue exhausts it.
const EXHAUSTED_EVERY: i64 = 10;
const ABANDONED_QUEUE: &str = "bulk_rescue_abandoned";
const LIVE_QUEUE: &str = "bulk_rescue_live";

/// What each run of a job saw, by job id.
type RunsByJob<T> = HashMap<i64, Vec<T>>;

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct AbandonedJob {
    n: i64,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct LiveJob {
    n: i64,
    hold_ms: u64,
}

fn queue_storage_builder(
    pool: &PgPool,
    staleness: Duration,
    heartbeat_interval: Duration,
) -> awa::ClientBuilder {
    Client::builder(pool.clone())
        .queue_storage(
            QueueStorageConfig::default(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        // Rotate the claim ring often so the abandoned claims spread across
        // slots and the ring wraps onto the oldest of them.
        .claim_rotate_interval(Duration::from_millis(200))
        .heartbeat_interval(heartbeat_interval)
        .heartbeat_staleness(staleness)
        .runtime_snapshot_interval(Duration::from_millis(250))
        .leader_check_interval(Duration::from_millis(200))
        .promote_interval(Duration::from_millis(100))
        .dlq_enabled_by_default(true)
}

async fn count(pool: &PgPool, sql: String) -> i64 {
    sqlx::query_scalar(&sql)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

async fn running(pool: &PgPool, store: &QueueStorage, queue: &str) -> i64 {
    store
        .queue_counts(pool, queue)
        .await
        .expect("queue counts")
        .running
}

async fn claim_ring_generation(pool: &PgPool) -> i64 {
    count(
        pool,
        "SELECT generation FROM awa.claim_ring_state WHERE singleton".into(),
    )
    .await
}

async fn abandoned_claim_slots(pool: &PgPool) -> i64 {
    count(
        pool,
        format!(
            "SELECT count(DISTINCT claim_slot) FROM (
                 SELECT claim_slot FROM awa.lease_claim_batches WHERE queue = '{ABANDONED_QUEUE}'
                 UNION ALL
                 SELECT claim_slot FROM awa.lease_claims WHERE queue = '{ABANDONED_QUEUE}'
             ) AS claims"
        ),
    )
    .await
}

/// Rescued and dead-lettered rows for a queue: what a false rescue would leave.
async fn rescued_rows(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM awa.deferred_jobs WHERE queue = $1 AND state = 'retryable')
              + (SELECT count(*) FROM awa.dlq_entries WHERE queue = $1)",
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("count rescued rows")
}

async fn wait_until<F, Fut>(timeout: Duration, what: &str, mut done: F) -> Duration
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    loop {
        if done().await {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < timeout,
            "timed out after {timeout:?} waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn largest_heartbeat_rescue_pass(exporter: &InMemoryMetricExporter) -> u64 {
    let mut largest = 0;
    for resource in exporter.get_finished_metrics().expect("read metrics") {
        for scope in resource.scope_metrics() {
            for metric in scope.metrics() {
                if metric.name() != "awa.maintenance.rescue.pass_size" {
                    continue;
                }
                if let AggregatedMetrics::U64(MetricData::Histogram(histogram)) = metric.data() {
                    for point in histogram.data_points() {
                        let heartbeat = point.attributes().any(|attribute| {
                            attribute.key.as_str() == "awa.rescue.kind"
                                && attribute.value.as_str() == "heartbeat"
                        });
                        if heartbeat {
                            largest = largest.max(point.max().unwrap_or(0));
                        }
                    }
                }
            }
        }
    }
    largest
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn with_database(url: &str, database: &str) -> String {
    let (base, _) = url
        .rsplit_once('/')
        .expect("database URL has a database name");
    format!("{base}/{database}")
}

/// A database of its own with queue storage active, dropped afterwards.
struct BulkRescueDatabase {
    name: String,
    pool: PgPool,
}

impl BulkRescueDatabase {
    async fn create() -> Self {
        let name = format!("awa_bulk_rescue_{}", std::process::id());
        let mut admin = PgConnection::connect(&with_database(&database_url(), "postgres"))
            .await
            .expect("admin connection");
        sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
            .execute(&mut admin)
            .await
            .expect("drop leftover database");
        sqlx::raw_sql(&format!("CREATE DATABASE {name}"))
            .execute(&mut admin)
            .await
            .expect("create database");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&with_database(&database_url(), &name))
            .await
            .expect("test pool");
        migrations::run(&pool).await.expect("migrate");
        QueueStorage::new(QueueStorageConfig::default())
            .expect("queue storage")
            .install(&pool)
            .await
            .expect("install queue storage");
        awa_testing::setup::activate_queue_storage(&pool).await;
        Self { name, pool }
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn url(&self) -> String {
        with_database(&database_url(), &self.name)
    }
}

impl Drop for BulkRescueDatabase {
    fn drop(&mut self) {
        let name = self.name.clone();
        let admin_url = with_database(&database_url(), "postgres");
        let cleanup = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime");
            runtime.block_on(async {
                if let Ok(mut admin) = PgConnection::connect(&admin_url).await {
                    let _ = sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                        .execute(&mut admin)
                        .await;
                }
            });
        });
        let _ = cleanup.join();
    }
}

/// A client on its own Tokio runtime, so it can be killed the way SIGKILL
/// would: every task is dropped mid-flight, no shutdown runs, and its
/// connections close with their transactions rolled back.
struct DoomedClient {
    kill: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl DoomedClient {
    fn start(url: String, staleness: Duration, heartbeat_interval: Duration) -> Self {
        let (kill, killed) = tokio::sync::oneshot::channel::<()>();
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .expect("doomed client runtime");
            let client = runtime.block_on(async {
                let pool = PgPoolOptions::new()
                    .max_connections(20)
                    .connect(&url)
                    .await
                    .expect("doomed client pool");
                let client = queue_storage_builder(&pool, staleness, heartbeat_interval)
                    .queue(
                        ABANDONED_QUEUE,
                        QueueConfig {
                            max_workers: ABANDONED as u32 + 1_000,
                            poll_interval: Duration::from_millis(10),
                            deadline_duration: Duration::ZERO,
                            claimers: 4,
                            claim_batch_size: 1_000,
                            ..QueueConfig::default()
                        },
                    )
                    .register::<AbandonedJob, _, _>(|_, _| async {
                        std::future::pending::<Result<JobResult, awa::JobError>>().await
                    })
                    .build()
                    .expect("doomed client");
                client.start().await.expect("start doomed client");
                client
            });
            started_tx.send(()).expect("report doomed client start");
            runtime.block_on(async {
                let _ = killed.await;
            });
            runtime.shutdown_background();
            drop(client);
        });
        started_rx.recv().expect("doomed client started");
        Self {
            kill: Some(kill),
            thread: Some(thread),
        }
    }

    fn kill(mut self) {
        if let Some(kill) = self.kill.take() {
            let _ = kill.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread.join().expect("doomed client thread");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rescue_drains_an_abandoned_backlog_without_rescuing_live_claims() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let db = BulkRescueDatabase::create().await;
    let pool = db.pool().clone();
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
    let claim_slots = store.claim_slot_count() as i64;

    // Once the doomed runtime has claimed its backlog, live jobs run for
    // three staleness windows, so a rescue that ignored their heartbeats
    // would take them. Until then they finish at once, so their claims do
    // not hold up the claim ring while the doomed runtime fills it.
    let staleness = scaled_staleness(Duration::from_secs(6));
    let heartbeat_interval = staleness / 8;
    let live_job_duration = staleness * 3;

    let live_pool = PgPoolOptions::new()
        .max_connections(30)
        .connect(&db.url())
        .await
        .expect("live pool");
    let live_runs: Arc<Mutex<RunsByJob<i16>>> = Arc::default();
    // The live runtime leads maintenance, so it sees every rescue.
    let rescues: Arc<Mutex<RunsByJob<(&'static str, i16)>>> = Arc::default();
    let live = {
        let live_runs = live_runs.clone();
        let live_rescues = rescues.clone();
        let abandoned_rescues = rescues.clone();
        queue_storage_builder(&live_pool, staleness, heartbeat_interval)
            // Under half the staleness, so the leader samples runtime
            // snapshots often enough to confirm the dead owner.
            .heartbeat_rescue_interval(Duration::from_secs(2))
            .deadline_rescue_interval(Duration::from_secs(2))
            .leader_election_interval(Duration::from_millis(100))
            .queue(
                LIVE_QUEUE,
                QueueConfig {
                    max_workers: 1_000,
                    poll_interval: Duration::from_millis(25),
                    deadline_duration: Duration::ZERO,
                    ..QueueConfig::default()
                },
            )
            .on_event::<LiveJob, _, _>(move |event| {
                if let JobEvent::Rescued { job, .. } = event {
                    live_rescues
                        .lock()
                        .expect("rescues")
                        .entry(job.id)
                        .or_default()
                        .push((LIVE_QUEUE, job.attempt));
                }
                async {}
            })
            .on_event::<AbandonedJob, _, _>(move |event| {
                if let JobEvent::Rescued { job, .. } = event {
                    abandoned_rescues
                        .lock()
                        .expect("rescues")
                        .entry(job.id)
                        .or_default()
                        .push((ABANDONED_QUEUE, job.attempt));
                }
                async {}
            })
            .register::<LiveJob, _, _>(move |job, ctx| {
                live_runs
                    .lock()
                    .expect("live runs")
                    .entry(ctx.job.id)
                    .or_default()
                    .push(ctx.job.attempt);
                async move {
                    tokio::time::sleep(Duration::from_millis(job.hold_ms)).await;
                    Ok(JobResult::Completed)
                }
            })
            .build()
            .expect("live client")
    };
    live.start().await.expect("start live client");
    wait_until(
        scaled_timeout(Duration::from_secs(10)),
        "live leader",
        || async { live.health_check().await.leader },
    )
    .await;

    // The live runtime keeps claiming new work for the whole test.
    let producing = Arc::new(AtomicBool::new(true));
    let live_hold_ms = Arc::new(AtomicU64::new(0));
    let producer = {
        let pool = pool.clone();
        let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
        let producing = producing.clone();
        let live_hold_ms = live_hold_ms.clone();
        tokio::spawn(async move {
            let mut n = 0;
            let mut produced = 0usize;
            while producing.load(Ordering::SeqCst) {
                let params: Vec<_> = (0..5)
                    .map(|i| {
                        awa::model::insert::params_with(
                            &LiveJob {
                                n: n + i,
                                hold_ms: live_hold_ms.load(Ordering::SeqCst),
                            },
                            InsertOpts {
                                queue: LIVE_QUEUE.into(),
                                ..Default::default()
                            },
                        )
                        .expect("live params")
                    })
                    .collect();
                store
                    .enqueue_params_batch(&pool, &params)
                    .await
                    .expect("enqueue live jobs");
                n += 5;
                produced += params.len();
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            produced
        })
    };

    // The doomed runtime claims the backlog in chunks, each after the claim
    // ring has rotated, so the abandoned claims span several slots.
    let generation_before = claim_ring_generation(&pool).await;
    let doomed = DoomedClient::start(db.url(), staleness, heartbeat_interval);
    let claim_started = Instant::now();
    for chunk_start in (0..ABANDONED).step_by(ABANDONED_CHUNK as usize) {
        let chunk_end = (chunk_start + ABANDONED_CHUNK).min(ABANDONED);
        let params: Vec<_> = (chunk_start..chunk_end)
            .map(|n| {
                awa::model::insert::params_with(
                    &AbandonedJob { n },
                    InsertOpts {
                        queue: ABANDONED_QUEUE.into(),
                        max_attempts: if n % EXHAUSTED_EVERY == 0 { 1 } else { 3 },
                        ..Default::default()
                    },
                )
                .expect("abandoned params")
            })
            .collect();
        let generation = claim_ring_generation(&pool).await;
        for batch in params.chunks(5_000) {
            store
                .enqueue_params_batch(&pool, batch)
                .await
                .expect("enqueue abandoned jobs");
        }
        wait_until(
            scaled_timeout(Duration::from_secs(60)),
            "the doomed runtime to claim the chunk",
            || async { running(&pool, &store, ABANDONED_QUEUE).await >= chunk_end },
        )
        .await;
        // Give the ring a chance to move on before the next chunk; once it
        // wraps onto a slot of abandoned claims it stays there.
        let rotate_deadline = Instant::now() + scaled_timeout(Duration::from_secs(3));
        while claim_ring_generation(&pool).await <= generation && Instant::now() < rotate_deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let claimed_in = claim_started.elapsed();
    assert_eq!(
        rescued_rows(&pool, ABANDONED_QUEUE).await,
        0,
        "rescue took a claim while its runtime was alive and heartbeating"
    );
    let generation_at_kill = claim_ring_generation(&pool).await;
    live_hold_ms.store(live_job_duration.as_millis() as u64, Ordering::SeqCst);
    doomed.kill();
    let slots = abandoned_claim_slots(&pool).await;
    assert!(
        slots >= 2,
        "the abandoned claims should span several claim slots: {slots} \
         (ring generation {generation_before} -> {generation_at_kill})"
    );

    // Before the drain: the claims become stale one staleness after the
    // kill, and the owner evidence needs one more staleness window. After
    // that the backlog must drain at the multi-batch rate, not 500 claims
    // per heartbeat-rescue interval.
    let drained_in = wait_until(
        staleness * 2 + scaled_timeout(Duration::from_secs(90)),
        "rescue to drain the abandoned claims",
        || async { running(&pool, &store, ABANDONED_QUEUE).await == 0 },
    )
    .await;
    let generation_drained = claim_ring_generation(&pool).await;
    eprintln!(
        "claimed {ABANDONED} in {claimed_in:?}; drained {drained_in:?} after the kill \
         (claims stale after {staleness:?})"
    );

    wait_until(
        scaled_timeout(Duration::from_secs(10)),
        "a rescue event for every abandoned job",
        || async { rescues.lock().expect("rescues").len() as i64 >= ABANDONED },
    )
    .await;
    type Rescued = (&'static str, i16);
    let rescued_twice = |rescues: &RunsByJob<Rescued>| -> Vec<(i64, Vec<Rescued>)> {
        rescues
            .iter()
            .filter(|(_, rescued)| rescued.as_slice() != [(ABANDONED_QUEUE, 1)])
            .take(5)
            .map(|(id, rescued)| (*id, rescued.clone()))
            .collect()
    };
    {
        let rescues = rescues.lock().expect("rescues");
        assert_eq!(rescues.len() as i64, ABANDONED);
        let unexpected = rescued_twice(&rescues);
        assert!(
            unexpected.is_empty(),
            "each abandoned job is rescued once and nothing else is: {unexpected:?}"
        );
    }

    let exhausted = ABANDONED / EXHAUSTED_EVERY;
    assert_eq!(
        count(
            &pool,
            format!(
                "SELECT count(*) FROM awa.dlq_entries
                 WHERE queue = '{ABANDONED_QUEUE}' AND dlq_reason = 'heartbeat_stale' AND attempt = 1"
            ),
        )
        .await,
        exhausted,
        "every one-attempt job is dead-lettered by its rescue"
    );
    let retryable = count(
        &pool,
        format!(
            "SELECT count(*) FROM awa.deferred_jobs
             WHERE queue = '{ABANDONED_QUEUE}' AND state = 'retryable'"
        ),
    )
    .await;
    let promoted = store
        .queue_counts(&pool, ABANDONED_QUEUE)
        .await
        .expect("abandoned queue counts")
        .available;
    assert_eq!(
        retryable + promoted,
        ABANDONED - exhausted,
        "every other abandoned job is waiting to retry"
    );
    meter_provider.force_flush().expect("flush metrics");
    let largest_pass = largest_heartbeat_rescue_pass(&exporter);
    assert!(
        largest_pass > 500,
        "a stale-heartbeat pass should take more than one 500-claim batch once the \
         owner is confirmed gone; the largest took {largest_pass}"
    );

    // Live claims can hold a slot for a whole live job.
    let rotated_in = wait_until(
        live_job_duration * 2 + scaled_timeout(Duration::from_secs(30)),
        "the claim ring to rotate through every slot after the drain",
        || async { claim_ring_generation(&pool).await >= generation_drained + claim_slots },
    )
    .await;
    eprintln!("claim ring rotated a full revolution {rotated_in:?} after the drain");

    // A fresh runtime runs every retried job exactly once, on its second attempt.
    let recovered: Arc<Mutex<RunsByJob<(i16, bool)>>> = Arc::default();
    let recovery_pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&db.url())
        .await
        .expect("recovery pool");
    let recovery = {
        let recovered = recovered.clone();
        queue_storage_builder(&recovery_pool, staleness, heartbeat_interval)
            .queue(
                ABANDONED_QUEUE,
                // Small claims, so finished attempts never wait on a long
                // completion backlog while the recovery runtime is under test.
                QueueConfig {
                    max_workers: 100,
                    poll_interval: Duration::from_millis(10),
                    deadline_duration: Duration::ZERO,
                    claim_batch_size: 100,
                    ..QueueConfig::default()
                },
            )
            .register::<AbandonedJob, _, _>(move |_, ctx| {
                let rescue_recorded = ctx
                    .job
                    .errors
                    .as_ref()
                    .and_then(|errors| errors.last())
                    .and_then(|error| error.get("error"))
                    .and_then(|error| error.as_str())
                    .is_some_and(|error| error.contains("presumed dead"));
                recovered
                    .lock()
                    .expect("recovered")
                    .entry(ctx.job.id)
                    .or_default()
                    .push((ctx.job.attempt, rescue_recorded));
                async { Ok(JobResult::Completed) }
            })
            .build()
            .expect("recovery client")
    };
    recovery.start().await.expect("start recovery client");
    wait_until(
        scaled_timeout(Duration::from_secs(120)),
        "the retried jobs to complete",
        || async { recovered.lock().expect("recovered").len() as i64 >= ABANDONED - exhausted },
    )
    .await;
    recovery.shutdown(Duration::from_secs(10)).await;
    {
        let recovered = recovered.lock().expect("recovered");
        assert_eq!(recovered.len() as i64, ABANDONED - exhausted);
        let unexpected: Vec<_> = recovered
            .iter()
            .filter(|(_, runs)| runs.first() != Some(&(2, true)))
            .take(5)
            .collect();
        assert!(
            unexpected.is_empty(),
            "each retried job first runs on attempt 2, with the rescue recorded: {unexpected:?}"
        );
    }

    producing.store(false, Ordering::SeqCst);
    let produced = producer.await.expect("producer");
    wait_until(
        live_job_duration + scaled_timeout(Duration::from_secs(30)),
        "the live jobs to finish",
        || async {
            running(&pool, &store, LIVE_QUEUE).await == 0
                && store
                    .queue_counts(&pool, LIVE_QUEUE)
                    .await
                    .expect("live counts")
                    .available
                    == 0
        },
    )
    .await;
    live.shutdown(Duration::from_secs(10)).await;

    assert_eq!(
        rescued_rows(&pool, LIVE_QUEUE).await,
        0,
        "rescue took a claim from the live runtime"
    );
    // On this line a retried job's finished attempt can still be rescued
    // from the recovery runtime, as it was before bounded rescue, so only
    // the live runtime's claims are checked here.
    {
        let rescues = rescues.lock().expect("rescues");
        let live_rescued: Vec<_> = rescues
            .iter()
            .filter(|(_, rescued)| rescued.iter().any(|(queue, _)| *queue == LIVE_QUEUE))
            .take(5)
            .collect();
        assert!(
            live_rescued.is_empty(),
            "no claim of the live runtime was rescued: {live_rescued:?}"
        );
    }
    let live_runs = live_runs.lock().expect("live runs");
    assert_eq!(live_runs.len(), produced, "every live job ran");
    let rerun: Vec<_> = live_runs
        .iter()
        .filter(|(_, attempts)| attempts.as_slice() != [1])
        .take(5)
        .collect();
    assert!(
        rerun.is_empty(),
        "a live job ran more than once or on a later attempt: {rerun:?}"
    );
    eprintln!(
        "live runtime ran {} jobs of {live_job_duration:?} each with no rescue",
        live_runs.len()
    );
}
