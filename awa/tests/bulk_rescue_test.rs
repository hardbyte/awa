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
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const ABANDONED: i64 = 50_000;
const ABANDONED_CHUNK: i64 = 10_000;
/// Every tenth abandoned job has one attempt, so its rescue exhausts it.
const EXHAUSTED_EVERY: i64 = 10;
const ABANDONED_QUEUE: &str = "bulk_rescue_abandoned";
const LIVE_QUEUE: &str = "bulk_rescue_live";

/// The backlog tests each load the database heavily enough to slow the other's
/// heartbeat passes past their staleness window, so they run one at a time:
/// this lock serialises them under `cargo test`, and a nextest test group in
/// `.config/nextest.toml` does so across processes.
static BACKLOG_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// What each run of a job saw, by job id.
type RunsByJob<T> = HashMap<i64, Vec<T>>;
/// The `(job_id, run_lease)` of every attempt a handler ran.
type HandledAttempts = Arc<Mutex<HashSet<(i64, i64)>>>;

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

/// Retry promotion transfers a row between these states in one transaction.
/// Read both counts from one statement so a commit between reads cannot count
/// the same job once in deferred storage and again in ready storage.
///
/// The overview's `available` is an estimate once a lane holds more than
/// `AVAILABLE_LANE_EXACT_COUNT_CAP` rows; both counts are then read exactly
/// from the queue-storage tables, still in one statement.
async fn waiting_retry_counts(pool: &PgPool, queue: &str) -> (i64, i64) {
    let overview = awa::model::admin::queue_overview(pool, queue)
        .await
        .expect("retry accounting overview")
        .expect("retry accounting queue");
    if !overview.available_is_estimate {
        return (overview.retryable, overview.available);
    }
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
    let schema = store.schema();
    sqlx::query_as(&format!(
        r#"
        SELECT
            (
                SELECT count(*)::bigint
                FROM {schema}.deferred_jobs
                WHERE queue = $1 AND state = 'retryable'
            ),
            (
                SELECT count(*)::bigint
                FROM {schema}.ready_entries AS ready
                JOIN {schema}.queue_claim_heads AS claims
                  ON claims.queue = ready.queue
                 AND claims.priority = ready.priority
                 AND claims.enqueue_shard = ready.enqueue_shard
                WHERE ready.queue = $1
                  AND ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.ready_tombstones AS tomb
                      WHERE tomb.ready_slot = ready.ready_slot
                        AND tomb.ready_generation = ready.ready_generation
                        AND tomb.queue = ready.queue
                        AND tomb.priority = ready.priority
                        AND tomb.enqueue_shard = ready.enqueue_shard
                        AND tomb.lane_seq = ready.lane_seq
                  )
            )
        "#
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("exact retry accounting")
}

#[tokio::test]
async fn retry_accounting_uses_one_snapshot_during_promotion() {
    let db = BulkRescueDatabase::create().await;
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
    let queue = "bulk_rescue_retry_snapshot";
    let params = awa::model::insert::params_with(
        &AbandonedJob { n: 0 },
        InsertOpts {
            queue: queue.into(),
            max_attempts: 3,
            ..Default::default()
        },
    )
    .expect("retry accounting job");
    store
        .enqueue_params_batch(db.pool(), &[params])
        .await
        .expect("enqueue retry accounting job");
    let claims = store
        .claim_runtime_batch(db.pool(), queue, 1, Duration::ZERO)
        .await
        .expect("claim retry accounting job");
    assert_eq!(claims.len(), 1);
    store
        .retry_after(
            db.pool(),
            claims[0].job.id,
            claims[0].job.run_lease,
            Duration::ZERO,
            None,
        )
        .await
        .expect("retry accounting job")
        .expect("claimed job becomes retryable");
    assert_eq!(waiting_retry_counts(db.pool(), queue).await, (1, 0));

    // Hold the observer's only connection after its first statement. A real
    // promotion commits before any second observer statement can start. The
    // old split read therefore counts this one job twice; the overview's two
    // counts always describe the same statement snapshot.
    let armed = Arc::new(AtomicBool::new(false));
    let read_finished = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let observer = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(scaled_timeout(Duration::from_secs(10)))
        .after_release({
            let armed = armed.clone();
            let read_finished = read_finished.clone();
            let release = release.clone();
            move |_, _| {
                let gate = armed.swap(false, Ordering::SeqCst);
                let read_finished = read_finished.clone();
                let release = release.clone();
                Box::pin(async move {
                    if gate {
                        read_finished.notify_one();
                        release.notified().await;
                    }
                    Ok(true)
                })
            }
        })
        .connect(&db.url())
        .await
        .expect("retry accounting observer pool");
    let primed = observer.acquire().await.expect("prime observer connection");
    drop(primed);
    tokio::time::timeout(scaled_timeout(Duration::from_secs(10)), async {
        while observer.num_idle() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("observer's initial connection is idle before arming");
    armed.store(true, Ordering::SeqCst);
    let promote = async {
        let observed = tokio::time::timeout(
            scaled_timeout(Duration::from_secs(10)),
            read_finished.notified(),
        )
        .await;
        if observed.is_err() {
            release.notify_one();
            panic!("retry accounting observer did not finish its first statement");
        }
        let promoted = store
            .promote_due(db.pool(), awa::JobState::Retryable, 1)
            .await;
        release.notify_one();
        assert_eq!(promoted.expect("promote between accounting reads"), 1);
    };
    let (counts, ()) = tokio::time::timeout(scaled_timeout(Duration::from_secs(10)), async {
        tokio::join!(waiting_retry_counts(&observer, queue), promote)
    })
    .await
    .expect("bounded retry accounting interleaving");
    observer.close().await;
    assert_eq!(counts.0 + counts.1, 1, "one job is waiting to retry");
    assert_eq!(waiting_retry_counts(db.pool(), queue).await, (0, 1));
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

/// Record, as each compact claim is closed as rescued, how long it had gone
/// without a heartbeat. The trigger runs inside the closing statement, after
/// rescue holds the attempt's lock, so no later heartbeat can land.
async fn record_compact_rescue_heartbeats(pool: &PgPool) {
    sqlx::raw_sql(
        r#"
        CREATE TABLE public.rescued_claim_heartbeats (
            job_id bigint NOT NULL,
            queue text NOT NULL,
            unheartbeated_for interval NOT NULL
        );
        CREATE FUNCTION public.record_rescued_claim_heartbeat() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO public.rescued_claim_heartbeats
            SELECT items.job_id, batches.queue,
                   NEW.closed_at - COALESCE(attempt.heartbeat_at, batches.claimed_at)
            FROM awa.lease_claim_batches AS batches
            CROSS JOIN LATERAL unnest(batches.receipt_ids, batches.job_ids, batches.run_leases)
                AS items(receipt_id, job_id, run_lease)
            LEFT JOIN awa.attempt_state AS attempt
              ON attempt.job_id = items.job_id AND attempt.run_lease = items.run_lease
            WHERE batches.claim_slot = NEW.claim_slot
              AND items.receipt_id = ANY(NEW.receipt_ids);
            RETURN NEW;
        END
        $$;
        CREATE TRIGGER record_rescued_claim_heartbeat
            AFTER INSERT ON awa.lease_claim_closure_batches
            FOR EACH ROW WHEN (NEW.outcome = 'rescued')
            EXECUTE FUNCTION public.record_rescued_claim_heartbeat();
        "#,
    )
    .execute(pool)
    .await
    .expect("record rescued claim heartbeats");
}

/// Abandoned-queue claims rescued so far: how many had a heartbeat (or claim)
/// younger than `staleness` when closed, how many there were, and the shortest
/// time any of them had gone without a heartbeat.
async fn rescued_heartbeat_ages(pool: &PgPool, staleness: Duration) -> (i64, i64, String) {
    sqlx::query_as(
        "SELECT count(*) FILTER (WHERE unheartbeated_for < $2 * interval '1 millisecond'),
                count(*),
                COALESCE(min(unheartbeated_for)::text, 'n/a')
         FROM public.rescued_claim_heartbeats
         WHERE queue = $1",
    )
    .bind(ABANDONED_QUEUE)
    .bind(staleness.as_millis() as f64)
    .fetch_one(pool)
    .await
    .expect("rescued claim heartbeat ages")
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
        static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);
        let name = format!(
            "awa_bulk_rescue_{}_{}",
            std::process::id(),
            NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
        );
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

/// Installs the persona soak's completion witness: triggers that copy every
/// committed completion, receipt or materialized, into a plain table that
/// prune never touches.
async fn install_completion_witness(pool: &PgPool) {
    sqlx::raw_sql(&include_str!("persona_soak/gate_proof.sql").replace("__SCHEMA__", "awa"))
        .execute(pool)
        .await
        .expect("install completion witness");
}

/// Committed completions of `queue` as `(job_id, run_lease, attempt)`, one row
/// per completion written, so a repeated completion shows up twice.
async fn witnessed_completions(pool: &PgPool, queue: &str) -> Vec<(i64, i64, i16)> {
    sqlx::query_as(
        "SELECT job_id, run_lease, attempt FROM public.awa_soak_completed_inserts
         WHERE queue = $1 ORDER BY job_id, run_lease",
    )
    .bind(queue)
    .fetch_all(pool)
    .await
    .expect("witnessed completions")
}

/// Asserts the witnessed completions are exactly the attempts the handlers
/// returned from, once each and all on `attempt`, and returns their job ids.
fn assert_completed_exactly(
    witnessed: &[(i64, i64, i16)],
    returned: &HashSet<(i64, i64)>,
    attempt: i16,
) -> HashSet<i64> {
    let pairs: HashSet<(i64, i64)> = witnessed
        .iter()
        .map(|(id, lease, _)| (*id, *lease))
        .collect();
    let ids: HashSet<i64> = witnessed.iter().map(|(id, _, _)| *id).collect();
    assert_eq!(witnessed.len(), pairs.len(), "an attempt completed twice");
    assert_eq!(witnessed.len(), ids.len(), "a job completed more than once");
    assert_eq!(
        &pairs, returned,
        "committed completions differ from handler returns"
    );
    let other_attempts: Vec<_> = witnessed
        .iter()
        .filter(|(_, _, observed)| *observed != attempt)
        .take(5)
        .collect();
    assert!(
        other_attempts.is_empty(),
        "completed on an attempt other than {attempt}: {other_attempts:?}"
    );
    ids
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
    let _backlog_test = BACKLOG_TEST.lock().await;
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
    install_completion_witness(&pool).await;
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
    let live_attempts: HandledAttempts = Arc::default();
    // The live runtime leads maintenance, so it sees every rescue.
    let rescues: Arc<Mutex<RunsByJob<(&'static str, i16)>>> = Arc::default();
    let live = {
        let live_runs = live_runs.clone();
        let live_attempts = live_attempts.clone();
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
                live_attempts
                    .lock()
                    .expect("live attempts")
                    .insert((ctx.job.id, ctx.job.run_lease));
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
    record_compact_rescue_heartbeats(&pool).await;
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
    let (fresh, rescued, freshest) = rescued_heartbeat_ages(&pool, staleness).await;
    assert_eq!(
        fresh, 0,
        "rescue closed {fresh} claims last heartbeated within the {staleness:?} staleness window"
    );
    assert_eq!(
        rescued_rows(&pool, ABANDONED_QUEUE).await,
        0,
        "rescue took a claim while its runtime was alive: {rescued} claims had gone {freshest} \
         or longer without a heartbeat, beyond the {staleness:?} staleness, so the live \
         runtime's heartbeat passes fell behind"
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
    let (retryable, promoted) = waiting_retry_counts(&pool, ABANDONED_QUEUE).await;
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
    let recovered_attempts: HandledAttempts = Arc::default();
    let recovery_pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&db.url())
        .await
        .expect("recovery pool");
    let recovery = {
        let recovered = recovered.clone();
        let recovered_attempts = recovered_attempts.clone();
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
                recovered_attempts
                    .lock()
                    .expect("recovered attempts")
                    .insert((ctx.job.id, ctx.job.run_lease));
                async { Ok(JobResult::Completed) }
            })
            .build()
            .expect("recovery client")
    };
    recovery.start().await.expect("start recovery client");
    // Completed rows leave the ring once queue prune wraps past them, so the
    // handler counts the recovery runs.
    wait_until(
        scaled_timeout(Duration::from_secs(120)),
        "every retried job to run",
        || async { recovered.lock().expect("recovered").len() as i64 >= ABANDONED - exhausted },
    )
    .await;
    wait_until(
        scaled_timeout(Duration::from_secs(30)),
        "the retried jobs to finish",
        || async {
            running(&pool, &store, ABANDONED_QUEUE).await == 0
                && waiting_retry_counts(&pool, ABANDONED_QUEUE).await == (0, 0)
                && count(
                    &pool,
                    format!(
                        "SELECT count(*) FROM public.awa_soak_completed_inserts
                         WHERE queue = '{ABANDONED_QUEUE}'"
                    ),
                )
                .await
                    >= ABANDONED - exhausted
        },
    )
    .await;
    recovery.shutdown(Duration::from_secs(10)).await;
    {
        let recovered = recovered.lock().expect("recovered");
        assert_eq!(recovered.len() as i64, ABANDONED - exhausted);
        let unexpected: Vec<_> = recovered
            .iter()
            .filter(|(_, runs)| runs.as_slice() != [(2, true)])
            .take(5)
            .collect();
        assert!(
            unexpected.is_empty(),
            "each retried job runs exactly once on attempt 2, with the rescue recorded: {unexpected:?}"
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
    {
        let rescues = rescues.lock().expect("rescues");
        assert_eq!(rescues.len() as i64, ABANDONED);
        let unexpected = rescued_twice(&rescues);
        assert!(
            unexpected.is_empty(),
            "neither live nor recovered attempts may be rescued: {unexpected:?}"
        );
    }

    // Prune has had time to drop many of these completions, so the witness,
    // not the queue tables, shows which attempts committed.
    let recovered_ids = assert_completed_exactly(
        &witnessed_completions(&pool, ABANDONED_QUEUE).await,
        &recovered_attempts.lock().expect("recovered attempts"),
        2,
    );
    let live_ids = assert_completed_exactly(
        &witnessed_completions(&pool, LIVE_QUEUE).await,
        &live_attempts.lock().expect("live attempts"),
        1,
    );
    assert_eq!(
        live_ids.len(),
        produced,
        "every live job committed its completion"
    );
    let dead_lettered: HashSet<i64> = sqlx::query_scalar(
        "SELECT job_id FROM awa.dlq_entries WHERE queue = $1 AND dlq_reason = 'heartbeat_stale'",
    )
    .bind(ABANDONED_QUEUE)
    .fetch_all(&pool)
    .await
    .expect("dead-lettered abandoned jobs")
    .into_iter()
    .collect();
    assert!(
        dead_lettered.is_disjoint(&recovered_ids),
        "a job was both dead-lettered and completed"
    );
    let abandoned_outcomes: HashSet<i64> = dead_lettered.union(&recovered_ids).copied().collect();
    assert_eq!(
        abandoned_outcomes,
        rescues
            .lock()
            .expect("rescues")
            .keys()
            .copied()
            .collect::<HashSet<_>>(),
        "every rescued job ends exactly once, dead-lettered or completed"
    );
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

const ROW_LOCAL_QUEUE: &str = "bulk_rescue_row_local";

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().map(|value| {
        value
            .parse()
            .unwrap_or_else(|err| panic!("{name}={value}: {err}"))
    })
}

async fn ring_generation(pool: &PgPool, ring: &str) -> i64 {
    count(
        pool,
        format!("SELECT generation FROM awa.{ring}_ring_state WHERE singleton"),
    )
    .await
}

/// First-attempt claims still open: a dead runtime's attempts that no rescue
/// has closed. A pruned claim slot takes its closed claims with it.
async fn open_first_attempt_claims(pool: &PgPool) -> i64 {
    count(
        pool,
        format!(
            "SELECT count(*) FROM awa.lease_claims
             WHERE queue = '{ROW_LOCAL_QUEUE}' AND run_lease = 1 AND closed_at IS NULL"
        ),
    )
    .await
}

/// A killed runtime left hundreds of thousands of row-local claims: claims
/// with a deadline, so each has its own `lease_claims` row rather than a
/// compact batch. They span two claim slots, all reference one queue slot,
/// were last heartbeated hours ago and are past their deadlines. Until they
/// close, queue prune skips that queue slot and claim prune skips both claim
/// slots, so neither ring can wrap. With default rescue intervals, rescue
/// must close every claim and both rings must move past the pinned slots.
///
/// `AWA_ROW_LOCAL_RESCUE_CLAIMS` sets the backlog (default 20,000);
/// `AWA_ROW_LOCAL_RESCUE_PASS_BUDGET_MS` and `AWA_ROW_LOCAL_RESCUE_PASS_MAX_ROWS`
/// override the pass controls, and `AWA_ROW_LOCAL_RESCUE_OBSERVE_SECS` reports
/// the drain rate after that long instead of requiring a full drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rescue_drains_abandoned_row_local_claims_and_unpins_both_rings() {
    let _backlog_test = BACKLOG_TEST.lock().await;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let abandoned = env_u64("AWA_ROW_LOCAL_RESCUE_CLAIMS").unwrap_or(20_000) as i64;
    let observe_for = env_u64("AWA_ROW_LOCAL_RESCUE_OBSERVE_SECS").map(Duration::from_secs);

    let db = BulkRescueDatabase::create().await;
    let pool = db.pool().clone();
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
    let queue_slots = store.queue_slot_count() as i64;
    let claim_slots = store.claim_slot_count() as i64;

    let setup_started = Instant::now();
    for first in (0..abandoned).step_by(5_000) {
        let params: Vec<_> = (first..(first + 5_000).min(abandoned))
            .map(|n| {
                awa::model::insert::params_with(
                    &AbandonedJob { n },
                    InsertOpts {
                        queue: ROW_LOCAL_QUEUE.into(),
                        ..Default::default()
                    },
                )
                .expect("abandoned params")
            })
            .collect();
        store
            .enqueue_params_batch(&pool, &params)
            .await
            .expect("enqueue abandoned jobs");
    }
    // Half the backlog in each of two claim slots, as a runtime that kept
    // claiming across a claim rotation leaves it.
    let mut claimed = 0;
    let mut rotated = false;
    while claimed < abandoned {
        if !rotated && claimed >= abandoned / 2 {
            store.rotate_claims(&pool).await.expect("rotate claims");
            rotated = true;
        }
        let batch = store
            .claim_runtime_batch(&pool, ROW_LOCAL_QUEUE, 5_000, Duration::from_secs(300))
            .await
            .expect("claim abandoned jobs");
        assert!(!batch.is_empty(), "the backlog should still be claimable");
        claimed += batch.len() as i64;
    }
    // The runtime died hours ago: its claims, deadlines and last heartbeats
    // all predate the staleness window, and its snapshot stopped then too.
    sqlx::raw_sql(&format!(
        "UPDATE awa.lease_claims
            SET claimed_at = claimed_at - interval '20 hours',
                deadline_at = deadline_at - interval '20 hours'
          WHERE queue = '{ROW_LOCAL_QUEUE}';
         INSERT INTO awa.attempt_state (job_id, run_lease, heartbeat_at, updated_at)
         SELECT job_id, run_lease, claimed_at + interval '1 minute', claimed_at + interval '1 minute'
           FROM awa.lease_claims WHERE queue = '{ROW_LOCAL_QUEUE}'
         ON CONFLICT (job_id, run_lease) DO UPDATE SET heartbeat_at = EXCLUDED.heartbeat_at;
         INSERT INTO awa.runtime_instances (
             instance_id, pid, version, started_at, last_seen_at, snapshot_interval_ms,
             healthy, postgres_connected, poll_loop_alive, heartbeat_alive,
             maintenance_alive, shutting_down, leader, storage_capability, transition_role
         ) VALUES (
             gen_random_uuid(), 1, 'killed', now() - interval '21 hours',
             now() - interval '19 hours', 10000, true, true, true, true, true, false, false,
             'queue_storage', 'queue_storage_target'
         );
         ANALYZE awa.ready_entries;
         ANALYZE awa.lease_claims;
         ANALYZE awa.attempt_state;"
    ))
    .execute(&pool)
    .await
    .expect("age the abandoned claims");
    let shape: (i64, i64, i64, i64) = sqlx::query_as(&format!(
        "SELECT count(*), count(DISTINCT claim_slot), count(DISTINCT ready_slot),
                (SELECT count(*) FROM awa.lease_claim_batches)
           FROM awa.lease_claims WHERE queue = '{ROW_LOCAL_QUEUE}'"
    ))
    .fetch_one(&pool)
    .await
    .expect("backlog shape");
    assert_eq!(
        shape,
        (abandoned, 2, 1, 0),
        "row-local claims over two claim slots and one queue slot"
    );
    let queue_generation = ring_generation(&pool, "queue").await;
    let claim_generation = ring_generation(&pool, "claim").await;
    eprintln!(
        "{abandoned} abandoned row-local claims built in {:?}",
        setup_started.elapsed()
    );

    // Completed rows leave the ring once queue prune wraps past them, so the
    // handler counts the recovery runs.
    let second_attempts = Arc::new(AtomicU64::new(0));
    let other_attempts = Arc::new(AtomicU64::new(0));
    let mut builder = Client::builder(pool.clone())
        .queue_storage(
            QueueStorageConfig::default(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .claim_rotate_interval(Duration::from_secs(1))
        .queue(
            ROW_LOCAL_QUEUE,
            QueueConfig {
                max_workers: 200,
                ..QueueConfig::default()
            },
        )
        .register::<AbandonedJob, _, _>({
            let second_attempts = second_attempts.clone();
            let other_attempts = other_attempts.clone();
            move |_, ctx| {
                let runs = if ctx.job.attempt == 2 {
                    &second_attempts
                } else {
                    &other_attempts
                };
                runs.fetch_add(1, Ordering::SeqCst);
                async { Ok(JobResult::Completed) }
            }
        });
    if let Some(budget) = env_u64("AWA_ROW_LOCAL_RESCUE_PASS_BUDGET_MS") {
        builder = builder.rescue_pass_budget(Duration::from_millis(budget));
    }
    if let Some(max_rows) = env_u64("AWA_ROW_LOCAL_RESCUE_PASS_MAX_ROWS") {
        builder = builder.rescue_pass_max_rows(max_rows as usize);
    }
    let client = builder.build().expect("client");
    let started = Instant::now();
    client.start().await.expect("start client");

    let mut first_rescue = None;
    let mut drained = None;
    loop {
        let open = open_first_attempt_claims(&pool).await;
        if open < abandoned && first_rescue.is_none() {
            first_rescue = Some(started.elapsed());
        }
        if open == 0 {
            drained = Some(started.elapsed());
            break;
        }
        if let Some(observe_for) = observe_for {
            if started.elapsed() >= observe_for {
                let rescued = abandoned - open;
                let since_first = started.elapsed() - first_rescue.unwrap_or_default();
                eprintln!(
                    "observed {rescued} of {abandoned} rescued in {:?} ({:?} after the first \
                     rescue): {:.1} claims/s",
                    started.elapsed(),
                    since_first,
                    rescued as f64 / since_first.as_secs_f64().max(f64::EPSILON)
                );
                break;
            }
        }
        assert!(
            started.elapsed() < scaled_timeout(Duration::from_secs(600 + abandoned as u64 / 200)),
            "{open} of {abandoned} abandoned claims are still open"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    if let Some(drained) = drained {
        let first_rescue = first_rescue.unwrap_or_default();
        eprintln!(
            "first rescue {first_rescue:?} after start; all {abandoned} closed {drained:?} after \
             start: {:.1} claims/s from the first rescue",
            abandoned as f64 / (drained - first_rescue).as_secs_f64().max(f64::EPSILON)
        );
        let unpinned = wait_until(
            scaled_timeout(Duration::from_secs(120 + abandoned as u64 / 10_000)),
            "both rings to rotate past the slots the backlog pinned",
            || async {
                ring_generation(&pool, "queue").await >= queue_generation + queue_slots
                    && ring_generation(&pool, "claim").await >= claim_generation + claim_slots
            },
        )
        .await;
        eprintln!(
            "queue and claim rings rotated past the pinned slots {unpinned:?} after the drain"
        );
        wait_until(
            scaled_timeout(Duration::from_secs(300 + abandoned as u64 / 500)),
            "every rescued job to run on its second attempt",
            || async { second_attempts.load(Ordering::SeqCst) >= abandoned as u64 },
        )
        .await;
        assert_eq!(second_attempts.load(Ordering::SeqCst), abandoned as u64);
        assert_eq!(
            other_attempts.load(Ordering::SeqCst),
            0,
            "only the abandoned first attempts are rescued"
        );
    }
    client.shutdown(Duration::from_secs(10)).await;
}
