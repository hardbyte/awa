//! Opt-in finite-work rescue and completion diagnostics.
//! Logged completion triggers retain committed attempt identities after pruning.
//! These probes add SQL, WAL and relation locks to disposable test databases.

#[path = "ci_timing.rs"]
mod ci_timing;

use awa::model::{migrations, QueueStorage, QueueStorageConfig};
use awa::{Client, InsertOpts, JobArgs, JobEvent, JobResult, QueueConfig};
use ci_timing::{scaled_staleness, scaled_timeout};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const ABANDONED: i64 = 50_000;
const ABANDONED_CHUNK: i64 = 10_000;
/// Every tenth abandoned job has one attempt, so its rescue exhausts it.
const EXHAUSTED_EVERY: i64 = 10;
const ABANDONED_QUEUE: &str = "bulk_rescue_abandoned";
const LIVE_QUEUE: &str = "bulk_rescue_live";

/// What each run of a job saw, by job id.
type RunsByJob<T> = HashMap<i64, Vec<T>>;

const AUDIT_SETUP_SQL: &str = r###"-- Disposable diagnostic database only.
-- No product schema/migration change. All DDL is before workload_start.
-- Keep original worker lifecycle subscriptions and needs_event behavior.
BEGIN;

CREATE TABLE public.awa_drain_completion_audit (
    job_id BIGINT NOT NULL,
    run_lease BIGINT NOT NULL,
    attempt SMALLINT NOT NULL,
    queue TEXT NOT NULL,
    PRIMARY KEY (job_id, run_lease)
) USING heap WITH (
    autovacuum_enabled = false,
    toast.autovacuum_enabled = false
);
-- CREATE TABLE (not UNLOGGED/TEMP) gives a logged, independent heap.
-- autovacuum_enabled=false disables regular vacuum AND ANALYZE for this
-- new probe heap; its TOAST relation has the same explicit setting.
-- Anti-wraparound vacuum is a PostgreSQL exception, not claimed impossible.

CREATE FUNCTION public.awa_drain_audit_done_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $body$
BEGIN
    INSERT INTO public.awa_drain_completion_audit (
        job_id, run_lease, attempt, queue
    ) VALUES (
        NEW.job_id, NEW.run_lease, NEW.attempt, NEW.queue
    )
    ON CONFLICT (job_id, run_lease) DO NOTHING;
    RETURN NEW;
END;
$body$;

CREATE FUNCTION public.awa_drain_audit_receipt_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $body$
BEGIN
    INSERT INTO public.awa_drain_completion_audit (
        job_id, run_lease, attempt, queue
    )
    SELECT item.job_id, item.run_lease, item.attempt, NEW.queue
    FROM unnest(NEW.job_ids, NEW.run_leases, NEW.attempts)
        AS item(job_id, run_lease, attempt)
    ON CONFLICT (job_id, run_lease) DO NOTHING;
    RETURN NEW;
END;
$body$;

-- Parent row triggers clone to the existing leaf partitions. All runtime
-- connections in this fixture are postgres; no new runtime grants/role needed.
CREATE TRIGGER awa_drain_audit_done_insert
AFTER INSERT ON awa.done_entries
FOR EACH ROW
WHEN (
    NEW.state = 'completed'::awa.job_state
    AND NEW.queue IN ('bulk_rescue_abandoned', 'bulk_rescue_live')
)
EXECUTE FUNCTION public.awa_drain_audit_done_insert();

-- receipt_completion_batches is exclusively successful completion history.
-- Existing cardinality constraints align all three arrays; the audit table's
-- NOT NULL/PK constraints reject malformed logical identities rather than
-- silently granting durable-work proof. No errors are swallowed.
CREATE TRIGGER awa_drain_audit_receipt_insert
AFTER INSERT ON awa.receipt_completion_batches
FOR EACH ROW
WHEN (NEW.queue IN ('bulk_rescue_abandoned', 'bulk_rescue_live'))
EXECUTE FUNCTION public.awa_drain_audit_receipt_insert();

COMMIT;
-- Pre-window validation must prove relpersistence='p', expected clone coverage,
-- reloptions on heap/TOAST, empty audit and original worker/harness hashes.
-- Never TRUNCATE/DELETE/ANALYZE this probe during the diagnostic window.
"###;

// This logged probe is diagnostic instrumentation: heap/index writes, WAL,
// relation modes and observer reads add cost. It is not a zero-overhead oracle.
const AUDIT_TABLE: &str = "public.awa_drain_completion_audit";
type AuditRow = (i64, i64, i16);
type AttemptPairs = Arc<Mutex<HashSet<(i64, i64)>>>;

fn record_phase(started: Instant, phase: &str, fields: serde_json::Value) {
    let mut record = fields.as_object().expect("phase fields object").clone();
    record.insert("phase".into(), serde_json::json!(phase));
    record.insert(
        "unix_ms".into(),
        serde_json::json!(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after Unix epoch")
            .as_millis()),
    );
    record.insert(
        "elapsed_ms".into(),
        serde_json::json!(started.elapsed().as_millis()),
    );
    eprintln!("LOCK_PRESSURE_DRAIN {}", serde_json::Value::Object(record));
}

async fn validate_completion_audit(pool: &PgPool) {
    let settings: (String, bool, bool) = sqlx::query_as(
        "SELECT probe.relpersistence::text,
                COALESCE('autovacuum_enabled=false' = ANY(probe.reloptions), false),
                COALESCE('autovacuum_enabled=false' = ANY(toast.reloptions), false)
         FROM pg_class probe JOIN pg_class toast ON toast.oid = probe.reltoastrelid
         WHERE probe.oid = 'public.awa_drain_completion_audit'::regclass",
    )
    .fetch_one(pool)
    .await
    .expect("audit persistence/autovacuum settings");
    assert_eq!(settings, ("p".into(), true, true));
    for (parent, trigger) in [
        ("awa.done_entries", "awa_drain_audit_done_insert"),
        (
            "awa.receipt_completion_batches",
            "awa_drain_audit_receipt_insert",
        ),
    ] {
        let clones: Vec<(String, bool)> = sqlx::query_as(
            "SELECT tree.relid::regclass::text,
                    EXISTS (SELECT 1 FROM pg_trigger AS t
                            WHERE t.tgrelid = tree.relid AND t.tgname = $2
                              AND t.tgtype = 5 AND t.tgenabled = 'O')
             FROM pg_partition_tree($1::regclass) tree ORDER BY tree.relid",
        )
        .bind(parent)
        .bind(trigger)
        .fetch_all(pool)
        .await
        .expect("parent and partition audit triggers");
        assert!(
            clones.len() > 1,
            "partitioned completion representation {parent}"
        );
        assert!(
            clones.iter().all(|(_, enabled)| *enabled),
            "missing audit trigger: {clones:?}"
        );
    }
    assert_eq!(
        count(pool, format!("SELECT count(*) FROM {AUDIT_TABLE}")).await,
        0
    );
}

async fn audit_rows(pool: &PgPool, queue: &str) -> Vec<AuditRow> {
    sqlx::query_as(
        "SELECT job_id, run_lease, attempt FROM public.awa_drain_completion_audit
         WHERE queue = $1 ORDER BY job_id, run_lease",
    )
    .bind(queue)
    .fetch_all(pool)
    .await
    .expect("committed completion audit rows")
}

fn assert_audited_attempts(
    rows: &[AuditRow],
    expected: &HashSet<(i64, i64)>,
    attempt: i16,
) -> HashSet<i64> {
    let pairs: HashSet<_> = rows.iter().map(|(id, lease, _)| (*id, *lease)).collect();
    let ids: HashSet<_> = rows.iter().map(|(id, _, _)| *id).collect();
    assert_eq!(rows.len(), pairs.len(), "audit duplicated an attempt");
    assert_eq!(
        rows.len(),
        ids.len(),
        "job completed on more than one run lease"
    );
    assert_eq!(
        &pairs, expected,
        "committed audit attempts differ from handlers"
    );
    let unexpected: Vec<_> = rows
        .iter()
        .filter(|(_, _, observed)| *observed != attempt)
        .take(5)
        .collect();
    assert!(
        unexpected.is_empty(),
        "unexpected completion attempt: {unexpected:?}"
    );
    ids
}

// An audit read sees only committed rows. One deadline covers the handler
// precursor, cheap count observations and the final exact identity read.
async fn wait_for_recovered_audit(
    pool: &PgPool,
    recovered: &Arc<Mutex<RunsByJob<(i16, bool)>>>,
    expected: usize,
    started: Instant,
    diagnostic_started: Instant,
) -> (Vec<AuditRow>, &'static str) {
    let deadline =
        tokio::time::Instant::from_std(started + scaled_timeout(Duration::from_secs(600)));
    let old_threshold = started + scaled_timeout(Duration::from_secs(120));
    let mut old_verdict = None;
    loop {
        let handlers = recovered.lock().expect("recovered").len();
        if handlers < expected {
            if Instant::now() >= old_threshold && old_verdict.is_none() {
                old_verdict = Some("missed");
                record_phase(
                    diagnostic_started,
                    "old_threshold",
                    serde_json::json!({
                        "verdict": "missed", "evidence": "handlers_incomplete_after_threshold",
                        "handler_ids": handlers, "expected": expected, "base_threshold_s": 120
                    }),
                );
            }
        } else {
            let before = Instant::now();
            let observation_deadline =
                deadline.min(tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(5)));
            let read = tokio::time::timeout_at(
                observation_deadline,
                sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM public.awa_drain_completion_audit WHERE queue = $1",
                )
                .bind(ABANDONED_QUEUE)
                .fetch_one(pool),
            )
            .await;
            let after = Instant::now();
            let completed = match read {
                Ok(Ok(value)) => value as usize,
                failure => {
                    if old_verdict.is_none() {
                        record_phase(
                            diagnostic_started,
                            "old_threshold",
                            serde_json::json!({
                                "verdict": "unknown", "evidence": "audit_observation_failed",
                                "failure": format!("{failure:?}"), "base_threshold_s": 120
                            }),
                        );
                    }
                    panic!("INCONCLUSIVE: completion audit observation failed: {failure:?}");
                }
            };
            assert!(
                completed <= expected,
                "extra committed recovery attempts: {completed} > {expected}"
            );
            if old_verdict.is_none() && (completed == expected || before >= old_threshold) {
                let verdict = if completed == expected && after <= old_threshold {
                    "met"
                } else if completed < expected && before >= old_threshold {
                    "missed"
                } else {
                    "unknown"
                };
                old_verdict = Some(verdict);
                record_phase(
                    diagnostic_started,
                    "old_threshold",
                    serde_json::json!({
                        "verdict": verdict, "evidence": "committed_audit_snapshot",
                        "completed": completed, "expected": expected,
                        "request_ms_after_recovery_start": before.duration_since(started).as_millis(),
                        "response_ms_after_recovery_start": after.duration_since(started).as_millis(),
                        "base_threshold_s": 120
                    }),
                );
            }
            if completed == expected {
                let rows = tokio::time::timeout_at(deadline, audit_rows(pool, ABANDONED_QUEUE))
                    .await
                    .expect("INCONCLUSIVE: final audit identity read exceeded recovery deadline");
                return (rows, old_verdict.expect("old threshold recorded"));
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "INCONCLUSIVE: durable completion proof missing after600s (not proof of lost finalization)");
        tokio::time::sleep_until(
            deadline.min(tokio::time::Instant::now() + Duration::from_millis(250)),
        )
        .await;
    }
}

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
async fn waiting_retry_counts(pool: &PgPool, queue: &str) -> (i64, i64) {
    let overview = awa::model::admin::queue_overview(pool, queue)
        .await
        .expect("retry accounting overview")
        .expect("retry accounting queue");
    (overview.retryable, overview.available)
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
        Self::create_with_completion_audit(true).await
    }

    async fn create_with_completion_audit(audit: bool) -> Self {
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
        if audit {
            sqlx::raw_sql(AUDIT_SETUP_SQL)
                .execute(&pool)
                .await
                .expect("install diagnostic completion audit");
            validate_completion_audit(&pool).await;
        }
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
#[ignore = "opt-in diagnostic; creates disposable databases"]
async fn diagnostic_finite_work_drain_completed() {
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
    let observe_raw_terminal = match std::env::var("AWA_DRAIN_RAW_TERMINAL_OBSERVATION").as_deref()
    {
        Ok("1") => true,
        Err(std::env::VarError::NotPresent) | Ok("0") => false,
        value => panic!("invalid optional raw terminal observation switch: {value:?}"),
    };
    let diagnostic_started = Instant::now();
    eprintln!("LOCK_PRESSURE_PHASE workload_start db={}", db.name);
    record_phase(
        diagnostic_started,
        "workload_start",
        serde_json::json!({
            "database": db.name, "audit_setup_complete": true, "audit_enabled": true,
            "raw_terminal_observation_enabled": observe_raw_terminal
        }),
    );
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
    let live_pairs: AttemptPairs = Arc::default();
    // The live runtime leads maintenance, so it sees every rescue.
    let rescues: Arc<Mutex<RunsByJob<(&'static str, i16)>>> = Arc::default();
    let live = {
        let live_runs = live_runs.clone();
        let live_pairs = live_pairs.clone();
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
                live_pairs
                    .lock()
                    .expect("live attempt pairs")
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
                let inserted = store
                    .enqueue_params_batch(&pool, &params)
                    .await
                    .expect("enqueue live jobs");
                assert_eq!(
                    inserted,
                    params.len(),
                    "all produced live jobs were inserted"
                );
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
    eprintln!("LOCK_PRESSURE_PHASE owner_kill");
    doomed.kill();
    record_phase(
        diagnostic_started,
        "owner_killed",
        serde_json::json!({"abandoned": ABANDONED}),
    );
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
        staleness * 2 + scaled_timeout(Duration::from_secs(900)),
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
    record_phase(
        diagnostic_started,
        "rescued",
        serde_json::json!({
            "abandoned": ABANDONED, "dlq": exhausted, "retryable": retryable,
            "available": promoted, "rescue_drain_ms": drained_in.as_millis()
        }),
    );
    meter_provider.force_flush().expect("flush metrics");
    let largest_pass = largest_heartbeat_rescue_pass(&exporter);
    eprintln!("lock-pressure benchmark largest heartbeat rescue pass: {largest_pass}");

    // Live claims can hold a slot for a whole live job.
    let rotated_in = wait_until(
        live_job_duration * 2 + scaled_timeout(Duration::from_secs(30)),
        "the claim ring to rotate through every slot after the drain",
        || async { claim_ring_generation(&pool).await >= generation_drained + claim_slots },
    )
    .await;
    eprintln!("claim ring rotated a full revolution {rotated_in:?} after the drain");
    record_phase(
        diagnostic_started,
        "revolution",
        serde_json::json!({"rotation_ms": rotated_in.as_millis()}),
    );

    // A fresh runtime runs every retried job exactly once, on its second attempt.
    let recovered: Arc<Mutex<RunsByJob<(i16, bool)>>> = Arc::default();
    let recovered_pairs: AttemptPairs = Arc::default();
    let recovery_pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&db.url())
        .await
        .expect("recovery pool");
    let recovery = {
        let recovered = recovered.clone();
        let recovered_pairs = recovered_pairs.clone();
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
                recovered_pairs
                    .lock()
                    .expect("recovery attempt pairs")
                    .insert((ctx.job.id, ctx.job.run_lease));
                async { Ok(JobResult::Completed) }
            })
            .build()
            .expect("recovery client")
    };
    recovery.start().await.expect("start recovery client");
    let recovery_started = Instant::now();
    record_phase(
        diagnostic_started,
        "recovery_start",
        serde_json::json!({
            "expected": ABANDONED - exhausted, "base_budget_s": 600,
            "scaled_budget_ms": scaled_timeout(Duration::from_secs(600)).as_millis(),
            "original_base_threshold_s": 120
        }),
    );
    let recovery_deadline =
        tokio::time::Instant::from_std(recovery_started + scaled_timeout(Duration::from_secs(600)));
    let (recovered_audit, old_threshold) = tokio::time::timeout_at(
        recovery_deadline,
        wait_for_recovered_audit(
            &pool,
            &recovered,
            (ABANDONED - exhausted) as usize,
            recovery_started,
            diagnostic_started,
        ),
    )
    .await
    .expect("INCONCLUSIVE:600s durable recovery observation budget exhausted");
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

    let recovered_expected_pairs = recovered_pairs
        .lock()
        .expect("recovery attempt pairs")
        .clone();
    let recovered_ids = assert_audited_attempts(&recovered_audit, &recovered_expected_pairs, 2);
    assert_eq!(recovered_ids.len() as i64, ABANDONED - exhausted);
    eprintln!("LOCK_PRESSURE_PHASE recovered");
    record_phase(
        diagnostic_started,
        "recovered",
        serde_json::json!({
            "completed_unique": recovered_ids.len(), "recovery_ms": recovery_started.elapsed().as_millis(),
            "old_120s_threshold": old_threshold
        }),
    );
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
    let live_expected_pairs = live_pairs.lock().expect("live attempt pairs").clone();
    let live_audit = tokio::time::timeout(
        scaled_timeout(Duration::from_secs(5)),
        audit_rows(&pool, LIVE_QUEUE),
    )
    .await
    .expect("INCONCLUSIVE: live final audit read timed out");
    let live_completed_ids = assert_audited_attempts(&live_audit, &live_expected_pairs, 1);
    assert_eq!(
        live_completed_ids.len(),
        produced,
        "every produced live job committed completion"
    );
    let dlq_rows: Vec<(i64, i64, i16)> = tokio::time::timeout(
        scaled_timeout(Duration::from_secs(5)),
        sqlx::query_as(
            "SELECT job_id, run_lease, attempt FROM awa.dlq_entries
             WHERE queue = $1 AND dlq_reason = 'heartbeat_stale' AND attempt = 1 ORDER BY job_id",
        )
        .bind(ABANDONED_QUEUE)
        .fetch_all(&pool),
    )
    .await
    .expect("INCONCLUSIVE: final DLQ identity read timed out")
    .expect("final DLQ identities");
    let dlq_ids: HashSet<_> = dlq_rows.iter().map(|(id, _, _)| *id).collect();
    assert_eq!(dlq_rows.len(), exhausted as usize);
    assert_eq!(
        dlq_ids.len(),
        exhausted as usize,
        "DLQ logical IDs are unique"
    );
    assert!(
        dlq_ids.is_disjoint(&recovered_ids),
        "DLQ and recovered outputs overlap"
    );
    let abandoned_outputs: HashSet<_> = dlq_ids.union(&recovered_ids).copied().collect();
    assert_eq!(abandoned_outputs.len() as i64, ABANDONED);
    assert!(
        abandoned_outputs.is_disjoint(&live_completed_ids),
        "live and abandoned IDs overlap"
    );

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
    {
        let rescues = rescues.lock().expect("rescues");
        assert_eq!(
            abandoned_outputs,
            rescues.keys().copied().collect::<HashSet<_>>(),
            "every abandoned ID has exactly one final outcome"
        );
        let live_runs = live_runs.lock().expect("live runs");
        assert_eq!(
            live_completed_ids,
            live_runs.keys().copied().collect::<HashSet<_>>(),
            "live committed IDs exactly match handlers"
        );
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
        record_phase(
            diagnostic_started,
            "final_durable_work",
            serde_json::json!({
                "abandoned_unique": ABANDONED, "dlq_unique": dlq_ids.len(),
                "recovery_completed_unique": recovered_ids.len(), "live_produced_unique": produced,
                "live_completed_unique": live_completed_ids.len(),
                "successful_unique_job_denominator": ABANDONED as usize + produced,
                "retry_is_new_logical_job": false, "old_120s_threshold": old_threshold,
                "all_correctness_assertions_passed": true, "instrumented_diagnostic": true
            }),
        );
    }
    // This optional observation is prunable and excluded from the finite-work
    // native counter window. It never determines durable completion success.
    if observe_raw_terminal {
        let raw_sql = format!("SELECT count(*) FROM awa.terminal_jobs WHERE queue = '{ABANDONED_QUEUE}' AND state = 'completed'");
        let raw = tokio::time::timeout(
            scaled_timeout(Duration::from_secs(120)),
            sqlx::query_scalar::<_, i64>(&raw_sql).fetch_one(&pool),
        )
        .await;
        eprintln!(
            "LOCK_PRESSURE_DRAIN_RAW_TERMINAL {}",
            serde_json::json!({
                "observation": format!("{raw:?}"), "durable_oracle": false,
                "outside_finite_work_window": true
            })
        );
    }
}

// Independent private databases; never seed/reset the measured workload DB.
async fn audit_preflight_claim(
    db: &BulkRescueDatabase,
    tagged: bool,
    jobs: usize,
) -> Vec<awa::model::ClaimedRuntimeJob> {
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("store");
    let params: Vec<_> = (0..jobs)
        .map(|n| {
            awa::model::insert::params_with(
                &AbandonedJob { n: n as i64 },
                InsertOpts {
                    queue: ABANDONED_QUEUE.into(),
                    tags: if tagged {
                        vec!["audit_slow".into()]
                    } else {
                        Vec::new()
                    },
                    ..Default::default()
                },
            )
            .expect("preflight params")
        })
        .collect();
    assert_eq!(
        store
            .enqueue_params_batch(db.pool(), &params)
            .await
            .expect("enqueue"),
        jobs
    );
    let claims = store
        .claim_runtime_batch(db.pool(), ABANDONED_QUEUE, jobs as i64, Duration::ZERO)
        .await
        .expect("preflight claim");
    assert_eq!(claims.len(), jobs);
    assert!(
        claims.iter().all(|claim| claim.claim.lease_claim_receipt),
        "receipt completion preflight"
    );
    assert!(claims
        .iter()
        .all(|claim| claim.job.tags.is_empty() != tagged));
    claims
}

#[tokio::test]
#[ignore = "opt-in diagnostic; creates disposable databases"]
async fn diagnostic_audit_fast_slow_commit_and_prune() {
    let db = BulkRescueDatabase::create().await;
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("store");
    let mut expected = HashSet::new();
    for tagged in [false, true] {
        let claims = audit_preflight_claim(&db, tagged, 1).await;
        let pair = (claims[0].job.id, claims[0].job.run_lease);
        assert_eq!(
            store
                .complete_runtime_batch(db.pool(), &claims)
                .await
                .expect("commit completion"),
            vec![pair]
        );
        expected.insert(pair);
        assert!(
            store
                .complete_runtime_batch(db.pool(), &claims)
                .await
                .expect("same-attempt API repeat")
                .is_empty(),
            "already committed attempt is stale"
        );
        assert_eq!(
            audit_rows(db.pool(), ABANDONED_QUEUE).await.len(),
            expected.len(),
            "same-attempt API repeat adds no audit evidence"
        );
    }
    let before = audit_rows(db.pool(), ABANDONED_QUEUE).await;
    assert_audited_attempts(&before, &expected, 1);
    assert_eq!(
        count(
            db.pool(),
            "SELECT count(*) FROM awa.receipt_completion_batches".into()
        )
        .await,
        1,
        "fast representation was written"
    );
    assert_eq!(
        count(
            db.pool(),
            "SELECT count(*) FROM awa.done_entries WHERE state='completed'".into()
        )
        .await,
        1,
        "slow representation was written"
    );
    assert!(matches!(
        store.rotate(db.pool()).await.expect("seal slot"),
        awa::model::RotateOutcome::Rotated { .. }
    ));
    let deadline = Instant::now() + scaled_timeout(Duration::from_secs(5));
    loop {
        match store
            .prune_oldest(db.pool(), Duration::ZERO)
            .await
            .expect("real terminal prune")
        {
            awa::model::PruneOutcome::Pruned { slot: 0, .. } => break,
            awa::model::PruneOutcome::Blocked { slot: 0 } if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            other => panic!("unexpected preflight prune outcome: {other:?}"),
        }
    }
    assert_eq!(
        count(
            db.pool(),
            "SELECT count(*) FROM awa.receipt_completion_batches".into()
        )
        .await,
        0
    );
    assert_eq!(
        count(db.pool(), "SELECT count(*) FROM awa.done_entries".into()).await,
        0
    );
    assert_eq!(
        audit_rows(db.pool(), ABANDONED_QUEUE).await,
        before,
        "audit commit proof survives real terminal prune"
    );
}

#[tokio::test]
#[ignore = "opt-in diagnostic; creates disposable databases"]
async fn diagnostic_audit_rollback_is_not_visible() {
    let db = BulkRescueDatabase::create().await;
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("store");
    let claims = audit_preflight_claim(&db, true, 1).await;
    let pair = (claims[0].job.id, claims[0].job.run_lease);
    let mut tx = db.pool().begin().await.expect("preflight transaction");
    assert_eq!(
        store
            .complete_runtime_batch_slow_in_tx(&mut tx, &claims)
            .await
            .expect("pending completion"),
        vec![pair]
    );
    let inside: i64 = sqlx::query_scalar("SELECT count(*) FROM public.awa_drain_completion_audit")
        .fetch_one(tx.as_mut())
        .await
        .expect("own pending audit row");
    assert_eq!(inside, 1);
    assert!(
        audit_rows(db.pool(), ABANDONED_QUEUE).await.is_empty(),
        "uncommitted audit is invisible"
    );
    tx.rollback().await.expect("rollback completion");
    assert!(audit_rows(db.pool(), ABANDONED_QUEUE).await.is_empty());
    assert_eq!(
        count(db.pool(), "SELECT count(*) FROM awa.done_entries".into()).await,
        0
    );
    assert_eq!(
        running(db.pool(), &store, ABANDONED_QUEUE).await,
        1,
        "rolled-back attempt remains running"
    );
}

#[tokio::test]
#[ignore = "opt-in diagnostic; creates disposable databases"]
async fn diagnostic_audit_native_completion_cost() {
    // The existing native backend collector, not pg_locks after COMMIT,
    // measures peak relation modes for these isolated named connections.
    // Missing collector records remain missing footprint evidence.
    for audit in [false, true] {
        for tagged in [false, true] {
            let db = BulkRescueDatabase::create_with_completion_audit(audit).await;
            let store = QueueStorage::new(QueueStorageConfig::default()).expect("store");
            let claims = audit_preflight_claim(&db, tagged, 10).await;
            let app = format!(
                "awa_read_footprint_{}_{}",
                if tagged { "slow" } else { "fast" },
                if audit { "audit" } else { "plain" }
            );
            let options = db
                .url()
                .parse::<PgConnectOptions>()
                .expect("preflight connect options")
                .application_name(&app);
            let measured = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await
                .expect("measured preflight pool");
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&measured)
                .await
                .expect("measured backend");
            let before: String = sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::text")
                .fetch_one(db.pool())
                .await
                .expect("WAL before");
            assert_eq!(
                store
                    .complete_runtime_batch(&measured, &claims)
                    .await
                    .expect("measured completion")
                    .len(),
                10
            );
            let wal: i64 = sqlx::query_scalar(
                "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::pg_lsn)::bigint",
            )
            .bind(&before)
            .fetch_one(db.pool())
            .await
            .expect("WAL after");
            measured.close().await;
            if audit {
                assert_eq!(audit_rows(db.pool(), ABANDONED_QUEUE).await.len(), 10);
            }
            eprintln!(
                "LOCK_PRESSURE_DRAIN_PREFLIGHT {}",
                serde_json::json!({
                    "application_name": app, "pid": pid, "audit_enabled": audit,
                    "representation": if tagged { "slow_done" } else { "fast_receipt" },
                    "jobs": 10, "wal_bytes_global_interval": wal,
                    "native_peak_modes": "requires_matching_external_collector_record",
                    "isolated_control_required": true, "zero_overhead_claim": false
                })
            );
        }
    }
}
