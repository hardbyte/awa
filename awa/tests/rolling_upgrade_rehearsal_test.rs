//! Released-artifact rolling-upgrade rehearsal (#427).
//!
//! This first cell exercises migrate-first operation from the latest released
//! 0.6 patch to the current v043 schema while traffic remains live. The old
//! worker is a PyPI wheel installed by the workflow, not a source checkout.

use async_trait::async_trait;
use awa::model::{
    admin,
    cron::{pause_cron_job, trigger_cron_job, upsert_cron_job, PeriodicJob},
    dlq, insert_with, migrations, storage, InsertOpts, JobState, QueueStorage, QueueStorageConfig,
};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashSet;
use std::env;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use uuid::Uuid;

static REHEARSAL_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Machine-readable evidence for one rehearsal cell (#427): phase
/// transitions, census watermarks, flip status, and the final
/// reconciliation, written as JSON when `AWA_REHEARSAL_ARTIFACT_DIR` is
/// set (each cell writes `<dir>/<cell>.json`). The release gate uploads
/// the directory as a workflow artifact, so a failed pre-tag run leaves
/// inspectable evidence rather than only scrollback. Without the env var
/// this is a no-op, so local `cargo test` runs are unaffected.
struct RehearsalReport {
    cell: &'static str,
    started: Instant,
    phases: Vec<serde_json::Value>,
    facts: serde_json::Map<String, serde_json::Value>,
}

impl RehearsalReport {
    fn new(cell: &'static str) -> Self {
        Self {
            cell,
            started: Instant::now(),
            phases: Vec::new(),
            facts: serde_json::Map::new(),
        }
    }

    /// Record a phase transition with elapsed-ms watermark.
    fn phase(&mut self, name: &str) {
        eprintln!("[rehearsal:{}] phase: {name}", self.cell);
        self.phases.push(serde_json::json!({
            "name": name,
            "elapsed_ms": self.started.elapsed().as_millis() as u64,
        }));
    }

    /// Record a named fact (census count, flip outcome, version…).
    fn fact(&mut self, key: &str, value: impl Into<serde_json::Value>) {
        self.facts.insert(key.to_string(), value.into());
    }

    /// Write the cell report. Called on the success path only — a panic
    /// means no report file, which the gate treats as failure evidence in
    /// combination with the captured test output.
    fn finish(mut self) {
        self.phase("complete");
        let Some(dir) = env::var_os("AWA_REHEARSAL_ARTIFACT_DIR") else {
            return;
        };
        let report = serde_json::json!({
            "cell": self.cell,
            "result": "pass",
            "current_version": env!("CARGO_PKG_VERSION"),
            "git_sha": option_env!("GITHUB_SHA"),
            "released_python": env::var("AWA_N_MINUS_ONE_PYTHON").ok(),
            "duration_ms": self.started.elapsed().as_millis() as u64,
            "phases": self.phases,
            "facts": serde_json::Value::Object(self.facts),
        });
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).expect("create rehearsal artifact dir");
        let path = dir.join(format!("{}.json", self.cell));
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&report).expect("serialize report"),
        )
        .expect("write rehearsal report");
        eprintln!(
            "[rehearsal:{}] report written to {}",
            self.cell,
            path.display()
        );
    }
}

fn database_url() -> String {
    env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:5432/awa_test".to_string())
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("manifest dir has parent")
        .to_path_buf()
}

fn released_python() -> PathBuf {
    let path = env::var_os("AWA_N_MINUS_ONE_PYTHON")
        .map(PathBuf::from)
        .expect("AWA_N_MINUS_ONE_PYTHON must name the released-artifact interpreter");
    if path.is_absolute() {
        path
    } else {
        workspace_root().join(path)
    }
}

fn mixed_fleet_helper() -> PathBuf {
    workspace_root().join("awa-python/tests/mixed_fleet_helper.py")
}

async fn pool() -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(30)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url())
        .await
        .expect("connect to upgrade rehearsal database")
}

async fn reset_database(pool: &sqlx::PgPool) {
    sqlx::query("DROP SCHEMA IF EXISTS awa CASCADE")
        .execute(pool)
        .await
        .expect("drop awa schema");
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct SimpleChaosJob {
    seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct UpgradeWorkloadJob {
    seq: i64,
    mode: String,
}

/// Runs every designed workload outcome. Each mode maps to exactly one
/// expected terminal `(state, attempt)` pair, asserted at reconciliation.
#[derive(Clone, Default)]
struct WorkloadWorker {
    snoozed: Arc<Mutex<HashSet<i64>>>,
}

#[async_trait]
impl Worker for WorkloadWorker {
    fn kind(&self) -> &'static str {
        "upgrade_workload_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: UpgradeWorkloadJob = serde_json::from_value(ctx.job.args.clone())
            .map_err(|err| JobError::terminal(format!("failed to decode workload args: {err}")))?;
        match args.mode.as_str() {
            "complete" => Ok(JobResult::Completed),
            "retry_once" => {
                if ctx.job.attempt == 1 {
                    Ok(JobResult::RetryAfter(Duration::from_millis(100)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "fail_terminal" => Err(JobError::terminal("designed terminal failure")),
            // Exhaustion must use the error path: `max_attempts` bounds
            // handler failures, and only failures move a job to the DLQ.
            "exhaust_retries" => Err(JobError::retryable_msg("designed retryable failure")),
            "snooze_once" => {
                if self.snoozed.lock().unwrap().insert(args.seq) {
                    Ok(JobResult::Snooze(Duration::from_millis(100)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "cancel_self" => Ok(JobResult::Cancel("designed handler cancellation".into())),
            "callback_then_complete" => {
                if ctx.job.attempt == 1 {
                    let callback = ctx
                        .register_callback(Duration::from_millis(150))
                        .await
                        .map_err(JobError::retryable)?;
                    Ok(JobResult::WaitForCallback(callback))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "multi_second" => {
                tokio::time::sleep(Duration::from_secs(2)).await;
                Ok(JobResult::Completed)
            }
            other => Err(JobError::terminal(format!(
                "unknown workload mode: {other}"
            ))),
        }
    }
}

#[derive(Clone)]
struct RecordingWorker {
    completed: Arc<Mutex<HashSet<i64>>>,
    delay: Duration,
}

#[async_trait]
impl Worker for RecordingWorker {
    fn kind(&self) -> &'static str {
        "simple_chaos_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let seq = ctx
            .job
            .args
            .get("seq")
            .and_then(serde_json::Value::as_i64)
            .expect("simple_chaos_job args must contain seq");
        tokio::time::sleep(self.delay).await;
        self.completed.lock().unwrap().insert(seq);
        Ok(JobResult::Completed)
    }
}

struct ReleasedWorker {
    child: Child,
    started: Arc<Mutex<HashSet<i64>>>,
    completed: Arc<Mutex<HashSet<i64>>>,
    _stdout_reader: tokio::task::JoinHandle<()>,
}

impl ReleasedWorker {
    fn completed(&self) -> HashSet<i64> {
        self.completed.lock().unwrap().clone()
    }

    fn started(&self) -> HashSet<i64> {
        self.started.lock().unwrap().clone()
    }

    async fn shutdown(&mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

async fn spawn_released_worker(queue: &str) -> ReleasedWorker {
    spawn_released_worker_with(queue, 80, None).await
}

async fn spawn_released_worker_with(
    queue: &str,
    sleep_ms: u64,
    deadline_duration_ms: Option<u64>,
) -> ReleasedWorker {
    let python = released_python();
    assert!(
        python.exists(),
        "released-artifact interpreter not found at {}",
        python.display()
    );

    let mut command = Command::new(python);
    command
        .arg(mixed_fleet_helper())
        .env("DATABASE_URL", database_url())
        .env("MIXED_QUEUE", queue)
        .env("MIXED_MODE", "worker_simple_chaos_job")
        .env("MIXED_SIMPLE_SLEEP_MS", sleep_ms.to_string())
        .env("MIXED_LEADER_ELECTION_INTERVAL_MS", "100")
        .env("MIXED_QS_SCHEMA", "awa")
        .env("MIXED_TRANSITION_ROLE", "auto")
        .env("PYTHONUNBUFFERED", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    if let Some(deadline_duration_ms) = deadline_duration_ms {
        command.env(
            "MIXED_DEADLINE_DURATION_MS",
            deadline_duration_ms.to_string(),
        );
    }

    let mut child = command.spawn().expect("spawn released 0.6 worker");
    let stdout = child.stdout.take().expect("released worker stdout");
    let started = Arc::new(Mutex::new(HashSet::new()));
    let started_reader = started.clone();
    let completed = Arc::new(Mutex::new(HashSet::new()));
    let completed_reader = completed.clone();
    let (ready_tx, ready_rx) = oneshot::channel();
    let reader = tokio::spawn(async move {
        let mut ready_tx = Some(ready_tx);
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("[released-0.6] {line}");
            if line.starts_with("READY") {
                if let Some(tx) = ready_tx.take() {
                    let _ = tx.send(());
                }
            }
            if line.starts_with("START") {
                if let Some(seq) = line
                    .split("seq=")
                    .nth(1)
                    .and_then(|value| value.trim().parse::<i64>().ok())
                {
                    started_reader.lock().unwrap().insert(seq);
                }
            }
            if line.starts_with("COMPLETE") {
                if let Some(seq) = line
                    .split("seq=")
                    .nth(1)
                    .and_then(|value| value.trim().parse::<i64>().ok())
                {
                    completed_reader.lock().unwrap().insert(seq);
                }
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(30), ready_rx)
        .await
        .expect("released worker did not become ready within 30s")
        .expect("released worker exited before READY");

    ReleasedWorker {
        child,
        started,
        completed,
        _stdout_reader: reader,
    }
}

async fn wait_for_released_start(worker: &ReleasedWorker, seq: i64, timeout: Duration) {
    let start = Instant::now();
    loop {
        if worker.started().contains(&seq) {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "released worker did not start job seq={seq}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_expired_compact_claim(pool: &sqlx::PgPool, job_id: i64, timeout: Duration) {
    let start = Instant::now();
    loop {
        let expired: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM awa.lease_claim_batches AS batches
                CROSS JOIN LATERAL unnest(batches.job_ids) AS items(job_id)
                WHERE items.job_id = $1
                  AND batches.deadline_at < clock_timestamp()
            )
            "#,
        )
        .bind(job_id)
        .fetch_one(pool)
        .await
        .expect("inspect compact deadline claim");
        if expired {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "released worker did not create an expired compact claim"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn assert_released_worker_is_fenced(queue: &str) {
    let output = tokio::time::timeout(Duration::from_secs(20), async {
        Command::new(released_python())
            .arg(mixed_fleet_helper())
            .env("DATABASE_URL", database_url())
            .env("MIXED_QUEUE", queue)
            .env("MIXED_MODE", "worker_simple_chaos_job")
            .env("MIXED_QS_SCHEMA", "awa")
            .env("MIXED_TRANSITION_ROLE", "auto")
            .env("PYTHONUNBUFFERED", "1")
            .kill_on_drop(true)
            .output()
            .await
            .expect("run released worker after flip")
    })
    .await
    .expect("released worker stayed alive after ledger-authority flip");

    assert!(
        !output.status.success(),
        "released worker unexpectedly started after ledger-authority flip"
    );
    let refusal = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        refusal.contains("schema") || refusal.contains("partition"),
        "released worker must fail loudly after flip; output:\n{refusal}"
    );
}

fn current_client(pool: sqlx::PgPool, queue: &str, completed: Arc<Mutex<HashSet<i64>>>) -> Client {
    current_client_with_heartbeat_rescue(pool, queue, completed, Duration::from_millis(100))
}

fn current_client_with_heartbeat_rescue(
    pool: sqlx::PgPool,
    queue: &str,
    completed: Arc<Mutex<HashSet<i64>>>,
    heartbeat_rescue_interval: Duration,
) -> Client {
    Client::builder(pool)
        .queue(
            queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(20),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: "awa".to_string(),
                ..Default::default()
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .heartbeat_interval(Duration::from_millis(50))
        .promote_interval(Duration::from_millis(50))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        // Both fleets heartbeat their claims every 50ms, so 500ms of silence
        // means a dead worker; a hard-killed released worker's in-flight job
        // must be rescued well inside the drain windows below.
        .heartbeat_staleness(Duration::from_millis(500))
        .heartbeat_rescue_interval(heartbeat_rescue_interval)
        .deadline_rescue_interval(Duration::from_millis(100))
        .register_worker(RecordingWorker {
            completed,
            delay: Duration::from_millis(80),
        })
        .build()
        .expect("build current worker")
}

/// A current-build runtime serving the shared simple queue alongside the
/// full designed-outcome workload queue.
fn workload_client(
    pool: sqlx::PgPool,
    simple_queue: &str,
    workload_queue: &str,
    simple_completed: Arc<Mutex<HashSet<i64>>>,
) -> Client {
    Client::builder(pool)
        .queue(
            simple_queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(20),
                ..QueueConfig::default()
            },
        )
        .queue(
            workload_queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(20),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: "awa".to_string(),
                ..Default::default()
            },
            // Rotation must not fire inside this cell: reconciliation loads
            // every job's terminal row back from hot storage at the end, and
            // ring rotation prunes aged done-partitions. The other cells keep
            // the aggressive one-second cadence, so rotation churn during the
            // upgrade window stays covered there.
            Duration::from_secs(600),
            Duration::from_millis(50),
        )
        .heartbeat_interval(Duration::from_millis(50))
        .promote_interval(Duration::from_millis(50))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .heartbeat_staleness(Duration::from_millis(500))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(100))
        .register_worker(RecordingWorker {
            completed: simple_completed,
            delay: Duration::from_millis(80),
        })
        .register_worker(WorkloadWorker::default())
        .build()
        .expect("build workload worker")
}

/// One designed workload job and the exact terminal outcome it must reach.
struct WorkloadExpectation {
    id: i64,
    mode: &'static str,
    state: JobState,
    /// Expected terminal attempt; `None` where the design never claims the
    /// job (admin-cancelled parked work).
    attempt: Option<i64>,
}

/// Insert one batch of every designed workload outcome and record each job's
/// expected terminal `(state, attempt)`. Returns the ids of the parked jobs
/// the caller must admin-cancel.
async fn bank_workload_batch(
    pool: &sqlx::PgPool,
    queue: &str,
    seq_base: i64,
    expectations: &mut Vec<WorkloadExpectation>,
) -> Vec<i64> {
    // (insertion label, handler mode, count, expected state, expected attempt)
    let plan: &[(&'static str, &'static str, i64, JobState, Option<i64>)] = &[
        ("complete", "complete", 3, JobState::Completed, Some(1)),
        ("retry_once", "retry_once", 3, JobState::Completed, Some(2)),
        (
            "fail_terminal",
            "fail_terminal",
            3,
            JobState::Failed,
            Some(1),
        ),
        (
            "exhaust_retries",
            "exhaust_retries",
            3,
            JobState::Failed,
            Some(2),
        ),
        (
            "snooze_once",
            "snooze_once",
            3,
            JobState::Completed,
            Some(1),
        ),
        (
            "cancel_self",
            "cancel_self",
            3,
            JobState::Cancelled,
            Some(1),
        ),
        (
            "callback_then_complete",
            "callback_then_complete",
            3,
            JobState::Completed,
            Some(2),
        ),
        (
            "multi_second",
            "multi_second",
            2,
            JobState::Completed,
            Some(1),
        ),
        ("scheduled", "complete", 3, JobState::Completed, Some(1)),
        ("parked", "complete", 3, JobState::Cancelled, None),
        ("deadline", "complete", 3, JobState::Completed, Some(1)),
    ];

    let mut parked_ids = Vec::new();
    let mut seq = seq_base;
    for &(label, handler_mode, count, state, attempt) in plan {
        for _ in 0..count {
            seq += 1;
            let mut opts = InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            };
            match label {
                "exhaust_retries" => opts.max_attempts = 2,
                "scheduled" => opts.run_at = Some(Utc::now() + chrono::Duration::seconds(3)),
                "parked" => opts.run_at = Some(Utc::now() + chrono::Duration::hours(1)),
                "deadline" => opts.deadline_duration = Some(chrono::Duration::seconds(10)),
                _ => {}
            }
            let inserted = insert_with(
                pool,
                &UpgradeWorkloadJob {
                    seq,
                    mode: handler_mode.to_string(),
                },
                opts,
            )
            .await
            .unwrap_or_else(|error| panic!("insert workload job {label}: {error}"));
            if label == "parked" {
                parked_ids.push(inserted.id);
            }
            expectations.push(WorkloadExpectation {
                id: inserted.id,
                mode: label,
                state,
                attempt,
            });
        }
    }
    parked_ids
}

struct Producer {
    running: Arc<AtomicBool>,
    accepted: Arc<Mutex<HashSet<i64>>>,
    handle: tokio::task::JoinHandle<()>,
}

impl Producer {
    async fn stop(self) -> HashSet<i64> {
        self.running.store(false, Ordering::Relaxed);
        self.handle.await.expect("producer task");
        self.accepted.lock().unwrap().clone()
    }
}

fn start_producer(pool: sqlx::PgPool, queue: String) -> Producer {
    let running = Arc::new(AtomicBool::new(true));
    let running_task = running.clone();
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let accepted_task = accepted.clone();
    let next_seq = Arc::new(AtomicI64::new(0));
    let handle = tokio::spawn(async move {
        while running_task.load(Ordering::Relaxed) {
            let seq = next_seq.fetch_add(1, Ordering::Relaxed);
            match insert_with(
                &pool,
                &SimpleChaosJob { seq },
                InsertOpts {
                    queue: queue.clone(),
                    ..Default::default()
                },
            )
            .await
            {
                Ok(_) => {
                    accepted_task.lock().unwrap().insert(seq);
                }
                Err(error) => {
                    eprintln!("[producer] insert {seq} failed during rollout: {error}");
                }
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    });

    Producer {
        running,
        accepted,
        handle,
    }
}

async fn wait_for_accepted(producer: &Producer, minimum: usize, timeout: Duration) {
    let start = Instant::now();
    loop {
        if producer.accepted.lock().unwrap().len() >= minimum {
            return;
        }
        assert!(start.elapsed() < timeout, "producer accepted too few jobs");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn schema_version(pool: &sqlx::PgPool) -> i32 {
    sqlx::query_scalar("SELECT max(version)::int FROM awa.schema_version")
        .fetch_one(pool)
        .await
        .expect("read schema version")
}

async fn ring_cursor_pair(pool: &sqlx::PgPool, ring: &str) -> ((i32, i64), (i32, i64)) {
    let (column_slot, column_generation, ledger_slot, ledger_generation) =
        sqlx::query_as::<_, (i32, i64, i32, i64)>(&format!(
            "SELECT state.current_slot, state.generation, ledger.slot, ledger.generation \
         FROM awa.{ring}_ring_state AS state \
         CROSS JOIN LATERAL ( \
             SELECT slot, generation FROM awa.{ring}_ring_rotations \
             ORDER BY generation DESC LIMIT 1 \
         ) AS ledger \
         WHERE state.singleton"
        ))
        .fetch_one(pool)
        .await
        .expect("read ring cursors");
    (
        (column_slot, column_generation),
        (ledger_slot, ledger_generation),
    )
}

async fn wait_for_old_only_rotation(pool: &sqlx::PgPool, timeout: Duration) {
    let start = Instant::now();
    loop {
        for ring in ["queue", "lease", "claim"] {
            let (columns, ledger) = ring_cursor_pair(pool, ring).await;
            if columns.1 > ledger.1 {
                eprintln!(
                    "[upgrade] observed released-only {ring} rotation: columns={columns:?} ledger={ledger:?}"
                );
                return;
            }
        }
        assert!(
            start.elapsed() < timeout,
            "released worker never advanced a compat cursor beyond its shadow ledger"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_mixed_fleet(
    old: &ReleasedWorker,
    old_before_migration: usize,
    current: &Arc<Mutex<HashSet<i64>>>,
    current_before_migration: usize,
    timeout: Duration,
) {
    let start = Instant::now();
    loop {
        if old.completed().len() > old_before_migration
            && current.lock().unwrap().len() > current_before_migration
        {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "both released and current workers must complete jobs after migration"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Flip ring authority once every pre-flip (NULL `binary_version`) heartbeat
/// is stale, while stamped current heartbeats may remain fresh. Mirrors the
/// operator flow the migrate-first cell proved: refusal is asserted there;
/// these cells assert a live stamped fleet does not block the flip.
async fn flip_after_released_heartbeats_stale(pool: &sqlx::PgPool) {
    tokio::time::sleep(Duration::from_secs(1)).await;
    let flipped: String = sqlx::query_scalar("SELECT awa.flip_ring_authority($1, FALSE, 0.5)")
        .bind("awa")
        .fetch_one(pool)
        .await
        .expect("flip after released heartbeats become stale");
    assert_eq!(flipped, "ledger");
}

async fn wait_for_drain(
    store: &QueueStorage,
    pool: &sqlx::PgPool,
    queue: &str,
    terminal: usize,
    timeout: Duration,
) {
    let start = Instant::now();
    loop {
        let counts = store
            .queue_counts(pool, queue)
            .await
            .expect("queue counts during upgrade rehearsal");
        if counts.available == 0 && counts.running == 0 && counts.terminal == terminal as i64 {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "upgrade rehearsal failed to drain: expected={terminal} counts={counts:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a released awa-pg 0.6.3 environment"]
async fn test_migrate_first_mixed_fleet_flip_and_fence() {
    let _rehearsal_guard = REHEARSAL_LOCK.lock().await;
    let mut report = RehearsalReport::new("migrate_first_mixed_fleet_flip_and_fence");
    report.phase("start");
    let pool = pool().await;
    reset_database(&pool).await;
    let queue = format!("upgrade_rehearsal_{}", Uuid::new_v4().simple());

    // The released worker owns the initial install, proving the starting
    // schema is the real N-1 artifact's v040 shape.
    let mut old_worker = spawn_released_worker(&queue).await;
    assert_eq!(schema_version(&pool).await, 40);
    let initial_status = storage::status(&pool)
        .await
        .expect("initial storage status");
    assert_eq!(initial_status.state, "active");
    assert_eq!(initial_status.active_engine, "queue_storage");
    report.phase("released_v040_installed");

    let producer = start_producer(pool.clone(), queue.clone());
    wait_for_accepted(&producer, 25, Duration::from_secs(20)).await;
    let old_before_migration = old_worker.completed().len();

    // Apply v041-v043 while the released runtime and producer remain live.
    migrations::run(&pool)
        .await
        .expect("current migrations must accept a live 0.6.3 runtime");
    assert_eq!(schema_version(&pool).await, 43);
    let authority: String =
        sqlx::query_scalar("SELECT authority FROM awa.ring_cursor_authority WHERE singleton")
            .fetch_one(&pool)
            .await
            .expect("read ring authority");
    assert_eq!(authority, "columns");
    report.phase("migrated_to_v043_columns_authority");

    let current_completed = Arc::new(Mutex::new(HashSet::new()));
    let current = current_client(pool.clone(), &queue, current_completed.clone());
    current.start().await.expect("start current worker on v043");
    wait_for_mixed_fleet(
        &old_worker,
        old_before_migration,
        &current_completed,
        0,
        Duration::from_secs(30),
    )
    .await;
    report.phase("mixed_fleet_both_completing");

    // Leave the released runtime as the only rotator and prove it can advance
    // the authoritative columns after the current worker's last shadow write.
    current.shutdown(Duration::from_secs(5)).await;
    wait_for_old_only_rotation(&pool, Duration::from_secs(30)).await;
    report.phase("released_only_rotation_advanced_columns");

    let accepted = producer.stop().await;
    let store = QueueStorage::new(QueueStorageConfig {
        schema: "awa".to_string(),
        ..Default::default()
    })
    .expect("queue storage");
    wait_for_drain(
        &store,
        &pool,
        &queue,
        accepted.len(),
        Duration::from_secs(60),
    )
    .await;

    old_worker.shutdown().await;
    let final_columns = [
        ("queue", ring_cursor_pair(&pool, "queue").await.0),
        ("lease", ring_cursor_pair(&pool, "lease").await.0),
        ("claim", ring_cursor_pair(&pool, "claim").await.0),
    ];

    let status = storage::ring_authority_status(&pool, "awa")
        .await
        .expect("ring authority status");
    assert!(
        status.blocking_instances >= 1,
        "fresh released runtime must block the flip"
    );
    let refusal = storage::flip_ring_authority(&pool, "awa", false)
        .await
        .expect_err("flip must refuse while the released heartbeat is fresh");
    assert!(refusal.to_string().contains("refusing to flip"));
    report.phase("flip_refused_while_released_heartbeat_fresh");
    report.fact("flip_blocking_instances", status.blocking_instances);

    tokio::time::sleep(Duration::from_secs(1)).await;
    let flipped: String = sqlx::query_scalar("SELECT awa.flip_ring_authority($1, FALSE, 0.5)")
        .bind("awa")
        .fetch_one(&pool)
        .await
        .expect("flip after released heartbeat becomes stale");
    assert_eq!(flipped, "ledger");
    report.phase("authority_flipped");
    report.fact("authority", "ledger");

    for (ring, expected) in final_columns {
        let (_, ledger) = ring_cursor_pair(&pool, ring).await;
        assert_eq!(
            ledger, expected,
            "{ring} ledger must match final compat cursor"
        );
    }

    assert_released_worker_is_fenced(&queue).await;
    report.phase("returning_released_artifact_fenced");

    let current_after_flip = current_client(pool.clone(), &queue, current_completed.clone());
    current_after_flip
        .start()
        .await
        .expect("current worker must start after flip");
    let mut all_accepted = accepted;
    let first_post_flip = all_accepted.iter().max().copied().unwrap_or(0) + 1;
    for seq in first_post_flip..first_post_flip + 20 {
        insert_with(
            &pool,
            &SimpleChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("post-flip insert");
        all_accepted.insert(seq);
    }

    wait_for_drain(
        &store,
        &pool,
        &queue,
        all_accepted.len(),
        Duration::from_secs(60),
    )
    .await;
    current_after_flip.shutdown(Duration::from_secs(5)).await;

    let mut observed = old_worker.completed();
    observed.extend(current_completed.lock().unwrap().iter().copied());
    report.fact("accepted", all_accepted.len());
    report.fact("observed", observed.len());
    assert_eq!(
        observed, all_accepted,
        "accepted and observed job sets differ"
    );
    report.phase("reconciled_exact");
    report.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a released awa-pg 0.6.3 environment"]
async fn test_migrate_first_deadline_rescue_resumes_with_current_leader() {
    let _rehearsal_guard = REHEARSAL_LOCK.lock().await;
    let mut report = RehearsalReport::new("migrate_first_deadline_rescue_handoff");
    report.phase("start");
    let pool = pool().await;
    reset_database(&pool).await;
    let queue = format!("upgrade_deadline_{}", Uuid::new_v4().simple());
    let seq = 1;

    // Keep the released handler alive beyond its deadline. Its runtime
    // heartbeat and maintenance loops continue while the job is running.
    let mut old_worker = spawn_released_worker_with(&queue, 60_000, Some(300)).await;
    assert_eq!(schema_version(&pool).await, 40);

    report.phase("released_v040_installed");
    migrations::run(&pool)
        .await
        .expect("migrate with released deadline worker live");
    assert_eq!(schema_version(&pool).await, 43);
    report.phase("migrated_to_v043");

    let inserted = insert_with(
        &pool,
        &SimpleChaosJob { seq },
        InsertOpts {
            queue: queue.clone(),
            ..Default::default()
        },
    )
    .await
    .expect("insert deadline-bound job after migration");
    wait_for_released_start(&old_worker, seq, Duration::from_secs(20)).await;
    wait_for_expired_compact_claim(&pool, inserted.id, Duration::from_secs(10)).await;
    report.phase("compact_claim_expired_under_released_leader");

    // The released runtime runs its deadline maintenance every 100ms, but
    // that implementation only scans row-local claims. The v043 SQL claim
    // path writes this job to a compact batch, so it remains on attempt 1.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let store = QueueStorage::new(QueueStorageConfig {
        schema: "awa".to_string(),
        ..Default::default()
    })
    .expect("queue storage");
    let pending = store
        .load_job(&pool, inserted.id)
        .await
        .expect("load expired compact claim")
        .expect("expired compact claim must remain visible");
    assert_eq!(pending.state, JobState::Running);
    assert_eq!(pending.attempt, 1);
    let heartbeat_is_fresh: bool = sqlx::query_scalar(
        "SELECT heartbeat_at > clock_timestamp() - interval '500 milliseconds' \
         FROM awa.attempt_state WHERE job_id = $1 AND run_lease = $2",
    )
    .bind(inserted.id)
    .bind(pending.run_lease)
    .fetch_one(&pool)
    .await
    .expect("read released attempt heartbeat");
    assert!(
        heartbeat_is_fresh,
        "deadline boundary must be observed while the released worker is healthy"
    );

    let current_completed = Arc::new(Mutex::new(HashSet::new()));
    // Keep heartbeat rescue out of this cell's race. Once current owns
    // maintenance leadership, the expired compact deadline must be the
    // reason the claim is force-closed.
    let current = current_client_with_heartbeat_rescue(
        pool.clone(),
        &queue,
        current_completed.clone(),
        Duration::from_secs(60),
    );
    current
        .start()
        .await
        .expect("start current maintenance runtime");

    // Maintenance is leader-owned. Starting a current worker does not make
    // it sweep deadlines while the released runtime remains leader.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let still_pending = store
        .load_job(&pool, inserted.id)
        .await
        .expect("load claim during mixed-version leadership")
        .expect("claim must remain visible during mixed-version leadership");
    assert_eq!(still_pending.state, JobState::Running);
    assert_eq!(still_pending.attempt, 1);
    report.phase("claim_pending_while_released_holds_leadership");

    // Hard-kill the released maintenance leader mid-job. Current must take
    // leadership and rescue the expired compact claim via the deadline path.
    old_worker.shutdown().await;
    let start = Instant::now();
    loop {
        if current_completed.lock().unwrap().contains(&seq) {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "current runtime did not rescue and complete the expired batch claim"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    wait_for_drain(&store, &pool, &queue, 1, Duration::from_secs(20)).await;
    current.shutdown(Duration::from_secs(5)).await;

    let completed = store
        .load_job(&pool, inserted.id)
        .await
        .expect("load rescued terminal job")
        .expect("rescued job must remain visible");
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(
        completed.attempt, 2,
        "expired compact claim must be rescued exactly once"
    );
    assert!(
        !old_worker.completed().contains(&seq),
        "released worker must not report a competing completion"
    );
    report.phase("rescued_exactly_once_by_current_leader");
    report.fact("rescued_attempt", 2);
    report.finish();
}

/// Binary-first upgrade order: the current binary is deployed before
/// v041-v043 apply. The current runtime requires relations those migrations
/// add, so on the released artifact's v040 schema it must refuse startup
/// loudly — naming pending migrations, not silently degrading — while the
/// released fleet keeps draining traffic. The roll completes only after the
/// migration lands; the released worker is then hard-killed (SIGKILL)
/// mid-traffic, and the current fleet must rescue its in-flight work.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a released awa-pg 0.6.3 environment"]
async fn test_binary_first_current_refuses_v040_then_rolls_after_migration() {
    let _rehearsal_guard = REHEARSAL_LOCK.lock().await;
    let mut report = RehearsalReport::new("binary_first_refusal_then_roll");
    report.phase("start");
    let pool = pool().await;
    reset_database(&pool).await;
    let queue = format!("upgrade_binary_first_{}", Uuid::new_v4().simple());

    // The released artifact owns the install, proving the starting schema is
    // the real N-1 artifact's v040 shape, and banks a completion baseline.
    let mut old_worker = spawn_released_worker(&queue).await;
    assert_eq!(schema_version(&pool).await, 40);

    let producer = start_producer(pool.clone(), queue.clone());
    wait_for_accepted(&producer, 25, Duration::from_secs(20)).await;

    // A current runtime deployed before the migration must fail closed with
    // operator guidance, not claim work against relations that don't exist.
    let refused_completions = Arc::new(Mutex::new(HashSet::new()));
    let refused = current_client(pool.clone(), &queue, refused_completions.clone());
    let refusal = refused
        .start()
        .await
        .expect_err("current runtime must refuse to start on a v040 schema");
    let refusal_message = refusal.to_string();
    assert!(
        refusal_message.contains("awa migrate"),
        "startup refusal must direct the operator to pending migrations: {refusal_message}"
    );
    assert!(
        refused_completions.lock().unwrap().is_empty(),
        "a refused runtime must not complete any work"
    );
    report.phase("current_refused_on_v040");

    // The released fleet alone must keep draining traffic while the current
    // deployment is refused.
    let old_during_refusal = old_worker.completed().len();
    let refusal_window = Instant::now();
    loop {
        if old_worker.completed().len() > old_during_refusal + 10 {
            break;
        }
        assert!(
            refusal_window.elapsed() < Duration::from_secs(30),
            "released fleet must keep completing while the current deployment is refused"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let old_before_migration = old_worker.completed().len();
    migrations::run(&pool)
        .await
        .expect("migration must apply with the released fleet live");
    assert_eq!(schema_version(&pool).await, 43);
    let authority: String =
        sqlx::query_scalar("SELECT authority FROM awa.ring_cursor_authority WHERE singleton")
            .fetch_one(&pool)
            .await
            .expect("read ring authority");
    assert_eq!(authority, "columns");
    report.phase("migrated_with_released_fleet_live");

    // The roll now completes: the same binary that was refused on v040 must
    // start unaided and join the released fleet.
    let current_completed = Arc::new(Mutex::new(HashSet::new()));
    let current = current_client(pool.clone(), &queue, current_completed.clone());
    current
        .start()
        .await
        .expect("current worker must start once the migration has applied");
    wait_for_mixed_fleet(
        &old_worker,
        old_before_migration,
        &current_completed,
        0,
        Duration::from_secs(30),
    )
    .await;
    report.phase("roll_completed_unaided_mixed_fleet");

    // Retire the released fleet the adversarial way: SIGKILL mid-traffic.
    // Any in-flight released claim must be rescued by the current fleet for
    // the exact drain below to hold.
    old_worker.shutdown().await;

    let accepted = producer.stop().await;
    let store = QueueStorage::new(QueueStorageConfig {
        schema: "awa".to_string(),
        ..Default::default()
    })
    .expect("queue storage");
    wait_for_drain(
        &store,
        &pool,
        &queue,
        accepted.len(),
        Duration::from_secs(60),
    )
    .await;

    // Flip while the stamped current fleet stays live: only fresh NULL
    // binary_version heartbeats (pre-flip binaries) may block the flip.
    report.phase("released_fleet_hard_killed");
    flip_after_released_heartbeats_stale(&pool).await;
    report.phase("authority_flipped_with_stamped_fleet_live");

    assert_released_worker_is_fenced(&queue).await;
    report.phase("returning_released_artifact_fenced");

    // The same current runtime keeps operating across the flip, with no
    // restart, and drains post-flip traffic in ledger authority.
    let mut all_accepted = accepted;
    let first_post_flip = all_accepted.iter().max().copied().unwrap_or(0) + 1;
    for seq in first_post_flip..first_post_flip + 20 {
        insert_with(
            &pool,
            &SimpleChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("post-flip insert");
        all_accepted.insert(seq);
    }
    wait_for_drain(
        &store,
        &pool,
        &queue,
        all_accepted.len(),
        Duration::from_secs(60),
    )
    .await;
    current.shutdown(Duration::from_secs(5)).await;

    let mut observed = old_worker.completed();
    observed.extend(current_completed.lock().unwrap().iter().copied());
    report.fact("accepted", all_accepted.len());
    report.fact("observed", observed.len());
    assert_eq!(
        observed, all_accepted,
        "accepted and observed job sets differ"
    );
    report.phase("reconciled_exact");
    report.finish();
}

/// Migrate-first upgrade under the full designed-outcome workload: a backlog
/// banked on the released artifact's v040 schema covers normal completions,
/// controlled retries, terminal and exhausted failures, snoozes, handler and
/// admin cancellations, callback waits, future-scheduled work,
/// deadline-bearing jobs, and multi-second jobs; cron fires before and after
/// the authority flip. Simple traffic keeps flowing on a shared queue and the
/// released worker is hard-killed mid-window. Reconciliation loads every
/// workload job from the discrete storage tables and requires its exact
/// designed terminal state and attempt, and the DLQ to hold exactly the
/// designed failures.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a released awa-pg 0.6.3 environment"]
async fn test_migrate_first_full_workload_reconciles_designed_outcomes() {
    let _rehearsal_guard = REHEARSAL_LOCK.lock().await;
    let mut report = RehearsalReport::new("migrate_first_full_workload");
    report.phase("start");
    let pool = pool().await;
    reset_database(&pool).await;
    let run_id = Uuid::new_v4().simple().to_string();
    let queue = format!("upgrade_workload_simple_{run_id}");
    let workload_queue = format!("upgrade_workload_outcomes_{run_id}");

    let mut old_worker = spawn_released_worker(&queue).await;
    assert_eq!(schema_version(&pool).await, 40);

    let producer = start_producer(pool.clone(), queue.clone());
    wait_for_accepted(&producer, 25, Duration::from_secs(20)).await;
    let old_before_migration = old_worker.completed().len();

    // Bank the full designed backlog on the real v040 schema. Nothing serves
    // the workload queue yet, so every one of these jobs crosses the
    // migration boundary in a pre-terminal state.
    let mut expectations = Vec::new();
    let pre_flip_parked = bank_workload_batch(&pool, &workload_queue, 0, &mut expectations).await;

    report.phase("workload_banked_on_v040");
    migrations::run(&pool)
        .await
        .expect("migration must apply with the released fleet and banked backlog live");
    assert_eq!(schema_version(&pool).await, 43);

    let simple_completed = Arc::new(Mutex::new(HashSet::new()));
    let current = workload_client(
        pool.clone(),
        &queue,
        &workload_queue,
        simple_completed.clone(),
    );
    current
        .start()
        .await
        .expect("start current workload runtime on v043");
    wait_for_mixed_fleet(
        &old_worker,
        old_before_migration,
        &simple_completed,
        0,
        Duration::from_secs(30),
    )
    .await;
    report.phase("migrated_mixed_fleet_completing");

    // Cron work fires while both fleets are live, before any flip. The
    // schedule itself must never fire on its own — the test wants exactly
    // the two explicit trigger_cron_job() calls below and nothing else.
    // `*/5 * * * *` looks inert but can coincide with a real wall-clock
    // boundary on a slow/contended run, and the periodic dispatcher fires
    // independently of manual triggers, producing an uncounted third
    // completion that overshoots `expectations` by one. Pausing right
    // after registration blocks *automatic* fires while leaving
    // trigger_cron_job() — which works on paused schedules by design —
    // fully functional for the two intentional fires.
    let cron_name = format!("upgrade_rehearsal_cron_{run_id}");
    let cron_job = PeriodicJob::builder(cron_name.clone(), "*/5 * * * *")
        .queue(workload_queue.clone())
        .build(&UpgradeWorkloadJob {
            seq: -1,
            mode: "complete".to_string(),
        })
        .expect("build cron job");
    upsert_cron_job(&pool, &cron_job)
        .await
        .expect("upsert cron job");
    pause_cron_job(&pool, &cron_name, Some("rehearsal-test"))
        .await
        .expect("pause cron schedule so only explicit triggers fire it");
    let pre_flip_fire = trigger_cron_job(&pool, &cron_name)
        .await
        .expect("trigger cron before flip");
    expectations.push(WorkloadExpectation {
        id: pre_flip_fire.id,
        mode: "cron",
        state: JobState::Completed,
        attempt: Some(1),
    });

    // Admin cancellation must terminate parked scheduled work exactly once.
    for job_id in pre_flip_parked {
        let cancelled = admin::cancel(&pool, job_id)
            .await
            .expect("admin cancel parked job")
            .expect("parked job must exist when cancelled");
        assert_eq!(cancelled.state, JobState::Cancelled);
    }

    // Hard-kill the released worker mid-traffic; its in-flight simple job
    // must be rescued by the current fleet for the exact drains below.
    old_worker.shutdown().await;

    let accepted = producer.stop().await;
    let store = QueueStorage::new(QueueStorageConfig {
        schema: "awa".to_string(),
        ..Default::default()
    })
    .expect("queue storage");
    wait_for_drain(
        &store,
        &pool,
        &queue,
        accepted.len(),
        Duration::from_secs(60),
    )
    .await;
    wait_for_drain(
        &store,
        &pool,
        &workload_queue,
        expectations.len(),
        Duration::from_secs(120),
    )
    .await;

    report.phase("pre_flip_workload_drained");
    flip_after_released_heartbeats_stale(&pool).await;
    assert_released_worker_is_fenced(&queue).await;
    report.phase("flipped_and_fenced");

    // The same designed workload must reconcile identically in ledger
    // authority, including a post-flip cron fire.
    let post_flip_parked =
        bank_workload_batch(&pool, &workload_queue, 1_000, &mut expectations).await;
    let post_flip_fire = trigger_cron_job(&pool, &cron_name)
        .await
        .expect("trigger cron after flip");
    expectations.push(WorkloadExpectation {
        id: post_flip_fire.id,
        mode: "cron",
        state: JobState::Completed,
        attempt: Some(1),
    });
    for job_id in post_flip_parked {
        let cancelled = admin::cancel(&pool, job_id)
            .await
            .expect("admin cancel parked job after flip")
            .expect("post-flip parked job must exist when cancelled");
        assert_eq!(cancelled.state, JobState::Cancelled);
    }
    wait_for_drain(
        &store,
        &pool,
        &workload_queue,
        expectations.len(),
        Duration::from_secs(120),
    )
    .await;
    current.shutdown(Duration::from_secs(5)).await;

    // Every workload job must land on its designed terminal outcome, read
    // back from the discrete storage tables.
    for expectation in &expectations {
        let job = store
            .load_job(&pool, expectation.id)
            .await
            .expect("load workload job")
            .unwrap_or_else(|| {
                panic!(
                    "workload job {} ({}) must remain visible",
                    expectation.id, expectation.mode
                )
            });
        assert_eq!(
            job.state, expectation.state,
            "job {} ({}) terminal state",
            expectation.id, expectation.mode
        );
        if let Some(attempt) = expectation.attempt {
            assert_eq!(
                i64::from(job.attempt),
                attempt,
                "job {} ({}) terminal attempt",
                expectation.id,
                expectation.mode
            );
        }
    }

    // Failed rows enter the durable DLQ archive when ring rotation prunes
    // them out of hot storage. Rotation is disabled in this cell (the per-job
    // loads above depend on it), so designed failures normally stay visible
    // as `failed` rows — but any row that does reach the archive must be a
    // designed failure, and nothing else may.
    let designed_failure_ids: HashSet<i64> = expectations
        .iter()
        .filter(|expectation| expectation.state == JobState::Failed)
        .map(|expectation| expectation.id)
        .collect();
    let archived = dlq::list_dlq(
        &pool,
        &dlq::ListDlqFilter {
            queue: Some(workload_queue.clone()),
            ..Default::default()
        },
    )
    .await
    .expect("list workload DLQ archive");
    for row in &archived {
        assert!(
            designed_failure_ids.contains(&row.job.id),
            "DLQ archive holds job {} which was not a designed failure",
            row.job.id
        );
    }

    let mut observed = old_worker.completed();
    observed.extend(simple_completed.lock().unwrap().iter().copied());
    report.fact("workload_jobs", expectations.len());
    report.fact("designed_failures", designed_failure_ids.len());
    report.fact("simple_accepted", accepted.len());
    report.fact("simple_observed", observed.len());
    assert_eq!(
        observed, accepted,
        "accepted and observed simple job sets differ"
    );
    report.phase("designed_outcomes_reconciled");
    report.finish();
}

/// Overlapped upgrade order: the current deployment and the migration race
/// under live released traffic. A rolling deployment keeps restarting its new
/// pods, so the current runtime start-retries through its schema refusal and
/// must come up unaided the moment v041-v043 commit. Both versions must then
/// claim and complete work concurrently before any flip.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a released awa-pg 0.6.3 environment"]
async fn test_overlap_current_roll_races_live_migration() {
    let _rehearsal_guard = REHEARSAL_LOCK.lock().await;
    let mut report = RehearsalReport::new("overlap_roll_races_migration");
    report.phase("start");
    let pool = pool().await;
    reset_database(&pool).await;
    let queue = format!("upgrade_overlap_{}", Uuid::new_v4().simple());

    let mut old_worker = spawn_released_worker(&queue).await;
    assert_eq!(schema_version(&pool).await, 40);

    let producer = start_producer(pool.clone(), queue.clone());
    wait_for_accepted(&producer, 25, Duration::from_secs(20)).await;
    let old_before_migration = old_worker.completed().len();

    // Roll and migrate concurrently. Each refused start is a crash-loop
    // iteration; only the schema-pending refusal is an acceptable reason.
    let current_completed = Arc::new(Mutex::new(HashSet::new()));
    let start_retry_pool = pool.clone();
    let start_retry_queue = queue.clone();
    let start_retry_completed = current_completed.clone();
    let start_retry = async move {
        let deadline = Instant::now();
        loop {
            let candidate = current_client(
                start_retry_pool.clone(),
                &start_retry_queue,
                start_retry_completed.clone(),
            );
            match candidate.start().await {
                Ok(()) => return candidate,
                Err(error) => {
                    let message = error.to_string();
                    assert!(
                        message.contains("awa migrate"),
                        "mid-roll start may only be refused for pending migrations: {message}"
                    );
                    assert!(
                        deadline.elapsed() < Duration::from_secs(30),
                        "current worker never came up after the live migration"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    };
    let (current, migration_result) = tokio::join!(start_retry, migrations::run(&pool));
    migration_result.expect("migration must apply with both fleets and the producer live");
    report.phase("migration_committed_during_roll");
    assert_eq!(schema_version(&pool).await, 43);

    // Both versions claim and complete concurrently after migration, before
    // any flip.
    wait_for_mixed_fleet(
        &old_worker,
        old_before_migration,
        &current_completed,
        0,
        Duration::from_secs(30),
    )
    .await;
    report.phase("both_versions_completing_pre_flip");

    let accepted = producer.stop().await;
    let store = QueueStorage::new(QueueStorageConfig {
        schema: "awa".to_string(),
        ..Default::default()
    })
    .expect("queue storage");
    wait_for_drain(
        &store,
        &pool,
        &queue,
        accepted.len(),
        Duration::from_secs(60),
    )
    .await;

    // Retire the released fleet (hard kill), flip with the stamped current
    // fleet still live, and fence the returning released artifact.
    old_worker.shutdown().await;
    flip_after_released_heartbeats_stale(&pool).await;
    assert_released_worker_is_fenced(&queue).await;
    report.phase("flipped_and_fenced");

    let mut all_accepted = accepted;
    let first_post_flip = all_accepted.iter().max().copied().unwrap_or(0) + 1;
    for seq in first_post_flip..first_post_flip + 20 {
        insert_with(
            &pool,
            &SimpleChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("post-flip insert");
        all_accepted.insert(seq);
    }
    wait_for_drain(
        &store,
        &pool,
        &queue,
        all_accepted.len(),
        Duration::from_secs(60),
    )
    .await;
    current.shutdown(Duration::from_secs(5)).await;

    let mut observed = old_worker.completed();
    observed.extend(current_completed.lock().unwrap().iter().copied());
    report.fact("accepted", all_accepted.len());
    report.fact("observed", observed.len());
    assert_eq!(
        observed, all_accepted,
        "accepted and observed job sets differ"
    );
    report.phase("reconciled_exact");
    report.finish();
}
