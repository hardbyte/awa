//! Released-artifact rolling-upgrade rehearsal (#427).
//!
//! This first cell exercises migrate-first operation from the latest released
//! 0.6 patch to the current v043 schema while traffic remains live. The old
//! worker is a PyPI wheel installed by the workflow, not a source checkout.

use async_trait::async_trait;
use awa::model::{
    insert_with, migrations, storage, InsertOpts, JobState, QueueStorage, QueueStorageConfig,
};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
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
        .heartbeat_rescue_interval(heartbeat_rescue_interval)
        .deadline_rescue_interval(Duration::from_millis(100))
        .register_worker(RecordingWorker {
            completed,
            delay: Duration::from_millis(80),
        })
        .build()
        .expect("build current worker")
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
    timeout: Duration,
) {
    let start = Instant::now();
    loop {
        if old.completed().len() > old_before_migration && !current.lock().unwrap().is_empty() {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "both released and current workers must complete jobs after migration"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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

    let current_completed = Arc::new(Mutex::new(HashSet::new()));
    let current = current_client(pool.clone(), &queue, current_completed.clone());
    current.start().await.expect("start current worker on v043");
    wait_for_mixed_fleet(
        &old_worker,
        old_before_migration,
        &current_completed,
        Duration::from_secs(30),
    )
    .await;

    // Leave the released runtime as the only rotator and prove it can advance
    // the authoritative columns after the current worker's last shadow write.
    current.shutdown(Duration::from_secs(5)).await;
    wait_for_old_only_rotation(&pool, Duration::from_secs(30)).await;

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

    tokio::time::sleep(Duration::from_secs(1)).await;
    let flipped: String = sqlx::query_scalar("SELECT awa.flip_ring_authority($1, FALSE, 0.5)")
        .bind("awa")
        .fetch_one(&pool)
        .await
        .expect("flip after released heartbeat becomes stale");
    assert_eq!(flipped, "ledger");

    for (ring, expected) in final_columns {
        let (_, ledger) = ring_cursor_pair(&pool, ring).await;
        assert_eq!(
            ledger, expected,
            "{ring} ledger must match final compat cursor"
        );
    }

    assert_released_worker_is_fenced(&queue).await;

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
    assert_eq!(
        observed, all_accepted,
        "accepted and observed job sets differ"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a released awa-pg 0.6.3 environment"]
async fn test_migrate_first_deadline_rescue_resumes_with_current_leader() {
    let _rehearsal_guard = REHEARSAL_LOCK.lock().await;
    let pool = pool().await;
    reset_database(&pool).await;
    let queue = format!("upgrade_deadline_{}", Uuid::new_v4().simple());
    let seq = 1;

    // Keep the released handler alive beyond its deadline. Its runtime
    // heartbeat and maintenance loops continue while the job is running.
    let mut old_worker = spawn_released_worker_with(&queue, 60_000, Some(300)).await;
    assert_eq!(schema_version(&pool).await, 40);

    migrations::run(&pool)
        .await
        .expect("migrate with released deadline worker live");
    assert_eq!(schema_version(&pool).await, 43);

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
}
