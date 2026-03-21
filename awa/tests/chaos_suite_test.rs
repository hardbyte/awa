//! Chaos and soak tests for longer-running failure-mode scenarios.
//!
//! These are intentionally ignored in normal CI and are meant for the slower
//! nightly/manual chaos lane.

use async_trait::async_trait;
use awa::model::{insert_with, migrations, InsertOpts};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use chrono::{Duration as ChronoDuration, Utc};
use opentelemetry_sdk::metrics::data::Sum;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::mpsc;
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn database_url_with_app_name(app_name: &str) -> String {
    let mut url = database_url();
    let sep = if url.contains('?') { '&' } else { '?' };
    url.push(sep);
    url.push_str("application_name=");
    url.push_str(app_name);
    url
}

async fn pool_with(max_conns: u32) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database")
}

async fn pool_with_url(database_url: &str, max_conns: u32) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(database_url)
        .await
        .expect("Failed to connect to database")
}

async fn setup(max_conns: u32) -> sqlx::PgPool {
    let pool = pool_with(max_conns).await;
    migrations::run(&pool).await.expect("Failed to migrate");
    pool
}

async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue jobs");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue meta");
}

async fn queue_state_counts(pool: &sqlx::PgPool, queue: &str) -> HashMap<String, i64> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"
        SELECT state::text, count(*)::bigint
        FROM awa.jobs
        WHERE queue = $1
        GROUP BY state
        "#,
    )
    .bind(queue)
    .fetch_all(pool)
    .await
    .expect("Failed to query state counts");

    rows.into_iter().collect()
}

fn state_count(counts: &HashMap<String, i64>, state: &str) -> i64 {
    counts.get(state).copied().unwrap_or(0)
}

async fn wait_for_counts(
    pool: &sqlx::PgPool,
    queue: &str,
    predicate: impl Fn(&HashMap<String, i64>) -> bool,
    timeout: Duration,
) -> HashMap<String, i64> {
    let start = Instant::now();
    loop {
        let counts = queue_state_counts(pool, queue).await;
        if predicate(&counts) {
            return counts;
        }
        assert!(
            start.elapsed() < timeout,
            "Timed out waiting for queue {queue} counts; last counts: {counts:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_single_leader(clients: &[&Client], timeout: Duration) -> usize {
    let start = Instant::now();
    loop {
        let mut leaders = Vec::new();
        for (idx, client) in clients.iter().enumerate() {
            let health = client.health_check().await;
            if health.leader {
                leaders.push(idx);
            }
        }
        if leaders.len() == 1 {
            return leaders[0];
        }
        assert!(
            start.elapsed() < timeout,
            "Timed out waiting for a single leader; leaders={leaders:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn python_test_bin() -> PathBuf {
    std::env::var_os("AWA_PYTHON_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("awa-python/.venv/bin/python"))
}

fn mixed_fleet_helper_path() -> PathBuf {
    workspace_root().join("awa-python/tests/mixed_fleet_helper.py")
}

struct PythonHelperProcess {
    child: Child,
    stdout: BufReader<ChildStdout>,
}

impl PythonHelperProcess {
    async fn wait_for_line(&mut self, expected: &str, timeout: Duration) -> String {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut seen = Vec::new();
        loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out waiting for python helper output: {expected}\n{}",
                seen.join("\n")
            );
            let mut line = String::new();
            let read = match tokio::time::timeout(
                Duration::from_millis(250),
                self.stdout.read_line(&mut line),
            )
            .await
            {
                Ok(result) => result.expect("Failed to read python helper stdout"),
                Err(_) => continue,
            };
            if read == 0 {
                let status = self
                    .child
                    .wait()
                    .await
                    .expect("Failed to wait for python helper");
                panic!(
                    "Python helper exited before emitting expected output: {expected}\nstatus={status}\n{}",
                    seen.join("\n")
                );
            }
            let text = line.trim().to_string();
            seen.push(text.clone());
            if text.contains(expected) {
                return text;
            }
        }
    }

    async fn stop(mut self) {
        if self.child.id().is_none() {
            return;
        }
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

async fn start_python_helper(
    mode: &str,
    queue: &str,
    extra_env: &[(&str, String)],
) -> PythonHelperProcess {
    let python = python_test_bin();
    assert!(
        python.exists(),
        "Python test interpreter not found at {}. Build the awa-python test venv or set AWA_PYTHON_BIN.",
        python.display()
    );

    let script = mixed_fleet_helper_path();
    assert!(
        script.exists(),
        "Mixed-fleet helper script not found at {}",
        script.display()
    );

    let mut command = Command::new(python);
    command
        .arg(script)
        .env("DATABASE_URL", database_url())
        .env("MIXED_QUEUE", queue)
        .env("MIXED_MODE", mode)
        .env("PYTHONUNBUFFERED", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());

    for (key, value) in extra_env {
        command.env(key, value);
    }

    let mut child = command.spawn().expect("Failed to spawn python helper");
    let stdout = child
        .stdout
        .take()
        .expect("Failed to capture python helper stdout");

    PythonHelperProcess {
        child,
        stdout: BufReader::new(stdout),
    }
}

async fn run_python_helper(mode: &str, queue: &str, extra_env: &[(&str, String)]) -> String {
    let python = python_test_bin();
    assert!(
        python.exists(),
        "Python test interpreter not found at {}. Build the awa-python test venv or set AWA_PYTHON_BIN.",
        python.display()
    );

    let script = mixed_fleet_helper_path();
    let mut command = Command::new(python);
    command
        .arg(script)
        .env("DATABASE_URL", database_url())
        .env("MIXED_QUEUE", queue)
        .env("MIXED_MODE", mode)
        .stderr(Stdio::inherit());

    for (key, value) in extra_env {
        command.env(key, value);
    }

    let output = command.output().await.expect("Failed to run python helper");
    assert!(
        output.status.success(),
        "Python helper failed with status {}",
        output.status
    );
    String::from_utf8(output.stdout).expect("Python helper output was not valid UTF-8")
}

async fn current_leader_backend_pid(pool: &sqlx::PgPool) -> Option<i32> {
    let rows: Vec<(i32,)> = sqlx::query_as(
        r#"
        SELECT pid
        FROM pg_locks
        WHERE locktype = 'advisory'
          AND granted
        ORDER BY pid
        "#,
    )
    .fetch_all(pool)
    .await
    .expect("Failed to query advisory lock holders");

    if rows.is_empty() {
        return None;
    }
    assert_eq!(
        rows.len(),
        1,
        "Expected exactly one advisory lock holder, got {rows:?}"
    );
    Some(rows[0].0)
}

async fn wait_for_new_leader_backend_pid(
    pool: &sqlx::PgPool,
    previous_pid: i32,
    timeout: Duration,
) -> i32 {
    let start = Instant::now();
    loop {
        if let Some(pid) = current_leader_backend_pid(pool).await {
            if pid != previous_pid {
                return pid;
            }
        }
        assert!(
            start.elapsed() < timeout,
            "Timed out waiting for a new leader backend pid after terminating {previous_pid}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn terminate_backend(pool: &sqlx::PgPool, pid: i32) {
    let terminated: (bool,) = sqlx::query_as("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(pool)
        .await
        .expect("Failed to terminate backend");
    assert!(
        terminated.0,
        "Postgres declined to terminate backend pid={pid}"
    );
}

async fn terminate_application_backends(pool: &sqlx::PgPool, app_name: &str) -> usize {
    let pids: Vec<(i32,)> = sqlx::query_as(
        r#"
        SELECT pid
        FROM pg_stat_activity
        WHERE application_name = $1
          AND pid <> pg_backend_pid()
          AND backend_type = 'client backend'
        "#,
    )
    .bind(app_name)
    .fetch_all(pool)
    .await
    .expect("Failed to query application backends");

    for (pid,) in &pids {
        terminate_backend(pool, *pid).await;
    }

    pids.len()
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

fn chaos_queue(prefix: &str) -> String {
    format!("{prefix}_{}", &Uuid::new_v4().simple().to_string()[..8])
}

fn complete_client(pool: sqlx::PgPool, queue: &str) -> Client {
    Client::builder(pool)
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .heartbeat_interval(Duration::from_millis(50))
        .promote_interval(Duration::from_millis(50))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .register_worker(CompleteWorker)
        .build()
        .expect("Failed to build complete client")
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SimpleChaosJob {
    seq: i64,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct ChaosProbe {
    marker: String,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct ChaosJob {
    seq: i64,
    mode: String,
}

struct CompleteWorker;

#[async_trait]
impl Worker for CompleteWorker {
    fn kind(&self) -> &'static str {
        "simple_chaos_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::Completed)
    }
}

struct MixedChaosWorker;

#[async_trait]
impl Worker for MixedChaosWorker {
    fn kind(&self) -> &'static str {
        "chaos_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: ChaosJob = serde_json::from_value(ctx.job.args.clone())
            .map_err(|err| JobError::terminal(format!("failed to decode chaos args: {err}")))?;

        match args.mode.as_str() {
            "complete" => Ok(JobResult::Completed),
            "retry_once" => {
                if ctx.job.attempt == 1 {
                    Ok(JobResult::RetryAfter(Duration::from_millis(100)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "terminal_fail" => Err(JobError::terminal("intentional chaos failure")),
            "callback_timeout" => {
                if ctx.job.attempt == 1 {
                    ctx.register_callback(Duration::from_millis(150))
                        .await
                        .map_err(JobError::retryable)?;
                    Ok(JobResult::WaitForCallback)
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "deadline_hang" => {
                if ctx.job.attempt == 1 {
                    sqlx::query(
                        r#"
                        UPDATE awa.jobs
                        SET deadline_at = now() + make_interval(secs => $2)
                        WHERE id = $1 AND run_lease = $3
                        "#,
                    )
                    .bind(ctx.job.id)
                    .bind(0.15_f64)
                    .bind(ctx.job.run_lease)
                    .execute(ctx.pool())
                    .await
                    .map_err(JobError::retryable)?;

                    for _ in 0..200 {
                        if ctx.is_cancelled() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }

                    if !ctx.is_cancelled() {
                        return Err(JobError::terminal(
                            "deadline rescue did not cancel the hanging job",
                        ));
                    }

                    Ok(JobResult::RetryAfter(Duration::from_millis(50)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            other => Err(JobError::terminal(format!("unknown chaos mode: {other}"))),
        }
    }
}

struct CallbackTimeoutWorker;

#[async_trait]
impl Worker for CallbackTimeoutWorker {
    fn kind(&self) -> &'static str {
        "simple_chaos_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        if ctx.job.attempt == 1 {
            // Register with a very long timeout so the leader's rescue cycle
            // can never expire these callbacks naturally. The test manually
            // backdates callback_timeout_at after killing the leader, making
            // the scenario fully deterministic (no timing race).
            ctx.register_callback(Duration::from_secs(3600))
                .await
                .map_err(JobError::retryable)?;
            Ok(JobResult::WaitForCallback)
        } else {
            Ok(JobResult::Completed)
        }
    }
}

struct MixedFleetRustWorker {
    tx: mpsc::UnboundedSender<String>,
}

#[async_trait]
impl Worker for MixedFleetRustWorker {
    fn kind(&self) -> &'static str {
        "chaos_probe"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: ChaosProbe = serde_json::from_value(ctx.job.args.clone()).map_err(|err| {
            JobError::terminal(format!("failed to decode mixed fleet args: {err}"))
        })?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.tx
            .send(args.marker)
            .expect("mixed fleet receiver dropped");
        Ok(JobResult::Completed)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_mixed_workload_soak_tracks_recovery_and_metrics() {
    let pool = setup(20).await;
    let queue = chaos_queue("chaos_mixed");
    clean_queue(&pool, &queue).await;

    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let client = Client::builder(pool.clone())
        .queue(
            &queue,
            QueueConfig {
                max_workers: 8,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .heartbeat_interval(Duration::from_millis(50))
        .promote_interval(Duration::from_millis(50))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(100))
        .leader_election_interval(Duration::from_millis(100))
        .register_worker(MixedChaosWorker)
        .build()
        .expect("Failed to build chaos client");

    client.start().await.expect("Failed to start chaos client");

    let per_mode = 10_i64;
    let modes = [
        "complete",
        "retry_once",
        "terminal_fail",
        "callback_timeout",
        "deadline_hang",
    ];
    let mut seq = 0_i64;
    for mode in modes {
        for _ in 0..per_mode {
            insert_with(
                &pool,
                &ChaosJob {
                    seq,
                    mode: mode.to_string(),
                },
                InsertOpts {
                    queue: queue.clone(),
                    max_attempts: 3,
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to insert chaos job");
            seq += 1;
        }
    }

    let expected_completed = per_mode * 4;
    let expected_failed = per_mode;
    let counts = wait_for_counts(
        &pool,
        &queue,
        |counts| {
            state_count(counts, "completed") == expected_completed
                && state_count(counts, "failed") == expected_failed
                && state_count(counts, "running") == 0
                && state_count(counts, "retryable") == 0
                && state_count(counts, "scheduled") == 0
                && state_count(counts, "waiting_external") == 0
        },
        Duration::from_secs(15),
    )
    .await;

    assert_eq!(state_count(&counts, "completed"), expected_completed);
    assert_eq!(state_count(&counts, "failed"), expected_failed);

    client.shutdown(Duration::from_secs(5)).await;

    meter_provider
        .force_flush()
        .expect("Failed to flush chaos metrics");
    let resource_metrics = exporter
        .get_finished_metrics()
        .expect("Failed to read chaos metrics");

    assert!(
        sum_counter_metric(&resource_metrics, "awa.job.completed") >= expected_completed as u64,
        "completed metric did not reflect recovered mixed workload"
    );
    assert!(
        sum_counter_metric(&resource_metrics, "awa.job.failed") >= expected_failed as u64,
        "failed metric did not reflect mixed workload failures"
    );
    assert!(
        sum_counter_metric(&resource_metrics, "awa.job.waiting_external") >= per_mode as u64,
        "waiting_external metric did not record parked callback jobs"
    );
    assert!(
        sum_counter_metric(&resource_metrics, "awa.maintenance.rescues") >= (per_mode * 2) as u64,
        "maintenance rescue metric did not record deadline + callback rescues"
    );

    let _ = meter_provider.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_mixed_rust_and_python_workers_share_same_queue() {
    let pool = setup(20).await;
    let queue = chaos_queue("chaos_mixed_lang");
    clean_queue(&pool, &queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            &queue,
            QueueConfig {
                max_workers: 1,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .heartbeat_interval(Duration::from_millis(50))
        .promote_interval(Duration::from_millis(50))
        .leader_election_interval(Duration::from_millis(100))
        .register_worker(MixedFleetRustWorker { tx })
        .build()
        .expect("Failed to build mixed-fleet client");

    client
        .start()
        .await
        .expect("Failed to start mixed-fleet Rust client");

    let mut python_worker = start_python_helper("worker_chaos_probe", &queue, &[]).await;

    let batch_size = 12_i64;

    let test_result = async {
        python_worker
            .wait_for_line("READY mode=worker_chaos_probe", Duration::from_secs(10))
            .await;

        let inserted = run_python_helper(
            "insert_chaos_probe_batch",
            &queue,
            &[
                ("MIXED_PREFIX", "python".to_string()),
                ("MIXED_COUNT", batch_size.to_string()),
            ],
        )
        .await;
        assert!(
            inserted.contains("INSERTED mode=insert_chaos_probe_batch")
                && inserted.contains(&format!("count={batch_size}")),
            "Unexpected python inserter output: {inserted}"
        );

        for idx in 0..batch_size {
            insert_with(
                &pool,
                &ChaosProbe {
                    marker: format!("rust-{idx}"),
                },
                InsertOpts {
                    queue: queue.clone(),
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to insert Rust-enqueued ChaosProbe");
        }

        let rust_processed_marker = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("Timed out waiting for Rust worker to process a shared-kind job")
            .expect("Rust mixed-fleet receiver closed unexpectedly");
        assert!(
            rust_processed_marker.starts_with("python-")
                || rust_processed_marker.starts_with("rust-"),
            "Unexpected marker processed by Rust worker: {rust_processed_marker}"
        );

        let python_line = python_worker
            .wait_for_line("COMPLETE mode=worker_chaos_probe", Duration::from_secs(10))
            .await;
        assert!(
            python_line.contains("marker=python-") || python_line.contains("marker=rust-"),
            "Unexpected python worker completion line: {python_line}"
        );

        let counts = wait_for_counts(
            &pool,
            &queue,
            |counts| {
                state_count(counts, "completed") == batch_size * 2
                    && state_count(counts, "running") == 0
                    && state_count(counts, "available") == 0
            },
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(state_count(&counts, "completed"), batch_size * 2);
    }
    .await;

    python_worker.stop().await;
    client.shutdown(Duration::from_secs(5)).await;

    test_result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_runtime_recovers_after_terminating_postgres_connections() {
    let app_name = format!(
        "chaos_disconnect_{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let app_pool = pool_with_url(&database_url_with_app_name(&app_name), 20).await;
    migrations::run(&app_pool)
        .await
        .expect("Failed to migrate app pool");

    let admin_pool = pool_with(2).await;
    let queue = chaos_queue("chaos_disconnect");
    clean_queue(&app_pool, &queue).await;

    let client = Client::builder(app_pool.clone())
        .queue(
            &queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .heartbeat_interval(Duration::from_millis(50))
        .promote_interval(Duration::from_millis(50))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .register_worker(CompleteWorker)
        .build()
        .expect("Failed to build disconnect-recovery client");

    client
        .start()
        .await
        .expect("Failed to start disconnect-recovery client");

    for seq in 0..8_i64 {
        insert_with(
            &app_pool,
            &SimpleChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert first available wave");
    }

    wait_for_counts(
        &app_pool,
        &queue,
        |counts| state_count(counts, "completed") >= 4,
        Duration::from_secs(5),
    )
    .await;

    let terminated = terminate_application_backends(&admin_pool, &app_name).await;
    assert!(
        terminated > 0,
        "Expected to terminate at least one backend for app_name={app_name}"
    );

    for seq in 8..16_i64 {
        insert_with(
            &app_pool,
            &SimpleChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                run_at: Some(Utc::now() + ChronoDuration::milliseconds(200)),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert second scheduled wave");
    }

    let counts = wait_for_counts(
        &app_pool,
        &queue,
        |counts| {
            state_count(counts, "completed") == 16
                && state_count(counts, "scheduled") == 0
                && state_count(counts, "running") == 0
                && state_count(counts, "available") == 0
        },
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(state_count(&counts, "completed"), 16);

    let health = client.health_check().await;
    assert!(
        health.postgres_connected,
        "client should reconnect to Postgres"
    );
    assert!(
        health.poll_loop_alive,
        "dispatch loop should still be alive"
    );
    assert!(
        health.heartbeat_alive,
        "heartbeat loop should still be alive"
    );

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_leader_failover_during_scheduled_promotion() {
    let pool = setup(20).await;
    let queue = chaos_queue("chaos_promotion");
    clean_queue(&pool, &queue).await;

    let client_a = complete_client(pool.clone(), &queue);
    let client_b = complete_client(pool.clone(), &queue);
    client_a.start().await.expect("Failed to start client A");
    client_b.start().await.expect("Failed to start client B");

    let leader_idx = wait_for_single_leader(&[&client_a, &client_b], Duration::from_secs(5)).await;

    for seq in 0..12_i64 {
        insert_with(
            &pool,
            &SimpleChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                run_at: Some(Utc::now() + ChronoDuration::milliseconds(200)),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert first scheduled wave");
    }

    for seq in 12..24_i64 {
        insert_with(
            &pool,
            &SimpleChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                run_at: Some(Utc::now() + ChronoDuration::milliseconds(1000)),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert second scheduled wave");
    }

    wait_for_counts(
        &pool,
        &queue,
        |counts| state_count(counts, "completed") >= 12,
        Duration::from_secs(5),
    )
    .await;

    if leader_idx == 0 {
        client_a.shutdown(Duration::from_secs(5)).await;
    } else {
        client_b.shutdown(Duration::from_secs(5)).await;
    }

    let follower = if leader_idx == 0 {
        &client_b
    } else {
        &client_a
    };
    let follower_idx = wait_for_single_leader(&[follower], Duration::from_secs(5)).await;
    assert_eq!(
        follower_idx, 0,
        "Follower never became leader after failover"
    );

    let counts = wait_for_counts(
        &pool,
        &queue,
        |counts| {
            state_count(counts, "completed") == 24
                && state_count(counts, "scheduled") == 0
                && state_count(counts, "running") == 0
                && state_count(counts, "available") == 0
        },
        Duration::from_secs(10),
    )
    .await;

    assert_eq!(state_count(&counts, "completed"), 24);

    follower.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_leader_connection_loss_re_elects_and_finishes_scheduled_promotion() {
    let pool = setup(20).await;
    let queue = chaos_queue("chaos_conn_drop");
    clean_queue(&pool, &queue).await;

    let client_a = complete_client(pool.clone(), &queue);
    let client_b = complete_client(pool.clone(), &queue);
    client_a.start().await.expect("Failed to start client A");
    client_b.start().await.expect("Failed to start client B");

    let _ = wait_for_single_leader(&[&client_a, &client_b], Duration::from_secs(5)).await;

    for seq in 0..12_i64 {
        insert_with(
            &pool,
            &SimpleChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                run_at: Some(Utc::now() + ChronoDuration::milliseconds(200)),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert first scheduled wave");
    }

    for seq in 12..24_i64 {
        insert_with(
            &pool,
            &SimpleChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                run_at: Some(Utc::now() + ChronoDuration::milliseconds(900)),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert second scheduled wave");
    }

    wait_for_counts(
        &pool,
        &queue,
        |counts| state_count(counts, "completed") >= 12,
        Duration::from_secs(5),
    )
    .await;

    let leader_pid = current_leader_backend_pid(&pool)
        .await
        .expect("Expected an advisory lock holder before terminating the leader connection");
    terminate_backend(&pool, leader_pid).await;

    let new_leader_pid =
        wait_for_new_leader_backend_pid(&pool, leader_pid, Duration::from_secs(5)).await;
    let _ = wait_for_single_leader(&[&client_a, &client_b], Duration::from_secs(5)).await;
    assert_ne!(new_leader_pid, leader_pid);

    let counts = wait_for_counts(
        &pool,
        &queue,
        |counts| {
            state_count(counts, "completed") == 24
                && state_count(counts, "scheduled") == 0
                && state_count(counts, "running") == 0
                && state_count(counts, "available") == 0
        },
        Duration::from_secs(10),
    )
    .await;

    assert_eq!(state_count(&counts, "completed"), 24);

    client_a.shutdown(Duration::from_secs(5)).await;
    client_b.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_leader_failover_rescues_callback_timeouts() {
    let pool = setup(20).await;
    let queue = chaos_queue("chaos_callback_failover");
    clean_queue(&pool, &queue).await;

    let build_callback_client = |pool: sqlx::PgPool| {
        Client::builder(pool)
            .queue(
                &queue,
                QueueConfig {
                    max_workers: 4,
                    poll_interval: Duration::from_millis(25),
                    ..QueueConfig::default()
                },
            )
            .heartbeat_interval(Duration::from_millis(50))
            .promote_interval(Duration::from_millis(50))
            .callback_rescue_interval(Duration::from_millis(100))
            .leader_election_interval(Duration::from_millis(100))
            .leader_check_interval(Duration::from_millis(100))
            .register_worker(CallbackTimeoutWorker)
            .build()
            .expect("Failed to build callback failover client")
    };

    let client_a = build_callback_client(pool.clone());
    let client_b = build_callback_client(pool.clone());
    client_a.start().await.expect("Failed to start client A");
    client_b.start().await.expect("Failed to start client B");

    let leader_idx = wait_for_single_leader(&[&client_a, &client_b], Duration::from_secs(5)).await;

    for seq in 0..12_i64 {
        insert_with(
            &pool,
            &SimpleChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                max_attempts: 3,
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert callback chaos job");
    }

    wait_for_counts(
        &pool,
        &queue,
        |counts| state_count(counts, "waiting_external") == 12,
        Duration::from_secs(5),
    )
    .await;

    if leader_idx == 0 {
        client_a.shutdown(Duration::from_secs(5)).await;
    } else {
        client_b.shutdown(Duration::from_secs(5)).await;
    }

    let follower = if leader_idx == 0 {
        &client_b
    } else {
        &client_a
    };
    let follower_idx = wait_for_single_leader(&[follower], Duration::from_secs(5)).await;
    assert_eq!(
        follower_idx, 0,
        "Follower never became leader after failover"
    );

    // Backdate callback_timeout_at so the follower's rescue cycle picks them up.
    // The callbacks were registered with a very long timeout (1h) to avoid a
    // timing race where the original leader rescues them before we kill it.
    // Now that the leader is dead and the follower has taken over, we expire
    // the callbacks by moving their timeout into the past.
    sqlx::query(
        "UPDATE awa.jobs SET callback_timeout_at = now() - interval '1 second' \
         WHERE queue = $1 AND state = 'waiting_external'",
    )
    .bind(&queue)
    .execute(&pool)
    .await
    .expect("Failed to backdate callback_timeout_at");

    let counts = wait_for_counts(
        &pool,
        &queue,
        |counts| {
            state_count(counts, "completed") == 12
                && state_count(counts, "waiting_external") == 0
                && state_count(counts, "retryable") == 0
                && state_count(counts, "scheduled") == 0
                && state_count(counts, "running") == 0
        },
        Duration::from_secs(15),
    )
    .await;

    assert_eq!(state_count(&counts, "completed"), 12);

    let attempts: (Option<i16>, Option<i16>) = sqlx::query_as(
        "SELECT min(attempt), max(attempt) FROM awa.jobs WHERE queue = $1 AND state = 'completed'",
    )
    .bind(&queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to query callback failover attempts");
    assert_eq!(attempts.0, Some(2));
    assert_eq!(attempts.1, Some(2));

    follower.shutdown(Duration::from_secs(5)).await;
}

/// Full Postgres outage: terminate ALL application backends twice in succession,
/// then verify the client recovers and processes all jobs with correct metrics.
///
/// This is heavier than the targeted disconnect test — it simulates a sustained
/// Postgres restart by disrupting ALL connections, not just one backend.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_full_postgres_outage_recovers_with_metrics() {
    let app_name = format!("chaos_outage_{}", &Uuid::new_v4().simple().to_string()[..8]);
    let app_pool = pool_with_url(&database_url_with_app_name(&app_name), 20).await;
    migrations::run(&app_pool)
        .await
        .expect("Failed to migrate app pool");

    let admin_pool = pool_with(2).await;
    let queue = chaos_queue("chaos_outage");
    clean_queue(&app_pool, &queue).await;

    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let client = Client::builder(app_pool.clone())
        .queue(
            &queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .heartbeat_interval(Duration::from_millis(50))
        .promote_interval(Duration::from_millis(50))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .register_worker(CompleteWorker)
        .build()
        .expect("Failed to build outage-recovery client");

    client
        .start()
        .await
        .expect("Failed to start outage-recovery client");

    // Insert first wave and wait for partial completion.
    for seq in 0..8_i64 {
        insert_with(
            &app_pool,
            &SimpleChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert first wave job");
    }

    wait_for_counts(
        &app_pool,
        &queue,
        |counts| state_count(counts, "completed") >= 4,
        Duration::from_secs(5),
    )
    .await;

    // First outage: terminate ALL application backends.
    let terminated_1 = terminate_application_backends(&admin_pool, &app_name).await;
    assert!(
        terminated_1 > 0,
        "Expected to terminate backends in first outage"
    );
    eprintln!("First outage: terminated {terminated_1} backends");

    // Sustained outage: terminate again after a brief pause.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let terminated_2 = terminate_application_backends(&admin_pool, &app_name).await;
    eprintln!("Second outage: terminated {terminated_2} backends");

    // Insert second wave as scheduled jobs (tests promotion after recovery).
    for seq in 8..16_i64 {
        insert_with(
            &app_pool,
            &SimpleChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                run_at: Some(Utc::now() + ChronoDuration::milliseconds(300)),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to insert second wave job");
    }

    // Wait for full recovery: all 16 jobs completed.
    let counts = wait_for_counts(
        &app_pool,
        &queue,
        |counts| {
            state_count(counts, "completed") == 16
                && state_count(counts, "scheduled") == 0
                && state_count(counts, "running") == 0
                && state_count(counts, "available") == 0
        },
        Duration::from_secs(15),
    )
    .await;
    assert_eq!(state_count(&counts, "completed"), 16);

    // Health check: the client should have recovered.
    let health = client.health_check().await;
    assert!(
        health.postgres_connected,
        "Client should reconnect to Postgres after full outage"
    );
    assert!(
        health.poll_loop_alive,
        "Dispatch loop should survive full outage"
    );
    assert!(
        health.heartbeat_alive,
        "Heartbeat loop should survive full outage"
    );

    client.shutdown(Duration::from_secs(5)).await;

    // Flush and assert metrics survived the outage.
    meter_provider
        .force_flush()
        .expect("Failed to flush outage metrics");
    let resource_metrics = exporter
        .get_finished_metrics()
        .expect("Failed to read outage metrics");

    assert!(
        sum_counter_metric(&resource_metrics, "awa.job.completed") >= 16,
        "completed metric should account for all jobs after outage recovery"
    );
    assert!(
        sum_counter_metric(&resource_metrics, "awa.job.claimed") >= 16,
        "claimed metric should account for all jobs after outage recovery"
    );
    assert!(
        sum_counter_metric(&resource_metrics, "awa.dispatch.claim_batches") >= 2,
        "dispatch should have run claim batches across pre- and post-outage"
    );

    let _ = meter_provider.shutdown();
}
