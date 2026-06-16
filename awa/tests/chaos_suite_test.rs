//! Chaos and soak tests for longer-running failure-mode scenarios.
//!
//! These are intentionally ignored in normal CI and are meant for the slower
//! nightly/manual chaos lane.

use async_trait::async_trait;
use awa::model::{insert_with, migrations, InsertOpts, QueueStorageConfig};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use chrono::{Duration as ChronoDuration, Utc};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
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
    // Transition-era chaos tests may run against either canonical storage or
    // queue_storage depending on the runtime capabilities each process reports.
    // When queue_storage exists, include both planes so a mixed-language smoke
    // test does not accidentally wait on the wrong one.
    if let Some(schema) = queue_storage_schema_for_counts(pool).await {
        let sql = format!(
            "WITH live_counts AS ( \
                 SELECT state::text, count(*)::bigint AS count FROM ( \
                     SELECT 'available'::awa.job_state AS state \
                     FROM {schema}.ready_entries AS ready \
                     JOIN {schema}.queue_claim_heads AS claims \
                       ON claims.queue = ready.queue \
                      AND claims.priority = ready.priority \
                      AND claims.enqueue_shard = ready.enqueue_shard \
                     WHERE ready.queue = $1 \
                       AND ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name) \
                     UNION ALL \
                     SELECT state FROM {schema}.deferred_jobs WHERE queue = $1 \
                     UNION ALL \
                     SELECT state FROM {schema}.leases WHERE queue = $1 \
                     UNION ALL \
                     SELECT 'running'::awa.job_state AS state \
                     FROM {schema}.lease_claims AS lc \
                     WHERE lc.queue = $1 \
                       AND lc.closed_at IS NULL \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM {schema}.lease_claim_closures AS cx \
                         WHERE cx.claim_slot = lc.claim_slot \
                           AND cx.job_id = lc.job_id \
                           AND cx.run_lease = lc.run_lease \
                       ) \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM {schema}.lease_claim_closure_batches AS cb \
                         WHERE cb.receipt_ranges @> lc.receipt_id \
                       ) \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM {schema}.leases AS lease \
                         WHERE lease.job_id = lc.job_id \
                           AND lease.run_lease = lc.run_lease \
                       ) \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM {schema}.deferred_jobs AS deferred \
                         WHERE deferred.job_id = lc.job_id \
                           AND deferred.run_lease = lc.run_lease \
                       ) \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM {schema}.terminal_jobs AS terminal \
                         WHERE terminal.job_id = lc.job_id \
                           AND terminal.run_lease = lc.run_lease \
                       ) \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM {schema}.dlq_entries AS dlq \
                         WHERE dlq.job_id = lc.job_id \
                           AND dlq.run_lease = lc.run_lease \
                       ) \
                     UNION ALL \
                     SELECT state FROM {schema}.terminal_jobs WHERE queue = $1 \
                     UNION ALL \
                     SELECT state FROM {schema}.dlq_entries WHERE queue = $1 \
                 ) AS jobs \
                 GROUP BY state \
             ), \
             pruned_terminal AS ( \
                 SELECT 'completed'::text AS state, \
                        COALESCE( \
                            sum( \
                                GREATEST( \
                                    COALESCE(lanes.pruned_completed_count, 0), \
                                    COALESCE(rollups.pruned_completed_count, 0) \
                                ) \
                            ), \
                            0 \
                        )::bigint AS count \
                 FROM ( \
                     SELECT queue, priority, pruned_completed_count \
                     FROM {schema}.queue_lanes \
                     WHERE queue = $1 \
                 ) AS lanes \
                 FULL OUTER JOIN ( \
                     SELECT queue, priority, pruned_completed_count \
                     FROM {schema}.queue_terminal_rollups \
                     WHERE queue = $1 \
                 ) AS rollups \
                 USING (queue, priority) \
             ) \
             SELECT state, sum(count)::bigint AS count \
             FROM ( \
                 SELECT state, count FROM live_counts \
                 UNION ALL \
                 SELECT state, count FROM pruned_terminal WHERE count > 0 \
             ) AS counts \
             GROUP BY state"
        );
        let rows: Vec<(String, i64)> = sqlx::query_as(&sql)
            .bind(queue)
            .fetch_all(pool)
            .await
            .expect("Failed to query queue-storage state counts");
        return rows.into_iter().collect();
    }

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

async fn active_queue_storage_schema(pool: &sqlx::PgPool) -> Option<String> {
    sqlx::query_scalar("SELECT awa.active_queue_storage_schema()")
        .fetch_one(pool)
        .await
        .expect("Failed to resolve active queue storage schema")
}

async fn queue_storage_schema_for_counts(pool: &sqlx::PgPool) -> Option<String> {
    active_queue_storage_schema(pool).await
}

fn state_count(counts: &HashMap<String, i64>, state: &str) -> i64 {
    counts.get(state).copied().unwrap_or(0)
}

fn chaos_timeout_multiplier() -> f64 {
    if let Ok(raw) = std::env::var("AWA_CHAOS_TIMEOUT_MULTIPLIER") {
        if let Ok(parsed) = raw.parse::<f64>() {
            return parsed.max(1.0);
        }
    }

    if std::env::var_os("CI").is_some() {
        3.0
    } else {
        1.0
    }
}

fn scaled_timeout(timeout: Duration) -> Duration {
    timeout.mul_f64(chaos_timeout_multiplier())
}

async fn wait_for_counts(
    pool: &sqlx::PgPool,
    queue: &str,
    predicate: impl Fn(&HashMap<String, i64>) -> bool,
    timeout: Duration,
) -> HashMap<String, i64> {
    let timeout = scaled_timeout(timeout);
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

async fn wait_for_kind_state_count(
    pool: &sqlx::PgPool,
    queue: &str,
    kind: &str,
    state: &str,
    min_count: i64,
    timeout: Duration,
) -> i64 {
    let timeout = scaled_timeout(timeout);
    let start = Instant::now();
    loop {
        let count = kind_state_count(pool, queue, kind, state).await;

        if count >= min_count {
            return count;
        }

        assert!(
            start.elapsed() < timeout,
            "Timed out waiting for at least {min_count} {kind} jobs in state {state}; last count: {count}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn kind_state_count(pool: &sqlx::PgPool, queue: &str, kind: &str, state: &str) -> i64 {
    if state == "running" {
        if let Some(schema) = active_queue_storage_schema(pool).await {
            let sql = format!(
                r#"
                SELECT count(*)::bigint
                FROM (
                    SELECT 1
                    FROM {schema}.leases AS lease
                    JOIN {schema}.ready_entries AS ready
                      ON ready.ready_slot = lease.ready_slot
                     AND ready.ready_generation = lease.ready_generation
                     AND ready.queue = lease.queue
                     AND ready.priority = lease.priority
                     AND ready.enqueue_shard = lease.enqueue_shard
                     AND ready.lane_seq = lease.lane_seq
                     AND ready.job_id = lease.job_id
                    WHERE lease.queue = $1
                      AND ready.kind = $2
                      AND lease.state = 'running'

                    UNION ALL

                    SELECT 1
                    FROM {schema}.lease_claims AS claims
                    JOIN {schema}.ready_entries AS ready
                      ON ready.ready_slot = claims.ready_slot
                     AND ready.ready_generation = claims.ready_generation
                     AND ready.queue = claims.queue
                     AND ready.priority = claims.priority
                     AND ready.enqueue_shard = claims.enqueue_shard
                     AND ready.lane_seq = claims.lane_seq
                     AND ready.job_id = claims.job_id
                    WHERE claims.queue = $1
                      AND ready.kind = $2
                      AND claims.closed_at IS NULL
                      AND NOT EXISTS (
                        SELECT 1 FROM {schema}.lease_claim_closures AS closures
                        WHERE closures.claim_slot = claims.claim_slot
                          AND closures.job_id = claims.job_id
                          AND closures.run_lease = claims.run_lease
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM {schema}.lease_claim_closure_batches AS closure_batches
                        WHERE closure_batches.receipt_ranges @> claims.receipt_id
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM {schema}.leases AS lease
                        WHERE lease.job_id = claims.job_id
                          AND lease.run_lease = claims.run_lease
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM {schema}.terminal_jobs AS terminal
                        WHERE terminal.job_id = claims.job_id
                          AND terminal.run_lease = claims.run_lease
                      )
                ) AS running_jobs
                "#,
            );
            return sqlx::query_scalar(&sql)
                .bind(queue)
                .bind(kind)
                .fetch_one(pool)
                .await
                .expect("Failed to query queue-storage running kind count");
        }
    }

    sqlx::query_scalar(
        r#"
        SELECT count(*)::bigint
        FROM awa.jobs
        WHERE queue = $1
          AND kind = $2
          AND state = $3::awa.job_state
        "#,
    )
    .bind(queue)
    .bind(kind)
    .bind(state)
    .fetch_one(pool)
    .await
    .expect("Failed to query job kind state count")
}

async fn backdate_running_kind(pool: &sqlx::PgPool, queue: &str, kind: &str) -> u64 {
    if let Some(schema) = active_queue_storage_schema(pool).await {
        let receipt_sql = format!(
            r#"
            UPDATE {schema}.lease_claims AS claims
            SET claimed_at = clock_timestamp() - interval '10 minutes',
                deadline_at = LEAST(
                    COALESCE(deadline_at, 'infinity'::timestamptz),
                    clock_timestamp() - interval '1 minute'
                )
            FROM {schema}.ready_entries AS ready
            WHERE ready.ready_slot = claims.ready_slot
              AND ready.ready_generation = claims.ready_generation
              AND ready.queue = claims.queue
              AND ready.priority = claims.priority
              AND ready.enqueue_shard = claims.enqueue_shard
              AND ready.lane_seq = claims.lane_seq
              AND ready.job_id = claims.job_id
              AND claims.queue = $1
              AND ready.kind = $2
              AND claims.closed_at IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.lease_claim_closures AS closures
                WHERE closures.claim_slot = claims.claim_slot
                  AND closures.job_id = claims.job_id
                  AND closures.run_lease = claims.run_lease
              )
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.lease_claim_closure_batches AS closure_batches
                WHERE closure_batches.receipt_ranges @> claims.receipt_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.leases AS lease
                WHERE lease.job_id = claims.job_id
                  AND lease.run_lease = claims.run_lease
              )
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.terminal_jobs AS terminal
                WHERE terminal.job_id = claims.job_id
                  AND terminal.run_lease = claims.run_lease
              )
            "#,
        );
        let receipt_rows = sqlx::query(&receipt_sql)
            .bind(queue)
            .bind(kind)
            .execute(pool)
            .await
            .expect("Failed to backdate queue-storage receipt claims")
            .rows_affected();

        let lease_sql = format!(
            r#"
            UPDATE {schema}.leases AS leases
            SET heartbeat_at = clock_timestamp() - interval '10 minutes',
                deadline_at = LEAST(
                    COALESCE(deadline_at, 'infinity'::timestamptz),
                    clock_timestamp() - interval '1 minute'
                )
            FROM {schema}.ready_entries AS ready
            WHERE ready.ready_slot = leases.ready_slot
              AND ready.ready_generation = leases.ready_generation
              AND ready.queue = leases.queue
              AND ready.priority = leases.priority
              AND ready.enqueue_shard = leases.enqueue_shard
              AND ready.lane_seq = leases.lane_seq
              AND ready.job_id = leases.job_id
              AND leases.queue = $1
              AND ready.kind = $2
              AND leases.state = 'running'
            "#,
        );
        let lease_rows = sqlx::query(&lease_sql)
            .bind(queue)
            .bind(kind)
            .execute(pool)
            .await
            .expect("Failed to backdate queue-storage materialized leases")
            .rows_affected();

        return receipt_rows + lease_rows;
    }

    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET heartbeat_at = now() - interval '10 minutes',
            deadline_at = now() - interval '1 minute'
        WHERE queue = $1
          AND kind = $2
          AND state = 'running'
        "#,
    )
    .bind(queue)
    .bind(kind)
    .execute(pool)
    .await
    .expect("Failed to backdate canonical running jobs")
    .rows_affected()
}

async fn backdate_running_jobs(pool: &sqlx::PgPool, queue: &str) -> u64 {
    if let Some(schema) = active_queue_storage_schema(pool).await {
        let receipt_sql = format!(
            r#"
            UPDATE {schema}.lease_claims AS claims
            SET claimed_at = clock_timestamp() - interval '10 minutes',
                deadline_at = LEAST(
                    COALESCE(deadline_at, 'infinity'::timestamptz),
                    clock_timestamp() - interval '1 minute'
            )
            WHERE claims.queue = $1
              AND claims.closed_at IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.lease_claim_closures AS closures
                WHERE closures.claim_slot = claims.claim_slot
                  AND closures.job_id = claims.job_id
                  AND closures.run_lease = claims.run_lease
              )
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.lease_claim_closure_batches AS closure_batches
                WHERE closure_batches.receipt_ranges @> claims.receipt_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.leases AS lease
                WHERE lease.job_id = claims.job_id
                  AND lease.run_lease = claims.run_lease
              )
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.terminal_jobs AS terminal
                WHERE terminal.job_id = claims.job_id
                  AND terminal.run_lease = claims.run_lease
              )
            "#,
        );
        let receipt_rows = sqlx::query(&receipt_sql)
            .bind(queue)
            .execute(pool)
            .await
            .expect("Failed to backdate queue-storage receipt claims")
            .rows_affected();

        let lease_sql = format!(
            r#"
            UPDATE {schema}.leases
            SET heartbeat_at = clock_timestamp() - interval '10 minutes',
                deadline_at = LEAST(
                    COALESCE(deadline_at, 'infinity'::timestamptz),
                    clock_timestamp() - interval '1 minute'
                )
            WHERE queue = $1
              AND state = 'running'
            "#,
        );
        let lease_rows = sqlx::query(&lease_sql)
            .bind(queue)
            .execute(pool)
            .await
            .expect("Failed to backdate queue-storage materialized leases")
            .rows_affected();

        return receipt_rows + lease_rows;
    }

    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET heartbeat_at = now() - interval '10 minutes',
            deadline_at = now() - interval '1 minute'
        WHERE queue = $1
          AND state = 'running'
        "#,
    )
    .bind(queue)
    .execute(pool)
    .await
    .expect("Failed to backdate canonical running jobs")
    .rows_affected()
}

async fn backdate_retryable_kind(pool: &sqlx::PgPool, queue: &str, kind: &str) -> u64 {
    if let Some(schema) = active_queue_storage_schema(pool).await {
        let sql = format!(
            r#"
            UPDATE {schema}.deferred_jobs
            SET run_at = clock_timestamp() - interval '1 minute'
            WHERE queue = $1
              AND kind = $2
              AND state = 'retryable'
            "#,
        );
        return sqlx::query(&sql)
            .bind(queue)
            .bind(kind)
            .execute(pool)
            .await
            .expect("Failed to backdate queue-storage retryable jobs")
            .rows_affected();
    }

    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET run_at = now() - interval '1 minute'
        WHERE queue = $1
          AND kind = $2
          AND state = 'retryable'
        "#,
    )
    .bind(queue)
    .bind(kind)
    .execute(pool)
    .await
    .expect("Failed to backdate canonical retryable jobs")
    .rows_affected()
}

async fn backdate_callback_timeouts(pool: &sqlx::PgPool, queue: &str) -> u64 {
    if let Some(schema) = active_queue_storage_schema(pool).await {
        let sql = format!(
            r#"
            UPDATE {schema}.leases
            SET callback_timeout_at = clock_timestamp() - interval '1 second'
            WHERE queue = $1
              AND state = 'waiting_external'
            "#,
        );
        return sqlx::query(&sql)
            .bind(queue)
            .execute(pool)
            .await
            .expect("Failed to backdate queue-storage callback timeouts")
            .rows_affected();
    }

    sqlx::query(
        "UPDATE awa.jobs SET callback_timeout_at = now() - interval '1 second' \
         WHERE queue = $1 AND state = 'waiting_external'",
    )
    .bind(queue)
    .execute(pool)
    .await
    .expect("Failed to backdate canonical callback timeouts")
    .rows_affected()
}

async fn wait_for_single_leader(clients: &[&Client], timeout: Duration) -> usize {
    let timeout = scaled_timeout(timeout);
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

async fn wait_for_client_leader(client: &Client, timeout: Duration) {
    let timeout = scaled_timeout(timeout);
    let start = Instant::now();
    loop {
        if client.health_check().await.leader {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "Timed out waiting for follower to become leader"
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
    stdout_lines: mpsc::UnboundedReceiver<String>,
    stdout_reader: tokio::task::JoinHandle<()>,
}

impl PythonHelperProcess {
    async fn wait_for_line(&mut self, expected: &str, timeout: Duration) -> String {
        let timeout = scaled_timeout(timeout);
        let deadline = tokio::time::Instant::now() + timeout;
        let mut seen = Vec::new();
        loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out waiting for python helper output: {expected}\n{}",
                seen.join("\n")
            );
            let text = match tokio::time::timeout(
                Duration::from_millis(250),
                self.stdout_lines.recv(),
            )
            .await
            {
                Ok(Some(line)) => line,
                Ok(None) => {
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
                Err(_) => continue,
            };
            if text.starts_with("STDOUT_READ_ERROR ") {
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
            seen.push(text.clone());
            if text.contains(expected) {
                return text;
            }
        }
    }

    async fn stop(mut self) {
        self.stdout_reader.abort();
        if self.child.id().is_none() {
            return;
        }
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

impl Drop for PythonHelperProcess {
    fn drop(&mut self) {
        self.stdout_reader.abort();
        if self.child.id().is_some() {
            let _ = self.child.start_kill();
        }
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
    let (stdout_tx, stdout_lines) = mpsc::unbounded_channel();
    let stdout_reader = tokio::spawn(async move {
        let mut stdout = BufReader::new(stdout);
        loop {
            let mut line = String::new();
            match stdout.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    if stdout_tx.send(line.trim().to_string()).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    let _ = stdout_tx.send(format!("STDOUT_READ_ERROR {err}"));
                    break;
                }
            }
        }
    });

    PythonHelperProcess {
        child,
        stdout_lines,
        stdout_reader,
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
    let timeout = scaled_timeout(timeout);
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
    if !terminated.0 {
        // Backend already disconnected — same end result as terminating it.
        eprintln!("terminate_backend: pid={pid} already gone (race with pool recycling)");
    }
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
        for scope_metrics in rm.scope_metrics() {
            for metric in scope_metrics.metrics() {
                if metric.name() == name {
                    if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
                        total += sum.data_points().map(|dp| dp.value()).sum::<u64>();
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
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .heartbeat_staleness(Duration::from_millis(250))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .register_worker(CompleteWorker)
        .build()
        .expect("Failed to build complete client")
}

fn mixed_client(pool: sqlx::PgPool, queue: &str, queue_storage_schema: Option<&str>) -> Client {
    let mut builder = Client::builder(pool)
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
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(100))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .register_worker(CompleteWorker)
        .register_worker(MixedChaosWorker);

    if let Some(schema) = queue_storage_schema {
        builder = builder.queue_storage(
            QueueStorageConfig {
                schema: schema.to_string(),
                ..QueueStorageConfig::default()
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        );
    }

    builder.build().expect("Failed to build mixed chaos client")
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
            "retry_once_manual" => {
                if ctx.job.attempt == 1 {
                    // The test backdates run_at after all retryable rows are visible,
                    // which removes timing races from the retry path.
                    Ok(JobResult::RetryAfter(Duration::from_secs(3600)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "terminal_fail" => Err(JobError::terminal("intentional chaos failure")),
            "callback_timeout" => {
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
            "deadline_hang" => {
                if ctx.job.attempt == 1 {
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
            let callback = ctx
                .register_callback(Duration::from_secs(3600))
                .await
                .map_err(JobError::retryable)?;
            Ok(JobResult::WaitForCallback(callback))
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
    let active_queue_storage_schema = active_queue_storage_schema(&pool).await;

    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let mut builder = Client::builder(pool.clone())
        .queue(
            &queue,
            QueueConfig {
                max_workers: 8,
                poll_interval: Duration::from_millis(25),
                deadline_duration: Duration::from_millis(150),
                ..QueueConfig::default()
            },
        )
        .heartbeat_interval(Duration::from_millis(50))
        .promote_interval(Duration::from_millis(50))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(100))
        .leader_election_interval(Duration::from_millis(100))
        .register_worker(MixedChaosWorker);
    if let Some(schema) = active_queue_storage_schema {
        builder = builder.queue_storage(
            QueueStorageConfig {
                schema,
                ..QueueStorageConfig::default()
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        );
    }
    let client = builder.build().expect("Failed to build chaos client");

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
        Duration::from_secs(60),
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
    let waiting_external_metric = sum_counter_metric(&resource_metrics, "awa.job.waiting_external");
    if waiting_external_metric < per_mode as u64 {
        eprintln!(
            "waiting_external metric undercounted parked callback jobs: observed={waiting_external_metric} expected_at_least={per_mode}"
        );
    }
    // Use a lower bound for rescues — the in-memory exporter can undercount
    // increments across multiple maintenance batches even after force_flush.
    // The queue-state assertions above are the authoritative correctness check.
    assert!(
        sum_counter_metric(&resource_metrics, "awa.maintenance.rescues") >= per_mode as u64,
        "maintenance rescue metric did not record deadline + callback rescues"
    );

    let _ = meter_provider.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_sustained_mixed_workload_survives_repeated_node_failures() {
    let pool = setup(40).await;
    let admin_pool = pool_with(4).await;
    let queue = chaos_queue("chaos_node_fail");
    clean_queue(&pool, &queue).await;

    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let node_a_app = format!("chaos_node_a_{}", &Uuid::new_v4().simple().to_string()[..8]);
    let node_b_app = format!("chaos_node_b_{}", &Uuid::new_v4().simple().to_string()[..8]);
    let node_a_pool = pool_with_url(&database_url_with_app_name(&node_a_app), 20).await;
    let node_b_pool = pool_with_url(&database_url_with_app_name(&node_b_app), 20).await;
    let active_queue_storage_schema = active_queue_storage_schema(&pool).await;

    let client_a = mixed_client(
        node_a_pool.clone(),
        &queue,
        active_queue_storage_schema.as_deref(),
    );
    let client_b = mixed_client(
        node_b_pool.clone(),
        &queue,
        active_queue_storage_schema.as_deref(),
    );

    let mut python_env = vec![("MIXED_SIMPLE_SLEEP_MS", "2000".to_string())];
    if let Some(schema) = &active_queue_storage_schema {
        python_env.push(("MIXED_QS_SCHEMA", schema.clone()));
    }
    let mut python_worker =
        start_python_helper("worker_simple_chaos_job", &queue, &python_env).await;

    python_worker
        .wait_for_line(
            "READY mode=worker_simple_chaos_job",
            Duration::from_secs(10),
        )
        .await;

    insert_with(
        &pool,
        &SimpleChaosJob { seq: 0 },
        InsertOpts {
            queue: queue.clone(),
            max_attempts: 3,
            ..Default::default()
        },
    )
    .await
    .expect("Failed to insert sentinel simple chaos job");

    // Synchronize on the durable DB state instead of helper stdout: the sentinel
    // job reaching `running` proves the Python worker has claimed it. Avoids
    // pipe-buffered stderr scheduling flake from the helper.
    wait_for_kind_state_count(
        &pool,
        &queue,
        "simple_chaos_job",
        "running",
        1,
        Duration::from_secs(10),
    )
    .await;

    async fn insert_wave(pool: &sqlx::PgPool, queue: &str, seq: &mut i64) {
        for _ in 0..2 {
            insert_with(
                pool,
                &SimpleChaosJob { seq: *seq },
                InsertOpts {
                    queue: queue.to_string(),
                    max_attempts: 3,
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to insert sustained simple chaos job");
            *seq += 1;
        }

        for mode in ["complete", "complete", "terminal_fail", "retry_once_manual"] {
            insert_with(
                pool,
                &ChaosJob {
                    seq: *seq,
                    mode: mode.to_string(),
                },
                InsertOpts {
                    queue: queue.to_string(),
                    max_attempts: 3,
                    ..Default::default()
                },
            )
            .await
            .expect("Failed to insert sustained chaos job");
            *seq += 1;
        }
    }

    let total_waves = 4_i64;
    let mut seq = 1_i64;

    // Insert waves BEFORE starting Rust clients. Python is the only worker
    // running so it exclusively claims simple_chaos_jobs (400ms each). This
    // eliminates the race with Rust's instant CompleteWorker and guarantees
    // Python has in-flight jobs when we kill it.
    insert_wave(&pool, &queue, &mut seq).await;
    insert_wave(&pool, &queue, &mut seq).await;

    // Confirm Python is mid-execution on at least one simple job. The DB
    // predicate (`running` count) is the durable synchronization point that
    // makes the later rescue assertion independent of helper stdout scheduling.
    let running_before_kill = wait_for_kind_state_count(
        &pool,
        &queue,
        "simple_chaos_job",
        "running",
        1,
        Duration::from_secs(10),
    )
    .await;

    // Kill Python while it has in-flight simple jobs.
    python_worker.stop().await;

    // Now start Rust clients. They handle chaos_jobs from the waves and rescue
    // the simple_chaos_job left behind by the dead Python helper.
    client_a
        .start()
        .await
        .expect("Failed to start mixed chaos client A");
    client_b
        .start()
        .await
        .expect("Failed to start mixed chaos client B");

    let _ = wait_for_single_leader(&[&client_a, &client_b], Duration::from_secs(5)).await;

    let backdated = backdate_running_kind(&pool, &queue, "simple_chaos_job").await;
    assert!(
        backdated > 0,
        "Expected to backdate at least one Python-owned running simple job; running before kill: {running_before_kill}"
    );

    wait_for_counts(
        &pool,
        &queue,
        |counts| {
            state_count(counts, "retryable") >= 2
                && state_count(counts, "failed") >= 2
                && state_count(counts, "completed") >= 4
        },
        Duration::from_secs(15),
    )
    .await;

    let terminated = terminate_application_backends(&admin_pool, &node_a_app).await;
    assert!(
        terminated > 0,
        "Expected to terminate at least one backend for app_name={node_a_app}"
    );

    let reconnect_start = Instant::now();
    loop {
        let health = client_a.health_check().await;
        if health.postgres_connected {
            break;
        }
        assert!(
            reconnect_start.elapsed() < scaled_timeout(Duration::from_secs(10)),
            "Timed out waiting for node A to reconnect after backend termination"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    insert_wave(&pool, &queue, &mut seq).await;
    insert_wave(&pool, &queue, &mut seq).await;

    wait_for_counts(
        &pool,
        &queue,
        |counts| {
            state_count(counts, "retryable") == total_waves
                && state_count(counts, "failed") == total_waves
        },
        Duration::from_secs(15),
    )
    .await;

    backdate_running_jobs(&pool, &queue).await;

    backdate_retryable_kind(&pool, &queue, "chaos_job").await;

    // 1 sentinel + 5 per wave (2 simple + 2 complete + 1 retry_once_manual)
    let expected_completed = 1 + (total_waves * 5);
    let expected_failed = total_waves;

    let final_timeout = scaled_timeout(Duration::from_secs(45));
    let final_start = Instant::now();
    let counts = loop {
        // A final first-attempt retry can commit while this assertion loop is
        // already running. Keep forcing retryable rows due so the test does
        // not depend on the exact interleaving of the last completion batch.
        backdate_retryable_kind(&pool, &queue, "chaos_job").await;

        let counts = queue_state_counts(&pool, &queue).await;
        if state_count(&counts, "completed") == expected_completed
            && state_count(&counts, "failed") == expected_failed
            && state_count(&counts, "running") == 0
            && state_count(&counts, "available") == 0
            && state_count(&counts, "retryable") == 0
            && state_count(&counts, "scheduled") == 0
            && state_count(&counts, "waiting_external") == 0
        {
            break counts;
        }

        assert!(
            final_start.elapsed() < final_timeout,
            "Timed out waiting for queue {queue} counts; last counts: {counts:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(state_count(&counts, "completed"), expected_completed);
    assert_eq!(state_count(&counts, "failed"), expected_failed);

    let max_simple_attempt: Option<i16> = sqlx::query_scalar(
        r#"
        SELECT max(attempt)
        FROM awa.jobs
        WHERE queue = $1
          AND kind = 'simple_chaos_job'
          AND state = 'completed'
        "#,
    )
    .bind(&queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to query completed simple job attempts");
    assert!(
        max_simple_attempt.unwrap_or(0) >= 2,
        "Expected at least one simple job to be rescued after the Python node died"
    );

    let _ = wait_for_single_leader(&[&client_a, &client_b], Duration::from_secs(5)).await;

    let health_a = client_a.health_check().await;
    let health_b = client_b.health_check().await;
    assert!(
        health_a.postgres_connected,
        "Node A should reconnect after its backend connections are terminated"
    );
    assert!(
        health_a.poll_loop_alive,
        "Node A poll loop should stay alive"
    );
    assert!(
        health_a.heartbeat_alive,
        "Node A heartbeat should stay alive"
    );
    assert!(
        health_b.postgres_connected,
        "Node B should remain connected"
    );
    assert!(
        health_b.poll_loop_alive,
        "Node B poll loop should stay alive"
    );
    assert!(
        health_b.heartbeat_alive,
        "Node B heartbeat should stay alive"
    );

    client_a.shutdown(Duration::from_secs(5)).await;
    client_b.shutdown(Duration::from_secs(5)).await;

    meter_provider
        .force_flush()
        .expect("Failed to flush node failure chaos metrics");
    let resource_metrics = exporter
        .get_finished_metrics()
        .expect("Failed to read node failure chaos metrics");

    // Use a lower bound for the OTel metric — the in-memory exporter may miss
    // some increments if they were recorded in a batch that flushed before the
    // final force_flush. The DB wait_for_counts assertion above is the
    // authoritative completeness check.
    let metric_completed = sum_counter_metric(&resource_metrics, "awa.job.completed");
    assert!(
        metric_completed >= (expected_completed as u64 / 2),
        "completed metric ({metric_completed}) far below expected ({expected_completed})"
    );
    assert!(
        sum_counter_metric(&resource_metrics, "awa.job.failed") >= expected_failed as u64,
        "failed metric did not reflect sustained node-failure workload"
    );
    assert!(
        sum_counter_metric(&resource_metrics, "awa.maintenance.rescues") >= 1,
        "maintenance rescue metric did not record recovery from the dead Python node"
    );

    let _ = meter_provider.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_mixed_rust_and_python_workers_share_same_queue() {
    let pool = setup(20).await;
    let queue = chaos_queue("chaos_mixed_lang");
    clean_queue(&pool, &queue).await;

    let worker_count = 2_u32;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            &queue,
            QueueConfig {
                max_workers: worker_count,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .heartbeat_interval(Duration::from_millis(50))
        .promote_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .heartbeat_staleness(Duration::from_millis(250))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .register_worker(MixedFleetRustWorker { tx })
        .build()
        .expect("Failed to build mixed-fleet client");

    client
        .start()
        .await
        .expect("Failed to start mixed-fleet Rust client");

    let mut python_worker = start_python_helper(
        "worker_chaos_probe",
        &queue,
        &[("MIXED_QUEUE_WORKERS", worker_count.to_string())],
    )
    .await;

    let batch_size = 12_i64;
    let expected_rust_markers = expected_mixed_fleet_markers("rust", batch_size);
    let expected_python_markers = expected_mixed_fleet_markers("python", batch_size);
    let expected_markers = expected_rust_markers
        .union(&expected_python_markers)
        .cloned()
        .collect::<HashSet<_>>();

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

        let deadline = tokio::time::sleep(scaled_timeout(Duration::from_secs(60)));
        tokio::pin!(deadline);
        let mut completed_markers = HashSet::new();

        loop {
            if completed_markers == expected_markers {
                break;
            }

            tokio::select! {
                marker = rx.recv() => {
                    let marker = marker.expect("Rust mixed-fleet receiver closed unexpectedly");
                    assert!(
                        expected_markers.contains(&marker),
                        "Unexpected marker processed by Rust worker: {marker}"
                    );
                    assert!(
                        completed_markers.insert(marker.clone()),
                        "Marker completed more than once: {marker}"
                    );
                }
                line = python_worker.stdout_lines.recv() => {
                    let line = line.expect("Python mixed-fleet worker stdout closed unexpectedly");
                    if line.contains("COMPLETE mode=worker_chaos_probe") {
                        let marker = mixed_fleet_marker_from_line(&line)
                            .unwrap_or_else(|| panic!("Python completion line missing marker: {line}"))
                            .to_string();
                        assert!(
                            expected_markers.contains(&marker),
                            "Unexpected python worker completion line: {line}"
                        );
                        assert!(
                            completed_markers.insert(marker.clone()),
                            "Marker completed more than once: {marker}"
                        );
                    }
                }
                () = &mut deadline => {
                    let counts = queue_state_counts(&pool, &queue).await;
                    let missing_rust =
                        missing_mixed_fleet_markers(&expected_rust_markers, &completed_markers);
                    let missing_python =
                        missing_mixed_fleet_markers(&expected_python_markers, &completed_markers);
                    panic!(
                        "Timed out waiting for mixed-fleet completions; completed={}, missing_rust={missing_rust:?}, missing_python={missing_python:?}, state_counts={counts:?}",
                        completed_markers.len(),
                    );
                }
            }
        }

        let quiet_deadline = tokio::time::sleep(scaled_timeout(Duration::from_millis(250)));
        tokio::pin!(quiet_deadline);
        loop {
            tokio::select! {
                marker = rx.recv() => {
                    let marker = marker.expect("Rust mixed-fleet receiver closed unexpectedly");
                    assert!(
                        expected_markers.contains(&marker),
                        "Unexpected Rust marker processed after expected drain: {marker}"
                    );
                    assert!(
                        completed_markers.insert(marker.clone()),
                        "Marker completed more than once after expected drain: {marker}"
                    );
                }
                line = python_worker.stdout_lines.recv() => {
                    let line = line.expect("Python mixed-fleet worker stdout closed unexpectedly");
                    if line.contains("COMPLETE mode=worker_chaos_probe") {
                        let marker = mixed_fleet_marker_from_line(&line)
                            .unwrap_or_else(|| panic!("Python completion line missing marker: {line}"))
                            .to_string();
                        assert!(
                            expected_markers.contains(&marker),
                            "Unexpected Python marker processed after expected drain: {marker}"
                        );
                        assert!(
                            completed_markers.insert(marker.clone()),
                            "Marker completed more than once after expected drain: {marker}"
                        );
                    }
                }
                () = &mut quiet_deadline => break,
            }
        }

        python_worker.stop().await;
    }
    .await;

    client.shutdown(Duration::from_secs(5)).await;

    test_result
}

fn expected_mixed_fleet_markers(prefix: &str, count: i64) -> HashSet<String> {
    (0..count).map(|idx| format!("{prefix}-{idx}")).collect()
}

fn missing_mixed_fleet_markers(expected: &HashSet<String>, seen: &HashSet<String>) -> Vec<String> {
    let mut missing: Vec<_> = expected.difference(seen).cloned().collect();
    missing.sort();
    missing
}

fn mixed_fleet_marker_from_line(line: &str) -> Option<&str> {
    line.split("marker=")
        .nth(1)
        .and_then(|tail| tail.split_whitespace().next())
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
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .heartbeat_staleness(Duration::from_millis(250))
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
    wait_for_client_leader(follower, Duration::from_secs(5)).await;

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
    let active_queue_storage_schema = active_queue_storage_schema(&pool).await;

    let build_callback_client = |pool: sqlx::PgPool| {
        let mut builder = Client::builder(pool)
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
            .register_worker(CallbackTimeoutWorker);
        if let Some(schema) = &active_queue_storage_schema {
            builder = builder.queue_storage(
                QueueStorageConfig {
                    schema: schema.clone(),
                    ..QueueStorageConfig::default()
                },
                Duration::from_millis(1_000),
                Duration::from_millis(50),
            );
        }
        builder
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
    wait_for_client_leader(follower, Duration::from_secs(5)).await;

    // Backdate callback_timeout_at so the follower's rescue cycle picks them up.
    // The callbacks were registered with a very long timeout (1h) to avoid a
    // timing race where the original leader rescues them before we kill it.
    // Now that the leader is dead and the follower has taken over, we expire
    // the callbacks by moving their timeout into the past.
    let expired = backdate_callback_timeouts(&pool, &queue).await;
    assert_eq!(expired, 12, "expected to expire all waiting callbacks");

    // After leader failover, the follower must: win election, start rescue
    // timer, rescue 12 timed-out callbacks (retryable → promoted → claimed →
    // completed). On slow CI runners this chain can take >15s.
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
        Duration::from_secs(30),
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
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .heartbeat_staleness(Duration::from_millis(250))
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
