//! Smoke test for Postgres hot-standby promotion behind a stable proxy endpoint.
//!
//! This is intentionally ignored in the normal test suite because it requires
//! Docker Compose and boots a primary/replica stack on demand.

use async_trait::async_trait;
use awa::model::{insert_with, migrations, InsertOpts};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use uuid::Uuid;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root should exist")
        .to_path_buf()
}

fn compose_file() -> PathBuf {
    repo_root().join("docker/failover-smoke/compose.yml")
}

fn project_name() -> String {
    format!("awa-failover-{}", Uuid::new_v4().simple())
}

fn database_url(port: u16) -> String {
    format!("postgres://postgres:test@127.0.0.1:{port}/awa_failover_test")
}

fn run_command(mut command: Command, context: &str) -> String {
    let output = command
        .output()
        .unwrap_or_else(|err| panic!("{context} failed to start: {err}"));
    assert!(
        output.status.success(),
        "{context} failed with status {}.\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8(output.stdout).expect("command output should be utf-8")
}

fn try_command(mut command: Command) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|err| format!("failed to start command: {err}"))?;
    if !output.status.success() {
        return Err(format!(
            "status {}.\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        ));
    }
    String::from_utf8(output.stdout).map_err(|err| format!("utf8 decode failed: {err}"))
}

fn docker_compose(project: &str, args: &[&str]) -> Command {
    let mut command = Command::new("docker");
    command.arg("compose");
    command.arg("-f").arg(compose_file());
    command.arg("-p").arg(project);
    command.args(args);
    command.current_dir(repo_root());
    command
}

struct ComposeStack {
    project: String,
}

impl ComposeStack {
    fn up() -> Self {
        let project = project_name();
        run_command(
            docker_compose(&project, &["up", "-d", "--build"]),
            "docker compose up",
        );
        Self { project }
    }

    fn proxy_port(&self) -> u16 {
        let output = run_command(
            docker_compose(&self.project, &["port", "haproxy", "5432"]),
            "docker compose port haproxy",
        );
        let binding = output.trim();
        let port = binding
            .rsplit(':')
            .next()
            .expect("compose port output should contain a port");
        port.parse::<u16>()
            .expect("compose port output should end in a valid port")
    }

    fn stop_primary(&self) {
        run_command(
            docker_compose(&self.project, &["stop", "primary"]),
            "docker compose stop primary",
        );
    }

    fn primary_wal_lsn(&self) -> String {
        run_command(
            docker_compose(
                &self.project,
                &[
                    "exec",
                    "-T",
                    "primary",
                    "sh",
                    "-lc",
                    "PGPASSWORD=test psql -U postgres -d awa_failover_test -At -c \"SELECT pg_current_wal_lsn()\"",
                ],
            ),
            "docker compose exec primary pg_current_wal_lsn",
        )
        .trim()
        .to_string()
    }

    fn replica_has_replayed(&self, lsn: &str) -> bool {
        let output = try_command(
            docker_compose(
                &self.project,
                &[
                    "exec",
                    "-T",
                    "replica",
                    "sh",
                    "-lc",
                    &format!(
                        "PGPASSWORD=test psql -U postgres -d awa_failover_test -At -c \"SELECT pg_last_wal_replay_lsn() >= '{lsn}'::pg_lsn\""
                    ),
                ],
            ),
        );
        matches!(output.as_deref(), Ok("t\n") | Ok("t"))
    }

    fn promote_replica(&self) {
        run_command(
            docker_compose(
                &self.project,
                &[
                    "exec",
                    "-T",
                    "replica",
                    "sh",
                    "-lc",
                    "PGPASSWORD=test psql -U postgres -d awa_failover_test -c \"SELECT pg_promote(wait_seconds => 30);\"",
                ],
            ),
            "docker compose exec replica pg_promote",
        );
    }
}

impl Drop for ComposeStack {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .arg("compose")
            .arg("-f")
            .arg(compose_file())
            .arg("-p")
            .arg(&self.project)
            .args(["down", "-v", "--remove-orphans"])
            .current_dir(repo_root())
            .status();
    }
}

async fn connect_pool(database_url: &str) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(5))
        .connect(database_url)
        .await
        .expect("failed to connect to failover test database")
}

async fn wait_for_pool(database_url: &str, timeout: Duration) -> sqlx::PgPool {
    let start = Instant::now();
    loop {
        match PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(2))
            .connect(database_url)
            .await
        {
            Ok(pool) => return pool,
            Err(err) => {
                assert!(
                    start.elapsed() < timeout,
                    "timed out connecting to failover test database: {err}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

async fn wait_for_replica_replay(stack: &ComposeStack, lsn: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        if stack.replica_has_replayed(lsn) {
            return;
        }

        assert!(
            start.elapsed() < timeout,
            "timed out waiting for replica to replay primary LSN {lsn}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_writable(database_url: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        if let Ok(pool) = PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(2))
            .connect(database_url)
            .await
        {
            let writable = sqlx::query_scalar::<_, bool>("SELECT NOT pg_is_in_recovery()")
                .fetch_one(&pool)
                .await
                .unwrap_or(false);
            if writable {
                pool.close().await;
                return;
            }
            pool.close().await;
        }

        assert!(
            start.elapsed() < timeout,
            "timed out waiting for promoted writable database"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
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
    .expect("failed to query queue state counts");

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
            "timed out waiting for queue {queue}; last counts: {counts:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_client_postgres_recovery(client: &Client, timeout: Duration) {
    let start = Instant::now();
    loop {
        let health = client.health_check().await;
        if health.postgres_connected
            && health.poll_loop_alive
            && health.heartbeat_alive
            && health.maintenance_alive
        {
            return;
        }

        assert!(
            start.elapsed() < timeout,
            "timed out waiting for client recovery; last health: {:?}",
            health
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct FailoverJob {
    seq: i64,
}

struct CompleteWorker;

#[async_trait]
impl Worker for CompleteWorker {
    fn kind(&self) -> &'static str {
        "failover_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::Completed)
    }
}

fn failover_client(pool: sqlx::PgPool, queue: &str) -> Client {
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
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(100))
        .register_worker(CompleteWorker)
        .build()
        .expect("failed to build failover smoke client")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose"]
async fn test_postgres_hot_standby_promotion_keeps_awa_working() {
    let stack = ComposeStack::up();
    let proxy_port = stack.proxy_port();
    let url = database_url(proxy_port);

    let app_pool = wait_for_pool(&url, Duration::from_secs(30)).await;
    migrations::run(&app_pool)
        .await
        .expect("migrations should succeed through proxy");

    let queue = format!("failover_smoke_{}", Uuid::new_v4().simple());
    let client_pool = connect_pool(&url).await;
    let client = failover_client(client_pool, &queue);
    client.start().await.expect("client should start");

    for seq in 0..8_i64 {
        insert_with(
            &app_pool,
            &FailoverJob { seq },
            InsertOpts {
                queue: queue.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("initial insert should succeed");
    }

    wait_for_counts(
        &app_pool,
        &queue,
        |counts| state_count(counts, "completed") == 8,
        Duration::from_secs(10),
    )
    .await;

    let synced_lsn = stack.primary_wal_lsn();
    wait_for_replica_replay(&stack, &synced_lsn, Duration::from_secs(30)).await;

    stack.stop_primary();
    stack.promote_replica();
    wait_for_writable(&url, Duration::from_secs(30)).await;
    wait_for_client_postgres_recovery(&client, Duration::from_secs(30)).await;

    for seq in 8..16_i64 {
        insert_with(
            &app_pool,
            &FailoverJob { seq },
            InsertOpts {
                queue: queue.clone(),
                run_at: Some(Utc::now() + ChronoDuration::milliseconds(200)),
                ..Default::default()
            },
        )
        .await
        .expect("post-promotion insert should succeed");
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
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(state_count(&counts, "completed"), 16);
    client.shutdown(Duration::from_secs(5)).await;
}
