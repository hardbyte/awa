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

async fn connect_pool_with(database_url: &str, max_connections: u32) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(5))
        .connect(database_url)
        .await
        .expect("failed to connect to failover test database")
}

async fn connect_pool(database_url: &str) -> sqlx::PgPool {
    connect_pool_with(database_url, 20).await
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
    if let Some(schema) = queue_storage_schema_for_counts(pool).await {
        let sql = format!(
            "WITH live_counts AS ( \
                 SELECT state::text, count(*)::bigint AS count \
                 FROM ( \
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
                     /* Compact receipt claims (#246) live in \
                        lease_claim_batches, not lease_claims — expand \
                        their members so a running claim is visible before \
                        it materialises into leases. Mirrors load_job. */ \
                     SELECT 'running'::awa.job_state AS state \
                     FROM {schema}.lease_claim_batches AS b \
                     CROSS JOIN LATERAL unnest( \
                         b.job_ids, b.run_leases, b.receipt_ids \
                     ) AS items(job_id, run_lease, receipt_id) \
                     WHERE b.queue = $1 \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM {schema}.lease_claim_closures AS cx \
                         WHERE cx.claim_slot = b.claim_slot \
                           AND cx.job_id = items.job_id \
                           AND cx.run_lease = items.run_lease \
                       ) \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM {schema}.lease_claim_closure_batches AS cb \
                         WHERE cb.claim_slot = b.claim_slot \
                           AND cb.receipt_ranges @> items.receipt_id \
                       ) \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM {schema}.leases AS lease \
                         WHERE lease.job_id = items.job_id \
                           AND lease.run_lease = items.run_lease \
                       ) \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM {schema}.deferred_jobs AS deferred \
                         WHERE deferred.job_id = items.job_id \
                           AND deferred.run_lease = items.run_lease \
                       ) \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM {schema}.terminal_jobs AS terminal \
                         WHERE terminal.job_id = items.job_id \
                           AND terminal.run_lease = items.run_lease \
                       ) \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM {schema}.dlq_entries AS dlq \
                         WHERE dlq.job_id = items.job_id \
                           AND dlq.run_lease = items.run_lease \
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
            .expect("failed to query queue-storage state counts");
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
    .expect("failed to query queue state counts");

    rows.into_iter().collect()
}

async fn active_queue_storage_schema(pool: &sqlx::PgPool) -> Option<String> {
    sqlx::query_scalar("SELECT awa.active_queue_storage_schema()")
        .fetch_one(pool)
        .await
        .expect("failed to resolve active queue storage schema")
}

async fn queue_storage_schema_for_counts(pool: &sqlx::PgPool) -> Option<String> {
    active_queue_storage_schema(pool).await
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
            "timed out waiting for queue {queue}; last counts: {counts:?}; storage: {}",
            storage_debug(pool, queue).await
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn storage_debug(pool: &sqlx::PgPool, queue: &str) -> String {
    let storage: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT current_engine, state, awa.active_queue_storage_schema() \
         FROM awa.storage_transition_state WHERE singleton",
    )
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    let canonical_count: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM awa.jobs WHERE queue = $1")
            .bind(queue)
            .fetch_one(pool)
            .await
            .unwrap_or(-1);

    let queue_storage_count = if let Some((_, _, Some(schema))) = &storage {
        let sql = format!(
            "SELECT \
                 (SELECT count(*)::bigint FROM {schema}.ready_entries WHERE queue = $1) + \
                 (SELECT count(*)::bigint FROM {schema}.deferred_jobs WHERE queue = $1) + \
                 (SELECT count(*)::bigint FROM {schema}.leases WHERE queue = $1) + \
                 (SELECT count(*)::bigint FROM {schema}.lease_claims WHERE queue = $1) + \
                 (SELECT count(*)::bigint FROM {schema}.lease_claim_closures AS cx \
                   JOIN {schema}.lease_claims AS lc \
                     ON lc.claim_slot = cx.claim_slot \
                    AND lc.job_id = cx.job_id \
                    AND lc.run_lease = cx.run_lease \
                  WHERE lc.queue = $1) + \
                 (SELECT count(*)::bigint FROM {schema}.lease_claim_closure_batches AS cb \
                   JOIN {schema}.lease_claims AS lc \
                     ON cb.receipt_ranges @> lc.receipt_id \
                  WHERE lc.queue = $1) + \
                 (SELECT count(*)::bigint FROM {schema}.terminal_jobs WHERE queue = $1) + \
                 (SELECT COALESCE( \
                     sum( \
                         GREATEST( \
                             COALESCE(lanes.pruned_completed_count, 0), \
                             COALESCE(rollups.pruned_completed_count, 0) \
                         ) \
                     ), \
                     0 \
                 )::bigint \
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
                  USING (queue, priority))"
        );
        sqlx::query_scalar::<_, i64>(&sql)
            .bind(queue)
            .fetch_one(pool)
            .await
            .unwrap_or(-1)
    } else {
        -1
    };

    format!(
        "transition={storage:?}, canonical_rows={canonical_count}, queue_storage_rows={queue_storage_count}"
    )
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

    let mut app_pool = wait_for_pool(&url, Duration::from_secs(30)).await;
    migrations::run(&app_pool)
        .await
        .expect("migrations should succeed through proxy");

    let queue = format!("failover_smoke_{}", Uuid::new_v4().simple());
    let client_pool = connect_pool(&url).await;
    let client = failover_client(client_pool, &queue);
    client.start().await.expect("client should start");

    app_pool.close().await;
    app_pool = connect_pool_with(&url, 1).await;

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

    app_pool.close().await;
    app_pool = connect_pool_with(&url, 1).await;

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
