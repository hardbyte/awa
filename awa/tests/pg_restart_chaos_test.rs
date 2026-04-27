//! Real Postgres restart chaos.
//!
//! Issue #197 release-readiness item: the existing chaos suite uses
//! `pg_terminate_backend()` (TCP-drop) and `postgres_failover_smoke_test`
//! does a `docker compose restart` on a primary/replica/haproxy stack
//! aimed at hot-standby promotion. Neither tests the simpler-but-common
//! production failure mode: **the Postgres process actually dies and
//! comes back up on the same host** — operator-initiated `pg_ctl
//! restart`, kernel OOM, manual `docker restart pg`. This test exercises
//! that path.
//!
//! Each scenario:
//!   1. Spins up a dedicated postgres container (own name, own port).
//!   2. Migrates schema, starts an awa Client driving a small workload.
//!   3. Mid-flight, runs `docker restart <container>` — the postgres
//!      process is killed and comes back up.
//!   4. Asserts every enqueued job eventually completes (post-restart
//!      reconnection, no lost work).
//!
//! Marked `#[ignore]` so it runs only via the nightly-chaos workflow.
//! Each scenario is its own test fn so failures localise to the
//! storage shape that broke.
//!
//! Local run:
//! ```bash
//! cargo test -p awa --test pg_restart_chaos_test \
//!   -- --ignored --test-threads=1 --nocapture
//! ```

use async_trait::async_trait;
use awa::model::{insert::insert_with, migrations, QueueStorage, QueueStorageConfig};
use awa::worker::TransitionWorkerRole;
use awa::{Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::process::Command;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

// ── Container management ──────────────────────────────────────────────

/// A Postgres container scoped to one chaos test. Drop force-removes it.
///
/// Container is launched without `--rm` because `docker restart` stops
/// the container before re-starting it, and `--rm` fires container
/// removal on the stop half — leaving the start half nothing to start.
/// `Drop::drop` does `docker rm -f` to cover both happy-path and
/// panic-path teardown. The host port is pre-allocated via a
/// transient `TcpListener` so it survives `docker restart` (without
/// pre-allocation, `-p 0:5432` re-rolls the host port on restart and
/// the test's existing pool can't reconnect).
struct PgContainer {
    name: String,
    port: u16,
}

impl PgContainer {
    fn start() -> Self {
        let suffix = &Uuid::new_v4().simple().to_string()[..8];
        let name = format!("awa-pg-restart-chaos-{suffix}");

        // Pick a free host port up front (TcpListener bind+drop) so we
        // can pin docker to the same port across restart. Letting
        // docker pick (`-p 0:5432`) means the host port re-rolls on
        // restart — the test's existing pool then can't reconnect
        // because its connection strings cached the old port.
        // Production postgres restarts on the same port, which is
        // the realistic shape we want to exercise.
        let port = pick_free_port();
        let port_arg = format!("127.0.0.1:{port}:5432");

        // No `--rm` because `docker restart` stops the container
        // first, which fires `--rm` cleanup before the start half
        // runs — and the second start finds nothing left. The
        // `Drop` impl does `docker rm -f` instead.
        let status = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &name,
                "-p",
                &port_arg,
                "-e",
                "POSTGRES_PASSWORD=test",
                "-e",
                "POSTGRES_USER=postgres",
                "postgres:17.2-alpine",
            ])
            .status()
            .expect("docker run failed to invoke");
        assert!(status.success(), "docker run for {name} failed");

        wait_for_ready(&name, port);
        bootstrap_db(port);
        Self { name, port }
    }

    /// Trigger a real Postgres restart (process dies, container restarts
    /// it via the postgres image's entrypoint). All client connections
    /// are dropped; the process comes back ready in 1-3 seconds.
    ///
    /// The host port mapping for `0:5432` can change after a restart
    /// on some docker versions/storage drivers (e.g. with iptables
    /// rule reload). Re-read the published port and update `self.port`
    /// so callers using `database_url()` after restart see the new
    /// mapping. Existing sqlx pools whose connection strings cached
    /// the old port will fail until the test inserter retries via
    /// the bench's reconnection logic — that's the intended
    /// connection-loss exercise.
    fn restart(&self) {
        let status = Command::new("docker")
            .args(["restart", "--timeout", "2", &self.name])
            .status()
            .expect("docker restart failed to invoke");
        assert!(status.success(), "docker restart {} failed", self.name);
        wait_for_ready(&self.name, self.port);
    }

    fn database_url(&self) -> String {
        format!(
            "postgres://postgres:test@localhost:{port}/awa_test",
            port = self.port
        )
    }
}

impl Drop for PgContainer {
    fn drop(&mut self) {
        // Best-effort cleanup. Without --rm we have to force-remove
        // the container ourselves; if the test panicked mid-restart
        // and the container is in a weird state, swallow the error
        // so the panic surfaces.
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

fn pick_free_port() -> u16 {
    // Bind to 127.0.0.1:0 so the kernel picks a free port, then drop
    // the listener and trust docker to bind it before anything else
    // grabs it. Tiny race window in practice, well below test
    // overhead. Several short-lived test bind cycles is the cost
    // we're paying to keep the port stable across restart.
    use std::net::TcpListener;
    let listener =
        TcpListener::bind("127.0.0.1:0").expect("could not bind ephemeral port for postgres");
    let port = listener
        .local_addr()
        .expect("local_addr on ephemeral listener")
        .port();
    drop(listener);
    port
}

fn wait_for_ready(name: &str, port: u16) {
    // Three-stage probe: container running, pg_isready inside, AND a
    // real `SELECT 1` query from the host. Without the last stage we
    // can race the postgres entrypoint's "ready" signal with its
    // accept-connections moment and get "server closed connection
    // unexpectedly" on the first real query. Generous 60s timeout
    // for slow CI runners.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last_status = String::new();
    loop {
        let running = Command::new("docker")
            .args(["inspect", "--format", "{{.State.Running}}", name])
            .output();
        if let Ok(o) = running.as_ref() {
            let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if s != "true" {
                last_status = format!("container not running: {s}");
            }
        }

        let inside = Command::new("docker")
            .args(["exec", name, "pg_isready", "-U", "postgres"])
            .output();
        let inside_ok = match inside {
            Ok(o) => o.status.success(),
            Err(_) => false,
        };

        // Real query check: psql will surface "FATAL: the database
        // system is starting up" until postgres has fully recovered.
        let psql_ok = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                "host",
                "-e",
                "PGPASSWORD=test",
                "postgres:17.2-alpine",
                "psql",
                "-h",
                "localhost",
                "-p",
                &port.to_string(),
                "-U",
                "postgres",
                "-tA",
                "-c",
                "SELECT 1",
            ])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        if !inside_ok {
            last_status = "pg_isready not yet ready".into();
        }
        if !psql_ok {
            last_status = format!("{last_status} | SELECT 1 not yet succeeding");
        }
        if inside_ok && psql_ok {
            return;
        }
        if Instant::now() >= deadline {
            panic!("postgres in {name} (port {port}) never became ready within 60s; {last_status}");
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn bootstrap_db(port: u16) {
    // Use a fresh process (not sqlx — keeps the test's pool clean and
    // avoids racing migrations during pool warmup).
    let status = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--network",
            "host",
            "-e",
            "PGPASSWORD=test",
            "postgres:17.2-alpine",
            "psql",
            "-h",
            "localhost",
            "-p",
            &port.to_string(),
            "-U",
            "postgres",
            "-c",
            "CREATE DATABASE awa_test",
        ])
        .status()
        .expect("psql failed to invoke");
    assert!(status.success(), "CREATE DATABASE awa_test failed");
}

// ── Workload helpers ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct PgRestartChaosJob {
    seq: i64,
}

/// Worker that counts handler invocations into a shared atomic. The
/// counter is the engine-agnostic source of truth for "work actually
/// done": queue_storage's done_entries get truncated by partition
/// rotation and queue_storage's awa.jobs view stops surfacing
/// completed rows once their done_entries partition rotates out, so
/// counting at the DB layer is a moving target. The handler counter
/// is monotonic and exact.
#[derive(Clone)]
struct ChaosWorker {
    handled: Arc<AtomicI64>,
}

#[async_trait]
impl Worker for ChaosWorker {
    fn kind(&self) -> &'static str {
        "pg_restart_chaos_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        self.handled.fetch_add(1, Ordering::Relaxed);
        Ok(JobResult::Completed)
    }
}

async fn wait_for_handled(handled: &AtomicI64, target: i64, timeout: Duration) -> i64 {
    let deadline = Instant::now() + timeout;
    let mut last = 0i64;
    while Instant::now() < deadline {
        let n = handled.load(Ordering::Relaxed);
        if n >= target {
            return n;
        }
        last = n;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    last
}

/// Configuration for one chaos scenario.
struct Scenario {
    name: &'static str,
    /// Whether to install queue storage. The Auto role plus
    /// auto-finalize migration v012 promotes a fresh DB straight to
    /// queue_storage active state. Canonical-only scenarios aren't
    /// included in this file (see header comment); both supported
    /// shapes here have queue_storage=true.
    queue_storage: bool,
    /// Whether to use the receipt-plane fast path (zero deadline).
    receipt_plane: bool,
}

async fn run_scenario(scenario: Scenario, jobs_pre_restart: i64, jobs_post_restart: i64) {
    let pg = PgContainer::start();
    let database_url = pg.database_url();
    eprintln!(
        "[{}] container={} port={} url={}",
        scenario.name, pg.name, pg.port, database_url
    );

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await
        .expect("connect");
    migrations::run(&pool).await.expect("migrations");
    let status: (String, String, Option<String>) = sqlx::query_as(
        "SELECT state, current_engine, prepared_engine FROM awa.storage_transition_state WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or_default();
    eprintln!(
        "[{}] migrations done; storage_status state={} current={} prepared={:?}",
        scenario.name, status.0, status.1, status.2
    );

    let queue = format!("pg_restart_{}", scenario.name);
    let schema = format!("awa_pg_restart_{}", scenario.name);

    if scenario.queue_storage {
        // Pre-install queue-storage schema; the Auto role's auto-finalize
        // path (commit 2457aac) will promote on first start since this is
        // a fresh DB.
        let store = QueueStorage::new(QueueStorageConfig {
            schema: schema.clone(),
            ..Default::default()
        })
        .expect("queue storage config");
        store.install(&pool).await.expect("schema install");
    }

    // deadline_duration: zero unlocks the receipt-plane fast path on
    // queue_storage; default (60s) for everything else so heartbeat-rescue
    // doesn't yank legitimately-running jobs out from under the worker
    // during the brief restart window.
    let mut queue_config = QueueConfig {
        max_workers: 4,
        poll_interval: Duration::from_millis(25),
        ..QueueConfig::default()
    };
    if scenario.receipt_plane {
        queue_config.deadline_duration = Duration::ZERO;
    }
    let mut builder = Client::builder(pool.clone()).queue(&queue, queue_config);
    if scenario.queue_storage {
        // Push rotate intervals past the test's wall-clock window so
        // completed jobs in done_entries / lease_claim_closures don't
        // get truncated mid-test. Without this, the "all jobs reached
        // terminal state" assertion below sees a moving target as
        // partition rotation prunes completed rows out of awa.jobs.
        // Same trick the queue_storage_runtime_test fixture uses
        // (line 362, claim_rotate_interval=60s).
        builder = builder
            .queue_storage(
                QueueStorageConfig {
                    schema: schema.clone(),
                    lease_claim_receipts: scenario.receipt_plane,
                    ..Default::default()
                },
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .claim_rotate_interval(Duration::from_secs(60));
    }
    let handled = Arc::new(AtomicI64::new(0));
    let client = builder
        .transition_role(TransitionWorkerRole::Auto)
        .heartbeat_interval(Duration::from_millis(100))
        .promote_interval(Duration::from_millis(50))
        .leader_election_interval(Duration::from_millis(200))
        .leader_check_interval(Duration::from_millis(100))
        .register_worker(ChaosWorker {
            handled: handled.clone(),
        })
        .build()
        .expect("build client");
    client.start().await.expect("client start");
    eprintln!("[{}] client started", scenario.name);

    // Pre-restart wave. Wait for some handler invocations so we know
    // the pipeline is live before restarting.
    for seq in 0..jobs_pre_restart {
        let res = insert_with(
            &pool,
            &PgRestartChaosJob { seq },
            InsertOpts {
                queue: queue.clone(),
                ..Default::default()
            },
        )
        .await;
        if let Err(e) = res {
            panic!("[{}] insert failed: {e}", scenario.name);
        }
    }
    let pre = wait_for_handled(&handled, jobs_pre_restart / 2, Duration::from_secs(15)).await;
    eprintln!(
        "[{}] pre-restart: {} completed (target was {})",
        scenario.name,
        pre,
        jobs_pre_restart / 2
    );
    assert!(
        pre >= jobs_pre_restart / 2,
        "[{}] pipeline never came online: completed={}",
        scenario.name,
        pre
    );

    // The actual restart. Postgres process dies, container restarts it,
    // listener comes back. All client connections drop.
    let restart_at = Instant::now();
    pg.restart();
    let restart_took = restart_at.elapsed();
    eprintln!(
        "[{}] postgres restart took {} ms",
        scenario.name,
        restart_took.as_millis()
    );

    // Post-restart wave. Insert MORE jobs. The pool's connections were
    // killed during restart; they reconnect lazily on next acquire.
    for seq in jobs_pre_restart..(jobs_pre_restart + jobs_post_restart) {
        // The first few inserts after restart can fail with connection
        // errors before sqlx replaces the killed connections in the pool.
        // Retry with backoff for up to 5s.
        let post_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match insert_with(
                &pool,
                &PgRestartChaosJob { seq },
                InsertOpts {
                    queue: queue.clone(),
                    ..Default::default()
                },
            )
            .await
            {
                Ok(_) => break,
                Err(_) if Instant::now() < post_deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => panic!(
                    "[{}] post-restart insert kept failing past deadline: {e}",
                    scenario.name
                ),
            }
        }
    }

    // All jobs (pre + post) must complete. Generous timeout because the
    // worker pool also has to reconnect and the heartbeat-rescue cycle
    // may need to fire on any in-flight-at-restart job.
    //
    // Note: the worker may invoke the handler more than `total_target`
    // times if a job that was running at restart-time gets rescued
    // and re-claimed. The assertion checks `>= total_target` rather
    // than `==` to allow for at-least-once delivery semantics across
    // a restart. The chaos test for at-most-once is a separate
    // concern.
    let total_target = jobs_pre_restart + jobs_post_restart;
    let final_count = wait_for_handled(&handled, total_target, Duration::from_secs(30)).await;
    eprintln!(
        "[{}] post-restart final completion: {}/{} (restart took {} ms)",
        scenario.name,
        final_count,
        total_target,
        restart_took.as_millis()
    );
    client.shutdown(Duration::from_secs(5)).await;
    assert!(
        final_count >= total_target,
        "[{}] not all jobs completed after pg restart: {}/{}",
        scenario.name,
        final_count,
        total_target
    );
}

// ── Scenarios ────────────────────────────────────────────────────────

// NOTE: canonical-engine PG-restart coverage is intentionally not in
// this file. The canonical dispatch path is being deprecated by 0.6
// (queue_storage is the default and auto-finalize promotes fresh
// installs straight to it), and the existing
// `chaos_suite_test::test_full_postgres_outage_recovers_with_metrics`
// already exercises the canonical-mode connection-loss recovery via
// `pg_terminate_backend()`. Adding a third canonical scenario here
// duplicates that coverage without testing anything 0.6 actually
// ships. The two scenarios that do live here cover the storage
// shapes 0.6 commits to.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_pg_restart_queue_storage() {
    if env::var("DISABLE_PG_RESTART_CHAOS").is_ok() {
        eprintln!("DISABLE_PG_RESTART_CHAOS set; skipping");
        return;
    }
    run_scenario(
        Scenario {
            name: "queue_storage",
            queue_storage: true,
            receipt_plane: false,
        },
        20,
        20,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_pg_restart_receipt_plane() {
    if env::var("DISABLE_PG_RESTART_CHAOS").is_ok() {
        eprintln!("DISABLE_PG_RESTART_CHAOS set; skipping");
        return;
    }
    run_scenario(
        Scenario {
            name: "receipt_plane",
            queue_storage: true,
            receipt_plane: true,
        },
        20,
        20,
    )
    .await;
}
