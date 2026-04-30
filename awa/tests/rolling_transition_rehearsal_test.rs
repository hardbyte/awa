//! Rolling transition rehearsal — P0 release-readiness item from issue #197.
//!
//! The 0.6 storage transition story commits to: an operator can move a
//! live cluster from canonical → prepared → mixed_transition → active
//! while producers and workers keep flowing. Existing tests cover each
//! of those state transitions individually, and the chaos suite covers
//! crash recovery, but nothing has driven the full flow end-to-end with
//! a continuous producer and both Rust and Python workers in the fleet.
//! This test does that.
//!
//! What's exercised:
//!
//!   1. Canonical baseline. Producer is enqueueing jobs, an Auto-role
//!      Rust worker and an Auto-role Python worker are processing them
//!      via the canonical engine. Storage state = canonical.
//!
//!   2. Prepare. Operator runs `storage::prepare(queue_storage)`.
//!      State → prepared. Auto workers stay canonical (the role's
//!      contract). Producer keeps producing canonical jobs.
//!
//!   3. Schema install + queue-storage-target worker. The
//!      `enter_mixed_transition` SQL gate requires a live
//!      queue_storage-capable runtime; we install the queue-storage
//!      schema and start a Rust worker with role=QueueStorageTarget
//!      so it heartbeats with the new capability.
//!
//!   4. Mixed transition. Operator runs `storage::enter_mixed_transition()`.
//!      State → mixed_transition. New writes route to queue_storage.
//!      Auto workers (Rust + Python) flip to canonical_drain_only and
//!      finish what's left. Producer keeps producing — those jobs go
//!      to queue_storage. The QueueStorageTarget worker processes them.
//!
//!   5. Drain canonical. Wait until canonical backlog (jobs_hot +
//!      scheduled_jobs) is zero.
//!
//!   6. Finalize. Operator runs `storage::finalize()`. State → active.
//!
//!   7. Stable on queue_storage. Producer keeps producing for a brief
//!      tail period; jobs continue to land and complete via the
//!      QueueStorageTarget worker.
//!
//!   8. Drain. Stop producer. Wait for in-flight to settle.
//!
//! Final assertion: total seeded count was reached or exceeded by the
//! sum of Rust + Python handler invocations. We use ≥ rather than ==
//! because the heartbeat-rescue cycle can re-execute a job that was
//! in flight at the routing flip; that's at-least-once semantics, not
//! a bug.
//!
//! Marked `#[ignore]` so it runs only via the nightly-chaos workflow.
//! Local run:
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
//!   cargo test -p awa --test rolling_transition_rehearsal_test \
//!     -- --ignored --nocapture
//! ```

use async_trait::async_trait;
use awa::model::{insert::insert_with, migrations, storage, QueueStorage, QueueStorageConfig};
use awa::worker::TransitionWorkerRole;
use awa::{Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use uuid::Uuid;

fn database_url() -> String {
    env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("manifest dir has parent")
        .to_path_buf()
}

fn python_test_bin() -> PathBuf {
    env::var_os("AWA_PYTHON_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("awa-python/.venv/bin/python"))
}

fn mixed_fleet_helper_path() -> PathBuf {
    workspace_root().join("awa-python/tests/mixed_fleet_helper.py")
}

// SimpleChaosJob is what the Python helper script's
// `worker_simple_chaos_job` mode is wired up for; reusing it keeps us
// on the existing fleet helper rather than minting a new one.
#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct SimpleChaosJob {
    seq: i64,
}

#[derive(Clone)]
struct CountingWorker {
    handled: Arc<AtomicI64>,
}

#[async_trait]
impl Worker for CountingWorker {
    fn kind(&self) -> &'static str {
        "simple_chaos_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        self.handled.fetch_add(1, Ordering::Relaxed);
        Ok(JobResult::Completed)
    }
}

struct PythonHelper {
    child: Child,
    handled_seqs: Arc<std::sync::Mutex<std::collections::HashSet<i64>>>,
    _stdout_reader: tokio::task::JoinHandle<()>,
}

impl PythonHelper {
    fn handler_count(&self) -> i64 {
        self.handled_seqs.lock().unwrap().len() as i64
    }

    async fn shutdown(mut self) {
        let _ = self.child.kill().await;
    }
}

async fn spawn_python_worker(
    queue: &str,
    qs_schema: &str,
    ready_tx: mpsc::UnboundedSender<()>,
) -> PythonHelper {
    let python = python_test_bin();
    assert!(
        python.exists(),
        "Python interpreter not found at {}; build awa-python venv or set AWA_PYTHON_BIN",
        python.display()
    );
    let script = mixed_fleet_helper_path();
    assert!(
        script.exists(),
        "Mixed-fleet helper not found at {}",
        script.display()
    );

    let mut command = Command::new(python);
    command
        .arg(script)
        .env("DATABASE_URL", database_url())
        .env("MIXED_QUEUE", queue)
        .env("MIXED_MODE", "worker_simple_chaos_job")
        .env("MIXED_SIMPLE_SLEEP_MS", "1")
        .env("MIXED_LEADER_ELECTION_INTERVAL_MS", "200")
        // Register as queue_storage-capable so the mixed_transition
        // gate is satisfied when we flip state mid-test.
        .env("MIXED_QS_SCHEMA", qs_schema)
        .env("MIXED_TRANSITION_ROLE", "auto")
        .env("PYTHONUNBUFFERED", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);

    let mut child = command.spawn().expect("Failed to spawn python helper");
    let stdout = child.stdout.take().expect("python helper stdout missing");
    let handled_seqs = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let handled_seqs_reader = handled_seqs.clone();

    let stdout_reader = tokio::spawn(async move {
        let mut reader = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            // The helper prints lines like:
            //   READY mode=worker_simple_chaos_job pid=N sleep_ms=1
            //   COMPLETE mode=worker_simple_chaos_job pid=N job_id=42 seq=7
            if line.starts_with("READY") {
                // Use try_send so a closed channel doesn't panic the reader.
                let _ = ready_tx.send(());
            } else if let Some(seq_str) = line.split("seq=").nth(1) {
                if line.starts_with("COMPLETE") {
                    if let Ok(seq) = seq_str.trim().parse::<i64>() {
                        handled_seqs_reader.lock().unwrap().insert(seq);
                    }
                }
            }
        }
    });

    PythonHelper {
        child,
        handled_seqs,
        _stdout_reader: stdout_reader,
    }
}

async fn pool() -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url())
        .await
        .expect("connect to test postgres — is it running on the DSN?")
}

async fn reset_schema(pool: &sqlx::PgPool, queue_storage_schema: &str) {
    // Wipe both canonical and queue-storage state to a fresh start.
    // Tests in this file own the entire schema for the duration of the
    // run; serialised execution is the caller's responsibility (the
    // file lives in awa/tests, the standard `cargo test --test-threads=1`
    // is sufficient).
    sqlx::query("DROP SCHEMA IF EXISTS awa CASCADE")
        .execute(pool)
        .await
        .expect("drop awa schema");
    sqlx::query(&format!(
        "DROP SCHEMA IF EXISTS {queue_storage_schema} CASCADE"
    ))
    .execute(pool)
    .await
    .expect("drop queue-storage schema");

    // Also drop any leftover rolling_transition schemas from earlier
    // aborted runs; we name schemas with a UUID suffix per run, so they
    // would otherwise accumulate forever and confuse the next run by
    // leaving stale tables (e.g. lease child partitions referencing a
    // schema mismatch).
    let leftover_schemas: Vec<String> = sqlx::query_scalar(
        "SELECT schema_name FROM information_schema.schemata \
         WHERE schema_name LIKE 'awa_rolling_transition_%'",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    for schema in leftover_schemas {
        let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(pool)
            .await;
    }
}

async fn canonical_backlog_count(pool: &sqlx::PgPool, queue: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT count(*)::bigint
        FROM awa.jobs_hot
        WHERE queue = $1
          AND state NOT IN ('completed', 'failed', 'cancelled')
        "#,
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .unwrap_or(0)
}

async fn storage_state_summary(pool: &sqlx::PgPool) -> String {
    let s = storage::status(pool).await.expect("storage status");
    format!(
        "state={} current={} active={} prepared={:?}",
        s.state, s.current_engine, s.active_engine, s.prepared_engine
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_rolling_transition_with_live_producers_rust_and_python() {
    let suffix = &Uuid::new_v4().simple().to_string()[..8];
    let queue = format!("rolling_transition_{suffix}");
    let qs_schema = format!("awa_rolling_transition_{suffix}");

    let pool = pool().await;
    reset_schema(&pool, &qs_schema).await;
    migrations::run(&pool).await.expect("migrations");
    eprintln!(
        "[rehearsal] setup done; storage: {}",
        storage_state_summary(&pool).await
    );

    // ── Phase 1: canonical baseline ────────────────────────────────────

    let seeded = Arc::new(AtomicI64::new(0));

    // Pre-seed a single canonical job before any worker starts. This
    // suppresses the fresh-install auto-finalize path
    // (awa.storage_auto_finalize_if_fresh: any job in awa.jobs blocks
    // it). Without this, the first worker to start would auto-promote
    // the cluster directly to active/queue_storage and the rehearsal
    // would never get the chance to drive the transition manually.
    // The fleet picks this seed up as part of normal canonical
    // processing — it counts toward `seeded` like any other job.
    insert_with(
        &pool,
        &SimpleChaosJob { seq: -1 },
        InsertOpts {
            queue: queue.clone(),
            ..Default::default()
        },
    )
    .await
    .expect("seed canonical job to suppress auto-finalize");
    seeded.fetch_add(1, Ordering::Relaxed);

    // Producer task: continuous insert at ~50/s. We track total seeded
    // via an atomic so the final assertion compares against actual
    // production, not an a priori expected count.
    let producer_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let producer_pool = pool.clone();
    let producer_queue = queue.clone();
    let producer_running_handle = producer_running.clone();
    let producer_seeded = seeded.clone();
    let producer = tokio::spawn(async move {
        let mut seq: i64 = 0;
        while producer_running_handle.load(Ordering::Relaxed) {
            let res = insert_with(
                &producer_pool,
                &SimpleChaosJob { seq },
                InsertOpts {
                    queue: producer_queue.clone(),
                    ..Default::default()
                },
            )
            .await;
            match res {
                Ok(_) => {
                    seq += 1;
                    producer_seeded.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    // Routing flips can briefly produce errors; back off
                    // and retry. A persistent failure surfaces as the
                    // total seeded count plateauing, which the final
                    // assertion catches.
                    eprintln!("[rehearsal] producer insert error: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await; // ~50/s
        }
    });

    let rust_handled = Arc::new(AtomicI64::new(0));
    // Configure with queue_storage so heartbeats register as
    // queue_storage-capable. Auto role + state=canonical means the
    // resolved engine is still canonical until the operator flips the
    // state machine — but the heartbeat *capability* matters for the
    // mixed_transition gate, which counts canonical-only runtimes and
    // refuses to advance while any are alive.
    let auto_rust_qs_config = QueueStorageConfig {
        schema: qs_schema.clone(),
        ..Default::default()
    };
    let auto_rust_deadline = if auto_rust_qs_config.lease_claim_receipts {
        Duration::ZERO
    } else {
        QueueConfig::default().deadline_duration
    };
    let auto_rust_client = Client::builder(pool.clone())
        .queue(
            &queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                deadline_duration: auto_rust_deadline,
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            auto_rust_qs_config,
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .heartbeat_interval(Duration::from_millis(100))
        .promote_interval(Duration::from_millis(50))
        .leader_election_interval(Duration::from_millis(200))
        .leader_check_interval(Duration::from_millis(100))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(25))
        .transition_role(TransitionWorkerRole::Auto)
        .register_worker(CountingWorker {
            handled: rust_handled.clone(),
        })
        .build()
        .expect("auto rust client build");
    auto_rust_client.start().await.expect("auto rust start");

    let (py_ready_tx, mut py_ready_rx) = mpsc::unbounded_channel();
    let python_worker = spawn_python_worker(&queue, &qs_schema, py_ready_tx).await;
    tokio::time::timeout(Duration::from_secs(20), py_ready_rx.recv())
        .await
        .expect("python worker should signal READY within 20s");
    eprintln!("[rehearsal] phase 1: producer + auto Rust + auto Python all up");

    // Let the canonical baseline run for a few seconds so we have
    // observable canonical traffic before the prepare.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let pre_prepare_canonical_processed: i64 =
        rust_handled.load(Ordering::Relaxed) + python_worker.handler_count();
    let pre_prepare_seeded = seeded.load(Ordering::Relaxed);
    eprintln!(
        "[rehearsal] phase 1 end: seeded={pre_prepare_seeded} processed={pre_prepare_canonical_processed} \
         canonical_backlog={}",
        canonical_backlog_count(&pool, &queue).await
    );
    assert!(
        pre_prepare_canonical_processed > 0,
        "canonical phase should have processed at least one job before prepare"
    );

    // ── Phase 2: prepare ───────────────────────────────────────────────

    storage::prepare(
        &pool,
        "queue_storage",
        serde_json::json!({ "schema": qs_schema }),
    )
    .await
    .expect("storage prepare");
    eprintln!(
        "[rehearsal] phase 2: prepared; storage: {}",
        storage_state_summary(&pool).await
    );
    // Auto workers should stay canonical here.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ── Phase 3: install schema + queue_storage_target worker ──────────

    let qs_store = QueueStorage::new(QueueStorageConfig {
        schema: qs_schema.clone(),
        ..Default::default()
    })
    .expect("queue storage config");
    qs_store
        .prepare_schema(&pool)
        .await
        .expect("queue-storage schema install");
    eprintln!("[rehearsal] phase 3a: queue-storage schema installed");

    let qs_target_handled = Arc::new(AtomicI64::new(0));
    let qs_store_config = QueueStorageConfig {
        schema: qs_schema.clone(),
        ..Default::default()
    };
    // Receipts plane (now default after ADR-023 Phase 6) requires
    // deadline_duration = ZERO; the canonical 60s default is rejected.
    let qs_deadline = if qs_store_config.lease_claim_receipts {
        Duration::ZERO
    } else {
        QueueConfig::default().deadline_duration
    };
    let qs_target_client = Client::builder(pool.clone())
        .queue(
            &queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                deadline_duration: qs_deadline,
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            qs_store_config,
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .heartbeat_interval(Duration::from_millis(100))
        .promote_interval(Duration::from_millis(50))
        .leader_election_interval(Duration::from_millis(200))
        .leader_check_interval(Duration::from_millis(100))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(25))
        .transition_role(TransitionWorkerRole::QueueStorageTarget)
        .register_worker(CountingWorker {
            handled: qs_target_handled.clone(),
        })
        .build()
        .expect("queue_storage_target client build");
    qs_target_client.start().await.expect("qs target start");
    // Give it a moment to register a heartbeat with the new capability
    // so the enter_mixed_transition gate sees a live queue-storage
    // runtime.
    tokio::time::sleep(Duration::from_secs(2)).await;
    eprintln!("[rehearsal] phase 3b: queue_storage_target worker live");

    // ── Phase 4: mixed transition ──────────────────────────────────────

    storage::enter_mixed_transition(&pool)
        .await
        .expect("enter_mixed_transition");
    eprintln!(
        "[rehearsal] phase 4: mixed transition; storage: {}",
        storage_state_summary(&pool).await
    );

    // ── Phase 5: drain canonical ───────────────────────────────────────

    let drain_deadline = Instant::now() + Duration::from_secs(60);
    let mut last_backlog = -1i64;
    loop {
        let n = canonical_backlog_count(&pool, &queue).await;
        if n == 0 {
            eprintln!("[rehearsal] phase 5: canonical backlog drained");
            break;
        }
        if Instant::now() >= drain_deadline {
            panic!(
                "canonical backlog never reached 0 within 60s; last seen: {n} \
                 (rust={} python={} qs_target={} seeded={})",
                rust_handled.load(Ordering::Relaxed),
                python_worker.handler_count(),
                qs_target_handled.load(Ordering::Relaxed),
                seeded.load(Ordering::Relaxed)
            );
        }
        if n != last_backlog {
            eprintln!("[rehearsal] phase 5: canonical backlog now {n}");
            last_backlog = n;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // ── Phase 6: finalize ──────────────────────────────────────────────

    storage::finalize(&pool).await.expect("storage finalize");
    eprintln!(
        "[rehearsal] phase 6: finalized; storage: {}",
        storage_state_summary(&pool).await
    );

    // ── Phase 7: stable on queue_storage ───────────────────────────────

    tokio::time::sleep(Duration::from_secs(5)).await;
    let post_finalize_seeded = seeded.load(Ordering::Relaxed);
    let post_finalize_qs_processed = qs_target_handled.load(Ordering::Relaxed);
    eprintln!(
        "[rehearsal] phase 7: post-finalize stable; seeded={post_finalize_seeded} \
         qs_target_processed={post_finalize_qs_processed}"
    );

    // ── Phase 8: drain ─────────────────────────────────────────────────

    producer_running.store(false, Ordering::Relaxed);
    producer.await.expect("producer joined");
    let total_seeded = seeded.load(Ordering::Relaxed);

    // Wait for in-flight to settle. New work goes to queue_storage now,
    // so canonical workers won't pick anything up. The QueueStorageTarget
    // worker drains the rest.
    let settle_deadline = Instant::now() + Duration::from_secs(30);
    let mut prior_total: i64 = -1;
    loop {
        let total = rust_handled.load(Ordering::Relaxed)
            + python_worker.handler_count()
            + qs_target_handled.load(Ordering::Relaxed);
        if total >= total_seeded {
            break;
        }
        if Instant::now() >= settle_deadline {
            panic!(
                "settle timeout: total handler invocations {} < seeded {}; \
                 rust={} python={} qs_target={}",
                total,
                total_seeded,
                rust_handled.load(Ordering::Relaxed),
                python_worker.handler_count(),
                qs_target_handled.load(Ordering::Relaxed)
            );
        }
        if total != prior_total {
            eprintln!(
                "[rehearsal] phase 8: settling; processed {total}/{total_seeded} \
                 (rust={}, python={}, qs_target={})",
                rust_handled.load(Ordering::Relaxed),
                python_worker.handler_count(),
                qs_target_handled.load(Ordering::Relaxed),
            );
            prior_total = total;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let rust_final = rust_handled.load(Ordering::Relaxed);
    let py_final = python_worker.handler_count();
    let qs_final = qs_target_handled.load(Ordering::Relaxed);
    eprintln!(
        "[rehearsal] DONE seeded={total_seeded} \
         rust_canonical={rust_final} python_canonical={py_final} qs_target={qs_final} \
         total={}",
        rust_final + py_final + qs_final
    );

    // ── Final assertions ───────────────────────────────────────────────

    // Each engine must have done some work.
    assert!(
        rust_final > 0,
        "auto Rust worker never processed a canonical job (rust_final={rust_final})"
    );
    assert!(
        py_final > 0,
        "auto Python worker never processed a canonical job (py_final={py_final})"
    );
    assert!(
        qs_final > 0,
        "queue_storage_target worker never processed a queue-storage job (qs_final={qs_final})"
    );

    // No jobs lost. We accept ≥ rather than == because heartbeat-rescue
    // can re-execute a job that was in flight at the routing flip;
    // that's at-least-once semantics.
    assert!(
        rust_final + py_final + qs_final >= total_seeded,
        "lost jobs: total processed {} < total seeded {}",
        rust_final + py_final + qs_final,
        total_seeded
    );

    // ── Cleanup ────────────────────────────────────────────────────────

    auto_rust_client.shutdown(Duration::from_secs(5)).await;
    qs_target_client.shutdown(Duration::from_secs(5)).await;
    python_worker.shutdown().await;
    reset_schema(&pool, &qs_schema).await;
}
