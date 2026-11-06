//! Python users: a separate process built on the `awa-python` bindings, started and
//! stopped by the harness. It produces (single inserts, `insert_many` above the old
//! bind-parameter limit, COPY, and enqueues inside transactions that sometimes roll
//! back), runs async workers (completing, retryable, `RetryAfter`, `Snooze`, progress,
//! deadline overruns and a handler that ignores its deadline) and exercises the
//! migrate-then-query startup path. One queue is shared with a Rust runtime so Rust and
//! Python workers claim from the same lanes. The process writes its own per-job ledger,
//! which this module folds into the harness ledger for the oracle.

use super::super::config::Persona;
use super::super::fleet::Fleet;
use super::super::ledger::{expect, flag, JobRecord};
use super::super::python_probe::{self, Evidence, Phase};
use super::super::shared::Shared;
use super::{attach_events, queue, tune};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, Worker};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::io::{BufRead, Seek, SeekFrom};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

pub const QUEUE_WORK: &str = "awa_soak_py_work";
pub const QUEUE_DEADLINE: &str = "awa_soak_py_deadline";
pub const QUEUE_SHARED: &str = "awa_soak_py_shared";
const SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/persona_soak/python/soak_python.py"
);

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct PySharedJob {
    pub seq: u64,
    pub behavior: String,
    pub work_ms: u64,
}

struct SharedQueueWorker {
    shared: Arc<Shared>,
}

#[async_trait::async_trait]
impl Worker for SharedQueueWorker {
    fn kind(&self) -> &'static str {
        "py_shared_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let _guard = self
            .shared
            .begin_execution(Persona::Python, &ctx.job, false);
        let work_ms = ctx.job.args["work_ms"].as_u64().unwrap_or(50);
        match ctx.job.args["behavior"].as_str().unwrap_or("ok") {
            "retry" if ctx.job.attempt < 3 => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                return Err(JobError::retryable_msg("transient failure"));
            }
            "progress" => {
                for step in 1..=4u8 {
                    tokio::time::sleep(Duration::from_millis(work_ms / 4)).await;
                    ctx.set_progress(step * 25, "step");
                    let _ = ctx.flush_progress().await;
                }
            }
            _ => tokio::time::sleep(Duration::from_millis(work_ms)).await,
        }
        if ctx.is_cancelled() {
            return Err(JobError::retryable_msg("interrupted"));
        }
        self.shared
            .ledger
            .mark(ctx.job.id, flag::RETURNED_COMPLETED);
        Ok(JobResult::Completed)
    }
}

/// Handle to the running Python process, shared with the chaos persona.
pub struct PythonProcess {
    child: Option<Child>,
    ledger_path: PathBuf,
    python: PathBuf,
    restarts: u32,
}

fn spawn(
    shared: &Arc<Shared>,
    python: &PathBuf,
    ledger_path: &PathBuf,
    role: &str,
) -> Result<Child, String> {
    spawn_process(shared, python, ledger_path, role, "soak_python", None)
}

fn spawn_process(
    shared: &Arc<Shared>,
    python: &PathBuf,
    ledger_path: &PathBuf,
    role: &str,
    application_name: &str,
    probe_phase: Option<Arc<AtomicU8>>,
) -> Result<Child, String> {
    let mut child = Command::new(python)
        .arg(SCRIPT)
        .env(
            "SOAK_DATABASE_URL",
            format!(
                "{}{}application_name={}",
                shared.config.database_url,
                if shared.config.database_url.contains('?') {
                    '&'
                } else {
                    '?'
                },
                application_name
            ),
        )
        .env("SOAK_LEDGER", ledger_path)
        .env("SOAK_SEED", shared.config.seed.to_string())
        .env("SOAK_SCALE", shared.config.scale.to_string())
        .env(
            "SOAK_PACE",
            (shared.config.load.as_secs_f64() / 900.0)
                .clamp(0.1, 1.0)
                .to_string(),
        )
        .env("SOAK_ROLE", role)
        .env("SOAK_QUEUE_WORK", QUEUE_WORK)
        .env("SOAK_QUEUE_DEADLINE", QUEUE_DEADLINE)
        .env("SOAK_QUEUE_SHARED", QUEUE_SHARED)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    for stream in [
        child
            .stdout
            .take()
            .map(|s| Box::new(s) as Box<dyn tokio::io::AsyncRead + Send + Unpin>),
        child
            .stderr
            .take()
            .map(|s| Box::new(s) as Box<dyn tokio::io::AsyncRead + Send + Unpin>),
    ]
    .into_iter()
    .flatten()
    {
        let shared = shared.clone();
        let probe_phase = probe_phase.clone();
        let application_name = application_name.to_string();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stream).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let (Some(phase), Some(marker)) = (&probe_phase, Phase::marker(&line)) {
                    // A close marker alone does not prove migrate-and-query
                    // reached readiness. IDLE also follows that completed call.
                    if marker != Phase::Closed || phase.load(Ordering::Acquire) > 0 {
                        phase.fetch_max(marker as u8, Ordering::Release);
                    }
                    shared.note("python", format!("{application_name}: {line}"));
                }
                absorb_output(&shared, &line);
            }
        });
    }
    Ok(child)
}

/// Turns the interesting stdout lines of the Python process into report counters.
fn absorb_output(shared: &Shared, line: &str) {
    let stats = shared.stats(Persona::Python);
    let field = |name: &str| {
        line.split_whitespace()
            .find_map(|part| part.strip_prefix(&format!("{name}=")))
            .and_then(|value| value.parse::<f64>().ok())
    };
    if line.starts_with("STAT ") {
        if let Some(active) = field("active") {
            shared
                .python_active
                .store(active as i64, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(max_active) = field("max_active") {
            stats.set_max("python_max_concurrent_handlers", max_active as i64);
        }
        if let Some(errors) = field("errors") {
            stats.set_max("python_errors", errors as i64);
        }
    } else if line.starts_with("BATCH ") {
        if let (Some(rows), Some(seconds)) = (field("rows"), field("seconds")) {
            stats.add("python_batches", 1);
            stats.add("python_batch_rows", rows as i64);
            stats.observe("python_batch_insert_ms", (seconds * 1000.0) as i64);
        }
    } else if line.starts_with("MIGRATED") {
        stats.add("python_migrations", 1);
    } else if line.starts_with("ERROR") || line.contains("Traceback") {
        if stats.get("python_error_lines") < 5 {
            shared.note("python", line.chars().take(200).collect::<String>());
        }
        stats.add("python_error_lines", 1);
    }
}

pub struct PythonHandle {
    process: tokio::sync::Mutex<PythonProcess>,
}

impl PythonHandle {
    /// Kills the process without a shutdown handshake and starts a new one after `downtime`.
    pub async fn kill_and_restart(
        &self,
        shared: &Arc<Shared>,
        downtime: Duration,
    ) -> Result<(), String> {
        let mut process = self.process.lock().await;
        if let Some(mut child) = process.child.take() {
            let _ = child.kill().await;
        }
        tokio::time::sleep(downtime).await;
        shared
            .python_active
            .store(0, std::sync::atomic::Ordering::Relaxed);
        process.restarts += 1;
        let child = spawn(
            shared,
            &process.python.clone(),
            &process.ledger_path.clone(),
            "full",
        )?;
        process.child = Some(child);
        shared.stats(Persona::Python).add("python_kills", 1);
        Ok(())
    }
}

/// Folds new lines of the Python ledger into the harness ledger.
struct Importer {
    path: PathBuf,
    offset: u64,
    rolled_back: HashSet<i64>,
}

impl Importer {
    fn import(&mut self, shared: &Shared) {
        let Ok(mut file) = std::fs::File::open(&self.path) else {
            return;
        };
        if file.seek(SeekFrom::Start(self.offset)).is_err() {
            return;
        }
        let mut reader = std::io::BufReader::new(file);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    if !line.ends_with('\n') {
                        break;
                    }
                    self.offset += read as u64;
                    if let Ok(event) = serde_json::from_str::<Value>(&line) {
                        self.apply(shared, &event);
                    }
                }
            }
        }
    }

    fn apply(&mut self, shared: &Shared, event: &Value) {
        let now = shared.now_ms() as u32;
        let id = event.get("id").and_then(Value::as_i64);
        let queue_of = |name: Option<&str>| {
            name.and_then(|name| shared.queues.get(name))
                .map_or(u32::MAX, |queue| queue.index)
        };
        match event.get("e").and_then(Value::as_str) {
            Some("enq") => {
                let Some(id) = id else { return };
                let allowed = event.get("x").and_then(Value::as_u64).unwrap_or(7) as u8;
                let queue = shared
                    .queues
                    .get(event.get("q").and_then(Value::as_str).unwrap_or(""));
                if let Some(queue) = &queue {
                    queue
                        .enqueued
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                let index = queue.map_or(u32::MAX, |queue| queue.index);
                let mut record =
                    JobRecord::new(Persona::Python, index, now, allowed, flag::NO_EVENTS);
                record.max_attempt = 0;
                // Keep what the Rust side may have seen already (shared queue).
                shared.ledger.with(
                    id,
                    || record.clone(),
                    |existing| {
                        existing.expect = allowed;
                        existing.flags &= !flag::UNREGISTERED;
                        existing.persona = Persona::Python;
                    },
                );
                shared.stats(Persona::Python).add("python_enqueued", 1);
            }
            Some("start") => {
                let Some(id) = id else { return };
                let queue = queue_of(event.get("q").and_then(Value::as_str));
                let attempt = event.get("attempt").and_then(Value::as_i64).unwrap_or(0) as i16;
                shared.ledger.with(
                    id,
                    || {
                        JobRecord::new(
                            Persona::Python,
                            queue,
                            now,
                            expect::ANY_TERMINAL,
                            flag::NO_EVENTS,
                        )
                    },
                    |record| {
                        record.executions = record.executions.saturating_add(1);
                        record.max_attempt = record.max_attempt.max(attempt);
                        if record.first_start_ms == 0 {
                            record.first_start_ms = now.max(1);
                        }
                        record.last_start_ms = now;
                    },
                );
                if let Some(queue) = event
                    .get("q")
                    .and_then(Value::as_str)
                    .and_then(|name| shared.queues.get(name))
                {
                    queue
                        .started
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                shared.stats(Persona::Python).add("python_executions", 1);
            }
            Some("end") => {
                let Some(id) = id else { return };
                let result = event.get("r").and_then(Value::as_str).unwrap_or("");
                shared.ledger.with(
                    id,
                    || {
                        JobRecord::new(
                            Persona::Python,
                            u32::MAX,
                            now,
                            expect::ANY_TERMINAL,
                            flag::NO_EVENTS,
                        )
                    },
                    |record| match result {
                        // A completed return is evidence only for jobs that must complete; for
                        // the rest the outcome is read from the database, since a late return
                        // after a deadline rescue is discarded by the store.
                        "completed" if record.expect == expect::COMPLETED => {
                            record.flags |= flag::RETURNED_COMPLETED
                        }
                        "cancel" => {
                            record.flags |= flag::HANDLER_CANCELLED;
                            record.cancel_events = record.cancel_events.saturating_add(1);
                        }
                        _ => {}
                    },
                );
            }
            Some("rolled_back") => {
                if let Some(ids) = event.get("ids").and_then(Value::as_array) {
                    for id in ids.iter().filter_map(Value::as_i64) {
                        self.rolled_back.insert(id);
                    }
                    shared
                        .stats(Persona::Python)
                        .add("python_rolled_back_jobs", ids.len() as i64);
                }
            }
            Some("error") => shared.stats(Persona::Python).add("python_logged_errors", 1),
            _ => {}
        }
    }
}

/// A migrate-then-query run in a throwaway process; its database sessions must not be
/// left stranded while it idles, and must be gone once it exits.
async fn migrate_probe(shared: &Arc<Shared>, python: &PathBuf, ledger_path: &PathBuf) {
    let stats = shared.stats(Persona::Python);
    let application_name = format!("soak_python_probe_{}", uuid::Uuid::new_v4().simple());
    let phase = Arc::new(AtomicU8::new(Phase::Migrating as u8));
    let mut child = match spawn_process(
        shared,
        python,
        ledger_path,
        "migrate_probe",
        &application_name,
        Some(phase.clone()),
    ) {
        Ok(child) => child,
        Err(error) => {
            shared.incomplete(
                format!("Python migrate probe could not start: {error}"),
                vec![],
            );
            return;
        }
    };
    let pool = match super::super::db::connect_pool(
        &shared.config.database_url,
        "soak_python_probe_watch",
        1,
    )
    .await
    {
        Ok(pool) => pool,
        Err(error) => {
            shared.incomplete(
                format!("Python probe observer could not connect: {error}"),
                vec![],
            );
            let _ = child.kill().await;
            return;
        }
    };
    let mut evidence = Evidence::default();
    let deadline = Instant::now() + crate::ci_timing::scaled_timeout(Duration::from_secs(45));
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    evidence.incomplete(format!("Python migrate probe exited with {status}"));
                }
                break;
            }
            Err(error) => {
                evidence.incomplete(format!(
                    "Python migrate probe exit could not be read: {error}"
                ));
                let _ = child.kill().await;
                break;
            }
            Ok(None) if Instant::now() >= deadline => {
                evidence
                    .incomplete("Python migrate-then-query probe did not exit within its deadline");
                let _ = child.kill().await;
                break;
            }
            Ok(None) => {}
        }
        // Capture readiness before starting the query: a MIGRATED marker that
        // arrives during it must not relabel an earlier migration snapshot.
        let before_query = Phase::from_byte(phase.load(Ordering::Acquire));
        evidence.observe(
            before_query,
            python_probe::sessions(&pool, &application_name).await,
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let deadline = Instant::now() + crate::ci_timing::scaled_timeout(Duration::from_secs(2));
    loop {
        let result = python_probe::sessions(&pool, &application_name).await;
        let stopped = result.as_ref().is_ok_and(|(count, _)| *count == 0);
        let failed = result.is_err();
        evidence.after_exit(result);
        if stopped || failed || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stats.add("python_migrate_probes", 1);
    stats.set_max("python_probe_peak_sessions", evidence.peak_sessions);
    stats.set_max(
        "python_probe_migrating_idle_transactions",
        evidence.migrating_idle_transactions,
    );
    stats.add("python_probe_ready_samples", evidence.ready_samples as i64);
    if !evidence.complete() {
        shared.incomplete(
            format!(
                "Python migrate probe {application_name} has incomplete evidence: {evidence:?}"
            ),
            vec![],
        );
    }
    if evidence.violation() {
        shared.violation(
            "python_connections_stranded",
            format!("Python migrate probe {application_name}: peak={} against pool4, idle-in-transaction after ready={}, sessions after exit={:?}", evidence.peak_sessions, evidence.ready_idle_transactions, evidence.remaining),
            vec![],
        );
    }
    pool.close().await;
}

pub async fn run(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::Python;
    let python = shared
        .config
        .python_bin
        .clone()
        .ok_or("the python persona needs AWA_SOAK_PYTHON, the interpreter of a virtualenv built with `maturin develop` (see scripts/persona-soak.sh)")?;
    let shared_queue = shared.queues.register(QUEUE_SHARED, persona, false);
    shared.queues.register(QUEUE_WORK, persona, false);
    shared.queues.register(QUEUE_DEADLINE, persona, false);

    // The Rust runtime that shares the queue with the Python workers.
    let rust_shared = shared.clone();
    let factory: super::super::db::ClientFactory = Arc::new(move |pool| {
        let builder = Client::builder(pool)
            .queue(QUEUE_SHARED, queue(6))
            .register_worker(SharedQueueWorker {
                shared: rust_shared.clone(),
            });
        attach_events(
            tune(builder, &rust_shared),
            &rust_shared,
            &["py_shared_job"],
        )
        .build()
        .expect("build python shared-queue client")
    });
    let fleet = Fleet::start(
        &shared,
        "pyrust",
        persona,
        1,
        8,
        2,
        vec![(shared_queue, 6)],
        factory,
    )
    .await?;

    std::fs::create_dir_all(&shared.config.report_dir).map_err(|error| error.to_string())?;
    let ledger_path = shared.config.report_dir.join("python-ledger.jsonl");
    let _ = std::fs::remove_file(&ledger_path);
    let child = spawn(&shared, &python, &ledger_path, "full")?;
    let handle = Arc::new(PythonHandle {
        process: tokio::sync::Mutex::new(PythonProcess {
            child: Some(child),
            ledger_path: ledger_path.clone(),
            python: python.clone(),
            restarts: 0,
        }),
    });
    shared
        .python
        .lock()
        .expect("python handle")
        .replace(handle.clone());
    shared.note("python", format!("{} started; queues {QUEUE_WORK}, {QUEUE_DEADLINE}, {QUEUE_SHARED} (shared with a Rust runtime)", python.display()));

    let tail_pool =
        super::super::db::connect_pool(&shared.config.database_url, "soak_python_tail", 1)
            .await
            .map_err(|error| error.to_string())?;
    let mut tail_tick = 0u64;
    let mut probes = tokio::task::JoinSet::new();
    let mut importer = Importer {
        path: ledger_path.clone(),
        offset: 0,
        rolled_back: HashSet::new(),
    };
    let pace = (shared.config.load.as_secs_f64() / 900.0).clamp(0.1, 1.0);
    let mut next_probe = Instant::now() + Duration::from_secs_f64(30.0 * pace.max(0.3));
    let probe_every = Duration::from_secs_f64(150.0 * pace.max(0.3));
    while !shared.load_stop.sleep(Duration::from_secs(1)).await {
        importer.import(&shared);
        tail_terminal_rows(&shared, &tail_pool, &mut tail_tick).await;
        {
            let mut process = handle.process.lock().await;
            if let Some(child) = process.child.as_mut() {
                if let Ok(Some(status)) = child.try_wait() {
                    shared.stats(persona).add("python_unexpected_exits", 1);
                    shared.note(
                        "python",
                        format!("python process exited unexpectedly: {status}; restarting"),
                    );
                    process.child = spawn(&shared, &python, &ledger_path, "full").ok();
                }
            }
        }
        if Instant::now() >= next_probe {
            next_probe = Instant::now() + probe_every;
            let (shared, python, ledger_path) =
                (shared.clone(), python.clone(), ledger_path.clone());
            probes.spawn(async move { migrate_probe(&shared, &python, &ledger_path).await });
        }
    }

    while probes.join_next().await.is_some() {}
    // Stop producing; the workers keep running while the system drains.
    if let Some(pid) = handle
        .process
        .lock()
        .await
        .child
        .as_ref()
        .and_then(Child::id)
    {
        let _ = std::process::Command::new("kill")
            .args(["-USR1", &pid.to_string()])
            .status();
    }
    shared.note("python", "load stopped; draining");
    while !shared.drain_stop.sleep(Duration::from_secs(1)).await {
        importer.import(&shared);
        tail_terminal_rows(&shared, &tail_pool, &mut tail_tick).await;
    }
    importer.import(&shared);
    if let Some(mut child) = handle.process.lock().await.child.take() {
        let _ = child.kill().await;
    }
    shared.python.lock().expect("python handle").take();
    fleet.shutdown_all(Duration::from_secs(20), &shared).await;
    importer.import(&shared);
    check_rolled_back(&shared, &importer.rolled_back).await?;
    Ok(())
}

/// Python jobs report no lifecycle events, and ring rotation prunes their terminal rows
/// within seconds, so their terminal outcomes are read from the store as they appear.
async fn tail_terminal_rows(shared: &Arc<Shared>, pool: &sqlx::PgPool, tick: &mut u64) {
    *tick += 1;
    if !(*tick).is_multiple_of(3) {
        return;
    }
    let schema = shared
        .info
        .queue_storage_schema
        .clone()
        .unwrap_or_else(|| "awa".into());
    let queues = vec![
        QUEUE_WORK.to_string(),
        QUEUE_DEADLINE.to_string(),
        QUEUE_SHARED.to_string(),
    ];
    let rows: Result<Vec<(i64, i64, String)>, _> = sqlx::query_as(&format!(
        "SELECT job_id, run_lease, state::text FROM {schema}.done_entries WHERE queue = ANY($1) AND finalized_at > now() - interval '25 seconds'
         UNION ALL SELECT job_id, run_lease, 'failed' FROM {schema}.dlq_entries WHERE queue = ANY($1) AND dlq_at > now() - interval '25 seconds'
         UNION ALL SELECT b.job_id, b.run_lease, 'completed' FROM {schema}.receipt_completion_batches c, LATERAL unnest(c.job_ids, c.run_leases) AS b(job_id, run_lease)
           WHERE c.queue = ANY($1) AND c.finalized_at > now() - interval '25 seconds'"
    ))
    .bind(&queues)
    .fetch_all(pool)
    .await;
    let rows = match rows {
        Ok(rows) => rows,
        Err(error) => {
            shared.stats(Persona::Python).add("python_tail_errors", 1);
            if shared.stats(Persona::Python).get("python_tail_errors") <= 3 {
                shared.note("python", format!("terminal row tail failed: {error}"));
            }
            return;
        }
    };
    shared
        .stats(Persona::Python)
        .add("python_tail_rows", rows.len() as i64);
    let now = shared.now_ms() as u32;
    for (id, run_lease, state) in rows {
        shared.ledger.with(
            id,
            || {
                JobRecord::new(
                    Persona::Python,
                    u32::MAX,
                    now,
                    expect::ANY_TERMINAL,
                    flag::NO_EVENTS,
                )
            },
            |record| {
                record.note_terminal(run_lease);
                match state.as_str() {
                    "completed" => record.flags |= flag::DB_SEEN_COMPLETED,
                    "failed" => record.flags |= flag::DB_SEEN_FAILED,
                    "cancelled" => record.flags |= flag::DB_SEEN_CANCELLED,
                    _ => {}
                }
            },
        );
    }
}

/// A transaction rolled back by the Python application must leave no job anywhere.
async fn check_rolled_back(shared: &Arc<Shared>, ids: &HashSet<i64>) -> Result<(), String> {
    if ids.is_empty() {
        return Ok(());
    }
    let schema = shared
        .info
        .queue_storage_schema
        .clone()
        .unwrap_or_else(|| "awa".into());
    let pool = super::super::db::connect_pool(&shared.config.database_url, "soak_python_check", 1)
        .await
        .map_err(|error| error.to_string())?;
    let ids: Vec<i64> = ids.iter().copied().collect();
    let mut visible: Vec<i64> = Vec::new();
    for chunk in ids.chunks(5000) {
        let found: Vec<i64> = sqlx::query_scalar(&format!(
            "SELECT job_id FROM {schema}.ready_entries WHERE job_id = ANY($1)
             UNION SELECT job_id FROM {schema}.deferred_jobs WHERE job_id = ANY($1)
             UNION SELECT job_id FROM {schema}.done_entries WHERE job_id = ANY($1)
             UNION SELECT job_id FROM {schema}.dlq_entries WHERE job_id = ANY($1)"
        ))
        .bind(chunk)
        .fetch_all(&pool)
        .await
        .map_err(|error| error.to_string())?;
        visible.extend(found);
    }
    let executed: Vec<i64> = ids
        .iter()
        .copied()
        .filter(|id| {
            shared
                .ledger
                .get(*id)
                .is_some_and(|record| record.executions > 0)
        })
        .collect();
    shared
        .stats(Persona::Python)
        .add("python_rolled_back_checked", ids.len() as i64);
    if !visible.is_empty() || !executed.is_empty() {
        shared.violation(
            "rolled_back_job_visible",
            format!("{} job(s) enqueued in rolled-back Python transactions exist in the store, {} of them executed", visible.len(), executed.len()),
            visible.iter().take(10).map(|id| format!("{{\"job_id\":{id}}}")).collect(),
        );
    }
    pool.close().await;
    Ok(())
}
