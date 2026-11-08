//! Claim and abandon: a runtime that claims as much as it can, holds thousands of jobs
//! with deadlines and heartbeats, and is SIGKILLed again and again.
//!
//! The runtime is a separate process: the test binary started again with only the
//! `claim_abandon_child` test selected, so that a kill leaves its claims genuinely
//! abandoned (no shutdown handshake, connections dropped by the kernel). Between kills a
//! surviving in-process runtime and the other personas keep working on the same
//! database. After the last kill an in-process runtime takes over the abandoned queues
//! and finishes whatever the rescue returns to them.
//!
//! The checks are in `abandon_oracle.rs`: how long the rescue backlog takes to clear
//! after each kill, whether the claim ring keeps rotating, whether any job was rescued
//! while its owner was alive, and whether attempts and the dead-letter queue follow
//! `max_attempts`.

use super::super::abandon_oracle::{
    epoch_ms, probe_backlog, Attempt, Kill, LiveCheck, Scope, FINISHER,
};
use super::super::config::{Persona, DEFAULT_HEARTBEAT_SECONDS, DEFAULT_STALENESS_SECONDS};
use super::super::db::{connect_pool, connect_pool_with_timeout, ClientFactory};
use super::super::fleet::Fleet;
use super::super::ledger::{expect, flag, JobRecord};
use super::super::rng::Rng;
use super::super::shared::Shared;
use super::{backoff, queue, register_rows, timings, tune};
use awa::model::{insert_many, insert_many_copy_from_pool};
use awa::{
    Client, InsertOpts, InsertParams, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

/// Zero-deadline claims.
pub const QUEUE_PLAIN: &str = "awa_soak_abandon_plain";
/// Claims with a deadline. Both queues' claims are written as compact claim batches.
pub const QUEUE_DEADLINE: &str = "awa_soak_abandon_deadline";
/// Short jobs served by the survivors, to see claim latency while the backlog drains.
pub const QUEUE_CANARY: &str = "awa_soak_abandon_canary";

/// The name of the test the child process runs.
pub const CHILD_TEST: &str = "claim_abandon_child";
const ENV_URL: &str = "AWA_SOAK_ABANDON_URL";
const ENV_GENERATION: &str = "AWA_SOAK_ABANDON_GENERATION";
const ENV_LEDGER: &str = "AWA_SOAK_ABANDON_LEDGER";
const ENV_WORKERS: &str = "AWA_SOAK_ABANDON_WORKERS";
const ENV_POOL: &str = "AWA_SOAK_ABANDON_POOL";
const ENV_HEARTBEAT: &str = "AWA_SOAK_HEARTBEAT_SECONDS";
const ENV_STALENESS: &str = "AWA_SOAK_STALENESS_SECONDS";

/// Longer than any process lives, so a job is only ever finished by the survivors.
const HOLD: Duration = Duration::from_secs(20 * 60);
const DEADLINE: Duration = Duration::from_secs(15 * 60);
const CHILD_POOL: u32 = 16;
const FINISHER_REPLICAS: usize = 2;
const FINISHER_POOL: u32 = 16;
/// Percentage of jobs that may only be attempted twice, so that repeated kills
/// dead-letter them.
const SHORT_RETRY_PERCENT: u64 = 25;
/// Consecutive heartbeat samples that may find no open claim while the process reports
/// running handlers. Covers a handler count read before the process's claims were visible.
const EMPTY_SAMPLES_BEFORE_INCOMPLETE: u32 = 5;

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct AbandonJob {
    pub seq: u64,
    pub cycle: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct AbandonCanaryJob {
    pub seq: u64,
}

/// The worker in the child process: records every start in the child ledger and then
/// holds the job until the process is killed.
struct ChildWorker {
    generation: u32,
    ledger: Mutex<std::fs::File>,
    active: Arc<AtomicI64>,
    peak: Arc<AtomicI64>,
}

#[async_trait::async_trait]
impl Worker for ChildWorker {
    fn kind(&self) -> &'static str {
        "abandon_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let line = format!(
            "{{\"e\":\"start\",\"id\":{},\"attempt\":{},\"gen\":{},\"q\":\"{}\",\"t\":{}}}\n",
            ctx.job.id,
            ctx.job.attempt,
            self.generation,
            ctx.job.queue,
            epoch_ms()
        );
        // One write per start, straight to the file: nothing is buffered that a kill could
        // lose, and the parent never sees a half line it cannot skip.
        if let Ok(mut file) = self.ledger.lock() {
            let _ = file.write_all(line.as_bytes());
        }
        let now = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(now, Ordering::Relaxed);
        tokio::time::sleep(HOLD).await;
        self.active.fetch_sub(1, Ordering::Relaxed);
        Ok(JobResult::Completed)
    }
}

/// Entry point of the child process; see [`CHILD_TEST`]. Returns at once when the
/// environment is not set up by the parent, so a plain `--ignored` run is unaffected.
pub async fn run_child() {
    let (Ok(url), Ok(generation), Ok(ledger), Ok(workers)) = (
        std::env::var(ENV_URL),
        std::env::var(ENV_GENERATION),
        std::env::var(ENV_LEDGER),
        std::env::var(ENV_WORKERS),
    ) else {
        return;
    };
    super::super::worker_errors::install(true).expect("child worker error tracing");
    let generation: u32 = generation.parse().expect("generation");
    let workers: u32 = workers.parse().expect("workers");
    let pool_size: u32 = std::env::var(ENV_POOL)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(CHILD_POOL);

    // Exit with the harness, so a harness that died leaves no process behind.
    let parent = std::os::unix::process::parent_id();
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(1));
        if std::os::unix::process::parent_id() != parent {
            std::process::exit(0);
        }
    });

    let pool = connect_pool(&url, &format!("soak_abandon_{generation}"), pool_size)
        .await
        .expect("child connect");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&ledger)
        .expect("child ledger");
    let active = Arc::new(AtomicI64::new(0));
    let peak = Arc::new(AtomicI64::new(0));
    let claiming = |deadline_duration: Duration| QueueConfig {
        max_workers: workers,
        poll_interval: Duration::from_millis(50),
        deadline_duration,
        claimers: 2,
        claim_batch_size: 1_000,
        ..QueueConfig::default()
    };
    // The child keeps away from maintenance leadership: the survivors do the rescuing, as
    // the other replicas of an application would.
    let seconds = |name: &str, default: u64| {
        Duration::from_secs(
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default),
        )
    };
    let client = timings(
        Client::builder(pool)
            .queue(QUEUE_PLAIN, claiming(Duration::ZERO))
            .queue(QUEUE_DEADLINE, claiming(DEADLINE))
            .register_worker(ChildWorker {
                generation,
                ledger: Mutex::new(file),
                active: active.clone(),
                peak: peak.clone(),
            }),
        seconds(ENV_HEARTBEAT, DEFAULT_HEARTBEAT_SECONDS),
        seconds(ENV_STALENESS, DEFAULT_STALENESS_SECONDS),
    )
    .leader_election_interval(Duration::from_secs(3600))
    .build()
    .expect("build child client");
    client.start().await.expect("start child client");
    println!("READY gen={generation}");
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        println!(
            "STAT gen={generation} active={} peak={}",
            active.load(Ordering::Relaxed),
            peak.load(Ordering::Relaxed)
        );
    }
}

/// Finishes the abandoned jobs once the child processes are gone.
struct FinisherWorker {
    shared: Arc<Shared>,
}

#[async_trait::async_trait]
impl Worker for FinisherWorker {
    fn kind(&self) -> &'static str {
        "abandon_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let _guard = self
            .shared
            .begin_execution(Persona::ClaimAbandon, &ctx.job, true);
        self.shared.abandon.note_attempt(
            ctx.job.id,
            Attempt {
                attempt: ctx.job.attempt,
                owner: FINISHER,
                at_ms: epoch_ms(),
            },
        );
        self.shared
            .ledger
            .mark(ctx.job.id, flag::RETURNED_COMPLETED);
        Ok(JobResult::Completed)
    }
}

struct CanaryWorker {
    shared: Arc<Shared>,
}

#[async_trait::async_trait]
impl Worker for CanaryWorker {
    fn kind(&self) -> &'static str {
        "abandon_canary_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let _guard = self
            .shared
            .begin_execution(Persona::ClaimAbandon, &ctx.job, true);
        let waited = (chrono::Utc::now() - ctx.job.created_at).num_milliseconds();
        self.shared
            .stats(Persona::ClaimAbandon)
            .observe("canary_wait_ms", waited);
        self.shared
            .ledger
            .mark(ctx.job.id, flag::RETURNED_COMPLETED);
        Ok(JobResult::Completed)
    }
}

/// Copies the running test binary into the report directory. Every process is started
/// from the copy, so a rebuild of the binary during a long run cannot change what is
/// started, or leave nothing to start.
fn copy_executable(directory: &std::path::Path) -> Result<PathBuf, String> {
    let copy = directory.join("claim-abandon-child");
    let _ = std::fs::remove_file(&copy);
    let source = std::path::Path::new("/proc/self/exe");
    let source = if source.exists() {
        source.to_path_buf()
    } else {
        std::env::current_exe().map_err(|error| format!("current_exe: {error}"))?
    };
    std::fs::copy(&source, &copy)
        .map_err(|error| format!("copy {} to {}: {error}", source.display(), copy.display()))?;
    Ok(copy)
}

fn spawn_child(
    shared: &Arc<Shared>,
    executable: &PathBuf,
    generation: u32,
    ledger: &PathBuf,
    workers_per_queue: u32,
    pool_size: u32,
) -> Result<Child, String> {
    let url = &shared.config.database_url;
    let mut child = Command::new(executable)
        .args([
            "--ignored",
            "--exact",
            CHILD_TEST,
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ENV_URL, url)
        .env(ENV_GENERATION, generation.to_string())
        .env(ENV_LEDGER, ledger)
        .env(ENV_WORKERS, workers_per_queue.to_string())
        .env(ENV_POOL, pool_size.to_string())
        .env(
            ENV_HEARTBEAT,
            shared.config.heartbeat_interval.as_secs().to_string(),
        )
        .env(
            ENV_STALENESS,
            shared.config.heartbeat_staleness.as_secs().to_string(),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| format!("spawn the claiming process: {error}"))?;
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
        let output_guard = super::super::worker_errors::child_output_guard();
        tokio::spawn(async move {
            let _output_guard = output_guard;
            let mut lines = BufReader::new(stream).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => absorb_output(&shared, &line),
                    Ok(None) => break,
                    Err(error) => {
                        shared.incomplete(
                            format!("claiming-child output read failed: {error}"),
                            vec![],
                        );
                        break;
                    }
                }
            }
        });
    }
    Ok(child)
}

fn absorb_output(shared: &Shared, line: &str) {
    if super::super::worker_errors::absorb_child_marker(line) {
        return;
    }
    let stats = shared.stats(Persona::ClaimAbandon);
    let field = |name: &str| {
        line.split_whitespace()
            .find_map(|part| part.strip_prefix(&format!("{name}=")))
            .and_then(|value| value.parse::<i64>().ok())
    };
    if line.starts_with("STAT ") {
        if let Some(active) = field("active") {
            shared.abandon.child_active.store(active, Ordering::Relaxed);
        }
        if let Some(peak) = field("peak") {
            stats.set_max("process_peak_held", peak);
        }
    } else if line.contains("panicked") || line.contains("Error") {
        if stats.get("process_error_lines") < 5 {
            shared.note("abandon", line.chars().take(200).collect::<String>());
        }
        stats.add("process_error_lines", 1);
    }
}

/// Folds the new lines of the child ledger into the harness ledger.
struct Importer {
    path: PathBuf,
    offset: u64,
}

impl Importer {
    fn import(&mut self, shared: &Shared) {
        let Ok(mut file) = std::fs::File::open(&self.path) else {
            return;
        };
        if file.seek(SeekFrom::Start(self.offset)).is_err() {
            return;
        }
        let mut text = String::new();
        if file.read_to_string(&mut text).is_err() {
            return;
        }
        for line in text.split_inclusive('\n') {
            if !line.ends_with('\n') {
                break;
            }
            self.offset += line.len() as u64;
            if let Ok(event) = serde_json::from_str::<Value>(line) {
                self.apply(shared, &event);
            }
        }
    }

    fn apply(&self, shared: &Shared, event: &Value) {
        if event.get("e").and_then(Value::as_str) != Some("start") {
            return;
        }
        let (Some(id), Some(attempt), Some(generation), Some(at_ms)) = (
            event.get("id").and_then(Value::as_i64),
            event.get("attempt").and_then(Value::as_i64),
            event.get("gen").and_then(Value::as_u64),
            event.get("t").and_then(Value::as_i64),
        ) else {
            return;
        };
        let queue = event
            .get("q")
            .and_then(Value::as_str)
            .and_then(|name| shared.queues.get(name));
        let queue_index = queue.as_ref().map_or(u32::MAX, |queue| queue.index);
        if let Some(queue) = &queue {
            queue.started.fetch_add(1, Ordering::Relaxed);
        }
        let now = shared.now_ms() as u32;
        shared.ledger.with(
            id,
            || {
                JobRecord::new(
                    Persona::ClaimAbandon,
                    queue_index,
                    now,
                    expect::COMPLETED | expect::FAILED,
                    flag::NO_EVENTS,
                )
            },
            |record| {
                record.executions = record.executions.saturating_add(1);
                record.max_attempt = record.max_attempt.max(attempt as i16);
                if record.first_start_ms == 0 {
                    record.first_start_ms = now.max(1);
                }
                record.last_start_ms = now;
            },
        );
        shared.abandon.note_attempt(
            id,
            Attempt {
                attempt: attempt as i16,
                owner: generation as u32,
                at_ms,
            },
        );
        shared
            .stats(Persona::ClaimAbandon)
            .add("process_executions", 1);
    }
}

fn params(rng: &mut Rng, seq: &mut u64, cycle: u32, count: usize) -> Vec<InsertParams> {
    (0..count)
        .map(|index| {
            *seq += 1;
            awa::insert::params_with(
                &AbandonJob { seq: *seq, cycle },
                InsertOpts {
                    queue: if index % 2 == 0 {
                        QUEUE_PLAIN
                    } else {
                        QUEUE_DEADLINE
                    }
                    .to_string(),
                    max_attempts: if rng.chance(SHORT_RETRY_PERCENT) {
                        2
                    } else {
                        25
                    },
                    ..Default::default()
                },
            )
            .expect("abandon params")
        })
        .collect()
}

/// Enqueues `count` jobs for the next process to claim, in chunks that COPY handles
/// comfortably.
async fn enqueue_cycle(
    shared: &Arc<Shared>,
    pool: &sqlx::PgPool,
    rng: &mut Rng,
    seq: &mut u64,
    cycle: u32,
    count: usize,
) -> Result<usize, String> {
    let mut enqueued = 0;
    while enqueued < count {
        let chunk = (count - enqueued).min(10_000);
        let batch = params(rng, seq, cycle, chunk);
        let rows = insert_many_copy_from_pool(pool, &batch)
            .await
            .map_err(|error| format!("enqueue: {error}"))?;
        for row in &rows {
            shared.abandon.register_job(row.id, row.max_attempts);
            if let Some(queue) = shared.queues.get(&row.queue) {
                queue.enqueued.fetch_add(1, Ordering::Relaxed);
            }
        }
        register_rows(
            shared,
            Persona::ClaimAbandon,
            &rows,
            |_| expect::COMPLETED | expect::FAILED,
            flag::NO_EVENTS,
        );
        shared
            .stats(Persona::ClaimAbandon)
            .add("enqueued", rows.len() as i64);
        enqueued += rows.len();
    }
    Ok(enqueued)
}

async fn canary_producer(shared: Arc<Shared>, pool: sqlx::PgPool) {
    let mut seq = 0u64;
    while !shared.load_stop.sleep(Duration::from_millis(200)).await {
        let batch: Vec<InsertParams> = (0..2)
            .map(|_| {
                seq += 1;
                awa::insert::params_with(
                    &AbandonCanaryJob { seq },
                    InsertOpts {
                        queue: QUEUE_CANARY.to_string(),
                        max_attempts: 5,
                        ..Default::default()
                    },
                )
                .expect("canary params")
            })
            .collect();
        match insert_many(&pool, &batch).await {
            Ok(rows) => {
                shared
                    .stats(Persona::ClaimAbandon)
                    .add("canary_enqueued", rows.len() as i64);
                register_rows(
                    &shared,
                    Persona::ClaimAbandon,
                    &rows,
                    |_| expect::COMPLETED,
                    flag::NO_EVENTS,
                );
            }
            Err(error) => backoff(&shared, error, Persona::ClaimAbandon).await,
        }
    }
}

/// Measures the live process's heartbeat passes from the timestamps its heartbeats write.
/// A pass updates every in-flight claim with the time of its own chunk, so once a pass has
/// finished the oldest heartbeat is newer than anything the previous pass wrote, and the
/// spread from oldest to newest is the pass's duration (to within the last chunk). Only
/// open claims on the persona's queues taken since the process started are read, so the
/// dead processes' open claims, whose heartbeats stopped, do not count.
///
/// Returns false when the sample saw no open claim at all, which is only possible while the
/// process holds jobs if the query does not read where claims are written.
async fn sample_heartbeats(
    shared: &Arc<Shared>,
    pool: &sqlx::PgPool,
    schema: &str,
    since_epoch_ms: i64,
) -> bool {
    let row: Result<(i64, Option<f64>, Option<f64>, f64), _> =
        sqlx::query_as(awa::audited_sql(format!(
            "WITH open_claims AS (
                SELECT b.claim_slot, i.job_id, i.run_lease, i.receipt_id
                FROM {schema}.lease_claim_batches b
                CROSS JOIN LATERAL unnest(b.job_ids, b.run_leases, b.receipt_ids)
                    AS i(job_id, run_lease, receipt_id)
                WHERE b.queue = ANY($1)
                  AND b.claimed_at >= to_timestamp($2::float8 / 1000.0)
                  AND NOT EXISTS (SELECT 1 FROM {schema}.lease_claim_closures cl
                                  WHERE cl.claim_slot = b.claim_slot
                                    AND cl.job_id = i.job_id AND cl.run_lease = i.run_lease)
                  AND NOT EXISTS (SELECT 1 FROM {schema}.lease_claim_closure_batches cb
                                  WHERE cb.claim_slot = b.claim_slot
                                    AND cb.receipt_ranges @> i.receipt_id
                                  OFFSET 0)
            )
            SELECT count(*)::bigint,
                EXTRACT(EPOCH FROM min(a.heartbeat_at))::float8,
                EXTRACT(EPOCH FROM max(a.heartbeat_at))::float8,
                EXTRACT(EPOCH FROM clock_timestamp())::float8
            FROM open_claims c
            LEFT JOIN {schema}.attempt_state a ON a.job_id = c.job_id AND a.run_lease = c.run_lease"
        )))
        .bind(vec![QUEUE_PLAIN, QUEUE_DEADLINE])
        .bind(since_epoch_ms as f64)
        .fetch_one(pool)
        .await;
    let (rows, oldest, newest, now) = match row {
        Ok(row) => row,
        Err(error) => {
            shared.incomplete(format!("heartbeat sample query failed: {error}"), vec![]);
            return true;
        }
    };
    if rows == 0 {
        return false;
    }
    // Claimed, but not yet reached by a heartbeat pass.
    let (Some(oldest), Some(newest)) = (oldest, newest) else {
        return true;
    };
    let active = shared.abandon.child_active.load(Ordering::Relaxed);
    shared
        .abandon
        .note_heartbeat_sample(shared, rows, oldest, newest, now, active);
    true
}

/// Probes the backlog every few seconds and the claim ring every second, until the
/// run is over.
async fn monitor(shared: Arc<Shared>, queues: Vec<String>) {
    let schema = shared
        .info
        .queue_storage_schema
        .clone()
        .unwrap_or_else(|| "awa".to_string());
    let Ok(pool) = connect_pool_with_timeout(
        &shared.config.database_url,
        "soak_abandon_monitor",
        2,
        Some(Duration::from_secs(60)),
    )
    .await
    else {
        shared.violation(
            "persona_failed",
            "abandon monitor could not connect",
            vec![],
        );
        return;
    };
    let staleness_secs = shared.config.heartbeat_staleness.as_secs_f64();
    let mut tick = 0u64;
    let mut empty_samples = 0u32;
    let mut lock_captures = super::super::ring_locks::CaptureBudget::default();
    while !shared.drain_stop.sleep(Duration::from_secs(1)).await {
        tick += 1;
        // The cursor lives where `ring_cursor_authority` says: the latest ledger row, or
        // the state singleton's columns before the authority flip.
        let ring: Result<(i32, i64, i32), _> = sqlx::query_as(awa::audited_sql(format!(
            "SELECT CASE WHEN authority.authority = 'ledger' THEN ledger.slot
                         ELSE state.current_slot END,
                    CASE WHEN authority.authority = 'ledger' THEN ledger.generation
                         ELSE state.generation END,
                    state.slot_count
             FROM {schema}.claim_ring_state AS state
             CROSS JOIN {schema}.ring_cursor_authority AS authority
             LEFT JOIN LATERAL (
                 SELECT slot, generation FROM {schema}.claim_ring_rotations
                 ORDER BY generation DESC LIMIT 1
             ) AS ledger ON TRUE
             WHERE state.singleton AND authority.singleton"
        )))
        .fetch_one(&pool)
        .await;
        let (slot, ring_generation, slot_count) = match ring {
            Ok(ring) => ring,
            Err(error) => {
                shared.incomplete(format!("claim-ring monitor query failed: {error}"), vec![]);
                continue;
            }
        };
        if let Some((generation, spawned)) = shared.abandon.live() {
            let holding = shared.abandon.child_active.load(Ordering::Relaxed);
            if sample_heartbeats(&shared, &pool, &schema, spawned).await || holding <= 0 {
                empty_samples = 0;
            } else {
                empty_samples += 1;
                if empty_samples == EMPTY_SAMPLES_BEFORE_INCOMPLETE {
                    shared.incomplete(
                        format!(
                            "the heartbeat sample found no open claim of process {generation} in {empty_samples} consecutive samples while it reported {holding} running handler(s)"
                        ),
                        vec![],
                    );
                }
            }
        } else {
            empty_samples = 0;
        }
        // While the ring is still, look at the slot it has to enter next: live claims
        // there explain the stall.
        let next_slot = if shared.abandon.ring_stalled() && tick.is_multiple_of(2) {
            probe_backlog(
                &pool,
                &schema,
                Scope::Slot((slot + 1).rem_euclid(slot_count)),
                staleness_secs,
                0,
            )
            .await
            .map(|probe| (probe.open - probe.expired, probe.expired))
            .map_err(|error| {
                shared.incomplete(format!("claim-slot monitor query failed: {error}"), vec![])
            })
            .ok()
        } else {
            None
        };
        shared
            .abandon
            .observe_ring(&shared, slot, ring_generation, next_slot);
        super::super::ring_locks::observe(
            &shared,
            &pool,
            &schema,
            slot_count,
            next_slot,
            &mut lock_captures,
        )
        .await;
        if tick.is_multiple_of(3) {
            // Read what the live process has started before the probe, so a claim started
            // meanwhile cannot make a claim look missing.
            let live = shared.abandon.live();
            let live_check = live.map(|(generation, _)| LiveCheck {
                generation,
                started: shared.abandon.started_by(generation),
            });
            let started = Instant::now();
            match probe_backlog(
                &pool,
                &schema,
                Scope::Queues(&queues),
                staleness_secs,
                live.map_or(0, |(_, spawned)| spawned),
            )
            .await
            {
                Ok(probe) => shared.abandon.observe(
                    &shared,
                    &probe,
                    live_check,
                    ring_generation,
                    started.elapsed().as_millis() as i64,
                ),
                Err(error) => {
                    shared.incomplete(
                        format!("abandoned-claim backlog query failed: {error}"),
                        vec![],
                    );
                    shared.stats(Persona::ClaimAbandon).add("probe_errors", 1);
                    if shared.stats(Persona::ClaimAbandon).get("probe_errors") <= 3 {
                        shared.note("abandon", format!("backlog probe failed: {error}"));
                    }
                }
            }
        }
    }
    shared.abandon.finish_ring(&shared);
    pool.close().await;
}

pub async fn run(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::ClaimAbandon;
    let config = shared.config.clone();
    let staleness_secs = config.heartbeat_staleness.as_secs_f64();
    let plain = shared.queues.register(QUEUE_PLAIN, persona, false);
    let deadline = shared.queues.register(QUEUE_DEADLINE, persona, false);
    let canary = shared.queues.register(QUEUE_CANARY, persona, false);
    let queues = vec![QUEUE_PLAIN.to_string(), QUEUE_DEADLINE.to_string()];
    if config.abandon_kill_every >= DEADLINE {
        return Err(format!(
            "AWA_SOAK_ABANDON_KILL_SECONDS must stay below the {}s job deadline, or the owner's own deadline rescue would count as a false rescue",
            DEADLINE.as_secs()
        ));
    }
    let target = config.abandon_claims.max(2);
    let workers_per_queue = (target / 2).max(1) as u32;

    std::fs::create_dir_all(&config.report_dir).map_err(|error| error.to_string())?;
    let ledger_path = config.report_dir.join("abandon-child-ledger.jsonl");
    let _ = std::fs::remove_file(&ledger_path);
    let executable = copy_executable(&config.report_dir)?;
    shared
        .abandon
        .set_csv(config.report_dir.join("abandon-backlog.csv"));

    let child_pool = shared.budget.reserve("abandon-child", CHILD_POOL, 4);
    // Held for the runtime that takes over at the end; Fleet::start replaces it.
    shared.budget.reserve(
        "abandon-finisher",
        FINISHER_POOL * FINISHER_REPLICAS as u32,
        FINISHER_REPLICAS as u32 * 2,
    );

    // The survivors: other replicas of the application. They lead maintenance, so they do
    // the rescuing, and they serve a canary queue.
    let canary_shared = shared.clone();
    let survivor_factory: ClientFactory = Arc::new(move |pool| {
        tune(
            Client::builder(pool)
                .queue(QUEUE_CANARY, queue(4))
                .register_worker(CanaryWorker {
                    shared: canary_shared.clone(),
                }),
            &canary_shared,
        )
        .build()
        .expect("build abandon survivor client")
    });
    let survivors = Fleet::start(
        &shared,
        "abandon-survivor",
        persona,
        2,
        8,
        2,
        vec![(canary, 4)],
        survivor_factory,
    )
    .await?;

    let producer_pool = connect_pool(
        &config.database_url,
        "soak_abandon_producer",
        shared.budget.reserve("abandon-producer", 4, 2),
    )
    .await
    .map_err(|error| error.to_string())?;
    let canary_task = tokio::spawn(canary_producer(shared.clone(), producer_pool.clone()));
    let monitor_task = tokio::spawn(monitor(shared.clone(), queues.clone()));
    let backlog_pool = connect_pool_with_timeout(
        &config.database_url,
        "soak_abandon_control",
        2,
        Some(Duration::from_secs(60)),
    )
    .await
    .map_err(|error| error.to_string())?;

    let mut rng = Rng::for_task(config.seed, "abandon");
    let mut importer = Importer {
        path: ledger_path.clone(),
        offset: 0,
    };
    let mut seq = 0u64;
    let schema = shared
        .info
        .queue_storage_schema
        .clone()
        .unwrap_or_else(|| "awa".to_string());
    shared.note(
        "abandon",
        format!(
            "armed: {} kill(s) every {}s, each leaving about {target} open claims ({} plain, {} with a deadline); rescue must clear within {}s",
            config.abandon_kills,
            config.abandon_kill_every.as_secs(),
            target / 2,
            target - target / 2,
            config.abandon_bound.as_secs()
        ),
    );

    let mut generation = 0u32;
    let mut load_ended = false;
    for number in 1..=config.abandon_kills {
        if shared.load_stop.is_stopped() {
            break;
        }
        // The first process finds a full queue. Later ones find the rescued jobs of the
        // earlier kills and only need fresh jobs to top their capacity up, so that jobs
        // are run, and abandoned, more than once.
        let fresh = if number == 1 { target } else { target / 2 };
        match enqueue_cycle(
            &shared,
            &producer_pool,
            &mut rng,
            &mut seq,
            number,
            fresh as usize,
        )
        .await
        {
            Ok(count) => shared.note(
                "abandon",
                format!("enqueued {count} fresh job(s) for process {number}"),
            ),
            Err(error) => {
                backoff(&shared, error, persona).await;
                continue;
            }
        }
        generation += 1;
        let mut child = spawn_child(
            &shared,
            &executable,
            generation,
            &ledger_path,
            workers_per_queue,
            child_pool,
        )?;
        shared.abandon.child_alive.store(true, Ordering::Relaxed);
        shared.abandon.child_active.store(0, Ordering::Relaxed);
        shared
            .abandon
            .set_live(Some((generation, epoch_ms() - 1_000)));
        let spawned = Instant::now();
        let mut ramped = false;
        shared.note(
            "abandon",
            format!("process {generation} started with {workers_per_queue} workers on each queue"),
        );
        let mut unexpected_exit = false;
        loop {
            importer.import(&shared);
            if shared.load_stop.sleep(Duration::from_secs(1)).await {
                load_ended = true;
                break;
            }
            if let Ok(Some(status)) = child.try_wait() {
                shared.violation(
                    "abandon_process_exited",
                    format!("the claiming process {generation} exited on its own: {status}"),
                    vec![],
                );
                unexpected_exit = true;
                break;
            }
            if !ramped
                && shared.abandon.child_active.load(Ordering::Relaxed) >= (target as i64) * 95 / 100
            {
                ramped = true;
                shared.note(
                    "abandon",
                    format!(
                        "process {generation} holds {} jobs after {:.0}s",
                        shared.abandon.child_active.load(Ordering::Relaxed),
                        spawned.elapsed().as_secs_f64()
                    ),
                );
                shared
                    .stats(persona)
                    .observe("ramp_seconds", spawned.elapsed().as_secs() as i64);
            }
            if spawned.elapsed() >= config.abandon_kill_every {
                break;
            }
        }

        let held = shared.abandon.child_active.load(Ordering::Relaxed);
        let signalled_epoch_ms = if unexpected_exit {
            let at = epoch_ms();
            shared.abandon.note_generation_killed(generation, at);
            shared.abandon.set_live(None);
            at
        } else {
            // SIGKILL: no shutdown handshake, no chance to release a claim.
            match shared
                .abandon
                .signal_generation_killed(generation, || child.start_kill())
            {
                Ok(at) => at,
                Err(error) => {
                    shared.incomplete(
                        format!("could not signal claiming process {generation}: {error}"),
                        vec![],
                    );
                    // No confirmed signal means no valid death boundary. Stop this
                    // persona and clean up its owned tasks instead of inventing one.
                    shared.abandon.set_live(None);
                    shared.abandon.child_alive.store(false, Ordering::Relaxed);
                    shared.abandon.child_active.store(0, Ordering::Relaxed);
                    drop(child); // kill_on_drop remains a best-effort cleanup.
                    canary_task.abort();
                    monitor_task.abort();
                    let _ = canary_task.await;
                    let _ = monitor_task.await;
                    survivors
                        .shutdown_all(Duration::from_secs(20), &shared)
                        .await;
                    producer_pool.close().await;
                    backlog_pool.close().await;
                    return Ok(());
                }
            }
        };
        let signalled_run_ms = shared.now_ms();
        shared.abandon.child_alive.store(false, Ordering::Relaxed);
        shared.abandon.child_active.store(0, Ordering::Relaxed);
        if !unexpected_exit {
            if let Err(error) = child.wait().await {
                shared.incomplete(
                    format!(
                        "could not reap claiming process {generation} after signaling it: {error}"
                    ),
                    vec![],
                );
            }
        }
        importer.import(&shared);
        let open_after_kill = probe_backlog(
            &backlog_pool,
            &schema,
            Scope::Queues(&queues),
            staleness_secs,
            0,
        )
        .await
        .map(|probe| probe.open)
        .unwrap_or_else(|error| {
            shared.incomplete(format!("post-kill backlog query failed: {error}"), vec![]);
            -1
        });
        let started = shared.abandon.started_by(generation);
        shared.abandon.record_kill(Kill {
            number,
            generation,
            at_run_ms: signalled_run_ms,
            signalled_epoch_ms,
            started_by_process: started,
            rescued_while_alive: shared.abandon.live_lost(generation),
            open_after_kill,
            expired_peak: 0,
            oldest_expired_peak_s: 0.0,
            cleared_after_s: None,
            reported_slow: false,
            load_ended,
        });
        shared.stats(persona).add("kills", 1);
        shared.stats(persona).add("claims_abandoned", started);
        shared.note(
            "abandon",
            format!(
                "SIGKILL process {generation} (kill {number}): {started} handlers started, {held} running at the last report, {open_after_kill} open claims on its queues"
            ),
        );
        if load_ended {
            break;
        }
        if shared.load_stop.sleep(config.abandon_restart_delay).await {
            break;
        }
    }

    // Everything the kills left behind is finished by an in-process runtime that is
    // never killed.
    let finisher_shared = shared.clone();
    let finisher_factory: ClientFactory = Arc::new(move |pool| {
        tune(
            Client::builder(pool)
                .queue(QUEUE_PLAIN, queue(256))
                .queue(QUEUE_DEADLINE, queue(256))
                .register_worker(FinisherWorker {
                    shared: finisher_shared.clone(),
                }),
            &finisher_shared,
        )
        .build()
        .expect("build abandon finisher client")
    });
    shared.note(
        "abandon",
        "kills over; starting the runtime that finishes the rescued jobs",
    );
    let finisher = Fleet::start(
        &shared,
        "abandon-finisher",
        persona,
        FINISHER_REPLICAS,
        FINISHER_POOL,
        4,
        vec![(plain, 256), (deadline, 256)],
        finisher_factory,
    )
    .await?;

    while !shared.drain_stop.sleep(Duration::from_secs(1)).await {
        importer.import(&shared);
    }
    importer.import(&shared);
    canary_task.abort();
    let _ = monitor_task.await;
    shared
        .abandon
        .analyse(&shared, &backlog_pool, &queues)
        .await;
    finisher
        .shutdown_all(Duration::from_secs(20), &shared)
        .await;
    survivors
        .shutdown_all(Duration::from_secs(20), &shared)
        .await;
    producer_pool.close().await;
    backlog_pool.close().await;
    Ok(())
}
