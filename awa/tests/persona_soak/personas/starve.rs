//! Starve: one queue under strict priority (priority aging off) whose priority-1 lane
//! never runs dry, with a trickle of priority-4 jobs. Without escalation the
//! priority-4 lane is never claimed while load runs, and its rows pin the queue ring
//! (#779). With escalation (the default) its head is claimed once it pins. Records
//! each priority-4 job's wait, the queue ring's pin, rotations and footprint.

use super::super::config::Persona;
use super::super::db::{connect_pool, connect_sampler_pool, ClientFactory};
use super::super::fleet::Fleet;
use super::super::ledger::{expect, flag};
use super::super::shared::Shared;
use super::{backoff, register_rows, tune};
use awa::model::insert_many;
use awa::model::queue_storage::{QueueStorage, QueueStorageConfig};
use awa::{
    Client, InsertOpts, InsertParams, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::io::Write;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const QUEUE: &str = "awa_soak_starve";
const BUSY_PRIORITY: i16 = 1;
const STARVED_PRIORITY: i16 = 4;
const SAMPLE_EVERY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct StarveJob {
    pub seq: u64,
    /// Harness clock (ms since the run started) when the job was enqueued.
    pub enqueued_ms: i64,
    /// The priority it was enqueued at. A claim with an aging interval returns the job
    /// at its aged priority, so `JobRow::priority` can't tell the lanes apart.
    pub lane_priority: i16,
}

/// One priority-4 job: harness milliseconds when it was enqueued, started and finished.
struct StarvedJob {
    job_id: i64,
    enqueued_ms: i64,
    started_ms: i64,
    finished_ms: i64,
}

struct State {
    busy_enqueued: AtomicI64,
    busy_completed: AtomicI64,
    starved: Mutex<Vec<StarvedJob>>,
}

struct StarveWorker {
    shared: Arc<Shared>,
    state: Arc<State>,
    handler: Duration,
}

#[async_trait::async_trait]
impl Worker for StarveWorker {
    fn kind(&self) -> &'static str {
        "starve_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let _guard = self.shared.begin_execution(Persona::Starve, &ctx.job, true);
        let started_ms = self.shared.now_ms();
        let args: StarveJob = serde_json::from_value(ctx.job.args.clone())
            .map_err(|error| JobError::terminal(error.to_string()))?;
        tokio::time::sleep(self.handler).await;
        self.shared
            .record_completed_return(Persona::Starve, &ctx.job)
            .await;
        let stats = self.shared.stats(Persona::Starve);
        if args.lane_priority == STARVED_PRIORITY {
            let finished_ms = self.shared.now_ms();
            stats.add("p4_completed", 1);
            stats.observe("p4_wait_ms", finished_ms - args.enqueued_ms);
            stats.observe("p4_claim_wait_ms", started_ms - args.enqueued_ms);
            self.state
                .starved
                .lock()
                .expect("starved jobs lock")
                .push(StarvedJob {
                    job_id: ctx.job.id,
                    enqueued_ms: args.enqueued_ms,
                    started_ms,
                    finished_ms,
                });
        } else {
            self.state.busy_completed.fetch_add(1, Ordering::Relaxed);
            stats.add("p1_completed", 1);
            stats.observe("p1_claim_wait_ms", started_ms - args.enqueued_ms);
        }
        Ok(JobResult::Completed)
    }
}

fn client_factory(shared: Arc<Shared>, state: Arc<State>) -> ClientFactory {
    Arc::new(move |pool| {
        let config = QueueConfig {
            max_workers: shared.config.starve_workers,
            priority_aging_interval: Duration::ZERO,
            ring_pin_aging_escalation: shared.config.starve_escalation,
            ..QueueConfig::default()
        };
        tune(Client::builder(pool).queue(QUEUE, config), &shared)
            .register_worker(StarveWorker {
                shared: shared.clone(),
                state: state.clone(),
                handler: shared.config.starve_handler,
            })
            .build()
            .expect("build starve client")
    })
}

fn params(shared: &Shared, seq: &mut u64, count: usize, priority: i16) -> Vec<InsertParams> {
    let enqueued_ms = shared.now_ms();
    (0..count)
        .map(|_| {
            *seq += 1;
            awa::insert::params_with(
                &StarveJob {
                    seq: *seq,
                    enqueued_ms,
                    lane_priority: priority,
                },
                InsertOpts {
                    queue: QUEUE.to_string(),
                    priority,
                    max_attempts: 5,
                    ..Default::default()
                },
            )
            .expect("starve params")
        })
        .collect()
}

pub async fn run(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::Starve;
    let queue = shared.queues.register(QUEUE, persona, false);
    let state = Arc::new(State {
        busy_enqueued: AtomicI64::new(0),
        busy_completed: AtomicI64::new(0),
        starved: Mutex::new(Vec::new()),
    });
    let workers = shared.config.starve_workers;
    let fleet = Fleet::start(
        &shared,
        "starve",
        persona,
        1,
        workers + 4,
        2,
        vec![(queue, i64::from(workers))],
        client_factory(shared.clone(), state.clone()),
    )
    .await?;

    let producer_pool = connect_pool(
        &shared.config.database_url,
        "soak_starve_producer",
        shared.budget.reserve("starve-producer", 2, 1),
    )
    .await
    .map_err(|error| error.to_string())?;
    let sampler_pool = connect_sampler_pool(
        &shared.config.database_url,
        "soak_starve_sampler",
        shared.budget.reserve("starve-sampler", 1, 1),
        shared.config.diag_statement_timeout,
        shared.config.diag_lock_timeout,
    )
    .await
    .map_err(|error| error.to_string())?;
    let sampler = tokio::spawn(sample_ring(shared.clone(), sampler_pool.clone()));

    let depth = shared.config.starve_depth as i64;
    let trickle = shared.config.starve_trickle;
    shared.note(
        "starve",
        format!(
            "{workers} workers, priority aging off, escalation {}; priority-1 backlog kept at {depth}, one priority-4 job every {:?}",
            if shared.config.starve_escalation { "on" } else { "off" },
            trickle
        ),
    );
    let mut seq = 0u64;
    let mut next_trickle = tokio::time::Instant::now();
    while !shared.load_stop.sleep(Duration::from_millis(100)).await {
        let backlog = state.busy_enqueued.load(Ordering::Relaxed)
            - state.busy_completed.load(Ordering::Relaxed);
        let mut batch = Vec::new();
        let busy = (depth - backlog).clamp(0, 2_000) as usize;
        if busy > 0 {
            batch.extend(params(&shared, &mut seq, busy, BUSY_PRIORITY));
        }
        let mut starved = 0;
        if tokio::time::Instant::now() >= next_trickle {
            next_trickle += trickle;
            batch.extend(params(&shared, &mut seq, 1, STARVED_PRIORITY));
            starved = 1;
        }
        if batch.is_empty() {
            continue;
        }
        match insert_many(&producer_pool, &batch).await {
            Ok(rows) => {
                state
                    .busy_enqueued
                    .fetch_add((rows.len() - starved) as i64, Ordering::Relaxed);
                let stats = shared.stats(persona);
                stats.add("enqueued", rows.len() as i64);
                stats.add("p1_enqueued", (rows.len() - starved) as i64);
                stats.add("p4_enqueued", starved as i64);
                register_rows(
                    &shared,
                    persona,
                    &rows,
                    |_| expect::COMPLETED,
                    flag::NO_EVENTS,
                );
            }
            Err(error) => backoff(&shared, error, persona).await,
        }
    }

    shared.note("starve", format!("load stopped after {seq} jobs; draining"));
    shared.drain_stop.wait().await;
    fleet.shutdown_all(Duration::from_secs(20), &shared).await;
    sampler.abort();
    write_starved(&shared, &state);
    producer_pool.close().await;
    sampler_pool.close().await;
    Ok(())
}

/// Every [`SAMPLE_EVERY`]: the queue ring's generation, the lanes pinning it and the
/// size of its tables, appended to `starve-ring.csv`.
async fn sample_ring(shared: Arc<Shared>, pool: PgPool) {
    let schema = shared
        .info
        .queue_storage_schema
        .clone()
        .unwrap_or_else(|| "awa".to_string());
    let store = match QueueStorage::new(QueueStorageConfig {
        schema: schema.clone(),
        ..QueueStorageConfig::default()
    }) {
        Ok(store) => store,
        Err(error) => {
            shared.note("starve", format!("ring sampler disabled: {error}"));
            return;
        }
    };
    let path = shared.config.report_dir.join("starve-ring.csv");
    let mut file = match std::fs::File::create(&path) {
        Ok(mut file) => {
            let _ = writeln!(
                file,
                "run_s,generation,current_slot,pinned,pin_slot,pin_lanes,starve_lane_pins,oldest_pin_head_age_s,footprint_mb"
            );
            file
        }
        Err(error) => {
            shared.note("starve", format!("ring sampler disabled: {error}"));
            return;
        }
    };
    let footprint_sql = format!(
        "SELECT COALESCE(sum(pg_total_relation_size(tree.relid)), 0)::bigint FROM unnest(ARRAY[\
         '{schema}.ready_entries', '{schema}.done_entries', '{schema}.ready_segments', \
         '{schema}.ready_tombstones', '{schema}.ready_claim_attempt_batches']::regclass[]) AS parent \
         CROSS JOIN LATERAL pg_partition_tree(parent) AS tree"
    );
    let ring_sql = format!(
        "SELECT generation, current_slot FROM {schema}.queue_ring_state WHERE singleton = TRUE"
    );
    let stats = shared.stats(Persona::Starve);
    loop {
        tokio::time::sleep(SAMPLE_EVERY).await;
        let run_s = shared.now_ms() as f64 / 1000.0;
        let ring: Result<(i64, i32), _> = sqlx::query_as(&ring_sql).fetch_one(&pool).await;
        let pin = store.queue_ring_pin(&pool).await;
        let footprint: Result<i64, _> = sqlx::query_scalar(&footprint_sql).fetch_one(&pool).await;
        let (Ok((generation, current_slot)), Ok(pin), Ok(footprint)) = (ring, pin, footprint)
        else {
            stats.add("ring_samples_skipped", 1);
            continue;
        };
        let footprint_mb = footprint as f64 / 1_048_576.0;
        let (pin_slot, lanes, starve_pins, oldest) = match &pin {
            Some(pin) => (
                pin.slot.to_string(),
                pin.lanes
                    .iter()
                    .map(|lane| format!("{}:{}", lane.queue, lane.priority))
                    .collect::<Vec<_>>()
                    .join(" "),
                pin.lanes.iter().any(|lane| lane.queue == QUEUE),
                pin.lanes
                    .iter()
                    .map(|lane| lane.head_age.as_secs_f64())
                    .fold(0.0, f64::max),
            ),
            None => (String::new(), String::new(), false, 0.0),
        };
        stats.add("ring_samples", 1);
        if pin.is_some() {
            stats.add("ring_samples_pinned", 1);
        }
        if starve_pins {
            stats.add("ring_samples_pinned_by_starve", 1);
        }
        stats.set_max("ring_footprint_mb_max", footprint_mb.round() as i64);
        stats.set_max("pin_head_age_s_max", oldest.round() as i64);
        stats.set_max("queue_ring_generation", generation);
        let _ = writeln!(
            file,
            "{run_s:.1},{generation},{current_slot},{},{pin_slot},{lanes},{starve_pins},{oldest:.1},{footprint_mb:.1}",
            pin.is_some()
        );
    }
}

/// Writes every priority-4 job's times to `starve-p4.csv`, and flags a starved job on a
/// run with escalation on.
fn write_starved(shared: &Shared, state: &State) {
    let jobs = state.starved.lock().expect("starved jobs lock");
    let path = shared.config.report_dir.join("starve-p4.csv");
    let written = (|| -> std::io::Result<()> {
        let mut file = std::fs::File::create(path)?;
        writeln!(file, "job_id,enqueued_ms,started_ms,finished_ms,wait_ms")?;
        for job in jobs.iter() {
            writeln!(
                file,
                "{},{},{},{},{}",
                job.job_id,
                job.enqueued_ms,
                job.started_ms,
                job.finished_ms,
                job.finished_ms - job.enqueued_ms
            )?;
        }
        Ok(())
    })();
    if let Err(error) = written {
        shared.note("starve", format!("could not write starve-p4.csv: {error}"));
    }
    if !shared.config.starve_escalation {
        return;
    }
    let bound = shared.config.starve_max_wait.as_millis() as i64;
    let late: Vec<&StarvedJob> = jobs
        .iter()
        .filter(|job| job.finished_ms - job.enqueued_ms > bound)
        .collect();
    if let Some(worst) = late
        .iter()
        .max_by_key(|job| job.finished_ms - job.enqueued_ms)
    {
        shared.violation(
            "starved_lane_unserved",
            format!(
                "{} priority-4 job(s) waited longer than {}s with ring-pin escalation on; worst {}s",
                late.len(),
                bound / 1000,
                (worst.finished_ms - worst.enqueued_ms) / 1000
            ),
            late.iter()
                .take(10)
                .map(|job| format!("{{\"job_id\":{},\"wait_ms\":{}}}", job.job_id, job.finished_ms - job.enqueued_ms))
                .collect(),
        );
    }
}
