//! Shard operator: an operator changes `awa.queue_meta.enqueue_shards` on two hot queues
//! while SQL-path producers (`insert_many`, `insert_many_copy`), `QueueStorage` producers,
//! promotion (scheduled jobs and retries) and claimers run, as different non-superuser
//! roles (see `roles.rs`).
//!
//! The operator raises a queue from 1 to 4 to 8 shards, lowers it to 2 and back to 1,
//! sometimes running `prepare_queue` first and sometimes not, restarting some runtimes
//! before others so cached shard counts disagree across the fleet, and, for some raises,
//! holding the producers back until the maintenance leader's runtime has created the new
//! lanes so a different role is the next to use them.

use super::super::config::Persona;
use super::super::db::{connect_pool, ClientFactory};
use super::super::fleet::Fleet;
use super::super::ledger::flag;
use super::super::registry::QueueInfo;
use super::super::rng::Rng;
use super::super::roles;
use super::super::shard_oracle;
use super::super::shared::Shared;
use super::super::stop::Stop;
use super::{attach_events, backoff, queue, register_rows, tune, ANY};
use awa::model::{insert_many, insert_many_copy_from_pool, QueueStorage, QueueStorageConfig};
use awa::{Client, InsertOpts, InsertParams, JobArgs, JobContext, JobError, JobResult, Worker};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const HOT_QUEUE: &str = "awa_soak_shardops_hot";
pub const WARM_QUEUE: &str = "awa_soak_shardops_warm";
const QUEUES: [&str; 2] = [HOT_QUEUE, WARM_QUEUE];
const KEYS: u64 = 16;
const WORKERS: u32 = 8;
/// The sequence of shard counts each queue is taken through, repeatedly.
const STEPS: [(i16, i16); 4] = [(1, 4), (4, 8), (8, 2), (2, 1)];
const MAINTENANCE_LEADER_LOCK_KEY: i64 = 0x4157_415f_4d41_494e;

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct ShardOpsJob {
    pub seq: u64,
    pub mode: String,
    pub key: Option<u64>,
    pub key_seq: u64,
}

struct ShardOpsWorker {
    shared: Arc<Shared>,
}

#[async_trait::async_trait]
impl Worker for ShardOpsWorker {
    fn kind(&self) -> &'static str {
        "shard_ops_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let _guard = self
            .shared
            .begin_execution(Persona::ShardOps, &ctx.job, false);
        let args: ShardOpsJob = serde_json::from_value(ctx.job.args.clone())
            .map_err(|error| JobError::terminal(format!("bad args: {error}")))?;
        self.shared.shardops.note_execution(
            args.seq,
            ctx.job.id,
            args.key.map(|key| (key, args.key_seq)),
        );
        tokio::time::sleep(Duration::from_millis(5 + args.seq % 20)).await;
        self.shared
            .shardops
            .note_returned(args.seq, self.shared.chaos_active());
        if args.mode == "retry" && ctx.job.attempt <= 1 {
            return Ok(JobResult::RetryAfter(Duration::from_millis(
                1_000 + args.seq % 2_000,
            )));
        }
        self.shared
            .ledger
            .mark(ctx.job.id, flag::RETURNED_COMPLETED);
        Ok(JobResult::Completed)
    }
}

fn client_factory(shared: Arc<Shared>) -> ClientFactory {
    Arc::new(move |pool| {
        let builder = Client::builder(pool)
            .queue(HOT_QUEUE, queue(WORKERS))
            .queue(WARM_QUEUE, queue(WORKERS))
            .register_worker(ShardOpsWorker {
                shared: shared.clone(),
            });
        attach_events(
            // Retrying election quickly lets these runtimes, which run as application
            // roles, take maintenance leadership from the other personas' runtimes.
            tune(builder, &shared).leader_election_interval(Duration::from_millis(100)),
            &shared,
            &["shard_ops_job"],
        )
        .build()
        .expect("build shard-ops client")
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Plain,
    Scheduled,
    Retry,
}

impl Mode {
    fn roll(rng: &mut Rng) -> Mode {
        match rng.below(100) {
            0..=11 => Mode::Retry,
            12..=31 => Mode::Scheduled,
            _ => Mode::Plain,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Mode::Plain => "plain",
            Mode::Scheduled => "scheduled",
            Mode::Retry => "retry",
        }
    }

    fn executions(self) -> u8 {
        if self == Mode::Retry {
            2
        } else {
            1
        }
    }
}

struct Built {
    params: InsertParams,
    seq: u64,
    executions: u8,
}

fn build(
    shared: &Shared,
    rng: &mut Rng,
    queue: &str,
    mode: Mode,
    key: Option<(u64, u64)>,
    run_in: Option<Duration>,
) -> Result<Built, String> {
    let seq = shared.shardops.next_seq();
    let run_at = match (mode, run_in) {
        (_, Some(delay)) => {
            Some(Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default())
        }
        (Mode::Scheduled, None) => {
            Some(Utc::now() + chrono::Duration::milliseconds(rng.range(800, 6_000) as i64))
        }
        _ => None,
    };
    let params = awa::insert::params_with(
        &ShardOpsJob {
            seq,
            mode: mode.name().to_string(),
            key: key.map(|(key, _)| key),
            key_seq: key.map_or(0, |(_, key_seq)| key_seq),
        },
        InsertOpts {
            queue: queue.to_string(),
            max_attempts: 5,
            run_at,
            ordering_key: key.map(|(key, _)| key.to_be_bytes().to_vec()),
            ..Default::default()
        },
    )
    .map_err(|error| error.to_string())?;
    Ok(Built {
        params,
        seq,
        executions: mode.executions(),
    })
}

fn pick_queue<'a>(rng: &mut Rng) -> &'a str {
    QUEUES[rng.below(QUEUES.len() as u64) as usize]
}

fn count_enqueued(shared: &Shared, queues: impl Iterator<Item = String>) {
    let mut count = 0;
    for name in queues {
        count += 1;
        if let Some(queue) = shared.queues.get(&name) {
            queue.enqueued.fetch_add(1, Ordering::Relaxed);
        }
    }
    shared.stats(Persona::ShardOps).add("enqueued", count);
}

/// Keeps the operator from changing the shard count while an insert is mid-flight
/// during the leader-first hold.
struct InsertGuard<'a>(&'a Shared);

impl<'a> InsertGuard<'a> {
    fn new(shared: &'a Shared) -> Self {
        shared
            .shardops
            .producers_inserting
            .fetch_add(1, Ordering::SeqCst);
        Self(shared)
    }
}

impl Drop for InsertGuard<'_> {
    fn drop(&mut self) {
        self.0
            .shardops
            .producers_inserting
            .fetch_sub(1, Ordering::SeqCst);
    }
}

async fn enqueue_failed(
    shared: &Shared,
    role: &str,
    operation: &str,
    error: impl std::fmt::Display,
) {
    let message = error.to_string();
    let short: String = message.chars().take(120).collect();
    shared.shardops.violation_once(
        shared,
        "shardops_enqueue_error",
        &format!("{role}:{operation}:{short}"),
        format!("role {role} failed {operation}: {message}"),
        vec![format!(
            "{{\"role\":\"{role}\",\"statement\":\"{operation}\",\"error\":{}}}",
            serde_json::Value::String(message.clone())
        )],
    );
    backoff(shared, &message, Persona::ShardOps).await;
}

/// A producer's `QueueStorage` handle. A runtime's cached shard count is only dropped
/// by a restart, so the operator replaces the handle to restart the producer.
struct Stores {
    handles: Mutex<Vec<Arc<QueueStorage>>>,
}

impl Stores {
    fn new(count: usize) -> Result<Self, String> {
        let handles = (0..count)
            .map(|_| fresh_store())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            handles: Mutex::new(handles),
        })
    }

    fn get(&self, index: usize) -> Arc<QueueStorage> {
        self.handles.lock().expect("stores")[index].clone()
    }

    fn renew(&self, index: usize) {
        if let Ok(store) = fresh_store() {
            self.handles.lock().expect("stores")[index] = store;
        }
    }

    fn len(&self) -> usize {
        self.handles.lock().expect("stores").len()
    }
}

fn fresh_store() -> Result<Arc<QueueStorage>, String> {
    QueueStorage::new(QueueStorageConfig::default())
        .map(Arc::new)
        .map_err(|error| error.to_string())
}

/// A SQL-path producer: `insert_many`, or `insert_many_copy` when `copy` is set.
async fn sql_producer(shared: Arc<Shared>, pool: PgPool, role: &'static str, copy: bool) {
    let label = if copy {
        "shardops-copy"
    } else {
        "shardops-sql"
    };
    let mut rng = Rng::for_task(shared.config.seed, label);
    let scale = shared.config.shardops_scale;
    let (tick, per_tick) = if copy {
        (Duration::from_secs(1), (40.0 * scale).ceil() as usize)
    } else {
        (Duration::from_millis(100), (4.0 * scale).ceil() as usize)
    };
    while !shared.load_stop.sleep(tick).await {
        if shared.shardops.producers_held() {
            continue;
        }
        let _guard = InsertGuard::new(&shared);
        let mut built = Vec::with_capacity(per_tick);
        for _ in 0..per_tick {
            let queue = pick_queue(&mut rng);
            let mode = Mode::roll(&mut rng);
            match build(&shared, &mut rng, queue, mode, None, None) {
                Ok(job) => built.push(job),
                Err(error) => {
                    shared.violation("shardops_build_failed", error, vec![]);
                    return;
                }
            }
        }
        let params: Vec<InsertParams> = built.iter().map(|job| job.params.clone()).collect();
        let result = if copy {
            insert_many_copy_from_pool(&pool, &params).await
        } else {
            insert_many(&pool, &params).await
        };
        match result {
            Ok(rows) => {
                for job in &built {
                    shared.shardops.note_enqueued(job.seq, job.executions);
                }
                count_enqueued(&shared, rows.iter().map(|row| row.queue.clone()));
                register_rows(&shared, Persona::ShardOps, &rows, |_| ANY, 0);
            }
            Err(error) => {
                enqueue_failed(
                    &shared,
                    role,
                    if copy {
                        "insert_many_copy"
                    } else {
                        "insert_many"
                    },
                    error,
                )
                .await
            }
        }
    }
}

/// A producer that enqueues through `QueueStorage`, the runtime's own path.
async fn store_producer(
    shared: Arc<Shared>,
    pool: PgPool,
    role: &'static str,
    stores: Arc<Stores>,
    store_index: usize,
) {
    let mut rng = Rng::for_task(shared.config.seed, "shardops-store");
    let per_tick = (3.0 * shared.config.shardops_scale).ceil() as usize;
    while !shared.load_stop.sleep(Duration::from_millis(100)).await {
        if shared.shardops.producers_held() {
            continue;
        }
        let _guard = InsertGuard::new(&shared);
        let mut built = Vec::with_capacity(per_tick);
        for _ in 0..per_tick {
            let queue = pick_queue(&mut rng);
            let mode = Mode::roll(&mut rng);
            match build(&shared, &mut rng, queue, mode, None, None) {
                Ok(job) => built.push(job),
                Err(error) => {
                    shared.violation("shardops_build_failed", error, vec![]);
                    return;
                }
            }
        }
        let params: Vec<InsertParams> = built.iter().map(|job| job.params.clone()).collect();
        let store = stores.get(store_index);
        match store.enqueue_params_batch(&pool, &params).await {
            Ok(_) => {
                for job in &built {
                    shared.shardops.note_enqueued(job.seq, job.executions);
                }
                count_enqueued(&shared, params.iter().map(|job| job.opts.queue.clone()));
            }
            Err(error) => {
                enqueue_failed(&shared, role, "QueueStorage::enqueue_params_batch", error).await
            }
        }
    }
}

/// Jobs with an ordering key, enqueued strictly in sequence per key, alternating between
/// the SQL path as one role and the `QueueStorage` path as the other.
async fn keyed_producer(
    shared: Arc<Shared>,
    sql_pool: PgPool,
    store_pool: PgPool,
    stores: Arc<Stores>,
    store_index: usize,
) {
    let mut rng = Rng::for_task(shared.config.seed, "shardops-keyed");
    let per_tick = (3.0 * shared.config.shardops_scale).ceil() as usize;
    let mut key_seq = vec![0u64; KEYS as usize];
    let mut tick = 0u64;
    while !shared.load_stop.sleep(Duration::from_millis(100)).await {
        if shared.shardops.producers_held() {
            continue;
        }
        tick += 1;
        let _guard = InsertGuard::new(&shared);
        let mut built = Vec::with_capacity(per_tick);
        let mut keys = Vec::with_capacity(per_tick);
        for _ in 0..per_tick {
            let key = rng.below(KEYS);
            key_seq[key as usize] += 1;
            let queue = QUEUES[(key % 2) as usize];
            match build(
                &shared,
                &mut rng,
                queue,
                Mode::Plain,
                Some((key, key_seq[key as usize])),
                None,
            ) {
                Ok(job) => {
                    built.push(job);
                    keys.push((key, key_seq[key as usize]));
                }
                Err(error) => {
                    shared.violation("shardops_build_failed", error, vec![]);
                    return;
                }
            }
        }
        let params: Vec<InsertParams> = built.iter().map(|job| job.params.clone()).collect();
        let window_before = shared.shardops.window();
        let outcome = if tick.is_multiple_of(2) {
            match insert_many(&sql_pool, &params).await {
                Ok(rows) => {
                    register_rows(&shared, Persona::ShardOps, &rows, |_| ANY, 0);
                    Ok(rows.len())
                }
                Err(error) => Err((
                    roles::APP_A,
                    "insert_many (ordering_key)",
                    error.to_string(),
                )),
            }
        } else {
            let store = stores.get(store_index);
            match store.enqueue_params_batch(&store_pool, &params).await {
                Ok(count) => Ok(count),
                Err(error) => Err((
                    roles::APP_B,
                    "QueueStorage::enqueue_params_batch (ordering_key)",
                    error.to_string(),
                )),
            }
        };
        match outcome {
            Ok(_) => {
                let window_after = shared.shardops.window();
                let window = if window_before == window_after {
                    window_before
                } else {
                    0
                };
                for (job, (key, seq)) in built.iter().zip(&keys) {
                    shared.shardops.note_enqueued(job.seq, job.executions);
                    shared.shardops.note_keyed(*key, *seq, window);
                }
                shared
                    .stats(Persona::ShardOps)
                    .add("enqueued_keyed", built.len() as i64);
                count_enqueued(&shared, params.iter().map(|job| job.opts.queue.clone()));
            }
            Err((role, operation, error)) => enqueue_failed(&shared, role, operation, error).await,
        }
    }
}

/// Everything the operator acts through.
struct Operator {
    shared: Arc<Shared>,
    fleets: Vec<Arc<Fleet>>,
    stores: Arc<Stores>,
    owner_pool: PgPool,
    migrator_pool: PgPool,
    admin_pool: PgPool,
    app_pool: PgPool,
    rng: Rng,
}

impl Operator {
    fn note(&self, text: impl Into<String>) {
        self.shared.note("shards", text);
    }

    fn operator_error(&self, what: &str, error: impl std::fmt::Display) {
        let message = error.to_string();
        self.shared.shardops.violation_once(
            &self.shared,
            "shardops_operator_error",
            &format!("{what}:{message}"),
            format!("{what} failed: {message}"),
            vec![format!(
                "{{\"statement\":\"{what}\",\"error\":{}}}",
                serde_json::Value::String(message.clone())
            )],
        );
    }

    async fn prepare(&mut self, queue: &str, shards: i16) {
        let as_migrator = self.rng.chance(50);
        let (role, pool) = if as_migrator {
            (roles::MIGRATOR, &self.migrator_pool)
        } else {
            (roles::OWNER, &self.owner_pool)
        };
        self.note(format!(
            "prepare_queue {queue} for {shards} shards as {role}"
        ));
        let store = match fresh_store() {
            Ok(store) => store,
            Err(error) => return self.operator_error("prepare_queue", error),
        };
        if let Err(error) = store.prepare_queue(pool, queue, shards).await {
            self.operator_error(&format!("prepare_queue as {role}"), error);
        }
    }

    async fn set_shards(&self, queue: &str, shards: i16) {
        let result = sqlx::query(
            "INSERT INTO awa.queue_meta (queue, enqueue_shards) VALUES ($1, $2)
             ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards",
        )
        .bind(queue)
        .bind(shards)
        .execute(&self.owner_pool)
        .await;
        match result {
            Ok(_) => {
                let now = self.shared.now_ms();
                self.shared.shardops.plans.lock().expect("plans").insert(
                    queue.to_string(),
                    shard_oracle::QueuePlan {
                        configured: shards,
                        changed_at_ms: now,
                        settled_at_ms: None,
                    },
                );
            }
            Err(error) => self.operator_error("set enqueue_shards", error),
        }
    }

    /// Restarts runtime `(fleet, replica)`, which drops its cached shard counts.
    async fn restart(&self, fleet: usize, replica: usize) {
        let Some(target) = self.fleets.get(fleet) else {
            return;
        };
        match target.restart(replica, Duration::from_secs(20)).await {
            Ok(true) => self
                .shared
                .stats(Persona::ShardOps)
                .add("runtime_restarts", 1),
            Ok(false) => {}
            Err(error) => self.operator_error("runtime restart", error),
        }
    }

    async fn runtimes(&self) -> Vec<(usize, usize)> {
        let mut all = Vec::new();
        for (index, fleet) in self.fleets.iter().enumerate() {
            for replica in fleet.live_replicas().await {
                all.push((index, replica));
            }
        }
        all
    }

    fn settle(&self, queue: &str) {
        let now = self.shared.now_ms();
        if let Some(plan) = self
            .shared
            .shardops
            .plans
            .lock()
            .expect("plans")
            .get_mut(queue)
        {
            plan.settled_at_ms = Some(now);
        }
        self.shared.shardops.open_window();
    }

    /// The runtime holding maintenance leadership, as `(pid, role, application_name)`.
    async fn leader(&self) -> Option<(i32, String, String)> {
        sqlx::query_as(
            "SELECT l.pid, a.usename::text, a.application_name
             FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
             WHERE l.locktype = 'advisory' AND l.granted AND a.application_name LIKE 'soak\\_%'
               AND ((l.classid::bigint << 32) | l.objid::bigint) = $1
             ORDER BY l.pid LIMIT 1",
        )
        .bind(MAINTENANCE_LEADER_LOCK_KEY)
        .fetch_optional(&self.admin_pool)
        .await
        .ok()
        .flatten()
    }

    /// Makes one of this persona's runtimes the maintenance leader, ending other
    /// runtimes' leadership if needed. Returns its role.
    async fn lead_with_own_runtime(&self) -> Option<String> {
        for _ in 0..10 {
            if let Some((pid, role, application)) = self.leader().await {
                if application.starts_with("soak_shardops") {
                    self.note(format!("maintenance leader is {application} ({role})"));
                    return Some(role);
                }
                self.note(format!("ending leadership of {application} ({role})"));
                let _ = sqlx::query("SELECT pg_terminate_backend($1)")
                    .bind(pid)
                    .fetch_one(&self.admin_pool)
                    .await;
            }
            if self.shared.load_stop.sleep(Duration::from_secs(2)).await {
                return None;
            }
        }
        self.note("another persona's runtime kept maintenance leadership");
        None
    }

    async fn lane_owners(&self, queue: &str, above: i16) -> String {
        let owners: Result<Vec<(String, i64)>, _> = sqlx::query_as(
            "SELECT pg_get_userbyid(c.relowner)::text, count(*)::bigint
             FROM awa.queue_enqueue_heads h
             JOIN pg_class c ON c.relname = h.seq_name
             JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'awa'
             WHERE h.queue = $1 AND h.enqueue_shard >= $2
             GROUP BY 1 ORDER BY 1",
        )
        .bind(queue)
        .bind(above)
        .fetch_all(&self.admin_pool)
        .await;
        match owners {
            Ok(owners) if owners.is_empty() => "none yet".to_string(),
            Ok(owners) => owners
                .into_iter()
                .map(|(role, count)| format!("{role}={count}"))
                .collect::<Vec<_>>()
                .join(","),
            Err(error) => format!("lookup failed: {error}"),
        }
    }

    /// Returns false when the load stopped mid-change.
    async fn change(&mut self, queue: &str, from: i16, to: i16) -> bool {
        let raising = to > from;
        let prepare =
            raising && (!self.shared.config.shardops_schema_create || self.rng.chance(50));
        let leader_first = raising && !prepare && self.rng.chance(40);
        self.note(format!(
            "{queue}: {from} -> {to} shards{}{}",
            if prepare { ", prepare_queue first" } else { "" },
            if leader_first {
                ", leader creates the new lanes first"
            } else {
                ""
            }
        ));
        self.shared.stats(Persona::ShardOps).add("shard_changes", 1);
        if prepare {
            self.prepare(queue, to).await;
        }
        self.shared.shardops.close_window();

        if leader_first {
            self.shared.shardops.hold_producers(true);
            for _ in 0..40 {
                if self
                    .shared
                    .shardops
                    .producers_inserting
                    .load(Ordering::SeqCst)
                    == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            self.set_shards(queue, to).await;
            for (fleet, replica) in self.runtimes().await {
                self.restart(fleet, replica).await;
            }
            for index in 0..self.stores.len() {
                self.stores.renew(index);
            }
            let leader_role = self.lead_with_own_runtime().await;
            // Scheduled jobs due shortly: the leader promotes them into new lanes before
            // any producer has touched those lanes.
            for _ in 0..6 {
                let mut built = Vec::new();
                for _ in 0..4 {
                    let delay = Duration::from_millis(self.rng.range(1_500, 4_000));
                    if let Ok(job) = build(
                        &self.shared,
                        &mut self.rng,
                        queue,
                        Mode::Scheduled,
                        None,
                        Some(delay),
                    ) {
                        built.push(job);
                    }
                }
                let params: Vec<InsertParams> =
                    built.iter().map(|job| job.params.clone()).collect();
                match insert_many(&self.app_pool, &params).await {
                    Ok(rows) => {
                        for job in &built {
                            self.shared.shardops.note_enqueued(job.seq, job.executions);
                        }
                        count_enqueued(&self.shared, rows.iter().map(|row| row.queue.clone()));
                        register_rows(&self.shared, Persona::ShardOps, &rows, |_| ANY, 0);
                    }
                    Err(error) => self.operator_error("seed scheduled jobs", error),
                }
                if self.shared.load_stop.sleep(Duration::from_secs(1)).await {
                    self.shared.shardops.hold_producers(false);
                    return false;
                }
            }
            for _ in 0..15 {
                let created: i64 = sqlx::query_scalar(
                    "SELECT count(*)::bigint FROM awa.queue_enqueue_heads WHERE queue = $1 AND enqueue_shard >= $2",
                )
                .bind(queue)
                .bind(from)
                .fetch_one(&self.admin_pool)
                .await
                .unwrap_or(0);
                if created > 0 {
                    break;
                }
                if self.shared.load_stop.sleep(Duration::from_secs(1)).await {
                    self.shared.shardops.hold_producers(false);
                    return false;
                }
            }
            let owners = self.lane_owners(queue, from).await;
            self.note(format!(
                "new lanes of {queue} created before producers resumed; sequence owners: {owners} (leader role {})",
                leader_role.as_deref().unwrap_or("not ours")
            ));
            self.shared.shardops.hold_producers(false);
            self.settle(queue);
            return true;
        }

        self.set_shards(queue, to).await;
        if self.shared.load_stop.sleep(Duration::from_secs(4)).await {
            return false;
        }
        // Restart some runtimes and producer stores now and the rest later, so cached
        // shard counts disagree across the fleet for a while.
        let runtimes = self.runtimes().await;
        let mut restart_now: Vec<(usize, usize)> = runtimes
            .iter()
            .copied()
            .filter(|_| self.rng.chance(50))
            .collect();
        if restart_now.is_empty() {
            restart_now.extend(runtimes.first().copied());
        }
        if restart_now.len() == runtimes.len() && runtimes.len() > 1 {
            restart_now.pop();
        }
        let renew_now: Vec<usize> = (0..self.stores.len())
            .filter(|_| self.rng.chance(50))
            .collect();
        self.note(format!(
            "{queue}: restarting {} of {} runtimes and {} of {} producer stores; the others keep the old shard count",
            restart_now.len(),
            runtimes.len(),
            renew_now.len(),
            self.stores.len()
        ));
        for (fleet, replica) in &restart_now {
            self.restart(*fleet, *replica).await;
        }
        for index in &renew_now {
            self.stores.renew(*index);
        }
        if self.shared.load_stop.sleep(Duration::from_secs(18)).await {
            return false;
        }
        for (fleet, replica) in runtimes
            .iter()
            .filter(|runtime| !restart_now.contains(runtime))
        {
            self.restart(*fleet, *replica).await;
        }
        for index in 0..self.stores.len() {
            if !renew_now.contains(&index) {
                self.stores.renew(index);
            }
        }
        self.settle(queue);
        let owners = self.lane_owners(queue, from.min(to)).await;
        self.note(format!(
            "{queue}: settled at {to} shards; lane sequence owners above shard {}: {owners}",
            from.min(to)
        ));
        true
    }

    async fn run(&mut self, stop: &Stop, period: Duration) {
        if stop.sleep(Duration::from_secs(20)).await {
            return;
        }
        let mut step = [0usize; QUEUES.len()];
        let mut turn = 0usize;
        loop {
            let which = turn % QUEUES.len();
            turn += 1;
            let (from, to) = STEPS[step[which] % STEPS.len()];
            step[which] += 1;
            let started = Instant::now();
            if !self.change(QUEUES[which], from, to).await {
                return;
            }
            let remaining = period
                .saturating_sub(started.elapsed())
                .max(Duration::from_secs(5));
            if stop.sleep(remaining).await {
                return;
            }
        }
    }
}

pub async fn run(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::ShardOps;
    let queue_infos: Vec<Arc<QueueInfo>> = QUEUES
        .iter()
        .map(|name| shared.queues.register(name, persona, false))
        .collect();
    let config = &shared.config;

    let owner_pool = roles::pool_as(config, roles::OWNER, "soak_shardops_owner", 2).await?;
    let migrator_pool =
        roles::pool_as(config, roles::MIGRATOR, "soak_shardops_migrator", 1).await?;
    let admin_pool = connect_pool(&config.database_url, "soak_shardops_admin", 2)
        .await
        .map_err(|error| error.to_string())?;

    // The schema owner provisions each queue's first shard before any application role runs.
    let owner_store = fresh_store()?;
    for name in QUEUES {
        owner_store
            .prepare_queue(&owner_pool, name, 1)
            .await
            .map_err(|error| format!("prepare_queue {name}: {error}"))?;
        shared.shardops.plans.lock().expect("plans").insert(
            name.to_string(),
            shard_oracle::QueuePlan {
                configured: 1,
                changed_at_ms: 0,
                settled_at_ms: Some(0),
            },
        );
    }
    shared.shardops.open_window();

    let served: Vec<(Arc<QueueInfo>, i64)> = queue_infos
        .iter()
        .map(|info| (info.clone(), i64::from(WORKERS)))
        .collect();
    let mut fleets = Vec::new();
    for (name, role) in [("shardops-a", roles::APP_A), ("shardops-b", roles::APP_B)] {
        fleets.push(
            Fleet::start_with_url(
                &shared,
                roles::url_for_role(config, role),
                name,
                persona,
                2,
                10,
                2,
                served.clone(),
                client_factory(shared.clone()),
            )
            .await?,
        );
    }

    let app_a = roles::pool_as(
        config,
        roles::APP_A,
        "soak_shardops_producer_a",
        shared.budget.reserve("shardops-producer-a", 6, 2),
    )
    .await?;
    let app_b = roles::pool_as(
        config,
        roles::APP_B,
        "soak_shardops_producer_b",
        shared.budget.reserve("shardops-producer-b", 6, 2),
    )
    .await?;
    let stores = Arc::new(Stores::new(2)?);

    let period = if config.load >= Duration::from_secs(600) {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(40)
    };
    let queue_names: Vec<String> = QUEUES.iter().map(|name| name.to_string()).collect();
    shared.note(
        "shards",
        format!(
            "queues {QUEUES:?} cycle through {STEPS:?} shards every {}s per change; roles: owner {}, migrator {}, applications {} and {}",
            period.as_secs(),
            roles::OWNER,
            roles::MIGRATOR,
            roles::APP_A,
            roles::APP_B
        ),
    );

    let watch_stop = Stop::new();
    let watcher = tokio::spawn(shard_oracle::watch(
        shared.clone(),
        admin_pool.clone(),
        queue_names.clone(),
        watch_stop.clone(),
    ));
    let producers = vec![
        tokio::spawn(sql_producer(
            shared.clone(),
            app_a.clone(),
            roles::APP_A,
            false,
        )),
        tokio::spawn(sql_producer(
            shared.clone(),
            app_b.clone(),
            roles::APP_B,
            true,
        )),
        tokio::spawn(store_producer(
            shared.clone(),
            app_b.clone(),
            roles::APP_B,
            stores.clone(),
            0,
        )),
        tokio::spawn(keyed_producer(
            shared.clone(),
            app_a.clone(),
            app_b.clone(),
            stores.clone(),
            1,
        )),
    ];
    let operator_stop = shared.load_stop.clone();
    let mut operator = Operator {
        shared: shared.clone(),
        fleets: fleets.clone(),
        stores: stores.clone(),
        owner_pool: owner_pool.clone(),
        migrator_pool,
        admin_pool: admin_pool.clone(),
        app_pool: app_a.clone(),
        rng: Rng::for_task(shared.config.seed, "shardops-operator"),
    };
    operator.run(&operator_stop, period).await;
    shared.shardops.hold_producers(false);
    for producer in producers {
        let _ = producer.await;
    }
    shared.note("shards", "load stopped; draining");

    shared.drain_stop.wait().await;
    shard_oracle::final_check(&shared, &admin_pool, &queue_names).await;
    watch_stop.trigger();
    let _ = watcher.await;
    for fleet in &fleets {
        fleet.shutdown_all(Duration::from_secs(20), &shared).await;
    }
    for pool in [app_a, app_b, owner_pool, admin_pool] {
        pool.close().await;
    }
    Ok(())
}
