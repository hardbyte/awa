//! Txn, transactional producers: application code that enqueues jobs inside its own
//! REPEATABLE READ or SERIALIZABLE transaction. Each transaction first reads, which fixes
//! its snapshot, works for a moment, enqueues a small batch through `insert_with`,
//! `insert_many` or `insert_many_copy` on the transaction, works a little more while it
//! holds the lane lock, and commits. Many producers share three queues, one of them
//! sharded, so an enqueue often waits on its lane's lock while the queue ring rotates.
//! Serialization failures (40001) are retried in a fresh transaction.
//!
//! Stresses the ready-ring generation an enqueue writes when its snapshot predates a
//! ring rotation and a lane-lock wait: a job placed in an older generation than a job
//! ahead of it in the same lane can be passed by claims and pruned unrun. Each producer
//! checks the placement of its own rows right after every commit, and a sampler counts
//! misplaced ready rows across the queues once a second; the oracle reports any committed
//! job that never ran.

use super::super::config::Persona;
use super::super::db::{connect_pool, ClientFactory};
use super::super::fleet::Fleet;
use super::super::ledger::{expect, flag};
use super::super::registry::QueueInfo;
use super::super::rng::Rng;
use super::super::shared::Shared;
use super::{attach_events, backoff, queue, register_rows, tune};
use awa::model::{insert_many, insert_many_copy, insert_with};
use awa::{
    AwaError, Client, InsertOpts, InsertParams, JobArgs, JobContext, JobError, JobResult, JobRow,
    Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const HOT_QUEUE: &str = "awa_soak_txn_hot";
pub const PLAIN_QUEUE: &str = "awa_soak_txn_plain";
pub const SHARDED_QUEUE: &str = "awa_soak_txn_sharded";
const ENQUEUE_SHARDS: i16 = 2;
const WORKERS_PER_QUEUE: u32 = 12;
const MAX_BATCH: u64 = 4;
/// Serialization failures one transaction may retry before it is given up.
const MAX_SERIALIZATION_RETRIES: u32 = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Isolation {
    RepeatableRead,
    Serializable,
}

impl Isolation {
    const ALL: [Isolation; 2] = [Isolation::RepeatableRead, Isolation::Serializable];

    fn sql(self) -> &'static str {
        match self {
            Isolation::RepeatableRead => "REPEATABLE READ",
            Isolation::Serializable => "SERIALIZABLE",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Isolation::RepeatableRead => "repeatable_read",
            Isolation::Serializable => "serializable",
        }
    }
}

/// The producer API that writes a transaction's jobs.
#[derive(Clone, Copy, Debug)]
enum Api {
    /// `insert_with`, once per job.
    Single,
    /// `insert_many`.
    Batch,
    /// `insert_many_copy`.
    CopyBatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct TxnJob {
    pub producer: u64,
    pub seq: u64,
    pub isolation: String,
}

struct TxnWorker {
    shared: Arc<Shared>,
}

#[async_trait::async_trait]
impl Worker for TxnWorker {
    fn kind(&self) -> &'static str {
        "txn_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let _guard = self.shared.begin_execution(Persona::Txn, &ctx.job, false);
        self.shared
            .record_completed_return(Persona::Txn, &ctx.job)
            .await;
        Ok(JobResult::Completed)
    }
}

fn client_factory(shared: Arc<Shared>) -> ClientFactory {
    Arc::new(move |pool| {
        let builder = Client::builder(pool)
            .queue(HOT_QUEUE, queue(WORKERS_PER_QUEUE))
            .queue(PLAIN_QUEUE, queue(WORKERS_PER_QUEUE))
            .queue(SHARDED_QUEUE, queue(WORKERS_PER_QUEUE))
            .register_worker(TxnWorker {
                shared: shared.clone(),
            });
        attach_events(tune(builder, &shared), &shared, &["txn_job"])
            .build()
            .expect("build txn client")
    })
}

fn is_serialization_failure(error: &AwaError) -> bool {
    match error {
        AwaError::Database(sqlx::Error::Database(database)) => {
            database.code().as_deref() == Some("40001")
        }
        _ => false,
    }
}

fn opts(queue: &str) -> InsertOpts {
    InsertOpts {
        queue: queue.to_string(),
        max_attempts: 5,
        ..Default::default()
    }
}

fn params(queue: &str, jobs: &[TxnJob]) -> Result<Vec<InsertParams>, AwaError> {
    jobs.iter()
        .map(|job| awa::insert::params_with(job, opts(queue)))
        .collect()
}

/// One application transaction: read, work, enqueue `jobs`, work while holding the
/// lane lock, commit. Returns the committed rows.
async fn transaction(
    pool: &PgPool,
    isolation: Isolation,
    api: Api,
    queue: &str,
    jobs: &[TxnJob],
    work_before: Duration,
    work_after: Duration,
) -> Result<Vec<JobRow>, AwaError> {
    let mut tx = pool.begin().await?;
    sqlx::query(&format!(
        "SET TRANSACTION ISOLATION LEVEL {}",
        isolation.sql()
    ))
    .execute(&mut *tx)
    .await?;
    let _configured: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.queue_meta WHERE queue = $1")
            .bind(queue)
            .fetch_one(&mut *tx)
            .await?;
    tokio::time::sleep(work_before).await;
    let rows = match api {
        Api::Single => {
            let mut rows = Vec::with_capacity(jobs.len());
            for job in jobs {
                rows.push(insert_with(&mut *tx, job, opts(queue)).await?);
            }
            rows
        }
        Api::Batch => insert_many(&mut *tx, &params(queue, jobs)?).await?,
        Api::CopyBatch => insert_many_copy(&mut tx, &params(queue, jobs)?).await?,
    };
    tokio::time::sleep(work_after).await;
    tx.commit().await?;
    Ok(rows)
}

/// Committed job ids and the isolation level of the transaction that wrote them.
type Committed = Arc<Mutex<HashMap<i64, Isolation>>>;

/// Lane-placement evidence gathered through short, bounded reads: the rows found in an
/// older ring generation than an earlier row of their lane.
struct Placement {
    pool: PgPool,
    schema: String,
    misplaced: Mutex<HashSet<i64>>,
}

impl Placement {
    fn record(&self, shared: &Shared, ids: Vec<i64>) {
        if ids.is_empty() {
            return;
        }
        let mut misplaced = self.misplaced.lock().expect("txn misplaced");
        let first = misplaced.is_empty();
        misplaced.extend(ids);
        drop(misplaced);
        if first {
            shared.note(
                "txn",
                "a ready row sits in an older ring generation than an earlier row of its lane",
            );
        }
    }

    /// Checks the rows a producer just committed: each one is misplaced when an earlier row
    /// of its lane sits in a newer ring generation. Rows already pruned cannot be checked
    /// and are counted apart. A read cut short by its timeout is a skipped sample.
    async fn check_batch(&self, shared: &Shared, ids: &[i64]) {
        let stats = shared.stats(Persona::Txn);
        shared.sample_attempted("txn_placement");
        let schema = &self.schema;
        let rows: Result<Vec<(i64, bool)>, _> = sqlx::query_as(&format!(
            "SELECT r.job_id,
                    EXISTS (SELECT 1 FROM {schema}.ready_entries p
                            WHERE p.queue = r.queue AND p.priority = r.priority
                              AND p.enqueue_shard = r.enqueue_shard
                              AND p.lane_seq < r.lane_seq
                              AND p.ready_generation > r.ready_generation)
             FROM {schema}.ready_entries r WHERE r.job_id = ANY($1)"
        ))
        .bind(ids)
        .fetch_all(&self.pool)
        .await;
        match rows {
            Ok(rows) => {
                let found: HashSet<i64> = rows.iter().map(|(id, _)| *id).collect();
                stats.add("placement_batches_checked", 1);
                stats.add("placement_rows_checked", found.len() as i64);
                stats.add(
                    "placement_rows_not_checked",
                    ids.len().saturating_sub(found.len()) as i64,
                );
                self.record(
                    shared,
                    rows.into_iter()
                        .filter_map(|(id, misplaced)| misplaced.then_some(id))
                        .collect(),
                );
            }
            Err(error) => {
                shared.sample_failed("txn_placement", "txn placement check failed", &error, false)
            }
        }
    }

    /// Ready rows of this persona's queues placed in an older ring generation than a row
    /// at an earlier position of the same lane. Returns whether the sample completed.
    async fn sample(&self, shared: &Shared, queues: &[String], is_final: bool) -> bool {
        if !is_final {
            shared.sample_attempted("txn_sampler");
        }
        let schema = &self.schema;
        let misplaced: Result<Vec<i64>, _> = sqlx::query_scalar(&format!(
            "SELECT job_id FROM (
                SELECT job_id, ready_generation,
                       max(ready_generation) OVER (
                           PARTITION BY queue, priority, enqueue_shard ORDER BY lane_seq
                           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS newest_before
                FROM {schema}.ready_entries WHERE queue = ANY($1)
             ) lanes WHERE ready_generation < newest_before"
        ))
        .bind(queues)
        .fetch_all(&self.pool)
        .await;
        match misplaced {
            Ok(ids) => {
                self.record(shared, ids);
                true
            }
            Err(error) => {
                shared.sample_failed("txn_sampler", "txn lane sampler failed", &error, is_final);
                false
            }
        }
    }
}

async fn produce(
    shared: Arc<Shared>,
    pool: PgPool,
    placement: Arc<Placement>,
    queues: Arc<Vec<Arc<QueueInfo>>>,
    committed: Committed,
    producer: u64,
) {
    let persona = Persona::Txn;
    let stats = shared.stats(persona);
    let mut rng = Rng::for_task(shared.config.seed, &format!("txn-producer-{producer}"));
    let hold_ms = shared.config.txn_hold.as_millis() as u64;
    let mut seq = 0u64;
    while !shared
        .load_stop
        .sleep(Duration::from_millis(rng.below(10)))
        .await
    {
        let isolation = if rng.chance(shared.config.txn_serializable_percent) {
            Isolation::Serializable
        } else {
            Isolation::RepeatableRead
        };
        let api = match rng.below(10) {
            0..=3 => Api::Batch,
            4..=6 => Api::Single,
            _ => Api::CopyBatch,
        };
        // Most transactions share the hot lane.
        let queue = match rng.below(10) {
            0..=5 => &queues[0],
            6..=7 => &queues[1],
            _ => &queues[2],
        };
        let jobs: Vec<TxnJob> = (0..rng.range(1, MAX_BATCH))
            .map(|_| {
                seq += 1;
                TxnJob {
                    producer,
                    seq,
                    isolation: isolation.label().to_string(),
                }
            })
            .collect();
        let work_before = Duration::from_millis(rng.below(hold_ms + 1));
        let work_after = Duration::from_millis(rng.below(hold_ms / 10 + 1));

        let mut retries = 0u32;
        loop {
            match transaction(
                &pool,
                isolation,
                api,
                &queue.name,
                &jobs,
                work_before,
                work_after,
            )
            .await
            {
                Ok(rows) => {
                    queue
                        .enqueued
                        .fetch_add(rows.len() as i64, Ordering::Relaxed);
                    stats.add(
                        &format!("enqueued_{}", isolation.label()),
                        rows.len() as i64,
                    );
                    stats.add(&format!("commits_{}", isolation.label()), 1);
                    {
                        let mut committed = committed.lock().expect("txn committed");
                        for row in &rows {
                            committed.insert(row.id, isolation);
                        }
                    }
                    register_rows(&shared, persona, &rows, |_| expect::COMPLETED, 0);
                    let ids: Vec<i64> = rows.iter().map(|row| row.id).collect();
                    placement.check_batch(&shared, &ids).await;
                    break;
                }
                Err(error) if is_serialization_failure(&error) => {
                    stats.add(&format!("serialization_retries_{}", isolation.label()), 1);
                    retries += 1;
                    if retries > MAX_SERIALIZATION_RETRIES {
                        stats.add("serialization_gave_up", 1);
                        break;
                    }
                }
                Err(error) => {
                    backoff(
                        &shared,
                        format!("{api:?} in {isolation:?}: {error}"),
                        persona,
                    )
                    .await;
                    break;
                }
            }
        }
    }
}

pub async fn run(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::Txn;
    let queues: Arc<Vec<Arc<QueueInfo>>> = Arc::new(
        [HOT_QUEUE, PLAIN_QUEUE, SHARDED_QUEUE]
            .into_iter()
            .map(|name| shared.queues.register(name, persona, false))
            .collect(),
    );
    let queue_names: Vec<String> = queues.iter().map(|queue| queue.name.clone()).collect();

    let diag_pool = super::super::db::connect_sampler_pool(
        &shared.config.database_url,
        "soak_txn_diagnostics",
        shared.budget.reserve("txn-diagnostics", 5, 2),
        shared.config.diag_statement_timeout,
        shared.config.diag_lock_timeout,
    )
    .await
    .map_err(|error| error.to_string())?;
    sqlx::query(
        "INSERT INTO awa.queue_meta (queue, enqueue_shards) VALUES ($1, $2)
         ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards",
    )
    .bind(SHARDED_QUEUE)
    .bind(ENQUEUE_SHARDS)
    .execute(&diag_pool)
    .await
    .map_err(|error| format!("set enqueue_shards: {error}"))?;

    let fleet = Fleet::start(
        &shared,
        "txn",
        persona,
        2,
        6,
        2,
        queues
            .iter()
            .map(|queue| (queue.clone(), i64::from(WORKERS_PER_QUEUE)))
            .collect(),
        client_factory(shared.clone()),
    )
    .await?;

    let producers = shared.config.txn_producers.max(1);
    let producer_pool = match connect_pool(
        &shared.config.database_url,
        "soak_txn_producer",
        shared.budget.reserve("txn-producer", producers as u32, 2),
    )
    .await
    {
        Ok(pool) => pool,
        Err(error) => {
            fleet.shutdown_all(Duration::from_secs(20), &shared).await;
            return Err(error.to_string());
        }
    };
    shared.note(
        "txn",
        format!(
            "{producers} producers, {}% SERIALIZABLE and the rest REPEATABLE READ, up to {} ms between the first read and the enqueue, {MAX_BATCH}-job batches at most",
            shared.config.txn_serializable_percent,
            shared.config.txn_hold.as_millis()
        ),
    );

    let schema = shared
        .info
        .queue_storage_schema
        .clone()
        .unwrap_or_else(|| "awa".to_string());
    let placement = Arc::new(Placement {
        pool: diag_pool.clone(),
        schema,
        misplaced: Mutex::new(HashSet::new()),
    });
    let committed: Committed = Arc::new(Mutex::new(HashMap::new()));
    let tasks: Vec<_> = (0..producers)
        .map(|producer| {
            tokio::spawn(produce(
                shared.clone(),
                producer_pool.clone(),
                placement.clone(),
                queues.clone(),
                committed.clone(),
                producer,
            ))
        })
        .collect();

    let mut completed_samples = 0u64;
    while !shared.load_stop.sleep(Duration::from_secs(1)).await {
        completed_samples += u64::from(placement.sample(&shared, &queue_names, false).await);
    }
    let mut producer_failure = None;
    for task in tasks {
        if let Err(error) = task.await {
            producer_failure.get_or_insert(format!("txn producer: {error}"));
        }
    }
    completed_samples += u64::from(placement.sample(&shared, &queue_names, true).await);
    if completed_samples == 0 {
        shared.incomplete("the txn lane sampler never completed a sample", vec![]);
    }
    diag_pool.close().await;
    producer_pool.close().await;
    if let Some(error) = producer_failure {
        fleet.shutdown_all(Duration::from_secs(20), &shared).await;
        return Err(error);
    }
    let misplaced = std::mem::take(&mut *placement.misplaced.lock().expect("txn misplaced"));
    if !misplaced.is_empty() {
        let committed = committed.lock().expect("txn committed");
        let sample: BTreeSet<i64> = misplaced.iter().copied().collect();
        shared.violation(
            "lane_generation_order",
            format!(
                "{} ready row(s) were placed in an older ring generation than an earlier row of their lane",
                misplaced.len()
            ),
            sample
                .into_iter()
                .take(20)
                .map(|id| {
                    format!(
                        "{{\"job_id\":{id},\"isolation\":\"{}\"}}",
                        committed.get(&id).map_or("?", |isolation| isolation.label())
                    )
                })
                .collect(),
        );
    }

    shared.note("txn", "load stopped; draining");
    shared.drain_stop.wait().await;

    let committed = std::mem::take(&mut *committed.lock().expect("txn committed"));
    let stats = shared.stats(persona);
    let mut never_started: HashMap<Isolation, i64> = HashMap::new();
    for (id, isolation) in &committed {
        let unrun = shared.ledger.get(*id).is_some_and(|record| {
            record.executions == 0
                && !record.has_durable_terminal()
                && !record.has(flag::OPERATOR_REMOVED)
        });
        if unrun {
            *never_started.entry(*isolation).or_default() += 1;
        }
    }
    for isolation in Isolation::ALL {
        let label = isolation.label();
        let misplaced_here = misplaced
            .iter()
            .filter(|id| committed.get(id) == Some(&isolation))
            .count();
        stats.add(&format!("misplaced_{label}"), misplaced_here as i64);
        stats.add(
            &format!("never_started_{label}"),
            never_started.get(&isolation).copied().unwrap_or(0),
        );
    }
    stats.add("misplaced", misplaced.len() as i64);
    stats.add("never_started", never_started.values().sum());
    shared.note(
        "txn",
        format!(
            "{} committed jobs, {} placed in an older ring generation than their lane predecessor, {} never started",
            committed.len(),
            misplaced.len(),
            never_started.values().sum::<i64>()
        ),
    );

    fleet.shutdown_all(Duration::from_secs(20), &shared).await;
    Ok(())
}
