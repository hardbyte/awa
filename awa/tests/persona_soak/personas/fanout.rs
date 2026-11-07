//! Fanout, replay-style fan-out: each parent job batch-inserts a hundred or so unique
//! child jobs into the queue it runs on, from many concurrent handlers. Some parents
//! then replay a batch that reuses an already-claimed unique key, fail with a unique
//! violation and fall back to inserting that batch row by row. Stresses concurrent
//! batch enqueue into one lane, unique claims and the insert-from-handler path.

use super::super::config::Persona;
use super::super::db::{connect_pool, ClientFactory};
use super::super::fleet::Fleet;
use super::super::ledger::{expect, flag};
use super::super::rng::Rng;
use super::super::shared::Shared;
use super::{attach_events, backoff, queue, register_rows, tune};
use awa::model::{insert_many, insert_with};
use awa::{
    AwaError, Client, InsertOpts, InsertParams, JobArgs, JobContext, JobError, JobResult, JobRow,
    UniqueOpts, Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const QUEUE: &str = "awa_soak_fanout";
const MIN_CHILDREN: u64 = 100;
const MAX_CHILDREN: u64 = 185;
const REPLAY_PERCENT: u64 = 10;

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct FanoutParent {
    pub seq: u64,
    pub children: u64,
    pub replay: bool,
}

/// Distinct `(parent_seq, index)` per child, so the unique key (queue + args) is
/// shared only by a deliberate replay.
#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct FanoutChild {
    pub parent_seq: u64,
    pub index: u64,
}

fn child_opts() -> InsertOpts {
    InsertOpts {
        queue: QUEUE.to_string(),
        unique: Some(UniqueOpts {
            by_queue: true,
            by_args: true,
            ..UniqueOpts::default()
        }),
        ..Default::default()
    }
}

fn is_unique_violation(error: &AwaError) -> bool {
    match error {
        AwaError::UniqueConflict { .. } => true,
        AwaError::Database(sqlx::Error::Database(database)) => {
            database.code().as_deref() == Some("23505")
        }
        _ => false,
    }
}

struct ParentWorker {
    shared: Arc<Shared>,
}

impl ParentWorker {
    /// Records rows the handler enqueued. A child can run before this is reached; the
    /// ledger merges the two.
    fn record(&self, rows: &[JobRow]) {
        let persona = Persona::Fanout;
        if let Some(queue) = self.shared.queues.get(QUEUE) {
            queue
                .enqueued
                .fetch_add(rows.len() as i64, Ordering::Relaxed);
        }
        self.shared
            .stats(persona)
            .add("enqueued_children", rows.len() as i64);
        register_rows(
            &self.shared,
            persona,
            rows,
            |_| expect::COMPLETED,
            flag::NO_EVENTS,
        );
    }

    async fn insert_batch(
        &self,
        pool: &PgPool,
        parent_seq: u64,
        indexes: impl Iterator<Item = u64> + Clone,
    ) -> Result<(), JobError> {
        let stats = self.shared.stats(Persona::Fanout);
        let params: Vec<InsertParams> = indexes
            .clone()
            .map(|index| {
                awa::insert::params_with(&FanoutChild { parent_seq, index }, child_opts())
                    .map_err(JobError::retryable)
            })
            .collect::<Result<_, _>>()?;
        let started = Instant::now();
        match insert_many(pool, &params).await {
            Ok(rows) => {
                stats.observe("batch_insert_ms", started.elapsed().as_millis() as i64);
                self.record(&rows);
                Ok(())
            }
            Err(error) if is_unique_violation(&error) => {
                stats.add("batch_unique_conflicts", 1);
                for index in indexes {
                    match insert_with(pool, &FanoutChild { parent_seq, index }, child_opts()).await
                    {
                        Ok(row) => {
                            stats.add("fallback_rows_inserted", 1);
                            self.record(&[row]);
                        }
                        Err(error) if is_unique_violation(&error) => {
                            stats.add("fallback_rows_skipped", 1)
                        }
                        Err(error) => return Err(JobError::retryable(error)),
                    }
                }
                Ok(())
            }
            Err(error) => {
                stats.add("batch_insert_errors", 1);
                Err(JobError::retryable(error))
            }
        }
    }
}

#[async_trait::async_trait]
impl Worker for ParentWorker {
    fn kind(&self) -> &'static str {
        "fanout_parent"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let _guard = self
            .shared
            .begin_execution(Persona::Fanout, &ctx.job, false);
        let args: FanoutParent = serde_json::from_value(ctx.job.args.clone())
            .map_err(|error| JobError::terminal(format!("bad args: {error}")))?;
        self.insert_batch(ctx.pool(), args.seq, 0..args.children)
            .await?;
        if args.replay {
            // Index 0 is already claimed, so the batch fails as a whole.
            let fresh = args.children..args.children * 2 - 1;
            self.insert_batch(ctx.pool(), args.seq, std::iter::once(0).chain(fresh))
                .await?;
        }
        self.shared
            .record_completed_return(Persona::Fanout, &ctx.job)
            .await;
        Ok(JobResult::Completed)
    }
}

struct ChildWorker {
    shared: Arc<Shared>,
}

#[async_trait::async_trait]
impl Worker for ChildWorker {
    fn kind(&self) -> &'static str {
        "fanout_child"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        // No lifecycle hooks: a hook per child would add a read per job.
        let _guard = self.shared.begin_execution(Persona::Fanout, &ctx.job, true);
        self.shared
            .record_completed_return(Persona::Fanout, &ctx.job)
            .await;
        Ok(JobResult::Completed)
    }
}

fn client_factory(shared: Arc<Shared>) -> ClientFactory {
    Arc::new(move |pool| {
        let builder = Client::builder(pool)
            .queue(QUEUE, queue(16))
            .register_worker(ParentWorker {
                shared: shared.clone(),
            })
            .register_worker(ChildWorker {
                shared: shared.clone(),
            });
        attach_events(tune(builder, &shared), &shared, &["fanout_parent"])
            .build()
            .expect("build fanout client")
    })
}

pub async fn run(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::Fanout;
    let queue_info = shared.queues.register(QUEUE, persona, false);
    let fleet = Fleet::start(
        &shared,
        "fanout",
        persona,
        2,
        12,
        2,
        vec![(queue_info.clone(), 16)],
        client_factory(shared.clone()),
    )
    .await?;

    let producer_pool = connect_pool(
        &shared.config.database_url,
        "soak_fanout_producer",
        shared.budget.reserve("fanout-producer", 2, 1),
    )
    .await
    .map_err(|error| error.to_string())?;
    let mut rng = Rng::for_task(shared.config.seed, "fanout-producer");
    let tick = Duration::from_millis(500);
    let parents_per_tick = shared.config.scaled(2) as usize;
    let mut seq = 0u64;
    shared.note(
        "fanout",
        format!(
            "{parents_per_tick} parents per {} ms tick, {MIN_CHILDREN}-{MAX_CHILDREN} unique children each, {REPLAY_PERCENT}% replay a used key",
            tick.as_millis()
        ),
    );

    while !shared.load_stop.sleep(tick).await {
        let mut params = Vec::with_capacity(parents_per_tick);
        for _ in 0..parents_per_tick {
            seq += 1;
            params.push(
                awa::insert::params_with(
                    &FanoutParent {
                        seq,
                        children: rng.range(MIN_CHILDREN, MAX_CHILDREN),
                        replay: rng.chance(REPLAY_PERCENT),
                    },
                    InsertOpts {
                        queue: QUEUE.to_string(),
                        max_attempts: 5,
                        ..Default::default()
                    },
                )
                .map_err(|error| error.to_string())?,
            );
        }
        match insert_many(&producer_pool, &params).await {
            Ok(rows) => {
                queue_info
                    .enqueued
                    .fetch_add(rows.len() as i64, Ordering::Relaxed);
                shared
                    .stats(persona)
                    .add("enqueued_parents", rows.len() as i64);
                register_rows(&shared, persona, &rows, |_| expect::COMPLETED, 0);
            }
            Err(error) => backoff(&shared, error, persona).await,
        }
    }

    shared.note(
        "fanout",
        format!("load stopped after {seq} parents; draining"),
    );
    shared.drain_stop.wait().await;
    fleet.shutdown_all(Duration::from_secs(20), &shared).await;
    producer_pool.close().await;
    Ok(())
}
