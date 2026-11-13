//! Priya, firehose: one hot queue, large batch inserts (above the bind-parameter
//! limit), COPY inserts, tiny jobs and several clients. Stresses bind limits, ring
//! rotation and prune under load and claim contention.

use super::super::config::Persona;
use super::super::db::{connect_pool, ClientFactory};
use super::super::fleet::Fleet;
use super::super::ledger::{expect, flag};
use super::super::rng::Rng;
use super::super::shared::Shared;
use super::{backoff, queue, register_rows, tune};
use awa::model::{insert_many, insert_many_copy_from_pool};
use awa::{Client, InsertOpts, InsertParams, JobArgs, JobContext, JobError, JobResult, Worker};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const HOT_QUEUE: &str = "awa_soak_priya_hot";

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct PriyaJob {
    pub seq: u64,
    pub source: String,
}

struct PriyaWorker {
    shared: Arc<Shared>,
}

#[async_trait::async_trait]
impl Worker for PriyaWorker {
    fn kind(&self) -> &'static str {
        "priya_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let _guard = self.shared.begin_execution(Persona::Priya, &ctx.job, true);
        self.shared
            .ledger
            .mark(ctx.job.id, flag::RETURNED_COMPLETED);
        Ok(JobResult::Completed)
    }
}

fn client_factory(shared: Arc<Shared>) -> ClientFactory {
    Arc::new(move |pool| {
        // No lifecycle hooks: a hook per completion would add a read per job and
        // change the load this persona exists to apply.
        tune(Client::builder(pool).queue(HOT_QUEUE, queue(48)), &shared)
            .register_worker(PriyaWorker {
                shared: shared.clone(),
            })
            .build()
            .expect("build priya client")
    })
}

fn batch(seq: &mut u64, count: usize, source: &str) -> Vec<InsertParams> {
    (0..count)
        .map(|_| {
            *seq += 1;
            awa::insert::params_with(
                &PriyaJob {
                    seq: *seq,
                    source: source.to_string(),
                },
                InsertOpts {
                    queue: HOT_QUEUE.to_string(),
                    max_attempts: 5,
                    ..Default::default()
                },
            )
            .expect("priya params")
        })
        .collect()
}

pub async fn run(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::Priya;
    let hot = shared.queues.register(HOT_QUEUE, persona, false);
    let fleet = Fleet::start(
        &shared,
        "priya",
        persona,
        3,
        12,
        3,
        vec![(hot, 48)],
        client_factory(shared.clone()),
    )
    .await?;

    let pool_size = shared.budget.reserve("priya-producer", 6, 2);
    let producer_pool = connect_pool(
        &shared.config.database_url,
        "soak_priya_producer",
        pool_size,
    )
    .await
    .map_err(|error| error.to_string())?;
    let mut rng = Rng::for_task(shared.config.seed, "priya-producer");
    let burst_size = shared.config.scaled(100_000) as usize;
    let copy_size = shared.config.scaled(10_000) as usize;
    let steady_per_tick = shared.config.scaled(30) as usize;
    let burst_every = Duration::from_secs(180);
    let copy_every = Duration::from_secs(45);
    let mut next_burst = Instant::now() + Duration::from_secs(10);
    let mut next_copy = Instant::now() + Duration::from_secs(25);
    let mut seq = 0u64;
    shared.note(
        "priya",
        format!("steady {} jobs/s, {burst_size}-row insert_many burst every {}s, {copy_size}-row COPY every {}s", steady_per_tick * 10, burst_every.as_secs(), copy_every.as_secs()),
    );

    while !shared.load_stop.sleep(Duration::from_millis(100)).await {
        let now = Instant::now();
        if now >= next_burst {
            next_burst = now + burst_every;
            // Vary the size around the target so chunk boundaries move between bursts.
            let size = burst_size + rng.below(5_000) as usize;
            let params = batch(&mut seq, size, "burst");
            let started = Instant::now();
            match insert_many(&producer_pool, &params).await {
                Ok(rows) => {
                    shared.stats(persona).add("enqueued", rows.len() as i64);
                    shared.stats(persona).add("bursts", 1);
                    shared
                        .stats(persona)
                        .observe("burst_insert_ms", started.elapsed().as_millis() as i64);
                    register_rows(
                        &shared,
                        persona,
                        &rows,
                        |_| expect::COMPLETED,
                        flag::NO_EVENTS | flag::SRC_BURST,
                    );
                    shared.note(
                        "priya",
                        format!(
                            "burst of {} rows inserted in {:?}",
                            rows.len(),
                            started.elapsed()
                        ),
                    );
                }
                Err(error) if shared.chaos_active() => {
                    backoff(&shared, format!("burst of {size}: {error}"), persona).await
                }
                Err(error) => {
                    shared.violation(
                        "insert_many_failed",
                        format!("insert_many of {size} rows failed: {error}"),
                        vec![],
                    );
                }
            }
        }
        if now >= next_copy {
            next_copy = now + copy_every;
            let params = batch(&mut seq, copy_size, "copy");
            let started = Instant::now();
            match insert_many_copy_from_pool(&producer_pool, &params).await {
                Ok(rows) => {
                    shared.stats(persona).add("enqueued", rows.len() as i64);
                    shared.stats(persona).add("copy_batches", 1);
                    shared
                        .stats(persona)
                        .observe("copy_insert_ms", started.elapsed().as_millis() as i64);
                    register_rows(
                        &shared,
                        persona,
                        &rows,
                        |_| expect::COMPLETED,
                        flag::NO_EVENTS | flag::SRC_COPY,
                    );
                }
                Err(error) => {
                    backoff(&shared, format!("copy of {copy_size}: {error}"), persona).await
                }
            }
        }
        let params = batch(&mut seq, steady_per_tick, "steady");
        match insert_many(&producer_pool, &params).await {
            Ok(rows) => {
                shared.stats(persona).add("enqueued", rows.len() as i64);
                register_rows(
                    &shared,
                    persona,
                    &rows,
                    |_| expect::COMPLETED,
                    flag::NO_EVENTS | flag::SRC_STEADY,
                );
            }
            Err(error) => backoff(&shared, error, persona).await,
        }
    }

    shared.note("priya", format!("load stopped after {seq} jobs; draining"));
    shared.drain_stop.wait().await;
    fleet.shutdown_all(Duration::from_secs(20), &shared).await;
    producer_pool.close().await;
    Ok(())
}
