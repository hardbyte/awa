//! Tom, multi-tenant fan-out: many queues each at a low rate, unique jobs, cron and
//! priority aging. Stresses lane and queue cardinality, per-queue maintenance cost
//! and cron fencing across runtimes.

use super::super::config::Persona;
use super::super::db::{connect_pool, ClientFactory};
use super::super::fleet::Fleet;
use super::super::ledger::{expect, JobRecord};
use super::super::rng::Rng;
use super::super::shared::Shared;
use super::{attach_events, backoff, register_rows, tune};
use awa::model::{insert_many, insert_with};
use awa::{
    AwaError, Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, PeriodicJob,
    QueueConfig, UniqueOpts, Worker,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const PREFIX: &str = "awa_soak_tom_";
const CRON_JOBS: usize = 4;

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct TomJob {
    pub seq: u64,
    pub work_ms: u64,
    pub unique_key: Option<u64>,
}

struct TomWorker {
    shared: Arc<Shared>,
}

#[async_trait::async_trait]
impl Worker for TomWorker {
    fn kind(&self) -> &'static str {
        "tom_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let _guard = self.shared.begin_execution(Persona::Tom, &ctx.job, false);
        let work_ms = ctx.job.args["work_ms"].as_u64().unwrap_or(50);
        tokio::time::sleep(Duration::from_millis(work_ms)).await;
        if ctx.is_cancelled() {
            return Err(JobError::retryable_msg("interrupted"));
        }
        self.shared
            .record_completed_return(Persona::Tom, &ctx.job)
            .await;
        Ok(JobResult::Completed)
    }
}

fn queue_name(index: usize) -> String {
    format!("{PREFIX}{index:03}")
}

/// Four schedules firing every ten seconds into the first four tenant queues.
pub fn declare_cron(mut builder: awa::ClientBuilder) -> awa::ClientBuilder {
    for cron in 0..CRON_JOBS {
        let job = PeriodicJob::builder(format!("soak_tom_cron_{cron}"), "*/10 * * * * *")
            .queue(queue_name(cron))
            .build(&TomJob {
                seq: 0,
                work_ms: 20,
                unique_key: None,
            })
            .expect("cron job");
        builder = builder.periodic(job);
    }
    builder
}

fn client_factory(shared: Arc<Shared>, queue_count: usize) -> ClientFactory {
    Arc::new(move |pool| {
        let mut builder = Client::builder(pool);
        for index in 0..queue_count {
            builder = builder.queue(
                queue_name(index),
                QueueConfig {
                    max_workers: 2,
                    priority_aging_interval: Duration::from_secs(5),
                    ..QueueConfig::default()
                },
            );
        }
        builder = builder.register_worker(TomWorker {
            shared: shared.clone(),
        });
        for cron in 0..CRON_JOBS {
            let job = PeriodicJob::builder(format!("soak_tom_cron_{cron}"), "*/10 * * * * *")
                .queue(queue_name(cron))
                .build(&TomJob {
                    seq: 0,
                    work_ms: 20,
                    unique_key: None,
                })
                .expect("cron job");
            builder = builder.periodic(job);
        }
        attach_events(tune(builder, &shared), &shared, &["tom_job"])
            .build()
            .expect("build tom client")
    })
}

pub async fn run(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::Tom;
    let queue_count = shared.config.scaled(100) as usize;
    let mut served = Vec::new();
    for index in 0..queue_count {
        served.push((
            shared.queues.register(&queue_name(index), persona, false),
            2,
        ));
    }
    let fleet = Fleet::start(
        &shared,
        "tom",
        persona,
        2,
        24,
        2,
        served,
        client_factory(shared.clone(), queue_count),
    )
    .await?;

    let producer_pool = connect_pool(
        &shared.config.database_url,
        "soak_tom_producer",
        shared.budget.reserve("tom-producer", 4, 1),
    )
    .await
    .map_err(|error| error.to_string())?;
    let tick = Duration::from_millis(100);
    let mut rng = Rng::for_task(shared.config.seed, "tom-producer");
    let plain_per_tick = (queue_count as f64 * 0.05).max(1.0) as usize;
    shared.note("tom", format!("{queue_count} queues, {plain_per_tick} jobs per 100 ms tick, {CRON_JOBS} cron schedules every 10 s"));
    let unique_double_accept = Arc::new(AtomicU64::new(0));
    let mut seq = 0u64;
    let mut unique_key = 0u64;
    let mut unique_tasks = tokio::task::JoinSet::new();

    while !shared.load_stop.sleep(tick).await {
        let mut params = Vec::with_capacity(plain_per_tick);
        for _ in 0..plain_per_tick {
            seq += 1;
            let scheduled = rng.chance(20);
            let run_at = scheduled.then(|| {
                Utc::now() + chrono::Duration::milliseconds(rng.range(1_000, 10_000) as i64)
            });
            let queue_index = rng.below(queue_count as u64) as usize;
            params.push(
                awa::insert::params_with(
                    &TomJob {
                        seq,
                        work_ms: rng.range(10, 200),
                        unique_key: None,
                    },
                    InsertOpts {
                        queue: queue_name(queue_index),
                        priority: rng.range(1, 4) as i16,
                        run_at,
                        ..Default::default()
                    },
                )
                .map_err(|error| error.to_string())?,
            );
            shared.stats(persona).add(
                if scheduled {
                    "planned_scheduled"
                } else {
                    "planned_immediate"
                },
                1,
            );
        }
        match insert_many(&producer_pool, &params).await {
            Ok(rows) => {
                shared.stats(persona).add("enqueued", rows.len() as i64);
                register_rows(&shared, persona, &rows, |_| expect::COMPLETED, 0);
            }
            Err(error) => backoff(&shared, error, persona).await,
        }

        // One logical unique job per second: two concurrent identical inserts.
        if seq.is_multiple_of(10) && unique_tasks.len() < 16 {
            unique_key += 1;
            let queue_index = rng.below(queue_count as u64) as usize;
            let pool = producer_pool.clone();
            let shared = shared.clone();
            let double = unique_double_accept.clone();
            let key = unique_key;
            let work_ms = rng.range(10, 100);
            unique_tasks.spawn(async move {
                let args = TomJob {
                    seq: key,
                    work_ms,
                    unique_key: Some(key),
                };
                let opts = || InsertOpts {
                    queue: queue_name(queue_index),
                    unique: Some(UniqueOpts {
                        by_queue: true,
                        by_args: true,
                        by_period: None,
                        states: 0b0001_1111,
                    }),
                    ..Default::default()
                };
                let (first, second) = tokio::join!(
                    insert_with(&pool, &args, opts()),
                    insert_with(&pool, &args, opts())
                );
                let mut accepted = 0;
                for outcome in [first, second] {
                    match outcome {
                        Ok(row) => {
                            accepted += 1;
                            let queue = shared
                                .queues
                                .get(&row.queue)
                                .map_or(u32::MAX, |queue| queue.index);
                            shared.ledger.register(
                                row.id,
                                JobRecord::new(
                                    Persona::Tom,
                                    queue,
                                    shared.now_ms() as u32,
                                    expect::COMPLETED,
                                    0,
                                ),
                            );
                        }
                        Err(AwaError::UniqueConflict { .. }) => {
                            shared.stats(Persona::Tom).add("unique_conflicts", 1)
                        }
                        Err(error) => shared.stats(Persona::Tom).add(
                            &format!("unique_error:{error}")
                                .chars()
                                .take(60)
                                .collect::<String>(),
                            1,
                        ),
                    }
                }
                shared.stats(Persona::Tom).add("unique_logical", 1);
                if accepted > 1 {
                    double.fetch_add(1, Ordering::Relaxed);
                    shared.violation(
                        "unique_double_accept",
                        format!(
                            "both concurrent identical unique inserts (key {key}) were accepted"
                        ),
                        vec![format!(
                            "{{\"unique_key\":{key},\"queue\":\"{}\"}}",
                            queue_name(queue_index)
                        )],
                    );
                }
            });
        }
        while unique_tasks.try_join_next().is_some() {}
    }
    while unique_tasks.join_next().await.is_some() {}

    shared.note(
        "tom",
        format!("load stopped after {seq} plain jobs; draining"),
    );
    shared.drain_stop.wait().await;
    fleet.shutdown_all(Duration::from_secs(20), &shared).await;
    producer_pool.close().await;
    Ok(())
}
