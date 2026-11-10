//! Sam, ordered streams: a queue with enqueue shards and ordering keys, a striped
//! queue (`queue_stripe_count`), scheduled jobs and a rate-limited queue. Stresses
//! routing, promotion and fairness.

use super::super::config::Persona;
use super::super::db::{connect_pool, ClientFactory};
use super::super::fleet::Fleet;
use super::super::ledger::{expect, flag};
use super::super::rng::Rng;
use super::super::shared::Shared;
use super::{attach_events, backoff, queue, register_rows, storage, tune};
use awa::model::{insert_many, QueueStorage, QueueStorageConfig};
use awa::{
    Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, QueueConfig, RateLimit, Worker,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const ORDERED_QUEUE: &str = "awa_soak_sam_ord";
pub const RATE_QUEUE: &str = "awa_soak_sam_rate";
pub const SCHEDULED_QUEUE: &str = "awa_soak_sam_sched";
pub const STRIPED_QUEUE: &str = "awa_soak_sam_stripe";
const KEYS: u64 = 64;
const ENQUEUE_SHARDS: i16 = 4;
const STRIPES: usize = 4;
const RATE_PER_SECOND: f64 = 15.0;
/// Starts this far behind the newest start for the same key count as order inversions.
const INVERSION_WINDOW: u64 = 64;

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct SamJob {
    pub stream: String,
    pub key: u64,
    pub seq: u64,
}

#[derive(Default)]
struct Order {
    newest_start: HashMap<u64, u64>,
    inversions: u64,
    worst_distance: u64,
    striped_seen: HashSet<u64>,
    striped_repeats: u64,
}

struct SamWorker {
    shared: Arc<Shared>,
    order: Arc<Mutex<Order>>,
}

#[async_trait::async_trait]
impl Worker for SamWorker {
    fn kind(&self) -> &'static str {
        "sam_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let _guard = self.shared.begin_execution(Persona::Sam, &ctx.job, false);
        let args: SamJob = serde_json::from_value(ctx.job.args.clone())
            .map_err(|error| JobError::terminal(format!("bad args: {error}")))?;
        {
            let mut order = self.order.lock().expect("order");
            if args.stream == "ordered" {
                let newest = order.newest_start.entry(args.key).or_insert(0);
                if args.seq + INVERSION_WINDOW < *newest {
                    let distance = *newest - args.seq;
                    order.inversions += 1;
                    order.worst_distance = order.worst_distance.max(distance);
                } else {
                    *newest = (*newest).max(args.seq);
                }
            } else if args.stream == "striped" && !order.striped_seen.insert(args.seq) {
                order.striped_repeats += 1;
            }
        }
        tokio::time::sleep(Duration::from_millis(5 + args.seq % 40)).await;
        if ctx.is_cancelled() {
            return Err(JobError::retryable_msg("interrupted"));
        }
        self.shared
            .ledger
            .mark(ctx.job.id, flag::RETURNED_COMPLETED);
        Ok(JobResult::Completed)
    }
}

fn plain_factory(shared: Arc<Shared>, order: Arc<Mutex<Order>>) -> ClientFactory {
    Arc::new(move |pool| {
        let builder = Client::builder(pool)
            .queue(ORDERED_QUEUE, queue(12))
            .queue(SCHEDULED_QUEUE, queue(8))
            .queue(
                RATE_QUEUE,
                QueueConfig {
                    max_workers: 8,
                    rate_limit: Some(RateLimit {
                        max_rate: RATE_PER_SECOND,
                        burst: RATE_PER_SECOND as u32,
                    }),
                    ..QueueConfig::default()
                },
            )
            .register_worker(SamWorker {
                shared: shared.clone(),
                order: order.clone(),
            });
        attach_events(tune(builder, &shared), &shared, &["sam_job"])
            .build()
            .expect("build sam client")
    })
}

fn striped_config() -> QueueStorageConfig {
    QueueStorageConfig {
        queue_stripe_count: STRIPES,
        ..QueueStorageConfig::default()
    }
}

fn striped_factory(shared: Arc<Shared>, order: Arc<Mutex<Order>>) -> ClientFactory {
    Arc::new(move |pool| {
        let builder = Client::builder(pool)
            .queue(STRIPED_QUEUE, queue(16))
            .register_worker(SamWorker {
                shared: shared.clone(),
                order: order.clone(),
            });
        let builder = storage(tune(builder, &shared), &shared, striped_config());
        attach_events(builder, &shared, &["sam_job"])
            .build()
            .expect("build sam stripe client")
    })
}

pub async fn run(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::Sam;
    let ordered = shared.queues.register(ORDERED_QUEUE, persona, false);
    let scheduled = shared.queues.register(SCHEDULED_QUEUE, persona, false);
    let rated = shared.queues.register(RATE_QUEUE, persona, true);
    let striped = shared.queues.register(STRIPED_QUEUE, persona, false);

    let setup_pool = connect_pool(&shared.config.database_url, "soak_sam_setup", 1)
        .await
        .map_err(|error| error.to_string())?;
    sqlx::query(
        "INSERT INTO awa.queue_meta (queue, enqueue_shards) VALUES ($1, $2)
         ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards",
    )
    .bind(ORDERED_QUEUE)
    .bind(ENQUEUE_SHARDS)
    .execute(&setup_pool)
    .await
    .map_err(|error| format!("set enqueue_shards: {error}"))?;
    setup_pool.close().await;

    let order = Arc::new(Mutex::new(Order::default()));
    let plain = Fleet::start(
        &shared,
        "sam",
        persona,
        2,
        14,
        2,
        vec![(ordered, 12), (scheduled, 8), (rated, 8)],
        plain_factory(shared.clone(), order.clone()),
    )
    .await?;
    let stripe_fleet = Fleet::start(
        &shared,
        "sam-stripe",
        persona,
        2,
        10,
        2,
        vec![(striped, 16)],
        striped_factory(shared.clone(), order.clone()),
    )
    .await?;

    let producer_pool = connect_pool(
        &shared.config.database_url,
        "soak_sam_producer",
        shared.budget.reserve("sam-producer", 4, 1),
    )
    .await
    .map_err(|error| error.to_string())?;
    let store = QueueStorage::new(striped_config()).map_err(|error| error.to_string())?;
    let mut rng = Rng::for_task(shared.config.seed, "sam-producer");
    let scale = shared.config.scale;
    let mut key_seq = vec![0u64; KEYS as usize];
    let mut stripe_seq = 0u64;
    let mut seq = 0u64;
    let mut rate_carry = 0.0_f64;
    let tick = Duration::from_millis(100);
    shared.note(
        "sam",
        format!("{ORDERED_QUEUE} ({ENQUEUE_SHARDS} shards, {KEYS} ordering keys), {STRIPED_QUEUE} ({STRIPES} stripes), {SCHEDULED_QUEUE}, {RATE_QUEUE} ({RATE_PER_SECOND}/s)"),
    );

    while !shared.load_stop.sleep(tick).await {
        // Ordered stream with ordering keys.
        let ordered_count = (4.0 * scale).ceil() as usize;
        let mut params = Vec::new();
        for _ in 0..ordered_count {
            let key = rng.below(KEYS);
            key_seq[key as usize] += 1;
            params.push(
                awa::insert::params_with(
                    &SamJob {
                        stream: "ordered".into(),
                        key,
                        seq: key_seq[key as usize],
                    },
                    InsertOpts {
                        queue: ORDERED_QUEUE.into(),
                        ordering_key: Some(key.to_be_bytes().to_vec()),
                        ..Default::default()
                    },
                )
                .map_err(|error| error.to_string())?,
            );
        }
        match insert_many(&producer_pool, &params).await {
            Ok(rows) => {
                shared
                    .stats(persona)
                    .add("enqueued_ordered", rows.len() as i64);
                register_rows(&shared, persona, &rows, |_| expect::COMPLETED, 0);
            }
            Err(error) => backoff(&shared, error, persona).await,
        }

        // Scheduled jobs a few seconds out, and the rate-limited stream.
        let mut params = Vec::new();
        for _ in 0..(1.0 * scale).ceil() as usize {
            seq += 1;
            params.push(
                awa::insert::params_with(
                    &SamJob {
                        stream: "scheduled".into(),
                        key: 0,
                        seq,
                    },
                    InsertOpts {
                        queue: SCHEDULED_QUEUE.into(),
                        run_at: Some(
                            Utc::now()
                                + chrono::Duration::milliseconds(rng.range(1_000, 12_000) as i64),
                        ),
                        ..Default::default()
                    },
                )
                .map_err(|error| error.to_string())?,
            );
        }
        // Stay below the queue's rate limit so a backlog does not build by design.
        rate_carry += RATE_PER_SECOND * 0.8 * tick.as_secs_f64();
        while rate_carry >= 1.0 {
            rate_carry -= 1.0;
            seq += 1;
            params.push(
                awa::insert::params_with(
                    &SamJob {
                        stream: "rated".into(),
                        key: 0,
                        seq,
                    },
                    InsertOpts {
                        queue: RATE_QUEUE.into(),
                        ..Default::default()
                    },
                )
                .map_err(|error| error.to_string())?,
            );
        }
        match insert_many(&producer_pool, &params).await {
            Ok(rows) => {
                shared
                    .stats(persona)
                    .add("enqueued_sched_rate", rows.len() as i64);
                register_rows(&shared, persona, &rows, |_| expect::COMPLETED, 0);
            }
            Err(error) => backoff(&shared, error, persona).await,
        }

        // Striped queue, enqueued through a store that shares the workers' routing.
        let mut params = Vec::new();
        for _ in 0..(3.0 * scale).ceil() as usize {
            stripe_seq += 1;
            params.push(
                awa::insert::params_with(
                    &SamJob {
                        stream: "striped".into(),
                        key: 0,
                        seq: stripe_seq,
                    },
                    InsertOpts {
                        queue: STRIPED_QUEUE.into(),
                        ..Default::default()
                    },
                )
                .map_err(|error| error.to_string())?,
            );
        }
        match store.enqueue_params_batch(&producer_pool, &params).await {
            Ok(count) => shared.stats(persona).add("enqueued_striped", count as i64),
            Err(error) => backoff(&shared, error, persona).await,
        }
    }

    shared.note(
        "sam",
        format!("load stopped: {stripe_seq} striped jobs; draining"),
    );
    shared.drain_stop.wait().await;
    plain.shutdown_all(Duration::from_secs(20), &shared).await;
    stripe_fleet
        .shutdown_all(Duration::from_secs(20), &shared)
        .await;
    producer_pool.close().await;

    let order = order.lock().expect("order");
    let executed_striped = order.striped_seen.len() as u64;
    let missing: Vec<u64> = (1..=stripe_seq)
        .filter(|seq| !order.striped_seen.contains(seq))
        .take(20)
        .collect();
    let stats = shared.stats(persona);
    stats.add("striped_executed_distinct", executed_striped as i64);
    stats.add("striped_repeat_executions", order.striped_repeats as i64);
    stats.add("order_inversions", order.inversions as i64);
    stats.set_max("order_worst_distance", order.worst_distance as i64);
    if executed_striped < stripe_seq {
        let message = format!(
            "{} of {stripe_seq} striped jobs never executed",
            stripe_seq - executed_striped
        );
        let rows = vec![format!(
            "{{\"queue\":\"{STRIPED_QUEUE}\",\"missing_seq_sample\":{missing:?}}}"
        )];
        if shared.quiesced.load(std::sync::atomic::Ordering::Relaxed) {
            shared.violation("lost_job", message, rows);
        } else {
            shared.finding(
                "striped_backlog_at_end",
                format!("{message}; the system had not quiesced"),
                rows,
            );
        }
    }
    Ok(())
}
