//! Mia, an ML pipeline: jobs from seconds to about a minute (compressed), deadlines
//! that sometimes expire, heartbeats and progress, retries into the DLQ and latency
//! spikes. Stresses deadline and stale rescues racing heartbeats, completions and
//! the DLQ.

use super::super::config::Persona;
use super::super::db::{connect_pool, ClientFactory};
use super::super::fleet::Fleet;
use super::super::ledger::{expect, flag};
use super::super::rng::Rng;
use super::super::shared::Shared;
use super::{attach_events, backoff, queue, register_rows, tune};
use awa::model::{insert, insert_many};
use awa::{Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, Worker};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

pub const TRAIN_QUEUE: &str = "awa_soak_mia_train";
pub const INFER_QUEUE: &str = "awa_soak_mia_infer";
pub const DEADLINE_QUEUE: &str = "awa_soak_mia_deadline";
/// Deadline given to jobs in the deadline queue; they run longer than this.
const DEADLINE: chrono::Duration = chrono::Duration::seconds(6);

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct MiaJob {
    pub seq: u64,
    pub behavior: String,
    pub work_ms: u64,
}

struct MiaWorker {
    shared: Arc<Shared>,
}

/// Sleeps in slices, reporting progress and stopping early when the job is cancelled
/// or the runtime is shutting down.
async fn work(ctx: &JobContext, total_ms: u64) -> bool {
    let slice = 500;
    let mut elapsed = 0;
    while elapsed < total_ms {
        if ctx.is_cancelled() {
            return false;
        }
        let step = slice.min(total_ms - elapsed);
        tokio::time::sleep(Duration::from_millis(step)).await;
        elapsed += step;
        if total_ms >= 4_000 && elapsed % 2_000 < slice {
            ctx.set_progress(((elapsed * 100) / total_ms).min(99) as u8, "training");
            let _ = ctx.flush_progress().await;
        }
    }
    true
}

#[async_trait::async_trait]
impl Worker for MiaWorker {
    fn kind(&self) -> &'static str {
        "mia_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let _guard = self.shared.begin_execution(Persona::Mia, &ctx.job, false);
        let args: MiaJob = serde_json::from_value(ctx.job.args.clone())
            .map_err(|error| JobError::terminal(format!("bad args: {error}")))?;
        let attempt = ctx.job.attempt;
        let finished = match args.behavior.as_str() {
            "flaky" if attempt < 3 => {
                work(ctx, args.work_ms.min(1_500)).await;
                return Err(JobError::retryable_msg("transient model server error"));
            }
            "doomed" => {
                work(ctx, args.work_ms.min(1_500)).await;
                return Err(JobError::retryable_msg("model artefact missing"));
            }
            "cancel" => {
                work(ctx, args.work_ms.min(1_000)).await;
                return Ok(JobResult::Cancel("rejected by validation".into()));
            }
            // The first attempt overruns its deadline; later attempts often recover.
            "overrun" if attempt >= 2 && !args.seq.is_multiple_of(3) => work(ctx, 1_000).await,
            _ => work(ctx, args.work_ms).await,
        };
        if !finished {
            return Err(JobError::retryable_msg("interrupted"));
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
            .queue(TRAIN_QUEUE, queue(24))
            .queue(INFER_QUEUE, queue(16))
            .queue(DEADLINE_QUEUE, queue(12))
            .register_worker(MiaWorker {
                shared: shared.clone(),
            });
        attach_events(tune(builder, &shared), &shared, &["mia_job"])
            .build()
            .expect("build mia client")
    })
}

/// One enqueued job: queue, behaviour, how long it works, deadline, max attempts and
/// the outcomes it may end in.
struct Plan {
    queue: &'static str,
    behavior: &'static str,
    work_ms: u64,
    max_attempts: i16,
    expect: u8,
}

fn plan(rng: &mut Rng) -> Plan {
    let roll = rng.below(100);
    if roll < 45 {
        Plan {
            queue: INFER_QUEUE,
            behavior: "fast",
            work_ms: rng.range(100, 2_000),
            max_attempts: 5,
            expect: expect::COMPLETED,
        }
    } else if roll < 70 {
        Plan {
            queue: TRAIN_QUEUE,
            behavior: "train",
            work_ms: rng.range(2_000, 20_000),
            max_attempts: 5,
            expect: expect::COMPLETED,
        }
    } else if roll < 78 {
        // Latency spike: a long job that heartbeats and reports progress throughout.
        Plan {
            queue: TRAIN_QUEUE,
            behavior: "spike",
            work_ms: rng.range(30_000, 60_000),
            max_attempts: 5,
            expect: expect::COMPLETED,
        }
    } else if roll < 88 {
        Plan {
            queue: DEADLINE_QUEUE,
            behavior: "overrun",
            work_ms: rng.range(10_000, 25_000),
            max_attempts: 3,
            expect: expect::COMPLETED | expect::FAILED,
        }
    } else if roll < 94 {
        Plan {
            queue: INFER_QUEUE,
            behavior: "flaky",
            work_ms: 500,
            max_attempts: 5,
            expect: expect::COMPLETED | expect::FAILED,
        }
    } else if roll < 98 {
        Plan {
            queue: INFER_QUEUE,
            behavior: "doomed",
            work_ms: 500,
            max_attempts: 3,
            expect: expect::FAILED,
        }
    } else {
        Plan {
            queue: INFER_QUEUE,
            behavior: "cancel",
            work_ms: 500,
            max_attempts: 3,
            expect: expect::CANCELLED,
        }
    }
}

pub async fn run(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::Mia;
    let train = shared.queues.register(TRAIN_QUEUE, persona, false);
    let infer = shared.queues.register(INFER_QUEUE, persona, false);
    let deadline = shared.queues.register(DEADLINE_QUEUE, persona, false);
    let fleet = Fleet::start(
        &shared,
        "mia",
        persona,
        2,
        14,
        2,
        vec![(train, 24), (infer, 16), (deadline, 12)],
        client_factory(shared.clone()),
    )
    .await?;

    let producer_pool = connect_pool(
        &shared.config.database_url,
        "soak_mia_producer",
        shared.budget.reserve("mia-producer", 2, 1),
    )
    .await
    .map_err(|error| error.to_string())?;
    let rate_per_second = (6.0 * shared.config.scale).max(0.5);
    let tick = Duration::from_millis(250);
    let mut rng = Rng::for_task(shared.config.seed, "mia-producer");
    let mut seq = 0u64;
    let mut carry = 0.0;
    shared.note("mia", format!("producing about {rate_per_second:.1} jobs/s across {TRAIN_QUEUE}, {INFER_QUEUE} and {DEADLINE_QUEUE}"));

    while !shared.load_stop.sleep(tick).await {
        carry += rate_per_second * tick.as_secs_f64();
        let count = carry as usize;
        carry -= count as f64;
        if count == 0 {
            continue;
        }
        let mut params = Vec::with_capacity(count);
        let mut expects = Vec::with_capacity(count);
        for _ in 0..count {
            seq += 1;
            let plan = plan(&mut rng);
            let args = MiaJob {
                seq,
                behavior: plan.behavior.to_string(),
                work_ms: plan.work_ms,
            };
            params.push(
                insert::params_with(
                    &args,
                    InsertOpts {
                        queue: plan.queue.to_string(),
                        max_attempts: plan.max_attempts,
                        deadline_duration: (plan.queue == DEADLINE_QUEUE).then_some(DEADLINE),
                        ..Default::default()
                    },
                )
                .map_err(|error| error.to_string())?,
            );
            expects.push(plan.expect);
            shared
                .stats(persona)
                .add(&format!("planned_{}", plan.behavior), 1);
        }
        match insert_many(&producer_pool, &params).await {
            Ok(rows) => {
                shared.stats(persona).add("enqueued", rows.len() as i64);
                register_rows(&shared, persona, &rows, |index| expects[index], 0);
            }
            Err(error) => backoff(&shared, error, persona).await,
        }
    }

    shared.note("mia", format!("load stopped after {seq} jobs; draining"));
    shared.drain_stop.wait().await;
    fleet.shutdown_all(Duration::from_secs(30), &shared).await;
    producer_pool.close().await;
    Ok(())
}

/// Flags Mia's exhausted jobs on `queue`: the rows a DLQ operation filtered to that
/// queue acts on. Returns how many were flagged.
pub fn mark_exhausted_in_queue(shared: &Shared, queue: &str, flags: u32) -> usize {
    let Some(queue) = shared.queues.get(queue) else {
        return 0;
    };
    shared.ledger.mark_where(
        |record| {
            record.persona == Persona::Mia
                && record.queue == queue.index
                && record.has(flag::EXHAUSTED)
        },
        flags,
    )
}

#[cfg(test)]
mod tests {
    use super::{mark_exhausted_in_queue, DEADLINE_QUEUE, INFER_QUEUE, TRAIN_QUEUE};
    use crate::persona_soak::config::{Persona, SoakConfig};
    use crate::persona_soak::db::{Budget, ServerInfo};
    use crate::persona_soak::ledger::{expect, flag, JobRecord};
    use crate::persona_soak::shared::Shared;

    #[test]
    fn a_dlq_operation_flags_only_its_own_queue() {
        let config = SoakConfig::for_test(
            "postgres://postgres:test@localhost/awa_soak_dlq_marks".to_string(),
        );
        let info = ServerInfo::default();
        let budget = Budget::new(&info, 0);
        let shared = Shared::new(config, info, budget);
        for (id, queue) in [(1, TRAIN_QUEUE), (2, INFER_QUEUE), (3, DEADLINE_QUEUE)] {
            let index = shared.queues.register(queue, Persona::Mia, false).index;
            shared.ledger.register(
                id,
                JobRecord::new(
                    Persona::Mia,
                    index,
                    0,
                    expect::ANY_TERMINAL,
                    flag::EXHAUSTED,
                ),
            );
        }
        assert_eq!(
            mark_exhausted_in_queue(&shared, TRAIN_QUEUE, flag::DLQ_PURGED),
            1
        );
        assert!(shared.ledger.get(1).unwrap().has(flag::DLQ_PURGED));
        assert!(!shared.ledger.get(2).unwrap().has(flag::DLQ_PURGED));
        assert!(!shared.ledger.get(3).unwrap().has(flag::DLQ_PURGED));
    }
}
