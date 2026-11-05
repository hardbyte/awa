//! Alex, the operator: under live load, pauses and resumes queues, drains, cancels,
//! moves and reprioritises jobs, retries and purges DLQ rows, runs the admin read
//! queries the UI issues, and rolls a client restart through another persona's fleet.

use super::super::config::Persona;
use super::super::db::{connect_pool, ClientFactory};
use super::super::fleet::Fleet;
use super::super::ledger::{expect, flag};
use super::super::registry::QueueInfo;
use super::super::rng::Rng;
use super::super::shared::Shared;
use super::{attach_events, backoff, queue, register_rows, tune};
use awa::model::admin::{self, ListJobsFilter};
use awa::model::batch_operations::{
    get_batch_operation, submit_batch_operation, BatchOperationFilter, BatchOperationSpec,
    BatchOperationState, SubmitBatchOperation,
};
use awa::model::dlq::{self, ListDlqFilter};
use awa::model::{insert_many, JobState, RetryFromDlqOpts};
use awa::{Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, Worker};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const QUEUE_A: &str = "awa_soak_alex_a";
pub const QUEUE_B: &str = "awa_soak_alex_b";
pub const QUEUE_DRAIN: &str = "awa_soak_alex_drain";

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
pub struct AlexJob {
    pub seq: u64,
    pub work_ms: u64,
}

struct AlexWorker {
    shared: Arc<Shared>,
}

#[async_trait::async_trait]
impl Worker for AlexWorker {
    fn kind(&self) -> &'static str {
        "alex_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let _guard = self.shared.begin_execution(Persona::Alex, &ctx.job, false);
        let work_ms = ctx.job.args["work_ms"].as_u64().unwrap_or(100);
        tokio::time::sleep(Duration::from_millis(work_ms)).await;
        if ctx.is_cancelled() {
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
            .queue(QUEUE_A, queue(1))
            .queue(QUEUE_B, queue(2))
            .queue(QUEUE_DRAIN, queue(1))
            .register_worker(AlexWorker {
                shared: shared.clone(),
            });
        attach_events(tune(builder, &shared), &shared, &["alex_job"])
            .build()
            .expect("build alex client")
    })
}

struct Context {
    shared: Arc<Shared>,
    pool: PgPool,
    diagnostics: PgPool,
    stalled_rows: std::sync::Mutex<Vec<String>>,
    rng: Rng,
}

/// pid, application, state, wait event, seconds running, blocking pids, query.
type BackendRow = (i32, String, String, String, i32, String, String);

/// An admin call this slow under load is a finding in its own right.
const SLOW_CALL: Duration = Duration::from_secs(30);
const STALLED_CALL: Duration = Duration::from_secs(120);

impl Context {
    fn stats(&self) -> &super::super::shared::Stats {
        self.shared.stats(Persona::Alex)
    }

    async fn timed<T, E: std::fmt::Display>(
        &self,
        name: &str,
        call: impl std::future::Future<Output = Result<T, E>>,
    ) -> Option<T> {
        let started = Instant::now();
        let mut call = std::pin::pin!(call);
        let result = tokio::select! {
            result = &mut call => result,
            _ = tokio::time::sleep(SLOW_CALL) => {
                self.diagnose_slow_call(name).await;
                call.await
            }
        };
        let ms = started.elapsed().as_millis() as i64;
        if started.elapsed() > STALLED_CALL {
            self.shared.violation(
                "admin_call_stalled",
                format!("admin call {name} took {ms} ms under load"),
                self.stalled_rows.lock().expect("stalled rows").clone(),
            );
        }
        self.stats().observe(name, ms);
        if ms > 5_000 {
            self.stats().add("slow_admin_calls_over_5s", 1);
            self.shared.note("alex", format!("{name} took {ms} ms"));
        }
        match result {
            Ok(value) => Some(value),
            Err(error) => {
                self.stats().add(&format!("{name}_errors"), 1);
                if self.stats().get(&format!("{name}_errors")) <= 3 {
                    self.shared.note("alex", format!("{name} failed: {error}"));
                }
                None
            }
        }
    }

    /// Records what the operator's backend is waiting on and who blocks it.
    async fn diagnose_slow_call(&self, name: &str) {
        let rows: Result<Vec<BackendRow>, _> = sqlx::query_as(
            "SELECT a.pid, a.application_name, COALESCE(a.state, ''),
                    COALESCE(a.wait_event_type, '') || ':' || COALESCE(a.wait_event, ''),
                    COALESCE(extract(epoch FROM now() - a.query_start)::int, 0),
                    pg_blocking_pids(a.pid)::text, left(a.query, 200)
             FROM pg_stat_activity a
             WHERE a.pid <> pg_backend_pid()
               AND (a.application_name = 'soak_alex_operator'
                    OR a.pid IN (SELECT unnest(pg_blocking_pids(op.pid)) FROM pg_stat_activity op
                                 WHERE op.application_name = 'soak_alex_operator'))",
        )
        .fetch_all(&self.diagnostics)
        .await;
        let lines: Vec<String> = match rows {
            Ok(rows) => rows
                .into_iter()
                .map(|(pid, app, state, wait, secs, blockers, query)| {
                    format!("{{\"pid\":{pid},\"application\":\"{app}\",\"state\":\"{state}\",\"wait\":\"{wait}\",\"running_s\":{secs},\"blocked_by\":\"{blockers}\",\"query\":{query:?}}}")
                })
                .collect(),
            Err(error) => vec![format!("diagnostic query failed: {error}")],
        };
        self.shared.note(
            "alex",
            format!(
                "{name} still running after {}s; backend state: {}",
                SLOW_CALL.as_secs(),
                lines.join(" | ")
            ),
        );
        *self.stalled_rows.lock().expect("stalled rows") = lines;
    }

    /// A random queue of a persona, other than Alex's own.
    fn queue_of(&mut self, persona: Persona) -> Option<Arc<QueueInfo>> {
        let queues = self.shared.queues.of(persona);
        self.rng.pick(&queues).cloned()
    }

    async fn admin_reads(&mut self) {
        let pool = self.pool.clone();
        self.timed("admin_queue_overviews", admin::queue_overviews(&pool))
            .await;
        self.timed("admin_state_counts", admin::state_counts(&pool))
            .await;
        self.timed("admin_job_kind_overviews", admin::job_kind_overviews(&pool))
            .await;
        self.timed("admin_runtime_overview", admin::runtime_overview(&pool))
            .await;
        self.timed(
            "admin_queue_runtime_summary",
            admin::queue_runtime_summary(&pool),
        )
        .await;
        self.timed("admin_dlq_depth_by_queue", dlq::dlq_depth_by_queue(&pool))
            .await;
        if let Some(queue) = self.queue_of(Persona::Tom) {
            let filter = ListJobsFilter {
                queue: Some(queue.name.clone()),
                limit: Some(50),
                ..Default::default()
            };
            self.timed("admin_list_jobs", admin::list_jobs(&pool, &filter))
                .await;
        }
    }

    async fn pause_resume(&mut self) {
        let target = match self.rng.below(3) {
            0 => self.shared.queues.get(QUEUE_B),
            1 => self.queue_of(Persona::Tom),
            _ => self
                .shared
                .queues
                .get(super::priya::HOT_QUEUE)
                .or_else(|| self.queue_of(Persona::Mia)),
        };
        let Some(queue) = target else { return };
        queue.paused.store(true, Ordering::Relaxed);
        let pool = self.pool.clone();
        if self
            .timed(
                "pause_queue",
                admin::pause_queue(&pool, &queue.name, Some("soak")),
            )
            .await
            .is_some()
        {
            self.stats().add("pauses", 1);
            self.shared
                .stop_sleep(Duration::from_secs(self.rng.range(3, 8)))
                .await;
            self.timed("resume_queue", admin::resume_queue(&pool, &queue.name))
                .await;
        }
        // Give the claimers a moment to pick the queue back up before stall checks resume.
        self.shared.stop_sleep(Duration::from_secs(3)).await;
        queue.paused.store(false, Ordering::Relaxed);
    }

    async fn drain(&mut self) {
        let target = if self.rng.chance(50) {
            self.shared.queues.get(QUEUE_DRAIN)
        } else {
            self.queue_of(Persona::Tom)
        };
        let Some(queue) = target else { return };
        // Until the drain returns, any job enqueued so far may legitimately be cancelled.
        queue.drained_at_ms.store(i64::MAX / 4, Ordering::Relaxed);
        let pool = self.pool.clone();
        if let Some(count) = self
            .timed("drain_queue", admin::drain_queue(&pool, &queue.name))
            .await
        {
            self.stats().add("drains", 1);
            self.stats().add("drained_jobs", count as i64);
        }
        queue
            .drained_at_ms
            .store(self.shared.now_ms(), Ordering::Relaxed);
    }

    /// Cancels live jobs of other personas, some of them already running.
    async fn cancel(&mut self) {
        let persona = *self
            .rng
            .pick(&[Persona::Mia, Persona::Tom, Persona::Sam, Persona::Priya])
            .expect("personas");
        let Some(queue) = self.queue_of(persona) else {
            return;
        };
        let states = [
            JobState::Available,
            JobState::Running,
            JobState::Scheduled,
            JobState::Retryable,
        ];
        let state = *self.rng.pick(&states).expect("states");
        let pool = self.pool.clone();
        let filter = ListJobsFilter {
            queue: Some(queue.name.clone()),
            state: Some(state),
            limit: Some(30),
            ..Default::default()
        };
        let Some(rows) = self
            .timed("admin_list_jobs", admin::list_jobs(&pool, &filter))
            .await
        else {
            return;
        };
        let ids: Vec<i64> = rows.iter().map(|row| row.id).take(8).collect();
        if ids.is_empty() {
            return;
        }
        if self.rng.chance(50) {
            let started_ms = self.shared.now_ms() as u32;
            let first = ids[0];
            let completed_before = self
                .shared
                .ledger
                .get(first)
                .is_some_and(|record| record.completed_events > 0);
            if let Some(Some(job)) = self.timed("cancel", admin::cancel(&pool, first)).await {
                self.after_cancel(job.id, completed_before, started_ms);
            }
        } else if let Some(cancelled) = self
            .timed("bulk_cancel", admin::bulk_cancel(&pool, &ids))
            .await
        {
            let now = self.shared.now_ms() as u32;
            for job in &cancelled {
                self.after_cancel(job.id, false, now);
            }
        }
    }

    fn after_cancel(&self, job_id: i64, completed_before: bool, call_started_ms: u32) {
        let now = self.shared.now_ms() as u32;
        self.stats().add("cancelled", 1);
        self.shared.ledger.with(
            job_id,
            || {
                super::super::ledger::JobRecord::new(
                    Persona::Alex,
                    u32::MAX,
                    now,
                    expect::ANY_TERMINAL,
                    0,
                )
            },
            |record| {
                record.flags |= flag::OPERATOR_REMOVED | flag::OPERATOR_CANCEL_OK;
                record.operator_cancel_ms = now;
            },
        );
        if completed_before {
            self.shared.violation(
                "cancel_overwrote_completed",
                format!("operator cancel of job {job_id} succeeded after it was reported completed (call began at {call_started_ms} ms)"),
                vec![format!("{{\"job_id\":{job_id}}}")],
            );
        }
    }

    async fn dlq_operations(&mut self) {
        let pool = self.pool.clone();
        let filter = ListDlqFilter {
            queue: Some(super::mia::INFER_QUEUE.to_string()),
            limit: Some(20),
            ..Default::default()
        };
        let Some(rows) = self.timed("dlq_list", dlq::list_dlq(&pool, &filter)).await else {
            return;
        };
        let ids: Vec<i64> = rows.iter().map(|row| row.job.id).collect();
        self.stats().set_max("dlq_listed_max", ids.len() as i64);
        match self.rng.below(4) {
            0 => {
                if let Some(&id) = ids.first() {
                    self.shared.ledger.mark(id, flag::DLQ_RETRIED);
                    let opts = RetryFromDlqOpts::default();
                    if self
                        .timed("dlq_retry", dlq::retry_from_dlq(&pool, id, &opts))
                        .await
                        .is_some()
                    {
                        self.stats().add("dlq_retried", 1);
                    }
                }
            }
            1 => {
                for id in ids.iter().take(3) {
                    self.shared.ledger.mark(*id, flag::DLQ_PURGED);
                    if self
                        .timed("dlq_purge_job", dlq::purge_dlq_job(&pool, *id))
                        .await
                        == Some(true)
                    {
                        self.stats().add("dlq_purged", 1);
                    }
                }
            }
            2 => {
                // Bulk retry: every exhausted Mia job may come back.
                self.shared.ledger.mark_where(
                    |record| record.persona == Persona::Mia && record.has(flag::EXHAUSTED),
                    flag::DLQ_RETRIED,
                );
                let bulk = ListDlqFilter {
                    queue: Some(super::mia::INFER_QUEUE.to_string()),
                    ..Default::default()
                };
                if let Some(count) = self
                    .timed(
                        "dlq_bulk_retry",
                        dlq::bulk_retry_from_dlq(&pool, &bulk, false),
                    )
                    .await
                {
                    self.stats().add("dlq_bulk_retried", count as i64);
                }
            }
            _ => {
                self.shared.ledger.mark_where(
                    |record| record.persona == Persona::Mia && record.has(flag::EXHAUSTED),
                    flag::DLQ_PURGED,
                );
                let bulk = ListDlqFilter {
                    queue: Some(super::mia::TRAIN_QUEUE.to_string()),
                    ..Default::default()
                };
                if let Some(count) = self
                    .timed("dlq_bulk_purge", dlq::purge_dlq(&pool, &bulk, false))
                    .await
                {
                    self.stats().add("dlq_bulk_purged", count as i64);
                }
            }
        }
    }

    async fn batch_operations(&mut self) {
        let pool = self.pool.clone();
        let (spec, filter, label) = if self.rng.chance(50) {
            (
                BatchOperationSpec::MoveQueue {
                    queue: QUEUE_B.to_string(),
                    priority: None,
                },
                BatchOperationFilter {
                    queue: Some(QUEUE_A.to_string()),
                    state: Some(JobState::Available),
                    ..Default::default()
                },
                "move_queue",
            )
        } else {
            (
                BatchOperationSpec::SetPriority { priority: 1 },
                BatchOperationFilter {
                    queue: Some(QUEUE_B.to_string()),
                    state: Some(JobState::Available),
                    ..Default::default()
                },
                "set_priority",
            )
        };
        let request = SubmitBatchOperation {
            spec,
            filter,
            submitted_by: Some("soak".into()),
            allow_all: false,
        };
        let Some(operation) = self
            .timed("batch_submit", submit_batch_operation(&pool, request))
            .await
        else {
            return;
        };
        let started = Instant::now();
        loop {
            let Some(current) = self
                .timed("batch_get", get_batch_operation(&pool, operation.id))
                .await
            else {
                return;
            };
            match current.state {
                BatchOperationState::Completed | BatchOperationState::Cancelled => {
                    self.stats().add(&format!("batch_{label}"), 1);
                    self.stats()
                        .add(&format!("batch_{label}_processed"), current.processed);
                    self.stats()
                        .observe("batch_operation_ms", started.elapsed().as_millis() as i64);
                    return;
                }
                BatchOperationState::Failed => {
                    self.shared.violation(
                        "batch_operation_failed",
                        format!(
                            "{label} batch operation {} failed: {:?}",
                            operation.id, current.last_error
                        ),
                        vec![],
                    );
                    return;
                }
                _ => {}
            }
            if started.elapsed() > Duration::from_secs(90) {
                self.stats().add("batch_operations_incomplete", 1);
                self.shared.note(
                    "alex",
                    format!(
                        "{label} batch operation {} still {:?} after 90s",
                        operation.id, current.state
                    ),
                );
                return;
            }
            if self.shared.stop_sleep(Duration::from_secs(1)).await {
                return;
            }
        }
    }

    async fn rolling_restart(&mut self) {
        let names = ["mia", "tom", "priya", "sam", "sam-stripe"];
        let name = *self.rng.pick(&names).expect("fleet names");
        let Some(fleet) = self.shared.fleet(name) else {
            return;
        };
        let live = fleet.live_replicas().await;
        let Some(&index) = self.rng.pick(&live) else {
            return;
        };
        self.shared
            .note("alex", format!("rolling restart: {name} replica {index}"));
        self.shared.chaos_window(Duration::from_secs(20));
        let started = Instant::now();
        match fleet.restart(index, Duration::from_secs(30)).await {
            Ok(_) => {
                self.stats().add("rolling_restarts", 1);
                self.stats()
                    .observe("rolling_restart_ms", started.elapsed().as_millis() as i64);
            }
            Err(error) => {
                self.shared.violation(
                    "restart_failed",
                    format!("restarting {name} replica {index}: {error}"),
                    vec![],
                );
            }
        }
    }
}

impl Shared {
    /// Sleeps unless the load phase ends first; true when it ended.
    pub async fn stop_sleep(&self, duration: Duration) -> bool {
        self.load_stop.sleep(duration).await
    }
}

pub async fn run(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::Alex;
    let queue_a = shared.queues.register(QUEUE_A, persona, false);
    let queue_b = shared.queues.register(QUEUE_B, persona, false);
    let queue_drain = shared.queues.register(QUEUE_DRAIN, persona, false);
    let fleet = Fleet::start(
        &shared,
        "alex",
        persona,
        2,
        10,
        2,
        vec![(queue_a, 1), (queue_b, 2), (queue_drain, 1)],
        client_factory(shared.clone()),
    )
    .await?;

    let pool = connect_pool(
        &shared.config.database_url,
        "soak_alex_operator",
        shared.budget.reserve("alex-operator", 4, 2),
    )
    .await
    .map_err(|error| error.to_string())?;
    let producer_pool = connect_pool(
        &shared.config.database_url,
        "soak_alex_producer",
        shared.budget.reserve("alex-producer", 2, 1),
    )
    .await
    .map_err(|error| error.to_string())?;

    // Alex's own traffic: a stream the move and reprioritise operations act on, and a
    // stream that gets drained while it is still being produced.
    let producer = {
        let shared = shared.clone();
        tokio::spawn(async move {
            let mut rng = Rng::for_task(shared.config.seed, "alex-producer");
            let mut seq = 0u64;
            while !shared.load_stop.sleep(Duration::from_millis(200)).await {
                let mut params = Vec::new();
                for queue in [
                    QUEUE_A,
                    QUEUE_A,
                    QUEUE_A,
                    QUEUE_A,
                    QUEUE_B,
                    QUEUE_DRAIN,
                    QUEUE_DRAIN,
                    QUEUE_DRAIN,
                ] {
                    seq += 1;
                    params.push(
                        awa::insert::params_with(
                            &AlexJob {
                                seq,
                                work_ms: rng.range(50, 300),
                            },
                            InsertOpts {
                                queue: queue.into(),
                                ..Default::default()
                            },
                        )
                        .expect("alex params"),
                    );
                }
                match insert_many(&producer_pool, &params).await {
                    Ok(rows) => {
                        shared
                            .stats(Persona::Alex)
                            .add("enqueued", rows.len() as i64);
                        register_rows(
                            &shared,
                            Persona::Alex,
                            &rows,
                            |_| expect::ANY_TERMINAL,
                            flag::OPERATOR_MOVED,
                        );
                    }
                    Err(error) => backoff(&shared, error, Persona::Alex).await,
                }
            }
            producer_pool.close().await;
        })
    };

    let diagnostics = connect_pool(
        &shared.config.database_url,
        "soak_alex_diagnostics",
        shared.budget.reserve("alex-diagnostics", 1, 1),
    )
    .await
    .map_err(|error| error.to_string())?;
    let mut context = Context {
        shared: shared.clone(),
        pool,
        diagnostics,
        stalled_rows: Default::default(),
        rng: Rng::for_task(shared.config.seed, "alex-operator"),
    };
    let load = shared.config.load.as_secs_f64();
    // Scale the cadence down for short runs so each action still happens a few times.
    let pace = (load / 900.0).clamp(0.1, 1.0);
    let mut schedule: Vec<(&str, f64, Instant)> = vec![
        ("reads", 2.0, Instant::now()),
        (
            "pause",
            60.0 * pace,
            Instant::now() + Duration::from_secs_f64(20.0 * pace),
        ),
        (
            "drain",
            75.0 * pace,
            Instant::now() + Duration::from_secs_f64(35.0 * pace),
        ),
        (
            "cancel",
            10.0 * pace,
            Instant::now() + Duration::from_secs_f64(15.0 * pace),
        ),
        (
            "dlq",
            45.0 * pace,
            Instant::now() + Duration::from_secs_f64(60.0 * pace),
        ),
        (
            "batch",
            40.0 * pace,
            Instant::now() + Duration::from_secs_f64(25.0 * pace),
        ),
        (
            "restart",
            150.0 * pace,
            Instant::now() + Duration::from_secs_f64(90.0 * pace),
        ),
    ];
    shared.note("alex", "operating: reads, pause/resume, drain, cancel, move/reprioritise, DLQ retry/purge, rolling restarts");
    while !shared.load_stop.is_stopped() {
        let now = Instant::now();
        for (name, interval, due) in schedule.iter_mut() {
            if *due > now || shared.load_stop.is_stopped() {
                continue;
            }
            *due = Instant::now() + Duration::from_secs_f64(*interval);
            match *name {
                "reads" => context.admin_reads().await,
                "pause" => context.pause_resume().await,
                "drain" => context.drain().await,
                "cancel" => context.cancel().await,
                "dlq" => context.dlq_operations().await,
                "batch" => context.batch_operations().await,
                "restart" => context.rolling_restart().await,
                _ => {}
            }
        }
        shared.load_stop.sleep(Duration::from_millis(250)).await;
    }

    let _ = producer.await;
    shared.drain_stop.wait().await;
    fleet.shutdown_all(Duration::from_secs(20), &shared).await;
    context.pool.close().await;
    context.diagnostics.close().await;
    Ok(())
}
