//! Pattern test: deadline-bounded polling with `RetryAfter` + handler-side deadline check.
//!
//! awa supports retries by count (`max_attempts`) and per-attempt
//! delay (exponential `awa.backoff_duration` or caller-controlled
//! `JobResult::RetryAfter`), but has no first-class "retry until
//! timestamp T" field. The recommended pattern is:
//!
//!   1. Embed the deadline in the job args so retries see the same cutoff.
//!   2. Have the handler check `Utc::now() >= deadline_at` at the top of
//!      every attempt and return `JobResult::Cancel(reason)` past the cutoff.
//!   3. Clamp the per-attempt `RetryAfter` delay so it does not overshoot
//!      the deadline — otherwise a poll at deadline−1s schedules the next
//!      attempt at deadline+(interval−1)s and the job sits `retryable` past
//!      the cutoff.
//!
//! This file pins that pattern end-to-end with two cases:
//!
//!   - `test_poll_completes_when_external_becomes_ready_before_deadline`:
//!     the external service flips to ready after a few polls. The job
//!     finishes in `Completed`.
//!   - `test_poll_cancels_when_deadline_expires_before_external_ready`:
//!     the external service never flips. The deadline branch in the
//!     handler fires and the job finishes in `Cancelled` with the
//!     deadline-exceeded reason recorded in `errors`.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{insert_with, migrations, InsertOpts};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, JobState, QueueConfig, Worker};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

// ── Mock external service (shared with `awa/examples/poll_until_deadline.rs`) ──

#[derive(Clone, Debug)]
enum ExternalStatus {
    Pending { polls_remaining: u32 },
    Ready,
    Failed,
}

type ExternalSvc = Arc<Mutex<HashMap<String, ExternalStatus>>>;

async fn probe(svc: &ExternalSvc, external_id: &str) -> ExternalStatus {
    let mut guard = svc.lock().await;
    let status = guard
        .get(external_id)
        .cloned()
        .unwrap_or(ExternalStatus::Failed);
    if let ExternalStatus::Pending { polls_remaining } = &status {
        let next = if *polls_remaining <= 1 {
            ExternalStatus::Ready
        } else {
            ExternalStatus::Pending {
                polls_remaining: polls_remaining - 1,
            }
        };
        guard.insert(external_id.to_string(), next);
    }
    status
}

// ── Job args + worker ───────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct PollExternalJob {
    external_id: String,
    deadline_at: DateTime<Utc>,
    poll_interval_ms: u64,
}

struct PollExternalWorker {
    svc: ExternalSvc,
}

#[async_trait::async_trait]
impl Worker for PollExternalWorker {
    fn kind(&self) -> &'static str {
        "poll_external_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: PollExternalJob = serde_json::from_value(ctx.job.args.clone())
            .map_err(|err| JobError::terminal(format!("invalid args: {err}")))?;
        let now = Utc::now();

        if now >= args.deadline_at {
            return Ok(JobResult::Cancel(format!(
                "deadline {} exceeded after {} attempts; external_id={}",
                args.deadline_at, ctx.job.attempt, args.external_id
            )));
        }

        match probe(&self.svc, &args.external_id).await {
            ExternalStatus::Ready => Ok(JobResult::Completed),
            ExternalStatus::Failed => Err(JobError::terminal(format!(
                "external_id={} rejected upstream",
                args.external_id
            ))),
            ExternalStatus::Pending { .. } => {
                let nominal_next = now + ChronoDuration::milliseconds(args.poll_interval_ms as i64);
                let next = nominal_next.min(args.deadline_at);
                let delay = (next - now).to_std().unwrap_or(Duration::from_millis(1));
                Ok(JobResult::RetryAfter(delay))
            }
        }
    }
}

// ── Test infrastructure ─────────────────────────────────────────────

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup_pool() -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");
    migrations::run(&pool)
        .await
        .expect("Failed to run migrations");
    pool
}

fn unique_queue(prefix: &str) -> String {
    format!("{prefix}_{}", &Uuid::new_v4().simple().to_string()[..8])
}

fn build_client(pool: sqlx::PgPool, queue: &str, svc: ExternalSvc) -> Client {
    Client::builder(pool)
        .queue(
            queue,
            QueueConfig {
                max_workers: 1,
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        // Promote `retryable` → `available` quickly so the next poll
        // attempt fires within the test's wall-clock window.
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .register_worker(PollExternalWorker { svc })
        .build()
        .expect("Failed to build client")
}

async fn wait_for_terminal(
    pool: &sqlx::PgPool,
    job_id: i64,
    timeout: Duration,
) -> (JobState, Vec<serde_json::Value>) {
    let start = Instant::now();
    loop {
        let row: Option<(String, Vec<serde_json::Value>)> = sqlx::query_as(
            "SELECT state::text, COALESCE(errors, ARRAY[]::jsonb[]) FROM awa.jobs WHERE id = $1",
        )
        .bind(job_id)
        .fetch_optional(pool)
        .await
        .expect("read job");
        if let Some((state, errors)) = row {
            let parsed: JobState = state.parse().expect("parse state");
            if matches!(
                parsed,
                JobState::Completed | JobState::Cancelled | JobState::Failed
            ) {
                return (parsed, errors);
            }
        }
        if start.elapsed() > timeout {
            panic!("Timed out waiting for job {job_id} terminal state");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_poll_completes_when_external_becomes_ready_before_deadline() {
    let pool = setup_pool().await;
    let queue = unique_queue("poll_ok");
    let external_id = "ext-ok";

    // External flips to Ready on the 3rd probe — well before the 1s deadline.
    let svc: ExternalSvc = Arc::new(Mutex::new(HashMap::from([(
        external_id.to_string(),
        ExternalStatus::Pending { polls_remaining: 3 },
    )])));

    let mut tx = pool.begin().await.expect("begin");
    let job = insert_with(
        &mut *tx,
        &PollExternalJob {
            external_id: external_id.into(),
            deadline_at: Utc::now() + ChronoDuration::seconds(1),
            poll_interval_ms: 50,
        },
        InsertOpts {
            queue: queue.clone(),
            // Plenty of headroom: ~20 nominal polls in 1s × 50ms.
            max_attempts: 100,
            ..Default::default()
        },
    )
    .await
    .expect("insert");
    tx.commit().await.expect("commit");

    let client = build_client(pool.clone(), &queue, svc.clone());
    client.start().await.expect("start");
    let (state, _errors) = wait_for_terminal(&pool, job.id, Duration::from_secs(5)).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(
        state,
        JobState::Completed,
        "job should complete once the external service is ready"
    );

    // After Completed there should be no Pending entries left for this id —
    // i.e. probe drained the counter and observed Ready.
    let final_status = svc.lock().await.get(external_id).cloned();
    assert!(
        matches!(final_status, Some(ExternalStatus::Ready)),
        "external service should be in Ready terminal state, got {final_status:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_poll_cancels_when_deadline_expires_before_external_ready() {
    let pool = setup_pool().await;
    let queue = unique_queue("poll_expire");
    let external_id = "ext-expire";

    // External never flips — `polls_remaining` is sized larger than the
    // deadline window allows for at the test's poll cadence.
    let svc: ExternalSvc = Arc::new(Mutex::new(HashMap::from([(
        external_id.to_string(),
        ExternalStatus::Pending {
            polls_remaining: 9_999,
        },
    )])));

    let deadline = Utc::now() + ChronoDuration::milliseconds(400);
    let mut tx = pool.begin().await.expect("begin");
    let job = insert_with(
        &mut *tx,
        &PollExternalJob {
            external_id: external_id.into(),
            deadline_at: deadline,
            poll_interval_ms: 50,
        },
        InsertOpts {
            queue: queue.clone(),
            max_attempts: 100,
            ..Default::default()
        },
    )
    .await
    .expect("insert");
    tx.commit().await.expect("commit");

    let client = build_client(pool.clone(), &queue, svc.clone());
    client.start().await.expect("start");
    let (state, errors) = wait_for_terminal(&pool, job.id, Duration::from_secs(5)).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(
        state,
        JobState::Cancelled,
        "job should land in Cancelled once the deadline passes"
    );

    // The handler's `Cancel(reason)` is recorded as an entry in the
    // job's `errors` jsonb[] column. Confirm the deadline-specific
    // reason made it through — future readers triaging the row should
    // see *why* it cancelled.
    let joined = errors
        .iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    assert!(
        joined.contains("deadline") && joined.contains("exceeded"),
        "cancellation reason should mention the deadline; got [{joined}]"
    );

    // The job must have attempted at least once (the deadline branch
    // fires *inside* the handler, so we always burn ≥1 attempt). Most
    // runs see several attempts as RetryAfter cycles before the cutoff.
    let attempt: i16 = sqlx::query_scalar("SELECT attempt FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .expect("read attempt");
    assert!(
        attempt >= 1,
        "expected at least one attempt before cancellation, got {attempt}"
    );
}
