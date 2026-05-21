//! Deadline-bounded polling: try every X until it works or expires.
//!
//! Demonstrates how to compose `JobResult::Snooze` with a handler-side
//! deadline check to get "poll an external system every 30 seconds for
//! up to 30 minutes, then give up cleanly". awa has no built-in
//! `expire_at: TIMESTAMPTZ` field — the pattern is to embed the
//! deadline in the job args (so retries see the same cutoff) and have
//! the handler enforce it.
//!
//! Why `Snooze` and not `RetryAfter`:
//! - `RetryAfter` means "this attempt failed; retry after delay" and
//!   *increments* `attempt`. Sized for genuine failures.
//! - `Snooze` means "this attempt didn't fail, it's just not time
//!   yet" and *preserves* `attempt`. Sized for polling-style waits.
//!
//! Each probe of an unfinished upstream is a "not yet" observation,
//! not a failed attempt — so `Snooze` is the semantically correct
//! primitive. As a bonus, `max_attempts` keeps its default meaning
//! ("bound on real failures") rather than having to be inflated to
//! `ceil(window / interval)` to survive the polling window.
//!
//! What this example exercises:
//! - `JobResult::Snooze` for fixed-interval polling without burning attempts
//! - `JobResult::Cancel` for graceful expiry (no DLQ, no error event)
//! - `JobError::Terminal` for permanent upstream failures
//! - Clamping the next-snooze delay to the deadline so the cutoff
//!   branch fires within one interval of the wall-clock deadline
//!
//! The "external service" is in-process — a `Mutex<HashMap>` that
//! flips from `Pending` to `Ready` (or `Failed`) after a configurable
//! number of polls — so the example runs anywhere awa runs, no
//! mocks or network required.
//!
//! Run:
//!   DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
//!   cargo run -p awa --example poll_until_deadline

use awa::model::insert_with;
use awa::{Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

// ── Mock external service ───────────────────────────────────────────
//
// A real implementation would issue HTTP requests. The shape of the
// handler logic is identical — only `probe` changes.

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
    // Each call counts down `polls_remaining` and returns the
    // *effective* status for this probe — once the counter hits zero
    // the probe both stores Ready and returns Ready, so the worker
    // sees readiness on the same attempt rather than one later.
    if let ExternalStatus::Pending { polls_remaining } = status {
        let next = if polls_remaining <= 1 {
            ExternalStatus::Ready
        } else {
            ExternalStatus::Pending {
                polls_remaining: polls_remaining - 1,
            }
        };
        guard.insert(external_id.to_string(), next.clone());
        return next;
    }
    status
}

// ── Job args ────────────────────────────────────────────────────────
//
// `deadline_at` is the wall-clock cutoff, embedded so retries see
// the same value. `poll_interval_secs` is the nominal cadence; the
// handler clamps the actual delay so the next attempt fires no later
// than `deadline_at`.

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct PollExternalJob {
    external_id: String,
    deadline_at: DateTime<Utc>,
    poll_interval_secs: u64,
}

// ── Worker ──────────────────────────────────────────────────────────

struct PollExternalWorker {
    svc: ExternalSvc,
}

#[async_trait::async_trait]
impl Worker for PollExternalWorker {
    fn kind(&self) -> &'static str {
        // Must match the auto-derived kind from `#[derive(JobArgs)]`
        // on `PollExternalJob` (struct name → snake_case).
        "poll_external_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let args: PollExternalJob = serde_json::from_value(ctx.job.args.clone())
            .map_err(|err| JobError::terminal(format!("invalid args: {err}")))?;
        let now = Utc::now();

        // Deadline check first, on every attempt. `Cancel` is a
        // graceful give-up: state → 'cancelled', no DLQ routing, no
        // failure event. Use `JobError::Terminal` instead if you want
        // the job in the DLQ for inspection.
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
                // Clamp the next snooze so it does not overshoot the
                // deadline. Without this, a poll at deadline−1s would
                // schedule the next attempt at deadline+(interval−1)s
                // and the job would sit `scheduled` past the cutoff.
                let nominal_next = now + ChronoDuration::seconds(args.poll_interval_secs as i64);
                let next = nominal_next.min(args.deadline_at);
                let delay = (next - now).to_std().unwrap_or(Duration::from_millis(1));
                Ok(JobResult::Snooze(delay))
            }
        }
    }
}

// ── Enqueue + run ───────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".into());
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    awa::model::migrations::run(&pool).await?;

    // Seed the mock external service with three jobs that take
    // different paths: completes after 3 polls, never ready, fails
    // immediately.
    let svc: ExternalSvc = Arc::new(Mutex::new(HashMap::from([
        (
            "eventually-ready".into(),
            ExternalStatus::Pending { polls_remaining: 3 },
        ),
        (
            "never-ready".into(),
            ExternalStatus::Pending {
                polls_remaining: 9999,
            },
        ),
        ("upstream-broken".into(), ExternalStatus::Failed),
    ])));

    // Short cadences so the example finishes in a few seconds —
    // production would be poll_interval_secs=30, window=30 minutes.
    // `max_attempts` keeps its default (25): Snooze does not burn
    // attempts, so this only caps genuine repeated failures.
    let queue = "poll_example";
    let poll_interval_secs: u64 = 1;
    let window = ChronoDuration::seconds(5);

    let mut tx = pool.begin().await?;
    for external_id in ["eventually-ready", "never-ready", "upstream-broken"] {
        insert_with(
            &mut *tx,
            &PollExternalJob {
                external_id: external_id.into(),
                deadline_at: Utc::now() + window,
                poll_interval_secs,
            },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await?;
    }
    tx.commit().await?;
    tracing::info!("enqueued 3 PollExternalJob (eventually-ready, never-ready, upstream-broken)");

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        // Tight intervals to make the demo visibly progress —
        // production defaults are fine for real deployments.
        .promote_interval(Duration::from_millis(100))
        .register_worker(PollExternalWorker { svc: svc.clone() })
        .build()?;

    client.start().await?;
    tracing::info!("client started — watch the logs; expect ~10s total runtime");
    tokio::time::sleep(window.to_std()? + Duration::from_secs(2)).await;
    client.shutdown(Duration::from_secs(5)).await;

    // Report terminal states.
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT id, args->>'external_id', state::text \
         FROM awa.jobs WHERE queue = $1 ORDER BY id",
    )
    .bind(queue)
    .fetch_all(&pool)
    .await?;
    for (id, external_id, state) in rows {
        tracing::info!(job_id = id, %external_id, %state, "terminal state");
    }
    Ok(())
}
