//! Integration tests for builder-side lifecycle hooks.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::queue_storage::{QueueStorage, QueueStorageConfig};
use awa::model::{admin, migrations};
use awa::{
    Client, JobArgs, JobError, JobEvent, JobResult, JobState, QueueConfig, UntypedJobEvent, Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup_pool() -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&database_url())
        .await
        .expect("Failed to connect to database — is Postgres running?");
    // Wipe and re-migrate so tests start from a known state regardless
    // of what previous tests left behind (queue-storage tables, an
    // advanced storage_transition_state, etc.).
    sqlx::query("DROP SCHEMA IF EXISTS awa CASCADE")
        .execute(&pool)
        .await
        .expect("Failed to drop awa schema");
    migrations::run(&pool)
        .await
        .expect("Failed to run migrations");

    QueueStorage::new(QueueStorageConfig::default())
        .expect("Failed to build queue storage")
        .install(&pool)
        .await
        .expect("Failed to install queue storage");
    pool
}

async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue jobs");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue meta");
}

async fn recv_event<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("Timed out waiting for lifecycle event")
        .expect("Lifecycle event channel closed")
}

fn test_gate() -> Arc<Semaphore> {
    static GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    GATE.get_or_init(|| Arc::new(Semaphore::new(1))).clone()
}

async fn active_queue_storage_schema(pool: &sqlx::PgPool) -> Option<String> {
    sqlx::query_scalar("SELECT awa.active_queue_storage_schema()")
        .fetch_optional(pool)
        .await
        .expect("Failed to query active queue storage schema")
        .flatten()
}

async fn backdate_running_heartbeat(pool: &sqlx::PgPool, job_id: i64) {
    if let Some(schema) = active_queue_storage_schema(pool).await {
        sqlx::query(&format!(
            "UPDATE {schema}.leases \
             SET heartbeat_at = now() - interval '5 minutes' \
             WHERE job_id = $1 AND state = 'running'"
        ))
        .bind(job_id)
        .execute(pool)
        .await
        .expect("Failed to backdate queue-storage heartbeat");
        return;
    }

    sqlx::query("UPDATE awa.jobs SET heartbeat_at = now() - interval '5 minutes' WHERE id = $1")
        .bind(job_id)
        .execute(pool)
        .await
        .expect("Failed to backdate heartbeat");
}

async fn wait_for_job_state(pool: &sqlx::PgPool, job_id: i64, state: JobState) -> awa::JobRow {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(job) = admin::get_job(pool, job_id).await {
            if job.state == state {
                return job;
            }
        }

        if tokio::time::Instant::now() >= deadline {
            panic!("Timed out waiting for job {job_id} to reach state {state:?}");
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct HookJob {
    action: String,
    value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct RawHookJob {
    value: String,
}

#[tokio::test]
async fn test_typed_completed_event_handler_runs() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup_pool().await;
    let queue = "lifecycle_completed";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<HookJob, _, _>(|_args, _ctx| async move { Ok(JobResult::Completed) })
        .on_event::<HookJob, _, _>(move |event| {
            let tx = tx.clone();
            async move {
                if let JobEvent::Completed { args, job, .. } = event {
                    tx.send((args.value, job.id, job.state)).unwrap();
                }
            }
        })
        .build()
        .unwrap();

    let inserted = awa::insert_with(
        &pool,
        &HookJob {
            action: "complete".into(),
            value: "alpha".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let (value, event_job_id, event_state) = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(value, "alpha");
    assert_eq!(event_job_id, inserted.id);
    assert_eq!(event_state, JobState::Completed);

    let stored = admin::get_job(&pool, inserted.id).await.unwrap();
    assert_eq!(stored.state, JobState::Completed);
}

#[tokio::test]
async fn test_typed_started_event_handler_runs() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup_pool().await;
    let queue = "lifecycle_started";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<HookJob, _, _>(|_args, _ctx| async move { Ok(JobResult::Completed) })
        .on_event::<HookJob, _, _>(move |event| {
            let tx = tx.clone();
            async move {
                if let JobEvent::Started { args, job } = event {
                    tx.send((args.value, job.id, job.state)).unwrap();
                }
            }
        })
        .build()
        .unwrap();

    let inserted = awa::insert_with(
        &pool,
        &HookJob {
            action: "start".into(),
            value: "just_started".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let (value, event_job_id, event_state) = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(value, "just_started");
    assert_eq!(event_job_id, inserted.id);
    assert_eq!(event_state, JobState::Running);
}

#[tokio::test]
async fn test_typed_retried_event_handler_runs() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup_pool().await;
    let queue = "lifecycle_retried";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<HookJob, _, _>(|args, _ctx| async move {
            Err(JobError::retryable_msg(format!("retry {}", args.value)))
        })
        .on_event::<HookJob, _, _>(move |event| {
            let tx = tx.clone();
            async move {
                if let JobEvent::Retried {
                    args,
                    job,
                    error,
                    attempt,
                    next_run_at,
                } = event
                {
                    tx.send((args.value, job.state, error, attempt, next_run_at))
                        .unwrap();
                }
            }
        })
        .build()
        .unwrap();

    let inserted = awa::insert_with(
        &pool,
        &HookJob {
            action: "retry".into(),
            value: "beta".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            max_attempts: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let (value, event_state, error, attempt, next_run_at) = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(value, "beta");
    assert_eq!(event_state, JobState::Retryable);
    assert_eq!(attempt, 1);
    assert!(error.contains("retry beta"));
    assert!(next_run_at > inserted.run_at);

    let stored = admin::get_job(&pool, inserted.id).await.unwrap();
    assert_eq!(stored.state, JobState::Retryable);
}

#[tokio::test]
async fn test_typed_exhausted_event_handler_runs() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup_pool().await;
    let queue = "lifecycle_exhausted";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<HookJob, _, _>(|args, _ctx| async move {
            Err(JobError::retryable_msg(format!("boom {}", args.value)))
        })
        .on_event::<HookJob, _, _>(move |event| {
            let tx = tx.clone();
            async move {
                if let JobEvent::Exhausted {
                    args,
                    job,
                    error,
                    attempt,
                } = event
                {
                    tx.send((args.value, job.state, error, attempt)).unwrap();
                }
            }
        })
        .build()
        .unwrap();

    let inserted = awa::insert_with(
        &pool,
        &HookJob {
            action: "exhaust".into(),
            value: "gamma".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            max_attempts: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let (value, event_state, error, attempt) = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(value, "gamma");
    assert_eq!(event_state, JobState::Failed);
    assert_eq!(attempt, 1);
    assert!(error.contains("boom gamma"));

    let stored = admin::get_job(&pool, inserted.id).await.unwrap();
    assert_eq!(stored.state, JobState::Failed);
}

#[tokio::test]
async fn test_typed_cancelled_event_handler_runs() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup_pool().await;
    let queue = "lifecycle_cancelled";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<HookJob, _, _>(|args, _ctx| async move {
            Ok(JobResult::Cancel(format!("cancel {}", args.value)))
        })
        .on_event::<HookJob, _, _>(move |event| {
            let tx = tx.clone();
            async move {
                if let JobEvent::Cancelled { args, job, reason } = event {
                    tx.send((args.value, job.state, reason)).unwrap();
                }
            }
        })
        .build()
        .unwrap();

    let inserted = awa::insert_with(
        &pool,
        &HookJob {
            action: "cancel".into(),
            value: "delta".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let (value, event_state, reason) = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(value, "delta");
    assert_eq!(event_state, JobState::Cancelled);
    assert_eq!(reason, "cancel delta");

    let stored = admin::get_job(&pool, inserted.id).await.unwrap();
    assert_eq!(stored.state, JobState::Cancelled);
}

struct RawHookWorker;

#[async_trait::async_trait]
impl Worker for RawHookWorker {
    fn kind(&self) -> &'static str {
        RawHookJob::kind()
    }

    async fn perform(&self, _ctx: &awa::JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::Completed)
    }
}

#[tokio::test]
async fn test_untyped_event_handlers_stack_for_raw_workers() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup_pool().await;
    let queue = "lifecycle_raw_stack";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register_worker(RawHookWorker)
        .on_event_kind(RawHookJob::kind(), {
            let tx = tx.clone();
            move |event| {
                let tx = tx.clone();
                async move {
                    if let UntypedJobEvent::Completed { job, .. } = event {
                        tx.send(("first".to_string(), job.id, job.state)).unwrap();
                    }
                }
            }
        })
        .on_event_kind(RawHookJob::kind(), move |event| {
            let tx = tx.clone();
            async move {
                if let UntypedJobEvent::Completed { job, .. } = event {
                    tx.send(("second".to_string(), job.id, job.state)).unwrap();
                }
            }
        })
        .build()
        .unwrap();

    let inserted = awa::insert_with(
        &pool,
        &RawHookJob {
            value: "epsilon".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let first = recv_event(&mut rx).await;
    let second = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    let labels = [first.0, second.0];
    assert!(labels.contains(&"first".to_string()));
    assert!(labels.contains(&"second".to_string()));
    assert_eq!(first.1, inserted.id);
    assert_eq!(second.1, inserted.id);
    assert_eq!(first.2, JobState::Completed);
    assert_eq!(second.2, JobState::Completed);
}

// ── Edge case: handler panic doesn't crash executor ─────────────

#[tokio::test]
async fn test_handler_panic_does_not_crash_executor() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup_pool().await;
    let queue = "lifecycle_panic";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<HookJob, _, _>(|_args, _ctx| async move { Ok(JobResult::Completed) })
        // First handler panics
        .on_event::<HookJob, _, _>(|event| async move {
            if matches!(event, JobEvent::Completed { .. }) {
                panic!("handler exploded!");
            }
        })
        // Second handler should still run despite the first panicking
        .on_event::<HookJob, _, _>(move |event| {
            let tx = tx.clone();
            async move {
                if let JobEvent::Completed { args, .. } = event {
                    tx.send(args.value).unwrap();
                }
            }
        })
        .build()
        .unwrap();

    awa::insert_with(
        &pool,
        &HookJob {
            action: "panic".into(),
            value: "survives".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    // The second handler should still fire
    let value = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(value, "survives");
}

// ── Edge case: no handlers registered — no extra DB query ───────

#[tokio::test]
async fn test_no_handlers_registered_still_completes() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup_pool().await;
    let queue = "lifecycle_no_handlers";
    clean_queue(&pool, queue).await;

    // No on_event registered — should work without any lifecycle overhead
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<HookJob, _, _>(|_args, _ctx| async move { Ok(JobResult::Completed) })
        .build()
        .unwrap();

    let inserted = awa::insert_with(
        &pool,
        &HookJob {
            action: "no_hooks".into(),
            value: "zeta".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let stored = wait_for_job_state(&pool, inserted.id, JobState::Completed).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(stored.state, JobState::Completed);
}

// ── Edge case: stale completion (job rescued) — no event fires ──

#[tokio::test]
async fn test_stale_completion_does_not_fire_event() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup_pool().await;
    let queue = "lifecycle_stale";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<HookJob, _, _>(|_args, ctx| async move {
            // Simulate slow handler — during which rescue could fire
            // The job will be rescued by heartbeat while we sleep
            tokio::time::sleep(Duration::from_secs(10)).await;
            // By the time we return, the job's lease has been bumped
            // so our completion will be stale
            let _ = ctx;
            Ok(JobResult::Completed)
        })
        .on_event::<HookJob, _, _>(move |event| {
            let tx = tx.clone();
            async move {
                // Send any event we receive
                match event {
                    JobEvent::Started { args, .. } => {
                        tx.send(format!("started:{}", args.value)).unwrap()
                    }
                    JobEvent::Completed { args, .. } => {
                        tx.send(format!("completed:{}", args.value)).unwrap()
                    }
                    JobEvent::Retried { args, .. } => {
                        tx.send(format!("retried:{}", args.value)).unwrap()
                    }
                    JobEvent::Exhausted { args, .. } => {
                        tx.send(format!("exhausted:{}", args.value)).unwrap()
                    }
                    JobEvent::Cancelled { args, .. } => {
                        tx.send(format!("cancelled:{}", args.value)).unwrap()
                    }
                    JobEvent::WaitingForCallback { args, .. } => {
                        tx.send(format!("waiting:{}", args.value)).unwrap()
                    }
                }
            }
        })
        .leader_election_interval(Duration::from_millis(100))
        .heartbeat_rescue_interval(Duration::from_millis(500))
        .build()
        .unwrap();

    let inserted = awa::insert_with(
        &pool,
        &HookJob {
            action: "stale".into(),
            value: "should_not_fire".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            max_attempts: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Immediately mark heartbeat as stale so rescue fires quickly
    backdate_running_heartbeat(&pool, inserted.id).await;

    client.start().await.unwrap();

    // Wait for rescue to fire and the handler to return stale
    tokio::time::sleep(Duration::from_secs(3)).await;
    client.shutdown(Duration::from_secs(2)).await;

    // Started may fire for claimed attempts, but the stale completion must not
    // produce a Completed event.
    while let Ok(msg) = rx.try_recv() {
        assert!(
            !msg.starts_with("completed:"),
            "Stale completion should not fire a Completed event, got: {msg}"
        );
    }
}

// ── Edge case: terminal error emits Exhausted ────────────────────

#[tokio::test]
async fn test_terminal_error_emits_exhausted() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup_pool().await;
    let queue = "lifecycle_terminal";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<HookJob, _, _>(|_args, _ctx| async move {
            Err(JobError::terminal("permanent failure"))
        })
        .on_event::<HookJob, _, _>(move |event| {
            let tx = tx.clone();
            async move {
                match event {
                    JobEvent::Started { .. } => {}
                    JobEvent::Exhausted { error, attempt, .. } => {
                        tx.send(("exhausted".to_string(), error, attempt)).unwrap();
                    }
                    other => {
                        tx.send((format!("{other:?}"), String::new(), 0)).unwrap();
                    }
                }
            }
        })
        .build()
        .unwrap();

    awa::insert_with(
        &pool,
        &HookJob {
            action: "terminal".into(),
            value: "eta".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            max_attempts: 5, // Plenty of retries — but terminal skips them all
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let (event_type, error, attempt) = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(event_type, "exhausted");
    assert!(error.contains("permanent failure"));
    assert_eq!(attempt, 1); // Only ran once — terminal, not retried
}

// ── Edge case: snooze emits Started but no outcome event ─────────

#[tokio::test]
async fn test_snooze_only_emits_started_event() {
    let _permit = test_gate()
        .acquire_owned()
        .await
        .expect("test gate should be available");
    let pool = setup_pool().await;
    let queue = "lifecycle_snooze";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register::<HookJob, _, _>(|_args, _ctx| async move {
            Ok(JobResult::Snooze(Duration::from_secs(3600)))
        })
        .on_event::<HookJob, _, _>(move |event| {
            let tx = tx.clone();
            async move {
                let label = match &event {
                    JobEvent::Started { .. } => "started",
                    JobEvent::WaitingForCallback { .. } => "waiting",
                    JobEvent::Completed { .. } => "completed",
                    JobEvent::Retried { .. } => "retried",
                    JobEvent::Exhausted { .. } => "exhausted",
                    JobEvent::Cancelled { .. } => "cancelled",
                };
                let _ = tx.send(label.to_string());
            }
        })
        .build()
        .unwrap();

    awa::insert_with(
        &pool,
        &HookJob {
            action: "snooze".into(),
            value: "theta".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client.start().await.unwrap();
    let label = recv_event(&mut rx).await;
    assert_eq!(label, "started");

    // Give enough time for the job to be claimed and snoozed
    tokio::time::sleep(Duration::from_millis(500)).await;
    client.shutdown(Duration::from_secs(2)).await;

    // No outcome event should have fired.
    assert!(
        rx.try_recv().is_err(),
        "Snooze should not produce a lifecycle outcome event"
    );
}

// ── Callback lifecycle events ───────────────────────────────────────────

#[derive(Debug)]
enum CbEvent {
    Waiting(awa::JobRow),
    Completed(awa::JobRow, Duration),
    Retried { job: awa::JobRow, attempt: i16 },
    Exhausted(awa::JobRow, String),
}

/// Like [`setup_pool`] but leaves the runtime on canonical storage (queue
/// storage is never installed), so tests can exercise the canonical executor
/// path.
async fn setup_pool_canonical() -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url())
        .await
        .expect("Failed to connect to database — is Postgres running?");
    sqlx::query("DROP SCHEMA IF EXISTS awa CASCADE")
        .execute(&pool)
        .await
        .expect("Failed to drop awa schema");
    migrations::run(&pool)
        .await
        .expect("Failed to run migrations");
    pool
}

/// Worker that parks on an external callback. Uses a `Worker` impl rather than
/// a closure because awaiting `ctx.register_callback()` borrows the context,
/// which the `'static` closure-handler bound disallows.
struct ParkingWorker;

#[async_trait::async_trait]
impl Worker for ParkingWorker {
    fn kind(&self) -> &'static str {
        HookJob::kind()
    }

    async fn perform(&self, ctx: &awa::JobContext) -> Result<JobResult, JobError> {
        let callback = ctx
            .register_callback(Duration::from_secs(3600))
            .await
            .map_err(JobError::retryable)?;
        Ok(JobResult::WaitForCallback(callback))
    }
}

/// Build a client whose `HookJob` worker parks on an external callback and
/// whose hook forwards callback lifecycle events onto `tx`. With `canonical`
/// set, the runtime uses canonical storage instead of queue storage.
fn parking_client(
    pool: &sqlx::PgPool,
    queue: &str,
    canonical: bool,
    tx: mpsc::UnboundedSender<CbEvent>,
) -> Client {
    let mut builder = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                poll_interval: Duration::from_millis(25),
                ..Default::default()
            },
        )
        .register_worker(ParkingWorker)
        .on_event::<HookJob, _, _>(move |event| {
            let tx = tx.clone();
            async move {
                match event {
                    JobEvent::WaitingForCallback { job, .. } => {
                        let _ = tx.send(CbEvent::Waiting(job));
                    }
                    JobEvent::Completed { job, duration, .. } => {
                        let _ = tx.send(CbEvent::Completed(job, duration));
                    }
                    JobEvent::Retried { job, attempt, .. } => {
                        let _ = tx.send(CbEvent::Retried { job, attempt });
                    }
                    JobEvent::Exhausted { job, error, .. } => {
                        let _ = tx.send(CbEvent::Exhausted(job, error));
                    }
                    _ => {}
                }
            }
        });
    if canonical {
        builder = builder.canonical_storage();
    }
    builder.build().unwrap()
}

async fn insert_hook_job(pool: &sqlx::PgPool, queue: &str, value: &str) -> awa::JobRow {
    awa::insert_with(
        pool,
        &HookJob {
            action: "wait".into(),
            value: value.into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn test_waiting_for_callback_event_fires_on_park() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool().await;
    let queue = "lifecycle_waiting";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = parking_client(&pool, queue, false, tx);
    let inserted = insert_hook_job(&pool, queue, "park").await;

    client.start().await.unwrap();
    let event = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    match event {
        CbEvent::Waiting(job) => {
            assert_eq!(job.id, inserted.id);
            assert_eq!(job.state, JobState::WaitingExternal);
            assert!(job.callback_id.is_some(), "callback_id should be set");
        }
        other => panic!("expected WaitingForCallback, got {other:?}"),
    }
}

#[tokio::test]
async fn test_client_complete_external_dispatches_completed_event() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool().await;
    let queue = "lifecycle_cb_complete";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = parking_client(&pool, queue, false, tx);
    let inserted = insert_hook_job(&pool, queue, "complete").await;

    client.start().await.unwrap();

    // Park first.
    let callback_id = match recv_event(&mut rx).await {
        CbEvent::Waiting(job) => job.callback_id.expect("callback_id set"),
        other => panic!("expected Waiting, got {other:?}"),
    };

    // Resolve through the worker Client → Completed hook should fire.
    let completed = client
        .complete_external(callback_id, None, None)
        .await
        .unwrap();
    assert_eq!(completed.id, inserted.id);

    let event = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    match event {
        CbEvent::Completed(job, duration) => {
            assert_eq!(job.id, inserted.id);
            assert_eq!(job.state, JobState::Completed);
            assert_eq!(
                duration,
                Duration::ZERO,
                "callback completion has no handler duration"
            );
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

#[tokio::test]
async fn test_client_fail_external_dispatches_exhausted_event() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool().await;
    let queue = "lifecycle_cb_fail";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = parking_client(&pool, queue, false, tx);
    let inserted = insert_hook_job(&pool, queue, "fail").await;

    client.start().await.unwrap();

    let callback_id = match recv_event(&mut rx).await {
        CbEvent::Waiting(job) => job.callback_id.expect("callback_id set"),
        other => panic!("expected Waiting, got {other:?}"),
    };

    client
        .fail_external(callback_id, "payment declined", None)
        .await
        .unwrap();

    let event = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    match event {
        CbEvent::Exhausted(job, error) => {
            assert_eq!(job.id, inserted.id);
            assert_eq!(job.state, JobState::Failed);
            assert_eq!(error, "payment declined");
        }
        other => panic!("expected Exhausted, got {other:?}"),
    }
}

#[tokio::test]
async fn test_client_resolve_callback_dispatches_completed_event() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool().await;
    let queue = "lifecycle_cb_resolve";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = parking_client(&pool, queue, false, tx);
    let inserted = insert_hook_job(&pool, queue, "resolve").await;

    client.start().await.unwrap();
    let callback_id = match recv_event(&mut rx).await {
        CbEvent::Waiting(job) => job.callback_id.expect("callback_id set"),
        other => panic!("expected Waiting, got {other:?}"),
    };

    let outcome = client
        .resolve_callback(callback_id, None, awa::DefaultAction::Complete, None)
        .await
        .unwrap();
    assert!(outcome.is_completed());

    let event = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    match event {
        CbEvent::Completed(job, duration) => {
            assert_eq!(job.id, inserted.id);
            assert_eq!(job.state, JobState::Completed);
            assert_eq!(duration, Duration::ZERO);
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

#[tokio::test]
async fn test_client_retry_external_dispatches_retried_event() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool().await;
    let queue = "lifecycle_cb_retry";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = parking_client(&pool, queue, false, tx);
    let inserted = insert_hook_job(&pool, queue, "retry").await;

    client.start().await.unwrap();
    let callback_id = match recv_event(&mut rx).await {
        CbEvent::Waiting(job) => job.callback_id.expect("callback_id set"),
        other => panic!("expected Waiting, got {other:?}"),
    };

    client.retry_external(callback_id, None).await.unwrap();

    // The worker will re-claim the now-retryable job and park it again, so we
    // may observe a fresh Waiting after the Retried; assert we see Retried,
    // and that the event's attempt is the parked attempt (>= 1), not the
    // post-transition value (admin::retry_external resets attempt to 0 as
    // part of requeuing).
    let mut saw_retried = false;
    for _ in 0..3 {
        match recv_event(&mut rx).await {
            CbEvent::Retried { job, attempt } => {
                assert_eq!(job.id, inserted.id);
                assert!(
                    attempt >= 1,
                    "Retried event must report the parked (failed) attempt, not the post-reset 0; got {attempt}"
                );
                saw_retried = true;
                break;
            }
            CbEvent::Waiting(_) => continue,
            other => panic!("expected Retried/Waiting, got {other:?}"),
        }
    }
    client.shutdown(Duration::from_secs(2)).await;
    assert!(saw_retried, "expected a Retried event after retry_external");
}

#[tokio::test]
async fn test_waiting_for_callback_event_fires_on_canonical_storage() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool_canonical().await;
    let queue = "lifecycle_waiting_canonical";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = parking_client(&pool, queue, true, tx);
    let inserted = insert_hook_job(&pool, queue, "park").await;

    client.start().await.unwrap();
    let event = recv_event(&mut rx).await;
    client.shutdown(Duration::from_secs(2)).await;

    match event {
        CbEvent::Waiting(job) => {
            assert_eq!(job.id, inserted.id);
            assert_eq!(job.state, JobState::WaitingExternal);
            assert!(job.callback_id.is_some());
        }
        other => panic!("expected WaitingForCallback on canonical storage, got {other:?}"),
    }
}

#[tokio::test]
async fn test_bare_admin_resolution_fires_no_hook() {
    let _permit = test_gate().acquire_owned().await.unwrap();
    let pool = setup_pool().await;
    let queue = "lifecycle_cb_bare_admin";
    clean_queue(&pool, queue).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = parking_client(&pool, queue, false, tx);
    let inserted = insert_hook_job(&pool, queue, "bare").await;

    client.start().await.unwrap();
    let callback_id = match recv_event(&mut rx).await {
        CbEvent::Waiting(job) => job.callback_id.expect("callback_id set"),
        other => panic!("expected Waiting, got {other:?}"),
    };

    // Resolve through the bare admin function (the boundary): the job
    // transitions but no lifecycle hook should fire.
    admin::complete_external(&pool, callback_id, None, None)
        .await
        .unwrap();
    let completed = wait_for_job_state(&pool, inserted.id, JobState::Completed).await;
    assert_eq!(completed.state, JobState::Completed);

    tokio::time::sleep(Duration::from_millis(300)).await;
    client.shutdown(Duration::from_secs(2)).await;
    assert!(
        rx.try_recv().is_err(),
        "bare admin resolution must not dispatch a lifecycle hook"
    );
}
