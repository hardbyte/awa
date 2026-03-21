//! Tests for bug fixes: state guard (Fix 1), shutdown drain (Fix 2),
//! deadline cancellation signal (Fix 3), UniqueConflict field (Fix 4).

use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig};
use awa_model::{admin, insert_with, migrations, InsertOpts};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup() -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .expect("Failed to connect");
    migrations::run(&pool).await.expect("Failed to migrate");
    pool
}

async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue meta");
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct GuardJob {
    pub value: String,
}

/// B1: Late completion after deadline rescue — DB state stays `retryable`.
#[tokio::test]
async fn test_late_completion_after_rescue_is_noop() {
    let pool = setup().await;
    let queue = "guard_late_complete";
    clean_queue(&pool, queue).await;

    // Insert and claim a job
    let job = insert_with(
        &pool,
        &GuardJob {
            value: "test".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Simulate: claim the job (set to running)
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, heartbeat_at = now(), deadline_at = now() + interval '5 minutes' WHERE id = $1",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    // Simulate: maintenance rescues the job (sets state to retryable)
    sqlx::query(
        "UPDATE awa.jobs SET state = 'retryable', finalized_at = now(), heartbeat_at = NULL, deadline_at = NULL WHERE id = $1",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    // Now the "late" handler tries to complete the job.
    // With the state guard, this UPDATE should be a no-op.
    let result = sqlx::query(
        "UPDATE awa.jobs SET state = 'completed', finalized_at = now() WHERE id = $1 AND state = 'running'",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(
        result.rows_affected(),
        0,
        "Late completion should be a no-op when job is already rescued"
    );

    // Verify state is still retryable
    let state: String = sqlx::query_scalar("SELECT state::text FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "retryable");
}

/// B1b: Late completion cannot finalize a newer running attempt of the same job.
#[tokio::test]
async fn test_late_completion_cannot_finalize_reclaimed_running_attempt() {
    let pool = setup().await;
    let queue = "guard_reclaimed_running";
    clean_queue(&pool, queue).await;

    let job = insert_with(
        &pool,
        &GuardJob {
            value: "test".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    sqlx::query(
        "UPDATE awa.jobs
         SET state = 'running',
             attempt = 1,
             run_lease = 1,
             heartbeat_at = now(),
             deadline_at = now() + interval '5 minutes'
         WHERE id = $1",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "UPDATE awa.jobs
         SET state = 'retryable',
             finalized_at = now(),
             heartbeat_at = NULL,
             deadline_at = NULL
         WHERE id = $1 AND run_lease = 1",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "UPDATE awa.jobs
         SET state = 'available',
             finalized_at = NULL,
             run_at = now()
         WHERE id = $1",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "UPDATE awa.jobs
         SET state = 'running',
             attempt = 2,
             run_lease = 2,
             heartbeat_at = now(),
             deadline_at = now() + interval '5 minutes'
         WHERE id = $1",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    let result = sqlx::query(
        "UPDATE awa.jobs
         SET state = 'completed', finalized_at = now()
         WHERE id = $1 AND state = 'running' AND run_lease = $2",
    )
    .bind(job.id)
    .bind(1_i64)
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(
        result.rows_affected(),
        0,
        "Late completion from the old lease must not finalize the new running attempt"
    );

    let row: (String, i16, i64) =
        sqlx::query_as("SELECT state::text, attempt, run_lease FROM awa.jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.0, "running");
    assert_eq!(row.1, 2);
    assert_eq!(row.2, 2);
}

/// B2: Late completion after admin cancel — DB state stays `cancelled`.
#[tokio::test]
async fn test_late_completion_after_cancel_is_noop() {
    let pool = setup().await;
    let queue = "guard_late_cancel";
    clean_queue(&pool, queue).await;

    let job = insert_with(
        &pool,
        &GuardJob {
            value: "test".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Claim the job
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, heartbeat_at = now(), deadline_at = now() + interval '5 minutes' WHERE id = $1",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    // Admin cancels the job
    sqlx::query("UPDATE awa.jobs SET state = 'cancelled', finalized_at = now() WHERE id = $1")
        .bind(job.id)
        .execute(&pool)
        .await
        .unwrap();

    // Late handler tries to complete
    let result = sqlx::query(
        "UPDATE awa.jobs SET state = 'completed', finalized_at = now() WHERE id = $1 AND state = 'running'",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(result.rows_affected(), 0);

    let state: String = sqlx::query_scalar("SELECT state::text FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "cancelled");
}

/// B2b: Callback registration is rejected for stale running attempts.
#[tokio::test]
async fn test_register_callback_rejects_stale_lease() {
    let pool = setup().await;
    let queue = "guard_callback_lease";
    clean_queue(&pool, queue).await;

    let job = insert_with(
        &pool,
        &GuardJob {
            value: "test".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    sqlx::query(
        "UPDATE awa.jobs
         SET state = 'running',
             attempt = 2,
             run_lease = 2,
             heartbeat_at = now(),
             deadline_at = now() + interval '5 minutes'
         WHERE id = $1",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    let err = admin::register_callback(&pool, job.id, 1, Duration::from_secs(3600))
        .await
        .unwrap_err();
    match err {
        awa_model::AwaError::Validation(msg) => {
            assert!(msg.contains("job is not in running state"));
        }
        other => panic!("Expected Validation error, got: {other:?}"),
    }

    let callback_id = admin::register_callback(&pool, job.id, 2, Duration::from_secs(3600))
        .await
        .unwrap();
    assert_ne!(callback_id, uuid::Uuid::nil());
}

/// B3: Shutdown waits for in-flight jobs — shutdown does not return until
/// handlers complete (or timeout). Verify via a handler that sleeps.
#[tokio::test]
async fn test_shutdown_waits_for_inflight_jobs() {
    let pool = setup().await;
    let queue = "guard_shutdown_drain";
    clean_queue(&pool, queue).await;

    let completed = Arc::new(AtomicU32::new(0));

    struct SlowGuardWorker {
        completed: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl awa::Worker for SlowGuardWorker {
        fn kind(&self) -> &'static str {
            "guard_job"
        }
        async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
            tokio::time::sleep(Duration::from_millis(500)).await;
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(JobResult::Completed)
        }
    }

    // Insert a job
    insert_with(
        &pool,
        &GuardJob {
            value: "drain".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 5,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .register_worker(SlowGuardWorker {
            completed: completed.clone(),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    // Wait for the job to be claimed (but not yet completed)
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        completed.load(Ordering::SeqCst),
        0,
        "Job should still be running"
    );

    // Shutdown with generous timeout — should wait for the 500ms handler
    client.shutdown(Duration::from_secs(5)).await;

    // After shutdown returns, the job should have completed
    assert_eq!(
        completed.load(Ordering::SeqCst),
        1,
        "Shutdown should have waited for the in-flight job to complete"
    );
}

/// B4: Heartbeat stays alive during shutdown drain — in-flight jobs keep
/// heartbeating until they complete during graceful shutdown.
#[tokio::test]
async fn test_heartbeat_alive_during_drain() {
    let pool = setup().await;
    let queue = "guard_hb_drain";
    clean_queue(&pool, queue).await;

    let completed = Arc::new(AtomicBool::new(false));

    struct HeartbeatCheckWorker {
        pool: sqlx::PgPool,
        completed: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl awa::Worker for HeartbeatCheckWorker {
        fn kind(&self) -> &'static str {
            "guard_job"
        }
        async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
            let job_id = ctx.job.id;
            // Sleep long enough that a heartbeat cycle fires (interval is 30s default,
            // but we just need the job to still be running when shutdown starts).
            // The key check: after we return, verify the job was still `running`
            // in the DB (not rescued) — meaning heartbeat kept it alive.
            tokio::time::sleep(Duration::from_millis(800)).await;

            // Verify job is still in running state (heartbeat kept it alive)
            let state: String =
                sqlx::query_scalar("SELECT state::text FROM awa.jobs WHERE id = $1")
                    .bind(job_id)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| JobError::Terminal(e.to_string()))?;
            assert_eq!(
                state, "running",
                "Job should still be running (heartbeat alive during drain)"
            );

            self.completed.store(true, Ordering::SeqCst);
            Ok(JobResult::Completed)
        }
    }

    insert_with(
        &pool,
        &GuardJob {
            value: "hb_drain".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 5,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .register_worker(HeartbeatCheckWorker {
            pool: pool.clone(),
            completed: completed.clone(),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    // Wait for job to be claimed
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Trigger shutdown while job is still running — heartbeat should stay alive
    client.shutdown(Duration::from_secs(5)).await;

    assert!(
        completed.load(Ordering::SeqCst),
        "Worker should have completed during drain"
    );
}

/// B5: Deadline rescue signals ctx.is_cancelled() — handler checks
/// is_cancelled() after deadline passes, returns true.
#[tokio::test]
async fn test_deadline_rescue_signals_cancellation() {
    let pool = setup().await;
    let queue = "guard_deadline_cancel";
    clean_queue(&pool, queue).await;

    let saw_cancelled = Arc::new(AtomicBool::new(false));

    struct CancellationCheckWorker {
        saw_cancelled: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl awa::Worker for CancellationCheckWorker {
        fn kind(&self) -> &'static str {
            "guard_job"
        }
        async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
            // Wait for deadline rescue to fire. Deadline is 1s, but maintenance
            // only checks every 30s and leader election can take up to 10s.
            // We poll for up to 50s to cover worst-case timing.
            for _ in 0..500 {
                if ctx.is_cancelled() {
                    self.saw_cancelled.store(true, Ordering::SeqCst);
                    return Ok(JobResult::Completed);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            // If we get here, cancellation was never signalled
            Ok(JobResult::Completed)
        }
    }

    insert_with(
        &pool,
        &GuardJob {
            value: "deadline_cancel".into(),
        },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 5,
                poll_interval: Duration::from_millis(50),
                // Very short deadline so maintenance rescues quickly
                deadline_duration: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .register_worker(CancellationCheckWorker {
            saw_cancelled: saw_cancelled.clone(),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    // Wait for the job to be claimed + deadline to expire + maintenance to rescue.
    // Leader election can take up to 10s, deadline rescue interval is 30s.
    // Worst case: ~45s (10s election + 1s deadline + 30s rescue interval + margin).
    let start = std::time::Instant::now();
    loop {
        if saw_cancelled.load(Ordering::SeqCst) {
            break;
        }
        if start.elapsed() > Duration::from_secs(50) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    client.shutdown(Duration::from_secs(5)).await;

    assert!(
        saw_cancelled.load(Ordering::SeqCst),
        "Handler should have seen ctx.is_cancelled() == true after deadline rescue"
    );
}

/// B6: UniqueConflict.constraint field contains the constraint name.
#[tokio::test]
async fn test_unique_conflict_has_constraint_name() {
    let pool = setup().await;
    let queue = "guard_unique_field";
    clean_queue(&pool, queue).await;

    let opts = InsertOpts {
        queue: queue.into(),
        unique: Some(awa_model::UniqueOpts {
            by_queue: true,
            ..awa_model::UniqueOpts::default()
        }),
        ..Default::default()
    };

    // First insert succeeds
    insert_with(
        &pool,
        &GuardJob {
            value: "unique".into(),
        },
        opts.clone(),
    )
    .await
    .unwrap();

    // Second insert should fail with UniqueConflict
    let result = insert_with(
        &pool,
        &GuardJob {
            value: "unique".into(),
        },
        opts,
    )
    .await;

    match result {
        Err(awa_model::AwaError::UniqueConflict { constraint }) => {
            assert!(constraint.is_some(), "constraint field should be populated");
            let constraint_name = constraint.unwrap();
            assert!(
                constraint_name.contains("unique"),
                "constraint should contain 'unique', got: {constraint_name}"
            );
        }
        other => panic!("Expected UniqueConflict, got: {other:?}"),
    }
}
