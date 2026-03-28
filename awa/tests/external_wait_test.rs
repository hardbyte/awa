//! Integration tests for webhook completion (waiting_external state).
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{admin, admin::DefaultAction, migrations};
use awa::{AwaError, JobArgs, JobContext, JobError, JobResult, JobRow, JobState, Worker};
use awa_testing::TestClient;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup() -> TestClient {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");

    let client = TestClient::from_pool(pool).await;
    client.migrate().await.expect("Failed to run migrations");
    client
}

async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue jobs");
}

// -- Job types for testing --

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct ExternalPayment {
    pub order_id: i64,
}

// -- Worker that correctly uses register_callback + WaitForCallback --

struct ExternalPaymentWorker;

#[async_trait::async_trait]
impl Worker for ExternalPaymentWorker {
    fn kind(&self) -> &'static str {
        "external_payment"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let callback = ctx
            .register_callback(Duration::from_secs(3600))
            .await
            .map_err(JobError::retryable)?;
        Ok(JobResult::WaitForCallback(callback))
    }
}

// -- Internal bridge misuse: WaitForCallback without register_callback --

#[cfg(feature = "__python-bridge")]
struct ForgotCallbackWorker;

#[cfg(feature = "__python-bridge")]
#[async_trait::async_trait]
impl Worker for ForgotCallbackWorker {
    fn kind(&self) -> &'static str {
        "external_payment"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::WaitForCallback(
            awa::CallbackGuard::from_bridge_token(uuid::Uuid::new_v4()),
        ))
    }
}

/// E1: register_callback → WaitForCallback → state = waiting_external, callback_id set
#[tokio::test]
async fn test_e1_happy_path_waiting_external() {
    let client = setup().await;
    let queue = "test_e1_external_wait";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 42 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_waiting_external());

    let updated = client.get_job(job.id).await.unwrap();
    assert_eq!(updated.state, JobState::WaitingExternal);
    assert!(updated.callback_id.is_some());
    assert!(updated.callback_timeout_at.is_some());
    // heartbeat and deadline should be cleared
    assert!(updated.heartbeat_at.is_none());
    assert!(updated.deadline_at.is_none());
}

/// E2: complete_external(callback_id) → completed
#[tokio::test]
async fn test_e2_complete_external() {
    let client = setup().await;
    let queue = "test_e2_complete_external";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 43 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_waiting_external());

    let waiting_job = client.get_job(job.id).await.unwrap();
    let callback_id = waiting_job.callback_id.unwrap();

    let completed = admin::complete_external(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"paid": true})),
        None,
    )
    .await
    .unwrap();
    assert_eq!(completed.state, JobState::Completed);
    assert!(completed.finalized_at.is_some());
    assert!(completed.callback_id.is_none());
    assert!(completed.callback_timeout_at.is_none());
}

/// E3: fail_external(callback_id, error) → failed, error recorded
#[tokio::test]
async fn test_e3_fail_external() {
    let client = setup().await;
    let queue = "test_e3_fail_external";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 44 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    let waiting_job = client.get_job(job.id).await.unwrap();
    let callback_id = waiting_job.callback_id.unwrap();

    let failed = admin::fail_external(client.pool(), callback_id, "payment declined", None)
        .await
        .unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert!(failed.finalized_at.is_some());
    assert!(failed.callback_id.is_none());
    // Check error was recorded
    let errors = failed.errors.unwrap();
    assert!(!errors.is_empty());
    let last_error = errors.last().unwrap();
    assert_eq!(last_error["error"], "payment declined");
}

/// E4: retry_external(callback_id) → available, attempt = 0
#[tokio::test]
async fn test_e4_retry_external() {
    let client = setup().await;
    let queue = "test_e4_retry_external";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 45 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    let waiting_job = client.get_job(job.id).await.unwrap();
    let callback_id = waiting_job.callback_id.unwrap();

    let retried = admin::retry_external(client.pool(), callback_id, None)
        .await
        .unwrap();
    assert_eq!(retried.state, JobState::Available);
    assert_eq!(retried.attempt, 0);
    assert!(retried.callback_id.is_none());
    assert!(retried.callback_timeout_at.is_none());

    // Job should be workable again
    let result = client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_waiting_external());
}

/// E5: Timeout → retryable (with backoff) or failed (exhausted)
#[tokio::test]
async fn test_e5_callback_timeout() {
    let client = setup().await;
    let queue = "test_e5_callback_timeout";
    clean_queue(client.pool(), queue).await;

    // Insert with max_attempts = 2 so we can test both retryable and failed paths
    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 46 },
        awa::InsertOpts {
            queue: queue.to_string(),
            max_attempts: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    // Simulate timeout by setting callback_timeout_at to the past
    sqlx::query(
        "UPDATE awa.jobs SET callback_timeout_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    // Run the rescue query directly (simulating maintenance)
    let rescued: Vec<JobRow> = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = CASE WHEN attempt >= max_attempts THEN 'failed'::awa.job_state ELSE 'retryable'::awa.job_state END,
            finalized_at = now(),
            callback_id = NULL,
            callback_timeout_at = NULL,
            run_at = CASE WHEN attempt >= max_attempts THEN run_at
                     ELSE now() + awa.backoff_duration(attempt, max_attempts) END,
            errors = errors || jsonb_build_object(
                'error', 'callback timed out',
                'attempt', attempt,
                'at', now()
            )::jsonb
        WHERE id IN (
            SELECT id FROM awa.jobs
            WHERE state = 'waiting_external'
              AND callback_timeout_at IS NOT NULL
              AND callback_timeout_at < now()
            LIMIT 500
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        "#,
    )
    .fetch_all(client.pool())
    .await
    .unwrap();

    assert_eq!(rescued.len(), 1);
    // attempt is 1 (from the claim), max_attempts is 2, so should be retryable
    assert_eq!(rescued[0].state, JobState::Retryable);
    assert!(rescued[0].callback_id.is_none());

    // Now test the exhausted case: promote, work again, timeout again
    sqlx::query("UPDATE awa.jobs SET state = 'available', run_at = now() WHERE id = $1")
        .bind(job.id)
        .execute(client.pool())
        .await
        .unwrap();

    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    sqlx::query(
        "UPDATE awa.jobs SET callback_timeout_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    let rescued2: Vec<JobRow> = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = CASE WHEN attempt >= max_attempts THEN 'failed'::awa.job_state ELSE 'retryable'::awa.job_state END,
            finalized_at = now(),
            callback_id = NULL,
            callback_timeout_at = NULL,
            run_at = CASE WHEN attempt >= max_attempts THEN run_at
                     ELSE now() + awa.backoff_duration(attempt, max_attempts) END,
            errors = errors || jsonb_build_object(
                'error', 'callback timed out',
                'attempt', attempt,
                'at', now()
            )::jsonb
        WHERE id IN (
            SELECT id FROM awa.jobs
            WHERE state = 'waiting_external'
              AND callback_timeout_at IS NOT NULL
              AND callback_timeout_at < now()
            LIMIT 500
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        "#,
    )
    .fetch_all(client.pool())
    .await
    .unwrap();

    assert_eq!(rescued2.len(), 1);
    // attempt is 2, max_attempts is 2, so should be failed
    assert_eq!(rescued2[0].state, JobState::Failed);
}

/// E6: Double completion → CallbackNotFound on second call
#[tokio::test]
async fn test_e6_double_completion() {
    let client = setup().await;
    let queue = "test_e6_double_completion";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 47 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    let waiting_job = client.get_job(job.id).await.unwrap();
    let callback_id = waiting_job.callback_id.unwrap();

    // First completion succeeds
    admin::complete_external(client.pool(), callback_id, None, None)
        .await
        .unwrap();

    // Second completion fails with CallbackNotFound
    let err = admin::complete_external(client.pool(), callback_id, None, None)
        .await
        .unwrap_err();
    match err {
        AwaError::CallbackNotFound { .. } => {}
        other => panic!("Expected CallbackNotFound, got: {other:?}"),
    }
}

/// E7: Wrong callback_id → CallbackNotFound
#[tokio::test]
async fn test_e7_wrong_callback_id() {
    let client = setup().await;
    let fake_id = uuid::Uuid::new_v4();

    let err = admin::complete_external(client.pool(), fake_id, None, None)
        .await
        .unwrap_err();
    match err {
        AwaError::CallbackNotFound { .. } => {}
        other => panic!("Expected CallbackNotFound, got: {other:?}"),
    }
}

/// E8: Admin cancel while waiting_external → cancelled
#[tokio::test]
async fn test_e8_admin_cancel_waiting() {
    let client = setup().await;
    let queue = "test_e8_admin_cancel";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 48 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    let cancelled = admin::cancel(client.pool(), job.id).await.unwrap().unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert!(cancelled.callback_id.is_none());
}

/// E9: Admin retry while waiting_external → available
#[tokio::test]
async fn test_e9_admin_retry_waiting() {
    let client = setup().await;
    let queue = "test_e9_admin_retry";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 49 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    let retried = admin::retry(client.pool(), job.id).await.unwrap().unwrap();
    assert_eq!(retried.state, JobState::Available);
    assert_eq!(retried.attempt, 0);
    assert!(retried.callback_id.is_none());
}

/// E10: Drain queue includes waiting_external → cancelled
#[tokio::test]
async fn test_e10_drain_queue_includes_waiting() {
    let client = setup().await;
    let queue = "test_e10_drain_queue";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 50 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    let drained = admin::drain_queue(client.pool(), queue).await.unwrap();
    assert_eq!(drained, 1);

    let updated = client.get_job(job.id).await.unwrap();
    assert_eq!(updated.state, JobState::Cancelled);
}

/// E11: Race: complete_external during running (before WaitForCallback) → completed
#[tokio::test]
async fn test_e11_race_complete_during_running() {
    let client = setup().await;
    let queue = "test_e11_race";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 51 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Manually claim the job (transition to running)
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, heartbeat_at = now() WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    // Simulate phase 1: register_callback (writes callback_id while still running)
    let callback_id = uuid::Uuid::new_v4();
    sqlx::query(
        "UPDATE awa.jobs SET callback_id = $2, callback_timeout_at = now() + interval '1 hour' WHERE id = $1",
    )
    .bind(job.id)
    .bind(callback_id)
    .execute(client.pool())
    .await
    .unwrap();

    // Racing: external system completes BEFORE the executor transitions to waiting_external
    // The job is still in 'running' state
    let completed = admin::complete_external(client.pool(), callback_id, None, None)
        .await
        .unwrap();
    assert_eq!(completed.state, JobState::Completed);
}

/// E12: Crash between register_callback and WaitForCallback → rescued, stale callback_id cleared
#[tokio::test]
async fn test_e12_crash_clears_stale_callback() {
    let client = setup().await;
    let queue = "test_e12_crash";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 52 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Manually set to running with a callback_id and a stale heartbeat
    let callback_id = uuid::Uuid::new_v4();
    sqlx::query(
        r#"UPDATE awa.jobs SET
            state = 'running',
            attempt = 1,
            heartbeat_at = now() - interval '5 minutes',
            callback_id = $2,
            callback_timeout_at = now() + interval '1 hour'
        WHERE id = $1"#,
    )
    .bind(job.id)
    .bind(callback_id)
    .execute(client.pool())
    .await
    .unwrap();

    // Run heartbeat rescue (simulating maintenance)
    let rescued: Vec<JobRow> = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = 'retryable',
            finalized_at = now(),
            heartbeat_at = NULL,
            deadline_at = NULL,
            callback_id = NULL,
            callback_timeout_at = NULL,
            errors = errors || jsonb_build_object(
                'error', 'heartbeat stale: worker presumed dead',
                'attempt', attempt,
                'at', now()
            )::jsonb
        WHERE id IN (
            SELECT id FROM awa.jobs
            WHERE id = $1
              AND state = 'running'
              AND heartbeat_at < now() - interval '90 seconds'
            LIMIT 500
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        "#,
    )
    .bind(job.id)
    .fetch_all(client.pool())
    .await
    .unwrap();

    assert_eq!(rescued.len(), 1);
    assert_eq!(rescued[0].state, JobState::Retryable);
    assert!(rescued[0].callback_id.is_none());
    assert!(rescued[0].callback_timeout_at.is_none());

    // Now the stale callback_id should not be found
    let err = admin::complete_external(client.pool(), callback_id, None, None)
        .await
        .unwrap_err();
    match err {
        AwaError::CallbackNotFound { .. } => {}
        other => panic!("Expected CallbackNotFound, got: {other:?}"),
    }
}

/// E13: Uniqueness enforced during waiting_external
#[tokio::test]
async fn test_e13_uniqueness_during_waiting_external() {
    let client = setup().await;
    let queue = "test_e13_unique";
    clean_queue(client.pool(), queue).await;

    // Build a PG bit string the same way the insert code does: iterate from bit 0
    // to bit 7, placing each at the leftmost-first PG position.
    // We want bits 0-4 (scheduled, available, running, completed, retryable) + bit 7 (waiting_external)
    let unique_states: u8 = 0b1001_1111; // bits 0,1,2,3,4,7
    let mut bit_string = String::with_capacity(8);
    for bit_position in 0..8u8 {
        if unique_states & (1 << bit_position) != 0 {
            bit_string.push('1');
        } else {
            bit_string.push('0');
        }
    }

    let job = sqlx::query_as::<_, JobRow>(
        r#"
        INSERT INTO awa.jobs (kind, queue, args, unique_key, unique_states)
        VALUES ('external_payment', $1, '{"order_id": 53}', E'\\xDEADBEEF', $2::bit(8))
        RETURNING *
        "#,
    )
    .bind(queue)
    .bind(&bit_string)
    .fetch_one(client.pool())
    .await
    .unwrap();

    // Work the job to get it to waiting_external
    let result = client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_waiting_external());

    // Trying to insert a duplicate should fail (unique violation)
    let err = sqlx::query_as::<_, JobRow>(
        r#"
        INSERT INTO awa.jobs (kind, queue, args, unique_key, unique_states)
        VALUES ('external_payment', $1, '{"order_id": 53}', E'\\xDEADBEEF', $2::bit(8))
        RETURNING *
        "#,
    )
    .bind(queue)
    .bind(&bit_string)
    .fetch_one(client.pool())
    .await;

    assert!(
        err.is_err(),
        "Expected unique violation but insert succeeded"
    );

    // Complete the job — uniqueness should still hold since completed is in bitmask
    let waiting_job = client.get_job(job.id).await.unwrap();
    admin::complete_external(client.pool(), waiting_job.callback_id.unwrap(), None, None)
        .await
        .unwrap();

    let updated = client.get_job(job.id).await.unwrap();
    assert_eq!(updated.state, JobState::Completed);
}

/// E14: V3 migration from v2
#[tokio::test]
async fn test_e14_migration() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");

    // Simply verify migration succeeds (idempotent)
    migrations::run(&pool).await.unwrap();
    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);
}

// ── E15: resolve_callback accepts running state (race condition fix) ──

/// E15: A fast callback arriving before the executor transitions running ->
/// waiting_external should now be accepted by resolve_callback.
#[tokio::test]
async fn test_e15_resolve_callback_during_running_state() {
    let client = setup().await;
    let queue = "test_e15_resolve_running";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 60 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Manually set to running with callback_id (simulating register done,
    // executor hasn't transitioned to waiting_external yet)
    let callback_id = uuid::Uuid::new_v4();
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, run_lease = run_lease + 1,
         heartbeat_at = now(), callback_id = $2, callback_timeout_at = now() + interval '1 hour'
         WHERE id = $1",
    )
    .bind(job.id)
    .bind(callback_id)
    .execute(client.pool())
    .await
    .unwrap();

    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        None,
        DefaultAction::Complete,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_completed());
    let completed = client.get_job(job.id).await.unwrap();
    assert_eq!(completed.state, JobState::Completed);
}

// ── E16: Stale callback rejected by run_lease guard ──

/// E16: A callback carrying an old run_lease should be rejected when the job
/// has been re-claimed with a new lease.
#[tokio::test]
async fn test_e16_stale_callback_rejected_by_run_lease() {
    let client = setup().await;
    let queue = "test_e16_run_lease";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 61 },
        awa::InsertOpts {
            queue: queue.to_string(),
            max_attempts: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Work the job to get it to waiting_external
    let result = client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_waiting_external());

    let waiting = client.get_job(job.id).await.unwrap();
    let _old_callback_id = waiting.callback_id.unwrap();
    let old_lease = waiting.run_lease;

    // Simulate rescue: move job back to available, clearing callback
    sqlx::query(
        "UPDATE awa.jobs SET state = 'available', callback_id = NULL,
         callback_timeout_at = NULL, callback_filter = NULL,
         callback_on_complete = NULL, callback_on_fail = NULL,
         callback_transform = NULL, attempt = 0, run_at = now()
         WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    // Re-claim with new lease (work again)
    let result2 = client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();
    assert!(result2.is_waiting_external());

    let reclaimed = client.get_job(job.id).await.unwrap();
    let new_callback_id = reclaimed.callback_id.unwrap();
    let new_lease = reclaimed.run_lease;
    assert_ne!(
        old_lease, new_lease,
        "run_lease should increment on re-claim"
    );

    // Try to complete the NEW callback with the OLD run_lease — should fail
    let err = admin::complete_external(client.pool(), new_callback_id, None, Some(old_lease))
        .await
        .unwrap_err();
    match err {
        AwaError::CallbackNotFound { .. } => {}
        other => panic!("Expected CallbackNotFound, got: {other:?}"),
    }

    // Complete with correct lease should succeed
    let completed = admin::complete_external(client.pool(), new_callback_id, None, Some(new_lease))
        .await
        .unwrap();
    assert_eq!(completed.state, JobState::Completed);
}

// ── E17: cancel_callback clears fields ──

/// E17: cancel_callback with matching lease NULLs all callback fields.
#[tokio::test]
async fn test_e17_cancel_callback_clears_fields() {
    let client = setup().await;
    let queue = "test_e17_cancel_callback";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 62 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Manually claim and register callback (staying in running state)
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, run_lease = run_lease + 1,
         heartbeat_at = now() WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    let running = client.get_job(job.id).await.unwrap();
    let run_lease = running.run_lease;

    let _callback_id = admin::register_callback(
        client.pool(),
        job.id,
        run_lease,
        std::time::Duration::from_secs(3600),
    )
    .await
    .unwrap();

    // Verify callback fields are set
    let with_cb = client.get_job(job.id).await.unwrap();
    assert!(with_cb.callback_id.is_some());
    assert!(with_cb.callback_timeout_at.is_some());

    // Cancel callback
    let cancelled = admin::cancel_callback(client.pool(), job.id, run_lease)
        .await
        .unwrap();
    assert!(cancelled, "cancel_callback should return true");

    // Verify all callback fields are cleared
    let after = client.get_job(job.id).await.unwrap();
    assert!(after.callback_id.is_none());
    assert!(after.callback_timeout_at.is_none());
    // Job is still running
    assert_eq!(after.state, JobState::Running);
}

// ── E18: cancel_callback with wrong lease is a no-op ──

/// E18: cancel_callback with a mismatched run_lease should not clear anything.
#[tokio::test]
async fn test_e18_cancel_callback_wrong_lease_noop() {
    let client = setup().await;
    let queue = "test_e18_cancel_noop";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 63 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Manually claim and register callback
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, run_lease = run_lease + 1,
         heartbeat_at = now() WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    let running = client.get_job(job.id).await.unwrap();
    let run_lease = running.run_lease;

    admin::register_callback(
        client.pool(),
        job.id,
        run_lease,
        std::time::Duration::from_secs(3600),
    )
    .await
    .unwrap();

    // Cancel with wrong lease
    let result = admin::cancel_callback(client.pool(), job.id, run_lease + 999)
        .await
        .unwrap();
    assert!(
        !result,
        "cancel_callback with wrong lease should return false"
    );

    // Callback fields should still be set
    let after = client.get_job(job.id).await.unwrap();
    assert!(after.callback_id.is_some());
    assert!(after.callback_timeout_at.is_some());
}

/// Internal bridge misuse should still fail the job descriptively at runtime.
#[cfg(feature = "__python-bridge")]
#[tokio::test]
async fn test_wait_for_callback_without_register() {
    let client = setup().await;
    let queue = "test_wait_no_register";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 99 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = client
        .work_one_in_queue(&ForgotCallbackWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_failed());

    let failed = client.get_job(job.id).await.unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert!(failed.finalized_at.is_some());
}

// ── Heartbeat callback ─────────────────────────────────────────

#[tokio::test]
async fn test_heartbeat_callback_resets_timeout() {
    let client = setup().await;
    let queue = "test_heartbeat_callback";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 200 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    // Verify job is waiting
    let waiting = client.get_job(job.id).await.unwrap();
    assert_eq!(waiting.state, JobState::WaitingExternal);
    let original_timeout = waiting.callback_timeout_at.expect("should have timeout");
    let callback_id = waiting.callback_id.expect("should have callback_id");

    // Heartbeat with a 2-hour timeout
    let updated = admin::heartbeat_callback(
        client.pool(),
        callback_id,
        std::time::Duration::from_secs(7200),
    )
    .await
    .unwrap();

    // Timeout should be extended
    assert_eq!(updated.state, JobState::WaitingExternal);
    let new_timeout = updated
        .callback_timeout_at
        .expect("should still have timeout");
    assert!(
        new_timeout > original_timeout,
        "Heartbeat should extend timeout: old={original_timeout}, new={new_timeout}"
    );
}

#[tokio::test]
async fn test_heartbeat_callback_not_found_for_completed_job() {
    let client = setup().await;
    let queue = "test_heartbeat_not_found";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 201 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    let waiting = client.get_job(job.id).await.unwrap();
    let callback_id = waiting.callback_id.unwrap();

    // Complete the job first
    admin::complete_external(client.pool(), callback_id, None, None)
        .await
        .unwrap();

    // Heartbeat on a completed job should fail
    let result = admin::heartbeat_callback(
        client.pool(),
        callback_id,
        std::time::Duration::from_secs(3600),
    )
    .await;

    assert!(
        matches!(result, Err(awa::AwaError::CallbackNotFound { .. })),
        "Heartbeat on completed job should return CallbackNotFound, got: {result:?}"
    );
}
