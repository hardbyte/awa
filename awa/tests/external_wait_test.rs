//! Integration tests for webhook completion (waiting_external state).
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{admin, admin::DefaultAction, migrations};
use awa::{AwaError, JobArgs, JobContext, JobError, JobResult, JobRow, JobState, Worker};
use awa_testing::TestClient;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
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

// ── Sequential callbacks (resume from wait) ─────────────────────

#[tokio::test]
async fn test_sequential_callback_resume() {
    let client = setup().await;
    let queue = "test_sequential_resume";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 300 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Claim and start execution
    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    // Job should be waiting for callback
    let waiting = client.get_job(job.id).await.unwrap();
    assert_eq!(waiting.state, JobState::WaitingExternal);
    let callback_id = waiting.callback_id.unwrap();

    // Resume with payload (not complete — the handler would continue)
    let resumed = admin::resume_external(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"payment_id": "pay_123", "amount": 42.50})),
        None,
    )
    .await
    .unwrap();

    assert_eq!(resumed.state, JobState::Running);

    // The payload should be stored in metadata
    let result = resumed.metadata.get("_awa_callback_result");
    assert!(result.is_some(), "Resume payload should be in metadata");
    assert_eq!(
        result.unwrap().get("payment_id").and_then(|v| v.as_str()),
        Some("pay_123")
    );

    // Heartbeat should be refreshed
    assert!(
        resumed.heartbeat_at.is_some(),
        "Heartbeat should be refreshed on resume"
    );
}

#[tokio::test]
async fn test_resume_then_complete() {
    let client = setup().await;
    let queue = "test_resume_complete";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 301 },
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
    let cb1 = waiting.callback_id.unwrap();

    // First callback: resume
    admin::resume_external(
        client.pool(),
        cb1,
        Some(serde_json::json!({"step": 1})),
        None,
    )
    .await
    .unwrap();

    // Job is now running — register a second callback (use current run_lease)
    let resumed_job = client.get_job(job.id).await.unwrap();
    let cb2 = admin::register_callback(
        client.pool(),
        job.id,
        resumed_job.run_lease,
        std::time::Duration::from_secs(3600),
    )
    .await
    .unwrap();

    // Transition to waiting_external for second wait
    sqlx::query(
        "UPDATE awa.jobs SET state = 'waiting_external', heartbeat_at = NULL WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    // Second callback: complete (not resume)
    let completed = admin::complete_external(
        client.pool(),
        cb2,
        Some(serde_json::json!({"step": 2, "final": true})),
        None,
    )
    .await
    .unwrap();

    assert_eq!(completed.state, JobState::Completed);
}

#[tokio::test]
async fn test_resume_not_found_after_complete() {
    let client = setup().await;
    let queue = "test_resume_not_found";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 302 },
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

    // Complete normally first
    admin::complete_external(client.pool(), callback_id, None, None)
        .await
        .unwrap();

    // Resume on a completed job should fail
    let result = admin::resume_external(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"too": "late"})),
        None,
    )
    .await;

    assert!(
        matches!(result, Err(awa::AwaError::CallbackNotFound { .. })),
        "Resume after complete should fail: {result:?}"
    );
}

// ── Sequential callbacks: comprehensive edge cases ────────────────

/// Worker that uses ctx.wait_for_callback() to suspend in-handler.
/// Sends the callback_id through a channel so the test can resume it.
struct SequentialWaitWorker {
    callback_id_tx: tokio::sync::mpsc::Sender<uuid::Uuid>,
    result_tx: tokio::sync::mpsc::Sender<Result<serde_json::Value, AwaError>>,
}

#[async_trait::async_trait]
impl Worker for SequentialWaitWorker {
    fn kind(&self) -> &'static str {
        "external_payment"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let guard = ctx
            .register_callback(Duration::from_secs(3600))
            .await
            .map_err(JobError::retryable)?;

        let callback_id = guard.id();
        let _ = self.callback_id_tx.send(callback_id).await;

        let payload = ctx.wait_for_callback(guard).await;
        let _ = self.result_tx.send(payload).await;

        Ok(JobResult::Completed)
    }
}

/// E19: Full wait_for_callback flow — handler suspends, resume_external wakes it.
#[tokio::test]
async fn test_e19_wait_for_callback_happy_path() {
    let client = setup().await;
    let queue = "test_e19_wait_happy";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 400 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let (cb_tx, mut cb_rx) = tokio::sync::mpsc::channel(1);
    let (result_tx, mut result_rx) = tokio::sync::mpsc::channel(1);

    // Manually claim the job
    let claimed: JobRow = sqlx::query_as(
        r#"
        WITH claimed AS (
            SELECT id FROM awa.jobs
            WHERE state = 'available' AND kind = 'external_payment' AND queue = $1
            LIMIT 1 FOR UPDATE SKIP LOCKED
        )
        UPDATE awa.jobs
        SET state = 'running', attempt = attempt + 1, run_lease = run_lease + 1,
            attempted_at = now(), heartbeat_at = now(), deadline_at = now() + interval '5 minutes'
        FROM claimed WHERE awa.jobs.id = claimed.id
        RETURNING awa.jobs.*
        "#,
    )
    .bind(queue)
    .fetch_one(client.pool())
    .await
    .unwrap();

    let pool = client.pool().clone();
    let worker = SequentialWaitWorker {
        callback_id_tx: cb_tx,
        result_tx,
    };

    // Spawn the handler in a background task
    let handler_task = tokio::spawn(async move {
        let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let state: Arc<
            std::collections::HashMap<std::any::TypeId, Box<dyn std::any::Any + Send + Sync>>,
        > = Arc::new(std::collections::HashMap::new());
        let progress = Arc::new(std::sync::Mutex::new(
            awa_worker::context::ProgressState::new(claimed.progress.clone()),
        ));
        let ctx = JobContext::new(claimed, cancel, state, pool, progress);
        worker.perform(&ctx).await
    });

    // Wait for the handler to register the callback and enter wait
    let callback_id = tokio::time::timeout(Duration::from_secs(5), cb_rx.recv())
        .await
        .expect("timeout waiting for callback_id")
        .expect("channel closed");

    // Give the handler time to transition to waiting_external
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify job is in waiting_external
    let waiting = client.get_job(job.id).await.unwrap();
    assert_eq!(waiting.state, JobState::WaitingExternal);

    // Resume with payload
    admin::resume_external(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"answer": 42})),
        None,
    )
    .await
    .unwrap();

    // Handler should receive the payload and complete
    let payload = tokio::time::timeout(Duration::from_secs(5), result_rx.recv())
        .await
        .expect("timeout waiting for result")
        .expect("channel closed");

    let payload = payload.expect("wait_for_callback should succeed");
    assert_eq!(payload["answer"], 42);

    // Handler task should complete
    let result = tokio::time::timeout(Duration::from_secs(2), handler_task)
        .await
        .expect("handler should finish")
        .expect("handler panicked");
    assert!(matches!(result, Ok(JobResult::Completed)));
}

/// E20: Two sequential callbacks via admin API (no runtime, direct DB).
/// Tests: resume cb1 → register cb2 → enter waiting → complete cb2.
#[tokio::test]
async fn test_e20_two_sequential_callbacks_admin_api() {
    let client = setup().await;
    let queue = "test_e20_two_seq";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 401 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Phase 1: Claim, register cb1, enter waiting
    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    let waiting1 = client.get_job(job.id).await.unwrap();
    assert_eq!(waiting1.state, JobState::WaitingExternal);
    let cb1 = waiting1.callback_id.unwrap();
    let run_lease = waiting1.run_lease;

    // Phase 2: Resume cb1 — job goes back to running
    let resumed = admin::resume_external(
        client.pool(),
        cb1,
        Some(serde_json::json!({"step": "payment_confirmed"})),
        None,
    )
    .await
    .unwrap();
    assert_eq!(resumed.state, JobState::Running);
    assert!(resumed.callback_id.is_none(), "cb1 should be cleared");
    assert!(resumed.heartbeat_at.is_some(), "heartbeat refreshed");
    // run_lease stays the same (same execution)
    assert_eq!(resumed.run_lease, run_lease);

    // Phase 3: Register cb2 (simulating handler continuing)
    let cb2 = admin::register_callback(client.pool(), job.id, run_lease, Duration::from_secs(7200))
        .await
        .unwrap();

    // Phase 4: Enter waiting again
    sqlx::query(
        "UPDATE awa.jobs SET state = 'waiting_external', heartbeat_at = NULL, deadline_at = NULL WHERE id = $1 AND state = 'running' AND run_lease = $2 AND callback_id IS NOT NULL",
    )
    .bind(job.id)
    .bind(run_lease)
    .execute(client.pool())
    .await
    .unwrap();

    let waiting2 = client.get_job(job.id).await.unwrap();
    assert_eq!(waiting2.state, JobState::WaitingExternal);
    assert_eq!(waiting2.callback_id.unwrap(), cb2);

    // Phase 5: Complete cb2 — terminal
    let completed = admin::complete_external(
        client.pool(),
        cb2,
        Some(serde_json::json!({"step": "shipping_done"})),
        None,
    )
    .await
    .unwrap();
    assert_eq!(completed.state, JobState::Completed);
    assert!(completed.callback_id.is_none());

    // Old cb1 should not be usable
    let err = admin::resume_external(client.pool(), cb1, None, None).await;
    assert!(matches!(err, Err(AwaError::CallbackNotFound { .. })));
}

/// E21: Timeout during second callback wait.
/// Resume from first callback, register second, timeout fires on second.
#[tokio::test]
async fn test_e21_timeout_during_second_callback() {
    let client = setup().await;
    let queue = "test_e21_timeout_second";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 402 },
        awa::InsertOpts {
            queue: queue.to_string(),
            max_attempts: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Claim → register → wait → resume
    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    let waiting1 = client.get_job(job.id).await.unwrap();
    let cb1 = waiting1.callback_id.unwrap();

    admin::resume_external(
        client.pool(),
        cb1,
        Some(serde_json::json!({"ok": true})),
        None,
    )
    .await
    .unwrap();

    // Register second callback, enter waiting
    let resumed = client.get_job(job.id).await.unwrap();
    let cb2 = admin::register_callback(
        client.pool(),
        job.id,
        resumed.run_lease,
        Duration::from_secs(3600),
    )
    .await
    .unwrap();

    sqlx::query(
        "UPDATE awa.jobs SET state = 'waiting_external', heartbeat_at = NULL, deadline_at = NULL WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    // Simulate timeout on second callback
    sqlx::query(
        "UPDATE awa.jobs SET callback_timeout_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    // Run rescue
    let rescued: Vec<JobRow> = sqlx::query_as::<_, JobRow>(
        r#"
        UPDATE awa.jobs
        SET state = CASE WHEN attempt >= max_attempts THEN 'failed'::awa.job_state ELSE 'retryable'::awa.job_state END,
            finalized_at = now(),
            callback_id = NULL,
            callback_timeout_at = NULL,
            errors = errors || jsonb_build_object('error', 'callback timed out', 'attempt', attempt, 'at', now())::jsonb
        WHERE id IN (
            SELECT id FROM awa.jobs
            WHERE state = 'waiting_external' AND callback_timeout_at IS NOT NULL AND callback_timeout_at < now()
            LIMIT 500 FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        "#,
    )
    .fetch_all(client.pool())
    .await
    .unwrap();

    assert_eq!(rescued.len(), 1);
    // attempt=1, max_attempts=3, so retryable
    assert_eq!(rescued[0].state, JobState::Retryable);
    assert!(rescued[0].callback_id.is_none());

    // cb2 should now be unfindable
    let err = admin::complete_external(client.pool(), cb2, None, None).await;
    assert!(matches!(err, Err(AwaError::CallbackNotFound { .. })));
}

/// E22: Heartbeat keeps extending timeout during sequential wait.
#[tokio::test]
async fn test_e22_heartbeat_during_sequential_wait() {
    let client = setup().await;
    let queue = "test_e22_hb_seq";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 403 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Claim → register → wait → resume → register cb2 → wait
    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    let waiting = client.get_job(job.id).await.unwrap();
    let cb1 = waiting.callback_id.unwrap();

    admin::resume_external(
        client.pool(),
        cb1,
        Some(serde_json::json!({"phase": 1})),
        None,
    )
    .await
    .unwrap();

    let resumed = client.get_job(job.id).await.unwrap();
    let cb2 = admin::register_callback(
        client.pool(),
        job.id,
        resumed.run_lease,
        Duration::from_secs(10),
    )
    .await
    .unwrap();

    sqlx::query(
        "UPDATE awa.jobs SET state = 'waiting_external', heartbeat_at = NULL, deadline_at = NULL WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    let before_hb = client.get_job(job.id).await.unwrap();
    let timeout_before = before_hb.callback_timeout_at.unwrap();

    // Heartbeat extends cb2 timeout
    let after_hb = admin::heartbeat_callback(client.pool(), cb2, Duration::from_secs(7200))
        .await
        .unwrap();
    let timeout_after = after_hb.callback_timeout_at.unwrap();
    assert!(
        timeout_after > timeout_before,
        "Heartbeat should extend second callback timeout"
    );

    // Now complete cb2
    let completed = admin::complete_external(client.pool(), cb2, None, None)
        .await
        .unwrap();
    assert_eq!(completed.state, JobState::Completed);
}

/// E23: Concurrent resume attempts — only one succeeds.
#[tokio::test]
async fn test_e23_concurrent_resume_attempts() {
    let client = setup().await;
    let queue = "test_e23_concurrent_resume";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 404 },
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

    // Spawn two concurrent resume attempts
    let pool1 = client.pool().clone();
    let pool2 = client.pool().clone();

    let (r1, r2) = tokio::join!(
        admin::resume_external(
            &pool1,
            callback_id,
            Some(serde_json::json!({"from": "task1"})),
            None,
        ),
        admin::resume_external(
            &pool2,
            callback_id,
            Some(serde_json::json!({"from": "task2"})),
            None,
        ),
    );

    // Exactly one should succeed, one should get CallbackNotFound
    let successes = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    let failures = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, Err(AwaError::CallbackNotFound { .. })))
        .count();

    assert_eq!(successes, 1, "Exactly one resume should succeed");
    assert_eq!(failures, 1, "Exactly one resume should fail");
}

/// E24: Resume with wrong run_lease is rejected.
#[tokio::test]
async fn test_e24_resume_wrong_lease() {
    let client = setup().await;
    let queue = "test_e24_resume_lease";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 405 },
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
    let correct_lease = waiting.run_lease;

    // Resume with wrong lease
    let err = admin::resume_external(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"data": "test"})),
        Some(correct_lease + 999),
    )
    .await;

    assert!(
        matches!(err, Err(AwaError::CallbackNotFound { .. })),
        "Resume with wrong lease should fail: {err:?}"
    );

    // Resume with correct lease should work
    let ok = admin::resume_external(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"data": "test"})),
        Some(correct_lease),
    )
    .await;
    assert!(ok.is_ok(), "Resume with correct lease should succeed");
}

/// E25: Crash/rescue during sequential flow — worker dies after first resume.
/// Heartbeat rescue should catch the stale running job.
#[tokio::test]
async fn test_e25_crash_after_resume() {
    let client = setup().await;
    let queue = "test_e25_crash_resume";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 406 },
        awa::InsertOpts {
            queue: queue.to_string(),
            max_attempts: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Claim → register → wait → resume
    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();

    let waiting = client.get_job(job.id).await.unwrap();
    let cb1 = waiting.callback_id.unwrap();

    admin::resume_external(
        client.pool(),
        cb1,
        Some(serde_json::json!({"ok": true})),
        None,
    )
    .await
    .unwrap();

    // Job is now running after resume. Simulate worker crash:
    // set heartbeat to the past
    sqlx::query("UPDATE awa.jobs SET heartbeat_at = now() - interval '5 minutes' WHERE id = $1")
        .bind(job.id)
        .execute(client.pool())
        .await
        .unwrap();

    // Run heartbeat rescue
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
            WHERE id = $1 AND state = 'running'
              AND heartbeat_at < now() - interval '90 seconds'
            LIMIT 500 FOR UPDATE SKIP LOCKED
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

    // Job can be re-claimed and go through the whole cycle again
    sqlx::query("UPDATE awa.jobs SET state = 'available', run_at = now() WHERE id = $1")
        .bind(job.id)
        .execute(client.pool())
        .await
        .unwrap();

    let result = client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_waiting_external());
}

/// E26: Admin cancel during resumed running state.
/// Handler has been resumed (running), admin cancels before second register.
#[tokio::test]
async fn test_e26_admin_cancel_after_resume() {
    let client = setup().await;
    let queue = "test_e26_cancel_resume";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 407 },
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
    let cb1 = waiting.callback_id.unwrap();

    // Resume first callback
    admin::resume_external(
        client.pool(),
        cb1,
        Some(serde_json::json!({"ok": true})),
        None,
    )
    .await
    .unwrap();

    let running = client.get_job(job.id).await.unwrap();
    assert_eq!(running.state, JobState::Running);

    // Admin cancels while handler is resumed/running
    let cancelled = admin::cancel(client.pool(), job.id).await.unwrap().unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert!(cancelled.callback_id.is_none());
}

/// E27: Resume preserves existing metadata (doesn't overwrite user fields).
#[tokio::test]
async fn test_e27_resume_preserves_metadata() {
    let client = setup().await;
    let queue = "test_e27_metadata";
    clean_queue(client.pool(), queue).await;

    // Insert with custom metadata
    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 408 },
        awa::InsertOpts {
            queue: queue.to_string(),
            metadata: serde_json::json!({"customer": "acme", "priority": "high"}),
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
    let cb = waiting.callback_id.unwrap();

    // Resume — should merge _awa_callback_result into metadata
    let resumed = admin::resume_external(
        client.pool(),
        cb,
        Some(serde_json::json!({"payment_id": "pay_xyz"})),
        None,
    )
    .await
    .unwrap();

    // Original fields preserved
    assert_eq!(resumed.metadata["customer"], "acme");
    assert_eq!(resumed.metadata["priority"], "high");
    // Callback result added
    assert_eq!(
        resumed.metadata["_awa_callback_result"]["payment_id"],
        "pay_xyz"
    );
}

/// E28: Resume with null payload stores null in metadata.
#[tokio::test]
async fn test_e28_resume_null_payload() {
    let client = setup().await;
    let queue = "test_e28_null_payload";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 409 },
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
    let cb = waiting.callback_id.unwrap();

    // Resume with no payload
    let resumed = admin::resume_external(client.pool(), cb, None, None)
        .await
        .unwrap();

    assert_eq!(resumed.state, JobState::Running);
    // _awa_callback_result should be null
    let result = &resumed.metadata["_awa_callback_result"];
    assert!(result.is_null(), "Null payload should store null: {result}");
}

/// E29: fail_external after resume — first callback resumed, second
/// callback resolved with fail_external.
#[tokio::test]
async fn test_e29_fail_after_resume() {
    let client = setup().await;
    let queue = "test_e29_fail_after_resume";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 410 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // cb1: register → wait → resume
    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();
    let w1 = client.get_job(job.id).await.unwrap();
    let cb1 = w1.callback_id.unwrap();

    admin::resume_external(
        client.pool(),
        cb1,
        Some(serde_json::json!({"step": 1})),
        None,
    )
    .await
    .unwrap();

    // cb2: register → wait → fail
    let resumed = client.get_job(job.id).await.unwrap();
    let cb2 = admin::register_callback(
        client.pool(),
        job.id,
        resumed.run_lease,
        Duration::from_secs(3600),
    )
    .await
    .unwrap();

    sqlx::query(
        "UPDATE awa.jobs SET state = 'waiting_external', heartbeat_at = NULL, deadline_at = NULL WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    // Fail the second callback
    let failed = admin::fail_external(client.pool(), cb2, "shipping provider unavailable", None)
        .await
        .unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert!(failed.callback_id.is_none());

    let errors = failed.errors.unwrap();
    let last = errors.last().unwrap();
    assert_eq!(last["error"], "shipping provider unavailable");
}

/// E30: resolve_callback with CEL on resumed job's second callback.
#[tokio::test]
async fn test_e30_resolve_callback_on_second_wait() {
    let client = setup().await;
    let queue = "test_e30_resolve_second";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 411 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // cb1: register → wait → resume
    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();
    let w1 = client.get_job(job.id).await.unwrap();
    admin::resume_external(
        client.pool(),
        w1.callback_id.unwrap(),
        Some(serde_json::json!({"step": 1})),
        None,
    )
    .await
    .unwrap();

    // cb2: register → wait
    let resumed = client.get_job(job.id).await.unwrap();
    let cb2 = admin::register_callback(
        client.pool(),
        job.id,
        resumed.run_lease,
        Duration::from_secs(3600),
    )
    .await
    .unwrap();

    sqlx::query(
        "UPDATE awa.jobs SET state = 'waiting_external', heartbeat_at = NULL, deadline_at = NULL WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    // resolve_callback with default_action=Complete
    let result = admin::resolve_callback(
        client.pool(),
        cb2,
        Some(serde_json::json!({"approved": true})),
        DefaultAction::Complete,
        None,
    )
    .await
    .unwrap();

    assert!(
        result.is_completed(),
        "resolve_callback should complete: {result:?}"
    );
    let final_job = client.get_job(job.id).await.unwrap();
    assert_eq!(final_job.state, JobState::Completed);
}

/// E31: retry_external on second callback — resets the entire job.
#[tokio::test]
async fn test_e31_retry_external_on_second_callback() {
    let client = setup().await;
    let queue = "test_e31_retry_second";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &ExternalPayment { order_id: 412 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // cb1: register → wait → resume
    client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();
    let w1 = client.get_job(job.id).await.unwrap();
    admin::resume_external(
        client.pool(),
        w1.callback_id.unwrap(),
        Some(serde_json::json!({"step": 1})),
        None,
    )
    .await
    .unwrap();

    // cb2: register → wait
    let resumed = client.get_job(job.id).await.unwrap();
    let cb2 = admin::register_callback(
        client.pool(),
        job.id,
        resumed.run_lease,
        Duration::from_secs(3600),
    )
    .await
    .unwrap();

    sqlx::query(
        "UPDATE awa.jobs SET state = 'waiting_external', heartbeat_at = NULL, deadline_at = NULL WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    // retry_external on cb2 — resets job to available
    let retried = admin::retry_external(client.pool(), cb2, None)
        .await
        .unwrap();
    assert_eq!(retried.state, JobState::Available);
    assert_eq!(retried.attempt, 0);
    assert!(retried.callback_id.is_none());

    // Job can be worked again
    let result = client
        .work_one_in_queue(&ExternalPaymentWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_waiting_external());
}
