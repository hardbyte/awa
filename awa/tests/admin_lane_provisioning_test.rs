//! Admin calls on an existing lane must not wait on its provisioning lock.
//!
//! Module-level admin functions build a new `QueueStorage` per call. Each test
//! provisions a lane, holds that lane's provisioning lock
//! (`{schema}:{queue}:{priority}:{shard}`) in a gate transaction, and requires
//! the admin call that revives a job onto the lane to finish while the gate is
//! held.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test admin_lane_provisioning_test -- --nocapture`

mod ci_timing;

use awa::model::{
    admin, batch_operations, dlq, reschedule, QueueStorage, QueueStorageConfig, RetryFromDlqOpts,
};
use awa_testing::setup;
use sqlx::PgPool;
use std::future::Future;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::sync::Mutex;
use uuid::Uuid;

static GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn queue_storage_pool() -> PgPool {
    let pool = setup::setup(10).await;
    setup::activate_queue_storage(&pool).await;
    pool
}

fn unique_queue(prefix: &str) -> String {
    format!("{prefix}_{}", &Uuid::new_v4().simple().to_string()[..12])
}

async fn provision_lane(pool: &PgPool, queue: &str, priority: i16) {
    QueueStorage::new(QueueStorageConfig::default())
        .expect("queue storage")
        .enqueue_batch(pool, queue, priority, 1)
        .await
        .expect("provision lane");
}

async fn newest_ready_job(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar("SELECT max(job_id) FROM awa.ready_entries WHERE queue = $1")
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("ready job id")
}

/// Run `operation` while a gate transaction holds the lane's provisioning lock.
async fn completes_beside_gate<T, F>(
    pool: &PgPool,
    queue: &str,
    priority: i16,
    operation: &str,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    let mut gate = pool.begin().await.expect("begin gate");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("awa:{queue}:{priority}:0"))
        .execute(&mut *gate)
        .await
        .expect("take provisioning lock");
    let result =
        tokio::time::timeout(ci_timing::scaled_timeout(Duration::from_secs(5)), future).await;
    gate.rollback().await.expect("release gate");
    result.unwrap_or_else(|_| {
        panic!("{operation} waited on the provisioning lock of an existing lane")
    })
}

#[tokio::test]
async fn dlq_retry_does_not_wait_on_lane_provisioning() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("lane_gate_dlq");
    provision_lane(&pool, &queue, 2).await;
    let job_id: i64 = sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries (job_id, kind, queue, priority, dlq_reason, original_run_lease) \
         VALUES (nextval('awa.job_id_seq'), 'lane_gate_job', $1, 2, 'seeded', 1) RETURNING job_id",
    )
    .bind(&queue)
    .fetch_one(&pool)
    .await
    .expect("seed dlq row");

    let revived = completes_beside_gate(
        &pool,
        &queue,
        2,
        "dlq::retry_from_dlq",
        dlq::retry_from_dlq(&pool, job_id, &RetryFromDlqOpts::default()),
    )
    .await
    .expect("retry from dlq")
    .expect("revived job");
    assert_eq!(revived.id, job_id);
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn admin_retry_does_not_wait_on_lane_provisioning() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("lane_gate_retry");
    provision_lane(&pool, &queue, 2).await;
    let job_id = newest_ready_job(&pool, &queue).await;
    admin::cancel(&pool, job_id).await.expect("cancel job");

    let revived = completes_beside_gate(
        &pool,
        &queue,
        2,
        "admin::retry",
        admin::retry(&pool, job_id),
    )
    .await
    .expect("admin retry")
    .expect("revived job");
    assert_eq!(revived.id, job_id);
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn batch_set_priority_does_not_wait_on_lane_provisioning() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("lane_gate_batch");
    provision_lane(&pool, &queue, 2).await;
    let job_id = newest_ready_job(&pool, &queue).await;
    provision_lane(&pool, &queue, 1).await;

    let operation = batch_operations::submit_batch_operation(
        &pool,
        batch_operations::SubmitBatchOperation {
            spec: batch_operations::BatchOperationSpec::SetPriority { priority: 1 },
            filter: batch_operations::BatchOperationFilter {
                ids: Some(vec![job_id]),
                ..Default::default()
            },
            submitted_by: None,
            allow_all: false,
        },
    )
    .await
    .expect("submit batch operation");

    let state = completes_beside_gate(&pool, &queue, 1, "batch set-priority", async {
        let runner = Uuid::new_v4();
        for _ in 0..100 {
            batch_operations::run_one_batch_operation_chunk(&pool, runner, 10)
                .await
                .expect("run batch chunk");
            let state = batch_operations::get_batch_operation(&pool, operation.id)
                .await
                .expect("batch operation")
                .state;
            if state == batch_operations::BatchOperationState::Completed {
                return state;
            }
        }
        batch_operations::BatchOperationState::Pending
    })
    .await;
    assert_eq!(state, batch_operations::BatchOperationState::Completed);
    let moved = admin::get_job(&pool, job_id).await.expect("moved job");
    assert_eq!(moved.priority, 1);
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn reschedule_duplicate_does_not_wait_on_lane_provisioning() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("lane_gate_reschedule");
    provision_lane(&pool, &queue, 2).await;

    let unique_key = format!("lane-gate-{queue}").into_bytes();
    let job_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO awa.jobs_hot (
            kind, queue, args, state, priority, attempt, max_attempts,
            run_at, heartbeat_at, attempted_at, created_at, errors, metadata,
            tags, unique_key, unique_states, run_lease
        )
        VALUES (
            'lane_gate_job', $1, '{}'::jsonb, 'running', 2, 1, 25,
            now(), now(), now(), now(), ARRAY[]::jsonb[],
            '{}'::jsonb, ARRAY[]::text[], $2, B'11000000', 3
        )
        RETURNING id
        "#,
    )
    .bind(&queue)
    .bind(&unique_key)
    .fetch_one(&pool)
    .await
    .expect("seed canonical running job");
    sqlx::query("INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, $2)")
        .bind(&unique_key)
        .bind(i64::MAX - 1)
        .execute(&pool)
        .await
        .expect("newer duplicate holds the claim");

    let outcome = completes_beside_gate(
        &pool,
        &queue,
        2,
        "reschedule_canonical_attempt",
        reschedule::reschedule_canonical_attempt(
            &pool,
            job_id,
            3,
            reschedule::Reschedule::Snooze { delay_secs: 60.0 },
            None,
            None,
        ),
    )
    .await
    .expect("reschedule");
    assert!(
        matches!(
            outcome,
            reschedule::RescheduleOutcome::CancelledDuplicate { .. }
        ),
        "{outcome:?}"
    );

    sqlx::query("DELETE FROM awa.job_unique_claims WHERE unique_key = $1")
        .bind(&unique_key)
        .execute(&pool)
        .await
        .expect("drop claim");
    setup::clean_queue(&pool, &queue).await;
}

#[derive(Debug, Default, Clone, Copy)]
struct ProvisioningCalls {
    lane_lock: i64,
    provision_lane: i64,
}

async fn provisioning_calls(pool: &PgPool) -> ProvisioningCalls {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT query, calls FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .fetch_all(pool)
    .await
    .expect("read pg_stat_statements");
    let mut calls = ProvisioningCalls::default();
    for (query, count) in rows {
        if query.contains("AS lane_lock") {
            calls.lane_lock += count;
        } else if query.contains(".provision_lane($1") {
            calls.provision_lane += count;
        }
    }
    calls
}

/// Opt-in: needs `pg_stat_statements` in `shared_preload_libraries`.
#[tokio::test]
#[ignore = "opt-in diagnostic; needs pg_stat_statements preloaded"]
async fn single_job_dlq_retries_do_not_provision_an_existing_lane() {
    const RETRIES: i64 = 200;
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("lane_gate_count");
    provision_lane(&pool, &queue, 2).await;
    let ids: Vec<i64> = sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries (job_id, kind, queue, priority, dlq_reason, original_run_lease) \
         SELECT nextval('awa.job_id_seq'), 'lane_gate_job', $1, 2, 'seeded', 1 \
         FROM generate_series(1, $2) RETURNING job_id",
    )
    .bind(&queue)
    .bind(RETRIES)
    .fetch_all(&pool)
    .await
    .expect("seed dlq rows");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&pool)
        .await
        .expect("pg_stat_statements");
    sqlx::query("SELECT pg_stat_statements_reset()")
        .execute(&pool)
        .await
        .expect("reset statement stats");
    for id in ids {
        dlq::retry_from_dlq(&pool, id, &RetryFromDlqOpts::default())
            .await
            .expect("retry from dlq")
            .expect("revived job");
    }
    let calls = provisioning_calls(&pool).await;
    eprintln!("{RETRIES} single-job DLQ retries: {calls:?}");
    assert_eq!(calls.lane_lock, 0, "{calls:?}");
    assert_eq!(calls.provision_lane, 0, "{calls:?}");
    setup::clean_queue(&pool, &queue).await;
}
