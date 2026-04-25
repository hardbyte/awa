//! Integration tests for queue_storage runtime flows.
//!
//! These tests exercise the full dispatcher/worker/maintenance wiring with the
//! queue_storage backend enabled.

use awa::model::{
    admin, insert, migrations, storage, AwaError, PruneOutcome, QueueStorage, QueueStorageConfig,
    RotateOutcome,
};
use awa::{
    Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, JobRow, JobState, QueueConfig,
    UniqueOpts, Worker,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

static QUEUE_STORAGE_RUNTIME_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn base_database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn replace_database_name(url: &str, database_name: &str) -> String {
    let (without_query, query_suffix) = match url.split_once('?') {
        Some((prefix, query)) => (prefix, Some(query)),
        None => (url, None),
    };
    let (base, _) = without_query
        .rsplit_once('/')
        .expect("database URL should include a database name");
    let mut out = format!("{base}/{database_name}");
    if let Some(query) = query_suffix {
        out.push('?');
        out.push_str(query);
    }
    out
}

fn database_name(url: &str) -> String {
    let without_query = url.split_once('?').map(|(prefix, _)| prefix).unwrap_or(url);
    without_query
        .rsplit_once('/')
        .map(|(_, database_name)| database_name.to_string())
        .expect("database URL should include a database name")
}

fn validate_database_name(database_name: &str) {
    assert!(
        !database_name.is_empty()
            && database_name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
        "queue_storage test database names must use only [A-Za-z0-9_]"
    );
}

fn database_url() -> String {
    std::env::var("DATABASE_URL_QUEUE_STORAGE")
        .unwrap_or_else(|_| replace_database_name(&base_database_url(), "awa_test_queue_storage"))
}

async fn ensure_database_exists(url: &str) {
    let database_name = database_name(url);
    validate_database_name(&database_name);
    let admin_url = replace_database_name(url, "postgres");
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await
        .expect("Failed to connect to admin database for queue_storage tests");
    let create_sql = format!("CREATE DATABASE {database_name}");
    match sqlx::query(&create_sql).execute(&admin_pool).await {
        Ok(_) => {}
        Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P04") => {}
        Err(err) => panic!("Failed to create queue_storage test database {database_name}: {err}"),
    }
}

async fn setup_pool(max_connections: u32) -> sqlx::PgPool {
    let url = database_url();
    ensure_database_exists(&url).await;
    let reset_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("Failed to connect to database for queue_storage schema reset");
    sqlx::raw_sql("DROP SCHEMA IF EXISTS awa CASCADE")
        .execute(&reset_pool)
        .await
        .expect("Failed to drop awa schema for queue_storage tests");
    reset_pool.close().await;

    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&url)
        .await
        .expect("Failed to connect to database");
    migrations::run(&pool)
        .await
        .expect("Failed to run migrations");
    pool
}

async fn recreate_store_schema(pool: &sqlx::PgPool, store: &QueueStorage) {
    let drop_sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", store.schema());
    sqlx::query(&drop_sql)
        .execute(pool)
        .await
        .expect("Failed to drop queue_storage schema");
}

async fn reset_shared_awa_state(pool: &sqlx::PgPool) {
    sqlx::query(
        r#"
        TRUNCATE
            awa.jobs_hot,
            awa.scheduled_jobs,
            awa.queue_meta,
            awa.job_unique_claims,
            awa.queue_state_counts,
            awa.job_kind_catalog,
            awa.job_queue_catalog,
            awa.runtime_instances,
            awa.queue_descriptors,
            awa.job_kind_descriptors,
            awa.cron_jobs,
            awa.runtime_storage_backends
        RESTART IDENTITY CASCADE
        "#,
    )
    .execute(pool)
    .await
    .expect("Failed to reset shared awa state for queue_storage tests");
}

async fn insert_runtime_instance(pool: &sqlx::PgPool, capability: &str) -> uuid::Uuid {
    let instance_id = uuid::Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_instances (
            instance_id,
            hostname,
            pid,
            version,
            storage_capability,
            started_at,
            last_seen_at,
            snapshot_interval_ms,
            healthy,
            postgres_connected,
            poll_loop_alive,
            heartbeat_alive,
            maintenance_alive,
            shutting_down,
            leader,
            global_max_workers,
            queues,
            queue_descriptor_hashes,
            job_kind_descriptor_hashes
        )
        VALUES (
            $1,
            'queue-storage-test',
            1,
            'test',
            $2,
            now(),
            now(),
            10000,
            TRUE,
            TRUE,
            TRUE,
            TRUE,
            TRUE,
            FALSE,
            TRUE,
            NULL,
            '[]'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb
        )
        "#,
    )
    .bind(instance_id)
    .bind(capability)
    .execute(pool)
    .await
    .expect("Failed to insert runtime instance");
    instance_id
}

async fn activate_queue_storage_transition(pool: &sqlx::PgPool, schema: &str) {
    storage::prepare(
        pool,
        "queue_storage",
        serde_json::json!({ "schema": schema }),
    )
    .await
    .expect("Failed to prepare queue storage transition");
    let gate_runtime = insert_runtime_instance(pool, "queue_storage").await;
    storage::enter_mixed_transition(pool)
        .await
        .expect("Failed to enter mixed transition for queue_storage tests");
    storage::finalize(pool)
        .await
        .expect("Failed to finalize queue storage transition for queue_storage tests");
    sqlx::query("DELETE FROM awa.runtime_instances WHERE instance_id = $1")
        .bind(gate_runtime)
        .execute(pool)
        .await
        .expect("Failed to remove queue storage gate runtime");
}

async fn create_store_with_config(pool: &sqlx::PgPool, config: QueueStorageConfig) -> QueueStorage {
    let store = QueueStorage::new(config).expect("Failed to create queue_storage store");
    recreate_store_schema(pool, &store).await;
    reset_shared_awa_state(pool).await;
    storage::abort(pool)
        .await
        .expect("Failed to reset storage transition state for queue_storage tests");
    store
        .prepare_schema(pool)
        .await
        .expect("Failed to prepare store schema");
    store.reset(pool).await.expect("Failed to reset store");
    activate_queue_storage_transition(pool, store.schema()).await;
    store
}

async fn create_store(pool: &sqlx::PgPool, schema: &str) -> QueueStorage {
    create_store_with_config(
        pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        },
    )
    .await
}

async fn attempt_state_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    let sql = format!(
        "SELECT count(*)::bigint FROM {}.attempt_state",
        store.schema()
    );
    sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .expect("Failed to count attempt_state rows")
}

async fn lease_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    let sql = format!("SELECT count(*)::bigint FROM {}.leases", store.schema());
    sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .expect("Failed to count leases")
}

async fn lease_claim_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    let sql = format!(
        "SELECT count(*)::bigint FROM {}.lease_claims",
        store.schema()
    );
    sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .expect("Failed to count lease_claims")
}

/// Count of receipt-backed attempts that are currently "open" — claimed
/// but not yet closed (completed, rescued, or materialized into a live
/// lease row). The runtime derives this set from the partitioned
/// `lease_claims` + `lease_claim_closures` tables anti-joined; this
/// helper mirrors that exact query so test assertions read the same
/// definition the runtime does.
async fn open_receipt_claim_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    let schema = store.schema();
    let sql = format!(
        r#"
        SELECT count(*)::bigint
        FROM {schema}.lease_claims AS claims
        WHERE NOT EXISTS (
            SELECT 1 FROM {schema}.lease_claim_closures AS closures
            WHERE closures.claim_slot = claims.claim_slot
              AND closures.job_id = claims.job_id
              AND closures.run_lease = claims.run_lease
        )
          AND NOT EXISTS (
            SELECT 1 FROM {schema}.leases AS lease
            WHERE lease.job_id = claims.job_id
              AND lease.run_lease = claims.run_lease
        )
        "#,
    );
    sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .expect("Failed to count open receipt claims (derived)")
}

async fn lease_claim_closure_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    let sql = format!(
        "SELECT count(*)::bigint FROM {}.lease_claim_closures",
        store.schema()
    );
    sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .expect("Failed to count lease_claim_closures")
}

fn queue_storage_client<W: Worker + 'static>(
    pool: &sqlx::PgPool,
    queue: &str,
    store_config: QueueStorageConfig,
    worker: W,
) -> Client {
    let deadline_duration = if store_config.experimental_lease_claim_receipts {
        Duration::ZERO
    } else {
        QueueConfig::default().deadline_duration
    };
    Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                deadline_duration,
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            store_config,
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        // Claim-ring prune actually TRUNCATEs idle child partitions.
        // Tests assert hard-coded lease_claim/closure counts assuming
        // those rows survive — keep that invariant by pushing claim
        // rotation past the test's wall-clock window. Tests that
        // exercise rotation directly drive `rotate_claims` explicitly
        // rather than rely on the timer.
        .claim_rotate_interval(Duration::from_secs(60))
        .register_worker(worker)
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(25))
        .build()
        .expect("Failed to build queue_storage client")
}

async fn enqueue_job<T: JobArgs>(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
    args: &T,
    opts: InsertOpts,
) -> i64 {
    let queue_names: Vec<String> = if store.queue_stripe_count() > 1 && !opts.queue.contains('#') {
        (0..store.queue_stripe_count())
            .map(|stripe| format!("{}#{stripe}", opts.queue))
            .collect()
    } else {
        vec![opts.queue.clone()]
    };
    let params = [insert::params_with(args, opts.clone()).expect("Failed to build insert params")];
    store
        .enqueue_params_batch(pool, &params)
        .await
        .expect("Failed to enqueue queue_storage job");

    let query = if opts.run_at.is_some() {
        format!(
            "SELECT job_id FROM {}.deferred_jobs WHERE queue = ANY($1) ORDER BY job_id DESC LIMIT 1",
            store.schema()
        )
    } else {
        format!(
            "SELECT job_id FROM {}.ready_entries WHERE queue = ANY($1) ORDER BY job_id DESC LIMIT 1",
            store.schema()
        )
    };

    sqlx::query_scalar::<_, i64>(&query)
        .bind(&queue_names)
        .fetch_one(pool)
        .await
        .expect("Failed to fetch queue_storage job id")
}

async fn wait_for_job_state(
    store: &QueueStorage,
    pool: &sqlx::PgPool,
    job_id: i64,
    target_states: &[JobState],
    timeout: Duration,
) -> JobRow {
    let start = Instant::now();
    let mut last_state = None;

    loop {
        if let Some(job) = store
            .load_job(pool, job_id)
            .await
            .expect("Failed to load queue_storage job")
        {
            last_state = Some(job.state);
            if target_states.contains(&job.state) {
                return job;
            }
        }

        if start.elapsed() > timeout {
            panic!(
                "Timed out waiting for job {job_id} to reach {:?}; last_state={last_state:?}",
                target_states
            );
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_callback_job(
    store: &QueueStorage,
    pool: &sqlx::PgPool,
    job_id: i64,
    timeout: Duration,
) -> JobRow {
    let start = Instant::now();

    loop {
        if let Some(job) = store
            .load_job(pool, job_id)
            .await
            .expect("Failed to load callback job")
        {
            if job.state == JobState::WaitingExternal && job.callback_id.is_some() {
                return job;
            }
        }

        if start.elapsed() > timeout {
            panic!("Timed out waiting for callback job {job_id} to enter waiting_external");
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn dlq_count(pool: &sqlx::PgPool, store: &QueueStorage, queue: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT count(*)::bigint FROM {}.dlq_entries WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("Failed to count dlq rows")
}

async fn failed_done_count(pool: &sqlx::PgPool, store: &QueueStorage, queue: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT count(*)::bigint FROM {}.done_entries WHERE queue = $1 AND state = 'failed'",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("Failed to count failed done rows")
}

async fn completed_done_count(pool: &sqlx::PgPool, store: &QueueStorage, queue: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT count(*)::bigint FROM {}.done_entries WHERE queue = $1 AND state = 'completed'",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("Failed to count completed done rows")
}

async fn dlq_reason(pool: &sqlx::PgPool, store: &QueueStorage, job_id: i64) -> String {
    sqlx::query_scalar::<_, String>(&format!(
        "SELECT dlq_reason FROM {}.dlq_entries WHERE job_id = $1 ORDER BY dlq_at DESC LIMIT 1",
        store.schema()
    ))
    .bind(job_id)
    .fetch_one(pool)
    .await
    .expect("Failed to fetch dlq reason")
}

fn failed_unique_insert_opts(queue: &str) -> InsertOpts {
    InsertOpts {
        queue: queue.to_string(),
        unique: Some(awa::UniqueOpts {
            states: 1 << JobState::Failed.bit_position(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn available_unique_insert_opts(queue: &str) -> InsertOpts {
    InsertOpts {
        queue: queue.to_string(),
        unique: Some(awa::UniqueOpts {
            states: 1 << JobState::Available.bit_position(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct RetryJob {
    id: i64,
}

struct RetryOnceWorker;

#[async_trait::async_trait]
impl Worker for RetryOnceWorker {
    fn kind(&self) -> &'static str {
        "retry_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        if ctx.job.attempt == 1 {
            Ok(JobResult::RetryAfter(Duration::from_millis(50)))
        } else {
            Ok(JobResult::Completed)
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SnoozeJob {
    id: i64,
}

struct SnoozeOnceWorker {
    seen: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Worker for SnoozeOnceWorker {
    fn kind(&self) -> &'static str {
        "snooze_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        if !self.seen.swap(true, Ordering::SeqCst) {
            Ok(JobResult::Snooze(Duration::from_millis(50)))
        } else {
            Ok(JobResult::Completed)
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct CallbackJob {
    id: i64,
}

struct CallbackWorker {
    timeout: Duration,
}

#[async_trait::async_trait]
impl Worker for CallbackWorker {
    fn kind(&self) -> &'static str {
        "callback_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let callback = ctx
            .register_callback(self.timeout)
            .await
            .map_err(JobError::retryable)?;
        Ok(JobResult::WaitForCallback(callback))
    }
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct DlqJob {
    id: i64,
}

struct TerminalFailureWorker;

#[async_trait::async_trait]
impl Worker for TerminalFailureWorker {
    fn kind(&self) -> &'static str {
        "dlq_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Err(JobError::terminal("boom"))
    }
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct CompleteJob {
    id: i64,
}

struct BlockingCompleteWorker {
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl Worker for BlockingCompleteWorker {
    fn kind(&self) -> &'static str {
        "complete_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        self.release.notified().await;
        Ok(JobResult::Completed)
    }
}

struct CompleteWorker;

#[async_trait::async_trait]
impl Worker for CompleteWorker {
    fn kind(&self) -> &'static str {
        "complete_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::Completed)
    }
}

struct ReceiptRescueWorker {
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl Worker for ReceiptRescueWorker {
    fn kind(&self) -> &'static str {
        "complete_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        if ctx.job.attempt > 1 {
            return Ok(JobResult::Completed);
        }
        self.release.notified().await;
        Ok(JobResult::Completed)
    }
}

struct ProgressRescueWorker;

#[async_trait::async_trait]
impl Worker for ProgressRescueWorker {
    fn kind(&self) -> &'static str {
        "heartbeat_rescue_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        if ctx.job.attempt == 1 {
            ctx.set_progress(10, "started");
            ctx.flush_progress().await.map_err(JobError::retryable)?;
            let started = Instant::now();
            loop {
                if ctx.is_cancelled() {
                    break;
                }
                if started.elapsed() > Duration::from_secs(5) {
                    return Err(JobError::terminal(
                        "progress rescue did not cancel stale attempt",
                    ));
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Ok(JobResult::Completed)
        } else {
            Ok(JobResult::Completed)
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct HeartbeatRescueJob {
    id: i64,
}

struct StaleHeartbeatWorker;

#[async_trait::async_trait]
impl Worker for StaleHeartbeatWorker {
    fn kind(&self) -> &'static str {
        "heartbeat_rescue_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        if ctx.job.attempt == 1 {
            let started = Instant::now();
            loop {
                if ctx.is_cancelled() {
                    break;
                }
                if started.elapsed() > Duration::from_secs(5) {
                    return Err(JobError::terminal(
                        "heartbeat rescue did not cancel stale attempt",
                    ));
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Ok(JobResult::RetryAfter(Duration::from_millis(50)))
        } else {
            Ok(JobResult::Completed)
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct MultiClientJob {
    id: i64,
}

struct MultiClientTrackingWorker {
    seen: Arc<Mutex<HashSet<i64>>>,
    saw_duplicate: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Worker for MultiClientTrackingWorker {
    fn kind(&self) -> &'static str {
        "multi_client_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let mut seen = self.seen.lock().await;
        if !seen.insert(ctx.job.id) {
            self.saw_duplicate.store(true, Ordering::SeqCst);
        }
        drop(seen);

        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(JobResult::Completed)
    }
}

/// ADR-023 claim-ring control-plane smoke test. Exercises
/// `rotate_claims`, the busy-check, and `prune_oldest_claims` on an
/// empty schema: rotate cycles through every slot, prune is a noop
/// when nothing's been written, install + reset leaves the ring
/// seeded correctly. The end-to-end test
/// `test_claim_ring_rotate_and_prune_under_load` covers the busy-path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_claim_ring_rotates_and_prunes_empty() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(4).await;
    let schema = "awa_qs_claim_ring";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            ..Default::default()
        },
    )
    .await;

    // Seeded state: current_slot = 0, generation = 0, slot_count = 4.
    let (initial_slot, initial_gen, initial_count): (i32, i64, i32) = sqlx::query_as(&format!(
        "SELECT current_slot, generation, slot_count FROM {schema}.claim_ring_state WHERE singleton"
    ))
    .fetch_one(&pool)
    .await
    .expect("read initial claim ring state");
    assert_eq!(initial_slot, 0);
    assert_eq!(initial_gen, 0);
    assert_eq!(initial_count, 4);

    let slot_rows: Vec<(i32, i64)> = sqlx::query_as(&format!(
        "SELECT slot, generation FROM {schema}.claim_ring_slots ORDER BY slot"
    ))
    .fetch_all(&pool)
    .await
    .expect("read initial claim ring slot rows");
    assert_eq!(
        slot_rows,
        vec![(0, 0), (1, -1), (2, -1), (3, -1)],
        "seeded slot table should have one open slot and the rest uninitialized"
    );

    // Rotate four times — should advance cursor 0 -> 1 -> 2 -> 3 -> 0,
    // generation 0 -> 1 -> 2 -> 3 -> 4. Empty partitions make the
    // busy-check trivially pass.
    for step in 1..=4_i64 {
        let outcome = store
            .rotate_claims(&pool)
            .await
            .expect("rotate_claims should succeed");
        let expected_slot = (step % 4) as i32;
        match outcome {
            RotateOutcome::Rotated { slot, generation } => {
                assert_eq!(slot, expected_slot, "slot at step {step}");
                assert_eq!(generation, step, "generation at step {step}");
            }
            other => panic!("rotate_claims step {step} unexpected outcome: {other:?}"),
        }
    }

    // On a schema with no claims written yet, prune either noops
    // (oldest_initialized_ring_slot returns None) or TRUNCATEs an
    // already-empty partition (Pruned) — both are legitimate. What we
    // must NOT see is SkippedActive (would mean the safety check
    // reported an open claim where none exists) or Blocked.
    let prune = store
        .prune_oldest_claims(&pool)
        .await
        .expect("prune_oldest_claims should succeed");
    assert!(
        matches!(prune, PruneOutcome::Noop | PruneOutcome::Pruned { .. }),
        "prune_oldest_claims on untouched ring must be Noop or Pruned, got {prune:?}"
    );

    // reset() re-seeds the ring to the initial shape — claim_ring_state
    // back to (0, 0, N), claim_ring_slots back to one-open-rest-uninit.
    store.reset(&pool).await.expect("reset should succeed");
    let (reset_slot, reset_gen, reset_count): (i32, i64, i32) = sqlx::query_as(&format!(
        "SELECT current_slot, generation, slot_count FROM {schema}.claim_ring_state WHERE singleton"
    ))
    .fetch_one(&pool)
    .await
    .expect("read claim ring state after reset");
    assert_eq!(reset_slot, 0);
    assert_eq!(reset_gen, 0);
    assert_eq!(reset_count, 4);

    let post_reset_rows: Vec<(i32, i64)> = sqlx::query_as(&format!(
        "SELECT slot, generation FROM {schema}.claim_ring_slots ORDER BY slot"
    ))
    .fetch_all(&pool)
    .await
    .expect("read claim ring slot rows after reset");
    assert_eq!(
        post_reset_rows,
        vec![(0, 0), (1, -1), (2, -1), (3, -1)],
        "reset should restore the seeded claim-ring slot table"
    );

    // prepare_schema() is idempotent: re-running after reset must not
    // duplicate rows or fail.
    store
        .prepare_schema(&pool)
        .await
        .expect("prepare_schema should be idempotent");
}

/// Wave-1 regression test for the claim-ring rotate+prune pair.
///
/// Exercises the full cycle: claim a job (populates
/// `lease_claims_<current>`), complete it (populates
/// `lease_claim_closures_<current>`), rotate the ring (must NOT flip
/// onto a slot that still has rows), prune the oldest slot (must
/// `TRUNCATE` both children because every claim has a closure), rotate
/// again (now succeeds because the target slot is empty).
///
/// This locks in two ADR-023 invariants that were broken before this
/// fix:
///
/// - `rotate_claims` refuses to advance onto a partition that still
///   has live rows (busy-check), so the ring doesn't lap silently
///   while prune is behind.
/// - `prune_oldest_claims` actually TRUNCATEs when the partition has
///   no open claims — without this `lease_claims` would grow
///   unboundedly under closure-only completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_claim_ring_rotate_and_prune_under_load() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(6).await;
    let schema = "awa_qs_claim_ring_reclaim";
    let queue = "qs_claim_ring_reclaim";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            experimental_lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    // Claim + complete a receipt-backed job. Without prune, this leaves
    // one row in lease_claims_0 and one row in lease_claim_closures_0.
    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 1 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
        },
        CompleteWorker,
    );
    client.start().await.expect("client start");

    let _ = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    client.shutdown(Duration::from_secs(5)).await;

    // Sanity: one claim + one closure both landed in slot 0.
    let slot0_claims: i64 =
        sqlx::query_scalar(&format!("SELECT count(*) FROM {schema}.lease_claims_0"))
            .fetch_one(&pool)
            .await
            .expect("count lease_claims_0");
    let slot0_closures: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claim_closures_0"
    ))
    .fetch_one(&pool)
    .await
    .expect("count lease_claim_closures_0");
    assert_eq!(slot0_claims, 1, "completed claim must live in slot 0");
    assert_eq!(slot0_closures, 1, "matching closure must live in slot 0");

    // Rotate from slot 0 → slot 1. Slot 1 is empty, so busy-check
    // passes and the cursor advances.
    match store
        .rotate_claims(&pool)
        .await
        .expect("rotate_claims -> slot 1")
    {
        RotateOutcome::Rotated { slot, generation } => {
            assert_eq!(slot, 1);
            assert_eq!(generation, 1);
        }
        other => panic!("expected Rotated {{ slot: 1, generation: 1 }}, got {other:?}"),
    }

    // Now try to rotate once more. Next target is slot 2, still empty,
    // so this also succeeds.
    match store
        .rotate_claims(&pool)
        .await
        .expect("rotate_claims -> slot 2")
    {
        RotateOutcome::Rotated { slot, generation } => {
            assert_eq!(slot, 2);
            assert_eq!(generation, 2);
        }
        other => panic!("expected Rotated {{ slot: 2, generation: 2 }}, got {other:?}"),
    }

    // Keep rotating until the next target wraps to slot 0, which still
    // holds the completed claim + closure pair. The busy-check must
    // refuse.
    match store
        .rotate_claims(&pool)
        .await
        .expect("rotate_claims -> slot 3")
    {
        RotateOutcome::Rotated { slot, .. } => assert_eq!(slot, 3),
        other => panic!("expected Rotated to slot 3, got {other:?}"),
    }
    let busy_outcome = store
        .rotate_claims(&pool)
        .await
        .expect("rotate_claims attempt -> slot 0 (busy)");
    assert!(
        matches!(busy_outcome, RotateOutcome::SkippedBusy { slot: 0 }),
        "rotate onto slot 0 with live rows must SkippedBusy, got {busy_outcome:?}"
    );

    // Prune the oldest initialized slot. With every claim in slot 0
    // having a matching closure, PartitionTruncateSafety holds and
    // prune TRUNCATEs both children.
    let prune_outcome = store
        .prune_oldest_claims(&pool)
        .await
        .expect("prune_oldest_claims");
    match prune_outcome {
        PruneOutcome::Pruned { slot } => assert_eq!(slot, 0),
        other => panic!("expected Pruned {{ slot: 0 }}, got {other:?}"),
    }

    // Both children of slot 0 are now empty.
    let post_prune_claims: i64 =
        sqlx::query_scalar(&format!("SELECT count(*) FROM {schema}.lease_claims_0"))
            .fetch_one(&pool)
            .await
            .expect("count lease_claims_0 after prune");
    let post_prune_closures: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claim_closures_0"
    ))
    .fetch_one(&pool)
    .await
    .expect("count lease_claim_closures_0 after prune");
    assert_eq!(
        post_prune_claims, 0,
        "lease_claims_0 must be empty post-prune"
    );
    assert_eq!(
        post_prune_closures, 0,
        "lease_claim_closures_0 must be empty post-prune"
    );

    // And now rotate onto slot 0 succeeds.
    match store
        .rotate_claims(&pool)
        .await
        .expect("rotate_claims -> slot 0 after prune")
    {
        RotateOutcome::Rotated { slot, .. } => assert_eq!(slot, 0),
        other => panic!("expected Rotated to slot 0 after prune, got {other:?}"),
    }
}

/// Wave-1 regression test for the prune safety predicate. If a claim
/// is still open (no matching closure), prune must return
/// `SkippedActive` instead of TRUNCATE-ing the partition and losing
/// the claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_prune_oldest_claims_refuses_to_truncate_open_claim() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(4).await;
    let schema = "awa_qs_claim_ring_open";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            experimental_lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    // Synthesize an open claim in slot 0 without a matching closure.
    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.lease_claims (
            claim_slot, job_id, run_lease, ready_slot, ready_generation,
            queue, priority, attempt, max_attempts, lane_seq
        ) VALUES (0, 999, 1, 0, 0, 'synthetic', 2, 1, 25, 999)
        "#
    ))
    .execute(&pool)
    .await
    .expect("seed open claim");

    // Rotate past slot 0 so it's no longer current.
    for _ in 0..1 {
        store
            .rotate_claims(&pool)
            .await
            .expect("rotate away from slot 0");
    }

    let outcome = store
        .prune_oldest_claims(&pool)
        .await
        .expect("prune_oldest_claims with open claim");
    assert!(
        matches!(outcome, PruneOutcome::SkippedActive { slot: 0 }),
        "prune must refuse to truncate a partition with an open claim, got {outcome:?}"
    );

    // The claim is still there — not lost.
    let survived: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claims_0 WHERE job_id = 999"
    ))
    .fetch_one(&pool)
    .await
    .expect("count survivor");
    assert_eq!(survived, 1, "open claim must survive SkippedActive prune");
}

/// Admin-cancel wakes an in-flight handler via the `awa:cancel`
/// NOTIFY channel. A slow handler checks `ctx.is_cancelled()` and exits
/// with a cancel result as soon as the flag flips. The test enqueues a
/// slow job, waits for it to reach Running, issues
/// `admin::cancel(job_id)` on a separate connection, and asserts the
/// handler observed the cancellation (via a shared atomic) within a
/// tight timeout — proving the NOTIFY → listener → in-flight-flag
/// plumbing is live.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_admin_cancel_wakes_in_flight_handler() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let schema = "awa_qs_admin_cancel_wake";
    let queue = "qs_admin_cancel_wake";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            ..Default::default()
        },
    )
    .await;

    // Shared across the handler and the test: the handler sets
    // `observed_cancel` to true the moment `ctx.is_cancelled()` flips.
    let running = Arc::new(tokio::sync::Notify::new());
    let observed_cancel = Arc::new(AtomicBool::new(false));

    struct CancelObservingWorker {
        running: Arc<tokio::sync::Notify>,
        observed_cancel: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Worker for CancelObservingWorker {
        fn kind(&self) -> &'static str {
            "complete_job"
        }

        async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
            // Tell the test harness we're alive.
            self.running.notify_waiters();
            // Poll the cancel flag every 50ms for up to 10s. As soon as
            // it flips, record and return Cancel.
            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline {
                if ctx.is_cancelled() {
                    self.observed_cancel.store(true, Ordering::SeqCst);
                    return Ok(JobResult::Cancel("admin cancelled".to_string()));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(JobResult::Completed)
        }
    }

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 7 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: false,
        },
        CancelObservingWorker {
            running: running.clone(),
            observed_cancel: observed_cancel.clone(),
        },
    );
    client.start().await.expect("client start");

    // Wait for the handler to actually start executing.
    tokio::time::timeout(Duration::from_secs(5), running.notified())
        .await
        .expect("handler should start running");

    // Issue an admin cancel on a fresh connection — this is what an
    // operator running `awa_model::admin::cancel` in another process
    // would do.
    awa::model::admin::cancel(&pool, job_id)
        .await
        .expect("admin::cancel should succeed on running job");

    // Within a reasonable window the handler should have observed
    // `ctx.is_cancelled() == true` via the NOTIFY-driven listener.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if observed_cancel.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        observed_cancel.load(Ordering::SeqCst),
        "handler must observe admin cancellation via NOTIFY → in-flight flag"
    );

    client.shutdown(Duration::from_secs(5)).await;
}

/// `open_receipt_claims` was the live-frontier table from the
/// pre-ADR-023 design. After the receipt plane moved to anti-joins
/// over the partitioned `lease_claims` / `lease_claim_closures`, the
/// table became dead weight; `prepare_schema` now drops it on every
/// install (refusing to drop a non-empty table — see ADR-023). This
/// test asserts the table is absent on a fresh schema and stays
/// absent across a full claim + complete cycle, while `lease_claims`
/// and `lease_claim_closures` reflect the lifecycle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_open_receipt_claims_is_absent_after_install() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(4).await;
    let schema = "awa_qs_open_receipt_claims_absent";
    let queue = "qs_open_receipt_claims_absent";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            experimental_lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    async fn open_receipt_claims_present(pool: &sqlx::PgPool, schema: &str) -> bool {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = 'open_receipt_claims'
            )
            "#,
        )
        .bind(schema)
        .fetch_one(pool)
        .await
        .expect("probe open_receipt_claims existence")
    }

    assert!(
        !open_receipt_claims_present(&pool, schema).await,
        "open_receipt_claims must not exist after a fresh prepare_schema"
    );

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 42 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
        },
        CompleteWorker,
    );
    client.start().await.expect("start client");

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);

    assert_eq!(
        lease_claim_count(&pool, &store).await,
        1,
        "the single receipt must live in lease_claims"
    );
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        1,
        "the completion must have written a closure row"
    );
    assert!(
        !open_receipt_claims_present(&pool, schema).await,
        "open_receipt_claims must remain absent across the full lifecycle"
    );

    client.shutdown(Duration::from_secs(5)).await;
}

/// Partition-routing smoke test for the ADR-023 receipt plane: a
/// receipt-backed claim + completion cycle lands rows in the expected
/// child partitions of `lease_claims` and `lease_claim_closures`, and
/// both rows share the same `claim_slot` so the closure co-locates with
/// the claim it tombstones.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lease_claim_partition_routing() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(4).await;
    let schema = "awa_qs_claim_partition_routing";
    let queue = "qs_claim_partition_routing";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            experimental_lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    // Rotate the claim ring forward so the current slot is not zero —
    // this proves the claim CTE actually reads claim_ring_state rather
    // than defaulting.
    for _ in 0..2 {
        store
            .rotate_claims(&pool)
            .await
            .expect("rotate_claims should succeed");
    }
    let current_slot: i32 = sqlx::query_scalar(&format!(
        "SELECT current_slot FROM {schema}.claim_ring_state WHERE singleton"
    ))
    .fetch_one(&pool)
    .await
    .expect("read current claim slot");
    assert_eq!(
        current_slot, 2,
        "ring should be at slot 2 after two rotations"
    );

    let job_id = enqueue_job(
        &pool,
        &store,
        &RetryJob { id: 777 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
        },
        RetryOnceWorker,
    );
    client.start().await.expect("client start");

    let _completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;

    // Assert claim and closure both live in claim_slot = 2, and in the
    // matching physical child partition.
    let claim_slot: i32 = sqlx::query_scalar(&format!(
        "SELECT claim_slot FROM {schema}.lease_claims WHERE job_id = $1 ORDER BY run_lease DESC LIMIT 1"
    ))
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("read claim_slot from lease_claims");
    assert_eq!(claim_slot, 2, "claim row should land in current slot");

    let closure_slot: i32 = sqlx::query_scalar(&format!(
        "SELECT claim_slot FROM {schema}.lease_claim_closures WHERE job_id = $1 ORDER BY closed_at DESC LIMIT 1"
    ))
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("read claim_slot from lease_claim_closures");
    assert_eq!(
        closure_slot, claim_slot,
        "closure must live in the same partition as its originating claim"
    );

    // Physically: both rows must be addressable via their child-partition names.
    let claim_in_child: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claims_2 WHERE job_id = $1"
    ))
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("count in lease_claims_2");
    assert!(claim_in_child >= 1, "claim row must be in lease_claims_2");

    let closure_in_child: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claim_closures_2 WHERE job_id = $1"
    ))
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("count in lease_claim_closures_2");
    assert!(
        closure_in_child >= 1,
        "closure row must be in lease_claim_closures_2"
    );

    client.shutdown(Duration::from_secs(5)).await;
}

/// Rotation-isolation check for the ADR-023 claim ring. A claim landed
/// in slot A before rotation stays in slot A. After rotation, a fresh
/// claim lands in slot B. Neither disturbs the other — partitioning
/// and ring state are consistent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lease_claim_rotation_isolation() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(4).await;
    let schema = "awa_qs_claim_rotation_isolation";
    let queue = "qs_claim_rotation_isolation";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            experimental_lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    let job_a = enqueue_job(
        &pool,
        &store,
        &RetryJob { id: 1 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
        },
        RetryOnceWorker,
    );
    client.start().await.expect("client start");

    let _ = wait_for_job_state(
        &store,
        &pool,
        job_a,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;

    let slot_a: i32 = sqlx::query_scalar(&format!(
        "SELECT claim_slot FROM {schema}.lease_claims WHERE job_id = $1 LIMIT 1"
    ))
    .bind(job_a)
    .fetch_one(&pool)
    .await
    .expect("read slot_a");

    // Rotate the ring so subsequent claims land in a different partition.
    store
        .rotate_claims(&pool)
        .await
        .expect("rotate_claims between jobs");

    let job_b = enqueue_job(
        &pool,
        &store,
        &RetryJob { id: 2 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    let _ = wait_for_job_state(
        &store,
        &pool,
        job_b,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;

    let slot_b: i32 = sqlx::query_scalar(&format!(
        "SELECT claim_slot FROM {schema}.lease_claims WHERE job_id = $1 LIMIT 1"
    ))
    .bind(job_b)
    .fetch_one(&pool)
    .await
    .expect("read slot_b");
    assert_ne!(
        slot_a, slot_b,
        "job B (post-rotation) must land in a different claim_slot than job A"
    );

    // Job A is still exactly where it was written — rotation didn't
    // mutate existing rows.
    let job_a_slot_still: i32 = sqlx::query_scalar(&format!(
        "SELECT claim_slot FROM {schema}.lease_claims WHERE job_id = $1 LIMIT 1"
    ))
    .bind(job_a)
    .fetch_one(&pool)
    .await
    .expect("read slot_a still");
    assert_eq!(
        slot_a, job_a_slot_still,
        "rotation must not move existing claim rows across partitions"
    );

    client.shutdown(Duration::from_secs(5)).await;
}

/// Receipt-plane partition-migration test (see ADR-023). Start from
/// a schema that still has the legacy regular (non-partitioned)
/// `lease_claims` + `lease_claim_closures`, seed some rows in them,
/// run `prepare_schema`, and assert:
/// - both parents are now partitioned (`relkind = 'p'`)
/// - all pre-existing rows landed in the current `claim_ring_state` slot
/// - the legacy tables are dropped
/// Validates the rename → create partitioned → copy → drop path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lease_claim_migration_preserves_rows() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(4).await;
    let schema = "awa_qs_claim_migration";
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("drop schema");
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&pool)
        .await
        .expect("create schema");

    // Stand up the legacy regular-table shape so the migration path
    // runs on `prepare_schema`.
    sqlx::query(&format!(
        r#"
        CREATE TABLE {schema}.lease_claims (
            job_id BIGINT NOT NULL,
            run_lease BIGINT NOT NULL,
            ready_slot INT NOT NULL,
            ready_generation BIGINT NOT NULL,
            queue TEXT NOT NULL,
            priority SMALLINT NOT NULL,
            attempt SMALLINT NOT NULL,
            max_attempts SMALLINT NOT NULL,
            lane_seq BIGINT NOT NULL,
            claimed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            materialized_at TIMESTAMPTZ,
            PRIMARY KEY (job_id, run_lease)
        )
        "#
    ))
    .execute(&pool)
    .await
    .expect("legacy lease_claims");

    sqlx::query(&format!(
        r#"
        CREATE TABLE {schema}.lease_claim_closures (
            job_id BIGINT NOT NULL,
            run_lease BIGINT NOT NULL,
            outcome TEXT NOT NULL,
            closed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (job_id, run_lease)
        )
        "#
    ))
    .execute(&pool)
    .await
    .expect("legacy lease_claim_closures");

    for job_id in 1..=5_i64 {
        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.lease_claims
                (job_id, run_lease, ready_slot, ready_generation, queue,
                 priority, attempt, max_attempts, lane_seq, claimed_at, materialized_at)
            VALUES ($1, 1, 0, 0, 'legacy', 2, 1, 25, $1, now(), NULL)
            "#
        ))
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("seed lease_claims row");
    }
    for job_id in [1_i64, 2] {
        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.lease_claim_closures
                (job_id, run_lease, outcome, closed_at)
            VALUES ($1, 1, 'completed', now())
            "#
        ))
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("seed closure row");
    }

    let store = QueueStorage::new(QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 4,
        ..Default::default()
    })
    .expect("construct store");

    reset_shared_awa_state(&pool).await;
    storage::abort(&pool)
        .await
        .expect("reset storage transition state");
    store
        .prepare_schema(&pool)
        .await
        .expect("prepare_schema with legacy data");

    // Both parents are partitioned now.
    for name in ["lease_claims", "lease_claim_closures"] {
        let relkind: String = sqlx::query_scalar(
            r#"
            SELECT c.relkind::text FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2
            "#,
        )
        .bind(schema)
        .bind(name)
        .fetch_one(&pool)
        .await
        .expect("relkind lookup");
        assert_eq!(
            relkind, "p",
            "{name} must be partitioned after prepare_schema"
        );
    }

    // Legacy tables are dropped.
    for name in ["lease_claims_legacy", "lease_claim_closures_legacy"] {
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = $2
            )
            "#,
        )
        .bind(schema)
        .bind(name)
        .fetch_one(&pool)
        .await
        .expect("legacy table existence");
        assert!(!exists, "{name} must be dropped after migration");
    }

    // All pre-existing rows landed in current claim_slot.
    let current_slot: i32 = sqlx::query_scalar(&format!(
        "SELECT current_slot FROM {schema}.claim_ring_state WHERE singleton"
    ))
    .fetch_one(&pool)
    .await
    .expect("read current slot");

    let claims_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claims WHERE claim_slot = $1"
    ))
    .bind(current_slot)
    .fetch_one(&pool)
    .await
    .expect("count migrated claims");
    assert_eq!(
        claims_count, 5,
        "all 5 legacy claim rows must migrate into current_slot"
    );

    let closures_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claim_closures WHERE claim_slot = $1"
    ))
    .bind(current_slot)
    .fetch_one(&pool)
    .await
    .expect("count migrated closures");
    assert_eq!(
        closures_count, 2,
        "both legacy closure rows must migrate into current_slot"
    );

    // prepare_schema is idempotent: second call on the already-partitioned
    // tables is a no-op and doesn't duplicate or drop rows.
    store
        .prepare_schema(&pool)
        .await
        .expect("prepare_schema idempotent after migration");

    let claims_count_after: i64 =
        sqlx::query_scalar(&format!("SELECT count(*) FROM {schema}.lease_claims"))
            .fetch_one(&pool)
            .await
            .expect("count claims after idempotent call");
    assert_eq!(
        claims_count_after, 5,
        "idempotent prepare must not duplicate"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_runtime_retry_after() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_retry_runtime";
    let schema = "awa_qs_runtime_retry";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &RetryJob { id: 1 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        },
        RetryOnceWorker,
    );
    client.start().await.expect("Failed to start retry client");

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(completed.attempt, 2);

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_two_clients_drain_without_duplicate_execution() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(20).await;
    let queue = "qs_two_clients";
    let schema = "awa_qs_two_clients";
    let store = create_store(&pool, schema).await;
    let store_config = QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        ..Default::default()
    };

    let seen = Arc::new(Mutex::new(HashSet::new()));
    let saw_duplicate = Arc::new(AtomicBool::new(false));

    let client_a = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            store_config.clone(),
            Duration::from_secs(60),
            Duration::from_millis(50),
        )
        .register_worker(MultiClientTrackingWorker {
            seen: seen.clone(),
            saw_duplicate: saw_duplicate.clone(),
        })
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(25))
        .build()
        .expect("Failed to build first queue_storage client");

    let client_b = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 2,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            store_config.clone(),
            Duration::from_secs(60),
            Duration::from_millis(50),
        )
        .register_worker(MultiClientTrackingWorker {
            seen: seen.clone(),
            saw_duplicate: saw_duplicate.clone(),
        })
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(25))
        .build()
        .expect("Failed to build second queue_storage client");

    client_a
        .start()
        .await
        .expect("Failed to start first queue_storage client");
    client_b
        .start()
        .await
        .expect("Failed to start second queue_storage client");

    let job_count = 64_i64;
    for id in 0..job_count {
        enqueue_job(
            &pool,
            &store,
            &MultiClientJob { id },
            InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        )
        .await;
    }

    let start = Instant::now();
    loop {
        let completed = completed_done_count(&pool, &store, queue).await;
        let unique_seen = seen.lock().await.len();

        if completed == job_count && unique_seen == job_count as usize {
            break;
        }

        if start.elapsed() > Duration::from_secs(20) {
            panic!(
                "Timed out draining two-client queue storage test; completed={completed}, unique_seen={unique_seen}, saw_duplicate={}",
                saw_duplicate.load(Ordering::SeqCst)
            );
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert!(
        !saw_duplicate.load(Ordering::SeqCst),
        "two queue-storage clients should not execute the same job twice"
    );
    assert_eq!(seen.lock().await.len(), job_count as usize);

    client_a.shutdown(Duration::from_secs(5)).await;
    client_b.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_late_completion_after_retry_after_is_noop() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_guard_late_complete_retry";
    let schema = "awa_qs_guard_late_complete_retry";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 101 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("Failed to claim guard retry job");
    assert_eq!(claimed.len(), 1);
    let claimed = claimed.into_iter().next().expect("missing claimed job");

    let retried = store
        .retry_after(
            &pool,
            job_id,
            claimed.job.run_lease,
            Duration::from_secs(5),
            None,
        )
        .await
        .expect("Failed to move running job to retryable")
        .expect("Expected running job to move to retryable");
    assert_eq!(retried.state, JobState::Retryable);

    let completed = store
        .complete_runtime_batch(&pool, std::slice::from_ref(&claimed))
        .await
        .expect("Failed to attempt stale completion after retry");
    assert!(
        completed.is_empty(),
        "late completion should be ignored once the lease has been retried"
    );

    let current = store
        .load_job(&pool, job_id)
        .await
        .expect("Failed to load retried guard job")
        .expect("Expected retried job to exist");
    assert_eq!(current.state, JobState::Retryable);
    assert_eq!(current.attempt, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_late_completion_cannot_finalize_reclaimed_running_attempt() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_guard_reclaimed_running";
    let schema = "awa_qs_guard_reclaimed_running";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 102 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let first_claim = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("Failed to claim first running attempt");
    let first_claim = first_claim
        .into_iter()
        .next()
        .expect("missing first claimed job");

    store
        .retry_after(
            &pool,
            job_id,
            first_claim.job.run_lease,
            Duration::ZERO,
            None,
        )
        .await
        .expect("Failed to move first lease to retryable")
        .expect("Expected running job to move to retryable");

    let promoted = store
        .promote_due(&pool, JobState::Retryable, 1)
        .await
        .expect("Failed to promote retryable job");
    assert_eq!(promoted, 1);

    let second_claim = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("Failed to claim reclaimed running attempt");
    let second_claim = second_claim
        .into_iter()
        .next()
        .expect("missing reclaimed running attempt");
    assert!(
        second_claim.job.run_lease > first_claim.job.run_lease,
        "reclaimed attempt should use a new run_lease"
    );

    let completed = store
        .complete_runtime_batch(&pool, std::slice::from_ref(&first_claim))
        .await
        .expect("Failed to attempt stale completion against reclaimed attempt");
    assert!(
        completed.is_empty(),
        "stale completion must not finalize a newer running attempt"
    );

    let current = store
        .load_job(&pool, job_id)
        .await
        .expect("Failed to load reclaimed running job")
        .expect("Expected reclaimed running job to exist");
    assert_eq!(current.state, JobState::Running);
    assert_eq!(current.attempt, 2);
    assert_eq!(current.run_lease, second_claim.job.run_lease);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_late_completion_after_cancel_is_noop() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_guard_late_cancel";
    let schema = "awa_qs_guard_late_cancel";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 103 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("Failed to claim guard cancel job");
    let claimed = claimed.into_iter().next().expect("missing claimed job");

    let cancelled = store
        .cancel_running(&pool, job_id, claimed.job.run_lease, "test cancel", None)
        .await
        .expect("Failed to cancel running job")
        .expect("Expected running job to be cancelled");
    assert_eq!(cancelled.state, JobState::Cancelled);

    let completed = store
        .complete_runtime_batch(&pool, std::slice::from_ref(&claimed))
        .await
        .expect("Failed to attempt stale completion after cancel");
    assert!(
        completed.is_empty(),
        "late completion should be ignored after cancel"
    );

    let current = store
        .load_job(&pool, job_id)
        .await
        .expect("Failed to load cancelled guard job")
        .expect("Expected cancelled job to exist");
    assert_eq!(current.state, JobState::Cancelled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_dlq_and_retry_race_has_single_winner() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_guard_dlq_race";
    let schema = "awa_qs_guard_dlq_race";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 104 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("Failed to claim DLQ race job");
    let claimed = claimed.into_iter().next().expect("missing claimed job");

    let (retry_result, dlq_result) = tokio::join!(
        store.retry_after(&pool, job_id, claimed.job.run_lease, Duration::ZERO, None),
        store.fail_to_dlq(
            &pool,
            job_id,
            claimed.job.run_lease,
            "raced to dlq",
            "boom",
            None,
        )
    );

    let retry_result = retry_result.expect("retry_after should not error");
    let dlq_result = dlq_result.expect("fail_to_dlq should not error");
    assert_ne!(
        retry_result.is_some(),
        dlq_result.is_some(),
        "retry and DLQ finalization must not both win the same lease"
    );

    if retry_result.is_some() {
        let current = store
            .load_job(&pool, job_id)
            .await
            .expect("Failed to load retried job")
            .expect("Expected retried job to exist");
        assert_eq!(current.state, JobState::Retryable);
        assert_eq!(dlq_count(&pool, &store, queue).await, 0);
    } else {
        assert_eq!(dlq_count(&pool, &store, queue).await, 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_register_callback_rejects_stale_lease() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_guard_callback_lease";
    let schema = "awa_qs_guard_callback_lease";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &CallbackJob { id: 104 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let first_claim = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("Failed to claim callback guard job");
    let first_claim = first_claim
        .into_iter()
        .next()
        .expect("missing callback guard claim");

    store
        .retry_after(
            &pool,
            job_id,
            first_claim.job.run_lease,
            Duration::ZERO,
            None,
        )
        .await
        .expect("Failed to retry callback guard job")
        .expect("Expected running callback guard job to move to retryable");
    let promoted = store
        .promote_due(&pool, JobState::Retryable, 1)
        .await
        .expect("Failed to promote callback guard retryable");
    assert_eq!(promoted, 1);

    let second_claim = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("Failed to reclaim callback guard job");
    let second_claim = second_claim
        .into_iter()
        .next()
        .expect("missing reclaimed callback guard job");

    let err = store
        .register_callback(
            &pool,
            job_id,
            first_claim.job.run_lease,
            Duration::from_secs(3600),
        )
        .await
        .unwrap_err();
    match err {
        AwaError::Validation(msg) => {
            assert!(msg.contains("job is not in running state"));
        }
        other => panic!("Expected Validation error, got: {other:?}"),
    }

    let callback_id = store
        .register_callback(
            &pool,
            job_id,
            second_claim.job.run_lease,
            Duration::from_secs(3600),
        )
        .await
        .expect("Failed to register callback for current lease");
    assert!(!callback_id.is_nil());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_short_jobs_do_not_create_attempt_state() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_attempt_state_short_job";
    let schema = "awa_qs_runtime_attempt_state_short";
    let store = create_store(&pool, schema).await;
    let release = Arc::new(tokio::sync::Notify::new());
    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        },
        BlockingCompleteWorker {
            release: release.clone(),
        },
    );

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 1 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    client
        .start()
        .await
        .expect("Failed to start short-job client");

    let running = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Running],
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(running.state, JobState::Running);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);

    release.notify_waiters();

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_short_jobs_complete_via_lease_claim_receipts() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_lease_claim_short_job";
    let schema = "awa_qs_runtime_lease_claim_short";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;
    let release = Arc::new(tokio::sync::Notify::new());
    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
            claim_slot_count: 2,
        },
        BlockingCompleteWorker {
            release: release.clone(),
        },
    );

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 2 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    client
        .start()
        .await
        .expect("Failed to start lease-claim client");

    let running = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Running],
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(running.state, JobState::Running);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_count(&pool, &store).await, 1);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 1);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 0);
    let running_counts = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to load queue counts while receipt-backed job is running");
    assert_eq!(running_counts.running, 1);

    release.notify_waiters();

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_count(&pool, &store).await, 1);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 1);
    let completed_counts = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to load queue counts after receipt-backed completion");
    assert_eq!(completed_counts.running, 0);

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_striped_short_jobs_complete_via_lease_claim_receipts() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_lease_claim_short_job_striped";
    let schema = "awa_qs_runtime_lease_claim_short_striped";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 4,
            experimental_lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;
    let release = Arc::new(tokio::sync::Notify::new());
    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 4,
            experimental_lease_claim_receipts: true,
            claim_slot_count: 2,
        },
        BlockingCompleteWorker {
            release: release.clone(),
        },
    );

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 2002 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    client
        .start()
        .await
        .expect("Failed to start striped lease-claim client");

    let running = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Running],
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(running.state, JobState::Running);

    release.notify_waiters();

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_lease_claim_receipts_require_zero_deadline_duration() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_lease_claim_deadline_guard";
    let schema = "awa_qs_runtime_lease_claim_deadline_guard";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let _job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 3 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let err = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(1))
        .await
        .expect_err("receipt-backed claims must reject non-zero deadlines");
    match err {
        AwaError::Validation(msg) => {
            assert!(
                msg.contains("require queue deadline_duration=0"),
                "unexpected validation message: {msg}"
            );
        }
        other => panic!("expected Validation error, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_claims_materialize_on_heartbeat() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_lease_claim_materialize_heartbeat";
    let schema = "awa_qs_runtime_lease_claim_materialize_heartbeat";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;
    let release = Arc::new(tokio::sync::Notify::new());
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                deadline_duration: Duration::ZERO,
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: schema.to_string(),
                queue_slot_count: 4,
                lease_slot_count: 2,
                queue_stripe_count: 1,
                experimental_lease_claim_receipts: true,
                claim_slot_count: 2,
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .register_worker(BlockingCompleteWorker {
            release: release.clone(),
        })
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(250))
        .deadline_rescue_interval(Duration::from_millis(250))
        .callback_rescue_interval(Duration::from_millis(25))
        .build()
        .expect("Failed to build heartbeat materialization client");

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 4 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    client
        .start()
        .await
        .expect("Failed to start heartbeat materialization client");

    let running = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Running],
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(running.state, JobState::Running);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_count(&pool, &store).await, 1);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 1);

    let materialization_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if attempt_state_count(&pool, &store).await == 1 {
            break;
        }
        if Instant::now() > materialization_deadline {
            panic!("timed out waiting for heartbeat to materialize receipt-backed attempt state");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let running = store
        .load_job(&pool, job_id)
        .await
        .expect("Failed to load receipt-backed running job after heartbeat")
        .expect("Expected receipt-backed running job after heartbeat");
    assert_eq!(running.state, JobState::Running);
    assert!(running.heartbeat_at.is_some());
    assert_eq!(lease_claim_count(&pool, &store).await, 1);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 1);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 0);
    assert_eq!(lease_count(&pool, &store).await, 0);

    release.notify_waiters();

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_count(&pool, &store).await, 1);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 1);

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_claims_retry_successfully() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_lease_claim_retry";
    let schema = "awa_qs_runtime_lease_claim_retry";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;
    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
            claim_slot_count: 2,
        },
        RetryOnceWorker,
    );

    let job_id = enqueue_job(
        &pool,
        &store,
        &RetryJob { id: 7 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    client
        .start()
        .await
        .expect("Failed to start receipt retry client");

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(completed.attempt, 2);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_count(&pool, &store).await, 2);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 2);

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_claims_fail_retryable_without_materializing_leases() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_lease_claim_fail_retryable";
    let schema = "awa_qs_runtime_lease_claim_fail_retryable";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 71 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("Failed to claim receipt-backed job");
    let claimed = claimed.into_iter().next().expect("missing claimed job");

    let retried = store
        .fail_retryable(
            &pool,
            job_id,
            claimed.job.run_lease,
            "synthetic error",
            None,
        )
        .await
        .expect("Failed to fail retryable receipt-backed job")
        .expect("Expected receipt-backed job to move to retryable");
    assert_eq!(retried.state, JobState::Retryable);
    assert_eq!(retried.attempt, 1);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_attempt_state_only_receipts_rescue_after_stale_heartbeat() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_lease_claim_attempt_rescue";
    let schema = "awa_qs_runtime_lease_claim_attempt_rescue";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &HeartbeatRescueJob { id: 6 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                deadline_duration: Duration::ZERO,
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: schema.to_string(),
                queue_slot_count: 4,
                lease_slot_count: 2,
                queue_stripe_count: 1,
                experimental_lease_claim_receipts: true,
                claim_slot_count: 2,
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .claim_rotate_interval(Duration::from_secs(60))
        .register_worker(ProgressRescueWorker)
        .heartbeat_interval(Duration::from_secs(60))
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .heartbeat_staleness(Duration::from_millis(250))
        .deadline_rescue_interval(Duration::from_secs(10))
        .callback_rescue_interval(Duration::from_secs(10))
        .build()
        .expect("Failed to build attempt-state receipt rescue client");

    client
        .start()
        .await
        .expect("Failed to start attempt-state receipt rescue client");

    let running = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Running],
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(running.state, JobState::Running);

    let materialization_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if attempt_state_count(&pool, &store).await == 1 {
            break;
        }
        if Instant::now() > materialization_deadline {
            panic!("timed out waiting for receipt-backed progress flush to create attempt_state");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 1);
    let running = store
        .load_job(&pool, job_id)
        .await
        .expect("Failed to load running attempt-state receipt job")
        .expect("Expected running attempt-state receipt job");
    assert_eq!(running.state, JobState::Running);
    assert!(running.heartbeat_at.is_some());

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(15),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(completed.attempt, 2);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_count(&pool, &store).await, 2);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 2);

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_claims_rescue_after_grace_window() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_lease_claim_rescue";
    let schema = "awa_qs_runtime_lease_claim_rescue";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;
    let release = Arc::new(tokio::sync::Notify::new());
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                deadline_duration: Duration::ZERO,
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: schema.to_string(),
                queue_slot_count: 4,
                lease_slot_count: 2,
                queue_stripe_count: 1,
                experimental_lease_claim_receipts: true,
                claim_slot_count: 2,
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .claim_rotate_interval(Duration::from_secs(60))
        .register_worker(ReceiptRescueWorker {
            release: release.clone(),
        })
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_interval(Duration::from_secs(60))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .heartbeat_staleness(Duration::from_millis(250))
        .deadline_rescue_interval(Duration::from_secs(10))
        .callback_rescue_interval(Duration::from_secs(10))
        .build()
        .expect("Failed to build receipt rescue client");

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 5 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    client
        .start()
        .await
        .expect("Failed to start receipt rescue client");

    let running = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Running],
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(running.state, JobState::Running);
    assert_eq!(running.attempt, 1);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_count(&pool, &store).await, 1);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 1);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 0);

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(15),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(completed.attempt, 2);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_count(&pool, &store).await, 2);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 2);

    release.notify_waiters();
    tokio::time::sleep(Duration::from_millis(250)).await;

    let current = store
        .load_job(&pool, job_id)
        .await
        .expect("Failed to load receipt rescue job after late completion")
        .expect("Expected receipt rescue job to exist");
    assert_eq!(current.state, JobState::Completed);
    assert_eq!(current.attempt, 2);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 2);

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_runtime_snooze() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_snooze_runtime";
    let schema = "awa_qs_runtime_snooze";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &SnoozeJob { id: 2 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        },
        SnoozeOnceWorker {
            seen: Arc::new(AtomicBool::new(false)),
        },
    );
    client.start().await.expect("Failed to start snooze client");

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(completed.attempt, 1, "snooze should not consume an attempt");

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_runtime_stale_heartbeat_rescue() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_heartbeat_rescue";
    let schema = "awa_qs_runtime_heartbeat_rescue";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &HeartbeatRescueJob { id: 3 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                deadline_duration: Duration::from_secs(30),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: schema.to_string(),
                queue_slot_count: 4,
                lease_slot_count: 2,
                ..Default::default()
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .register_worker(StaleHeartbeatWorker)
        .heartbeat_interval(Duration::from_secs(5))
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .heartbeat_staleness(Duration::from_millis(250))
        .deadline_rescue_interval(Duration::from_secs(10))
        .callback_rescue_interval(Duration::from_secs(10))
        .build()
        .expect("Failed to build heartbeat rescue client");
    client
        .start()
        .await
        .expect("Failed to start heartbeat rescue client");

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(15),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(completed.attempt, 2);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_admin_queries_cover_running_and_failed_rows() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_admin_runtime";
    let schema = "awa_qs_admin_runtime";
    let store = create_store(&pool, schema).await;
    let release = Arc::new(tokio::sync::Notify::new());

    let running_job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 91 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    let failed_job_id = enqueue_job(
        &pool,
        &store,
        &DlqJob { id: 92 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: schema.to_string(),
                queue_slot_count: 4,
                lease_slot_count: 2,
                ..Default::default()
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .register_worker(BlockingCompleteWorker {
            release: release.clone(),
        })
        .register_worker(TerminalFailureWorker)
        .dlq_enabled_by_default(true)
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(25))
        .build()
        .expect("Failed to build queue_storage admin client");
    client
        .start()
        .await
        .expect("Failed to start queue_storage admin client");

    let running = wait_for_job_state(
        &store,
        &pool,
        running_job_id,
        &[JobState::Running],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(running.state, JobState::Running);

    let failed = wait_for_job_state(
        &store,
        &pool,
        failed_job_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(failed.state, JobState::Failed);

    let queues = admin::queue_overviews(&pool)
        .await
        .expect("Failed to load queue overviews");
    let queue_overview = queues
        .iter()
        .find(|overview| overview.queue == queue)
        .expect("Missing queue overview for queue_storage queue");
    assert_eq!(queue_overview.running, 1);
    assert_eq!(queue_overview.failed, 1);
    assert_eq!(queue_overview.total_queued, 1);

    let job_kinds = admin::job_kind_overviews(&pool)
        .await
        .expect("Failed to load job kind overviews");
    let complete_kind = job_kinds
        .iter()
        .find(|overview| overview.kind == "complete_job")
        .expect("Missing complete_job kind overview");
    assert_eq!(complete_kind.job_count, 1);
    assert_eq!(complete_kind.queue_count, 1);
    let failed_kind = job_kinds
        .iter()
        .find(|overview| overview.kind == "dlq_job")
        .expect("Missing dlq_job kind overview");
    assert_eq!(failed_kind.job_count, 1);
    assert_eq!(failed_kind.queue_count, 1);

    let running_jobs = admin::list_jobs(
        &pool,
        &admin::ListJobsFilter {
            state: Some(JobState::Running),
            queue: Some(queue.to_string()),
            ..Default::default()
        },
    )
    .await
    .expect("Failed to list running queue_storage jobs");
    assert_eq!(running_jobs.len(), 1);
    assert_eq!(running_jobs[0].id, running_job_id);

    let failed_jobs = admin::list_jobs(
        &pool,
        &admin::ListJobsFilter {
            state: Some(JobState::Failed),
            queue: Some(queue.to_string()),
            ..Default::default()
        },
    )
    .await
    .expect("Failed to list failed queue_storage jobs");
    assert_eq!(failed_jobs.len(), 1);
    assert_eq!(failed_jobs[0].id, failed_job_id);

    release.notify_waiters();
    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_prune_skips_live_ready_slot_until_completion() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_prune_live_slot";
    let schema = "awa_qs_runtime_prune_live_slot";
    let store = create_store(&pool, schema).await;

    let release = Arc::new(tokio::sync::Notify::new());
    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        },
        BlockingCompleteWorker {
            release: release.clone(),
        },
    );

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 4 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    client
        .start()
        .await
        .expect("Failed to start prune-live-slot client");

    let running = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Running],
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(running.state, JobState::Running);

    let rotated = store
        .rotate(&pool)
        .await
        .expect("Failed to rotate queue ring");
    assert!(
        matches!(rotated, RotateOutcome::Rotated { slot: 1, .. }),
        "unexpected rotate outcome: {rotated:?}"
    );

    let prune_while_running = store
        .prune_oldest(&pool)
        .await
        .expect("Failed to prune oldest live slot");
    assert!(
        matches!(prune_while_running, PruneOutcome::SkippedActive { slot: 0 }),
        "unexpected prune outcome while lease is live: {prune_while_running:?}"
    );

    release.notify_waiters();

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);

    let prune_after_completion = store
        .prune_oldest(&pool)
        .await
        .expect("Failed to prune oldest completed slot");
    assert!(
        matches!(prune_after_completion, PruneOutcome::Pruned { slot: 0 }),
        "unexpected prune outcome after completion: {prune_after_completion:?}"
    );

    let counts_after_prune = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to sample queue counts after pruning completed slot");
    assert_eq!(counts_after_prune.available, 0);
    assert_eq!(counts_after_prune.running, 0);
    assert_eq!(counts_after_prune.completed, 1);

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_queue_counts_reads_legacy_lane_rollups_and_backfills_them() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_legacy_pruned_rollup";
    let schema = "awa_qs_legacy_pruned_rollup";
    let store = create_store(&pool, schema).await;

    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.queue_lanes (
            queue,
            priority,
            next_seq,
            claim_seq,
            available_count,
            pruned_completed_count
        )
        VALUES ($1, 1, 1, 1, 0, 7)
        ON CONFLICT (queue, priority) DO UPDATE
        SET pruned_completed_count = EXCLUDED.pruned_completed_count
        "#
    ))
    .bind(queue)
    .execute(&pool)
    .await
    .expect("Failed to seed legacy lane rollup");

    let counts_before_backfill = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to read queue counts before backfill");
    assert_eq!(counts_before_backfill.completed, 7);

    store
        .prepare_schema(&pool)
        .await
        .expect("Failed to rerun queue storage schema preparation");

    let legacy_lane_rollup: i64 = sqlx::query_scalar(&format!(
        "SELECT pruned_completed_count FROM {schema}.queue_lanes WHERE queue = $1 AND priority = 1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to read legacy lane rollup after backfill");
    assert_eq!(legacy_lane_rollup, 0);

    let cold_rollup: i64 = sqlx::query_scalar(&format!(
        "SELECT pruned_completed_count FROM {schema}.queue_terminal_rollups WHERE queue = $1 AND priority = 1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to read cold terminal rollup after backfill");
    assert_eq!(cold_rollup, 7);

    let counts_after_backfill = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to read queue counts after backfill");
    assert_eq!(counts_after_backfill.completed, 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_queue_counts_cached_uses_snapshot_until_stale() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_counts_cached";
    let schema = "awa_qs_counts_cached";
    let store = create_store(&pool, schema).await;

    let initial = store
        .queue_counts_cached(&pool, queue, Duration::from_secs(60))
        .await
        .expect("Failed to populate initial queue count snapshot");
    assert_eq!(initial.available, 0);
    assert_eq!(initial.running, 0);
    assert_eq!(initial.completed, 0);

    store
        .enqueue_batch(&pool, queue, 1, 1)
        .await
        .expect("Failed to enqueue queued job");

    let stale = store
        .queue_counts_cached(&pool, queue, Duration::from_secs(60))
        .await
        .expect("Failed to read cached queue counts");
    assert_eq!(stale.available, 0);
    assert_eq!(stale.running, 0);
    assert_eq!(stale.completed, 0);

    sqlx::query(&format!(
        r#"
        UPDATE {schema}.queue_count_snapshots
        SET refreshed_at = now() - interval '1 hour'
        WHERE queue = $1
        "#
    ))
    .bind(queue)
    .execute(&pool)
    .await
    .expect("Failed to age queue count snapshot");

    let refreshed = store
        .queue_counts_cached(&pool, queue, Duration::from_secs(60))
        .await
        .expect("Failed to refresh queue counts from exact query");
    assert_eq!(refreshed.available, 1);
    assert_eq!(refreshed.running, 0);
    assert_eq!(refreshed.completed, 0);

    let exact = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to read exact queue counts");
    assert_eq!(exact, refreshed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_queue_counts_and_claims_aggregate_across_stripes() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_striped_counts";
    let schema = "awa_qs_striped_counts";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 4,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(store.queue_stripe_count(), 4);

    store
        .enqueue_batch(&pool, queue, 1, 8)
        .await
        .expect("Failed to enqueue striped jobs");

    let physical_queues: Vec<String> = sqlx::query_scalar(&format!(
        r#"
        SELECT DISTINCT queue
        FROM {schema}.ready_entries
        ORDER BY queue
        "#
    ))
    .fetch_all(&pool)
    .await
    .expect("Failed to read physical stripe queues");
    assert!(
        physical_queues.len() > 1,
        "expected jobs to span multiple physical queues, got {physical_queues:?}"
    );
    assert!(
        physical_queues
            .iter()
            .all(|physical_queue| physical_queue.starts_with(&format!("{queue}#"))),
        "expected physical striped queue names, got {physical_queues:?}"
    );

    let counts = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to aggregate queue counts across stripes");
    assert_eq!(counts.available, 8);
    assert_eq!(counts.running, 0);
    assert_eq!(counts.completed, 0);

    let claimed = store
        .claim_batch(&pool, queue, 8)
        .await
        .expect("Failed to claim striped logical queue");
    assert_eq!(claimed.len(), 8);
    assert!(
        claimed
            .iter()
            .all(|entry| entry.queue.starts_with(&format!("{queue}#"))),
        "expected physical striped queue names on claimed entries: {claimed:?}"
    );

    let counts_after = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to read queue counts after striped claim");
    assert_eq!(counts_after.available, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_striped_claims_probe_stripes_round_robin() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_striped_round_robin";
    let schema = "awa_qs_striped_round_robin";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 2,
            ..Default::default()
        },
    )
    .await;

    store
        .enqueue_batch(&pool, queue, 1, 4)
        .await
        .expect("Failed to enqueue striped jobs");

    let mut claimed_queues = Vec::new();
    for _ in 0..4 {
        let claimed = store
            .claim_batch(&pool, queue, 1)
            .await
            .expect("Failed to claim striped logical queue");
        assert_eq!(claimed.len(), 1);
        claimed_queues.push(claimed[0].queue.clone());
    }

    assert_eq!(
        claimed_queues,
        vec![
            format!("{queue}#0"),
            format!("{queue}#1"),
            format!("{queue}#0"),
            format!("{queue}#1"),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_claim_runtime_does_not_wait_for_lease_rotation_lock() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_claim_lease_lock";
    let schema = "awa_qs_runtime_claim_lease_lock";
    let store = create_store(&pool, schema).await;

    store
        .enqueue_batch(&pool, queue, 1, 1)
        .await
        .expect("Failed to enqueue lease-lock job");

    let mut lock_tx = pool.begin().await.expect("Failed to begin lease lock tx");
    sqlx::query(&format!(
        r#"
        SELECT current_slot
        FROM {schema}.lease_ring_state
        WHERE singleton = TRUE
        FOR UPDATE
        "#
    ))
    .execute(lock_tx.as_mut())
    .await
    .expect("Failed to lock lease ring state");

    let claimed_while_locked = tokio::time::timeout(
        Duration::from_millis(200),
        store.claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30)),
    )
    .await;
    let claimed_while_locked = claimed_while_locked
        .expect("claim should not block on lease ring state lock")
        .expect("claim should succeed while lease ring state is locked");
    assert_eq!(claimed_while_locked.len(), 1);

    lock_tx
        .rollback()
        .await
        .expect("Failed to release lease ring lock");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_claim_runtime_applies_priority_aging_dynamically() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_dynamic_priority_aging";
    let schema = "awa_qs_dynamic_priority_aging";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            experimental_lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let aging_interval = Duration::from_secs(60);
    let aged_job_id = enqueue_job(
        &pool,
        &store,
        &RetryJob { id: 1 },
        InsertOpts {
            queue: queue.into(),
            priority: 4,
            ..Default::default()
        },
    )
    .await;

    sqlx::query(&format!(
        "UPDATE {schema}.ready_entries SET run_at = $1 WHERE job_id = $2"
    ))
    .bind(Utc::now() - chrono::Duration::seconds(aging_interval.as_secs() as i64 * 4))
    .bind(aged_job_id)
    .execute(&pool)
    .await
    .expect("Failed to backdate aged queue storage job");

    let fresh_high_priority_job_id = enqueue_job(
        &pool,
        &store,
        &RetryJob { id: 2 },
        InsertOpts {
            queue: queue.into(),
            priority: 1,
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch_with_aging(&pool, queue, 1, Duration::ZERO, aging_interval)
        .await
        .expect("Failed to claim aged queue storage job");

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.id, aged_job_id);
    assert_ne!(claimed[0].job.id, fresh_high_priority_job_id);
    assert_eq!(claimed[0].claim.priority, 4);
    assert_eq!(claimed[0].job.priority, 1);
    assert_eq!(
        claimed[0]
            .job
            .metadata
            .get("_awa_original_priority")
            .and_then(|value| value.as_i64()),
        Some(4)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_bounded_claimers_limit_active_claimers_per_queue() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let schema = "awa_qs_bounded_claimers_limit";
    let store = create_store(&pool, schema).await;
    let queue = "qs_bounded_claimers_limit";
    let instance_a = Uuid::new_v4();
    let instance_b = Uuid::new_v4();
    let ttl = Duration::from_secs(3);
    let idle_threshold = Duration::from_millis(500);

    let lease_a = store
        .acquire_queue_claimer(&pool, queue, instance_a, 1, ttl, idle_threshold)
        .await
        .expect("instance A should acquire claimer")
        .expect("instance A should get a claimer slot");
    assert_eq!(lease_a.claimer_slot, 0);

    let lease_b = store
        .acquire_queue_claimer(&pool, queue, instance_b, 1, ttl, idle_threshold)
        .await
        .expect("instance B acquire should succeed");
    assert!(
        lease_b.is_none(),
        "bounded claimers should block extra owners"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_bounded_claimers_can_steal_idle_slot() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let schema = "awa_qs_bounded_claimers_idle";
    let store = create_store(&pool, schema).await;
    let queue = "qs_bounded_claimers_idle";
    let instance_a = Uuid::new_v4();
    let instance_b = Uuid::new_v4();
    let ttl = Duration::from_secs(3);
    let idle_threshold = Duration::from_millis(500);

    let lease_a = store
        .acquire_queue_claimer(&pool, queue, instance_a, 1, ttl, idle_threshold)
        .await
        .expect("instance A should acquire claimer")
        .expect("instance A should get a claimer slot");

    sqlx::query(&format!(
        "UPDATE {schema}.queue_claimer_leases SET last_claimed_at = $1 WHERE queue = $2 AND claimer_slot = $3"
    ))
    .bind(Utc::now() - chrono::Duration::milliseconds(1_000))
    .bind(queue)
    .bind(lease_a.claimer_slot)
    .execute(&pool)
    .await
    .expect("failed to age claimer lease idle");

    let lease_b = store
        .acquire_queue_claimer(&pool, queue, instance_b, 1, ttl, idle_threshold)
        .await
        .expect("instance B should acquire idle claimer")
        .expect("instance B should steal idle claimer slot");

    assert_eq!(lease_b.claimer_slot, lease_a.claimer_slot);
    assert!(
        lease_b.lease_epoch > lease_a.lease_epoch,
        "stealing should bump the lease epoch"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_prune_oldest_blocks_on_reader_lock() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_prune_reader_lock";
    let schema = "awa_qs_runtime_prune_reader_lock";
    let store = create_store(&pool, schema).await;

    store
        .enqueue_batch(&pool, queue, 1, 1)
        .await
        .expect("Failed to enqueue prune-reader job");
    let claimed = store
        .claim_batch(&pool, queue, 1)
        .await
        .expect("Failed to claim prune-reader job");
    assert_eq!(claimed.len(), 1);
    let completed = store
        .complete_batch(&pool, &claimed)
        .await
        .expect("Failed to complete prune-reader job");
    assert_eq!(completed, 1);

    let rotated = store
        .rotate(&pool)
        .await
        .expect("Failed to rotate queue ring for prune-reader test");
    assert!(
        matches!(rotated, RotateOutcome::Rotated { slot: 1, .. }),
        "unexpected rotate outcome: {rotated:?}"
    );

    let mut reader_tx = pool.begin().await.expect("Failed to begin reader lock tx");
    sqlx::query(&format!(
        "LOCK TABLE {schema}.ready_entries_0, {schema}.done_entries_0 IN ACCESS SHARE MODE"
    ))
    .execute(reader_tx.as_mut())
    .await
    .expect("Failed to lock ready/done reader tables");

    let blocked = store
        .prune_oldest(&pool)
        .await
        .expect("Failed to prune while reader lock held");
    assert!(
        matches!(blocked, PruneOutcome::Blocked { slot: 0 }),
        "unexpected prune outcome while reader lock held: {blocked:?}"
    );

    reader_tx
        .rollback()
        .await
        .expect("Failed to release reader lock");

    let pruned = store
        .prune_oldest(&pool)
        .await
        .expect("Failed to prune after reader lock release");
    assert!(
        matches!(pruned, PruneOutcome::Pruned { slot: 0 }),
        "unexpected prune outcome after reader lock release: {pruned:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_runtime_complete_external() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_callback_complete";
    let schema = "awa_qs_runtime_callback";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &CallbackJob { id: 3 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        },
        CallbackWorker {
            timeout: Duration::from_secs(30),
        },
    );
    client
        .start()
        .await
        .expect("Failed to start callback client");

    let waiting = wait_for_callback_job(&store, &pool, job_id, Duration::from_secs(10)).await;
    let callback_id = waiting
        .callback_id
        .expect("waiting job should have callback id");

    let completed = admin::complete_external(
        &pool,
        callback_id,
        Some(serde_json::json!({"ok": true})),
        None,
    )
    .await
    .expect("Failed to complete external callback");
    assert_eq!(completed.state, JobState::Completed);

    let stored = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(stored.state, JobState::Completed);
    assert!(stored.callback_id.is_none());

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_runtime_terminal_failure_moves_to_dlq() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_terminal_dlq";
    let schema = "awa_qs_runtime_dlq_terminal";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &DlqJob { id: 4 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: schema.to_string(),
                queue_slot_count: 4,
                lease_slot_count: 2,
                ..Default::default()
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .register_worker(TerminalFailureWorker)
        .dlq_enabled_by_default(true)
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(25))
        .build()
        .expect("Failed to build terminal dlq client");
    client
        .start()
        .await
        .expect("Failed to start terminal dlq client");

    let failed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(dlq_count(&pool, &store, queue).await, 1);
    assert_eq!(failed_done_count(&pool, &store, queue).await, 0);
    assert_eq!(dlq_reason(&pool, &store, job_id).await, "terminal_error");

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_runtime_callback_timeout_moves_to_dlq() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_callback_dlq";
    let schema = "awa_qs_runtime_dlq_callback";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &CallbackJob { id: 5 },
        InsertOpts {
            queue: queue.to_string(),
            max_attempts: 1,
            ..Default::default()
        },
    )
    .await;

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: schema.to_string(),
                queue_slot_count: 4,
                lease_slot_count: 2,
                ..Default::default()
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .register_worker(CallbackWorker {
            timeout: Duration::from_millis(100),
        })
        .dlq_enabled_by_default(true)
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(25))
        .build()
        .expect("Failed to build callback dlq client");
    client
        .start()
        .await
        .expect("Failed to start callback dlq client");

    let waiting = wait_for_callback_job(&store, &pool, job_id, Duration::from_secs(10)).await;
    assert!(waiting.callback_timeout_at.is_some());

    let failed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(failed.state, JobState::Failed);
    let dlq_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if dlq_count(&pool, &store, queue).await == 1
            && failed_done_count(&pool, &store, queue).await == 0
        {
            break;
        }
        assert!(
            Instant::now() <= dlq_deadline,
            "timed out waiting for callback timeout failure to move into DLQ"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(dlq_reason(&pool, &store, job_id).await, "callback_timeout");

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_dlq_api_round_trip() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_dlq_api";
    let schema = "awa_qs_runtime_dlq_api";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &DlqJob { id: 6 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: schema.to_string(),
                queue_slot_count: 4,
                lease_slot_count: 2,
                ..Default::default()
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .register_worker(TerminalFailureWorker)
        .dlq_enabled_by_default(true)
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(25))
        .build()
        .expect("Failed to build dlq api client");
    client
        .start()
        .await
        .expect("Failed to start dlq api client");

    let failed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(failed.state, JobState::Failed);

    client.shutdown(Duration::from_secs(5)).await;

    let dlq_entry = awa::model::dlq::get_dlq_job(&pool, job_id)
        .await
        .expect("Failed to fetch dlq job")
        .expect("dlq job should exist");
    assert_eq!(dlq_entry.reason, "terminal_error");

    let dump = admin::dump_job(&pool, job_id)
        .await
        .expect("Failed to dump dlq job");
    let dlq_meta = dump.dlq.expect("dump should include dlq metadata");
    assert_eq!(dlq_meta.reason, "terminal_error");
    assert!(
        !dump.summary.can_retry,
        "dlq rows should not advertise the live-job retry action"
    );

    let dlq_list = awa::model::dlq::list_dlq(
        &pool,
        &awa::model::ListDlqFilter {
            queue: Some(queue.to_string()),
            ..Default::default()
        },
    )
    .await
    .expect("Failed to list dlq rows");
    assert_eq!(dlq_list.len(), 1);
    assert_eq!(
        awa::model::dlq::dlq_depth(&pool, Some(queue))
            .await
            .expect("Failed to sample dlq depth"),
        1
    );

    let revived =
        awa::model::dlq::retry_from_dlq(&pool, job_id, &awa::model::RetryFromDlqOpts::default())
            .await
            .expect("Failed to retry dlq job")
            .expect("retry should return a revived job");
    assert_eq!(revived.state, JobState::Available);
    assert_eq!(revived.attempt, 0);
    assert_eq!(
        awa::model::dlq::dlq_depth(&pool, Some(queue))
            .await
            .expect("Failed to resample dlq depth"),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_dlq_bulk_move_and_bulk_retry() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_dlq_bulk_ops";
    let schema = "awa_qs_runtime_dlq_bulk_ops";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &DlqJob { id: 7 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        },
        TerminalFailureWorker,
    );
    client
        .start()
        .await
        .expect("Failed to start bulk move client");

    let failed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(failed_done_count(&pool, &store, queue).await, 1);
    assert_eq!(dlq_count(&pool, &store, queue).await, 0);

    client.shutdown(Duration::from_secs(5)).await;

    let move_err = awa::model::dlq::bulk_move_failed_to_dlq(&pool, None, None, "ops_move", false)
        .await
        .expect_err("bulk move without scope should be rejected");
    assert!(matches!(move_err, AwaError::Validation(_)));

    let moved = awa::model::dlq::bulk_move_failed_to_dlq(&pool, None, None, "ops_move", true)
        .await
        .expect("Failed to bulk-move failed rows into the DLQ");
    assert_eq!(moved, 1);
    assert_eq!(failed_done_count(&pool, &store, queue).await, 0);
    assert_eq!(dlq_count(&pool, &store, queue).await, 1);

    let empty_filter = awa::model::ListDlqFilter::default();
    let retry_err = awa::model::dlq::bulk_retry_from_dlq(&pool, &empty_filter, false)
        .await
        .expect_err("bulk retry without scope should be rejected");
    assert!(matches!(retry_err, AwaError::Validation(_)));

    let retried = awa::model::dlq::bulk_retry_from_dlq(&pool, &empty_filter, true)
        .await
        .expect("Failed to bulk-retry DLQ rows");
    assert_eq!(retried, 1);
    assert_eq!(dlq_count(&pool, &store, queue).await, 0);

    let revived = admin::get_job(&pool, job_id)
        .await
        .expect("Failed to load revived job");
    assert_eq!(revived.state, JobState::Available);
    assert_eq!(revived.attempt, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_dlq_purge_guard_and_filtered_purge() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_dlq_purge_guard";
    let schema = "awa_qs_runtime_dlq_purge_guard";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &DlqJob { id: 8 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: schema.to_string(),
                queue_slot_count: 4,
                lease_slot_count: 2,
                ..Default::default()
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .register_worker(TerminalFailureWorker)
        .dlq_enabled_by_default(true)
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(25))
        .build()
        .expect("Failed to build purge-guard client");
    client
        .start()
        .await
        .expect("Failed to start purge-guard client");

    let failed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(dlq_count(&pool, &store, queue).await, 1);

    client.shutdown(Duration::from_secs(5)).await;

    let empty_filter = awa::model::ListDlqFilter::default();
    let purge_err = awa::model::dlq::purge_dlq(&pool, &empty_filter, false)
        .await
        .expect_err("purge without scope should be rejected");
    assert!(matches!(purge_err, AwaError::Validation(_)));

    let purged = awa::model::dlq::purge_dlq(&pool, &empty_filter, true)
        .await
        .expect("Failed to purge filtered DLQ rows");
    assert_eq!(purged, 1);
    assert_eq!(dlq_count(&pool, &store, queue).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_retry_from_dlq_surfaces_unique_conflict() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_dlq_unique_conflict";
    let schema = "awa_qs_runtime_dlq_unique_conflict";
    let store = create_store(&pool, schema).await;
    let opts = InsertOpts {
        queue: queue.to_string(),
        unique: Some(UniqueOpts {
            by_queue: true,
            by_args: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    let original_id = enqueue_job(&pool, &store, &DlqJob { id: 9 }, opts.clone()).await;

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        },
        TerminalFailureWorker,
    );
    client
        .start()
        .await
        .expect("Failed to start unique-conflict client");

    let failed = wait_for_job_state(
        &store,
        &pool,
        original_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(failed.state, JobState::Failed);
    client.shutdown(Duration::from_secs(5)).await;

    let moved = awa::model::dlq::move_failed_to_dlq(&pool, original_id, "unique_conflict")
        .await
        .expect("Failed to move failed row into the DLQ");
    assert!(moved.is_some(), "original row should land in the DLQ");

    let replacement_id = enqueue_job(&pool, &store, &DlqJob { id: 9 }, opts).await;
    assert_ne!(replacement_id, original_id);

    let retry_err = awa::model::dlq::retry_from_dlq(
        &pool,
        original_id,
        &awa::model::RetryFromDlqOpts::default(),
    )
    .await
    .expect_err("retry must fail while replacement holds the unique claim");
    assert!(matches!(retry_err, AwaError::UniqueConflict { .. }));

    let dlq_entry = awa::model::dlq::get_dlq_job(&pool, original_id)
        .await
        .expect("Failed to fetch DLQ row after unique conflict");
    assert!(
        dlq_entry.is_some(),
        "DLQ row should survive the failed retry"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_admin_bulk_retry_rolls_back_on_unique_conflict() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_admin_bulk_retry_atomic";
    let schema = "awa_qs_admin_bulk_retry_atomic";
    let store = create_store(&pool, schema).await;
    let opts = available_unique_insert_opts(queue);

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        },
        TerminalFailureWorker,
    );
    client
        .start()
        .await
        .expect("Failed to start bulk-retry atomicity client");

    let first_id = enqueue_job(&pool, &store, &DlqJob { id: 91 }, opts.clone()).await;
    let first_failed = wait_for_job_state(
        &store,
        &pool,
        first_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(first_failed.state, JobState::Failed);

    let second_id = enqueue_job(&pool, &store, &DlqJob { id: 91 }, opts).await;
    let second_failed = wait_for_job_state(
        &store,
        &pool,
        second_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(second_failed.state, JobState::Failed);
    client.shutdown(Duration::from_secs(5)).await;

    let retry_err = admin::bulk_retry(&pool, &[first_id, second_id])
        .await
        .expect_err("bulk_retry must fail atomically on unique conflict");
    assert!(matches!(retry_err, AwaError::UniqueConflict { .. }));

    let first_after = store
        .load_job(&pool, first_id)
        .await
        .expect("Failed to reload first failed job")
        .expect("First failed job missing after retry rollback");
    let second_after = store
        .load_job(&pool, second_id)
        .await
        .expect("Failed to reload second failed job")
        .expect("Second failed job missing after retry rollback");
    assert_eq!(first_after.state, JobState::Failed);
    assert_eq!(second_after.state, JobState::Failed);
    assert_eq!(failed_done_count(&pool, &store, queue).await, 2);
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("Failed to sample queue counts after retry rollback")
            .available,
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_admin_retry_failed_by_kind_rolls_back_on_unique_conflict() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_admin_retry_kind_atomic";
    let schema = "awa_qs_admin_retry_kind_atomic";
    let store = create_store(&pool, schema).await;
    let opts = available_unique_insert_opts(queue);

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        },
        TerminalFailureWorker,
    );
    client
        .start()
        .await
        .expect("Failed to start retry-by-kind atomicity client");

    let first_id = enqueue_job(&pool, &store, &DlqJob { id: 92 }, opts.clone()).await;
    let first_failed = wait_for_job_state(
        &store,
        &pool,
        first_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(first_failed.state, JobState::Failed);

    let second_id = enqueue_job(&pool, &store, &DlqJob { id: 92 }, opts).await;
    let second_failed = wait_for_job_state(
        &store,
        &pool,
        second_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(second_failed.state, JobState::Failed);
    client.shutdown(Duration::from_secs(5)).await;

    let retry_err = admin::retry_failed_by_kind(&pool, TerminalFailureWorker.kind())
        .await
        .expect_err("retry_failed_by_kind must fail atomically on unique conflict");
    assert!(matches!(retry_err, AwaError::UniqueConflict { .. }));

    let first_after = store
        .load_job(&pool, first_id)
        .await
        .expect("Failed to reload first failed job")
        .expect("First failed job missing after retry rollback");
    let second_after = store
        .load_job(&pool, second_id)
        .await
        .expect("Failed to reload second failed job")
        .expect("Second failed job missing after retry rollback");
    assert_eq!(first_after.state, JobState::Failed);
    assert_eq!(second_after.state, JobState::Failed);
    assert_eq!(failed_done_count(&pool, &store, queue).await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_admin_discard_failed_releases_unique_claims_from_done() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_discard_failed_done";
    let schema = "awa_qs_discard_failed_done";
    let store = create_store(&pool, schema).await;
    let opts = failed_unique_insert_opts(queue);
    let job_id = enqueue_job(&pool, &store, &DlqJob { id: 7 }, opts.clone()).await;

    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            ..Default::default()
        },
        TerminalFailureWorker,
    );
    client
        .start()
        .await
        .expect("Failed to start discard-failed client");

    let failed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(failed.state, JobState::Failed);
    client.shutdown(Duration::from_secs(5)).await;

    assert_eq!(failed_done_count(&pool, &store, queue).await, 1);
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("Failed to sample queue counts")
            .completed,
        1
    );

    let discarded = admin::discard_failed(&pool, TerminalFailureWorker.kind())
        .await
        .expect("Failed to discard failed jobs");
    assert_eq!(discarded, 1);
    assert_eq!(failed_done_count(&pool, &store, queue).await, 0);
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("Failed to resample queue counts")
            .completed,
        0
    );

    let reinserted = insert::insert_with(&pool, &DlqJob { id: 7 }, opts)
        .await
        .expect("discard_failed should release failed-state unique claims");
    assert_eq!(reinserted.state, JobState::Available);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_admin_discard_failed_releases_unique_claims_from_dlq() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_discard_failed_dlq";
    let schema = "awa_qs_discard_failed_dlq";
    let store = create_store(&pool, schema).await;
    let opts = failed_unique_insert_opts(queue);
    let job_id = enqueue_job(&pool, &store, &DlqJob { id: 8 }, opts.clone()).await;

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: schema.to_string(),
                queue_slot_count: 4,
                lease_slot_count: 2,
                ..Default::default()
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .register_worker(TerminalFailureWorker)
        .dlq_enabled_by_default(true)
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(25))
        .build()
        .expect("Failed to build discard-failed dlq client");
    client
        .start()
        .await
        .expect("Failed to start discard-failed dlq client");

    let failed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(failed.state, JobState::Failed);
    client.shutdown(Duration::from_secs(5)).await;

    assert_eq!(dlq_count(&pool, &store, queue).await, 1);

    let discarded = admin::discard_failed(&pool, TerminalFailureWorker.kind())
        .await
        .expect("Failed to discard dlq jobs");
    assert_eq!(discarded, 1);
    assert_eq!(dlq_count(&pool, &store, queue).await, 0);

    let reinserted = insert::insert_with(&pool, &DlqJob { id: 8 }, opts)
        .await
        .expect("discard_failed should release dlq unique claims");
    assert_eq!(reinserted.state, JobState::Available);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_jobs_view_insert_select_delete_compat() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let queue = "qs_jobs_view_compat";
    let schema = "awa_qs_jobs_view_compat";
    let store = create_store(&pool, schema).await;

    let available_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO awa.jobs (kind, queue, args, state, metadata, tags)
        VALUES ($1, $2, $3, 'available', $4, $5)
        RETURNING id
        "#,
    )
    .bind("raw_view_available")
    .bind(queue)
    .bind(serde_json::json!({"id": 9}))
    .bind(serde_json::json!({"source": "raw_view"}))
    .bind(vec!["raw".to_string()])
    .fetch_one(&pool)
    .await
    .expect("Failed to insert available row through awa.jobs");

    let scheduled_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO awa.jobs (kind, queue, args, state, run_at)
        VALUES ($1, $2, $3, 'scheduled', now() + interval '5 minutes')
        RETURNING id
        "#,
    )
    .bind("raw_view_scheduled")
    .bind(queue)
    .bind(serde_json::json!({"id": 10}))
    .fetch_one(&pool)
    .await
    .expect("Failed to insert scheduled row through awa.jobs");

    let jobs: Vec<JobRow> = sqlx::query_as("SELECT * FROM awa.jobs WHERE queue = $1 ORDER BY id")
        .bind(queue)
        .fetch_all(&pool)
        .await
        .expect("Failed to read queue_storage rows through awa.jobs");
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].id, available_id);
    assert_eq!(jobs[0].state, JobState::Available);
    assert_eq!(jobs[0].metadata["source"], serde_json::json!("raw_view"));
    assert_eq!(jobs[0].tags, vec!["raw".to_string()]);
    assert_eq!(jobs[1].id, scheduled_id);
    assert_eq!(jobs[1].state, JobState::Scheduled);

    let ready_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.ready_entries WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to count ready entries");
    assert_eq!(ready_count, 1);

    let deferred_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.deferred_jobs WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to count deferred rows");
    assert_eq!(deferred_count, 1);

    let deleted = sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(&pool)
        .await
        .expect("Failed to delete queue_storage rows through awa.jobs")
        .rows_affected();

    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM awa.jobs WHERE queue = $1")
            .bind(queue)
            .fetch_one(&pool)
            .await
            .expect("Failed to count remaining awa.jobs rows");
    let ready_after_delete: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.ready_entries WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to recount ready entries");
    let deferred_after_delete: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.deferred_jobs WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to recount deferred rows");
    assert_eq!(remaining, 0);
    assert_eq!(ready_after_delete, 0);
    assert_eq!(deferred_after_delete, 0);
    assert_eq!(
        deleted, 0,
        "Postgres reports zero rows for this INSTEAD OF DELETE view path even when the underlying queue rows are removed"
    );
}
