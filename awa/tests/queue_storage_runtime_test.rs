//! Integration tests for queue_storage runtime flows.
//!
//! These tests exercise the full dispatcher/worker/maintenance wiring with the
//! queue_storage backend enabled.

use awa::model::{admin, insert, migrations, QueueStorage, QueueStorageConfig};
use awa::{
    Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, JobRow, JobState, QueueConfig,
    Worker,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

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
        Err(err) => panic!(
            "Failed to create queue_storage test database {database_name}: {err}"
        ),
    }
}

async fn setup_pool(max_connections: u32) -> sqlx::PgPool {
    let url = database_url();
    ensure_database_exists(&url).await;
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

async fn create_store(pool: &sqlx::PgPool, schema: &str) -> QueueStorage {
    let store = QueueStorage::new(QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
    })
    .expect("Failed to create queue_storage store");
    recreate_store_schema(pool, &store).await;
    store.install(pool).await.expect("Failed to install store");
    store.reset(pool).await.expect("Failed to reset store");
    store
}

async fn attempt_state_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    let sql = format!("SELECT count(*)::bigint FROM {}.attempt_state", store.schema());
    sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .expect("Failed to count attempt_state rows")
}

fn queue_storage_client<W: Worker + 'static>(
    pool: &sqlx::PgPool,
    queue: &str,
    store_config: QueueStorageConfig,
    worker: W,
) -> Client {
    Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 4,
                poll_interval: Duration::from_millis(25),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            store_config,
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
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
    let params = [insert::params_with(args, opts.clone()).expect("Failed to build insert params")];
    store
        .enqueue_params_batch(pool, &params)
        .await
        .expect("Failed to enqueue queue_storage job");

    let query = if opts.run_at.is_some() {
        format!(
            "SELECT job_id FROM {}.deferred_jobs WHERE queue = $1 ORDER BY job_id DESC LIMIT 1",
            store.schema()
        )
    } else {
        format!(
            "SELECT job_id FROM {}.ready_entries WHERE queue = $1 ORDER BY job_id DESC LIMIT 1",
            store.schema()
        )
    };

    sqlx::query_scalar::<_, i64>(&query)
        .bind(&opts.queue)
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

    client
        .shutdown(Duration::from_secs(5))
        .await;
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
    assert_eq!(dlq_count(&pool, &store, queue).await, 1);
    assert_eq!(failed_done_count(&pool, &store, queue).await, 0);
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

    let remaining: i64 = sqlx::query_scalar("SELECT count(*)::bigint FROM awa.jobs WHERE queue = $1")
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
