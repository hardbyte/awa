//! Ignored soak and burst tests for the queue_storage runtime backend.
//!
//! These focus on longer mixed workloads and large terminal-failure bursts,
//! with periodic dead-tuple sampling so we can watch whether churn stays
//! confined to the small hot tables.

use async_trait::async_trait;
use awa::model::{insert, migrations, QueueStorage, QueueStorageConfig};
use awa::{Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

static QUEUE_STORAGE_SOAK_LOCK: LazyLock<AsyncMutex<()>> = LazyLock::new(|| AsyncMutex::new(()));

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
        "queue_storage soak database names must use only [A-Za-z0-9_]"
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
        .expect("Failed to connect to admin database for queue_storage soak tests");
    let create_sql = format!("CREATE DATABASE {database_name}");
    match sqlx::query(&create_sql).execute(&admin_pool).await {
        Ok(_) => {}
        Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P04") => {}
        Err(err) => {
            panic!("Failed to create queue_storage soak database {database_name}: {err}")
        }
    }
}

async fn setup_pool(max_connections: u32) -> sqlx::PgPool {
    let url = database_url();
    ensure_database_exists(&url).await;
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&url)
        .await
        .expect("Failed to connect to queue_storage soak database");
    migrations::run(&pool)
        .await
        .expect("Failed to run migrations");
    pool
}

async fn ensure_pgstattuple(pool: &sqlx::PgPool) {
    let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS pgstattuple")
        .execute(pool)
        .await;
}

async fn recreate_store_schema(pool: &sqlx::PgPool, store: &QueueStorage) {
    let drop_sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", store.schema());
    sqlx::query(&drop_sql)
        .execute(pool)
        .await
        .expect("Failed to drop queue_storage soak schema");
}

async fn create_store(
    pool: &sqlx::PgPool,
    schema: &str,
    queue_slot_count: usize,
    lease_slot_count: usize,
) -> QueueStorage {
    let store = QueueStorage::new(QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count,
        lease_slot_count,
    })
    .expect("Failed to create queue_storage soak store");
    recreate_store_schema(pool, &store).await;
    store.install(pool).await.expect("Failed to install store");
    store.reset(pool).await.expect("Failed to reset store");
    store
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

async fn queue_state_counts(pool: &sqlx::PgPool, queue: &str) -> HashMap<String, i64> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"
        SELECT state::text, count(*)::bigint
        FROM awa.jobs
        WHERE queue = $1
        GROUP BY state
        "#,
    )
    .bind(queue)
    .fetch_all(pool)
    .await
    .expect("Failed to fetch queue_storage soak queue counts");

    rows.into_iter().collect()
}

fn count_state(counts: &HashMap<String, i64>, state: &str) -> i64 {
    counts.get(state).copied().unwrap_or(0)
}

fn in_flight_count(counts: &HashMap<String, i64>) -> i64 {
    count_state(counts, "available")
        + count_state(counts, "running")
        + count_state(counts, "retryable")
        + count_state(counts, "scheduled")
        + count_state(counts, "waiting_external")
}

fn finalized_count(counts: &HashMap<String, i64>) -> i64 {
    count_state(counts, "completed")
        + count_state(counts, "failed")
        + count_state(counts, "cancelled")
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

async fn dlq_depth(pool: &sqlx::PgPool, store: &QueueStorage, queue: &str) -> i64 {
    let sql = format!(
        "SELECT count(*)::bigint FROM {}.dlq_entries WHERE queue = $1",
        store.schema()
    );
    sqlx::query_scalar::<_, i64>(&sql)
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("Failed to count dlq rows")
}

async fn sample_dead_tuples(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
    schema: &str,
    relname_filter: &str,
) -> i64 {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE(sum(n_dead_tup), 0)::bigint
        FROM pg_stat_user_tables
        WHERE schemaname = $1
          AND relname LIKE $2
        "#,
    )
    .bind(schema)
    .bind(relname_filter)
    .fetch_one(conn.as_mut())
    .await
    .expect("Failed to sample dead tuples")
}

async fn sample_pgstattuple_dead_tuples(
    pool: &sqlx::PgPool,
    schema: &str,
    relname_filter: &str,
) -> i64 {
    let mut conn = pool
        .acquire()
        .await
        .expect("Failed to acquire pgstattuple connection");

    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE(sum((pgstattuple(c.oid::regclass)).dead_tuple_count), 0)::bigint
        FROM pg_class AS c
        INNER JOIN pg_namespace AS n
            ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relkind = 'r'
          AND c.relname LIKE $2
        "#,
    )
    .bind(schema)
    .bind(relname_filter)
    .fetch_one(conn.as_mut())
    .await
    .expect("Failed to sample exact dead tuples")
}

#[derive(Debug, Clone, Copy, Default)]
struct DeadTupleCounts {
    queue_lanes: i64,
    ready: i64,
    done: i64,
    leases: i64,
    attempt_state: i64,
}

impl DeadTupleCounts {
    fn total(self) -> i64 {
        self.queue_lanes + self.ready + self.done + self.leases + self.attempt_state
    }
}

async fn estimated_dead_tuples(pool: &sqlx::PgPool, store: &QueueStorage) -> DeadTupleCounts {
    let mut conn = pool
        .acquire()
        .await
        .expect("Failed to acquire dead-tuple connection");
    DeadTupleCounts {
        queue_lanes: sample_dead_tuples(&mut conn, store.schema(), "queue_lanes").await,
        ready: sample_dead_tuples(&mut conn, store.schema(), "ready_entries%").await,
        done: sample_dead_tuples(&mut conn, store.schema(), "done_entries%").await,
        leases: sample_dead_tuples(&mut conn, store.schema(), "leases%").await,
        attempt_state: sample_dead_tuples(&mut conn, store.schema(), "attempt_state").await,
    }
}

async fn exact_dead_tuples(pool: &sqlx::PgPool, store: &QueueStorage) -> DeadTupleCounts {
    DeadTupleCounts {
        queue_lanes: sample_pgstattuple_dead_tuples(pool, store.schema(), "queue_lanes").await,
        ready: sample_pgstattuple_dead_tuples(pool, store.schema(), "ready_entries%").await,
        done: sample_pgstattuple_dead_tuples(pool, store.schema(), "done_entries%").await,
        leases: sample_pgstattuple_dead_tuples(pool, store.schema(), "leases%").await,
        attempt_state: sample_pgstattuple_dead_tuples(pool, store.schema(), "attempt_state").await,
    }
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct MixedSoakJob {
    seq: i64,
    mode: String,
}

#[derive(Default)]
struct MixedWorkloadState {
    snoozed_once: Mutex<HashSet<i64>>,
    handler_count: AtomicU64,
}

struct MixedWorkloadWorker {
    state: Arc<MixedWorkloadState>,
}

#[async_trait]
impl Worker for MixedWorkloadWorker {
    fn kind(&self) -> &'static str {
        "mixed_soak_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        self.state.handler_count.fetch_add(1, Ordering::Relaxed);

        let args: MixedSoakJob = serde_json::from_value(ctx.job.args.clone())
            .map_err(|err| JobError::terminal(format!("failed to decode soak args: {err}")))?;

        match args.mode.as_str() {
            "complete" => Ok(JobResult::Completed),
            "retry_once" => {
                if ctx.job.attempt == 1 {
                    Ok(JobResult::RetryAfter(Duration::from_millis(50)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "snooze_once" => {
                let first_time = {
                    let mut seen = self
                        .state
                        .snoozed_once
                        .lock()
                        .expect("snooze state mutex poisoned");
                    seen.insert(ctx.job.id)
                };
                if first_time {
                    Ok(JobResult::Snooze(Duration::from_millis(100)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "terminal_fail" => Err(JobError::terminal("intentional terminal soak failure")),
            "callback_timeout" => {
                if ctx.job.attempt == 1 {
                    let callback = ctx
                        .register_callback(Duration::from_millis(250))
                        .await
                        .map_err(JobError::retryable)?;
                    Ok(JobResult::WaitForCallback(callback))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            "deadline_hang" => {
                if ctx.job.attempt == 1 {
                    let started = Instant::now();
                    loop {
                        if ctx.is_cancelled() {
                            break;
                        }
                        if started.elapsed() > Duration::from_secs(5) {
                            return Err(JobError::terminal(
                                "deadline rescue did not cancel hanging soak job",
                            ));
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Ok(JobResult::RetryAfter(Duration::from_millis(50)))
                } else {
                    Ok(JobResult::Completed)
                }
            }
            other => Err(JobError::terminal(format!("unknown soak mode: {other}"))),
        }
    }
}

fn build_client(
    pool: &sqlx::PgPool,
    queue: &str,
    schema: &str,
    max_workers: u32,
    queue_slot_count: usize,
    lease_slot_count: usize,
    worker: MixedWorkloadWorker,
) -> Client {
    Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers,
                poll_interval: Duration::from_millis(25),
                deadline_duration: Duration::from_millis(200),
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: schema.to_string(),
                queue_slot_count,
                lease_slot_count,
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .register_worker(worker)
        .dlq_enabled_by_default(true)
        .heartbeat_interval(Duration::from_millis(50))
        .promote_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(100))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .build()
        .expect("Failed to build queue_storage soak client")
}

fn insert_params_for_mode(queue: &str, seq: i64, mode: &str) -> awa::InsertParams {
    let max_attempts = match mode {
        "callback_timeout" | "terminal_fail" => 1,
        _ => 5,
    };
    insert::params_with(
        &MixedSoakJob {
            seq,
            mode: mode.to_string(),
        },
        InsertOpts {
            queue: queue.to_string(),
            max_attempts,
            ..Default::default()
        },
    )
    .expect("Failed to build queue_storage soak insert params")
}

fn mode_cycle() -> Vec<&'static str> {
    let mut modes = Vec::new();
    modes.extend(std::iter::repeat_n("complete", 60));
    modes.extend(std::iter::repeat_n("retry_once", 10));
    modes.extend(std::iter::repeat_n("snooze_once", 10));
    modes.extend(std::iter::repeat_n("deadline_hang", 10));
    modes.extend(std::iter::repeat_n("terminal_fail", 5));
    modes.extend(std::iter::repeat_n("callback_timeout", 5));
    modes
}

async fn wait_for_drain(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
    queue: &str,
    seeded: u64,
    sample_prefix: &str,
    peak_dead_total: &mut i64,
    peak_attempt_state: &mut i64,
    peak_in_flight: &mut i64,
) -> HashMap<String, i64> {
    let timeout = Duration::from_secs(env_u64("AWA_QS_SOAK_DRAIN_TIMEOUT_SECS", 180));
    let start = Instant::now();
    let mut last_sample = Instant::now();
    let mut last_finalized = 0_i64;

    loop {
        let counts = queue_state_counts(pool, queue).await;
        let in_flight = in_flight_count(&counts);
        let finalized = finalized_count(&counts);

        *peak_in_flight = (*peak_in_flight).max(in_flight);

        if last_sample.elapsed() >= Duration::from_secs(1) || in_flight == 0 {
            let estimated = estimated_dead_tuples(pool, store).await;
            let attempt_state = attempt_state_count(pool, store).await;
            let dlq = dlq_depth(pool, store, queue).await;

            *peak_dead_total = (*peak_dead_total).max(estimated.total());
            *peak_attempt_state = (*peak_attempt_state).max(attempt_state);

            println!(
                "[{sample_prefix}] drain seeded={} finalized={} in_flight={} completed={} failed={} scheduled={} retryable={} waiting={} dlq={} dead_total={} leases_dead={} attempt_state={}",
                seeded,
                finalized,
                in_flight,
                count_state(&counts, "completed"),
                count_state(&counts, "failed"),
                count_state(&counts, "scheduled"),
                count_state(&counts, "retryable"),
                count_state(&counts, "waiting_external"),
                dlq,
                estimated.total(),
                estimated.leases,
                attempt_state,
            );

            last_sample = Instant::now();
        }

        if in_flight == 0 {
            return counts;
        }

        assert!(
            start.elapsed() < timeout,
            "Timed out draining queue_storage soak queue {queue}; seeded={seeded} finalized={finalized} last_finalized={last_finalized} counts={counts:?}"
        );
        last_finalized = finalized;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_queue_storage_mixed_workload_soak() {
    let _guard = QUEUE_STORAGE_SOAK_LOCK.lock().await;
    let pool = setup_pool(30).await;
    ensure_pgstattuple(&pool).await;

    let queue = format!("qs_soak_{}", &Uuid::new_v4().simple().to_string()[..8]);
    let schema = format!("awa_qs_soak_{}", &Uuid::new_v4().simple().to_string()[..8]);
    let queue_slot_count = env_usize("AWA_QS_SOAK_QUEUE_SLOTS", 16);
    let lease_slot_count = env_usize("AWA_QS_SOAK_LEASE_SLOTS", 4);
    let max_workers = env_u32("AWA_QS_SOAK_MAX_WORKERS", 64);
    let batch_size = env_usize("AWA_QS_SOAK_BATCH_SIZE", 200);
    let target_rate = env_u64("AWA_QS_SOAK_TARGET_RATE", 3_000);
    let duration_secs = env_u64("AWA_QS_SOAK_DURATION_SECS", 20);

    let store = create_store(&pool, &schema, queue_slot_count, lease_slot_count).await;
    let worker_state = Arc::new(MixedWorkloadState::default());
    let client = build_client(
        &pool,
        &queue,
        &schema,
        max_workers,
        queue_slot_count,
        lease_slot_count,
        MixedWorkloadWorker {
            state: worker_state.clone(),
        },
    );

    client
        .start()
        .await
        .expect("Failed to start queue_storage mixed soak client");

    let pattern = mode_cycle();
    let started = Instant::now();
    let producer_deadline = started + Duration::from_secs(duration_secs);
    let mut next_sample = started + Duration::from_secs(1);
    let mut seeded_total = 0_u64;
    let mut seq = 0_i64;
    let mut seeded_by_mode: HashMap<String, u64> = HashMap::new();
    let mut peak_dead_total = 0_i64;
    let mut peak_attempt_state = 0_i64;
    let mut peak_in_flight = 0_i64;
    let mut last_completed = 0_i64;

    loop {
        let now = Instant::now();
        if now >= producer_deadline {
            break;
        }

        let desired_seeded = (started.elapsed().as_secs_f64() * target_rate as f64).floor() as u64;
        while seeded_total < desired_seeded {
            let remaining = (desired_seeded - seeded_total) as usize;
            let count = remaining.min(batch_size);
            let params: Vec<_> = (0..count)
                .map(|offset| {
                    let mode = pattern[(seq as usize + offset) % pattern.len()];
                    *seeded_by_mode.entry(mode.to_string()).or_default() += 1;
                    insert_params_for_mode(&queue, seq + offset as i64, mode)
                })
                .collect();

            store
                .enqueue_params_batch(&pool, &params)
                .await
                .expect("Failed to enqueue queue_storage soak batch");

            seq += count as i64;
            seeded_total += count as u64;
        }

        if now >= next_sample {
            let counts = queue_state_counts(&pool, &queue).await;
            let estimated = estimated_dead_tuples(&pool, &store).await;
            let attempt_state = attempt_state_count(&pool, &store).await;
            let finalized = finalized_count(&counts);
            let completed_delta = count_state(&counts, "completed") - last_completed;
            let dlq = dlq_depth(&pool, &store, &queue).await;
            let in_flight = in_flight_count(&counts);

            peak_dead_total = peak_dead_total.max(estimated.total());
            peak_attempt_state = peak_attempt_state.max(attempt_state);
            peak_in_flight = peak_in_flight.max(in_flight);
            last_completed = count_state(&counts, "completed");

            println!(
                "[queue-storage-soak] second={} seeded={} finalized={} completed_delta={} in_flight={} completed={} failed={} retryable={} waiting={} dlq={} dead_total={} ready_dead={} leases_dead={} attempt_state={}",
                started.elapsed().as_secs(),
                seeded_total,
                finalized,
                completed_delta,
                in_flight,
                count_state(&counts, "completed"),
                count_state(&counts, "failed"),
                count_state(&counts, "retryable"),
                count_state(&counts, "waiting_external"),
                dlq,
                estimated.total(),
                estimated.ready,
                estimated.leases,
                attempt_state,
            );

            next_sample += Duration::from_secs(1);
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let post_produce = started.elapsed();
    let final_counts = wait_for_drain(
        &pool,
        &store,
        &queue,
        seeded_total,
        "queue-storage-soak",
        &mut peak_dead_total,
        &mut peak_attempt_state,
        &mut peak_in_flight,
    )
    .await;
    let total_elapsed = started.elapsed();
    client.shutdown(Duration::from_secs(5)).await;

    let exact = exact_dead_tuples(&pool, &store).await;
    let final_attempt_state = attempt_state_count(&pool, &store).await;
    let final_dlq = dlq_depth(&pool, &store, &queue).await;
    let expected_failed = seeded_by_mode.get("terminal_fail").copied().unwrap_or(0)
        + seeded_by_mode.get("callback_timeout").copied().unwrap_or(0);
    let expected_completed = seeded_total - expected_failed;
    let handler_total = worker_state.handler_count.load(Ordering::Relaxed);

    println!(
        "[queue-storage-soak] summary duration={}s seeded={} produced_for={:.2}s drained_in={:.2}s handler={:.0}/s finalized={:.0}/s peak_in_flight={} peak_dead_total={} peak_attempt_state={} exact_dead_total={} exact_dead=(queue_lanes={},ready={},done={},leases={},attempt_state={}) final_counts={:?} seeded_by_mode={:?} final_dlq={}",
        duration_secs,
        seeded_total,
        post_produce.as_secs_f64(),
        total_elapsed.as_secs_f64(),
        handler_total as f64 / total_elapsed.as_secs_f64(),
        finalized_count(&final_counts) as f64 / total_elapsed.as_secs_f64(),
        peak_in_flight,
        peak_dead_total,
        peak_attempt_state,
        exact.total(),
        exact.queue_lanes,
        exact.ready,
        exact.done,
        exact.leases,
        exact.attempt_state,
        final_counts,
        seeded_by_mode,
        final_dlq,
    );

    assert_eq!(finalized_count(&final_counts) as u64, seeded_total);
    assert_eq!(
        count_state(&final_counts, "completed") as u64,
        expected_completed
    );
    assert_eq!(count_state(&final_counts, "failed") as u64, expected_failed);
    assert_eq!(final_dlq as u64, expected_failed);
    assert_eq!(in_flight_count(&final_counts), 0);
    assert_eq!(final_attempt_state, 0);
    assert!(
        exact.total() < env_u64("AWA_QS_SOAK_MAX_EXACT_DEAD_TUPLES", 10_000) as i64,
        "queue_storage mixed soak exact dead tuples unexpectedly high: {}",
        exact.total()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_queue_storage_terminal_failure_burst() {
    let _guard = QUEUE_STORAGE_SOAK_LOCK.lock().await;
    let pool = setup_pool(20).await;
    ensure_pgstattuple(&pool).await;

    let queue = format!(
        "qs_terminal_burst_{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let schema = format!(
        "awa_qs_terminal_burst_{}",
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let queue_slot_count = env_usize("AWA_QS_TERMINAL_BURST_QUEUE_SLOTS", 16);
    let lease_slot_count = env_usize("AWA_QS_TERMINAL_BURST_LEASE_SLOTS", 4);
    let max_workers = env_u32("AWA_QS_TERMINAL_BURST_MAX_WORKERS", 64);
    let batch_size = env_usize("AWA_QS_TERMINAL_BURST_BATCH_SIZE", 500);
    let total_jobs = env_u64("AWA_QS_TERMINAL_BURST_TOTAL", 10_000);

    let store = create_store(&pool, &schema, queue_slot_count, lease_slot_count).await;
    let worker_state = Arc::new(MixedWorkloadState::default());
    let params: Vec<_> = (0..total_jobs)
        .map(|seq| insert_params_for_mode(&queue, seq as i64, "terminal_fail"))
        .collect();
    for chunk in params.chunks(batch_size) {
        store
            .enqueue_params_batch(&pool, chunk)
            .await
            .expect("Failed to enqueue queue_storage terminal burst batch");
    }

    let client = build_client(
        &pool,
        &queue,
        &schema,
        max_workers,
        queue_slot_count,
        lease_slot_count,
        MixedWorkloadWorker {
            state: worker_state.clone(),
        },
    );

    let started = Instant::now();
    client
        .start()
        .await
        .expect("Failed to start queue_storage terminal burst client");

    let mut peak_dead_total = 0_i64;
    let mut peak_attempt_state = 0_i64;
    let mut peak_in_flight = 0_i64;
    let final_counts = wait_for_drain(
        &pool,
        &store,
        &queue,
        total_jobs,
        "queue-storage-terminal-burst",
        &mut peak_dead_total,
        &mut peak_attempt_state,
        &mut peak_in_flight,
    )
    .await;
    let total_elapsed = started.elapsed();
    client.shutdown(Duration::from_secs(5)).await;

    let exact = exact_dead_tuples(&pool, &store).await;
    let final_attempt_state = attempt_state_count(&pool, &store).await;
    let final_dlq = dlq_depth(&pool, &store, &queue).await;
    let handler_total = worker_state.handler_count.load(Ordering::Relaxed);

    println!(
        "[queue-storage-terminal-burst] summary total={} drain={:.2}s handler={:.0}/s finalized={:.0}/s peak_in_flight={} peak_dead_total={} peak_attempt_state={} exact_dead_total={} exact_dead=(queue_lanes={},ready={},done={},leases={},attempt_state={}) final_counts={:?} final_dlq={}",
        total_jobs,
        total_elapsed.as_secs_f64(),
        handler_total as f64 / total_elapsed.as_secs_f64(),
        finalized_count(&final_counts) as f64 / total_elapsed.as_secs_f64(),
        peak_in_flight,
        peak_dead_total,
        peak_attempt_state,
        exact.total(),
        exact.queue_lanes,
        exact.ready,
        exact.done,
        exact.leases,
        exact.attempt_state,
        final_counts,
        final_dlq,
    );

    assert_eq!(finalized_count(&final_counts) as u64, total_jobs);
    assert_eq!(count_state(&final_counts, "failed") as u64, total_jobs);
    assert_eq!(count_state(&final_counts, "completed"), 0);
    assert_eq!(final_dlq as u64, total_jobs);
    assert_eq!(in_flight_count(&final_counts), 0);
    assert_eq!(final_attempt_state, 0);
    assert!(
        exact.total() < env_u64("AWA_QS_TERMINAL_BURST_MAX_EXACT_DEAD_TUPLES", 10_000) as i64,
        "queue_storage terminal burst exact dead tuples unexpectedly high: {}",
        exact.total()
    );
}
