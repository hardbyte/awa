//! Benchmark and smoke tests for the queue storage engine.
//!
//! `test_queue_storage_storage_benchmark` is intentionally a smoke benchmark:
//! it emits raw overlap/rotation/prune data for ADR validation, but it is not
//! the source of truth for release claims. Throughput/latency thresholds live
//! in `benchmark_test.rs`, and the reproducible validation output is checked in
//! under `docs/adr/bench/`.

mod bench_output;

use awa::model::{
    migrations, AwaError, PruneOutcome, QueueStorage, QueueStorageConfig, RotateOutcome,
};
use bench_output::{BenchLatency, BenchMetrics, BenchThroughput, BenchmarkResult, SCHEMA_VERSION};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn is_lock_timeout_sqlx(err: &sqlx::Error) -> bool {
    matches!(
        err,
        sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("55P03")
    )
}

fn is_lock_timeout_awa(err: &AwaError) -> bool {
    matches!(err, AwaError::Database(db_err) if is_lock_timeout_sqlx(db_err))
}

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
        "queue_storage benchmark database names must use only [A-Za-z0-9_]"
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
        .expect("Failed to connect to admin database for queue_storage benchmarks");
    let create_sql = format!("CREATE DATABASE {database_name}");
    match sqlx::query(&create_sql).execute(&admin_pool).await {
        Ok(_) => {}
        Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P04") => {}
        Err(err) => {
            panic!("Failed to create queue_storage benchmark database {database_name}: {err}")
        }
    }
}

async fn pool_with(max_conns: u32) -> sqlx::PgPool {
    let url = database_url();
    ensure_database_exists(&url).await;
    let pool = PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(&url)
        .await
        .expect("Failed to connect to database");
    migrations::run(&pool)
        .await
        .expect("Failed to run queue_storage benchmark migrations");
    pool
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

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[derive(Debug, Clone, Serialize)]
struct QueueStorageSample {
    second: u64,
    seeded_total: u64,
    completed_total: u64,
    completed_delta: u64,
    available: i64,
    running: i64,
    completed: i64,
    queue_lanes_dead_tup: i64,
    ready_dead_tup: i64,
    done_dead_tup: i64,
    leases_dead_tup: i64,
    attempt_state_dead_tup: i64,
    queue_prune_ok: u64,
    queue_prune_blocked: u64,
    queue_prune_skipped_active: u64,
    lease_prune_ok: u64,
    lease_prune_blocked: u64,
    lease_prune_skipped_active: u64,
}

#[derive(Debug, Clone)]
struct QueueStorageSnapshot {
    available: i64,
    running: i64,
    completed: i64,
    queue_lanes_dead_tup: i64,
    ready_dead_tup: i64,
    done_dead_tup: i64,
    leases_dead_tup: i64,
    attempt_state_dead_tup: i64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
struct ExactDeadTuples {
    queue_lanes: i64,
    ready: i64,
    done: i64,
    leases: i64,
    attempt_state: i64,
}

#[derive(Default)]
struct MaintenanceCounters {
    queue_prune_ok: AtomicU64,
    queue_prune_blocked: AtomicU64,
    queue_prune_skipped_active: AtomicU64,
    queue_rotate_ok: AtomicU64,
    queue_rotate_skipped_busy: AtomicU64,
    lease_prune_ok: AtomicU64,
    lease_prune_blocked: AtomicU64,
    lease_prune_skipped_active: AtomicU64,
    lease_rotate_ok: AtomicU64,
    lease_rotate_skipped_busy: AtomicU64,
}

async fn ensure_pgstattuple(pool: &sqlx::PgPool) {
    sqlx::query("CREATE EXTENSION IF NOT EXISTS pgstattuple")
        .execute(pool)
        .await
        .expect("Failed to create pgstattuple extension for queue-storage benchmark sampling");
}

async fn recreate_store_schema(pool: &sqlx::PgPool, store: &QueueStorage) {
    let drop_sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", store.schema());
    sqlx::query(&drop_sql)
        .execute(pool)
        .await
        .expect("Failed to drop experimental queue storage schema");
}

async fn sample_dead_tuples(pool: &sqlx::PgPool, schema: &str, relname_filter: &str) -> i64 {
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
    .fetch_one(pool)
    .await
    .expect("Failed to sample dead tuples")
}

async fn sample_pgstattuple_dead_tuples(
    pool: &sqlx::PgPool,
    schema: &str,
    relname_filter: &str,
) -> Option<i64> {
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
    .ok()
}

async fn sample_exact_dead_tuples(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
) -> Option<ExactDeadTuples> {
    Some(ExactDeadTuples {
        queue_lanes: sample_pgstattuple_dead_tuples(pool, store.schema(), "queue_lanes").await?,
        ready: sample_pgstattuple_dead_tuples(pool, store.schema(), "ready_entries_%").await?,
        done: sample_pgstattuple_dead_tuples(pool, store.schema(), "done_entries_%").await?,
        leases: sample_pgstattuple_dead_tuples(pool, store.schema(), "leases_%").await?,
        attempt_state: sample_pgstattuple_dead_tuples(pool, store.schema(), "attempt_state")
            .await?,
    })
}

async fn sample_snapshot(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
    queue: &str,
) -> QueueStorageSnapshot {
    let _ = sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(pool)
        .await;
    let _ = sqlx::query("SELECT pg_stat_clear_snapshot()")
        .execute(pool)
        .await;

    let counts = store
        .queue_counts(pool, queue)
        .await
        .expect("Failed to sample queue counts");

    let queue_lanes_dead_tup = sample_dead_tuples(pool, store.schema(), "queue_lanes").await;
    let ready_dead_tup = sample_dead_tuples(pool, store.schema(), "ready_entries_%").await;
    let done_dead_tup = sample_dead_tuples(pool, store.schema(), "done_entries_%").await;
    let leases_dead_tup = sample_dead_tuples(pool, store.schema(), "leases_%").await;
    let attempt_state_dead_tup = sample_dead_tuples(pool, store.schema(), "attempt_state").await;

    QueueStorageSnapshot {
        available: counts.available,
        running: counts.running,
        completed: counts.completed,
        queue_lanes_dead_tup,
        ready_dead_tup,
        done_dead_tup,
        leases_dead_tup,
        attempt_state_dead_tup,
    }
}

fn percentile(values: &[f64], percentile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("latency percentile NaN"));
    let rank = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted.get(rank).copied()
}

async fn wait_or_stop(duration: Duration, stop: &mut tokio::sync::watch::Receiver<bool>) -> bool {
    if *stop.borrow() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        changed = stop.changed() => changed.is_err() || *stop.borrow(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn producer_loop(
    pool: sqlx::PgPool,
    store: Arc<QueueStorage>,
    queue: String,
    priority: i16,
    jobs_per_sec: u64,
    tick: Duration,
    seeded: Arc<AtomicU64>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut carry = 0.0_f64;

    loop {
        tokio::select! {
            _ = stop.changed() => {
                if *stop.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                carry += jobs_per_sec as f64 * tick.as_secs_f64();
                let batch = carry.floor() as i64;
                carry -= batch as f64;
                if batch <= 0 {
                    continue;
                }

                store
                    .enqueue_batch(&pool, &queue, priority, batch)
                    .await
                    .expect("Vacuum-aware enqueue batch failed");
                seeded.fetch_add(batch as u64, Ordering::Relaxed);
            }
        }
    }
}

async fn consumer_loop(
    pool: sqlx::PgPool,
    store: Arc<QueueStorage>,
    queue: String,
    batch_size: i64,
    claim_latencies_ms: Arc<Mutex<Vec<f64>>>,
    completed: Arc<AtomicU64>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *stop.borrow() {
            break;
        }

        let claim_started = Instant::now();
        let claimed = store
            .claim_batch(&pool, &queue, batch_size)
            .await
            .expect("Vacuum-aware claim batch failed");

        if claimed.is_empty() {
            tokio::select! {
                _ = stop.changed() => {
                    if *stop.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(2)) => {}
            }
            continue;
        }

        claim_latencies_ms
            .lock()
            .expect("claim latency lock poisoned")
            .push(claim_started.elapsed().as_secs_f64() * 1000.0);

        let completed_rows = store
            .complete_batch(&pool, &claimed)
            .await
            .expect("Vacuum-aware completion batch failed");
        completed.fetch_add(completed_rows as u64, Ordering::Relaxed);
    }
}

async fn maintenance_loop(
    pool: sqlx::PgPool,
    store: Arc<QueueStorage>,
    queue_rotate_interval: Duration,
    lease_rotate_interval: Duration,
    vacuum_interval: Option<Duration>,
    counters: Arc<MaintenanceCounters>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut queue_rotate_timer = tokio::time::interval(queue_rotate_interval);
    queue_rotate_timer.tick().await;
    let mut lease_rotate_timer = tokio::time::interval(lease_rotate_interval);
    lease_rotate_timer.tick().await;
    let mut vacuum_timer = vacuum_interval.map(tokio::time::interval);
    if let Some(timer) = &mut vacuum_timer {
        timer.tick().await;
    }

    loop {
        tokio::select! {
            _ = stop.changed() => {
                if *stop.borrow() {
                    break;
                }
            }
            _ = queue_rotate_timer.tick() => {
                match store.rotate(&pool).await.expect("Vacuum-aware rotate failed") {
                    RotateOutcome::Rotated { .. } => {
                        counters.queue_rotate_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    RotateOutcome::SkippedBusy { .. } => {
                        counters.queue_rotate_skipped_busy.fetch_add(1, Ordering::Relaxed);
                    }
                }

                match store.prune_oldest(&pool).await {
                    Ok(PruneOutcome::Noop) => {}
                    Ok(PruneOutcome::Pruned { .. }) => {
                        counters.queue_prune_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(PruneOutcome::Blocked { .. }) => {
                        counters.queue_prune_blocked.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(PruneOutcome::SkippedActive { .. }) => {
                        counters
                            .queue_prune_skipped_active
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(err) if is_lock_timeout_awa(&err) => {
                        counters.queue_prune_blocked.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(err) => panic!("Vacuum-aware prune failed: {err:?}"),
                }
            }
            _ = lease_rotate_timer.tick() => {
                match store.rotate_leases(&pool).await.expect("Vacuum-aware lease rotate failed") {
                    RotateOutcome::Rotated { .. } => {
                        counters.lease_rotate_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    RotateOutcome::SkippedBusy { .. } => {
                        counters
                            .lease_rotate_skipped_busy
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }

                match store.prune_oldest_leases(&pool).await {
                    Ok(PruneOutcome::Noop) => {}
                    Ok(PruneOutcome::Pruned { .. }) => {
                        counters.lease_prune_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(PruneOutcome::Blocked { .. }) => {
                        counters
                            .lease_prune_blocked
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(PruneOutcome::SkippedActive { .. }) => {
                        counters
                            .lease_prune_skipped_active
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(err) if is_lock_timeout_awa(&err) => {
                        counters
                            .lease_prune_blocked
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(err) => panic!("Vacuum-aware lease prune failed: {err:?}"),
                }
            }
            _ = async {
                if let Some(timer) = &mut vacuum_timer {
                    timer.tick().await;
                } else {
                    std::future::pending::<()>().await;
                }
            }, if vacuum_timer.is_some() => {
                store
                    .vacuum_leases(&pool)
                    .await
                    .expect("Vacuum-aware lease vacuum failed");
            }
        }
    }
}

async fn overlap_reader(
    pool: sqlx::PgPool,
    schema: String,
    queue: String,
    initial_delay: Duration,
    hold_time: Duration,
    reader_mode: String,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    if wait_or_stop(initial_delay, &mut stop).await {
        return;
    }

    loop {
        if *stop.borrow() {
            break;
        }

        let mut conn = pool
            .acquire()
            .await
            .expect("Failed to acquire overlap reader connection");

        sqlx::query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(conn.as_mut())
            .await
            .expect("Failed to start overlap reader transaction");

        match reader_mode.as_str() {
            "history_snapshot" => {
                let query =
                    format!("SELECT count(*)::bigint FROM {schema}.ready_entries WHERE queue = $1");
                let _: i64 = sqlx::query_scalar(&query)
                    .bind(&queue)
                    .fetch_one(conn.as_mut())
                    .await
                    .expect("Failed to pin history reader snapshot");
            }
            _ => {
                let query = format!(
                    "SELECT COALESCE(sum(available_count + pruned_completed_count), 0)::bigint FROM {schema}.queue_lanes WHERE queue = $1"
                );
                let _: i64 = sqlx::query_scalar(&query)
                    .bind(&queue)
                    .fetch_one(conn.as_mut())
                    .await
                    .expect("Failed to pin cache reader snapshot");
            }
        }

        if wait_or_stop(hold_time, &mut stop).await {
            sqlx::query("COMMIT")
                .execute(conn.as_mut())
                .await
                .expect("Failed to commit overlap reader transaction");
            break;
        }

        sqlx::query("COMMIT")
            .execute(conn.as_mut())
            .await
            .expect("Failed to commit overlap reader transaction");

        if *stop.borrow() {
            break;
        }
    }
}

fn average_completed_rate(samples: &[QueueStorageSample]) -> f64 {
    if samples.is_empty() {
        0.0
    } else {
        samples
            .iter()
            .map(|sample| sample.completed_delta as f64)
            .sum::<f64>()
            / samples.len() as f64
    }
}

#[tokio::test]
async fn test_queue_storage_round_trip_smoke() {
    let pool = pool_with(12).await;
    let store = QueueStorage::new(QueueStorageConfig::default())
        .expect("Failed to construct queue storage store");
    recreate_store_schema(&pool, &store).await;
    store.install(&pool).await.expect("Failed to install store");
    store.reset(&pool).await.expect("Failed to reset store");

    store
        .enqueue_batch(&pool, "smoke", 2, 10)
        .await
        .expect("Failed to enqueue smoke jobs");
    let claimed = store
        .claim_batch(&pool, "smoke", 4)
        .await
        .expect("Failed to claim smoke jobs");
    assert_eq!(claimed.len(), 4);

    let counts = store
        .queue_counts(&pool, "smoke")
        .await
        .expect("Failed to sample smoke counts");
    assert_eq!(counts.available, 6);
    assert_eq!(counts.running, 4);
    assert_eq!(counts.completed, 0);

    let completed = store
        .complete_batch(&pool, &claimed)
        .await
        .expect("Failed to complete smoke claims");
    assert_eq!(completed, 4);

    loop {
        let batch = store
            .claim_batch(&pool, "smoke", 8)
            .await
            .expect("Failed to drain smoke queue");
        if batch.is_empty() {
            break;
        }
        store
            .complete_batch(&pool, &batch)
            .await
            .expect("Failed to finish smoke claims");
    }

    let rotated_queue = store
        .rotate(&pool)
        .await
        .expect("Failed to rotate smoke queue slot");
    assert!(matches!(rotated_queue, RotateOutcome::Rotated { .. }));
    let pruned_queue = store
        .prune_oldest(&pool)
        .await
        .expect("Failed to prune smoke queue slot");
    assert!(matches!(pruned_queue, PruneOutcome::Pruned { slot: 0 }));

    let rotated_leases = store
        .rotate_leases(&pool)
        .await
        .expect("Failed to rotate smoke lease slot");
    assert!(matches!(rotated_leases, RotateOutcome::Rotated { .. }));
    let pruned_leases = store
        .prune_oldest_leases(&pool)
        .await
        .expect("Failed to prune smoke lease slot");
    assert!(matches!(pruned_leases, PruneOutcome::Pruned { slot: 0 }));

    let final_counts = store
        .queue_counts(&pool, "smoke")
        .await
        .expect("Failed to sample final smoke counts");
    assert_eq!(final_counts.available, 0);
    assert_eq!(final_counts.running, 0);
    assert_eq!(final_counts.completed, 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore]
async fn test_queue_storage_storage_benchmark() {
    let max_conns = env_u32("AWA_VA_POOL_MAX", 24);
    let pool = pool_with(max_conns).await;
    ensure_pgstattuple(&pool).await;

    let queue_slot_count = env_u32("AWA_VA_QUEUE_SLOT_COUNT", env_u32("AWA_VA_SLOT_COUNT", 16));
    let lease_slot_count = env_u32("AWA_VA_LEASE_SLOT_COUNT", 8);
    let store = Arc::new(
        QueueStorage::new(QueueStorageConfig {
            queue_slot_count: queue_slot_count as usize,
            lease_slot_count: lease_slot_count as usize,
            ..Default::default()
        })
        .expect("Failed to construct queue storage benchmark store"),
    );
    recreate_store_schema(&pool, &store).await;
    store
        .install(&pool)
        .await
        .expect("Failed to install queue storage store");

    let queue = env_string("AWA_VA_QUEUE", "bench_queue_storage");
    let priority = env_u32("AWA_VA_PRIORITY", 2) as i16;
    let producer_rate = env_u64("AWA_VA_ENQUEUE_RATE", 4_000);
    let producer_tick_ms = env_u64("AWA_VA_PRODUCER_TICK_MS", 50);
    let claim_batch_size = env_u64("AWA_VA_CLAIM_BATCH_SIZE", 256) as i64;
    let baseline_secs = env_u64("AWA_VA_BASELINE_SECS", 4);
    let overlap_secs = env_u64("AWA_VA_OVERLAP_SECS", 8);
    let cooldown_secs = env_u64("AWA_VA_COOLDOWN_SECS", 4);
    let overlap_readers = env_u32("AWA_VA_OVERLAP_READERS", 1);
    let overlap_hold_secs = env_u64("AWA_VA_OVERLAP_HOLD_SECS", 8);
    let overlap_stagger_secs = env_u64("AWA_VA_OVERLAP_STAGGER_SECS", 0);
    let reader_mode = env_string("AWA_VA_READER_MODE", "cache_snapshot");
    let queue_rotate_interval_ms = env_u64(
        "AWA_VA_QUEUE_ROTATE_INTERVAL_MS",
        env_u64("AWA_VA_ROTATE_INTERVAL_MS", 1_000),
    );
    let lease_rotate_interval_ms = env_u64("AWA_VA_LEASE_ROTATE_INTERVAL_MS", 100);
    let vacuum_interval_ms = env_u64("AWA_VA_VACUUM_INTERVAL_MS", 250);
    let vacuum_interval = if env_u32("AWA_VA_VACUUM_LEASES", 0) != 0 {
        Some(Duration::from_millis(vacuum_interval_ms))
    } else {
        None
    };
    let total_secs = baseline_secs + overlap_secs + cooldown_secs;

    store
        .reset(&pool)
        .await
        .expect("Failed to reset queue storage benchmark store");

    let seeded = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicU64::new(0));
    let claim_latencies_ms = Arc::new(Mutex::new(Vec::new()));
    let maintenance_counters = Arc::new(MaintenanceCounters::default());

    let (producer_tx, producer_rx) = tokio::sync::watch::channel(false);
    let (consumer_tx, consumer_rx) = tokio::sync::watch::channel(false);
    let (reader_tx, reader_rx) = tokio::sync::watch::channel(false);
    let (maintenance_tx, maintenance_rx) = tokio::sync::watch::channel(false);

    let producer_handle = tokio::spawn(producer_loop(
        pool.clone(),
        store.clone(),
        queue.clone(),
        priority,
        producer_rate,
        Duration::from_millis(producer_tick_ms),
        seeded.clone(),
        producer_rx,
    ));

    let consumer_handle = tokio::spawn(consumer_loop(
        pool.clone(),
        store.clone(),
        queue.clone(),
        claim_batch_size,
        claim_latencies_ms.clone(),
        completed.clone(),
        consumer_rx,
    ));

    let maintenance_handle = tokio::spawn(maintenance_loop(
        pool.clone(),
        store.clone(),
        Duration::from_millis(queue_rotate_interval_ms),
        Duration::from_millis(lease_rotate_interval_ms),
        vacuum_interval,
        maintenance_counters.clone(),
        maintenance_rx,
    ));

    let initial = sample_snapshot(&pool, &store, &queue).await;
    let mut samples = Vec::with_capacity(total_secs as usize);
    let mut overlap_handles = Vec::new();
    let benchmark_started = Instant::now();
    let mut previous_completed = 0_u64;
    let mut readers_started = false;

    for second in 1..=total_secs {
        if !readers_started && second == baseline_secs + 1 {
            readers_started = true;
            for index in 0..overlap_readers {
                overlap_handles.push(tokio::spawn(overlap_reader(
                    pool.clone(),
                    store.schema().to_string(),
                    queue.clone(),
                    Duration::from_secs(overlap_stagger_secs * index as u64),
                    Duration::from_secs(overlap_hold_secs),
                    reader_mode.clone(),
                    reader_rx.clone(),
                )));
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        let snapshot = sample_snapshot(&pool, &store, &queue).await;
        let completed_total = completed.load(Ordering::Relaxed);
        let completed_delta = completed_total.saturating_sub(previous_completed);
        previous_completed = completed_total;

        let sample = QueueStorageSample {
            second,
            seeded_total: seeded.load(Ordering::Relaxed),
            completed_total,
            completed_delta,
            available: snapshot.available,
            running: snapshot.running,
            completed: snapshot.completed,
            queue_lanes_dead_tup: snapshot.queue_lanes_dead_tup,
            ready_dead_tup: snapshot.ready_dead_tup,
            done_dead_tup: snapshot.done_dead_tup,
            leases_dead_tup: snapshot.leases_dead_tup,
            attempt_state_dead_tup: snapshot.attempt_state_dead_tup,
            queue_prune_ok: maintenance_counters.queue_prune_ok.load(Ordering::Relaxed),
            queue_prune_blocked: maintenance_counters
                .queue_prune_blocked
                .load(Ordering::Relaxed),
            queue_prune_skipped_active: maintenance_counters
                .queue_prune_skipped_active
                .load(Ordering::Relaxed),
            lease_prune_ok: maintenance_counters.lease_prune_ok.load(Ordering::Relaxed),
            lease_prune_blocked: maintenance_counters
                .lease_prune_blocked
                .load(Ordering::Relaxed),
            lease_prune_skipped_active: maintenance_counters
                .lease_prune_skipped_active
                .load(Ordering::Relaxed),
        };

        println!(
            "[queue storage] second={:>2} seeded={} completed={} (+{}) available={} running={} dead(q_lanes/ready/done/leases/attempt)={}/{}/{}/{}/{} queue_prune_ok={} queue_prune_blocked={} lease_prune_ok={} lease_prune_blocked={}",
            sample.second,
            sample.seeded_total,
            sample.completed_total,
            sample.completed_delta,
            sample.available,
            sample.running,
            sample.queue_lanes_dead_tup,
            sample.ready_dead_tup,
            sample.done_dead_tup,
            sample.leases_dead_tup,
            sample.attempt_state_dead_tup,
            sample.queue_prune_ok,
            sample.queue_prune_blocked,
            sample.lease_prune_ok,
            sample.lease_prune_blocked,
        );

        samples.push(sample);
    }

    let _ = producer_tx.send(true);
    producer_handle
        .await
        .expect("Vacuum-aware producer task failed");

    tokio::time::sleep(Duration::from_millis(250)).await;
    let _ = consumer_tx.send(true);
    let _ = maintenance_tx.send(true);
    let _ = reader_tx.send(true);

    consumer_handle
        .await
        .expect("Vacuum-aware consumer task failed");
    maintenance_handle
        .await
        .expect("Vacuum-aware maintenance task failed");
    for handle in overlap_handles {
        handle.await.expect("Vacuum-aware overlap reader failed");
    }

    let final_snapshot = sample_snapshot(&pool, &store, &queue).await;
    let final_exact_dead = sample_exact_dead_tuples(&pool, &store).await;
    let elapsed = benchmark_started.elapsed();
    let baseline_window = &samples[..baseline_secs as usize];
    let overlap_end = (baseline_secs + overlap_secs) as usize;
    let overlap_window = &samples[baseline_secs as usize..overlap_end];
    let cooldown_window = &samples[overlap_end..];

    let baseline_rate = average_completed_rate(baseline_window);
    let overlap_rate = average_completed_rate(overlap_window);
    let cooldown_rate = average_completed_rate(cooldown_window);

    let max_total_dead_tup = samples
        .iter()
        .map(|sample| {
            sample.queue_lanes_dead_tup
                + sample.ready_dead_tup
                + sample.done_dead_tup
                + sample.leases_dead_tup
                + sample.attempt_state_dead_tup
        })
        .max()
        .unwrap_or(0);
    let final_total_dead_tup = final_snapshot.queue_lanes_dead_tup
        + final_snapshot.ready_dead_tup
        + final_snapshot.done_dead_tup
        + final_snapshot.leases_dead_tup
        + final_snapshot.attempt_state_dead_tup;

    let claim_latencies = claim_latencies_ms
        .lock()
        .expect("claim latency lock poisoned")
        .clone();
    let p50 = percentile(&claim_latencies, 0.50);
    let p95 = percentile(&claim_latencies, 0.95);
    let p99 = percentile(&claim_latencies, 0.99);

    println!(
        "[queue storage] baseline={baseline_rate:.0}/s overlap={overlap_rate:.0}/s cooldown={cooldown_rate:.0}/s max_dead={} final_dead={} queue_prune_ok={} queue_prune_blocked={} queue_rotate_ok={} queue_rotate_skipped_busy={} lease_prune_ok={} lease_prune_blocked={} lease_rotate_ok={} lease_rotate_skipped_busy={}",
        max_total_dead_tup,
        final_total_dead_tup,
        maintenance_counters.queue_prune_ok.load(Ordering::Relaxed),
        maintenance_counters.queue_prune_blocked.load(Ordering::Relaxed),
        maintenance_counters.queue_rotate_ok.load(Ordering::Relaxed),
        maintenance_counters
            .queue_rotate_skipped_busy
            .load(Ordering::Relaxed),
        maintenance_counters.lease_prune_ok.load(Ordering::Relaxed),
        maintenance_counters
            .lease_prune_blocked
            .load(Ordering::Relaxed),
        maintenance_counters.lease_rotate_ok.load(Ordering::Relaxed),
        maintenance_counters
            .lease_rotate_skipped_busy
            .load(Ordering::Relaxed),
    );
    println!(
        "[queue storage] claim_latency_ms p50={:.3} p95={:.3} p99={:.3}",
        p50.unwrap_or(0.0),
        p95.unwrap_or(0.0),
        p99.unwrap_or(0.0),
    );
    if let Some(exact) = final_exact_dead {
        println!(
            "[queue storage] exact_dead_tuples queue_lanes={} ready={} done={} leases={} attempt_state={} total={}",
            exact.queue_lanes,
            exact.ready,
            exact.done,
            exact.leases,
            exact.attempt_state,
            exact.queue_lanes + exact.ready + exact.done + exact.leases + exact.attempt_state,
        );
    } else {
        println!("[queue storage] exact_dead_tuples unavailable");
    }

    let mut outcomes = HashMap::new();
    outcomes.insert("completed".to_string(), final_snapshot.completed as u64);
    outcomes.insert("available".to_string(), final_snapshot.available as u64);
    outcomes.insert("running".to_string(), final_snapshot.running as u64);

    BenchmarkResult {
        schema_version: SCHEMA_VERSION,
        scenario: format!("queue_storage_{}", reader_mode),
        language: "rust".to_string(),
        seeded: seeded.load(Ordering::Relaxed),
        metrics: BenchMetrics {
            throughput: Some(BenchThroughput {
                handler_per_s: overlap_rate,
                db_finalized_per_s: overlap_rate,
            }),
            enqueue_per_s: Some(producer_rate as f64),
            drain_time_s: Some(elapsed.as_secs_f64()),
            latency_ms: Some(BenchLatency { p50, p95, p99 }),
            rescue: None,
        },
        outcomes,
        metadata: Some(serde_json::json!({
            "profile": "queue_storage_storage",
            "queue": queue,
            "priority": priority,
            "queue_slot_count": store.queue_slot_count(),
            "lease_slot_count": store.lease_slot_count(),
            "producer_rate_per_s": producer_rate,
            "claim_batch_size": claim_batch_size,
            "baseline_secs": baseline_secs,
            "overlap_secs": overlap_secs,
            "cooldown_secs": cooldown_secs,
            "overlap_readers": overlap_readers,
            "overlap_hold_secs": overlap_hold_secs,
            "overlap_stagger_secs": overlap_stagger_secs,
            "reader_mode": reader_mode,
            "queue_rotate_interval_ms": queue_rotate_interval_ms,
            "lease_rotate_interval_ms": lease_rotate_interval_ms,
            "vacuum_leases": vacuum_interval.is_some(),
            "vacuum_interval_ms": vacuum_interval.map(|d| d.as_millis() as u64),
            "baseline_completed_per_s": baseline_rate,
            "overlap_completed_per_s": overlap_rate,
            "cooldown_completed_per_s": cooldown_rate,
            "initial_total_dead_tup": initial.queue_lanes_dead_tup + initial.ready_dead_tup + initial.done_dead_tup + initial.leases_dead_tup + initial.attempt_state_dead_tup,
            "max_total_dead_tup": max_total_dead_tup,
            "final_total_dead_tup": final_total_dead_tup,
            "final_queue_lanes_dead_tup": final_snapshot.queue_lanes_dead_tup,
            "final_ready_dead_tup": final_snapshot.ready_dead_tup,
            "final_done_dead_tup": final_snapshot.done_dead_tup,
            "final_leases_dead_tup": final_snapshot.leases_dead_tup,
            "final_attempt_state_dead_tup": final_snapshot.attempt_state_dead_tup,
            "exact_final_dead_tuples": final_exact_dead,
            "queue_prune_ok": maintenance_counters.queue_prune_ok.load(Ordering::Relaxed),
            "queue_prune_blocked": maintenance_counters.queue_prune_blocked.load(Ordering::Relaxed),
            "queue_prune_skipped_active": maintenance_counters.queue_prune_skipped_active.load(Ordering::Relaxed),
            "queue_rotate_ok": maintenance_counters.queue_rotate_ok.load(Ordering::Relaxed),
            "queue_rotate_skipped_busy": maintenance_counters.queue_rotate_skipped_busy.load(Ordering::Relaxed),
            "lease_prune_ok": maintenance_counters.lease_prune_ok.load(Ordering::Relaxed),
            "lease_prune_blocked": maintenance_counters.lease_prune_blocked.load(Ordering::Relaxed),
            "lease_prune_skipped_active": maintenance_counters.lease_prune_skipped_active.load(Ordering::Relaxed),
            "lease_rotate_ok": maintenance_counters.lease_rotate_ok.load(Ordering::Relaxed),
            "lease_rotate_skipped_busy": maintenance_counters.lease_rotate_skipped_busy.load(Ordering::Relaxed),
            "samples": samples,
        })),
    }
    .emit();
}

#[allow(dead_code)]
fn _assert_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<QueueStorage>();
    assert_send_sync::<AwaError>();
}
