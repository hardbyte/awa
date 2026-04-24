//! Benchmark tests for Awa — validates PRD success metrics.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test benchmark_test -- --ignored --nocapture`
//!
//! PRD targets:
//!   - >5,000 jobs/sec sustained (Rust workers, single queue, no uniqueness)
//!   - <50ms median pickup latency (LISTEN/NOTIFY enabled)

mod bench_output;

use awa::model::{
    insert_many, insert_many_copy_from_pool, migrations, QueueStorage, QueueStorageConfig,
};
use awa::{
    Client, InsertOpts, InsertParams, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker,
};
use bench_output::{BenchMetrics, BenchmarkResult, SCHEMA_VERSION};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:awa@localhost:5432/awa_test".to_string())
}

async fn pool_with(max_conns: u32) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database")
}

async fn setup(max_conns: u32) -> sqlx::PgPool {
    let pool = pool_with(max_conns).await;
    migrations::run(&pool).await.expect("Failed to migrate");
    pool
}

async fn ensure_pgstattuple(pool: &sqlx::PgPool) {
    sqlx::query("CREATE EXTENSION IF NOT EXISTS pgstattuple")
        .execute(pool)
        .await
        .expect("Failed to create pgstattuple extension for benchmark dead-tuple sampling");
}

async fn recreate_queue_storage_schema(pool: &sqlx::PgPool, store: &QueueStorage) {
    let drop_sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", store.schema());
    sqlx::query(&drop_sql)
        .execute(pool)
        .await
        .expect("Failed to drop queue storage benchmark schema");
}

async fn reset_storage_transition_state(pool: &sqlx::PgPool) {
    sqlx::query(
        r#"
        UPDATE awa.storage_transition_state
        SET current_engine = 'canonical',
            prepared_engine = NULL,
            state = 'canonical',
            transition_epoch = transition_epoch + 1,
            details = '{}'::jsonb,
            updated_at = now(),
            finalized_at = NULL
        WHERE singleton
        "#,
    )
    .execute(pool)
    .await
    .expect("Failed to reset storage transition state");

    sqlx::query("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        .execute(pool)
        .await
        .expect("Failed to clear active queue storage backend");
    sqlx::query("DELETE FROM awa.runtime_instances")
        .execute(pool)
        .await
        .expect("Failed to clear runtime snapshots");
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
            'benchmark-runtime',
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
    .expect("Failed to insert benchmark runtime instance");
    instance_id
}

async fn activate_queue_storage_transition(pool: &sqlx::PgPool, schema: &str) {
    reset_storage_transition_state(pool).await;
    sqlx::query("SELECT * FROM awa.storage_prepare('queue_storage', $1)")
        .bind(serde_json::json!({ "schema": schema }))
        .execute(pool)
        .await
        .expect("Failed to prepare queue storage transition");
    let gate_runtime = insert_runtime_instance(pool, "queue_storage").await;
    sqlx::query("SELECT * FROM awa.storage_enter_mixed_transition()")
        .execute(pool)
        .await
        .expect("Failed to enter mixed transition");
    sqlx::query("SELECT * FROM awa.storage_finalize()")
        .execute(pool)
        .await
        .expect("Failed to finalize queue storage transition");
    sqlx::query("DELETE FROM awa.runtime_instances WHERE instance_id = $1")
        .bind(gate_runtime)
        .execute(pool)
        .await
        .expect("Failed to remove benchmark gate runtime");
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

async fn sample_canonical_exact_dead_tuples(pool: &sqlx::PgPool) -> HashMap<&'static str, i64> {
    let mut out = HashMap::new();
    for (label, relname) in [
        ("jobs_hot", "jobs_hot"),
        ("scheduled_jobs", "scheduled_jobs"),
        ("queue_state_counts", "queue_state_counts"),
    ] {
        let count = sample_pgstattuple_dead_tuples(pool, "awa", relname)
            .await
            .unwrap_or(-1);
        out.insert(label, count);
    }
    out
}

/// Clean only jobs and queue_meta for a specific queue.
async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    reset_storage_transition_state(pool).await;
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

async fn reset_runtime_state(pool: &sqlx::PgPool) {
    sqlx::query(
        "TRUNCATE awa.jobs_hot, awa.scheduled_jobs, awa.queue_meta, awa.job_unique_claims, awa.queue_state_counts, awa.job_kind_catalog, awa.job_queue_catalog RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("Failed to reset runtime benchmark state");
    reset_storage_transition_state(pool).await;
}

#[derive(Debug, Clone)]
struct DbProfileSnapshot {
    wal_insert_lsn: String,
    xact_commit: i64,
    xact_rollback: i64,
    tup_inserted: i64,
    temp_bytes: i64,
    temp_files: i64,
}

async fn capture_db_profile(pool: &sqlx::PgPool) -> Option<DbProfileSnapshot> {
    let _ = sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(pool)
        .await;
    let _ = sqlx::query("SELECT pg_stat_clear_snapshot()")
        .execute(pool)
        .await;

    let row = sqlx::query_as::<_, (String, i64, i64, i64, i64, i64)>(
        r#"
        SELECT
            pg_current_wal_insert_lsn()::text,
            COALESCE(xact_commit, 0),
            COALESCE(xact_rollback, 0),
            COALESCE(tup_inserted, 0),
            COALESCE(temp_bytes, 0),
            COALESCE(temp_files, 0)
        FROM pg_stat_database
        WHERE datname = current_database()
        "#,
    )
    .fetch_optional(pool)
    .await
    .ok()??;

    Some(DbProfileSnapshot {
        wal_insert_lsn: row.0,
        xact_commit: row.1,
        xact_rollback: row.2,
        tup_inserted: row.3,
        temp_bytes: row.4,
        temp_files: row.5,
    })
}

async fn capture_db_profile_delta(
    pool: &sqlx::PgPool,
    start: &DbProfileSnapshot,
) -> Option<serde_json::Value> {
    let end = capture_db_profile(pool).await?;
    let wal_bytes: i64 =
        sqlx::query_scalar("SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::bigint")
            .bind(&end.wal_insert_lsn)
            .bind(&start.wal_insert_lsn)
            .fetch_one(pool)
            .await
            .ok()?;

    Some(serde_json::json!({
        "wal_bytes": wal_bytes,
        "xact_commit_delta": end.xact_commit - start.xact_commit,
        "xact_rollback_delta": end.xact_rollback - start.xact_rollback,
        "tup_inserted_delta": end.tup_inserted - start.tup_inserted,
        "temp_bytes_delta": end.temp_bytes - start.temp_bytes,
        "temp_files_delta": end.temp_files - start.temp_files,
    }))
}

async fn queue_state_counts(pool: &sqlx::PgPool, queues: &[String]) -> HashMap<String, u64> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT state::text, count(*)::bigint FROM awa.jobs WHERE queue = ANY($1) GROUP BY state",
    )
    .bind(queues)
    .fetch_all(pool)
    .await
    .expect("Failed to fetch queue state counts");

    rows.into_iter()
        .map(|(state, count)| (state, count as u64))
        .collect()
}

fn emit_enqueue_result(
    scenario: &str,
    seeded: u64,
    elapsed: Duration,
    outcomes: HashMap<String, u64>,
    metadata: serde_json::Value,
) {
    let enqueue_rate = seeded as f64 / elapsed.as_secs_f64();

    BenchmarkResult {
        schema_version: SCHEMA_VERSION,
        scenario: scenario.to_string(),
        language: "rust".to_string(),
        seeded,
        metrics: BenchMetrics {
            throughput: None,
            enqueue_per_s: Some(enqueue_rate),
            drain_time_s: Some(elapsed.as_secs_f64()),
            latency_ms: None,
            rescue: None,
        },
        outcomes,
        metadata: Some(metadata),
    }
    .emit();
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

#[derive(Debug, Clone, Copy)]
enum EnqueueMode {
    Insert,
    Copy,
}

impl EnqueueMode {
    fn as_str(self) -> &'static str {
        match self {
            EnqueueMode::Insert => "insert",
            EnqueueMode::Copy => "copy",
        }
    }
}

#[derive(Debug, Clone)]
struct EnqueueScenario {
    name: &'static str,
    mode: EnqueueMode,
    producers: usize,
    same_queue: bool,
}

impl EnqueueScenario {
    fn producer_queues(&self) -> Vec<String> {
        if self.same_queue {
            vec![format!("{}_shared", self.name); self.producers]
        } else {
            (0..self.producers)
                .map(|index| format!("{}_p{index}", self.name))
                .collect()
        }
    }
}

async fn run_enqueue_scenario(
    pool: &sqlx::PgPool,
    scenario: &EnqueueScenario,
    jobs_per_producer: i64,
    insert_batch_size: usize,
    copy_chunk_size: usize,
) {
    let producer_queues = scenario.producer_queues();
    let mut unique_queues = producer_queues.clone();
    unique_queues.sort();
    unique_queues.dedup();
    reset_runtime_state(pool).await;

    let barrier = Arc::new(tokio::sync::Barrier::new(scenario.producers + 1));
    let mut tasks = Vec::with_capacity(scenario.producers);

    for (producer_idx, queue) in producer_queues.iter().enumerate() {
        let pool = pool.clone();
        let queue = queue.clone();
        let barrier = barrier.clone();
        let mode = scenario.mode;
        let start_seq = producer_idx as i64 * jobs_per_producer;
        let params: Vec<InsertParams> = (0..jobs_per_producer)
            .map(|offset| {
                awa::model::insert::params_with(
                    &BenchJob {
                        seq: start_seq + offset,
                    },
                    InsertOpts {
                        queue: queue.clone(),
                        ..Default::default()
                    },
                )
                .unwrap()
            })
            .collect();

        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            match mode {
                EnqueueMode::Insert => {
                    for chunk in params.chunks(insert_batch_size) {
                        insert_many(&pool, chunk).await.unwrap();
                    }
                }
                EnqueueMode::Copy => {
                    for chunk in params.chunks(copy_chunk_size) {
                        insert_many_copy_from_pool(&pool, chunk).await.unwrap();
                    }
                }
            }
        }));
    }

    let db_profile_start = capture_db_profile(pool).await;
    let start = Instant::now();
    barrier.wait().await;

    for task in tasks {
        task.await.expect("contention benchmark task panicked");
    }

    let elapsed = start.elapsed();
    let outcomes = queue_state_counts(pool, &unique_queues).await;
    let total_inserted: u64 = outcomes.values().sum();
    let seeded = (scenario.producers as i64 * jobs_per_producer) as u64;
    assert_eq!(total_inserted, seeded, "All seeded jobs should be present");

    let enqueue_rate = seeded as f64 / elapsed.as_secs_f64();
    println!(
        "[enqueue-bench] scenario={} mode={} producers={} queue_layout={} seeded={} elapsed={:.2}s rate={:.0}/s",
        scenario.name,
        scenario.mode.as_str(),
        scenario.producers,
        if scenario.same_queue { "same" } else { "distinct" },
        seeded,
        elapsed.as_secs_f64(),
        enqueue_rate
    );

    let mut metadata = serde_json::json!({
        "measurement": "enqueue",
        "mode": scenario.mode.as_str(),
        "producers": scenario.producers,
        "queue_layout": if scenario.same_queue { "same" } else { "distinct" },
        "jobs_per_producer": jobs_per_producer,
        "insert_batch_size": insert_batch_size,
        "copy_chunk_size": copy_chunk_size,
        "queues": unique_queues,
    });

    if let Some(start) = db_profile_start.as_ref() {
        if let Some(db_profile) = capture_db_profile_delta(pool, start).await {
            metadata
                .as_object_mut()
                .expect("benchmark metadata should be an object")
                .insert("db_profile".to_string(), db_profile);
        }
    }

    emit_enqueue_result(scenario.name, seeded, elapsed, outcomes, metadata);
}

// ─── Job types ───────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct BenchJob {
    pub seq: i64,
}

/// No-op worker that completes immediately.
struct BenchWorker;

#[async_trait::async_trait]
impl Worker for BenchWorker {
    fn kind(&self) -> &'static str {
        "bench_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::Completed)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 1: Sustained throughput with full Client runtime
// PRD target: >5,000 jobs/sec (Rust workers, single queue, no uniqueness)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_throughput_rust_workers() {
    let pool = setup(20).await;
    ensure_pgstattuple(&pool).await;
    let queue = "bench_throughput";
    clean_queue(&pool, queue).await;

    let total_jobs: i64 = env_i64(
        "AWA_RUNTIME_TOTAL_JOBS",
        env_i64("AWA_VA_RUNTIME_TOTAL_JOBS", 5_000),
    );
    let batch_size = env_usize(
        "AWA_RUNTIME_BATCH_SIZE",
        env_usize("AWA_VA_RUNTIME_BATCH_SIZE", 500),
    );

    // Build and start the Client with workers
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 100,
                poll_interval: Duration::from_millis(50),
                ..QueueConfig::default()
            },
        )
        .register_worker(BenchWorker)
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");

    // Insert jobs in batches
    let benchmark_start = Instant::now();
    let insert_start = Instant::now();
    for batch_start in (0..total_jobs).step_by(batch_size) {
        let batch_end = (batch_start + batch_size as i64).min(total_jobs);
        let params: Vec<_> = (batch_start..batch_end)
            .map(|i| {
                awa::model::insert::params_with(
                    &BenchJob { seq: i },
                    InsertOpts {
                        queue: queue.into(),
                        ..Default::default()
                    },
                )
                .unwrap()
            })
            .collect();
        insert_many(&pool, &params).await.unwrap();
    }
    let insert_elapsed = insert_start.elapsed();
    println!(
        "[bench] Inserted {} jobs in {:.2}s ({:.0} inserts/sec)",
        total_jobs,
        insert_elapsed.as_secs_f64(),
        total_jobs as f64 / insert_elapsed.as_secs_f64()
    );

    // Wait for all jobs to complete, polling periodically
    let processing_start = Instant::now();
    let timeout = Duration::from_secs(30);
    let mut last_count = 0i64;
    let mut stall_checks = 0u32;

    loop {
        if processing_start.elapsed() > timeout {
            let completed: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'completed'",
            )
            .bind(queue)
            .fetch_one(&pool)
            .await
            .unwrap();
            panic!(
                "Timeout after 30s: only {}/{} jobs completed",
                completed, total_jobs
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        let completed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'completed'",
        )
        .bind(queue)
        .fetch_one(&pool)
        .await
        .unwrap();

        if completed == total_jobs {
            let processing_elapsed = processing_start.elapsed();
            let throughput = total_jobs as f64 / processing_elapsed.as_secs_f64();
            let end_to_end_elapsed = benchmark_start.elapsed();
            let end_to_end_throughput = total_jobs as f64 / end_to_end_elapsed.as_secs_f64();
            println!(
                "[bench] All {} jobs completed in {:.2}s",
                total_jobs,
                processing_elapsed.as_secs_f64()
            );
            println!("[bench] Post-insert throughput: {:.0} jobs/sec", throughput);
            println!(
                "[bench] End-to-end throughput: {:.0} jobs/sec over {:.2}s",
                end_to_end_throughput,
                end_to_end_elapsed.as_secs_f64()
            );

            client.shutdown(Duration::from_secs(5)).await;

            let dead = sample_canonical_exact_dead_tuples(&pool).await;
            let jobs_hot_dead = *dead.get("jobs_hot").unwrap_or(&-1);
            let scheduled_dead = *dead.get("scheduled_jobs").unwrap_or(&-1);
            let counts_dead = *dead.get("queue_state_counts").unwrap_or(&-1);
            let total_dead = [jobs_hot_dead, scheduled_dead, counts_dead]
                .into_iter()
                .filter(|v| *v >= 0)
                .sum::<i64>();
            println!(
                "[bench] exact_dead_tuples jobs_hot={} scheduled_jobs={} queue_state_counts={} total={}",
                jobs_hot_dead, scheduled_dead, counts_dead, total_dead
            );

            // Use a lower bound for CI variance (3000), but the PRD target is 5000
            assert!(
                throughput >= 3000.0,
                "Throughput {:.0} jobs/sec is below minimum threshold of 3000 jobs/sec \
                 (PRD target: 5000 jobs/sec)",
                throughput
            );
            return;
        }

        // Track progress for stall detection
        if completed == last_count {
            stall_checks += 1;
            if stall_checks > 50 {
                // 5 seconds with no progress
                panic!(
                    "Processing stalled at {}/{} completed jobs",
                    completed, total_jobs
                );
            }
        } else {
            stall_checks = 0;
            last_count = completed;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_throughput_rust_workers_queue_storage() {
    let pool = setup(20).await;
    ensure_pgstattuple(&pool).await;
    reset_runtime_state(&pool).await;

    let queue = "bench_throughput_queue_storage";
    let total_jobs: i64 = env_i64("AWA_VA_RUNTIME_TOTAL_JOBS", 5_000);
    let batch_size = env_usize("AWA_VA_RUNTIME_BATCH_SIZE", 500);
    let queue_slot_count = env_usize("AWA_VA_RUNTIME_QUEUE_SLOTS", 16);
    let lease_slot_count = env_usize("AWA_VA_RUNTIME_LEASE_SLOTS", 4);
    let queue_stripe_count = env_usize("AWA_VA_RUNTIME_QUEUE_STRIPES", 1);
    let queue_rotate_ms = env_i64("AWA_VA_RUNTIME_QUEUE_ROTATE_MS", 1_000);
    let lease_rotate_ms = env_i64("AWA_VA_RUNTIME_LEASE_ROTATE_MS", 50);

    let store_config = QueueStorageConfig {
        queue_slot_count,
        lease_slot_count,
        queue_stripe_count,
        experimental_lease_claim_receipts: true,
        claim_slot_count: 2,
        ..Default::default()
    };
    let store =
        QueueStorage::new(store_config.clone()).expect("Failed to build queue storage store");
    recreate_queue_storage_schema(&pool, &store).await;
    store
        .install(&pool)
        .await
        .expect("Failed to install queue storage store");
    store
        .reset(&pool)
        .await
        .expect("Failed to reset queue storage store");
    activate_queue_storage_transition(&pool, store.schema()).await;

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 100,
                poll_interval: Duration::from_millis(50),
                deadline_duration: Duration::ZERO,
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            store_config,
            Duration::from_millis(queue_rotate_ms as u64),
            Duration::from_millis(lease_rotate_ms as u64),
        )
        .register_worker(BenchWorker)
        .build()
        .expect("Failed to build queue storage client");

    client
        .start()
        .await
        .expect("Failed to start queue storage client");

    let benchmark_start = Instant::now();
    let insert_start = Instant::now();
    for batch_start in (0..total_jobs).step_by(batch_size) {
        let batch_end = (batch_start + batch_size as i64).min(total_jobs);
        let params: Vec<_> = (batch_start..batch_end)
            .map(|i| {
                awa::model::insert::params_with(
                    &BenchJob { seq: i },
                    InsertOpts {
                        queue: queue.into(),
                        ..Default::default()
                    },
                )
                .unwrap()
            })
            .collect();

        store
            .enqueue_params_batch(&pool, &params)
            .await
            .expect("Failed to enqueue queue storage runtime batch");
    }
    let insert_elapsed = insert_start.elapsed();
    println!(
        "[bench-va] Inserted {} jobs in {:.2}s ({:.0} inserts/sec)",
        total_jobs,
        insert_elapsed.as_secs_f64(),
        total_jobs as f64 / insert_elapsed.as_secs_f64()
    );

    let processing_start = Instant::now();
    let timeout = Duration::from_secs(30);
    let mut last_completed = 0i64;
    let mut stall_checks = 0u32;

    loop {
        if processing_start.elapsed() > timeout {
            let counts = store
                .queue_counts(&pool, queue)
                .await
                .expect("Failed to sample queue storage queue counts");
            panic!(
                "Vacuum-aware timeout after 30s: completed={}/{} available={} running={}",
                counts.completed, total_jobs, counts.available, counts.running
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        let counts = store
            .queue_counts(&pool, queue)
            .await
            .expect("Failed to sample queue storage queue counts");

        if counts.completed == total_jobs {
            let processing_elapsed = processing_start.elapsed();
            let throughput = total_jobs as f64 / processing_elapsed.as_secs_f64();
            let end_to_end_elapsed = benchmark_start.elapsed();
            let end_to_end_throughput = total_jobs as f64 / end_to_end_elapsed.as_secs_f64();
            println!(
                "[bench-va] All {} jobs completed in {:.2}s",
                total_jobs,
                processing_elapsed.as_secs_f64()
            );
            println!(
                "[bench-va] Post-insert throughput: {:.0} jobs/sec",
                throughput
            );
            println!(
                "[bench-va] End-to-end throughput: {:.0} jobs/sec over {:.2}s",
                end_to_end_throughput,
                end_to_end_elapsed.as_secs_f64()
            );

            client.shutdown(Duration::from_secs(5)).await;

            let queue_lanes_dead =
                sample_pgstattuple_dead_tuples(&pool, store.schema(), "queue_lanes").await;
            let ready_dead =
                sample_pgstattuple_dead_tuples(&pool, store.schema(), "ready_entries_%").await;
            let done_dead =
                sample_pgstattuple_dead_tuples(&pool, store.schema(), "done_entries_%").await;
            let leases_dead =
                sample_pgstattuple_dead_tuples(&pool, store.schema(), "leases_%").await;
            let attempt_state_dead =
                sample_pgstattuple_dead_tuples(&pool, store.schema(), "attempt_state").await;

            println!(
                "[bench-va] exact_dead_tuples queue_lanes={} ready={} done={} leases={} attempt_state={} total={}",
                queue_lanes_dead.unwrap_or(-1),
                ready_dead.unwrap_or(-1),
                done_dead.unwrap_or(-1),
                leases_dead.unwrap_or(-1),
                attempt_state_dead.unwrap_or(-1),
                queue_lanes_dead.unwrap_or(0)
                    + ready_dead.unwrap_or(0)
                    + done_dead.unwrap_or(0)
                    + leases_dead.unwrap_or(0)
                    + attempt_state_dead.unwrap_or(0),
            );

            assert!(
                throughput >= 3000.0,
                "Vacuum-aware throughput {:.0} jobs/sec is below minimum threshold of 3000 jobs/sec",
                throughput
            );
            return;
        }

        if counts.completed == last_completed {
            stall_checks += 1;
            if stall_checks > 50 {
                panic!(
                    "Vacuum-aware processing stalled at {}/{} completed jobs",
                    counts.completed, total_jobs
                );
            }
        } else {
            stall_checks = 0;
            last_completed = counts.completed;
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 2: Pickup latency with LISTEN/NOTIFY
// PRD target: <50ms median pickup latency
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_pickup_latency_listen_notify() {
    let pool = setup(10).await;
    let queue = "bench_latency";
    clean_queue(&pool, queue).await;

    // Channel for workers to report their pickup time
    let (pickup_tx, mut pickup_rx) = tokio::sync::mpsc::unbounded_channel::<std::time::Instant>();

    // Build a worker that records pickup time
    struct LatencyWorker {
        tx: tokio::sync::mpsc::UnboundedSender<std::time::Instant>,
    }

    #[async_trait::async_trait]
    impl Worker for LatencyWorker {
        fn kind(&self) -> &'static str {
            "bench_job"
        }

        async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
            let _ = self.tx.send(Instant::now());
            Ok(JobResult::Completed)
        }
    }

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 10,
                poll_interval: Duration::from_millis(200),
                ..QueueConfig::default()
            },
        )
        .register_worker(LatencyWorker { tx: pickup_tx })
        .build()
        .expect("Failed to build client");

    client.start().await.expect("Failed to start client");

    // Wait for the dispatcher to be ready (LISTEN established)
    tokio::time::sleep(Duration::from_millis(500)).await;

    let iterations = 50;
    let mut latencies: Vec<Duration> = Vec::with_capacity(iterations);

    for i in 0..iterations {
        // Clean any leftover from previous iteration
        let insert_time = Instant::now();

        awa::model::insert_with(
            &pool,
            &BenchJob { seq: i as i64 },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Wait for the worker to pick up the job (with timeout)
        let pickup_time = tokio::time::timeout(Duration::from_secs(5), pickup_rx.recv())
            .await
            .expect("Timeout waiting for job pickup")
            .expect("Channel closed unexpectedly");

        let latency = pickup_time.duration_since(insert_time);
        latencies.push(latency);
    }

    client.shutdown(Duration::from_secs(5)).await;

    // Calculate percentiles
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    let min_latency = latencies[0];
    let max_latency = latencies[latencies.len() - 1];

    println!("[bench] Pickup latency over {} iterations:", iterations);
    println!("[bench]   min:  {:?}", min_latency);
    println!("[bench]   p50:  {:?}", p50);
    println!("[bench]   p95:  {:?}", p95);
    println!("[bench]   p99:  {:?}", p99);
    println!("[bench]   max:  {:?}", max_latency);

    assert!(
        p50 < Duration::from_millis(50),
        "Median pickup latency {:?} exceeds PRD target of 50ms",
        p50
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_pickup_latency_listen_notify_queue_storage() {
    let pool = setup(10).await;
    let queue = "bench_latency_queue_storage";
    let store_config = QueueStorageConfig {
        queue_slot_count: env_usize("AWA_VA_LATENCY_QUEUE_SLOTS", 16),
        lease_slot_count: env_usize("AWA_VA_LATENCY_LEASE_SLOTS", 4),
        queue_stripe_count: env_usize("AWA_VA_LATENCY_QUEUE_STRIPES", 1),
        experimental_lease_claim_receipts: true,
        claim_slot_count: 2,
        ..Default::default()
    };
    let queue_rotate_ms = env_i64("AWA_VA_LATENCY_QUEUE_ROTATE_MS", 1_000);
    let lease_rotate_ms = env_i64("AWA_VA_LATENCY_LEASE_ROTATE_MS", 50);
    let store =
        QueueStorage::new(store_config.clone()).expect("Failed to build queue storage store");
    recreate_queue_storage_schema(&pool, &store).await;
    store
        .install(&pool)
        .await
        .expect("Failed to install queue storage latency store");
    store
        .reset(&pool)
        .await
        .expect("Failed to reset queue storage latency store");
    activate_queue_storage_transition(&pool, store.schema()).await;

    let (pickup_tx, mut pickup_rx) = tokio::sync::mpsc::unbounded_channel::<std::time::Instant>();

    struct LatencyWorker {
        tx: tokio::sync::mpsc::UnboundedSender<std::time::Instant>,
    }

    #[async_trait::async_trait]
    impl Worker for LatencyWorker {
        fn kind(&self) -> &'static str {
            "bench_job"
        }

        async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
            let _ = self.tx.send(Instant::now());
            Ok(JobResult::Completed)
        }
    }

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 10,
                poll_interval: Duration::from_millis(200),
                deadline_duration: Duration::ZERO,
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            store_config,
            Duration::from_millis(queue_rotate_ms as u64),
            Duration::from_millis(lease_rotate_ms as u64),
        )
        .register_worker(LatencyWorker { tx: pickup_tx })
        .build()
        .expect("Failed to build queue storage client");

    client
        .start()
        .await
        .expect("Failed to start queue storage client");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let iterations = 50;
    let mut latencies: Vec<Duration> = Vec::with_capacity(iterations);

    for i in 0..iterations {
        let insert_time = Instant::now();
        let params = [awa::model::insert::params_with(
            &BenchJob { seq: i as i64 },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .unwrap()];

        store
            .enqueue_params_batch(&pool, &params)
            .await
            .expect("Failed to enqueue queue storage latency job");

        let pickup_time = tokio::time::timeout(Duration::from_secs(5), pickup_rx.recv())
            .await
            .expect("Timeout waiting for queue storage job pickup")
            .expect("Vacuum-aware pickup channel closed unexpectedly");

        latencies.push(pickup_time.duration_since(insert_time));
    }

    client.shutdown(Duration::from_secs(5)).await;

    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    let min_latency = latencies[0];
    let max_latency = latencies[latencies.len() - 1];

    println!("[bench-va] Pickup latency over {} iterations:", iterations);
    println!("[bench-va]   min:  {:?}", min_latency);
    println!("[bench-va]   p50:  {:?}", p50);
    println!("[bench-va]   p95:  {:?}", p95);
    println!("[bench-va]   p99:  {:?}", p99);
    println!("[bench-va]   max:  {:?}", max_latency);

    assert!(
        p50 < Duration::from_millis(50),
        "Vacuum-aware median pickup latency {:?} exceeds PRD target of 50ms",
        p50
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Test 3: Raw insert throughput (no workers)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_throughput_insert_only() {
    let pool = setup(20).await;
    let queue = "bench_insert_only";
    reset_runtime_state(&pool).await;
    clean_queue(&pool, queue).await;

    let total_jobs: i64 = 10_000;
    let batch_size: i64 = 1_000;

    let db_profile_start = capture_db_profile(&pool).await;
    let start = Instant::now();

    for batch_start in (0..total_jobs).step_by(batch_size as usize) {
        let batch_end = (batch_start + batch_size).min(total_jobs);
        let params: Vec<_> = (batch_start..batch_end)
            .map(|i| {
                awa::model::insert::params_with(
                    &BenchJob { seq: i },
                    InsertOpts {
                        queue: queue.into(),
                        ..Default::default()
                    },
                )
                .unwrap()
            })
            .collect();
        insert_many(&pool, &params).await.unwrap();
    }

    let elapsed = start.elapsed();
    let insert_rate = total_jobs as f64 / elapsed.as_secs_f64();

    println!(
        "[bench] Inserted {} jobs in {:.2}s ({:.0} inserts/sec)",
        total_jobs,
        elapsed.as_secs_f64(),
        insert_rate
    );

    // Verify all jobs were inserted
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'available'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, total_jobs, "All jobs should be inserted");

    let mut outcomes = HashMap::new();
    outcomes.insert("available".to_string(), count as u64);
    let mut metadata = serde_json::json!({
        "measurement": "enqueue",
        "mode": "insert",
        "producers": 1,
        "queue_layout": "single",
        "insert_batch_size": batch_size,
    });
    if let Some(start) = db_profile_start.as_ref() {
        if let Some(db_profile) = capture_db_profile_delta(&pool, start).await {
            metadata
                .as_object_mut()
                .expect("benchmark metadata should be an object")
                .insert("db_profile".to_string(), db_profile);
        }
    }
    emit_enqueue_result(
        "insert_only_single",
        total_jobs as u64,
        elapsed,
        outcomes,
        metadata,
    );

    assert!(
        insert_rate >= 10_000.0,
        "Insert rate {:.0} jobs/sec is below minimum threshold of 10,000 jobs/sec",
        insert_rate
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Test 4: COPY insert throughput vs chunked INSERT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_throughput_copy_insert() {
    let pool = setup(20).await;
    let total_jobs: i64 = 10_000;
    let batch_size: i64 = 1_000;
    let copy_chunk_size = env_usize("AWA_BENCH_COPY_CHUNK_SIZE", batch_size as usize);
    reset_runtime_state(&pool).await;

    // ── Chunked INSERT baseline ──
    let queue_insert = "bench_copy_insert";
    clean_queue(&pool, queue_insert).await;

    let insert_profile_start = capture_db_profile(&pool).await;
    let insert_start = Instant::now();
    for batch_start in (0..total_jobs).step_by(batch_size as usize) {
        let batch_end = (batch_start + batch_size).min(total_jobs);
        let params: Vec<_> = (batch_start..batch_end)
            .map(|i| {
                awa::model::insert::params_with(
                    &BenchJob { seq: i },
                    InsertOpts {
                        queue: queue_insert.into(),
                        ..Default::default()
                    },
                )
                .unwrap()
            })
            .collect();
        insert_many(&pool, &params).await.unwrap();
    }
    let insert_elapsed = insert_start.elapsed();
    let insert_rate = total_jobs as f64 / insert_elapsed.as_secs_f64();
    let insert_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'available'",
    )
    .bind(queue_insert)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        insert_count, total_jobs,
        "All insert benchmark jobs should be present"
    );

    // ── COPY ──
    let queue_copy = "bench_copy_copy";
    clean_queue(&pool, queue_copy).await;

    let copy_profile_start = capture_db_profile(&pool).await;
    let copy_start = Instant::now();
    let params: Vec<_> = (0..total_jobs)
        .map(|i| {
            awa::model::insert::params_with(
                &BenchJob { seq: i },
                InsertOpts {
                    queue: queue_copy.into(),
                    ..Default::default()
                },
            )
            .unwrap()
        })
        .collect();
    for chunk in params.chunks(copy_chunk_size) {
        insert_many_copy_from_pool(&pool, chunk).await.unwrap();
    }
    let copy_elapsed = copy_start.elapsed();
    let copy_rate = total_jobs as f64 / copy_elapsed.as_secs_f64();
    let copy_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'available'",
    )
    .bind(queue_copy)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        copy_count, total_jobs,
        "All COPY benchmark jobs should be present"
    );

    println!(
        "[bench] Chunked INSERT: {} jobs in {:.2}s ({:.0} inserts/sec)",
        total_jobs,
        insert_elapsed.as_secs_f64(),
        insert_rate
    );
    println!(
        "[bench] COPY:           {} jobs in {:.2}s ({:.0} inserts/sec)",
        total_jobs,
        copy_elapsed.as_secs_f64(),
        copy_rate
    );
    println!("[bench] COPY speedup:   {:.1}x", copy_rate / insert_rate);

    let mut insert_outcomes = HashMap::new();
    insert_outcomes.insert("available".to_string(), insert_count as u64);
    let mut insert_metadata = serde_json::json!({
        "measurement": "enqueue",
        "mode": "insert",
        "producers": 1,
        "queue_layout": "single",
        "insert_batch_size": batch_size,
        "copy_chunk_size": copy_chunk_size,
        "compared_to": "copy_single",
    });
    if let Some(start) = insert_profile_start.as_ref() {
        if let Some(db_profile) = capture_db_profile_delta(&pool, start).await {
            insert_metadata
                .as_object_mut()
                .expect("benchmark metadata should be an object")
                .insert("db_profile".to_string(), db_profile);
        }
    }
    emit_enqueue_result(
        "insert_single",
        total_jobs as u64,
        insert_elapsed,
        insert_outcomes,
        insert_metadata,
    );

    let mut copy_outcomes = HashMap::new();
    copy_outcomes.insert("available".to_string(), copy_count as u64);
    let mut copy_metadata = serde_json::json!({
        "measurement": "enqueue",
        "mode": "copy",
        "producers": 1,
        "queue_layout": "single",
        "insert_batch_size": batch_size,
        "copy_chunk_size": copy_chunk_size,
        "compared_to": "insert_single",
    });
    if let Some(start) = copy_profile_start.as_ref() {
        if let Some(db_profile) = capture_db_profile_delta(&pool, start).await {
            copy_metadata
                .as_object_mut()
                .expect("benchmark metadata should be an object")
                .insert("db_profile".to_string(), db_profile);
        }
    }
    emit_enqueue_result(
        "copy_single",
        total_jobs as u64,
        copy_elapsed,
        copy_outcomes,
        copy_metadata,
    );

    // COPY should be at least as fast as chunked INSERT
    // (In practice it's significantly faster, but we use a generous threshold)
    assert!(
        copy_rate >= insert_rate * 0.8,
        "COPY rate {:.0}/s should be at least 80% of INSERT rate {:.0}/s",
        copy_rate,
        insert_rate
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Test 5: Multi-producer enqueue contention (INSERT vs COPY)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore]
async fn test_enqueue_contention_matrix() {
    let producers = env_usize("AWA_BENCH_CONTENTION_PRODUCERS", 4);
    let jobs_per_producer = env_i64("AWA_BENCH_CONTENTION_JOBS_PER_PRODUCER", 3_000);
    let insert_batch_size = env_usize("AWA_BENCH_INSERT_BATCH_SIZE", 1_000);
    let copy_chunk_size = env_usize("AWA_BENCH_COPY_CHUNK_SIZE", insert_batch_size);

    let pool = setup((producers as u32 * 4).max(20)).await;

    let scenarios = vec![
        EnqueueScenario {
            name: "insert_single",
            mode: EnqueueMode::Insert,
            producers: 1,
            same_queue: true,
        },
        EnqueueScenario {
            name: "copy_single",
            mode: EnqueueMode::Copy,
            producers: 1,
            same_queue: true,
        },
        EnqueueScenario {
            name: "insert_contention_distinct",
            mode: EnqueueMode::Insert,
            producers,
            same_queue: false,
        },
        EnqueueScenario {
            name: "copy_contention_distinct",
            mode: EnqueueMode::Copy,
            producers,
            same_queue: false,
        },
        EnqueueScenario {
            name: "insert_contention_same_queue",
            mode: EnqueueMode::Insert,
            producers,
            same_queue: true,
        },
        EnqueueScenario {
            name: "copy_contention_same_queue",
            mode: EnqueueMode::Copy,
            producers,
            same_queue: true,
        },
    ];

    for scenario in &scenarios {
        run_enqueue_scenario(
            &pool,
            scenario,
            jobs_per_producer,
            insert_batch_size,
            copy_chunk_size,
        )
        .await;
    }
}
