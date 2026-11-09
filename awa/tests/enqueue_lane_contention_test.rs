//! Per-lane enqueue contention: many concurrent producers batch-inserting
//! into one queue lane.
//!
//! Every enqueue on queue storage reserves its lane sequence under a
//! transaction-scoped advisory lock (`reserve_enqueue_seq`) that is held until
//! the enqueue transaction ends, so producers for one `(queue, priority,
//! shard)` lane serialise on it. This suite measures what that serialisation
//! costs: lane throughput and batch-insert latency at high producer
//! concurrency, with and without unique keys, with and without a unique
//! conflict fallback, and across `enqueue_shards`.
//!
//! * `test_enqueue_lane_contention_gate` runs a fixed set of scenarios, checks
//!   lane integrity, asserts contention-scaled floors, and emits
//!   `@@BENCH_JSON@@` records that `.github/scripts/check-bench-regression.py`
//!   compares against `benchmarks/baseline.json`.
//! * `test_enqueue_lane_contention_matrix` is the exploration entry point: it
//!   sweeps producer counts for one configuration chosen through environment
//!   variables and prints every scenario.
//!
//! Matrix tunables (all optional):
//!   AWA_LANE_BENCH_PRODUCERS      comma list, default "1,4,8,16,32"
//!   AWA_LANE_BENCH_BATCH          jobs per batch, default 185
//!   AWA_LANE_BENCH_UNIQUE         0|1, default 1
//!   AWA_LANE_BENCH_SHARDS         `enqueue_shards` for the queue, default 1
//!   AWA_LANE_BENCH_MODE           insert_many | insert_many_copy | insert_many_skip |
//!                                 queue_storage | legacy_row_function
//!   AWA_LANE_BENCH_DUP_PCT        % of batches carrying one duplicate key that
//!                                 fall back to row-at-a-time inserts, default 0
//!   AWA_LANE_BENCH_SECS           measured seconds per scenario, default 8
//!   AWA_LANE_BENCH_DRAIN          0|1: claim everything that landed afterwards and
//!                                 check every shard drains
//!   AWA_LANE_BENCH_SPLIT          0|1: run each batch in an explicit
//!                                 transaction and time statement and commit
//!                                 separately (insert_many mode only)
//!   AWA_LANE_BENCH_WITHOUT_PATCH  0|1: drop `awa.insert_jobs_compat_batch`
//!                                 first, so insert_many_copy measures its
//!                                 per-row path (the next migration run puts
//!                                 the function back)
//!
//! Requires a running Postgres instance.
//! Run with:
//!   cargo test --release --package awa --test enqueue_lane_contention_test \
//!       -- --ignored --nocapture --test-threads=1

mod bench_output;
mod ci_timing;

use awa::model::{
    insert_many, insert_many_copy_from_pool, insert_many_skip_unique_conflicts,
    prepare_raw_job_insert, AwaError, QueueStorage,
};
use awa::{InsertOpts, InsertParams, QueueStorageConfig, UniqueOpts};
use awa_testing::setup;
use bench_output::{BenchLatency, BenchMetrics, BenchmarkResult, SCHEMA_VERSION};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const APPLICATION_NAME: &str = "awa_lane_bench";
const WARMUP: Duration = Duration::from_millis(1_500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    InsertMany,
    InsertManyCopy,
    InsertManySkip,
    QueueStorage,
    LegacyRowFunction,
}

impl Mode {
    fn parse(raw: &str) -> Self {
        match raw {
            "insert_many" => Mode::InsertMany,
            "insert_many_copy" => Mode::InsertManyCopy,
            "insert_many_skip" => Mode::InsertManySkip,
            "queue_storage" => Mode::QueueStorage,
            "legacy_row_function" => Mode::LegacyRowFunction,
            other => panic!("unknown AWA_LANE_BENCH_MODE {other}"),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Mode::InsertMany => "insert_many",
            Mode::InsertManyCopy => "insert_many_copy",
            Mode::InsertManySkip => "insert_many_skip",
            Mode::QueueStorage => "queue_storage",
            Mode::LegacyRowFunction => "legacy_row_function",
        }
    }
}

#[derive(Debug, Clone)]
struct Scenario {
    name: String,
    mode: Mode,
    producers: usize,
    batch_size: usize,
    unique: bool,
    shards: i16,
    duplicate_batch_pct: u32,
    measured: Duration,
    split_commit: bool,
    drain: bool,
}

#[derive(Debug, Default, Clone)]
struct Samples {
    /// Latency of the producer call, in milliseconds: one `insert_many`
    /// statement, or for a duplicate batch the failed batch plus the
    /// row-at-a-time fallback.
    batch_ms: Vec<f64>,
    /// Latency of each single-row statement issued by the fallback.
    fallback_row_ms: Vec<f64>,
    /// Statement and commit latency when the batch runs in an explicit
    /// transaction.
    statement_ms: Vec<f64>,
    commit_ms: Vec<f64>,
    /// Jobs the producers asked to insert in the measured window.
    requested: u64,
    /// Jobs the inserts returned in the measured window: requested jobs less
    /// those skipped for a unique conflict.
    jobs: u64,
    /// Jobs the inserts returned over the whole run, warmup and the batches
    /// still in flight at the end of the window included.
    acknowledged_total: u64,
    batches: u64,
    duplicate_batches: u64,
    fallback_rows: u64,
}

impl Samples {
    fn merge(&mut self, other: Samples) {
        self.batch_ms.extend(other.batch_ms);
        self.fallback_row_ms.extend(other.fallback_row_ms);
        self.statement_ms.extend(other.statement_ms);
        self.commit_ms.extend(other.commit_ms);
        self.requested += other.requested;
        self.jobs += other.jobs;
        self.acknowledged_total += other.acknowledged_total;
        self.batches += other.batches;
        self.duplicate_batches += other.duplicate_batches;
        self.fallback_rows += other.fallback_rows;
    }
}

#[derive(Debug)]
struct ScenarioResult {
    scenario: Scenario,
    jobs_per_s: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    mean_waiters: f64,
    samples: Samples,
    landed: i64,
    distinct_lane_seqs: i64,
    drained: Option<(i64, f64)>,
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64 - 1.0) * quantile).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn sorted(values: &[f64]) -> Vec<f64> {
    let mut copy = values.to_vec();
    copy.sort_by(|left, right| left.partial_cmp(right).expect("latencies are finite"));
    copy
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn job_params(queue: &str, tag: &str, producer: usize, counter: u64, unique: bool) -> InsertParams {
    InsertParams {
        kind: "lane_bench_job".to_string(),
        args: serde_json::json!({
            "tag": tag,
            "producer": producer,
            "n": counter,
            "payload": "x".repeat(64),
        }),
        opts: InsertOpts {
            queue: queue.to_string(),
            unique: unique.then(|| UniqueOpts {
                by_queue: true,
                by_args: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    }
}

fn is_unique_violation(err: &AwaError) -> bool {
    match err {
        AwaError::UniqueConflict { .. } => true,
        AwaError::Database(sqlx::Error::Database(db_err)) => {
            db_err.code().as_deref() == Some("23505")
        }
        _ => false,
    }
}

async fn bench_pool(max_connections: u32) -> PgPool {
    let options = PgConnectOptions::from_str(&setup::database_url())
        .expect("parse DATABASE_URL")
        .application_name(APPLICATION_NAME);
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await
        .expect("connect bench pool")
}

async fn waiter_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        r#"
        SELECT count(*)::bigint
        FROM pg_stat_activity
        WHERE application_name = $1
          AND wait_event_type = 'Lock'
          AND wait_event = 'advisory'
        "#,
    )
    .bind(APPLICATION_NAME)
    .fetch_one(pool)
    .await
    .unwrap_or(0)
}

async fn seed_queue_shards(pool: &PgPool, queue: &str, shards: i16) {
    sqlx::query(
        "INSERT INTO awa.queue_meta (queue, enqueue_shards) VALUES ($1, $2)
         ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards",
    )
    .bind(queue)
    .bind(shards)
    .execute(pool)
    .await
    .expect("seed queue_meta.enqueue_shards");
}

/// One statement that calls `awa.insert_job_compat` once per row, the shape
/// batch inserts had before `awa.insert_jobs_compat_batch`. It measures what a
/// batch costs the lane lock on this runner when every row goes through the
/// per-row function, which is the yardstick the gate compares `insert_many`
/// against.
const LEGACY_ROW_FUNCTION_SQL: &str = r#"
    SELECT inserted.*
    FROM unnest(
        $1::text[], $2::text[], $3::jsonb[], $4::text[], $5::smallint[], $6::smallint[],
        $7::timestamptz[], $8::jsonb[], $9::jsonb[], $10::bytea[], $11::text[]
    ) WITH ORDINALITY AS input(
        kind, queue, args, state, priority, max_attempts, run_at,
        metadata, tags, unique_key, unique_states, ord
    )
    CROSS JOIN LATERAL awa.insert_job_compat(
        input.kind,
        input.queue,
        input.args,
        input.state::awa.job_state,
        input.priority,
        input.max_attempts,
        input.run_at,
        input.metadata,
        ARRAY(
            SELECT tag.value
            FROM jsonb_array_elements_text(input.tags) WITH ORDINALITY AS tag(value, tag_ord)
            ORDER BY tag.tag_ord
        ),
        input.unique_key,
        input.unique_states::bit(8),
        NULL
    ) AS inserted
    ORDER BY input.ord
"#;

async fn legacy_row_function_insert(
    pool: &PgPool,
    jobs: &[InsertParams],
) -> Result<usize, AwaError> {
    let rows: Vec<_> = jobs
        .iter()
        .map(|job| {
            prepare_raw_job_insert(job.kind.clone(), job.args.clone(), job.opts.clone())
                .expect("prepare row")
        })
        .collect();
    let inserted = sqlx::query(LEGACY_ROW_FUNCTION_SQL)
        .bind(rows.iter().map(|row| row.kind()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.queue()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.args()).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| row.state_db_str())
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(|row| row.priority()).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| row.max_attempts())
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(|row| row.run_at()).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.metadata()).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| serde_json::json!(row.tags()))
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(|row| row.unique_key()).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| row.unique_states_bit_string())
                .collect::<Vec<_>>(),
        )
        .fetch_all(pool)
        .await?;
    Ok(inserted.len())
}

#[allow(clippy::too_many_arguments)]
async fn producer_loop(
    pool: PgPool,
    store: Arc<QueueStorage>,
    scenario: Scenario,
    queue: String,
    tag: String,
    producer: usize,
    measure_from: Instant,
    stop_at: Instant,
    counter_base: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> Samples {
    let mut samples = Samples::default();
    let mut batch_index: u64 = 0;
    let mut previous_batch: Vec<InsertParams> = Vec::new();

    while Instant::now() < stop_at && !stop.load(Ordering::Relaxed) {
        let base = counter_base.fetch_add(scenario.batch_size as u64, Ordering::Relaxed);
        let mut jobs: Vec<InsertParams> = (0..scenario.batch_size as u64)
            .map(|offset| job_params(&queue, &tag, producer, base + offset, scenario.unique))
            .collect();

        // A replayed key: the first job repeats one this producer already
        // inserted, so the batch hits exactly one unique conflict.
        let duplicate = scenario.unique
            && scenario.duplicate_batch_pct > 0
            && !previous_batch.is_empty()
            && (batch_index + 1) * u64::from(scenario.duplicate_batch_pct) / 100
                > batch_index * u64::from(scenario.duplicate_batch_pct) / 100;
        if duplicate {
            jobs[0] = previous_batch[0].clone();
        }
        batch_index += 1;

        let started = Instant::now();
        let mut statement_ms = None;
        let mut commit_ms = None;
        let mut fallback_rows_ms: Vec<f64> = Vec::new();

        // Rows the insert returned (or, for the queue-storage batch, reports):
        // a skipping insert omits the jobs whose unique key is taken.
        let outcome: Result<usize, AwaError> = match scenario.mode {
            Mode::InsertMany if scenario.split_commit => {
                let mut tx = pool.begin().await.expect("begin");
                let statement_started = Instant::now();
                let result = insert_many(&mut *tx, &jobs).await;
                let statement_elapsed = statement_started.elapsed();
                match result {
                    Ok(rows) => {
                        let commit_started = Instant::now();
                        tx.commit().await.expect("commit");
                        statement_ms = Some(statement_elapsed.as_secs_f64() * 1e3);
                        commit_ms = Some(commit_started.elapsed().as_secs_f64() * 1e3);
                        Ok(rows.len())
                    }
                    Err(err) => Err(err),
                }
            }
            Mode::InsertMany => insert_many(&pool, &jobs).await.map(|rows| rows.len()),
            Mode::InsertManyCopy => insert_many_copy_from_pool(&pool, &jobs)
                .await
                .map(|rows| rows.len()),
            Mode::InsertManySkip => insert_many_skip_unique_conflicts(&pool, &jobs)
                .await
                .map(|rows| rows.len()),
            Mode::QueueStorage => store.enqueue_params_batch(&pool, &jobs).await,
            Mode::LegacyRowFunction => legacy_row_function_insert(&pool, &jobs).await,
        };

        let mut fallback_count = 0u64;
        let inserted: u64 = match outcome {
            Ok(rows) => rows as u64,
            Err(err) if is_unique_violation(&err) => {
                // What a replaying producer does on a unique conflict:
                // insert the batch again one row at a time, skipping
                // duplicates.
                let mut fallback_inserted = 0u64;
                for job in &jobs {
                    let row_started = Instant::now();
                    match insert_many(&pool, std::slice::from_ref(job)).await {
                        Ok(rows) => fallback_inserted += rows.len() as u64,
                        Err(err) if is_unique_violation(&err) => {}
                        Err(err) => panic!("fallback insert failed: {err}"),
                    }
                    fallback_rows_ms.push(row_started.elapsed().as_secs_f64() * 1e3);
                    fallback_count += 1;
                }
                fallback_inserted
            }
            Err(err) => panic!("enqueue failed: {err}"),
        };
        let elapsed = started.elapsed();

        samples.acknowledged_total += inserted;
        if started >= measure_from {
            samples.batch_ms.push(elapsed.as_secs_f64() * 1e3);
            samples.requested += scenario.batch_size as u64;
            samples.jobs += inserted;
            samples.batches += 1;
            if let Some(ms) = statement_ms {
                samples.statement_ms.push(ms);
            }
            if let Some(ms) = commit_ms {
                samples.commit_ms.push(ms);
            }
            if fallback_count > 0 {
                samples.duplicate_batches += 1;
                samples.fallback_rows += fallback_count;
                samples.fallback_row_ms.extend(fallback_rows_ms);
            }
        }
        previous_batch = jobs;
    }
    samples
}

async fn run_scenario(pool: &PgPool, scenario: &Scenario) -> ScenarioResult {
    let queue = format!(
        "lane_bench_{}",
        Uuid::new_v4().simple().to_string().get(..12).unwrap()
    );
    let tag = Uuid::new_v4().simple().to_string();
    seed_queue_shards(pool, &queue, scenario.shards).await;

    let producer_pool = bench_pool(scenario.producers as u32 + 4).await;
    let store = Arc::new(QueueStorage::new(QueueStorageConfig::default()).expect("queue storage"));

    let started = Instant::now();
    let measure_from = started + WARMUP;
    let stop_at = measure_from + scenario.measured;
    let counter_base = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let mut tasks = Vec::new();
    for producer in 0..scenario.producers {
        tasks.push(tokio::spawn(producer_loop(
            producer_pool.clone(),
            Arc::clone(&store),
            scenario.clone(),
            queue.clone(),
            tag.clone(),
            producer,
            measure_from,
            stop_at,
            Arc::clone(&counter_base),
            Arc::clone(&stop),
        )));
    }

    // Sample advisory-lock waiters over the measured window only.
    tokio::time::sleep_until(tokio::time::Instant::from_std(measure_from)).await;
    let mut waiter_samples: Vec<i64> = Vec::new();
    while Instant::now() < stop_at {
        waiter_samples.push(waiter_count(pool).await);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let mut samples = Samples::default();
    for task in tasks {
        samples.merge(task.await.expect("producer task"));
    }
    producer_pool.close().await;

    let (landed, distinct_lane_seqs): (i64, i64) = sqlx::query_as(
        r#"
        SELECT count(*)::bigint,
               count(DISTINCT (enqueue_shard, lane_seq))::bigint
        FROM awa.ready_entries
        WHERE queue = $1
        "#,
    )
    .bind(&queue)
    .fetch_one(pool)
    .await
    .expect("count landed rows");

    // Claim everything that landed, from every shard, with the claim path the
    // workers use.
    let drained = if scenario.drain {
        let drain_started = Instant::now();
        let mut claimed = 0i64;
        loop {
            let batch = store
                .claim_runtime_batch(pool, &queue, 500, Duration::from_secs(3_600))
                .await
                .expect("claim batch");
            if batch.is_empty() {
                break;
            }
            claimed += batch.len() as i64;
        }
        Some((claimed, drain_started.elapsed().as_secs_f64()))
    } else {
        None
    };

    let latencies = sorted(&samples.batch_ms);
    let window = scenario.measured.as_secs_f64();
    ScenarioResult {
        scenario: scenario.clone(),
        jobs_per_s: samples.jobs as f64 / window,
        p50_ms: percentile(&latencies, 0.50),
        p95_ms: percentile(&latencies, 0.95),
        p99_ms: percentile(&latencies, 0.99),
        max_ms: latencies.last().copied().unwrap_or(0.0),
        mean_waiters: if waiter_samples.is_empty() {
            0.0
        } else {
            waiter_samples.iter().sum::<i64>() as f64 / waiter_samples.len() as f64
        },
        samples,
        landed,
        distinct_lane_seqs,
        drained,
    }
}

fn print_result(result: &ScenarioResult) {
    let scenario = &result.scenario;
    println!(
        "[lane-bench] {name} mode={mode} producers={producers} batch={batch} unique={unique} \
         shards={shards} dup_pct={dup} | {rate:.0} jobs/s | batch ms p50={p50:.1} p95={p95:.1} \
         p99={p99:.1} max={max:.1} | waiters_mean={waiters:.1} | inserted={inserted} of \
         requested={requested} | landed={landed} distinct_seqs={distinct}",
        name = scenario.name,
        mode = scenario.mode.label(),
        producers = scenario.producers,
        batch = scenario.batch_size,
        unique = scenario.unique,
        shards = scenario.shards,
        dup = scenario.duplicate_batch_pct,
        rate = result.jobs_per_s,
        p50 = result.p50_ms,
        p95 = result.p95_ms,
        p99 = result.p99_ms,
        max = result.max_ms,
        waiters = result.mean_waiters,
        inserted = result.samples.jobs,
        requested = result.samples.requested,
        landed = result.landed,
        distinct = result.distinct_lane_seqs,
    );
    if !result.samples.statement_ms.is_empty() {
        let statement = sorted(&result.samples.statement_ms);
        let commit = sorted(&result.samples.commit_ms);
        println!(
            "[lane-bench]   split: statement ms p50={:.1} p99={:.1} | commit ms p50={:.2} p99={:.2}",
            percentile(&statement, 0.5),
            percentile(&statement, 0.99),
            percentile(&commit, 0.5),
            percentile(&commit, 0.99),
        );
    }
    if let Some((claimed, seconds)) = result.drained {
        println!(
            "[lane-bench]   drain: claimed {claimed} of {} landed in {seconds:.2}s ({:.0}/s)",
            result.landed,
            claimed as f64 / seconds.max(1e-9),
        );
    }
    if result.samples.duplicate_batches > 0 {
        let rows = sorted(&result.samples.fallback_row_ms);
        println!(
            "[lane-bench]   fallback: {} duplicate batches, {} single-row statements, \
             row ms p50={:.1} p99={:.1} max={:.1}",
            result.samples.duplicate_batches,
            result.samples.fallback_rows,
            percentile(&rows, 0.5),
            percentile(&rows, 0.99),
            rows.last().copied().unwrap_or(0.0),
        );
    }
}

fn emit_json(result: &ScenarioResult) {
    let mut outcomes = HashMap::new();
    outcomes.insert("jobs".to_string(), result.samples.jobs);
    outcomes.insert("requested".to_string(), result.samples.requested);
    outcomes.insert("batches".to_string(), result.samples.batches);
    BenchmarkResult {
        schema_version: SCHEMA_VERSION,
        scenario: result.scenario.name.clone(),
        language: "rust".to_string(),
        seeded: result.landed.max(0) as u64,
        metrics: BenchMetrics {
            throughput: None,
            enqueue_per_s: Some(result.jobs_per_s),
            drain_time_s: None,
            latency_ms: Some(BenchLatency {
                p50: Some(result.p50_ms),
                p95: Some(result.p95_ms),
                p99: Some(result.p99_ms),
            }),
            rescue: None,
        },
        outcomes,
        metadata: Some(serde_json::json!({
            "producers": result.scenario.producers,
            "batch_size": result.scenario.batch_size,
            "unique": result.scenario.unique,
            "shards": result.scenario.shards,
            "mean_lock_waiters": result.mean_waiters,
        })),
    }
    .emit();
}

/// Lane integrity: exactly the jobs the inserts returned landed, each once,
/// under a distinct `(shard, lane_seq)` (no producer lost or duplicated a
/// reservation).
fn assert_lane_integrity(result: &ScenarioResult) {
    if let Some((claimed, _)) = result.drained {
        assert_eq!(
            claimed, result.landed,
            "{}: claim path did not drain every shard",
            result.scenario.name
        );
    }
    assert_eq!(
        result.landed, result.distinct_lane_seqs,
        "{}: ready rows share a (shard, lane_seq)",
        result.scenario.name
    );
    assert_eq!(
        result.landed as u64, result.samples.acknowledged_total,
        "{}: rows landed against jobs the inserts returned",
        result.scenario.name
    );
}

async fn bench_setup() -> PgPool {
    let pool = setup::setup(16).await;
    setup::activate_queue_storage(&pool).await;
    pool
}

fn scenario(
    name: &str,
    producers: usize,
    shards: i16,
    unique: bool,
    duplicate_batch_pct: u32,
) -> Scenario {
    Scenario {
        name: name.to_string(),
        mode: Mode::InsertManySkip,
        producers,
        batch_size: 185,
        unique,
        shards,
        duplicate_batch_pct,
        measured: Duration::from_secs(8),
        split_commit: false,
        drain: false,
    }
}

/// Fixed scenarios whose results gate regressions in the nightly run.
///
/// Absolute floors sit well below the rates a shared runner sustains, and the
/// latency ceiling well above, so they only catch a lane that has stopped
/// making progress; `benchmarks/baseline.json` carries the regression
/// thresholds on rate and p99. What the gate asserts about lock hold time is
/// relative, because it must hold on any runner: one producer's 185-row batch
/// has to take well under the time the same rows take through the per-row
/// function, measured in the same run, and sixteen producers on one lane have
/// to out-run what the per-row function could ever sustain for it. Every
/// bound goes through `ci_timing` so a contended runner loosens it.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore]
async fn test_enqueue_lane_contention_gate() {
    let pool = bench_setup().await;

    let mut row_function = scenario("enqueue_lane_1p_row_function_185", 1, 1, true, 0);
    row_function.mode = Mode::LegacyRowFunction;
    row_function.measured = Duration::from_secs(4);
    let row_function = run_scenario(&pool, &row_function).await;
    print_result(&row_function);
    assert_lane_integrity(&row_function);

    let mut one_producer = scenario("enqueue_lane_1p_unique_185", 1, 1, true, 0);
    one_producer.measured = Duration::from_secs(4);
    let one_producer = run_scenario(&pool, &one_producer).await;
    print_result(&one_producer);
    assert_lane_integrity(&one_producer);

    let single_lane = run_scenario(
        &pool,
        &scenario("enqueue_lane_16p_unique_185", 16, 1, true, 0),
    )
    .await;
    print_result(&single_lane);
    emit_json(&single_lane);
    assert_lane_integrity(&single_lane);

    let sharded = run_scenario(
        &pool,
        &scenario("enqueue_lane_16p_unique_185_shards4", 16, 4, true, 0),
    )
    .await;
    print_result(&sharded);
    emit_json(&sharded);
    assert_lane_integrity(&sharded);

    let floor = ci_timing::contention_floor(GATE_MIN_JOBS_PER_S);
    assert!(
        single_lane.jobs_per_s >= floor as f64,
        "single-lane enqueue rate {:.0}/s fell below the floor of {floor}/s",
        single_lane.jobs_per_s
    );
    let p99_ceiling = ci_timing::scaled_timeout(Duration::from_millis(GATE_MAX_P99_MS));
    assert!(
        Duration::from_secs_f64(single_lane.p99_ms / 1e3) <= p99_ceiling,
        "single-lane p99 batch latency {:.0}ms exceeded {:?}",
        single_lane.p99_ms,
        p99_ceiling
    );

    // The batch holds the lane lock for a fraction of the per-row function's
    // time, and the lane's ceiling follows. Both bounds are ratios of numbers
    // measured moments apart on the same machine.
    let max_hold_ratio = (GATE_MAX_HOLD_RATIO * ci_timing::chaos_timeout_multiplier()).min(0.9);
    let hold_ratio = one_producer.p50_ms / row_function.p50_ms;
    assert!(
        hold_ratio <= max_hold_ratio,
        "a 185-row batch took {:.1}ms against {:.1}ms through the per-row function \
         (ratio {hold_ratio:.2}, limit {max_hold_ratio:.2})",
        one_producer.p50_ms,
        row_function.p50_ms
    );
    let min_scaling = GATE_MIN_LANE_SCALING / ci_timing::chaos_timeout_multiplier();
    let lane_scaling = single_lane.jobs_per_s / row_function.jobs_per_s;
    assert!(
        lane_scaling >= min_scaling.max(1.0),
        "16 producers on one lane sustain {:.0}/s, {lane_scaling:.1}x the per-row function's \
         single-producer {:.0}/s (need {:.1}x)",
        single_lane.jobs_per_s,
        row_function.jobs_per_s,
        min_scaling.max(1.0)
    );
}

const GATE_MIN_JOBS_PER_S: i64 = 2_000;
const GATE_MAX_P99_MS: u64 = 15_000;
const GATE_MAX_HOLD_RATIO: f64 = 0.4;
const GATE_MIN_LANE_SCALING: f64 = 2.0;

/// Exploration sweep; see the module docs for the tunables.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_enqueue_lane_contention_matrix() {
    let pool = bench_setup().await;
    if env_usize("AWA_LANE_BENCH_WITHOUT_PATCH", 0) != 0 {
        sqlx::query(
            "DROP FUNCTION IF EXISTS awa.insert_jobs_compat_batch(text[], text[], jsonb[], \
             text[], smallint[], smallint[], timestamptz[], jsonb[], jsonb[], bytea[], text[], \
             text[], boolean)",
        )
        .execute(&pool)
        .await
        .expect("drop the batch function");
    }
    let producers: Vec<usize> = env_string("AWA_LANE_BENCH_PRODUCERS", "1,4,8,16,32")
        .split(',')
        .filter_map(|raw| raw.trim().parse().ok())
        .collect();
    let mode = Mode::parse(&env_string("AWA_LANE_BENCH_MODE", "insert_many"));
    let batch_size = env_usize("AWA_LANE_BENCH_BATCH", 185);
    let unique = env_usize("AWA_LANE_BENCH_UNIQUE", 1) != 0;
    let shards = env_usize("AWA_LANE_BENCH_SHARDS", 1) as i16;
    let duplicate_pct = env_usize("AWA_LANE_BENCH_DUP_PCT", 0) as u32;
    let secs = env_usize("AWA_LANE_BENCH_SECS", 8) as u64;
    let split = env_usize("AWA_LANE_BENCH_SPLIT", 0) != 0;

    for producer_count in producers {
        let result = run_scenario(
            &pool,
            &Scenario {
                name: format!("matrix_{producer_count}p"),
                mode,
                producers: producer_count,
                batch_size,
                unique,
                shards,
                duplicate_batch_pct: duplicate_pct,
                measured: Duration::from_secs(secs),
                split_commit: split,
                drain: env_usize("AWA_LANE_BENCH_DRAIN", 0) != 0,
            },
        )
        .await;
        print_result(&result);
        assert_lane_integrity(&result);
    }
}
