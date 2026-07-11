//! Integration tests for queue_storage runtime flows.
//!
//! These tests exercise the full dispatcher/worker/maintenance wiring with the
//! queue_storage backend enabled.

use awa::model::{
    admin, batch_operations, insert, migrations, storage, AwaError, BatchOperationFilter,
    BatchOperationSpec, PruneOutcome, QueueStorage, QueueStorageConfig, RotateOutcome, SkipReason,
    SubmitBatchOperation,
};
use awa::{
    Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, JobRow, JobState, QueueConfig,
    UniqueOpts, Worker,
};
use chrono::{DateTime, Utc};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

#[derive(Debug, PartialEq, sqlx::FromRow)]
struct RawReceiptClaimRow {
    ready_slot: i32,
    ready_generation: i64,
    job_id: i64,
    priority: i16,
    attempt: i16,
    run_lease: i64,
    lane_seq: i64,
    claim_slot: i32,
}

fn install_in_memory_metrics() -> (InMemoryMetricExporter, SdkMeterProvider) {
    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());
    (exporter, meter_provider)
}

fn sum_counter_metric_with_attribute(
    resource_metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
    attr_name: &str,
    attr_value: &str,
) -> u64 {
    let mut total = 0;
    for rm in resource_metrics {
        for scope_metrics in rm.scope_metrics() {
            for metric in scope_metrics.metrics() {
                if metric.name() != name {
                    continue;
                }
                if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
                    total += sum
                        .data_points()
                        .filter(|dp| {
                            dp.attributes().any(|kv| {
                                kv.key.as_str() == attr_name && kv.value.as_str() == attr_value
                            })
                        })
                        .map(|dp| dp.value())
                        .sum::<u64>();
                }
            }
        }
    }
    total
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

fn validate_database_name(database_name: &str) {
    assert!(
        !database_name.is_empty()
            && database_name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
        "queue_storage test database names must use only [A-Za-z0-9_]"
    );
}

/// Every test gets its own database cloned from a once-per-process template,
/// so tests run concurrently without sharing state and without re-running the
/// full migration chain 89 times (which dominated this binary's runtime).
const TEST_DB_PREFIX: &str = "awa_qsrt_";

static TEMPLATE_DB: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
static TEST_DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Budget of concurrently-open test pool connections. Each test acquires
/// permits equal to its pool's `max_connections`, so the aggregate worst case
/// stays below the Postgres server's default `max_connections = 100`
/// (with headroom for the transient admin/template connections). Uncapped,
/// ~90 parallel tests exhaust the server and fail with `PoolTimedOut`.
const TEST_CONNECTION_BUDGET: u32 = 80;
static TEST_CONNECTION_PERMITS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(TEST_CONNECTION_BUDGET as usize);

/// Per-test permit floor. Each test runs a full client (dispatcher, workers,
/// maintenance leader); without a floor, small-pool tests admit ~20 runtimes
/// at once and CPU starvation makes wait-for-state assertions time out.
/// Flooring the weight at BUDGET/8 keeps test concurrency near 8 regardless
/// of pool size while still charging big-pool tests their real weight.
const TEST_PERMIT_FLOOR: u32 = TEST_CONNECTION_BUDGET / 8;

/// Held for the duration of a test; dropping it returns the test's connection
/// budget to the pool of permits.
struct TestDbGuard {
    _permit: tokio::sync::SemaphorePermit<'static>,
}

async fn connect_admin_pool() -> sqlx::PgPool {
    let admin_url = replace_database_name(&base_database_url(), "postgres");
    PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await
        .expect("Failed to connect to admin database for queue_storage tests")
}

async fn ensure_template_database() -> &'static str {
    TEMPLATE_DB
        .get_or_init(|| async {
            let template_name = format!("{TEST_DB_PREFIX}template");
            let admin_pool = connect_admin_pool().await;

            // Drop leftovers from previous runs (including a stale template,
            // so migration changes always take effect). Cargo runs test
            // binaries sequentially and only this binary uses the prefix, so
            // nothing live can match. A previous local run leaves ~90 small
            // databases; dropping them serially costs ~15-20s of the first
            // run on a reused cluster (CI clusters start clean and skip
            // this). Concurrent drops are not worth it: DROP DATABASE
            // serializes server-side and the queued tasks just time out.
            let leftovers: Vec<String> =
                sqlx::query_scalar("SELECT datname FROM pg_database WHERE datname LIKE $1")
                    .bind(format!("{TEST_DB_PREFIX}%"))
                    .fetch_all(&admin_pool)
                    .await
                    .expect("Failed to list leftover queue_storage test databases");
            for leftover in leftovers {
                validate_database_name(&leftover);
                sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS {leftover} WITH (FORCE)"))
                    .execute(&admin_pool)
                    .await
                    .expect("Failed to drop leftover queue_storage test database");
            }

            sqlx::raw_sql(&format!("CREATE DATABASE {template_name}"))
                .execute(&admin_pool)
                .await
                .expect("Failed to create queue_storage template database");
            admin_pool.close().await;

            let template_url = replace_database_name(&base_database_url(), &template_name);
            let template_pool = PgPoolOptions::new()
                .max_connections(4)
                .connect(&template_url)
                .await
                .expect("Failed to connect to queue_storage template database");
            migrations::run(&template_pool)
                .await
                .expect("Failed to run migrations on queue_storage template database");
            // CREATE DATABASE ... TEMPLATE requires the template to have no
            // active connections.
            template_pool.close().await;

            template_name
        })
        .await
}

async fn setup_pool(max_connections: u32) -> (TestDbGuard, sqlx::PgPool) {
    assert!(
        max_connections <= TEST_CONNECTION_BUDGET,
        "test requested a {max_connections}-connection pool, above the \
         {TEST_CONNECTION_BUDGET}-connection test budget — it would deadlock waiting for permits"
    );
    let permit = TEST_CONNECTION_PERMITS
        .acquire_many(max_connections.max(TEST_PERMIT_FLOOR))
        .await
        .expect("test connection semaphore is never closed");
    let template_name = ensure_template_database().await;
    let db_name = format!(
        "{TEST_DB_PREFIX}{}_{}",
        std::process::id(),
        TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
    validate_database_name(&db_name);

    let admin_pool = connect_admin_pool().await;
    let create_sql = format!("CREATE DATABASE {db_name} TEMPLATE {template_name}");
    let mut attempts = 0;
    loop {
        match sqlx::raw_sql(&create_sql).execute(&admin_pool).await {
            Ok(_) => break,
            // 55006: "source database is being accessed by other users" —
            // another test is mid-copy from the same template. Retry.
            Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("55006") => {
                attempts += 1;
                assert!(
                    attempts < 600,
                    "gave up cloning queue_storage template database after {attempts} attempts"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => panic!("Failed to create queue_storage test database {db_name}: {err}"),
        }
    }
    admin_pool.close().await;

    let url = replace_database_name(&base_database_url(), &db_name);
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&url)
        .await
        .expect("Failed to connect to database");
    // Guard first: call sites bind `let (_db_guard, pool) = ...`, and locals
    // drop in reverse declaration order, so the pool handle drops before the
    // permit is released.
    (TestDbGuard { _permit: permit }, pool)
}

#[tokio::test]
async fn test_queue_storage_prepare_schema_completes_with_single_connection_pool() {
    let (_db_guard, pool) = setup_pool(1).await;
    let store =
        QueueStorage::new(QueueStorageConfig::default()).expect("queue storage config is valid");

    tokio::time::timeout(Duration::from_secs(30), store.prepare_schema(&pool))
        .await
        .expect("prepare_schema should not self-starve on a one-connection pool")
        .expect("prepare_schema should succeed");

    assert!(
        storage::queue_storage_schema_ready(&pool, store.schema())
            .await
            .expect("schema readiness query should succeed"),
        "prepared queue-storage schema should be reported as ready"
    );
}

#[tokio::test]
async fn test_queue_storage_prepare_schema_concurrent_startups_serialize() {
    let (_db_guard, pool) = setup_pool(4).await;

    let mut handles = Vec::new();
    for _ in 0..4 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let store = QueueStorage::new(QueueStorageConfig::default())
                .expect("queue storage config is valid");
            store.prepare_schema(&pool).await
        }));
    }

    tokio::time::timeout(Duration::from_secs(30), async {
        for handle in handles {
            handle
                .await
                .expect("prepare_schema task should not panic")
                .expect("prepare_schema should succeed");
        }
    })
    .await
    .expect("concurrent prepare_schema calls should not deadlock");

    assert!(
        storage::queue_storage_schema_ready(&pool, "awa")
            .await
            .expect("schema readiness query should succeed"),
        "prepared queue-storage schema should be reported as ready"
    );
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
    // Default `transition_role` so the inserted runtime satisfies the
    // tightened `enter_mixed_transition` gate (which requires a live
    // queue_storage_target). Tests that assert on canonical-only
    // pre-flight should pass `capability=canonical` here; the gate's
    // canonical-blocker check fires first.
    let role = match capability {
        "canonical" => "auto",
        "canonical_drain_only" => "canonical_drain",
        "queue_storage" => "queue_storage_target",
        _ => "auto",
    };
    let instance_id = uuid::Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_instances (
            instance_id,
            hostname,
            pid,
            version,
            storage_capability,
            transition_role,
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
            $3,
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
    .bind(role)
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
    // Tests that go through this helper exercise the legacy
    // (non-receipts) lease-materialization path. The receipts mode
    // tests construct their own config with `lease_claim_receipts:
    // true`; this helper pins the legacy mode explicitly so it stays
    // pinned across default flips (see ADR-023 Phase 6).
    create_store_with_config(
        pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            lease_claim_receipts: false,
            ..Default::default()
        },
    )
    .await
}

async fn run_queue_storage_batch_to_completion(pool: &sqlx::PgPool, operation_id: Uuid) {
    let runner = Uuid::new_v4();
    for _ in 0..100 {
        let outcome = batch_operations::run_one_batch_operation_chunk(pool, runner, 10)
            .await
            .expect("run batch operation chunk");
        if outcome.finalized || !outcome.claimed {
            break;
        }
    }
    let operation = batch_operations::get_batch_operation(pool, operation_id)
        .await
        .expect("get batch operation");
    assert_eq!(
        operation.state,
        batch_operations::BatchOperationState::Completed
    );
}

/// Scan candidate keys until every shard in `[0, shards)` has a
/// representative ordering key that hashes to it. Used by shard
/// fairness / shard-lowering tests so the test setup doesn't depend
/// on which strings happen to route to which shard.
fn build_keys_per_shard(shards: i16) -> std::collections::HashMap<i16, Vec<u8>> {
    let mut keys: std::collections::HashMap<i16, Vec<u8>> = std::collections::HashMap::new();
    for n in 0..1_000_000u64 {
        if keys.len() as i16 == shards {
            break;
        }
        let key = format!("shard-fixture-{n}");
        let shard = awa_model::queue_storage::shard_for_ordering_key(key.as_bytes(), shards);
        keys.entry(shard).or_insert_with(|| key.into_bytes());
    }
    assert_eq!(
        keys.len() as i16,
        shards,
        "test setup should find one key per shard",
    );
    keys
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

async fn lease_claim_batch_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    let sql = format!(
        "SELECT count(*)::bigint FROM {}.lease_claim_batches",
        store.schema()
    );
    sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .expect("Failed to count lease_claim_batches")
}

/// Count of receipt-backed attempts that are currently "open" — claimed
/// but not yet closed, materialized into a live lease row, or moved to
/// another state table. The runtime derives this set from the partitioned
/// claim ledgers with anti-joins; this helper mirrors that query so
/// test assertions read the same definition the runtime does.
async fn open_receipt_claim_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    let schema = store.schema();
    let sql = format!(
        r#"
        WITH claim_items AS (
            SELECT
                claims.claim_slot,
                claims.job_id,
                claims.run_lease,
                claims.receipt_id,
                claims.closed_at
            FROM {schema}.lease_claims AS claims
            UNION ALL
            SELECT
                claim_batches.claim_slot,
                items.job_id,
                items.run_lease,
                items.receipt_id,
                NULL::timestamptz AS closed_at
            FROM {schema}.lease_claim_batches AS claim_batches
            CROSS JOIN LATERAL unnest(
                claim_batches.job_ids,
                claim_batches.run_leases,
                claim_batches.receipt_ids
            ) AS items(job_id, run_lease, receipt_id)
        )
        SELECT count(*)::bigint
        FROM claim_items AS claims
        WHERE claims.closed_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM {schema}.lease_claim_closures AS closures
              WHERE closures.claim_slot = claims.claim_slot
                AND closures.job_id = claims.job_id
                AND closures.run_lease = claims.run_lease
          )
          AND NOT EXISTS (
              SELECT 1
              FROM {schema}.lease_claim_closure_batches AS closure_batches
              WHERE closure_batches.claim_slot = claims.claim_slot
                AND closure_batches.receipt_ranges @> claims.receipt_id
          )
          AND NOT EXISTS (
              SELECT 1 FROM {schema}.leases AS lease
              WHERE lease.job_id = claims.job_id
                AND lease.run_lease = claims.run_lease
          )
          AND NOT EXISTS (
              SELECT 1 FROM {schema}.deferred_jobs AS deferred
              WHERE deferred.job_id = claims.job_id
                AND deferred.run_lease = claims.run_lease
          )
          AND NOT EXISTS (
              SELECT 1 FROM {schema}.terminal_jobs AS terminal
              WHERE terminal.job_id = claims.job_id
                AND terminal.run_lease = claims.run_lease
          )
          AND NOT EXISTS (
              SELECT 1 FROM {schema}.dlq_entries AS dlq
              WHERE dlq.job_id = claims.job_id
                AND dlq.run_lease = claims.run_lease
        )
        "#,
    );
    sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .expect("Failed to count open receipt claims (derived)")
}

async fn receipt_claim_slot_for_job(pool: &sqlx::PgPool, store: &QueueStorage, job_id: i64) -> i32 {
    let schema = store.schema();
    sqlx::query_scalar(&format!(
        r#"
        SELECT claim_slot
        FROM (
            SELECT claims.claim_slot, claims.job_id, claims.run_lease
            FROM {schema}.lease_claims AS claims
            UNION ALL
            SELECT claim_batches.claim_slot, items.job_id, items.run_lease
            FROM {schema}.lease_claim_batches AS claim_batches
            CROSS JOIN LATERAL unnest(
                claim_batches.job_ids,
                claim_batches.run_leases
            ) AS items(job_id, run_lease)
        ) AS claim_items
        WHERE job_id = $1
        ORDER BY run_lease DESC
        LIMIT 1
        "#
    ))
    .bind(job_id)
    .fetch_one(pool)
    .await
    .expect("read claim_slot from receipt claim evidence")
}

async fn receipt_claim_count_in_child(
    pool: &sqlx::PgPool,
    schema: &str,
    claim_slot: i32,
    job_id: i64,
) -> i64 {
    let claim_child = format!("{schema}.lease_claims_{claim_slot}");
    let claim_batch_child = format!("{schema}.lease_claim_batches_{claim_slot}");
    sqlx::query_scalar(&format!(
        r#"
        SELECT
            (SELECT count(*)::bigint FROM {claim_child} WHERE job_id = $1)
            +
            COALESCE((
                SELECT count(*)::bigint
                FROM {claim_batch_child} AS claim_batches
                CROSS JOIN LATERAL unnest(claim_batches.job_ids) AS items(job_id)
                WHERE items.job_id = $1
            ), 0)
        "#
    ))
    .bind(job_id)
    .fetch_one(pool)
    .await
    .expect("count receipt claim evidence in child")
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

async fn lease_claim_closure_batch_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    let sql = format!(
        "SELECT count(*)::bigint FROM {}.lease_claim_closure_batches",
        store.schema()
    );
    sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .expect("Failed to count lease_claim_closure_batches")
}

async fn ready_tombstone_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    let sql = format!(
        "SELECT count(*)::bigint FROM {}.ready_tombstones",
        store.schema()
    );
    sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .expect("Failed to count ready_tombstones")
}

async fn ready_segment_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    let sql = format!(
        "SELECT count(*)::bigint FROM {}.ready_segments",
        store.schema()
    );
    sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .expect("Failed to count ready_segments")
}

async fn tombstone_ready_job(pool: &sqlx::PgPool, store: &QueueStorage, job_id: i64) {
    let schema = store.schema();
    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.ready_tombstones (
            ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
        )
        SELECT ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
        FROM {schema}.ready_entries
        WHERE job_id = $1
        ON CONFLICT DO NOTHING
        "#
    ))
    .bind(job_id)
    .execute(pool)
    .await
    .expect("Failed to tombstone ready job");
}

async fn claim_cursor_for(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
    queue: &str,
    priority: i16,
    enqueue_shard: i16,
) -> i64 {
    let schema = store.schema();
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT {schema}.sequence_next_value(seq_name)
         FROM {schema}.queue_claim_heads
         WHERE queue = $1 AND priority = $2 AND enqueue_shard = $3"
    ))
    .bind(queue)
    .bind(priority)
    .bind(enqueue_shard)
    .fetch_one(pool)
    .await
    .expect("Failed to read claim cursor")
}

fn queue_storage_client<W: Worker + 'static>(
    pool: &sqlx::PgPool,
    queue: &str,
    store_config: QueueStorageConfig,
    worker: W,
) -> Client {
    let deadline_duration = if store_config.lease_claim_receipts {
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
            Duration::from_secs(60),
            Duration::from_millis(50),
        )
        // Queue-ring and claim-ring prune actually TRUNCATE idle child
        // partitions and fold terminal rows into rollups. Tests assert on
        // retained rows — post-shutdown admin retry / DLQ moves read
        // `done_entries`, and hard-coded lease_claim/closure counts assume
        // those rows survive — so push both rotations past the test's
        // wall-clock window. A 1s queue rotation with 4 slots lets prune
        // reclaim a failed job's terminal row ~4s after it lands, which made
        // every "observe Failed, then act on it" test a race. Tests that
        // exercise rotation/prune directly use an inline builder with a fast
        // cadence or drive `rotate_queues` / `rotate_claims` explicitly.
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
        match store.load_job(pool, job_id).await {
            Ok(Some(job)) => {
                last_state = Some(job.state);
                if target_states.contains(&job.state) {
                    return job;
                }
            }
            Ok(None) => {}
            Err(AwaError::Database(sqlx::Error::PoolTimedOut)) => {
                // The runtime tests intentionally run many clients in parallel
                // against small per-test pools. A transient local pool timeout
                // is not a state-machine failure; keep polling until the
                // caller's explicit timeout expires.
            }
            Err(err) => panic!("Failed to load queue_storage job: {err:?}"),
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

async fn wait_for_bool_flag(flag: &AtomicBool, wake: &Notify, timeout: Duration, message: &str) {
    let deadline = Instant::now() + timeout;
    loop {
        if flag.load(Ordering::SeqCst) {
            return;
        }

        let now = Instant::now();
        assert!(now < deadline, "{message}");
        let remaining = deadline.saturating_duration_since(now);
        let _ = tokio::time::timeout(remaining, wake.notified()).await;
    }
}

async fn age_receipt_claim(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
    job_id: i64,
    run_lease: i64,
    age: Duration,
) {
    let age_millis = i64::try_from(age.as_millis()).expect("test receipt age fits in i64 millis");
    sqlx::query(&format!(
        r#"
        WITH aged_rows AS (
            UPDATE {schema}.lease_claims
            SET claimed_at = clock_timestamp() - ($1 * interval '1 millisecond')
            WHERE job_id = $2 AND run_lease = $3
            RETURNING job_id
        ),
        aged_batches AS (
            UPDATE {schema}.lease_claim_batches AS claim_batches
            SET claimed_at = clock_timestamp() - ($1 * interval '1 millisecond')
            WHERE EXISTS (
                SELECT 1
                FROM unnest(claim_batches.job_ids, claim_batches.run_leases) AS items(job_id, run_lease)
                WHERE items.job_id = $2 AND items.run_lease = $3
            )
            RETURNING batch_id
        )
        SELECT (SELECT count(*) FROM aged_rows) + (SELECT count(*) FROM aged_batches)
        "#,
        schema = store.schema()
    ))
    .bind(age_millis)
    .bind(job_id)
    .bind(run_lease)
    .fetch_one(pool)
    .await
    .expect("Failed to age receipt claim for rescue test");
}

async fn age_attempt_heartbeat(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
    job_id: i64,
    run_lease: i64,
    age: Duration,
) {
    let age_millis = i64::try_from(age.as_millis()).expect("test heartbeat age fits in i64 millis");
    sqlx::query(&format!(
        "UPDATE {}.attempt_state
         SET heartbeat_at = clock_timestamp() - ($1 * interval '1 millisecond'),
             updated_at = clock_timestamp()
         WHERE job_id = $2 AND run_lease = $3",
        store.schema()
    ))
    .bind(age_millis)
    .bind(job_id)
    .bind(run_lease)
    .execute(pool)
    .await
    .expect("Failed to age attempt heartbeat for rescue test");
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

async fn wait_for_dlq_count(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
    queue: &str,
    expected: i64,
    timeout: Duration,
) {
    let start = Instant::now();

    loop {
        let count = dlq_count(pool, store, queue).await;
        if count == expected {
            return;
        }

        if start.elapsed() > timeout {
            panic!(
                "Timed out waiting for {expected} dlq rows in queue {queue}; last_count={count}",
            );
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_failed_done_count(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
    queue: &str,
    expected: i64,
    timeout: Duration,
) {
    let start = Instant::now();

    loop {
        let count = failed_done_count(pool, store, queue).await;
        if count == expected {
            return;
        }

        if start.elapsed() > timeout {
            panic!(
                "Timed out waiting for {expected} failed done rows in queue {queue}; last_count={count}",
            );
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
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

async fn completed_terminal_count(pool: &sqlx::PgPool, store: &QueueStorage, queue: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT count(*)::bigint FROM {}.terminal_jobs WHERE queue = $1 AND state = 'completed'",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("Failed to count completed terminal rows")
}

async fn receipt_completion_batch_count(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
    queue: &str,
) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT count(*)::bigint FROM {}.receipt_completion_batches WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("Failed to count receipt completion batches")
}

async fn receipt_completion_tombstone_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT count(*)::bigint FROM {}.receipt_completion_tombstones",
        store.schema()
    ))
    .fetch_one(pool)
    .await
    .expect("Failed to count receipt completion tombstones")
}

async fn done_body_columns(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
    job_id: i64,
) -> (
    Option<serde_json::Value>,
    Option<i16>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<serde_json::Value>,
) {
    sqlx::query_as(&format!(
        r#"
        SELECT args, max_attempts, run_at, created_at, payload
        FROM {}.done_entries
        WHERE job_id = $1
        ORDER BY finalized_at DESC
        LIMIT 1
        "#,
        store.schema()
    ))
    .bind(job_id)
    .fetch_one(pool)
    .await
    .expect("Failed to fetch done row body columns")
}

async fn terminal_view_body_columns(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
    job_id: i64,
) -> (
    serde_json::Value,
    i16,
    DateTime<Utc>,
    DateTime<Utc>,
    serde_json::Value,
) {
    sqlx::query_as(&format!(
        r#"
        SELECT args, max_attempts, run_at, created_at, payload
        FROM {}.terminal_jobs
        WHERE job_id = $1
        ORDER BY finalized_at DESC
        LIMIT 1
        "#,
        store.schema()
    ))
    .bind(job_id)
    .fetch_one(pool)
    .await
    .expect("Failed to fetch terminal_jobs view body columns")
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

#[derive(Clone)]
struct BlockingCompleteWorkerGate {
    release: Arc<Notify>,
    released: Arc<AtomicBool>,
    entered: Arc<AtomicBool>,
    entered_wake: Arc<Notify>,
}

impl BlockingCompleteWorkerGate {
    fn new() -> Self {
        Self {
            release: Arc::new(Notify::new()),
            released: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(AtomicBool::new(false)),
            entered_wake: Arc::new(Notify::new()),
        }
    }

    fn worker(&self) -> BlockingCompleteWorker {
        BlockingCompleteWorker { gate: self.clone() }
    }

    async fn wait_until_entered(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if self.entered.load(Ordering::SeqCst) {
                return;
            }

            let now = Instant::now();
            if now >= deadline {
                panic!("timed out waiting for blocking worker to await release");
            }

            let remaining = deadline.saturating_duration_since(now);
            let _ = tokio::time::timeout(remaining, self.entered_wake.notified()).await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

struct BlockingCompleteWorker {
    gate: BlockingCompleteWorkerGate,
}

#[async_trait::async_trait]
impl Worker for BlockingCompleteWorker {
    fn kind(&self) -> &'static str {
        "complete_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        self.gate.entered.store(true, Ordering::SeqCst);
        self.gate.entered_wake.notify_waiters();
        loop {
            let notified = self.gate.release.notified();
            if self.gate.released.load(Ordering::SeqCst) {
                break;
            }
            notified.await;
        }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_storage_completed_done_row_is_narrow_and_hydrates_from_ready() {
    let (_db_guard, pool) = setup_pool(6).await;
    let queue = "qs_narrow_done_complete";
    let schema = "awa_qs_narrow_done_complete";
    let store = create_store(&pool, schema).await;
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

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("Failed to claim narrow done job");
    let claimed = claimed.into_iter().next().expect("missing claimed job");
    store
        .complete_runtime_batch(&pool, std::slice::from_ref(&claimed))
        .await
        .expect("Failed to complete narrow done job");

    let (args, max_attempts, run_at, created_at, payload) =
        done_body_columns(&pool, &store, job_id).await;
    assert!(
        args.is_none(),
        "ready-backed completed rows should not duplicate args"
    );
    assert!(
        max_attempts.is_none(),
        "ready-backed completed rows should not duplicate max_attempts"
    );
    assert!(
        run_at.is_none(),
        "ready-backed completed rows should not duplicate run_at"
    );
    assert!(
        created_at.is_none(),
        "ready-backed completed rows should not duplicate created_at"
    );
    assert!(
        payload.is_none(),
        "unchanged terminal payload should be elided and hydrated from ready_entries"
    );
    let (view_args, view_max_attempts, view_run_at, view_created_at, view_payload) =
        terminal_view_body_columns(&pool, &store, job_id).await;
    assert_eq!(view_args["id"], serde_json::json!(42));
    assert_eq!(view_max_attempts, claimed.job.max_attempts);
    assert_eq!(view_run_at, claimed.job.run_at);
    assert_eq!(view_created_at, claimed.job.created_at);
    assert_eq!(view_payload, serde_json::json!({}));

    let loaded = store
        .load_job(&pool, job_id)
        .await
        .expect("Failed to load completed narrow done job")
        .expect("completed job should be loadable");
    assert_eq!(loaded.state, JobState::Completed);
    assert_eq!(loaded.args["id"], serde_json::json!(42));
    assert_eq!(loaded.max_attempts, claimed.job.max_attempts);
    assert_eq!(loaded.created_at, claimed.job.created_at);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_storage_claim_runtime_does_not_write_ready_segment_cache() {
    let (_db_guard, pool) = setup_pool(6).await;
    let queue = "qs_claim_segment_cache_cold";
    let schema = "awa_qs_claim_segment_cache_cold";
    let store = create_store(&pool, schema).await;
    enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 42 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    sqlx::query(&format!(
        r#"
        UPDATE {schema}.queue_claim_heads
        SET ready_segment_slot = -17,
            ready_segment_generation = -18,
            ready_segment_next_lane_seq = -19
        WHERE queue = $1
        "#
    ))
    .bind(queue)
    .execute(&pool)
    .await
    .expect("seed legacy ready-segment cache sentinels");

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("Failed to claim cache-free segment job");
    let claimed = claimed.into_iter().next().expect("missing claimed job");

    let (cached_slot, cached_generation, cached_next): (Option<i32>, Option<i64>, Option<i64>) =
        sqlx::query_as(&format!(
            r#"
            SELECT ready_segment_slot, ready_segment_generation, ready_segment_next_lane_seq
            FROM {schema}.queue_claim_heads
            WHERE queue = $1
              AND priority = $2
              AND enqueue_shard = $3
            "#
        ))
        .bind(queue)
        .bind(claimed.claim.priority)
        .bind(claimed.claim.enqueue_shard)
        .fetch_one(&pool)
        .await
        .expect("fetch retained ready segment cache columns");

    assert_eq!(
        (cached_slot, cached_generation, cached_next),
        (Some(-17), Some(-18), Some(-19)),
        "claim routing must not write the retained legacy ready-segment cache columns"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_storage_failed_narrow_done_row_can_retry_from_ready_hydration() {
    let (_db_guard, pool) = setup_pool(6).await;
    let queue = "qs_narrow_done_retry";
    let schema = "awa_qs_narrow_done_retry";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 77 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("Failed to claim narrow failed job");
    let claimed = claimed.into_iter().next().expect("missing claimed job");
    store
        .fail_terminal(&pool, job_id, claimed.job.run_lease, "boom", None)
        .await
        .expect("Failed to fail narrow done job")
        .expect("running job should fail");

    let (args, max_attempts, run_at, created_at, payload) =
        done_body_columns(&pool, &store, job_id).await;
    assert!(
        args.is_none(),
        "ready-backed failed rows should not duplicate args"
    );
    assert!(
        max_attempts.is_none(),
        "ready-backed failed rows should not duplicate max_attempts"
    );
    assert!(
        run_at.is_none(),
        "ready-backed failed rows should not duplicate run_at"
    );
    assert!(
        created_at.is_none(),
        "ready-backed failed rows should not duplicate created_at"
    );
    assert!(
        payload.is_some(),
        "failed rows still store terminal payload delta with error history"
    );

    let retried = store
        .retry_job(&pool, job_id)
        .await
        .expect("Failed to retry failed narrow done row")
        .expect("failed narrow done row should retry");
    assert_eq!(retried.state, JobState::Available);
    assert_eq!(retried.args["id"], serde_json::json!(77));
    assert_eq!(retried.max_attempts, claimed.job.max_attempts);
    assert_eq!(retried.created_at, claimed.job.created_at);

    let counts = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to count retried queue");
    assert_eq!(
        counts.available, 1,
        "retry must delete the retained terminal backing row before re-enqueue"
    );
    let retried_claims = store
        .claim_runtime_batch(&pool, queue, 10, Duration::from_secs(30))
        .await
        .expect("Failed to claim retried narrow done job");
    assert_eq!(retried_claims.len(), 1);
    assert_eq!(retried_claims[0].job.id, job_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_storage_cancel_available_tombstones_and_retains_ready_backing() {
    let (_db_guard, pool) = setup_pool(6).await;
    let queue = "qs_wide_available_cancel";
    let schema = "awa_qs_wide_available_cancel";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 88 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let cancelled = store
        .cancel_job(&pool, job_id)
        .await
        .expect("Failed to cancel available job")
        .expect("available job should cancel");
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert_eq!(
        ready_tombstone_count(&pool, &store).await,
        1,
        "available cancel should tombstone the ready lane"
    );
    let retained_ready_rows: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {schema}.ready_entries WHERE job_id = $1"
    ))
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("count retained ready row");
    assert_eq!(
        retained_ready_rows, 1,
        "available cancel should retain the ready backing row until queue prune"
    );

    let (args, max_attempts, run_at, created_at, _payload) =
        done_body_columns(&pool, &store, job_id).await;
    assert_eq!(
        args.expect("available-cancel terminal row should retain args")["id"],
        serde_json::json!(88)
    );
    assert!(
        max_attempts.is_some(),
        "available-cancel terminal row should remain wide for direct done_entries readers"
    );
    assert!(
        run_at.is_some(),
        "available-cancel terminal row should remain wide for direct done_entries readers"
    );
    assert!(
        created_at.is_some(),
        "available-cancel terminal row should remain wide for direct done_entries readers"
    );

    let loaded = store
        .load_job(&pool, job_id)
        .await
        .expect("Failed to load cancelled available job")
        .expect("cancelled available job should be loadable");
    assert_eq!(loaded.state, JobState::Cancelled);
    assert_eq!(loaded.args["id"], serde_json::json!(88));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_storage_batch_set_priority_and_move_queue() {
    let (_db_guard, pool) = setup_pool(6).await;
    let source_queue = "qs_batch_source";
    let dest_queue = "qs_batch_dest";
    let schema = "awa_qs_batch_ops";
    let store = create_store(&pool, schema).await;
    sqlx::query("DELETE FROM awa.batch_operations")
        .execute(&pool)
        .await
        .expect("clean batch operations");

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 912 },
        InsertOpts {
            queue: source_queue.to_string(),
            priority: 4,
            ..Default::default()
        },
    )
    .await;

    let set_priority = batch_operations::submit_batch_operation(
        &pool,
        SubmitBatchOperation {
            spec: BatchOperationSpec::SetPriority { priority: 1 },
            filter: BatchOperationFilter {
                queue: Some(source_queue.to_string()),
                ..Default::default()
            },
            submitted_by: Some("test".to_string()),
            allow_all: false,
        },
    )
    .await
    .expect("submit set_priority batch op");
    run_queue_storage_batch_to_completion(&pool, set_priority.id).await;

    let reprioritized = store
        .load_job(&pool, job_id)
        .await
        .expect("load reprioritized job")
        .expect("reprioritized job should exist");
    assert_eq!(reprioritized.priority, 1);
    assert_eq!(
        reprioritized.metadata.get("_awa_original_priority"),
        Some(&serde_json::json!(4))
    );

    let move_queue = batch_operations::submit_batch_operation(
        &pool,
        SubmitBatchOperation {
            spec: BatchOperationSpec::MoveQueue {
                queue: dest_queue.to_string(),
                priority: Some(2),
            },
            filter: BatchOperationFilter {
                ids: Some(vec![job_id]),
                ..Default::default()
            },
            submitted_by: Some("test".to_string()),
            allow_all: false,
        },
    )
    .await
    .expect("submit move_queue batch op");
    run_queue_storage_batch_to_completion(&pool, move_queue.id).await;

    let moved = store
        .load_job(&pool, job_id)
        .await
        .expect("load moved job")
        .expect("moved job should exist");
    assert_eq!(moved.queue, dest_queue);
    assert_eq!(moved.priority, 2);
    assert_eq!(
        moved.metadata.get("_awa_original_queue"),
        Some(&serde_json::json!(source_queue))
    );

    let claimed = store
        .claim_runtime_batch(&pool, dest_queue, 1, Duration::from_secs(30))
        .await
        .expect("claim moved job");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.id, job_id);
    assert_eq!(claimed[0].job.queue, dest_queue);
    assert_eq!(claimed[0].job.priority, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_storage_batch_ready_noop_does_not_tombstone() {
    let (_db_guard, pool) = setup_pool(6).await;
    let queue = "qs_batch_noop";
    let schema = "awa_qs_batch_noop";
    let store = create_store(&pool, schema).await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 913 },
        InsertOpts {
            queue: queue.to_string(),
            priority: 2,
            ..Default::default()
        },
    )
    .await;

    let moved = store
        .set_priority(&pool, job_id, 2)
        .await
        .expect("set_priority no-op should not fail");
    assert!(!moved, "set_priority to the existing priority is a no-op");

    let tombstones: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {schema}.ready_tombstones WHERE job_id = $1"
    ))
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("count tombstones");
    assert_eq!(tombstones, 0, "no-op must not tombstone the ready row");

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("claim after no-op");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.id, job_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_storage_ready_tombstone_head_advances_claim_cursor() {
    let (_db_guard, pool) = setup_pool(6).await;
    let queue = "qs_tombstone_head";
    let schema = "awa_qs_tombstone_head";
    let store = create_store(&pool, schema).await;
    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 901 },
        InsertOpts {
            queue: queue.to_string(),
            priority: 2,
            ..Default::default()
        },
    )
    .await;

    tombstone_ready_job(&pool, &store, job_id).await;
    assert_eq!(ready_tombstone_count(&pool, &store).await, 1);
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("queue counts")
            .available,
        0,
        "exact counts must not report tombstoned ready rows as available"
    );

    let claimed: Vec<RawReceiptClaimRow> = sqlx::query_as(&format!(
        "SELECT ready_slot, ready_generation, job_id, priority, attempt, run_lease, lane_seq, claim_slot
         FROM {schema}.claim_ready_runtime($1, $2, $3, $4)"
    ))
    .bind(queue)
    .bind(1_i64)
    .bind(0.0_f64)
    .bind(0.0_f64)
    .fetch_all(&pool)
    .await
    .expect("raw claim over tombstoned head");

    assert!(
        claimed.is_empty(),
        "claim allocator must not emit a tombstoned ready row"
    );
    assert_eq!(
        claim_cursor_for(&pool, &store, queue, 2, 0).await,
        2,
        "a tombstoned head lane is committed spent evidence and should advance the cursor"
    );
    assert_eq!(lease_count(&pool, &store).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_storage_ready_tombstone_non_head_does_not_skip_live_prefix() {
    let (_db_guard, pool) = setup_pool(6).await;
    let queue = "qs_tombstone_non_head";
    let schema = "awa_qs_tombstone_non_head";
    let store = create_store(&pool, schema).await;
    let first_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 911 },
        InsertOpts {
            queue: queue.to_string(),
            priority: 2,
            ..Default::default()
        },
    )
    .await;
    let second_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 912 },
        InsertOpts {
            queue: queue.to_string(),
            priority: 2,
            ..Default::default()
        },
    )
    .await;

    tombstone_ready_job(&pool, &store, second_id).await;
    assert_eq!(ready_tombstone_count(&pool, &store).await, 1);
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("queue counts")
            .available,
        1,
        "only the non-tombstoned prefix row should be available"
    );

    let claimed: Vec<RawReceiptClaimRow> = sqlx::query_as(&format!(
        "SELECT ready_slot, ready_generation, job_id, priority, attempt, run_lease, lane_seq, claim_slot
         FROM {schema}.claim_ready_runtime($1, $2, $3, $4)"
    ))
    .bind(queue)
    .bind(2_i64)
    .bind(0.0_f64)
    .bind(0.0_f64)
    .fetch_all(&pool)
    .await
    .expect("raw claim with non-head tombstone");

    assert_eq!(
        claimed.iter().map(|row| row.job_id).collect::<Vec<_>>(),
        vec![first_id],
        "later tombstones must not be claimed and must not suppress an earlier live row"
    );
    assert_eq!(
        claim_cursor_for(&pool, &store, queue, 2, 0).await,
        1,
        "a later tombstone must not move the non-transactional cursor past an earlier live row"
    );
    assert_eq!(lease_count(&pool, &store).await, 1);
}

struct ReceiptRescueWorker {
    release: Arc<tokio::sync::Notify>,
    first_attempt_started: Arc<AtomicBool>,
    first_attempt_start_wake: Arc<tokio::sync::Notify>,
    first_attempt_finished: Arc<AtomicBool>,
    first_attempt_wake: Arc<tokio::sync::Notify>,
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
        self.first_attempt_started.store(true, Ordering::SeqCst);
        self.first_attempt_start_wake.notify_waiters();
        self.release.notified().await;
        self.first_attempt_finished.store(true, Ordering::SeqCst);
        self.first_attempt_wake.notify_one();
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
                if started.elapsed() > Duration::from_secs(30) {
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
/// `rotate_claims`, the #371 idle gate, and `prune_oldest_claims` on an
/// empty schema: rotate is idle-skipped when nothing has been written
/// (the cursor freezes instead of lapping empty slots), prune is a
/// noop, install + reset leaves the ring seeded correctly. The
/// end-to-end test `test_claim_ring_rotate_and_prune_under_load`
/// covers cursor advancement and the busy-path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_claim_ring_rotates_and_prunes_empty() {
    let (_db_guard, pool) = setup_pool(4).await;
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

    // Rotate four times — every attempt is idle-skipped (#371): the
    // ring received no writes, so there is nothing to seal and the
    // cursor must freeze at slot 0 instead of lapping empty slots
    // (each lap used to churn a dead claim_ring_state tuple per tick).
    for step in 1..=4_i64 {
        let outcome = store
            .rotate_claims(&pool)
            .await
            .expect("rotate_claims should succeed");
        match outcome {
            RotateOutcome::SkippedIdle { slot } => {
                assert_eq!(slot, 0, "idle ring must keep slot 0 at step {step}");
            }
            other => panic!("rotate_claims step {step} unexpected outcome: {other:?}"),
        }
    }
    let (idle_slot, idle_gen): (i32, i64) = sqlx::query_as(&format!(
        "SELECT current_slot, generation FROM {schema}.claim_ring_state WHERE singleton"
    ))
    .fetch_one(&pool)
    .await
    .expect("read claim ring state after idle rotations");
    assert_eq!(
        (idle_slot, idle_gen),
        (0, 0),
        "idle rotations must not advance the cursor or generation"
    );

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

/// The #371 idle gate must not freeze rotation while sealed slots still
/// hold rows. Prune never advances a slot's generation (only rotation
/// does, by reusing slots), so a gate scoped to the current slot alone
/// would strand later sealed slots' rows on a drained ring until new
/// traffic resumed rotation. The gate probes the ring's parent tables:
/// rotation keeps cycling until prune has reclaimed everything, and
/// only then reports `SkippedIdle`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_idle_gate_holds_rotation_open_until_sealed_slots_reclaimed() {
    let (_db_guard, pool) = setup_pool(4).await;
    let schema = "awa_qs_idle_reclaim";
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
    let queue = "idle_reclaim_q";

    // Emulate a ring that rotated twice under traffic and then fully
    // drained its ready backlog, leaving processed rows in the two
    // sealed slots. This is exactly what rotate() writes: the opened
    // slot's generation on queue_ring_slots plus the cursor on
    // queue_ring_state.
    for (slot, generation) in [(0_i32, 0_i64), (1, 1)] {
        sqlx::query(&format!(
            "INSERT INTO {schema}.done_entries (
                ready_slot, ready_generation, job_id, kind, queue, state,
                priority, attempt, run_lease, lane_seq, enqueue_shard,
                attempted_at, finalized_at, payload
            ) VALUES ($1, $2, $3, 'idle_reclaim_job', $4,
                      'completed'::awa.job_state, 2::smallint, 1::smallint,
                      1::bigint, $5::bigint, 0::smallint, now(), now(),
                      '{{}}'::jsonb)"
        ))
        .bind(slot)
        .bind(generation)
        .bind(8_000_000_i64 + i64::from(slot))
        .bind(queue)
        .bind(100_i64 + i64::from(slot))
        .execute(&pool)
        .await
        .expect("seed sealed done row");
    }
    sqlx::query(&format!(
        "UPDATE {schema}.queue_ring_slots SET generation = 1 WHERE slot = 1"
    ))
    .execute(&pool)
    .await
    .expect("seal slot 1");
    sqlx::query(&format!(
        "UPDATE {schema}.queue_ring_slots SET generation = 2 WHERE slot = 2"
    ))
    .execute(&pool)
    .await
    .expect("open slot 2");
    sqlx::query(&format!(
        "UPDATE {schema}.queue_ring_state SET current_slot = 2, generation = 2 \
         WHERE singleton = TRUE"
    ))
    .execute(&pool)
    .await
    .expect("advance cursor as two busy rotations would");

    // While sealed rows remain anywhere in the ring, rotation must NOT
    // idle-skip — the frozen-generation reclamation trap.
    let outcome = store.rotate(&pool).await.expect("rotate with sealed rows");
    assert!(
        !matches!(outcome, RotateOutcome::SkippedIdle { .. }),
        "rotation must stay live while sealed slots hold rows, got {outcome:?}"
    );

    // Prune reclaims oldest-first; rotation stays live between prunes.
    let pruned = store
        .prune_oldest(&pool, Duration::ZERO)
        .await
        .expect("prune oldest sealed slot");
    assert!(
        matches!(pruned, PruneOutcome::Pruned { slot: 0, .. }),
        "first prune should reclaim slot 0, got {pruned:?}"
    );
    let outcome = store
        .rotate(&pool)
        .await
        .expect("rotate with one sealed slot left");
    assert!(
        !matches!(outcome, RotateOutcome::SkippedIdle { .. }),
        "rotation must stay live while slot 1 still holds rows, got {outcome:?}"
    );

    let pruned = store
        .prune_oldest(&pool, Duration::ZERO)
        .await
        .expect("prune second sealed slot");
    assert!(
        matches!(pruned, PruneOutcome::Pruned { slot: 1, .. }),
        "second prune should reclaim slot 1, got {pruned:?}"
    );

    // Everything reclaimed: the ring is genuinely empty and rotation
    // may now freeze.
    let outcome = store.rotate(&pool).await.expect("rotate on empty ring");
    assert!(
        matches!(outcome, RotateOutcome::SkippedIdle { .. }),
        "fully reclaimed ring should idle-skip, got {outcome:?}"
    );
    let (frozen_slot, frozen_gen): (i32, i64) = sqlx::query_as(&format!(
        "SELECT current_slot, generation FROM {schema}.queue_ring_state WHERE singleton"
    ))
    .fetch_one(&pool)
    .await
    .expect("read frozen cursor");
    let outcome = store.rotate(&pool).await.expect("repeat idle rotate");
    assert!(matches!(outcome, RotateOutcome::SkippedIdle { .. }));
    let (still_slot, still_gen): (i32, i64) = sqlx::query_as(&format!(
        "SELECT current_slot, generation FROM {schema}.queue_ring_state WHERE singleton"
    ))
    .fetch_one(&pool)
    .await
    .expect("read cursor after repeat idle rotate");
    assert_eq!(
        (frozen_slot, frozen_gen),
        (still_slot, still_gen),
        "idle rotations must not move the cursor"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_reset_truncates_compact_receipt_evidence() {
    let (_db_guard, pool) = setup_pool(8).await;
    let queue = "qs_reset_compact_receipts";
    let schema = "awa_qs_reset_compact_receipts";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    let original_job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 44 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        original_job_id, 1,
        "fresh schema should allocate the first job id"
    );

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim compact completion candidate");
    assert_eq!(claimed.len(), 1);
    assert!(
        claimed[0].claim.lease_claim_receipt,
        "test must exercise receipt-backed compact completion"
    );
    assert!(
        claimed[0].claim.claim_batch_id.is_some(),
        "compact receipt claims should carry direct claim-batch identity"
    );
    assert_eq!(
        claimed[0].claim.claim_batch_index,
        Some(1),
        "single-item compact claim batch should return a one-based item index"
    );
    store
        .complete_runtime_batch(&pool, &claimed)
        .await
        .expect("complete compact receipt job");

    assert_eq!(
        completed_done_count(&pool, &store, queue).await,
        0,
        "compact receipt success should not write a completed done_entries row"
    );
    assert_eq!(
        receipt_completion_batch_count(&pool, &store, queue).await,
        1,
        "compact terminal history must exist before reset"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        1,
        "compact claim closure evidence must exist before reset"
    );
    assert_eq!(
        completed_terminal_count(&pool, &store, queue).await,
        1,
        "terminal view should see the compact completion before reset"
    );

    store
        .reset(&pool)
        .await
        .expect("standalone reset should succeed");

    assert_eq!(
        receipt_completion_batch_count(&pool, &store, queue).await,
        0
    );
    assert_eq!(receipt_completion_tombstone_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_closure_batch_count(&pool, &store).await, 0);

    let reset_job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 45 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        reset_job_id, 1,
        "reset restarts job ids, so stale compact evidence would collide"
    );

    let claimed_after_reset = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim reused job id after reset");
    assert_eq!(
        claimed_after_reset.len(),
        1,
        "stale compact evidence must not skip a reused job id after reset"
    );
    assert_eq!(claimed_after_reset[0].job.id, reset_job_id);
}

/// Wave-1 regression test for the claim-ring rotate+prune pair.
///
/// Exercises the full cycle: claim a job (populates
/// `lease_claims_<current>`), complete it (populates compact terminal
/// history), rotate the ring (must NOT flip onto a slot that still has
/// rows), prune the oldest slot (must `TRUNCATE` both children because
/// every claim has closure evidence), rotate
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
    let (_db_guard, pool) = setup_pool(6).await;
    let schema = "awa_qs_claim_ring_reclaim";
    let queue = "qs_claim_ring_reclaim";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    // Claim + complete a receipt-backed job. Without prune, this leaves
    // compact claim evidence in lease_claim_batches_0 and one compact receipt
    // completion batch.
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
            lease_claim_receipts: true,
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

    // Sanity: the claim landed in slot 0 and compact claim-local batch
    // evidence closes it without an explicit per-job closure row.
    let slot0_claims = receipt_claim_count_in_child(&pool, schema, 0, job_id).await;
    let slot0_closures: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claim_closures_0"
    ))
    .fetch_one(&pool)
    .await
    .expect("count lease_claim_closures_0");
    assert_eq!(
        slot0_claims, 1,
        "completed claim evidence must live in slot 0"
    );
    assert_eq!(
        slot0_closures, 0,
        "compact successful completion must not write explicit closure rows"
    );
    assert_eq!(
        receipt_completion_batch_count(&pool, &store, queue).await,
        1,
        "compact receipt completion batch is terminal history"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        1,
        "compact claim-closure batch is the closure evidence"
    );

    // Rotate from slot 0 → slot 1. Slot 1 is empty, so busy-check
    // passes, and slot 0 holds the completed claim evidence, so the
    // idle gate (#371) sees a non-empty current slot and the cursor
    // advances.
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

    // Walk the cursor around to slot 3 "under load": claim + complete
    // one job in each intermediate slot so every rotation seals real
    // claim evidence. (An empty current slot would now be SkippedIdle
    // by the #371 idle gate rather than rotated — the pure-timer walk
    // this test previously relied on no longer advances an idle ring.)
    for (job_seq, expected_slot) in [(2_i64, 2_i32), (3, 3)] {
        let walk_job = enqueue_job(
            &pool,
            &store,
            &CompleteJob { id: job_seq },
            InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        )
        .await;
        let walk_claimed = store
            .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
            .await
            .expect("claim walk job");
        assert_eq!(walk_claimed.len(), 1, "walk job should be claimed");
        assert_eq!(walk_claimed[0].job.id, walk_job);
        store
            .complete_runtime_batch(&pool, &walk_claimed)
            .await
            .expect("complete walk job");

        match store
            .rotate_claims(&pool)
            .await
            .expect("rotate_claims walk step")
        {
            RotateOutcome::Rotated { slot, .. } => assert_eq!(slot, expected_slot),
            other => panic!("expected Rotated to slot {expected_slot}, got {other:?}"),
        }
    }

    // Seal live evidence into slot 3 as well, so the final
    // rotate-after-prune below exercises the busy/idle gates the same
    // way a loaded ring would.
    let tail_job = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 4 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    let tail_claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim tail job");
    assert_eq!(tail_claimed.len(), 1, "tail job should be claimed");
    assert_eq!(tail_claimed[0].job.id, tail_job);
    store
        .complete_runtime_batch(&pool, &tail_claimed)
        .await
        .expect("complete tail job");

    // The next target wraps to slot 0, which still holds the completed
    // claim. The busy-check must refuse.
    let busy_outcome = store
        .rotate_claims(&pool)
        .await
        .expect("rotate_claims attempt -> slot 0 (busy)");
    assert!(
        matches!(busy_outcome, RotateOutcome::SkippedBusy { slot: 0, .. }),
        "rotate onto slot 0 with live rows must SkippedBusy, got {busy_outcome:?}"
    );

    // Prune the oldest initialized slot. The completed claim has a
    // compact receipt batch, so PartitionTruncateSafety holds even
    // without a completed-closure row, and prune TRUNCATEs both children.
    sqlx::query(&format!(
        r#"
        UPDATE {schema}.claim_ring_slots
        SET rescue_cursor_claimed_at = clock_timestamp(),
            rescue_cursor_job_id = $1,
            rescue_cursor_run_lease = 1
        WHERE slot = 0
        "#
    ))
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("seed non-default rescue cursor before claim prune");

    let prune_outcome = store
        .prune_oldest_claims(&pool)
        .await
        .expect("prune_oldest_claims");
    match prune_outcome {
        PruneOutcome::Pruned { slot, .. } => assert_eq!(slot, 0),
        other => panic!("expected Pruned {{ slot: 0 }}, got {other:?}"),
    }

    // Claim-ring children for slot 0 are now empty.
    let post_prune_claims: i64 =
        sqlx::query_scalar(&format!("SELECT count(*) FROM {schema}.lease_claims_0"))
            .fetch_one(&pool)
            .await
            .expect("count lease_claims_0 after prune");
    let post_prune_claim_batches: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claim_batches_0"
    ))
    .fetch_one(&pool)
    .await
    .expect("count lease_claim_batches_0 after prune");
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
        post_prune_claim_batches, 0,
        "lease_claim_batches_0 must be empty post-prune"
    );
    assert_eq!(
        post_prune_closures, 0,
        "lease_claim_closures_0 must be empty post-prune"
    );
    let cursor_reset: (bool, i64, i64) = sqlx::query_as(&format!(
        r#"
        SELECT
            rescue_cursor_claimed_at = '-infinity'::timestamptz,
            rescue_cursor_job_id,
            rescue_cursor_run_lease
        FROM {schema}.claim_ring_slots
        WHERE slot = 0
        "#
    ))
    .fetch_one(&pool)
    .await
    .expect("read claim rescue cursor after claim prune");
    assert_eq!(
        cursor_reset,
        (true, 0, 0),
        "claim prune should reset only the matching claim-slot rescue cursor"
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_prune_oldest_leases_does_not_reset_claim_rescue_cursor() {
    let (_db_guard, pool) = setup_pool(4).await;
    let schema = "awa_qs_lease_prune_keeps_claim_cursor";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    // The lease ring is empty so `rotate_leases` idle-skips (#371);
    // advance the cursor directly (rotation of a busy ring updates only
    // lease_ring_state) so slot 0 becomes the prune target.
    match store
        .rotate_leases(&pool)
        .await
        .expect("rotate_leases on empty ring")
    {
        RotateOutcome::SkippedIdle { slot } => assert_eq!(slot, 0),
        other => panic!("expected SkippedIdle {{ slot: 0 }}, got {other:?}"),
    }
    sqlx::query(&format!(
        r#"
        UPDATE {schema}.lease_ring_state
        SET current_slot = 1, generation = 1
        WHERE singleton = TRUE
        "#
    ))
    .execute(&pool)
    .await
    .expect("advance lease ring cursor for prune setup");

    sqlx::query(&format!(
        r#"
        UPDATE {schema}.claim_ring_slots
        SET rescue_cursor_claimed_at = clock_timestamp(),
            rescue_cursor_job_id = 42,
            rescue_cursor_run_lease = 7
        WHERE slot = 0
        "#
    ))
    .execute(&pool)
    .await
    .expect("seed claim rescue cursor before lease prune");

    let prune = store
        .prune_oldest_leases(&pool)
        .await
        .expect("prune_oldest_leases");
    assert!(
        matches!(prune, PruneOutcome::Pruned { slot: 0, .. }),
        "lease prune should truncate empty lease slot 0, got {prune:?}"
    );

    let cursor: (bool, i64, i64) = sqlx::query_as(&format!(
        r#"
        SELECT
            rescue_cursor_claimed_at = '-infinity'::timestamptz,
            rescue_cursor_job_id,
            rescue_cursor_run_lease
        FROM {schema}.claim_ring_slots
        WHERE slot = 0
        "#
    ))
    .fetch_one(&pool)
    .await
    .expect("read claim rescue cursor after lease prune");
    assert_eq!(
        cursor,
        (false, 42, 7),
        "lease pruning must not mutate claim rescue cursors; lease and claim slots are independent rings"
    );
}

/// Wave-1 regression test for the prune safety predicate. If a claim
/// is still open (no matching closure), prune must return
/// `SkippedActive` before taking child-table `ACCESS EXCLUSIVE` locks
/// instead of blocking behind readers or truncating the partition and
/// losing the claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_prune_oldest_claims_refuses_to_truncate_open_claim() {
    let (_db_guard, pool) = setup_pool(4).await;
    let schema = "awa_qs_claim_ring_open";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            lease_claim_receipts: true,
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

    let mut reader_tx = pool.begin().await.expect("begin claim reader tx");
    sqlx::query(&format!(
        "LOCK TABLE {schema}.lease_claims_0, {schema}.lease_claim_closures_0, {schema}.lease_claim_closure_batches_0 IN ACCESS SHARE MODE"
    ))
    .execute(reader_tx.as_mut())
    .await
    .expect("lock claim children in access share mode");

    let outcome = store
        .prune_oldest_claims(&pool)
        .await
        .expect("prune_oldest_claims with open claim");
    assert!(
        matches!(
            outcome,
            PruneOutcome::SkippedActive {
                slot: 0,
                reason: SkipReason::ClaimOpen,
                count: 1
            }
        ),
        "prune must prove the open claim before trying exclusive child locks, got {outcome:?}"
    );

    reader_tx.rollback().await.expect("release reader lock");

    // The claim is still there — not lost.
    let survived: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claims_0 WHERE job_id = 999"
    ))
    .fetch_one(&pool)
    .await
    .expect("count survivor");
    assert_eq!(survived, 1, "open claim must survive SkippedActive prune");
}

/// Regression for the post-lock claim-prune proof. The claim row is
/// inserted but left uncommitted while prune performs its optimistic
/// pre-lock proof, so the proof sees an empty slot. The writer transaction
/// keeps a relation lock that makes prune wait for `ACCESS EXCLUSIVE`; once
/// the writer commits, the post-lock proof must see the now-visible open
/// claim and skip instead of truncating it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_prune_oldest_claims_rechecks_open_claim_after_lock_wait() {
    let (_db_guard, pool) = setup_pool(8).await;
    let schema = "awa_qs_claim_ring_post_lock";
    let store = Arc::new(
        create_store_with_config(
            &pool,
            QueueStorageConfig {
                schema: schema.to_string(),
                queue_slot_count: 4,
                lease_slot_count: 2,
                claim_slot_count: 4,
                lease_claim_receipts: true,
                ..Default::default()
            },
        )
        .await
        .with_prune_lock_timeout(Duration::from_secs(2))
        .expect("test prune lock timeout"),
    );

    // The ring is empty at this point, so `rotate_claims` idle-skips
    // (#371); move the cursor off slot 0 directly so prune targets it.
    sqlx::query(&format!(
        "UPDATE {schema}.claim_ring_state SET current_slot = 1, generation = 1 WHERE singleton = TRUE"
    ))
    .execute(&pool)
    .await
    .expect("advance claim ring cursor away from slot 0");

    let mut writer_tx = pool.begin().await.expect("begin claim writer tx");
    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.lease_claims (
            claim_slot, job_id, run_lease, ready_slot, ready_generation,
            queue, priority, attempt, max_attempts, lane_seq
        ) VALUES (0, 1001, 1, 0, 0, 'synthetic', 2, 1, 25, 1001)
        "#
    ))
    .execute(writer_tx.as_mut())
    .await
    .expect("seed uncommitted open claim");

    let prune_store = Arc::clone(&store);
    let prune_pool = pool.clone();
    let prune_task =
        tokio::spawn(async move { prune_store.prune_oldest_claims(&prune_pool).await });

    let needle = format!("{schema}.lease_claims_0");
    let wait_deadline = Instant::now() + Duration::from_secs(1);
    let mut saw_lock_wait = false;
    while Instant::now() < wait_deadline {
        let waiting: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND wait_event_type = 'Lock'
                  AND position($1 in query) > 0
            )
            "#,
        )
        .bind(&needle)
        .fetch_one(&pool)
        .await
        .expect("poll prune lock wait");
        if waiting {
            saw_lock_wait = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        saw_lock_wait,
        "prune should wait on the claim child before the writer commits"
    );

    writer_tx.commit().await.expect("commit open claim");

    let outcome = tokio::time::timeout(Duration::from_secs(3), prune_task)
        .await
        .expect("prune task timed out")
        .expect("prune task panicked")
        .expect("prune_oldest_claims");
    assert!(
        matches!(
            outcome,
            PruneOutcome::SkippedActive {
                slot: 0,
                reason: SkipReason::ClaimOpen,
                count: 1
            }
        ),
        "post-lock proof must see the committed open claim, got {outcome:?}"
    );

    let survived: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claims_0 WHERE job_id = 1001"
    ))
    .fetch_one(&pool)
    .await
    .expect("count post-lock survivor");
    assert_eq!(
        survived, 1,
        "claim committed during the lock wait must survive SkippedActive prune"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_prune_oldest_rechecks_pending_ready_after_lock_wait() {
    let (_db_guard, pool) = setup_pool(8).await;
    let queue = "qs_ready_ring_post_lock";
    let schema = "awa_qs_ready_ring_post_lock";
    let store = Arc::new(
        create_store(&pool, schema)
            .await
            .with_prune_lock_timeout(Duration::from_secs(2))
            .expect("test prune lock timeout"),
    );

    let seed_job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 91 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    sqlx::query(&format!(
        "DELETE FROM {schema}.ready_segments WHERE ready_slot = 0 AND queue = $1"
    ))
    .bind(queue)
    .execute(&pool)
    .await
    .expect("remove seed ready segment");
    sqlx::query(&format!(
        "DELETE FROM {schema}.ready_entries WHERE job_id = $1"
    ))
    .bind(seed_job_id)
    .execute(&pool)
    .await
    .expect("remove seed ready row");

    // Slot 0 is now empty, so `rotate` idle-skips (#371) rather than
    // advancing. This test needs the cursor off slot 0 so slot 0 becomes
    // the prune target; move it directly, exactly as a rotation of a busy
    // ring would (rotate updates only queue_ring_state / queue_ring_slots).
    let idle = store
        .rotate(&pool)
        .await
        .expect("rotate away from ready slot 0");
    assert!(
        matches!(idle, RotateOutcome::SkippedIdle { slot: 0 }),
        "empty ready slot 0 should idle-skip: {idle:?}"
    );
    sqlx::query(&format!(
        "UPDATE {schema}.queue_ring_state SET current_slot = 1, generation = 1 WHERE singleton = TRUE"
    ))
    .execute(&pool)
    .await
    .expect("advance queue ring cursor off slot 0 for prune setup");

    let post_lock_job_id = 1_000_091_i64;
    let mut writer_tx = pool.begin().await.expect("begin ready writer tx");
    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.ready_entries (
            ready_slot, ready_generation, job_id, kind, queue, args, priority,
            attempt, run_lease, max_attempts, lane_seq, enqueue_shard, run_at,
            attempted_at, created_at, unique_key, unique_states, payload
        ) VALUES (
            0, 0, $1, 'complete_job', $2, '{{}}'::jsonb, 2,
            0, 0, 25, 1, 0, clock_timestamp(),
            NULL, clock_timestamp(), NULL, NULL, '{{}}'::jsonb
        )
        "#
    ))
    .bind(post_lock_job_id)
    .bind(queue)
    .execute(writer_tx.as_mut())
    .await
    .expect("seed uncommitted ready row");
    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.ready_segments (
            ready_slot, ready_generation, queue, priority, enqueue_shard,
            first_lane_seq, next_lane_seq, first_run_at
        ) VALUES (0, 0, $1, 2, 0, 1, 2, clock_timestamp())
        ON CONFLICT (ready_slot, ready_generation, queue, priority, enqueue_shard, first_lane_seq)
        DO NOTHING
        "#
    ))
    .bind(queue)
    .execute(writer_tx.as_mut())
    .await
    .expect("seed uncommitted ready segment");

    let prune_store = Arc::clone(&store);
    let prune_pool = pool.clone();
    let prune_task =
        tokio::spawn(async move { prune_store.prune_oldest(&prune_pool, Duration::ZERO).await });

    let needle = format!("{schema}.ready_entries_0");
    let wait_deadline = Instant::now() + Duration::from_secs(1);
    let mut saw_lock_wait = false;
    while Instant::now() < wait_deadline {
        let waiting: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND wait_event_type = 'Lock'
                  AND position($1 in query) > 0
            )
            "#,
        )
        .bind(&needle)
        .fetch_one(&pool)
        .await
        .expect("poll ready prune lock wait");
        if waiting {
            saw_lock_wait = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        saw_lock_wait,
        "queue prune should wait on the ready child before the writer commits"
    );

    writer_tx.commit().await.expect("commit pending ready row");

    let outcome = tokio::time::timeout(Duration::from_secs(3), prune_task)
        .await
        .expect("queue prune task timed out")
        .expect("queue prune task panicked")
        .expect("prune_oldest");
    assert!(
        matches!(
            outcome,
            PruneOutcome::SkippedActive {
                slot: 0,
                reason: SkipReason::QueuePendingReady,
                count: 1
            }
        ),
        "post-lock proof must see the committed pending ready row, got {outcome:?}"
    );

    let survived: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {schema}.ready_entries_0 WHERE job_id = $1"
    ))
    .bind(post_lock_job_id)
    .fetch_one(&pool)
    .await
    .expect("count post-lock ready survivor");
    assert_eq!(
        survived, 1,
        "ready row committed during the lock wait must survive SkippedActive prune"
    );
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
    let (_db_guard, pool) = setup_pool(10).await;
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
            lease_claim_receipts: false,
        },
        CancelObservingWorker {
            running: running.clone(),
            observed_cancel: observed_cancel.clone(),
        },
    );
    // Construct the `Notified` future BEFORE starting the client, so
    // a `notify_waiters()` call from the worker can't fire-and-forget
    // before we register interest. `Notify::notified()` only catches
    // notifications received after the future is constructed; if the
    // dispatcher is fast enough to claim and start the handler before
    // we await below, the notification is otherwise lost and the
    // timeout fires.
    let running_notified = running.notified();
    tokio::pin!(running_notified);
    client.start().await.expect("client start");

    // Wait for the handler to actually start executing.
    tokio::time::timeout(Duration::from_secs(5), running_notified)
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

/// `prepare_schema` drops `open_receipt_claims` on every install,
/// refusing to drop a non-empty table (see ADR-023). This test
/// asserts the table is absent on a fresh schema and stays absent
/// across a full claim + complete cycle, while `lease_claims` and
/// `lease_claim_closures` reflect the lifecycle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_open_receipt_claims_is_absent_after_install() {
    let (_db_guard, pool) = setup_pool(10).await;
    let schema = "awa_qs_open_receipt_claims_absent";
    let queue = "qs_open_receipt_claims_absent";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            lease_claim_receipts: true,
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
            lease_claim_receipts: true,
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
        0,
        "zero-deadline receipts should not write per-job lease_claim rows"
    );
    assert_eq!(
        lease_claim_batch_count(&pool, &store).await,
        1,
        "the single receipt should live in a compact claim batch"
    );
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "compact successful completion must not write per-job closure rows"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        1,
        "compact successful completion uses claim-local batch closure evidence"
    );
    assert_eq!(
        receipt_completion_batch_count(&pool, &store, queue).await,
        1,
        "successful receipt completion should write compact terminal history"
    );
    assert!(
        !open_receipt_claims_present(&pool, schema).await,
        "open_receipt_claims must remain absent across the full lifecycle"
    );

    client.shutdown(Duration::from_secs(5)).await;
}

/// Regression guard for the #246 compact-claim visibility contract: a
/// receipt claim — zero-deadline OR deadline-backed — lands only in
/// `lease_claim_batches`, so any "running / in-flight" enumeration that
/// reads `lease_claims` alone sees zero jobs while several are actually
/// claimed. This bit two test-side state readers: the OTLP dashboard
/// helper (telemetry_test.rs) reported 0 running for a saturated queue,
/// and the Python chaos helper (`_fetch_job_row` in test_chaos_recovery.py)
/// returned None for a deadline-backed job that was running-with-heartbeat.
/// Here we claim without materializing into `leases` and assert the two
/// surfaces those helpers stand in for: the batch-expanding
/// `open_receipt_claim_count`, and the production `QueueStorage::load_job`
/// admin read (which the Python chaos helper mirrors and which
/// `admin::get_job` delegates to) reporting each member as `running`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_compact_batch_claims_are_visible_as_open() {
    let (_db_guard, pool) = setup_pool(6).await;
    let schema = "awa_qs_compact_batch_visible";
    let queue = "qs_compact_batch_visible";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    let claim_count: i64 = 3;
    let mut job_ids = Vec::new();
    for id in 0..claim_count {
        job_ids.push(
            enqueue_job(
                &pool,
                &store,
                &CompleteJob { id },
                InsertOpts {
                    queue: queue.to_string(),
                    ..Default::default()
                },
            )
            .await,
        );
    }

    // Claim directly via the runtime function with a positive deadline so
    // the jobs sit in `lease_claim_batches` mid-flight carrying a shared
    // `deadline_at` (the deadline-backed shape the chaos tests exercise) —
    // no worker runs, so nothing materializes into `leases` or closes the
    // claim.
    let claimed: Vec<RawReceiptClaimRow> = sqlx::query_as(&format!(
        "SELECT ready_slot, ready_generation, job_id, priority, attempt, run_lease, lane_seq, claim_slot
         FROM {schema}.claim_ready_runtime($1, $2, $3, $4)"
    ))
    .bind(queue)
    .bind(claim_count)
    // p_deadline_secs > 0 → the batch carries a non-NULL deadline_at.
    .bind(3600.0_f64)
    // p_aging_secs
    .bind(0.0_f64)
    .fetch_all(&pool)
    .await
    .expect("claim ready jobs into a compact batch");
    assert_eq!(
        claimed.len(),
        claim_count as usize,
        "the runtime claim should hand back every enqueued attempt"
    );

    assert_eq!(
        lease_claim_count(&pool, &store).await,
        0,
        "receipt claims must not write per-job lease_claims rows"
    );
    assert!(
        lease_claim_batch_count(&pool, &store).await >= 1,
        "the claim must land as one or more compact lease_claim_batches rows"
    );
    assert_eq!(
        lease_count(&pool, &store).await,
        0,
        "an un-materialized claim must not have a leases row"
    );

    // Surface 1 — the count/enumeration helper. The bug: a
    // `lease_claims`-only running enumeration returns 0 here.
    assert_eq!(
        open_receipt_claim_count(&pool, &store).await,
        claim_count,
        "batch-backed claims must be visible as open/running in-flight work"
    );

    // Surface 2 — the production single-job admin read (`load_job`, behind
    // `admin::get_job`). Every claimed member must report as `running`, and
    // its shared batch deadline must surface. This is the exact contract the
    // Python chaos helper's `_fetch_job_row` stands in for.
    for job_id in &job_ids {
        let job = store
            .load_job(&pool, *job_id)
            .await
            .expect("load_job for a batch-claimed job")
            .unwrap_or_else(|| panic!("load_job returned None for running job {job_id}"));
        assert_eq!(
            job.state.to_string(),
            "running",
            "load_job must report compact-batch claim {job_id} as running"
        );
        assert!(
            job.deadline_at.is_some(),
            "load_job must surface the batch's shared deadline for {job_id}"
        );
    }
}

/// Partition-routing smoke test for the ADR-023 receipt plane: a
/// receipt-backed claim + completion cycle lands the claim in the
/// expected claim child and records the claim slot on compact terminal
/// history.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lease_claim_partition_routing() {
    let (_db_guard, pool) = setup_pool(4).await;
    let schema = "awa_qs_claim_partition_routing";
    let queue = "qs_claim_partition_routing";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    // Move the claim-ring cursor forward so the current slot is not
    // zero — this proves the claim CTE actually reads claim_ring_state
    // rather than defaulting. The ring is empty, so `rotate_claims`
    // idle-skips (#371); advance the cursor directly, exactly as a
    // rotation of a busy ring would (rotate updates only
    // claim_ring_state).
    sqlx::query(&format!(
        "UPDATE {schema}.claim_ring_state SET current_slot = 2, generation = 2 WHERE singleton"
    ))
    .execute(&pool)
    .await
    .expect("advance claim ring cursor for routing setup");
    let current_slot: i32 = sqlx::query_scalar(&format!(
        "SELECT current_slot FROM {schema}.claim_ring_state WHERE singleton"
    ))
    .fetch_one(&pool)
    .await
    .expect("read current claim slot");
    assert_eq!(current_slot, 2, "ring cursor should be at slot 2");

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 777 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim partition-routed job");
    assert_eq!(claimed.len(), 1, "job should be claimed");
    assert_eq!(claimed[0].job.id, job_id, "claimed job id");
    assert!(
        claimed[0].claim.lease_claim_receipt,
        "test setup must exercise the receipt claim path"
    );
    assert_eq!(
        claimed[0].claim.claim_slot, current_slot,
        "claim should use the rotated current slot"
    );
    store
        .complete_runtime_batch(&pool, &claimed)
        .await
        .expect("complete partition-routed claim");

    let completed = store
        .load_job(&pool, job_id)
        .await
        .expect("load completed job")
        .expect("completed job row");
    assert_eq!(completed.state, JobState::Completed);

    // Assert the claim evidence lives in the current claim slot and matching
    // physical child partition.
    let claim_slot = receipt_claim_slot_for_job(&pool, &store, job_id).await;
    assert_eq!(
        claim_slot, current_slot,
        "claim evidence should land in current slot"
    );

    // Physically: the claim evidence must be addressable via its child partition.
    let claim_in_child = receipt_claim_count_in_child(&pool, schema, current_slot, job_id).await;
    assert!(
        claim_in_child >= 1,
        "claim evidence must be in claim slot child"
    );

    let closure_in_child: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claim_closures_2 WHERE job_id = $1"
    ))
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("count in lease_claim_closures_2");
    assert_eq!(
        closure_in_child, 0,
        "compact successful completion should not write an explicit closure row"
    );

    let compact_batch_claim_slot: i32 = sqlx::query_scalar(&format!(
        "SELECT claim_slot
         FROM {schema}.receipt_completion_batches
         WHERE job_ids @> ARRAY[$1]::bigint[]
         ORDER BY batch_id DESC
         LIMIT 1"
    ))
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("read compact batch claim_slot");
    assert_eq!(
        compact_batch_claim_slot, current_slot,
        "compact receipt batch must retain the originating claim slot"
    );

    let receipt_id = claimed[0]
        .claim
        .receipt_id
        .expect("receipt claim should carry receipt_id");
    let compact_batch_closes_receipt: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS (
             SELECT 1
             FROM {schema}.lease_claim_closure_batches AS batches
             WHERE batches.claim_slot = $1
               AND batches.receipt_ranges @> $2
         )"
    ))
    .bind(current_slot)
    .bind(receipt_id)
    .fetch_one(&pool)
    .await
    .expect("read compact closure batch receipt range");
    assert!(
        compact_batch_closes_receipt,
        "compact closure batch must close the exact receipt through receipt_ranges"
    );
}

/// Rotation-isolation check for the ADR-023 claim ring. A claim landed
/// in slot A before rotation stays in slot A. After rotation, a fresh
/// claim lands in slot B. Neither disturbs the other — partitioning
/// and ring state are consistent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lease_claim_rotation_isolation() {
    let (_db_guard, pool) = setup_pool(4).await;
    let schema = "awa_qs_claim_rotation_isolation";
    let queue = "qs_claim_rotation_isolation";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            lease_claim_receipts: true,
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

    let claimed_a = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim job A");
    assert_eq!(claimed_a.len(), 1, "job A should be claimed");
    assert_eq!(claimed_a[0].job.id, job_a, "claimed job A id");
    let slot_a = claimed_a[0].claim.claim_slot;
    store
        .complete_runtime_batch(&pool, &claimed_a)
        .await
        .expect("complete job A claim");

    // Rotate the ring so subsequent claims land in a different partition.
    let rotated_slot = match store
        .rotate_claims(&pool)
        .await
        .expect("rotate_claims between jobs")
    {
        RotateOutcome::Rotated { slot, .. } => slot,
        other => panic!("rotate_claims between jobs unexpected outcome: {other:?}"),
    };
    assert_ne!(
        slot_a, rotated_slot,
        "rotation should advance to a different claim slot"
    );

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
    let claimed_b = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim job B");
    assert_eq!(claimed_b.len(), 1, "job B should be claimed");
    assert_eq!(claimed_b[0].job.id, job_b, "claimed job B id");
    let slot_b = claimed_b[0].claim.claim_slot;
    store
        .complete_runtime_batch(&pool, &claimed_b)
        .await
        .expect("complete job B claim");
    assert_eq!(
        rotated_slot, slot_b,
        "job B (post-rotation) must land in the newly-opened claim_slot"
    );
    assert_ne!(
        slot_a, slot_b,
        "job B (post-rotation) must land in a different claim_slot than job A"
    );

    // Job A is still exactly where it was written — rotation didn't
    // mutate existing rows.
    let job_a_slot_still = receipt_claim_slot_for_job(&pool, &store, job_a).await;
    assert_eq!(
        slot_a, job_a_slot_still,
        "rotation must not move existing claim rows across partitions"
    );
}

/// Read `(current_slot, generation)` for one of the three ring-state
/// singletons (`ring` is "queue" / "lease" / "claim").
async fn ring_cursor(pool: &sqlx::PgPool, schema: &str, ring: &str) -> (i32, i64) {
    sqlx::query_as::<_, (i32, i64)>(&format!(
        "SELECT current_slot, generation FROM {schema}.{ring}_ring_state WHERE singleton = TRUE"
    ))
    .fetch_one(pool)
    .await
    .expect("read ring state")
}

/// Physical row identity of a ring-state singleton. If any UPDATE runs
/// against the row, `ctid` and/or `xmin` change — this is the
/// deterministic form of "no dead tuple was created", independent of
/// pg_stat flush timing.
async fn ring_row_identity(pool: &sqlx::PgPool, schema: &str, table: &str) -> (String, String) {
    sqlx::query_as::<_, (String, String)>(&format!(
        "SELECT ctid::text, xmin::text FROM {schema}.{table}"
    ))
    .fetch_one(pool)
    .await
    .expect("read ring-state row identity")
}

/// `pg_stat_user_tables.n_dead_tup` for the ring bookkeeping tables.
async fn ring_state_dead_tuples(pool: &sqlx::PgPool, schema: &str) -> Vec<(String, i64)> {
    sqlx::query_as::<_, (String, i64)>(
        r#"
        SELECT relname, n_dead_tup
        FROM pg_stat_user_tables
        WHERE schemaname = $1
          AND relname IN (
              'queue_ring_state',
              'queue_ring_slots',
              'lease_ring_state',
              'claim_ring_state'
          )
        ORDER BY relname
        "#,
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .expect("sample ring-state dead tuples")
}

/// #371 idle gate: rotating a ring that received no writes must be a
/// pure no-op — `SkippedIdle`, cursor and generation unchanged, and
/// crucially *zero* ring-state row versions created. Before this gate
/// every timer tick UPDATEd the ring-state singleton (plus a
/// `queue_ring_slots` row on the queue ring), and under a pinned MVCC
/// horizon those dead tuples were unreclaimable (~14.4k/hour on
/// `lease_ring_state` at the historical 250ms cadence).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_idle_ring_rotation_skips_without_ring_state_churn() {
    let (_db_guard, pool) = setup_pool(4).await;
    let schema = "awa_qs_idle_rotation_skip";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    // Reset dead-tuple stats from schema setup so the baseline below is
    // clean. VACUUM reports its result to pg_stat synchronously.
    for table in [
        "queue_ring_state",
        "queue_ring_slots",
        "lease_ring_state",
        "claim_ring_state",
    ] {
        sqlx::query(&format!("VACUUM {schema}.{table}"))
            .execute(&pool)
            .await
            .expect("vacuum ring-state table");
    }
    let baseline_dead = ring_state_dead_tuples(&pool, schema).await;

    let queue_before = ring_cursor(&pool, schema, "queue").await;
    let lease_before = ring_cursor(&pool, schema, "lease").await;
    let claim_before = ring_cursor(&pool, schema, "claim").await;
    let queue_row_before = ring_row_identity(&pool, schema, "queue_ring_state").await;
    let lease_row_before = ring_row_identity(&pool, schema, "lease_ring_state").await;
    let claim_row_before = ring_row_identity(&pool, schema, "claim_ring_state").await;

    for round in 0..25 {
        match store.rotate(&pool).await.expect("idle queue rotate") {
            RotateOutcome::SkippedIdle { slot } => assert_eq!(
                slot, queue_before.0,
                "idle queue rotate must keep the current slot (round {round})"
            ),
            other => panic!("idle queue rotate round {round}: expected SkippedIdle, got {other:?}"),
        }
        match store.rotate_leases(&pool).await.expect("idle lease rotate") {
            RotateOutcome::SkippedIdle { slot } => assert_eq!(
                slot, lease_before.0,
                "idle lease rotate must keep the current slot (round {round})"
            ),
            other => panic!("idle lease rotate round {round}: expected SkippedIdle, got {other:?}"),
        }
        match store.rotate_claims(&pool).await.expect("idle claim rotate") {
            RotateOutcome::SkippedIdle { slot } => assert_eq!(
                slot, claim_before.0,
                "idle claim rotate must keep the current slot (round {round})"
            ),
            other => panic!("idle claim rotate round {round}: expected SkippedIdle, got {other:?}"),
        }
    }

    assert_eq!(
        ring_cursor(&pool, schema, "queue").await,
        queue_before,
        "idle rotation must not advance the queue ring cursor or generation"
    );
    assert_eq!(
        ring_cursor(&pool, schema, "lease").await,
        lease_before,
        "idle rotation must not advance the lease ring cursor or generation"
    );
    assert_eq!(
        ring_cursor(&pool, schema, "claim").await,
        claim_before,
        "idle rotation must not advance the claim ring cursor or generation"
    );

    // No UPDATE ran: the singleton rows are physically the same tuple.
    assert_eq!(
        ring_row_identity(&pool, schema, "queue_ring_state").await,
        queue_row_before,
        "idle rotation must not create a new queue_ring_state row version"
    );
    assert_eq!(
        ring_row_identity(&pool, schema, "lease_ring_state").await,
        lease_row_before,
        "idle rotation must not create a new lease_ring_state row version"
    );
    assert_eq!(
        ring_row_identity(&pool, schema, "claim_ring_state").await,
        claim_row_before,
        "idle rotation must not create a new claim_ring_state row version"
    );

    // pg_stat corroboration: give the (asynchronous) stats machinery a
    // moment, then require the dead-tuple counters to stay flat. 75
    // idle rotations would have produced >= 25 dead tuples on each
    // ring-state table before the idle gate; the small slack absorbs a
    // late stats flush from schema setup, not rotation churn.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let _ = sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(&pool)
        .await;
    let final_dead = ring_state_dead_tuples(&pool, schema).await;
    for ((relname, baseline), (relname_after, after)) in baseline_dead.iter().zip(final_dead.iter())
    {
        assert_eq!(relname, relname_after, "dead-tuple samples must align");
        assert!(
            *after <= baseline + 5,
            "{relname}: n_dead_tup grew from {baseline} to {after} across 25 idle rotate \
             ticks — idle rotation is churning ring bookkeeping rows again (#371)"
        );
    }
}

/// The #371 idle gate must never fire while traffic flows: a ring whose
/// current slot holds live rows rotates exactly as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_active_ring_rotation_proceeds_despite_idle_gate() {
    let (_db_guard, pool) = setup_pool(4).await;
    let schema = "awa_qs_active_rotation_proceeds";
    let queue = "qs_active_rotation_proceeds";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    // Queue ring: an enqueued (not yet claimed) job makes the current
    // slot non-empty, so rotation seals it and advances.
    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 371 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    match store.rotate(&pool).await.expect("rotate active queue ring") {
        RotateOutcome::Rotated { slot, generation } => {
            assert_eq!(slot, 1);
            assert_eq!(generation, 1);
        }
        other => panic!("active queue ring must rotate, got {other:?}"),
    }

    // Claim the job (compact receipt path) and materialize a lease via
    // a registered callback, so both the claim ring and the lease ring
    // hold live rows in their current slots.
    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim job");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.id, job_id);
    let _callback_id = store
        .register_callback(
            &pool,
            claimed[0].job.id,
            claimed[0].job.run_lease,
            Duration::from_secs(30),
        )
        .await
        .expect("register callback to materialize a lease");
    assert_eq!(lease_count(&pool, &store).await, 1);

    // Lease ring: the materialized lease keeps the current slot busy.
    match store
        .rotate_leases(&pool)
        .await
        .expect("rotate active lease ring")
    {
        RotateOutcome::Rotated { slot, generation } => {
            assert_eq!(slot, 1);
            assert_eq!(generation, 1);
        }
        other => panic!("active lease ring must rotate, got {other:?}"),
    }

    // Claim ring: the compact claim wrote receipt evidence into the
    // current claim slot.
    match store
        .rotate_claims(&pool)
        .await
        .expect("rotate active claim ring")
    {
        RotateOutcome::Rotated { slot, generation } => {
            assert_eq!(slot, 1);
            assert_eq!(generation, 1);
        }
        other => panic!("active claim ring must rotate, got {other:?}"),
    }
}

/// Drain-then-idle (#371): a ring that carried traffic keeps rotating
/// until its slots are sealed and pruned; once fully drained, further
/// rotation skips and the generation stops advancing. This is the
/// expected steady state of a quiet queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_drained_ring_rotation_skips_after_prune() {
    let (_db_guard, pool) = setup_pool(4).await;
    let schema = "awa_qs_drained_rotation_skips";
    let queue = "qs_drained_rotation_skips";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 4,
            lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    // Traffic: enqueue, claim (compact receipt path), complete.
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
    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim job");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.id, job_id);
    store
        .complete_runtime_batch(&pool, &claimed)
        .await
        .expect("complete job");

    // While slot 0 holds the traffic's evidence, rotation proceeds.
    match store.rotate(&pool).await.expect("seal queue slot 0") {
        RotateOutcome::Rotated { slot, .. } => assert_eq!(slot, 1),
        other => panic!("queue ring with evidence must rotate, got {other:?}"),
    }
    match store.rotate_claims(&pool).await.expect("seal claim slot 0") {
        RotateOutcome::Rotated { slot, .. } => assert_eq!(slot, 1),
        other => panic!("claim ring with evidence must rotate, got {other:?}"),
    }

    // Drain: prune the sealed slots.
    match store
        .prune_oldest(&pool, Duration::ZERO)
        .await
        .expect("prune queue slot 0")
    {
        PruneOutcome::Pruned { slot, .. } => assert_eq!(slot, 0),
        other => panic!("queue slot 0 should prune after drain, got {other:?}"),
    }
    match store
        .prune_oldest_claims(&pool)
        .await
        .expect("prune claim slot 0")
    {
        PruneOutcome::Pruned { slot, .. } => assert_eq!(slot, 0),
        other => panic!("claim slot 0 should prune after drain, got {other:?}"),
    }

    // Fully drained: every further rotate tick skips and the cursors
    // freeze. Operators observe the generation stop advancing on a
    // quiet queue — that is the intended behavior, not a pinned ring.
    let queue_cursor = ring_cursor(&pool, schema, "queue").await;
    let lease_cursor = ring_cursor(&pool, schema, "lease").await;
    let claim_cursor = ring_cursor(&pool, schema, "claim").await;
    for _ in 0..3 {
        assert!(
            matches!(
                store.rotate(&pool).await.expect("idle queue rotate"),
                RotateOutcome::SkippedIdle { slot } if slot == queue_cursor.0
            ),
            "drained queue ring must skip rotation"
        );
        assert!(
            matches!(
                store.rotate_leases(&pool).await.expect("idle lease rotate"),
                RotateOutcome::SkippedIdle { slot } if slot == lease_cursor.0
            ),
            "compact traffic never wrote lease rows; lease ring must skip rotation"
        );
        assert!(
            matches!(
                store.rotate_claims(&pool).await.expect("idle claim rotate"),
                RotateOutcome::SkippedIdle { slot } if slot == claim_cursor.0
            ),
            "drained claim ring must skip rotation"
        );
    }
    assert_eq!(ring_cursor(&pool, schema, "queue").await, queue_cursor);
    assert_eq!(ring_cursor(&pool, schema, "lease").await, lease_cursor);
    assert_eq!(ring_cursor(&pool, schema, "claim").await, claim_cursor);

    // New traffic un-idles the ring: the next enqueue makes the current
    // slot non-empty and rotation resumes on the following tick.
    let _revived = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 2 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    match store
        .rotate(&pool)
        .await
        .expect("rotate revived queue ring")
    {
        RotateOutcome::Rotated { slot, .. } => assert_eq!(slot, 2),
        other => panic!("revived queue ring must rotate, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_legacy_zero_deadline_claim_conversion_error_rolls_back() {
    let (_db_guard, pool) = setup_pool(4).await;
    let schema = "awa_qs_legacy_zero_deadline_claim_rollback";
    let queue = "qs_legacy_zero_deadline_claim_rollback";
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

    sqlx::query(&format!(
        "UPDATE {schema}.ready_entries SET payload = '{{\"metadata\":\"bad\"}}'::jsonb WHERE job_id = $1"
    ))
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("corrupt ready payload");

    store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect_err("corrupt payload should fail runtime conversion");
    assert_eq!(
        lease_count(&pool, &store).await,
        0,
        "failed conversion must not leave an unrescueable legacy zero-deadline lease"
    );

    sqlx::query(&format!(
        "UPDATE {schema}.ready_entries SET payload = '{{}}'::jsonb WHERE job_id = $1"
    ))
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("repair ready payload");

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim should remain available after conversion rollback");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.id, job_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_legacy_zero_deadline_claim_without_receipts_succeeds() {
    let (_db_guard, pool) = setup_pool(4).await;
    let schema = "awa_qs_legacy_zero_deadline_claim";
    let queue = "qs_legacy_zero_deadline_claim";
    let store = create_store(&pool, schema).await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 311 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("zero-deadline legacy claim should not reference a missing parameter");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.id, job_id);
    assert!(
        !claimed[0].claim.lease_claim_receipt,
        "legacy non-receipts claim should materialize into leases"
    );
    assert_eq!(
        claimed[0].claim.receipt_id, None,
        "legacy non-receipts claims must not carry receipt identities"
    );

    let deadline_at: Option<DateTime<Utc>> = sqlx::query_scalar(&format!(
        "SELECT deadline_at FROM {schema}.leases WHERE job_id = $1"
    ))
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("claimed lease should exist");
    assert!(
        deadline_at.is_none(),
        "zero-deadline legacy claim should leave leases.deadline_at NULL"
    );
    assert_eq!(
        lease_claim_count(&pool, &store).await,
        0,
        "legacy non-receipts claim must not write receipt rows"
    );
}

/// Receipt-plane partition-migration test (see ADR-023). Start from
/// a schema that still has the legacy regular (non-partitioned)
/// `lease_claims` + `lease_claim_closures`, seed some rows in them,
/// run `prepare_schema`, and assert:
/// - receipt claim/closure parents are now partitioned (`relkind = 'p'`)
/// - all pre-existing rows landed in the current `claim_ring_state` slot
/// - the legacy tables are dropped
/// Validates the rename → create partitioned → copy → drop path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lease_claim_migration_preserves_rows() {
    let (_db_guard, pool) = setup_pool(4).await;
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
            enqueue_shard SMALLINT NOT NULL DEFAULT 0,
            claimed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            materialized_at TIMESTAMPTZ,
            deadline_at TIMESTAMPTZ,
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
                 priority, attempt, max_attempts, lane_seq, enqueue_shard,
                 claimed_at, materialized_at, deadline_at)
            VALUES ($1, 1, 0, 0, 'legacy', 2, 1, 25, $1, ($1 % 2)::smallint,
                    now(), NULL, TIMESTAMPTZ '2030-01-01 00:00:00+00')
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

    // Receipt claim/closure parents are partitioned now.
    for name in [
        "lease_claims",
        "lease_claim_closures",
        "lease_claim_closure_batches",
    ] {
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

    let preserved_claim_shape: (i16, bool) = sqlx::query_as(&format!(
        r#"
        SELECT enqueue_shard,
               deadline_at = TIMESTAMPTZ '2030-01-01 00:00:00+00'
        FROM {schema}.lease_claims
        WHERE job_id = 3
          AND run_lease = 1
        "#
    ))
    .fetch_one(&pool)
    .await
    .expect("read migrated claim metadata");
    assert_eq!(preserved_claim_shape, (1, true));

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
    let (_db_guard, pool) = setup_pool(10).await;
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
            lease_claim_receipts: false,
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
    let (_db_guard, pool) = setup_pool(20).await;
    let queue = "qs_two_clients";
    let schema = "awa_qs_two_clients";
    let store = create_store(&pool, schema).await;
    let store_config = QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        lease_claim_receipts: false,
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
        let completed = completed_terminal_count(&pool, &store, queue).await;
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
    let (_db_guard, pool) = setup_pool(10).await;
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
    let (_db_guard, pool) = setup_pool(10).await;
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
    let (_db_guard, pool) = setup_pool(10).await;
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
async fn test_queue_storage_receipt_cancel_writes_batch_closure() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_receipt_cancel_closure";
    let schema = "awa_qs_receipt_cancel_closure";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;
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
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("Failed to claim receipt cancel job");
    let claimed = claimed.into_iter().next().expect("missing claimed job");
    assert!(
        claimed.claim.lease_claim_receipt,
        "zero-deadline claim should use receipt plane"
    );

    let cancelled = store
        .cancel_running(&pool, job_id, claimed.job.run_lease, "test cancel", None)
        .await
        .expect("Failed to cancel receipt-backed running job")
        .expect("Expected receipt-backed running job to be cancelled");
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "a compact (batch-sourced) claim has no lease_claims row, so cancel closes it through the batch ledger rather than an explicit closure"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        1,
        "cancelled compact attempts keep batch closure evidence"
    );

    let completed = store
        .complete_runtime_batch(&pool, std::slice::from_ref(&claimed))
        .await
        .expect("Failed to attempt stale completion after receipt cancel");
    assert!(
        completed.is_empty(),
        "late completion should be ignored after receipt cancel"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_dlq_and_retry_race_has_single_winner() {
    let (_db_guard, pool) = setup_pool(10).await;
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
        wait_for_dlq_count(&pool, &store, queue, 0, Duration::from_secs(5)).await;
    } else {
        wait_for_dlq_count(&pool, &store, queue, 1, Duration::from_secs(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_register_callback_rejects_stale_lease() {
    let (_db_guard, pool) = setup_pool(10).await;
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
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_attempt_state_short_job";
    let schema = "awa_qs_runtime_attempt_state_short";
    let store = create_store(&pool, schema).await;
    let gate = BlockingCompleteWorkerGate::new();
    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            lease_claim_receipts: false,
            ..Default::default()
        },
        gate.worker(),
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

    gate.wait_until_entered(Duration::from_secs(5)).await;
    gate.release();

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
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_lease_claim_short_job";
    let schema = "awa_qs_runtime_lease_claim_short";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;
    let gate = BlockingCompleteWorkerGate::new();
    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
        gate.worker(),
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
    assert_eq!(lease_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_batch_count(&pool, &store).await, 1);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 1);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 0);
    let running_counts = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to load queue counts while receipt-backed job is running");
    assert_eq!(running_counts.running, 1);

    gate.wait_until_entered(Duration::from_secs(5)).await;
    gate.release();

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
    assert_eq!(lease_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_batch_count(&pool, &store).await, 1);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "compact successful completion must not write per-job closure rows"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        1,
        "compact successful completion closes the receipt through claim-local batch evidence"
    );
    assert_eq!(completed_done_count(&pool, &store, queue).await, 0);
    assert_eq!(completed_terminal_count(&pool, &store, queue).await, 1);
    assert_eq!(
        receipt_completion_batch_count(&pool, &store, queue).await,
        1
    );
    let completed_counts = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to load queue counts after receipt-backed completion");
    assert_eq!(completed_counts.running, 0);

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_custom_metadata_completion_uses_wide_terminal_row() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_receipt_custom_metadata_wide";
    let schema = "awa_qs_receipt_custom_metadata_wide";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 22 },
        InsertOpts {
            queue: queue.to_string(),
            metadata: serde_json::json!({ "tenant": "acme" }),
            ..Default::default()
        },
    )
    .await;
    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("claim custom-metadata job");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.id, job_id);

    store
        .complete_runtime_batch(&pool, &claimed)
        .await
        .expect("complete custom-metadata job");

    assert_eq!(
        done_entries_count(&pool, schema, queue).await,
        1,
        "custom metadata needs a wide terminal row"
    );
    assert_eq!(
        receipt_completion_batch_count(&pool, &store, queue).await,
        0,
        "custom metadata must not use compact receipt batches"
    );
    assert_eq!(completed_terminal_count(&pool, &store, queue).await, 1);

    let metadata: serde_json::Value = sqlx::query_scalar(&format!(
        "SELECT payload->'metadata' FROM {schema}.terminal_jobs WHERE job_id = $1"
    ))
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("read completed custom metadata");
    assert_eq!(metadata, serde_json::json!({ "tenant": "acme" }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_sql_compat_delete_tombstones_compact_receipt_completion() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_receipt_delete_compat_closure";
    let schema = "awa_qs_receipt_delete_compat_closure";
    let store_config = QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        queue_stripe_count: 1,
        lease_claim_receipts: true,
        claim_slot_count: 2,
    };
    let store = create_store_with_config(&pool, store_config.clone()).await;
    let client = queue_storage_client(&pool, queue, store_config, CompleteWorker);

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 12 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    client
        .start()
        .await
        .expect("Failed to start receipt delete-compat client");
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

    assert_eq!(completed_done_count(&pool, &store, queue).await, 0);
    assert_eq!(completed_terminal_count(&pool, &store, queue).await, 1);
    assert_eq!(
        receipt_completion_batch_count(&pool, &store, queue).await,
        1
    );
    assert_eq!(lease_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_batch_count(&pool, &store).await, 1);
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "compact successful completion should not write explicit closure evidence"
    );

    let deleted: bool = sqlx::query_scalar("SELECT awa.delete_job_compat($1)")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("delete_job_compat should delete completed job");
    assert!(deleted, "completed job should be deleted through compat");

    assert_eq!(completed_done_count(&pool, &store, queue).await, 0);
    assert_eq!(completed_terminal_count(&pool, &store, queue).await, 0);
    assert_eq!(
        receipt_completion_batch_count(&pool, &store, queue).await,
        1,
        "compat delete should tombstone compact completions without removing the batch row"
    );
    assert_eq!(receipt_completion_tombstone_count(&pool, &store).await, 1);
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "compat delete must not add explicit closure rows for compact batches"
    );
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);

    match store
        .rotate_claims(&pool)
        .await
        .expect("rotate claim ring after compat delete")
    {
        RotateOutcome::Rotated { slot, .. } => assert_eq!(slot, 1),
        other => panic!("expected claim ring rotation after compat delete, got {other:?}"),
    }

    match store
        .prune_oldest_claims(&pool)
        .await
        .expect("claim prune after compat delete")
    {
        PruneOutcome::Pruned { slot, .. } => assert_eq!(slot, 0),
        other => panic!(
            "compat-deleted terminal should leave enough closure evidence to prune claim slot, got {other:?}"
        ),
    }
}

/// A compact (zero-deadline, receipts-mode) claim that is rescued while
/// still open must close into `lease_claim_closure_batches`, NOT an
/// explicit `lease_claim_closures` row. The compact claim has only a
/// `lease_claim_batches` row (no `lease_claims` row), so the queue prune
/// gate `queue_prune_has_unclosed_claim_refs_tx` can only see the closure
/// through the batch ledger's `compact_count`. An explicit closure for it
/// is invisible to the gate (its `explicit_count` JOINs `lease_claims`),
/// which permanently wedged the queue slot on
/// `SkippedActive { QueueUnclosedClaimRefs }` until the claim ring pruned.
/// This drives the rescue path directly and proves the queue slot prunes
/// without first pruning the claim ring.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_rescued_compact_claim_closes_via_batch_and_queue_prunes() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_rescue_compact_batch_closure";
    let schema = "awa_qs_rescue_compact_batch_closure";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 31 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    // Compact claim: zero deadline routes into lease_claim_batches with no
    // lease_claims row.
    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("compact claim of zero-deadline job");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.id, job_id);
    assert!(
        claimed[0].claim.lease_claim_receipt,
        "zero-deadline receipt claim must be compact"
    );
    let run_lease = claimed[0].job.run_lease;
    assert_eq!(
        lease_claim_batch_count(&pool, &store).await,
        1,
        "compact claim must land in lease_claim_batches"
    );
    assert_eq!(
        lease_claim_count(&pool, &store).await,
        0,
        "compact claim must not write a lease_claims row"
    );
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 1);

    // Make the open claim stale so heartbeat rescue force-closes it. The
    // compact claim has no attempt_state row, so the rescue staleness
    // predicate falls back to claimed_at.
    age_receipt_claim(&pool, &store, job_id, run_lease, Duration::from_secs(600)).await;

    let rescued = store
        .rescue_stale_heartbeats(&pool, Duration::from_secs(60))
        .await
        .expect("rescue stale compact claim");
    assert_eq!(rescued.len(), 1, "stale compact claim must be rescued");
    assert_eq!(rescued[0].id, job_id);

    // Core fix assertion: the rescue closed the compact claim into the
    // batch ledger, not the explicit closure table.
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "rescue of a compact claim must not write an explicit closure row"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        1,
        "rescue of a compact claim must close it via lease_claim_closure_batches"
    );
    assert_eq!(
        open_receipt_claim_count(&pool, &store).await,
        0,
        "the rescued compact claim must read as closed"
    );

    // Rotate the QUEUE ring off slot 0 so prune_oldest targets it.
    match store.rotate(&pool).await.expect("rotate queue ring") {
        RotateOutcome::Rotated { slot, .. } => assert_eq!(slot, 1),
        other => panic!("expected queue ring rotation to slot 1, got {other:?}"),
    }

    // The queue slot must prune WITHOUT first pruning the claim ring: the
    // batch closure is visible to the queue prune gate via compact_count.
    let prune_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let outcome = store
            .prune_oldest(&pool, Duration::ZERO)
            .await
            .expect("prune oldest queue slot after compact rescue");
        match outcome {
            PruneOutcome::Pruned { slot, .. } => {
                assert_eq!(slot, 0);
                break;
            }
            PruneOutcome::Blocked { slot: 0 } if Instant::now() < prune_deadline => {
                continue;
            }
            PruneOutcome::SkippedActive {
                slot: 0,
                reason: SkipReason::QueueUnclosedClaimRefs,
                ..
            } => panic!(
                "compact claim closed via batch ledger must not wedge queue prune on unclosed claim refs"
            ),
            other => panic!("unexpected prune outcome after compact rescue: {other:?}"),
        }
    }
}

/// Terminal close (admin cancel) of an OPEN compact claim must also close
/// it into `lease_claim_closure_batches` rather than an explicit closure
/// row, for the same queue-prune-visibility reason as the rescue path.
/// Admin cancel of a never-materialized compact claim routes through the
/// receipt-only cancel branch in `cancel_job_tx`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_cancelled_compact_claim_closes_via_batch_and_queue_prunes() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_cancel_compact_batch_closure";
    let schema = "awa_qs_cancel_compact_batch_closure";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 32 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("compact claim of zero-deadline job");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.id, job_id);
    assert!(claimed[0].claim.lease_claim_receipt);
    assert_eq!(lease_claim_batch_count(&pool, &store).await, 1);
    assert_eq!(lease_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 1);

    let cancelled = store
        .cancel_job(&pool, job_id)
        .await
        .expect("cancel open compact claim")
        .expect("cancel must move the compact claim to a terminal row");
    assert_eq!(cancelled.id, job_id);
    assert_eq!(cancelled.state, JobState::Cancelled);

    // Core fix assertion: cancel of a compact claim closes it via the
    // batch ledger, not an explicit closure row.
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "cancel of a compact claim must not write an explicit closure row"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        1,
        "cancel of a compact claim must close it via lease_claim_closure_batches"
    );
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);

    match store.rotate(&pool).await.expect("rotate queue ring") {
        RotateOutcome::Rotated { slot, .. } => assert_eq!(slot, 1),
        other => panic!("expected queue ring rotation to slot 1, got {other:?}"),
    }

    let prune_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let outcome = store
            .prune_oldest(&pool, Duration::ZERO)
            .await
            .expect("prune oldest queue slot after compact cancel");
        match outcome {
            PruneOutcome::Pruned { slot, .. } => {
                assert_eq!(slot, 0);
                break;
            }
            PruneOutcome::Blocked { slot: 0 } if Instant::now() < prune_deadline => {
                continue;
            }
            PruneOutcome::SkippedActive {
                slot: 0,
                reason: SkipReason::QueueUnclosedClaimRefs,
                ..
            } => panic!(
                "compact claim closed via batch ledger must not wedge queue prune on unclosed claim refs"
            ),
            other => panic!("unexpected prune outcome after compact cancel: {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_capacity_wake_drains_after_partial_drain() {
    let (exporter, meter_provider) = install_in_memory_metrics();
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_capacity_wake_partial_drain";
    let schema = "awa_qs_capacity_wake_partial_drain";
    let store_config = QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        lease_claim_receipts: true,
        claim_slot_count: 2,
        ..Default::default()
    };
    let store = create_store_with_config(&pool, store_config.clone()).await;
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 8,
                poll_interval: Duration::from_secs(5),
                deadline_duration: Duration::ZERO,
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            store_config,
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .claim_rotate_interval(Duration::from_secs(60))
        .register_worker(CompleteWorker)
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .deadline_rescue_interval(Duration::from_millis(100))
        .callback_rescue_interval(Duration::from_millis(25))
        .build()
        .expect("Failed to build capacity wake client");

    client
        .start()
        .await
        .expect("Failed to start capacity wake client");

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 2003 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        // Keep the dispatcher fallback poll interval long so any post-completion
        // empty claim within the 250ms observation window must come from the
        // capacity wake path. The first enqueue can still race dispatcher
        // LISTEN setup in CI, so allow one missed-NOTIFY fallback poll before
        // declaring the job stuck.
        Duration::from_secs(15),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);

    tokio::time::sleep(Duration::from_millis(250)).await;
    client.shutdown(Duration::from_secs(5)).await;
    meter_provider
        .force_flush()
        .expect("Failed to flush metrics");
    let resource_metrics = exporter
        .get_finished_metrics()
        .expect("Failed to get metrics");

    let capacity_empty_claims = sum_counter_metric_with_attribute(
        &resource_metrics,
        "awa.dispatch.empty_claims",
        "awa.dispatch.reason",
        "capacity",
    );
    assert!(
        capacity_empty_claims > 0,
        "partial-drain completion wake should immediately drain capacity again"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_striped_short_jobs_complete_via_lease_claim_receipts() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_lease_claim_short_job_striped";
    let schema = "awa_qs_runtime_lease_claim_short_striped";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 4,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;
    let gate = BlockingCompleteWorkerGate::new();
    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 4,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
        gate.worker(),
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

    gate.wait_until_entered(Duration::from_secs(5)).await;
    gate.release();

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

/// Receipts mode + non-zero deadline_duration: the claim path writes the
/// shared deadline onto compact `lease_claim_batches.deadline_at` (#246 —
/// one row per claimed batch, not one lease_claims row per job), and the
/// batch deadline-rescue maintenance path force-closes members whose
/// deadline has passed without closure evidence or a materialized lease.
/// This exercises the receipts-side counterpart that
/// `rescue_expired_receipt_deadlines_tx` adds alongside the lease-side
/// `rescue_expired_deadlines` scan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_deadline_rescue_force_closes_expired_claim() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_lease_claim_deadline_rescue";
    let schema = "awa_qs_runtime_lease_claim_deadline_rescue";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 3 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    // Sub-second deadline: rescue should sweep this claim on the next
    // maintenance tick. The claim write path stores the shared deadline
    // on the compact claim batch; deadline > 0 no longer materializes
    // per-job lease_claims rows.
    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_millis(100))
        .await
        .expect("receipts-mode claim with deadline_duration > 0 should succeed");
    assert_eq!(claimed.len(), 1, "expected one claimed job");
    assert!(
        claimed[0].claim.lease_claim_receipt,
        "claim should be on the receipts path"
    );
    assert!(
        claimed[0].claim.claim_batch_id.is_some(),
        "deadline-backed claims must use the compact batch representation (#246)"
    );

    // Verify deadline_at landed on the compact claim batch and that no
    // per-job lease_claims row was written.
    let deadline_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(&format!(
        "SELECT deadline_at FROM {schema}.lease_claim_batches WHERE batch_id = $1"
    ))
    .bind(claimed[0].claim.claim_batch_id)
    .fetch_one(&pool)
    .await
    .expect("lease_claim_batches row should exist");
    assert!(
        deadline_at.is_some(),
        "deadline_at must be set on the claim batch when deadline_duration > 0"
    );
    let row_claims: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {schema}.lease_claims"
    ))
    .fetch_one(&pool)
    .await
    .expect("count lease_claims");
    assert_eq!(
        row_claims, 0,
        "deadline-backed claims must not write row-local lease_claims rows"
    );

    sqlx::query(&format!(
        "UPDATE {schema}.lease_claim_batches \
         SET deadline_at = clock_timestamp() - interval '1 millisecond' \
         WHERE batch_id = $1"
    ))
    .bind(claimed[0].claim.claim_batch_id)
    .execute(&pool)
    .await
    .expect("Failed to expire claim batch deadline");

    let rescued = store
        .rescue_expired_deadlines(&pool)
        .await
        .expect("rescue_expired_deadlines should succeed");
    assert_eq!(rescued.len(), 1, "exactly one claim should be rescued");
    assert_eq!(rescued[0].id, job_id);

    // Closure evidence is compact: a lease_claim_closure_batches row with
    // outcome='deadline_expired' covering the member's receipt_id.
    let outcome: String = sqlx::query_scalar(&format!(
        "SELECT outcome FROM {schema}.lease_claim_closure_batches \
         WHERE receipt_ranges @> $1::bigint"
    ))
    .bind(claimed[0].claim.receipt_id)
    .fetch_one(&pool)
    .await
    .expect("closure batch row should exist after rescue");
    assert_eq!(outcome, "deadline_expired");

    // The rescued attempt is re-routed through retry policy; the job must
    // become claimable again on a later attempt.
    let reclaimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(300))
        .await
        .expect("claim after promote should succeed");
    let reclaimed = if reclaimed.is_empty() {
        // The rescued row lands in deferred_jobs with backoff; promote it.
        sqlx::query(&format!(
            "UPDATE {schema}.deferred_jobs SET run_at = clock_timestamp() WHERE job_id = $1"
        ))
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("make deferred rescued job due");
        store
            .promote_due(&pool, JobState::Retryable, 10)
            .await
            .expect("promote rescued job");
        store
            .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(300))
            .await
            .expect("claim promoted rescued job")
    } else {
        reclaimed
    };
    assert_eq!(
        reclaimed.len(),
        1,
        "rescued job must be claimable again after deadline force-close"
    );
    assert_eq!(reclaimed[0].job.id, job_id);
    assert_eq!(
        reclaimed[0].job.run_lease,
        claimed[0].job.run_lease + 1,
        "re-claim must be a fresh attempt"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_deadline_rescue_cursor_advances_over_terminal_evidence() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_receipt_deadline_cursor";
    let schema = "awa_qs_receipt_deadline_cursor";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let first = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 21 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    let second = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 22 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    // Two separate claim calls produce two compact claim batches (#246),
    // so the batch deadline cursor has two distinct (deadline_at, batch_id)
    // entries to sweep.
    let claimed_first = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("claim first deadline cursor job");
    assert_eq!(claimed_first.len(), 1);
    assert_eq!(claimed_first[0].job.id, first);
    let claimed_second = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("claim second deadline cursor job");
    assert_eq!(claimed_second.len(), 1);
    assert_eq!(claimed_second[0].job.id, second);
    let claimed: Vec<_> = claimed_first
        .iter()
        .cloned()
        .chain(claimed_second.iter().cloned())
        .collect();
    assert!(claimed.iter().all(|entry| entry.claim.lease_claim_receipt));
    assert!(claimed
        .iter()
        .all(|entry| entry.claim.claim_batch_id.is_some()));
    let first_batch = claimed_first[0]
        .claim
        .claim_batch_id
        .expect("first claim batch id");
    let second_batch = claimed_second[0]
        .claim
        .claim_batch_id
        .expect("second claim batch id");

    store
        .complete_runtime_batch(&pool, &claimed[0..1])
        .await
        .expect("complete first receipt job");

    sqlx::query(&format!(
        r#"
        UPDATE {schema}.lease_claim_batches
        SET deadline_at = CASE
            WHEN batch_id = $1 THEN clock_timestamp() - interval '10 seconds'
            WHEN batch_id = $2 THEN clock_timestamp() + interval '10 minutes'
        END
        WHERE batch_id IN ($1, $2)
        "#
    ))
    .bind(first_batch)
    .bind(second_batch)
    .execute(&pool)
    .await
    .expect("set claim batch deadlines");

    let rescued = store
        .rescue_expired_deadlines(&pool)
        .await
        .expect("deadline rescue should advance over terminal evidence");
    assert!(
        rescued.is_empty(),
        "terminal evidence should close the expired first claim and the future second claim must not be rescued"
    );

    let cursor_batch: i64 = sqlx::query_scalar(&format!(
        "SELECT batch_deadline_cursor_batch_id FROM {schema}.claim_ring_slots WHERE slot = $1"
    ))
    .bind(claimed[0].claim.claim_slot)
    .fetch_one(&pool)
    .await
    .expect("read batch deadline cursor");
    assert_eq!(
        cursor_batch, first_batch,
        "batch deadline cursor should advance past the completed batch instead of rechecking it forever"
    );

    sqlx::query(&format!(
        "UPDATE {schema}.lease_claim_batches \
         SET deadline_at = clock_timestamp() - interval '1 second' \
         WHERE batch_id = $1"
    ))
    .bind(second_batch)
    .execute(&pool)
    .await
    .expect("expire second claim batch deadline");

    let rescued = store
        .rescue_expired_deadlines(&pool)
        .await
        .expect("deadline rescue should continue after cursor");
    assert_eq!(rescued.len(), 1);
    assert_eq!(rescued[0].id, second);

    // Compact members close compactly: rescue writes a
    // lease_claim_closure_batches row, never a row-local closure.
    let row_closures: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {schema}.lease_claim_closures"
    ))
    .fetch_one(&pool)
    .await
    .expect("count receipt closures");
    assert_eq!(
        row_closures, 0,
        "compact members must not write row-local closures"
    );
    let expired_closures: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {schema}.lease_claim_closure_batches WHERE outcome = 'deadline_expired'"
    ))
    .fetch_one(&pool)
    .await
    .expect("count deadline_expired closure batches");
    assert_eq!(expired_closures, 1);
}

/// Compact deadline claim happy path (#246): a deadline-backed batch claim
/// completes through the ordinary compact completion path; the batch's
/// deadline never fires, rescue is a no-op, and the closure evidence
/// balances the claim evidence (the claim-prune count proof).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_compact_deadline_claim_completes_normally() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_compact_deadline_complete";
    let schema = "awa_qs_compact_deadline_complete";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 61 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(300))
        .await
        .expect("deadline-backed compact claim");
    assert_eq!(claimed.len(), 1);
    assert!(claimed[0].claim.claim_batch_id.is_some());

    store
        .complete_runtime_batch(&pool, &claimed)
        .await
        .expect("complete deadline-backed compact claim");

    let completed: String = sqlx::query_scalar(&format!(
        "SELECT outcome FROM {schema}.lease_claim_closure_batches \
         WHERE receipt_ranges @> $1::bigint"
    ))
    .bind(claimed[0].claim.receipt_id)
    .fetch_one(&pool)
    .await
    .expect("completion closure batch for deadline-backed claim");
    assert_eq!(completed, "completed");

    // Rescue must not touch the completed member even after the deadline
    // passes: closure evidence wins.
    sqlx::query(&format!(
        "UPDATE {schema}.lease_claim_batches SET deadline_at = clock_timestamp() - interval '1 second'"
    ))
    .execute(&pool)
    .await
    .expect("expire completed batch deadline");
    let rescued = store
        .rescue_expired_deadlines(&pool)
        .await
        .expect("rescue after completion");
    assert!(
        rescued.is_empty(),
        "completed deadline-backed claim must not be rescued"
    );

    // Claim-prune count proof shape: claims (rows + batch members) must
    // equal closures (explicit + compact closed_count).
    let (claims_total, closures_total): (i64, i64) = sqlx::query_as(&format!(
        r#"
        SELECT
            (SELECT count(*)::bigint FROM {schema}.lease_claims)
            + (SELECT COALESCE(sum(claimed_count), 0)::bigint FROM {schema}.lease_claim_batches),
            (SELECT count(*)::bigint FROM {schema}.lease_claim_closures)
            + (SELECT COALESCE(sum(closed_count), 0)::bigint FROM {schema}.lease_claim_closure_batches)
        "#
    ))
    .fetch_one(&pool)
    .await
    .expect("count claim/closure totals");
    assert_eq!(
        claims_total, closures_total,
        "completed deadline-backed batch must satisfy the prune count proof"
    );
    let _ = job_id;
}

/// Partial-batch deadline expiry (#246): two jobs claimed in one call share
/// one compact batch and one deadline. One member completes before expiry;
/// after the batch deadline passes, rescue force-closes ONLY the open
/// member, and the count proof balances (one 'completed' + one
/// 'deadline_expired' closure for two claimed members).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_compact_deadline_partial_batch_rescues_only_expired_member() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_compact_deadline_partial";
    let schema = "awa_qs_compact_deadline_partial";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let first = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 62 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    let second = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 63 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 2, Duration::from_secs(300))
        .await
        .expect("claim two jobs in one deadline-backed batch");
    assert_eq!(claimed.len(), 2);
    let batch_ids: std::collections::BTreeSet<i64> = claimed
        .iter()
        .map(|entry| entry.claim.claim_batch_id.expect("compact batch id"))
        .collect();
    assert_eq!(
        batch_ids.len(),
        1,
        "both jobs must land in one compact claim batch"
    );

    let completed_entry = claimed
        .iter()
        .find(|entry| entry.job.id == first)
        .expect("claimed first job")
        .clone();
    let open_entry = claimed
        .iter()
        .find(|entry| entry.job.id == second)
        .expect("claimed second job")
        .clone();

    store
        .complete_runtime_batch(&pool, std::slice::from_ref(&completed_entry))
        .await
        .expect("complete one member of the deadline batch");

    sqlx::query(&format!(
        "UPDATE {schema}.lease_claim_batches SET deadline_at = clock_timestamp() - interval '1 second'"
    ))
    .execute(&pool)
    .await
    .expect("expire shared batch deadline");

    let rescued = store
        .rescue_expired_deadlines(&pool)
        .await
        .expect("rescue expired batch members");
    assert_eq!(rescued.len(), 1, "only the open member is rescued");
    assert_eq!(rescued[0].id, second);

    let completed_outcome: String = sqlx::query_scalar(&format!(
        "SELECT outcome FROM {schema}.lease_claim_closure_batches WHERE receipt_ranges @> $1::bigint"
    ))
    .bind(completed_entry.claim.receipt_id)
    .fetch_one(&pool)
    .await
    .expect("completed member closure");
    assert_eq!(completed_outcome, "completed");
    let expired_outcome: String = sqlx::query_scalar(&format!(
        "SELECT outcome FROM {schema}.lease_claim_closure_batches WHERE receipt_ranges @> $1::bigint"
    ))
    .bind(open_entry.claim.receipt_id)
    .fetch_one(&pool)
    .await
    .expect("expired member closure");
    assert_eq!(expired_outcome, "deadline_expired");

    // Cursor advances past the batch once every member is closed.
    let cursor_batch: i64 = sqlx::query_scalar(&format!(
        "SELECT batch_deadline_cursor_batch_id FROM {schema}.claim_ring_slots WHERE slot = $1"
    ))
    .bind(open_entry.claim.claim_slot)
    .fetch_one(&pool)
    .await
    .expect("read batch deadline cursor");
    assert_eq!(
        cursor_batch,
        open_entry.claim.claim_batch_id.expect("batch id"),
        "batch deadline cursor should advance past the fully-closed batch"
    );

    let (claims_total, closures_total): (i64, i64) = sqlx::query_as(&format!(
        r#"
        SELECT
            (SELECT count(*)::bigint FROM {schema}.lease_claims)
            + (SELECT COALESCE(sum(claimed_count), 0)::bigint FROM {schema}.lease_claim_batches),
            (SELECT count(*)::bigint FROM {schema}.lease_claim_closures)
            + (SELECT COALESCE(sum(closed_count), 0)::bigint FROM {schema}.lease_claim_closure_batches)
        "#
    ))
    .fetch_one(&pool)
    .await
    .expect("count claim/closure totals");
    assert_eq!(claims_total, 2);
    assert_eq!(
        claims_total, closures_total,
        "partially-expired batch must still satisfy the prune count proof"
    );
}

/// Legacy in-flight row-local deadline claims (written before the #246
/// compact-deadline upgrade) must still be force-closed by the row-local
/// deadline rescue path after a rolling upgrade.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_legacy_row_deadline_claim_still_rescued() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_legacy_row_deadline";
    let schema = "awa_qs_legacy_row_deadline";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 64 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(300))
        .await
        .expect("claim job to transplant into legacy shape");
    assert_eq!(claimed.len(), 1);
    let entry = claimed[0].clone();

    // Transplant the compact batch claim into the pre-upgrade shape: one
    // row-local lease_claims row with an (already expired) deadline and no
    // batch row — exactly what in-flight deadline claims look like when a
    // 0.6 node wrote them just before the upgrade landed.
    sqlx::query(&format!(
        "DELETE FROM {schema}.lease_claim_batches WHERE batch_id = $1"
    ))
    .bind(entry.claim.claim_batch_id)
    .execute(&pool)
    .await
    .expect("remove compact batch row");
    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.lease_claims (
            claim_slot, receipt_id, job_id, run_lease, ready_slot,
            ready_generation, queue, priority, attempt, max_attempts,
            lane_seq, enqueue_shard, deadline_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
            clock_timestamp() - interval '1 second'
        )
        "#
    ))
    .bind(entry.claim.claim_slot)
    .bind(entry.claim.receipt_id)
    .bind(entry.job.id)
    .bind(entry.job.run_lease)
    .bind(entry.claim.ready_slot)
    .bind(entry.claim.ready_generation)
    .bind(&entry.claim.queue)
    .bind(entry.claim.priority)
    .bind(entry.job.attempt)
    .bind(entry.job.max_attempts)
    .bind(entry.claim.lane_seq)
    .bind(entry.claim.enqueue_shard)
    .execute(&pool)
    .await
    .expect("insert legacy row-local deadline claim");

    let rescued = store
        .rescue_expired_deadlines(&pool)
        .await
        .expect("rescue legacy row-local deadline claim");
    assert_eq!(rescued.len(), 1);
    assert_eq!(rescued[0].id, job_id);

    let outcome: String = sqlx::query_scalar(&format!(
        "SELECT outcome FROM {schema}.lease_claim_closures WHERE job_id = $1 AND run_lease = $2"
    ))
    .bind(job_id)
    .bind(entry.job.run_lease)
    .fetch_one(&pool)
    .await
    .expect("legacy row closure");
    assert_eq!(outcome, "deadline_expired");
}

/// Rotation/prune interaction (#246): an expired deadline batch in a sealed
/// claim slot keeps `prune_oldest_claims` at SkippedActive (the count proof
/// sees an open member) until deadline rescue writes the closure; the next
/// prune TRUNCATEs the slot and resets the batch deadline cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_expired_deadline_batch_blocks_prune_until_rescued() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_deadline_batch_prune_gate";
    let schema = "awa_qs_deadline_batch_prune_gate";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 4,
        },
    )
    .await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 65 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(300))
        .await
        .expect("deadline-backed compact claim in slot 0");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].claim.claim_slot, 0);

    // Seal slot 0 by rotating the claim ring, then expire the batch.
    match store.rotate_claims(&pool).await.expect("rotate claims") {
        RotateOutcome::Rotated { slot, .. } => assert_eq!(slot, 1),
        other => panic!("expected rotate to slot 1, got {other:?}"),
    }
    sqlx::query(&format!(
        "UPDATE {schema}.lease_claim_batches SET deadline_at = clock_timestamp() - interval '1 second'"
    ))
    .execute(&pool)
    .await
    .expect("expire sealed-slot batch deadline");

    // Prune must refuse while the expired member has no closure evidence.
    let blocked = store
        .prune_oldest_claims(&pool)
        .await
        .expect("prune with open claim");
    assert!(
        matches!(
            blocked,
            PruneOutcome::SkippedActive {
                slot: 0,
                reason: SkipReason::ClaimOpen,
                ..
            }
        ),
        "prune must skip while the expired batch member is unclosed, got {blocked:?}"
    );

    // Deadline rescue force-closes the sealed slot's expired member
    // (oldest initialized slot is swept first).
    let rescued = store
        .rescue_expired_deadlines(&pool)
        .await
        .expect("rescue sealed-slot expired batch");
    assert_eq!(rescued.len(), 1);
    assert_eq!(rescued[0].id, job_id);

    let pruned = store
        .prune_oldest_claims(&pool)
        .await
        .expect("prune after rescue");
    match pruned {
        PruneOutcome::Pruned { slot, .. } => assert_eq!(slot, 0),
        other => panic!("expected Pruned {{ slot: 0 }}, got {other:?}"),
    }

    let (cursor_deadline_reset, cursor_batch): (bool, i64) = sqlx::query_as(&format!(
        r#"
        SELECT
            batch_deadline_cursor_deadline_at = '-infinity'::timestamptz,
            batch_deadline_cursor_batch_id
        FROM {schema}.claim_ring_slots
        WHERE slot = 0
        "#
    ))
    .fetch_one(&pool)
    .await
    .expect("read batch deadline cursor after prune");
    assert!(
        cursor_deadline_reset && cursor_batch == 0,
        "prune must reset the batch deadline cursor for the truncated slot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_rescue_cursor_sweeps_past_fresh_claims_and_wraps() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_receipt_rescue_cursor";
    let schema = "awa_qs_runtime_receipt_rescue_cursor";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let job_a = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 11 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    let job_b = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 12 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    let job_c = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 13 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 3, Duration::ZERO)
        .await
        .expect("claim three receipt-backed jobs");
    assert_eq!(claimed.len(), 3);
    assert!(
        claimed.iter().all(|entry| entry.claim.lease_claim_receipt),
        "all claims should stay on the receipt plane"
    );

    let claim_a = claimed
        .iter()
        .find(|entry| entry.job.id == job_a)
        .expect("claim for job A")
        .clone();
    let claim_b = claimed
        .iter()
        .find(|entry| entry.job.id == job_b)
        .expect("claim for job B");
    let claim_c = claimed
        .iter()
        .find(|entry| entry.job.id == job_c)
        .expect("claim for job C")
        .clone();
    let claim_b = claim_b.clone();

    age_receipt_claim(
        &pool,
        &store,
        job_a,
        claim_a.job.run_lease,
        Duration::from_secs(300),
    )
    .await;
    age_receipt_claim(
        &pool,
        &store,
        job_b,
        claim_b.job.run_lease,
        Duration::from_secs(200),
    )
    .await;
    age_receipt_claim(
        &pool,
        &store,
        job_c,
        claim_c.job.run_lease,
        Duration::from_secs(100),
    )
    .await;

    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.attempt_state (job_id, run_lease, heartbeat_at, updated_at)
        VALUES ($1, $2, clock_timestamp(), clock_timestamp())
        "#
    ))
    .bind(job_b)
    .bind(claim_b.job.run_lease)
    .execute(&pool)
    .await
    .expect("seed fresh heartbeat blocker");

    let completed = store
        .complete_runtime_batch(&pool, std::slice::from_ref(&claim_a))
        .await
        .expect("complete first claim");
    assert_eq!(completed, vec![(job_a, claim_a.job.run_lease)]);

    let rescued = store
        .rescue_stale_heartbeats(&pool, Duration::from_secs(90))
        .await
        .expect("receipt rescue should rescue stale claims");
    assert_eq!(
        rescued.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![job_c],
        "fresh heartbeat on job B must not prevent rescuing stale job C"
    );

    let cursor: (DateTime<Utc>, i64, i64) = sqlx::query_as(&format!(
        r#"
        SELECT rescue_cursor_claimed_at, rescue_cursor_job_id, rescue_cursor_run_lease
        FROM {schema}.claim_ring_slots
        WHERE slot = $1
        "#
    ))
    .bind(claim_a.claim.claim_slot)
    .fetch_one(&pool)
    .await
    .expect("read rescue cursor after first rescue");
    assert_eq!(
        (cursor.1, cursor.2),
        (job_c, claim_c.job.run_lease),
        "fresh job B must not pin the sweep cursor ahead of stale job C"
    );

    sqlx::query(&format!(
        r#"
        UPDATE {schema}.attempt_state
        SET heartbeat_at = clock_timestamp() - interval '300 seconds',
            updated_at = clock_timestamp()
        WHERE job_id = $1 AND run_lease = $2
        "#
    ))
    .bind(job_b)
    .bind(claim_b.job.run_lease)
    .execute(&pool)
    .await
    .expect("make heartbeat blocker stale");

    let rescued = store
        .rescue_stale_heartbeats(&pool, Duration::from_secs(90))
        .await
        .expect("receipt rescue should resume after blocker turns stale");
    assert_eq!(
        rescued.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![job_b],
        "second rescue should wrap and close the formerly fresh claim"
    );

    let cursor: (DateTime<Utc>, i64, i64) = sqlx::query_as(&format!(
        r#"
        SELECT rescue_cursor_claimed_at, rescue_cursor_job_id, rescue_cursor_run_lease
        FROM {schema}.claim_ring_slots
        WHERE slot = $1
        "#
    ))
    .bind(claim_a.claim.claim_slot)
    .fetch_one(&pool)
    .await
    .expect("read rescue cursor after second rescue");
    assert_eq!(
        (cursor.1, cursor.2),
        (job_c, claim_c.job.run_lease),
        "cursor should advance through the rescued blocker and already-closed tail"
    );
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_rescue_skips_attempt_locked_for_completion() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_receipt_rescue_advisory";
    let schema = "awa_qs_receipt_rescue_advisory";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 77 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim receipt-backed job");
    assert_eq!(claimed.len(), 1);
    let claim = claimed.into_iter().next().expect("claimed job");
    assert!(claim.claim.lease_claim_receipt);
    assert!(
        claim.claim.receipt_id.is_some(),
        "receipt rescue interlock depends on exact receipt identity being available"
    );

    age_receipt_claim(
        &pool,
        &store,
        job_id,
        claim.job.run_lease,
        Duration::from_secs(300),
    )
    .await;

    let mut completion_lock = pool.begin().await.expect("begin completion lock tx");
    sqlx::query(
        r#"
        SELECT pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended(
                format('awa.receipt.complete:%s:%s', $1::bigint, $2::bigint),
                0
            )
        )
        "#,
    )
    .bind(job_id)
    .bind(claim.job.run_lease)
    .execute(completion_lock.as_mut())
    .await
    .expect("hold receipt completion advisory lock");

    let rescued_while_locked = store
        .rescue_stale_heartbeats(&pool, Duration::from_secs(90))
        .await
        .expect("receipt rescue while completion lock is held");
    assert!(
        rescued_while_locked.is_empty(),
        "receipt rescue must skip attempts currently held by worker completion"
    );
    assert_eq!(
        open_receipt_claim_count(&pool, &store).await,
        1,
        "skipped receipt claim must stay open for a later rescue or completion"
    );

    completion_lock
        .rollback()
        .await
        .expect("release completion advisory lock");

    let rescued_after_release = store
        .rescue_stale_heartbeats(&pool, Duration::from_secs(90))
        .await
        .expect("receipt rescue after completion lock release");
    assert_eq!(
        rescued_after_release
            .iter()
            .map(|job| job.id)
            .collect::<Vec<_>>(),
        vec![job_id],
        "receipt rescue should close the stale claim once the completion lock is free"
    );
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_claims_materialize_on_heartbeat() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_lease_claim_materialize_heartbeat";
    let schema = "awa_qs_runtime_lease_claim_materialize_heartbeat";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;
    let gate = BlockingCompleteWorkerGate::new();
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
                lease_claim_receipts: true,
                claim_slot_count: 2,
            },
            Duration::from_secs(60),
            Duration::from_millis(50),
        )
        .register_worker(gate.worker())
        .claim_rotate_interval(Duration::from_secs(60))
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
    assert_eq!(
        ready_segment_count(&pool, &store).await,
        1,
        "enqueue should append a compact ready segment for claim routing"
    );

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
    assert_eq!(lease_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_batch_count(&pool, &store).await, 1);
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
    assert_eq!(lease_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_batch_count(&pool, &store).await, 1);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 1);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 0);
    assert_eq!(lease_count(&pool, &store).await, 0);

    gate.wait_until_entered(Duration::from_secs(5)).await;
    gate.release();

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

    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "compact success should not write per-job completed closure rows"
    );
    assert_eq!(
        receipt_completion_batch_count(&pool, &store, queue).await,
        1,
        "attempt-state-only compact success should keep terminal history in receipt_completion_batches"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_queue_counts_do_not_double_count_materialized_receipt_claims() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_receipt_materialized_counts";
    let schema = "awa_qs_receipt_materialized_counts";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    store
        .enqueue_batch(&pool, queue, 1, 1)
        .await
        .expect("enqueue receipt-count job");
    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim receipt-count job");
    assert_eq!(claimed.len(), 1);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 1);

    let receipt_only_counts = store
        .queue_counts(&pool, queue)
        .await
        .expect("queue_counts with receipt-only claim");
    assert_eq!(receipt_only_counts.running, 1);

    let callback_id = store
        .register_callback(
            &pool,
            claimed[0].job.id,
            claimed[0].job.run_lease,
            Duration::from_secs(30),
        )
        .await
        .expect("register callback and materialize receipt claim");
    assert_eq!(lease_count(&pool, &store).await, 1);
    assert_eq!(lease_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_batch_count(&pool, &store).await, 1);
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "materializing a receipt-backed attempt must not close the receipt"
    );
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);

    let materialized_counts = store
        .queue_counts(&pool, queue)
        .await
        .expect("queue_counts with materialized receipt claim");
    assert_eq!(
        materialized_counts.running, 1,
        "materialized receipt-backed attempts have both a lease row and an \
         open receipt row, but queue_counts must count the attempt once"
    );

    let completed = store
        .complete_external(
            &pool,
            callback_id,
            Some(serde_json::json!({"ok": true})),
            Some(claimed[0].job.run_lease),
            false,
        )
        .await
        .expect("complete materialized receipt callback");
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "the materialized attempt's claim is compact (batch-sourced), so completion closes it through the batch ledger, not an explicit closure"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        1,
        "successful materialized receipt completion must close the compact claim via the batch ledger"
    );
    assert_eq!(completed_terminal_count(&pool, &store, queue).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_completes_materialized_receipt_after_lease_ring_rotation() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_receipt_materialized_rotated";
    let schema = "awa_qs_receipt_materialized_rotated";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    store
        .enqueue_batch(&pool, queue, 1, 1)
        .await
        .expect("enqueue receipt-materialized job");
    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim receipt-materialized job");
    assert_eq!(claimed.len(), 1);
    assert!(claimed[0].claim.lease_claim_receipt);
    assert_eq!(lease_count(&pool, &store).await, 0);

    let claim_lease_slot = claimed[0].claim.lease_slot;
    // With no lease rows anywhere, `rotate_leases` now reports
    // SkippedIdle (#371) instead of advancing. The invariant under test
    // is that materialization reads the ring's *current* slot rather
    // than the slot recorded at claim time, so move the cursor directly
    // — in production the ring advances like this whenever other jobs'
    // leases keep it busy.
    match store.rotate_leases(&pool).await.expect("rotate lease ring") {
        RotateOutcome::SkippedIdle { slot } => assert_eq!(
            slot, claim_lease_slot,
            "compact claim leaves no lease rows, so the lease ring is idle"
        ),
        other => panic!("expected idle lease ring before materialization, got {other:?}"),
    }
    sqlx::query(&format!(
        r#"
        UPDATE {schema}.lease_ring_state
        SET current_slot = (current_slot + 1) % slot_count,
            generation = generation + 1
        WHERE singleton = TRUE
        "#
    ))
    .execute(&pool)
    .await
    .expect("advance lease ring cursor for materialization regression setup");
    let rotated_lease_slot: i32 = sqlx::query_scalar(&format!(
        "SELECT current_slot FROM {schema}.lease_ring_state WHERE singleton = TRUE"
    ))
    .fetch_one(&pool)
    .await
    .expect("read advanced lease ring cursor");
    assert_ne!(
        rotated_lease_slot, claim_lease_slot,
        "test must materialize into a different lease slot than the original claim carried"
    );

    let callback_id = store
        .register_callback(
            &pool,
            claimed[0].job.id,
            claimed[0].job.run_lease,
            Duration::from_secs(30),
        )
        .await
        .expect("register callback and materialize receipt claim");
    let materialized_lease_slot: i32 = sqlx::query_scalar(&format!(
        "SELECT lease_slot FROM {schema}.leases WHERE job_id = $1 AND run_lease = $2"
    ))
    .bind(claimed[0].job.id)
    .bind(claimed[0].job.run_lease)
    .fetch_one(&pool)
    .await
    .expect("read materialized lease slot");
    assert_ne!(
        materialized_lease_slot, claim_lease_slot,
        "regression setup must prove the materialized lease slot differs"
    );

    let resumed = store
        .complete_external(
            &pool,
            callback_id,
            Some(serde_json::json!({"answer": 42})),
            Some(claimed[0].job.run_lease),
            true,
        )
        .await
        .expect("resume materialized callback");
    assert_eq!(resumed.state, JobState::Running);

    let updated = store
        .complete_runtime_batch(&pool, &claimed)
        .await
        .expect("complete original receipt claim after materialization");
    assert_eq!(
        updated,
        vec![(claimed[0].job.id, claimed[0].job.run_lease)],
        "completion must find the materialized lease by stable attempt identity"
    );
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "the materialized attempt's claim is compact (batch-sourced), so completion closes it through the batch ledger, not an explicit closure"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        1,
        "materialized receipt completion should close through the compact batch ledger"
    );
    assert_eq!(completed_terminal_count(&pool, &store, queue).await, 1);

    let loaded = store
        .load_job(&pool, claimed[0].job.id)
        .await
        .expect("load completed materialized receipt")
        .expect("completed materialized receipt row");
    assert_eq!(loaded.state, JobState::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_claims_retry_successfully() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_lease_claim_retry";
    let schema = "awa_qs_runtime_lease_claim_retry";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
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
            lease_claim_receipts: true,
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
    assert_eq!(lease_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_batch_count(&pool, &store).await, 2);
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "both compact attempts are batch-sourced, so neither writes an explicit closure row"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        2,
        "the retryable failure and the final compact success each close their compact claim through the batch ledger"
    );

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_claims_fail_retryable_without_materializing_leases() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_lease_claim_fail_retryable";
    let schema = "awa_qs_runtime_lease_claim_fail_retryable";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
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
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "a compact (batch-sourced) claim has no lease_claims row, so a retryable failure closes it through lease_claim_closure_batches, not an explicit closure"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        1,
        "retryable receipt failure closes the compact claim via the batch ledger"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_attempt_state_only_receipts_rescue_after_stale_heartbeat() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_lease_claim_attempt_rescue";
    let schema = "awa_qs_runtime_lease_claim_attempt_rescue";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
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
                lease_claim_receipts: true,
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
        .heartbeat_staleness(Duration::from_secs(2))
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

    age_attempt_heartbeat(
        &pool,
        &store,
        job_id,
        running.run_lease,
        Duration::from_secs(120),
    )
    .await;

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(completed.attempt, 2);
    assert_eq!(attempt_state_count(&pool, &store).await, 0);
    assert_eq!(lease_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_batch_count(&pool, &store).await, 2);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "both attempts are compact (batch-sourced), so neither the rescue nor the retry success writes an explicit closure row"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        2,
        "rescue of the stale compact attempt and the retry success each close their compact claim through the batch ledger"
    );

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_claim_gap_does_not_skip_uncommitted_enqueue_sequence() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_sequence_gap_uncommitted_enqueue";
    let schema = "awa_qs_sequence_gap_uncommitted_enqueue";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let _job_id = enqueue_job(
        &pool,
        &store,
        &HeartbeatRescueJob { id: 7 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("initial claim should succeed");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].claim.lane_seq, 1);

    let claim_cursor = || async {
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT {schema}.sequence_next_value(seq_name)
             FROM {schema}.queue_claim_heads
             WHERE queue = $1 AND priority = $2 AND enqueue_shard = $3"
        ))
        .bind(queue)
        .bind(2_i16)
        .bind(0_i16)
        .fetch_one(&pool)
        .await
        .expect("claim cursor")
    };
    assert_eq!(claim_cursor().await, 2);

    let mut tx = pool.begin().await.expect("begin enqueue reservation");
    let reserved: i64 = sqlx::query_scalar(&format!(
        "SELECT {schema}.reserve_enqueue_seq($1, $2, $3, $4)"
    ))
    .bind(queue)
    .bind(2_i16)
    .bind(0_i16)
    .bind(1_i64)
    .fetch_one(tx.as_mut())
    .await
    .expect("reserve enqueue sequence");
    assert_eq!(reserved, 2);

    let missed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim against uncommitted enqueue reservation should not fail");
    assert!(
        missed.is_empty(),
        "uncommitted enqueue reservation must not be claimable"
    );
    assert_eq!(
        claim_cursor().await,
        2,
        "claim cursor must not advance to an enqueue sequence reservation whose ready row is not committed"
    );

    tx.rollback().await.expect("rollback reservation holder");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_enqueue_reservation_orders_ready_visibility() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_enqueue_reservation_orders_visibility";
    let schema = "awa_qs_enqueue_reservation_orders_visibility";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let _job_id = enqueue_job(
        &pool,
        &store,
        &HeartbeatRescueJob { id: 70 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("initial claim should succeed");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].claim.lane_seq, 1);

    let claim_cursor = || async {
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT {schema}.sequence_next_value(seq_name)
             FROM {schema}.queue_claim_heads
             WHERE queue = $1 AND priority = $2 AND enqueue_shard = $3"
        ))
        .bind(queue)
        .bind(2_i16)
        .bind(0_i16)
        .fetch_one(&pool)
        .await
        .expect("claim cursor")
    };
    assert_eq!(claim_cursor().await, 2);

    let mut reservation_tx = pool.begin().await.expect("begin enqueue reservation");
    let reserved: i64 = sqlx::query_scalar(&format!(
        "SELECT {schema}.reserve_enqueue_seq($1, $2, $3, $4)"
    ))
    .bind(queue)
    .bind(2_i16)
    .bind(0_i16)
    .bind(1_i64)
    .fetch_one(reservation_tx.as_mut())
    .await
    .expect("reserve enqueue sequence");
    assert_eq!(reserved, 2);

    let later_started = Arc::new(Notify::new());
    let later_started_task = Arc::clone(&later_started);
    let later_pool = pool.clone();
    let later_schema = schema.to_string();
    let later_queue = queue.to_string();
    let later = tokio::spawn(async move {
        let mut tx = later_pool.begin().await.expect("begin later enqueue");
        later_started_task.notify_one();
        let lane_seq: i64 = sqlx::query_scalar(&format!(
            "SELECT {later_schema}.reserve_enqueue_seq($1, $2, $3, $4)"
        ))
        .bind(&later_queue)
        .bind(2_i16)
        .bind(0_i16)
        .bind(1_i64)
        .fetch_one(tx.as_mut())
        .await
        .expect("reserve later enqueue sequence");

        let job_id: i64 = sqlx::query_scalar(&format!(
            "SELECT nextval('{later_schema}.job_id_seq'::regclass)::bigint"
        ))
        .fetch_one(tx.as_mut())
        .await
        .expect("allocate later job id");
        let (ready_slot, ready_generation): (i32, i64) = sqlx::query_as(&format!(
            "SELECT current_slot, generation
             FROM {later_schema}.queue_ring_state
             WHERE singleton = TRUE"
        ))
        .fetch_one(tx.as_mut())
        .await
        .expect("current queue ring");

        sqlx::query(&format!(
            r#"
            INSERT INTO {later_schema}.ready_entries (
                ready_slot, ready_generation, job_id, kind, queue, args,
                priority, attempt, run_lease, max_attempts, lane_seq,
                enqueue_shard, run_at, attempted_at, created_at,
                unique_key, unique_states, payload
            )
            VALUES (
                $1, $2, $3, 'heartbeat_rescue_job', $4, '{{"id": 71}}'::jsonb,
                2, 0, 0, 3, $5,
                0, clock_timestamp(), NULL, clock_timestamp(),
                NULL, NULL, '{{}}'::jsonb
            )
            "#
        ))
        .bind(ready_slot)
        .bind(ready_generation)
        .bind(job_id)
        .bind(&later_queue)
        .bind(lane_seq)
        .execute(tx.as_mut())
        .await
        .expect("insert later ready row");

        sqlx::query(&format!(
            r#"
            INSERT INTO {later_schema}.ready_segments (
                ready_slot, ready_generation, queue, priority, enqueue_shard,
                first_lane_seq, next_lane_seq, first_run_at
            )
            VALUES ($1, $2, $3, 2, 0, $4, $4 + 1, clock_timestamp())
            "#
        ))
        .bind(ready_slot)
        .bind(ready_generation)
        .bind(&later_queue)
        .bind(lane_seq)
        .execute(tx.as_mut())
        .await
        .expect("insert later ready segment");

        tx.commit().await.expect("commit later enqueue");
        lane_seq
    });

    later_started.notified().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !later.is_finished(),
        "a later enqueue on the same lane must wait while an earlier reservation can still commit"
    );

    let missed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim against open earlier reservation should not fail");
    assert!(
        missed.is_empty(),
        "claim must not emit a later committed row while an earlier reservation can still commit"
    );
    assert_eq!(
        claim_cursor().await,
        2,
        "claim cursor must not advance past an open earlier reservation"
    );

    reservation_tx
        .rollback()
        .await
        .expect("rollback earlier reservation");
    let later_lane = tokio::time::timeout(Duration::from_secs(5), later)
        .await
        .expect("later enqueue should unblock after rollback")
        .expect("later enqueue task should succeed");
    assert_eq!(
        later_lane, 3,
        "rolled-back sequence reservations remain gaps, so the next committed lane advances"
    );

    let claimed_after_rollback = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim after earlier reservation rollback should succeed");
    assert_eq!(claimed_after_rollback.len(), 1);
    assert_eq!(claimed_after_rollback[0].claim.lane_seq, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_claim_dedupes_when_post_commit_cursor_advance_is_lost() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_receipt_claim_lost_cursor_advance";
    let schema = "awa_qs_receipt_claim_lost_cursor_advance";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &HeartbeatRescueJob { id: 8 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let claim_cursor = || async {
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT {schema}.sequence_next_value(seq_name)
             FROM {schema}.queue_claim_heads
             WHERE queue = $1 AND priority = $2 AND enqueue_shard = $3"
        ))
        .bind(queue)
        .bind(2_i16)
        .bind(0_i16)
        .fetch_one(&pool)
        .await
        .expect("claim cursor")
    };
    let claim_seq_name: String = sqlx::query_scalar(&format!(
        "SELECT seq_name
         FROM {schema}.queue_claim_heads
         WHERE queue = $1 AND priority = $2 AND enqueue_shard = $3"
    ))
    .bind(queue)
    .bind(2_i16)
    .bind(0_i16)
    .fetch_one(&pool)
    .await
    .expect("claim sequence name");

    let first: Vec<RawReceiptClaimRow> = sqlx::query_as(&format!(
        "SELECT ready_slot, ready_generation, job_id, priority, attempt, run_lease, lane_seq, claim_slot
         FROM {schema}.claim_ready_runtime($1, $2, $3, $4)"
    ))
    .bind(queue)
    .bind(1_i64)
    .bind(0.0_f64)
    .bind(0.0_f64)
    .fetch_all(&pool)
    .await
    .expect("first raw receipt claim");

    assert_eq!(
        first,
        vec![RawReceiptClaimRow {
            ready_slot: 0,
            ready_generation: 0,
            job_id,
            priority: 2,
            attempt: 1,
            run_lease: 1,
            lane_seq: 1,
            claim_slot: 0,
        }]
    );
    assert_eq!(
        claim_cursor().await,
        1,
        "raw claim intentionally leaves the post-commit claim cursor advance unsent"
    );

    let claim_attempt_batches: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(sum(claimed_count), 0)::bigint
         FROM {schema}.ready_claim_attempt_batches
         WHERE ready_slot = $1
           AND ready_generation = $2
           AND queue = $3
           AND priority = $4
           AND enqueue_shard = $5
           AND claim_slot = $6
           AND first_lane_seq <= $7
           AND next_lane_seq > $7
           AND lane_ranges @> int8range($7, $7 + 1, '[)')"
    ))
    .bind(first[0].ready_slot)
    .bind(first[0].ready_generation)
    .bind(queue)
    .bind(2_i16)
    .bind(0_i16)
    .bind(first[0].claim_slot)
    .bind(first[0].lane_seq)
    .fetch_one(&pool)
    .await
    .expect("count ready claim-attempt batch evidence");
    assert_eq!(
        claim_attempt_batches, 1,
        "claim must durably write queue-slot-local attempt batch evidence"
    );

    let mut locked_claim_child = pool.begin().await.expect("begin claim child lock");
    let claim_child = format!("{schema}.lease_claims_{}", first[0].claim_slot);
    let claim_batch_child = format!("{schema}.lease_claim_batches_{}", first[0].claim_slot);
    sqlx::query(&format!(
        "LOCK TABLE {claim_child}, {claim_batch_child} IN ACCESS EXCLUSIVE MODE"
    ))
    .execute(locked_claim_child.as_mut())
    .await
    .expect("lock old claim child");

    let recovered_while_claim_child_locked = tokio::time::timeout(Duration::from_secs(2), async {
        sqlx::query_as::<_, (i64, i64, i64, i32)>(&format!(
            "SELECT job_id, run_lease, lane_seq, claim_slot
                 FROM {schema}.claim_ready_runtime($1, $2, $3, $4)"
        ))
        .bind(queue)
        .bind(1_i64)
        .bind(0.0_f64)
        .bind(0.0_f64)
        .fetch_all(&pool)
        .await
    })
    .await
    .expect("stale cursor recovery must not block on old claim child")
    .expect("stale cursor recovery while old claim child is locked");
    assert!(
        recovered_while_claim_child_locked.is_empty(),
        "queue-slot-local attempt evidence must prevent duplicate claim without reading the claim child"
    );
    assert_eq!(
        claim_cursor().await,
        2,
        "queue-slot-local attempt evidence should advance the stale claim cursor"
    );
    locked_claim_child
        .rollback()
        .await
        .expect("release claim child lock");
    sqlx::query("SELECT setval(format('%I.%I', $1, $2)::regclass, $3, $4)")
        .bind(schema)
        .bind(&claim_seq_name)
        .bind(1_i64)
        .bind(false)
        .execute(&pool)
        .await
        .expect("reset claim cursor after locked-child recovery phase");

    match store.rotate_claims(&pool).await.expect("rotate claims") {
        RotateOutcome::Rotated { slot, .. } => assert_eq!(slot, 1),
        other => panic!("expected claim ring to rotate to slot 1, got {other:?}"),
    }

    let second: Vec<(i64, i64, i64, i32)> = sqlx::query_as(&format!(
        "SELECT job_id, run_lease, lane_seq, claim_slot
         FROM {schema}.claim_ready_runtime($1, $2, $3, $4)"
    ))
    .bind(queue)
    .bind(1_i64)
    .bind(0.0_f64)
    .bind(0.0_f64)
    .fetch_all(&pool)
    .await
    .expect("second raw receipt claim with stale cursor");

    assert!(
        second.is_empty(),
        "stale claim cursor plus claim-ring rotation must not emit a second open receipt"
    );
    assert_eq!(
        claim_cursor().await,
        2,
        "spent receipt evidence should advance the stale claim cursor over the emitted attempt"
    );

    let receipt_rows: i64 = sqlx::query_scalar(&format!(
        r#"
        WITH claim_items AS (
            SELECT job_id, run_lease
            FROM {schema}.lease_claims
            WHERE job_id = $1 AND run_lease = $2
            UNION ALL
            SELECT items.job_id, items.run_lease
            FROM {schema}.lease_claim_batches AS claim_batches
            CROSS JOIN LATERAL unnest(
                claim_batches.job_ids,
                claim_batches.run_leases
            ) AS items(job_id, run_lease)
            WHERE items.job_id = $1 AND items.run_lease = $2
        )
        SELECT count(*)::bigint FROM claim_items
        "#
    ))
    .bind(job_id)
    .bind(1_i64)
    .fetch_one(&pool)
    .await
    .expect("count receipt rows");
    assert_eq!(
        receipt_rows, 1,
        "there must be only one logical receipt claim for the attempt across claim partitions"
    );

    assert_eq!(
        open_receipt_claim_count(&pool, &store).await,
        1,
        "the original receipt remains open for completion or rescue"
    );

    sqlx::query("SELECT setval(format('%I.%I', $1, $2)::regclass, $3, $4)")
        .bind(schema)
        .bind(&claim_seq_name)
        .bind(1_i64)
        .bind(false)
        .execute(&pool)
        .await
        .expect("reset claim cursor for closed-receipt phase");

    sqlx::query(&format!(
        "INSERT INTO {schema}.lease_claim_closures (claim_slot, job_id, run_lease, outcome)
         VALUES ($1, $2, $3, 'completed')"
    ))
    .bind(0_i32)
    .bind(job_id)
    .bind(1_i64)
    .execute(&pool)
    .await
    .expect("close first receipt");

    let after_closure: Vec<(i64, i64, i64, i32)> = sqlx::query_as(&format!(
        "SELECT job_id, run_lease, lane_seq, claim_slot
         FROM {schema}.claim_ready_runtime($1, $2, $3, $4)"
    ))
    .bind(queue)
    .bind(1_i64)
    .bind(0.0_f64)
    .bind(0.0_f64)
    .fetch_all(&pool)
    .await
    .expect("raw receipt claim after closure with stale cursor");

    assert!(
        after_closure.is_empty(),
        "a closed receipt still marks the attempt as spent while the claim cursor is stale"
    );
    assert_eq!(
        claim_cursor().await,
        2,
        "closed receipt evidence should also advance the stale claim cursor"
    );

    let compact_job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 9 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    let compact_claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim compact completion candidate");
    assert_eq!(compact_claimed.len(), 1);
    assert_eq!(compact_claimed[0].job.id, compact_job_id);
    assert!(
        compact_claimed[0].claim.lease_claim_receipt,
        "test setup must use receipt claim fast path"
    );
    let compact_claim_slot = compact_claimed[0].claim.claim_slot;
    let compact_lane_seq = compact_claimed[0].claim.lane_seq;
    let compact_run_lease = compact_claimed[0].job.run_lease;
    store
        .complete_runtime_batch(&pool, &compact_claimed)
        .await
        .expect("complete through compact receipt batch");

    let compact_terminal_batches: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint
         FROM {schema}.receipt_completion_batches
         WHERE job_ids @> ARRAY[$1]::bigint[]"
    ))
    .bind(compact_job_id)
    .fetch_one(&pool)
    .await
    .expect("count compact terminal batches");
    assert_eq!(
        compact_terminal_batches, 1,
        "regression must exercise compact receipt terminal history"
    );

    sqlx::query("SELECT setval(format('%I.%I', $1, $2)::regclass, $3, $4)")
        .bind(schema)
        .bind(&claim_seq_name)
        .bind(compact_lane_seq)
        .bind(false)
        .execute(&pool)
        .await
        .expect("reset claim cursor for compact terminal-evidence phase");

    sqlx::query(&format!(
        "DELETE FROM {schema}.lease_claim_closure_batches
         WHERE claim_slot = $1"
    ))
    .bind(compact_claim_slot)
    .execute(&pool)
    .await
    .expect("remove compact claim-ring closure evidence");
    sqlx::query(&format!(
        "DELETE FROM {schema}.lease_claim_closures WHERE job_id = $1 AND run_lease = $2"
    ))
    .bind(compact_job_id)
    .bind(compact_run_lease)
    .execute(&pool)
    .await
    .expect("remove explicit closure evidence");
    sqlx::query(&format!(
        "DELETE FROM {schema}.lease_claims WHERE job_id = $1 AND run_lease = $2"
    ))
    .bind(compact_job_id)
    .bind(compact_run_lease)
    .execute(&pool)
    .await
    .expect("remove claim evidence");
    sqlx::query(&format!(
        r#"
        DELETE FROM {schema}.lease_claim_batches AS claim_batches
        WHERE EXISTS (
            SELECT 1
            FROM unnest(claim_batches.job_ids, claim_batches.run_leases) AS items(job_id, run_lease)
            WHERE items.job_id = $1 AND items.run_lease = $2
        )
        "#
    ))
    .bind(compact_job_id)
    .bind(compact_run_lease)
    .execute(&pool)
    .await
    .expect("remove compact claim evidence");

    let remaining_attempt_batches: i64 = sqlx::query_scalar(&format!(
        r#"
        SELECT count(*)::bigint
        FROM {schema}.ready_claim_attempt_batches
        WHERE ready_slot = $1
          AND ready_generation = $2
          AND queue = $3
          AND priority = $4
          AND enqueue_shard = $5
          AND claim_slot = $6
          AND first_lane_seq <= $7
          AND next_lane_seq > $7
          AND lane_ranges @> int8range($7, $7 + 1, '[)')
        "#
    ))
    .bind(compact_claimed[0].claim.ready_slot)
    .bind(compact_claimed[0].claim.ready_generation)
    .bind(queue)
    .bind(compact_claimed[0].claim.priority)
    .bind(compact_claimed[0].claim.enqueue_shard)
    .bind(compact_claim_slot)
    .bind(compact_lane_seq)
    .fetch_one(&pool)
    .await
    .expect("count queue-slot-local attempt evidence");
    assert_eq!(
        remaining_attempt_batches, 1,
        "ready-claim-attempt evidence must remain until the ready slot is pruned with ready_entries"
    );

    let after_receipt_prune: Vec<(i64, i64, i64, i32)> = sqlx::query_as(&format!(
        "SELECT job_id, run_lease, lane_seq, claim_slot
         FROM {schema}.claim_ready_runtime($1, $2, $3, $4)"
    ))
    .bind(queue)
    .bind(1_i64)
    .bind(0.0_f64)
    .bind(0.0_f64)
    .fetch_all(&pool)
    .await
    .expect("raw receipt claim after claim-ring evidence is gone");

    assert!(
        after_receipt_prune.is_empty(),
        "queue-slot-local attempt evidence must prevent re-emitting a spent attempt after claim partitions are pruned"
    );
    assert_eq!(
        claim_cursor().await,
        compact_lane_seq + 1,
        "queue-slot-local attempt evidence should advance the stale claim cursor after claim evidence is gone"
    );
}

/// The live-head claim fast path drops the per-row `ready_claim_attempt_batches`
/// probe and relies on the head-row probe plus the durable attempt-ledger range
/// to dedup. This pins that a MULTI-row claim batch is fully deduped after a lost
/// post-commit cursor advance: the head probe catches the head and the recovery
/// path advances the stale cursor across the whole contiguous claimed range, so
/// no lane in the batch is re-emitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_multi_row_claim_dedupes_when_cursor_advance_is_lost() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_receipt_multi_row_lost_cursor_advance";
    let schema = "awa_qs_receipt_multi_row_lost_cursor_advance";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let batch_len = 3_i64;
    for id in 0..batch_len {
        enqueue_job(
            &pool,
            &store,
            &HeartbeatRescueJob { id: 20 + id },
            InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        )
        .await;
    }

    let claim_cursor = || async {
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT {schema}.sequence_next_value(seq_name)
             FROM {schema}.queue_claim_heads
             WHERE queue = $1 AND priority = $2 AND enqueue_shard = $3"
        ))
        .bind(queue)
        .bind(2_i16)
        .bind(0_i16)
        .fetch_one(&pool)
        .await
        .expect("claim cursor")
    };

    // Raw claim of the whole lane in one batch; calling the SQL function
    // directly leaves the post-commit claim cursor advance unsent, exactly
    // as a worker crash between commit and advance would.
    let first: Vec<RawReceiptClaimRow> = sqlx::query_as(&format!(
        "SELECT ready_slot, ready_generation, job_id, priority, attempt, run_lease, lane_seq, claim_slot
         FROM {schema}.claim_ready_runtime($1, $2, $3, $4)"
    ))
    .bind(queue)
    .bind(batch_len)
    .bind(0.0_f64)
    .bind(0.0_f64)
    .fetch_all(&pool)
    .await
    .expect("first multi-row receipt claim");

    let mut claimed_lanes: Vec<i64> = first.iter().map(|row| row.lane_seq).collect();
    claimed_lanes.sort_unstable();
    assert_eq!(
        claimed_lanes,
        vec![1, 2, 3],
        "the whole contiguous lane must be claimed in one batch"
    );
    assert!(
        first.iter().all(|row| row.run_lease == 1),
        "every lane in the batch is a first attempt"
    );
    assert_eq!(
        claim_cursor().await,
        1,
        "raw claim intentionally leaves the post-commit claim cursor advance unsent"
    );

    // The whole claimed range is durably recorded as attempt-ledger evidence,
    // so recovery can dedup every lane without the per-row probe.
    let attempt_batch_total: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(sum(claimed_count), 0)::bigint
         FROM {schema}.ready_claim_attempt_batches
         WHERE ready_slot = $1
           AND ready_generation = $2
           AND queue = $3
           AND priority = $4
           AND enqueue_shard = $5
           AND lane_ranges @> int8range(1, $6 + 1, '[)')"
    ))
    .bind(first[0].ready_slot)
    .bind(first[0].ready_generation)
    .bind(queue)
    .bind(2_i16)
    .bind(0_i16)
    .bind(batch_len)
    .fetch_one(&pool)
    .await
    .expect("count attempt-ledger evidence over the claimed range");
    assert_eq!(
        attempt_batch_total, batch_len,
        "the claim must durably record attempt evidence spanning the whole claimed range"
    );

    // Re-claim with the stale cursor: every lane in the batch must be deduped,
    // and the cursor must advance past the whole spent range.
    let second: Vec<RawReceiptClaimRow> = sqlx::query_as(&format!(
        "SELECT ready_slot, ready_generation, job_id, priority, attempt, run_lease, lane_seq, claim_slot
         FROM {schema}.claim_ready_runtime($1, $2, $3, $4)"
    ))
    .bind(queue)
    .bind(batch_len)
    .bind(0.0_f64)
    .bind(0.0_f64)
    .fetch_all(&pool)
    .await
    .expect("second multi-row receipt claim with stale cursor");
    assert!(
        second.is_empty(),
        "the head probe plus attempt-ledger range must dedup every lane in a multi-row batch"
    );
    assert_eq!(
        claim_cursor().await,
        batch_len + 1,
        "recovery must advance the stale claim cursor past the whole spent range"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_receipt_claims_rescue_after_grace_window() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_lease_claim_rescue";
    let schema = "awa_qs_runtime_lease_claim_rescue";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;
    let release = Arc::new(Notify::new());
    let first_attempt_started = Arc::new(AtomicBool::new(false));
    let first_attempt_start_wake = Arc::new(Notify::new());
    let first_attempt_finished = Arc::new(AtomicBool::new(false));
    let first_attempt_wake = Arc::new(Notify::new());
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
                lease_claim_receipts: true,
                claim_slot_count: 2,
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .claim_rotate_interval(Duration::from_secs(60))
        .register_worker(ReceiptRescueWorker {
            release: release.clone(),
            first_attempt_started: first_attempt_started.clone(),
            first_attempt_start_wake: first_attempt_start_wake.clone(),
            first_attempt_finished: first_attempt_finished.clone(),
            first_attempt_wake: first_attempt_wake.clone(),
        })
        .promote_interval(Duration::from_millis(25))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .heartbeat_interval(Duration::from_secs(60))
        .heartbeat_rescue_interval(Duration::from_millis(100))
        .heartbeat_staleness(Duration::from_secs(60))
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

    wait_for_bool_flag(
        &first_attempt_started,
        &first_attempt_start_wake,
        Duration::from_secs(5),
        "timed out waiting for first receipt-backed attempt to start",
    )
    .await;

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
    assert_eq!(lease_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_batch_count(&pool, &store).await, 1);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 1);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 0);

    age_receipt_claim(&pool, &store, job_id, 1, Duration::from_secs(120)).await;

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
    assert_eq!(lease_claim_count(&pool, &store).await, 0);
    assert_eq!(lease_claim_batch_count(&pool, &store).await, 2);
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "both attempts are compact (batch-sourced), so neither the rescue nor the retry success writes an explicit closure row"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        2,
        "rescue of the stale compact attempt and the retry success each close their compact claim through the batch ledger"
    );

    release.notify_waiters();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if first_attempt_finished.load(Ordering::SeqCst) {
            break;
        }

        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for rescued first attempt to return"
        );
        let remaining = deadline.saturating_duration_since(now);
        let _ = tokio::time::timeout(remaining, first_attempt_wake.notified()).await;
    }

    let current = store
        .load_job(&pool, job_id)
        .await
        .expect("Failed to load receipt rescue job after late completion")
        .expect("Expected receipt rescue job to exist");
    assert_eq!(current.state, JobState::Completed);
    assert_eq!(current.attempt, 2);
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "late stale completion must not add an explicit closure for either compact attempt"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        2,
        "late stale completion must not change the closed receipt set held in the batch ledger"
    );

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_runtime_snooze() {
    let (_db_guard, pool) = setup_pool(10).await;
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
            lease_claim_receipts: false,
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
    let (_db_guard, pool) = setup_pool(10).await;
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
                lease_claim_receipts: false,
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
        .heartbeat_staleness(Duration::from_secs(2))
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
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_admin_runtime";
    let schema = "awa_qs_admin_runtime";
    let store = create_store(&pool, schema).await;
    let gate = BlockingCompleteWorkerGate::new();

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
                lease_claim_receipts: false,
                ..Default::default()
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .register_worker(gate.worker())
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

    gate.wait_until_entered(Duration::from_secs(5)).await;
    gate.release();
    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_prune_skips_live_ready_slot_until_completion() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_prune_live_slot";
    let schema = "awa_qs_runtime_prune_live_slot";
    let store = create_store(&pool, schema).await;

    let gate = BlockingCompleteWorkerGate::new();
    let client = queue_storage_client(
        &pool,
        queue,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            lease_claim_receipts: false,
            ..Default::default()
        },
        gate.worker(),
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
        .prune_oldest(&pool, Duration::ZERO)
        .await
        .expect("Failed to prune oldest live slot");
    assert!(
        matches!(
            prune_while_running,
            PruneOutcome::SkippedActive { slot: 0, .. } | PruneOutcome::Blocked { slot: 0 }
        ),
        "prune must not truncate while lease is live: {prune_while_running:?}"
    );

    gate.wait_until_entered(Duration::from_secs(5)).await;
    gate.release();

    let completed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Completed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(completed.state, JobState::Completed);

    let prune_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let prune_after_completion = store
            .prune_oldest(&pool, Duration::ZERO)
            .await
            .expect("Failed to prune oldest completed slot");
        match prune_after_completion {
            PruneOutcome::Pruned { slot: 0, .. } => break,
            PruneOutcome::Blocked { slot: 0 } if Instant::now() < prune_deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            other => panic!("unexpected prune outcome after completion: {other:?}"),
        }
    }

    let counts_after_prune = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to sample queue counts after pruning completed slot");
    assert_eq!(counts_after_prune.available, 0);
    assert_eq!(counts_after_prune.running, 0);
    assert_eq!(counts_after_prune.terminal, 1);
    assert_eq!(
        ready_segment_count(&pool, &store).await,
        0,
        "queue prune should reclaim ready segment metadata with the ready slot"
    );

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_storage_prune_waits_until_ready_tombstone_cursor_spent() {
    let (_db_guard, pool) = setup_pool(6).await;
    let queue = "qs_prune_tombstone";
    let schema = "awa_qs_prune_tombstone";
    let store = create_store(&pool, schema).await;

    let job_id = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 77 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    tombstone_ready_job(&pool, &store, job_id).await;
    assert_eq!(ready_tombstone_count(&pool, &store).await, 1);

    let rotated = store
        .rotate(&pool)
        .await
        .expect("Failed to rotate queue ring");
    assert!(
        matches!(rotated, RotateOutcome::Rotated { slot: 1, .. }),
        "unexpected rotate outcome: {rotated:?}"
    );

    let mut reader_tx = pool.begin().await.expect("begin ready reader tx");
    sqlx::query(&format!(
        "LOCK TABLE {schema}.ready_entries_0, {schema}.done_entries_0, {schema}.ready_tombstones_0, {schema}.receipt_completion_batches_0, {schema}.receipt_completion_tombstones_0, {schema}.queue_terminal_count_deltas_0 IN ACCESS SHARE MODE"
    ))
    .execute(reader_tx.as_mut())
    .await
    .expect("lock queue prune children in access share mode");

    let prune_before_cursor_advance = store
        .prune_oldest(&pool, Duration::ZERO)
        .await
        .expect("Failed to prune tombstoned ready slot");
    assert!(
        matches!(
            prune_before_cursor_advance,
            PruneOutcome::SkippedActive {
                slot: 0,
                reason: SkipReason::QueuePendingReady,
                count: 1
            }
        ),
        "tombstoned ready rows remain cursor evidence until the lane cursor passes them: {prune_before_cursor_advance:?}"
    );

    reader_tx
        .rollback()
        .await
        .expect("release ready reader lock");

    let claimed: Vec<RawReceiptClaimRow> = sqlx::query_as(&format!(
        "SELECT ready_slot, ready_generation, job_id, priority, attempt, run_lease, lane_seq, claim_slot
         FROM {schema}.claim_ready_runtime($1, $2, $3, $4)"
    ))
    .bind(queue)
    .bind(1_i64)
    .bind(0.0_f64)
    .bind(0.0_f64)
    .fetch_all(&pool)
    .await
    .expect("raw claim over tombstoned head");
    assert!(
        claimed.is_empty(),
        "claim allocator must not emit a tombstoned ready row"
    );

    let prune_after_cursor_advance = store
        .prune_oldest(&pool, Duration::ZERO)
        .await
        .expect("Failed to prune tombstoned ready slot after cursor advance");
    assert!(
        matches!(
            prune_after_cursor_advance,
            PruneOutcome::Pruned { slot: 0, .. }
        ),
        "once the cursor has passed the tombstone, queue prune should reclaim the slot: {prune_after_cursor_advance:?}"
    );

    assert_eq!(
        ready_tombstone_count(&pool, &store).await,
        0,
        "queue prune should truncate tombstones with the matching ready slot"
    );
    let ready_rows: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {schema}.ready_entries WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to count retained ready rows after prune");
    assert_eq!(ready_rows, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_prune_pending_ready_match_is_scoped_by_enqueue_shard() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_prune_pending_shard_scope";
    let schema = "awa_qs_prune_pending_shard_scope";
    let store = create_store(&pool, schema).await;

    sqlx::query(
        r#"
        INSERT INTO awa.queue_meta (queue, enqueue_shards)
        VALUES ($1, 2)
        ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards
        "#,
    )
    .bind(queue)
    .execute(&pool)
    .await
    .expect("Failed to seed enqueue_shards = 2");

    let _first = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 1 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;
    let _second = enqueue_job(
        &pool,
        &store,
        &CompleteJob { id: 2 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await;

    let ready_heads: Vec<(i16, i64)> = sqlx::query_as(&format!(
        "SELECT enqueue_shard, lane_seq FROM {schema}.ready_entries WHERE queue = $1 ORDER BY enqueue_shard"
    ))
    .bind(queue)
    .fetch_all(&pool)
    .await
    .expect("Failed to inspect seeded ready rows");
    assert_eq!(ready_heads.len(), 2);
    assert_ne!(
        ready_heads[0].0, ready_heads[1].0,
        "test setup needs two ready rows routed to different shards"
    );
    assert_eq!(
        ready_heads[0].1, ready_heads[1].1,
        "test setup needs duplicate lane_seq values across shards"
    );

    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(300))
        .await
        .expect("Failed to claim one row");
    assert_eq!(claimed.len(), 1);
    let completed = store
        .complete_runtime_batch(&pool, &claimed)
        .await
        .expect("Failed to complete one row");
    assert_eq!(completed.len(), 1);

    let rotated = store
        .rotate(&pool)
        .await
        .expect("Failed to rotate queue ring");
    assert!(
        matches!(rotated, RotateOutcome::Rotated { slot: 1, .. }),
        "unexpected rotate outcome: {rotated:?}"
    );

    let prune = store
        .prune_oldest(&pool, Duration::ZERO)
        .await
        .expect("Failed to prune oldest queue slot");
    assert!(
        matches!(
            prune,
            PruneOutcome::SkippedActive {
                slot: 0,
                reason: SkipReason::QueuePendingReady,
                count: 1
            }
        ),
        "prune must not let a done row from one shard satisfy a pending ready row from another shard: {prune:?}"
    );

    let counts = store
        .queue_counts(&pool, queue)
        .await
        .expect("Failed to sample queue counts");
    assert_eq!(counts.available, 1);
    assert_eq!(counts.running, 0);
    assert_eq!(counts.terminal, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_queue_counts_reads_legacy_lane_rollups_and_backfills_them() {
    let (_db_guard, pool) = setup_pool(10).await;
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
            pruned_completed_count
        )
        VALUES ($1, 1, 1, 1, 7)
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
    assert_eq!(counts_before_backfill.terminal, 7);

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
    assert_eq!(counts_after_backfill.terminal, 7);
}

/// Pin that prepare_schema removes the legacy queue_count_snapshots
/// table. Older runtimes carried this snapshot table to cache an exact
/// queue_counts result; the dispatcher now derives the available count
/// directly from the head tables, so the snapshot is no longer
/// populated. prepare_schema drops it to reclaim the storage and to
/// remove the misleading shape from psql inspections.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_prepare_schema_drops_legacy_count_snapshots_table() {
    let (_db_guard, pool) = setup_pool(10).await;
    let schema = "awa_qs_legacy_snapshot_drop";
    let _store = create_store(&pool, schema).await;

    let table_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = 'queue_count_snapshots'
         )",
    )
    .bind(schema)
    .fetch_one(&pool)
    .await
    .expect("Failed to probe queue_count_snapshots existence");

    assert!(
        !table_exists,
        "prepare_schema should drop the legacy queue_count_snapshots table — \
         the dispatcher derives the available count from the head tables now"
    );
}

/// Drift-detection guard for the head-table-derived available count.
///
/// `queue_counts_exact` (admin API) scans `ready_entries` with
/// `lane_seq >= claim_cursor` and no matching ready tombstone; the dispatcher
/// hot path reads the cheaper `sum(enqueue_cursor - claim_cursor)` from the
/// two head tables. The two are only equivalent when every lifecycle path
/// that adds or removes a live ready row keeps the head tables honest:
///
///   * enqueue → bumps the lane's enqueue sequence
///   * claim   → bumps the lane's claim sequence past the lane_seq
///   * Rust cancel of an unclaimed head-lane → bumps the claim sequence after
///     commit
///   * SQL-compat deletes and non-head deletes → may leave a hot-path
///     over-count until later committed rows on that lane are claimed
///
/// A missed bump would persistently over-count and burn dispatcher
/// claim attempts on phantom availability. This test pins the
/// invariants at every steady state across the lifecycle paths the
/// production runtime exercises: enqueue, claim (with priority aging),
/// cancel of an available row, and the canonical-side
/// `awa.insert_job_compat` / `awa.delete_job_compat` paths.
///
/// At every checkpoint:
///   - `store.queue_counts(...).available` (public API) ==
///     `count(*) FROM ready_entries WHERE lane_seq >= claim_cursor`
///     anti-joined with `ready_tombstones`
///   - `sum(enqueue_cursor - claim_cursor)` (hot-path approximation)
///     `>= scan` (never under-counts)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_available_count_matches_ready_entries_scan() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_avail_count_drift";
    let schema = "awa_qs_avail_count_drift";
    let store = create_store(&pool, schema).await;

    async fn assert_all_three_agree(
        pool: &sqlx::PgPool,
        store: &QueueStorage,
        queue: &str,
        checkpoint: &str,
    ) {
        let schema = store.schema();
        // Ground-truth scan — same available-row predicate as the exact API.
        let scan: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*)::bigint
             FROM {schema}.ready_entries AS ready
             JOIN {schema}.queue_claim_heads AS claims
               ON claims.queue = ready.queue
              AND claims.priority = ready.priority
              AND claims.enqueue_shard = ready.enqueue_shard
             WHERE ready.queue = $1
               AND ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
               AND NOT EXISTS (
                   SELECT 1
                   FROM {schema}.ready_tombstones AS tomb
                   WHERE tomb.ready_slot = ready.ready_slot
                     AND tomb.ready_generation = ready.ready_generation
                     AND tomb.queue = ready.queue
                     AND tomb.priority = ready.priority
                     AND tomb.enqueue_shard = ready.enqueue_shard
                     AND tomb.lane_seq = ready.lane_seq
               )"
        ))
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("Failed to run legacy ready_entries scan");

        // There are two available-count formulations and they only
        // agree when no admin DELETE has punched a gap between
        // claim_cursor and enqueue_cursor:
        //
        // - Hot-path signal (queue_claimer_signal): cheap derived
        //   `sum(enqueue_cursor - claim_cursor)`. Two PK reads per lane.
        //   Tolerates transient over-counts after admin DELETEs of
        //   non-head lanes and in-flight sequence reservations.
        // - Admin API (queue_counts): exact, via a scan over
        //   ready_entries with `lane_seq >= claim_cursor`. Same
        //   predicate as the ground-truth scan below.
        //
        // The test pins API == scan (the admin contract) and only
        // asserts a never-undercount invariant on the hot-path
        // approximation, which is allowed to drift up by the number
        // of mid-ring deletes since the last claim on that lane.
        let derived_approx: i64 = sqlx::query_scalar(&format!(
            "SELECT COALESCE(
                sum(GREATEST(
                    {schema}.sequence_next_value(qe.seq_name)
                        - {schema}.sequence_next_value(qc.seq_name),
                    0
                )),
                0
            )::bigint
             FROM {schema}.queue_enqueue_heads AS qe
             JOIN {schema}.queue_claim_heads AS qc
               ON qc.queue = qe.queue
              AND qc.priority = qe.priority
              AND qc.enqueue_shard = qe.enqueue_shard
             WHERE qe.queue = $1"
        ))
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("Failed to read derived available count");
        assert!(
            derived_approx >= scan,
            "[{checkpoint}] derived hot-path count must never under-count vs scan: scan={scan} derived={derived_approx}"
        );

        let api = store
            .queue_counts(pool, queue)
            .await
            .expect("Failed to call queue_counts")
            .available;

        assert_eq!(
            scan, api,
            "[{checkpoint}] queue_counts API diverged from the legacy scan: scan={scan} api={api}"
        );
    }

    // ── checkpoint 1: empty ──────────────────────────────────────────
    assert_all_three_agree(&pool, &store, queue, "empty").await;

    // ── checkpoint 2: enqueue 20 ─────────────────────────────────────
    store
        .enqueue_batch(&pool, queue, 2, 20)
        .await
        .expect("Failed to enqueue priority=2 jobs");
    assert_all_three_agree(&pool, &store, queue, "after enqueue 20 @ p2").await;

    // ── checkpoint 3: claim 5 (no aging) ─────────────────────────────
    let claimed = store
        .claim_runtime_batch_with_aging_for_instance(
            &pool,
            queue,
            5,
            Duration::ZERO,
            Duration::ZERO,
            Uuid::new_v4(),
            4,
            Duration::from_secs(3),
            Duration::from_millis(500),
        )
        .await
        .expect("Failed to claim 5 jobs without aging");
    assert_eq!(claimed.len(), 5);
    assert_all_three_agree(&pool, &store, queue, "after claim 5 @ p2 no-aging").await;

    // ── checkpoint 4: enqueue another 10 at a different priority ─────
    store
        .enqueue_batch(&pool, queue, 5, 10)
        .await
        .expect("Failed to enqueue priority=5 jobs");
    assert_all_three_agree(&pool, &store, queue, "after enqueue 10 @ p5").await;

    // ── checkpoint 5: cancel an available row ────────────────────────
    // Pick a still-available job at priority 2 and cancel it.
    let candidate: i64 = sqlx::query_scalar(&format!(
        "SELECT job_id
         FROM {schema}.ready_entries AS ready
         JOIN {schema}.queue_claim_heads AS claims
           ON claims.queue = ready.queue
          AND claims.priority = ready.priority
          AND claims.enqueue_shard = ready.enqueue_shard
         WHERE ready.queue = $1
           AND ready.priority = 2
           AND ready.lane_seq >= {schema}.sequence_next_value(claims.seq_name)
         ORDER BY ready.lane_seq ASC
         LIMIT 1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to pick a candidate job for cancellation");
    let cancelled = store
        .cancel_job(&pool, candidate)
        .await
        .expect("Failed to cancel ready job");
    assert!(cancelled.is_some());
    assert_all_three_agree(&pool, &store, queue, "after cancel 1 @ p2").await;

    // ── checkpoint 6: claim 3 with priority aging on ─────────────────
    // The interval is large (10s) so aging won't actually bump anything
    // within the test's wall-clock window — the point is to take the
    // aging branch in claim_ready_runtime where v_lane_priority is set
    // from claims.priority (the row's stored lane), not the
    // effective_priority computed from elapsed run_at. The counter
    // decrement must target the original lane priority regardless of
    // aging promotion.
    let aged = store
        .claim_runtime_batch_with_aging_for_instance(
            &pool,
            queue,
            3,
            Duration::ZERO,
            Duration::from_secs(10),
            Uuid::new_v4(),
            4,
            Duration::from_secs(3),
            Duration::from_millis(500),
        )
        .await
        .expect("Failed to claim with aging on");
    assert!(!aged.is_empty(), "expected at least one aged claim");
    assert_all_three_agree(&pool, &store, queue, "after claim 3 with aging").await;

    // ── checkpoint 7: canonical-side insert_job_compat ───────────────
    // Routes through awa.insert_job_compat → queue_storage runtime
    // insert. Verifies the canonical compat insert path also
    // increments the counter (v012 SQL maintains it there).
    sqlx::query(
        "SELECT * FROM awa.insert_job_compat(
            'compat_kind', $1, '{}'::jsonb, 'available'::awa.job_state,
            2::smallint, 25::smallint, NULL::timestamptz,
            '{}'::jsonb, ARRAY[]::text[],
            NULL::bytea, NULL::text::bit(8)
        )",
    )
    .bind(queue)
    .execute(&pool)
    .await
    .expect("Failed to insert via canonical compat path");
    assert_all_three_agree(&pool, &store, queue, "after canonical insert_job_compat").await;

    // ── checkpoint 8: canonical-side delete_job_compat ───────────────
    // Same compat route in reverse. The SQL function cannot safely move the
    // non-transactional claim sequence before its caller's transaction commits,
    // so this pins the exact API count and never-undercount hot-path contract.
    let compat_id: i64 = sqlx::query_scalar(&format!(
        "SELECT job_id
         FROM {schema}.ready_entries
         WHERE queue = $1 AND kind = 'compat_kind'
         ORDER BY lane_seq DESC
         LIMIT 1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to find compat job");
    sqlx::query("SELECT awa.delete_job_compat($1)")
        .bind(compat_id)
        .execute(&pool)
        .await
        .expect("Failed to call delete_job_compat");
    assert_all_three_agree(&pool, &store, queue, "after canonical delete_job_compat").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_queue_counts_and_claims_aggregate_across_stripes() {
    let (_db_guard, pool) = setup_pool(10).await;
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
    assert_eq!(counts.terminal, 0);

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

/// `queue_counts_fast` is an index-only depth probe intended for
/// high-cadence pollers (admin dashboards, depth-target producers,
/// soak observability). It returns the same shape as `queue_counts`
/// but skips two expensive scans:
///
/// - `count(*) FROM done_entries WHERE queue=$1` — replaced by a sum
///   over `queue_terminal_rollups.pruned_completed_count`, which only
///   reflects rows that have already been rolled up.
/// - `count(*) FROM lease_claims WHERE queue=$1 AND NOT EXISTS (...)`
///   — replaced by `count(*) FROM leases WHERE queue=$1 AND state =
///   'running'`, which omits receipt-plane claims that have not
///   materialised a lease row.
///
/// Under steady state (empty leases table + nothing live in
/// done_entries) the fast variant agrees with the exact variant on
/// `available` and `running`. The exact variant's `completed` is the
/// authority; the fast variant under-counts by the live (un-rolled-up)
/// done_entries segment, which is documented and acceptable for the
/// observability use case.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_queue_counts_fast_matches_exact_on_steady_state() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_counts_fast_steady";
    let schema = "awa_qs_counts_fast_steady";
    let store = create_store(&pool, schema).await;

    // Empty queue: both variants must agree on every field.
    let empty_fast = store
        .queue_counts_fast(&pool, queue)
        .await
        .expect("queue_counts_fast on empty queue");
    let empty_exact = store
        .queue_counts(&pool, queue)
        .await
        .expect("queue_counts on empty queue");
    assert_eq!(empty_fast.available, 0);
    assert_eq!(empty_fast.running, 0);
    assert_eq!(empty_fast.terminal, 0);
    assert_eq!(empty_fast.available, empty_exact.available);
    assert_eq!(empty_fast.running, empty_exact.running);
    assert_eq!(empty_fast.terminal, empty_exact.terminal);

    // Available rows present: both variants agree.
    store
        .enqueue_batch(&pool, queue, 1, 5)
        .await
        .expect("Failed to enqueue jobs");
    let with_available_fast = store
        .queue_counts_fast(&pool, queue)
        .await
        .expect("queue_counts_fast with rows available");
    let with_available_exact = store
        .queue_counts(&pool, queue)
        .await
        .expect("queue_counts with rows available");
    assert_eq!(with_available_fast.available, 5);
    assert_eq!(
        with_available_fast.available,
        with_available_exact.available
    );
    assert_eq!(with_available_fast.running, with_available_exact.running);

    // Claim the rows (lease-materialisation path; no receipt-plane
    // divergence with `lease_claim_receipts: false` from create_store).
    let claimed = store
        .claim_batch(&pool, queue, 5)
        .await
        .expect("Failed to claim");
    assert_eq!(claimed.len(), 5);
    let running_fast = store
        .queue_counts_fast(&pool, queue)
        .await
        .expect("queue_counts_fast with rows running");
    let running_exact = store
        .queue_counts(&pool, queue)
        .await
        .expect("queue_counts with rows running");
    assert_eq!(running_fast.running, 5);
    assert_eq!(running_fast.running, running_exact.running);
    assert_eq!(running_fast.available, running_exact.available);

    // Complete the rows so they land in done_entries. At this point
    // queue_counts (exact) sees them via the live_terminal CTE; the
    // fast variant under-counts because rollups haven't absorbed the
    // segment yet. That divergence is documented and intentional.
    let completed_count = store
        .complete_batch(&pool, &claimed)
        .await
        .expect("Failed to complete");
    assert_eq!(completed_count, 5);
    let pre_prune_fast = store
        .queue_counts_fast(&pool, queue)
        .await
        .expect("queue_counts_fast pre-prune");
    let pre_prune_exact = store
        .queue_counts(&pool, queue)
        .await
        .expect("queue_counts pre-prune");
    assert_eq!(pre_prune_exact.terminal, 5);
    assert_eq!(
        pre_prune_fast.terminal, 0,
        "fast under-counts pre-prune because live done_entries aren't \
         in the rollup yet (documented behaviour)"
    );

    // Rotate + prune the now-completed slot. After prune,
    // queue_terminal_rollups.pruned_completed_count absorbs the
    // live segment and the fast and exact variants should agree.
    match store.rotate(&pool).await.expect("rotate") {
        awa_model::queue_storage::RotateOutcome::Rotated { .. } => {}
        other => panic!("expected Rotated, got {other:?}"),
    }
    match store
        .prune_oldest(&pool, Duration::ZERO)
        .await
        .expect("prune_oldest")
    {
        awa_model::queue_storage::PruneOutcome::Pruned { .. } => {}
        other => panic!("expected Pruned, got {other:?}"),
    }
    let post_prune_fast = store
        .queue_counts_fast(&pool, queue)
        .await
        .expect("queue_counts_fast post-prune");
    let post_prune_exact = store
        .queue_counts(&pool, queue)
        .await
        .expect("queue_counts post-prune");
    assert_eq!(post_prune_exact.terminal, 5);
    assert_eq!(
        post_prune_fast.terminal, 5,
        "fast must match exact once the segment has rolled up"
    );
    assert_eq!(post_prune_fast.terminal, post_prune_exact.terminal);
}

/// ADR-026 invariant: wide terminal insert paths append positive
/// `queue_terminal_count_deltas` rows so that folded live counts plus pending
/// deltas equals the `done_entries` cardinality. Compact receipt completions
/// are counted directly from retained `receipt_completion_batches` instead.
///
/// The test exercises both insert paths:
/// - `insert_done_rows_tx` via `complete_batch` on the lease-materialisation
///   path (default `create_store` builds the store without the receipt
///   fast-complete candidate set).
/// - the fused receipt fast path's compact batch counting is exercised in
///   `test_queue_terminal_live_counts_matches_terminal_jobs_via_receipt_fast_path`
///   below.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_terminal_live_counts_matches_done_entries_via_insert_helper() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_terminal_live_counts_helper";
    let schema = "awa_qs_terminal_live_counts_helper";
    let store = create_store(&pool, schema).await;

    assert_eq!(
        terminal_counter_sum(&pool, schema, queue).await,
        done_entries_count(&pool, schema, queue).await,
        "invariant holds on empty queue"
    );

    store
        .enqueue_batch(&pool, queue, 1, 7)
        .await
        .expect("enqueue");
    let claimed = store.claim_batch(&pool, queue, 7).await.expect("claim");
    assert_eq!(claimed.len(), 7);
    let completed = store
        .complete_batch(&pool, &claimed)
        .await
        .expect("complete");
    assert_eq!(completed, 7);

    let live_sum = live_count_sum(&pool, schema, queue).await;
    let delta_sum = terminal_delta_sum(&pool, schema, queue).await;
    let terminal_sum = terminal_counter_sum(&pool, schema, queue).await;
    let done_count = done_entries_count(&pool, schema, queue).await;
    assert_eq!(done_count, 7);
    assert_eq!(
        live_sum, 0,
        "hot completion path must not update folded counters"
    );
    assert_eq!(delta_sum, 7, "hot completion path appends pending deltas");
    assert_eq!(
        terminal_sum, done_count,
        "folded counters plus deltas must equal done_entries cardinality"
    );

    // A second completion batch on the same queue must accumulate, not
    // replace.
    store
        .enqueue_batch(&pool, queue, 1, 3)
        .await
        .expect("enqueue 2");
    let claimed2 = store.claim_batch(&pool, queue, 3).await.expect("claim 2");
    store
        .complete_batch(&pool, &claimed2)
        .await
        .expect("complete 2");
    let live_sum = live_count_sum(&pool, schema, queue).await;
    let delta_sum = terminal_delta_sum(&pool, schema, queue).await;
    let terminal_sum = terminal_counter_sum(&pool, schema, queue).await;
    let done_count = done_entries_count(&pool, schema, queue).await;
    assert_eq!(done_count, 10);
    assert_eq!(
        live_sum, 0,
        "folded counter remains untouched before rollup"
    );
    assert_eq!(delta_sum, 10, "delta ledger accumulates across batches");
    assert_eq!(
        terminal_sum, done_count,
        "exact counter accumulates across batches"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_terminal_live_counts_matches_terminal_jobs_via_receipt_fast_path() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_terminal_live_counts_fast";
    let schema = "awa_qs_terminal_live_counts_fast";
    // lease_claim_receipts: true makes the receipt fast-complete path
    // available (assuming the claimed jobs are otherwise eligible —
    // see Self::receipt_fast_complete_candidate).
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    store
        .enqueue_batch(&pool, queue, 1, 5)
        .await
        .expect("enqueue");
    let claimed = store
        .claim_runtime_batch(&pool, queue, 5, std::time::Duration::from_secs(30))
        .await
        .expect("claim");

    // Guard the test against silent fast-path elision: if anything in
    // `receipt_fast_complete_candidate`'s eligibility ever changes (or
    // if `enqueue_batch` starts producing jobs with metadata/tags/
    // unique_keys that disqualify the fast path), this assertion fails
    // loudly so the counter wiring stays under test. Without it, a
    // future regression could silently fall through to
    // `complete_claimed_batch` (which routes via `insert_done_rows_tx`)
    // and the fused-CTE counter_upsert stage would go untested.
    for entry in &claimed {
        assert!(
            entry.claim.lease_claim_receipt,
            "test setup must drive the receipt fast path: \
             entry has lease_claim_receipt={}",
            entry.claim.lease_claim_receipt
        );
        assert!(
            entry.job.unique_key.is_none()
                && entry.job.tags.is_empty()
                && entry.job.errors.as_ref().is_none_or(Vec::is_empty),
            "test setup must satisfy receipt_fast_complete_candidate: \
             unique_key.is_none={}, tags.empty={}, errors.empty={}",
            entry.job.unique_key.is_none(),
            entry.job.tags.is_empty(),
            entry.job.errors.as_ref().is_none_or(Vec::is_empty),
        );
    }

    let completed = store
        .complete_runtime_batch(&pool, &claimed)
        .await
        .expect("complete");
    assert_eq!(completed.len(), 5);
    let live_sum = live_count_sum(&pool, schema, queue).await;
    let delta_sum = terminal_delta_sum(&pool, schema, queue).await;
    let terminal_sum = terminal_counter_sum(&pool, schema, queue).await;
    let done_count = done_entries_count(&pool, schema, queue).await;
    let terminal_count = completed_terminal_count(&pool, &store, queue).await;
    assert_eq!(
        done_count, 0,
        "receipt fast path must avoid wide completed done_entries rows"
    );
    assert_eq!(terminal_count, 5);
    assert_eq!(
        receipt_completion_batch_count(&pool, &store, queue).await,
        1
    );
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "compact successful completions must not write per-job closure rows"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        1,
        "compact successful completions must write one claim-closure batch"
    );
    assert_eq!(
        open_receipt_claim_count(&pool, &store).await,
        0,
        "compact receipt batch must close the claim logically"
    );
    assert_eq!(
        live_sum, 0,
        "fused receipt fast path must not update folded counters"
    );
    assert_eq!(
        delta_sum, 0,
        "fused receipt fast path must not append derived terminal-count deltas"
    );
    assert_eq!(
        terminal_delta_row_count(&pool, schema, queue).await,
        0,
        "compact receipt completion should not write count-delta rows"
    );
    assert_eq!(
        terminal_sum, 0,
        "folded counters plus deltas track done_entries only for compact receipt completions"
    );
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("exact counts")
            .terminal,
        terminal_count,
        "exact public terminal count must include retained compact receipt batches"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_compact_receipt_completion_is_idempotent_without_closure_rows() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_compact_receipt_idempotent";
    let schema = "awa_qs_compact_receipt_idempotent";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    store
        .enqueue_batch(&pool, queue, 1, 1)
        .await
        .expect("enqueue");
    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    assert!(claimed[0].claim.lease_claim_receipt);
    let receipt_id = claimed[0]
        .claim
        .receipt_id
        .expect("receipt-backed claims must carry receipt_id");

    let claimed_a = claimed.clone();
    let claimed_b = claimed.clone();
    let (first, second) = tokio::join!(
        store.complete_runtime_batch(&pool, &claimed_a),
        store.complete_runtime_batch(&pool, &claimed_b)
    );
    let first = first.expect("first complete");
    let second = second.expect("second complete");
    assert_eq!(
        first.len() + second.len(),
        1,
        "duplicate compact completions must have a single winner"
    );
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "idempotent compact success must not rely on explicit closure rows"
    );
    assert_eq!(
        lease_claim_closure_batch_count(&pool, &store).await,
        1,
        "idempotent compact success should keep one claim-closure batch"
    );
    let closure_batch_segment: (i32, i64, i32) = sqlx::query_as(&format!(
        "SELECT ready_slot, ready_generation, closed_count
         FROM {schema}.lease_claim_closure_batches
         WHERE claim_slot = $1"
    ))
    .bind(claimed[0].claim.claim_slot)
    .fetch_one(&pool)
    .await
    .expect("read compact closure batch segment metadata");
    assert_eq!(
        closure_batch_segment,
        (
            claimed[0].claim.ready_slot,
            claimed[0].claim.ready_generation,
            1
        ),
        "compact closure batches must carry ready segment metadata for prune count proofs"
    );
    let closure_receipt_ids: Vec<i64> = sqlx::query_scalar(&format!(
        "SELECT receipt_ids
         FROM {schema}.lease_claim_closure_batches
         WHERE claim_slot = $1"
    ))
    .bind(claimed[0].claim.claim_slot)
    .fetch_one(&pool)
    .await
    .expect("read compact closure receipt ids");
    assert_eq!(
        closure_receipt_ids,
        vec![receipt_id],
        "compact completion should close the exact receipt identity returned by claim"
    );
    assert_eq!(
        receipt_completion_batch_count(&pool, &store, queue).await,
        1,
        "duplicate completion must not create duplicate compact batches"
    );
    assert_eq!(completed_terminal_count(&pool, &store, queue).await, 1);
    assert_eq!(terminal_delta_sum(&pool, schema, queue).await, 0);
    assert_eq!(
        terminal_delta_row_count(&pool, schema, queue).await,
        0,
        "duplicate compact completion must not write terminal-count deltas"
    );
    assert_eq!(open_receipt_claim_count(&pool, &store).await, 0);
}

/// ADR-026: every terminal-delete path keeps the exact counter invariant tight
/// by appending negative terminal-count deltas.
/// Exercises retry-from-terminal (`retry_job_tx`), single DLQ move
/// (`move_failed_to_dlq`), bulk DLQ move (`bulk_move_failed_to_dlq`),
/// and discard (`discard_failed_by_kind`). The invariant
/// `SUM(live_terminal_count) + SUM(terminal_delta) == count(*) FROM done_entries`
/// must hold after each operation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_terminal_live_counts_decrement_on_terminal_delete_paths() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_terminal_live_counts_delete";
    let schema = "awa_qs_terminal_live_counts_delete";
    let store = create_store(&pool, schema).await;

    // Seed a known terminal population: 3 completed, 3 failed, 3 cancelled,
    // direct SQL into done_entries with matching counter rows. Going through
    // claim/complete would also work but adds noise; the writer paths are
    // already tested above. Here we want to focus the test on delete-path
    // decrements.
    seed_terminal_rows(&pool, schema, queue, "completed", 3).await;
    seed_terminal_rows(&pool, schema, queue, "failed", 3).await;
    seed_terminal_rows(&pool, schema, queue, "cancelled", 3).await;

    let live_sum = live_count_sum(&pool, schema, queue).await;
    let done_count = done_entries_count(&pool, schema, queue).await;
    assert_eq!(done_count, 9);
    assert_eq!(live_sum, done_count, "invariant after seeding terminals");

    // 1. Single DLQ move: pick one failed job and move it.
    let failed_job_id = first_failed_job_id(&pool, schema, queue).await;
    store
        .move_failed_to_dlq(&pool, failed_job_id, "manual-test")
        .await
        .expect("move_failed_to_dlq");
    assert_invariant_holds(&pool, schema, queue, "after move_failed_to_dlq").await;

    // 2. Bulk DLQ move: drain all remaining failed rows. Seeded 3
    //    `failed` rows; the single move in step 1 removed one, so 2
    //    remain. Exact assertion catches a silent no-op on the
    //    single-move path (which would make bulk pick up all 3 and
    //    still leave the invariant balanced).
    let moved = store
        .bulk_move_failed_to_dlq(&pool, None, Some(queue), "bulk-test")
        .await
        .expect("bulk_move_failed_to_dlq");
    assert_eq!(
        moved, 2,
        "expected exactly 2 failed rows to remain after step 1"
    );
    assert_invariant_holds(&pool, schema, queue, "after bulk_move_failed_to_dlq").await;

    // 3. Discard: seed fresh failed rows of a unique kind, then
    //    `discard_failed_by_kind`. We use a unique kind for this seeding
    //    to scope the discard precisely.
    seed_terminal_rows_with_kind(&pool, schema, queue, "failed", "discard_kind", 4).await;
    assert_invariant_holds(&pool, schema, queue, "after reseed before discard").await;
    let discarded = store
        .discard_failed_by_kind(&pool, "discard_kind")
        .await
        .expect("discard_failed_by_kind");
    assert_eq!(discarded, 4, "discard should remove the 4 seeded rows");
    assert_invariant_holds(&pool, schema, queue, "after discard_failed_by_kind").await;

    // 4. Retry-from-terminal: pick a seeded cancelled row and retry it.
    //    retry_job_tx deletes the terminal row and inserts a fresh ready
    //    row.
    let cancelled_job_id = first_cancelled_job_id(&pool, schema, queue).await;
    let retried = store
        .retry_job(&pool, cancelled_job_id)
        .await
        .expect("retry_job");
    assert!(retried.is_some(), "retry_job should succeed for cancelled");
    assert_invariant_holds(&pool, schema, queue, "after retry_job from terminal").await;
}

/// ADR-026: prune folds terminal rows into queue_terminal_rollups and clears
/// the slot's folded counter and pending delta rows, keeping exact counts
/// intact across rotate + prune even if the async rollup has not run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_terminal_live_counts_prune_folds_into_rollups() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_terminal_live_counts_prune";
    let schema = "awa_qs_terminal_live_counts_prune";
    let store = create_store(&pool, schema).await;

    store
        .enqueue_batch(&pool, queue, 1, 5)
        .await
        .expect("enqueue");
    let claimed = store.claim_batch(&pool, queue, 5).await.expect("claim");
    store
        .complete_batch(&pool, &claimed)
        .await
        .expect("complete");

    assert_eq!(done_entries_count(&pool, schema, queue).await, 5);
    assert_eq!(
        live_count_sum(&pool, schema, queue).await,
        0,
        "folded counter is untouched before async rollup"
    );
    assert_eq!(
        terminal_delta_sum(&pool, schema, queue).await,
        5,
        "pending deltas hold exact terminal count before rollup"
    );

    match store.rotate(&pool).await.expect("rotate") {
        awa_model::queue_storage::RotateOutcome::Rotated { .. } => {}
        other => panic!("expected Rotated, got {other:?}"),
    }
    match store
        .prune_oldest(&pool, Duration::ZERO)
        .await
        .expect("prune_oldest")
    {
        awa_model::queue_storage::PruneOutcome::Pruned { .. } => {}
        other => panic!("expected Pruned, got {other:?}"),
    }

    // After prune:
    // - done_entries for this queue is empty (partition truncated)
    // - queue_terminal_live_counts has no rows for this slot
    // - queue_terminal_count_deltas has no rows for this slot
    // - queue_terminal_rollups.pruned_completed_count absorbed the 5
    assert_eq!(done_entries_count(&pool, schema, queue).await, 0);
    assert_eq!(
        live_count_sum(&pool, schema, queue).await,
        0,
        "live counter for the pruned slot must be cleared"
    );
    assert_eq!(
        terminal_delta_sum(&pool, schema, queue).await,
        0,
        "pending deltas for the pruned slot must be truncated"
    );
    let rollup: i64 = sqlx::query_scalar::<_, i64>(&format!(
        "SELECT COALESCE(SUM(pruned_completed_count), 0)::bigint \
         FROM {schema}.queue_terminal_rollups WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("rollup sum");
    assert_eq!(rollup, 5, "rollup absorbed the pruned slot's counter");
}

/// #337: prune re-homes failed terminal rows inside the retention floor
/// into the live `done_entries` segment as self-contained wide rows
/// (still retryable, exact counts intact), while failed rows past the
/// floor fold into `queue_terminal_rollups.pruned_failed_count` and
/// non-failed terminals fold into `pruned_completed_count`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_prune_carries_failed_rows_inside_retention_floor() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_prune_failed_retention";
    let schema = "awa_qs_prune_failed_retention";
    let store = create_store(&pool, schema).await;

    // Slot 0: one completed row, one fresh failed row (inside the
    // floor), one failed row backdated past the floor.
    let mut job_ids = Vec::new();
    for id in [1, 2, 3] {
        let job_id = enqueue_job(
            &pool,
            &store,
            &CompleteJob { id },
            InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        )
        .await;
        let claimed = store
            .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
            .await
            .expect("claim retention-floor job");
        let claimed = claimed.into_iter().next().expect("missing claimed job");
        if id == 1 {
            store
                .complete_runtime_batch(&pool, std::slice::from_ref(&claimed))
                .await
                .expect("complete retention-floor job");
        } else {
            store
                .fail_terminal(&pool, job_id, claimed.job.run_lease, "boom", None)
                .await
                .expect("fail retention-floor job")
                .expect("running job should fail");
        }
        job_ids.push(job_id);
    }
    let carried_job_id = job_ids[1];
    let expired_job_id = job_ids[2];
    sqlx::query(&format!(
        "UPDATE {schema}.done_entries SET finalized_at = now() - interval '2 hours' \
         WHERE job_id = $1"
    ))
    .bind(expired_job_id)
    .execute(&pool)
    .await
    .expect("backdate expired failed row");

    // The soon-to-be-carried row starts narrow (ready-backed).
    let (args, max_attempts, _run_at, created_at, _payload) =
        done_body_columns(&pool, &store, carried_job_id).await;
    assert!(args.is_none(), "failed row should start ready-backed");
    assert!(max_attempts.is_none());
    assert!(created_at.is_none());

    match store.rotate(&pool).await.expect("rotate") {
        RotateOutcome::Rotated { slot: 1, .. } => {}
        other => panic!("expected Rotated {{ slot: 1 }}, got {other:?}"),
    }
    match store
        .prune_oldest(&pool, Duration::from_secs(3600))
        .await
        .expect("prune_oldest")
    {
        PruneOutcome::Pruned {
            slot: 0,
            carried_failed_rows: 1,
        } => {}
        other => panic!("expected Pruned {{ slot: 0, carried_failed_rows: 1 }}, got {other:?}"),
    }

    // The fresh failed row was carried to the live slot as a wide,
    // self-contained row; the completed and expired rows are gone.
    assert_eq!(done_entries_count(&pool, schema, queue).await, 1);
    let (carried_slot, carried_state): (i32, String) = sqlx::query_as(&format!(
        "SELECT ready_slot, state::text FROM {schema}.done_entries WHERE job_id = $1"
    ))
    .bind(carried_job_id)
    .fetch_one(&pool)
    .await
    .expect("carried row");
    assert_eq!(carried_slot, 1, "carried row must live in the current slot");
    assert_eq!(carried_state, "failed");
    let (args, max_attempts, run_at, created_at, _payload) =
        done_body_columns(&pool, &store, carried_job_id).await;
    assert!(
        args.is_some() && max_attempts.is_some() && run_at.is_some() && created_at.is_some(),
        "carried row must be widened — its ready backing row was truncated"
    );

    // Exact counts moved with the carried row (ADR-026 invariant), and
    // the rollup split the truncated rows by state.
    assert_invariant_holds(&pool, schema, queue, "after retention-floor prune").await;
    let (pruned_completed, pruned_failed): (i64, i64) = sqlx::query_as(&format!(
        "SELECT COALESCE(SUM(pruned_completed_count), 0)::bigint, \
                COALESCE(SUM(pruned_failed_count), 0)::bigint \
         FROM {schema}.queue_terminal_rollups WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("rollup sums");
    assert_eq!(
        pruned_completed, 1,
        "completed row folds into the completed column"
    );
    assert_eq!(
        pruned_failed, 1,
        "expired failed row folds into the failed column"
    );

    let counts = store.queue_counts(&pool, queue).await.expect("counts");
    assert_eq!(counts.available, 0);
    assert_eq!(counts.terminal, 3, "rollups (2) + carried live row (1)");
    assert_eq!(counts.pruned_failed, 1);

    // The carried row stays retryable.
    let retried = store
        .retry_job(&pool, carried_job_id)
        .await
        .expect("retry carried job");
    assert!(
        retried.is_some(),
        "carried failed row must remain retryable"
    );
    assert_invariant_holds(&pool, schema, queue, "after retrying carried row").await;
    let counts = store.queue_counts(&pool, queue).await.expect("counts");
    assert_eq!(counts.available, 1);
    assert_eq!(counts.terminal, 2, "retry removed the carried live row");
}

/// #337: `retry_failed_by_queue` reports matched-vs-retried counts and
/// surfaces the cumulative number of failed rows pruned past the
/// retention floor; `retry_failed_by_kind` reports `None` for the
/// pruned count because rollups carry no kind dimension.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_retry_failed_outcome_surfaces_pruned_rows() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_retry_failed_outcome";
    let schema = "awa_qs_retry_failed_outcome";
    let store = create_store(&pool, schema).await;

    // Two failed rows: one fresh (inside the floor, carried by prune)
    // and one backdated past the floor (pruned for good).
    let mut job_ids = Vec::new();
    for id in [1, 2] {
        let job_id = enqueue_job(
            &pool,
            &store,
            &CompleteJob { id },
            InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        )
        .await;
        let claimed = store
            .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
            .await
            .expect("claim retry-outcome job")
            .into_iter()
            .next()
            .expect("missing claimed job");
        store
            .fail_terminal(&pool, job_id, claimed.job.run_lease, "boom", None)
            .await
            .expect("fail retry-outcome job")
            .expect("running job should fail");
        job_ids.push(job_id);
    }
    let carried_job_id = job_ids[0];
    let expired_job_id = job_ids[1];
    sqlx::query(&format!(
        "UPDATE {schema}.done_entries SET finalized_at = now() - interval '2 hours' \
         WHERE job_id = $1"
    ))
    .bind(expired_job_id)
    .execute(&pool)
    .await
    .expect("backdate expired failed row");

    match store.rotate(&pool).await.expect("rotate") {
        RotateOutcome::Rotated { .. } => {}
        other => panic!("expected Rotated, got {other:?}"),
    }
    match store
        .prune_oldest(&pool, Duration::from_secs(3600))
        .await
        .expect("prune_oldest")
    {
        PruneOutcome::Pruned {
            carried_failed_rows: 1,
            ..
        } => {}
        other => panic!("expected Pruned with one carried row, got {other:?}"),
    }

    let outcome = admin::retry_failed_by_queue(&pool, queue)
        .await
        .expect("retry_failed_by_queue");
    assert_eq!(
        outcome.retried.len(),
        1,
        "only the carried failed row is still retryable"
    );
    assert_eq!(outcome.retried[0].id, carried_job_id);
    assert_eq!(
        outcome.matched, 1,
        "the expired row must not match the retry scan"
    );
    assert_eq!(
        outcome.pruned_failed_count,
        Some(1),
        "queue-scoped retries surface failed rows pruned past retention"
    );

    // Kind-scoped retries cannot scope the pruned count to the kind.
    let outcome = admin::retry_failed_by_kind(&pool, CompleteJob::kind())
        .await
        .expect("retry_failed_by_kind");
    assert!(
        outcome.retried.is_empty(),
        "the carried row was already retried above"
    );
    assert_eq!(outcome.matched, 0);
    assert_eq!(outcome.pruned_failed_count, None);
}

/// Bulk retry callers can pass duplicate ids (or receive duplicate ids
/// from defensive scans). Queue storage must attempt each job once so
/// `matched` reports unique jobs, not input cardinality.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_retry_jobs_by_ids_dedupes_duplicate_ids() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_retry_jobs_dedupe";
    let schema = "awa_qs_retry_jobs_dedupe";
    let store = create_store(&pool, schema).await;

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
    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("claim dedupe job")
        .into_iter()
        .next()
        .expect("missing claimed job");
    store
        .fail_terminal(&pool, job_id, claimed.job.run_lease, "boom", None)
        .await
        .expect("fail dedupe job")
        .expect("running job should fail");

    let (retried, matched) = store
        .retry_jobs_by_ids(&pool, &[job_id, job_id, job_id])
        .await
        .expect("retry duplicate ids");

    assert_eq!(matched, 1, "duplicate ids count as one matched job");
    assert_eq!(retried.len(), 1, "duplicate ids retry once");
    assert_eq!(retried[0].id, job_id);
    assert_eq!(done_entries_count(&pool, schema, queue).await, 0);
    let counts = store.queue_counts(&pool, queue).await.expect("counts");
    assert_eq!(counts.available, 1);
    assert_eq!(counts.terminal, 0);
}

/// #337: a mix of completed and failed terminal rows under a generous
/// retention floor. The completed rows fold into `pruned_completed_count`;
/// the failed rows (including a NARROW ready-backed one) are carried into
/// the live segment as self-contained wide rows, so `pruned_failed_count`
/// stays untouched. `admin::retry_failed_by_queue` then revives the
/// carried jobs end-to-end, proving the carried bodies survived the
/// widening; `rebuild_terminal_counters` re-aggregates from `done_entries`
/// and the ADR-026 invariant still holds against the carried rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_prune_carry_forward_survives_retry_and_rebuild() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_prune_carry_forward_rebuild";
    let schema = "awa_qs_prune_carry_forward_rebuild";
    let store = create_store(&pool, schema).await;

    // Slot 0: two completed rows and two failed rows, all inside the
    // floor. Every row enters narrow (ready-backed); the two failed rows
    // are the carry candidates.
    let mut completed_ids = Vec::new();
    let mut failed_ids = Vec::new();
    for id in [1, 2, 3, 4] {
        let job_id = enqueue_job(
            &pool,
            &store,
            &CompleteJob { id },
            InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        )
        .await;
        let claimed = store
            .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
            .await
            .expect("claim carry-forward job")
            .into_iter()
            .next()
            .expect("missing claimed job");
        if id <= 2 {
            store
                .complete_runtime_batch(&pool, std::slice::from_ref(&claimed))
                .await
                .expect("complete carry-forward job");
            completed_ids.push(job_id);
        } else {
            store
                .fail_terminal(&pool, job_id, claimed.job.run_lease, "boom", None)
                .await
                .expect("fail carry-forward job")
                .expect("running job should fail");
            failed_ids.push(job_id);
        }
    }

    // Both failed rows start NARROW (ready-backed): their wide body
    // columns are elided and only hydrate from the ready child.
    for &failed_id in &failed_ids {
        let (args, max_attempts, _run_at, created_at, _payload) =
            done_body_columns(&pool, &store, failed_id).await;
        assert!(
            args.is_none() && max_attempts.is_none() && created_at.is_none(),
            "failed row {failed_id} should start ready-backed (narrow)"
        );
    }

    match store.rotate(&pool).await.expect("rotate") {
        RotateOutcome::Rotated { slot: 1, .. } => {}
        other => panic!("expected Rotated {{ slot: 1 }}, got {other:?}"),
    }
    match store
        .prune_oldest(&pool, Duration::from_secs(3600))
        .await
        .expect("prune_oldest")
    {
        PruneOutcome::Pruned {
            slot: 0,
            carried_failed_rows: 2,
        } => {}
        other => panic!("expected Pruned {{ slot: 0, carried_failed_rows: 2 }}, got {other:?}"),
    }

    // Only the two failed rows survive in done_entries, both widened to
    // self-contained wide rows in the live slot with intact bodies.
    assert_eq!(done_entries_count(&pool, schema, queue).await, 2);
    for &failed_id in &failed_ids {
        let (carried_slot, carried_state): (i32, String) = sqlx::query_as(&format!(
            "SELECT ready_slot, state::text FROM {schema}.done_entries WHERE job_id = $1"
        ))
        .bind(failed_id)
        .fetch_one(&pool)
        .await
        .expect("carried row");
        assert_eq!(
            carried_slot, 1,
            "carried row {failed_id} must live in the live slot"
        );
        assert_eq!(carried_state, "failed");
        let (args, max_attempts, run_at, created_at, payload) =
            done_body_columns(&pool, &store, failed_id).await;
        assert!(
            args.is_some()
                && max_attempts.is_some()
                && run_at.is_some()
                && created_at.is_some()
                && payload.is_some(),
            "carried row {failed_id} must be widened — its ready backing row was truncated"
        );
    }

    // Completed rows folded into the completed column; pruned_failed_count
    // is untouched because no failed row was past the floor.
    let (pruned_completed, pruned_failed): (i64, i64) = sqlx::query_as(&format!(
        "SELECT COALESCE(SUM(pruned_completed_count), 0)::bigint, \
                COALESCE(SUM(pruned_failed_count), 0)::bigint \
         FROM {schema}.queue_terminal_rollups WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("rollup sums");
    assert_eq!(
        pruned_completed, 2,
        "both completed rows fold into the completed column"
    );
    assert_eq!(
        pruned_failed, 0,
        "no failed row was past the floor, so the failed column stays at zero"
    );

    let counts = store.queue_counts(&pool, queue).await.expect("counts");
    assert_eq!(
        counts.terminal, 4,
        "rollup completed (2) + carried live rows (2)"
    );
    assert_eq!(counts.pruned_failed, 0);

    // The carried rows' exact-count evidence moved with them: rebuilding
    // the counters from done_entries ground truth keeps the ADR-026
    // invariant intact against the carried rows (mirror of the #290
    // rebuild test, but exercised on carried terminal facts).
    let rebuilt = store
        .rebuild_terminal_counters(&pool)
        .await
        .expect("rebuild terminal counters after carry");
    assert!(
        rebuilt >= 1,
        "rebuild should re-populate counter rows for the carried slot"
    );
    assert_invariant_holds(&pool, schema, queue, "after rebuild over carried rows").await;
    assert_eq!(
        terminal_counter_sum(&pool, schema, queue).await,
        2,
        "rebuilt counter equals the two carried failed rows"
    );

    // End-to-end revival through the admin path: both carried jobs are
    // still failed-and-retryable, proving the widened bodies are
    // self-contained.
    let outcome = admin::retry_failed_by_queue(&pool, queue)
        .await
        .expect("retry_failed_by_queue");
    assert_eq!(
        outcome.matched, 2,
        "both carried failed rows match the retry scan"
    );
    assert_eq!(
        outcome.retried.len(),
        2,
        "both carried failed rows are revived"
    );
    let revived: HashSet<i64> = outcome.retried.iter().map(|job| job.id).collect();
    let expected: HashSet<i64> = failed_ids.iter().copied().collect();
    assert_eq!(
        revived, expected,
        "the revived jobs are exactly the carried ones"
    );
    for job in &outcome.retried {
        assert_eq!(
            job.state,
            JobState::Available,
            "revived job is runnable again"
        );
    }
    assert_eq!(
        outcome.pruned_failed_count,
        Some(0),
        "no rows were pruned past the floor for this queue"
    );

    // Retrying the carried rows removed them from done_entries; the
    // invariant continues to hold.
    assert_invariant_holds(&pool, schema, queue, "after reviving carried rows").await;
    let counts = store.queue_counts(&pool, queue).await.expect("counts");
    assert_eq!(counts.available, 2, "both carried jobs are runnable again");
    assert_eq!(
        counts.terminal, 2,
        "retry removed the two carried live rows"
    );
}

/// #337: failed rows past the floor are pruned for good. With
/// `Duration::ZERO` the floor is disabled, so a fresh failed row folds
/// into `pruned_failed_count` exactly like an expired one. The pruned
/// rows are no longer retryable: `retry_failed_by_queue` returns an empty
/// `retried` set with `matched == 0`, surfaces the cumulative pruned
/// count, and `queue_counts_exact` still includes the pruned rows in its
/// terminal total while `QueueCounts.pruned_failed` reports them
/// standing. `retry_failed_by_kind` cannot scope the pruned count to a
/// kind, so it reports `None`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_prune_past_floor_folds_failed_into_rollup() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_prune_past_floor_fold";
    let schema = "awa_qs_prune_past_floor_fold";
    let store = create_store(&pool, schema).await;

    // Slot 0: three failed rows, all fresh and inside any real floor.
    for id in [1, 2, 3] {
        let job_id = enqueue_job(
            &pool,
            &store,
            &CompleteJob { id },
            InsertOpts {
                queue: queue.to_string(),
                ..Default::default()
            },
        )
        .await;
        let claimed = store
            .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
            .await
            .expect("claim past-floor job")
            .into_iter()
            .next()
            .expect("missing claimed job");
        store
            .fail_terminal(&pool, job_id, claimed.job.run_lease, "boom", None)
            .await
            .expect("fail past-floor job")
            .expect("running job should fail");
    }
    assert_eq!(done_entries_count(&pool, schema, queue).await, 3);

    match store.rotate(&pool).await.expect("rotate") {
        RotateOutcome::Rotated { slot: 1, .. } => {}
        other => panic!("expected Rotated {{ slot: 1 }}, got {other:?}"),
    }
    // Duration::ZERO disables the floor: every failed row is treated as
    // past-floor and pruned for good — no carry.
    match store
        .prune_oldest(&pool, Duration::ZERO)
        .await
        .expect("prune_oldest")
    {
        PruneOutcome::Pruned {
            slot: 0,
            carried_failed_rows: 0,
        } => {}
        other => panic!("expected Pruned {{ slot: 0, carried_failed_rows: 0 }}, got {other:?}"),
    }

    // No live done rows remain; all three failed rows folded into the
    // failed rollup column.
    assert_eq!(done_entries_count(&pool, schema, queue).await, 0);
    let (pruned_completed, pruned_failed): (i64, i64) = sqlx::query_as(&format!(
        "SELECT COALESCE(SUM(pruned_completed_count), 0)::bigint, \
                COALESCE(SUM(pruned_failed_count), 0)::bigint \
         FROM {schema}.queue_terminal_rollups WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("rollup sums");
    assert_eq!(pruned_completed, 0, "no completed rows in this scenario");
    assert_eq!(
        pruned_failed, 3,
        "all three failed rows fold into the failed column"
    );

    // The pruned failed rows are no longer retryable.
    let outcome = admin::retry_failed_by_queue(&pool, queue)
        .await
        .expect("retry_failed_by_queue");
    assert!(
        outcome.retried.is_empty(),
        "pruned failed rows cannot be revived"
    );
    assert_eq!(
        outcome.matched, 0,
        "no done_entries failed rows remain to match"
    );
    assert_eq!(
        outcome.pruned_failed_count,
        Some(3),
        "queue-scoped retries surface all three failed rows pruned past retention"
    );

    // The exact terminal total still counts the pruned rows, and the
    // standing pruned_failed surface matches the rollup sum.
    let counts = store.queue_counts(&pool, queue).await.expect("counts");
    assert_eq!(
        counts.terminal, 3,
        "exact terminal total includes the pruned failed rows"
    );
    assert_eq!(
        counts.pruned_failed, 3,
        "QueueCounts.pruned_failed == N pruned failed rows"
    );

    // Kind-scoped retries cannot scope the pruned count to a kind.
    let outcome = admin::retry_failed_by_kind(&pool, CompleteJob::kind())
        .await
        .expect("retry_failed_by_kind");
    assert!(outcome.retried.is_empty());
    assert_eq!(outcome.matched, 0);
    assert_eq!(
        outcome.pruned_failed_count, None,
        "rollups carry no kind dimension, so the pruned count is unscopable"
    );
}

/// #337: a carried failed row must be carried again — exactly once — when
/// its new slot becomes prunable. Rotating the ring all the way around so
/// the slot holding the carried row is the oldest sealed slot, then
/// pruning with the floor again, re-homes the same row into the new live
/// slot without duplicating it in `done_entries` and without drifting the
/// exact counters.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_prune_re_carries_failed_row_exactly_once() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_prune_re_carry_stable";
    let schema = "awa_qs_prune_re_carry_stable";
    // Default 4-slot queue ring: slot 0 holds the failed row, it is
    // carried to slot 1 by the first prune, and a full lap (1→2→3→0)
    // makes slot 1 the oldest sealed slot for the second prune.
    let store = create_store(&pool, schema).await;

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
    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::from_secs(30))
        .await
        .expect("claim re-carry job")
        .into_iter()
        .next()
        .expect("missing claimed job");
    store
        .fail_terminal(&pool, job_id, claimed.job.run_lease, "boom", None)
        .await
        .expect("fail re-carry job")
        .expect("running job should fail");

    // First carry: rotate 0→1, prune slot 0 → row carried into slot 1.
    match store.rotate(&pool).await.expect("rotate") {
        RotateOutcome::Rotated { slot: 1, .. } => {}
        other => panic!("expected Rotated {{ slot: 1 }}, got {other:?}"),
    }
    match store
        .prune_oldest(&pool, Duration::from_secs(3600))
        .await
        .expect("prune_oldest")
    {
        PruneOutcome::Pruned {
            slot: 0,
            carried_failed_rows: 1,
        } => {}
        other => panic!("expected Pruned {{ slot: 0, carried_failed_rows: 1 }}, got {other:?}"),
    }
    assert_eq!(
        done_entries_count(&pool, schema, queue).await,
        1,
        "exactly one done row after first carry"
    );
    let first_slot: i32 = sqlx::query_scalar(&format!(
        "SELECT ready_slot FROM {schema}.done_entries WHERE job_id = $1"
    ))
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("first carried slot");
    assert_eq!(first_slot, 1, "row carried into the live slot");
    assert_invariant_holds(&pool, schema, queue, "after first carry").await;

    // Lap the ring so the carried row's slot (1) becomes the oldest
    // sealed slot again: 1→2→3→0. Slots 2/3/0 are empty, so `rotate`
    // now idle-skips (#371) instead of advancing; move the cursor
    // directly, exactly as a rotation of a busy ring would (each hop is
    // current_slot+1, generation+1). A real fleet laps the same way
    // whenever other queues' traffic keeps the ring busy. After the
    // three hops the cursor is (slot 0, generation 4), which makes
    // `oldest_initialized_ring_slot` target slot 1 for the second prune.
    sqlx::query(&format!(
        r#"
        UPDATE {schema}.queue_ring_state
        SET current_slot = 0, generation = 4
        WHERE singleton = TRUE
        "#
    ))
    .execute(&pool)
    .await
    .expect("lap queue ring cursor around to slot 0 for re-carry setup");

    // Second prune: current slot is 0, so the oldest sealed non-current
    // slot is slot 1 (the carried row). It is carried again into slot 0.
    match store
        .prune_oldest(&pool, Duration::from_secs(3600))
        .await
        .expect("prune_oldest re-carry")
    {
        PruneOutcome::Pruned {
            slot: 1,
            carried_failed_rows: 1,
        } => {}
        other => panic!("expected Pruned {{ slot: 1, carried_failed_rows: 1 }}, got {other:?}"),
    }

    // Carried exactly once: still a single done row, now in slot 0, with
    // an intact wide body and consistent counters (no duplication).
    assert_eq!(
        done_entries_count(&pool, schema, queue).await,
        1,
        "re-carry must not duplicate the done row"
    );
    let second_slot: i32 = sqlx::query_scalar(&format!(
        "SELECT ready_slot FROM {schema}.done_entries WHERE job_id = $1"
    ))
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("re-carried slot");
    assert_eq!(second_slot, 0, "row re-carried into the new live slot");
    let (args, max_attempts, run_at, created_at, payload) =
        done_body_columns(&pool, &store, job_id).await;
    assert!(
        args.is_some()
            && max_attempts.is_some()
            && run_at.is_some()
            && created_at.is_some()
            && payload.is_some(),
        "re-carried row stays a self-contained wide row"
    );
    assert_invariant_holds(&pool, schema, queue, "after re-carry").await;
    assert_eq!(
        terminal_counter_sum(&pool, schema, queue).await,
        1,
        "counters track exactly one terminal row across the re-carry"
    );

    // No failed row was ever past the floor, so nothing folded into the
    // rollup despite two prunes.
    let pruned_failed: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(SUM(pruned_failed_count), 0)::bigint \
         FROM {schema}.queue_terminal_rollups WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("rollup failed sum");
    assert_eq!(
        pruned_failed, 0,
        "the row was always carried, never pruned past the floor"
    );

    // Still retryable after two carries.
    let retried = store
        .retry_job(&pool, job_id)
        .await
        .expect("retry re-carried job");
    assert!(retried.is_some(), "re-carried row must remain retryable");
    assert_invariant_holds(&pool, schema, queue, "after retrying re-carried row").await;
}

/// ADR-026: maintenance can fold pending terminal-count deltas for a sealed
/// queue slot into `queue_terminal_live_counts` without changing the exact
/// terminal count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_terminal_count_delta_rollup_folds_sealed_slots() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_terminal_count_delta_rollup";
    let schema = "awa_qs_terminal_count_delta_rollup";
    let store = create_store(&pool, schema).await;

    store
        .enqueue_batch(&pool, queue, 1, 6)
        .await
        .expect("enqueue");
    let claimed = store.claim_batch(&pool, queue, 6).await.expect("claim");
    store
        .complete_batch(&pool, &claimed)
        .await
        .expect("complete");

    assert_eq!(done_entries_count(&pool, schema, queue).await, 6);
    assert_eq!(live_count_sum(&pool, schema, queue).await, 0);
    assert_eq!(terminal_delta_sum(&pool, schema, queue).await, 6);
    assert_eq!(terminal_counter_sum(&pool, schema, queue).await, 6);

    match store.rotate(&pool).await.expect("rotate") {
        awa_model::queue_storage::RotateOutcome::Rotated { .. } => {}
        other => panic!("expected Rotated, got {other:?}"),
    }

    let outcome = store
        .rollup_terminal_count_deltas(&pool, 4)
        .await
        .expect("rollup terminal deltas");
    assert_eq!(outcome.rolled_slots, 1);
    assert_eq!(outcome.delta_rows, 6);
    assert_eq!(outcome.grouped_keys, 6);
    assert_eq!(outcome.skipped_active_slots, 0);
    assert_eq!(outcome.blocked_slots, 0);
    assert_eq!(live_count_sum(&pool, schema, queue).await, 6);
    assert_eq!(terminal_delta_sum(&pool, schema, queue).await, 0);
    assert_eq!(terminal_counter_sum(&pool, schema, queue).await, 6);
    assert_eq!(done_entries_count(&pool, schema, queue).await, 6);

    let counts = store
        .queue_counts(&pool, queue)
        .await
        .expect("queue counts");
    assert_eq!(counts.terminal, 6);
}

/// Terminal-count rollup mutates `queue_terminal_live_counts`, so it should
/// stand down while another backend is pinning the MVCC horizon. Exact reads
/// remain correct because they include pending delta rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_terminal_count_delta_rollup_skips_pinned_mvcc_horizon() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_terminal_delta_rollup_mvcc_pin";
    let schema = "awa_qs_terminal_delta_rollup_mvcc_pin";
    let store = create_store(&pool, schema).await;

    store
        .enqueue_batch(&pool, queue, 1, 6)
        .await
        .expect("enqueue");
    let claimed = store.claim_batch(&pool, queue, 6).await.expect("claim");
    store
        .complete_batch(&pool, &claimed)
        .await
        .expect("complete");
    match store.rotate(&pool).await.expect("rotate") {
        awa_model::queue_storage::RotateOutcome::Rotated { .. } => {}
        other => panic!("expected Rotated, got {other:?}"),
    }

    let mut pinned = pool.acquire().await.expect("acquire pinned connection");
    sqlx::query("BEGIN")
        .execute(&mut *pinned)
        .await
        .expect("begin pinned transaction");
    sqlx::query("SELECT txid_current()")
        .fetch_one(&mut *pinned)
        .await
        .expect("establish pinned transaction xid");

    let outcome = store
        .rollup_terminal_count_deltas(&pool, 4)
        .await
        .expect("rollup terminal deltas under pin");
    assert_eq!(outcome.rolled_slots, 0);
    assert_eq!(outcome.delta_rows, 0);
    assert_eq!(outcome.grouped_keys, 0);
    assert_eq!(outcome.skipped_active_slots, 0);
    assert_eq!(outcome.blocked_slots, 0);
    assert!(outcome.skipped_mvcc_pinned);
    assert_eq!(live_count_sum(&pool, schema, queue).await, 0);
    assert_eq!(terminal_delta_sum(&pool, schema, queue).await, 6);
    assert_eq!(terminal_counter_sum(&pool, schema, queue).await, 6);

    let counts = store
        .queue_counts(&pool, queue)
        .await
        .expect("queue counts remain exact under pin");
    assert_eq!(counts.terminal, 6);

    sqlx::query("ROLLBACK")
        .execute(&mut *pinned)
        .await
        .expect("release pinned transaction");
    drop(pinned);

    let outcome = store
        .rollup_terminal_count_deltas(&pool, 4)
        .await
        .expect("rollup terminal deltas after pin release");
    assert_eq!(outcome.rolled_slots, 1);
    assert_eq!(outcome.delta_rows, 6);
    assert_eq!(outcome.grouped_keys, 6);
    assert!(!outcome.skipped_mvcc_pinned);
    assert_eq!(live_count_sum(&pool, schema, queue).await, 6);
    assert_eq!(terminal_delta_sum(&pool, schema, queue).await, 0);
    assert_eq!(terminal_counter_sum(&pool, schema, queue).await, 6);
}

/// ADR-026: rollup candidate selection must not let old empty sealed slots
/// hide a later sealed slot that has pending deltas. This matters when
/// operators choose more queue slots than a single maintenance tick can fold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_terminal_count_delta_rollup_skips_empty_old_slots() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_terminal_delta_rollup_late_slot";
    let schema = "awa_qs_terminal_delta_rollup_late_slot";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 32,
            lease_slot_count: 2,
            claim_slot_count: 2,
            lease_claim_receipts: false,
            ..Default::default()
        },
    )
    .await;

    // Lap the ring 21 slots forward. The ring is empty, so `rotate` now
    // idle-skips (#371) rather than advancing; replay exactly the writes
    // 21 real rotations from (slot 0, generation 0) would make — the
    // ring-state cursor lands on (slot 21, generation 21), and each slot
    // n in 1..=21 records generation n in `queue_ring_slots` (rotation
    // stamps the *incoming* slot's generation). A loaded ring laps the
    // same way; this test only needs the sealed-slot bookkeeping, not the
    // traffic that would normally drive it.
    for slot in 1..=21_i32 {
        sqlx::query(&format!(
            "UPDATE {schema}.queue_ring_slots SET generation = $2 WHERE slot = $1"
        ))
        .bind(slot)
        .bind(slot as i64)
        .execute(&pool)
        .await
        .expect("stamp lapped slot generation");
    }
    sqlx::query(&format!(
        "UPDATE {schema}.queue_ring_state SET current_slot = 21, generation = 21 WHERE singleton = TRUE"
    ))
    .execute(&pool)
    .await
    .expect("advance queue ring cursor to slot 21");

    let target_slot = 20_i32;
    let target_generation: i64 = sqlx::query_scalar(&format!(
        "SELECT generation FROM {schema}.queue_ring_slots WHERE slot = $1"
    ))
    .bind(target_slot)
    .fetch_one(&pool)
    .await
    .expect("target slot generation");
    assert!(target_generation >= 0);

    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.queue_terminal_count_deltas (
            ready_slot, ready_generation, queue, priority, enqueue_shard,
            counter_bucket, terminal_delta
        )
        VALUES ($1, $2, $3, 1, 0, 0, 7)
        "#
    ))
    .bind(target_slot)
    .bind(target_generation)
    .bind(queue)
    .execute(&pool)
    .await
    .expect("seed terminal delta in later sealed slot");

    assert_eq!(live_count_sum(&pool, schema, queue).await, 0);
    assert_eq!(terminal_delta_sum(&pool, schema, queue).await, 7);

    let outcome = store
        .rollup_terminal_count_deltas(&pool, 1)
        .await
        .expect("rollup late pending slot");
    assert_eq!(outcome.rolled_slots, 1);
    assert_eq!(outcome.delta_rows, 1);
    assert_eq!(outcome.grouped_keys, 1);
    assert_eq!(outcome.skipped_active_slots, 0);
    assert_eq!(outcome.blocked_slots, 0);
    assert_eq!(live_count_sum(&pool, schema, queue).await, 7);
    assert_eq!(terminal_delta_sum(&pool, schema, queue).await, 0);
}

/// Direct-SQL seed for terminal-state test rows. Inserts into
/// `done_entries` AND folded `queue_terminal_live_counts` so the invariant
/// holds at seed time; the test then exercises delete paths to verify they
/// append negative deltas correctly.
async fn seed_terminal_rows(pool: &sqlx::PgPool, schema: &str, queue: &str, state: &str, n: i64) {
    seed_terminal_rows_with_kind(pool, schema, queue, state, "chaos_job", n).await;
}

async fn seed_terminal_rows_with_kind(
    pool: &sqlx::PgPool,
    schema: &str,
    queue: &str,
    state: &str,
    kind: &str,
    n: i64,
) {
    // Insert N done_entries rows at (ready_slot=0, priority=2,
    // enqueue_shard=0). lane_seq stays unique per call by reading the
    // current max and adding rownums.
    let next_lane_seq: i64 = sqlx::query_scalar::<_, i64>(&format!(
        "SELECT COALESCE(max(lane_seq), 0)::bigint + 1 \
         FROM {schema}.done_entries WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("max lane_seq");
    let next_job_id: i64 = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(max(id), 1000000)::bigint + 1 FROM awa.jobs_hot",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(1_000_000);
    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.done_entries (
            ready_slot, ready_generation, job_id, kind, queue, state,
            priority, attempt, run_lease, lane_seq, enqueue_shard,
            attempted_at, finalized_at, payload
        )
        SELECT
            0,
            1,
            $1::bigint + g - 1,
            $2,
            $3,
            $4::awa.job_state,
            2::smallint,
            1::smallint,
            1::bigint,
            $5::bigint + g - 1,
            0::smallint,
            now(),
            now(),
            '{{}}'::jsonb
        FROM generate_series(1, $6::int) AS g
        "#
    ))
    .bind(next_job_id)
    .bind(kind)
    .bind(queue)
    .bind(state)
    .bind(next_lane_seq)
    .bind(n as i32)
    .execute(pool)
    .await
    .expect("seed done_entries");

    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.queue_terminal_live_counts AS counts (
            ready_slot, queue, priority, enqueue_shard, counter_bucket, live_terminal_count
        )
        SELECT
            0,
            $1,
            2::smallint,
            0::smallint,
            bucket.counter_bucket,
            count(*)::bigint
        FROM (
            SELECT mod(mod($2::bigint + g - 1, 256::bigint) + 256::bigint, 256::bigint)::smallint AS counter_bucket
            FROM generate_series(1, $3::int) AS g
        ) AS bucket
        GROUP BY bucket.counter_bucket
        ON CONFLICT (ready_slot, queue, priority, enqueue_shard, counter_bucket) DO UPDATE
        SET live_terminal_count = counts.live_terminal_count + EXCLUDED.live_terminal_count
        "#
    ))
    .bind(queue)
    .bind(next_job_id)
    .bind(n as i32)
    .execute(pool)
    .await
    .expect("seed live counter");
}

async fn first_failed_job_id(pool: &sqlx::PgPool, schema: &str, queue: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT job_id FROM {schema}.done_entries \
         WHERE queue = $1 AND state = 'failed' LIMIT 1"
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("first failed job id")
}

async fn first_cancelled_job_id(pool: &sqlx::PgPool, schema: &str, queue: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT job_id FROM {schema}.done_entries \
         WHERE queue = $1 AND state = 'cancelled' LIMIT 1"
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("first cancelled job id")
}

async fn assert_invariant_holds(pool: &sqlx::PgPool, schema: &str, queue: &str, label: &str) {
    let terminal = terminal_counter_sum(pool, schema, queue).await;
    let done = done_entries_count(pool, schema, queue).await;
    assert_eq!(
        terminal, done,
        "ADR-026 invariant ({label}): folded live counts plus pending deltas ({terminal}) \
         must equal count(*) FROM done_entries ({done})"
    );
}

/// ADR-026 — SQL compat `DELETE FROM awa.jobs WHERE id = $1` routes to
/// `awa.delete_job_compat()`. The done_entries branch appends a negative
/// terminal-count delta, so deleting a terminal row via the SQL compat path
/// keeps exact counts aligned with the underlying table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_terminal_live_counts_decrement_on_sql_compat_delete() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_terminal_live_counts_sql_compat";
    let schema = "awa_qs_terminal_live_counts_sql_compat";
    // create_store activates this schema via the storage transition
    // dance (prepare → enter_mixed_transition → finalize), so
    // `awa.active_queue_storage_schema()` returns `schema` and
    // `awa.delete_job_compat()` routes to it.
    let _store = create_store(&pool, schema).await;

    seed_terminal_rows(&pool, schema, queue, "completed", 5).await;
    assert_eq!(done_entries_count(&pool, schema, queue).await, 5);
    assert_eq!(live_count_sum(&pool, schema, queue).await, 5);

    let target_id: i64 = sqlx::query_scalar::<_, i64>(&format!(
        "SELECT job_id FROM {schema}.done_entries WHERE queue = $1 LIMIT 1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("first done job id");

    let deleted: bool = sqlx::query_scalar("SELECT awa.delete_job_compat($1)")
        .bind(target_id)
        .fetch_one(&pool)
        .await
        .expect("delete_job_compat call");
    assert!(
        deleted,
        "delete_job_compat should return true for an existing terminal row"
    );

    assert_eq!(done_entries_count(&pool, schema, queue).await, 4);
    assert_invariant_holds(&pool, schema, queue, "after SQL compat delete").await;
}

/// #290 — `rebuild_terminal_counters` truncates and re-aggregates from
/// `done_entries`, restoring the invariant after any drift.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_terminal_live_counts_rebuild_restores_invariant() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_terminal_live_counts_rebuild";
    let schema = "awa_qs_terminal_live_counts_rebuild";
    let store = create_store(&pool, schema).await;

    // Seed 7 terminal rows with matching counter entries, then manually
    // poison the counter to simulate rollover drift.
    seed_terminal_rows(&pool, schema, queue, "completed", 7).await;
    sqlx::query(&format!(
        "UPDATE {schema}.queue_terminal_live_counts SET live_terminal_count = 999 WHERE queue = $1"
    ))
    .bind(queue)
    .execute(&pool)
    .await
    .expect("poison counter");

    // Sanity-check the drift is present. With bucketed counters this
    // poisons every bucket row for the queue, so the exact poisoned sum
    // depends on how many buckets the seeded job_ids touched.
    assert_ne!(live_count_sum(&pool, schema, queue).await, 7);
    assert_eq!(done_entries_count(&pool, schema, queue).await, 7);

    let rebuilt = store
        .rebuild_terminal_counters(&pool)
        .await
        .expect("rebuild");
    assert!(rebuilt >= 1, "rebuild should re-populate counter rows");
    assert_invariant_holds(&pool, schema, queue, "after rebuild").await;
}

/// #290 — `queue_counts_exact` honours the
/// `queue_ring_state.terminal_counter_trusted_at` marker. When the marker
/// is NULL (e.g. a rolling-upgrade window where a pre-#290 binary may
/// have written terminal rows without maintaining the counter), the
/// read path falls back to scanning `terminal_jobs`; otherwise it uses
/// the counter directly. This keeps the "exact" naming honest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_terminal_counter_trust_marker_gates_read_path() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_terminal_counter_trust";
    let schema = "awa_qs_terminal_counter_trust";
    let store = create_store(&pool, schema).await;

    // Fresh installs auto-mark trusted (empty done_entries at
    // prepare_schema time).
    assert!(
        store
            .terminal_counter_trusted(&pool)
            .await
            .expect("trust check"),
        "fresh install should auto-mark the trust marker"
    );

    // Seed 5 terminal rows. The counter and done_entries agree.
    seed_terminal_rows(&pool, schema, queue, "completed", 5).await;
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("counts")
            .terminal,
        5,
        "trusted path returns counter sum"
    );

    // Simulate a pre-#290 binary's drift by:
    //  1. Inserting an "orphan" done_entries row without a matching
    //     counter increment.
    //  2. Manually clearing the trust marker (operator hasn't yet run
    //     `awa storage rebuild-terminal-counters`).
    sqlx::query(&format!(
        "INSERT INTO {schema}.done_entries (
            ready_slot, ready_generation, job_id, kind, queue, state,
            priority, attempt, run_lease, lane_seq, enqueue_shard,
            attempted_at, finalized_at, payload
        ) VALUES (0, 1, 7000000, 'chaos_job', $1, 'completed'::awa.job_state,
                  2::smallint, 1::smallint, 1::bigint, 9999::bigint,
                  0::smallint, now(), now(), '{{}}'::jsonb)"
    ))
    .bind(queue)
    .execute(&pool)
    .await
    .expect("seed orphan done row");
    sqlx::query(&format!(
        "UPDATE {schema}.queue_ring_state \
         SET terminal_counter_trusted_at = NULL WHERE singleton = TRUE"
    ))
    .execute(&pool)
    .await
    .expect("clear trust marker");

    assert!(
        !store
            .terminal_counter_trusted(&pool)
            .await
            .expect("trust check"),
        "trust marker cleared"
    );

    // Untrusted path scans terminal_jobs → reports 6 (the seeded 5 +
    // the orphan row). If the read still trusted the counter, it
    // would return 5 and silently mask the drift.
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("counts")
            .terminal,
        6,
        "untrusted path scans terminal_jobs instead of the counter"
    );

    // Rebuild restores the invariant AND flips the trust marker back.
    store
        .rebuild_terminal_counters(&pool)
        .await
        .expect("rebuild");
    assert!(
        store
            .terminal_counter_trusted(&pool)
            .await
            .expect("trust check"),
        "rebuild should set the trust marker"
    );
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("counts")
            .terminal,
        6,
        "trusted path post-rebuild returns the rebuilt counter sum"
    );
    assert_invariant_holds(&pool, schema, queue, "after rebuild trust flip").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_terminal_counts_include_compact_receipt_batches() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_terminal_counter_compact_receipt";
    let schema = "awa_qs_terminal_counter_compact_receipt";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            lease_claim_receipts: true,
            ..Default::default()
        },
    )
    .await;

    store
        .enqueue_batch(&pool, queue, 1, 3)
        .await
        .expect("enqueue compact receipt jobs");
    let claimed = store
        .claim_runtime_batch(&pool, queue, 3, Duration::ZERO)
        .await
        .expect("claim compact receipt jobs");
    assert_eq!(claimed.len(), 3);
    store
        .complete_runtime_batch(&pool, &claimed)
        .await
        .expect("complete compact receipt jobs");

    assert_eq!(done_entries_count(&pool, schema, queue).await, 0);
    assert_eq!(completed_terminal_count(&pool, &store, queue).await, 3);
    assert_eq!(
        terminal_counter_sum(&pool, schema, queue).await,
        0,
        "compact receipt completions do not write done-entry terminal counters"
    );
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("trusted compact counts")
            .terminal,
        3,
        "trusted exact counts include retained compact receipt batches directly"
    );

    sqlx::query(&format!(
        "UPDATE {schema}.queue_ring_state \
         SET terminal_counter_trusted_at = NULL WHERE singleton = TRUE"
    ))
    .execute(&pool)
    .await
    .expect("clear trust marker");

    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("untrusted compact counts")
            .terminal,
        3,
        "untrusted path must count compact receipt completions via terminal_jobs"
    );

    sqlx::query(&format!(
        "TRUNCATE TABLE {schema}.queue_terminal_live_counts, {schema}.queue_terminal_count_deltas"
    ))
    .execute(&pool)
    .await
    .expect("clear counters before rebuild");
    assert_eq!(terminal_counter_sum(&pool, schema, queue).await, 0);

    store
        .rebuild_terminal_counters(&pool)
        .await
        .expect("rebuild compact terminal counters");
    assert_eq!(
        terminal_counter_sum(&pool, schema, queue).await,
        0,
        "rebuild aggregates done_entries only; compact batches stay direct-counted"
    );
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("trusted compact counts after rebuild")
            .terminal,
        3,
        "trusted path after rebuild still counts compact receipt completions directly"
    );
}

async fn live_count_sum(pool: &sqlx::PgPool, schema: &str, queue: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT COALESCE(SUM(live_terminal_count), 0)::bigint \
         FROM {schema}.queue_terminal_live_counts WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("sum live_terminal_count")
}

async fn terminal_delta_sum(pool: &sqlx::PgPool, schema: &str, queue: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT COALESCE(SUM(terminal_delta), 0)::bigint \
         FROM {schema}.queue_terminal_count_deltas WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("sum terminal_delta")
}

async fn terminal_delta_row_count(pool: &sqlx::PgPool, schema: &str, queue: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT count(*)::bigint \
         FROM {schema}.queue_terminal_count_deltas WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("count terminal deltas")
}

async fn terminal_counter_sum(pool: &sqlx::PgPool, schema: &str, queue: &str) -> i64 {
    live_count_sum(pool, schema, queue).await + terminal_delta_sum(pool, schema, queue).await
}

async fn done_entries_count(pool: &sqlx::PgPool, schema: &str, queue: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT count(*)::bigint FROM {schema}.done_entries WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("count done_entries")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_striped_claims_probe_stripes_round_robin() {
    let (_db_guard, pool) = setup_pool(10).await;
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
async fn test_queue_storage_striped_runtime_claims_do_not_deadlock_with_enqueues() {
    let (_db_guard, pool) = setup_pool(20).await;
    let queue = "qs_striped_claim_enqueue";
    let schema = "awa_qs_striped_claim_enqueue";
    let config = QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        queue_stripe_count: 2,
        ..Default::default()
    };
    let store = Arc::new(create_store_with_config(&pool, config).await);

    let producer_pool = pool.clone();
    let producer_store = Arc::clone(&store);
    let producer = tokio::spawn(async move {
        for _ in 0..64 {
            producer_store
                .enqueue_batch(&producer_pool, queue, 1, 16)
                .await
                .expect("striped enqueue should not deadlock");
            tokio::task::yield_now().await;
        }
    });

    let claimer_pool = pool.clone();
    let claimer_store = Arc::clone(&store);
    let claimer = tokio::spawn(async move {
        let mut claimed_total = 0usize;
        for _ in 0..128 {
            let claimed = claimer_store
                .claim_runtime_batch(&claimer_pool, queue, 8, Duration::ZERO)
                .await
                .expect("striped runtime claim should not deadlock");
            claimed_total += claimed.len();
            tokio::task::yield_now().await;
        }
        claimed_total
    });

    let (_producer_done, claimed_total) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::try_join!(producer, claimer)
    })
    .await
    .expect("striped enqueue/claim workload timed out")
    .expect("striped enqueue/claim task panicked");

    assert!(
        claimed_total > 0,
        "expected concurrent striped runtime claims to claim at least one job"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_claim_runtime_does_not_wait_for_lease_rotation_lock() {
    let (_db_guard, pool) = setup_pool(10).await;
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

    // `lock_tx` stays open until after the claim returns; a real dependency on
    // the held row lock still times out, while loaded CI gets scheduler headroom.
    let claimed_while_locked = tokio::time::timeout(
        Duration::from_secs(3),
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

async fn backdate_one_lane_ready_job_run_at(
    pool: &sqlx::PgPool,
    schema: &str,
    job_id: i64,
    run_at: DateTime<Utc>,
) {
    let updated_ready = sqlx::query(&format!(
        "UPDATE {schema}.ready_entries SET run_at = $1 WHERE job_id = $2"
    ))
    .bind(run_at)
    .bind(job_id)
    .execute(pool)
    .await
    .expect("Failed to backdate queue-storage ready row");
    assert_eq!(
        updated_ready.rows_affected(),
        1,
        "expected exactly one ready row for manual aging backdate"
    );

    let updated_segment = sqlx::query(&format!(
        r#"
        WITH target AS (
            SELECT
                ready_slot,
                ready_generation,
                queue,
                priority,
                enqueue_shard,
                lane_seq
            FROM {schema}.ready_entries
            WHERE job_id = $2
        )
        UPDATE {schema}.ready_segments AS segment
        SET first_run_at = $1
        FROM target
        WHERE segment.ready_slot = target.ready_slot
          AND segment.ready_generation = target.ready_generation
          AND segment.queue = target.queue
          AND segment.priority = target.priority
          AND segment.enqueue_shard = target.enqueue_shard
          AND segment.first_lane_seq = target.lane_seq
          AND segment.next_lane_seq = target.lane_seq + 1
        "#
    ))
    .bind(run_at)
    .bind(job_id)
    .execute(pool)
    .await
    .expect("Failed to backdate queue-storage ready segment");
    assert_eq!(
        updated_segment.rows_affected(),
        1,
        "manual aging backdate expects a single-lane ready segment"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_claim_runtime_applies_priority_aging_dynamically() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_dynamic_priority_aging";
    let schema = "awa_qs_dynamic_priority_aging";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
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

    backdate_one_lane_ready_job_run_at(
        &pool,
        schema,
        aged_job_id,
        Utc::now() - chrono::Duration::seconds(aging_interval.as_secs() as i64 * 4),
    )
    .await;

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
async fn test_queue_storage_aged_completion_stays_compact_and_keeps_lane_priority() {
    let (_db_guard, pool) = setup_pool(10).await;
    let queue = "qs_aged_completion_lane_priority";
    let schema = "awa_qs_aged_completion_lane_priority";
    let store = create_store_with_config(
        &pool,
        QueueStorageConfig {
            schema: schema.to_string(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            queue_stripe_count: 1,
            lease_claim_receipts: true,
            claim_slot_count: 2,
        },
    )
    .await;

    let low_id = enqueue_job(
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
    let high_id = enqueue_job(
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

    let aging_interval = Duration::from_secs(60);
    let high_claimed = store
        .claim_runtime_batch_with_aging(&pool, queue, 1, Duration::ZERO, aging_interval)
        .await
        .expect("Failed to claim high-priority job");
    assert_eq!(high_claimed.len(), 1);
    assert_eq!(high_claimed[0].job.id, high_id);
    store
        .complete_runtime_batch(&pool, &high_claimed)
        .await
        .expect("Failed to complete high-priority job");

    backdate_one_lane_ready_job_run_at(
        &pool,
        schema,
        low_id,
        Utc::now() - chrono::Duration::seconds(aging_interval.as_secs() as i64 * 4),
    )
    .await;

    let aged_claimed = store
        .claim_runtime_batch_with_aging(&pool, queue, 1, Duration::ZERO, aging_interval)
        .await
        .expect("Failed to claim aged low-priority job");
    assert_eq!(aged_claimed.len(), 1);
    assert_eq!(aged_claimed[0].job.id, low_id);
    assert_eq!(aged_claimed[0].claim.priority, 4);
    assert_eq!(aged_claimed[0].job.priority, 1);
    store
        .complete_runtime_batch(&pool, &aged_claimed)
        .await
        .expect("Failed to complete aged low-priority job");

    assert_eq!(
        done_entries_count(&pool, schema, queue).await,
        0,
        "aged successful receipt completions should stay on the compact path"
    );
    assert_eq!(
        receipt_completion_batch_count(&pool, &store, queue).await,
        2,
        "both successful completions should use compact receipt batches"
    );

    let stored_priority: i16 = sqlx::query_scalar(&format!(
        "SELECT priority FROM {schema}.terminal_jobs WHERE job_id = $1"
    ))
    .bind(low_id)
    .fetch_one(&pool)
    .await
    .expect("Failed to read aged terminal entry");
    assert_eq!(stored_priority, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_bounded_claimers_limit_active_claimers_per_queue() {
    let (_db_guard, pool) = setup_pool(10).await;
    let schema = "awa_qs_bounded_claimers_limit";
    let store = create_store(&pool, schema).await;
    let queue = "qs_bounded_claimers_limit";
    let instance_a = Uuid::new_v4();
    let instance_b = Uuid::new_v4();
    let ttl = Duration::from_secs(30);
    let idle_threshold = Duration::from_secs(10);

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
    let (_db_guard, pool) = setup_pool(10).await;
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
async fn test_queue_storage_claimer_heartbeat_skips_fresh_lease() {
    let (_db_guard, pool) = setup_pool(10).await;
    let schema = "awa_qs_bounded_claimers_heartbeat";
    let store = create_store(&pool, schema).await;
    let queue = "qs_bounded_claimers_heartbeat";
    let instance = Uuid::new_v4();
    let ttl = Duration::from_secs(300);
    let idle_threshold = Duration::from_secs(120);

    let lease = store
        .acquire_queue_claimer(&pool, queue, instance, 1, ttl, idle_threshold)
        .await
        .expect("instance should acquire claimer")
        .expect("instance should get a claimer slot");

    let before: DateTime<Utc> = sqlx::query_scalar(&format!(
        "SELECT last_claimed_at FROM {schema}.queue_claimer_leases WHERE queue = $1 AND claimer_slot = $2"
    ))
    .bind(queue)
    .bind(lease.claimer_slot)
    .fetch_one(&pool)
    .await
    .expect("failed to read initial heartbeat");

    store
        .enqueue_batch(&pool, queue, 1, 1)
        .await
        .expect("failed to enqueue fresh-lease claim job");
    let claimed = store
        .claim_runtime_batch_with_aging_for_instance(
            &pool,
            queue,
            1,
            Duration::from_secs(300),
            Duration::from_secs(60),
            instance,
            1,
            ttl,
            idle_threshold,
        )
        .await
        .expect("fresh lease claim should succeed");
    assert_eq!(claimed.len(), 1);

    let after_fresh: DateTime<Utc> = sqlx::query_scalar(&format!(
        "SELECT last_claimed_at FROM {schema}.queue_claimer_leases WHERE queue = $1 AND claimer_slot = $2"
    ))
    .bind(queue)
    .bind(lease.claimer_slot)
    .fetch_one(&pool)
    .await
    .expect("failed to read skipped heartbeat");
    assert_eq!(
        after_fresh, before,
        "fresh heartbeat should not rewrite queue_claimer_leases"
    );

    sqlx::query(&format!(
        "UPDATE {schema}.queue_claimer_leases SET last_claimed_at = $1 WHERE queue = $2 AND claimer_slot = $3"
    ))
    .bind(
        Utc::now()
            - chrono::Duration::from_std(idle_threshold + Duration::from_secs(1))
                .expect("idle threshold should fit chrono duration"),
    )
    .bind(queue)
    .bind(lease.claimer_slot)
    .execute(&pool)
    .await
    .expect("failed to age claimer lease heartbeat");

    store
        .enqueue_batch(&pool, queue, 1, 1)
        .await
        .expect("failed to enqueue stale-lease claim job");
    let claimed = store
        .claim_runtime_batch_with_aging_for_instance(
            &pool,
            queue,
            1,
            Duration::from_secs(300),
            Duration::from_secs(60),
            instance,
            1,
            ttl,
            idle_threshold,
        )
        .await
        .expect("stale lease claim should succeed");
    assert_eq!(claimed.len(), 1);

    let after_stale: DateTime<Utc> = sqlx::query_scalar(&format!(
        "SELECT last_claimed_at FROM {schema}.queue_claimer_leases WHERE queue = $1 AND claimer_slot = $2"
    ))
    .bind(queue)
    .bind(lease.claimer_slot)
    .fetch_one(&pool)
    .await
    .expect("failed to read refreshed heartbeat");
    assert!(
        after_stale > after_fresh,
        "stale heartbeat should refresh queue_claimer_leases"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_prune_oldest_blocks_on_reader_lock() {
    let (_db_guard, pool) = setup_pool(10).await;
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
        .prune_oldest(&pool, Duration::ZERO)
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
        .prune_oldest(&pool, Duration::ZERO)
        .await
        .expect("Failed to prune after reader lock release");
    assert!(
        matches!(pruned, PruneOutcome::Pruned { slot: 0, .. }),
        "unexpected prune outcome after reader lock release: {pruned:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_runtime_complete_external() {
    let (_db_guard, pool) = setup_pool(10).await;
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
            lease_claim_receipts: false,
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
    let (_db_guard, pool) = setup_pool(10).await;
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
                lease_claim_receipts: false,
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
    wait_for_dlq_count(&pool, &store, queue, 1, Duration::from_secs(5)).await;
    wait_for_failed_done_count(&pool, &store, queue, 0, Duration::from_secs(5)).await;
    assert_eq!(dlq_reason(&pool, &store, job_id).await, "terminal_error");

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_runtime_callback_timeout_moves_to_dlq() {
    let (_db_guard, pool) = setup_pool(10).await;
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
                lease_claim_receipts: false,
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

    // NOTE: this test deliberately does not poll for the transient
    // `WaitingExternal` state. With a 100ms callback timeout and a 25ms
    // callback-rescue interval, that surface is only observable for ~100ms,
    // which can lie entirely inside a single load_job round under CI runner
    // load (load_job issues 6 sequential queries) — leading to the test
    // missing the window even though the callback path fired correctly.
    // The terminal `dlq_reason == "callback_timeout"` assertion below is
    // sufficient evidence that the worker registered the callback and that
    // the rescue path expired it: that reason is set exclusively by the
    // callback-rescue maintenance pass in awa-worker, which only fires for
    // jobs that reached `state = 'waiting_external'` with a non-null
    // `callback_timeout_at`.

    let failed = wait_for_job_state(
        &store,
        &pool,
        job_id,
        &[JobState::Failed],
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(failed.state, JobState::Failed);
    wait_for_dlq_count(&pool, &store, queue, 1, Duration::from_secs(5)).await;
    wait_for_failed_done_count(&pool, &store, queue, 0, Duration::from_secs(5)).await;
    assert_eq!(dlq_reason(&pool, &store, job_id).await, "callback_timeout");

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_dlq_api_round_trip() {
    let (_db_guard, pool) = setup_pool(10).await;
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
                lease_claim_receipts: false,
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
    let (_db_guard, pool) = setup_pool(10).await;
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
            lease_claim_receipts: false,
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
    wait_for_failed_done_count(&pool, &store, queue, 1, Duration::from_secs(5)).await;
    wait_for_dlq_count(&pool, &store, queue, 0, Duration::from_secs(5)).await;

    client.shutdown(Duration::from_secs(5)).await;

    let move_err = awa::model::dlq::bulk_move_failed_to_dlq(&pool, None, None, "ops_move", false)
        .await
        .expect_err("bulk move without scope should be rejected");
    assert!(matches!(move_err, AwaError::Validation(_)));

    let moved = awa::model::dlq::bulk_move_failed_to_dlq(&pool, None, None, "ops_move", true)
        .await
        .expect("Failed to bulk-move failed rows into the DLQ");
    assert_eq!(moved, 1);
    wait_for_failed_done_count(&pool, &store, queue, 0, Duration::from_secs(5)).await;
    wait_for_dlq_count(&pool, &store, queue, 1, Duration::from_secs(5)).await;

    let empty_filter = awa::model::ListDlqFilter::default();
    let retry_err = awa::model::dlq::bulk_retry_from_dlq(&pool, &empty_filter, false)
        .await
        .expect_err("bulk retry without scope should be rejected");
    assert!(matches!(retry_err, AwaError::Validation(_)));

    let retried = awa::model::dlq::bulk_retry_from_dlq(&pool, &empty_filter, true)
        .await
        .expect("Failed to bulk-retry DLQ rows");
    assert_eq!(retried, 1);
    wait_for_dlq_count(&pool, &store, queue, 0, Duration::from_secs(5)).await;

    let revived = admin::get_job(&pool, job_id)
        .await
        .expect("Failed to load revived job");
    assert_eq!(revived.state, JobState::Available);
    assert_eq!(revived.attempt, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_dlq_purge_guard_and_filtered_purge() {
    let (_db_guard, pool) = setup_pool(10).await;
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
                lease_claim_receipts: false,
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
    wait_for_dlq_count(&pool, &store, queue, 1, Duration::from_secs(5)).await;

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
    wait_for_dlq_count(&pool, &store, queue, 0, Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_retry_from_dlq_surfaces_unique_conflict() {
    let (_db_guard, pool) = setup_pool(10).await;
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
            lease_claim_receipts: false,
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
    let (_db_guard, pool) = setup_pool(10).await;
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
            lease_claim_receipts: false,
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
    wait_for_failed_done_count(&pool, &store, queue, 2, Duration::from_secs(5)).await;
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
    let (_db_guard, pool) = setup_pool(10).await;
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
            lease_claim_receipts: false,
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
    wait_for_failed_done_count(&pool, &store, queue, 2, Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_admin_discard_failed_releases_unique_claims_from_done() {
    let (_db_guard, pool) = setup_pool(10).await;
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
            lease_claim_receipts: false,
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

    wait_for_failed_done_count(&pool, &store, queue, 1, Duration::from_secs(5)).await;
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("Failed to sample queue counts")
            .terminal,
        1
    );

    let discarded = admin::discard_failed(&pool, TerminalFailureWorker.kind())
        .await
        .expect("Failed to discard failed jobs");
    assert_eq!(discarded, 1);
    wait_for_failed_done_count(&pool, &store, queue, 0, Duration::from_secs(5)).await;
    assert_eq!(
        store
            .queue_counts(&pool, queue)
            .await
            .expect("Failed to resample queue counts")
            .terminal,
        0
    );

    let reinserted = insert::insert_with(&pool, &DlqJob { id: 7 }, opts)
        .await
        .expect("discard_failed should release failed-state unique claims");
    assert_eq!(reinserted.state, JobState::Available);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_admin_discard_failed_releases_unique_claims_from_dlq() {
    let (_db_guard, pool) = setup_pool(10).await;
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
                lease_claim_receipts: false,
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

    wait_for_dlq_count(&pool, &store, queue, 1, Duration::from_secs(5)).await;

    let discarded = admin::discard_failed(&pool, TerminalFailureWorker.kind())
        .await
        .expect("Failed to discard dlq jobs");
    assert_eq!(discarded, 1);
    wait_for_dlq_count(&pool, &store, queue, 0, Duration::from_secs(5)).await;

    let reinserted = insert::insert_with(&pool, &DlqJob { id: 8 }, opts)
        .await
        .expect("discard_failed should release dlq unique claims");
    assert_eq!(reinserted.state, JobState::Available);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_jobs_view_insert_select_delete_compat() {
    let (_db_guard, pool) = setup_pool(10).await;
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
    let retained_ready_after_delete: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.ready_entries WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to recount ready entries");
    let tombstones_after_delete: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.ready_tombstones WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to count ready tombstones");
    let deferred_after_delete: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.deferred_jobs WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to recount deferred rows");
    assert_eq!(remaining, 0);
    assert_eq!(
        retained_ready_after_delete, 1,
        "compat DELETE should retain the ready backing row until queue prune"
    );
    assert_eq!(
        tombstones_after_delete, 1,
        "compat DELETE should tombstone the retained ready row"
    );
    assert_eq!(deferred_after_delete, 0);
    assert_eq!(
        deleted, 2,
        "INSTEAD OF DELETE trigger should report both deleted rows (one ready + one deferred) once delete_job_compat correctly returns TRUE"
    );
}

/// Priority-aging end-to-end check: a low-priority job that has been
/// waiting longer than the configured aging interval is claimed at a
/// raised effective priority, and its `_awa_original_priority` metadata
/// records the lane it came from.
///
/// Motivated by the 2026-05-09 sweep's `starvation_awa_60min` cell
/// reporting `aged_completion_rate=0` across a 60-minute soak. This test
/// confirms the *mechanism* works at a known interval; if it ever stops,
/// the bench will surface the regression as a zero counter again — but
/// without this test, a future change to the SQL aging clause could
/// silently break aging without breaking any other test.
///
/// We use a 100 ms aging interval and backdate the ready row by 250 ms so
/// that a priority-4 job's effective priority drops by at least one step
/// (`floor(elapsed / interval) = 2`, capped at min priority 1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_priority_aging_lifts_effective_priority_and_records_original() {
    let (_db_guard, pool) = setup_pool(4).await;
    let queue = "qs_priority_aging_lift";
    let schema = "awa_qs_priority_aging_lift";
    let store = create_store(&pool, schema).await;

    // Enqueue a single priority-4 job. Single row keeps the test
    // deterministic — the claim path either ages it or it doesn't.
    store
        .enqueue_batch(&pool, queue, 4, 1)
        .await
        .expect("Failed to enqueue priority-4 job");

    // Backdate past two aging windows so floor(elapsed / interval) = 2,
    // i.e. a priority-4 row's effective priority becomes 2.
    let aging_interval = Duration::from_millis(100);
    let job_id = sqlx::query_scalar::<_, i64>(&format!(
        "SELECT job_id FROM {schema}.ready_entries WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("Failed to load ready row for priority aging test");
    backdate_one_lane_ready_job_run_at(
        &pool,
        schema,
        job_id,
        Utc::now() - chrono::Duration::milliseconds(250),
    )
    .await;

    let claimed = store
        .claim_runtime_batch_with_aging_for_instance(
            &pool,
            queue,
            1,
            Duration::ZERO,
            aging_interval,
            Uuid::new_v4(),
            4,
            Duration::from_secs(3),
            Duration::from_millis(500),
        )
        .await
        .expect("Failed to claim with aging on");

    assert_eq!(
        claimed.len(),
        1,
        "expected the priority-4 job to be claimed"
    );
    let job = &claimed[0].job;

    assert!(
        job.priority < 4,
        "expected effective priority < 4 after aging; got {}",
        job.priority
    );

    let original = job
        .metadata
        .get("_awa_original_priority")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| {
            panic!(
                "claimed aged job missing _awa_original_priority metadata; got metadata={}",
                job.metadata
            )
        });
    assert_eq!(
        original, 4,
        "_awa_original_priority should record the lane priority"
    );
}

/// Counterpart to the aging test: when the aging interval is so large
/// that no aging fires within the test's wall-clock window, the claimed
/// job's priority equals the lane priority and `_awa_original_priority`
/// is *absent* from metadata. Pins the no-aging-on-no-elapsed-time
/// branch of `into_job_row`'s `priority < lane_priority` guard so a
/// future refactor can't accidentally always-stamp the metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_priority_aging_off_does_not_stamp_original() {
    let (_db_guard, pool) = setup_pool(4).await;
    let queue = "qs_priority_aging_off";
    let schema = "awa_qs_priority_aging_off";
    let store = create_store(&pool, schema).await;

    store
        .enqueue_batch(&pool, queue, 4, 1)
        .await
        .expect("Failed to enqueue priority-4 job");

    let claimed = store
        .claim_runtime_batch_with_aging_for_instance(
            &pool,
            queue,
            1,
            Duration::ZERO,
            // Aging interval much larger than the test's wall clock.
            Duration::from_secs(3_600),
            Uuid::new_v4(),
            4,
            Duration::from_secs(3),
            Duration::from_millis(500),
        )
        .await
        .expect("Failed to claim with aging off (effectively)");

    assert_eq!(claimed.len(), 1);
    let job = &claimed[0].job;
    assert_eq!(job.priority, 4, "no aging should leave priority unchanged");
    assert!(
        job.metadata.get("_awa_original_priority").is_none(),
        "_awa_original_priority must not be stamped when no aging fired; got metadata={}",
        job.metadata
    );
}

/// The in-process lane cache must self-heal when an earlier
/// `ensure_lane` call ran inside a transaction that ultimately rolled
/// back. After the rollback the cache still holds an entry claiming
/// the lane rows exist, but the next enqueue's `UPDATE
/// queue_enqueue_heads` will find no row — `advance_enqueue_head`
/// invalidates the cached entry, re-runs the lane inserts (bypassing
/// the cache fast path), and retries the UPDATE so the enqueue still
/// succeeds.
///
/// This test exercises the single-threaded shape of the recovery.
/// The concurrent shape — another transaction re-marks the cache
/// between the invalidate and the retry — is handled by
/// `advance_enqueue_head` calling `ensure_lane_inserts` directly
/// rather than `ensure_lane` (which would re-take the fast path).
/// That race is hard to reproduce deterministically from a single
/// test without exposing the cache, so the invariant lives in the
/// helper's comment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_storage_ensure_lane_cache_recovers_after_rollback() {
    let (_db_guard, pool) = setup_pool(4).await;
    let queue = "qs_ensure_lane_rollback";
    let schema = "awa_qs_ensure_lane_rollback";
    let store = create_store(&pool, schema).await;

    // Simulate the rolled-back ensure_lane: run the three lane
    // inserts in a transaction that we explicitly roll back. After
    // rollback the cache still thinks the lane exists because
    // `enqueue_batch` ran an earlier successful insert against a
    // different lane on this same store. We force-poison by running
    // an enqueue that succeeds for one priority, then truncating the
    // head row out from under the cache to mimic the rolled-back
    // state for that priority.
    store
        .enqueue_batch(&pool, queue, 4, 1)
        .await
        .expect("seed enqueue should succeed");

    // The cache now believes (queue, priority=4) is ensured. Wipe the
    // physical head row out from under the cache to mimic the
    // observable state after a rolled-back ensure_lane. Also clear
    // the seeded ready_entries / queue_lanes rows so the lane is
    // genuinely empty when the recovery path re-creates the head;
    // this avoids a PK collision on the freshly-reset lane_seq.
    for stmt in [
        format!("DELETE FROM {schema}.queue_enqueue_heads WHERE queue = $1 AND priority = $2"),
        format!("DELETE FROM {schema}.queue_claim_heads WHERE queue = $1 AND priority = $2"),
        format!("DELETE FROM {schema}.queue_lanes WHERE queue = $1 AND priority = $2"),
        format!("DELETE FROM {schema}.ready_entries WHERE queue = $1 AND priority = $2"),
    ] {
        sqlx::query(&stmt)
            .bind(queue)
            .bind(4_i16)
            .execute(&pool)
            .await
            .expect("wipe lane rows out from under the cache");
    }

    // A subsequent enqueue must still succeed: `advance_enqueue_head`
    // detects the empty UPDATE, invalidates the cached lane, and
    // re-runs ensure_lane before retrying.
    store
        .enqueue_batch(&pool, queue, 4, 3)
        .await
        .expect("post-rollback enqueue should self-heal via cache invalidation");

    let (next_seq, ready_count, max_lane_seq): (i64, i64, Option<i64>) = sqlx::query_as(&format!(
        "SELECT
             {schema}.sequence_next_value(heads.seq_name),
             count(ready.*)::bigint,
             max(ready.lane_seq)
         FROM {schema}.queue_enqueue_heads AS heads
         LEFT JOIN {schema}.ready_entries AS ready
           ON ready.queue = heads.queue
          AND ready.priority = heads.priority
          AND ready.enqueue_shard = heads.enqueue_shard
         WHERE heads.queue = $1 AND heads.priority = $2
         GROUP BY heads.seq_name"
    ))
    .bind(queue)
    .bind(4_i16)
    .fetch_one(&pool)
    .await
    .expect("queue_enqueue_heads row should exist after recovery");

    assert_eq!(
        ready_count, 3,
        "recovery enqueue should write all three replacement ready rows"
    );
    assert_eq!(
        Some(next_seq - 1),
        max_lane_seq,
        "enqueue sequence should sit immediately after the recovered ready rows"
    );
}

/// At `enqueue_shards > 1` every plane carries a shard column:
/// `queue_enqueue_heads`, `queue_claim_heads`, and `ready_entries`
/// extend their primary keys to include it; `leases`, `done_entries`,
/// and compact receipt completion batches carry the same terminal
/// identity. `lease_claims` carries the shard as a regular column.
/// This test seeds `queue_meta.enqueue_shards = 4`, enqueues enough
/// jobs to land on every shard, drains them through a worker, and
/// asserts:
///
/// 1. Every job completes.
/// 2. public terminal rows are spread across all 4 shards (the
///    producer rotor actually rotated, and the claim path returned the
///    shard so terminal history picked up the right `enqueue_shard`).
/// 3. terminal identity carries multiple rows that share
///    `(ready_slot, queue, priority, lane_seq)` across different shards
///    — i.e. the shard column is load-bearing in the key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_multi_shard_round_trip_through_completion() {
    let (_db_guard, pool) = setup_pool(8).await;
    let queue = "qs_multi_shard_round_trip";
    let schema = "awa_qs_multi_shard_round_trip";
    let store_config = QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        queue_stripe_count: 1,
        lease_claim_receipts: true,
        claim_slot_count: 2,
    };
    let store = create_store_with_config(&pool, store_config.clone()).await;

    // Opt the queue into 4 shards. Without this row the queue defaults
    // to a single shard and the test wouldn't exercise the multi-shard
    // path at all.
    sqlx::query(
        r#"
        INSERT INTO awa.queue_meta (queue, enqueue_shards)
        VALUES ($1, 4)
        ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards
        "#,
    )
    .bind(queue)
    .execute(&pool)
    .await
    .expect("seed queue_meta.enqueue_shards = 4");

    // Enqueue enough jobs that the producer-side rotor visits every
    // shard. 16 batches × 1 job = 16 producer-side calls; with the
    // rotor at modulo 4 each shard sees 4 batches.
    let mut job_ids = Vec::with_capacity(16);
    for i in 0..16 {
        let job_id = enqueue_job(
            &pool,
            &store,
            &CompleteJob { id: i },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await;
        job_ids.push(job_id);
    }

    let client = queue_storage_client(&pool, queue, store_config, CompleteWorker);
    client.start().await.expect("client start");

    for job_id in &job_ids {
        wait_for_job_state(
            &store,
            &pool,
            *job_id,
            &[JobState::Completed],
            Duration::from_secs(15),
        )
        .await;
    }

    // Every shard should surface at least one public terminal row.
    let shard_counts: Vec<(i16, i64)> = sqlx::query_as(&format!(
        "SELECT enqueue_shard, count(*)::bigint
         FROM {schema}.terminal_jobs
         WHERE queue = $1
           AND state = 'completed'
         GROUP BY enqueue_shard
         ORDER BY enqueue_shard"
    ))
    .bind(queue)
    .fetch_all(&pool)
    .await
    .expect("count terminal rows per shard");

    let shards_observed: Vec<i16> = shard_counts.iter().map(|(s, _)| *s).collect();
    assert_eq!(
        shards_observed,
        vec![0, 1, 2, 3],
        "all four shards should hold terminal rows; got {shard_counts:?}",
    );
    let total: i64 = shard_counts.iter().map(|(_, c)| c).sum();
    assert_eq!(
        total, 16,
        "exactly the enqueued jobs landed in terminal history"
    );

    // The shard column is load-bearing terminal identity because two
    // distinct shards can share a `(ready_slot, queue, priority,
    // lane_seq)` tuple. Each shard's `lane_seq` starts independently at
    // 1, so at S=4 with 4 jobs per shard there must be at least one
    // tuple that repeats.
    let max_dupes: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(max(c), 0)::bigint FROM (
             SELECT count(*) AS c
             FROM {schema}.terminal_jobs
             WHERE queue = $1
               AND state = 'completed'
             GROUP BY ready_slot, queue, priority, lane_seq
         ) AS grouped"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("count overlapping (slot, queue, priority, lane_seq) groups");
    assert!(
        max_dupes >= 2,
        "at S=4 the shard column carries the PK — at least one (ready_slot, queue, priority, lane_seq) \
         tuple should be reused across shards; got max group size {max_dupes}",
    );

    client.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_storage_multi_shard_public_available_counts_are_exact() {
    let (_db_guard, pool) = setup_pool(8).await;
    let queue = "qs_multi_shard_public_counts";
    let schema = "awa_qs_multi_shard_public_counts";
    let store_config = QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        queue_stripe_count: 1,
        lease_claim_receipts: true,
        claim_slot_count: 2,
    };
    let store = create_store_with_config(&pool, store_config.clone()).await;

    sqlx::query(
        r#"
        INSERT INTO awa.queue_meta (queue, enqueue_shards)
        VALUES ($1, 4)
        ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards
        "#,
    )
    .bind(queue)
    .execute(&pool)
    .await
    .expect("seed queue_meta.enqueue_shards = 4");

    for i in 0..16 {
        enqueue_job(
            &pool,
            &store,
            &CompleteJob { id: i },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await;
    }

    let direct_ready_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {schema}.ready_entries WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("read ready_entries count");
    assert_eq!(direct_ready_count, 16);

    let state_counts = admin::state_counts(&pool)
        .await
        .expect("read queue-storage state counts");
    assert_eq!(
        state_counts.get(&JobState::Available).copied().unwrap_or(0),
        16,
        "admin state_counts must not multiply ready rows by other shard cursors",
    );

    let overview = admin::queue_overview(&pool, queue)
        .await
        .expect("read queue overview")
        .expect("queue overview should include enqueued queue");
    assert_eq!(overview.available, 16);
    assert_eq!(overview.total_queued, 16);

    let client = queue_storage_client(&pool, queue, store_config, CompleteWorker);
    let health = client.health_check().await;
    assert_eq!(
        health
            .queues
            .get(queue)
            .expect("queue health should include configured queue")
            .available,
        16,
        "client health should report exact ready rows across enqueue shards",
    );

    client.shutdown(Duration::from_secs(1)).await;
}

/// `ordering_key` pins a job to a deterministic shard based on a
/// portable hash of the key bytes. Producers can use it to keep jobs
/// for the same logical partition (customer, order, account) on the
/// same shard, which preserves partitioned FIFO across batches even
/// when the per-store rotor would otherwise spread them.
///
/// This test enqueues batches with distinct ordering keys into a
/// 4-shard queue and asserts:
/// 1. All rows for the same key share one shard.
/// 2. That shard matches `shard_for_ordering_key(key, 4)`.
/// 3. Across enough distinct keys every shard is visited.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queue_storage_ordering_key_routes_to_stable_shard() {
    let (_db_guard, pool) = setup_pool(4).await;
    let queue = "qs_ordering_key_routes";
    let schema = "awa_qs_ordering_key_routes";
    let store_config = QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        queue_stripe_count: 1,
        lease_claim_receipts: true,
        claim_slot_count: 2,
    };
    let store = create_store_with_config(&pool, store_config).await;

    sqlx::query(
        r#"
        INSERT INTO awa.queue_meta (queue, enqueue_shards)
        VALUES ($1, 4)
        ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards
        "#,
    )
    .bind(queue)
    .execute(&pool)
    .await
    .expect("seed queue_meta.enqueue_shards = 4");

    let keys: [&[u8]; 16] = [
        b"customer-1",
        b"customer-2",
        b"customer-3",
        b"customer-4",
        b"customer-5",
        b"customer-6",
        b"customer-7",
        b"customer-8",
        b"order-100",
        b"order-101",
        b"order-200",
        b"order-201",
        b"account-a",
        b"account-b",
        b"account-c",
        b"account-d",
    ];

    let mut expected_per_job: Vec<(i64, i16)> = Vec::new();
    for (idx, key) in keys.iter().enumerate() {
        let expected_shard = awa_model::queue_storage::shard_for_ordering_key(key, 4);
        for rep in 0..3 {
            let opts = InsertOpts {
                queue: queue.into(),
                ordering_key: Some(key.to_vec()),
                ..Default::default()
            };
            let job_id = enqueue_job(
                &pool,
                &store,
                &CompleteJob {
                    id: (idx * 100 + rep) as i64,
                },
                opts,
            )
            .await;
            expected_per_job.push((job_id, expected_shard));
        }
    }

    let rows: Vec<(i64, i16)> = sqlx::query_as(&format!(
        "SELECT job_id, enqueue_shard FROM {schema}.ready_entries WHERE queue = $1"
    ))
    .bind(queue)
    .fetch_all(&pool)
    .await
    .expect("read ready_entries rows");

    let observed: std::collections::HashMap<i64, i16> = rows.into_iter().collect();
    for (job_id, expected_shard) in &expected_per_job {
        let got = observed
            .get(job_id)
            .copied()
            .unwrap_or_else(|| panic!("job {job_id} should be in ready_entries"));
        assert_eq!(
            got, *expected_shard,
            "job {job_id} should land on shard {expected_shard} (ordering-key derived), got {got}",
        );
    }

    let shards_hit: HashSet<i16> = expected_per_job.iter().map(|(_, s)| *s).collect();
    assert_eq!(
        shards_hit.len(),
        4,
        "16 distinct keys should reach all 4 shards via shard-key routing; got {shards_hit:?}",
    );
}

/// At `enqueue_shards > 1` the claim ordering is
/// `(effective_priority, run_at, priority)` across every shard's
/// candidate head. Older rows beat younger rows regardless of which
/// shard they sit on, so the natural fairness mechanism is run_at —
/// the shard whose oldest waiting row has the earliest run_at wins
/// the next claim, the other shards' rows age and win their turn
/// next. This test drives a steady producer load that touches every
/// shard, drains it through a worker, and asserts every shard's
/// `claim_seq` ended at its `next_seq` — i.e. no shard was starved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_multi_shard_claim_path_does_not_starve_shards() {
    let (_db_guard, pool) = setup_pool(8).await;
    let queue = "qs_shard_fairness";
    let schema = "awa_qs_shard_fairness";
    let store_config = QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        queue_stripe_count: 1,
        lease_claim_receipts: true,
        claim_slot_count: 2,
    };
    let store = create_store_with_config(&pool, store_config.clone()).await;

    sqlx::query(
        r#"
        INSERT INTO awa.queue_meta (queue, enqueue_shards)
        VALUES ($1, 4)
        ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards
        "#,
    )
    .bind(queue)
    .execute(&pool)
    .await
    .expect("seed queue_meta.enqueue_shards = 4");

    // Build one ordering key per target shard by scanning candidate
    // strings until each shard has a key.
    let keys_per_shard = build_keys_per_shard(4);

    let mut job_ids = Vec::with_capacity(64);
    for shard in 0..4i16 {
        let key = keys_per_shard.get(&shard).expect("key for shard").clone();
        for rep in 0..16u64 {
            let job_id = enqueue_job(
                &pool,
                &store,
                &CompleteJob {
                    id: (shard as i64) * 100 + rep as i64,
                },
                InsertOpts {
                    queue: queue.into(),
                    ordering_key: Some(key.clone()),
                    ..Default::default()
                },
            )
            .await;
            job_ids.push(job_id);
        }
    }

    let pre_counts: Vec<(i16, i64)> = sqlx::query_as(&format!(
        "SELECT enqueue_shard, count(*)::bigint
         FROM {schema}.ready_entries
         WHERE queue = $1
         GROUP BY enqueue_shard
         ORDER BY enqueue_shard"
    ))
    .bind(queue)
    .fetch_all(&pool)
    .await
    .expect("read pre-drain shard counts");
    assert_eq!(
        pre_counts.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        vec![0, 1, 2, 3],
        "every shard should hold rows pre-drain",
    );

    let client = queue_storage_client(&pool, queue, store_config, CompleteWorker);
    client.start().await.expect("client start");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let done_count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*)::bigint
             FROM {schema}.terminal_jobs
             WHERE queue = $1
               AND state = 'completed'"
        ))
        .bind(queue)
        .fetch_one(&pool)
        .await
        .expect("read done count while waiting for fairness drain");
        if done_count == job_ids.len() as i64 {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "Timed out waiting for fairness drain: terminal_count {done_count} != expected {}",
            job_ids.len(),
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let heads: Vec<(i16, i64, i64)> = sqlx::query_as(&format!(
        "SELECT claims.enqueue_shard,
                {schema}.sequence_next_value(claims.seq_name) AS claim_seq,
                {schema}.sequence_next_value(enqueues.seq_name) AS next_seq
         FROM {schema}.queue_claim_heads AS claims
         JOIN {schema}.queue_enqueue_heads AS enqueues
           ON enqueues.queue = claims.queue
          AND enqueues.priority = claims.priority
          AND enqueues.enqueue_shard = claims.enqueue_shard
         WHERE claims.queue = $1
         ORDER BY claims.enqueue_shard"
    ))
    .bind(queue)
    .fetch_all(&pool)
    .await
    .expect("read post-drain claim heads");

    assert_eq!(heads.len(), 4, "all four shard heads should exist");
    for (shard, claim_seq, next_seq) in &heads {
        assert!(
            *claim_seq > 0,
            "shard {shard} was starved — claim_seq still at 0 after drain",
        );
        assert_eq!(
            *claim_seq, *next_seq,
            "shard {shard} did not fully drain — claim_seq {claim_seq} != next_seq {next_seq}",
        );
    }

    client.shutdown(Duration::from_secs(5)).await;
}

/// Lowering `awa.queue_meta.enqueue_shards` is safe as long as every
/// row in flight on a now-out-of-range shard still gets claimed and
/// finalised. The claim path joins `queue_claim_heads` to
/// `queue_enqueue_heads` without filtering on the current shard
/// count, so this should be automatic. This test:
///
/// 1. Seeds `enqueue_shards = 4` and enqueues with keys routed to
///    every shard.
/// 2. Confirms all 4 shards hold ready rows.
/// 3. Drops `enqueue_shards` to 2 and constructs a fresh store
///    (the in-process cache holds the old value on the original
///    handle — a fresh handle observes the new value and exercises
///    the same code path a restarted worker would take).
/// 4. Starts a worker on the fresh handle and asserts every job
///    completes — including the ones on shards 2 and 3 that the
///    fresh runtime would never enqueue to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_storage_lowering_enqueue_shards_drains_existing_rows() {
    let (_db_guard, pool) = setup_pool(8).await;
    let queue = "qs_shard_lowering";
    let schema = "awa_qs_shard_lowering";
    let store_config = QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        queue_stripe_count: 1,
        lease_claim_receipts: true,
        claim_slot_count: 2,
    };
    let producer_store = create_store_with_config(&pool, store_config.clone()).await;

    sqlx::query(
        r#"
        INSERT INTO awa.queue_meta (queue, enqueue_shards)
        VALUES ($1, 4)
        ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards
        "#,
    )
    .bind(queue)
    .execute(&pool)
    .await
    .expect("seed queue_meta.enqueue_shards = 4");

    let keys_per_shard = build_keys_per_shard(4);

    let mut job_ids = Vec::with_capacity(16);
    for shard in 0..4i16 {
        let key = keys_per_shard.get(&shard).expect("key for shard").clone();
        for rep in 0..4u64 {
            let job_id = enqueue_job(
                &pool,
                &producer_store,
                &CompleteJob {
                    id: (shard as i64) * 100 + rep as i64,
                },
                InsertOpts {
                    queue: queue.into(),
                    ordering_key: Some(key.clone()),
                    ..Default::default()
                },
            )
            .await;
            job_ids.push(job_id);
        }
    }

    let pre_shards: Vec<i16> = sqlx::query_scalar(&format!(
        "SELECT DISTINCT enqueue_shard
         FROM {schema}.ready_entries
         WHERE queue = $1
         ORDER BY enqueue_shard"
    ))
    .bind(queue)
    .fetch_all(&pool)
    .await
    .expect("read pre-lower shard set");
    assert_eq!(
        pre_shards,
        vec![0, 1, 2, 3],
        "all four shards should hold rows before the lowering",
    );

    sqlx::query("UPDATE awa.queue_meta SET enqueue_shards = 2 WHERE queue = $1")
        .bind(queue)
        .execute(&pool)
        .await
        .expect("lower queue_meta.enqueue_shards to 2");

    // A fresh handle stands in for a restarted worker: the existing
    // `producer_store` has cached `enqueue_shards = 4`, so to exercise
    // the post-lowering code path we construct a new handle that reads
    // the new value. The schema already exists; we just need a fresh
    // QueueStorage value bound to the same schema.
    let drain_store =
        QueueStorage::new(store_config.clone()).expect("Failed to create drain QueueStorage");

    let client = queue_storage_client(&pool, queue, store_config, CompleteWorker);
    client.start().await.expect("client start");

    for job_id in &job_ids {
        wait_for_job_state(
            &drain_store,
            &pool,
            *job_id,
            &[JobState::Completed],
            Duration::from_secs(30),
        )
        .await;
    }

    let done_shards: Vec<i16> = sqlx::query_scalar(&format!(
        "SELECT DISTINCT enqueue_shard
         FROM {schema}.terminal_jobs
         WHERE queue = $1
           AND state = 'completed'
         ORDER BY enqueue_shard"
    ))
    .bind(queue)
    .fetch_all(&pool)
    .await
    .expect("read post-drain shard set");
    assert_eq!(
        done_shards,
        vec![0, 1, 2, 3],
        "every shard's rows including the out-of-range ones should drain to terminal_jobs",
    );

    client.shutdown(Duration::from_secs(5)).await;
}
