//! Receipt-plane chaos tests for the ADR-023 claim-ring redesign.
//!
//! These tests are `#[ignore]`-d so they only run in the nightly chaos
//! lane. Each one targets a failure mode the runtime tests can't reach
//! deterministically: rescue making progress under flood, prune
//! refusing to truncate while live traffic flows, the lock-table
//! barrier between TRUNCATE and concurrent inserts, and the orphan-
//! lease race between admin cancel and materialize.

use awa::model::{
    admin, insert, migrations, storage, PruneOutcome, QueueStorage, QueueStorageConfig,
    RotateOutcome,
};
use awa::{InsertOpts, JobState};
use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::sync::Mutex;

static QUEUE_STORAGE_RUNTIME_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup_pool(max_connections: u32) -> sqlx::PgPool {
    let url = database_url();
    let reset_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect for schema reset");
    sqlx::raw_sql("DROP SCHEMA IF EXISTS awa CASCADE")
        .execute(&reset_pool)
        .await
        .expect("drop awa schema");
    reset_pool.close().await;

    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&url)
        .await
        .expect("connect");
    migrations::run(&pool).await.expect("migrate");
    pool
}

async fn insert_gate_runtime(pool: &sqlx::PgPool) -> uuid::Uuid {
    let instance_id = uuid::Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO awa.runtime_instances (
            instance_id, hostname, pid, version, storage_capability,
            started_at, last_seen_at, snapshot_interval_ms, healthy,
            postgres_connected, poll_loop_alive, heartbeat_alive,
            maintenance_alive, shutting_down, leader, global_max_workers,
            queues, queue_descriptor_hashes, job_kind_descriptor_hashes
        )
        VALUES (
            $1, 'receipt-plane-chaos', 1, 'test', $2,
            now(), now(), 10000, TRUE,
            TRUE, TRUE, TRUE,
            TRUE, FALSE, FALSE, 1,
            '[]'::jsonb, '{}'::jsonb, '{}'::jsonb
        )
        "#,
    )
    .bind(instance_id)
    .bind("queue_storage")
    .execute(pool)
    .await
    .expect("insert gate runtime");
    instance_id
}

async fn activate_queue_storage_transition(pool: &sqlx::PgPool, schema: &str) {
    storage::prepare(
        pool,
        "queue_storage",
        serde_json::json!({ "schema": schema }),
    )
    .await
    .expect("prepare queue storage transition");
    let gate_runtime = insert_gate_runtime(pool).await;
    storage::enter_mixed_transition(pool)
        .await
        .expect("enter mixed transition");
    storage::finalize(pool).await.expect("finalize transition");
    sqlx::query("DELETE FROM awa.runtime_instances WHERE instance_id = $1")
        .bind(gate_runtime)
        .execute(pool)
        .await
        .expect("remove gate runtime");
}

async fn create_store(pool: &sqlx::PgPool, schema: &str, claim_slot_count: usize) -> QueueStorage {
    let store = QueueStorage::new(QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count,
        experimental_lease_claim_receipts: true,
        ..Default::default()
    })
    .expect("queue storage");
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(pool)
        .await
        .expect("drop store schema");
    sqlx::raw_sql(
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
    .expect("reset shared awa state");
    storage::abort(pool).await.expect("abort transition state");
    store.prepare_schema(pool).await.expect("prepare schema");
    store.reset(pool).await.expect("reset");
    activate_queue_storage_transition(pool, store.schema()).await;
    store
}

async fn lease_claim_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.lease_claims",
        store.schema()
    ))
    .fetch_one(pool)
    .await
    .expect("count lease_claims")
}

async fn lease_claim_closure_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.lease_claim_closures",
        store.schema()
    ))
    .fetch_one(pool)
    .await
    .expect("count lease_claim_closures")
}

async fn leases_count(pool: &sqlx::PgPool, store: &QueueStorage) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.leases",
        store.schema()
    ))
    .fetch_one(pool)
    .await
    .expect("count leases")
}

async fn insert_synthetic_open_claim(
    pool: &sqlx::PgPool,
    schema: &str,
    claim_slot: i32,
    job_id: i64,
    run_lease: i64,
    queue: &str,
    claimed_at: chrono::DateTime<Utc>,
) {
    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.lease_claims (
            claim_slot, job_id, run_lease, ready_slot, ready_generation,
            queue, priority, attempt, max_attempts, lane_seq,
            claimed_at, materialized_at
        ) VALUES ($1, $2, $3, 0, 0, $4, 2, 1, 25, $2, $5, NULL)
        "#
    ))
    .bind(claim_slot)
    .bind(job_id)
    .bind(run_lease)
    .bind(queue)
    .bind(claimed_at)
    .execute(pool)
    .await
    .expect("insert synthetic open claim");
}

/// Seed both a `ready_entries` row and a matching `lease_claims` row.
/// Required when the test exercises a rescue path: rescue closes the
/// claim and tries to re-enqueue back to ready by JOINing on the
/// (ready_slot, ready_generation, queue, priority, lane_seq) tuple,
/// which has to actually exist.
async fn insert_synthetic_claim_with_ready_row(
    pool: &sqlx::PgPool,
    schema: &str,
    claim_slot: i32,
    job_id: i64,
    run_lease: i64,
    queue: &str,
    claimed_at: chrono::DateTime<Utc>,
) {
    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.ready_entries (
            ready_slot, ready_generation, job_id, kind, queue,
            args, priority, attempt, run_lease, max_attempts, lane_seq,
            run_at, attempted_at, created_at, payload
        ) VALUES (
            0, 0, $1, 'qs_chaos_synth', $2,
            '{{}}'::jsonb, 2, 1, $3, 25, $1,
            clock_timestamp(), clock_timestamp(), clock_timestamp(), '{{}}'::jsonb
        )
        "#
    ))
    .bind(job_id)
    .bind(queue)
    .bind(run_lease)
    .execute(pool)
    .await
    .expect("insert synthetic ready row");
    insert_synthetic_open_claim(
        pool, schema, claim_slot, job_id, run_lease, queue, claimed_at,
    )
    .await;
}

// ────────────────────────────────────────────────────────────────────
// Test 1: rescue makes progress under overload
// ────────────────────────────────────────────────────────────────────

/// Flood the active claim partition with stale open claims and confirm
/// `rescue_stale_heartbeats` (which delegates to
/// `rescue_stale_receipt_claims_tx` on receipts-on schemas) makes
/// monotonic progress: each call closes some prefix of the stale set
/// without lapping or losing rows. The rescue path is bounded at 500
/// rows per call (via `LIMIT 500 FOR UPDATE OF claims SKIP LOCKED`),
/// so we seed 1500 stale claims and assert at most four calls drain
/// them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_receipt_rescue_makes_progress_under_overload() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let schema = "awa_qs_chaos_rescue_overload";
    let store = create_store(&pool, schema, 4).await;

    let stale_at = Utc::now() - chrono::Duration::seconds(3600);
    for i in 0..1_500_i64 {
        insert_synthetic_claim_with_ready_row(
            &pool,
            schema,
            0,
            10_000 + i,
            10_000 + i,
            "qs_chaos_rescue",
            stale_at,
        )
        .await;
    }

    assert_eq!(lease_claim_count(&pool, &store).await, 1_500);
    assert_eq!(lease_claim_closure_count(&pool, &store).await, 0);

    for call in 1..=4 {
        let _ = store
            .rescue_stale_heartbeats(&pool, Duration::from_secs(60))
            .await
            .expect("rescue_stale_heartbeats");
        let closures = lease_claim_closure_count(&pool, &store).await;
        assert!(
            closures <= 1_500,
            "closures cannot exceed seeded claim count, got {closures} on call {call}"
        );
        if closures == 1_500 {
            break;
        }
    }

    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        1_500,
        "rescue must close every stale claim within bounded calls"
    );

    store
        .rotate_claims(&pool)
        .await
        .expect("rotate claims off slot 0");

    let prune = store
        .prune_oldest_claims(&pool)
        .await
        .expect("prune slot 0 after rescue drain");
    assert!(
        matches!(prune, PruneOutcome::Pruned { slot: 0 }),
        "prune must succeed once every claim has a closure, got {prune:?}"
    );
}

// ────────────────────────────────────────────────────────────────────
// Test 2: prune refuses to truncate while traffic holds an open claim
// ────────────────────────────────────────────────────────────────────

/// Concurrent prune attempts against a partition with a live open
/// claim must all return `SkippedActive`; none may succeed. The static
/// `test_prune_oldest_claims_refuses_to_truncate_open_claim` covers
/// the single-call case; this test confirms the safety predicate
/// holds under real concurrency.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_prune_skips_active_under_concurrent_traffic() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let schema = "awa_qs_chaos_prune_concurrent";
    let store = create_store(&pool, schema, 4).await;

    insert_synthetic_open_claim(
        &pool,
        schema,
        0,
        99_999,
        99_999,
        "qs_chaos_prune",
        Utc::now(),
    )
    .await;

    let rotated = store.rotate_claims(&pool).await.expect("rotate off slot 0");
    assert!(
        matches!(rotated, RotateOutcome::Rotated { slot: 1, .. }),
        "expected to rotate to slot 1, got {rotated:?}"
    );

    let mut handles = Vec::new();
    for _ in 0..50 {
        let pool_clone = pool.clone();
        let schema_owned = schema.to_string();
        handles.push(tokio::spawn(async move {
            let store_clone = QueueStorage::new(QueueStorageConfig {
                schema: schema_owned,
                queue_slot_count: 4,
                lease_slot_count: 2,
                claim_slot_count: 4,
                experimental_lease_claim_receipts: true,
                ..Default::default()
            })
            .expect("queue storage handle");
            store_clone
                .prune_oldest_claims(&pool_clone)
                .await
                .expect("prune call")
        }));
    }

    let mut skipped_count = 0;
    let mut pruned_count = 0;
    for handle in handles {
        match handle.await.expect("prune task") {
            PruneOutcome::SkippedActive { slot: 0 } => skipped_count += 1,
            PruneOutcome::Pruned { .. } => pruned_count += 1,
            other => panic!("unexpected prune outcome: {other:?}"),
        }
    }

    assert_eq!(
        pruned_count, 0,
        "no prune call may succeed while open claim is live"
    );
    assert!(
        skipped_count > 0,
        "expected at least one SkippedActive, got {skipped_count}"
    );

    let survived: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {schema}.lease_claims_0 WHERE job_id = 99999"
    ))
    .fetch_one(&pool)
    .await
    .expect("count survivor");
    assert_eq!(
        survived, 1,
        "open claim must survive 50 concurrent SkippedActive prunes"
    );
}

// ────────────────────────────────────────────────────────────────────
// Test 3: prune's ACCESS EXCLUSIVE blocks against a concurrent reader
// ────────────────────────────────────────────────────────────────────

/// `prune_oldest_claims` takes `LOCK TABLE ACCESS EXCLUSIVE` on both
/// child partitions after `SET LOCAL lock_timeout = '50ms'`. If a
/// concurrent transaction holds `ACCESS SHARE` on the same partition,
/// prune must return `Blocked` rather than silently proceed. Mirrors
/// `test_queue_storage_prune_oldest_blocks_on_reader_lock` for the
/// queue ring, applied to the claim ring.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_prune_claims_blocked_by_concurrent_reader() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let schema = "awa_qs_chaos_prune_lock_timeout";
    let store = create_store(&pool, schema, 4).await;

    // Seed a claim + closure pair so PartitionTruncateSafety holds —
    // otherwise prune would return SkippedActive for that reason and
    // we'd never reach the lock_timeout path.
    insert_synthetic_open_claim(
        &pool,
        schema,
        0,
        77_777,
        77_777,
        "qs_chaos_prune_block",
        Utc::now(),
    )
    .await;
    sqlx::query(&format!(
        "INSERT INTO {schema}.lease_claim_closures (claim_slot, job_id, run_lease, outcome) \
         VALUES (0, 77777, 77777, 'completed')"
    ))
    .execute(&pool)
    .await
    .expect("seed closure");

    store.rotate_claims(&pool).await.expect("rotate off 0");

    let mut reader_tx = pool.begin().await.expect("begin reader tx");
    sqlx::query(&format!(
        "LOCK TABLE {schema}.lease_claims_0, {schema}.lease_claim_closures_0 IN ACCESS SHARE MODE"
    ))
    .execute(reader_tx.as_mut())
    .await
    .expect("LOCK TABLE ACCESS SHARE");

    let blocked = store
        .prune_oldest_claims(&pool)
        .await
        .expect("prune while reader holds ACCESS SHARE");
    assert!(
        matches!(blocked, PruneOutcome::Blocked { slot: 0 }),
        "prune must time out (Blocked) while ACCESS SHARE is held, got {blocked:?}"
    );

    reader_tx.rollback().await.expect("release reader lock");

    let pruned = store
        .prune_oldest_claims(&pool)
        .await
        .expect("prune after reader release");
    assert!(
        matches!(pruned, PruneOutcome::Pruned { slot: 0 }),
        "prune must succeed once reader releases, got {pruned:?}"
    );

    let post: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {schema}.lease_claims_0"))
        .fetch_one(&pool)
        .await
        .expect("count post-prune");
    assert_eq!(post, 0, "TRUNCATE must clear lease_claims_0");
}

// ────────────────────────────────────────────────────────────────────
// Test 4: admin cancel during materialize leaves no orphan lease
// ────────────────────────────────────────────────────────────────────

/// The receipt-only branch of `cancel_job_tx` defensively
/// `DELETE FROM leases` after writing the closure. If a concurrent
/// materialize commits a `leases` row between cancel's "no lease
/// found" check and its FOR UPDATE on `lease_claims`, that row
/// would otherwise be an orphan against the cancelled job. This
/// test injects the orphan directly so it doesn't depend on hitting
/// the actual race window, then issues admin cancel and asserts no
/// orphan survives.
///
/// Sequence:
///   1. Enqueue a real receipt-backed job via the store's normal
///      enqueue path.
///   2. Manually claim it (synthesizes the `lease_claims` row that
///      the receipt-only cancel branch operates on).
///   3. Inject a synthetic `leases` row for the same (job_id,
///      run_lease) — simulating a materialize that committed in the
///      gap.
///   4. Issue `admin::cancel(job_id)`. This walks the cancel path:
///      DELETE leases (sees the synthetic lease, takes the lease
///      branch and runs successfully), insert done, close_receipt.
///      The defensive DELETE in the receipt-only branch protects
///      the case where cancel misses the lease in the first DELETE;
///      this test exercises the path where the lease is seen.
///   5. Assert `leases` is empty for that pair, the closure was
///      written, and the job state is `Cancelled`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_admin_cancel_during_materialize_no_orphan_lease() {
    let _guard = QUEUE_STORAGE_RUNTIME_LOCK.lock().await;
    let pool = setup_pool(10).await;
    let schema = "awa_qs_chaos_cancel_orphan";
    let queue = "qs_chaos_cancel_orphan";
    let store = create_store(&pool, schema, 4).await;

    #[derive(serde::Serialize, serde::Deserialize, awa::JobArgs)]
    #[awa(kind = "qs_chaos_cancel_orphan_job")]
    struct ChaosCancelJob {
        id: i64,
    }

    let params = [insert::params_with(
        &ChaosCancelJob { id: 1 },
        InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .expect("build params")];
    store
        .enqueue_params_batch(&pool, &params)
        .await
        .expect("enqueue chaos cancel job");

    let job_id: i64 = sqlx::query_scalar(&format!(
        "SELECT job_id FROM {schema}.ready_entries WHERE queue = $1 ORDER BY job_id DESC LIMIT 1"
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("read job_id");

    // Drive a real claim through the store to populate lease_claims
    // with a runtime-shaped row.
    let claimed = store
        .claim_runtime_batch(&pool, queue, 1, Duration::ZERO)
        .await
        .expect("claim");
    let claimed = claimed.into_iter().next().expect("missing claim");
    let run_lease = claimed.job.run_lease;

    // Inject a synthetic leases row simulating a concurrent
    // materialize that committed in the cancel race window.
    sqlx::query(&format!(
        r#"
        INSERT INTO {schema}.leases (
            lease_slot, lease_generation, ready_slot, ready_generation,
            job_id, queue, state, priority, attempt, run_lease,
            max_attempts, lane_seq, heartbeat_at, deadline_at, attempted_at
        ) VALUES (
            0, 0, 0, 0,
            $1, $2, 'running', 2, 1, $3,
            25, $1, NULL, NULL, clock_timestamp()
        )
        "#
    ))
    .bind(job_id)
    .bind(queue)
    .bind(run_lease)
    .execute(&pool)
    .await
    .expect("inject orphan lease");

    assert_eq!(
        leases_count(&pool, &store).await,
        1,
        "test setup: one synthetic lease present"
    );
    assert_eq!(
        lease_claim_count(&pool, &store).await,
        1,
        "test setup: one claim row present"
    );
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        0,
        "test setup: no closure yet"
    );

    admin::cancel(&pool, job_id)
        .await
        .expect("admin cancel succeeds");

    assert_eq!(
        leases_count(&pool, &store).await,
        0,
        "lease branch must DELETE the synthetic lease"
    );
    assert_eq!(
        lease_claim_closure_count(&pool, &store).await,
        1,
        "cancel must write a closure on the receipt plane"
    );

    let job = store
        .load_job(&pool, job_id)
        .await
        .expect("load_job")
        .expect("job exists");
    assert!(
        matches!(job.state, JobState::Cancelled),
        "job state must be Cancelled, got {:?}",
        job.state
    );
}
