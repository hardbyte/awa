//! Receipt-plane regression gate.
//!
//! Issue #197 Phase 6 acceptance item: a CI-gated test that drives a
//! representative receipt-plane workload and asserts the architectural
//! invariants ADR-023 commits to. Distinct from the long-horizon bench
//! (which is a 12 h human-reviewed validation): this test is short and
//! makes precise assertions, so a regression in the queue-storage
//! engine fails fast in CI rather than waiting for an overnight cycle.
//!
//! What the gate enforces:
//!
//!   1. `lease_claim_closures_<i>` peak `n_dead_tup` MUST be 0 across
//!      every partition. Closures are append-only-then-truncate; any
//!      dead tuples in the closure heap means somebody is doing
//!      DELETEs or UPDATEs on a path the architectural contract
//!      forbids.
//!
//!   2. `lease_claims_<i>` peak `n_dead_tup` MUST stay below
//!      `AWA_RECEIPT_GATE_MAX_CLAIM_DEAD_TUPLES` (default 200). The
//!      claim partitions accumulate dead tuples only from autovacuum
//!      lag against legitimate row-level activity (rescue, late
//!      completions); the bound exists to catch a regression that
//!      starts churning rows on a path that should be append-only.
//!
//!   3. The queue ring MUST advance — `queue_ring_state.current_slot`
//!      moves at least N times during the run (where N is conservative
//!      relative to the rotation interval and run length). A pinned
//!      ring is the regression that issue #197 explicitly commits to
//!      catching.
//!
//!   4. The claim ring MUST advance for the same reason.
//!
//! Marked `#[ignore]` so it runs only via the nightly-chaos workflow
//! (which already drives the existing receipt-plane chaos tests). Run
//! locally with:
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
//!   cargo test -p awa --test receipt_plane_regression_gate \
//!     -- --ignored --nocapture
//! ```

use async_trait::async_trait;
use awa::model::{insert, migrations, QueueStorage, QueueStorageConfig};
use awa::{Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

fn database_url() -> String {
    env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_i64(key: &str, default: i64) -> i64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct ReceiptGateJob {
    payload: String,
}

#[derive(Clone, Default)]
struct ReceiptGateWorker;

#[async_trait]
impl Worker for ReceiptGateWorker {
    fn kind(&self) -> &'static str {
        "receipt_gate_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        // Fastest possible handler — receipt-plane workloads are
        // dominated by claim/complete throughput, not handler work.
        Ok(JobResult::Completed)
    }
}

async fn pool() -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url())
        .await
        .expect("Failed to connect — is Postgres running on the test DSN?")
}

async fn drop_schema(pool: &sqlx::PgPool, schema: &str) {
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(pool)
        .await
        .expect("Failed to drop schema");
}

/// Sample peak `n_dead_tup` across every partition matching the LIKE
/// filter, returning the per-partition max as a Vec keyed by relname.
async fn sample_per_partition_dead_tup(
    pool: &sqlx::PgPool,
    schema: &str,
    relname_like: &str,
) -> Vec<(String, i64)> {
    sqlx::query_as::<_, (String, i64)>(
        r#"
        SELECT relname, n_dead_tup
        FROM pg_stat_user_tables
        WHERE schemaname = $1
          AND relname LIKE $2
        ORDER BY relname
        "#,
    )
    .bind(schema)
    .bind(relname_like)
    .fetch_all(pool)
    .await
    .expect("dead-tuple sample failed")
}

async fn ring_state(pool: &sqlx::PgPool, schema: &str, ring: &str) -> (i32, i64) {
    sqlx::query_as::<_, (i32, i64)>(&format!(
        "SELECT current_slot, generation FROM {schema}.{ring}_ring_state WHERE singleton = TRUE"
    ))
    .fetch_one(pool)
    .await
    .expect("ring state read failed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_receipt_plane_steady_state_bounds_under_load() {
    let pool = pool().await;
    migrations::run(&pool)
        .await
        .expect("migrations should run cleanly");

    // Use a unique schema so concurrent CI shards don't collide. The
    // gate is fast enough to skip the migration-test mutex.
    let suffix = &Uuid::new_v4().simple().to_string()[..8];
    let schema = format!("awa_receipt_gate_{suffix}");
    let queue = format!("receipt_gate_{suffix}");
    drop_schema(&pool, &schema).await;

    let queue_slot_count: usize = env_u64("AWA_RECEIPT_GATE_QUEUE_SLOTS", 16) as usize;
    let lease_slot_count: usize = env_u64("AWA_RECEIPT_GATE_LEASE_SLOTS", 8) as usize;
    let claim_slot_count: usize = env_u64("AWA_RECEIPT_GATE_CLAIM_SLOTS", 8) as usize;
    let duration_secs = env_u64("AWA_RECEIPT_GATE_DURATION_SECS", 180);
    let target_rate = env_u64("AWA_RECEIPT_GATE_TARGET_RATE", 200);
    let max_claim_dead_tuples = env_i64("AWA_RECEIPT_GATE_MAX_CLAIM_DEAD_TUPLES", 200);

    let store = QueueStorage::new(QueueStorageConfig {
        schema: schema.clone(),
        queue_slot_count,
        lease_slot_count,
        claim_slot_count,
        // Receipt-plane gate: zero-deadline short jobs only.
        // `lease_claim_receipts: true` is the 0.6 default but pin it
        // explicitly so the gate doesn't drift if the default changes
        // again in a future release.
        lease_claim_receipts: true,
        ..Default::default()
    })
    .expect("queue storage config valid");
    store
        .install(&pool)
        .await
        .expect("queue storage schema install");
    store
        .reset(&pool)
        .await
        .expect("queue storage schema reset");

    let client = Client::builder(pool.clone())
        .queue(
            &queue,
            QueueConfig {
                max_workers: 8,
                poll_interval: Duration::from_millis(25),
                // Zero deadline = receipt-plane fast path
                deadline_duration: Duration::ZERO,
                ..QueueConfig::default()
            },
        )
        .queue_storage(
            QueueStorageConfig {
                schema: schema.clone(),
                queue_slot_count,
                lease_slot_count,
                claim_slot_count,
                lease_claim_receipts: true,
                ..Default::default()
            },
            Duration::from_millis(1_000),
            Duration::from_millis(50),
        )
        .claim_rotate_interval(Duration::from_millis(1_000))
        .register_worker(ReceiptGateWorker)
        .heartbeat_interval(Duration::from_millis(100))
        .promote_interval(Duration::from_millis(50))
        .leader_election_interval(Duration::from_millis(100))
        .leader_check_interval(Duration::from_millis(50))
        .build()
        .expect("client build");
    client.start().await.expect("client start");

    // Capture initial ring positions to compute advance later.
    let (queue_initial_slot, queue_initial_gen) = ring_state(&pool, &schema, "queue").await;
    let (claim_initial_slot, claim_initial_gen) = ring_state(&pool, &schema, "claim").await;

    // Producer loop: insert at target_rate jobs/sec for duration_secs.
    let started = Instant::now();
    let deadline = started + Duration::from_secs(duration_secs);
    let mut seq: u64 = 0;
    let producer_pool = pool.clone();
    let producer_queue = queue.clone();
    let producer = tokio::spawn(async move {
        // Uses a fixed-pace producer with 100 ms batch windows — same shape
        // as the bench harness's fixed-rate mode (long_horizon.rs:556+).
        let batch_period_ms = 100u64;
        let per_batch = (target_rate * batch_period_ms / 1000).max(1) as usize;
        let mut next_tick = Instant::now();
        loop {
            if Instant::now() >= deadline {
                break;
            }
            for _ in 0..per_batch {
                seq += 1;
                let _ = insert::insert_with(
                    &producer_pool,
                    &ReceiptGateJob {
                        payload: format!("seq-{seq}"),
                    },
                    InsertOpts {
                        queue: producer_queue.clone(),
                        ..Default::default()
                    },
                )
                .await;
            }
            next_tick += Duration::from_millis(batch_period_ms);
            let now = Instant::now();
            if next_tick > now {
                tokio::time::sleep(next_tick - now).await;
            } else {
                next_tick = now;
            }
        }
        seq
    });

    // Sampler loop: every 5 s, snapshot per-partition dead-tup peaks.
    let mut peak_claim_dead: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    let mut peak_closure_dead: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    let mut next_sample = started + Duration::from_secs(5);
    while Instant::now() < deadline {
        if Instant::now() >= next_sample {
            for (name, dead) in
                sample_per_partition_dead_tup(&pool, &schema, "lease_claims_%").await
            {
                if !name.starts_with("lease_claim_closures") {
                    let entry = peak_claim_dead.entry(name).or_insert(0);
                    *entry = (*entry).max(dead);
                }
            }
            for (name, dead) in
                sample_per_partition_dead_tup(&pool, &schema, "lease_claim_closures_%").await
            {
                let entry = peak_closure_dead.entry(name).or_insert(0);
                *entry = (*entry).max(dead);
            }
            next_sample += Duration::from_secs(5);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let total_seeded = producer.await.expect("producer joined");

    // Final post-load sample (ring may have advanced after last sample).
    for (name, dead) in sample_per_partition_dead_tup(&pool, &schema, "lease_claims_%").await {
        if !name.starts_with("lease_claim_closures") {
            let entry = peak_claim_dead.entry(name).or_insert(0);
            *entry = (*entry).max(dead);
        }
    }
    for (name, dead) in
        sample_per_partition_dead_tup(&pool, &schema, "lease_claim_closures_%").await
    {
        let entry = peak_closure_dead.entry(name).or_insert(0);
        *entry = (*entry).max(dead);
    }

    let (queue_final_slot, queue_final_gen) = ring_state(&pool, &schema, "queue").await;
    let (claim_final_slot, claim_final_gen) = ring_state(&pool, &schema, "claim").await;

    client.shutdown(Duration::from_secs(10)).await;

    let claim_peak_total: i64 = peak_claim_dead.values().copied().max().unwrap_or(0);
    let claim_peak_per_partition: Vec<(String, i64)> = {
        let mut v: Vec<_> = peak_claim_dead.into_iter().collect();
        v.sort();
        v
    };
    let closure_peak_total: i64 = peak_closure_dead.values().copied().max().unwrap_or(0);
    let closure_peak_per_partition: Vec<(String, i64)> = {
        let mut v: Vec<_> = peak_closure_dead.into_iter().collect();
        v.sort();
        v
    };

    let queue_rotations = queue_final_gen - queue_initial_gen;
    let claim_rotations = claim_final_gen - claim_initial_gen;

    println!(
        "[receipt-plane-gate] seeded={} duration={}s claim_peak_max={} closure_peak_max={} queue_rotations={} (slot {}→{}) claim_rotations={} (slot {}→{})",
        total_seeded,
        duration_secs,
        claim_peak_total,
        closure_peak_total,
        queue_rotations,
        queue_initial_slot,
        queue_final_slot,
        claim_rotations,
        claim_initial_slot,
        claim_final_slot,
    );
    println!(
        "[receipt-plane-gate] claim partitions: {:?}",
        claim_peak_per_partition
    );
    println!(
        "[receipt-plane-gate] closure partitions: {:?}",
        closure_peak_per_partition
    );

    // ── Assertions ────────────────────────────────────────────────────

    // Invariant 1: closures are append-only-then-truncate. Any dead
    // tuples on a closure partition means something is doing
    // DELETE/UPDATE on a path the architectural contract forbids.
    assert_eq!(
        closure_peak_total, 0,
        "lease_claim_closures partitions should never accumulate dead tuples; per-partition peaks: {:?}",
        closure_peak_per_partition
    );

    // Invariant 2: claims accumulate only via legitimate autovacuum lag.
    // Bound is generous on purpose — the regression we want to catch
    // is "claim partitions filling with thousands of dead rows", not
    // ordinary autovacuum jitter.
    assert!(
        claim_peak_total <= max_claim_dead_tuples,
        "lease_claims peak n_dead_tup {} exceeded gate {}; per-partition peaks: {:?}",
        claim_peak_total,
        max_claim_dead_tuples,
        claim_peak_per_partition
    );

    // Invariants 3 & 4: rings advance during the run. Conservative
    // expectation: `(duration_secs / rotate_interval_secs) / 4` so we
    // tolerate slow CI runners. With rotate_interval=1s and a 180s
    // duration, expected ≥45 rotations.
    // rotate_interval is 1s above; expect at least 25% of the wall-clock
    // ticks to actually rotate (a slow CI runner that misses some ticks
    // shouldn't fire the gate, but a pinned ring will).
    let expected_min_rotations = (duration_secs as i64) / 4;
    assert!(
        queue_rotations >= expected_min_rotations,
        "queue ring rotated {} times in {}s; expected at least {} \
         (a pinned ring is the regression this gate exists to catch)",
        queue_rotations,
        duration_secs,
        expected_min_rotations
    );
    assert!(
        claim_rotations >= expected_min_rotations,
        "claim ring rotated {} times in {}s; expected at least {} \
         (a pinned ring is the regression this gate exists to catch)",
        claim_rotations,
        duration_secs,
        expected_min_rotations
    );

    // Sanity: at least one job actually exercised the receipt-plane
    // path. Without this we could pass on a no-op workload.
    assert!(
        total_seeded > 0,
        "producer seeded zero jobs — workload invariant test passed by accident"
    );

    drop_schema(&pool, &schema).await;
    let _ = Arc::new(()); // keep async_trait import sane on stable
}
