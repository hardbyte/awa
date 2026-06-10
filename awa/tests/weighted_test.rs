//! Weighted/opportunistic queue concurrency integration tests.
//!
//! Tests the OverflowPool, ConcurrencyMode, and global_max_workers behavior.

use awa::{BuildError, Client, JobArgs, JobContext, JobError, JobResult, QueueConfig};
use awa_model::{insert_with, migrations, InsertOpts};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup() -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url())
        .await
        .expect("Failed to connect");
    migrations::run(&pool).await.expect("Failed to migrate");
    pool
}

async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue meta");
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct WeightedJob {
    pub index: i64,
}

struct SlowWorker {
    completed: Arc<AtomicU32>,
    delay: Duration,
}

#[async_trait::async_trait]
impl awa::Worker for SlowWorker {
    fn kind(&self) -> &'static str {
        "weighted_job"
    }
    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        tokio::time::sleep(self.delay).await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(JobResult::Completed)
    }
}

/// Test 5: Hard-reserved backward compat — each queue capped independently.
#[tokio::test]
async fn test_hard_reserved_backward_compat() {
    let pool = setup().await;
    let queue = "wt_hard_compat";
    clean_queue(&pool, queue).await;

    for i in 0..10 {
        insert_with(
            &pool,
            &WeightedJob { index: i },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    let completed = Arc::new(AtomicU32::new(0));
    // No global_max_workers → hard-reserved mode
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 5,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .register_worker(SlowWorker {
            completed: completed.clone(),
            delay: Duration::from_millis(100),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    let start = std::time::Instant::now();
    loop {
        if completed.load(Ordering::SeqCst) >= 10 {
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(completed.load(Ordering::SeqCst), 10);
}

/// Test 8: Global cap — total in-flight never exceeds global_max_workers.
#[tokio::test]
async fn test_global_cap_not_exceeded() {
    let pool = setup().await;
    let queue_a = "wt_cap_a";
    let queue_b = "wt_cap_b";
    clean_queue(&pool, queue_a).await;
    clean_queue(&pool, queue_b).await;

    // Insert 20 jobs per queue
    for i in 0..20 {
        insert_with(
            &pool,
            &WeightedJob { index: i },
            InsertOpts {
                queue: queue_a.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        insert_with(
            &pool,
            &WeightedJob { index: i + 20 },
            InsertOpts {
                queue: queue_b.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    let completed = Arc::new(AtomicU32::new(0));
    let max_concurrent = Arc::new(AtomicU32::new(0));
    let current_concurrent = Arc::new(AtomicU32::new(0));

    // Custom worker that tracks concurrent execution
    struct ConcurrentTrackWorker {
        completed: Arc<AtomicU32>,
        max_concurrent: Arc<AtomicU32>,
        current_concurrent: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl awa::Worker for ConcurrentTrackWorker {
        fn kind(&self) -> &'static str {
            "weighted_job"
        }
        async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
            let current = self.current_concurrent.fetch_add(1, Ordering::SeqCst) + 1;
            // Update max seen concurrent
            self.max_concurrent.fetch_max(current, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(200)).await;
            self.current_concurrent.fetch_sub(1, Ordering::SeqCst);
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(JobResult::Completed)
        }
    }

    let global_max = 10u32;
    let client = Client::builder(pool.clone())
        .queue(
            queue_a,
            QueueConfig {
                min_workers: 2,
                weight: 1,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .queue(
            queue_b,
            QueueConfig {
                min_workers: 2,
                weight: 1,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .global_max_workers(global_max)
        .register_worker(ConcurrentTrackWorker {
            completed: completed.clone(),
            max_concurrent: max_concurrent.clone(),
            current_concurrent: current_concurrent.clone(),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    let start = std::time::Instant::now();
    loop {
        if completed.load(Ordering::SeqCst) >= 40 {
            break;
        }
        if start.elapsed() > Duration::from_secs(15) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client.shutdown(Duration::from_secs(3)).await;

    let max_seen = max_concurrent.load(Ordering::SeqCst);
    assert_eq!(
        completed.load(Ordering::SeqCst),
        40,
        "All 40 jobs should complete"
    );
    assert!(
        max_seen <= global_max,
        "Max concurrent ({max_seen}) should not exceed global_max_workers ({global_max})"
    );
}

/// Test 9: Build validation — min sum > global.
#[tokio::test]
async fn test_min_workers_exceed_global_rejected() {
    let pool = setup().await;
    let result = Client::builder(pool)
        .queue(
            "wt_val_a",
            QueueConfig {
                min_workers: 15,
                ..Default::default()
            },
        )
        .queue(
            "wt_val_b",
            QueueConfig {
                min_workers: 10,
                ..Default::default()
            },
        )
        .global_max_workers(20)
        .build();

    assert!(matches!(
        result,
        Err(BuildError::MinWorkersExceedGlobal {
            total_min: 25,
            global_max: 20
        })
    ));
}

/// Test 6: Idle overflow — only one queue loaded, gets all overflow.
#[tokio::test]
async fn test_idle_overflow_to_loaded_queue() {
    let pool = setup().await;
    let queue_a = "wt_overflow_a";
    let queue_b = "wt_overflow_b";
    clean_queue(&pool, queue_a).await;
    clean_queue(&pool, queue_b).await;

    // Insert 30 jobs only in queue_a, queue_b is empty
    for i in 0..30 {
        insert_with(
            &pool,
            &WeightedJob { index: i },
            InsertOpts {
                queue: queue_a.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    let completed = Arc::new(AtomicU32::new(0));
    // global_max=20, each queue min=5 → 10 overflow
    // Only queue_a is loaded, so it should get all overflow (5 local + 10 overflow = 15)
    let client = Client::builder(pool.clone())
        .queue(
            queue_a,
            QueueConfig {
                min_workers: 5,
                weight: 1,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .queue(
            queue_b,
            QueueConfig {
                min_workers: 5,
                weight: 1,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .global_max_workers(20)
        .register_worker(SlowWorker {
            completed: completed.clone(),
            delay: Duration::from_millis(100),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    let start = std::time::Instant::now();
    loop {
        if completed.load(Ordering::SeqCst) >= 30 {
            break;
        }
        if start.elapsed() > Duration::from_secs(10) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(completed.load(Ordering::SeqCst), 30);
    // With only 5 min_workers and no overflow, it would take ~600ms (30 jobs / 5 = 6 batches * 100ms)
    // With overflow, more jobs run concurrently → faster
    // The key assertion is just that all 30 complete within a reasonable time
}

/// Test 7: Floor guarantee — both queues fully loaded, each reaches >= min_workers concurrent.
#[tokio::test]
async fn test_floor_guarantee_under_load() {
    let pool = setup().await;
    let queue_a = "wt_floor_a";
    let queue_b = "wt_floor_b";
    clean_queue(&pool, queue_a).await;
    clean_queue(&pool, queue_b).await;

    // Insert 40 jobs per queue (enough to sustain load)
    for i in 0..40 {
        insert_with(
            &pool,
            &WeightedJob { index: i },
            InsertOpts {
                queue: queue_a.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        insert_with(
            &pool,
            &WeightedJob { index: i + 40 },
            InsertOpts {
                queue: queue_b.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    let completed_a = Arc::new(AtomicU32::new(0));
    let completed_b = Arc::new(AtomicU32::new(0));
    let max_concurrent_a = Arc::new(AtomicU32::new(0));
    let max_concurrent_b = Arc::new(AtomicU32::new(0));
    let current_a = Arc::new(AtomicU32::new(0));
    let current_b = Arc::new(AtomicU32::new(0));

    struct FloorWorker {
        completed_a: Arc<AtomicU32>,
        completed_b: Arc<AtomicU32>,
        max_concurrent_a: Arc<AtomicU32>,
        max_concurrent_b: Arc<AtomicU32>,
        current_a: Arc<AtomicU32>,
        current_b: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl awa::Worker for FloorWorker {
        fn kind(&self) -> &'static str {
            "weighted_job"
        }
        async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
            if ctx.job.queue == "wt_floor_a" {
                let c = self.current_a.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_concurrent_a.fetch_max(c, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(300)).await;
                self.current_a.fetch_sub(1, Ordering::SeqCst);
                self.completed_a.fetch_add(1, Ordering::SeqCst);
            } else {
                let c = self.current_b.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_concurrent_b.fetch_max(c, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(300)).await;
                self.current_b.fetch_sub(1, Ordering::SeqCst);
                self.completed_b.fetch_add(1, Ordering::SeqCst);
            }
            Ok(JobResult::Completed)
        }
    }

    // global_max=20, A min=5, B min=5 → 10 overflow shared equally
    let client = Client::builder(pool.clone())
        .queue(
            queue_a,
            QueueConfig {
                min_workers: 5,
                weight: 1,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .queue(
            queue_b,
            QueueConfig {
                min_workers: 5,
                weight: 1,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .global_max_workers(20)
        .register_worker(FloorWorker {
            completed_a: completed_a.clone(),
            completed_b: completed_b.clone(),
            max_concurrent_a: max_concurrent_a.clone(),
            max_concurrent_b: max_concurrent_b.clone(),
            current_a: current_a.clone(),
            current_b: current_b.clone(),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    let start = std::time::Instant::now();
    loop {
        let total = completed_a.load(Ordering::SeqCst) + completed_b.load(Ordering::SeqCst);
        if total >= 60 {
            break;
        }
        if start.elapsed() > Duration::from_secs(15) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client.shutdown(Duration::from_secs(3)).await;

    // Both queues should have reached at least min_workers concurrent
    let max_a = max_concurrent_a.load(Ordering::SeqCst);
    let max_b = max_concurrent_b.load(Ordering::SeqCst);
    assert!(
        max_a >= 5,
        "Queue A should reach at least 5 concurrent (min_workers), saw max of {max_a}"
    );
    assert!(
        max_b >= 5,
        "Queue B should reach at least 5 concurrent (min_workers), saw max of {max_b}"
    );
}

/// Test 10: Weight proportionality — A(w=3) B(w=1), global=24, min=2 each.
/// A should get ~3x the overflow capacity of B.
#[tokio::test]
async fn test_weight_proportionality() {
    let pool = setup().await;
    let queue_a = "wt_prop_a";
    let queue_b = "wt_prop_b";
    clean_queue(&pool, queue_a).await;
    clean_queue(&pool, queue_b).await;

    // Insert enough jobs so both queues stay loaded through the measurement
    // window: neither queue can drain before the final snapshot at
    // SNAPSHOT_END total completions.
    for i in 0..120 {
        insert_with(
            &pool,
            &WeightedJob { index: i },
            InsertOpts {
                queue: queue_a.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        insert_with(
            &pool,
            &WeightedJob { index: i + 120 },
            InsertOpts {
                queue: queue_b.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    let completed_a = Arc::new(AtomicU32::new(0));
    let completed_b_counter = Arc::new(AtomicU32::new(0));
    let max_concurrent_a = Arc::new(AtomicU32::new(0));
    let max_concurrent_b = Arc::new(AtomicU32::new(0));
    let current_a = Arc::new(AtomicU32::new(0));
    let current_b = Arc::new(AtomicU32::new(0));
    // The proportionality measurement must be recorded by the workers
    // themselves at fixed completion counts. The queues hold a finite number
    // of jobs; if the test task polls and snapshots, a starved runner can let
    // both queues fully drain between polls and the counts converge regardless
    // of weighting. The first SNAPSHOT_BASE completions are also excluded as
    // ramp-up: weighted overflow allocation needs a few dispatch rounds to
    // settle, and including startup in the share measurement makes the
    // assertion timing-sensitive under parallel test load.
    const SNAPSHOT_BASE: u32 = 40;
    const SNAPSHOT_END: u32 = 160;
    const SNAPSHOT_UNSET: u32 = u32::MAX;
    let total_completed = Arc::new(AtomicU32::new(0));
    let base_a = Arc::new(AtomicU32::new(SNAPSHOT_UNSET));
    let base_b = Arc::new(AtomicU32::new(SNAPSHOT_UNSET));
    let snapshot_a = Arc::new(AtomicU32::new(SNAPSHOT_UNSET));
    let snapshot_b = Arc::new(AtomicU32::new(SNAPSHOT_UNSET));

    struct ProportionWorker {
        completed_a: Arc<AtomicU32>,
        completed_b: Arc<AtomicU32>,
        max_concurrent_a: Arc<AtomicU32>,
        max_concurrent_b: Arc<AtomicU32>,
        current_a: Arc<AtomicU32>,
        current_b: Arc<AtomicU32>,
        total_completed: Arc<AtomicU32>,
        base_a: Arc<AtomicU32>,
        base_b: Arc<AtomicU32>,
        snapshot_a: Arc<AtomicU32>,
        snapshot_b: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl awa::Worker for ProportionWorker {
        fn kind(&self) -> &'static str {
            "weighted_job"
        }
        async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
            if ctx.job.queue == "wt_prop_a" {
                let c = self.current_a.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_concurrent_a.fetch_max(c, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(150)).await;
                self.current_a.fetch_sub(1, Ordering::SeqCst);
                self.completed_a.fetch_add(1, Ordering::SeqCst);
            } else {
                let c = self.current_b.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_concurrent_b.fetch_max(c, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(150)).await;
                self.current_b.fetch_sub(1, Ordering::SeqCst);
                self.completed_b.fetch_add(1, Ordering::SeqCst);
            }
            match self.total_completed.fetch_add(1, Ordering::SeqCst) + 1 {
                SNAPSHOT_BASE => {
                    self.base_a
                        .store(self.completed_a.load(Ordering::SeqCst), Ordering::SeqCst);
                    self.base_b
                        .store(self.completed_b.load(Ordering::SeqCst), Ordering::SeqCst);
                }
                SNAPSHOT_END => {
                    self.snapshot_a
                        .store(self.completed_a.load(Ordering::SeqCst), Ordering::SeqCst);
                    self.snapshot_b
                        .store(self.completed_b.load(Ordering::SeqCst), Ordering::SeqCst);
                }
                _ => {}
            }
            Ok(JobResult::Completed)
        }
    }

    // global=24, A(min=2, w=3), B(min=2, w=1) → overflow=20
    // A fair share: 20 * 3/4 = 15 → A total = 2 + 15 = 17
    // B fair share: 20 * 1/4 = 5  → B total = 2 + 5  = 7
    let client = Client::builder(pool.clone())
        .queue(
            queue_a,
            QueueConfig {
                min_workers: 2,
                weight: 3,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .queue(
            queue_b,
            QueueConfig {
                min_workers: 2,
                weight: 1,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .global_max_workers(24)
        .register_worker(ProportionWorker {
            completed_a: completed_a.clone(),
            completed_b: completed_b_counter.clone(),
            max_concurrent_a: max_concurrent_a.clone(),
            max_concurrent_b: max_concurrent_b.clone(),
            current_a: current_a.clone(),
            current_b: current_b.clone(),
            total_completed: total_completed.clone(),
            base_a: base_a.clone(),
            base_b: base_b.clone(),
            snapshot_a: snapshot_a.clone(),
            snapshot_b: snapshot_b.clone(),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    // Wait for enough jobs to complete to see steady-state behavior. Wait on
    // the snapshot publication itself, not the completion counter: the worker
    // increments the counter before storing the snapshot, so polling the
    // counter could observe SNAPSHOT_END a beat before the stores land.
    let start = std::time::Instant::now();
    loop {
        if snapshot_a.load(Ordering::SeqCst) != SNAPSHOT_UNSET {
            break;
        }
        if start.elapsed() > Duration::from_secs(20) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client.shutdown(Duration::from_secs(3)).await;

    // Weighted overflow controls sustained share, not a strict peak concurrency
    // bound, so assert on completed work rather than max instantaneous
    // concurrency. Prefer the worker-recorded post-ramp-up window
    // [SNAPSHOT_BASE, SNAPSHOT_END]; if the run was too slow to reach
    // SNAPSHOT_END, neither queue drained, so the final counts are still a
    // valid share measurement.
    let (ca, cb, window) = if snapshot_a.load(Ordering::SeqCst) != SNAPSHOT_UNSET {
        (
            snapshot_a.load(Ordering::SeqCst) - base_a.load(Ordering::SeqCst),
            snapshot_b.load(Ordering::SeqCst) - base_b.load(Ordering::SeqCst),
            "post-ramp-up window",
        )
    } else {
        (
            completed_a.load(Ordering::SeqCst),
            completed_b_counter.load(Ordering::SeqCst),
            "timeout fallback (full run)",
        )
    };

    assert!(
        ca > cb,
        "Queue A (weight=3) should complete more jobs ({ca}) than B (weight=1, {cb}) in the {window}"
    );
}

/// Test 11: Permit-before-claim — no jobs stuck in `running` with no active executor.
/// After shutdown, all running jobs from our worker should be completed or drained.
#[tokio::test]
async fn test_permit_before_claim_no_orphans() {
    let pool = setup().await;
    let queue = "wt_permit_claim";
    clean_queue(&pool, queue).await;

    for i in 0..20 {
        insert_with(
            &pool,
            &WeightedJob { index: i },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    let completed = Arc::new(AtomicU32::new(0));
    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                min_workers: 3,
                weight: 1,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .global_max_workers(10)
        .register_worker(SlowWorker {
            completed: completed.clone(),
            delay: Duration::from_millis(100),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    // Let some jobs process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Shutdown and drain
    client.shutdown(Duration::from_secs(5)).await;

    // After shutdown: no jobs should be in running state for this queue
    // (they should all be completed or still available/retryable)
    let running: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'running'")
            .bind(queue)
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(
        running, 0,
        "No jobs should be stuck in running state after shutdown"
    );
}

/// Test: Health check reports weighted capacity.
#[tokio::test]
async fn test_health_check_weighted_mode() {
    let pool = setup().await;
    let queue = "wt_health";
    clean_queue(&pool, queue).await;

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                min_workers: 5,
                weight: 3,
                poll_interval: Duration::from_millis(200),
                ..Default::default()
            },
        )
        .global_max_workers(20)
        .register_worker(SlowWorker {
            completed: Arc::new(AtomicU32::new(0)),
            delay: Duration::from_millis(10),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let health = client.health_check().await;
    let queue_health = health.queues.get(queue).expect("queue should be in health");

    match &queue_health.capacity {
        awa::QueueCapacity::Weighted {
            min_workers,
            weight,
            ..
        } => {
            assert_eq!(*min_workers, 5);
            assert_eq!(*weight, 3);
        }
        other => panic!("Expected Weighted capacity, got: {other:?}"),
    }

    client.shutdown(Duration::from_secs(1)).await;
}

/// Test: Health check reports hard-reserved capacity.
#[tokio::test]
async fn test_health_check_hard_reserved_mode() {
    let pool = setup().await;
    let queue = "wt_health_hard";
    clean_queue(&pool, queue).await;

    let client = Client::builder(pool.clone())
        .queue(
            queue,
            QueueConfig {
                max_workers: 42,
                poll_interval: Duration::from_millis(200),
                ..Default::default()
            },
        )
        .register_worker(SlowWorker {
            completed: Arc::new(AtomicU32::new(0)),
            delay: Duration::from_millis(10),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let health = client.health_check().await;
    let queue_health = health.queues.get(queue).expect("queue should be in health");

    match &queue_health.capacity {
        awa::QueueCapacity::HardReserved { max_workers } => {
            assert_eq!(*max_workers, 42);
        }
        other => panic!("Expected HardReserved capacity, got: {other:?}"),
    }

    client.shutdown(Duration::from_secs(1)).await;
}
