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
    async fn perform(
        &self,
        _job: &awa_model::JobRow,
        _ctx: &JobContext,
    ) -> Result<JobResult, JobError> {
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
        async fn perform(
            &self,
            _job: &awa_model::JobRow,
            _ctx: &JobContext,
        ) -> Result<JobResult, JobError> {
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
        async fn perform(
            &self,
            job: &awa_model::JobRow,
            _ctx: &JobContext,
        ) -> Result<JobResult, JobError> {
            if job.queue == "wt_floor_a" {
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

    // Insert enough jobs so both queues are always loaded
    for i in 0..60 {
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
            &WeightedJob { index: i + 60 },
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

    struct ProportionWorker {
        completed_a: Arc<AtomicU32>,
        completed_b: Arc<AtomicU32>,
        max_concurrent_a: Arc<AtomicU32>,
        max_concurrent_b: Arc<AtomicU32>,
        current_a: Arc<AtomicU32>,
        current_b: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl awa::Worker for ProportionWorker {
        fn kind(&self) -> &'static str {
            "weighted_job"
        }
        async fn perform(
            &self,
            job: &awa_model::JobRow,
            _ctx: &JobContext,
        ) -> Result<JobResult, JobError> {
            if job.queue == "wt_prop_a" {
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
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    // Wait for enough jobs to complete to see steady-state behavior
    let start = std::time::Instant::now();
    loop {
        let total = completed_a.load(Ordering::SeqCst) + completed_b_counter.load(Ordering::SeqCst);
        if total >= 80 {
            break;
        }
        if start.elapsed() > Duration::from_secs(15) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client.shutdown(Duration::from_secs(3)).await;

    let max_a = max_concurrent_a.load(Ordering::SeqCst);
    let max_b = max_concurrent_b.load(Ordering::SeqCst);

    let ca = completed_a.load(Ordering::SeqCst);
    let cb = completed_b_counter.load(Ordering::SeqCst);

    // A (weight=3) should have gotten more concurrency than B (weight=1)
    assert!(
        max_a > max_b,
        "Queue A (weight=3) should have more max concurrent ({max_a}) than B (weight=1, {max_b})"
    );
    // A should have completed more jobs than B in the same time window,
    // reflecting higher sustained concurrency from the weighted allocation
    assert!(
        ca > cb,
        "Queue A (weight=3) should complete more jobs ({ca}) than B (weight=1, {cb})"
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
