//! Rate limiting integration tests.
//!
//! Tests that the token bucket rate limiter correctly throttles job dispatch.

use awa::{BuildError, Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, RateLimit};
use awa_model::{insert_with, migrations, InsertOpts};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
struct RateLimitJob {
    pub index: i64,
}

struct RateLimitWorker {
    completed_count: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl awa::Worker for RateLimitWorker {
    fn kind(&self) -> &'static str {
        "rate_limit_job"
    }
    async fn perform(
        &self,
        _job: &awa_model::JobRow,
        _ctx: &JobContext,
    ) -> Result<JobResult, JobError> {
        self.completed_count.fetch_add(1, Ordering::SeqCst);
        Ok(JobResult::Completed)
    }
}

/// Test 1: No rate limit — jobs dispatch without throttling.
#[tokio::test]
async fn test_no_rate_limit_fast_dispatch() {
    let pool = setup().await;
    let queue = "rl_no_limit";
    clean_queue(&pool, queue).await;

    // Insert 20 jobs
    for i in 0..20 {
        insert_with(
            &pool,
            &RateLimitJob { index: i },
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
                max_workers: 50,
                poll_interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .register_worker(RateLimitWorker {
            completed_count: completed.clone(),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    let start = Instant::now();
    // Wait for all jobs to complete (up to 5s)
    loop {
        if completed.load(Ordering::SeqCst) >= 20 {
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(completed.load(Ordering::SeqCst), 20);
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "Without rate limit, 20 jobs should complete quickly"
    );
}

/// Test 2: Rate limit 10/sec — 30 jobs should take ~2s minimum.
#[tokio::test]
async fn test_rate_limit_throttles_dispatch() {
    let pool = setup().await;
    let queue = "rl_throttled";
    clean_queue(&pool, queue).await;

    // Insert 30 jobs
    for i in 0..30 {
        insert_with(
            &pool,
            &RateLimitJob { index: i },
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
                max_workers: 50,
                poll_interval: Duration::from_millis(100),
                rate_limit: Some(RateLimit {
                    max_rate: 10.0,
                    burst: 10,
                }),
                ..Default::default()
            },
        )
        .register_worker(RateLimitWorker {
            completed_count: completed.clone(),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    let start = Instant::now();
    loop {
        if completed.load(Ordering::SeqCst) >= 30 {
            break;
        }
        if start.elapsed() > Duration::from_secs(10) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let elapsed = start.elapsed();
    client.shutdown(Duration::from_secs(2)).await;

    let count = completed.load(Ordering::SeqCst);
    assert_eq!(count, 30, "All 30 jobs should complete");
    // With rate limit 10/sec and burst 10: first 10 immediately, remaining 20 at 10/sec = 2s
    assert!(
        elapsed >= Duration::from_millis(1500),
        "Rate-limited dispatch should take >= 1.5s, took {elapsed:?}"
    );
}

/// Test 3: Burst 20, rate 5/sec — first 20 dispatched fast, remaining 10 take ~2s.
#[tokio::test]
async fn test_rate_limit_burst_then_throttle() {
    let pool = setup().await;
    let queue = "rl_burst";
    clean_queue(&pool, queue).await;

    // Insert 30 jobs
    for i in 0..30 {
        insert_with(
            &pool,
            &RateLimitJob { index: i },
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
                max_workers: 50,
                poll_interval: Duration::from_millis(100),
                rate_limit: Some(RateLimit {
                    max_rate: 5.0,
                    burst: 20,
                }),
                ..Default::default()
            },
        )
        .register_worker(RateLimitWorker {
            completed_count: completed.clone(),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    let start = Instant::now();
    loop {
        if completed.load(Ordering::SeqCst) >= 30 {
            break;
        }
        if start.elapsed() > Duration::from_secs(15) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let elapsed = start.elapsed();
    client.shutdown(Duration::from_secs(2)).await;

    let count = completed.load(Ordering::SeqCst);
    assert_eq!(count, 30, "All 30 jobs should complete");
    // First 20 come from burst (fast), remaining 10 at 5/sec = 2s
    assert!(
        elapsed >= Duration::from_millis(1500),
        "After burst, remaining jobs should be throttled. Took {elapsed:?}"
    );
}

/// Test 4: Rate limit + max_workers — concurrency is the bottleneck.
#[tokio::test]
async fn test_rate_limit_with_low_max_workers() {
    let pool = setup().await;
    let queue = "rl_low_workers";
    clean_queue(&pool, queue).await;

    // Insert 10 jobs
    for i in 0..10 {
        insert_with(
            &pool,
            &RateLimitJob { index: i },
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
                max_workers: 5,
                poll_interval: Duration::from_millis(50),
                rate_limit: Some(RateLimit {
                    max_rate: 1000.0,
                    burst: 1000,
                }),
                ..Default::default()
            },
        )
        .register_worker(RateLimitWorker {
            completed_count: completed.clone(),
        })
        .build()
        .unwrap();

    client.start().await.unwrap();

    let start = Instant::now();
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

/// Validation: invalid rate limit (max_rate <= 0).
#[tokio::test]
async fn test_invalid_rate_limit_rejected() {
    let pool = setup().await;
    let result = Client::builder(pool)
        .queue(
            "rl_invalid",
            QueueConfig {
                rate_limit: Some(RateLimit {
                    max_rate: 0.0,
                    burst: 10,
                }),
                ..Default::default()
            },
        )
        .build();

    assert!(matches!(result, Err(BuildError::InvalidRateLimit)));
}

/// Validation: weight == 0 rejected.
#[tokio::test]
async fn test_zero_weight_rejected() {
    let pool = setup().await;
    let result = Client::builder(pool)
        .queue(
            "rl_zero_weight",
            QueueConfig {
                weight: 0,
                ..Default::default()
            },
        )
        .build();

    assert!(matches!(result, Err(BuildError::InvalidWeight)));
}
