//! Micro-benchmark for descriptor sync throughput. Compares the batched
//! implementation at different descriptor counts. Not a regression gate —
//! it's ignored by default, run manually via
//! `cargo test --release --package awa-model --test bench_descriptor_sync -- --ignored --nocapture`
//!
//! The old per-descriptor loop is gone from the repo but the baseline number
//! it produced (≈N × per-upsert RTT) is what makes the batched number
//! meaningful.

use awa_model::admin::{
    sync_job_kind_descriptors, sync_queue_descriptors, JobKindDescriptor, NamedJobKindDescriptor,
    NamedQueueDescriptor, QueueDescriptor,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::{Duration, Instant};

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".into())
}

async fn pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .expect("connect")
}

async fn reset(pool: &PgPool, prefix: &str) {
    sqlx::query("DELETE FROM awa.queue_descriptors WHERE queue LIKE $1")
        .bind(format!("{prefix}%"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM awa.job_kind_descriptors WHERE kind LIKE $1")
        .bind(format!("{prefix}%"))
        .execute(pool)
        .await
        .unwrap();
}

#[ignore]
#[tokio::test]
async fn bench_sync_queue_descriptors_scaling() {
    let pool = pool().await;
    awa_model::migrations::run(&pool).await.unwrap();
    reset(&pool, "bench_").await;

    for n in [10, 100, 500, 2000] {
        let descriptors: Vec<NamedQueueDescriptor> = (0..n)
            .map(|i| NamedQueueDescriptor {
                queue: format!("bench_queue_{i}"),
                descriptor: QueueDescriptor::new()
                    .display_name(format!("Queue {i}"))
                    .description("scaling bench")
                    .owner("bench")
                    .tag("scaling-bench"),
            })
            .collect();

        // Warm once so the prepared-statement cache and page cache are primed.
        sync_queue_descriptors(&pool, &descriptors, Duration::from_secs(10))
            .await
            .unwrap();

        let start = Instant::now();
        const ITERS: u32 = 5;
        for _ in 0..ITERS {
            sync_queue_descriptors(&pool, &descriptors, Duration::from_secs(10))
                .await
                .unwrap();
        }
        let elapsed = start.elapsed();
        let per_call = elapsed / ITERS;
        eprintln!(
            "sync_queue_descriptors n={n}: {:?} total over {ITERS} calls, {:?}/call, {:.1} µs/descriptor",
            elapsed,
            per_call,
            per_call.as_micros() as f64 / n as f64,
        );

        reset(&pool, "bench_").await;
    }
}

#[ignore]
#[tokio::test]
async fn bench_sync_job_kind_descriptors_scaling() {
    let pool = pool().await;
    awa_model::migrations::run(&pool).await.unwrap();
    reset(&pool, "bench_").await;

    for n in [10, 100, 500, 2000] {
        let descriptors: Vec<NamedJobKindDescriptor> = (0..n)
            .map(|i| NamedJobKindDescriptor {
                kind: format!("bench_kind_{i}"),
                descriptor: JobKindDescriptor::new()
                    .display_name(format!("Kind {i}"))
                    .description("scaling bench")
                    .tag("scaling-bench"),
            })
            .collect();

        sync_job_kind_descriptors(&pool, &descriptors, Duration::from_secs(10))
            .await
            .unwrap();

        let start = Instant::now();
        const ITERS: u32 = 5;
        for _ in 0..ITERS {
            sync_job_kind_descriptors(&pool, &descriptors, Duration::from_secs(10))
                .await
                .unwrap();
        }
        let elapsed = start.elapsed();
        let per_call = elapsed / ITERS;
        eprintln!(
            "sync_job_kind_descriptors n={n}: {:?} total over {ITERS} calls, {:?}/call, {:.1} µs/descriptor",
            elapsed,
            per_call,
            per_call.as_micros() as f64 / n as f64,
        );

        reset(&pool, "bench_").await;
    }
}
