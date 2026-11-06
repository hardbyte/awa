//! Heartbeat pass cost against in-flight claim count. Ignored by default:
//!
//! ```text
//! DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
//!   cargo test --release -p awa --test heartbeat_pass_benchmark_test -- --ignored --nocapture
//! ```
//!
//! Each size claims that many jobs through the claim path the dispatcher uses,
//! in claims of `AWA_HEARTBEAT_BENCH_CLAIM_BATCH` jobs (default 16, as a busy
//! runtime refills a few freed workers at a time), and times a heartbeat pass
//! over them in the heartbeat service's 500-attempt chunks.
//! `AWA_HEARTBEAT_BENCH_SIZES` overrides the sizes.

use awa::model::queue_storage::ClaimHeartbeatRef;
use awa::model::{insert, migrations, QueueStorage, QueueStorageConfig};
use awa::{audited_sql, InsertOpts, JobArgs};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
use std::time::{Duration, Instant};

const HEARTBEAT_CHUNK: usize = 500;
const PASSES: usize = 6;

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct HeartbeatBenchJob {
    n: i64,
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn with_database(url: &str, database: &str) -> String {
    let (base, _) = url
        .rsplit_once('/')
        .expect("database URL has a database name");
    format!("{base}/{database}")
}

async fn fresh_database(name: &str) -> PgPool {
    // Database names come only from the fixed prefix and this process ID.
    let mut admin = PgConnection::connect(&with_database(&database_url(), "postgres"))
        .await
        .expect("admin connection");
    sqlx::raw_sql(audited_sql(format!(
        "DROP DATABASE IF EXISTS {name} WITH (FORCE)"
    )))
    .execute(&mut admin)
    .await
    .expect("drop leftover database");
    sqlx::raw_sql(audited_sql(format!("CREATE DATABASE {name}")))
        .execute(&mut admin)
        .await
        .expect("create database");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&with_database(&database_url(), name))
        .await
        .expect("bench pool");
    migrations::run(&pool).await.expect("migrate");
    pool
}

async fn drop_database(name: &str) {
    // Database names come only from the fixed prefix and this process ID.
    let mut admin = PgConnection::connect(&with_database(&database_url(), "postgres"))
        .await
        .expect("admin connection");
    let _ = sqlx::raw_sql(audited_sql(format!(
        "DROP DATABASE IF EXISTS {name} WITH (FORCE)"
    )))
    .execute(&mut admin)
    .await;
}

#[tokio::test]
#[ignore = "benchmark; creates and drops its own database"]
async fn heartbeat_pass_time_by_in_flight_claims() {
    let sizes: Vec<usize> = std::env::var("AWA_HEARTBEAT_BENCH_SIZES")
        .ok()
        .map(|sizes| {
            sizes
                .split(',')
                .filter_map(|size| size.trim().parse().ok())
                .collect()
        })
        .unwrap_or_else(|| vec![1_000, 10_000, 25_000]);
    let claim_batch: i64 = std::env::var("AWA_HEARTBEAT_BENCH_CLAIM_BATCH")
        .ok()
        .and_then(|size| size.parse().ok())
        .unwrap_or(16);
    let server: String = {
        let pool = fresh_database(&format!("awa_hb_bench_{}", std::process::id())).await;
        let version = sqlx::query_scalar("SHOW server_version")
            .fetch_one(&pool)
            .await
            .expect("server version");
        pool.close().await;
        version
    };
    for size in sizes {
        let name = format!("awa_hb_bench_{}", std::process::id());
        let pool = fresh_database(&name).await;
        let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
        store.install(&pool).await.expect("install queue storage");
        awa_testing::setup::activate_queue_storage(&pool).await;
        let queue = "heartbeat_bench";
        for first in (0..size as i64).step_by(1_000) {
            let params: Vec<_> = (first..(first + 1_000).min(size as i64))
                .map(|n| {
                    insert::params_with(
                        &HeartbeatBenchJob { n },
                        InsertOpts {
                            queue: queue.to_string(),
                            ..Default::default()
                        },
                    )
                    .expect("params")
                })
                .collect();
            store
                .enqueue_params_batch(&pool, &params)
                .await
                .expect("enqueue");
        }
        let mut claimed = Vec::with_capacity(size);
        while claimed.len() < size {
            let batch = store
                .claim_runtime_batch(&pool, queue, claim_batch, Duration::ZERO)
                .await
                .expect("claim");
            assert!(!batch.is_empty(), "claimed {} of {size}", claimed.len());
            claimed.extend(batch);
        }
        sqlx::query("ANALYZE")
            .execute(&pool)
            .await
            .expect("analyze");

        // The first pass creates every attempt's heartbeat row; later passes
        // refresh them.
        let mut passes = Vec::with_capacity(PASSES);
        let mut refreshed = 0;
        for _ in 0..PASSES {
            let started = Instant::now();
            refreshed = heartbeat_pass(&store, &pool, &claimed).await;
            passes.push(started.elapsed());
        }
        let first = passes[0];
        let mut later = passes[1..].to_vec();
        later.sort_unstable();
        eprintln!(
            "heartbeat pass: server={server} in_flight={size} claim_batch={claim_batch} \
             refreshed={refreshed} first_ms={} median_ms={} max_ms={}",
            first.as_millis(),
            later[later.len() / 2].as_millis(),
            later[later.len() - 1].as_millis()
        );
        assert_eq!(refreshed, size, "every in-flight attempt is refreshed");
        pool.close().await;
        drop_database(&name).await;
    }
}

async fn heartbeat_pass(
    store: &QueueStorage,
    pool: &PgPool,
    claimed: &[awa::model::ClaimedRuntimeJob],
) -> usize {
    // As the heartbeat service splits them: compact receipt claims by their
    // batch identity, anything else by attempt key.
    let mut claims = Vec::new();
    let mut plain = Vec::new();
    for job in claimed {
        match ClaimHeartbeatRef::from_claim(job.job.id, job.job.run_lease, &job.claim) {
            Some(claim) => claims.push(claim),
            None => plain.push((job.job.id, job.job.run_lease)),
        }
    }
    let mut refreshed = 0;
    for chunk in claims.chunks(HEARTBEAT_CHUNK) {
        refreshed += store
            .heartbeat_claims_batch(pool, chunk)
            .await
            .expect("heartbeat claims");
    }
    for chunk in plain.chunks(HEARTBEAT_CHUNK) {
        refreshed += store.heartbeat_batch(pool, chunk).await.expect("heartbeat");
    }
    refreshed
}
