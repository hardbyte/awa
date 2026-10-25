//! Per-minute queue and fleet history written by the maintenance leader (#543).
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::admin;
use awa::model::queue_history::{self, FleetHistoryRow, QueueHistoryRow};
use awa::model::{insert_with, InsertOpts};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use awa_testing::TestClient;
use chrono::{DurationRound, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::sync::Mutex;

static LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn setup() -> TestClient {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string());
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await
        .expect("Failed to connect to database");
    let client = TestClient::from_pool(pool).await;
    client.migrate().await.expect("Failed to run migrations");
    client
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct HistoryProbe {
    n: u32,
}

struct HistoryProbeWorker;

#[async_trait::async_trait]
impl Worker for HistoryProbeWorker {
    fn kind(&self) -> &'static str {
        "history_probe"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Ok(JobResult::Completed)
    }
}

#[tokio::test]
async fn test_history_rows_round_trip_and_prune() {
    let _guard = LOCK.lock().await;
    let client = setup().await;
    let pool = client.pool();
    assert!(queue_history::history_installed(pool).await.unwrap());

    let queue = "qh_round_trip";
    sqlx::query("DELETE FROM awa.queue_history WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .unwrap();
    let bucket = (Utc::now() - TimeDelta::hours(3))
        .duration_trunc(TimeDelta::minutes(1))
        .unwrap();
    let row = QueueHistoryRow {
        queue: queue.into(),
        bucket,
        available: 12,
        running: 3,
        scheduled: 1,
        retryable: 2,
        failed: 0,
        waiting_external: 0,
        lag_seconds: Some(4.5),
        paused: false,
        claimed: Some(30),
        completed: Some(29),
        failed_attempts: Some(1),
        retried: None,
        rate_limited: Some(0),
        claim_wait_ms: Some(900),
        live_workers: 2,
        capacity: Some(16),
    };
    let fleet = FleetHistoryRow {
        bucket,
        live_instances: 2,
        healthy_instances: 2,
        stale_instances: 0,
        leader_instance_id: None,
        versions: vec!["0.7.0".into()],
    };
    queue_history::record_sample(pool, std::slice::from_ref(&row), &fleet)
        .await
        .unwrap();
    // A second leader writing the same minute keeps the first row.
    let mut duplicate = row.clone();
    duplicate.available = 999;
    queue_history::record_sample(pool, &[duplicate], &fleet)
        .await
        .unwrap();

    let since = bucket - TimeDelta::minutes(1);
    let history = queue_history::queue_history_since(pool, since)
        .await
        .unwrap();
    assert_eq!(history.get(queue), Some(&vec![row]));
    let fleet_rows = queue_history::fleet_history_since(pool, since)
        .await
        .unwrap();
    assert!(fleet_rows.iter().any(|r| r.bucket == bucket));

    queue_history::prune_history(pool, Duration::from_secs(3600))
        .await
        .unwrap();
    let history = queue_history::queue_history_since(pool, since)
        .await
        .unwrap();
    assert!(!history.contains_key(queue));
}

#[tokio::test]
async fn test_maintenance_leader_writes_queue_and_fleet_history() {
    let _guard = LOCK.lock().await;
    let client = setup().await;
    let pool = client.pool().clone();
    let queue = "qh_leader_sample";
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM awa.queue_history WHERE queue = $1")
        .bind(queue)
        .execute(&pool)
        .await
        .unwrap();
    admin::pause_queue(&pool, queue, Some("queue_history_test"))
        .await
        .unwrap();
    for n in 0..2 {
        insert_with(
            &pool,
            &HistoryProbe { n },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    // Canonical depth comes from the admin count cache, which the leader
    // refreshes every couple of seconds; flush it so the first sample sees
    // the inserts.
    admin::flush_dirty_admin_metadata(&pool).await.unwrap();

    let runtime = Client::builder(pool.clone())
        .queue(queue, QueueConfig::default())
        .register_worker(HistoryProbeWorker)
        .leader_election_interval(Duration::from_millis(100))
        .runtime_snapshot_interval(Duration::from_millis(100))
        .build()
        .unwrap();
    runtime.start().await.unwrap();

    // This runtime samples as soon as it wins the election. If another
    // process on the database holds leadership, its once-a-minute sample
    // covers this queue too, so allow for a full minute.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let since = Utc::now() - TimeDelta::minutes(5);
    let row = loop {
        let history = queue_history::queue_history_since(&pool, since)
            .await
            .unwrap();
        if let Some(row) = history.get(queue).and_then(|rows| rows.last()) {
            break row.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no queue history row for {queue}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(row.available, 2);
    assert!(row.paused);
    let fleet = queue_history::fleet_history_since(&pool, since)
        .await
        .unwrap();
    assert!(fleet.iter().any(|row| row.live_instances >= 1));

    runtime.shutdown(Duration::from_secs(2)).await;
    admin::resume_queue(&pool, queue).await.unwrap();
}
