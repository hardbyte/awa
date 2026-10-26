//! One queue through normal operation, an incident and recovery, as
//! `/api/health/queues` reports it (#543).
//!
//! Job rows are real; worker runtimes are seeded snapshots and the per-minute
//! history is seeded rows, so each phase is deterministic without waiting
//! minutes for the maintenance leader.

use awa_model::admin::{
    self, QueueOutcomeCounters, QueueRuntimeConfigSnapshot, QueueRuntimeMode, QueueRuntimeSnapshot,
    RuntimeSnapshotInput, StorageCapability, TransitionRole,
};
use awa_model::queue_history::{self, FleetHistoryRow, QueueHistoryRow};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::{DurationRound, TimeDelta, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tower::util::ServiceExt;
use uuid::Uuid;

const QUEUE: &str = "scenario_email";

async fn setup_pool() -> sqlx::PgPool {
    let pool = awa_testing::setup::setup(4).await;
    awa_model::migrations::run(&pool)
        .await
        .expect("failed to run migrations");
    pool
}

async fn report(app: &axum::Router) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/health/queues?fail_on=critical")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

fn queue_entry(report: &Value) -> Value {
    report["queues"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["queue"] == QUEUE)
        .cloned()
        .expect("scenario queue missing from report")
}

async fn set_waiting(pool: &sqlx::PgPool, count: i64, waited: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(QUEUE)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        r#"
        INSERT INTO awa.jobs (kind, queue, args, state, run_at)
        SELECT 'scenario_job', $1, '{}'::jsonb, 'available', now() - $3::interval
        FROM generate_series(1, $2)
        "#,
    )
    .bind(QUEUE)
    .bind(count)
    .bind(waited)
    .execute(pool)
    .await
    .unwrap();
    admin::flush_dirty_admin_metadata(pool).await.unwrap();
}

/// Four minutes of history ending one minute ago.
async fn set_history(pool: &sqlx::PgPool, depths: [i64; 4], claimed: i64, live_workers: i32) {
    sqlx::query("DELETE FROM awa.queue_history WHERE queue = $1")
        .bind(QUEUE)
        .execute(pool)
        .await
        .unwrap();
    let minute = Utc::now().duration_trunc(TimeDelta::minutes(1)).unwrap();
    for (i, available) in depths.into_iter().enumerate() {
        let bucket = minute - TimeDelta::minutes(4 - i as i64);
        let row = QueueHistoryRow {
            queue: QUEUE.into(),
            bucket,
            available,
            running: 0,
            scheduled: 0,
            retryable: 0,
            failed: 0,
            waiting_external: 0,
            lag_seconds: None,
            paused: false,
            claimed: Some(claimed),
            completed: Some(claimed),
            failed_attempts: Some(0),
            retried: Some(0),
            rate_limited: Some(0),
            claim_wait_ms: Some(claimed * 50),
            live_workers,
            capacity: Some(8),
        };
        let fleet = FleetHistoryRow {
            bucket,
            live_instances: live_workers,
            healthy_instances: live_workers,
            stale_instances: 0,
            leader_instance_id: None,
            versions: vec![],
        };
        queue_history::record_sample(pool, std::slice::from_ref(&row), &fleet)
            .await
            .unwrap();
    }
}

async fn set_workers(pool: &sqlx::PgPool, instance: Uuid, live: bool) {
    sqlx::query("DELETE FROM awa.runtime_instances WHERE queues @> $1::jsonb")
        .bind(serde_json::json!([{ "queue": QUEUE }]))
        .execute(pool)
        .await
        .unwrap();
    if !live {
        return;
    }
    admin::upsert_runtime_snapshot(
        pool,
        &RuntimeSnapshotInput {
            instance_id: instance,
            hostname: Some("scenario-worker".into()),
            pid: 4242,
            version: "0.7.0-test".into(),
            storage_capability: StorageCapability::Canonical,
            transition_role: TransitionRole::Auto,
            started_at: Utc::now(),
            snapshot_interval_ms: 10_000,
            healthy: true,
            postgres_connected: true,
            poll_loop_alive: true,
            heartbeat_alive: true,
            maintenance_alive: true,
            shutting_down: false,
            leader: false,
            global_max_workers: None,
            queues: vec![QueueRuntimeSnapshot {
                queue: QUEUE.into(),
                in_flight: 2,
                overflow_held: None,
                config: QueueRuntimeConfigSnapshot {
                    mode: QueueRuntimeMode::HardReserved,
                    max_workers: Some(8),
                    min_workers: None,
                    weight: None,
                    global_max_workers: None,
                    poll_interval_ms: 200,
                    deadline_duration_secs: 300,
                    priority_aging_interval_secs: 60,
                    claimers: Some(1),
                    claim_batch_size: Some(32),
                    dlq_enabled: Some(true),
                    rate_limit: None,
                },
                counters: Some(QueueOutcomeCounters::default()),
            }],
            queue_descriptor_hashes: HashMap::new(),
            job_kind_descriptor_hashes: HashMap::new(),
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_queue_health_through_normal_incident_and_recovery() {
    let pool = setup_pool().await;
    let instance = Uuid::new_v4();
    let app = awa_ui::router(pool.clone(), Duration::ZERO)
        .await
        .expect("router should initialize");

    // Normal: workers serve the queue and a small backlog holds level.
    set_workers(&pool, instance, true).await;
    set_waiting(&pool, 40, "5 seconds").await;
    set_history(&pool, [40, 42, 38, 41], 300, 1).await;
    let (_, normal) = report(&app).await;
    let q = queue_entry(&normal);
    assert_eq!(q["health"], "steady", "{q}");
    assert_eq!(q["needs_attention"], false);
    assert!(q["limited_by"].is_null());
    assert_eq!(q["flow"]["completed_per_minute"], 300.0);

    // Incident: the workers are gone and work piles up unclaimed.
    set_workers(&pool, instance, false).await;
    set_waiting(&pool, 2400, "3 minutes").await;
    set_history(&pool, [400, 900, 1400, 1900], 0, 0).await;
    let (status, incident) = report(&app).await;
    let q = queue_entry(&incident);
    assert_eq!(q["health"], "stalled", "{q}");
    assert_eq!(q["limited_by"], "no_workers");
    assert_eq!(q["needs_attention"], true);
    assert_eq!(incident["status"], "critical");
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    // Recovery: workers are back and the backlog drains.
    set_workers(&pool, instance, true).await;
    set_waiting(&pool, 1200, "2 minutes").await;
    set_history(&pool, [2400, 2100, 1800, 1500], 600, 1).await;
    let (_, recovery) = report(&app).await;
    let q = queue_entry(&recovery);
    assert_eq!(q["health"], "draining", "{q}");
    assert_eq!(q["needs_attention"], false);
    let clear = q["clear_seconds"].as_f64().unwrap();
    assert!((120.0..600.0).contains(&clear), "clear_seconds {clear}");

    // Cleared: nothing waiting.
    set_waiting(&pool, 0, "0 seconds").await;
    set_history(&pool, [1200, 800, 400, 0], 400, 1).await;
    let (_, cleared) = report(&app).await;
    // With no jobs and no descriptor the queue leaves the queue list
    // altogether; if anything still lists it, it must be idle.
    let still_listed = cleared["queues"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["queue"] == QUEUE)
        .cloned();
    if let Some(q) = still_listed {
        assert_eq!(q["health"], "idle", "{q}");
    }

    set_workers(&pool, instance, false).await;
}
