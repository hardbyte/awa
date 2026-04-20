use awa_model::admin::{
    self, QueueRuntimeConfigSnapshot, QueueRuntimeMode, QueueRuntimeSnapshot, RateLimitSnapshot,
    RuntimeOverview, RuntimeSnapshotInput, StorageCapability,
};
use awa_model::storage::StorageStatusReport;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::{Duration, Utc};
use serde_json::Value;
use std::collections::HashMap;
use tower::util::ServiceExt;
use uuid::Uuid;

async fn setup_pool() -> sqlx::PgPool {
    awa_testing::setup::setup(4).await
}

async fn clean_runtime_snapshots_for_queue(pool: &sqlx::PgPool, queue: &str) {
    let queue_filter = serde_json::json!([{ "queue": queue }]);
    sqlx::query("DELETE FROM awa.runtime_instances WHERE queues @> $1::jsonb")
        .bind(queue_filter)
        .execute(pool)
        .await
        .expect("failed to clean runtime snapshots for test queue");
}

async fn seed_runtime_snapshot(pool: &sqlx::PgPool, queue: &str, hostname: &str) -> Uuid {
    clean_runtime_snapshots_for_queue(pool, queue).await;
    seed_runtime_snapshot_with(
        pool,
        RuntimeSnapshotInput {
            instance_id: Uuid::new_v4(),
            hostname: Some(hostname.to_string()),
            pid: 1234,
            version: "0.4.0-test".to_string(),
            storage_capability: StorageCapability::Canonical,
            started_at: Utc::now(),
            snapshot_interval_ms: 10_000,
            healthy: true,
            postgres_connected: true,
            poll_loop_alive: true,
            heartbeat_alive: true,
            maintenance_alive: true,
            shutting_down: false,
            leader: true,
            global_max_workers: Some(16),
            queues: vec![QueueRuntimeSnapshot {
                queue: queue.to_string(),
                in_flight: 4,
                overflow_held: Some(1),
                config: weighted_config(3),
            }],
            queue_descriptor_hashes: HashMap::new(),
            job_kind_descriptor_hashes: HashMap::new(),
        },
    )
    .await
}

async fn seed_runtime_snapshot_with(pool: &sqlx::PgPool, snapshot: RuntimeSnapshotInput) -> Uuid {
    admin::upsert_runtime_snapshot(pool, &snapshot)
        .await
        .expect("failed to seed runtime snapshot");

    snapshot.instance_id
}

async fn mark_runtime_snapshot_stale(pool: &sqlx::PgPool, instance_id: Uuid) {
    sqlx::query("UPDATE awa.runtime_instances SET last_seen_at = $2 WHERE instance_id = $1")
        .bind(instance_id)
        .bind(Utc::now() - Duration::minutes(10))
        .execute(pool)
        .await
        .expect("failed to age runtime snapshot");
}

fn weighted_config(weight: u32) -> QueueRuntimeConfigSnapshot {
    QueueRuntimeConfigSnapshot {
        mode: QueueRuntimeMode::Weighted,
        max_workers: None,
        min_workers: Some(2),
        weight: Some(weight),
        global_max_workers: Some(16),
        poll_interval_ms: 200,
        deadline_duration_secs: 300,
        priority_aging_interval_secs: 60,
        rate_limit: Some(RateLimitSnapshot {
            max_rate: 5.5,
            burst: 10,
        }),
    }
}

#[tokio::test]
async fn test_get_runtime_endpoint_returns_runtime_overview() {
    let pool = setup_pool().await;
    let queue = "ui_runtime_api_overview";
    let instance_id = seed_runtime_snapshot(&pool, queue, "runtime-api-worker").await;
    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/runtime")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("runtime request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let overview: RuntimeOverview =
        serde_json::from_slice(&body).expect("runtime overview should deserialize");

    let instance = overview
        .instances
        .iter()
        .find(|instance| instance.instance_id == instance_id)
        .expect("seeded runtime instance should be present");
    assert_eq!(instance.hostname.as_deref(), Some("runtime-api-worker"));
    assert_eq!(instance.storage_capability, StorageCapability::Canonical);
    assert!(instance.leader);
    assert!(instance.maintenance_alive);
    assert!(instance
        .queues
        .iter()
        .any(|snapshot| snapshot.queue == queue));
}

#[tokio::test]
async fn test_runtime_endpoint_marks_stale_instances_and_excludes_them_from_live_counts() {
    let pool = setup_pool().await;
    let queue = "ui_runtime_api_stale";
    clean_runtime_snapshots_for_queue(&pool, queue).await;
    let stale_id = seed_runtime_snapshot_with(
        &pool,
        RuntimeSnapshotInput {
            instance_id: Uuid::new_v4(),
            hostname: Some("stale-worker".to_string()),
            pid: 4001,
            version: "0.4.0-test".to_string(),
            storage_capability: StorageCapability::Canonical,
            started_at: Utc::now() - Duration::minutes(30),
            snapshot_interval_ms: 10_000,
            healthy: true,
            postgres_connected: true,
            poll_loop_alive: true,
            heartbeat_alive: true,
            maintenance_alive: true,
            shutting_down: false,
            leader: true,
            global_max_workers: Some(16),
            queues: vec![QueueRuntimeSnapshot {
                queue: queue.to_string(),
                in_flight: 9,
                overflow_held: Some(4),
                config: weighted_config(5),
            }],
            queue_descriptor_hashes: HashMap::new(),
            job_kind_descriptor_hashes: HashMap::new(),
        },
    )
    .await;
    let live_id = seed_runtime_snapshot_with(
        &pool,
        RuntimeSnapshotInput {
            instance_id: Uuid::new_v4(),
            hostname: Some("live-worker".to_string()),
            pid: 4002,
            version: "0.4.0-test".to_string(),
            storage_capability: StorageCapability::Canonical,
            started_at: Utc::now() - Duration::minutes(5),
            snapshot_interval_ms: 10_000,
            healthy: true,
            postgres_connected: true,
            poll_loop_alive: true,
            heartbeat_alive: true,
            maintenance_alive: true,
            shutting_down: false,
            leader: true,
            global_max_workers: Some(16),
            queues: vec![QueueRuntimeSnapshot {
                queue: queue.to_string(),
                in_flight: 4,
                overflow_held: Some(1),
                config: weighted_config(3),
            }],
            queue_descriptor_hashes: HashMap::new(),
            job_kind_descriptor_hashes: HashMap::new(),
        },
    )
    .await;
    mark_runtime_snapshot_stale(&pool, stale_id).await;
    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/runtime")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("runtime request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let overview: RuntimeOverview =
        serde_json::from_slice(&body).expect("runtime overview should deserialize");

    let queue_instances: Vec<_> = overview
        .instances
        .iter()
        .filter(|instance| {
            instance
                .queues
                .iter()
                .any(|snapshot| snapshot.queue == queue)
        })
        .collect();
    assert_eq!(queue_instances.len(), 2);
    assert_eq!(queue_instances[0].instance_id, live_id);
    assert!(!queue_instances[0].stale);
    assert!(queue_instances[0].leader);
    assert_eq!(
        queue_instances[0].storage_capability,
        StorageCapability::Canonical
    );

    let stale = queue_instances
        .iter()
        .find(|instance| instance.instance_id == stale_id)
        .copied()
        .expect("stale instance should be present");
    assert!(stale.stale);
    assert!(stale.leader);
}

#[tokio::test]
async fn test_get_queue_runtime_endpoint_returns_queue_summary() {
    let pool = setup_pool().await;
    let queue = "ui_runtime_api_queue_summary";
    seed_runtime_snapshot(&pool, queue, "runtime-queue-worker").await;
    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/queues/runtime")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("queue runtime request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let payload: Value = serde_json::from_slice(&body).expect("payload should deserialize");
    let queue_summary = payload
        .as_array()
        .expect("queue runtime payload should be an array")
        .iter()
        .find(|entry| entry.get("queue").and_then(Value::as_str) == Some(queue))
        .expect("seeded queue should be present");

    assert_eq!(
        queue_summary.get("instance_count").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        queue_summary.get("total_in_flight").and_then(Value::as_u64),
        Some(4)
    );
    assert_eq!(
        queue_summary
            .get("config")
            .and_then(|cfg| cfg.get("mode"))
            .and_then(Value::as_str),
        Some("weighted")
    );
    assert_eq!(
        queue_summary
            .get("config")
            .and_then(|cfg| cfg.get("rate_limit"))
            .and_then(|cfg| cfg.get("burst"))
            .and_then(Value::as_u64),
        Some(10)
    );
}

#[tokio::test]
async fn test_queue_runtime_endpoint_aggregates_live_instances_and_flags_config_mismatch() {
    let pool = setup_pool().await;
    let queue = "ui_runtime_api_queue_mismatch";
    clean_runtime_snapshots_for_queue(&pool, queue).await;

    seed_runtime_snapshot_with(
        &pool,
        RuntimeSnapshotInput {
            instance_id: Uuid::new_v4(),
            hostname: Some("worker-a".to_string()),
            pid: 5001,
            version: "0.4.0-test".to_string(),
            storage_capability: StorageCapability::Canonical,
            started_at: Utc::now() - Duration::minutes(2),
            snapshot_interval_ms: 10_000,
            healthy: true,
            postgres_connected: true,
            poll_loop_alive: true,
            heartbeat_alive: true,
            maintenance_alive: true,
            shutting_down: false,
            leader: true,
            global_max_workers: Some(16),
            queues: vec![QueueRuntimeSnapshot {
                queue: queue.to_string(),
                in_flight: 4,
                overflow_held: Some(1),
                config: weighted_config(3),
            }],
            queue_descriptor_hashes: HashMap::new(),
            job_kind_descriptor_hashes: HashMap::new(),
        },
    )
    .await;

    seed_runtime_snapshot_with(
        &pool,
        RuntimeSnapshotInput {
            instance_id: Uuid::new_v4(),
            hostname: Some("worker-b".to_string()),
            pid: 5002,
            version: "0.4.0-test".to_string(),
            storage_capability: StorageCapability::Canonical,
            started_at: Utc::now() - Duration::minutes(1),
            snapshot_interval_ms: 10_000,
            healthy: false,
            postgres_connected: true,
            poll_loop_alive: true,
            heartbeat_alive: false,
            maintenance_alive: true,
            shutting_down: false,
            leader: false,
            global_max_workers: Some(16),
            queues: vec![QueueRuntimeSnapshot {
                queue: queue.to_string(),
                in_flight: 6,
                overflow_held: Some(2),
                config: weighted_config(5),
            }],
            queue_descriptor_hashes: HashMap::new(),
            job_kind_descriptor_hashes: HashMap::new(),
        },
    )
    .await;

    let stale_id = seed_runtime_snapshot_with(
        &pool,
        RuntimeSnapshotInput {
            instance_id: Uuid::new_v4(),
            hostname: Some("worker-stale".to_string()),
            pid: 5003,
            version: "0.4.0-test".to_string(),
            storage_capability: StorageCapability::Canonical,
            started_at: Utc::now() - Duration::minutes(20),
            snapshot_interval_ms: 10_000,
            healthy: true,
            postgres_connected: true,
            poll_loop_alive: true,
            heartbeat_alive: true,
            maintenance_alive: true,
            shutting_down: false,
            leader: false,
            global_max_workers: Some(16),
            queues: vec![QueueRuntimeSnapshot {
                queue: queue.to_string(),
                in_flight: 99,
                overflow_held: Some(50),
                config: weighted_config(9),
            }],
            queue_descriptor_hashes: HashMap::new(),
            job_kind_descriptor_hashes: HashMap::new(),
        },
    )
    .await;
    mark_runtime_snapshot_stale(&pool, stale_id).await;

    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/queues/runtime")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("queue runtime request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let summaries: Vec<Value> =
        serde_json::from_slice(&body).expect("queue summaries should deserialize");
    let queue_summary = summaries
        .iter()
        .find(|entry| entry.get("queue").and_then(Value::as_str) == Some(queue))
        .expect("seeded queue should be present");

    assert_eq!(
        queue_summary.get("instance_count").and_then(Value::as_u64),
        Some(3)
    );
    assert_eq!(
        queue_summary.get("live_instances").and_then(Value::as_u64),
        Some(2)
    );
    assert_eq!(
        queue_summary.get("stale_instances").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        queue_summary
            .get("healthy_instances")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        queue_summary.get("total_in_flight").and_then(Value::as_u64),
        Some(10)
    );
    assert_eq!(
        queue_summary
            .get("overflow_held_total")
            .and_then(Value::as_u64),
        Some(3)
    );
    assert_eq!(
        queue_summary
            .get("config_mismatch")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        queue_summary
            .get("config")
            .and_then(|cfg| cfg.get("weight"))
            .and_then(Value::as_u64),
        Some(3)
    );
}

#[tokio::test]
async fn test_storage_endpoint_returns_canonical_baseline_report() {
    let pool = setup_pool().await;
    sqlx::query("SELECT * FROM awa.storage_abort()")
        .execute(&pool)
        .await
        .expect("storage abort reset should succeed");

    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/storage")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let report: StorageStatusReport =
        serde_json::from_slice(&body).expect("storage report should deserialize");

    // On a canonical-only cluster the readiness gates should all be
    // closed — prepared_engine is None and no queue-storage runtimes
    // are reporting. The blocker lists must be populated so operators
    // can see what's missing before kicking off a rollout.
    assert_eq!(report.status.active_engine, "canonical");
    assert!(report.prepared_queue_storage_schema.is_none());
    assert!(!report.prepared_schema_ready);
    assert!(!report.can_enter_mixed_transition);
    assert!(!report.can_finalize);
    assert!(
        !report.enter_mixed_transition_blockers.is_empty(),
        "canonical baseline should list at least one enter-mixed-transition blocker"
    );
    assert!(
        !report.finalize_blockers.is_empty(),
        "canonical baseline should list at least one finalize blocker"
    );
}
