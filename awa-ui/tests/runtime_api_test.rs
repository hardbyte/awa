use awa_model::admin::{
    self, QueueRuntimeConfigSnapshot, QueueRuntimeMode, QueueRuntimeSnapshot, RateLimitSnapshot,
    RuntimeOverview, RuntimeSnapshotInput,
};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::Utc;
use serde_json::Value;
use tower::util::ServiceExt;
use uuid::Uuid;

async fn setup_pool() -> sqlx::PgPool {
    awa_testing::setup::setup(4).await
}

async fn seed_runtime_snapshot(pool: &sqlx::PgPool, queue: &str, hostname: &str) -> Uuid {
    let queue_filter = serde_json::json!([{ "queue": queue }]);
    sqlx::query("DELETE FROM awa.runtime_instances WHERE queues @> $1::jsonb")
        .bind(queue_filter)
        .execute(pool)
        .await
        .expect("failed to clean runtime snapshots for test queue");

    let instance_id = Uuid::new_v4();
    let snapshot = RuntimeSnapshotInput {
        instance_id,
        hostname: Some(hostname.to_string()),
        pid: 1234,
        version: "0.4.0-test".to_string(),
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
            config: QueueRuntimeConfigSnapshot {
                mode: QueueRuntimeMode::Weighted,
                max_workers: None,
                min_workers: Some(2),
                weight: Some(3),
                global_max_workers: Some(16),
                poll_interval_ms: 200,
                deadline_duration_secs: 300,
                priority_aging_interval_secs: 60,
                rate_limit: Some(RateLimitSnapshot {
                    max_rate: 5.5,
                    burst: 10,
                }),
            },
        }],
    };

    admin::upsert_runtime_snapshot(pool, &snapshot)
        .await
        .expect("failed to seed runtime snapshot");

    instance_id
}

#[tokio::test]
async fn test_get_runtime_endpoint_returns_runtime_overview() {
    let pool = setup_pool().await;
    let queue = "ui_runtime_api_overview";
    let instance_id = seed_runtime_snapshot(&pool, queue, "runtime-api-worker").await;
    let app = awa_ui::router(pool.clone());

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
    assert!(instance.leader);
    assert!(instance.maintenance_alive);
    assert!(instance
        .queues
        .iter()
        .any(|snapshot| snapshot.queue == queue));
}

#[tokio::test]
async fn test_get_queue_runtime_endpoint_returns_queue_summary() {
    let pool = setup_pool().await;
    let queue = "ui_runtime_api_queue_summary";
    seed_runtime_snapshot(&pool, queue, "runtime-queue-worker").await;
    let app = awa_ui::router(pool.clone());

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
