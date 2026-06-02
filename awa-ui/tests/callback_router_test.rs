//! Tests for the callback-only receiver router (`awa_ui::callback_router`).
//!
//! Verifies the deployable-role split: the router exposes only the three
//! callback ingress endpoints, refuses requests without a valid signature
//! when configured to do so, and does NOT serve admin REST or static UI.

use awa_model::callback_contract;
use awa_ui::callback_router::{CallbackAuth, CallbackReceiverConfig};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use std::time::Duration;
use tower::util::ServiceExt;

async fn setup_pool() -> sqlx::PgPool {
    let pool = awa_testing::setup::setup(4).await;
    awa_model::migrations::run(&pool)
        .await
        .expect("failed to run migrations for callback router tests");
    pool
}

async fn create_waiting_callback(pool: &sqlx::PgPool, queue: &str) -> (i64, uuid::Uuid) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("failed to clean queue");

    let job_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO awa.jobs_hot (
            kind, queue, args, state, attempt, max_attempts, run_lease,
            attempted_at, heartbeat_at, deadline_at
        )
        VALUES (
            'callback_test', $1, '{}'::jsonb, 'running', 1, 5, 1,
            now(), now(), now() + interval '5 minutes'
        )
        RETURNING id
        "#,
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("failed to insert running job");

    let callback_id = awa_model::admin::register_callback(pool, job_id, 1, Duration::from_secs(60))
        .await
        .expect("failed to register callback");

    sqlx::query(
        "UPDATE awa.jobs SET state = 'waiting_external', heartbeat_at = NULL, deadline_at = NULL WHERE id = $1",
    )
    .bind(job_id)
    .execute(pool)
    .await
    .expect("failed to move job to waiting_external");

    (job_id, callback_id)
}

async fn post_json(
    app: &axum::Router,
    path: &str,
    body: Value,
    signature: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");

    if let Some(signature) = signature {
        request = request.header(callback_contract::SIGNATURE_HEADER, signature);
    }

    let response = app
        .clone()
        .oneshot(
            request
                .body(Body::from(body.to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let json = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

async fn get(app: &axum::Router, path: &str) -> StatusCode {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    response.status()
}

#[tokio::test]
async fn callback_router_complete_requires_valid_signature() {
    let pool = setup_pool().await;
    let (job_id, callback_id) = create_waiting_callback(&pool, "callback_router_complete").await;
    let secret = [13u8; 32];
    let app = awa_ui::callback_router(
        pool.clone(),
        CallbackReceiverConfig::new(CallbackAuth::Signed(secret)),
    )
    .await
    .expect("router should build");

    let path = format!("/api/callbacks/{callback_id}/complete");

    let (status, body) = post_json(&app, &path, json!({"payload": {"ok": true}}), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"], "missing X-Awa-Signature header");

    let stale_state: String = sqlx::query_scalar("SELECT state::text FROM awa.jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("job lookup should succeed");
    assert_eq!(stale_state, "waiting_external");

    let signature = callback_contract::sign(&secret, &callback_id.to_string());
    let (status, body) = post_json(
        &app,
        &path,
        json!({"payload": {"ok": true}}),
        Some(&signature),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], "Completed");
}

#[tokio::test]
async fn callback_router_fail_and_heartbeat_routes() {
    let pool = setup_pool().await;
    let (_, callback_id) = create_waiting_callback(&pool, "callback_router_fail").await;
    let secret = [21u8; 32];
    let app = awa_ui::callback_router(
        pool.clone(),
        CallbackReceiverConfig::new(CallbackAuth::Signed(secret)),
    )
    .await
    .expect("router should build");

    let signature = callback_contract::sign(&secret, &callback_id.to_string());

    let heartbeat_path = format!("/api/callbacks/{callback_id}/heartbeat");
    let (status, _) = post_json(
        &app,
        &heartbeat_path,
        json!({"timeout_seconds": 120}),
        Some(&signature),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let fail_path = format!("/api/callbacks/{callback_id}/fail");
    let (status, body) = post_json(
        &app,
        &fail_path,
        json!({"error": "rendered nothing"}),
        Some(&signature),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["state"].as_str().is_some());
}

#[tokio::test]
async fn callback_router_does_not_expose_admin_or_static() {
    let pool = setup_pool().await;
    let app = awa_ui::callback_router(
        pool.clone(),
        CallbackReceiverConfig::new(CallbackAuth::Unsigned),
    )
    .await
    .expect("router should build");

    assert_eq!(get(&app, "/api/stats").await, StatusCode::NOT_FOUND);
    assert_eq!(get(&app, "/api/jobs").await, StatusCode::NOT_FOUND);
    assert_eq!(get(&app, "/api/queues").await, StatusCode::NOT_FOUND);
    assert_eq!(get(&app, "/api/dlq").await, StatusCode::NOT_FOUND);
    assert_eq!(get(&app, "/api/runtime").await, StatusCode::NOT_FOUND);
    assert_eq!(get(&app, "/api/capabilities").await, StatusCode::NOT_FOUND);
    assert_eq!(get(&app, "/").await, StatusCode::NOT_FOUND);
    assert_eq!(get(&app, "/index.html").await, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn callback_router_unsigned_mode_accepts_unauthenticated_requests() {
    let pool = setup_pool().await;
    let (_, callback_id) = create_waiting_callback(&pool, "callback_router_unsigned").await;
    let app = awa_ui::callback_router(
        pool.clone(),
        CallbackReceiverConfig::new(CallbackAuth::Unsigned),
    )
    .await
    .expect("router should build");

    let path = format!("/api/callbacks/{callback_id}/complete");
    let (status, body) = post_json(&app, &path, json!({}), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], "Completed");
}

#[tokio::test]
async fn callback_router_custom_path_prefix() {
    let pool = setup_pool().await;
    let (_, callback_id) = create_waiting_callback(&pool, "callback_router_prefix").await;
    let secret = [42u8; 32];
    let app = awa_ui::callback_router(
        pool.clone(),
        CallbackReceiverConfig::new(CallbackAuth::Signed(secret)).with_path_prefix("/awa-cb"),
    )
    .await
    .expect("router should build");

    let signature = callback_contract::sign(&secret, &callback_id.to_string());

    let custom_path = format!("/awa-cb/{callback_id}/complete");
    let (status, _) = post_json(&app, &custom_path, json!({}), Some(&signature)).await;
    assert_eq!(status, StatusCode::OK);

    let default_path = format!("/api/callbacks/{callback_id}/complete");
    let (status, _) = post_json(&app, &default_path, json!({}), Some(&signature)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
