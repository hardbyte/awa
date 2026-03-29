use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use std::time::Duration;
use tower::util::ServiceExt;

async fn setup_pool() -> sqlx::PgPool {
    let pool = awa_testing::setup::setup(4).await;
    awa_model::migrations::run(&pool)
        .await
        .expect("failed to run migrations for callback API tests");
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
        request = request.header("X-Awa-Signature", signature);
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
    let json = serde_json::from_slice(&body).expect("response should deserialize");
    (status, json)
}

#[tokio::test]
async fn callback_complete_requires_valid_signature_when_configured() {
    let pool = setup_pool().await;
    let (job_id, callback_id) = create_waiting_callback(&pool, "callback_auth").await;
    let secret = [7u8; 32];
    let app = awa_ui::router_with_callback_secret(pool.clone(), Duration::ZERO, Some(secret))
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

    let signature = blake3::keyed_hash(&secret, callback_id.to_string().as_bytes())
        .to_hex()
        .to_string();
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
async fn callback_heartbeat_rejects_invalid_timeout() {
    let pool = setup_pool().await;
    let (_, callback_id) = create_waiting_callback(&pool, "callback_timeout_validation").await;
    let app = awa_ui::router(pool.clone(), Duration::ZERO)
        .await
        .expect("router should build");

    let path = format!("/api/callbacks/{callback_id}/heartbeat");
    let (status, body) = post_json(&app, &path, json!({"timeout_seconds": -1.0}), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body["error"],
        "validation error: timeout_seconds must be a finite, non-negative number"
    );
}
