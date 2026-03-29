//! Integration tests for HttpWorker (serverless job dispatch).
//!
//! Requires `http-worker` feature and DATABASE_URL.
//! Uses a mock HTTP server (axum) to simulate function endpoints.

#![cfg(feature = "http-worker")]

use awa::model::admin;
use awa::{HttpWorker, HttpWorkerConfig, HttpWorkerMode, JobArgs, JobState};
use awa_testing::TestClient;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup() -> TestClient {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");

    let client = TestClient::from_pool(pool).await;
    client.migrate().await.expect("Failed to run migrations");
    client
}

async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue");
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct HttpTask {
    pub task_id: String,
}

/// Spin up a mock function endpoint that captures requests.
async fn mock_function_server(handler: axum::Router) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, handler).await.unwrap();
    });
    (addr, handle)
}

// ── Sync mode tests ─────────────────────────────────────────────

/// HW1: Sync mode — function returns 200 → job completes.
#[tokio::test]
async fn test_hw1_sync_mode_success() {
    let client = setup().await;
    let queue = "hw_sync_ok";
    clean_queue(client.pool(), queue).await;

    let call_count = Arc::new(AtomicU32::new(0));
    let count = call_count.clone();

    let app = axum::Router::new().route(
        "/function",
        axum::routing::post(move |body: axum::Json<serde_json::Value>| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                assert!(body.get("job_id").is_some());
                assert!(body.get("kind").is_some());
                assert!(body.get("args").is_some());
                axum::http::StatusCode::OK
            }
        }),
    );

    let (addr, _handle) = mock_function_server(app).await;

    let worker = HttpWorker::new(
        "http_task".to_string(),
        HttpWorkerConfig {
            url: format!("http://{addr}/function"),
            mode: HttpWorkerMode::Sync {
                response_timeout: Duration::from_secs(5),
            },
            ..Default::default()
        },
    );

    awa::insert_with(
        client.pool(),
        &HttpTask {
            task_id: "sync-1".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();
    assert!(
        result.is_completed(),
        "Sync 200 should complete: {result:?}"
    );
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

/// HW2: Sync mode — function returns 500 → job retried.
#[tokio::test]
async fn test_hw2_sync_mode_server_error() {
    let client = setup().await;
    let queue = "hw_sync_500";
    clean_queue(client.pool(), queue).await;

    let app = axum::Router::new().route(
        "/function",
        axum::routing::post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
    );

    let (addr, _handle) = mock_function_server(app).await;

    let worker = HttpWorker::new(
        "http_task".to_string(),
        HttpWorkerConfig {
            url: format!("http://{addr}/function"),
            mode: HttpWorkerMode::Sync {
                response_timeout: Duration::from_secs(5),
            },
            ..Default::default()
        },
    );

    awa::insert_with(
        client.pool(),
        &HttpTask {
            task_id: "sync-500".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();
    // 500 maps to retryable via retryable_msg
    assert!(
        matches!(result, awa_testing::WorkResult::Retryable(_)),
        "500 should be retryable: {result:?}"
    );
}

/// HW3: Sync mode — function returns 400 → job fails terminally.
#[tokio::test]
async fn test_hw3_sync_mode_client_error() {
    let client = setup().await;
    let queue = "hw_sync_400";
    clean_queue(client.pool(), queue).await;

    let app = axum::Router::new().route(
        "/function",
        axum::routing::post(|| async { axum::http::StatusCode::BAD_REQUEST }),
    );

    let (addr, _handle) = mock_function_server(app).await;

    let worker = HttpWorker::new(
        "http_task".to_string(),
        HttpWorkerConfig {
            url: format!("http://{addr}/function"),
            mode: HttpWorkerMode::Sync {
                response_timeout: Duration::from_secs(5),
            },
            ..Default::default()
        },
    );

    awa::insert_with(
        client.pool(),
        &HttpTask {
            task_id: "sync-400".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_failed(), "400 should be terminal: {result:?}");
}

// ── Async mode tests ────────────────────────────────────────────

/// HW4: Async mode — function returns 202, then calls back → job completes.
#[tokio::test]
async fn test_hw4_async_mode_callback_complete() {
    let client = setup().await;
    let queue = "hw_async_ok";
    clean_queue(client.pool(), queue).await;

    // Capture the callback_id from the request
    let captured_callback_id = Arc::new(tokio::sync::Mutex::new(None::<String>));
    let captured = captured_callback_id.clone();

    let app = axum::Router::new().route(
        "/function",
        axum::routing::post(move |body: axum::Json<serde_json::Value>| {
            let captured = captured.clone();
            async move {
                let cb_id = body
                    .get("callback_id")
                    .and_then(|v| v.as_str())
                    .unwrap()
                    .to_string();
                *captured.lock().await = Some(cb_id);
                axum::http::StatusCode::ACCEPTED
            }
        }),
    );

    let (addr, _handle) = mock_function_server(app).await;

    let worker = HttpWorker::new(
        "http_task".to_string(),
        HttpWorkerConfig {
            url: format!("http://{addr}/function"),
            mode: HttpWorkerMode::Async,
            callback_timeout: Duration::from_secs(60),
            callback_base_url: Some(format!("http://{addr}")),
            ..Default::default()
        },
    );

    let job = awa::insert_with(
        client.pool(),
        &HttpTask {
            task_id: "async-1".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();
    assert!(
        result.is_waiting_external(),
        "Async 202 should park: {result:?}"
    );

    // Verify callback_id was sent to the function
    let callback_id_str = captured_callback_id.lock().await.clone().unwrap();
    let callback_uuid = uuid::Uuid::parse_str(&callback_id_str).unwrap();

    // Verify job is in waiting_external
    let waiting = client.get_job(job.id).await.unwrap();
    assert_eq!(waiting.state, JobState::WaitingExternal);
    assert_eq!(waiting.callback_id.unwrap(), callback_uuid);

    // External system calls back
    let completed = admin::complete_external(
        client.pool(),
        callback_uuid,
        Some(serde_json::json!({"result": "success"})),
        None,
    )
    .await
    .unwrap();
    assert_eq!(completed.state, JobState::Completed);
}

/// HW5: Async mode — function returns 500 → job retried (never parks).
#[tokio::test]
async fn test_hw5_async_mode_function_rejects() {
    let client = setup().await;
    let queue = "hw_async_reject";
    clean_queue(client.pool(), queue).await;

    let app = axum::Router::new().route(
        "/function",
        axum::routing::post(|| async {
            (axum::http::StatusCode::SERVICE_UNAVAILABLE, "overloaded")
        }),
    );

    let (addr, _handle) = mock_function_server(app).await;

    let worker = HttpWorker::new(
        "http_task".to_string(),
        HttpWorkerConfig {
            url: format!("http://{addr}/function"),
            mode: HttpWorkerMode::Async,
            callback_timeout: Duration::from_secs(60),
            ..Default::default()
        },
    );

    awa::insert_with(
        client.pool(),
        &HttpTask {
            task_id: "async-reject".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();
    assert!(
        matches!(result, awa_testing::WorkResult::Retryable(_)),
        "503 should be retryable: {result:?}"
    );
}

/// HW6: Async mode — function unreachable → job retried.
#[tokio::test]
async fn test_hw6_async_mode_unreachable() {
    let client = setup().await;
    let queue = "hw_async_unreachable";
    clean_queue(client.pool(), queue).await;

    let worker = HttpWorker::new(
        "http_task".to_string(),
        HttpWorkerConfig {
            url: "http://127.0.0.1:1/function".to_string(), // Port 1 — unreachable
            mode: HttpWorkerMode::Async,
            callback_timeout: Duration::from_secs(60),
            request_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    );

    awa::insert_with(
        client.pool(),
        &HttpTask {
            task_id: "async-unreachable".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();
    assert!(
        matches!(result, awa_testing::WorkResult::Retryable(_)),
        "Connection failure should be retryable: {result:?}"
    );
}

// ── HMAC / headers tests ────────────────────────────────────────

/// HW7: Custom headers are sent with the request.
#[tokio::test]
async fn test_hw7_custom_headers() {
    let client = setup().await;
    let queue = "hw_headers";
    clean_queue(client.pool(), queue).await;

    let received_auth = Arc::new(tokio::sync::Mutex::new(None::<String>));
    let captured = received_auth.clone();

    let app = axum::Router::new().route(
        "/function",
        axum::routing::post(move |headers: axum::http::HeaderMap| {
            let captured = captured.clone();
            async move {
                let auth = headers
                    .get("Authorization")
                    .map(|v| v.to_str().unwrap().to_string());
                *captured.lock().await = auth;
                axum::http::StatusCode::OK
            }
        }),
    );

    let (addr, _handle) = mock_function_server(app).await;

    let mut headers = std::collections::HashMap::new();
    headers.insert("Authorization".to_string(), "Bearer my-token".to_string());

    let worker = HttpWorker::new(
        "http_task".to_string(),
        HttpWorkerConfig {
            url: format!("http://{addr}/function"),
            mode: HttpWorkerMode::Sync {
                response_timeout: Duration::from_secs(5),
            },
            headers,
            ..Default::default()
        },
    );

    awa::insert_with(
        client.pool(),
        &HttpTask {
            task_id: "headers-1".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();

    let auth = received_auth.lock().await.clone();
    assert_eq!(auth.as_deref(), Some("Bearer my-token"));
}

/// HW8: HMAC signature is sent and verifiable.
#[tokio::test]
async fn test_hw8_hmac_signature() {
    let client = setup().await;
    let queue = "hw_hmac";
    clean_queue(client.pool(), queue).await;

    let secret: [u8; 32] = [42u8; 32];
    let captured_sig = Arc::new(tokio::sync::Mutex::new(None::<String>));
    let captured_cb = Arc::new(tokio::sync::Mutex::new(None::<String>));
    let sig_ref = captured_sig.clone();
    let cb_ref = captured_cb.clone();

    let app = axum::Router::new().route(
        "/function",
        axum::routing::post(
            move |headers: axum::http::HeaderMap, body: axum::Json<serde_json::Value>| {
                let sig_ref = sig_ref.clone();
                let cb_ref = cb_ref.clone();
                async move {
                    let sig = headers
                        .get("X-Awa-Signature")
                        .map(|v| v.to_str().unwrap().to_string());
                    let cb_id = body
                        .get("callback_id")
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    *sig_ref.lock().await = sig;
                    *cb_ref.lock().await = cb_id;
                    axum::http::StatusCode::ACCEPTED
                }
            },
        ),
    );

    let (addr, _handle) = mock_function_server(app).await;

    let worker = HttpWorker::new(
        "http_task".to_string(),
        HttpWorkerConfig {
            url: format!("http://{addr}/function"),
            mode: HttpWorkerMode::Async,
            callback_timeout: Duration::from_secs(60),
            hmac_secret: Some(secret),
            ..Default::default()
        },
    );

    awa::insert_with(
        client.pool(),
        &HttpTask {
            task_id: "hmac-1".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();

    let sig = captured_sig
        .lock()
        .await
        .clone()
        .expect("signature should be present");
    let cb_id = captured_cb
        .lock()
        .await
        .clone()
        .expect("callback_id should be present");

    // Verify the signature matches what verify_callback_signature expects
    assert!(
        awa_worker::http_worker::verify_callback_signature(&secret, &cb_id, &sig),
        "Signature should verify: cb_id={cb_id}, sig={sig}"
    );

    // Wrong secret should fail
    let wrong_secret = [99u8; 32];
    assert!(
        !awa_worker::http_worker::verify_callback_signature(&wrong_secret, &cb_id, &sig),
        "Wrong secret should not verify"
    );
}

/// HW9: Callback URL is correctly constructed.
#[tokio::test]
async fn test_hw9_callback_url() {
    let client = setup().await;
    let queue = "hw_callback_url";
    clean_queue(client.pool(), queue).await;

    let captured_url = Arc::new(tokio::sync::Mutex::new(None::<String>));
    let url_ref = captured_url.clone();

    let app = axum::Router::new().route(
        "/function",
        axum::routing::post(move |body: axum::Json<serde_json::Value>| {
            let url_ref = url_ref.clone();
            async move {
                let url = body
                    .get("callback_url")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                *url_ref.lock().await = url;
                axum::http::StatusCode::ACCEPTED
            }
        }),
    );

    let (addr, _handle) = mock_function_server(app).await;

    let worker = HttpWorker::new(
        "http_task".to_string(),
        HttpWorkerConfig {
            url: format!("http://{addr}/function"),
            mode: HttpWorkerMode::Async,
            callback_timeout: Duration::from_secs(60),
            callback_base_url: Some("https://awa.example.com".into()),
            ..Default::default()
        },
    );

    awa::insert_with(
        client.pool(),
        &HttpTask {
            task_id: "url-1".into(),
        },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&worker, Some(queue))
        .await
        .unwrap();

    let url = captured_url
        .lock()
        .await
        .clone()
        .expect("callback_url should be present");
    assert!(
        url.starts_with("https://awa.example.com/api/callbacks/"),
        "URL should start with base: {url}"
    );
    assert!(
        url.ends_with("/complete"),
        "URL should end with /complete: {url}"
    );
}
