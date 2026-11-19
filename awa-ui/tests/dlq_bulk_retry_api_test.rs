//! `POST /api/dlq/bulk-retry` with an optional `batch_size`.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::util::ServiceExt;
use uuid::Uuid;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn queue_storage_pool() -> PgPool {
    let pool = awa_testing::setup::setup(4).await;
    awa_testing::setup::activate_queue_storage(&pool).await;
    pool
}

async fn post_json(app: &axum::Router, payload: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dlq/bulk-retry")
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    (
        status,
        serde_json::from_slice(&body).expect("response should deserialize"),
    )
}

async fn seed_dlq(pool: &PgPool, queue: &str, rows: i64) -> Vec<i64> {
    sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries (job_id, kind, queue, priority, dlq_reason, original_run_lease) \
         SELECT nextval('awa.job_id_seq'), 'ui_dlq_job', $1, 2, 'seeded', 1 \
         FROM generate_series(1, $2) RETURNING job_id",
    )
    .bind(queue)
    .bind(rows)
    .fetch_all(pool)
    .await
    .expect("seed dlq rows")
}

#[tokio::test]
async fn bulk_retry_route_honours_an_optional_batch_size() {
    let _guard = TEST_LOCK.lock().await;
    let pool = queue_storage_pool().await;
    let queue = format!(
        "ui_dlq_limit_{}",
        &Uuid::new_v4().simple().to_string()[..12]
    );
    seed_dlq(&pool, &queue, 5).await;
    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");

    let (status, body) = post_json(&app, json!({ "queue": queue, "batch_size": 2 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["count"], 2);
    assert_eq!(body["done"], false);
    let cursor = body["cursor"].clone();

    let (status, body) = post_json(
        &app,
        json!({ "queue": queue, "batch_size": 2, "cursor": cursor }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["count"], 2);

    let (status, body) = post_json(&app, json!({ "queue": queue })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({ "count": 1, "skipped": 0, "conflicts": [], "done": true })
    );

    let key = format!("ui-dlq-{queue}").into_bytes();
    let keyed: i64 = sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries \
             (job_id, kind, queue, priority, dlq_reason, original_run_lease, unique_key, unique_states) \
         VALUES (nextval('awa.job_id_seq'), 'ui_dlq_job', $1, 2, 'seeded', 1, $2, '11000000') \
         RETURNING job_id",
    )
    .bind(&queue)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("seed keyed dlq row");
    seed_dlq(&pool, &queue, 1).await;
    sqlx::query("INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, $2)")
        .bind(&key)
        .bind(i64::MAX - 4)
        .execute(&pool)
        .await
        .expect("live job holds the key");

    let (status, body) = post_json(&app, json!({ "queue": queue, "batch_size": 10 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["count"], 1);
    assert_eq!(body["skipped"], 1);
    assert_eq!(body["conflicts"], json!([keyed]));
    assert_eq!(body["done"], true);

    sqlx::query("DELETE FROM awa.job_unique_claims WHERE unique_key = $1")
        .bind(&key)
        .execute(&pool)
        .await
        .expect("drop claim");
    sqlx::query("DELETE FROM awa.dlq_entries WHERE queue = $1")
        .bind(&queue)
        .execute(&pool)
        .await
        .expect("clear dlq");

    seed_dlq(&pool, &queue, 2).await;
    let mut holder = pool.begin().await.expect("begin holder");
    sqlx::query("SELECT job_id FROM awa.dlq_entries WHERE queue = $1 FOR UPDATE")
        .bind(&queue)
        .fetch_all(&mut *holder)
        .await
        .expect("hold every matching row");
    let (status, body) = post_json(&app, json!({ "queue": queue, "batch_size": 10 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["count"], 0);
    assert_eq!(body["busy"], true);
    assert_eq!(body["done"], false);
    holder.rollback().await.expect("release rows");

    awa_testing::setup::clean_queue(&pool, &queue).await;
}
