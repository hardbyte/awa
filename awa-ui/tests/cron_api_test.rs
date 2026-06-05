//! API test for GET /api/cron — verifies next_fire_at is present.
//!
//! Requires DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::util::ServiceExt;

async fn setup_pool() -> sqlx::PgPool {
    awa_testing::setup::setup(2).await
}

/// Seed a cron schedule directly into the DB for testing.
async fn seed_cron_schedule(pool: &sqlx::PgPool, name: &str) {
    // Clean any existing schedule with this name
    sqlx::query("DELETE FROM awa.cron_jobs WHERE name = $1")
        .bind(name)
        .execute(pool)
        .await
        .unwrap();

    sqlx::query(
        r#"INSERT INTO awa.cron_jobs (name, cron_expr, timezone, kind, queue, args, priority, max_attempts, tags, metadata)
           VALUES ($1, '* * * * *', 'UTC', 'test_cron_job', 'default', '{}'::jsonb, 2, 25, '{}', '{}'::jsonb)"#,
    )
    .bind(name)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn test_cron_api_includes_next_fire_at() {
    let pool = setup_pool().await;
    let schedule_name = "cron_api_test_next_fire";
    seed_cron_schedule(&pool, schedule_name).await;

    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/cron")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let cron_jobs: Vec<Value> = serde_json::from_slice(&body).unwrap();

    // Find our test schedule
    let test_schedule = cron_jobs
        .iter()
        .find(|j| j["name"] == schedule_name)
        .expect("test cron schedule not found in response");

    // Verify next_fire_at is present and is a valid timestamp
    let next_fire = test_schedule.get("next_fire_at");
    assert!(
        next_fire.is_some(),
        "next_fire_at field missing from cron response"
    );
    assert!(
        next_fire.unwrap().is_string(),
        "next_fire_at should be a timestamp string, got: {next_fire:?}"
    );

    // Allow a small boundary slack because next_fire_at is inclusive when the
    // request lands exactly on a schedule tick.
    let next_fire_str = next_fire.unwrap().as_str().unwrap();
    let next_fire_dt = chrono::DateTime::parse_from_rfc3339(next_fire_str)
        .expect("next_fire_at should be valid RFC3339");
    assert!(
        next_fire_dt >= chrono::Utc::now() - chrono::Duration::seconds(1),
        "next_fire_at should not be stale, got {next_fire_dt}"
    );

    // Verify the standard CronJobRow fields are still present (flatten works)
    assert!(test_schedule.get("cron_expr").is_some());
    assert!(test_schedule.get("timezone").is_some());
    assert!(test_schedule.get("last_enqueued_at").is_some());

    // Clean up
    sqlx::query("DELETE FROM awa.cron_jobs WHERE name = $1")
        .bind(schedule_name)
        .execute(&pool)
        .await
        .unwrap();
}

async fn paused_at(pool: &sqlx::PgPool, name: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>(
        "SELECT paused_at FROM awa.cron_jobs WHERE name = $1",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn test_pause_endpoint_pauses_schedule() {
    let pool = setup_pool().await;
    let schedule = "cron_api_pause_ok";
    seed_cron_schedule(&pool, schedule).await;

    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/cron/{schedule}/pause"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"paused_by": "ops"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    assert!(
        paused_at(&pool, schedule).await.is_some(),
        "pause endpoint should set paused_at"
    );
    let by: Option<String> =
        sqlx::query_scalar("SELECT paused_by FROM awa.cron_jobs WHERE name=$1")
            .bind(schedule)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(by.as_deref(), Some("ops"));

    sqlx::query("DELETE FROM awa.cron_jobs WHERE name = $1")
        .bind(schedule)
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_pause_endpoint_no_body_ok() {
    let pool = setup_pool().await;
    let schedule = "cron_api_pause_no_body";
    seed_cron_schedule(&pool, schedule).await;

    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/cron/{schedule}/pause"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "pause without a JSON body must succeed (paused_by is optional)"
    );
    assert!(paused_at(&pool, schedule).await.is_some());

    sqlx::query("DELETE FROM awa.cron_jobs WHERE name = $1")
        .bind(schedule)
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_resume_endpoint_clears_pause() {
    let pool = setup_pool().await;
    let schedule = "cron_api_resume_ok";
    seed_cron_schedule(&pool, schedule).await;

    sqlx::query("UPDATE awa.cron_jobs SET paused_at = now(), paused_by = 'seed' WHERE name = $1")
        .bind(schedule)
        .execute(&pool)
        .await
        .unwrap();
    assert!(paused_at(&pool, schedule).await.is_some());

    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/cron/{schedule}/resume"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        paused_at(&pool, schedule).await.is_none(),
        "resume endpoint should clear paused_at"
    );

    sqlx::query("DELETE FROM awa.cron_jobs WHERE name = $1")
        .bind(schedule)
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_pause_unknown_schedule_returns_404() {
    let pool = setup_pool().await;
    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/cron/cron_api_pause_unknown_xyz/pause")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_list_response_includes_paused_state() {
    let pool = setup_pool().await;
    let schedule = "cron_api_list_paused";
    seed_cron_schedule(&pool, schedule).await;
    sqlx::query("UPDATE awa.cron_jobs SET paused_at = now(), paused_by = 'seed' WHERE name = $1")
        .bind(schedule)
        .execute(&pool)
        .await
        .unwrap();

    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/cron")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let rows: Vec<Value> = serde_json::from_slice(&body).unwrap();

    let row = rows
        .iter()
        .find(|r| r["name"] == schedule)
        .expect("test schedule should appear in list");
    assert!(
        row.get("paused_at").map(|v| !v.is_null()).unwrap_or(false),
        "list response must surface paused_at: {row}"
    );
    assert_eq!(row.get("paused_by").and_then(|v| v.as_str()), Some("seed"));

    sqlx::query("DELETE FROM awa.cron_jobs WHERE name = $1")
        .bind(schedule)
        .execute(&pool)
        .await
        .unwrap();
}
