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

    let app = awa_ui::router(pool.clone());
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

    // Verify it's parseable as a datetime and is in the future
    let next_fire_str = next_fire.unwrap().as_str().unwrap();
    let next_fire_dt = chrono::DateTime::parse_from_rfc3339(next_fire_str)
        .expect("next_fire_at should be valid RFC3339");
    assert!(
        next_fire_dt > chrono::Utc::now(),
        "next_fire_at should be in the future, got {next_fire_dt}"
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
