use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tower::util::ServiceExt;
use uuid::Uuid;

fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn setup_pool() -> sqlx::PgPool {
    let pool = awa_testing::setup::setup(4).await;
    awa_model::migrations::run(&pool)
        .await
        .expect("failed to run migrations for admin API tests");
    pool
}

async fn clean_jobs(pool: &sqlx::PgPool, queues: &[&str], kinds: &[&str]) {
    if !queues.is_empty() {
        sqlx::query("DELETE FROM awa.jobs WHERE queue = ANY($1)")
            .bind(queues)
            .execute(pool)
            .await
            .expect("failed to clean jobs by queue");

        sqlx::query("DELETE FROM awa.queue_meta WHERE queue = ANY($1)")
            .bind(queues)
            .execute(pool)
            .await
            .expect("failed to clean queue meta");

        sqlx::query("DELETE FROM awa.queue_state_counts WHERE queue = ANY($1)")
            .bind(queues)
            .execute(pool)
            .await
            .expect("failed to clean queue state counts");

        sqlx::query("DELETE FROM awa.job_queue_catalog WHERE queue = ANY($1)")
            .bind(queues)
            .execute(pool)
            .await
            .expect("failed to clean queue catalog");
    }

    if !kinds.is_empty() {
        sqlx::query("DELETE FROM awa.jobs WHERE kind = ANY($1)")
            .bind(kinds)
            .execute(pool)
            .await
            .expect("failed to clean jobs by kind");

        sqlx::query("DELETE FROM awa.job_kind_catalog WHERE kind = ANY($1)")
            .bind(kinds)
            .execute(pool)
            .await
            .expect("failed to clean kind catalog");
    }
}

async fn get_json(app: &axum::Router, path: &str) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    serde_json::from_slice(&body).expect("response should deserialize")
}

async fn seed_scale_fixture(pool: &sqlx::PgPool, prefix: &str) {
    let queue_pattern = format!("{prefix}queue_%");
    let kind_pattern = format!("{prefix}kind_%");
    let mut conn = pool.acquire().await.expect("pool acquire should succeed");

    sqlx::query("SET session_replication_role = replica")
        .execute(&mut *conn)
        .await
        .expect("disable triggers should succeed");

    sqlx::query("DELETE FROM awa.jobs_hot WHERE queue LIKE $1 OR kind LIKE $2")
        .bind(&queue_pattern)
        .bind(&kind_pattern)
        .execute(&mut *conn)
        .await
        .expect("cleanup jobs_hot should succeed");
    sqlx::query("DELETE FROM awa.scheduled_jobs WHERE queue LIKE $1 OR kind LIKE $2")
        .bind(&queue_pattern)
        .bind(&kind_pattern)
        .execute(&mut *conn)
        .await
        .expect("cleanup scheduled_jobs should succeed");

    sqlx::query("SET session_replication_role = DEFAULT")
        .execute(&mut *conn)
        .await
        .expect("enable triggers should succeed");

    sqlx::query("DELETE FROM awa.queue_state_counts WHERE queue LIKE $1")
        .bind(&queue_pattern)
        .execute(&mut *conn)
        .await
        .expect("cleanup queue_state_counts should succeed");
    sqlx::query("DELETE FROM awa.job_queue_catalog WHERE queue LIKE $1")
        .bind(&queue_pattern)
        .execute(&mut *conn)
        .await
        .expect("cleanup job_queue_catalog should succeed");
    sqlx::query("DELETE FROM awa.job_kind_catalog WHERE kind LIKE $1")
        .bind(&kind_pattern)
        .execute(&mut *conn)
        .await
        .expect("cleanup job_kind_catalog should succeed");

    sqlx::query("SET session_replication_role = replica")
        .execute(&mut *conn)
        .await
        .expect("disable triggers should succeed");

    sqlx::query(
        r#"
        INSERT INTO awa.scheduled_jobs (kind, queue, args, state, run_at, created_at)
        SELECT
            format($1 || 'kind_%s', g % 50),
            format($1 || 'queue_%s', g % 100),
            '{}'::jsonb,
            'scheduled',
            now() + interval '1 day',
            now()
        FROM generate_series(1, 200000) AS g
        "#,
    )
    .bind(prefix)
    .execute(&mut *conn)
    .await
    .expect("seed scheduled jobs should succeed");

    sqlx::query(
        r#"
        INSERT INTO awa.jobs_hot (kind, queue, args, state, run_at, created_at)
        SELECT
            format($1 || 'kind_%s', g % 50),
            format($1 || 'queue_%s', g % 100),
            '{}'::jsonb,
            'available',
            now() - interval '30 seconds',
            now()
        FROM generate_series(1, 2000) AS g
        "#,
    )
    .bind(prefix)
    .execute(&mut *conn)
    .await
    .expect("seed available jobs should succeed");

    sqlx::query(
        r#"
        INSERT INTO awa.jobs_hot (kind, queue, args, state, run_at, created_at, finalized_at)
        SELECT
            format($1 || 'kind_%s', g % 50),
            format($1 || 'queue_%s', g % 100),
            '{}'::jsonb,
            'completed',
            now() - interval '2 hours',
            now() - interval '2 hours',
            now() - interval '5 minutes'
        FROM generate_series(1, 2000) AS g
        "#,
    )
    .bind(prefix)
    .execute(&mut *conn)
    .await
    .expect("seed completed jobs should succeed");

    sqlx::query("SET session_replication_role = DEFAULT")
        .execute(&mut *conn)
        .await
        .expect("enable triggers should succeed");

    sqlx::query("SELECT awa.rebuild_admin_metadata()")
        .execute(&mut *conn)
        .await
        .expect("admin metadata rebuild should succeed");
}

#[tokio::test]
async fn test_stats_and_catalog_endpoints_reflect_cached_admin_metadata() {
    let _guard = test_lock().lock().unwrap();
    let pool = setup_pool().await;
    let suffix = Uuid::new_v4().simple().to_string();
    let queue_a = format!("api_stats_meta_a_{suffix}");
    let queue_b = format!("api_stats_meta_b_{suffix}");
    let kind_a = format!("api_stats_meta_kind_a_{suffix}");
    let kind_b = format!("api_stats_meta_kind_b_{suffix}");
    let kind_c = format!("api_stats_meta_kind_c_{suffix}");
    clean_jobs(
        &pool,
        &[queue_a.as_str(), queue_b.as_str()],
        &[kind_a.as_str(), kind_b.as_str(), kind_c.as_str()],
    )
    .await;

    let app = awa_ui::router(pool.clone());
    let baseline = get_json(&app, "/api/stats").await;

    sqlx::query(
        r#"
        INSERT INTO awa.jobs (kind, queue, args, state, run_at)
        VALUES
            ($1, $2, '{}'::jsonb, 'available', now()),
            ($3, $2, '{}'::jsonb, 'scheduled', now() + interval '20 minutes'),
            ($4, $5, '{}'::jsonb, 'retryable', now() + interval '10 minutes')
        "#,
    )
    .bind(&kind_a)
    .bind(&queue_a)
    .bind(&kind_b)
    .bind(&kind_c)
    .bind(&queue_b)
    .execute(&pool)
    .await
    .expect("fixture insert should succeed");

    let stats = get_json(&app, "/api/stats").await;
    let available = stats["available"].as_i64().unwrap_or(0);
    let scheduled = stats["scheduled"].as_i64().unwrap_or(0);
    let retryable = stats["retryable"].as_i64().unwrap_or(0);
    assert_eq!(available, baseline["available"].as_i64().unwrap_or(0) + 1);
    assert_eq!(scheduled, baseline["scheduled"].as_i64().unwrap_or(0) + 1);
    assert_eq!(retryable, baseline["retryable"].as_i64().unwrap_or(0) + 1);

    let kinds = get_json(&app, "/api/stats/kinds").await;
    let kinds = kinds.as_array().expect("kinds payload should be an array");
    assert!(kinds
        .iter()
        .any(|value| value.as_str() == Some(kind_a.as_str())));
    assert!(kinds
        .iter()
        .any(|value| value.as_str() == Some(kind_b.as_str())));
    assert!(kinds
        .iter()
        .any(|value| value.as_str() == Some(kind_c.as_str())));

    let queues = get_json(&app, "/api/stats/queues").await;
    let queues = queues
        .as_array()
        .expect("queues payload should be an array");
    assert!(queues
        .iter()
        .any(|value| value.as_str() == Some(queue_a.as_str())));
    assert!(queues
        .iter()
        .any(|value| value.as_str() == Some(queue_b.as_str())));

    sqlx::query("DELETE FROM awa.jobs WHERE kind = $1")
        .bind(&kind_c)
        .execute(&pool)
        .await
        .expect("delete should succeed");

    let kinds = get_json(&app, "/api/stats/kinds").await;
    let kinds = kinds.as_array().expect("kinds payload should be an array");
    assert!(!kinds
        .iter()
        .any(|value| value.as_str() == Some(kind_c.as_str())));

    let queues = get_json(&app, "/api/stats/queues").await;
    let queues = queues
        .as_array()
        .expect("queues payload should be an array");
    assert!(!queues
        .iter()
        .any(|value| value.as_str() == Some(queue_b.as_str())));
}

#[tokio::test]
async fn test_queues_endpoint_surfaces_total_queued_and_retryable_counts() {
    let _guard = test_lock().lock().unwrap();
    let pool = setup_pool().await;
    let queue = "api_queue_stats_rollup";
    clean_jobs(&pool, &[queue], &[]).await;

    sqlx::query(
        r#"
        INSERT INTO awa.jobs (kind, queue, args, state, run_at, finalized_at)
        VALUES
            ('queue_api_available', $1, '{}'::jsonb, 'available', now() - interval '45 seconds', NULL),
            ('queue_api_scheduled', $1, '{}'::jsonb, 'scheduled', now() + interval '20 minutes', NULL),
            ('queue_api_retryable', $1, '{}'::jsonb, 'retryable', now() + interval '10 minutes', NULL),
            ('queue_api_running', $1, '{}'::jsonb, 'running', now(), NULL),
            ('queue_api_waiting', $1, '{}'::jsonb, 'waiting_external', now(), NULL),
            ('queue_api_failed', $1, '{}'::jsonb, 'failed', now(), now() - interval '5 minutes'),
            ('queue_api_completed', $1, '{}'::jsonb, 'completed', now(), now() - interval '5 minutes')
        "#,
    )
    .bind(queue)
    .execute(&pool)
    .await
    .expect("fixture insert should succeed");

    awa_model::admin::pause_queue(&pool, queue, Some("test"))
        .await
        .expect("pause should succeed");

    let app = awa_ui::router(pool.clone());
    let payload = get_json(&app, "/api/queues").await;
    let queue_stats = payload
        .as_array()
        .expect("queues payload should be an array")
        .iter()
        .find(|entry| entry.get("queue").and_then(Value::as_str) == Some(queue))
        .expect("seeded queue should be present");

    assert_eq!(
        queue_stats.get("total_queued").and_then(Value::as_i64),
        Some(5)
    );
    assert_eq!(
        queue_stats.get("scheduled").and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        queue_stats.get("available").and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        queue_stats.get("retryable").and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(queue_stats.get("running").and_then(Value::as_i64), Some(1));
    assert_eq!(
        queue_stats.get("waiting_external").and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(queue_stats.get("failed").and_then(Value::as_i64), Some(1));
    assert_eq!(
        queue_stats
            .get("completed_last_hour")
            .and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        queue_stats.get("paused").and_then(Value::as_bool),
        Some(true)
    );
    assert!(
        queue_stats
            .get("lag_seconds")
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
            > 0.0
    );
}

#[tokio::test]
#[ignore = "scale validation"]
async fn test_admin_endpoints_scale_with_large_deferred_backlog() {
    let _guard = test_lock().lock().unwrap();
    let pool = setup_pool().await;
    let prefix = "api_scale_";
    seed_scale_fixture(&pool, prefix).await;
    let app = awa_ui::router(pool.clone());

    for path in [
        "/api/stats",
        "/api/queues",
        "/api/stats/kinds",
        "/api/stats/queues",
    ] {
        let started = Instant::now();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        let elapsed = started.elapsed();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            elapsed < Duration::from_millis(50),
            "{path} took {:?}, expected under 50ms",
            elapsed
        );
        println!("{path} {:?}", elapsed);
    }

    clean_jobs(&pool, &[], &[]).await;
    let queue_pattern = format!("{prefix}queue_%");
    let kind_pattern = format!("{prefix}kind_%");
    let mut conn = pool.acquire().await.expect("pool acquire should succeed");
    sqlx::query("SET session_replication_role = replica")
        .execute(&mut *conn)
        .await
        .expect("disable triggers should succeed");
    sqlx::query("DELETE FROM awa.jobs_hot WHERE queue LIKE $1 OR kind LIKE $2")
        .bind(&queue_pattern)
        .bind(&kind_pattern)
        .execute(&mut *conn)
        .await
        .expect("cleanup jobs_hot should succeed");
    sqlx::query("DELETE FROM awa.scheduled_jobs WHERE queue LIKE $1 OR kind LIKE $2")
        .bind(&queue_pattern)
        .bind(&kind_pattern)
        .execute(&mut *conn)
        .await
        .expect("cleanup scheduled_jobs should succeed");
    sqlx::query("SET session_replication_role = DEFAULT")
        .execute(&mut *conn)
        .await
        .expect("enable triggers should succeed");
    sqlx::query("DELETE FROM awa.queue_state_counts WHERE queue LIKE $1")
        .bind(&queue_pattern)
        .execute(&mut *conn)
        .await
        .expect("cleanup queue_state_counts should succeed");
    sqlx::query("DELETE FROM awa.job_queue_catalog WHERE queue LIKE $1")
        .bind(&queue_pattern)
        .execute(&mut *conn)
        .await
        .expect("cleanup job_queue_catalog should succeed");
    sqlx::query("DELETE FROM awa.job_kind_catalog WHERE kind LIKE $1")
        .bind(&kind_pattern)
        .execute(&mut *conn)
        .await
        .expect("cleanup job_kind_catalog should succeed");
}
