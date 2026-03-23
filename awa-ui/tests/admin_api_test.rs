use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use std::time::Instant;
use tower::util::ServiceExt;

async fn setup_pool() -> sqlx::PgPool {
    awa_testing::setup::setup(4).await
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
    }

    if !kinds.is_empty() {
        sqlx::query("DELETE FROM awa.jobs WHERE kind = ANY($1)")
            .bind(kinds)
            .execute(pool)
            .await
            .expect("failed to clean jobs by kind");
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

    sqlx::query(
        r#"
        INSERT INTO awa.queue_state_counts (
            queue, scheduled, available, running, completed,
            retryable, failed, cancelled, waiting_external
        )
        SELECT
            queue,
            count(*) FILTER (WHERE state = 'scheduled') AS scheduled,
            count(*) FILTER (WHERE state = 'available') AS available,
            count(*) FILTER (WHERE state = 'running') AS running,
            count(*) FILTER (WHERE state = 'completed') AS completed,
            count(*) FILTER (WHERE state = 'retryable') AS retryable,
            count(*) FILTER (WHERE state = 'failed') AS failed,
            count(*) FILTER (WHERE state = 'cancelled') AS cancelled,
            count(*) FILTER (WHERE state = 'waiting_external') AS waiting_external
        FROM (
            SELECT queue, state FROM awa.jobs_hot WHERE queue LIKE $1
            UNION ALL
            SELECT queue, state FROM awa.scheduled_jobs WHERE queue LIKE $1
        ) AS jobs
        GROUP BY queue
        ON CONFLICT (queue) DO UPDATE
        SET scheduled = EXCLUDED.scheduled,
            available = EXCLUDED.available,
            running = EXCLUDED.running,
            completed = EXCLUDED.completed,
            retryable = EXCLUDED.retryable,
            failed = EXCLUDED.failed,
            cancelled = EXCLUDED.cancelled,
            waiting_external = EXCLUDED.waiting_external
        "#,
    )
    .bind(&queue_pattern)
    .execute(&mut *conn)
    .await
    .expect("backfill queue counts should succeed");

    sqlx::query(
        r#"
        INSERT INTO awa.job_kind_catalog (kind, ref_count)
        SELECT kind, count(*) AS ref_count
        FROM (
            SELECT kind FROM awa.jobs_hot WHERE kind LIKE $1
            UNION ALL
            SELECT kind FROM awa.scheduled_jobs WHERE kind LIKE $1
        ) AS jobs
        GROUP BY kind
        ON CONFLICT (kind) DO UPDATE
        SET ref_count = EXCLUDED.ref_count
        "#,
    )
    .bind(&kind_pattern)
    .execute(&mut *conn)
    .await
    .expect("backfill kind catalog should succeed");

    sqlx::query(
        r#"
        INSERT INTO awa.job_queue_catalog (queue, ref_count)
        SELECT queue, count(*) AS ref_count
        FROM (
            SELECT queue FROM awa.jobs_hot WHERE queue LIKE $1
            UNION ALL
            SELECT queue FROM awa.scheduled_jobs WHERE queue LIKE $1
        ) AS jobs
        GROUP BY queue
        ON CONFLICT (queue) DO UPDATE
        SET ref_count = EXCLUDED.ref_count
        "#,
    )
    .bind(&queue_pattern)
    .execute(&mut *conn)
    .await
    .expect("backfill queue catalog should succeed");
}

#[tokio::test]
async fn test_stats_and_catalog_endpoints_reflect_cached_admin_metadata() {
    let pool = setup_pool().await;
    let queue_a = "api_stats_meta_a";
    let queue_b = "api_stats_meta_b";
    let kind_a = "api_stats_meta_kind_a";
    let kind_b = "api_stats_meta_kind_b";
    let kind_c = "api_stats_meta_kind_c";
    clean_jobs(&pool, &[queue_a, queue_b], &[kind_a, kind_b, kind_c]).await;

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
    .bind(kind_a)
    .bind(queue_a)
    .bind(kind_b)
    .bind(kind_c)
    .bind(queue_b)
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
    assert!(kinds.iter().any(|value| value.as_str() == Some(kind_a)));
    assert!(kinds.iter().any(|value| value.as_str() == Some(kind_b)));
    assert!(kinds.iter().any(|value| value.as_str() == Some(kind_c)));

    let queues = get_json(&app, "/api/stats/queues").await;
    let queues = queues
        .as_array()
        .expect("queues payload should be an array");
    assert!(queues.iter().any(|value| value.as_str() == Some(queue_a)));
    assert!(queues.iter().any(|value| value.as_str() == Some(queue_b)));

    sqlx::query("DELETE FROM awa.jobs WHERE kind = $1")
        .bind(kind_c)
        .execute(&pool)
        .await
        .expect("delete should succeed");

    let kinds = get_json(&app, "/api/stats/kinds").await;
    let kinds = kinds.as_array().expect("kinds payload should be an array");
    assert!(!kinds.iter().any(|value| value.as_str() == Some(kind_c)));

    let queues = get_json(&app, "/api/stats/queues").await;
    let queues = queues
        .as_array()
        .expect("queues payload should be an array");
    assert!(!queues.iter().any(|value| value.as_str() == Some(queue_b)));
}

#[tokio::test]
async fn test_queues_endpoint_surfaces_total_queued_and_retryable_counts() {
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
