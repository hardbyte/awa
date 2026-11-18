//! Bulk admin routes run in bounded batches.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::util::ServiceExt;
use uuid::Uuid;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", &Uuid::new_v4().simple().to_string()[..12])
}

async fn request(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(match body {
                    Some(body) => Body::from(body.to_string()),
                    None => Body::empty(),
                })
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    (
        status,
        serde_json::from_slice(&bytes).expect("response should deserialize"),
    )
}

async fn seed_dlq(pool: &PgPool, queue: &str, rows: i64) {
    sqlx::query(
        "INSERT INTO awa.dlq_entries (job_id, kind, queue, priority, dlq_reason, original_run_lease) \
         SELECT nextval('awa.job_id_seq'), 'ui_bulk_job', $1, 2, 'seeded', 1 FROM generate_series(1, $2)",
    )
    .bind(queue)
    .bind(rows)
    .execute(pool)
    .await
    .expect("seed dlq");
}

#[tokio::test]
async fn dlq_purge_runs_one_batch_or_every_batch_and_counts_are_capped() {
    let _guard = TEST_LOCK.lock().await;
    let pool = awa_testing::setup::setup(4).await;
    awa_testing::setup::activate_queue_storage(&pool).await;
    let queue = unique_name("ui_bulk_purge");
    seed_dlq(&pool, &queue, 5).await;
    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");

    let (status, body) = request(&app, "GET", &format!("/api/dlq/count?queue={queue}"), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({ "count": 5, "capped": false }));

    let (status, body) = request(
        &app,
        "POST",
        "/api/dlq/bulk-purge",
        Some(json!({ "queue": queue, "batch_size": 2 })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["count"], 2);
    assert_eq!(body["done"], false);

    let (status, body) = request(
        &app,
        "POST",
        "/api/dlq/bulk-purge",
        Some(json!({ "queue": queue })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({ "count": 3, "skipped": 0, "conflicts": [], "left_above_sequence": 0, "done": true })
    );
}

#[tokio::test]
async fn queue_drain_runs_one_batch_from_a_cursor_or_every_batch() {
    let _guard = TEST_LOCK.lock().await;
    let pool = awa_testing::setup::setup(4).await;
    awa_testing::setup::reset_to_canonical(&pool).await;
    let queue = unique_name("ui_bulk_drain");
    let params: Vec<awa_model::InsertParams> = (0..5)
        .map(|seq| awa_model::InsertParams {
            kind: "ui_bulk_job".to_string(),
            args: json!({ "seq": seq }),
            opts: awa_model::InsertOpts {
                queue: queue.clone(),
                ..Default::default()
            },
        })
        .collect();
    awa_model::insert_many(&pool, &params)
        .await
        .expect("insert jobs");
    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");

    let (status, body) = request(
        &app,
        "POST",
        &format!("/api/queues/{queue}/drain?batch_size=2"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        (body["drained"].clone(), body["count"].clone()),
        (json!(2), json!(2))
    );
    let cursor = body["cursor"].clone();

    // A job enqueued after the run started is left alone by the rest of the
    // run, which carries the run's high-water mark in its cursor.
    let later = awa_model::insert_many(
        &pool,
        &[awa_model::InsertParams {
            kind: "ui_bulk_job".to_string(),
            args: json!({ "seq": "later" }),
            opts: awa_model::InsertOpts {
                queue: queue.clone(),
                ..Default::default()
            },
        }],
    )
    .await
    .expect("insert later job")[0]
        .id;
    let mut drained = 0;
    let mut cursor = cursor;
    loop {
        let (status, body) = request(
            &app,
            "POST",
            &format!(
                "/api/queues/{queue}/drain?batch_size=2&stage={}&after_job_id={}&before_job_id={}&sequence_bound={}",
                cursor["stage"],
                cursor["after_job_id"],
                cursor["before_job_id"],
                cursor["sequence_bound"],
            ),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["left_above_sequence"], 0, "{body}");
        drained += body["drained"].as_u64().expect("drained");
        cursor = body["cursor"].clone();
        if body["done"] == true {
            break;
        }
    }
    assert_eq!(drained, 3);
    let later_state: String = sqlx::query_scalar("SELECT state::text FROM awa.jobs WHERE id = $1")
        .bind(later)
        .fetch_one(&pool)
        .await
        .expect("later job state");
    assert_eq!(later_state, "available");

    let (status, body) = request(&app, "POST", &format!("/api/queues/{queue}/drain"), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["drained"], 1);
    assert_eq!(body["done"], true);
    awa_testing::setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn bulk_routes_reject_a_batch_size_above_the_cap() {
    let _guard = TEST_LOCK.lock().await;
    let pool = awa_testing::setup::setup(4).await;
    awa_testing::setup::activate_queue_storage(&pool).await;
    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");
    let too_big = awa_model::bulk::MAX_BATCH_SIZE + 1;

    let (status, body) = request(
        &app,
        "POST",
        "/api/dlq/bulk-purge",
        Some(json!({ "queue": "any", "batch_size": too_big })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (status, body) = request(
        &app,
        "POST",
        &format!("/api/queues/any/drain?batch_size={too_big}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}

/// Fail a plain job and a unique job whose key a live job then takes, and
/// return their IDs.
async fn failed_jobs_one_conflicting(pool: &sqlx::PgPool, queue: &str) -> Vec<i64> {
    let unique = |key: &str| awa_model::InsertParams {
        kind: "ui_bulk_unique_job".to_string(),
        args: json!({ "key": key }),
        opts: awa_model::InsertOpts {
            queue: queue.to_string(),
            unique: Some(awa_model::UniqueOpts::default()),
            ..Default::default()
        },
    };
    let plain = awa_model::InsertParams {
        kind: "ui_bulk_job".to_string(),
        args: json!({ "seq": 1 }),
        opts: awa_model::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    };
    let jobs = awa_model::insert_many(pool, &[plain, unique(queue)])
        .await
        .expect("insert jobs");
    let ids: Vec<i64> = jobs.iter().map(|job| job.id).collect();
    sqlx::query(
        "UPDATE awa.jobs SET state = 'failed', attempt = 1, finalized_at = now() WHERE id = ANY($1)",
    )
    .bind(&ids)
    .execute(pool)
    .await
    .expect("fail jobs");
    awa_model::insert_many(pool, &[unique(queue)])
        .await
        .expect("a live job takes the key");
    ids
}

#[tokio::test]
async fn bulk_retry_by_id_reports_skipped_conflicts_when_asked() {
    let _guard = TEST_LOCK.lock().await;
    let pool = awa_testing::setup::setup(4).await;
    awa_testing::setup::reset_to_canonical(&pool).await;
    let queue = unique_name("ui_bulk_retry_ids");
    let ids = failed_jobs_one_conflicting(&pool, &queue).await;
    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");

    let (status, body) = request(
        &app,
        "POST",
        "/api/jobs/bulk-retry",
        Some(json!({ "ids": ids, "report_skipped": true })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["jobs"].as_array().expect("jobs").len(), 1, "{body}");
    assert_eq!(body["jobs"][0]["id"], ids[0]);
    assert_eq!(
        (body["skipped"].clone(), body["conflicts"].clone()),
        (json!(1), json!([ids[1]]))
    );
    awa_testing::setup::clean_queue(&pool, &queue).await;
}

/// A client that does not ask for the report, such as a web UI loaded from
/// the previous release during a rolling update, gets the array of retried
/// jobs the route has always returned.
#[tokio::test]
async fn bulk_retry_by_id_returns_the_array_by_default() {
    let _guard = TEST_LOCK.lock().await;
    let pool = awa_testing::setup::setup(4).await;
    awa_testing::setup::reset_to_canonical(&pool).await;
    let queue = unique_name("ui_bulk_retry_array");
    let ids = failed_jobs_one_conflicting(&pool, &queue).await;
    let app = awa_ui::router(pool.clone(), std::time::Duration::ZERO)
        .await
        .expect("router should initialize");

    let (status, body) = request(
        &app,
        "POST",
        "/api/jobs/bulk-retry",
        Some(json!({ "ids": ids })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let jobs = body.as_array().expect("an array of retried jobs");
    assert_eq!(jobs.len(), 1, "{body}");
    assert_eq!(jobs[0]["id"], ids[0]);
    awa_testing::setup::clean_queue(&pool, &queue).await;
}
