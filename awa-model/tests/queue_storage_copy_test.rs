use awa_model::{
    migrations, AwaError, InsertOpts, InsertParams, QueueStorage, QueueStorageConfig, UniqueOpts,
};
use chrono::{TimeDelta, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::LazyLock;
use tokio::sync::Mutex;

static QUEUE_STORAGE_COPY_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup_store(schema: &str) -> (PgPool, QueueStorage) {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url())
        .await
        .expect("connect");
    migrations::run(&pool).await.expect("run migrations");

    let store = QueueStorage::new(QueueStorageConfig {
        schema: schema.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })
    .expect("create queue storage");
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", store.schema()))
        .execute(&pool)
        .await
        .expect("drop queue storage schema");
    store
        .prepare_schema(&pool)
        .await
        .expect("prepare queue storage schema");
    store.reset(&pool).await.expect("reset queue storage");

    (pool, store)
}

#[tokio::test]
async fn queue_storage_copy_enqueues_ready_and_deferred_rows() {
    let _guard = QUEUE_STORAGE_COPY_LOCK.lock().await;
    let (pool, store) = setup_store("awa_qs_copy_enqueues").await;
    let queue = "qs_copy_ready_deferred";

    let jobs = vec![
        InsertParams {
            kind: "copy_ready".to_string(),
            args: serde_json::json!({"seq": 0}),
            opts: InsertOpts {
                queue: queue.to_string(),
                priority: 1,
                metadata: serde_json::json!({"source": "copy"}),
                tags: vec!["bulk".to_string()],
                ..Default::default()
            },
        },
        InsertParams {
            kind: "copy_ready".to_string(),
            args: serde_json::json!({"seq": 1}),
            opts: InsertOpts {
                queue: queue.to_string(),
                priority: 1,
                ..Default::default()
            },
        },
        InsertParams {
            kind: "copy_scheduled".to_string(),
            args: serde_json::json!({"seq": 2}),
            opts: InsertOpts {
                queue: queue.to_string(),
                priority: 3,
                run_at: Some(Utc::now() + TimeDelta::minutes(10)),
                ..Default::default()
            },
        },
    ];

    let inserted = store
        .enqueue_params_copy(&pool, &jobs)
        .await
        .expect("copy enqueue");
    assert_eq!(inserted, 3);

    let ready: Vec<(i64, serde_json::Value)> = sqlx::query_as(&format!(
        "SELECT lane_seq, payload FROM {}.ready_entries WHERE queue = $1 ORDER BY lane_seq",
        store.schema()
    ))
    .bind(queue)
    .fetch_all(&pool)
    .await
    .expect("read ready rows");
    assert_eq!(ready.len(), 2);
    assert_eq!(ready[1].0, ready[0].0 + 1);
    assert_eq!(ready[0].1["metadata"]["source"], "copy");
    assert_eq!(ready[0].1["tags"], serde_json::json!(["bulk"]));

    let deferred_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.deferred_jobs WHERE queue = $1 AND state = 'scheduled'",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("count deferred rows");
    assert_eq!(deferred_count, 1);

    let available_count: i64 = sqlx::query_scalar(&format!(
        "SELECT available_count FROM {}.queue_lanes WHERE queue = $1 AND priority = 1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("read lane count");
    assert_eq!(available_count, 2);
}

#[tokio::test]
async fn queue_storage_copy_rolls_back_on_unique_conflict() {
    let _guard = QUEUE_STORAGE_COPY_LOCK.lock().await;
    let (pool, store) = setup_store("awa_qs_copy_unique_conflict").await;
    let queue = "qs_copy_unique_conflict";

    let opts = InsertOpts {
        queue: queue.to_string(),
        unique: Some(UniqueOpts::default()),
        ..Default::default()
    };
    let jobs = vec![
        InsertParams {
            kind: "copy_unique".to_string(),
            args: serde_json::json!({"same": true}),
            opts: opts.clone(),
        },
        InsertParams {
            kind: "copy_unique".to_string(),
            args: serde_json::json!({"same": true}),
            opts,
        },
    ];

    let err = store
        .enqueue_params_copy(&pool, &jobs)
        .await
        .expect_err("duplicate unique batch should fail");
    assert!(
        matches!(err, AwaError::UniqueConflict { .. }),
        "unexpected error: {err:?}"
    );

    let ready_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*)::bigint FROM {}.ready_entries WHERE queue = $1",
        store.schema()
    ))
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("count ready rows");
    assert_eq!(ready_count, 0);
}
