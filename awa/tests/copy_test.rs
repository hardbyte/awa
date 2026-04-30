//! Integration tests for COPY-based batch ingestion.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test copy_test -- --nocapture`

use awa::model::{admin, insert_many_copy, insert_many_copy_from_pool, migrations};
use awa::{InsertOpts, InsertParams, JobState, UniqueOpts};
use sqlx::postgres::PgPoolOptions;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup() -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");
    migrations::run(&pool).await.expect("Failed to migrate");
    sqlx::query("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
        .execute(&pool)
        .await
        .expect("Failed to reset active runtime backend");
    pool
}

async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue jobs");
}

fn make_job(seq: i64, queue: &str) -> InsertParams {
    InsertParams {
        kind: "copy_test_job".to_string(),
        args: serde_json::json!({"seq": seq}),
        opts: InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    }
}

// ── Test 1: Empty input ──────────────────────────────────────────────

#[tokio::test]
async fn test_copy_empty_input() {
    let pool = setup().await;
    let result = insert_many_copy_from_pool(&pool, &[]).await.unwrap();
    assert!(result.is_empty());
}

// ── Test 2: Single job ──────────────────────────────────────────────

#[tokio::test]
async fn test_copy_single_job() {
    let pool = setup().await;
    let queue = "copy_single";
    clean_queue(&pool, queue).await;

    let jobs = vec![make_job(1, queue)];
    let result = insert_many_copy_from_pool(&pool, &jobs).await.unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].kind, "copy_test_job");
    assert_eq!(result[0].queue, queue);
    assert_eq!(result[0].state, JobState::Available);
    assert_eq!(result[0].args["seq"], 1);
}

// ── Test 3: 1000 jobs ───────────────────────────────────────────────

#[tokio::test]
async fn test_copy_1000_jobs() {
    let pool = setup().await;
    let queue = "copy_1000";
    clean_queue(&pool, queue).await;

    let jobs: Vec<InsertParams> = (0..1000).map(|i| make_job(i, queue)).collect();
    let result = insert_many_copy_from_pool(&pool, &jobs).await.unwrap();

    assert_eq!(result.len(), 1000);
    for (i, row) in result.iter().enumerate() {
        assert_eq!(row.kind, "copy_test_job");
        assert_eq!(row.queue, queue);
        assert_eq!(row.state, JobState::Available);
        assert_eq!(row.args["seq"], i as i64);
    }
}

// ── Test 4: Special characters in args ──────────────────────────────

#[tokio::test]
async fn test_copy_special_chars_in_args() {
    let pool = setup().await;
    let queue = "copy_special_args";
    clean_queue(&pool, queue).await;

    let special_args = serde_json::json!({
        "quotes": "he said \"hello\"",
        "newlines": "line1\nline2\nline3",
        "commas": "a,b,c",
        "tabs": "col1\tcol2",
        "backslashes": "path\\to\\file",
        "unicode": "emoji: \u{1F600} and \u{00E9}",
        "nested": {"key": "value with \"quotes\""},
        "null_str": "NULL",
        "empty": ""
    });

    let jobs = vec![InsertParams {
        kind: "special_args_job".to_string(),
        args: special_args.clone(),
        opts: InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    }];

    let result = insert_many_copy_from_pool(&pool, &jobs).await.unwrap();
    assert_eq!(result.len(), 1);

    // Verify round-trip
    let row = &result[0];
    assert_eq!(row.args["quotes"], "he said \"hello\"");
    assert_eq!(row.args["newlines"], "line1\nline2\nline3");
    assert_eq!(row.args["commas"], "a,b,c");
    assert_eq!(row.args["tabs"], "col1\tcol2");
    assert_eq!(row.args["backslashes"], "path\\to\\file");
    assert_eq!(row.args["nested"]["key"], "value with \"quotes\"");
    assert_eq!(row.args["null_str"], "NULL");
    assert_eq!(row.args["empty"], "");
}

// ── Test 5: Tags with pathological values ───────────────────────────

#[tokio::test]
async fn test_copy_pathological_tags() {
    let pool = setup().await;
    let queue = "copy_tags";
    clean_queue(&pool, queue).await;

    let pathological_tags = vec![
        "simple".to_string(),
        "with,comma".to_string(),
        "with\"quote".to_string(),
        "with{brace".to_string(),
        "with}brace".to_string(),
        "with\\backslash".to_string(),
        " whitespace_only ".to_string(),
        "NULL".to_string(),
        String::new(), // empty string
    ];

    let jobs = vec![InsertParams {
        kind: "tag_test_job".to_string(),
        args: serde_json::json!({"test": true}),
        opts: InsertOpts {
            queue: queue.to_string(),
            tags: pathological_tags.clone(),
            ..Default::default()
        },
    }];

    let result = insert_many_copy_from_pool(&pool, &jobs).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].tags, pathological_tags);
}

// ── Test 6: Unique constraint jobs ──────────────────────────────────

#[tokio::test]
async fn test_copy_unique_constraint() {
    let pool = setup().await;
    let queue = "copy_unique";
    clean_queue(&pool, queue).await;

    let unique_opts = InsertOpts {
        queue: queue.to_string(),
        unique: Some(UniqueOpts {
            by_args: true,
            ..Default::default()
        }),
        ..Default::default()
    };

    // Insert first batch
    let jobs1 = vec![
        InsertParams {
            kind: "unique_job".to_string(),
            args: serde_json::json!({"id": 1}),
            opts: unique_opts.clone(),
        },
        InsertParams {
            kind: "unique_job".to_string(),
            args: serde_json::json!({"id": 2}),
            opts: unique_opts.clone(),
        },
    ];
    let result1 = insert_many_copy_from_pool(&pool, &jobs1).await.unwrap();
    assert_eq!(result1.len(), 2);

    // Insert second batch with overlap — id:1 conflicts, id:3 is new
    let jobs2 = vec![
        InsertParams {
            kind: "unique_job".to_string(),
            args: serde_json::json!({"id": 1}), // conflict
            opts: unique_opts.clone(),
        },
        InsertParams {
            kind: "unique_job".to_string(),
            args: serde_json::json!({"id": 3}), // new
            opts: unique_opts.clone(),
        },
    ];
    let result2 = insert_many_copy_from_pool(&pool, &jobs2).await.unwrap();
    // Only id:3 should be inserted (id:1 conflicts)
    assert_eq!(result2.len(), 1);
    assert_eq!(result2[0].args["id"], 3);
}

#[tokio::test]
async fn test_copy_unique_constraint_deduplicates_within_batch() {
    let pool = setup().await;
    let queue = "copy_unique_same_batch";
    clean_queue(&pool, queue).await;

    let unique_opts = InsertOpts {
        queue: queue.to_string(),
        unique: Some(UniqueOpts {
            by_args: true,
            by_queue: true,
            ..Default::default()
        }),
        ..Default::default()
    };

    let jobs = vec![
        InsertParams {
            kind: "unique_job".to_string(),
            args: serde_json::json!({"id": 1}),
            opts: unique_opts.clone(),
        },
        InsertParams {
            kind: "unique_job".to_string(),
            args: serde_json::json!({"id": 1}),
            opts: unique_opts.clone(),
        },
        InsertParams {
            kind: "unique_job".to_string(),
            args: serde_json::json!({"id": 2}),
            opts: unique_opts.clone(),
        },
    ];

    let result = insert_many_copy_from_pool(&pool, &jobs).await.unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].args["id"], 1);
    assert_eq!(result[1].args["id"], 2);

    let stored: Vec<i64> = sqlx::query_scalar(
        "SELECT (args->>'id')::bigint FROM awa.jobs WHERE queue = $1 ORDER BY 1",
    )
    .bind(queue)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(stored, vec![1, 2]);
}

// ── Test 7: Mixed run_at (NULL = available, future = scheduled) ─────

#[tokio::test]
async fn test_copy_mixed_run_at() {
    let pool = setup().await;
    let queue = "copy_run_at";
    clean_queue(&pool, queue).await;

    let future = chrono::Utc::now() + chrono::Duration::hours(24);
    let jobs = vec![
        InsertParams {
            kind: "run_at_job".to_string(),
            args: serde_json::json!({"seq": 1}),
            opts: InsertOpts {
                queue: queue.to_string(),
                run_at: None, // available
                ..Default::default()
            },
        },
        InsertParams {
            kind: "run_at_job".to_string(),
            args: serde_json::json!({"seq": 2}),
            opts: InsertOpts {
                queue: queue.to_string(),
                run_at: Some(future), // scheduled
                ..Default::default()
            },
        },
    ];

    let result = insert_many_copy_from_pool(&pool, &jobs).await.unwrap();
    assert_eq!(result.len(), 2);

    let available = result.iter().find(|r| r.args["seq"] == 1).unwrap();
    let scheduled = result.iter().find(|r| r.args["seq"] == 2).unwrap();
    assert_eq!(available.state, JobState::Available);
    assert_eq!(scheduled.state, JobState::Scheduled);
}

// ── Test 8: Admin metadata tracks direct COPY paths ─────────────────

#[tokio::test]
async fn test_copy_updates_admin_metadata_for_direct_paths() {
    let pool = setup().await;
    let hot_queue = "copy_admin_hot";
    let scheduled_queue = "copy_admin_scheduled";
    let hot_kind = "copy_admin_hot_kind";
    let scheduled_kind = "copy_admin_scheduled_kind";

    clean_queue(&pool, hot_queue).await;
    clean_queue(&pool, scheduled_queue).await;
    sqlx::query("DELETE FROM awa.jobs WHERE kind = ANY($1)")
        .bind(vec![hot_kind, scheduled_kind])
        .execute(&pool)
        .await
        .unwrap();

    let hot_jobs = vec![
        InsertParams {
            kind: hot_kind.to_string(),
            args: serde_json::json!({"seq": 1}),
            opts: InsertOpts {
                queue: hot_queue.to_string(),
                ..Default::default()
            },
        },
        InsertParams {
            kind: hot_kind.to_string(),
            args: serde_json::json!({"seq": 2}),
            opts: InsertOpts {
                queue: hot_queue.to_string(),
                ..Default::default()
            },
        },
    ];
    insert_many_copy_from_pool(&pool, &hot_jobs).await.unwrap();

    let scheduled_jobs = vec![
        InsertParams {
            kind: scheduled_kind.to_string(),
            args: serde_json::json!({"seq": 3}),
            opts: InsertOpts {
                queue: scheduled_queue.to_string(),
                run_at: Some(chrono::Utc::now() + chrono::Duration::minutes(30)),
                ..Default::default()
            },
        },
        InsertParams {
            kind: scheduled_kind.to_string(),
            args: serde_json::json!({"seq": 4}),
            opts: InsertOpts {
                queue: scheduled_queue.to_string(),
                run_at: Some(chrono::Utc::now() + chrono::Duration::minutes(45)),
                ..Default::default()
            },
        },
    ];
    insert_many_copy_from_pool(&pool, &scheduled_jobs)
        .await
        .unwrap();

    admin::flush_dirty_admin_metadata(&pool).await.unwrap();
    let stats = admin::queue_overviews(&pool).await.unwrap();
    let hot_stats = stats.iter().find(|stat| stat.queue == hot_queue).unwrap();
    assert_eq!(hot_stats.available, 2);
    assert_eq!(hot_stats.total_queued, 2);

    let scheduled_stats = stats
        .iter()
        .find(|stat| stat.queue == scheduled_queue)
        .unwrap();
    assert_eq!(scheduled_stats.scheduled, 2);
    assert_eq!(scheduled_stats.total_queued, 2);

    let kinds = admin::distinct_kinds(&pool).await.unwrap();
    assert!(kinds.contains(&hot_kind.to_string()));
    assert!(kinds.contains(&scheduled_kind.to_string()));

    let queues = admin::distinct_queues(&pool).await.unwrap();
    assert!(queues.contains(&hot_queue.to_string()));
    assert!(queues.contains(&scheduled_queue.to_string()));
}

// ── Test 9: Metadata with special chars ─────────────────────────────

#[tokio::test]
async fn test_copy_metadata_special_chars() {
    let pool = setup().await;
    let queue = "copy_metadata";
    clean_queue(&pool, queue).await;

    let metadata = serde_json::json!({
        "source": "import \"legacy\"",
        "nested": {"deep": {"value": "with\nnewline"}},
        "escaped": "back\\slash"
    });

    let jobs = vec![InsertParams {
        kind: "metadata_job".to_string(),
        args: serde_json::json!({"test": true}),
        opts: InsertOpts {
            queue: queue.to_string(),
            metadata: metadata.clone(),
            ..Default::default()
        },
    }];

    let result = insert_many_copy_from_pool(&pool, &jobs).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].metadata, metadata);
}

// ── Test 10: Atomicity (transaction rollback) ───────────────────────

#[tokio::test]
async fn test_copy_atomicity() {
    let pool = setup().await;
    let queue = "copy_atomic";
    clean_queue(&pool, queue).await;

    // Start a transaction, insert via COPY, then rollback
    let mut tx = pool.begin().await.unwrap();
    let jobs: Vec<InsertParams> = (0..10).map(|i| make_job(i, queue)).collect();
    let result = insert_many_copy(&mut tx, &jobs).await.unwrap();
    assert_eq!(result.len(), 10);

    // Rollback
    tx.rollback().await.unwrap();

    // Verify nothing was inserted
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

// ── Test 11: COPY within caller-managed transaction ─────────────────

#[tokio::test]
async fn test_copy_within_caller_transaction() {
    let pool = setup().await;
    let queue = "copy_caller_tx";
    clean_queue(&pool, queue).await;

    let mut tx = pool.begin().await.unwrap();

    // Do some other work in the same transaction
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1 AND kind = 'other_job'")
        .bind(queue)
        .execute(&mut *tx)
        .await
        .unwrap();

    // COPY jobs within the same transaction
    let jobs: Vec<InsertParams> = (0..5).map(|i| make_job(i, queue)).collect();
    let result = insert_many_copy(&mut tx, &jobs).await.unwrap();
    assert_eq!(result.len(), 5);

    // Commit the whole transaction
    tx.commit().await.unwrap();

    // Verify jobs are there
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 5);
}

// ── Test 12: Reused staging table within one transaction ────────────

#[tokio::test]
async fn test_copy_multiple_calls_within_same_transaction() {
    let pool = setup().await;
    let queue = "copy_reused_staging";
    clean_queue(&pool, queue).await;

    let mut tx = pool.begin().await.unwrap();

    let batch_a: Vec<InsertParams> = (0..3).map(|i| make_job(i, queue)).collect();
    let batch_b: Vec<InsertParams> = (3..6).map(|i| make_job(i, queue)).collect();

    let result_a = insert_many_copy(&mut tx, &batch_a).await.unwrap();
    let result_b = insert_many_copy(&mut tx, &batch_b).await.unwrap();

    assert_eq!(result_a.len(), 3);
    assert_eq!(result_b.len(), 3);

    tx.commit().await.unwrap();

    let rows: Vec<i64> = sqlx::query_scalar(
        "SELECT (args->>'seq')::bigint FROM awa.jobs WHERE queue = $1 ORDER BY 1",
    )
    .bind(queue)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(rows, vec![0, 1, 2, 3, 4, 5]);
}
