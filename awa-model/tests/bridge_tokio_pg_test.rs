//! Integration tests for the tokio-postgres bridge adapter.
//!
//! Run with:
//!   cargo test --package awa-model --features tokio-postgres --test bridge_tokio_pg_test

#![cfg(feature = "tokio-postgres")]

use awa_model::bridge::tokio_pg;
use awa_model::job::{InsertOpts, UniqueOpts};
use awa_model::{AwaError, JobArgs};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct BridgeEmail {
    to: String,
    subject: String,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct BridgeReport {
    name: String,
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn connect() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&database_url(), tokio_postgres::NoTls)
        .await
        .expect("Failed to connect to Postgres");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    client
}

async fn ensure_migrated() {
    // Always run migrations — idempotent, and avoids false signals from stale schemas.
    let pool = sqlx::PgPool::connect(&database_url()).await.unwrap();
    awa_model::migrations::run(&pool).await.unwrap();
    pool.close().await;
}

/// Clean up jobs by ID so tests don't leak rows into the shared database.
async fn cleanup_jobs(client: &tokio_postgres::Client, job_ids: &[i64]) {
    if job_ids.is_empty() {
        return;
    }
    for id in job_ids {
        let _ = client
            .execute("DELETE FROM awa.jobs WHERE id = $1", &[id])
            .await;
    }
}

// ---------------------------------------------------------------------------
// insert_job (default opts)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_job_defaults() {
    let client = connect().await;
    ensure_migrated().await;

    let job = tokio_pg::insert_job(
        &client,
        &BridgeEmail {
            to: "defaults@example.com".into(),
            subject: "Hello".into(),
        },
    )
    .await
    .unwrap();

    assert!(job.id > 0);
    assert_eq!(job.kind, "bridge_email");
    assert_eq!(job.queue, "default");
    assert_eq!(job.state, awa_model::JobState::Available);
    assert_eq!(job.priority, 2);
    assert_eq!(job.max_attempts, 25);

    cleanup_jobs(&client, &[job.id]).await;
}

// ---------------------------------------------------------------------------
// insert_job_with (custom opts)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_job_with_custom_opts() {
    let client = connect().await;
    ensure_migrated().await;

    let job = tokio_pg::insert_job_with(
        &client,
        &BridgeEmail {
            to: "custom@example.com".into(),
            subject: "Priority".into(),
        },
        InsertOpts {
            queue: "bridge_custom".into(),
            priority: 1,
            max_attempts: 5,
            tags: vec!["bridge".into(), "tokio-pg".into()],
            ..InsertOpts::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(job.queue, "bridge_custom");
    assert_eq!(job.priority, 1);
    assert_eq!(job.max_attempts, 5);
    assert_eq!(job.tags, vec!["bridge", "tokio-pg"]);

    cleanup_jobs(&client, &[job.id]).await;
}

// ---------------------------------------------------------------------------
// Transaction commit / rollback atomicity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_transaction_commit() {
    let mut client = connect().await;
    ensure_migrated().await;

    let txn = client.transaction().await.unwrap();

    let job = tokio_pg::insert_job(
        &txn,
        &BridgeEmail {
            to: "commit@example.com".into(),
            subject: "Committed".into(),
        },
    )
    .await
    .unwrap();
    let job_id = job.id;

    txn.commit().await.unwrap();

    let count: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM awa.jobs WHERE id = $1",
            &[&job_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);

    cleanup_jobs(&client, &[job_id]).await;
}

#[tokio::test]
async fn test_transaction_rollback() {
    let mut client = connect().await;
    ensure_migrated().await;

    let txn = client.transaction().await.unwrap();

    let job = tokio_pg::insert_job(
        &txn,
        &BridgeEmail {
            to: "rollback@example.com".into(),
            subject: "Gone".into(),
        },
    )
    .await
    .unwrap();
    let job_id = job.id;

    txn.rollback().await.unwrap();

    let count: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM awa.jobs WHERE id = $1",
            &[&job_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    // No cleanup needed — row was rolled back.
}

// ---------------------------------------------------------------------------
// Mixed app row + job insert in one transaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mixed_app_and_job_insert_in_one_transaction() {
    let mut client = connect().await;
    ensure_migrated().await;

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS bridge_test_orders (id SERIAL PRIMARY KEY, email TEXT NOT NULL)",
            &[],
        )
        .await
        .unwrap();

    let txn = client.transaction().await.unwrap();

    let order_id: i32 = txn
        .query_one(
            "INSERT INTO bridge_test_orders (email) VALUES ($1) RETURNING id",
            &[&"mixed@example.com"],
        )
        .await
        .unwrap()
        .get(0);

    let job = tokio_pg::insert_job_with(
        &txn,
        &BridgeEmail {
            to: "mixed@example.com".into(),
            subject: format!("Order {order_id}"),
        },
        InsertOpts {
            queue: "bridge_mixed".into(),
            metadata: serde_json::json!({"order_id": order_id}),
            ..InsertOpts::default()
        },
    )
    .await
    .unwrap();
    let job_id = job.id;

    txn.commit().await.unwrap();

    // Both visible after commit
    let order_count: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM bridge_test_orders WHERE id = $1",
            &[&order_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(order_count, 1);

    let job_count: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM awa.jobs WHERE id = $1",
            &[&job_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(job_count, 1);

    // Cleanup
    cleanup_jobs(&client, &[job_id]).await;
    client
        .execute("DELETE FROM bridge_test_orders WHERE id = $1", &[&order_id])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_mixed_insert_rollback_is_atomic() {
    let mut client = connect().await;
    ensure_migrated().await;

    // Use a distinct table to avoid parallel CREATE TABLE conflicts
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS bridge_test_orders_rb (id SERIAL PRIMARY KEY, email TEXT NOT NULL)",
            &[],
        )
        .await
        .unwrap();

    let txn = client.transaction().await.unwrap();

    let order_id: i32 = txn
        .query_one(
            "INSERT INTO bridge_test_orders_rb (email) VALUES ($1) RETURNING id",
            &[&"atomic-rollback@example.com"],
        )
        .await
        .unwrap()
        .get(0);

    let job = tokio_pg::insert_job(
        &txn,
        &BridgeEmail {
            to: "atomic-rollback@example.com".into(),
            subject: "Gone together".into(),
        },
    )
    .await
    .unwrap();
    let job_id = job.id;

    txn.rollback().await.unwrap();

    // Neither visible
    let order_count: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM bridge_test_orders_rb WHERE id = $1",
            &[&order_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(order_count, 0);

    let job_count: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM awa.jobs WHERE id = $1",
            &[&job_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(job_count, 0);
    // No cleanup needed — rolled back.
}

// ---------------------------------------------------------------------------
// Scheduled jobs (future and past run_at)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_future_run_at_creates_scheduled_job() {
    let client = connect().await;
    ensure_migrated().await;

    let run_at = chrono::Utc::now() + chrono::Duration::hours(24);
    let job = tokio_pg::insert_job_with(
        &client,
        &BridgeReport {
            name: "future-report".into(),
        },
        InsertOpts {
            run_at: Some(run_at),
            ..InsertOpts::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(job.state, awa_model::JobState::Scheduled);

    cleanup_jobs(&client, &[job.id]).await;
}

#[tokio::test]
async fn test_past_run_at_creates_scheduled_job() {
    let client = connect().await;
    ensure_migrated().await;

    let past = chrono::DateTime::parse_from_rfc3339("2000-01-01T00:00:00+00:00")
        .unwrap()
        .with_timezone(&chrono::Utc);

    let job = tokio_pg::insert_job_with(
        &client,
        &BridgeReport {
            name: "past-report".into(),
        },
        InsertOpts {
            run_at: Some(past),
            ..InsertOpts::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(job.state, awa_model::JobState::Scheduled);

    cleanup_jobs(&client, &[job.id]).await;
}

// ---------------------------------------------------------------------------
// Null byte validation (args and metadata)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_null_bytes_in_args_rejected() {
    let client = connect().await;
    ensure_migrated().await;

    let result = tokio_pg::insert_job_raw(
        &client,
        "bad_job".into(),
        serde_json::json!({"key": "val\u{0000}ue"}),
        InsertOpts::default(),
    )
    .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("null bytes"));
}

#[tokio::test]
async fn test_null_bytes_in_metadata_rejected() {
    let client = connect().await;
    ensure_migrated().await;

    let result = tokio_pg::insert_job_with(
        &client,
        &BridgeEmail {
            to: "meta@example.com".into(),
            subject: "Bad metadata".into(),
        },
        InsertOpts {
            metadata: serde_json::json!({"bad": "val\u{0000}ue"}),
            ..InsertOpts::default()
        },
    )
    .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("null bytes"));
}

// ---------------------------------------------------------------------------
// Uniqueness conflict mapping
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_unique_conflict_returns_awa_error() {
    let client = connect().await;
    ensure_migrated().await;

    let unique_addr = format!(
        "unique-{}@example.com",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );

    let opts = InsertOpts {
        queue: "bridge_unique".into(),
        unique: Some(UniqueOpts {
            by_args: true,
            ..UniqueOpts::default()
        }),
        ..InsertOpts::default()
    };

    let job = tokio_pg::insert_job_with(
        &client,
        &BridgeEmail {
            to: unique_addr.clone(),
            subject: "First".into(),
        },
        opts.clone(),
    )
    .await
    .unwrap();
    assert!(job.id > 0);

    // Duplicate insert should return UniqueConflict
    let result = tokio_pg::insert_job_with(
        &client,
        &BridgeEmail {
            to: unique_addr,
            subject: "First".into(),
        },
        opts,
    )
    .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        AwaError::UniqueConflict { constraint } => {
            assert!(constraint.is_some(), "constraint name should be populated");
        }
        other => panic!("expected UniqueConflict, got: {other}"),
    }

    cleanup_jobs(&client, &[job.id]).await;
}

// ---------------------------------------------------------------------------
// insert_job_raw
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_job_raw() {
    let client = connect().await;
    ensure_migrated().await;

    let job = tokio_pg::insert_job_raw(
        &client,
        "custom_raw_job".into(),
        serde_json::json!({"to": "raw@example.com", "urgent": true}),
        InsertOpts {
            queue: "bridge_raw".into(),
            ..InsertOpts::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(job.kind, "custom_raw_job");
    assert_eq!(job.queue, "bridge_raw");

    cleanup_jobs(&client, &[job.id]).await;
}
