//! Integration tests for the sqlx 0.9 bridge adapter.
//!
//! Run with:
//!   cargo test --package awa-model --features sqlx09 --test bridge_sqlx09_test

#![cfg(feature = "sqlx09")]

use awa_model::bridge::sqlx09 as awa_sqlx09;
use awa_model::job::{InsertOpts, InsertParams, JobState, UniqueOpts};
use awa_model::{AwaError, JobArgs};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct BridgeEmail {
    to: String,
    subject: String,
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

/// A sqlx 0.9 pool for the application side, after migrating with awa's
/// own sqlx 0.8 pool.
async fn app_pool() -> sqlx09::PgPool {
    let awa_pool = sqlx::PgPool::connect(&database_url()).await.unwrap();
    awa_model::migrations::run(&awa_pool).await.unwrap();
    awa_pool.close().await;

    sqlx09::PgPool::connect(&database_url()).await.unwrap()
}

fn unique_address(prefix: &str) -> String {
    format!(
        "{prefix}-{}@example.com",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    )
}

async fn job_exists(pool: &sqlx09::PgPool, job_id: i64) -> bool {
    sqlx09::query_scalar("SELECT EXISTS (SELECT 1 FROM awa.jobs WHERE id = $1)")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn cleanup_jobs(pool: &sqlx09::PgPool, job_ids: &[i64]) {
    sqlx09::query("DELETE FROM awa.jobs WHERE id = ANY($1)")
        .bind(job_ids)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_insert_in_committed_transaction_decodes_full_row() {
    let pool = app_pool().await;
    let address = unique_address("commit");

    let mut transaction = pool.begin().await.unwrap();
    let job = awa_sqlx09::insert_job_with(
        &mut *transaction,
        &BridgeEmail {
            to: address.clone(),
            subject: "Hello".into(),
        },
        InsertOpts {
            queue: "bridge_sqlx09".into(),
            priority: 1,
            max_attempts: 7,
            tags: vec!["sqlx09".into()],
            metadata: serde_json::json!({"source": "bridge"}),
            ..InsertOpts::default()
        },
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    assert_eq!(job.kind, "bridge_email");
    assert_eq!(job.queue, "bridge_sqlx09");
    assert_eq!(job.state, JobState::Available);
    assert_eq!(job.priority, 1);
    assert_eq!(job.max_attempts, 7);
    assert_eq!(job.tags, vec!["sqlx09".to_string()]);
    assert_eq!(job.metadata["source"], "bridge");
    assert_eq!(job.args["to"], address.as_str());
    assert!(job_exists(&pool, job.id).await);

    cleanup_jobs(&pool, &[job.id]).await;
}

#[tokio::test]
async fn test_rolled_back_transaction_leaves_no_job() {
    let pool = app_pool().await;

    let mut transaction = pool.begin().await.unwrap();
    let job = awa_sqlx09::insert_job(
        &mut *transaction,
        &BridgeEmail {
            to: unique_address("rollback"),
            subject: "Never sent".into(),
        },
    )
    .await
    .unwrap();
    transaction.rollback().await.unwrap();

    assert!(!job_exists(&pool, job.id).await);
}

#[tokio::test]
async fn test_future_run_at_creates_scheduled_job() {
    let pool = app_pool().await;

    let job = awa_sqlx09::insert_job_with(
        &pool,
        &BridgeEmail {
            to: unique_address("scheduled"),
            subject: "Later".into(),
        },
        InsertOpts {
            run_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            ..InsertOpts::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(job.state, JobState::Scheduled);

    cleanup_jobs(&pool, &[job.id]).await;
}

#[tokio::test]
async fn test_unique_conflict_in_transaction_returns_awa_error() {
    let pool = app_pool().await;
    let address = unique_address("unique");
    let opts = InsertOpts {
        queue: "bridge_sqlx09_unique".into(),
        unique: Some(UniqueOpts {
            by_args: true,
            ..UniqueOpts::default()
        }),
        ..InsertOpts::default()
    };
    let args = BridgeEmail {
        to: address,
        subject: "Once".into(),
    };

    let first = awa_sqlx09::insert_job_with(&pool, &args, opts.clone())
        .await
        .unwrap();

    let mut transaction = pool.begin().await.unwrap();
    let duplicate = awa_sqlx09::insert_job_with(&mut *transaction, &args, opts).await;
    transaction.rollback().await.unwrap();

    match duplicate {
        Err(AwaError::UniqueConflict { constraint }) => {
            assert!(constraint.is_some(), "constraint name should be populated");
        }
        other => panic!("expected UniqueConflict, got: {other:?}"),
    }

    cleanup_jobs(&pool, &[first.id]).await;
}

#[tokio::test]
async fn test_insert_many_in_transaction() {
    let pool = app_pool().await;
    let jobs: Vec<InsertParams> = (0..3)
        .map(|index| InsertParams {
            kind: "bridge_email".into(),
            args: serde_json::json!({"to": unique_address(&format!("many{index}")), "subject": "Batch"}),
            opts: InsertOpts {
                queue: "bridge_sqlx09_many".into(),
                ..InsertOpts::default()
            },
        })
        .collect();

    let mut transaction = pool.begin().await.unwrap();
    let inserted = awa_sqlx09::insert_many(&mut *transaction, &jobs)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    assert_eq!(inserted.len(), 3);
    assert!(inserted
        .iter()
        .all(|job| job.queue == "bridge_sqlx09_many" && job.state == JobState::Available));

    let job_ids: Vec<i64> = inserted.iter().map(|job| job.id).collect();
    for job_id in &job_ids {
        assert!(job_exists(&pool, *job_id).await);
    }
    cleanup_jobs(&pool, &job_ids).await;
}
