//! Integration tests for periodic/cron job functionality.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::{
    cron::{atomic_enqueue, delete_cron_job, list_cron_jobs, upsert_cron_job},
    migrations,
};
use awa::{Client, JobArgs, JobContext, JobResult, JobState, PeriodicJob, QueueConfig};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database")
}

async fn setup() -> PgPool {
    let pool = pool().await;
    migrations::run(&pool)
        .await
        .expect("Failed to run migrations");
    pool
}

/// Clean specific cron job names for test isolation (safe for parallel tests).
async fn clean_cron_names(pool: &PgPool, names: &[&str]) {
    for name in names {
        let _ = sqlx::query("DELETE FROM awa.cron_jobs WHERE name = $1")
            .bind(name)
            .execute(pool)
            .await;
    }
}

/// Clean jobs for a specific queue.
async fn clean_queue(pool: &PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue");
}

// -- Job types for testing --

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct DailyReport {
    format: String,
}

// -- Tests --

#[tokio::test]
async fn test_v2_migration_creates_cron_jobs_table() {
    let pool = setup().await;
    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);

    // Verify cron_jobs table exists
    let has_cron_table: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'cron_jobs')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        has_cron_table,
        "awa.cron_jobs table must exist after v2 migration"
    );
}

#[tokio::test]
async fn test_migration_idempotent() {
    let pool = setup().await;

    // Running migrations again should be a no-op
    migrations::run(&pool).await.unwrap();
    migrations::run(&pool).await.unwrap();

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);
}

#[tokio::test]
async fn test_upsert_inserts_new_schedule() {
    let pool = setup().await;
    clean_cron_names(&pool, &["test_insert_new"]).await;

    let job = PeriodicJob::builder("test_insert_new", "0 9 * * *")
        .build_raw(
            "daily_report".to_string(),
            serde_json::json!({"format": "pdf"}),
        )
        .unwrap();

    upsert_cron_job(&pool, &job).await.unwrap();

    let rows = list_cron_jobs(&pool).await.unwrap();
    let found = rows.iter().find(|r| r.name == "test_insert_new").unwrap();
    assert_eq!(found.cron_expr, "0 9 * * *");
    assert_eq!(found.kind, "daily_report");
    assert_eq!(found.timezone, "UTC");
    assert_eq!(found.queue, "default");
    assert_eq!(found.priority, 2);
    assert!(found.last_enqueued_at.is_none());
}

#[tokio::test]
async fn test_upsert_updates_existing_schedule() {
    let pool = setup().await;
    clean_cron_names(&pool, &["test_upsert_update"]).await;

    let job_v1 = PeriodicJob::builder("test_upsert_update", "0 9 * * *")
        .build_raw(
            "daily_report".to_string(),
            serde_json::json!({"format": "pdf"}),
        )
        .unwrap();
    upsert_cron_job(&pool, &job_v1).await.unwrap();

    // Update the schedule
    let job_v2 = PeriodicJob::builder("test_upsert_update", "30 8 * * *")
        .timezone("Pacific/Auckland")
        .queue("reports")
        .build_raw(
            "daily_report".to_string(),
            serde_json::json!({"format": "csv"}),
        )
        .unwrap();
    upsert_cron_job(&pool, &job_v2).await.unwrap();

    let rows = list_cron_jobs(&pool).await.unwrap();
    let found = rows
        .iter()
        .find(|r| r.name == "test_upsert_update")
        .unwrap();
    assert_eq!(found.cron_expr, "30 8 * * *");
    assert_eq!(found.timezone, "Pacific/Auckland");
    assert_eq!(found.queue, "reports");
    assert_eq!(found.args, serde_json::json!({"format": "csv"}));
}

#[tokio::test]
async fn test_multi_deployment_no_orphan_deletion() {
    let pool = setup().await;
    clean_cron_names(&pool, &["deploy_a_job", "deploy_b_job"]).await;

    // Deployment A registers its schedules
    let job_a = PeriodicJob::builder("deploy_a_job", "0 * * * *")
        .build_raw("sync_a".to_string(), serde_json::json!({}))
        .unwrap();
    upsert_cron_job(&pool, &job_a).await.unwrap();

    // Deployment B registers its schedules
    let job_b = PeriodicJob::builder("deploy_b_job", "30 * * * *")
        .build_raw("sync_b".to_string(), serde_json::json!({}))
        .unwrap();
    upsert_cron_job(&pool, &job_b).await.unwrap();

    // Both should exist — neither deployment deletes the other's schedules
    let rows = list_cron_jobs(&pool).await.unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
    assert!(
        names.contains(&"deploy_a_job"),
        "Deployment A's schedule must be preserved"
    );
    assert!(
        names.contains(&"deploy_b_job"),
        "Deployment B's schedule must be preserved"
    );
}

#[tokio::test]
async fn test_atomic_cte_mark_and_insert() {
    let pool = setup().await;
    clean_cron_names(&pool, &["test_atomic_enqueue"]).await;
    let queue = "cron_atomic_cte";
    clean_queue(&pool, queue).await;

    // Insert a cron job directly
    let job = PeriodicJob::builder("test_atomic_enqueue", "* * * * *")
        .queue(queue)
        .build_raw(
            "daily_report".to_string(),
            serde_json::json!({"format": "pdf"}),
        )
        .unwrap();
    upsert_cron_job(&pool, &job).await.unwrap();

    let fire_time = Utc::now();

    // Atomic enqueue should succeed
    let result = atomic_enqueue(&pool, "test_atomic_enqueue", fire_time, None)
        .await
        .unwrap();
    assert!(result.is_some(), "Should have enqueued a job");

    let job_row = result.unwrap();
    assert_eq!(job_row.kind, "daily_report");
    assert_eq!(job_row.queue, queue);
    assert_eq!(job_row.state, JobState::Available);
    assert_eq!(job_row.priority, 2);

    // Verify cron metadata was stamped
    assert_eq!(
        job_row.metadata.get("cron_name").and_then(|v| v.as_str()),
        Some("test_atomic_enqueue")
    );
    assert!(job_row.metadata.get("cron_fire_time").is_some());

    // Verify last_enqueued_at was updated
    let rows = list_cron_jobs(&pool).await.unwrap();
    let cron_row = rows
        .iter()
        .find(|r| r.name == "test_atomic_enqueue")
        .unwrap();
    assert!(cron_row.last_enqueued_at.is_some());
}

#[tokio::test]
async fn test_atomic_cte_dedup_second_call() {
    let pool = setup().await;
    clean_cron_names(&pool, &["test_dedup_cte"]).await;
    let queue = "cron_atomic_dedup";
    clean_queue(&pool, queue).await;

    let job = PeriodicJob::builder("test_dedup_cte", "* * * * *")
        .queue(queue)
        .build_raw("daily_report".to_string(), serde_json::json!({}))
        .unwrap();
    upsert_cron_job(&pool, &job).await.unwrap();

    let fire_time = Utc::now();

    // First call succeeds
    let result1 = atomic_enqueue(&pool, "test_dedup_cte", fire_time, None)
        .await
        .unwrap();
    assert!(result1.is_some());

    // Second call with same previous_enqueued_at=None should fail (CAS mismatch)
    let result2 = atomic_enqueue(&pool, "test_dedup_cte", fire_time, None)
        .await
        .unwrap();
    assert!(result2.is_none(), "Second call should return None (dedup)");
}

#[tokio::test]
async fn test_no_backfill_only_latest_fire() {
    let pool = setup().await;
    clean_cron_names(&pool, &["test_no_backfill"]).await;
    let queue = "cron_no_backfill";
    clean_queue(&pool, queue).await;

    // Insert a per-minute cron job with last_enqueued_at 1 hour ago
    let job = PeriodicJob::builder("test_no_backfill", "* * * * *")
        .queue(queue)
        .build_raw("hourly_sync".to_string(), serde_json::json!({}))
        .unwrap();
    upsert_cron_job(&pool, &job).await.unwrap();

    let one_hour_ago = Utc::now() - chrono::Duration::hours(1);
    sqlx::query("UPDATE awa.cron_jobs SET last_enqueued_at = $1 WHERE name = $2")
        .bind(one_hour_ago)
        .bind("test_no_backfill")
        .execute(&pool)
        .await
        .unwrap();

    // Use the PeriodicJob's fire time calculation to verify only 1 fire
    let now = Utc::now();
    let fire = job.latest_fire_time(now, Some(one_hour_ago));
    assert!(fire.is_some(), "Should find a fire time");

    // The fire should be the latest minute, not one hour's worth of fires
    let fire_time = fire.unwrap();
    assert!(
        (now - fire_time).num_seconds() < 120,
        "Fire time should be within the last 2 minutes, not backfilled. Got {:?}",
        fire_time
    );

    // Actually enqueue it
    let result = atomic_enqueue(&pool, "test_no_backfill", fire_time, Some(one_hour_ago))
        .await
        .unwrap();
    assert!(result.is_some());

    // Count jobs created — should be exactly 1
    let count: (i64,) = sqlx::query_as("SELECT count(*)::bigint FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count.0, 1,
        "Should create exactly 1 job, not backfill all missed fires"
    );
}

#[tokio::test]
async fn test_delete_cron_job() {
    let pool = setup().await;
    clean_cron_names(&pool, &["test_delete"]).await;

    let job = PeriodicJob::builder("test_delete", "0 9 * * *")
        .build_raw("test_job".to_string(), serde_json::json!({}))
        .unwrap();
    upsert_cron_job(&pool, &job).await.unwrap();

    let deleted = delete_cron_job(&pool, "test_delete").await.unwrap();
    assert!(deleted);

    let deleted_again = delete_cron_job(&pool, "test_delete").await.unwrap();
    assert!(!deleted_again, "Second delete should return false");
}

#[tokio::test]
async fn test_end_to_end_periodic_job_enqueued() {
    let pool = setup().await;
    clean_cron_names(&pool, &["e2e_test"]).await;
    let queue = "cron_e2e";
    clean_queue(&pool, queue).await;

    // Build a client with a periodic job that fires every minute
    let client = Client::builder(pool.clone())
        .queue(queue, QueueConfig::default())
        .register::<DailyReport, _, _>(|_args: DailyReport, _ctx: &JobContext| async move {
            Ok(JobResult::Completed)
        })
        .periodic(
            PeriodicJob::builder("e2e_test", "* * * * *")
                .queue(queue)
                .build(&DailyReport {
                    format: "pdf".into(),
                })
                .unwrap(),
        )
        .build()
        .unwrap();

    client.start().await.unwrap();

    // Poll until the cron evaluator fires, or timeout.
    // The maintenance service needs to: win leader election, sync schedules, then evaluate.
    // If another test binary's maintenance service holds the advisory lock,
    // leader election retries every 10s, so we need a generous timeout.
    let start = std::time::Instant::now();
    // The cron fires "* * * * *" (every minute). If we start late in a minute,
    // we may need to wait almost a full minute for the next fire plus leader
    // election time (up to 10s). Use 90s to avoid flaky failures.
    let timeout = std::time::Duration::from_secs(90);
    let jobs = loop {
        let found: Vec<awa::JobRow> =
            sqlx::query_as("SELECT * FROM awa.jobs WHERE queue = $1 AND kind = 'daily_report'")
                .bind(queue)
                .fetch_all(&pool)
                .await
                .unwrap();
        if !found.is_empty() {
            break found;
        }
        assert!(
            start.elapsed() < timeout,
            "Periodic job was not enqueued within {timeout:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };

    // Verify cron metadata on the job
    let job = &jobs[0];
    assert_eq!(
        job.metadata.get("cron_name").and_then(|v| v.as_str()),
        Some("e2e_test")
    );
    assert!(job.metadata.get("cron_fire_time").is_some());

    // Verify the cron_jobs table was synced
    let cron_rows = list_cron_jobs(&pool).await.unwrap();
    let found = cron_rows.iter().find(|r| r.name == "e2e_test");
    assert!(found.is_some(), "Schedule should be synced to DB");

    client.shutdown(std::time::Duration::from_secs(2)).await;
}

#[tokio::test]
async fn test_periodic_job_builder_with_job_args_trait() {
    let job = PeriodicJob::builder("typed_test", "0 9 * * *")
        .timezone("Pacific/Auckland")
        .queue("reports")
        .priority(1)
        .build(&DailyReport {
            format: "csv".into(),
        })
        .unwrap();

    assert_eq!(job.name, "typed_test");
    assert_eq!(job.kind, "daily_report");
    assert_eq!(job.args, serde_json::json!({"format": "csv"}));
    assert_eq!(job.timezone, "Pacific/Auckland");
    assert_eq!(job.queue, "reports");
    assert_eq!(job.priority, 1);
}

#[tokio::test]
async fn test_cron_job_with_tags_and_metadata() {
    let pool = setup().await;
    clean_cron_names(&pool, &["tagged_job"]).await;
    let queue = "cron_tags_meta";
    clean_queue(&pool, queue).await;

    let job = PeriodicJob::builder("tagged_job", "0 9 * * *")
        .queue(queue)
        .tags(vec!["important".to_string(), "daily".to_string()])
        .metadata(serde_json::json!({"team": "analytics"}))
        .build_raw("report".to_string(), serde_json::json!({}))
        .unwrap();
    upsert_cron_job(&pool, &job).await.unwrap();

    let rows = list_cron_jobs(&pool).await.unwrap();
    let found = rows.iter().find(|r| r.name == "tagged_job").unwrap();
    assert_eq!(found.tags, vec!["important", "daily"]);
    assert_eq!(
        found.metadata.get("team").and_then(|v| v.as_str()),
        Some("analytics")
    );

    // Enqueue and verify tags/metadata propagate to the job
    let fire_time = Utc::now();
    let result = atomic_enqueue(&pool, "tagged_job", fire_time, None)
        .await
        .unwrap();
    let job_row = result.unwrap();
    assert_eq!(job_row.tags, vec!["important", "daily"]);
    // Metadata should include both the original and the cron stamps
    assert_eq!(
        job_row.metadata.get("team").and_then(|v| v.as_str()),
        Some("analytics")
    );
    assert_eq!(
        job_row.metadata.get("cron_name").and_then(|v| v.as_str()),
        Some("tagged_job")
    );
}
