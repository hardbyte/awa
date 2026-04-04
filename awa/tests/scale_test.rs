//! Large-scale integration tests for Awa.
//!
//! Tests concurrent SKIP LOCKED contention, crash recovery, priority aging,
//! bulk insert at scale, and queue isolation under load.
//!
//! Each test uses a unique queue name to avoid interference when running in parallel.

use awa_macros::JobArgs;
use awa_model::{insert_many, insert_with, migrations, InsertOpts, JobRow, JobState, UniqueOpts};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn pool() -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url())
        .await
        .expect("Failed to connect")
}

async fn setup() -> sqlx::PgPool {
    let pool = pool().await;
    migrations::run(&pool).await.expect("Failed to migrate");
    pool
}

/// Clean only jobs and queue_meta for a specific queue.
/// Call this at the start of each test to remove leftovers from previous runs.
async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue jobs");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue meta");
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct ScaleJob {
    pub index: i64,
}

// ── Bulk insert at scale ──────────────────────────────────────────────

#[tokio::test]
async fn test_bulk_insert_1000_jobs() {
    let pool = setup().await;
    let queue = "scale_bulk_1000";
    clean_queue(&pool, queue).await;

    let params: Vec<_> = (0..1000)
        .map(|i| {
            awa_model::insert::params_with(
                &ScaleJob { index: i },
                InsertOpts {
                    queue: queue.into(),
                    ..Default::default()
                },
            )
            .unwrap()
        })
        .collect();

    let jobs = insert_many(&pool, &params).await.unwrap();
    assert_eq!(jobs.len(), 1000);

    // Verify all have unique IDs
    let mut ids: Vec<i64> = jobs.iter().map(|j| j.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 1000, "All 1000 jobs should have unique IDs");

    // Verify all are available in this queue
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE state = 'available' AND queue = $1",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1000);
}

// ── Concurrent SKIP LOCKED contention ─────────────────────────────────

#[tokio::test]
async fn test_concurrent_claim_no_double_dispatch() {
    let pool = setup().await;
    let queue = "scale_contention";
    clean_queue(&pool, queue).await;

    // Insert 100 jobs
    let params: Vec<_> = (0..100)
        .map(|i| {
            awa_model::insert::params_with(
                &ScaleJob { index: i },
                InsertOpts {
                    queue: queue.into(),
                    ..Default::default()
                },
            )
            .unwrap()
        })
        .collect();
    insert_many(&pool, &params).await.unwrap();

    // Spawn 10 concurrent claimers, each trying to claim 20 jobs
    let claimed_ids = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let mut handles = vec![];

    for _ in 0..10 {
        let pool = pool.clone();
        let claimed = claimed_ids.clone();
        let q = queue.to_string();
        handles.push(tokio::spawn(async move {
            let jobs: Vec<JobRow> = sqlx::query_as(
                r#"
                WITH claimed AS (
                    SELECT id FROM awa.jobs_hot
                    WHERE state = 'available' AND queue = $1
                    ORDER BY run_at ASC, id ASC
                    LIMIT 20
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE awa.jobs_hot
                SET state = 'running', attempt = attempt + 1,
                    attempted_at = now(), heartbeat_at = now(),
                    deadline_at = now() + interval '5 minutes'
                FROM claimed
                WHERE awa.jobs_hot.id = claimed.id
                RETURNING awa.jobs_hot.*
                "#,
            )
            .bind(&q)
            .fetch_all(&pool)
            .await
            .unwrap();

            let ids: Vec<i64> = jobs.iter().map(|j| j.id).collect();
            claimed.lock().await.extend(ids);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let all_claimed = claimed_ids.lock().await;
    let total_claimed = all_claimed.len();

    // Every claimed ID should be unique — no double dispatch
    let mut sorted = all_claimed.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        total_claimed,
        "SKIP LOCKED should prevent double dispatch"
    );

    // All 100 should be claimed
    assert_eq!(total_claimed, 100, "All 100 jobs should be claimed");
}

#[tokio::test]
async fn test_stale_candidate_cannot_reclaim_running_row() {
    let pool = setup().await;
    let queue = "scale_stale_candidate";
    clean_queue(&pool, queue).await;

    let job = insert_with(
        &pool,
        &ScaleJob { index: 1 },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let candidate_id: i64 = sqlx::query_scalar(
        r#"
        SELECT id FROM awa.jobs_hot
        WHERE state = 'available' AND queue = $1
        ORDER BY run_at ASC, id ASC
        LIMIT 1
        "#,
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(candidate_id, job.id);

    let first_claim: Option<(String, i16, i64)> = sqlx::query_as(
        r#"
        UPDATE awa.jobs_hot
        SET state = 'running',
            attempt = attempt + 1,
            run_lease = run_lease + 1,
            attempted_at = now(),
            heartbeat_at = now(),
            deadline_at = now() + interval '5 minutes'
        WHERE id = $1
          AND state = 'available'
        RETURNING state::text, attempt, run_lease
        "#,
    )
    .bind(candidate_id)
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert_eq!(first_claim, Some(("running".to_string(), 1, 1)));

    let second_claim: Option<(String, i16, i64)> = sqlx::query_as(
        r#"
        UPDATE awa.jobs_hot
        SET state = 'running',
            attempt = attempt + 1,
            run_lease = run_lease + 1,
            attempted_at = now(),
            heartbeat_at = now(),
            deadline_at = now() + interval '5 minutes'
        WHERE id = $1
          AND state = 'available'
        RETURNING state::text, attempt, run_lease
        "#,
    )
    .bind(candidate_id)
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert_eq!(second_claim, None, "stale candidate must not re-claim row");

    let final_row: (String, i16, i64) =
        sqlx::query_as("SELECT state::text, attempt, run_lease FROM awa.jobs WHERE id = $1")
            .bind(candidate_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(final_row, ("running".to_string(), 1, 1));
}

// ── Priority aging via maintenance task ──────────────────────────────

#[tokio::test]
async fn test_priority_aging_reorders_jobs() {
    let pool = setup().await;
    let queue = "scale_aging";
    clean_queue(&pool, queue).await;

    // Insert a priority-4 job that has been waiting 300 seconds.
    // With aging_interval=60s, it should age from 4 → 1.
    let old_time = chrono::Utc::now() - chrono::Duration::seconds(300);
    sqlx::query(
        r#"
        INSERT INTO awa.jobs_hot (kind, queue, args, state, priority, run_at)
        VALUES ('scale_job', $1, '{"index": 1}', 'available', 4, $2)
        "#,
    )
    .bind(queue)
    .bind(old_time)
    .execute(&pool)
    .await
    .unwrap();

    // Insert a priority-3 job waiting 90 seconds → should age from 3 → 2.
    let medium_time = chrono::Utc::now() - chrono::Duration::seconds(90);
    sqlx::query(
        r#"
        INSERT INTO awa.jobs_hot (kind, queue, args, state, priority, run_at)
        VALUES ('scale_job', $1, '{"index": 2}', 'available', 3, $2)
        "#,
    )
    .bind(queue)
    .bind(medium_time)
    .execute(&pool)
    .await
    .unwrap();

    // Insert a fresh priority-1 job — should not be aged.
    sqlx::query(
        r#"
        INSERT INTO awa.jobs_hot (kind, queue, args, state, priority, run_at)
        VALUES ('scale_job', $1, '{"index": 3}', 'available', 1, now())
        "#,
    )
    .bind(queue)
    .execute(&pool)
    .await
    .unwrap();

    // Run the maintenance aging query multiple times (simulating multiple ticks).
    // Each pass decrements priority by 1 for eligible jobs. Scoped to our queue.
    let aging_interval_secs: f64 = 60.0;
    let aging_sql = r#"
        UPDATE awa.jobs_hot
        SET priority = priority - 1,
            metadata = CASE
                WHEN NOT (metadata ? '_awa_original_priority')
                THEN metadata || jsonb_build_object('_awa_original_priority', priority)
                ELSE metadata
            END
        WHERE id IN (
            SELECT id FROM awa.jobs_hot
            WHERE state = 'available'
              AND queue = $2
              AND priority > 1
              AND run_at <= now() - make_interval(secs => $1)
            LIMIT 1000
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id
    "#;

    // Run 5 aging passes (enough for priority-4 → 1 at 300s wait)
    let mut total_aged = 0;
    for _ in 0..5 {
        let aged: Vec<i64> = sqlx::query_scalar(aging_sql)
            .bind(aging_interval_secs)
            .bind(queue)
            .fetch_all(&pool)
            .await
            .unwrap();
        total_aged += aged.len();
        if aged.is_empty() {
            break;
        }
    }
    assert!(total_aged > 0, "At least some jobs should have been aged");

    // Verify the priority column was updated correctly
    let jobs: Vec<(i16, serde_json::Value)> = sqlx::query_as(
        "SELECT priority, args FROM awa.jobs_hot WHERE queue = $1 ORDER BY args->>'index'",
    )
    .bind(queue)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(jobs.len(), 3);
    // Index 1: priority 4, waited 300s, ages 4→3→2→1 over passes → 1
    assert_eq!(jobs[0].0, 1, "Priority-4 job waited 300s should age to 1");
    // Index 2: priority 3, waited 90s, ages 3→2→1 over passes → 1
    assert_eq!(jobs[1].0, 1, "Priority-3 job waited 90s should age to 1");
    // Index 3: priority 1, fresh — unchanged
    assert_eq!(jobs[2].0, 1, "Priority-1 job should remain at 1");

    // Verify original priority stored in metadata
    let orig: Option<serde_json::Value> = sqlx::query_scalar(
        "SELECT metadata->'_awa_original_priority' FROM awa.jobs_hot WHERE queue = $1 AND args->>'index' = '1'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        orig,
        Some(serde_json::json!(4)),
        "Original priority should be preserved in metadata"
    );

    // Verify that the dispatch query (strict priority ordering) now picks
    // the aged-up old job first (it has priority 1 and earlier run_at)
    let claimed: Vec<JobRow> = sqlx::query_as(
        r#"
        UPDATE awa.jobs_hot
        SET state = 'running', attempt = attempt + 1
        FROM (
            SELECT id FROM awa.jobs_hot
            WHERE state = 'available' AND queue = $1 AND run_at <= now()
            ORDER BY priority ASC, run_at ASC, id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        ) AS claimed
        WHERE awa.jobs_hot.id = claimed.id AND awa.jobs_hot.state = 'available'
        RETURNING awa.jobs_hot.*
        "#,
    )
    .bind(queue)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(claimed.len(), 1);
    let args: serde_json::Value = claimed[0].args.clone();
    assert_eq!(
        args["index"], 1,
        "Aged priority-4 job (now priority-1) should be claimed first"
    );

    // Verify original priority is readable from metadata (set by aging task)
    let original: Option<i64> = claimed[0]
        .metadata
        .get("_awa_original_priority")
        .and_then(|v| v.as_i64());
    assert_eq!(
        original,
        Some(4),
        "Original priority should be readable from metadata"
    );
}

// ── Crash recovery: stale heartbeat ───────────────────────────────────

#[tokio::test]
async fn test_stale_heartbeat_rescue() {
    let pool = setup().await;
    let queue = "scale_stale_hb";
    clean_queue(&pool, queue).await;

    let job = insert_with(
        &pool,
        &ScaleJob { index: 1 },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Mark as running with stale heartbeat (120s ago)
    let stale_time = chrono::Utc::now() - chrono::Duration::seconds(120);
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, heartbeat_at = $1, deadline_at = now() + interval '1 hour' WHERE id = $2",
    )
    .bind(stale_time)
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    // Run rescue
    let rescued: Vec<JobRow> = sqlx::query_as(
        r#"
        UPDATE awa.jobs
        SET state = 'retryable', finalized_at = now(), heartbeat_at = NULL, deadline_at = NULL,
            errors = errors || jsonb_build_object('error', 'heartbeat stale', 'attempt', attempt, 'at', now())::jsonb
        WHERE id IN (
            SELECT id FROM awa.jobs
            WHERE state = 'running' AND heartbeat_at < now() - interval '90 seconds'
            LIMIT 500 FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        "#,
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert!(
        rescued.iter().any(|j| j.id == job.id),
        "Stale job should be rescued"
    );
    let rescued_job = rescued.iter().find(|j| j.id == job.id).unwrap();
    assert_eq!(rescued_job.state, JobState::Retryable);
}

// ── Crash recovery: deadline exceeded ─────────────────────────────────

#[tokio::test]
async fn test_deadline_exceeded_rescue() {
    let pool = setup().await;
    let queue = "scale_deadline";
    clean_queue(&pool, queue).await;

    let job = insert_with(
        &pool,
        &ScaleJob { index: 1 },
        InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let past_deadline = chrono::Utc::now() - chrono::Duration::seconds(30);
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, heartbeat_at = now(), deadline_at = $1 WHERE id = $2",
    )
    .bind(past_deadline)
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    let rescued: Vec<JobRow> = sqlx::query_as(
        r#"
        UPDATE awa.jobs
        SET state = 'retryable', finalized_at = now(), heartbeat_at = NULL, deadline_at = NULL,
            errors = errors || jsonb_build_object('error', 'deadline exceeded', 'attempt', attempt, 'at', now())::jsonb
        WHERE id IN (
            SELECT id FROM awa.jobs
            WHERE state = 'running' AND deadline_at IS NOT NULL AND deadline_at < now()
            LIMIT 500 FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        "#,
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert!(rescued.iter().any(|j| j.id == job.id));
    assert_eq!(
        rescued.iter().find(|j| j.id == job.id).unwrap().state,
        JobState::Retryable
    );
}

// ── Queue isolation ───────────────────────────────────────────────────

#[tokio::test]
async fn test_queue_isolation_under_load() {
    let pool = setup().await;
    clean_queue(&pool, "scale_iso_a").await;
    clean_queue(&pool, "scale_iso_b").await;

    // Insert 50 jobs in queue_a, 50 in queue_b (unique names)
    for i in 0..50 {
        insert_with(
            &pool,
            &ScaleJob { index: i },
            InsertOpts {
                queue: "scale_iso_a".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        insert_with(
            &pool,
            &ScaleJob { index: i + 50 },
            InsertOpts {
                queue: "scale_iso_b".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    // Claim all from queue_a only
    let queue_a_jobs: Vec<JobRow> = sqlx::query_as(
        r#"
        WITH claimed AS (
            SELECT id FROM awa.jobs_hot
            WHERE state = 'available' AND queue = 'scale_iso_a'
            ORDER BY run_at ASC, id ASC
            LIMIT 100
            FOR UPDATE SKIP LOCKED
        )
        UPDATE awa.jobs_hot SET state = 'running', attempt = 1
        FROM claimed WHERE awa.jobs_hot.id = claimed.id
        RETURNING awa.jobs_hot.*
        "#,
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(queue_a_jobs.len(), 50);
    assert!(queue_a_jobs.iter().all(|j| j.queue == "scale_iso_a"));

    // queue_b should still have 50 available
    let queue_b_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs_hot WHERE queue = 'scale_iso_b' AND state = 'available'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(queue_b_count, 50);
}

// ── Uniqueness under concurrent insert ────────────────────────────────

#[tokio::test]
async fn test_unique_constraint_prevents_duplicates() {
    let pool = setup().await;
    clean_queue(&pool, "scale_unique").await;

    let opts = InsertOpts {
        queue: "scale_unique".into(),
        unique: Some(UniqueOpts {
            by_queue: true,
            ..UniqueOpts::default()
        }),
        ..Default::default()
    };

    let job1 = insert_with(&pool, &ScaleJob { index: 42 }, opts.clone())
        .await
        .unwrap();
    assert!(job1.unique_key.is_some());

    // Second insert with same args should conflict
    let result = insert_with(&pool, &ScaleJob { index: 42 }, opts.clone()).await;
    assert!(
        result.is_err(),
        "Duplicate insert should fail with unique conflict"
    );
}

// ── Concurrent insert + claim throughput ──────────────────────────────

#[tokio::test]
async fn test_concurrent_insert_and_claim() {
    let pool = setup().await;
    let queue = "scale_concurrent";
    clean_queue(&pool, queue).await;

    let inserted = Arc::new(AtomicU64::new(0));
    let claimed = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Spawn 5 inserters, each inserting 100 jobs
    for batch in 0..5 {
        let pool = pool.clone();
        let count = inserted.clone();
        let q = queue.to_string();
        handles.push(tokio::spawn(async move {
            for i in 0..100 {
                insert_with(
                    &pool,
                    &ScaleJob {
                        index: batch * 100 + i,
                    },
                    InsertOpts {
                        queue: q.clone(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                count.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    // Give inserters a head start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Spawn 5 claimers — they only exit once all 500 jobs have been inserted
    // AND there are no more available jobs to claim.
    for _ in 0..5 {
        let pool = pool.clone();
        let count = claimed.clone();
        let inserted_count = inserted.clone();
        let q = queue.to_string();
        handles.push(tokio::spawn(async move {
            loop {
                let jobs: Vec<JobRow> = sqlx::query_as(
                    r#"
                    WITH claimed AS (
                        SELECT id FROM awa.jobs_hot
                        WHERE state = 'available' AND queue = $1
                        LIMIT 10
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE awa.jobs_hot SET state = 'completed', finalized_at = now()
                    FROM claimed WHERE awa.jobs_hot.id = claimed.id
                    RETURNING awa.jobs_hot.*
                    "#,
                )
                .bind(&q)
                .fetch_all(&pool)
                .await
                .unwrap();

                if jobs.is_empty() {
                    // Only exit if all inserters are done AND nothing is available
                    if inserted_count.load(Ordering::SeqCst) >= 500 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        let remaining: i64 = sqlx::query_scalar(
                            "SELECT count(*) FROM awa.jobs_hot WHERE state = 'available' AND queue = $1",
                        )
                        .bind(&q)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                        if remaining == 0 {
                            break;
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                } else {
                    count.fetch_add(jobs.len() as u64, Ordering::SeqCst);
                }
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(inserted.load(Ordering::SeqCst), 500);
    assert_eq!(claimed.load(Ordering::SeqCst), 500);
}

// ── Scheduled promotion ───────────────────────────────────────────────

#[tokio::test]
async fn test_scheduled_jobs_become_available() {
    let pool = setup().await;
    let queue = "scale_scheduled";
    clean_queue(&pool, queue).await;

    // Insert a job scheduled in the past (should be promoted)
    let past = chrono::Utc::now() - chrono::Duration::seconds(10);
    insert_with(
        &pool,
        &ScaleJob { index: 1 },
        InsertOpts {
            queue: queue.into(),
            run_at: Some(past),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Insert a job scheduled in the future (should NOT be promoted)
    let future_time = chrono::Utc::now() + chrono::Duration::hours(1);
    insert_with(
        &pool,
        &ScaleJob { index: 2 },
        InsertOpts {
            queue: queue.into(),
            run_at: Some(future_time),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Run promotion only for this queue
    let promoted = sqlx::query(
        "UPDATE awa.jobs SET state = 'available' WHERE state = 'scheduled' AND run_at <= now() AND queue = $1",
    )
    .bind(queue)
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(promoted.rows_affected(), 1);

    let available: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE state = 'available' AND queue = $1",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();
    let scheduled: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE state = 'scheduled' AND queue = $1",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(available, 1);
    assert_eq!(scheduled, 1);
}
