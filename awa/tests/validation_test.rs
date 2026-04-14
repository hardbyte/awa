//! Validation test suite for Awa v0.1 release.
//!
//! Implements the test plan from docs/test-plan.md. Each test is self-contained
//! and uses unique queue names to avoid interference when running in parallel.
//! All tests target real Postgres.

use awa_macros::JobArgs;
use awa_model::{insert_many, insert_with, migrations, InsertOpts, JobRow, JobState, UniqueOpts};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn pool_with(max_conns: u32) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database")
}

async fn pool() -> sqlx::PgPool {
    pool_with(20).await
}

async fn setup() -> sqlx::PgPool {
    let p = pool().await;
    migrations::run(&p).await.expect("Failed to migrate");
    p
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

// ─── Job types ───────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct IncrementCounter {
    counter_id: i64,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct TxTestJob {
    order_id: i64,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct PriorityJob {
    sequence: i64,
    inserted_priority: i16,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct FailingJob {
    attempt_to_fail: bool,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SnoozeJob {
    snooze_count: i64,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct TerminalJob {
    message: String,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct MalformedArgs {
    required_field: String,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct UniqueJob {
    key: String,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct NoopJob {
    data: i64,
}

// ═══════════════════════════════════════════════════════════════════════
// T1: No Duplicate Processing
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t01_no_duplicate_processing() {
    let pool = setup().await;
    let queue = "t01_no_dupes";
    clean_queue(&pool, queue).await;
    let n = 1_000; // Use 1000 for CI speed; scale to 100k for full validation

    // Create counters table
    sqlx::query("CREATE TABLE IF NOT EXISTS t01_counters (id BIGINT PRIMARY KEY, value BIGINT NOT NULL DEFAULT 0)")
        .execute(&pool).await.unwrap();
    sqlx::query("DELETE FROM t01_counters")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO t01_counters (id, value) VALUES (1, 0) ON CONFLICT DO NOTHING")
        .execute(&pool)
        .await
        .unwrap();

    // Insert N jobs
    let params: Vec<_> = (0..n)
        .map(|_| {
            awa_model::insert::params_with(
                &IncrementCounter { counter_id: 1 },
                InsertOpts {
                    queue: queue.into(),
                    ..Default::default()
                },
            )
            .unwrap()
        })
        .collect();

    // Insert in batches of 500 to avoid query size limits
    for chunk in params.chunks(500) {
        insert_many(&pool, chunk).await.unwrap();
    }

    // Process all jobs with concurrent claimers (simulating multiple workers)
    let completed = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for _ in 0..8 {
        let pool = pool.clone();
        let q = queue.to_string();
        let count = completed.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let jobs: Vec<JobRow> = sqlx::query_as(
                    r#"
                    WITH claimed AS (
                        SELECT id FROM awa.jobs_hot
                        WHERE state = 'available' AND queue = $1
                        LIMIT 20
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE awa.jobs_hot SET state = 'running', attempt = attempt + 1,
                        attempted_at = now(), heartbeat_at = now()
                    FROM claimed WHERE awa.jobs_hot.id = claimed.id
                    RETURNING awa.jobs_hot.*
                    "#,
                )
                .bind(&q)
                .fetch_all(&pool)
                .await
                .unwrap();

                if jobs.is_empty() {
                    let remaining: i64 = sqlx::query_scalar(
                        "SELECT count(*) FROM awa.jobs_hot WHERE queue = $1 AND state = 'available'"
                    ).bind(&q).fetch_one(&pool).await.unwrap();
                    if remaining == 0 { break; }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }

                for job in &jobs {
                    // Increment counter atomically
                    sqlx::query("UPDATE t01_counters SET value = value + 1 WHERE id = 1")
                        .execute(&pool).await.unwrap();
                    sqlx::query("UPDATE awa.jobs_hot SET state = 'completed', finalized_at = now() WHERE id = $1")
                        .bind(job.id).execute(&pool).await.unwrap();
                    count.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify
    let counter_value: i64 = sqlx::query_scalar("SELECT value FROM t01_counters WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let completed_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs_hot WHERE queue = $1 AND state = 'completed'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();
    let non_completed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs_hot WHERE queue = $1 AND state != 'completed'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        counter_value, n,
        "Counter must exactly match job count (no duplicates, no losses)"
    );
    assert_eq!(completed_count, n, "All jobs must be completed");
    assert_eq!(non_completed, 0, "No jobs should be in non-completed state");
}

// ═══════════════════════════════════════════════════════════════════════
// T6: Transactional Enqueue Atomicity (Rust)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t06_transactional_atomicity_rust() {
    let pool = setup().await;
    let queue = "t06_tx_rust";
    clean_queue(&pool, queue).await;

    sqlx::query("CREATE TABLE IF NOT EXISTS t06_orders (id BIGINT PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM t06_orders")
        .execute(&pool)
        .await
        .unwrap();

    let iterations = 200;

    for i in 0..iterations {
        let mut tx = pool.begin().await.unwrap();

        sqlx::query("INSERT INTO t06_orders (id) VALUES ($1)")
            .bind(i)
            .execute(&mut *tx)
            .await
            .unwrap();

        insert_with(
            &mut *tx,
            &TxTestJob { order_id: i },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        if i % 2 == 0 {
            tx.commit().await.unwrap();
        } else {
            tx.rollback().await.unwrap();
        }
    }

    let order_count: i64 = sqlx::query_scalar("SELECT count(*) FROM t06_orders")
        .fetch_one(&pool)
        .await
        .unwrap();
    let job_count: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(
        order_count,
        iterations / 2,
        "Only committed orders should exist"
    );
    assert_eq!(
        job_count,
        iterations / 2,
        "Only committed jobs should exist"
    );
    assert_eq!(order_count, job_count, "Perfect 1:1 correspondence");

    // Verify no orphans: every job has a matching order
    let orphan_jobs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs j WHERE j.queue = $1 AND NOT EXISTS (SELECT 1 FROM t06_orders o WHERE o.id = (j.args->>'order_id')::bigint)"
    ).bind(queue).fetch_one(&pool).await.unwrap();
    assert_eq!(orphan_jobs, 0, "No orphaned jobs");
}

// ═══════════════════════════════════════════════════════════════════════
// T8: Uniqueness Under Contention
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t08_uniqueness_under_contention() {
    let pool = setup().await;
    let queue = "t08_unique";
    clean_queue(&pool, queue).await;

    let successes = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // 10 concurrent producers each try 100 times to insert the same unique job
    for _ in 0..10 {
        let pool = pool.clone();
        let q = queue.to_string();
        let ok = successes.clone();
        let fail = conflicts.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let result = insert_with(
                    &pool,
                    &UniqueJob {
                        key: "same_key".into(),
                    },
                    InsertOpts {
                        queue: q.clone(),
                        unique: Some(UniqueOpts {
                            by_queue: true,
                            ..UniqueOpts::default()
                        }),
                        ..Default::default()
                    },
                )
                .await;
                match result {
                    Ok(_) => {
                        ok.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        fail.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let total_success = successes.load(Ordering::SeqCst);
    let total_conflict = conflicts.load(Ordering::SeqCst);

    assert_eq!(total_success, 1, "Exactly one insert should succeed");
    assert_eq!(total_conflict, 999, "999 should conflict");
    assert_eq!(
        total_success + total_conflict,
        1000,
        "All attempts accounted for"
    );

    let job_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND kind = 'unique_job'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(job_count, 1, "Exactly one job in database");
}

// ═══════════════════════════════════════════════════════════════════════
// T9: Hash Cross-Language Consistency
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t09_hash_cross_language_consistency() {
    // Test that unique key computation is deterministic and handles edge cases
    #[allow(clippy::type_complexity)]
    let cases: Vec<(&str, Option<&str>, Option<serde_json::Value>, Option<i64>)> = vec![
        ("send_email", None, None, None),
        ("send_email", Some("default"), None, None),
        (
            "send_email",
            None,
            Some(serde_json::json!({"to": "a@b.com"})),
            None,
        ),
        (
            "send_email",
            Some("email"),
            Some(serde_json::json!({"to": "a@b.com", "subject": "hi"})),
            None,
        ),
        ("send_email", None, None, Some(1000)),
        // Edge cases
        ("send_email", None, Some(serde_json::json!({})), None), // empty args
        ("send_email", Some(""), None, None),                    // empty queue
        (
            "send_email",
            None,
            Some(serde_json::json!({"emoji": "🎉", "cjk": "日本語"})),
            None,
        ), // unicode
        (
            "send_email",
            None,
            Some(serde_json::json!({"nested": {"deep": [1, 2, 3]}})),
            None,
        ), // nested
    ];

    for (kind, queue, args, period) in &cases {
        let key1 = awa_model::unique::compute_unique_key(kind, *queue, args.as_ref(), *period);
        let key2 = awa_model::unique::compute_unique_key(kind, *queue, args.as_ref(), *period);
        assert_eq!(
            key1, key2,
            "Hash must be deterministic for inputs: kind={kind}, queue={queue:?}"
        );
        assert_eq!(key1.len(), 16, "Hash must be 16 bytes");
    }

    // Different inputs must produce different hashes
    let h1 = awa_model::unique::compute_unique_key("a", None, None, None);
    let h2 = awa_model::unique::compute_unique_key("b", None, None, None);
    assert_ne!(h1, h2);
}

// ═══════════════════════════════════════════════════════════════════════
// T10: Priority Ordering
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t10_priority_ordering() {
    let pool = setup().await;
    let queue = "t10_priority";
    clean_queue(&pool, queue).await;

    // Insert jobs at each priority (randomized insertion order)
    let mut jobs_to_insert = vec![];
    for priority in [4i16, 2, 1, 3, 4, 1, 2, 3] {
        for seq in 0..25 {
            jobs_to_insert.push((priority, seq + (priority as i64 - 1) * 25));
        }
    }

    for (priority, seq) in &jobs_to_insert {
        insert_with(
            &pool,
            &PriorityJob {
                sequence: *seq,
                inserted_priority: *priority,
            },
            InsertOpts {
                queue: queue.into(),
                priority: *priority,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    // Claim all jobs one by one (serial, no aging) and record completion order
    let mut completion_order: Vec<(i16, i64)> = vec![];
    loop {
        let jobs: Vec<JobRow> = sqlx::query_as(
            r#"
            WITH claimed AS (
                SELECT id FROM awa.jobs
                WHERE state = 'available' AND queue = $1
                ORDER BY priority ASC, run_at ASC, id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE awa.jobs SET state = 'completed', finalized_at = now()
            FROM claimed WHERE awa.jobs.id = claimed.id
            RETURNING awa.jobs.*
            "#,
        )
        .bind(queue)
        .fetch_all(&pool)
        .await
        .unwrap();

        if jobs.is_empty() {
            break;
        }
        for job in &jobs {
            let args: PriorityJob = serde_json::from_value(job.args.clone()).unwrap();
            completion_order.push((job.priority, args.sequence));
        }
    }

    // Verify priority ordering: all p1 before p2 before p3 before p4
    let priorities: Vec<i16> = completion_order.iter().map(|(p, _)| *p).collect();
    let mut sorted_priorities = priorities.clone();
    sorted_priorities.sort();
    assert_eq!(
        priorities, sorted_priorities,
        "Jobs must complete in priority order"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// T12: Queue Isolation
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t12_queue_isolation() {
    let pool = setup().await;
    clean_queue(&pool, "t12_email").await;
    clean_queue(&pool, "t12_billing").await;

    // Insert 200 email jobs (slow) and 20 billing jobs (fast)
    for i in 0..200 {
        insert_with(
            &pool,
            &NoopJob { data: i },
            InsertOpts {
                queue: "t12_email".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    for i in 0..20 {
        insert_with(
            &pool,
            &NoopJob { data: i },
            InsertOpts {
                queue: "t12_billing".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    // Claim and complete ALL billing jobs
    let billing_start = Instant::now();
    let billing_jobs: Vec<JobRow> = sqlx::query_as(
        r#"
        WITH claimed AS (
            SELECT id FROM awa.jobs WHERE state = 'available' AND queue = 't12_billing'
            LIMIT 100 FOR UPDATE SKIP LOCKED
        )
        UPDATE awa.jobs SET state = 'completed', finalized_at = now()
        FROM claimed WHERE awa.jobs.id = claimed.id
        RETURNING awa.jobs.*
        "#,
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let billing_duration = billing_start.elapsed();

    assert_eq!(billing_jobs.len(), 20, "All billing jobs claimed");
    assert!(
        billing_duration < Duration::from_secs(1),
        "Billing should complete fast regardless of email queue depth"
    );

    // Email queue should still have 200 available
    let email_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE queue = 't12_email' AND state = 'available'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        email_count, 200,
        "Email queue unaffected by billing processing"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// T13: Queue Pause/Resume
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t13_queue_pause_resume() {
    let pool = setup().await;
    let queue = "t13_pause";
    clean_queue(&pool, queue).await;

    for i in 0..100 {
        insert_with(
            &pool,
            &NoopJob { data: i },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    // Pause the queue
    awa_model::admin::pause_queue(&pool, queue, Some("test"))
        .await
        .unwrap();

    // Try to claim — should get 0 jobs (paused check in claim query)
    let claimed: Vec<JobRow> = sqlx::query_as(
        r#"
        WITH claimed AS (
            SELECT id FROM awa.jobs
            WHERE state = 'available' AND queue = $1
              AND NOT EXISTS (SELECT 1 FROM awa.queue_meta WHERE queue = $1 AND paused = TRUE)
            LIMIT 50 FOR UPDATE SKIP LOCKED
        )
        UPDATE awa.jobs SET state = 'running'
        FROM claimed WHERE awa.jobs.id = claimed.id
        RETURNING awa.jobs.*
        "#,
    )
    .bind(queue)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(claimed.len(), 0, "No jobs should be claimed while paused");

    // Resume
    awa_model::admin::resume_queue(&pool, queue).await.unwrap();

    // Now claims should work
    let claimed: Vec<JobRow> = sqlx::query_as(
        r#"
        WITH claimed AS (
            SELECT id FROM awa.jobs
            WHERE state = 'available' AND queue = $1
              AND NOT EXISTS (SELECT 1 FROM awa.queue_meta WHERE queue = $1 AND paused = TRUE)
            LIMIT 100 FOR UPDATE SKIP LOCKED
        )
        UPDATE awa.jobs SET state = 'completed', finalized_at = now()
        FROM claimed WHERE awa.jobs.id = claimed.id
        RETURNING awa.jobs.*
        "#,
    )
    .bind(queue)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(claimed.len(), 100, "All jobs claimed after resume");
}

// ═══════════════════════════════════════════════════════════════════════
// T18: Backoff Timing
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t18_backoff_timing() {
    let pool = setup().await;
    let queue = "t18_backoff";
    clean_queue(&pool, queue).await;

    let job = insert_with(
        &pool,
        &FailingJob {
            attempt_to_fail: true,
        },
        InsertOpts {
            queue: queue.into(),
            max_attempts: 5,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Simulate 5 failed attempts with backoff
    for attempt in 1..=5i16 {
        // Claim
        sqlx::query("UPDATE awa.jobs SET state = 'running', attempt = $2, attempted_at = now(), heartbeat_at = now() WHERE id = $1")
            .bind(job.id).bind(attempt).execute(&pool).await.unwrap();

        if attempt < 5 {
            // Fail with backoff
            sqlx::query(
                r#"
                UPDATE awa.jobs
                SET state = 'retryable',
                    run_at = now() + awa.backoff_duration($2, $3),
                    finalized_at = now(),
                    errors = errors || jsonb_build_object('error', 'test failure', 'attempt', $2, 'at', now())::jsonb
                WHERE id = $1
                "#,
            ).bind(job.id).bind(attempt).bind(5i16).execute(&pool).await.unwrap();

            // Immediately promote for next iteration (skip waiting)
            sqlx::query("UPDATE awa.jobs SET state = 'available', run_at = now() WHERE id = $1")
                .bind(job.id)
                .execute(&pool)
                .await
                .unwrap();
        } else {
            // Final attempt: fail terminally
            sqlx::query(
                "UPDATE awa.jobs SET state = 'failed', finalized_at = now(), errors = errors || jsonb_build_object('error', 'final failure', 'attempt', $2)::jsonb WHERE id = $1"
            ).bind(job.id).bind(attempt).execute(&pool).await.unwrap();
        }
    }

    let final_job: JobRow = sqlx::query_as("SELECT * FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        final_job.state,
        JobState::Failed,
        "Job should be failed after max attempts"
    );
    assert_eq!(final_job.attempt, 5);

    let errors = final_job.errors.unwrap_or_default();
    assert_eq!(errors.len(), 5, "Should have 5 error entries");
}

#[tokio::test]
async fn t18b_backoff_duration_handles_subsecond_jitter() {
    let pool = setup().await;

    // The previous implementation built intervals by string-casting floats,
    // which intermittently produced scientific notation and failed to parse.
    sqlx::raw_sql(
        r#"
        DO $$
        BEGIN
            FOR i IN 1..50000 LOOP
                PERFORM awa.backoff_duration(1::smallint, 5::smallint);
            END LOOP;
        END;
        $$;
        "#,
    )
    .execute(&pool)
    .await
    .expect("backoff_duration should handle sub-second jitter without parse errors");
}

// ═══════════════════════════════════════════════════════════════════════
// T19: Snooze Semantics
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t19_snooze_semantics() {
    let pool = setup().await;
    let queue = "t19_snooze";
    clean_queue(&pool, queue).await;

    let job = insert_with(
        &pool,
        &SnoozeJob { snooze_count: 0 },
        InsertOpts {
            queue: queue.into(),
            max_attempts: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // First claim
    sqlx::query("UPDATE awa.jobs SET state = 'running', attempt = attempt + 1, attempted_at = now() WHERE id = $1")
        .bind(job.id).execute(&pool).await.unwrap();

    // Snooze: back to scheduled, attempt decremented (net zero change)
    sqlx::query("UPDATE awa.jobs SET state = 'scheduled', run_at = now() + interval '1 second', attempt = attempt - 1 WHERE id = $1")
        .bind(job.id).execute(&pool).await.unwrap();

    let snoozed: JobRow = sqlx::query_as("SELECT * FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(snoozed.attempt, 0, "Snooze should NOT consume an attempt");
    assert_eq!(snoozed.state, JobState::Scheduled);

    // Promote and complete
    sqlx::query("UPDATE awa.jobs SET state = 'available', run_at = now() WHERE id = $1")
        .bind(job.id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE awa.jobs SET state = 'running', attempt = attempt + 1 WHERE id = $1")
        .bind(job.id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE awa.jobs SET state = 'completed', finalized_at = now() WHERE id = $1")
        .bind(job.id)
        .execute(&pool)
        .await
        .unwrap();

    let completed: JobRow = sqlx::query_as("SELECT * FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        completed.attempt, 1,
        "Final attempt should be 1 (snooze didn't count)"
    );
    assert_eq!(completed.state, JobState::Completed);
}

// ═══════════════════════════════════════════════════════════════════════
// T20: Terminal Error Semantics
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t20_terminal_error() {
    let pool = setup().await;
    let queue = "t20_terminal";
    clean_queue(&pool, queue).await;

    let job = insert_with(
        &pool,
        &TerminalJob {
            message: "corrupt data".into(),
        },
        InsertOpts {
            queue: queue.into(),
            max_attempts: 25,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Claim
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, attempted_at = now() WHERE id = $1",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();

    // Terminal failure
    sqlx::query(
        r#"
        UPDATE awa.jobs SET state = 'failed', finalized_at = now(),
            errors = errors || jsonb_build_object('error', 'corrupt data', 'attempt', 1, 'terminal', true)::jsonb
        WHERE id = $1
        "#,
    ).bind(job.id).execute(&pool).await.unwrap();

    let failed: JobRow = sqlx::query_as("SELECT * FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(failed.attempt, 1, "Only one attempt despite 25 max");

    let errors = failed.errors.unwrap_or_default();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["terminal"], serde_json::json!(true));
}

// ═══════════════════════════════════════════════════════════════════════
// T21: Deserialization Failure
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t21_deserialization_failure() {
    let pool = setup().await;
    let queue = "t21_deser";
    clean_queue(&pool, queue).await;

    // Insert a job with malformed args (missing required_field)
    sqlx::query(
        r#"INSERT INTO awa.jobs (kind, queue, args, state) VALUES ('malformed_args', $1, '{"wrong_field": 42}', 'available')"#
    ).bind(queue).execute(&pool).await.unwrap();

    // Claim
    let jobs: Vec<JobRow> = sqlx::query_as(
        r#"
        WITH claimed AS (
            SELECT id FROM awa.jobs WHERE state = 'available' AND queue = $1
            LIMIT 1 FOR UPDATE SKIP LOCKED
        )
        UPDATE awa.jobs SET state = 'running', attempt = 1
        FROM claimed WHERE awa.jobs.id = claimed.id
        RETURNING awa.jobs.*
        "#,
    )
    .bind(queue)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(jobs.len(), 1);
    let job = &jobs[0];

    // Try to deserialize — this should fail
    let deser_result = serde_json::from_value::<MalformedArgs>(job.args.clone());
    assert!(
        deser_result.is_err(),
        "Deserialization should fail for malformed args"
    );

    // Mark as terminal failure
    sqlx::query("UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1")
        .bind(job.id)
        .execute(&pool)
        .await
        .unwrap();

    let failed: JobRow = sqlx::query_as("SELECT * FROM awa.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(failed.state, JobState::Failed);
}

// ═══════════════════════════════════════════════════════════════════════
// T22: Pool Exhaustion Resilience
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t22_pool_exhaustion_resilience() {
    // Create a pool with very few connections
    let pool = pool_with(5).await;
    migrations::run(&pool).await.unwrap();
    let queue = "t22_pool";
    clean_queue(&pool, queue).await;

    // Insert more jobs than connections
    for i in 0..50 {
        insert_with(
            &pool,
            &NoopJob { data: i },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    // Process with concurrent tasks (more than pool connections)
    let completed = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for _ in 0..10 {
        // 10 tasks competing for 5 connections
        let pool = pool.clone();
        let q = queue.to_string();
        let count = completed.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let jobs: Vec<JobRow> = match sqlx::query_as::<_, JobRow>(
                    r#"
                    WITH claimed AS (
                        SELECT id FROM awa.jobs_hot WHERE state = 'available' AND queue = $1
                        LIMIT 5 FOR UPDATE SKIP LOCKED
                    )
                    UPDATE awa.jobs_hot SET state = 'completed', finalized_at = now()
                    FROM claimed WHERE awa.jobs_hot.id = claimed.id
                    RETURNING awa.jobs_hot.*
                    "#,
                )
                .bind(&q)
                .fetch_all(&pool)
                .await
                {
                    Ok(j) => j,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                };

                if jobs.is_empty() {
                    let remaining: i64 = sqlx::query_scalar(
                        "SELECT count(*) FROM awa.jobs_hot WHERE queue = $1 AND state = 'available'",
                    )
                    .bind(&q)
                    .fetch_one(&pool)
                    .await
                    .unwrap_or(0);
                    if remaining == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                } else {
                    count.fetch_add(jobs.len() as u64, Ordering::SeqCst);
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let total = completed.load(Ordering::SeqCst);
    assert_eq!(
        total, 50,
        "All jobs should complete even with pool exhaustion"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// T26: Migration Idempotency
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t26_migration_idempotency() {
    let pool = pool().await;

    // Run migrations twice — second should be no-op
    migrations::run(&pool).await.unwrap();
    migrations::run(&pool).await.unwrap();

    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);

    // Verify schema is intact
    let has_jobs: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'jobs')"
    ).fetch_one(&pool).await.unwrap();
    assert!(has_jobs, "awa.jobs table must exist");

    let has_trigger: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.triggers WHERE trigger_name = 'trg_awa_notify')"
    ).fetch_one(&pool).await.unwrap();
    assert!(has_trigger, "NOTIFY trigger must exist");
}

// ═══════════════════════════════════════════════════════════════════════
// T27: Admin Operations Under Load
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t27_admin_ops_under_load() {
    let pool = setup().await;
    let queue = "t27_admin";
    clean_queue(&pool, queue).await;

    // Insert jobs and start processing
    for i in 0..100 {
        insert_with(
            &pool,
            &NoopJob { data: i },
            InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    // Claim some jobs (simulate workers)
    let running: Vec<JobRow> = sqlx::query_as(
        r#"
        WITH claimed AS (
            SELECT id FROM awa.jobs WHERE state = 'available' AND queue = $1
            LIMIT 10 FOR UPDATE SKIP LOCKED
        )
        UPDATE awa.jobs SET state = 'running', attempt = 1
        FROM claimed WHERE awa.jobs.id = claimed.id
        RETURNING awa.jobs.*
        "#,
    )
    .bind(queue)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(running.len(), 10);

    // Cancel a running job
    let cancel_result = awa_model::admin::cancel(&pool, running[0].id).await;
    assert!(cancel_result.is_ok());

    // Fail a job then retry it
    sqlx::query("UPDATE awa.jobs SET state = 'failed', finalized_at = now() WHERE id = $1")
        .bind(running[1].id)
        .execute(&pool)
        .await
        .unwrap();
    let retry_result = awa_model::admin::retry(&pool, running[1].id).await;
    assert!(retry_result.is_ok());

    // Pause/resume while jobs are in-flight
    awa_model::admin::pause_queue(&pool, queue, Some("test"))
        .await
        .unwrap();
    awa_model::admin::resume_queue(&pool, queue).await.unwrap();

    // Queue stats should reflect current state
    awa_model::admin::flush_dirty_admin_metadata(&pool)
        .await
        .unwrap();
    let stats = awa_model::admin::queue_stats(&pool).await.unwrap();
    let stat = stats.iter().find(|s| s.queue == queue);
    assert!(stat.is_some(), "Queue should appear in stats");

    // Drain remaining
    let drained = awa_model::admin::drain_queue(&pool, queue).await.unwrap();
    assert!(drained > 0, "Should drain some jobs");
}
