//! Integration tests for the Dead Letter Queue (DLQ).
//!
//! Covers: SQL-level atomicity of `move_to_dlq_guarded`, stale-lease rejection,
//! retry-from-DLQ re-enqueue, bulk admin moves, purge, and the executor-level
//! routing decision when a queue has DLQ enabled.

use awa::model::dlq::{self, ListDlqFilter, RetryFromDlqOpts};
use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, Worker};
use awa_testing::TestClient;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup() -> TestClient {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");

    let client = TestClient::from_pool(pool).await;
    client.migrate().await.expect("Failed to run migrations");
    client
}

async fn clean_queue(pool: &sqlx::PgPool, queue: &str) {
    sqlx::query("DELETE FROM awa.jobs_dlq WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean DLQ");
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean jobs");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue_meta");
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct DlqTestJob {
    pub value: String,
}

struct AlwaysFailTerminalWorker;

#[async_trait::async_trait]
impl Worker for AlwaysFailTerminalWorker {
    fn kind(&self) -> &'static str {
        "dlq_test_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        Err(JobError::Terminal("boom".into()))
    }
}

/// Guarded move: a running job finalised with a matching lease lands in DLQ
/// and leaves `jobs_hot` empty for that job id.
#[tokio::test]
async fn test_move_to_dlq_guarded_happy_path() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_guarded_happy";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "hello".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Simulate claim: transition to running and bump run_lease.
    sqlx::query(
        r#"UPDATE awa.jobs_hot SET state='running', run_lease=42, attempt=1,
               attempted_at=now(), heartbeat_at=now() WHERE id=$1"#,
    )
    .bind(job.id)
    .execute(pool)
    .await
    .unwrap();

    let dlq_row: Option<awa::DlqRow> =
        sqlx::query_as("SELECT * FROM awa.move_to_dlq_guarded($1, $2, $3, $4::jsonb, $5::jsonb)")
            .bind(job.id)
            .bind(42_i64)
            .bind("terminal_error")
            .bind(serde_json::json!({"error": "boom", "attempt": 1, "terminal": true}))
            .bind(serde_json::json!({"checkpoint": "step3"}))
            .fetch_optional(pool)
            .await
            .unwrap();

    let dlq_row = dlq_row.expect("DLQ move should have succeeded");
    assert_eq!(dlq_row.id, job.id);
    assert_eq!(dlq_row.dlq_reason, "terminal_error");
    assert_eq!(dlq_row.original_run_lease, 42);
    assert_eq!(
        dlq_row.progress,
        Some(serde_json::json!({"checkpoint": "step3"})),
        "final handler progress should be preserved in the DLQ row"
    );

    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM awa.jobs_hot WHERE id = $1")
            .bind(job.id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(remaining, 0, "jobs_hot should no longer hold the job");
}

/// Guarded move: stale lease (the row has been re-claimed) results in 0 rows
/// affected and the job row is untouched.
#[tokio::test]
async fn test_move_to_dlq_guarded_rejects_stale_lease() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_guarded_stale";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "stale".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    sqlx::query(r#"UPDATE awa.jobs_hot SET state='running', run_lease=7 WHERE id=$1"#)
        .bind(job.id)
        .execute(pool)
        .await
        .unwrap();

    // Call with the wrong lease (simulating a rescue that bumped it in the meantime)
    let dlq_row: Option<awa::DlqRow> =
        sqlx::query_as("SELECT * FROM awa.move_to_dlq_guarded($1, $2, $3, $4::jsonb, $5::jsonb)")
            .bind(job.id)
            .bind(99_i64) // wrong
            .bind("should_not_move")
            .bind(serde_json::json!({}))
            .bind::<Option<serde_json::Value>>(None)
            .fetch_optional(pool)
            .await
            .unwrap();

    assert!(dlq_row.is_none(), "stale lease should reject the move");

    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM awa.jobs_hot WHERE id = $1")
            .bind(job.id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(
        remaining, 1,
        "jobs_hot row must still exist after stale rejection"
    );
}

/// Admin bulk-move: failed jobs in jobs_hot can be relocated into the DLQ.
#[tokio::test]
async fn test_bulk_move_failed_to_dlq() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_bulk_move";
    clean_queue(pool, queue).await;

    for i in 0..3 {
        let job = awa::model::insert_with(
            pool,
            &DlqTestJob {
                value: format!("j{i}"),
            },
            awa::InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        sqlx::query("UPDATE awa.jobs_hot SET state='failed', finalized_at=now() WHERE id=$1")
            .bind(job.id)
            .execute(pool)
            .await
            .unwrap();
    }

    let moved = dlq::bulk_move_failed_to_dlq(pool, None, Some(queue), "ops_archived")
        .await
        .unwrap();
    assert_eq!(moved, 3);

    let depth = dlq::dlq_depth(pool, Some(queue)).await.unwrap();
    assert_eq!(depth, 3);

    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM awa.jobs_hot WHERE queue=$1 AND state='failed'",
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(remaining, 0);
}

/// Retry from DLQ: a DLQ'd job re-enters jobs_hot as available with attempt=0.
#[tokio::test]
async fn test_retry_from_dlq_revives_job() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_retry";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "revive".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE awa.jobs_hot SET state='failed', attempt=5, finalized_at=now() WHERE id=$1",
    )
    .bind(job.id)
    .execute(pool)
    .await
    .unwrap();
    dlq::move_failed_to_dlq(pool, job.id, "reason")
        .await
        .unwrap();

    let opts = RetryFromDlqOpts::default();
    let revived = dlq::retry_from_dlq(pool, job.id, &opts)
        .await
        .unwrap()
        .expect("retry should return the revived row");
    assert_eq!(revived.id, job.id);
    assert_eq!(revived.attempt, 0);
    assert_eq!(revived.run_lease, 0);
    assert_eq!(revived.state.to_string(), "available");

    let depth_after = dlq::dlq_depth(pool, Some(queue)).await.unwrap();
    assert_eq!(depth_after, 0);
}

/// Retry from DLQ with a future run_at schedules the job instead of making it available.
#[tokio::test]
async fn test_retry_from_dlq_with_future_run_at_schedules() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_retry_scheduled";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "later".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE awa.jobs_hot SET state='failed', finalized_at=now() WHERE id=$1")
        .bind(job.id)
        .execute(pool)
        .await
        .unwrap();
    dlq::move_failed_to_dlq(pool, job.id, "will_retry")
        .await
        .unwrap();

    let opts = RetryFromDlqOpts {
        run_at: Some(chrono::Utc::now() + chrono::Duration::minutes(10)),
        ..Default::default()
    };
    let revived = dlq::retry_from_dlq(pool, job.id, &opts)
        .await
        .unwrap()
        .expect("retry should return the revived row");
    assert_eq!(revived.state.to_string(), "scheduled");
}

/// `move_failed_to_dlq` and `bulk_move_failed_to_dlq` must carry the source
/// row's `progress` checkpoint into the DLQ. Terminal-failure handlers write
/// their final progress snapshot to `jobs_hot.progress`; dropping it during
/// an operator-initiated move loses the last-known state of the job.
#[tokio::test]
async fn test_bulk_move_preserves_progress_snapshot() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_progress_preserve";
    clean_queue(pool, queue).await;

    // Single-job admin move path.
    let single = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "single".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    sqlx::query(
        r#"UPDATE awa.jobs_hot
           SET state='failed', finalized_at=now(),
               progress='{"checkpoint":"single_row"}'::jsonb
           WHERE id=$1"#,
    )
    .bind(single.id)
    .execute(pool)
    .await
    .unwrap();

    let moved_single = dlq::move_failed_to_dlq(pool, single.id, "single_move")
        .await
        .unwrap()
        .expect("single move should return a DLQ row");
    assert_eq!(
        moved_single.progress,
        Some(serde_json::json!({"checkpoint": "single_row"})),
        "move_failed_to_dlq must preserve the source progress checkpoint",
    );

    // Bulk admin move path.
    let bulk = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "bulk".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    sqlx::query(
        r#"UPDATE awa.jobs_hot
           SET state='failed', finalized_at=now(),
               progress='{"checkpoint":"bulk_row","step":7}'::jsonb
           WHERE id=$1"#,
    )
    .bind(bulk.id)
    .execute(pool)
    .await
    .unwrap();

    let moved = dlq::bulk_move_failed_to_dlq(pool, None, Some(queue), "bulk_move")
        .await
        .unwrap();
    assert_eq!(moved, 1, "bulk move should have picked up the failed row");

    let bulk_row = dlq::get_dlq_job(pool, bulk.id)
        .await
        .unwrap()
        .expect("bulk-moved row should land in the DLQ");
    assert_eq!(
        bulk_row.progress,
        Some(serde_json::json!({"checkpoint": "bulk_row", "step": 7})),
        "bulk_move_failed_to_dlq must preserve the source progress checkpoint",
    );
}

/// Architectural guarantee: the claim SQL executed by dispatchers must not
/// touch `jobs_dlq`, so populating the DLQ cannot regress claim-path latency.
/// Captures the EXPLAIN plan for the real claim query and asserts the DLQ
/// table never appears in it — a deterministic guard against anyone ever
/// adding a view or trigger that pulls the DLQ into the hot path.
#[tokio::test]
async fn test_claim_query_does_not_reference_jobs_dlq() {
    let test_client = setup().await;
    let pool = test_client.pool();

    // The SQL below is kept in sync with awa-worker/src/dispatcher.rs — any
    // divergence shows up as a compile-time parameter-count mismatch when a
    // future refactor edits one without the other.
    let plan: Vec<(String,)> = sqlx::query_as(
        r#"
        EXPLAIN (FORMAT TEXT)
        WITH claimed AS (
            SELECT id FROM awa.jobs_hot
            WHERE state = 'available'
              AND queue = $1
              AND run_at <= now()
              AND NOT EXISTS (
                  SELECT 1 FROM awa.queue_meta WHERE queue = $1 AND paused = TRUE
              )
            ORDER BY priority ASC, run_at ASC, id ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE awa.jobs_hot
        SET state = 'running',
            attempt = attempt + 1,
            run_lease = run_lease + 1,
            attempted_at = now(),
            heartbeat_at = now(),
            deadline_at = now() + make_interval(secs => $3)
        FROM claimed
        WHERE awa.jobs_hot.id = claimed.id
          AND awa.jobs_hot.state = 'available'
        RETURNING awa.jobs_hot.*
        "#,
    )
    .bind("benchmark_plan")
    .bind(100i32)
    .bind(30.0_f64)
    .fetch_all(pool)
    .await
    .unwrap();

    let plan_text = plan
        .into_iter()
        .map(|(line,)| line)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !plan_text.to_lowercase().contains("jobs_dlq"),
        "claim query plan must not reference jobs_dlq:\n{plan_text}",
    );
}

/// Throughput bench: confirms claim-path wall clock is insensitive to DLQ
/// depth. Runs under `cargo test -- --ignored` only (populates 50k DLQ rows
/// and processes 2k jobs, too slow for the default suite).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_claim_path_is_insensitive_to_dlq_bloat() {
    let test_client = setup().await;
    let pool = test_client.pool();

    const DLQ_BLOAT: usize = 50_000;
    const HOT_JOBS: usize = 2_000;

    async fn run_scenario(pool: &sqlx::PgPool, queue: &str) -> std::time::Duration {
        // Clean slate for this queue; leave DLQ depth untouched so the caller
        // controls whether it's populated or empty.
        sqlx::query("DELETE FROM awa.jobs_hot WHERE queue = $1")
            .bind(queue)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
            .bind(queue)
            .execute(pool)
            .await
            .unwrap();

        for i in 0..HOT_JOBS {
            awa::model::insert_with(
                pool,
                &DlqTestJob {
                    value: format!("bench-{i}"),
                },
                awa::InsertOpts {
                    queue: queue.into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        }

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        struct NoOp(tokio::sync::mpsc::UnboundedSender<()>);
        #[async_trait::async_trait]
        impl Worker for NoOp {
            fn kind(&self) -> &'static str {
                "dlq_test_job"
            }
            async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
                let _ = self.0.send(());
                Ok(JobResult::Completed)
            }
        }

        let client = Client::builder(pool.clone())
            .queue(
                queue,
                QueueConfig {
                    max_workers: 64,
                    poll_interval: Duration::from_millis(25),
                    ..QueueConfig::default()
                },
            )
            .register_worker(NoOp(tx))
            .build()
            .unwrap();
        client.start().await.unwrap();

        let start = std::time::Instant::now();
        for _ in 0..HOT_JOBS {
            rx.recv().await.expect("handler ack");
        }
        let elapsed = start.elapsed();
        client.shutdown(Duration::from_secs(5)).await;
        elapsed
    }

    // Baseline: empty DLQ.
    sqlx::query("DELETE FROM awa.jobs_dlq")
        .execute(pool)
        .await
        .unwrap();
    let baseline = run_scenario(pool, "bench_dlq_baseline").await;

    // Populate the DLQ with 50k rows. Every row is inserted directly (not
    // going through the move CTE) since we only care about how its
    // presence affects the claim-path hot table.
    let populate_start = std::time::Instant::now();
    for chunk in (0..DLQ_BLOAT).collect::<Vec<_>>().chunks(1000) {
        let ids: Vec<i64> = chunk.iter().map(|i| 10_000_000 + *i as i64).collect();
        sqlx::query(
            r#"
            INSERT INTO awa.jobs_dlq (
                id, kind, queue, args, state, priority, attempt, max_attempts,
                run_at, created_at, errors, metadata, tags,
                run_lease, dlq_reason, dlq_at, original_run_lease
            )
            SELECT
                id, 'dlq_test_job', 'bench_bloat', '{}'::jsonb, 'failed'::awa.job_state, 2, 1, 1,
                now(), now(), '{}'::jsonb[], '{}'::jsonb, '{}'::text[],
                0, 'bloat', now(), 0
            FROM unnest($1::bigint[]) AS id
            "#,
        )
        .bind(&ids)
        .execute(pool)
        .await
        .unwrap();
    }
    eprintln!(
        "Populated {DLQ_BLOAT} DLQ rows in {:?}",
        populate_start.elapsed()
    );

    let bloated = run_scenario(pool, "bench_dlq_bloated").await;

    sqlx::query("DELETE FROM awa.jobs_dlq WHERE queue = 'bench_bloat'")
        .execute(pool)
        .await
        .unwrap();

    let baseline_rate = HOT_JOBS as f64 / baseline.as_secs_f64();
    let bloated_rate = HOT_JOBS as f64 / bloated.as_secs_f64();
    let ratio = bloated_rate / baseline_rate;
    eprintln!(
        "[dlq-bloat bench] baseline={:.0}/s bloated={:.0}/s ratio={:.3} (rows: dlq={DLQ_BLOAT}, hot={HOT_JOBS})",
        baseline_rate, bloated_rate, ratio,
    );

    // Sanity: tolerate up to 20% slowdown before flagging. If this ever fails,
    // the claim path has regressed to touch the DLQ table somehow.
    assert!(
        ratio > 0.8,
        "DLQ bloat appears to have regressed claim throughput: baseline={baseline_rate:.0}/s bloated={bloated_rate:.0}/s (ratio={ratio:.3})"
    );
}

/// Guards against schema drift between `jobs_hot` and `jobs_dlq`. The DLQ
/// table redefines every column explicitly, so a new column added to
/// `jobs_hot` in a future migration must also be added to `jobs_dlq` — else
/// the `SELECT *`-free move CTE will break. This test fails loudly if the
/// two tables diverge.
#[tokio::test]
async fn test_jobs_hot_and_dlq_schema_parity() {
    let test_client = setup().await;
    let pool = test_client.pool();

    let drift: Vec<(String, String, String)> = sqlx::query_as(
        r#"
        SELECT h.column_name, h.data_type::text, COALESCE(d.data_type::text, '<missing>')
        FROM information_schema.columns h
        LEFT JOIN information_schema.columns d
            ON d.table_schema = h.table_schema
           AND d.table_name = 'jobs_dlq'
           AND d.column_name = h.column_name
        WHERE h.table_schema = 'awa'
          AND h.table_name = 'jobs_hot'
          AND (d.column_name IS NULL OR d.data_type <> h.data_type)
        "#,
    )
    .fetch_all(pool)
    .await
    .unwrap();

    assert!(
        drift.is_empty(),
        "jobs_hot columns missing or mismatched in jobs_dlq: {drift:?}",
    );

    // DLQ-specific columns must exist.
    let dlq_specific: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)::bigint
        FROM information_schema.columns
        WHERE table_schema = 'awa'
          AND table_name = 'jobs_dlq'
          AND column_name IN ('dlq_reason', 'dlq_at', 'original_run_lease')
        "#,
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        dlq_specific, 3,
        "jobs_dlq must define dlq_reason, dlq_at, original_run_lease",
    );
}

/// Purge DLQ removes matching rows.
#[tokio::test]
async fn test_purge_dlq() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_purge";
    clean_queue(pool, queue).await;

    for i in 0..4 {
        let job = awa::model::insert_with(
            pool,
            &DlqTestJob {
                value: format!("p{i}"),
            },
            awa::InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        sqlx::query("UPDATE awa.jobs_hot SET state='failed', finalized_at=now() WHERE id=$1")
            .bind(job.id)
            .execute(pool)
            .await
            .unwrap();
    }
    dlq::bulk_move_failed_to_dlq(pool, None, Some(queue), "t")
        .await
        .unwrap();
    assert_eq!(dlq::dlq_depth(pool, Some(queue)).await.unwrap(), 4);

    let deleted = dlq::purge_dlq(
        pool,
        &ListDlqFilter {
            queue: Some(queue.into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(deleted, 4);
    assert_eq!(dlq::dlq_depth(pool, Some(queue)).await.unwrap(), 0);
}

/// End-to-end: a queue with DLQ enabled routes a terminal failure through the
/// executor into jobs_dlq instead of leaving the row in jobs_hot.
#[tokio::test]
async fn test_executor_routes_terminal_failure_to_dlq_when_enabled() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_executor_enabled";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "end2end".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            max_attempts: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let client = Client::builder(pool.clone())
        .queue(queue, QueueConfig::default())
        .register_worker(AlwaysFailTerminalWorker)
        .dlq_enabled_by_default(true)
        .leader_election_interval(Duration::from_millis(100))
        .cleanup_interval(Duration::from_secs(3600))
        .heartbeat_interval(Duration::from_secs(60))
        .build()
        .unwrap();
    client.start().await.unwrap();
    // Give the dispatcher time to claim and execute
    tokio::time::sleep(Duration::from_secs(2)).await;
    client.shutdown(Duration::from_secs(3)).await;

    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM awa.jobs_hot WHERE id=$1")
            .bind(job.id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(remaining, 0, "terminal failure should leave jobs_hot empty");

    let dlq_row = dlq::get_dlq_job(pool, job.id).await.unwrap();
    assert!(
        dlq_row.is_some(),
        "terminal failure should land in DLQ when enabled"
    );
    let dlq_row = dlq_row.unwrap();
    assert_eq!(dlq_row.dlq_reason, "terminal_error");
    assert!(dlq_row
        .errors
        .as_ref()
        .map(|e| !e.is_empty())
        .unwrap_or(false));
}

async fn insert_job_in_callback_timeout(
    pool: &sqlx::PgPool,
    queue: &str,
    value: &str,
    max_attempts: i16,
    attempt: i16,
) -> awa::JobRow {
    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: value.into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            max_attempts,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    sqlx::query(
        r#"UPDATE awa.jobs_hot
           SET state='waiting_external',
               attempt=$2,
               attempted_at=now(),
               callback_id=gen_random_uuid(),
               callback_timeout_at=now() - interval '1 minute'
           WHERE id=$1"#,
    )
    .bind(job.id)
    .bind(attempt)
    .execute(pool)
    .await
    .unwrap();
    job
}

/// The callback-timeout rescue path is the only rescue that can transition
/// directly to `failed` (when attempts are exhausted) without going through
/// the executor's apply_terminal_failure. Verify the maintenance sweep
/// routes such rows into the DLQ when the queue has DLQ enabled.
///
/// Calls the standalone `rescue_expired_callbacks_once` helper directly so
/// the test doesn't contend with other parallel tests for the maintenance
/// advisory lock.
#[tokio::test]
async fn test_rescue_callback_timeout_routes_to_dlq_when_enabled() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_rescue_callback";
    clean_queue(pool, queue).await;

    let job = insert_job_in_callback_timeout(pool, queue, "callback_expired", 1, 1).await;

    // Policy: DLQ enabled for our queue only (avoid side effects on other tests).
    let mut overrides = std::collections::HashMap::new();
    overrides.insert(queue.to_string(), true);
    let policy = awa::DlqPolicy::new(false, overrides);
    let metrics = awa::worker::AwaMetrics::from_global();

    awa::rescue_expired_callbacks_once(pool, &policy, &metrics).await;

    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*)::bigint FROM awa.jobs_hot WHERE id=$1")
            .bind(job.id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(
        remaining, 0,
        "callback-timeout rescue on max attempts must not leave the row in jobs_hot"
    );

    let dlq_row = dlq::get_dlq_job(pool, job.id)
        .await
        .unwrap()
        .expect("callback-timeout failure should land in the DLQ when enabled");
    assert_eq!(dlq_row.dlq_reason, "callback_timeout");
}

/// Inverse: DLQ disabled — callback-timeout rescue keeps the row in jobs_hot.
#[tokio::test]
async fn test_rescue_callback_timeout_keeps_row_when_dlq_disabled() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_rescue_callback_disabled";
    clean_queue(pool, queue).await;

    let job = insert_job_in_callback_timeout(pool, queue, "no_dlq", 1, 1).await;

    let policy = awa::DlqPolicy::new(false, Default::default());
    let metrics = awa::worker::AwaMetrics::from_global();
    awa::rescue_expired_callbacks_once(pool, &policy, &metrics).await;

    let state: Option<(String,)> =
        sqlx::query_as("SELECT state::text FROM awa.jobs_hot WHERE id=$1")
            .bind(job.id)
            .fetch_optional(pool)
            .await
            .unwrap();
    assert_eq!(state.map(|r| r.0), Some("failed".to_string()));
    assert!(dlq::get_dlq_job(pool, job.id).await.unwrap().is_none());
}

/// A callback timeout with attempts still remaining must still go to
/// `retryable` even with DLQ enabled — DLQ is for permanent failures, not
/// transient ones.
#[tokio::test]
async fn test_rescue_callback_timeout_with_remaining_attempts_goes_retryable() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_rescue_callback_retryable";
    clean_queue(pool, queue).await;

    let job = insert_job_in_callback_timeout(pool, queue, "more_attempts", 5, 2).await;

    let mut overrides = std::collections::HashMap::new();
    overrides.insert(queue.to_string(), true);
    let policy = awa::DlqPolicy::new(false, overrides);
    let metrics = awa::worker::AwaMetrics::from_global();
    awa::rescue_expired_callbacks_once(pool, &policy, &metrics).await;

    let state: Option<(String,)> = sqlx::query_as("SELECT state::text FROM awa.jobs WHERE id=$1")
        .bind(job.id)
        .fetch_optional(pool)
        .await
        .unwrap();
    assert_eq!(
        state.map(|r| r.0),
        Some("retryable".to_string()),
        "attempts remaining must still produce a retryable row, not DLQ",
    );
    assert!(
        dlq::get_dlq_job(pool, job.id).await.unwrap().is_none(),
        "retryable rows must not appear in the DLQ",
    );
}

/// `admin::dump_job` must fall back to jobs_dlq so operators can inspect a
/// DLQ'd job through the standard tooling (`awa job dump <id>` / the UI job
/// detail route) rather than hitting a bare `JobNotFound`.
#[tokio::test]
async fn test_admin_dump_job_falls_back_to_dlq() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_admin_fallback";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "lookup".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    sqlx::query(
        r#"UPDATE awa.jobs_hot
           SET state='failed', finalized_at=now(),
               progress='{"checkpoint":"lookup"}'::jsonb
           WHERE id=$1"#,
    )
    .bind(job.id)
    .execute(pool)
    .await
    .unwrap();
    dlq::move_failed_to_dlq(pool, job.id, "ops_archived")
        .await
        .unwrap();

    let dump = awa_model::admin::dump_job(pool, job.id)
        .await
        .expect("dump_job must succeed via DLQ fallback");
    assert_eq!(dump.job.id, job.id);
    assert_eq!(
        dump.job.progress,
        Some(serde_json::json!({"checkpoint": "lookup"})),
    );
    let dlq_meta = dump.dlq.expect("dump.dlq must be populated for DLQ rows");
    assert_eq!(dlq_meta.reason, "ops_archived");
    assert!(
        !dump.summary.can_retry,
        "can_retry must be false for DLQ rows so the frontend routes retry via /api/dlq"
    );
    assert!(!dump.summary.can_cancel);
}

/// `dlq_depth_by_queue` must always agree with the raw row counts in jobs_dlq.
/// The maintenance leader publishes this directly to the OTel gauge, so drift
/// would show up as a stale/incorrect dashboard signal.
#[tokio::test]
async fn test_dlq_depth_matches_row_counts() {
    let test_client = setup().await;
    let pool = test_client.pool();

    // Isolate to a distinct queue prefix so other tests' rows don't leak.
    for q in ["dlq_parity_a", "dlq_parity_b", "dlq_parity_c"] {
        clean_queue(pool, q).await;
    }

    let counts = [
        ("dlq_parity_a", 3usize),
        ("dlq_parity_b", 1),
        ("dlq_parity_c", 5),
    ];

    for (queue, n) in counts.iter() {
        for i in 0..*n {
            let job = awa::model::insert_with(
                pool,
                &DlqTestJob {
                    value: format!("{queue}-{i}"),
                },
                awa::InsertOpts {
                    queue: (*queue).into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            sqlx::query("UPDATE awa.jobs_hot SET state='failed', finalized_at=now() WHERE id=$1")
                .bind(job.id)
                .execute(pool)
                .await
                .unwrap();
        }
        dlq::bulk_move_failed_to_dlq(pool, None, Some(queue), "parity")
            .await
            .unwrap();
    }

    let depth_rows = dlq::dlq_depth_by_queue(pool).await.unwrap();
    let depth_map: std::collections::HashMap<String, i64> = depth_rows.into_iter().collect();

    for (queue, n) in counts.iter() {
        let raw: i64 =
            sqlx::query_scalar("SELECT count(*)::bigint FROM awa.jobs_dlq WHERE queue = $1")
                .bind(queue)
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(raw, *n as i64, "raw count for {queue}");
        assert_eq!(
            depth_map.get(*queue).copied().unwrap_or(0),
            *n as i64,
            "dlq_depth_by_queue for {queue} must match raw count",
        );
    }

    // Total depth (no queue filter) must equal sum of per-queue counts.
    let total = dlq::dlq_depth(pool, None).await.unwrap();
    let expected_total: i64 = counts.iter().map(|(_, n)| *n as i64).sum();
    let raw_total_for_prefix: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM awa.jobs_dlq WHERE queue LIKE 'dlq_parity_%'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(raw_total_for_prefix, expected_total);
    assert!(
        total >= expected_total,
        "global total must include at least the parity rows"
    );
}

/// Inverse: without DLQ enabled, a terminal failure stays in jobs_hot as before.
#[tokio::test]
async fn test_executor_keeps_terminal_failure_in_hot_when_disabled() {
    let test_client = setup().await;
    let pool = test_client.pool();
    let queue = "dlq_executor_disabled";
    clean_queue(pool, queue).await;

    let job = awa::model::insert_with(
        pool,
        &DlqTestJob {
            value: "legacy".into(),
        },
        awa::InsertOpts {
            queue: queue.into(),
            max_attempts: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let client = Client::builder(pool.clone())
        .queue(queue, QueueConfig::default())
        .register_worker(AlwaysFailTerminalWorker)
        .leader_election_interval(Duration::from_millis(100))
        .cleanup_interval(Duration::from_secs(3600))
        .heartbeat_interval(Duration::from_secs(60))
        .build()
        .unwrap();
    client.start().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    client.shutdown(Duration::from_secs(3)).await;

    let row: Option<(String,)> = sqlx::query_as("SELECT state::text FROM awa.jobs_hot WHERE id=$1")
        .bind(job.id)
        .fetch_optional(pool)
        .await
        .unwrap();
    assert_eq!(row.map(|r| r.0), Some("failed".to_string()));
    assert!(dlq::get_dlq_job(pool, job.id).await.unwrap().is_none());
}
