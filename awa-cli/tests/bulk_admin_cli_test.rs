//! The bulk admin commands run in bounded batches.

use assert_cmd::Command;
use awa_model::{insert_many, InsertOpts, InsertParams, UniqueOpts};
use sqlx::PgPool;
use std::sync::LazyLock;
use tokio::sync::Mutex;
use uuid::Uuid;

static GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn canonical_pool() -> PgPool {
    let pool = awa_testing::setup::setup(4).await;
    awa_testing::setup::reset_to_canonical(&pool).await;
    pool
}

async fn queue_storage_pool() -> PgPool {
    let pool = awa_testing::setup::setup(4).await;
    awa_testing::setup::activate_queue_storage(&pool).await;
    pool
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", &Uuid::new_v4().simple().to_string()[..12])
}

fn run_cli(args: &[&str]) -> Command {
    let mut command = Command::cargo_bin("awa").expect("awa binary should build");
    command
        .env("DATABASE_URL", awa_testing::setup::database_url())
        .args(args);
    command
}

fn output(assert: &assert_cmd::assert::Assert) -> (String, String) {
    let out = assert.get_output();
    (
        String::from_utf8(out.stdout.clone()).expect("stdout"),
        String::from_utf8(out.stderr.clone()).expect("stderr"),
    )
}

async fn insert(
    pool: &PgPool,
    queue: &str,
    kind: &str,
    count: usize,
    unique_key: Option<&str>,
) -> Vec<i64> {
    let params: Vec<InsertParams> = (0..count)
        .map(|seq| InsertParams {
            kind: kind.to_string(),
            args: match unique_key {
                Some(key) => serde_json::json!({ "key": key }),
                None => serde_json::json!({ "seq": seq, "queue": queue }),
            },
            opts: InsertOpts {
                queue: queue.to_string(),
                unique: unique_key.map(|_| UniqueOpts::default()),
                ..Default::default()
            },
        })
        .collect();
    insert_many(pool, &params)
        .await
        .expect("insert jobs")
        .into_iter()
        .map(|job| job.id)
        .collect()
}

async fn fail_canonical(pool: &PgPool, ids: &[i64]) {
    sqlx::query(
        "UPDATE awa.jobs SET state = 'failed', attempt = 1, finalized_at = now() WHERE id = ANY($1)",
    )
    .bind(ids)
    .execute(pool)
    .await
    .expect("fail jobs");
}

async fn count_state(pool: &PgPool, ids: &[i64], state: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE id = ANY($1) AND state::text = $2")
        .bind(ids)
        .bind(state)
        .fetch_one(pool)
        .await
        .expect("count state")
}

#[tokio::test]
async fn retry_failed_runs_in_batches_and_reports_skipped_conflicts() {
    let _guard = GUARD.lock().await;
    let pool = canonical_pool().await;
    let queue = unique_name("cli_retry_failed");
    let key = unique_name("key");
    let mut failed = insert(&pool, &queue, "cli_job", 4, None).await;
    let keyed = insert(&pool, &queue, "cli_unique_job", 1, Some(&key)).await[0];
    failed.push(keyed);
    fail_canonical(&pool, &failed).await;
    insert(&pool, &queue, "cli_unique_job", 1, Some(&key)).await;

    let assert = run_cli(&[
        "job",
        "retry-failed",
        "--queue",
        &queue,
        "--batch-size",
        "2",
    ])
    .assert()
    .code(2);
    let (stdout, stderr) = output(&assert);
    assert!(stdout.contains("Retried 4 failed jobs."), "{stdout}");
    assert!(stderr.contains("(2 so far)"), "{stderr}");
    assert!(
        stderr.contains(&format!(
            "Skipped 1 job(s) whose unique key a live job holds: {keyed}"
        )),
        "{stderr}"
    );
    assert_eq!(count_state(&pool, &failed, "available").await, 4);
    awa_testing::setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn queue_drain_runs_in_batches() {
    let _guard = GUARD.lock().await;
    let pool = canonical_pool().await;
    let queue = unique_name("cli_drain");
    let ids = insert(&pool, &queue, "cli_job", 5, None).await;

    let assert = run_cli(&["queue", "drain", &queue, "--batch-size", "2"])
        .assert()
        .success();
    let (stdout, stderr) = output(&assert);
    assert!(
        stdout.contains(&format!("Drained 5 jobs from queue '{queue}'.")),
        "{stdout}"
    );
    assert!(stderr.contains("(4 so far)"), "{stderr}");
    assert_eq!(count_state(&pool, &ids, "cancelled").await, 5);
    awa_testing::setup::clean_queue(&pool, &queue).await;
}

/// Turn available queue-storage jobs into failed terminal jobs.
async fn fail_queue_storage(pool: &PgPool, ids: &[i64]) {
    sqlx::query(
        r#"
        WITH moved AS (
            DELETE FROM awa.ready_entries
            WHERE job_id = ANY($1)
            RETURNING ready_slot, ready_generation, job_id, kind, queue, args,
                      priority, attempt, run_lease, max_attempts, lane_seq, run_at,
                      attempted_at, created_at, unique_key, unique_states, payload
        )
        INSERT INTO awa.done_entries (
            ready_slot, ready_generation, job_id, kind, queue, args, state,
            priority, attempt, run_lease, max_attempts, lane_seq, run_at,
            attempted_at, finalized_at, created_at, unique_key, unique_states, payload
        )
        SELECT ready_slot, ready_generation, job_id, kind, queue, args,
               'failed'::awa.job_state, priority, GREATEST(attempt, 1), run_lease,
               max_attempts, lane_seq, run_at, COALESCE(attempted_at, now()), now(),
               created_at, unique_key, unique_states, payload
        FROM moved
        "#,
    )
    .bind(ids)
    .execute(pool)
    .await
    .expect("fail queue-storage jobs");
}

async fn dlq_depth(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM awa.dlq_entries WHERE queue = $1")
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("dlq depth")
}

#[tokio::test]
async fn dlq_move_purge_and_discard_run_in_batches() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_name("cli_dlq_bulk");
    let failed = insert(&pool, &queue, "cli_job", 5, None).await;
    fail_queue_storage(&pool, &failed).await;

    let assert = run_cli(&["dlq", "move", "--queue", &queue, "--batch-size", "2"])
        .assert()
        .success();
    let (stdout, stderr) = output(&assert);
    assert!(
        stdout.contains("Moved 5 failed jobs into the DLQ."),
        "{stdout}"
    );
    assert!(
        stderr.contains("(2 so far)") && stderr.contains("(4 so far)"),
        "{stderr}"
    );
    assert_eq!(dlq_depth(&pool, &queue).await, 5);

    let assert = run_cli(&[
        "dlq",
        "purge",
        "--queue",
        &queue,
        "--batch-size",
        "2",
        "--limit",
        "3",
    ])
    .assert()
    .success();
    let (stdout, stderr) = output(&assert);
    assert!(stdout.contains("Purged 3 DLQ rows."), "{stdout}");
    assert!(
        stderr.contains("(2 so far)") && stderr.contains("(3 so far)"),
        "{stderr}"
    );
    assert_eq!(dlq_depth(&pool, &queue).await, 2);

    // Discard covers the kind's failed jobs, then its DLQ rows.
    let kind = unique_name("cli_discard_kind");
    let more = insert(&pool, &queue, &kind, 3, None).await;
    fail_queue_storage(&pool, &more).await;
    sqlx::query("UPDATE awa.dlq_entries SET kind = $2 WHERE queue = $1")
        .bind(&queue)
        .bind(&kind)
        .execute(&pool)
        .await
        .expect("rekind dlq rows");
    let assert = run_cli(&["job", "discard", "--kind", &kind, "--batch-size", "2"])
        .assert()
        .success();
    let (stdout, stderr) = output(&assert);
    assert!(
        stdout.contains(&format!("Discarded 5 failed jobs of kind '{kind}'.")),
        "{stdout}"
    );
    // Three failed jobs in batches of two, then the two DLQ rows.
    assert!(
        stderr.contains("(2 so far)")
            && stderr.contains("(3 so far)")
            && stderr.contains("(5 so far)"),
        "{stderr}"
    );
    assert_eq!(dlq_depth(&pool, &queue).await, 0);
    awa_testing::setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn bulk_commands_reject_a_batch_size_above_the_cap() {
    let assert = run_cli(&["queue", "drain", "any", "--batch-size", "10001"])
        .assert()
        .code(2);
    let (_, stderr) = output(&assert);
    assert!(stderr.contains("--batch-size"), "{stderr}");
}

#[tokio::test]
async fn batch_ops_purge_keeps_limit_and_adds_batch_size() {
    let _guard = GUARD.lock().await;
    let pool = canonical_pool().await;
    let marker = unique_name("cli_purge");
    sqlx::query(
        "INSERT INTO awa.batch_operations (id, op_kind, filter, spec, state, submitted_by, finalized_at) \
         SELECT gen_random_uuid(), 'set_priority', '{}'::jsonb, '{\"priority\": 1}'::jsonb, 'completed', $1, \
                '2000-01-01T00:00:00Z'::timestamptz FROM generate_series(1, 5)",
    )
    .bind(&marker)
    .execute(&pool)
    .await
    .expect("seed batch operations");
    let remaining = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM awa.batch_operations WHERE submitted_by = $1",
        )
        .bind(&marker)
        .fetch_one(&pool)
        .await
        .expect("count")
    };

    run_cli(&[
        "batch-ops",
        "purge",
        "--before",
        "2001-01-01T00:00:00Z",
        "--limit",
        "1",
    ])
    .assert()
    .success();
    assert_eq!(
        remaining().await,
        4,
        "--limit keeps its single-call meaning"
    );

    let assert = run_cli(&[
        "batch-ops",
        "purge",
        "--before",
        "2001-01-01T00:00:00Z",
        "--batch-size",
        "2",
    ])
    .assert()
    .success();
    let (stdout, _) = output(&assert);
    assert!(stdout.contains("Purged"), "{stdout}");
    assert_eq!(remaining().await, 0, "--batch-size runs until none match");
}
