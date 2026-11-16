//! `awa dlq retry-bulk --batch-size` against queue storage.

use assert_cmd::Command;
use sqlx::PgPool;
use std::sync::LazyLock;
use tokio::sync::Mutex;
use uuid::Uuid;

static GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn queue_storage_pool() -> PgPool {
    let pool = awa_testing::setup::setup(4).await;
    awa_testing::setup::activate_queue_storage(&pool).await;
    pool
}

fn unique_queue(prefix: &str) -> String {
    format!("{prefix}_{}", &Uuid::new_v4().simple().to_string()[..12])
}

fn run_cli(args: &[&str]) -> Command {
    let mut command = Command::cargo_bin("awa").expect("awa binary should build");
    command
        .env("DATABASE_URL", awa_testing::setup::database_url())
        .args(args);
    command
}

async fn seed_dlq(pool: &PgPool, queue: &str, rows: i64) -> Vec<i64> {
    sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries (job_id, kind, queue, priority, dlq_reason, original_run_lease) \
         SELECT nextval('awa.job_id_seq'), 'cli_dlq_job', $1, 2, 'seeded', 1 \
         FROM generate_series(1, $2) RETURNING job_id",
    )
    .bind(queue)
    .bind(rows)
    .fetch_all(pool)
    .await
    .expect("seed dlq rows")
}

async fn dlq_depth(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM awa.dlq_entries WHERE queue = $1")
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("dlq depth")
}

#[tokio::test]
async fn retry_bulk_drains_in_batches_up_to_the_limit() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("cli_dlq_batches");
    seed_dlq(&pool, &queue, 7).await;

    let limited = run_cli(&[
        "dlq",
        "retry-bulk",
        "--queue",
        &queue,
        "--batch-size",
        "2",
        "--limit",
        "5",
    ])
    .assert()
    .success();
    let stdout = String::from_utf8(limited.get_output().stdout.clone()).expect("stdout");
    assert!(stdout.contains("Retried 5 DLQ rows."), "{stdout}");
    assert_eq!(dlq_depth(&pool, &queue).await, 2);

    let drained = run_cli(&["dlq", "retry-bulk", "--queue", &queue, "--batch-size", "2"])
        .assert()
        .success();
    let stdout = String::from_utf8(drained.get_output().stdout.clone()).expect("stdout");
    assert!(stdout.contains("Retried 2 DLQ rows."), "{stdout}");
    assert_eq!(dlq_depth(&pool, &queue).await, 0);

    awa_testing::setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn retry_bulk_stops_on_a_unique_conflict_and_names_the_job() {
    let _guard = GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("cli_dlq_conflict");
    let key = format!("cli-dlq-{queue}").into_bytes();
    let keyed: i64 = sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries \
             (job_id, kind, queue, priority, dlq_reason, original_run_lease, unique_key, unique_states) \
         VALUES (nextval('awa.job_id_seq'), 'cli_dlq_job', $1, 2, 'seeded', 1, $2, '11000000') \
         RETURNING job_id",
    )
    .bind(&queue)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("seed keyed dlq row");
    seed_dlq(&pool, &queue, 2).await;
    sqlx::query("INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, $2)")
        .bind(&key)
        .bind(i64::MAX - 3)
        .execute(&pool)
        .await
        .expect("live job holds the key");

    let failed = run_cli(&["dlq", "retry-bulk", "--queue", &queue, "--batch-size", "10"])
        .assert()
        .failure();
    let stderr = String::from_utf8(failed.get_output().stderr.clone()).expect("stderr");
    assert!(stderr.contains(&format!("DLQ job {keyed} ")), "{stderr}");
    assert_eq!(dlq_depth(&pool, &queue).await, 3);

    sqlx::query("DELETE FROM awa.job_unique_claims WHERE unique_key = $1")
        .bind(&key)
        .execute(&pool)
        .await
        .expect("drop claim");
    awa_testing::setup::clean_queue(&pool, &queue).await;
}
