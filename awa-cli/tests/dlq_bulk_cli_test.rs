//! `awa dlq retry-bulk` and `awa dlq purge` submit a batch operation and
//! leave the rows to the maintenance leader.

use assert_cmd::Command;
use awa_model::batch_operations::{self, BatchOperationKind, BatchOperationState};
use std::time::Duration;
use uuid::Uuid;

async fn seed_dlq_row(pool: &sqlx::PgPool, queue: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries (job_id, kind, queue, priority, dlq_reason, original_run_lease) \
         VALUES (nextval('awa.job_id_seq'), 'cli_dlq_bulk_job', $1, 2, 'seeded', 1) RETURNING job_id",
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("seed dlq row")
}

fn operation_id(stdout: &str) -> Uuid {
    let id = stdout
        .split_whitespace()
        .nth(4)
        .unwrap_or_else(|| panic!("no operation id in {stdout:?}"));
    Uuid::parse_str(id).unwrap_or_else(|err| panic!("{id:?} in {stdout:?}: {err}"))
}

#[tokio::test]
async fn dlq_retry_bulk_submits_a_batch_operation() {
    let db = awa_testing::setup::TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "cli_dlq_retry_bulk";
    seed_dlq_row(&pool, queue).await;

    Command::cargo_bin("awa")
        .expect("awa binary should build")
        .env("DATABASE_URL", db.url())
        .args(["dlq", "retry-bulk"])
        .assert()
        .failure();
    assert!(
        batch_operations::list_batch_operations(&pool, &Default::default())
            .await
            .unwrap()
            .is_empty(),
        "an unscoped retry submits nothing"
    );

    let output = Command::cargo_bin("awa")
        .expect("awa binary should build")
        .env("DATABASE_URL", db.url())
        .args(["dlq", "retry-bulk", "--queue", queue])
        .output()
        .expect("run awa");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.starts_with("Submitted retry_from_dlq batch operation "),
        "{stdout}"
    );
    let id = operation_id(&stdout);
    assert!(
        stdout.contains(&format!("awa batch-ops get {id}")),
        "{stdout}"
    );

    let operation = batch_operations::get_batch_operation(&pool, id)
        .await
        .unwrap();
    assert_eq!(operation.op_kind, BatchOperationKind::RetryFromDlq);
    assert_eq!(operation.state, BatchOperationState::Pending);
    assert_eq!(operation.submitted_by.as_deref(), Some("cli"));
    assert_eq!(operation.filter.0.queue.as_deref(), Some(queue));
    assert_eq!(
        awa_model::dlq::dlq_depth(&pool, Some(queue)).await.unwrap(),
        1,
        "submitting changes no row"
    );
}

#[tokio::test]
async fn dlq_purge_wait_reports_the_finished_operation() {
    let db = awa_testing::setup::TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "cli_dlq_purge_wait";
    seed_dlq_row(&pool, queue).await;
    seed_dlq_row(&pool, "cli_dlq_purge_other").await;

    let url = db.url();
    let cli = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("awa")
            .expect("awa binary should build")
            .env("DATABASE_URL", url)
            .args(["dlq", "purge", "--queue", queue, "--wait"])
            .timeout(Duration::from_secs(60))
            .output()
            .expect("run awa")
    });

    let runner = Uuid::new_v4();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !cli.is_finished() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "awa dlq purge --wait did not return"
        );
        batch_operations::run_one_batch_operation_chunk(&pool, runner, 100)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let output = cli.await.unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let id = operation_id(&stdout);
    assert!(
        stdout.contains(&format!(
            "Batch operation {id} completed: 1 processed, 0 skipped, 0 errored."
        )),
        "{stdout}"
    );
    let operation = batch_operations::get_batch_operation(&pool, id)
        .await
        .unwrap();
    assert_eq!(operation.op_kind, BatchOperationKind::PurgeDlq);
    assert_eq!(operation.submitted_by.as_deref(), Some("cli"));
    assert_eq!(
        awa_model::dlq::dlq_depth(&pool, Some(queue)).await.unwrap(),
        0
    );
    assert_eq!(
        awa_model::dlq::dlq_depth(&pool, Some("cli_dlq_purge_other"))
            .await
            .unwrap(),
        1
    );
}
