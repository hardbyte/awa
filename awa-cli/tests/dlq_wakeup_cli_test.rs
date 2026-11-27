//! `awa dlq retry` wakes the queue's claimers before the process exits.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use assert_cmd::Command;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn dlq_retry_wakes_claimers_before_the_process_exits() {
    let pool = awa_testing::setup::setup(4).await;
    awa_testing::setup::activate_queue_storage(&pool).await;
    let queue = format!(
        "cli_dlq_wake_{}",
        &Uuid::new_v4().simple().to_string()[..12]
    );
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries (job_id, kind, queue, priority, dlq_reason, original_run_lease) \
         VALUES (nextval('awa.job_id_seq'), 'cli_dlq_job', $1, 2, 'seeded', 1) RETURNING job_id",
    )
    .bind(&queue)
    .fetch_one(&pool)
    .await
    .expect("seed dlq row");
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
        .await
        .expect("connect listener");
    listener
        .listen(&format!("awa:{queue}"))
        .await
        .expect("listen");

    // The retry commits, then hands its queue's wake-up to a background
    // sender; the process exits right after, so the CLI must flush it first.
    Command::cargo_bin("awa")
        .expect("awa binary should build")
        .env("DATABASE_URL", awa_testing::setup::database_url())
        .args(["dlq", "retry", &id.to_string()])
        .assert()
        .success();

    let notification = tokio::time::timeout(Duration::from_secs(2), listener.recv())
        .await
        .expect("the retry woke the queue before the CLI exited")
        .expect("listener stays connected");
    assert_eq!(notification.channel(), format!("awa:{queue}"));

    awa_testing::setup::clean_queue(&pool, &queue).await;
}
