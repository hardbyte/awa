use assert_cmd::Command;
use awa_model::{insert_with, InsertOpts, JobArgs};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct InspectEmail {
    to: String,
    subject: String,
}

impl JobArgs for InspectEmail {
    fn kind() -> &'static str {
        "inspect_email"
    }
}

fn database_url() -> String {
    awa_testing::setup::database_url()
}

async fn setup_pool() -> sqlx::PgPool {
    awa_testing::setup::setup(4).await
}

fn unique_queue(prefix: &str) -> String {
    format!("{prefix}_{}", rand_suffix())
}

fn rand_suffix() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

async fn seed_inspection_job(pool: &sqlx::PgPool, queue: &str) -> (i64, Uuid) {
    awa_testing::setup::clean_queue(pool, queue).await;

    let job = insert_with(
        pool,
        &InspectEmail {
            to: "alice@example.com".into(),
            subject: "Inspect".into(),
        },
        InsertOpts {
            queue: queue.to_string(),
            metadata: json!({"tenant": "acme"}),
            tags: vec!["cli".into(), "inspect".into()],
            ..Default::default()
        },
    )
    .await
    .expect("job should insert");

    let callback_id = Uuid::new_v4();
    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'waiting_external',
            priority = 1,
            attempt = 3,
            run_lease = 7,
            attempted_at = now() - interval '2 minutes',
            heartbeat_at = now() - interval '10 seconds',
            callback_id = $2,
            callback_timeout_at = now() + interval '30 minutes',
            callback_filter = 'payload.kind == "payment"',
            callback_on_complete = 'payload.status == "ok"',
            callback_on_fail = 'payload.status == "fail"',
            callback_transform = 'payload',
            progress = $3::jsonb,
            metadata = $4::jsonb,
            errors = ARRAY[$5::jsonb, $6::jsonb]
        WHERE id = $1
        "#,
    )
    .bind(job.id)
    .bind(callback_id)
    .bind(json!({
        "percent": 42,
        "message": "waiting on gateway",
        "metadata": {"cursor": 99}
    }))
    .bind(json!({
        "tenant": "acme",
        "_awa_original_priority": 4
    }))
    .bind(json!({
        "error": "first failure",
        "attempt": 1,
        "at": "2026-04-01T00:00:00Z"
    }))
    .bind(json!({
        "error": "second failure",
        "attempt": 2,
        "at": "2026-04-01T00:05:00Z"
    }))
    .execute(pool)
    .await
    .expect("job should update");

    (job.id, callback_id)
}

fn run_cli(args: &[&str]) -> Command {
    let mut command = Command::cargo_bin("awa").expect("awa binary should build");
    command.env("DATABASE_URL", database_url()).args(args);
    command
}

#[tokio::test]
async fn job_dump_prints_rich_job_snapshot() {
    let pool = setup_pool().await;
    let queue = unique_queue("cli_dump_job");
    let (job_id, _callback_id) = seed_inspection_job(&pool, &queue).await;

    let assert = run_cli(&["job", "dump", &job_id.to_string()])
        .assert()
        .success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let payload: Value = serde_json::from_str(&stdout).expect("dump output should be json");

    assert_eq!(payload["job"]["id"].as_i64(), Some(job_id));
    assert_eq!(payload["job"]["queue"].as_str(), Some(queue.as_str()));
    assert_eq!(payload["job"]["state"].as_str(), Some("waiting_external"));
    assert_eq!(payload["summary"]["original_priority"].as_i64(), Some(4));
    assert_eq!(payload["summary"]["can_retry"].as_bool(), Some(true));
    assert_eq!(payload["summary"]["can_cancel"].as_bool(), Some(true));
    assert_eq!(payload["summary"]["error_count"].as_u64(), Some(2));

    let labels: Vec<_> = payload["timeline"]
        .as_array()
        .expect("timeline array")
        .iter()
        .filter_map(|item| item.get("label").and_then(Value::as_str))
        .collect();
    assert!(labels.contains(&"Created"));
    assert!(labels.contains(&"Attempt 1 failed"));
    assert!(labels.contains(&"Attempt 2 failed"));
    assert!(labels.contains(&"Attempt 3 — waiting for callback"));

    awa_testing::setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn dump_run_defaults_to_current_attempt_snapshot() {
    let pool = setup_pool().await;
    let queue = unique_queue("cli_dump_run_current");
    let (job_id, callback_id) = seed_inspection_job(&pool, &queue).await;

    let assert = run_cli(&["job", "dump-run", &job_id.to_string()])
        .assert()
        .success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let payload: Value = serde_json::from_str(&stdout).expect("dump output should be json");
    let callback_id_text = callback_id.to_string();

    assert_eq!(payload["job_id"].as_i64(), Some(job_id));
    assert_eq!(payload["queue"].as_str(), Some(queue.as_str()));
    assert_eq!(payload["source"].as_str(), Some("current_job_row"));
    assert_eq!(payload["selected_attempt"].as_i64(), Some(3));
    assert_eq!(payload["current_attempt"].as_i64(), Some(3));
    assert_eq!(payload["selected_run_lease"].as_i64(), Some(7));
    assert_eq!(payload["state"].as_str(), Some("waiting_external"));
    assert_eq!(
        payload["callback"]["callback_id"].as_str(),
        Some(callback_id_text.as_str())
    );
    assert_eq!(payload["progress"]["percent"].as_i64(), Some(42));
    assert_eq!(payload["metadata"]["tenant"].as_str(), Some("acme"));

    awa_testing::setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn dump_run_reconstructs_historical_attempt_from_errors() {
    let pool = setup_pool().await;
    let queue = unique_queue("cli_dump_run_history");
    let (job_id, _callback_id) = seed_inspection_job(&pool, &queue).await;

    let assert = run_cli(&["job", "dump-run", &job_id.to_string(), "--attempt", "2"])
        .assert()
        .success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let payload: Value = serde_json::from_str(&stdout).expect("dump output should be json");

    assert_eq!(payload["source"].as_str(), Some("error_history"));
    assert_eq!(payload["selected_attempt"].as_i64(), Some(2));
    assert_eq!(payload["state"].as_str(), Some("retryable"));
    assert_eq!(payload["error"].as_str(), Some("second failure"));
    assert_eq!(payload["terminal"].as_bool(), Some(false));
    assert!(payload["selected_run_lease"].is_null());
    assert_eq!(
        payload["finished_at"].as_str(),
        Some("2026-04-01T00:05:00Z")
    );
    assert!(payload["notes"]
        .as_array()
        .expect("notes array")
        .iter()
        .any(|entry| entry.as_str().unwrap_or_default().contains("errors[]")));

    awa_testing::setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn dump_run_preserves_historical_attempts_after_retry_reset() {
    let pool = setup_pool().await;
    let queue = unique_queue("cli_dump_run_retry_reset");
    let (job_id, _callback_id) = seed_inspection_job(&pool, &queue).await;

    awa_model::admin::retry(&pool, job_id)
        .await
        .expect("retry should succeed");

    let assert = run_cli(&["job", "dump-run", &job_id.to_string(), "--attempt", "2"])
        .assert()
        .success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let payload: Value = serde_json::from_str(&stdout).expect("dump output should be json");

    assert_eq!(payload["source"].as_str(), Some("error_history"));
    assert_eq!(payload["selected_attempt"].as_i64(), Some(2));
    assert_eq!(payload["current_attempt"].as_i64(), Some(0));
    assert_eq!(payload["state"].as_str(), Some("retryable"));
    assert_eq!(payload["error"].as_str(), Some("second failure"));

    awa_testing::setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn dump_run_rejects_unknown_attempt() {
    let pool = setup_pool().await;
    let queue = unique_queue("cli_dump_run_invalid");
    let (job_id, _callback_id) = seed_inspection_job(&pool, &queue).await;

    let assert = run_cli(&["job", "dump-run", &job_id.to_string(), "--attempt", "4"])
        .assert()
        .failure();
    let stderr = String::from_utf8(assert.get_output().stderr.clone()).expect("stderr utf8");
    assert!(stderr.contains("attempt 4 is not available"));

    awa_testing::setup::clean_queue(&pool, &queue).await;
}
