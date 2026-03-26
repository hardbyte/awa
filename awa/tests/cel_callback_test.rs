//! Integration tests for CEL callback expressions.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::admin::{self, CallbackConfig, DefaultAction};
use awa::model::migrations;
use awa::{AwaError, JobArgs, JobContext, JobError, JobResult, JobState, Worker};
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
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to clean queue jobs");
}

// -- Job types --

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct WebhookPayment {
    pub order_id: i64,
}

// -- Worker that registers a callback (no CEL) --

struct PlainCallbackWorker;

#[async_trait::async_trait]
impl Worker for PlainCallbackWorker {
    fn kind(&self) -> &'static str {
        "webhook_payment"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let callback = ctx
            .register_callback(Duration::from_secs(3600))
            .await
            .map_err(JobError::retryable)?;
        Ok(JobResult::WaitForCallback(callback))
    }
}

// -- Worker that registers a callback with CEL expressions --

#[cfg(feature = "cel")]
struct CelCallbackWorker {
    config: CallbackConfig,
}

#[cfg(feature = "cel")]
#[async_trait::async_trait]
impl Worker for CelCallbackWorker {
    fn kind(&self) -> &'static str {
        "webhook_payment"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let callback = ctx
            .register_callback_with_config(Duration::from_secs(3600), &self.config)
            .await
            .map_err(JobError::retryable)?;
        Ok(JobResult::WaitForCallback(callback))
    }
}

/// Helper: insert a job, work it to waiting_external, return the callback_id.
async fn insert_and_wait<W: Worker>(
    client: &TestClient,
    queue: &str,
    worker: &W,
) -> (i64, uuid::Uuid) {
    let job = awa::insert_with(
        client.pool(),
        &WebhookPayment { order_id: 100 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = client.work_one_in_queue(worker, Some(queue)).await.unwrap();
    assert!(result.is_waiting_external());

    let updated = client.get_job(job.id).await.unwrap();
    let callback_id = updated.callback_id.unwrap();
    (job.id, callback_id)
}

// ── C1: resolve_callback no expressions + default_action=Complete → completed ──

#[tokio::test]
async fn test_c1_resolve_no_expressions_default_complete() {
    let client = setup().await;
    let queue = "test_c1_resolve_default_complete";
    clean_queue(client.pool(), queue).await;

    let (job_id, callback_id) = insert_and_wait(&client, queue, &PlainCallbackWorker).await;

    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"status": "ok"})),
        DefaultAction::Complete,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_completed());
    if let admin::ResolveOutcome::Completed { payload, job } = result {
        assert_eq!(job.state, JobState::Completed);
        assert_eq!(job.id, job_id);
        assert!(payload.is_some());
    }
}

// ── C2: resolve_callback no expressions + default_action=Ignore → ignored ──

#[tokio::test]
async fn test_c2_resolve_no_expressions_default_ignore() {
    let client = setup().await;
    let queue = "test_c2_resolve_default_ignore";
    clean_queue(client.pool(), queue).await;

    let (job_id, callback_id) = insert_and_wait(&client, queue, &PlainCallbackWorker).await;

    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"status": "ok"})),
        DefaultAction::Ignore,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_ignored());

    // Job should still be waiting_external
    let job = client.get_job(job_id).await.unwrap();
    assert_eq!(job.state, JobState::WaitingExternal);
}

// ── C3: filter=false → Ignored, job stays waiting_external ──

#[cfg(feature = "cel")]
#[tokio::test]
async fn test_c3_filter_false_ignored() {
    let client = setup().await;
    let queue = "test_c3_filter_false";
    clean_queue(client.pool(), queue).await;

    let worker = CelCallbackWorker {
        config: CallbackConfig {
            filter: Some(r#"payload.status != "test""#.to_string()),
            on_complete: Some("true".to_string()),
            ..Default::default()
        },
    };

    let (job_id, callback_id) = insert_and_wait(&client, queue, &worker).await;

    // Send a payload that fails the filter
    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"status": "test"})),
        DefaultAction::Complete,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_ignored());

    // Job still waiting
    let job = client.get_job(job_id).await.unwrap();
    assert_eq!(job.state, JobState::WaitingExternal);
}

// ── C4: filter=true, on_complete=true → Completed ──

#[cfg(feature = "cel")]
#[tokio::test]
async fn test_c4_filter_pass_complete() {
    let client = setup().await;
    let queue = "test_c4_filter_pass_complete";
    clean_queue(client.pool(), queue).await;

    let worker = CelCallbackWorker {
        config: CallbackConfig {
            filter: Some(r#"payload.status != "test""#.to_string()),
            on_complete: Some(r#"payload.event == "charge.succeeded""#.to_string()),
            ..Default::default()
        },
    };

    let (job_id, callback_id) = insert_and_wait(&client, queue, &worker).await;

    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"status": "live", "event": "charge.succeeded"})),
        DefaultAction::Ignore,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_completed());
    let job = client.get_job(job_id).await.unwrap();
    assert_eq!(job.state, JobState::Completed);
}

// ── C5: on_fail=true → Failed, error includes expression text ──

#[cfg(feature = "cel")]
#[tokio::test]
async fn test_c5_on_fail_matched() {
    let client = setup().await;
    let queue = "test_c5_on_fail";
    clean_queue(client.pool(), queue).await;

    let on_fail_expr = r#"payload.event == "charge.failed""#;
    let worker = CelCallbackWorker {
        config: CallbackConfig {
            on_fail: Some(on_fail_expr.to_string()),
            on_complete: Some(r#"payload.event == "charge.succeeded""#.to_string()),
            ..Default::default()
        },
    };

    let (job_id, callback_id) = insert_and_wait(&client, queue, &worker).await;

    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"event": "charge.failed"})),
        DefaultAction::Complete,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_failed());
    let job = client.get_job(job_id).await.unwrap();
    assert_eq!(job.state, JobState::Failed);

    // Error should have a separate "expression" field with the source
    let errors = job.errors.unwrap();
    let last_error = errors.last().unwrap();
    assert_eq!(
        last_error["error"].as_str().unwrap(),
        "callback failed: on_fail expression matched"
    );
    assert_eq!(
        last_error["expression"].as_str().unwrap(),
        on_fail_expr,
        "Error should contain expression source in dedicated field"
    );
}

// ── C6: on_fail=true AND on_complete=true → Failed (fail takes precedence) ──

#[cfg(feature = "cel")]
#[tokio::test]
async fn test_c6_fail_takes_precedence() {
    let client = setup().await;
    let queue = "test_c6_fail_precedence";
    clean_queue(client.pool(), queue).await;

    let worker = CelCallbackWorker {
        config: CallbackConfig {
            on_fail: Some("true".to_string()),
            on_complete: Some("true".to_string()),
            ..Default::default()
        },
    };

    let (job_id, callback_id) = insert_and_wait(&client, queue, &worker).await;

    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"anything": true})),
        DefaultAction::Complete,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_failed());
    let job = client.get_job(job_id).await.unwrap();
    assert_eq!(job.state, JobState::Failed);
}

// ── C7: on_complete=true + transform → Completed with transformed payload ──

#[cfg(feature = "cel")]
#[tokio::test]
async fn test_c7_transform_payload() {
    let client = setup().await;
    let queue = "test_c7_transform";
    clean_queue(client.pool(), queue).await;

    // Use a simple arithmetic transform (CEL map literal keys may vary by impl)
    let worker = CelCallbackWorker {
        config: CallbackConfig {
            on_complete: Some("true".to_string()),
            transform: Some("int(payload.amount_cents) / 100".to_string()),
            ..Default::default()
        },
    };

    let (_job_id, callback_id) = insert_and_wait(&client, queue, &worker).await;

    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"amount_cents": 4200})),
        DefaultAction::Ignore,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_completed());
    if let admin::ResolveOutcome::Completed { payload, .. } = result {
        let p = payload.unwrap();
        assert_eq!(p, serde_json::json!(42));
    }
}

// ── C8: Invalid filter (fail-open) → still evaluates conditions ──

#[cfg(feature = "cel")]
#[tokio::test]
async fn test_c8_invalid_filter_fail_open() {
    let client = setup().await;
    let queue = "test_c8_invalid_filter";
    clean_queue(client.pool(), queue).await;

    // We can't use register_callback_with_config for invalid syntax
    // (it validates at registration). So we manually set the expression.
    let job = awa::insert_with(
        client.pool(),
        &WebhookPayment { order_id: 200 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = client
        .work_one_in_queue(&PlainCallbackWorker, Some(queue))
        .await
        .unwrap();
    assert!(result.is_waiting_external());

    let waiting = client.get_job(job.id).await.unwrap();
    let callback_id = waiting.callback_id.unwrap();

    // Manually set invalid filter + valid on_complete
    sqlx::query(
        "UPDATE awa.jobs SET callback_filter = $2, callback_on_complete = $3 WHERE id = $1",
    )
    .bind(job.id)
    .bind("this is not valid CEL !!!!")
    .bind("true")
    .execute(client.pool())
    .await
    .unwrap();

    // Should fail-open on filter (pass through) then evaluate on_complete → complete
    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"ok": true})),
        DefaultAction::Ignore,
        None,
    )
    .await
    .unwrap();

    assert!(
        result.is_completed(),
        "Invalid filter should fail-open (pass through)"
    );
}

// ── C9: Invalid transform (fail-open) → completes with original payload ──

#[cfg(feature = "cel")]
#[tokio::test]
async fn test_c9_invalid_transform_fail_open() {
    let client = setup().await;
    let queue = "test_c9_invalid_transform";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &WebhookPayment { order_id: 201 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    client
        .work_one_in_queue(&PlainCallbackWorker, Some(queue))
        .await
        .unwrap();

    let waiting = client.get_job(job.id).await.unwrap();
    let callback_id = waiting.callback_id.unwrap();

    // Set valid on_complete + invalid transform
    sqlx::query(
        "UPDATE awa.jobs SET callback_on_complete = $2, callback_transform = $3 WHERE id = $1",
    )
    .bind(job.id)
    .bind("true")
    .bind("totally broken !!! CEL")
    .execute(client.pool())
    .await
    .unwrap();

    let payload = serde_json::json!({"original": true});
    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(payload.clone()),
        DefaultAction::Ignore,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_completed());
    if let admin::ResolveOutcome::Completed {
        payload: result_payload,
        ..
    } = result
    {
        assert_eq!(
            result_payload.unwrap(),
            payload,
            "Invalid transform should fall back to original payload"
        );
    }
}

// ── C10: Neither condition matches → falls through to default_action ──

#[cfg(feature = "cel")]
#[tokio::test]
async fn test_c10_fallthrough_to_default() {
    let client = setup().await;
    let queue = "test_c10_fallthrough";
    clean_queue(client.pool(), queue).await;

    let worker = CelCallbackWorker {
        config: CallbackConfig {
            on_complete: Some(r#"payload.event == "charge.succeeded""#.to_string()),
            on_fail: Some(r#"payload.event == "charge.failed""#.to_string()),
            ..Default::default()
        },
    };

    let (job_id, callback_id) = insert_and_wait(&client, queue, &worker).await;

    // Payload matches neither condition
    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"event": "charge.pending"})),
        DefaultAction::Ignore,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_ignored());
    let job = client.get_job(job_id).await.unwrap();
    assert_eq!(job.state, JobState::WaitingExternal);
}

// ── C11: register_callback_with_config with invalid CEL syntax → validation error ──

#[cfg(feature = "cel")]
#[tokio::test]
async fn test_c11_invalid_cel_at_registration() {
    let client = setup().await;
    let queue = "test_c11_invalid_cel_reg";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &WebhookPayment { order_id: 300 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Claim the job to running state
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, heartbeat_at = now() WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    let config = CallbackConfig {
        filter: Some("this is not valid CEL !!!!".to_string()),
        ..Default::default()
    };

    let err = admin::register_callback_with_config(
        client.pool(),
        job.id,
        0,
        Duration::from_secs(3600),
        &config,
    )
    .await
    .unwrap_err();

    match err {
        AwaError::Validation(msg) => {
            assert!(
                msg.contains("invalid CEL expression"),
                "Error should mention invalid CEL expression: {msg}"
            );
        }
        other => panic!("Expected Validation error, got: {other:?}"),
    }
}

// ── C12: double resolve_callback → CallbackNotFound on second ──

#[tokio::test]
async fn test_c12_double_resolve() {
    let client = setup().await;
    let queue = "test_c12_double_resolve";
    clean_queue(client.pool(), queue).await;

    let (_job_id, callback_id) = insert_and_wait(&client, queue, &PlainCallbackWorker).await;

    // First resolve succeeds
    admin::resolve_callback(
        client.pool(),
        callback_id,
        None,
        DefaultAction::Complete,
        None,
    )
    .await
    .unwrap();

    // Second resolve fails
    let err = admin::resolve_callback(
        client.pool(),
        callback_id,
        None,
        DefaultAction::Complete,
        None,
    )
    .await
    .unwrap_err();

    match err {
        AwaError::CallbackNotFound { .. } => {}
        other => panic!("Expected CallbackNotFound, got: {other:?}"),
    }
}

// ── C13: V4 migration from V3, idempotent ──

#[tokio::test]
async fn test_c13_migration_idempotent() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .expect("Failed to connect to database");

    migrations::run(&pool).await.unwrap();
    let version = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version, migrations::CURRENT_VERSION);

    // Run again — should be idempotent
    migrations::run(&pool).await.unwrap();
    let version2 = migrations::current_version(&pool).await.unwrap();
    assert_eq!(version2, migrations::CURRENT_VERSION);
}

// ── C14: Deeply nested payload + traversal expression ──

#[cfg(feature = "cel")]
#[tokio::test]
async fn test_c14_deeply_nested_payload() {
    let client = setup().await;
    let queue = "test_c14_nested";
    clean_queue(client.pool(), queue).await;

    let worker = CelCallbackWorker {
        config: CallbackConfig {
            on_complete: Some(r#"payload.data.charge.status == "succeeded""#.to_string()),
            ..Default::default()
        },
    };

    let (_job_id, callback_id) = insert_and_wait(&client, queue, &worker).await;

    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(serde_json::json!({
            "data": {
                "charge": {
                    "status": "succeeded",
                    "amount": 4200
                }
            }
        })),
        DefaultAction::Ignore,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_completed());
}

// ── C15: Missing field in payload (CEL error, fail-open) ──

#[cfg(feature = "cel")]
#[tokio::test]
async fn test_c15_missing_field_fail_open() {
    let client = setup().await;
    let queue = "test_c15_missing_field";
    clean_queue(client.pool(), queue).await;

    let worker = CelCallbackWorker {
        config: CallbackConfig {
            on_complete: Some(r#"payload.nonexistent_field == "value""#.to_string()),
            ..Default::default()
        },
    };

    let (job_id, callback_id) = insert_and_wait(&client, queue, &worker).await;

    // Payload doesn't have the field — CEL error, fail-open to false
    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"other": "data"})),
        DefaultAction::Ignore,
        None,
    )
    .await
    .unwrap();

    // on_complete errors → false (fail-open), falls through to default=Ignore
    assert!(result.is_ignored());
    let job = client.get_job(job_id).await.unwrap();
    assert_eq!(job.state, JobState::WaitingExternal);
}

// ── C16: resolve_callback accepts running state (race handling) ──
// A fast callback can arrive before the executor transitions running ->
// waiting_external. resolve_callback now matches complete_external/fail_external
// behavior by accepting both states.

#[tokio::test]
async fn test_c16_resolve_accepts_running() {
    let client = setup().await;
    let queue = "test_c16_running_accepted";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &WebhookPayment { order_id: 400 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Set job to running with a callback_id (simulating register_callback done,
    // but executor hasn't transitioned to waiting_external yet)
    let callback_id = uuid::Uuid::new_v4();
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, heartbeat_at = now(),
         callback_id = $2, callback_timeout_at = now() + interval '1 hour'
         WHERE id = $1",
    )
    .bind(job.id)
    .bind(callback_id)
    .execute(client.pool())
    .await
    .unwrap();

    // resolve_callback should now find it (accepts running state)
    let result = admin::resolve_callback(
        client.pool(),
        callback_id,
        Some(serde_json::json!({"status": "ok"})),
        DefaultAction::Complete,
        None,
    )
    .await
    .unwrap();

    assert!(result.is_completed());
    let updated = client.get_job(job.id).await.unwrap();
    assert_eq!(updated.state, JobState::Completed);
}

// ── C17: Concurrent resolve_callback (FOR UPDATE prevents race) ──
// Use multi_thread + tokio::spawn to ensure both futures truly run
// in parallel on separate tasks, exercising the FOR UPDATE lock.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_c17_concurrent_resolve() {
    let client = setup().await;
    let queue = "test_c17_concurrent";
    clean_queue(client.pool(), queue).await;

    let (_job_id, callback_id) = insert_and_wait(&client, queue, &PlainCallbackWorker).await;

    let pool1 = client.pool().clone();
    let pool2 = client.pool().clone();

    // Spawn on separate tasks to guarantee true parallel execution
    let h1 = tokio::spawn(async move {
        admin::resolve_callback(&pool1, callback_id, None, DefaultAction::Complete, None).await
    });
    let h2 = tokio::spawn(async move {
        admin::resolve_callback(&pool2, callback_id, None, DefaultAction::Complete, None).await
    });

    let r1 = h1.await.expect("task 1 panicked");
    let r2 = h2.await.expect("task 2 panicked");

    // One should succeed, the other should fail with CallbackNotFound
    let successes = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    let failures = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r.as_ref().err(), Some(AwaError::CallbackNotFound { .. })))
        .count();

    assert_eq!(successes, 1, "Exactly one resolve should succeed");
    assert_eq!(
        failures, 1,
        "Exactly one resolve should get CallbackNotFound"
    );
}

// ── C18: register_callback_with_config with expressions but cel feature disabled → error ──
// This test only makes sense when compiled without the cel feature.
// We test it indirectly by verifying the code path exists.

#[cfg(not(feature = "cel"))]
#[tokio::test]
async fn test_c18_cel_disabled_register_error() {
    let client = setup().await;
    let queue = "test_c18_cel_disabled";
    clean_queue(client.pool(), queue).await;

    let job = awa::insert_with(
        client.pool(),
        &WebhookPayment { order_id: 500 },
        awa::InsertOpts {
            queue: queue.to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Manually set to running
    sqlx::query(
        "UPDATE awa.jobs SET state = 'running', attempt = 1, heartbeat_at = now() WHERE id = $1",
    )
    .bind(job.id)
    .execute(client.pool())
    .await
    .unwrap();

    let config = CallbackConfig {
        filter: Some("true".to_string()),
        ..Default::default()
    };

    let err = admin::register_callback_with_config(
        client.pool(),
        job.id,
        0,
        Duration::from_secs(3600),
        &config,
    )
    .await
    .unwrap_err();

    match err {
        AwaError::Validation(msg) => {
            assert!(msg.contains("CEL expressions require the 'cel' feature"));
        }
        other => panic!("Expected Validation error, got: {other:?}"),
    }
}

// ── C19: resolve_callback with expressions in DB but cel feature disabled → error ──
// Like C18, this only runs without cel feature.

#[cfg(not(feature = "cel"))]
#[tokio::test]
async fn test_c19_cel_disabled_resolve_error() {
    let client = setup().await;
    let queue = "test_c19_cel_disabled_resolve";
    clean_queue(client.pool(), queue).await;

    let (job_id, callback_id) = insert_and_wait(&client, queue, &PlainCallbackWorker).await;

    // Manually set CEL expressions
    sqlx::query("UPDATE awa.jobs SET callback_on_complete = 'true' WHERE id = $1")
        .bind(job_id)
        .execute(client.pool())
        .await
        .unwrap();

    // Without cel feature, expressions present → Validation error (non-destructive)
    let err = admin::resolve_callback(
        client.pool(),
        callback_id,
        None,
        DefaultAction::Complete,
        None,
    )
    .await
    .unwrap_err();

    match err {
        AwaError::Validation(msg) => {
            assert!(msg.contains("CEL expressions present but 'cel' feature is not enabled"));
        }
        other => panic!("Expected Validation error, got: {other:?}"),
    }

    // Job should still be waiting_external (not mutated)
    let job = client.get_job(job_id).await.unwrap();
    assert_eq!(job.state, JobState::WaitingExternal);
}
