use awa::{CallbackPollResult, DefaultAction, InsertOpts, JobState};
use awa_seaorm::{migrate, JobRepository};
use sea_orm::DatabaseConnection;
use std::time::Duration;

async fn setup_database(queue: &str) -> (sqlx::PgPool, DatabaseConnection) {
    let pool = awa_testing::setup::setup(3).await;
    awa_testing::setup::clean_queue(&pool, queue).await;
    let db = DatabaseConnection::from(pool.clone());
    migrate(&db).await.expect("awa migration should succeed");
    (pool, db)
}

fn opts(queue: &str) -> InsertOpts {
    InsertOpts {
        queue: queue.to_string(),
        ..Default::default()
    }
}

async fn mark_running(pool: &sqlx::PgPool, job_id: i64, run_lease: i64) {
    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'running',
            run_lease = $2,
            attempted_at = now(),
            heartbeat_at = now()
        WHERE id = $1
        "#,
    )
    .bind(job_id)
    .bind(run_lease)
    .execute(pool)
    .await
    .expect("mark job running");
}

#[tokio::test]
async fn repository_completes_external_callback() {
    let queue = "seaorm_callback_complete";
    let (pool, db) = setup_database(queue).await;
    let repo = JobRepository::new(&db);

    let job = repo
        .insert_raw("seaorm_callback_job", serde_json::json!({}), opts(queue))
        .await
        .expect("insert callback job");
    mark_running(&pool, job.id, 42).await;

    let callback_id = repo
        .register_callback(job.id, 42, Duration::from_secs(60))
        .await
        .expect("register callback");
    assert!(repo
        .enter_callback_wait(job.id, 42, callback_id)
        .await
        .expect("enter wait"));

    let completed = repo
        .complete_external(callback_id, Some(serde_json::json!({"ok": true})), Some(42))
        .await
        .expect("complete callback");
    assert_eq!(completed.state, JobState::Completed);
    assert!(completed.metadata.get("_awa_callback_result").is_none());
}

#[tokio::test]
async fn repository_resolves_callback_with_default_action() {
    let queue = "seaorm_callback_resolve";
    let (pool, db) = setup_database(queue).await;
    let repo = JobRepository::new(&db);

    let job = repo
        .insert_raw("seaorm_resolve_job", serde_json::json!({}), opts(queue))
        .await
        .expect("insert callback job");
    mark_running(&pool, job.id, 77).await;

    let callback_id = repo
        .register_callback(job.id, 77, Duration::from_secs(60))
        .await
        .expect("register callback");
    repo.enter_callback_wait(job.id, 77, callback_id)
        .await
        .expect("enter wait");

    let outcome = repo
        .resolve_callback(
            callback_id,
            Some(serde_json::json!({"status": "ok"})),
            DefaultAction::Complete,
            Some(77),
        )
        .await
        .expect("resolve callback");

    assert!(outcome.is_completed());
}

#[tokio::test]
async fn repository_resumes_external_callback_and_cleans_payload() {
    let queue = "seaorm_callback_resume";
    let (pool, db) = setup_database(queue).await;
    let repo = JobRepository::new(&db);

    let job = repo
        .insert_raw("seaorm_resume_job", serde_json::json!({}), opts(queue))
        .await
        .expect("insert callback job");
    mark_running(&pool, job.id, 88).await;

    let callback_id = repo
        .register_callback(job.id, 88, Duration::from_secs(60))
        .await
        .expect("register callback");
    repo.enter_callback_wait(job.id, 88, callback_id)
        .await
        .expect("enter wait");

    let payload = serde_json::json!({"status": "resume"});
    let resumed = repo
        .resume_external(callback_id, Some(payload.clone()), Some(88))
        .await
        .expect("resume callback");
    assert_eq!(resumed.state, JobState::Running);
    assert_eq!(resumed.callback_id, None);
    assert_eq!(resumed.metadata.get("_awa_callback_result"), Some(&payload));

    match repo
        .check_callback_state(job.id, callback_id)
        .await
        .expect("check callback state")
    {
        CallbackPollResult::Resolved(resolved) => assert_eq!(resolved, payload),
        other => panic!("expected resolved callback, got {other:?}"),
    }

    let reloaded = repo.get_job(job.id).await.expect("reload resumed job");
    assert!(reloaded.metadata.get("_awa_callback_result").is_none());
}

#[tokio::test]
async fn repository_handles_callback_failure_retry_heartbeat_cancel_and_check() {
    let queue = "seaorm_callback_lifecycle";
    let (pool, db) = setup_database(queue).await;
    let repo = JobRepository::new(&db);

    let fail_job = repo
        .insert_raw(
            "seaorm_fail_callback_job",
            serde_json::json!({}),
            opts(queue),
        )
        .await
        .expect("insert fail callback job");
    mark_running(&pool, fail_job.id, 101).await;
    let fail_callback = repo
        .register_callback(fail_job.id, 101, Duration::from_secs(60))
        .await
        .expect("register fail callback");
    repo.enter_callback_wait(fail_job.id, 101, fail_callback)
        .await
        .expect("enter fail wait");

    match repo
        .check_callback_state(fail_job.id, fail_callback)
        .await
        .expect("check pending callback")
    {
        CallbackPollResult::Pending => {}
        other => panic!("expected pending callback, got {other:?}"),
    }

    let heartbeat = repo
        .heartbeat_callback(fail_callback, Duration::from_secs(120))
        .await
        .expect("heartbeat callback");
    assert_eq!(heartbeat.state, JobState::WaitingExternal);
    assert!(heartbeat.callback_timeout_at.is_some());

    let failed = repo
        .fail_external(fail_callback, "callback failed", Some(101))
        .await
        .expect("fail callback");
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(failed.callback_id, None);
    assert!(failed.callback_timeout_at.is_none());

    let retry_job = repo
        .insert_raw(
            "seaorm_retry_callback_job",
            serde_json::json!({}),
            opts(queue),
        )
        .await
        .expect("insert retry callback job");
    mark_running(&pool, retry_job.id, 102).await;
    let retry_callback = repo
        .register_callback(retry_job.id, 102, Duration::from_secs(60))
        .await
        .expect("register retry callback");
    repo.enter_callback_wait(retry_job.id, 102, retry_callback)
        .await
        .expect("enter retry wait");

    let retried = repo
        .retry_external(retry_callback, Some(102))
        .await
        .expect("retry callback");
    assert_eq!(retried.state, JobState::Available);
    assert_eq!(retried.callback_id, None);
    assert!(retried.callback_timeout_at.is_none());

    let cancel_job = repo
        .insert_raw(
            "seaorm_cancel_callback_job",
            serde_json::json!({}),
            opts(queue),
        )
        .await
        .expect("insert cancel callback job");
    mark_running(&pool, cancel_job.id, 103).await;
    repo.register_callback(cancel_job.id, 103, Duration::from_secs(60))
        .await
        .expect("register cancel callback");

    assert!(repo
        .cancel_callback(cancel_job.id, 103)
        .await
        .expect("cancel callback"));
    let cancelled = repo
        .get_job(cancel_job.id)
        .await
        .expect("reload cancelled callback");
    assert_eq!(cancelled.state, JobState::Running);
    assert_eq!(cancelled.callback_id, None);
    assert!(cancelled.callback_timeout_at.is_none());
}
