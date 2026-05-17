use awa::{AwaError, InsertOpts, InsertParams, JobState, ListJobsFilter};
use awa_seaorm::{migrate, JobRepository};
use sea_orm::DatabaseConnection;
use uuid::Uuid;

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

async fn mark_failed(pool: &sqlx::PgPool, ids: &[i64]) {
    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'failed', finalized_at = now()
        WHERE id = ANY($1)
        "#,
    )
    .bind(ids.to_vec())
    .execute(pool)
    .await
    .expect("mark jobs failed");
}

#[tokio::test]
async fn repository_lists_dumps_retries_and_cancels_jobs() {
    let queue = "seaorm_lifecycle";
    let (_pool, db) = setup_database(queue).await;
    let repo = JobRepository::new(&db);

    let job = repo
        .insert_raw(
            "seaorm_lifecycle_job",
            serde_json::json!({"n": 1}),
            opts(queue),
        )
        .await
        .expect("insert lifecycle job");

    let loaded = repo.get_job(job.id).await.expect("load inserted job");
    assert_eq!(loaded.id, job.id);
    assert_eq!(loaded.unique_states, None);

    let listed = repo
        .list_jobs(&ListJobsFilter {
            queue: Some(queue.to_string()),
            limit: Some(10),
            ..Default::default()
        })
        .await
        .expect("list jobs");
    assert!(listed.iter().any(|row| row.id == job.id));

    let dump = repo.dump_job(job.id).await.expect("dump job");
    assert!(dump.summary.can_cancel);

    let cancelled = repo.cancel(job.id).await.expect("cancel job").unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);

    let retried = repo.retry(job.id).await.expect("retry job").unwrap();
    assert_eq!(retried.state, JobState::Available);

    let run = repo.dump_run(job.id, Some(0)).await.expect("dump run");
    assert_eq!(run.job_id, job.id);
}

#[tokio::test]
async fn repository_handles_bulk_and_queue_operations() {
    let queue = "seaorm_lifecycle_bulk";
    let (_pool, db) = setup_database(queue).await;
    let repo = JobRepository::new(&db);

    let first = repo
        .insert_raw("seaorm_bulk_job", serde_json::json!({"n": 1}), opts(queue))
        .await
        .expect("insert first");
    let second = repo
        .insert_raw("seaorm_bulk_job", serde_json::json!({"n": 2}), opts(queue))
        .await
        .expect("insert second");

    repo.pause_queue(queue, Some("test"))
        .await
        .expect("pause queue");
    repo.resume_queue(queue).await.expect("resume queue");

    let cancelled = repo
        .bulk_cancel(&[first.id, second.id])
        .await
        .expect("bulk cancel");
    assert_eq!(cancelled.len(), 2);

    let retried = repo
        .bulk_retry(&[first.id, second.id])
        .await
        .expect("bulk retry");
    assert_eq!(retried.len(), 2);

    let drained = repo.drain_queue(queue).await.expect("drain queue");
    assert!(drained >= 2);
}

#[tokio::test]
async fn bulk_retry_clears_callback_fields() {
    let queue = "seaorm_lifecycle_bulk_retry_callback";
    let (pool, db) = setup_database(queue).await;
    let repo = JobRepository::new(&db);

    let job = repo
        .insert_raw(
            "seaorm_bulk_retry_callback_job",
            serde_json::json!({}),
            opts(queue),
        )
        .await
        .expect("insert callback job");
    let callback_id = Uuid::new_v4();

    sqlx::query(
        r#"
        UPDATE awa.jobs
        SET state = 'waiting_external',
            callback_id = $2,
            callback_timeout_at = now() + interval '1 minute',
            callback_filter = 'payload.ok',
            callback_on_complete = 'payload.done',
            callback_on_fail = 'payload.failed',
            callback_transform = 'payload.value',
            run_lease = 99
        WHERE id = $1
        "#,
    )
    .bind(job.id)
    .bind(callback_id)
    .execute(&pool)
    .await
    .expect("mark job waiting on callback");

    let retried = repo.bulk_retry(&[job.id]).await.expect("bulk retry");
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].state, JobState::Available);
    assert_eq!(retried[0].callback_id, None);
    assert_eq!(retried[0].callback_timeout_at, None);
    assert_eq!(retried[0].callback_filter, None);
    assert_eq!(retried[0].callback_on_complete, None);
    assert_eq!(retried[0].callback_on_fail, None);
    assert_eq!(retried[0].callback_transform, None);
}

#[tokio::test]
async fn repository_insert_many_is_atomic() {
    let queue = "seaorm_lifecycle_insert_many";
    let (pool, db) = setup_database(queue).await;
    let repo = JobRepository::new(&db);

    let inserted = repo
        .insert_many(&[
            InsertParams {
                kind: "seaorm_insert_many_ok".to_string(),
                args: serde_json::json!({"n": 1}),
                opts: opts(queue),
            },
            InsertParams {
                kind: "seaorm_insert_many_ok".to_string(),
                args: serde_json::json!({"n": 2}),
                opts: opts(queue),
            },
        ])
        .await
        .expect("insert_many success");
    assert_eq!(inserted.len(), 2);
    assert!(inserted.iter().all(|row| row.state == JobState::Available));

    let err = repo
        .insert_many(&[
            InsertParams {
                kind: "seaorm_insert_many_rollback".to_string(),
                args: serde_json::json!({"n": 1}),
                opts: opts(queue),
            },
            InsertParams {
                kind: "seaorm_insert_many_rollback".to_string(),
                args: serde_json::json!({"bad": "hello\u{0000}world"}),
                opts: opts(queue),
            },
        ])
        .await
        .expect_err("invalid second row should fail insert_many");
    assert!(matches!(err, AwaError::Validation(_)));

    let rolled_back: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND kind = 'seaorm_insert_many_rollback'",
    )
    .bind(queue)
    .fetch_one(&pool)
    .await
    .expect("count rolled-back insert_many rows");
    assert_eq!(rolled_back, 0);
}

#[tokio::test]
async fn repository_handles_failed_job_batch_operations() {
    let queue = "seaorm_lifecycle_failed_batch";
    let (pool, db) = setup_database(queue).await;
    let repo = JobRepository::new(&db);
    let suffix = Uuid::new_v4().simple().to_string();
    let failed_kind = format!("seaorm_failed_kind_{suffix}");
    let other_kind = format!("seaorm_failed_other_{suffix}");
    let discard_kind = format!("seaorm_failed_discard_{suffix}");

    let first = repo
        .insert_raw(
            failed_kind.clone(),
            serde_json::json!({"n": 1}),
            opts(queue),
        )
        .await
        .expect("insert first failed batch job");
    let second = repo
        .insert_raw(
            failed_kind.clone(),
            serde_json::json!({"n": 2}),
            opts(queue),
        )
        .await
        .expect("insert second failed batch job");
    let third = repo
        .insert_raw(other_kind, serde_json::json!({"n": 3}), opts(queue))
        .await
        .expect("insert other failed batch job");
    mark_failed(&pool, &[first.id, second.id, third.id]).await;

    let by_kind = repo
        .retry_failed_by_kind(&failed_kind)
        .await
        .expect("retry failed by kind");
    assert_eq!(by_kind.len(), 2);
    assert!(by_kind.iter().all(|row| row.state == JobState::Available));
    assert!(by_kind.iter().any(|row| row.id == first.id));
    assert!(by_kind.iter().any(|row| row.id == second.id));

    let by_queue = repo
        .retry_failed_by_queue(queue)
        .await
        .expect("retry failed by queue");
    assert_eq!(by_queue.len(), 1);
    assert_eq!(by_queue[0].id, third.id);
    assert_eq!(by_queue[0].state, JobState::Available);

    let discarded = repo
        .insert_raw(discard_kind.clone(), serde_json::json!({}), opts(queue))
        .await
        .expect("insert discard failed job");
    mark_failed(&pool, &[discarded.id]).await;

    let deleted = repo
        .discard_failed(&discard_kind)
        .await
        .expect("discard failed by kind");
    assert_eq!(deleted, 1);
}
