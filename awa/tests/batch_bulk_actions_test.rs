//! Batch operations for operator bulk actions (ADR-030): retry, cancel,
//! discard, move to the DLQ, retry from the DLQ and purge the DLQ, each
//! applied a chunk per transaction.

use awa::audited_sql;
use awa::model::admin;
use awa::model::batch_operations::{
    self, BatchOperation, BatchOperationSpec, BatchOperationState, SubmitBatchOperation,
};
use awa::model::dlq;
use awa::model::queue_storage::QueueStorage;
use awa::{AwaError, InsertOpts, InsertParams, JobFilter, JobRow, JobState, UniqueOpts};
use awa_testing::setup::TestDatabase;
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::json;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

const QUEUE: &str = "bulk_actions";
const KIND: &str = "bulk_action_job";
const CHUNK: i64 = 3;

async fn setup() -> (TestDatabase, PgPool, QueueStorage) {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let schema = QueueStorage::active_schema(&pool).await.unwrap().unwrap();
    let store = QueueStorage::from_existing_schema(schema).unwrap();
    (db, pool, store)
}

fn params(n: i64, opts: InsertOpts) -> InsertParams {
    InsertParams {
        kind: KIND.into(),
        args: json!({ "n": n }),
        opts: InsertOpts {
            queue: QUEUE.into(),
            ..opts
        },
    }
}

async fn insert(pool: &PgPool, jobs: &[InsertParams]) -> Vec<i64> {
    awa::insert_many(pool, jobs)
        .await
        .unwrap()
        .into_iter()
        .map(|job| job.id)
        .collect()
}

/// Claim every available job in the queue.
async fn claim_all(pool: &PgPool, store: &QueueStorage, count: usize) -> Vec<JobRow> {
    let claimed = store
        .claim_runtime_batch(pool, QUEUE, count as i64, Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(claimed.len(), count, "every inserted job is claimed");
    claimed.into_iter().map(|claimed| claimed.job).collect()
}

/// Insert `count` jobs and fail each terminally. Returns their ids.
async fn failed_jobs(pool: &PgPool, store: &QueueStorage, from: i64, count: i64) -> Vec<i64> {
    let jobs: Vec<_> = (from..from + count)
        .map(|n| params(n, InsertOpts::default()))
        .collect();
    let ids = insert(pool, &jobs).await;
    for job in claim_all(pool, store, ids.len()).await {
        store
            .fail_terminal(pool, job.id, job.run_lease, "boom", None)
            .await
            .unwrap()
            .unwrap();
    }
    ids
}

/// Insert `count` jobs with `opts` and dead-letter each. Returns their ids.
async fn dlq_jobs(
    pool: &PgPool,
    store: &QueueStorage,
    from: i64,
    count: i64,
    opts: InsertOpts,
) -> Vec<i64> {
    let jobs: Vec<_> = (from..from + count)
        .map(|n| params(n, opts.clone()))
        .collect();
    let ids = insert(pool, &jobs).await;
    for job in claim_all(pool, store, ids.len()).await {
        store
            .fail_to_dlq(pool, job.id, job.run_lease, "poison", "bad input", None)
            .await
            .unwrap()
            .unwrap();
    }
    ids
}

async fn submit(pool: &PgPool, spec: BatchOperationSpec, filter: JobFilter) -> BatchOperation {
    batch_operations::submit_batch_operation(
        pool,
        SubmitBatchOperation {
            spec,
            filter,
            submitted_by: Some("test".into()),
            allow_all: false,
        },
    )
    .await
    .unwrap()
}

/// Run chunks until the operation is finalized, and return it.
async fn run_to_completion(pool: &PgPool, id: Uuid) -> BatchOperation {
    let runner = Uuid::new_v4();
    for _ in 0..100 {
        let outcome = batch_operations::run_one_batch_operation_chunk(pool, runner, CHUNK)
            .await
            .unwrap();
        if outcome.finalized {
            return batch_operations::get_batch_operation(pool, id)
                .await
                .unwrap();
        }
        assert!(outcome.claimed, "an open operation is claimed");
    }
    panic!("batch operation {id} did not finalize");
}

/// Each item's outcome and error.
async fn item_outcomes(pool: &PgPool, id: Uuid) -> HashMap<i64, (String, Option<String>)> {
    let rows: Vec<(i64, String, Option<String>)> = sqlx::query_as(
        "SELECT job_id, state, error FROM awa.batch_operation_items WHERE operation_id = $1",
    )
    .bind(id)
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|(job_id, state, error)| (job_id, (state, error)))
        .collect()
}

/// How many transactions recorded the operation's item outcomes.
async fn item_transactions(pool: &PgPool, id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(DISTINCT xmin::text)::bigint FROM awa.batch_operation_items \
         WHERE operation_id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn job_state(pool: &PgPool, id: i64) -> JobState {
    admin::get_job(pool, id).await.unwrap().state
}

async fn is_dead_lettered(pool: &PgPool, id: i64) -> bool {
    dlq::get_dlq_job(pool, id).await.unwrap().is_some()
}

/// The done and DLQ rows stored for a job.
async fn stored_terminal_rows(pool: &PgPool, store: &QueueStorage, id: i64) -> i64 {
    let schema = store.schema();
    sqlx::query_scalar(audited_sql(format!(
        "SELECT (SELECT count(*) FROM {schema}.done_entries WHERE job_id = $1) \
              + (SELECT count(*) FROM {schema}.dlq_entries WHERE job_id = $1)"
    )))
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn claim_ready(pool: &PgPool, store: &QueueStorage) -> Vec<i64> {
    store
        .claim_runtime_batch(pool, QUEUE, 100, Duration::from_secs(300))
        .await
        .unwrap()
        .into_iter()
        .map(|claimed| claimed.job.id)
        .collect()
}

async fn unique_claim_owner(pool: &PgPool, key: &[u8]) -> Option<i64> {
    sqlx::query_scalar("SELECT job_id FROM awa.job_unique_claims WHERE unique_key = $1")
        .bind(key)
        .fetch_optional(pool)
        .await
        .unwrap()
}

fn assert_counters(op: &BatchOperation, total: i64, processed: i64, skipped: i64, errored: i64) {
    assert_eq!(op.state, BatchOperationState::Completed, "{op:?}");
    assert_eq!(op.total_matched, Some(total), "{op:?}");
    assert_eq!(op.processed, processed, "{op:?}");
    assert_eq!(op.skipped, skipped, "{op:?}");
    assert_eq!(op.errored, errored, "{op:?}");
}

fn assert_all_processed(items: &HashMap<i64, (String, Option<String>)>, ids: &[i64]) {
    assert_eq!(items.len(), ids.len());
    for id in ids {
        assert_eq!(items[id], ("processed".to_string(), None), "job {id}");
    }
}

#[tokio::test]
async fn retry_revives_failed_and_cancelled_jobs_a_chunk_at_a_time() {
    let (_db, pool, store) = setup().await;
    let mut ids = failed_jobs(&pool, &store, 0, 5).await;
    let cancelled = insert(&pool, &[params(5, InsertOpts::default())]).await[0];
    store.cancel_job(&pool, cancelled).await.unwrap().unwrap();
    ids.push(cancelled);
    // Dead-lettered and available jobs are not retried.
    let dead = dlq_jobs(&pool, &store, 10, 1, InsertOpts::default()).await[0];
    let available = insert(&pool, &[params(20, InsertOpts::default())]).await[0];

    let preview = batch_operations::preview_batch_operation(
        &pool,
        BatchOperationSpec::Retry,
        JobFilter::new().queue(QUEUE),
    )
    .await
    .unwrap();
    assert_eq!(preview.total_matched, ids.len() as i64);

    let op = submit(
        &pool,
        BatchOperationSpec::Retry,
        JobFilter::new().queue(QUEUE),
    )
    .await;
    let op = run_to_completion(&pool, op.id).await;
    assert_counters(&op, ids.len() as i64, ids.len() as i64, 0, 0);
    assert_all_processed(&item_outcomes(&pool, op.id).await, &ids);
    // Six items in chunks of three: one transaction per chunk.
    assert_eq!(item_transactions(&pool, op.id).await, 2);
    for &id in &ids {
        let job = admin::get_job(&pool, id).await.unwrap();
        assert_eq!(job.state, JobState::Available, "job {id}");
        assert_eq!(job.attempt, 0, "job {id}");
    }
    assert!(is_dead_lettered(&pool, dead).await);
    assert_eq!(job_state(&pool, available).await, JobState::Available);

    // The revived jobs are claimable.
    let claimed = store
        .claim_runtime_batch(&pool, QUEUE, 100, Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(claimed.len(), ids.len() + 1);
}

#[tokio::test]
async fn retry_records_a_unique_conflict_as_errored_and_retries_the_rest() {
    let (_db, pool, store) = setup().await;
    let unique = InsertOpts {
        unique: Some(UniqueOpts::default()),
        ..Default::default()
    };
    // Seven failed jobs; the default unique states release a job's key when
    // it fails, so a newer job can take the fourth one's key.
    let jobs: Vec<_> = (0..7).map(|n| params(n, unique.clone())).collect();
    let ids = insert(&pool, &jobs).await;
    for job in claim_all(&pool, &store, ids.len()).await {
        store
            .fail_terminal(&pool, job.id, job.run_lease, "boom", None)
            .await
            .unwrap()
            .unwrap();
    }
    let blocked = ids[3];
    let key = admin::get_job(&pool, blocked)
        .await
        .unwrap()
        .unique_key
        .expect("unique job has a key");
    let newer = awa::insert_many(
        &pool,
        &[InsertParams {
            kind: KIND.into(),
            args: json!({ "n": 3 }),
            opts: InsertOpts {
                queue: "bulk_actions_newer".into(),
                ..unique.clone()
            },
        }],
    )
    .await
    .unwrap()
    .remove(0);
    assert_eq!(newer.unique_key.as_deref(), Some(key.as_slice()));
    assert_eq!(unique_claim_owner(&pool, &key).await, Some(newer.id));

    let op = submit(
        &pool,
        BatchOperationSpec::Retry,
        JobFilter::new().queue(QUEUE),
    )
    .await;
    let op = run_to_completion(&pool, op.id).await;
    assert_counters(&op, 7, 6, 0, 1);
    // The chunks without the conflict apply in one transaction each; the
    // chunk holding it falls back to a transaction per item.
    assert_eq!(item_transactions(&pool, op.id).await, 1 + CHUNK + 1);
    assert!(
        op.last_error
            .as_deref()
            .is_some_and(|error| error.contains(&format!("job_id={blocked}"))),
        "{op:?}"
    );

    let items = item_outcomes(&pool, op.id).await;
    for &id in &ids {
        let (state, error) = &items[&id];
        if id == blocked {
            assert_eq!(state, "errored");
            let error = error.as_deref().unwrap();
            assert!(error.contains(&format!("job_id={blocked}")), "{error}");
            assert_eq!(job_state(&pool, id).await, JobState::Failed);
        } else {
            assert_eq!((state.as_str(), error), ("processed", &None), "job {id}");
            assert_eq!(job_state(&pool, id).await, JobState::Available, "job {id}");
        }
    }
    assert_eq!(unique_claim_owner(&pool, &key).await, Some(newer.id));
    assert_eq!(job_state(&pool, newer.id).await, JobState::Available);
}

#[tokio::test]
async fn cancel_stops_waiting_scheduled_and_running_jobs() {
    let (_db, pool, store) = setup().await;
    let completed = insert(&pool, &[params(6, InsertOpts::default())]).await[0];
    let claimed = store
        .claim_runtime_batch(&pool, QUEUE, 1, Duration::from_secs(300))
        .await
        .unwrap();
    store.complete_runtime_batch(&pool, &claimed).await.unwrap();
    let running = insert(&pool, &[params(0, InsertOpts::default())]).await;
    claim_all(&pool, &store, 1).await;
    let available: Vec<i64> = insert(
        &pool,
        &(1..5)
            .map(|n| params(n, InsertOpts::default()))
            .collect::<Vec<_>>(),
    )
    .await;
    let scheduled = insert(
        &pool,
        &[params(
            5,
            InsertOpts {
                run_at: Some(Utc::now() + ChronoDuration::hours(1)),
                ..Default::default()
            },
        )],
    )
    .await;
    assert_eq!(job_state(&pool, running[0]).await, JobState::Running);
    assert_eq!(job_state(&pool, scheduled[0]).await, JobState::Scheduled);
    let ids: Vec<i64> = running
        .iter()
        .chain(&available)
        .chain(&scheduled)
        .copied()
        .collect();

    let op = submit(
        &pool,
        BatchOperationSpec::Cancel,
        JobFilter::new().queue(QUEUE),
    )
    .await;
    let op = run_to_completion(&pool, op.id).await;
    assert_counters(&op, ids.len() as i64, ids.len() as i64, 0, 0);
    assert_all_processed(&item_outcomes(&pool, op.id).await, &ids);
    for &id in &ids {
        assert_eq!(job_state(&pool, id).await, JobState::Cancelled, "job {id}");
    }
    assert_eq!(job_state(&pool, completed).await, JobState::Completed);
}

#[tokio::test]
async fn discard_deletes_failed_jobs_but_not_the_dlq() {
    let (_db, pool, store) = setup().await;
    let ids = failed_jobs(&pool, &store, 0, 7).await;
    let dead = dlq_jobs(&pool, &store, 10, 1, InsertOpts::default()).await[0];

    let op = submit(
        &pool,
        BatchOperationSpec::Discard,
        JobFilter::new().queue(QUEUE),
    )
    .await;
    let op = run_to_completion(&pool, op.id).await;
    assert_counters(&op, 7, 7, 0, 0);
    assert_all_processed(&item_outcomes(&pool, op.id).await, &ids);
    for &id in &ids {
        assert_eq!(stored_terminal_rows(&pool, &store, id).await, 0, "job {id}");
    }
    assert!(claim_ready(&pool, &store).await.is_empty());
    assert!(is_dead_lettered(&pool, dead).await);
}

#[tokio::test]
async fn move_to_dlq_dead_letters_failed_jobs_with_the_reason() {
    let (_db, pool, store) = setup().await;
    let ids = failed_jobs(&pool, &store, 0, 7).await;
    let cancelled = insert(&pool, &[params(7, InsertOpts::default())]).await[0];
    store.cancel_job(&pool, cancelled).await.unwrap().unwrap();

    let op = submit(
        &pool,
        BatchOperationSpec::MoveToDlq {
            reason: Some("triage".into()),
        },
        JobFilter::new().queue(QUEUE),
    )
    .await;
    let op = run_to_completion(&pool, op.id).await;
    assert_counters(&op, 7, 7, 0, 0);
    assert_all_processed(&item_outcomes(&pool, op.id).await, &ids);
    for &id in &ids {
        let row = dlq::get_dlq_job(&pool, id).await.unwrap().expect("in DLQ");
        assert_eq!(row.reason, "triage");
    }
    assert_eq!(job_state(&pool, cancelled).await, JobState::Cancelled);

    // Without a reason the DLQ rows say they came from a batch operation.
    let more = failed_jobs(&pool, &store, 20, 1).await;
    let op = submit(
        &pool,
        BatchOperationSpec::MoveToDlq { reason: None },
        JobFilter::new().ids(more.clone()),
    )
    .await;
    let op = run_to_completion(&pool, op.id).await;
    assert_counters(&op, 1, 1, 0, 0);
    let row = dlq::get_dlq_job(&pool, more[0]).await.unwrap().unwrap();
    assert_eq!(row.reason, "batch operation");
}

#[tokio::test]
async fn retry_from_dlq_revives_dead_lettered_jobs() {
    let (_db, pool, store) = setup().await;
    let ids = dlq_jobs(&pool, &store, 0, 7, InsertOpts::default()).await;
    // A failed job outside the DLQ is left alone.
    let failed = failed_jobs(&pool, &store, 10, 1).await[0];

    let preview = batch_operations::preview_batch_operation(
        &pool,
        BatchOperationSpec::RetryFromDlq,
        JobFilter::new().queue(QUEUE),
    )
    .await
    .unwrap();
    assert_eq!(preview.total_matched, 7);
    assert_eq!(preview.sample.len(), 7);

    let op = submit(
        &pool,
        BatchOperationSpec::RetryFromDlq,
        JobFilter::new().queue(QUEUE).dead_lettered(true),
    )
    .await;
    let op = run_to_completion(&pool, op.id).await;
    assert_counters(&op, 7, 7, 0, 0);
    assert_all_processed(&item_outcomes(&pool, op.id).await, &ids);
    for &id in &ids {
        assert!(!is_dead_lettered(&pool, id).await, "job {id}");
        assert_eq!(job_state(&pool, id).await, JobState::Available, "job {id}");
    }
    assert_eq!(job_state(&pool, failed).await, JobState::Failed);
    assert!(!is_dead_lettered(&pool, failed).await);
}

#[tokio::test]
async fn purge_dlq_deletes_rows_and_releases_their_unique_claims() {
    let (_db, pool, store) = setup().await;
    let unique = InsertOpts {
        unique: Some(UniqueOpts {
            states: UniqueOpts::default().states | (1 << JobState::Failed.bit_position()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let ids = dlq_jobs(&pool, &store, 0, 7, unique.clone()).await;
    let failed = failed_jobs(&pool, &store, 10, 1).await[0];
    let mut keys = Vec::new();
    for &id in &ids {
        let key = admin::get_job(&pool, id).await.unwrap().unique_key.unwrap();
        assert_eq!(unique_claim_owner(&pool, &key).await, Some(id));
        keys.push(key);
    }

    let op = submit(
        &pool,
        BatchOperationSpec::PurgeDlq,
        JobFilter::new().queue(QUEUE),
    )
    .await;
    let op = run_to_completion(&pool, op.id).await;
    assert_counters(&op, 7, 7, 0, 0);
    assert_all_processed(&item_outcomes(&pool, op.id).await, &ids);
    for (&id, key) in ids.iter().zip(&keys) {
        assert_eq!(stored_terminal_rows(&pool, &store, id).await, 0, "job {id}");
        assert_eq!(unique_claim_owner(&pool, key).await, None, "job {id}");
    }
    assert_eq!(job_state(&pool, failed).await, JobState::Failed);

    // A purged job's key is free for a new job.
    let again = insert(&pool, &[params(0, unique)]).await[0];
    assert_eq!(unique_claim_owner(&pool, &keys[0]).await, Some(again));
}

#[tokio::test]
async fn a_job_that_moved_on_before_its_chunk_is_skipped() {
    let (_db, pool, store) = setup().await;
    let ids = failed_jobs(&pool, &store, 0, 7).await;

    let op = submit(
        &pool,
        BatchOperationSpec::Discard,
        JobFilter::new().queue(QUEUE),
    )
    .await;
    let runner = Uuid::new_v4();
    let first = batch_operations::run_one_batch_operation_chunk(&pool, runner, CHUNK)
        .await
        .unwrap();
    assert_eq!((first.processed, first.skipped), (CHUNK, 0));
    // The last job is retried before the runner reaches it.
    let moved_on = *ids.last().unwrap();
    admin::retry(&pool, moved_on).await.unwrap().unwrap();

    let op = run_to_completion(&pool, op.id).await;
    assert_counters(&op, 7, 6, 1, 0);
    let items = item_outcomes(&pool, op.id).await;
    assert_eq!(items[&moved_on], ("skipped".to_string(), None));
    assert_eq!(job_state(&pool, moved_on).await, JobState::Available);
}

#[tokio::test]
async fn a_bulk_action_can_be_cancelled_mid_run() {
    let (_db, pool, store) = setup().await;
    let ids = failed_jobs(&pool, &store, 0, 9).await;

    let op = submit(
        &pool,
        BatchOperationSpec::Retry,
        JobFilter::new().queue(QUEUE),
    )
    .await;
    let runner = Uuid::new_v4();
    let first = batch_operations::run_one_batch_operation_chunk(&pool, runner, CHUNK)
        .await
        .unwrap();
    assert!(first.claimed && !first.finalized);
    assert_eq!(first.processed, CHUNK);

    batch_operations::request_batch_operation_cancellation(&pool, op.id)
        .await
        .unwrap();
    let next = batch_operations::run_one_batch_operation_chunk(&pool, runner, CHUNK)
        .await
        .unwrap();
    assert!(next.finalized);

    let op = batch_operations::get_batch_operation(&pool, op.id)
        .await
        .unwrap();
    assert_eq!(op.state, BatchOperationState::Cancelled);
    assert_eq!(op.total_matched, Some(9));
    assert_eq!(
        (op.processed, op.skipped, op.errored),
        (CHUNK, 9 - CHUNK, 0)
    );
    let items = item_outcomes(&pool, op.id).await;
    for (n, &id) in ids.iter().enumerate() {
        let (expected_item, expected_job) = if (n as i64) < CHUNK {
            ("processed", JobState::Available)
        } else {
            ("skipped", JobState::Failed)
        };
        assert_eq!(items[&id].0, expected_item, "job {id}");
        assert_eq!(job_state(&pool, id).await, expected_job, "job {id}");
    }
}

#[tokio::test]
async fn filters_outside_a_kinds_eligible_states_are_refused() {
    let (_db, pool, _store) = setup().await;
    let refused = |spec: BatchOperationSpec, filter: JobFilter, expected: &'static str| {
        let pool = pool.clone();
        async move {
            let err = batch_operations::submit_batch_operation(
                &pool,
                SubmitBatchOperation {
                    spec: spec.clone(),
                    filter: filter.clone(),
                    submitted_by: None,
                    allow_all: false,
                },
            )
            .await
            .unwrap_err();
            assert!(
                matches!(err, AwaError::Validation(_)) && err.to_string().contains(expected),
                "{spec:?} {filter:?}: {err}"
            );
            let err = batch_operations::preview_batch_operation(&pool, spec, filter)
                .await
                .unwrap_err();
            assert!(err.to_string().contains(expected), "{err}");
        }
    };
    let queue = || JobFilter::new().queue(QUEUE);

    refused(
        BatchOperationSpec::Retry,
        queue().state(JobState::Available),
        "retry only supports failed or cancelled state filters",
    )
    .await;
    refused(
        BatchOperationSpec::Retry,
        queue().dead_lettered(true),
        "retry does not act on dead-lettered jobs",
    )
    .await;
    refused(
        BatchOperationSpec::Cancel,
        queue().state(JobState::Failed),
        "cancel only supports available, scheduled, retryable, running or waiting_external",
    )
    .await;
    refused(
        BatchOperationSpec::Discard,
        queue().state(JobState::Cancelled),
        "discard only supports failed state filters",
    )
    .await;
    refused(
        BatchOperationSpec::Discard,
        queue().dead_lettered(true),
        "discard does not act on dead-lettered jobs",
    )
    .await;
    refused(
        BatchOperationSpec::MoveToDlq { reason: None },
        queue().dead_lettered(true),
        "move_to_dlq does not act on dead-lettered jobs",
    )
    .await;
    refused(
        BatchOperationSpec::MoveToDlq {
            reason: Some(String::new()),
        },
        queue(),
        "DLQ reason must be",
    )
    .await;
    refused(
        BatchOperationSpec::RetryFromDlq,
        queue().dead_lettered(false),
        "retry_from_dlq acts only on dead-lettered jobs",
    )
    .await;
    refused(
        BatchOperationSpec::PurgeDlq,
        queue().state(JobState::Completed),
        "purge_dlq only supports failed state filters",
    )
    .await;
    // The existing kinds keep their message.
    refused(
        BatchOperationSpec::SetPriority { priority: 1 },
        queue().dead_lettered(true),
        "set_priority only supports available or scheduled state filters",
    )
    .await;

    // A state the kind acts on is accepted.
    batch_operations::submit_batch_operation(
        &pool,
        SubmitBatchOperation {
            spec: BatchOperationSpec::Retry,
            filter: queue().state(JobState::Cancelled),
            submitted_by: None,
            allow_all: false,
        },
    )
    .await
    .unwrap();
}

#[test]
fn bulk_action_specs_use_the_op_kind_tag() {
    for (spec, json) in [
        (BatchOperationSpec::Retry, json!({ "op_kind": "retry" })),
        (BatchOperationSpec::Cancel, json!({ "op_kind": "cancel" })),
        (BatchOperationSpec::Discard, json!({ "op_kind": "discard" })),
        (
            BatchOperationSpec::MoveToDlq {
                reason: Some("why".into()),
            },
            json!({ "op_kind": "move_to_dlq", "reason": "why" }),
        ),
        (
            BatchOperationSpec::RetryFromDlq,
            json!({ "op_kind": "retry_from_dlq" }),
        ),
        (
            BatchOperationSpec::PurgeDlq,
            json!({ "op_kind": "purge_dlq" }),
        ),
    ] {
        assert_eq!(serde_json::to_value(&spec).unwrap(), json);
        assert_eq!(
            serde_json::from_value::<BatchOperationSpec>(json).unwrap(),
            spec
        );
    }
    assert_eq!(
        serde_json::from_value::<BatchOperationSpec>(json!({ "op_kind": "move_to_dlq" })).unwrap(),
        BatchOperationSpec::MoveToDlq { reason: None }
    );
}
