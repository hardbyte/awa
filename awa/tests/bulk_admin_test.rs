//! Batched admin operations on both storage engines.
//!
//! Every batched operation changes at most `batch_size` rows per call, each
//! call in its own transaction, never changes a row twice when runs overlap,
//! reports `busy` instead of passing a row another transaction holds, and
//! skips and reports rows whose change would violate a unique claim. A run
//! covers the jobs that existed when it started, and a drain reaches a job
//! that moves between tables while it runs.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test bulk_admin_test -- --nocapture`

use awa::model::bulk::{run_batches, BulkBatch, BulkCursor, BulkRunOptions};
use awa::model::{admin, dlq, insert_many, AwaError, ListDlqFilter};
use awa::{InsertOpts, InsertParams, UniqueOpts};
use awa_testing::setup;
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::sync::LazyLock;
use tokio::sync::Mutex;
use uuid::Uuid;

static GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    Canonical,
    QueueStorage,
}

async fn pool_for(engine: Engine) -> PgPool {
    let pool = setup::setup(10).await;
    match engine {
        Engine::Canonical => setup::reset_to_canonical(&pool).await,
        Engine::QueueStorage => setup::activate_queue_storage(&pool).await,
    }
    pool
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", &Uuid::new_v4().simple().to_string()[..12])
}

async fn insert_jobs(pool: &PgPool, queue: &str, kind: &str, count: usize) -> Vec<i64> {
    let params: Vec<InsertParams> = (0..count)
        .map(|seq| InsertParams {
            kind: kind.to_string(),
            args: serde_json::json!({ "seq": seq, "queue": queue }),
            opts: InsertOpts {
                queue: queue.to_string(),
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

async fn insert_unique_job(pool: &PgPool, queue: &str, kind: &str, key: &str) -> i64 {
    insert_many(
        pool,
        &[InsertParams {
            kind: kind.to_string(),
            args: serde_json::json!({ "key": key }),
            opts: InsertOpts {
                queue: queue.to_string(),
                unique: Some(UniqueOpts::default()),
                ..Default::default()
            },
        }],
    )
    .await
    .expect("insert unique job")[0]
        .id
}

/// Turn available jobs into failed terminal jobs.
async fn fail_jobs(pool: &PgPool, engine: Engine, ids: &[i64]) {
    match engine {
        Engine::Canonical => {
            sqlx::query(
                "UPDATE awa.jobs SET state = 'failed', attempt = 1, finalized_at = now() \
                 WHERE id = ANY($1)",
            )
            .bind(ids)
            .execute(pool)
            .await
            .expect("fail canonical jobs");
        }
        Engine::QueueStorage => {
            sqlx::query(
                r#"
                WITH moved AS (
                    DELETE FROM awa.ready_entries
                    WHERE job_id = ANY($1)
                    RETURNING ready_slot, ready_generation, job_id, kind, queue, args,
                              priority, attempt, run_lease, max_attempts, lane_seq, run_at,
                              attempted_at, created_at, unique_key, unique_states, payload
                ),
                released AS (
                    SELECT awa.release_queue_storage_unique_claim(
                        job_id, unique_key, unique_states, 'available'::awa.job_state
                    )
                    FROM moved
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
            sqlx::query("DELETE FROM awa.job_unique_claims WHERE job_id = ANY($1)")
                .bind(ids)
                .execute(pool)
                .await
                .expect("release claims of failed jobs");
        }
    }
}

async fn states(pool: &PgPool, ids: &[i64]) -> BTreeMap<String, i64> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT state::text, count(*)::bigint FROM awa.jobs WHERE id = ANY($1) GROUP BY state",
    )
    .bind(ids)
    .fetch_all(pool)
    .await
    .expect("job states")
    .into_iter()
    .collect()
}

fn count_of(states: &BTreeMap<String, i64>, state: &str) -> i64 {
    states.get(state).copied().unwrap_or(0)
}

/// Hold row locks on `ids` in the relation the batch picks from.
async fn hold(
    pool: &PgPool,
    relation: &str,
    id_column: &str,
    ids: &[i64],
) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut holder = pool.begin().await.expect("begin holder");
    sqlx::query(&format!(
        "SELECT 1 FROM {relation} WHERE {id_column} = ANY($1) FOR UPDATE"
    ))
    .bind(ids)
    .fetch_all(&mut *holder)
    .await
    .expect("hold rows");
    holder
}

fn failed_relation(engine: Engine) -> (&'static str, &'static str) {
    match engine {
        Engine::Canonical => ("awa.jobs_hot", "id"),
        Engine::QueueStorage => ("awa.done_entries", "job_id"),
    }
}

async fn retry_queue(pool: &PgPool, queue: &str, size: i64, cursor: BulkCursor) -> BulkBatch {
    admin::retry_failed_by_queue_batch(pool, queue, size, cursor)
        .await
        .expect("retry batch")
}

async fn retry_failed_bound_and_run(engine: Engine) {
    let _guard = GUARD.lock().await;
    let pool = pool_for(engine).await;
    let queue = unique_name("bulk_retry_failed");
    let ids = insert_jobs(&pool, &queue, "bulk_job", 5).await;
    fail_jobs(&pool, engine, &ids).await;

    let first = retry_queue(&pool, &queue, 2, BulkCursor::default()).await;
    assert_eq!(
        (first.count, first.done, first.busy),
        (2, false, false),
        "{engine:?}"
    );
    assert_eq!(first.cursor.after_job_id, ids[1]);
    let now = states(&pool, &ids).await;
    assert_eq!(count_of(&now, "available"), 2, "{engine:?} {now:?}");
    assert_eq!(count_of(&now, "failed"), 3, "{engine:?} {now:?}");

    let run = run_batches(
        BulkRunOptions::default().batch_size(2),
        |size, cursor| admin::retry_failed_by_queue_batch(&pool, &queue, size, cursor),
        |batch, _| assert!(batch.count <= 2),
    )
    .await
    .expect("run");
    assert!(run.complete);
    assert_eq!(run.count, 3, "{engine:?}");
    assert_eq!(count_of(&states(&pool, &ids).await, "available"), 5);
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn retry_failed_takes_at_most_batch_size_jobs_per_call_canonical() {
    retry_failed_bound_and_run(Engine::Canonical).await;
}

#[tokio::test]
async fn retry_failed_takes_at_most_batch_size_jobs_per_call_queue_storage() {
    retry_failed_bound_and_run(Engine::QueueStorage).await;
}

async fn retry_failed_skips_conflicts(engine: Engine) {
    let _guard = GUARD.lock().await;
    let pool = pool_for(engine).await;
    let queue = unique_name("bulk_retry_conflict");
    let key = unique_name("key");
    let before = insert_jobs(&pool, &queue, "bulk_job", 2).await;
    let keyed = insert_unique_job(&pool, &queue, "bulk_unique_job", &key).await;
    let after = insert_jobs(&pool, &queue, "bulk_job", 2).await;
    let failed: Vec<i64> = before
        .iter()
        .chain([&keyed])
        .chain(after.iter())
        .copied()
        .collect();
    fail_jobs(&pool, engine, &failed).await;
    let live = insert_unique_job(&pool, &queue, "bulk_unique_job", &key).await;
    assert_ne!(live, keyed);

    let run = run_batches(
        BulkRunOptions::default().batch_size(10),
        |size, cursor| admin::retry_failed_by_queue_batch(&pool, &queue, size, cursor),
        |_, _| {},
    )
    .await
    .expect("run");
    assert!(run.complete, "{engine:?} {run:?}");
    assert_eq!(
        (run.count, run.skipped, run.conflicts.clone()),
        (4, 1, vec![keyed])
    );
    let now = states(&pool, &failed).await;
    assert_eq!(count_of(&now, "available"), 4, "{engine:?} {now:?}");
    assert_eq!(count_of(&now, "failed"), 1, "{engine:?} {now:?}");

    let again = run_batches(
        BulkRunOptions::default(),
        |size, cursor| admin::retry_failed_by_queue_batch(&pool, &queue, size, cursor),
        |_, _| {},
    )
    .await
    .expect("second run");
    assert_eq!(
        (again.count, again.skipped),
        (0, 1),
        "a new run tries the row again"
    );
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn retry_failed_skips_and_reports_unique_conflicts_canonical() {
    retry_failed_skips_conflicts(Engine::Canonical).await;
}

#[tokio::test]
async fn retry_failed_skips_and_reports_unique_conflicts_queue_storage() {
    retry_failed_skips_conflicts(Engine::QueueStorage).await;
}

async fn retry_failed_is_busy_while_held(engine: Engine) {
    let _guard = GUARD.lock().await;
    let pool = pool_for(engine).await;
    let queue = unique_name("bulk_retry_busy");
    let ids = insert_jobs(&pool, &queue, "bulk_job", 3).await;
    fail_jobs(&pool, engine, &ids).await;
    let (relation, column) = failed_relation(engine);

    let holder = hold(&pool, relation, column, &ids[..1]).await;
    let held = retry_queue(&pool, &queue, 10, BulkCursor::default()).await;
    assert_eq!(
        (held.count, held.busy, held.done),
        (0, true, false),
        "{engine:?}"
    );
    assert_eq!((held.cursor.stage, held.cursor.after_job_id), (0, 0));
    assert!(
        held.cursor.before_job_id > ids[2],
        "the first batch records the run's high-water mark even when busy"
    );
    holder.rollback().await.expect("release");

    let released = retry_queue(&pool, &queue, 10, held.cursor).await;
    assert_eq!((released.count, released.done), (3, true), "{engine:?}");
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn retry_failed_is_busy_while_a_matching_job_is_held_canonical() {
    retry_failed_is_busy_while_held(Engine::Canonical).await;
}

#[tokio::test]
async fn retry_failed_is_busy_while_a_matching_job_is_held_queue_storage() {
    retry_failed_is_busy_while_held(Engine::QueueStorage).await;
}

async fn retry_failed_concurrent_runs(engine: Engine) {
    const JOBS: usize = 200;
    let _guard = GUARD.lock().await;
    let pool = pool_for(engine).await;
    let queue = unique_name("bulk_retry_concurrent");
    let ids = insert_jobs(&pool, &queue, "bulk_job", JOBS).await;
    fail_jobs(&pool, engine, &ids).await;

    let mut runs = Vec::new();
    for _ in 0..4 {
        let pool = pool.clone();
        let queue = queue.clone();
        runs.push(tokio::spawn(async move {
            run_batches(
                BulkRunOptions::default().batch_size(7),
                |size, cursor| admin::retry_failed_by_queue_batch(&pool, &queue, size, cursor),
                |batch, _| assert!(batch.count <= 7),
            )
            .await
            .expect("run")
        }));
    }
    let mut total = 0;
    for run in runs {
        let run = run.await.expect("run task");
        assert!(run.complete, "{run:?}");
        total += run.count;
    }
    assert_eq!(
        total, JOBS as u64,
        "{engine:?}: a job was retried twice or not at all"
    );
    assert_eq!(
        count_of(&states(&pool, &ids).await, "available"),
        JOBS as i64
    );
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_retry_failed_runs_retry_each_job_once_canonical() {
    retry_failed_concurrent_runs(Engine::Canonical).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_retry_failed_runs_retry_each_job_once_queue_storage() {
    retry_failed_concurrent_runs(Engine::QueueStorage).await;
}

async fn discard_bound_and_busy(engine: Engine) {
    let _guard = GUARD.lock().await;
    let pool = pool_for(engine).await;
    let queue = unique_name("bulk_discard");
    let kind = unique_name("bulk_discard_kind");
    let ids = insert_jobs(&pool, &queue, &kind, 5).await;
    fail_jobs(&pool, engine, &ids).await;

    let first = admin::discard_failed_batch(&pool, &kind, 2, BulkCursor::default())
        .await
        .expect("discard batch");
    assert_eq!(first.count, 2, "{engine:?}");
    assert_eq!(count_of(&states(&pool, &ids).await, "failed"), 3);

    let (relation, column) = failed_relation(engine);
    let holder = hold(&pool, relation, column, &ids[2..3]).await;
    let held = admin::discard_failed_batch(&pool, &kind, 10, first.cursor)
        .await
        .expect("held batch");
    assert_eq!((held.count, held.busy), (0, true), "{engine:?}");
    holder.rollback().await.expect("release");

    let run = run_batches(
        BulkRunOptions::default(),
        |size, cursor| admin::discard_failed_batch(&pool, &kind, size, cursor),
        |_, _| {},
    )
    .await
    .expect("run");
    assert!(run.complete);
    assert_eq!(run.count, 3, "{engine:?}");
    assert!(states(&pool, &ids).await.is_empty());
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn discard_failed_works_in_bounded_batches_canonical() {
    discard_bound_and_busy(Engine::Canonical).await;
}

#[tokio::test]
async fn discard_failed_works_in_bounded_batches_queue_storage() {
    discard_bound_and_busy(Engine::QueueStorage).await;
}

#[tokio::test]
async fn drain_cancels_in_bounded_batches_and_waits_for_held_jobs_canonical() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::Canonical).await;
    let queue = unique_name("bulk_drain");
    let ids = insert_jobs(&pool, &queue, "bulk_job", 5).await;

    let first = admin::drain_queue_batch(&pool, &queue, 2, BulkCursor::default())
        .await
        .expect("drain batch");
    assert_eq!((first.count, first.done), (2, false));
    assert_eq!(count_of(&states(&pool, &ids).await, "cancelled"), 2);

    let holder = hold(&pool, "awa.jobs_hot", "id", &ids[2..3]).await;
    let held = admin::drain_queue_batch(&pool, &queue, 10, first.cursor)
        .await
        .expect("held batch");
    assert_eq!((held.count, held.busy), (0, true));
    holder.rollback().await.expect("release");

    let run = run_batches(
        BulkRunOptions::default(),
        |size, cursor| admin::drain_queue_batch(&pool, &queue, size, cursor),
        |_, _| {},
    )
    .await
    .expect("run");
    assert!(run.complete);
    assert_eq!(run.count, 3);
    assert_eq!(count_of(&states(&pool, &ids).await, "cancelled"), 5);
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_drains_cancel_each_job_once_canonical() {
    const JOBS: usize = 200;
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::Canonical).await;
    let queue = unique_name("bulk_drain_concurrent");
    let ids = insert_jobs(&pool, &queue, "bulk_job", JOBS).await;
    let mut runs = Vec::new();
    for _ in 0..4 {
        let pool = pool.clone();
        let queue = queue.clone();
        runs.push(tokio::spawn(async move {
            run_batches(
                BulkRunOptions::default().batch_size(7),
                |size, cursor| admin::drain_queue_batch(&pool, &queue, size, cursor),
                |_, _| {},
            )
            .await
            .expect("run")
        }));
    }
    let mut total = 0;
    for run in runs {
        total += run.await.expect("run task").count;
    }
    assert_eq!(total, JOBS as u64);
    assert_eq!(
        count_of(&states(&pool, &ids).await, "cancelled"),
        JOBS as i64
    );
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn drain_on_queue_storage_takes_at_most_batch_size_jobs_per_call_and_honours_the_limit() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::QueueStorage).await;
    let queue = unique_name("bulk_drain_qs");
    let ids = insert_jobs(&pool, &queue, "bulk_job", 5).await;

    let first = admin::drain_queue_batch(&pool, &queue, 2, BulkCursor::default())
        .await
        .expect("drain");
    assert_eq!((first.count, first.done, first.busy), (2, false, false));
    assert_eq!(count_of(&states(&pool, &ids).await, "cancelled"), 2);

    let limited = run_batches(
        BulkRunOptions::default().batch_size(2).limit(Some(2)),
        |size, cursor| admin::drain_queue_batch(&pool, &queue, size, cursor),
        |batch, _| assert!(batch.count <= 2),
    )
    .await
    .expect("limited run");
    assert_eq!((limited.count, limited.complete), (2, false));
    assert_eq!(count_of(&states(&pool, &ids).await, "cancelled"), 4);

    let rest = run_batches(
        BulkRunOptions::default().batch_size(2),
        |size, cursor| admin::drain_queue_batch(&pool, &queue, size, cursor),
        |_, _| {},
    )
    .await
    .expect("run");
    assert_eq!((rest.count, rest.complete), (1, true));
    assert_eq!(count_of(&states(&pool, &ids).await, "cancelled"), 5);
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn drain_on_queue_storage_is_busy_while_a_scheduled_job_is_held() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::QueueStorage).await;
    let queue = unique_name("bulk_drain_qs_held");
    let scheduled = insert_scheduled_jobs(&pool, &queue, 3).await;
    let available = insert_jobs(&pool, &queue, "bulk_job", 2).await;

    // Hold the middle scheduled job: the pass takes the first, then stops
    // before the held one rather than passing it.
    let holder = hold(&pool, "awa.deferred_jobs", "job_id", &scheduled[1..2]).await;
    let first = admin::drain_queue_batch(&pool, &queue, 10, BulkCursor::default())
        .await
        .expect("first batch");
    assert_eq!(
        (first.count, first.busy, first.done, first.cursor.stage),
        (2, false, false, 1),
        "{first:?}"
    );
    assert!(first.cursor.after_job_id < scheduled[1], "{first:?}");
    let held = admin::drain_queue_batch(&pool, &queue, 10, first.cursor)
        .await
        .expect("held batch");
    assert_eq!(
        (held.count, held.busy, held.done),
        (0, true, false),
        "{held:?}"
    );
    holder.rollback().await.expect("release");

    let mut cursor = held.cursor;
    let mut drained = first.count;
    loop {
        let batch = admin::drain_queue_batch(&pool, &queue, 10, cursor)
            .await
            .expect("batch");
        drained += batch.count;
        cursor = batch.cursor;
        assert!(!batch.busy, "{batch:?}");
        if batch.done {
            break;
        }
    }
    let all: Vec<i64> = scheduled.iter().chain(&available).copied().collect();
    assert_eq!(drained, 5);
    assert_eq!(count_of(&states(&pool, &all).await, "cancelled"), 5);
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn queue_storage_cancel_by_id_dedupes_and_sorts_ids() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::QueueStorage).await;
    let queue = unique_name("bulk_cancel_dedupe");
    let ids = insert_jobs(&pool, &queue, "bulk_job", 3).await;
    let shuffled = vec![ids[2], ids[0], ids[2], ids[1], ids[0]];
    let outcome = admin::bulk_cancel_in_batches(&pool, &shuffled, 2)
        .await
        .expect("cancel");
    let cancelled: Vec<i64> = outcome.jobs.iter().map(|job| job.id).collect();
    assert_eq!(cancelled, ids, "each job once, in job ID order");
    setup::clean_queue(&pool, &queue).await;
}

/// Seed a DLQ row whose unique key claims only the `failed` state, with its
/// claim, as a failed job with that key leaves when it moves to the DLQ.
async fn seed_claimed_dlq_row(pool: &PgPool, queue: &str, key: &[u8], dlq_age_days: i32) -> i64 {
    let job_id: i64 = sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries \
             (job_id, kind, queue, priority, dlq_reason, original_run_lease, unique_key, unique_states, dlq_at) \
         VALUES (nextval('awa.job_id_seq'), 'bulk_job', $1, 2, 'seeded', 1, $2, '00000100', \
                 now() - make_interval(days => $3)) \
         RETURNING job_id",
    )
    .bind(queue)
    .bind(key)
    .bind(dlq_age_days)
    .fetch_one(pool)
    .await
    .expect("seed dlq row");
    sqlx::query("INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, $2)")
        .bind(key)
        .bind(job_id)
        .execute(pool)
        .await
        .expect("seed claim");
    job_id
}

async fn claim_holder(pool: &PgPool, key: &[u8]) -> Option<i64> {
    sqlx::query_scalar("SELECT job_id FROM awa.job_unique_claims WHERE unique_key = $1")
        .bind(key)
        .fetch_optional(pool)
        .await
        .expect("read claim")
}

#[tokio::test]
async fn every_dlq_delete_releases_the_rows_unique_claims() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::QueueStorage).await;
    let queue = unique_name("bulk_dlq_claims");
    let key = |name: &str| format!("{queue}-{name}").into_bytes();
    let filter = ListDlqFilter {
        queue: Some(queue.clone()),
        ..Default::default()
    };

    let batch_key = key("batch");
    let row = seed_claimed_dlq_row(&pool, &queue, &batch_key, 0).await;
    assert_eq!(claim_holder(&pool, &batch_key).await, Some(row));
    dlq::purge_dlq_batch(&pool, &filter, false, 10, BulkCursor::default())
        .await
        .expect("purge batch");
    assert_eq!(
        claim_holder(&pool, &batch_key).await,
        None,
        "purge_dlq_batch"
    );

    let all_key = key("all");
    seed_claimed_dlq_row(&pool, &queue, &all_key, 0).await;
    assert_eq!(
        dlq::purge_dlq(&pool, &filter, false).await.expect("purge"),
        1
    );
    assert_eq!(claim_holder(&pool, &all_key).await, None, "purge_dlq");

    let one_key = key("one");
    let row = seed_claimed_dlq_row(&pool, &queue, &one_key, 0).await;
    assert!(dlq::purge_dlq_job(&pool, row).await.expect("purge job"));
    assert_eq!(claim_holder(&pool, &one_key).await, None, "purge_dlq_job");

    let old_key = key("old");
    seed_claimed_dlq_row(&pool, &queue, &old_key, 30).await;
    let cleaned = dlq::cleanup_dlq(
        &pool,
        std::time::Duration::from_secs(86_400),
        100,
        Some(&queue),
    )
    .await
    .expect("cleanup");
    assert_eq!(cleaned, 1);
    assert_eq!(claim_holder(&pool, &old_key).await, None, "cleanup_dlq");

    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn dlq_count_applies_the_filters_cursor() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::QueueStorage).await;
    let queue = unique_name("bulk_dlq_count_cursor");
    let ids: Vec<i64> = sqlx::query_scalar(
        "INSERT INTO awa.dlq_entries (job_id, kind, queue, priority, dlq_reason, original_run_lease) \
         SELECT nextval('awa.job_id_seq'), 'bulk_job', $1, 2, 'seeded', 1 FROM generate_series(1, 5) \
         RETURNING job_id",
    )
    .bind(&queue)
    .fetch_all(&pool)
    .await
    .expect("seed dlq");
    let filter = ListDlqFilter {
        queue: Some(queue.clone()),
        before_id: Some(ids[3]),
        ..Default::default()
    };
    let counted = dlq::count_dlq(&pool, &filter, 100).await.expect("count");
    assert_eq!((counted.count, counted.capped), (3, false));
    setup::clean_queue(&pool, &queue).await;
}

/// How many transactions wrote the rows `ids` now have in `relation`: rows
/// written by one transaction share an `xmin`.
async fn writing_transactions(pool: &PgPool, relation: &str, column: &str, ids: &[i64]) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT count(DISTINCT xmin::text)::bigint FROM {relation} WHERE {column} = ANY($1)"
    ))
    .bind(ids)
    .fetch_one(pool)
    .await
    .expect("count writing transactions")
}

#[derive(Debug, Clone, Copy)]
enum IdsCall {
    RetryInBatches,
    CancelInBatches,
    RetryAtOnce,
    CancelAtOnce,
}

/// Run an explicit-ID bulk call over 6 jobs with a batch size of 2 and count
/// the transactions that wrote the changed rows.
async fn ids_call_transactions(engine: Engine, call: IdsCall) -> i64 {
    let _guard = GUARD.lock().await;
    let pool = pool_for(engine).await;
    let queue = unique_name("bulk_ids");
    let ids = insert_jobs(&pool, &queue, "bulk_job", 6).await;
    let retry = matches!(call, IdsCall::RetryInBatches | IdsCall::RetryAtOnce);
    if retry {
        fail_jobs(&pool, engine, &ids).await;
    }
    let changed = match call {
        IdsCall::RetryInBatches => admin::bulk_retry_in_batches(&pool, &ids, 2)
            .await
            .expect("bulk retry")
            .jobs
            .len(),
        IdsCall::CancelInBatches => admin::bulk_cancel_in_batches(&pool, &ids, 2)
            .await
            .expect("bulk cancel")
            .jobs
            .len(),
        IdsCall::RetryAtOnce => admin::bulk_retry(&pool, &ids)
            .await
            .expect("bulk retry")
            .len(),
        IdsCall::CancelAtOnce => admin::bulk_cancel(&pool, &ids)
            .await
            .expect("bulk cancel")
            .len(),
    };
    assert_eq!(changed, 6, "{engine:?} {call:?}");
    let (relation, column) = match (engine, retry) {
        (Engine::Canonical, _) => ("awa.jobs_hot", "id"),
        (Engine::QueueStorage, true) => ("awa.ready_entries", "job_id"),
        (Engine::QueueStorage, false) => ("awa.done_entries", "job_id"),
    };
    let transactions = writing_transactions(&pool, relation, column, &ids).await;
    setup::clean_queue(&pool, &queue).await;
    transactions
}

#[tokio::test]
async fn bulk_retry_in_batches_writes_one_transaction_per_chunk() {
    for engine in [Engine::Canonical, Engine::QueueStorage] {
        assert_eq!(
            ids_call_transactions(engine, IdsCall::RetryInBatches).await,
            3,
            "{engine:?}"
        );
        assert_eq!(
            ids_call_transactions(engine, IdsCall::RetryAtOnce).await,
            1,
            "{engine:?}: bulk_retry keeps its single transaction"
        );
    }
}

#[tokio::test]
async fn bulk_cancel_in_batches_writes_one_transaction_per_chunk() {
    for engine in [Engine::Canonical, Engine::QueueStorage] {
        assert_eq!(
            ids_call_transactions(engine, IdsCall::CancelInBatches).await,
            3,
            "{engine:?}"
        );
        assert_eq!(
            ids_call_transactions(engine, IdsCall::CancelAtOnce).await,
            1,
            "{engine:?}: bulk_cancel keeps its single transaction"
        );
    }
}

async fn bulk_retry_ids_skips_conflicts(engine: Engine) {
    let _guard = GUARD.lock().await;
    let pool = pool_for(engine).await;
    let queue = unique_name("bulk_ids_conflict");
    let key = unique_name("key");
    let plain = insert_jobs(&pool, &queue, "bulk_job", 2).await;
    let keyed = insert_unique_job(&pool, &queue, "bulk_unique_job", &key).await;
    let ids = vec![plain[0], keyed, plain[1]];
    fail_jobs(&pool, engine, &ids).await;
    insert_unique_job(&pool, &queue, "bulk_unique_job", &key).await;

    let outcome = admin::bulk_retry_in_batches(&pool, &ids, 10)
        .await
        .expect("bulk retry");
    let mut retried: Vec<i64> = outcome.jobs.iter().map(|job| job.id).collect();
    retried.sort_unstable();
    assert_eq!(retried, plain, "{engine:?}");
    assert_eq!(
        (outcome.skipped, outcome.conflicts.clone()),
        (1, vec![keyed])
    );
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn bulk_retry_skips_and_reports_unique_conflicts_canonical() {
    bulk_retry_ids_skips_conflicts(Engine::Canonical).await;
}

#[tokio::test]
async fn bulk_retry_skips_and_reports_unique_conflicts_queue_storage() {
    bulk_retry_ids_skips_conflicts(Engine::QueueStorage).await;
}

#[tokio::test]
async fn dlq_move_and_purge_work_in_bounded_batches() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::QueueStorage).await;
    let queue = unique_name("bulk_dlq_move");
    let ids = insert_jobs(&pool, &queue, "bulk_job", 5).await;
    fail_jobs(&pool, Engine::QueueStorage, &ids).await;

    let moved = dlq::bulk_move_failed_to_dlq_batch(
        &pool,
        None,
        Some(&queue),
        "bulk_test",
        false,
        2,
        BulkCursor::default(),
    )
    .await
    .expect("move batch");
    assert_eq!((moved.count, moved.done), (2, false));
    assert_eq!(dlq::dlq_depth(&pool, Some(&queue)).await.expect("depth"), 2);

    let holder = hold(&pool, "awa.done_entries", "job_id", &ids[2..3]).await;
    let held = dlq::bulk_move_failed_to_dlq_batch(
        &pool,
        None,
        Some(&queue),
        "bulk_test",
        false,
        10,
        moved.cursor,
    )
    .await
    .expect("held batch");
    assert_eq!((held.count, held.busy), (0, true));
    holder.rollback().await.expect("release");

    let run = run_batches(
        BulkRunOptions::default(),
        |size, cursor| {
            dlq::bulk_move_failed_to_dlq_batch(
                &pool,
                None,
                Some(&queue),
                "bulk_test",
                false,
                size,
                cursor,
            )
        },
        |_, _| {},
    )
    .await
    .expect("move run");
    assert_eq!((run.count, run.complete), (3, true));
    assert_eq!(dlq::dlq_depth(&pool, Some(&queue)).await.expect("depth"), 5);

    let filter = ListDlqFilter {
        queue: Some(queue.clone()),
        ..Default::default()
    };
    let purged = dlq::purge_dlq_batch(&pool, &filter, false, 2, BulkCursor::default())
        .await
        .expect("purge batch");
    assert_eq!(purged.count, 2);
    let holder = hold(&pool, "awa.dlq_entries", "job_id", &ids[2..3]).await;
    let held = dlq::purge_dlq_batch(&pool, &filter, false, 10, purged.cursor)
        .await
        .expect("held purge");
    assert_eq!((held.count, held.busy), (0, true));
    holder.rollback().await.expect("release");
    let run = run_batches(
        BulkRunOptions::default(),
        |size, cursor| dlq::purge_dlq_batch(&pool, &filter, false, size, cursor),
        |_, _| {},
    )
    .await
    .expect("purge run");
    assert_eq!((run.count, run.complete), (3, true));
    assert_eq!(dlq::dlq_depth(&pool, Some(&queue)).await.expect("depth"), 0);
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_dlq_purges_delete_each_row_once() {
    const ROWS: i64 = 200;
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::QueueStorage).await;
    let queue = unique_name("bulk_dlq_purge_concurrent");
    sqlx::query(
        "INSERT INTO awa.dlq_entries (job_id, kind, queue, priority, dlq_reason, original_run_lease) \
         SELECT nextval('awa.job_id_seq'), 'bulk_job', $1, 2, 'seeded', 1 FROM generate_series(1, $2)",
    )
    .bind(&queue)
    .bind(ROWS)
    .execute(&pool)
    .await
    .expect("seed dlq");
    let filter = ListDlqFilter {
        queue: Some(queue.clone()),
        ..Default::default()
    };
    let mut runs = Vec::new();
    for _ in 0..4 {
        let pool = pool.clone();
        let filter = filter.clone();
        runs.push(tokio::spawn(async move {
            run_batches(
                BulkRunOptions::default().batch_size(7),
                |size, cursor| dlq::purge_dlq_batch(&pool, &filter, false, size, cursor),
                |_, _| {},
            )
            .await
            .expect("run")
        }));
    }
    let mut total = 0;
    for run in runs {
        total += run.await.expect("run task").count;
    }
    assert_eq!(total, ROWS as u64);
    assert_eq!(dlq::dlq_depth(&pool, Some(&queue)).await.expect("depth"), 0);
}

#[tokio::test]
async fn dlq_count_is_capped() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::QueueStorage).await;
    let queue = unique_name("bulk_dlq_count");
    sqlx::query(
        "INSERT INTO awa.dlq_entries (job_id, kind, queue, priority, dlq_reason, original_run_lease) \
         SELECT nextval('awa.job_id_seq'), 'bulk_job', $1, 2, 'seeded', 1 FROM generate_series(1, 5)",
    )
    .bind(&queue)
    .execute(&pool)
    .await
    .expect("seed dlq");
    let filter = ListDlqFilter {
        queue: Some(queue.clone()),
        ..Default::default()
    };
    let exact = dlq::count_dlq(&pool, &filter, 10).await.expect("count");
    assert_eq!((exact.count, exact.capped), (5, false));
    let capped = dlq::count_dlq(&pool, &filter, 3).await.expect("count");
    assert_eq!((capped.count, capped.capped), (3, true));
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn batched_calls_reject_a_batch_size_below_one() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::Canonical).await;
    let err = admin::retry_failed_by_kind_batch(&pool, "any", 0, BulkCursor::default())
        .await
        .expect_err("zero batch size");
    assert!(matches!(err, AwaError::Validation(_)), "{err:?}");
}

/// Insert a job, fail it, and return its ID: a producer adding a matching
/// job while a retry or discard run is under way.
async fn insert_failed_job(pool: &PgPool, engine: Engine, queue: &str, kind: &str) -> i64 {
    let id = insert_jobs(pool, queue, kind, 1).await[0];
    fail_jobs(pool, engine, &[id]).await;
    id
}

async fn retry_failed_leaves_jobs_failed_during_the_run(engine: Engine) {
    let _guard = GUARD.lock().await;
    let pool = pool_for(engine).await;
    let queue = unique_name("bulk_retry_mark");
    let ids = insert_jobs(&pool, &queue, "bulk_job", 6).await;
    fail_jobs(&pool, engine, &ids).await;

    let later = std::sync::Mutex::new(Vec::new());
    let run = run_batches(
        // The limit only stops a run that chases new jobs; a correct run
        // finishes well before it. Each batch after the first adds a job.
        BulkRunOptions::default().batch_size(2).limit(Some(50)),
        |size, cursor| {
            let pool = &pool;
            let queue = &queue;
            let later = &later;
            async move {
                // From the second batch on, after the run has started.
                if cursor.before_job_id != 0 {
                    let id = insert_failed_job(pool, engine, queue, "bulk_job").await;
                    later.lock().unwrap().push(id);
                }
                admin::retry_failed_by_queue_batch(pool, queue, size, cursor).await
            }
        },
        |_, _| {},
    )
    .await
    .expect("run");
    let later = later.into_inner().unwrap();
    assert!(run.complete, "{engine:?} {run:?}");
    assert_eq!(
        run.count, 6,
        "{engine:?}: only the jobs failed at the start"
    );
    assert_eq!(count_of(&states(&pool, &ids).await, "available"), 6);
    assert_eq!(
        count_of(&states(&pool, &later).await, "failed"),
        later.len() as i64,
        "{engine:?}: jobs failed during the run stay failed"
    );
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn retry_failed_covers_the_jobs_failed_when_it_started_canonical() {
    retry_failed_leaves_jobs_failed_during_the_run(Engine::Canonical).await;
}

#[tokio::test]
async fn retry_failed_covers_the_jobs_failed_when_it_started_queue_storage() {
    retry_failed_leaves_jobs_failed_during_the_run(Engine::QueueStorage).await;
}

async fn discard_leaves_jobs_failed_during_the_run(engine: Engine) {
    let _guard = GUARD.lock().await;
    let pool = pool_for(engine).await;
    let queue = unique_name("bulk_discard_mark");
    let kind = unique_name("bulk_discard_mark_kind");
    let ids = insert_jobs(&pool, &queue, &kind, 5).await;
    fail_jobs(&pool, engine, &ids).await;

    let later = std::sync::Mutex::new(Vec::new());
    let run = run_batches(
        BulkRunOptions::default().batch_size(2).limit(Some(50)),
        |size, cursor| {
            let (pool, queue, kind, later) = (&pool, &queue, &kind, &later);
            async move {
                if cursor.before_job_id != 0 {
                    let id = insert_failed_job(pool, engine, queue, kind).await;
                    later.lock().unwrap().push(id);
                }
                admin::discard_failed_batch(pool, kind, size, cursor).await
            }
        },
        |_, _| {},
    )
    .await
    .expect("run");
    let later = later.into_inner().unwrap();
    assert!(run.complete, "{engine:?} {run:?}");
    assert_eq!(run.count, 5, "{engine:?}");
    assert!(states(&pool, &ids).await.is_empty());
    assert_eq!(
        count_of(&states(&pool, &later).await, "failed"),
        later.len() as i64
    );
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn discard_covers_the_jobs_failed_when_it_started_canonical() {
    discard_leaves_jobs_failed_during_the_run(Engine::Canonical).await;
}

#[tokio::test]
async fn discard_covers_the_jobs_failed_when_it_started_queue_storage() {
    discard_leaves_jobs_failed_during_the_run(Engine::QueueStorage).await;
}

async fn drain_leaves_jobs_enqueued_during_the_run(engine: Engine) {
    let _guard = GUARD.lock().await;
    let pool = pool_for(engine).await;
    let queue = unique_name("bulk_drain_mark");
    let ids = insert_jobs(&pool, &queue, "bulk_job", 6).await;

    let later = std::sync::Mutex::new(Vec::new());
    let run = run_batches(
        BulkRunOptions::default().batch_size(2).limit(Some(50)),
        |size, cursor| {
            let (pool, queue, later) = (&pool, &queue, &later);
            async move {
                // Two new jobs per batch of two: a producer the drain could
                // never catch up with.
                if cursor.before_job_id != 0 {
                    let ids = insert_jobs(pool, queue, "bulk_job", 2).await;
                    later.lock().unwrap().extend(ids);
                }
                admin::drain_queue_batch(pool, queue, size, cursor).await
            }
        },
        |_, _| {},
    )
    .await
    .expect("run");
    let later = later.into_inner().unwrap();
    assert!(run.complete, "{run:?}");
    assert_eq!(run.count, 6);
    assert_eq!(count_of(&states(&pool, &ids).await, "cancelled"), 6);
    assert_eq!(
        count_of(&states(&pool, &later).await, "available"),
        later.len() as i64,
        "{engine:?}: jobs enqueued during the drain are not cancelled"
    );
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn drain_finishes_and_leaves_jobs_enqueued_during_the_run_canonical() {
    drain_leaves_jobs_enqueued_during_the_run(Engine::Canonical).await;
}

#[tokio::test]
async fn drain_finishes_and_leaves_jobs_enqueued_during_the_run_queue_storage() {
    drain_leaves_jobs_enqueued_during_the_run(Engine::QueueStorage).await;
}

/// Insert jobs into `queue` that run an hour from now: scheduled jobs.
async fn insert_scheduled_jobs(pool: &PgPool, queue: &str, count: usize) -> Vec<i64> {
    let params: Vec<InsertParams> = (0..count)
        .map(|seq| InsertParams {
            kind: "bulk_job".to_string(),
            args: serde_json::json!({ "seq": seq, "scheduled": true }),
            opts: InsertOpts {
                queue: queue.to_string(),
                run_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
                ..Default::default()
            },
        })
        .collect();
    insert_many(pool, &params)
        .await
        .expect("insert scheduled jobs")
        .into_iter()
        .map(|job| job.id)
        .collect()
}

#[tokio::test]
async fn drain_reaches_jobs_that_move_between_tables_during_the_run_canonical() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::Canonical).await;
    let queue = unique_name("bulk_drain_moves");
    let waiting = insert_jobs(&pool, &queue, "bulk_job", 2).await;
    sqlx::query(
        "UPDATE awa.jobs SET state = 'waiting_external', callback_id = gen_random_uuid(), \
             attempt = 1, attempted_at = now() \
         WHERE id = ANY($1)",
    )
    .bind(&waiting)
    .execute(&pool)
    .await
    .expect("park jobs on a callback");
    let scheduled = insert_scheduled_jobs(&pool, &queue, 2).await;
    let available = insert_jobs(&pool, &queue, "bulk_job", 1).await;
    let in_scheduled = |ids: Vec<i64>| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM awa.scheduled_jobs WHERE id = ANY($1)",
            )
            .bind(ids)
            .fetch_one(&pool)
            .await
            .expect("count scheduled rows")
        }
    };

    // The first batch cancels one waiting job. The other times out into
    // retryable, moving from jobs_hot to scheduled_jobs, before the run has
    // passed it.
    let first = admin::drain_queue_batch(&pool, &queue, 1, BulkCursor::default())
        .await
        .expect("first batch");
    assert_eq!((first.count, first.cursor.stage), (1, 0));
    sqlx::query(
        "UPDATE awa.jobs SET state = 'retryable', callback_id = NULL, \
             run_at = now() + interval '1 hour' \
         WHERE id = $1",
    )
    .bind(waiting[1])
    .execute(&pool)
    .await
    .expect("time the callback out");
    assert_eq!(in_scheduled(vec![waiting[1]]).await, 1);

    // The scheduled pass cancels one job; the other is promoted to
    // available, moving back to jobs_hot, before the run has passed it.
    let mut cursor = first.cursor;
    let mut drained = first.count;
    loop {
        let batch = admin::drain_queue_batch(&pool, &queue, 1, cursor)
            .await
            .expect("batch");
        drained += batch.count;
        cursor = batch.cursor;
        if cursor.stage == 1 && cursor.after_job_id == scheduled[0] {
            break;
        }
        assert!(!batch.done, "the run ended before the scheduled pass");
    }
    sqlx::query("UPDATE awa.jobs SET state = 'available', run_at = now() WHERE id = $1")
        .bind(scheduled[1])
        .execute(&pool)
        .await
        .expect("promote the job");
    assert_eq!(in_scheduled(vec![scheduled[1]]).await, 0);

    loop {
        let batch = admin::drain_queue_batch(&pool, &queue, 1, cursor)
            .await
            .expect("batch");
        drained += batch.count;
        cursor = batch.cursor;
        if batch.done {
            break;
        }
    }
    let all: Vec<i64> = waiting
        .iter()
        .chain(&scheduled)
        .chain(&available)
        .copied()
        .collect();
    assert_eq!(
        count_of(&states(&pool, &all).await, "cancelled"),
        5,
        "{:?}",
        states(&pool, &all).await
    );
    assert_eq!(drained, 5);
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn dlq_purge_leaves_rows_added_during_the_run() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::QueueStorage).await;
    let queue = unique_name("bulk_dlq_purge_mark");
    let seed = |rows: i64| {
        let (pool, queue) = (pool.clone(), queue.clone());
        async move {
            sqlx::query_scalar::<_, i64>(
                "INSERT INTO awa.dlq_entries (job_id, kind, queue, priority, dlq_reason, original_run_lease) \
                 SELECT nextval('awa.job_id_seq'), 'bulk_job', $1, 2, 'seeded', 1 \
                 FROM generate_series(1, $2) RETURNING job_id",
            )
            .bind(&queue)
            .bind(rows)
            .fetch_all(&pool)
            .await
            .expect("seed dlq")
        }
    };
    seed(6).await;
    let filter = ListDlqFilter {
        queue: Some(queue.clone()),
        ..Default::default()
    };
    let later = std::sync::Mutex::new(Vec::new());
    let run = run_batches(
        BulkRunOptions::default().batch_size(2).limit(Some(50)),
        |size, cursor| {
            let (seed, filter, later, pool) = (&seed, &filter, &later, &pool);
            async move {
                if cursor.before_job_id != 0 {
                    let added = seed(2).await;
                    later.lock().unwrap().extend(added);
                }
                dlq::purge_dlq_batch(pool, filter, false, size, cursor).await
            }
        },
        |_, _| {},
    )
    .await
    .expect("run");
    let later = later.into_inner().unwrap();
    assert!(run.complete, "{run:?}");
    assert_eq!(run.count, 6);
    assert_eq!(
        dlq::dlq_depth(&pool, Some(&queue)).await.expect("depth"),
        later.len() as i64
    );
    setup::clean_queue(&pool, &queue).await;
}

/// Add a row with an explicit job ID far above the job ID sequence: an
/// available canonical job in `queue`, or a queue-storage DLQ row, the kind
/// of row a migration from canonical storage leaves above the sequence.
async fn insert_explicit_id_row(pool: &PgPool, engine: Engine, queue: &str) -> i64 {
    match engine {
        Engine::Canonical => sqlx::query_scalar(
            "INSERT INTO awa.jobs (id, kind, queue, args) \
             VALUES (nextval('awa.jobs_id_seq') + 1000000000, 'bulk_job', $1, '{}') \
             RETURNING id",
        )
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("insert explicit-ID job"),
        Engine::QueueStorage => sqlx::query_scalar(
            "INSERT INTO awa.dlq_entries (
                 job_id, kind, queue, priority, dlq_reason, original_run_lease
             )
             VALUES (nextval('awa.job_id_seq') + 1000000000, 'bulk_job', $1, 2, 'seeded', 1)
             RETURNING job_id",
        )
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("insert explicit-ID DLQ row"),
    }
}

/// A row with an explicit ID above the sequence widens the run's paging
/// bound, but a job enqueued after the run started still is not drained.
async fn drain_leaves_new_jobs_beside_an_explicit_id(engine: Engine) {
    let _guard = GUARD.lock().await;
    let pool = pool_for(engine).await;
    let queue = unique_name("bulk_drain_explicit");
    let ids = insert_jobs(&pool, &queue, "bulk_job", 2).await;
    let explicit = insert_explicit_id_row(&pool, engine, &queue).await;

    let first = admin::drain_queue_batch(&pool, &queue, 1, BulkCursor::default())
        .await
        .expect("first batch");
    assert_eq!(first.count, 1, "{engine:?}");
    assert!(
        first.cursor.before_job_id > explicit,
        "{engine:?} {first:?}"
    );
    let later = insert_jobs(&pool, &queue, "bulk_job", 1).await;

    let mut cursor = first.cursor;
    for _ in 0..20 {
        let batch = admin::drain_queue_batch(&pool, &queue, 1, cursor)
            .await
            .expect("resumed batch");
        cursor = batch.cursor;
        if batch.done {
            break;
        }
    }
    assert_eq!(
        count_of(&states(&pool, &ids).await, "cancelled"),
        2,
        "{engine:?}"
    );
    assert_eq!(
        count_of(&states(&pool, &later).await, "available"),
        1,
        "{engine:?}: a job enqueued after the drain started is not cancelled"
    );
    if engine == Engine::Canonical {
        assert_eq!(
            count_of(&states(&pool, &[explicit]).await, "cancelled"),
            1,
            "the explicit-ID job existed when the drain started"
        );
    }
    let delete = match engine {
        Engine::Canonical => "DELETE FROM awa.jobs WHERE id = $1",
        Engine::QueueStorage => "DELETE FROM awa.dlq_entries WHERE job_id = $1",
    };
    sqlx::query(delete)
        .bind(explicit)
        .execute(&pool)
        .await
        .expect("delete the explicit-ID row");
    setup::clean_queue(&pool, &queue).await;
}

#[tokio::test]
async fn drain_leaves_new_jobs_beside_an_explicit_id_canonical() {
    drain_leaves_new_jobs_beside_an_explicit_id(Engine::Canonical).await;
}

#[tokio::test]
async fn drain_leaves_new_jobs_beside_an_explicit_id_queue_storage() {
    drain_leaves_new_jobs_beside_an_explicit_id(Engine::QueueStorage).await;
}

/// Make every claim-cursor move of `queue`'s lanes fail, by wrapping
/// `awa.set_sequence_next`, until [`restore_claim_cursor_moves`].
async fn break_claim_cursor_moves(pool: &PgPool, queue: &str) {
    let names: Vec<String> =
        sqlx::query_scalar("SELECT seq_name FROM awa.queue_claim_heads WHERE queue = $1")
            .bind(queue)
            .fetch_all(pool)
            .await
            .expect("claim sequence names");
    assert!(!names.is_empty(), "the queue has a lane");
    let names = names
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut tx = pool.begin().await.expect("begin");
    sqlx::query(
        "ALTER FUNCTION awa.set_sequence_next(text, bigint) RENAME TO set_sequence_next_unbroken",
    )
    .execute(&mut *tx)
    .await
    .expect("rename set_sequence_next");
    sqlx::query(&format!(
        r#"
        CREATE FUNCTION awa.set_sequence_next(p_seq_name TEXT, p_next BIGINT)
        RETURNS VOID LANGUAGE plpgsql AS $$
        BEGIN
            IF p_seq_name IN ({names}) THEN
                RAISE EXCEPTION 'claim cursor move refused by the test';
            END IF;
            PERFORM awa.set_sequence_next_unbroken(p_seq_name, p_next);
        END
        $$
        "#
    ))
    .execute(&mut *tx)
    .await
    .expect("wrap set_sequence_next");
    tx.commit().await.expect("commit");
}

async fn restore_claim_cursor_moves(pool: &PgPool) {
    let mut tx = pool.begin().await.expect("begin");
    sqlx::query("DROP FUNCTION awa.set_sequence_next(text, bigint)")
        .execute(&mut *tx)
        .await
        .expect("drop wrapper");
    sqlx::query(
        "ALTER FUNCTION awa.set_sequence_next_unbroken(text, bigint) RENAME TO set_sequence_next",
    )
    .execute(&mut *tx)
    .await
    .expect("restore set_sequence_next");
    tx.commit().await.expect("commit");
}

/// A queue-storage drain moves the claim cursors over the rows it cancelled
/// after it commits them. When that move fails, the drain still reports the
/// committed cancellations instead of an error that would hide them.
#[tokio::test]
async fn drain_reports_committed_cancellations_when_moving_claim_cursors_fails() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::QueueStorage).await;
    let queue = unique_name("bulk_drain_cursor_move");

    let ids = insert_jobs(&pool, &queue, "bulk_job", 3).await;
    break_claim_cursor_moves(&pool, &queue).await;
    let batch = admin::drain_queue_batch(&pool, &queue, 10, BulkCursor::default()).await;
    restore_claim_cursor_moves(&pool).await;
    let batch = batch.expect("drain_queue_batch reports the committed batch");
    assert_eq!((batch.count, batch.done), (3, true));
    assert_eq!(count_of(&states(&pool, &ids).await, "cancelled"), 3);

    let ids = insert_jobs(&pool, &queue, "bulk_job", 2).await;
    break_claim_cursor_moves(&pool, &queue).await;
    let drained = admin::drain_queue(&pool, &queue).await;
    restore_claim_cursor_moves(&pool).await;
    assert_eq!(drained.expect("drain_queue reports the committed count"), 2);
    assert_eq!(count_of(&states(&pool, &ids).await, "cancelled"), 2);

    setup::clean_queue(&pool, &queue).await;
}

/// Make taking failed done rows of `kind` out of `awa.done_entries` raise a
/// unique violation, as reviving a job whose unique claim cannot be restored
/// does, until [`restore_done_deletes`].
async fn refuse_done_deletes_of(pool: &PgPool, kind: &str) {
    sqlx::query(&format!(
        r#"
        CREATE FUNCTION awa.test_refuse_done_delete() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF OLD.kind = '{kind}' THEN
                RAISE unique_violation USING MESSAGE = 'retry refused by the test';
            END IF;
            RETURN OLD;
        END
        $$
        "#
    ))
    .execute(pool)
    .await
    .expect("create refusing function");
    sqlx::query(
        "CREATE TRIGGER test_refuse_done_delete BEFORE DELETE ON awa.done_entries \
         FOR EACH ROW EXECUTE FUNCTION awa.test_refuse_done_delete()",
    )
    .execute(pool)
    .await
    .expect("create refusing trigger");
}

async fn restore_done_deletes(pool: &PgPool) {
    sqlx::query("DROP TRIGGER IF EXISTS test_refuse_done_delete ON awa.done_entries")
        .execute(pool)
        .await
        .expect("drop refusing trigger");
    sqlx::query("DROP FUNCTION IF EXISTS awa.test_refuse_done_delete()")
        .execute(pool)
        .await
        .expect("drop refusing function");
}

/// A job whose retry is refused with a unique conflict while it is taken out
/// of its done row, before its key is claimed, is skipped and reported, and
/// the batch's other jobs are still retried.
#[tokio::test]
async fn bounded_retry_skips_a_job_refused_while_taken() {
    let _guard = GUARD.lock().await;
    let pool = pool_for(Engine::QueueStorage).await;
    let queue = unique_name("bulk_retry_refused_take");
    let mut ids = insert_jobs(&pool, &queue, "bulk_job", 2).await;
    let refused = insert_jobs(&pool, &queue, "bulk_refused_job", 1).await;
    ids.extend(&refused);
    fail_jobs(&pool, Engine::QueueStorage, &ids).await;

    refuse_done_deletes_of(&pool, "bulk_refused_job").await;
    let by_queue =
        admin::retry_failed_by_queue_batch(&pool, &queue, 10, BulkCursor::default()).await;
    let by_ids = admin::bulk_retry_in_batches(&pool, &refused, 10).await;
    restore_done_deletes(&pool).await;

    let batch = by_queue.expect("retry_failed_batch skips the refused job");
    assert_eq!(
        (batch.count, batch.skipped, batch.conflicts.clone()),
        (2, 1, refused.clone())
    );
    let outcome = by_ids.expect("retry_jobs_in_batches skips the refused job");
    assert_eq!(
        (
            outcome.jobs.len(),
            outcome.skipped,
            outcome.conflicts.clone()
        ),
        (0, 1, refused.clone())
    );
    let now = states(&pool, &ids).await;
    assert_eq!(
        (count_of(&now, "available"), count_of(&now, "failed")),
        (2, 1),
        "{now:?}"
    );
    setup::clean_queue(&pool, &queue).await;
}
