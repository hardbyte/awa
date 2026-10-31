//! ADR-049: one filter for listing, counting, batch operations and the DLQ,
//! and tags indexed on queue storage.

use awa::model::admin;
use awa::model::batch_operations::{self, BatchOperationSpec, SubmitBatchOperation};
use awa::model::dlq;
use awa::model::queue_storage::{PruneOutcome, QueueStorage};
use awa::model::tag_index::{self, BuildMode, TagIndexState};
use awa::{DlqPage, InsertOpts, InsertParams, JobCount, JobFilter, JobPage, JobRow, JobState};
use awa_testing::setup::TestDatabase;
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::json;
use sqlx::PgPool;
use std::collections::BTreeSet;
use std::time::Duration;

const QUEUE: &str = "job_filter";

fn tags_for(n: i64) -> Vec<String> {
    if n >= 16 {
        return Vec::new();
    }
    vec![
        if n % 3 == 0 { "batch:a" } else { "batch:b" }.to_string(),
        format!("order:{n}"),
    ]
}

fn kind_for(n: i64) -> &'static str {
    if n % 2 == 0 {
        "jf_even"
    } else {
        "jf_odd"
    }
}

/// Twenty jobs spread over every state queue storage reports, plus two
/// scheduled ones. Returns every job id.
async fn seed_queue_storage(pool: &PgPool, store: &QueueStorage) -> Vec<i64> {
    let mut params: Vec<InsertParams> = (0..20)
        .map(|n| InsertParams {
            kind: kind_for(n).into(),
            args: json!({ "n": n }),
            opts: InsertOpts {
                queue: QUEUE.into(),
                tags: tags_for(n),
                max_attempts: 3,
                ..Default::default()
            },
        })
        .collect();
    for n in [100, 101] {
        params.push(InsertParams {
            kind: kind_for(n).into(),
            args: json!({ "n": n }),
            opts: InsertOpts {
                queue: QUEUE.into(),
                tags: vec!["batch:a".into(), format!("order:{n}")],
                run_at: Some(Utc::now() + ChronoDuration::hours(1)),
                ..Default::default()
            },
        });
    }
    let inserted = awa::insert_many(pool, &params).await.unwrap();
    let ids: Vec<i64> = inserted.iter().map(|job| job.id).collect();

    let claimed = store
        .claim_runtime_batch(pool, QUEUE, 12, Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 12, "the twelve oldest jobs are claimed");
    let by_n = |n: usize| claimed.iter().find(|c| c.job.id == ids[n]).unwrap().clone();
    let complete: Vec<_> = (0..5).map(by_n).collect();
    store.complete_runtime_batch(pool, &complete).await.unwrap();
    let job = by_n(5).job;
    store
        .fail_terminal(pool, job.id, job.run_lease, "boom", None)
        .await
        .unwrap()
        .unwrap();
    let job = by_n(6).job;
    store
        .cancel_running(pool, job.id, job.run_lease, "stop", None)
        .await
        .unwrap()
        .unwrap();
    let job = by_n(7).job;
    store
        .fail_retryable(pool, job.id, job.run_lease, "again", None)
        .await
        .unwrap()
        .unwrap();
    let job = by_n(8).job;
    store
        .fail_to_dlq(pool, job.id, job.run_lease, "poison", "bad input", None)
        .await
        .unwrap()
        .unwrap();
    // Jobs 9-11 stay running; 12-19 stay available; the last two are
    // scheduled.
    ids
}

/// Every job's current row and whether it is dead-lettered, read one at a
/// time through the single-job path the filter is checked against.
async fn current_rows(pool: &PgPool, ids: &[i64]) -> Vec<(JobRow, bool)> {
    let mut rows = Vec::new();
    for &id in ids {
        let (job, dlq) = admin::get_job_with_source(pool, id).await.unwrap();
        rows.push((job, dlq.is_some()));
    }
    rows
}

fn filters() -> Vec<JobFilter> {
    let now = Utc::now();
    vec![
        JobFilter::new(),
        JobFilter::new().kind("jf_even"),
        JobFilter::new().queue(QUEUE),
        JobFilter::new().tag("batch:a"),
        JobFilter::new().tags(["batch:a", "order:3"]),
        JobFilter::new().tag("order:8"),
        JobFilter::new().state(JobState::Failed),
        JobFilter::new().states([JobState::Failed, JobState::Cancelled]),
        JobFilter::new().dead_lettered(true),
        JobFilter::new()
            .dead_lettered(false)
            .state(JobState::Failed),
        JobFilter::new().state(JobState::Running),
        JobFilter::new().state(JobState::Retryable),
        JobFilter::new().state(JobState::Scheduled).tag("batch:a"),
        JobFilter::new().state(JobState::Available).tag("batch:b"),
        JobFilter::new().state(JobState::Completed).kind("jf_odd"),
        JobFilter::new().created_at_gte(now - ChronoDuration::minutes(5)),
        JobFilter::new().created_at_lt(now - ChronoDuration::minutes(5)),
    ]
}

async fn assert_filters_agree(pool: &PgPool, ids: &[i64]) {
    let rows = current_rows(pool, ids).await;
    let mut filters = filters();
    filters.push(JobFilter::new().ids(ids.iter().copied().step_by(3)));
    for filter in filters {
        let expected: BTreeSet<i64> = rows
            .iter()
            .filter(|(job, dlq)| filter.matches(job, *dlq))
            .map(|(job, _)| job.id)
            .collect();
        let found: Vec<i64> = admin::find_jobs(pool, &filter, JobPage::first(1000))
            .await
            .unwrap()
            .iter()
            .map(|job| job.id)
            .filter(|id| ids.contains(id))
            .collect();
        assert!(
            found.windows(2).all(|pair| pair[0] > pair[1]),
            "newest first for {filter:?}: {found:?}"
        );
        assert_eq!(
            found.iter().copied().collect::<BTreeSet<_>>(),
            expected,
            "find_jobs for {filter:?}"
        );
        // Every job in this test is in the queue; scope the count to it so
        // other tests' jobs on a shared database cannot interfere.
        let scoped = JobFilter {
            queue: Some(QUEUE.into()),
            ..filter.clone()
        };
        assert_eq!(
            admin::count_jobs(pool, &scoped).await.unwrap(),
            JobCount::Exact(expected.len() as i64),
            "count_jobs for {scoped:?}"
        );
    }
}

#[tokio::test]
async fn filters_agree_with_each_jobs_current_row_on_queue_storage() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let schema = QueueStorage::active_schema(&pool).await.unwrap().unwrap();
    let store = QueueStorage::from_existing_schema(schema).unwrap();
    let ids = seed_queue_storage(&pool, &store).await;
    assert_filters_agree(&pool, &ids).await;
}

#[tokio::test]
async fn the_canonical_engine_is_refused_rather_than_searched() {
    let db = TestDatabase::canonical().await;
    let pool = db.pool().clone();
    let spec = BatchOperationSpec::SetPriority { priority: 1 };
    let submit = || {
        batch_operations::submit_batch_operation(
            &pool,
            SubmitBatchOperation {
                spec: spec.clone(),
                filter: JobFilter::new().queue(QUEUE),
                submitted_by: None,
                allow_all: false,
            },
        )
    };
    let refused = |err: awa::AwaError| {
        let message = err.to_string();
        assert!(message.contains("queue_storage"), "{message}");
    };

    // A fresh install before its first worker: nothing to find, and nothing
    // could run a batch operation.
    assert!(
        admin::find_jobs(&pool, &JobFilter::new(), JobPage::default())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        admin::count_jobs(&pool, &JobFilter::new()).await.unwrap(),
        JobCount::Exact(0)
    );
    refused(submit().await.unwrap_err());

    awa::insert_many(
        &pool,
        &[InsertParams {
            kind: "jf_canonical".into(),
            args: json!({}),
            opts: InsertOpts {
                queue: QUEUE.into(),
                ..Default::default()
            },
        }],
    )
    .await
    .unwrap();
    refused(
        admin::find_jobs(&pool, &JobFilter::new(), JobPage::default())
            .await
            .unwrap_err(),
    );
    refused(
        admin::count_jobs(&pool, &JobFilter::new())
            .await
            .unwrap_err(),
    );
    refused(
        batch_operations::preview_batch_operation(
            &pool,
            spec.clone(),
            JobFilter::new().queue(QUEUE),
        )
        .await
        .unwrap_err(),
    );
    refused(submit().await.unwrap_err());
}

#[tokio::test]
async fn a_batch_needs_a_field_that_narrows_it() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let submit = |filter: JobFilter| {
        batch_operations::submit_batch_operation(
            &pool,
            SubmitBatchOperation {
                spec: BatchOperationSpec::SetPriority { priority: 1 },
                filter,
                submitted_by: None,
                allow_all: false,
            },
        )
    };
    // Every job a batch operation can reach is not dead-lettered.
    let err = submit(JobFilter::new().dead_lettered(false))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("requires a filter"), "{err}");
    submit(JobFilter::new().queue(QUEUE).dead_lettered(false))
        .await
        .unwrap();
}

#[tokio::test]
async fn pages_cover_the_population_once_newest_first() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let schema = QueueStorage::active_schema(&pool).await.unwrap().unwrap();
    let store = QueueStorage::from_existing_schema(schema).unwrap();
    let ids = seed_queue_storage(&pool, &store).await;
    let filter = JobFilter::new().queue(QUEUE);
    let mut seen = Vec::new();
    let mut page = JobPage::first(3);
    loop {
        let jobs = admin::find_jobs(&pool, &filter, page).await.unwrap();
        let Some(last) = jobs.last() else { break };
        page = JobPage::before(last.id, 3);
        seen.extend(jobs.iter().map(|job| job.id));
    }
    let mut expected = ids.clone();
    expected.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn batch_preview_selects_the_eligible_part_of_the_population() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let schema = QueueStorage::active_schema(&pool).await.unwrap().unwrap();
    let store = QueueStorage::from_existing_schema(schema).unwrap();
    let ids = seed_queue_storage(&pool, &store).await;
    let rows = current_rows(&pool, &ids).await;
    for filter in [
        JobFilter::new().queue(QUEUE),
        JobFilter::new().queue(QUEUE).tag("batch:a"),
        JobFilter::new().queue(QUEUE).state(JobState::Scheduled),
    ] {
        let expected: BTreeSet<i64> = rows
            .iter()
            .filter(|(job, dlq)| {
                filter.matches(job, *dlq)
                    && matches!(job.state, JobState::Available | JobState::Scheduled)
            })
            .map(|(job, _)| job.id)
            .collect();
        let preview = batch_operations::preview_batch_operation(
            &pool,
            BatchOperationSpec::SetPriority { priority: 1 },
            filter.clone(),
        )
        .await
        .unwrap();
        assert_eq!(preview.total_matched, expected.len() as i64, "{filter:?}");
        assert!(preview.sample.iter().all(|job| expected.contains(&job.id)));
        let listed: BTreeSet<i64> = admin::find_jobs(&pool, &filter, JobPage::first(1000))
            .await
            .unwrap()
            .iter()
            .filter(|job| matches!(job.state, JobState::Available | JobState::Scheduled))
            .map(|job| job.id)
            .collect();
        assert_eq!(listed, expected, "{filter:?}");
    }
}

#[tokio::test]
async fn extended_filters_wait_for_the_fleet() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    sqlx::query(
        "INSERT INTO awa.runtime_instances (instance_id, hostname, pid, version, started_at, \
           last_seen_at, snapshot_interval_ms, healthy, postgres_connected, poll_loop_alive, \
           heartbeat_alive, shutting_down, leader, queues, maintenance_alive, \
           queue_descriptor_hashes, job_kind_descriptor_hashes, storage_capability, \
           transition_role) \
         VALUES (gen_random_uuid(), 'old-host', 1, '0.6.9', now(), now(), 10000, true, true, \
           true, true, false, false, '[]', true, '{}', '{}', 'queue_storage', 'auto')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let submit = |filter: JobFilter| SubmitBatchOperation {
        spec: BatchOperationSpec::SetPriority { priority: 1 },
        filter,
        submitted_by: Some("test".into()),
        allow_all: false,
    };
    let err = batch_operations::submit_batch_operation(
        &pool,
        submit(JobFilter::new().tags(["batch:a", "batch:b"])),
    )
    .await
    .expect_err("an old runner would ignore `tags`");
    assert!(err.to_string().contains("0.6.9"), "{err}");
    batch_operations::submit_batch_operation(&pool, submit(JobFilter::new().tag("batch:a")))
        .await
        .expect("one tag keeps the ADR-030 shape an old runner reads");
}

#[tokio::test]
async fn dlq_paths_share_the_filter() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let schema = QueueStorage::active_schema(&pool).await.unwrap().unwrap();
    let store = QueueStorage::from_existing_schema(schema).unwrap();
    let ids = seed_queue_storage(&pool, &store).await;
    let rows = dlq::find_dlq(&pool, &JobFilter::new().tag("order:8"), DlqPage::default())
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.job.id).collect::<Vec<_>>(),
        vec![ids[8]]
    );
    assert!(
        dlq::find_dlq(&pool, &JobFilter::new().tag("order:7"), DlqPage::default())
            .await
            .unwrap()
            .is_empty()
    );
    // The jobs listing marks the dead-lettered job, so the admin UI routes
    // actions on it through the DLQ.
    let listed = admin::find_jobs_with_source(&pool, &JobFilter::new(), JobPage::default())
        .await
        .unwrap();
    for (job, dlq) in &listed {
        assert_eq!(dlq.is_some(), job.id == ids[8], "job {}", job.id);
    }
    // Every DLQ row is a dead-lettered failed job, so none of these
    // narrows it.
    for unscoped in [
        JobFilter::new(),
        JobFilter::new().state(JobState::Failed),
        JobFilter::new().dead_lettered(true),
    ] {
        assert!(
            dlq::purge_dlq_matching(&pool, &unscoped, false)
                .await
                .is_err(),
            "{unscoped:?} needs allow_all"
        );
        assert!(
            dlq::bulk_retry_from_dlq_matching(&pool, &unscoped, false)
                .await
                .is_err(),
            "{unscoped:?} needs allow_all"
        );
    }
    assert_eq!(
        dlq::purge_dlq_matching_before(
            &pool,
            &JobFilter::new().tag("order:8"),
            None,
            Some(ids[8]),
            false
        )
        .await
        .unwrap(),
        0,
        "the cursor narrows the filter rather than replacing it"
    );
    assert_eq!(
        dlq::bulk_retry_from_dlq_matching(&pool, &JobFilter::new().tag("order:8"), false)
            .await
            .unwrap(),
        1
    );
    let (job, dlq) = admin::get_job_with_source(&pool, ids[8]).await.unwrap();
    assert!(dlq.is_none());
    assert_eq!(job.state, JobState::Available);
}

#[tokio::test]
async fn fresh_installs_index_tags_and_untagged_jobs_add_no_entries() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let statuses = tag_index::tag_index_status(&pool).await.unwrap();
    assert!(!statuses.is_empty());
    assert!(
        statuses.iter().all(|s| s.state == TagIndexState::Valid),
        "{statuses:?}"
    );

    let schema = QueueStorage::active_schema(&pool).await.unwrap().unwrap();
    let store = QueueStorage::from_existing_schema(schema.clone()).unwrap();
    seed_queue_storage(&pool, &store).await;
    // A retried job has rows in both tables, so count jobs, not rows.
    // Untagged jobs carry `"tags": []` from the compat
    // insert path and must fall outside the partial index.
    let indexed: i64 = sqlx::query_scalar(awa::audited_sql(format!(
        "SELECT count(DISTINCT job_id) FROM ( \
           SELECT job_id FROM {schema}.ready_entries WHERE (payload -> 'tags') <> '[]'::jsonb \
           UNION ALL \
           SELECT job_id FROM {schema}.deferred_jobs WHERE (payload -> 'tags') <> '[]'::jsonb \
         ) AS indexed"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        indexed, 18,
        "only the eighteen tagged jobs match the index predicate"
    );
    let untagged_ready: i64 = sqlx::query_scalar(awa::audited_sql(format!(
        "SELECT count(*) FROM {schema}.ready_entries WHERE payload -> 'tags' = '[]'::jsonb"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        untagged_ready > 0,
        "the compat insert path writes an empty tag list"
    );

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(tx.as_mut())
        .await
        .unwrap();
    let plan: Vec<String> = sqlx::query_scalar(awa::audited_sql(format!(
        "EXPLAIN SELECT job_id FROM {schema}.ready_entries AS r \
         WHERE (r.payload -> 'tags') <> '[]'::jsonb \
           AND r.payload -> 'tags' @> '[\"order:3\"]'::jsonb"
    )))
    .fetch_all(tx.as_mut())
    .await
    .unwrap();
    assert!(
        plan.iter().any(|line| line.contains("_tags_idx")),
        "{}",
        plan.join("\n")
    );
}

#[tokio::test]
async fn the_build_repairs_invalid_indexes_and_waits_for_populated_ring_partitions() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let schema = QueueStorage::active_schema(&pool).await.unwrap().unwrap();
    let store = QueueStorage::from_existing_schema(schema.clone()).unwrap();
    seed_queue_storage(&pool, &store).await;
    let populated: String = sqlx::query_scalar(awa::audited_sql(format!(
        "SELECT tableoid::regclass::text FROM {schema}.ready_entries LIMIT 1"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    let populated = populated.rsplit('.').next().unwrap().to_string();
    sqlx::query(awa::audited_sql(format!(
        "DROP INDEX {schema}.{populated}_tags_idx"
    )))
    .execute(&pool)
    .await
    .unwrap();
    // An interrupted concurrent build leaves an invalid index behind.
    sqlx::query(awa::audited_sql(format!(
        "UPDATE pg_index SET indisvalid = false \
         WHERE indexrelid = '{schema}.dlq_entries_tags_idx'::regclass"
    )))
    .execute(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    assert_eq!(
        tag_index::index_empty_partitions(&mut conn).await.unwrap(),
        0,
        "the cheap pass never builds on a populated partition"
    );
    drop(conn);

    let pass = tag_index::build_tag_indexes(&pool, BuildMode::WhenEmpty)
        .await
        .unwrap();
    assert_eq!(pass.repaired, 1);
    assert_eq!(
        pass.waiting
            .iter()
            .map(|t| t.table.as_str())
            .collect::<Vec<_>>(),
        vec![populated.as_str()]
    );
    let pass = tag_index::build_tag_indexes(&pool, BuildMode::Now)
        .await
        .unwrap();
    assert_eq!(pass.built, 1);
    assert!(pass.complete());
    assert!(tag_index::tag_index_status(&pool)
        .await
        .unwrap()
        .iter()
        .all(|s| s.state == TagIndexState::Valid));
}

#[tokio::test]
async fn a_pruned_job_drops_out_of_tag_lookups() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let schema = QueueStorage::active_schema(&pool).await.unwrap().unwrap();
    let store = QueueStorage::from_existing_schema(schema).unwrap();
    let job = awa::insert_many(
        &pool,
        &[InsertParams {
            kind: "jf_prune".into(),
            args: json!({}),
            opts: InsertOpts {
                queue: QUEUE.into(),
                tags: vec!["prune:me".into()],
                ..Default::default()
            },
        }],
    )
    .await
    .unwrap()
    .remove(0);
    let claimed = store
        .claim_runtime_batch(&pool, QUEUE, 1, Duration::from_secs(60))
        .await
        .unwrap();
    store.complete_runtime_batch(&pool, &claimed).await.unwrap();
    let tagged = JobFilter::new().tag("prune:me");
    assert_eq!(
        admin::find_jobs(&pool, &tagged, JobPage::default())
            .await
            .unwrap()
            .iter()
            .map(|j| (j.id, j.state))
            .collect::<Vec<_>>(),
        vec![(job.id, JobState::Completed)],
        "a completed job's tags live on its ready row"
    );
    let mut pruned = false;
    for _ in 0..64 {
        let _ = store.rotate(&pool).await.unwrap();
        let _ = store.rotate_leases(&pool).await.unwrap();
        let _ = store.rotate_claims(&pool).await.unwrap();
        let _ = store.prune_oldest_claims(&pool).await.unwrap();
        let _ = store.prune_oldest_leases(&pool).await.unwrap();
        if matches!(
            store.prune_oldest(&pool, Duration::ZERO).await.unwrap(),
            PruneOutcome::Pruned { .. }
        ) && admin::get_job(&pool, job.id).await.is_err()
        {
            pruned = true;
            break;
        }
    }
    assert!(pruned, "the job's ring slot was never pruned");
    assert!(admin::find_jobs(&pool, &tagged, JobPage::default())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        admin::count_jobs(&pool, &tagged).await.unwrap(),
        JobCount::Exact(0)
    );
}
