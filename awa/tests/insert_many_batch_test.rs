//! Compatibility inserts and the opt-in batch insert function.
//!
//! A batch does its lane-independent work (job ids, unique claims, lane
//! registration) before it reserves its lanes, and holds a lane lock only for
//! the reservation, the ready-row writes and the commit. These tests cover that
//! ordering, skip-on-conflict inserts, and the shape of what lands in the
//! lanes.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test insert_many_batch_test -- --nocapture`

use awa::model::{
    insert_many, insert_many_copy_from_pool, insert_many_skip_unique_conflicts, AwaError,
    QueueStorage,
};
use awa::{InsertOpts, InsertParams, JobState, QueueStorageConfig, UniqueOpts};
use awa_testing::setup;
use sqlx::PgPool;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

/// The tests switch the storage engine of a shared database.
static ENGINE_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

const WAIT_DEADLINE: Duration = Duration::from_secs(20);

async fn queue_storage_pool() -> PgPool {
    let pool = setup::setup(8).await;
    setup::activate_queue_storage(&pool).await;
    pool
}

fn unique_queue(label: &str) -> String {
    format!(
        "batch_{label}_{}",
        &Uuid::new_v4().simple().to_string()[..12]
    )
}

fn job(queue: &str, seq: i64, unique: bool) -> InsertParams {
    InsertParams {
        kind: "insert_many_batch_job".to_string(),
        args: serde_json::json!({ "seq": seq }),
        opts: InsertOpts {
            queue: queue.to_string(),
            unique: unique.then(|| UniqueOpts {
                by_queue: true,
                by_args: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    }
}

async fn ready_lane_seqs(pool: &PgPool, queue: &str) -> Vec<(i64, i64)> {
    sqlx::query_as(
        "SELECT (args->>'seq')::bigint, lane_seq FROM awa.ready_entries
         WHERE queue = $1 ORDER BY lane_seq",
    )
    .bind(queue)
    .fetch_all(pool)
    .await
    .expect("read ready rows")
}

async fn claims_for_queue(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*)::bigint FROM awa.job_unique_claims AS claims
         WHERE claims.job_id IN (SELECT job_id FROM awa.ready_entries WHERE queue = $1)",
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("count claims")
}

fn is_unique_violation(err: &AwaError) -> bool {
    match err {
        AwaError::UniqueConflict { .. } => true,
        AwaError::Database(sqlx::Error::Database(db_err)) => {
            db_err.code().as_deref() == Some("23505")
        }
        _ => false,
    }
}

async fn lane_lock_key(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT hashtextextended(
            format('%I.%I', 'awa', awa.queue_lane_sequence_name('queue_enqueue_seq', $1, 2::smallint, 0::smallint)),
            0)",
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("derive lane lock key")
}

#[tokio::test]
async fn batch_lands_in_one_lane_in_input_order_and_is_claimable() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("order");

    let jobs: Vec<InsertParams> = (0..300).map(|seq| job(&queue, seq, seq % 2 == 0)).collect();
    let inserted = insert_many(&pool, &jobs).await.expect("insert batch");
    assert_eq!(
        inserted
            .iter()
            .map(|row| row.args["seq"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        (0..300).collect::<Vec<_>>(),
        "rows come back in input order"
    );
    assert!(inserted.iter().all(|row| row.state == JobState::Available));

    let lane = ready_lane_seqs(&pool, &queue).await;
    assert_eq!(lane.len(), 300);
    let first = lane[0].1;
    for (offset, (seq, lane_seq)) in lane.iter().enumerate() {
        assert_eq!(*seq, offset as i64, "lane_seq follows input order");
        assert_eq!(*lane_seq, first + offset as i64, "lane_seq is dense");
    }

    // The lane's segment metadata must let the claim path find every row.
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
    let mut claimed = Vec::new();
    while claimed.len() < 300 {
        let batch = store
            .claim_runtime_batch(&pool, &queue, 100, Duration::from_secs(60))
            .await
            .expect("claim");
        assert!(!batch.is_empty(), "claim path lost rows of the batch");
        claimed.extend(
            batch
                .into_iter()
                .map(|job| job.job.args["seq"].as_i64().unwrap()),
        );
    }
    assert_eq!(
        claimed,
        (0..300).collect::<Vec<_>>(),
        "claimed in lane order"
    );
}

#[tokio::test]
async fn batch_with_mixed_states_routes_each_row() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("mixed");

    let later = chrono::Utc::now() + chrono::Duration::hours(1);
    let mut jobs: Vec<InsertParams> = (0..6).map(|seq| job(&queue, seq, false)).collect();
    jobs[1].opts.run_at = Some(later);
    jobs[4].opts.run_at = Some(later);

    let inserted = insert_many(&pool, &jobs).await.expect("insert mixed batch");
    let states: Vec<JobState> = inserted.iter().map(|row| row.state).collect();
    assert_eq!(
        states,
        vec![
            JobState::Available,
            JobState::Scheduled,
            JobState::Available,
            JobState::Available,
            JobState::Scheduled,
            JobState::Available,
        ]
    );
    let ready: Vec<i64> = ready_lane_seqs(&pool, &queue)
        .await
        .into_iter()
        .map(|(seq, _)| seq)
        .collect();
    assert_eq!(ready, vec![0, 2, 3, 5]);
    let deferred: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM awa.deferred_jobs WHERE queue = $1 AND state = 'scheduled'",
    )
    .bind(&queue)
    .fetch_one(&pool)
    .await
    .expect("count deferred");
    assert_eq!(deferred, 2);
}

#[tokio::test]
async fn unique_conflict_fails_the_batch_atomically() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("atomic");

    insert_many(&pool, &[job(&queue, 1, true)])
        .await
        .expect("first insert");

    let batch = vec![
        job(&queue, 0, true),
        job(&queue, 1, true),
        job(&queue, 2, true),
    ];
    let err = insert_many(&pool, &batch)
        .await
        .expect_err("duplicate key must fail the batch");
    assert!(is_unique_violation(&err), "{err:?}");

    let landed: Vec<i64> = ready_lane_seqs(&pool, &queue)
        .await
        .into_iter()
        .map(|(seq, _)| seq)
        .collect();
    assert_eq!(landed, vec![1], "nothing from the failed batch landed");
    assert_eq!(claims_for_queue(&pool, &queue).await, 1);
}

#[tokio::test]
async fn skip_unique_conflicts_inserts_the_rest_in_order() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("skip");

    insert_many(&pool, &[job(&queue, 1, true), job(&queue, 3, true)])
        .await
        .expect("seed");

    // Keys 1 and 3 are already claimed, 4 repeats inside the batch, and the
    // last row carries no unique key at all.
    let batch = vec![
        job(&queue, 0, true),
        job(&queue, 1, true),
        job(&queue, 2, true),
        job(&queue, 3, true),
        job(&queue, 4, true),
        job(&queue, 4, true),
        job(&queue, 5, false),
    ];
    let inserted = insert_many_skip_unique_conflicts(&pool, &batch)
        .await
        .expect("skip insert");
    assert_eq!(
        inserted
            .iter()
            .map(|row| row.args["seq"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 2, 4, 5]
    );

    let lane = ready_lane_seqs(&pool, &queue).await;
    assert_eq!(
        lane.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(),
        vec![1, 3, 0, 2, 4, 5],
        "skipped rows take no lane position"
    );
    for pair in lane.windows(2) {
        assert_eq!(pair[1].1, pair[0].1 + 1, "lane_seq stays dense");
    }

    // Everything skipped: an empty result, not an error.
    let inserted = insert_many_skip_unique_conflicts(&pool, &[job(&queue, 0, true)])
        .await
        .expect("all skipped");
    assert!(inserted.is_empty());
}

#[tokio::test]
async fn copy_skips_unique_conflicts_in_one_pass() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("copy_skip");

    insert_many(&pool, &[job(&queue, 2, true)])
        .await
        .expect("seed");
    let batch: Vec<InsertParams> = (0..6).map(|seq| job(&queue, seq, true)).collect();
    let inserted = insert_many_copy_from_pool(&pool, &batch)
        .await
        .expect("copy insert");
    assert_eq!(
        inserted
            .iter()
            .map(|row| row.args["seq"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 1, 3, 4, 5]
    );
    assert_eq!(ready_lane_seqs(&pool, &queue).await.len(), 6);
}

#[tokio::test]
async fn canonical_storage_inserts_and_skips_row_by_row() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = setup::setup(8).await;
    setup::reset_to_canonical(&pool).await;
    let queue = unique_queue("canonical");

    insert_many(&pool, &[job(&queue, 1, true)])
        .await
        .expect("seed");
    let batch: Vec<InsertParams> = (0..4).map(|seq| job(&queue, seq, true)).collect();
    let err = insert_many(&pool, &batch).await.expect_err("duplicate");
    assert!(is_unique_violation(&err), "{err:?}");

    let inserted = insert_many_skip_unique_conflicts(&pool, &batch)
        .await
        .expect("skip insert");
    assert_eq!(
        inserted
            .iter()
            .map(|row| row.args["seq"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 2, 3]
    );
}

/// While another transaction holds a lane lock, a batch bound for that lane
/// has already done its lane-independent work (it holds its unique claims) and
/// has not yet written a ready row. Every row's unique claim, not just the
/// first's, is held by then.
#[tokio::test]
async fn batch_waits_for_the_lane_after_its_lane_independent_work() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("window");

    // Provision the lane so the gate lock is the only thing to wait for.
    insert_many(&pool, &[job(&queue, -1, false)])
        .await
        .expect("provision lane");

    let key = lane_lock_key(&pool, &queue).await;
    let mut gate = pool.begin().await.expect("begin gate");
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *gate)
        .await
        .expect("take lane lock");

    let jobs_for_probe: Vec<InsertParams> = (0..50).map(|seq| job(&queue, seq, true)).collect();
    let producer = {
        let pool = pool.clone();
        let jobs = jobs_for_probe.clone();
        tokio::spawn(async move { insert_many_skip_unique_conflicts(&pool, &jobs).await })
    };

    let started = Instant::now();
    let waiter = loop {
        let pid: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_locks
             WHERE locktype = 'advisory' AND NOT granted
               AND ((classid::bigint << 32) | objid::bigint) = $1
             LIMIT 1",
        )
        .bind(key)
        .fetch_optional(&pool)
        .await
        .expect("poll pg_locks");
        if let Some(pid) = pid {
            break pid;
        }
        assert!(started.elapsed() < WAIT_DEADLINE, "producer never waited");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let holds_relation_lock = |relation: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_locks
                    WHERE pid = $1 AND locktype = 'relation' AND granted
                      AND relation = $2::regclass
                )",
            )
            .bind(waiter)
            .bind(relation)
            .fetch_one(&pool)
            .await
            .expect("read relation locks")
        }
    };
    // The claim of the batch's last row is already held, uncommitted: an
    // insert of the same key waits on the producer's transaction.
    let last = jobs_for_probe.last().expect("probe job");
    let last_key =
        awa::model::prepare_raw_job_insert(last.kind.clone(), last.args.clone(), last.opts.clone())
            .expect("prepare last job")
            .unique_key()
            .expect("unique key")
            .to_vec();
    let mut probe = pool.begin().await.expect("begin probe");
    sqlx::query("SET LOCAL lock_timeout = '300ms'")
        .execute(&mut *probe)
        .await
        .expect("set lock timeout");
    let probe_result = sqlx::query(
        "INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, 0)
         ON CONFLICT (unique_key) DO NOTHING",
    )
    .bind(&last_key)
    .execute(&mut *probe)
    .await;
    match probe_result {
        Err(sqlx::Error::Database(db_err)) => assert_eq!(
            db_err.code().as_deref(),
            Some("55P03"),
            "the last row's claim must already be held while the lane lock is awaited"
        ),
        other => panic!("claim of the last row was not held while waiting for the lane: {other:?}"),
    }
    drop(probe);
    assert!(
        !holds_relation_lock("awa.ready_entries").await,
        "no ready row is written while the lane lock is awaited"
    );

    gate.commit().await.expect("release lane lock");
    let inserted = tokio::time::timeout(WAIT_DEADLINE, producer)
        .await
        .expect("producer finished")
        .expect("producer task")
        .expect("insert batch");
    assert_eq!(inserted.len(), 50);
    assert_eq!(ready_lane_seqs(&pool, &queue).await.len(), 51);
}

/// Producers on the per-row function, as binaries before v053 are, and
/// producers on the batch function share one lane at the same time. Each batch
/// still occupies a contiguous run of `lane_seq`, in input order, and the lane
/// has no duplicate or missing positions.
#[tokio::test]
async fn old_and_new_producers_share_a_lane_without_reordering() {
    const TASKS: usize = 8;
    const BATCHES: usize = 25;
    const BATCH: i64 = 40;

    let _guard = ENGINE_GUARD.lock().await;
    let pool = setup::setup(TASKS as u32 + 4).await;
    setup::activate_queue_storage(&pool).await;
    let queue = unique_queue("mixed_fleet");

    // Provision the lane before mixing generic and batch producers.
    insert_many(&pool, &[job(&queue, -1, false)])
        .await
        .expect("warm up");

    let mut tasks = Vec::new();
    for task in 0..TASKS {
        let pool = pool.clone();
        let queue = queue.clone();
        tasks.push(tokio::spawn(async move {
            for batch in 0..BATCHES {
                let jobs: Vec<InsertParams> = (0..BATCH)
                    .map(|seq| InsertParams {
                        kind: "insert_many_batch_job".to_string(),
                        args: serde_json::json!({ "seq": seq, "batch": format!("{task}-{batch}") }),
                        opts: InsertOpts {
                            queue: queue.clone(),
                            ..Default::default()
                        },
                    })
                    .collect();
                if task % 2 == 0 {
                    insert_through_row_function(&pool, &jobs).await;
                } else {
                    insert_many_skip_unique_conflicts(&pool, &jobs)
                        .await
                        .expect("batch function insert");
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("producer task");
    }

    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT args->>'batch', (args->>'seq')::bigint, lane_seq FROM awa.ready_entries
         WHERE queue = $1 AND args ? 'batch' ORDER BY lane_seq",
    )
    .bind(&queue)
    .fetch_all(&pool)
    .await
    .expect("read lane");
    assert_eq!(rows.len(), TASKS * BATCHES * BATCH as usize);
    for pair in rows.windows(2) {
        assert!(pair[1].2 > pair[0].2, "lane_seq is unique and ascending");
    }
    let mut by_batch: std::collections::HashMap<&str, Vec<(i64, i64)>> =
        std::collections::HashMap::new();
    for (batch, seq, lane_seq) in &rows {
        by_batch.entry(batch).or_default().push((*seq, *lane_seq));
    }
    for (batch, members) in by_batch {
        for (offset, (seq, lane_seq)) in members.iter().enumerate() {
            assert_eq!(*seq, offset as i64, "batch {batch} keeps input order");
            assert_eq!(
                *lane_seq,
                members[0].1 + offset as i64,
                "batch {batch} occupies a contiguous run of the lane"
            );
        }
    }
}

/// What a binary from before v053 sends: one `insert_job_compat` call per row.
async fn insert_through_row_function(pool: &PgPool, jobs: &[InsertParams]) {
    sqlx::query(
        r#"
        SELECT inserted.*
        FROM unnest($1::text[], $2::text[], $3::jsonb[]) WITH ORDINALITY AS input(kind, queue, args, ord)
        CROSS JOIN LATERAL awa.insert_job_compat(input.kind, input.queue, input.args) AS inserted
        ORDER BY input.ord
        "#,
    )
    .bind(jobs.iter().map(|job| job.kind.as_str()).collect::<Vec<_>>())
    .bind(jobs.iter().map(|job| job.opts.queue.as_str()).collect::<Vec<_>>())
    .bind(jobs.iter().map(|job| &job.args).collect::<Vec<_>>())
    .fetch_all(pool)
    .await
    .expect("per-row function insert");
}
