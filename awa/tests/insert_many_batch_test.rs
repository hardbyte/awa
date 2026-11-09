//! Compatibility inserts and the batch insert function from the
//! `insert_jobs_compat_batch` schema patch.
//!
//! A batch does its lane-independent work (job ids, unique claims) before it
//! registers and reserves its lanes one at a time in canonical order, and
//! holds a lane lock only for the registration, the reservation, the ready-row
//! writes and the commit. These tests cover that ordering, skip-on-conflict
//! inserts, and the shape of what lands in the lanes.
//!
//! Requires a running Postgres instance.
//! Run with: `cargo test --package awa --test insert_many_batch_test -- --nocapture`

use awa::model::{
    insert_many, insert_many_copy_from_pool, insert_many_skip_unique_conflicts, AwaError,
    QueueStorage,
};
use awa::{InsertOpts, InsertParams, JobState, QueueStorageConfig, UniqueOpts};
use awa_testing::setup;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::str::FromStr;
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

/// The producer entry points: generic `insert_many` on the per-row function,
/// and the two that use the batch function.
#[derive(Debug, Clone, Copy)]
enum Path {
    PerRow,
    SkipUniqueConflicts,
    Copy,
}

impl Path {
    const ALL: [Path; 3] = [Path::PerRow, Path::SkipUniqueConflicts, Path::Copy];

    async fn try_insert(
        self,
        pool: &PgPool,
        jobs: &[InsertParams],
    ) -> Result<Vec<awa::JobRow>, AwaError> {
        match self {
            Path::PerRow => insert_many(pool, jobs).await,
            Path::SkipUniqueConflicts => insert_many_skip_unique_conflicts(pool, jobs).await,
            Path::Copy => insert_many_copy_from_pool(pool, jobs).await,
        }
    }

    async fn insert(self, pool: &PgPool, jobs: &[InsertParams]) -> Vec<awa::JobRow> {
        self.try_insert(pool, jobs)
            .await
            .unwrap_or_else(|err| panic!("{self:?} insert: {err:?}"))
    }

    /// Ready segments a batch of `rows` ready rows in one lane, sharing a
    /// `run_at`, writes: one per row through the per-row function, one per
    /// run through the batch function.
    fn segments_for(self, rows: i64) -> i64 {
        match self {
            Path::PerRow => rows,
            Path::SkipUniqueConflicts | Path::Copy => 1,
        }
    }
}

async fn segment_count(pool: &PgPool, queue: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*)::bigint FROM awa.ready_segments WHERE queue = $1")
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("count segments")
}

#[tokio::test]
async fn batch_lands_in_one_lane_in_input_order_and_is_claimable() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    for path in Path::ALL {
        assert_batch_lands_in_one_lane_in_input_order(&pool, path).await;
    }
}

async fn assert_batch_lands_in_one_lane_in_input_order(pool: &PgPool, path: Path) {
    let queue = unique_queue("order");

    let jobs: Vec<InsertParams> = (0..300).map(|seq| job(&queue, seq, seq % 2 == 0)).collect();
    let inserted = path.insert(pool, &jobs).await;
    assert_eq!(
        inserted
            .iter()
            .map(|row| row.args["seq"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        (0..300).collect::<Vec<_>>(),
        "rows come back in input order"
    );
    assert!(inserted.iter().all(|row| row.state == JobState::Available));

    let lane = ready_lane_seqs(pool, &queue).await;
    assert_eq!(lane.len(), 300);
    let first = lane[0].1;
    for (offset, (seq, lane_seq)) in lane.iter().enumerate() {
        assert_eq!(*seq, offset as i64, "lane_seq follows input order");
        assert_eq!(*lane_seq, first + offset as i64, "lane_seq is dense");
    }
    assert_eq!(
        segment_count(pool, &queue).await,
        path.segments_for(300),
        "{path:?} wrote an unexpected number of ready segments"
    );

    // The lane's segment metadata must let the claim path find every row.
    let store = QueueStorage::new(QueueStorageConfig::default()).expect("queue storage");
    let mut claimed = Vec::new();
    while claimed.len() < 300 {
        let batch = store
            .claim_runtime_batch(pool, &queue, 100, Duration::from_secs(60))
            .await
            .expect("claim");
        assert!(
            !batch.is_empty(),
            "{path:?}: claim path lost rows of the batch"
        );
        claimed.extend(
            batch
                .into_iter()
                .map(|job| job.job.args["seq"].as_i64().unwrap()),
        );
    }
    assert_eq!(
        claimed,
        (0..300).collect::<Vec<_>>(),
        "{path:?}: claimed in lane order"
    );
}

#[tokio::test]
async fn batch_with_mixed_states_routes_each_row() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    for path in Path::ALL {
        assert_mixed_states_route_each_row(&pool, path).await;
    }
}

async fn assert_mixed_states_route_each_row(pool: &PgPool, path: Path) {
    let queue = unique_queue("mixed");

    let later = chrono::Utc::now() + chrono::Duration::hours(1);
    let mut jobs: Vec<InsertParams> = (0..6).map(|seq| job(&queue, seq, false)).collect();
    jobs[1].opts.run_at = Some(later);
    jobs[4].opts.run_at = Some(later);

    let inserted = path.insert(pool, &jobs).await;
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
        ],
        "{path:?}"
    );
    let ready: Vec<i64> = ready_lane_seqs(pool, &queue)
        .await
        .into_iter()
        .map(|(seq, _)| seq)
        .collect();
    assert_eq!(ready, vec![0, 2, 3, 5]);
    let deferred: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM awa.deferred_jobs WHERE queue = $1 AND state = 'scheduled'",
    )
    .bind(&queue)
    .fetch_one(pool)
    .await
    .expect("count deferred");
    assert_eq!(deferred, 2, "{path:?}");
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
    // The batch keeps its claims only for its first attempt's wait budget
    // (500 ms by default), so a probe that lands later shows nothing; such a
    // run is repeated rather than judged.
    for _ in 0..3 {
        if lane_window_holds_claims(&pool).await {
            return;
        }
    }
    panic!("every probe of the batch's claims landed after its first attempt's wait budget");
}

/// Returns false when the probe landed too late to judge.
async fn lane_window_holds_claims(pool: &PgPool) -> bool {
    const JUDGEABLE: Duration = Duration::from_millis(400);
    let queue = unique_queue("window");

    // Provision the lane so the gate lock is the only thing to wait for.
    insert_many(pool, &[job(&queue, -1, false)])
        .await
        .expect("provision lane");

    let key = lane_lock_key(pool, &queue).await;
    let mut gate = pool.begin().await.expect("begin gate");
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *gate)
        .await
        .expect("take lane lock");

    let jobs_for_probe: Vec<InsertParams> = (0..50).map(|seq| job(&queue, seq, true)).collect();
    let started = Instant::now();
    let producer = {
        let pool = pool.clone();
        let jobs = jobs_for_probe.clone();
        tokio::spawn(async move { insert_many_skip_unique_conflicts(&pool, &jobs).await })
    };

    let waiter = loop {
        let pid: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_locks
             WHERE locktype = 'advisory' AND NOT granted
               AND ((classid::bigint << 32) | objid::bigint) = $1
             LIMIT 1",
        )
        .bind(key)
        .fetch_optional(pool)
        .await
        .expect("poll pg_locks");
        if let Some(pid) = pid {
            break pid;
        }
        assert!(started.elapsed() < WAIT_DEADLINE, "producer never waited");
        tokio::time::sleep(Duration::from_millis(5)).await;
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
    sqlx::query("SET LOCAL lock_timeout = '20ms'")
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
    let probed_after = started.elapsed();
    match probe_result {
        Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("55P03") => {}
        Ok(_) if probed_after > JUDGEABLE => {
            probe.rollback().await.expect("roll back probe");
            gate.rollback().await.expect("release lane lock");
            tokio::time::timeout(WAIT_DEADLINE, producer)
                .await
                .expect("producer finished")
                .expect("producer task")
                .expect("insert batch");
            return false;
        }
        other => panic!(
            "claim of the last row was not held while waiting for the lane \
             ({probed_after:?} after the batch started): {other:?}"
        ),
    }
    drop(probe);
    let writes_ready_rows: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_locks
            WHERE pid = $1 AND locktype = 'relation' AND granted
              AND relation = 'awa.ready_entries'::regclass
        )",
    )
    .bind(waiter)
    .fetch_one(pool)
    .await
    .expect("read relation locks");
    assert!(
        !writes_ready_rows,
        "no ready row is written while the lane lock is awaited"
    );

    gate.commit().await.expect("release lane lock");
    let inserted = tokio::time::timeout(WAIT_DEADLINE, producer)
        .await
        .expect("producer finished")
        .expect("producer task")
        .expect("insert batch");
    assert_eq!(inserted.len(), 50);
    assert_eq!(ready_lane_seqs(pool, &queue).await.len(), 51);
    true
}

/// Producers on the per-row function, as released 0.6 binaries are, and
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

/// What a released 0.6 binary sends: one `insert_job_compat` call per row.
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

/// A per-row producer (as generic `insert_many` and released 0.6 binaries
/// insert) holds lane A and then enqueues into lane B, which nobody has used
/// yet; a batch for A and B is waiting for A meanwhile. The batch must not
/// have registered B before it holds A, or the two wait on each other.
#[tokio::test]
async fn batch_registers_a_new_lane_only_when_it_reaches_it() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    for path in [Path::SkipUniqueConflicts, Path::Copy] {
        assert_batch_does_not_hold_a_new_lane_while_waiting(&pool, path).await;
    }
}

async fn assert_batch_does_not_hold_a_new_lane_while_waiting(pool: &PgPool, path: Path) {
    let suffix = &Uuid::new_v4().simple().to_string()[..12];
    let lane_a = format!("batch_lane_a_{suffix}");
    let lane_b = format!("batch_lane_b_{suffix}");
    insert_many(pool, &[job(&lane_a, -1, false)])
        .await
        .expect("provision lane A");

    // The per-row producer's transaction holds lane A's lock from here on.
    let mut per_row = pool.begin().await.expect("begin per-row producer");
    insert_many(&mut *per_row, &[job(&lane_a, 100, false)])
        .await
        .expect("per-row insert into lane A");

    let batch_jobs = vec![job(&lane_a, 0, false), job(&lane_b, 0, false)];
    let batch = {
        let pool = pool.clone();
        tokio::spawn(async move { path.insert(&pool, &batch_jobs).await })
    };
    let key_a = lane_lock_key(pool, &lane_a).await;
    let started = Instant::now();
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_locks
             WHERE locktype = 'advisory' AND NOT granted
               AND ((classid::bigint << 32) | objid::bigint) = $1)",
        )
        .bind(key_a)
        .fetch_one(pool)
        .await
        .expect("poll pg_locks");
        if waiting {
            break;
        }
        assert!(
            started.elapsed() < WAIT_DEADLINE,
            "{path:?}: batch never waited for lane A"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let second = tokio::time::timeout(
        WAIT_DEADLINE,
        insert_many(&mut *per_row, &[job(&lane_b, 100, false)]),
    )
    .await
    .unwrap_or_else(|_| panic!("{path:?}: per-row producer blocked on lane B"));
    second.unwrap_or_else(|err| panic!("{path:?}: per-row insert into lane B: {err:?}"));
    per_row.commit().await.expect("commit per-row producer");

    let inserted = tokio::time::timeout(WAIT_DEADLINE, batch)
        .await
        .expect("batch finished")
        .expect("batch task");
    assert_eq!(inserted.len(), 2, "{path:?}");
    assert_eq!(ready_lane_seqs(pool, &lane_a).await.len(), 3, "{path:?}");
    assert_eq!(ready_lane_seqs(pool, &lane_b).await.len(), 2, "{path:?}");
}

/// Polls until some session waits for the lane lock `key`.
async fn wait_until_lane_awaited(pool: &PgPool, key: i64, what: &str) {
    let started = Instant::now();
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_locks
             WHERE locktype = 'advisory' AND NOT granted
               AND ((classid::bigint << 32) | objid::bigint) = $1)",
        )
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("poll pg_locks");
        if waiting {
            return;
        }
        assert!(started.elapsed() < WAIT_DEADLINE, "{what} never waited");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A per-row producer holds the lane and then inserts a unique key that a
/// waiting batch has claimed but not committed. The batch gives up its claims
/// within its bounded lane wait, so the per-row insert proceeds instead of
/// both transactions waiting on each other until PostgreSQL aborts one with
/// `deadlock detected`. The batch then finds the key claimed and skips it.
#[tokio::test]
async fn per_row_insert_of_a_key_a_waiting_batch_claimed_does_not_deadlock() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    for path in [Path::SkipUniqueConflicts, Path::Copy] {
        assert_batch_gives_up_claims_while_waiting_for_a_lane(&pool, &pool, path).await;
    }
}

/// The same cycle with the batch's session at a `deadlock_timeout` far above
/// the per-row producer's default of one second. The batch's wait budget is
/// capped, so it still gives way before the per-row producer's deadlock check.
#[tokio::test]
async fn batch_with_a_long_deadlock_timeout_still_gives_way_to_a_per_row_producer() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let batch_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(
            PgConnectOptions::from_str(&setup::database_url())
                .expect("parse database url")
                .options([("deadlock_timeout", "20s")]),
        )
        .await
        .expect("connect with a long deadlock_timeout");
    for path in [Path::SkipUniqueConflicts, Path::Copy] {
        assert_batch_gives_up_claims_while_waiting_for_a_lane(&pool, &batch_pool, path).await;
    }
}

async fn assert_batch_gives_up_claims_while_waiting_for_a_lane(
    pool: &PgPool,
    batch_pool: &PgPool,
    path: Path,
) {
    let queue = unique_queue("claim_cycle");
    insert_many(pool, &[job(&queue, -1, false)])
        .await
        .expect("provision lane");

    let mut per_row = pool.begin().await.expect("begin per-row producer");
    insert_many(&mut *per_row, &[job(&queue, 100, false)])
        .await
        .expect("per-row insert takes the lane");

    let batch = {
        let batch_pool = batch_pool.clone();
        let jobs = vec![job(&queue, 0, true)];
        tokio::spawn(async move { path.try_insert(&batch_pool, &jobs).await })
    };
    wait_until_lane_awaited(pool, lane_lock_key(pool, &queue).await, "batch").await;

    let per_row_insert = tokio::time::timeout(
        WAIT_DEADLINE,
        insert_many(&mut *per_row, &[job(&queue, 0, true)]),
    )
    .await
    .unwrap_or_else(|_| panic!("{path:?}: per-row insert of the claimed key never finished"));
    let inserted_by_row = per_row_insert
        .unwrap_or_else(|err| panic!("{path:?}: per-row insert of the claimed key: {err:?}"));
    assert_eq!(inserted_by_row.len(), 1, "{path:?}");
    per_row.commit().await.expect("commit per-row producer");

    let inserted_by_batch = tokio::time::timeout(WAIT_DEADLINE, batch)
        .await
        .expect("batch finished")
        .expect("batch task")
        .unwrap_or_else(|err| panic!("{path:?}: batch: {err:?}"));
    assert!(
        inserted_by_batch.is_empty(),
        "{path:?}: the batch skips the key the per-row producer committed"
    );
    let seqs: Vec<i64> = ready_lane_seqs(pool, &queue)
        .await
        .into_iter()
        .map(|(seq, _)| seq)
        .collect();
    assert_eq!(seqs, vec![-1, 100, 0], "{path:?}");
    assert_eq!(claims_for_queue(pool, &queue).await, 1, "{path:?}");
}

/// A transaction that enqueues lane B and then lane A in two statements, and
/// a batch for lanes A and B that holds A while it waits for B. The batch
/// releases A within its bounded wait instead of deadlocking, then waits for
/// B without holding anything and lands once the transaction commits.
#[tokio::test]
async fn multi_statement_producer_and_batch_in_opposite_lane_order_do_not_deadlock() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    for path in [Path::SkipUniqueConflicts, Path::Copy] {
        let suffix = &Uuid::new_v4().simple().to_string()[..12];
        let lane_a = format!("batch_order_a_{suffix}");
        let lane_b = format!("batch_order_b_{suffix}");
        insert_many(&pool, &[job(&lane_a, -1, false), job(&lane_b, -1, false)])
            .await
            .expect("provision lanes");

        let mut producer = pool.begin().await.expect("begin producer");
        insert_many(&mut *producer, &[job(&lane_b, 100, false)])
            .await
            .expect("producer takes lane B");

        let batch = {
            let pool = pool.clone();
            let jobs = vec![job(&lane_a, 0, false), job(&lane_b, 0, false)];
            tokio::spawn(async move { path.try_insert(&pool, &jobs).await })
        };
        wait_until_lane_awaited(&pool, lane_lock_key(&pool, &lane_b).await, "batch").await;

        tokio::time::timeout(
            WAIT_DEADLINE,
            insert_many(&mut *producer, &[job(&lane_a, 100, false)]),
        )
        .await
        .unwrap_or_else(|_| panic!("{path:?}: producer never got lane A"))
        .unwrap_or_else(|err| panic!("{path:?}: producer insert into lane A: {err:?}"));
        producer.commit().await.expect("commit producer");

        let inserted = tokio::time::timeout(WAIT_DEADLINE, batch)
            .await
            .expect("batch finished")
            .expect("batch task")
            .unwrap_or_else(|err| panic!("{path:?}: batch: {err:?}"));
        assert_eq!(inserted.len(), 2, "{path:?}");
        let lane_a_seqs: Vec<i64> = ready_lane_seqs(&pool, &lane_a)
            .await
            .into_iter()
            .map(|(seq, _)| seq)
            .collect();
        assert_eq!(lane_a_seqs, vec![-1, 100, 0], "{path:?}");
        let lane_b_seqs: Vec<i64> = ready_lane_seqs(&pool, &lane_b)
            .await
            .into_iter()
            .map(|(seq, _)| seq)
            .collect();
        assert_eq!(lane_b_seqs, vec![-1, 100, 0], "{path:?}");
    }
}

/// A lane held for longer than the batch's bounded wait: the batch gives its
/// unique claims back and then waits for that lane first, holding nothing, so
/// its key stays free for others while it keeps its place in the lane's queue.
/// It lands as soon as the lane is released.
#[tokio::test]
async fn batch_waits_for_a_busy_lane_without_holding_its_claims() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("busy_lane");
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

    let batch_job = job(&queue, 0, true);
    let unique_key = awa::model::prepare_raw_job_insert(
        batch_job.kind.clone(),
        batch_job.args.clone(),
        batch_job.opts.clone(),
    )
    .expect("prepare job")
    .unique_key()
    .expect("unique key")
    .to_vec();
    let batch = {
        let pool = pool.clone();
        tokio::spawn(async move { insert_many_skip_unique_conflicts(&pool, &[batch_job]).await })
    };
    wait_until_lane_awaited(&pool, key, "batch").await;

    // Past the first attempt's wait budget (500 ms) and its retry pause, the
    // batch waits for the lane with nothing claimed.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let mut probe = pool.begin().await.expect("begin probe");
    sqlx::query("SET LOCAL lock_timeout = '100ms'")
        .execute(&mut *probe)
        .await
        .expect("set lock timeout");
    sqlx::query("INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ($1, 0)")
        .bind(&unique_key)
        .execute(&mut *probe)
        .await
        .expect("the batch holds no claim while it waits for the busy lane");
    probe.rollback().await.expect("roll back probe");
    wait_until_lane_awaited(&pool, key, "batch").await;
    assert!(!batch.is_finished(), "the batch still waits for the lane");

    gate.commit().await.expect("release lane lock");
    let inserted = tokio::time::timeout(WAIT_DEADLINE, batch)
        .await
        .expect("batch finished")
        .expect("batch task")
        .expect("insert batch");
    assert_eq!(inserted.len(), 1);
    let seqs: Vec<i64> = ready_lane_seqs(&pool, &queue)
        .await
        .into_iter()
        .map(|(seq, _)| seq)
        .collect();
    assert_eq!(seqs, vec![-1, 0]);
}

/// The caller's `lock_timeout` bounds the whole batch insert: a lane held
/// throughout fails it with `lock_not_available`, as it fails a per-row
/// insert, and the batch leaves its unique claims behind.
#[tokio::test]
async fn batch_gives_up_at_the_callers_lock_timeout() {
    let _guard = ENGINE_GUARD.lock().await;
    let pool = queue_storage_pool().await;
    let queue = unique_queue("caller_timeout");
    insert_many(&pool, &[job(&queue, -1, false)])
        .await
        .expect("provision lane");

    let mut gate = pool.begin().await.expect("begin gate");
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lane_lock_key(&pool, &queue).await)
        .execute(&mut *gate)
        .await
        .expect("take lane lock");

    let mut producer = pool.begin().await.expect("begin producer");
    sqlx::query("SET LOCAL lock_timeout = '300ms'")
        .execute(&mut *producer)
        .await
        .expect("set lock timeout");
    let started = Instant::now();
    let err = insert_many_skip_unique_conflicts(&mut *producer, &[job(&queue, 0, true)])
        .await
        .expect_err("the lane stays held past the caller's lock_timeout");
    assert!(
        matches!(&err, AwaError::Database(sqlx::Error::Database(db_err))
            if db_err.code().as_deref() == Some("55P03")),
        "{err:?}"
    );
    assert!(started.elapsed() < WAIT_DEADLINE);
    producer.rollback().await.expect("roll back producer");
    gate.rollback().await.expect("release lane lock");

    let inserted = insert_many_skip_unique_conflicts(&pool, &[job(&queue, 0, true)])
        .await
        .expect("insert once the lane is free");
    assert_eq!(inserted.len(), 1, "the key was not left claimed");
}

/// Batch and per-row producers race on the same unique keys and lanes, round
/// after round. Per-row producers enqueue the last lane in one statement and
/// the others in a second, so they hold a lane while they claim keys and wait
/// for lanes that batches hold. No producer may fail with `deadlock
/// detected`, every key lands exactly once, and the rows in the lanes are
/// exactly the rows the committed producers returned.
#[tokio::test]
async fn mixed_producers_on_shared_keys_and_lanes_never_deadlock() {
    const TASKS: usize = 8;
    const ROUNDS: usize = 12;
    const KEYS: usize = 6;

    let _guard = ENGINE_GUARD.lock().await;
    let pool = setup::setup(TASKS as u32 + 4).await;
    setup::activate_queue_storage(&pool).await;
    let suffix = Uuid::new_v4().simple().to_string()[..12].to_string();
    let lanes: Vec<String> = ["a", "b", "c"]
        .iter()
        .map(|lane| format!("batch_mix_{lane}_{suffix}"))
        .collect();
    insert_many(
        &pool,
        &lanes
            .iter()
            .map(|lane| job(lane, -1, false))
            .collect::<Vec<_>>(),
    )
    .await
    .expect("provision lanes");

    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(TASKS));
    let mut tasks = Vec::new();
    for task in 0..TASKS {
        let pool = pool.clone();
        let barrier = barrier.clone();
        let lanes = lanes.clone();
        tasks.push(tokio::spawn(async move {
            let mut committed: Vec<i64> = Vec::new();
            let mut deadlocks = 0usize;
            for round in 0..ROUNDS {
                // Each task takes the round's keys in its own order.
                let mut keys: Vec<usize> = (0..KEYS).collect();
                keys.rotate_left((task * 5 + round) % KEYS);
                if (task + round) % 2 == 1 {
                    keys.reverse();
                }
                let mut jobs: Vec<InsertParams> = keys
                    .iter()
                    .map(|&key| keyed_job(&lanes, round, key))
                    .collect();
                jobs.extend((0..lanes.len()).map(|lane| filler_job(&lanes, round, task, lane)));
                barrier.wait().await;

                let outcome: Result<Vec<awa::JobRow>, AwaError> = match task % 4 {
                    0 | 1 => {
                        // Per-row producers share one statement order, last
                        // lane first and keys ascending, so they cannot
                        // deadlock with each other.
                        let last_lane = &lanes[lanes.len() - 1];
                        let mut sorted = jobs.clone();
                        sorted.sort_by_key(|job| {
                            job.args
                                .get("key")
                                .and_then(|key| key.as_u64())
                                .unwrap_or(u64::MAX)
                        });
                        let (first, second): (Vec<InsertParams>, Vec<InsertParams>) = sorted
                            .into_iter()
                            .partition(|job| &job.opts.queue == last_lane);
                        async {
                            let mut tx = pool.begin().await?;
                            let mut rows = insert_many(&mut *tx, &first).await?;
                            rows.extend(insert_many(&mut *tx, &second).await?);
                            tx.commit().await?;
                            Ok(rows)
                        }
                        .await
                    }
                    2 => insert_many_skip_unique_conflicts(&pool, &jobs).await,
                    _ => insert_many_copy_from_pool(&pool, &jobs).await,
                };
                match outcome {
                    Ok(rows) => committed.extend(rows.iter().map(|row| row.id)),
                    Err(err) if err.is_deadlock() => deadlocks += 1,
                    Err(err) if task % 4 < 2 && is_unique_violation(&err) => {}
                    Err(err) => panic!("task {task} round {round}: {err:?}"),
                }
            }
            (committed, deadlocks)
        }));
    }

    let mut committed: Vec<i64> = Vec::new();
    let mut deadlocks = 0usize;
    for task in tasks {
        let (ids, task_deadlocks) = task.await.expect("producer task");
        committed.extend(ids);
        deadlocks += task_deadlocks;
    }
    assert_eq!(deadlocks, 0, "producers failed with deadlock detected");

    let landed: Vec<(i64, serde_json::Value)> = sqlx::query_as(
        "SELECT job_id, args FROM awa.ready_entries
         WHERE queue = ANY($1) AND args ? 'round'",
    )
    .bind(&lanes)
    .fetch_all(&pool)
    .await
    .expect("read lanes");
    let mut landed_ids: Vec<i64> = landed.iter().map(|(id, _)| *id).collect();
    landed_ids.sort_unstable();
    committed.sort_unstable();
    assert_eq!(
        landed_ids, committed,
        "the lanes hold exactly the rows committed producers returned"
    );
    for round in 0..ROUNDS {
        for key in 0..KEYS {
            let copies = landed
                .iter()
                .filter(|(_, args)| {
                    args.get("round") == Some(&serde_json::json!(round))
                        && args.get("key") == Some(&serde_json::json!(key))
                })
                .count();
            assert_eq!(copies, 1, "round {round} key {key} lands exactly once");
        }
    }
}

/// Round `round`'s unique job `key`, in lane `key % lanes.len()`.
fn keyed_job(lanes: &[String], round: usize, key: usize) -> InsertParams {
    InsertParams {
        kind: "insert_many_batch_job".to_string(),
        args: serde_json::json!({ "round": round, "key": key }),
        opts: InsertOpts {
            queue: lanes[key % lanes.len()].clone(),
            unique: Some(UniqueOpts {
                by_queue: true,
                by_args: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    }
}

/// A job without a unique key that only `task` inserts.
fn filler_job(lanes: &[String], round: usize, task: usize, lane: usize) -> InsertParams {
    InsertParams {
        kind: "insert_many_batch_job".to_string(),
        args: serde_json::json!({ "round": round, "task": task, "lane": lane }),
        opts: InsertOpts {
            queue: lanes[lane].clone(),
            ..Default::default()
        },
    }
}
