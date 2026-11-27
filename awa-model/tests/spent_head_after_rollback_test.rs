//! #813 end to end: a rolled-back insert ahead of a cancelled, deleted, moved
//! or reprioritised job must not stop the queue from claiming.
//!
//! Each case follows the issue's reproduction through the public entry
//! points: claim a job so the lane's claim cursor sits past it, roll back an
//! insert on the same lane, spend the next job before it is claimed, then
//! enqueue a live job on that lane and one on a lower priority of the same
//! queue. Both must be claimed, and at most one claim call may come back
//! empty: the one that moves the lane's cursor over the spent run.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use awa_model::{
    admin, audited_sql, insert, migrations, storage, InsertOpts, InsertParams, QueueStorage,
    QueueStorageConfig,
};
use sqlx::PgPool;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const SCHEMA: &str = "spent_head";

/// The spent prefix before v061, which stops at the first gap.
const OLD_PREFIX: &str = "                    selected_with_spent.lane_seq
                        - row_number() OVER (ORDER BY selected_with_spent.lane_seq)::bigint AS prefix_group
                FROM selected_with_spent
                WHERE attempt_spent
            )
            SELECT max(spent_prefix.lane_seq) + 1
            INTO v_spent_next_seq
            FROM spent_prefix
            WHERE prefix_group = v_lane_claim_seq - 1;";

#[derive(Clone, Copy, Debug)]
enum Spend {
    Cancel,
    Delete,
    Move,
    Reprioritise,
}

#[derive(Clone, Copy, Debug)]
enum Layout {
    /// A rolled-back insert, then a spent job.
    RollbackSpent,
    /// A spent job, a rolled-back insert, then another spent job.
    SpentRollbackSpent,
}

async fn setup(pool: &PgPool) -> Result<QueueStorage, Box<dyn std::error::Error>> {
    migrations::run(pool).await?;
    let store = QueueStorage::new(QueueStorageConfig {
        schema: SCHEMA.to_string(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })?;
    store.prepare_schema(pool).await?;
    store.reset(pool).await?;
    storage::prepare(
        pool,
        "queue_storage",
        serde_json::json!({ "schema": SCHEMA }),
    )
    .await?;
    let gate = uuid::Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO awa.runtime_instances (
            instance_id, hostname, pid, version, storage_capability, transition_role,
            started_at, last_seen_at, snapshot_interval_ms, healthy, postgres_connected,
            poll_loop_alive, heartbeat_alive, maintenance_alive, shutting_down, leader,
            global_max_workers, queues, queue_descriptor_hashes, job_kind_descriptor_hashes)
        VALUES ($1, 'spent-head-test', 1, 'test', 'queue_storage', 'queue_storage_target',
            now(), now(), 10000, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE, TRUE, NULL,
            '[]'::jsonb, '{}'::jsonb, '{}'::jsonb)"#,
    )
    .bind(gate)
    .execute(pool)
    .await?;
    storage::enter_mixed_transition(pool).await?;
    storage::finalize(pool).await?;
    sqlx::query("DELETE FROM awa.runtime_instances WHERE instance_id = $1")
        .bind(gate)
        .execute(pool)
        .await?;
    Ok(store)
}

fn job(queue: &str, priority: i16, label: &str) -> InsertParams {
    InsertParams {
        kind: "spent_head".into(),
        args: serde_json::json!({ "label": label }),
        opts: InsertOpts {
            queue: queue.to_string(),
            priority,
            ..Default::default()
        },
    }
}

async fn enqueue(
    pool: &PgPool,
    queue: &str,
    priority: i16,
    label: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let rows = insert::insert_many(pool, &[job(queue, priority, label)]).await?;
    Ok(rows[0].id)
}

async fn roll_back_insert(pool: &PgPool, queue: &str) -> TestResult {
    let mut tx = pool.begin().await?;
    insert::insert_many(&mut *tx, &[job(queue, 2, "rolled back")]).await?;
    tx.rollback().await?;
    Ok(())
}

async fn spend(pool: &PgPool, store: &QueueStorage, queue: &str, how: Spend) -> TestResult {
    let id = enqueue(pool, queue, 2, "spent").await?;
    match how {
        Spend::Cancel => {
            admin::cancel(pool, id)
                .await?
                .ok_or("cancel found no job")?;
        }
        Spend::Delete => {
            let deleted = sqlx::query("DELETE FROM awa.jobs WHERE id = $1")
                .bind(id)
                .execute(pool)
                .await?
                .rows_affected();
            if deleted != 1 {
                return Err(format!("deleted {deleted} rows").into());
            }
        }
        Spend::Move => {
            if !store
                .move_queue(pool, id, &format!("{queue}_elsewhere"), None)
                .await?
            {
                return Err("move found no job".into());
            }
        }
        Spend::Reprioritise => {
            if !store.set_priority(pool, id, 3).await? {
                return Err("set_priority found no job".into());
            }
        }
    }
    Ok(())
}

/// Builds the case on a fresh queue and returns how many claim calls came
/// back empty before the live job and the lower-priority one were both
/// claimed, or `None` if eight calls did not claim them.
async fn run_case(
    pool: &PgPool,
    store: &QueueStorage,
    queue: &str,
    how: Spend,
    layout: Layout,
) -> Result<Option<usize>, Box<dyn std::error::Error>> {
    let first = enqueue(pool, queue, 2, "first").await?;
    let claimed = store
        .claim_runtime_batch(pool, queue, 1, Duration::from_secs(60))
        .await?;
    if claimed.iter().map(|c| c.job.id).collect::<Vec<_>>() != vec![first] {
        return Err("the first job was not claimed".into());
    }

    match layout {
        Layout::RollbackSpent => {
            roll_back_insert(pool, queue).await?;
            spend(pool, store, queue, how).await?;
        }
        Layout::SpentRollbackSpent => {
            spend(pool, store, queue, how).await?;
            roll_back_insert(pool, queue).await?;
            spend(pool, store, queue, how).await?;
        }
    }
    let live = enqueue(pool, queue, 2, "live").await?;
    let lower = enqueue(pool, queue, 4, "lower priority").await?;

    let mut waiting = vec![live, lower];
    let mut empty = 0;
    for _ in 0..8 {
        let claimed = store
            .claim_runtime_batch(pool, queue, 10, Duration::from_secs(60))
            .await?;
        if claimed.is_empty() {
            empty += 1;
        }
        waiting.retain(|id| !claimed.iter().any(|c| c.job.id == *id));
        if waiting.is_empty() {
            return Ok(Some(empty));
        }
    }
    Ok(None)
}

async fn claim_function(pool: &PgPool) -> Result<String, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT pg_get_functiondef(p.oid)
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = $1 AND p.proname = 'claim_ready_runtime'",
    )
    .bind(SCHEMA)
    .fetch_one(pool)
    .await
}

const CASES: [(Spend, Layout); 8] = [
    (Spend::Cancel, Layout::RollbackSpent),
    (Spend::Delete, Layout::RollbackSpent),
    (Spend::Move, Layout::RollbackSpent),
    (Spend::Reprioritise, Layout::RollbackSpent),
    (Spend::Cancel, Layout::SpentRollbackSpent),
    (Spend::Delete, Layout::SpentRollbackSpent),
    (Spend::Move, Layout::SpentRollbackSpent),
    (Spend::Reprioritise, Layout::SpentRollbackSpent),
];

#[sqlx::test(migrations = false)]
async fn a_rolled_back_insert_ahead_of_a_spent_job_does_not_stall_the_queue(
    pool: PgPool,
) -> TestResult {
    let store = setup(&pool).await?;
    let mut stalled = Vec::new();
    for (index, (how, layout)) in CASES.into_iter().enumerate() {
        let queue = format!("spent_head_{index}");
        match run_case(&pool, &store, &queue, how, layout).await? {
            Some(empty) if empty <= 1 => {}
            outcome => stalled.push(format!("{how:?} {layout:?}: {outcome:?}")),
        }
    }
    assert!(stalled.is_empty(), "stalled cases: {stalled:#?}");
    Ok(())
}

/// The check above catches the released spent prefix, which crosses one gap
/// per claim: each spent-rollback-spent case costs it a second empty claim.
#[sqlx::test(migrations = false)]
async fn the_released_spent_prefix_costs_an_empty_claim_per_gap(pool: PgPool) -> TestResult {
    let store = setup(&pool).await?;
    let current = claim_function(&pool).await?;
    let start = current
        .find("count(*) FILTER (WHERE NOT selected_with_spent.attempt_spent)")
        .ok_or("no v061 prefix")?;
    let end = current[start..]
        .find("WHERE unspent_through = 0;")
        .ok_or("no v061 prefix end")?
        + start
        + "WHERE unspent_through = 0;".len();
    let line_start = current[..start].rfind('\n').ok_or("no line start")? + 1;
    let old = format!(
        "{}{}{}",
        &current[..line_start],
        OLD_PREFIX,
        &current[end..]
    );
    sqlx::raw_sql(audited_sql(old)).execute(&pool).await?;

    for (index, (how, layout)) in CASES.into_iter().enumerate() {
        let queue = format!("released_{index}");
        let empty = run_case(&pool, &store, &queue, how, layout).await?;
        // A cancel moves the claim cursor over the cancelled row itself after
        // it commits, so only the second spent row is left for a claim.
        let expected = match (layout, how) {
            (Layout::RollbackSpent, _) | (Layout::SpentRollbackSpent, Spend::Cancel) => 1,
            (Layout::SpentRollbackSpent, _) => 2,
        };
        assert_eq!(empty, Some(expected), "{how:?} {layout:?}");
    }
    Ok(())
}
