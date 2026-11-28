//! #885: a claim must not move a lane's cursor past an unclaimed row, whatever
//! layout of ready segments the lane has and however a mover changes it
//! between the claim choosing its target slot and reading it.
//!
//! Nothing in a release moves rows between slots yet, so each case builds the
//! layout with direct SQL: an interleaved lane, and a lane whose prefix a
//! mover relocates and commits while a claim is already past its first
//! statement. The stale-target cases hold the claim between its snapshot and
//! its head lock: the mover takes an ACCESS EXCLUSIVE lock on the lane's claim
//! sequence (`ALTER SEQUENCE ... OWNER TO`), which the claim's first statement
//! reads through `sequence_next_value` after taking its snapshot.

use awa_model::{audited_sql, migrations, QueueStorage, QueueStorageConfig};
use sqlx::PgPool;
use std::collections::BTreeSet;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const PRIORITY: i16 = 2;
const SLOTS: i32 = 4;

async fn install(pool: &PgPool, schema: &str) -> Result<QueueStorage, Box<dyn std::error::Error>> {
    migrations::run(pool).await?;
    let store = QueueStorage::new(QueueStorageConfig {
        schema: schema.into(),
        queue_slot_count: SLOTS as usize,
        lease_slot_count: 2,
        claim_slot_count: 2,
        lease_claim_receipts: true,
        ..Default::default()
    })?;
    store.install(pool).await?;
    Ok(store)
}

/// The lane positions of the queue's ready rows, in order.
async fn lane_seqs(
    pool: &PgPool,
    schema: &str,
    queue: &str,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    Ok(sqlx::query_scalar(audited_sql(format!(
        "SELECT lane_seq FROM {schema}.ready_entries WHERE queue = $1 ORDER BY lane_seq"
    )))
    .bind(queue)
    .fetch_all(pool)
    .await?)
}

/// The slot holding the queue's ready rows.
async fn only_slot(
    pool: &PgPool,
    schema: &str,
    queue: &str,
) -> Result<(i32, i64), Box<dyn std::error::Error>> {
    Ok(sqlx::query_as(audited_sql(format!(
        "SELECT DISTINCT ready_slot, ready_generation FROM {schema}.ready_entries WHERE queue = $1"
    )))
    .bind(queue)
    .fetch_one(pool)
    .await?)
}

/// Replaces the queue's segments with one covering all its rows, the shape a
/// single enqueue of the whole lane leaves.
async fn merge_segments(pool: &PgPool, schema: &str, queue: &str) -> TestResult {
    sqlx::query(audited_sql(format!(
        "WITH gone AS (
             DELETE FROM {schema}.ready_segments WHERE queue = $1
             RETURNING ready_slot, ready_generation, priority, enqueue_shard,
                       first_lane_seq, next_lane_seq, first_run_at
         )
         INSERT INTO {schema}.ready_segments
         (ready_slot, ready_generation, queue, priority, enqueue_shard,
          first_lane_seq, next_lane_seq, first_run_at)
         SELECT min(ready_slot), min(ready_generation), $1, min(priority), min(enqueue_shard),
                min(first_lane_seq), max(next_lane_seq), min(first_run_at)
         FROM gone"
    )))
    .bind(queue)
    .execute(pool)
    .await?;
    Ok(())
}

/// Moves the lane positions `[from, to)` of a lane held in one segment to
/// `target_slot`, splitting the segment around them, in the caller's
/// transaction.
async fn move_range(
    tx: &mut sqlx::PgConnection,
    schema: &str,
    queue: &str,
    source: (i32, i64),
    target_slot: i32,
    from: i64,
    to: i64,
) -> TestResult {
    let (segment_first, segment_next, first_run_at): (i64, i64, chrono::DateTime<chrono::Utc>) =
        sqlx::query_as(audited_sql(format!(
            "SELECT first_lane_seq, next_lane_seq, first_run_at FROM {schema}.ready_segments
             WHERE queue = $1 AND ready_slot = $2 AND ready_generation = $3
               AND first_lane_seq <= $4 AND next_lane_seq >= $5"
        )))
        .bind(queue)
        .bind(source.0)
        .bind(source.1)
        .bind(from)
        .bind(to)
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query(audited_sql(format!(
        "DELETE FROM {schema}.ready_segments
         WHERE queue = $1 AND ready_slot = $2 AND ready_generation = $3 AND first_lane_seq = $4"
    )))
    .bind(queue)
    .bind(source.0)
    .bind(source.1)
    .bind(segment_first)
    .execute(&mut *tx)
    .await?;
    sqlx::query(audited_sql(format!(
        "UPDATE {schema}.ready_entries SET ready_slot = $2
         WHERE queue = $1 AND ready_slot = $3 AND lane_seq >= $4 AND lane_seq < $5"
    )))
    .bind(queue)
    .bind(target_slot)
    .bind(source.0)
    .bind(from)
    .bind(to)
    .execute(&mut *tx)
    .await?;
    let pieces = [
        (segment_first, from, source.0),
        (from, to, target_slot),
        (to, segment_next, source.0),
    ];
    for (first, next, slot) in pieces {
        if first >= next {
            continue;
        }
        sqlx::query(audited_sql(format!(
            "INSERT INTO {schema}.ready_segments
             (ready_slot, ready_generation, queue, priority, enqueue_shard,
              first_lane_seq, next_lane_seq, first_run_at)
             VALUES ($1, $2, $3, $4, 0, $5, $6, $7)"
        )))
        .bind(slot)
        .bind(source.1)
        .bind(queue)
        .bind(PRIORITY)
        .bind(first)
        .bind(next)
        .bind(first_run_at)
        .execute(&mut *tx)
        .await?;
    }
    Ok(())
}

async fn claim_once(
    pool: &PgPool,
    schema: &str,
    queue: &str,
    max_batch: i64,
) -> Result<Vec<i64>, sqlx::Error> {
    sqlx::query_scalar(audited_sql(format!(
        "SELECT lane_seq FROM {schema}.claim_ready_runtime($1, {max_batch}, 0::float8, 0::float8)"
    )))
    .bind(queue)
    .fetch_all(pool)
    .await
}

/// Claims until the lane stays empty and returns every position handed out.
async fn drain(
    pool: &PgPool,
    schema: &str,
    queue: &str,
    max_batch: i64,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let mut claimed = Vec::new();
    let mut empty_in_a_row = 0;
    for _ in 0..60 {
        let batch = claim_once(pool, schema, queue, max_batch).await?;
        if batch.is_empty() {
            empty_in_a_row += 1;
            if empty_in_a_row == 3 {
                break;
            }
        } else {
            empty_in_a_row = 0;
            claimed.extend(batch);
        }
    }
    Ok(claimed)
}

fn assert_every_row_claimed_once(claimed: &[i64], expected: &[i64], what: &str) {
    let mut sorted = claimed.to_vec();
    sorted.sort_unstable();
    let unique: BTreeSet<i64> = sorted.iter().copied().collect();
    assert_eq!(
        sorted.len(),
        unique.len(),
        "{what}: a row was claimed twice"
    );
    let missing: Vec<i64> = expected
        .iter()
        .copied()
        .filter(|seq| !unique.contains(seq))
        .collect();
    assert!(
        missing.is_empty(),
        "{what}: the cursor passed unclaimed rows {missing:?}"
    );
}

/// Enqueues 30 rows and leaves positions 11-20 in another slot, between two
/// runs in the first.
async fn build_interleaved_lane(
    pool: &PgPool,
    store: &QueueStorage,
    queue: &str,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let schema = store.schema();
    store.enqueue_batch(pool, queue, PRIORITY, 30).await?;
    merge_segments(pool, schema, queue).await?;
    let all = lane_seqs(pool, schema, queue).await?;
    let source = only_slot(pool, schema, queue).await?;
    let mut tx = pool.begin().await?;
    move_range(
        &mut tx,
        schema,
        queue,
        source,
        (source.0 + 1) % SLOTS,
        all[10],
        all[20],
    )
    .await?;
    tx.commit().await?;
    Ok(all)
}

#[sqlx::test(migrations = false)]
async fn interleaved_lane_claims_every_row(pool: PgPool) -> TestResult {
    let schema = "bound_interleaved";
    let store = install(&pool, schema).await?;
    let queue = "interleaved";
    let all = build_interleaved_lane(&pool, &store, queue).await?;

    // A batch of 15 reaches from the first run in the source slot into the
    // run after the other slot's rows.
    let claimed = drain(&pool, schema, queue, 15).await?;
    assert_every_row_claimed_once(&claimed, &all, "interleaved lane");
    Ok(())
}

/// A claim whose first statement started before a mover committed, and that
/// takes its head lock after.
async fn claim_across_a_mover(
    pool: &PgPool,
    schema: &str,
    queue: &str,
    moved_rows: usize,
) -> Result<(Vec<i64>, Vec<i64>), Box<dyn std::error::Error>> {
    let all = lane_seqs(pool, schema, queue).await?;
    let source = only_slot(pool, schema, queue).await?;

    let claim_sequence: String = sqlx::query_scalar(audited_sql(format!(
        "SELECT seq_name FROM {schema}.queue_claim_heads
         WHERE queue = $1 AND priority = {PRIORITY} AND enqueue_shard = 0"
    )))
    .bind(queue)
    .fetch_one(pool)
    .await?;
    let mut mover = pool.begin().await?;
    sqlx::query(audited_sql(format!(
        "ALTER SEQUENCE {schema}.{claim_sequence} OWNER TO CURRENT_USER"
    )))
    .execute(&mut *mover)
    .await?;

    let claim_pool = pool.clone();
    let claim_schema = schema.to_string();
    let claim_queue = queue.to_string();
    let claim =
        tokio::spawn(async move { claim_once(&claim_pool, &claim_schema, &claim_queue, 10).await });

    // Wait until the claim has started its first statement and is blocked on
    // the mover's lock.
    let mut blocked = false;
    for _ in 0..200 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
             WHERE pid <> pg_backend_pid() AND wait_event_type = 'Lock'
               AND query LIKE '%claim_ready_runtime%'",
        )
        .fetch_one(pool)
        .await?;
        if waiting > 0 {
            blocked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(blocked, "the claim never blocked on the mover's lock");

    move_range(
        &mut mover,
        schema,
        queue,
        source,
        (source.0 + 1) % SLOTS,
        all[0],
        all[moved_rows],
    )
    .await?;
    mover.commit().await?;
    let first_claim = claim.await??;
    let rest = drain(pool, schema, queue, 10).await?;
    let mut claimed = first_claim.clone();
    claimed.extend(rest);
    Ok((claimed, all))
}

#[sqlx::test(migrations = false)]
async fn a_claim_whose_target_went_stale_does_not_skip_the_moved_prefix(
    pool: PgPool,
) -> TestResult {
    let schema = "bound_stale_prefix";
    let store = install(&pool, schema).await?;
    let queue = "stale";
    store.enqueue_batch(&pool, queue, PRIORITY, 10).await?;
    merge_segments(&pool, schema, queue).await?;
    let (claimed, all) = claim_across_a_mover(&pool, schema, queue, 5).await?;
    assert_every_row_claimed_once(&claimed, &all, "stale target, half the range moved");
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn a_claim_whose_head_row_left_the_slot_does_not_jump_the_cursor(pool: PgPool) -> TestResult {
    let schema = "bound_stale_head";
    let store = install(&pool, schema).await?;
    let queue = "stale_head";
    store.enqueue_batch(&pool, queue, PRIORITY, 30).await?;
    merge_segments(&pool, schema, queue).await?;
    // The first 20 rows move, so the next row of the stale target is at 21.
    let (claimed, all) = claim_across_a_mover(&pool, schema, queue, 20).await?;
    assert_every_row_claimed_once(&claimed, &all, "stale target, head row moved");
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn a_lane_with_one_segment_per_row_still_claims_a_full_batch(pool: PgPool) -> TestResult {
    let schema = "bound_segment_per_row";
    let store = install(&pool, schema).await?;
    let queue = "narrow";
    for _ in 0..25 {
        store.enqueue_batch(&pool, queue, PRIORITY, 1).await?;
    }
    let segments: i64 = sqlx::query_scalar(audited_sql(format!(
        "SELECT count(*) FROM {schema}.ready_segments WHERE queue = $1"
    )))
    .bind(queue)
    .fetch_one(&pool)
    .await?;
    assert_eq!(segments, 25, "each enqueue is its own segment");

    let first = claim_once(&pool, schema, queue, 10).await?;
    assert_eq!(
        first.len(),
        10,
        "a full batch from a lane of one-row segments"
    );
    let all = lane_seqs(&pool, schema, queue).await?;
    let mut claimed = first;
    claimed.extend(drain(&pool, schema, queue, 10).await?);
    assert_every_row_claimed_once(&claimed, &all, "one segment per row");
    Ok(())
}

async fn claim_functions(
    pool: &PgPool,
    schema: &str,
) -> Result<Vec<(String, i64, String)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT p.proname::text, p.oid::bigint, pg_get_functiondef(p.oid)
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = $1 AND p.proname ~ '^claim_ready_(runtime|slot_[0-9]+)$'
         ORDER BY p.proname",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
}

/// Removes the text from the first `start` after `from` up to and including
/// the first `end` after it.
fn cut(text: &mut String, start: &str, end: &str) {
    let begin = text.find(start).expect("start marker");
    let finish = begin + text[begin..].find(end).expect("end marker") + end.len();
    text.replace_range(begin..finish, "");
}

/// The claim functions as released before v064: no re-resolve after the head
/// lock and no bound on the reads.
fn without_the_bound(definition: &str, is_runtime: bool) -> String {
    let mut old = definition.to_string();
    if is_runtime {
        cut(
            &mut old,
            "\n\n            -- The head lock can be granted",
            "LIMIT 1;",
        );
        cut(
            &mut old,
            "\n              -- Rows of this slot at or after",
            "9223372036854775807)",
        );
    } else {
        cut(
            &mut old,
            "\n                  -- Stop below the next segment of another slot",
            "9223372036854775807)",
        );
    }
    old
}

#[sqlx::test(migrations = false)]
async fn v064_bounds_the_released_claim_functions_in_place(pool: PgPool) -> TestResult {
    let schema = "bound_migration";
    let store = install(&pool, schema).await?;
    let installed = claim_functions(&pool, schema).await?;
    assert_eq!(installed.len(), 1 + SLOTS as usize);
    for (name, _, definition) in &installed {
        assert!(
            definition.contains("GREATEST(p_max_batch, 0) + 1")
                || definition.contains("GREATEST(p_claim_limit, 0) + 1"),
            "{name}: the installer carries the bound"
        );
    }

    for (name, _, definition) in &installed {
        let old = without_the_bound(definition, name == "claim_ready_runtime");
        assert_ne!(&old, definition);
        sqlx::raw_sql(audited_sql(old)).execute(&pool).await?;
    }
    let unbounded = build_interleaved_lane(&pool, &store, "before").await?;
    let claimed = drain(&pool, schema, "before", 15).await?;
    assert!(
        unbounded.iter().any(|seq| !claimed.contains(seq)),
        "the released functions skip rows of an interleaved lane"
    );

    for (_, _, sql) in migrations::migration_sql_range(63, 64) {
        sqlx::raw_sql(audited_sql(sql)).execute(&pool).await?;
    }
    assert_eq!(
        claim_functions(&pool, schema).await?,
        installed,
        "v064 rewrites each function in place to the installer's text"
    );
    for (_, _, sql) in migrations::migration_sql_range(63, 64) {
        sqlx::raw_sql(audited_sql(sql)).execute(&pool).await?;
    }
    assert_eq!(
        claim_functions(&pool, schema).await?,
        installed,
        "replaying v064 leaves the functions alone"
    );

    let all = build_interleaved_lane(&pool, &store, "after").await?;
    let claimed = drain(&pool, schema, "after", 15).await?;
    assert_every_row_claimed_once(&claimed, &all, "migrated claim functions");
    Ok(())
}
