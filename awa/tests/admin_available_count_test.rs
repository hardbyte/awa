//! The admin views count a lane's available rows exactly up to a per-lane
//! cap, and from the lane's head sequences beyond it.

use awa::model::admin::{self, QueueOverview, AVAILABLE_LANE_EXACT_COUNT_CAP};
use awa::model::{migrations, QueueStorage};
use awa::{JobState, QueueStorageConfig};
use sqlx::PgPool;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const CAP: i64 = AVAILABLE_LANE_EXACT_COUNT_CAP;

async fn setup(pool: &PgPool) -> Result<QueueStorage, Box<dyn std::error::Error>> {
    migrations::run(pool).await?;
    awa_testing::setup::activate_queue_storage(pool).await;
    Ok(QueueStorage::new(QueueStorageConfig::default())?)
}

/// Tombstone `count` rows of a lane, from its first.
async fn tombstone_first_rows(
    pool: &PgPool,
    queue: &str,
    priority: i16,
    count: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO awa.ready_tombstones
             (ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id)
         SELECT ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
         FROM awa.ready_entries
         WHERE queue = $1 AND priority = $2
         ORDER BY lane_seq
         LIMIT $3",
    )
    .bind(queue)
    .bind(priority)
    .bind(count)
    .execute(pool)
    .await?;
    Ok(())
}

async fn overview(pool: &PgPool, queue: &str) -> Result<QueueOverview, Box<dyn std::error::Error>> {
    Ok(admin::queue_overviews(pool)
        .await?
        .into_iter()
        .find(|overview| overview.queue == queue)
        .unwrap_or_else(|| panic!("no overview for {queue}")))
}

#[sqlx::test(migrations = false)]
async fn lanes_within_the_cap_are_counted_exactly(pool: PgPool) -> TestResult {
    let store = setup(&pool).await?;
    store.enqueue_batch(&pool, "below_cap", 1, 100).await?;
    tombstone_first_rows(&pool, "below_cap", 1, 10).await?;
    store.enqueue_batch(&pool, "at_cap", 1, CAP).await?;
    tombstone_first_rows(&pool, "at_cap", 1, 7).await?;

    let below = overview(&pool, "below_cap").await?;
    assert_eq!(
        (below.available, below.available_is_estimate),
        (90, false),
        "tombstoned rows are not available"
    );
    let at_cap = overview(&pool, "at_cap").await?;
    assert_eq!(
        (at_cap.available, at_cap.available_is_estimate),
        (CAP - 7, false)
    );
    assert!(below.lag_seconds.is_some());
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn lanes_over_the_cap_report_the_head_sequence_depth(pool: PgPool) -> TestResult {
    let store = setup(&pool).await?;
    store
        .enqueue_batch(&pool, "over_cap", 1, CAP + 1_000)
        .await?;
    tombstone_first_rows(&pool, "over_cap", 1, 200).await?;

    let over = overview(&pool, "over_cap").await?;
    assert!(over.available_is_estimate);
    assert_eq!(
        over.available,
        CAP + 1_000,
        "the estimate is next_seq - claim_seq, which still counts tombstones"
    );
    assert!(over.lag_seconds.is_some());
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn a_queue_mixing_exact_and_estimated_lanes_is_an_estimate(pool: PgPool) -> TestResult {
    let store = setup(&pool).await?;
    store.enqueue_batch(&pool, "mixed", 1, 50).await?;
    tombstone_first_rows(&pool, "mixed", 1, 5).await?;
    store.enqueue_batch(&pool, "mixed", 2, CAP + 500).await?;
    store.enqueue_batch(&pool, "exact_neighbour", 1, 20).await?;

    let mixed = overview(&pool, "mixed").await?;
    assert!(mixed.available_is_estimate);
    assert_eq!(mixed.available, 45 + CAP + 500);
    let neighbour = overview(&pool, "exact_neighbour").await?;
    assert_eq!(
        (neighbour.available, neighbour.available_is_estimate),
        (20, false)
    );

    let counts = admin::state_counts(&pool).await?;
    assert_eq!(
        counts[&JobState::Available],
        45 + CAP + 500 + 20,
        "dashboard counts use the same per-lane rule"
    );
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn recent_available_rows_are_counted_behind_an_older_backlog(pool: PgPool) -> TestResult {
    let store = setup(&pool).await?;
    store
        .enqueue_batch(&pool, "backlog", 1, CAP + 2_000)
        .await?;
    sqlx::query("UPDATE awa.ready_entries SET created_at = now() - interval '3 hours'")
        .execute(&pool)
        .await?;
    store.enqueue_batch(&pool, "backlog", 1, 20).await?;

    let recent_available: i64 = admin::state_timeseries(&pool, 60)
        .await?
        .iter()
        .filter(|bucket| bucket.state == JobState::Available)
        .map(|bucket| bucket.count)
        .sum();
    assert_eq!(recent_available, 20);
    Ok(())
}
