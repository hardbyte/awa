#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use awa_model::{audited_sql, migrations, QueueStorage, QueueStorageConfig};
use sqlx::PgPool;

type TestResult = Result<(), Box<dyn std::error::Error>>;

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
/// The spent prefix 0.6.19's claim_spent_prefix_gaps schema patch installs,
/// which crosses gaps without the generation bound.
const GAP_PREFIX: &str =
    "                    count(*) FILTER (WHERE NOT selected_with_spent.attempt_spent)
                        OVER (ORDER BY selected_with_spent.lane_seq) AS unspent_through
                FROM selected_with_spent
            )
            SELECT max(spent_prefix.lane_seq) + 1
            INTO v_spent_next_seq
            FROM spent_prefix
            WHERE unspent_through = 0;";

/// The v061 spent prefix: the leading run of spent rows, up to the first
/// segment of another generation that ends inside it.
fn new_prefix(schema: &str) -> String {
    format!(
        "                    count(*) FILTER (WHERE NOT selected_with_spent.attempt_spent)
                        OVER (ORDER BY selected_with_spent.lane_seq) AS unspent_through
                FROM selected_with_spent
            )
            SELECT LEAST(
                max(spent_prefix.lane_seq) + 1,
                (
                    SELECT min(other.first_lane_seq)
                    FROM {schema}.ready_segments AS other
                    WHERE other.queue = p_queue
                      AND other.priority = v_lane_priority
                      AND other.enqueue_shard = v_lane_shard
                      AND other.next_lane_seq > v_lane_claim_seq
                      AND other.next_lane_seq <= max(spent_prefix.lane_seq)
                      AND (other.ready_slot, other.ready_generation)
                          IS DISTINCT FROM (v_target_slot, v_target_generation)
                )
            )
            INTO v_spent_next_seq
            FROM spent_prefix
            WHERE unspent_through = 0;"
    )
}

async fn claim_function(pool: &PgPool, schema: &str) -> Result<(i64, String), sqlx::Error> {
    sqlx::query_as(
        "SELECT p.oid::bigint, pg_get_functiondef(p.oid)
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = $1 AND p.proname = 'claim_ready_runtime'",
    )
    .bind(schema)
    .fetch_one(pool)
    .await
}

async fn apply_v061(pool: &PgPool) -> TestResult {
    for (_, _, sql) in migrations::migration_sql_range(60, 61) {
        sqlx::raw_sql(audited_sql(sql)).execute(pool).await?;
    }
    Ok(())
}

/// Builds a lane at positions 1 to 6 holding a tombstoned row, a rolled-back
/// reservation, a tombstoned row, a rolled-back reservation, another
/// tombstoned row and a live one.
async fn build_gapped_lane(pool: &PgPool, store: &QueueStorage, queue: &str) -> TestResult {
    let schema = store.schema();
    let reserve_and_roll_back = || async {
        let mut tx = pool.begin().await?;
        sqlx::query(audited_sql(format!(
            "SELECT {schema}.reserve_enqueue_seq($1, 2::smallint, 0::smallint, 1)"
        )))
        .bind(queue)
        .execute(&mut *tx)
        .await?;
        tx.rollback().await
    };
    let tombstone_last = || async {
        sqlx::query(audited_sql(format!(
            "INSERT INTO {schema}.ready_tombstones
             (ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id)
             SELECT ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
             FROM {schema}.ready_entries WHERE queue = $1
             ORDER BY lane_seq DESC LIMIT 1"
        )))
        .bind(queue)
        .execute(pool)
        .await
    };
    store.enqueue_batch(pool, queue, 2, 1).await?;
    tombstone_last().await?;
    reserve_and_roll_back().await?;
    store.enqueue_batch(pool, queue, 2, 1).await?;
    tombstone_last().await?;
    reserve_and_roll_back().await?;
    store.enqueue_batch(pool, queue, 2, 1).await?;
    tombstone_last().await?;
    store.enqueue_batch(pool, queue, 2, 1).await?;
    Ok(())
}

/// Runs `claims` claims on the lane and returns the lane positions they hand
/// out and the claim cursor after them.
async fn claim(
    pool: &PgPool,
    store: &QueueStorage,
    queue: &str,
    claims: usize,
) -> Result<(Vec<i64>, i64), Box<dyn std::error::Error>> {
    let schema = store.schema();
    let mut claimed = Vec::new();
    for _ in 0..claims {
        let lanes: Vec<i64> = sqlx::query_scalar(audited_sql(format!(
            "SELECT lane_seq FROM {schema}.claim_ready_runtime($1, 10, 0::float8, 0::float8)"
        )))
        .bind(queue)
        .fetch_all(pool)
        .await?;
        claimed.extend(lanes);
    }
    let cursor: i64 = sqlx::query_scalar(audited_sql(format!(
        "SELECT {schema}.sequence_next_value(seq_name) FROM {schema}.queue_claim_heads
         WHERE queue = $1 AND priority = 2 AND enqueue_shard = 0"
    )))
    .bind(queue)
    .fetch_one(pool)
    .await?;
    Ok((claimed, cursor))
}

/// The lane positions two claims on a fresh gapped lane hand out, and the
/// claim cursor after them.
async fn claim_past_gaps_and_tombstones(
    pool: &PgPool,
    store: &QueueStorage,
    queue: &str,
) -> Result<(Vec<i64>, i64), Box<dyn std::error::Error>> {
    build_gapped_lane(pool, store, queue).await?;
    claim(pool, store, queue, 2).await
}

#[sqlx::test(migrations = false)]
async fn v061_lets_a_claim_cross_rolled_back_reservations(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    for receipts in [false, true] {
        let schema = if receipts {
            "gap_receipts"
        } else {
            "gap_legacy"
        };
        let store = QueueStorage::new(QueueStorageConfig {
            schema: schema.into(),
            queue_slot_count: 4,
            lease_slot_count: 2,
            claim_slot_count: 2,
            lease_claim_receipts: receipts,
            ..Default::default()
        })?;
        store.install(&pool).await?;
        let (oid, installed) = claim_function(&pool, schema).await?;
        let new = new_prefix(schema);
        assert!(installed.contains(&new), "the installer has v061");

        // v061 also bounds the unbounded prefix 0.6.19's schema patch installs.
        sqlx::raw_sql(audited_sql(installed.replace(&new, GAP_PREFIX)))
            .execute(&pool)
            .await?;
        apply_v061(&pool).await?;
        assert_eq!(
            claim_function(&pool, schema).await?,
            (oid, installed.clone()),
            "{schema}: v061 bounds the 0.6.19 prefix in place"
        );

        // Put the released body back, so the migration is exercised on an
        // installed function and not only through the installer.
        let old = installed.replace(&new, OLD_PREFIX);
        assert_ne!(old, installed);
        sqlx::raw_sql(audited_sql(old)).execute(&pool).await?;
        let (claimed, cursor) = claim_past_gaps_and_tombstones(&pool, &store, "old").await?;
        assert!(
            claimed.is_empty(),
            "{schema}: the old body crosses one gap per claim"
        );
        assert_eq!(cursor, 4, "{schema}: the old body stops at the second gap");

        apply_v061(&pool).await?;
        let (migrated_oid, migrated) = claim_function(&pool, schema).await?;
        assert_eq!(migrated_oid, oid, "{schema}: replaced in place");
        assert_eq!(migrated, installed, "{schema}: v061 matches the installer");
        apply_v061(&pool).await?;
        assert_eq!(
            claim_function(&pool, schema).await?.1,
            installed,
            "{schema}: replaying v061 preserves the body"
        );

        let (claimed, cursor) = claim_past_gaps_and_tombstones(&pool, &store, "new").await?;
        assert_eq!(claimed, vec![6], "{schema}: the live row is claimed");
        assert_eq!(
            cursor, 6,
            "{schema}: the claim crosses the gaps and tombstones"
        );

        // A segment of another generation inside the spent run: a row a
        // producer wrote from a snapshot older than a ring rotation. The claim
        // must stop at it rather than cross it.
        let queue = "bounded";
        build_gapped_lane(&pool, &store, queue).await?;
        let (slot, generation): (i32, i64) = sqlx::query_as(audited_sql(format!(
            "SELECT ready_slot, ready_generation FROM {schema}.ready_entries
             WHERE queue = $1 LIMIT 1"
        )))
        .bind(queue)
        .fetch_one(&pool)
        .await?;
        sqlx::query(audited_sql(format!(
            "INSERT INTO {schema}.ready_segments
             (ready_slot, ready_generation, queue, priority, enqueue_shard,
              first_lane_seq, next_lane_seq, first_run_at)
             VALUES ($1, $2, $3, 2, 0, 4, 5, now())"
        )))
        .bind((slot + 1) % 4)
        .bind(generation - 1)
        .bind(queue)
        .execute(&pool)
        .await?;
        let (claimed, cursor) = claim(&pool, &store, queue, 1).await?;
        assert!(claimed.is_empty(), "{schema}: the head is spent");
        assert_eq!(
            cursor, 4,
            "{schema}: the claim stops at the other generation's segment"
        );
        sqlx::query(audited_sql(format!(
            "DELETE FROM {schema}.ready_segments WHERE queue = $1 AND ready_slot = $2"
        )))
        .bind(queue)
        .bind((slot + 1) % 4)
        .execute(&pool)
        .await?;
        let (claimed, cursor) = claim(&pool, &store, queue, 2).await?;
        assert_eq!(claimed, vec![6], "{schema}: then the live row is claimed");
        assert_eq!(cursor, 6, "{schema}: past the remaining gap and tombstone");
    }
    Ok(())
}
