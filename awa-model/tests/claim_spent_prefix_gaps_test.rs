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
const NEW_PREFIX: &str =
    "                    count(*) FILTER (WHERE NOT selected_with_spent.attempt_spent)
                        OVER (ORDER BY selected_with_spent.lane_seq) AS unspent_through
                FROM selected_with_spent
            )
            SELECT max(spent_prefix.lane_seq) + 1
            INTO v_spent_next_seq
            FROM spent_prefix
            WHERE unspent_through = 0;";

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

/// A lane holding a tombstoned row, a rolled-back reservation, a tombstoned
/// row, a rolled-back reservation, another tombstoned row and a live one.
/// Returns the lane positions two successive claims hand out, and the claim
/// cursor after them.
async fn claim_past_gaps_and_tombstones(
    pool: &PgPool,
    store: &QueueStorage,
    queue: &str,
) -> Result<(Vec<i64>, i64), Box<dyn std::error::Error>> {
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

    let mut claimed = Vec::new();
    for _ in 0..2 {
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
        assert!(installed.contains(NEW_PREFIX), "the installer has v061");

        // Put the old body back, so the migration is exercised on an
        // installed function and not only through the installer.
        let old = installed.replace(NEW_PREFIX, OLD_PREFIX);
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
    }
    Ok(())
}
