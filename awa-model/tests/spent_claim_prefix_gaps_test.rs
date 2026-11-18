use awa_model::{migrations, QueueStorage, QueueStorageConfig};
use sqlx::PgPool;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const PATCH: &str = "spent_claim_prefix_gaps";
const RELEASED_PREFIX: &str = "                    selected_with_spent.lane_seq
                        - row_number() OVER (ORDER BY selected_with_spent.lane_seq)::bigint AS prefix_group
                FROM selected_with_spent
                WHERE attempt_spent
            )
            SELECT max(spent_prefix.lane_seq) + 1
            INTO v_spent_next_seq
            FROM spent_prefix
            WHERE prefix_group = v_lane_claim_seq - 1;";
const PATCHED_PREFIX: &str =
    "                    count(*) FILTER (WHERE NOT selected_with_spent.attempt_spent)
                        OVER (ORDER BY selected_with_spent.lane_seq) AS unspent_through
                FROM selected_with_spent
            )
            SELECT max(spent_prefix.lane_seq) + 1
            INTO v_spent_next_seq
            FROM spent_prefix
            WHERE unspent_through = 0;";

async fn patch_pending(pool: &PgPool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Ok(migrations::schema_patch_status(pool)
        .await?
        .into_iter()
        .find(|status| status.name == PATCH)
        .expect("the patch is registered")
        .needs_repair)
}

async fn installer(pool: &PgPool) -> Result<String, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT pg_get_functiondef(
             'awa.install_queue_storage_substrate(text,integer,integer,integer,boolean)'::regprocedure)",
    )
    .fetch_one(pool)
    .await
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
        sqlx::query(&format!(
            "SELECT {schema}.reserve_enqueue_seq($1, 2::smallint, 0::smallint, 1)"
        ))
        .bind(queue)
        .execute(&mut *tx)
        .await?;
        tx.rollback().await
    };
    let tombstone_last = || async {
        sqlx::query(&format!(
            "INSERT INTO {schema}.ready_tombstones
             (ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id)
             SELECT ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
             FROM {schema}.ready_entries WHERE queue = $1
             ORDER BY lane_seq DESC LIMIT 1"
        ))
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
        let lanes: Vec<i64> = sqlx::query_scalar(&format!(
            "SELECT lane_seq FROM {schema}.claim_ready_runtime($1, 10, 0::float8, 0::float8)"
        ))
        .bind(queue)
        .fetch_all(pool)
        .await?;
        claimed.extend(lanes);
    }
    let cursor: i64 = sqlx::query_scalar(&format!(
        "SELECT {schema}.sequence_next_value(seq_name) FROM {schema}.queue_claim_heads
         WHERE queue = $1 AND priority = 2 AND enqueue_shard = 0"
    ))
    .bind(queue)
    .fetch_one(pool)
    .await?;
    Ok((claimed, cursor))
}

#[sqlx::test(migrations = false)]
async fn claim_spent_prefix_patch_crosses_rolled_back_reservations(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    assert!(patch_pending(&pool).await?.is_empty());
    let patch = migrations::SCHEMA_PATCHES
        .iter()
        .find(|patch| patch.name == PATCH)
        .expect("the patch is registered");

    // An earlier patch that embeds the released v023 installer puts the
    // released installer back.
    sqlx::raw_sql(include_str!(
        "../migrations/v023_install_queue_storage_substrate.sql"
    ))
    .execute(&pool)
    .await?;
    let released_installer = installer(&pool).await?;
    assert!(released_installer.contains(RELEASED_PREFIX));
    assert_eq!(
        patch_pending(&pool).await?,
        vec!["awa.install_queue_storage_substrate".to_string()],
        "the probe names the released installer"
    );
    sqlx::raw_sql(patch.sql).execute(&pool).await?;
    assert!(patch_pending(&pool).await?.is_empty());
    assert_eq!(
        installer(&pool).await?,
        released_installer.replace(RELEASED_PREFIX, PATCHED_PREFIX),
        "the patch changes only the installer's spent prefix"
    );

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
        assert!(
            installed.contains(PATCHED_PREFIX),
            "{schema}: a substrate installed after the patch carries the new claim"
        );

        // Put the released function body back, as a substrate installed
        // before the patch has it.
        let released = installed.replace(PATCHED_PREFIX, RELEASED_PREFIX);
        sqlx::raw_sql(&released).execute(&pool).await?;
        assert_eq!(
            patch_pending(&pool).await?,
            vec![format!("claim function {schema}.claim_ready_runtime")],
            "the probe names the released function"
        );
        let (claimed, cursor) = claim_past_gaps_and_tombstones(&pool, &store, "released").await?;
        assert!(claimed.is_empty(), "{schema}: released body stalls");
        assert_eq!(cursor, 2, "{schema}: released body stops at the first gap");

        sqlx::raw_sql(patch.sql).execute(&pool).await?;
        assert!(patch_pending(&pool).await?.is_empty());
        let (patched_oid, patched) = claim_function(&pool, schema).await?;
        assert_eq!(patched_oid, oid, "{schema}: replaced in place");
        assert_eq!(
            patched, installed,
            "{schema}: patched body matches a fresh install"
        );
        sqlx::raw_sql(patch.sql).execute(&pool).await?;
        assert_eq!(
            claim_function(&pool, schema).await?.1,
            installed,
            "{schema}: replaying the patch preserves the body"
        );

        let (claimed, cursor) = claim_past_gaps_and_tombstones(&pool, &store, "patched").await?;
        assert_eq!(claimed, vec![6], "{schema}: the live row is claimed");
        assert_eq!(
            cursor, 6,
            "{schema}: the claim crosses the gaps and tombstones"
        );
    }
    Ok(())
}
