use awa_model::{migrations, QueueStorage, QueueStorageConfig};
use sqlx::PgPool;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const PATCH: &str = "correlated_claim_tombstones";
const FENCE: &str =
    "                        -- Keep this bounded claim on indexed probes, not a full-partition hash.\n                        OFFSET 0\n";

async fn patch_pending(pool: &PgPool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Ok(migrations::schema_patch_status(pool)
        .await?
        .into_iter()
        .find(|status| status.name == PATCH)
        .expect("the patch is registered")
        .needs_repair)
}

#[sqlx::test(migrations = false)]
async fn claim_tombstone_patch_keeps_bounded_claims_on_index_probes(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    assert!(patch_pending(&pool).await?.is_empty());
    let patch = migrations::SCHEMA_PATCHES
        .iter()
        .find(|patch| patch.name == PATCH)
        .expect("the patch is registered");

    for receipts in [false, true] {
        let schema = if receipts {
            "claim_receipts"
        } else {
            "claim_legacy"
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
        store.enqueue_batch(&pool, "probe", 2, 20_000).await?;
        sqlx::raw_sql(&format!(
            "INSERT INTO {schema}.ready_tombstones
             (ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id)
             SELECT ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
             FROM {schema}.ready_entries WHERE lane_seq % 2 = 0;
             ANALYZE {schema}.ready_entries;
             ANALYZE {schema}.ready_tombstones;"
        ))
        .execute(&pool)
        .await?;

        // Put the released function bodies back, so the patch is exercised on
        // installed functions and not only through the new installer.
        let functions: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT p.oid::bigint, p.proname::text, pg_get_functiondef(p.oid)
             FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname = $1 AND (p.proname = 'claim_ready_runtime'
                 OR p.proname ~ '^claim_ready_slot_[0-9]+$') ORDER BY p.oid",
        )
        .bind(schema)
        .fetch_all(&pool)
        .await?;
        assert_eq!(functions.len(), 1);
        for (_, _, definition) in &functions {
            let old = definition.replace(FENCE, "");
            assert_ne!(&old, definition, "fixture must restore the old lookup");
            sqlx::raw_sql(&old).execute(&pool).await?;
        }
        let mut pending = patch_pending(&pool).await?;
        pending.sort();
        let mut expected: Vec<String> = functions
            .iter()
            .map(|(_, name, _)| format!("claim function {schema}.{name}"))
            .collect();
        expected.sort();
        assert_eq!(pending, expected, "the probe names each unfenced function");

        sqlx::raw_sql(patch.sql).execute(&pool).await?;
        assert!(patch_pending(&pool).await?.is_empty());
        for (oid, _, _) in &functions {
            let upgraded: String = sqlx::query_scalar("SELECT pg_get_functiondef($1::oid)")
                .bind(*oid)
                .fetch_one(&pool)
                .await?;
            assert!(upgraded.contains("AND tomb.lane_seq = ready.lane_seq OFFSET 0"));
            sqlx::raw_sql(patch.sql).execute(&pool).await?;
            let replayed: String = sqlx::query_scalar("SELECT pg_get_functiondef($1::oid)")
                .bind(*oid)
                .fetch_one(&pool)
                .await?;
            assert_eq!(
                upgraded, replayed,
                "replaying the patch must preserve the body"
            );
        }

        for mode in ["force_custom_plan", "force_generic_plan"] {
            let mut tx = pool.begin().await?;
            sqlx::raw_sql(&format!(
                "SET LOCAL plan_cache_mode = {mode};
                 SET LOCAL work_mem = '125MB'; SET LOCAL random_page_cost = 1.2;"
            ))
            .execute(&mut *tx)
            .await?;
            let claimed: Vec<i64> = sqlx::query_scalar(&format!(
                "SELECT lane_seq FROM {schema}.claim_ready_runtime('probe',32,0::float8,0::float8)"
            ))
            .fetch_all(&mut *tx)
            .await?;
            assert_eq!(claimed.len(), 16, "only live rows in the batch are claimed");
            assert!(claimed.iter().all(|lane| lane % 2 == 1));
            let scanned: i64 = sqlx::query_scalar(
                "SELECT COALESCE(sum(seq_tup_read),0)::bigint
                 FROM pg_stat_xact_all_tables
                 WHERE schemaname=$1 AND relname LIKE 'ready_tombstones%'",
            )
            .bind(schema)
            .fetch_one(&mut *tx)
            .await?;
            assert_eq!(
                scanned, 0,
                "{schema}: {mode} must not scan tombstone partitions"
            );
            tx.rollback().await?;
        }
    }
    Ok(())
}
