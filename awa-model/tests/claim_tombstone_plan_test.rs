use awa_model::{audited_sql, migrations, QueueStorage, QueueStorageConfig};
use sqlx::PgPool;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[sqlx::test(migrations = false)]
async fn claim_tombstone_upgrade_keeps_bounded_claims_on_index_probes(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
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
        sqlx::raw_sql(audited_sql(format!(
            "INSERT INTO {schema}.ready_tombstones
             (ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id)
             SELECT ready_slot, ready_generation, queue, priority, enqueue_shard, lane_seq, job_id
             FROM {schema}.ready_entries WHERE lane_seq % 2 = 0;
             ANALYZE {schema}.ready_entries;
             ANALYZE {schema}.ready_tombstones;"
        )))
        .execute(&pool)
        .await?;

        // Exercise the upgrade on installed functions, not just the new installer.
        let functions: Vec<(i64, String)> = sqlx::query_as(
            "SELECT p.oid::bigint, pg_get_functiondef(p.oid)
             FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
             WHERE n.nspname=$1 AND (p.proname='claim_ready_runtime'
                 OR p.proname ~ '^claim_ready_slot_[0-9]+$') ORDER BY p.oid",
        )
        .bind(schema)
        .fetch_all(&pool)
        .await?;
        assert_eq!(functions.len(), 5);
        for (_, definition) in &functions {
            let old = definition.replace(
                "                        -- Keep this bounded claim on indexed probes, not a full-partition hash.\n                        OFFSET 0\n",
                "",
            );
            assert_ne!(&old, definition, "fixture must restore the old lookup");
            sqlx::raw_sql(audited_sql(old)).execute(&pool).await?;
        }

        // Functions that share a claim function's name but are not one the
        // installer made, each with an unfenced check: an overload of the
        // runtime and of slot 0, and a slot beyond the installed slot count.
        // The migration must leave all three alone.
        let decoy_body = format!(
            "RETURNS boolean LANGUAGE sql AS $decoy$
             SELECT EXISTS (
                 SELECT 1 FROM {schema}.ready_tombstones AS tomb, {schema}.ready_entries AS ready
                 WHERE tomb.queue = ready.queue
                   AND tomb.lane_seq = ready.lane_seq
             ) $decoy$;"
        );
        sqlx::raw_sql(audited_sql(format!(
            "CREATE FUNCTION {schema}.claim_ready_runtime(p_queue text) {decoy_body}
             CREATE FUNCTION {schema}.claim_ready_slot_0(p_queue text) {decoy_body}
             CREATE FUNCTION {schema}.claim_ready_slot_7(
                 text, int, bigint, smallint, smallint, bigint, bigint,
                 double precision, timestamptz, timestamptz, double precision, bigint
             ) {decoy_body}"
        )))
        .execute(&pool)
        .await?;
        let decoys = "SELECT p.oid::bigint, pg_get_functiondef(p.oid)
             FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname = $1 AND p.prorettype = 'boolean'::regtype
               AND (p.proname = 'claim_ready_runtime'
                    OR p.proname ~ '^claim_ready_slot_[0-9]+$')
             ORDER BY p.oid";
        let decoys_before: Vec<(i64, String)> =
            sqlx::query_as(decoys).bind(schema).fetch_all(&pool).await?;
        assert_eq!(decoys_before.len(), 3);

        for (_, _, sql) in migrations::migration_sql_range(59, 60) {
            sqlx::raw_sql(audited_sql(sql)).execute(&pool).await?;
        }

        let decoys_after: Vec<(i64, String)> =
            sqlx::query_as(decoys).bind(schema).fetch_all(&pool).await?;
        assert_eq!(
            decoys_before, decoys_after,
            "{schema}: only the installed claim functions are rewritten"
        );
        sqlx::raw_sql(audited_sql(format!(
            "DROP FUNCTION {schema}.claim_ready_runtime(text);
             DROP FUNCTION {schema}.claim_ready_slot_0(text);
             DROP FUNCTION {schema}.claim_ready_slot_7(
                 text, int, bigint, smallint, smallint, bigint, bigint,
                 double precision, timestamptz, timestamptz, double precision, bigint
             );"
        )))
        .execute(&pool)
        .await?;
        for (oid, _) in &functions {
            let upgraded: String = sqlx::query_scalar("SELECT pg_get_functiondef($1::oid)")
                .bind(*oid)
                .fetch_one(&pool)
                .await?;
            assert!(upgraded.contains("AND tomb.lane_seq = ready.lane_seq OFFSET 0"));
            sqlx::raw_sql(include_str!(
                "../migrations/v060_correlated_claim_tombstones.sql"
            ))
            .execute(&pool)
            .await?;
            let replayed: String = sqlx::query_scalar("SELECT pg_get_functiondef($1::oid)")
                .bind(*oid)
                .fetch_one(&pool)
                .await?;
            assert_eq!(
                upgraded, replayed,
                "migration replay must preserve the body"
            );
        }

        for mode in ["force_custom_plan", "force_generic_plan"] {
            let mut tx = pool.begin().await?;
            sqlx::raw_sql(audited_sql(format!(
                "SET LOCAL plan_cache_mode = {mode};
                 SET LOCAL work_mem = '125MB'; SET LOCAL random_page_cost = 1.2;"
            )))
            .execute(&mut *tx)
            .await?;
            let claimed: Vec<i64> = sqlx::query_scalar(audited_sql(format!(
                "SELECT lane_seq FROM {schema}.claim_ready_runtime('probe',32,0::float8,0::float8)"
            )))
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
