use awa_model::{migrations, PruneOutcome, QueueStorage, QueueStorageConfig, RotateOutcome};
use sqlx::PgPool;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const PATCH: &str = "live_slot_claims";
const BARRIER_START: &str = "            -- The slot queue prune retires next.";
const BARRIER_END: &str = "                ORDER BY slots.slot\n            );\n\n";
const SLOT_FILTERS: [&str; 4] = [
    "segment.ready_slot = ANY(v_live_slots)\n                      AND ",
    "segment.ready_slot = ANY(v_live_slots)\n                      AND ",
    "segment.ready_slot = ANY(v_live_slots)\n                      AND ",
    "later_segment.ready_slot = ANY(v_live_slots)\n                      AND ",
];

async fn patch_pending(pool: &PgPool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Ok(migrations::schema_patch_status(pool)
        .await?
        .into_iter()
        .find(|status| status.name == PATCH)
        .expect("the patch is registered")
        .needs_repair)
}

/// The definition a 0.6.17 install left behind: a cached plan, no retire
/// barrier and no slot filter on the ready_segments scans.
fn released_definition(definition: &str) -> String {
    let start = definition
        .find(BARRIER_START)
        .expect("current body has the barrier");
    let end = start
        + definition[start..]
            .find(BARRIER_END)
            .expect("barrier block ends")
        + BARRIER_END.len();
    let mut old = format!("{}{}", &definition[..start], &definition[end..]);
    let filters_before = old.matches("ANY(v_live_slots)").count();
    assert_eq!(filters_before, SLOT_FILTERS.len());
    for filter in SLOT_FILTERS {
        old = old.replacen(filter, "", 1);
    }
    assert!(!old.contains("v_live_slots)"));
    let set_line = " SET plan_cache_mode TO 'force_custom_plan'\n";
    assert!(old.contains(set_line), "{old}");
    old.replace(set_line, "")
}

async fn definition(pool: &PgPool, schema: &str) -> Result<(i64, String), sqlx::Error> {
    sqlx::query_as(
        "SELECT p.oid::bigint, pg_get_functiondef(p.oid)
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = $1 AND p.proname = 'claim_ready_runtime'",
    )
    .bind(schema)
    .fetch_one(pool)
    .await
}

/// The patch refreshes the claim function of every queue-storage schema from
/// a released body, keeping the schema's receipts mode, its privileges and
/// its object identity, and replays as a no-op. A prune that takes the retire
/// barrier works against the released body as well as the patched one.
#[sqlx::test(migrations = false)]
async fn live_slot_claims_patch_refreshes_released_claim_functions(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    assert!(patch_pending(&pool).await?.is_empty());
    let patch = migrations::SCHEMA_PATCHES
        .iter()
        .find(|patch| patch.name == PATCH)
        .expect("the patch is registered");

    for receipts in [true, false] {
        let schema = if receipts {
            "live_slot_receipts"
        } else {
            "live_slot_legacy"
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
        sqlx::raw_sql(&format!(
            "GRANT EXECUTE ON FUNCTION {schema}.claim_ready_runtime(text,bigint,float8,float8) TO pg_monitor"
        ))
        .execute(&pool)
        .await?;

        let (oid, current) = definition(&pool, schema).await?;
        sqlx::raw_sql(&released_definition(&current))
            .execute(&pool)
            .await?;
        assert_eq!(
            patch_pending(&pool).await?,
            vec![format!("claim function {schema}.claim_ready_runtime")],
            "the probe names the released function"
        );

        // A 0.6.18 prune against the released function: the barrier is
        // taken, nothing reads it, and the slot is retired as before.
        store.enqueue_batch(&pool, "probe", 2, 4).await?;
        let claimed = store
            .claim_runtime_batch(&pool, "probe", 4, Duration::from_secs(30))
            .await?;
        assert_eq!(claimed.len(), 4);
        store.complete_runtime_batch(&pool, &claimed).await?;
        assert!(matches!(
            store.rotate(&pool).await?,
            RotateOutcome::Rotated { slot: 1, .. }
        ));
        let outcome = store.prune_oldest(&pool, Duration::ZERO).await?;
        assert!(
            matches!(outcome, PruneOutcome::Pruned { slot: 0, .. }),
            "{schema}: prune against the released function: {outcome:?}"
        );

        let acl_before: Option<String> =
            sqlx::query_scalar("SELECT proacl::text FROM pg_proc WHERE oid = $1::oid")
                .bind(oid)
                .fetch_one(&pool)
                .await?;
        sqlx::raw_sql(patch.sql).execute(&pool).await?;
        assert!(patch_pending(&pool).await?.is_empty());
        let (patched_oid, patched) = definition(&pool, schema).await?;
        assert_eq!(patched_oid, oid, "the function is replaced in place");
        assert_eq!(patched, current, "the patch restores the current body");
        assert_eq!(
            patched.contains("claim_attempt_batches AS ("),
            receipts,
            "the receipts mode is kept"
        );
        let acl_after: Option<String> =
            sqlx::query_scalar("SELECT proacl::text FROM pg_proc WHERE oid = $1::oid")
                .bind(oid)
                .fetch_one(&pool)
                .await?;
        assert_eq!(acl_after, acl_before, "privileges are kept");

        sqlx::raw_sql(patch.sql).execute(&pool).await?;
        assert_eq!(
            definition(&pool, schema).await?.1,
            patched,
            "replay is a no-op"
        );

        store.enqueue_batch(&pool, "probe", 2, 4).await?;
        let claimed = store
            .claim_runtime_batch(&pool, "probe", 4, Duration::from_secs(30))
            .await?;
        assert_eq!(claimed.len(), 4, "{schema}: the patched function claims");
        store.complete_runtime_batch(&pool, &claimed).await?;
    }
    Ok(())
}

/// While the retire barrier of the slot prune targets next is held, the
/// claim function leaves that slot out; once it is released the slot is
/// claimable again, so a late row is never lost.
#[sqlx::test(migrations = false)]
async fn claims_skip_the_slot_being_retired_only_while_its_barrier_is_held(
    pool: PgPool,
) -> TestResult {
    migrations::run(&pool).await?;
    let schema = "live_slot_barrier";
    let store = QueueStorage::new(QueueStorageConfig {
        schema: schema.into(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })?;
    store.install(&pool).await?;
    store.enqueue_batch(&pool, "late", 2, 2).await?;
    assert!(matches!(
        store.rotate(&pool).await?,
        RotateOutcome::Rotated { slot: 1, .. }
    ));

    let mut holder = pool.begin().await?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended(format('awa.queue_storage.retire:%s:queue:%s', $1::text, 0), 0))",
    )
    .bind(schema)
    .execute(&mut *holder)
    .await?;
    let skipped = store
        .claim_runtime_batch(&pool, "late", 2, Duration::ZERO)
        .await?;
    assert!(skipped.is_empty(), "the slot being retired is left out");
    holder.rollback().await?;

    let claimed = store
        .claim_runtime_batch(&pool, "late", 2, Duration::ZERO)
        .await?;
    assert_eq!(
        claimed.len(),
        2,
        "the rows are claimable once the barrier is gone"
    );
    assert!(claimed.iter().all(|job| job.claim.ready_slot == 0));
    Ok(())
}
