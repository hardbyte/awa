use awa_model::{migrations, PruneOutcome, QueueStorage, QueueStorageConfig, RotateOutcome};
use sqlx::PgPool;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const PATCH: &str = "live_slot_claims";
const MARKER: &str = "awa:claim-routing:v2";
/// `claim_ready_runtime` as 0.6.17 installs it with receipts, for schema
/// `__SCHEMA__`.
const RELEASED_BODY: &str = include_str!("fixtures/claim_ready_runtime_0_6_17.sql");
const DISPATCHER: &str = "claim_ready_runtime(text,bigint,float8,float8)";

async fn patch_pending(pool: &PgPool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Ok(migrations::schema_patch_status(pool)
        .await?
        .into_iter()
        .find(|status| status.name == PATCH)
        .expect("the patch is registered")
        .needs_repair)
}

fn patch_sql() -> &'static str {
    migrations::SCHEMA_PATCHES
        .iter()
        .find(|patch| patch.name == PATCH)
        .expect("the patch is registered")
        .sql
}

fn store(schema: &str, queue_slots: usize, receipts: bool) -> QueueStorage {
    QueueStorage::new(QueueStorageConfig {
        schema: schema.into(),
        queue_slot_count: queue_slots,
        lease_slot_count: 2,
        claim_slot_count: 2,
        lease_claim_receipts: receipts,
        ..Default::default()
    })
    .expect("valid queue storage config")
}

/// Every claim function of `schema` with its oid, definition and ACL.
async fn claim_functions(
    pool: &PgPool,
    schema: &str,
) -> Result<Vec<(String, i64, String, Option<String>)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT p.proname::text, p.oid::bigint, pg_get_functiondef(p.oid), p.proacl::text
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = $1 AND (p.proname = 'claim_ready_runtime' OR p.proname ~ '^claim_slot_[0-9]+$')
         ORDER BY p.proname",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
}

/// Put back the claim function a 0.6.17 install leaves behind.
async fn install_released_body(pool: &PgPool, schema: &str) -> Result<(), sqlx::Error> {
    let slot_functions: Vec<String> = sqlx::query_scalar(
        "SELECT p.oid::regprocedure::text
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = $1 AND p.proname ~ '^claim_slot_[0-9]+$'",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;
    for function in slot_functions {
        sqlx::raw_sql(&format!("DROP FUNCTION {function}"))
            .execute(pool)
            .await?;
    }
    sqlx::raw_sql(&RELEASED_BODY.replace("__SCHEMA__", schema))
        .execute(pool)
        .await?;
    Ok(())
}

async fn claim_and_complete(store: &QueueStorage, pool: &PgPool, count: i64) -> TestResult {
    store.enqueue_batch(pool, "probe", 2, count).await?;
    for deadline in [Duration::ZERO, Duration::from_secs(30)] {
        let claimed = store
            .claim_runtime_batch(pool, "probe", count / 2, deadline)
            .await?;
        assert_eq!(claimed.len() as i64, count / 2, "{}", store.schema());
        store.complete_runtime_batch(pool, &claimed).await?;
    }
    Ok(())
}

/// The patch replaces a released claim function in place, keeps its owner
/// and privileges, gives each per-slot function the same privileges, and
/// replays as a no-op. A 0.6.18 prune works against the released function
/// as well as the patched one.
#[sqlx::test(migrations = false)]
async fn live_slot_claims_patch_refreshes_released_claim_functions(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    assert!(patch_pending(&pool).await?.is_empty());

    let schema = "live_slot_receipts";
    let store = store(schema, 4, true);
    store.install(&pool).await?;
    let installed = claim_functions(&pool, schema).await?;
    assert_eq!(installed.len(), 5, "a dispatcher and one function per slot");

    install_released_body(&pool, schema).await?;
    sqlx::raw_sql(&format!(
        "REVOKE EXECUTE ON FUNCTION {schema}.{DISPATCHER} FROM PUBLIC;
         GRANT EXECUTE ON FUNCTION {schema}.{DISPATCHER} TO pg_monitor"
    ))
    .execute(&pool)
    .await?;
    assert_eq!(
        patch_pending(&pool).await?,
        vec![format!("claim function {schema}.claim_ready_runtime")],
        "the probe names the released function"
    );
    let (_, oid, _, acl) = claim_functions(&pool, schema).await?.remove(0);

    // A 0.6.18 prune against the released function: the barrier is taken,
    // nothing reads it, and the slot is retired as on 0.6.17.
    claim_and_complete(&store, &pool, 4).await?;
    assert!(matches!(
        store.rotate(&pool).await?,
        RotateOutcome::Rotated { slot: 1, .. }
    ));
    let outcome = store.prune_oldest(&pool, Duration::ZERO).await?;
    assert!(
        matches!(outcome, PruneOutcome::Pruned { slot: 0, .. }),
        "prune against the released function: {outcome:?}"
    );

    sqlx::raw_sql(patch_sql()).execute(&pool).await?;
    assert!(patch_pending(&pool).await?.is_empty());
    let patched = claim_functions(&pool, schema).await?;
    assert_eq!(patched.len(), 5);
    assert_eq!(patched[0].1, oid, "the dispatcher is replaced in place");
    assert_eq!(patched[0].3, acl, "the dispatcher keeps its privileges");
    for (name, _, definition, slot_acl) in &patched {
        assert!(definition.contains(MARKER), "{name} carries the marker");
        assert_eq!(slot_acl, &acl, "{name} carries the dispatcher's privileges");
    }
    assert_eq!(
        patched
            .iter()
            .map(|(name, _, definition, _)| (name.clone(), definition.clone()))
            .collect::<Vec<_>>(),
        installed
            .iter()
            .map(|(name, _, definition, _)| (name.clone(), definition.clone()))
            .collect::<Vec<_>>(),
        "the patch writes what the installer writes"
    );

    sqlx::raw_sql(patch_sql()).execute(&pool).await?;
    assert_eq!(
        claim_functions(&pool, schema).await?,
        patched,
        "replay is a no-op"
    );

    claim_and_complete(&store, &pool, 4).await?;
    Ok(())
}

/// A later 0.7 reinstall detects receipts mode from the dispatcher's text:
/// `INSERT INTO <schema>.lease_claims` or `INSERT INTO <schema>.lease_claim_batches`.
/// Every generated dispatcher must carry both, and the routing marker.
#[sqlx::test(migrations = false)]
async fn every_generated_dispatcher_names_its_receipt_inserts(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    for (schema, slots) in [("awa", 16), ("slots_4", 4), ("slots_32", 32)] {
        if schema != "awa" {
            store(schema, slots, true).install(&pool).await?;
        }
        let functions = claim_functions(&pool, schema).await?;
        assert_eq!(functions.len(), slots + 1, "{schema}");
        let dispatcher = &functions[0].2;
        for needle in [
            format!("INSERT INTO {schema}.lease_claims"),
            format!("INSERT INTO {schema}.lease_claim_batches"),
            MARKER.to_string(),
        ] {
            assert!(dispatcher.contains(&needle), "{schema}: missing {needle}");
        }
        if schema != "awa" {
            claim_and_complete(&store(schema, slots, true), &pool, 4).await?;
        }
    }
    Ok(())
}

/// A schema with lease_claim_receipts = FALSE keeps its claim function; the
/// probe does not report it and the patch does not touch it.
#[sqlx::test(migrations = false)]
async fn live_slot_claims_leave_legacy_schemas_alone(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    let schema = "live_slot_legacy";
    let store = store(schema, 4, false);
    store.install(&pool).await?;
    let before = claim_functions(&pool, schema).await?;
    assert_eq!(before.len(), 1, "no per-slot functions without receipts");
    assert!(!before[0].2.contains(MARKER));
    assert!(patch_pending(&pool).await?.is_empty());
    sqlx::raw_sql(patch_sql()).execute(&pool).await?;
    assert_eq!(claim_functions(&pool, schema).await?, before);
    claim_and_complete(&store, &pool, 4).await?;
    Ok(())
}

/// A migrator that is not the owner of the claim functions but can SET ROLE
/// to it creates them as the owner and gets its own role back. One that
/// cannot is refused, and nothing changes.
#[sqlx::test(migrations = false)]
async fn live_slot_claims_patch_runs_as_the_function_owner(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    let schema = "live_slot_owned";
    sqlx::raw_sql(
        "DROP ROLE IF EXISTS live_slot_owner; DROP ROLE IF EXISTS live_slot_migrator;
         DROP ROLE IF EXISTS live_slot_stranger;
         CREATE ROLE live_slot_owner; CREATE ROLE live_slot_migrator;
         CREATE ROLE live_slot_stranger;
         GRANT live_slot_owner TO live_slot_migrator;
         GRANT USAGE ON SCHEMA awa TO live_slot_migrator, live_slot_stranger;
         GRANT EXECUTE ON FUNCTION awa.refresh_claim_functions(text, boolean)
             TO live_slot_migrator, live_slot_stranger;",
    )
    .execute(&pool)
    .await?;
    store(schema, 4, true).install(&pool).await?;
    sqlx::raw_sql(&format!(
        "ALTER SCHEMA {schema} OWNER TO live_slot_owner;
         GRANT USAGE ON SCHEMA {schema} TO live_slot_stranger;
         GRANT SELECT ON {schema}.queue_ring_state TO live_slot_migrator, live_slot_stranger;
         ALTER FUNCTION {schema}.{DISPATCHER} OWNER TO live_slot_owner;"
    ))
    .execute(&pool)
    .await?;
    install_released_body(&pool, schema).await?;

    let mut conn = pool.acquire().await?;
    sqlx::raw_sql("SET ROLE live_slot_stranger")
        .execute(&mut *conn)
        .await?;
    let refused = sqlx::query(&format!(
        "SELECT awa.refresh_claim_functions('{schema}', TRUE)"
    ))
    .execute(&mut *conn)
    .await
    .expect_err("a role that cannot act as the owner is refused");
    assert!(
        refused.to_string().contains("cannot act as that role"),
        "{refused}"
    );

    sqlx::raw_sql("RESET ROLE; SET ROLE live_slot_migrator")
        .execute(&mut *conn)
        .await?;
    let mut tx = sqlx::Connection::begin(&mut *conn).await?;
    sqlx::query(&format!(
        "SELECT awa.refresh_claim_functions('{schema}', TRUE)"
    ))
    .execute(&mut *tx)
    .await?;
    let (role, user): (String, String) =
        sqlx::query_as("SELECT current_setting('role'), current_user::text")
            .fetch_one(&mut *tx)
            .await?;
    assert_eq!(
        (role.as_str(), user.as_str()),
        ("live_slot_migrator", "live_slot_migrator"),
        "the caller's role is restored"
    );
    tx.commit().await?;
    sqlx::raw_sql("RESET ROLE").execute(&mut *conn).await?;
    drop(conn);

    let owners: Vec<(String, String)> = sqlx::query_as(
        "SELECT p.proname::text, pg_get_userbyid(p.proowner)::text
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = $1 AND (p.proname = 'claim_ready_runtime' OR p.proname ~ '^claim_slot_[0-9]+$')",
    )
    .bind(schema)
    .fetch_all(&pool)
    .await?;
    assert_eq!(owners.len(), 5);
    assert!(
        owners.iter().all(|(_, owner)| owner == "live_slot_owner"),
        "{owners:?}"
    );
    assert!(patch_pending(&pool).await?.is_empty());
    Ok(())
}

/// The probe reports a refreshed schema whose per-slot function is missing,
/// and the patch puts it back.
#[sqlx::test(migrations = false)]
async fn live_slot_claims_probe_names_a_missing_slot_function(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    let schema = "live_slot_missing";
    store(schema, 4, true).install(&pool).await?;
    sqlx::raw_sql(&format!(
        "DROP FUNCTION {schema}.claim_slot_2(text,bigint,smallint,smallint,text,bigint,bigint,boolean,float8,float8)"
    ))
    .execute(&pool)
    .await?;
    assert_eq!(
        patch_pending(&pool).await?,
        vec![format!("claim function {schema}.claim_slot_2")]
    );
    sqlx::raw_sql(patch_sql()).execute(&pool).await?;
    assert!(patch_pending(&pool).await?.is_empty());
    claim_and_complete(&store(schema, 4, true), &pool, 4).await?;
    Ok(())
}

/// While the retire barrier of the slot prune targets next is held, claims
/// leave that slot out; once it is released the slot is claimable again, so
/// a row left in it is never lost.
#[sqlx::test(migrations = false)]
async fn claims_skip_the_slot_being_retired_only_while_its_barrier_is_held(
    pool: PgPool,
) -> TestResult {
    migrations::run(&pool).await?;
    let schema = "live_slot_barrier";
    let store = store(schema, 4, true);
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
