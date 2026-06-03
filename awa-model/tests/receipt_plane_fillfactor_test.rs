//! Regression test for #169: receipt-plane partitions must carry
//! `fillfactor=50` (and the tightened autovacuum knobs) after a full
//! `awa migrate`. The two UPDATE-heavy partitioned tables —
//! `awa.leases` and `awa.lease_claims` — are the only ones that need
//! the on-page slack; the insert+delete-only siblings (`ready_entries`,
//! `done_entries`, `lease_claim_closures`) intentionally stay at the
//! default fillfactor=100.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn migrated_pool() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .expect("connect");
    awa_model::migrations::run(&pool)
        .await
        .expect("run migrations");
    pool
}

async fn reloptions_for_partitions(pool: &PgPool, parent: &str) -> Vec<(String, Vec<String>)> {
    sqlx::query_as::<_, (String, Option<Vec<String>>)>(
        r#"
        SELECT c.relname, c.reloptions::text[]
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        JOIN pg_inherits AS inh ON inh.inhrelid = c.oid
        WHERE n.nspname = 'awa'
          AND inh.inhparent = ($1 || '.' || $2)::regclass
          AND c.relkind = 'r'
        ORDER BY c.relname
        "#,
    )
    .bind("awa")
    .bind(parent)
    .fetch_all(pool)
    .await
    .expect("read reloptions")
    .into_iter()
    .map(|(name, opts)| (name, opts.unwrap_or_default()))
    .collect()
}

fn has_option(opts: &[String], key: &str, expected: &str) -> bool {
    opts.iter()
        .any(|o| o.split_once('=') == Some((key, expected)))
}

#[tokio::test]
async fn leases_partitions_carry_fillfactor_50() {
    let pool = migrated_pool().await;
    let partitions = reloptions_for_partitions(&pool, "leases").await;
    assert!(
        !partitions.is_empty(),
        "expected at least one awa.leases partition after migrate"
    );
    for (name, opts) in &partitions {
        assert!(
            has_option(opts, "fillfactor", "50"),
            "{name} reloptions missing fillfactor=50: {opts:?}"
        );
        assert!(
            has_option(opts, "autovacuum_vacuum_scale_factor", "0.0"),
            "{name} reloptions missing autovacuum_vacuum_scale_factor=0.0: {opts:?}"
        );
        assert!(
            has_option(opts, "autovacuum_vacuum_threshold", "200"),
            "{name} reloptions missing autovacuum_vacuum_threshold=200: {opts:?}"
        );
    }
}

#[tokio::test]
async fn lease_claims_partitions_carry_fillfactor_50() {
    let pool = migrated_pool().await;
    let partitions = reloptions_for_partitions(&pool, "lease_claims").await;
    assert!(
        !partitions.is_empty(),
        "expected at least one awa.lease_claims partition after migrate"
    );
    for (name, opts) in &partitions {
        assert!(
            has_option(opts, "fillfactor", "50"),
            "{name} reloptions missing fillfactor=50: {opts:?}"
        );
    }
}

/// Simulates the "upgraded DB, post-v024, operator prepares a new
/// custom schema" scenario: the install function still cached in
/// pg_proc may have the pre-v024 body that leaves partitions at the
/// default fillfactor=100. v024 introduces
/// `awa.apply_receipt_plane_fillfactor` precisely to close that gap.
///
/// To prove the helper is actually what restores the reloptions (and
/// the test isn't passing vacuously because the install body in *this*
/// checkout already sets them), we install the substrate, RESET all
/// the partition reloptions back to the default to simulate the
/// pre-v024 helper body, then call the helper and assert the settings
/// come back.
#[tokio::test]
async fn apply_receipt_plane_fillfactor_helper_restores_reset_partitions() {
    let pool = migrated_pool().await;
    let schema = format!("awa_fillfactor_test_{}", uuid::Uuid::new_v4().simple());

    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("clean any prior schema");
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&pool)
        .await
        .expect("create test schema");

    sqlx::query("SELECT awa.install_queue_storage_substrate($1)")
        .bind(&schema)
        .execute(&pool)
        .await
        .expect("install substrate via helper");

    // Simulate the pre-v024 install body: strip every reloption the
    // v024 helper is meant to restore. We discover the partitions via
    // pg_inherits so the simulation works against any slot count.
    let partition_names: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT (n.nspname || '.' || c.relname)::text
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        JOIN pg_inherits AS inh ON inh.inhrelid = c.oid
        WHERE n.nspname = $1
          AND (inh.inhparent = ($1 || '.leases')::regclass
            OR inh.inhparent = ($1 || '.lease_claims')::regclass)
          AND c.relkind = 'r'
        "#,
    )
    .bind(&schema)
    .fetch_all(&pool)
    .await
    .expect("list custom-schema partitions");
    assert!(
        !partition_names.is_empty(),
        "expected at least one partition under {schema}"
    );
    for name in &partition_names {
        sqlx::query(&format!(
            "ALTER TABLE {name} RESET ( \
             fillfactor, \
             autovacuum_vacuum_scale_factor, \
             autovacuum_vacuum_threshold, \
             autovacuum_vacuum_cost_limit, \
             autovacuum_vacuum_cost_delay)"
        ))
        .execute(&pool)
        .await
        .expect("reset partition reloptions");
    }

    // Confirm the RESET actually stripped the reloptions — otherwise
    // the subsequent helper call would pass vacuously.
    let post_reset_with_fillfactor: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        JOIN pg_inherits AS inh ON inh.inhrelid = c.oid
        WHERE n.nspname = $1
          AND (inh.inhparent = ($1 || '.leases')::regclass
            OR inh.inhparent = ($1 || '.lease_claims')::regclass)
          AND c.relkind = 'r'
          AND c.reloptions IS NOT NULL
          AND 'fillfactor=50' = ANY(c.reloptions)
        "#,
    )
    .bind(&schema)
    .fetch_one(&pool)
    .await
    .expect("count partitions still carrying fillfactor=50");
    assert_eq!(
        post_reset_with_fillfactor, 0,
        "RESET should have cleared fillfactor on all partitions before \
         the helper call so the assertion below proves the helper works"
    );

    sqlx::query("SELECT awa.apply_receipt_plane_fillfactor($1)")
        .bind(&schema)
        .execute(&pool)
        .await
        .expect("apply fillfactor helper");

    let partitions: Vec<(String, Option<Vec<String>>)> = sqlx::query_as(
        r#"
        SELECT c.relname, c.reloptions::text[]
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        JOIN pg_inherits AS inh ON inh.inhrelid = c.oid
        WHERE n.nspname = $1
          AND (inh.inhparent = ($1 || '.leases')::regclass
            OR inh.inhparent = ($1 || '.lease_claims')::regclass)
          AND c.relkind = 'r'
        ORDER BY c.relname
        "#,
    )
    .bind(&schema)
    .fetch_all(&pool)
    .await
    .expect("read custom-schema reloptions");

    assert!(
        !partitions.is_empty(),
        "expected leases and lease_claims partitions in {schema}"
    );
    for (name, opts) in &partitions {
        let opts = opts.clone().unwrap_or_default();
        assert!(
            has_option(&opts, "fillfactor", "50"),
            "{schema}.{name} reloptions missing fillfactor=50 after helper call: {opts:?}"
        );
    }

    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("cleanup test schema");
}

/// Prove that `QueueStorage::prepare_schema`'s call to
/// `apply_receipt_plane_fillfactor` is what restores tunings on a
/// re-prepare against an already-installed substrate. This is the
/// scenario that matters on the upgrade path: the install helper
/// cached in pg_proc may be the pre-v024 body that doesn't set
/// fillfactor in its CREATE TABLE loops, but
/// `install_queue_storage_substrate` is idempotent — on a re-prepare
/// it skips the CREATE TABLE IF NOT EXISTS branch entirely. So even
/// if the helper *did* carry the in-loop ALTERs (as on a fresh
/// install in this checkout), they wouldn't re-fire on re-prepare.
/// What does fire is the orchestrator's explicit
/// `apply_receipt_plane_fillfactor` call after install.
///
/// Sequence:
///   1. `prepare_schema` (creates substrate, leaves at fillfactor=50).
///   2. RESET reloptions on every partition to simulate the
///      pre-v024 state.
///   3. `prepare_schema` again. If the orchestrator hook is removed,
///      this becomes a no-op and the partitions stay reset.
///   4. Assert fillfactor=50 is back.
#[tokio::test]
async fn prepare_schema_reapplies_receipt_plane_fillfactor_on_reprepare() {
    use awa_model::{QueueStorage, QueueStorageConfig};

    let pool = migrated_pool().await;
    let schema = format!("awa_prepare_test_{}", uuid::Uuid::new_v4().simple());
    let store = QueueStorage::new(QueueStorageConfig {
        schema: schema.clone(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })
    .expect("construct QueueStorage");

    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("clean any prior schema");

    store
        .prepare_schema(&pool)
        .await
        .expect("first prepare_schema should succeed");

    // Simulate the pre-v024 state: strip every reloption the v024 hook
    // is meant to restore. Discover partitions via pg_inherits so the
    // simulation works regardless of slot count.
    let partition_names: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT (n.nspname || '.' || c.relname)::text
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        JOIN pg_inherits AS inh ON inh.inhrelid = c.oid
        WHERE n.nspname = $1
          AND (inh.inhparent = ($1 || '.leases')::regclass
            OR inh.inhparent = ($1 || '.lease_claims')::regclass)
          AND c.relkind = 'r'
        "#,
    )
    .bind(&schema)
    .fetch_all(&pool)
    .await
    .expect("list partitions");
    assert!(!partition_names.is_empty(), "expected partitions");
    for name in &partition_names {
        sqlx::query(&format!(
            "ALTER TABLE {name} RESET ( \
             fillfactor, \
             autovacuum_vacuum_scale_factor, \
             autovacuum_vacuum_threshold, \
             autovacuum_vacuum_cost_limit, \
             autovacuum_vacuum_cost_delay)"
        ))
        .execute(&pool)
        .await
        .expect("reset partition reloptions");
    }

    // Sanity-check the reset took effect — without this the final
    // assertion could pass vacuously if the RESET didn't actually clear
    // anything.
    let after_reset_with_fillfactor: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        JOIN pg_inherits AS inh ON inh.inhrelid = c.oid
        WHERE n.nspname = $1
          AND (inh.inhparent = ($1 || '.leases')::regclass
            OR inh.inhparent = ($1 || '.lease_claims')::regclass)
          AND c.relkind = 'r'
          AND c.reloptions IS NOT NULL
          AND 'fillfactor=50' = ANY(c.reloptions)
        "#,
    )
    .bind(&schema)
    .fetch_one(&pool)
    .await
    .expect("count partitions still carrying fillfactor=50");
    assert_eq!(
        after_reset_with_fillfactor, 0,
        "RESET must have cleared fillfactor=50 on every partition; \
         otherwise the orchestrator's contribution can't be observed"
    );

    // Re-prepare. install_queue_storage_substrate is idempotent and the
    // CREATE TABLE IF NOT EXISTS branches don't re-fire on existing
    // partitions, so only the orchestrator's explicit
    // apply_receipt_plane_fillfactor call can restore the reloptions.
    store
        .prepare_schema(&pool)
        .await
        .expect("second prepare_schema should succeed");

    let partitions: Vec<(String, Option<Vec<String>>)> = sqlx::query_as(
        r#"
        SELECT c.relname, c.reloptions::text[]
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        JOIN pg_inherits AS inh ON inh.inhrelid = c.oid
        WHERE n.nspname = $1
          AND (inh.inhparent = ($1 || '.leases')::regclass
            OR inh.inhparent = ($1 || '.lease_claims')::regclass)
          AND c.relkind = 'r'
        ORDER BY c.relname
        "#,
    )
    .bind(&schema)
    .fetch_all(&pool)
    .await
    .expect("read partition reloptions after re-prepare");

    for (name, opts) in &partitions {
        let opts = opts.clone().unwrap_or_default();
        assert!(
            has_option(&opts, "fillfactor", "50"),
            "{schema}.{name} reloptions missing fillfactor=50 after re-prepare: {opts:?}"
        );
    }

    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("cleanup test schema");
}

/// Ownership boundary: `apply_receipt_plane_fillfactor` and the v024
/// migration sweep must NOT alter unrelated app-owned schemas that
/// happen to have a partitioned `leases` / `lease_claims` pair.
/// Gating on the AWA-specific `claim_ready_runtime` function signature
/// is what keeps the sweep inside AWA's namespace. This test creates
/// a lookalike schema, calls the helper, and asserts the lookalike
/// partitions stay at the default reloptions.
#[tokio::test]
async fn apply_receipt_plane_fillfactor_skips_non_awa_schemas() {
    let pool = migrated_pool().await;
    let schema = format!("app_owned_test_{}", uuid::Uuid::new_v4().simple());

    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("clean any prior schema");
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&pool)
        .await
        .expect("create app-owned schema");
    sqlx::query(&format!(
        "CREATE TABLE {schema}.leases ( \
         lease_slot INT NOT NULL, id BIGINT NOT NULL, \
         PRIMARY KEY (lease_slot, id) \
         ) PARTITION BY LIST (lease_slot)"
    ))
    .execute(&pool)
    .await
    .expect("create lookalike leases parent");
    sqlx::query(&format!(
        "CREATE TABLE {schema}.leases_0 PARTITION OF {schema}.leases FOR VALUES IN (0)"
    ))
    .execute(&pool)
    .await
    .expect("create lookalike leases partition");
    sqlx::query(&format!(
        "CREATE TABLE {schema}.lease_claims ( \
         claim_slot INT NOT NULL, id BIGINT NOT NULL, \
         PRIMARY KEY (claim_slot, id) \
         ) PARTITION BY LIST (claim_slot)"
    ))
    .execute(&pool)
    .await
    .expect("create lookalike lease_claims parent");
    sqlx::query(&format!(
        "CREATE TABLE {schema}.lease_claims_0 PARTITION OF {schema}.lease_claims FOR VALUES IN (0)"
    ))
    .execute(&pool)
    .await
    .expect("create lookalike lease_claims partition");

    // The helper sees no claim_ready_runtime function in this schema,
    // so it should return without altering anything.
    sqlx::query("SELECT awa.apply_receipt_plane_fillfactor($1)")
        .bind(&schema)
        .execute(&pool)
        .await
        .expect("apply_receipt_plane_fillfactor on unrelated schema");

    let altered: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        JOIN pg_inherits AS inh ON inh.inhrelid = c.oid
        WHERE n.nspname = $1
          AND c.relkind = 'r'
          AND c.reloptions IS NOT NULL
        "#,
    )
    .bind(&schema)
    .fetch_one(&pool)
    .await
    .expect("count altered partitions in lookalike schema");
    assert_eq!(
        altered, 0,
        "apply_receipt_plane_fillfactor must not touch partitions in a \
         schema that lacks the AWA claim_ready_runtime sentinel"
    );

    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("cleanup lookalike schema");
}

/// `ready_entries`, `done_entries`, and `lease_claim_closures` are
/// insert+delete only; lowering their fillfactor would waste pages
/// without restoring any HOT-update behaviour. Assert they stay at the
/// default (no fillfactor set) so a future change that mass-applies
/// fillfactor=50 across all partitioned tables gets caught here.
#[tokio::test]
async fn insert_only_partitions_keep_default_fillfactor() {
    let pool = migrated_pool().await;
    for parent in ["ready_entries", "done_entries", "lease_claim_closures"] {
        let partitions = reloptions_for_partitions(&pool, parent).await;
        assert!(
            !partitions.is_empty(),
            "expected at least one awa.{parent} partition"
        );
        for (name, opts) in &partitions {
            assert!(
                !opts.iter().any(|o| o.starts_with("fillfactor=")),
                "{name} unexpectedly has a fillfactor override (insert-only table): {opts:?}"
            );
        }
    }
}
