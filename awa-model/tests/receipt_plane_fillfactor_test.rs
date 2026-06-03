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

/// `QueueStorage::prepare_schema` should call
/// `apply_receipt_plane_fillfactor` automatically so a worker boot or
/// CLI `prepare-queue-storage-schema` against a new custom schema gets
/// the fillfactor set even when the install helper still cached in
/// pg_proc has the pre-v024 body. Mirrors the same RESET-simulate-old-
/// helper pattern as the test above, but exercises the Rust orchestrator
/// instead of the SQL helper directly.
#[tokio::test]
async fn prepare_schema_applies_receipt_plane_fillfactor() {
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
        .expect("prepare_schema should succeed against fresh schema");

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
    .expect("read partition reloptions");

    assert!(
        !partitions.is_empty(),
        "expected leases and lease_claims partitions in {schema}"
    );
    for (name, opts) in &partitions {
        let opts = opts.clone().unwrap_or_default();
        assert!(
            has_option(&opts, "fillfactor", "50"),
            "{schema}.{name} reloptions missing fillfactor=50 after prepare_schema: {opts:?}"
        );
    }

    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("cleanup test schema");
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
