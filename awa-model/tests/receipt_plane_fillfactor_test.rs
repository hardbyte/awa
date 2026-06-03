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

/// Custom schemas register themselves in `awa.runtime_storage_backends`
/// when activated. v024's DO block iterates that table and applies the
/// fillfactor to every registered schema. This test stamps a fake
/// registration row, calls `awa.apply_receipt_plane_fillfactor` (the
/// v024 helper, which is also what operators run for ad-hoc custom
/// schemas), and asserts the partitions land at fillfactor=50.
#[tokio::test]
async fn apply_receipt_plane_fillfactor_helper_covers_custom_schemas() {
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

    // The install function in pg_proc on an existing DB still has the
    // pre-v024 body (no in-loop fillfactor ALTER), so a fresh
    // `install_queue_storage_substrate` call leaves partitions at the
    // default fillfactor=100. v024's apply_... helper is what closes
    // that gap.
    sqlx::query("SELECT awa.install_queue_storage_substrate($1)")
        .bind(&schema)
        .execute(&pool)
        .await
        .expect("install substrate via helper");
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
            "{schema}.{name} reloptions missing fillfactor=50: {opts:?}"
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
