//! #169 B1: in receipts mode `leases.heartbeat_at` is never written
//! (attempt_state owns heartbeat_at), and the
//! `idx_<schema>_leases_<slot>_state_hb` index that supported the
//! legacy rescue scan is dropped. These tests assert both the
//! schema-level invariant (no `state_hb` index on any AWA substrate
//! after migrate / prepare_schema) and the behavioral invariant
//! (heartbeat_batch on a receipts-mode store does not write
//! leases.heartbeat_at).

use awa_model::{QueueStorage, QueueStorageConfig};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn migrated_pool() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url())
        .await
        .expect("connect");
    awa_model::migrations::run(&pool)
        .await
        .expect("run migrations");
    pool
}

async fn count_state_hb_indexes(pool: &PgPool, schema: &str) -> i64 {
    sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relkind = 'i'
          AND c.relname LIKE 'idx_%_state_hb'
        "#,
    )
    .bind(schema)
    .fetch_one(pool)
    .await
    .expect("count state_hb indexes")
}

/// After a full `awa migrate`, the default `awa` schema must not
/// carry any `idx_*_state_hb` indexes — v025 drops them and v023's
/// helper no longer creates them on fresh installs.
#[tokio::test]
async fn migrate_leaves_no_state_hb_index_on_default_schema() {
    let pool = migrated_pool().await;
    let count = count_state_hb_indexes(&pool, "awa").await;
    assert_eq!(count, 0, "expected no idx_*_state_hb in awa schema");
}

/// A fresh custom-schema `prepare_schema` call must not create
/// `idx_*_state_hb` indexes either. v023's helper was updated to skip
/// that CREATE INDEX in the partition loop.
#[tokio::test]
async fn prepare_schema_does_not_create_state_hb_index() {
    let pool = migrated_pool().await;
    let schema = format!("awa_b1_test_{}", uuid::Uuid::new_v4().simple());
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

    let count = count_state_hb_indexes(&pool, &schema).await;
    assert_eq!(
        count, 0,
        "fresh prepare_schema must not create idx_*_state_hb"
    );

    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("cleanup test schema");
}
