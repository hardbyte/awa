//! v050 drops the redundant ready-partition lane index only in schemas that
//! custom-schema discovery trusts. A look-alike schema owned by another role
//! keeps its index even when a superuser applies the migration, and a custom
//! schema prepared by the awa owner loses it.

use awa_model::{audited_sql, migrations, QueueStorage, QueueStorageConfig};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Executor, PgPool};
use std::str::FromStr;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const LEGIT_SCHEMA: &str = "legit_lanes";
const DECOY_SCHEMA: &str = "decoy_lanes";
const LEGIT_SLOTS: i32 = 4;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

#[tokio::test]
async fn v050_drops_lane_index_only_in_trusted_schemas() -> TestResult {
    let url = database_url();
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let owner = format!("v050_owner_{suffix}");
    let outsider = format!("v050_outsider_{suffix}");
    let database = format!("v050_db_{suffix}");
    admin
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE ROLE {owner} LOGIN NOSUPERUSER PASSWORD 'v050_test'; \
             CREATE ROLE {outsider} LOGIN NOSUPERUSER PASSWORD 'v050_test';"
        ))))
        .await?;
    admin
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE DATABASE {database} OWNER {owner}"
        ))))
        .await?;

    let options = PgConnectOptions::from_str(&url)?.database(&database);
    let result = exercise(options, &owner, &outsider).await;

    admin
        .execute(sqlx::raw_sql(audited_sql(format!(
            "DROP DATABASE {database} WITH (FORCE)"
        ))))
        .await?;
    admin
        .execute(sqlx::raw_sql(audited_sql(format!(
            "DROP ROLE {outsider}; DROP ROLE {owner};"
        ))))
        .await?;
    admin.close().await;
    result
}

async fn exercise(options: PgConnectOptions, owner: &str, outsider: &str) -> TestResult {
    let superuser_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await?;
    let owner_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone().username(owner).password("v050_test"))
        .await?;
    let outsider_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.username(outsider).password("v050_test"))
        .await?;

    for (_, _, statement) in migrations::migration_sql_range(0, 49) {
        owner_pool
            .execute(sqlx::raw_sql(audited_sql(statement)))
            .await?;
    }
    QueueStorage::new(QueueStorageConfig {
        schema: LEGIT_SCHEMA.into(),
        queue_slot_count: LEGIT_SLOTS as usize,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })?
    .prepare_schema(&owner_pool)
    .await?;
    for slot in 0..LEGIT_SLOTS {
        owner_pool
            .execute(sqlx::raw_sql(audited_sql(format!(
                "CREATE INDEX idx_{LEGIT_SCHEMA}_ready_{slot}_lane_shard \
                 ON {LEGIT_SCHEMA}.ready_entries_{slot} (queue, priority, enqueue_shard, lane_seq)"
            ))))
            .await?;
    }

    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE SCHEMA {DECOY_SCHEMA} AUTHORIZATION {outsider}; \
             GRANT USAGE ON SCHEMA {DECOY_SCHEMA} TO {owner};"
        ))))
        .await?;
    outsider_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            r#"
            CREATE FUNCTION {DECOY_SCHEMA}.claim_ready_runtime(
                TEXT, BIGINT, DOUBLE PRECISION, DOUBLE PRECISION
            ) RETURNS VOID LANGUAGE sql AS $decoy$ SELECT $decoy$;
            CREATE TABLE {DECOY_SCHEMA}.queue_ring_state (
                singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
                slot_count INT NOT NULL
            );
            INSERT INTO {DECOY_SCHEMA}.queue_ring_state (slot_count) VALUES (1);
            CREATE TABLE {DECOY_SCHEMA}.ready_entries (
                ready_slot INT NOT NULL,
                queue TEXT NOT NULL,
                priority SMALLINT NOT NULL,
                enqueue_shard SMALLINT NOT NULL,
                lane_seq BIGINT NOT NULL
            ) PARTITION BY LIST (ready_slot);
            CREATE TABLE {DECOY_SCHEMA}.ready_entries_0
                PARTITION OF {DECOY_SCHEMA}.ready_entries FOR VALUES IN (0);
            CREATE INDEX idx_{DECOY_SCHEMA}_ready_0_lane_shard
                ON {DECOY_SCHEMA}.ready_entries_0 (queue, priority, enqueue_shard, lane_seq);
            "#
        ))))
        .await?;

    assert_eq!(
        lane_indexes(&superuser_pool, LEGIT_SCHEMA).await?,
        LEGIT_SLOTS as i64
    );
    assert_eq!(lane_indexes(&superuser_pool, DECOY_SCHEMA).await?, 1);

    let mut reader = owner_pool.begin().await?;
    reader
        .execute(sqlx::raw_sql(audited_sql(format!(
            "LOCK TABLE {LEGIT_SCHEMA}.ready_entries_1 IN ACCESS SHARE MODE"
        ))))
        .await?;
    superuser_pool.execute("SET lock_timeout = '200ms'").await?;
    let mut timed_out = None;
    for (_, _, statement) in migrations::migration_sql_range(49, 50) {
        if let Err(error) = superuser_pool
            .execute(sqlx::raw_sql(audited_sql(statement)))
            .await
        {
            timed_out = Some(error);
            break;
        }
    }
    let error = timed_out.expect("v050 must not wait past lock_timeout");
    let database_error = error.as_database_error().expect("a database error");
    assert_eq!(database_error.code().as_deref(), Some("55P03"));
    assert!(
        database_error
            .message()
            .contains(&format!("{LEGIT_SCHEMA}.ready_entries_1"))
            && database_error.message().contains("re-run"),
        "the timeout names the partition and says the step can be re-run: {}",
        database_error.message()
    );
    reader.rollback().await?;
    superuser_pool.execute("RESET lock_timeout").await?;
    let version_after_timeout: i32 =
        sqlx::query_scalar("SELECT max(version) FROM awa.schema_version")
            .fetch_one(&superuser_pool)
            .await?;
    assert_eq!(
        version_after_timeout, 49,
        "a timed-out v050 must not record its version"
    );
    assert_eq!(
        lane_indexes(&superuser_pool, LEGIT_SCHEMA).await?,
        LEGIT_SLOTS as i64,
        "a timed-out v050 must leave every lane index, including those dropped before the timeout"
    );

    for (_, _, statement) in migrations::migration_sql_range(49, 50) {
        superuser_pool
            .execute(sqlx::raw_sql(audited_sql(statement)))
            .await?;
    }

    assert_eq!(
        lane_indexes(&superuser_pool, LEGIT_SCHEMA).await?,
        0,
        "v050 must drop the lane index from a custom schema the awa owner prepared"
    );
    assert_eq!(
        lane_indexes(&superuser_pool, DECOY_SCHEMA).await?,
        1,
        "v050 must leave a schema owned by another role untouched, even for a superuser"
    );
    let version: i32 = sqlx::query_scalar("SELECT max(version) FROM awa.schema_version")
        .fetch_one(&superuser_pool)
        .await?;
    assert_eq!(version, 50);

    superuser_pool.close().await;
    owner_pool.close().await;
    outsider_pool.close().await;
    Ok(())
}

async fn lane_indexes(pool: &PgPool, schema: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pg_class AS index_class \
         JOIN pg_namespace AS namespace ON namespace.oid = index_class.relnamespace \
         WHERE namespace.nspname = $1 AND index_class.relkind = 'i' \
           AND index_class.relname ~ '^idx_.+_ready_[0-9]+_lane_shard$'",
    )
    .bind(schema)
    .fetch_one(pool)
    .await
}
