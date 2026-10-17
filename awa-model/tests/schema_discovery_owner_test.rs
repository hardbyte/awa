//! Migration loops over custom queue-storage schemas only touch schemas the
//! migrating role owns (directly or through membership). A schema planted
//! by another role with a look-alike `claim_ready_runtime` and ring-state
//! views must be skipped without evaluating those views, while a custom
//! schema prepared by the migrator is still refreshed.

use awa_model::{audited_sql, migrations, QueueStorage, QueueStorageConfig};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Executor, PgPool};
use std::str::FromStr;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const OWNED_SCHEMA: &str = "owned_custom";
const DECOY_SCHEMA: &str = "decoy";
const CLAIM_SIGNATURE: &str =
    "claim_ready_runtime(text, bigint, double precision, double precision)";

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

#[tokio::test]
async fn migration_discovery_skips_foreign_schemas_and_refreshes_owned_ones() -> TestResult {
    let url = database_url();
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let migrator = format!("discovery_migrator_{suffix}");
    let outsider = format!("discovery_outsider_{suffix}");
    let database = format!("discovery_db_{suffix}");
    admin
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE ROLE {migrator} LOGIN NOSUPERUSER PASSWORD 'discovery_test'; \
             CREATE ROLE {outsider} LOGIN NOSUPERUSER PASSWORD 'discovery_test';"
        ))))
        .await?;
    admin
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE DATABASE {database} OWNER {migrator}"
        ))))
        .await?;

    let options = PgConnectOptions::from_str(&url)?.database(&database);
    let result = exercise(options, &migrator, &outsider).await;

    admin
        .execute(sqlx::raw_sql(audited_sql(format!(
            "DROP DATABASE {database} WITH (FORCE)"
        ))))
        .await?;
    admin
        .execute(sqlx::raw_sql(audited_sql(format!(
            "DROP ROLE {outsider}; DROP ROLE {migrator};"
        ))))
        .await?;
    admin.close().await;
    result
}

async fn exercise(options: PgConnectOptions, migrator: &str, outsider: &str) -> TestResult {
    let superuser_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await?;
    let migrator_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(
            options
                .clone()
                .username(migrator)
                .password("discovery_test"),
        )
        .await?;
    let outsider_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.username(outsider).password("discovery_test"))
        .await?;

    migrations::run(&migrator_pool).await?;
    QueueStorage::new(QueueStorageConfig {
        schema: OWNED_SCHEMA.into(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })?
    .prepare_schema(&migrator_pool)
    .await?;

    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE SCHEMA {DECOY_SCHEMA} AUTHORIZATION {outsider}; \
             GRANT USAGE ON SCHEMA {DECOY_SCHEMA} TO {migrator}; \
             GRANT CREATE ON SCHEMA public TO {outsider};"
        ))))
        .await?;
    plant_decoys(&outsider_pool).await?;

    assert_eq!(
        legacy_discovery(&migrator_pool).await?,
        vec![
            "awa".to_string(),
            DECOY_SCHEMA.to_string(),
            OWNED_SCHEMA.to_string(),
            "public".to_string(),
        ],
        "the decoys must match the signature-only discovery they target"
    );
    assert_eq!(
        hardened_discovery(&migrator_pool).await?,
        vec!["awa".to_string(), OWNED_SCHEMA.to_string()],
    );

    migrator_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "COMMENT ON FUNCTION {OWNED_SCHEMA}.{CLAIM_SIGNATURE} IS 'stale'"
        ))))
        .await?;
    for (_, _, statement) in migrations::migration_sql_range(47, 48) {
        migrator_pool
            .execute(sqlx::raw_sql(audited_sql(statement)))
            .await?;
    }

    let tripwire_hits: i64 = sqlx::query_scalar(audited_sql(format!(
        "SELECT count(*)::bigint FROM {DECOY_SCHEMA}.tripwire"
    )))
    .fetch_one(&migrator_pool)
    .await?;
    assert_eq!(
        tripwire_hits, 0,
        "a migration replay must not evaluate another role's ring-state views"
    );

    let refreshed_comment: Option<String> = sqlx::query_scalar(audited_sql(format!(
        "SELECT obj_description('{OWNED_SCHEMA}.{CLAIM_SIGNATURE}'::regprocedure, 'pg_proc')"
    )))
    .fetch_one(&migrator_pool)
    .await?;
    assert!(
        refreshed_comment
            .as_deref()
            .is_some_and(|comment| comment.starts_with("Queue-storage claim allocator")),
        "the migrator's own custom schema must still be reinstalled, got {refreshed_comment:?}"
    );

    superuser_pool.close().await;
    migrator_pool.close().await;
    outsider_pool.close().await;
    Ok(())
}

async fn plant_decoys(outsider_pool: &PgPool) -> TestResult {
    outsider_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            r#"
            CREATE TABLE {DECOY_SCHEMA}.tripwire (fired_by TEXT NOT NULL);
            GRANT SELECT, INSERT ON {DECOY_SCHEMA}.tripwire TO PUBLIC;
            CREATE FUNCTION {DECOY_SCHEMA}.tripwire_slot_count() RETURNS INT
            LANGUAGE plpgsql AS $tripwire$
            BEGIN
                INSERT INTO {DECOY_SCHEMA}.tripwire VALUES (current_user);
                RETURN 4;
            END
            $tripwire$;
            "#
        ))))
        .await?;
    for schema in [DECOY_SCHEMA, "public"] {
        outsider_pool
            .execute(sqlx::raw_sql(audited_sql(format!(
                r#"
                CREATE FUNCTION {schema}.claim_ready_runtime(
                    TEXT, BIGINT, DOUBLE PRECISION, DOUBLE PRECISION
                ) RETURNS VOID LANGUAGE sql AS $decoy$ SELECT $decoy$;
                CREATE VIEW {schema}.queue_ring_state AS
                    SELECT TRUE AS singleton, {DECOY_SCHEMA}.tripwire_slot_count() AS slot_count;
                CREATE VIEW {schema}.lease_ring_state AS
                    SELECT TRUE AS singleton, {DECOY_SCHEMA}.tripwire_slot_count() AS slot_count;
                CREATE VIEW {schema}.claim_ring_state AS
                    SELECT TRUE AS singleton, {DECOY_SCHEMA}.tripwire_slot_count() AS slot_count;
                GRANT SELECT ON {schema}.queue_ring_state, {schema}.lease_ring_state,
                    {schema}.claim_ring_state TO PUBLIC;
                "#
            ))))
            .await?;
    }
    Ok(())
}

async fn legacy_discovery(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT n.nspname::text
        FROM pg_namespace AS n
        WHERE has_schema_privilege(current_user, n.oid, 'USAGE')
          AND EXISTS (
              SELECT 1 FROM pg_proc AS awa_p
              WHERE awa_p.pronamespace = n.oid
                AND awa_p.proname = 'claim_ready_runtime'
                AND oidvectortypes(awa_p.proargtypes)
                    = 'text, bigint, double precision, double precision'
          )
        ORDER BY n.nspname
        "#,
    )
    .fetch_all(pool)
    .await
}

async fn hardened_discovery(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT schema_name FROM awa.queue_storage_schemas()")
        .fetch_all(pool)
        .await
}
