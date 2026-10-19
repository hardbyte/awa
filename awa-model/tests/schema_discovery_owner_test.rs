//! Migration loops over custom queue-storage schemas only touch schemas the
//! migrating role or awa schema owner owns. A schema planted
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

    let standalone_steps: Vec<_> = migrations::migration_sql()
        .into_iter()
        .filter(|(_, _, sql)| {
            sql.contains("FROM awa.queue_storage_schemas() AS discovered")
                && !sql.contains("CREATE OR REPLACE FUNCTION awa.install_queue_storage_substrate(")
        })
        .collect();
    assert!(!standalone_steps.is_empty());
    migrator_pool.execute(
        "ALTER FUNCTION awa.install_queue_storage_substrate(text, integer, integer, integer, boolean) \
         SET application_name = 'standalone_discovery_marker'",
    ).await?;
    let installer_before: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('awa.install_queue_storage_substrate(text, integer, integer, integer, boolean)'::regprocedure)",
    ).fetch_one(&migrator_pool).await?;
    drop_discovery_helpers(&migrator_pool).await?;
    migrations::run(&migrator_pool).await?;
    let helper_absent: bool =
        sqlx::query_scalar("SELECT to_regprocedure('awa.queue_storage_schemas()') IS NULL")
            .fetch_one(&migrator_pool)
            .await?;
    assert!(
        helper_absent,
        "already-applied migrations must not be replayed"
    );
    for (version, _, sql) in standalone_steps {
        drop_discovery_helpers(&migrator_pool).await?;
        migrator_pool
            .execute(sqlx::raw_sql(audited_sql(sql)))
            .await?;
        assert_eq!(
            hardened_discovery(&migrator_pool).await?,
            vec!["awa".to_string(), OWNED_SCHEMA.to_string()],
            "pending migration {version} must bootstrap discovery without an existing helper",
        );
    }
    let installer_after: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('awa.install_queue_storage_substrate(text, integer, integer, integer, boolean)'::regprocedure)",
    ).fetch_one(&migrator_pool).await?;
    assert_eq!(
        installer_before, installer_after,
        "standalone steps must not redefine the installer"
    );

    QueueStorage::new(QueueStorageConfig {
        schema: "injected_custom".into(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })?
    .prepare_schema(&migrator_pool)
    .await?;
    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "GRANT USAGE, CREATE ON SCHEMA injected_custom TO {outsider}; \
         GRANT USAGE ON SCHEMA {OWNED_SCHEMA} TO {outsider}; \
         GRANT SELECT ON ALL TABLES IN SCHEMA {OWNED_SCHEMA} TO {outsider}; \
         DROP TABLE injected_custom.queue_ring_rotations;"
        ))))
        .await?;
    assert!(
        !hardened_discovery(&migrator_pool)
            .await?
            .contains(&"injected_custom".into()),
        "a foreign CREATE grant must be rejected before another object is planted"
    );
    outsider_pool.execute(sqlx::raw_sql(audited_sql(format!(
        "CREATE TABLE injected_custom.queue_ring_rotations \
         (LIKE {OWNED_SCHEMA}.queue_ring_rotations INCLUDING ALL); \
         CREATE FUNCTION {DECOY_SCHEMA}.tripwire_trigger() RETURNS trigger LANGUAGE plpgsql AS $trigger$ \
         BEGIN INSERT INTO {DECOY_SCHEMA}.tripwire VALUES (current_user); RETURN NEW; END $trigger$; \
         CREATE TRIGGER tripwire BEFORE INSERT ON injected_custom.queue_ring_rotations \
         FOR EACH ROW EXECUTE FUNCTION {DECOY_SCHEMA}.tripwire_trigger();"
    )))).await?;
    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "REVOKE CREATE ON SCHEMA injected_custom FROM {outsider}"
        ))))
        .await?;
    assert!(
        !hardened_discovery(&migrator_pool)
            .await?
            .contains(&"injected_custom".into()),
        "revoking CREATE must not hide an existing foreign-owned object"
    );

    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "GRANT TRIGGER ON {OWNED_SCHEMA}.queue_ring_state TO {outsider}"
        ))))
        .await?;
    assert!(
        !hardened_discovery(&migrator_pool)
            .await?
            .contains(&OWNED_SCHEMA.into()),
        "a foreign TRIGGER grant must be rejected before another trigger is planted"
    );
    outsider_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE TRIGGER foreign_trigger BEFORE INSERT ON {OWNED_SCHEMA}.queue_ring_state \
         FOR EACH ROW EXECUTE FUNCTION {DECOY_SCHEMA}.tripwire_trigger()"
        ))))
        .await?;
    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "REVOKE TRIGGER ON {OWNED_SCHEMA}.queue_ring_state FROM {outsider}"
        ))))
        .await?;
    assert!(
        !hardened_discovery(&migrator_pool)
            .await?
            .contains(&OWNED_SCHEMA.into()),
        "an existing foreign trigger function must still be rejected after revoking TRIGGER"
    );
    migrator_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "DROP TRIGGER foreign_trigger ON {OWNED_SCHEMA}.queue_ring_state"
        ))))
        .await?;
    assert!(hardened_discovery(&migrator_pool)
        .await?
        .contains(&OWNED_SCHEMA.into()));

    outsider_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE FUNCTION {DECOY_SCHEMA}.tripwire_when() RETURNS BOOLEAN LANGUAGE plpgsql AS $when$ \
             BEGIN INSERT INTO {DECOY_SCHEMA}.tripwire VALUES (current_user); RETURN FALSE; END $when$;"
        ))))
        .await?;
    for when_expression in [
        format!("{DECOY_SCHEMA}.tripwire_when()"),
        // pg_depend records no dependency on the foreign function here: it is
        // only text, and pinned catalog functions are never recorded.
        format!(
            "pg_catalog.query_to_xml('SELECT {DECOY_SCHEMA}.tripwire_when()', true, false, '') \
             IS NULL"
        ),
    ] {
        assert_foreign_when_clause_is_rejected(
            &migrator_pool,
            &superuser_pool,
            &outsider_pool,
            outsider,
            &when_expression,
        )
        .await?;
    }

    assert_configured_substrates_fail_closed(&migrator_pool, &superuser_pool, outsider).await?;

    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "GRANT CREATE ON SCHEMA awa TO {outsider}"
        ))))
        .await?;
    let pending = migrations::migration_sql_range(47, 48);
    let error = migrator_pool
        .execute(sqlx::raw_sql(audited_sql(pending[0].2.clone())))
        .await
        .expect_err("the default schema must be checked before any installer refresh");
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("42501")
    );
    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "REVOKE CREATE ON SCHEMA awa FROM {outsider}"
        ))))
        .await?;

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

    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE SCHEMA foreign_tables AUTHORIZATION {outsider}; \
         GRANT USAGE ON SCHEMA foreign_tables TO {migrator};"
        ))))
        .await?;
    outsider_pool.execute(sqlx::raw_sql(audited_sql(format!(
        "CREATE FUNCTION foreign_tables.claim_ready_runtime(TEXT, BIGINT, DOUBLE PRECISION, DOUBLE PRECISION) \
         RETURNS VOID LANGUAGE sql AS $decoy$ SELECT $decoy$; \
         CREATE TABLE foreign_tables.queue_ring_state (LIKE {OWNED_SCHEMA}.queue_ring_state INCLUDING ALL); \
         CREATE TABLE foreign_tables.lease_ring_state (LIKE {OWNED_SCHEMA}.lease_ring_state INCLUDING ALL); \
         CREATE TABLE foreign_tables.claim_ring_state (LIKE {OWNED_SCHEMA}.claim_ring_state INCLUDING ALL); \
         CREATE TRIGGER tripwire BEFORE INSERT ON foreign_tables.queue_ring_state \
         FOR EACH ROW EXECUTE FUNCTION {DECOY_SCHEMA}.tripwire_trigger();"
    )))).await?;
    assert!(legacy_discovery(&superuser_pool)
        .await?
        .contains(&"foreign_tables".into()));
    assert_eq!(
        hardened_discovery(&superuser_pool).await?,
        vec!["awa".to_string(), OWNED_SCHEMA.to_string()],
        "superuser authority must not turn foreign ownership into trusted ownership"
    );
    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "GRANT {outsider} TO {migrator}"
        ))))
        .await?;
    assert_eq!(
        hardened_discovery(&migrator_pool).await?,
        vec!["awa".to_string(), OWNED_SCHEMA.to_string()],
        "role membership alone must not authorize refreshing foreign schemas"
    );
    for (_, _, statement) in migrations::migration_sql_range(47, 48) {
        superuser_pool
            .execute(sqlx::raw_sql(audited_sql(statement)))
            .await?;
    }
    let hits: i64 = sqlx::query_scalar(audited_sql(format!(
        "SELECT count(*)::bigint FROM {DECOY_SCHEMA}.tripwire"
    )))
    .fetch_one(&superuser_pool)
    .await?;
    assert_eq!(
        hits, 0,
        "superuser refresh must not execute foreign triggers"
    );

    superuser_pool.close().await;
    migrator_pool.close().await;
    outsider_pool.close().await;
    Ok(())
}

/// A trigger's WHEN expression is evaluated by the session whose statement
/// fires the trigger, so functions it calls run with the migrator's
/// privileges even when the trigger routine itself is the installer's own.
/// The expression is stored in `pg_trigger.tgqual`, not behind `tgfoid`, and
/// a trigger survives revocation of the TRIGGER privilege that created it.
/// Discovery must reject the schema while such a trigger exists, and a
/// replay must neither evaluate it nor refresh that schema.
async fn assert_foreign_when_clause_is_rejected(
    migrator_pool: &PgPool,
    superuser_pool: &PgPool,
    outsider_pool: &PgPool,
    outsider: &str,
    when_expression: &str,
) -> TestResult {
    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "GRANT TRIGGER ON {OWNED_SCHEMA}.queue_ring_state TO {outsider}"
        ))))
        .await?;
    outsider_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE TRIGGER foreign_when BEFORE INSERT OR UPDATE OR DELETE \
             ON {OWNED_SCHEMA}.queue_ring_state FOR EACH ROW \
             WHEN ({when_expression}) \
             EXECUTE FUNCTION {OWNED_SCHEMA}.reject_compat_ring_cursor_update_after_flip()"
        ))))
        .await?;
    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "REVOKE TRIGGER ON {OWNED_SCHEMA}.queue_ring_state FROM {outsider}"
        ))))
        .await?;

    assert!(
        !hardened_discovery(migrator_pool)
            .await?
            .contains(&OWNED_SCHEMA.into()),
        "a trusted trigger routine with a foreign WHEN expression must make the schema untrusted: \
         {when_expression}"
    );

    migrator_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "COMMENT ON FUNCTION {OWNED_SCHEMA}.{CLAIM_SIGNATURE} IS 'stale'"
        ))))
        .await?;
    let mut replay_failed_closed = false;
    for (_, _, statement) in migrations::migration_sql_range(47, 48) {
        if let Err(error) = migrator_pool
            .execute(sqlx::raw_sql(audited_sql(statement)))
            .await
        {
            assert_eq!(
                error
                    .as_database_error()
                    .and_then(|error| error.code())
                    .as_deref(),
                Some("42501"),
                "a replay may only stop with the fail-closed privilege error: {error}"
            );
            replay_failed_closed = true;
            break;
        }
    }
    let tripwire_hits: i64 = sqlx::query_scalar(audited_sql(format!(
        "SELECT count(*)::bigint FROM {DECOY_SCHEMA}.tripwire"
    )))
    .fetch_one(migrator_pool)
    .await?;
    assert_eq!(
        tripwire_hits, 0,
        "a replay must not evaluate a foreign WHEN expression"
    );
    if !replay_failed_closed {
        let comment: Option<String> = sqlx::query_scalar(audited_sql(format!(
            "SELECT obj_description('{OWNED_SCHEMA}.{CLAIM_SIGNATURE}'::regprocedure, 'pg_proc')"
        )))
        .fetch_one(migrator_pool)
        .await?;
        assert_eq!(
            comment.as_deref(),
            Some("stale"),
            "a replay must skip a schema carrying a foreign WHEN expression"
        );
    }

    migrator_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "DROP TRIGGER foreign_when ON {OWNED_SCHEMA}.queue_ring_state"
        ))))
        .await?;
    assert!(hardened_discovery(migrator_pool)
        .await?
        .contains(&OWNED_SCHEMA.into()));
    Ok(())
}

async fn drop_discovery_helpers(pool: &PgPool) -> Result<(), sqlx::Error> {
    pool.execute(sqlx::raw_sql(
        "DROP FUNCTION IF EXISTS awa.queue_storage_schemas(); \
         DROP FUNCTION IF EXISTS awa.queue_storage_schema_is_trusted(text);",
    ))
    .await?;
    Ok(())
}

async fn assert_configured_substrates_fail_closed(
    migrator_pool: &PgPool,
    superuser_pool: &PgPool,
    outsider: &str,
) -> TestResult {
    let registered_before: Option<String> = sqlx::query_scalar(
        "SELECT schema_name FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'",
    )
    .fetch_optional(migrator_pool)
    .await?;
    let transition_before: (Option<String>, serde_json::Value) = sqlx::query_as(
        "SELECT prepared_engine, details FROM awa.storage_transition_state WHERE singleton",
    )
    .fetch_one(migrator_pool)
    .await?;
    sqlx::query("DELETE FROM awa.schema_version WHERE version = $1")
        .bind(migrations::CURRENT_VERSION)
        .execute(migrator_pool)
        .await?;
    let previous_version: i32 = sqlx::query_scalar("SELECT max(version) FROM awa.schema_version")
        .fetch_one(migrator_pool)
        .await?;
    sqlx::query(
        "INSERT INTO awa.runtime_storage_backends (backend, schema_name) VALUES ('queue_storage', 'injected_custom') \
         ON CONFLICT (backend) DO UPDATE SET schema_name = EXCLUDED.schema_name",
    ).execute(migrator_pool).await?;
    let error = migrations::run(migrator_pool)
        .await
        .expect_err("an unsafe registered substrate must block migration");
    match error {
        awa_model::AwaError::Database(error) => {
            assert_eq!(
                error
                    .as_database_error()
                    .and_then(|error| error.code())
                    .as_deref(),
                Some("42501"),
            );
            assert!(
                error
                    .to_string()
                    .contains("unsafe configured queue-storage schema: injected_custom"),
                "refusal must come from the preflight, got {error}"
            );
        }
        error => panic!("expected ownership preflight refusal, got {error}"),
    }
    let version_after: i32 = sqlx::query_scalar("SELECT max(version) FROM awa.schema_version")
        .fetch_one(migrator_pool)
        .await?;
    assert_eq!(
        version_after, previous_version,
        "refusal must not advance the global version"
    );
    sqlx::query("UPDATE awa.runtime_storage_backends SET schema_name = 'awa' WHERE backend = 'queue_storage'")
        .execute(migrator_pool).await?;
    superuser_pool
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE SCHEMA awa_exp AUTHORIZATION {outsider}"
        ))))
        .await?;
    let (_, _, pending_sql) = migrations::migration_sql().pop().unwrap();
    for (details, expected_schema) in [
        (
            serde_json::json!({"schema": "injected_custom"}),
            "injected_custom",
        ),
        (serde_json::json!({}), "awa_exp"),
    ] {
        sqlx::query("UPDATE awa.storage_transition_state SET prepared_engine = 'queue_storage', details = $1 WHERE singleton")
            .bind(details).execute(migrator_pool).await?;
        let error = migrator_pool
            .execute(sqlx::raw_sql(audited_sql(pending_sql.clone())))
            .await
            .expect_err("unsafe prepared targets must block exported bundles too");
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref(),
            Some("42501")
        );
        assert!(
            error.to_string().contains(&format!(
                "unsafe configured queue-storage schema: {expected_schema}"
            )),
            "refusal must come from the preflight, got {error}"
        );
        let version_after: i32 = sqlx::query_scalar("SELECT max(version) FROM awa.schema_version")
            .fetch_one(migrator_pool)
            .await?;
        assert_eq!(version_after, previous_version);
    }
    sqlx::query("UPDATE awa.storage_transition_state SET prepared_engine = $1, details = $2 WHERE singleton")
        .bind(transition_before.0).bind(transition_before.1).execute(migrator_pool).await?;
    if let Some(schema) = registered_before {
        sqlx::query("UPDATE awa.runtime_storage_backends SET schema_name = $1 WHERE backend = 'queue_storage'")
            .bind(schema).execute(migrator_pool).await?;
    } else {
        sqlx::query("DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'")
            .execute(migrator_pool)
            .await?;
    }
    migrations::run(migrator_pool).await?;
    let version_after: i32 = sqlx::query_scalar("SELECT max(version) FROM awa.schema_version")
        .fetch_one(migrator_pool)
        .await?;
    assert_eq!(
        version_after,
        migrations::CURRENT_VERSION,
        "an unregistered decoy must not prevent a legitimate upgrade"
    );
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
