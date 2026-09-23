use awa_model::{migrations, QueueStorage, QueueStorageConfig};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Executor, PgPool};
use std::str::FromStr;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const CLAIM_RUNTIME: &str = "claim_ready_runtime(text,bigint,double precision,double precision)";

#[tokio::test]
async fn runtime_uses_prepared_lane_sequences_without_schema_create() -> TestResult {
    let url = std::env::var("DATABASE_URL")?;
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let owner = format!("lane_owner_{suffix}");
    let runtime = format!("lane_runtime_{suffix}");
    let database = format!("lane_db_{suffix}");
    admin
        .execute(sqlx::raw_sql(&format!(
            "CREATE ROLE {owner} LOGIN PASSWORD 'lane_test'; \
             CREATE ROLE {runtime} LOGIN PASSWORD 'lane_test';"
        )))
        .await?;
    admin
        .execute(sqlx::raw_sql(&format!(
            "CREATE DATABASE {database} OWNER {owner}"
        )))
        .await?;
    let options = PgConnectOptions::from_str(&url)?
        .database(&database)
        .password("lane_test");
    let result = exercise(options, &owner, &runtime).await;
    admin
        .execute(sqlx::raw_sql(&format!(
            "DROP DATABASE {database} WITH (FORCE)"
        )))
        .await?;
    admin
        .execute(sqlx::raw_sql(&format!(
            "DROP ROLE {runtime}; DROP ROLE {owner};"
        )))
        .await?;
    admin.close().await;
    result
}

async fn exercise(options: PgConnectOptions, owner: &str, runtime: &str) -> TestResult {
    let owner_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone().username(owner))
        .await?;
    for (_, _, statement) in migrations::migration_sql_range(0, migrations::CURRENT_VERSION) {
        owner_pool.execute(sqlx::raw_sql(&statement)).await?;
    }
    owner_pool
        .execute(sqlx::raw_sql(
            "CREATE SCHEMA custom_lanes; \
             SELECT awa.install_queue_storage_substrate('custom_lanes', 16, 8, 8, true);",
        ))
        .await?;
    assert!(pending_patch_names(&owner_pool)
        .await?
        .contains(&"prepared_lane_sequences"));

    let mut claim_runtimes = Vec::new();
    for schema in ["awa", "custom_lanes"] {
        owner_pool
            .execute(sqlx::raw_sql(&format!(
                r#"
                GRANT USAGE ON SCHEMA {schema} TO {runtime};
                GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {schema} TO {runtime};
                GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {schema} TO {runtime};
                GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {runtime};
                ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {runtime};
                SELECT {schema}.ensure_lane_sequences('prepared_queue', 2::smallint, 0::smallint);
                "#
            )))
            .await?;
        claim_runtimes.push(claim_runtime_definition(&owner_pool, schema).await?);
    }

    let runtime_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.username(runtime))
        .await?;
    for schema in ["awa", "custom_lanes"] {
        let error = runtime_pool
            .execute(sqlx::raw_sql(&format!(
                "SELECT {schema}.ensure_lane_sequences('prepared_queue', 2::smallint, 0::smallint)"
            )))
            .await
            .unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("42501")
        );
    }

    migrations::run(&owner_pool).await?;
    assert!(pending_patch_names(&owner_pool).await?.is_empty());
    owner_pool
        .execute(sqlx::raw_sql(
            migrations::SCHEMA_PATCHES
                .iter()
                .find(|patch| patch.name == "prepared_lane_sequences")
                .unwrap()
                .sql,
        ))
        .await?;
    assert!(pending_patch_names(&owner_pool).await?.is_empty());

    for (schema, before) in ["awa", "custom_lanes"].into_iter().zip(claim_runtimes) {
        assert_eq!(claim_runtime_definition(&owner_pool, schema).await?, before);
        check_runtime(&runtime_pool, schema).await?;

        let cursor_sql = format!(
            "SELECT {schema}.sequence_next_value(seq_name) FROM {schema}.queue_enqueue_heads \
             WHERE queue = 'prepared_queue' AND priority = 2 AND enqueue_shard = 0"
        );
        let before: i64 = sqlx::query_scalar(&cursor_sql)
            .fetch_one(&runtime_pool)
            .await?;
        let store = QueueStorage::new(QueueStorageConfig {
            schema: schema.into(),
            ..Default::default()
        })?;
        store
            .prepare_queue(&owner_pool, "prepared_queue", 1)
            .await?;
        let after: i64 = sqlx::query_scalar(&cursor_sql)
            .fetch_one(&runtime_pool)
            .await?;
        assert_eq!(before, after);

        let striped = QueueStorage::new(QueueStorageConfig {
            schema: schema.into(),
            queue_stripe_count: 2,
            ..Default::default()
        })?;
        striped
            .prepare_queue(&owner_pool, "striped_queue", 2)
            .await?;
        striped
            .prepare_queue(&owner_pool, "striped_queue", 2)
            .await?;
        let lane_count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM {schema}.queue_enqueue_heads WHERE queue LIKE 'striped_queue%'"
        ))
        .fetch_one(&owner_pool)
        .await?;
        assert_eq!(lane_count, 16);
        assert!(striped
            .prepare_queue(&owner_pool, "invalid", 0)
            .await
            .is_err());
        assert!(striped
            .prepare_queue(&owner_pool, "invalid", 65)
            .await
            .is_err());
    }

    QueueStorage::new(QueueStorageConfig {
        schema: "fresh_lanes".into(),
        ..Default::default()
    })?
    .prepare_schema(&owner_pool)
    .await?;
    assert!(pending_patch_names(&owner_pool).await?.is_empty());

    runtime_pool.close().await;
    owner_pool.close().await;
    Ok(())
}

async fn pending_patch_names(
    pool: &PgPool,
) -> Result<Vec<&'static str>, Box<dyn std::error::Error>> {
    Ok(migrations::pending_schema_patches(pool)
        .await?
        .into_iter()
        .map(|patch| patch.name)
        .collect())
}

async fn claim_runtime_definition(pool: &PgPool, schema: &str) -> Result<String, sqlx::Error> {
    sqlx::query_scalar("SELECT pg_get_functiondef(to_regprocedure($1))")
        .bind(format!("{schema}.{CLAIM_RUNTIME}"))
        .fetch_one(pool)
        .await
}

async fn check_runtime(pool: &PgPool, schema: &str) -> TestResult {
    let can_create: bool =
        sqlx::query_scalar("SELECT has_schema_privilege(current_user, $1, 'CREATE')")
            .bind(schema)
            .fetch_one(pool)
            .await?;
    assert!(!can_create);
    pool.execute(sqlx::raw_sql(&format!(
        r#"
        SELECT {schema}.ensure_lane_sequences('prepared_queue', 2::smallint, 0::smallint);
        INSERT INTO {schema}.queue_lanes(queue, priority) VALUES ('prepared_queue', 2) ON CONFLICT DO NOTHING;
        INSERT INTO {schema}.queue_enqueue_heads(queue, priority, enqueue_shard) VALUES ('prepared_queue', 2, 0) ON CONFLICT DO NOTHING;
        INSERT INTO {schema}.queue_claim_heads(queue, priority, enqueue_shard) VALUES ('prepared_queue', 2, 0) ON CONFLICT DO NOTHING;
        SELECT {schema}.reserve_enqueue_seq('prepared_queue', 2::smallint, 0::smallint, 3);
        "#
    )))
    .await?;
    let error = pool
        .execute(sqlx::raw_sql(&format!(
            "SELECT {schema}.ensure_lane_sequences('unprepared_queue', 2::smallint, 0::smallint)"
        )))
        .await
        .unwrap_err();
    let database_error = error.as_database_error().unwrap();
    assert_eq!(database_error.code().as_deref(), Some("42501"));
    assert!(database_error
        .message()
        .contains("has not been provisioned"));
    let error = pool
        .execute(sqlx::raw_sql(&format!(
            "CREATE TABLE {schema}.must_not_create(id integer)"
        )))
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("42501")
    );
    Ok(())
}
