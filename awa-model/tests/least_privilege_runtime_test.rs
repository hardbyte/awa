use awa_model::{migrations, QueueStorage, QueueStorageConfig};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Executor, PgPool};
use std::str::FromStr;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const SCHEMAS: [&str; 3] = ["awa", "custom_lanes", "custom_legacy"];

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
    admin.execute(sqlx::raw_sql(awa_model::audited_sql(format!("CREATE ROLE {owner} LOGIN PASSWORD 'lane_test'; CREATE ROLE {runtime} LOGIN PASSWORD 'lane_test';")))).await?;
    admin
        .execute(sqlx::raw_sql(awa_model::audited_sql(format!(
            "CREATE DATABASE {database} OWNER {owner}"
        ))))
        .await?;
    let options = PgConnectOptions::from_str(&url)?
        .database(&database)
        .password("lane_test");
    let result = exercise(options, &owner, &runtime).await;
    admin
        .execute(sqlx::raw_sql(awa_model::audited_sql(format!(
            "DROP DATABASE {database} WITH (FORCE)"
        ))))
        .await?;
    admin
        .execute(sqlx::raw_sql(awa_model::audited_sql(format!(
            "DROP ROLE {runtime}; DROP ROLE {owner};"
        ))))
        .await?;
    admin.close().await;
    result
}

async fn exercise(options: PgConnectOptions, owner: &str, runtime: &str) -> TestResult {
    let owner_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone().username(owner))
        .await?;
    for (_, _, statement) in migrations::migration_sql_range(0, 48) {
        owner_pool
            .execute(sqlx::raw_sql(awa_model::audited_sql(statement)))
            .await?;
    }
    let mut schema_shapes_before = Vec::new();
    for schema in SCHEMAS {
        if schema != "awa" {
            QueueStorage::new(QueueStorageConfig {
                schema: schema.into(),
                queue_slot_count: 6,
                lease_slot_count: 4,
                claim_slot_count: 3,
                lease_claim_receipts: schema != "custom_legacy",
                ..Default::default()
            })?
            .prepare_schema(&owner_pool)
            .await?;
        }
        schema_shapes_before.push(schema_snapshot(&owner_pool, schema).await?);
        let legacy_helper =
            include_str!("fixtures/v046_ensure_lane_sequences.sql").replace("__schema__", schema);
        owner_pool
            .execute(sqlx::raw_sql(awa_model::audited_sql(legacy_helper)))
            .await?;
        owner_pool.execute(sqlx::raw_sql(awa_model::audited_sql(format!(r#"
            GRANT USAGE ON SCHEMA {schema} TO {runtime};
            GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {schema} TO {runtime};
            GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {schema} TO {runtime};
            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {runtime};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {runtime};
            SELECT {schema}.ensure_lane_sequences('prepared_queue', 2::smallint, 0::smallint);
        "#)))).await?;
    }
    for schema in SCHEMAS {
        let store = QueueStorage::new(QueueStorageConfig {
            schema: schema.into(),
            queue_stripe_count: 2,
            ..Default::default()
        })?;
        store.prepare_queue(&owner_pool, "striped_queue", 2).await?;
        store.prepare_queue(&owner_pool, "striped_queue", 2).await?;
        let lane_count: i64 = sqlx::query_scalar(awa_model::audited_sql(format!(
            "SELECT count(*) FROM {schema}.queue_enqueue_heads WHERE queue LIKE 'striped_queue%'"
        )))
        .fetch_one(&owner_pool)
        .await?;
        assert_eq!(lane_count, 16);
        assert!(store
            .prepare_queue(&owner_pool, "invalid", 0)
            .await
            .is_err());
        assert!(store
            .prepare_queue(&owner_pool, "invalid", 65)
            .await
            .is_err());
    }
    let runtime_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.username(runtime))
        .await?;
    for schema in SCHEMAS {
        let error = runtime_pool
            .execute(sqlx::raw_sql(awa_model::audited_sql(format!(
                "SELECT {schema}.ensure_lane_sequences('prepared_queue', 2::smallint, 0::smallint)"
            ))))
            .await
            .unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("42501")
        );
    }
    migrations::run(&owner_pool).await?;
    for (schema, shape_before) in SCHEMAS.into_iter().zip(schema_shapes_before) {
        assert_eq!(schema_snapshot(&owner_pool, schema).await?, shape_before);
        check_runtime(&runtime_pool, schema).await?;
        let cursor_sql = format!("SELECT {schema}.sequence_next_value(seq_name) FROM {schema}.queue_enqueue_heads WHERE queue='prepared_queue' AND priority=2 AND enqueue_shard=0");
        let before: i64 = sqlx::query_scalar(awa_model::audited_sql(cursor_sql.clone()))
            .fetch_one(&runtime_pool)
            .await?;
        QueueStorage::new(QueueStorageConfig {
            schema: schema.into(),
            ..Default::default()
        })?
        .prepare_queue(&owner_pool, "prepared_queue", 1)
        .await?;
        let after: i64 = sqlx::query_scalar(awa_model::audited_sql(cursor_sql))
            .fetch_one(&runtime_pool)
            .await?;
        assert_eq!(before, after);
    }
    assert_no_head_table_ddl_locks(&owner_pool).await?;
    runtime_pool.close().await;
    owner_pool.close().await;
    Ok(())
}

async fn schema_snapshot(
    pool: &PgPool,
    schema: &str,
) -> Result<(i32, i32, i32, String), sqlx::Error> {
    sqlx::query_as(awa_model::audited_sql(format!(
        "SELECT \
            (SELECT slot_count FROM {schema}.queue_ring_state WHERE singleton), \
            (SELECT slot_count FROM {schema}.lease_ring_state WHERE singleton), \
            (SELECT slot_count FROM {schema}.claim_ring_state WHERE singleton), \
            pg_get_functiondef('{schema}.claim_ready_runtime(text,bigint,double precision,double precision)'::regprocedure)"
    )))
    .fetch_one(pool)
    .await
}

async fn assert_no_head_table_ddl_locks(pool: &PgPool) -> TestResult {
    let mut transaction = pool.begin().await?;
    for (_, _, statement) in migrations::migration_sql_range(48, 49) {
        transaction
            .execute(sqlx::raw_sql(awa_model::audited_sql(statement)))
            .await?;
    }
    let exclusive_locks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks \
         WHERE pid = pg_backend_pid() AND mode = 'AccessExclusiveLock' \
           AND relation IN ( \
             'awa.queue_enqueue_heads'::regclass, 'awa.queue_claim_heads'::regclass, \
             'custom_lanes.queue_enqueue_heads'::regclass, 'custom_lanes.queue_claim_heads'::regclass, \
             'custom_legacy.queue_enqueue_heads'::regclass, 'custom_legacy.queue_claim_heads'::regclass \
           )",
    )
    .fetch_one(transaction.as_mut())
    .await?;
    assert_eq!(exclusive_locks, 0);
    transaction.rollback().await?;
    Ok(())
}

async fn check_runtime(pool: &PgPool, schema: &str) -> TestResult {
    let can_create: bool =
        sqlx::query_scalar("SELECT has_schema_privilege(current_user, $1, 'CREATE')")
            .bind(schema)
            .fetch_one(pool)
            .await?;
    assert!(!can_create);
    pool.execute(sqlx::raw_sql(awa_model::audited_sql(format!(r#"
        SELECT {schema}.ensure_lane_sequences('prepared_queue', 2::smallint, 0::smallint);
        INSERT INTO {schema}.queue_lanes(queue, priority) VALUES ('prepared_queue', 2) ON CONFLICT DO NOTHING;
        INSERT INTO {schema}.queue_enqueue_heads(queue, priority, enqueue_shard) VALUES ('prepared_queue', 2, 0) ON CONFLICT DO NOTHING;
        INSERT INTO {schema}.queue_claim_heads(queue, priority, enqueue_shard) VALUES ('prepared_queue', 2, 0) ON CONFLICT DO NOTHING;
        SELECT {schema}.reserve_enqueue_seq('prepared_queue', 2::smallint, 0::smallint, 3);
    "#)))).await?;
    let error = pool
        .execute(sqlx::raw_sql(awa_model::audited_sql(format!(
            "SELECT {schema}.ensure_lane_sequences('unprepared_queue', 2::smallint, 0::smallint)"
        ))))
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("42501")
    );
    assert!(error
        .as_database_error()
        .unwrap()
        .message()
        .contains("has not been provisioned"));
    let error = pool
        .execute(sqlx::raw_sql(awa_model::audited_sql(format!(
            "CREATE TABLE {schema}.must_not_create(id integer)"
        ))))
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("42501")
    );
    Ok(())
}
