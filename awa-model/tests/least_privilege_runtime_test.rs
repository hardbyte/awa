use awa_model::{migrations, QueueStorage, QueueStorageConfig};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::str::FromStr;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const SCHEMAS: [&str; 2] = ["awa", "custom_lanes"];

async fn run_sql(pool: &PgPool, sql: impl AsRef<str>) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(sql.as_ref()).execute(pool).await?;
    Ok(())
}

fn database_error_code(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(|database_error| database_error.code().map(|code| code.to_string()))
}

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
    run_sql(
        &admin,
        format!(
            "CREATE ROLE {owner} LOGIN PASSWORD 'lane_test'; \
             CREATE ROLE {runtime} LOGIN PASSWORD 'lane_test';"
        ),
    )
    .await?;
    run_sql(&admin, format!("CREATE DATABASE {database} OWNER {owner}")).await?;
    let options = PgConnectOptions::from_str(&url)?
        .database(&database)
        .password("lane_test");
    let result = exercise(options, &owner, &runtime).await;
    run_sql(&admin, format!("DROP DATABASE {database} WITH (FORCE)")).await?;
    run_sql(&admin, format!("DROP ROLE {runtime}; DROP ROLE {owner};")).await?;
    admin.close().await;
    result
}

async fn exercise(options: PgConnectOptions, owner: &str, runtime: &str) -> TestResult {
    let owner_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone().username(owner))
        .await?;
    for (_, _, statement) in migrations::migration_sql() {
        run_sql(&owner_pool, statement).await?;
    }
    for schema in SCHEMAS {
        if schema != "awa" {
            QueueStorage::new(QueueStorageConfig {
                schema: schema.into(),
                ..Default::default()
            })?
            .prepare_schema(&owner_pool)
            .await?;
        }
        let previous_helper =
            include_str!("fixtures/v06_ensure_lane_sequences.sql").replace("__schema__", schema);
        run_sql(&owner_pool, previous_helper).await?;
        run_sql(
            &owner_pool,
            format!(
                r#"
                GRANT USAGE ON SCHEMA {schema} TO {runtime};
                GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {schema} TO {runtime};
                GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {schema} TO {runtime};
                GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {runtime};
                ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {runtime};
                SELECT {schema}.ensure_lane_sequences('prepared_queue', 2::smallint, 0::smallint);
                "#
            ),
        )
        .await?;
    }
    for schema in SCHEMAS {
        let store = QueueStorage::new(QueueStorageConfig {
            schema: schema.into(),
            queue_stripe_count: 2,
            ..Default::default()
        })?;
        store.prepare_queue(&owner_pool, "striped_queue", 2).await?;
        store.prepare_queue(&owner_pool, "striped_queue", 2).await?;
        let lane_count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM {schema}.queue_enqueue_heads WHERE queue LIKE 'striped_queue%'"
        ))
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
        let error = run_sql(
            &runtime_pool,
            format!(
                "SELECT {schema}.ensure_lane_sequences('prepared_queue', 2::smallint, 0::smallint)"
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(database_error_code(&error).as_deref(), Some("42501"));
    }

    let pending = migrations::pending_schema_patches(&owner_pool).await?;
    assert!(pending
        .iter()
        .any(|patch| patch.name == "prepared_lane_sequences"));
    migrations::run(&owner_pool).await?;
    assert!(migrations::pending_schema_patches(&owner_pool)
        .await?
        .is_empty());

    for schema in SCHEMAS {
        check_runtime(&runtime_pool, schema).await?;
        let cursor_sql = format!(
            "SELECT {schema}.sequence_next_value(seq_name) FROM {schema}.queue_enqueue_heads \
             WHERE queue='prepared_queue' AND priority=2 AND enqueue_shard=0"
        );
        let before: i64 = sqlx::query_scalar(&cursor_sql)
            .fetch_one(&runtime_pool)
            .await?;
        QueueStorage::new(QueueStorageConfig {
            schema: schema.into(),
            ..Default::default()
        })?
        .prepare_queue(&owner_pool, "prepared_queue", 1)
        .await?;
        let after: i64 = sqlx::query_scalar(&cursor_sql)
            .fetch_one(&runtime_pool)
            .await?;
        assert_eq!(before, after);
    }
    runtime_pool.close().await;
    owner_pool.close().await;
    Ok(())
}

async fn check_runtime(pool: &PgPool, schema: &str) -> TestResult {
    let can_create: bool =
        sqlx::query_scalar("SELECT has_schema_privilege(current_user, $1, 'CREATE')")
            .bind(schema)
            .fetch_one(pool)
            .await?;
    assert!(!can_create);
    run_sql(
        pool,
        format!(
            r#"
            SELECT {schema}.ensure_lane_sequences('prepared_queue', 2::smallint, 0::smallint);
            INSERT INTO {schema}.queue_lanes(queue, priority) VALUES ('prepared_queue', 2) ON CONFLICT DO NOTHING;
            INSERT INTO {schema}.queue_enqueue_heads(queue, priority, enqueue_shard) VALUES ('prepared_queue', 2, 0) ON CONFLICT DO NOTHING;
            INSERT INTO {schema}.queue_claim_heads(queue, priority, enqueue_shard) VALUES ('prepared_queue', 2, 0) ON CONFLICT DO NOTHING;
            SELECT {schema}.reserve_enqueue_seq('prepared_queue', 2::smallint, 0::smallint, 3);
            "#
        ),
    )
    .await?;
    let error = run_sql(
        pool,
        format!(
            "SELECT {schema}.ensure_lane_sequences('unprepared_queue', 2::smallint, 0::smallint)"
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(database_error_code(&error).as_deref(), Some("42501"));
    assert!(error
        .as_database_error()
        .unwrap()
        .message()
        .contains("has not been provisioned"));
    let error = run_sql(
        pool,
        format!("CREATE TABLE {schema}.must_not_create(id integer)"),
    )
    .await
    .unwrap_err();
    assert_eq!(database_error_code(&error).as_deref(), Some("42501"));
    Ok(())
}

/// Relation locks the current transaction holds on queue-storage objects, as
/// `(relation, mode)` pairs.
async fn queue_storage_relation_locks(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT format('%I.%I', n.nspname, c.relname), l.mode \
         FROM pg_locks AS l \
         JOIN pg_class AS c ON c.oid = l.relation \
         JOIN pg_namespace AS n ON n.oid = c.relnamespace \
         WHERE l.pid = pg_backend_pid() \
           AND l.locktype = 'relation' \
           AND n.nspname = ANY($1) \
         ORDER BY 1, 2",
    )
    .bind(SCHEMAS.to_vec())
    .fetch_all(&mut **tx)
    .await
}

#[tokio::test]
async fn prepared_lane_patch_replaces_helpers_without_table_locks() -> TestResult {
    let url = std::env::var("DATABASE_URL")?;
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await?;
    let database = format!("lane_lock_db_{}", uuid::Uuid::new_v4().simple());
    run_sql(&admin, format!("CREATE DATABASE {database}")).await?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(PgConnectOptions::from_str(&url)?.database(&database))
        .await?;
    let result = exercise_patch_locks(&pool).await;
    pool.close().await;
    run_sql(&admin, format!("DROP DATABASE {database} WITH (FORCE)")).await?;
    admin.close().await;
    result
}

async fn exercise_patch_locks(pool: &PgPool) -> TestResult {
    migrations::run(pool).await?;
    for schema in SCHEMAS {
        if schema != "awa" {
            QueueStorage::new(QueueStorageConfig {
                schema: schema.into(),
                ..Default::default()
            })?
            .prepare_schema(pool)
            .await?;
        }
        run_sql(
            pool,
            include_str!("fixtures/v06_ensure_lane_sequences.sql").replace("__schema__", schema),
        )
        .await?;
    }
    let patch = migrations::SCHEMA_PATCHES
        .iter()
        .find(|patch| patch.name == "prepared_lane_sequences")
        .expect("prepared_lane_sequences patch");
    let applied: bool = sqlx::query_scalar(patch.applied_probe)
        .fetch_one(pool)
        .await?;
    assert!(!applied);

    // The patch holds no lock on any queue-storage table or index until commit.
    let mut tx = pool.begin().await?;
    sqlx::raw_sql(patch.sql).execute(&mut *tx).await?;
    assert_eq!(queue_storage_relation_locks(&mut tx).await?, vec![]);
    tx.commit().await?;

    let applied: bool = sqlx::query_scalar(patch.applied_probe)
        .fetch_one(pool)
        .await?;
    assert!(applied);
    for schema in SCHEMAS {
        let patched: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_proc \
             WHERE pronamespace = $1::regnamespace \
               AND proname IN ('ensure_lane_sequences', \
                               'queue_enqueue_head_sequence_sync', \
                               'queue_claim_head_sequence_sync') \
               AND prosrc LIKE '%has not been provisioned%'",
        )
        .bind(schema)
        .fetch_one(pool)
        .await?;
        assert_eq!(patched, 3, "schema {schema}");
    }

    // Re-running the installer instead would hold exclusive locks on live
    // tables such as leases until commit, which is why the patch avoids it.
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT awa.install_queue_storage_substrate('custom_lanes')")
        .execute(&mut *tx)
        .await?;
    let locks = queue_storage_relation_locks(&mut tx).await?;
    tx.rollback().await?;
    assert!(locks.iter().any(|(relation, mode)| {
        relation == "custom_lanes.leases" && mode == "AccessExclusiveLock"
    }));
    Ok(())
}

/// The patch counts as applied only when every installed substrate has the
/// new helpers: a custom schema restored with the old helpers keeps it
/// pending after `awa` passes, and the report names that schema.
#[tokio::test]
async fn patch_probe_names_a_stale_custom_schema() -> TestResult {
    let url = std::env::var("DATABASE_URL")?;
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let database = format!("lane_probe_db_{suffix}");
    let migrator = format!("lane_migrator_{suffix}");
    run_sql(&admin, format!("CREATE DATABASE {database}")).await?;
    run_sql(
        &admin,
        format!("CREATE ROLE {migrator} LOGIN PASSWORD 'lane_test'"),
    )
    .await?;
    let options = PgConnectOptions::from_str(&url)?.database(&database);
    let result = exercise_stale_custom_schema(options, &migrator).await;
    run_sql(&admin, format!("DROP DATABASE {database} WITH (FORCE)")).await?;
    run_sql(&admin, format!("DROP ROLE {migrator}")).await?;
    admin.close().await;
    result
}

async fn exercise_stale_custom_schema(options: PgConnectOptions, migrator: &str) -> TestResult {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await?;
    migrations::run(&pool).await?;
    QueueStorage::new(QueueStorageConfig {
        schema: "custom_lanes".into(),
        ..Default::default()
    })?
    .prepare_schema(&pool)
    .await?;
    assert!(migrations::pending_schema_patches(&pool).await?.is_empty());

    // `awa` keeps the new helpers; the custom schema gets the old one back,
    // as a restore from a pre-0.6.10 dump would leave it.
    run_sql(
        &pool,
        include_str!("fixtures/v06_ensure_lane_sequences.sql")
            .replace("__schema__", "custom_lanes"),
    )
    .await?;
    let pending = migrations::pending_schema_patches(&pool).await?;
    assert_eq!(
        pending.iter().map(|patch| patch.name).collect::<Vec<_>>(),
        vec!["prepared_lane_sequences"]
    );
    let status = lane_patch_status(&pool).await?;
    assert!(!status.applied);
    assert_eq!(
        status.needs_repair,
        vec!["lane helpers in schema custom_lanes".to_string()]
    );

    let report = migrations::run_with_report(&pool).await?;
    let lane = report
        .patches
        .iter()
        .find(|patch| patch.name == "prepared_lane_sequences")
        .expect("lane patch report");
    assert_eq!(lane.outcome, migrations::PatchOutcome::Applied);
    assert_eq!(lane.repaired, status.needs_repair);
    assert!(lane.still_needs_repair.is_empty());
    assert!(report.complete());
    assert_eq!(report.version_before, migrations::CURRENT_VERSION);
    assert_eq!(report.version_after, migrations::CURRENT_VERSION);
    assert!(report.migrations_applied.is_empty());
    assert_eq!(report.storage_routing_before, report.storage_routing_after);
    assert!(migrations::pending_schema_patches(&pool).await?.is_empty());

    // A substrate the migrating role cannot use is reported, not skipped.
    QueueStorage::new(QueueStorageConfig {
        schema: "private_lanes".into(),
        ..Default::default()
    })?
    .prepare_schema(&pool)
    .await?;
    run_sql(
        &pool,
        include_str!("fixtures/v06_ensure_lane_sequences.sql")
            .replace("__schema__", "private_lanes"),
    )
    .await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    run_sql(
        &pool,
        format!(
            "GRANT CONNECT ON DATABASE {database} TO {migrator}; \
             GRANT USAGE ON SCHEMA awa TO {migrator}; \
             GRANT SELECT ON ALL TABLES IN SCHEMA awa TO {migrator};"
        ),
    )
    .await?;
    let migrator_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.username(migrator).password("lane_test"))
        .await?;
    let status = lane_patch_status(&migrator_pool).await?;
    assert_eq!(
        status.needs_repair,
        vec!["lane helpers in schema private_lanes".to_string()]
    );
    migrator_pool.close().await;

    let report = migrations::run_with_report(&pool).await?;
    assert!(report.complete());
    pool.close().await;
    Ok(())
}

async fn lane_patch_status(pool: &PgPool) -> Result<migrations::SchemaPatchStatus, sqlx::Error> {
    Ok(migrations::schema_patch_status(pool)
        .await
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?
        .into_iter()
        .find(|status| status.name == "prepared_lane_sequences")
        .expect("lane patch status"))
}
