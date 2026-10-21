use awa_model::{audited_sql, migrations, storage};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::str::FromStr;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const SCHEMAS: [&str; 2] = ["awa", "custom_lanes"];
const RELEASED_0_6_VERSION: i32 = 40;

#[tokio::test]
async fn create_is_revoked_under_running_0_6_runtimes_without_failures() -> TestResult {
    let url = std::env::var("DATABASE_URL")?;
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let owner = format!("revoke_owner_{suffix}");
    let runtime = format!("revoke_runtime_{suffix}");
    let database = format!("revoke_db_{suffix}");
    admin
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE ROLE {owner} LOGIN PASSWORD 'revoke_test'; \
             CREATE ROLE {runtime} LOGIN PASSWORD 'revoke_test';"
        ))))
        .await?;
    admin
        .execute(sqlx::raw_sql(audited_sql(format!(
            "CREATE DATABASE {database} OWNER {owner}"
        ))))
        .await?;
    let options = PgConnectOptions::from_str(&url)?
        .database(&database)
        .password("revoke_test");
    let result = exercise(options, &owner, &runtime).await;
    admin
        .execute(sqlx::raw_sql(audited_sql(format!(
            "DROP DATABASE {database} WITH (FORCE)"
        ))))
        .await?;
    admin
        .execute(sqlx::raw_sql(audited_sql(format!(
            "DROP ROLE {runtime}; DROP ROLE {owner};"
        ))))
        .await?;
    admin.close().await;
    result
}

async fn exercise(options: PgConnectOptions, owner: &str, runtime: &str) -> TestResult {
    let owner_options = options.clone().username(owner);
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(owner_options.clone())
        .await?;
    for (_, _, statement) in migrations::migration_sql_range(0, RELEASED_0_6_VERSION) {
        owner.execute(sqlx::raw_sql(audited_sql(statement))).await?;
    }
    owner
        .execute("SELECT awa.install_queue_storage_substrate('custom_lanes')")
        .await?;
    owner
        .execute(
            "INSERT INTO awa.queue_meta (queue, enqueue_shards) VALUES ('existing', 2) \
             ON CONFLICT (queue) DO UPDATE SET enqueue_shards = 2",
        )
        .await?;
    for schema in SCHEMAS {
        let released_helpers =
            include_str!("fixtures/v040_lane_sequence_helpers.sql").replace("__schema__", schema);
        owner
            .execute(sqlx::raw_sql(audited_sql(released_helpers)))
            .await?;
        owner
            .execute(sqlx::raw_sql(audited_sql(format!(
                r#"
                GRANT USAGE, CREATE ON SCHEMA {schema} TO {runtime};
                GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {schema} TO {runtime};
                GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {schema} TO {runtime};
                GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {runtime};
                "#
            ))))
            .await?;
    }
    let runtime_options = options.username(runtime);
    let mut long_lived = PgConnection::connect_with(&runtime_options).await?;
    for schema in SCHEMAS {
        create_lane_as_0_6(&mut long_lived, schema, "existing", 2, 0).await?;
        reserve(&mut long_lived, schema, "existing", 2, 0).await?;
    }

    revoke_create(&owner, runtime).await?;
    let error = reserve(&mut long_lived, "awa", "existing", 2, 0)
        .await
        .expect_err("0.6 helpers need CREATE even for an existing sequence");
    assert_eq!(sqlstate(&error), Some("42501".into()));
    grant_create(&owner, runtime).await?;

    owner
        .execute(sqlx::raw_sql(audited_sql(format!(
            "GRANT TRIGGER ON awa.queue_lanes TO {runtime}"
        ))))
        .await?;
    let blocked = storage::prepare_create_revoke(&owner).await?;
    let awa_outcome = blocked
        .iter()
        .find(|outcome| outcome.schema == "awa")
        .unwrap();
    assert!(!awa_outcome.prepared);
    assert!(awa_outcome
        .blocking
        .iter()
        .any(|violation| violation
            == &format!("relation awa.queue_lanes: TRIGGER granted to {runtime}")));
    assert!(!helper_is_guarded(&owner, "awa").await?);
    owner
        .execute(sqlx::raw_sql(audited_sql(format!(
            "REVOKE TRIGGER ON awa.queue_lanes FROM {runtime}"
        ))))
        .await?;

    let impatient = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _| {
            Box::pin(async move {
                conn.execute("SET lock_timeout = '2s'").await?;
                Ok(())
            })
        })
        .connect_with(owner_options.clone())
        .await?;
    let mut in_flight = PgConnection::connect_with(&runtime_options).await?;
    let mut open_enqueue = in_flight.begin().await?;
    sqlx::query("SELECT awa.reserve_enqueue_seq('existing', 2::smallint, 0::smallint, 1::bigint)")
        .execute(open_enqueue.as_mut())
        .await?;

    let prepared = storage::prepare_create_revoke(&impatient).await?;
    assert_eq!(
        prepared
            .iter()
            .map(|outcome| outcome.schema.as_str())
            .collect::<Vec<_>>(),
        SCHEMAS
    );
    for outcome in &prepared {
        assert!(outcome.prepared, "{outcome:?}");
        assert!(outcome.blocking.is_empty());
        assert_eq!(
            outcome.revoke,
            vec![format!(
                "REVOKE CREATE ON SCHEMA \"{}\" FROM {runtime};",
                outcome.schema
            )]
        );
        assert!(helper_is_guarded(&owner, &outcome.schema).await?);
    }
    let awa_outcome = prepared
        .iter()
        .find(|outcome| outcome.schema == "awa")
        .unwrap();
    assert_eq!(awa_outcome.provisioned_lanes, 4 * 2 - 1);
    let mut sequence_owner = PgConnection::connect_with(&runtime_options).await?;
    for outcome in &prepared {
        assert_eq!(outcome.sequence_grants.len(), 1, "{outcome:?}");
        let grant = &outcome.sequence_grants[0];
        assert_eq!(grant.run_as, runtime);
        sequence_owner
            .execute(sqlx::raw_sql(audited_sql(grant.statement.clone())))
            .await?;
        impatient
            .execute(sqlx::raw_sql(audited_sql(outcome.revoke.join("\n"))))
            .await?;
    }
    sequence_owner.close().await?;
    open_enqueue.commit().await?;
    in_flight.close().await?;
    impatient.close().await;

    for schema in SCHEMAS {
        reserve(&mut long_lived, schema, "existing", 2, 0).await?;
    }
    let mut restarted = PgConnection::connect_with(&runtime_options).await?;
    create_lane_as_0_6(&mut restarted, "awa", "existing", 1, 1).await?;
    reserve(&mut restarted, "awa", "existing", 1, 1).await?;
    let error = create_lane_as_0_6(&mut restarted, "awa", "unprovisioned", 2, 0)
        .await
        .expect_err("a queue never used before needs prepare-queue");
    assert_eq!(sqlstate(&error), Some("42501".into()));

    migrations::run(&owner).await?;
    assert_eq!(
        migrations::current_version(&owner).await?,
        migrations::CURRENT_VERSION
    );
    for schema in SCHEMAS {
        reserve(&mut long_lived, schema, "existing", 2, 0).await?;
    }
    long_lived.close().await?;
    restarted.close().await?;
    owner.close().await;
    Ok(())
}

/// The lane rows a 0.6 runtime inserts on its first enqueue to a lane. Their
/// triggers create the lane's sequences through the lane helpers.
async fn create_lane_as_0_6(
    conn: &mut PgConnection,
    schema: &str,
    queue: &str,
    priority: i16,
    shard: i16,
) -> Result<(), sqlx::Error> {
    let mut tx = conn.begin().await?;
    for insert in [
        format!(
            "INSERT INTO {schema}.queue_lanes (queue, priority) \
             SELECT $1, $2 WHERE $3::smallint IS NOT NULL ON CONFLICT (queue, priority) DO NOTHING"
        ),
        format!(
            "INSERT INTO {schema}.queue_enqueue_heads (queue, priority, enqueue_shard) \
             VALUES ($1, $2, $3) ON CONFLICT (queue, priority, enqueue_shard) DO NOTHING"
        ),
        format!(
            "INSERT INTO {schema}.queue_claim_heads (queue, priority, enqueue_shard) \
             VALUES ($1, $2, $3) ON CONFLICT (queue, priority, enqueue_shard) DO NOTHING"
        ),
    ] {
        sqlx::query(audited_sql(insert))
            .bind(queue)
            .bind(priority)
            .bind(shard)
            .execute(tx.as_mut())
            .await?;
    }
    tx.commit().await
}

async fn reserve(
    conn: &mut PgConnection,
    schema: &str,
    queue: &str,
    priority: i16,
    shard: i16,
) -> Result<i64, sqlx::Error> {
    let mut tx = conn.begin().await?;
    let start = sqlx::query_scalar(audited_sql(format!(
        "SELECT {schema}.reserve_enqueue_seq($1, $2, $3, 1::bigint)"
    )))
    .bind(queue)
    .bind(priority)
    .bind(shard)
    .fetch_one(tx.as_mut())
    .await?;
    tx.commit().await?;
    Ok(start)
}

async fn revoke_create(owner: &PgPool, runtime: &str) -> Result<(), sqlx::Error> {
    for schema in SCHEMAS {
        owner
            .execute(sqlx::raw_sql(audited_sql(format!(
                "REVOKE CREATE ON SCHEMA {schema} FROM {runtime}"
            ))))
            .await?;
    }
    Ok(())
}

async fn grant_create(owner: &PgPool, runtime: &str) -> Result<(), sqlx::Error> {
    for schema in SCHEMAS {
        owner
            .execute(sqlx::raw_sql(audited_sql(format!(
                "GRANT CREATE ON SCHEMA {schema} TO {runtime}"
            ))))
            .await?;
    }
    Ok(())
}

async fn helper_is_guarded(owner: &PgPool, schema: &str) -> Result<bool, sqlx::Error> {
    let definition: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef(to_regprocedure(format('%I.ensure_lane_sequences(text, smallint, smallint)', $1::text)))",
    )
    .bind(schema)
    .fetch_one(owner)
    .await?;
    Ok(definition.contains("has not been provisioned"))
}

fn sqlstate(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .map(|code| code.into_owned())
}
