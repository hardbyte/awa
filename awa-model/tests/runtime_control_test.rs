use awa_model::{migrations, runtime_control};
use sqlx::{Executor, PgPool};
use uuid::Uuid;

#[sqlx::test(migrations = false)]
async fn halt_resume_is_durable_cas_and_patch_replays_without_clearing_it(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    let first = runtime_control::halt(
        &pool,
        runtime_control::PRUNE_SCOPE,
        "lock pressure",
        "drain",
    )
    .await
    .unwrap();
    let second =
        runtime_control::halt(&pool, runtime_control::PRUNE_SCOPE, "new decision", "drain")
            .await
            .unwrap();
    assert_ne!(first.halt_id, second.halt_id);
    assert!(second.revision > first.revision);
    assert!(
        runtime_control::resume(&pool, runtime_control::PRUNE_SCOPE, first.halt_id.unwrap())
            .await
            .is_err()
    );
    let patch = include_str!("../migrations/patches/prune_controls.sql");
    pool.execute(sqlx::raw_sql(patch)).await.unwrap();
    let observed = runtime_control::read(&mut pool.acquire().await.unwrap())
        .await
        .unwrap();
    assert_eq!(observed.halt_id, second.halt_id);
    assert_eq!(observed.revision, second.revision);
    // No runtime acknowledgments or participant table are involved in drain resume.
    let resumed =
        runtime_control::resume(&pool, runtime_control::PRUNE_SCOPE, second.halt_id.unwrap())
            .await
            .unwrap();
    assert!(resumed.halt_id.is_none());
    assert!(resumed.revision > second.revision);
    assert!(
        runtime_control::resume(&pool, runtime_control::PRUNE_SCOPE, second.halt_id.unwrap())
            .await
            .is_err()
    );
    for scope in [
        "dispatch",
        "cron",
        "maintenance.promote",
        "maintenance.rescue",
        "all",
    ] {
        assert!(runtime_control::halt(&pool, scope, "unsupported", "drain")
            .await
            .is_err());
    }
    assert!(
        runtime_control::halt(&pool, runtime_control::PRUNE_SCOPE, "unsupported", "cancel")
            .await
            .is_err()
    );
}

#[sqlx::test(migrations = false)]
async fn operator_boundary_survives_broad_default_privileges_and_replay(pool: PgPool) {
    for (_, _, sql) in migrations::migration_sql_range(0, 40) {
        pool.execute(sqlx::raw_sql(&sql)).await.unwrap();
    }
    // Unique cluster roles allow PG17/18 runs and parallel scratch databases.
    let suffix = Uuid::new_v4().simple().to_string();
    let producer = format!("control_producer_{suffix}");
    let operator = format!("control_operator_{suffix}");
    pool.execute(sqlx::raw_sql(&format!("CREATE ROLE {producer}; CREATE ROLE {operator}; ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES TO {producer}; ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {producer};"))).await.unwrap();
    migrations::run(&pool).await.unwrap();
    pool.execute(sqlx::raw_sql(&format!("GRANT USAGE ON SCHEMA awa TO {producer}, {operator}; GRANT SELECT ON awa.runtime_controls TO {producer}; GRANT EXECUTE ON FUNCTION awa.halt_runtime_scope(text,text,text), awa.resume_runtime_scope(text,uuid) TO {operator};"))).await.unwrap();
    let patch = include_str!("../migrations/patches/prune_controls.sql");
    pool.execute(sqlx::raw_sql(patch)).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    conn.execute(sqlx::raw_sql(&format!("SET ROLE {producer}")))
        .await
        .unwrap();
    for sql in [
        "SELECT * FROM awa.halt_runtime_scope('maintenance.prune','no','drain')",
        "SELECT * FROM awa.resume_runtime_scope('maintenance.prune',gen_random_uuid())",
        "UPDATE awa.runtime_controls SET halt_id = NULL",
        "DELETE FROM awa.runtime_controls",
        "TRUNCATE awa.runtime_controls",
        "INSERT INTO awa.runtime_controls(scope) VALUES ('maintenance.prune')",
    ] {
        let err = conn.execute(sql).await.unwrap_err();
        assert_eq!(
            err.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("42501"),
            "{sql}: {err}"
        );
    }
    conn.execute(sqlx::raw_sql(&format!("SET ROLE {operator}")))
        .await
        .unwrap();
    let halt: runtime_control::RuntimeControl = sqlx::query_as(
        "SELECT * FROM awa.halt_runtime_scope('maintenance.prune','allowed','drain')",
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    assert!(halt.halt_id.is_some());
    conn.execute("RESET ROLE").await.unwrap();
    // Remove ACL dependencies before dropping cluster-global test roles.
    pool.execute(sqlx::raw_sql(
        &(format!("DROP OWNED BY {producer}, {operator}; DROP ROLE {producer}, {operator};")),
    ))
    .await
    .unwrap();
}

#[sqlx::test(migrations = false)]
async fn controls_survive_installer_patch_replay_in_either_order(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    let version = migrations::current_version(&pool).await.unwrap();
    let halt = runtime_control::halt(
        &pool,
        runtime_control::PRUNE_SCOPE,
        "installer ordering",
        "drain",
    )
    .await
    .unwrap();
    let control = migrations::SCHEMA_PATCHES
        .iter()
        .find(|patch| patch.name == "prune_controls")
        .unwrap();
    assert!(!control.sql.contains("install_queue_storage_substrate"));
    // Replaying substrate installer patches before and after controls must
    // retain the operator's decision without an installer dependency here.
    for before in [true, false] {
        if before {
            pool.execute(sqlx::raw_sql(control.sql)).await.unwrap();
        }
        for patch in migrations::SCHEMA_PATCHES
            .iter()
            .filter(|patch| patch.name != "prune_controls")
        {
            pool.execute(sqlx::raw_sql(patch.sql)).await.unwrap();
        }
        if !before {
            pool.execute(sqlx::raw_sql(control.sql)).await.unwrap();
        }
        let observed = runtime_control::read(&mut pool.acquire().await.unwrap())
            .await
            .unwrap();
        assert_eq!(observed.halt_id, halt.halt_id);
        assert_eq!(observed.revision, halt.revision);
        assert_eq!(migrations::current_version(&pool).await.unwrap(), version);
    }
}
