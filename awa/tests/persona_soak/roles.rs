//! The database roles of the shard-ops persona: the shape of a production install that
//! runs its application processes without superuser or schema ownership.
//!
//! * `OWNER` owns the database and the `awa` schema, runs migrations and `prepare_queue`.
//! * `MIGRATOR` is a migration-like role that is a member of `OWNER` and has default
//!   privileges for the objects it creates.
//! * `APP_A` and `APP_B` are members of the `RUNTIME_GROUP` role and have no default
//!   privileges of their own. They run producers and the maintenance leader.
//!
//! Grants follow the restricted-runtime section of `docs/security.md`.

use super::config::{replace_credentials, SoakConfig};
use super::db::connect_pool;
use sqlx::PgPool;

pub const OWNER: &str = "awa_soak_owner";
pub const MIGRATOR: &str = "awa_soak_migrator";
pub const RUNTIME_GROUP: &str = "awa_soak_runtime";
pub const APP_A: &str = "awa_soak_app_a";
pub const APP_B: &str = "awa_soak_app_b";
const PASSWORD: &str = "soak";

pub fn url_for_role(config: &SoakConfig, role: &str) -> String {
    replace_credentials(&config.database_url, role, PASSWORD)
}

/// Creates the cluster-level roles. Safe to repeat.
pub async fn create_roles(admin: &PgPool) -> Result<(), String> {
    let login = [OWNER, MIGRATOR, APP_A, APP_B];
    let statements: Vec<String> = login
        .iter()
        .map(|role| {
            format!(
                "DO $$ BEGIN
                   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                     CREATE ROLE {role} LOGIN PASSWORD '{PASSWORD}' NOSUPERUSER NOCREATEDB NOCREATEROLE;
                   END IF;
                 END $$"
            )
        })
        .chain([
            format!(
                "DO $$ BEGIN
                   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{RUNTIME_GROUP}') THEN
                     CREATE ROLE {RUNTIME_GROUP} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
                   END IF;
                 END $$"
            ),
            format!("GRANT {OWNER} TO {MIGRATOR}"),
            format!("GRANT {RUNTIME_GROUP} TO {APP_A}, {APP_B}"),
        ])
        .collect();
    for statement in statements {
        sqlx::query(&statement)
            .execute(admin)
            .await
            .map_err(|error| format!("create roles: {error}"))?;
    }
    Ok(())
}

/// Applies the restricted-runtime grants. Run once the schema is migrated, over a
/// superuser connection to the soak database.
pub async fn grant_runtime(
    superuser_pool: &PgPool,
    database: &str,
    schema_create: bool,
    mixed_personas: bool,
) -> Result<(), String> {
    let mut statements = vec![
        format!("GRANT USAGE ON SCHEMA awa TO {RUNTIME_GROUP}"),
        format!("GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA awa TO {RUNTIME_GROUP}"),
        format!("GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA awa TO {RUNTIME_GROUP}"),
        format!("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa TO {RUNTIME_GROUP}"),
        format!("REVOKE EXECUTE ON FUNCTION awa.install_queue_storage_substrate(TEXT, INT, INT, INT, BOOLEAN) FROM {RUNTIME_GROUP}"),
        format!("GRANT TEMP ON DATABASE {database} TO {RUNTIME_GROUP}"),
    ];
    if schema_create {
        statements.push(format!("GRANT CREATE ON SCHEMA awa TO {RUNTIME_GROUP}"));
    }
    let mut creators = vec![OWNER.to_string(), MIGRATOR.to_string()];
    if mixed_personas {
        // The other personas keep their superuser runtimes and producers. Their objects
        // must stay usable by the application roles, which now lead maintenance for every
        // queue, without covering anything the application roles create themselves.
        let superuser: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(superuser_pool)
            .await
            .map_err(|error| format!("current_user: {error}"))?;
        creators.push(superuser);
    }
    for creator in &creators {
        statements.push(format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {creator} IN SCHEMA awa GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO {RUNTIME_GROUP}"
        ));
        statements.push(format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {creator} IN SCHEMA awa GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {RUNTIME_GROUP}"
        ));
        statements.push(format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {creator} IN SCHEMA awa GRANT EXECUTE ON FUNCTIONS TO {RUNTIME_GROUP}"
        ));
    }
    for statement in statements {
        sqlx::query(&statement)
            .execute(superuser_pool)
            .await
            .map_err(|error| format!("grant runtime privileges ({statement}): {error}"))?;
    }
    Ok(())
}

/// A pool that connects as `role`.
pub async fn pool_as(
    config: &SoakConfig,
    role: &str,
    application_name: &str,
    max_connections: u32,
) -> Result<PgPool, String> {
    connect_pool(
        &url_for_role(config, role),
        application_name,
        max_connections,
    )
    .await
    .map_err(|error| format!("connect as {role}: {error}"))
}
