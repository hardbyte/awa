//! Regression test: the full migration chain must apply under a
//! least-privilege *owner* role, not only under a superuser.
//!
//! The queue-storage migrations discover substrate schemas by scanning
//! `pg_namespace`. The old idiom probed every schema with
//! `to_regprocedure()` / `to_regclass()`, which raise
//! `permission denied for schema X` for any schema the current role lacks
//! `USAGE` on — most notably `pg_toast`. That made the whole migration set
//! apply cleanly only as a superuser (which bypasses schema ACLs), so on
//! managed Postgres — where the migration role owns the `awa` schema but is
//! deliberately not a superuser — provisioning failed at the first such
//! migration.
//!
//! This test provisions a fresh database owned by a `NOSUPERUSER` role and
//! asserts `migrations::run` reaches `CURRENT_VERSION`. It fails against the
//! old scan idiom (with `permission denied for schema pg_toast`) and passes
//! once discovery uses catalog lookups gated on `has_schema_privilege()`.

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

#[tokio::test]
async fn migrations_apply_as_non_superuser_owner() {
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .expect("admin connect");

    // Unique names so concurrent test runs don't collide.
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let role = format!("awa_lp_{suffix}");
    let db = format!("awa_lp_{suffix}");
    let password = "awa_lp_test_pw";

    // A least-privilege login role: explicitly NOSUPERUSER so it is subject
    // to the same schema ACLs (pg_toast etc.) a managed-Postgres owner hits.
    sqlx::query(&format!(
        "CREATE ROLE \"{role}\" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}'"
    ))
    .execute(&admin)
    .await
    .expect("create least-privilege role");

    // A fresh database owned by that role: it can CREATE the awa schema, and
    // every object it creates is owned by it — matching the real deployment.
    // (CREATE DATABASE cannot run inside a transaction.)
    sqlx::query(&format!("CREATE DATABASE \"{db}\" OWNER \"{role}\""))
        .execute(&admin)
        .await
        .expect("create owned database");

    let result = run_migrations_as(&database_url(), &db, &role, password).await;

    // Always tear down, regardless of the outcome.
    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"))
        .execute(&admin)
        .await;
    let _ = sqlx::query(&format!("DROP ROLE IF EXISTS \"{role}\""))
        .execute(&admin)
        .await;
    admin.close().await;

    result.expect("migrations must apply cleanly as a non-superuser owner");
}

async fn run_migrations_as(
    admin_url: &str,
    db: &str,
    role: &str,
    password: &str,
) -> Result<(), String> {
    let opts = PgConnectOptions::from_str(admin_url)
        .map_err(|e| format!("parse DATABASE_URL: {e}"))?
        .database(db)
        .username(role)
        .password(password);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts)
        .await
        .map_err(|e| format!("connect as {role}: {e}"))?;

    // The test only proves something if the role really is non-superuser.
    let is_super: bool =
        sqlx::query_scalar("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
            .fetch_one(&pool)
            .await
            .map_err(|e| format!("check rolsuper: {e}"))?;
    if is_super {
        return Err("test role unexpectedly has SUPERUSER".into());
    }

    awa_model::migrations::run(&pool)
        .await
        .map_err(|e| format!("run migrations: {e}"))?;

    let version = awa_model::migrations::current_version(&pool)
        .await
        .map_err(|e| format!("current_version: {e}"))?;
    pool.close().await;

    if version != awa_model::migrations::CURRENT_VERSION {
        return Err(format!(
            "expected schema version {}, reached {version}",
            awa_model::migrations::CURRENT_VERSION
        ));
    }
    Ok(())
}
