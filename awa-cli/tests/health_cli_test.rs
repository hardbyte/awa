//! `awa health` CLI probe tests (#368).
//!
//! Read-only against the standard test database (plus one unreachable-URL
//! case), so no schema isolation is needed.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use assert_cmd::Command;
use sqlx::postgres::PgPoolOptions;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn ensure_migrated() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .expect("Failed to connect to test database — is Postgres running?");
    awa_model::migrations::run(&pool)
        .await
        .expect("migrations should apply");
}

#[tokio::test]
async fn test_health_ready_against_migrated_database() {
    ensure_migrated().await;

    let assert = tokio::task::spawn_blocking(move || {
        let mut command = Command::cargo_bin("awa").expect("awa binary should build");
        command
            .arg("--database-url")
            .arg(database_url())
            .arg("health")
            .arg("--json")
            .assert()
    })
    .await
    .expect("command task should join");

    let output = assert.success().get_output().stdout.clone();
    let report: serde_json::Value =
        serde_json::from_slice(&output).expect("health --json should emit JSON");
    assert_eq!(report["ready"], true);
    assert_eq!(report["postgres_connected"], true);
    assert_eq!(report["schema_compatible"], true);
    assert_eq!(
        report["expected_schema_version"],
        awa_model::migrations::CURRENT_VERSION
    );
    assert!(report["storage_state"].is_string());
}

#[tokio::test]
async fn test_health_unreachable_database_exits_nonzero_with_report() {
    let assert = tokio::task::spawn_blocking(move || {
        let mut command = Command::cargo_bin("awa").expect("awa binary should build");
        command
            .arg("--database-url")
            .arg("postgres://postgres:wrong@127.0.0.1:9/awa_nowhere")
            .arg("health")
            .arg("--json")
            .arg("--connect-timeout")
            .arg("2")
            .assert()
    })
    .await
    .expect("command task should join");

    let output = assert.failure().code(1).get_output().stdout.clone();
    let report: serde_json::Value =
        serde_json::from_slice(&output).expect("health --json should emit JSON even when down");
    assert_eq!(report["ready"], false);
    assert_eq!(report["postgres_connected"], false);
}
