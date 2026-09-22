//! `awa storage prepare-queue-storage-schema --reset --schema awa` must be
//! hard-rejected because the `awa` schema is also where the canonical
//! migration tables live; a `DROP SCHEMA awa CASCADE` would take them with
//! it and leave the database unrecoverable.
//!
//! See `docs/queue-storage-substrate.md` for the contract.

use assert_cmd::Command;
use awa_testing::setup::TestDatabase;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

fn run_cli(args: &[&str]) -> Command {
    let mut command = Command::cargo_bin("awa").expect("awa binary should build");
    command
        .env("DATABASE_URL", database_url())
        .env("RUST_LOG", "warn")
        .args(args);
    command
}

#[test]
fn reset_on_default_awa_schema_is_rejected() {
    let assert = run_cli(&[
        "storage",
        "prepare-queue-storage-schema",
        "--schema",
        "awa",
        "--reset",
    ])
    .assert()
    .failure();
    let stderr = String::from_utf8(assert.get_output().stderr.clone()).expect("stderr utf8");
    assert!(
        stderr.contains("Refusing to DROP SCHEMA awa CASCADE"),
        "expected the reset-guard error message, got: {stderr}"
    );
    assert!(
        stderr.contains("--schema <other>"),
        "guard error must point operators at the custom-schema escape, got: {stderr}"
    );
    assert!(
        stderr.contains("awa storage abort"),
        "guard error must point operators at the abort flow, got: {stderr}"
    );
}

#[tokio::test]
async fn reset_on_default_awa_schema_does_not_touch_the_database() {
    // The guard fires before any SQL runs. Confirm by asserting that
    // `awa.schema_version` is still populated after a rejected reset.
    let db = TestDatabase::canonical().await;
    let pool = db.pool();
    let version_before: i32 = sqlx::query_scalar("SELECT MAX(version) FROM awa.schema_version")
        .fetch_one(pool)
        .await
        .expect("schema_version exists pre-call");

    let database_url = db.url();
    tokio::task::spawn_blocking(move || {
        run_cli(&[
            "storage",
            "prepare-queue-storage-schema",
            "--schema",
            "awa",
            "--reset",
        ])
        .env("DATABASE_URL", database_url)
        .assert()
        .failure();
    })
    .await
    .expect("blocking task");

    let version_after: i32 = sqlx::query_scalar("SELECT MAX(version) FROM awa.schema_version")
        .fetch_one(pool)
        .await
        .expect("schema_version still exists after rejected reset");
    assert_eq!(
        version_before, version_after,
        "rejected reset must not mutate schema_version"
    );
}
