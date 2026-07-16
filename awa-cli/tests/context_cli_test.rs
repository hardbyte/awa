//! Named-context CLI safety-rule tests (#437).
//!
//! Exercises the end-to-end behaviour of `~/.config/awa/config.toml`
//! contexts through the real binary: `awa context list/show`, the
//! explicit-context requirement for mutating commands, and the
//! `production = true` confirmation gate. Pure resolution/redaction logic
//! is unit-tested in `src/context.rs`; these tests pin the wiring.
//!
//! Only the `--yes` success path touches the database (a queue
//! pause/resume pair). Set
//! DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use assert_cmd::Command;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

/// Write a config file into a fresh per-test directory and return its path.
fn write_config(test_name: &str, contents: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir()
        .join("awa-context-cli-test")
        .join(format!("{}-{}", std::process::id(), test_name));
    std::fs::create_dir_all(&dir).expect("create temp config dir");
    let path = dir.join("config.toml");
    std::fs::write(&path, contents).expect("write temp config");
    path
}

/// An `awa` invocation isolated from the developer's real config and env.
fn awa(config_path: &std::path::Path) -> Command {
    let mut command = Command::cargo_bin("awa").expect("awa binary should build");
    command
        .env("AWA_CONFIG", config_path)
        .env_remove("AWA_CONTEXT")
        .env_remove("DATABASE_URL");
    command
}

fn two_context_config(test_name: &str) -> std::path::PathBuf {
    write_config(
        test_name,
        &format!(
            r#"
            default_context = "local"

            [contexts.local]
            url = "{url}"

            [contexts.prod]
            url = "{url}"
            production = true
            "#,
            url = database_url(),
        ),
    )
}

#[test]
fn context_list_shows_contexts_with_redacted_targets() {
    let config = two_context_config("list");
    let assert = awa(&config).args(["context", "list"]).assert().success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout).to_string();
    assert!(stdout.contains("local"), "{stdout}");
    assert!(stdout.contains("prod"), "{stdout}");
    // The test database URL has a password; it must never be printed.
    assert!(!stdout.contains(":test@"), "{stdout}");
    assert!(stdout.contains("REDACTED"), "{stdout}");
}

#[test]
fn context_show_defaults_to_default_context() {
    let config = two_context_config("show");
    let assert = awa(&config).args(["context", "show"]).assert().success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout).to_string();
    assert!(stdout.contains("name:        local"), "{stdout}");
}

#[test]
fn mutation_refuses_env_fallback_with_multiple_contexts() {
    let config = two_context_config("env-fallback");
    // DATABASE_URL alone must not satisfy a mutating command once several
    // contexts exist — that stale-shell-variable path is the foot-gun.
    let assert = awa(&config)
        .env("DATABASE_URL", database_url())
        .args(["queue", "pause", "ctx-test-env-fallback"])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).to_string();
    assert!(stderr.contains("--context"), "{stderr}");
}

#[test]
fn read_only_command_may_use_default_context() {
    let config = two_context_config("read-only");
    awa(&config).args(["queue", "stats"]).assert().success();
}

#[test]
fn production_context_refuses_mutation_without_yes_when_non_interactive() {
    let config = two_context_config("prod-gate");
    let assert = awa(&config)
        .args(["--context", "prod", "queue", "pause", "ctx-test-prod-gate"])
        .write_stdin("") // stdin is a pipe, not a tty
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).to_string();
    assert!(stderr.contains("production = true"), "{stderr}");
    assert!(stderr.contains("--yes"), "{stderr}");
}

#[test]
fn production_context_with_yes_proceeds_and_echoes_target() {
    let config = two_context_config("prod-yes");
    let assert = awa(&config)
        .args([
            "--context",
            "prod",
            "--yes",
            "queue",
            "pause",
            "ctx-test-prod-yes",
        ])
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).to_string();
    assert!(stderr.contains("context 'prod'"), "{stderr}");
    assert!(stderr.contains("[production]"), "{stderr}");
    assert!(
        !stderr.contains(":test@"),
        "password must be stripped: {stderr}"
    );

    // Clean up the pause.
    awa(&config)
        .args([
            "--context",
            "prod",
            "--yes",
            "queue",
            "resume",
            "ctx-test-prod-yes",
        ])
        .assert()
        .success();
}
