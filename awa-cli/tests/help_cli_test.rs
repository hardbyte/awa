use assert_cmd::Command;

#[test]
fn help_does_not_disclose_environment_database_url() {
    let secret_url = "postgres://secret-user:secret-password@db.example/awa";
    let output = Command::cargo_bin("awa")
        .expect("awa binary")
        .env("DATABASE_URL", secret_url)
        .arg("--help")
        .output()
        .expect("run awa --help");

    assert!(output.status.success());
    let rendered = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(rendered.contains("DATABASE_URL"));
    assert!(!rendered.contains(secret_url));
    assert!(!rendered.contains("secret-password"));
}
