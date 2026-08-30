use assert_cmd::Command;

fn rendered_help(args: &[&str], env_name: &str, secret: &str) -> String {
    let output = Command::cargo_bin("awa")
        .expect("awa binary")
        .env(env_name, secret)
        .args(args)
        .output()
        .expect("run awa help command");

    assert!(output.status.success());
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn help_does_not_disclose_environment_database_url() {
    let secret_url = "postgres://secret-user:secret-password@db.example/awa";
    let rendered = rendered_help(&["--help"], "DATABASE_URL", secret_url);
    assert!(rendered.contains("DATABASE_URL"));
    assert!(!rendered.contains(secret_url));
    assert!(!rendered.contains("secret-password"));
}

#[test]
fn serve_help_does_not_disclose_callback_secret() {
    let secret = "serve-callback-secret-sentinel";
    let rendered = rendered_help(&["serve", "--help"], "AWA_CALLBACK_HMAC_SECRET", secret);
    assert!(rendered.contains("AWA_CALLBACK_HMAC_SECRET"));
    assert!(!rendered.contains(secret));
}

#[test]
fn callback_receiver_help_does_not_disclose_callback_secret() {
    let secret = "receiver-callback-secret-sentinel";
    let rendered = rendered_help(
        &["callbacks", "serve", "--help"],
        "AWA_CALLBACK_HMAC_SECRET",
        secret,
    );
    assert!(rendered.contains("AWA_CALLBACK_HMAC_SECRET"));
    assert!(!rendered.contains(secret));
}
