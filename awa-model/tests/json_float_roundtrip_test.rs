//! A float stored in `jsonb` must read back as the same `f64` that was written.
//!
//! Needs a reachable Postgres at DATABASE_URL; no schema is required.

use sqlx::postgres::PgPoolOptions;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".into())
}

#[tokio::test]
async fn jsonb_floats_read_back_exactly() {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url())
        .await
        .expect("connect");

    // Postgres prints jsonb numbers in plain decimal, so a long one reaches
    // the parser as many digits; the default serde_json parser rounds this
    // value to the neighbouring f64.
    let written = 5.130112436774387e-96_f64;
    let read_back: serde_json::Value = sqlx::query_scalar("SELECT $1::jsonb")
        .bind(serde_json::json!(written))
        .fetch_one(&pool)
        .await
        .expect("round-trip through jsonb");

    assert_eq!(read_back.as_f64(), Some(written));
}
