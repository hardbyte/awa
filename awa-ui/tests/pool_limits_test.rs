//! The admin UI pool's session limits bound queries that a dropped request
//! leaves running on Postgres.

use std::time::{Duration, Instant};

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;

async fn show(conn: &mut sqlx::PgConnection, setting: &str) -> String {
    sqlx::query_scalar(&format!("SHOW {setting}"))
        .fetch_one(conn)
        .await
        .unwrap()
}

async fn admin_pool(statement_timeout: Duration) -> PgPool {
    let options: PgConnectOptions = awa_testing::setup::database_url().parse().unwrap();
    awa_ui::admin_pool_options(PgPoolOptions::new())
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(20))
        .connect_with(awa_ui::admin_connect_options(
            options,
            Some(statement_timeout),
        ))
        .await
        .unwrap()
}

#[tokio::test]
async fn statement_timeout_is_the_session_default() {
    let pool = admin_pool(Duration::from_secs(2)).await;
    let mut conn = pool.acquire().await.unwrap();
    assert_eq!(show(&mut conn, "statement_timeout").await, "2s");
    // A startup option, not a session SET, so RESET returns to it.
    sqlx::query("SET statement_timeout = 0")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("RESET statement_timeout")
        .execute(&mut *conn)
        .await
        .unwrap();
    assert_eq!(show(&mut conn, "statement_timeout").await, "2s");

    let version: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    if version >= 140_000 {
        assert_eq!(
            show(&mut conn, "client_connection_check_interval").await,
            "5s"
        );
    }
}

#[tokio::test]
async fn a_dropped_request_frees_its_connection_at_the_statement_timeout() {
    let pool = admin_pool(Duration::from_secs(2)).await;
    let backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();

    // The client gives up on a slow read: its future is dropped mid-query.
    let abandoned = tokio::time::timeout(
        Duration::from_millis(300),
        sqlx::query("SELECT pg_sleep(30)").execute(&pool),
    )
    .await;
    assert!(
        abandoned.is_err(),
        "the read was still running when dropped"
    );

    // The pool's one connection waits for that query to end before it can
    // be reused; the statement timeout ends it, rather than its 30 s sleep.
    let started = Instant::now();
    let after: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    // The same connection, recovered rather than closed: closing it after
    // the timeout's error is the path that hangs over TLS.
    assert_eq!(after, backend);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the next request waited {:?} for the abandoned query",
        started.elapsed()
    );
}
