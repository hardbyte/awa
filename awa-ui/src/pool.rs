//! Session limits for the connection pool that serves the admin UI.
//!
//! When a client gives up on a request (a closed tab, a proxy timeout, a
//! poll that outlives its interval), the handler is dropped but its query
//! keeps running on Postgres: nothing cancels it. sqlx then waits for that
//! query to finish before the connection can rejoin the pool. A slow admin
//! read polled from an open dashboard therefore stacks one running query
//! and one held connection per poll.
//!
//! These settings bound both: a statement timeout ends the abandoned query,
//! and the release hook returns its connection to the pool afterwards.
//! Without that hook, sqlx 0.9 treats the abandoned query's error as a broken
//! connection and closes it, and closing a TLS connection mid-stream hangs,
//! so the pool slot would never be freed.

use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgConnection, PgPoolOptions};
use sqlx::Connection;

/// `client_connection_check_interval` exists from Postgres 14.
const CLIENT_CONNECTION_CHECK_MIN_VERSION: u32 = 140_000;

/// Add `statement_timeout` to the connection's startup options, so no single
/// admin statement runs longer and `RESET statement_timeout` returns to this
/// value rather than to none. `None` or a zero duration leaves the server's
/// setting in place.
pub fn admin_connect_options(
    options: PgConnectOptions,
    statement_timeout: Option<Duration>,
) -> PgConnectOptions {
    match statement_timeout.filter(|timeout| !timeout.is_zero()) {
        Some(timeout) => {
            // Round up: a sub-millisecond timeout written as `0ms` would
            // disable the limit instead of setting one.
            let millis = timeout.as_micros().div_ceil(1000);
            options.options([("statement_timeout", format!("{millis}ms"))])
        }
        None => options,
    }
}

/// Set the session limits that need a server version check, on each new
/// connection. Use with [`PgPoolOptions::after_connect`], or through
/// [`admin_pool_options`].
pub async fn configure_admin_connection(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    if conn
        .server_version_num()
        .is_some_and(|version| version >= CLIENT_CONNECTION_CHECK_MIN_VERSION)
    {
        // While a query runs, Postgres checks every 5 s whether the client is
        // still connected, so a connection the UI server closed, or lost when
        // it exited, stops its query instead of running it to the end.
        sqlx::query("SET client_connection_check_interval = '5s'")
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

/// Make a released connection reusable after its request was dropped
/// mid-query. Use with [`PgPoolOptions::after_release`], or through
/// [`admin_pool_options`].
///
/// The first ping waits for the abandoned query and reports its result: a
/// database error there (the statement timeout, say) belongs to that query,
/// not to the connection, which a second ping confirms. Never returns an
/// error: sqlx closes such a connection with a hard close, which hangs on a
/// TLS connection and leaks its pool slot. `Ok(false)` closes it gracefully.
pub async fn recover_admin_connection(conn: &mut PgConnection) -> Result<bool, sqlx::Error> {
    match conn.ping().await {
        Ok(()) => Ok(true),
        Err(sqlx::Error::Database(_)) => Ok(conn.ping().await.is_ok()),
        Err(_) => Ok(false),
    }
}

/// `options` with [`configure_admin_connection`] on every new connection and
/// [`recover_admin_connection`] on every release.
pub fn admin_pool_options(options: PgPoolOptions) -> PgPoolOptions {
    options
        .after_connect(|conn, _meta| Box::pin(configure_admin_connection(conn)))
        .after_release(|conn, _meta| Box::pin(recover_admin_connection(conn)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timeout_option(timeout: Option<Duration>) -> Option<String> {
        admin_connect_options(PgConnectOptions::new(), timeout)
            .get_options()
            .map(str::to_owned)
    }

    #[test]
    fn statement_timeout_is_written_in_whole_milliseconds() {
        assert_eq!(
            timeout_option(Some(Duration::from_secs(60))).as_deref(),
            Some("-c statement_timeout=60000ms")
        );
        assert_eq!(
            timeout_option(Some(Duration::from_micros(500))).as_deref(),
            Some("-c statement_timeout=1ms")
        );
        assert_eq!(
            timeout_option(Some(Duration::from_micros(1_500))).as_deref(),
            Some("-c statement_timeout=2ms")
        );
        assert_eq!(timeout_option(Some(Duration::ZERO)), None);
        assert_eq!(timeout_option(None), None);
    }
}
