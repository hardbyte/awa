//! PyO3 wrapper around the awa-ui axum server.
//!
//! Mirrors `awa-cli`'s `Serve` command so `python -m awa serve` runs the
//! same dashboard in-process — no separate Rust binary required. The
//! function blocks on the calling thread until the server exits (Ctrl+C
//! / SIGTERM via tokio's default ctrl_c handler).

use std::time::Duration;

use awa_ui::state::ReadOnlyMode;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use sqlx::postgres::PgPoolOptions;

use crate::errors;

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    database_url,
    *,
    host = "127.0.0.1".to_string(),
    port = 3000,
    pool_max = 10,
    pool_min = 2,
    pool_idle_timeout = 300,
    pool_max_lifetime = 1800,
    pool_acquire_timeout = 10,
    cache_ttl = 5,
    callback_hmac_secret = None,
    read_only = false,
))]
pub fn serve(
    py: Python<'_>,
    database_url: String,
    host: String,
    port: u16,
    pool_max: u32,
    pool_min: u32,
    pool_idle_timeout: u64,
    pool_max_lifetime: u64,
    pool_acquire_timeout: u64,
    cache_ttl: u64,
    callback_hmac_secret: Option<String>,
    read_only: bool,
) -> PyResult<()> {
    let callback_hmac_secret = callback_hmac_secret
        .as_deref()
        .map(parse_callback_hmac_secret)
        .transpose()
        .map_err(|err| PyValueError::new_err(format!("invalid callback HMAC secret: {err}")))?;
    let read_only_mode = if read_only {
        ReadOnlyMode::ReadOnly
    } else {
        ReadOnlyMode::Auto
    };
    let cache_duration = Duration::from_secs(cache_ttl);
    let addr = format!("{host}:{port}");

    py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let pool = PgPoolOptions::new()
                .max_connections(pool_max)
                .min_connections(pool_min)
                .idle_timeout(Duration::from_secs(pool_idle_timeout))
                .max_lifetime(Duration::from_secs(pool_max_lifetime))
                .acquire_timeout(Duration::from_secs(pool_acquire_timeout))
                .connect(&database_url)
                .await
                .map_err(errors::map_connect_error)?;

            let app =
                awa_ui::router_with(pool, cache_duration, callback_hmac_secret, read_only_mode)
                    .await
                    .map_err(errors::map_sqlx_error)?;

            let listener = tokio::net::TcpListener::bind(&addr)
                .await
                .map_err(|err| PyValueError::new_err(format!("bind {addr}: {err}")))?;
            if read_only {
                tracing::info!("AWA UI listening on http://{addr} (forced read-only)");
            } else {
                tracing::info!("AWA UI listening on http://{addr}");
            }

            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .map_err(|err| PyValueError::new_err(format!("server error: {err}")))?;

            Ok::<(), PyErr>(())
        })
    })
}

fn parse_callback_hmac_secret(secret: &str) -> Result<[u8; 32], String> {
    let bytes =
        hex::decode(secret).map_err(|_| "callback HMAC secret must be valid hex".to_string())?;
    <[u8; 32]>::try_from(bytes.as_slice())
        .map_err(|_| "callback HMAC secret must be exactly 32 bytes (64 hex characters)".into())
}

/// Resolves on Ctrl+C so axum's graceful_shutdown drops the listener cleanly.
async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        tracing::warn!("ctrl_c handler failed: {err}");
    }
    tracing::info!("shutdown signal received, stopping server");
}
