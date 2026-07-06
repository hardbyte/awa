//! Opt-in health/readiness listener for the worker runtime (#368).
//!
//! Serves exactly two endpoints for infrastructure probes:
//!
//! - `GET /healthz` — process liveness. Returns `200` for as long as the
//!   listener task is running, including during graceful drain: a draining
//!   worker must not be killed by a liveness probe.
//! - `GET /readyz` — readiness. Returns `200` only while the runtime can do
//!   useful work: database reachable, schema at least the binary's expected
//!   version, every queue's claim loop ticking, heartbeat and maintenance
//!   services alive, and not shutting down. Otherwise `503` with a JSON body
//!   naming every failing check.
//!
//! Deliberately hand-rolled over `tokio::net::TcpListener`: two GET routes
//! do not justify pulling an HTTP stack into `awa-worker` (the admin UI keeps
//! its own axum server in `awa-ui`). Requests larger than a probe would ever
//! send are rejected, and every response closes the connection.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sqlx::PgPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Probe requests are tiny; anything beyond this is not a health check.
const MAX_REQUEST_BYTES: usize = 4096;
/// Bound the whole request/response exchange so a stuck peer cannot pin the
/// listener's accept loop resources.
const CONNECTION_DEADLINE: Duration = Duration::from_secs(5);

/// Shared handles the readiness computation reads. Cloned from the runtime's
/// own liveness flags — the listener observes the same state the runtime
/// snapshot reporter publishes to `awa.runtime_instances`.
#[derive(Clone)]
pub(crate) struct HealthProbe {
    pub(crate) pool: PgPool,
    pub(crate) dispatcher_alive: Arc<HashMap<String, Arc<AtomicBool>>>,
    pub(crate) heartbeat_alive: Arc<AtomicBool>,
    pub(crate) maintenance_alive: Arc<AtomicBool>,
    pub(crate) leader: Arc<AtomicBool>,
    pub(crate) dispatch_cancel: CancellationToken,
}

/// The `/readyz` response body. Field names are part of the documented probe
/// surface (see `docs/deployment.md`).
#[derive(Debug, Serialize)]
pub(crate) struct ReadyzReport {
    pub(crate) ready: bool,
    pub(crate) postgres_connected: bool,
    pub(crate) schema_version: Option<i32>,
    pub(crate) expected_schema_version: i32,
    pub(crate) schema_compatible: bool,
    pub(crate) poll_loop_alive: bool,
    pub(crate) heartbeat_alive: bool,
    pub(crate) maintenance_alive: bool,
    pub(crate) shutting_down: bool,
    pub(crate) leader: bool,
}

impl HealthProbe {
    pub(crate) async fn readyz(&self) -> ReadyzReport {
        let postgres_connected = sqlx::query("SELECT 1").execute(&self.pool).await.is_ok();

        // A newer schema than the binary expects is the supported rolling-
        // deploy skew (older binary against newer schema); an older schema
        // means `awa migrate` has not run and the runtime cannot work.
        let schema_version = if postgres_connected {
            awa_model::migrations::current_version(&self.pool)
                .await
                .ok()
        } else {
            None
        };
        let expected_schema_version = awa_model::migrations::CURRENT_VERSION;
        let schema_compatible =
            schema_version.is_some_and(|version| version >= expected_schema_version);

        let poll_loop_alive = self
            .dispatcher_alive
            .values()
            .all(|alive| alive.load(Ordering::SeqCst));
        let heartbeat_alive = self.heartbeat_alive.load(Ordering::SeqCst);
        let maintenance_alive = self.maintenance_alive.load(Ordering::SeqCst);
        let shutting_down = self.dispatch_cancel.is_cancelled();

        ReadyzReport {
            ready: postgres_connected
                && schema_compatible
                && poll_loop_alive
                && heartbeat_alive
                && maintenance_alive
                && !shutting_down,
            postgres_connected,
            schema_version,
            expected_schema_version,
            schema_compatible,
            poll_loop_alive,
            heartbeat_alive,
            maintenance_alive,
            shutting_down,
            leader: self.leader.load(Ordering::SeqCst),
        }
    }
}

/// Accept loop. Runs until `cancel` fires; each connection is handled on its
/// own task with a hard deadline.
pub(crate) async fn serve(listener: TcpListener, probe: HealthProbe, cancel: CancellationToken) {
    let addr = listener.local_addr().ok();
    info!(?addr, "Health listener started (/healthz, /readyz)");
    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                info!(?addr, "Health listener stopped");
                return;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        let probe = probe.clone();
                        tokio::spawn(async move {
                            let outcome = tokio::time::timeout(
                                CONNECTION_DEADLINE,
                                handle_connection(stream, probe),
                            )
                            .await;
                            if let Ok(Err(err)) = outcome {
                                debug!(%peer, error = %err, "Health probe connection error");
                            }
                        });
                    }
                    Err(err) => {
                        warn!(error = %err, "Health listener accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream, probe: HealthProbe) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    let request_line = loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..read]);
        if let Some(end) = buf.iter().position(|&b| b == b'\n') {
            break String::from_utf8_lossy(&buf[..end]).trim_end().to_string();
        }
        if buf.len() > MAX_REQUEST_BYTES {
            return write_response(&mut stream, 431, "{\"error\":\"request too large\"}").await;
        }
    };

    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    let path = path.split('?').next().unwrap_or_default();

    if method != "GET" {
        return write_response(&mut stream, 405, "{\"error\":\"method not allowed\"}").await;
    }

    match path {
        "/healthz" => write_response(&mut stream, 200, "{\"status\":\"ok\"}").await,
        "/readyz" => {
            let report = probe.readyz().await;
            let status = if report.ready { 200 } else { 503 };
            let body =
                serde_json::to_string(&report).unwrap_or_else(|_| "{\"ready\":false}".to_string());
            write_response(&mut stream, status, &body).await
        }
        _ => write_response(&mut stream, 404, "{\"error\":\"not found\"}").await,
    }
}

async fn write_response(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        _ => "Unknown",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len(),
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}
