//! Cancel-notification listener.
//!
//! Listens on the `awa:cancel` Postgres channel for job cancellations
//! issued by admin operations (`awa_model::admin::cancel`,
//! `QueueStorage::cancel_job`, etc.) and fires the matching in-flight
//! `cancel` flag so the handler currently executing that attempt sees
//! `ctx.is_cancelled() == true` and can stop cleanly.
//!
//! The admin cancel path emits `pg_notify('awa:cancel', payload)` where
//! `payload` is `{"job_id": <i64>, "run_lease": <i64>}`. This listener
//! parses that, looks the pair up in the shared `InFlightRegistry`, and
//! — if a local worker is running the matching attempt — sets the
//! `Arc<AtomicBool>` cancel flag the handler's `JobContext` holds. A cancel
//! for an attempt that has not registered yet is kept for a bounded time, so
//! an attempt claimed before the cancel committed but dispatched after it
//! is not run at all.
//!
//! If the first `PgListener::connect_with` or `listen()` fails, the listener
//! logs a warning and keeps retrying in the background with backoff. Until it
//! connects, admin cancels take effect only through completion-time checks
//! and heartbeat / deadline rescue, without the early wake-up.

use crate::runtime::InFlightMap;
use serde::Deserialize;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

const CANCEL_CHANNEL: &str = "awa:cancel";

/// First wait before retrying a listener that could not start.
const CONNECT_RETRY_INITIAL: Duration = Duration::from_millis(250);
/// Longest wait between retries of a listener that could not start.
const CONNECT_RETRY_MAX: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
struct CancelPayload {
    job_id: i64,
    run_lease: i64,
}

pub(crate) struct CancelListener {
    pool: PgPool,
    in_flight: InFlightMap,
    cancel: CancellationToken,
}

impl CancelListener {
    pub fn new(pool: PgPool, in_flight: InFlightMap, cancel: CancellationToken) -> Self {
        Self {
            pool,
            in_flight,
            cancel,
        }
    }

    /// Starts the listener. The first connection is attempted before this
    /// returns, so a caller that spawns it before claiming does not miss
    /// cancels; if it fails, the returned task keeps retrying.
    pub async fn spawn(self) -> Option<JoinHandle<()>> {
        match self.connect().await {
            Ok(listener) => {
                debug!(channel = CANCEL_CHANNEL, "Cancel listener started");
                Some(tokio::spawn(async move {
                    self.run(listener).await;
                }))
            }
            Err(err) => {
                warn!(
                    error = %err,
                    channel = CANCEL_CHANNEL,
                    "Failed to start the admin cancel listener; retrying in the background. \
                     Until it connects, admin cancels take effect only via heartbeat/deadline \
                     rescue"
                );
                Some(tokio::spawn(async move {
                    self.retry_then_run().await;
                }))
            }
        }
    }

    async fn connect(&self) -> Result<PgListener, sqlx::Error> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(CANCEL_CHANNEL).await?;
        Ok(listener)
    }

    async fn retry_then_run(self) {
        let mut delay = CONNECT_RETRY_INITIAL;
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
            // A stalled connection or LISTEN must not hold up shutdown.
            let connected = tokio::select! {
                _ = self.cancel.cancelled() => return,
                connected = self.connect() => connected,
            };
            match connected {
                Ok(listener) => {
                    info!(
                        channel = CANCEL_CHANNEL,
                        "Admin cancel listener started after retrying"
                    );
                    return self.run(listener).await;
                }
                Err(err) => {
                    debug!(error = %err, "Admin cancel listener still cannot connect; retrying");
                    delay = (delay * 2).min(CONNECT_RETRY_MAX);
                }
            }
        }
    }

    async fn run(self, mut listener: PgListener) {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    debug!("Cancel listener shutting down");
                    return;
                }
                notification = listener.try_recv() => {
                    match notification {
                        Ok(Some(n)) => self.handle_notification(n.payload()),
                        // The next `try_recv` reconnects and listens again.
                        // Notifications are not queued for a listener that is
                        // gone, so a cancel committed in the gap reaches its
                        // handler only through the completion-time check.
                        Ok(None) => warn!(
                            "PG cancel listener connection lost; reconnecting. Admin cancels \
                             committed before it is back will not wake their handlers"
                        ),
                        Err(err) => {
                            warn!(error = %err, "PG cancel listener error; will retry");
                            tokio::select! {
                                _ = self.cancel.cancelled() => return,
                                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                            }
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn handle_notification(&self, payload: &str) {
        let parsed: CancelPayload = match serde_json::from_str(payload) {
            Ok(parsed) => parsed,
            Err(err) => {
                warn!(
                    error = %err,
                    payload = %payload,
                    "Malformed awa:cancel payload; ignoring"
                );
                return;
            }
        };

        // The cancel names an attempt whose claim has committed. A local
        // attempt may not have registered yet, so the registry keeps the
        // cancel for it and the attempt starts already cancelled.
        if self
            .in_flight
            .signal_cancel((parsed.job_id, parsed.run_lease))
        {
            debug!(
                job_id = parsed.job_id,
                run_lease = parsed.run_lease,
                "Signalled cancellation for locally-running attempt"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{InFlightRegistry, InFlightState, ProgressState};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    /// Forwards every connection accepted on `listener` to `upstream`.
    fn forward(listener: tokio::net::TcpListener, upstream: String) {
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(upstream).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        });
    }

    #[sqlx::test(migrations = false)]
    async fn a_listener_whose_first_connection_fails_keeps_retrying(pool: PgPool) {
        let options = pool.connect_options();
        let upstream = format!("{}:{}", options.get_host(), options.get_port());
        // Reserve a port, then free it so the first connection is refused.
        let port = {
            let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            probe.local_addr().unwrap().port()
        };
        let unreachable = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(2))
            .connect_lazy_with((*options).clone().host("127.0.0.1").port(port));

        let in_flight = Arc::new(InFlightRegistry::default());
        let flag = Arc::new(AtomicBool::new(false));
        in_flight.insert(
            (1, 1),
            InFlightState {
                cancel: flag.clone(),
                progress: Arc::new(std::sync::Mutex::new(ProgressState::new(None))),
                claim: None,
                finalizing: false,
            },
        );
        let shutdown = CancellationToken::new();
        let handle = CancelListener::new(unreachable, in_flight, shutdown.clone())
            .spawn()
            .await
            .expect("a failed first connection still leaves a retrying listener");
        assert!(!handle.is_finished());

        // The database becomes reachable on that port.
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        forward(listener, upstream);

        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                sqlx::query("SELECT pg_notify($1, $2)")
                    .bind(CANCEL_CHANNEL)
                    .bind(r#"{"job_id": 1, "run_lease": 1}"#)
                    .execute(&pool)
                    .await
                    .unwrap();
                if flag.load(Ordering::SeqCst) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the retried listener must deliver cancels");

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("the listener stops on shutdown")
            .unwrap();
    }

    /// Shutdown stops a listener whose retried connection has stalled.
    #[tokio::test]
    async fn shutdown_interrupts_a_stalled_reconnect() {
        let port = {
            let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            probe.local_addr().unwrap().port()
        };
        let unreachable = sqlx::postgres::PgPoolOptions::new()
            // Longer than the shutdown bound below, so a reconnect that is not
            // raced against shutdown fails the test.
            .acquire_timeout(Duration::from_secs(5))
            .connect_lazy(&format!(
                "postgres://postgres:test@127.0.0.1:{port}/awa_test"
            ))
            .unwrap();
        let shutdown = CancellationToken::new();
        let handle = CancelListener::new(
            unreachable,
            Arc::new(InFlightRegistry::default()),
            shutdown.clone(),
        )
        .spawn()
        .await
        .expect("a failed first connection still leaves a retrying listener");

        // The port now accepts connections and never answers them.
        let stall = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        let (held, _) = tokio::time::timeout(Duration::from_secs(10), stall.accept())
            .await
            .expect("the listener retries its connection")
            .unwrap();

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("shutdown must not wait for a stalled reconnect")
            .unwrap();
        drop(held);
    }
}
