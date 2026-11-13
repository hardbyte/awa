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
//! If `PgListener::connect_with` or `listen()` fail, the listener logs a
//! warning and exits; admin cancels silently fall back to heartbeat /
//! deadline rescue for detection. That matches the dispatcher fallback:
//! admin cancel eventually takes effect on completion-time checks, but
//! the handler doesn't get the early wake-up.

use crate::runtime::InFlightMap;
use serde::Deserialize;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const CANCEL_CHANNEL: &str = "awa:cancel";

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

    pub async fn spawn(self) -> Option<JoinHandle<()>> {
        let mut listener = match PgListener::connect_with(&self.pool).await {
            Ok(listener) => listener,
            Err(err) => {
                warn!(
                    error = %err,
                    "Failed to create PG listener for admin cancel; admin cancels will \
                     only take effect via heartbeat/deadline rescue"
                );
                return None;
            }
        };

        if let Err(err) = listener.listen(CANCEL_CHANNEL).await {
            warn!(
                error = %err,
                channel = CANCEL_CHANNEL,
                "Failed to LISTEN on cancel channel; admin cancels will only take \
                 effect via heartbeat/deadline rescue"
            );
            return None;
        }

        debug!(channel = CANCEL_CHANNEL, "Cancel listener started");
        Some(tokio::spawn(async move {
            self.run(listener).await;
        }))
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
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
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
        // attempt may not have registered yet: the dispatcher still has
        // round-trips to make after the claim. The registry keeps the cancel
        // for it, so the attempt starts already cancelled.
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
