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
//! `Arc<AtomicBool>` cancel flag the handler's `JobContext` holds.
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
use std::sync::atomic::Ordering;
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

    pub async fn run(self) {
        let mut listener = match PgListener::connect_with(&self.pool).await {
            Ok(listener) => listener,
            Err(err) => {
                warn!(
                    error = %err,
                    "Failed to create PG listener for admin cancel; admin cancels will \
                     only take effect via heartbeat/deadline rescue"
                );
                return;
            }
        };

        if let Err(err) = listener.listen(CANCEL_CHANNEL).await {
            warn!(
                error = %err,
                channel = CANCEL_CHANNEL,
                "Failed to LISTEN on cancel channel; admin cancels will only take \
                 effect via heartbeat/deadline rescue"
            );
            return;
        }

        debug!(channel = CANCEL_CHANNEL, "Cancel listener started");

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    debug!("Cancel listener shutting down");
                    return;
                }
                notification = listener.recv() => {
                    match notification {
                        Ok(n) => self.handle_notification(n.payload()),
                        Err(err) => {
                            warn!(error = %err, "PG cancel listener error; will retry");
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }

    fn handle_notification(&self, payload: &str) {
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

        if let Some(flag) = self.in_flight.get_cancel((parsed.job_id, parsed.run_lease)) {
            flag.store(true, Ordering::SeqCst);
            debug!(
                job_id = parsed.job_id,
                run_lease = parsed.run_lease,
                "Signalled cancellation for locally-running attempt"
            );
        }
    }
}
