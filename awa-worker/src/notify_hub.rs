//! One `LISTEN` connection per runtime that fans queue wake-ups out to the
//! dispatchers of every registered queue.
//!
//! Every dispatcher used to hold its own listener connection, so a runtime
//! with N queues pinned N pool connections and N backends in `LISTEN`.
//! The hub listens on every `awa:{queue}` channel once and signals the
//! queue's shared [`Notify`]; dispatchers wait on that instead of a
//! connection of their own.
//!
//! When the listener cannot be created the hub reports `None` and the
//! dispatchers run poll-only, exactly as they did when their own `LISTEN`
//! failed (a transaction-mode pooler makes this the expected path).

use sqlx::postgres::PgListener;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

pub(crate) fn queue_channel(queue: &str) -> String {
    format!("awa:{queue}")
}

pub(crate) struct QueueNotifyHub {
    pool: PgPool,
    wakes: HashMap<String, Arc<Notify>>,
    cancel: CancellationToken,
}

impl QueueNotifyHub {
    /// `wakes` maps each queue name to the `Notify` its dispatchers wait on.
    pub fn new(
        pool: PgPool,
        wakes: HashMap<String, Arc<Notify>>,
        cancel: CancellationToken,
    ) -> Self {
        let wakes = wakes
            .into_iter()
            .map(|(queue, wake)| (queue_channel(&queue), wake))
            .collect();
        Self {
            pool,
            wakes,
            cancel,
        }
    }

    /// Connect and `LISTEN` on every queue channel. Returns `None` when
    /// notifications are unavailable, in which case callers must poll.
    pub async fn spawn(self) -> Option<JoinHandle<()>> {
        if self.wakes.is_empty() {
            return None;
        }
        let mut listener = match PgListener::connect_with(&self.pool).await {
            Ok(listener) => listener,
            Err(err) => {
                warn!(
                    error = %err,
                    "Failed to create PG listener for queue wake-ups, falling back to polling only"
                );
                return None;
            }
        };
        let channels: Vec<&str> = self.wakes.keys().map(String::as_str).collect();
        if let Err(err) = listener.listen_all(channels).await {
            warn!(
                error = %err,
                "Failed to LISTEN on queue channels, falling back to polling only"
            );
            return None;
        }
        debug!(
            channels = self.wakes.len(),
            "Listening for job notifications"
        );
        Some(tokio::spawn(async move {
            self.run(listener).await;
        }))
    }

    async fn run(self, mut listener: PgListener) {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    debug!("Queue notify hub shutting down");
                    return;
                }
                notification = listener.try_recv() => {
                    match notification {
                        Ok(Some(notification)) => {
                            if let Some(wake) = self.wakes.get(notification.channel()) {
                                wake.notify_one();
                            }
                        }
                        // The listener reconnected; notifications sent
                        // during the outage are gone, so wake every queue.
                        Ok(None) => self.wake_all(),
                        Err(err) => {
                            warn!(error = %err, "PG listener error, will retry");
                            self.wake_all();
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }

    fn wake_all(&self) {
        for wake in self.wakes.values() {
            wake.notify_one();
        }
    }
}
