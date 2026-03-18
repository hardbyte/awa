use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Background heartbeat service that periodically updates heartbeat_at
/// for all in-flight jobs.
pub struct HeartbeatService {
    pool: PgPool,
    in_flight: Arc<RwLock<HashMap<i64, Arc<AtomicBool>>>>,
    interval: std::time::Duration,
    batch_size: usize,
    alive: Arc<AtomicBool>,
    cancel: CancellationToken,
}

impl HeartbeatService {
    pub fn new(
        pool: PgPool,
        in_flight: Arc<RwLock<HashMap<i64, Arc<AtomicBool>>>>,
        interval: std::time::Duration,
        alive: Arc<AtomicBool>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            pool,
            in_flight,
            interval,
            batch_size: 500,
            alive,
            cancel,
        }
    }

    /// Run the heartbeat loop. Returns when cancelled.
    #[tracing::instrument(skip(self), fields(interval_ms = self.interval.as_millis() as u64))]
    pub async fn run(&self) {
        self.alive.store(true, Ordering::SeqCst);
        debug!(
            interval_ms = self.interval.as_millis(),
            "Heartbeat service started"
        );

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    debug!("Heartbeat service shutting down");
                    // Final heartbeat before exit
                    self.heartbeat_once().await;
                    break;
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.heartbeat_once().await;
                }
            }
        }

        self.alive.store(false, Ordering::SeqCst);
    }

    #[tracing::instrument(skip(self))]
    async fn heartbeat_once(&self) {
        let job_ids: Vec<i64> = {
            let guard = self.in_flight.read().await;
            guard.keys().copied().collect()
        };

        if job_ids.is_empty() {
            return;
        }

        // Batch heartbeats in chunks of batch_size
        for chunk in job_ids.chunks(self.batch_size) {
            let chunk_vec: Vec<i64> = chunk.to_vec();
            match sqlx::query(
                "UPDATE awa.jobs SET heartbeat_at = now() WHERE id = ANY($1) AND state = 'running'",
            )
            .bind(&chunk_vec)
            .execute(&self.pool)
            .await
            {
                Ok(result) => {
                    debug!(
                        count = chunk_vec.len(),
                        updated = result.rows_affected(),
                        "Heartbeat batch sent"
                    );
                }
                Err(err) => {
                    warn!(error = %err, "Failed to send heartbeat batch");
                }
            }
        }
    }
}
