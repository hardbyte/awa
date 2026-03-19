use crate::runtime::{InFlightMap, RunLease};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Background heartbeat service that periodically updates heartbeat_at
/// for all in-flight jobs.
pub struct HeartbeatService {
    pool: PgPool,
    in_flight: InFlightMap,
    interval: std::time::Duration,
    batch_size: usize,
    alive: Arc<AtomicBool>,
    cancel: CancellationToken,
}

impl HeartbeatService {
    pub(crate) fn new(
        pool: PgPool,
        in_flight: InFlightMap,
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
        let jobs: Vec<(i64, RunLease)> = self.in_flight.keys();

        if jobs.is_empty() {
            return;
        }

        // Batch heartbeats in chunks of batch_size
        for chunk in jobs.chunks(self.batch_size) {
            let job_ids: Vec<i64> = chunk.iter().map(|(job_id, _)| *job_id).collect();
            let run_leases: Vec<i64> = chunk.iter().map(|(_, run_lease)| *run_lease).collect();
            match sqlx::query(
                r#"
                UPDATE awa.jobs_hot AS jobs
                SET heartbeat_at = now()
                FROM unnest($1::bigint[], $2::bigint[]) AS inflight(id, run_lease)
                WHERE jobs.id = inflight.id
                  AND jobs.run_lease = inflight.run_lease
                  AND jobs.state = 'running'
                "#,
            )
            .bind(&job_ids)
            .bind(&run_leases)
            .execute(&self.pool)
            .await
            {
                Ok(result) => {
                    debug!(
                        count = job_ids.len(),
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
