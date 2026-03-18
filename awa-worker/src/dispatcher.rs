use crate::executor::JobExecutor;
use awa_model::JobRow;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Configuration for a single queue.
#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub max_workers: u32,
    pub poll_interval: Duration,
    pub deadline_duration: Duration,
    pub priority_aging_interval: Duration,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_workers: 50,
            poll_interval: Duration::from_millis(200),
            deadline_duration: Duration::from_secs(300), // 5 minutes
            priority_aging_interval: Duration::from_secs(60),
        }
    }
}

/// Dispatcher polls a single queue for available jobs and dispatches them.
pub struct Dispatcher {
    queue: String,
    config: QueueConfig,
    pool: PgPool,
    executor: Arc<JobExecutor>,
    _in_flight: Arc<RwLock<HashMap<i64, Arc<AtomicBool>>>>,
    semaphore: Arc<Semaphore>,
    alive: Arc<AtomicBool>,
    cancel: CancellationToken,
}

impl Dispatcher {
    pub fn new(
        queue: String,
        config: QueueConfig,
        pool: PgPool,
        executor: Arc<JobExecutor>,
        in_flight: Arc<RwLock<HashMap<i64, Arc<AtomicBool>>>>,
        alive: Arc<AtomicBool>,
        cancel: CancellationToken,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_workers as usize));
        Self {
            queue,
            config,
            pool,
            executor,
            _in_flight: in_flight,
            semaphore,
            alive,
            cancel,
        }
    }

    /// Run the poll loop. Returns when cancelled.
    #[tracing::instrument(skip(self), fields(queue = %self.queue, max_workers = self.config.max_workers))]
    pub async fn run(&self) {
        self.alive.store(true, Ordering::SeqCst);
        info!(
            queue = %self.queue,
            max_workers = self.config.max_workers,
            poll_interval_ms = self.config.poll_interval.as_millis(),
            "Dispatcher started"
        );

        // Set up LISTEN/NOTIFY for this queue
        let notify_channel = format!("awa:{}", self.queue);
        let mut listener = match sqlx::postgres::PgListener::connect_with(&self.pool).await {
            Ok(listener) => listener,
            Err(err) => {
                error!(error = %err, "Failed to create PG listener, falling back to polling only");
                // Fall back to poll-only mode
                self.poll_loop_only().await;
                self.alive.store(false, Ordering::SeqCst);
                return;
            }
        };

        if let Err(err) = listener.listen(&notify_channel).await {
            warn!(error = %err, channel = %notify_channel, "Failed to LISTEN, falling back to polling");
            self.poll_loop_only().await;
            self.alive.store(false, Ordering::SeqCst);
            return;
        }

        debug!(channel = %notify_channel, "Listening for job notifications");

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    debug!(queue = %self.queue, "Dispatcher shutting down");
                    break;
                }
                // Wait for either a notification or the poll interval
                notification = listener.recv() => {
                    match notification {
                        Ok(_) => {
                            debug!(queue = %self.queue, "Woken by NOTIFY");
                            self.poll_once().await;
                        }
                        Err(err) => {
                            warn!(error = %err, "PG listener error, will retry");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    self.poll_once().await;
                }
            }
        }

        self.alive.store(false, Ordering::SeqCst);
    }

    /// Poll-only fallback (no LISTEN/NOTIFY).
    async fn poll_loop_only(&self) {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    debug!(queue = %self.queue, "Dispatcher (poll-only) shutting down");
                    break;
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    self.poll_once().await;
                }
            }
        }
    }

    /// Single poll iteration: claim available jobs up to the semaphore limit.
    #[tracing::instrument(skip(self), fields(queue = %self.queue))]
    async fn poll_once(&self) {
        // How many workers are available?
        let available = self.semaphore.available_permits();
        if available == 0 {
            return;
        }

        let batch_size = available.min(10) as i32; // Claim in small batches
        let deadline_secs = self.config.deadline_duration.as_secs_f64();
        let aging_secs = self.config.priority_aging_interval.as_secs_f64();

        let jobs: Vec<JobRow> = match sqlx::query_as::<_, JobRow>(
            r#"
            WITH claimed AS (
                SELECT id
                FROM awa.jobs
                WHERE state = 'available'
                  AND queue = $1
                  AND run_at <= now()
                  AND NOT EXISTS (
                      SELECT 1 FROM awa.queue_meta
                      WHERE queue = $1 AND paused = TRUE
                  )
                ORDER BY
                  GREATEST(1, priority - FLOOR(EXTRACT(EPOCH FROM (now() - run_at)) / $4)::int) ASC,
                  run_at ASC,
                  id ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            UPDATE awa.jobs
            SET state = 'running',
                attempt = attempt + 1,
                attempted_at = now(),
                heartbeat_at = now(),
                deadline_at = now() + make_interval(secs => $3)
            FROM claimed
            WHERE awa.jobs.id = claimed.id
            RETURNING awa.jobs.*
            "#,
        )
        .bind(&self.queue)
        .bind(batch_size)
        .bind(deadline_secs)
        .bind(aging_secs)
        .fetch_all(&self.pool)
        .await
        {
            Ok(jobs) => jobs,
            Err(err) => {
                warn!(queue = %self.queue, error = %err, "Failed to claim jobs");
                return;
            }
        };

        if !jobs.is_empty() {
            debug!(queue = %self.queue, count = jobs.len(), "Claimed jobs");
        }

        for job in jobs {
            // Acquire a semaphore permit before dispatching
            let permit = match self.semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    warn!("Semaphore closed");
                    break;
                }
            };

            let cancel_flag = Arc::new(AtomicBool::new(false));
            let handle = self.executor.execute(job, cancel_flag);

            // Spawn a task to release the permit when the job completes
            tokio::spawn(async move {
                let _ = handle.await;
                drop(permit);
            });
        }
    }

    /// Get the number of in-flight jobs for this queue.
    pub async fn in_flight_count(&self) -> usize {
        self.config.max_workers as usize - self.semaphore.available_permits()
    }
}
