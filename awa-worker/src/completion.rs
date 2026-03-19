use crate::runtime::RunLease;
use awa_model::AwaError;
use sqlx::PgPool;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const COMPLETION_BATCH_SIZE: usize = 512;
const COMPLETION_FLUSH_INTERVAL: Duration = Duration::from_millis(1);
const COMPLETION_CHANNEL_CAPACITY: usize = 4096;
const COMPLETION_SHARDS: usize = 8;

struct CompletionRequest {
    job_id: i64,
    run_lease: RunLease,
    response: oneshot::Sender<Result<bool, AwaError>>,
}

#[derive(Clone)]
pub(crate) struct CompletionBatcherHandle {
    shards: Vec<mpsc::Sender<CompletionRequest>>,
}

impl CompletionBatcherHandle {
    pub async fn complete(&self, job_id: i64, run_lease: RunLease) -> Result<bool, AwaError> {
        let shard = (job_id.rem_euclid(self.shards.len() as i64)) as usize;
        let (response_tx, response_rx) = oneshot::channel();
        self.shards[shard]
            .send(CompletionRequest {
                job_id,
                run_lease,
                response: response_tx,
            })
            .await
            .map_err(|_| AwaError::Validation("completion batcher stopped".into()))?;

        response_rx
            .await
            .map_err(|_| AwaError::Validation("completion batcher dropped response".into()))?
    }
}

pub(crate) struct CompletionBatcher {
    workers: Vec<CompletionWorker>,
}

impl CompletionBatcher {
    pub fn new(
        pool: PgPool,
        cancel: CancellationToken,
        metrics: crate::metrics::AwaMetrics,
    ) -> (Self, CompletionBatcherHandle) {
        let mut shards = Vec::with_capacity(COMPLETION_SHARDS);
        let mut workers = Vec::with_capacity(COMPLETION_SHARDS);

        for shard_id in 0..COMPLETION_SHARDS {
            let (tx, rx) = mpsc::channel(COMPLETION_CHANNEL_CAPACITY);
            shards.push(tx);
            workers.push(CompletionWorker {
                shard_id,
                pool: pool.clone(),
                rx,
                cancel: cancel.clone(),
                metrics: metrics.clone(),
            });
        }

        (Self { workers }, CompletionBatcherHandle { shards })
    }

    pub fn spawn(self) -> Vec<tokio::task::JoinHandle<()>> {
        self.workers
            .into_iter()
            .map(|worker| tokio::spawn(async move { worker.run().await }))
            .collect()
    }
}

struct CompletionWorker {
    shard_id: usize,
    pool: PgPool,
    rx: mpsc::Receiver<CompletionRequest>,
    cancel: CancellationToken,
    metrics: crate::metrics::AwaMetrics,
}

impl CompletionWorker {
    async fn run(mut self) {
        let mut pending = Vec::with_capacity(COMPLETION_BATCH_SIZE);

        loop {
            if pending.is_empty() {
                tokio::select! {
                    _ = self.cancel.cancelled() => break,
                    request = self.rx.recv() => {
                        match request {
                            Some(request) => pending.push(request),
                            None => break,
                        }
                    }
                }
            } else {
                let timer = tokio::time::sleep(COMPLETION_FLUSH_INTERVAL);
                tokio::pin!(timer);

                tokio::select! {
                    _ = self.cancel.cancelled() => break,
                    _ = &mut timer => {
                        self.flush(&mut pending).await;
                    }
                    request = self.rx.recv() => {
                        match request {
                            Some(request) => {
                                pending.push(request);
                                if pending.len() >= COMPLETION_BATCH_SIZE {
                                    self.flush(&mut pending).await;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        }

        while let Ok(request) = self.rx.try_recv() {
            pending.push(request);
            if pending.len() >= COMPLETION_BATCH_SIZE {
                self.flush(&mut pending).await;
            }
        }
        if !pending.is_empty() {
            self.flush(&mut pending).await;
        }
        debug!(shard = self.shard_id, "Completion batcher shard stopped");
    }

    async fn flush(&self, pending: &mut Vec<CompletionRequest>) {
        if pending.is_empty() {
            return;
        }

        let batch: Vec<_> = std::mem::take(pending);
        let job_ids: Vec<i64> = batch.iter().map(|request| request.job_id).collect();
        let run_leases: Vec<i64> = batch.iter().map(|request| request.run_lease).collect();
        let flush_start = std::time::Instant::now();

        let updated = sqlx::query_scalar::<_, i64>(
            r#"
            WITH completed (id, run_lease) AS (
                SELECT * FROM unnest($1::bigint[], $2::bigint[])
            )
            UPDATE awa.jobs_hot AS jobs
            SET state = 'completed',
                finalized_at = now(),
                progress = NULL
            FROM completed
            WHERE jobs.id = completed.id
              AND jobs.run_lease = completed.run_lease
              AND jobs.state = 'running'
            RETURNING jobs.id
            "#,
        )
        .bind(&job_ids)
        .bind(&run_leases)
        .fetch_all(&self.pool)
        .await;

        match updated {
            Ok(updated_ids) => {
                let updated: HashSet<i64> = updated_ids.into_iter().collect();
                let updated_count = updated.len();
                self.metrics.record_completion_flush(
                    self.shard_id,
                    job_ids.len() as u64,
                    flush_start.elapsed(),
                );
                for request in batch {
                    let _ = request.response.send(Ok(updated.contains(&request.job_id)));
                }
                debug!(
                    shard = self.shard_id,
                    batch_size = job_ids.len(),
                    updated = updated_count,
                    "Flushed completed job batch"
                );
            }
            Err(err) => {
                self.metrics.record_completion_flush(
                    self.shard_id,
                    job_ids.len() as u64,
                    flush_start.elapsed(),
                );
                warn!(
                    error = %err,
                    shard = self.shard_id,
                    batch_size = job_ids.len(),
                    "Failed to flush completed job batch"
                );
                let message = format!("completion batch flush failed: {err}");
                for request in batch {
                    let _ = request
                        .response
                        .send(Err(AwaError::Validation(message.clone())));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use awa_model::migrations;
    use sqlx::postgres::PgPoolOptions;
    use std::time::Instant;

    fn database_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
    }

    async fn setup(max_conns: u32) -> PgPool {
        let pool = PgPoolOptions::new()
            .max_connections(max_conns)
            .connect(&database_url())
            .await
            .expect("Failed to connect to database");
        migrations::run(&pool).await.expect("Failed to migrate");
        pool
    }

    async fn clean_queue(pool: &PgPool, queue: &str) {
        sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
            .bind(queue)
            .execute(pool)
            .await
            .expect("Failed to clean queue jobs");
        sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
            .bind(queue)
            .execute(pool)
            .await
            .expect("Failed to clean queue meta");
    }

    async fn seed_running_jobs(pool: &PgPool, queue: &str, total_jobs: i64) -> Vec<(i64, i64)> {
        sqlx::query(
            r#"
            INSERT INTO awa.jobs_hot (
                kind, queue, args, state, priority, attempt, run_lease,
                max_attempts, run_at, attempted_at, heartbeat_at, deadline_at,
                metadata, tags
            )
            SELECT
                'bench_job',
                $1,
                jsonb_build_object('seq', g),
                'running'::awa.job_state,
                2,
                1,
                1,
                25,
                now(),
                now(),
                now(),
                now() + interval '5 minutes',
                '{}'::jsonb,
                '{}'::text[]
            FROM generate_series(1, $2) AS g
            "#,
        )
        .bind(queue)
        .bind(total_jobs)
        .execute(pool)
        .await
        .expect("Failed to seed running jobs");

        sqlx::query_as::<_, (i64, i64)>(
            "SELECT id, run_lease FROM awa.jobs_hot WHERE queue = $1 ORDER BY id ASC",
        )
        .bind(queue)
        .fetch_all(pool)
        .await
        .expect("Failed to load seeded rows")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn benchmark_completion_batcher_ack_throughput() {
        let pool = setup(20).await;
        let queue = "bench_completion_batcher";
        clean_queue(&pool, queue).await;

        let jobs = seed_running_jobs(&pool, queue, 10_000).await;
        let (batcher, handle) = CompletionBatcher::new(
            pool.clone(),
            CancellationToken::new(),
            crate::metrics::AwaMetrics::from_global(),
        );
        let workers = batcher.spawn();

        let start = Instant::now();
        let mut set = tokio::task::JoinSet::new();
        for (job_id, run_lease) in jobs {
            let handle = handle.clone();
            set.spawn(async move { handle.complete(job_id, run_lease).await });
        }

        let mut completed = 0usize;
        while let Some(result) = set.join_next().await {
            let updated = result
                .expect("completion task panicked")
                .expect("completion failed");
            assert!(updated, "Expected completion batcher to update row");
            completed += 1;
        }
        let elapsed = start.elapsed();

        let completed_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM awa.jobs WHERE queue = $1 AND state = 'completed'",
        )
        .bind(queue)
        .fetch_one(&pool)
        .await
        .unwrap();

        println!(
            "[completion] batcher_ack completed={} in {:.3}s ({:.0}/s)",
            completed,
            elapsed.as_secs_f64(),
            completed as f64 / elapsed.as_secs_f64()
        );
        assert_eq!(completed_rows, completed as i64);

        for worker in workers {
            worker.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn benchmark_completion_direct_batched_query_throughput() {
        let pool = setup(20).await;
        let queue = "bench_completion_direct";
        clean_queue(&pool, queue).await;

        let jobs = seed_running_jobs(&pool, queue, 10_000).await;
        let start = Instant::now();
        for chunk in jobs.chunks(COMPLETION_BATCH_SIZE) {
            let job_ids: Vec<i64> = chunk.iter().map(|(job_id, _)| *job_id).collect();
            let run_leases: Vec<i64> = chunk.iter().map(|(_, run_lease)| *run_lease).collect();
            let updated: Vec<i64> = sqlx::query_scalar(
                r#"
                WITH completed (id, run_lease) AS (
                    SELECT * FROM unnest($1::bigint[], $2::bigint[])
                )
                UPDATE awa.jobs_hot AS jobs
                SET state = 'completed',
                    finalized_at = now()
                FROM completed
                WHERE jobs.id = completed.id
                  AND jobs.run_lease = completed.run_lease
                  AND jobs.state = 'running'
                RETURNING jobs.id
                "#,
            )
            .bind(&job_ids)
            .bind(&run_leases)
            .fetch_all(&pool)
            .await
            .expect("Direct completion batch failed");
            assert_eq!(updated.len(), chunk.len());
        }
        let elapsed = start.elapsed();

        println!(
            "[completion] direct_batched_sql completed={} in {:.3}s ({:.0}/s)",
            jobs.len(),
            elapsed.as_secs_f64(),
            jobs.len() as f64 / elapsed.as_secs_f64()
        );
    }
}
