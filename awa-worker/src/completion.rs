use crate::runtime::RunLease;
use crate::storage::RuntimeStorage;
use awa_model::{AwaError, ClaimedEntry, ClaimedRuntimeJob};
use sqlx::PgPool;
use std::collections::HashSet;
use std::env;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const COMPLETION_BATCH_SIZE: usize = 512;
const COMPLETION_CHANNEL_CAPACITY: usize = 4096;

fn completion_flush_interval() -> Duration {
    env::var("AWA_COMPLETION_FLUSH_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(1))
}

fn completion_shards(storage: &RuntimeStorage) -> usize {
    env::var("AWA_COMPLETION_SHARDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(match storage {
            RuntimeStorage::Canonical => 8,
            RuntimeStorage::QueueStorage(_) => 4,
        })
}
const COMPLETE_BATCH_SQL: &str = r#"
    WITH completed (id, run_lease) AS (
        SELECT * FROM unnest($1::bigint[], $2::bigint[])
    ),
    locked AS (
        SELECT jobs.ctid, jobs.id, jobs.run_lease
        FROM awa.jobs_hot AS jobs
        JOIN completed
          ON jobs.id = completed.id
         AND jobs.run_lease = completed.run_lease
        WHERE jobs.state = 'running'
        ORDER BY jobs.id
        FOR UPDATE OF jobs
    )
    UPDATE awa.jobs_hot AS jobs
    SET state = 'completed',
        finalized_at = now(),
        progress = NULL
    FROM locked
    WHERE jobs.ctid = locked.ctid
    RETURNING locked.id, locked.run_lease
"#;

struct CompletionRequest {
    job_id: i64,
    run_lease: RunLease,
    claim: Option<ClaimedEntry>,
    runtime_job: Option<ClaimedRuntimeJob>,
    response: oneshot::Sender<Result<bool, AwaError>>,
}

#[derive(Clone)]
pub(crate) struct CompletionBatcherHandle {
    shards: Vec<mpsc::Sender<CompletionRequest>>,
}

impl CompletionBatcherHandle {
    pub async fn complete(&self, job_id: i64, run_lease: RunLease) -> Result<bool, AwaError> {
        self.complete_inner(job_id, run_lease, None, None).await
    }

    pub async fn complete_runtime_job(
        &self,
        runtime_job: ClaimedRuntimeJob,
    ) -> Result<bool, AwaError> {
        self.complete_inner(
            runtime_job.job.id,
            runtime_job.job.run_lease,
            Some(runtime_job.claim.clone()),
            Some(runtime_job),
        )
        .await
    }

    async fn complete_inner(
        &self,
        job_id: i64,
        run_lease: RunLease,
        claim: Option<ClaimedEntry>,
        runtime_job: Option<ClaimedRuntimeJob>,
    ) -> Result<bool, AwaError> {
        let shard = (job_id.rem_euclid(self.shards.len() as i64)) as usize;
        let (response_tx, response_rx) = oneshot::channel();
        self.shards[shard]
            .send(CompletionRequest {
                job_id,
                run_lease,
                claim,
                runtime_job,
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
        storage: RuntimeStorage,
    ) -> (Self, CompletionBatcherHandle) {
        let shard_count = completion_shards(&storage);
        let flush_interval = completion_flush_interval();
        let mut shards = Vec::with_capacity(shard_count);
        let mut workers = Vec::with_capacity(shard_count);

        for shard_id in 0..shard_count {
            let (tx, rx) = mpsc::channel(COMPLETION_CHANNEL_CAPACITY);
            shards.push(tx);
            workers.push(CompletionWorker {
                shard_id,
                pool: pool.clone(),
                rx,
                cancel: cancel.clone(),
                metrics: metrics.clone(),
                storage: storage.clone(),
                flush_interval,
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
    storage: RuntimeStorage,
    flush_interval: Duration,
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
                let timer = tokio::time::sleep(self.flush_interval);
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

        let mut batch: Vec<_> = std::mem::take(pending);
        batch.sort_unstable_by_key(|request| (request.job_id, request.run_lease));
        let job_ids: Vec<i64> = batch.iter().map(|request| request.job_id).collect();
        let run_leases: Vec<i64> = batch.iter().map(|request| request.run_lease).collect();
        let flush_start = std::time::Instant::now();

        let updated = match &self.storage {
            RuntimeStorage::Canonical => sqlx::query_as::<_, (i64, i64)>(COMPLETE_BATCH_SQL)
                .bind(&job_ids)
                .bind(&run_leases)
                .fetch_all(&self.pool)
                .await
                .map_err(AwaError::Database),
            RuntimeStorage::QueueStorage(runtime) => {
                if let Some(runtime_jobs) = batch
                    .iter()
                    .map(|request| request.runtime_job.clone())
                    .collect::<Option<Vec<ClaimedRuntimeJob>>>()
                {
                    runtime
                        .store
                        .complete_runtime_batch(&self.pool, &runtime_jobs)
                        .await
                } else if let Some(claimed) = batch
                    .iter()
                    .map(|request| request.claim.clone())
                    .collect::<Option<Vec<ClaimedEntry>>>()
                {
                    runtime
                        .store
                        .complete_claimed_batch(&self.pool, &claimed)
                        .await
                } else {
                    runtime
                        .store
                        .complete_job_batch_by_id(
                            &self.pool,
                            &job_ids
                                .iter()
                                .copied()
                                .zip(run_leases.iter().copied())
                                .collect::<Vec<_>>(),
                        )
                        .await
                }
            }
        };

        match updated {
            Ok(updated_rows) => {
                let updated: HashSet<(i64, i64)> = updated_rows.into_iter().collect();
                let updated_count = updated.len();
                self.metrics.record_completion_flush(
                    self.shard_id,
                    job_ids.len() as u64,
                    flush_start.elapsed(),
                );
                for request in batch {
                    let _ = request
                        .response
                        .send(Ok(updated.contains(&(request.job_id, request.run_lease))));
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
    use std::sync::Arc;
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
        sqlx::query("DELETE FROM awa.jobs_hot WHERE queue = $1")
            .bind(queue)
            .execute(pool)
            .await
            .expect("Failed to clean canonical queue jobs");
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

    async fn reset_running_jobs(pool: &PgPool, queue: &str) {
        sqlx::query(
            r#"
            UPDATE awa.jobs_hot
            SET state = 'running',
                finalized_at = NULL,
                heartbeat_at = now(),
                progress = NULL
            WHERE queue = $1
            "#,
        )
        .bind(queue)
        .execute(pool)
        .await
        .expect("Failed to reset running jobs");
    }

    async fn set_run_lease(pool: &PgPool, job_id: i64, run_lease: i64) {
        sqlx::query(
            r#"
            UPDATE awa.jobs_hot
            SET run_lease = $2,
                attempt = GREATEST(attempt, $2::smallint),
                heartbeat_at = now(),
                finalized_at = NULL,
                progress = NULL
            WHERE id = $1
            "#,
        )
        .bind(job_id)
        .bind(run_lease)
        .execute(pool)
        .await
        .expect("Failed to set run lease");
    }

    async fn complete_jobs_in_lock_order(
        pool: &PgPool,
        first_id: i64,
        first_lease: i64,
        second_id: i64,
        second_lease: i64,
    ) -> Vec<(i64, i64)> {
        sqlx::query_as::<_, (i64, i64)>(COMPLETE_BATCH_SQL)
            .bind(vec![second_id, first_id])
            .bind(vec![second_lease, first_lease])
            .fetch_all(pool)
            .await
            .expect("completion sweep failed")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn completion_and_heartbeat_batches_do_not_deadlock_with_reversed_input_order() {
        let pool = setup(10).await;
        let queue = "test_completion_heartbeat_lock_order";
        clean_queue(&pool, queue).await;

        let jobs = seed_running_jobs(&pool, queue, 2).await;
        let (first_id, first_lease) = jobs[0];
        let (second_id, second_lease) = jobs[1];

        for iteration in 0..20 {
            if iteration > 0 {
                reset_running_jobs(&pool, queue).await;
            }

            let barrier = Arc::new(tokio::sync::Barrier::new(3));

            let completion_pool = pool.clone();
            let completion_barrier = barrier.clone();
            let completion_task = tokio::spawn(async move {
                completion_barrier.wait().await;
                complete_jobs_in_lock_order(
                    &completion_pool,
                    first_id,
                    first_lease,
                    second_id,
                    second_lease,
                )
                .await
            });

            let heartbeat_pool = pool.clone();
            let heartbeat_barrier = barrier.clone();
            let heartbeat_task = tokio::spawn(async move {
                heartbeat_barrier.wait().await;
                sqlx::query(
                    r#"
                    WITH inflight AS (
                        SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::jsonb[]) AS v(id, run_lease, progress)
                    ),
                    locked AS (
                        SELECT jobs.ctid, inflight.progress
                        FROM awa.jobs_hot AS jobs
                        JOIN inflight
                          ON jobs.id = inflight.id
                         AND jobs.run_lease = inflight.run_lease
                        WHERE jobs.state = 'running'
                        ORDER BY jobs.id
                        FOR UPDATE OF jobs
                    )
                    UPDATE awa.jobs_hot AS jobs
                    SET heartbeat_at = now(),
                        progress = locked.progress
                    FROM locked
                    WHERE jobs.ctid = locked.ctid
                    "#,
                )
                .bind(vec![first_id, second_id])
                .bind(vec![first_lease, second_lease])
                .bind(vec![
                    serde_json::json!({ "iteration": iteration, "job": 1 }),
                    serde_json::json!({ "iteration": iteration, "job": 2 }),
                ])
                .execute(&heartbeat_pool)
                .await
            });

            barrier.wait().await;

            let (completed_ids, heartbeat_result) =
                tokio::time::timeout(Duration::from_secs(5), async move {
                    let completed_ids = completion_task.await.expect("completion task panicked");
                    let heartbeat_result = heartbeat_task.await.expect("heartbeat task panicked");
                    (completed_ids, heartbeat_result)
                })
                .await
                .expect("concurrent completion/heartbeat batches timed out");

            heartbeat_result.expect("heartbeat batch query failed");
            assert!(
                completed_ids.len() <= 2,
                "completion batch returned too many rows in iteration {iteration}"
            );

            complete_jobs_in_lock_order(&pool, first_id, first_lease, second_id, second_lease)
                .await;

            let completed: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM awa.jobs_hot WHERE queue = $1 AND state = 'completed'",
            )
            .bind(queue)
            .fetch_one(&pool)
            .await
            .expect("Failed to count completed rows");
            assert_eq!(
                completed, 2,
                "expected both rows completed after concurrent batch iteration {iteration}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stale_completion_in_same_batch_returns_false_for_old_run_lease() {
        let pool = setup(10).await;
        let queue = "test_completion_stale_lease_batch";
        clean_queue(&pool, queue).await;

        let jobs = seed_running_jobs(&pool, queue, 1).await;
        let (job_id, stale_lease) = jobs[0];
        let current_lease = stale_lease + 1;
        set_run_lease(&pool, job_id, current_lease).await;

        let (batcher, handle) = CompletionBatcher::new(
            pool.clone(),
            CancellationToken::new(),
            crate::metrics::AwaMetrics::from_global(),
            crate::storage::RuntimeStorage::Canonical,
        );
        let workers = batcher.spawn();

        let barrier = Arc::new(tokio::sync::Barrier::new(3));

        let stale_handle = handle.clone();
        let stale_barrier = barrier.clone();
        let stale_task = tokio::spawn(async move {
            stale_barrier.wait().await;
            stale_handle.complete(job_id, stale_lease).await
        });

        let current_handle = handle.clone();
        let current_barrier = barrier.clone();
        let current_task = tokio::spawn(async move {
            current_barrier.wait().await;
            current_handle.complete(job_id, current_lease).await
        });

        barrier.wait().await;

        let stale_result = stale_task
            .await
            .expect("stale completion task panicked")
            .expect("stale completion request failed");
        let current_result = current_task
            .await
            .expect("current completion task panicked")
            .expect("current completion request failed");

        assert!(
            !stale_result,
            "stale completion should be reported as ignored"
        );
        assert!(
            current_result,
            "current completion should be reported as applied"
        );

        let row: (String, i64) =
            sqlx::query_as("SELECT state::text, run_lease FROM awa.jobs_hot WHERE id = $1")
                .bind(job_id)
                .fetch_one(&pool)
                .await
                .expect("Failed to load completed row");
        assert_eq!(row.0, "completed");
        assert_eq!(row.1, current_lease);

        for worker in workers {
            worker.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn completion_and_heartbeat_only_batches_do_not_deadlock_with_reversed_input_order() {
        let pool = setup(10).await;
        let queue = "test_completion_heartbeat_only_lock_order";
        clean_queue(&pool, queue).await;

        let jobs = seed_running_jobs(&pool, queue, 2).await;
        let (first_id, first_lease) = jobs[0];
        let (second_id, second_lease) = jobs[1];

        for iteration in 0..20 {
            if iteration > 0 {
                reset_running_jobs(&pool, queue).await;
            }

            let barrier = Arc::new(tokio::sync::Barrier::new(3));

            let completion_pool = pool.clone();
            let completion_barrier = barrier.clone();
            let completion_task = tokio::spawn(async move {
                completion_barrier.wait().await;
                complete_jobs_in_lock_order(
                    &completion_pool,
                    first_id,
                    first_lease,
                    second_id,
                    second_lease,
                )
                .await
            });

            let heartbeat_pool = pool.clone();
            let heartbeat_barrier = barrier.clone();
            let heartbeat_task = tokio::spawn(async move {
                heartbeat_barrier.wait().await;
                sqlx::query(
                    r#"
                    WITH inflight AS (
                        SELECT * FROM unnest($1::bigint[], $2::bigint[]) AS v(id, run_lease)
                    ),
                    locked AS (
                        SELECT jobs.ctid
                        FROM awa.jobs_hot AS jobs
                        JOIN inflight
                          ON jobs.id = inflight.id
                         AND jobs.run_lease = inflight.run_lease
                        WHERE jobs.state = 'running'
                        ORDER BY jobs.id
                        FOR UPDATE OF jobs
                    )
                    UPDATE awa.jobs_hot AS jobs
                    SET heartbeat_at = now()
                    FROM locked
                    WHERE jobs.ctid = locked.ctid
                    "#,
                )
                .bind(vec![first_id, second_id])
                .bind(vec![first_lease, second_lease])
                .execute(&heartbeat_pool)
                .await
            });

            barrier.wait().await;

            let (completed_ids, heartbeat_result) =
                tokio::time::timeout(Duration::from_secs(5), async move {
                    let completed_ids = completion_task.await.expect("completion task panicked");
                    let heartbeat_result = heartbeat_task.await.expect("heartbeat task panicked");
                    (completed_ids, heartbeat_result)
                })
                .await
                .expect("concurrent completion/heartbeat-only batches timed out");

            heartbeat_result.expect("heartbeat-only batch query failed");
            assert!(
                completed_ids.len() <= 2,
                "completion batch returned too many rows in iteration {iteration}"
            );

            complete_jobs_in_lock_order(&pool, first_id, first_lease, second_id, second_lease)
                .await;

            let completed: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM awa.jobs_hot WHERE queue = $1 AND state = 'completed'",
            )
            .bind(queue)
            .fetch_one(&pool)
            .await
            .expect("Failed to count completed rows");
            assert_eq!(
                completed, 2,
                "expected both rows completed after concurrent heartbeat-only iteration {iteration}"
            );
        }
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
            crate::storage::RuntimeStorage::Canonical,
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
