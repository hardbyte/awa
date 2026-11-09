use awa::model::{migrations, runtime_control, storage, QueueStorage};
use awa::{Client, InsertOpts, JobArgs, JobResult, JobState, QueueConfig, QueueStorageConfig};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const QUEUE: &str = "startup_control_probe";

#[derive(Serialize, Deserialize, JobArgs)]
struct StartupProbe {}

async fn build_client(pool: &PgPool, performed: Arc<AtomicUsize>) -> (Client, PgPool) {
    let runtime_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let client = Client::builder(runtime_pool.clone())
        .queue(
            QUEUE,
            QueueConfig {
                max_workers: 1,
                poll_interval: Duration::from_millis(20),
                ..Default::default()
            },
        )
        .register::<StartupProbe, _, _>(move |_args, _context| {
            let performed = performed.clone();
            async move {
                performed.fetch_add(1, Ordering::SeqCst);
                Ok(JobResult::Completed)
            }
        })
        .leader_election_interval(Duration::from_millis(50))
        .runtime_snapshot_interval(Duration::from_millis(100))
        .build()
        .unwrap();
    (client, runtime_pool)
}

async fn assert_active_queue_storage(pool: &PgPool) {
    let status = storage::status(pool).await.unwrap();
    assert_eq!(status.state, "active");
    assert_eq!(status.active_engine, "queue_storage");
    assert_eq!(status.current_engine, "queue_storage");
}

async fn assert_descriptors_unpublished(pool: &PgPool) {
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM awa.queue_descriptors WHERE queue = $1")
            .bind(QUEUE)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(
        count, 0,
        "descriptor publication remains gated by prune control"
    );
}

async fn enqueue_and_complete(pool: &PgPool, performed: &AtomicUsize) {
    let job = awa::insert_with(
        pool,
        &StartupProbe {},
        InsertOpts {
            queue: QUEUE.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let store = QueueStorage::new(QueueStorageConfig::default()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let loaded = store.load_job(pool, job.id).await.unwrap().unwrap();
            if loaded.state == JobState::Completed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("fresh startup must dispatch and acknowledge completion in queue storage");
    assert_eq!(performed.load(Ordering::SeqCst), 1);
}

#[sqlx::test(migrations = false)]
async fn fresh_start_during_prune_halt_activates_queue_storage_and_dispatches(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    runtime_control::halt(&pool, runtime_control::PRUNE_SCOPE, "startup", "drain")
        .await
        .unwrap();
    let performed = Arc::new(AtomicUsize::new(0));
    let (client, runtime_pool) = build_client(&pool, performed.clone()).await;

    client.start().await.unwrap();
    assert_active_queue_storage(&pool).await;
    assert_descriptors_unpublished(&pool).await;
    enqueue_and_complete(&pool, &performed).await;

    client.shutdown(Duration::from_secs(5)).await;
    runtime_pool.close().await;
}

#[sqlx::test(migrations = false)]
async fn fresh_start_after_control_read_timeout_activates_queue_storage_and_dispatches(
    pool: PgPool,
) {
    migrations::run(&pool).await.unwrap();
    let mut blocked_control = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE awa.runtime_controls IN ACCESS EXCLUSIVE MODE")
        .execute(blocked_control.as_mut())
        .await
        .unwrap();
    let performed = Arc::new(AtomicUsize::new(0));
    let (client, runtime_pool) = build_client(&pool, performed.clone()).await;

    tokio::time::timeout(Duration::from_secs(10), client.start())
        .await
        .expect("control read timeout must not prevent startup")
        .unwrap();
    assert_active_queue_storage(&pool).await;
    assert_descriptors_unpublished(&pool).await;
    blocked_control.rollback().await.unwrap();
    enqueue_and_complete(&pool, &performed).await;

    client.shutdown(Duration::from_secs(5)).await;
    runtime_pool.close().await;
}
