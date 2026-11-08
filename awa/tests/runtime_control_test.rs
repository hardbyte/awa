use awa::model::{migrations, runtime_control, QueueStorage};
use awa::{Client, InsertOpts, JobArgs, JobResult, QueueConfig, QueueStorageConfig};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize, JobArgs)]
struct Probe {
    value: u32,
}

async fn wait_for_completed(pool: &PgPool, count: i64) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let completed: i64 = sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE queue = 'control_probe' AND state = 'completed'").fetch_one(pool).await.unwrap();
            if completed == count { return; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("dispatch, promotion and rescue must continue during a prune halt");
}

#[sqlx::test(migrations = false)]
async fn prune_halt_survives_handoff_and_preserves_exact_job_accounting(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    let store = QueueStorage::new(QueueStorageConfig::default()).unwrap();
    store.install(&pool).await.unwrap();
    let halt = runtime_control::halt(
        &pool,
        runtime_control::PRUNE_SCOPE,
        "reclamation rehearsal",
        "drain",
    )
    .await
    .unwrap();
    let performed = Arc::new(AtomicUsize::new(0));
    let build = |runtime_pool: PgPool| {
        let performed = performed.clone();
        Client::builder(runtime_pool)
            .queue(
                "control_probe",
                QueueConfig {
                    max_workers: 2,
                    poll_interval: Duration::from_millis(20),
                    ..Default::default()
                },
            )
            .register::<Probe, _, _>(move |_args, _context| {
                let performed = performed.clone();
                async move {
                    performed.fetch_add(1, Ordering::SeqCst);
                    Ok(JobResult::Completed)
                }
            })
            .queue_storage(
                QueueStorageConfig::default(),
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .claim_rotate_interval(Duration::from_secs(1))
            .promote_interval(Duration::from_millis(50))
            .deadline_rescue_interval(Duration::from_millis(100))
            .heartbeat_rescue_interval(Duration::from_millis(100))
            .leader_election_interval(Duration::from_millis(50))
            .runtime_snapshot_interval(Duration::from_millis(100))
            .build()
            .unwrap()
    };
    let first = build(pool.clone());
    first.start().await.unwrap();
    let before: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT generation FROM awa.queue_ring_state WHERE singleton), (SELECT generation FROM awa.lease_ring_state WHERE singleton), (SELECT generation FROM awa.claim_ring_state WHERE singleton)").fetch_one(&pool).await.unwrap();
    // Enqueue inside an application transaction, including scheduled work.
    let mut application = pool.begin().await.unwrap();
    for value in 0..8 {
        awa::insert_with(
            application.as_mut(),
            &Probe { value },
            InsertOpts {
                queue: "control_probe".into(),
                run_at: (value % 2 == 1)
                    .then_some(chrono::Utc::now() + chrono::Duration::milliseconds(100)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    application.commit().await.unwrap();
    wait_for_completed(&pool, 8).await;
    // A claimed owner that dies is recovered by the deadline path while prune
    // remains halted. Claim it before adding its queue to the live dispatcher.
    let orphan = awa::insert_with(
        &pool,
        &Probe { value: 99 },
        InsertOpts {
            queue: "orphan_probe".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let claimed = store
        .claim_runtime_batch(&pool, "orphan_probe", 1, Duration::from_millis(100))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let job = store.load_job(&pool, orphan.id).await.unwrap().unwrap();
            if job.state != awa::model::JobState::Running {
                assert!(matches!(
                    job.state,
                    awa::model::JobState::Retryable | awa::model::JobState::Available
                ));
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("deadline rescue remains available");
    assert!(first.health_check().await.leader);
    // Each process has its own pool. Sharing the SQLx fixture's five slots
    // would leave no query capacity after both runtimes' persistent listeners
    // and the maintenance leader have connected.
    let standby_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let standby = build(standby_pool.clone());
    standby.start().await.unwrap();
    // Wait across an ordinary rotation tick: halting must suppress rotation,
    // pruning, folds and housekeeping, not just an isolated helper call.
    tokio::time::sleep(Duration::from_secs(11)).await;
    let current = runtime_control::read(&mut pool.acquire().await.unwrap())
        .await
        .unwrap();
    assert_eq!(current.halt_id, halt.halt_id);
    let observations: Vec<serde_json::Value> = sqlx::query_scalar("SELECT observation FROM awa.runtime_instances JOIN awa.runtime_control_observations USING (instance_id) WHERE leader").fetch_all(&pool).await.unwrap();
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0]["observed_revision"], halt.revision);
    assert_eq!(observations[0]["state"], "halted");
    let standby_observation: serde_json::Value = sqlx::query_scalar("SELECT observation FROM awa.runtime_instances JOIN awa.runtime_control_observations USING (instance_id) WHERE NOT leader AND NOT shutting_down")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(standby_observation["observed_revision"], halt.revision);
    assert_eq!(standby_observation["state"], "halted");
    // Descriptor publication is also gated, without suppressing runtime liveness.
    let descriptor_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.queue_descriptors WHERE queue = 'control_probe'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(descriptor_count, 0);
    let after: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT generation FROM awa.queue_ring_state WHERE singleton), (SELECT generation FROM awa.lease_ring_state WHERE singleton), (SELECT generation FROM awa.claim_ring_state WHERE singleton)").fetch_one(&pool).await.unwrap();
    assert_eq!(before, after, "all three rotation branches stay gated");
    standby.shutdown(Duration::from_secs(5)).await;
    standby_pool.close().await;
    first.shutdown(Duration::from_secs(5)).await;
    let second = build(pool.clone());
    second.start().await.unwrap();
    awa::insert_with(
        &pool,
        &Probe { value: 100 },
        InsertOpts {
            queue: "control_probe".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    wait_for_completed(&pool, 9).await;
    assert_eq!(performed.load(Ordering::SeqCst), 9);
    let newer = runtime_control::halt(
        &pool,
        runtime_control::PRUNE_SCOPE,
        "newer decision",
        "drain",
    )
    .await
    .unwrap();
    assert!(
        runtime_control::resume(&pool, runtime_control::PRUNE_SCOPE, halt.halt_id.unwrap())
            .await
            .is_err()
    );
    let total: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.jobs WHERE queue IN ('control_probe','orphan_probe')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        total, 10,
        "nine completed and one rescued orphan, no lost or duplicated jobs"
    );
    runtime_control::resume(&pool, runtime_control::PRUNE_SCOPE, newer.halt_id.unwrap())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let resumed: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT generation FROM awa.queue_ring_state WHERE singleton), (SELECT generation FROM awa.lease_ring_state WHERE singleton), (SELECT generation FROM awa.claim_ring_state WHERE singleton)").fetch_one(&pool).await.unwrap();
            if resumed.0 > after.0 && resumed.1 > after.1 && resumed.2 > after.2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("all three rings must resume rotation after the current halt is lifted");
    second.shutdown(Duration::from_secs(5)).await;
}
