//! `ClientBuilder::maintenance_leader(false)`: a runtime that never contends
//! for the maintenance advisory lock, even when no other runtime can lead.
//!
//! Each test gets its own database, and the leader lock is per database, so
//! no other test binary can hold it. Runtimes run on their own tokio runtime
//! so a test can kill one without a shutdown handshake, as a crashed process
//! would end.

mod ci_timing;

use awa::model::{admin, migrations, runtime_control, QueueStorage};
use awa::{
    Client, InsertOpts, JobArgs, JobResult, JobState, PeriodicJob, QueueConfig, QueueStorageConfig,
};
use ci_timing::scaled_timeout;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;

const QUEUE: &str = "leader_eligibility";
const LEADER_LOCK_KEY: i64 = 0x4157_415f_4d41_494e;
const ELECTION_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Serialize, Deserialize, JobArgs)]
struct EligibilityProbe {}

/// A runtime on its own tokio runtime. Dropping it kills the runtime: its
/// tasks stop and its connections close without a shutdown handshake.
struct Host {
    runtime: Option<tokio::runtime::Runtime>,
    client: Arc<Client>,
}

impl Host {
    async fn start(pool: &PgPool, name: &str, maintenance_leader: bool, periodic: bool) -> Self {
        let options: PgConnectOptions = (*pool.connect_options()).clone().application_name(name);
        tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let client = runtime.block_on(async move {
                let pool = PgPoolOptions::new()
                    .max_connections(8)
                    .connect_with(options)
                    .await
                    .unwrap();
                let mut builder = Client::builder(pool)
                    .queue(
                        QUEUE,
                        QueueConfig {
                            max_workers: 2,
                            poll_interval: Duration::from_millis(20),
                            ..Default::default()
                        },
                    )
                    .register::<EligibilityProbe, _, _>(|_args, _context| async move {
                        Ok(JobResult::Completed)
                    })
                    .leader_election_interval(ELECTION_INTERVAL)
                    .leader_check_interval(Duration::from_millis(50))
                    .runtime_snapshot_interval(Duration::from_millis(100))
                    .maintenance_leader(maintenance_leader);
                if periodic {
                    builder = builder.periodic(
                        PeriodicJob::builder("leader_eligibility_nightly", "0 3 * * *")
                            .queue(QUEUE)
                            .build(&EligibilityProbe {})
                            .unwrap(),
                    );
                }
                let client = builder.build().unwrap();
                client.start().await.unwrap();
                Arc::new(client)
            });
            Self {
                runtime: Some(runtime),
                client,
            }
        })
        .await
        .unwrap()
    }

    async fn is_leader(&self) -> bool {
        let client = self.client.clone();
        self.runtime
            .as_ref()
            .unwrap()
            .spawn(async move { client.health_check().await })
            .await
            .unwrap()
            .leader
    }

    async fn health(&self) -> awa::HealthCheck {
        let client = self.client.clone();
        self.runtime
            .as_ref()
            .unwrap()
            .spawn(async move { client.health_check().await })
            .await
            .unwrap()
    }

    /// Graceful stop. The runtime stays owned by `self`, so a caller that
    /// abandons this future (a timeout) still drops it through `Drop`.
    async fn shutdown(self) {
        let client = self.client.clone();
        self.runtime
            .as_ref()
            .unwrap()
            .spawn(async move { client.shutdown(Duration::from_secs(5)).await })
            .await
            .unwrap();
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// The `application_name` of the session holding the leader lock in this
/// database, if any.
async fn lock_holder(pool: &PgPool) -> Option<String> {
    sqlx::query_scalar(
        "SELECT a.application_name::text
         FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
         WHERE l.locktype = 'advisory' AND l.granted
           AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
           AND ((l.classid::bigint << 32) | l.objid::bigint) = $1",
    )
    .bind(LEADER_LOCK_KEY)
    .fetch_optional(pool)
    .await
    .unwrap()
}

async fn wait_for_holder(pool: &PgPool, name: &str) {
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    loop {
        if lock_holder(pool).await.as_deref() == Some(name) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{name} did not take the leader lock; holder: {:?}",
            lock_holder(pool).await
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Assert nobody holds the leader lock across many election intervals.
async fn assert_lock_stays_free(pool: &PgPool, host: &Host, window: Duration) {
    let deadline = tokio::time::Instant::now() + window;
    while tokio::time::Instant::now() < deadline {
        assert_eq!(lock_holder(pool).await, None, "the leader lock was taken");
        assert!(
            !host.is_leader().await,
            "an opted-out runtime reports leadership"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The only runtime snapshot in the database, once it has been published.
async fn only_instance(pool: &PgPool) -> admin::RuntimeInstance {
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    loop {
        let mut instances = admin::list_runtime_instances(pool).await.unwrap();
        if instances.len() == 1 {
            return instances.remove(0);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected one runtime snapshot, found {}",
            instances.len()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn run_one_job(pool: &PgPool) {
    let job = awa::insert_with(
        pool,
        &EligibilityProbe {},
        InsertOpts {
            queue: QUEUE.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let store = QueueStorage::new(QueueStorageConfig::default()).unwrap();
    tokio::time::timeout(scaled_timeout(Duration::from_secs(10)), async {
        loop {
            let loaded = store.load_job(pool, job.id).await.unwrap().unwrap();
            if loaded.state == JobState::Completed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("an opted-out runtime still executes jobs");
}

#[sqlx::test(migrations = false)]
async fn opted_out_runtime_never_leads_even_alone(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    let host = Host::start(&pool, "opted_out", false, true).await;

    assert_lock_stays_free(&pool, &host, Duration::from_millis(1_500)).await;

    // Everything but leadership still runs.
    let health = host.health().await;
    assert!(health.healthy, "{health:?}");
    assert!(health.maintenance_alive);
    assert!(health.heartbeat_alive);
    run_one_job(&pool).await;

    let instance = only_instance(&pool).await;
    assert!(!instance.maintenance_leader);
    assert!(!instance.leader);
    let overview = admin::runtime_overview(&pool).await.unwrap();
    assert_eq!(overview.live_instances, 1);
    assert_eq!(overview.leader_eligible_instances, 0);
    let status = runtime_control::status(&pool).await.unwrap();
    assert_eq!(status.leader_eligible_runtimes, 0);
    assert_eq!(status.runtimes.len(), 1);
    assert!(!status.runtimes[0].maintenance_leader);

    // The leader syncs only its own declarations, so a runtime that never
    // leads registers its schedules itself.
    let synced: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM awa.cron_jobs WHERE name = 'leader_eligibility_nightly')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(synced, "an opted-out runtime must sync its periodic jobs");

    host.shutdown().await;
}

#[sqlx::test(migrations = false)]
async fn leadership_stays_vacant_until_an_eligible_runtime_returns(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    let opted_out = Host::start(&pool, "opted_out", false, false).await;
    let eligible = Host::start(&pool, "eligible", true, false).await;

    wait_for_holder(&pool, "eligible").await;
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    while !eligible.is_leader().await {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!opted_out.is_leader().await);
    let overview = admin::runtime_overview(&pool).await.unwrap();
    assert_eq!(overview.live_instances, 2);
    assert_eq!(overview.leader_eligible_instances, 1);
    assert_eq!(
        runtime_control::status(&pool)
            .await
            .unwrap()
            .leader_eligible_runtimes,
        1
    );

    // Kill the leader without a shutdown handshake. Its session closes, so
    // the lock is free, and only the opted-out runtime is left to take it.
    drop(eligible);
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    while lock_holder(&pool).await.is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the killed leader's session kept the lock"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_lock_stays_free(&pool, &opted_out, Duration::from_millis(1_500)).await;

    let returning = Host::start(&pool, "eligible_again", true, false).await;
    wait_for_holder(&pool, "eligible_again").await;
    assert!(!opted_out.is_leader().await);

    returning.shutdown().await;
    opted_out.shutdown().await;
}

/// A new binary against a database without the schema patch: the opt-out is
/// enforced but not reported until `awa migrate` adds the table.
#[sqlx::test(migrations = false)]
async fn opt_out_is_enforced_without_the_patch_and_reported_once_applied(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    sqlx::query("DROP TABLE awa.runtime_maintenance_eligibility")
        .execute(&pool)
        .await
        .unwrap();
    assert!(!admin::maintenance_eligibility_reportable(&pool)
        .await
        .unwrap());

    let host = Host::start(&pool, "opted_out", false, false).await;
    assert_lock_stays_free(&pool, &host, Duration::from_millis(1_000)).await;
    assert!(
        only_instance(&pool).await.maintenance_leader,
        "without the patch every runtime reads as eligible"
    );

    migrations::run(&pool).await.unwrap();
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    loop {
        let overview = admin::runtime_overview(&pool).await.unwrap();
        if overview.instances.iter().all(|i| !i.maintenance_leader) {
            assert_eq!(overview.leader_eligible_instances, 0);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the next snapshot after the patch must record the opt-out"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(lock_holder(&pool).await, None);

    host.shutdown().await;
}

/// The leader's cleanup removes an opted-out runtime's row once its snapshot
/// row has gone, and keeps a row younger than the grace period.
#[sqlx::test(migrations = false)]
async fn orphan_eligibility_rows_are_swept(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    let orphan = uuid::Uuid::new_v4();
    let fresh = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO awa.runtime_maintenance_eligibility (instance_id, maintenance_leader, recorded_at)
         VALUES ($1, false, now() - interval '1 hour'), ($2, false, now())",
    )
    .bind(orphan)
    .bind(fresh)
    .execute(&pool)
    .await
    .unwrap();

    let deleted = admin::cleanup_runtime_maintenance_eligibility(
        &pool,
        chrono::TimeDelta::try_minutes(10).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(deleted, 1);
    let left: Vec<uuid::Uuid> =
        sqlx::query_scalar("SELECT instance_id FROM awa.runtime_maintenance_eligibility")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(left, vec![fresh]);
}

/// An opted-out runtime syncs its periodic jobs outside any leader term. A
/// sync waiting on a cron row that another session has locked must not hold
/// shutdown open indefinitely.
#[sqlx::test(migrations = false)]
async fn opted_out_shutdown_is_bounded_while_cron_sync_waits_on_a_lock(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    let first = Host::start(&pool, "registers_cron", false, true).await;
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    loop {
        let synced: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM awa.cron_jobs WHERE name = 'leader_eligibility_nightly')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if synced {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "cron never synced");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    first.shutdown().await;

    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM awa.cron_jobs WHERE name = 'leader_eligibility_nightly' FOR UPDATE")
        .execute(&mut *holder)
        .await
        .unwrap();

    let blocked = Host::start(&pool, "blocked_cron_sync", false, true).await;
    let deadline = tokio::time::Instant::now() + scaled_timeout(Duration::from_secs(10));
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                            WHERE application_name = 'blocked_cron_sync'
                              AND wait_event_type = 'Lock')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if waiting {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the startup cron sync never waited on the locked row"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 5s drain + 5s maintenance grace, with margin.
    tokio::time::timeout(scaled_timeout(Duration::from_secs(20)), blocked.shutdown())
        .await
        .expect("shutdown waited on the blocked cron sync without bound");
    holder.rollback().await.unwrap();
}

/// A role that may INSERT into the eligibility table but not SELECT from it.
/// Naming a conflict target would need SELECT, and a failed opt-out write must
/// never stop the runtime snapshot that marks the runtime live.
#[sqlx::test(migrations = false)]
async fn partial_grants_never_block_the_runtime_snapshot(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let insert_only = format!("eligibility_insert_{suffix}");
    let ungranted = format!("eligibility_none_{suffix}");
    for role in [&insert_only, &ungranted] {
        sqlx::raw_sql(&format!(
            "CREATE ROLE {role} LOGIN PASSWORD 'eligibility';
             GRANT USAGE ON SCHEMA awa TO {role};
             GRANT SELECT, INSERT, UPDATE ON awa.runtime_instances TO {role};"
        ))
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::raw_sql(&format!(
        "GRANT INSERT ON awa.runtime_maintenance_eligibility TO {insert_only};
         GRANT DELETE ON awa.runtime_maintenance_eligibility TO {insert_only};"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let connect_as = |role: &str| {
        let options = (*pool.connect_options())
            .clone()
            .username(role)
            .password("eligibility");
        PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
    };
    let snapshot = |instance_id| admin::RuntimeSnapshotInput {
        instance_id,
        hostname: Some("partial-grants".into()),
        pid: 1,
        version: "test".into(),
        storage_capability: admin::StorageCapability::Canonical,
        transition_role: admin::TransitionRole::Auto,
        started_at: chrono::Utc::now(),
        snapshot_interval_ms: 10_000,
        healthy: true,
        postgres_connected: true,
        poll_loop_alive: true,
        heartbeat_alive: true,
        maintenance_alive: true,
        shutting_down: false,
        leader: false,
        maintenance_leader: false,
        global_max_workers: None,
        queues: vec![],
        queue_descriptor_hashes: Default::default(),
        job_kind_descriptor_hashes: Default::default(),
    };
    let recorded = |instance_id| {
        let pool = pool.clone();
        async move {
            let runtime: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM awa.runtime_instances WHERE instance_id = $1)",
            )
            .bind(instance_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            let opt_out: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM awa.runtime_maintenance_eligibility
                                WHERE instance_id = $1 AND NOT maintenance_leader)",
            )
            .bind(instance_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            (runtime, opt_out)
        }
    };

    let insert_pool = connect_as(&insert_only).await.unwrap();
    let id = uuid::Uuid::new_v4();
    // Twice: the second write takes the conflict path.
    for _ in 0..2 {
        admin::upsert_runtime_snapshot(&insert_pool, &snapshot(id))
            .await
            .expect("an INSERT-only role publishes its snapshot");
    }
    assert_eq!(recorded(id).await, (true, true));
    // Without SELECT the sweep cannot evaluate its WHERE clause, so it skips,
    // and readers fall back to "every runtime is eligible".
    assert_eq!(
        admin::cleanup_runtime_maintenance_eligibility(
            &insert_pool,
            chrono::TimeDelta::try_minutes(10).unwrap()
        )
        .await
        .unwrap(),
        0
    );
    let listed = admin::list_runtime_instances(&insert_pool).await.unwrap();
    assert!(listed.iter().all(|instance| instance.maintenance_leader));
    // Recorded but not readable by this role, so the runtime warns at
    // startup that its opt-out is not reported.
    assert!(!admin::maintenance_eligibility_reportable(&insert_pool)
        .await
        .unwrap());
    insert_pool.close().await;

    let ungranted_pool = connect_as(&ungranted).await.unwrap();
    let id = uuid::Uuid::new_v4();
    admin::upsert_runtime_snapshot(&ungranted_pool, &snapshot(id))
        .await
        .expect("a role with no grant on the table publishes its snapshot");
    assert_eq!(recorded(id).await, (true, false));
    assert!(!admin::maintenance_eligibility_reportable(&ungranted_pool)
        .await
        .unwrap());
    ungranted_pool.close().await;

    for role in [&insert_only, &ungranted] {
        sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role};"))
            .execute(&pool)
            .await
            .unwrap();
    }
}
