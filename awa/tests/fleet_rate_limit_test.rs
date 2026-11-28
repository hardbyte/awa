//! Fleet rate limits (ADR-054, #718): runtimes serving one queue split a
//! fleet-wide rate between them, and an operator can enable and clear the
//! limit live, with each runtime reporting the override revision it applied.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

mod ci_timing;

use awa::{Client, JobArgs, JobContext, JobError, JobResult, QueueConfig, RateLimit};
use awa_model::admin::{
    clear_queue_runtime_overrides_audited, queue_override_application,
    queue_runtime_override_state, set_queue_runtime_overrides_audited, QueueRuntimeOverrideAudit,
    QueueRuntimeOverrides,
};
use awa_model::{insert_many, migrations, InsertOpts, InsertParams};
use ci_timing::{contention_floor, scaled_timeout};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Overrides refresh every 200 ms and runtimes publish snapshots every
/// 100 ms, so shares settle well inside the warm-up.
const REFRESH_MS: &str = "200";
const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(100);
const WARM_UP: Duration = Duration::from_secs(2);
const WINDOW: Duration = Duration::from_secs(3);

/// Setup resets the runtime backend, which deletes every runtime snapshot
/// and so would hide one test's runtimes from another's shares. The tests
/// in this file run one at a time.
static SERIAL: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

async fn setup(queue: &str) -> PgPool {
    // Must precede every client build: the dispatcher caches the cadence.
    std::env::set_var("AWA_OVERRIDE_REFRESH_MS", REFRESH_MS);
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url())
        .await
        .expect("Failed to connect — is Postgres running?");
    migrations::run(&pool).await.expect("Failed to migrate");
    awa_testing::setup::reset_runtime_backend(&pool).await;
    sqlx::query("DELETE FROM awa.jobs WHERE queue = $1")
        .bind(queue)
        .execute(&pool)
        .await
        .expect("queue cleanup");
    sqlx::query("DELETE FROM awa.queue_meta WHERE queue = $1")
        .bind(queue)
        .execute(&pool)
        .await
        .expect("queue meta cleanup");
    pool
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct FleetRateJob {
    pub index: i64,
}

/// When each job's handler started, across every runtime in the test.
#[derive(Clone, Default)]
struct Starts(Arc<Mutex<Vec<Instant>>>);

impl Starts {
    fn between(&self, from: Instant, to: Instant) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|start| **start >= from && **start < to)
            .count()
    }
}

struct StartRecorder(Starts);

#[async_trait::async_trait]
impl awa::Worker for StartRecorder {
    fn kind(&self) -> &'static str {
        "fleet_rate_job"
    }

    async fn perform(&self, _ctx: &JobContext) -> Result<JobResult, JobError> {
        self.0 .0.lock().unwrap().push(Instant::now());
        Ok(JobResult::Completed)
    }
}

async fn enqueue(pool: &PgPool, queue: &str, count: i64) {
    let jobs: Vec<InsertParams> = (0..count)
        .map(|index| {
            awa_model::insert::params_with(
                &FleetRateJob { index },
                InsertOpts {
                    queue: queue.into(),
                    ..Default::default()
                },
            )
            .expect("params")
        })
        .collect();
    insert_many(pool, &jobs).await.expect("enqueue");
}

async fn start_fleet(
    pool: &PgPool,
    queue: &str,
    runtimes: usize,
    fleet_rate_limit: Option<RateLimit>,
    starts: &Starts,
) -> Vec<Client> {
    let mut clients = Vec::with_capacity(runtimes);
    for _ in 0..runtimes {
        let client = Client::builder(pool.clone())
            .queue(
                queue,
                QueueConfig {
                    max_workers: 4,
                    poll_interval: Duration::from_millis(20),
                    fleet_rate_limit: fleet_rate_limit.clone(),
                    ..QueueConfig::default()
                },
            )
            .runtime_snapshot_interval(SNAPSHOT_INTERVAL)
            .register_worker(StartRecorder(starts.clone()))
            .build()
            .expect("client should build");
        client.start().await.expect("client should start");
        clients.push(client);
    }
    clients
}

async fn shutdown(clients: Vec<Client>) {
    for client in clients {
        client.shutdown(Duration::from_secs(5)).await;
    }
}

/// Job starts during [`WINDOW`], once [`WARM_UP`] has let every runtime see
/// the others and settle its share.
async fn starts_in_window(starts: &Starts) -> usize {
    tokio::time::sleep(WARM_UP).await;
    let from = Instant::now();
    tokio::time::sleep(WINDOW).await;
    starts.between(from, Instant::now())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fleet_rate_limit_holds_across_one_two_and_four_runtimes() {
    let _serial = serial().await;
    const RATE: f64 = 20.0;
    for runtimes in [1usize, 2, 4] {
        let queue = format!("fleet_rate_{runtimes}");
        let pool = setup(&queue).await;
        enqueue(&pool, &queue, 400).await;
        let starts = Starts::default();
        let clients = start_fleet(
            &pool,
            &queue,
            runtimes,
            Some(RateLimit {
                max_rate: RATE,
                burst: 1,
            }),
            &starts,
        )
        .await;

        let started = starts_in_window(&starts).await as f64;
        shutdown(clients).await;

        // Each runtime may start one burst token above its share's refill.
        let expected = RATE * WINDOW.as_secs_f64();
        let ceiling = expected * 1.15 + runtimes as f64;
        assert!(
            started <= ceiling,
            "{runtimes} runtimes started {started} jobs in {WINDOW:?}, above the fleet \
             limit's {expected} (ceiling {ceiling})"
        );
        let floor = contention_floor((expected * 0.6) as i64) as f64;
        assert!(
            started >= floor,
            "{runtimes} runtimes started only {started} jobs in {WINDOW:?}; the fleet limit \
             allows {expected} (floor {floor})"
        );
    }
}

/// Wait until every live runtime serving `queue` reports `revision` applied.
async fn wait_for_application(pool: &PgPool, queue: &str, revision: i64, runtimes: usize) {
    let deadline = Instant::now() + scaled_timeout(Duration::from_secs(10));
    loop {
        let application = queue_override_application(pool, queue, revision)
            .await
            .expect("application");
        if application.live_runtimes == runtimes && application.applied_current == runtimes {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "revision {revision} not applied by all {runtimes} runtimes: {application:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_override_enables_and_clears_a_fleet_limit_live() {
    let _serial = serial().await;
    const RATE: f64 = 10.0;
    const RUNTIMES: usize = 2;
    let queue = "fleet_rate_override";
    let pool = setup(queue).await;
    let starts = Starts::default();
    let clients = start_fleet(&pool, queue, RUNTIMES, None, &starts).await;

    let audit = QueueRuntimeOverrideAudit {
        updated_by: Some("fleet_rate_limit_test".into()),
        reason: Some("protect the downstream API".into()),
    };
    let revision = set_queue_runtime_overrides_audited(
        &pool,
        queue,
        &QueueRuntimeOverrides {
            fleet_rate_limit: Some(RATE),
            fleet_rate_burst: Some(1),
            ..Default::default()
        },
        &audit,
    )
    .await
    .expect("set overrides");
    assert!(revision > 0, "a write on v065 advances the revision");
    let state = queue_runtime_override_state(&pool, queue)
        .await
        .expect("state")
        .expect("the write created the control row");
    assert_eq!(state.revision, revision);
    assert_eq!(state.updated_by, audit.updated_by);
    assert_eq!(state.reason, audit.reason);

    wait_for_application(&pool, queue, revision, RUNTIMES).await;
    let application = queue_override_application(&pool, queue, revision)
        .await
        .expect("application");
    let shares: f64 = application
        .runtimes
        .iter()
        .map(|runtime| {
            let control = runtime
                .rate_control
                .as_ref()
                .expect("reports its rate control");
            assert!(
                control.effective_rate_limit.is_some(),
                "the limit is enabled live"
            );
            control.fleet_share.expect("a fleet share")
        })
        .sum();
    assert!(
        (shares - 1.0).abs() < 1e-9,
        "the runtimes' shares cover the fleet limit once: {shares}"
    );

    enqueue(&pool, queue, 300).await;
    let started = starts_in_window(&starts).await as f64;
    let expected = RATE * WINDOW.as_secs_f64();
    assert!(
        started <= expected * 1.15 + RUNTIMES as f64,
        "the live fleet limit allows {expected} starts in {WINDOW:?}, saw {started}"
    );

    let cleared = clear_queue_runtime_overrides_audited(&pool, queue, &audit)
        .await
        .expect("clear overrides");
    assert!(cleared > revision);
    wait_for_application(&pool, queue, cleared, RUNTIMES).await;
    let application = queue_override_application(&pool, queue, cleared)
        .await
        .expect("application");
    assert!(
        application.runtimes.iter().all(|runtime| runtime
            .rate_control
            .as_ref()
            .is_some_and(|control| control.effective_rate_limit.is_none())),
        "clearing the override removes the limit live: {application:?}"
    );

    let deadline = Instant::now() + scaled_timeout(Duration::from_secs(5));
    while starts.between(Instant::now() - Duration::from_secs(3600), Instant::now()) < 300 {
        assert!(
            Instant::now() < deadline,
            "without the limit the backlog drains quickly"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    shutdown(clients).await;
}
