//! Poison jobs: a job that panics its handler, kills its worker, or hangs past
//! its deadline on every attempt must stop at `max_attempts`, like one whose
//! handler returns an error, instead of being rescued forever.
//!
//! A hung handler that never heartbeats stands in for a worker that dies on
//! every attempt: from the database's side the two are indistinguishable.

use awa::model::{admin, dlq};
use awa::{Client, JobArgs, JobRow, JobState, QueueConfig};
use awa_testing::setup::TestDatabase;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct PanicJob {
    n: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct HangJob {
    n: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct HealthyJob {
    n: i32,
}

#[derive(Clone, Copy)]
enum Engine {
    Canonical,
    QueueStorage,
}

/// How long a poison job may take to exhaust two attempts. The deadline is
/// five minutes unless a test shortens it, so finishing inside this bound
/// shows rescue was not what ended each attempt.
const EXHAUST_WITHIN: Duration = Duration::from_secs(30);

async fn database(engine: Engine) -> TestDatabase {
    match engine {
        Engine::Canonical => TestDatabase::canonical().await,
        Engine::QueueStorage => TestDatabase::queue_storage().await,
    }
}

/// Fast maintenance so rescue and promotion happen within a test's patience.
fn builder(pool: &sqlx::PgPool, engine: Engine) -> awa::ClientBuilder {
    let builder = Client::builder(pool.clone());
    let builder = match engine {
        Engine::Canonical => builder.canonical_storage(),
        Engine::QueueStorage => builder.queue_storage(
            Default::default(),
            Duration::from_secs(60),
            Duration::from_secs(1),
        ),
    };
    builder
        .leader_election_interval(Duration::from_millis(100))
        .promote_interval(Duration::from_millis(50))
        .heartbeat_rescue_interval(Duration::from_millis(200))
        .deadline_rescue_interval(Duration::from_millis(200))
        .runtime_snapshot_interval(Duration::from_millis(100))
}

fn queue_config(max_workers: u32, deadline: Duration) -> QueueConfig {
    QueueConfig {
        max_workers,
        poll_interval: Duration::from_millis(25),
        deadline_duration: deadline,
        ..Default::default()
    }
}

async fn insert<T: JobArgs + Serialize>(pool: &sqlx::PgPool, args: &T, queue: &str) -> JobRow {
    awa::insert_with(
        pool,
        args,
        awa::InsertOpts {
            queue: queue.into(),
            max_attempts: 2,
            ..Default::default()
        },
    )
    .await
    .expect("insert job")
}

async fn eventually<T, F, Fut>(within: Duration, what: &str, mut probe: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + within;
    loop {
        if let Some(value) = probe().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {within:?} waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn dead_lettered(pool: &sqlx::PgPool, job_id: i64) -> dlq::DlqRow {
    eventually(EXHAUST_WITHIN, "the job to reach the DLQ", || async {
        dlq::get_dlq_job(pool, job_id).await.expect("read DLQ")
    })
    .await
}

async fn failed(pool: &sqlx::PgPool, job_id: i64) -> JobRow {
    eventually(EXHAUST_WITHIN, "the job to fail", || async {
        admin::get_job(pool, job_id)
            .await
            .ok()
            .filter(|job| job.state == JobState::Failed)
    })
    .await
}

fn error_messages(job: &JobRow) -> Vec<String> {
    job.errors
        .iter()
        .flatten()
        .filter_map(|entry| {
            entry
                .get("error")
                .and_then(|e| e.as_str())
                .map(str::to_owned)
        })
        .collect()
}

/// The in-flight count a live runtime last reported for `queue`.
async fn queue_in_flight(pool: &sqlx::PgPool, queue: &str) -> Option<u32> {
    let instances = admin::list_runtime_instances(pool).await.ok()?;
    instances
        .iter()
        .flat_map(|instance| instance.queues.iter())
        .find(|snapshot| snapshot.queue == queue)
        .map(|snapshot| snapshot.in_flight)
}

async fn assert_panics_fail_fast(engine: Engine) {
    let db = database(engine).await;
    let pool = db.pool().clone();
    let queue = "poison_panic";
    let client = builder(&pool, engine)
        .queue(queue, queue_config(1, Duration::from_secs(300)))
        .dlq_enabled_by_default(true)
        .register::<PanicJob, _, _>(|args, _ctx| async move {
            if args.n >= 0 {
                panic!("poison payload {}", args.n);
            }
            Ok(awa::JobResult::Completed)
        })
        .register::<HealthyJob, _, _>(|_args, _ctx| async move { Ok(awa::JobResult::Completed) })
        .build()
        .expect("build client");
    let poison = insert(&pool, &PanicJob { n: 7 }, queue).await;
    client.start().await.expect("start client");

    let job = match engine {
        Engine::QueueStorage => {
            let row = dead_lettered(&pool, poison.id).await;
            assert_eq!(row.reason, "max_attempts_exhausted");
            row.job
        }
        Engine::Canonical => failed(&pool, poison.id).await,
    };
    assert_eq!(job.attempt, 2);
    let messages = error_messages(&job);
    assert_eq!(
        messages
            .iter()
            .filter(|m| m.contains("handler panicked: poison payload 7"))
            .count(),
        2,
        "each attempt should record the panic message: {messages:?}"
    );

    // The panicking attempts left nothing registered in flight, and the
    // runtime still runs other work.
    let healthy = insert(&pool, &HealthyJob { n: 1 }, queue).await;
    eventually(EXHAUST_WITHIN, "the healthy job to complete", || async {
        admin::get_job(&pool, healthy.id)
            .await
            .ok()
            .filter(|job| job.state == JobState::Completed)
    })
    .await;
    eventually(
        EXHAUST_WITHIN,
        "in-flight count to return to zero",
        || async { queue_in_flight(&pool, queue).await.filter(|n| *n == 0) },
    )
    .await;

    client.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_storage_handler_panic_fails_fast_and_dead_letters() {
    assert_panics_fail_fast(Engine::QueueStorage).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_handler_panic_fails_fast() {
    assert_panics_fail_fast(Engine::Canonical).await;
}

/// A worker that never heartbeats and never finishes: every attempt is
/// rescued as a dead worker. The hung handlers stay alive here, where a dead
/// process would have released their worker slots by dying, so the queue gets
/// a slot per attempt.
fn crash_looping_client(pool: &sqlx::PgPool, engine: Engine, queue: &str, dlq: bool) -> Client {
    builder(pool, engine)
        .queue(queue, queue_config(4, Duration::from_secs(300)))
        .dlq_enabled_by_default(dlq)
        .register::<HangJob, _, _>(|_args, _ctx| async move {
            std::future::pending::<()>().await;
            Ok(awa::JobResult::Completed)
        })
        .heartbeat_interval(Duration::from_secs(600))
        .heartbeat_staleness(Duration::from_secs(1))
        .build()
        .expect("build client")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_storage_crash_loop_stops_at_max_attempts_and_dead_letters() {
    let db = database(Engine::QueueStorage).await;
    let pool = db.pool().clone();
    let queue = "poison_crash_dlq";
    let client = crash_looping_client(&pool, Engine::QueueStorage, queue, true);
    let job = insert(&pool, &HangJob { n: 1 }, queue).await;
    client.start().await.expect("start client");

    let row = dead_lettered(&pool, job.id).await;
    assert_eq!(row.reason, "heartbeat_stale");
    assert_eq!(row.job.attempt, 2);
    let messages = error_messages(&row.job);
    assert_eq!(
        messages
            .iter()
            .filter(|m| m.contains("presumed dead"))
            .count(),
        2,
        "{messages:?}"
    );
    client.shutdown(Duration::from_millis(200)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_storage_crash_loop_without_dlq_fails() {
    let db = database(Engine::QueueStorage).await;
    let pool = db.pool().clone();
    let queue = "poison_crash_failed";
    let client = crash_looping_client(&pool, Engine::QueueStorage, queue, false);
    let job = insert(&pool, &HangJob { n: 2 }, queue).await;
    client.start().await.expect("start client");

    let job = failed(&pool, job.id).await;
    assert_eq!(job.attempt, 2);
    assert!(dlq::get_dlq_job(&pool, job.id).await.unwrap().is_none());
    client.shutdown(Duration::from_millis(200)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_crash_loop_stops_at_max_attempts() {
    let db = database(Engine::Canonical).await;
    let pool = db.pool().clone();
    let queue = "poison_crash_canonical";
    let client = crash_looping_client(&pool, Engine::Canonical, queue, false);
    let job = insert(&pool, &HangJob { n: 3 }, queue).await;
    client.start().await.expect("start client");

    let job = failed(&pool, job.id).await;
    assert_eq!(job.attempt, 2);
    let messages = error_messages(&job);
    assert_eq!(
        messages
            .iter()
            .filter(|m| m.contains("presumed dead"))
            .count(),
        2,
        "{messages:?}"
    );
    client.shutdown(Duration::from_millis(200)).await;
}

/// A handler that keeps heartbeating but never finishes and ignores
/// cancellation: every attempt is rescued at its deadline. One worker slot, so
/// the retry only runs if the executor gives up the hung attempt's slot.
fn hanging_client(pool: &sqlx::PgPool, engine: Engine, queue: &str) -> Client {
    builder(pool, engine)
        .queue(queue, queue_config(1, Duration::from_secs(1)))
        .dlq_enabled_by_default(true)
        .register::<HangJob, _, _>(|_args, _ctx| async move {
            std::future::pending::<()>().await;
            Ok(awa::JobResult::Completed)
        })
        .heartbeat_interval(Duration::from_millis(200))
        .build()
        .expect("build client")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_storage_deadline_loop_stops_at_max_attempts_and_dead_letters() {
    let db = database(Engine::QueueStorage).await;
    let pool = db.pool().clone();
    let queue = "poison_deadline_dlq";
    let client = hanging_client(&pool, Engine::QueueStorage, queue);
    let job = insert(&pool, &HangJob { n: 4 }, queue).await;
    client.start().await.expect("start client");

    let row = dead_lettered(&pool, job.id).await;
    assert_eq!(row.reason, "deadline_exceeded");
    assert_eq!(row.job.attempt, 2);
    client.shutdown(Duration::from_millis(200)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_deadline_loop_stops_at_max_attempts() {
    let db = database(Engine::Canonical).await;
    let pool = db.pool().clone();
    let queue = "poison_deadline_canonical";
    let client = hanging_client(&pool, Engine::Canonical, queue);
    let job = insert(&pool, &HangJob { n: 5 }, queue).await;
    client.start().await.expect("start client");

    let job = failed(&pool, job.id).await;
    assert_eq!(job.attempt, 2);
    let messages = error_messages(&job);
    assert_eq!(
        messages
            .iter()
            .filter(|m| m.contains("hard deadline exceeded"))
            .count(),
        2,
        "{messages:?}"
    );
    client.shutdown(Duration::from_millis(200)).await;
}
