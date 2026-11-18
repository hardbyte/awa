//! #807 regression: a running job whose unique `states` mask excludes
//! `running` must still finalize a snooze or retry once an identical job has
//! been enqueued and holds the unique claim.
//!
//! The first job is claimed, a duplicate is parked on a queue nothing
//! consumes so it keeps the claim, and the handler then returns `Snooze`,
//! `RetryAfter` or a retryable error. Before the fix the queue-storage
//! finalize failed with `unique conflict` and the attempt stayed running
//! until stale-receipt rescue. The claim holder wins: the attempt is
//! cancelled as a duplicate, with a `Cancelled` lifecycle event and a
//! cancelled-job count.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::{Client, JobArgs, JobError, JobEvent, JobResult, QueueConfig, UniqueOpts};
use awa_model::{admin, insert_with, migrations, AwaError, InsertOpts};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

// Every test flips the cluster-wide storage engine, so they must never run
// concurrently — process-local mutex for in-binary threads plus a Postgres
// advisory lock for process-per-test runners.
static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
const SNOOZE_TEST_LOCK_KEY: i64 = 0x6177_6173_6e7a_7563; // "awasnzuc"

struct SnoozeTestGuard {
    _local: tokio::sync::MutexGuard<'static, ()>,
    _conn: PgConnection,
}

async fn acquire_guard() -> SnoozeTestGuard {
    let local = TEST_MUTEX.get_or_init(|| Mutex::new(())).lock().await;
    let mut conn = PgConnection::connect(&database_url())
        .await
        .expect("Failed to open test lock connection");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(SNOOZE_TEST_LOCK_KEY)
        .execute(&mut conn)
        .await
        .expect("Failed to acquire test advisory lock");
    SnoozeTestGuard {
        _local: local,
        _conn: conn,
    }
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string())
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct UniqueSnoozeJob {
    n: i64,
}

/// `scheduled | available | retryable`: the claim is released while running.
const PENDING_ONLY_STATES: u8 = 0b0001_0011;
/// `PENDING_ONLY_STATES | cancelled`: the duplicate cancellation would
/// claim the key too.
const CANCELLED_CLAIMING_STATES: u8 = 0b0101_0011;
/// Each case, in each run, works its own queues, so one case's deleted rows
/// and rolled-back retries never sit in a lane ahead of another case's job.
fn worked_queue(n: i64) -> String {
    format!("snooze_uc_{}_{n}", std::process::id())
}
/// Nothing consumes this queue, so the duplicate keeps the unique claim.
fn parked_queue(n: i64) -> String {
    format!("snooze_uc_parked_{}_{n}", std::process::id())
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct RetriedFollowUp {
    n: i64,
}

#[derive(Clone, Copy, Debug)]
enum Engine {
    Canonical,
    QueueStorage,
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Snooze,
    RetryAfter,
    RetryableError,
}

async fn clean(pool: &PgPool) {
    for kind in [UniqueSnoozeJob::kind(), RetriedFollowUp::kind()] {
        sqlx::query("DELETE FROM awa.jobs WHERE kind = $1")
            .bind(kind)
            .execute(pool)
            .await
            .expect("job cleanup");
    }
}

async fn follow_up_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE kind = $1")
        .bind(RetriedFollowUp::kind())
        .fetch_one(pool)
        .await
        .expect("follow-up probe")
}

async fn insert_unique(pool: &PgPool, queue: &str, n: i64, states: u8) -> i64 {
    insert_with(
        pool,
        &UniqueSnoozeJob { n },
        InsertOpts {
            queue: queue.into(),
            unique: Some(UniqueOpts {
                by_queue: false,
                by_args: true,
                by_period: None,
                states,
            }),
            ..Default::default()
        },
    )
    .await
    .expect("unique insert")
    .id
}

async fn visible_state(pool: &PgPool, id: i64) -> Option<String> {
    sqlx::query_scalar("SELECT state::text FROM awa.jobs WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("state probe")
}

async fn errors_text(pool: &PgPool, id: i64) -> String {
    sqlx::query_scalar("SELECT COALESCE(errors::text, '') FROM awa.jobs WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("errors probe")
}

fn cancelled_count(metrics: &[ResourceMetrics]) -> u64 {
    let mut total = 0;
    for resource in metrics {
        for scope in resource.scope_metrics() {
            for metric in scope.metrics() {
                if metric.name() != "awa.job.cancelled" {
                    continue;
                }
                if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
                    total += sum
                        .data_points()
                        .filter(|point| {
                            point.attributes().any(|kv| {
                                kv.key.as_str() == "awa.job.kind"
                                    && kv.value.as_str() == UniqueSnoozeJob::kind()
                            })
                        })
                        .map(|point| point.value())
                        .sum::<u64>();
                }
            }
        }
    }
    total
}

fn is_unique_conflict(err: &AwaError) -> bool {
    match err {
        AwaError::UniqueConflict { .. } => true,
        AwaError::Database(sqlx::Error::Database(db_err)) => {
            db_err.code().as_deref() == Some("23505")
        }
        _ => false,
    }
}

/// An operator retry of the cancelled duplicate must not take the key from
/// the newer job, must take it once that job is gone, and restores the
/// job's original unique mask.
async fn assert_retry_rechecks_claim(pool: &PgPool, job_a: i64, job_b: i64) {
    let err = admin::retry(pool, job_a)
        .await
        .expect_err("retry must fail while the newer job holds the unique claim");
    assert!(is_unique_conflict(&err), "unexpected retry error: {err:?}");
    assert_eq!(
        visible_state(pool, job_a).await.as_deref(),
        Some("cancelled")
    );
    assert_eq!(claim_holder(pool, job_b).await, job_b);

    sqlx::query("DELETE FROM awa.jobs WHERE id = $1")
        .bind(job_b)
        .execute(pool)
        .await
        .expect("delete the claim holder");
    let retried = admin::retry(pool, job_a)
        .await
        .expect("retry once the claim is free")
        .expect("retried row");
    assert_eq!(retried.state.to_string(), "available");
    assert_eq!(claim_holder(pool, job_a).await, job_a);

    // The retry restored the original mask, which claims `cancelled`.
    admin::cancel(pool, job_a)
        .await
        .expect("cancel the retried job")
        .expect("cancelled row");
    assert_eq!(
        claim_holder(pool, job_a).await,
        job_a,
        "a cancelled job whose mask claims cancelled keeps its key"
    );
}

async fn claim_holder(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT c.job_id FROM awa.job_unique_claims c JOIN awa.jobs j ON j.unique_key = c.unique_key WHERE j.id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("claim probe")
}

/// Runs job A until its handler returns `outcome` while duplicate B holds
/// the unique claim, waits for A to leave `running`, and checks that A was
/// cancelled as a duplicate and B kept the claim. With `retried_follow_ups`
/// the kind also registers a Retried enqueue spec, which must not fire.
async fn run(
    engine: Engine,
    outcome: Outcome,
    retried_follow_ups: bool,
    retry_after_cancel: bool,
    states: u8,
    n: i64,
    duplicate_error: &str,
) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let _guard = acquire_guard().await;
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&database_url())
        .await
        .expect("Failed to connect — is Postgres running?");
    migrations::run(&pool).await.expect("Failed to migrate");
    match engine {
        Engine::Canonical => awa_testing::setup::reset_to_canonical(&pool).await,
        Engine::QueueStorage => awa_testing::setup::activate_queue_storage(&pool).await,
    }
    clean(&pool).await;

    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let cancelled_events: Arc<StdMutex<Vec<(i64, String)>>> = Arc::default();
    let running = Arc::new(Notify::new());
    let duplicate_inserted = Arc::new(Notify::new());
    let first_attempt = Arc::new(AtomicBool::new(true));

    let builder = Client::builder(pool.clone())
        .queue(worked_queue(n), QueueConfig::default())
        .register::<UniqueSnoozeJob, _, _>({
            let (running, duplicate_inserted, first_attempt) = (
                running.clone(),
                duplicate_inserted.clone(),
                first_attempt.clone(),
            );
            move |_args: UniqueSnoozeJob, _ctx| {
                let (running, duplicate_inserted, first_attempt) = (
                    running.clone(),
                    duplicate_inserted.clone(),
                    first_attempt.clone(),
                );
                async move {
                    if first_attempt.swap(false, Ordering::SeqCst) {
                        running.notify_one();
                        duplicate_inserted.notified().await;
                        let delay = Duration::from_secs(1);
                        return match outcome {
                            Outcome::Snooze => Ok(JobResult::Snooze(delay)),
                            Outcome::RetryAfter => Ok(JobResult::RetryAfter(delay)),
                            Outcome::RetryableError => {
                                Err(JobError::retryable_msg("transient failure"))
                            }
                        };
                    }
                    Ok(JobResult::Completed)
                }
            }
        });
    let builder = builder.on_event::<UniqueSnoozeJob, _, _>({
        let cancelled_events = cancelled_events.clone();
        move |event| {
            let cancelled_events = cancelled_events.clone();
            async move {
                if let JobEvent::Cancelled { job, reason, .. } = event {
                    cancelled_events
                        .lock()
                        .expect("cancelled events")
                        .push((job.id, reason));
                }
            }
        }
    });
    let builder = if retried_follow_ups {
        builder.on_retried_enqueue::<UniqueSnoozeJob, RetriedFollowUp, _>(
            |args, _job, _error, _attempt, _next_run_at| RetriedFollowUp { n: args.n },
        )
    } else {
        builder
    };
    let client = builder.build().expect("client build");

    let job_a = insert_unique(&pool, &worked_queue(n), n, states).await;
    client.start().await.expect("client start");

    tokio::time::timeout(Duration::from_secs(15), running.notified())
        .await
        .expect("job A never started");
    // A is running and, under this mask, no longer holds the unique claim,
    // so an identical insert is accepted and takes it.
    let job_b = insert_unique(&pool, &parked_queue(n), n, states).await;
    assert_ne!(job_a, job_b);
    duplicate_inserted.notify_one();

    // Stale-receipt rescue would take minutes under the default staleness;
    // a finalize that works settles A well inside this window.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let state = loop {
        let state = visible_state(&pool, job_a).await;
        if !matches!(state.as_deref(), None | Some("running")) {
            break state;
        }
        if tokio::time::Instant::now() > deadline {
            break state;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    // The lifecycle event is dispatched after the finalize commits.
    let event_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while cancelled_events
        .lock()
        .expect("cancelled events")
        .is_empty()
        && tokio::time::Instant::now() < event_deadline
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.shutdown(Duration::from_secs(5)).await;
    meter_provider.force_flush().expect("flush metrics");
    let metrics = exporter.get_finished_metrics().expect("read metrics");

    assert_eq!(
        state.as_deref(),
        Some("cancelled"),
        "{engine:?} {outcome:?} with a duplicate holding the unique claim was not finalized as a duplicate cancellation"
    );
    let errors = errors_text(&pool, job_a).await;
    assert!(
        errors.contains(duplicate_error),
        "cancelled job should record why: {errors}"
    );
    if matches!(outcome, Outcome::RetryableError) {
        assert!(
            errors.contains("transient failure"),
            "cancelled job should keep the handler error: {errors}"
        );
    }
    assert_eq!(
        visible_state(&pool, job_b).await.as_deref(),
        Some("available")
    );
    assert_eq!(
        claim_holder(&pool, job_b).await,
        job_b,
        "duplicate keeps the unique claim"
    );
    assert_eq!(
        follow_up_count(&pool).await,
        0,
        "a duplicate cancellation is not a retry"
    );
    let events = cancelled_events.lock().expect("cancelled events").clone();
    assert_eq!(
        events.len(),
        1,
        "one Cancelled event for the duplicate cancellation: {events:?}"
    );
    assert_eq!(
        events[0].0, job_a,
        "the Cancelled event names the cancelled job"
    );
    assert!(
        events[0].1.contains(duplicate_error),
        "Cancelled event reason should name the duplicate: {events:?}"
    );
    assert_eq!(
        cancelled_count(&metrics),
        1,
        "awa.job.cancelled counts the duplicate cancellation"
    );
    if retry_after_cancel {
        assert_retry_rechecks_claim(&pool, job_a, job_b).await;
    }

    clean(&pool).await;
    awa_testing::setup::reset_runtime_backend(&pool).await;
}

#[tokio::test]
async fn snooze_with_duplicate_holding_unique_claim_is_finalized() {
    run(
        Engine::QueueStorage,
        Outcome::Snooze,
        false,
        false,
        PENDING_ONLY_STATES,
        1,
        "snoozed as duplicate",
    )
    .await;
}

#[tokio::test]
async fn retry_after_with_duplicate_holding_unique_claim_is_finalized() {
    run(
        Engine::QueueStorage,
        Outcome::RetryAfter,
        false,
        false,
        PENDING_ONLY_STATES,
        2,
        "retried as duplicate",
    )
    .await;
}

#[tokio::test]
async fn retryable_error_with_duplicate_holding_unique_claim_is_finalized() {
    run(
        Engine::QueueStorage,
        Outcome::RetryableError,
        false,
        false,
        PENDING_ONLY_STATES,
        3,
        "retried as duplicate",
    )
    .await;
}

#[tokio::test]
async fn retry_after_with_follow_ups_and_duplicate_holding_unique_claim_is_finalized() {
    run(
        Engine::QueueStorage,
        Outcome::RetryAfter,
        true,
        false,
        PENDING_ONLY_STATES,
        4,
        "retried as duplicate",
    )
    .await;
}

#[tokio::test]
async fn retryable_error_with_follow_ups_and_duplicate_holding_unique_claim_is_finalized() {
    run(
        Engine::QueueStorage,
        Outcome::RetryableError,
        true,
        false,
        PENDING_ONLY_STATES,
        5,
        "retried as duplicate",
    )
    .await;
}

#[tokio::test]
async fn canonical_snooze_with_duplicate_holding_unique_claim_is_finalized() {
    run(
        Engine::Canonical,
        Outcome::Snooze,
        false,
        false,
        PENDING_ONLY_STATES,
        6,
        "snoozed as duplicate",
    )
    .await;
}

#[tokio::test]
async fn canonical_retry_after_with_duplicate_holding_unique_claim_is_finalized() {
    run(
        Engine::Canonical,
        Outcome::RetryAfter,
        false,
        false,
        PENDING_ONLY_STATES,
        7,
        "retried as duplicate",
    )
    .await;
}

#[tokio::test]
async fn canonical_retryable_error_with_duplicate_holding_unique_claim_is_finalized() {
    run(
        Engine::Canonical,
        Outcome::RetryableError,
        false,
        false,
        PENDING_ONLY_STATES,
        8,
        "retried as duplicate",
    )
    .await;
}

#[tokio::test]
async fn canonical_retry_after_with_follow_ups_and_duplicate_holding_unique_claim_is_finalized() {
    run(
        Engine::Canonical,
        Outcome::RetryAfter,
        true,
        false,
        PENDING_ONLY_STATES,
        9,
        "retried as duplicate",
    )
    .await;
}

#[tokio::test]
async fn snooze_with_cancelled_claiming_mask_is_finalized() {
    run(
        Engine::QueueStorage,
        Outcome::Snooze,
        false,
        false,
        CANCELLED_CLAIMING_STATES,
        10,
        "snoozed as duplicate",
    )
    .await;
}

#[tokio::test]
async fn retryable_error_with_cancelled_claiming_mask_is_finalized() {
    run(
        Engine::QueueStorage,
        Outcome::RetryableError,
        false,
        false,
        CANCELLED_CLAIMING_STATES,
        11,
        "retried as duplicate",
    )
    .await;
}

#[tokio::test]
async fn canonical_snooze_with_cancelled_claiming_mask_is_finalized() {
    run(
        Engine::Canonical,
        Outcome::Snooze,
        false,
        false,
        CANCELLED_CLAIMING_STATES,
        12,
        "snoozed as duplicate",
    )
    .await;
}

#[tokio::test]
async fn canonical_retryable_error_with_cancelled_claiming_mask_is_finalized() {
    run(
        Engine::Canonical,
        Outcome::RetryableError,
        false,
        false,
        CANCELLED_CLAIMING_STATES,
        13,
        "retried as duplicate",
    )
    .await;
}

#[tokio::test]
async fn retry_of_snooze_duplicate_cancellation_rechecks_claim() {
    run(
        Engine::QueueStorage,
        Outcome::Snooze,
        false,
        true,
        CANCELLED_CLAIMING_STATES,
        14,
        "snoozed as duplicate",
    )
    .await;
}

#[tokio::test]
async fn canonical_retry_of_snooze_duplicate_cancellation_rechecks_claim() {
    run(
        Engine::Canonical,
        Outcome::Snooze,
        false,
        true,
        CANCELLED_CLAIMING_STATES,
        15,
        "snoozed as duplicate",
    )
    .await;
}
