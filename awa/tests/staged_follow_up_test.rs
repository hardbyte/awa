//! ADR-046 integration tests: follow-up jobs staged by a handler and inserted
//! in the attempt's completion transaction.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::admin;
use awa::{
    Client, EnqueueRequest, JobArgs, JobError, JobResult, JobState, QueueConfig, StageError,
    UniqueOpts,
};
use awa_testing::setup::TestDatabase;
use serde::{Deserialize, Serialize};
use std::time::Duration;

async fn wait_for_state(pool: &sqlx::PgPool, job_id: i64, state: JobState) -> awa::JobRow {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(job) = admin::get_job(pool, job_id).await {
            if job.state == state {
                return job;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            let current = admin::get_job(pool, job_id).await.map(|job| job.state);
            panic!("timed out waiting for job {job_id} to reach {state:?}; now {current:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn count_kind(pool: &sqlx::PgPool, kind: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM awa.jobs WHERE kind = $1")
        .bind(kind)
        .fetch_one(pool)
        .await
        .unwrap()
}

fn queue_config() -> QueueConfig {
    QueueConfig {
        poll_interval: Duration::from_millis(25),
        ..Default::default()
    }
}

fn client_builder(pool: &sqlx::PgPool, canonical: bool) -> awa::ClientBuilder {
    let builder = Client::builder(pool.clone());
    if canonical {
        builder.canonical_storage()
    } else {
        builder
    }
}

async fn insert_in(pool: &sqlx::PgPool, args: &impl JobArgs, queue: &str) -> i64 {
    awa::insert_with(
        pool,
        args,
        awa::InsertOpts {
            queue: queue.to_string(),
            max_attempts: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .id
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Resolve {
    source: i64,
    item_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Process {
    item_id: i64,
}

// ── commit with completion, deduplicated by the follow-up's own key ──────

async fn staged_follow_ups_commit_and_deduplicate(db: TestDatabase, canonical: bool) {
    let pool = db.pool().clone();
    let queue = if canonical {
        "staged_dedup"
    } else {
        "staged_dedup_qs"
    };
    let client = client_builder(&pool, canonical)
        .queue(queue, queue_config())
        .register::<Resolve, _, _>(|args, ctx| {
            let follow_ups = ctx.follow_ups();
            async move {
                // The value comes from the handler, not from a registered spec.
                let item_id = args.item_id * 10;
                // The same key twice in one attempt collapses to one row too.
                for _ in 0..2 {
                    follow_ups.enqueue_on_completion(
                        EnqueueRequest::new(Process { item_id })
                            .unique(UniqueOpts::default())
                            .metadata(serde_json::json!({ "source": args.source })),
                    )?;
                }
                Ok(JobResult::Completed)
            }
        })
        .build()
        .unwrap();

    // Three sources resolve to two distinct items.
    let mut triggers = Vec::new();
    for (source, item_id) in [(1, 7), (2, 7), (3, 8)] {
        triggers.push(insert_in(&pool, &Resolve { source, item_id }, queue).await);
    }
    client.start().await.unwrap();
    for id in &triggers {
        wait_for_state(&pool, *id, JobState::Completed).await;
    }
    let items: Vec<i64> = sqlx::query_scalar(
        "SELECT (args->>'item_id')::bigint FROM awa.jobs WHERE kind = $1 ORDER BY 1",
    )
    .bind(Process::kind())
    .fetch_all(&pool)
    .await
    .unwrap();
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(items, vec![70, 80], "one follow-up per distinct item");
}

#[tokio::test]
async fn staged_follow_ups_commit_and_deduplicate_on_canonical() {
    staged_follow_ups_commit_and_deduplicate(TestDatabase::canonical().await, true).await;
}

#[tokio::test]
async fn staged_follow_ups_commit_and_deduplicate_under_queue_storage() {
    staged_follow_ups_commit_and_deduplicate(TestDatabase::queue_storage().await, false).await;
}

// ── discarded on every outcome other than Completed ──────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Outcome {
    mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct NeverInserted {
    mode: String,
}

struct OutcomeWorker;

#[async_trait::async_trait]
impl awa::Worker for OutcomeWorker {
    fn kind(&self) -> &'static str {
        Outcome::kind()
    }

    async fn perform(&self, ctx: &awa::JobContext) -> Result<JobResult, JobError> {
        let args: Outcome = serde_json::from_value(ctx.job.args.clone())
            .map_err(|err| JobError::terminal(err.to_string()))?;
        ctx.enqueue_on_completion(NeverInserted {
            mode: args.mode.clone(),
        })?;
        match args.mode.as_str() {
            "retry" => Ok(JobResult::RetryAfter(Duration::from_millis(1))),
            "snooze" => Ok(JobResult::Snooze(Duration::from_secs(3600))),
            "cancel" => Ok(JobResult::Cancel("stop".into())),
            "error" => Err(JobError::retryable_msg("boom")),
            "terminal" => Err(JobError::terminal("fatal")),
            "panic" => panic!("handler panicked after staging"),
            "callback" => {
                let guard = ctx
                    .register_callback(Duration::from_secs(3600))
                    .await
                    .map_err(JobError::retryable)?;
                Ok(JobResult::WaitForCallback(guard))
            }
            other => Err(JobError::terminal(format!("unknown mode {other}"))),
        }
    }
}

async fn staged_follow_ups_are_discarded_on_other_outcomes(db: TestDatabase, canonical: bool) {
    let pool = db.pool().clone();
    let queue = if canonical {
        "staged_discard"
    } else {
        "staged_discard_qs"
    };
    let client = client_builder(&pool, canonical)
        .queue(queue, queue_config())
        .register_worker(OutcomeWorker)
        .build()
        .unwrap();

    let expected = [
        ("retry", JobState::Failed),
        ("snooze", JobState::Scheduled),
        ("cancel", JobState::Cancelled),
        ("error", JobState::Failed),
        ("terminal", JobState::Failed),
        ("panic", JobState::Failed),
        ("callback", JobState::WaitingExternal),
    ];
    let mut jobs = Vec::new();
    for (mode, state) in expected {
        let id = insert_in(&pool, &Outcome { mode: mode.into() }, queue).await;
        jobs.push((id, state));
    }
    client.start().await.unwrap();
    for (id, state) in jobs {
        wait_for_state(&pool, id, state).await;
    }
    // Give any stray insert a moment to show up before asserting absence.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let inserted = count_kind(&pool, NeverInserted::kind()).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(
        inserted, 0,
        "only a Completed attempt inserts its staged follow-ups"
    );
}

#[tokio::test]
async fn staged_follow_ups_are_discarded_on_other_outcomes_on_canonical() {
    staged_follow_ups_are_discarded_on_other_outcomes(TestDatabase::canonical().await, true).await;
}

#[tokio::test]
async fn staged_follow_ups_are_discarded_on_other_outcomes_under_queue_storage() {
    staged_follow_ups_are_discarded_on_other_outcomes(TestDatabase::queue_storage().await, false)
        .await;
}

// ── a failed stage poisons the attempt; staging closes when the handler returns ──

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Poisoned {
    id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Oversized {
    pad: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Late {
    id: i64,
}

async fn failed_or_late_staging_never_commits(db: TestDatabase, canonical: bool) {
    let pool = db.pool().clone();
    let queue = if canonical {
        "staged_poison"
    } else {
        "staged_poison_qs"
    };
    let (late_tx, mut late_rx) = tokio::sync::mpsc::unbounded_channel();
    let client = client_builder(&pool, canonical)
        .queue(queue, queue_config())
        .register::<Poisoned, _, _>(|_args, ctx| {
            let follow_ups = ctx.follow_ups();
            async move {
                // Swallow the error and claim success: the attempt must fail
                // rather than complete without the follow-up it asked for.
                let _ = follow_ups.enqueue_on_completion(Oversized {
                    pad: "x".repeat(awa::MAX_STAGED_BYTES + 1),
                });
                Ok(JobResult::Completed)
            }
        })
        .register::<Late, _, _>(move |args, ctx| {
            let follow_ups = ctx.follow_ups();
            let late_tx = late_tx.clone();
            async move {
                // Stage from a task that outlives the handler.
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    let _ = late_tx.send(follow_ups.enqueue_on_completion(Late { id: args.id }));
                });
                Ok(JobResult::Completed)
            }
        })
        .build()
        .unwrap();

    let poisoned = insert_in(&pool, &Poisoned { id: 1 }, queue).await;
    let late = insert_in(&pool, &Late { id: 1 }, queue).await;
    client.start().await.unwrap();

    let failed = wait_for_state(&pool, poisoned, JobState::Failed).await;
    let error = failed
        .errors
        .as_ref()
        .and_then(|errors| errors.last())
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(
        error.contains("failed to stage"),
        "the staging error must be recorded, got {error}"
    );

    wait_for_state(&pool, late, JobState::Completed).await;
    let late_result = tokio::time::timeout(Duration::from_secs(5), late_rx.recv())
        .await
        .expect("late staging attempt")
        .expect("late staging result");
    assert_eq!(late_result, Err(StageError::Sealed));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(count_kind(&pool, Oversized::kind()).await, 0);
    assert_eq!(
        count_kind(&pool, Late::kind()).await,
        1,
        "only the trigger itself"
    );
    client.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn failed_or_late_staging_never_commits_on_canonical() {
    failed_or_late_staging_never_commits(TestDatabase::canonical().await, true).await;
}

#[tokio::test]
async fn failed_or_late_staging_never_commits_under_queue_storage() {
    failed_or_late_staging_never_commits(TestDatabase::queue_storage().await, false).await;
}

// ── staged follow-ups and registered specs share the completion ───────────

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Both {
    id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct FromSpec {
    id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct FromHandler {
    id: i64,
}

async fn staged_and_registered_follow_ups_commit_together(db: TestDatabase, canonical: bool) {
    let pool = db.pool().clone();
    let queue = if canonical {
        "staged_both"
    } else {
        "staged_both_qs"
    };
    let client = client_builder(&pool, canonical)
        .queue(queue, queue_config())
        .register::<Both, _, _>(|args, ctx| {
            let follow_ups = ctx.follow_ups();
            async move {
                for offset in 0..3 {
                    follow_ups.enqueue_on_completion(FromHandler {
                        id: args.id * 100 + offset,
                    })?;
                }
                Ok(JobResult::Completed)
            }
        })
        .on_completed_enqueue::<Both, FromSpec, _>(|args, _job| FromSpec { id: args.id })
        .build()
        .unwrap();

    let id = insert_in(&pool, &Both { id: 4 }, queue).await;
    client.start().await.unwrap();
    wait_for_state(&pool, id, JobState::Completed).await;
    let kinds: Vec<String> =
        sqlx::query_scalar("SELECT kind FROM awa.jobs WHERE kind IN ($1, $2) ORDER BY id")
            .bind(FromSpec::kind())
            .bind(FromHandler::kind())
            .fetch_all(&pool)
            .await
            .unwrap();
    client.shutdown(Duration::from_secs(2)).await;

    // Registered specs insert first, then the staged follow-ups in order.
    assert_eq!(
        kinds,
        vec![
            FromSpec::kind(),
            FromHandler::kind(),
            FromHandler::kind(),
            FromHandler::kind()
        ],
        "fan-out stages one follow-up per call, after the registered spec"
    );
}

#[tokio::test]
async fn staged_and_registered_follow_ups_commit_together_on_canonical() {
    staged_and_registered_follow_ups_commit_together(TestDatabase::canonical().await, true).await;
}

#[tokio::test]
async fn staged_and_registered_follow_ups_commit_together_under_queue_storage() {
    staged_and_registered_follow_ups_commit_together(TestDatabase::queue_storage().await, false)
        .await;
}

// ── what the insert would reject fails the stage, not the completion ────

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct BadFollowUp {
    id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Rejected {
    id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Exhausted {
    id: i64,
}

async fn invalid_follow_up_fails_the_stage(db: TestDatabase, canonical: bool) {
    let pool = db.pool().clone();
    let queue = if canonical {
        "staged_invalid"
    } else {
        "staged_invalid_qs"
    };
    let client = client_builder(&pool, canonical)
        .queue(queue, queue_config())
        .register::<BadFollowUp, _, _>(|args, ctx| {
            let follow_ups = ctx.follow_ups();
            async move {
                // Priority outside 1..=4 would fail the insert's check
                // constraint, so staging rejects it and poisons the attempt.
                let _ = follow_ups.enqueue_on_completion(
                    EnqueueRequest::new(Rejected { id: args.id }).priority(9),
                );
                Ok(JobResult::Completed)
            }
        })
        // A poisoned attempt fails terminally, so exhausted specs fire.
        .on_exhausted_enqueue::<BadFollowUp, Exhausted, _>(|args, _job, _error, _attempt| {
            Exhausted { id: args.id }
        })
        .build()
        .unwrap();

    let id = insert_in(&pool, &BadFollowUp { id: 1 }, queue).await;
    client.start().await.unwrap();
    let failed = wait_for_state(&pool, id, JobState::Failed).await;
    let exhausted = count_kind(&pool, Exhausted::kind()).await;
    let rejected = count_kind(&pool, Rejected::kind()).await;
    client.shutdown(Duration::from_secs(2)).await;

    let error = failed
        .errors
        .as_ref()
        .and_then(|errors| errors.last())
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(error.contains("priority 9"), "got {error}");
    assert_eq!(
        exhausted, 1,
        "the exhausted spec fires for a poisoned attempt"
    );
    assert_eq!(rejected, 0);
}

#[tokio::test]
async fn invalid_follow_up_fails_the_stage_on_canonical() {
    invalid_follow_up_fails_the_stage(TestDatabase::canonical().await, true).await;
}

#[tokio::test]
async fn invalid_follow_up_fails_the_stage_under_queue_storage() {
    invalid_follow_up_fails_the_stage(TestDatabase::queue_storage().await, false).await;
}

/// An insert error staging cannot foresee rolls the completion back with
/// the follow-up. A test-only trigger stands in for it; canonical inserts
/// land in `awa.jobs_hot`, so the trigger goes there.
#[tokio::test]
async fn unforeseen_insert_error_rolls_the_completion_back_on_canonical() {
    let db = TestDatabase::canonical().await;
    let pool = db.pool().clone();
    let queue = "staged_rollback";
    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION public.reject_staged() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.kind = 'rejected' THEN
                RAISE EXCEPTION 'rejected by test trigger';
            END IF;
            RETURN NEW;
        END $$
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_staged BEFORE INSERT ON awa.jobs_hot \
         FOR EACH ROW EXECUTE FUNCTION public.reject_staged()",
    )
    .execute(&pool)
    .await
    .unwrap();

    let client = client_builder(&pool, true)
        .queue(queue, queue_config())
        .register::<BadFollowUp, _, _>(|args, ctx| {
            let follow_ups = ctx.follow_ups();
            async move {
                follow_ups.enqueue_on_completion(Rejected { id: args.id })?;
                Ok(JobResult::Completed)
            }
        })
        .build()
        .unwrap();

    let id = insert_in(&pool, &BadFollowUp { id: 1 }, queue).await;
    client.start().await.unwrap();
    wait_for_state(&pool, id, JobState::Running).await;
    // The completion keeps failing; give it several finalize attempts.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let job = admin::get_job(&pool, id).await.unwrap();
    let rejected = count_kind(&pool, Rejected::kind()).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(job.state, JobState::Running, "the completion rolled back");
    assert_eq!(job.attempt, 1);
    assert_eq!(rejected, 0);
}

// ── a callback wait keeps the buffer; an external completion discards it ──

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct AwaitsCallback {
    id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct AfterCallback {
    id: i64,
}

struct AwaitsCallbackWorker;

#[async_trait::async_trait]
impl awa::Worker for AwaitsCallbackWorker {
    fn kind(&self) -> &'static str {
        AwaitsCallback::kind()
    }

    async fn perform(&self, ctx: &awa::JobContext) -> Result<JobResult, JobError> {
        let args: AwaitsCallback = serde_json::from_value(ctx.job.args.clone())
            .map_err(|err| JobError::terminal(err.to_string()))?;
        // Stage before waiting: only the handler's own completion inserts it.
        ctx.enqueue_on_completion(AfterCallback { id: args.id })?;
        let guard = ctx
            .register_callback(Duration::from_secs(3600))
            .await
            .map_err(JobError::retryable)?;
        ctx.wait_for_callback(guard)
            .await
            .map_err(JobError::retryable)?;
        Ok(JobResult::Completed)
    }
}

async fn callback_wait_keeps_staged_follow_ups(db: TestDatabase, canonical: bool) {
    let pool = db.pool().clone();
    let queue = if canonical {
        "staged_callback"
    } else {
        "staged_callback_qs"
    };
    let client = client_builder(&pool, canonical)
        .queue(queue, queue_config())
        .register_worker(AwaitsCallbackWorker)
        .build()
        .unwrap();

    let resumed = insert_in(&pool, &AwaitsCallback { id: 1 }, queue).await;
    let completed_externally = insert_in(&pool, &AwaitsCallback { id: 2 }, queue).await;
    client.start().await.unwrap();

    let waiting = wait_for_state(&pool, resumed, JobState::WaitingExternal).await;
    admin::resume_external(&pool, waiting.callback_id.unwrap(), None, None)
        .await
        .unwrap();
    wait_for_state(&pool, resumed, JobState::Completed).await;

    let waiting = wait_for_state(&pool, completed_externally, JobState::WaitingExternal).await;
    client
        .complete_external(waiting.callback_id.unwrap(), None, None)
        .await
        .unwrap();
    wait_for_state(&pool, completed_externally, JobState::Completed).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let ids: Vec<i64> =
        sqlx::query_scalar("SELECT (args->>'id')::bigint FROM awa.jobs WHERE kind = $1 ORDER BY 1")
            .bind(AfterCallback::kind())
            .fetch_all(&pool)
            .await
            .unwrap();
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(
        ids,
        vec![1],
        "resumed and completed by its handler inserts; completed externally does not"
    );
}

#[tokio::test]
async fn callback_wait_keeps_staged_follow_ups_on_canonical() {
    callback_wait_keeps_staged_follow_ups(TestDatabase::canonical().await, true).await;
}

#[tokio::test]
async fn callback_wait_keeps_staged_follow_ups_under_queue_storage() {
    callback_wait_keeps_staged_follow_ups(TestDatabase::queue_storage().await, false).await;
}

// ── the test harness inserts staged follow-ups like the runtime ───────────

struct HarnessWorker;

#[async_trait::async_trait]
impl awa::Worker for HarnessWorker {
    fn kind(&self) -> &'static str {
        Resolve::kind()
    }

    async fn perform(&self, ctx: &awa::JobContext) -> Result<JobResult, JobError> {
        let args: Resolve = serde_json::from_value(ctx.job.args.clone())
            .map_err(|err| JobError::terminal(err.to_string()))?;
        // On its own queue: the queue-storage harness claims whatever is
        // next on a queue, whatever its kind.
        ctx.enqueue_on_completion(
            EnqueueRequest::new(Process {
                item_id: args.item_id,
            })
            .queue("staged_harness_process")
            .unique(UniqueOpts::default()),
        )?;
        Ok(JobResult::Completed)
    }
}

async fn test_client_inserts_staged_follow_ups(db: TestDatabase) {
    let pool = db.pool().clone();
    let harness = awa_testing::TestClient::from_pool(pool.clone()).await;
    for source in [1, 2] {
        awa::insert(&pool, &Resolve { source, item_id: 5 })
            .await
            .unwrap();
        let result = harness.work_one(&HarnessWorker).await.unwrap();
        assert!(result.is_completed(), "source {source}: {result:?}");
    }
    assert_eq!(count_kind(&pool, Process::kind()).await, 1);
}

#[tokio::test]
async fn test_client_inserts_staged_follow_ups_on_canonical() {
    test_client_inserts_staged_follow_ups(TestDatabase::canonical().await).await;
}

#[tokio::test]
async fn test_client_inserts_staged_follow_ups_under_queue_storage() {
    test_client_inserts_staged_follow_ups(TestDatabase::queue_storage().await).await;
}

/// Cancels its own job mid-handler, then stages and completes anyway.
struct CancelledMidHandler {
    pool: sqlx::PgPool,
}

#[async_trait::async_trait]
impl awa::Worker for CancelledMidHandler {
    fn kind(&self) -> &'static str {
        Resolve::kind()
    }

    async fn perform(&self, ctx: &awa::JobContext) -> Result<JobResult, JobError> {
        admin::cancel(&self.pool, ctx.job.id)
            .await
            .map_err(JobError::retryable)?;
        ctx.enqueue_on_completion(
            EnqueueRequest::new(Process { item_id: 9 }).queue("staged_harness_process"),
        )?;
        Ok(JobResult::Completed)
    }
}

async fn test_client_drops_staged_follow_ups_of_a_lost_attempt(db: TestDatabase) {
    let pool = db.pool().clone();
    let harness = awa_testing::TestClient::from_pool(pool.clone()).await;
    let job = awa::insert(
        &pool,
        &Resolve {
            source: 1,
            item_id: 9,
        },
    )
    .await
    .unwrap();
    let result = harness
        .work_one(&CancelledMidHandler { pool: pool.clone() })
        .await;
    assert!(result.is_err(), "the completion lost its lease: {result:?}");
    assert_eq!(count_kind(&pool, Process::kind()).await, 0);
    let job = admin::get_job(&pool, job.id).await.unwrap();
    assert_eq!(job.state, JobState::Cancelled);
}

#[tokio::test]
async fn test_client_drops_staged_follow_ups_of_a_lost_attempt_on_canonical() {
    test_client_drops_staged_follow_ups_of_a_lost_attempt(TestDatabase::canonical().await).await;
}

#[tokio::test]
async fn test_client_drops_staged_follow_ups_of_a_lost_attempt_under_queue_storage() {
    test_client_drops_staged_follow_ups_of_a_lost_attempt(TestDatabase::queue_storage().await)
        .await;
}
