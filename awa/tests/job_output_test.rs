//! ADR-048 integration tests: an output recorded by a handler is published
//! by the attempt's own transition, as the job's output on completion or in
//! the latest error entry of a failure.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

use awa::model::admin;
use awa::{
    Client, EnqueueRequest, JobArgs, JobError, JobEvent, JobResult, JobState, OutputError,
    QueueConfig, MAX_OUTPUT_BYTES,
};
use awa_testing::setup::TestDatabase;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
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

fn queue_config() -> QueueConfig {
    QueueConfig {
        poll_interval: Duration::from_millis(25),
        ..Default::default()
    }
}

async fn insert(
    pool: &sqlx::PgPool,
    args: &impl JobArgs,
    queue: &str,
    max_attempts: i16,
    tags: &[&str],
) -> i64 {
    awa::insert_with(
        pool,
        args,
        awa::InsertOpts {
            queue: queue.to_string(),
            max_attempts,
            tags: tags.iter().map(|tag| tag.to_string()).collect(),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .id
}

fn outputs_in(errors: &Option<Vec<Value>>) -> Vec<Value> {
    errors
        .iter()
        .flatten()
        .filter_map(|entry| entry.get("output").cloned())
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Summarize {
    n: i64,
}

// ── completion stores the output, on the compact and the general path ──

/// Untagged jobs complete on the compact receipt path, tagged jobs on the
/// general path; both read the output back through `get_job`.
#[tokio::test]
async fn completion_stores_the_output_on_both_terminal_paths() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "output_paths";
    let events: Arc<Mutex<Vec<Option<Value>>>> = Arc::default();
    let seen = events.clone();
    let client = Client::builder(pool.clone())
        .queue(queue, queue_config())
        .register::<Summarize, _, _>(|args, ctx| {
            let output = ctx.output_slot();
            async move {
                output
                    .set(&json!({ "n": args.n, "doubled": args.n * 2 }))
                    .unwrap();
                Ok(JobResult::Completed)
            }
        })
        .on_event::<Summarize, _, _>(move |event| {
            let seen = seen.clone();
            async move {
                if let JobEvent::Completed { job, .. } = event {
                    seen.lock().unwrap().push(job.output);
                }
            }
        })
        .build()
        .unwrap();

    // One at a time: a flush that holds any general-path job completes the
    // whole flush on the general path.
    let compact = insert(&pool, &Summarize { n: 1 }, queue, 1, &[]).await;
    client.start().await.unwrap();
    let compact_row = wait_for_state(&pool, compact, JobState::Completed).await;
    let general = insert(&pool, &Summarize { n: 2 }, queue, 1, &["tagged"]).await;
    let general_row = wait_for_state(&pool, general, JobState::Completed).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(compact_row.output, Some(json!({"n": 1, "doubled": 2})));
    assert_eq!(general_row.output, Some(json!({"n": 2, "doubled": 4})));

    let compact_batches: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.receipt_completion_batches WHERE outputs IS NOT NULL AND $1 = ANY(job_ids)",
    )
    .bind(compact)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        compact_batches, 1,
        "the untagged job stays on the compact path"
    );
    let general_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.done_entries WHERE job_id = $1 AND output IS NOT NULL",
    )
    .bind(general)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(general_rows, 1, "the tagged job takes the general path");

    // Lifecycle events carry the sealed output.
    let mut seen = events.lock().unwrap().clone();
    seen.sort_by_key(|output| output.as_ref().and_then(|o| o["n"].as_i64()));
    assert_eq!(
        seen,
        vec![
            Some(json!({"n": 1, "doubled": 2})),
            Some(json!({"n": 2, "doubled": 4}))
        ]
    );

    // List reads leave outputs out.
    let listed = admin::list_jobs(
        &pool,
        &admin::ListJobsFilter {
            queue: Some(queue.to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|job| job.output.is_none()));
}

// ── failures keep the output in the latest error entry ──────────────────

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Flaky {
    tag: String,
}

/// Attempt 1 returns `RetryAfter`, attempt 2 fails retryably, and attempt 3
/// records nothing and fails terminally. Only the latest entry that recorded
/// an output keeps it, and the dispatch row never carries failed outputs.
#[tokio::test]
async fn failures_keep_only_the_latest_output_and_dispatch_strips_it() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "output_failures";
    let dispatch_errors: Arc<Mutex<Vec<Vec<Value>>>> = Arc::default();
    let seen = dispatch_errors.clone();
    let client = Client::builder(pool.clone())
        .queue(queue, queue_config())
        .register::<Flaky, _, _>(move |_args, ctx| {
            let attempt = ctx.job.attempt;
            seen.lock()
                .unwrap()
                .push(ctx.job.errors.clone().unwrap_or_default());
            let output = ctx.output_slot();
            async move {
                match attempt {
                    1 => {
                        output.set(&json!({ "attempt": 1 })).unwrap();
                        Ok(JobResult::RetryAfter(Duration::from_millis(10)))
                    }
                    2 => {
                        output.set(&json!({ "attempt": 2 })).unwrap();
                        Err(JobError::retryable_msg("second attempt failed"))
                    }
                    _ => Err(JobError::Terminal("third attempt failed".into())),
                }
            }
        })
        .build()
        .unwrap();

    let id = insert(&pool, &Flaky { tag: "a".into() }, queue, 3, &[]).await;
    // The retryable failure backs off about four seconds before attempt 3.
    client.start().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let failed = loop {
        let job = admin::get_job(&pool, id).await.unwrap();
        if job.state.is_terminal() {
            break job;
        }
        assert!(tokio::time::Instant::now() < deadline, "job never failed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(failed.output, None, "a failed job has no job output");
    let errors = failed.errors.clone().unwrap();
    assert_eq!(errors.len(), 3, "{errors:?}");
    assert_eq!(
        outputs_in(&failed.errors),
        vec![json!({"attempt": 2})],
        "only the latest entry that recorded an output keeps it: {errors:?}"
    );
    assert!(errors[0]["error"]
        .as_str()
        .unwrap()
        .starts_with("retry requested after"));
    assert!(errors[1].get("output").is_some());
    assert!(errors[2].get("output").is_none());

    let dispatch_errors = dispatch_errors.lock().unwrap();
    assert_eq!(dispatch_errors.len(), 3);
    assert!(
        dispatch_errors
            .iter()
            .flatten()
            .all(|entry| entry.get("output").is_none()),
        "the dispatch row strips failed outputs: {dispatch_errors:?}"
    );
    assert_eq!(
        dispatch_errors[2].len(),
        2,
        "the third attempt saw two entries"
    );
}

// ── snooze discards; a failed set clears and does not fail the attempt ──

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Poll {
    tag: String,
}

#[tokio::test]
async fn snooze_discards_and_a_failed_set_clears_without_failing() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "output_snooze";
    let errors: Arc<Mutex<Vec<OutputError>>> = Arc::default();
    let recorded = errors.clone();
    let client = Client::builder(pool.clone())
        .queue(queue, queue_config())
        .register::<Poll, _, _>(move |_args, ctx| {
            let attempt = ctx.job.attempt;
            let output = ctx.output_slot();
            let recorded = recorded.clone();
            async move {
                if attempt == 1 && ctx_snoozes_once() {
                    output.set(&json!("not yet")).unwrap();
                    return Ok(JobResult::Snooze(Duration::from_millis(10)));
                }
                output.set(&json!("started")).unwrap();
                let too_large = "x".repeat(MAX_OUTPUT_BYTES);
                if let Err(err) = output.set(&too_large) {
                    recorded.lock().unwrap().push(err);
                }
                Ok(JobResult::Completed)
            }
        })
        .build()
        .unwrap();

    let id = insert(&pool, &Poll { tag: "p".into() }, queue, 1, &[]).await;
    client.start().await.unwrap();
    let job = wait_for_state(&pool, id, JobState::Completed).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(job.output, None, "the failed set cleared the slot");
    assert!(
        job.errors.unwrap_or_default().is_empty(),
        "snooze wrote no entry"
    );
    let errors = errors.lock().unwrap();
    assert!(
        matches!(errors.as_slice(), [OutputError::TooLarge { .. }]),
        "{errors:?}"
    );
}

/// The first claim of the `Poll` job snoozes; later claims complete.
fn ctx_snoozes_once() -> bool {
    use std::sync::atomic::{AtomicBool, Ordering};
    static SNOOZED: AtomicBool = AtomicBool::new(false);
    !SNOOZED.swap(true, Ordering::SeqCst)
}

// ── canonical keeps none ─────────────────────────────────────────────────

#[tokio::test]
async fn canonical_discards_outputs() {
    let db = TestDatabase::canonical().await;
    let pool = db.pool().clone();
    let queue = "output_canonical";
    let client = Client::builder(pool.clone())
        .canonical_storage()
        .queue(queue, queue_config())
        .register::<Summarize, _, _>(|args, ctx| {
            let output = ctx.output_slot();
            async move {
                output.set(&json!({ "n": args.n })).unwrap();
                if args.n == 0 {
                    return Err(JobError::Terminal("fails".into()));
                }
                Ok(JobResult::Completed)
            }
        })
        .build()
        .unwrap();
    let completed = insert(&pool, &Summarize { n: 1 }, queue, 1, &[]).await;
    let failed = insert(&pool, &Summarize { n: 0 }, queue, 1, &[]).await;
    client.start().await.unwrap();
    let completed = wait_for_state(&pool, completed, JobState::Completed).await;
    let failed = wait_for_state(&pool, failed, JobState::Failed).await;
    client.shutdown(Duration::from_secs(2)).await;
    assert_eq!(completed.output, None);
    assert!(outputs_in(&failed.errors).is_empty(), "{:?}", failed.errors);
}

// ── enqueue specs never see an output ────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Noted {
    seen_output: bool,
}

#[tokio::test]
async fn enqueue_specs_receive_the_row_without_its_output() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "output_specs";
    let client = Client::builder(pool.clone())
        .queue(queue, queue_config())
        .register::<Summarize, _, _>(|args, ctx| {
            let output = ctx.output_slot();
            async move {
                output.set(&json!({ "n": args.n })).unwrap();
                Ok(JobResult::Completed)
            }
        })
        .on_completed_enqueue_with::<Summarize, Noted, _>(|_args, job| {
            EnqueueRequest::new(Noted {
                seen_output: job.output.is_some()
                    || job
                        .errors
                        .iter()
                        .flatten()
                        .any(|entry| entry.get("output").is_some()),
            })
            .queue("output_specs_noted")
        })
        .build()
        .unwrap();
    let id = insert(&pool, &Summarize { n: 5 }, queue, 1, &[]).await;
    client.start().await.unwrap();
    let job = wait_for_state(&pool, id, JobState::Completed).await;
    client.shutdown(Duration::from_secs(2)).await;
    assert_eq!(job.output, Some(json!({"n": 5})), "stored despite the spec");
    let seen: Vec<bool> =
        sqlx::query_scalar("SELECT (args->>'seen_output')::bool FROM awa.jobs WHERE kind = $1")
            .bind(Noted::kind())
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(seen, vec![false]);
}

// ── external completion stores an output only when asked ────────────────

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Await {
    n: i64,
}

struct AwaitWorker;

#[async_trait::async_trait]
impl awa::Worker for AwaitWorker {
    fn kind(&self) -> &'static str {
        Await::kind()
    }

    async fn perform(&self, ctx: &awa::JobContext) -> Result<JobResult, JobError> {
        // The parking attempt's output is discarded.
        ctx.set_output(&json!("parked")).unwrap();
        let guard = ctx
            .register_callback(Duration::from_secs(60))
            .await
            .map_err(JobError::retryable)?;
        Ok(JobResult::WaitForCallback(guard))
    }
}

#[tokio::test]
async fn external_completion_stores_the_output_it_is_given() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "output_external";
    let client = Client::builder(pool.clone())
        .queue(queue, queue_config())
        .register_worker(AwaitWorker)
        .build()
        .unwrap();
    let with_output = insert(&pool, &Await { n: 1 }, queue, 1, &[]).await;
    let without_output = insert(&pool, &Await { n: 2 }, queue, 1, &[]).await;
    client.start().await.unwrap();
    let parked = wait_for_state(&pool, with_output, JobState::WaitingExternal).await;
    let plain = wait_for_state(&pool, without_output, JobState::WaitingExternal).await;
    let callback = parked.callback_id.unwrap();

    let too_large = json!("x".repeat(MAX_OUTPUT_BYTES));
    let err = client
        .complete_external_with(
            callback,
            awa::model::ExternalCompletion::new().output(too_large),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, awa::AwaError::Validation(_)), "{err:?}");
    assert_eq!(
        admin::get_job(&pool, with_output).await.unwrap().state,
        JobState::WaitingExternal,
        "an invalid output completes nothing"
    );

    let completed = client
        .complete_external_with(
            callback,
            awa::model::ExternalCompletion::new().output(json!({"approved": true})),
        )
        .await
        .unwrap();
    assert_eq!(completed.output, Some(json!({"approved": true})));
    let plain_done = client
        .complete_external(plain.callback_id.unwrap(), Some(json!("ignored")), None)
        .await
        .unwrap();
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(
        admin::get_job(&pool, with_output).await.unwrap().output,
        Some(json!({"approved": true}))
    );
    assert_eq!(plain_done.output, None);
    assert_eq!(
        admin::get_job(&pool, without_output).await.unwrap().output,
        None
    );
}

// ── a schema without the v052 columns ────────────────────────────────────

/// Renaming the columns away makes the schema look like one before v052 to
/// the catalog probe, while the view keeps working (it tracks columns by
/// number). Completion outputs are dropped, failed outputs are kept, and
/// reads leave the column out.
#[tokio::test]
async fn a_schema_without_output_columns_drops_completion_outputs() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    sqlx::query("ALTER TABLE awa.done_entries RENAME COLUMN output TO output_pre_v052")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "ALTER TABLE awa.receipt_completion_batches RENAME COLUMN outputs TO outputs_pre_v052",
    )
    .execute(&pool)
    .await
    .unwrap();

    let queue = "output_pre_v052";
    let client = Client::builder(pool.clone())
        .queue(queue, queue_config())
        .register::<Summarize, _, _>(|args, ctx| {
            let output = ctx.output_slot();
            async move {
                output.set(&json!({ "n": args.n })).unwrap();
                if args.n == 0 {
                    return Err(JobError::Terminal("fails".into()));
                }
                Ok(JobResult::Completed)
            }
        })
        .build()
        .unwrap();
    let compact = insert(&pool, &Summarize { n: 1 }, queue, 1, &[]).await;
    let general = insert(&pool, &Summarize { n: 2 }, queue, 1, &["tagged"]).await;
    let failed = insert(&pool, &Summarize { n: 0 }, queue, 1, &[]).await;
    client.start().await.unwrap();
    let compact = wait_for_state(&pool, compact, JobState::Completed).await;
    let general = wait_for_state(&pool, general, JobState::Completed).await;
    let failed = wait_for_state(&pool, failed, JobState::Failed).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(compact.output, None);
    assert_eq!(general.output, None);
    assert_eq!(outputs_in(&failed.errors), vec![json!({"n": 0})]);
}
