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
    // The default one-second ring rotation lets prune truncate completed
    // rows before the table checks below run on a slow machine.
    let client = Client::builder(pool.clone())
        .queue_storage(
            awa::QueueStorageConfig::default(),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        )
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

    let batch_outputs = |job_id: i64| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM awa.receipt_completion_batches \
                 WHERE outputs IS NOT NULL AND $1 = ANY(job_ids)",
            )
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let done_outputs = |job_id: i64| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM awa.done_entries WHERE job_id = $1 AND output IS NOT NULL",
            )
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    assert_eq!(
        (batch_outputs(compact).await, done_outputs(compact).await),
        (1, 0),
        "the untagged job stays on the compact path"
    );
    assert_eq!(
        (batch_outputs(general).await, done_outputs(general).await),
        (0, 1),
        "the tagged job takes the general path"
    );

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
    let listed = admin::find_jobs(
        &pool,
        &awa::JobFilter::new().queue(queue),
        awa::JobPage::default(),
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
        // A retry moves the row between tables; a read in between can miss it.
        if let Ok(job) = admin::get_job(&pool, id).await {
            if job.state.is_terminal() {
                break job;
            }
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
            Some(
                EnqueueRequest::new(Noted {
                    seen_output: job.output.is_some()
                        || job
                            .errors
                            .iter()
                            .flatten()
                            .any(|entry| entry.get("output").is_some()),
                })
                .queue("output_specs_noted"),
            )
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
    // The completed jobs are read back after shutdown; with the default
    // one-second ring rotation, prune can truncate them first.
    let client = Client::builder(pool.clone())
        .queue_storage(
            awa::QueueStorageConfig::default(),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        )
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

// ── a flush split by output size keeps every output with its job ──

/// Twenty outputs of about 3.5 KiB pass the 64 KiB batch budget, so one
/// flush writes several batch rows. Every output reads back on its own job,
/// and a batch row whose jobs recorded nothing stores no output array.
#[tokio::test]
async fn a_flush_split_by_output_size_keeps_every_output_with_its_job() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let store = awa::QueueStorage::from_existing_schema("awa").unwrap();
    for n in 0..40 {
        insert(&pool, &Summarize { n }, "output_split", 1, &[]).await;
    }
    for n in 0..5 {
        insert(&pool, &Summarize { n }, "output_split_none", 1, &[]).await;
    }
    let lease = Duration::from_secs(60);
    let mut claimed = store
        .claim_runtime_batch(&pool, "output_split", 40, lease)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 40);
    for (index, job) in claimed.iter_mut().enumerate() {
        job.job.output = (index % 2 == 0).then(|| json!({ "n": index, "pad": "x".repeat(3_500) }));
    }
    let mut flush = claimed.clone();
    flush.extend(
        store
            .claim_runtime_batch(&pool, "output_split_none", 5, lease)
            .await
            .unwrap(),
    );
    let updated = store.complete_runtime_batch(&pool, &flush).await.unwrap();
    assert_eq!(updated.len(), flush.len());

    for job in &claimed {
        let row = store.load_job(&pool, job.job.id).await.unwrap().unwrap();
        assert_eq!(row.state, JobState::Completed);
        assert_eq!(row.output, job.job.output, "job {}", job.job.id);
    }
    let (with_outputs, all_null, without_outputs): (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE outputs IS NOT NULL), \
                count(*) FILTER (WHERE outputs IS NOT NULL AND NOT EXISTS \
                    (SELECT 1 FROM unnest(outputs) AS output WHERE output IS NOT NULL)), \
                count(*) FILTER (WHERE outputs IS NULL) \
         FROM awa.receipt_completion_batches",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        with_outputs >= 2,
        "the flush was not split: {with_outputs} rows"
    );
    assert_eq!(all_null, 0, "a batch row stored an array of only nulls");
    assert!(
        without_outputs >= 1,
        "the output-free queue's row stores no array"
    );
}

// ── panic and handler cancel keep the output in their entry ──

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Ends {
    how: String,
}

#[tokio::test]
async fn panic_and_cancel_keep_the_output_in_their_error_entry() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "output_panic_cancel";
    let client = Client::builder(pool.clone())
        .queue(queue, queue_config())
        .register::<Ends, _, _>(|args, ctx| {
            let output = ctx.output_slot();
            async move {
                output.set(&json!({ "how": args.how })).unwrap();
                match args.how.as_str() {
                    "panic" => panic!("handler panicked after recording"),
                    _ => Ok(JobResult::Cancel("cancelled by the handler".into())),
                }
            }
        })
        .build()
        .unwrap();
    let panicked = insert(
        &pool,
        &Ends {
            how: "panic".into(),
        },
        queue,
        1,
        &[],
    )
    .await;
    let cancelled = insert(
        &pool,
        &Ends {
            how: "cancel".into(),
        },
        queue,
        1,
        &[],
    )
    .await;
    client.start().await.unwrap();
    let panicked = wait_for_state(&pool, panicked, JobState::Failed).await;
    let cancelled = wait_for_state(&pool, cancelled, JobState::Cancelled).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(panicked.output, None);
    assert_eq!(outputs_in(&panicked.errors), vec![json!({"how": "panic"})]);
    assert_eq!(cancelled.output, None);
    assert_eq!(
        outputs_in(&cancelled.errors),
        vec![json!({"how": "cancel"})]
    );
}

// ── the DLQ keeps a failed output; a DLQ retry dispatches without it ──

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct DeadLetter {
    n: i64,
}

#[tokio::test]
async fn the_dlq_keeps_the_failed_output_and_a_retry_dispatches_without_it() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "output_dlq";
    let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let leaked: Arc<Mutex<Vec<String>>> = Arc::default();
    let (runs_seen, leaked_seen) = (runs.clone(), leaked.clone());
    let client = Client::builder(pool.clone())
        .queue(queue, queue_config())
        .dlq_enabled_by_default(true)
        .register::<DeadLetter, _, _>(move |_args, ctx| {
            let run = runs_seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let row = serde_json::to_string(&(&ctx.job.errors, &ctx.job.metadata)).unwrap();
            if row.contains("dlq-marker") {
                leaked_seen.lock().unwrap().push(row);
            }
            let output = ctx.output_slot();
            async move {
                if run == 0 {
                    output.set(&json!({ "marker": "dlq-marker" })).unwrap();
                    return Err(JobError::Terminal("dead-lettered".into()));
                }
                output.set(&json!({ "revived": true })).unwrap();
                Ok(JobResult::Completed)
            }
        })
        .build()
        .unwrap();
    let id = insert(&pool, &DeadLetter { n: 1 }, queue, 1, &[]).await;
    client.start().await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let dead = loop {
        if let Some(row) = awa::model::dlq::get_dlq_job(&pool, id).await.unwrap() {
            break row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job never reached the DLQ"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(
        outputs_in(&dead.job.errors),
        vec![json!({ "marker": "dlq-marker" })]
    );

    awa::model::dlq::retry_from_dlq(&pool, id, &awa::RetryFromDlqOpts::default())
        .await
        .unwrap()
        .expect("revived");
    let done = wait_for_state(&pool, id, JobState::Completed).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(done.output, Some(json!({ "revived": true })));
    assert!(
        leaked.lock().unwrap().is_empty(),
        "a dispatch row carried a failed output: {:?}",
        leaked.lock().unwrap()
    );
}

// ── rescue keeps the earlier output; a stale completion stores nothing ──

#[tokio::test]
async fn rescue_keeps_the_earlier_output_and_a_stale_completion_stores_nothing() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let store = awa::QueueStorage::from_existing_schema("awa").unwrap();
    let queue = "output_rescue";
    let lease = Duration::from_secs(60);
    let id = insert(&pool, &Summarize { n: 1 }, queue, 5, &[]).await;

    let first = store
        .claim_runtime_batch(&pool, queue, 1, lease)
        .await
        .unwrap()
        .remove(0);
    store
        .fail_retryable_after_with_claim(
            &pool,
            id,
            first.job.run_lease,
            Duration::ZERO,
            awa::AttemptError::new("first attempt").with_output(Some(json!({ "attempt": 1 }))),
            None,
            Some(&first.claim),
        )
        .await
        .unwrap()
        .expect("attempt 1 failed");
    store
        .promote_due(&pool, JobState::Retryable, 10)
        .await
        .unwrap();

    let mut second = store
        .claim_runtime_batch(&pool, queue, 1, lease)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(second.job.attempt, 2);
    let rescued = store
        .rescue_stale_heartbeats(&pool, Duration::ZERO)
        .await
        .unwrap();
    assert!(
        rescued.iter().any(|row| row.id == id),
        "attempt 2 was not rescued"
    );

    // The rescued attempt's late completion is stale: nothing it carries is
    // stored.
    second.job.output = Some(json!({ "attempt": 2 }));
    let updated = store
        .complete_runtime_batch(&pool, std::slice::from_ref(&second))
        .await
        .unwrap();
    assert!(
        updated.is_empty(),
        "a rescued attempt completed: {updated:?}"
    );

    let row = store.load_job(&pool, id).await.unwrap().unwrap();
    assert_ne!(row.state, JobState::Completed);
    assert_eq!(row.output, None);
    assert_eq!(
        outputs_in(&row.errors),
        vec![json!({ "attempt": 1 })],
        "the rescue entry carries no output and keeps the earlier one: {:?}",
        row.errors
    );
}

// ── typed outputs ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Thumbnail {
    width: u32,
    url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
#[awa(output = Thumbnail)]
struct Resize {
    width: u32,
    fail: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Verdict {
    approved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
#[awa(output = Verdict)]
struct Review {
    n: i64,
}

/// A kind declaring an output publishes its type in the kind descriptor; the
/// handler records through the typed slot, which refuses another kind's
/// slot, and readers decode with `output_as` / `error_output_as`.
#[tokio::test]
async fn typed_outputs_round_trip_and_publish_their_type() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "output_typed";
    let mismatches: Arc<Mutex<Vec<String>>> = Arc::default();
    let seen = mismatches.clone();
    let client = Client::builder(pool.clone())
        .queue(queue, queue_config())
        .register::<Resize, _, _>(move |args, ctx| {
            // The wrong kind's slot fails, and clears like any failed set.
            match ctx
                .typed_output::<Review>()
                .set(&Verdict { approved: true })
            {
                Err(OutputError::Invalid(message)) => seen.lock().unwrap().push(message),
                other => panic!("expected a kind mismatch, got {other:?}"),
            }
            assert_eq!(ctx.output(), None);
            let output = ctx.typed_output::<Resize>();
            async move {
                output
                    .set(&Thumbnail {
                        width: args.width,
                        url: format!("https://img/{}", args.width),
                    })
                    .unwrap();
                assert_eq!(output.get().unwrap().unwrap().width, args.width);
                if args.fail {
                    return Err(JobError::terminal("thumbnail rejected"));
                }
                Ok(JobResult::Completed)
            }
        })
        .job_kind_descriptor::<Review>(awa::JobKindDescriptor::new().display_name("Review"))
        .build()
        .unwrap();

    let completed = insert(
        &pool,
        &Resize {
            width: 64,
            fail: false,
        },
        queue,
        1,
        &[],
    )
    .await;
    let failed = insert(
        &pool,
        &Resize {
            width: 32,
            fail: true,
        },
        queue,
        1,
        &[],
    )
    .await;
    client.start().await.unwrap();
    let completed = wait_for_state(&pool, completed, JobState::Completed).await;
    let failed = wait_for_state(&pool, failed, JobState::Failed).await;
    client.shutdown(Duration::from_secs(2)).await;

    assert_eq!(
        completed.output_as::<Resize>().unwrap(),
        Some(Thumbnail {
            width: 64,
            url: "https://img/64".to_string()
        })
    );
    let err = completed.output_as::<Review>().unwrap_err();
    assert!(matches!(err, awa::AwaError::Validation(_)), "{err:?}");
    assert_eq!(completed.error_output_as::<Resize>().unwrap(), None);
    assert_eq!(failed.output_as::<Resize>().unwrap(), None);
    assert_eq!(
        failed.error_output_as::<Resize>().unwrap().map(|t| t.width),
        Some(32)
    );
    let mismatches = mismatches.lock().unwrap().clone();
    assert_eq!(mismatches.len(), 2);
    assert!(
        mismatches[0].contains("is of kind resize, not review"),
        "{mismatches:?}"
    );

    // Both the registered kind and the descriptor-only kind publish their
    // output type; the hand-written descriptor keeps its other fields.
    let descriptors =
        admin::job_kind_descriptors_for_names(&pool, &["resize".to_string(), "review".to_string()])
            .await
            .unwrap();
    assert_eq!(
        descriptors["resize"].output,
        Some(awa::OutputDescriptor::new("Thumbnail"))
    );
    assert_eq!(
        descriptors["review"].output,
        Some(awa::OutputDescriptor::new("Verdict"))
    );
    assert_eq!(
        descriptors["review"].display_name.as_deref(),
        Some("Review")
    );
    let overview = admin::job_kind_overviews(&pool)
        .await
        .unwrap()
        .into_iter()
        .find(|kind| kind.kind == "resize")
        .unwrap();
    assert_eq!(overview.output, Some(json!({"type": "Thumbnail"})));
}

/// An explicit output descriptor, such as one carrying a JSON Schema, wins
/// over the declared type name.
#[tokio::test]
async fn an_explicit_output_descriptor_is_kept() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let schema = json!({"type": "object", "required": ["width", "url"]});
    let client = Client::builder(pool.clone())
        .queue("output_descriptor", queue_config())
        .job_kind_descriptor::<Resize>(
            awa::JobKindDescriptor::new()
                .output(awa::OutputDescriptor::new("Thumbnail").schema(schema.clone())),
        )
        .register::<Resize, _, _>(|_, _| async { Ok(JobResult::Completed) })
        .build()
        .unwrap();
    client.start().await.unwrap();
    client.shutdown(Duration::from_secs(2)).await;

    let descriptors = admin::job_kind_descriptors_for_names(&pool, &["resize".to_string()])
        .await
        .unwrap();
    assert_eq!(
        descriptors["resize"].output,
        Some(awa::OutputDescriptor::new("Thumbnail").schema(schema))
    );
}

struct ReviewWorker;

#[async_trait::async_trait]
impl awa::Worker for ReviewWorker {
    fn kind(&self) -> &'static str {
        Review::kind()
    }

    async fn perform(&self, ctx: &awa::JobContext) -> Result<JobResult, JobError> {
        let guard = ctx
            .register_callback(Duration::from_secs(60))
            .await
            .map_err(JobError::retryable)?;
        Ok(JobResult::WaitForCallback(guard))
    }
}

/// `ExternalCompletion::output_for::<T>` completes only a job of kind `T`.
#[tokio::test]
async fn typed_external_completion_checks_the_kind() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let queue = "output_typed_external";
    let client = Client::builder(pool.clone())
        .queue(queue, queue_config())
        .register_worker(ReviewWorker)
        .build()
        .unwrap();
    let job = insert(&pool, &Review { n: 1 }, queue, 1, &[]).await;
    client.start().await.unwrap();
    let parked = wait_for_state(&pool, job, JobState::WaitingExternal).await;
    let callback = parked.callback_id.unwrap();

    let wrong_kind = awa::model::ExternalCompletion::new()
        .output_for::<Resize>(&Thumbnail {
            width: 1,
            url: "x".to_string(),
        })
        .unwrap();
    let err = client
        .complete_external_with(callback, wrong_kind)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, awa::AwaError::Validation(message) if message.contains("not resize")),
        "{err:?}"
    );
    assert_eq!(
        admin::get_job(&pool, job).await.unwrap().state,
        JobState::WaitingExternal,
        "a mismatched kind completes nothing"
    );

    let completed = client
        .complete_external_with(
            callback,
            awa::model::ExternalCompletion::new()
                .output_for::<Review>(&Verdict { approved: true })
                .unwrap(),
        )
        .await
        .unwrap();
    client.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        completed.output_as::<Review>().unwrap(),
        Some(Verdict { approved: true })
    );
}

/// Before v052 the descriptor catalog has no output column; the sync leaves
/// the output type out and stores the rest of the descriptor.
#[tokio::test]
async fn descriptors_sync_without_the_output_column() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    sqlx::query("ALTER TABLE awa.job_kind_descriptors RENAME COLUMN output TO output_hidden")
        .execute(&pool)
        .await
        .unwrap();
    let client = Client::builder(pool.clone())
        .queue("output_old_catalog", queue_config())
        .job_kind_descriptor::<Resize>(awa::JobKindDescriptor::new().display_name("Resize"))
        .register::<Resize, _, _>(|_, _| async { Ok(JobResult::Completed) })
        .build()
        .unwrap();
    client.start().await.unwrap();
    client.shutdown(Duration::from_secs(2)).await;

    let descriptors = admin::job_kind_descriptors_for_names(&pool, &["resize".to_string()])
        .await
        .unwrap();
    assert_eq!(
        descriptors["resize"].display_name.as_deref(),
        Some("Resize")
    );
    assert_eq!(descriptors["resize"].output, None);
    let overview = admin::job_kind_overviews(&pool)
        .await
        .unwrap()
        .into_iter()
        .find(|kind| kind.kind == "resize")
        .unwrap();
    assert_eq!(overview.output, None);
}
