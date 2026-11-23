//! An application on sqlx 0.9 that runs awa 0.6.
//!
//! awa 0.6 is built on sqlx 0.8 and takes sqlx 0.8 types in its API, so it
//! gets a pool of its own from sqlx 0.8, while the application keeps sqlx 0.9
//! for everything else. To enqueue in the same transaction as the
//! application's own writes, the application prepares the job with
//! `awa::adapter::postgres::prepare_raw_job_insert` and runs
//! `awa::adapter::postgres::INSERT_JOB_SQL` on its sqlx 0.9 transaction. The
//! preparation is the one awa's own insert uses, so unique keys, scheduled
//! state and ordering keys come out the same.
//!
//! Run with `DATABASE_URL` set; it exits non-zero if anything goes wrong.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use awa::adapter::postgres::{prepare_raw_job_insert, INSERT_JOB_SQL, UNIQUE_VIOLATION_SQLSTATE};
use awa::{Client, InsertOpts, JobContext, JobError, JobResult, QueueConfig, UniqueOpts, Worker};
use serde_json::json;
use sqlx::Row;

const QUEUE: &str = "welcome";
const WELCOME: &str = "send_welcome_email";
const REMINDER: &str = "send_reminder_email";

struct SendEmail {
    sent: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Worker for SendEmail {
    fn kind(&self) -> &'static str {
        WELCOME
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let email = ctx.job.args["email"].as_str().unwrap_or_default();
        println!("sending welcome email to {email}");
        self.sent.fetch_add(1, Ordering::SeqCst);
        Ok(JobResult::Completed)
    }
}

/// Inserts one prepared awa job on a sqlx 0.9 connection or transaction and
/// returns the stored state.
async fn enqueue(
    conn: &mut sqlx::PgConnection,
    kind: &str,
    args: serde_json::Value,
    opts: InsertOpts,
) -> Result<String, Box<dyn std::error::Error>> {
    let job = prepare_raw_job_insert(kind, args, opts)?;
    // INSERT_JOB_SQL is a constant, so sqlx 0.9 takes it as is.
    let row = sqlx::query(INSERT_JOB_SQL)
        .bind(job.kind())
        .bind(job.queue())
        .bind(job.args())
        .bind(job.state_db_str())
        .bind(job.priority())
        .bind(job.max_attempts())
        .bind(job.run_at())
        .bind(job.metadata())
        .bind(job.tags())
        .bind(job.unique_key())
        .bind(job.unique_states_bit_string())
        .bind(job.ordering_key())
        .fetch_one(conn)
        .await?;
    let state: String = row.try_get("state_str")?;
    Ok(state)
}

/// Records a sign-up and enqueues its welcome email, unique per address, in
/// one sqlx 0.9 transaction, then commits or rolls back both.
async fn sign_up(
    app: &sqlx::PgPool,
    email: &str,
    commit: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut tx = app.begin().await?;
    sqlx::query("INSERT INTO app_signups (email) VALUES ($1)")
        .bind(email)
        .execute(&mut *tx)
        .await?;
    let opts = InsertOpts {
        queue: QUEUE.into(),
        unique: Some(UniqueOpts {
            by_args: true,
            ..UniqueOpts::default()
        }),
        ..InsertOpts::default()
    };
    enqueue(&mut tx, WELCOME, json!({ "email": email }), opts).await?;
    if commit {
        tx.commit().await?;
    } else {
        tx.rollback().await?;
    }
    Ok(())
}

fn is_unique_violation(err: &(dyn std::error::Error + 'static)) -> bool {
    err.downcast_ref::<sqlx::Error>()
        .and_then(|err| err.as_database_error())
        .and_then(|err| err.code())
        .is_some_and(|code| code == UNIQUE_VIOLATION_SQLSTATE)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")?;

    // The application's pool, on sqlx 0.9.
    let app = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    // awa's pool, on sqlx 0.8.
    let awa_pool = sqlx08::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await?;

    awa::migrations::run(&awa_pool).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS app_signups (id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL)",
    )
    .execute(&app)
    .await?;

    let sent = Arc::new(AtomicUsize::new(0));
    let client = Client::builder(awa_pool.clone())
        .queue(QUEUE, QueueConfig::default())
        .register_worker(SendEmail { sent: sent.clone() })
        .build()?;
    client.start().await?;

    sign_up(&app, "ada@example.com", true).await?;
    // Rolled back with the sign-up, so never run.
    sign_up(&app, "rolled.back@example.com", false).await?;
    // The same unique job again: the insert fails with a unique violation,
    // and the application's transaction rolls back with it.
    match sign_up(&app, "ada@example.com", true).await {
        Err(err) if is_unique_violation(err.as_ref()) => {}
        other => return Err(format!("expected a unique violation, got {other:?}").into()),
    }
    // A job with a run time is stored as scheduled, as with awa's own insert.
    let mut conn = app.acquire().await?;
    let state = enqueue(
        &mut conn,
        REMINDER,
        json!({ "email": "ada@example.com" }),
        InsertOpts {
            queue: "reminders".into(),
            run_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            ..InsertOpts::default()
        },
    )
    .await?;
    if state != "scheduled" {
        return Err(format!("a job with a future run time was stored as {state}").into());
    }
    drop(conn);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while sent.load(Ordering::SeqCst) < 1 {
        if tokio::time::Instant::now() > deadline {
            return Err("the committed job did not run within 30 seconds".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Give a wrongly enqueued job time to run before checking.
    tokio::time::sleep(Duration::from_secs(2)).await;
    client.shutdown(Duration::from_secs(10)).await;

    let sent = sent.load(Ordering::SeqCst);
    if sent != 1 {
        return Err(format!("expected exactly the committed job to run, ran {sent}").into());
    }
    println!(
        "ok: the committed job ran once on awa's sqlx 0.8 pool; the rolled-back one and the \
         duplicate never ran; the future job is scheduled"
    );
    Ok(())
}
