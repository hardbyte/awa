//! An application on sqlx 0.9 that runs awa 0.6.
//!
//! awa 0.6 is built on sqlx 0.8 and takes sqlx 0.8 types in its API, so it
//! gets a pool of its own from sqlx 0.8, while the application keeps sqlx 0.9
//! for everything else. To enqueue in the same transaction as the
//! application's own writes, it uses `awa::bridge::sqlx09` (the `sqlx09`
//! feature), which takes a sqlx 0.9 executor and prepares jobs exactly as
//! awa's own insert does, so unique keys, scheduled state and ordering keys
//! come out the same.
//!
//! Run with `DATABASE_URL` set; it exits non-zero if anything goes wrong.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use awa::bridge::sqlx09 as awa_sqlx09;
use awa::{
    AwaError, Client, InsertOpts, JobArgs, JobContext, JobError, JobResult, JobState, QueueConfig,
    UniqueOpts, Worker,
};
use serde::{Deserialize, Serialize};

const QUEUE: &str = "welcome";

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SendWelcomeEmail {
    email: String,
}

#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct SendReminderEmail {
    email: String,
}

struct WelcomeEmailWorker {
    sent: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Worker for WelcomeEmailWorker {
    fn kind(&self) -> &'static str {
        SendWelcomeEmail::kind()
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let email = ctx.job.args["email"].as_str().unwrap_or_default();
        println!("sending welcome email to {email}");
        self.sent.fetch_add(1, Ordering::SeqCst);
        Ok(JobResult::Completed)
    }
}

/// Records a sign-up and enqueues its welcome email, unique per address, in
/// one sqlx 0.9 transaction, then commits or rolls back both.
async fn sign_up(app: &sqlx::PgPool, email: &str, commit: bool) -> Result<(), AwaError> {
    let mut tx = app.begin().await.map_err(AwaError::Sqlx09)?;
    sqlx::query("INSERT INTO app_signups (email) VALUES ($1)")
        .bind(email)
        .execute(&mut *tx)
        .await
        .map_err(AwaError::Sqlx09)?;
    awa_sqlx09::insert_job_with(
        &mut *tx,
        &SendWelcomeEmail {
            email: email.to_string(),
        },
        InsertOpts {
            queue: QUEUE.into(),
            unique: Some(UniqueOpts {
                by_args: true,
                ..UniqueOpts::default()
            }),
            ..InsertOpts::default()
        },
    )
    .await?;
    if commit {
        tx.commit().await.map_err(AwaError::Sqlx09)?;
    } else {
        tx.rollback().await.map_err(AwaError::Sqlx09)?;
    }
    Ok(())
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
        .register_worker(WelcomeEmailWorker { sent: sent.clone() })
        .build()?;
    client.start().await?;

    sign_up(&app, "ada@example.com", true).await?;
    // Rolled back with the sign-up, so never run.
    sign_up(&app, "rolled.back@example.com", false).await?;
    // The same unique job again: the insert fails with a unique violation,
    // and the application's transaction rolls back with it.
    match sign_up(&app, "ada@example.com", true).await {
        Err(AwaError::UniqueConflict { .. }) => {}
        other => return Err(format!("expected a unique violation, got {other:?}").into()),
    }
    // A job with a run time is stored as scheduled, as with awa's own insert.
    let reminder = awa_sqlx09::insert_job_with(
        &app,
        &SendReminderEmail {
            email: "ada@example.com".into(),
        },
        InsertOpts {
            queue: "reminders".into(),
            run_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            ..InsertOpts::default()
        },
    )
    .await?;
    if reminder.state != JobState::Scheduled {
        return Err(format!(
            "a job with a future run time was stored as {:?}",
            reminder.state
        )
        .into());
    }

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
