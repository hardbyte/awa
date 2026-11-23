//! An application on sqlx 0.9 that runs awa 0.6.
//!
//! awa 0.6 is built on sqlx 0.8 and takes sqlx 0.8 types in its API, so it
//! gets a pool of its own from sqlx 0.8, while the application keeps sqlx 0.9
//! for everything else. To enqueue in the same transaction as the
//! application's own writes, the application calls awa's SQL producer,
//! `awa.insert_job_compat`, on its sqlx 0.9 transaction.
//!
//! Run with `DATABASE_URL` set; it exits non-zero if anything goes wrong.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use awa::{Client, JobContext, JobError, JobResult, QueueConfig, Worker};

const QUEUE: &str = "welcome";
const KIND: &str = "send_welcome_email";

struct SendWelcomeEmail {
    sent: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Worker for SendWelcomeEmail {
    fn kind(&self) -> &'static str {
        KIND
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let email = ctx.job.args["email"].as_str().unwrap_or_default();
        println!("sending welcome email to {email}");
        self.sent.fetch_add(1, Ordering::SeqCst);
        Ok(JobResult::Completed)
    }
}

/// Records a sign-up and enqueues its welcome email in one sqlx 0.9
/// transaction, then commits or rolls back both.
async fn sign_up(app: &sqlx::PgPool, email: &str, commit: bool) -> Result<i64, sqlx::Error> {
    let mut tx = app.begin().await?;
    sqlx::query("INSERT INTO app_signups (email) VALUES ($1)")
        .bind(email)
        .execute(&mut *tx)
        .await?;
    let job_id: i64 = sqlx::query_scalar(
        "SELECT (awa.insert_job_compat(p_kind => $1, p_queue => $2, \
                                      p_args => jsonb_build_object('email', $3::text))).id",
    )
    .bind(KIND)
    .bind(QUEUE)
    .bind(email)
    .fetch_one(&mut *tx)
    .await?;
    if commit {
        tx.commit().await?;
    } else {
        tx.rollback().await?;
    }
    Ok(job_id)
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
        .register_worker(SendWelcomeEmail { sent: sent.clone() })
        .build()?;
    client.start().await?;

    sign_up(&app, "ada@example.com", true).await?;
    // Rolled back with the sign-up, so never run.
    sign_up(&app, "rolled.back@example.com", false).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while sent.load(Ordering::SeqCst) < 1 {
        if tokio::time::Instant::now() > deadline {
            return Err("the committed job did not run within 30 seconds".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Give a wrongly enqueued second job time to run before checking.
    tokio::time::sleep(Duration::from_secs(2)).await;
    client.shutdown(Duration::from_secs(10)).await;

    let sent = sent.load(Ordering::SeqCst);
    if sent != 1 {
        return Err(format!("expected exactly the committed job to run, ran {sent}").into());
    }
    println!(
        "ok: the committed job ran once on awa's sqlx 0.8 pool; the rolled-back one never ran"
    );
    Ok(())
}
