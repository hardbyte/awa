//! A complete AWA Rust quickstart.
//!
//! Run with a PostgreSQL database available at `DATABASE_URL`:
//! `cargo run -p awa --example quickstart`.

use awa::{
    admin, insert_with, migrations, Client, InsertOpts, JobArgs, JobResult, JobState, QueueConfig,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::{env, time::Duration};

#[derive(Debug, Serialize, Deserialize)]
struct SendEmail {
    to: String,
    subject: String,
}

impl JobArgs for SendEmail {
    fn kind() -> &'static str {
        "send_email"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    migrations::run(&pool).await?;

    let client = Client::builder(pool.clone())
        .queue(
            "email",
            QueueConfig {
                max_workers: 2,
                ..Default::default()
            },
        )
        .register::<SendEmail, _, _>(|args, _ctx| async move {
            println!("sending email to {}: {}", args.to, args.subject);
            Ok(JobResult::Completed)
        })
        .build()?;

    client.start().await?;

    let job = insert_with(
        &pool,
        &SendEmail {
            to: "alice@example.com".into(),
            subject: "Welcome".into(),
        },
        InsertOpts {
            queue: "email".into(),
            ..Default::default()
        },
    )
    .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let job = loop {
        let current = admin::get_job(&pool, job.id).await?;
        match current.state {
            JobState::Completed => break current,
            JobState::Failed | JobState::Cancelled => {
                return Err(std::io::Error::other(format!(
                    "job {} ended in terminal state {}",
                    current.id, current.state
                ))
                .into());
            }
            _ if tokio::time::Instant::now() >= deadline => {
                return Err(std::io::Error::other(format!(
                    "timed out waiting for job {} (last state: {})",
                    current.id, current.state
                ))
                .into());
            }
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    };
    println!("job {} state = {:?}", job.id, job.state);

    client.shutdown(Duration::from_secs(5)).await;
    Ok(())
}
