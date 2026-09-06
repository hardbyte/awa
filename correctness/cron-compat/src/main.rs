//! Released N-1 executable. No copied enqueue/upsert SQL.
use awa::{Client, JobArgs, JobResult, PeriodicJob, QueueConfig};
use serde::{Deserialize, Serialize};
use std::time::Duration;
#[derive(Debug, Serialize, Deserialize, JobArgs)]
struct CompatCron {
    value: u32,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let mode = std::env::args().nth(1).unwrap_or_else(|| "enqueue".into());
    let job = PeriodicJob::builder("compat_cron_481", "* * * * * *")
        .queue("compat_cron_481")
        .build(&CompatCron { value: 1 })?;
    match mode.as_str() {
        "bootstrap" => {
            awa::model::migrations::run(&pool).await?;
            awa::model::cron::upsert_cron_job(&pool, &job).await?;
        }
        "upsert" => {
            awa::model::cron::upsert_cron_job(&pool, &job).await?;
        }
        "enqueue" => {
            let row = awa::model::cron::list_cron_jobs(&pool)
                .await?
                .into_iter()
                .find(|r| r.name == job.name)
                .ok_or("missing schedule")?;
            let result = awa::model::cron::atomic_enqueue(
                &pool,
                &job.name,
                chrono::Utc::now(),
                row.last_enqueued_at,
            )
            .await?;
            println!(
                "{}",
                serde_json::json!({"artifact":"awa=0.6.7","enqueued":result.is_some()})
            );
        }
        "serve" => {
            let client = Client::builder(pool.clone())
                .queue(
                    "compat_cron_481",
                    QueueConfig {
                        max_workers: 1,
                        ..Default::default()
                    },
                )
                .register::<CompatCron, _, _>(|_, _| async { Ok(JobResult::Completed) })
                .periodic(job)
                .runtime_snapshot_interval(Duration::from_millis(100))
                .leader_election_interval(Duration::from_millis(100))
                .build()?;
            client.start().await?;
            println!("READY awa=0.6.7");
            use tokio::io::AsyncBufReadExt;
            let mut line = String::new();
            tokio::io::BufReader::new(tokio::io::stdin())
                .read_line(&mut line)
                .await?;
            client.shutdown(Duration::from_secs(5)).await;
        }
        _ => return Err("unknown mode".into()),
    }
    pool.close().await;
    Ok(())
}
