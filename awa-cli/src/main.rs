use clap::{Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;

#[derive(Parser)]
#[command(name = "awa", about = "Awa — Postgres-native background job queue")]
struct Cli {
    /// Database URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run database migrations
    Migrate {
        /// Extract migration SQL to a directory instead of applying
        #[arg(long)]
        extract_to: Option<String>,
    },
    /// Job management
    Job {
        #[command(subcommand)]
        command: JobCommands,
    },
    /// Queue management
    Queue {
        #[command(subcommand)]
        command: QueueCommands,
    },
    /// Cron/periodic job management
    Cron {
        #[command(subcommand)]
        command: CronCommands,
    },
}

#[derive(Subcommand)]
enum JobCommands {
    /// Retry a failed or cancelled job
    Retry { id: i64 },
    /// Cancel a job
    Cancel { id: i64 },
    /// Retry all failed jobs by kind
    RetryFailed {
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        queue: Option<String>,
    },
    /// Discard failed jobs by kind
    Discard {
        #[arg(long)]
        kind: String,
    },
    /// List jobs
    List {
        #[arg(long)]
        state: Option<String>,
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long, default_value = "20")]
        limit: i64,
    },
}

#[derive(Subcommand)]
enum CronCommands {
    /// List all registered cron job schedules
    List,
    /// Remove a cron job schedule by name
    Remove { name: String },
}

#[derive(Subcommand)]
enum QueueCommands {
    /// Pause a queue
    Pause { queue: String },
    /// Resume a queue
    Resume { queue: String },
    /// Drain a queue (cancel all pending jobs)
    Drain { queue: String },
    /// Show queue statistics
    Stats,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&cli.database_url)
        .await?;

    match cli.command {
        Commands::Migrate { extract_to } => {
            if let Some(dir) = extract_to {
                std::fs::create_dir_all(&dir)?;
                for (version, description, sql) in awa_model::migrations::migration_sql() {
                    let filename = format!("{dir}/V{version}__{description}.sql");
                    let filename = filename.replace(' ', "_");
                    std::fs::write(&filename, sql)?;
                    println!("Extracted: {filename}");
                }
            } else {
                awa_model::migrations::run(&pool).await?;
                println!("Migrations applied successfully.");
            }
        }

        Commands::Job { command } => match command {
            JobCommands::Retry { id } => {
                awa_model::admin::retry(&pool, id).await?;
                println!("Retried job {id}");
            }

            JobCommands::Cancel { id } => {
                awa_model::admin::cancel(&pool, id).await?;
                println!("Cancelled job {id}");
            }

            JobCommands::RetryFailed { kind, queue } => {
                let count = if let Some(kind) = kind {
                    let jobs = awa_model::admin::retry_failed_by_kind(&pool, &kind).await?;
                    jobs.len()
                } else if let Some(queue) = queue {
                    let jobs = awa_model::admin::retry_failed_by_queue(&pool, &queue).await?;
                    jobs.len()
                } else {
                    eprintln!("Must specify --kind or --queue");
                    std::process::exit(1);
                };
                println!("Retried {count} failed jobs");
            }

            JobCommands::Discard { kind } => {
                let count = awa_model::admin::discard_failed(&pool, &kind).await?;
                println!("Discarded {count} failed jobs of kind '{kind}'");
            }

            JobCommands::List {
                state,
                kind,
                queue,
                limit,
            } => {
                let state = state.map(|s| match s.as_str() {
                    "available" => awa_model::JobState::Available,
                    "running" => awa_model::JobState::Running,
                    "completed" => awa_model::JobState::Completed,
                    "failed" => awa_model::JobState::Failed,
                    "cancelled" => awa_model::JobState::Cancelled,
                    "retryable" => awa_model::JobState::Retryable,
                    "scheduled" => awa_model::JobState::Scheduled,
                    other => {
                        eprintln!("Unknown state: {other}");
                        std::process::exit(1);
                    }
                });

                let filter = awa_model::admin::ListJobsFilter {
                    state,
                    kind,
                    queue,
                    limit: Some(limit),
                };

                let jobs = awa_model::admin::list_jobs(&pool, &filter).await?;
                if jobs.is_empty() {
                    println!("No jobs found.");
                } else {
                    println!(
                        "{:<8} {:<25} {:<10} {:<10} {:<5} {:<5}",
                        "ID", "KIND", "QUEUE", "STATE", "ATT", "MAX"
                    );
                    for job in &jobs {
                        println!(
                            "{:<8} {:<25} {:<10} {:<10} {:<5} {:<5}",
                            job.id, &job.kind, &job.queue, job.state, job.attempt, job.max_attempts,
                        );
                    }
                    println!("\n{} jobs listed.", jobs.len());
                }
            }
        },

        Commands::Cron { command } => match command {
            CronCommands::List => {
                let schedules = awa_model::cron::list_cron_jobs(&pool).await?;
                if schedules.is_empty() {
                    println!("No cron job schedules found.");
                } else {
                    println!(
                        "{:<25} {:<20} {:<12} {:<25} {:<10}",
                        "NAME", "CRON", "TIMEZONE", "KIND", "QUEUE"
                    );
                    for s in &schedules {
                        println!(
                            "{:<25} {:<20} {:<12} {:<25} {:<10}",
                            s.name, s.cron_expr, s.timezone, s.kind, s.queue,
                        );
                    }
                    println!("\n{} schedules listed.", schedules.len());
                }
            }
            CronCommands::Remove { name } => {
                let deleted = awa_model::cron::delete_cron_job(&pool, &name).await?;
                if deleted {
                    println!("Removed cron schedule '{name}'");
                } else {
                    println!("No cron schedule found with name '{name}'");
                }
            }
        },

        Commands::Queue { command } => match command {
            QueueCommands::Pause { queue } => {
                awa_model::admin::pause_queue(&pool, &queue, Some("cli")).await?;
                println!("Paused queue '{queue}'");
            }
            QueueCommands::Resume { queue } => {
                awa_model::admin::resume_queue(&pool, &queue).await?;
                println!("Resumed queue '{queue}'");
            }
            QueueCommands::Drain { queue } => {
                let count = awa_model::admin::drain_queue(&pool, &queue).await?;
                println!("Drained {count} jobs from queue '{queue}'");
            }
            QueueCommands::Stats => {
                let stats = awa_model::admin::queue_stats(&pool).await?;
                if stats.is_empty() {
                    println!("No queues found.");
                } else {
                    println!(
                        "{:<15} {:<10} {:<10} {:<10} {:<15} {:<10}",
                        "QUEUE", "AVAIL", "RUNNING", "FAILED", "COMPLETED/1H", "LAG(s)"
                    );
                    for stat in &stats {
                        println!(
                            "{:<15} {:<10} {:<10} {:<10} {:<15} {:<10}",
                            stat.queue,
                            stat.available,
                            stat.running,
                            stat.failed,
                            stat.completed_last_hour,
                            stat.lag_seconds
                                .map(|s| format!("{:.1}", s))
                                .unwrap_or_else(|| "-".to_string()),
                        );
                    }
                }
            }
        },
    }

    Ok(())
}
