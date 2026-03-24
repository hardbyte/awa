use clap::{Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;

#[derive(Parser)]
#[command(name = "awa", about = "Awa — Postgres-native background job queue")]
struct Cli {
    /// Database URL (not required for migrate --sql without --pending)
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

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
        /// Print migration SQL to stdout instead of applying
        #[arg(long)]
        sql: bool,
        /// Only include migrations after this version (exclusive)
        #[arg(long)]
        from: Option<i32>,
        /// Only include migrations up to this version (inclusive)
        #[arg(long)]
        to: Option<i32>,
        /// Show a single migration version
        #[arg(long, conflicts_with_all = ["from", "to"])]
        version: Option<i32>,
        /// Auto-detect: from=current DB version, to=latest
        #[arg(long, conflicts_with_all = ["from", "version"])]
        pending: bool,
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
    /// Start the web UI server
    Serve {
        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        /// Port to listen on
        #[arg(long, default_value = "3000")]
        port: u16,
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

    // Build the pool lazily — some commands (migrate --sql) don't need a DB.
    let require_pool = |url: &Option<String>| -> Result<String, Box<dyn std::error::Error>> {
        url.clone().ok_or_else(|| {
            "DATABASE_URL is required for this command. Set --database-url or DATABASE_URL env var."
                .into()
        })
    };

    match cli.command {
        Commands::Migrate {
            extract_to,
            sql,
            from,
            to,
            version,
            pending,
        } => {
            // Resolve the version range.
            let current_version = awa_model::migrations::CURRENT_VERSION;

            let (range_from, range_to) = if let Some(v) = version {
                if v < 1 || v > current_version {
                    eprintln!("Version {v} is out of range. Valid versions: 1..{current_version}");
                    std::process::exit(1);
                }
                (v - 1, v)
            } else if pending {
                let db_url = require_pool(&cli.database_url)?;
                let pool = PgPoolOptions::new()
                    .max_connections(2)
                    .connect(&db_url)
                    .await?;
                let db_version = awa_model::migrations::current_version(&pool).await?;
                (db_version, current_version)
            } else {
                (from.unwrap_or(0), to.unwrap_or(current_version))
            };

            if range_from >= range_to {
                if pending {
                    println!("Schema is up to date (version {range_from}).");
                } else {
                    eprintln!("No migrations in range ({range_from}, {range_to}].");
                }
                return Ok(());
            }

            let selected = awa_model::migrations::migration_sql_range(range_from, range_to);

            if selected.is_empty() {
                println!("No migrations matched the selected range.");
                return Ok(());
            }

            if sql {
                // Print to stdout — no DB required.
                for (v, description, sql_text) in &selected {
                    println!("-- Migration V{v}: {description}\n{sql_text}\n");
                }
            } else if let Some(dir) = extract_to {
                std::fs::create_dir_all(&dir)?;
                for (v, description, sql_text) in &selected {
                    let filename = format!("{dir}/V{v}__{description}.sql");
                    let filename = filename.replace(' ', "_");
                    std::fs::write(&filename, &sql_text)?;
                    println!("Extracted: {filename}");
                }
            } else {
                // Default: apply migrations to DB.
                let db_url = require_pool(&cli.database_url)?;
                let pool = PgPoolOptions::new()
                    .max_connections(5)
                    .connect(&db_url)
                    .await?;
                awa_model::migrations::run(&pool).await?;
                println!("Migrations applied successfully.");
            }
        }

        // All remaining commands require a database connection.
        command => {
            let db_url = require_pool(&cli.database_url)?;
            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect(&db_url)
                .await?;

            match command {
                Commands::Migrate { .. } => unreachable!(),

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
                            let jobs =
                                awa_model::admin::retry_failed_by_queue(&pool, &queue).await?;
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
                            ..Default::default()
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
                                    job.id,
                                    &job.kind,
                                    &job.queue,
                                    job.state,
                                    job.attempt,
                                    job.max_attempts,
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

                Commands::Serve { host, port } => {
                    let app = awa_ui::router(pool);
                    let addr = format!("{host}:{port}");
                    let listener = tokio::net::TcpListener::bind(&addr).await?;
                    tracing::info!("AWA UI listening on http://{addr}");
                    axum::serve(listener, app).await?;
                }

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
                                "{:<15} {:<10} {:<10} {:<10} {:<15} {:<10} {:<8}",
                                "QUEUE",
                                "AVAIL",
                                "RUNNING",
                                "FAILED",
                                "COMPLETED/1H",
                                "LAG(s)",
                                "PAUSED"
                            );
                            for stat in &stats {
                                println!(
                                    "{:<15} {:<10} {:<10} {:<10} {:<15} {:<10} {:<8}",
                                    stat.queue,
                                    stat.available,
                                    stat.running,
                                    stat.failed,
                                    stat.completed_last_hour,
                                    stat.lag_seconds
                                        .map(|s| format!("{:.1}", s))
                                        .unwrap_or_else(|| "-".to_string()),
                                    if stat.paused { "yes" } else { "no" },
                                );
                            }
                        }
                    }
                },
            }
        }
    }

    Ok(())
}
