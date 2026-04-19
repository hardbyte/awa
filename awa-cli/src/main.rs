use std::time::Duration;

use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;

#[derive(Parser)]
#[command(
    name = "awa",
    version,
    about = "Awa — Postgres-native background job queue"
)]
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
    /// Storage transition management
    Storage {
        #[command(subcommand)]
        command: StorageCommands,
    },
    /// Dead Letter Queue management
    Dlq {
        #[command(subcommand)]
        command: DlqCommands,
    },
    /// Start the web UI server
    Serve {
        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        /// Port to listen on
        #[arg(long, default_value = "3000")]
        port: u16,
        /// Maximum number of database connections
        #[arg(long, default_value = "10", env = "AWA_POOL_MAX")]
        pool_max: u32,
        /// Minimum idle connections kept open
        #[arg(long, default_value = "2", env = "AWA_POOL_MIN")]
        pool_min: u32,
        /// Seconds before an idle connection is closed
        #[arg(long, default_value = "300", env = "AWA_POOL_IDLE_TIMEOUT")]
        pool_idle_timeout: u64,
        /// Maximum lifetime of a connection in seconds
        #[arg(long, default_value = "1800", env = "AWA_POOL_MAX_LIFETIME")]
        pool_max_lifetime: u64,
        /// Seconds to wait when acquiring a connection
        #[arg(long, default_value = "10", env = "AWA_POOL_ACQUIRE_TIMEOUT")]
        pool_acquire_timeout: u64,
        /// Cache TTL for dashboard queries in seconds
        #[arg(long, default_value = "5", env = "AWA_CACHE_TTL")]
        cache_ttl: u64,
        /// Hex-encoded 32-byte key used to verify callback signatures.
        #[arg(long, env = "AWA_CALLBACK_HMAC_SECRET")]
        callback_hmac_secret: Option<String>,
        /// Force the server into read-only mode regardless of DB privilege.
        ///
        /// By default the server probes the Postgres connection and enables
        /// read-only mode only when the DB reports `transaction_read_only =
        /// on` (e.g. a read replica). Setting this flag forces read-only —
        /// mutation endpoints return 503 and `/api/capabilities` reports
        /// `read_only: true`. Useful for incident read-outs, shared debug
        /// instances, or public UI sessions against a writable DB.
        #[arg(long, env = "AWA_READ_ONLY")]
        read_only: bool,
    },
}

fn parse_callback_hmac_secret(secret: &str) -> Result<[u8; 32], String> {
    let bytes =
        hex::decode(secret).map_err(|_| "callback HMAC secret must be valid hex".to_string())?;
    <[u8; 32]>::try_from(bytes.as_slice())
        .map_err(|_| "callback HMAC secret must be exactly 32 bytes (64 hex characters)".into())
}

#[derive(Subcommand)]
enum JobCommands {
    /// Dump a single job as a detailed JSON inspection snapshot
    Dump { id: i64 },
    /// Dump one attempt as a detailed JSON inspection snapshot
    DumpRun {
        id: i64,
        /// Attempt number to inspect. Defaults to the current attempt.
        #[arg(long)]
        attempt: Option<i16>,
    },
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
enum DlqCommands {
    /// List rows in the Dead Letter Queue
    List {
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        before_id: Option<i64>,
        #[arg(long)]
        before_dlq_at: Option<DateTime<Utc>>,
        #[arg(long, default_value = "20")]
        limit: i64,
    },
    /// Show DLQ depth (total, optionally by queue)
    Depth {
        #[arg(long)]
        queue: Option<String>,
    },
    /// Retry a single DLQ'd job by id
    Retry { id: i64 },
    /// Retry DLQ rows in bulk matching the filter
    RetryBulk {
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long)]
        tag: Option<String>,
        /// Retry every row in the DLQ when no filter is provided.
        /// Required without `--kind`, `--queue`, or `--tag` to guard against
        /// accidentally reviving the entire DLQ.
        #[arg(long)]
        all: bool,
    },
    /// Move existing failed jobs (in jobs_hot) into the DLQ
    Move {
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long, default_value = "manual")]
        reason: String,
        /// Move every failed row when no filter is provided.
        #[arg(long)]
        all: bool,
    },
    /// Purge (delete) DLQ rows matching the filter
    Purge {
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long)]
        tag: Option<String>,
        /// Purge every row in the DLQ when no filter is provided.
        /// Required without `--kind`, `--queue`, or `--tag` to guard against
        /// accidentally wiping the DLQ.
        #[arg(long)]
        all: bool,
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
enum StorageCommands {
    /// Show the current storage transition state
    Status,
    /// Prepare a future storage engine without changing execution routing
    Prepare {
        #[arg(long)]
        engine: String,
        /// Optional JSON details recorded alongside the prepared engine
        #[arg(long)]
        details: Option<String>,
    },
    /// Abort a prepared or mixed-transition storage rollout before final activation
    Abort,
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
                    std::fs::write(&filename, sql_text)?;
                    println!("Extracted: {filename}");
                }
            } else {
                // Default: apply migrations to DB (sequential DDL).
                let db_url = require_pool(&cli.database_url)?;
                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&db_url)
                    .await?;
                awa_model::migrations::run(&pool).await?;
                println!("Migrations applied successfully.");
            }
        }

        // Serve gets its own tuned pool — handle it before the generic CLI pool.
        Commands::Serve {
            host,
            port,
            pool_max,
            pool_min,
            pool_idle_timeout,
            pool_max_lifetime,
            pool_acquire_timeout,
            cache_ttl,
            callback_hmac_secret,
            read_only,
        } => {
            let db_url = require_pool(&cli.database_url)?;
            let pool = PgPoolOptions::new()
                .max_connections(pool_max)
                .min_connections(pool_min)
                .idle_timeout(Duration::from_secs(pool_idle_timeout))
                .max_lifetime(Duration::from_secs(pool_max_lifetime))
                .acquire_timeout(Duration::from_secs(pool_acquire_timeout))
                .connect(&db_url)
                .await?;

            let cache_duration = Duration::from_secs(cache_ttl);
            let callback_hmac_secret = callback_hmac_secret
                .as_deref()
                .map(parse_callback_hmac_secret)
                .transpose()
                .map_err(|err| format!("invalid callback HMAC secret: {err}"))?;
            let read_only_mode = if read_only {
                awa_ui::state::ReadOnlyMode::ReadOnly
            } else {
                awa_ui::state::ReadOnlyMode::Auto
            };
            let app =
                awa_ui::router_with(pool, cache_duration, callback_hmac_secret, read_only_mode)
                    .await?;
            let addr = format!("{host}:{port}");
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            if read_only {
                tracing::info!("AWA UI listening on http://{addr} (forced read-only)");
            } else {
                tracing::info!("AWA UI listening on http://{addr}");
            }
            axum::serve(listener, app).await?;
        }

        // All remaining CLI commands are single-shot (one query, then exit)
        // so a single connection is sufficient.
        command => {
            let db_url = require_pool(&cli.database_url)?;
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&db_url)
                .await?;

            match command {
                Commands::Migrate { .. } | Commands::Serve { .. } => unreachable!(),

                Commands::Job { command } => match command {
                    JobCommands::Dump { id } => {
                        let dump = awa_model::admin::dump_job(&pool, id).await?;
                        println!("{}", serde_json::to_string_pretty(&dump)?);
                    }

                    JobCommands::DumpRun { id, attempt } => {
                        let dump = awa_model::admin::dump_run(&pool, id, attempt).await?;
                        println!("{}", serde_json::to_string_pretty(&dump)?);
                    }

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
                        let state = state.map(|s| {
                            s.parse::<awa_model::JobState>().unwrap_or_else(|e| {
                                eprintln!("{e}");
                                std::process::exit(1);
                            })
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

                Commands::Dlq { command } => match command {
                    DlqCommands::List {
                        kind,
                        queue,
                        tag,
                        before_id,
                        before_dlq_at,
                        limit,
                    } => {
                        let filter = awa_model::dlq::ListDlqFilter {
                            kind,
                            queue,
                            tag,
                            before_id,
                            before_dlq_at,
                            limit: Some(limit),
                        };
                        let rows = awa_model::dlq::list_dlq(&pool, &filter).await?;
                        if rows.is_empty() {
                            println!("DLQ is empty (no matching rows).");
                        } else {
                            println!(
                                "{:<8} {:<25} {:<10} {:<30} {:<25}",
                                "ID", "KIND", "QUEUE", "REASON", "DLQ_AT"
                            );
                            for row in &rows {
                                // Truncate by characters, not bytes: byte
                                // slicing mid-codepoint panics on Unicode
                                // reasons (e.g. an operator typing a
                                // non-ASCII note).
                                let char_count = row.reason.chars().count();
                                let reason = if char_count > 30 {
                                    let prefix: String = row.reason.chars().take(27).collect();
                                    format!("{prefix}...")
                                } else {
                                    row.reason.clone()
                                };
                                println!(
                                    "{:<8} {:<25} {:<10} {:<30} {:<25}",
                                    row.job.id, row.job.kind, row.job.queue, reason, row.dlq_at
                                );
                            }
                            println!("\n{} rows.", rows.len());
                            if let Some(last) = rows.last() {
                                println!(
                                    "Next page: --before-id {} --before-dlq-at {}",
                                    last.job.id, last.dlq_at
                                );
                            }
                        }
                    }
                    DlqCommands::Depth { queue } => {
                        if let Some(queue_name) = queue {
                            let depth = awa_model::dlq::dlq_depth(&pool, Some(&queue_name)).await?;
                            println!("{queue_name}: {depth}");
                        } else {
                            let total = awa_model::dlq::dlq_depth(&pool, None).await?;
                            let by_queue = awa_model::dlq::dlq_depth_by_queue(&pool).await?;
                            println!("Total: {total}");
                            for (q, count) in &by_queue {
                                println!("  {q}: {count}");
                            }
                        }
                    }
                    DlqCommands::Retry { id } => {
                        let opts = awa_model::dlq::RetryFromDlqOpts::default();
                        match awa_model::dlq::retry_from_dlq(&pool, id, &opts).await? {
                            Some(job) => {
                                awa_worker::AwaMetrics::from_global()
                                    .record_dlq_retried(Some(&job.queue), 1);
                                println!("Retried DLQ job {id} → job state {}", job.state);
                            }
                            None => println!("No DLQ row with id {id}"),
                        }
                    }
                    DlqCommands::RetryBulk {
                        kind,
                        queue,
                        tag,
                        all,
                    } => {
                        let filter = awa_model::dlq::ListDlqFilter {
                            kind,
                            queue: queue.clone(),
                            tag,
                            ..Default::default()
                        };
                        let count =
                            awa_model::dlq::bulk_retry_from_dlq(&pool, &filter, all).await?;
                        if count > 0 {
                            awa_worker::AwaMetrics::from_global()
                                .record_dlq_retried(queue.as_deref(), count);
                        }
                        println!("Retried {count} DLQ rows.");
                    }
                    DlqCommands::Move {
                        kind,
                        queue,
                        reason,
                        all,
                    } => {
                        let count = awa_model::dlq::bulk_move_failed_to_dlq(
                            &pool,
                            kind.as_deref(),
                            queue.as_deref(),
                            &reason,
                            all,
                        )
                        .await?;
                        // Emit the same `awa.job.dlq_moved` counter the
                        // executor uses for automatic routing, so dashboards
                        // and alerting see admin bulk moves too.
                        awa_worker::AwaMetrics::from_global().record_dlq_moved_bulk(
                            kind.as_deref(),
                            queue.as_deref(),
                            &reason,
                            count,
                        );
                        println!("Moved {count} failed jobs into the DLQ.");
                    }
                    DlqCommands::Purge {
                        kind,
                        queue,
                        tag,
                        all,
                    } => {
                        let filter = awa_model::dlq::ListDlqFilter {
                            kind,
                            queue: queue.clone(),
                            tag,
                            ..Default::default()
                        };
                        let count = awa_model::dlq::purge_dlq(&pool, &filter, all).await?;
                        if count > 0 {
                            awa_worker::AwaMetrics::from_global()
                                .record_dlq_purged(queue.as_deref(), count);
                        }
                        println!("Purged {count} DLQ rows.");
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

                Commands::Storage { command } => match command {
                    StorageCommands::Status => {
                        let status = awa_model::storage::status(&pool).await?;
                        println!("{}", serde_json::to_string_pretty(&status)?);
                    }
                    StorageCommands::Prepare { engine, details } => {
                        let details = match details {
                            Some(raw) => serde_json::from_str(&raw)?,
                            None => serde_json::json!({}),
                        };
                        let status = awa_model::storage::prepare(&pool, &engine, details).await?;
                        println!("{}", serde_json::to_string_pretty(&status)?);
                    }
                    StorageCommands::Abort => {
                        let status = awa_model::storage::abort(&pool).await?;
                        println!("{}", serde_json::to_string_pretty(&status)?);
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
                        let stats = awa_model::admin::queue_overviews(&pool).await?;
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
