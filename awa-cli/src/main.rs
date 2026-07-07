use std::time::Duration;

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;

mod health;
mod storage_wait;

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
    /// Durable batch operation management
    BatchOps {
        #[command(subcommand)]
        command: BatchOpsCommands,
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
    /// Callback receiver subcommands
    Callbacks {
        #[command(subcommand)]
        command: CallbackCommands,
    },
    /// Cluster readiness probe: database reachable, schema migrated, fleet heartbeats
    Health {
        /// Emit the report as JSON
        #[arg(long)]
        json: bool,
        /// Seconds to wait for the database connection
        #[arg(long, default_value = "5")]
        connect_timeout: u64,
    },
}

#[derive(Subcommand)]
enum CallbackCommands {
    /// Start a callback-only receiver (no admin UI, no admin API).
    ///
    /// Use this when callbacks must be externally reachable but the admin
    /// surface must remain private. See ADR-027 and docs/http-callbacks.md.
    Serve {
        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        /// Port to listen on
        #[arg(long, default_value = "4000")]
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
        /// Hex-encoded 32-byte key used to verify callback signatures.
        /// Required unless `--allow-unsigned` is set.
        #[arg(long, env = "AWA_CALLBACK_HMAC_SECRET")]
        callback_hmac_secret: Option<String>,
        /// Path prefix the callback routes are mounted under. Defaults to
        /// `/api/callbacks`, matching the built-in `awa serve` layout.
        #[arg(
            long,
            default_value = "/api/callbacks",
            env = "AWA_CALLBACK_PATH_PREFIX"
        )]
        path_prefix: String,
        /// Accept inbound requests without `X-Awa-Signature` verification.
        /// Only safe when the receiver is reachable from a trusted network
        /// (mTLS at the load balancer, IP allow-list, private VPC, etc.).
        /// Mutually exclusive with `--callback-hmac-secret`.
        #[arg(long, env = "AWA_CALLBACK_ALLOW_UNSIGNED")]
        allow_unsigned: bool,
    },
}

fn parse_callback_hmac_secret(secret: &str) -> Result<[u8; 32], String> {
    let bytes = hex::decode(secret).map_err(|_| "callback secret must be valid hex".to_string())?;
    <[u8; 32]>::try_from(bytes.as_slice())
        .map_err(|_| "callback secret must be exactly 32 bytes (64 hex characters)".into())
}

fn parse_batch_operation_state(
    state: &str,
) -> Result<awa_model::batch_operations::BatchOperationState, Box<dyn std::error::Error>> {
    match state {
        "pending" => Ok(awa_model::batch_operations::BatchOperationState::Pending),
        "scanning" => Ok(awa_model::batch_operations::BatchOperationState::Scanning),
        "running" => Ok(awa_model::batch_operations::BatchOperationState::Running),
        "cancelling" => Ok(awa_model::batch_operations::BatchOperationState::Cancelling),
        "completed" => Ok(awa_model::batch_operations::BatchOperationState::Completed),
        "cancelled" => Ok(awa_model::batch_operations::BatchOperationState::Cancelled),
        "failed" => Ok(awa_model::batch_operations::BatchOperationState::Failed),
        _ => Err(format!("unknown batch operation state: {state}").into()),
    }
}

fn parse_batch_operation_spec(
    op_kind: &str,
    spec: &str,
) -> Result<awa_model::batch_operations::BatchOperationSpec, Box<dyn std::error::Error>> {
    match op_kind {
        "set_priority" => {
            #[derive(serde::Deserialize)]
            struct SetPrioritySpec {
                priority: i16,
            }
            let spec: SetPrioritySpec = serde_json::from_str(spec)?;
            Ok(
                awa_model::batch_operations::BatchOperationSpec::SetPriority {
                    priority: spec.priority,
                },
            )
        }
        "move_queue" => {
            #[derive(serde::Deserialize)]
            struct MoveQueueSpec {
                queue: String,
                priority: Option<i16>,
            }
            let spec: MoveQueueSpec = serde_json::from_str(spec)?;
            Ok(awa_model::batch_operations::BatchOperationSpec::MoveQueue {
                queue: spec.queue,
                priority: spec.priority,
            })
        }
        _ => Err(format!("unknown batch operation kind: {op_kind}").into()),
    }
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
    /// Retry all failed jobs by kind or queue
    RetryFailed(RetryFailedArgs),
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
    /// Move existing failed terminal rows into the DLQ
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
enum BatchOpsCommands {
    /// List batch operations
    List {
        #[arg(long)]
        state: Option<String>,
        #[arg(long, default_value = "20", value_parser = clap::value_parser!(i64).range(1..))]
        limit: i64,
    },
    /// Show one batch operation as JSON
    Get { id: uuid::Uuid },
    /// Preview a batch operation without submitting it
    Preview {
        /// Operation kind: set_priority or move_queue
        #[arg(long)]
        op_kind: String,
        /// JSON spec, e.g. '{"priority":1}' or '{"queue":"escalations"}'
        #[arg(long)]
        spec: String,
        /// JSON filter, e.g. '{"queue":"default"}'
        #[arg(long, default_value = "{}")]
        filter: String,
    },
    /// Submit a batch operation
    Submit {
        /// Operation kind: set_priority or move_queue
        #[arg(long)]
        op_kind: String,
        /// JSON spec, e.g. '{"priority":1}' or '{"queue":"escalations"}'
        #[arg(long)]
        spec: String,
        /// JSON filter, e.g. '{"kind":"send_email"}'
        #[arg(long, default_value = "{}")]
        filter: String,
        /// Required when the filter is empty
        #[arg(long)]
        all: bool,
        #[arg(long)]
        submitted_by: Option<String>,
    },
    /// Request cancellation for a running batch operation
    Cancel { id: uuid::Uuid },
    /// Purge finalized operations before a timestamp
    Purge {
        #[arg(long)]
        before: DateTime<Utc>,
        #[arg(long, default_value = "1000", value_parser = clap::value_parser!(i64).range(1..))]
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
    /// Materialize the queue-storage schema without activating routing
    PrepareQueueStorageSchema {
        #[arg(long, default_value = "awa")]
        schema: String,
        #[arg(long, default_value_t = 16)]
        queue_slot_count: u32,
        #[arg(long, default_value_t = 8)]
        lease_slot_count: u32,
        /// Drop and recreate the target schema before preparing it
        #[arg(long)]
        reset: bool,
    },
    /// Abort a prepared or mixed-transition storage rollout before final activation
    Abort,
    /// Enter mixed transition and begin routing new writes to the prepared engine
    EnterMixedTransition,
    /// Finalize the storage transition once drain and capability gates pass
    Finalize {
        /// Dry-run: print the readiness report and exit. Exits 0 when
        /// ready to finalize, exits 2 when one or more blockers remain.
        /// No SQL state change.
        #[arg(long, conflicts_with = "wait")]
        check: bool,
        /// Poll the readiness gates until they stay clear for a few
        /// consecutive checks, then invoke finalize. Optional duration
        /// cap (e.g. `10m`, `2h30m`, `90s`). Without a value, waits
        /// indefinitely. Polls every 5s by default.
        #[arg(long, value_name = "DURATION", num_args = 0..=1, default_missing_value = "")]
        wait: Option<String>,
    },
    /// Rebuild terminal-count tables from `terminal_jobs`.
    ///
    /// Use this after upgrading from a pre-#290 fleet, after any incident
    /// that may have left folded counters or pending deltas inconsistent
    /// with `terminal_jobs`, or as a routine drift-recovery step before relying
    /// on counter-fed reads for billing-grade accuracy. Wraps the rebuild in
    /// an advisory lock; best run on a quiesced fleet.
    RebuildTerminalCounters,
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

#[derive(Args)]
#[group(required = true, multiple = false)]
struct RetryFailedArgs {
    #[arg(long)]
    kind: Option<String>,
    #[arg(long)]
    queue: Option<String>,
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
                .map_err(|err| format!("invalid callback secret: {err}"))?;
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

        // Callback-only receiver. See ADR-027 and docs/http-callbacks.md.
        Commands::Callbacks {
            command:
                CallbackCommands::Serve {
                    host,
                    port,
                    pool_max,
                    pool_min,
                    pool_idle_timeout,
                    pool_max_lifetime,
                    pool_acquire_timeout,
                    callback_hmac_secret,
                    path_prefix,
                    allow_unsigned,
                },
        } => {
            let auth = match (callback_hmac_secret.as_deref(), allow_unsigned) {
                (Some(_), true) => {
                    return Err(
                        "--callback-hmac-secret and --allow-unsigned are mutually exclusive".into(),
                    );
                }
                (Some(hex), false) => {
                    let secret = parse_callback_hmac_secret(hex)
                        .map_err(|err| format!("invalid callback secret: {err}"))?;
                    awa_ui::callback_router::CallbackAuth::Signed(secret)
                }
                (None, true) => awa_ui::callback_router::CallbackAuth::Unsigned,
                (None, false) => {
                    return Err(
                        "a callback signing secret is required by default; pass --callback-hmac-secret <hex32> or, for a trusted-network deployment, --allow-unsigned"
                            .into(),
                    );
                }
            };

            let db_url = require_pool(&cli.database_url)?;
            let pool = PgPoolOptions::new()
                .max_connections(pool_max)
                .min_connections(pool_min)
                .idle_timeout(Duration::from_secs(pool_idle_timeout))
                .max_lifetime(Duration::from_secs(pool_max_lifetime))
                .acquire_timeout(Duration::from_secs(pool_acquire_timeout))
                .connect(&db_url)
                .await?;

            let config = awa_ui::callback_router::CallbackReceiverConfig { auth, path_prefix };
            let app = awa_ui::callback_router(pool, config).await?;
            let addr = format!("{host}:{port}");
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            if allow_unsigned {
                tracing::warn!(
                    "AWA callback receiver listening on http://{addr} (UNSIGNED — only safe on a trusted network)"
                );
            } else {
                tracing::info!("AWA callback receiver listening on http://{addr}");
            }
            axum::serve(listener, app).await?;
        }

        Commands::Health {
            json,
            connect_timeout,
        } => {
            let db_url = require_pool(&cli.database_url)?;
            let pool_result = PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(connect_timeout))
                .connect(&db_url)
                .await;

            let report = match pool_result {
                Ok(pool) => {
                    health::probe_with_timeout(&pool, Duration::from_secs(connect_timeout)).await
                }
                Err(_) => health::unreachable_report(),
            };

            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("{}", health::render_human(&report));
            }
            if !report.ready {
                std::process::exit(1);
            }
        }

        // Most remaining CLI commands are single-shot (one query, then exit)
        // so a small pool is sufficient. `storage prepare-queue-storage-schema`
        // is the exception: it acquires an advisory-lock connection and then
        // runs DDL via the same pool, which deadlocks at max_connections=1.
        // Allow up to 4 connections so the lock connection and the DDL
        // executor coexist.
        command => {
            let db_url = require_pool(&cli.database_url)?;
            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect(&db_url)
                .await?;

            match command {
                Commands::Migrate { .. }
                | Commands::Serve { .. }
                | Commands::Callbacks { .. }
                | Commands::Health { .. } => {
                    unreachable!()
                }

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

                    JobCommands::RetryFailed(RetryFailedArgs { kind, queue }) => {
                        let outcome = if let Some(kind) = kind {
                            awa_model::admin::retry_failed_by_kind(&pool, &kind).await?
                        } else if let Some(queue) = queue {
                            awa_model::admin::retry_failed_by_queue(&pool, &queue).await?
                        } else {
                            unreachable!("clap requires exactly one retry-failed filter")
                        };
                        let retried = outcome.retried.len() as u64;
                        let mut message = format!("Retried {retried} failed jobs");
                        if outcome.matched != retried {
                            let dropped = outcome.matched.saturating_sub(retried);
                            message.push_str(&format!(
                                " (matched {}; {dropped} raced or pruned)",
                                outcome.matched
                            ));
                        }
                        if let Some(pruned) = outcome.pruned_failed_count {
                            if pruned > 0 {
                                message.push_str(&format!(
                                    "; {pruned} failed rows have been pruned past retention \
                                     and are no longer retryable"
                                ));
                            }
                        }
                        println!("{message}");
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

                Commands::Dlq { command } => {
                    // Construct AwaMetrics once per `awa dlq` invocation —
                    // `AwaMetrics::from_global()` rebuilds the entire instrument
                    // set on each call. Cheap here since the CLI fires at most
                    // one DLQ branch per invocation, but the pattern matches
                    // the long-lived call sites.
                    let metrics = awa_worker::AwaMetrics::from_global();
                    match command {
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
                                let depth =
                                    awa_model::dlq::dlq_depth(&pool, Some(&queue_name)).await?;
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
                                    metrics.record_dlq_retried(Some(&job.queue), 1);
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
                                metrics.record_dlq_retried(queue.as_deref(), count);
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
                            metrics.record_dlq_moved_bulk(
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
                                metrics.record_dlq_purged(queue.as_deref(), count);
                            }
                            println!("Purged {count} DLQ rows.");
                        }
                    }
                }

                Commands::BatchOps { command } => match command {
                    BatchOpsCommands::List { state, limit } => {
                        let state = state
                            .as_deref()
                            .map(parse_batch_operation_state)
                            .transpose()?;
                        let operations = awa_model::batch_operations::list_batch_operations(
                            &pool,
                            &awa_model::batch_operations::ListBatchOperationsFilter {
                                state,
                                limit: Some(limit),
                            },
                        )
                        .await?;
                        println!("{}", serde_json::to_string_pretty(&operations)?);
                    }
                    BatchOpsCommands::Get { id } => {
                        let operation =
                            awa_model::batch_operations::get_batch_operation(&pool, id).await?;
                        println!("{}", serde_json::to_string_pretty(&operation)?);
                    }
                    BatchOpsCommands::Preview {
                        op_kind,
                        spec,
                        filter,
                    } => {
                        let spec = parse_batch_operation_spec(&op_kind, &spec)?;
                        let filter: awa_model::batch_operations::BatchOperationFilter =
                            serde_json::from_str(&filter)?;
                        let preview = awa_model::batch_operations::preview_batch_operation(
                            &pool, spec, filter,
                        )
                        .await?;
                        println!("{}", serde_json::to_string_pretty(&preview)?);
                    }
                    BatchOpsCommands::Submit {
                        op_kind,
                        spec,
                        filter,
                        all,
                        submitted_by,
                    } => {
                        let spec = parse_batch_operation_spec(&op_kind, &spec)?;
                        let filter: awa_model::batch_operations::BatchOperationFilter =
                            serde_json::from_str(&filter)?;
                        let operation = awa_model::batch_operations::submit_batch_operation(
                            &pool,
                            awa_model::batch_operations::SubmitBatchOperation {
                                spec,
                                filter,
                                submitted_by,
                                allow_all: all,
                            },
                        )
                        .await?;
                        println!("{}", serde_json::to_string_pretty(&operation)?);
                    }
                    BatchOpsCommands::Cancel { id } => {
                        let operation =
                            awa_model::batch_operations::request_batch_operation_cancellation(
                                &pool, id,
                            )
                            .await?;
                        println!("{}", serde_json::to_string_pretty(&operation)?);
                    }
                    BatchOpsCommands::Purge { before, limit } => {
                        let purged = awa_model::batch_operations::purge_batch_operations_before(
                            &pool, before, limit,
                        )
                        .await?;
                        println!("Purged {purged} batch operations.");
                    }
                },

                Commands::Cron { command } => match command {
                    CronCommands::List => {
                        let schedules = awa_model::cron::list_cron_jobs(&pool).await?;
                        if schedules.is_empty() {
                            println!("No cron job schedules found.");
                        } else {
                            println!(
                                "{:<25} {:<20} {:<12} {:<12} {:<25} {:<10}",
                                "NAME", "CRON", "TIMEZONE", "MISSED", "KIND", "QUEUE"
                            );
                            for s in &schedules {
                                println!(
                                    "{:<25} {:<20} {:<12} {:<12} {:<25} {:<10}",
                                    s.name,
                                    s.cron_expr,
                                    s.timezone,
                                    s.missed_fire_policy,
                                    s.kind,
                                    s.queue,
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
                        let report = awa_model::storage::status_report(&pool).await?;
                        println!("{}", serde_json::to_string_pretty(&report)?);
                    }
                    StorageCommands::Prepare { engine, details } => {
                        // Auto-fill `details.schema` for queue-storage when the
                        // operator didn't pass --details. Without this, v011's
                        // SQL fallback would resolve to the historical
                        // `awa_exp` default, mismatching the runtime's
                        // configured schema (`awa` in 0.6) and breaking
                        // `enter-mixed-transition`. Operators who pass
                        // --details with their own schema name override.
                        let details = match details {
                            Some(raw) => serde_json::from_str(&raw)?,
                            None if engine == "queue_storage" => serde_json::json!({
                                "schema": awa_model::QueueStorageConfig::default().schema,
                            }),
                            None => serde_json::json!({}),
                        };
                        awa_model::storage::prepare(&pool, &engine, details).await?;
                        let report = awa_model::storage::status_report(&pool).await?;
                        println!("{}", serde_json::to_string_pretty(&report)?);
                    }
                    StorageCommands::PrepareQueueStorageSchema {
                        schema,
                        queue_slot_count,
                        lease_slot_count,
                        reset,
                    } => {
                        // The default `awa` schema also holds the canonical
                        // migration tables (schema_version, runtime_instances,
                        // storage_transition_state, ...). `DROP SCHEMA awa
                        // CASCADE` would take them with it and leave the
                        // database unrecoverable. See
                        // docs/queue-storage-substrate.md.
                        if reset && schema == "awa" {
                            return Err(
                                "Refusing to DROP SCHEMA awa CASCADE — schema 'awa' is the \
                                 default migration-owned queue-storage substrate and also \
                                 contains the canonical migration tables (schema_version, \
                                 runtime_instances, storage_transition_state, etc.). \
                                 Use --schema <other> for a throwaway substrate, or \
                                 'awa storage abort' to rewind an in-flight transition."
                                    .into(),
                            );
                        }
                        let store = awa_model::QueueStorage::new(awa_model::QueueStorageConfig {
                            schema: schema.clone(),
                            queue_slot_count: queue_slot_count as usize,
                            lease_slot_count: lease_slot_count as usize,
                            ..Default::default()
                        })?;
                        if reset {
                            sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                                .execute(&pool)
                                .await?;
                        }
                        store.prepare_schema(&pool).await?;
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "schema": schema,
                                "queue_slot_count": queue_slot_count,
                                "lease_slot_count": lease_slot_count,
                                "routing_changed": false,
                            }))?
                        );
                    }
                    StorageCommands::Abort => {
                        awa_model::storage::abort(&pool).await?;
                        let report = awa_model::storage::status_report(&pool).await?;
                        println!("{}", serde_json::to_string_pretty(&report)?);
                    }
                    StorageCommands::EnterMixedTransition => {
                        awa_model::storage::enter_mixed_transition(&pool).await?;
                        let report = awa_model::storage::status_report(&pool).await?;
                        println!("{}", serde_json::to_string_pretty(&report)?);
                    }
                    StorageCommands::Finalize { check, wait } => {
                        if check {
                            // Dry-run: print the same readiness report as
                            // `awa storage status` plus a concise blocker
                            // summary, and exit non-zero (2) if blocked.
                            let report = awa_model::storage::status_report(&pool).await?;
                            println!("{}", serde_json::to_string_pretty(&report)?);
                            if report.can_finalize {
                                eprintln!("storage finalize: ready");
                            } else {
                                eprintln!(
                                    "storage finalize: blocked ({} blocker{})",
                                    report.finalize_blockers.len(),
                                    if report.finalize_blockers.len() == 1 {
                                        ""
                                    } else {
                                        "s"
                                    }
                                );
                                for blocker in &report.finalize_blockers {
                                    eprintln!("  - {blocker}");
                                }
                                std::process::exit(2);
                            }
                        } else if let Some(wait_arg) = wait {
                            let cap = if wait_arg.is_empty() {
                                None
                            } else {
                                Some(storage_wait::parse_duration(&wait_arg).map_err(|e| {
                                    format!("--wait: invalid duration {wait_arg:?}: {e}")
                                })?)
                            };
                            match storage_wait::wait_for_finalize(&pool, cap).await? {
                                storage_wait::WaitOutcome::Finalized(report) => {
                                    println!("{}", serde_json::to_string_pretty(&report)?);
                                }
                                storage_wait::WaitOutcome::TimedOut(report) => {
                                    println!("{}", serde_json::to_string_pretty(&report)?);
                                    eprintln!(
                                        "storage finalize --wait: timed out after {:?} with {} blocker(s)",
                                        cap.unwrap_or_default(),
                                        report.finalize_blockers.len()
                                    );
                                    for blocker in &report.finalize_blockers {
                                        eprintln!("  - {blocker}");
                                    }
                                    std::process::exit(2);
                                }
                            }
                        } else {
                            awa_model::storage::finalize(&pool).await?;
                            let report = awa_model::storage::status_report(&pool).await?;
                            println!("{}", serde_json::to_string_pretty(&report)?);
                        }
                    }
                    StorageCommands::RebuildTerminalCounters => {
                        // Resolve the live queue-storage schema from the
                        // transition state; no point letting the operator
                        // pass it as a flag and risk targeting an inactive
                        // engine.
                        let schema = awa_model::QueueStorage::active_schema(&pool)
                            .await?
                            .ok_or_else(|| {
                                "no active queue-storage schema; nothing to rebuild".to_string()
                            })?;
                        let store = awa_model::QueueStorage::from_existing_schema(&schema)?;
                        let inserted = store.rebuild_terminal_counters(&pool).await?;
                        eprintln!(
                            "rebuilt terminal counters in schema '{schema}': \
                             {inserted} folded counter row(s) populated from terminal_jobs; \
                             pending deltas cleared"
                        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn job_retry_failed_requires_exactly_one_filter() {
        assert!(
            Cli::try_parse_from(["awa", "job", "retry-failed", "--kind", "email"]).is_ok(),
            "--kind alone should be accepted"
        );
        assert!(
            Cli::try_parse_from(["awa", "job", "retry-failed", "--queue", "critical"]).is_ok(),
            "--queue alone should be accepted"
        );
        assert!(
            Cli::try_parse_from(["awa", "job", "retry-failed"]).is_err(),
            "retry-failed must require a filter"
        );
        assert!(
            Cli::try_parse_from([
                "awa",
                "job",
                "retry-failed",
                "--kind",
                "email",
                "--queue",
                "critical",
            ])
            .is_err(),
            "retry-failed must reject ambiguous filters"
        );
    }
}
