use std::time::Duration;

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;

mod context;
mod health;
mod storage_wait;

#[derive(Parser)]
#[command(
    name = "awa",
    version,
    about = "Awa — Postgres-native background job queue"
)]
struct Cli {
    /// Database URL (not required for migrate --sql without --pending).
    ///
    /// Resolution order: --database-url > --context/AWA_CONTEXT >
    /// DATABASE_URL env > default_context from ~/.config/awa/config.toml.
    #[arg(long)]
    database_url: Option<String>,

    /// Named context from ~/.config/awa/config.toml (see `awa context list`)
    #[arg(long, global = true, env = "AWA_CONTEXT")]
    context: Option<String>,

    /// Skip the interactive confirmation for contexts marked
    /// `production = true` (for automation)
    #[arg(long, global = true)]
    yes: bool,

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
        /// Skip migration compatibility checks against live runtimes.
        ///
        /// A migration may require a minimum runtime version or, when no
        /// rolling-compatible shape exists, no live runtimes. This overrides
        /// those checks; use only after independently verifying compatibility.
        #[arg(long)]
        allow_live_runtimes: bool,
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
        /// Human-readable name identifying this instance/database.
        ///
        /// The UI is single-backend by design — run one `awa serve` per
        /// database. This name is exposed via `/api/capabilities` and
        /// rendered in the header, browser tab title, and favicon so
        /// operators with several instances open can tell them apart.
        #[arg(long, env = "AWA_INSTANCE_NAME")]
        instance_name: Option<String>,
        /// Accent color for this instance as a CSS hex value
        /// (e.g. '#0ea5e9'). Tints the UI header badge and favicon.
        #[arg(long, env = "AWA_INSTANCE_COLOR")]
        instance_color: Option<String>,
        /// Link to a peer Awa UI, as NAME=URL (repeatable).
        ///
        /// Rendered as plain links in the UI header — a zero-data-plane
        /// "switcher" between per-database instances. Example:
        /// `--peer alloydb=https://awa-alloydb.internal`.
        #[arg(long = "peer", value_name = "NAME=URL")]
        peer: Vec<String>,
    },
    /// Callback receiver subcommands
    Callbacks {
        #[command(subcommand)]
        command: CallbackCommands,
    },
    /// Inspect named contexts from ~/.config/awa/config.toml
    Context {
        #[command(subcommand)]
        command: ContextCommands,
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

#[derive(Subcommand)]
enum ContextCommands {
    /// List contexts defined in the awa config file
    List,
    /// Show one context in detail (defaults to default_context)
    Show { name: Option<String> },
}

/// Classify a command as read-only or mutating for context resolution.
///
/// Read-only commands may fall back to `DATABASE_URL` or `default_context`;
/// mutating commands require an explicit `--context` or `--database-url`
/// once more than one context is defined, and are confirmation-gated on
/// contexts marked `production = true`.
fn access_for(command: &Commands) -> context::Access {
    use context::Access::{Mutating, ReadOnly};
    match command {
        // `migrate` only mutates when it applies; --sql / --extract-to
        // render SQL (the --pending probe is a read).
        Commands::Migrate {
            sql, extract_to, ..
        } => {
            if *sql || extract_to.is_some() {
                ReadOnly
            } else {
                Mutating
            }
        }
        // Long-running servers; the UI has its own read-only enforcement.
        Commands::Serve { .. } | Commands::Callbacks { .. } => ReadOnly,
        Commands::Health { .. } | Commands::Context { .. } => ReadOnly,
        Commands::Job { command } => match command {
            JobCommands::Dump { .. } | JobCommands::DumpRun { .. } | JobCommands::List { .. } => {
                ReadOnly
            }
            JobCommands::Retry { .. }
            | JobCommands::Cancel { .. }
            | JobCommands::RetryFailed(_)
            | JobCommands::Discard { .. } => Mutating,
        },
        Commands::Queue { command } => match command {
            QueueCommands::Stats => ReadOnly,
            QueueCommands::Overrides {
                command: OverrideCommands::Show { .. },
            } => ReadOnly,
            QueueCommands::Pause { .. }
            | QueueCommands::Resume { .. }
            | QueueCommands::Drain { .. }
            | QueueCommands::Overrides { .. } => Mutating,
        },
        Commands::Cron { command } => match command {
            CronCommands::List => ReadOnly,
            CronCommands::Remove { .. } => Mutating,
        },
        Commands::Storage { command } => match command {
            StorageCommands::Status => ReadOnly,
            StorageCommands::Finalize { check: true, .. } => ReadOnly,
            StorageCommands::FlipRingAuthority { check: true, .. } => ReadOnly,
            StorageCommands::Prepare { .. }
            | StorageCommands::PrepareQueueStorageSchema { .. }
            | StorageCommands::Abort
            | StorageCommands::EnterMixedTransition
            | StorageCommands::Finalize { .. }
            | StorageCommands::RebuildTerminalCounters
            | StorageCommands::FlipRingAuthority { .. } => Mutating,
        },
        Commands::Dlq { command } => match command {
            DlqCommands::List { .. } | DlqCommands::Depth { .. } => ReadOnly,
            DlqCommands::Retry { .. }
            | DlqCommands::RetryBulk { .. }
            | DlqCommands::Move { .. }
            | DlqCommands::Purge { .. } => Mutating,
        },
        Commands::BatchOps { command } => match command {
            BatchOpsCommands::List { .. }
            | BatchOpsCommands::Get { .. }
            | BatchOpsCommands::Preview { .. } => ReadOnly,
            BatchOpsCommands::Submit { .. }
            | BatchOpsCommands::Cancel { .. }
            | BatchOpsCommands::Purge { .. } => Mutating,
        },
    }
}

/// Interactive y/N gate for mutating commands against a `production = true`
/// context. `--yes` skips it; a non-interactive stdin without `--yes`
/// refuses rather than hanging (or silently proceeding) in automation.
fn confirm_production(
    target: &context::ResolvedTarget,
    assume_yes: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{BufRead, IsTerminal, Write};

    if assume_yes {
        return Ok(());
    }
    let name = target.context.as_deref().unwrap_or("<unnamed>");
    if !std::io::stdin().is_terminal() {
        return Err(format!(
            "context '{name}' is marked production = true and this command mutates the \
             database; re-run with --yes to proceed non-interactively"
        )
        .into());
    }
    eprint!("context '{name}' is marked production = true — proceed? [y/N] ");
    std::io::stderr().flush()?;
    let mut answer = String::new();
    std::io::stdin().lock().read_line(&mut answer)?;
    let answer = answer.trim().to_ascii_lowercase();
    if answer == "y" || answer == "yes" {
        Ok(())
    } else {
        Err(format!("aborted: not confirmed for production context '{name}'").into())
    }
}

/// Validate an instance accent color: '#' followed by 3, 4, 6, or 8 hex
/// digits. Rejecting anything else keeps arbitrary strings out of the UI's
/// inline styles.
fn parse_instance_color(color: &str) -> Result<String, String> {
    let trimmed = color.trim();
    let hex = trimmed.strip_prefix('#').ok_or_else(|| {
        format!("instance color must be a hex value like '#0ea5e9', got {trimmed:?}")
    })?;
    let valid_len = matches!(hex.len(), 3 | 4 | 6 | 8);
    if !valid_len || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!(
            "instance color must be '#' followed by 3, 4, 6, or 8 hex digits, got {trimmed:?}"
        ));
    }
    Ok(trimmed.to_string())
}

/// Parse a `--peer NAME=URL` spec into a link rendered by the UI header.
fn parse_peer_spec(spec: &str) -> Result<awa_ui::state::PeerLink, String> {
    let (name, url) = spec
        .split_once('=')
        .ok_or_else(|| format!("--peer expects NAME=URL, got {spec:?}"))?;
    let name = name.trim();
    let url = url.trim();
    if name.is_empty() {
        return Err(format!("--peer name must not be empty in {spec:?}"));
    }
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(format!(
            "--peer URL must start with http:// or https://, got {url:?}"
        ));
    }
    Ok(awa_ui::state::PeerLink {
        name: name.to_string(),
        url: url.to_string(),
    })
}

/// `awa context list|show` — config inspection, no database involved.
fn run_context_command(command: &ContextCommands) -> Result<(), Box<dyn std::error::Error>> {
    let loaded = context::load_from_default_path()?;
    let Some(loaded) = loaded else {
        let path = context::default_config_path()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "~/.config/awa/config.toml".to_string());
        println!("No awa config file found at {path}.");
        println!("Define [contexts.<name>] tables there to name your databases — see docs/configuration.md.");
        return Ok(());
    };
    if let Some(warning) = &loaded.warning {
        eprintln!("{warning}");
    }
    let config = &loaded.config;

    // Resolved, password-stripped display target for one context.
    let display_target = |entry: &context::ContextEntry| -> String {
        if let Some(url) = &entry.url {
            return context::redact_url(url);
        }
        let var = entry.url_env.as_deref().unwrap_or_default();
        match std::env::var(var) {
            Ok(url) => context::redact_url(&url),
            Err(_) => format!("(env {var} unset)"),
        }
    };

    match command {
        ContextCommands::List => {
            println!("# {}", loaded.path.display());
            if config.contexts.is_empty() {
                println!("No contexts defined.");
                return Ok(());
            }
            println!(
                "{:<2} {:<20} {:<11} {:<28} TARGET",
                "", "NAME", "PRODUCTION", "SOURCE"
            );
            for (name, entry) in &config.contexts {
                let default_marker = if config.default_context.as_deref() == Some(name) {
                    "*"
                } else {
                    ""
                };
                let source = entry
                    .url_env
                    .as_deref()
                    .map(|var| format!("env {var}"))
                    .unwrap_or_else(|| "url (in config file)".to_string());
                println!(
                    "{:<2} {:<20} {:<11} {:<28} {}",
                    default_marker,
                    name,
                    if entry.production { "yes" } else { "no" },
                    source,
                    display_target(entry),
                );
            }
            println!("\n* = default_context (honored by read-only commands only)");
        }
        ContextCommands::Show { name } => {
            let name = name
                .as_deref()
                .or(config.default_context.as_deref())
                .ok_or("no context name given and no default_context is set")?;
            let entry = config.contexts.get(name).ok_or_else(|| {
                format!("unknown context '{name}' — run `awa context list` to see defined contexts")
            })?;
            println!("name:        {name}");
            println!(
                "default:     {}",
                if config.default_context.as_deref() == Some(name) {
                    "yes (read-only commands only)"
                } else {
                    "no"
                }
            );
            println!(
                "production:  {}",
                if entry.production {
                    "yes (mutations require confirmation or --yes)"
                } else {
                    "no"
                }
            );
            match &entry.url_env {
                Some(var) => println!("source:      env {var}"),
                None => println!("source:      url (in config file)"),
            }
            println!("target:      {}", display_target(entry));
            println!("config:      {}", loaded.path.display());
        }
    }
    Ok(())
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
    /// Flip the #371 ring-cursor authority `columns -> ledger` for a
    /// queue-storage schema (one-way, idempotent).
    ///
    /// After a rolling 0.6 -> 0.7 upgrade the ring cursors run in `columns`
    /// (compat) mode so a mixed fleet is safe. Once every worker is on 0.7,
    /// flipping to `ledger` mode enables the dead-tuple-free rotation of
    /// #371. Refuses if any fresh-heartbeat runtime is not known to be
    /// flip-aware (an old or pre-flip binary may still be reading the compat
    /// columns) unless `--force` is given. The maintenance loop also
    /// auto-flips once the fleet has been fully 0.7 for a stable period.
    FlipRingAuthority {
        /// Queue-storage schema to flip.
        #[arg(long, default_value = "awa")]
        schema: String,
        /// Print the authority + fleet flip-readiness and exit without
        /// changing anything. Exits 0 when a flip would be allowed, 2 when
        /// blocking runtimes remain.
        #[arg(long, conflicts_with = "force")]
        check: bool,
        /// Flip even while fresh-heartbeat runtimes are not known to be
        /// flip-aware. Only safe once you have confirmed no pre-flip binary
        /// is live.
        #[arg(long)]
        force: bool,
    },
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
    /// Manage live runtime overrides for a queue (applied without restarts)
    Overrides {
        #[command(subcommand)]
        command: OverrideCommands,
    },
}

#[derive(Subcommand)]
enum OverrideCommands {
    /// Set the override state for a queue. Flags you pass become the new
    /// override set; omitted flags are cleared (whole-state semantics —
    /// run `show` first when adjusting one knob). Workers apply changes
    /// within their refresh cadence (~10s), no restart needed.
    Set {
        queue: String,
        /// Override the dispatch poll interval, in milliseconds
        #[arg(long)]
        poll_interval_ms: Option<i32>,
        /// Override the claim batch size
        #[arg(long)]
        claim_batch_size: Option<i32>,
        /// Override the sustained rate limit (jobs/second; only retunes a
        /// queue built with a rate limit)
        #[arg(long)]
        rate_limit: Option<f64>,
        /// Override the per-attempt deadline, in milliseconds (cannot cross
        /// the zero boundary — claim mode is fixed at startup)
        #[arg(long)]
        deadline_ms: Option<i64>,
    },
    /// Clear every override — workers revert to builder-configured values
    Clear { queue: String },
    /// Show the current override state
    Show {
        queue: String,
        #[arg(long)]
        json: bool,
    },
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
async fn main() {
    // Errors render through Display, not the derive(Debug) shape: operator
    // guidance (e.g. the ADR-037 migrate-gate message naming the finalize
    // steps) lives in the Display impls and must reach the terminal.
    if let Err(err) = run().await {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    // `awa context ...` inspects the config file only — no database.
    if let Commands::Context { command } = &cli.command {
        return run_context_command(command);
    }

    // An explicit --database-url is the documented highest-priority override
    // and must stay usable as the escape hatch even when the context config
    // itself is broken (typo, permissions): downgrade a load failure to a
    // warning in that case rather than aborting. Without the flag, a broken
    // config is fatal — silently ignoring it could route a command at the
    // wrong database via the DATABASE_URL fallback.
    let loaded_config = match context::load_from_default_path() {
        Ok(loaded) => loaded,
        Err(error) if cli.database_url.is_some() => {
            eprintln!(
                "warning: ignoring unreadable context config (--database-url given): {error}"
            );
            None
        }
        Err(error) => return Err(error),
    };
    if let Some(warning) = loaded_config.as_ref().and_then(|l| l.warning.as_deref()) {
        eprintln!("{warning}");
    }
    let access = access_for(&cli.command);

    // Resolve the database target lazily — some commands (migrate --sql)
    // don't need a DB. Resolution order: --database-url > --context/
    // AWA_CONTEXT > DATABASE_URL env > default_context, with the read/write
    // asymmetry and production gate enforced in `context` (#437). The
    // password-stripped target is echoed to stderr before acting. Memoized
    // so commands that connect twice (migrate --pending) echo and confirm
    // once.
    let resolved_db: std::cell::OnceCell<String> = std::cell::OnceCell::new();
    let resolve_db = || -> Result<String, Box<dyn std::error::Error>> {
        if let Some(url) = resolved_db.get() {
            return Ok(url.clone());
        }
        let env_database_url = std::env::var("DATABASE_URL").ok();
        let target = context::resolve_target(
            cli.database_url.as_deref(),
            cli.context.as_deref(),
            env_database_url.as_deref(),
            loaded_config.as_ref().map(|l| &l.config),
            access,
            |var| std::env::var(var).ok(),
        )?;
        eprintln!("{}", target.describe());
        if access == context::Access::Mutating && target.production {
            confirm_production(&target, cli.yes)?;
        }
        Ok(resolved_db.get_or_init(|| target.url).clone())
    };

    match cli.command {
        Commands::Migrate {
            extract_to,
            sql,
            from,
            to,
            version,
            pending,
            allow_live_runtimes,
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
                let db_url = resolve_db()?;
                let pool = PgPoolOptions::new()
                    .max_connections(2)
                    .connect(&db_url)
                    .await?;
                // `current_version` normalizes legacy pre-0.4 numbering (fine
                // on this mutation-adjacent path, per #392) but is now bounded
                // so it never rewrites a schema newer than the binary — it
                // returns the raw version untouched. Refuse that loudly rather
                // than silently reporting "up to date" (#392).
                let db_version = awa_model::migrations::current_version(&pool).await?;
                if db_version > current_version {
                    eprintln!(
                        "error: schema is at v{db_version} but this binary supports up to \
                         v{current_version} — upgrade the awa binaries (see \
                         docs/upgrade-0.6-to-0.7.md); refusing to migrate a newer schema"
                    );
                    std::process::exit(1);
                }
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
                // Default: apply migrations to DB (sequential DDL). The plan
                // line and per-migration timing are emitted as tracing `info!`
                // events from `apply_migrations` (see the subscriber init).
                let db_url = resolve_db()?;
                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&db_url)
                    .await?;
                let options = awa_model::migrations::MigrateOptions {
                    allow_live_runtimes,
                };
                awa_model::migrations::run_with_options(&pool, options).await?;
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
            instance_name,
            instance_color,
            peer,
        } => {
            // Validate identity flags before touching the database so a
            // typo'd color or peer spec fails fast.
            let instance = awa_ui::state::InstanceIdentity {
                name: instance_name
                    .map(|name| name.trim().to_string())
                    .filter(|name| !name.is_empty()),
                color: instance_color
                    .as_deref()
                    .map(parse_instance_color)
                    .transpose()?,
                peers: peer
                    .iter()
                    .map(|spec| parse_peer_spec(spec))
                    .collect::<Result<Vec<_>, _>>()?,
            };
            let db_url = resolve_db()?;
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
            let app = awa_ui::router_with_identity(
                pool,
                cache_duration,
                callback_hmac_secret,
                read_only_mode,
                instance,
            )
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

            let db_url = resolve_db()?;
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
            let db_url = resolve_db()?;
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
            let db_url = resolve_db()?;
            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect(&db_url)
                .await?;

            match command {
                Commands::Migrate { .. }
                | Commands::Serve { .. }
                | Commands::Callbacks { .. }
                | Commands::Health { .. }
                | Commands::Context { .. } => {
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
                                    job.kind,
                                    job.queue,
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
                    StorageCommands::FlipRingAuthority {
                        schema,
                        check,
                        force,
                    } => {
                        let status =
                            awa_model::storage::ring_authority_status(&pool, &schema).await?;
                        if check {
                            println!(
                                "schema '{schema}': authority={} live={} flip_aware={} blocking={}",
                                status.authority,
                                status.live_instances,
                                status.flip_aware_instances,
                                status.blocking_instances,
                            );
                            if status.authority == "ledger" {
                                println!("already flipped to ledger authority");
                            } else if status.blocking_instances > 0 {
                                eprintln!(
                                    "{} fresh runtime(s) are not known to be flip-aware; \
                                     roll the whole fleet to 0.7 (or use --force)",
                                    status.blocking_instances
                                );
                                std::process::exit(2);
                            } else {
                                println!("ready to flip: no blocking runtimes");
                            }
                        } else {
                            let result =
                                awa_model::storage::flip_ring_authority(&pool, &schema, force)
                                    .await?;
                            eprintln!(
                                "ring-cursor authority for schema '{schema}' is now '{result}' \
                                 (was '{}'){}",
                                status.authority,
                                if force { " [--force]" } else { "" },
                            );
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
                    QueueCommands::Overrides { command } => match command {
                        OverrideCommands::Set {
                            queue,
                            poll_interval_ms,
                            claim_batch_size,
                            rate_limit,
                            deadline_ms,
                        } => {
                            let overrides = awa_model::admin::QueueRuntimeOverrides {
                                poll_interval_ms,
                                claim_batch_size,
                                rate_limit,
                                deadline_ms,
                            };
                            awa_model::admin::set_queue_runtime_overrides(
                                &pool, &queue, &overrides,
                            )
                            .await?;
                            println!(
                                "Overrides set for queue '{queue}': {}",
                                serde_json::to_string(&overrides)?
                            );
                            println!("Workers apply them within their refresh cadence (~10s).");
                        }
                        OverrideCommands::Clear { queue } => {
                            awa_model::admin::clear_queue_runtime_overrides(&pool, &queue).await?;
                            println!("Overrides cleared for queue '{queue}' — workers revert to builder values.");
                        }
                        OverrideCommands::Show { queue, json } => {
                            let overrides =
                                awa_model::admin::queue_runtime_overrides(&pool, &queue).await?;
                            match overrides {
                                Some(overrides) if json => {
                                    println!("{}", serde_json::to_string_pretty(&overrides)?)
                                }
                                Some(overrides) => {
                                    println!("queue '{queue}' overrides:");
                                    println!(
                                        "  poll_interval_ms:  {:?}",
                                        overrides.poll_interval_ms
                                    );
                                    println!(
                                        "  claim_batch_size:  {:?}",
                                        overrides.claim_batch_size
                                    );
                                    println!("  rate_limit:        {:?}", overrides.rate_limit);
                                    println!("  deadline_ms:       {:?}", overrides.deadline_ms);
                                }
                                None if json => println!("null"),
                                None => println!(
                                    "queue '{queue}' has no control-plane row (no overrides)."
                                ),
                            }
                        }
                    },
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

    #[test]
    fn serve_accepts_instance_identity_flags() {
        let cli = Cli::try_parse_from([
            "awa",
            "serve",
            "--instance-name",
            "cloudsql-prod",
            "--instance-color",
            "#0ea5e9",
            "--peer",
            "alloydb=https://awa-alloydb.internal",
            "--peer",
            "staging=http://localhost:3001",
        ])
        .expect("serve should accept identity flags");
        match cli.command {
            Commands::Serve {
                instance_name,
                instance_color,
                peer,
                ..
            } => {
                assert_eq!(instance_name.as_deref(), Some("cloudsql-prod"));
                assert_eq!(instance_color.as_deref(), Some("#0ea5e9"));
                assert_eq!(peer.len(), 2);
            }
            _ => panic!("expected serve"),
        }
    }

    #[test]
    fn context_flag_is_global_and_yes_is_global() {
        let cli = Cli::try_parse_from([
            "awa",
            "queue",
            "drain",
            "default",
            "--context",
            "alloydb",
            "--yes",
        ])
        .expect("--context/--yes should parse after the subcommand");
        assert_eq!(cli.context.as_deref(), Some("alloydb"));
        assert!(cli.yes);
    }

    #[test]
    fn instance_color_validation() {
        assert_eq!(parse_instance_color("#0ea5e9").unwrap(), "#0ea5e9");
        assert_eq!(parse_instance_color(" #abc ").unwrap(), "#abc");
        assert!(
            parse_instance_color("#abcde").is_err(),
            "5 hex digits invalid"
        );
        assert!(
            parse_instance_color("red").is_err(),
            "named colors rejected"
        );
        assert!(
            parse_instance_color("#0ea5e9;background:url(x)").is_err(),
            "style injection rejected"
        );
    }

    #[test]
    fn peer_spec_parsing() {
        let peer = parse_peer_spec("alloydb=https://awa-alloydb.internal").unwrap();
        assert_eq!(peer.name, "alloydb");
        assert_eq!(peer.url, "https://awa-alloydb.internal");
        assert!(parse_peer_spec("no-equals").is_err());
        assert!(
            parse_peer_spec("=https://x").is_err(),
            "empty name rejected"
        );
        assert!(
            parse_peer_spec("x=javascript:alert(1)").is_err(),
            "non-http scheme rejected"
        );
    }

    #[test]
    fn access_classification_matches_the_safety_matrix() {
        use context::Access::{Mutating, ReadOnly};

        let access = |args: &[&str]| {
            let cli = Cli::try_parse_from([&["awa"], args].concat())
                .unwrap_or_else(|e| panic!("args {args:?} should parse: {e}"));
            access_for(&cli.command)
        };

        // Read-only surface honors default_context / DATABASE_URL.
        assert_eq!(access(&["job", "list"]), ReadOnly);
        assert_eq!(access(&["queue", "stats"]), ReadOnly);
        assert_eq!(access(&["dlq", "depth"]), ReadOnly);
        assert_eq!(access(&["health"]), ReadOnly);
        assert_eq!(access(&["storage", "status"]), ReadOnly);
        assert_eq!(access(&["storage", "finalize", "--check"]), ReadOnly);
        assert_eq!(
            access(&["storage", "flip-ring-authority", "--check"]),
            ReadOnly
        );
        assert_eq!(access(&["migrate", "--sql"]), ReadOnly);
        assert_eq!(
            access(&[
                "batch-ops",
                "preview",
                "--op-kind",
                "set_priority",
                "--spec",
                "{}"
            ]),
            ReadOnly
        );
        assert_eq!(access(&["queue", "overrides", "show", "q"]), ReadOnly);

        // The issue's destructive set is all mutating.
        assert_eq!(access(&["migrate"]), Mutating);
        assert_eq!(access(&["migrate", "--pending"]), Mutating);
        assert_eq!(access(&["storage", "enter-mixed-transition"]), Mutating);
        assert_eq!(access(&["storage", "finalize"]), Mutating);
        assert_eq!(access(&["storage", "flip-ring-authority"]), Mutating);
        assert_eq!(access(&["dlq", "retry", "1"]), Mutating);
        assert_eq!(access(&["dlq", "purge", "--all"]), Mutating);
        assert_eq!(access(&["dlq", "move", "--all"]), Mutating);
        assert_eq!(access(&["queue", "drain", "q"]), Mutating);
        assert_eq!(access(&["queue", "pause", "q"]), Mutating);
        assert_eq!(
            access(&[
                "batch-ops",
                "submit",
                "--op-kind",
                "set_priority",
                "--spec",
                "{}",
                "--all"
            ]),
            Mutating
        );
        assert_eq!(access(&["cron", "remove", "nightly"]), Mutating);

        // Other write commands are held to the same rules.
        assert_eq!(access(&["job", "retry", "1"]), Mutating);
        assert_eq!(access(&["job", "cancel", "1"]), Mutating);
        assert_eq!(access(&["queue", "overrides", "clear", "q"]), Mutating);
        assert_eq!(
            access(&[
                "batch-ops",
                "cancel",
                "00000000-0000-0000-0000-000000000000"
            ]),
            Mutating
        );
    }
}
