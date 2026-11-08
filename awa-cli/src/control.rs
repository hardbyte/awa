use clap::Subcommand;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Subcommand)]
pub(crate) enum ControlCommands {
    /// Show requested control state and each runtime's last observation.
    Status,
    /// Stop new reclamation and housekeeping; let admitted work drain.
    Halt {
        #[arg(long, default_value = "maintenance.prune")]
        scope: String,
        #[arg(long, default_value = "drain")]
        mode: String,
        #[arg(long)]
        reason: String,
    },
    /// Lift exactly the named current halt, without waiting for drain completion.
    Resume {
        #[arg(long, default_value = "maintenance.prune")]
        scope: String,
        #[arg(long)]
        halt: Uuid,
    },
}

pub(crate) async fn run(
    pool: &PgPool,
    command: ControlCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    use awa_model::runtime_control;
    let result = match command {
        ControlCommands::Status => serde_json::to_value(runtime_control::status(pool).await?)?,
        ControlCommands::Halt {
            scope,
            mode,
            reason,
        } => serde_json::to_value(runtime_control::halt(pool, &scope, &reason, &mode).await?)?,
        ControlCommands::Resume { scope, halt } => {
            serde_json::to_value(runtime_control::resume(pool, &scope, halt).await?)?
        }
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
