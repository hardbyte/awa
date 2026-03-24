use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[path = "../shared.rs"]
mod shared;

use shared::{build_demo_client, create_pool, prepare_schema};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let pool = create_pool().await?;
    prepare_schema(&pool).await?;

    let client = build_demo_client(pool, None)?;
    client.start().await?;

    tracing::info!("Rust demo workers running. Press Ctrl-C to stop.");
    tokio::signal::ctrl_c().await?;
    client.shutdown(Duration::from_secs(5)).await;
    Ok(())
}
