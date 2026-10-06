// FluxCache - Main Entry Point
//
// Parses CLI arguments, initializes tracing, and starts the server.

use clap::Parser;
use fluxcache::config::{CliArgs, Config};
use fluxcache::server::FluxServer;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();
    let config = Config::from_args(args)?;

    // Initialize tracing
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log_level));

    if config.json_logs {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_thread_ids(true)
            .init();
    }

    tracing::info!("FluxCache v{} starting", env!("CARGO_PKG_VERSION"));
    tracing::info!(
        "Config: listen={}, admin={}, max_memory={}MB, eviction={}, shards={}, persistence={}",
        config.listen,
        config.admin_listen,
        config.max_memory / (1024 * 1024),
        config.eviction_policy,
        config.shards,
        config.persistence,
    );

    let server = FluxServer::new(config).await?;
    server.run().await?;

    Ok(())
}
