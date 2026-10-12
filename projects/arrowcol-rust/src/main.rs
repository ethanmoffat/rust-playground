use anyhow::{Context, Result};
use arrowcol_rust::{Cli, CollectorConfig, RunOutcome, run};
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = CollectorConfig::load(&cli.config)?;
    initialize_tracing(&config)?;
    if cli.validate_and_exit {
        info!(config = %cli.config.display(), "direct collector configuration is valid");
        return Ok(());
    }
    match run(config).await? {
        RunOutcome::Clean => Ok(()),
        RunOutcome::IncompleteDrain { incomplete } => {
            anyhow::bail!("collector shutdown left {incomplete} incomplete publication group(s)")
        }
    }
}

fn initialize_tracing(config: &CollectorConfig) -> Result<()> {
    let filter = EnvFilter::try_new(&config.telemetry.log_filter)
        .with_context(|| format!("parse telemetry.log_filter {:?}", config.telemetry.log_filter))?;
    if config.telemetry.json_logs {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .try_init()
            .map_err(|error| anyhow::anyhow!("initialize JSON tracing: {error}"))?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .map_err(|error| anyhow::anyhow!("initialize tracing: {error}"))?;
    }
    Ok(())
}
