use clap::Parser;
use kusto_explorer::{Cli, initialize_tracing, run};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    initialize_tracing(cli.log_json)?;
    run(cli).await
}
