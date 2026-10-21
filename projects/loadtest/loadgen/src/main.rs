use anyhow::Result;
use arrowcol_loadgen::{Cli, Command, runner, validation};
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run(arguments) => runner::run(*arguments).await,
        Command::Validate(arguments) => validation::validate(arguments).await,
        Command::MirrorDelta(arguments) => arrowcol_loadgen::mirror_delta(arguments).await,
        Command::PrintSchemas => {
            print!("{}", arrowcol_loadgen::schema::schema_ddl());
            Ok(())
        }
    }
}
