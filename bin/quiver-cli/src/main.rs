//! `quiver-cli`: generate Quiver storage from real data exactly as the
//! Arrowcol Direct collector writes it, then look inside.

mod data;
mod dir;
mod engine;
mod format;
mod generate;
mod inspect;
mod lifecycle;

use clap::{Parser, Subcommand};

/// Generate Quiver storage from real data like the Arrowcol Direct collector, and look inside it.
#[derive(Parser)]
#[command(name = "quiver-cli", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write CSV rows to Quiver like the Direct collector: an fsynced WAL entry per bundle, then segments.
    Generate(generate::GenerateArgs),
    /// Every file in a Quiver directory and what it means.
    Tree(inspect::DirArgs),
    /// Finalized segments: bundles, rows, schema streams, acknowledgements.
    Segments(inspect::DirArgs),
    /// One bundle's manifest and rows, from a segment or the WAL.
    Show(inspect::ShowArgs),
    /// WAL entries and the cursor: what is in a segment and what replays on open.
    Wal(inspect::WalArgs),
    /// Acknowledge bundles like the Delta publisher; fully acknowledged segments are deleted.
    Consume(lifecycle::ConsumeArgs),
    /// Persist bundles in a child process, then kill -9 it before a segment is finalized.
    Crash(lifecycle::CrashArgs),
    /// Open the directory like a restarted collector: replay the WAL, then finalize.
    Recover(inspect::DirArgs),
    #[command(hide = true)]
    CrashChild(lifecycle::CrashArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Generate(args) => generate::run(args).await,
        Command::Tree(args) => inspect::tree(&args),
        Command::Segments(args) => inspect::segments(&args),
        Command::Show(args) => inspect::show(&args),
        Command::Wal(args) => inspect::wal(&args),
        Command::Consume(args) => lifecycle::consume(&args).await,
        Command::Crash(args) => lifecycle::crash(&args),
        Command::Recover(args) => lifecycle::recover(&args.dir).await,
        Command::CrashChild(args) => lifecycle::crash_child(&args).await,
    }
}
