//! Commands that open the directory as the collector does: `consume`,
//! `crash`, and `recover`. They only accept directories quiver-cli created.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use arrowcol_buffer::DELTA_SUBSCRIBER;
use arrowcol_protocol::{CollectorInstanceId, LeaseId};
use clap::Args;
use otel_arrow_dfe_quiver::inspect::inspect_wal;
use otel_arrow_dfe_quiver::subscriber::SubscriberId;
use serde_json::Value;

use crate::data::{BUILTIN_CSV_PATH, BenchmarkTable, read_csv};
use crate::dir::{DEFAULT_DIR, MARKER_FILE, require_cli_dir, segment_files};
use crate::engine::{self, MANUAL_SEGMENTS};
use crate::format::{self, Align, Table};
use crate::generate::delivery_bundle;

#[derive(Args)]
pub(crate) struct ConsumeArgs {
    /// Quiver data directory created by `quiver-cli generate`.
    #[arg(default_value = DEFAULT_DIR)]
    dir: PathBuf,
    /// Bundles to acknowledge, oldest first.
    #[arg(long, default_value_t = 8)]
    ack: usize,
}

#[derive(Args)]
pub(crate) struct CrashArgs {
    /// Quiver data directory created by `quiver-cli generate`.
    #[arg(default_value = DEFAULT_DIR)]
    pub(crate) dir: PathBuf,
    /// Bundles the child process persists before it is killed.
    #[arg(long, default_value_t = 5)]
    pub(crate) bundles: usize,
}

pub(crate) async fn consume(args: &ConsumeArgs) -> Result<()> {
    require_cli_dir(&args.dir)?;
    let before = segment_bundle_counts(&args.dir)?;
    let engine = engine::open(&args.dir, MANUAL_SEGMENTS).await?;
    let subscriber = SubscriberId::new(DELTA_SUBSCRIBER)?;
    let mut acked = BTreeMap::<u64, u32>::new();
    for _ in 0..args.ack {
        let Some(handle) = engine
            .next_bundle(&subscriber, Some(Duration::from_secs(2)), None)
            .await?
        else {
            break;
        };
        *acked.entry(handle.bundle_ref().segment_seq.raw()).or_default() += 1;
        handle.ack();
    }
    let _ = engine.maintain().await?;
    engine.shutdown().await?;
    let after = segment_bundle_counts(&args.dir)?;

    let total = acked.values().sum::<u32>();
    println!(
        "{DELTA_SUBSCRIBER} acknowledged {total} bundles, oldest first, as the collector does after each Delta commit"
    );
    println!();
    let mut table = Table::new(&[
        ("segment", Align::Left),
        ("acknowledged", Align::Right),
        ("result", Align::Left),
    ]);
    for (seq, count) in &acked {
        let bundles = before.get(seq).copied().unwrap_or_default();
        let result = if after.contains_key(seq) {
            "kept: other bundles are still unacknowledged".to_owned()
        } else {
            "every bundle acknowledged: segment deleted".to_owned()
        };
        table.row(vec![segment_name(*seq), format!("{count}/{bundles}"), result]);
    }
    println!("{}", table.render());
    println!();
    println!("  segments on disk: {} -> {}", before.len(), after.len());
    Ok(())
}

/// Bundle count of every finalized segment, by sequence.
fn segment_bundle_counts(dir: &Path) -> Result<BTreeMap<u64, u32>> {
    segment_files(dir)?
        .into_iter()
        .map(|file| {
            let reader = otel_arrow_dfe_quiver::segment::SegmentReader::open(&file.path)?;
            Ok((file.seq, u32::try_from(reader.bundle_count())?))
        })
        .collect()
}

fn segment_name(seq: u64) -> String {
    format!("{seq:016}.qseg")
}

/// Spawns a child process that persists bundles like the collector and kills
/// it with SIGKILL before any segment is finalized.
pub(crate) fn crash(args: &CrashArgs) -> Result<()> {
    require_cli_dir(&args.dir)?;
    if args.bundles == 0 {
        bail!("--bundles must be positive");
    }
    let mut child = Command::new(std::env::current_exe()?)
        .arg("crash-child")
        .arg(&args.dir)
        .arg("--bundles")
        .arg(args.bundles.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .context("start the child process")?;
    println!(
        "child process {} writes bundles like the collector (WAL fsync before each acknowledgement) and never finalizes a segment",
        child.id()
    );
    let stdout = child.stdout.take().context("child stdout")?;
    let mut persisted = 0;
    for line in BufReader::new(stdout).lines() {
        let line = line?;
        if let Some(ordinal) = line.strip_prefix("persisted ") {
            persisted += 1;
            println!("  bundle {ordinal} fsynced to the WAL and acknowledged");
            if persisted == args.bundles {
                break;
            }
        }
    }
    child.kill().context("kill the child process")?;
    let status = child.wait()?;
    match status.signal() {
        Some(signal) => println!("kill -9: child ended by signal {signal} with its open segment never finalized"),
        None => bail!("the child process exited before it could be killed: {status}"),
    }
    if persisted < args.bundles {
        bail!("the child persisted only {persisted} of {} bundles", args.bundles);
    }
    println!();
    println!("next: quiver-cli wal {}", args.dir.display());
    Ok(())
}

/// The child half of [`crash`]: persists bundles, reports each one, then
/// holds the engine open like a live collector until it is killed.
pub(crate) async fn crash_child(args: &CrashArgs) -> Result<()> {
    let marker: Value = serde_json::from_slice(&std::fs::read(args.dir.join(MARKER_FILE))?)?;
    let table_name = marker["table"].as_str().context("marker table")?;
    let csv = marker["source_csv"]
        .as_str()
        .filter(|source| *source != BUILTIN_CSV_PATH)
        .map(PathBuf::from);
    let rows_per_bundle = marker["rows_per_bundle"]
        .as_u64()
        .and_then(|rows| usize::try_from(rows).ok())
        .context("marker rows_per_bundle")?;
    let table = BenchmarkTable::load(table_name)?;
    let batches = read_csv(&table, csv.as_deref(), rows_per_bundle)?;
    let engine = engine::open(&args.dir, MANUAL_SEGMENTS).await?;
    let owner = CollectorInstanceId::new();
    let lease = LeaseId::new();
    let mut stdout = std::io::stdout();
    for (ordinal, batch) in batches.iter().cycle().take(args.bundles).enumerate() {
        engine
            .ingest(&delivery_bundle(&table, owner, lease, batch.clone())?)
            .await?;
        writeln!(stdout, "persisted {}", ordinal + 1)?;
        stdout.flush()?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    std::future::pending::<()>().await;
    Ok(())
}

/// Opens the directory like a restarted collector: the engine replays every
/// WAL-only entry into a new open segment, which is then finalized.
pub(crate) async fn recover(dir: &Path) -> Result<()> {
    require_cli_dir(dir)?;
    let wal_before = inspect_wal(dir)?;
    let segments_before = segment_files(dir)?.into_iter().map(|file| file.seq).collect::<Vec<_>>();
    let replayable = wal_before.unconsumed().count();

    // Opening replays the WAL like a collector restart; sealing by hand keeps
    // the replayed bundles in one segment however long the replay takes.
    let engine = engine::open(dir, MANUAL_SEGMENTS).await?;
    engine.flush().await?;
    let _ = engine.maintain().await?;
    engine.shutdown().await?;

    let wal_after = inspect_wal(dir)?;
    let segments_after = segment_files(dir)?;
    println!("opened {} like a restarted collector", dir.display());
    println!();
    println!(
        "  before   {} segments, {replayable} WAL-only entries{}",
        segments_before.len(),
        if wal_before.stopped_early.is_some() {
            " (plus a torn final entry)"
        } else {
            ""
        }
    );
    for file in segments_after
        .iter()
        .filter(|file| !segments_before.contains(&file.seq))
    {
        let reader = otel_arrow_dfe_quiver::segment::SegmentReader::open(&file.path)?;
        let rows = reader.manifest().iter().map(|entry| entry.item_count()).sum::<u64>();
        println!(
            "  replay   WAL entries -> new segment {} ({} bundles, {} rows)",
            file.name(),
            reader.bundle_count(),
            format::count(rows)
        );
    }
    println!(
        "  after    {} segments, {} WAL-only entries",
        segments_after.len(),
        wal_after.unconsumed().count()
    );
    Ok(())
}
