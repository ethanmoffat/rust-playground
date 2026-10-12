use std::{fs, path::PathBuf};

use arrowcol_catalog::{CatalogSchemaVersion, DdlCommand, DdlParser, SchemaVersion, deterministic_table_id};
use arrowcol_sdk_gen::{
    ArtifactBundle, SafeOutputPath, check_generated_sdk, generate_sdk, package_generated_sdk, verify_compatibility,
    write_artifact_bundle,
};
use clap::{Parser, Subcommand};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(
    name = "arrowcol-sdk-gen",
    about = "Generate deterministic Rust and TypeScript SDKs from immutable catalog artifacts"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Generate and atomically replace both language SDKs.
    Generate {
        /// Directory containing manifest.json, schema.arrow, and descriptor.pb.
        #[arg(long)]
        artifacts: PathBuf,
        /// Workspace-relative output below .temp or target.
        #[arg(long)]
        out: PathBuf,
    },
    /// Check existing generated output byte-for-byte without modifying it.
    Check {
        /// Directory containing manifest.json, schema.arrow, and descriptor.pb.
        #[arg(long)]
        artifacts: PathBuf,
        /// Workspace-relative generated output below .temp or target.
        #[arg(long)]
        out: PathBuf,
    },
    /// Verify protobuf WIRE_JSON and physical Arrow/Delta compatibility.
    Compatibility {
        /// Previous immutable artifact directory.
        #[arg(long)]
        previous: PathBuf,
        /// Candidate immutable artifact directory.
        #[arg(long)]
        current: PathBuf,
    },
    /// Build canonical catalog artifacts from a CREATE TABLE fixture.
    Fixture {
        /// UTF-8 file containing exactly one supported CREATE TABLE statement.
        #[arg(long)]
        ddl: PathBuf,
        /// Workspace-relative artifact output below .temp or target.
        #[arg(long)]
        out: PathBuf,
        /// Timezone used for TIMESTAMP WITH TIME ZONE.
        #[arg(long, default_value = "UTC")]
        timezone: String,
    },
    /// Package generated output as a deterministic .tar.gz artifact.
    PackageArtifact {
        /// Workspace-relative generated SDK directory below .temp or target.
        #[arg(long)]
        input: PathBuf,
        /// Workspace-relative archive path below .temp or target.
        #[arg(long)]
        out: PathBuf,
    },
}

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("arrowcol-sdk-gen: {error}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let workspace = std::env::current_dir()?;
    match cli.command {
        Command::Generate { artifacts, out } => {
            let bundle = ArtifactBundle::load_directory(artifacts)?;
            let output = SafeOutputPath::new(&workspace, out)?;
            print_json(&generate_sdk(&bundle, &output)?)?;
        }
        Command::Check { artifacts, out } => {
            let bundle = ArtifactBundle::load_directory(artifacts)?;
            let output = SafeOutputPath::new(&workspace, out)?;
            print_json(&check_generated_sdk(&bundle, &output)?)?;
        }
        Command::Compatibility { previous, current } => {
            let previous = ArtifactBundle::load_directory(previous)?;
            let current = ArtifactBundle::load_directory(current)?;
            print_json(&verify_compatibility(&previous, &current)?)?;
        }
        Command::Fixture { ddl, out, timezone } => {
            let metadata = fs::symlink_metadata(&ddl)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(format!("DDL fixture must be a regular non-symlink file: {}", ddl.display()).into());
            }
            let ddl_text = fs::read_to_string(&ddl)?;
            let DdlCommand::CreateTable(create) = DdlParser::new(timezone).parse(&ddl_text)? else {
                return Err("DDL fixture must contain CREATE TABLE".into());
            };
            let schema = CatalogSchemaVersion::new(
                deterministic_table_id(create.identity()),
                create.identity().clone(),
                SchemaVersion::FIRST,
                create.fields().to_vec(),
                create.canonical_ddl(),
            )?;
            let bundle = ArtifactBundle::from_schema(schema)?;
            let output = SafeOutputPath::new(&workspace, out)?;
            write_artifact_bundle(&bundle, &output)?;
            print_json(&FixtureResult {
                output: output.relative().to_string_lossy().replace('\\', "/"),
                table: bundle.schema().identity().to_string(),
                version: bundle.schema().version().get(),
                fingerprint: bundle.schema().fingerprint().to_string(),
            })?;
        }
        Command::PackageArtifact { input, out } => {
            let input = SafeOutputPath::new(&workspace, input)?;
            let output = SafeOutputPath::new(&workspace, out)?;
            print_json(&package_generated_sdk(&input, &output)?)?;
        }
    }
    Ok(())
}

fn print_json(value: &impl Serialize) -> Result<(), serde_json::Error> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[derive(Serialize)]
struct FixtureResult {
    output: String,
    table: String,
    version: u64,
    fingerprint: String,
}
