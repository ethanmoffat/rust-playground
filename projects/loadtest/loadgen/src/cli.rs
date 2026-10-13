use std::{fmt, path::PathBuf, str::FromStr, time::Duration};

use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

/// Arrowcol benchmark command line.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "arrowcol-loadgen",
    about = "Deterministic many-table Arrow Flight load generator"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// Supported load-generator operations.
#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    /// Register schemas, generate SDK artifacts, and run bounded load.
    Run(Box<RunArgs>),
    /// Validate retained manifests and independent Delta snapshots.
    Validate(ValidateArgs),
    /// Print the exact public Flight SQL schema fixture.
    PrintSchemas,
}

/// Benchmark scenario.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    /// Arrow traffic routed only to durable direct collectors.
    Direct,
    /// Arrow traffic routed only to best-effort dataflow collectors.
    Dataflow,
    /// The same logical batch is sent concurrently to both pools.
    Paired,
}

impl Scenario {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Dataflow => "dataflow",
            Self::Paired => "paired",
        }
    }
}

/// Whole-second duration accepted by the benchmark UX.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurationArg(Duration);

impl DurationArg {
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl FromStr for DurationArg {
    type Err = DurationParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(DurationParseError);
        }
        let (magnitude, multiplier) = match value.as_bytes().last().copied() {
            Some(b's' | b'S') => (&value[..value.len() - 1], 1_u64),
            Some(b'm' | b'M') => (&value[..value.len() - 1], 60_u64),
            Some(b'h' | b'H') => (&value[..value.len() - 1], 3_600_u64),
            Some(byte) if byte.is_ascii_digit() => (value, 1_u64),
            _ => return Err(DurationParseError),
        };
        if magnitude.is_empty() || !magnitude.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(DurationParseError);
        }
        let magnitude = magnitude.parse::<u64>().map_err(|_| DurationParseError)?;
        let seconds = magnitude.checked_mul(multiplier).ok_or(DurationParseError)?;
        if seconds > i32::MAX as u64 {
            return Err(DurationParseError);
        }
        Ok(Self(Duration::from_secs(seconds)))
    }
}

/// Invalid duration syntax or range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurationParseError;

impl fmt::Display for DurationParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("expected integer seconds or a whole-number duration such as 30s, 2m, or 1h")
    }
}

impl std::error::Error for DurationParseError {}

/// Runtime and connection settings.
#[derive(Clone, Debug, Args)]
pub struct RunArgs {
    /// Stable run identifier. Defaults to a UTC timestamp and random suffix.
    #[arg(long, env = "LOADTEST_RUN_ID")]
    pub run_id: Option<String>,

    /// Warmup duration.
    #[arg(long, env = "LOADTEST_WARMUP", default_value = "2m")]
    pub warmup: DurationArg,

    /// Measurement duration. Ignored by continuous mode.
    #[arg(long, env = "LOADTEST_MEASURE", default_value = "5m")]
    pub measure: DurationArg,

    /// Delay after schema/SDK preparation before warmup.
    #[arg(long, env = "LOADTEST_CAPTURE_SETTLE", default_value = "30s")]
    pub capture_settle: DurationArg,

    /// Maximum time to wait for direct receipts and graceful publication drain.
    #[arg(long, env = "LOADTEST_DRAIN_TIMEOUT", default_value = "2m")]
    pub drain_timeout: DurationArg,

    /// Per-request timeout.
    #[arg(long, env = "LOADTEST_REQUEST_TIMEOUT", default_value = "1m")]
    pub request_timeout: DurationArg,

    /// Run paired load until Ctrl+C. This overrides configured scenarios and measurement duration.
    #[arg(long, env = "LOADTEST_CONTINUOUS", default_value_t = false)]
    pub continuous: bool,

    /// Retained for the outer Compose UX; the load process records it but does not own the stack.
    #[arg(long, env = "LOADTEST_KEEP_STACK", default_value_t = false)]
    pub keep_stack: bool,

    /// Scenarios to run in order.
    #[arg(
        long = "scenario",
        env = "LOADTEST_SCENARIOS",
        value_delimiter = ',',
        default_value = "direct,dataflow,paired"
    )]
    pub scenarios: Vec<Scenario>,

    /// Optional fully qualified tables to load; empty means all registered tables.
    #[arg(long = "table", env = "LOADTEST_TABLES", value_delimiter = ',')]
    pub tables: Vec<String>,

    /// Concurrent scheduling lanes. Each lane has at most one logical batch in flight.
    #[arg(long, env = "LOADTEST_LANES", default_value_t = 4)]
    pub lanes: usize,

    /// Rows per Arrow RecordBatch.
    #[arg(long, env = "LOADTEST_BATCH_ROWS", default_value_t = 8_192)]
    pub batch_rows: usize,

    /// Deterministic corpus and scheduler seed.
    #[arg(long, env = "LOADTEST_SEED", default_value_t = 20_260_923)]
    pub seed: u64,

    /// Maximum physical attempts for one pool/logical batch.
    #[arg(long, env = "LOADTEST_MAX_ATTEMPTS", default_value_t = 16)]
    pub max_attempts: u32,

    /// Skip one-time schema/duplicate/backpressure probes for follow-up chaos sends.
    #[arg(long, env = "LOADTEST_SKIP_PROBES", default_value_t = false, hide = true)]
    pub skip_probes: bool,

    /// Reuse previously generated SDK artifacts for a follow-up run.
    #[arg(long, env = "LOADTEST_REUSE_GENERATED", default_value_t = false, hide = true)]
    pub reuse_generated: bool,

    /// Skip capability snapshots for a degraded follow-up chaos send.
    #[arg(long, env = "LOADTEST_SKIP_CAPABILITIES", default_value_t = false, hide = true)]
    pub skip_capabilities: bool,

    /// Router Arrow Flight endpoint.
    #[arg(long, env = "LOADTEST_ROUTER_ENDPOINT", default_value = "http://router:50050")]
    pub router_endpoint: String,

    /// Direct collector used for Flight SQL catalog registration.
    #[arg(long, env = "LOADTEST_CATALOG_ENDPOINT", default_value = "http://direct-1:50051")]
    pub catalog_endpoint: String,

    /// Router HTTP base URL used for receipt polling.
    #[arg(long, env = "LOADTEST_RECEIPT_ENDPOINT", default_value = "http://router:50050")]
    pub receipt_endpoint: String,

    /// Trusted-edge HMAC secret file.
    #[arg(long, env = "LOADTEST_EDGE_SECRET_FILE", default_value = "/run/secrets/edge-hmac")]
    pub edge_secret_file: PathBuf,

    /// Router-to-collector HMAC secret used only for direct Flight SQL control.
    #[arg(
        long,
        env = "LOADTEST_INTERNAL_SECRET_FILE",
        default_value = "/run/secrets/internal-hmac"
    )]
    pub internal_secret_file: PathBuf,

    /// SQL Server host used to export immutable catalog artifacts and routing evidence.
    #[arg(long, env = "ARROWCOL_SQL_HOST", default_value = "sqlserver")]
    pub sql_host: String,

    /// SQL Server port.
    #[arg(long, env = "ARROWCOL_SQL_PORT", default_value_t = 1_433)]
    pub sql_port: u16,

    /// Arrowcol application database.
    #[arg(long, env = "ARROWCOL_SQL_DATABASE", default_value = "arrowcol")]
    pub sql_database: String,

    /// Arrowcol runtime principal.
    #[arg(long, env = "ARROWCOL_SQL_USERNAME", default_value = "arrowcol_app")]
    pub sql_username: String,

    /// Arrowcol application-principal password file.
    #[arg(
        long,
        env = "ARROWCOL_SQL_PASSWORD_FILE",
        default_value = "/run/secrets/arrowcol-app-password"
    )]
    pub sql_password_file: PathBuf,

    /// Workspace-relative scratch directory below .temp or target.
    #[arg(
        long,
        env = "LOADTEST_SCRATCH_ROOT",
        default_value = "projects/loadtest/.temp/loadgen"
    )]
    pub scratch_root: PathBuf,

    /// Retained benchmark result directory.
    #[arg(long, env = "LOADTEST_RESULTS", default_value = "/results")]
    pub results: PathBuf,

    /// Direct-pool Delta root.
    #[arg(long, env = "LOADTEST_DIRECT_ROOT", default_value = "/delta/direct")]
    pub direct_root: PathBuf,

    /// Dataflow-pool Delta root.
    #[arg(long, env = "LOADTEST_DATAFLOW_ROOT", default_value = "/delta/dataflow")]
    pub dataflow_root: PathBuf,

    /// arrowcol-sdk-gen executable.
    #[arg(long, env = "ARROWCOL_SDK_GEN", default_value = "arrowcol-sdk-gen")]
    pub sdk_generator: PathBuf,

    /// Comma-separated direct collector admin base URLs.
    #[arg(
        long,
        env = "LOADTEST_DIRECT_ADMIN_ENDPOINTS",
        value_delimiter = ',',
        default_value = "http://direct-1:8081"
    )]
    pub direct_admin_endpoints: Vec<String>,

    /// Comma-separated dataflow collector admin base URLs.
    #[arg(
        long,
        env = "LOADTEST_DATAFLOW_ADMIN_ENDPOINTS",
        value_delimiter = ',',
        default_value = "http://dataflow-1:32011"
    )]
    pub dataflow_admin_endpoints: Vec<String>,
}

impl RunArgs {
    /// Returns scenarios after continuous-mode precedence is applied.
    #[must_use]
    pub fn effective_scenarios(&self) -> Vec<Scenario> {
        if self.continuous {
            vec![Scenario::Paired]
        } else {
            self.scenarios.clone()
        }
    }
}

/// Inputs for independent retained-state validation.
#[derive(Clone, Debug, Args)]
pub struct ValidateArgs {
    /// Retained benchmark result directory.
    #[arg(long, env = "LOADTEST_RESULTS", default_value = "/results")]
    pub results: PathBuf,

    /// Direct-pool Delta root.
    #[arg(long, env = "LOADTEST_DIRECT_ROOT", default_value = "/delta/direct")]
    pub direct_root: PathBuf,

    /// Dataflow-pool Delta root.
    #[arg(long, env = "LOADTEST_DATAFLOW_ROOT", default_value = "/delta/dataflow")]
    pub dataflow_root: PathBuf,

    /// Maximum time to wait for independently visible Delta snapshots.
    #[arg(long, env = "LOADTEST_DRAIN_TIMEOUT", default_value = "2m")]
    pub timeout: DurationArg,

    /// Require every collector in each implementation pool to own a table.
    #[arg(long, env = "LOADTEST_REQUIRE_ALL_COLLECTORS", default_value_t = true)]
    pub require_all_collectors: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_accept_documented_units_and_reject_fractional_or_overflow_values() {
        assert_eq!(
            "30".parse::<DurationArg>().map(DurationArg::get),
            Ok(Duration::from_secs(30))
        );
        assert_eq!(
            "2m".parse::<DurationArg>().map(DurationArg::get),
            Ok(Duration::from_secs(120))
        );
        assert_eq!(
            "1H".parse::<DurationArg>().map(DurationArg::get),
            Ok(Duration::from_secs(3_600))
        );
        assert!("1.5m".parse::<DurationArg>().is_err());
        assert!("2147483648s".parse::<DurationArg>().is_err());
    }

    #[test]
    fn continuous_mode_forces_paired_scheduling() {
        let arguments = RunArgs {
            run_id: Some("test".to_owned()),
            warmup: DurationArg(Duration::ZERO),
            measure: DurationArg(Duration::from_secs(1)),
            capture_settle: DurationArg(Duration::ZERO),
            drain_timeout: DurationArg(Duration::from_secs(1)),
            request_timeout: DurationArg(Duration::from_secs(1)),
            continuous: true,
            keep_stack: false,
            scenarios: vec![Scenario::Direct, Scenario::Dataflow],
            tables: Vec::new(),
            lanes: 1,
            batch_rows: 1,
            seed: 1,
            max_attempts: 1,
            skip_probes: false,
            reuse_generated: false,
            skip_capabilities: false,
            router_endpoint: "http://localhost".to_owned(),
            catalog_endpoint: "http://localhost".to_owned(),
            receipt_endpoint: "http://localhost".to_owned(),
            edge_secret_file: "secret".into(),
            internal_secret_file: "internal".into(),
            sql_host: "localhost".to_owned(),
            sql_port: 1_433,
            sql_database: "arrowcol".to_owned(),
            sql_username: "arrowcol_app".to_owned(),
            sql_password_file: "password".into(),
            scratch_root: "target/loadtest".into(),
            results: "target/results".into(),
            direct_root: "target/direct".into(),
            dataflow_root: "target/dataflow".into(),
            sdk_generator: "arrowcol-sdk-gen".into(),
            direct_admin_endpoints: Vec::new(),
            dataflow_admin_endpoints: Vec::new(),
        };
        assert_eq!(arguments.effective_scenarios(), vec![Scenario::Paired]);
    }
}
