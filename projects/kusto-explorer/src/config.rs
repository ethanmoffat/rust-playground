//! Command-line and environment configuration.

use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf, time::Duration};

use clap::Parser;
use kusto_explorer_api::{ConnectionInfo, DEFAULT_ROW_CAP, MAX_ROW_CAP};
use reqwest::Url;

/// Command-line arguments; every flag can also be set through its environment variable.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "kusto-explorer",
    version,
    about = "Web UI and query proxy for Kusto clusters"
)]
pub struct Cli {
    /// Socket address the HTTP server listens on.
    #[arg(long, env = "KUSTO_EXPLORER_BIND", default_value = "0.0.0.0:8080")]
    pub bind: SocketAddr,

    /// Clusters the server may reach, as `Name=uri` pairs separated by `;`.
    #[arg(long, env = "KUSTO_EXPLORER_CONNECTIONS", default_value = "Kusto=http://kusto:8080")]
    pub connections: String,

    /// Database selected for new query tabs.
    #[arg(long, env = "KUSTO_EXPLORER_DEFAULT_DATABASE")]
    pub default_database: Option<String>,

    /// Directory containing the built browser UI.
    #[arg(
        long,
        env = "KUSTO_EXPLORER_STATIC_DIR",
        default_value = "/usr/share/kusto-explorer/www"
    )]
    pub static_dir: PathBuf,

    /// Upper bound for one upstream query, in seconds.
    #[arg(long, env = "KUSTO_EXPLORER_QUERY_TIMEOUT_SECONDS", default_value_t = 240)]
    pub query_timeout_seconds: u64,

    /// Largest upstream response body accepted, in MiB.
    #[arg(long, env = "KUSTO_EXPLORER_MAX_RESPONSE_MIB", default_value_t = 256)]
    pub max_response_mib: usize,

    /// Rows per result table returned when a request does not choose a cap.
    #[arg(long, env = "KUSTO_EXPLORER_DEFAULT_ROW_CAP", default_value_t = DEFAULT_ROW_CAP)]
    pub default_row_cap: usize,

    /// Emit logs as JSON lines instead of human-readable text.
    #[arg(long, env = "KUSTO_EXPLORER_LOG_JSON", default_value_t = false)]
    pub log_json: bool,

    /// Validate the configuration, print it, and exit.
    #[arg(long)]
    pub validate_and_exit: bool,
}

/// One configured cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connection {
    /// Stable identifier derived from the display name.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Cluster origin.
    pub uri: Url,
}

/// Validated runtime settings.
#[derive(Clone, Debug)]
pub struct Settings {
    /// Listen address.
    pub bind: SocketAddr,
    /// Configured clusters.
    pub connections: Vec<Connection>,
    /// Database selected for new tabs.
    pub default_database: Option<String>,
    /// Built browser UI directory.
    pub static_dir: PathBuf,
    /// Upstream query timeout.
    pub query_timeout: Duration,
    /// Largest upstream response body accepted, in bytes.
    pub max_response_bytes: usize,
    /// Default rows per result table.
    pub default_row_cap: usize,
}

/// Configuration errors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// No connection was configured.
    #[error("at least one connection is required")]
    NoConnections,
    /// A connection entry is not `Name=uri`.
    #[error("connection entry {0:?} must look like Name=http://host:port")]
    Malformed(String),
    /// A connection URI is invalid or unsafe.
    #[error("connection {name:?} has an invalid cluster URI: {reason}")]
    InvalidUri {
        /// Connection name.
        name: String,
        /// Why the URI was rejected.
        reason: String,
    },
    /// A numeric limit is out of range.
    #[error("{0}")]
    OutOfRange(String),
}

impl Cli {
    /// Validates the arguments.
    ///
    /// # Errors
    ///
    /// Returns an error when a connection or limit is invalid.
    pub fn settings(&self) -> Result<Settings, ConfigError> {
        if self.query_timeout_seconds == 0 || self.query_timeout_seconds > 3600 {
            return Err(ConfigError::OutOfRange(
                "query timeout must be between 1 and 3600 seconds".to_owned(),
            ));
        }
        if self.max_response_mib == 0 || self.max_response_mib > 4096 {
            return Err(ConfigError::OutOfRange(
                "maximum response size must be between 1 and 4096 MiB".to_owned(),
            ));
        }
        if self.default_row_cap == 0 || self.default_row_cap > MAX_ROW_CAP {
            return Err(ConfigError::OutOfRange(format!(
                "default row cap must be between 1 and {MAX_ROW_CAP}"
            )));
        }
        Ok(Settings {
            bind: self.bind,
            connections: parse_connections(&self.connections)?,
            default_database: self
                .default_database
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned),
            static_dir: self.static_dir.clone(),
            query_timeout: Duration::from_secs(self.query_timeout_seconds),
            max_response_bytes: self.max_response_mib.saturating_mul(1024 * 1024),
            default_row_cap: self.default_row_cap,
        })
    }
}

impl Settings {
    /// Returns the connection with `id`.
    #[must_use]
    pub fn connection(&self, id: &str) -> Option<&Connection> {
        self.connections.iter().find(|connection| connection.id == id)
    }

    /// Returns the browser-facing description of every connection.
    #[must_use]
    pub fn connection_infos(&self) -> Vec<ConnectionInfo> {
        self.connections
            .iter()
            .map(|connection| ConnectionInfo {
                id: connection.id.clone(),
                name: connection.name.clone(),
                cluster_uri: display_uri(&connection.uri),
                default_database: self.default_database.clone(),
            })
            .collect()
    }
}

/// Formats a cluster origin without a trailing slash.
#[must_use]
pub fn display_uri(uri: &Url) -> String {
    uri.as_str().trim_end_matches('/').to_owned()
}

/// Parses `Name=uri;Name=uri` into validated connections with unique identifiers.
///
/// # Errors
///
/// Returns an error for empty input, malformed entries, or URIs that are not plain HTTP(S) origins.
pub fn parse_connections(raw: &str) -> Result<Vec<Connection>, ConfigError> {
    let mut connections = Vec::new();
    let mut ids = BTreeSet::new();
    for entry in raw.split(';').map(str::trim).filter(|entry| !entry.is_empty()) {
        let (name, uri) = entry
            .split_once('=')
            .map(|(name, uri)| (name.trim(), uri.trim()))
            .filter(|(name, uri)| !name.is_empty() && !uri.is_empty())
            .ok_or_else(|| ConfigError::Malformed(entry.to_owned()))?;
        let uri = parse_cluster_uri(name, uri)?;
        let base = slug(name);
        let base = if base.is_empty() { "cluster".to_owned() } else { base };
        let mut id = base.clone();
        let mut suffix = 2;
        while !ids.insert(id.clone()) {
            id = format!("{base}-{suffix}");
            suffix += 1;
        }
        connections.push(Connection {
            id,
            name: name.to_owned(),
            uri,
        });
    }
    if connections.is_empty() {
        return Err(ConfigError::NoConnections);
    }
    Ok(connections)
}

fn parse_cluster_uri(name: &str, raw: &str) -> Result<Url, ConfigError> {
    let invalid = |reason: &str| ConfigError::InvalidUri {
        name: name.to_owned(),
        reason: reason.to_owned(),
    };
    let uri = Url::parse(raw).map_err(|error| invalid(&error.to_string()))?;
    if !matches!(uri.scheme(), "http" | "https") {
        return Err(invalid("scheme must be http or https"));
    }
    if uri.host_str().is_none_or(str::is_empty) {
        return Err(invalid("host is required"));
    }
    if !uri.username().is_empty() || uri.password().is_some() {
        return Err(invalid("credentials are not allowed"));
    }
    if uri.query().is_some() || uri.fragment().is_some() {
        return Err(invalid("query strings and fragments are not allowed"));
    }
    if uri.path() != "/" {
        return Err(invalid("use the cluster origin without a path"));
    }
    Ok(uri)
}

fn slug(name: &str) -> String {
    let mut slug = String::with_capacity(name.len());
    for character in name.chars() {
        if character.is_ascii_alphanumeric() {
            slug.push(character.to_ascii_lowercase());
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }
    slug.trim_matches('-').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_named_connections_with_unique_ids() {
        let connections = parse_connections(
            " Loadtest emulator=http://kusto:8080 ; Other=https://example.net/;loadtest-emulator=http://k2:80",
        )
        .unwrap_or_default();
        let ids: Vec<_> = connections.iter().map(|connection| connection.id.as_str()).collect();
        assert_eq!(ids, ["loadtest-emulator", "other", "loadtest-emulator-2"]);
        assert_eq!(connections[0].name, "Loadtest emulator");
        assert_eq!(display_uri(&connections[0].uri), "http://kusto:8080");
        assert_eq!(display_uri(&connections[1].uri), "https://example.net");
    }

    #[test]
    fn rejects_unsafe_or_malformed_connections() {
        assert_eq!(parse_connections(" ; "), Err(ConfigError::NoConnections));
        assert!(matches!(parse_connections("no-uri"), Err(ConfigError::Malformed(_))));
        for uri in [
            "ftp://kusto:8080",
            "http://reader@kusto:8080",
            "http://kusto:8080/v1/rest/query",
            "http://kusto:8080/?db=x",
            "not a uri",
        ] {
            assert!(
                matches!(
                    parse_connections(&format!("K={uri}")),
                    Err(ConfigError::InvalidUri { .. })
                ),
                "{uri}"
            );
        }
    }

    #[test]
    fn validates_limits_and_trims_default_database() {
        let mut cli = Cli::parse_from(["kusto-explorer", "--default-database", "  loadtest "]);
        let settings = cli.settings();
        assert!(settings.is_ok());
        let settings = settings.unwrap_or_else(|_| unreachable_settings());
        assert_eq!(settings.default_database.as_deref(), Some("loadtest"));
        assert_eq!(settings.connections[0].id, "kusto");
        assert_eq!(settings.connection_infos()[0].cluster_uri, "http://kusto:8080");
        cli.query_timeout_seconds = 0;
        assert!(cli.settings().is_err());
        cli.query_timeout_seconds = 10;
        cli.default_row_cap = MAX_ROW_CAP + 1;
        assert!(cli.settings().is_err());
    }

    fn unreachable_settings() -> Settings {
        Settings {
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            connections: Vec::new(),
            default_database: None,
            static_dir: PathBuf::new(),
            query_timeout: Duration::from_secs(1),
            max_response_bytes: 0,
            default_row_cap: 1,
        }
    }
}
