//! Wire types shared by the kusto-explorer server and the browser UI.
//!
//! The browser only ever talks to the kusto-explorer server; the server is the
//! single component that reaches a Kusto cluster, so every type here is the
//! contract between those two halves of the application.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Default number of rows per result table returned to the browser.
pub const DEFAULT_ROW_CAP: usize = 50_000;

/// Upper bound for a caller-supplied row cap.
pub const MAX_ROW_CAP: usize = 500_000;

/// Runtime configuration exposed to the browser.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppConfig {
    /// Server version.
    pub version: String,
    /// Clusters the server is allowed to reach.
    pub connections: Vec<ConnectionInfo>,
    /// Row cap applied when a request does not specify one.
    pub default_row_cap: usize,
}

/// A cluster the server is configured to reach.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInfo {
    /// Stable identifier used by every API call.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Cluster URI as seen by the server.
    pub cluster_uri: String,
    /// Database selected for new query tabs.
    pub default_database: Option<String>,
}

/// One database on a cluster.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseInfo {
    /// Database name.
    pub name: String,
    /// Optional pretty name.
    pub pretty_name: Option<String>,
}

/// Normalized schema of one database.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseSchema {
    /// Connection the schema belongs to.
    pub connection_id: String,
    /// Cluster URI used by the editor language service.
    pub cluster_uri: String,
    /// Database name.
    pub database: String,
    /// Tables.
    pub tables: Vec<EntitySchema>,
    /// Materialized views.
    pub materialized_views: Vec<EntitySchema>,
    /// External tables.
    pub external_tables: Vec<EntitySchema>,
    /// Stored functions.
    pub functions: Vec<FunctionSchema>,
    /// Raw `.show database schema as json` document for the editor language service.
    pub raw: Value,
}

/// A table-like entity with columns.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntitySchema {
    /// Entity name.
    pub name: String,
    /// Optional folder.
    pub folder: Option<String>,
    /// Optional documentation string.
    pub doc_string: Option<String>,
    /// Ordered columns.
    pub columns: Vec<ColumnSchema>,
}

/// One column.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnSchema {
    /// Column name.
    pub name: String,
    /// KQL scalar type such as `long` or `datetime`.
    pub column_type: String,
    /// Optional documentation string.
    pub doc_string: Option<String>,
}

/// A stored function.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionSchema {
    /// Function name.
    pub name: String,
    /// Optional folder.
    pub folder: Option<String>,
    /// Optional documentation string.
    pub doc_string: Option<String>,
    /// Parameter list such as `(start:datetime, limit:long)`.
    pub parameters: String,
    /// Function body.
    pub body: String,
}

/// Request to run a query or management command.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
    /// Configured connection identifier.
    pub connection_id: String,
    /// Database in context.
    pub database: String,
    /// Query or management command text.
    pub query: String,
    /// Client request identifier used for tracing and cancellation.
    pub client_request_id: String,
    /// Maximum rows returned per result table.
    pub row_cap: Option<usize>,
}

/// Whether a request ran as a query or as a management command.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryKind {
    /// Query language request.
    #[default]
    Query,
    /// Management (dot) command.
    Command,
}

/// Result of a query or management command.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    /// Client request identifier echoed back.
    pub client_request_id: String,
    /// Request kind.
    pub kind: QueryKind,
    /// Primary result tables.
    pub tables: Vec<ResultTable>,
    /// Visualization requested with the `render` operator.
    pub visualization: Option<Visualization>,
    /// Execution statistics.
    pub stats: QueryStats,
    /// Informational messages and warnings reported by the service.
    pub messages: Vec<QueryMessage>,
}

/// One result table.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultTable {
    /// Table name.
    pub name: String,
    /// Columns.
    pub columns: Vec<ResultColumn>,
    /// Rows, each with one JSON value per column.
    pub rows: Vec<Vec<Value>>,
    /// Rows produced by the service before the row cap was applied.
    pub total_rows: usize,
    /// Whether rows were dropped by the row cap.
    pub truncated: bool,
}

/// One result column.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultColumn {
    /// Column name.
    pub name: String,
    /// KQL scalar type such as `long` or `datetime`.
    pub column_type: String,
}

/// Visualization properties produced by the `render` operator.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Visualization {
    /// Index of the result table the visualization applies to.
    pub table_index: usize,
    /// Visualization kind such as `timechart` or `columnchart`.
    pub visualization: String,
    /// Chart title.
    pub title: Option<String>,
    /// Column used for the x-axis.
    pub x_column: Option<String>,
    /// Columns used for the y-axis.
    pub y_columns: Vec<String>,
    /// Columns that split the data into series.
    pub series: Vec<String>,
    /// X-axis title.
    pub x_title: Option<String>,
    /// Y-axis title.
    pub y_title: Option<String>,
    /// Kind modifier such as `stacked`.
    pub kind: Option<String>,
    /// Legend visibility such as `hidden`.
    pub legend: Option<String>,
    /// Y-axis scale such as `log`.
    pub y_scale: Option<String>,
    /// Y-axis minimum.
    pub ymin: Option<f64>,
    /// Y-axis maximum.
    pub ymax: Option<f64>,
}

/// Execution statistics.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryStats {
    /// Wall-clock time measured by the server around the upstream request.
    pub server_elapsed_ms: f64,
    /// Execution time reported by the service.
    pub execution_time_ms: Option<f64>,
    /// Total CPU time reported by the service.
    pub cpu_time: Option<String>,
    /// Peak memory per node reported by the service.
    pub memory_peak_bytes: Option<u64>,
    /// Rows scanned.
    pub rows_scanned: Option<u64>,
    /// Extents scanned.
    pub extents_scanned: Option<u64>,
    /// Extents considered.
    pub extents_total: Option<u64>,
    /// Rows returned across result tables before the row cap.
    pub result_rows: usize,
    /// Size of the upstream response body.
    pub response_bytes: usize,
}

/// Severity of a service message.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageSeverity {
    /// Informational.
    #[default]
    Info,
    /// Warning.
    Warning,
    /// Error.
    Error,
}

/// A message reported alongside query results.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryMessage {
    /// Severity.
    pub severity: MessageSeverity,
    /// Message text.
    pub text: String,
}

/// Error body returned by every failing API call.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiError {
    /// Short, user-facing message.
    pub message: String,
    /// Optional detail such as the service error text.
    pub detail: Option<String>,
    /// Optional service error code.
    pub code: Option<String>,
}

/// Request to cancel a running query.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelRequest {
    /// Configured connection identifier.
    pub connection_id: String,
    /// Client request identifier of the running query.
    pub client_request_id: String,
}

/// Result of a cancellation request.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelResponse {
    /// Whether the service accepted the cancellation.
    pub cancelled: bool,
    /// Optional explanation.
    pub message: Option<String>,
}

/// Readiness of every configured connection.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Readiness {
    /// Whether every connection is reachable.
    pub ready: bool,
    /// Per-connection health.
    pub connections: Vec<ConnectionHealth>,
}

/// Health of one connection.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionHealth {
    /// Connection identifier.
    pub id: String,
    /// Whether the cluster answered.
    pub reachable: bool,
    /// Service version when reachable.
    pub version: Option<String>,
    /// Error when unreachable.
    pub error: Option<String>,
}

/// Returns the query text after leading whitespace and `//` comment lines.
#[must_use]
pub fn strip_leading_trivia(query: &str) -> &str {
    let mut rest = query.trim_start();
    while let Some(comment) = rest.strip_prefix("//") {
        rest = comment.split_once('\n').map_or("", |(_, tail)| tail).trim_start();
    }
    rest
}

/// Returns whether `query` is a management (dot) command.
#[must_use]
pub fn is_management_command(query: &str) -> bool {
    strip_leading_trivia(query).starts_with('.')
}

/// Returns whether `id` is safe to forward as a client request identifier.
#[must_use]
pub fn is_valid_client_request_id(id: &str) -> bool {
    (1..=128).contains(&id.len())
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b':' | b';'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn management_commands_skip_whitespace_and_comments() {
        assert!(is_management_command(".show tables"));
        assert!(is_management_command("  \n// list tables\n// twice\n  .show tables"));
        assert!(!is_management_command("StormEvents | take 10"));
        assert!(!is_management_command("// .show tables\nT | count"));
        assert!(!is_management_command(""));
        assert!(!is_management_command("// only a comment"));
    }

    #[test]
    fn client_request_ids_reject_header_injection() {
        assert!(is_valid_client_request_id(
            "KX.execute;3f1c2a7e-0b8c-4c55-9a51-5b8c4f1e2d3a"
        ));
        assert!(!is_valid_client_request_id(""));
        assert!(!is_valid_client_request_id("a\r\nX-Injected: 1"));
        assert!(!is_valid_client_request_id(&"a".repeat(129)));
    }

    #[test]
    fn wire_format_is_camel_case() {
        let request = QueryRequest {
            connection_id: "local".to_owned(),
            database: "db".to_owned(),
            query: "T".to_owned(),
            client_request_id: "KX.1".to_owned(),
            row_cap: Some(10),
        };
        let json = serde_json::to_value(&request).unwrap_or_default();
        assert_eq!(json["connectionId"], "local");
        assert_eq!(json["clientRequestId"], "KX.1");
        assert_eq!(json["rowCap"], 10);
        assert_eq!(serde_json::to_value(QueryKind::Command).unwrap_or_default(), "command");
    }
}
