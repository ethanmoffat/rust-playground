//! Kusto REST client and response normalization.

use std::{collections::BTreeMap, time::Duration};

use kusto_explorer_api::{
    ApiError, ColumnSchema, DatabaseInfo, EntitySchema, FunctionSchema, MessageSeverity, QueryKind, QueryMessage,
    QueryResponse, QueryStats, ResultColumn, ResultTable, Visualization,
};
use reqwest::{Client, StatusCode, Url, header};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};

const APP_NAME: &str = "kusto-explorer";
const CLIENT_VERSION: &str = concat!("kusto-explorer/", env!("CARGO_PKG_VERSION"));
const TRANSPORT_GRACE: Duration = Duration::from_secs(5);
const MAX_ERROR_TEXT: usize = 4096;
const TOC_COLUMNS: [&str; 5] = ["Ordinal", "Kind", "Name", "Id", "PrettyName"];

/// Errors raised while talking to a cluster.
#[derive(Debug, thiserror::Error)]
pub enum KustoError {
    /// The cluster could not be reached.
    #[error("The cluster could not be reached: {0}")]
    Unreachable(String),
    /// The request exceeded its timeout.
    #[error("The request timed out after {} seconds", .0.as_secs())]
    Timeout(Duration),
    /// The cluster rejected the request.
    #[error("{message}")]
    Service {
        /// Upstream HTTP status.
        status: StatusCode,
        /// Service error code.
        code: Option<String>,
        /// Most specific message reported by the service.
        message: String,
        /// Supporting detail.
        detail: Option<String>,
    },
    /// The response exceeded the configured size limit.
    #[error("The cluster response exceeded the {0} byte limit; narrow the query or lower the row count")]
    TooLarge(usize),
    /// The response could not be understood.
    #[error("The cluster returned an unexpected response: {0}")]
    Protocol(String),
}

impl KustoError {
    /// HTTP status returned to the browser.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            Self::Service { status, .. } if status.is_client_error() => StatusCode::BAD_REQUEST,
            Self::Unreachable(_) | Self::Service { .. } | Self::TooLarge(_) | Self::Protocol(_) => {
                StatusCode::BAD_GATEWAY
            }
        }
    }

    /// Browser-facing error body.
    #[must_use]
    pub fn to_api(&self) -> ApiError {
        match self {
            Self::Service {
                code, message, detail, ..
            } => ApiError {
                message: message.clone(),
                detail: detail.clone(),
                code: code.clone(),
            },
            other => ApiError {
                message: other.to_string(),
                detail: None,
                code: None,
            },
        }
    }
}

/// One upstream request.
#[derive(Clone, Copy, Debug)]
pub struct Execute<'a> {
    /// Cluster origin.
    pub cluster: &'a Url,
    /// Database in context, if any.
    pub database: Option<&'a str>,
    /// Query or command text.
    pub csl: &'a str,
    /// Query or management endpoint.
    pub kind: QueryKind,
    /// Client request identifier.
    pub client_request_id: &'a str,
    /// Server-side timeout.
    pub timeout: Duration,
}

/// HTTP client for the Kusto REST API.
#[derive(Clone, Debug)]
pub struct KustoClient {
    http: Client,
    max_response_bytes: usize,
}

impl KustoClient {
    /// Creates a client that rejects responses larger than `max_response_bytes`.
    ///
    /// # Errors
    ///
    /// Returns an error when the TLS backend cannot be initialized.
    pub fn new(max_response_bytes: usize) -> Result<Self, reqwest::Error> {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(CLIENT_VERSION)
            .build()?;
        Ok(Self {
            http,
            max_response_bytes,
        })
    }

    /// Sends one request and returns the raw response body.
    ///
    /// # Errors
    ///
    /// Returns an error for transport failures, timeouts, oversized bodies, or service errors.
    pub async fn execute(&self, request: Execute<'_>) -> Result<Vec<u8>, KustoError> {
        let route = match request.kind {
            QueryKind::Command => "v1/rest/mgmt",
            QueryKind::Query => "v2/rest/query",
        };
        let url = request
            .cluster
            .join(route)
            .map_err(|error| KustoError::Protocol(error.to_string()))?;
        let properties = json!({"Options": {"servertimeout": timespan(request.timeout)}}).to_string();
        let mut body = json!({"csl": request.csl, "properties": properties});
        if let Some(database) = request.database {
            body["db"] = json!(database);
        }
        let response = self
            .http
            .post(url)
            .timeout(request.timeout + TRANSPORT_GRACE)
            .header(header::ACCEPT, "application/json")
            .header("x-ms-client-request-id", request.client_request_id)
            .header("x-ms-app", APP_NAME)
            .header("x-ms-client-version", CLIENT_VERSION)
            .json(&body)
            .send()
            .await
            .map_err(|error| transport_error(&error, request.timeout))?;
        let status = response.status();
        let body = self.read_body(response, request.timeout).await?;
        if status.is_success() {
            Ok(body)
        } else {
            Err(service_error(status, &body))
        }
    }

    async fn read_body(&self, mut response: reqwest::Response, timeout: Duration) -> Result<Vec<u8>, KustoError> {
        let limit = self.max_response_bytes;
        if response
            .content_length()
            .is_some_and(|length| usize::try_from(length).map_or(true, |length| length > limit))
        {
            return Err(KustoError::TooLarge(limit));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| transport_error(&error, timeout))?
        {
            if body.len().saturating_add(chunk.len()) > limit {
                return Err(KustoError::TooLarge(limit));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

fn transport_error(error: &reqwest::Error, timeout: Duration) -> KustoError {
    if error.is_timeout() {
        return KustoError::Timeout(timeout);
    }
    let mut message = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    KustoError::Unreachable(message)
}

/// Formats a duration as a Kusto timespan literal.
#[must_use]
pub fn timespan(duration: Duration) -> String {
    let seconds = duration.as_secs();
    format!("{:02}:{:02}:{:02}", seconds / 3600, seconds % 3600 / 60, seconds % 60)
}

/// Converts an error response into a [`KustoError::Service`].
#[must_use]
pub fn service_error(status: StatusCode, body: &[u8]) -> KustoError {
    if let Ok(value) = serde_json::from_slice::<Value>(body)
        && let Some(error) = value.get("error")
    {
        let text = |key: &str| error.get(key).and_then(Value::as_str).map(str::to_owned);
        let message = text("@message")
            .or_else(|| text("message"))
            .unwrap_or_else(|| format!("The cluster returned HTTP {status}"));
        let activity = error
            .pointer("/@context/activityId")
            .and_then(Value::as_str)
            .map(|id| format!("activity {id}"));
        let detail = [text("@type"), activity].into_iter().flatten().collect::<Vec<_>>();
        return KustoError::Service {
            status,
            code: text("code"),
            message: clip(&message),
            detail: (!detail.is_empty()).then(|| detail.join(", ")),
        };
    }
    let text = String::from_utf8_lossy(body);
    let text = text.trim();
    let (message, detail) = text.split_once('\n').map_or((text, None), |(head, tail)| {
        (head.trim(), Some(tail.trim()).filter(|tail| !tail.is_empty()))
    });
    KustoError::Service {
        status,
        code: message.split_once(':').map(|(code, _)| code.trim().to_owned()),
        message: if message.is_empty() {
            format!("The cluster returned HTTP {status}")
        } else {
            clip(message)
        },
        detail: detail.map(clip),
    }
}

fn clip(text: &str) -> String {
    if text.len() <= MAX_ERROR_TEXT {
        return text.to_owned();
    }
    let mut end = MAX_ERROR_TEXT;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Frame<'a> {
    frame_type: String,
    #[serde(default)]
    table_id: Option<i64>,
    #[serde(default)]
    table_kind: Option<String>,
    #[serde(default)]
    table_name: Option<String>,
    #[serde(default)]
    columns: Vec<Column>,
    #[serde(default, borrow)]
    rows: Vec<&'a RawValue>,
    #[serde(default)]
    cancelled: bool,
    #[serde(default)]
    one_api_errors: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Column {
    column_name: String,
    #[serde(default)]
    column_type: Option<String>,
    #[serde(default)]
    data_type: Option<String>,
}

impl Column {
    fn kusto_type(&self) -> String {
        self.column_type
            .clone()
            .filter(|kind| !kind.is_empty())
            .unwrap_or_else(|| clr_to_kusto(self.data_type.as_deref().unwrap_or_default()).to_owned())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct V1Response<'a> {
    #[serde(borrow)]
    tables: Vec<V1Table<'a>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct V1Table<'a> {
    table_name: String,
    #[serde(default)]
    columns: Vec<Column>,
    #[serde(default, borrow)]
    rows: Vec<&'a RawValue>,
}

/// Maps a .NET type name from a v1 response to a KQL scalar type.
#[must_use]
pub fn clr_to_kusto(data_type: &str) -> &'static str {
    match data_type.trim_start_matches("System.") {
        "DateTime" => "datetime",
        "Int64" | "UInt64" => "long",
        "Int32" | "Int16" | "Byte" | "UInt32" | "UInt16" => "int",
        "Boolean" | "SByte" => "bool",
        "Double" | "Single" => "real",
        "Guid" => "guid",
        "TimeSpan" => "timespan",
        "Object" => "dynamic",
        "Decimal" | "SqlDecimal" | "Data.SqlTypes.SqlDecimal" => "decimal",
        _ => "string",
    }
}

fn protocol(context: &str, error: &serde_json::Error) -> KustoError {
    KustoError::Protocol(format!("{context}: {error}"))
}

fn utf8(body: &[u8]) -> Result<&str, KustoError> {
    std::str::from_utf8(body).map_err(|_| KustoError::Protocol("the response is not UTF-8".to_owned()))
}

/// Normalizes a query (`v2`) or management (`v1`) response.
///
/// # Errors
///
/// Returns [`KustoError::Protocol`] when the body is not a Kusto response.
pub fn parse_response(kind: QueryKind, body: &[u8], row_cap: usize) -> Result<QueryResponse, KustoError> {
    let mut response = match kind {
        QueryKind::Query => parse_v2(body, row_cap)?,
        QueryKind::Command => parse_v1(body, row_cap)?,
    };
    response.kind = kind;
    let mut seen = std::collections::BTreeSet::new();
    response
        .messages
        .retain(|message| seen.insert((message.severity as u8, message.text.clone())));
    response.stats.result_rows = response.tables.iter().map(|table| table.total_rows).sum();
    for (index, table) in response.tables.iter().enumerate() {
        if table.truncated {
            response.messages.push(QueryMessage {
                severity: MessageSeverity::Warning,
                text: format!(
                    "Result table {} has {} rows; only the first {} are shown. Raise the row limit in Settings or narrow the query.",
                    index + 1,
                    table.total_rows,
                    table.rows.len()
                ),
            });
        }
    }
    Ok(response)
}

fn parse_v2(body: &[u8], row_cap: usize) -> Result<QueryResponse, KustoError> {
    let frames: Vec<&RawValue> =
        serde_json::from_str(utf8(body)?).map_err(|error| protocol("invalid v2 response", &error))?;
    let mut response = QueryResponse::default();
    let mut visualizations = BTreeMap::new();
    let mut table_ids = Vec::new();
    for frame in frames {
        let frame: Frame<'_> =
            serde_json::from_str(frame.get()).map_err(|error| protocol("invalid v2 frame", &error))?;
        match (frame.frame_type.as_str(), frame.table_kind.as_deref()) {
            ("DataTable", Some("PrimaryResult")) => {
                let name = frame.table_name.clone().unwrap_or_else(|| "PrimaryResult".to_owned());
                let table = build_table(name, &frame.columns, &frame.rows, row_cap, &mut response.messages)?;
                response.tables.push(table);
                table_ids.push(frame.table_id);
            }
            ("DataTable", Some("QueryProperties")) => {
                for row in &frame.rows {
                    let Ok(Value::Array(cells)) = serde_json::from_str::<Value>(row.get()) else {
                        continue;
                    };
                    if let [Value::Number(id), Value::String(key), value] = cells.as_slice()
                        && key == "Visualization"
                        && let (Some(id), Some(visualization)) = (id.as_i64(), parse_visualization(value))
                    {
                        visualizations.entry(id).or_insert(visualization);
                    }
                }
            }
            ("DataTable", Some("QueryCompletionInformation")) => {
                apply_completion(
                    &frame.columns,
                    &frame.rows,
                    &mut response,
                    "EventTypeName",
                    "LevelName",
                    "Payload",
                );
            }
            ("DataSetCompletion", _) => {
                if frame.cancelled {
                    response.messages.push(QueryMessage {
                        severity: MessageSeverity::Warning,
                        text: "The query was cancelled before it completed.".to_owned(),
                    });
                }
                for error in &frame.one_api_errors {
                    response.messages.push(error_message(error));
                }
            }
            _ => {}
        }
    }
    response.visualization = table_ids.iter().enumerate().find_map(|(index, id)| {
        id.and_then(|id| visualizations.get(&id))
            .map(|visualization| Visualization {
                table_index: index,
                ..visualization.clone()
            })
    });
    Ok(response)
}

fn parse_v1(body: &[u8], row_cap: usize) -> Result<QueryResponse, KustoError> {
    let parsed: V1Response<'_> =
        serde_json::from_str(utf8(body)?).map_err(|error| protocol("invalid v1 response", &error))?;
    let tables = parsed.tables;
    let mut response = QueryResponse::default();
    let toc = tables.last().filter(|table| {
        tables.len() > 1
            && table.columns.len() == TOC_COLUMNS.len()
            && table
                .columns
                .iter()
                .zip(TOC_COLUMNS)
                .all(|(column, name)| column.column_name == name)
    });
    let Some(toc) = toc else {
        for table in &tables {
            let built = build_table(
                table.table_name.clone(),
                &table.columns,
                &table.rows,
                row_cap,
                &mut response.messages,
            )?;
            response.tables.push(built);
        }
        return Ok(response);
    };
    for row in &toc.rows {
        let Ok(Value::Array(cells)) = serde_json::from_str::<Value>(row.get()) else {
            continue;
        };
        let (Some(ordinal), Some(kind), name) = (
            cells
                .first()
                .and_then(Value::as_u64)
                .and_then(|ordinal| usize::try_from(ordinal).ok()),
            cells.get(1).and_then(Value::as_str),
            cells.get(2).and_then(Value::as_str),
        ) else {
            continue;
        };
        let Some(table) = tables.get(ordinal) else {
            continue;
        };
        match kind {
            "QueryResult" => {
                let name = name.unwrap_or(&table.table_name).to_owned();
                let built = build_table(name, &table.columns, &table.rows, row_cap, &mut response.messages)?;
                response.tables.push(built);
            }
            "QueryProperties" if response.visualization.is_none() => {
                response.visualization =
                    table
                        .rows
                        .iter()
                        .find_map(|row| match serde_json::from_str::<Value>(row.get()) {
                            Ok(Value::Array(cells)) => cells.last().and_then(parse_visualization),
                            _ => None,
                        });
            }
            _ => {}
        }
    }
    if let Some(status) = tables.iter().find(|table| {
        ["SeverityName", "StatusDescription"]
            .iter()
            .all(|name| table.columns.iter().any(|column| column.column_name == *name))
    }) {
        apply_completion(
            &status.columns,
            &status.rows,
            &mut response,
            "SeverityName",
            "SeverityName",
            "StatusDescription",
        );
    }
    Ok(response)
}

fn build_table(
    name: String,
    columns: &[Column],
    rows: &[&RawValue],
    row_cap: usize,
    messages: &mut Vec<QueryMessage>,
) -> Result<ResultTable, KustoError> {
    let mut kept = Vec::with_capacity(rows.len().min(row_cap));
    let mut total_rows = 0;
    for row in rows {
        let text = row.get().trim_start();
        if text.starts_with('{') {
            let value: Value = serde_json::from_str(text).map_err(|error| protocol("invalid row", &error))?;
            for key in ["OneApiErrors", "Exceptions"] {
                for error in value.get(key).and_then(Value::as_array).into_iter().flatten() {
                    messages.push(error_message(error));
                }
            }
            continue;
        }
        total_rows += 1;
        if kept.len() < row_cap {
            kept.push(serde_json::from_str::<Vec<Value>>(text).map_err(|error| protocol("invalid row", &error))?);
        }
    }
    Ok(ResultTable {
        name,
        columns: columns
            .iter()
            .map(|column| ResultColumn {
                name: column.column_name.clone(),
                column_type: column.kusto_type(),
            })
            .collect(),
        truncated: total_rows > kept.len(),
        rows: kept,
        total_rows,
    })
}

fn error_message(error: &Value) -> QueryMessage {
    let text = match error {
        Value::String(text) => text.clone(),
        _ => {
            let inner = error.get("error").unwrap_or(error);
            inner
                .get("@message")
                .or_else(|| inner.get("message"))
                .and_then(Value::as_str)
                .map_or_else(|| inner.to_string(), str::to_owned)
        }
    };
    QueryMessage {
        severity: MessageSeverity::Error,
        text: clip(&text),
    }
}

fn apply_completion(
    columns: &[Column],
    rows: &[&RawValue],
    response: &mut QueryResponse,
    event_column: &str,
    level_column: &str,
    payload_column: &str,
) {
    let index = |name: &str| columns.iter().position(|column| column.column_name == name);
    let (Some(event), Some(level), Some(payload)) = (index(event_column), index(level_column), index(payload_column))
    else {
        return;
    };
    for row in rows {
        let Ok(Value::Array(cells)) = serde_json::from_str::<Value>(row.get()) else {
            continue;
        };
        let text = |position: usize| cells.get(position).and_then(Value::as_str).unwrap_or_default();
        let payload_text = text(payload);
        if matches!(text(event), "QueryResourceConsumption" | "Stats") {
            if let Ok(payload) = serde_json::from_str::<Value>(payload_text) {
                apply_resource_stats(&payload, &mut response.stats);
            }
            continue;
        }
        let severity = match text(level) {
            "Warning" => MessageSeverity::Warning,
            "Error" | "Critical" | "Fatal" => MessageSeverity::Error,
            _ => continue,
        };
        let message = serde_json::from_str::<Value>(payload_text)
            .ok()
            .and_then(|payload| payload.get("Text").and_then(Value::as_str).map(str::to_owned))
            .unwrap_or_else(|| payload_text.to_owned());
        response.messages.push(QueryMessage {
            severity,
            text: clip(&message),
        });
    }
}

fn apply_resource_stats(payload: &Value, stats: &mut QueryStats) {
    let number = |pointer: &str| payload.pointer(pointer).and_then(Value::as_u64);
    stats.execution_time_ms = payload
        .get("ExecutionTime")
        .and_then(Value::as_f64)
        .map(|seconds| seconds * 1000.0);
    stats.cpu_time = payload
        .pointer("/resource_usage/cpu/total cpu")
        .and_then(Value::as_str)
        .map(str::to_owned);
    stats.memory_peak_bytes = number("/resource_usage/memory/peak_per_node");
    stats.rows_scanned = number("/input_dataset_statistics/rows/scanned");
    stats.extents_scanned = number("/input_dataset_statistics/extents/scanned");
    stats.extents_total = number("/input_dataset_statistics/extents/total");
}

/// Parses the `Visualization` property of a result set.
#[must_use]
pub fn parse_visualization(value: &Value) -> Option<Visualization> {
    let parsed;
    let value = match value {
        Value::String(text) => {
            parsed = serde_json::from_str::<Value>(text).ok()?;
            &parsed
        }
        other => other,
    };
    let text = |key: &str| {
        value
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_owned)
    };
    let list = |key: &str| match value.get(key) {
        Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).map(str::to_owned).collect(),
        Some(Value::String(items)) => items
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    };
    let bound = |key: &str| value.get(key).and_then(Value::as_f64).filter(|bound| bound.is_finite());
    let visualization = text("Visualization")?;
    if visualization.eq_ignore_ascii_case("table") {
        return None;
    }
    Some(Visualization {
        table_index: 0,
        visualization,
        title: text("Title"),
        x_column: text("XColumn"),
        y_columns: list("YColumns"),
        series: list("Series"),
        x_title: text("XTitle"),
        y_title: text("YTitle"),
        kind: text("Kind"),
        legend: text("Legend"),
        y_scale: text("YAxis"),
        ymin: bound("Ymin"),
        ymax: bound("Ymax"),
    })
}

/// Reads the first table of a management response as rows keyed by column name.
///
/// # Errors
///
/// Returns [`KustoError::Protocol`] when the body is not a v1 response.
pub fn first_table_records(body: &[u8]) -> Result<Vec<BTreeMap<String, Value>>, KustoError> {
    let parsed: V1Response<'_> =
        serde_json::from_str(utf8(body)?).map_err(|error| protocol("invalid v1 response", &error))?;
    let Some(table) = parsed.tables.first() else {
        return Ok(Vec::new());
    };
    let mut records = Vec::with_capacity(table.rows.len());
    for row in &table.rows {
        let Ok(Value::Array(cells)) = serde_json::from_str::<Value>(row.get()) else {
            continue;
        };
        records.push(
            table
                .columns
                .iter()
                .map(|column| column.column_name.clone())
                .zip(cells)
                .collect(),
        );
    }
    Ok(records)
}

/// Parses `.show databases`.
///
/// # Errors
///
/// Returns [`KustoError::Protocol`] when the body is not a v1 response.
pub fn parse_databases(body: &[u8]) -> Result<Vec<DatabaseInfo>, KustoError> {
    let mut databases: Vec<_> = first_table_records(body)?
        .into_iter()
        .filter_map(|record| {
            let name = record.get("DatabaseName")?.as_str()?.to_owned();
            let pretty_name = record
                .get("PrettyName")
                .and_then(Value::as_str)
                .filter(|pretty| !pretty.is_empty())
                .map(str::to_owned);
            Some(DatabaseInfo { name, pretty_name })
        })
        .collect();
    databases.sort_by_key(|database| database.name.to_lowercase());
    Ok(databases)
}

/// Parses `.show version` into a display string.
///
/// # Errors
///
/// Returns [`KustoError::Protocol`] when the body is not a v1 response.
pub fn parse_version(body: &[u8]) -> Result<Option<String>, KustoError> {
    Ok(first_table_records(body)?.into_iter().next().and_then(|record| {
        ["ProductVersion", "BuildVersion"]
            .iter()
            .find_map(|key| record.get(*key).and_then(Value::as_str).map(str::to_owned))
    }))
}

/// Parses `.cancel query` into whether a running query was cancelled and the service reason.
///
/// # Errors
///
/// Returns [`KustoError::Protocol`] when the body is not a v1 response.
pub fn parse_cancel(body: &[u8]) -> Result<(bool, Option<String>), KustoError> {
    let record = first_table_records(body)?.into_iter().next().unwrap_or_default();
    Ok((
        record
            .get("RunningQueryCanceled")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        record.get("ReasonPhrase").and_then(Value::as_str).map(str::to_owned),
    ))
}

/// Extracts the JSON document returned by `.show database X schema as json`.
///
/// # Errors
///
/// Returns [`KustoError::Protocol`] when the response does not contain a schema document.
pub fn parse_schema_document(body: &[u8]) -> Result<Value, KustoError> {
    let text = first_table_records(body)?
        .into_iter()
        .next()
        .and_then(|record| record.into_values().next())
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| KustoError::Protocol("the schema response was empty".to_owned()))?;
    serde_json::from_str(&text).map_err(|error| protocol("invalid schema document", &error))
}

/// Entities found in a schema document.
#[derive(Debug, Default, PartialEq)]
pub struct SchemaEntities {
    /// Tables.
    pub tables: Vec<EntitySchema>,
    /// Materialized views.
    pub materialized_views: Vec<EntitySchema>,
    /// External tables.
    pub external_tables: Vec<EntitySchema>,
    /// Functions.
    pub functions: Vec<FunctionSchema>,
}

/// Normalizes the entities of `database` in a schema document.
#[must_use]
pub fn normalize_schema(document: &Value, database: &str) -> SchemaEntities {
    let databases = document.get("Databases").and_then(Value::as_object);
    let Some(schema) = databases.and_then(|databases| {
        databases.get(database).or_else(|| {
            databases
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(database))
                .map(|(_, schema)| schema)
        })
    }) else {
        return SchemaEntities::default();
    };
    let entities = |key: &str| {
        let mut entities: Vec<EntitySchema> = schema
            .get(key)
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .map(|(name, entity)| EntitySchema {
                name: string(entity, "Name").unwrap_or_else(|| name.clone()),
                folder: string(entity, "Folder"),
                doc_string: string(entity, "DocString"),
                columns: entity
                    .get("OrderedColumns")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .map(|column| ColumnSchema {
                        name: string(column, "Name").unwrap_or_default(),
                        column_type: string(column, "CslType")
                            .unwrap_or_else(|| clr_to_kusto(&string(column, "Type").unwrap_or_default()).to_owned()),
                        doc_string: string(column, "DocString"),
                    })
                    .collect(),
            })
            .collect();
        entities.sort_by_key(|entity| entity.name.to_lowercase());
        entities
    };
    let mut functions: Vec<FunctionSchema> = schema
        .get("Functions")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .map(|(name, function)| FunctionSchema {
            name: string(function, "Name").unwrap_or_else(|| name.clone()),
            folder: string(function, "Folder"),
            doc_string: string(function, "DocString"),
            parameters: format!(
                "({})",
                function
                    .get("InputParameters")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .map(parameter)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            body: string(function, "Body").unwrap_or_default(),
        })
        .collect();
    functions.sort_by_key(|function| function.name.to_lowercase());
    SchemaEntities {
        tables: entities("Tables"),
        materialized_views: entities("MaterializedViews"),
        external_tables: entities("ExternalTables"),
        functions,
    }
}

fn string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

fn parameter(value: &Value) -> String {
    let name = string(value, "Name").unwrap_or_default();
    let columns: Vec<String> = value
        .get("Columns")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|column| {
            format!(
                "{}:{}",
                string(column, "Name").unwrap_or_default(),
                string(column, "CslType").unwrap_or_else(|| "dynamic".to_owned())
            )
        })
        .collect();
    let kind = if columns.is_empty() {
        string(value, "CslType").unwrap_or_else(|| clr_to_kusto(&string(value, "Type").unwrap_or_default()).to_owned())
    } else {
        format!("({})", columns.join(", "))
    };
    match string(value, "CslDefaultValue") {
        Some(default) => format!("{name}:{kind} = {default}"),
        None => format!("{name}:{kind}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Vec<u8> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name);
        std::fs::read(path).unwrap_or_default()
    }

    fn parsed(kind: QueryKind, name: &str, row_cap: usize) -> QueryResponse {
        parse_response(kind, &fixture(name), row_cap).unwrap_or_default()
    }

    #[test]
    fn v2_render_produces_table_visualization_and_stats() {
        let response = parsed(QueryKind::Query, "v2-columnchart.json", 1000);
        assert_eq!(response.tables.len(), 1);
        let table = &response.tables[0];
        assert_eq!(table.rows.len(), 26);
        assert_eq!(table.total_rows, 26);
        assert!(!table.truncated);
        assert_eq!(table.columns[1].name, "Rows");
        assert_eq!(table.columns[1].column_type, "long");
        let visualization = response.visualization.unwrap_or_default();
        assert_eq!(visualization.visualization, "columnchart");
        assert_eq!(visualization.title.as_deref(), Some("Rows per table"));
        assert_eq!(visualization.ymin, None);
        assert!(response.stats.execution_time_ms.is_some_and(|ms| ms > 0.0));
        assert_eq!(response.stats.rows_scanned, Some(634_880));
        assert_eq!(response.stats.result_rows, 26);
        assert!(response.messages.is_empty(), "{:?}", response.messages);
    }

    #[test]
    fn v2_render_properties_map_lists_and_bounds() {
        let visualization = parsed(QueryKind::Query, "v2-linechart.json", 10)
            .visualization
            .unwrap_or_default();
        assert_eq!(visualization.x_column.as_deref(), Some("x"));
        assert_eq!(visualization.y_columns, ["y", "z"]);
        assert_eq!(visualization.series, ["s"]);
        assert_eq!(visualization.kind.as_deref(), Some("stacked"));
        assert_eq!(visualization.legend.as_deref(), Some("hidden"));
        assert_eq!(visualization.y_scale.as_deref(), Some("log"));
        assert_eq!((visualization.ymin, visualization.ymax), (Some(1.0), Some(10.0)));
    }

    #[test]
    fn v2_keeps_every_primary_table_and_exact_long_values() {
        let response = parsed(QueryKind::Query, "v2-types.json", 10);
        assert_eq!(response.tables.len(), 2);
        assert!(response.visualization.is_none());
        let table = &response.tables[0];
        let types: Vec<_> = table.columns.iter().map(|column| column.column_type.as_str()).collect();
        assert_eq!(
            types,
            [
                "string", "datetime", "dynamic", "timespan", "real", "bool", "guid", "decimal", "long", "int", "long"
            ]
        );
        assert_eq!(
            serde_json::to_string(&table.rows[0][8]).unwrap_or_default(),
            "9007199254740993"
        );
        assert_eq!(table.rows[0][10], Value::Null);
        assert_eq!(response.tables[1].rows, vec![vec![json!(2)]]);
    }

    #[test]
    fn row_cap_truncates_and_warns() {
        let response = parsed(QueryKind::Query, "v2-columnchart.json", 3);
        assert_eq!(response.tables[0].rows.len(), 3);
        assert_eq!(response.tables[0].total_rows, 26);
        assert!(response.tables[0].truncated);
        assert_eq!(response.stats.result_rows, 26);
        assert!(
            response
                .messages
                .iter()
                .any(|message| message.severity == MessageSeverity::Warning)
        );
    }

    #[test]
    fn partial_failures_become_error_messages() {
        let response = parsed(QueryKind::Query, "v2-truncated.json", 100);
        assert_eq!(response.tables[0].rows.len(), 5);
        assert!(
            response
                .messages
                .iter()
                .any(|message| message.severity == MessageSeverity::Error
                    && message.text.contains("E_QUERY_RESULT_SET_TOO_LARGE"))
        );
    }

    #[test]
    fn v1_uses_table_of_contents_and_status_stats() {
        let response = parsed(QueryKind::Command, "v1-tables-count.json", 100);
        assert_eq!(response.kind, QueryKind::Command);
        assert_eq!(response.tables.len(), 1);
        assert_eq!(response.tables[0].name, "PrimaryResult");
        assert_eq!(response.tables[0].columns[0].column_type, "long");
        assert_eq!(response.tables[0].rows, vec![vec![json!(26)]]);
        assert!(response.visualization.is_none());
        assert!(response.stats.execution_time_ms.is_some());
    }

    #[test]
    fn v1_without_table_of_contents_returns_every_table() {
        let response = parsed(QueryKind::Command, "v1-show-version.json", 100);
        assert_eq!(response.tables.len(), 1);
        assert_eq!(response.tables[0].columns[1].column_type, "datetime");
    }

    #[test]
    fn service_errors_prefer_the_detailed_message() {
        let KustoError::Service {
            code, message, detail, ..
        } = service_error(StatusCode::BAD_REQUEST, &fixture("v2-error.json"))
        else {
            panic!("expected a service error");
        };
        assert_eq!(code.as_deref(), Some("General_BadRequest"));
        assert!(message.contains("SEM0100"), "{message}");
        assert!(detail.unwrap_or_default().contains("KustoBadRequestException"));
        let plain = service_error(
            StatusCode::BAD_REQUEST,
            b"General_BadRequest: Request is invalid.\nError details: x",
        );
        assert!(matches!(&plain, KustoError::Service { code: Some(code), .. } if code == "General_BadRequest"));
        assert_eq!(plain.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn management_helpers_parse_real_responses() {
        let databases = parse_databases(&fixture("v1-show-databases.json")).unwrap_or_default();
        let names: Vec<_> = databases.iter().map(|database| database.name.as_str()).collect();
        assert_eq!(names, ["loadtest", "NetDefaultDB"]);
        let version = parse_version(&fixture("v1-show-version.json")).unwrap_or_default();
        assert!(version.is_some_and(|version| version.contains("master")));
        let (cancelled, reason) = parse_cancel(&fixture("v1-cancel.json")).unwrap_or_default();
        assert!(!cancelled);
        assert!(reason.is_some());
    }

    #[test]
    fn schema_document_normalizes_entities() {
        let document = parse_schema_document(&fixture("v1-schema.json")).unwrap_or_default();
        let entities = normalize_schema(&document, "loadtest");
        assert_eq!(entities.tables.len(), 26);
        assert!(
            entities
                .tables
                .windows(2)
                .all(|pair| pair[0].name.to_lowercase() <= pair[1].name.to_lowercase())
        );
        assert!(entities.tables.iter().all(|table| !table.columns.is_empty()));
        assert_eq!(entities.tables[0].columns[0].column_type, "datetime");

        let synthetic = json!({"Databases": {"Db": {
            "Tables": {},
            "MaterializedViews": {"mv": {"Name": "mv", "OrderedColumns": [{"Name": "c", "Type": "System.Int64"}]}},
            "ExternalTables": {"ext": {"Name": "ext", "Folder": "lake", "OrderedColumns": []}},
            "Functions": {"f": {"Name": "f", "Body": "{ T }", "DocString": "doc", "InputParameters": [
                {"Name": "since", "CslType": "datetime", "CslDefaultValue": "ago(1d)"},
                {"Name": "T", "Columns": [{"Name": "x", "CslType": "long"}]}
            ]}}
        }}});
        let entities = normalize_schema(&synthetic, "db");
        assert_eq!(entities.materialized_views[0].columns[0].column_type, "long");
        assert_eq!(entities.external_tables[0].folder.as_deref(), Some("lake"));
        assert_eq!(
            entities.functions[0].parameters,
            "(since:datetime = ago(1d), T:(x:long))"
        );
        assert_eq!(entities.functions[0].doc_string.as_deref(), Some("doc"));
        assert_eq!(normalize_schema(&synthetic, "missing"), SchemaEntities::default());
    }

    #[test]
    fn timespans_and_clr_types_are_formatted() {
        assert_eq!(timespan(Duration::from_secs(240)), "00:04:00");
        assert_eq!(timespan(Duration::from_secs(3600)), "01:00:00");
        assert_eq!(clr_to_kusto("System.Int64"), "long");
        assert_eq!(clr_to_kusto("SByte"), "bool");
        assert_eq!(clr_to_kusto("Unknown"), "string");
    }
}
