//! HTTP routes, static assets, and security headers.

use std::{path::Path as FsPath, sync::Arc, time::Duration, time::Instant};

use axum::{
    Json, Router,
    extract::{Path, Request, State, rejection::JsonRejection},
    http::{HeaderName, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use kusto_explorer_api::{
    ApiError, AppConfig, CancelRequest, CancelResponse, ConnectionHealth, DatabaseInfo, DatabaseSchema, MAX_ROW_CAP,
    QueryKind, QueryRequest, QueryResponse, Readiness, is_management_command, is_valid_client_request_id,
    strip_leading_trivia,
};
use serde_json::json;
use tokio::task::JoinSet;
use tower_http::{compression::CompressionLayer, services::ServeDir, trace::TraceLayer};
use tracing::{debug, info, warn};

use crate::{
    config::{Connection, Settings, display_uri},
    kusto::{self, Execute, KustoClient, KustoError},
};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const MAX_QUERY_BYTES: usize = 1024 * 1024;
const METADATA_TIMEOUT: Duration = Duration::from_secs(60);
const READINESS_TIMEOUT: Duration = Duration::from_secs(3);
const WORKER_PATH: &str = "/assets/kusto.worker.js";

/// Content security policy for documents and scripts.
pub const DOCUMENT_CSP: &str = "default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; \
    style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; font-src 'self' data:; worker-src 'self' blob:; \
    connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'";

/// Content security policy for the language-service worker, whose runtime compiles code with `Function`.
pub const WORKER_CSP: &str = "default-src 'self'; script-src 'self' 'unsafe-eval' 'wasm-unsafe-eval'; \
    connect-src 'self'; object-src 'none'; base-uri 'none'";

/// Shared request state.
#[derive(Debug)]
pub struct AppState {
    settings: Settings,
    client: KustoClient,
}

impl AppState {
    /// Creates the state shared by every request.
    #[must_use]
    pub fn new(settings: Settings, client: KustoClient) -> Self {
        Self { settings, client }
    }
}

/// Builds the application router.
pub fn router(state: Arc<AppState>) -> Router {
    let static_dir = state.settings.static_dir.clone();
    if !static_dir.join("index.html").is_file() {
        warn!(static_dir = %static_dir.display(), "index.html not found; only the API will be served");
    }
    let api = Router::new()
        .route("/config", get(config))
        .route("/connections/{id}/databases", get(databases))
        .route("/connections/{id}/databases/{database}/schema", get(schema))
        .route("/query", post(query))
        .route("/cancel", post(cancel))
        .fallback(api_not_found);
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .nest("/api", api)
        .fallback_service(static_files(&static_dir))
        .with_state(state)
        .layer(middleware::from_fn(response_headers))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
}

fn static_files(directory: &FsPath) -> ServeDir {
    ServeDir::new(directory).precompressed_br().precompressed_gzip()
}

async fn response_headers(request: Request, next: Next) -> Response {
    let path = request.uri().path().to_owned();
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    let dynamic = path.starts_with("/api/") || path == "/healthz" || path == "/readyz";
    let policy = if path == WORKER_PATH { WORKER_CSP } else { DOCUMENT_CSP };
    // No Cross-Origin-Opener-Policy: the UI is usually served over plain HTTP on a non-localhost
    // origin (for example http://kusto-explorer:8080), where browsers ignore it and log an error.
    let values: [(HeaderName, &'static str); 6] = [
        (header::CONTENT_SECURITY_POLICY, policy),
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        (header::REFERRER_POLICY, "no-referrer"),
        (header::X_FRAME_OPTIONS, "DENY"),
        (HeaderName::from_static("cross-origin-resource-policy"), "same-origin"),
        (header::CACHE_CONTROL, if dynamic { "no-store" } else { "no-cache" }),
    ];
    for (name, value) in values {
        headers.insert(name, HeaderValue::from_static(value));
    }
    response
}

/// Error returned by API handlers.
#[derive(Debug)]
pub struct Failure {
    status: StatusCode,
    body: ApiError,
}

impl Failure {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ApiError {
                message: message.into(),
                detail: None,
                code: None,
            },
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }
}

impl From<KustoError> for Failure {
    fn from(error: KustoError) -> Self {
        Self {
            status: error.status(),
            body: error.to_api(),
        }
    }
}

impl From<JsonRejection> for Failure {
    fn from(rejection: JsonRejection) -> Self {
        Self::new(rejection.status(), rejection.body_text())
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

type ApiResult<T> = Result<Json<T>, Failure>;

async fn healthz() -> Json<serde_json::Value> {
    Json(json!({"status": "healthy", "version": VERSION}))
}

async fn readyz(State(state): State<Arc<AppState>>) -> (StatusCode, Json<Readiness>) {
    let mut probes = JoinSet::new();
    for (index, connection) in state.settings.connections.iter().enumerate() {
        let state = Arc::clone(&state);
        let connection = connection.clone();
        probes.spawn(async move {
            let probe = tokio::time::timeout(READINESS_TIMEOUT, version(&state.client, &connection)).await;
            let (reachable, version, error) = match probe {
                Ok(Ok(version)) => (true, version, None),
                Ok(Err(error)) => (false, None, Some(error.to_string())),
                Err(_) => (false, None, Some(KustoError::Timeout(READINESS_TIMEOUT).to_string())),
            };
            (
                index,
                ConnectionHealth {
                    id: connection.id,
                    reachable,
                    version,
                    error,
                },
            )
        });
    }
    let mut connections = Vec::with_capacity(state.settings.connections.len());
    while let Some(result) = probes.join_next().await {
        match result {
            Ok(probe) => connections.push(probe),
            Err(error) => warn!(%error, "readiness probe task failed"),
        }
    }
    connections.sort_by_key(|(index, _)| *index);
    let connections: Vec<_> = connections.into_iter().map(|(_, health)| health).collect();
    let ready =
        connections.len() == state.settings.connections.len() && connections.iter().all(|health| health.reachable);
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(Readiness { ready, connections }))
}

async fn version(client: &KustoClient, connection: &Connection) -> Result<Option<String>, KustoError> {
    let body = client
        .execute(Execute {
            cluster: &connection.uri,
            database: None,
            csl: ".show version",
            kind: QueryKind::Command,
            client_request_id: &request_id("readyz"),
            timeout: READINESS_TIMEOUT,
        })
        .await?;
    kusto::parse_version(&body)
}

async fn config(State(state): State<Arc<AppState>>) -> Json<AppConfig> {
    Json(AppConfig {
        version: VERSION.to_owned(),
        connections: state.settings.connection_infos(),
        default_row_cap: state.settings.default_row_cap,
    })
}

async fn api_not_found() -> Failure {
    Failure::new(StatusCode::NOT_FOUND, "Unknown API route")
}

fn connection<'a>(state: &'a AppState, id: &str) -> Result<&'a Connection, Failure> {
    state
        .settings
        .connection(id)
        .ok_or_else(|| Failure::new(StatusCode::NOT_FOUND, format!("Unknown connection {id:?}")))
}

/// Returns whether `name` is a database name the server will forward.
#[must_use]
pub fn is_valid_database_name(name: &str) -> bool {
    (1..=260).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b' ' | b'.' | b'-'))
        && !name.trim().is_empty()
}

fn database_name(name: &str) -> Result<&str, Failure> {
    if is_valid_database_name(name) {
        Ok(name)
    } else {
        Err(Failure::bad_request(format!("Invalid database name {name:?}")))
    }
}

fn request_id(action: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    format!("KX.{action};{nanos:x}")
}

async fn databases(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> ApiResult<Vec<DatabaseInfo>> {
    let connection = connection(&state, &id)?;
    let body = state
        .client
        .execute(Execute {
            cluster: &connection.uri,
            database: None,
            csl: ".show databases",
            kind: QueryKind::Command,
            client_request_id: &request_id("databases"),
            timeout: METADATA_TIMEOUT,
        })
        .await?;
    Ok(Json(kusto::parse_databases(&body)?))
}

async fn schema(
    State(state): State<Arc<AppState>>,
    Path((id, database)): Path<(String, String)>,
) -> ApiResult<DatabaseSchema> {
    let connection = connection(&state, &id)?;
    let database = database_name(&database)?;
    let command = format!(".show database ['{database}'] schema as json");
    let body = state
        .client
        .execute(Execute {
            cluster: &connection.uri,
            database: Some(database),
            csl: &command,
            kind: QueryKind::Command,
            client_request_id: &request_id("schema"),
            timeout: METADATA_TIMEOUT,
        })
        .await?;
    let document = tokio::task::spawn_blocking(move || kusto::parse_schema_document(&body))
        .await
        .map_err(|error| Failure::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))??;
    let entities = kusto::normalize_schema(&document, database);
    Ok(Json(DatabaseSchema {
        connection_id: connection.id.clone(),
        cluster_uri: display_uri(&connection.uri),
        database: database.to_owned(),
        tables: entities.tables,
        materialized_views: entities.materialized_views,
        external_tables: entities.external_tables,
        functions: entities.functions,
        raw: document,
    }))
}

async fn query(
    State(state): State<Arc<AppState>>,
    request: Result<Json<QueryRequest>, JsonRejection>,
) -> ApiResult<QueryResponse> {
    let Json(request) = request?;
    let connection = connection(&state, &request.connection_id)?;
    let database = match request.database.trim() {
        "" => None,
        name => Some(database_name(name)?),
    };
    if !is_valid_client_request_id(&request.client_request_id) {
        return Err(Failure::bad_request("Invalid client request id"));
    }
    if request.query.trim().is_empty() {
        return Err(Failure::bad_request("The query is empty"));
    }
    if request.query.len() > MAX_QUERY_BYTES {
        return Err(Failure::bad_request("The query is larger than 1 MiB"));
    }
    let row_cap = request.row_cap.unwrap_or(state.settings.default_row_cap);
    if !(1..=MAX_ROW_CAP).contains(&row_cap) {
        return Err(Failure::bad_request(format!(
            "The row limit must be between 1 and {MAX_ROW_CAP}"
        )));
    }
    let (kind, csl) = if is_management_command(&request.query) {
        (QueryKind::Command, strip_leading_trivia(&request.query))
    } else {
        (QueryKind::Query, request.query.as_str())
    };
    debug!(connection = %connection.id, query = csl, "running request");
    let started = Instant::now();
    let body = state
        .client
        .execute(Execute {
            cluster: &connection.uri,
            database,
            csl,
            kind,
            client_request_id: &request.client_request_id,
            timeout: state.settings.query_timeout,
        })
        .await
        .inspect_err(|error| {
            info!(connection = %connection.id, client_request_id = %request.client_request_id, %error, "request failed");
        })?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let response_bytes = body.len();
    let mut response = tokio::task::spawn_blocking(move || kusto::parse_response(kind, &body, row_cap))
        .await
        .map_err(|error| Failure::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))??;
    response.client_request_id = request.client_request_id;
    response.stats.server_elapsed_ms = elapsed_ms;
    response.stats.response_bytes = response_bytes;
    info!(
        connection = %connection.id,
        client_request_id = %response.client_request_id,
        kind = ?kind,
        tables = response.tables.len(),
        rows = response.stats.result_rows,
        elapsed_ms = elapsed_ms.round(),
        "request completed"
    );
    Ok(Json(response))
}

async fn cancel(
    State(state): State<Arc<AppState>>,
    request: Result<Json<CancelRequest>, JsonRejection>,
) -> ApiResult<CancelResponse> {
    let Json(request) = request?;
    let connection = connection(&state, &request.connection_id)?;
    if !is_valid_client_request_id(&request.client_request_id) {
        return Err(Failure::bad_request("Invalid client request id"));
    }
    let command = format!(".cancel query \"{}\"", request.client_request_id);
    let body = state
        .client
        .execute(Execute {
            cluster: &connection.uri,
            database: None,
            csl: &command,
            kind: QueryKind::Command,
            client_request_id: &request_id("cancel"),
            timeout: METADATA_TIMEOUT,
        })
        .await?;
    let (cancelled, message) = kusto::parse_cancel(&body)?;
    Ok(Json(CancelResponse { cancelled, message }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_names_reject_quotes_and_brackets() {
        assert!(is_valid_database_name("loadtest"));
        assert!(is_valid_database_name("My Db.v2-x_y"));
        assert!(!is_valid_database_name(""));
        assert!(!is_valid_database_name("   "));
        assert!(!is_valid_database_name("db']; .drop table T"));
        assert!(!is_valid_database_name(&"a".repeat(261)));
    }

    #[test]
    fn generated_request_ids_are_valid() {
        assert!(is_valid_client_request_id(&request_id("schema")));
    }
}
