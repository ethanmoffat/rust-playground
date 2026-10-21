//! End-to-end API tests against an in-process mock cluster that replays captured responses.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::post,
};
use http_body_util::BodyExt;
use kusto_explorer::{
    Settings,
    config::parse_connections,
    kusto::KustoClient,
    server::{AppState, DOCUMENT_CSP, WORKER_CSP, router},
};
use serde_json::{Value, json};
use tower::ServiceExt;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Clone, Debug, Default)]
struct Recorded {
    route: String,
    csl: String,
    database: Option<String>,
    properties: String,
    request_id: String,
    app: String,
}

#[derive(Clone, Default)]
struct Mock {
    requests: Arc<Mutex<Vec<Recorded>>>,
}

impl Mock {
    fn requests(&self) -> Vec<Recorded> {
        self.requests
            .lock()
            .map(|requests| requests.clone())
            .unwrap_or_default()
    }

    fn last(&self) -> Recorded {
        self.requests().pop().unwrap_or_default()
    }
}

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)).unwrap_or_default()
}

async fn mock_kusto(State(mock): State<Mock>, uri: Uri, headers: HeaderMap, Json(body): Json<Value>) -> Response {
    let text = |value: Option<&Value>| value.and_then(Value::as_str).unwrap_or_default().to_owned();
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    let csl = text(body.get("csl"));
    if let Ok(mut requests) = mock.requests.lock() {
        requests.push(Recorded {
            route: uri.path().to_owned(),
            csl: csl.clone(),
            database: body.get("db").and_then(Value::as_str).map(str::to_owned),
            properties: text(body.get("properties")),
            request_id: header("x-ms-client-request-id"),
            app: header("x-ms-app"),
        });
    }
    let (status, name) = match csl.as_str() {
        ".show version" => (StatusCode::OK, "v1-show-version.json"),
        ".show databases" => (StatusCode::OK, "v1-show-databases.json"),
        ".show database ['loadtest'] schema as json" => (StatusCode::OK, "v1-schema.json"),
        ".show tables | count" => (StatusCode::OK, "v1-tables-count.json"),
        csl if csl.starts_with(".cancel query") => (StatusCode::OK, "v1-cancel.json"),
        csl if csl.contains("render columnchart") => (StatusCode::OK, "v2-columnchart.json"),
        csl if csl.contains("types") => (StatusCode::OK, "v2-types.json"),
        csl if csl.contains("huge") => return (StatusCode::OK, vec![b' '; 3 * 1024 * 1024]).into_response(),
        _ => (StatusCode::BAD_REQUEST, "v2-error.json"),
    };
    (status, [(header::CONTENT_TYPE, "application/json")], fixture(name)).into_response()
}

async fn start_mock() -> Result<(Mock, SocketAddr), std::io::Error> {
    let mock = Mock::default();
    let app = Router::new()
        .route("/v1/rest/mgmt", post(mock_kusto))
        .route("/v2/rest/query", post(mock_kusto))
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, app).await });
    Ok((mock, address))
}

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn static_dir() -> Result<TempDir, std::io::Error> {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!("kusto-explorer-test-{}-{id}", std::process::id()));
    std::fs::create_dir_all(directory.join("assets"))?;
    std::fs::write(
        directory.join("index.html"),
        "<!doctype html><title>kusto-explorer</title>",
    )?;
    std::fs::write(directory.join("assets/kusto.worker.js"), "self.onmessage = () => {};")?;
    std::fs::write(directory.join("assets/app.js"), "console.log('plain');")?;
    std::fs::write(directory.join("assets/app.js.gz"), [0x1f, 0x8b, 0x08, 0x00])?;
    Ok(TempDir(directory))
}

async fn app_with(connections: &str) -> Result<(Router, Mock, TempDir), Box<dyn std::error::Error>> {
    let (mock, address) = start_mock().await?;
    let directory = static_dir()?;
    let settings = Settings {
        bind: SocketAddr::from(([127, 0, 0, 1], 0)),
        connections: parse_connections(&connections.replace("{mock}", &format!("http://{address}")))?,
        default_database: Some("loadtest".to_owned()),
        static_dir: directory.0.clone(),
        query_timeout: Duration::from_secs(30),
        max_response_bytes: 1024 * 1024,
        default_row_cap: 1000,
    };
    let client = KustoClient::new(settings.max_response_bytes)?;
    Ok((router(Arc::new(AppState::new(settings, client))), mock, directory))
}

async fn app() -> Result<(Router, Mock, TempDir), Box<dyn std::error::Error>> {
    app_with("Loadtest emulator={mock}").await
}

async fn send(
    app: &Router,
    request: Request<Body>,
) -> Result<(StatusCode, HeaderMap, Vec<u8>), Box<dyn std::error::Error>> {
    let response = app.clone().oneshot(request).await?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.into_body().collect().await?.to_bytes().to_vec();
    Ok((status, headers, body))
}

async fn get_json(app: &Router, uri: &str) -> Result<(StatusCode, Value), Box<dyn std::error::Error>> {
    let (status, _, body) = send(app, Request::get(uri).body(Body::empty())?).await?;
    Ok((status, serde_json::from_slice(&body)?))
}

async fn post_json(app: &Router, uri: &str, body: &Value) -> Result<(StatusCode, Value), Box<dyn std::error::Error>> {
    let request = Request::post(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))?;
    let (status, _, body) = send(app, request).await?;
    Ok((status, serde_json::from_slice(&body)?))
}

fn query(text: &str) -> Value {
    json!({"connectionId": "loadtest-emulator", "database": "loadtest", "query": text, "clientRequestId": "KX.test;1"})
}

#[tokio::test]
async fn config_lists_connections_by_id() -> TestResult {
    let (app, _, _directory) = app().await?;
    let (status, config) = get_json(&app, "/api/config").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(config["connections"][0]["id"], "loadtest-emulator");
    assert_eq!(config["connections"][0]["name"], "Loadtest emulator");
    assert_eq!(config["connections"][0]["defaultDatabase"], "loadtest");
    assert_eq!(config["defaultRowCap"], 1000);
    Ok(())
}

#[tokio::test]
async fn queries_use_the_v2_endpoint_and_return_render_metadata() -> TestResult {
    let (app, mock, _directory) = app().await?;
    let (status, response) = post_json(
        &app,
        "/api/query",
        &query("union * | summarize Rows=count() by TableName | render columnchart"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["kind"], "query");
    assert_eq!(response["clientRequestId"], "KX.test;1");
    assert_eq!(response["tables"][0]["rows"].as_array().map(Vec::len), Some(26));
    assert_eq!(response["visualization"]["visualization"], "columnchart");
    assert_eq!(response["visualization"]["tableIndex"], 0);
    assert_eq!(response["stats"]["rowsScanned"], 634_880);
    assert!(
        response["stats"]["responseBytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    let recorded = mock.last();
    assert_eq!(recorded.route, "/v2/rest/query");
    assert_eq!(recorded.database.as_deref(), Some("loadtest"));
    assert_eq!(recorded.request_id, "KX.test;1");
    assert_eq!(recorded.app, "kusto-explorer");
    let properties: Value = serde_json::from_str(&recorded.properties)?;
    assert_eq!(properties["Options"]["servertimeout"], "00:00:30");
    Ok(())
}

#[tokio::test]
async fn row_cap_truncates_large_results() -> TestResult {
    let (app, _, _directory) = app().await?;
    let mut request = query("T | render columnchart");
    request["rowCap"] = json!(3);
    let (status, response) = post_json(&app, "/api/query", &request).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["tables"][0]["rows"].as_array().map(Vec::len), Some(3));
    assert_eq!(response["tables"][0]["totalRows"], 26);
    assert_eq!(response["tables"][0]["truncated"], true);
    assert_eq!(response["messages"][0]["severity"], "warning");
    Ok(())
}

#[tokio::test]
async fn exact_long_values_and_multiple_tables_survive() -> TestResult {
    let (app, _, _directory) = app().await?;
    let request = Request::post("/api/query")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(query("types").to_string()))?;
    let (status, _, body) = send(&app, request).await?;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body)?;
    assert!(text.contains("9007199254740993"), "{text}");
    let response: Value = serde_json::from_str(&text)?;
    assert_eq!(response["tables"].as_array().map(Vec::len), Some(2));
    Ok(())
}

#[tokio::test]
async fn management_commands_use_the_mgmt_endpoint_without_leading_comments() -> TestResult {
    let (app, mock, _directory) = app().await?;
    let (status, response) = post_json(&app, "/api/query", &query("// how many?\n.show tables | count")).await?;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["kind"], "command");
    assert_eq!(response["tables"][0]["rows"], json!([[26]]));
    let recorded = mock.last();
    assert_eq!(recorded.route, "/v1/rest/mgmt");
    assert_eq!(recorded.csl, ".show tables | count");
    Ok(())
}

#[tokio::test]
async fn service_errors_are_reported_with_detail() -> TestResult {
    let (app, _, _directory) = app().await?;
    let (status, error) = post_json(&app, "/api/query", &query("no_such_table | take 1")).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["code"], "General_BadRequest");
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|message| message.contains("SEM0100")),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn oversized_upstream_responses_are_rejected() -> TestResult {
    let (app, _, _directory) = app().await?;
    let (status, error) = post_json(&app, "/api/query", &query("huge")).await?;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|message| message.contains("byte limit")),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn invalid_requests_never_reach_the_cluster() -> TestResult {
    let (app, mock, _directory) = app().await?;
    let mut cases = Vec::new();
    let mut unknown = query("print 1");
    unknown["connectionId"] = json!("http://evil.example");
    cases.push((unknown, StatusCode::NOT_FOUND));
    let mut database = query("print 1");
    database["database"] = json!("x']; .drop table T //");
    cases.push((database, StatusCode::BAD_REQUEST));
    let mut request_id = query("print 1");
    request_id["clientRequestId"] = json!("bad\r\nid");
    cases.push((request_id, StatusCode::BAD_REQUEST));
    cases.push((query("   "), StatusCode::BAD_REQUEST));
    cases.push((query(&"x".repeat(1024 * 1024 + 1)), StatusCode::BAD_REQUEST));
    let mut row_cap = query("print 1");
    row_cap["rowCap"] = json!(0);
    cases.push((row_cap, StatusCode::BAD_REQUEST));
    for (request, expected) in cases {
        let (status, error) = post_json(&app, "/api/query", &request).await?;
        assert_eq!(status, expected, "{error}");
        assert!(error["message"].as_str().is_some_and(|message| !message.is_empty()));
    }
    let (status, error) = post_json(&app, "/api/query", &json!({"query": 1})).await?;
    assert!(status.is_client_error());
    assert!(error["message"].is_string());
    let (status, _) = get_json(&app, "/api/connections/loadtest-emulator/databases/bad%27name/schema").await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = get_json(&app, "/api/connections/nope/databases").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = get_json(&app, "/api/does-not-exist").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(mock.requests().is_empty(), "{:?}", mock.requests());
    Ok(())
}

#[tokio::test]
async fn databases_and_schema_are_normalized() -> TestResult {
    let (app, mock, _directory) = app().await?;
    let (status, databases) = get_json(&app, "/api/connections/loadtest-emulator/databases").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(databases[0]["name"], "loadtest");
    assert_eq!(mock.last().database, None);
    let (status, schema) = get_json(&app, "/api/connections/loadtest-emulator/databases/loadtest/schema").await?;
    assert_eq!(status, StatusCode::OK, "{schema}");
    assert_eq!(schema["database"], "loadtest");
    assert_eq!(
        schema["clusterUri"]
            .as_str()
            .map(|uri| uri.starts_with("http://127.0.0.1:")),
        Some(true)
    );
    assert_eq!(schema["tables"].as_array().map(Vec::len), Some(26));
    assert!(schema["raw"]["Databases"]["loadtest"]["Tables"].is_object());
    assert_eq!(mock.last().csl, ".show database ['loadtest'] schema as json");
    Ok(())
}

#[tokio::test]
async fn cancel_targets_the_client_request_id() -> TestResult {
    let (app, mock, _directory) = app().await?;
    let request = json!({"connectionId": "loadtest-emulator", "clientRequestId": "KX.execute;42"});
    let (status, response) = post_json(&app, "/api/cancel", &request).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["cancelled"], false);
    assert_eq!(mock.last().csl, ".cancel query \"KX.execute;42\"");
    Ok(())
}

#[tokio::test]
async fn readiness_reflects_every_connection() -> TestResult {
    let (app, _, _directory) = app().await?;
    let (status, readiness) = get_json(&app, "/readyz").await?;
    assert_eq!(status, StatusCode::OK, "{readiness}");
    assert_eq!(readiness["connections"][0]["reachable"], true);
    let (app, _, _directory) = app_with("Up={mock};Down=http://127.0.0.1:9").await?;
    let (status, readiness) = get_json(&app, "/readyz").await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(readiness["ready"], false);
    assert_eq!(readiness["connections"][0]["reachable"], true);
    assert_eq!(readiness["connections"][1]["reachable"], false);
    assert!(readiness["connections"][1]["error"].is_string());
    let (status, health) = get_json(&app, "/healthz").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(health["status"], "healthy");
    Ok(())
}

#[tokio::test]
async fn static_assets_carry_security_headers() -> TestResult {
    let (app, _, _directory) = app().await?;
    let header_text = |headers: &HeaderMap, name: header::HeaderName| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    let (status, headers, body) = send(&app, Request::get("/").body(Body::empty())?).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8(body)?.contains("kusto-explorer"));
    assert_eq!(header_text(&headers, header::CONTENT_SECURITY_POLICY), DOCUMENT_CSP);
    assert_eq!(header_text(&headers, header::CACHE_CONTROL), "no-cache");
    assert_eq!(header_text(&headers, header::X_CONTENT_TYPE_OPTIONS), "nosniff");

    let (_, headers, _) = send(&app, Request::get("/assets/kusto.worker.js").body(Body::empty())?).await?;
    assert_eq!(header_text(&headers, header::CONTENT_SECURITY_POLICY), WORKER_CSP);

    let request = Request::get("/assets/app.js")
        .header(header::ACCEPT_ENCODING, "gzip")
        .body(Body::empty())?;
    let (status, headers, _) = send(&app, request).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(header_text(&headers, header::CONTENT_ENCODING), "gzip");

    let (_, headers, _) = send(&app, Request::get("/api/config").body(Body::empty())?).await?;
    assert_eq!(header_text(&headers, header::CACHE_CONTROL), "no-store");
    let (status, _, _) = send(&app, Request::get("/missing.js").body(Body::empty())?).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}
