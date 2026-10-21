//! Requests to the kusto-explorer server.

use std::{cell::RefCell, collections::HashMap};

use kusto_explorer_api::{
    ApiError, AppConfig, CancelRequest, CancelResponse, DatabaseInfo, DatabaseSchema, QueryRequest, QueryResponse,
    Readiness,
};
use leptos::prelude::window;
use serde::de::DeserializeOwned;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;

use crate::browser;

/// Error code reported when a request was aborted by the user.
pub const ABORTED: &str = "Aborted";

thread_local! {
    static ABORTS: RefCell<HashMap<String, web_sys::AbortController>> = RefCell::new(HashMap::new());
}

fn failure(message: impl Into<String>, detail: Option<String>, code: &str) -> ApiError {
    ApiError {
        message: message.into(),
        detail,
        code: Some(code.to_owned()),
    }
}

fn js_failure(value: &JsValue) -> ApiError {
    if browser::js_name(value).as_deref() == Some("AbortError") {
        return failure("The request was cancelled.", None, ABORTED);
    }
    failure(
        "The kusto-explorer server could not be reached.",
        Some(browser::js_message(value)),
        "NetworkError",
    )
}

fn encode(segment: &str) -> String {
    String::from(js_sys::encode_uri_component(segment))
}

struct Reply {
    ok: bool,
    status: u16,
    text: String,
}

async fn send(method: &str, url: &str, body: Option<String>, abort_key: Option<&str>) -> Result<Reply, ApiError> {
    let init = web_sys::RequestInit::new();
    init.set_method(method);
    let headers = web_sys::Headers::new().map_err(|error| js_failure(&error))?;
    let _ = headers.set("Accept", "application/json");
    if let Some(body) = &body {
        let _ = headers.set("Content-Type", "application/json");
        init.set_body(&JsValue::from_str(body));
    }
    init.set_headers(&headers);
    if let Some(key) = abort_key
        && let Ok(controller) = web_sys::AbortController::new()
    {
        init.set_signal(Some(&controller.signal()));
        ABORTS.with(|aborts| aborts.borrow_mut().insert(key.to_owned(), controller));
    }
    let reply = async {
        let response: web_sys::Response = JsFuture::from(window().fetch_with_str_and_init(url, &init))
            .await
            .map_err(|error| js_failure(&error))?
            .unchecked_into();
        let text = JsFuture::from(response.text().map_err(|error| js_failure(&error))?)
            .await
            .map_err(|error| js_failure(&error))?;
        Ok(Reply {
            ok: response.ok(),
            status: response.status(),
            text: text.as_string().unwrap_or_default(),
        })
    }
    .await;
    if let Some(key) = abort_key {
        ABORTS.with(|aborts| aborts.borrow_mut().remove(key));
    }
    reply
}

fn decode<T: DeserializeOwned>(reply: Reply) -> Result<T, ApiError> {
    if reply.ok {
        return serde_json::from_str(&reply.text).map_err(|error| {
            failure(
                "The server sent a response that could not be read.",
                Some(error.to_string()),
                "InvalidResponse",
            )
        });
    }
    Err(serde_json::from_str::<ApiError>(&reply.text)
        .ok()
        .filter(|error| !error.message.is_empty())
        .unwrap_or_else(|| {
            let detail = (!reply.text.trim().is_empty()).then(|| reply.text.chars().take(4000).collect());
            failure(
                format!("The request failed with HTTP {}.", reply.status),
                detail,
                &format!("Http{}", reply.status),
            )
        }))
}

async fn get<T: DeserializeOwned>(url: &str) -> Result<T, ApiError> {
    decode(send("GET", url, None, None).await?)
}

/// Loads the server configuration.
///
/// # Errors
///
/// Returns the server or network error.
pub async fn config() -> Result<AppConfig, ApiError> {
    get("/api/config").await
}

/// Lists the databases of a connection.
///
/// # Errors
///
/// Returns the server or network error.
pub async fn databases(connection_id: &str) -> Result<Vec<DatabaseInfo>, ApiError> {
    get(&format!("/api/connections/{}/databases", encode(connection_id))).await
}

/// Loads the schema of a database.
///
/// # Errors
///
/// Returns the server or network error.
pub async fn schema(connection_id: &str, database: &str) -> Result<DatabaseSchema, ApiError> {
    get(&format!(
        "/api/connections/{}/databases/{}/schema",
        encode(connection_id),
        encode(database)
    ))
    .await
}

/// Runs a query or management command; [`abort`] with the client request id stops waiting for it.
///
/// # Errors
///
/// Returns the Kusto, server, or network error, or [`ABORTED`] after [`abort`].
pub async fn query(request: &QueryRequest) -> Result<QueryResponse, ApiError> {
    let body = serde_json::to_string(request).map_err(|error| {
        failure(
            "The query could not be encoded.",
            Some(error.to_string()),
            "InvalidRequest",
        )
    })?;
    decode(send("POST", "/api/query", Some(body), Some(&request.client_request_id)).await?)
}

/// Stops waiting for the request started with `client_request_id`.
pub fn abort(client_request_id: &str) -> bool {
    ABORTS
        .with(|aborts| aborts.borrow_mut().remove(client_request_id))
        .map(|controller| controller.abort())
        .is_some()
}

/// Asks the cluster to cancel a running query.
///
/// # Errors
///
/// Returns the server or network error.
pub async fn cancel(request: &CancelRequest) -> Result<CancelResponse, ApiError> {
    let body = serde_json::to_string(request).map_err(|error| {
        failure(
            "The request could not be encoded.",
            Some(error.to_string()),
            "InvalidRequest",
        )
    })?;
    decode(send("POST", "/api/cancel", Some(body), None).await?)
}

/// Checks whether every configured cluster answers.
///
/// # Errors
///
/// Returns the network error; an unreachable cluster is reported in the result instead.
pub async fn readiness() -> Result<Readiness, ApiError> {
    let reply = send("GET", "/readyz", None, None).await?;
    serde_json::from_str(&reply.text).map_err(|error| {
        failure(
            "The readiness response could not be read.",
            Some(error.to_string()),
            "InvalidResponse",
        )
    })
}
