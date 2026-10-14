use std::str::FromStr;
use std::sync::Arc;

use arrowcol_flight::{AdminHandlers, CAPABILITIES_PATH, HEALTH_PATH, PRESSURE_PATH, READINESS_PATH};
use arrowcol_protocol::{ErrorCode, ProtocolVersion, ReceiptId, ReceiptStatus};
use arrowcol_receipts::ReceiptStore;
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, Response, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::metrics::CollectorMetrics;

/// Shared admin HTTP state.
#[derive(Clone)]
pub struct AdminState {
    handlers: AdminHandlers,
    receipts: Arc<dyn ReceiptStore>,
    max_receipt_ids: usize,
    metrics: CollectorMetrics,
}

impl AdminState {
    /// Creates admin state.
    #[must_use]
    pub const fn new(
        handlers: AdminHandlers,
        receipts: Arc<dyn ReceiptStore>,
        max_receipt_ids: usize,
        metrics: CollectorMetrics,
    ) -> Self {
        Self {
            handlers,
            receipts,
            max_receipt_ids,
            metrics,
        }
    }
}

/// Builds the complete health/readiness/capability/pressure/receipt/metrics API.
pub fn router(state: AdminState) -> Router {
    Router::new()
        .route(HEALTH_PATH, get(health))
        .route(READINESS_PATH, get(readiness))
        .route(CAPABILITIES_PATH, get(capabilities))
        .route(PRESSURE_PATH, get(pressure))
        .route("/metrics", get(metrics))
        .route("/v1/receipts/{receipt_id}", get(receipt))
        .route("/v1/receipts", get(receipts))
        .with_state(state)
}

/// Serves admin HTTP until cancellation.
///
/// # Errors
///
/// Returns listener/server failures.
pub async fn serve(listener: TcpListener, state: AdminState, cancellation: CancellationToken) -> std::io::Result<()> {
    axum::serve(listener, router(state))
        .with_graceful_shutdown(cancellation.cancelled_owned())
        .await
}

async fn health(State(state): State<AdminState>) -> Response<Body> {
    admin_response(state.handlers.health())
}

async fn readiness(State(state): State<AdminState>) -> Response<Body> {
    admin_response(state.handlers.readiness())
}

async fn capabilities(State(state): State<AdminState>) -> Response<Body> {
    admin_response(state.handlers.capabilities().await)
}

async fn pressure(State(state): State<AdminState>) -> Response<Body> {
    admin_response(state.handlers.pressure().await)
}

async fn metrics(State(state): State<AdminState>) -> Response<Body> {
    match state.metrics.encode() {
        Ok(body) => response(StatusCode::OK, "text/plain; version=0.0.4; charset=utf-8", body),
        Err(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Internal,
            "could not encode metrics",
        ),
    }
}

async fn receipt(State(state): State<AdminState>, Path(receipt_id): Path<String>) -> Response<Body> {
    let receipt_id = match ReceiptId::from_str(&receipt_id) {
        Ok(receipt_id) => receipt_id,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                ErrorCode::ReceiptTransitionInvalid,
                "receipt identifier is invalid",
            );
        }
    };
    match state.receipts.receipt(receipt_id).await {
        Ok(Some(record)) => match record.protocol_status() {
            Ok(status) => json(
                StatusCode::OK,
                &ReceiptResponse {
                    receipt_id,
                    found: true,
                    purge_allowed: record.can_client_purge(),
                    status: Some(status),
                },
            ),
            Err(_) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::Internal,
                "receipt state is invalid",
            ),
        },
        Ok(None) => api_error(
            StatusCode::NOT_FOUND,
            ErrorCode::ReceiptNotFound,
            "receipt was not found",
        ),
        Err(_) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Internal,
            "receipt persistence is unavailable",
        ),
    }
}

#[derive(Deserialize)]
struct ReceiptQuery {
    ids: String,
}

async fn receipts(State(state): State<AdminState>, Query(query): Query<ReceiptQuery>) -> Response<Body> {
    let values: Vec<_> = query.ids.split(',').filter(|value| !value.is_empty()).collect();
    if values.is_empty() || values.len() > state.max_receipt_ids {
        return api_error(
            StatusCode::BAD_REQUEST,
            ErrorCode::ReceiptTransitionInvalid,
            "receipt query exceeds its configured bound",
        );
    }
    let mut ids = Vec::with_capacity(values.len());
    for value in values {
        match ReceiptId::from_str(value) {
            Ok(id) => ids.push(id),
            Err(_) => {
                return api_error(
                    StatusCode::BAD_REQUEST,
                    ErrorCode::ReceiptTransitionInvalid,
                    "receipt query contains an invalid identifier",
                );
            }
        }
    }
    match state.receipts.receipts(&ids).await {
        Ok(records) => {
            let entries = ids
                .into_iter()
                .zip(records)
                .map(|(receipt_id, record)| match record {
                    Some(record) => match record.protocol_status() {
                        Ok(status) => ReceiptBatchEntry {
                            receipt_id,
                            found: true,
                            purge_allowed: record.can_client_purge(),
                            status: Some(status),
                            error: None,
                        },
                        Err(_) => ReceiptBatchEntry {
                            receipt_id,
                            found: true,
                            purge_allowed: false,
                            status: None,
                            error: Some("receipt state is invalid"),
                        },
                    },
                    None => ReceiptBatchEntry {
                        receipt_id,
                        found: false,
                        purge_allowed: false,
                        status: None,
                        error: Some("receipt was not found"),
                    },
                })
                .collect();
            json(StatusCode::OK, &ReceiptBatchResponse { receipts: entries })
        }
        Err(_) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Internal,
            "receipt persistence is unavailable",
        ),
    }
}

#[derive(Serialize)]
struct ReceiptResponse {
    receipt_id: ReceiptId,
    found: bool,
    purge_allowed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<ReceiptStatus>,
}

#[derive(Serialize)]
struct ReceiptBatchResponse {
    receipts: Vec<ReceiptBatchEntry>,
}

#[derive(Serialize)]
struct ReceiptBatchEntry {
    receipt_id: ReceiptId,
    found: bool,
    purge_allowed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<ReceiptStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'static str>,
}

#[derive(Serialize)]
struct ApiError {
    protocol_version: ProtocolVersion,
    code: ErrorCode,
    message: &'static str,
}

fn admin_response(parts: arrowcol_flight::AdminHttpResponse) -> Response<Body> {
    response(
        StatusCode::from_u16(parts.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        parts.content_type(),
        parts.body().clone(),
    )
}

fn api_error(status: StatusCode, code: ErrorCode, message: &'static str) -> Response<Body> {
    json(
        status,
        &ApiError {
            protocol_version: ProtocolVersion::CURRENT,
            code,
            message,
        },
    )
}

fn json(status: StatusCode, payload: &impl Serialize) -> Response<Body> {
    match serde_json::to_vec(payload) {
        Ok(body) => response(status, "application/json", body),
        Err(_) => response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "application/json",
            b"{\"message\":\"collector operation failed\"}".to_vec(),
        ),
    }
}

fn response(status: StatusCode, content_type: &'static str, body: impl Into<Body>) -> Response<Body> {
    let mut response = body.into().into_response();
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}
