use std::sync::Arc;

use arrowcol_protocol::{CollectorCapabilities, PressureSnapshot, ProtocolVersion};
use async_trait::async_trait;
use bytes::Bytes;
use serde::Serialize;

use crate::{RuntimeError, RuntimeResult};

/// Conventional health endpoint path.
pub const HEALTH_PATH: &str = "/healthz";
/// Conventional readiness endpoint path.
pub const READINESS_PATH: &str = "/readyz";
/// Arrowcol capability document endpoint path.
pub const CAPABILITIES_PATH: &str = "/v1/capabilities";
/// Arrowcol pressure snapshot endpoint path.
pub const PRESSURE_PATH: &str = "/v1/pressure";

/// Process health returned by reusable admin handlers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HealthStatus {
    protocol_version: ProtocolVersion,
    healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl HealthStatus {
    /// Creates a healthy status.
    #[must_use]
    pub const fn healthy() -> Self {
        Self {
            protocol_version: ProtocolVersion::CURRENT,
            healthy: true,
            reason: None,
        }
    }

    /// Creates an unhealthy status with a safe operator-facing reason.
    #[must_use]
    pub fn unhealthy(reason: impl Into<String>) -> Self {
        Self {
            protocol_version: ProtocolVersion::CURRENT,
            healthy: false,
            reason: Some(reason.into()),
        }
    }

    /// Returns whether the process is healthy.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        self.healthy
    }

    /// Returns the optional health reason.
    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

/// Readiness returned by reusable admin handlers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReadinessStatus {
    protocol_version: ProtocolVersion,
    ready: bool,
    accepting_deliveries: bool,
    active_streams: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl ReadinessStatus {
    /// Creates a readiness document.
    #[must_use]
    pub fn new(ready: bool, accepting_deliveries: bool, active_streams: usize, reason: Option<String>) -> Self {
        Self {
            protocol_version: ProtocolVersion::CURRENT,
            ready,
            accepting_deliveries,
            active_streams,
            reason,
        }
    }

    /// Returns whether the process should receive routed traffic.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.ready
    }

    /// Returns whether new deliveries are currently admitted.
    #[must_use]
    pub const fn accepting_deliveries(&self) -> bool {
        self.accepting_deliveries
    }

    /// Returns the number of active requests.
    #[must_use]
    pub const fn active_streams(&self) -> usize {
        self.active_streams
    }

    /// Returns the optional readiness reason.
    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

/// Supplies process health without choosing an HTTP framework.
pub trait HealthReporter: Send + Sync {
    /// Returns current process health.
    fn health(&self) -> HealthStatus;
}

/// Supplies routing readiness without choosing an HTTP framework.
pub trait ReadinessReporter: Send + Sync {
    /// Returns current process readiness.
    fn readiness(&self) -> ReadinessStatus;
}

/// Supplies the versioned collector capability document.
#[async_trait]
pub trait CapabilityReporter: Send + Sync {
    /// Loads current capabilities.
    ///
    /// # Errors
    ///
    /// Returns a safe internal failure when capabilities cannot be produced.
    async fn capabilities(&self) -> RuntimeResult<CollectorCapabilities>;
}

/// Supplies the versioned collector pressure document.
#[async_trait]
pub trait PressureReporter: Send + Sync {
    /// Loads current pressure.
    ///
    /// # Errors
    ///
    /// Returns a safe internal failure when pressure cannot be produced.
    async fn pressure(&self) -> RuntimeResult<PressureSnapshot>;
}

/// Framework-neutral HTTP response parts for an admin endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminHttpResponse {
    status: u16,
    content_type: &'static str,
    body: Bytes,
}

impl AdminHttpResponse {
    /// Returns the HTTP status code.
    #[must_use]
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the response content type.
    #[must_use]
    pub const fn content_type(&self) -> &'static str {
        self.content_type
    }

    /// Returns the encoded response body.
    #[must_use]
    pub const fn body(&self) -> &Bytes {
        &self.body
    }
}

/// Reusable health, readiness, capability, and pressure handlers.
#[derive(Clone)]
pub struct AdminHandlers {
    health: Arc<dyn HealthReporter>,
    readiness: Arc<dyn ReadinessReporter>,
    capabilities: Arc<dyn CapabilityReporter>,
    pressure: Arc<dyn PressureReporter>,
}

impl AdminHandlers {
    /// Creates framework-neutral admin handlers.
    #[must_use]
    pub fn new(
        health: Arc<dyn HealthReporter>,
        readiness: Arc<dyn ReadinessReporter>,
        capabilities: Arc<dyn CapabilityReporter>,
        pressure: Arc<dyn PressureReporter>,
    ) -> Self {
        Self {
            health,
            readiness,
            capabilities,
            pressure,
        }
    }

    /// Returns a health response.
    #[must_use]
    pub fn health(&self) -> AdminHttpResponse {
        let status = self.health.health();
        json_response(if status.is_healthy() { 200 } else { 503 }, &status)
    }

    /// Returns a readiness response.
    #[must_use]
    pub fn readiness(&self) -> AdminHttpResponse {
        let status = self.readiness.readiness();
        json_response(if status.is_ready() { 200 } else { 503 }, &status)
    }

    /// Returns a capability response.
    pub async fn capabilities(&self) -> AdminHttpResponse {
        match self.capabilities.capabilities().await {
            Ok(capabilities) => json_response(200, &capabilities),
            Err(error) => error_response(error),
        }
    }

    /// Returns a pressure response.
    pub async fn pressure(&self) -> AdminHttpResponse {
        match self.pressure.pressure().await {
            Ok(pressure) => json_response(200, &pressure),
            Err(error) => error_response(error),
        }
    }
}

fn json_response(status: u16, payload: &impl Serialize) -> AdminHttpResponse {
    match serde_json::to_vec(payload) {
        Ok(body) => AdminHttpResponse {
            status,
            content_type: "application/json",
            body: Bytes::from(body),
        },
        Err(_) => error_response(RuntimeError::internal()),
    }
}

fn error_response(error: RuntimeError) -> AdminHttpResponse {
    #[derive(Serialize)]
    struct ErrorDocument<'a> {
        protocol_version: ProtocolVersion,
        error: &'a str,
    }

    let payload = ErrorDocument {
        protocol_version: ProtocolVersion::CURRENT,
        error: error.public_message(),
    };
    let body = serde_json::to_vec(&payload)
        .map(Bytes::from)
        .unwrap_or_else(|_| Bytes::from_static(b"{\"error\":\"collector operation failed\"}"));
    AdminHttpResponse {
        status: 500,
        content_type: "application/json",
        body,
    }
}
