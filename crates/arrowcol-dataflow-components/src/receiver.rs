use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrowcol_catalog::FlightSqlDdlHandler;
use arrowcol_flight::{
    AllowAllAuthenticator, ArrowColFlightService, BatchAdmission, DrainController, PutResultEmitter, RuntimeError,
    RuntimeResult, StreamAbortReason, StreamCompletion, StreamContext, StreamLifecycle, StreamSummary,
};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, CollectorInstanceId, ErrorCode, ImplementationPool,
    PositiveMeasurement, PrincipalId, PrincipalIssuer,
};
use arrowcol_routing::{CollectorRegistration, CollectorState, RoutingStore};
use async_trait::async_trait;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ReceiverConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::{CallData, NodeControlMsg};
use otel_arrow_dfe_engine::error::{Error, ReceiverErrorKind, TypedError};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_engine::shared::receiver;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::{
    Interests, MessageSourceSharedEffectHandlerExtension, ProducerEffectHandlerExtension, ReceiverFactory,
};
use otel_arrow_dfe_otap::OTAP_RECEIVER_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::OtapPayload;
use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::FlightReceiverConfig;
use crate::config::{checked_add_millis, config_error, now_unix_millis, parse_config};
use crate::envelope::{ArrowcolBatchEnvelope, encode_record_batch};
use crate::metrics::{PendingTracker, RuntimeMetrics, global_metrics, process_instance_id};
use crate::routing::{LeaseResolver, StoreLeaseResolver};

/// DFE URN for arbitrary Arrow Flight ingress.
pub const ARROW_FLIGHT_RECEIVER_URN: &str = "urn:otel:receiver:arrow_flight";

/// Abstracts DFE message admission for focused zero-copy and backpressure tests.
#[async_trait]
pub trait PdataSender: Send + Sync {
    /// Sends one owned pdata message through a bounded downstream route.
    async fn send(&self, correlation_id: u64, pdata: OtapPdata) -> Result<(), PdataSendError>;
}

/// DFE send failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PdataSendError {
    /// The selected route is closed.
    #[error("DFE downstream route is closed")]
    Closed,
    /// The selected route remained unavailable.
    #[error("DFE downstream route is unavailable")]
    Unavailable,
}

struct EnginePdataSender {
    effect_handler: receiver::EffectHandler<OtapPdata>,
}

#[async_trait]
impl PdataSender for EnginePdataSender {
    async fn send(&self, correlation_id: u64, mut pdata: OtapPdata) -> Result<(), PdataSendError> {
        let mut call_data = CallData::new();
        call_data.push(correlation_id.into());
        self.effect_handler
            .subscribe_to(Interests::ACKS | Interests::NACKS, call_data, &mut pdata);
        self.effect_handler
            .send_message_with_source_node(pdata)
            .await
            .map_err(map_send_error)
    }
}

fn map_send_error(error: TypedError<OtapPdata>) -> PdataSendError {
    match error {
        TypedError::ChannelSendError(otel_arrow_dfe_channel::error::SendError::Closed(_))
        | TypedError::RuntimeMsgError(otel_arrow_dfe_channel::error::SendError::Closed(_))
        | TypedError::NodeControlMsgSendError {
            error: otel_arrow_dfe_channel::error::SendError::Closed(_),
            ..
        } => PdataSendError::Closed,
        TypedError::ChannelSendError(otel_arrow_dfe_channel::error::SendError::Full(_))
        | TypedError::RuntimeMsgError(otel_arrow_dfe_channel::error::SendError::Full(_))
        | TypedError::NodeControlMsgSendError {
            error: otel_arrow_dfe_channel::error::SendError::Full(_),
            ..
        }
        | TypedError::Error(_) => PdataSendError::Unavailable,
    }
}

/// Best-effort Arrow Flight admission into DFE channels.
pub struct DfeAdmission {
    sender: Arc<dyn PdataSender>,
    leases: Arc<dyn LeaseResolver>,
    instance_id: CollectorInstanceId,
    permits: Arc<Semaphore>,
    pending: Arc<PendingTracker>,
    metrics: Arc<RuntimeMetrics>,
}

impl DfeAdmission {
    /// Creates admission with a hard concurrent-stream bound.
    #[must_use]
    pub fn new(
        sender: Arc<dyn PdataSender>,
        leases: Arc<dyn LeaseResolver>,
        instance_id: CollectorInstanceId,
        max_concurrent_streams: usize,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            sender,
            leases,
            instance_id,
            permits: Arc::new(Semaphore::new(max_concurrent_streams)),
            pending: Arc::new(PendingTracker::new(Arc::clone(&metrics))),
            metrics,
        }
    }

    fn resolve_feedback(&self, correlation_id: u64, ack: bool) {
        self.pending.resolve(correlation_id, ack);
    }
}

#[async_trait]
impl BatchAdmission for DfeAdmission {
    async fn begin(
        &self,
        context: StreamContext,
        _emitter: Arc<dyn PutResultEmitter>,
    ) -> RuntimeResult<Box<dyn StreamLifecycle>> {
        let lease = self.leases.resolve(context.metadata().table()).await?;
        if lease.holder_instance_id != self.instance_id {
            return Err(RuntimeError::new(
                ErrorCode::LeaseFenced,
                tonic::Code::FailedPrecondition,
                "collector does not own the table lease",
            ));
        }
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::draining())?;
        let envelope = ArrowcolBatchEnvelope {
            table: context.metadata().table().clone(),
            source_schema_version: context.registered_schema().version(),
            target_schema_version: context.current_schema().version(),
            lease_id: lease.lease_id,
            holder_instance_id: lease.holder_instance_id,
            fencing_token: lease.fencing_epoch,
        };
        self.metrics.stream_started(&envelope.table);
        Ok(Box::new(DfeStreamLifecycle {
            sender: Arc::clone(&self.sender),
            pending: Arc::clone(&self.pending),
            metrics: Arc::clone(&self.metrics),
            envelope,
            permit: Some(permit),
            finished: false,
        }))
    }
}

struct DfeStreamLifecycle {
    sender: Arc<dyn PdataSender>,
    pending: Arc<PendingTracker>,
    metrics: Arc<RuntimeMetrics>,
    envelope: ArrowcolBatchEnvelope,
    permit: Option<OwnedSemaphorePermit>,
    finished: bool,
}

struct PendingSendGuard {
    pending: Arc<PendingTracker>,
    correlation_id: u64,
    armed: bool,
}

impl PendingSendGuard {
    fn new(pending: Arc<PendingTracker>, correlation_id: u64) -> Self {
        Self {
            pending,
            correlation_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingSendGuard {
    fn drop(&mut self) {
        if self.armed {
            self.pending.resolve(self.correlation_id, false);
        }
    }
}

impl DfeStreamLifecycle {
    fn finish(&mut self, completed: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        if completed {
            self.metrics.stream_completed(&self.envelope.table);
        } else {
            self.metrics.stream_aborted(&self.envelope.table);
        }
        let _ = self.permit.take();
    }
}

impl Drop for DfeStreamLifecycle {
    fn drop(&mut self) {
        self.finish(false);
    }
}

#[async_trait]
impl StreamLifecycle for DfeStreamLifecycle {
    async fn push_batch(&mut self, batch: RecordBatch) -> RuntimeResult<()> {
        let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
        let bytes = arrow_deltalake::record_batch_logical_bytes(&batch).map_err(|_| RuntimeError::internal())?;
        self.metrics.batch_received(rows, bytes);
        let records = encode_record_batch(self.envelope.clone(), batch).map_err(|_| RuntimeError::internal())?;
        let correlation_id = self
            .pending
            .insert(
                self.envelope.table.clone(),
                rows,
                bytes,
                self.envelope.source_schema_version,
                self.envelope.target_schema_version,
            )
            .map_err(|_| RuntimeError::internal())?;
        let mut pending_send = PendingSendGuard::new(Arc::clone(&self.pending), correlation_id);
        let pdata = OtapPdata::new_todo_context(OtapPayload::from_pdata_arrow(records));
        match self.sender.send(correlation_id, pdata).await {
            Ok(()) => {
                pending_send.disarm();
                Ok(())
            }
            Err(error) => Err(match error {
                PdataSendError::Closed => RuntimeError::draining(),
                PdataSendError::Unavailable => RuntimeError::new(
                    ErrorCode::CapacityExhausted,
                    tonic::Code::Unavailable,
                    "DFE downstream admission is unavailable",
                ),
            }),
        }
    }

    async fn complete(&mut self, _summary: StreamSummary) -> RuntimeResult<StreamCompletion> {
        self.finish(true);
        Ok(StreamCompletion::best_effort())
    }

    async fn abort(mut self: Box<Self>, _reason: StreamAbortReason) {
        self.finish(false);
    }
}

struct ArrowFlightReceiver {
    config: FlightReceiverConfig,
    routing: Arc<dyn RoutingStore>,
    metrics: Arc<RuntimeMetrics>,
}

fn validate_receiver_config(value: &serde_json::Value) -> Result<(), otel_arrow_dfe_config::error::Error> {
    let config: FlightReceiverConfig = parse_config(value)?;
    config.validate().map_err(config_error)
}

fn create_receiver(
    _pipeline: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    receiver_config: &ReceiverConfig,
    _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities,
) -> Result<ReceiverWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    let config: FlightReceiverConfig = parse_config(&node_config.config)?;
    config.validate().map_err(config_error)?;
    let routing = config.routing.build().map_err(config_error)?;
    let metrics = global_metrics();
    metrics.configure_receiver(
        config.registration.max_concurrent_streams,
        config.registration.rss_limit_bytes,
        config.registration.buffered_bytes_limit,
        config.registration.buffered_rows_limit,
        config.registration.buffered_batches_limit,
        receiver_config.output_pdata_channel.capacity,
    );
    Ok(ReceiverWrapper::shared(
        ArrowFlightReceiver {
            config,
            routing,
            metrics,
        },
        node,
        node_config,
        receiver_config,
    ))
}

/// External DFE receiver registration.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Receiver)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static ARROW_FLIGHT_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: ARROW_FLIGHT_RECEIVER_URN,
    create: create_receiver,
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED
        .with_pdata(otel_arrow_dfe_engine::wiring_contract::PdataWiringContract::PLUGGABLE_ARROW_SOURCE),
    validate_config: validate_receiver_config,
};

#[async_trait]
impl receiver::Receiver<OtapPdata> for ArrowFlightReceiver {
    async fn start(
        self: Box<Self>,
        mut control: receiver::ControlChannel<OtapPdata>,
        effect_handler: receiver::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let instance_id = process_instance_id();
        let limits =
            self.config.limits.build().map_err(|error| {
                receiver_error(&effect_handler, ReceiverErrorKind::Configuration, error.to_string())
            })?;
        let (catalog, ddl) = self
            .config
            .catalog
            .start()
            .await
            .map_err(|error| receiver_error(&effect_handler, ReceiverErrorKind::Connect, error.to_string()))?;
        self.metrics.set_catalog_ready(true);
        let leases: Arc<dyn LeaseResolver> = Arc::new(StoreLeaseResolver::new(Arc::clone(&self.routing), instance_id));
        let admission = Arc::new(DfeAdmission::new(
            Arc::new(EnginePdataSender {
                effect_handler: effect_handler.clone(),
            }),
            leases,
            instance_id,
            self.config.registration.max_concurrent_streams,
            Arc::clone(&self.metrics),
        ));
        let admission_trait: Arc<dyn BatchAdmission> = Arc::clone(&admission) as Arc<dyn BatchAdmission>;
        let drain = DrainController::new();
        let ddl_handler: Arc<dyn FlightSqlDdlHandler> = ddl;
        let service = ArrowColFlightService::new(
            ImplementationPool::Dataflow,
            catalog,
            Arc::new(
                allow_all_authenticator()
                    .map_err(|error| receiver_error(&effect_handler, ReceiverErrorKind::Configuration, error))?,
            ),
            admission_trait,
            limits.clone(),
        )
        .map_err(|error| receiver_error(&effect_handler, ReceiverErrorKind::Configuration, error.to_string()))?
        .with_ddl_handler(ddl_handler)
        .with_drain_controller(drain.clone());

        register_collector(&self.routing, &self.config, instance_id, &self.metrics)
            .await
            .map_err(|error| receiver_error(&effect_handler, ReceiverErrorKind::Connect, error))?;

        let flight_listener = effect_handler.tcp_listener(self.config.listening_addr)?;
        let admin_listener = effect_handler.tcp_listener(self.config.admin_listening_addr)?;
        let shutdown = CancellationToken::new();
        let registration_shutdown = shutdown.child_token();
        let registration = tokio::spawn(registration_loop(
            Arc::clone(&self.routing),
            self.config.clone(),
            instance_id,
            Arc::clone(&self.metrics),
            registration_shutdown,
        ));
        let flight_shutdown = shutdown.child_token();
        let mut flight = tokio::spawn(async move {
            Server::builder()
                .add_service(service.into_tonic_service())
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(flight_listener),
                    flight_shutdown.cancelled_owned(),
                )
                .await
        });
        let admin_shutdown = shutdown.child_token();
        let admin_state = AdminState {
            metrics: Arc::clone(&self.metrics),
            max_request_bytes: u64::try_from(limits.max_message_bytes).unwrap_or(u64::MAX),
        };
        let mut admin = tokio::spawn(async move {
            axum::serve(admin_listener, admin_router(admin_state))
                .with_graceful_shutdown(admin_shutdown.cancelled_owned())
                .await
        });

        loop {
            tokio::select! {
                control_message = control.recv() => {
                    match control_message {
                        Ok(NodeControlMsg::Ack(ack)) => {
                            if let Some(id) = correlation_id(&ack.unwind.route.calldata) {
                                admission.resolve_feedback(id, true);
                            }
                        }
                        Ok(NodeControlMsg::Nack(nack)) => {
                            if let Some(id) = correlation_id(&nack.unwind.route.calldata) {
                                admission.resolve_feedback(id, false);
                            }
                        }
                        Ok(NodeControlMsg::DrainIngress { deadline, .. }) => {
                            self.metrics.set_accepting(false);
                            shutdown.cancel();
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            let _ = drain.graceful_stop(remaining).await;
                            finalize_registration(
                                &self.routing,
                                instance_id,
                                CollectorState::Draining,
                                &self.metrics,
                            ).await;
                            let _ = effect_handler.notify_receiver_drained().await;
                            await_tasks(flight, admin, registration, remaining).await;
                            return Ok(TerminalState::default());
                        }
                        Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                            self.metrics.set_accepting(false);
                            shutdown.cancel();
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            let _ = drain.graceful_stop(remaining).await;
                            finalize_registration(
                                &self.routing,
                                instance_id,
                                CollectorState::Stopped,
                                &self.metrics,
                            ).await;
                            await_tasks(flight, admin, registration, remaining).await;
                            return Ok(TerminalState::default());
                        }
                        Ok(_) => {}
                        Err(error) => {
                            shutdown.cancel();
                            self.metrics.set_healthy(false);
                            return Err(Error::ChannelRecvError(error));
                        }
                    }
                }
                result = &mut flight => {
                    shutdown.cancel();
                    self.metrics.set_healthy(false);
                    return Err(receiver_error(
                        &effect_handler,
                        ReceiverErrorKind::Transport,
                        task_result_message("Flight server", result),
                    ));
                }
                result = &mut admin => {
                    shutdown.cancel();
                    self.metrics.set_healthy(false);
                    return Err(receiver_error(
                        &effect_handler,
                        ReceiverErrorKind::Transport,
                        task_result_message("admin server", result),
                    ));
                }
            }
        }
    }
}

fn correlation_id(call_data: &CallData) -> Option<u64> {
    call_data.first().copied().map(u64::from)
}

fn allow_all_authenticator() -> Result<AllowAllAuthenticator, String> {
    let principal = AuthenticatedPrincipal::new(
        PrincipalId::new("local-development").map_err(|error| error.to_string())?,
        PrincipalIssuer::new("arrowcol-dataflow").map_err(|error| error.to_string())?,
        BTreeSet::new(),
    )
    .map_err(|error| error.to_string())?;
    let authenticated_at = now_unix_millis()?;
    let context = AuthContext::new(principal, AuthMethod::WorkloadIdentity, authenticated_at, None)
        .map_err(|error| error.to_string())?;
    Ok(AllowAllAuthenticator::new(context))
}

fn receiver_error(
    effect_handler: &receiver::EffectHandler<OtapPdata>,
    kind: ReceiverErrorKind,
    error: impl Into<String>,
) -> Error {
    Error::ReceiverError {
        receiver: effect_handler.receiver_id(),
        kind,
        error: error.into(),
        source_detail: String::new(),
    }
}

fn task_result_message<E>(name: &str, result: Result<Result<(), E>, tokio::task::JoinError>) -> String
where
    E: std::fmt::Display,
{
    match result {
        Ok(Ok(())) => format!("{name} stopped before receiver shutdown"),
        Ok(Err(error)) => format!("{name} failed: {error}"),
        Err(error) => format!("{name} task failed: {error}"),
    }
}

async fn await_tasks(
    mut flight: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    mut admin: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    mut registration: tokio::task::JoinHandle<()>,
    timeout: Duration,
) {
    let wait = async {
        let _ = (&mut flight).await;
        let _ = (&mut admin).await;
        let _ = (&mut registration).await;
    };
    if tokio::time::timeout(timeout, wait).await.is_err() {
        flight.abort();
        admin.abort();
        registration.abort();
    }
}

async fn register_collector(
    store: &Arc<dyn RoutingStore>,
    config: &FlightReceiverConfig,
    instance_id: CollectorInstanceId,
    metrics: &Arc<RuntimeMetrics>,
) -> Result<(), String> {
    let now = now_unix_millis()?;
    let expiry = checked_add_millis(now, config.registration.registration_ttl)?;
    store
        .register_collector(&CollectorRegistration {
            instance_id,
            instance_key: config.registration.instance_key.clone(),
            implementation_pool: ImplementationPool::Dataflow,
            flight_endpoint: config.registration.flight_endpoint,
            catalog_capable: true,
            max_concurrent_streams: PositiveMeasurement::new(
                u64::try_from(config.registration.max_concurrent_streams)
                    .map_err(|_| "max concurrent streams exceeds u64".to_owned())?,
            )
            .map_err(|error| error.to_string())?,
            started_at_unix_ms: now,
            heartbeat_at_unix_ms: now,
            lease_expires_at_unix_ms: expiry,
        })
        .await
        .map_err(|error| error.to_string())?;
    if let Ok(pressure) = metrics.pressure() {
        store
            .publish_pressure(&pressure.rich)
            .await
            .map_err(|error| error.to_string())?;
    }
    metrics.set_registered(true);
    Ok(())
}

async fn registration_loop(
    store: Arc<dyn RoutingStore>,
    config: FlightReceiverConfig,
    instance_id: CollectorInstanceId,
    metrics: Arc<RuntimeMetrics>,
    shutdown: CancellationToken,
) {
    let started = now_unix_millis().ok();
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + config.registration.heartbeat_interval,
        config.registration.heartbeat_interval,
    );
    let mut pressure = tokio::time::interval_at(
        tokio::time::Instant::now() + config.registration.pressure_interval,
        config.registration.pressure_interval,
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    pressure.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = heartbeat.tick() => {
                let Some(started) = started else {
                    metrics.set_registered(false);
                    continue;
                };
                let Ok(now) = now_unix_millis() else {
                    metrics.set_registered(false);
                    continue;
                };
                let Ok(expiry) = checked_add_millis(now, config.registration.registration_ttl) else {
                    metrics.set_registered(false);
                    continue;
                };
                let registration = CollectorRegistration {
                    instance_id,
                    instance_key: config.registration.instance_key.clone(),
                    implementation_pool: ImplementationPool::Dataflow,
                    flight_endpoint: config.registration.flight_endpoint,
                    catalog_capable: true,
                    max_concurrent_streams: match PositiveMeasurement::new(
                        u64::try_from(config.registration.max_concurrent_streams).unwrap_or(u64::MAX)
                    ) {
                        Ok(value) => value,
                        Err(_) => {
                            metrics.set_registered(false);
                            continue;
                        }
                    },
                    started_at_unix_ms: started,
                    heartbeat_at_unix_ms: now,
                    lease_expires_at_unix_ms: expiry,
                };
                metrics.set_registered(store.register_collector(&registration).await.is_ok());
            }
            _ = pressure.tick() => {
                if let Ok(snapshot) = metrics.pressure()
                    && store.publish_pressure(&snapshot.rich).await.is_err()
                {
                    metrics.set_registered(false);
                }
            }
        }
    }
}

async fn finalize_registration(
    store: &Arc<dyn RoutingStore>,
    instance_id: CollectorInstanceId,
    state: CollectorState,
    metrics: &Arc<RuntimeMetrics>,
) {
    let Ok(now) = now_unix_millis() else {
        return;
    };
    let _ = store.set_collector_state(instance_id, state, now).await;
    if let Ok(snapshot) = metrics.pressure() {
        let _ = store.publish_pressure(&snapshot.rich).await;
    }
}

#[derive(Clone)]
struct AdminState {
    metrics: Arc<RuntimeMetrics>,
    max_request_bytes: u64,
}

fn admin_router(state: AdminState) -> Router {
    Router::new()
        .route("/healthz", get(admin_health))
        .route("/readyz", get(admin_readiness))
        .route("/v1/capabilities", get(admin_capabilities))
        .route("/v1/pressure", get(admin_pressure))
        .route("/metrics", get(admin_metrics))
        .with_state(state)
}

#[derive(Serialize)]
struct HealthDocument {
    healthy: bool,
}

async fn admin_health(State(state): State<AdminState>) -> impl IntoResponse {
    let healthy = state.metrics.healthy();
    (
        if healthy {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(HealthDocument { healthy }),
    )
}

#[derive(Serialize)]
struct ReadinessDocument {
    ready: bool,
    accepting_deliveries: bool,
    delivery_guarantee: &'static str,
    receipt_polling: bool,
}

async fn admin_readiness(State(state): State<AdminState>) -> impl IntoResponse {
    let ready = state.metrics.ready();
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(ReadinessDocument {
            ready,
            accepting_deliveries: ready,
            delivery_guarantee: "best_effort",
            receipt_polling: false,
        }),
    )
}

async fn admin_capabilities(State(state): State<AdminState>) -> impl IntoResponse {
    Json(state.metrics.capabilities(state.max_request_bytes))
}

async fn admin_pressure(State(state): State<AdminState>) -> Response {
    match state.metrics.pressure() {
        Ok(pressure) => Json(pressure).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn admin_metrics(State(state): State<AdminState>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.prometheus(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::sync::Mutex;

    use anyhow::Result;
    use arrow_array::{ArrayRef, Int32Array, Int64Array, StringArray, UInt64Array};
    use arrow_flight::encode::FlightDataEncoderBuilder;
    use arrow_flight::error::FlightError;
    use arrow_flight::flight_descriptor::DescriptorType;
    use arrow_flight::{FlightData, FlightDescriptor};
    use arrow_schema::{DataType, Field, Schema};
    use arrowcol_catalog::{
        CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, MemoryCatalogStore, MemoryMetastore,
    };
    use arrowcol_flight::{DeliveryGuarantee, PutResultMetadata};
    use arrowcol_protocol::{
        BasisPoints, CatalogName, ClaimedSchemaFingerprint, IngestMetadata, Measurement, PressureGauge,
        ProtocolVersion, SchemaName, TableName, UnixMillis, fingerprint_schema,
    };
    use arrowcol_routing::{
        CollectorRegistration, LeaseClaim, LeaseKey, MemoryRoutingStore, RichPressureSnapshot, RoutingStore,
        SubsystemState,
    };
    use futures::{TryStreamExt, stream};
    use tonic::Request;
    use tonic::metadata::MetadataValue;

    use super::*;

    struct CaptureSender {
        sent: Mutex<Vec<(u64, OtapPdata)>>,
        gate: Option<Arc<Semaphore>>,
    }

    #[async_trait]
    impl PdataSender for CaptureSender {
        async fn send(&self, correlation_id: u64, pdata: OtapPdata) -> Result<(), PdataSendError> {
            let _permit = match &self.gate {
                Some(gate) => Some(
                    Arc::clone(gate)
                        .acquire_owned()
                        .await
                        .map_err(|_| PdataSendError::Closed)?,
                ),
                None => None,
            };
            self.sent.lock().expect("capture lock").push((correlation_id, pdata));
            Ok(())
        }
    }

    struct FailingSender;

    #[async_trait]
    impl PdataSender for FailingSender {
        async fn send(&self, _correlation_id: u64, _pdata: OtapPdata) -> Result<(), PdataSendError> {
            Err(PdataSendError::Unavailable)
        }
    }

    fn envelope() -> ArrowcolBatchEnvelope {
        ArrowcolBatchEnvelope {
            table: arrowcol_protocol::TableIdentity::new(
                CatalogName::new("demo").expect("catalog"),
                SchemaName::new("generated").expect("schema"),
                TableName::new("events").expect("table"),
            ),
            source_schema_version: arrowcol_catalog::SchemaVersion::FIRST,
            target_schema_version: arrowcol_catalog::SchemaVersion::FIRST,
            lease_id: arrowcol_protocol::LeaseId::new(),
            holder_instance_id: CollectorInstanceId::new(),
            fencing_token: arrowcol_protocol::FencingToken::new(1).expect("fence"),
        }
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![1])) as ArrayRef]).expect("batch")
    }

    async fn flight_request(
        identity: &arrowcol_protocol::TableIdentity,
        version: arrowcol_catalog::SchemaVersion,
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> Result<Request<impl futures::Stream<Item = Result<FlightData, tonic::Status>> + Send + 'static>> {
        let descriptor = FlightDescriptor {
            r#type: DescriptorType::Path as i32,
            cmd: Default::default(),
            path: vec![
                identity.catalog().to_string(),
                identity.schema().to_string(),
                identity.table().to_string(),
                format!("v{version}"),
            ],
        };
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(Arc::clone(&schema))
            .with_flight_descriptor(Some(descriptor))
            .build(stream::iter(batches.into_iter().map(Ok::<RecordBatch, FlightError>)))
            .try_collect::<Vec<_>>()
            .await?;
        let mut request = Request::new(stream::iter(encoded.into_iter().map(Ok)));
        let metadata = IngestMetadata::new(
            ProtocolVersion::CURRENT,
            ImplementationPool::Dataflow,
            identity.clone(),
            ClaimedSchemaFingerprint::new(fingerprint_schema(schema.as_ref())),
        );
        for (key, value) in metadata.to_ascii_pairs() {
            request
                .metadata_mut()
                .insert(key, MetadataValue::try_from(value.as_str())?);
        }
        Ok(request)
    }

    async fn routing_lease(
        store: &Arc<MemoryRoutingStore>,
        instance_id: CollectorInstanceId,
        table: &arrowcol_protocol::TableIdentity,
    ) -> Result<()> {
        let now = now_unix_millis().map_err(anyhow::Error::msg)?;
        store
            .register_collector(&CollectorRegistration {
                instance_id,
                instance_key: "dfe-flight-test".to_owned(),
                implementation_pool: ImplementationPool::Dataflow,
                flight_endpoint: "127.0.0.1:32010".parse()?,
                catalog_capable: true,
                max_concurrent_streams: PositiveMeasurement::new(2)?,
                started_at_unix_ms: now,
                heartbeat_at_unix_ms: now,
                lease_expires_at_unix_ms: UnixMillis::new(now.get() + 60_000)?,
            })
            .await?;
        let zero = PressureGauge::new(Measurement::ZERO, PositiveMeasurement::new(2)?)?;
        store
            .publish_pressure(&RichPressureSnapshot {
                instance_id,
                observed_at_unix_ms: now,
                cpu_utilization_bps: BasisPoints::new(0)?,
                rss_bytes: Measurement::ZERO,
                rss_limit_bytes: PositiveMeasurement::new(1)?,
                active_streams: zero,
                received_bytes: Measurement::ZERO,
                received_rows: Measurement::ZERO,
                received_batches: Measurement::ZERO,
                buffered_bytes: zero,
                buffered_rows: zero,
                buffered_batches: zero,
                available_permits: PressureGauge::new(Measurement::new(2)?, PositiveMeasurement::new(2)?)?,
                pending_commits: zero,
                oldest_buffer_age_ms: Measurement::ZERO,
                delta_pressure_bps: BasisPoints::new(0)?,
                quiver_pressure_bps: BasisPoints::new(0)?,
                dfe_pressure_bps: BasisPoints::new(0)?,
                delta_state: SubsystemState::Ready,
                quiver_state: SubsystemState::Disabled,
                dfe_state: SubsystemState::Ready,
                accepting_deliveries: true,
                tables: Vec::new(),
            })
            .await?;
        store
            .claim_lease(&LeaseClaim {
                key: LeaseKey::new(ImplementationPool::Dataflow, table.clone()),
                candidate_instance_id: instance_id,
                now_unix_ms: now,
                expires_at_unix_ms: UnixMillis::new(now.get() + 60_000)?,
                minimum_snapshot_at_unix_ms: now,
            })
            .await?;
        Ok(())
    }

    /// Scenario: A bounded DFE sender is saturated.
    /// Guarantees: The send future remains backpressured rather than dropping or bypassing capacity.
    #[tokio::test]
    async fn sender_backpressure_is_awaited() {
        let gate = Arc::new(Semaphore::new(0));
        let sender = Arc::new(CaptureSender {
            sent: Mutex::new(Vec::new()),
            gate: Some(Arc::clone(&gate)),
        });
        let records = crate::encode_record_batch(envelope(), batch()).expect("records");
        let pdata = OtapPdata::new_todo_context(OtapPayload::from_pdata_arrow(records));
        let send = sender.send(1, pdata);
        assert!(tokio::time::timeout(Duration::from_millis(20), send).await.is_err());
        gate.add_permits(1);
    }

    /// Scenario: A downstream DFE route fails after a Flight batch was decoded.
    /// Guarantees: The Flight lifecycle returns a capacity failure and removes the pressure entry.
    #[tokio::test]
    async fn downstream_failure_propagates_without_false_admission() {
        let metrics = Arc::new(RuntimeMetrics::new(CollectorInstanceId::new()));
        metrics.configure_receiver(1, 1_000_000, 1_000_000, 100, 10, 1);
        metrics.configure_exporter(1);
        let pending = Arc::new(PendingTracker::new(Arc::clone(&metrics)));
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&semaphore).acquire_owned().await.expect("permit");
        let envelope = envelope();
        metrics.stream_started(&envelope.table);
        let mut lifecycle = DfeStreamLifecycle {
            sender: Arc::new(FailingSender),
            pending,
            metrics: Arc::clone(&metrics),
            envelope,
            permit: Some(permit),
            finished: false,
        };

        let error = lifecycle.push_batch(batch()).await.expect_err("send failure");
        assert_eq!(error.code(), ErrorCode::CapacityExhausted);
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.buffered_batches.current().get(), 0);
    }

    /// Scenario: Flight admission is cancelled while a bounded DFE route is backpressured.
    /// Guarantees: The cancelled send removes its pending pressure entry instead of blocking drain forever.
    #[tokio::test]
    async fn cancelled_backpressured_send_releases_pending_pressure() {
        let gate = Arc::new(Semaphore::new(0));
        let metrics = Arc::new(RuntimeMetrics::new(CollectorInstanceId::new()));
        metrics.configure_receiver(1, 1_000_000, 1_000_000, 100, 10, 1);
        metrics.configure_exporter(1);
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&semaphore).acquire_owned().await.expect("permit");
        let envelope = envelope();
        metrics.stream_started(&envelope.table);
        let mut lifecycle = DfeStreamLifecycle {
            sender: Arc::new(CaptureSender {
                sent: Mutex::new(Vec::new()),
                gate: Some(gate),
            }),
            pending: Arc::new(PendingTracker::new(Arc::clone(&metrics))),
            metrics: Arc::clone(&metrics),
            envelope,
            permit: Some(permit),
            finished: false,
        };

        let send = lifecycle.push_batch(batch());
        assert!(tokio::time::timeout(Duration::from_millis(20), send).await.is_err());
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.buffered_batches.current().get(), 0);
        Box::new(lifecycle).abort(StreamAbortReason::ClientCancelled).await;
    }

    /// Scenario: Flight cancellation aborts an admitted stream before completion.
    /// Guarantees: The stream permit and active-stream pressure are released exactly once.
    #[tokio::test]
    async fn cancellation_releases_stream_capacity() {
        let metrics = Arc::new(RuntimeMetrics::new(CollectorInstanceId::new()));
        metrics.configure_receiver(1, 1_000_000, 1_000_000, 100, 10, 1);
        metrics.configure_exporter(1);
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&semaphore).acquire_owned().await.expect("permit");
        let envelope = envelope();
        metrics.stream_started(&envelope.table);
        let lifecycle = DfeStreamLifecycle {
            sender: Arc::new(CaptureSender {
                sent: Mutex::new(Vec::new()),
                gate: None,
            }),
            pending: Arc::new(PendingTracker::new(Arc::clone(&metrics))),
            metrics: Arc::clone(&metrics),
            envelope,
            permit: Some(permit),
            finished: false,
        };

        Box::new(lifecycle).abort(StreamAbortReason::ClientCancelled).await;
        assert_eq!(semaphore.available_permits(), 1);
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.active_streams.current().get(), 0);
    }

    /// Scenario: Flight receives an exact arbitrary Arrow schema on an actively leased table.
    /// Guarantees: The batch enters Arrowcol pdata and the final PutResult is explicitly best-effort without a receipt.
    #[tokio::test]
    async fn exact_flight_ingest_returns_best_effort_pdata() -> Result<()> {
        let catalog = Arc::new(MemoryCatalogStore::new());
        let metastore = Arc::new(MemoryMetastore::new());
        let control = CatalogControlPlane::new(
            DdlParser::default(),
            Arc::clone(&catalog) as Arc<dyn CatalogStore>,
            metastore,
        );
        let ddl = control
            .execute_ddl(arrowcol_catalog::FlightSqlDdlRequest::new(
                "CREATE TABLE demo.generated.events (id BIGINT NOT NULL, body VARCHAR)",
            ))
            .await?;
        let current = catalog
            .current_schema(ddl.identity())
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing schema"))?;
        let schema = Arc::new(current.arrow_schema()?);
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("one"), None])) as ArrayRef,
            ],
        )?;
        let routing = Arc::new(MemoryRoutingStore::default());
        let instance_id = CollectorInstanceId::new();
        routing_lease(&routing, instance_id, ddl.identity()).await?;
        let sender = Arc::new(CaptureSender {
            sent: Mutex::new(Vec::new()),
            gate: None,
        });
        let metrics = Arc::new(RuntimeMetrics::new(instance_id));
        metrics.configure_receiver(2, 1_000_000_000, 1_000_000, 10_000, 16, 2);
        metrics.configure_exporter(2);
        let admission = Arc::new(DfeAdmission::new(
            Arc::clone(&sender) as Arc<dyn PdataSender>,
            Arc::new(StoreLeaseResolver::new(
                Arc::clone(&routing) as Arc<dyn RoutingStore>,
                instance_id,
            )),
            instance_id,
            2,
            metrics,
        ));
        let service = ArrowColFlightService::new(
            ImplementationPool::Dataflow,
            catalog as Arc<dyn CatalogStore>,
            Arc::new(allow_all_authenticator().map_err(anyhow::Error::msg)?),
            admission as Arc<dyn BatchAdmission>,
            arrowcol_flight::RuntimeLimits::default(),
        )?;
        let results = service
            .handle_do_put(flight_request(ddl.identity(), current.version(), Arc::clone(&schema), vec![input]).await?)
            .await?
            .into_inner()
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(results.len(), 1);
        let completion = PutResultMetadata::from_put_result(&results[0])?;
        assert_eq!(completion.delivery_guarantee(), Some(DeliveryGuarantee::BestEffort));
        assert_eq!(completion.receipt_id(), None);
        let captured = sender.sent.lock().expect("capture lock");
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].1.signal_format(),
            otel_arrow_dfe_config::SignalFormat::PluggableArrowRecords
        );
        Ok(())
    }

    /// Scenario: Flight receives an unregistered breaking Arrow schema.
    /// Guarantees: Exact catalog validation rejects it before any DFE sender side effect.
    #[tokio::test]
    async fn breaking_flight_schema_is_rejected_before_dfe() -> Result<()> {
        let catalog = Arc::new(MemoryCatalogStore::new());
        let metastore = Arc::new(MemoryMetastore::new());
        let control = CatalogControlPlane::new(
            DdlParser::default(),
            Arc::clone(&catalog) as Arc<dyn CatalogStore>,
            metastore,
        );
        let ddl = control
            .execute_ddl(arrowcol_catalog::FlightSqlDdlRequest::new(
                "CREATE TABLE demo.generated.events (id BIGINT NOT NULL)",
            ))
            .await?;
        let current = catalog
            .current_schema(ddl.identity())
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing schema"))?;
        let routing = Arc::new(MemoryRoutingStore::default());
        let instance_id = CollectorInstanceId::new();
        routing_lease(&routing, instance_id, ddl.identity()).await?;
        let sender = Arc::new(CaptureSender {
            sent: Mutex::new(Vec::new()),
            gate: None,
        });
        let metrics = Arc::new(RuntimeMetrics::new(instance_id));
        metrics.configure_receiver(2, 1_000_000_000, 1_000_000, 10_000, 16, 2);
        let admission = Arc::new(DfeAdmission::new(
            Arc::clone(&sender) as Arc<dyn PdataSender>,
            Arc::new(StoreLeaseResolver::new(
                Arc::clone(&routing) as Arc<dyn RoutingStore>,
                instance_id,
            )),
            instance_id,
            2,
            metrics,
        ));
        let service = ArrowColFlightService::new(
            ImplementationPool::Dataflow,
            catalog as Arc<dyn CatalogStore>,
            Arc::new(allow_all_authenticator().map_err(anyhow::Error::msg)?),
            admission as Arc<dyn BatchAdmission>,
            arrowcol_flight::RuntimeLimits::default(),
        )?;
        let bad_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let bad_batch = RecordBatch::try_new(
            Arc::clone(&bad_schema),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )?;
        let status = match service
            .handle_do_put(flight_request(ddl.identity(), current.version(), bad_schema, vec![bad_batch]).await?)
            .await
        {
            Ok(_) => return Err(anyhow::anyhow!("schema unexpectedly reached DFE")),
            Err(status) => status,
        };
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(sender.sent.lock().expect("capture lock").is_empty());
        Ok(())
    }

    /// Scenario: Native Quiver persistence is requested for Arrowcol pdata.
    /// Guarantees: Strict validation reports the exact typed capability gap instead of inventing persistence.
    #[test]
    fn native_quiver_is_rejected_honestly() {
        let value = serde_json::json!({
            "listening_addr": "127.0.0.1:32010",
            "admin_listening_addr": "127.0.0.1:32011",
            "catalog": {"type": "memory"},
            "routing": {"type": "memory"},
            "registration": {
                "instance_key": "test",
                "flight_endpoint": "127.0.0.1:32010"
            },
            "native_quiver": true
        });
        let error = validate_receiver_config(&value).expect_err("unsupported");
        assert!(error.to_string().contains(crate::QUIVER_UNSUPPORTED_GAP));
    }
}
