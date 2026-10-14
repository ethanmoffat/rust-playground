use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PollInfo, PutResult, SchemaResult, Ticket,
};
use arrowcol_router_spike::{
    ACK_EVERY_HEADER, BACKEND_DELAY_HEADER, BACKEND_HEADER, EXPECTED_BATCHES_HEADER, EventSink, FAIL_AFTER_HEADER,
    REQUEST_ID_HEADER, ROUTING_HEADER, expected_schema, validate_request_id, validate_routing_key,
};
use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use futures::{Stream, StreamExt, TryStreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    id: String,
    #[arg(long)]
    listen: SocketAddr,
    #[arg(long)]
    events: PathBuf,
}

#[derive(Clone)]
struct ToyFlight {
    backend_id: String,
    events: EventSink,
}

struct RequestGuard {
    backend: String,
    request_id: String,
    route: String,
    events: EventSink,
    batches: usize,
    rows: usize,
    bytes: usize,
    finished: bool,
}

impl RequestGuard {
    fn new(backend: String, request_id: String, route: String, events: EventSink) -> Self {
        let _ = events.emit(format!(
            "kind=start\tbackend={backend}\trequest={request_id}\troute={route}"
        ));
        Self {
            backend,
            request_id,
            route,
            events,
            batches: 0,
            rows: 0,
            bytes: 0,
            finished: false,
        }
    }

    fn finish(&mut self, outcome: &str) {
        if self.finished {
            return;
        }
        let _ = self.events.emit(format!(
            "kind=finish\tbackend={}\trequest={}\troute={}\toutcome={outcome}\tbatches={}\trows={}\tbytes={}",
            self.backend, self.request_id, self.route, self.batches, self.rows, self.bytes
        ));
        self.finished = true;
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.finish("cancelled");
        }
    }
}

fn metadata_text(request: &Request<tonic::Streaming<FlightData>>, key: &'static str) -> Option<String> {
    request
        .metadata()
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn metadata_usize(
    request: &Request<tonic::Streaming<FlightData>>,
    key: &'static str,
    maximum: usize,
) -> Result<Option<usize>, Status> {
    metadata_text(request, key)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| Status::invalid_argument(format!("{key} is not an integer")))
                .and_then(|parsed| {
                    if parsed > maximum {
                        Err(Status::invalid_argument(format!("{key} exceeds maximum {maximum}")))
                    } else {
                        Ok(parsed)
                    }
                })
        })
        .transpose()
}

fn status_with_backend(code: Code, message: impl Into<String>, backend: &str) -> Status {
    let mut metadata = MetadataMap::new();
    if let Ok(value) = MetadataValue::try_from(backend) {
        metadata.insert(BACKEND_HEADER, value);
    }
    Status::with_metadata(code, message, metadata)
}

fn put_result(backend: &str, request_id: &str, outcome: &str, batches: usize, rows: usize, bytes: usize) -> PutResult {
    PutResult {
        app_metadata: Bytes::from(format!(
            "backend={backend};request={request_id};outcome={outcome};batches={batches};rows={rows};bytes={bytes}"
        )),
    }
}

#[async_trait]
impl FlightService for ToyFlight {
    type HandshakeStream = ResponseStream<HandshakeResponse>;
    type ListFlightsStream = ResponseStream<FlightInfo>;
    type DoGetStream = ResponseStream<FlightData>;
    type DoPutStream = ResponseStream<PutResult>;
    type DoExchangeStream = ResponseStream<FlightData>;
    type DoActionStream = ResponseStream<arrow_flight::Result>;
    type ListActionsStream = ResponseStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(&self, _request: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(&self, _request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(&self, _request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get"))
    }

    async fn do_put(
        &self,
        request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let route = metadata_text(&request, ROUTING_HEADER)
            .ok_or_else(|| Status::invalid_argument("missing routing metadata"))?;
        validate_routing_key(&route).map_err(Status::invalid_argument)?;

        let request_id = metadata_text(&request, REQUEST_ID_HEADER)
            .ok_or_else(|| Status::invalid_argument("missing request id metadata"))?;
        validate_request_id(&request_id).map_err(Status::invalid_argument)?;

        let fail_after = metadata_usize(&request, FAIL_AFTER_HEADER, 1_000_000)?;
        let expected_batches = metadata_usize(&request, EXPECTED_BATCHES_HEADER, 1_000_000)?
            .ok_or_else(|| Status::invalid_argument("missing expected batch count"))?;
        let ack_every = metadata_usize(&request, ACK_EVERY_HEADER, 1_024)?.unwrap_or(16).max(1);
        let delay_ms = metadata_usize(&request, BACKEND_DELAY_HEADER, 1_000)?.unwrap_or_default() as u64;

        let backend_id = self.backend_id.clone();
        let events = self.events.clone();
        let inbound = request.into_inner().map_err(Into::into);
        let mut record_batches = FlightRecordBatchStream::new_from_flight_data(inbound);
        let (sender, receiver) = mpsc::channel(8);
        let task_backend = backend_id.clone();
        let task_request_id = request_id.clone();

        tokio::spawn(async move {
            let mut guard = RequestGuard::new(task_backend.clone(), task_request_id.clone(), route, events);

            while let Some(item) = record_batches.next().await {
                let batch = match item {
                    Ok(batch) => batch,
                    Err(error) => {
                        let status = status_with_backend(
                            Code::InvalidArgument,
                            format!("invalid Arrow Flight stream: {error}"),
                            &task_backend,
                        );
                        let _ = sender.send(Err(status)).await;
                        guard.finish("decode_error");
                        return;
                    }
                };

                if batch.schema() != expected_schema() {
                    let status = status_with_backend(Code::InvalidArgument, "unexpected Arrow schema", &task_backend);
                    let _ = sender.send(Err(status)).await;
                    guard.finish("schema_error");
                    return;
                }

                guard.batches += 1;
                guard.rows += batch.num_rows();
                guard.bytes = guard.bytes.saturating_add(batch.get_array_memory_size());

                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }

                if fail_after == Some(guard.batches) {
                    let status = status_with_backend(
                        Code::Aborted,
                        format!("{} injected failure", task_backend),
                        &task_backend,
                    );
                    let _ = sender.send(Err(status)).await;
                    guard.finish("injected_failure");
                    return;
                }

                if guard.batches.is_multiple_of(ack_every)
                    && sender
                        .send(Ok(put_result(
                            &task_backend,
                            &task_request_id,
                            "progress",
                            guard.batches,
                            guard.rows,
                            guard.bytes,
                        )))
                        .await
                        .is_err()
                {
                    return;
                }
            }

            if guard.batches != expected_batches {
                let status = status_with_backend(
                    Code::Cancelled,
                    format!(
                        "{} received {} of {} expected batches",
                        task_backend, guard.batches, expected_batches
                    ),
                    &task_backend,
                );
                let _ = sender.send(Err(status)).await;
                guard.finish("truncated_cancel");
                return;
            }

            if sender
                .send(Ok(put_result(
                    &task_backend,
                    &task_request_id,
                    "complete",
                    guard.batches,
                    guard.rows,
                    guard.bytes,
                )))
                .await
                .is_err()
            {
                return;
            }
            guard.finish("complete");
        });

        let mut response = Response::new(Box::pin(ReceiverStream::new(receiver)) as Self::DoPutStream);
        response.metadata_mut().insert(
            BACKEND_HEADER,
            MetadataValue::try_from(backend_id.as_str()).map_err(|_| Status::internal("invalid backend id"))?,
        );
        response.metadata_mut().insert(
            REQUEST_ID_HEADER,
            MetadataValue::try_from(request_id.as_str()).map_err(|_| Status::internal("invalid request id"))?,
        );
        Ok(response)
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(&self, _request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(&self, _request: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    validate_routing_key(&args.id).map_err(anyhow::Error::msg)?;
    let events = EventSink::open(&args.events).with_context(|| format!("open event log {}", args.events.display()))?;
    events.emit(format!(
        "kind=server_start\tbackend={}\tlisten={}",
        args.id, args.listen
    ))?;

    let service = ToyFlight {
        backend_id: args.id.clone(),
        events: events.clone(),
    };
    let flight = FlightServiceServer::new(service)
        .max_decoding_message_size(2 * 1024 * 1024)
        .max_encoding_message_size(2 * 1024 * 1024);

    Server::builder()
        .add_service(flight)
        .serve_with_shutdown(args.listen, shutdown_signal(args.id, events))
        .await
        .context("serve Arrow Flight backend")
}

async fn shutdown_signal(backend: String, events: EventSink) {
    #[cfg(unix)]
    {
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        match terminate {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = terminate.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    let _ = events.emit(format!("kind=shutdown_requested\tbackend={backend}"));
}
