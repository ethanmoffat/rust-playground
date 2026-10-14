use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use arrowcol_router_spike::{
    DO_PUT_PATH, EventSink, MAX_REQUEST_ID_BYTES, MAX_ROUTING_KEY_BYTES, REQUEST_ID_HEADER, ROUTING_HEADER,
    validate_request_id, validate_routing_key,
};
use async_trait::async_trait;
use clap::Parser;
use pingora::apps::HttpServerOptions;
use pingora::prelude::*;
use pingora::protocols::Digest;
use pingora::protocols::http::v2::server::default_h2_options;
use pingora::server::configuration::ServerConf;
use pingora::upstreams::peer::ALPN;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    listen: SocketAddr,
    #[arg(long)]
    alpha: SocketAddr,
    #[arg(long)]
    beta: SocketAddr,
    #[arg(long)]
    offline: SocketAddr,
    #[arg(long)]
    events: PathBuf,
    #[arg(long)]
    state_dir: PathBuf,
    #[arg(long, default_value_t = 0)]
    grace_period_seconds: u64,
    #[arg(long, default_value_t = 10)]
    shutdown_timeout_seconds: u64,
}

#[derive(Clone)]
struct Router {
    alpha: SocketAddr,
    beta: SocketAddr,
    offline: SocketAddr,
    events: EventSink,
}

#[derive(Default)]
struct RequestContext {
    route: Option<String>,
    backend: Option<&'static str>,
    request_id: Option<String>,
    request_bytes: usize,
    request_chunks: usize,
}

impl Router {
    fn select(&self, route: &str) -> Option<(&'static str, SocketAddr)> {
        match route {
            "alpha" | "alpha-fail" | "alpha-cancel" | "alpha-drain" | "memory" => Some(("alpha", self.alpha)),
            "beta" | "beta-drain" => Some(("beta", self.beta)),
            "offline" => Some(("offline", self.offline)),
            _ => None,
        }
    }
}

#[async_trait]
impl ProxyHttp for Router {
    type CTX = RequestContext;

    fn new_ctx(&self) -> Self::CTX {
        RequestContext::default()
    }

    async fn upstream_peer(&self, session: &mut Session, context: &mut Self::CTX) -> pingora::Result<Box<HttpPeer>> {
        let request = session.req_header();
        if request.method.as_str() != "POST" || request.uri.path() != DO_PUT_PATH {
            return Err(Error::explain(
                ErrorType::HTTPStatus(404),
                "only Arrow Flight DoPut is routed",
            ));
        }

        let route = request
            .headers
            .get(ROUTING_HEADER)
            .ok_or_else(|| Error::explain(ErrorType::HTTPStatus(400), "missing x-arrowcol-table routing metadata"))?
            .to_str()
            .map_err(|_| Error::explain(ErrorType::HTTPStatus(400), "x-arrowcol-table is not visible ASCII"))?;
        validate_routing_key(route)
            .map_err(|message| Error::explain(ErrorType::HTTPStatus(400), format!("invalid route: {message}")))?;

        let request_id = request
            .headers
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("missing");
        validate_request_id(request_id)
            .map_err(|message| Error::explain(ErrorType::HTTPStatus(400), format!("invalid request id: {message}")))?;

        let (backend, address) = self.select(route).ok_or_else(|| {
            Error::explain(
                ErrorType::HTTPStatus(404),
                format!("no backend for routing key {route}"),
            )
        })?;
        context.route = Some(route.to_owned());
        context.backend = Some(backend);
        context.request_id = Some(request_id.to_owned());

        let mut peer = HttpPeer::new(address, false, String::new());
        peer.options.alpn = ALPN::H2;
        peer.options.max_h2_streams = 64;
        peer.options.h2_stream_window_size = Some(1024 * 1024);
        peer.options.h2_connection_window_size = Some(8 * 1024 * 1024);
        peer.options.connection_timeout = Some(Duration::from_secs(1));
        peer.options.total_connection_timeout = Some(Duration::from_secs(2));
        peer.options.idle_timeout = Some(Duration::from_secs(30));
        Ok(Box::new(peer))
    }

    async fn request_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        _end_of_stream: bool,
        context: &mut Self::CTX,
    ) -> pingora::Result<()> {
        if let Some(bytes) = body {
            context.request_chunks = context.request_chunks.saturating_add(1);
            context.request_bytes = context.request_bytes.saturating_add(bytes.len());
        }
        Ok(())
    }

    async fn connected_to_upstream(
        &self,
        _session: &mut Session,
        reused: bool,
        _peer: &HttpPeer,
        fd: RawFd,
        _digest: Option<&Digest>,
        context: &mut Self::CTX,
    ) -> pingora::Result<()> {
        let route = context.route.as_deref().unwrap_or("unknown");
        let backend = context.backend.unwrap_or("unknown");
        let request = context.request_id.as_deref().unwrap_or("missing");
        let _ = self.events.emit(format!(
            "kind=upstream\trequest={request}\troute={route}\tbackend={backend}\treused={reused}\tfd={fd}"
        ));
        Ok(())
    }

    async fn logging(&self, _session: &mut Session, error: Option<&Error>, context: &mut Self::CTX) {
        let route = context.route.as_deref().unwrap_or("unknown");
        let backend = context.backend.unwrap_or("unknown");
        let request = context.request_id.as_deref().unwrap_or("missing");
        let error = error
            .map(ToString::to_string)
            .unwrap_or_else(|| "none".to_string())
            .replace(['\t', '\n'], " ");
        let _ = self.events.emit(format!(
            "kind=complete\trequest={request}\troute={route}\tbackend={backend}\tchunks={}\tbytes={}\terror={error}",
            context.request_chunks, context.request_bytes
        ));
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.state_dir)
        .with_context(|| format!("create state directory {}", args.state_dir.display()))?;
    let events = EventSink::open(&args.events).with_context(|| format!("open event log {}", args.events.display()))?;

    let configuration = ServerConf {
        threads: 2,
        listener_tasks_per_fd: 1,
        grace_period_seconds: Some(args.grace_period_seconds),
        graceful_shutdown_timeout_seconds: Some(args.shutdown_timeout_seconds),
        pid_file: args.state_dir.join("proxy.pid").display().to_string(),
        upgrade_sock: args.state_dir.join("proxy-upgrade.sock").display().to_string(),
        ..ServerConf::default()
    };

    let mut server = Server::new_with_opt_and_conf(None, configuration);
    server.bootstrap();

    let router = Router {
        alpha: args.alpha,
        beta: args.beta,
        offline: args.offline,
        events: events.clone(),
    };
    let mut service = http_proxy_service(&server.configuration, router);
    let proxy = service.app_logic_mut().context("proxy service missing app logic")?;
    let mut server_options = HttpServerOptions::default();
    server_options.h2c = true;
    server_options.h2_idle_timeout = Some(Duration::from_secs(30));
    proxy.server_options = Some(server_options);
    let mut h2_options = default_h2_options();
    h2_options.max_concurrent_streams(128);
    proxy.h2_options = Some(h2_options);
    service.add_tcp(&args.listen.to_string());
    server.add_service(service);

    events.emit(format!(
        "kind=proxy_start\tlisten={}\talpha={}\tbeta={}\toffline={}\tmax_route_bytes={MAX_ROUTING_KEY_BYTES}\tmax_request_id_bytes={MAX_REQUEST_ID_BYTES}\tupstream_h2_streams=64",
        args.listen, args.alpha, args.beta, args.offline
    ))?;
    server.run_forever()
}
