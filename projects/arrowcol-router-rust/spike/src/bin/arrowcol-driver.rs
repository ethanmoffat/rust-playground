use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor, PutResult};
use arrowcol_router_spike::{
    ACK_EVERY_HEADER, BACKEND_DELAY_HEADER, BACKEND_HEADER, EXPECTED_BATCHES_HEADER, FAIL_AFTER_HEADER,
    REQUEST_ID_HEADER, ROUTING_HEADER, expected_schema, make_record_batch,
};
use clap::{Parser, ValueEnum};
use futures::{StreamExt, stream};
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Response, Status, Streaming};

const MIB: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Profile {
    Quick,
    Full,
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, value_enum, default_value_t = Profile::Quick)]
    profile: Profile,
    #[arg(long)]
    evidence_dir: Option<PathBuf>,
    #[arg(long, hide = true)]
    cancel_worker: bool,
    #[arg(long, hide = true)]
    worker_proxy: Option<SocketAddr>,
    #[arg(long, hide = true)]
    worker_ready: Option<PathBuf>,
}

#[derive(Clone)]
struct UploadSpec {
    route: String,
    request_id: String,
    batches: usize,
    rows: usize,
    payload_bytes_per_row: usize,
    client_delay_ms: u64,
    backend_delay_ms: u64,
    fail_after: Option<usize>,
    ack_every: usize,
}

impl UploadSpec {
    fn logical_payload_bytes(&self) -> usize {
        self.batches
            .saturating_mul(self.rows)
            .saturating_mul(self.payload_bytes_per_row)
    }
}

struct UploadSuccess {
    backend: String,
    put_results: Vec<String>,
    trailers: MetadataMap,
}

struct ManagedChild {
    name: String,
    child: Child,
    log_path: PathBuf,
}

impl ManagedChild {
    fn id(&self) -> u32 {
        self.child.id()
    }

    fn try_status(&mut self) -> Result<Option<ExitStatus>> {
        self.child
            .try_wait()
            .with_context(|| format!("query {} status", self.name))
    }

    fn terminate(&mut self) -> Result<()> {
        if self.try_status()?.is_some() {
            return Ok(());
        }
        let status = Command::new("/bin/kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .with_context(|| format!("send SIGTERM to {}", self.name))?;
        ensure!(status.success(), "SIGTERM command failed for {}", self.name);
        Ok(())
    }

    async fn wait_for_exit(&mut self, timeout: Duration) -> Result<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.try_status()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                bail!(
                    "{} did not exit within {timeout:?}; log: {}",
                    self.name,
                    self.log_path.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[derive(Clone, Debug)]
struct Event {
    fields: HashMap<String, String>,
}

impl Event {
    fn get(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(String::as_str)
    }
}

#[derive(Debug)]
struct RssObservation {
    baseline_bytes: usize,
    peak_bytes: usize,
    samples: usize,
    logical_payload_bytes: usize,
}

#[derive(Clone, Copy)]
struct Workload {
    concurrent_streams_per_backend: usize,
    concurrent_batches: usize,
    memory_small_batches: usize,
    memory_large_batches: usize,
    drain_batches: usize,
}

impl Workload {
    fn for_profile(profile: Profile) -> Self {
        match profile {
            Profile::Quick => Self {
                concurrent_streams_per_backend: 4,
                concurrent_batches: 12,
                memory_small_batches: 64,
                memory_large_batches: 512,
                drain_batches: 96,
            },
            Profile::Full => Self {
                concurrent_streams_per_backend: 8,
                concurrent_batches: 48,
                memory_small_batches: 128,
                memory_large_batches: 2_048,
                drain_batches: 256,
            },
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(not(target_os = "linux"))]
    bail!("the RSS and signal qualification driver currently requires Linux");

    let args = Args::parse();
    if args.cancel_worker {
        return run_cancel_worker(
            args.worker_proxy.context("--worker-proxy is required")?,
            args.worker_ready.context("--worker-ready is required")?,
        )
        .await;
    }
    let workload = Workload::for_profile(args.profile);
    let evidence_dir = create_evidence_dir(args.evidence_dir)?;
    let alpha_events = evidence_dir.join("backend-alpha.events");
    let beta_events = evidence_dir.join("backend-beta.events");
    let proxy_events = evidence_dir.join("proxy.events");

    let alpha_addr = reserve_address()?;
    let beta_addr = reserve_address()?;
    let proxy_addr = reserve_address()?;
    let offline_addr = reserve_address()?;
    let binaries = sibling_binaries()?;

    let mut alpha = spawn_child(
        "backend-alpha",
        &binaries.backend,
        &[
            "--id".into(),
            "alpha".into(),
            "--listen".into(),
            alpha_addr.to_string(),
            "--events".into(),
            alpha_events.display().to_string(),
        ],
        &evidence_dir,
    )?;
    let mut beta = spawn_child(
        "backend-beta",
        &binaries.backend,
        &[
            "--id".into(),
            "beta".into(),
            "--listen".into(),
            beta_addr.to_string(),
            "--events".into(),
            beta_events.display().to_string(),
        ],
        &evidence_dir,
    )?;
    wait_for_port(&mut alpha, alpha_addr, Duration::from_secs(10)).await?;
    wait_for_port(&mut beta, beta_addr, Duration::from_secs(10)).await?;

    let state_dir = evidence_dir.join("proxy-state");
    let mut proxy = spawn_child(
        "proxy",
        &binaries.proxy,
        &[
            "--listen".into(),
            proxy_addr.to_string(),
            "--alpha".into(),
            alpha_addr.to_string(),
            "--beta".into(),
            beta_addr.to_string(),
            "--offline".into(),
            offline_addr.to_string(),
            "--events".into(),
            proxy_events.display().to_string(),
            "--state-dir".into(),
            state_dir.display().to_string(),
            "--grace-period-seconds".into(),
            "3".into(),
            "--shutdown-timeout-seconds".into(),
            "15".into(),
        ],
        &evidence_dir,
    )?;
    wait_for_port(&mut proxy, proxy_addr, Duration::from_secs(10)).await?;

    let channel = connect(proxy_addr).await?;
    let client = FlightServiceClient::new(channel);
    let mut expected_routes = Vec::new();

    let alpha_basic = base_spec("alpha", "basic-alpha", 24);
    let beta_basic = base_spec("beta", "basic-beta", 24);
    verify_success(
        upload_success(client.clone(), alpha_basic.clone()).await?,
        "alpha",
        alpha_basic.batches,
    )?;
    verify_success(
        upload_success(client.clone(), beta_basic.clone()).await?,
        "beta",
        beta_basic.batches,
    )?;
    expected_routes.push((alpha_basic.request_id.clone(), "alpha"));
    expected_routes.push((beta_basic.request_id.clone(), "beta"));

    let warm_alpha = base_spec("alpha", "warm-alpha", 4);
    let warm_beta = base_spec("beta", "warm-beta", 4);
    verify_success(
        upload_success(client.clone(), warm_alpha.clone()).await?,
        "alpha",
        warm_alpha.batches,
    )?;
    verify_success(
        upload_success(client.clone(), warm_beta.clone()).await?,
        "beta",
        warm_beta.batches,
    )?;
    expected_routes.push((warm_alpha.request_id.clone(), "alpha"));
    expected_routes.push((warm_beta.request_id.clone(), "beta"));

    let mut concurrent = Vec::new();
    for index in 0..workload.concurrent_streams_per_backend {
        for (route, backend) in [("alpha", "alpha"), ("beta", "beta")] {
            let spec = UploadSpec {
                request_id: format!("concurrent-{route}-{index}"),
                route: route.to_string(),
                batches: workload.concurrent_batches,
                ..base_spec(route, "unused", workload.concurrent_batches)
            };
            expected_routes.push((spec.request_id.clone(), backend));
            let task_client = client.clone();
            concurrent.push(tokio::spawn(async move {
                let success = upload_success(task_client, spec.clone()).await?;
                verify_success(success, backend, spec.batches)
            }));
        }
    }
    for task in concurrent {
        task.await.context("concurrent upload task panicked")??;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    verify_upstream_h2_reuse(&proxy_events)?;

    let fail_spec = UploadSpec {
        route: "alpha-fail".into(),
        request_id: "midstream-failure".into(),
        batches: 32,
        fail_after: Some(3),
        ack_every: 1,
        ..base_spec("alpha-fail", "unused", 32)
    };
    let failure = upload_failure(client.clone(), fail_spec.clone()).await?;
    ensure!(
        failure.code() == Code::Aborted,
        "midstream failure code was {:?}",
        failure.code()
    );
    ensure!(
        metadata_string(failure.metadata(), BACKEND_HEADER).as_deref() == Some("alpha"),
        "midstream failure lost backend trailer metadata: {:?}",
        failure.metadata()
    );
    expected_routes.push((fail_spec.request_id.clone(), "alpha"));

    let offline_spec = UploadSpec {
        route: "offline".into(),
        request_id: "backend-unavailable".into(),
        batches: 64,
        ..base_spec("offline", "unused", 64)
    };
    let offline_failure = tokio::time::timeout(
        Duration::from_secs(6),
        upload_failure(client.clone(), offline_spec.clone()),
    )
    .await
    .context("backend-unavailable case timed out")??;
    ensure!(
        offline_failure.code() != Code::Ok,
        "backend-unavailable case unexpectedly succeeded"
    );
    ensure!(
        !event_for_request(&alpha_events, &offline_spec.request_id)?,
        "offline request reached alpha"
    );
    ensure!(
        !event_for_request(&beta_events, &offline_spec.request_id)?,
        "offline request reached beta"
    );

    cancellation_case(
        &binaries.driver,
        proxy_addr,
        &evidence_dir,
        &alpha_events,
        &proxy_events,
    )
    .await?;
    expected_routes.push(("client-cancel".into(), "alpha"));

    let memory_small = UploadSpec {
        route: "memory".into(),
        request_id: "memory-small".into(),
        batches: workload.memory_small_batches,
        backend_delay_ms: 2,
        ..base_spec("memory", "unused", workload.memory_small_batches)
    };
    let small_rss = upload_with_rss(proxy.id(), client.clone(), memory_small.clone(), "alpha").await?;
    expected_routes.push((memory_small.request_id.clone(), "alpha"));

    tokio::time::sleep(Duration::from_millis(200)).await;
    let memory_large = UploadSpec {
        route: "memory".into(),
        request_id: "memory-large".into(),
        batches: workload.memory_large_batches,
        backend_delay_ms: 2,
        ..base_spec("memory", "unused", workload.memory_large_batches)
    };
    let large_rss = upload_with_rss(proxy.id(), client.clone(), memory_large.clone(), "alpha").await?;
    expected_routes.push((memory_large.request_id.clone(), "alpha"));
    verify_memory_bound(args.profile, &small_rss, &large_rss)?;

    verify_backend_isolation(&alpha_events, &beta_events, &expected_routes)?;

    let backend_drain = UploadSpec {
        route: "beta-drain".into(),
        request_id: "backend-graceful-drain".into(),
        batches: workload.drain_batches,
        backend_delay_ms: 5,
        ..base_spec("beta-drain", "unused", workload.drain_batches)
    };
    let drain_client = client.clone();
    let backend_drain_task = tokio::spawn(async move {
        upload_success(drain_client, backend_drain.clone())
            .await
            .and_then(|success| verify_success(success, "beta", backend_drain.batches))
    });
    wait_for_event(
        &beta_events,
        "backend-graceful-drain",
        "start",
        None,
        Duration::from_secs(5),
    )
    .await?;
    beta.terminate()?;
    backend_drain_task.await.context("backend drain task panicked")??;
    let beta_status = beta.wait_for_exit(Duration::from_secs(15)).await?;
    ensure!(beta_status.success(), "beta backend exited with {beta_status}");

    let proxy_drain = UploadSpec {
        route: "alpha-drain".into(),
        request_id: "proxy-graceful-drain".into(),
        batches: workload.drain_batches,
        backend_delay_ms: 5,
        ..base_spec("alpha-drain", "unused", workload.drain_batches)
    };
    let proxy_drain_client = client.clone();
    let proxy_drain_task = tokio::spawn(async move {
        upload_success(proxy_drain_client, proxy_drain.clone())
            .await
            .and_then(|success| verify_success(success, "alpha", proxy_drain.batches))
    });
    wait_for_event(
        &alpha_events,
        "proxy-graceful-drain",
        "start",
        None,
        Duration::from_secs(5),
    )
    .await?;
    proxy.terminate()?;
    proxy_drain_task.await.context("proxy drain task panicked")??;
    let proxy_status = proxy.wait_for_exit(Duration::from_secs(20)).await?;
    ensure!(proxy_status.success(), "proxy exited with {proxy_status}");

    alpha.terminate()?;
    let alpha_status = alpha.wait_for_exit(Duration::from_secs(15)).await?;
    ensure!(alpha_status.success(), "alpha backend exited with {alpha_status}");

    write_summary(&evidence_dir, args.profile, &small_rss, &large_rss, &proxy_events)?;
    println!(
        "PASS profile={:?} evidence={} small_peak_mib={:.1} large_peak_mib={:.1}",
        args.profile,
        evidence_dir.display(),
        small_rss.peak_bytes as f64 / MIB as f64,
        large_rss.peak_bytes as f64 / MIB as f64
    );
    Ok(())
}

fn base_spec(route: &str, request_id: &str, batches: usize) -> UploadSpec {
    UploadSpec {
        route: route.to_string(),
        request_id: request_id.to_string(),
        batches,
        rows: 32,
        payload_bytes_per_row: 8 * 1024,
        client_delay_ms: 0,
        backend_delay_ms: 0,
        fail_after: None,
        ack_every: 8,
    }
}

struct BinaryPaths {
    backend: PathBuf,
    proxy: PathBuf,
    driver: PathBuf,
}

fn sibling_binaries() -> Result<BinaryPaths> {
    let current = std::env::current_exe().context("resolve driver executable")?;
    let directory = current.parent().context("driver executable has no parent directory")?;
    let backend = directory.join("arrowcol-backend");
    let proxy = directory.join("arrowcol-proxy");
    let driver = directory.join("arrowcol-driver");
    ensure!(
        backend.is_file() && proxy.is_file() && driver.is_file(),
        "backend/proxy binaries are missing next to {}; run `cargo build --locked --bins` first",
        current.display()
    );
    Ok(BinaryPaths { backend, proxy, driver })
}

fn create_evidence_dir(requested: Option<PathBuf>) -> Result<PathBuf> {
    let directory = if let Some(path) = requested {
        path
    } else {
        let epoch_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        spike_workspace_root()?
            .join("target")
            .join(format!("spike-evidence-{epoch_ms}-{}", std::process::id()))
    };
    ensure!(
        !directory.exists(),
        "evidence directory already exists: {}",
        directory.display()
    );
    std::fs::create_dir_all(&directory)
        .with_context(|| format!("create evidence directory {}", directory.display()))?;
    Ok(directory)
}

fn spike_workspace_root() -> Result<PathBuf> {
    let current = std::env::current_exe().context("resolve driver executable")?;
    for ancestor in current.ancestors() {
        let manifest = ancestor.join("Cargo.toml");
        if manifest.is_file() {
            let text = std::fs::read_to_string(&manifest).with_context(|| format!("read {}", manifest.display()))?;
            if text.contains("name = \"arrowcol-router-spike\"") {
                return Ok(ancestor.to_path_buf());
            }
        }
    }
    bail!(
        "could not locate the arrowcol-router-spike workspace above {}",
        current.display()
    )
}

fn reserve_address() -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").context("reserve loopback port")?;
    listener.local_addr().context("read reserved address")
}

fn spawn_child(name: &str, binary: &Path, arguments: &[String], evidence_dir: &Path) -> Result<ManagedChild> {
    let log_path = evidence_dir.join(format!("{name}.log"));
    let stdout = File::create(&log_path).with_context(|| format!("create {}", log_path.display()))?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("clone {}", log_path.display()))?;
    let child = Command::new(binary)
        .args(arguments)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| format!("spawn {name} from {}", binary.display()))?;
    Ok(ManagedChild {
        name: name.to_string(),
        child,
        log_path,
    })
}

async fn wait_for_port(child: &mut ManagedChild, address: SocketAddr, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return Ok(());
        }
        if let Some(status) = child.try_status()? {
            bail!(
                "{} exited before readiness with {status}; log: {}",
                child.name,
                child.log_path.display()
            );
        }
        if Instant::now() >= deadline {
            bail!(
                "{} was not ready at {address} within {timeout:?}; log: {}",
                child.name,
                child.log_path.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_file(child: &mut ManagedChild, path: &Path, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if path.is_file() {
            return Ok(());
        }
        if let Some(status) = child.try_status()? {
            bail!(
                "{} exited before creating {} with {status}; log: {}",
                child.name,
                path.display(),
                child.log_path.display()
            );
        }
        if Instant::now() >= deadline {
            bail!(
                "{} did not create {} within {timeout:?}; log: {}",
                child.name,
                path.display(),
                child.log_path.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn connect(address: SocketAddr) -> Result<Channel> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(3))
        .http2_keep_alive_interval(Duration::from_secs(5))
        .keep_alive_while_idle(true);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match endpoint.clone().connect().await {
            Ok(channel) => return Ok(channel),
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(error).context("connect Flight client to proxy"),
        }
    }
}

fn request_stream(spec: UploadSpec) -> impl futures::Stream<Item = FlightData> + Send + 'static {
    let route = spec.route.clone();
    let rows = spec.rows;
    let payload_bytes_per_row = spec.payload_bytes_per_row;
    let delay_ms = spec.client_delay_ms;
    let batches = stream::iter(0..spec.batches).then(move |batch_index| {
        let route = route.clone();
        async move {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            make_record_batch(&route, batch_index, rows, payload_bytes_per_row).map_err(FlightError::from)
        }
    });

    FlightDataEncoderBuilder::new()
        .with_schema(expected_schema())
        .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![spec.route, spec.request_id])))
        .with_max_flight_data_size(512 * 1024)
        .build(batches)
        .map(|item| item.unwrap_or_else(|error| panic!("failed to encode generated RecordBatch: {error}")))
}

fn streaming_request(spec: UploadSpec) -> Result<Request<impl futures::Stream<Item = FlightData> + Send + 'static>> {
    let stream = request_stream(spec.clone());
    let mut request = Request::new(stream);
    insert_metadata(request.metadata_mut(), ROUTING_HEADER, &spec.route)?;
    insert_metadata(request.metadata_mut(), REQUEST_ID_HEADER, &spec.request_id)?;
    insert_metadata(request.metadata_mut(), ACK_EVERY_HEADER, &spec.ack_every.to_string())?;
    insert_metadata(
        request.metadata_mut(),
        EXPECTED_BATCHES_HEADER,
        &spec.batches.to_string(),
    )?;
    if spec.backend_delay_ms > 0 {
        insert_metadata(
            request.metadata_mut(),
            BACKEND_DELAY_HEADER,
            &spec.backend_delay_ms.to_string(),
        )?;
    }
    if let Some(fail_after) = spec.fail_after {
        insert_metadata(request.metadata_mut(), FAIL_AFTER_HEADER, &fail_after.to_string())?;
    }
    Ok(request)
}

fn insert_metadata(metadata: &mut MetadataMap, key: &'static str, value: &str) -> Result<()> {
    metadata.insert(
        key,
        MetadataValue::try_from(value).with_context(|| format!("convert {key} metadata value {value:?}"))?,
    );
    Ok(())
}

async fn invoke_upload(
    mut client: FlightServiceClient<Channel>,
    spec: UploadSpec,
) -> Result<std::result::Result<Response<Streaming<PutResult>>, Status>> {
    let request = streaming_request(spec)?;
    Ok(client.do_put(request).await)
}

async fn upload_success(client: FlightServiceClient<Channel>, spec: UploadSpec) -> Result<UploadSuccess> {
    let response = invoke_upload(client, spec.clone())
        .await?
        .with_context(|| format!("DoPut {} failed before response", spec.request_id))?;
    let backend = metadata_string(response.metadata(), BACKEND_HEADER)
        .with_context(|| format!("{} response missing backend metadata", spec.request_id))?;
    let response_request = metadata_string(response.metadata(), REQUEST_ID_HEADER)
        .with_context(|| format!("{} response missing request id metadata", spec.request_id))?;
    ensure!(
        response_request == spec.request_id,
        "response request id mismatch: {response_request}"
    );

    let mut stream = response.into_inner();
    let mut put_results = Vec::new();
    while let Some(result) = stream
        .message()
        .await
        .with_context(|| format!("read PutResult for {}", spec.request_id))?
    {
        put_results.push(
            String::from_utf8(result.app_metadata.to_vec())
                .with_context(|| format!("decode PutResult for {}", spec.request_id))?,
        );
    }
    let trailers = stream
        .trailers()
        .await
        .with_context(|| format!("read trailers for {}", spec.request_id))?
        .with_context(|| format!("{} response had no gRPC trailers", spec.request_id))?;
    let grpc_status = metadata_string(&trailers, "grpc-status")
        .with_context(|| format!("{} trailers missing grpc-status", spec.request_id))?;
    ensure!(
        grpc_status == "0",
        "{} returned grpc-status {grpc_status}",
        spec.request_id
    );
    ensure!(!put_results.is_empty(), "{} returned no PutResults", spec.request_id);
    Ok(UploadSuccess {
        backend,
        put_results,
        trailers,
    })
}

async fn upload_failure(client: FlightServiceClient<Channel>, spec: UploadSpec) -> Result<Status> {
    match invoke_upload(client, spec.clone()).await? {
        Err(status) => Ok(status),
        Ok(response) => {
            let mut stream = response.into_inner();
            loop {
                match stream.message().await {
                    Ok(Some(_)) => {}
                    Ok(None) => bail!("{} unexpectedly completed successfully", spec.request_id),
                    Err(status) => return Ok(status),
                }
            }
        }
    }
}

fn verify_success(success: UploadSuccess, expected_backend: &str, batches: usize) -> Result<()> {
    ensure!(
        success.backend == expected_backend,
        "response came from {}, expected {expected_backend}",
        success.backend
    );
    let final_result = success.put_results.last().context("missing final PutResult")?;
    ensure!(
        final_result.contains(&format!("backend={expected_backend};")),
        "final PutResult lost backend identity: {final_result}"
    );
    ensure!(
        final_result.contains("outcome=complete"),
        "final PutResult was not complete: {final_result}"
    );
    ensure!(
        final_result.contains(&format!("batches={batches};")),
        "final PutResult batch count mismatch: {final_result}"
    );
    ensure!(
        metadata_string(&success.trailers, "grpc-status").as_deref() == Some("0"),
        "success trailers did not survive"
    );
    Ok(())
}

fn metadata_string(metadata: &MetadataMap, key: &'static str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

async fn cancellation_case(
    driver: &Path,
    proxy: SocketAddr,
    evidence_dir: &Path,
    alpha_events: &Path,
    proxy_events: &Path,
) -> Result<()> {
    let ready = evidence_dir.join("cancel-worker.ready");
    let mut worker = spawn_child(
        "cancel-worker",
        driver,
        &[
            "--cancel-worker".into(),
            "--worker-proxy".into(),
            proxy.to_string(),
            "--worker-ready".into(),
            ready.display().to_string(),
        ],
        evidence_dir,
    )?;
    wait_for_file(&mut worker, &ready, Duration::from_secs(10)).await?;
    worker.terminate()?;
    let status = worker.wait_for_exit(Duration::from_secs(5)).await?;
    ensure!(
        !status.success(),
        "cancellation worker unexpectedly exited successfully"
    );
    wait_for_event(alpha_events, "client-cancel", "finish", None, Duration::from_secs(5)).await?;
    let backend_cancelled = read_events(alpha_events)?.iter().any(|event| {
        event.get("request") == Some("client-cancel")
            && event.get("kind") == Some("finish")
            && matches!(event.get("outcome"), Some("cancelled" | "truncated_cancel"))
    });
    ensure!(
        backend_cancelled,
        "backend did not classify the interrupted stream as cancelled or truncated"
    );
    wait_for_event(proxy_events, "client-cancel", "complete", None, Duration::from_secs(5)).await?;
    let proxy_reset = read_events(proxy_events)?.iter().any(|event| {
        event.get("request") == Some("client-cancel")
            && event.get("kind") == Some("complete")
            && event
                .get("error")
                .is_some_and(|error| error.contains("Downstream ReadError") || error.contains("connection reset"))
    });
    ensure!(proxy_reset, "proxy did not record the downstream cancellation/reset");
    Ok(())
}

async fn run_cancel_worker(proxy: SocketAddr, ready: PathBuf) -> Result<()> {
    let spec = UploadSpec {
        route: "alpha-cancel".into(),
        request_id: "client-cancel".into(),
        batches: 4_096,
        client_delay_ms: 2,
        backend_delay_ms: 2,
        ack_every: 1,
        ..base_spec("alpha-cancel", "unused", 4_096)
    };
    let channel = connect(proxy).await?;
    let mut client = FlightServiceClient::new(channel);
    let response = client
        .do_put(streaming_request(spec)?)
        .await
        .context("start cancellation worker DoPut")?;
    ensure!(
        metadata_string(response.metadata(), BACKEND_HEADER).as_deref() == Some("alpha"),
        "cancellation response missing backend identity"
    );
    let mut response_stream = response.into_inner();
    let first = tokio::time::timeout(Duration::from_secs(5), response_stream.message())
        .await
        .context("waiting for cancellation progress PutResult timed out")??
        .context("cancellation stream ended before progress PutResult")?;
    ensure!(
        String::from_utf8_lossy(&first.app_metadata).contains("backend=alpha;"),
        "cancellation progress PutResult lost backend identity"
    );
    File::create(&ready).with_context(|| format!("create cancellation ready marker {}", ready.display()))?;
    std::future::pending::<()>().await;
    Ok(())
}

async fn upload_with_rss(
    proxy_pid: u32,
    client: FlightServiceClient<Channel>,
    spec: UploadSpec,
    expected_backend: &'static str,
) -> Result<RssObservation> {
    let baseline_bytes = read_rss_bytes(proxy_pid)?;
    let done = Arc::new(AtomicBool::new(false));
    let monitor_done = done.clone();
    let monitor = tokio::spawn(async move {
        let mut peak = baseline_bytes;
        let mut samples = 0usize;
        loop {
            if let Ok(rss) = read_rss_bytes(proxy_pid) {
                peak = peak.max(rss);
                samples += 1;
            }
            if monitor_done.load(Ordering::Acquire) {
                return (peak, samples);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    let logical_payload_bytes = spec.logical_payload_bytes();
    let batches = spec.batches;
    let result = upload_success(client, spec).await;
    done.store(true, Ordering::Release);
    let (peak_bytes, samples) = monitor.await.context("RSS monitor panicked")?;
    verify_success(result?, expected_backend, batches)?;
    ensure!(samples >= 2, "RSS monitor captured only {samples} samples");
    Ok(RssObservation {
        baseline_bytes,
        peak_bytes,
        samples,
        logical_payload_bytes,
    })
}

fn read_rss_bytes(pid: u32) -> Result<usize> {
    let status_path = format!("/proc/{pid}/status");
    let status =
        std::fs::read_to_string(&status_path).with_context(|| format!("read {status_path} for RSS sampling"))?;
    let line = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .context("VmRSS missing from proc status")?;
    let kib = line
        .split_whitespace()
        .nth(1)
        .context("VmRSS value missing")?
        .parse::<usize>()
        .context("parse VmRSS")?;
    Ok(kib.saturating_mul(1024))
}

fn verify_memory_bound(profile: Profile, small: &RssObservation, large: &RssObservation) -> Result<()> {
    ensure!(
        large.logical_payload_bytes >= small.logical_payload_bytes.saturating_mul(8),
        "large memory workload did not grow payload volume by at least 8x"
    );
    let large_growth = large.peak_bytes.saturating_sub(large.baseline_bytes);
    let small_growth = small.peak_bytes.saturating_sub(small.baseline_bytes);
    let absolute_limit = match profile {
        Profile::Quick => 96 * MIB,
        Profile::Full => 128 * MIB,
    };
    ensure!(
        large_growth <= absolute_limit,
        "proxy RSS grew {:.1} MiB for a {:.1} MiB logical stream (limit {:.1} MiB)",
        large_growth as f64 / MIB as f64,
        large.logical_payload_bytes as f64 / MIB as f64,
        absolute_limit as f64 / MIB as f64
    );
    ensure!(
        large_growth <= small_growth.saturating_add(64 * MIB),
        "proxy RSS growth scaled unexpectedly: small {:.1} MiB, large {:.1} MiB",
        small_growth as f64 / MIB as f64,
        large_growth as f64 / MIB as f64
    );
    Ok(())
}

fn read_events(path: &Path) -> Result<Vec<Event>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("read event log {}", path.display()))?;
    Ok(text
        .lines()
        .map(|line| {
            let fields = line
                .split('\t')
                .filter_map(|part| part.split_once('='))
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect();
            Event { fields }
        })
        .collect())
}

fn event_for_request(path: &Path, request_id: &str) -> Result<bool> {
    Ok(read_events(path)?
        .iter()
        .any(|event| event.get("request") == Some(request_id)))
}

async fn wait_for_event(
    path: &Path,
    request_id: &str,
    kind: &str,
    extra: Option<(&str, &str)>,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let found = read_events(path)?.iter().any(|event| {
            event.get("request") == Some(request_id)
                && event.get("kind") == Some(kind)
                && extra.map(|(key, value)| event.get(key) == Some(value)).unwrap_or(true)
        });
        if found {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "event kind={kind} request={request_id} extra={extra:?} not found in {}",
                path.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn verify_backend_isolation(alpha_path: &Path, beta_path: &Path, expected: &[(String, &str)]) -> Result<()> {
    let alpha = read_events(alpha_path)?;
    let beta = read_events(beta_path)?;
    for (request_id, backend) in expected {
        let alpha_started = alpha
            .iter()
            .any(|event| event.get("kind") == Some("start") && event.get("request") == Some(request_id));
        let beta_started = beta
            .iter()
            .any(|event| event.get("kind") == Some("start") && event.get("request") == Some(request_id));
        match *backend {
            "alpha" => ensure!(
                alpha_started && !beta_started,
                "{request_id} isolation failed: alpha={alpha_started} beta={beta_started}"
            ),
            "beta" => ensure!(
                beta_started && !alpha_started,
                "{request_id} isolation failed: alpha={alpha_started} beta={beta_started}"
            ),
            other => bail!("unexpected backend expectation {other}"),
        }
    }
    Ok(())
}

fn verify_upstream_h2_reuse(proxy_events: &Path) -> Result<()> {
    let events = read_events(proxy_events)?;
    for backend in ["alpha", "beta"] {
        let upstream: Vec<&Event> = events
            .iter()
            .filter(|event| event.get("kind") == Some("upstream") && event.get("backend") == Some(backend))
            .collect();
        let reused = upstream
            .iter()
            .filter(|event| event.get("reused") == Some("true"))
            .count();
        ensure!(reused >= 2, "{backend} had only {reused} reused upstream H2 stream(s)");
        let mut fd_counts: HashMap<&str, usize> = HashMap::new();
        for event in upstream {
            if let Some(fd) = event.get("fd") {
                *fd_counts.entry(fd).or_default() += 1;
            }
        }
        ensure!(
            fd_counts.values().any(|count| *count >= 3),
            "{backend} did not multiplex at least three streams on one upstream fd: {fd_counts:?}"
        );
    }
    Ok(())
}

fn write_summary(
    evidence_dir: &Path,
    profile: Profile,
    small: &RssObservation,
    large: &RssObservation,
    proxy_events: &Path,
) -> Result<()> {
    let mut summary = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(evidence_dir.join("summary.txt"))
        .context("create evidence summary")?;
    writeln!(summary, "profile={profile:?}")?;
    writeln!(
        summary,
        "small logical_mib={:.1} baseline_mib={:.1} peak_mib={:.1} samples={}",
        small.logical_payload_bytes as f64 / MIB as f64,
        small.baseline_bytes as f64 / MIB as f64,
        small.peak_bytes as f64 / MIB as f64,
        small.samples
    )?;
    writeln!(
        summary,
        "large logical_mib={:.1} baseline_mib={:.1} peak_mib={:.1} samples={}",
        large.logical_payload_bytes as f64 / MIB as f64,
        large.baseline_bytes as f64 / MIB as f64,
        large.peak_bytes as f64 / MIB as f64,
        large.samples
    )?;
    writeln!(summary, "proxy_events={}", proxy_events.display())?;
    writeln!(
        summary,
        "result=PASS routing trailers put_results h2_reuse cancellation_truncation_detection failure unavailability memory drain"
    )?;
    Ok(())
}
