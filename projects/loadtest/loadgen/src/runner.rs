use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use arrow::{array::RecordBatch, datatypes::Schema};
use arrowcol_protocol::{ImplementationPool, ReceiptId, UnixMillis};
use arrowcol_routing::{RoutingStore, SqlServerRoutingConfig, SqlServerRoutingStore};
use serde::Serialize;
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    cli::{RunArgs, Scenario},
    generator::{LogicalBatch, Scheduler, build_logical_batch, encode_record_batch},
    manifest::{
        AckOutcome, CAPABILITY_EVIDENCE_FILE, ManifestWriter, PROBE_EVIDENCE_FILE, RECEIPT_MANIFEST_FILE,
        ROUTING_EVIDENCE_FILE, RUN_METADATA_FILE, ReceiptManifestEntry, SendManifestEntry, run_metadata,
    },
    schema::{TABLE_COUNT, schema_contract_sha256},
    sdk::prepare_tables,
    transport::{
        FlightAcknowledgement, FlightTransport, ReceiptFacts, now_unix_millis, receipt_poll_map, status_fields,
    },
};

/// Executes a production finite or continuous benchmark run.
///
/// # Errors
///
/// Returns invalid configuration, dependency, delivery, receipt, or evidence failures.
pub async fn run(arguments: RunArgs) -> Result<()> {
    validate_arguments(&arguments)?;
    let run_id = arguments
        .run_id
        .clone()
        .unwrap_or_else(|| format!("run-{}-{}", now_unix_millis(), Uuid::new_v4().simple()));
    let manifests = ManifestWriter::create(&arguments.results).await?;
    let schema_digest = schema_contract_sha256()?;
    manifests
        .write_json(RUN_METADATA_FILE, &run_metadata(&run_id, &arguments, &schema_digest))
        .await?;

    let transport = FlightTransport::connect(
        &arguments.router_endpoint,
        (!arguments.reuse_generated).then_some(arguments.catalog_endpoint.as_str()),
        &arguments.receipt_endpoint,
        &arguments.edge_secret_file,
        &arguments.internal_secret_file,
        &arguments.direct_admin_endpoints,
        arguments.request_timeout.get(),
    )
    .await?;
    let tables = if arguments.reuse_generated {
        crate::sdk::load_prepared_tables(&arguments, &run_id, &manifests).await?
    } else {
        prepare_tables(&arguments, &run_id, &transport, &manifests).await?
    };
    if tables.len() != TABLE_COUNT {
        bail!("prepared {} schemas instead of {TABLE_COUNT}", tables.len());
    }
    let requested_tables = arguments.tables.iter().cloned().collect::<BTreeSet<_>>();
    let load_tables = tables
        .into_iter()
        .filter(|table| requested_tables.is_empty() || requested_tables.contains(&table.identity.to_string()))
        .collect::<Vec<_>>();
    if load_tables.is_empty() || (!requested_tables.is_empty() && load_tables.len() != requested_tables.len()) {
        bail!("requested load table filter contains an unknown table");
    }

    if !arguments.skip_capabilities {
        let capabilities = serde_json::json!({
            "direct": transport.capability_evidence(&arguments.direct_admin_endpoints).await?,
            "dataflow": transport.capability_evidence(&arguments.dataflow_admin_endpoints).await?,
        });
        manifests.write_json(CAPABILITY_EVIDENCE_FILE, &capabilities).await?;
    }

    if !arguments.capture_settle.get().is_zero() {
        tokio::time::sleep(arguments.capture_settle.get()).await;
    }

    let mut batches = BTreeMap::new();
    for table in &load_tables {
        let batch = build_logical_batch(Arc::clone(table), arguments.batch_rows, arguments.seed).await?;
        batches.insert(table.identity.to_string(), Arc::new(batch));
    }
    if !arguments.skip_probes {
        let probe = run_probes(
            &transport,
            batches
                .values()
                .next()
                .cloned()
                .context("generated probe batch is missing")?,
            arguments.lanes,
            arguments.request_timeout.get(),
            arguments.drain_timeout.get(),
        )
        .await?;
        manifests.write_json(PROBE_EVIDENCE_FILE, &probe).await?;
    }
    let scheduled_table_count = load_tables.len();
    let scheduler = Arc::new(Scheduler::new(load_tables, arguments.seed)?);
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancellation.cancel();
        }
    });

    let (receipt_sender, receipt_task) = ReceiptTracker::start(
        transport.clone(),
        manifests.clone(),
        run_id.clone(),
        arguments.drain_timeout.get(),
        arguments.lanes.saturating_mul(16).max(32),
    );

    let mut run_error = None;
    for scenario in arguments.effective_scenarios() {
        if cancellation.is_cancelled() {
            break;
        }
        if !arguments.warmup.get().is_zero()
            && let Err(error) = run_phase(
                &run_id,
                scenario,
                "warmup",
                Some(arguments.warmup.get()),
                &arguments,
                Arc::clone(&scheduler),
                &batches,
                transport.clone(),
                manifests.clone(),
                receipt_sender.clone(),
                cancellation.clone(),
                scheduled_table_count,
            )
            .await
        {
            run_error = Some(error);
            cancellation.cancel();
            break;
        }
        let duration = if arguments.continuous {
            None
        } else {
            Some(arguments.measure.get())
        };
        if let Err(error) = run_phase(
            &run_id,
            scenario,
            "measure",
            duration,
            &arguments,
            Arc::clone(&scheduler),
            &batches,
            transport.clone(),
            manifests.clone(),
            receipt_sender.clone(),
            cancellation.clone(),
            scheduled_table_count,
        )
        .await
        {
            run_error = Some(error);
            cancellation.cancel();
            break;
        }
    }
    drop(receipt_sender);
    let receipt_report = receipt_task.await.context("receipt tracker task failed")??;
    let routing = capture_routing(&arguments).await?;
    manifests.write_json(ROUTING_EVIDENCE_FILE, &routing).await?;
    manifests.flush().await?;

    if let Some(error) = run_error {
        return Err(error);
    }
    if receipt_report.failures > 0 || receipt_report.timeouts > 0 {
        bail!(
            "direct receipt drain completed with {} explicit failures and {} timeouts; see {RECEIPT_MANIFEST_FILE}",
            receipt_report.failures,
            receipt_report.timeouts
        );
    }
    println!(
        "PASS run={} logical_batches={} receipts_published={}",
        run_id,
        scheduler.scheduled(),
        receipt_report.published
    );
    Ok(())
}

async fn run_probes(
    transport: &FlightTransport,
    batch: Arc<LogicalBatch>,
    lanes: usize,
    request_timeout: Duration,
    drain_timeout: Duration,
) -> Result<ProbeEvidence> {
    let mut fields = batch
        .record_batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let first = fields.first_mut().context("probe schema has no fields")?;
    *first = first.clone().with_nullable(true);
    let malformed = RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.record_batch.columns().to_vec())?;
    let (malformed, _) = encode_record_batch(&batch.table, &malformed).await?;
    let malformed = Arc::new(malformed);
    let mut rejection = None;
    for attempt in 1..=16_u32 {
        match transport
            .send_batch(
                &batch.table,
                ImplementationPool::Direct,
                Arc::clone(&malformed),
                request_timeout,
            )
            .await
        {
            Ok(_) => bail!("breaking schema probe unexpectedly created an acknowledgement"),
            Err(error)
                if error.chain().any(|cause| {
                    cause
                        .downcast_ref::<tonic::Status>()
                        .is_some_and(|status| status.code() == tonic::Code::InvalidArgument)
                }) =>
            {
                let (code, message) = status_fields(&error);
                rejection = Some(format!("{code}: {message}"));
                break;
            }
            Err(error) if attempt == 16 => {
                return Err(error).context("breaking schema probe never reached schema validation");
            }
            Err(_) => tokio::time::sleep(probe_retry_delay(attempt)).await,
        }
    }
    let rejection = rejection.context("breaking schema probe produced no result")?;

    let first = send_probe_with_retry(transport, &batch, ImplementationPool::Direct, request_timeout).await?;
    let second = send_probe_with_retry(transport, &batch, ImplementationPool::Direct, request_timeout).await?;
    let (FlightAcknowledgement::DurableReceipt(first), FlightAcknowledgement::DurableReceipt(second)) = (first, second)
    else {
        bail!("duplicate direct probe did not return durable receipts");
    };
    if first == second {
        bail!("byte-identical direct deliveries reused a receipt identifier");
    }
    wait_for_probe_receipts(transport, &[first, second], drain_timeout).await?;

    let limit = lanes.clamp(1, 8);
    let semaphore = Arc::new(Semaphore::new(limit));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut sends = JoinSet::new();
    for _ in 0..limit.saturating_mul(2) {
        let transport = transport.clone();
        let batch = Arc::clone(&batch);
        let semaphore = Arc::clone(&semaphore);
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        sends.spawn(async move {
            let permit = semaphore.acquire_owned().await.context("probe semaphore closed")?;
            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(current, Ordering::SeqCst);
            let result = send_probe_with_retry(&transport, &batch, ImplementationPool::Dataflow, request_timeout).await;
            active.fetch_sub(1, Ordering::SeqCst);
            drop(permit);
            match result? {
                FlightAcknowledgement::BestEffortAdmission => Ok::<(), anyhow::Error>(()),
                FlightAcknowledgement::DurableReceipt(_) => {
                    bail!("dataflow backpressure probe returned a durable receipt")
                }
            }
        });
    }
    while let Some(result) = sends.join_next().await {
        result.context("bounded backpressure probe task failed")??;
    }
    let observed = peak.load(Ordering::SeqCst);
    if observed > limit {
        bail!("bounded backpressure probe exceeded its in-flight limit");
    }
    Ok(ProbeEvidence {
        breaking_schema_rejected_before_receipt: true,
        breaking_schema_error: rejection,
        duplicate_receipts: [first.to_string(), second.to_string()],
        duplicate_receipts_published: true,
        bounded_inflight_limit: limit,
        bounded_inflight_observed: observed,
        dataflow_probe_admissions: limit.saturating_mul(2),
    })
}

async fn send_probe_with_retry(
    transport: &FlightTransport,
    batch: &LogicalBatch,
    pool: ImplementationPool,
    timeout: Duration,
) -> Result<FlightAcknowledgement> {
    let mut last_error = None;
    for attempt in 1..=16_u32 {
        match transport
            .send_batch(&batch.table, pool, Arc::clone(&batch.encoded), timeout)
            .await
        {
            Ok(acknowledgement) => return Ok(acknowledgement),
            Err(error) => {
                last_error = Some(error);
                if attempt < 16 {
                    tokio::time::sleep(probe_retry_delay(attempt)).await;
                }
            }
        }
    }
    Err(last_error.context("probe delivery retries were exhausted")?)
}

fn probe_retry_delay(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(4);
    Duration::from_millis(100_u64.saturating_mul(1_u64 << exponent).min(1_000))
}

async fn wait_for_probe_receipts(transport: &FlightTransport, receipts: &[ReceiptId], timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let responses = receipt_poll_map(receipts, transport.poll_receipts(receipts).await?)?;
        let mut published = 0_usize;
        for receipt in receipts {
            let response = responses.get(receipt).context("probe receipt response is missing")?;
            if let Some(status) = &response.status {
                let facts = ReceiptFacts::from_status(status);
                if facts.state == "published" && response.purge_allowed {
                    published += 1;
                } else if facts.terminal {
                    bail!("duplicate probe receipt {receipt} ended as {}", facts.state);
                }
            }
        }
        if published == receipts.len() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("duplicate receipt probe exceeded its bounded publication timeout");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn validate_arguments(arguments: &RunArgs) -> Result<()> {
    if arguments.lanes == 0 || arguments.batch_rows == 0 || arguments.max_attempts == 0 {
        bail!("lanes, batch rows, and max attempts must be positive");
    }
    if arguments.seed > i64::MAX as u64 {
        bail!("seed must not exceed {}", i64::MAX);
    }
    if arguments.measure.get().is_zero()
        || arguments.request_timeout.get().is_zero()
        || arguments.drain_timeout.get().is_zero()
    {
        bail!("measure, request timeout, and drain timeout must be positive");
    }
    let scenarios = arguments.effective_scenarios();
    if scenarios.is_empty() {
        bail!("at least one benchmark scenario is required");
    }
    if !arguments.continuous && scenarios.iter().collect::<BTreeSet<_>>().len() != scenarios.len() {
        bail!("benchmark scenarios must not contain duplicates");
    }
    Ok(())
}

#[derive(Serialize)]
struct ProbeEvidence {
    breaking_schema_rejected_before_receipt: bool,
    breaking_schema_error: String,
    duplicate_receipts: [String; 2],
    duplicate_receipts_published: bool,
    bounded_inflight_limit: usize,
    bounded_inflight_observed: usize,
    dataflow_probe_admissions: usize,
}

#[allow(clippy::too_many_arguments)]
async fn run_phase(
    run_id: &str,
    scenario: Scenario,
    phase: &str,
    duration: Option<Duration>,
    arguments: &RunArgs,
    scheduler: Arc<Scheduler>,
    batches: &BTreeMap<String, Arc<LogicalBatch>>,
    transport: FlightTransport,
    manifests: ManifestWriter,
    receipt_sender: mpsc::Sender<PendingReceipt>,
    cancellation: CancellationToken,
    table_count: usize,
) -> Result<()> {
    let deadline = duration.map(|duration| Instant::now() + duration);
    let first_ordinal = scheduler.scheduled();
    let mut lanes = JoinSet::new();
    let batches = Arc::new(batches.clone());
    for lane in 0..arguments.lanes {
        let scheduler = Arc::clone(&scheduler);
        let batches = Arc::clone(&batches);
        let transport = transport.clone();
        let manifests = manifests.clone();
        let receipt_sender = receipt_sender.clone();
        let cancellation = cancellation.clone();
        let run_id = run_id.to_owned();
        let phase = phase.to_owned();
        let request_timeout = arguments.request_timeout.get();
        let max_attempts = arguments.max_attempts;
        lanes.spawn(async move {
            loop {
                if cancellation.is_cancelled() {
                    break;
                }
                let enough_tables = scheduler.scheduled().saturating_sub(first_ordinal) >= table_count as u64;
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) && enough_tables {
                    break;
                }
                let scheduled = scheduler.next();
                let batch = batches
                    .get(&scheduled.table.identity.to_string())
                    .context("scheduled table has no generated batch")?;
                let logical_batch_id = format!("{}:{}:{}:{}", run_id, scenario.as_str(), phase, scheduled.ordinal);
                match scenario {
                    Scenario::Direct => {
                        send_pool_with_retry(
                            &run_id,
                            scenario,
                            &phase,
                            &logical_batch_id,
                            scheduled.ordinal,
                            scheduled.table_batch_ordinal,
                            lane,
                            ImplementationPool::Direct,
                            Arc::clone(batch),
                            &transport,
                            &manifests,
                            &receipt_sender,
                            request_timeout,
                            max_attempts,
                        )
                        .await?;
                    }
                    Scenario::Dataflow => {
                        send_pool_with_retry(
                            &run_id,
                            scenario,
                            &phase,
                            &logical_batch_id,
                            scheduled.ordinal,
                            scheduled.table_batch_ordinal,
                            lane,
                            ImplementationPool::Dataflow,
                            Arc::clone(batch),
                            &transport,
                            &manifests,
                            &receipt_sender,
                            request_timeout,
                            max_attempts,
                        )
                        .await?;
                    }
                    Scenario::Paired => {
                        let direct = send_pool_with_retry(
                            &run_id,
                            scenario,
                            &phase,
                            &logical_batch_id,
                            scheduled.ordinal,
                            scheduled.table_batch_ordinal,
                            lane,
                            ImplementationPool::Direct,
                            Arc::clone(batch),
                            &transport,
                            &manifests,
                            &receipt_sender,
                            request_timeout,
                            max_attempts,
                        );
                        let dataflow = send_pool_with_retry(
                            &run_id,
                            scenario,
                            &phase,
                            &logical_batch_id,
                            scheduled.ordinal,
                            scheduled.table_batch_ordinal,
                            lane,
                            ImplementationPool::Dataflow,
                            Arc::clone(batch),
                            &transport,
                            &manifests,
                            &receipt_sender,
                            request_timeout,
                            max_attempts,
                        );
                        let (direct, dataflow) = tokio::join!(direct, dataflow);
                        direct?;
                        dataflow?;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });
    }
    drop(receipt_sender);
    while let Some(result) = lanes.join_next().await {
        result.context("load lane task failed")??;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn send_pool_with_retry(
    run_id: &str,
    scenario: Scenario,
    phase: &str,
    logical_batch_id: &str,
    ordinal: u64,
    table_batch_ordinal: u64,
    lane: usize,
    pool: ImplementationPool,
    batch: Arc<LogicalBatch>,
    transport: &FlightTransport,
    manifests: &ManifestWriter,
    receipt_sender: &mpsc::Sender<PendingReceipt>,
    request_timeout: Duration,
    max_attempts: u32,
) -> Result<()> {
    let mut last_error = None;
    for attempt in 1..=max_attempts {
        let sent_at = now_unix_millis();
        match transport
            .send_batch(&batch.table, pool, Arc::clone(&batch.encoded), request_timeout)
            .await
        {
            Ok(acknowledgement) => {
                let acknowledged_at = now_unix_millis();
                let (outcome, receipt_id) = match acknowledgement {
                    FlightAcknowledgement::DurableReceipt(receipt) => {
                        if pool != ImplementationPool::Direct {
                            bail!("dataflow pool returned a durable receipt");
                        }
                        (AckOutcome::DurableReceipt, Some(receipt))
                    }
                    FlightAcknowledgement::BestEffortAdmission => {
                        if pool != ImplementationPool::Dataflow {
                            bail!("direct pool returned best-effort admission");
                        }
                        (AckOutcome::BestEffortAdmission, None)
                    }
                };
                manifests
                    .write_send(&SendManifestEntry {
                        run_id: run_id.to_owned(),
                        scenario,
                        phase: phase.to_owned(),
                        logical_batch_id: logical_batch_id.to_owned(),
                        ordinal,
                        table_batch_ordinal,
                        table: batch.table.identity.to_string(),
                        schema_version: batch.table.schema_version,
                        schema_fingerprint: batch.table.fingerprint.clone(),
                        rows: batch.rows,
                        arrow_bytes: batch.arrow_bytes,
                        payload_sha256: batch.payload_sha256.clone(),
                        row_sha256: batch.row_sha256.clone(),
                        lane,
                        pool: pool.to_string(),
                        attempt,
                        sent_at_unix_ms: sent_at,
                        acknowledged_at_unix_ms: acknowledged_at,
                        outcome,
                        receipt_id,
                        error_code: None,
                        error: None,
                    })
                    .await?;
                if let Some(receipt_id) = receipt_id {
                    receipt_sender
                        .send(PendingReceipt {
                            logical_batch_id: logical_batch_id.to_owned(),
                            table: batch.table.identity.to_string(),
                            receipt_id,
                        })
                        .await
                        .context("receipt tracker stopped before admission completed")?;
                }
                return Ok(());
            }
            Err(error) => {
                let (error_code, message) = status_fields(&error);
                manifests
                    .write_send(&SendManifestEntry {
                        run_id: run_id.to_owned(),
                        scenario,
                        phase: phase.to_owned(),
                        logical_batch_id: logical_batch_id.to_owned(),
                        ordinal,
                        table_batch_ordinal,
                        table: batch.table.identity.to_string(),
                        schema_version: batch.table.schema_version,
                        schema_fingerprint: batch.table.fingerprint.clone(),
                        rows: batch.rows,
                        arrow_bytes: batch.arrow_bytes,
                        payload_sha256: batch.payload_sha256.clone(),
                        row_sha256: batch.row_sha256.clone(),
                        lane,
                        pool: pool.to_string(),
                        attempt,
                        sent_at_unix_ms: sent_at,
                        acknowledged_at_unix_ms: now_unix_millis(),
                        outcome: AckOutcome::Error,
                        receipt_id: None,
                        error_code: Some(error_code),
                        error: Some(message),
                    })
                    .await?;
                last_error = Some(error);
                if attempt < max_attempts {
                    let exponent = attempt.saturating_sub(1).min(5);
                    let delay = 100_u64.saturating_mul(1_u64 << exponent).min(2_000);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
        }
    }
    Err(last_error.context("delivery attempts exhausted")?)
}

#[derive(Clone)]
struct PendingReceipt {
    logical_batch_id: String,
    table: String,
    receipt_id: ReceiptId,
}

#[derive(Default)]
struct ReceiptReport {
    published: usize,
    failures: usize,
    timeouts: usize,
}

struct ReceiptTracker;

impl ReceiptTracker {
    fn start(
        transport: FlightTransport,
        manifests: ManifestWriter,
        run_id: String,
        drain_timeout: Duration,
        capacity: usize,
    ) -> (
        mpsc::Sender<PendingReceipt>,
        tokio::task::JoinHandle<Result<ReceiptReport>>,
    ) {
        let (sender, mut receiver) = mpsc::channel::<PendingReceipt>(capacity);
        let task = tokio::spawn(async move {
            let mut pending = BTreeMap::<ReceiptId, PendingReceipt>::new();
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut channel_closed = false;
            let mut deadline = None;
            let mut report = ReceiptReport::default();
            loop {
                if channel_closed && pending.is_empty() {
                    break;
                }
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    for pending in pending.into_values() {
                        manifests
                            .write_receipt(&ReceiptManifestEntry {
                                run_id: run_id.clone(),
                                logical_batch_id: pending.logical_batch_id,
                                table: pending.table,
                                receipt_id: pending.receipt_id,
                                state: "timeout".to_owned(),
                                purge_allowed: false,
                                terminal_at_unix_ms: now_unix_millis(),
                                delta_version: None,
                                rows_published: None,
                                bytes_published: None,
                                error_code: Some("receipt_poll_timeout".to_owned()),
                            })
                            .await?;
                        report.timeouts += 1;
                    }
                    break;
                }
                tokio::select! {
                    received = receiver.recv(), if !channel_closed => {
                        match received {
                            Some(receipt) => {
                                if pending.insert(receipt.receipt_id, receipt).is_some() {
                                    bail!("duplicate server receipt identifier entered the poller");
                                }
                            }
                            None => {
                                channel_closed = true;
                                deadline = Some(Instant::now() + drain_timeout);
                            }
                        }
                    }
                    _ = interval.tick() => {
                        if pending.is_empty() {
                            continue;
                        }
                        let ids = pending.keys().copied().collect::<Vec<_>>();
                        for chunk in ids.chunks(256) {
                            let responses = match transport.poll_receipts(chunk).await {
                                Ok(responses) => responses,
                                Err(_) => continue,
                            };
                            let polled = receipt_poll_map(chunk, responses)?;
                            for receipt_id in chunk {
                                let Some(response) = polled.get(receipt_id) else {
                                    continue;
                                };
                                if !response.found {
                                    continue;
                                }
                                let Some(status) = response.status.as_ref() else {
                                    continue;
                                };
                                let facts = ReceiptFacts::from_status(status);
                                if !facts.terminal {
                                    continue;
                                }
                                let tracked = pending
                                    .remove(receipt_id)
                                    .context("terminal receipt disappeared from tracker")?;
                                manifests
                                    .write_receipt(&ReceiptManifestEntry {
                                        run_id: run_id.clone(),
                                        logical_batch_id: tracked.logical_batch_id,
                                        table: tracked.table,
                                        receipt_id: tracked.receipt_id,
                                        state: facts.state.clone(),
                                        purge_allowed: response.purge_allowed,
                                        terminal_at_unix_ms: now_unix_millis(),
                                        delta_version: facts.delta_version,
                                        rows_published: facts.rows_published,
                                        bytes_published: facts.bytes_published,
                                        error_code: facts.error_code,
                                    })
                                    .await?;
                                if facts.state == "published" && response.purge_allowed {
                                    report.published += 1;
                                } else {
                                    report.failures += 1;
                                }
                            }
                        }
                    }
                }
            }
            Ok(report)
        });
        (sender, task)
    }
}

async fn capture_routing(arguments: &RunArgs) -> Result<RoutingEvidence> {
    let config = SqlServerRoutingConfig::with_password_file(
        &arguments.sql_host,
        arguments.sql_port,
        &arguments.sql_database,
        &arguments.sql_username,
        &arguments.sql_password_file,
    )?
    .trust_server_certificate(true)
    .with_timeouts(arguments.request_timeout.get(), arguments.request_timeout.get());
    let store = SqlServerRoutingStore::new(config);
    let loaded_at = UnixMillis::new(now_unix_millis())?;
    let view = store.load_view(loaded_at).await?;
    let collectors = view
        .collectors
        .iter()
        .map(|(id, collector)| RoutingCollectorEvidence {
            instance_id: id.to_string(),
            instance_key: collector.registration.instance_key.clone(),
            implementation_pool: collector.registration.implementation_pool.to_string(),
            endpoint: collector.registration.flight_endpoint.to_string(),
            state: format!("{:?}", collector.state).to_lowercase(),
            received_rows: collector.pressure.as_ref().map(|pressure| pressure.received_rows.get()),
            received_batches: collector
                .pressure
                .as_ref()
                .map(|pressure| pressure.received_batches.get()),
        })
        .collect::<Vec<_>>();
    let leases = view
        .leases
        .values()
        .map(|lease| RoutingLeaseEvidence {
            implementation_pool: lease.key.implementation_pool.to_string(),
            table: lease.key.table.to_string(),
            holder_instance_id: lease.holder_instance_id.to_string(),
            fencing_epoch: lease.fencing_epoch.get(),
            lease_version: lease.lease_version,
            state: format!("{:?}", lease.state).to_lowercase(),
        })
        .collect();
    Ok(RoutingEvidence {
        loaded_at_unix_ms: view.loaded_at_unix_ms.get(),
        collectors,
        leases,
    })
}

#[derive(Serialize)]
struct RoutingEvidence {
    loaded_at_unix_ms: u64,
    collectors: Vec<RoutingCollectorEvidence>,
    leases: Vec<RoutingLeaseEvidence>,
}

#[derive(Serialize)]
struct RoutingCollectorEvidence {
    instance_id: String,
    instance_key: String,
    implementation_pool: String,
    endpoint: String,
    state: String,
    received_rows: Option<u64>,
    received_batches: Option<u64>,
}

#[derive(Serialize)]
struct RoutingLeaseEvidence {
    implementation_pool: String,
    table: String,
    holder_instance_id: String,
    fencing_epoch: u64,
    lease_version: u64,
    state: String,
}
