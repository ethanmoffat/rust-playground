use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use delta_inspect::{SnapshotSummary, TableSummary};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::fs;

use crate::{
    cli::ValidateArgs,
    manifest::{
        AckOutcome, CAPABILITY_EVIDENCE_FILE, RECEIPT_MANIFEST_FILE, ROUTING_EVIDENCE_FILE, ReceiptManifestEntry,
        SCHEMA_CONTRACT_FILE, SEND_MANIFEST_FILE, SchemaContract, SendManifestEntry, VALIDATION_FILE,
    },
};

/// Independently validates manifests, direct/dataflow Delta snapshots, routing,
/// delivery guarantees, and paired logical parity.
///
/// # Errors
///
/// Returns malformed evidence/snapshot failures or an aggregate failed validation.
pub async fn validate(arguments: ValidateArgs) -> Result<()> {
    let sends: Vec<SendManifestEntry> = read_json_lines(&arguments.results.join(SEND_MANIFEST_FILE)).await?;
    let receipts: Vec<ReceiptManifestEntry> = read_json_lines(&arguments.results.join(RECEIPT_MANIFEST_FILE)).await?;
    let contracts: Vec<SchemaContract> = read_json(&arguments.results.join(SCHEMA_CONTRACT_FILE)).await?;
    let routing: RoutingEvidence = read_json(&arguments.results.join(ROUTING_EVIDENCE_FILE)).await?;
    let capabilities: Value = read_json(&arguments.results.join(CAPABILITY_EVIDENCE_FILE)).await?;

    let mut failures = Vec::new();
    check(!sends.is_empty(), "send manifest is empty", &mut failures);
    check(
        contracts.len() == 13,
        "schema contract does not contain 13 tables",
        &mut failures,
    );
    let contract_map = contracts
        .iter()
        .map(|contract| (contract.table.clone(), contract))
        .collect::<BTreeMap<_, _>>();
    check(
        contract_map.len() == contracts.len(),
        "schema contract contains duplicate table identities",
        &mut failures,
    );

    let receipt_map = receipts
        .iter()
        .map(|receipt| (receipt.receipt_id, receipt))
        .collect::<BTreeMap<_, _>>();
    check(
        receipt_map.len() == receipts.len(),
        "receipt manifest contains duplicate receipt identifiers",
        &mut failures,
    );

    let mut attempts = BTreeMap::<(String, String), Vec<&SendManifestEntry>>::new();
    let mut paired = BTreeMap::<String, BTreeMap<String, &SendManifestEntry>>::new();
    for send in &sends {
        check(
            contract_map.contains_key(&send.table),
            &format!("send manifest contains unknown table {}", send.table),
            &mut failures,
        );
        if send.outcome == AckOutcome::DurableReceipt {
            check(
                send.pool == "direct" && send.receipt_id.is_some(),
                "durable acknowledgement is not a direct receipt",
                &mut failures,
            );
        }
        if send.outcome == AckOutcome::BestEffortAdmission {
            check(
                send.pool == "dataflow" && send.receipt_id.is_none(),
                "best-effort acknowledgement is not honest dataflow admission",
                &mut failures,
            );
        }
        attempts
            .entry((send.pool.clone(), send.logical_batch_id.clone()))
            .or_default()
            .push(send);
        if send.scenario == crate::cli::Scenario::Paired && send.outcome != AckOutcome::Error {
            paired
                .entry(send.logical_batch_id.clone())
                .or_default()
                .entry(send.pool.clone())
                .or_insert(send);
        }
    }

    for (logical_id, pools) in &paired {
        let direct = pools.get("direct");
        let dataflow = pools.get("dataflow");
        check(
            direct.is_some() && dataflow.is_some(),
            &format!("paired logical batch {logical_id} did not succeed in both pools"),
            &mut failures,
        );
        if let (Some(direct), Some(dataflow)) = (direct, dataflow) {
            check(
                direct.table == dataflow.table
                    && direct.rows == dataflow.rows
                    && direct.schema_fingerprint == dataflow.schema_fingerprint
                    && direct.payload_sha256 == dataflow.payload_sha256
                    && direct.row_sha256 == dataflow.row_sha256,
                &format!("paired logical batch {logical_id} changed between pools"),
                &mut failures,
            );
        }
    }

    let mut admitted_rows = BTreeMap::<String, BTreeMap<String, u64>>::new();
    let mut required_rows = BTreeMap::<String, BTreeMap<String, u64>>::new();
    let mut expected_tables = BTreeMap::<String, BTreeSet<String>>::new();
    for ((pool, logical_id), values) in &attempts {
        let successes = values
            .iter()
            .copied()
            .filter(|entry| entry.outcome != AckOutcome::Error)
            .collect::<Vec<_>>();
        check(
            !successes.is_empty(),
            &format!("{pool} logical batch {logical_id} exhausted every attempt"),
            &mut failures,
        );
        if successes.is_empty() {
            continue;
        }
        let first = successes[0];
        check(
            successes.iter().all(|entry| {
                entry.table == first.table
                    && entry.rows == first.rows
                    && entry.payload_sha256 == first.payload_sha256
                    && entry.schema_fingerprint == first.schema_fingerprint
            }),
            &format!("{pool} logical batch {logical_id} changed across retries"),
            &mut failures,
        );
        let delivered = if pool == "direct" {
            let mut published = false;
            for success in successes {
                let Some(receipt_id) = success.receipt_id else {
                    continue;
                };
                let terminal = receipt_map.get(&receipt_id);
                check(
                    terminal.is_some(),
                    &format!("direct receipt {receipt_id} lacks a terminal poll result"),
                    &mut failures,
                );
                if let Some(terminal) = terminal {
                    check(
                        matches!(terminal.state.as_str(), "published" | "failed" | "publication_unknown"),
                        &format!("direct receipt {receipt_id} is not terminal"),
                        &mut failures,
                    );
                    published |= terminal.state == "published" && terminal.purge_allowed;
                }
            }
            check(
                published,
                &format!("direct logical batch {logical_id} has no published copy"),
                &mut failures,
            );
            published
        } else {
            true
        };
        if delivered {
            let rows = u64::try_from(first.rows).unwrap_or(u64::MAX);
            *admitted_rows
                .entry(pool.clone())
                .or_default()
                .entry(first.table.clone())
                .or_default() += rows;
            let required = required_rows
                .entry(pool.clone())
                .or_default()
                .entry(first.table.clone())
                .or_insert(0);
            if pool == "direct" {
                *required += rows;
            } else if *required == 0 || rows < *required {
                *required = rows;
            }
            expected_tables
                .entry(pool.clone())
                .or_default()
                .insert(first.table.clone());
        }
    }

    let all_tables = contract_map.keys().cloned().collect::<BTreeSet<_>>();
    for (pool, tables) in &expected_tables {
        check(
            tables == &all_tables,
            &format!("{pool} traffic did not cover every registered table"),
            &mut failures,
        );
    }

    let direct = if expected_tables.contains_key("direct") {
        Some(
            wait_for_snapshot(
                &arguments.direct_root,
                required_rows.get("direct").context("direct row expectations missing")?,
                arguments.timeout.get(),
            )
            .await?,
        )
    } else {
        None
    };
    let dataflow = if expected_tables.contains_key("dataflow") {
        Some(
            wait_for_snapshot(
                &arguments.dataflow_root,
                required_rows
                    .get("dataflow")
                    .context("dataflow row expectations missing")?,
                arguments.timeout.get(),
            )
            .await?,
        )
    } else {
        None
    };
    if let Some(snapshot) = &direct {
        validate_snapshot(
            "direct",
            snapshot,
            &contract_map,
            required_rows.get("direct"),
            &mut failures,
        );
    }

    async fn wait_for_snapshot(
        root: &Path,
        expected: &BTreeMap<String, u64>,
        timeout: Duration,
    ) -> Result<SnapshotSummary> {
        let deadline = Instant::now() + timeout;
        loop {
            let error = match delta_inspect::summarize(root).await {
                Ok(snapshot)
                    if expected.iter().all(|(table, rows)| {
                        snapshot.tables.get(table).is_some_and(|summary| summary.rows >= *rows)
                    }) =>
                {
                    return Ok(snapshot);
                }
                Ok(snapshot) => anyhow::anyhow!(
                    "snapshot has {} tables and {} files but expected rows are not visible",
                    snapshot.tables.len(),
                    snapshot.files
                ),
                Err(error) => error,
            };
            if Instant::now() >= deadline {
                return Err(error);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    if let Some(snapshot) = &dataflow {
        validate_snapshot(
            "dataflow",
            snapshot,
            &contract_map,
            required_rows.get("dataflow"),
            &mut failures,
        );
    }
    if let (Some(direct), Some(dataflow)) = (&direct, &dataflow) {
        for table in &all_tables {
            if let (Some(direct), Some(dataflow)) = (direct.tables.get(table), dataflow.tables.get(table)) {
                check(
                    direct.schema == dataflow.schema,
                    &format!("{table} Delta schemas differ between pools"),
                    &mut failures,
                );
                check(
                    direct.row_hashes.keys().eq(dataflow.row_hashes.keys()),
                    &format!("{table} deterministic row values differ between pools"),
                    &mut failures,
                );
            }
        }
    }

    validate_routing(
        &routing,
        &all_tables,
        &expected_tables,
        arguments.require_all_collectors,
        &mut failures,
    );
    validate_capabilities(&capabilities, &mut failures);

    let report = ValidationReport {
        passed: failures.is_empty(),
        failures,
        admitted_rows,
        required_rows,
        direct,
        dataflow,
        routing,
    };
    let mut bytes = serde_json::to_vec_pretty(&report)?;
    bytes.push(b'\n');
    fs::write(arguments.results.join(VALIDATION_FILE), bytes).await?;
    if !report.passed {
        bail!(
            "loadtest validation failed with {} issue(s); see {}",
            report.failures.len(),
            arguments.results.join(VALIDATION_FILE).display()
        );
    }
    println!(
        "PASS: 13 schemas, complete rows, paired parity, receipts, routing, capabilities, and {} Delta files",
        report.direct.as_ref().map_or(0, |snapshot| snapshot.files)
            + report.dataflow.as_ref().map_or(0, |snapshot| snapshot.files)
    );
    Ok(())
}

fn validate_snapshot(
    pool: &str,
    snapshot: &SnapshotSummary,
    contracts: &BTreeMap<String, &SchemaContract>,
    expected_rows: Option<&BTreeMap<String, u64>>,
    failures: &mut Vec<String>,
) {
    let Some(expected_rows) = expected_rows else {
        check(false, &format!("{pool} expected-row accounting is missing"), failures);
        return;
    };
    for (table, rows) in expected_rows {
        let actual = snapshot.tables.get(table);
        check(
            actual.is_some(),
            &format!("{pool} Delta snapshot is missing {table}"),
            failures,
        );
        let Some(actual) = actual else {
            continue;
        };
        check(
            actual.rows >= *rows,
            &format!(
                "{pool} {table} has {} rows but at least {rows} logical rows were required",
                actual.rows
            ),
            failures,
        );
        validate_table_storage(pool, table, actual, failures);
        if let Some(contract) = contracts.get(table) {
            validate_delta_schema(pool, table, actual, contract, failures);
        }
    }
    for table in snapshot.tables.keys() {
        check(
            contracts.contains_key(table),
            &format!("{pool} Delta root contains cross-table output {table}"),
            failures,
        );
    }
}

fn validate_table_storage(pool: &str, table: &str, summary: &TableSummary, failures: &mut Vec<String>) {
    check(
        summary.version >= 1,
        &format!("{pool} {table} has no data Delta version"),
        failures,
    );
    check(
        summary.active_files > 0
            && summary.bytes > 0
            && summary.minimum_file_bytes > 0
            && summary.maximum_file_bytes <= 1_073_741_824,
        &format!("{pool} {table} has invalid active file-size evidence"),
        failures,
    );
    check(
        summary.files.iter().map(|file| file.rows).sum::<u64>() == summary.rows,
        &format!("{pool} {table} file row counts do not match snapshot rows"),
        failures,
    );
}

fn validate_delta_schema(
    pool: &str,
    table: &str,
    summary: &TableSummary,
    contract: &SchemaContract,
    failures: &mut Vec<String>,
) {
    let fields = summary
        .schema
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut expected = Vec::<(String, String, bool)>::new();
    for field in &contract.fields {
        let delta_type = match field.arrow_type.as_str() {
            "Boolean" => "boolean",
            "UInt8" | "Int16" => "short",
            "Int32" => "integer",
            "Int64" => "long",
            "Float64" => "double",
            "Utf8" => "string",
            value if value.starts_with("Timestamp(") => "timestamp",
            other => {
                check(
                    false,
                    &format!("unsupported expected Arrow type {other} for {table}"),
                    failures,
                );
                continue;
            }
        };
        expected.push((field.name.clone(), delta_type.to_owned(), field.nullable));
        if delta_type == "timestamp" {
            expected.push((format!("{}__unix_nanos", field.name), "long".to_owned(), field.nullable));
        }
    }
    check(
        fields.len() == expected.len(),
        &format!(
            "{pool} {table} Delta schema has {} fields; expected {}",
            fields.len(),
            expected.len()
        ),
        failures,
    );
    for (index, (name, data_type, nullable)) in expected.iter().enumerate() {
        let Some(field) = fields.get(index) else {
            break;
        };
        check(
            field.get("name").and_then(Value::as_str) == Some(name.as_str())
                && field.get("type").and_then(Value::as_str) == Some(data_type.as_str())
                && field.get("nullable").and_then(Value::as_bool) == Some(*nullable),
            &format!("{pool} {table} Delta schema mismatch at field {index} ({name})"),
            failures,
        );
    }
}

fn validate_routing(
    routing: &RoutingEvidence,
    all_tables: &BTreeSet<String>,
    expected_pools: &BTreeMap<String, BTreeSet<String>>,
    require_all_collectors: bool,
    failures: &mut Vec<String>,
) {
    let collectors = routing
        .collectors
        .iter()
        .map(|collector| (collector.instance_id.as_str(), collector))
        .collect::<BTreeMap<_, _>>();
    for pool in expected_pools.keys() {
        let pool_collectors = routing
            .collectors
            .iter()
            .filter(|collector| &collector.implementation_pool == pool)
            .collect::<Vec<_>>();
        check(
            pool_collectors.len() == 3,
            &format!("{pool} did not register exactly three collectors"),
            failures,
        );
        let leases = routing
            .leases
            .iter()
            .filter(|lease| &lease.implementation_pool == pool && all_tables.contains(&lease.table))
            .collect::<Vec<_>>();
        check(
            leases.len() == all_tables.len(),
            &format!("{pool} does not have one routing lease for every table"),
            failures,
        );
        let mut leased_tables = BTreeSet::new();
        let mut holders = BTreeSet::new();
        for lease in leases {
            check(
                leased_tables.insert(lease.table.clone()),
                &format!("{pool} has duplicate routing affinity for {}", lease.table),
                failures,
            );
            let collector = collectors.get(lease.holder_instance_id.as_str());
            check(
                collector.is_some_and(|collector| collector.implementation_pool == *pool),
                &format!("{pool} {} is routed to a collector in the wrong pool", lease.table),
                failures,
            );
            holders.insert(lease.holder_instance_id.clone());
        }
        if require_all_collectors {
            check(
                holders.len() == 3,
                &format!("{pool} table distribution did not use all three collectors"),
                failures,
            );
        }
        for holder in holders {
            if let Some(collector) = collectors.get(holder.as_str()) {
                check(
                    collector.received_batches.unwrap_or(0) > 0,
                    &format!("{pool} lease holder {} received no batches", collector.instance_key),
                    failures,
                );
            }
        }
    }
}

fn validate_capabilities(capabilities: &Value, failures: &mut Vec<String>) {
    let direct = capabilities
        .get("direct")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let dataflow = capabilities
        .get("dataflow")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    check(direct.len() == 3, "direct capability evidence is incomplete", failures);
    check(
        dataflow.len() == 3,
        "dataflow capability evidence is incomplete",
        failures,
    );
    for entry in direct {
        let response = &entry["response"];
        let values = response["capabilities"].as_array().cloned().unwrap_or_default();
        check(
            response["implementation_pool"] == "direct" && values.iter().any(|value| value == "receipt_status"),
            "direct collector did not advertise durable receipt status",
            failures,
        );
    }
    for entry in dataflow {
        let response = &entry["response"];
        let values = response["capabilities"].as_array().cloned().unwrap_or_default();
        check(
            response["delivery_guarantee"] == "best_effort"
                && response["completion_mode"] == "best_effort_admission"
                && !values.iter().any(|value| value == "receipt_status"),
            "dataflow collector capability gap was not reported honestly",
            failures,
        );
    }
}

fn check(condition: bool, message: &str, failures: &mut Vec<String>) {
    if !condition {
        failures.push(message.to_owned());
    }
}

async fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = fs::read(path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

async fn read_json_lines<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    let text = fs::read_to_string(path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str(line).with_context(|| format!("parse {} line {}", path.display(), index + 1))
        })
        .collect()
}

#[derive(Deserialize, Serialize)]
struct RoutingEvidence {
    loaded_at_unix_ms: u64,
    collectors: Vec<RoutingCollectorEvidence>,
    leases: Vec<RoutingLeaseEvidence>,
}

#[derive(Deserialize, Serialize)]
struct RoutingCollectorEvidence {
    instance_id: String,
    instance_key: String,
    implementation_pool: String,
    endpoint: String,
    state: String,
    received_rows: Option<u64>,
    received_batches: Option<u64>,
}

#[derive(Deserialize, Serialize)]
struct RoutingLeaseEvidence {
    implementation_pool: String,
    table: String,
    holder_instance_id: String,
    fencing_epoch: u64,
    lease_version: u64,
    state: String,
}

#[derive(Serialize)]
struct ValidationReport {
    passed: bool,
    failures: Vec<String>,
    admitted_rows: BTreeMap<String, BTreeMap<String, u64>>,
    required_rows: BTreeMap<String, BTreeMap<String, u64>>,
    direct: Option<SnapshotSummary>,
    dataflow: Option<SnapshotSummary>,
    routing: RoutingEvidence,
}
