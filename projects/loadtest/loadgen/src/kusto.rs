use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use delta_inspect::{SnapshotInventory, SnapshotSummary, TableInventory};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    cli::{MirrorDeltaArgs, RunArgs},
    transport::now_unix_millis,
};

const DATABASE: &str = "loadtest";
const STATUS: &str = "kusto-status.json";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct KustoClient {
    http: Client,
    endpoint: Url,
    results: PathBuf,
    database: String,
}

impl KustoClient {
    fn new(endpoint: &str, results: &Path) -> Result<Self> {
        let endpoint = Url::parse(endpoint).context("invalid Kusto endpoint")?;
        ensure!(
            endpoint.scheme() == "http"
                && endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none()
                && endpoint.path() == "/",
            "Kusto endpoint must be a plain HTTP emulator origin without credentials"
        );
        Ok(Self {
            http: Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .connect_timeout(Duration::from_secs(3))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint,
            results: results.to_owned(),
            database: DATABASE.to_owned(),
        })
    }

    async fn execute(&self, command: &str, management: bool, phase: &str) -> Result<Vec<BTreeMap<String, Value>>> {
        let route = if management { "v1/rest/mgmt" } else { "v2/rest/query" };
        let mut request = json!({
            "csl": command,
            "properties": "{\"Options\":{\"notruncation\":true}}"
        });
        if phase != "bootstrap" {
            request["db"] = json!(self.database);
        }
        let response = self
            .http
            .post(self.endpoint.join(route)?)
            .json(&request)
            .send()
            .await
            .context("Kusto HTTP request failed")?;
        let status = response.status();
        let body: Value = response.json().await.context("Kusto returned non-JSON response")?;
        append(
            &self.results.join("kusto-queries.jsonl"),
            &json!({
                "at_unix_ms": now_unix_millis(), "phase": phase,
                "management": management, "csl": command,
                "http_status": status.as_u16(), "response": body
            }),
        )?;
        ensure!(status.is_success(), "Kusto HTTP {status}: {body}");
        decode_response(&body)
    }

    async fn bootstrap(&self) -> Result<()> {
        let rows = self.execute(".show databases", true, "bootstrap").await?;
        if rows
            .iter()
            .any(|row| row.get("DatabaseName").and_then(Value::as_str) == Some(self.database.as_str()))
        {
            return Ok(());
        }
        let marker = self.results.join("kusto-database.json");
        let database = identifier(&self.database)?;
        let path = format!("/kustodata/dbs/{}", self.database);
        let command = if marker.exists() {
            format!(".attach database {database} from @\"{path}/md\"")
        } else {
            format!(".create database {database} persist (@\"{path}/md\", @\"{path}/data\")")
        };
        self.execute(&command, true, "bootstrap").await?;
        write_json(&marker, &json!({"database": self.database}))?;
        Ok(())
    }

    async fn tags(&self, table: &str) -> Result<BTreeSet<String>> {
        let rows = self
            .execute(
                &format!(".show table {table} extents | project Tags"),
                true,
                "reconcile",
            )
            .await?;
        let mut result = BTreeSet::new();
        for row in rows {
            let value = row.get("Tags").context("Kusto extent tags are missing")?;
            result.extend(ingestion_tags(value)?);
        }
        Ok(result)
    }

    async fn schema(&self, table: &str) -> Result<Vec<(String, String)>> {
        self.execute(
            &format!("{table} | getschema | project ColumnName, ColumnType"),
            false,
            "schema",
        )
        .await?
        .into_iter()
        .map(|row| {
            Ok((
                row.get("ColumnName")
                    .and_then(Value::as_str)
                    .context("Kusto schema column name missing")?
                    .to_owned(),
                row.get("ColumnType")
                    .and_then(Value::as_str)
                    .context("Kusto schema column type missing")?
                    .to_owned(),
            ))
        })
        .collect()
    }

    async fn ingest(&self, table: &str, file: &str, identity: &str, phase: &str) -> Result<()> {
        let command = format!(
            ".ingest into table {table} ({}) with (format=\"parquet\", ingestionMappingReference=\"loadtest\", \
             tags={}, ingestIfNotExists={})",
            literal(file)?,
            literal(&json!([format!("ingest-by:loadtest:{identity}")]).to_string())?,
            literal(&json!([format!("loadtest:{identity}")]).to_string())?,
        );
        self.execute(&command, true, phase).await?;
        Ok(())
    }
}

fn ingestion_tags(value: &Value) -> Result<BTreeSet<String>> {
    let tags = if let Some(text) = value.as_str() {
        if text.trim_start().starts_with('[') {
            serde_json::from_str::<Vec<String>>(text).context("invalid Kusto extent tag array")?
        } else {
            text.lines().map(str::to_owned).collect()
        }
    } else {
        serde_json::from_value::<Vec<String>>(value.clone()).context("invalid Kusto extent tags")?
    };
    Ok(tags
        .iter()
        .filter_map(|tag| tag.strip_prefix("ingest-by:loadtest:").map(str::to_owned))
        .collect())
}

fn decode_response(body: &Value) -> Result<Vec<BTreeMap<String, Value>>> {
    if body.get("error").is_some() || body.get("Error").is_some() {
        bail!("Kusto command failed: {body}");
    }
    let tables = if let Some(frames) = body.as_array() {
        ensure!(
            frames.iter().any(|frame| frame["FrameType"] == "DataSetCompletion"),
            "Kusto query response has no completion frame"
        );
        for frame in frames {
            ensure!(
                frame["HasErrors"] != true
                    && frame
                        .get("OneApiErrors")
                        .is_none_or(|errors| errors.is_null() || errors.as_array().is_some_and(Vec::is_empty))
                    && frame["FrameType"] != "DataSetError"
                    && frame["FrameType"] != "TableFragment",
                "Kusto partial/progressive query failure: {frame}"
            );
        }
        frames
            .iter()
            .filter(|frame| frame["FrameType"] == "DataTable")
            .collect::<Vec<_>>()
    } else {
        body.get("Tables")
            .and_then(Value::as_array)
            .context("Kusto response has no result tables")?
            .iter()
            .collect()
    };
    let mut result = Vec::new();
    let mut found_primary = false;
    for table in tables {
        let columns = table["Columns"].as_array().context("Kusto result columns missing")?;
        let names = columns
            .iter()
            .map(|column| {
                column["ColumnName"]
                    .as_str()
                    .context("Kusto result column name missing")
            })
            .collect::<Result<Vec<_>>>()?;
        let rows = table["Rows"].as_array().context("Kusto result rows missing")?;
        let primary = table["TableKind"] == "PrimaryResult" || (table.get("TableKind").is_none() && !found_primary);
        for row in rows {
            let values = row.as_array().context("Kusto result row is not an array")?;
            ensure!(names.len() == values.len(), "Kusto result row width mismatch");
            if let Some(index) = names.iter().position(|name| *name == "Severity") {
                ensure!(
                    values[index].as_u64().is_some_and(|severity| severity > 2),
                    "Kusto in-band query failure: {row}"
                );
            }
            if primary {
                result.push(
                    names
                        .iter()
                        .zip(values)
                        .map(|(name, value)| ((*name).to_owned(), value.clone()))
                        .collect(),
                );
            }
        }
        found_primary |= primary;
    }
    ensure!(found_primary, "Kusto response has no primary result");
    Ok(result)
}

fn literal(value: &str) -> Result<String> {
    Ok(serde_json::to_string(value)?)
}

fn identifier(value: &str) -> Result<String> {
    ensure!(
        !value.is_empty()
            && value
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
            && !value.starts_with(|character: char| character.is_ascii_digit()),
        "unsupported Kusto identifier {value:?}"
    );
    Ok(format!("['{value}']"))
}

fn table_name(pool: &str, identity: &str) -> Result<String> {
    ensure!(matches!(pool, "direct" | "dataflow"), "unknown Kusto source pool");
    let name = identity
        .strip_prefix("telemetry.sqlserver.")
        .context("unexpected loadtest table namespace")?;
    ensure!(
        name.starts_with("sqlserver_") && !name.contains('.'),
        "unexpected loadtest table identity"
    );
    identifier(&format!("{pool}_{name}"))
}

fn columns(schema: &Value) -> Result<Vec<(String, String)>> {
    schema["fields"]
        .as_array()
        .context("Delta schema fields missing")?
        .iter()
        .map(|field| {
            let name = field["name"].as_str().context("Delta column name missing")?;
            identifier(name)?;
            let data_type = match field["type"].as_str().context("unsupported nested Delta field")? {
                "boolean" => "bool",
                "byte" | "short" | "integer" => "int",
                "long" => "long",
                "decimal(20,0)" => "decimal",
                "float" | "double" => "real",
                "string" => "string",
                "timestamp" => "datetime",
                other => bail!("unsupported Delta type {other} for {name}"),
            };
            Ok((name.to_owned(), data_type.to_owned()))
        })
        .collect()
}

fn file_identity(run_id: &str, pool: &str, table: &str, file: &str) -> String {
    hex::encode(Sha256::digest(format!("{run_id}\0{pool}\0{table}\0{file}")))
}

fn container_file(pool: &str, table: &str, file: &str) -> Result<String> {
    table_name(pool, table)?;
    ensure!(
        !file.is_empty()
            && Path::new(file)
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "unsafe active file path"
    );
    Ok(format!("/delta/{pool}/{}/{file}", table.replace('.', "/")))
}

fn append(file: &Path, value: &Value) -> Result<()> {
    let mut output = OpenOptions::new().create(true).append(true).open(file)?;
    serde_json::to_writer(&mut output, value)?;
    output.write_all(b"\n")?;
    Ok(())
}

fn write_json(file: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = file.with_extension("json.next");
    fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    fs::rename(temporary, file)?;
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StandaloneLayout {
    delta_root: PathBuf,
    container_root: PathBuf,
    endpoint: String,
    database: String,
    table_prefix: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tables: Vec<String>,
}

impl StandaloneLayout {
    fn new(arguments: &MirrorDeltaArgs) -> Result<Self> {
        identifier(&arguments.database)?;
        identifier(&arguments.table_prefix)?;
        ensure!(
            arguments.container_root.is_absolute()
                && arguments
                    .container_root
                    .components()
                    .all(|part| matches!(part, Component::RootDir | Component::Normal(_))),
            "container root must be an absolute Linux mount path without parent components"
        );
        let delta_root = fs::canonicalize(&arguments.delta_root).context("open Arrowcol Delta root")?;
        ensure!(delta_root.is_dir(), "Delta root is not a directory");
        let mut layout = Self {
            delta_root,
            container_root: arguments.container_root.clone(),
            endpoint: arguments.endpoint.clone(),
            database: arguments.database.clone(),
            table_prefix: arguments.table_prefix.clone(),
            tables: arguments.tables.clone(),
        };
        layout.tables.sort();
        layout.tables.dedup();
        for table in &layout.tables {
            layout.table_name(table)?;
        }
        Ok(layout)
    }

    fn table_name(&self, table: &str) -> Result<String> {
        let parts = table.split('.').collect::<Vec<_>>();
        ensure!(parts.len() == 3, "expected Arrowcol catalog.schema.table identity");
        for part in &parts {
            identifier(part)?;
        }
        identifier(&format!("{}_{}", self.table_prefix, parts.join("_")))
    }

    fn container_file(&self, table: &str, file: &str) -> Result<String> {
        self.table_name(table)?;
        ensure!(
            !file.is_empty()
                && !file.contains('\\')
                && Path::new(file)
                    .components()
                    .all(|part| matches!(part, Component::Normal(_))),
            "unsafe active file path"
        );
        self.container_root
            .join(table.replace('.', "/"))
            .join(file)
            .to_str()
            .context("non-UTF8 container file path")
            .map(str::to_owned)
    }
}

#[derive(Deserialize, Serialize)]
struct BridgeCheckpoint {
    layout: StandaloneLayout,
    observed: SnapshotInventory,
}

impl BridgeCheckpoint {
    fn observe(&mut self, snapshot: SnapshotInventory) -> Result<()> {
        let mut names = BTreeSet::new();
        for table in snapshot.tables.keys() {
            ensure!(
                names.insert(self.layout.table_name(table)?),
                "Delta table names collide in Kusto"
            );
        }
        for (table, previous) in &self.observed.tables {
            let current = snapshot
                .tables
                .get(table)
                .context("previously observed Delta table disappeared")?;
            ensure!(current.schema == previous.schema, "Delta schema changed for {table}");
            ensure!(
                current.version >= previous.version,
                "Delta version moved backwards for {table}"
            );
            let files = current.files.iter().collect::<BTreeSet<_>>();
            ensure!(
                previous.files.iter().all(|file| files.contains(file)),
                "active files were removed/replaced for {table}; only append-only Delta is supported"
            );
        }
        self.observed = snapshot;
        Ok(())
    }
}

/// Mirror fixed-schema, append-only Arrowcol Delta output independently of load generation.
///
/// # Errors
///
/// Rejects invalid settings, incompatible checkpoint reuse, and one-shot synchronization
/// failures. Watch mode records synchronization failures as degraded and retries.
pub async fn mirror_delta(arguments: MirrorDeltaArgs) -> Result<()> {
    ensure!(
        !arguments.poll_interval.get().is_zero(),
        "poll interval must be positive"
    );
    let layout = StandaloneLayout::new(&arguments)?;
    fs::create_dir_all(&arguments.state_dir)?;
    let checkpoint_path = arguments.state_dir.join("delta-bridge.json");
    let mut checkpoint = if checkpoint_path.exists() {
        let saved: BridgeCheckpoint =
            serde_json::from_slice(&fs::read(&checkpoint_path)?).context("read Delta bridge checkpoint")?;
        ensure!(
            saved.layout == layout,
            "bridge settings differ from checkpoint; reuse the original settings or use a new state directory and destination"
        );
        saved
    } else {
        BridgeCheckpoint {
            layout: layout.clone(),
            observed: SnapshotInventory {
                tables: BTreeMap::new(),
            },
        }
    };
    // Bind ingestion identities to the source and destination, not a process or demo run.
    let source_id = hex::encode(Sha256::digest(serde_json::to_vec(&layout)?));
    let mut mirror = Mirror::new(&layout.endpoint, &arguments.state_dir, &source_id)?;
    mirror.client.database.clone_from(&layout.database);
    mirror.layout = Some(layout.clone());
    write_json(&checkpoint_path, &checkpoint)?;
    eprintln!(
        "Mirroring {} -> {} database {} (Ctrl+C stops only the bridge)",
        layout.delta_root.display(),
        layout.endpoint,
        layout.database
    );
    loop {
        let result = async {
            let snapshot = delta_inspect::inventory_selected(&layout.delta_root, &layout.tables).await?;
            ensure!(
                !arguments.once || !snapshot.tables.is_empty(),
                "no committed Delta tables found"
            );
            checkpoint.observe(snapshot)?;
            write_json(&checkpoint_path, &checkpoint)?;
            mirror.client.bootstrap().await?;
            mirror
                .sync("direct", &checkpoint.observed, &AtomicBool::new(false), true)
                .await
        }
        .await;
        mirror.persist(result.as_ref().err())?;
        if result.is_ok() {
            if checkpoint.observed.tables.is_empty() {
                mirror.status.state = "pending".to_owned();
                write_json(&arguments.state_dir.join(STATUS), &mirror.status)?;
            }
            if arguments.once {
                eprintln!(
                    "Kusto caught up: {} tables, {} files",
                    mirror.status.tables.len(),
                    mirror.status.files_ingested
                );
            }
        }
        if arguments.once {
            return result;
        }
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("listen for Ctrl+C")?;
                return result;
            }
            () = tokio::time::sleep(arguments.poll_interval.get()) => {}
        }
    }
}

#[derive(Default, Deserialize, Serialize)]
struct MirrorStatus {
    state: String,
    run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    live_observed: bool,
    live_tables: BTreeSet<String>,
    files_ingested: usize,
    tables: BTreeMap<String, TableProgress>,
}

#[derive(Deserialize, Serialize)]
struct TableProgress {
    version: u64,
    active_files: usize,
    ingested_files: usize,
}

struct Mirror {
    client: KustoClient,
    status: MirrorStatus,
    schemas: BTreeMap<String, Value>,
    ingested: BTreeMap<String, BTreeSet<String>>,
    layout: Option<StandaloneLayout>,
}

impl Mirror {
    fn new(endpoint: &str, results: &Path, run_id: &str) -> Result<Self> {
        Ok(Self {
            client: KustoClient::new(endpoint, results)?,
            status: MirrorStatus {
                state: "healthy".to_owned(),
                run_id: run_id.to_owned(),
                ..MirrorStatus::default()
            },
            schemas: BTreeMap::new(),
            ingested: BTreeMap::new(),
            layout: None,
        })
    }

    fn persist(&mut self, error: Option<&anyhow::Error>) -> Result<()> {
        if let Some(error) = error {
            let message = format!("{error:#}");
            if self.status.error.as_ref() != Some(&message) {
                let scope = if self.layout.is_some() {
                    "independent Delta bridge"
                } else {
                    "collector load continues"
                };
                eprintln!("Kusto DEGRADED ({scope}): {message}");
            }
            self.status.state = "degraded".to_owned();
            self.status.error = Some(message);
        } else {
            self.status.state = "healthy".to_owned();
            self.status.error = None;
        }
        self.status.live_observed = self.status.live_tables.len() == 26;
        self.status.files_ingested = self.ingested.values().map(BTreeSet::len).sum();
        write_json(&self.client.results.join(STATUS), &self.status)
    }

    async fn ensure_table(&mut self, name: &str, schema: &Value) -> Result<()> {
        if let Some(previous) = self.schemas.get(name) {
            ensure!(
                previous == schema,
                "Delta schema changed for {name}; only fixed schemas are supported"
            );
            return Ok(());
        }
        let expected = columns(schema)?;
        let declarations = expected
            .iter()
            .map(|(name, kind)| Ok(format!("{}:{kind}", identifier(name)?)))
            .collect::<Result<Vec<_>>>()?
            .join(",");
        self.client
            .execute(&format!(".create table {name} ({declarations})"), true, "schema")
            .await?;
        ensure!(
            self.client.schema(name).await? == expected,
            "Kusto schema differs for {name}"
        );
        let mapping = expected
            .iter()
            .map(|(name, kind)| json!({"Column":name, "DataType":kind, "Properties":{"Field":name}}))
            .collect::<Vec<_>>();
        self.client
            .execute(
                &format!(
                    ".create-or-alter table {name} ingestion parquet mapping \"loadtest\" {}",
                    literal(&serde_json::to_string(&mapping)?)?
                ),
                true,
                "schema",
            )
            .await?;
        self.ingested.insert(name.to_owned(), self.client.tags(name).await?);
        self.schemas.insert(name.to_owned(), schema.clone());
        Ok(())
    }

    async fn sync(&mut self, pool: &str, snapshot: &SnapshotInventory, live: &AtomicBool, all: bool) -> Result<()> {
        let names = snapshot
            .tables
            .keys()
            .map(|table| self.destination(pool, table))
            .collect::<Result<BTreeSet<_>>>()?;
        ensure!(
            names.len() == snapshot.tables.len(),
            "Delta table names collide in Kusto"
        );
        let prefix = self.layout.as_ref().map_or(pool, |layout| layout.table_prefix.as_str());
        for name in self
            .schemas
            .keys()
            .filter(|name| name.starts_with(&format!("['{prefix}_")))
        {
            ensure!(names.contains(name), "Delta table disappeared for {name}");
        }
        for (table, snapshot) in &snapshot.tables {
            if !all && !live.load(Ordering::Acquire) {
                break;
            }
            let name = self.destination(pool, table)?;
            self.ensure_table(&name, &snapshot.schema).await?;
            let expected = snapshot
                .files
                .iter()
                .map(|file| file_identity(&self.status.run_id, pool, table, file))
                .collect::<BTreeSet<_>>();
            let known = self.ingested.get(&name).context("Kusto table reconciliation missing")?;
            ensure!(
                known.is_subset(&expected),
                "active files were removed/replaced for {name}"
            );
            let missing = snapshot
                .files
                .iter()
                .filter(|file| !known.contains(&file_identity(&self.status.run_id, pool, table, file)))
                .take(if all { usize::MAX } else { 1 })
                .cloned()
                .collect::<Vec<_>>();
            for file in missing {
                let identity = file_identity(&self.status.run_id, pool, table, &file);
                let source = match &self.layout {
                    Some(layout) => layout.container_file(table, &file)?,
                    None => container_file(pool, table, &file)?,
                };
                // Reconcile before each retry: an earlier timeout may have committed.
                let known = self.client.tags(&name).await?;
                self.ingested.insert(name.clone(), known);
                if !self.ingested[&name].contains(&identity) {
                    self.client
                        .ingest(&name, &source, &identity, if all { "catchup" } else { "live" })
                        .await?;
                    let tags = self.client.tags(&name).await?;
                    ensure!(
                        tags.contains(&identity),
                        "Kusto did not publish ingestion identity for {source}"
                    );
                    self.ingested.insert(name.clone(), tags);
                }
                append(
                    &self.client.results.join("kusto-ingestions.jsonl"),
                    &json!({
                        "at_unix_ms":now_unix_millis(), "run_id":self.status.run_id,
                        "pool":pool, "table":table, "delta_version":snapshot.version,
                        "file":file, "identity":identity
                    }),
                )?;
            }
            if live.load(Ordering::Acquire)
                && !self.status.live_tables.contains(&name)
                && !self.ingested[&name].is_empty()
            {
                let rows = self
                    .client
                    .execute(&format!("{name} | count"), false, "live-observation")
                    .await?;
                if live.load(Ordering::Acquire)
                    && rows
                        .first()
                        .and_then(|row| row.get("Count"))
                        .and_then(Value::as_u64)
                        .is_some_and(|count| count > 0)
                {
                    self.status.live_tables.insert(name.clone());
                }
            }
            self.status.tables.insert(
                name.clone(),
                TableProgress {
                    version: snapshot.version,
                    active_files: snapshot.files.len(),
                    ingested_files: self.ingested[&name].len(),
                },
            );
        }
        Ok(())
    }

    fn destination(&self, pool: &str, table: &str) -> Result<String> {
        match &self.layout {
            Some(layout) => layout.table_name(table),
            None => table_name(pool, table),
        }
    }

    async fn sync_roots(&mut self, direct: &Path, dataflow: &Path, live: &AtomicBool, all: bool) -> Result<()> {
        self.client.bootstrap().await?;
        for (pool, root) in [("direct", direct), ("dataflow", dataflow)] {
            let snapshot = delta_inspect::inventory(root).await?;
            self.sync(pool, &snapshot, live, all).await?;
        }
        Ok(())
    }
}

pub(crate) struct LiveSync {
    stop: CancellationToken,
    generating: Arc<AtomicBool>,
    task: JoinHandle<Result<()>>,
}

impl LiveSync {
    pub(crate) fn start(arguments: &RunArgs, run_id: &str) -> Option<Self> {
        let endpoint = arguments.kusto_endpoint.clone()?;
        let arguments = arguments.clone();
        let run_id = run_id.to_owned();
        let stop = CancellationToken::new();
        let generating = Arc::new(AtomicBool::new(true));
        let stopped = stop.clone();
        let active = Arc::clone(&generating);
        let task = tokio::spawn(async move {
            let result = async {
                let mut mirror = Mirror::new(&endpoint, &arguments.results, &run_id)?;
                while !stopped.is_cancelled() {
                    let result = mirror
                        .sync_roots(&arguments.direct_root, &arguments.dataflow_root, &active, false)
                        .await;
                    mirror.persist(result.as_ref().err())?;
                    tokio::select! {
                        () = stopped.cancelled() => break,
                        () = tokio::time::sleep(POLL_INTERVAL) => {}
                    }
                }
                let final_sync = tokio::time::timeout(arguments.drain_timeout.get(), async {
                    settle_dataflow(
                        &arguments.dataflow_admin_endpoints,
                        arguments.drain_timeout.get(),
                        arguments.quiver,
                    )
                    .await?;
                    mirror
                        .sync_roots(&arguments.direct_root, &arguments.dataflow_root, &active, true)
                        .await
                })
                .await
                .context("Kusto final catch-up timed out")?;
                mirror.persist(final_sync.as_ref().err())?;
                final_sync
            }
            .await;
            if let Err(error) = &result {
                degraded(&arguments.results, &run_id, error)?;
            }
            result
        });
        Some(Self { stop, generating, task })
    }

    pub(crate) fn finish_generation(&self) {
        self.generating.store(false, Ordering::Release);
    }

    pub(crate) async fn stop(self) -> Result<()> {
        self.finish_generation();
        self.stop.cancel();
        self.task.await.context("Kusto synchronization task failed")?
    }
}

pub(crate) async fn settle_dataflow(endpoints: &[String], timeout: Duration, quiver: bool) -> Result<()> {
    ensure!(!endpoints.is_empty(), "Dataflow publication endpoints are missing");
    let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
    let deadline = Instant::now() + timeout;
    let mut replay = ReplayQuiescence::default();
    loop {
        let mut settled = true;
        let mut replay_counters = Vec::new();
        for endpoint in endpoints {
            let response = client
                .get(format!("{}/metrics", endpoint.trim_end_matches('/')))
                .send()
                .await?;
            let text = response.error_for_status()?.text().await?;
            settled &= publication_idle(&text)?;
            if quiver {
                settled &= metric_value(&text, "arrowcol_dataflow_durable_backlog_batches")? == 0;
                for name in [
                    "arrowcol_dataflow_replayed_batches_total",
                    "arrowcol_dataflow_replay_deferred_total",
                ] {
                    replay_counters.push(metric_value(&text, name)?);
                }
            }
        }
        if quiver {
            settled = replay.observe(settled, replay_counters, Instant::now());
        }
        if settled {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "Dataflow publication did not settle before Kusto validation"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn publication_idle(metrics: &str) -> Result<bool> {
    let mut idle = true;
    for name in [
        "arrowcol_flight_active_streams",
        "arrowcol_dfe_buffered_batches",
        "arrowcol_delta_pending_commits",
    ] {
        idle &= metric_value(metrics, name)? == 0;
    }
    Ok(idle)
}

fn metric_value(metrics: &str, name: &str) -> Result<u64> {
    Ok(metrics
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .with_context(|| format!("missing publication metric {name}"))?
        .parse::<u64>()?)
}

/// How long Quiver replay counters must stay unchanged before durable Dataflow output is frozen.
const REPLAY_QUIET_PERIOD: Duration = Duration::from_secs(5);

/// Batches replayed from a predecessor's Quiver WAL bypass the in-memory publication gauges, so a
/// durable run additionally waits until replay activity stops before snapshots are captured.
#[derive(Default)]
struct ReplayQuiescence {
    last: Option<Vec<u64>>,
    quiet_since: Option<Instant>,
}

impl ReplayQuiescence {
    fn observe(&mut self, idle: bool, counters: Vec<u64>, now: Instant) -> bool {
        let unchanged = self.last.as_ref() == Some(&counters);
        self.last = Some(counters);
        if !idle || !unchanged {
            self.quiet_since = None;
            return false;
        }
        now.duration_since(*self.quiet_since.get_or_insert(now)) >= REPLAY_QUIET_PERIOD
    }
}

fn captured_inventory(snapshot: &SnapshotSummary) -> SnapshotInventory {
    SnapshotInventory {
        tables: snapshot
            .tables
            .iter()
            .map(|(name, table)| {
                (
                    name.clone(),
                    TableInventory {
                        schema: table.schema.clone(),
                        version: table.version,
                        files: table.files.iter().map(|file| file.path.clone()).collect(),
                    },
                )
            })
            .collect(),
    }
}

fn payload_predicate(schema: &Value, sample: &Value) -> Result<String> {
    columns(schema)?
        .iter()
        .map(|(name, kind)| {
            let column = identifier(name)?;
            let value = sample.get(name).with_context(|| format!("reference value missing for {name}"))?;
            if value.is_null() {
                return Ok(if kind == "string" { format!("isempty({column})") } else { format!("isnull({column})") });
            }
            let expected = match kind.as_str() {
                "string" => literal(value.as_str().context("reference string type mismatch")?)?,
                "datetime" => format!("todatetime({})", literal(value.as_str().context("reference timestamp type mismatch")?)?),
                "long" | "int" => {
                    let number = if let Some(text) = value.as_str() { text.parse::<i64>()? } else { value.as_i64().context("reference integer type mismatch")? };
                    format!("{kind}({number})")
                }
                "decimal" => {
                    let number = if let Some(text) = value.as_str() { text.parse::<i128>()? } else {
                        value.as_i64().map(i128::from).or_else(|| value.as_u64().map(i128::from))
                            .context("reference decimal integer type mismatch")?
                    };
                    format!("decimal({number})")
                }
                "real" => {
                    let value = value.as_f64().context("reference real type mismatch")?;
                    ensure!(value.is_finite(), "non-finite reference real value");
                    return Ok(format!("isnotnull({column}) and abs({column} - real({value})) <= max_of(1e-9, abs(real({value})) * 1e-12)"));
                }
                "bool" => value.as_bool().context("reference boolean type mismatch")?.to_string(),
                _ => bail!("unsupported Kusto reference type"),
            };
            Ok(format!("{column} == {expected}"))
        })
        .collect::<Result<Vec<_>>>()
        .map(|values| values.join(" and "))
}

pub(crate) async fn validate(
    endpoint: &str,
    results: &Path,
    direct_root: &Path,
    dataflow_root: &Path,
    direct: &SnapshotSummary,
    dataflow: &SnapshotSummary,
) -> Result<Value> {
    let mut status: MirrorStatus = serde_json::from_slice(&fs::read(results.join(STATUS))?)?;
    let metadata: Value = serde_json::from_slice(&fs::read(results.join("run-metadata.json"))?)?;
    ensure!(
        metadata["run_id"].as_str() == Some(&status.run_id),
        "Kusto evidence belongs to another run"
    );
    let mut mirror = Mirror::new(endpoint, results, &status.run_id)?;
    let live = AtomicBool::new(false);
    mirror.client.bootstrap().await?;
    let mut evidence = BTreeMap::new();
    for (pool, root, snapshot) in [("direct", direct_root, direct), ("dataflow", dataflow_root, dataflow)] {
        ensure!(
            snapshot.tables.len() == 13,
            "{pool} Kusto validation requires all 13 tables"
        );
        let selected = captured_inventory(snapshot);
        ensure!(
            delta_inspect::inventory(root).await? == selected,
            "{pool} changed after the validation snapshot was captured"
        );
        mirror.sync(pool, &selected, &live, true).await?;
        for (table, summary) in &snapshot.tables {
            let name = table_name(pool, table)?;
            ensure!(
                summary.rows > 0 && summary.row_hashes.len() == 1,
                "expected one nonempty deterministic corpus for {name}"
            );
            ensure!(
                mirror.client.schema(&name).await? == columns(&summary.schema)?,
                "Kusto schema changed for {name}"
            );
            let sample = delta_inspect::sample_row(root, table, summary)?;
            let predicate = payload_predicate(&summary.schema, &sample)?;
            let query =
                format!("{name} | summarize Rows=count(), Invalid=countif(not(coalesce(({predicate}), false)))");
            let rows = mirror.client.execute(&query, false, "validation").await?;
            let row = rows.first().context("Kusto validation result missing")?;
            ensure!(
                row.get("Rows").and_then(Value::as_u64) == Some(summary.rows),
                "Kusto row count differs from captured {pool} snapshot for {table}: {row:?}"
            );
            ensure!(
                row.get("Invalid").and_then(Value::as_u64) == Some(0),
                "Kusto payload differs from Delta for {table}: {row:?}"
            );
            let file = summary.files.first().context("snapshot has no replay file")?;
            mirror
                .client
                .ingest(
                    &name,
                    &container_file(pool, table, &file.path)?,
                    &file_identity(&status.run_id, pool, table, &file.path),
                    "replay",
                )
                .await?;
            let replay = mirror
                .client
                .execute(&format!("{name} | count"), false, "replay")
                .await?;
            ensure!(
                replay.first().and_then(|row| row.get("Count")).and_then(Value::as_u64) == Some(summary.rows),
                "Kusto replay changed row count for {name}"
            );
            evidence.insert(
                name,
                json!({
                    "delta_version":summary.version, "active_files":summary.active_files,
                    "expected_rows":summary.rows, "actual":row, "replay_unchanged":true
                }),
            );
        }
        mirror.sync(pool, &selected, &live, true).await?;
    }
    ensure!(
        status.live_observed && status.live_tables.len() == 26,
        "all 26 Kusto tables were not observed while generation was active"
    );
    status.state = "healthy".to_owned();
    status.error = None;
    status.tables = mirror.status.tables;
    status.files_ingested = mirror.ingested.values().map(BTreeSet::len).sum();
    write_json(&results.join(STATUS), &status)?;
    Ok(json!({"passed":true, "run_id":status.run_id, "live_observed":true, "tables":evidence}))
}

pub(crate) fn validation_failure(results: &Path, error: &anyhow::Error) -> Result<Value> {
    let report = json!({"passed":false, "error":format!("{error:#}")});
    write_json(&results.join("kusto-validation.json"), &report)?;
    Ok(report)
}

pub(crate) fn degraded(results: &Path, run_id: &str, error: &anyhow::Error) -> Result<()> {
    let path = results.join(STATUS);
    let mut status: MirrorStatus = if path.exists() {
        serde_json::from_slice(&fs::read(&path)?)?
    } else {
        MirrorStatus {
            run_id: run_id.to_owned(),
            ..MirrorStatus::default()
        }
    };
    status.state = "degraded".to_owned();
    status.error = Some(format!("{error:#}"));
    write_json(&path, &status)
}

pub(crate) fn write_validation(results: &Path, report: &Value) -> Result<()> {
    write_json(&results.join("kusto-validation.json"), report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::atomic::{AtomicU64, AtomicUsize},
        thread,
    };

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct Directory(PathBuf);

    impl Directory {
        fn new() -> Result<Self> {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let root = PathBuf::from(format!("target/loadgen-kusto-tests/{}-{sequence}", std::process::id()));
            fs::create_dir_all(&root)?;
            Ok(Self(root))
        }
    }

    impl Drop for Directory {
        fn drop(&mut self) {
            if self.0.exists()
                && let Err(error) = fs::remove_dir_all(&self.0)
            {
                panic!("remove test directory: {error}");
            }
        }
    }

    fn response(names: &[&str], rows: Vec<Value>) -> Value {
        json!({"Tables":[{
            "Columns":names.iter().map(|name|json!({"ColumnName":name})).collect::<Vec<_>>(),
            "Rows":rows
        }]})
    }

    struct Server {
        endpoint: String,
        stop: Arc<AtomicBool>,
        task: Option<thread::JoinHandle<Result<()>>>,
    }

    impl Server {
        fn start(mut handler: impl FnMut(Value) -> Result<Option<Value>> + Send + 'static) -> Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let endpoint = format!("http://{}", listener.local_addr()?);
            let stop = Arc::new(AtomicBool::new(false));
            let stopped = Arc::clone(&stop);
            let task = thread::spawn(move || -> Result<()> {
                while !stopped.load(Ordering::Acquire) {
                    let (mut stream, _) = listener.accept()?;
                    if stopped.load(Ordering::Acquire) {
                        break;
                    }
                    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
                    let mut header = Vec::new();
                    while !header.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        stream.read_exact(&mut byte)?;
                        header.push(byte[0]);
                    }
                    let header = String::from_utf8(header)?;
                    let length = header
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(str::to_owned)
                        })
                        .context("content length missing")?
                        .trim()
                        .parse::<usize>()?;
                    let mut bytes = vec![0; length];
                    stream.read_exact(&mut bytes)?;
                    if let Some(body) = handler(serde_json::from_slice(&bytes)?)? {
                        let body = body.to_string();
                        write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )?;
                    }
                }
                Ok(())
            });
            Ok(Self {
                endpoint,
                stop,
                task: Some(task),
            })
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _connection = TcpStream::connect(self.endpoint.trim_start_matches("http://"));
            if let Some(task) = self.task.take() {
                match task.join() {
                    Ok(Ok(())) => {}
                    error => panic!("test server failed: {error:?}"),
                }
            }
        }
    }

    #[test]
    fn response_parser_rejects_http_success_shaped_query_errors() -> Result<()> {
        assert!(decode_response(&json!({"error":{"code":"BadRequest"}})).is_err());
        assert!(decode_response(&json!({})).is_err());
        assert!(
            decode_response(&json!([
                {"FrameType":"DataTable","TableKind":"PrimaryResult","Columns":[{"ColumnName":"Rows"}],"Rows":[[7]]},
                {"FrameType":"DataSetCompletion","HasErrors":true,"OneApiErrors":[{"code":"PartialQueryFailure"}]}
            ]))
            .is_err()
        );
        assert!(decode_response(&response(&["Severity"], vec![json!([2])])).is_err());
        assert!(decode_response(&response(&["Rows"], vec![json!([1, 2])])).is_err());
        let parsed = decode_response(&json!([
            {"FrameType":"DataTable","TableKind":"PrimaryResult","Columns":[{"ColumnName":"Rows"}],"Rows":[[9007199254740993_u64]]},
            {"FrameType":"DataSetCompletion","HasErrors":false}
        ]))?;
        assert_eq!(parsed[0]["Rows"].as_u64(), Some(9_007_199_254_740_993));
        Ok(())
    }

    #[test]
    fn schemas_and_predicates_preserve_typed_and_nanosecond_values() -> Result<()> {
        let schema = json!({"fields":[
            {"name":"flag","type":"boolean"},{"name":"small","type":"short"},
            {"name":"count","type":"long"},{"name":"value","type":"double"},
            {"name":"time","type":"timestamp"},{"name":"time__unix_nanos","type":"long"},
            {"name":"text","type":"string"},{"name":"unsigned_count","type":"decimal(20,0)"}
        ]});
        assert_eq!(
            columns(&schema)?
                .iter()
                .map(|(_, kind)| kind.as_str())
                .collect::<Vec<_>>(),
            ["bool", "int", "long", "real", "datetime", "long", "string", "decimal"]
        );
        let predicate = payload_predicate(
            &schema,
            &json!({
                "flag":true,"small":7,"count":"9007199254740993","value":0.5,
                "time":"1970-01-01 00:00:00 +00:00","time__unix_nanos":-1,"text":"a\"b\\c",
                "unsigned_count":"18446744073709551615"
            }),
        )?;
        assert!(predicate.contains("long(9007199254740993)"));
        assert!(predicate.contains("decimal(18446744073709551615)"));
        assert!(predicate.contains("long(-1)"));
        assert!(predicate.contains("todatetime("));
        assert!(predicate.contains("a\\\"b\\\\c"));
        assert!(columns(&json!({"fields":[{"name":"bad","type":"binary"}]})).is_err());
        assert!(identifier("x'); .drop database loadtest").is_err());
        assert!(table_name("direct", "unexpected.sqlserver.sqlserver_wait_stats").is_err());
        assert!(container_file("direct", "telemetry.sqlserver.sqlserver_wait_stats", "../outside").is_err());
        assert!(container_file("direct", "telemetry.sqlserver.sqlserver_wait_stats", "/outside").is_err());
        assert_ne!(
            file_identity("a", "direct", "t", "f"),
            file_identity("b", "direct", "t", "f")
        );
        Ok(())
    }

    #[test]
    fn publication_barrier_requires_all_observable_work_to_finish() -> Result<()> {
        let idle =
            "arrowcol_flight_active_streams 0\narrowcol_dfe_buffered_batches 0\narrowcol_delta_pending_commits 0\n";
        assert!(publication_idle(idle)?);
        assert!(!publication_idle(
            &idle.replace("buffered_batches 0", "buffered_batches 1")
        )?);
        assert!(!publication_idle(
            &idle.replace("pending_commits 0", "pending_commits 1")
        )?);
        assert!(!publication_idle(
            &idle.replace("active_streams 0", "active_streams 1")
        )?);
        assert!(publication_idle("").is_err());
        Ok(())
    }

    #[test]
    fn durable_settle_waits_for_quiver_replay_to_stop() {
        let start = Instant::now();
        let at = |seconds| start + Duration::from_secs(seconds);
        let mut replay = ReplayQuiescence::default();
        assert!(
            !replay.observe(true, vec![1, 0], at(0)),
            "first observation has no baseline"
        );
        assert!(!replay.observe(true, vec![1, 0], at(1)));
        assert!(
            !replay.observe(true, vec![2, 0], at(5)),
            "a replay restarts the quiet period"
        );
        assert!(!replay.observe(true, vec![2, 0], at(6)));
        assert!(
            !replay.observe(false, vec![2, 0], at(12)),
            "in-memory work restarts the quiet period"
        );
        assert!(!replay.observe(true, vec![2, 0], at(13)));
        assert!(!replay.observe(true, vec![2, 0], at(17)));
        assert!(replay.observe(true, vec![2, 0], at(18)));
    }

    #[test]
    fn extent_tags_support_the_emulators_newline_separated_strings() -> Result<()> {
        let expected = BTreeSet::from(["one".to_owned(), "two".to_owned()]);
        assert_eq!(
            ingestion_tags(&json!("ingest-by:loadtest:one\r\ningest-by:loadtest:two"))?,
            expected
        );
        assert_eq!(
            ingestion_tags(&json!("[\"ingest-by:loadtest:one\",\"ingest-by:loadtest:two\"]"))?,
            expected
        );
        assert!(ingestion_tags(&json!("")).is_ok_and(|tags| tags.is_empty()));
        assert!(ingestion_tags(&json!("[invalid]")).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn lost_ingestion_response_is_reconciled_without_resending_and_removals_fail() -> Result<()> {
        let directory = Directory::new()?;
        let run_id = "run-test";
        let table = "telemetry.sqlserver.sqlserver_wait_stats";
        let file = "part.parquet";
        let identity = file_identity(run_id, "direct", table, file);
        let calls = Arc::new(AtomicUsize::new(0));
        let requests = Arc::clone(&calls);
        let server = Server::start(move |request| {
            let command = request["csl"].as_str().context("command missing")?;
            if command.contains("getschema") {
                return Ok(Some(response(
                    &["ColumnName", "ColumnType"],
                    vec![json!(["id", "long"])],
                )));
            }
            if command.starts_with(".ingest into") {
                requests.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            if command.contains("extents") {
                let ingested = requests.load(Ordering::Relaxed) != 0;
                return Ok(Some(response(
                    &["Tags"],
                    if ingested {
                        vec![json!([json!([format!("ingest-by:loadtest:{identity}")]).to_string()])]
                    } else {
                        vec![]
                    },
                )));
            }
            Ok(Some(response(&[], vec![])))
        })?;
        let mut mirror = Mirror::new(&server.endpoint, &directory.0, run_id)?;
        let snapshot = SnapshotInventory {
            tables: BTreeMap::from([(
                table.to_owned(),
                TableInventory {
                    schema: json!({"fields":[{"name":"id","type":"long"}]}),
                    version: 1,
                    files: vec![file.to_owned()],
                },
            )]),
        };
        let live = AtomicBool::new(false);
        assert!(mirror.sync("direct", &snapshot, &live, true).await.is_err());
        mirror.sync("direct", &snapshot, &live, true).await?;
        mirror.sync("direct", &snapshot, &live, true).await?;
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(mirror.ingested.values().map(BTreeSet::len).sum::<usize>(), 1);
        let mut removed = snapshot.clone();
        removed.tables.get_mut(table).context("table")?.files.clear();
        assert!(mirror.sync("direct", &removed, &live, true).await.is_err());
        let mut changed = snapshot;
        changed.tables.get_mut(table).context("table")?.schema["fields"][0]["type"] = json!("string");
        assert!(mirror.sync("direct", &changed, &live, true).await.is_err());
        Ok(())
    }

    #[test]
    fn independent_checkpoint_rejects_non_append_changes_and_name_collisions() -> Result<()> {
        let directory = Directory::new()?;
        let arguments = MirrorDeltaArgs {
            delta_root: directory.0.clone(),
            tables: Vec::new(),
            container_root: "/delta/arrowcol".into(),
            endpoint: "http://127.0.0.1:8080".to_owned(),
            database: "arrowcol_demo".to_owned(),
            table_prefix: "demo".to_owned(),
            state_dir: directory.0.join("state"),
            poll_interval: "1s".parse()?,
            once: true,
        };
        let layout = StandaloneLayout::new(&arguments)?;
        assert_eq!(
            layout.container_file("catalog.public.events", "part.parquet")?,
            "/delta/arrowcol/catalog/public/events/part.parquet"
        );
        assert!(layout.container_file("catalog.public.events", "../outside").is_err());
        assert!(layout.table_name("catalog.public.bad/name").is_err());
        let table = TableInventory {
            schema: json!({"fields":[{"name":"id","type":"long"}]}),
            version: 1,
            files: vec!["part.parquet".to_owned()],
        };
        let snapshot = SnapshotInventory {
            tables: BTreeMap::from([("catalog.public.events".to_owned(), table.clone())]),
        };
        let mut checkpoint = BridgeCheckpoint {
            layout,
            observed: snapshot.clone(),
        };
        let mut appended = snapshot.clone();
        appended
            .tables
            .get_mut("catalog.public.events")
            .context("table")?
            .files
            .push("next.parquet".to_owned());
        checkpoint.observe(appended)?;
        // Persisted observations must still reject removals after a process restart.
        let mut restored: BridgeCheckpoint = serde_json::from_slice(&serde_json::to_vec(&checkpoint)?)?;
        assert!(restored.observe(snapshot.clone()).is_err());
        let mut changed = checkpoint.observed.clone();
        changed.tables.get_mut("catalog.public.events").context("table")?.schema = json!({"fields":[]});
        assert!(restored.observe(changed).is_err());
        let mut rollback = checkpoint.observed.clone();
        rollback
            .tables
            .get_mut("catalog.public.events")
            .context("table")?
            .version = 0;
        assert!(restored.observe(rollback).is_err());
        assert!(
            restored
                .observe(SnapshotInventory {
                    tables: BTreeMap::new()
                })
                .is_err()
        );
        assert!(
            restored
                .observe(SnapshotInventory {
                    tables: BTreeMap::from([
                        ("a_b.c.events".to_owned(), table.clone()),
                        ("a.b_c.events".to_owned(), table),
                    ]),
                })
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn independent_bridge_reads_committed_delta_and_resumes_without_reingestion() -> Result<()> {
        use arrow::{
            array::{Int64Array, RecordBatch},
            datatypes::{DataType, Field, Schema},
        };
        use arrow_deltalake::delta::DeltaTableWriter;

        let directory = Directory::new()?;
        let delta_root = directory.0.join("delta");
        let table_root = delta_root.join("catalog/public/events");
        let mut writer = DeltaTableWriter::open(&table_root)?;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )?;
        writer.write(&batch).await?;
        writer.flush().await?;
        fs::write(table_root.join("orphan.parquet"), b"not a committed file")?;
        let unrelated_log = delta_root.join("catalog/public/unselected/_delta_log");
        fs::create_dir_all(&unrelated_log)?;
        fs::write(
            unrelated_log.join("00000000000000000000.json"),
            b"invalid unrelated Delta log",
        )?;
        assert!(delta_inspect::inventory(&delta_root).await.is_err());
        let calls = Arc::new(AtomicUsize::new(0));
        let requests = Arc::clone(&calls);
        let mut tags = BTreeSet::new();
        let server = Server::start(move |request| {
            let command = request["csl"].as_str().context("command missing")?;
            if command == ".show databases" {
                return Ok(Some(response(&["DatabaseName"], vec![json!(["arrowcol_demo"])])));
            }
            ensure!(request["db"] == "arrowcol_demo", "wrong destination database");
            if command.contains("getschema") {
                return Ok(Some(response(
                    &["ColumnName", "ColumnType"],
                    vec![json!(["id", "long"])],
                )));
            }
            if command.starts_with(".ingest into") {
                ensure!(
                    command.contains("['demo_catalog_public_events']"),
                    "wrong destination table"
                );
                ensure!(
                    command.contains("/delta/arrowcol/catalog/public/events/"),
                    "wrong source path"
                );
                ensure!(!command.contains("orphan"), "uncommitted file ingested");
                let tag = command
                    .split("ingest-by:loadtest:")
                    .nth(1)
                    .context("ingestion tag missing")?;
                tags.insert(format!(
                    "ingest-by:loadtest:{}",
                    tag.chars().take(64).collect::<String>()
                ));
                requests.fetch_add(1, Ordering::Relaxed);
            }
            if command.contains("extents") {
                return Ok(Some(response(&["Tags"], vec![json!([json!(tags).to_string()])])));
            }
            Ok(Some(response(&[], vec![])))
        })?;
        let arguments = MirrorDeltaArgs {
            delta_root,
            tables: vec!["catalog.public.events".to_owned()],
            container_root: "/delta/arrowcol".into(),
            endpoint: server.endpoint.clone(),
            database: "arrowcol_demo".to_owned(),
            table_prefix: "demo".to_owned(),
            state_dir: directory.0.join("state"),
            poll_interval: "1s".parse()?,
            once: true,
        };
        mirror_delta(arguments.clone()).await?;
        mirror_delta(arguments.clone()).await?;
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        writer.write(&batch).await?;
        writer.flush().await?;
        mirror_delta(arguments.clone()).await?;
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        let status: MirrorStatus = serde_json::from_slice(&fs::read(arguments.state_dir.join(STATUS))?)?;
        assert_eq!(status.state, "healthy");
        assert_eq!(status.files_ingested, 2);
        let mut incompatible = arguments;
        incompatible.table_prefix = "other".to_owned();
        assert!(mirror_delta(incompatible).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn independent_one_shot_reports_empty_and_unavailable_sources() -> Result<()> {
        let directory = Directory::new()?;
        let root = directory.0.join("delta");
        fs::create_dir(&root)?;
        let arguments = MirrorDeltaArgs {
            delta_root: root,
            tables: Vec::new(),
            container_root: "/delta/arrowcol".into(),
            endpoint: "http://127.0.0.1:1".to_owned(),
            database: "arrowcol_demo".to_owned(),
            table_prefix: "demo".to_owned(),
            state_dir: directory.0.join("state"),
            poll_interval: "1s".parse()?,
            once: true,
        };
        assert!(mirror_delta(arguments.clone()).await.is_err());
        let status: MirrorStatus = serde_json::from_slice(&fs::read(arguments.state_dir.join(STATUS))?)?;
        assert_eq!(status.state, "degraded");
        assert!(
            status
                .error
                .context("error missing")?
                .contains("no committed Delta tables")
        );
        let mut invalid = arguments;
        invalid.container_root = "relative".into();
        assert!(mirror_delta(invalid).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_emulator_is_recorded_as_degraded_without_a_load_cancellation() -> Result<()> {
        use clap::Parser;
        let directory = Directory::new()?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        drop(listener);
        let cli = crate::cli::Cli::try_parse_from([
            "arrowcol-loadgen",
            "run",
            "--kusto-endpoint",
            &endpoint,
            "--results",
            directory.0.to_str().context("directory")?,
            "--drain-timeout",
            "1s",
        ])?;
        let crate::cli::Command::Run(arguments) = cli.command else {
            bail!("run arguments")
        };
        let sync = LiveSync::start(&arguments, "degraded-run").context("configured synchronization")?;
        assert!(sync.stop().await.is_err());
        let status: MirrorStatus = serde_json::from_slice(&fs::read(directory.0.join(STATUS))?)?;
        assert_eq!(status.state, "degraded");
        assert!(status.error.is_some());
        assert!(!status.live_observed);
        assert!(KustoClient::new("http://user:password@localhost", &directory.0).is_err());
        Ok(())
    }
}
