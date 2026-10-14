import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import YAML from 'yaml';
import { benchmarkComposeFiles, buildBenchmarkComposeOverride, buildBenchmarkRuntimeConfigs } from '../src/runtime-config.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const direct = YAML.parse(readFileSync(path.join(root, 'projects/loadtest/config/direct.yaml.tmpl'), 'utf8'));
const dfe = YAML.parse(readFileSync(path.join(root, 'projects/arrowcol-dataflow-rust/config.yaml'), 'utf8'));
const nodes = dfe.groups.arrowcol.pipelines.ingest.nodes;

test('production DFE and direct share bounded buffer, file, adaptive sizing and flush policies', () => {
  const batching = nodes.delta.config.batching;
  for (const key of [
    'global_capacity', 'per_table_capacity', 'max_arrow_memory_per_flush', 'max_rows_per_flush',
    'max_batches_per_flush', 'max_concurrent_flushes', 'adaptive_file',
  ]) {
    assert.deepEqual(batching[key], direct.buffer[key], key);
  }
  assert.equal(batching.max_age, `${direct.buffer.max_age_ms / 1000}s`);
  const commit = nodes.delta.config.commit;
  assert.equal(commit.batch_window, `${direct.delta.commit_batch_window_ms / 1000}s`);
  for (const key of ['max_files_per_commit', 'max_pending_files']) assert.equal(commit[key], direct.delta[key], key);
});

test('configured aggregate stream and decode limits match across direct and shared-permit DFE', () => {
  const flight = nodes.flight.config;
  assert.equal(flight.registration.max_concurrent_streams, direct.flight.max_concurrent_streams);
  assert.equal(flight.registration.rss_limit_bytes, direct.routing.rss_limit_bytes);
  assert.equal(flight.registration.buffered_bytes_limit, direct.buffer.global_capacity.bytes);
  assert.equal(flight.registration.buffered_rows_limit, direct.buffer.global_capacity.rows);
  assert.equal(flight.registration.buffered_batches_limit, direct.buffer.global_capacity.batches);
  for (const key of ['max_metadata_bytes', 'max_schema_bytes', 'batch_channel_capacity', 'put_result_channel_capacity', 'max_ddl_bytes']) {
    assert.equal(flight.limits[key], direct.flight[key], key);
  }
  const compose = YAML.parse(readFileSync(path.join(root, 'projects/loadtest/compose.yaml'), 'utf8'));
  assert.equal(Number(compose.services['dataflow-1'].environment.ARROWCOL_MAX_MESSAGE_BYTES), direct.flight.max_message_bytes);
  assert.equal(flight.limits.max_stream_duration, `${direct.flight.max_stream_duration_ms / 60000}m`);
  assert.equal(flight.limits.admission_timeout, `${direct.flight.admission_timeout_ms / 1000}s`);
});

test('direct retains durable lease/receipt contracts rather than downgrading to DFE admission semantics', () => {
  assert.equal(direct.routing.require_lease_headers, true);
  assert.equal(direct.auth.mode, 'router_hmac');
  assert(direct.receipts.owner_lease_ttl_ms > 0);
  assert(direct.receipts.receipt_lease_duration_ms > 0);
  assert.equal(nodes.flight.type, 'receiver:arrow_flight');
  assert.equal(nodes.delta.type, 'exporter:arrow_deltalake');
});

test('actual runtime builder applies matching process-wide 128 MiB budgets without per-core division', () => {
  const directSource = readFileSync(path.join(root, 'projects/loadtest/config/direct.yaml.tmpl'), 'utf8');
  const dataflowSource = readFileSync(path.join(root, 'projects/arrowcol-dataflow-rust/config.yaml'), 'utf8');
  const runtime = buildBenchmarkRuntimeConfigs(directSource, dataflowSource, {
    instanceKey: 'loadtest-direct-1', advertisedEndpoint: '10.89.0.21:50051', generation: 2,
  });
  const builtDirect = YAML.parse(runtime.directYaml);
  const builtDataflow = YAML.parse(runtime.dataflowYaml);
  const builtNodes = builtDataflow.groups.arrowcol.pipelines.ingest.nodes;
  const batching = builtNodes.delta.config.batching;
  assert.deepEqual(builtDirect.buffer.global_capacity, { bytes: 134217728, rows: 4000000, batches: 2048 });
  assert.deepEqual(builtDirect.buffer.per_table_capacity, { bytes: 33554432, rows: 1000000, batches: 512 });
  for (const key of ['global_capacity', 'per_table_capacity', 'max_arrow_memory_per_flush']) {
    assert.deepEqual(batching[key], builtDirect.buffer[key], key);
  }
  assert.equal(batching.max_arrow_memory_per_flush, 33554432);
  assert.equal(builtNodes.flight.config.registration.buffered_bytes_limit, 134217728);
  assert.equal(builtNodes.flight.config.registration.buffered_rows_limit, 4000000);
  assert.equal(builtNodes.flight.config.registration.buffered_batches_limit, 2048);
  for (const key of ['max_rows_per_flush', 'max_batches_per_flush', 'max_concurrent_flushes', 'adaptive_file']) {
    assert.deepEqual(batching[key], direct.buffer[key], key);
    assert.deepEqual(builtDirect.buffer[key], direct.buffer[key], key);
  }
  assert.equal(builtDirect.buffer.max_age_ms, direct.buffer.max_age_ms);
  assert.equal(batching.max_age, nodes.delta.config.batching.max_age);
  assert.deepEqual(builtDirect.delta, direct.delta);
  assert.deepEqual(builtNodes.delta.config.commit, nodes.delta.config.commit);
  assert.deepEqual(builtDirect.receipts, direct.receipts);
  assert.deepEqual(builtDirect.auth, direct.auth);
  assert.equal(builtDirect.identity.generation, 2);
  assert.equal(builtDirect.flight.max_concurrent_streams, 256);
  assert.equal(builtNodes.flight.config.registration.max_concurrent_streams, 256);
  for (const buffer of [builtDirect.buffer, batching]) {
    assert(buffer.max_rows_per_flush <= buffer.per_table_capacity.rows);
    assert(buffer.max_batches_per_flush <= buffer.per_table_capacity.batches);
    assert(buffer.max_arrow_memory_per_flush <= buffer.per_table_capacity.bytes);
    for (const key of ['bytes', 'rows', 'batches']) assert(buffer.per_table_capacity[key] <= buffer.global_capacity[key]);
  }
  assert.equal(YAML.parse(directSource).buffer.global_capacity.bytes, 2147483648);
  assert.equal(YAML.parse(dataflowSource).groups.arrowcol.pipelines.ingest.nodes.delta.config.batching.global_capacity.bytes, 2147483648);
  const retained = buildBenchmarkRuntimeConfigs(runtime.directYaml, dataflowSource);
  assert.equal(YAML.parse(retained.directYaml).identity.generation, 2, 'budget construction does not advance restart generations');
});

test('DFE factory reuses the process-wide backend registry keyed by Delta root', () => {
  const exporter = readFileSync(path.join(root, 'crates/arrowcol-dataflow-components/src/exporter.rs'), 'utf8');
  assert.match(exporter, /type BackendRegistry = Mutex<HashMap<PathBuf, Weak<DeltaBackend>>>/u);
  assert.match(exporter, /static BACKENDS: LazyLock<BackendRegistry>/u);
  assert.match(exporter, /registry\.get\(&config\.base_path\)\.and_then\(Weak::upgrade\)/u);
  assert.match(exporter, /return Ok\(Self \{ backend: existing \}\)/u);
  assert.match(exporter, /GenericDeltaExporter::shared\(config, global_metrics\(\)\)/u);
  assert.match(exporter, /capacity: Capacity::new\(config\.batching\.global_capacity\)/u);
  assert.match(exporter, /flush_slots: Arc::new\(Semaphore::new\(config\.batching\.max_concurrent_flushes\)\)/u);
});

test('runtime Compose override mounts generated DFE policy without changing CPUs or base Compose', () => {
  const directory = '/workspace-root/onelake/benchmark/fair-run';
  const override = JSON.parse(buildBenchmarkComposeOverride(directory));
  assert.deepEqual(Object.keys(override.services), ['dataflow-1']);
  assert.deepEqual(override.services['dataflow-1'].volumes, [{
    type: 'bind', source: `${directory}/dataflow-state-1/dataflow.yaml`,
    target: '/etc/arrowcol-dataflow-rust/config.yaml', read_only: true,
    bind: { create_host_path: false },
  }]);
  assert.deepEqual(benchmarkComposeFiles('compose.yaml', {}), ['--file', 'compose.yaml']);
  assert.deepEqual(benchmarkComposeFiles('compose.yaml', { LOADTEST_COMPOSE_OVERRIDE_FILE: 'runtime.json' }),
    ['--file', 'compose.yaml', '--file', 'runtime.json']);
});
