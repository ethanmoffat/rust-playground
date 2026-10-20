import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import YAML from 'yaml';

import { safeCollectorPolicies, safeComposeSettings, sha256 } from '../src/provenance.js';
import { buildBenchmarkRuntimeConfigs } from '../src/runtime-config.js';

test('provenance whitelists nonsensitive effective resource settings and never stores raw Compose environment', () => {
  const service = {
    cpus: 2, cpu_quota: 200000, cpu_period: 100000,
    command: ['--num-cores', '2', '--password', 'TOP_SECRET'],
    environment: { TOKIO_WORKER_THREADS: '2', ARROWCOL_MAX_MESSAGE_BYTES: '33554432', MSSQL_MEMORY_LIMIT_MB: '16384', PASSWORD: 'TOP_SECRET' },
    secrets: ['TOP_SECRET'],
  };
  const settings = safeComposeSettings({ services: {
    'direct-1': service, 'dataflow-1': service, sqlserver: service,
    kusto: {
      ...service, mem_limit: '4294967296', image: 'emulator@sha256:pinned',
      environment: { ACCEPT_EULA: 'Y', PASSWORD: 'TOP_SECRET' },
      ports: [{ host_ip: '127.0.0.1', published: '8080', target: 8080, protocol: 'tcp', password: 'TOP_SECRET' }],
      volumes: [
        { type: 'volume', source: 'TOP_SECRET', target: '/kustodata' },
        { type: 'bind', source: '/TOP_SECRET', target: '/delta/direct', read_only: true },
      ],
    },
  } });
  assert(!JSON.stringify(settings).includes('TOP_SECRET'));
  assert.equal(settings['dataflow-1'].numCores, '2');
  assert.equal(settings['direct-1'].environment.TOKIO_WORKER_THREADS, '2');
  assert.equal(settings.sqlserver.environment.MSSQL_MEMORY_LIMIT_MB, '16384');
  assert.equal(settings.kusto.memoryLimit, '4294967296');
  assert.equal(settings.kusto.image, 'emulator@sha256:pinned');
  assert.match(settings.kusto.purpose, /not a performance benchmark/u);
  assert.deepEqual(settings.kusto.environment, { ACCEPT_EULA: 'Y' });
  assert.deepEqual(settings.kusto.ports, [{ target: 8080, published: '8080', host_ip: '127.0.0.1', protocol: 'tcp' }]);
  assert.deepEqual(settings.kusto.mounts, [
    { type: 'volume', target: '/kustodata' }, { type: 'bind', target: '/delta/direct', read_only: true },
  ]);
  assert.notEqual(sha256('before'), sha256('after'));
  assert.equal(sha256('before').length, 64);
});

test('policy provenance reports generated 128 MiB budgets rather than unchanged production defaults', () => {
  const runtime = buildBenchmarkRuntimeConfigs(
    readFileSync(path.resolve('projects/loadtest/config/direct.yaml.tmpl'), 'utf8'),
    readFileSync(path.resolve('projects/arrowcol-dataflow-rust/config.yaml'), 'utf8'),
    { instanceKey: 'loadtest-direct-1', advertisedEndpoint: '10.89.0.21:50051', generation: 1 },
  );
  const policies = safeCollectorPolicies(YAML.parse(runtime.directYaml), YAML.parse(runtime.dataflowYaml));
  assert.equal(policies.direct.buffer.global_capacity.bytes, 134217728);
  assert.equal(policies.dataflow.batching.global_capacity.bytes, 134217728);
  assert.equal(policies.direct.buffer.per_table_capacity.bytes, 33554432);
  assert.equal(policies.dataflow.batching.max_arrow_memory_per_flush, 33554432);
  assert.equal(policies.dataflow.registration.buffered_bytes_limit, 134217728);
  assert.equal(policies.dataflow.registration.buffered_rows_limit, 4000000);
  assert.equal(policies.dataflow.registration.buffered_batches_limit, 2048);
});

test('collector policy provenance includes actual bounded flush/commit limits without auth or SQL secrets', () => {
  const direct = YAML.parse(readFileSync(path.resolve('projects/loadtest/config/direct.yaml.tmpl'), 'utf8'));
  const dataflow = YAML.parse(readFileSync(path.resolve('projects/arrowcol-dataflow-rust/config.yaml'), 'utf8'));
  direct.auth.password = 'TOP_SECRET';
  direct.sql_server.password = 'TOP_SECRET';
  dataflow.groups.arrowcol.pipelines.ingest.nodes.flight.config.catalog.password = 'TOP_SECRET';
  const policies = safeCollectorPolicies(direct, dataflow);
  assert(!JSON.stringify(policies).includes('TOP_SECRET'));
  assert.equal(policies.direct.buffer.max_arrow_memory_per_flush, 536870912);
  assert.equal(policies.dataflow.commit.max_pending_files, 256);
  assert.equal(policies.direct.receipts.receipt_lease_duration_ms, 20000);
  assert.deepEqual(policies.direct.sql_pool, {
    receipts_max_size: 16, routing_max_size: 4, catalog_max_size: 4, acquire_timeout_ms: 5000,
  });
});
