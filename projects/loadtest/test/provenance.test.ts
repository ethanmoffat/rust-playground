import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import YAML from 'yaml';

import { safeCollectorPolicies, safeComposeSettings, sha256 } from '../src/provenance.js';

test('provenance whitelists nonsensitive effective resource settings and never stores raw Compose environment', () => {
  const service = {
    cpus: 2, cpu_quota: 200000, cpu_period: 100000,
    command: ['--num-cores', '2', '--password', 'TOP_SECRET'],
    environment: { TOKIO_WORKER_THREADS: '2', ARROWCOL_MAX_MESSAGE_BYTES: '33554432', MSSQL_MEMORY_LIMIT_MB: '16384', PASSWORD: 'TOP_SECRET' },
    secrets: ['TOP_SECRET'],
  };
  const settings = safeComposeSettings({ services: { 'direct-1': service, 'dataflow-1': service, sqlserver: service } });
  assert(!JSON.stringify(settings).includes('TOP_SECRET'));
  assert.equal(settings['dataflow-1'].numCores, '2');
  assert.equal(settings['direct-1'].environment.TOKIO_WORKER_THREADS, '2');
  assert.equal(settings.sqlserver.environment.MSSQL_MEMORY_LIMIT_MB, '16384');
  assert.notEqual(sha256('before'), sha256('after'));
  assert.equal(sha256('before').length, 64);
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
});
