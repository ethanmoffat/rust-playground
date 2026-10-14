import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import YAML from 'yaml';

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
