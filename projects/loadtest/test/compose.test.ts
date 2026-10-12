import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import YAML from 'yaml';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const compose = YAML.parse(readFileSync(path.join(root, 'projects/loadtest/compose.yaml'), 'utf8')) as {
  services: Record<string, Record<string, unknown>>;
};
const observability = YAML.parse(
  readFileSync(path.join(root, 'projects/loadtest/compose.observability.yaml'), 'utf8'),
) as { services: Record<string, { networks?: { default?: { ipv4_address?: string } } }> };

test('benchmark Compose declares three collectors per pool plus driver and validator', () => {
  const names = Object.keys(compose.services);
  for (const service of [
    'sqlserver', 'hive-metastore', 'router',
    'direct-1', 'direct-2', 'direct-3',
    'dataflow-1', 'dataflow-2', 'dataflow-3',
    'load-driver', 'validator',
  ]) {
    assert(names.includes(service), `missing ${service}`);
    assert.deepEqual(compose.services[service].profiles, ['benchmark']);
  }
});

test('direct and dataflow pools use shared but separate Delta roots', () => {
  const volumeSource = (service: string): string => {
    const volumes = compose.services[service].volumes as Array<{ source?: string; target?: string }>;
    const delta = volumes.find((volume) => volume.target?.startsWith('/delta/'));
    assert(delta?.source);
    return delta.source;
  };
  const direct = volumeSource('direct-1');
  const dataflow = volumeSource('dataflow-1');
  assert.equal(compose.services['direct-2'].extends, 'direct-1');
  assert.equal(compose.services['direct-3'].extends, 'direct-1');
  assert.equal(compose.services['dataflow-2'].extends, 'dataflow-1');
  assert.equal(compose.services['dataflow-3'].extends, 'dataflow-1');
  assert.notEqual(direct, dataflow);
});

test('existing OTEL demo services remain outside the benchmark profile', () => {
  assert.equal(compose.services['otelcol-rust'].profiles, undefined);
  assert.equal(compose.services['hello-world'].profiles, undefined);
  const driverEnvironment = compose.services['load-driver'].environment as Record<string, string>;
  assert.match(driverEnvironment.LOADTEST_ROUTER_ENDPOINT, /LOADTEST_ROUTER_IP.*50050/u);
  assert.equal(driverEnvironment.LOADTEST_DIRECT_ROOT, '/delta/direct');
  assert.equal(driverEnvironment.LOADTEST_DATAFLOW_ROOT, '/delta/dataflow');
});

test('observability overlay retains its fixed addresses on the shared benchmark subnet', () => {
  const services = [
    'prometheus', 'grafana', 'loki', 'tempo', 'alloy',
    'cadvisor', 'sql-exporter', 'json-exporter', 'blackbox-exporter',
  ];
  assert.deepEqual(
    services.map((service) => observability.services[service]?.networks?.default?.ipv4_address),
    services.map((_, index) => `10.89.0.${100 + index}`),
  );
});
