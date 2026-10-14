import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import YAML from 'yaml';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const compose = YAML.parse(readFileSync(path.join(root, 'projects/loadtest/compose.yaml'), 'utf8')) as {
  services: Record<string, {
    profiles?: string[];
    image?: string;
    cpus?: string;
    environment?: Record<string, string>;
    command?: string[];
    ports?: string[];
    secrets?: Array<string | { source: string; target?: string }>;
    tmpfs?: string[];
    volumes?: Array<string | { source?: string; target?: string }>;
    networks?: { default?: { ipv4_address?: string } };
  }>;
};

test('stack is SQL, Hive, router, one collector per pool, LGTM, Portainer and validator', () => {
  const names = Object.keys(compose.services);
  for (const service of ['sqlserver', 'hive-metastore', 'router', 'direct-1', 'dataflow-1', 'lgtm', 'cadvisor', 'portainer', 'validator']) {
    assert(names.includes(service), `missing ${service}`);
    assert.deepEqual(compose.services[service].profiles, ['benchmark']);
  }
  for (const gone of ['direct-2', 'dataflow-2', 'otelcol-rust', 'hello-world', 'load-driver', 'prometheus']) {
    assert(!names.includes(gone), `unexpected ${gone}`);
  }
});

test('direct and dataflow pools use separate Delta roots', () => {
  const root = (service: string): string | undefined =>
    compose.services[service].volumes?.flatMap((volume) =>
      typeof volume !== 'string' && volume.target?.startsWith('/delta/') ? [volume.source] : [])[0];
  assert(root('direct-1'));
  assert.notEqual(root('direct-1'), root('dataflow-1'));
});

test('SQL Server publishes an overridable loopback port and retains its data and secret mounts', () => {
  const sqlserver = compose.services.sqlserver;
  assert.deepEqual(sqlserver.ports, ['127.0.0.1:${SQLSERVER_PORT:-14333}:1433']);
  assert(sqlserver.volumes?.includes('sqlserver-data:/var/opt/mssql'));
  assert(sqlserver.secrets?.includes('sqlserver_sa_password'));
  const devcontainer = JSON.parse(readFileSync(path.join(root, '.devcontainer/devcontainer.json'), 'utf8'));
  assert.equal(devcontainer.portsAttributes['14333'].onAutoForward, 'ignore');
});

test('SQL Server has an overridable 16 GiB memory budget for continuous benchmarks', () => {
  assert.equal(compose.services.sqlserver.environment?.MSSQL_MEMORY_LIMIT_MB,
    '${SQLSERVER_MEMORY_LIMIT_MB:-16384}');
});

test('both collectors have equal CPU budgets and explicit matching runtime thread/core counts', () => {
  assert.equal(compose.services['direct-1'].cpus, '${LOADTEST_CPUS:-2}');
  assert.equal(compose.services['dataflow-1'].cpus, compose.services['direct-1'].cpus);
  assert.equal(compose.services['direct-1'].environment?.TOKIO_WORKER_THREADS, '${LOADTEST_CPUS:-2}');
  assert.deepEqual(compose.services['dataflow-1'].command?.slice(0, 2), ['--num-cores', '${LOADTEST_CPUS:-2}']);
});

test('portainer is pinned, loopback-only, seeded from the SA secret and stateless', () => {
  const portainer = compose.services.portainer;
  assert.match(portainer.image ?? '', /^portainer\/portainer-ce:[0-9.]+@sha256:[0-9a-f]{64}$/u);
  assert(portainer.ports?.length, 'portainer must publish its UI');
  for (const port of portainer.ports) {
    assert(port.startsWith('127.0.0.1:') && port.endsWith(':9443'), `portainer port ${port} must be loopback-only`);
  }
  assert(portainer.command?.includes('--admin-password-file=/run/secrets/sqlserver_sa_password'));
  assert(portainer.secrets?.includes('sqlserver_sa_password'));
  assert(portainer.volumes?.some((volume) =>
    typeof volume !== 'string' && volume.source === '/var/run/docker.sock' && volume.target === '/var/run/docker.sock'));
  assert(!portainer.volumes?.some((volume) =>
    typeof volume === 'string' ? volume.split(':')[1] === '/data' : volume.target === '/data'),
  'admin must be re-seeded on every start');
  assert(portainer.tmpfs?.some((mount) => mount.startsWith('/data:')));
  assert(portainer.networks?.default?.ipv4_address, 'a dynamic address could take a static service IP');
});
