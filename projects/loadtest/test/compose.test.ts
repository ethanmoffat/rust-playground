import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import YAML from 'yaml';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const compose = YAML.parse(readFileSync(path.join(root, 'projects/loadtest/compose.yaml'), 'utf8')) as {
  services: Record<string, { profiles?: string[]; volumes?: Array<{ source?: string; target?: string }> }>;
};

test('stack is SQL, Hive, router, one collector per pool, LGTM and validator', () => {
  const names = Object.keys(compose.services);
  for (const service of ['sqlserver', 'hive-metastore', 'router', 'direct-1', 'dataflow-1', 'lgtm', 'cadvisor', 'validator']) {
    assert(names.includes(service), `missing ${service}`);
    assert.deepEqual(compose.services[service].profiles, ['benchmark']);
  }
  for (const gone of ['direct-2', 'dataflow-2', 'otelcol-rust', 'hello-world', 'load-driver', 'prometheus']) {
    assert(!names.includes(gone), `unexpected ${gone}`);
  }
});

test('direct and dataflow pools use separate Delta roots', () => {
  const root = (service: string): string | undefined =>
    compose.services[service].volumes?.find((volume) => volume.target?.startsWith('/delta/'))?.source;
  assert(root('direct-1'));
  assert.notEqual(root('direct-1'), root('dataflow-1'));
});
