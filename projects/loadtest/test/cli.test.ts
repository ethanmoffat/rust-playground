import assert from 'node:assert/strict';
import { test } from 'node:test';

import { benchmarkProject, parseBenchmarkOptions, parseDuration } from '../src/cli.js';

test('durations accept whole-number units', () => {
  assert.equal(parseDuration('30', 'd'), 30);
  assert.equal(parseDuration('2m', 'd'), 120);
  assert.equal(parseDuration('1H', 'd'), 3600);
  assert.throws(() => parseDuration('1.5m', 'd'), /must look like/u);
});

test('explicit run IDs are exact bounded Compose and path identities', () => {
  assert.equal(parseBenchmarkOptions([]).runId, 'latest');
  assert.equal(parseBenchmarkOptions(['--run-id=soak-20261014']).runId, 'soak-20261014');
  assert.equal(benchmarkProject('a'.repeat(38)).length, 63);
  for (const value of ['../escape', '.', '..', '/root', 'UPPER', '-leading', 'a b', 'a.b', 'a/b', 'a\\b', 'a'.repeat(39), 'a\n', 'é', '']) {
    assert.throws(() => parseBenchmarkOptions([`--run-id=${value}`]), /run-id/u, value);
  }
  assert.notEqual(benchmarkProject('soak-a'), benchmarkProject('soak_a'));
});

test('defaults and overrides', () => {
  assert.equal(parseBenchmarkOptions([]).warmupSeconds, 30);
  const options = parseBenchmarkOptions(['--warmup=2m', '--concurrency', '8', '--batch-rows', '64']);
  assert.deepEqual([options.warmupSeconds, options.concurrency, options.batchRows], [120, 8, 64]);
  assert.throws(() => parseBenchmarkOptions(['--unknown', '1']), /Unknown/u);
  assert.throws(() => parseBenchmarkOptions(['--concurrency']), /requires a value/u);
});
