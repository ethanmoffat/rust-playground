import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  parseBenchmarkOptions,
  parseDuration,
  shouldRunIndependentValidation,
  shouldStartRuntimeObservability,
} from '../src/cli.js';

test('duration parsing accepts documented whole-number units', () => {
  assert.equal(parseDuration('30', 'duration', false), 30);
  assert.equal(parseDuration('30s', 'duration', false), 30);
  assert.equal(parseDuration('2m', 'duration', false), 120);
  assert.equal(parseDuration('1H', 'duration', false), 3600);
  assert.throws(() => parseDuration('1.5m', 'duration', false), /whole-number/u);
  assert.throws(() => parseDuration('0', 'duration', false), /range/u);
  assert.equal(parseDuration('0', 'duration', true), 0);
});

test('CLI overrides environment and continuous forces paired semantics', () => {
  const options = parseBenchmarkOptions([
    '--run-id=live',
    '--warmup', '2m',
    '--measure=10s',
    '--continuous',
    '--keep-stack',
    '--lanes', '8',
    '--batch-rows', '64',
  ], {
    LOADTEST_RUN_ID: 'environment',
    LOADTEST_WARMUP: '1s',
    LOADTEST_SCENARIOS: 'direct,dataflow',
  }, new Date('2026-10-11T00:00:00Z'));
  assert.equal(options.runId, 'live');
  assert.equal(options.warmupSeconds, 120);
  assert.equal(options.measureSeconds, 10);
  assert.equal(options.continuous, true);
  assert.equal(options.keepStack, true);
  assert.deepEqual(options.scenarios, ['paired']);
  assert.equal(options.lanes, 8);
  assert.equal(options.batchRows, 64);
});

test('environment overrides defaults and malformed options fail closed', () => {
  const options = parseBenchmarkOptions([], {
    LOADTEST_RUN_ID: 'env-run',
    LOADTEST_WARMUP: '0',
    LOADTEST_MEASURE: '1m',
    LOADTEST_CAPTURE_SETTLE: '0',
    LOADTEST_DRAIN_TIMEOUT: '10s',
    LOADTEST_REQUEST_TIMEOUT: '5s',
    LOADTEST_LANES: '2',
    LOADTEST_BATCH_ROWS: '128',
    LOADTEST_SEED: '9',
    LOADTEST_SCENARIOS: 'direct,paired',
  });

  assert.equal(options.runId, 'env-run');
  assert.deepEqual(options.scenarios, ['direct', 'paired']);
  assert.throws(() => parseBenchmarkOptions(['--continuous=true'], {}), /does not take/u);
  assert.throws(() => parseBenchmarkOptions(['--run-id', '../escape'], {}), /run-id/u);
  assert.throws(() => parseBenchmarkOptions(['--unknown'], {}), /Unknown/u);
});

test('continuous UI runs defer the unbounded historical Delta scan', () => {
  assert.equal(shouldRunIndependentValidation({ continuous: true }), false);
  assert.equal(shouldRunIndependentValidation({ continuous: false }), true);
});

test('CI integration omits the heavy runtime observability stack unless explicitly requested', () => {
  assert.equal(shouldStartRuntimeObservability({ CI_INTEGRATION: 'true' }), false);
  assert.equal(shouldStartRuntimeObservability({ CI_INTEGRATION: 'false' }), true);
  assert.equal(shouldStartRuntimeObservability({ CI_INTEGRATION: 'true', LOADTEST_OBSERVABILITY: 'true' }), true);
  assert.equal(shouldStartRuntimeObservability({ LOADTEST_OBSERVABILITY: 'false' }), false);
  assert.throws(
    () => shouldStartRuntimeObservability({ LOADTEST_OBSERVABILITY: 'sometimes' }),
    /must be true or false/u,
  );
});
