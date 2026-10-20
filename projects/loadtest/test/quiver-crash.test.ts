import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import {
  QUIVER_RECOVERY_FILE, quiverRecoveryFailures, quiverRecoveryMetrics, runQuiverCrash, selectQuiverMetrics,
} from '../src/quiver-crash.js';

function exposition(values: Record<string, number>): string {
  return Object.entries(values).map(([name, value]) => `# TYPE ${name} counter\n${name} ${value}`).join('\n');
}

interface FakeCollectors {
  phase: 'running' | 'killed' | 'restarted';
  events: string[];
}

function fakeScrape(state: FakeCollectors, replay: boolean, adopt = true): (endpoint: string) => Promise<string> {
  return async (endpoint) => {
    if (state.phase === 'killed') throw new Error('connection refused');
    const restarted = state.phase === 'restarted';
    if (endpoint.includes('direct')) {
      return exposition({
        [quiverRecoveryMetrics.direct.persisted]: restarted ? 2 : 40,
        [quiverRecoveryMetrics.direct.backlog]: restarted ? 0 : 6,
        [quiverRecoveryMetrics.direct.recovered]: restarted && replay ? 6 : 0,
        [quiverRecoveryMetrics.direct.adoptedReceipts]: restarted && replay && adopt ? 6 : 0,
      });
    }
    return exposition({
      [quiverRecoveryMetrics.dataflow.persisted]: restarted ? 1 : 30,
      [quiverRecoveryMetrics.dataflow.backlog]: restarted ? 0 : 4,
      [quiverRecoveryMetrics.dataflow.recovered]: restarted && replay ? 4 : 0,
    });
  };
}

function crashOptions(
  results: string, state: FakeCollectors, replay: boolean, loadEnded = new AbortController().signal, adopt = true,
) {
  return {
    crashAfterSeconds: 0.01,
    directAdmin: 'http://direct',
    dataflowAdmin: 'http://dataflow',
    results,
    kill: async () => { state.phase = 'killed'; state.events.push('kill'); },
    restart: async () => { state.phase = 'restarted'; state.events.push('restart'); },
    log: (message: string) => { state.events.push(`log:${message}`); },
    loadEnded,
    checkRunning: () => undefined,
    scrape: fakeScrape(state, replay, adopt),
    pollIntervalMs: 1,
    startTimeoutMs: 1_000,
    recoveryTimeoutMs: 50,
    adoptionTimeoutMs: 50,
  };
}

test('crash demo kills persisting collectors, restarts them, and records Quiver replay', async () => {
  const results = mkdtempSync(path.join(tmpdir(), 'quiver-crash-'));
  try {
    const state: FakeCollectors = { phase: 'running', events: [] };
    const evidence = await runQuiverCrash(crashOptions(results, state, true));
    assert.deepEqual(state.events.filter((event) => !event.startsWith('log:')), ['kill', 'restart']);
    assert.equal(evidence.passed, true);
    assert.deepEqual(evidence.recovered, { direct: true, dataflow: true });
    assert.equal(evidence.beforeCrash.direct.backlog, 6, 'records batches that were on disk but not yet in Delta');
    assert.equal(evidence.afterRecovery.direct.adoptedReceipts, 6);
    const written = JSON.parse(readFileSync(path.join(results, QUIVER_RECOVERY_FILE), 'utf8'));
    assert.equal(written.passed, true);
    assert(written.recoveredAt);
  } finally {
    rmSync(results, { recursive: true, force: true });
  }
});

test('crash demo fails when a restarted collector replays nothing from disk', async () => {
  const results = mkdtempSync(path.join(tmpdir(), 'quiver-crash-'));
  try {
    const state: FakeCollectors = { phase: 'running', events: [] };
    await assert.rejects(runQuiverCrash(crashOptions(results, state, false)),
      /direct collector did not replay any Quiver batch.*dataflow collector did not replay/u);
    const written = JSON.parse(readFileSync(path.join(results, QUIVER_RECOVERY_FILE), 'utf8'));
    assert.equal(written.passed, false);
    assert.equal(written.recoveredAt, undefined);
  } finally {
    rmSync(results, { recursive: true, force: true });
  }
});

test('crash demo records replay even when no receipt needed adoption', async () => {
  const results = mkdtempSync(path.join(tmpdir(), 'quiver-crash-'));
  try {
    const state: FakeCollectors = { phase: 'running', events: [] };
    const evidence = await runQuiverCrash(crashOptions(results, state, true, new AbortController().signal, false));
    assert.equal(evidence.passed, true, 'adoption is evidence, not a requirement: every receipt may already be published');
    assert.equal(evidence.afterRecovery.direct.adoptedReceipts, 0);
    assert.equal(evidence.afterRecovery.direct.recovered, 6);
  } finally {
    rmSync(results, { recursive: true, force: true });
  }
});

test('crash demo refuses to crash after load generation has ended', async () => {
  const results = mkdtempSync(path.join(tmpdir(), 'quiver-crash-'));
  try {
    const state: FakeCollectors = { phase: 'running', events: [] };
    const ended = new AbortController();
    ended.abort();
    await assert.rejects(runQuiverCrash(crashOptions(results, state, true, ended.signal)), /ended before the Quiver crash/u);
    assert.deepEqual(state.events, []);
  } finally {
    rmSync(results, { recursive: true, force: true });
  }
});

test('recovery verdict requires replay from both collectors', () => {
  const recovered = { direct: selectQuiverMetrics('direct', { [quiverRecoveryMetrics.direct.recovered]: 1 }),
    dataflow: selectQuiverMetrics('dataflow', {}) };
  assert.deepEqual(quiverRecoveryFailures(recovered), ['dataflow collector did not replay any Quiver batch after the crash']);
});
