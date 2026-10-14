import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import path from 'node:path';
import { test } from 'node:test';

import { parseCounters, parseMetrics, parseResources, parseStatus, runResourceSampler, startResourceSampler } from '../src/resource-sampler.js';

const status = 'Pid:\t1\nVmRSS:\t2048 kB\nRssAnon:\t1024 kB\nThreads:\t7\n';
const resources = `\n__status__\n${status}\n__current__\n4096\n\n__stat__\nanon 2048\ninactive_file 1024\n\n__cpu__\nusage_usec 9000\nuser_usec 7000\nsystem_usec 2000\n`;

test('sampler parses PID 1 RSS, anonymous RSS, threads and cgroup working set/CPU', () => {
  assert.deepEqual(parseStatus(status), { pid: 1, rssBytes: 2097152, rssAnonBytes: 1048576, threads: 7 });
  assert.deepEqual(parseCounters('usage_usec 9\nsystem_usec 2'), { usage_usec: 9, system_usec: 2 });
  const sample = parseResources(resources) as any;
  assert.equal(sample.memory.value.workingSetBytes, 3072);
  assert.equal(sample.cpu.value.usage_usec, 9000);
  assert.equal(sample.process.value.threads, 7);
  assert.throws(() => parseStatus('Pid: 1'), /Missing/u);
  assert.equal((parseResources(resources.replace('4096', '12')) as any).memory.value.workingSetBytes, 0);
  const unavailable = parseResources(resources.replace(status, '__READ_ERROR__\n')) as any;
  assert.equal(unavailable.process.ok, false);
  assert.equal(unavailable.cpu.ok, true);
});

test('runtime counters/backlog retain numeric values without exposing metric labels or arbitrary output', () => {
  assert.deepEqual(parseMetrics([
    '# HELP ignored',
    'arrowcol_direct_received_rows_total 123',
    'arrowcol_delta_pending_commits 7',
    'arrowcol_direct_stream_outcomes_total{outcome="TOP_SECRET"} 2',
    'arrowcol_direct_stream_outcomes_total{outcome="another"} 3',
    'unrelated_password_info{password="TOP_SECRET"} 1',
  ].join('\n')), {
    arrowcol_direct_received_rows_total: 123,
    arrowcol_delta_pending_commits: 7,
    arrowcol_direct_stream_outcomes_total: 5,
  });
  assert.throws(() => parseMetrics('not metrics'), /No Arrowcol/u);
});

function fixture(t: { after: (callback: () => void) => void }): string {
  const directory = mkdtempSync(path.resolve('projects/loadtest/test/.fixture-sampler-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

test('independent sampler keeps sampling during synchronous generator work and stops only its owned process', async (t) => {
  const results = fixture(t);
  const mockDocker = path.join(results, 'docker');
  writeFileSync(mockDocker, `#!${process.execPath}
const command = process.argv[2];
if (command === 'ps') console.log('fixture-collector');
else if (command === 'inspect') console.log(JSON.stringify({id:'fixture-collector',imageId:'sha256:fixture',hostPid:123,running:true,restartCount:0}));
else if (command === 'exec') console.log(${JSON.stringify(resources)});
else process.exit(1);
`, { mode: 0o755 });
  const stop = await startResourceSampler({
    project: 'rust-playground-loadtest-fixture', results,
    directAdmin: 'http://127.0.0.1:1', dataflowAdmin: 'http://127.0.0.1:1',
  }, { ...process.env, PATH: `${results}:${process.env.PATH}` });
  try {
    const work = spawnSync(process.execPath, ['-e', 'Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 11200)']);
    assert.equal(work.status, 0);
    const lines = readFileSync(path.join(results, 'resource-samples.jsonl'), 'utf8').trim().split('\n').map((line) => JSON.parse(line));
    assert(lines.length >= 2, 'sampler must continue while the runner event loop is blocked');
    assert(Date.parse(lines[1].timestamp) - Date.parse(lines[0].timestamp) >= 9500);
    for (const collector of lines[0].collectors) {
      assert.equal(collector.resources.value.process.value.rssBytes, 2097152);
      assert.equal(collector.resources.value.memory.value.workingSetBytes, 3072);
      assert.equal(collector.admin.metrics.ok, false, 'HTTP failures must not become empty successful samples');
      assert.equal(collector.admin.readiness.ok, false);
    }
  } finally {
    await stop();
  }
  const final = JSON.parse(readFileSync(path.join(results, 'sampler-status.json'), 'utf8'));
  assert.equal(final.state, 'stopped');
  assert.throws(() => process.kill(final.pid, 0), { code: 'ESRCH' });
});

test('admin metrics and non-ready HTTP status are sampled without saving secret response content', async (t) => {
  const results = fixture(t);
  const server = createServer((request, response) => {
    if (request.url === '/metrics') response.end('arrowcol_dfe_buffered_bytes 42\n');
    else {
      response.statusCode = 503;
      response.end('{"ready":false,"password":"TOP_SECRET"}');
    }
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert(address && typeof address !== 'string');
  const endpoint = `http://127.0.0.1:${address.port}`;
  const running = runResourceSampler({
    project: 'rust-playground-loadtest-no-such-fixture', results,
    directAdmin: endpoint, dataflowAdmin: endpoint,
  });
  try {
    await assert.rejects(runResourceSampler({
      project: 'rust-playground-loadtest-no-such-fixture', results,
      directAdmin: endpoint, dataflowAdmin: endpoint,
    }), { code: 'EEXIST' });
    const deadline = Date.now() + 8000;
    while (Date.now() < deadline) {
      try {
        if (readFileSync(path.join(results, 'resource-samples.jsonl'), 'utf8').length > 0) break;
      } catch {}
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    process.emit('SIGTERM');
    await running;
    const content = readFileSync(path.join(results, 'resource-samples.jsonl'), 'utf8');
    assert(!content.includes('TOP_SECRET'));
    const sample = JSON.parse(content.trim().split('\n')[0]);
    assert.deepEqual(sample.collectors[0].admin.metrics.value, { arrowcol_dfe_buffered_bytes: 42 });
    assert.deepEqual(sample.collectors[0].admin.readiness.value, { status: 503, ready: false });
    assert.equal(sample.collectors[0].identity.ok, false);
  } finally {
    process.emit('SIGTERM');
    await running;
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});
