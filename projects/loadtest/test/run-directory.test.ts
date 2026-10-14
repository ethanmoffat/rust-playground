import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import { deltaTableLocations, prepareBenchmarkDirectory } from '../src/main.js';

test('Delta table locations show aligned absolute workspace paths for the selected run', () => {
  const directory = path.resolve('onelake/benchmark/custom-run');
  assert.equal(deltaTableLocations('onelake/benchmark/custom-run'), [
    'Delta tables on disk (workspace paths)',
    '-------------------------------------',
    `  Direct:   ${path.join(directory, 'direct')}`,
    `  Dataflow: ${path.join(directory, 'dataflow')}`,
  ].join('\n'));
});

function staleRun(root: string): { root: string; directory: string } {
  const directory = path.join(root, 'latest');
  mkdirSync(directory);
  writeFileSync(path.join(directory, 'result.json'), '{"old":true}');
  return { root, directory };
}

function fixture(t: { after: (callback: () => void) => void }): string {
  const root = mkdtempSync(path.resolve('projects/loadtest/test/.fixture-run-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return root;
}

test('new runs reserve separate directories; even empty reserved runs are never replaced', (t) => {
  const root = fixture(t);
  assert.equal(prepareBenchmarkDirectory(root, 'latest', () => false).directory, path.join(root, 'latest'));
  assert.throws(() => prepareBenchmarkDirectory(root, 'latest', () => false), /unused --run-id/u);
  assert.equal(prepareBenchmarkDirectory(root, 'soak-2', () => false).directory, path.join(root, 'soak-2'));
});

test('historical output is neither renamed nor replaced without a stack', (t) => {
  const { root, directory } = staleRun(fixture(t));
  assert.throws(() => prepareBenchmarkDirectory(root, 'latest', () => false), /unused --run-id/u);
  assert.equal(readFileSync(path.join(directory, 'result.json'), 'utf8'), '{"old":true}');
});

test('retained resources block a run even without a directory', (t) => {
  const root = fixture(t);
  assert.throws(() => prepareBenchmarkDirectory(root, 'latest', () => true), /retained SQL volume\/history/u);
});

test('path traversal and dangling directory symlinks cannot bypass reservation', (t) => {
  const root = fixture(t);
  assert.throws(() => prepareBenchmarkDirectory(root, '../escape', () => false), /run-id/u);
  symlinkSync(path.join(root, 'missing'), path.join(root, 'latest'));
  assert.throws(() => prepareBenchmarkDirectory(root, 'latest', () => false), /unused --run-id/u);
});

test('CLI rejects a retained SQL volume before bootstrap and never issues destructive Docker commands', (t) => {
  const root = fixture(t);
  const calls = path.join(root, 'docker-calls.jsonl');
  writeFileSync(path.join(root, 'docker'), `#!${process.execPath}
import { appendFileSync } from 'node:fs';
appendFileSync(${JSON.stringify(calls)}, JSON.stringify(process.argv.slice(2))+'\\n');
if (process.argv[2] === 'volume') console.log('retained-sql-volume');
`, { mode: 0o755 });
  const result = spawnSync(process.execPath, [
    ...process.execArgv, fileURLToPath(new URL('../src/main.ts', import.meta.url)),
    '--run-id', 'fixture-existing-sql-volume',
  ], { encoding: 'utf8', env: { ...process.env, PATH: `${root}:${process.env.PATH}` }, timeout: 10_000 });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /unused --run-id/u);
  const commands = readFileSync(calls, 'utf8').trim().split('\n').map((line) => JSON.parse(line) as string[]);
  assert(commands.some((args) => args[0] === 'volume' && args[1] === 'ls'));
  assert(commands.every((args) => !args.some((arg) => ['rm', 'down', 'kill', '--volumes'].includes(arg))));
  assert(!commands.some((args) => args[0] === 'compose'));
});
