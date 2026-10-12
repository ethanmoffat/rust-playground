import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import { prepareBenchmarkDirectory } from '../src/main.js';

test('new and empty benchmark directories are reusable', () => {
  const root = mkdtempSync(path.join(tmpdir(), 'arrowcol-run-directory-'));
  const absent = prepareBenchmarkDirectory(root, 'live', true, () => false);
  assert.equal(absent.directory, path.join(root, 'live'));
  assert.equal(absent.archived, undefined);

  mkdirSync(absent.directory);
  const empty = prepareBenchmarkDirectory(root, 'live', true, () => false);
  assert.equal(empty.directory, absent.directory);
  assert.equal(empty.archived, undefined);
});

test('finite benchmark IDs remain protected from accidental overwrite', () => {
  const root = mkdtempSync(path.join(tmpdir(), 'arrowcol-run-directory-'));
  const directory = path.join(root, 'bench-1');
  mkdirSync(directory);
  writeFileSync(path.join(directory, 'result.json'), '{}');
  assert.throws(
    () => prepareBenchmarkDirectory(root, 'bench-1', false, () => false),
    /already contains data/u,
  );
});

test('continuous runs archive stale output when no matching stack exists', () => {
  const root = mkdtempSync(path.join(tmpdir(), 'arrowcol-run-directory-'));
  const directory = path.join(root, 'live');
  mkdirSync(directory);
  writeFileSync(path.join(directory, 'result.json'), '{"old":true}');

  const result = prepareBenchmarkDirectory(
    root,
    'live',
    true,
    () => false,
    new Date('2026-10-12T18:00:00Z'),
  );

  assert.equal(result.directory, directory);
  assert.equal(result.archived, path.join(root, 'archive/live-20261012T180000Z'));
  assert.equal(readFileSync(path.join(result.archived, 'result.json'), 'utf8'), '{"old":true}');
});

test('continuous runs refuse to replace an existing Compose stack', () => {
  const root = mkdtempSync(path.join(tmpdir(), 'arrowcol-run-directory-'));
  const directory = path.join(root, 'live');
  mkdirSync(directory);
  writeFileSync(path.join(directory, 'result.json'), '{}');
  assert.throws(
    () => prepareBenchmarkDirectory(root, 'live', true, () => true),
    /benchmark-down --run-id live/u,
  );
});
