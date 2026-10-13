import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import { prepareBenchmarkDirectory } from '../src/main.js';

function staleRun(): { root: string; directory: string } {
  const root = mkdtempSync(path.join(tmpdir(), 'arrowcol-run-directory-'));
  const directory = path.join(root, 'latest');
  mkdirSync(directory);
  writeFileSync(path.join(directory, 'result.json'), '{"old":true}');
  return { root, directory };
}

test('new and empty directories are reused', () => {
  const root = mkdtempSync(path.join(tmpdir(), 'arrowcol-run-directory-'));
  assert.equal(prepareBenchmarkDirectory(root, 'latest', () => false).archived, undefined);
  mkdirSync(path.join(root, 'latest'));
  assert.equal(prepareBenchmarkDirectory(root, 'latest', () => false).archived, undefined);
});

test('stale output is archived when no stack exists', () => {
  const { root, directory } = staleRun();
  const result = prepareBenchmarkDirectory(root, 'latest', () => false, new Date('2026-10-12T18:00:00Z'));
  assert.equal(result.directory, directory);
  assert.equal(result.archived, path.join(root, 'archive/latest-20261012T180000Z'));
  assert.equal(readFileSync(path.join(result.archived, 'result.json'), 'utf8'), '{"old":true}');
});

test('a running stack blocks replacement', () => {
  const { root } = staleRun();
  assert.throws(() => prepareBenchmarkDirectory(root, 'latest', () => true), /still running/u);
});
