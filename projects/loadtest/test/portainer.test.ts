import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import { portainerAccess } from '../src/main.js';

function passwordFile(content: string): string {
  const file = path.join(mkdtempSync(path.join(tmpdir(), 'arrowcol-portainer-')), 'sqlserver-sa-password.txt');
  writeFileSync(file, content);
  return file;
}

test('portainer access prints URL, port, username and password', () => {
  const line = portainerAccess('9443', passwordFile('Aa1!secret\n'));
  assert.match(line, /https:\/\/localhost:9443 /u);
  assert.match(line, /port 9443,/u);
  assert.match(line, /username admin,/u);
  assert.match(line, /password Aa1!secret;/u);
});

test('portainer password matches the seeded value with or without a trailing newline', () => {
  const fake = 'fixture-value';
  assert.match(portainerAccess('9444', passwordFile(fake)), new RegExp(`localhost:9444 .*password ${fake};`, 'u'));
  assert.doesNotMatch(portainerAccess('9443', passwordFile('Aa1!secret\n')), /\n/u);
});
