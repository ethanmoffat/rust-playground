import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const workspace = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const state = path.join(workspace, 'projects/loadtest/.temp/compose-check');
for (const directory of ['direct', 'dataflow', 'results', 'router-state']) {
  mkdirSync(path.join(state, directory), { recursive: true });
}
const args = [
  'compose',
  '--project-name', 'loadtest-config-check',
  '--file', path.join(workspace, 'projects/loadtest/compose.yaml'),
];
const overlay = path.join(workspace, 'projects/loadtest/compose.observability.yaml');
if (existsSync(overlay)) args.push('--file', overlay);
args.push('--profile', 'benchmark', 'config', '--quiet');
const result = spawnSync('docker', args, {
  cwd: workspace,
  env: {
    ...process.env,
    LOADTEST_UID: String(process.getuid?.() ?? 10001),
    LOADTEST_GID: String(process.getgid?.() ?? 10001),
    LOADTEST_RUN_ID: 'config-check',
    ONELAKE_HOST_PATH: state,
    LOADTEST_SCRATCH_HOST_PATH: path.join(workspace, 'projects/loadtest/.temp'),
    LOADTEST_BIN_HOST_PATH: path.join(workspace, 'target/debug'),
  },
  encoding: 'utf8',
});
if (result.error || result.status !== 0) {
  throw new Error(`Compose config failed: ${result.error?.message ?? result.status}\n${result.stdout}\n${result.stderr}`);
}
