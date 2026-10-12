import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import {
  chmodSync, copyFileSync, existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync,
} from 'node:fs';
import { hostname } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

import {
  benchmarkUsage,
  parseBenchmarkOptions,
  shouldRunIndependentValidation,
  shouldStartRuntimeObservability,
  type BenchmarkOptions,
} from './cli.js';
import { commitSpansCores, parseInspection, signalsReady, validateSignals, type Inspection } from './validate.js';

const workspace = realpathSync(path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..'));
const composeFile = path.join(workspace, 'projects/loadtest/compose.yaml');
const observabilityComposeFile = path.join(workspace, 'projects/loadtest/compose.observability.yaml');

function run(command: string, args: string[], env: NodeJS.ProcessEnv = process.env, timeout = 300_000): string {
  const result = spawnSync(command, args, { cwd: workspace, env, encoding: 'utf8', timeout, maxBuffer: 64 * 1024 * 1024 });
  if (result.error || result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed: ${result.error?.message ?? result.status}\n${result.stdout}\n${result.stderr}`);
  }
  return result.stdout;
}

export function mapHostPath(localPath: string, mounts: unknown): string {
  if (!Array.isArray(mounts)) {
    throw new Error('Docker did not return container mounts');
  }
  const bindings = mounts.filter((mount): mount is { Type: string; Source: string; Destination: string } =>
    typeof mount === 'object' && mount !== null &&
    mount.Type === 'bind' && typeof mount.Source === 'string' && typeof mount.Destination === 'string')
    .filter((mount) => localPath === mount.Destination || localPath.startsWith(`${mount.Destination}/`))
    .sort((left, right) => right.Destination.length - left.Destination.length);
  const binding = bindings[0];
  if (!binding) {
    throw new Error(`No host bind mount covers ${localPath}; set LOADTEST_HOST_WORKSPACE explicitly`);
  }
  return path.join(binding.Source, path.relative(binding.Destination, localPath));
}

function hostWorkspace(): string {
  if (process.env.LOADTEST_HOST_WORKSPACE) {
    return path.resolve(process.env.LOADTEST_HOST_WORKSPACE);
  }
  if (!existsSync('/.dockerenv') && !process.env.REMOTE_CONTAINERS_IPC) {
    return workspace;
  }
  const mounts: unknown = JSON.parse(run('docker', ['inspect', '--format', '{{json .Mounts}}', hostname()]));
  return mapHostPath(workspace, mounts);
}

function hostPath(localPath: string): string {
  const absolute = path.resolve(localPath);
  return path.join(hostWorkspace(), path.relative(workspace, absolute));
}

function environment(directory: string): NodeJS.ProcessEnv {
  if (process.getuid?.() === 0) {
    throw new Error('Run loadtest as a non-root user so output stays locally readable');
  }
  return {
    ...process.env,
    LOADTEST_UID: String(process.getuid?.() ?? 10001),
    LOADTEST_GID: String(process.getgid?.() ?? 10001),
    ONELAKE_HOST_PATH: path.join(hostWorkspace(), path.relative(workspace, directory)),
    LOADTEST_SCRATCH_HOST_PATH: hostPath(path.join(workspace, 'projects/loadtest/.temp')),
    LOADTEST_BIN_HOST_PATH: hostPath(path.join(workspace, 'projects/loadtest/.temp/bin')),
  };
}

function compose(project: string, env: NodeJS.ProcessEnv, args: string[], timeout = 300_000): string {
  const files = ['--file', composeFile];
  if (existsSync(observabilityComposeFile)) files.push('--file', observabilityComposeFile);
  return run('docker', ['compose', '--project-name', project, ...files, ...args], env, timeout);
}

function composeInteractive(project: string, env: NodeJS.ProcessEnv, args: string[], timeout?: number): void {
  const files = ['--file', composeFile];
  if (existsSync(observabilityComposeFile)) files.push('--file', observabilityComposeFile);
  const result = spawnSync('docker', ['compose', '--project-name', project, ...files, ...args], {
    cwd: workspace,
    env,
    stdio: 'inherit',
    timeout,
  });
  if (result.error || result.status !== 0) {
    throw new Error(`docker compose ${args.join(' ')} failed: ${result.error?.message ?? result.status}`);
  }
}

function runInteractive(
  command: string,
  args: string[],
  env: NodeJS.ProcessEnv,
  timeout?: number,
): void {
  const result = spawnSync(command, args, {
    cwd: workspace,
    env,
    stdio: 'inherit',
    timeout,
  });
  if (result.error || result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed: ${result.error?.message ?? result.status}`);
  }
}

function inspect(directory: string): Inspection {
  const value: unknown = JSON.parse(run(path.join(workspace, 'target/debug/delta-inspect'), [directory]));
  return parseInspection(value);
}

function report(inspection: Inspection): void {
  console.log(`${inspection.files} committed Delta data files`);
  for (const [table, rows] of Object.entries(inspection.tables)) {
    console.log(`\n${table}: ${rows.length} rows, version ${inspection.versions[table]}`);
    console.log(`Schema: ${JSON.stringify(inspection.schemas[table])}`);
    console.log(JSON.stringify(rows.slice(0, 2), null, 2));
  }
}

function tablesInitialized(directory: string): boolean {
  const tables = readdirSync(directory, { withFileTypes: true }).filter((entry) => entry.isDirectory());
  return tables.length > 0 && tables.every((table) =>
    existsSync(path.join(directory, table.name, '_delta_log/00000000000000000000.json')));
}

async function waitForSignals(directory: string, runIds: string[], container: string): Promise<Inspection> {
  const deadline = Date.now() + 90_000;
  while (Date.now() < deadline) {
    if (run('docker', ['inspect', '--format', '{{.State.Running}}', container]).trim() !== 'true') {
      throw new Error('Collector exited before committing the expected signals');
    }
    if (tablesInitialized(directory)) {
      const result = inspect(directory);
      if (runIds.every((runId) => signalsReady(result, runId, 3))) {
        for (const runId of runIds) validateSignals(result, runId, 3);
        return result;
      }
    }
    await delay(1000);
  }
  throw new Error(`Timed out waiting for committed Delta signals for ${runIds.join(', ')}`);
}

function crossCoreCommit(directory: string, inspection: Inspection): boolean {
  const log = path.join(directory, 'logs/_delta_log');
  for (const file of readdirSync(log).filter((name) => /^\d{20}\.json$/u.test(name))) {
    const paths = readFileSync(path.join(log, file), 'utf8').trim().split('\n').flatMap((line) => {
      const action: unknown = JSON.parse(line);
      assert(typeof action === 'object' && action !== null && !Array.isArray(action), 'Invalid Delta action');
      if (!('add' in action)) return [];
      const add = action.add;
      assert(typeof add === 'object' && add !== null && 'path' in add && typeof add.path === 'string', 'Invalid Delta add action');
      return [add.path];
    });
    if (commitSpansCores(inspection, 'logs', paths)) return true;
  }
  return false;
}

function emit(project: string, env: NodeJS.ProcessEnv, runId: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn('docker', ['compose', '--project-name', project, '--file', composeFile,
      'run', '--rm', '--no-deps', 'hello-world', '--telemetry', '--count', '3', '--interval-ms', '100', '--run-id', runId],
    { cwd: workspace, env, stdio: ['ignore', 'ignore', 'pipe'] });
    let stderr = '';
    child.stderr.setEncoding('utf8').on('data', (chunk: string) => { stderr = `${stderr}${chunk}`.slice(-32_768); });
    const timer = setTimeout(() => {
      child.kill('SIGTERM');
      reject(new Error(`Emitter timed out for ${runId}`));
    }, 60_000);
    child.once('error', (error) => { clearTimeout(timer); reject(error); });
    child.once('exit', (code) => {
      clearTimeout(timer);
      if (code === 0) resolve();
      else reject(new Error(`Emitter ${runId} failed (${code}): ${stderr}`));
    });
  });
}

function stopCollector(project: string, env: NodeJS.ProcessEnv): void {
  compose(project, env, ['stop', 'otelcol-rust']);
  const container = compose(project, env, ['ps', '--all', '--quiet', 'otelcol-rust']).trim();
  const exitCode = run('docker', ['inspect', '--format', '{{.State.ExitCode}}', container]).trim();
  if (exitCode !== '0') {
    throw new Error(`Collector shutdown failed with exit code ${exitCode}`);
  }
}

async function otelE2e(): Promise<void> {
  const runId = `e2e-${randomUUID()}`;
  const directory = path.join(workspace, 'onelake/e2e', runId);
  mkdirSync(directory, { recursive: true });
  const env = { ...environment(directory), LOADTEST_CORES: '2' };
  const project = `rust-playground-${runId}`;
  console.log(`E2E output: ${path.relative(workspace, directory)}`);
  let failure: unknown;
  try {
    compose(project, env, ['config', '--quiet']);
    compose(project, env, ['up', '--detach', '--no-build', '--wait', '--wait-timeout', '90', 'otelcol-rust']);
    const container = compose(project, env, ['ps', '--all', '--quiet', 'otelcol-rust']).trim();
    const runIds: string[] = [];
    let committed: Inspection | undefined;
    for (let round = 0; round < 3; round++) {
      const emitters = Array.from({ length: 4 }, (_, index) => `${runId}-${round}-${index}`);
      runIds.push(...emitters);
      await Promise.all(emitters.map((id) => emit(project, env, id)));
      committed = await waitForSignals(directory, runIds, container);
      if (crossCoreCommit(directory, committed)) break;
    }
    assert(committed && crossCoreCommit(directory, committed), 'No Delta commit batched files from different collector cores');
    const shutdownId = `${runId}-shutdown`;
    await emit(project, env, shutdownId);
    assert(!signalsReady(inspect(directory), shutdownId, 3), 'Shutdown test had no unpublished telemetry');
    stopCollector(project, env);
    const first = inspect(directory);
    runIds.push(shutdownId);
    for (const id of runIds) validateSignals(first, id, 3);
    const restartId = `${runId}-restart`;
    compose(project, env, ['up', '--detach', '--no-build', '--wait', '--wait-timeout', '90', 'otelcol-rust']);
    await emit(project, env, restartId);
    stopCollector(project, env);
    const result = inspect(directory);
    writeFileSync(path.join(directory, 'inspection.json'), JSON.stringify(result, null, 2));
    for (const id of runIds) validateSignals(result, id, 3);
    validateSignals(result, restartId, 3);
    for (const table of ['logs', 'spans', 'number_data_points']) {
      if (result.versions[table] <= first.versions[table]) {
        throw new Error(`Delta version did not advance after restart for ${table}`);
      }
    }
    console.log(`PASS: multi-core batched commits, forced shutdown, restart, and three signals from ${result.files} committed Delta files`);
  } catch (error) {
    failure = error;
  } finally {
    try {
      writeFileSync(path.join(directory, 'compose.log'), compose(project, env, ['logs', '--no-color']));
    } catch (error) {
      console.error(`Unable to capture Compose logs: ${String(error)}`);
      failure ??= error;
    }
    try {
      compose(project, env, ['down', '--remove-orphans']);
    } catch (error) {
      console.error(`Compose cleanup failed: ${String(error)}`);
      failure ??= error;
    }
  }
  if (failure) {
    throw failure;
  }
}

function benchmarkProject(runId: string): string {
  const safe = runId.toLowerCase().replace(/[^a-z0-9_-]+/gu, '-').replace(/^-+|-+$/gu, '');
  return `rust-playground-loadtest-${safe}`.slice(0, 63);
}

function writeBenchmarkSecrets(runId: string): { edge: string; internal: string } {
  const root = path.join(workspace, 'projects/loadtest/.temp/secrets', runId);
  mkdirSync(root, { recursive: true });
  const edge = path.join(root, 'edge-hmac.txt');
  const internal = path.join(root, 'internal-hmac.txt');
  writeFileSync(edge, `${randomBytes(48).toString('base64')}\n`, { mode: 0o600 });
  writeFileSync(internal, `${randomBytes(48).toString('base64')}\n`, { mode: 0o600 });
  return { edge, internal };
}

function cargoTargetDirectory(): string {
  const target = process.env.CARGO_TARGET_DIR
    ? path.resolve(workspace, process.env.CARGO_TARGET_DIR)
    : path.join(workspace, 'target');
  return target;
}

function stageHelperBinaries(runId: string): string {
  const source = path.join(cargoTargetDirectory(), 'debug');
  const destination = path.join(workspace, 'projects/loadtest/.temp/bin', runId);
  mkdirSync(destination, { recursive: true });
  for (const binary of ['arrowcol-loadgen', 'arrowcol-sdk-gen']) {
    const input = path.join(source, binary);
    if (!existsSync(input)) throw new Error(`Required helper artifact is missing: ${input}`);
    const output = path.join(destination, binary);
    copyFileSync(input, output);
    chmodSync(output, 0o755);
  }
  return destination;
}

function writeDirectConfig(directory: string, service: string, network: BenchmarkNetwork): void {
  const state = path.join(directory, `direct-state-${service.split('-')[1]}`);
  mkdirSync(state, { recursive: true });
  const generationFile = path.join(state, 'generation');
  const previous = existsSync(generationFile)
    ? Number(readFileSync(generationFile, 'utf8').trim())
    : 0;
  if (!Number.isSafeInteger(previous) || previous < 0 || previous >= 2_147_483_646) {
    throw new Error(`Invalid persisted direct collector generation for ${service}`);
  }
  const generation = previous + 1;
  const address = network.direct[Number(service.split('-')[1]) - 1];
  assert(address, `No network address is configured for ${service}`);
  const template = readFileSync(path.join(workspace, 'projects/loadtest/config/direct.yaml.tmpl'), 'utf8');
  const config = template
    .replaceAll('__INSTANCE_KEY__', `loadtest-${service}`)
    .replaceAll('__ADVERTISED_ENDPOINT__', `${address}:50051`)
    .replaceAll('__GENERATION__', String(generation));
  writeFileSync(path.join(state, 'direct.yaml'), config);
  writeFileSync(generationFile, `${generation}\n`);
}

function writeRuntimeConfigs(directory: string, network: BenchmarkNetwork): void {
  for (const service of ['direct-1', 'direct-2', 'direct-3']) writeDirectConfig(directory, service, network);
  writeRouterConfig(directory, 500);
}

function writeRouterConfig(directory: string, refreshIntervalMs: number): void {
  const config = readFileSync(path.join(workspace, 'projects/loadtest/config/router.yaml'), 'utf8')
    .replace(/refresh_interval_ms: [0-9]+/u, `refresh_interval_ms: ${refreshIntervalMs}`);
  writeFileSync(
    path.join(directory, 'router-state/config.yaml'),
    config,
  );
}

function benchmarkEnvironment(
  directory: string,
  options: BenchmarkOptions,
  network: BenchmarkNetwork,
  binaryDirectory: string,
): NodeJS.ProcessEnv {
  const secrets = writeBenchmarkSecrets(options.runId);
  const infra = path.join(workspace, 'projects/arrowcol-infra/secrets');
  return {
    ...environment(directory),
    LOADTEST_RUN_ID: options.runId,
    LOADTEST_WARMUP: `${options.warmupSeconds}s`,
    LOADTEST_MEASURE: `${options.measureSeconds}s`,
    LOADTEST_CAPTURE_SETTLE: `${options.captureSettleSeconds}s`,
    LOADTEST_DRAIN_TIMEOUT: `${options.drainTimeoutSeconds}s`,
    LOADTEST_REQUEST_TIMEOUT: `${options.requestTimeoutSeconds}s`,
    LOADTEST_CONTINUOUS: String(options.continuous),
    LOADTEST_KEEP_STACK: String(options.keepStack),
    LOADTEST_LANES: String(options.lanes),
    LOADTEST_BATCH_ROWS: String(options.batchRows),
    LOADTEST_SEED: String(options.seed),
    LOADTEST_SCENARIOS: options.scenarios.join(','),
    LOADTEST_EDGE_HMAC_FILE: hostPath(secrets.edge),
    LOADTEST_INTERNAL_HMAC_FILE: hostPath(secrets.internal),
    LOADTEST_SCRATCH_HOST_PATH: hostPath(path.join(workspace, 'projects/loadtest/.temp')),
    LOADTEST_BIN_HOST_PATH: hostPath(binaryDirectory),
    LOADTEST_SUBNET: network.subnet,
    LOADTEST_SQL_IP: network.sql,
    LOADTEST_HIVE_IP: network.hive,
    LOADTEST_ROUTER_IP: network.router,
    LOADTEST_DIRECT_1_IP: network.direct[0],
    LOADTEST_DIRECT_2_IP: network.direct[1],
    LOADTEST_DIRECT_3_IP: network.direct[2],
    LOADTEST_DATAFLOW_1_IP: network.dataflow[0],
    LOADTEST_DATAFLOW_2_IP: network.dataflow[1],
    LOADTEST_DATAFLOW_3_IP: network.dataflow[2],
    SQLSERVER_SA_PASSWORD_FILE: hostPath(path.join(infra, 'sqlserver-sa-password.txt')),
    ARROWCOL_APP_PASSWORD_FILE: hostPath(path.join(infra, 'arrowcol-app-password.txt')),
    ARROWCOL_MIGRATOR_PASSWORD_FILE: hostPath(path.join(infra, 'arrowcol-migrator-password.txt')),
    HIVE_METASTORE_PASSWORD_FILE: hostPath(path.join(infra, 'hive-metastore-password.txt')),
  };
}

interface BenchmarkContext {
  directory: string;
  env: NodeJS.ProcessEnv;
  network: BenchmarkNetwork;
  networkName?: string;
  project: string;
  options: BenchmarkOptions;
  workspaceContainer?: string;
}

interface BenchmarkNetwork {
  subnet: string;
  sql: string;
  hive: string;
  router: string;
  direct: [string, string, string];
  dataflow: [string, string, string];
}

function cidrRange(value: string): [number, number] | undefined {
  const match = /^(\d+)\.(\d+)\.(\d+)\.(\d+)\/(\d+)$/u.exec(value);
  if (!match) return undefined;
  const octets = match.slice(1, 5).map(Number);
  const prefix = Number(match[5]);
  if (octets.some((octet) => !Number.isInteger(octet) || octet < 0 || octet > 255) ||
      !Number.isInteger(prefix) || prefix < 0 || prefix > 32) return undefined;
  const address = (((octets[0] * 256 + octets[1]) * 256 + octets[2]) * 256 + octets[3]) >>> 0;
  const mask = prefix === 0 ? 0 : (0xffff_ffff << (32 - prefix)) >>> 0;
  const start = (address & mask) >>> 0;
  const size = 2 ** (32 - prefix);
  return [start, start + size - 1];
}

function selectBenchmarkNetwork(_runId: string): BenchmarkNetwork {
  const ids = run('docker', ['network', 'ls', '--quiet']).trim().split('\n').filter(Boolean);
  const used = ids.length === 0
    ? []
    : (JSON.parse(run('docker', ['network', 'inspect', ...ids])) as Array<{
      IPAM?: { Config?: Array<{ Subnet?: string }> };
    }>).flatMap((network) => network.IPAM?.Config ?? [])
      .flatMap((config) => config.Subnet ? [cidrRange(config.Subnet)] : [])
      .filter((range): range is [number, number] => range !== undefined);
  const subnet = '10.89.0.0/24';
  const range = cidrRange(subnet);
  if (!range || used.some(([start, end]) => range[0] <= end && start <= range[1])) {
    throw new Error(`Benchmark subnet ${subnet} is already in use; stop the retained benchmark stack first`);
  }
  return {
    subnet,
    sql: '10.89.0.2',
    hive: '10.89.0.3',
    router: '10.89.0.10',
    direct: ['10.89.0.21', '10.89.0.22', '10.89.0.23'],
    dataflow: ['10.89.0.31', '10.89.0.32', '10.89.0.33'],
  };
}

function readBenchmarkNetwork(directory: string): BenchmarkNetwork {
  return JSON.parse(readFileSync(path.join(directory, 'router-state/network.json'), 'utf8')) as BenchmarkNetwork;
}

function connectWorkspaceContainer(
  project: string,
  env: NodeJS.ProcessEnv,
): { container?: string; network?: string } {
  const container = hostname();
  const inspected = spawnSync('docker', ['inspect', container], { cwd: workspace, stdio: 'ignore' });
  if (inspected.status !== 0) return {};
  const sqlContainer = compose(project, env, ['--profile', 'benchmark', 'ps', '--quiet', 'sqlserver']).trim();
  if (!sqlContainer) throw new Error('SQL Server container is unavailable for workspace network attachment');
  const network = run('docker', [
    'inspect', '--format', '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\\n"}}{{end}}', sqlContainer,
  ]).trim().split('\n').find(Boolean);
  if (!network) throw new Error('Benchmark Compose network was not found');
  const connected = spawnSync('docker', ['network', 'connect', network, container], {
    cwd: workspace,
    stdio: 'ignore',
  });
  if (connected.status !== 0) {
    const networks = run('docker', [
      'inspect', '--format', '{{json .NetworkSettings.Networks}}', container,
    ]);
    if (!Object.hasOwn(JSON.parse(networks) as object, network)) {
      throw new Error(`Could not attach workspace container to ${network}`);
    }
  }
  return { container, network };
}

function disconnectWorkspaceContainer(context: Pick<BenchmarkContext, 'workspaceContainer' | 'networkName'>): void {
  if (!context.workspaceContainer || !context.networkName) return;
  spawnSync('docker', [
    'network', 'disconnect', context.networkName, context.workspaceContainer,
  ], { cwd: workspace, stdio: 'ignore' });
}

function localLoadgenEnvironment(
  context: Pick<BenchmarkContext, 'directory' | 'env' | 'network' | 'options'>,
  results: string,
  overrides: Record<string, string> = {},
): NodeJS.ProcessEnv {
  const secrets = path.join(workspace, 'projects/loadtest/.temp/secrets', context.options.runId);
  const infra = path.join(workspace, 'projects/arrowcol-infra/secrets');
  return {
    ...context.env,
    LOADTEST_RESULTS: results,
    LOADTEST_DIRECT_ROOT: path.join(context.directory, 'direct'),
    LOADTEST_DATAFLOW_ROOT: path.join(context.directory, 'dataflow'),
    LOADTEST_ROUTER_ENDPOINT: `http://${context.network.router}:50050`,
    LOADTEST_CATALOG_ENDPOINT: `http://${context.network.direct[0]}:50051`,
    LOADTEST_RECEIPT_ENDPOINT: `http://${context.network.router}:50050`,
    LOADTEST_EDGE_SECRET_FILE: path.join(secrets, 'edge-hmac.txt'),
    LOADTEST_INTERNAL_SECRET_FILE: path.join(secrets, 'internal-hmac.txt'),
    LOADTEST_SCRATCH_ROOT: 'projects/loadtest/.temp/loadgen',
    LOADTEST_DIRECT_ADMIN_ENDPOINTS: context.network.direct.map((ip) => `http://${ip}:8081`).join(','),
    LOADTEST_DATAFLOW_ADMIN_ENDPOINTS: context.network.dataflow.map((ip) => `http://${ip}:32011`).join(','),
    ARROWCOL_SQL_HOST: context.network.sql,
    ARROWCOL_SQL_PASSWORD_FILE: path.join(infra, 'arrowcol-app-password.txt'),
    ARROWCOL_SDK_GEN: path.join(cargoTargetDirectory(), 'debug/arrowcol-sdk-gen'),
    ...overrides,
  };
}

function runLocalLoadgen(
  context: Pick<BenchmarkContext, 'directory' | 'env' | 'network' | 'options'>,
  command: 'run' | 'validate',
  results: string,
  overrides: Record<string, string> = {},
  expectFailure = false,
): void {
  let failed = false;
  try {
    runInteractive(
      path.join(cargoTargetDirectory(), 'debug/arrowcol-loadgen'),
      [command],
      localLoadgenEnvironment(context, results, overrides),
      900_000,
    );
  } catch (error) {
    failed = true;
    if (!expectFailure) throw error;
  }
  if (expectFailure) assert(failed, `${command} unexpectedly succeeded`);
}

function benchmark(options: BenchmarkOptions): BenchmarkContext {
  run(path.join(workspace, 'projects/arrowcol-infra/scripts/bootstrap-secrets.sh'), []);
  const directory = path.join(workspace, 'onelake/benchmark', options.runId);
  if (existsSync(directory) && readdirSync(directory).length > 0) {
    throw new Error(`Benchmark run directory already contains data: ${path.relative(workspace, directory)}`);
  }
  for (const child of [
    'direct', 'dataflow', 'otel', 'results', 'router-state',
    'direct-state-1', 'direct-state-2', 'direct-state-3',
  ]) {
    mkdirSync(path.join(directory, child), { recursive: true });
  }
  mkdirSync(path.join(workspace, 'projects/loadtest/.temp'), { recursive: true });
  const network = selectBenchmarkNetwork(options.runId);
  writeRuntimeConfigs(directory, network);
  writeFileSync(path.join(directory, 'router-state/network.json'), `${JSON.stringify(network, null, 2)}\n`);
  const env = benchmarkEnvironment(directory, options, network, stageHelperBinaries(options.runId));
  const project = benchmarkProject(options.runId);
  const services = [
    'sqlserver', 'database-bootstrap', 'arrowcol-migrate', 'hive-schema', 'hive-metastore',
    'direct-1', 'direct-2', 'direct-3', 'dataflow-1', 'dataflow-2', 'dataflow-3', 'router',
  ];
  if (existsSync(observabilityComposeFile) && shouldStartRuntimeObservability()) {
    services.push(
      'prometheus', 'grafana', 'loki', 'tempo', 'alloy', 'cadvisor',
      'sql-exporter', 'json-exporter', 'blackbox-exporter',
    );
  }
  if (options.otelBaseline) services.push('otelcol-rust', 'hello-world');
  let workspaceConnection: { container?: string; network?: string } = {};
  let failure: unknown;
  try {
    compose(project, env, ['--profile', 'benchmark', 'config', '--quiet']);
    compose(project, env, [
      '--profile', 'benchmark', 'build', 'sqlserver', 'hive-metastore',
    ], 3_600_000);
    compose(project, env, [
      '--profile', 'benchmark', 'up', '--detach', '--no-build', '--wait', '--wait-timeout', '600', ...services,
    ], 3_600_000);
    workspaceConnection = connectWorkspaceContainer(project, env);
    const context = { directory, env, network, options };
    runLocalLoadgen(context, 'run', path.join(directory, 'results'));
    if (shouldRunIndependentValidation(options)) {
      composeInteractive(project, env, ['--profile', 'benchmark', 'run', '--rm', '--no-deps', 'validator'], 900_000);
    }
  } catch (error) {
    failure = error;
  } finally {
    try {
      writeFileSync(path.join(directory, 'results/compose.log'),
        compose(project, env, ['--profile', 'benchmark', 'logs', '--no-color', '--timestamps']));
      writeFileSync(path.join(directory, 'results/compose-ps.json'),
        compose(project, env, ['--profile', 'benchmark', 'ps', '--all', '--format', 'json']));
    } catch (error) {
      console.error(`Unable to capture benchmark Compose diagnostics: ${String(error)}`);
      failure ??= error;
    }
    if (!options.keepStack) {
      disconnectWorkspaceContainer({
        workspaceContainer: workspaceConnection.container,
        networkName: workspaceConnection.network,
      });
      try {
        compose(project, env, ['--profile', 'benchmark', 'down', '--volumes', '--remove-orphans'], 600_000);
      } catch (error) {
        console.error(`Benchmark Compose cleanup failed: ${String(error)}`);
        failure ??= error;
      }
    }
  }
  if (failure) throw failure;
  console.log(`Benchmark results: ${path.relative(workspace, path.join(directory, 'results'))}`);
  return {
    directory,
    env,
    network,
    networkName: workspaceConnection.network,
    project,
    options,
    workspaceContainer: workspaceConnection.container,
  };
}

interface RoutingEvidence {
  collectors: Array<{ instance_id: string; instance_key: string; implementation_pool: string }>;
  leases: Array<{ implementation_pool: string; table: string; holder_instance_id: string }>;
}

function nestedDriver(
  context: BenchmarkContext,
  name: string,
  extraEnvironment: Record<string, string>,
  expectFailure = false,
): void {
  mkdirSync(path.join(context.directory, 'results', name), { recursive: true });
  const environmentArguments = {
    LOADTEST_WARMUP: '0s',
    LOADTEST_MEASURE: '2s',
    LOADTEST_CAPTURE_SETTLE: '0s',
    LOADTEST_DRAIN_TIMEOUT: '60s',
    LOADTEST_REQUEST_TIMEOUT: '10s',
    LOADTEST_CONTINUOUS: 'false',
    LOADTEST_LANES: '2',
    LOADTEST_BATCH_ROWS: '64',
    LOADTEST_SEED: String(context.options.seed),
    LOADTEST_SCENARIOS: 'direct',
    LOADTEST_SKIP_PROBES: 'true',
    LOADTEST_REUSE_GENERATED: 'true',
    LOADTEST_SKIP_CAPABILITIES: 'true',
    LOADTEST_REQUIRE_ALL_COLLECTORS: 'false',
    ...extraEnvironment,
  };
  runLocalLoadgen(
    context,
    'run',
    path.join(context.directory, 'results', name),
    environmentArguments,
    expectFailure,
  );
}

function catalogEndpoint(context: BenchmarkContext, excludedService?: string): string {
  for (let index = 0; index < context.network.direct.length; index++) {
    const service = `direct-${index + 1}`;
    if (service !== excludedService) return `http://${context.network.direct[index]}:50051`;
  }

  throw new Error('No healthy direct catalog endpoint is available');
}

function directAdminEndpoints(context: BenchmarkContext, excludedService?: string): string {
  const endpoints = context.network.direct
    .map((ip, index) => ({ endpoint: `http://${ip}:8081`, service: `direct-${index + 1}` }))
    .filter(({ service }) => service !== excludedService)
    .filter(({ service }) => {
      const container = compose(context.project, context.env, [
        '--profile', 'benchmark', 'ps', '--all', '--quiet', service,
      ]).trim();
      if (!container) return false;
      const state = run('docker', [
        'inspect', '--format', '{{.State.Running}} {{if .State.Health}}{{.State.Health.Status}}{{end}}', container,
      ]).trim();
      return state === 'true healthy';
    })
    .map(({ endpoint }) => endpoint);
  if (endpoints.length === 0) throw new Error('No healthy direct admin endpoint is available');
  while (endpoints.length < 3) endpoints.push(endpoints[0]);
  return endpoints.slice(0, 3).join(',');
}

function routingOwner(
  directory: string,
  excludedService?: string,
): { instanceId: string; service: string; table: string } {
  const routing = JSON.parse(readFileSync(path.join(directory, 'routing.json'), 'utf8')) as RoutingEvidence;
  const collectors = new Map(routing.collectors.map((collector) => [collector.instance_id, collector]));
  for (const lease of routing.leases) {
    if (lease.implementation_pool !== 'direct') continue;
    const collector = collectors.get(lease.holder_instance_id);
    if (!collector?.instance_key.startsWith('loadtest-direct-')) continue;
    const service = collector.instance_key.replace('loadtest-', '');
    if (service !== excludedService) {
      return { instanceId: lease.holder_instance_id, service, table: lease.table };
    }
  }

  throw new Error('No eligible direct routing owner was found');
}

function forceRoutingExpiry(context: BenchmarkContext, holderInstanceId: string): void {
  if (!/^[0-9a-f-]{36}$/iu.test(holderInstanceId)) throw new Error('Invalid routing holder UUID');
  const sql = serviceContainer(context, 'sqlserver');
  run('docker', [
    'exec', sql, 'bash', '-lc',
    `/usr/local/bin/sqlcmd-secret -b -S localhost -U sa -d arrowcol -Q "` +
      `SET ANSI_NULLS ON; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; ` +
      `SET ARITHABORT ON; SET CONCAT_NULL_YIELDS_NULL ON; ` +
      `SET QUOTED_IDENTIFIER ON; SET NUMERIC_ROUNDABORT OFF; ` +
      `UPDATE arrowcol.routing_collectors ` +
      `SET state='failed', heartbeat_at_utc=DATEADD(second,-2,SYSUTCDATETIME()), ` +
      `lease_expires_at_utc=DATEADD(second,-1,SYSUTCDATETIME()) ` +
      `WHERE instance_id='${holderInstanceId}'; ` +
      `UPDATE arrowcol.collector_instances ` +
      `SET state='failed', heartbeat_at_utc=DATEADD(second,-2,SYSUTCDATETIME()), ` +
      `lease_expires_at_utc=DATEADD(second,-1,SYSUTCDATETIME()), ` +
      `stopped_at_utc=SYSUTCDATETIME() ` +
      `WHERE instance_id='${holderInstanceId}' AND stopped_at_utc IS NULL; ` +
      `DELETE FROM arrowcol.routing_table_leases ` +
      `WHERE holder_instance_id='${holderInstanceId}'; ` +
      `UPDATE arrowcol.table_leases ` +
      `SET renewed_at_utc=DATEADD(second,-2,SYSUTCDATETIME()), ` +
      `expires_at_utc=DATEADD(second,-1,SYSUTCDATETIME()) ` +
      `WHERE holder_instance_id='${holderInstanceId}' AND released_at_utc IS NULL;"`,
  ]);
}

function resetDirectLeases(context: BenchmarkContext): void {
  const sql = serviceContainer(context, 'sqlserver');
  run('docker', [
    'exec', sql, 'bash', '-lc',
    `/usr/local/bin/sqlcmd-secret -b -S localhost -U sa -d arrowcol -Q "` +
      `SET ANSI_NULLS ON; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; ` +
      `SET ARITHABORT ON; SET CONCAT_NULL_YIELDS_NULL ON; ` +
      `SET QUOTED_IDENTIFIER ON; SET NUMERIC_ROUNDABORT OFF; ` +
      `DELETE FROM arrowcol.routing_table_leases WHERE implementation_pool='direct'; ` +
      `UPDATE arrowcol.table_leases ` +
      `SET renewed_at_utc=DATEADD(second,-2,SYSUTCDATETIME()), ` +
      `expires_at_utc=DATEADD(second,-1,SYSUTCDATETIME()) ` +
      `WHERE released_at_utc IS NULL;"`,
  ]);
}

function serviceContainer(context: BenchmarkContext, service: string): string {
  const container = compose(context.project, context.env, [
    '--profile', 'benchmark', 'ps', '--all', '--quiet', service,
  ]).trim();
  if (!container) throw new Error(`Compose service ${service} has no container`);
  return container;
}

function recreateService(context: BenchmarkContext, service: string): void {
  if (service.startsWith('direct-')) writeDirectConfig(context.directory, service, context.network);
  compose(context.project, context.env, [
    '--profile', 'benchmark', 'rm', '--stop', '--force', service,
  ]);
  compose(context.project, context.env, [
    '--profile', 'benchmark', 'up', '--detach', '--no-deps', '--wait', '--wait-timeout', '180', service,
  ], 300_000);
}

async function chaos(options: BenchmarkOptions): Promise<void> {
  const context = benchmark({ ...options, keepStack: true, continuous: false });
  const chaosEvidence: Record<string, unknown> = {};
  let failure: unknown;
  try {
    const initialOwner = routingOwner(path.join(context.directory, 'results'), 'direct-1');
    const killedContainer = serviceContainer(context, initialOwner.service);
    run('docker', ['kill', '--signal', 'KILL', killedContainer]);
    forceRoutingExpiry(context, initialOwner.instanceId);
    writeRouterConfig(context.directory, 60_000);
    recreateService(context, 'router');
    nestedDriver(context, 'owner-kill-resend', {
      LOADTEST_RUN_ID: options.runId,
      LOADTEST_TABLES: initialOwner.table,
      LOADTEST_CATALOG_ENDPOINT: catalogEndpoint(context, initialOwner.service),
      LOADTEST_DIRECT_ADMIN_ENDPOINTS: directAdminEndpoints(context, initialOwner.service),
      LOADTEST_MAX_ATTEMPTS: '16',
    });
    recreateService(context, initialOwner.service);
    chaosEvidence.ownerKillLeaseExpiryResend = {
      service: initialOwner.service,
      table: initialOwner.table,
      container: killedContainer,
      passed: true,
    };

    const staleOwner = routingOwner(path.join(context.directory, 'results/owner-kill-resend'), 'direct-1');
    const staleContainer = serviceContainer(context, staleOwner.service);
    run('docker', ['pause', staleContainer]);
    forceRoutingExpiry(context, staleOwner.instanceId);
    recreateService(context, 'router');
    nestedDriver(context, 'stale-fence-resend', {
      LOADTEST_RUN_ID: options.runId,
      LOADTEST_TABLES: staleOwner.table,
      LOADTEST_CATALOG_ENDPOINT: catalogEndpoint(context, staleOwner.service),
      LOADTEST_DIRECT_ADMIN_ENDPOINTS: directAdminEndpoints(context, staleOwner.service),
      LOADTEST_MAX_ATTEMPTS: '16',
    }, true);
    run('docker', ['unpause', staleContainer]);
    await delay(10_000);
    const staleReady = spawnSync('docker', [
      'exec', staleContainer, 'curl', '--fail', '--silent', 'http://127.0.0.1:8081/readyz',
    ], { cwd: workspace, stdio: 'ignore' }).status === 0;
    assert(!staleReady, 'stale direct owner remained ready after its lease was transferred');
    recreateService(context, staleOwner.service);
    const recoveryOwner = routingOwner(
      path.join(context.directory, 'results/stale-fence-resend'),
    );
    const recoveryContainer = serviceContainer(context, recoveryOwner.service);
    spawnSync('docker', ['kill', '--signal', 'KILL', recoveryContainer], {
      cwd: workspace,
      stdio: 'ignore',
    });
    forceRoutingExpiry(context, recoveryOwner.instanceId);
    recreateService(context, 'router');
    nestedDriver(context, 'stale-fence-recovery', {
      LOADTEST_RUN_ID: options.runId,
      LOADTEST_TABLES: staleOwner.table,
      LOADTEST_DIRECT_ADMIN_ENDPOINTS: directAdminEndpoints(context, recoveryOwner.service),
      LOADTEST_MAX_ATTEMPTS: '16',
    });
    recreateService(context, recoveryOwner.service);
    chaosEvidence.staleFence = {
      service: staleOwner.service,
      table: staleOwner.table,
      container: staleContainer,
      passed: true,
    };

    for (const service of ['direct-3', 'dataflow-3']) {
      compose(context.project, context.env, [
        '--profile', 'benchmark', 'stop', '--timeout', '120', service,
      ], 180_000);
      const container = serviceContainer(context, service);
      const exitCode = run('docker', ['inspect', '--format', '{{.State.ExitCode}}', container]).trim();
      assert.equal(exitCode, '0', `${service} graceful drain exited ${exitCode}`);
      recreateService(context, service);
    }
    chaosEvidence.gracefulDrainRestart = { services: ['direct-3', 'dataflow-3'], passed: true };

    compose(context.project, context.env, ['--profile', 'benchmark', 'stop', '--timeout', '30', 'sqlserver'], 120_000);
    nestedDriver(context, 'sql-outage', {
      LOADTEST_RUN_ID: options.runId,
      LOADTEST_TABLES: initialOwner.table,
      LOADTEST_REQUEST_TIMEOUT: '5s',
      LOADTEST_DRAIN_TIMEOUT: '10s',
      LOADTEST_CATALOG_ENDPOINT: catalogEndpoint(context),
    }, true);
    compose(context.project, context.env, [
      '--profile', 'benchmark', 'up', '--detach', '--no-deps', '--wait', '--wait-timeout', '180', 'sqlserver',
    ], 300_000);
    for (const service of ['direct-1', 'direct-2', 'direct-3']) recreateService(context, service);
    resetDirectLeases(context);
    recreateService(context, 'router');
    compose(context.project, context.env, [
      '--profile', 'benchmark', 'up', '--detach', '--no-deps', '--wait', '--wait-timeout', '180',
      'dataflow-1', 'dataflow-2', 'dataflow-3',
    ], 600_000);
    nestedDriver(context, 'sql-recovery', {
      LOADTEST_RUN_ID: options.runId,
      LOADTEST_TABLES: initialOwner.table,
      LOADTEST_CATALOG_ENDPOINT: catalogEndpoint(context),
    });
    chaosEvidence.sqlInterruptionRecovery = { boundedFailure: true, recovery: true };
    runLocalLoadgen(
      context,
      'validate',
      path.join(context.directory, 'results'),
      { LOADTEST_DRAIN_TIMEOUT: '90s', LOADTEST_REQUIRE_ALL_COLLECTORS: 'true' },
    );
  } catch (error) {
    failure = error;
  } finally {
    try {
      writeFileSync(
        path.join(context.directory, 'results/chaos.json'),
        `${JSON.stringify({ passed: failure === undefined, ...chaosEvidence }, null, 2)}\n`,
      );
      writeFileSync(path.join(context.directory, 'results/compose-chaos.log'),
        compose(context.project, context.env, ['--profile', 'benchmark', 'logs', '--no-color', '--timestamps']));
    } catch (error) {
      failure ??= error;
    }
    if (!options.keepStack) {
      disconnectWorkspaceContainer(context);
      try {
        compose(context.project, context.env, [
          '--profile', 'benchmark', 'down', '--volumes', '--remove-orphans',
        ], 600_000);
      } catch (error) {
        failure ??= error;
      }
    }
  }
  if (failure) throw failure;
}

async function main(): Promise<void> {
  const action = process.argv[2];
  if (action === 'otel-e2e') {
    await otelE2e();
    return;
  }
  if (action === 'e2e') {
    await otelE2e();
    await chaos(parseBenchmarkOptions([
      '--run-id', `e2e-arrow-${randomUUID()}`,
      '--warmup', '0',
      '--measure', '1s',
      '--capture-settle', '0',
      '--drain-timeout', '2m',
      '--request-timeout', '30s',
      '--lanes', '2',
      '--batch-rows', '16',
      '--scenarios', 'direct,dataflow,paired',
    ]));
    return;
  }
  if (action === 'chaos') {
    if (process.argv.slice(3).some((argument) => argument === '--help' || argument === '-h')) {
      console.log(benchmarkUsage);
      return;
    }
    await chaos(parseBenchmarkOptions(process.argv.slice(3)));
    return;
  }
  if (action === 'benchmark' || action === 'run') {
    if (process.argv.slice(3).some((argument) => argument === '--help' || argument === '-h')) {
      console.log(benchmarkUsage);
      return;
    }
    benchmark(parseBenchmarkOptions(process.argv.slice(3)));
    return;
  }
  if (action === 'benchmark-down') {
    const options = parseBenchmarkOptions(process.argv.slice(3));
    const directory = path.join(workspace, 'onelake/benchmark', options.runId);
    const env = benchmarkEnvironment(
      directory,
      options,
      readBenchmarkNetwork(directory),
      stageHelperBinaries(options.runId),
    );
    const project = benchmarkProject(options.runId);
    const connection = connectWorkspaceContainer(project, env);
    disconnectWorkspaceContainer({
      workspaceContainer: connection.container,
      networkName: connection.network,
    });
    console.log(compose(project, env, [
      '--profile', 'benchmark', 'down', '--volumes', '--remove-orphans',
    ], 600_000));
    return;
  }
  const directory = path.join(workspace, 'onelake/demo');
  mkdirSync(directory, { recursive: true });
  if (action === 'inspect') {
    report(inspect(process.env.ONELAKE_INSPECT_PATH ? path.resolve(process.env.ONELAKE_INSPECT_PATH) : directory));
    return;
  }
  const env = environment(directory);
  const project = 'rust-playground-loadtest';
  switch (action) {
    case 'up':
      console.log(compose(project, env, ['up', '--detach', '--no-build', '--wait', '--wait-timeout', '90'], 1_800_000));
      break;
    case 'down':
      console.log(compose(project, env, ['down', '--remove-orphans']));
      break;
    case 'logs':
      console.log(compose(project, env, ['logs', '--no-color', '--tail', '100']));
      break;
    default:
      throw new Error('Usage: loadtest <up|down|logs|inspect|otel-e2e|e2e|benchmark|run|chaos|benchmark-down>');
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
