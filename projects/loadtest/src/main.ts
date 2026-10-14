import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import {
  chmodSync, closeSync, copyFileSync, existsSync, lstatSync, mkdirSync, openSync, readFileSync, realpathSync, writeFileSync,
} from 'node:fs';
import { hostname } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  benchmarkUsage,
  benchmarkProject,
  parseBenchmarkOptions,
  validateRunId,
  type BenchmarkOptions,
} from './cli.js';
import { startResourceSampler } from './resource-sampler.js';
import { writeProvenance } from './provenance.js';
import { benchmarkComposeFiles, buildBenchmarkComposeOverride, buildBenchmarkRuntimeConfigs } from './runtime-config.js';

const workspace = realpathSync(path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..'));
const composeFile = path.join(workspace, 'projects/loadtest/compose.yaml');

const startedAt = Date.now();

function step(message: string): void {
  const elapsed = Math.round((Date.now() - startedAt) / 1000);
  const stamp = `${String(Math.floor(elapsed / 60)).padStart(2, '0')}:${String(elapsed % 60).padStart(2, '0')}`;
  console.log(`\n[loadtest +${stamp}] ${message}`);
}

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
  const files = benchmarkComposeFiles(composeFile, env);
  return run('docker', ['compose', '--project-name', project, ...files, ...args], env, timeout);
}

function composeToFile(project: string, env: NodeJS.ProcessEnv, args: string[], file: string): void {
  const fd = openSync(file, 'w');
  try {
    const result = spawnSync('docker', ['compose', '--project-name', project, ...benchmarkComposeFiles(composeFile, env), ...args], {
      cwd: workspace, env, stdio: ['ignore', fd, 'inherit'], timeout: 600_000,
    });
    if (result.error || result.status !== 0) throw new Error(`docker compose ${args.join(' ')} failed: ${result.error?.message ?? result.status}`);
  } finally {
    closeSync(fd);
  }
}

function composeInteractive(project: string, env: NodeJS.ProcessEnv, args: string[], timeout?: number): void {
  const files = benchmarkComposeFiles(composeFile, env);
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

function benchmarkProjectHasResources(project: string): boolean {
  const filter = `label=com.docker.compose.project=${project}`;
  return run('docker', ['ps', '--all', '--quiet', '--filter', filter]).trim().length > 0
    || run('docker', ['network', 'ls', '--quiet', '--filter', filter]).trim().length > 0
    || run('docker', ['volume', 'ls', '--quiet', '--filter', filter]).trim().length > 0
    || run('docker', ['volume', 'ls', '--quiet', '--filter', `name=^${project}_`]).trim().length > 0;
}

export function prepareBenchmarkDirectory(
  benchmarkRoot: string,
  runId: string,
  projectHasResources: () => boolean,
): { directory: string } {
  validateRunId(runId);
  const directory = path.join(benchmarkRoot, runId);
  const conflict = (): Error => new Error(
    `Benchmark run '${runId}' already has a directory, stack or retained SQL volume/history; use an unused --run-id. Nothing was stopped or removed.`,
  );
  if (projectHasResources()) throw conflict();
  try {
    lstatSync(directory);
    throw conflict();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  mkdirSync(benchmarkRoot, { recursive: true });
  try {
    mkdirSync(directory);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'EEXIST') throw conflict();
    throw error;
  }
  return { directory };
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
  const address = network.direct;
  const template = readFileSync(path.join(workspace, 'projects/loadtest/config/direct.yaml.tmpl'), 'utf8');
  const runtime = buildBenchmarkRuntimeConfigs(template,
    readFileSync(path.join(workspace, 'projects/arrowcol-dataflow-rust/config.yaml'), 'utf8'),
    { instanceKey: `loadtest-${service}`, advertisedEndpoint: `${address}:50051`, generation });
  writeFileSync(path.join(state, 'direct.yaml'), runtime.directYaml);
  const dataflowState = path.join(directory, 'dataflow-state-1');
  mkdirSync(dataflowState, { recursive: true });
  writeFileSync(path.join(dataflowState, 'dataflow.yaml'), runtime.dataflowYaml);
  writeFileSync(path.join(directory, 'router-state/compose-runtime.json'),
    `${buildBenchmarkComposeOverride(hostPath(directory))}\n`);
  writeFileSync(generationFile, `${generation}\n`);
}

function writeRuntimeConfigs(directory: string, network: BenchmarkNetwork): void {
  writeDirectConfig(directory, 'direct-1', network);
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
    LOADTEST_COMPOSE_OVERRIDE_FILE: path.join(directory, 'router-state/compose-runtime.json'),
    LOADTEST_WARMUP: `${options.warmupSeconds}s`,
    ...(options.measureSeconds === undefined ? {} : { LOADTEST_MEASURE: `${options.measureSeconds}s` }),
    LOADTEST_CAPTURE_SETTLE: `${options.captureSettleSeconds}s`,
    LOADTEST_DRAIN_TIMEOUT: `${options.drainTimeoutSeconds}s`,
    LOADTEST_REQUEST_TIMEOUT: `${options.requestTimeoutSeconds}s`,
    LOADTEST_CONTINUOUS: String(options.measureSeconds === undefined),
    LOADTEST_KEEP_STACK: 'true',
    LOADTEST_LANES: String(options.concurrency),
    LOADTEST_BATCH_ROWS: String(options.batchRows),
    LOADTEST_SEED: String(options.seed),
    LOADTEST_SCENARIOS: 'paired',
    LOADTEST_EDGE_HMAC_FILE: hostPath(secrets.edge),
    LOADTEST_INTERNAL_HMAC_FILE: hostPath(secrets.internal),
    LOADTEST_SCRATCH_HOST_PATH: hostPath(path.join(workspace, 'projects/loadtest/.temp')),
    LOADTEST_BIN_HOST_PATH: hostPath(binaryDirectory),
    LOADTEST_SUBNET: network.subnet,
    LOADTEST_SQL_IP: network.sql,
    LOADTEST_HIVE_IP: network.hive,
    LOADTEST_ROUTER_IP: network.router,
    LOADTEST_DIRECT_1_IP: network.direct,
    LOADTEST_DATAFLOW_1_IP: network.dataflow,
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
  direct: string;
  dataflow: string;
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
  for (let index = 0; index < 256; index++) {
    const base = `10.89.${index}`;
    const subnet = `${base}.0/24`;
    const range = cidrRange(subnet)!;
    if (used.some(([start, end]) => range[0] <= end && start <= range[1])) continue;
    return {
      subnet, sql: `${base}.2`, hive: `${base}.3`, router: `${base}.10`,
      direct: `${base}.21`, dataflow: `${base}.31`,
    };
  }
  throw new Error('No unused benchmark subnet in 10.89.0.0/16; retained stacks were left untouched');
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
    LOADTEST_CATALOG_ENDPOINT: `http://${context.network.direct}:50051`,
    LOADTEST_RECEIPT_ENDPOINT: `http://${context.network.router}:50050`,
    LOADTEST_EDGE_SECRET_FILE: path.join(secrets, 'edge-hmac.txt'),
    LOADTEST_INTERNAL_SECRET_FILE: path.join(secrets, 'internal-hmac.txt'),
    LOADTEST_SCRATCH_ROOT: `projects/loadtest/.temp/loadgen/${context.options.runId}`,
    LOADTEST_DIRECT_ADMIN_ENDPOINTS: `http://${context.network.direct}:8081`,
    LOADTEST_DATAFLOW_ADMIN_ENDPOINTS: `http://${context.network.dataflow}:32011`,
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
      command === 'validate' ? 3_600_000 : context.options.measureSeconds === undefined ? undefined : (context.options.warmupSeconds + context.options.measureSeconds + 600) * 1000,
    );
  } catch (error) {
    failed = true;
    if (!expectFailure) throw error;
  }
  if (expectFailure) assert(failed, `${command} unexpectedly succeeded`);
}

export function portainerAccess(port: string, passwordFile: string): string {
  // Portainer trims exactly one trailing newline from --admin-password-file.
  const password = readFileSync(passwordFile, 'utf8').replace(/\n$/u, '');
  return `Portainer: https://localhost:${port} (port ${port}, username admin, password ${password}; self-signed TLS)`;
}

export function sqlserverAccess(port: string, passwordFile: string): string {
  const password = readFileSync(passwordFile, 'utf8').replace(/\n+$/u, '');
  assert(password.length > 0 && !/[\r\n]/u.test(password), 'SQL Server password secret must be non-empty and single-line');
  return `SQL Server: localhost,${port} (host port ${port} -> container port 1433, username sa, password ${password})`;
}

export function deltaTableLocations(directory: string): string {
  const root = path.resolve(directory);
  return [
    'Delta tables on disk (workspace paths)',
    '-------------------------------------',
    `  Direct:   ${path.join(root, 'direct')}`,
    `  Dataflow: ${path.join(root, 'dataflow')}`,
  ].join('\n');
}

async function benchmark(options: BenchmarkOptions): Promise<BenchmarkContext> {
  step(`Load test: warmup ${options.warmupSeconds}s, concurrency ${options.concurrency}, batch rows ${options.batchRows}`);
  step('Bootstrapping secrets and preparing run directory');
  const project = benchmarkProject(options.runId);
  const prepared = prepareBenchmarkDirectory(
    path.join(workspace, 'onelake/benchmark'),
    options.runId,
    () => benchmarkProjectHasResources(project),
  );
  const directory = prepared.directory;
  for (const child of [
    'direct', 'dataflow', 'otel', 'results', 'router-state',
    'direct-state-1',
  ]) {
    mkdirSync(path.join(directory, child), { recursive: true });
  }
  let network: BenchmarkNetwork;
  let env: NodeJS.ProcessEnv;
  try {
    run(path.join(workspace, 'projects/arrowcol-infra/scripts/bootstrap-secrets.sh'), []);
    mkdirSync(path.join(workspace, 'projects/loadtest/.temp'), { recursive: true });
    network = selectBenchmarkNetwork(options.runId);
    writeRuntimeConfigs(directory, network);
    writeFileSync(path.join(directory, 'router-state/network.json'), `${JSON.stringify(network, null, 2)}\n`);
    env = benchmarkEnvironment(directory, options, network, stageHelperBinaries(options.runId));
  } catch (error) {
    writeFileSync(path.join(directory, 'results/runner-failure.json'), `${JSON.stringify({
      timestamp: new Date().toISOString(), phase: 'preparation', kind: error instanceof Error ? error.name : 'Error',
    })}\n`);
    throw error;
  }
  const services = [
    'sqlserver', 'database-bootstrap', 'arrowcol-migrate', 'hive-schema', 'hive-metastore',
    'lgtm', 'cadvisor', 'portainer', 'direct-1', 'dataflow-1', 'router',
  ];
  let workspaceConnection: { container?: string; network?: string } = {};
  let failure: unknown;
  let phase = 'compose-validation';
  let stopSampler: (() => Promise<void>) | undefined;
  const resultsDirectory = path.relative(workspace, path.join(directory, 'results'));
  try {
    step('Validating Compose configuration');
    writeProvenance(workspace, directory, project, options, env);
    compose(project, env, ['--profile', 'benchmark', 'config', '--quiet']);
    phase = 'image-build';
    step('Building sqlserver and hive-metastore images (can take several minutes on first run)');
    composeInteractive(project, env, [
      '--profile', 'benchmark', 'build', 'sqlserver', 'hive-metastore',
    ], 3_600_000);
    phase = 'stack-start';
    step(`Starting ${services.length} services and waiting for health (up to 10 minutes)`);
    composeInteractive(project, env, [
      '--profile', 'benchmark', 'up', '--detach', '--no-build', '--wait', '--wait-timeout', '600', ...services,
    ], 3_600_000);
    workspaceConnection = connectWorkspaceContainer(project, env);
    phase = 'startup-provenance';
    writeProvenance(workspace, directory, project, options, env, true);
    phase = 'sampler-start';
    stopSampler = await startResourceSampler({
      project, results: path.join(directory, 'results'),
      directAdmin: `http://${network.direct}:8081`,
      dataflowAdmin: `http://${network.dataflow}:32011`,
    });
    step(
      `Grafana: http://localhost:${process.env.GRAFANA_PORT ?? 3000} (admin/admin); ` +
      `logs: docker compose --project-name ${project} logs -f`,
    );
    step(portainerAccess(
      process.env.PORTAINER_PORT ?? '9444',
      path.join(workspace, 'projects/arrowcol-infra/secrets/sqlserver-sa-password.txt'),
    ));
    step(sqlserverAccess(
      process.env.SQLSERVER_PORT ?? '14333',
      path.join(workspace, 'projects/arrowcol-infra/secrets/sqlserver-sa-password.txt'),
    ));
    step(deltaTableLocations(directory));
    const context = { directory, env, network, options };
    phase = 'load-generation';
    step('Running load generator (Ctrl+C to stop; stack is kept)');
    runLocalLoadgen(context, 'run', path.join(directory, 'results'));
    if (options.validate) {
      phase = 'delta-validation';
      step('Validating Delta output');
      runLocalLoadgen(context, 'validate', path.join(directory, 'results'));
    }
  } catch (error) {
    failure = error;
  } finally {
    try {
      await stopSampler?.();
    } catch (error) {
      failure ??= error;
    }
    step('Capturing Compose logs and diagnostics');
    try {
      composeToFile(project, env, ['--profile', 'benchmark', 'logs', '--no-color', '--timestamps'],
        path.join(directory, 'results/compose.log'));
      writeFileSync(path.join(directory, 'results/compose-ps.json'),
        compose(project, env, ['--profile', 'benchmark', 'ps', '--all', '--format', 'json']));
    } catch (error) {
      console.error(`Unable to capture benchmark Compose diagnostics: ${String(error)}`);
      failure ??= error;
    }
  }
  if (failure) {
    writeFileSync(path.join(directory, 'results/runner-failure.json'), `${JSON.stringify({
      timestamp: new Date().toISOString(), phase, kind: failure instanceof Error ? failure.name : 'Error',
    })}\n`);
    step(`FAILED. Diagnostics: ${resultsDirectory}/compose.log`);
    throw failure;
  }
  step(`Done. Results: ${resultsDirectory}. Stack kept; use an unused --run-id for another run.`);
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

async function main(): Promise<void> {
  const userArguments = process.argv.slice(2);
  if (userArguments.some((argument) => argument === '--help' || argument === '-h')) {
    console.log(benchmarkUsage);
    return;
  }
  const options = parseBenchmarkOptions(userArguments);
  await benchmark(options);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
