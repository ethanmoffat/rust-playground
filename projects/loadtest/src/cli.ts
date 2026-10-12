export interface BenchmarkOptions {
  runId: string;
  warmupSeconds: number;
  measureSeconds: number;
  captureSettleSeconds: number;
  drainTimeoutSeconds: number;
  requestTimeoutSeconds: number;
  continuous: boolean;
  keepStack: boolean;
  lanes: number;
  batchRows: number;
  seed: number;
  scenarios: string[];
  otelBaseline: boolean;
}

export function shouldRunIndependentValidation(options: Pick<BenchmarkOptions, 'continuous'>): boolean {
  return !options.continuous;
}

export function shouldStartRuntimeObservability(environment: NodeJS.ProcessEnv = process.env): boolean {
  const configured = environment.LOADTEST_OBSERVABILITY;
  if (configured !== undefined && configured !== '') {
    if (configured.toLowerCase() === 'true') return true;
    if (configured.toLowerCase() === 'false') return false;
    throw new Error('LOADTEST_OBSERVABILITY must be true or false');
  }
  return environment.CI_INTEGRATION !== 'true';
}

const MAX_DURATION_SECONDS = 2_147_483_647;

export function parseDuration(value: string, name: string, allowZero: boolean): number {
  const match = /^([0-9]+)([smh]?)$/iu.exec(value);
  if (!match) {
    throw new Error(`${name} must be integer seconds or a whole-number duration such as 30s, 2m, or 1h`);
  }
  const multiplier = match[2].toLowerCase() === 'h' ? 3600 : match[2].toLowerCase() === 'm' ? 60 : 1;
  const magnitude = Number(match[1]);
  const seconds = magnitude * multiplier;
  if (!Number.isSafeInteger(seconds) || seconds > MAX_DURATION_SECONDS || (!allowZero && seconds === 0)) {
    throw new Error(`${name} is outside its supported ${allowZero ? '0..' : '1..'}${MAX_DURATION_SECONDS} second range`);
  }
  return seconds;
}

function positiveInteger(value: string, name: string): number {
  if (!/^[0-9]+$/u.test(value)) throw new Error(`${name} must be a positive integer`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) throw new Error(`${name} must be a positive integer`);
  return parsed;
}

function nonnegativeInteger(value: string, name: string): number {
  if (!/^[0-9]+$/u.test(value)) throw new Error(`${name} must be a non-negative integer`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) throw new Error(`${name} must be a non-negative integer`);
  return parsed;
}

function booleanEnvironment(value: string | undefined, name: string, fallback: boolean): boolean {
  if (value === undefined || value === '') return fallback;
  if (value.toLowerCase() === 'true') return true;
  if (value.toLowerCase() === 'false') return false;
  throw new Error(`${name} must be true or false`);
}

function scenarios(value: string): string[] {
  const parsed = [...new Set(value.split(',').map((entry) => entry.trim()).filter(Boolean))];
  const supported = new Set(['direct', 'dataflow', 'paired']);
  if (parsed.length === 0 || parsed.some((entry) => !supported.has(entry))) {
    throw new Error('LOADTEST_SCENARIOS must contain direct, dataflow, and/or paired');
  }
  return parsed;
}

function defaultRunId(now: Date): string {
  return `run-${now.toISOString().replace(/[-:.]/gu, '').replace('T', '-').replace('Z', '')}`;
}

function validateRunId(value: string): string {
  if (!/^[A-Za-z0-9._-]{1,128}$/u.test(value) || value === '.' || value === '..') {
    throw new Error('--run-id must use 1..128 ASCII letters, digits, dot, underscore, or hyphen');
  }
  return value;
}

export function parseBenchmarkOptions(
  args: string[],
  environment: NodeJS.ProcessEnv = process.env,
  now = new Date(),
): BenchmarkOptions {
  const values = new Map<string, string>();
  let continuous = booleanEnvironment(environment.LOADTEST_CONTINUOUS, 'LOADTEST_CONTINUOUS', false);
  let keepStack = booleanEnvironment(environment.LOADTEST_KEEP_STACK, 'LOADTEST_KEEP_STACK', false);
  let otelBaseline = booleanEnvironment(environment.LOADTEST_OTEL_BASELINE, 'LOADTEST_OTEL_BASELINE', false);
  for (let index = 0; index < args.length; index++) {
    const argument = args[index];
    if (argument === '--continuous' || argument === '--keep-stack' || argument === '--otel-baseline') {
      if (argument === '--continuous') continuous = true;
      if (argument === '--keep-stack') keepStack = true;
      if (argument === '--otel-baseline') otelBaseline = true;
      continue;
    }
    if (/^--(?:continuous|keep-stack|otel-baseline)=/u.test(argument)) {
      throw new Error(`${argument.split('=')[0]} does not take a value`);
    }
    const inline = /^(--[a-z-]+)=(.*)$/u.exec(argument);
    const option = inline?.[1] ?? argument;
    if (!option.startsWith('--')) throw new Error(`Unexpected argument '${argument}'`);
    const supported = new Set([
      '--run-id', '--warmup', '--measure', '--capture-settle', '--drain-timeout',
      '--request-timeout', '--lanes', '--batch-rows', '--seed', '--scenarios',
    ]);
    if (!supported.has(option)) throw new Error(`Unknown option '${option}'`);
    const value = inline?.[2] ?? args[++index];
    if (value === undefined || value === '' || (!inline && value.startsWith('--'))) {
      throw new Error(`${option} requires a value`);
    }
    values.set(option, value);
  }
  const get = (option: string, env: string, fallback: string): string =>
    values.get(option) ?? environment[env] ?? fallback;
  const runId = validateRunId(get('--run-id', 'LOADTEST_RUN_ID', defaultRunId(now)));
  const selectedScenarios = continuous
    ? ['paired']
    : scenarios(get('--scenarios', 'LOADTEST_SCENARIOS', 'direct,dataflow,paired'));
  return {
    runId,
    warmupSeconds: parseDuration(get('--warmup', 'LOADTEST_WARMUP', '2m'), '--warmup', true),
    measureSeconds: parseDuration(get('--measure', 'LOADTEST_MEASURE', '5m'), '--measure', false),
    captureSettleSeconds: parseDuration(
      get('--capture-settle', 'LOADTEST_CAPTURE_SETTLE', '30s'), '--capture-settle', true,
    ),
    drainTimeoutSeconds: parseDuration(
      get('--drain-timeout', 'LOADTEST_DRAIN_TIMEOUT', '2m'), '--drain-timeout', false,
    ),
    requestTimeoutSeconds: parseDuration(
      get('--request-timeout', 'LOADTEST_REQUEST_TIMEOUT', '1m'), '--request-timeout', false,
    ),
    continuous,
    keepStack,
    lanes: positiveInteger(get('--lanes', 'LOADTEST_LANES', '4'), '--lanes'),
    batchRows: positiveInteger(get('--batch-rows', 'LOADTEST_BATCH_ROWS', '8192'), '--batch-rows'),
    seed: nonnegativeInteger(get('--seed', 'LOADTEST_SEED', '20260923'), '--seed'),
    scenarios: selectedScenarios,
    otelBaseline,
  };
}

export const benchmarkUsage = `Usage: loadtest <benchmark|run> [options]

Options:
  --run-id <id>
  --warmup <duration>
  --measure <duration>
  --capture-settle <duration>
  --drain-timeout <duration>
  --request-timeout <duration>
  --lanes <count>
  --batch-rows <count>
  --seed <integer>
  --scenarios <direct,dataflow,paired>
  --continuous
  --keep-stack
  --otel-baseline

Durations accept integer seconds, 30s, 2m, or 1h. CLI values override
LOADTEST_* environment values, which override built-in defaults. Continuous
mode always runs paired Arrow traffic and ignores the measurement duration.`;
