export interface BenchmarkOptions {
  runId: string;
  warmupSeconds: number;
  measureSeconds?: number;
  captureSettleSeconds: number;
  drainTimeoutSeconds: number;
  requestTimeoutSeconds: number;
  concurrency: number;
  batchRows: number;
  seed: number;
  validate: boolean;
}

const MAX_DURATION_SECONDS = 2_147_483_647;

export function validateRunId(value: string): string {
  if (!/^[a-z0-9][a-z0-9_-]{0,37}$/u.test(value)) {
    throw new Error('--run-id must be 1–38 lowercase ASCII letters, digits, underscores or hyphens, starting with a letter or digit');
  }
  return value;
}

export function benchmarkProject(runId: string): string {
  return `rust-playground-loadtest-${validateRunId(runId)}`;
}

export function parseDuration(value: string, name: string): number {
  const match = /^([0-9]+)([smh]?)$/iu.exec(value);
  if (!match) throw new Error(`${name} must look like 30, 30s, 2m, or 1h`);
  const unit = match[2].toLowerCase();
  const seconds = Number(match[1]) * (unit === 'h' ? 3600 : unit === 'm' ? 60 : 1);
  if (!Number.isSafeInteger(seconds) || seconds > MAX_DURATION_SECONDS) throw new Error(`${name} is out of range`);
  return seconds;
}

function positiveInteger(value: string, name: string): number {
  const parsed = Number(value);
  if (!/^[0-9]+$/u.test(value) || !Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

export function parseBenchmarkOptions(args: string[]): BenchmarkOptions {
  let warmup = '30s';
  let concurrency = '4';
  let batchRows = '8192';
  let measure: string | undefined;
  let runId = 'latest';
  let validate = false;
  for (let index = 0; index < args.length; index++) {
    if (args[index] === '--validate') {
      validate = true;
      continue;
    }
    const inline = /^(--[a-z-]+)=(.*)$/u.exec(args[index]);
    const option = inline?.[1] ?? args[index];
    const value = inline?.[2] ?? args[++index];
    if (value === undefined || value === '') throw new Error(`${option} requires a value`);
    if (option === '--warmup') warmup = value;
    else if (option === '--concurrency') concurrency = value;
    else if (option === '--measure') measure = value;
    else if (option === '--batch-rows') batchRows = value;
    else if (option === '--run-id') runId = validateRunId(value);
    else throw new Error(`Unknown option '${option}'. Options: ${benchmarkUsage}`);
  }
  return {
    runId,
    warmupSeconds: parseDuration(warmup, '--warmup'),
    measureSeconds: measure === undefined ? undefined : parseDuration(measure, '--measure'),
    captureSettleSeconds: 30,
    drainTimeoutSeconds: 120,
    requestTimeoutSeconds: 60,
    concurrency: positiveInteger(concurrency, '--concurrency'),
    batchRows: positiveInteger(batchRows, '--batch-rows'),
    seed: 20260923,
    validate,
  };
}

export const benchmarkUsage = '[--run-id latest (must be unused)] [--warmup 30s] [--measure 1m (omit to run until Ctrl+C)] [--concurrency 4] [--batch-rows 8192] [--validate]';
