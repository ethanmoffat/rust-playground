export interface BenchmarkOptions {
  runId: string;
  warmupSeconds: number;
  measureSeconds: number;
  captureSettleSeconds: number;
  drainTimeoutSeconds: number;
  requestTimeoutSeconds: number;
  concurrency: number;
  batchRows: number;
  seed: number;
}

const MAX_DURATION_SECONDS = 2_147_483_647;

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
  for (let index = 0; index < args.length; index++) {
    const inline = /^(--[a-z-]+)=(.*)$/u.exec(args[index]);
    const option = inline?.[1] ?? args[index];
    const value = inline?.[2] ?? args[++index];
    if (value === undefined || value === '') throw new Error(`${option} requires a value`);
    if (option === '--warmup') warmup = value;
    else if (option === '--concurrency') concurrency = value;
    else if (option === '--batch-rows') batchRows = value;
    else throw new Error(`Unknown option '${option}'. Options: ${benchmarkUsage}`);
  }
  return {
    runId: 'latest',
    warmupSeconds: parseDuration(warmup, '--warmup'),
    measureSeconds: 300,
    captureSettleSeconds: 30,
    drainTimeoutSeconds: 120,
    requestTimeoutSeconds: 60,
    concurrency: positiveInteger(concurrency, '--concurrency'),
    batchRows: positiveInteger(batchRows, '--batch-rows'),
    seed: 20260923,
  };
}

export const benchmarkUsage = '[--warmup 30s] [--concurrency 4] [--batch-rows 8192]';
