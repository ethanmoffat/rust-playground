import { readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

export const kustoComposeStartArguments = [
  '--profile', 'benchmark', 'up', '--detach', '--no-build', '--no-deps', 'kusto',
];

export interface KustoStatus {
  state: 'healthy' | 'degraded';
  error?: string;
  live_observed: boolean;
  files_ingested: number;
}

export interface KustoStartup {
  state: 'healthy' | 'degraded';
  endpoint: string;
  startedAt: string;
  finishedAt: string;
  phase: 'start' | 'probe';
  error?: string;
}

export function safeKustoError(error: unknown): string {
  let text = error instanceof Error ? error.message : String(error);
  if (error instanceof Error && error.cause instanceof Error) text += ` (${error.cause.message})`;
  return text
    .replace(/https?:\/\/[^\s"'<>]+/gu, value => {
      try {
        const url = new URL(value);
        url.username = '';
        url.password = '';
        url.search = '';
        url.hash = '';
        return url.toString();
      } catch {
        return '[invalid URL]';
      }
    })
    .replace(/\b(Bearer|Basic)\s+[A-Za-z0-9+/_.=-]+/giu, '$1 [redacted]')
    .replace(/((?:password|passwd|secret|token|api[_-]?key|sig(?:nature)?)["']?\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,;}]+)/giu, '$1[redacted]')
    .replace(/[\u0000-\u001f\u007f]/gu, ' ')
    .replace(/\s+/gu, ' ')
    .trim()
    .slice(0, 2048);
}

function clusterIsReady(body: unknown): boolean {
  if (!body || typeof body !== 'object') return false;
  const document = body as Record<string, unknown>;
  if (document.error || document.HasErrors === true ||
      (Array.isArray(document.Exceptions) && document.Exceptions.length > 0)) return false;
  return Array.isArray(document.Tables) && document.Tables.some((table: unknown) => {
    if (!table || typeof table !== 'object') return false;
    const { Columns: columns, Rows: rows } = table as Record<string, unknown>;
    return Array.isArray(columns) && columns.some(column => column?.ColumnName === 'NodeId') &&
      Array.isArray(rows) && rows.some(row => Array.isArray(row) && row.length === columns.length);
  });
}

async function waitForKusto(endpoint: string, timeoutMs: number, requestTimeoutMs: number, intervalMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown = new Error('Kusto readiness deadline elapsed');
  let lastHttpStatus: number | undefined;
  do {
    try {
      const response = await fetch(new URL('/v1/rest/mgmt', endpoint), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ csl: '.show cluster' }),
        signal: AbortSignal.timeout(Math.max(1, Math.min(requestTimeoutMs, deadline - Date.now()))),
      });
      lastHttpStatus = response.status;
      if (!response.ok) {
        await response.body?.cancel();
        throw new Error(`Kusto .show cluster returned HTTP ${response.status}`);
      }
      let body: unknown;
      try {
        body = await response.json();
      } catch {
        throw new Error('Kusto .show cluster returned invalid JSON');
      }
      if (!clusterIsReady(body)) throw new Error('Kusto .show cluster did not return a ready cluster');
      return;
    } catch (error) {
      lastError = error;
    }
    const remaining = deadline - Date.now();
    if (remaining <= 0) break;
    await delay(Math.min(intervalMs, remaining));
  } while (Date.now() < deadline);
  throw new Error(`Kusto readiness failed: ${safeKustoError(lastError)}` +
    (lastHttpStatus === undefined ? '' : `; last response HTTP ${lastHttpStatus}`));
}

export async function startKusto(options: {
  endpoint: string;
  results: string;
  start: () => void | Promise<void>;
  timeoutMs?: number;
  requestTimeoutMs?: number;
  intervalMs?: number;
}): Promise<KustoStartup> {
  const startedAt = new Date().toISOString();
  let phase: KustoStartup['phase'] = 'start';
  let failed = false;
  let failure: unknown;
  try {
    await options.start();
    phase = 'probe';
    await waitForKusto(options.endpoint, options.timeoutMs ?? 120_000, options.requestTimeoutMs ?? 5_000, options.intervalMs ?? 1_000);
  } catch (error) {
    failed = true;
    failure = error;
  }
  const startup: KustoStartup = {
    state: failed ? 'degraded' : 'healthy',
    endpoint: options.endpoint,
    startedAt,
    finishedAt: new Date().toISOString(),
    phase,
    ...(failed ? { error: safeKustoError(failure) } : {}),
  };
  writeFileSync(path.join(options.results, 'kusto-startup.json'), `${JSON.stringify(startup, null, 2)}\n`);
  return startup;
}

export function readKustoStatus(results: string): KustoStatus {
  try {
    const status = JSON.parse(readFileSync(path.join(results, 'kusto-status.json'), 'utf8')) as Partial<KustoStatus> | null;
    if (!status || !['healthy', 'degraded'].includes(status.state ?? '') ||
        typeof status.live_observed !== 'boolean' || !Number.isSafeInteger(status.files_ingested) ||
        (status.files_ingested ?? -1) < 0 || (status.error !== undefined && typeof status.error !== 'string')) {
      throw new Error('Invalid Kusto synchronization status');
    }
    const error = status.error ? safeKustoError(status.error) : undefined;
    return {
      state: status.state === 'degraded' || error ? 'degraded' : 'healthy',
      live_observed: status.live_observed,
      files_ingested: status.files_ingested!,
      ...(error ? { error } : {}),
    };
  } catch {
    return {
      state: 'degraded', live_observed: false, files_ingested: 0,
      error: 'Kusto synchronization status is missing or invalid; inspect kusto-startup.json and compose.log',
    };
  }
}

export function kustoStatusSummary(status: KustoStatus): string {
  if (status.state === 'degraded') return `Kusto DEGRADED: ${status.error ?? 'synchronization failed'}`;
  return `Kusto healthy (${status.files_ingested} files ingested; live query ${status.live_observed ? 'observed' : 'NOT observed'})`;
}

export function requireHealthyKusto(status: KustoStatus): void {
  if (status.state !== 'healthy') throw new Error(kustoStatusSummary(status));
  if (!status.live_observed) throw new Error('Kusto validation failed: no in-run live query observation was recorded');
}
