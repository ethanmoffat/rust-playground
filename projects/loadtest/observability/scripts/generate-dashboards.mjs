#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptsDirectory = path.dirname(fileURLToPath(import.meta.url));
const observabilityDirectory = path.resolve(scriptsDirectory, '..');
const dashboardsDirectory = path.join(observabilityDirectory, 'grafana', 'dashboards');
const arrowDirectory = path.join(dashboardsDirectory, 'arrow-flight');
const otelDirectory = path.join(dashboardsDirectory, 'otel-baseline');

const prometheus = { type: 'prometheus', uid: 'prometheus' };
const loki = { type: 'loki', uid: 'loki' };
const tempo = { type: 'tempo', uid: 'tempo' };

let nextPanelId = 1;

function promTarget(expr, legendFormat, refId, options = {}) {
  return {
    datasource: prometheus,
    editorMode: 'code',
    expr,
    legendFormat,
    range: options.instant !== true,
    instant: options.instant === true,
    format: options.format ?? 'time_series',
    refId,
  };
}

function panelBase(type, title, gridPos, description = '') {
  return {
    datasource: prometheus,
    description,
    fieldConfig: {
      defaults: {
        color: { mode: 'palette-classic' },
        custom: {
          axisCenteredZero: false,
          axisColorMode: 'text',
          axisLabel: '',
          axisPlacement: 'auto',
          barAlignment: 0,
          drawStyle: 'line',
          fillOpacity: 12,
          gradientMode: 'none',
          hideFrom: { legend: false, tooltip: false, viz: false },
          lineInterpolation: 'linear',
          lineWidth: 1,
          pointSize: 4,
          scaleDistribution: { type: 'linear' },
          showPoints: 'never',
          spanNulls: false,
          stacking: { group: 'A', mode: 'none' },
          thresholdsStyle: { mode: 'off' },
        },
        mappings: [],
        thresholds: {
          mode: 'absolute',
          steps: [
            { color: 'green', value: null },
            { color: 'red', value: 80 },
          ],
        },
      },
      overrides: [],
    },
    gridPos,
    id: nextPanelId++,
    options: {
      legend: { calcs: ['lastNotNull'], displayMode: 'list', placement: 'bottom', showLegend: true },
      tooltip: { hideZeros: false, mode: 'multi', sort: 'desc' },
    },
    title,
    type,
  };
}

function timeseries(title, gridPos, targets, unit = 'short', description = '', overrides = []) {
  const panel = panelBase('timeseries', title, gridPos, description);
  panel.targets = targets;
  panel.fieldConfig.defaults.unit = unit;
  panel.fieldConfig.overrides = overrides;
  return panel;
}

function stat(title, gridPos, targets, unit = 'short', description = '') {
  const panel = panelBase('stat', title, gridPos, description);
  panel.targets = targets;
  panel.fieldConfig.defaults.unit = unit;
  panel.options = {
    colorMode: 'value',
    graphMode: 'area',
    justifyMode: 'auto',
    orientation: 'auto',
    reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false },
    showPercentChange: false,
    textMode: 'auto',
    wideLayout: true,
  };
  return panel;
}

function table(title, gridPos, targets, description = '') {
  const panel = panelBase('table', title, gridPos, description);
  panel.targets = targets;
  panel.options = {
    cellHeight: 'sm',
    footer: { countRows: false, fields: '', reducer: ['sum'], show: false },
    showHeader: true,
  };
  return panel;
}

function text(title, gridPos, content, description = '') {
  const panel = panelBase('text', title, gridPos, description);
  delete panel.datasource;
  delete panel.fieldConfig;
  panel.options = { code: { language: 'plaintext', showLineNumbers: false, showMiniMap: false }, content, mode: 'markdown' };
  return panel;
}

function logs(title, gridPos, expr, description = '') {
  const panel = panelBase('logs', title, gridPos, description);
  panel.datasource = loki;
  panel.targets = [{ datasource: loki, editorMode: 'code', expr, queryType: 'range', refId: 'A' }];
  panel.options = {
    dedupStrategy: 'none',
    enableLogDetails: true,
    prettifyLogMessage: false,
    showCommonLabels: false,
    showLabels: false,
    showTime: true,
    sortOrder: 'Descending',
    wrapLogMessage: true,
  };
  return panel;
}

function traces(title, gridPos, query, description = '') {
  const panel = panelBase('traces', title, gridPos, description);
  panel.datasource = tempo;
  panel.targets = [{
    datasource: tempo,
    limit: 50,
    query,
    queryType: 'traceql',
    refId: 'A',
    tableType: 'traces',
  }];
  panel.options = {};
  return panel;
}

function variable(name, label, query, regex, options = {}) {
  return {
    allValue: '.*',
    current: { selected: false, text: 'All', value: '$__all' },
    datasource: prometheus,
    definition: query,
    hide: 0,
    includeAll: true,
    label,
    multi: true,
    name,
    options: [],
    query: { query, refId: `variable-${name}` },
    refresh: options.refresh ?? 1,
    regex,
    skipUrlSync: false,
    sort: 1,
    type: 'query',
  };
}

function dashboardBase(uid, title, tags, variables) {
  return {
    annotations: {
      list: [{
        builtIn: 1,
        datasource: { type: 'grafana', uid: '-- Grafana --' },
        enable: true,
        hide: true,
        iconColor: 'rgba(0, 211, 255, 1)',
        name: 'Annotations & Alerts',
        type: 'dashboard',
      }],
    },
    editable: false,
    fiscalYearStartMonth: 0,
    graphTooltip: 1,
    id: null,
    links: [],
    liveNow: true,
    panels: [],
    refresh: '2s',
    schemaVersion: 42,
    tags,
    templating: { list: variables },
    time: { from: 'now-30m', to: 'now' },
    timepicker: {
      refresh_intervals: ['2s', '5s', '10s', '30s', '1m'],
      time_options: ['5m', '15m', '30m', '1h', '3h', '6h'],
    },
    timezone: 'browser',
    title,
    uid,
    version: 1,
    weekStart: '',
  };
}

const commonVariables = [
  variable(
    'run',
    'Run',
    'query_result(topk(20, count by (run_id) (up{run_id!=""})))',
    '/run_id="([^"]+)"/',
  ),
  variable(
    'scenario',
    'Scenario',
    'query_result(topk(10, count by (scenario) (up{run_id=~"$run",scenario!=""})))',
    '/scenario="([^"]+)"/',
  ),
];

function arrowDashboard() {
  nextPanelId = 1;
  const dashboard = dashboardBase(
    'arrow-flight-platform',
    'Arrow Flight Platform & Collector Comparison',
    ['arrow', 'flight', 'comparison', 'loadtest'],
    [
      ...commonVariables,
      variable(
        'table',
        'Table',
        'query_result(topk(50, count by (table) (arrowcol_sql_routing_lease_info{run_id=~"$run",scenario=~"$scenario"})))',
        '/table="([^"]+)"/',
      ),
      variable(
        'implementation_pool',
        'Implementation pool',
        'query_result(topk(10, count by (implementation_pool) (up{run_id=~"$run",scenario=~"$scenario",implementation_pool!=""})))',
        '/implementation_pool="([^"]+)"/',
      ),
      variable(
        'instance',
        'Instance',
        'query_result(topk(20, count by (instance) (up{run_id=~"$run",scenario=~"$scenario",instance!=""})))',
        '/instance="([^"]+)"/',
      ),
      variable(
        'service',
        'Container service',
        'query_result(topk(30, count by (container_label_com_docker_compose_service) (container_last_seen{run_id=~"$run",scenario=~"$scenario",container_label_com_docker_compose_service!=""})))',
        '/container_label_com_docker_compose_service="([^"]+)"/',
      ),
    ],
  );

  dashboard.annotations.list.push({
    datasource: prometheus,
    enable: true,
    expr: 'increase(arrowcol_router_lease_events_total{run_id=~"$run",scenario=~"$scenario"}[10s]) > 0',
    iconColor: 'orange',
    name: 'Router lease events',
    tagKeys: 'event',
    target: {
      datasource: prometheus,
      expr: 'increase(arrowcol_router_lease_events_total{run_id=~"$run",scenario=~"$scenario"}[10s]) > 0',
      refId: 'router-lease-events',
    },
  });

  dashboard.panels = [
    text(
      'Arrow Flight comparison scope',
      { h: 4, w: 24, x: 0, y: 0 },
      [
        'This is the home dashboard for the router, three **direct** collectors, three **DFE** collectors,',
        'the load driver, SQL/Hive dependencies, and container resources.',
        '',
        '- Metrics never use receipt IDs as labels.',
        '- Container network receive bytes are a wire-traffic proxy, not decoded Arrow size.',
        '- Exact rendezvous weights, direct flush reasons, and configured file targets are not exported;',
        '  the relevant panels state that limitation instead of inventing series.',
        '- Click a Loki-derived TraceID to open Tempo when a service emits a trace identifier.',
      ].join('\n'),
    ),
    stat(
      'Service scrape health',
      { h: 5, w: 6, x: 0, y: 4 },
      [promTarget(
        'min by (job, instance) (up{run_id=~"$run",scenario=~"$scenario",job=~"arrow-router|arrow-direct|arrow-dataflow|arrow-sql-control-plane|cadvisor"})',
        '{{job}} / {{instance}}',
        'A',
      )],
      'short',
      '1 means the metrics target is reachable. The optional OTEL baseline is intentionally excluded.',
    ),
    timeseries(
      'Throughput: rows/s',
      { h: 5, w: 9, x: 6, y: 4 },
      [promTarget(
        'sum by (implementation_pool) (arrowcol:received_rows_per_second{run_id=~"$run",scenario=~"$scenario",implementation_pool=~"$implementation_pool",instance=~"$instance"})',
        '{{implementation_pool}}',
        'A',
      )],
      'rows/s',
      'Collector-admitted rows per second. Direct and DFE use their checked-in Prometheus counters.',
    ),
    timeseries(
      'Arrow logical bytes/s',
      { h: 5, w: 9, x: 15, y: 4 },
      [promTarget(
        'sum by (implementation_pool) (arrowcol:received_bytes_per_second{run_id=~"$run",scenario=~"$scenario",implementation_pool=~"$implementation_pool",instance=~"$instance"})',
        '{{implementation_pool}}',
        'A',
      )],
      'Bps',
      'Logical Arrow bytes admitted by collector implementations.',
    ),
    timeseries(
      'Wire bytes/s by container (proxy)',
      { h: 6, w: 12, x: 0, y: 9 },
      [promTarget(
        'sum by (container_label_com_docker_compose_service) (arrowcol:container_network_receive_bytes_per_second{run_id=~"$run",scenario=~"$scenario",container_label_com_docker_compose_service=~"$service"})',
        '{{container_label_com_docker_compose_service}}',
        'A',
      )],
      'Bps',
      'cAdvisor container receive bytes. This is the available wire proxy; protocol/header overhead is included.',
    ),
    timeseries(
      'Active streams',
      { h: 6, w: 12, x: 12, y: 9 },
      [promTarget(
        'sum by (implementation_pool, instance) (arrowcol:active_streams{run_id=~"$run",scenario=~"$scenario",implementation_pool=~"$implementation_pool",instance=~"$instance"})',
        '{{implementation_pool}} / {{instance}}',
        'A',
      )],
      'short',
      'Active streams at router and collector boundaries.',
    ),
    timeseries(
      'Routing decisions',
      { h: 6, w: 12, x: 0, y: 15 },
      [promTarget(
        'sum by (operation, pool, source) (rate(arrowcol_router_decisions_total{run_id=~"$run",scenario=~"$scenario"}[$__rate_interval]))',
        '{{operation}} / {{pool}} / {{source}}',
        'A',
      )],
      'ops',
      'Router decision rate by operation, implementation pool, and decision source.',
    ),
    timeseries(
      'Router failures',
      { h: 6, w: 12, x: 12, y: 15 },
      [promTarget(
        'sum by (operation, pool, reason) (rate(arrowcol_router_failures_total{run_id=~"$run",scenario=~"$scenario"}[$__rate_interval]))',
        '{{operation}} / {{pool}} / {{reason}}',
        'A',
      )],
      'ops',
      'Structured router failures, including closed admission and dependency failures.',
    ),
    table(
      'Table ownership',
      { h: 6, w: 24, x: 0, y: 21 },
      [promTarget(
        'arrowcol_sql_routing_lease_info{run_id=~"$run",scenario=~"$scenario",implementation_pool=~"$implementation_pool",table=~"$table"}',
        '',
        'A',
        { instant: true, format: 'table' },
      )],
      'Current SQL-backed routing ownership. UUID lease IDs are intentionally not metric labels.',
    ),
    timeseries(
      'Routing lease age',
      { h: 6, w: 8, x: 0, y: 27 },
      [promTarget(
        'arrowcol_sql_routing_lease_age_seconds{run_id=~"$run",scenario=~"$scenario",implementation_pool=~"$implementation_pool",table=~"$table"}',
        '{{implementation_pool}} / {{table}} / {{holder_instance}}',
        'A',
      )],
      's',
    ),
    timeseries(
      'Lease churn',
      { h: 6, w: 8, x: 8, y: 27 },
      [promTarget(
        'sum by (implementation_pool, table) (changes(arrowcol_sql_routing_lease_version{run_id=~"$run",scenario=~"$scenario",implementation_pool=~"$implementation_pool",table=~"$table"}[$__range]))',
        '{{implementation_pool}} / {{table}}',
        'A',
      )],
      'short',
      'Lease-version changes over the selected dashboard range.',
    ),
    timeseries(
      'Collector pressure / staleness',
      { h: 6, w: 8, x: 16, y: 27 },
      [
        promTarget(
          'arrowcol_sql_collector_maximum_pressure_ratio{run_id=~"$run",scenario=~"$scenario",implementation_pool=~"$implementation_pool",instance=~"$instance"}',
          '{{implementation_pool}} / {{instance}} pressure',
          'A',
        ),
        promTarget(
          'arrowcol_sql_pressure_snapshot_age_seconds{run_id=~"$run",scenario=~"$scenario",implementation_pool=~"$implementation_pool",instance=~"$instance"}',
          '{{implementation_pool}} / {{instance}} age',
          'B',
        ),
      ],
      'short',
      'Maximum bounded pressure input and snapshot age. The router does not export its final rendezvous weight.',
      [{
        matcher: { id: 'byRegexp', options: '/.*age/' },
        properties: [{ id: 'unit', value: 's' }],
      }],
    ),
    timeseries(
      'Direct receipts by state',
      { h: 6, w: 12, x: 0, y: 33 },
      [promTarget(
        'sum by (receipt_state, table) (arrowcol_sql_direct_receipts{run_id=~"$run",scenario=~"$scenario",implementation_pool="direct",instance=~"$instance",table=~"$table"})',
        '{{receipt_state}} / {{table}}',
        'A',
      )],
      'short',
      'Active or recently updated direct SQL delivery receipts. Receipt IDs are never labels.',
    ),
    timeseries(
      'Direct receipt time-to-publish',
      { h: 6, w: 12, x: 12, y: 33 },
      [promTarget(
        'arrowcol_sql_direct_receipt_time_to_publish_seconds{run_id=~"$run",scenario=~"$scenario",instance=~"$instance",table=~"$table",quantile=~"p50|p95|p99"}',
        '{{quantile}} / {{instance}} / {{table}}',
        'A',
      )],
      's',
      'Creation-to-durable-publication quantiles over the last six hours.',
    ),
    timeseries(
      'Direct receipt failures',
      { h: 6, w: 12, x: 0, y: 39 },
      [promTarget(
        'sum by (failure_disposition, error_code, table) (increase(arrowcol_sql_direct_receipt_failures_total{run_id=~"$run",scenario=~"$scenario",instance=~"$instance",table=~"$table"}[$__range]))',
        '{{failure_disposition}} / {{error_code}} / {{table}}',
        'A',
      )],
      'short',
    ),
    timeseries(
      'Direct durable rows/s',
      { h: 6, w: 12, x: 12, y: 39 },
      [promTarget(
        'sum by (instance, table) (rate(arrowcol_sql_direct_rows_published_total{run_id=~"$run",scenario=~"$scenario",instance=~"$instance",table=~"$table"}[$__rate_interval]))',
        '{{instance}} / {{table}}',
        'A',
      )],
      'rows/s',
    ),
    timeseries(
      'Buffer rows',
      { h: 6, w: 8, x: 0, y: 45 },
      [promTarget(
        'arrowcol_sql_table_buffered_rows{run_id=~"$run",scenario=~"$scenario",implementation_pool=~"$implementation_pool",instance=~"$instance",table=~"$table"}',
        '{{implementation_pool}} / {{instance}} / {{table}}',
        'A',
      )],
      'rows',
    ),
    timeseries(
      'Buffer bytes',
      { h: 6, w: 8, x: 8, y: 45 },
      [promTarget(
        'arrowcol_sql_table_buffered_bytes{run_id=~"$run",scenario=~"$scenario",implementation_pool=~"$implementation_pool",instance=~"$instance",table=~"$table"}',
        '{{implementation_pool}} / {{instance}} / {{table}}',
        'A',
      )],
      'bytes',
    ),
    timeseries(
      'Oldest buffer age',
      { h: 6, w: 8, x: 16, y: 45 },
      [promTarget(
        'arrowcol_sql_table_oldest_buffer_age_seconds{run_id=~"$run",scenario=~"$scenario",implementation_pool=~"$implementation_pool",instance=~"$instance",table=~"$table"}',
        '{{implementation_pool}} / {{instance}} / {{table}}',
        'A',
      )],
      's',
    ),
    text(
      'Flush reasons — unavailable',
      { h: 6, w: 8, x: 0, y: 51 },
      [
        '### Unsupported metric',
        '',
        'The direct buffer has typed flush reasons in code, but no Prometheus or SQL observation exports them.',
        'This dashboard deliberately does **not** infer a reason from size or age.',
      ].join('\n'),
    ),
    timeseries(
      'File size actuals',
      { h: 6, w: 8, x: 8, y: 51 },
      [promTarget(
        'sum by (instance, table) (rate(arrowcol_sql_direct_bytes_published_total{run_id=~"$run",scenario=~"$scenario",instance=~"$instance",table=~"$table"}[$__rate_interval])) / clamp_min(sum by (instance, table) (rate(arrowcol_sql_direct_files_added_total{run_id=~"$run",scenario=~"$scenario",instance=~"$instance",table=~"$table"}[$__rate_interval])), 1e-9)',
        '{{instance}} / {{table}} actual bytes per file',
        'A',
      )],
      'bytes',
      'Observed Parquet bytes per added data file. The configured adaptive target is not exported, so no target line is fabricated.',
    ),
    timeseries(
      'Delta commit latency',
      { h: 6, w: 8, x: 16, y: 51 },
      [promTarget(
        'arrowcol_sql_direct_delta_commit_latency_seconds{run_id=~"$run",scenario=~"$scenario",instance=~"$instance",table=~"$table",quantile=~"p50|p95|p99"}',
        '{{quantile}} / {{instance}} / {{table}}',
        'A',
      )],
      's',
      'Direct receipt flushing-to-published event latency over the last six hours.',
    ),
    timeseries(
      'DFE channel occupancy',
      { h: 6, w: 12, x: 0, y: 57 },
      [
        promTarget(
          'arrowcol_dfe_channel_occupancy{run_id=~"$run",scenario=~"$scenario",instance=~"$instance"}',
          '{{instance}} occupancy',
          'A',
        ),
        promTarget(
          'arrowcol_dfe_channel_capacity{run_id=~"$run",scenario=~"$scenario",instance=~"$instance"}',
          '{{instance}} capacity',
          'B',
        ),
      ],
      'short',
      'Ack-tracked DFE occupancy and configured bounded channel capacity from /v1/pressure.',
    ),
    timeseries(
      'DFE downstream Ack/Nack',
      { h: 6, w: 12, x: 12, y: 57 },
      [
        promTarget(
          'sum by (instance) (rate(arrowcol_downstream_acks_total{run_id=~"$run",scenario=~"$scenario",instance=~"$instance"}[$__rate_interval]))',
          '{{instance}} ack',
          'A',
        ),
        promTarget(
          'sum by (instance) (rate(arrowcol_downstream_nacks_total{run_id=~"$run",scenario=~"$scenario",instance=~"$instance"}[$__rate_interval]))',
          '{{instance}} nack',
          'B',
        ),
      ],
      'ops',
    ),
    stat(
      'DFE delivery capability',
      { h: 6, w: 8, x: 0, y: 63 },
      [
        promTarget(
          'min by (instance) (arrowcol_dfe_arbitrary_arrow_supported{run_id=~"$run",scenario=~"$scenario",instance=~"$instance"})',
          '{{instance}} arbitrary Arrow',
          'A',
        ),
        promTarget(
          'min by (instance) (arrowcol_dfe_at_least_once_supported{run_id=~"$run",scenario=~"$scenario",instance=~"$instance"})',
          '{{instance}} at-least-once',
          'B',
        ),
      ],
      'bool',
      'Arbitrary registered Arrow is supported; durable completion/at-least-once is not.',
    ),
    text(
      'Unsupported: DFE receipts',
      { h: 6, w: 8, x: 8, y: 63 },
      [
        '### No SQL delivery receipts',
        '',
        'The DFE pool returns **best-effort admission** only. It has no receipt polling endpoint,',
        'no SQL delivery receipt, and no at-least-once claim. Direct-receipt panels exclude DFE.',
      ].join('\n'),
    ),
    text(
      'Unsupported: arbitrary-Arrow native Quiver',
      { h: 6, w: 8, x: 16, y: 63 },
      [
        '### Capability gap',
        '',
        'Native Quiver cannot persist/reconstruct arbitrary Arrow representation metadata, table identity,',
        'schema lineage, and receipt semantics. Configuration is rejected rather than silently converting to OTAP.',
      ].join('\n'),
    ),
    timeseries(
      'Schema failures',
      { h: 6, w: 12, x: 0, y: 69 },
      [
        promTarget(
          'sum by (saga_state, operation_kind, error_code, table) (increase(arrowcol_sql_schema_saga_failures_total{run_id=~"$run",scenario=~"$scenario",table=~"$table"}[$__range]))',
          '{{saga_state}} / {{operation_kind}} / {{error_code}} / {{table}}',
          'A',
        ),
        promTarget(
          'sum by (outcome, instance) (increase(arrowcol_direct_stream_outcomes_total{run_id=~"$run",scenario=~"$scenario",instance=~"$instance",outcome=~"decode_failed|admission_failed"}[$__range]))',
          '{{outcome}} / {{instance}}',
          'B',
        ),
      ],
      'short',
    ),
    timeseries(
      'Schema versions and compatibility',
      { h: 6, w: 12, x: 12, y: 69 },
      [promTarget(
        'sum by (compatibility, schema_state, table) (arrowcol_sql_schema_versions{run_id=~"$run",scenario=~"$scenario",table=~"$table"})',
        '{{compatibility}} / {{schema_state}} / {{table}}',
        'A',
      )],
      'short',
    ),
    timeseries(
      'CPU utilization',
      { h: 6, w: 8, x: 0, y: 75 },
      [promTarget(
        'arrowcol:container_cpu_cores{run_id=~"$run",scenario=~"$scenario",container_label_com_docker_compose_service=~"$service"}',
        '{{container_label_com_docker_compose_service}}',
        'A',
      )],
      'cores',
    ),
    timeseries(
      'RSS',
      { h: 6, w: 8, x: 8, y: 75 },
      [promTarget(
        'arrowcol:container_rss_bytes{run_id=~"$run",scenario=~"$scenario",container_label_com_docker_compose_service=~"$service"}',
        '{{container_label_com_docker_compose_service}}',
        'A',
      )],
      'bytes',
    ),
    timeseries(
      'Disk I/O',
      { h: 6, w: 8, x: 16, y: 75 },
      [promTarget(
        'sum by (container_label_com_docker_compose_service) (rate(container_fs_reads_bytes_total{run_id=~"$run",scenario=~"$scenario",container_label_com_docker_compose_service=~"$service"}[$__rate_interval]) + rate(container_fs_writes_bytes_total{run_id=~"$run",scenario=~"$scenario",container_label_com_docker_compose_service=~"$service"}[$__rate_interval]))',
        '{{container_label_com_docker_compose_service}}',
        'A',
      )],
      'Bps',
    ),
    stat(
      'SQL / Hive health',
      { h: 5, w: 8, x: 0, y: 81 },
      [promTarget(
        'probe_success{run_id=~"$run",scenario=~"$scenario",job="dependency-tcp",component=~"sqlserver|hive-metastore"}',
        '{{component}}',
        'A',
      )],
      'bool',
      'TCP reachability only. No privileged SQL/Hive internal exporter is claimed.',
    ),
    timeseries(
      'Container health',
      { h: 5, w: 8, x: 8, y: 81 },
      [promTarget(
        'time() - max by (container_label_com_docker_compose_service) (container_last_seen{run_id=~"$run",scenario=~"$scenario",container_label_com_docker_compose_service=~"$service"})',
        '{{container_label_com_docker_compose_service}} seconds since seen',
        'A',
      )],
      's',
      'Seconds since cAdvisor last observed each selected container.',
    ),
    stat(
      'Load driver / target health',
      { h: 5, w: 8, x: 16, y: 81 },
      [
        promTarget(
          'clamp_max(count(container_last_seen{run_id=~"$run",scenario=~"$scenario",container_label_com_docker_compose_service="load-driver"}), 1) or vector(0)',
          'load-driver container',
          'A',
        ),
        promTarget(
          'min by (component) (probe_success{run_id=~"$run",scenario=~"$scenario",job="service-readiness"})',
          '{{component}} readiness',
          'B',
        ),
      ],
      'bool',
      'The load driver is observed through cAdvisor because it does not expose a Prometheus endpoint.',
    ),
    logs(
      'Service logs',
      { h: 8, w: 24, x: 0, y: 86 },
      '{run_id=~"$run",scenario=~"$scenario",service_name=~"$service"}',
      'Docker logs from the loadtest services. Trace IDs remain log content/structured metadata, not metric labels.',
    ),
    traces(
      'Recent traces (when emitted)',
      { h: 8, w: 24, x: 0, y: 94 },
      '{ resource.service.name =~ `${service:regex}` }',
      'Tempo is empty for services that do not emit OTLP traces. Loki TraceID derived fields link here when IDs are present.',
    ),
  ];

  return dashboard;
}

function otelDashboard() {
  nextPanelId = 1;
  const dashboard = dashboardBase(
    'otel-collector-baseline',
    'OTEL Collector Baseline',
    ['otel', 'baseline', 'loadtest'],
    [
      ...commonVariables,
      variable(
        'signal',
        'Signal',
        'query_result(topk(10, count by (signal) (messages_total{job="otel-baseline",run_id=~"$run",scenario=~"$scenario",signal!=""})))',
        '/signal="([^"]+)"/',
      ),
      variable(
        'scope',
        'Metric scope',
        'query_result(topk(30, count by (otel_scope_name) ({job="otel-baseline",run_id=~"$run",scenario=~"$scenario",otel_scope_name!=""})))',
        '/otel_scope_name="([^"]+)"/',
      ),
    ],
  );

  dashboard.panels = [
    text(
      'Baseline scope',
      { h: 4, w: 24, x: 0, y: 0 },
      [
        'This folder is intentionally separate from **Arrow Flight**.',
        '',
        'It shows only the optional `otelcol-rust` baseline and the pinned DFE engine admin metrics.',
        'If the baseline service is not part of the main stack, `up{job="otel-baseline"}` is expected to be 0.',
      ].join('\n'),
    ),
    stat(
      'OTEL baseline target health',
      { h: 5, w: 6, x: 0, y: 4 },
      [promTarget(
        'up{job="otel-baseline",run_id=~"$run",scenario=~"$scenario"}',
        '{{instance}}',
        'A',
      )],
      'bool',
    ),
    timeseries(
      'OTEL message throughput',
      { h: 5, w: 9, x: 6, y: 4 },
      [promTarget(
        'sum by (otel_scope_name, signal, outcome) (rate(messages_total{job="otel-baseline",run_id=~"$run",scenario=~"$scenario",otel_scope_name=~"$scope",signal=~"$signal"}[$__rate_interval]))',
        '{{otel_scope_name}} / {{signal}} / {{outcome}}',
        'A',
      )],
      'ops',
      'DFE engine message counters. Zero-valued metric sets may be omitted until activity occurs.',
    ),
    timeseries(
      'OTEL channel queue depth',
      { h: 5, w: 9, x: 15, y: 4 },
      [
        promTarget(
          'queue_depth{job="otel-baseline",run_id=~"$run",scenario=~"$scenario",otel_scope_name="channel.receiver"}',
          '{{otel_scope_node_id}} depth',
          'A',
        ),
        promTarget(
          'capacity{job="otel-baseline",run_id=~"$run",scenario=~"$scenario",otel_scope_name="channel.receiver"}',
          '{{otel_scope_node_id}} capacity',
          'B',
        ),
      ],
      'short',
    ),
    timeseries(
      'OTEL CPU / RSS',
      { h: 6, w: 12, x: 0, y: 9 },
      [
        promTarget(
          'cpu_utilization{job="otel-baseline",run_id=~"$run",scenario=~"$scenario",otel_scope_name="engine"}',
          'CPU utilization',
          'A',
        ),
        promTarget(
          'memory_rss_bytes{job="otel-baseline",run_id=~"$run",scenario=~"$scenario",otel_scope_name="engine"}',
          'RSS bytes',
          'B',
        ),
      ],
      'short',
      'Engine self-observability. RSS is bytes; CPU is a 0–1 ratio.',
      [
        {
          matcher: { id: 'byName', options: 'RSS bytes' },
          properties: [{ id: 'unit', value: 'bytes' }],
        },
        {
          matcher: { id: 'byName', options: 'CPU utilization' },
          properties: [{ id: 'unit', value: 'percentunit' }],
        },
      ],
    ),
    timeseries(
      'OTEL pipeline CPU',
      { h: 6, w: 12, x: 12, y: 9 },
      [promTarget(
        'cpu_utilization{job="otel-baseline",run_id=~"$run",scenario=~"$scenario",otel_scope_name="pipeline"}',
        '{{otel_scope_pipeline_name}} / {{otel_scope_core_id}}',
        'A',
      )],
      'percentunit',
    ),
    timeseries(
      'OTEL exporter attempts',
      { h: 6, w: 12, x: 0, y: 15 },
      [promTarget(
        'sum by (signal, outcome) (rate(messages_total{job="otel-baseline",run_id=~"$run",scenario=~"$scenario",otel_scope_name="exporter.attempted",signal=~"$signal"}[$__rate_interval]))',
        '{{signal}} / {{outcome}}',
        'A',
      )],
      'ops',
    ),
    timeseries(
      'OTEL exporter attempt latency',
      { h: 6, w: 12, x: 12, y: 15 },
      [
        promTarget(
          'histogram_quantile(0.95, sum by (le, otel_scope_name) (rate(duration_seconds_bucket{job="otel-baseline",run_id=~"$run",scenario=~"$scenario",otel_scope_name=~"exporter.attempted|node.completion"}[$__rate_interval])))',
          '{{otel_scope_name}} p95 classic',
          'A',
        ),
        promTarget(
          'histogram_quantile(0.95, sum by (otel_scope_name) (rate(duration_seconds{job="otel-baseline",run_id=~"$run",scenario=~"$scenario",otel_scope_name=~"exporter.attempted|node.completion"}[$__rate_interval])))',
          '{{otel_scope_name}} p95 native',
          'B',
        ),
      ],
      's',
      'Classic and native histogram forms are queried separately; only the emitted form is shown.',
    ),
    timeseries(
      'Baseline container CPU',
      { h: 6, w: 12, x: 0, y: 21 },
      [promTarget(
        'arrowcol:container_cpu_cores{run_id=~"$run",scenario=~"$scenario",container_label_com_docker_compose_service="otelcol-rust"}',
        'otelcol-rust',
        'A',
      )],
      'cores',
    ),
    timeseries(
      'Baseline container RSS',
      { h: 6, w: 12, x: 12, y: 21 },
      [promTarget(
        'arrowcol:container_rss_bytes{run_id=~"$run",scenario=~"$scenario",container_label_com_docker_compose_service="otelcol-rust"}',
        'otelcol-rust',
        'A',
      )],
      'bytes',
    ),
    logs(
      'OTEL baseline logs',
      { h: 8, w: 24, x: 0, y: 27 },
      '{run_id=~"$run",scenario=~"$scenario",service_name="otelcol-rust"}',
    ),
    traces(
      'OTEL baseline traces (when emitted)',
      { h: 8, w: 24, x: 0, y: 35 },
      '{ resource.service.name = `otelcol-rust` }',
    ),
  ];

  return dashboard;
}

fs.mkdirSync(arrowDirectory, { recursive: true });
fs.mkdirSync(otelDirectory, { recursive: true });
fs.writeFileSync(
  path.join(arrowDirectory, 'arrow-flight-platform.json'),
  `${JSON.stringify(arrowDashboard(), null, 2)}\n`,
);
fs.writeFileSync(
  path.join(otelDirectory, 'otel-collector-baseline.json'),
  `${JSON.stringify(otelDashboard(), null, 2)}\n`,
);
