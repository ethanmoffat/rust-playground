#!/usr/bin/env node

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import YAML from 'yaml';

const scriptsDirectory = path.dirname(fileURLToPath(import.meta.url));
const observabilityDirectory = path.resolve(scriptsDirectory, '..');
const loadtestDirectory = path.resolve(observabilityDirectory, '..');
const repositoryRoot = path.resolve(loadtestDirectory, '../..');
const generatedRulesPath = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(observabilityDirectory, '.validation', 'dashboard-promql.rules.yml');

function walk(directory, predicate) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const fullPath = path.join(directory, entry.name);
    if (entry.isDirectory()) return walk(fullPath, predicate);
    return predicate(fullPath) ? [fullPath] : [];
  });
}

function readYaml(file) {
  const documents = YAML.parseAllDocuments(fs.readFileSync(file, 'utf8'));
  for (const document of documents) {
    assert.equal(document.errors.length, 0, `${path.relative(repositoryRoot, file)}: ${document.errors.join('; ')}`);
  }
  assert.equal(documents.length, 1, `${path.relative(repositoryRoot, file)} must contain one YAML document`);
  return documents[0].toJSON();
}

function collectStrings(value, output = []) {
  if (typeof value === 'string') output.push(value);
  else if (Array.isArray(value)) value.forEach((entry) => collectStrings(entry, output));
  else if (value && typeof value === 'object') Object.values(value).forEach((entry) => collectStrings(entry, output));
  return output;
}

const yamlFiles = [
  ...walk(observabilityDirectory, (file) => /\.ya?ml$/u.test(file)),
  path.join(loadtestDirectory, 'compose.observability.yaml'),
];
for (const file of yamlFiles) readYaml(file);

const compose = readYaml(path.join(loadtestDirectory, 'compose.observability.yaml'));
const mainCompose = readYaml(path.join(loadtestDirectory, 'compose.yaml'));
const prometheusConfig = readYaml(path.join(observabilityDirectory, 'prometheus', 'prometheus.yml'));
const dashboardProvisioning = readYaml(
  path.join(observabilityDirectory, 'grafana', 'provisioning', 'dashboards', 'dashboards.yaml'),
);
const datasourceProvisioning = readYaml(
  path.join(observabilityDirectory, 'grafana', 'provisioning', 'datasources', 'datasources.yaml'),
);
const sqlCollectorFiles = walk(
  path.join(observabilityDirectory, 'sql-exporter', 'collectors'),
  (file) => file.endsWith('.yml'),
);
const sqlCollectors = sqlCollectorFiles.map(readYaml);

for (const serviceName of [
  'router',
  'direct-1',
  'direct-2',
  'direct-3',
  'dataflow-1',
  'dataflow-2',
  'dataflow-3',
  'load-driver',
  'sqlserver',
  'hive-metastore',
]) {
  assert(mainCompose.services?.[serviceName], `Prometheus target service disappeared from main Compose: ${serviceName}`);
}

assert.equal(prometheusConfig.global.scrape_interval, '2s');
assert.equal(prometheusConfig.global.evaluation_interval, '2s');
assert.equal(prometheusConfig.global.scrape_native_histograms, true);
assert.equal(prometheusConfig.global.convert_classic_histograms_to_nhcb, true);

const requiredJobs = new Set([
  'prometheus',
  'arrow-router',
  'arrow-direct',
  'arrow-dataflow',
  'otel-baseline',
  'arrow-sql-control-plane',
  'dfe-capabilities',
  'dfe-pressure',
  'service-readiness',
  'dependency-tcp',
  'cadvisor',
  'observability-stack',
]);
for (const job of prometheusConfig.scrape_configs) {
  requiredJobs.delete(job.job_name);
  for (const config of job.static_configs ?? []) {
    for (const label of ['implementation_pool', 'instance', 'run_id', 'scenario']) {
      assert.equal(typeof config.labels?.[label], 'string', `${job.job_name} static target is missing ${label}`);
    }
  }
}
assert.deepEqual([...requiredJobs], [], `missing Prometheus jobs: ${[...requiredJobs].join(', ')}`);

const imagePattern = /^[^@\s]+:[^@\s]+@sha256:[0-9a-f]{64}$/u;
for (const [serviceName, service] of Object.entries(compose.services)) {
  assert.match(service.image, imagePattern, `${serviceName} image must use an exact tag and digest`);
  assert(!service.image.includes(':latest'), `${serviceName} must not use latest`);
  if (service.privileged === true) {
    assert.equal(serviceName, 'cadvisor', 'only cAdvisor may be privileged');
  }
}
const observabilityIps = Object.entries(compose.services).map(([serviceName, service]) => {
  const ip = service.networks?.default?.ipv4_address;
  assert.match(ip, /^10\.89\.0\.1\d\d$/u, `${serviceName} must use the reserved observability IP range`);
  return ip;
});
assert.equal(new Set(observabilityIps).size, observabilityIps.length, 'observability service IPs must be unique');

for (const serviceName of ['prometheus', 'grafana', 'loki', 'tempo']) {
  assert(Array.isArray(compose.services[serviceName].tmpfs), `${serviceName} data must be tmpfs-backed`);
}
assert(!compose.volumes, 'the overlay must not define persistent named volumes');
assert(
  compose.services.prometheus.command.some((entry) => entry.includes('--storage.tsdb.retention.time=6h')),
  'Prometheus retention must be bounded',
);
assert.equal(compose.services.grafana.environment.GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH,
  '/var/lib/grafana/dashboards/arrow-flight/arrow-flight-platform.json');

for (const serviceName of ['prometheus', 'loki', 'tempo', 'alloy', 'sql-exporter', 'json-exporter', 'blackbox-exporter']) {
  assert.equal(compose.services[serviceName].read_only, true, `${serviceName} root filesystem must be read-only`);
}

const datasourceUids = new Set(datasourceProvisioning.datasources.map((source) => source.uid));
assert.deepEqual([...datasourceUids].sort(), ['loki', 'prometheus', 'tempo']);
assert.equal(datasourceProvisioning.datasources.find((source) => source.uid === 'prometheus').jsonData.timeInterval, '2s');
assert(
  datasourceProvisioning.datasources.find((source) => source.uid === 'loki').jsonData.derivedFields
    .some((field) => field.datasourceUid === 'tempo'),
  'Loki must link trace IDs to Tempo',
);

const providers = new Map(dashboardProvisioning.providers.map((provider) => [provider.folderUid, provider]));
assert.equal(providers.get('arrow-flight')?.folder, 'Arrow Flight');
assert.equal(providers.get('otel-collector-baseline')?.folder, 'OTEL Collector Baseline');
assert.notEqual(providers.get('arrow-flight')?.options.path, providers.get('otel-collector-baseline')?.options.path);

const dashboardFiles = walk(
  path.join(observabilityDirectory, 'grafana', 'dashboards'),
  (file) => file.endsWith('.json'),
);
assert.equal(dashboardFiles.length, 2, 'exactly two provisioned dashboards are expected');
const dashboards = dashboardFiles.map((file) => {
  const dashboard = JSON.parse(fs.readFileSync(file, 'utf8'));
  return { dashboard, file };
});
assert.equal(new Set(dashboards.map(({ dashboard }) => dashboard.uid)).size, dashboards.length, 'dashboard UIDs must be unique');
assert.equal(new Set(dashboards.map(({ dashboard }) => dashboard.title)).size, dashboards.length, 'dashboard titles must be unique');

const arrow = dashboards.find(({ dashboard }) => dashboard.uid === 'arrow-flight-platform');
const otel = dashboards.find(({ dashboard }) => dashboard.uid === 'otel-collector-baseline');
assert(arrow, 'Arrow dashboard is missing');
assert(otel, 'OTEL baseline dashboard is missing');
assert(arrow.file.includes(`${path.sep}arrow-flight${path.sep}`), 'Arrow dashboard is in the wrong folder');
assert(otel.file.includes(`${path.sep}otel-baseline${path.sep}`), 'OTEL dashboard is in the wrong folder');

const arrowPanelTitles = new Set(arrow.dashboard.panels.map((panel) => panel.title));
const requiredArrowPanels = [
  'Throughput: rows/s',
  'Wire bytes/s by container (proxy)',
  'Active streams',
  'Routing decisions',
  'Table ownership',
  'Routing lease age',
  'Lease churn',
  'Collector pressure / staleness',
  'Direct receipts by state',
  'Direct receipt time-to-publish',
  'Direct receipt failures',
  'Buffer rows',
  'Buffer bytes',
  'Oldest buffer age',
  'Flush reasons — unavailable',
  'File size actuals',
  'Delta commit latency',
  'DFE channel occupancy',
  'DFE downstream Ack/Nack',
  'DFE delivery capability',
  'Unsupported: DFE receipts',
  'Unsupported: arbitrary-Arrow native Quiver',
  'Schema failures',
  'CPU utilization',
  'RSS',
  'Disk I/O',
  'SQL / Hive health',
  'Container health',
  'Service logs',
  'Recent traces (when emitted)',
];
for (const title of requiredArrowPanels) assert(arrowPanelTitles.has(title), `Arrow dashboard is missing ${title}`);

const otelPanelTitles = new Set(otel.dashboard.panels.map((panel) => panel.title));
for (const title of [
  'OTEL baseline target health',
  'OTEL message throughput',
  'OTEL channel queue depth',
  'OTEL CPU / RSS',
  'OTEL exporter attempts',
  'OTEL exporter attempt latency',
  'OTEL baseline logs',
  'OTEL baseline traces (when emitted)',
]) {
  assert(otelPanelTitles.has(title), `OTEL dashboard is missing ${title}`);
}
assert(!collectStrings(otel.dashboard).some((value) => value.includes('arrowcol_router_')),
  'OTEL dashboard must not contain Arrow router metrics');

const arrowVariables = new Map(arrow.dashboard.templating.list.map((variableEntry) => [variableEntry.name, variableEntry]));
for (const variableName of ['run', 'scenario', 'table']) {
  assert(arrowVariables.has(variableName), `Arrow dashboard variable ${variableName} is missing`);
  assert(
    arrowVariables.get(variableName).query.query.includes('topk('),
    `${variableName} variable query must bound cardinality with topk`,
  );
}

for (const collector of sqlCollectors) {
  for (const metric of collector.metrics ?? []) {
    assert(!(metric.key_labels ?? []).includes('receipt_id'), `${metric.metric_name} must not label receipt IDs`);
    assert(!(metric.key_labels ?? []).includes('lease_id'), `${metric.metric_name} must not label lease IDs`);
  }
}
const dashboardText = dashboards.map(({ dashboard }) => JSON.stringify(dashboard)).join('\n');
assert(!/receipt_id\s*=|receipt_id"\s*:/u.test(dashboardText), 'dashboards must not query receipt IDs');

const contract = JSON.parse(fs.readFileSync(path.join(observabilityDirectory, 'metrics-contract.json'), 'utf8'));
for (const [component, definition] of Object.entries(contract)) {
  const sourcePath = path.join(repositoryRoot, definition.source);
  assert(fs.existsSync(sourcePath), `${component} metric source is missing: ${definition.source}`);
  const source = fs.readFileSync(sourcePath, 'utf8');
  for (const expected of definition.metrics ?? definition.contracts) {
    assert(source.includes(expected), `${component} metric contract changed or disappeared: ${expected}`);
  }
}

const definedSqlMetrics = new Set(sqlCollectors.flatMap((collector) => collector.metrics.map((metric) => metric.metric_name)));
for (const expected of [
  'arrowcol_sql_routing_lease_info',
  'arrowcol_sql_direct_receipts',
  'arrowcol_sql_direct_receipt_time_to_publish_seconds',
  'arrowcol_sql_direct_delta_commit_latency_seconds',
  'arrowcol_sql_schema_saga_failures_total',
]) {
  assert(definedSqlMetrics.has(expected), `SQL exporter is missing ${expected}`);
}

function normalizedPromql(expr) {
  return expr
    .replaceAll('$__rate_interval', '30s')
    .replaceAll('$__interval', '30s')
    .replaceAll('$__range', '30m')
    .replace(/\$\{[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z0-9_]+)?\}/gu, '.*')
    .replace(/\$[A-Za-z_][A-Za-z0-9_]*/gu, '.*');
}

const promql = [];
for (const { dashboard } of dashboards) {
  for (const panel of dashboard.panels) {
    const panelDatasource = panel.datasource?.uid;
    for (const target of panel.targets ?? []) {
      const datasourceUid = target.datasource?.uid ?? panelDatasource;
      if (datasourceUid === 'prometheus' && typeof target.expr === 'string') {
        promql.push(normalizedPromql(target.expr));
      }
    }
  }
  for (const annotation of dashboard.annotations?.list ?? []) {
    const expr = annotation.target?.expr ?? annotation.expr;
    if (annotation.datasource?.uid === 'prometheus' && typeof expr === 'string') {
      promql.push(normalizedPromql(expr));
    }
  }
}

fs.mkdirSync(path.dirname(generatedRulesPath), { recursive: true });
fs.writeFileSync(generatedRulesPath, YAML.stringify({
  groups: [{
    name: 'dashboard-promql-validation',
    rules: promql.map((expr, index) => ({
      record: `dashboard_validation_query_${String(index + 1).padStart(3, '0')}`,
      expr,
    })),
  }],
}));

console.log(`Validated ${yamlFiles.length} YAML files, ${dashboards.length} dashboards, and ${promql.length} PromQL expressions.`);
