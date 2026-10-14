import path from 'node:path';

import YAML from 'yaml';

export const benchmarkBufferBudget = Object.freeze({
  global_capacity: Object.freeze({ bytes: 134217728, rows: 4000000, batches: 2048 }),
  per_table_capacity: Object.freeze({ bytes: 33554432, rows: 1000000, batches: 512 }),
  max_arrow_memory_per_flush: 33554432,
});

interface DirectIdentity {
  instanceKey: string;
  advertisedEndpoint: string;
  generation: number;
}

export function buildBenchmarkRuntimeConfigs(
  directSource: string,
  dataflowSource: string,
  identity?: DirectIdentity,
): { directYaml: string; dataflowYaml: string } {
  if (identity) {
    if (!Number.isSafeInteger(identity.generation) || identity.generation <= 0) throw new Error('Invalid direct generation');
    directSource = directSource
      .replaceAll('__INSTANCE_KEY__', identity.instanceKey)
      .replaceAll('__ADVERTISED_ENDPOINT__', identity.advertisedEndpoint)
      .replaceAll('__GENERATION__', String(identity.generation));
  }
  const direct = YAML.parse(directSource);
  const dataflow = YAML.parse(dataflowSource);
  const nodes = dataflow.groups.arrowcol.pipelines.ingest.nodes;
  for (const buffer of [direct.buffer, nodes.delta.config.batching]) {
    buffer.global_capacity = { ...benchmarkBufferBudget.global_capacity };
    buffer.per_table_capacity = { ...benchmarkBufferBudget.per_table_capacity };
    buffer.max_arrow_memory_per_flush = benchmarkBufferBudget.max_arrow_memory_per_flush;
    if (buffer.max_rows_per_flush > buffer.per_table_capacity.rows ||
        buffer.max_batches_per_flush > buffer.per_table_capacity.batches) {
      throw new Error('Benchmark capacity is below the preserved flush row/batch limits');
    }
  }
  Object.assign(nodes.flight.config.registration, {
    buffered_bytes_limit: benchmarkBufferBudget.global_capacity.bytes,
    buffered_rows_limit: benchmarkBufferBudget.global_capacity.rows,
    buffered_batches_limit: benchmarkBufferBudget.global_capacity.batches,
  });
  return { directYaml: YAML.stringify(direct), dataflowYaml: YAML.stringify(dataflow) };
}

export function buildBenchmarkComposeOverride(hostDirectory: string): string {
  return JSON.stringify({
    services: {
      'dataflow-1': {
        volumes: [{
          type: 'bind',
          source: path.join(hostDirectory, 'dataflow-state-1/dataflow.yaml'),
          target: '/etc/arrowcol-dataflow-rust/config.yaml',
          read_only: true,
          bind: { create_host_path: false },
        }],
      },
    },
  }, null, 2);
}

export function benchmarkComposeFiles(baseFile: string, env: NodeJS.ProcessEnv): string[] {
  return [
    '--file', baseFile,
    ...(env.LOADTEST_COMPOSE_OVERRIDE_FILE ? ['--file', env.LOADTEST_COMPOSE_OVERRIDE_FILE] : []),
  ];
}
