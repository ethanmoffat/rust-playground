import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join, win32 } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  cleanupDevcontainer,
  buildCiEnvironment,
  createCiLabel,
  extractContainerId,
  exportImages,
  exportReader,
  importIntegrationArtifacts,
  identifyCiEnvironment,
  normalizeComposeProject,
  prepareCiEnvironment,
  startDevcontainer,
  type CiFileSystem,
  verifyDevcontainer,
  verifyHost,
} from '../src/ci-lib.js';
import {
  CommandError,
  type CommandOptions,
  type CommandResult,
  type CommandRunner,
} from '../src/process.js';

interface CommandCall {
  readonly args: readonly string[];
  readonly command: string;
  readonly options?: CommandOptions;
}

class FakeRunner implements CommandRunner {
  readonly calls: CommandCall[] = [];

  constructor(private readonly results: CommandResult[]) {}

  run(
    command: string,
    args: readonly string[],
    options?: CommandOptions,
  ): CommandResult {
    this.calls.push({ args: [...args], command, options });
    const result = this.results.shift();
    assert.ok(result, `Unexpected command: ${command} ${args.join(' ')}`);
    return result;
  }

  assertComplete(): void {
    assert.equal(this.results.length, 0);
  }
}

class FakeFileSystem implements CiFileSystem {
  readonly appends: Array<[string, string]> = [];
  readonly directories: string[] = [];
  readonly removed: string[] = [];
  readonly writes: Array<[string, string]> = [];
  readonly contents = new Map<string, string>();
  readonly entries = new Map<string, readonly string[]>();
  readonly files = new Set<string>();

  appendFile(filePath: string, contents: string): void {
    this.appends.push([filePath, contents]);
  }

  ensurePrivateDirectory(directoryPath: string): void {
    this.directories.push(directoryPath);
  }

  fileExists(filePath: string): boolean {
    return this.files.has(filePath) || this.contents.has(filePath);
  }

  readDirectory(directoryPath: string): readonly string[] {
    const entries = this.entries.get(directoryPath);
    assert.ok(entries, `Unexpected directory read: ${directoryPath}`);
    return entries;
  }

  readFile(filePath: string): string {
    const contents = this.contents.get(filePath);
    assert.ok(contents !== undefined, `Unexpected file read: ${filePath}`);
    return contents;
  }

  removeFile(filePath: string): void {
    this.removed.push(filePath);
  }

  writePrivateFile(filePath: string, contents: string): void {
    this.writes.push([filePath, contents]);
    this.contents.set(filePath, contents);
  }
}

const success = (stdout = ''): CommandResult => ({
  exitCode: 0,
  stderr: '',
  stdout,
});

const failure = (exitCode: number, stdout = ''): CommandResult => ({
  exitCode,
  stderr: 'failure',
  stdout,
});

const sourceContainerId = 'a'.repeat(64);
const publishedContainerId = 'b'.repeat(64);

test('prepares only private host credential state required by Compose', () => {
  const fileSystem = new FakeFileSystem();
  const home = win32.join('C:\\', 'Users', 'runner');
  const workspace = win32.join('C:\\', 'checkout');

  prepareCiEnvironment({
    fileSystem,
    home,
    workspace,
  });

  assert.deepEqual(fileSystem.directories, [
    join(home, '.azure'),
    join(home, '.config', 'gh'),
  ]);
  assert.deepEqual(fileSystem.writes, [
    [
      join(workspace, '.devcontainer', '.env'),
      [
        'HOST_AZURE_DIR=C:/Users/runner/.azure',
        'HOST_GH_CONFIG_DIR=C:/Users/runner/.config/gh',
        '',
      ].join('\n'),
    ],
  ]);
  assert.equal(
    fileSystem.writes.some(([filePath]) => filePath === join(workspace, '.env')),
    false,
  );
});

test('normalizes lifecycle identifiers without accepting container names', () => {
  assert.equal(
    extractContainerId(
      JSON.stringify({
        containerId: sourceContainerId,
        remoteUser: 'vscode',
      }),
    ),
    sourceContainerId,
  );
  assert.equal(extractContainerId('{"containerId":"workspace"}'), undefined);
  assert.equal(extractContainerId('not JSON'), undefined);
  assert.equal(normalizeComposeProject('<no value>\n'), undefined);
  assert.equal(
    normalizeComposeProject('rust-playground-ci\n'),
    'rust-playground-ci',
  );
  assert.throws(
    () => normalizeComposeProject('Unsafe Project'),
    /Invalid Docker Compose project/u,
  );
  assert.equal(
    createCiLabel('42', '3', 'source'),
    'rust-playground.ci-run=42-3-source',
  );
  assert.equal(
    createCiLabel('42', '3', 'published'),
    'rust-playground.ci-run=42-3-published',
  );
});

test('starts the source config, validates the exact container, and writes outputs', () => {
  const workspace = join('C:\\', 'checkout');
  const runner = new FakeRunner([
    success(`${JSON.stringify({ containerId: sourceContainerId })}\n`),
    success(`${sourceContainerId}\n`),
    success('100-2-source\n'),
    success('rust-playground-ci\n'),
  ]);
  const fileSystem = new FakeFileSystem();
  const output: string[] = [];

  startDevcontainer(
    {
      githubOutput: join('C:\\', 'github', 'output'),
      kind: 'source',
      runAttempt: '2',
      runId: '100',
      workspace,
    },
    runner,
    fileSystem,
    (value) => output.push(value),
  );

  assert.deepEqual(runner.calls[0], {
    args: [
      '--no-install',
      'devcontainer',
      'up',
      '--workspace-folder',
      workspace,
      '--config',
      join(workspace, '.devcontainer', 'source', 'devcontainer.json'),
      '--frozen-lockfile',
      '--id-label',
      'rust-playground.ci-run=100-2-source',
    ],
    command: 'npx',
    options: {
      cwd: workspace,
      stdout: 'capture',
    },
  });
  assert.deepEqual(
    runner.calls.slice(1).map(({ args }) => args),
    [
      ['inspect', '--format', '{{ .Id }}', sourceContainerId],
      [
        'inspect',
        '--format',
        '{{ index .Config.Labels "rust-playground.ci-run" }}',
        sourceContainerId,
      ],
      [
        'inspect',
        '--format',
        '{{ index .Config.Labels "com.docker.compose.project" }}',
        sourceContainerId,
      ],
    ],
  );
  assert.deepEqual(fileSystem.appends, [
    [
      join('C:\\', 'github', 'output'),
      'ci_label=rust-playground.ci-run=100-2-source\nstart_kind=source\n',
    ],
    [
      join('C:\\', 'github', 'output'),
      `container_id=${sourceContainerId}\ncompose_project=rust-playground-ci\n`,
    ],
  ]);
  assert.deepEqual(output, [
    `${JSON.stringify({ containerId: sourceContainerId })}\n`,
  ]);
  runner.assertComplete();
});

test('published startup uses the immutable-image config without source locking', () => {
  const workspace = join('C:\\', 'checkout');
  const runner = new FakeRunner([
    success(JSON.stringify({ containerId: publishedContainerId })),
    success(publishedContainerId),
    success('101-1-published'),
    success('rust-playground-published'),
  ]);

  startDevcontainer(
    {
      githubOutput: join('C:\\', 'github', 'output'),
      kind: 'published',
      runAttempt: '1',
      runId: '101',
      workspace,
    },
    runner,
    new FakeFileSystem(),
    () => undefined,
  );

  assert.deepEqual(runner.calls[0]?.args, [
    '--no-install',
    'devcontainer',
    'up',
    '--workspace-folder',
    workspace,
    '--config',
    join(workspace, '.devcontainer', 'devcontainer.json'),
    '--id-label',
    'rust-playground.ci-run=101-1-published',
  ]);
  runner.assertComplete();
});

test('records a labeled partial startup before returning the original failure', () => {
  const runner = new FakeRunner([
    failure(17, 'not JSON\n'),
    success(`${sourceContainerId}\n`),
    success(`${sourceContainerId}\n`),
    success('200-1-source\n'),
    success('partial-project\n'),
  ]);
  const fileSystem = new FakeFileSystem();

  assert.throws(
    () =>
      startDevcontainer(
        {
          githubOutput: join('C:\\', 'github', 'output'),
          kind: 'source',
          runAttempt: '1',
          runId: '200',
          workspace: join('C:\\', 'checkout'),
        },
        runner,
        fileSystem,
        () => undefined,
      ),
    (error: unknown) => error instanceof CommandError && error.exitCode === 17,
  );

  assert.deepEqual(runner.calls[1], {
    args: [
      'ps',
      '-aq',
      '--no-trunc',
      '--filter',
      'label=rust-playground.ci-run=200-1-source',
    ],
    command: 'docker',
    options: {
      stderr: 'capture',
      stdout: 'capture',
    },
  });
  assert.deepEqual(fileSystem.appends.at(-1), [
    join('C:\\', 'github', 'output'),
    `container_id=${sourceContainerId}\ncompose_project=partial-project\n`,
  ]);
  runner.assertComplete();
});

test('failed startup still emits the unique cleanup label when no container exists', () => {
  const runner = new FakeRunner([
    failure(9, 'startup failed\n'),
    success(''),
  ]);
  const fileSystem = new FakeFileSystem();

  assert.throws(
    () =>
      startDevcontainer(
        {
          githubOutput: join('C:\\', 'github', 'output'),
          kind: 'source',
          runAttempt: '4',
          runId: '300',
          workspace: join('C:\\', 'checkout'),
        },
        runner,
        fileSystem,
        () => undefined,
      ),
    (error: unknown) => error instanceof CommandError && error.exitCode === 9,
  );
  assert.deepEqual(fileSystem.appends, [
    [
      join('C:\\', 'github', 'output'),
      'ci_label=rust-playground.ci-run=300-4-source\nstart_kind=source\n',
    ],
  ]);
  runner.assertComplete();
});

test('successful startup fails closed if no exact container can be recovered', () => {
  const runner = new FakeRunner([success('not JSON\n'), success('')]);

  assert.throws(
    () =>
      startDevcontainer(
        {
          githubOutput: join('C:\\', 'github', 'output'),
          kind: 'source',
          runAttempt: '1',
          runId: '301',
          workspace: join('C:\\', 'checkout'),
        },
        runner,
        new FakeFileSystem(),
        () => undefined,
      ),
    /succeeded without returning a container ID/u,
  );
  runner.assertComplete();
});

test('startup rejects a container whose inspected CI label does not match', () => {
  const runner = new FakeRunner([
    success(JSON.stringify({ containerId: sourceContainerId })),
    success(sourceContainerId),
    success('999-1-source'),
  ]);

  assert.throws(
    () =>
      startDevcontainer(
        {
          githubOutput: join('C:\\', 'github', 'output'),
          kind: 'source',
          runAttempt: '1',
          runId: '302',
          workspace: join('C:\\', 'checkout'),
        },
        runner,
        new FakeFileSystem(),
        () => undefined,
      ),
    /expected rust-playground\.ci-run=302-1-source/u,
  );
  runner.assertComplete();
});

test('runs parallel verification and the clean-tree gate in the exact container', () => {
  const runner = new FakeRunner([success(), success()]);

  verifyDevcontainer({ containerId: sourceContainerId, parallel: 4 }, runner);

  assert.deepEqual(runner.calls, [
    {
      args: [
        'exec',
        '--user',
        'root',
        '--workdir',
        '/workspaces/rust-playground',
        sourceContainerId,
        'bash',
        '-lc',
        'install -d -m 0755 -o "$(id -u vscode)" -g "$(id -g vscode)" onelake',
      ],
      command: 'docker',
      options: undefined,
    },
    {
      args: [
        'exec',
        '--user',
        'vscode',
        '--workdir',
        '/workspaces/rust-playground',
        sourceContainerId,
        'bash',
        '-lc',
        'npx --no-install nx "$@" && npx --no-install nx run tools-scripts:clean-worktree --outputStyle=static',
        'ci-verify',
        'run-many',
        '-t',
        'verify',
        '--all',
        '--parallel=4',
        '--outputStyle=static',
      ],
      command: 'docker',
      options: undefined,
    },
  ]);
  runner.assertComplete();
});

test('does not run verification when CI output directory preparation fails', () => {
  const runner = new FakeRunner([failure(1, 'cannot prepare output')]);
  assert.throws(
    () => verifyDevcontainer({ containerId: sourceContainerId }, runner),
    CommandError,
  );
  assert.equal(runner.calls.length, 1);
  runner.assertComplete();
});

test('cleans only the recorded Compose project and its volumes', () => {
  const workspace = join('C:\\', 'checkout');
  const runner = new FakeRunner([success()]);
  const fileSystem = new FakeFileSystem();

  cleanupDevcontainer(
    {
      ciLabels: ['rust-playground.ci-run=400-1-source'],
      composeProject: 'recorded-project',
      containerId: sourceContainerId,
      workspace,
    },
    runner,
    fileSystem,
  );

  assert.deepEqual(runner.calls, [
    {
      args: [
        'compose',
        '--project-name',
        'recorded-project',
        '--project-directory',
        join(workspace, '.devcontainer'),
        '--file',
        join(workspace, '.devcontainer', 'docker-compose.yml'),
        'down',
        '--volumes',
        '--remove-orphans',
      ],
      command: 'docker',
      options: undefined,
    },
  ]);
  assert.deepEqual(fileSystem.removed, [
    join(workspace, '.devcontainer', '.env'),
    join(workspace, '.temp', 'ci', 'devcontainer.json'),
    join(workspace, '.temp', 'ci', 'compose.json'),
  ]);
  runner.assertComplete();
});

test('discovers and deduplicates partial-startup projects by the unique label', () => {
  const workspace = join('C:\\', 'checkout');
  const secondContainerId = 'c'.repeat(64);
  const label = 'rust-playground.ci-run=401-2-source';
  const runner = new FakeRunner([
    success(`${sourceContainerId}\n${secondContainerId}\n`),
    success(sourceContainerId),
    success('401-2-source'),
    success('partial-project'),
    success(secondContainerId),
    success('401-2-source'),
    success('partial-project'),
    success(),
  ]);
  const fileSystem = new FakeFileSystem();

  cleanupDevcontainer(
    {
      ciLabels: [label],
      workspace,
    },
    runner,
    fileSystem,
  );

  assert.deepEqual(runner.calls[0], {
    args: [
      'ps',
      '-aq',
      '--no-trunc',
      '--filter',
      `label=${label}`,
    ],
    command: 'docker',
    options: {
      stderr: 'capture',
      stdout: 'capture',
    },
  });
  assert.equal(
    runner.calls.filter(({ args }) => args[0] === 'compose').length,
    1,
  );
  assert.equal(
    runner.calls.some(
      ({ args }) => args[0] === 'rm' && args.includes(sourceContainerId),
    ),
    false,
  );
  assert.deepEqual(fileSystem.removed, [
    join(workspace, '.devcontainer', '.env'),
    join(workspace, '.temp', 'ci', 'devcontainer.json'),
    join(workspace, '.temp', 'ci', 'compose.json'),
  ]);
  runner.assertComplete();
});

test('force-removes only a uniquely labeled non-Compose partial container', () => {
  const workspace = join('C:\\', 'checkout');
  const runner = new FakeRunner([
    success(`${sourceContainerId}\n`),
    success(sourceContainerId),
    success('402-1-source'),
    success('<no value>\n'),
    success(),
  ]);
  const fileSystem = new FakeFileSystem();

  cleanupDevcontainer(
    {
      ciLabels: ['rust-playground.ci-run=402-1-source'],
      workspace,
    },
    runner,
    fileSystem,
  );

  assert.deepEqual(runner.calls.at(-1), {
    args: ['rm', '--force', sourceContainerId],
    command: 'docker',
    options: undefined,
  });
  assert.deepEqual(fileSystem.removed, [
    join(workspace, '.devcontainer', '.env'),
    join(workspace, '.temp', 'ci', 'devcontainer.json'),
    join(workspace, '.temp', 'ci', 'compose.json'),
  ]);
  runner.assertComplete();
});

test('refuses to clean an exact container with a mismatched run label', () => {
  const workspace = join('C:\\', 'checkout');
  const runner = new FakeRunner([
    success(''),
    success(sourceContainerId),
    success('999-1-source'),
  ]);
  const fileSystem = new FakeFileSystem();

  assert.throws(
    () =>
      cleanupDevcontainer(
        {
          ciLabels: ['rust-playground.ci-run=403-1-source'],
          containerId: sourceContainerId,
          workspace,
        },
        runner,
        fileSystem,
      ),
    /does not have an expected CI run label/u,
  );
  assert.equal(
    runner.calls.some(({ args }) => args[0] === 'rm' || args[0] === 'compose'),
    false,
  );
  assert.deepEqual(fileSystem.removed, [
    join(workspace, '.devcontainer', '.env'),
    join(workspace, '.temp', 'ci', 'devcontainer.json'),
    join(workspace, '.temp', 'ci', 'compose.json'),
  ]);
  runner.assertComplete();
});

test('removes generated CI environment state even when Compose cleanup fails', () => {
  const workspace = join('C:\\', 'checkout');
  const runner = new FakeRunner([failure(12)]);
  const fileSystem = new FakeFileSystem();

  assert.throws(
    () =>
      cleanupDevcontainer(
        {
          composeProject: 'failed-project',
          workspace,
        },
        runner,
        fileSystem,
      ),
    (error: unknown) => error instanceof CommandError && error.exitCode === 12,
  );
  assert.deepEqual(fileSystem.removed, [
    join(workspace, '.devcontainer', '.env'),
    join(workspace, '.temp', 'ci', 'devcontainer.json'),
    join(workspace, '.temp', 'ci', 'compose.json'),
  ]);
  runner.assertComplete();
});

test('Nx targets expose CI lifecycle without adding verification recursion', () => {
  const repositoryRoot = fileURLToPath(new URL('../../../', import.meta.url));
  const project = JSON.parse(
    readFileSync(join(repositoryRoot, 'tools', 'scripts', 'project.json'), 'utf8'),
  ) as {
    targets: Record<
      string,
      {
        dependsOn?: readonly string[];
        options?: { command?: string; commands?: readonly string[] };
      }
    >;
  };

  for (const target of [
    'typecheck',
    'test',
    'verify',
    'clean-worktree',
    'ci-prepare',
    'ci-plan',
    'ci-build-environment',
    'ci-start-artifact',
    'ci-verify-host',
    'ci-export-images',
    'ci-export-reader',
    'ci-import-integration',
    'ci-start-source',
    'ci-start-published',
    'ci-verify',
    'ci-cleanup',
    'registry-login',
    'registry-tags',
    'registry-manifest',
    'registry-public',
  ]) {
    assert.ok(project.targets[target], `missing target: ${target}`);
  }
  assert.deepEqual(project.targets.verify?.dependsOn, ['typecheck', 'test']);
  assert.deepEqual(project.targets.verify?.options?.commands, []);
  assert.doesNotMatch(
    JSON.stringify(project.targets.verify),
    /ci-start|devcontainer:(?:up|test)/u,
  );
  assert.match(
    project.targets['ci-start-source']?.options?.command ?? '',
    /ci\.ts start-source$/u,
  );
});

test('runs a targeted container shard without replaying unrelated verification', () => {
  const runner = new FakeRunner([success(), success()]);
  verifyDevcontainer({
    containerId: sourceContainerId,
    task: 'arrow-deltalake:test',
    parallel: 3,
  }, runner);
  assert.deepEqual(runner.calls[1]?.args.slice(-4), [
    'run', 'arrow-deltalake:test', '--parallel=3', '--outputStyle=static',
  ]);
  assert.equal(runner.calls[1]?.args.includes('--excludeTaskDependencies'), false);
  runner.assertComplete();
});

test('only artifact-backed E2E may skip Nx build dependencies', () => {
  const runner = new FakeRunner([success(), success()]);
  verifyDevcontainer({
    containerId: sourceContainerId,
    task: 'loadtest:e2e',
    integration: true,
  }, runner);
  assert.equal(runner.calls[1]?.args.at(-1), '--excludeTaskDependencies');
  runner.assertComplete();
  const invalid = new FakeRunner([]);
  assert.throws(() => verifyDevcontainer({
    containerId: sourceContainerId,
    task: 'arrow-deltalake:test',
    integration: true,
  }, invalid), /Only loadtest:e2e/u);
  invalid.assertComplete();
});

test('host shards use argv and do not report clean-tree success after a failed check', () => {
  const runner = new FakeRunner([success(), success()]);
  verifyHost({ task: 'tools-scripts:typecheck', parallel: 4, workspace: '/checkout' }, runner);
  assert.deepEqual(runner.calls[0]?.args, [
    '--no-install', 'nx', 'run', 'tools-scripts:typecheck', '--parallel=4', '--outputStyle=static',
  ]);
  assert.equal(runner.calls[1]?.args.includes('tools-scripts:clean-worktree'), true);
  runner.assertComplete();
  const failed = new FakeRunner([failure(7)]);
  assert.throws(() => verifyHost({ task: 'tools-scripts:test', workspace: '/checkout' }, failed), CommandError);
  assert.equal(failed.calls.length, 1);
  failed.assertComplete();
});

test('CI image jobs use persistent scoped BuildKit caching without injecting credentials into argv', () => {
  const runner = new FakeRunner([success(), success()]);
  verifyHost({ task: 'otelcol-rust:image', dockerCache: true, workspace: '/checkout' }, runner);
  const args = runner.calls[0]?.args ?? [];
  assert.ok(args.includes('--configuration=ci-verify'));
  assert.ok(!args.some((argument) => argument.startsWith('--cacheFrom') || argument.startsWith('--cacheTo')));
  assert.doesNotMatch(JSON.stringify(args), /token=|secret=/u);
  runner.assertComplete();
  assert.throws(() => verifyHost({
    task: 'tools-scripts:test', dockerCache: true, workspace: '/checkout',
  }, new FakeRunner([])), /requires an image task/u);
});

test('host E2E reuses artifacts but cannot skip build dependencies for unrelated checks', () => {
  const runner = new FakeRunner([success(), success()]);
  verifyHost({ task: 'loadtest:e2e', integration: true, workspace: '/checkout' }, runner);
  assert.equal(runner.calls[0]?.args.at(-1), '--excludeTaskDependencies');
  runner.assertComplete();
  assert.throws(() => verifyHost({
    task: 'tools-scripts:test', integration: true, workspace: '/checkout',
  }, new FakeRunner([])), /Only loadtest:e2e/u);
});

test('container batches run every selected project target with one shared build directory', () => {
  const runner = new FakeRunner([success(), success()]);
  verifyDevcontainer({
    containerId: sourceContainerId, task: ['arrow-deltalake:lint', 'arrow-deltalake:test'], parallel: 4,
  }, runner);
  assert.deepEqual(runner.calls[1]?.args.slice(-5), [
    'run-many', '--projects=arrow-deltalake', '--targets=lint,test',
    '--parallel=4', '--outputStyle=static',
  ]);
  runner.assertComplete();
});

test('builds the frozen source environment once and exports it without registry publication', () => {
  const id = `sha256:${'f'.repeat(64)}`;
  const runner = new FakeRunner([success(), success(), success(id), success(), success()]);
  const fileSystem = environmentFileSystem();
  const { image, key, baseImage } = identifyCiEnvironment('/checkout', fileSystem);
  buildCiEnvironment({
    githubOutput: '/output',
    workspace: '/checkout',
  }, runner, fileSystem);
  assert.deepEqual(runner.calls[0]?.args, [
    '--no-install', 'nx', 'run', 'devcontainer:build', '--excludeTaskDependencies',
  ]);
  assert.deepEqual(runner.calls[1]?.args, [
    'build', '--builder', 'default', '--file', '.github/ci/Dockerfile',
    '--build-arg', `SOURCE_DEVCONTAINER=${baseImage}`,
    '--tag', image, '.',
  ]);
  assert.deepEqual(runner.calls[3]?.args, [
    'save', '--output', '/checkout/.temp/ci/environment/environment.tar', image,
  ]);
  assert.equal(runner.calls[4]?.command, 'zstd');
  assert.deepEqual(runner.calls[4]?.args, [
    '--threads=0', '--fast=3', '--rm', '--force',
    '/checkout/.temp/ci/environment/environment.tar',
    '-o', '/checkout/.temp/ci/environment/environment.tar.zst',
  ]);
  assert.deepEqual(fileSystem.appends, [['/output', `image=${image}\nkey=${key}\n`]]);
  assert.deepEqual(JSON.parse(fileSystem.writes[0]?.[1] ?? ''), { image, id });
  assert.doesNotMatch(JSON.stringify(runner.calls), /login|push|prune/u);
  runner.assertComplete();
});

test('artifact startup refuses registry fallback and exposes cache directories as binds', () => {
  const id = `sha256:${'f'.repeat(64)}`;
  const runner = new FakeRunner([
    success(id),
    success(JSON.stringify({ containerId: sourceContainerId })),
    success(sourceContainerId),
    success('123-2-artifact'),
    success('ci-project'),
  ]);
  const fileSystem = environmentFileSystem();
  const { image } = identifyCiEnvironment('/checkout', fileSystem);
  setEnvironmentManifest(fileSystem, image, id);
  fileSystem.contents.set('/checkout/.devcontainer/devcontainer.json', JSON.stringify({
    service: 'workspace',
    workspaceFolder: '/workspaces/rust-playground',
    postCreateCommand: ['node', '.devcontainer/scripts/post-create.ts'],
  }));
  startDevcontainer({
    githubOutput: '/output', kind: 'artifact', runAttempt: '2', runId: '123',
    workspace: '/checkout', image,
  }, runner, fileSystem, () => undefined);
  assert.deepEqual(runner.calls[0]?.args, ['image', 'inspect', '--format', '{{ .Id }}', image]);
  assert.ok(runner.calls[1]?.args.includes('/checkout/.temp/ci/devcontainer.json'));
  const compose = JSON.parse(fileSystem.contents.get('/checkout/.temp/ci/compose.json') ?? '');
  assert.equal(compose.services.workspace.pull_policy, 'never');
  assert.equal(compose.services.workspace.image, image);
  assert.equal(compose.services.workspace.volumes.length, 3);
  assert.ok(compose.services.workspace.volumes.every((volume: { type: string }) => volume.type === 'bind'));
  const config = JSON.parse(fileSystem.contents.get('/checkout/.temp/ci/devcontainer.json') ?? '');
  assert.deepEqual(config.dockerComposeFile, [
    '/checkout/.devcontainer/docker-compose.yml', '/checkout/.temp/ci/compose.json',
  ]);
  assert.deepEqual(config.postCreateCommand, ['node', '.devcontainer/scripts/post-create.ts']);
  runner.assertComplete();
});

test('missing source image stops artifact startup while retaining the cleanup label', () => {
  const runner = new FakeRunner([failure(1)]);
  const fileSystem = environmentFileSystem();
  const { image } = identifyCiEnvironment('/checkout', fileSystem);
  setEnvironmentManifest(fileSystem, image, `sha256:${'f'.repeat(64)}`);
  assert.throws(() => startDevcontainer({
    githubOutput: '/output', kind: 'artifact', runAttempt: '1', runId: '124',
    workspace: '/checkout', image,
  }, runner, fileSystem), CommandError);
  assert.equal(runner.calls.length, 1);
  assert.equal(fileSystem.writes.length, 0);
  assert.match(fileSystem.appends[0]?.[1] ?? '', /124-1-artifact/u);
  runner.assertComplete();
});

test('artifact startup cannot substitute an image with stale source environment inputs', () => {
  const runner = new FakeRunner([]);
  const fileSystem = environmentFileSystem();
  assert.throws(() => startDevcontainer({
    githubOutput: '/output', kind: 'artifact', runAttempt: '1', runId: '124',
    workspace: '/checkout', image: `rust-playground-ci:env-${'b'.repeat(64)}`,
  }, runner, fileSystem), /source environment fingerprint/u);
  runner.assertComplete();
});

function environmentFileSystem(): FakeFileSystem {
  const fileSystem = new FakeFileSystem();
  fileSystem.contents.set('/checkout/.devcontainer/content-hash.txt', 'a'.repeat(64));
  for (const file of [
    '.dockerignore',
    '.devcontainer/Dockerfile',
    '.devcontainer/devcontainer.build.json',
    '.devcontainer/devcontainer-lock.json',
    '.devcontainer/source/devcontainer.json',
    '.devcontainer/source/devcontainer-lock.json',
    '.github/ci/Dockerfile',
  ]) {
    fileSystem.contents.set(`/checkout/${file}`, file);
  }
  return fileSystem;
}

function setEnvironmentManifest(fileSystem: FakeFileSystem, image: string, id: string): void {
  fileSystem.contents.set('/checkout/.temp/ci/environment/environment.json', JSON.stringify({ image, id }));
}

test('verified cached environments load directly without rebuilding tools or recompressing', () => {
  const fileSystem = environmentFileSystem();
  const definition = identifyCiEnvironment('/checkout', fileSystem);
  const id = `sha256:${'f'.repeat(64)}`;
  setEnvironmentManifest(fileSystem, definition.image, id);
  fileSystem.files.add('/checkout/.temp/ci/environment/environment.tar.zst');
  const runner = new FakeRunner([success(), success(id)]);
  buildCiEnvironment({ githubOutput: '/output', workspace: '/checkout' }, runner, fileSystem);
  assert.deepEqual(runner.calls[0]?.args, [
    'load', '--input', '/checkout/.temp/ci/environment/environment.tar.zst',
  ]);
  assert.equal(runner.calls[1]?.args.at(-1), definition.image);
  assert.equal(runner.calls.length, 2);
  runner.assertComplete();
});

test('incomplete or stale environment caches fail rather than hide corruption with a success fallback', () => {
  const incomplete = environmentFileSystem();
  incomplete.files.add('/checkout/.temp/ci/environment/environment.tar.zst');
  assert.throws(() => buildCiEnvironment({
    githubOutput: '/output', workspace: '/checkout',
  }, new FakeRunner([]), incomplete), /incomplete/u);
  const stale = environmentFileSystem();
  stale.files.add('/checkout/.temp/ci/environment/environment.tar.zst');
  setEnvironmentManifest(stale, `rust-playground-ci:env-${'b'.repeat(64)}`, `sha256:${'f'.repeat(64)}`);
  assert.throws(() => buildCiEnvironment({
    githubOutput: '/output', workspace: '/checkout',
  }, new FakeRunner([]), stale), /fingerprint mismatch/u);
});

test('environment keys change with toolchain inputs but not application-only changes', () => {
  const fileSystem = environmentFileSystem();
  const first = identifyCiEnvironment('/checkout', fileSystem).key;
  fileSystem.contents.set('/checkout/crates/new/src/lib.rs', 'fn updated() {}');
  assert.equal(identifyCiEnvironment('/checkout', fileSystem).key, first);
  fileSystem.contents.set('/checkout/.github/ci/Dockerfile', 'changed CI compiler settings');
  assert.notEqual(identifyCiEnvironment('/checkout', fileSystem).key, first);
});

test('environment image identities are checked before starting a cached artifact container', () => {
  const fileSystem = environmentFileSystem();
  const { image } = identifyCiEnvironment('/checkout', fileSystem);
  setEnvironmentManifest(fileSystem, image, `sha256:${'f'.repeat(64)}`);
  const runner = new FakeRunner([success(`sha256:${'a'.repeat(64)}`)]);
  assert.throws(() => startDevcontainer({
    githubOutput: '/output', kind: 'artifact', runAttempt: '1', runId: '123',
    workspace: '/checkout', image,
  }, runner, fileSystem), /image identity mismatch/u);
  runner.assertComplete();
});

test('exports exact runtime image identities alongside the Docker archive', () => {
  const id = `sha256:${'f'.repeat(64)}`;
  const runner = new FakeRunner([success(id), success()]);
  const fileSystem = new FakeFileSystem();
  exportImages({
    images: ['ghcr.io/example/collector:dev'], key: 'collector-image', workspace: '/checkout',
  }, runner, fileSystem);
  assert.deepEqual(runner.calls[1]?.args, [
    'save', '--output', '/checkout/.temp/ci/images/collector-image.tar', 'ghcr.io/example/collector:dev',
  ]);
  assert.deepEqual(JSON.parse(fileSystem.writes[0]?.[1] ?? ''), {
    images: [{ reference: 'ghcr.io/example/collector:dev', id }],
  });
  runner.assertComplete();
});

function integrationFileSystem(id: string): FakeFileSystem {
  const fileSystem = new FakeFileSystem();
  fileSystem.entries.set('/checkout/.temp/ci/images', ['collector.json', 'collector.tar']);
  fileSystem.contents.set('/checkout/.temp/ci/images/collector.json', JSON.stringify({
    images: [{ reference: 'ghcr.io/example/collector:dev', id }],
  }));
  return fileSystem;
}

test('imports complete artifacts, checks image identities, and retains executable reader permissions', () => {
  const id = `sha256:${'f'.repeat(64)}`;
  const runner = new FakeRunner([success(), success(id), success(), success()]);
  const fileSystem = integrationFileSystem(id);
  importIntegrationArtifacts({
    images: ['ghcr.io/example/collector:dev'], workspace: '/checkout',
  }, runner, fileSystem);
  assert.deepEqual(runner.calls[0]?.args, ['load', '--input', '/checkout/.temp/ci/images/collector.tar']);
  assert.deepEqual(runner.calls[2]?.args, [
    '--extract', '--file', '/checkout/.temp/ci/reader.tar',
    '--directory', '/checkout/target/ci/build/debug', 'delta-inspect',
  ]);
  assert.equal(runner.calls[3]?.command, 'test');
  assert.deepEqual(runner.calls[3]?.args, ['-x', '/checkout/target/ci/build/debug/delta-inspect']);
  runner.assertComplete();
});

test('host E2E imports the same executable into the standard Nx reader path without a tooling container', () => {
  const id = `sha256:${'f'.repeat(64)}`;
  const runner = new FakeRunner([success(), success(id), success(), success()]);
  importIntegrationArtifacts({
    images: ['ghcr.io/example/collector:dev'], workspace: '/checkout', host: true,
  }, runner, integrationFileSystem(id));
  assert.deepEqual(runner.calls[2]?.args, [
    '--extract', '--file', '/checkout/.temp/ci/reader.tar',
    '--directory', '/checkout/target/debug', 'delta-inspect',
  ]);
  runner.assertComplete();
});
test('integration artifacts fail on missing archives, wrong images, or mismatched identities', () => {
  const id = `sha256:${'f'.repeat(64)}`;
  const incomplete = integrationFileSystem(id);
  incomplete.entries.set('/checkout/.temp/ci/images', ['collector.json']);
  assert.throws(() => importIntegrationArtifacts({
    images: ['ghcr.io/example/collector:dev'], workspace: '/checkout',
  }, new FakeRunner([]), incomplete), /missing or incomplete/u);
  assert.throws(() => importIntegrationArtifacts({
    images: ['ghcr.io/example/other:dev'], workspace: '/checkout',
  }, new FakeRunner([]), integrationFileSystem(id)), /Unexpected or duplicate/u);
  const mismatched = new FakeRunner([success(), success(`sha256:${'a'.repeat(64)}`)]);
  assert.throws(() => importIntegrationArtifacts({
    images: ['ghcr.io/example/collector:dev'], workspace: '/checkout',
  }, mismatched, integrationFileSystem(id)), /identity mismatch/u);
  mismatched.assertComplete();
});

test('reader export preserves executable permissions inside a tar archive', () => {
  const runner = new FakeRunner([success()]);
  const fileSystem = new FakeFileSystem();
  exportReader({ containerId: sourceContainerId, workspace: '/checkout' }, runner, fileSystem);
  assert.deepEqual(runner.calls[0]?.args, [
    'exec', '--user', 'vscode', '--workdir', '/workspaces/rust-playground', sourceContainerId,
    'tar', '--create', '--file', '.temp/ci/reader.tar', '--directory', 'target/debug', 'delta-inspect',
  ]);
  runner.assertComplete();
});
